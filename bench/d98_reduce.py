#!/usr/bin/env python3
"""Reduce the D98 run files into the tables that go in bench/d98_outer_runtime_lock.txt.

TWO KINDS OF NUMBER, AND THEY ARE NOT EQUALLY TRUSTWORTHY.

  COUNTS   reaps per acquisition of the statement lock, and statements completed while the sweep
           ran. Integers. This box runs a dozen build agents and the fleet measure lock no longer
           gates on load, so nothing here is quiet -- but load does not change how many times a
           lock was taken. These carry the claim.

  DURATIONS p50/p99/max client latency. Real, and upper bounds: every one is inflated by whatever
           the fleet was doing, which is why `load_at_acquire` is stamped beside them.

Percentiles from different runs are NOT pooled -- that is not arithmetic you can do on summaries.
Every invocation's row is printed, and the headline pair is deliberately the CONSERVATIVE one:
the BEST `before` against the WORST `after`, so the factor quoted is the smallest the data
supports.
"""
import sys, glob, os
from collections import defaultdict

FIELDS = ("samples", "p50", "p99", "max", "wall", "reaped", "acqs", "per_acq", "done")


def rows(path):
    """(N, arm, K) -> dict of the SUMMARY row, for one run file."""
    out = {}
    with open(path) as fh:
        lines = fh.read().splitlines()
    try:
        i = next(k for k, l in enumerate(lines) if l.startswith("# SUMMARY"))
    except StopIteration:
        return out
    for l in lines[i:]:
        f = l.split()
        if len(f) != 12 or not f[0].isdigit():
            continue
        n, arm, k = int(f[0]), f[1], int(f[2])
        vals = {}
        for name, raw in zip(FIELDS, f[3:]):
            vals[name] = float(raw) if name == "per_acq" and raw != "-" else (
                None if raw == "-" else int(raw))
        out[(n, arm, k)] = vals
    return out


def us(ns):
    return "--" if ns is None else f"{ns/1000.0:,.1f}"


def main(d):
    # ---- REFUSE BEFORE REDUCING ------------------------------------------------------------
    #
    # The driver writes exit_codes.txt, one line per block. A block killed by `timeout` leaves a
    # TRUNCATED run file that parses fine and simply contributes fewer cells -- so silently
    # skipping it would drop rows out of the tables below and the tables would still print, which
    # is the failure this check exists to stop. A missing status file is itself refused: a
    # reduction whose completeness cannot be established is not a reduction.
    status = os.path.join(d, "exit_codes.txt")
    bad = []
    if os.path.exists(status):
        with open(status) as fh:
            lines = [l.split() for l in fh.read().split("\n") if l.strip()]
        if not lines:
            print(f"{status} IS EMPTY - refusing: zero blocks recorded is not a clean run",
                  file=sys.stderr)
            return 2
        for f in lines:
            kv = dict(x.split("=", 1) for x in f[1:] if "=" in x)
            if kv.get("rc") != "0" or kv.get("summary") != "yes":
                bad.append(" ".join(f))
    else:
        print(f"NO {status} - this run predates the completeness record, or the driver did not "
              f"finish. Every run file is checked for a SUMMARY instead.", file=sys.stderr)

    runs = {}
    truncated = []
    for p in sorted(glob.glob(os.path.join(d, "*.txt"))):
        b = os.path.basename(p)[:-4]
        if b in ("lock", "exit_codes"):
            continue
        r = rows(p)
        if r:
            runs[b] = r
        else:
            truncated.append(b)
    if bad or truncated:
        for f in bad:
            print(f"BLOCK DID NOT FINISH: {f}", file=sys.stderr)
        for b in truncated:
            print(f"RUN FILE HAS NO SUMMARY (truncated or killed): {b}.txt", file=sys.stderr)
        print("REFUSING TO REDUCE. A truncated evidence file reads exactly like a complete one; "
              "reducing it would drop rows and still print a table.", file=sys.stderr)
        return 3
    if not runs:
        print("NO RUN FILES WITH A SUMMARY TABLE - refusing to report", file=sys.stderr)
        return 2
    keys = sorted({k for r in runs.values() for k in r})

    # ---- THE CLAIM, AS INTEGERS ------------------------------------------------------------
    print("## 1. THE CLAIM AS COUNTS. Fleet load cannot move these.")
    print("##")
    print("## reap/acq is how many branches ONE acquisition of the per-statement lock reaped.")
    print("## Before, ONE acquisition holds the whole sweep, so the ratio rises linearly with the")
    print("## number that expired. After, it is bounded by REAP_CHUNK whatever expires.")
    print("## LeaseThread::start takes the lock once more for resume_interrupted_reaps, before any")
    print("## client can connect, and that hold is counted too -- so before reads acqs=2 at every")
    print("## K (ratio K/2) and after reads acqs=1+ceil(K/REAP_CHUNK). The SHAPE is the result.")
    print("## done_in_sweep is how many client statements COMPLETED while the sweep was running.")
    print()
    print(f"{'N':>8} {'K':>6} {'arm':>7} {'before acqs':>12} {'after acqs':>11} "
          f"{'before r/acq':>13} {'after r/acq':>12} {'before done':>12} {'after done':>11}")
    for (n, arm, k) in keys:
        if arm != "locked":
            continue
        bef = [runs[l][(n, arm, k)] for l in runs if "before" in l and (n, arm, k) in runs[l]]
        aft = [runs[l][(n, arm, k)] for l in runs if "after" in l and (n, arm, k) in runs[l]]
        if not bef or not aft:
            continue
        print(f"{n:>8} {k:>6} {arm:>7} "
              f"{max(r['acqs'] for r in bef):>12} {max(r['acqs'] for r in aft):>11} "
              f"{max(r['per_acq'] for r in bef):>13.2f} {max(r['per_acq'] for r in aft):>12.2f} "
              f"{max(r['done'] for r in bef):>12} {max(r['done'] for r in aft):>11}")

    # ---- every cell -------------------------------------------------------------------------
    print("\n## 2. EVERY INVOCATION, EVERY CELL. Latency in MICROSECONDS, sweep wall in ms.\n")
    print(f"{'run':<24} {'N':>7} {'arm':>7} {'K':>6} {'samples':>9} {'p50_us':>12} "
          f"{'p99_us':>12} {'max_us':>12} {'wall_ms':>10} {'reaped':>7} {'acqs':>5} "
          f"{'r/acq':>7} {'done_in_sw':>11}")
    for label in sorted(runs):
        for k in keys:
            if k not in runs[label]:
                continue
            v = runs[label][k]
            pa = "-" if v["per_acq"] is None else f"{v['per_acq']:.2f}"
            print(f"{label:<24} {k[0]:>7} {k[1]:>7} {k[2]:>6} {v['samples']:>9} "
                  f"{us(v['p50']):>12} {us(v['p99']):>12} {us(v['max']):>12} "
                  f"{v['wall']/1e6:>10,.1f} {v['reaped']:>7} {v['acqs']:>5} {pa:>7} "
                  f"{v['done']:>11}")

    # ---- the latency headline ---------------------------------------------------------------
    print("\n## 3. CLIENT LATENCY, `locked` arm (the production wiring). UPPER BOUNDS -- see the")
    print("## load_at_acquire stamps in lock.txt. BEST before against WORST after, so the factor")
    print("## is the smallest the data supports. `control` is the `free` arm: same N, same K, the")
    print("## same sweep work under a PRIVATE lock. `floor` is `idle`: no sweep at all.\n")
    print(f"{'N':>7} {'K':>6} {'before p50':>12} {'after p50':>12} {'factor':>9} "
          f"{'before p99':>12} {'after p99':>12} {'factor':>9} {'control p99':>12} {'floor p99':>11}")
    for (n, arm, k) in keys:
        if arm != "locked":
            continue
        bef = [runs[l][(n, arm, k)] for l in runs if "before" in l and (n, arm, k) in runs[l]]
        aft = [runs[l][(n, arm, k)] for l in runs if "after" in l and (n, arm, k) in runs[l]]
        ctl = [runs[l][(n, "free", k)] for l in runs if (n, "free", k) in runs[l]]
        fl = [runs[l][(n, "idle", 0)] for l in runs if (n, "idle", 0) in runs[l]]
        if not bef or not aft:
            continue
        b50, a50 = min(r["p50"] for r in bef), max(r["p50"] for r in aft)
        b99, a99 = min(r["p99"] for r in bef), max(r["p99"] for r in aft)
        c99 = max(r["p99"] for r in ctl) if ctl else None
        f99 = max(r["p99"] for r in fl) if fl else None
        print(f"{n:>7} {k:>6} {us(b50):>12} {us(a50):>12} {b50/max(a50,1):>8.1f}x "
              f"{us(b99):>12} {us(a99):>12} {b99/max(a99,1):>8.1f}x {us(c99):>12} {us(f99):>11}")

    # ---- the slope --------------------------------------------------------------------------
    print("\n## 4. THE SLOPE. The claim is a shape, not a constant. K_prop rises with N (the agent")
    print("## workload); K_fixed=64 does not. A cost that grows with TOTAL branches would show in")
    print("## the K_fixed row; one that grows with what expired shows in K_prop.\n")
    for which in ("before", "after"):
        print(f"### {which}, locked arm")
        for metric, unit in (("per_acq", "reaps/acq"), ("p50", "us")):
            for kind in ("prop", "fixed"):
                series = []
                for (n, arm, k) in keys:
                    if arm != "locked" or ((kind == "fixed") != (k == 64)):
                        continue
                    vs = [runs[l][(n, arm, k)] for l in runs
                          if which in l and (n, arm, k) in runs[l]]
                    if vs:
                        v = max(r[metric] for r in vs)
                        series.append((n, k, v))
                if len(series) < 2:
                    continue
                series.sort()
                lo, hi = series[0][2], series[-1][2]
                span = hi / lo if lo else float("nan")
                fmt = (lambda v: f"{v:.2f}") if metric == "per_acq" else us
                desc = " -> ".join(f"N={n} K={k}: {fmt(v)}" for n, k, v in series)
                print(f"  {metric:<8} K_{kind:<5} {desc} {unit}   "
                      f"[{series[0][0]} to {series[-1][0]}: {span:.1f}x]")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "."))
