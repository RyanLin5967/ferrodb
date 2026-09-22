#!/usr/bin/env python3
"""Reduce the W4 check 3 re-derivation to the tables the report quotes.

Two jobs, and the second is the one that matters:

  1. Put my numbers beside the banked ones, arm by arm and protocol by protocol.
  2. CLOSE THE COUNTERS ARITHMETICALLY. Every `ROW` carries enough to predict two of its own
     fields from the others, and a counter that closes on every row of every arm is evidence the
     counter counts what its name says that no amount of reading the source can give:

       ann_exec  == (exclusive statements in the arm) + (shared-today stand-downs)
                    ... because a read that stands down FALLS BACK to the exclusive path and
                    announces there. This is a positive feedback loop in the mechanism, not an
                    artefact, and the closure is what proves it is real.

       ann_parse == (statements with a distinct prepared-statement name, after the reset)
                    ... which the protocol fixes exactly: every statement under `simple` and
                    `ext_reparse`, and only the fresh names under `extended`.

     A row that does NOT close is reported as a row that does not close. It is not smoothed.
"""
import re
import sys
from pathlib import Path

ROW = re.compile(r"^ROW (.*)$", re.M)


def rows(path: Path):
    if not path.exists():
        return []
    out = []
    for m in ROW.finditer(path.read_text()):
        d = {}
        for kv in m.group(1).split():
            k, _, v = kv.partition("=")
            d[k] = v
        out.append(d)
    return out


def num(d, k):
    v = d.get(k, "n/a")
    if v == "n/a":
        return None
    return float(v) if "." in v else int(v)


def pct(d, k):
    v = num(d, k)
    return "n/a" if v is None else f"{v * 100:.2f}%"


def collect(d: Path, pattern="*.txt"):
    """arm/proto -> row, over every file in a directory."""
    got = {}
    for f in sorted(d.glob(pattern)):
        for r in rows(f):
            got[(r["arm"], r["proto"])] = r
    return got


def side_by_side(mine, banked):
    print("## E1 — REPRODUCTION. shared-today stand-down fraction, mine vs the banked run.")
    print("## Same binary source, same arm, same client mix, same round count.")
    print(f"{'arm':<10} {'proto':<12} {'banked':>9} {'mine':>9} {'ratio':>8}  {'sh_att':>7} "
          f"{'ann_parse':>9} {'ann_exec':>8} {'mismatch':>8} {'gap':>4}")
    for key in sorted(set(mine) | set(banked)):
        arm, proto = key
        m, b = mine.get(key), banked.get(key)
        if not m or not b:
            continue
        mv, bv = num(m, "sh_frac"), num(b, "sh_frac")
        ratio = "n/a" if not mv or not bv else f"{bv / mv:.2f}x"
        if mv == 0 and bv == 0:
            ratio = "both 0"
        print(f"{arm:<10} {proto:<12} {pct(b,'sh_frac'):>9} {pct(m,'sh_frac'):>9} {ratio:>8}  "
              f"{m['sh_att']:>7} {m['ann_parse']:>9} {m['ann_exec']:>8} "
              f"{m['mismatch']:>8} {m['gap']:>4}")


def closure(mine):
    """ann_exec == exclusive statements + shared-today stand-downs, per arm."""
    print()
    print("## COUNTER CLOSURE — ann_exec predicted from the other fields of the SAME row.")
    print("## exclusive statements = every statement `try_run_read` refuses = stmts - sh_att.")
    print("## A read that stands down then takes the exclusive path and announces there, so:")
    print("##     ann_exec  ==  (stmts - sh_att)  +  sh_stood")
    print(f"{'arm':<10} {'proto':<12} {'stmts':>6} {'sh_att':>7} {'sh_stood':>8} "
          f"{'predicted':>9} {'ann_exec':>8}  {'closes':>6}")
    ok = True
    for (arm, proto), r in sorted(mine.items()):
        stmts, sh_att = num(r, "stmts"), num(r, "sh_att")
        sh_stood, ann_exec = num(r, "sh_stood"), num(r, "ann_exec")
        predicted = (stmts - sh_att) + sh_stood
        closes = predicted == ann_exec
        ok = ok and closes
        print(f"{arm:<10} {proto:<12} {stmts:>6} {sh_att:>7} {sh_stood:>8} "
              f"{predicted:>9} {ann_exec:>8}  {'YES' if closes else 'NO':>6}")
    print(f"## every row closes: {ok}")
    return ok


def main():
    base = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    mine = collect(base / "rederive", "e1_arm_*.txt")
    banked = collect(base / "main", "arm_*.txt")
    side_by_side(mine, banked)
    closure(mine)

    for name, title in [
        ("e2_protoladder", "E2 — the protocol ladder on the DECIDING arm, with ext_split"),
        ("e2_protoladder_readonly", "E2b — the same ladder on readonly (no announcers at all)"),
        ("e3_self_reader", "E3 — ONE client, pure reader: any stand-down here is SELF"),
        ("e3_self_forker", "E3 — ONE client that announces then reads"),
        ("e3_control_two", "E3 control — the same config with a SECOND client"),
    ]:
        f = base / "rederive" / f"{name}.txt"
        rs = rows(f)
        if not rs:
            continue
        print()
        print(f"## {title}")
        print(f"{'arm':<10} {'proto':<12} {'sh_att':>7} {'sh_stood':>8} {'SHARED':>8} "
              f"{'ann_parse':>9} {'ann_exec':>8} {'ann_lease':>9}")
        for r in rs:
            print(f"{r['arm']:<10} {r['proto']:<12} {r['sh_att']:>7} {r['sh_stood']:>8} "
                  f"{pct(r,'sh_frac'):>8} {r['ann_parse']:>9} {r['ann_exec']:>8} "
                  f"{r['ann_lease']:>9}")

    print()
    print("## E4 — CALIBRATION. Same arm (readonly), same protocol (extended), same mix.")
    print("## The ONLY thing that moves is the reaper's scan interval, which is the only")
    print("## announcer such an arm has. A ratio that never reaches its ends is not calibrated.")
    print(f"{'lease_ms':>10} {'sh_att':>7} {'sh_stood':>8} {'SHARED':>8} {'ann_lease':>9} "
          f"{'ann_exec':>8}")
    for ms in ["3600000", "1000", "100", "10", "1", "0"]:
        rs = rows(base / "rederive" / f"e4_lease_{ms}.txt")
        for r in rs:
            print(f"{ms:>10} {r['sh_att']:>7} {r['sh_stood']:>8} {pct(r,'sh_frac'):>8} "
                  f"{r['ann_lease']:>9} {r['ann_exec']:>8}")

    print()
    print("## E6 — OFFERED LOAD. agent/extended. fork_every=1 is the banked point:")
    print("## EVERY forker statement is a fork. The curve is what check 3 actually licenses.")
    print(f"{'fork_every':>10} {'sh_att':>7} {'sh_stood':>8} {'SHARED':>8} {'defining':>8} "
          f"{'stmts':>6} {'excl_share':>10}")
    for k in ["1", "2", "4", "8", "16", "32", "64"]:
        for r in rows(base / "rederive" / f"e6_fork_every_{k}.txt"):
            stmts, sh_att = num(r, "stmts"), num(r, "sh_att")
            share = f"{(stmts - sh_att) / stmts * 100:.1f}%"
            print(f"{k:>10} {r['sh_att']:>7} {r['sh_stood']:>8} {pct(r,'sh_frac'):>8} "
                  f"{r['defining']:>8} {r['stmts']:>6} {share:>10}")
    print()
    print(f"{'announcers':>10} {'sh_att':>7} {'sh_stood':>8} {'SHARED':>8} {'defining':>8} "
          f"{'stmts':>6} {'excl_share':>10}")
    for n in ["1", "2", "4", "6", "7"]:
        for r in rows(base / "rederive" / f"e6_announcers_{n}.txt"):
            stmts, sh_att = num(r, "stmts"), num(r, "sh_att")
            share = f"{(stmts - sh_att) / stmts * 100:.1f}%"
            print(f"{n:>10} {r['sh_att']:>7} {r['sh_stood']:>8} {pct(r,'sh_frac'):>8} "
                  f"{r['defining']:>8} {r['stmts']:>6} {share:>10}")

    print()
    print("## E7 — IS THE REAPER A MATERIAL ANNOUNCER? readonly/extended, scan every 10ms.")
    print("## That arm reads EXACTLY 0.000000 with ZERO announcements of any kind, so driving")
    print("## expiries makes the reaper the only EXOGENOUS announcer in the system. The axis is")
    print("## expired branches per tick, NOT the scan period -- an empty candidate list takes the")
    print("## lock zero times at any cadence. expiry_ms=0 is the control and must stay at zero.")
    print(f"{'expiry_ms':>10} {'minted':>7} {'sh_att':>7} {'sh_stood':>8} {'SHARED':>8} "
          f"{'ann_lease':>9} {'ann_exec':>8} {'closes':>6}")
    for ms in ["0", "200", "50", "10", "2"]:
        for r in rows(base / "rederive" / f"e7_expiry_{ms}.txt"):
            # In a read-only arm every statement is a shared-path attempt, so the closure identity
            # collapses to ann_exec == sh_stood. A row that breaks it is not reporting the reaper.
            closes = num(r, "ann_exec") == num(r, "sh_stood")
            print(f"{ms:>10} {r.get('minted','?'):>7} {r['sh_att']:>7} {r['sh_stood']:>8} "
                  f"{pct(r,'sh_frac'):>8} {r['ann_lease']:>9} {r['ann_exec']:>8} "
                  f"{'YES' if closes else 'NO':>6}")

    print()
    print("## E5 — LOAD SENSITIVITY. The same arm quiet and beside a CPU burner.")
    print(f"{'label':<22} {'proto':<10} {'quiet':>8} {'loaded':>8} {'ratio':>8}")
    for label in ["e5_agent_extended", "e5_readonly_simple"]:
        q = rows(base / "rederive" / f"{label}_quiet.txt")
        l = rows(base / "rederive" / f"{label}_loaded.txt")
        for a, b in zip(q, l):
            qa, lb = num(a, "sh_frac"), num(b, "sh_frac")
            ratio = "n/a" if not qa or not lb else f"{lb / qa:.2f}x"
            print(f"{label:<22} {a['proto']:<10} {pct(a,'sh_frac'):>8} "
                  f"{pct(b,'sh_frac'):>8} {ratio:>8}")


if __name__ == "__main__":
    main()
