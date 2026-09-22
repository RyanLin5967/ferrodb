#!/usr/bin/env python3
"""D138 — fit the PARK axis slope from each arm's RAW file, at every row, not at the endpoints.

D137's law is `scan = parked_sessions + C`, and its force is that it holds at ALL EIGHT sampled
rows with an INTEGER slope of 1 — an endpoint ratio would be satisfied by curves that are not
lines. D138's acceptance test is that the same fit over the indexed arm returns slope 0, and it
names the specific failure it exists to catch: **a slope near 0.5 means one of the three keyed
sites was missed**, because the axis would then be paying the scan on some appends and not others.

So this refuses on anything that is not a clean integer-slope line:
  * fewer than 8 rows                -> the axis did not complete
  * consecutive differences that are not all equal -> not a line; report it rather than fit it
  * a slope that is neither 1 nor 0  -> print it and FAIL, naming the 0.5 case explicitly

The expected values are D137's, written here; nothing is read back out of the code under test.
"""
import re, sys, pathlib

ROW = re.compile(r"^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\d+)\s*$")

def park_rows(path):
    """(sessions, scan_hit, scan_miss, shadow_hit, shadow_miss, hits, misses, log_len) per row."""
    text = pathlib.Path(path).read_text().splitlines()
    try:
        start = next(i for i, l in enumerate(text) if "AXIS: PARK" in l)
    except StopIteration:
        sys.exit(f"REFUSING — {path} has no PARK axis. A run that collected nothing has not passed.")
    out = []
    for line in text[start:]:
        if line.startswith("    final log length"):
            break
        m = ROW.match(line)
        if not m:
            continue
        g = m.groups()
        num = lambda s: None if s == "-" else float(s)
        out.append(dict(sessions=int(g[0]), log_len=int(g[1]), hits=int(g[2]), misses=int(g[3]),
                        scan_hit=num(g[4]), scan_miss=num(g[5]),
                        shadow_hit=num(g[6]), shadow_miss=num(g[7]), scanned=int(g[8])))
    return out

def slope(rows, key):
    """The slope per SESSION, only if every consecutive difference agrees. Else None + the diffs."""
    ys = [r[key] for r in rows]
    if any(y is None for y in ys):
        return None, None, "not recorded in this arm"
    xs = [r["sessions"] for r in rows]
    steps = [(ys[i+1] - ys[i]) / (xs[i+1] - xs[i]) for i in range(len(xs) - 1)]
    if max(steps) - min(steps) > 1e-9:
        return None, steps, "NOT A LINE — consecutive slopes disagree"
    return steps[0], steps, "line"

def report(path, label):
    rows = park_rows(path)
    print(f"--- {label}: {path}")
    if len(rows) != 8:
        sys.exit(f"REFUSING — {label} completed {len(rows)} PARK rows, not 8. Not a result.")
    # The workload must be the SAME in both arms or the slopes are not comparable.
    print(f"    rows=8  log_len {rows[0]['log_len']} -> {rows[-1]['log_len']}  "
          f"calls/block {rows[0]['hits']} hit / {rows[0]['misses']} miss")
    res = {}
    for key in ("scan_hit", "scan_miss", "shadow_hit", "shadow_miss"):
        s, steps, why = slope(rows, key)
        res[key] = s
        if s is None:
            print(f"    {key:<12} slope  -       ({why})")
        else:
            print(f"    {key:<12} slope {s:+.4f}  intercept {rows[0][key] - s*rows[0]['sessions']:.1f}  ({why}, all 7 steps equal)")
    return rows, res

def main():
    here = pathlib.Path(__file__).parent
    print("D138 — PARK axis slope, fitted at every row from the raw artifacts.\n")
    _, old = report(here / "d138_old_raw.txt", "arm OLD (unindexed, main 29ad1f5)")
    print()
    new_rows, new = report(here / "d138_new_raw.txt", "arm NEW (indexed, dfe2c70)")
    print()

    fails = []
    # 1. The baseline must reproduce D137, or this comparison has no base.
    for k in ("scan_hit", "scan_miss"):
        if old[k] != 1.0:
            fails.append(f"arm OLD {k} slope is {old[k]}, not D137's 1. The baseline did not reproduce.")
    # 2. The acceptance test.
    for k in ("scan_hit", "scan_miss"):
        s = new[k]
        if s is None:
            fails.append(f"arm NEW {k} was not recorded at all — that is no result, not a zero.")
        elif abs(s) > 1e-9:
            extra = "  <-- ~0.5 is the 'one of the three sites was missed' case" if 0.3 < s < 0.7 else ""
            fails.append(f"arm NEW {k} slope is {s}, not 0.{extra}")
    # 3. The scan was removed, not the workload. Same calls, same log growth, in both arms.
    o_rows = park_rows(here / "d138_old_raw.txt")
    for i, (a, b) in enumerate(zip(o_rows, new_rows)):
        for k in ("sessions", "log_len", "hits", "misses"):
            if a[k] != b[k]:
                fails.append(f"row {i}: {k} differs between arms ({a[k]} vs {b[k]}) — "
                             "different workloads cannot be compared.")
    # 4. The removed scan must still be VISIBLE in the indexed arm, at slope 1.
    for k in ("shadow_hit", "shadow_miss"):
        if new[k] != 1.0:
            fails.append(f"arm NEW {k} slope is {new[k]}, not 1 — the shadow of the removed scan "
                         "is what makes the zero beside it readable.")

    print("VERDICT")
    print(f"  scan slope 1 -> 0 : {old['scan_hit']:+.4f} -> {new['scan_hit']:+.4f} (hit), "
          f"{old['scan_miss']:+.4f} -> {new['scan_miss']:+.4f} (miss)")
    print(f"  shadow of the removed scan, inside the indexed binary: {new['shadow_hit']:+.4f} (hit), "
          f"{new['shadow_miss']:+.4f} (miss)")
    if fails:
        print("\n".join("  FAIL: " + f for f in fails))
        sys.exit(1)
    print("  PASS — slope 1 -> 0 at all 8 rows, identical workload, removed scan still slope 1.")

main()
