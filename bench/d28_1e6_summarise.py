#!/usr/bin/env python3
"""Reduce bench/d28_1e6_ab.txt to per-arm medians, and refuse rather than print a number it cannot
stand behind.

Medians over reps, not means: this box is shared and one round landing beside another agent's build
is a fat tail rather than a shift in the middle. The 10^5 pair measured the noise floor on the
CONTROL column directly -- br_all spread 1175-2830 ms on identical code paths -- and wrote down
"anything under about 2x here is load". That number is why the control is reported with its full
range rather than as a point.

Three refusals, each reading an OUTCOME rather than a word:

  1. ZERO COLLECTED. A run that produced no data row has not passed, whatever it exited with.
  2. SWITCH NOT LIVE. If arm B is not orders of magnitude slower than arm A on br_1row, the
     `Pushdown::Off` scaffold did not take and both arms ran the same code. That reports 1.00x,
     which reads exactly like an honest null -- the failure mode the 10^5 run named when it hashed
     its two binaries. The test is the RATIO and nothing else. An earlier version also required
     arm B to exceed an absolute 1000 ms, which is a 10^6 number: a fire-check at N=200 refused a
     run whose switch was demonstrably live at 212x, so the absolute term made the reduction wrong
     at every N but one. Identical code paths cannot differ 100x -- the 10^5 pair measured the
     control's own spread at 2.4x under load -- so the ratio alone separates the cases.
  2b. WRONG N. "Did this measure what I asked for" is read from the harness's own first column
     rather than from the N passed to the driver, and every row must agree on it.
  3. UNBALANCED ROTATION. If every position did not hold every arm, a monotonic load drift is not
     cancelled and the per-arm medians carry a position bias. Saying so is the point of rotating.
"""
import re
import sys
from statistics import median

COLS = ["br_1row", "br_all", "runs", "live_cnt"]

path = sys.argv[1] if len(sys.argv) > 1 else "bench/d28_1e6_ab.txt"
rows, voids, loads_in, loads_out, walls = [], [], [], [], []

for line in open(path):
    m = re.match(r"^DATA rep=(\d+) pos=(\d+) arm=(\w+) (.*)$", line.strip())
    if m:
        d = {"rep": int(m.group(1)), "pos": int(m.group(2)), "arm": m.group(3)}
        for k, v in re.findall(r"(\w+)=([\d.]+)", m.group(4)):
            d[k] = float(v)
        rows.append(d)
        continue
    m = re.match(r"^(VOID-DISK|VOID-ENOSPC|NO-ROW-EMITTED) (.*)$", line.strip())
    if m:
        voids.append(line.strip())
        continue
    m = re.match(r"^# loadavg at (launch|finish): ([\d.]+)", line)
    if m:
        (loads_in if m.group(1) == "launch" else loads_out).append(float(m.group(2)))
        continue
    m = re.match(r"^# wall: (\d+) s", line)
    if m:
        walls.append(int(m.group(1)))

print(f"data rows={len(rows)}  void/failed blocks={len(voids)}")
for v in voids:
    print(f"  !! {v}")

# ── 1. zero collected ─────────────────────────────────────────────────────────────────────────
if not rows:
    sys.exit("REFUSING: zero data rows parsed. That is a broken run, not an empty result.")

arms = sorted({r["arm"] for r in rows})
if set(arms) != {"A", "B"}:
    sys.exit(f"REFUSING: expected both arms, parsed {arms}. A one-armed A/B is not an A/B.")

med = {a: {c: median([r[c] for r in rows if r["arm"] == a and c in r]) for c in COLS} for a in arms}

# ── 2. did the scaffold actually take? ────────────────────────────────────────────────────────
ratio_1row = med["B"]["br_1row"] / med["A"]["br_1row"] if med["A"]["br_1row"] else float("inf")
if ratio_1row < 100:
    print()
    print("REFUSING: VOID-SWITCH -- arm B does not look unhinted.")
    print(f"  arm B br_1row median {med['B']['br_1row']:.3f} ms, arm A {med['A']['br_1row']:.3f} ms,"
          f" ratio {ratio_1row:.2f}x")
    print("  A B-arm in the same order as A means the Pushdown::Off scaffold did not reach the")
    print("  binary and BOTH arms ran the shipped path. That reports ~1.00x, which is")
    print("  indistinguishable from an honest null. Not a result.")
    sys.exit(2)

# ── 2b. did this measure the N it claims? ─────────────────────────────────────────────────────
ns = {r["n"] for r in rows if "n" in r}
if not ns:
    sys.exit("REFUSING: no row carries the N the harness reported, so the scale is unverified.")
if len(ns) > 1:
    sys.exit(f"REFUSING: rows disagree about N: {sorted(ns)}. One file, one scale.")
n_measured = int(next(iter(ns)))
print(f"N as the HARNESS reports it: {n_measured:,} (branches actually forked, not the argument)")

print()
print(f"{'column':<10}{'unhinted (B)':>18}{'hinted (A)':>18}{'B/A':>14}")
for c in COLS:
    r = med["B"][c] / med["A"][c] if med["A"][c] else float("inf")
    tag = ""
    if c == "br_all":
        tag = "   <- control, flat BY DESIGN"
    elif c == "live_cnt":
        tag = "   <- S20 column"
    print(f"{c:<10}{med['B'][c]:>15,.3f} ms{med['A'][c]:>15,.3f} ms{r:>13,.1f}x{tag}")

print()
print("  raw, sorted:")
for c in COLS:
    for a in ("A", "B"):
        vals = sorted(r[c] for r in rows if r["arm"] == a and c in r)
        print(f"    {c:<9} {a} " + str([round(v, 3) for v in vals]))

# ── the control's own spread, which is what says whether a small ratio means anything ─────────
ctrl = sorted(r["br_all"] for r in rows if "br_all" in r)
if len(ctrl) >= 2:
    print()
    print(f"  CONTROL SPREAD: br_all across BOTH arms {ctrl[0]:,.1f} - {ctrl[-1]:,.1f} ms "
          f"({ctrl[-1] / ctrl[0]:.2f}x on an identical code path).")
    print("  Any column moving less than that is load, not the hint.")

# ── 3. did the rotation cancel the drift? ─────────────────────────────────────────────────────
print()
if loads_in:
    print(f"  loadavg at launch: min {min(loads_in):.2f} max {max(loads_in):.2f} "
          f"first {loads_in[0]:.2f} last {loads_in[-1]:.2f}")
if loads_out:
    print(f"  loadavg at finish: min {min(loads_out):.2f} max {max(loads_out):.2f} "
          f"first {loads_out[0]:.2f} last {loads_out[-1]:.2f}")
if walls:
    print(f"  block wall time: min {min(walls)}s max {max(walls)}s total {sum(walls)}s")

positions = sorted({r["pos"] for r in rows})
print("  br_all by POSITION (pooled over arms; the rotation should flatten this):")
balanced = True
for p in positions:
    vals = [r["br_all"] for r in rows if r["pos"] == p and "br_all" in r]
    arms_at = sorted({r["arm"] for r in rows if r["pos"] == p})
    if vals:
        print(f"    pos {p}: median {median(vals):>12,.1f} ms  (arms: {','.join(arms_at)})")
    if set(arms_at) != set(arms):
        balanced = False
if balanced and len(positions) == len(arms):
    print("    -> every position held every arm: the square is balanced")
else:
    print("    -> UNBALANCED: positions do not each hold every arm, so a monotonic load drift is")
    print("       NOT cancelled and the per-arm medians carry a position bias. Re-run whole reps.")
    sys.exit(3)
