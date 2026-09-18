#!/usr/bin/env python3
"""Reduce bench/d35_c1_pagetable.txt to medians per (arm, arm-label, thread count).

Medians over the reps, not means: the design entry's own numbers are medians over 3 reps and the
machine is shared, so one arm landing next to another agent's build is a fat tail rather than a
shift in the middle. The reported quantity is the SLOPE -- throughput at 16 threads relative to 1 --
because that is what reproduced across every rep of the D35 gate while multipliers did not.
"""
import re
import sys
from statistics import median

path = sys.argv[1]
blocks = []
cur = None
for line in open(path):
    m = re.match(r"===== rep (\d+) \| (\w+) \| (.*?) =====", line)
    if m:
        cur = {"rep": int(m.group(1)), "arm": m.group(2), "label": m.group(3), "rows": [],
               "exit": None, "pos": None, "la_in": None, "la_out": None}
        blocks.append(cur)
        continue
    if cur is None:
        continue
    m = re.match(r"^# position in rep: (\d+)", line)
    if m:
        cur["pos"] = int(m.group(1))
        continue
    m = re.match(r"^# loadavg at (launch|finish): ([\d.]+)", line)
    if m:
        cur["la_in" if m.group(1) == "launch" else "la_out"] = float(m.group(2))
        continue
    if line.startswith("HARNESS_EXIT="):
        cur["exit"] = line.strip().split("=")[1]
        cur = None
        continue
    # threads wall_s fetches faults_per_s reads reads_per_fetch refused wrong
    m = re.match(r"^(\d+)\t([\d.]+)\t(\d+)\t(\d+)\t(\d+)\t([\d.]+)\t(\d+)\t(\d+)\s*$", line)
    if m:
        cur["rows"].append({
            "threads": int(m.group(1)),
            "per_s": int(m.group(4)),
            "reads": int(m.group(5)),
            "rpf": float(m.group(6)),
            "refused": int(m.group(7)),
            "wrong": int(m.group(8)),
        })

bad = [b for b in blocks if b["exit"] != "0"]
print(f"blocks={len(blocks)}  non-zero HARNESS_EXIT={len(bad)}")
if bad:
    for b in bad:
        print(f"  !! rep{b['rep']} {b['arm']} {b['label']}: HARNESS_EXIT={b['exit']}")
total_wrong = sum(r["wrong"] for b in blocks for r in b["rows"])
total_refused = sum(r["refused"] for b in blocks for r in b["rows"])
print(f"STAMP GUARD: wrong={total_wrong} across every row   refused={total_refused}")
if not blocks:
    sys.exit("no blocks parsed - the artifact or this parser is broken")

labels = []
for b in blocks:
    if b["label"] not in labels:
        labels.append(b["label"])

for label in labels:
    print()
    print(f"### {label}")
    arms = []
    for b in blocks:
        if b["label"] == label and b["arm"] not in arms:
            arms.append(b["arm"])
    tcounts = sorted({r["threads"] for b in blocks if b["label"] == label for r in b["rows"]})
    print("arm   " + "".join(f"{t:>14}T" for t in tcounts) + f"{'16T/1T':>12}")
    meds = {}
    for arm in arms:
        row = []
        for t in tcounts:
            vals = [r["per_s"] for b in blocks if b["label"] == label and b["arm"] == arm
                    for r in b["rows"] if r["threads"] == t]
            row.append(median(vals) if vals else float("nan"))
        meds[arm] = row
        slope = row[-1] / row[0] if row[0] else float("nan")
        print(f"{arm:<6}" + "".join(f"{v:>15,.0f}" for v in row) + f"{slope:>11.3f}x")
    if len(arms) == 2:
        a, b_ = arms
        print("ratio " + "".join(f"{meds[b_][i]/meds[a][i]:>14.2f}x" for i in range(len(tcounts))))
    # reads_per_fetch, the only window this harness has onto hit rate.
    for arm in arms:
        rpf = []
        for t in tcounts:
            vals = [r["rpf"] for b in blocks if b["label"] == label and b["arm"] == arm
                    for r in b["rows"] if r["threads"] == t]
            rpf.append(median(vals) if vals else float("nan"))
        print(f"  rpf {arm:<5}" + "".join(f"{v:>14.3f}" for v in rpf))
    # Per-rep slopes: the sign has to hold in EVERY rep, not only in the median.
    for arm in arms:
        per_rep = []
        for b in blocks:
            if b["label"] != label or b["arm"] != arm:
                continue
            one = [r["per_s"] for r in b["rows"] if r["threads"] == tcounts[0]]
            sixteen = [r["per_s"] for r in b["rows"] if r["threads"] == tcounts[-1]]
            if one and sixteen:
                per_rep.append(sixteen[0] / one[0])
        print(f"  per-rep {tcounts[-1]}T/{tcounts[0]}T {arm:<5}" +
              "".join(f"{v:>10.3f}x" for v in per_rep))

    # ── Was the load drifting, and did the rotation cancel it? ────────────────────────────────
    #
    # A fixed arm order under a monotonic drift becomes a systematic POSITION bias -- which is how
    # the first run of this factorial was wrong. Two diagnostics, printed rather than assumed:
    #   1. the loadavg range across the run, so drift is visible at all;
    #   2. 1-thread throughput by POSITION, pooled over arms. Under a Latin square each position
    #      holds each arm once, so a position effect here is drift and not the arms.
    las = [b["la_in"] for b in blocks if b["label"] == label and b["la_in"] is not None]
    if las:
        print(f"  loadavg at launch: min {min(las):.2f} max {max(las):.2f} "
              f"first {las[0]:.2f} last {las[-1]:.2f}")
    positions = sorted({b["pos"] for b in blocks if b["label"] == label and b["pos"]})
    if positions:
        print("  1T throughput by POSITION (pooled over arms; a Latin square should flatten this):")
        for pos in positions:
            vals = [r["per_s"] for b in blocks if b["label"] == label and b["pos"] == pos
                    for r in b["rows"] if r["threads"] == tcounts[0]]
            arms_at = sorted({b["arm"] for b in blocks if b["label"] == label and b["pos"] == pos})
            if vals:
                print(f"    pos {pos}: median {median(vals):>15,.0f}  (arms: {','.join(arms_at)})")
        if len({len({b['arm'] for b in blocks if b['label'] == label and b['pos'] == p})
                for p in positions}) == 1 and len(positions) == len(arms):
            print("    -> every position held every arm: the square is balanced")
        else:
            print("    -> UNBALANCED: positions do not each hold every arm, so drift is NOT cancelled")
