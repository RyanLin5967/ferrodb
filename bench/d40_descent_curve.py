#!/usr/bin/env python3
"""Measure the D40 catalog-descent curve on BOTH call graphs, with one instrument.

The AFTER run is the shipped code. The BEFORE run restores the PRE-D40 call graph and nothing
else: `drain_pending` ends in the global scan again, and `reap_expired` runs it unconditionally
instead of on a cadence. `TwoTierReaper::sweep_descents` stays exactly where it is, so both curves
are counted at the same site, by the same counter, in the same build profile.

Restoring the call graph rather than checking out the old file is deliberate: the old file has no
counter in it, so measuring it would need the counter added back by hand, which is one more thing
that can differ between the two arms. Here the only difference is the two call sites named below.

Usage:  python3 bench/d40_descent_curve.py [N ...]
Restore is `git checkout <SHA> --`, never `git checkout --`: the latter restores the INDEX.
"""
import subprocess, sys, os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REAPER = os.path.join(REPO, "src/branch/reaper.rs")
POINTS = sys.argv[1:] or ["125", "250", "500", "1000", "2016"]

# (what it restores, anchor, replacement)
PRE_D40 = [
    ("drain_pending ends in the global scan",
     "        self.sweep_touched_extents(&touched)?;",
     "        let _ = &touched;\n        self.collect_orphaned_extents()?;"),
    ("reap_expired scans unconditionally",
     "        self.collect_orphans_if_due(now_millis)?;",
     "        self.collect_orphaned_extents()?;"),
]


def run(cmd):
    return subprocess.run(cmd, cwd=REPO, shell=True, capture_output=True, text=True)


def sha():
    return run("git rev-parse HEAD").stdout.strip()


def clean():
    return run("git diff HEAD --stat -- src/").stdout.strip() == ""


def measure(label):
    b = run("timeout 1800 cargo build --example d40_descent_curve 2>&1")
    if "error" in (b.stdout + b.stderr):
        print(b.stdout + b.stderr, file=sys.stderr)
        sys.exit(2)
    r = run(f"timeout 3600 ./target/debug/examples/d40_descent_curve {' '.join(POINTS)}")
    if r.returncode != 0:
        # A run that collected nothing has not passed. Say which, and exit non-zero.
        print(f"FATAL: {label} run exited {r.returncode}\n{r.stdout}\n{r.stderr}", file=sys.stderr)
        sys.exit(2)
    print(f"==== {label} ====")
    print(r.stdout.rstrip())
    return r.stdout


HEAD = sha()
if not clean():
    print("FATAL: src/ is dirty; commit before measuring", file=sys.stderr)
    sys.exit(2)

after = measure("AFTER — shipped D40 call graph")

for what, old, new in PRE_D40:
    src = open(REAPER).read()
    if src.count(old) != 1:
        print(f"FATAL: {what}: anchor matched {src.count(old)} times, expected 1", file=sys.stderr)
        run(f"git checkout {HEAD} -- src/branch/reaper.rs")
        sys.exit(2)
    open(REAPER, "w").write(src.replace(old, new))
print(f"\n(restored the pre-D40 call graph: {'; '.join(w for w, _, _ in PRE_D40)})\n")

try:
    before = measure("BEFORE — pre-D40 call graph, same counter, same build")
finally:
    run(f"git checkout {HEAD} -- src/branch/reaper.rs")
    run("timeout 1800 cargo build --example d40_descent_curve")
    if not clean():
        print("FATAL: tree did not restore", file=sys.stderr)
        sys.exit(2)
    print("\n(tree restored to HEAD and rebuilt)")
