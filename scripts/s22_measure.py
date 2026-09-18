#!/usr/bin/env python3
"""S22 before/after measurement, both arms, interleaved.

# Why two binaries rather than two checkouts

The BEFORE and AFTER arms must run the SAME harness, or the comparison is between two
instruments as much as between two buffer pools. So this builds the harness twice from one
tree -- once with src/buffer/{buffer_pool.rs,arc.rs} as they are, once with those two files
reverted to the pre-S22 commit -- and keeps both binaries. Everything else in the tree,
including the benchmark source, is byte-identical between them.

# Why interleaved

This machine is shared with other agents and has been running at a load average around 30
on 18 cores. Running all the BEFORE points and then all the AFTER points would confound the
change with whatever else started in between. Each arm is run BEFORE-then-AFTER back to
back, and the whole sweep is repeated, so a load excursion hits both sides.

# What each arm is for

RESIDENT       working set fits the pool, every fetch is a HIT, no IO at all. Measures the
               hit path, where the only shared thing left is the pool's own bookkeeping.
OVERSUBSCRIBED working set exceeds the pool, every fetch is a page FAULT. The modelled-IO
               variant is the primary instrument: a fixed sleep per read makes serialisation
               arithmetic rather than inference, and sleeping threads do not compete for CPU,
               so it is the arm least distorted by the machine's load.
"""
import datetime
import os
import pathlib
import shutil
import subprocess
import sys

# Default to the worktree this script lives in, NOT a hard-coded path: these were
# written in a private worktree and a baked-in absolute path would silently mutate or
# benchmark somebody else's tree.
ROOT = pathlib.Path(os.environ.get("S22_ROOT") or subprocess.run(
    ["git", "rev-parse", "--show-toplevel"], cwd=pathlib.Path(__file__).resolve().parent,
    capture_output=True, text=True, check=True).stdout.strip())
BIN_SRC = ROOT / "target/release/examples/bufpool_fault_concurrency"
BINS = pathlib.Path("/tmp/s22_bins")
# The commit whose buffer pool still holds arc_cache across DiskManager::read.
BEFORE_REV = "cb12805"
POOL_FILES = ["src/buffer/buffer_pool.rs", "src/buffer/arc.rs"]

ARMS = [
    ("RESIDENT (hit path; working set 512 pages fits the 1024-frame pool)",
     ["--resident", "200000", "1,2,4,8,16", "0", "3"]),
    ("OVERSUBSCRIBED, MODELLED IO 500us per read (primary instrument)",
     ["500", "1,2,4,8,16", "500", "1"]),
    ("OVERSUBSCRIBED, REAL FILE warm OS page cache (corroboration only)",
     ["--real", "4000", "1,2,4,8,16", "0", "40"]),
]
REPS = 2


def run(cmd, **kw):
    return subprocess.run(cmd, shell=True, cwd=ROOT, capture_output=True, text=True, **kw)


def loadavg():
    return ", ".join(f"{x:.2f}" for x in os.getloadavg())


def build(tag):
    r = run("timeout 900 cargo build --release --example bufpool_fault_concurrency")
    if r.returncode != 0:
        sys.exit(f"{tag}: build failed\n{r.stdout[-3000:]}\n{r.stderr[-3000:]}")
    BINS.mkdir(parents=True, exist_ok=True)
    dest = BINS / tag
    shutil.copy2(BIN_SRC, dest)
    return dest


def main():
    dirty = run("git status --porcelain -- " + " ".join(POOL_FILES)).stdout.strip()
    if dirty:
        sys.exit(f"REFUSING: pool sources are already modified:\n{dirty}")

    out = [
        "# S22 buffer pool: BEFORE vs AFTER, both arms, interleaved",
        f"# generated {datetime.datetime.now().astimezone().isoformat()}",
        f"# host: {os.cpu_count()} cores, load average at start: {loadavg()}",
        "#",
        "# BEFORE = src/buffer/{buffer_pool.rs,arc.rs} at " + BEFORE_REV +
        " (fetch_page holds arc_cache across DiskManager::read)",
        "# AFTER  = the same two files at HEAD " + run("git rev-parse --short HEAD").stdout.strip() +
        " (per-frame latch + in_transit marker)",
        "# Everything else, harness included, is byte-identical between the two binaries.",
        "#",
        "# NOTE ON LOAD: this machine is shared with other agents and was running at a load",
        "# average near 30 on 18 cores throughout. The modelled-IO arm is dominated by a fixed",
        "# sleep and is the arm to trust; the real-file and resident arms are CPU-bound and their",
        "# ABSOLUTE numbers are depressed by that load. Arms are interleaved so both sides see it.",
        "",
    ]

    try:
        after_bin = build("after")
        r = run(f"git checkout {BEFORE_REV} -- " + " ".join(POOL_FILES))
        if r.returncode != 0:
            sys.exit(f"could not revert pool sources: {r.stderr}")
        # Prove the revert actually landed rather than trusting the exit code.
        src = (ROOT / "src/buffer/buffer_pool.rs").read_text()
        if "in_transit" in src:
            sys.exit("REFUSING: reverted buffer_pool.rs still mentions in_transit.")
        if "arc_cache.lock().unwrap();" not in src:
            sys.exit("REFUSING: reverted buffer_pool.rs does not look like the pre-S22 pool.")
        before_bin = build("before")
    finally:
        run("git checkout HEAD -- " + " ".join(POOL_FILES))
        src = (ROOT / "src/buffer/buffer_pool.rs").read_text()
        if "in_transit" not in src:
            sys.exit("RESTORE FAILED: buffer_pool.rs is not back at HEAD.")
        print("pool sources restored to HEAD (verified)", flush=True)

    for rep in range(1, REPS + 1):
        for label, args in ARMS:
            for tag, binary in (("BEFORE", before_bin), ("AFTER", after_bin)):
                hdr = f"===== rep {rep} | {tag} | {label} ====="
                print(hdr, flush=True)
                out.append(hdr)
                out.append(f"# loadavg at launch: {loadavg()}")
                p = subprocess.run(
                    [str(binary)] + args, capture_output=True, text=True, cwd=ROOT, timeout=1800
                )
                out.append(p.stdout.rstrip())
                if p.stderr.strip():
                    out.append("# stderr:")
                    out.extend("# " + l for l in p.stderr.rstrip().splitlines())
                out.append(f"HARNESS_EXIT={p.returncode}")
                out.append("")
                print(f"  exit={p.returncode}", flush=True)

    dest = ROOT / "bench/s22_bufpool_before_after.txt"
    dest.write_text("\n".join(out) + "\n")
    print(f"wrote {dest}", flush=True)


if __name__ == "__main__":
    main()
