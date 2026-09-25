#!/usr/bin/env python3
"""bench/d42_childcpu_probe.py — measure the HEALTHY FLOOR of BOTH of D42's signals.

D42's classifier decides whether a 45 s timeout in `tests/integration_consensus_failover.rs`
means "the cluster failed" or "we were descheduled". It reads two signals: the poll loop's own
iteration count, and the node child processes' CPU consumed per wall second. Both need a
threshold, and a threshold picked after seeing the falsifier runs would be tuning, not
calibration. So this probe measures both numbers FIRST, in one window, independently of either
falsifier, and the constants in the test cite this file.

WHAT IT MEASURES AND WHY THAT IS THE RIGHT QUANTITY. It stands up the same three-process cluster
the test does, waits for a leader, and then lets the cluster sit IDLE for the length of one
ELECTION_BUDGET while sampling each node's CPU time. An idle cluster with a settled leader is the
LOWER BOUND on healthy child CPU: a cluster that is genuinely broken (no quorum, repeated election
timeouts) burns strictly more than one that is quietly heartbeating, and a starved one burns less.
So a threshold set a decade below this floor cannot be reached by a healthy-but-failing cluster,
which is exactly the direction falsifier 2 tests.

The nodes block in `recv_timeout(min(5ms, until_tick))` (examples/consensus_node.rs:140,
Node::poll in src/consensus/node.rs), so ~200 wakeups/s/node is the duty cycle being measured. It
is not zero, and it is not a spin.

During the same window the probe's own thread runs the identical 10 ms poll loop `wait_for` runs,
so the self-scheduling floor is measured against the same machine state as the child floor rather
than against a different minute.

Usage: bench/d42_childcpu_probe.py [seconds]      (default 45, matching ELECTION_BUDGET)
"""
import os
import re
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BIN = os.path.join(ROOT, "target", "debug", "examples", "consensus_node")
IDS = [1, 2, 3]


def cpu_seconds(pids):
    """Total CPU seconds per pid, from `ps -o pid=,time=`.

    `-e` is deliberately absent: `ps -eo ... -p PID` ignores the pid filter entirely and dumps
    every process on the machine, which would silently sum the whole box into this measurement.
    """
    if not pids:
        return {}
    out = subprocess.run(
        ["ps", "-o", "pid=,time=", "-p", ",".join(str(p) for p in pids)],
        capture_output=True, text=True,
    )
    got = {}
    for line in out.stdout.splitlines():
        m = re.match(r"\s*(\d+)\s+(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)\s*$", line)
        if m:
            h, mi, s = m.group(2), m.group(3), m.group(4)
            got[int(m.group(1))] = (int(h or 0) * 3600) + (int(mi) * 60) + float(s)
    return got


def main():
    window = float(sys.argv[1]) if len(sys.argv) > 1 else 45.0
    if not os.path.exists(BIN):
        sys.exit(f"REFUSING — {BIN} is missing; run: cargo build --examples")

    root = tempfile.mkdtemp(prefix="d42probe-")
    procs, lines, lock = {}, [], threading.Lock()

    def reader(nid, stdout):
        for ln in stdout:
            with lock:
                lines.append((nid, ln.rstrip("\n")))

    for i in IDS:
        d = os.path.join(root, f"n{i}")
        os.makedirs(d, exist_ok=True)
        p = subprocess.Popen([BIN, str(i), d], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        procs[i] = p
        threading.Thread(target=reader, args=(i, p.stdout), daemon=True).start()

    addrs = {}
    t0 = time.time()
    while len(addrs) < 3 and time.time() - t0 < 30:
        with lock:
            for nid, ln in lines:
                if ln.startswith("READY "):
                    addrs[nid] = ln[len("READY "):]
        time.sleep(0.05)
    if len(addrs) < 3:
        for p in procs.values():
            p.kill()
        sys.exit(f"REFUSING — only {len(addrs)}/3 nodes said READY; the probe measured nothing.")

    members = ",".join(str(i) for i in IDS)
    for i in IDS:
        peers = ",".join(f"{j}={addrs[j]}" for j in IDS if j != i)
        procs[i].stdin.write(f"START {members} {peers}\n")
        procs[i].stdin.flush()

    # A leader must exist before the idle window starts; measuring across the election would
    # report the election's cost as the idle floor and inflate the threshold.
    t0 = time.time()
    leader = None
    while leader is None and time.time() - t0 < 45:
        with lock:
            for nid, ln in lines:
                if ln.startswith("ROLE Leader "):
                    leader = nid
        time.sleep(0.05)
    if leader is None:
        for p in procs.values():
            p.kill()
        sys.exit("REFUSING — no leader within 45s; there is no healthy floor to measure.")
    elect_s = time.time() - t0

    pids = [p.pid for p in procs.values()]
    before = cpu_seconds(pids)
    w0 = time.time()
    # The same 10 ms poll loop `wait_for` runs, so signal B's floor is measured on this window.
    poll, iters, max_stall, last = 0.010, 0, 0.0, time.time()
    while time.time() - w0 < window:
        time.sleep(poll)
        iters += 1
        now = time.time()
        max_stall = max(max_stall, now - last)
        last = now
    after = cpu_seconds(pids)
    wall = time.time() - w0
    nominal = window / poll
    self_ratio = iters / nominal

    missing = [p for p in pids if p not in before or p not in after]
    for p in procs.values():
        p.kill()
    if missing:
        sys.exit(f"REFUSING — ps lost pids {missing} during the window; the sum would be short.")

    try:
        load = os.getloadavg()
    except OSError:
        load = (float("nan"),) * 3

    deltas = {p: after[p] - before[p] for p in pids}
    total = sum(deltas.values())
    per_node = total / (len(pids) * wall)

    print("D42 healthy-floor probe — idle 3-node cluster, leader settled")
    print(f"  binary          : {BIN}")
    print(f"  leader elected  : n{leader} after {elect_s:.2f}s")
    print(f"  window          : {wall:.2f}s wall")
    print(f"  loadavg (1/5/15): {load[0]:.2f} / {load[1]:.2f} / {load[2]:.2f}")
    print("")
    print("  -- signal B: this thread's own scheduling, 10ms poll loop --")
    print(f"  iterations      : {iters} of {nominal:.0f} nominal = {self_ratio:.4f}")
    print(f"  max stall       : {max_stall*1000:.1f}ms between consecutive polls")
    print(f"  a decade below  : {self_ratio/10:.4f}")
    print("")
    print("  -- signal C: the node child processes' CPU --")
    for i in IDS:
        print(f"  n{i} pid {procs[i].pid:<6} cpu delta {deltas[procs[i].pid]:.3f}s "
              f"= {deltas[procs[i].pid]/wall:.4f} cpu-s per wall-s")
    print(f"  TOTAL           : {total:.3f} cpu-s over {wall:.2f}s wall across {len(pids)} nodes")
    print(f"  FLOOR           : {per_node:.4f} cpu-s per node-wall-s")
    print(f"  a decade below  : {per_node/10:.5f}")


if __name__ == "__main__":
    main()
