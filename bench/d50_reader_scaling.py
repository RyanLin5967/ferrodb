#!/usr/bin/env python3
"""D50-FERRODB-SNAPSHOT-COST — does ferrodb's read path scale with concurrent readers?

The question D48/D49 answered for turso, asked of ferrodb. On unmodified turso a per-statement
acquisition of one process-wide lock to open a read snapshot was the ENTIRE multi-reader scaling
wall (16T: x0.327 shared, x4.130 with the acquisition ablated away). ferrodb's analogue is
documented in its own source:

    src/pgwire/mod.rs:64    pub catalog: Mutex<Catalog>
    src/pgwire/mod.rs:100   "the catalog mutex ... is taken OUTERMOST, for the duration of a
                             single statement, and released before the next message is read"

and it is forced by a type: `executor::run(stmt, catalog: &mut Catalog, ...)`. So ferrodb takes
an EXCLUSIVE mutex for a whole statement where turso takes a SHARED lock for a snapshot claim.

⚠ This script exists because reading is not measuring. One hour before it was written, a careful
read of turso's source predicted its hit path would NOT degrade, and the run said otherwise. A
structural argument earns a prediction, not a conclusion.

Two arms, the same shape D48 used:

  A  SHARED   — N clients against ONE server. The real configuration.
  B  PRIVATE  — N clients against N servers, each on its own port and its own database file.
                THE CONTROL: it removes the shared Mutex<Catalog> and nothing else about the
                client workload changes. Without it, arm A's slope means nothing, because a
                harness can be the wall.

Order is rotated per round (Latin square) and medians are taken per point: interleaving cancels
drift across a run, but only rotation cancels a within-round position bias, which has constant
sign and cannot be averaged away.

Guards REFUSE rather than warn: a point where any client completed zero statements, or where a
SELECT returned a row count other than 1, exits non-zero. A run that collected nothing has not
passed, and a query that finds nothing is not a read path however fast it runs.
"""
import os
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tests", "pg"))
from pg_client import Conn  # noqa: E402

POINTS = [1, 2, 4, 8, 16]
ROUNDS = 3
WARMUP = 0.3
MEASURE = 1.0
ROWS = 200  # small on purpose: the working set must be RESIDENT, not large


def hammer(host, port, key, start_barrier, stop, out, idx):
    """One client: its own connection, the same SELECT, until stop is set."""
    c = Conn(host, port)
    c.startup()
    sql = f"SELECT v FROM t WHERE id = {key};"
    t0 = time.monotonic()
    while time.monotonic() - t0 < WARMUP:
        c.query(sql)
    start_barrier.wait()
    iters = 0
    rows = 0
    errs = 0
    while not stop.is_set():
        # pg_client.query returns (fields, rows, tags, errors) -- a 4-tuple whose len() is
        # ALWAYS 4. Taking len() of it was the first version of this line and the seed guard
        # caught it by refusing, which is the whole reason the guard asserts a row COUNT and
        # not merely 'no error'.
        _fields, r, _tags, errors = c.query(sql)
        rows += len(r)
        errs += len(errors)
        iters += 1
    out[idx] = (iters, rows, errs)
    c.terminate()


def sweep_point(endpoints, threads):
    """endpoints: list of (host, port). Client t uses endpoints[t % len(endpoints)]."""
    start = threading.Barrier(threads + 1)
    stop = threading.Event()
    out = [None] * threads
    ts = []
    for t in range(threads):
        host, port = endpoints[t % len(endpoints)]
        key = (t * 97) % ROWS + 1
        th = threading.Thread(target=hammer, args=(host, port, key, start, stop, out, t))
        th.start()
        ts.append(th)
    start.wait()
    t0 = time.monotonic()
    time.sleep(MEASURE)
    stop.set()
    elapsed = time.monotonic() - t0
    for th in ts:
        th.join()

    if any(o is None for o in out):
        sys.exit(f"GUARD: {out.count(None)} of {threads} clients returned nothing")
    iters = sum(o[0] for o in out)
    rows = sum(o[1] for o in out)
    errs = sum(o[2] for o in out)
    slowest = min(o[0] for o in out)
    if errs:
        sys.exit(f"GUARD: {errs} statements returned a protocol ERROR; a failing query is not a read path")
    if iters == 0 or slowest == 0:
        sys.exit(f"GUARD: {threads} clients produced {iters} statements (slowest client {slowest})")
    if rows != iters:
        sys.exit(f"GUARD: {iters} statements returned {rows} rows; each must return exactly one")
    return iters / elapsed, slowest


def run_arm(endpoints, label):
    print()
    print(f"=== ARM {label} ===")
    print("# order rotated per round; a within-round position bias has constant sign")
    samples = {p: [] for p in POINTS}
    for r in range(ROUNDS):
        order = [POINTS[(i + r) % len(POINTS)] for i in range(len(POINTS))]
        print(f"# round {r} order: {' '.join(str(x) for x in order)}")
        for p in order:
            ops, slowest = sweep_point(endpoints, p)
            samples[p].append(ops)
            print(f"#   {p}C -> {ops:.0f} stmt/s (slowest client {slowest})")
    base = statistics.median(samples[POINTS[0]])
    print()
    print(f"{'clients':>8}{'total_stmt_s':>15}{'per_client':>14}{'total_vs_1C':>14}{'per_client_vs_1C':>18}")
    for p in POINTS:
        m = statistics.median(samples[p])
        print(f"{p:>8}{m:>15.0f}{m / p:>14.0f}{m / base:>14.3f}{(m / p) / base:>18.3f}")
    return {p: statistics.median(samples[p]) for p in POINTS}


def main():
    # argv: host port[,port,...]  -- one port = arm A only; many = arm A on the first, arm B on all
    host = sys.argv[1]
    ports = [int(x) for x in sys.argv[2].split(",")]
    print(f"# D50-FERRODB-SNAPSHOT-COST  host={host} ports={ports} rows={ROWS} "
          f"warmup={WARMUP}s measure={MEASURE}s rounds={ROUNDS}")
    a = run_arm([(host, ports[0])], "SHARED  (N clients, ONE server, one Mutex<Catalog>)")
    if len(ports) > 1:
        b = run_arm([(host, p) for p in ports],
                    "PRIVATE (N clients, N servers) -- the CONTROL")
        print()
        print(f"{'clients':>8}{'shared_vs_1C':>15}{'private_vs_1C':>16}{'apart':>10}")
        for p in POINTS:
            print(f"{p:>8}{a[p] / a[POINTS[0]]:>15.3f}{b[p] / b[POINTS[0]]:>16.3f}{b[p] / a[p]:>10.2f}x")


if __name__ == "__main__":
    main()
