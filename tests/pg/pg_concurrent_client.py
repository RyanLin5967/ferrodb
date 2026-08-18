#!/usr/bin/env python3
"""B12 — two connections at once, and both of them getting work done.

`pgwire::serve` used to handle one connection start to finish before accepting the next. That is
not a small limitation: a second client did not merely wait, it **hung in startup**, because the
server was not in `accept()` at all. `tests/pg/pg_agent_client.py` says so in its own comment and
works around it by closing A's socket before opening B's.

So the claim to prove here is not "no error occurred" — a serial server produces no error either,
it just never answers. The claim is *interleaved progress*, and every step below is an effect one
connection can only observe because the other one is still open and still being served:

    A writes row 1  ->  B reads row 1  ->  B writes row 2  ->  A reads row 2  ->  ...

Neither half of that trace is producible by a server that finishes A before starting B. Sockets
carry timeouts throughout, so a server that deadlocks fails loudly here instead of hanging.

Exits non-zero and prints FAIL on the first violation.
"""
import struct
import sys
import threading
import time

from pg_client import Conn

TIMEOUT = 20

CHECKS = 0
FAILURES = []


def check(cond, what):
    global CHECKS
    CHECKS += 1
    if not cond:
        FAILURES.append(what)
        print(f"FAIL {what}")


def rows_of(conn, sql):
    _fields, rows, _tags, errors = conn.query(sql)
    if errors:
        FAILURES.append(f"server error on `{sql}`: {errors}")
        print(f"FAIL server error on `{sql}`: {errors}")
        return []
    return rows


def main():
    host, port = sys.argv[1], int(sys.argv[2])

    # ---- 1. two sockets, open at the same time ------------------------------------------------
    # Opened and started up before either runs a statement. On the sequential server B's startup
    # never returns, because the accept loop is inside A's handler.
    a = Conn(host, port)
    a.s.settimeout(TIMEOUT)
    a.startup()
    b = Conn(host, port)
    b.s.settimeout(TIMEOUT)
    b.startup()
    check(True, "two connections completed startup while both were open")

    rows_of(a, "CREATE TABLE ping (id INTEGER NOT NULL, who VARCHAR(16));")

    # ---- 2. the alternation ------------------------------------------------------------------
    trace = []
    for i in range(5):
        rows_of(a, f"INSERT INTO ping VALUES ({2 * i}, 'A');")
        trace.append(("A", "write", 2 * i))
        seen = rows_of(b, f"SELECT who FROM ping WHERE id = {2 * i};")
        trace.append(("B", "read", 2 * i))
        check(
            seen == [["A"]],
            f"B could not read what A had just written (round {i}): {seen}",
        )

        rows_of(b, f"INSERT INTO ping VALUES ({2 * i + 1}, 'B');")
        trace.append(("B", "write", 2 * i + 1))
        seen = rows_of(a, f"SELECT who FROM ping WHERE id = {2 * i + 1};")
        trace.append(("A", "read", 2 * i + 1))
        check(
            seen == [["B"]],
            f"A could not read what B had just written (round {i}): {seen}",
        )

    # The trace itself is the evidence: it changes connection twenty times, and a server that
    # served A to completion first could only produce a trace with one change.
    switches = sum(1 for x, y in zip(trace, trace[1:]) if x[0] != y[0])
    check(switches >= 10, f"the two connections did not interleave: {switches} switches in {trace}")

    # ---- 3. one connection inside a transaction does not stop the other -----------------------
    # The catalog lock is taken per statement, not per connection and not per transaction. Holding
    # it for either would be invisible in the checks above and fatal here.
    rows_of(a, "BEGIN;")
    rows_of(a, "INSERT INTO ping VALUES (100, 'A');")
    started = time.monotonic()
    seen = rows_of(b, "SELECT who FROM ping WHERE id = 0;")
    elapsed = time.monotonic() - started
    check(seen == [["A"]], f"B could not read while A held an open transaction: {seen}")
    check(elapsed < TIMEOUT / 2, f"B waited {elapsed:.1f}s for A's open transaction")
    rows_of(a, "COMMIT;")

    a.terminate()
    b.terminate()

    # ---- 4. many connections at once ----------------------------------------------------------
    # Four threads, four sockets, all writing to one table. This is the shape that finds a lock
    # ordering mistake: it is the only check here where two statements are genuinely in flight at
    # the same moment.
    errors = []
    done = threading.Barrier(5, timeout=TIMEOUT)

    def worker(w):
        try:
            c = Conn(host, port)
            c.s.settimeout(TIMEOUT)
            c.startup()
            for i in range(10):
                _f, _r, _t, errs = c.query(f"INSERT INTO ping VALUES ({1000 + w * 10 + i}, 'w{w}');")
                if errs:
                    errors.append(f"worker {w} row {i}: {errs}")
            c.terminate()
        except Exception as e:  # noqa: BLE001
            errors.append(f"worker {w} raised {type(e).__name__}: {e}")
        finally:
            try:
                done.wait()
            except threading.BrokenBarrierError:
                pass

    threads = [threading.Thread(target=worker, args=(w,), daemon=True) for w in range(4)]
    for t in threads:
        t.start()
    try:
        done.wait()
    except threading.BrokenBarrierError:
        pass
    for t in threads:
        t.join(timeout=TIMEOUT)
    check(not errors, f"concurrent writers reported errors: {errors}")
    check(all(not t.is_alive() for t in threads), "a writer thread never finished: the server wedged")

    c = Conn(host, port)
    c.s.settimeout(TIMEOUT)
    c.startup()
    landed = rows_of(c, "SELECT id FROM ping WHERE id > 999;")
    check(
        len(landed) == 40,
        f"{len(landed)} of 40 concurrently written rows are in the table — "
        f"concurrent writes are being lost",
    )

    # ---- 5. two agent sessions at once --------------------------------------------------------
    # The lock-ordering claim in one check. An agent session takes the runtime's state lock and the
    # branch catalog's lock *underneath* the catalog mutex this server now holds per statement; two
    # of them running at once is what a wrong order deadlocks on. Different rows, so the two
    # merges have nothing to conflict about — the question here is liveness, not merge semantics.
    rows_of(c, "CREATE TABLE stock (id INTEGER NOT NULL, qty INTEGER);")
    rows_of(c, "INSERT INTO stock VALUES (1, 100);")
    rows_of(c, "INSERT INTO stock VALUES (2, 200);")

    agent_errors = []

    def agent(w, row):
        try:
            ac = Conn(host, port)
            ac.s.settimeout(TIMEOUT)
            ac.startup()
            for sql in (
                f"BEGIN AGENT SESSION AS 'agent{w}' RUN 'r_{w}';",
                f"UPDATE stock SET qty = qty + 1 WHERE id = {row};",
                "MERGE;",
            ):
                _f, _r, _t, errs = ac.query(sql)
                if errs:
                    agent_errors.append(f"agent {w} on `{sql}`: {errs}")
            ac.terminate()
        except Exception as e:  # noqa: BLE001
            agent_errors.append(f"agent {w} raised {type(e).__name__}: {e}")

    ags = [threading.Thread(target=agent, args=(w, w + 1), daemon=True) for w in range(2)]
    for t in ags:
        t.start()
    for t in ags:
        t.join(timeout=TIMEOUT)
    check(
        all(not t.is_alive() for t in ags),
        "an agent-session thread never finished: two branches at once deadlocked the server",
    )
    check(not agent_errors, f"concurrent agent sessions reported errors: {agent_errors}")
    after = sorted(rows_of(c, "SELECT id, qty FROM stock;"))
    check(
        after == [["1", "101"], ["2", "201"]],
        f"both concurrent merges should have landed, giving 101 and 201: {after}",
    )
    c.terminate()

    if FAILURES:
        print(f"FAIL {len(FAILURES)} of {CHECKS} checks failed")
        sys.exit(1)
    print(f"OK {CHECKS} checks passed")


if __name__ == "__main__":
    main()
