#!/usr/bin/env python3
"""Agent-branch isolation over the PostgreSQL wire protocol, across TWO connections.

Reuses the connection from `pg_client.py` — the protocol reading is already independent of ferrodb's
encoder there, and duplicating it would not make it more independent. What is new here is the
*scenario*, and it needs two sockets because the claims are about what one client cannot see and
what another can.

The server was map-backed until 2026-08-16: it built `Session::new()`, so an agent session over the
wire staged rows in a `BTreeMap`. Two of the checks below would have passed anyway — a map hides a
row from another connection perfectly well — which is exactly why the third is here. Reading A's
uncommitted branch from B's socket is only possible if both connections share one runtime, and a
per-connection runtime is the obvious way to wire this wrong.

Exits non-zero and prints FAIL on the first violation.
"""
import sys

from pg_client import Conn


def main():
    host, port = sys.argv[1], int(sys.argv[2])
    checks = 0
    failures = []

    def check(cond, what):
        nonlocal checks
        checks += 1
        if not cond:
            failures.append(what)
            print(f"FAIL {what}")

    def rows_of(conn, sql):
        fields, rows, tags, errors = conn.query(sql)
        if errors:
            print(f"FAIL server error on `{sql}`: {errors}")
            failures.append(sql)
            return []
        return rows

    # **Sequential, not simultaneous.** `pgwire::serve` handles one connection start to finish
    # before accepting the next — deliberately, so the server makes no concurrency claim — so
    # holding two sockets open at once deadlocks the second in startup. That constraint makes this
    # scenario stronger rather than weaker: A's branch has to outlive A's socket for B to read it,
    # which a runtime rebuilt per connection could not do.
    a = Conn(host, port)
    a.startup()
    rows_of(a, "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);")
    rows_of(a, "INSERT INTO inv VALUES (1, 20);")

    before = rows_of(a, "SELECT qty FROM inv WHERE id = 1;")
    check(before == [["20"]], f"the committed row reads back before any branch: {before}")

    rows_of(a, "BEGIN AGENT SESSION AS 'pricing' RUN 'r_1';")
    rows_of(a, "UPDATE inv SET qty = qty - 5 WHERE id = 1;")
    seen_a = rows_of(a, "SELECT qty FROM inv WHERE id = 1;")
    check(seen_a == [["15"]], f"A, the writer, sees its own uncommitted write: {seen_a}")

    # A leaves WITHOUT merging.
    a.terminate()

    b = Conn(host, port)
    b.startup()
    seen_b = rows_of(b, "SELECT qty FROM inv WHERE id = 1;")
    check(seen_b == [["20"]], f"B does not see A's unmerged write: {seen_b}")

    # **A's branch DOES outlive A's socket, and that is the load-bearing fact of this script.**
    #
    # This check used to read `AS OF BRANCH b1` and assert "unknown branch", with a comment saying a
    # session that ends without merging abandons its branch. Both halves were wrong. The branch is
    # not abandoned — `Session` has no `Drop`, nothing on the disconnect path calls `abandon`, and the
    # runtime is shared by every connection on purpose — and the check passed only because the branch
    # is named `b_1`, with the underscore, so `b1` is a name that never existed. It would have passed
    # against a server that abandoned branches and against one that did not, which makes it no
    # instrument at all. Measured against a running server on 2026-08-17: `AS OF BRANCH b_1` from a
    # second connection returns A's uncommitted 15, while `b1` and `b_99` both answer "unknown
    # branch".
    #
    # **What actually happens to that branch in THIS server: nothing.** Saying so rather than pointing
    # at the lease, because this comment replaced one unverified claim and must not install another.
    # `TwoTierReaper` is constructed in `examples/agent_isolation_demo.rs` and two integration tests
    # and NOWHERE else — not in `examples/pgserver.rs`, not in the CLI (grepped 2026-08-18). So a
    # client that disconnects without merging leaks its branch for the life of the process. The lease
    # reaper is the design's answer to exactly that (exit criterion 8, non-cooperative expiry) and it
    # exists and is tested; it is simply not wired into the server under test here.
    _f, rows, _t, errors = b.query("SELECT qty FROM inv AS OF BRANCH b_1;")
    check(
        rows == [["15"]] and not errors,
        f"A's branch did not outlive the socket that opened it, so no other connection can inspect "
        f"an agent's work in progress: {rows} {errors}",
    )

    # The negative half, on a name that genuinely does not exist. Without it the check above would be
    # satisfied by a server that resolved every branch name to something.
    _f, _r, _t, errors = b.query("SELECT qty FROM inv AS OF BRANCH b_99;")
    check(
        any("unknown branch" in e.get("M", "") for e in errors),
        f"a branch name that was never minted resolved to something: {errors}",
    )

    # **The check a per-connection runtime fails.** Branch ids come from the shared catalog, so B's
    # first branch must be b2 — a runtime rebuilt per connection would start its counter over and
    # hand out b1 again, and every client would silently collide in the same branch namespace.
    _f, rows, _t, errors = b.query("BEGIN AGENT SESSION AS 'analytics' RUN 'r_2';")
    named = " ".join(" ".join(c or "" for c in r) for r in rows)
    # `b_2`, with the underscore — the server renders `branch_name: "b_2"`. The first version of
    # this matched on `"b2"` and failed against a perfectly correct server, which is the ordinary
    # way an instrument reports a defect that is its own.
    check(
        "b_2" in named and "b_1" not in named,
        f"B's first branch is not b_2, so the branch counter did not carry across connections: "
        f"{named!r} {errors}",
    )
    b.terminate()

    if failures:
        print(f"FAIL {len(failures)} of {checks} checks failed")
        sys.exit(1)
    print(f"OK {checks} checks passed")


if __name__ == "__main__":
    main()
