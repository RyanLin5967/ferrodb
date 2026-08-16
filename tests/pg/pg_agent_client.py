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

    # A's branch does not outlive A's socket: a session that ends without merging abandons its
    # branch, which is the whole point of the lease design — an agent that crashes must not leave
    # its work pinned forever. Asserted rather than assumed, because the first version of this
    # script expected `b1` to still resolve and was simply wrong about the design.
    _f, _r, _t, errors = b.query("SELECT qty FROM inv AS OF BRANCH b1;")
    check(
        any("unknown branch" in e.get("M", "") for e in errors),
        f"A's branch outlived the socket that opened it, so an abandoned agent pins storage: {errors}",
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
