#!/usr/bin/env python3
"""B9 — the agent-layer system views, read back through an independent wire client.

Reuses the connection from `pg_client.py`: the protocol reading there is already an independent
second implementation of the wire format, and duplicating it would not make it more independent.
What is new here is what is asserted — that a `SELECT` over each view arrives as **typed rows with
real column names**, which is a claim about the socket and cannot be made from inside Rust.

# The two things this checks that a Rust test cannot

1. **`RowDescription`.** A view's columns reach a client as a field list with a name and a type OID
   per column. Before B9 an agent statement arrived as ONE `text` column holding a Rust `Debug`
   literal, and an ordinary result got `column1..N` because no schema was carried past the executor.
   Only a client sees the difference.

2. **An empty view is not an empty answer.** `ferro_quarantine` is read here BEFORE anything is
   held, and the assertion is that all four fields still arrive with `SELECT 0`. That is the only
   signal separating "nothing is quarantined" from "this view is broken" — a client that got zero
   fields and zero rows could not tell them apart, and a server deriving its field list from the
   first row (which is what the pre-existing `Outcome::Rows` path does) would send exactly that.

# The quarantine scenario, and why it is shaped like this

`pgwire::serve` handles one connection start to finish before accepting the next, so two agent
sessions can never be open at the same instant. The stale-premise shape still reaches the server,
because an agent branch OUTLIVES the socket that opened it: `Session` has no `Drop`, nothing
abandons a branch on disconnect, and the runtime is shared by every connection deliberately. So:

  conn1  opens a session, POINT-reads row 1 (which is what retains an exact version), writes row 2,
         and leaves without merging.
  conn2  opens its own session, rewrites row 1, and merges — publishing row 1 and moving the premise
         conn1 reasoned from.
  conn3  merges conn1's branch BY NAME. The read-premise check fires, the branch is held, and the
         write to row 2 is not published.

The load-bearing part is that conn1 and conn2 wrote DIFFERENT rows. Nothing the merge engine
compares overlaps, so without a read-set check there is nothing here to object to — a workload where
two agents contend on the same row would never reach this path.

Exits non-zero and prints FAIL on the first violation.
"""
import sys

from pg_client import Conn

# Postgres type OIDs, from the spec rather than from ferrodb's `mod oid`.
BOOL, INT8, INT4, TEXT, NUMERIC = 16, 20, 23, 25, 1700

# Every view, and the exact field list it must announce. Written out rather than derived, so a
# change to the view definitions has to be made deliberately in two places.
EXPECTED_FIELDS = {
    "ferro_branches": [
        ("branch_id", INT8),
        ("generation", INT4),
        ("branch_name", TEXT),
        ("parent_id", INT8),
        ("fork_epoch", INT8),
        ("root_page_id", INT8),
        ("state", TEXT),
        # BIGINT since D60 removed the depth cap: the record field is a u32, and an i32 column
        # would report a negative depth rather than a large one. A client-visible type change,
        # pinned here deliberately — this assertion is what forced it to be justified rather than
        # slipped in.
        ("depth", INT8),
        ("arenas", INT4),
        ("live_children", INT4),
        ("lease_deadline", NUMERIC),
    ],
    "ferro_runs": [
        ("branch_id", INT8),
        ("generation", INT4),
        ("branch_name", TEXT),
        ("prov_id", INT4),
        ("agent_id", TEXT),
        ("run_id", TEXT),
        ("model_name", TEXT),
        ("model_version", TEXT),
        ("prompt_hash", TEXT),
        ("started_at", INT8),
        ("parent_branch", TEXT),
    ],
    "ferro_row_authors": [
        ("table_name", TEXT),
        ("row_id", NUMERIC),
        ("prov_id", INT4),
        ("agent_id", TEXT),
        ("run_id", TEXT),
        ("model_name", TEXT),
        ("model_version", TEXT),
    ],
    "ferro_quarantine": [
        ("branch_id", INT8),
        ("generation", INT4),
        ("branch_name", TEXT),
        ("reason", TEXT),
    ],
    "ferro_run_activity": [
        ("branch_id", INT8),
        ("generation", INT4),
        ("branch_name", TEXT),
        ("agent_id", TEXT),
        ("run_id", TEXT),
        ("ops_captured", INT8),
        ("guards_captured", INT8),
        ("staged_rows", INT8),
        ("rows_read_exact", INT8),
        ("scan_reads", INT8),
        ("scan_rows_observed", INT8),
        ("blind_writes", INT8),
    ],
}


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

    def q(conn, sql, expect_error=False):
        fields, rows, tags, errors = conn.query(sql)
        if errors and not expect_error:
            failures.append(sql)
            print(f"FAIL server error on `{sql}`: {[e.get('M') for e in errors]}")
        return fields, rows, tags, errors

    def col(fields, rows, name):
        at = [f[0] for f in (fields or [])]
        if name not in at:
            failures.append(f"no column {name}")
            print(f"FAIL no column {name!r} in {at}")
            return []
        i = at.index(name)
        return [r[i] for r in rows]

    # `(name, oid)`, dropping the format code.
    #
    # `pg_client.py` used to yield `(name, oid)` and B12's extended-protocol work (`60c6820`) added
    # the wire format as a third element, because a column announced as text and sent as binary is a
    # silent mojibake bug it wanted assertable. This client was written against the two-element
    # shape, so every comparison below failed on ARITY while every name and OID matched exactly.
    #
    # Normalised here rather than by widening the expectations, so what these assertions check is
    # unchanged: the names and the type OIDs, in order. A client that wants to assert the format
    # code should do it deliberately and separately — silently folding it into these lists would
    # make an unrelated format change read as a schema change.
    def name_oid(fields):
        return [(f[0], f[1]) for f in (fields or [])]

    def fields_match(view, fields, where):
        want = EXPECTED_FIELDS[view]
        check(
            name_oid(fields) == want,
            f"{view} {where}: RowDescription is {name_oid(fields)}, expected {want}",
        )

    # ---- conn1: seed, then an agent session whose premise will move -----------------------------
    a = Conn(host, port)
    a.startup()
    q(a, "CREATE TABLE oncall (id INTEGER NOT NULL, qty INTEGER);")
    q(a, "INSERT INTO oncall VALUES (1, 100);")
    q(a, "INSERT INTO oncall VALUES (2, 200);")
    q(a, "INSERT INTO oncall VALUES (3, 300);")

    # **Every view is readable and fully typed with nothing in it yet.** ferro_branches is the one
    # exception to "nothing in it": the trunk is always a branch, which is also the anti-vacuity
    # anchor for the whole file — a materialiser returning nothing at all would fail here while
    # every "must be empty" check below passed.
    for view in EXPECTED_FIELDS:
        fields, rows, tags, _ = q(a, f"SELECT * FROM {view};")
        fields_match(view, fields, "before any agent ran")
        expected_rows = 1 if view == "ferro_branches" else 0
        check(
            len(rows) == expected_rows,
            f"{view} had {len(rows)} rows before any agent ran, expected {expected_rows}: {rows}",
        )
        check(
            tags == [f"SELECT {expected_rows}"],
            f"{view} command tag was {tags}",
        )

    # The trunk row, and the value that a BIGINT column would have reported as -1.
    fields, rows, _, _ = q(a, "SELECT * FROM ferro_branches;")
    check(col(fields, rows, "branch_id") == ["0"], f"the trunk is not branch 0: {rows}")
    check(col(fields, rows, "branch_name") == ["b_0"], f"{rows}")
    check(
        col(fields, rows, "parent_id") == [None],
        f"the trunk's parent must arrive as SQL NULL (length -1), not a string: {rows}",
    )
    check(
        col(fields, rows, "lease_deadline") == ["18446744073709551615"],
        f"the trunk's u64::MAX lease did not survive the wire: {rows}",
    )

    q(a, "BEGIN AGENT SESSION AS 'held' RUN 'r_held' MODEL 'claude-opus-5/2026-05';")
    # A POINT read. This is what retains an exact version rather than a predicate summary, and it is
    # the premise conn2 is about to move.
    q(a, "SELECT qty FROM oncall WHERE id = 1;")
    # A DIFFERENT row, so nothing the merge engine compares will overlap with conn2's write.
    q(a, "UPDATE oncall SET qty = 222 WHERE id = 2;")

    # ferro_runs and ferro_run_activity, populated, read from INSIDE the agent's own session.
    fields, rows, _, _ = q(a, "SELECT * FROM ferro_runs;")
    fields_match("ferro_runs", fields, "with one session open")
    check(len(rows) == 1, f"ferro_runs did not see the open session: {rows}")
    check(col(fields, rows, "agent_id") == ["held"], f"{rows}")
    check(col(fields, rows, "run_id") == ["r_held"], f"{rows}")
    check(col(fields, rows, "model_name") == ["claude-opus-5"], f"{rows}")
    check(col(fields, rows, "model_version") == ["2026-05"], f"{rows}")
    check(len(col(fields, rows, "prompt_hash")[0]) == 64, f"{rows}")

    fields, rows, _, _ = q(a, "SELECT * FROM ferro_run_activity;")
    fields_match("ferro_run_activity", fields, "with one session open")
    check(col(fields, rows, "staged_rows") == ["1"], f"one row was written: {rows}")
    check(col(fields, rows, "rows_read_exact") == ["1"], f"one point read: {rows}")
    check(col(fields, rows, "blind_writes") == ["1"], f"row 2 was written unread: {rows}")

    # Projection and WHERE over a view: the field list must narrow to what was asked for.
    fields, rows, _, _ = q(a, "SELECT agent_id, staged_rows FROM ferro_run_activity WHERE staged_rows = 1;")
    check(
        name_oid(fields) == [("agent_id", TEXT), ("staged_rows", INT8)],
        f"a projected view did not narrow its RowDescription: {fields}",
    )
    check(rows == [["held", "1"]], f"{rows}")

    # A computed projection column. Its declared type in the plan is the binder's `Integer`
    # placeholder while the value is a BOOLEAN; announcing int4 here and sending 't' is a parse error
    # at any conforming driver, so the OID has to come from the value.
    fields, rows, _, _ = q(a, "SELECT branch_id = 9999 FROM ferro_branches;")
    check(
        name_oid(fields) == [("?column?", BOOL)],
        f"a computed column was announced as its placeholder type instead of the value's: {fields}",
    )
    check(rows == [["f"], ["f"]], f"{rows}")

    # A view is not branch-relative, and refuses rather than answering the present as the past.
    _f, _r, _t, errors = q(a, "SELECT * FROM ferro_runs AS OF BRANCH b_1;", expect_error=True)
    check(
        any("system view" in e.get("M", "") for e in errors),
        f"AS OF BRANCH on a system view was not refused: {errors}",
    )
    # A table may not shadow a view.
    _f, _r, _t, errors = q(a, "CREATE TABLE ferro_runs (id INTEGER NOT NULL);", expect_error=True)
    check(
        any("system view" in e.get("M", "") for e in errors),
        f"a table was allowed to take a view's name: {errors}",
    )
    # ...and the connection is still usable after both refusals.
    _f, rows, _t, _e = q(a, "SELECT * FROM ferro_runs;")
    check(len(rows) == 1, "the connection was unusable after a refusal")

    a.terminate()

    # ---- conn2: move the premise ------------------------------------------------------------------
    b = Conn(host, port)
    b.startup()
    # The branch conn1 opened outlives conn1's socket. Asserted, not assumed: the whole scenario
    # depends on it, and `pg_agent_client.py` carries a comment claiming the opposite.
    _f, rows, _t, _e = q(b, "SELECT * FROM ferro_runs;")
    check(
        len(rows) == 1 and rows[0][4] == "held",
        f"the branch conn1 opened did not outlive its socket, so the scenario cannot run: {rows}",
    )

    q(b, "BEGIN AGENT SESSION AS 'mover' RUN 'r_mover';")
    q(b, "UPDATE oncall SET qty = 111 WHERE id = 1;")
    fields, rows, _, _ = q(b, "MERGE;")
    # The merge result is typed columns now, not one Debug string.
    names = [f[0] for f in (fields or [])]
    check(
        "applied_to_target" in names and "merge_id" in names and "outcome" in names,
        f"MERGE did not come back as typed columns: {fields}",
    )
    check(len(fields) > 1, f"MERGE came back as a single column: {fields}")
    check(
        ("applied_to_target", BOOL) in name_oid(fields),
        f"applied_to_target is not a bool column: {fields}",
    )
    check(col(fields, rows, "applied_to_target") == ["t"], f"conn2's merge did not publish: {rows}")
    b.terminate()

    # ---- conn3: merge the held branch by name, then look at the view -----------------------------
    c = Conn(host, port)
    c.startup()

    fields, rows, tags, _ = q(c, "SELECT * FROM ferro_quarantine;")
    fields_match("ferro_quarantine", fields, "before anything is held")
    check(rows == [], f"something was held before the stale merge: {rows}")

    fields, rows, _, _ = q(c, "MERGE BRANCH b_1;")
    check(
        col(fields, rows, "applied_to_target") == ["f"],
        f"a merge held by the gate reported that it published: {rows}",
    )

    fields, rows, tags, _ = q(c, "SELECT * FROM ferro_quarantine;")
    fields_match("ferro_quarantine", fields, "with one branch held")
    check(len(rows) == 1, f"the held branch is not in the view: {rows}")
    check(tags == ["SELECT 1"], f"{tags}")
    check(col(fields, rows, "branch_id") == ["1"], f"{rows}")
    reason = col(fields, rows, "reason")[0]
    check(reason is not None, "the reason arrived as SQL NULL")
    check(
        reason is not None and "read-premise" in reason and "changed in the base" in reason,
        f"the view has the branch but not what it is held for: {reason!r}",
    )

    # The held branch is also in ferro_branches, in state Quarantined. `live_branches` filters to
    # Live and could never show it.
    fields, rows, _, _ = q(c, "SELECT branch_id, state FROM ferro_branches WHERE branch_id = 1;")
    check(rows == [["1", "Quarantined"]], f"a held branch is missing or mislabelled: {rows}")

    # The held branch's write was NOT published — quarantine is a hold, not an advisory note.
    _f, rows, _t, _e = q(c, "SELECT qty FROM oncall WHERE id = 2;")
    check(rows == [["200"]], f"a held branch published its write anyway: {rows}")

    # ferro_row_authors: populated by conn2's merge, and still answering after that branch is gone.
    fields, rows, _, _ = q(c, "SELECT * FROM ferro_row_authors;")
    fields_match("ferro_row_authors", fields, "after a merge published a row")
    check(len(rows) >= 1, f"no row was attributed after a merge: {rows}")
    check(col(fields, rows, "table_name") == ["oncall"], f"{rows}")
    check(col(fields, rows, "row_id") == ["1"], f"{rows}")
    check(col(fields, rows, "agent_id") == ["mover"], f"{rows}")
    check(col(fields, rows, "run_id") == ["r_mover"], f"{rows}")

    c.terminate()

    if failures:
        print(f"FAIL {len(failures)} of {checks} checks failed")
        sys.exit(1)
    print(f"OK {checks} checks passed")


if __name__ == "__main__":
    main()
