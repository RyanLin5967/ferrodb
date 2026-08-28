#!/usr/bin/env python3
"""B12 — a **second** real driver: pg8000.

asyncpg and pg8000 share no code and no author. asyncpg is a Cython extension built on its own
protocol implementation; pg8000 is pure Python and speaks DBAPI 2.0. Where they agree about this
server, the agreement is not a shared misreading — and where they disagree, the difference is
information: pg8000 exercised two things asyncpg never sends.

  * `begin transaction` — DBAPI opens a transaction before every statement, and it uses the
    two-word spelling. ferrodb's parser knows `BEGIN` only, so `src/pgwire/session.rs` normalises
    the standard spellings. Without that, pg8000 cannot run a single statement.
  * A parameter whose type the *client* pins. pg8000's DBAPI sends no parameter types in `Parse` by
    default (`_input_oids` starts empty), like asyncpg — but `setinputsizes()` makes it declare
    them, and this file uses it, so the branch where a declared type wins over this server's
    inference is exercised by a real driver rather than only by a unit test.

**One limitation is real and is not a wire problem:** this engine refuses DDL inside a transaction
block (`executor.rs`), so `CREATE TABLE` needs `autocommit`. That is stated here rather than worked
around silently, because a reader of this file is entitled to know which of the two layers said no.

**Fails loudly if pg8000 is missing.** Exits non-zero and prints FAIL on the first violation.
"""
import decimal
import sys

try:
    import pg8000.dbapi
except ImportError as e:  # noqa: BLE001
    print(
        "FAIL pg8000 is not installed, so the claim 'a second real driver works against this "
        f"server' cannot be checked ({e}). Install it with: python3 -m pip install --user pg8000"
    )
    sys.exit(2)

CHECKS = 0
FAILURES = []


def check(cond, what):
    global CHECKS
    CHECKS += 1
    if not cond:
        FAILURES.append(what)
        print(f"FAIL {what}")


def rows(cur):
    """Every row as a plain list.

    `pg8000.dbapi` returns a *tuple* of rows, so `fetchall() == [[1]]` is false against a perfectly
    correct server. Normalising here rather than writing tuple literals in every check: the first
    version of this file compared against lists and reported six failures that were entirely its
    own, which is the ordinary way an instrument reports a defect it invented.
    """
    return [list(r) for r in cur.fetchall()]


def main(host, port):
    con = pg8000.dbapi.connect(host=host, port=port, user="ferro", database="ferro")
    # DDL cannot run inside a transaction block in this engine, and DBAPI opens one for every
    # statement. This is the engine's rule, not the protocol's.
    con.autocommit = True
    cur = con.cursor()

    cur.execute("CREATE TABLE p8 (id INTEGER NOT NULL, name VARCHAR(32), qty BIGINT, price DECIMAL)")
    cur.execute(
        "INSERT INTO p8 VALUES (%s, %s, %s, %s)",
        (1, "eight", 9007199254740993, decimal.Decimal("2.50")),
    )
    cur.execute("INSERT INTO p8 VALUES (%s, %s, %s, %s)", (2, "nine", 1, decimal.Decimal("3.00")))

    cur.execute("SELECT id, name, qty, price FROM p8 WHERE id = %s", (1,))
    got = rows(cur)
    check(len(got) == 1, f"the parameterised query returned {got}")
    row = got[0]
    check(row[0] == 1, f"int4: {row[0]!r}")
    check(row[1] == "eight", f"text: {row[1]!r}")
    check(row[2] == 9007199254740993, f"int8 past 2^53 lost precision: {row[2]!r}")
    check(
        isinstance(row[3], decimal.Decimal) and str(row[3]) == "2.50",
        f"numeric arrived as {row[3]!r}; the trailing zero is the scale it was written with",
    )

    oids = [d[1] for d in cur.description]
    check(oids == [23, 25, 20, 1700], f"the described types were {oids}")
    names = [d[0] for d in cur.description]
    check(names == ["id", "name", "qty", "price"], f"the described names were {names}")

    # A text parameter.
    cur.execute("SELECT id FROM p8 WHERE name = %s", ("eight",))
    check(rows(cur) == [[1]], "a text parameter did not select the right row")

    # The same query with the parameter's type *declared by the client*. This server infers types
    # from the column a parameter is compared against, and a declared type has to win over that
    # inference — otherwise a client that pinned a type gets values encoded for a different one.
    cur.setinputsizes(25)  # text
    cur.execute("SELECT id FROM p8 WHERE name = %s", ("eight",))
    check(rows(cur) == [[1]], "a client-declared parameter type did not work")
    cur.setinputsizes()

    # The breaking shape: a parameter that is itself SQL. If parameters were spliced into the
    # statement text this would end the string and start a new command.
    hostile = "'); DROP TABLE p8; --"
    cur.execute("INSERT INTO p8 VALUES (%s, %s, %s, %s)", (3, hostile, 1, decimal.Decimal("0")))
    cur.execute("SELECT name FROM p8 WHERE id = %s", (3,))
    check(rows(cur) == [[hostile]], "the hostile parameter did not survive intact")
    cur.execute("SELECT id FROM p8 WHERE id = %s", (1,))
    check(rows(cur) == [[1]], "the table is gone: a parameter was executed as SQL")

    # An UPDATE reports how many rows it touched.
    cur.execute("UPDATE p8 SET qty = %s WHERE id = %s", (42, 2))
    check(cur.rowcount == 1, f"UPDATE reported rowcount {cur.rowcount}")
    cur.execute("SELECT qty FROM p8 WHERE id = %s", (2,))
    check(rows(cur) == [[42]], "the UPDATE did not land")

    # Now with DBAPI's own transaction handling, which is where `begin transaction` comes from.
    con.autocommit = False
    cur.execute("INSERT INTO p8 VALUES (%s, %s, %s, %s)", (4, "txn", 1, decimal.Decimal("0")))
    con.commit()
    cur.execute("SELECT name FROM p8 WHERE id = %s", (4,))
    check(rows(cur) == [["txn"]], "a DBAPI-managed transaction did not commit")

    # An error must not poison the connection.
    try:
        cur.execute("SELECT * FROM nope_not_here")
        check(False, "a query against a missing table did not raise")
    except pg8000.dbapi.DatabaseError as e:
        check("nope_not_here" in str(e), f"the error did not name the table: {e}")
    con.rollback()
    cur.execute("SELECT id FROM p8 WHERE id = %s", (1,))
    check(rows(cur) == [[1]], "the connection was unusable after a failed statement")

    con.close()

    if FAILURES:
        print(f"FAIL {len(FAILURES)} of {CHECKS} checks failed")
        sys.exit(1)
    print(f"OK {CHECKS} checks passed (pg8000)")


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]))
