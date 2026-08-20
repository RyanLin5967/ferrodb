#!/usr/bin/env python3
"""B12 — a **real, third-party Postgres driver** against this server: asyncpg.

Why asyncpg and not the hand-written client next door: `pg_client.py` is a second reading of the
protocol, but it is a reading written *here*, for this server. asyncpg was written for PostgreSQL,
by people who have never seen this code, and it uses the extended query protocol for every single
statement — `Parse`/`Describe`/`Flush`, then `Bind`/`Execute`/`Sync`, with parameters and results
in **binary** format. Before B12 it could not run one query: the server answered every message tag
except `Q` and `X` with "not implemented".

**This script fails loudly if asyncpg is missing.** A test that skips is a test that always passes,
and "the driver works" is exactly the claim that must not be provable by absence.

Exits non-zero and prints FAIL on the first violation.
"""
import asyncio
import decimal
import sys

try:
    import asyncpg
except ImportError as e:  # noqa: BLE001 - the message is the point
    print(
        "FAIL asyncpg is not installed, so the claim 'a real driver works against this server' "
        f"cannot be checked ({e}). Install it with: python3 -m pip install --user asyncpg"
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


async def main(host, port):
    dsn = dict(host=host, port=port, user="ferro", database="ferro")
    con = await asyncpg.connect(**dsn)

    # ---- connection ---------------------------------------------------------------------------
    v = con.get_server_version()
    check(v.major >= 9, f"the driver could not parse server_version: {v}")

    await con.execute(
        "CREATE TABLE inv (id INTEGER NOT NULL, name VARCHAR(64), qty BIGINT, "
        "price DECIMAL, ok BOOLEAN, ratio FLOAT)"
    )

    # ---- parameters in every position ---------------------------------------------------------
    # A parameter in the VALUES list, which is where a driver's own INSERT helper puts them.
    tag = await con.execute(
        "INSERT INTO inv VALUES ($1, $2, $3, $4, $5, $6)",
        1, "widget", 20, decimal.Decimal("1.50"), True, 0.5,
    )
    check(tag.startswith("INSERT"), f"parameterised INSERT reported {tag!r}")

    # ...and a second row, so every SELECT below has something it must NOT return.
    await con.execute(
        "INSERT INTO inv VALUES ($1, $2, $3, $4, $5, $6)",
        2, "gadget", 9007199254740993, decimal.Decimal("123456789.123456789"), False, 2.25,
    )

    # The claim in the exit criteria: a parameterised query, run by a real driver.
    row = await con.fetchrow(
        "SELECT id, name, qty, price, ok, ratio FROM inv WHERE id = $1", 2
    )
    check(row is not None, "the parameterised query returned no row at all")
    check(row["id"] == 2, f"the parameter selected the wrong row: {row}")

    # ---- types, in both directions ------------------------------------------------------------
    # The driver decodes by the OID this server announced, in binary. A type mismatch here is not
    # an exception, it is a wrong value — which is why each of these is checked by value and by
    # Python type.
    check(isinstance(row["id"], int) and row["id"] == 2, f"int4: {row['id']!r}")
    check(row["name"] == "gadget" and isinstance(row["name"], str), f"text: {row['name']!r}")
    check(
        row["qty"] == 9007199254740993,
        f"int8 past 2^53 lost precision: {row['qty']!r} — this is the value that proves it did "
        f"not travel through a double",
    )
    check(
        isinstance(row["price"], decimal.Decimal)
        and row["price"] == decimal.Decimal("123456789.123456789"),
        f"numeric: {row['price']!r}",
    )
    check(row["ok"] is False, f"bool: {row['ok']!r}")
    check(row["ratio"] == 2.25, f"float8: {row['ratio']!r}")

    # The trailing zero in `1.50` is information — it is the scale the row was written with — and
    # the wire format carries it in `dscale`. A round trip through a float, or through a numeric
    # codec that normalises, loses exactly this.
    price = await con.fetchval("SELECT price FROM inv WHERE id = $1", 1)
    check(str(price) == "1.50", f"numeric dropped its scale: {price!r}")

    # NULL is not a value of any type, and must not arrive as one.
    await con.execute("INSERT INTO inv VALUES ($1, $2, $3, $4, $5, $6)",
                      3, None, None, None, None, None)
    row = await con.fetchrow("SELECT id, name, qty FROM inv WHERE id = $1", 3)
    check(row["name"] is None and row["qty"] is None, f"NULL came back as {row!r}")

    # ---- a parameter is a value, and can never be SQL -----------------------------------------
    # The breaking shape: a text parameter that is itself a statement. If parameters were spliced
    # into the SQL text this would drop the table — and this server's scanner has no `''` escape,
    # so the usual quoting would not save it.
    hostile = "'); DROP TABLE inv; --"
    await con.execute("INSERT INTO inv VALUES ($1, $2, $3, $4, $5, $6)",
                      4, hostile, 1, decimal.Decimal("0"), True, 1.0)
    back = await con.fetchval("SELECT name FROM inv WHERE id = $1", 4)
    check(back == hostile, f"the hostile parameter did not survive intact: {back!r}")
    still_there = await con.fetchval("SELECT id FROM inv WHERE id = $1", 1)
    check(still_there == 1, "the table is gone: a parameter was executed as SQL")

    # A quote alone is the smaller version of the same shape, and the one an ordinary name hits.
    await con.execute("INSERT INTO inv VALUES ($1, $2, $3, $4, $5, $6)",
                      5, "O'Brien", 1, decimal.Decimal("0"), True, 1.0)
    check(
        await con.fetchval("SELECT name FROM inv WHERE id = $1", 5) == "O'Brien",
        "a single quote in a parameter was mangled",
    )

    # ---- prepared statements ------------------------------------------------------------------
    ps = await con.prepare("SELECT id, name FROM inv WHERE id = $1")
    params = ps.get_parameters()
    check(
        len(params) == 1 and params[0].name == "int4",
        f"the server described $1 as {params!r}; it is compared against an INTEGER column",
    )
    attrs = [a.name for a in ps.get_attributes()]
    check(attrs == ["id", "name"], f"the described columns are not the selected ones: {attrs}")
    check([r["id"] for r in await ps.fetch(1)] == [1], "a prepared statement did not run")
    check([r["id"] for r in await ps.fetch(2)] == [2], "a prepared statement was not reusable")

    # The driver checks the argument count against ParameterDescription, so this failing is
    # evidence the description reached it.
    try:
        await con.fetch("SELECT id FROM inv WHERE id = $1")
        check(False, "the driver accepted a call with no argument for $1")
    except Exception as e:  # noqa: BLE001
        check(
            isinstance(e, asyncpg.exceptions.InterfaceError),
            f"expected the driver to refuse a missing argument, got {type(e).__name__}: {e}",
        )

    # ---- update, delete, and their tags -------------------------------------------------------
    tag = await con.execute("UPDATE inv SET qty = $1 WHERE id = $2", 41, 1)
    check(tag == "UPDATE 1", f"UPDATE tag was {tag!r}")
    check(await con.fetchval("SELECT qty FROM inv WHERE id = $1", 1) == 41, "the UPDATE did nothing")
    tag = await con.execute("DELETE FROM inv WHERE id = $1", 5)
    check(tag == "DELETE 1", f"DELETE tag was {tag!r}")

    # ---- executemany, cursors, transactions ---------------------------------------------------
    await con.executemany(
        "INSERT INTO inv VALUES ($1, $2, $3, $4, $5, $6)",
        [(10 + i, f"bulk{i}", i, decimal.Decimal("0"), True, 1.0) for i in range(5)],
    )
    check(
        await con.fetchval("SELECT name FROM inv WHERE id = $1", 14) == "bulk4",
        "executemany did not write every row",
    )

    async with con.transaction():
        seen = [r["id"] async for r in con.cursor("SELECT id FROM inv")]
    # A cursor is a portal executed in windows: the driver sends Execute with a row limit and
    # resumes on PortalSuspended. A server that re-ran the statement each time would repeat rows.
    check(len(seen) == len(set(seen)), f"a cursor returned duplicate rows: {seen}")
    check(len(seen) == 9, f"a cursor saw {len(seen)} rows, expected 9: {seen}")

    async with con.transaction():
        await con.execute("INSERT INTO inv VALUES ($1, $2, $3, $4, $5, $6)",
                          20, "txn", 1, decimal.Decimal("0"), True, 1.0)
    check(await con.fetchval("SELECT name FROM inv WHERE id = $1", 20) == "txn",
          "a committed transaction did not land")

    # ---- errors leave the connection usable ---------------------------------------------------
    try:
        await con.fetch("SELECT * FROM no_such_table")
        check(False, "a query against a missing table did not raise")
    except asyncpg.PostgresError as e:
        check("no_such_table" in str(e), f"the error did not name the table: {e}")
    check(
        await con.fetchval("SELECT id FROM inv WHERE id = $1", 1) == 1,
        "the connection was unusable after a failed statement",
    )

    # ---- session parameters -------------------------------------------------------------------
    await con.execute("SET application_name TO 'asyncpg-check'")
    check(
        await con.fetchval("SHOW application_name") == "asyncpg-check",
        "SHOW did not read back what SET wrote",
    )

    await con.close()

    # ---- a connection pool, which is two connections at once -----------------------------------
    # The pool also exercises the release path: asyncpg sends
    # `SELECT pg_advisory_unlock_all(); CLOSE ALL; UNLISTEN *; RESET ALL;` as one simple query
    # every time a connection goes back, and a server that refuses any of the four breaks it.
    pool = await asyncpg.create_pool(min_size=2, max_size=2, **dsn)

    async def read(i):
        async with pool.acquire() as c:
            return await c.fetchval("SELECT name FROM inv WHERE id = $1", i)

    got = await asyncio.gather(read(1), read(2), read(1), read(2))
    check(got == ["widget", "gadget", "widget", "gadget"], f"pooled reads returned {got}")
    await pool.close()

    if FAILURES:
        print(f"FAIL {len(FAILURES)} of {CHECKS} checks failed")
        sys.exit(1)
    print(f"OK {CHECKS} checks passed (asyncpg {asyncpg.__version__})")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], int(sys.argv[2])))
