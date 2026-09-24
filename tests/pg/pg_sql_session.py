#!/usr/bin/env python3
"""Run SQL statements in order on ONE connection, and print what each returned.

    pg_sql_session.py HOST PORT SQL [SQL ...]

A sequencer for Rust tests that need a real client session over the wire: an agent session lives
on its connection, so BEGIN AGENT SESSION, the writes and the MERGE have to share one socket. It
checks nothing about the protocol itself: `pg_client.py` is the independent reading of the protocol
and this only drives it.

Output, per statement: one `ROW\t<v1>\t<v2>...` line per row (SQL NULL as `\\N`), then
`DONE\t<command tag>`. A server error prints `ERROR\t<statement>\t<errors>` and exits 2 at once:
a later statement run on the session the error left behind would report something unrelated.
"""
import sys

from pg_client import Conn


def main():
    if len(sys.argv) < 4:
        print("ERROR\tusage: pg_sql_session.py HOST PORT SQL [SQL ...]")
        sys.exit(2)
    host, port, statements = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
    conn = Conn(host, port)
    conn.startup()
    for sql in statements:
        _fields, rows, tags, errors = conn.query(sql)
        if errors:
            print(f"ERROR\t{sql}\t{errors}")
            sys.stdout.flush()
            sys.exit(2)
        for row in rows:
            print("ROW\t" + "\t".join("\\N" if v is None else v for v in row))
        print("DONE\t" + (tags[-1] if tags else ""))
    sys.stdout.flush()


if __name__ == "__main__":
    main()
