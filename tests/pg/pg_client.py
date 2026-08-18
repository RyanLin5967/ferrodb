#!/usr/bin/env python3
"""An independent PostgreSQL v3 wire client, written from the protocol spec.

Deliberately does NOT reuse ferrodb's encoder. The point is to check the server against a separate
reading of the wire format: if both sides shared an implementation, a consistent misunderstanding
of the protocol would pass every test and still fail against a real client.

It covers the simple query protocol and, since B12, the extended one — Parse / Bind / Describe /
Execute / Close / Sync / Flush, in both text and binary formats. The extended checks here are
deliberately kept even though `tests/pg/pg_asyncpg_client.py` runs a real driver over the same
ground: asyncpg agrees with itself about what it sends, and this file is a second reading of the
specification that agrees with nobody.

Exits non-zero and prints FAIL on the first violation.
"""
import socket, struct, sys

SSL_REQUEST = 80877103
PROTOCOL_V3 = 196608


class Conn:
    def __init__(self, host, port):
        self.s = socket.create_connection((host, port), timeout=10)
        self.buf = b""

    def _recv(self, n):
        while len(self.buf) < n:
            chunk = self.s.recv(65536)
            if not chunk:
                raise EOFError("server closed the connection")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def msg(self):
        """One backend message: 1-byte tag, then a length that INCLUDES itself but not the tag."""
        tag = self._recv(1)
        (length,) = struct.unpack("!i", self._recv(4))
        body = self._recv(length - 4)
        return tag, body

    def send(self, tag, payload=b""):
        self.s.sendall(tag + struct.pack("!i", len(payload) + 4) + payload)

    def startup(self):
        # Every real client probes for TLS first; the server must answer with a bare byte.
        self.s.sendall(struct.pack("!ii", 8, SSL_REQUEST))
        reply = self.s.recv(1)
        assert reply in (b"N", b"S"), f"bad SSL reply {reply!r}"
        assert reply == b"N", "server claimed TLS support it does not have"

        params = b"user\x00ferro\x00database\x00ferro\x00application_name\x00wirecheck\x00\x00"
        payload = struct.pack("!i", PROTOCOL_V3) + params
        self.s.sendall(struct.pack("!i", len(payload) + 4) + payload)

        saw_auth = saw_ready = False
        self.params = {}
        while True:
            tag, body = self.msg()
            if tag == b"R":
                (code,) = struct.unpack("!i", body[:4])
                assert code == 0, f"expected AuthenticationOk, got {code}"
                saw_auth = True
            elif tag == b"Z":
                assert body == b"I", f"expected idle status, got {body!r}"
                saw_ready = True
                break
            elif tag == b"S":
                k, v, _ = body.split(b"\x00", 2)
                self.params[k.decode()] = v.decode()
            elif tag == b"K":
                pass
            elif tag == b"E":
                raise AssertionError(f"error during startup: {body!r}")
            else:
                raise AssertionError(f"unexpected startup message {tag!r}")
        assert saw_auth and saw_ready
        return self

    # ---- reading -----------------------------------------------------------------------------

    def _row_desc(self, body):
        (n,) = struct.unpack("!h", body[:2])
        fields, off = [], 2
        for _ in range(n):
            end = body.index(b"\x00", off)
            name = body[off:end].decode()
            off = end + 1
            _tbl, _col, oid, _sz, _mod, fmt = struct.unpack("!ihihih", body[off:off + 18])
            off += 18
            fields.append((name, oid, fmt))
        return fields

    def _data_row(self, body):
        (n,) = struct.unpack("!h", body[:2])
        cols, off = [], 2
        for _ in range(n):
            (ln,) = struct.unpack("!i", body[off:off + 4])
            off += 4
            if ln == -1:
                cols.append(None)  # SQL NULL
            else:
                cols.append(body[off:off + ln])
                off += ln
        return cols

    def _error(self, body):
        parts = {}
        for chunk in body.split(b"\x00"):
            if chunk:
                parts[chunk[0:1].decode()] = chunk[1:].decode()
        return parts

    def collect(self, until=b"Z"):
        """Read messages until `until` arrives, returning everything seen."""
        out = {"fields": None, "rows": [], "tags": [], "errors": [], "flags": []}
        while True:
            tag, body = self.msg()
            if tag == b"T":
                out["fields"] = self._row_desc(body)
            elif tag == b"D":
                out["rows"].append(self._data_row(body))
            elif tag == b"C":
                out["tags"].append(body.rstrip(b"\x00").decode())
            elif tag == b"E":
                out["errors"].append(self._error(body))
            elif tag == b"I":
                out["tags"].append("EMPTY")
            elif tag == b"t":
                (n,) = struct.unpack("!h", body[:2])
                out["param_oids"] = list(struct.unpack("!" + "i" * n, body[2:2 + 4 * n]))
            elif tag in (b"1", b"2", b"3", b"n", b"s"):
                out["flags"].append(tag.decode())
            elif tag == b"S":
                pass  # ParameterStatus can arrive at any time
            elif tag == b"Z":
                out["status"] = body.decode()
            else:
                raise AssertionError(f"unexpected message {tag!r} in response")
            if tag == until:
                return out

    def query(self, sql):
        payload = sql.encode() + b"\x00"
        self.send(b"Q", payload)
        out = self.collect()
        return out["fields"], [[None if c is None else c.decode() for c in r] for r in out["rows"]], \
            out["tags"], out["errors"]

    # ---- extended query protocol -------------------------------------------------------------

    def parse(self, name, sql, param_oids=()):
        payload = name.encode() + b"\x00" + sql.encode() + b"\x00"
        payload += struct.pack("!h", len(param_oids))
        for oid in param_oids:
            payload += struct.pack("!i", oid)
        self.send(b"P", payload)

    def bind(self, portal, stmt, params=(), param_formats=(), result_formats=()):
        payload = portal.encode() + b"\x00" + stmt.encode() + b"\x00"
        payload += struct.pack("!h", len(param_formats))
        for f in param_formats:
            payload += struct.pack("!h", f)
        payload += struct.pack("!h", len(params))
        for p in params:
            if p is None:
                payload += struct.pack("!i", -1)
            else:
                payload += struct.pack("!i", len(p)) + p
        payload += struct.pack("!h", len(result_formats))
        for f in result_formats:
            payload += struct.pack("!h", f)
        self.send(b"B", payload)

    def describe(self, kind, name):
        self.send(b"D", kind + name.encode() + b"\x00")

    def execute(self, portal="", limit=0):
        self.send(b"E", portal.encode() + b"\x00" + struct.pack("!i", limit))

    def close_(self, kind, name):
        self.send(b"C", kind + name.encode() + b"\x00")

    def sync(self):
        self.send(b"S")

    def flush(self):
        self.send(b"H")

    def terminate(self):
        self.send(b"X")
        self.s.close()


def main():
    host, port = sys.argv[1], int(sys.argv[2])
    c = Conn(host, port).startup()
    checks = 0

    def check(cond, what):
        nonlocal checks
        checks += 1
        if not cond:
            print(f"FAIL {what}")
            sys.exit(1)

    check(c.params.get("server_version"), f"no server_version was reported: {c.params}")
    check(
        c.params.get("client_encoding") == "UTF8",
        f"client_encoding was not reported as UTF8: {c.params}",
    )

    _, _, tags, errs = c.query("CREATE TABLE t (id INTEGER NOT NULL, name VARCHAR(32));")
    check(not errs, f"CREATE TABLE errored: {errs}")

    _, _, tags, errs = c.query("INSERT INTO t VALUES (1, 'alpha');")
    check(not errs, f"INSERT errored: {errs}")
    check(any(t.startswith("INSERT") for t in tags), f"INSERT tag missing: {tags}")

    c.query("INSERT INTO t VALUES (2, 'beta');")

    fields, rows, tags, errs = c.query("SELECT id, name FROM t;")
    check(not errs, f"SELECT errored: {errs}")
    check(fields is not None and len(fields) == 2, f"expected 2 fields, got {fields}")
    check(fields[0][1] == 23, f"first column should be int4 oid 23, got {fields[0][1]}")
    check(fields[1][1] == 25, f"second column should be text oid 25, got {fields[1][1]}")
    # The column names now come from the catalog rather than being positional placeholders, which
    # is what lets a driver hand back a record you can index by name.
    check([f[0] for f in fields] == ["id", "name"], f"columns were not named: {fields}")
    got = sorted((r[0], r[1]) for r in rows)
    check(got == [("1", "alpha"), ("2", "beta")], f"unexpected rows: {rows}")
    check(any(t == "SELECT 2" for t in tags), f"expected 'SELECT 2', got {tags}")

    # A bad statement must come back as ErrorResponse and the connection must stay usable.
    _, _, _, errs = c.query("SELECT * FROM does_not_exist;")
    check(bool(errs), "a query against a missing table did not produce an ErrorResponse")
    fields, rows, _, errs = c.query("SELECT id FROM t;")
    check(not errs and len(rows) == 2, "the connection was unusable after an error")

    # ---- extended query protocol -------------------------------------------------------------
    #
    # Prepare + Describe ends in Flush, not Sync — which is exactly what asyncpg sends, and a
    # server that answers a Flush with ReadyForQuery hangs it. Here the check is the mirror image:
    # nothing but the described shape may arrive, and no ReadyForQuery.
    c.parse("s1", "SELECT id, name FROM t WHERE id = $1")
    c.describe(b"S", "s1")
    c.flush()
    out = c.collect(until=b"T")
    check("1" in out["flags"], f"no ParseComplete: {out}")
    check(out.get("param_oids") == [23], f"$1 was not described as int4: {out.get('param_oids')}")
    check(
        [f[0] for f in out["fields"]] == ["id", "name"],
        f"RowDescription did not name the columns: {out['fields']}",
    )
    check(
        [f[1] for f in out["fields"]] == [23, 25],
        f"RowDescription had the wrong types: {out['fields']}",
    )

    # Text-format parameter.
    c.bind("", "s1", params=[b"2"], param_formats=[0], result_formats=[0])
    c.execute("")
    c.sync()
    out = c.collect()
    check(not out["errors"], f"text-parameter execute errored: {out['errors']}")
    check(out["rows"] == [[b"2", b"beta"]], f"wrong row for $1 = 2: {out['rows']}")
    check(out["tags"] == ["SELECT 1"], f"wrong tag: {out['tags']}")

    # Binary parameter, binary results. This is the shape asyncpg uses for everything, and the one
    # a text-only server answers wrongly without ever raising an error: the client hands the ASCII
    # "1" to a four-byte integer decoder.
    c.bind("", "s1", params=[struct.pack("!i", 1)], param_formats=[1], result_formats=[1])
    c.execute("")
    c.sync()
    out = c.collect()
    check(not out["errors"], f"binary-parameter execute errored: {out['errors']}")
    check(len(out["rows"]) == 1, f"expected one row, got {out['rows']}")
    check(
        out["rows"][0][0] == struct.pack("!i", 1),
        f"int4 did not come back as four big-endian bytes: {out['rows'][0][0]!r}",
    )
    check(out["rows"][0][1] == b"alpha", f"text in binary format is its own bytes: {out['rows'][0]}")

    # A NULL parameter is length -1 and is not the string "NULL".
    c.parse("s_null", "SELECT id FROM t WHERE name = $1")
    c.bind("", "s_null", params=[None], param_formats=[0], result_formats=[0])
    c.execute("")
    c.sync()
    out = c.collect()
    check(not out["errors"], f"a NULL parameter errored: {out['errors']}")
    check(out["rows"] == [], f"nothing equals NULL, so no rows: {out['rows']}")

    # Portal suspension: ask for one row of two, then resume. A server that re-ran the statement on
    # the second Execute would send row 1 twice.
    c.parse("s_all", "SELECT id FROM t")
    c.bind("p1", "s_all", result_formats=[0])
    c.execute("p1", limit=1)
    c.sync()
    first = c.collect()
    check("s" in first["flags"], f"no PortalSuspended after a limited Execute: {first['flags']}")
    check(len(first["rows"]) == 1, f"row limit was ignored: {first['rows']}")
    c.execute("p1", limit=0)
    c.sync()
    second = c.collect()
    check(len(second["rows"]) == 1, f"the portal did not resume: {second['rows']}")
    check(
        first["rows"][0] != second["rows"][0],
        "the portal restarted instead of resuming, so a row was sent twice",
    )
    check(second["tags"] == ["SELECT 2"], f"the tag must count the whole portal: {second['tags']}")
    c.close_(b"P", "p1")
    c.close_(b"S", "s_all")
    c.sync()
    closed = c.collect()
    check(closed["flags"].count("3") == 2, f"Close was not acknowledged twice: {closed['flags']}")

    # An error mid-sequence must skip the rest of the sequence, not run it. Bind fails here, and
    # the Execute pipelined behind it must produce nothing at all — one error, one ReadyForQuery.
    c.parse("s_two", "SELECT id FROM t WHERE id = $1")
    c.bind("", "s_two", params=[], result_formats=[0])   # wrong number of parameters
    c.execute("")
    c.sync()
    out = c.collect()
    check(len(out["errors"]) == 1, f"expected exactly one error, got {out['errors']}")
    check(not out["rows"], "the Execute behind a failed Bind ran anyway")
    check(out.get("status") == "I", f"expected an idle ReadyForQuery, got {out.get('status')}")

    # ...and the connection is usable immediately afterwards.
    c.bind("", "s_two", params=[b"1"], param_formats=[0], result_formats=[0])
    c.execute("")
    c.sync()
    out = c.collect()
    check(not out["errors"] and out["rows"] == [[b"1"]], f"unusable after a failed Bind: {out}")

    # A truncated message is refused rather than panicking the connection thread. Before B12 this
    # check asserted that extended query was refused wholesale; now the same bytes are a malformed
    # Parse, and the requirement is the same one: say so, and stay alive.
    c.s.sendall(b"P" + struct.pack("!i", 4 + 1) + b"\x00")
    c.sync()
    out = c.collect()
    check(bool(out["errors"]), "a truncated Parse was silently ignored")
    fields, rows, _, errs = c.query("SELECT id FROM t;")
    check(not errs and len(rows) == 2, "the connection did not survive a malformed message")

    # SET and SHOW, which a driver uses to negotiate the session.
    _, _, tags, errs = c.query("SET application_name TO 'wire-test';")
    check(not errs and tags == ["SET"], f"SET failed: {errs} {tags}")
    fields, rows, _, errs = c.query("SHOW application_name;")
    check(not errs and rows == [["wire-test"]], f"SHOW did not read back the SET: {rows} {errs}")
    check(fields[0][0] == "application_name", f"SHOW column name: {fields}")
    _, _, _, errs = c.query("SET client_encoding TO 'LATIN1';")
    check(
        bool(errs),
        "the server accepted an encoding it cannot produce, which silently corrupts every string",
    )
    # RESET ALL is part of the four-statement string asyncpg's pool sends on every release.
    _, _, tags, errs = c.query(
        "SELECT pg_advisory_unlock_all();\nCLOSE ALL;\nUNLISTEN *;\nRESET ALL;"
    )
    check(not errs, f"the connection-release batch errored: {errs}")
    check(len(tags) == 4, f"expected four command tags for four statements, got {tags}")
    fields, rows, _, errs = c.query("SHOW application_name;")
    check(rows == [[""]], f"RESET ALL did not put application_name back: {rows}")

    c.terminate()
    print(f"OK {checks} checks passed")


if __name__ == "__main__":
    main()
