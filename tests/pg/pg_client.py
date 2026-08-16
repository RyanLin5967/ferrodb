#!/usr/bin/env python3
"""An independent PostgreSQL v3 wire client, written from the protocol spec.

Deliberately does NOT reuse ferrodb's encoder. The point is to check the server against a separate
reading of the wire format: if both sides shared an implementation, a consistent misunderstanding
of the protocol would pass every test and still fail against a real client.

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

    def startup(self):
        # Every real client probes for TLS first; the server must answer with a bare byte.
        self.s.sendall(struct.pack("!ii", 8, SSL_REQUEST))
        reply = self.s.recv(1)
        assert reply in (b"N", b"S"), f"bad SSL reply {reply!r}"
        assert reply == b"N", "server claimed TLS support it does not have"

        params = b"user\x00ferro\x00database\x00ferro\x00\x00"
        payload = struct.pack("!i", PROTOCOL_V3) + params
        self.s.sendall(struct.pack("!i", len(payload) + 4) + payload)

        saw_auth = saw_ready = False
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
            elif tag in (b"S", b"K"):
                pass
            elif tag == b"E":
                raise AssertionError(f"error during startup: {body!r}")
            else:
                raise AssertionError(f"unexpected startup message {tag!r}")
        assert saw_auth and saw_ready
        return self

    def query(self, sql):
        payload = sql.encode() + b"\x00"
        self.s.sendall(b"Q" + struct.pack("!i", len(payload) + 4) + payload)
        fields, rows, tags, errors = None, [], [], []
        while True:
            tag, body = self.msg()
            if tag == b"T":
                (n,) = struct.unpack("!h", body[:2])
                fields, off = [], 2
                for _ in range(n):
                    end = body.index(b"\x00", off)
                    name = body[off:end].decode()
                    off = end + 1
                    _tbl, _col, oid, _sz, _mod, _fmt = struct.unpack("!ihihih", body[off:off + 18])
                    off += 18
                    fields.append((name, oid))
            elif tag == b"D":
                (n,) = struct.unpack("!h", body[:2])
                cols, off = [], 2
                for _ in range(n):
                    (ln,) = struct.unpack("!i", body[off:off + 4])
                    off += 4
                    if ln == -1:
                        cols.append(None)          # SQL NULL
                    else:
                        cols.append(body[off:off + ln].decode())
                        off += ln
                rows.append(cols)
            elif tag == b"C":
                tags.append(body.rstrip(b"\x00").decode())
            elif tag == b"E":
                parts = {}
                for chunk in body.split(b"\x00"):
                    if chunk:
                        parts[chunk[0:1].decode()] = chunk[1:].decode()
                errors.append(parts)
            elif tag == b"I":
                tags.append("EMPTY")
            elif tag == b"Z":
                break
            else:
                raise AssertionError(f"unexpected message {tag!r} in query response")
        return fields, rows, tags, errors

    def terminate(self):
        self.s.sendall(b"X" + struct.pack("!i", 4))
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
    got = sorted((r[0], r[1]) for r in rows)
    check(got == [("1", "alpha"), ("2", "beta")], f"unexpected rows: {rows}")
    check(any(t == "SELECT 2" for t in tags), f"expected 'SELECT 2', got {tags}")

    # A bad statement must come back as ErrorResponse and the connection must stay usable.
    _, _, _, errs = c.query("SELECT * FROM does_not_exist;")
    check(bool(errs), "a query against a missing table did not produce an ErrorResponse")
    fields, rows, _, errs = c.query("SELECT id FROM t;")
    check(not errs and len(rows) == 2, "the connection was unusable after an error")

    # Extended query must be refused explicitly rather than ignored.
    c.s.sendall(b"P" + struct.pack("!i", 4 + 1) + b"\x00")
    saw_err = False
    while True:
        tag, body = c.msg()
        if tag == b"E":
            saw_err = True
        elif tag == b"Z":
            break
    check(saw_err, "an unimplemented message type was silently ignored")

    c.terminate()
    print(f"OK {checks} checks passed")


if __name__ == "__main__":
    main()
