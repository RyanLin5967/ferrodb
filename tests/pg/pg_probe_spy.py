#!/usr/bin/env python3
"""What does a real driver actually send when it connects? — the instrument, not the recollection.

`usage: python3 pg_probe_spy.py {asyncpg|pg8000}`

This is a measurement tool, not a test. It stands up a Postgres-shaped socket that answers only the
startup handshake and then records every frontend message the driver sends, so the question "which
session parameters and which pg_catalog tables does this driver demand on connect?" has an answer
that came from the driver rather than from reading its source or remembering.

**Measured 2026-08-18, asyncpg 0.31.0 and pg8000 (system python3.9):**

    asyncpg   SSLRequest, Startup(client_encoding 'utf-8', user, database), then nothing
    pg8000    SSLRequest, Startup(user, database), then nothing

Both drivers issue **no statement at all** at connect time: they read the session parameters they
care about out of the `ParameterStatus` messages, and they query `pg_catalog` only later and only
for a type OID they have no codec for — which, for the OIDs this server announces, never happens.
That is why `src/pgwire/session.rs` implements no `pg_catalog` tables and refuses them instead: the
demand measured here is zero, and inventing rows to meet a demand that does not exist would only
give a driver wrong types to cache.

Re-run it after upgrading a driver. If a future version starts probing, this prints what it asks
for, and the answer stops being a claim about the past.
"""
import socket, struct, sys, threading

def frame(tag, body=b""):
    return tag + struct.pack("!i", len(body) + 4) + body

def cstr(s):
    return s.encode() + b"\x00"

def serve(port_holder, log, ready):
    srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0)); srv.listen(5)
    port_holder.append(srv.getsockname()[1]); ready.set()
    conn, _ = srv.accept()
    conn.settimeout(10)
    buf = b""
    def recv(n):
        nonlocal buf
        while len(buf) < n:
            c = conn.recv(65536)
            if not c: raise EOFError
            buf += c
        out, buf = buf[:n], buf[n:]
        return out
    try:
        # startup (possibly preceded by an SSL request)
        while True:
            (ln,) = struct.unpack("!i", recv(4))
            body = recv(ln - 4)
            code = struct.unpack("!i", body[:4])[0]
            if code == 80877103:
                log.append(("SSLRequest", ""))
                conn.sendall(b"N")
                continue
            log.append(("Startup", body[4:].replace(b"\x00", b" ").decode(errors="replace").strip()))
            break
        out = frame(b"R", struct.pack("!i", 0))
        for k, v in [("server_version", "9.6.0 (spy)"), ("client_encoding", "UTF8"),
                     ("DateStyle", "ISO, MDY"), ("integer_datetimes", "on"),
                     ("IntervalStyle", "postgres"), ("is_superuser", "on"),
                     ("server_encoding", "UTF8"), ("session_authorization", "ferro"),
                     ("standard_conforming_strings", "on"), ("TimeZone", "UTC"),
                     ("application_name", "")]:
            out += frame(b"S", cstr(k) + cstr(v))
        out += frame(b"K", struct.pack("!ii", 1, 1)) + frame(b"Z", b"I")
        conn.sendall(out)
        while True:
            tag = recv(1)
            (ln,) = struct.unpack("!i", recv(4))
            body = recv(ln - 4) if ln > 4 else b""
            text = body.replace(b"\x00", b" ").decode(errors="replace").strip()
            log.append((tag.decode(), text[:120]))
            if tag == b"X":
                break
            # Answer just enough to keep a driver moving: a Sync gets ReadyForQuery, a Query gets
            # an empty result, a Parse/Describe gets ParseComplete/NoData.
            if tag == b"S":
                conn.sendall(frame(b"Z", b"I"))
            elif tag == b"Q":
                conn.sendall(frame(b"C", cstr("SELECT 0")) + frame(b"Z", b"I"))
            elif tag == b"P":
                conn.sendall(frame(b"1"))
            elif tag == b"D":
                conn.sendall(frame(b"t", struct.pack("!h", 0)) + frame(b"n"))
            elif tag == b"B":
                conn.sendall(frame(b"2"))
            elif tag == b"E":
                conn.sendall(frame(b"C", cstr("SELECT 0")))
            elif tag == b"H":
                pass
    except Exception as e:
        log.append(("<end>", type(e).__name__))
    finally:
        conn.close(); srv.close()

def main(which):
    port, log, ready = [], [], threading.Event()
    t = threading.Thread(target=serve, args=(port, log, ready), daemon=True); t.start()
    ready.wait()
    p = port[0]
    try:
        if which == "asyncpg":
            import asyncio, asyncpg
            async def go():
                c = await asyncpg.connect(host="127.0.0.1", port=p, user="ferro", database="ferro")
                await c.close()
            asyncio.run(go())
        else:
            import pg8000.dbapi
            c = pg8000.dbapi.connect(host="127.0.0.1", port=p, user="ferro", database="ferro")
            c.close()
    except Exception as e:
        print(f"[client raised {type(e).__name__}: {str(e)[:80]}]")
    t.join(timeout=12)
    print(f"=== {which}: messages sent between startup and the first application statement ===")
    for tag, text in log:
        print(f"  {tag!r:12} {text}")

main(sys.argv[1])
