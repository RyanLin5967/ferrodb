#!/usr/bin/env python3
"""One client, one process. Prints "<iters> <rows> <errors>" after <seconds> of hammering.

Exists to answer a question about the D50 THREADED harness rather than about ferrodb: N Python
threads share one GIL, and per-statement wire parsing is Python bytecode, so a threaded client can
become the ceiling and show up as BOTH arms plateauing together. Run N of these as separate
PROCESSES and the GIL is no longer shared; if the total rises, the threaded harness was the wall.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tests", "pg"))
from pg_client import Conn  # noqa: E402

host, port, key, secs, warm = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), 0.3
c = Conn(host, port)
c.startup()
sql = f"SELECT v FROM t WHERE id = {key};"
t0 = time.monotonic()
while time.monotonic() - t0 < warm:
    c.query(sql)
iters = rows = errs = 0
t0 = time.monotonic()
while time.monotonic() - t0 < secs:
    _f, r, _t, e = c.query(sql)
    rows += len(r)
    errs += len(e)
    iters += 1
print(f"{iters} {rows} {errs}")
c.terminate()
