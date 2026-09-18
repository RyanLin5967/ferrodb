#!/usr/bin/env bash
# D50 discriminator: N client PROCESSES (no shared GIL) against ONE server, vs the threaded arm.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.."
DIR=$(mktemp -d /tmp/d50p_XXXXXX); SRV=""
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null; sleep 0.5
            [ -n "$SRV" ] && kill -0 "$SRV" 2>/dev/null && { kill -9 "$SRV" 2>/dev/null; echo "# escalated to SIGKILL"; }
            echo "# cleanup: server $SRV down"; rm -rf "$DIR"; }
trap cleanup EXIT INT TERM
./target/release/examples/pgserver "$DIR/main.db" 127.0.0.1:0 > "$DIR/srv.out" 2>&1 &
SRV=$!
for _ in $(seq 1 200); do addr=$(grep -oE '127\.0\.0\.1:[0-9]+' "$DIR/srv.out" 2>/dev/null | head -1); [ -n "$addr" ] && break; sleep 0.1; done
[ -z "$addr" ] && { echo "REFUSED: no bound address"; exit 2; }
PORT=${addr##*:}
python3 - "$PORT" <<'PY'
import os, sys
sys.path.insert(0, os.path.join("tests", "pg"))
from pg_client import Conn
c = Conn("127.0.0.1", int(sys.argv[1])); c.startup()
for sql in ["CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);"] + [f"INSERT INTO t VALUES ({i}, {i*7});" for i in range(1, 201)]:
    _f, _r, _t, e = c.query(sql)
    if e: sys.exit(f"GUARD: seeding failed: {e}")
_f, r, _t, e = c.query("SELECT v FROM t WHERE id = 7;")
if e or len(r) != 1: sys.exit(f"GUARD: probe returned {len(r)} rows, errors {e}")
print("# seeded and verified")
PY
[ $? -ne 0 ] && exit 2
echo "# clients   total_stmt_s   per_client   total_vs_1C   (SEPARATE PROCESSES, one server)"
BASE=""
for N in 1 2 4 8 16; do
  OUT=$(mktemp)
  # Collect the CLIENT pids and wait on THOSE. A bare `wait` waits for every child of this
  # shell, and one of them is the SERVER, which never exits -- so the first version of this
  # loop hung forever at N=1 with the header already printed, which reads exactly like a slow
  # run. Wait on what you started, never on "everything".
  CPIDS=()
  for i in $(seq 0 $((N-1))); do
    python3 bench/d50_one_client.py 127.0.0.1 "$PORT" $(( (i*97) % 200 + 1 )) 2 >> "$OUT" &
    CPIDS+=($!)
  done
  for p in "${CPIDS[@]}"; do wait "$p"; done
  tot=$(awk '{i+=$1; r+=$2; e+=$3} END{print i" "r" "e}' "$OUT")
  set -- $tot; iters=$1; rows=$2; errs=$3
  lines=$(wc -l < "$OUT"); rm -f "$OUT"
  [ "$lines" -ne "$N" ] && { echo "GUARD: $lines of $N clients reported"; exit 2; }
  [ "$errs" -ne 0 ] && { echo "GUARD: $errs protocol errors"; exit 2; }
  [ "$rows" -ne "$iters" ] && { echo "GUARD: $iters statements returned $rows rows"; exit 2; }
  ops=$(echo "$iters 2" | awk '{printf "%.0f", $1/$2}')
  [ -z "$BASE" ] && BASE=$ops
  echo "$N $ops $(echo "$ops $N" | awk '{printf "%.0f", $1/$2}') $(echo "$ops $BASE" | awk '{printf "%.3f", $1/$2}')" | awk '{printf "%8s %14s %12s %13s\n", $1, $2, $3, $4}'
done
