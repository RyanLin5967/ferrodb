#!/usr/bin/env bash
# D50-FERRODB-SNAPSHOT-COST runner.
#
# Starts N ferrodb pgwire servers (one database each), seeds each, runs the two-arm sweep, and
# KILLS EVERY SERVER IT STARTED on any exit path. That last part is not decoration: this repo has
# already frozen the machine twice with a load harness that left 317 orphans behind, so the pids
# are tracked explicitly and the trap fires on EXIT, INT and TERM.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.."

N=${1:-16}
DIR=$(mktemp -d /tmp/d50_XXXXXX)
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  # Prove it, rather than trusting the signal: a survivor here is the failure mode being guarded.
  sleep 1
  left=0
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill -0 "$p" 2>/dev/null && left=$((left+1)); done
  echo "# cleanup: ${#PIDS[@]} servers started, $left still alive after SIGTERM"
  [ "$left" -gt 0 ] && { for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done; echo "# escalated to SIGKILL"; }
  rm -rf "$DIR"
}
trap cleanup EXIT INT TERM

PORTS=""
for i in $(seq 0 $((N-1))); do
  mkdir -p "$DIR/db$i"
  ./target/release/examples/pgserver "$DIR/db$i/main.db" 127.0.0.1:0 > "$DIR/srv$i.out" 2>&1 &
  PIDS+=($!)
done

# Wait on the ARTIFACT (the printed address), never on a sleep-and-hope.
for i in $(seq 0 $((N-1))); do
  for _ in $(seq 1 200); do
    addr=$(grep -oE '127\.0\.0\.1:[0-9]+' "$DIR/srv$i.out" 2>/dev/null | head -1)
    [ -n "$addr" ] && break
    sleep 0.1
  done
  [ -z "$addr" ] && { echo "REFUSED: server $i never printed a bound address"; exit 2; }
  PORTS="${PORTS:+$PORTS,}${addr##*:}"
done
echo "# $N servers up on ports $PORTS"

# Seed every database identically.
python3 - "$PORTS" <<'PY'
import os, sys
sys.path.insert(0, os.path.join("tests", "pg"))
from pg_client import Conn
for port in sys.argv[1].split(","):
    c = Conn("127.0.0.1", int(port)); c.startup()
    # INTEGER NOT NULL, not INT: the first version used INT, the CREATE errored, its error was
    # not read, and the failure surfaced two statements later as "this database has no tables
    # yet". Every seeding statement is error-checked now, at the statement that produced it.
    _f, _r, _t, e = c.query("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);")
    if e:
        sys.exit(f"GUARD: CREATE TABLE on port {port} errored: {e}")
    for i in range(1, 201):
        _f, _r, _t, e = c.query(f"INSERT INTO t VALUES ({i}, {i*7});")
        if e:
            sys.exit(f"GUARD: INSERT {i} on port {port} errored: {e}")
    _f, rows, _t, errors = c.query("SELECT v FROM t WHERE id = 7;")
    if errors:
        sys.exit(f"GUARD: seed check on port {port} errored: {errors}")
    if len(rows) != 1:
        sys.exit(f"GUARD: seed check on port {port} returned {len(rows)} rows, expected 1")
    c.terminate()
print("# seeded and verified: every server answers the probe query with exactly 1 row")
PY
[ $? -ne 0 ] && exit 2

timeout 900 python3 bench/d50_reader_scaling.py 127.0.0.1 "$PORTS"
