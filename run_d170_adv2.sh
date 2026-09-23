#!/bin/bash
# D170 ADVERSARY, ROUND 2 — the full 2x2 (stub|arena) x (pushdown|no pushdown) in ONE binary,
# plus the overlay-arm counters and an extended D71_N axis. Order reversed between blocks so
# drift cannot favour one side.
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-d170-adversary || exit 1
OUT=bench/d170_adversary_probe2.txt
: > "$OUT"

~/wt/logs/measure-lock.sh acquire d170-adversary2 >>"$OUT" 2>&1 || {
  echo "LOCK NOT ACQUIRED — NO NUMBER IS REPORTED" >>"$OUT"; echo "D170_ADV2_COMPLETE" >>"$OUT"; exit 1; }
trap '~/wt/logs/measure-lock.sh release d170-adversary2 >>"'"$OUT"'" 2>&1; echo "D170_ADV2_COMPLETE" >>"'"$OUT"'"' EXIT

{
  echo "host: $(uname -sm)  started: $(date -u +%FT%TZ)"
  echo "load: $(uptime | sed 's/.*averages*: *//')"
  echo "tree: $(git rev-parse HEAD)  (worktree d170-adversary, instrumented)"
} >>"$OUT" 2>&1

timeout 1800 cargo build --release --example d170_adv_probe >>"$OUT" 2>&1 || {
  echo "BUILD FAILED — NO RESULT" >>"$OUT"; exit 1; }
B=target/release/examples/d170_adv_probe

echo "" >>"$OUT"; echo "=== FIRE-CHECK: the overlay-arm detector must report all three arms ===" >>"$OUT"
timeout 300 env D71_ARM=staged D170_FIRECHECK=1 "$B" >>"$OUT" 2>&1; echo "rc=$?" >>"$OUT"

run() { # $1 = label, $2 = STUB|ARENA, $3 = nopushdown 0|1
  echo "" >>"$OUT"
  echo "=== $1 — load $(uptime | sed 's/.*averages*: *//' | awk '{print $1}') ===" >>"$OUT"
  if [ "$2" = "STUB" ]; then
    timeout 1200 env D71_ARM=staged D170_STUB=1 D170_NOPUSHDOWN=$3 "$B" >>"$OUT" 2>&1
  else
    timeout 1200 env D71_ARM=staged D170_NOPUSHDOWN=$3 "$B" >>"$OUT" 2>&1
  fi
  echo "rc=$?" >>"$OUT"
}

echo "" >>"$OUT"; echo "############ BLOCK A (forward order) ############" >>"$OUT"
run "A1 STUB  + pushdown ON"   STUB  0
run "A2 STUB  + NOPUSHDOWN"    STUB  1
run "A3 ARENA + NOPUSHDOWN"    ARENA 1
run "A4 ARENA + pushdown ON"   ARENA 0

echo "" >>"$OUT"; echo "############ BLOCK B (reversed order) ############" >>"$OUT"
run "B1 ARENA + pushdown ON"   ARENA 0
run "B2 ARENA + NOPUSHDOWN"    ARENA 1
run "B3 STUB  + NOPUSHDOWN"    STUB  1
run "B4 STUB  + pushdown ON"   STUB  0

# The D71_N axis, pushed to 12800 staged rows as requested, with the arm counters beside it.
for NP in 0 1; do
  echo "" >>"$OUT"
  echo "=== N-SWEEP to 12800 — ARENA — NOPUSHDOWN=$NP — load $(uptime | sed 's/.*averages*: *//' | awk '{print $1}') ===" >>"$OUT"
  timeout 1800 env D71_ARM=staged D170_NSWEEP=1 D170_ROWS=4000 D170_NOPUSHDOWN=$NP "$B" >>"$OUT" 2>&1
  echo "rc=$?" >>"$OUT"
done

echo "" >>"$OUT"
echo "finished: $(date -u +%FT%TZ)  load: $(uptime | sed 's/.*averages*: *//')" >>"$OUT"
