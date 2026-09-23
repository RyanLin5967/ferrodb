#!/bin/bash
# D171 — separate W (staged rows) from S (statements). Prereg: bench/d171_prereg.txt (8b861c7,
# amended 690bb75, both BEFORE any measurement here).
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-d170-adversary || exit 1
OUT=bench/d171_stage_cost.txt
: > "$OUT"

~/wt/logs/measure-lock.sh acquire d171 >>"$OUT" 2>&1 || {
  echo "LOCK NOT ACQUIRED — NO NUMBER IS REPORTED" >>"$OUT"; echo "D171_COMPLETE" >>"$OUT"; exit 1; }
trap '~/wt/logs/measure-lock.sh release d171 >>"'"$OUT"'" 2>&1; echo "D171_COMPLETE" >>"'"$OUT"'"' EXIT

{
  echo "host: $(uname -sm)  started: $(date -u +%FT%TZ)"
  echo "load: $(uptime | sed 's/.*averages*: *//')"
  echo "tree: $(git rev-parse HEAD)  branch d170-adversary"
} >>"$OUT" 2>&1

timeout 1800 cargo build --release --example d171_stage_cost >>"$OUT" 2>&1 || {
  echo "BUILD FAILED — NO RESULT" >>"$OUT"; exit 1; }
B=target/release/examples/d171_stage_cost
ROWS=16000
AXIS=100,400,1600,6400,12800

arm() { # $1 = arm letter, $2 = pass label
  echo "" >>"$OUT"
  echo "=== ARM $1 — pass $2 — load $(uptime | sed 's/.*averages*: *//' | awk '{print $1}') ===" >>"$OUT"
  timeout 3600 env D171_ROWS=$ROWS D171_AXIS=$AXIS D171_ARM=$1 D171_W=100 D171_S=12800 "$B" >>"$OUT" 2>&1
  echo "rc=$?" >>"$OUT"
}

# Order reversed between passes so drift cannot favour an arm.
echo "" >>"$OUT"; echo "######## PASS 1 (A,B,C) ########" >>"$OUT"
arm A 1
arm B 1
arm C 1
echo "" >>"$OUT"; echo "######## PASS 2 (C,B,A) ########" >>"$OUT"
arm C 2
arm B 2
arm A 2

echo "" >>"$OUT"
echo "finished: $(date -u +%FT%TZ)  load: $(uptime | sed 's/.*averages*: *//')" >>"$OUT"
