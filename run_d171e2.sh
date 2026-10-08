#!/bin/bash
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-d170-adversary || exit 1
OUT=bench/d171_stage_cost_armE2.txt
: > "$OUT"
~/wt/logs/measure-lock.sh acquire d171e2 >>"$OUT" 2>&1 || {
  echo "LOCK NOT ACQUIRED — NO NUMBER IS REPORTED" >>"$OUT"; echo "D171E2_COMPLETE" >>"$OUT"; exit 1; }
trap '~/wt/logs/measure-lock.sh release d171e2 >>"'"$OUT"'" 2>&1; echo "D171E2_COMPLETE" >>"'"$OUT"'"' EXIT
{ echo "host: $(uname -sm)  started: $(date -u +%FT%TZ)"
  echo "load: $(uptime | sed 's/.*averages*: *//')"
  echo "tree: $(git rev-parse HEAD)"; } >>"$OUT" 2>&1
timeout 1800 cargo build --release --example d171_stage_cost >>"$OUT" 2>&1 || { echo "BUILD FAILED" >>"$OUT"; exit 1; }
B=target/release/examples/d171_stage_cost
AXIS=50,100,200,400,800,1600,3200
c() { echo "" >>"$OUT"; echo "=== $1 — load $(uptime | sed 's/.*averages*: *//' | awk '{print $1}') ===" >>"$OUT"
  shift; timeout 3600 env "$@" D171_ROWS=4000 D171_AXIS=$AXIS D171_W=50 "$B" >>"$OUT" 2>&1; echo "rc=$?" >>"$OUT"; }
c "E1 STUB + NOPUSHDOWN + arm A (W=S) — THEIR SWEEP, REPRODUCED" D171_STUB=1 D170_NOPUSHDOWN=1 D171_ARM=A
c "E2 STUB + NOPUSHDOWN + arm B (W=50 fixed) — S alone"          D171_STUB=1 D170_NOPUSHDOWN=1 D171_ARM=B
c "E5 ARENA + NOPUSHDOWN + arm B (W=50 fixed) — S alone"         D170_NOPUSHDOWN=1 D171_ARM=B
echo "" >>"$OUT"; echo "finished: $(date -u +%FT%TZ)  load: $(uptime | sed 's/.*averages*: *//')" >>"$OUT"
