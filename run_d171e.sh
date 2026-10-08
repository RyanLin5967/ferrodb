#!/bin/bash
# D171 arms D and E — reproduce d71_point_update_curve.txt's "second wall" sweep exactly, and
# read it as an ADDITIVE EXCESS as well as a ratio. Registered in bench/d171_prereg.txt A3/A4,
# both committed BEFORE this run.
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-d170-adversary || exit 1
OUT=bench/d171_stage_cost_armE.txt
: > "$OUT"

~/wt/logs/measure-lock.sh acquire d171e >>"$OUT" 2>&1 || {
  echo "LOCK NOT ACQUIRED — NO NUMBER IS REPORTED" >>"$OUT"; echo "D171E_COMPLETE" >>"$OUT"; exit 1; }
trap '~/wt/logs/measure-lock.sh release d171e >>"'"$OUT"'" 2>&1; echo "D171E_COMPLETE" >>"'"$OUT"'"' EXIT

{
  echo "host: $(uname -sm)  started: $(date -u +%FT%TZ)"
  echo "load: $(uptime | sed 's/.*averages*: *//')"
  echo "tree: $(git rev-parse HEAD)  branch d170-adversary"
  echo ""
  echo "REPRODUCING bench/d71_point_update_curve.txt's 'A SECOND WALL I PROPOSED AND THEN KILLED"
  echo "BY MEASURING IT': fixed 4000-row table, N updates in session, N = 50..3200. Its numbers:"
  echo "   50 -> 0.708 ms   100 -> 0.695   200 -> 0.717   400 -> 0.765"
  echo "  800 -> 0.762      1600 -> 0.790  3200 -> 1.027   => it read 1.45x and concluded"
  echo "  'THE HYPOTHESIS IS DEAD ... no session-size wall and no quadratic term.'"
} >>"$OUT" 2>&1

timeout 1800 cargo build --release --example d171_stage_cost >>"$OUT" 2>&1 || {
  echo "BUILD FAILED — NO RESULT" >>"$OUT"; exit 1; }
B=target/release/examples/d171_stage_cost
AXIS=50,100,200,400,800,1600,3200

cell() { # $1 label, $2 STUB(1/0), $3 NOPUSHDOWN(1/0), $4 arm, $5 rows
  echo "" >>"$OUT"
  echo "=== $1 — load $(uptime | sed 's/.*averages*: *//' | awk '{print $1}') ===" >>"$OUT"
  if [ "$2" = "1" ]; then
    timeout 3600 env D171_STUB=1 D171_ROWS=$5 D171_AXIS=$AXIS D171_ARM=$4 D171_W=50 D170_NOPUSHDOWN=$3 "$B" >>"$OUT" 2>&1
  else
    timeout 3600 env D171_ROWS=$5 D171_AXIS=$AXIS D171_ARM=$4 D171_W=50 D170_NOPUSHDOWN=$3 "$B" >>"$OUT" 2>&1
  fi
  echo "rc=$?" >>"$OUT"
}

# E: their configuration exactly (stub + no pushdown), then each variable flipped one at a time.
cell "E1 STUB  + NOPUSHDOWN + arm A (W=S) — THEIR SWEEP, REPRODUCED"  1 1 A 4000
cell "E2 STUB  + NOPUSHDOWN + arm B (W=50 fixed) — S alone"           1 1 B 4000
cell "E3 STUB  + pushdown   + arm A (W=S)"                            1 0 A 4000
cell "E4 STUB  + pushdown   + arm B (W=50 fixed) — S alone"           1 0 B 4000
cell "E5 ARENA + NOPUSHDOWN + arm B (W=50 fixed) — S alone"           0 1 B 4000
cell "E6 ARENA + pushdown   + arm B (W=50 fixed) — S alone"           0 0 B 4000

echo "" >>"$OUT"
echo "finished: $(date -u +%FT%TZ)  load: $(uptime | sed 's/.*averages*: *//')" >>"$OUT"
