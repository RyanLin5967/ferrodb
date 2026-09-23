#!/bin/bash
# D170 ADVERSARY — build once, then A/B the pushdown IN ONE BINARY on the ARENA path.
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-d170-adversary || exit 1
OUT=bench/d170_adversary_probe.txt
: > "$OUT"

~/wt/logs/measure-lock.sh acquire d170-adversary >>"$OUT" 2>&1 || {
  echo "LOCK NOT ACQUIRED — NO NUMBER IS REPORTED" >>"$OUT"; echo "D170_ADV_ABORT" >>"$OUT"; exit 1; }
trap '~/wt/logs/measure-lock.sh release d170-adversary >>"'"$OUT"'" 2>&1; echo "D170_ADV_COMPLETE" >>"'"$OUT"'"' EXIT

{
  echo "host: $(uname -sm)  started: $(date -u +%FT%TZ)"
  echo "load: $(uptime | sed 's/.*averages*: *//')"
  echo "tree: $(git rev-parse HEAD)  (worktree d170-adversary, branch_update instrumented)"
  echo "df:  $(df -h /System/Volumes/Data | tail -1)"
} >>"$OUT" 2>&1

echo "=== BUILD ===" >>"$OUT"
timeout 1800 cargo build --release --example d170_adv_probe >>"$OUT" 2>&1 || {
  echo "BUILD FAILED — NO RESULT" >>"$OUT"; exit 1; }
echo "BUILD OK" >>"$OUT"

B=target/release/examples/d170_adv_probe

# Q1+Q2 counterbalanced: HEAD, NOPUSH, NOPUSH, HEAD. Arm order alternates so drift cannot
# favour one side, which is the method d71_pushdown_fix.txt used and d167 was faulted for skipping.
for pair in 1 2; do
  for mode in a b; do
    if [ "$pair$mode" = "1a" ] || [ "$pair$mode" = "2b" ]; then NP=0; TAG="HEAD (pushdown ON)"; else NP=1; TAG="NOPUSHDOWN (pre-e6958d6)"; fi
    echo "" >>"$OUT"
    echo "=== PAIR $pair — STAGED — $TAG — load $(uptime | sed 's/.*averages*: *//' | awk '{print $1}') ===" >>"$OUT"
    timeout 1200 env D71_ARM=staged D170_NOPUSHDOWN=$NP "$B" >>"$OUT" 2>&1
    echo "rc=$?" >>"$OUT"
  done
done

# Q3 — the axis. Fixed 4000-row table, sweep the number of staged rows, both states.
for NP in 0 1; do
  echo "" >>"$OUT"
  echo "=== Q3 N-SWEEP — STAGED — D170_NOPUSHDOWN=$NP — load $(uptime | sed 's/.*averages*: *//' | awk '{print $1}') ===" >>"$OUT"
  timeout 1200 env D71_ARM=staged D170_NSWEEP=1 D170_ROWS=4000 D170_NOPUSHDOWN=$NP "$B" >>"$OUT" 2>&1
  echo "rc=$?" >>"$OUT"
done

# PLAIN control, once: it must NOT move between pushdown states, because ordinary DML never
# touches the runtime. If it does move, the A/B is contaminated and Q2 is void.
for NP in 0 1; do
  echo "" >>"$OUT"
  echo "=== PLAIN CONTROL — D170_NOPUSHDOWN=$NP (must be unaffected) ===" >>"$OUT"
  timeout 1200 env -u D71_ARM D170_NOPUSHDOWN=$NP "$B" >>"$OUT" 2>&1
  echo "rc=$?" >>"$OUT"
done

echo "" >>"$OUT"
echo "finished: $(date -u +%FT%TZ)  load: $(uptime | sed 's/.*averages*: *//')" >>"$OUT"
