#!/bin/bash
# D65 run 2 — one lock hold across build + smoke + run. Lane d65-reopen.
set -u
LOCK=/tmp/ferrodb-suite.lock; ME=""; SAMP=""
W=/Users/idide/wt/ferrodb-d65-reopen-curve
L=/Users/idide/wt/logs/d65-reopen
OUT=$W/bench/d65_reopen_curve_1e6_run2.txt
rel(){ [ -n "$ME" ] && [ "$(cat $LOCK/owner 2>/dev/null)" = "$ME" ] && rm -rf "$LOCK"; }
cleanup(){ [ -n "$SAMP" ] && kill "$SAMP" 2>/dev/null; rm -rf "$L/tmp"; rel; echo "$(date -u +%FT%TZ) released" >> "$L/status"; }
trap cleanup EXIT
for s in TERM INT HUP; do trap "echo \"\$(date -u +%FT%TZ) got SIG$s (ppid \$PPID)\" >> \"\$L/status\"; exit 143" $s; done
echo "$(date -u +%FT%TZ) waiting for lock (pid $$)" >> "$L/status"
until mkdir "$LOCK" 2>/dev/null; do o=$(awk '{print $1}' "$LOCK/owner" 2>/dev/null)
  [ -n "$o" ] && ! kill -0 "$o" 2>/dev/null && { rm -rf "$LOCK"; continue; }; sleep 5; done
ME="$$ d65-reopen $(date -u +%FT%TZ)"; printf '%s\n' "$ME" > "$LOCK/owner"
echo "$(date -u +%FT%TZ) ACQUIRED: $ME" >> "$L/status"
export PATH="$HOME/.cargo/bin:$PATH"
cd "$W" || exit 3
( while :; do echo "$(date -u +%T) $(uptime | sed 's/.*averages*: *//')" >> "$L/load.txt"; sleep 30; done ) & SAMP=$!

echo "$(date -u +%FT%TZ) build start, HEAD $(git rev-parse --short HEAD), dirty=[$(git status --short | tr '\n' ' ')]" >> "$L/status"
timeout 1500 cargo build --release --example branch_curve_writes > "$L/build.log" 2>&1; brc=$?
echo "$(date -u +%FT%TZ) build rc=$brc" >> "$L/status"
[ $brc -ne 0 ] && exit 4
BIN=$W/target/release/examples/branch_curve_writes
mkdir -p "$L/tmp"; export TMPDIR="$L/tmp/"

timeout 300 "$BIN" 1000,2000 8 1 > "$L/smoke.txt" 2>&1; src=$?
echo "$(date -u +%FT%TZ) smoke rc=$src" >> "$L/status"
[ $src -ne 0 ] && exit 5
grep -q "reload ctl" "$L/smoke.txt" || { echo "smoke: binary lacks the amended control column" >> "$L/status"; exit 6; }
grep -q "reload-control negatives at N=" "$L/smoke.txt" || { echo "smoke: negative controls did not print" >> "$L/status"; exit 6; }
# POSITIVE check, not absence of a failure word: exactly 2 data rows, each with reload ctl == ok
# and cat live@re == N+1 and arena re pg == N.
good=$(awk '$1 ~ /^[0-9]+$/ && NF>=14 && $14=="ok" && $10==$1+1 && $13==$1 {n++} END{print n+0}' "$L/smoke.txt")
rows=$(awk '$1 ~ /^[0-9]+$/ && NF>=14 {n++} END{print n+0}' "$L/smoke.txt")
echo "$(date -u +%FT%TZ) smoke rows=$rows good=$good" >> "$L/status"
[ "$rows" = 2 ] && [ "$good" = 2 ] || { echo "smoke: controls did not all pass, not running the curve" >> "$L/status"; exit 7; }
rm -rf "$L/tmp"/*

{
  echo "[HERE] $(date -u +%FT%TZ)  binary built from $(git rev-parse --short HEAD) (tree dirty=[$(git status --short | tr '\n' ' ')])"
  echo "suite lock owner: $(cat $LOCK/owner)   measure lock: $(/Users/idide/wt/logs/measure-lock.sh status 2>&1 | tr '\n' ' ')"
  echo "free before: $(df -h "$L" | awk 'NR==2{print $4}')   load before: $(uptime | sed 's/.*averages*: *//')   other cargo pids: [$(pgrep -x cargo | tr '\n' ' ')]"
  echo
} >> "$OUT"
echo "$(date -u +%FT%TZ) curve start" >> "$L/status"
timeout 5400 "$BIN" 1000,10000,100000,250000,500000,750000,1000000 8 8 >> "$OUT" 2>&1; rc=$?
{
  echo "# rc=$rc"
  echo "free after: $(df -h "$L" | awk 'NR==2{print $4}')   load after: $(uptime | sed 's/.*averages*: *//')   at $(date -u +%FT%TZ)"
} >> "$OUT"
{ echo; echo "=== LOAD LOG (uptime every 30 s, whole lock hold incl. build) ==="; cat "$L/load.txt"; } >> "$OUT"
echo "$(date -u +%FT%TZ) curve rc=$rc" >> "$L/status"
exit 0
