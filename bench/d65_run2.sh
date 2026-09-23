#!/bin/bash
# D65 run 2 — one lock hold across build + smoke + run. Lane d65-reopen.
set -u
LOCK=/tmp/ferrodb-suite.lock; ME=""; SAMP=""
W=/Users/idide/wt/ferrodb-d65-reopen-curve
L=/Users/idide/wt/logs/d65-reopen
OUT=${D65_OUT:-$W/bench/d65_reopen_curve_1e6_run2.txt}
PIDF=$L/run.pid
rel(){ [ -n "$ME" ] && [ "$(cat $LOCK/owner 2>/dev/null)" = "$ME" ] && rm -rf "$LOCK"; }
cleanup(){ [ -n "$SAMP" ] && kill "$SAMP" 2>/dev/null; rel; [ "$(cat "$PIDF" 2>/dev/null)" = "$$" ] && { rm -rf "$L/tmp"; rm -f "$PIDF"; }; echo "$(date -u +%FT%TZ) released (pid $$)" >> "$L/status"; }
trap cleanup EXIT
for s in TERM INT HUP; do trap "echo \"\$(date -u +%FT%TZ) got SIG$s (ppid \$PPID)\" >> \"\$L/status\"; exit 143" $s; done

# >>> single-instance guard (added after two copies of this lane each launched this script) >>>
# Refuses rather than warns, and runs BEFORE the lock wait, so a duplicate never even queues.
#  (1) ONE LIVE INSTANCE: $PIDF is created with O_EXCL (noclobber). If it exists and its pid is
#      alive, refuse. If its pid is dead, take over. If it cannot be parsed, refuse and say so.
#  (2) ONE RUN PER ARTIFACT: if $OUT already carries a run header ("[HERE]" at line start), refuse;
#      a re-run needs a NEW path via D65_OUT. Checked again right before appending.
# Blind spots, stated: kill -0 says only that SOME process has that pid — a recycled pid makes a
# dead holder look alive, which REFUSES (fails safe, never duplicates). The guard cannot see a
# launch of a DIFFERENT script at the same artifact; (2) catches that only once it has appended.
claim(){ ( set -o noclobber; echo "$$" > "$PIDF" ) 2>/dev/null; }
refuse(){ echo "$(date -u +%FT%TZ) REFUSED launch pid $$: $1" >> "$L/status"; echo "REFUSED: $1" >&2; exit "$2"; }
if ! claim; then
  op=$(cat "$PIDF" 2>/dev/null)
  case "$op" in ''|*[!0-9]*) refuse "$PIDF exists but holds '$op', not a pid; resolve by hand" 9 ;; esac
  kill -0 "$op" 2>/dev/null && refuse "run pid $op named in $PIDF is alive" 9
  echo "$(date -u +%FT%TZ) pid $$: $PIDF named dead pid $op; taking over" >> "$L/status"
  rm -f "$PIDF"; claim || refuse "lost the race to re-claim $PIDF" 9
fi
has_run(){ grep -q '^\[HERE\]' "$OUT" 2>/dev/null; }
has_run && { rm -f "$PIDF"; refuse "$OUT already holds a run header; one run, one path - set D65_OUT" 10; }
# <<< single-instance guard <<<
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

has_run && refuse "$OUT gained a run header while this launch waited" 10
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
