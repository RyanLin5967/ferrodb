#!/usr/bin/env bash
# bench/d42_fire_channel.sh — force verify-suite.sh's INCONCLUSIVE channel to fire, end to end.
#
# tools/verify-suite-selftest.sh part 3 checks two of the three things that can rot: that the marker
# strings agree between the Rust source and the shell script, and that certify-head.sh refuses an
# INCONCLUSIVE directory while still accepting an honest green one. It does NOT check the middle
# link — that verify-suite.sh, running a REAL suite whose log contains a REAL marker, refuses.
# A detector nobody has made fire is not a clean result, so this forces that link.
#
# HOW. In a THROWAWAY worktree (given as $1), every tests/*.rs except the failover target is moved
# aside, so the per-target sweep is `--lib` plus one integration target instead of the whole suite.
# Then the same keeper used by bench/d42_fire_starved_children.sh SIGSTOPs the consensus nodes, the
# target earns INCONCLUSIVE, and this script requires that verify-suite.sh:
#
#   * exits 4, the code reserved for this refusal,
#   * prints NO `passed=` total — a starved run is not a number,
#   * writes NO SUMMARY.txt — so nothing downstream can certify it,
#   * writes INCONCLUSIVE.txt — so certify-head.sh can refuse it,
#   * and is then actually refused by certify-head.sh.
#
# ANTI-VACUITY: it first requires that the keeper stopped something and that the marker really
# reached the log. Without both, a refusal could be coming from some other guard entirely and this
# run would prove nothing.
#
# Usage: bench/d42_fire_channel.sh /path/to/throwaway/worktree
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
SELF_DIR=$(cd "$(dirname "$0")/.." && pwd -P)

TREE=${1:-}
[ -n "$TREE" ] || { echo "REFUSING — no throwaway worktree given. Usage: $0 <worktree>"; exit 1; }
TREE=$(cd "$TREE" 2>/dev/null && pwd -P) || { echo "REFUSING — '$1' does not exist"; exit 1; }
[ "$TREE" = "$SELF_DIR" ] && { echo "REFUSING — that is this script's OWN tree; pass a throwaway."; exit 1; }
[ -f "$TREE/tools/verify-suite.sh" ] || { echo "REFUSING — $TREE has no tools/verify-suite.sh"; exit 1; }

BIN="$TREE/target/debug/examples/consensus_node"
PAT="^$BIN "
HOLD=${HOLD:-90}
OUTDIR="$TREE/../d42-channel-out.$$"
STOPLOG=$(mktemp)
keeper=""
cleanup() {
    [ -n "$keeper" ] && kill -9 "$keeper" 2>/dev/null
    for p in $(pgrep -f "$PAT" 2>/dev/null); do kill -CONT "$p" 2>/dev/null; kill -9 "$p" 2>/dev/null; done
    rm -f "$STOPLOG"
    return 0
}
trap cleanup EXIT INT TERM

cd "$TREE" || exit 1
cargo build --examples >/dev/null 2>&1 || { echo "REFUSING — cargo build --examples failed"; exit 1; }
[ -x "$BIN" ] || { echo "REFUSING — $BIN is missing"; exit 1; }
if [ -n "$(pgrep -f "$PAT" 2>/dev/null)" ]; then
    echo "REFUSING — consensus_node processes from $TREE are already running."
    exit 1
fi

# Shrink the sweep to `--lib` plus the one target. Moved, not deleted, so the throwaway stays
# inspectable; the whole tree is discarded afterwards anyway.
mkdir -p "$TREE/.d42-parked"
moved=0
for f in "$TREE"/tests/*.rs; do
    case "$(basename "$f")" in
        integration_consensus_failover.rs) ;;
        *) mv "$f" "$TREE/.d42-parked/" && moved=$((moved+1)) ;;
    esac
done

echo "D42 channel fire-check — verify-suite.sh must REFUSE a starved run"
echo "  when     : $(date -u +%FT%TZ)"
echo "  tree     : $TREE"
echo "  head     : $(git log -1 --format=%h)"
echo "  parked   : $moved other tests/*.rs, leaving --lib + integration_consensus_failover"
echo "  loadavg  : $(uptime | sed 's/.*load averages*: //')"
echo ""

(
    n=0
    while [ "$n" -lt 3 ]; do n=$(pgrep -f "$PAT" 2>/dev/null | wc -l | tr -d ' '); sleep 0.05; done
    pids=$(pgrep -f "$PAT" 2>/dev/null | tr '\n' ',' | sed 's/,$//')
    i=0; lis=0
    while [ "$lis" -lt 3 ] && [ "$i" -lt 600 ]; do
        lis=$(lsof -nP -iTCP -sTCP:LISTEN -a -p "$pids" 2>/dev/null | grep -c LISTEN)
        [ "$lis" -lt 3 ] && sleep 0.05
        i=$((i+1))
    done
    got=$(pgrep -f "$PAT" 2>/dev/null | tr '\n' ' ')
    c=0; for p in $got; do kill -STOP "$p" 2>/dev/null && c=$((c+1)); done
    echo "$(date -u +%T) STOPPED $c pid(s) ($lis of 3 were listening): $got" >> "$STOPLOG"
    e=0
    while [ "$e" -lt $((HOLD * 2)) ]; do
        for p in $(pgrep -f "$PAT" 2>/dev/null); do kill -STOP "$p" 2>/dev/null; done
        sleep 0.5; e=$((e+1))
    done
    for p in $(pgrep -f "$PAT" 2>/dev/null); do kill -CONT "$p" 2>/dev/null; done
    echo "$(date -u +%T) CONTINUED after ${HOLD}s" >> "$STOPLOG"
) &
keeper=$!
disown $keeper 2>/dev/null

echo "---------------- tools/verify-suite.sh d42-channel (per-target) ----------------"
VOUT=$(VERIFY_MODE=per-target VERIFY_OUT="$OUTDIR" VERIFY_TIMEOUT=1200 \
       timeout 2400 bash tools/verify-suite.sh d42-channel 2>&1)
vrc=$?
kill -9 "$keeper" 2>/dev/null; keeper=""
for p in $(pgrep -f "$PAT" 2>/dev/null); do kill -CONT "$p" 2>/dev/null; done
echo "$VOUT"
echo "---------------- exit $vrc ----------------"
echo ""
echo "keeper log:"; sed 's/^/   /' "$STOPLOG"
echo ""

LOG=$(ls "$OUTDIR"/suite-d42-channel.log 2>/dev/null | head -1)
fails=0
say_ok()  { echo "ok:   $*"; }
say_bad() { echo "⛔    $*"; fails=$((fails+1)); }

# ---- anti-vacuity: the refusal must be THIS one, not some other guard ----
grep -q 'STOPPED [1-9]' "$STOPLOG" \
    && say_ok "the keeper actually stopped node processes" \
    || say_bad "the keeper stopped nothing — whatever refused below, it was not starvation"
if [ -n "$LOG" ] && grep -qF 'FERRODB-VERDICT: INCONCLUSIVE' "$LOG"; then
    say_ok "the marker really reached the suite log"
else
    say_bad "no INCONCLUSIVE marker in the suite log — the channel was never given anything to detect"
fi

# ---- the channel itself ----
[ "$vrc" = "4" ] && say_ok "verify-suite.sh exited 4 (the reserved INCONCLUSIVE refusal)" \
                 || say_bad "verify-suite.sh exited $vrc, expected 4"
grep -qE 'passed=[0-9]+' <<<"$VOUT" && say_bad "it printed a total anyway — a starved run is not a number" \
                                    || say_ok "it printed no total"
grep -q 'REFUSING' <<<"$VOUT" && say_ok "it printed a refusal" \
                              || say_bad "no REFUSING line"
[ -f "$OUTDIR/SUMMARY.txt" ] && say_bad "SUMMARY.txt was written — a starved run could be certified" \
                             || say_ok "no SUMMARY.txt was written"
[ -f "$OUTDIR/INCONCLUSIVE.txt" ] && say_ok "INCONCLUSIVE.txt was written: $(head -1 "$OUTDIR/INCONCLUSIVE.txt")" \
                                  || say_bad "no INCONCLUSIVE.txt — certify-head would have nothing to refuse on"

# ---- and the landing guard must refuse it ----
if bash tools/certify-head.sh "$OUTDIR" HEAD >/dev/null 2>&1; then
    say_bad "certify-head.sh CERTIFIED the starved run"
else
    say_ok "certify-head.sh refused the starved run: $(bash tools/certify-head.sh "$OUTDIR" HEAD 2>&1 | head -1)"
fi

echo ""
echo "artifacts left at: $OUTDIR"
if [ "$fails" -eq 0 ]; then echo "CHANNEL FIRE-CHECK PASSED"; exit 0; fi
echo "CHANNEL FIRE-CHECK FAILED — $fails check(s) did not hold."
exit 1
