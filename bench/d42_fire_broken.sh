#!/usr/bin/env bash
# bench/d42_fire_broken.sh — D42 falsifier 2 of 2: the classifier must NOT fire spuriously.
#
# A detector that fires on everything is worse than none. This one breaks the cluster for real —
# it kills a SECOND node, so that only 1 of 3 survives and no quorum can ever form again — and
# requires the verdict FAILED, at the normal 45 s, with an iteration count near nominal.
#
# ⛔ THE PRE-REGISTERED KILL CONDITION. If a genuinely broken cluster earns INCONCLUSIVE, B+C are a
# relabelling of FAILED and the mechanism must be ABANDONED, not tuned. This script exits non-zero
# and says so; do not adjust a threshold to make it pass.
#
# WHERE IT RUNS. In a THROWAWAY worktree, given as $1, never the branch's own tree. A fire-check
# leaves code deliberately broken on disk for minutes, and this project has already paid for that
# twice: one agent recorded another's deliberately broken tree into a bench artifact as a real
# regression, and a `git checkout --` restore silently reverted an uncommitted fix so every
# subsequent arm measured the wrong code. So: commit first, break elsewhere.
#
# Usage: bench/d42_fire_broken.sh /path/to/throwaway/worktree
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
SELF_DIR=$(cd "$(dirname "$0")/.." && pwd)

TREE=${1:-}
[ -n "$TREE" ] || { echo "REFUSING — no throwaway worktree given. Usage: $0 <worktree>"; exit 1; }
[ -d "$TREE/.git" ] || [ -f "$TREE/.git" ] || { echo "REFUSING — '$TREE' is not a git worktree"; exit 1; }
if [ "$(cd "$TREE" && pwd -P)" = "$SELF_DIR" ]; then
    echo "REFUSING — that is this script's OWN tree. Breaking it would leave the branch broken on"
    echo "  disk and poison any concurrent run against it. Pass a throwaway worktree."
    exit 1
fi

RS="$TREE/tests/integration_consensus_failover.rs"
[ -f "$RS" ] || { echo "REFUSING — $RS is missing"; exit 1; }

LOCK=/tmp/ferrodb-suite.lock
held=0
cleanup() { [ "$held" = 1 ] && rm -rf "$LOCK"; return 0; }
trap cleanup EXIT INT TERM

# ── the injection ───────────────────────────────────────────────────────────────────────────────
# Aimed at the exact window the classifier covers: the wait for a NEW leader, immediately after the
# original leader is killed. A break placed anywhere else would leave that window intact and the
# run would prove nothing.
python3 - "$RS" <<'PY'
import sys
p = sys.argv[1]
src = open(p).read()
anchor = "    nodes[leader_ix].dead = true;\n"
if anchor not in src:
    sys.exit("REFUSING — the injection anchor is not in the file; the break would land nowhere.")
inject = anchor + """
    // ── D42 FALSIFIER 2 — INJECTED, NEVER COMMITTED TO ANY BRANCH ──────────────────────────
    // Kill a SECOND node. With 1 of 3 alive, quorum (2) is unreachable and the cluster is
    // genuinely, permanently broken. The wait below MUST expire at 45s, and the classifier MUST
    // call that FAILED on a machine where this process and its nodes are being scheduled.
    let victim_ix = nodes.iter().position(|n| !n.dead).expect("a survivor to break the quorum");
    nodes[victim_ix].child.kill().expect("kill a second node");
    let _ = nodes[victim_ix].child.wait();
    nodes[victim_ix].dead = true;
    eprintln!("D42-FALSIFIER-2: killed n{} as well; 1 of 3 alive, quorum unreachable", nodes[victim_ix].id);
"""
open(p, "w").write(src.replace(anchor, inject, 1))
print("injected: a second node is killed, so no quorum can form")
PY
[ $? -eq 0 ] || exit 1

cd "$TREE" || exit 1
cargo build --examples >/dev/null 2>&1 || { echo "REFUSING — cargo build --examples failed"; exit 1; }
cargo test --no-run --test integration_consensus_failover >/dev/null 2>&1 \
    || { echo "REFUSING — the injected test does not build"; exit 1; }

w=0
while ! mkdir "$LOCK" 2>/dev/null; do
    o=$(cat "$LOCK/owner" 2>/dev/null || echo unknown); p=${o%% *}
    if [ -n "$p" ] && ! kill -0 "$p" 2>/dev/null; then rm -rf "$LOCK"; continue; fi
    [ "$w" -ge 3600 ] && { echo "REFUSING — waited ${w}s for the suite lock held by: $o"; exit 3; }
    [ "$w" -eq 0 ] && echo "queued behind a running suite ($o)" >&2
    sleep 15; w=$((w+15))
done
printf '%s %s %s\n' "$$" "d42-fire-broken" "$(date -u +%FT%TZ)" > "$LOCK/owner"
held=1

echo "D42 falsifier 2 — a genuinely broken cluster must earn FAILED, not INCONCLUSIVE"
echo "  when          : $(date -u +%FT%TZ)"
echo "  tree          : $TREE"
echo "  head          : $(git log -1 --format=%h) + the injection above (uncommitted, deliberate)"
echo "  loadavg before: $(uptime | sed 's/.*load averages*: //')"
echo ""
echo "---------------- cargo test --test integration_consensus_failover ----------------"
OUT=$(timeout 600 cargo test --test integration_consensus_failover 2>&1)
rc=$?
echo "$OUT"
echo "---------------- exit $rc ----------------"
echo "  loadavg after : $(uptime | sed 's/.*load averages*: //')"
echo ""

# ── the verdict on the verdict ──────────────────────────────────────────────────────────────────
fails=0
if grep -qF 'FERRODB-VERDICT: INCONCLUSIVE' <<<"$OUT"; then
    echo "⛔ FALSIFIER 2 FAILED — a genuinely broken cluster earned INCONCLUSIVE."
    echo "   This is the pre-registered kill condition. B+C are a relabelling of FAILED."
    echo "   ABANDON the mechanism. Do NOT tune a threshold to make this pass."
    fails=$((fails+1))
elif grep -qF 'FERRODB-VERDICT: CLASSIFIER-BROKEN' <<<"$OUT"; then
    echo "⛔ FALSIFIER 2 FAILED — the classifier could not measure at all on a quiet machine."
    fails=$((fails+1))
elif grep -q 'timed out after 45s waiting for a new leader after the kill' <<<"$OUT"; then
    echo "ok: the broken cluster produced the ORIGINAL timeout panic, unchanged"
else
    echo "⛔ FALSIFIER 2 INCONCLUSIVE ITSELF — the run did not reach the expected timeout at all."
    echo "   Without an expiry there is no verdict to check, so this proves nothing."
    fails=$((fails+1))
fi
if grep -q 'verdict       : FAILED' <<<"$OUT"; then
    echo "ok: the classifier explicitly recorded FAILED"
    grep -E 'self-schedule|child CPU|max stall|window        :' <<<"$OUT" | sed 's/^/   /'
else
    echo "⛔ no 'verdict : FAILED' line — the classifier did not run on this expiry."
    fails=$((fails+1))
fi

echo ""
if [ "$fails" -eq 0 ]; then
    echo "FALSIFIER 2 PASSED — a broken cluster still reports FAILED, at 45s, as it did before D42."
    exit 0
fi
echo "FALSIFIER 2 FAILED — $fails check(s) did not hold."
exit 1
