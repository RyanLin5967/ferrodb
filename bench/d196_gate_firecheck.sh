#!/usr/bin/env bash
# d196_gate_firecheck.sh — fire-check for D196: tools/land-gate.sh must refuse unless HEAD is the
# commit being landed, and must say so before anything builds.
# Pre-registration, written before the first run: bench/d196_gate_firecheck_prereg.md.
#
# NOTHING HERE BUILDS, and that is enforced rather than hoped for:
#   * every arm runs in a THROWAWAY CLONE under $TMPDIR, never in the source repository;
#   * a fake `cargo` and `rustup` sit first on PATH. Reaching either writes a sentinel and exits 97,
#     and every arm asserts the sentinel is absent, so a gate that got as far as a build FAILS its
#     arm instead of compiling;
#   * arms with the real tools use an EMPTY verify dir, so a gate that passes step 0 stops at step 1
#     (certify-head refuses a directory with no summary);
#   * arms that must reach the END of the gate run on a clone-local commit whose three tools are
#     stubs. That commit exists only in the clone. Nothing here pushes or writes to the source repo.
#
# ARMS (predicates are in the pre-registration; each is a literal string, never the subject's output):
#   A1  new gate, HEAD = M (C + one commit), landing C   -> REFUSES at step 0, before [1/3]
#   B1  new gate, HEAD = C, landing C (full sha)         -> passes step 0, stops at certify-head
#   B2  as B1, landing named by a 7-char sha             -> passes step 0 (it compares shas, not text)
#   B3  as B1, landing named by a branch                 -> passes step 0
#   C1  PRE-D196 gate text, the A1 state                 -> does NOT refuse there: reaches [1/3]
#   D1  new gate, stubbed tools, HEAD = S, landing S     -> runs to OK (the happy path still exists)
#   D2  PRE-D196 gate, stubbed, HEAD = MS, landing S     -> prints OK while its step 3 built MS: D196
#   D3  new gate, the D2 state                           -> REFUSES at step 0; no tool runs
#   E1  new gate, stubbed, prepush checks out MS mid-run -> REFUSES at the end: HEAD moved
#   E2  PRE-D196 gate, the E1 state                      -> prints OK: the end check is new too
#
# Usage: bench/d196_gate_firecheck.sh [<candidate, default HEAD>] [<pre-D196 commit, default 9aa6968>]
# Exit 0 only if every arm ran and every predicate held. KEEP=1 leaves the clone for inspection.
set -uo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE

cd "$(dirname "$0")/.." || { echo "FIRECHECK: cannot reach the repository root"; exit 2; }
SRC_GIT=$(git rev-parse --path-format=absolute --git-common-dir) || { echo "FIRECHECK: not in a git repo"; exit 2; }
CAND=$(git rev-parse --verify "${1:-HEAD}^{commit}") || { echo "FIRECHECK: candidate does not resolve"; exit 2; }
OLD=$(git rev-parse --verify "${2:-9aa6968}^{commit}") || { echo "FIRECHECK: pre-D196 commit does not resolve"; exit 2; }
command -v timeout >/dev/null || { echo "FIRECHECK: no \`timeout\` on PATH; refusing to run unbounded"; exit 2; }

NEW_MARK='this checkout is not the commit being landed'
END_MARK='HEAD moved during this gate'

T=$(mktemp -d "${TMPDIR:-/tmp}/d196fc.XXXXXX") || { echo "FIRECHECK: mktemp failed"; exit 2; }
case "$T" in */d196fc.??????) ;; *) echo "FIRECHECK: unexpected temp path '$T'"; exit 2 ;; esac
cleanup() {
    [ "${KEEP:-0}" = 1 ] && { echo "# KEEP=1: clone left at $T"; return; }
    case "$T" in */d196fc.??????) [ -d "$T" ] && rm -rf -- "$T" ;; esac
}
trap cleanup EXIT

R=$T/repo
V=$T/verify-empty
SENT=$T/CARGO_REACHED
mkdir -p "$V" "$T/fakebin" || exit 2
for tool in cargo rustup; do
    printf '#!/bin/sh\necho "FAKE %s $*" >> "%s"\nexit 97\n' "$tool" "$SENT" > "$T/fakebin/$tool"
    chmod +x "$T/fakebin/$tool"
done

g() { git -C "$R" -c user.name=d196-firecheck -c user.email=d196-firecheck@invalid \
          -c core.hooksPath=/dev/null -c commit.gpgsign=false "$@"; }

echo "# D196 land-gate fire-check"
echo "# date        $(date -u +%FT%TZ)"
echo "# source      $SRC_GIT"
echo "# script blob $(git hash-object bench/d196_gate_firecheck.sh)  (working copy)"
echo "# candidate C $CAND  $(git log -1 --format=%s "$CAND")"
echo "# pre-D196    $OLD  $(git log -1 --format=%s "$OLD")"
echo "# gate blob   C: $(git rev-parse "$CAND:tools/land-gate.sh")   pre-D196: $(git rev-parse "$OLD:tools/land-gate.sh")"
echo "# bash        $BASH_VERSION   $(git --version)"
echo "# clone       $R"

# ---- fixture --------------------------------------------------------------------------------
git clone -q --no-checkout "$SRC_GIT" "$R" || { echo "FIRECHECK: clone failed"; exit 2; }
g checkout -q --detach "$CAND" || { echo "FIRECHECK: candidate not reachable in the clone"; exit 2; }
g branch -q d196-fc-candidate "$CAND"

echo "d196 fire-check fixture: a commit that is not the candidate" > "$R/D196_FIXTURE_MOVED"
g add D196_FIXTURE_MOVED && g commit -q -m "d196 fixture M: C plus one commit" || exit 2
M=$(g rev-parse HEAD)

g checkout -q --detach "$CAND"
cat > "$R/tools/certify-head.sh" <<'STUB'
#!/usr/bin/env bash
# D196 fire-check STUB. Exists only in a throwaway clone.
echo "STUB certify-head: OK (dir=$1 want=$2)"
exit 0
STUB
cat > "$R/tools/staleness.sh" <<'STUB'
#!/usr/bin/env bash
# D196 fire-check STUB. Exists only in a throwaway clone.
echo "branch $3 vs $2: STUB"
echo "COVERED: STUB staleness"
exit 0
STUB
cat > "$R/tools/prepush.sh" <<'STUB'
#!/usr/bin/env bash
# D196 fire-check STUB. Exists only in a throwaway clone. Reports the tree a real prepush would build.
cd "$(dirname "$0")/.." || exit 1
echo "STUB prepush: would build HEAD=$(git rev-parse HEAD)"
if [ -n "${D196_FC_MOVE_HEAD_TO:-}" ]; then
    git checkout -q --detach "$D196_FC_MOVE_HEAD_TO" && echo "STUB prepush: moved HEAD to $(git rev-parse HEAD)"
fi
exit 0
STUB
g add tools/certify-head.sh tools/staleness.sh tools/prepush.sh && g commit -q -m "d196 fixture S: C with stubbed tools" || exit 2
S=$(g rev-parse HEAD)
echo "d196 fire-check fixture: a commit that is not the candidate" > "$R/D196_FIXTURE_MOVED"
g add D196_FIXTURE_MOVED && g commit -q -m "d196 fixture MS: S plus one commit" || exit 2
MS=$(g rev-parse HEAD)

# The pre-D196 gate text rides along as ONE untracked file, excluded so the gate's own dirty-tree
# check does not see it. It is the only difference between the A1 and C1 states.
g show "$OLD:tools/land-gate.sh" > "$R/tools/land-gate.pre-d196.sh" || exit 2
echo "tools/land-gate.pre-d196.sh" >> "$R/.git/info/exclude"

echo "# fixture     M  $M"
echo "# fixture     S  $S"
echo "# fixture     MS $MS"
echo "# fake verify dir (empty) $V"

PREDS=0; FAILS=0; ARMS=0
ok()   { PREDS=$((PREDS + 1)); echo "    PASS  $*"; }
bad()  { PREDS=$((PREDS + 1)); FAILS=$((FAILS + 1)); echo "    FAIL  $*"; }
pre()  { # a precondition on the fixture, not on the subject
    if eval "$2"; then ok "precondition: $1"; else bad "precondition: $1"; fi; }
has()  { local hit; hit=$(grep -n -F -- "$1" "$OUT" | head -3)
    if [ -n "$hit" ]; then ok "has   '$1'"; echo "$hit" | sed 's/^/            | /'
    else bad "has   '$1' -- ABSENT"; fi; }
lacks(){ local hit; hit=$(grep -n -F -- "$1" "$OUT" | head -3)
    if [ -z "$hit" ]; then ok "lacks '$1'"
    else bad "lacks '$1' -- PRESENT:"; echo "$hit" | sed 's/^/            | /'; fi; }
rc_is(){ if [ "$RC" = "$1" ]; then ok "rc=$RC (want $1)"; else bad "rc=$RC (want $1)"; fi; }
no_build(){ if [ -e "$SENT" ]; then bad "cargo/rustup NOT reached -- REACHED:"; sed 's/^/            | /' "$SENT"
            else ok "cargo/rustup not reached (no sentinel)"; fi; }

run_arm() { # name head-commit gate-file want [VAR=value for the gate's environment]
    local name=$1 head=$2 gate=$3 want=$4 extra=${5:-}
    ARMS=$((ARMS + 1))
    OUT=$T/arm_$name.out
    rm -f "$SENT"
    g checkout -q --detach "$head"
    echo
    echo "================================================================================"
    echo "ARM $name   gate=$gate   HEAD=$(g rev-parse HEAD)   landing=$want   base=$OLD ${extra:+  env: $extra}"
    if [ -z "$(g status --porcelain)" ]; then ok "precondition: clone clean at HEAD=$head"
    else bad "precondition: clone clean at HEAD=$head -- DIRTY:"; g status --porcelain | sed 's/^/            | /'; fi
    echo "  \$ bash $gate $V $want $OLD"
    ( cd "$R" && env PATH="$T/fakebin:$PATH" $extra timeout 120 bash "$gate" "$V" "$want" "$OLD" ) > "$OUT" 2>&1
    RC=$?
    sed 's/^/  > /' "$OUT"
    echo "  rc=$RC   HEAD after: $(g rev-parse HEAD)"
    echo "  predicates:"
}

# ---- preconditions: the two gate texts really differ in the way the arms assume ------------------
echo
echo "PRECONDITIONS"
pre "candidate gate text carries the step-0 refusal"  "g show $CAND:tools/land-gate.sh | grep -q -F '$NEW_MARK'"
pre "candidate gate text carries the end HEAD check"  "g show $CAND:tools/land-gate.sh | grep -q -F '$END_MARK'"
pre "pre-D196 gate text lacks the step-0 refusal"      "! grep -q -F '$NEW_MARK' '$R/tools/land-gate.pre-d196.sh'"
pre "pre-D196 gate text lacks the end HEAD check"      "! grep -q -F '$END_MARK' '$R/tools/land-gate.pre-d196.sh'"
pre "M differs from C"                                  "[ '$M' != '$CAND' ]"
pre "S's land-gate.sh is C's (only the tools changed)"  "[ \"\$(g rev-parse $S:tools/land-gate.sh)\" = \"\$(g rev-parse $CAND:tools/land-gate.sh)\" ]"
pre "C's real tools are not stubs"                      "! g show $CAND:tools/prepush.sh | grep -q STUB"
# The no-build detector, forced to fire: the same call shape prepush.sh uses (`timeout N cargo ...`
# through PATH), run in $T, which holds no Cargo.toml, so even a failed interception builds nothing.
pre "under the arms' PATH, cargo resolves to the fake" "[ \"\$(env PATH=$T/fakebin:\$PATH sh -c 'command -v cargo')\" = $T/fakebin/cargo ]"
( cd "$T" && env PATH="$T/fakebin:$PATH" timeout 10 cargo build --examples ) >/dev/null 2>&1
FAKE_RC=$?
pre "the fake cargo exits 97 when reached (got $FAKE_RC)" "[ $FAKE_RC = 97 ]"
pre "the fake cargo leaves the sentinel the arms look for" "grep -q -F 'FAKE cargo build --examples' '$SENT'"
rm -f "$SENT"

# ---- the three arms the brief asked for -------------------------------------------------------
run_arm A1 "$M" tools/land-gate.sh "$CAND"
rc_is 1
has "land-gate: REFUSING — $NEW_MARK."
has "HEAD      $M"
has "landing   $CAND"
has "Run it from the candidate's worktree"
lacks "[1/3]"
lacks "land-gate: landing"
no_build

run_arm B1 "$CAND" tools/land-gate.sh "$CAND"
rc_is 1
lacks "$NEW_MARK"
has "HEAD is the commit being landed [step 0]"
has "land-gate: [1/3] certify-head.sh"
has "certify-head: REFUSING — no persisted suite summary"
lacks "[2/3]"
no_build

run_arm B2 "$CAND" tools/land-gate.sh "$(git rev-parse --short=7 "$CAND")"
rc_is 1
lacks "$NEW_MARK"
has "HEAD is the commit being landed [step 0]"
has "certify-head: REFUSING — no persisted suite summary"
no_build

run_arm B3 "$CAND" tools/land-gate.sh d196-fc-candidate
rc_is 1
lacks "$NEW_MARK"
has "HEAD is the commit being landed [step 0]"
has "certify-head: REFUSING — no persisted suite summary"
no_build

run_arm C1 "$M" tools/land-gate.pre-d196.sh "$CAND"
rc_is 1
lacks "$NEW_MARK"
has "land-gate: landing"
has "land-gate: [1/3] certify-head.sh"
has "certify-head: REFUSING — no persisted suite summary"
no_build

# ---- beyond the brief: the whole gate, on stubbed tools ------------------------------------
run_arm D1 "$S" tools/land-gate.sh "$S"
rc_is 0
has "HEAD is the commit being landed [step 0]"
has "STUB certify-head: OK"
has "STUB prepush: would build HEAD=$S"
has "land-gate: OK — the evidence in"
lacks "REFUSING"
no_build

run_arm D2 "$MS" tools/land-gate.pre-d196.sh "$S"
rc_is 0
has "STUB certify-head: OK (dir=$V want=$S)"
has "STUB prepush: would build HEAD=$MS"
has "land-gate: OK — the evidence in"
no_build

run_arm D3 "$MS" tools/land-gate.sh "$S"
rc_is 1
has "land-gate: REFUSING — $NEW_MARK."
lacks "STUB"
no_build

run_arm E1 "$S" tools/land-gate.sh "$S" "D196_FC_MOVE_HEAD_TO=$MS"
rc_is 1
has "STUB prepush: moved HEAD to $MS"
has "land-gate: REFUSING — $END_MARK: $S -> $MS."
lacks "the tree became dirty"
lacks "land-gate: OK"
no_build

run_arm E2 "$S" tools/land-gate.pre-d196.sh "$S" "D196_FC_MOVE_HEAD_TO=$MS"
rc_is 0
has "STUB prepush: moved HEAD to $MS"
has "land-gate: OK — the evidence in"
no_build

# ---- verdict ----------------------------------------------------------------------------------
echo
echo "================================================================================"
REGISTERED_ARMS=10
echo "arms run: $ARMS (registered $REGISTERED_ARMS)   predicates: $PREDS   failed: $FAILS"
if [ "$ARMS" != "$REGISTERED_ARMS" ] || [ "$PREDS" = 0 ]; then
    echo "VERDICT: FAIL — not every registered arm ran, or nothing was checked. That is not a pass."
    echo "# END d196_gate_firecheck rc=1"; exit 1
fi
if [ "$FAILS" != 0 ]; then
    echo "VERDICT: FAIL — $FAILS predicate(s) did not hold."
    echo "# END d196_gate_firecheck rc=1"; exit 1
fi
echo "VERDICT: PASS — every arm ran and every predicate held."
echo "# END d196_gate_firecheck rc=0"
exit 0
