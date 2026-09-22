#!/usr/bin/env bash
# land-gate.sh — the ONE command that must pass before a branch is merged into main.
#
# WHY THIS EXISTS, and it is a gap the repo's own text already describes.
#
# There are three land-time guards and they answer three different questions:
#
#   verify-suite.sh   did the tests pass?                       (and it writes the verdict)
#   certify-head.sh   does that verdict name the commit being landed?   SUITE vs BRANCH
#   staleness.sh      has the base moved under files this branch touches?  BRANCH vs BASE
#   prepush.sh        do the checks CI runs that the suite does not pass?
#
# `certify-head.sh`'s own header says: *"It says nothing about whether the BASE is current. That is
# staleness.sh's question, and both must pass. Neither subsumes the other."* Correct — and then
# **nothing in this repo ran both.** `prepush.sh` ends by telling the operator *"Run all three:
# verify-suite.sh, certify-head.sh, and this"* — a list of three that **omits `staleness.sh`**,
# i.e. omits the only guard that can see a base-moved green. The instruction and the guard set
# disagreed, and the instruction is what an operator actually follows.
#
# ⭐ AND THE GAP IS NOT ONLY "NOBODY RAN IT" — IT IS **WHEN** IT WAS RUN.
# Measured 2026-09-22 on D126 by another session's agent, and this script exists because of it:
# a branch certified green at `34549e4`; WHILE THAT SUITE RAN, main moved `ac2ab26 -> 4e0bcc1` as
# D125 landed into `src/branch/arena.rs` — a file D126 also edits. Every guard had been satisfied
# *before* the run. The tree that was tested was then not the tree that would ship, and no guard
# fired, because staleness had already been asked and answered.
#
# ⇒ **A base that moves DURING a suite is invisible to a staleness check taken BEFORE it.** So the
# ordering here is the content of this script, not a detail: staleness is asked LAST, against the
# commit the suite actually certified, immediately before the merge.
#
# WHAT THIS IS NOT. It is not a fourth comparison and adds no new judgement of its own — that
# would be the `two guards is one you cannot test` failure, where a redundant downstream check
# masks every mutant of the one in front of it. It composes the existing three, in an order, and
# refuses on anything that is not unambiguously clean.
#
# STATED BLIND SPOTS, per this repo's convention that a guard names its own:
#   * It cannot tell you any run was honest. It reads verdicts; it re-runs nothing.
#   * It inherits every blind spot of the three tools it calls, including that `certify-head.sh`
#     cannot see a run whose summary was never persisted.
#   * It checks the tree is clean at the start AND at the end, but it cannot stop a writer that
#     lands entirely between two of its own steps. It narrows that window; it does not close it.
#   * It says nothing about whether the branch SHOULD land — only that the evidence is about the
#     tree that will ship.
#
# Usage: tools/land-gate.sh <verify-out-dir> <commit-ish to land> [<base, default: main>]
set -uo pipefail

say()  { printf 'land-gate: %s\n' "$*"; }
die()  { printf 'land-gate: REFUSING — %s\n' "$*" >&2; exit 1; }

[ $# -ge 2 ] || die "usage: land-gate.sh <verify-out-dir> <commit-ish> [<base>]"
DIR=$1
WANT=$2
BASE=${3:-main}

cd "$(dirname "$0")/.." || die "cannot reach the repository root."
command -v git >/dev/null || die "no \`git\` on PATH."
git rev-parse --git-dir >/dev/null 2>&1 || die "not inside a git repository."

WANT_SHA=$(git rev-parse --verify "$WANT^{commit}" 2>/dev/null) \
    || die "'$WANT' does not resolve to a commit."
BASE_SHA=$(git rev-parse --verify "$BASE^{commit}" 2>/dev/null) \
    || die "'$BASE' does not resolve to a commit."

# A dirty tree means the thing about to be merged is not the thing that was tested, whatever every
# other guard says.
DIRTY_BEFORE=$(git status --porcelain | wc -l | tr -d ' ')
[ "$DIRTY_BEFORE" = 0 ] || die "the working tree has $DIRTY_BEFORE uncommitted change(s).
  Commit or stash them: a green certifies a tree, and this is not that tree."

say "landing   $WANT ($(git rev-parse --short "$WANT_SHA"))"
say "onto base $BASE ($(git rev-parse --short "$BASE_SHA"))"
say "evidence  $DIR"
printf '\n'

# ---- 1. SUITE vs BRANCH -------------------------------------------------------------------
say "[1/3] certify-head.sh — does the verdict name the commit being landed?"
bash tools/certify-head.sh "$DIR" "$WANT_SHA" || die "certify-head refused (above). Re-run the suite at $WANT."
printf '\n'

# ---- 2. BRANCH vs BASE, ASKED **NOW** -----------------------------------------------------
# ⭐ THE ORDER IS THE POINT. Asked here, after the suite, this sees a base that moved DURING the
# run — which is exactly the case that produced this script and that an earlier staleness check
# structurally cannot see.
say "[2/3] staleness.sh — has the base moved under this branch's files SINCE the suite ran?"
STALE_OUT=$(bash tools/staleness.sh . "$BASE_SHA" "$WANT_SHA" 2>&1)
STALE_RC=$?
printf '%s\n' "$STALE_OUT" | sed 's/^/      /'
# \u26d4 THE VERDICT IS A LINE, NOT A PREFIX. The first cut of this matched `case "$STALE_OUT" in
# COVERED*)`, which can never fire: staleness.sh prints a header line ("branch X vs Y: N behind,
# M ahead") BEFORE its verdict, so the blob never STARTS with a verdict word. Every verdict fell
# to the `*)` catch-all. It refused rather than allowed -- the safe direction, and the reason this
# was recoverable -- but it would have refused a genuine STALE-GREEN with "unrecognised verdict",
# i.e. the right answer for the wrong reason, which is indistinguishable from luck.
#
# It survived its own fire-check because that check fed it SYNTHETIC verdict strings ("COVERED",
# "STALE-GREEN: ...") rather than real `staleness.sh` output. Nine cases passed and the first real
# one failed. ⇒ A parser must be fire-checked against the ACTUAL emitter, never against a
# hand-written example of what you believe the emitter says.
VERDICT=$(printf '%s\n' "$STALE_OUT" | grep -oE '^(COVERED[A-Z-]*|STALE-GREEN|UNKNOWN)' | head -1)
case "$VERDICT" in
    COVERED*) : ;;                       # COVERED, COVERED-NO-CODE, COVERED-COMMENTS-ONLY
    STALE-GREEN) die "STALE-GREEN — the base moved under code this branch has not seen.
  Merge $BASE into $WANT and RE-RUN the suite. Do not reason about whether it matters:
  'close enough' is the judgement these guards exist to delete." ;;
    UNKNOWN) die "staleness could not determine a verdict (rc=$STALE_RC).
  A guard that cannot parse its own input must never fall through to allow." ;;
    *) die "staleness emitted no recognisable verdict LINE (rc=$STALE_RC). Refusing rather than guessing.
  Its output is above. A verdict must appear at the start of one of its lines." ;;
esac
[ "$STALE_RC" = 0 ] || die "staleness printed a COVERED verdict but exited $STALE_RC. Refusing on the disagreement."
printf '\n'

# ---- 3. THE CHECKS CI RUNS THAT THE SUITE DOES NOT ----------------------------------------
say "[3/3] prepush.sh — the CI-parity checks a green suite structurally cannot see."
bash tools/prepush.sh || die "prepush refused (above)."
printf '\n'

# ---- the window this script itself opens ---------------------------------------------------
DIRTY_AFTER=$(git status --porcelain | wc -l | tr -d ' ')
[ "$DIRTY_AFTER" = 0 ] || die "the tree became dirty DURING this gate ($DIRTY_AFTER change(s)).
  Something wrote to the checkout while it was being certified; nothing here is trustworthy."
NOW_BASE=$(git rev-parse --verify "$BASE^{commit}")
[ "$NOW_BASE" = "$BASE_SHA" ] || die "$BASE moved during this gate: $BASE_SHA -> $NOW_BASE.
  That is precisely the failure this script exists to catch, arriving inside the script. Re-run."

say "OK — the evidence in $DIR is about the tree that will ship."
say "  suite names the commit · base is covered · CI-parity passes · tree unmoved throughout."
