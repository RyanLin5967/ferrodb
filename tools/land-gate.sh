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
# ⛔ D196 — IT MUST RUN IN THE CANDIDATE'S OWN CHECKOUT, AND IT NOW REFUSES OTHERWISE.
# Found 2026-09-24 00:47Z. Step 3's `prepush.sh` is the CI-parity BUILD, and it builds the checkout
# this script's own file lives in: both scripts `cd` to their repository root, so your cwd is
# irrelevant. Nothing asserted that checkout was the commit being landed. Run from the MAIN
# checkout, as landings on 2026-09-23 were, steps 1 and 2 were asked about the candidate while step
# 3 built main, the base, and all three printed under one OK. CI after each push was green, so no
# damage is known. The hole was real anyway.
# ⇒ Step 0 refuses unless `git rev-parse HEAD` IS the commit being landed, before anything else
# runs. The closing block asks again, because a checkout DURING step 3 is the same hazard through
# the other door. Both compare SHAS, the resulting state, not how the gate was invoked, so no
# spelling of the call walks around them.
#
# WHAT THIS IS NOT. It re-asks none of the three tools' questions — that would be the `two guards
# is one you cannot test` failure, where a redundant downstream check masks every mutant of the
# one in front of it. It composes the existing three, in an order, and refuses on anything that is
# not unambiguously clean. Its only judgement of its own is about the checkout it runs in (HEAD,
# clean, base unmoved), which none of the three asks.
#
# STATED BLIND SPOTS, per this repo's convention that a guard names its own:
#   * It cannot tell you any run was honest. It reads verdicts; it re-runs nothing.
#   * It inherits every blind spot of the three tools it calls, including that `certify-head.sh`
#     cannot see a run whose summary was never persisted.
#   * It checks the tree is clean, and HEAD is the candidate, at the start AND at the end, but it
#     cannot stop a writer that lands entirely between two of its own steps: a checkout that moves
#     HEAD away and BACK inside step 3 is invisible. It narrows that window; it does not close it.
#   * (D196) The HEAD check compares COMMITS, and the dirty check reads `git status`. Neither sees
#     an ignored file (`/target`, `*.db`, anything .gitignore matches), so an ignored file that
#     feeds the build is outside both.
#   * (D196) The check travels inside the file it guards. A checkout whose own tools/land-gate.sh
#     predates D196 runs the old text and gets no step 0. That includes a candidate branched before
#     D196 that has not merged main since.
#   * (D196) It cannot see the merge that follows. It certifies the sha it prints. Merging anything
#     else afterwards is outside it: a branch name that has moved since, or the commit you meant
#     when you passed `HEAD` from the wrong checkout, which makes step 0 true by construction.
#     Merge the printed sha.
#   * It says nothing about whether the branch SHOULD land — only that the evidence is about the
#     tree that will ship.
#
# Usage: tools/land-gate.sh <verify-out-dir> <commit-ish to land> [<base, default: main>]
#   Run the copy INSIDE the candidate's worktree, with that worktree clean and at <commit-ish>:
#     cd <candidate worktree> && bash tools/land-gate.sh <dir> <sha> main
#   Then merge that sha from wherever you merge. Step 0 refuses any other checkout.
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

# ---- 0. THE CHECKOUT IS THE CANDIDATE (D196) ---------------------------------------------
# Before anything else: step 3 builds THIS checkout, so if HEAD is not the commit being landed,
# every later step certifies a tree that will not ship. Equality, not ancestry: a checkout one
# harmless-looking commit away is refused too, because which tree got built is not a judgement.
TREE=$(git rev-parse --show-toplevel)
HEAD_SHA=$(git rev-parse --verify --quiet 'HEAD^{commit}') \
    || die "HEAD in $TREE does not resolve to a commit, so there is no telling what step 3 would build."
[ "$HEAD_SHA" = "$WANT_SHA" ] || die "this checkout is not the commit being landed.
  checkout  $TREE
  HEAD      $HEAD_SHA  $(git log -1 --format=%s "$HEAD_SHA")
  landing   $WANT_SHA  $(git log -1 --format=%s "$WANT_SHA")
  Step 3 (prepush.sh) builds THIS checkout, so from here the gate would certify
  $(git rev-parse --short "$HEAD_SHA"), not $(git rev-parse --short "$WANT_SHA").
  Run it from the candidate's worktree, using the copy of this script INSIDE it (the gate
  examines the checkout its own file lives in, not your cwd):
    cd <candidate worktree> && bash tools/land-gate.sh $DIR $WANT_SHA $BASE
  Find that worktree with: git worktree list | grep $(git rev-parse --short "$WANT_SHA")"

# A dirty tree means the thing about to be merged is not the thing that was tested, whatever every
# other guard says.
DIRTY_BEFORE=$(git status --porcelain | wc -l | tr -d ' ')
[ "$DIRTY_BEFORE" = 0 ] || die "the working tree has $DIRTY_BEFORE uncommitted change(s).
  Commit or stash them: a green certifies a tree, and this is not that tree."

say "landing   $WANT ($(git rev-parse --short "$WANT_SHA"))"
say "onto base $BASE ($(git rev-parse --short "$BASE_SHA"))"
say "evidence  $DIR"
say "checkout  $TREE — HEAD is the commit being landed [step 0]"
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
# D196's other door: a clean checkout of another commit DURING step 3 leaves no dirt behind, and
# step 3 then built that commit. Step 0's question, asked again.
NOW_HEAD=$(git rev-parse --verify --quiet 'HEAD^{commit}')
[ "$NOW_HEAD" = "$WANT_SHA" ] || die "HEAD moved during this gate: $WANT_SHA -> ${NOW_HEAD:-<unresolvable>}.
  Step 3 built whatever was checked out while it ran, so nothing here says it built $WANT. Re-run."
NOW_BASE=$(git rev-parse --verify "$BASE^{commit}")
[ "$NOW_BASE" = "$BASE_SHA" ] || die "$BASE moved during this gate: $BASE_SHA -> $NOW_BASE.
  That is precisely the failure this script exists to catch, arriving inside the script. Re-run."

say "OK — the evidence in $DIR is about the tree that will ship."
say "  suite names the commit · base is covered · CI-parity passes · tree unmoved throughout"
say "  · built in the candidate's own checkout."
