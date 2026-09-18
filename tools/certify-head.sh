#!/usr/bin/env bash
# certify-head.sh — refuse to land a suite result that does not name the commit being landed.
#
# WHY THIS EXISTS. `tools/staleness.sh` compares BRANCH to BASE. Nothing compared SUITE to BRANCH,
# and that gap has now produced the same defect three times on this project — once inside D40's own
# evidence directory, where the canonical filenames carried a superseded `head=`. The shape is
# always identical and always invisible: a suite runs, finishes green, and the branch tip moves
# afterwards. `verify-suite.sh` cannot catch it — it compares HEAD before and after its own run and
# has already exited by the time the amend lands. So the check has to live at LAND time, here.
#
# WHAT IT CANNOT DO — stated in the guard, per the rule that a guard must name its own blind spots:
#   * It cannot tell you the run was honest. It reads a summary line; it does not re-run anything.
#   * It cannot see a run whose summary was never persisted. That is why it REFUSES on zero
#     summaries rather than passing — a missing verdict is not a green one.
#   * It says nothing about whether the BASE is current. That is `staleness.sh`'s question, and
#     both must pass. Neither subsumes the other.
#   * A comment-only delta between the suite's commit and the tip will still be REFUSED. That is
#     deliberate: "close enough" is the judgement this guard exists to delete. Re-run instead.
#
# Usage: certify-head.sh <verify-out-dir> [<commit-ish to land, default HEAD>]
set -uo pipefail

die() { echo "certify-head: REFUSING — $*" >&2; exit 1; }

[ $# -ge 1 ] || die "no verify-out directory given. Usage: certify-head.sh <dir> [<commit-ish>]"
DIR=$1
WANT=${2:-HEAD}

[ -d "$DIR" ] || die "'$DIR' is not a directory."
command -v git >/dev/null || die "no \`git\` on PATH; cannot resolve any commit."
git rev-parse --git-dir >/dev/null 2>&1 || die "not inside a git repository."

# --- find the persisted verdict -------------------------------------------------------------
# Accept either SUMMARY.txt (written by verify-suite.sh) or RUNNER.txt (written by a wrapper that
# captured its stdout). Collect every line carrying a head= field, from every candidate file.
SUMS=$(grep -h 'head=' "$DIR"/SUMMARY.txt "$DIR"/RUNNER.txt 2>/dev/null | grep 'mode=')
if [ -z "$SUMS" ]; then
    die "no persisted suite summary with a head= field under '$DIR'.
  Looked for SUMMARY.txt and RUNNER.txt. A verdict that exists only in a terminal buffer is not
  evidence. Re-run with VERIFY_OUT='$DIR' so the summary is written to disk."
fi

# More than one DISTINCT head= is ambiguous: two runs landed in one directory and this guard
# cannot know which one you mean. Refuse rather than pick.
HEADS=$(printf '%s\n' "$SUMS" | sed -n 's/.*head=\([0-9a-f][0-9a-f]*\).*/\1/p' | sort -u)
NHEADS=$(printf '%s\n' "$HEADS" | grep -c .)
[ "$NHEADS" -ge 1 ] || die "found summary lines but could not parse a head= out of any of them:
$SUMS"
if [ "$NHEADS" -gt 1 ]; then
    die "'$DIR' holds $NHEADS different head= values: $(printf '%s ' $HEADS)
  Two runs share one output directory. Separate them before certifying; this guard will not guess."
fi
GOT=$HEADS

# --- resolve both sides to full oids --------------------------------------------------------
GOT_FULL=$(git rev-parse --verify --quiet "${GOT}^{commit}") \
    || die "the suite names head=$GOT, which does not resolve to a commit in this repository.
  Either the run was made in a different tree, or that commit has been garbage-collected."
WANT_FULL=$(git rev-parse --verify --quiet "${WANT}^{commit}") \
    || die "'$WANT' does not resolve to a commit."

# --- the verdict must itself be green, or the head check is beside the point -----------------
BAD=$(printf '%s\n' "$SUMS" | grep -v 'rc=0 ' | head -1)
[ -z "$BAD" ] || die "the summary is not green, so head= is not the problem yet:
  $BAD"
FAILED=$(printf '%s\n' "$SUMS" | sed -n 's/.*failed=\([0-9][0-9]*\).*/\1/p' | sort -u | grep -v '^0$' | head -1)
[ -z "$FAILED" ] || die "the summary reports failed=$FAILED. Fix the red before certifying a head."

# --- the actual comparison ------------------------------------------------------------------
if [ "$GOT_FULL" != "$WANT_FULL" ]; then
    echo "certify-head: REFUSING — the suite does not name the commit you are landing." >&2
    echo "  suite ran at : $(git rev-parse --short "$GOT_FULL")  $(git log -1 --format=%s "$GOT_FULL" 2>/dev/null)" >&2
    echo "  landing      : $(git rev-parse --short "$WANT_FULL")  $(git log -1 --format=%s "$WANT_FULL" 2>/dev/null)" >&2
    echo "  delta:" >&2
    git diff --stat "$GOT_FULL" "$WANT_FULL" 2>/dev/null | sed 's/^/    /' >&2
    echo "  Re-run the suite at $(git rev-parse --short "$WANT_FULL"). Do not reason about whether" >&2
    echo "  the delta matters — that judgement is what this guard exists to delete." >&2
    exit 1
fi

echo "certify-head: OK — suite head=$(git rev-parse --short "$GOT_FULL") == landing $(git rev-parse --short "$WANT_FULL")"
printf '%s\n' "$SUMS" | sed 's/^/  /'
exit 0
