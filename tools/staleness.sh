#!/usr/bin/env bash
# Is an agent branch's green result evidence about the tree we are about to merge it INTO?
#
# WHY THIS EXISTS. Measured 2026-09-18: four agent branches all reported a green suite, all
# were honest, and all four had branched from an `agent-isolation` that had since moved 19-25
# commits. Not one carried `tests/lock_order_allowlist.rs`, which is ON `agent-isolation` and
# exists precisely to catch the hazard two of them could create -- while S22 and D23 BOTH edit
# `BufferPoolManager`. Nothing announces this: the branch is clean, the agent is honest, the
# suite really did pass. And a `Merge` commit in the log is NOT proof -- W4's
# `5c8583a Merge commit ...` was merging its OWN sub-branch.
#
# WHY IT IS NOT JUST `merge-base --is-ancestor`. That was version 1, and it is too strict in a
# way that makes it useless in practice: ONE docs or bench commit on the base re-stales every
# branch in flight, so the check cries wolf and gets ignored -- the classic denylist failure in
# reverse. What actually predicts an unmeasured interaction is not "the base moved", it is
# "the base moved UNDER THE FILES THIS BRANCH TOUCHES".
#
# Verdicts, and each is a different action:
#   COVERED            base is an ancestor. The green is evidence about the merged tree. Merge.
#   COVERED-NO-CODE    base moved, but only in docs/bench/ledger files. Merge.
#   STALE-GREEN        base moved under code. Send it back to merge and re-run, AT MINIMUM the
#                      targets touching the overlapping files, which are named below.
#   UNKNOWN            could not be determined -> exits non-zero and REFUSES. A guard that
#                      cannot parse its own input must never fall through to allow.
#
# Usage: tools/staleness.sh [worktree-or-repo] [base-ref] [head-ref]
set -u
wt="${1:-.}"; base="${2:-agent-isolation}"; head="${3:-HEAD}"

die() { echo "UNKNOWN: $*"; exit 2; }

[ -d "$wt" ] || die "not a directory: $wt"
git -C "$wt" rev-parse --git-dir >/dev/null 2>&1 || die "not a git tree: $wt"
b=$(git -C "$wt" rev-parse --verify "$base^{commit}" 2>/dev/null) || die "cannot resolve base '$base' in $wt"
h=$(git -C "$wt" rev-parse --verify "$head^{commit}" 2>/dev/null) || die "cannot resolve head '$head' in $wt"
mb=$(git -C "$wt" merge-base "$b" "$h" 2>/dev/null) || die "no merge base between $base and $head"

behind=$(git -C "$wt" rev-list --count "$h".."$b" 2>/dev/null) || die "rev-list failed"
ahead=$(git -C "$wt" rev-list --count "$b".."$h" 2>/dev/null) || die "rev-list failed"
name=$(git -C "$wt" rev-parse --abbrev-ref "$head" 2>/dev/null)
[ -z "$name" ] || [ "$name" = "HEAD" ] && name=$(git -C "$wt" rev-parse --short "$h")
echo "branch $name vs $base: ${behind} behind, ${ahead} ahead"

if [ "$behind" -eq 0 ]; then
  echo "COVERED"; exit 0
fi

# What moved on the base, and what this branch touched. Code is what can interact; a bench
# artifact or a ledger row cannot change what a test measures.
code_re='^(src/|tests/|examples/|benches/|cdc-consumer/|build\.rs|Cargo\.toml|Cargo\.lock|tools/)'
base_files=$(git -C "$wt" diff --name-only "$mb" "$b" 2>/dev/null) || die "diff base failed"
head_files=$(git -C "$wt" diff --name-only "$mb" "$h" 2>/dev/null) || die "diff head failed"
base_code=$(printf '%s\n' "$base_files" | grep -E "$code_re" | sort -u)
head_code=$(printf '%s\n' "$head_files" | grep -E "$code_re" | sort -u)

if [ -z "$base_code" ]; then
  echo "COVERED-NO-CODE: the base moved ${behind} commit(s), none touching code."
  printf '%s\n' "$base_files" | sed 's/^/    /' | head -12
  exit 0
fi

# Could the base's code diff change BEHAVIOUR at all? A comment-only diff cannot -- with one
# Rust-specific exception that makes the naive form of this check wrong: a `///` doc comment may
# contain a ```-fenced DOCTEST, which cargo compiles and runs. So a fence marker anywhere in the
# changed lines disqualifies the claim, and the script says so rather than quietly allowing.
rust_code=$(printf '%s\n' "$base_code" | grep -E '\.rs$' || true)
other_code=$(printf '%s\n' "$base_code" | grep -vE '\.rs$' || true)
diff_body=$(git -C "$wt" diff -U0 "$mb" "$b" -- $(printf '%s ' $rust_code) 2>/dev/null)
changed=$(printf '%s\n' "$diff_body" | grep -E '^[+-]' | grep -vE '^(\+\+\+|---)')
noncomment=$(printf '%s\n' "$changed" | sed -E 's/^[+-]//' | grep -vE '^[[:space:]]*(//|$)' | grep -c . )
fences=$(printf '%s\n' "$changed" | grep -c '```')
# Report the breakdown either way -- a blunt verdict over a mixed diff is what made v1 useless.
echo "  base moved under: $(printf '%s\n' "$rust_code" | grep -c .) Rust file(s), $(printf '%s\n' "$other_code" | grep -c .) other code file(s)"
if [ "${noncomment:-1}" -eq 0 ] && [ "${fences:-1}" -eq 0 ]; then
  if [ -z "$other_code" ]; then
    echo "COVERED-COMMENTS-ONLY: every changed line in every changed Rust file is a comment,"
    echo "  and nothing else under code moved, so behaviour cannot have changed."
    printf '%s\n' "$rust_code" | sed 's/^/    /' | head -12
    exit 0
  fi
  echo "  NOTE: the Rust changes are COMMENT-ONLY, so compiled behaviour cannot have changed."
  echo "  What moved that is not Rust -- judge these yourself, a build or verification script CAN"
  echo "  change what a suite measures even though it compiles nothing:"
  printf '%s\n' "$other_code" | sed 's/^/      /' | head -12
fi
if [ "${noncomment:-1}" -eq 0 ] && [ "${fences:-0}" -ne 0 ]; then
  echo "NOTE: the base's code diff is comment-only, BUT it touches a \`\`\` fence -- a Rust doc"
  echo "  comment can carry a DOCTEST that cargo compiles and runs, so comment-only does not imply"
  echo "  behaviour-free here. Treating it as STALE-GREEN. Settle it with: cargo test --doc"
fi

overlap=$(comm -12 <(printf '%s\n' "$base_code") <(printf '%s\n' "$head_code"))
echo "STALE-GREEN: the base moved under code this branch has not seen."
echo "  code files changed on $base since the fork point: $(printf '%s\n' "$base_code" | grep -c .)"
if [ -n "$overlap" ]; then
  echo "  ** BOTH SIDES EDIT THESE -- this is where an unmeasured interaction lives:"
  printf '%s\n' "$overlap" | sed 's/^/      /'
else
  echo "  (no file is edited by both sides -- lower risk, but still re-run: a lock-order"
  echo "   inversion does not fail loudly, it hangs rarely under load, and it can be created"
  echo "   by two files that never appear in the same diff.)"
fi
echo "  tests present on $base but NOT on this branch:"
comm -13 <(git -C "$wt" ls-tree -r --name-only "$h" tests/ 2>/dev/null | sort) \
         <(git -C "$wt" ls-tree -r --name-only "$b" tests/ 2>/dev/null | sort) \
  | sed 's/^/      /' | head -10
exit 1
