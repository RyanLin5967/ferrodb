#!/bin/bash
# A FAST PRE-CHECK, NOT A VERIFICATION. Runs the lib tests plus only the integration targets a
# change plausibly reaches, so iterating costs ~2 minutes instead of ~25.
#
# WHY IT EXISTS. `verify-suite.sh` compiles and links 90 separate test binaries; the runtime is
# mostly LINKING, not testing, so the cost is nearly the same whether a change touched one module
# or forty. Running it after every edit to one file burned most of a day.
#
# ⛔ THIS SCRIPT CANNOT TELL YOU A CHANGE IS SAFE. It prints, every time, how many targets it did
# NOT run. The full suite remains the gate before any push, and it earns that: in this repo the
# full suite has caught, in three separate runs, a catalog that was never flushed to disk, a
# `set_root` that silently deleted every arena a branch owned, and a child of a governed branch
# coming back ungoverned. NONE of those were visible to the lib tests.
#
#   tools/verify-impacted.sh              # changed-vs-HEAD, fast tier
#   tools/verify-impacted.sh --binaries   # also every target that spawns a real binary
#   tools/verify-impacted.sh --since <ref> # changed-vs-ref
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
export PATH="$HOME/.cargo/bin:$PATH"

WITH_BINARIES=0
SINCE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --binaries) WITH_BINARIES=1 ;;
    --since) shift; SINCE="$1" ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

if [ -n "$SINCE" ]; then
  CHANGED=$(git diff --name-only "$SINCE" -- src examples tests 2>/dev/null)
else
  CHANGED=$( { git diff --name-only -- src examples tests; git diff --cached --name-only -- src examples tests; } | sort -u )
fi

if [ -z "$CHANGED" ]; then
  echo "REFUSED: no changed files under src/, examples/ or tests/."
  echo "  A run that selected nothing has not passed. Name a ref with --since, or make a change."
  exit 1
fi

echo "changed:"; printf '%s\n' "$CHANGED" | sed 's/^/  /'

# Symbols a change could be reached through: types, plus the module's own file stem, plus
# free functions. `pub fn` names are kept because a trait method rename is exactly the kind of
# change an integration test sees.
SYMS=$(
  { printf '%s\n' "$CHANGED" | while read -r f; do
      [ -f "$f" ] || continue
      basename "$f" .rs
      grep -hoE '^\s*pub (struct|enum|trait|fn) [A-Za-z_][A-Za-z0-9_]*' "$f" 2>/dev/null \
        | awk '{print $NF}'
    done
  } | sort -u
)

# ⛔ DISCRIMINATING POWER, NOT A STOPWORD LIST. The first version of this script selected 71 of 90
# targets for a one-file change, because `pub fn create` and `pub fn open` appear in essentially
# every test file in the repo. A hand-maintained stopword list is the wrong fix: it only ever
# excludes the words somebody already tripped over, which is the denylist failure mode. So each
# candidate is MEASURED - a symbol matching more than a third of the suite is not selecting, it is
# noise, and it is dropped and named. Self-tuning, and it reports what it threw away.
TOTAL=$(ls tests/*.rs | wc -l | tr -d ' ')
NOISE_AT=$(( TOTAL / 3 ))
KEPT=""; DROPPED=""
while read -r s; do
  [ -n "$s" ] || continue
  hits=$(grep -lw -- "$s" tests/*.rs 2>/dev/null | wc -l | tr -d ' ')
  if [ "$hits" -gt "$NOISE_AT" ]; then DROPPED="$DROPPED $s($hits)"; else KEPT="$KEPT $s"; fi
done <<< "$SYMS"

echo "selectors: $KEPT"
[ -n "$DROPPED" ] && echo "not selectors (match >$NOISE_AT of $TOTAL targets, so they select nothing):$DROPPED"

SELECTED=$(
  { for t in tests/*.rs; do
      n=$(basename "$t" .rs)
      if printf '%s\n' "$CHANGED" | grep -qx "$t"; then echo "$n"; continue; fi
      for s in $KEPT; do
        if grep -qw -- "$s" "$t" 2>/dev/null; then echo "$n"; break; fi
      done
    done
    if [ "$WITH_BINARIES" = 1 ]; then
      grep -ln 'Command::new\|cli_run\|spawn(' tests/*.rs 2>/dev/null \
        | while read -r t; do basename "$t" .rs; done
    fi
  } | sort -u
)

COUNT=$(printf '%s\n' "$SELECTED" | grep -c . || true)
SKIPPED=$(( TOTAL - COUNT ))

echo
echo "selected $COUNT of $TOTAL integration targets:"
printf '%s\n' "$SELECTED" | sed 's/^/  /'
echo

# Examples must be rebuilt or targets that spawn a binary test a stale one - this repo's harness
# refuses outright when it detects that, which is the correct behaviour and a slow way to find out.
echo "==> building examples (a stale binary makes every spawning target meaningless)"
timeout 900 cargo build --examples 2>&1 | grep -E '^error' -A 4 | head -20

ARGS=""
while read -r n; do [ -n "$n" ] && ARGS="$ARGS --test $n"; done <<< "$SELECTED"

echo "==> lib tests + selected targets"
# shellcheck disable=SC2086
timeout 1800 cargo test --lib $ARGS 2>&1 | grep -E '^(test result|error|thread .* panicked)' -A 2 | head -60
RC=${PIPESTATUS[0]}

echo
echo "------------------------------------------------------------------"
echo "rc=$RC   ⛔ SKIPPED $SKIPPED of $TOTAL TARGETS. THIS IS NOT A VERIFICATION."
echo "Before pushing, run:  ./tools/verify-suite.sh <label>"
[ "$WITH_BINARIES" = 0 ] && echo "Targets that spawn a real binary were NOT selected unless a symbol matched. Those are the"
[ "$WITH_BINARIES" = 0 ] && echo "ones that caught every serious bug in the catalog work. Use --binaries before you trust this."
echo "------------------------------------------------------------------"
exit "$RC"
