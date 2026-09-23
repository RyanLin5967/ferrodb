#!/usr/bin/env bash
# bench/d181_run.sh <OUT> <HALF-LABEL> <NOTE> — build and run examples/d181_conjunct_order,
# writing a provenance header followed by the harness output.
#
# WHY THIS EXISTS RATHER THAN A COMMAND IN A REPORT.
#
# 1. PROVENANCE. The first version of bench/d181_conjunct_order_BEFORE_RAW.txt had no header at
#    all: no commit, no command, no binary path. A number whose tree is unknown cannot be
#    re-derived, and this project has repeatedly quoted an artifact whose own line 1 said it was
#    measured somewhere other than where the reader assumed. The header is emitted by the same
#    script that does the run, so it cannot drift from it and cannot be forgotten.
#
# 2. IT REFUSES WHILE ANOTHER AGENT'S SUITE OR BENCH HOLDS THE MACHINE-WIDE LOCK, and that is the
#    whole point of it being a script. Measured 2026-09-23: I checked /tmp/ferrodb-suite.lock once,
#    found it free, and then ran four cargo invocations over the following minutes. The lock changed
#    hands under me and the last three landed inside another agent's suite window, stealing one to
#    two minutes of multi-core work from a run whose consensus-failover test has a 45 s wall-clock
#    budget that this project has already blown four times to descheduling.
#
#    The refusal is NOT about protecting this measurement. Every number this harness prints is an
#    integer count of tuples pulled and index entries walked; those are identical on a quiet box and
#    a loaded one, which is why this row uses counts and not durations at all. The refusal protects
#    OTHER PEOPLE'S runs from this one. A guard I have to remember to run is not a guard — that is
#    precisely the thing that failed, twice in one day.
#
#    VERIFY_IGNORE_LOCK=1 bypasses it, for the genuinely single-agent case. Say so in the artifact
#    if you use it.
#
#    ⚠ THE FIRST VERSION OF THIS GUARD HAD THE BUG IT EXISTS TO PREVENT. It checked the lock ONCE,
#    at script start, and then ran two heavy steps -- a release build and the harness itself. That
#    is precisely the shape that failed by hand: check once, then act several times while the lock
#    changes hands underneath. A guard that inherits the bug looks like protection and provides
#    none. `lock_check` is therefore called IMMEDIATELY BEFORE EACH heavy step, and reads the state
#    at the moment of the action rather than at the moment of the decision.
#
#    IT ALSO REFUSES WHEN IT CANNOT TELL. The test is on the lock DIRECTORY, not on the `owner`
#    file inside it: a run that has created the directory but not yet written `owner` holds the
#    lock just as much, and testing the file would have fallen through to ALLOW in exactly that
#    window. If the directory exists but `owner` is unreadable or missing, it refuses and says so
#    rather than guessing. A guard that cannot parse its own input must ask, never allow.
#
# 3. THE DIRTY-FILE COUNT EXCLUDES THE OUTPUT FILE. The first header reported `dirty files: 1`
#    because `git status` saw the artifact the run was in the middle of writing. A provenance field
#    that counts the run's own output is a field that can never read zero, so it would have been
#    ignored within a week.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1

OUT=${1:?usage: d181_run.sh <out> <half-label> <note>}
HALF=${2:?}
NOTE=${3:-}
LOCKDIR=/tmp/ferrodb-suite.lock
LOCK=$LOCKDIR/owner
BIN=$(pwd)/target/release/examples/d181_conjunct_order

# Refuse unless the lock can be POSITIVELY established as absent. Called immediately before every
# heavy step -- see note 2. Tests the DIRECTORY, because a run that has created it but not yet
# written `owner` holds the lock just as much.
lock_check() {
    [ "${VERIFY_IGNORE_LOCK:-0}" = "1" ] && return 0
    [ -e "$LOCKDIR" ] || return 0
    echo "REFUSING ($1) — the machine-wide suite lock is held:" >&2
    if [ -r "$LOCK" ]; then
        sed 's/^/    /' "$LOCK" >&2
    else
        echo "    (the lock directory exists but its owner file is unreadable or absent." >&2
        echo "     Refusing anyway: a guard that cannot read its own input must ask, never allow.)" >&2
    fi
    echo "  Working here would steal CPU from their run. This harness's numbers are integers and" >&2
    echo "  do not need a quiet box; their suite does. Wait for the lock, or set" >&2
    echo "  VERIFY_IGNORE_LOCK=1 and say so in the artifact." >&2
    exit 2
}

lock_check "before the build"
if ! cargo build --release --example d181_conjunct_order >/dev/null 2>&1; then
    echo "REFUSING — the harness did not build. No artifact written." >&2
    cargo build --release --example d181_conjunct_order 2>&1 | tail -20 >&2
    exit 1
fi

sha=$(git rev-parse HEAD)
branch=$(git rev-parse --abbrev-ref HEAD)
[ "$branch" = HEAD ] && branch="(detached HEAD -- no branch)"
# Exclude the artifact this run is about to write; see note 3.
dirty_files=$(git status --porcelain -- . ":(exclude)$OUT")
dirty=$(printf '%s' "$dirty_files" | grep -c . | tr -d ' ')

tmp=$(mktemp)
{
    echo "=============================================================================="
    echo "PROVENANCE -- $HALF"
    echo "=============================================================================="
    echo "commit        : $sha"
    echo "branch        : $branch"
    echo "dirty files   : $dirty   (excluding this artifact; 0 means the binary IS this commit)"
    # NAME them when non-zero. A bare count cannot tell a reader whether the dirt was a SOURCE file
    # -- which would mean the binary is not this commit and the header is lying -- or an unrelated
    # text artifact. The first AFTER run reported `1` and the reader had no way to find out which.
    if [ "$dirty" != "0" ]; then
        echo "                ^ WHICH:"
        printf '%s\n' "$dirty_files" | sed 's/^/                  /'
        echo "                Check this list before trusting the run: anything under src/ or"
        echo "                examples/ means the binary does NOT correspond to the commit above."
    fi
    echo "note          : $NOTE"
    echo "binary        : $BIN"
    echo "binary mtime  : $(stat -f '%Sm' -t '%FT%TZ' "$BIN" 2>/dev/null)"
    echo "built with    : cargo build --release --example d181_conjunct_order"
    echo "rustc         : $(rustc --version)"
    echo "command       : $BIN"
    echo "env           : none. The harness reads no environment variable; sizes are SIZES in its"
    echo "                source and the fixture is built in-process in a fresh tempdir per arm."
    echo "run at        : $(date -u +%FT%TZ)"
    echo "host          : $(uname -srm)"
    echo "load at start : $(uptime | sed 's/.*load averages*: //')"
    echo "suite lock    : free, checked immediately before the build AND immediately before the"
    echo "                run -- not once at start. (VERIFY_IGNORE_LOCK=${VERIFY_IGNORE_LOCK:-0})"
    echo ""
    echo "LOAD IMMUNITY. Every number below is an INTEGER COUNT -- heap tuples pulled and index"
    echo "entries walked -- not a duration. The same plan examines the same rows on a quiet box and"
    echo "a loaded one, so the load line above is recorded for completeness and is NOT a caveat."
    echo "Two of this project's timing runs have been voided by a shared machine"
    echo "(bench/d101_rerun_VOID_disk_emergency.txt, rc=137). This measurement has no such failure"
    echo "mode, which is why the row was specified in counts rather than durations."
    echo ""
    echo "REPRODUCE:"
    echo "  git checkout $sha"
    echo "  bench/d181_run.sh <out> '$HALF' '$NOTE'"
    echo ""
    echo "EXPECTED VALUES were pre-registered in bench/d181_prereg.txt, committed before the first"
    echo "run of this harness. Amendments to that file are append-only."
    echo "=============================================================================="
    echo ""
} > "$tmp"

# Re-checked here, not inherited from the check above: the build may have taken minutes and the
# lock changes hands. This is the second heavy step and it gets its own look at the state.
lock_check "before the harness run"
if ! "$BIN" >> "$tmp" 2>&1; then
    echo "REFUSING — the harness exited non-zero (its own fire-check refuses a run it cannot" >&2
    echo "  vouch for). No artifact written. Output kept at $tmp" >&2
    exit 1
fi

mv "$tmp" "$OUT"
echo "wrote $OUT at $sha"
