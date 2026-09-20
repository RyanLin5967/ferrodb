#!/usr/bin/env bash
# tools/verify-suite.sh [label] — run the full suite and report a number you can trust.
#
# WHY THIS LIVES IN THE REPO. It used to live in the session scratchpad under /private/tmp, and
# LEDGER.md's Phase E plus RESUME-CHECKPOINT.md both instructed the next pass to run it from there.
# The machine restarted on 2026-08-20 and /private/tmp was wiped, so both documents pointed at a
# file that no longer existed — a broken instruction in the two places a fresh pass is told to look
# first. Anything a ledger tells the future to execute has to be versioned with the code.
#
# WHAT IT GUARDS. Four ways this project has produced a wrong test count before:
#   1. a run whose tree moved underneath it (a mid-write sample once read 375 against a real 374),
#   2. `cargo test | head`, where the pipe SIGPIPEs cargo and the partial total reads as a regression,
#   3. a run killed by too short a timeout, whose partial count looks like a finished one.
#   4. a suite run against STALE EXAMPLE BINARIES. `cargo test` does not rebuild `examples/`, and
#      eight integration tests carry a guard that FAILS when the binary they spawn is older than
#      `src/` or `examples/`. So any run taken straight after a merge that touched `src/` reports a
#      wave of failures that are the harness working, not the code breaking. This bit the project
#      before: a suite count "is meaningless unless examples were rebuilt after the last src/ edit".
#      The build is therefore part of the measurement, not a thing the caller is trusted to remember.
#   5. A RUST-ONLY RUN REPORTED AS "the suite". `cargo test` does not build or run `cdc-consumer/`,
#      a separate Go module holding both CDC sinks. Row I20's own summary says so — "`cargo test`
#      does not run it ... this is the suite where four of the five fixes live" — and on 2026-08-24
#      I20's fifth commit (`0783479`, the fresh-context adversarial pass that found four real
#      defects in the finding-4 fix) touched ONLY Go files, so no Rust count could see it at all.
#      A merge verified on the Rust count alone leaves such a commit entirely unmeasured. The Go
#      suite is therefore part of the measurement too, and its absence is a REFUSAL, not a skip:
#      this repo already settled that argument in `require_python_module`
#      (`tests/integration_pgwire.rs`) — "a skipped check would report success for the wrong reason".
#   6. A RUN THAT WAS NEVER SCHEDULED, reported as a RED. `integration_consensus_failover.rs` waits
#      on a 45 s wall-clock budget, and this project runs suites under an agent fleet. That budget
#      has expired FOUR times — `suite-d19-merge-0245Z`, `suite-E79c-0457Z`, `suite-E79c-1036Z`,
#      D40 run 1 — on a test that passes alone in ~3.1 s, i.e. with a 15x margin. Each cost a re-run
#      and twice nearly cost a wrong diagnosis. A wall-clock deadline structurally cannot tell "the
#      cluster failed" from "this process was descheduled", so the test now classifies its own
#      expiry from two in-process signals (its poll loop's iteration count, and the node child
#      processes' CPU per wall second) and says INCONCLUSIVE when it was starved. See D42 in
#      SCALE-DESIGN.md and the classifier block in that test.
#      ⛔ THE BUDGET WAS NOT RAISED. Raising it is "never edit a test to make it pass" wearing a
#      constant. What changed is what an expiry is allowed to MEAN.
#      This script owns the CHANNEL that verdict rides: the refusal below. INCONCLUSIVE is the same
#      principle as "zero tests collected" — this run does not get to be a number — applied to a
#      different cause, and it is strictly MORE conservative than the red it replaces: it exits
#      non-zero, prints no total, and writes no SUMMARY.txt, so nothing downstream can certify it.
# So: no pipe, a generous bound, examples rebuilt first, both suites run, and HEAD plus the
# dirty-file count compared before and after. If either moved, it refuses to print a number at all
# rather than printing one that cannot be trusted.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1

LABEL=${1:-verify}

# ── 6. A MACHINE-WIDE SUITE LOCK. Only one suite runs at a time, fleet-wide. ──────────────────
#
# Measured 2026-08-28: five dispatched agents each reached their verification phase and each ran a
# full suite at the same time on one machine. They starved each other so badly that every one sat
# in a `sleep 120` poll loop for over five hours, burning 150-250k tokens apiece waiting on runs
# that take minutes alone. Nobody was stuck and nothing was broken - the contention WAS the cost.
#
# This is a lock rather than a line in an agent brief on purpose. A brief is advice that the next
# agent may not read; a lock in the script every agent already calls makes the bad state
# unrepresentable. Serialised suites are also strictly faster in wall-clock here: five runs sharing
# the CPU finish no sooner than five runs taking turns, and taking turns produces numbers that can
# be trusted, because a starved run's timings are indistinguishable from a real regression.
#
# Set VERIFY_NOLOCK=1 to bypass, for the single-agent case where nothing can contend.
#
# ── 7. A SIGNAL MUST ABORT THE RUN, NOT JUST DROP THE LOCK. ───────────────────────────────────
#
# The release below used to be `trap 'rm -rf "$SUITE_LOCK"' EXIT INT TERM`, which is a guard that
# fails open in the exact situation it exists for. Bash runs a trap handler and then RESUMES the
# script unless the handler itself exits. So when a caller's `timeout N` fired, that handler
# deleted the machine-wide suite lock and the suite CARRIED ON — unbounded, and now with fleet-wide
# mutual exclusion disarmed, which is precisely the five-way starvation note 6 exists to prevent.
# The failure is silent in both directions: nothing reports that the bound was exceeded, and
# nothing reports that the lock is gone.
#
# MEASURED 2026-09-02, not reasoned about. Run `baseline-2258Z` was started under `timeout 5400`
# and was found 6447s in — 1047s past its own bound — still starting new targets, with
# /tmp/ferrodb-suite.lock ALREADY GONE. SIGTERM would not stop it; it took SIGKILL. A three-line
# fixture reproduces it: `trap 'rm -rf $L' TERM` plus a `sleep`, TERMed mid-sleep, still prints the
# phase after the signal and leaves the lock deleted.
#
# Two things are required and neither is sufficient alone:
#   a. the handler must EXIT, and exit as a REFUSAL. A suite cut short is not a count, so it must
#      never reach the total-printing tail — the same rule the zero-collected and tree-moved
#      guards already apply.
#   b. it must take its children with it, promptly. Bash defers a trap until the current
#      FOREGROUND command returns, so a signal arriving inside a 30-minute `cargo test` sits
#      unhandled for up to 30 minutes; and killing the script alone orphans the running cargo
#      (observed the same night: an orphaned `--test integration_cdc_feed` outlived its parent and
#      had to be killed by hand). `run_bounded` therefore backgrounds each child and `wait`s,
#      because `wait` IS interruptible by a trap while a foreground builtin is not.
_HELD_LOCK=0
_release_lock() {
    [ "${_HELD_LOCK:-0}" = "1" ] || return 0
    _HELD_LOCK=0
    rm -rf "$SUITE_LOCK"
}
_kill_descendants() {
    local p=$1 c
    for c in $(pgrep -P "$p" 2>/dev/null); do _kill_descendants "$c"; done
    kill -9 "$p" 2>/dev/null
}
_abort() {
    trap '' TERM INT                      # a second signal must not re-enter this handler
    local c
    for c in $(pgrep -P $$ 2>/dev/null); do _kill_descendants "$c"; done
    _release_lock
    echo "$LABEL: REFUSING — aborted by SIG$1 after ${SECONDS}s. A suite cut short is not a count." >&2
    echo "  This handler used to release the lock and let the run continue unbounded; see note 7." >&2
    echo "  log: ${LOG:-<not opened yet>}" >&2
    exit 143
}
# EXIT covers the normal and error paths; TERM/INT must go through _abort so they refuse.
trap '_release_lock' EXIT
trap '_abort TERM' TERM
trap '_abort INT' INT

# Every long child goes through one of these two, never a bare foreground `timeout`. $BOUND is
# read at call time, so these may be defined before it is set.
run_bounded() {
    local dest=$1; shift
    timeout "$BOUND" "$@" >> "$dest" 2>&1 &
    _child=$!
    wait "$_child"
}
run_bounded_in() {
    local dir=$1 dest=$2; shift 2
    ( cd "$dir" && exec timeout "$BOUND" "$@" ) >> "$dest" 2>&1 &
    _child=$!
    wait "$_child"
}

SUITE_LOCK=${SUITE_LOCK:-/tmp/ferrodb-suite.lock}
LOCK_WAIT=${LOCK_WAIT:-5400}          # how long to queue before giving up
if [ "${VERIFY_NOLOCK:-0}" != "1" ]; then
    _waited=0
    while ! mkdir "$SUITE_LOCK" 2>/dev/null; do
        _owner=$(cat "$SUITE_LOCK/owner" 2>/dev/null || echo "unknown")
        _pid=${_owner%% *}
        # Break a lock ONLY when its holder is genuinely gone. `$$` here is this script's own pid
        # and it lives for the whole run, so `kill -0` is meaningful - unlike a lock whose recorded
        # pid was a subshell that exited the instant it wrote the file, where the same check reports
        # dead for every healthy holder.
        if [ -n "$_pid" ] && ! kill -0 "$_pid" 2>/dev/null; then
            echo "$LABEL: suite lock held by dead pid $_pid — breaking it" >&2
            rm -rf "$SUITE_LOCK"; continue
        fi
        if [ "$_waited" -ge "$LOCK_WAIT" ]; then
            echo "$LABEL: REFUSING — waited ${LOCK_WAIT}s for the suite lock held by: $_owner" >&2
            echo "  Another suite is still running. Running anyway would give BOTH runs numbers" >&2
            echo "  that cannot be told apart from a regression." >&2
            exit 3
        fi
        [ "$_waited" -eq 0 ] && echo "$LABEL: queued behind a running suite ($_owner)" >&2
        sleep 15; _waited=$((_waited+15))
    done
    printf '%s %s %s\n' "$$" "$LABEL" "$(date -u +%FT%TZ)" > "$SUITE_LOCK/owner"
    _HELD_LOCK=1
    [ "$_waited" -gt 0 ] && echo "$LABEL: acquired the suite lock after ${_waited}s" >&2
fi
OUT=${VERIFY_OUT:-$(mktemp -d)}
# `mktemp -d` creates its directory; a caller-supplied VERIFY_OUT may not exist. Without this, every
# redirect below fails, which makes the build step look like it failed and the guard refuse with the
# wrong reason — a fail-safe direction, but a false diagnosis. Found by fire-checking the guard.
mkdir -p "$OUT" || { echo "$LABEL: REFUSING — cannot create output dir $OUT"; exit 1; }
# A STALE inconclusive marker from an earlier run into this same directory would block a later,
# honest green forever, because certify-head.sh refuses on its presence. Clearing it here is what
# makes that refusal safe to write.
rm -f "$OUT/INCONCLUSIVE.txt"
LOG="$OUT/suite-$LABEL.log"
BOUND=${VERIFY_TIMEOUT:-7200}

command -v timeout >/dev/null || { echo "$LABEL: REFUSING — no \`timeout\`; an unbounded suite can wedge a pass"; exit 1; }

h0=$(git log -1 --format=%h); d0=$(git status --short | wc -l | tr -d ' ')

# Examples first — see note 4 above. A failure here is a real build failure and must stop the run:
# continuing would measure the previous binaries and call the result a suite.
: > "$LOG.examples"
if ! run_bounded "$LOG.examples" cargo build --examples; then
    echo "$LABEL: REFUSING — \`cargo build --examples\` failed, so the suite would spawn stale binaries"
    tail -20 "$LOG.examples"
    echo "  log: $LOG.examples"
    exit 1
fi

# MODE=whole runs one `cargo test`; MODE=per-target runs the lib and each tests/*.rs separately and
# concatenates their output into the same $LOG, so everything downstream — the parsers, the
# zero-collected refusal, the tree-moved check, the Go stage — is shared and cannot drift between
# the two. Per-target exists because a single whole-suite run on this machine is not durable: with
# an agent fleet live it gets starved or SIGTERMed mid-suite, and a partial log's total reads as a
# regression (that has happened here: `1067` from a run killed inside integration_pgwire). Splitting
# it means a starved or flaky target is one short line to re-run instead of a whole suite to redo,
# and the failure is attributable to a named target. Coverage is the same set of binaries either way.
MODE=${VERIFY_MODE:-whole}
case "$MODE" in whole|per-target) ;; *)
    echo "$LABEL: REFUSING — VERIFY_MODE must be 'whole' or 'per-target', got '$MODE'. A mode that"
    echo "  silently fell back to one of them could measure less than the caller asked for."; exit 1 ;;
esac

if [ "$MODE" = whole ]; then
    : > "$LOG"; run_bounded "$LOG" cargo test --no-fail-fast; rc=$?
else
    : > "$LOG"; rc=0
    # `--lib` first, then every integration target by name. `ls tests/*.rs` and not a glob in a
    # for-list: under zsh a non-matching bare glob aborts the command, and an aborted loop prints
    # nothing, which the parser below would read as zero collected rather than as a broken sweep.
    targets=$(ls tests/*.rs 2>/dev/null | xargs -n1 basename 2>/dev/null | sed 's/\.rs$//')
    if [ -z "$targets" ]; then
        echo "$LABEL: REFUSING — per-target mode found no tests/*.rs to run. That is a broken sweep,"
        echo "  not an empty suite."; exit 1
    fi
    # A verdict must appear AFTER this target's own marker. `tail -N | grep` is WRONG here and was
    # the first version: a killed target emits only a few lines, so a fixed window still contains
    # the PREVIOUS target's `test result:` and the check passes on a stale verdict. Fire-checked with
    # a deliberately hanging fixture: the window version waved it through (its verdict line sat 2
    # lines before the marker) and the sweep carried on, dropping the target silently from the total.
    verdict_since_marker() {
        local from
        from=$(grep -n '^=== target ' "$LOG" | tail -1 | cut -d: -f1)
        [ -n "$from" ] || return 1
        tail -n +"$from" "$LOG" | grep -q '^test result:'
    }

    echo "=== per-target sweep: --lib plus $(echo "$targets" | wc -l | tr -d ' ') integration targets" >> "$LOG"
    echo "=== target --lib" >> "$LOG"
    run_bounded "$LOG" cargo test --lib --no-fail-fast || rc=$?
    # The lib run needs the same verdict-line check as every integration target below. Without it a
    # killed `--lib` contributes no `test result:` line, the loop's check never looks at it, and the
    # only remaining net is the zero-collected refusal — which does not fire, because the integration
    # targets still collect plenty. The total would come back ~778 short and green. Found by reading
    # this block back after writing it, not by a test.
    if ! verdict_since_marker; then
        echo "$LABEL: REFUSING — \`cargo test --lib\` produced no 'test result:' line, so it was"
        echo "  killed, timed out, or failed to build. Its ~778 tests would silently vanish from the"
        echo "  total and the remaining targets would still report green."
        echo "  log: $LOG"; exit 1
    fi
    for t in $targets; do
        echo "=== target $t" >> "$LOG"
        run_bounded "$LOG" cargo test --test "$t" --no-fail-fast || rc=$?
        # Every target must produce a verdict line. A target that produced none was killed, timed
        # out, or failed to build, and its silence must not be averaged away into a green total.
        if ! verdict_since_marker; then
            echo "$LABEL: REFUSING — target '$t' produced no 'test result:' line, so it was killed,"
            echo "  timed out, or failed to build. A silent target is not a passing one, and its"
            echo "  tests would vanish from the total while every other target still reported green."
            echo "  log: $LOG"; exit 1
        fi
    done
fi

# The Go module, AFTER the Rust suite and never beside it. Both suites bind TCP ports, and this
# project has a documented load-sensitive port race; row I19's own resume state carries the same
# instruction ("only AFTER the Rust suite; port contention"). Sequential is slower and honest.
GOLOG="$OUT/suite-$LABEL.go.log"
gorc=0; gp=0; gf=0; go_ran=no
if [ -f cdc-consumer/go.mod ]; then
    go_ran=yes
    if ! command -v go >/dev/null; then
        echo "$LABEL: REFUSING — cdc-consumer/go.mod exists but there is no \`go\` on PATH, so the"
        echo "  suite that holds both CDC sinks cannot run. A skipped check reports success for the"
        echo "  wrong reason; install Go or delete the module, but do not measure without it."
        exit 1
    fi
    # -count=1 defeats Go's per-package result cache: a cached `ok` is not a run.
    # -mod=readonly so a missing dependency cannot rewrite go.sum and trip the tree-moved check
    # below with a false "the tree moved" instead of the real "your module is incomplete".
    : > "$GOLOG"
    run_bounded_in cdc-consumer "$GOLOG" go test -v -count=1 -mod=readonly ./...
    gorc=$?
    gp=$(grep -c '^--- PASS' "$GOLOG"); gf=$(grep -c '^--- FAIL' "$GOLOG")
    # A run that collected nothing has not passed. `go test` prints "no test files" and exits 0,
    # which is a zero-collected run wearing a green exit code.
    if [ "$gp" -eq 0 ] && [ "$gf" -eq 0 ]; then
        echo "$LABEL: REFUSING — the Go suite collected zero tests (rc=$gorc). That is a broken run,"
        echo "  not a green one."
        tail -20 "$GOLOG"; echo "  log: $GOLOG"; exit 1
    fi
fi

h1=$(git log -1 --format=%h); d1=$(git status --short | wc -l | tr -d ' ')

if [ "$h0" != "$h1" ] || [ "$d0" != "$d1" ]; then
    echo "$LABEL: UNTRUSTWORTHY — the tree moved during the run ($h0/$d0 -> $h1/$d1). Re-run; do not record this."
    echo "  log: $LOG"
    exit 1
fi

# ── THE INCONCLUSIVE CHANNEL (note 6). A starved run is not a red. ──────────────────────────────
#
# These two strings are `VERDICT_INCONCLUSIVE` and `VERDICT_CLASSIFIER_BROKEN` in
# tests/integration_consensus_failover.rs. A shell script cannot read a Rust constant, so the
# duplication is unavoidable; `tools/verify-suite-selftest.sh` part 3 fails if the copies diverge,
# which is what stops it rotting into a guard that greps for a string nothing emits any more.
#
# This runs AFTER the tree-moved check on purpose: a run whose tree moved is untrustworthy whatever
# its tests said, and that verdict must not be overwritten by a gentler one.
D42_INCONCLUSIVE='FERRODB-VERDICT: INCONCLUSIVE'
D42_BROKEN='FERRODB-VERDICT: CLASSIFIER-BROKEN'
if grep -qF "$D42_INCONCLUSIVE" "$LOG" 2>/dev/null || grep -qF "$D42_BROKEN" "$LOG" 2>/dev/null; then
    nf=$(awk '/^test result:/ {gsub(/;/,""); for(i=1;i<=NF;i++) if($i=="failed") s+=$(i-1)} END {print s+0}' "$LOG")
    {
        echo "$LABEL: INCONCLUSIVE at $(date -u +%FT%TZ) — head=$h1 log=$LOG"
        grep -F -A 8 -e "$D42_INCONCLUSIVE" -e "$D42_BROKEN" "$LOG"
    } > "$OUT/INCONCLUSIVE.txt"
    echo "$LABEL: REFUSING — a test classified its own wall-clock expiry as INCONCLUSIVE: it was" >&2
    echo "  descheduled, or its child processes were, so the expiry is not evidence about the code." >&2
    grep -F -m1 -e "$D42_INCONCLUSIVE" -e "$D42_BROKEN" "$LOG" | sed 's/^/  /' >&2
    echo "  This run does not get to be a number in EITHER direction — not a pass, and not the red" >&2
    echo "  it would have been reported as before. Re-run it on a quieter machine." >&2
    echo "  The run also recorded failed=$nf. Those are not dismissed; they are UNMEASURED until a" >&2
    echo "  run that was not starved reports them." >&2
    echo "  evidence: $OUT/INCONCLUSIVE.txt" >&2
    echo "  log: $LOG" >&2
    exit 4
fi

p=$(awk '/^test result:/ {gsub(/;/,""); for(i=1;i<=NF;i++) if($i=="passed") s+=$(i-1)} END {print s+0}' "$LOG")
f=$(awk '/^test result:/ {gsub(/;/,""); for(i=1;i<=NF;i++) if($i=="failed") s+=$(i-1)} END {print s+0}' "$LOG")
be=$(grep -cE '^error(\[|:)' "$LOG")
# A run that collected nothing has not passed, whatever its exit code says.
if [ "${p:-0}" -eq 0 ]; then
    echo "$LABEL: REFUSING — zero tests collected (rc=$rc). That is a broken run, not a green one."
    echo "  log: $LOG"; exit 1
fi
summary="$LABEL: mode=$MODE rc=$rc passed=$p failed=$f build_errors=$be head=$h1 log=$LOG"
if [ "$go_ran" = yes ]; then
    gosummary="$LABEL: go rc=$gorc passed=$gp failed=$gf log=$GOLOG"
else
    gosummary="$LABEL: go NOT RUN — no cdc-consumer/go.mod in this tree"
fi
# PERSIST THE VERDICT. A summary that exists only in a terminal buffer is not evidence, and `head=`
# is the one field `tools/staleness.sh` structurally cannot check — it compares BRANCH to BASE,
# never SUITE to BRANCH. That gap has produced the same defect three times here, once inside D40's
# own evidence directory. The failure is always invisible to `git log`, because an amended commit
# keeps its subject line. `tools/certify-head.sh` reads this file at land time.
# Refuse if it cannot be written: a verdict that could not be recorded is not a verdict.
if ! printf '%s\n%s\n' "$summary" "$gosummary" > "$OUT/SUMMARY.txt"; then
    echo "$LABEL: REFUSING — cannot write $OUT/SUMMARY.txt; the verdict would exist only on screen."
    exit 1
fi
echo "$summary"
echo "$gosummary"
[ "$f" = "0" ] && [ "$be" = "0" ] && [ "$rc" = "0" ] && [ "$gorc" = "0" ] && [ "$gf" = "0" ]
