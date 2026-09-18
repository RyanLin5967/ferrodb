#!/usr/bin/env bash
# prepush.sh — run the checks CI runs that a LOCAL GREEN SUITE DOES NOT COVER.
#
# WHY THIS EXISTS. Two pushes on 2026-09-18 turned CI red behind a locally green 2065-test suite,
# and neither could have been caught by running the suite again:
#
#   1. `examples/runtime_curve.rs` declared POSIX `kill` and `getrusage` with no `cfg(unix)` gate.
#      Windows fails at `LNK2019: unresolved external symbol`. ⛔ `cargo check` CANNOT see it --
#      IT DOES NOT LINK -- and `cargo test` never builds for Windows at all.
#   2. `LogBranchCatalog::put` lost its last non-test caller, which under CI's `-D dead_code`
#      is an ERROR. ⛔ `cargo test` compiles with `cfg(test)`, where the test module still calls
#      it, so the whole suite is green while `cargo build --examples` fails to compile the lib.
#
# Both share a shape: **a change that makes something unused, or that adds FFI, is invisible to the
# suite and fatal to CI.** A correctness fix that deletes callers is the likeliest trigger, so the
# cleaner the refactor the more this is needed.
#
# ⭐ CI's RUSTFLAGS are READ FROM THE WORKFLOW FILE, never hardcoded here. Hardcoding them means
# this script goes stale the moment CI changes and then reports a green that means nothing -- the
# exact failure it exists to prevent.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

WF=.github/workflows/tests.yml
fail=0
step() { printf '\n== %s ==\n' "$1"; }
bad()  { echo "PREPUSH: REFUSING — $*"; fail=1; }

[ -f "$WF" ] || { echo "PREPUSH: REFUSING — cannot find $WF; I do not know what CI runs."; exit 1; }

# --- CI's RUSTFLAGS, from the workflow itself -------------------------------------------------
FLAGS=$(sed -n 's/^[[:space:]]*RUSTFLAGS:[[:space:]]*"\(.*\)"[[:space:]]*$/\1/p' "$WF" | head -1)
if [ -z "$FLAGS" ]; then
    echo "PREPUSH: REFUSING — no RUSTFLAGS found in $WF."
    echo "  Either CI stopped setting them (then delete this check deliberately) or the format"
    echo "  changed and this script would otherwise pass while testing the wrong thing."
    exit 1
fi
echo "CI RUSTFLAGS (read from $WF): $FLAGS"

# --- 1. every extern "C" must sit under a cfg(unix) gate --------------------------------------
step "FFI gate sweep (examples/ and src/)"
UNGATED=$(python3 - <<'PY'
import os, sys
bad = []
for root in ("examples", "src"):
    for dp, _, fns in os.walk(root):
        for fn in fns:
            if not fn.endswith(".rs"):
                continue
            p = os.path.join(dp, fn)
            lines = open(p, encoding="utf-8", errors="replace").read().split("\n")
            for i, l in enumerate(lines):
                if 'extern "C"' in l:
                    ctx = "\n".join(lines[max(0, i - 16):i])
                    if "cfg(unix)" not in ctx and 'cfg(target_family = "unix")' not in ctx:
                        bad.append(f"{p}:{i+1}")
print("\n".join(bad))
PY
)
if [ -n "$UNGATED" ]; then
    bad "extern \"C\" with no cfg(unix) gate within 16 lines above:"
    echo "$UNGATED" | sed 's/^/    /'
    echo "    These link-fail on Windows (LNK2019) and cargo check cannot see it."
else
    echo "ok: no ungated extern \"C\""
fi

# --- 2. the two builds CI does, with CI's flags ------------------------------------------------
# `cargo build --examples` is the one that LINKS. `check --all-targets` compiles the
# cfg(not(unix)) arms, which are dead code locally and otherwise never typechecked at all.
for cmd in "build --examples" "check --all-targets"; do
    step "RUSTFLAGS=\"$FLAGS\" cargo $cmd"
    # shellcheck disable=SC2086
    if RUSTFLAGS="$FLAGS" timeout 1800 cargo $cmd 2>&1 | tee /tmp/prepush.$$ | tail -3; then
        grep -qE 'error\[E|^error|could not compile' /tmp/prepush.$$ && bad "cargo $cmd reported errors under CI's flags"
    else
        bad "cargo $cmd FAILED under CI's flags"
    fi
    rm -f /tmp/prepush.$$
done

# --- 3. the Windows target, for the cfg arms (it does NOT link) --------------------------------
if rustup target list --installed 2>/dev/null | grep -q x86_64-pc-windows-msvc; then
    step "cargo check --target x86_64-pc-windows-msvc (compiles the cfg(not(unix)) arms)"
    RUSTFLAGS="$FLAGS" timeout 1800 cargo check --target x86_64-pc-windows-msvc --all-targets >/tmp/prepush.win.$$ 2>&1 \
        || bad "the Windows target does not compile"
    grep -qE 'error\[E|could not compile' /tmp/prepush.win.$$ && bad "Windows target reported errors"
    rm -f /tmp/prepush.win.$$
    echo "ok (note: this does NOT link, so it cannot see a missing POSIX symbol — check 1 is what covers that)"
else
    echo "SKIPPED: x86_64-pc-windows-msvc target not installed."
    echo "  ⚠ That is a REDUCTION IN COVERAGE, not a pass. rustup target add x86_64-pc-windows-msvc"
fi

printf '\n'
if [ "$fail" = 0 ]; then
    echo "PREPUSH: OK — the checks CI runs that the suite does not."
    echo "  This is NOT a substitute for tools/verify-suite.sh, and it says nothing about head=."
    echo "  Run all three: verify-suite.sh, certify-head.sh, and this."
else
    echo "PREPUSH: REFUSED. Do not push."
fi
exit "$fail"
