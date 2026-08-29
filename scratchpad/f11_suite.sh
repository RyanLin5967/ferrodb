#!/usr/bin/env bash
# Per-target suite run with a durable result file.
#
# Per target and not one `cargo test`: a full run on this machine has been SIGTERMed mid-suite by
# other work, and a suite that was killed reports as a suite that was never counted. Each target's
# result line is appended as it finishes, so a kill loses one target rather than the number.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
# CI's exact flags. `-D dead_code` is the disabled-test gate: a seam with no caller, or a `#[test]`
# attribute that landed on the wrong function, fails the build there and must fail it here too.
export RUSTFLAGS="-D duplicate_macro_attributes -D dead_code"
cd /Users/idide/wt/ferrodb-F11-server-reaps
OUT=scratchpad/F11-suite.txt
: > $OUT
run() {
  local label="$1"; shift
  local line
  line=$(timeout 1800 cargo test "$@" 2>&1 | grep -E "^test result" | tail -1)
  if [ -z "$line" ]; then line="NO RESULT LINE — refused, killed or failed to build"; fi
  printf '%-46s %s\n' "$label" "$line" >> $OUT
}
# Examples first: `cargo test` does not rebuild them, and several targets spawn them behind a
# freshness guard that refuses on a stale binary.
if ! timeout 1800 cargo build --examples > scratchpad/F11-suite-build.txt 2>&1; then
  echo "EXAMPLES DID NOT BUILD under the CI RUSTFLAGS — see scratchpad/F11-suite-build.txt" >> $OUT
  exit 1
fi
run "lib" --lib
run "doc" --doc
for f in tests/*.rs; do
  t=$(basename "$f" .rs)
  run "$t" --test "$t"
done
echo "SUITE DONE" >> $OUT
