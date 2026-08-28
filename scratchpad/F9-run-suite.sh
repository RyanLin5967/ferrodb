#!/bin/bash
# Per-target suite run with a durable, RESUMABLE result file. A full `cargo test` on this machine
# is SIGTERMed mid-suite by the agent fleet, and an interrupted run reports as a pass — so each
# target's result is appended the moment it lands and an already-recorded target is skipped.
export PATH="$HOME/.cargo/bin:$PATH"
OUT=scratchpad/F9-suite.txt
touch "$OUT"
have() { grep -q "^$1	" "$OUT"; }
run() {
  local label="$1"; shift
  have "$label" && return 0
  local line
  line=$(timeout 600 "$@" 2>&1 | grep -E "^test result:" | tr '\n' ' ')
  if [ -z "$line" ]; then line="NO RESULT LINE"; fi
  printf '%s\t%s\n' "$label" "$line" >> "$OUT"
}
run "LIB" cargo test --lib
for t in tests/*.rs; do
  run "$(basename "$t" .rs)" cargo test --test "$(basename "$t" .rs)"
done
printf 'DONE\t-\n' >> "$OUT"
