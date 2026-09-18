#!/bin/bash
# Per-target suite run with a durable, RESUMABLE result file.
#
# Per target and not one `cargo test`, because a full run on this machine is SIGTERMed mid-suite by
# the agent fleet and an interrupted run reports as a pass. Resumable, because the same fleet takes
# the cargo build-directory lock: a target that only ever saw "Blocking waiting for file lock" is
# retried and, if it still says nothing, recorded as NO RESULT LINE with its raw output kept —
# never silently counted as a pass.
export PATH="$HOME/.cargo/bin:$PATH"
OUT=bench/evidence/F9-suite.txt
DUMP="${F9_DUMP:-/tmp}"
touch "$OUT"
have() { grep -q "^$1	" "$OUT"; }
run() {
  local label="$1"; shift
  have "$label" && return 0
  local raw line
  for _ in 1 2 3; do
    raw=$(timeout 900 "$@" 2>&1)
    line=$(printf '%s\n' "$raw" | grep -E "^test result:" | tr '\n' ' ')
    [ -n "$line" ] && break
    printf '%s\n' "$raw" | tail -60 > "$DUMP/F9-suite-fail-$label.txt"
  done
  [ -z "$line" ] && line="NO RESULT LINE (raw output in $DUMP/F9-suite-fail-$label.txt)"
  printf '%s\t%s\n' "$label" "$line" >> "$OUT"
}
run "LIB" cargo test --lib
for t in tests/*.rs; do
  run "$(basename "$t" .rs)" cargo test --test "$(basename "$t" .rs)"
done
printf 'DONE\t-\n' >> "$OUT"
