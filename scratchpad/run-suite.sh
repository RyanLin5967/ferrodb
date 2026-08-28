#!/bin/bash
# Per-target suite run with a durable result file. A single `cargo test` gets SIGTERMed mid-suite
# by the agent fleet on this machine, and an interrupted run reports as a pass.
export PATH="$HOME/.cargo/bin:$PATH"
OUT=scratchpad/F2-suite.tsv
: > "$OUT"
run() {  # name, args...
  local name="$1"; shift
  local log; log=$(timeout 1800 cargo test "$@" 2>&1)
  local line; line=$(printf '%s\n' "$log" | grep -E "^test result:" | tail -20)
  if [ -z "$line" ]; then
    printf '%s\tNO-RESULT-LINE\t%s\n' "$name" "$(printf '%s' "$log" | tail -3 | tr '\n' ' ')" >> "$OUT"
    return
  fi
  printf '%s\n' "$line" | while read -r l; do printf '%s\t%s\n' "$name" "$l" >> "$OUT"; done
}
run lib --lib
for t in $(ls tests/*.rs | sed 's|tests/||; s|\.rs$||'); do run "$t" --test "$t"; done
run doc --doc
echo DONE >> "$OUT"
