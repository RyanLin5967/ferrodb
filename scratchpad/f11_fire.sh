#!/usr/bin/env bash
# Fire-check one F11 mutant: apply, prove it BUILDS, run the tests it must kill, restore.
#
# A mutant that does not build prints nothing and looks exactly like a surviving one, so the build
# gate is not optional. Raw cargo output is written to scratchpad/raw/ FIRST and the summary is
# extracted from the file afterwards — an earlier version extracted straight out of a pipe and twice
# recorded an empty entry for a test that had in fact failed, which reads exactly like a survivor.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-F11-server-reaps
OUT=scratchpad/F11-mutants.md
RAW=scratchpad/raw
mkdir -p $RAW
NAME=$1
LIBF=${2:-}
INTF=${3:-}

git checkout -- src/ >/dev/null 2>&1
python3 scratchpad/f11_mutate.py "$NAME" >/dev/null || { echo "$NAME: could not apply"; exit 1; }
printf '\n### %s\n' "$NAME" >> $OUT

if ! timeout 900 cargo build --lib --examples >"$RAW/$NAME.build" 2>&1; then
  echo "- **DID NOT BUILD** — not evidence about anything:" >> $OUT
  tail -12 "$RAW/$NAME.build" | sed 's/^/      /' >> $OUT
  git checkout -- src/; timeout 900 cargo build --lib --examples >/dev/null 2>&1
  echo "$NAME: BUILD FAILED"; exit 1
fi
echo "- compiles: yes" >> $OUT

record() { # <raw-file> <heading>
  local raw="$1" head="$2" extracted
  extracted=$(grep -E -A2 "^test result|panicked at|^---- |error\[" "$raw" | grep -v "^--$" | head -22)
  echo "- \`$head\`:" >> $OUT
  if [ -z "$extracted" ]; then
    # A run that collected nothing has not passed. Say so rather than leaving a blank that reads
    # like a mutant nobody could kill.
    echo "      NOTHING EXTRACTED from $raw — inspect it; this is not a result." >> $OUT
    return 1
  fi
  printf '%s\n' "$extracted" | sed 's/^/      /' >> $OUT
}

rc=0
if [ -n "$LIBF" ]; then
  # shellcheck disable=SC2086 -- LIBF may carry several space-separated libtest filters
  timeout 900 cargo test --lib -- $LIBF >"$RAW/$NAME.lib" 2>&1
  record "$RAW/$NAME.lib" "cargo test --lib -- $LIBF" || rc=1
fi
if [ -n "$INTF" ]; then
  # shellcheck disable=SC2086
  timeout 900 cargo test --test integration_server_reaps -- $INTF >"$RAW/$NAME.int" 2>&1
  record "$RAW/$NAME.int" "cargo test --test integration_server_reaps -- $INTF" || rc=1
fi

git checkout -- src/
timeout 900 cargo build --lib --examples >/dev/null 2>&1
echo "$NAME: done (rc=$rc)"
exit $rc
