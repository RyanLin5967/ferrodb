#!/bin/zsh
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-E82-ddl-atomicity
D=.e82/targets
mkdir -p "$D"
run () {
  local t=$1; shift
  grep -q "^test result" "$D/$t.txt" 2>/dev/null && return
  timeout 900 cargo test "$@" > "$D/$t.raw" 2>&1
  local rc=$?
  grep -E "^test result|^failures:|^    [a-z_0-9:]+$" "$D/$t.raw" > "$D/$t.txt" 2>/dev/null
  grep -q "^test result" "$D/$t.txt" 2>/dev/null || \
    echo "NO RESULT LINE (rc=$rc) - this target did NOT run to completion" > "$D/$t.txt"
  rm -f "$D/$t.raw"
}
# the paths this change touches, first
FIRST=(integration_alter_refusal_paths integration_merge_ddl_atomicity integration_alter_column
       integration_alter_refusal_safety integration_schema_merge integration_agent_merge
       integration_crash_safety integration_replica_applier integration_cdc_ddl)
for t in $FIRST; do [[ -f tests/$t.rs ]] && run "$t" --test "$t"; done
run __lib --lib
for f in tests/*.rs; do run "$(basename $f .rs)" --test "$(basename $f .rs)"; done
run __doc --doc
touch .e82/SWEEP_COMPLETE
