#!/bin/bash
# One lock hold: clean -> mutant A (sec) -> restore -> mutant B (primary) -> restore -> clean.
SP="$1"
W=/Users/idide/wt/ferrodb-d187-null-index-scan
LOCK=/tmp/ferrodb-suite.lock; ME=""
restore(){ cd "$W" && git checkout -- src/execution/sec_index_scan.rs src/execution/index_scan.rs; }
rel(){ restore; [ -n "$ME" ] && [ "$(cat $LOCK/owner 2>/dev/null)" = "$ME" ] && rm -rf "$LOCK"; }; trap rel EXIT
until mkdir "$LOCK" 2>/dev/null; do o=$(awk '{print $1}' "$LOCK/owner" 2>/dev/null)
  [ -n "$o" ] && ! kill -0 "$o" 2>/dev/null && { rm -rf "$LOCK"; continue; }; sleep 5; done
ME="$$ d187-guard-firecheck $(date -u +%FT%TZ)"; printf '%s\n' "$ME" > "$LOCK/owner"
export PATH="$HOME/.cargo/bin:$PATH"
cd "$W" || exit 99
SWAP='$n = s/^([ \t]*)self\.examined \+= 1;\n((?:[ \t]*\/\/[^\n]*\n)*[ \t]*if self\.skip_nulls && matches!\((?:sec|key), Value::Null\) \{\n[ \t]*continue;\n[ \t]*\}\n)/$2$1self.examined += 1;\n/mg; die "want 1 got $n" unless $n == 1'
phase(){ # name file(or -) 
  name="$1"; f="$2"; out="$SP/fc_$name.txt"
  {
    echo "# phase: $name"
    echo "# lock: $ME"
    echo "# head: $(git rev-parse HEAD)"
    echo "# shasum BEFORE sec_index_scan.rs: $(shasum src/execution/sec_index_scan.rs | awk '{print $1}')"
    echo "# shasum BEFORE index_scan.rs:     $(shasum src/execution/index_scan.rs | awk '{print $1}')"
    if [ "$f" != "-" ]; then
      perl -0777 -pi -e "$SWAP" "$f" || { echo "# MUTATION FAILED TO APPLY"; exit 98; }
      echo "# mutated: $f"
      echo "# shasum AFTER  $f: $(shasum "$f" | awk '{print $1}')"
      git diff -- "$f"
    fi
    echo "# git status:"; git status --porcelain
    echo "# cmd: cargo test --test d187_null_skip_is_counted -- --nocapture"
    echo "# start: $(date -u +%FT%TZ)"
    timeout 3000 cargo test --test d187_null_skip_is_counted -- --nocapture 2>&1
    echo "# test rc=$?  end: $(date -u +%FT%TZ)"
    if [ "$f" != "-" ]; then
      git checkout -- "$f"
      echo "# restored $f via git checkout --; shasum now: $(shasum "$f" | awk '{print $1}')"
      echo "# git status after restore:"; git status --porcelain
    fi
  } > "$out" 2>&1
  grep '^# test rc=' "$out"
}
phase 0_clean -
phase A_sec_swapped src/execution/sec_index_scan.rs
phase B_primary_swapped src/execution/index_scan.rs
phase Z_clean_after -
echo DONE
