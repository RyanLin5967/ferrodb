#!/usr/bin/env bash
# F1 mutant fire-check: apply one patch to the fix commit, build the lib test binary, restore transport.rs FROM GIT
# before running anything, then run every consensus:: test against the mutant's copied binary.
# Usage: bash bench/f1_accept/fire.sh <fix-sha> <mutant-name>...   (refuses unless HEAD is <fix-sha> and the tree is clean)
set -u
export PATH="$HOME/.cargo/bin:$PATH"
WT=/Users/idide/wt/ferrodb-d224-f1-accept.noindex
S=/private/tmp/claude-501/-Users-idide-projects-ferrodb/b2b44149-483d-42d1-b512-89bf5de5a135/scratchpad/r11-dist/f1
FIX=${1:?fix sha}; shift
cd "$WT" || exit 2
[ "$(git rev-parse --short HEAD)" = "$FIX" ] || { echo "REFUSED: HEAD is not $FIX"; exit 2; }
trap 'git checkout -- src/consensus/transport.rs' EXIT
for m in "$@"; do
  out=bench/f1_accept/raw/${m}_$FIX.txt
  [ -z "$(git status --porcelain -- src)" ] || { echo "REFUSED: src is dirty before $m"; exit 2; }
  git apply "bench/f1_accept/mutants/$m.patch" || { echo "REFUSED: $m does not apply"; exit 2; }
  { echo "# mutant $m on $FIX, build start $(date -u +%FT%TZ) load=$(sysctl -n vm.loadavg)"
    taskpolicy -b timeout 5400 cargo test --lib --no-run 2>&1 | grep -E '^error|Executable|Finished'
    echo "# build rc=${PIPESTATUS[0]}"; } > "$out" 2>&1
  cp target/debug/deps/ferrodb-d929e200b07daf14 "$S/ferrodb_test_$m"
  git checkout -- src/consensus/transport.rs
  [ -z "$(git status --porcelain -- src)" ] || { echo "REFUSED: restore failed after $m"; exit 2; }
  { echo "# run start $(date -u +%FT%TZ) load=$(sysctl -n vm.loadavg) binary sha256 $(shasum -a 256 "$S/ferrodb_test_$m" | cut -c1-16)"
    taskpolicy -b timeout 1800 "$S/ferrodb_test_$m" consensus:: 2>&1
    echo "# rc=$? end $(date -u +%FT%TZ)"; } >> "$out" 2>&1
  echo "$m: $(grep '^test result' "$out")"
done
