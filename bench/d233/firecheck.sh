#!/usr/bin/env bash
# D233 fire-check: each mutant reverts ONE piece of free-at-empty in BPlusTreeManager::delete and must
# turn at least one named test red. Pre-registration: artie-research frontier/lane_d233_free_at_empty.md
# section 5. Run from the worktree root at DEFAULT QoS (never taskpolicy -b). This is fan work: it runs
# only when the lead releases the FAN-QUEUE row for D233.
#
# Blind spots, stated: three selections per mutant (storage::index unit tests, the catalog's lease-pass
# test, and three B+tree integration targets), not the whole suite, so a mutant killed only elsewhere
# reads as a survivor here. A compile error is recorded as COMPILE-FAIL, which is not a kill.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=2a1b783           # the tree these mutants were written against
OUT=bench/d233/firecheck
mkdir -p "$OUT"

if ! git diff --quiet "$SUBJECT_SHA" -- src/ tests/; then
  echo "REFUSED: src/ or tests/ differ from $SUBJECT_SHA; the mutants' text may not apply" >&2
  exit 2
fi
if [ -n "$(git status --porcelain -- src/ tests/)" ]; then
  echo "REFUSED: uncommitted changes under src/ or tests/" >&2
  exit 2
fi

# name|file|old text (python literal)|new text (python literal)
# '|' is the field separator, so a literal pipe inside mutant text is written |.
I=src/storage/index.rs
MUTANTS=(
  "U1_never_unlink|$I|"'"        leaf.key_arr.is_empty() && (leaf.prev.is_some() \u007c\u007c leaf.next.is_some())\n"|"        false && leaf.key_arr.is_empty()\n"'
  "U2_no_prev_splice|$I|"'"            left.next = leaf.next;\n"|""'
  "U3_no_next_splice|$I|"'"            right.prev = leaf.prev;\n"|""'
  "U4_parent_keeps_pointer|$I|"'"        self.unlink_from_parent(stack, leaf_id)\n"|"        let _ = stack;\n        Ok(())\n"'
  "U5_wrong_child_removed|$I|"'"            parent.child_ptrs.remove(slot);\n"|"            parent.child_ptrs.remove(if slot == 0 { 1 } else { slot - 1 });\n"'
  "U6_no_cascade|$I|"'"            if parent.child_ptrs.len() == 1 {\n                doomed = parent_id;\n                continue;\n            }\n"|""'
)

run_targets() { # $1 = label
  timeout 1800 cargo test --lib storage::index > "$OUT/$1.index.txt" 2>&1
  echo "index_rc=$?" >> "$OUT/$1.index.txt"
  timeout 1800 cargo test --lib branch::table_catalog::tests::a_lease_pass > "$OUT/$1.catalog.txt" 2>&1
  echo "catalog_rc=$?" >> "$OUT/$1.catalog.txt"
  timeout 1800 cargo test --test d58_latch_free_descent --test d126_atomic_upsert --test integration_btree_concurrency > "$OUT/$1.collateral.txt" 2>&1
  echo "collateral_rc=$?" >> "$OUT/$1.collateral.txt"
}

summarise() { # $1 = label
  for t in index catalog collateral; do
    f="$OUT/$1.$t.txt"
    res=$(grep -E "^test result:" "$f" | tail -1)
    [ -z "$res" ] && res="COMPILE-FAIL or no result line"
    failed=$(grep -E "^test .* FAILED$" "$f" | sed -E 's/^test ([^ ]+) .*/\1/' | tr '\n' ' ')
    echo "$1 $t: $res | failed: ${failed:-none}"
  done
}

echo "== control (unmutated $SUBJECT_SHA)"
run_targets control
summarise control | tee "$OUT/summary.txt"

survivors=0
for m in "${MUTANTS[@]}"; do
  IFS='|' read -r name FILE old new <<< "$m"
  echo "== $name"
  if ! timeout 60 python3 - "$FILE" "$old" "$new" <<'EOF'
import ast, sys
path, old, new = sys.argv[1], ast.literal_eval(sys.argv[2]), ast.literal_eval(sys.argv[3])
src = open(path).read()
n = src.count(old)
if n != 1:
    sys.exit(f"mutant text occurs {n} times, not once; refusing")
open(path, "w").write(src.replace(old, new))
EOF
  then
    echo "$name: NOT APPLIED" | tee -a "$OUT/summary.txt"
    survivors=$((survivors + 1))
    continue
  fi
  git diff --stat -- "$FILE" > "$OUT/$name.diffstat.txt"
  run_targets "$name"
  git checkout "$SUBJECT_SHA" -- "$FILE"
  if ! git diff --quiet "$SUBJECT_SHA" -- src/; then
    echo "ABORT: restore of $FILE after $name left a difference" >&2
    exit 3
  fi
  summarise "$name" | tee -a "$OUT/summary.txt"
  if ! grep -qE "^test .* FAILED$" "$OUT/$name.index.txt" "$OUT/$name.catalog.txt" "$OUT/$name.collateral.txt"; then
    echo "$name: SURVIVED (no test failed)" | tee -a "$OUT/summary.txt"
    survivors=$((survivors + 1))
  fi
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "survivors=$survivors" | tee -a "$OUT/summary.txt"
[ "$survivors" -eq 0 ]
