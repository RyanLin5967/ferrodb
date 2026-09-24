#!/usr/bin/env bash
# Wall #19 fire-check: each mutant reverts ONE piece of the refusal (or of the reap pruning) and must
# turn at least one named test red. Pre-registration: artie-research frontier/lane_wall19_attested.md,
# section 8.4. Run from the worktree root at DEFAULT QoS (never taskpolicy -b). This is fan work:
# it runs only when FAN-QUEUE row #14 is released.
#
# Blind spots, stated: it runs two targets per mutant (the attest lib module and
# integration_branch_attestation), not the whole suite, so a mutant killed only elsewhere reads as
# a survivor here. It judges kills by cargo's own "test result" line and FAILED names, not by exit
# code alone: a compile error is recorded as COMPILE-FAIL, which is not a kill.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=97e3111           # the tree these mutants were written against
FILE=src/branch/attest.rs
OUT=bench/wall19/firecheck
mkdir -p "$OUT"

if ! git diff --quiet "$SUBJECT_SHA" -- src/ tests/ examples/; then
  echo "REFUSED: src/, tests/ or examples/ differ from $SUBJECT_SHA; the mutants' text may not apply" >&2
  exit 2
fi
if [ -n "$(git status --porcelain -- src/ tests/ examples/)" ]; then
  echo "REFUSED: uncommitted changes under src/, tests/ or examples/" >&2
  exit 2
fi

# name|old text (python literal)|new text (python literal)
MUTANTS=(
  'M1_append_genesis_fallback|"            None => return self.refuse(AppendRefused::NoLiveHead { branch }),\n"|"            None => Attestation::genesis(),\n"'
  'M2_fork_genesis_fallback|"            None => return self.refuse(AppendRefused::NoLiveParent { parent }),\n"|"            None => Attestation::genesis(),\n"'
  'M3_trunk_reap_allowed|"        if op == BranchOp::Reap && branch.is_trunk() {\n            return self.refuse(AppendRefused::TrunkReap);\n        }\n"|""'
  'M4_refusal_not_counted|"        self.refused += 1;\n"|""'
  'M5_trunk_exemption_removed|"            None if branch.is_trunk() => Attestation::genesis(),\n"|""'
  'M6_reap_keeps_head|"        if e.op == BranchOp::Reap {\n            self.heads.remove(&e.branch);\n        } else {\n            self.heads.insert(e.branch, att);\n        }\n"|"        self.heads.insert(e.branch, att);\n"'
)

run_targets() { # $1 = label
  timeout 1800 cargo test --lib branch::attest > "$OUT/$1.lib.txt" 2>&1
  echo "lib_rc=$?" >> "$OUT/$1.lib.txt"
  timeout 1800 cargo test --test integration_branch_attestation > "$OUT/$1.integ.txt" 2>&1
  echo "integ_rc=$?" >> "$OUT/$1.integ.txt"
}

summarise() { # $1 = label
  for t in lib integ; do
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
  IFS='|' read -r name old new <<< "$m"
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
  if ! grep -qE "^test .* FAILED$" "$OUT/$name.lib.txt" "$OUT/$name.integ.txt"; then
    echo "$name: SURVIVED (no test failed)" | tee -a "$OUT/summary.txt"
    survivors=$((survivors + 1))
  fi
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ examples/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "survivors=$survivors" | tee -a "$OUT/summary.txt"
[ "$survivors" -eq 0 ]
