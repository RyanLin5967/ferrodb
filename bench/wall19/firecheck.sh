#!/usr/bin/env bash
# Wall #19 + D199 fire-check: each mutant reverts ONE piece of the refusal, the reap pruning, or the
# lease-reap attestation, and must turn at least one named test red. Pre-registration: artie-research frontier/lane_wall19_attested.md,
# sections 10.7 and 11 (they supersede 10.6, 10.4, 9.4 and 8.4). Run from the worktree root at DEFAULT QoS (never taskpolicy -b). This is fan work:
# it runs only when FAN-QUEUE row #14 is released.
#
# Blind spots, stated: it runs three selections per mutant (the attest lib module, the whole lease-thread
# test module (its four D199 tests and the rest), and integration_branch_attestation), not the whole suite, so a mutant killed only elsewhere reads as
# a survivor here. It judges kills by cargo's own "test result" line and FAILED names, not by exit
# code alone: a compile error is recorded as COMPILE-FAIL, which is not a kill.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=2673674           # the tree these mutants were written against (1db848e before §11)
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

# name|file|old text (python literal)|new text (python literal)
# '|' is the field separator, so a literal pipe inside mutant text is written \u007c.
A=src/branch/attest.rs
R=src/agent_sql/runtime.rs
MUTANTS=(
  "M1_append_genesis_fallback|$A|"'"            None => return self.refuse(AppendRefused::NoLiveHead { branch }),\n"|"            None => Attestation::genesis(),\n"'
  "M2_fork_genesis_fallback|$A|"'"            None => return self.refuse(AppendRefused::NoLiveParent { parent }),\n"|"            None => Attestation::genesis(),\n"'
  "M3_trunk_reap_allowed|$A|"'"        if op == BranchOp::Reap && branch.is_trunk() {\n            return self.refuse(AppendRefused::TrunkReap);\n        }\n"|""'
  "M4_refusal_not_counted|$A|"'"        self.refused += 1;\n"|""'
  "M5_trunk_exemption_removed|$A|"'"            None if branch.is_trunk() => Attestation::genesis(),\n"|""'
  "M6_reap_keeps_head|$A|"'"        if e.op == BranchOp::Reap {\n            self.heads.remove(&e.branch);\n"|"        if false {\n            self.heads.remove(&e.branch);\n"'
  "M7_scan_forget_does_not_attest|$R|"'"\n            self.attest_landed_reaps(&gone);\n"|"\n"'
  "M8_sweep_forget_does_not_attest|$R|"'"\n                self.attest_landed_reaps(&gone);\n"|"\n"'
  "M9_no_head_check|$R|"'"            let Some(fork_epoch) = h.opened_at(branch) else {\n                continue;\n            };\n"|"            let fork_epoch = h.opened_at(branch).unwrap_or_default();\n"'
  "M10_lease_reap_published|$R|"'"            let _ = Self::append_reap(&mut h, branch, fork_epoch, false);\n"|"            let _ = Self::append_reap(&mut h, branch, fork_epoch, true);\n"'
  "M11_attest_before_landed|$R|"'"                Ok(r) if r.generation > b.generation => landed.push(b),\n"|"                Ok(_) => landed.push(b),\n"'
  "M12_lease_reap_current_epoch|$R|"'"            let _ = Self::append_reap(&mut h, branch, fork_epoch, false);\n"|"            let _ = Self::append_reap(&mut h, branch, self.branches.current_epoch(), false);\n"'
  "M13_opened_follows_latest|$A|"'"            let opened = self.heads.get(&e.branch).map_or(e.epoch, \u007ct\u007c t.opened);\n"|"            let opened = e.epoch;\n"'
  "M14_unreadable_not_counted|$R|"'"        for branch in unreadable {\n            if h.head_of(branch).is_some() {\n                h.count_refusal();\n            }\n"|"        for branch in unreadable {\n            if h.head_of(branch).is_some() {\n            }\n"'
  "M15_unreadable_as_landed|$R|"'"                Err(_) => unreadable.push(b),\n"|"                Err(_) => landed.push(b),\n"'
  "M16_first_entry_not_last|$A|"'"            if e.branch == branch {\n                last = Some(i);\n"|"            if e.branch == branch && last.is_none() {\n                last = Some(i);\n"'
  "M17_seal_attests_behind_the_fallible_step|$R|"'"        let retired = self.retire(branch);\n"|"        let retired = Ok::<(), FerroError>(self.retire(branch)?);\n"'
  "M18_seal_attests_a_flip_it_did_not_make|$R|"'"            .filter(\u007cr\u007c r.generation == branch.generation && r.state != BranchState::Reaped);\n"|"            ;\n"'
  "M19_seal_unreadable_not_counted|$R|"'"            Err(_) => {\n                let mut h = self.attested.lock().unwrap();\n                if h.head_of(branch).is_some() {\n                    h.count_refusal();\n                }\n            }\n"|"            Err(_) => {}\n"'
  "M20_seal_unreadable_guessed_landed|$R|"'"            Err(_) => {\n                let mut h = self.attested.lock().unwrap();\n                if h.head_of(branch).is_some() {\n                    h.count_refusal();\n                }\n            }\n"|"            Err(_) => {\n                let _ = self.attest_reap(branch, fork_epoch, published);\n            }\n"'
)

run_targets() { # $1 = label
  timeout 1800 cargo test --lib branch::attest > "$OUT/$1.lib.txt" 2>&1
  echo "lib_rc=$?" >> "$OUT/$1.lib.txt"
  timeout 1800 cargo test --lib lease_thread::tests:: > "$OUT/$1.lease.txt" 2>&1
  echo "lease_rc=$?" >> "$OUT/$1.lease.txt"
  timeout 1800 cargo test --test integration_branch_attestation > "$OUT/$1.integ.txt" 2>&1
  echo "integ_rc=$?" >> "$OUT/$1.integ.txt"
}

summarise() { # $1 = label
  for t in lib lease integ; do
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
# The control must be clean, or every "kill" below could be the control's own failure (the defect
# D233 review 2 G1 found in the same script shape): any FAILED line, a missing result line, or a
# non-zero rc voids the run.
for t in lib lease integ; do
  f="$OUT/control.$t.txt"
  if grep -qE "^test .* FAILED$" "$f" || ! grep -qE "^test result:" "$f" || ! grep -qE "^${t}_rc=0$" "$f"; then
    echo "control: VOID ($t is not clean)" | tee -a "$OUT/summary.txt"
    exit 2
  fi
done

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
  if ! grep -qE "^test .* FAILED$" "$OUT/$name.lib.txt" "$OUT/$name.lease.txt" "$OUT/$name.integ.txt"; then
    echo "$name: SURVIVED (no test failed)" | tee -a "$OUT/summary.txt"
    survivors=$((survivors + 1))
  fi
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ examples/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "survivors=$survivors" | tee -a "$OUT/summary.txt"
[ "$survivors" -eq 0 ]
