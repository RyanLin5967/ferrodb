#!/bin/bash
# SessionStart hook, matcher: compact | resume | startup.
#
# The DELIVERY half. ferrodb had a PreCompact hook that banked a correct handoff and no hook at
# all that delivered it, so on 2026-09-17 a compaction landed, a fresh context started, and five
# tool calls went by before Ryan asked whether the hook had worked. BANKING IS NOT DELIVERY.
#
# All assembly lives in sessionstart_handoff.py, which budgets its own stdout and names whatever
# it had to drop. Do NOT add `cat` calls here -- add a section to that script with a priority, or
# it will silently push something else out. That is exactly how dbresearch lost 64 of 66 KB while
# printing the words "injected in full below".
set -u
R=/Users/idide/projects/ferrodb
. "$R/.claude/hooks/toolpath.sh" 2>/dev/null || true
${TIMEOUT:+$TIMEOUT 120} "${PY:-python3}" "$R/.claude/hooks/sessionstart_handoff.py" 2>&1
rc=$?
if [ $rc -ne 0 ]; then
  # Fail LOUD and still deliver the pointer. A hook that dies quietly is indistinguishable from
  # one with nothing to say, which is the recorded failure mode of every instrument here.
  # ⛔ POISON THE STALE PAYLOAD. Added 2026-09-17 after the failure this comment describes.
  # The assembler exited 69 (Xcode-shim python3, see toolpath.sh) and therefore did not rewrite
  # .sessionstart_full.txt. The PREVIOUS compaction's file stayed on disk -- four hours old, with
  # its sentinel still matching, so the PreToolUse gate could be satisfied in full by reading state
  # that had been false for hours. A stale handoff that passes its own freshness check is strictly
  # worse than no handoff: it is wrong state wearing a verified badge, which is the exact failure
  # the global rules name ("dated advice at the head of any long-lived text ages against itself").
  # So on failure the payload is REPLACED by a refusal that carries its own fresh tokens: the gate
  # still works, and what it forces you to read is the truth, which is that there is no handoff.
  F="$R/.claude/hooks/.sessionstart_full.txt"; S="$R/.claude/hooks/.sessionstart_sentinel"
  if [ -f "$F" ]; then
    AGE=$(( $(date +%s) - $(stat -f %m "$F" 2>/dev/null || echo 0) ))
    mv -f "$F" "$F.stale" 2>/dev/null
    : > "$S"
    {
      echo "ferrodb HANDOFF -- ⛔ NOT ASSEMBLED. THIS FILE IS A REFUSAL, NOT A HANDOFF."
      echo "The SessionStart assembler failed (rc=$rc). The payload that was on disk was"
      echo "$((AGE/60)) MINUTES OLD and has been moved to .sessionstart_full.txt.stale so that it"
      echo "cannot be mistaken for current state. Do not read it for anything except history."
      echo
      echo "READ THESE BY HAND, THIS TURN, BEFORE ANY OTHER WORK:"
      ls -1t "$R"/COMPACTION_HANDOFF_*_AUTO.md 2>/dev/null | head -1 | sed "s/^/    /"
      echo "    $R/COMPACTION_STATE_AUTO.md"
      echo "    /Users/idide/wt/artie-research/SCALE-LEDGER.md"
      echo "    /Users/idide/wt/artie-research/SCALE-NEXT"
      echo
      echo "Then diagnose the assembler: bash $R/.claude/hooks/sessionstart_handoff.sh"
      for i in 1 2 3 4 5; do
        T="HANDOFF-READ-PROOF-$(head -c 16 /dev/urandom | od -An -tx1 | tr -d " \n")"
        echo "    [checkpoint $i/5] $T"
        printf "%s" "$T" | shasum -a 256 | cut -d" " -f1 >> "$S"
        echo
      done
    } > "$F"
    echo "PreCompact: stale payload quarantined to .sessionstart_full.txt.stale (was ${AGE}s old);"
    echo "  .sessionstart_full.txt now carries a REFUSAL with fresh proof tokens."
  fi
  echo "PreCompact handoff assembler FAILED (rc=$rc). THE HANDOFF WAS NOT DELIVERED."
  echo "  Read these by hand THIS TURN, before acting:"
  ls -1t "$R"/COMPACTION_HANDOFF_*_AUTO.md 2>/dev/null | head -1 | sed 's/^/    /'
  echo "    $R/COMPACTION_STATE_AUTO.md"
  echo "    /Users/idide/wt/artie-research/SCALE-LEDGER.md"
fi
exit 0
