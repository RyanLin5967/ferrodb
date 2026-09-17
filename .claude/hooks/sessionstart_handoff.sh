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
timeout 120 python3 "$R/.claude/hooks/sessionstart_handoff.py" 2>&1
rc=$?
if [ $rc -ne 0 ]; then
  # Fail LOUD and still deliver the pointer. A hook that dies quietly is indistinguishable from
  # one with nothing to say, which is the recorded failure mode of every instrument here.
  echo "PreCompact handoff assembler FAILED (rc=$rc). THE HANDOFF WAS NOT DELIVERED."
  echo "  Read these by hand THIS TURN, before acting:"
  ls -1t "$R"/COMPACTION_HANDOFF_*_AUTO.md 2>/dev/null | head -1 | sed 's/^/    /'
  echo "    $R/COMPACTION_STATE_AUTO.md"
  echo "    /Users/idide/wt/artie-research/SCALE-LEDGER.md"
fi
exit 0
