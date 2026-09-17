#!/bin/bash
# PreCompact hook. Fires immediately BEFORE a compaction (auto or manual).
#
# It does NOT ask the session to remember anything — it BANKS the machine-readable half to disk
# itself, so that half survives even if the session never got the chance to write a handoff. The
# principle, taken from ~/projects/dbresearch where it was learned the hard way:
# A REQUEST DELIVERED AS CONTEXT IS BEING DESTROYED IS NOT A GATE. A GATE IS ONE THAT RUNS.
#
# What stays the session's job is JUDGEMENT: what is live, which numbers must not be quoted, which
# threads are open. Everything machine-extractable is extracted, not requested.
#
# Never fails the session: every command is bounded and errors are swallowed.
set -u
R=/Users/idide/projects/ferrodb
A=/Users/idide/wt/artie-research
OUT=$R/COMPACTION_STATE_AUTO.md

STDIN_JSON=$(timeout 5 cat 2>/dev/null || true)
TRANSCRIPT=$(printf '%s' "$STDIN_JSON" | timeout 5 python3 -c 'import sys,json
try: print((json.load(sys.stdin) or {}).get("transcript_path","") or "")
except Exception: print("")' 2>/dev/null)
timeout 120 python3 "$R/.claude/hooks/precompact_prose.py" "$TRANSCRIPT" 2>&1 | tail -2
PROSE_RC=${PIPESTATUS[0]:-9}

{
  echo "# AUTO-BANKED STATE, written by the PreCompact hook. NOT hand-written."
  echo "# Generated: $(date -u '+%Y-%m-%dT%H:%M:%SZ') UTC / $(date '+%Y-%m-%d %H:%M %Z')"
  echo "# The extracted prose half is COMPACTION_HANDOFF_<date>_AUTO.md — read that FIRST."
  echo
  echo "## ferrodb HEAD  (branch, and whether anything is uncommitted == unbanked == at risk)"
  timeout 20 git -C "$R" log --oneline -8 2>/dev/null | sed 's/^/  /'
  echo "  branch: $(timeout 10 git -C "$R" rev-parse --abbrev-ref HEAD 2>/dev/null)"
  echo "  ahead of origin/main: $(timeout 20 git -C "$R" rev-list --count origin/main..HEAD 2>/dev/null)"
  echo
  echo "## ferrodb UNCOMMITTED at compaction time"
  timeout 20 git -C "$R" status --porcelain 2>/dev/null | head -40 | sed 's/^/  /'
  echo
  echo "## artie-research HEAD  (the LEDGERS live here — it is a SEPARATE git repo)"
  timeout 20 git -C "$A" log --oneline -6 2>/dev/null | sed 's/^/  /'
  echo "  uncommitted:"
  timeout 20 git -C "$A" status --porcelain 2>/dev/null | grep -v '^??' | head -20 | sed 's/^/  /'
  echo
  echo "## ★ THE SCALE LEDGER — the loop's ONLY memory. OPEN rows are the work."
  echo "## A cron-launched pass reads this and nothing else, so an OPEN row that is really done,"
  echo "## or a DONE row with no numbers in Evidence, misdirects every future pass."
  timeout 20 grep -E '^\| (S[0-9]+|Row) ' "$A/SCALE-LEDGER.md" 2>/dev/null | cut -c1-240 | sed 's/^/  /'
  echo
  echo "## ★ THE LOOP — a cron job is HELD IN MEMORY and dies with this session."
  echo "## CronList is the authority, never a tick in front of you. If CronList is empty, every"
  echo "## tick you can see is withdrawn text and the correct action is to do NOTHING."
  timeout 10 sed -n '/^| revision/,/^$/p' "$A/SCALE-LOOP.md" 2>/dev/null | sed 's/^/  /'
  echo "  skill revision: $(grep -m1 '^# REVISION' "$HOME/.claude/skills/ferrodb-scale/SKILL.md" 2>/dev/null)"
  echo
  echo "## ★ DESIGN DECISIONS — SCALE-DESIGN.md. A decision only in a commit message has no half-life."
  echo "## Each carries FALSIFIERS. A pass that builds one without checking them has not built it."
  timeout 20 grep -E '^## D[0-9]+|^### The decision|^### What would falsify|^\*\*Falsifier' "$A/SCALE-DESIGN.md" 2>/dev/null | head -16 | sed 's/^/  /'
  echo
  echo "## ★ WHAT IS STILL LINEAR — the bar here is O(log N), not 'faster'."
  echo "## An O(N) walk that runs REPEATEDLY (per sweep, per fork, per statement) is a bug at 10^6"
  echo "## even when it is fast today. reap_expired cloned every record every 30s and no measurement"
  echo "## of fork latency would ever have shown it."
  timeout 20 grep -rn 'all_records\|live_branches' "$R/src/branch/" 2>/dev/null | grep -v test | head -8 | sed 's/^/  /'
  echo
  echo "## ★ MEASUREMENTS — the committed artifacts. A number from anywhere else is recalled, not measured."
  for f in "$R"/bench/*.txt; do
    [ -f "$f" ] || continue
    echo "  --- $(basename "$f") ---"
    timeout 10 grep -E '^ *[0-9]+ \||x[0-9.]+|O\(N' "$f" 2>/dev/null | head -8 | sed 's/^/    /'
  done
  echo
  echo "## ★ LITTLE-JUMP CHECK — last 8 commits. Constant, or shape? Two constants means stop and"
  echo "## fix the structure underneath them instead of taking the next row."
  timeout 20 git -C "$R" log --oneline -8 2>/dev/null | sed 's/^/  /'
  echo
  echo "## RUNNING RIGHT NOW  (a suite or agent invisible to the next session is lost work)"
  echo "  suites/builds:"
  timeout 10 pgrep -fl 'cargo test|cargo build|verify-suite|go test' 2>/dev/null | grep -v pgrep | head -6 | cut -c1-110 | sed 's/^/    /'
  echo "  agent worktrees with a live claude:"
  for p in $(timeout 10 pgrep -f 'bin/claude' 2>/dev/null | head -20); do
    c=$(timeout 5 lsof -a -p "$p" -d cwd -Fn 2>/dev/null | grep '^n' | cut -c2-)
    case "$c" in /Users/idide/wt/ferrodb-*) echo "    $(basename "$c")";; esac
  done
  echo "  launchd jobs:"
  timeout 10 launchctl list 2>/dev/null | grep -iE 'ferrodb|parkwatch|rotate' | sed 's/^/    /'
  echo
  echo "## ACCOUNT POOL at compaction time (a park is not a failure; an exhausted pool is)"
  timeout 30 cswap list 2>/dev/null | grep -E '^ *[0-9]+:|5h:|7d:' | head -24 | sed 's/^/  /'
} > "$OUT" 2>/dev/null

if [ "${PROSE_RC:-9}" != "0" ]; then
  echo "⛔ PreCompact: the PROSE extractor REFUSED (rc=${PROSE_RC:-9}). No dated handoff was written."
  echo "⛔ Write the judgement half BY HAND THIS TURN — the automatic half is gone."
fi
echo "PreCompact: banked machine state to COMPACTION_STATE_AUTO.md; the extractable prose half (Ryan verbatim, last tool calls, agents) is in COMPACTION_HANDOFF_<date>_AUTO.md. STILL YOURS, because none of it is extractable: (1) WHICH MEASUREMENTS ARE TRUSTED — a number taken while several suites or agents were running cannot be told from a regression, and this session has taken some under contention; (2) WHICH CLAIMS ARE ALREADY DEAD — do not resurrect 'no database records what was read' (provenance is a 20-year field), 'nobody gives you the reviewer' (lakeFS pre-merge hooks), 'git for data is novel' (Dolt), or 'generational branch arenas are an insight' (it is batching plus a sizing fix); (3) WHETHER THE LAST FEW COMMITS CHANGED CONSTANTS OR SHAPES — if constants, the next pass must fix the structure, not take the next row; (4) any design decided in conversation but not yet in SCALE-DESIGN.md. A handoff that carries only method lets the next session run the machinery perfectly while repeating a claim that was already killed."
exit 0
