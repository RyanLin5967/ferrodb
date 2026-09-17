#!/bin/bash
# PreToolUse gate, matcher: *. REFUSES every tool call by the MAIN session until the full
# SessionStart handoff has actually been read.
#
# WHY. 2026-09-17, Ryan: "it should stop you from doing literally everything else, until you
# have read the full handoff." The SessionStart digest is budgeted to ~11 KB and the untrimmed
# payload is on disk. A POINTER TO A FILE IS NOT A READ -- measured in ~/projects/dbresearch,
# where minutes after that hook fired the session read the digest, saw the pointer, and
# dispatched four agents without opening the file. No budget fixes that; the enforcement has to
# move from WORDING to OUTCOME, per the standing rule: "Guard outcomes, not wording."
#
# HOW IT READS AN OUTCOME. sessionstart_handoff.py mints five fresh tokens per write and spreads
# them through .sessionstart_full.txt, putting NONE of them in the budgeted stdout. Only output
# that actually traversed the file puts all five in the transcript. Five, not one, so a `tail`
# cannot mint proof; hashes in the sentinel, not the tokens themselves, so `cat`ting the sentinel
# cannot either. Fresh tokens per write mean every compaction demands a fresh read.
#
# EVERY "ALLOW" BRANCH BELOW WAS PAID FOR SOMEWHERE ELSE. A RUNG MUST BE ABLE TO IDENTIFY ITS
# SUBJECT: the sibling of this hook in dbresearch wedged six subagents and a live agent named
# `residue-tee-hbm` lost every Bash and Read call mid-run, because its DEFAULT BRANCH denied.
# A guard's default branch is where its real policy lives. This one's default is ALLOW, and it
# says so on stderr every time it cannot identify its subject.
#
# BLIND SPOTS, stated rather than assumed away:
#   - It proves the tokens REACHED the transcript, not that the text was understood. That is the
#     most an outcome test can do, and it is still strictly more than a pointer.
#   - The deadlock-breaker is a WORDING test. Deliberate, and safe in the only direction open to
#     it: it can only allow a call that names the file, and such a call either reads it -- which
#     satisfies the real gate -- or does nothing and the next call is refused again.
#   - It scans only the transcript tail; a genuine read is recent by construction.
#   - UNTIL 2026-09-17 IT COULD NOT SAY WHY IT ALLOWED ANYTHING. Ryan asked whether the hooks had
#     stopped the session before it read the handoff; they had not, and the gate's own six rungs
#     could not answer which one had fired, because an allow is a silent `exit 0`. A guard that
#     cannot be audited is a guard you have to take on faith, which is the thing it exists to
#     replace. Every rung now names itself in .handoff_gate.log before returning.
set -uo pipefail

R="${CLAUDE_PROJECT_DIR:-/Users/idide/projects/ferrodb}"
GATELOG="$R/.claude/hooks/.handoff_gate.log"
# ⛔ 2026-09-17: this gate's payload parser WAS a bare `python3`, which on this machine resolves to
# an Xcode shim that refuses to run. It produced no stdout, TRANSCRIPT came back empty, and the
# rung below that must-not-block on an unidentifiable caller allowed EVERY call for a whole
# session. The gate never reported a fault because falling open IS its documented behaviour there.
# toolpath.sh probes for an interpreter that actually executes. See its header.
. "$R/.claude/hooks/toolpath.sh" 2>/dev/null || true
if [ -z "${PY:-}" ]; then
  echo "handoff gate: NO WORKING PYTHON3 -- this gate cannot read its own input and is allowing" >&2
  echo "handoff gate: every call. That is a DEAD GATE, not a clean one. Fix toolpath.sh." >&2
  rung 0-no-interpreter ALLOW
fi
rung() {  # rung <name> <verdict>; append then return the verdict's code
  printf '%s rung=%-22s %s\n' "$(date -u +%FT%TZ)" "$1" "$2" >> "$GATELOG" 2>/dev/null
  [ "$2" = ALLOW ] && exit 0 || return 0
}
SENTINEL_FILE="$R/.claude/hooks/.sessionstart_sentinel"
FULL="$R/.claude/hooks/.sessionstart_full.txt"
PAYLOAD=$(cat)

# 0. Nothing has ever been banked -> nothing to enforce.
[ -s "$SENTINEL_FILE" ] || { echo "handoff gate: no sentinel on disk; nothing to enforce." >&2; rung 0-no-sentinel ALLOW; }

TRANSCRIPT=$(printf '%s' "$PAYLOAD" | ${TIMEOUT:+$TIMEOUT 15} "$PY" -c 'import json,sys
try: print(json.load(sys.stdin).get("transcript_path","") or "")
except Exception: print("__UNPARSEABLE__")' 2>/dev/null)

# 1. Cannot parse -> cannot tell a subagent from the main thread -> MUST NOT BLOCK.
[ "$TRANSCRIPT" = "__UNPARSEABLE__" ] && { echo "handoff gate: unparseable payload; not blocking." >&2; rung 1-unparseable ALLOW; }
# 2. Subagent -> never blocked. This is the branch that keeps a fan-out from being wedged.
case "$TRANSCRIPT" in */subagents/*) rung 2-subagent ALLOW ;; esac
# 3. Already proven for this session + this sentinel -> cached allow.
MARK="$R/.claude/hooks/.handoff_read.$(basename "${TRANSCRIPT:-none}" .jsonl).$(head -1 "$SENTINEL_FILE" | tail -c 9)"
[ -f "$MARK" ] && rung 3-cached-allow ALLOW
# 4. Deadlock-breaker: allow the call that READS the file.
# ⛔ NARROWED 2026-09-17. It used to be `case $PAYLOAD in *sessionstart_full.txt*`, a substring test
# over the WHOLE payload -- so ANY call that merely MENTIONED the filename cleared the gate, including
# five diagnostic commands that only `stat`ed and `ls`ed it. That is the wording-guard failure the
# standing rule names ("guard outcomes, not wording"), and this rung was the one place wording was
# deliberately accepted. It is still a wording test -- it has to be, or reading the file is impossible
# -- but it now requires a command that can actually EMIT the file, so the allow it grants is one that
# puts the tokens in the transcript rather than one that talks about them.
case "$PAYLOAD" in
  *'"file_path"'*sessionstart_full.txt*)                 rung 4-read-tool ALLOW ;;
  *cat*sessionstart_full.txt*|*less*sessionstart_full.txt*|\
  *sed*sessionstart_full.txt*|*awk*sessionstart_full.txt*) rung 4-cat-command ALLOW ;;
esac
# 5. No identifiable transcript -> MUST NOT BLOCK (see header: residue-tee-hbm).
if [ -z "$TRANSCRIPT" ] || [ ! -f "$TRANSCRIPT" ]; then
  echo "handoff gate: no readable transcript for this caller; not blocking." >&2
  rung 5-no-transcript ALLOW
fi

# 6. THE OUTCOME TEST.
if tail -c 8000000 "$TRANSCRIPT" 2>/dev/null | ${TIMEOUT:+$TIMEOUT 60} "$PY" -c '
import sys, re, hashlib
want = {l.strip() for l in open(sys.argv[1]) if l.strip()}
seen = {hashlib.sha256(m.group(0).encode()).hexdigest()
        for m in re.finditer(r"HANDOFF-READ-PROOF-[0-9a-f]{32}", sys.stdin.read())}
sys.exit(0 if want and want <= seen else 1)' "$SENTINEL_FILE"; then
  : > "$MARK"; rung 6-tokens-proven ALLOW
fi

cat >&2 <<MSG
HANDOFF-READ GATE: REFUSED. The full post-compaction handoff has NOT been read this session.

  The SessionStart output was a BUDGETED DIGEST, not the handoff. The untrimmed payload is:

      cat $FULL

  It carries five proof tokens spread through it. Reading the head, or the digest, or asserting
  that you read it, does NOT clear this gate -- all five must appear in the transcript.
  Read it in full, then re-issue this tool call.
MSG
rung 6-REFUSED DENY
exit 2
