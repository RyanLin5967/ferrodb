#!/bin/bash
# Fire-check for pretooluse_handoff_read.sh. A guard that has never been made to REFUSE is not a
# guard, and one that has never been made to ALLOW is a wedge. Both directions, every time.
set -u
H=/Users/idide/projects/ferrodb/.claude/hooks
G=$H/pretooluse_handoff_read.sh
S=$H/.sessionstart_sentinel
T=$(mktemp -d); overall=0
TOKENS=$(grep -o 'HANDOFF-READ-PROOF-[0-9a-f]\{32\}' "$H/.sessionstart_full.txt")
N=$(printf '%s\n' "$TOKENS" | grep -c .)

check() { # name expected_rc payload
  local name=$1 want=$2 payload=$3 rc
  printf '%s' "$payload" | "$G" >/dev/null 2>&1; rc=$?
  if [ "$rc" = "$want" ]; then echo "  PASS  $name (rc=$rc)"
  else echo "  FAIL  $name: wanted rc=$want got rc=$rc"; overall=1; fi
}

mk() { local f=$T/$1.jsonl; shift; printf '%s\n' "$@" > "$f"; echo "$f"; }

echo "sentinel holds $(grep -c . "$S") hashes; full payload holds $N tokens"
[ "$N" = 5 ] || { echo "  FAIL  expected 5 tokens in the full payload, found $N"; overall=1; }

A=$(mk empty "nothing here")
check "main session, NO tokens read            -> REFUSE" 2 "{\"transcript_path\":\"$A\"}"
B=$(mk onetoken "$(printf '%s\n' "$TOKENS" | head -1)")
check "main session, ONE token (tail-mint)     -> REFUSE" 2 "{\"transcript_path\":\"$B\"}"
C=$(mk alltokens "$TOKENS")
check "main session, ALL FIVE tokens read      -> ALLOW " 0 "{\"transcript_path\":\"$C\"}"
mkdir -p "$T/sess/subagents"; echo x > "$T/sess/subagents/agent-1.jsonl"
check "SUBAGENT transcript                     -> ALLOW " 0 "{\"transcript_path\":\"$T/sess/subagents/agent-1.jsonl\"}"
check "unparseable payload                     -> ALLOW " 0 "not json at all"
check "transcript_path absent                  -> ALLOW " 0 '{"tool_name":"Bash"}'
check "transcript_path names a missing file    -> ALLOW " 0 '{"transcript_path":"/nope/x.jsonl"}'
D=$(mk deadlock "nothing")
check "deadlock-breaker (payload names file)   -> ALLOW " 0 "{\"transcript_path\":\"$D\",\"tool_input\":{\"file_path\":\"$H/.sessionstart_full.txt\"}}"

# The no-sentinel case must allow, or a fresh clone wedges on its first tool call.
mv "$S" "$S.bak"; E=$(mk nosent "nothing")
check "no sentinel on disk                     -> ALLOW " 0 "{\"transcript_path\":\"$E\"}"
mv "$S.bak" "$S"
rm -rf "$T" "$H"/.handoff_read.empty.* "$H"/.handoff_read.onetoken.* "$H"/.handoff_read.alltokens.* "$H"/.handoff_read.deadlock.* "$H"/.handoff_read.nosent.* 2>/dev/null
[ $overall = 0 ] && echo "ALL GATE CHECKS PASS" || echo "GATE FIRE-CHECK FAILED"
exit $overall
