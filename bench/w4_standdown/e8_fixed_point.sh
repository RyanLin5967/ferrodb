#!/bin/bash
# E8 — WHERE DOES THE FIXED POINT MOVE WHEN DML STOPS ANNOUNCING?
#
# Runs in the SAME locked window as E7, after it, serial.
#
# ─── PRE-REGISTERED, WRITTEN AND COMMITTED BEFORE THE FIRST RUN ────────────────────────────────
# The arm: agent_dml, extended, 8 clients (4 readers + 2 forkers + 2 DML), 200 rounds to match
# E6's curve. W4_DML_EVERY moves DML out of the announcer set by substituting a SELECT; the fork
# rate is held fixed within each sweep and varied BETWEEN them, because that is the axis E6 showed
# governs everything.
#
#   Sweep A, fork_every=1  -> fork term is 800 exclusive statements, DML term 400.
#   Sweep B, fork_every=64 -> fork term is ~12 exclusive statements, DML term 400 (97% of it).
#
# PREDICTIONS, and the third is the one that would kill the lane:
#   A) fork_every=1: the fixed point BARELY MOVES (< ~10 points). E6 reads ~90% at 800 exclusive
#      statements, and removing DML leaves exactly that. ⇒ forks dominate; W4's payoff is bounded
#      by something W4 does not change.
#   B) fork_every=64: the fixed point COLLAPSES (to roughly E6's ~10% at that announcer count).
#      ⇒ where forks are rare, DML really is the announcer set and W4 recovers most of the win.
#   C) If A collapses too, forks were never the binding term and I have mis-read E6.
#   D) If B does NOT collapse, something outside both terms dominates -- parse_one on every
#      statement is the named candidate -- and W4's payoff is bounded at every fork rate.
#
# VALIDATION, non-negotiable: the counter identity must show the perturbation did what it claims.
#   ann_exec == (stmts - sh_att) + sh_stood  must hold on every row, AND
#   the structural term (stmts - sh_att) must fall by EXACTLY the drop in dml_att, with the fork
#   term (2 x defining) unchanged across a sweep. If the fork term moves, the arms are not matched
#   and the comparison is void.
# ───────────────────────────────────────────────────────────────────────────────────────────────
export PATH="$HOME/.cargo/bin:$PATH"
SCR=/private/tmp/claude-501/-Users-idide-projects-ferrodb/b2b44149-483d-42d1-b512-89bf5de5a135/scratchpad/w4c3
WT=/Users/idide/wt/ferrodb-agent-a3242b062ca90442e
OUT=$WT/bench/w4_standdown/rederive
BIN=./target/release/examples/w4_standdown_count

rm -f "$SCR/E8_DONE"
while [ ! -e "$SCR/E7_DONE" ]; do sleep 15; done
case "$(cat "$SCR/E7_DONE")" in
  complete) ;;
  *) echo "REFUSING: E7 ended as '$(cat "$SCR/E7_DONE")'; not running E8 on an unbuilt binary"
     echo e7-failed > "$SCR/E8_DONE"; exit 3;;
esac
cd "$WT" || exit 1
[ -x "$BIN" ] || { echo "REFUSED: no $BIN"; echo no-binary > "$SCR/E8_DONE"; exit 1; }

for fe in 1 64; do
  for de in 1 2 4 8 32 200; do
    echo "== e8_fe${fe}_dml${de} =="
    W4_CLIENTS=8 W4_ROUNDS=200 W4_ARMS=agent_dml W4_PROTOS=extended \
      W4_FORK_EVERY="$fe" W4_DML_EVERY="$de" \
      timeout 900 "$BIN" > "$OUT/e8_fe${fe}_dml${de}.txt" 2> "$OUT/e8_fe${fe}_dml${de}.err" \
      || { echo "  REFUSED:"; head -3 "$OUT/e8_fe${fe}_dml${de}.err"; }
  done
done
echo complete > "$SCR/E8_DONE"
echo "E8 done $(date -u +%FT%TZ)"
