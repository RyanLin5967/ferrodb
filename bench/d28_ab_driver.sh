#!/bin/bash
# LATIN-SQUARE A/B for D28. Arms alternate round by round AND the within-round order rotates,
# so neither the hour nor the position inside a round is confounded with the arm.
#
# ⛔ CORRECTED 2026-09-18. This header used to say only "Alternates arms round by round so load,
# which drifts over tens of minutes, hits both arms in the same proportion" -- and the loop below
# read `for arm in A B`, a FIXED order, every round. That claim was half true in a way that reads
# as fully true:
#
#   * what interleaving DOES fix: slow drift across the whole run. A all-then-B all confounds the
#     arm with the hour, and round-by-round alternation genuinely removes that.
#   * ⛔ what it does NOT fix, and what the header implied it did: anything that varies WITHIN a
#     round. With A always measured first, cache and page-cache warmth left by A's teardown, the
#     thermal step from A's own work, and any background task that happens to start mid-round all
#     land on B systematically. Averaging over rounds does not cancel a bias that has the same sign
#     in every round -- that is the definition of a systematic error, and the only thing that
#     removes it is rotating the order.
#
# This project has already paid for that lesson twice: D44's corrected run is a 32-block LATIN
# SQUARE for exactly this reason, and the finding is written down as "interleaving does not cancel
# drift -- this box DRIFTS". This driver predates that and never got the fix.
#
# ⚠ EVERY D28 A/B ARTIFACT TAKEN WITH THE OLD DRIVER WAS MEASURED A-BEFORE-B IN ALL ROUNDS. That
# does not make those numbers wrong; it means their error bars are not what an interleaved design
# would give you, and any D28 A/B difference smaller than the within-round position effect is not
# established by them. Re-run before quoting a narrow margin.
#
# $1 = N, $2 = rounds, $3 = output file
N="$1"; ROUNDS="$2"; OUT="$3"
A=/tmp/d28ab3/bin/armA_withD28
B=/tmp/d28ab3/bin/armB_noD28

# The data row is keyed on "first field is a bare integer", NOT on N: the harness reports
# per*threads (99968 for N=100000), so matching N drops every row.
row() { awk '$1 ~ /^[0-9]+$/ && NF >= 12 { last = $0 } END { if (last != "") print last }'; }

echo "# LATIN-SQUARE A/B, N=$N, $ROUNDS rounds per arm; within-round order rotates by round parity" >> "$OUT"
echo "# round arm  br_1row_ms  br_all_ms  runs_ms  live_cnt_ms  loadavg" >> "$OUT"
for r in $(seq 1 "$ROUNDS"); do
  # The Latin square, for two arms: odd rounds run A then B, even rounds run B then A. Over an
  # even number of rounds each arm occupies each within-round position exactly half the time, so a
  # position effect cancels instead of accumulating. The printed `arm` field is unchanged, so every
  # existing parser of this file keeps working -- what changed is the ORDER the rows were produced
  # in, and that order is now recorded per round in the output (see the ORDER line below).
  if [ $(( r % 2 )) -eq 1 ]; then ORDER="A B"; else ORDER="B A"; fi
  echo "# round $r order: $ORDER" >> "$OUT"
  for arm in $ORDER; do
    bin=$A; [ "$arm" = B ] && bin=$B
    load=$(uptime | sed 's/.*averages*: *//' | awk '{print $1}')
    err=/tmp/d28ab3/round_${r}${arm}.err
    # stderr is KEPT, not discarded. An earlier version sent it to /dev/null, so a round killed by
    # a full disk was indistinguishable from a round that merely produced no row -- and an ENOSPC
    # round reads as a slow round rather than a failed one unless something looks.
    line=$(timeout 5400 "$bin" query "$N" 64 2>"$err" | row)
    # OUTCOME, not wording. The text grep below cannot be trusted on its own: a full disk is
    # exactly the condition under which the error text cannot be WRITTEN, and round 5 of the
    # 10^5 run proved it -- 5A left an empty .err and 5B left none, so the grep matched nothing
    # and a disk death was labelled NO-ROW-EMITTED. Free space at round end is the one signal a
    # full disk cannot suppress. 1 GiB = 1048576 KiB.
    freekb=$(df -k /tmp | awk 'NR==2 {print $4}')
    if [ -z "$line" ] && [ "${freekb:-0}" -lt 1048576 ]; then
      echo "$r $arm VOID-DISK -- ${freekb}KiB free at round end; NOT a datapoint and NOT a red result" >> "$OUT"
      continue
    fi
    if grep -qE "No space left|os error 28" "$err" 2>/dev/null; then
      echo "$r $arm VOID-ENOSPC -- disk filled mid-run; NOT a datapoint and NOT a red result" >> "$OUT"
      continue
    fi
    if [ -z "$line" ]; then
      echo "$r $arm NO-ROW-EMITTED -- run failed or was killed; NOT a datapoint (see $err)" >> "$OUT"
      continue
    fi
    echo "$line" | awk -v r="$r" -v a="$arm" -v l="$load" \
      '{printf "%d %s %12s %12s %12s %12s   %s\n", r, a, $6, $7, $8, $11, l}' >> "$OUT"
    sync
    rm -f "$err"
  done
done
echo "# done" >> "$OUT"
