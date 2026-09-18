#!/bin/bash
# Interleaved A/B for D28. Alternates arms round by round so load, which drifts over tens of
# minutes, hits both arms in the same proportion; all-A-then-all-B confounds the arm with the hour.
#
# $1 = N, $2 = rounds, $3 = output file
N="$1"; ROUNDS="$2"; OUT="$3"
A=/tmp/d28ab3/bin/armA_withD28
B=/tmp/d28ab3/bin/armB_noD28

# The data row is keyed on "first field is a bare integer", NOT on N: the harness reports
# per*threads (99968 for N=100000), so matching N drops every row.
row() { awk '$1 ~ /^[0-9]+$/ && NF >= 12 { last = $0 } END { if (last != "") print last }'; }

echo "# interleaved A/B, N=$N, $ROUNDS rounds per arm" >> "$OUT"
echo "# round arm  br_1row_ms  br_all_ms  runs_ms  live_cnt_ms  loadavg" >> "$OUT"
for r in $(seq 1 "$ROUNDS"); do
  for arm in A B; do
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
