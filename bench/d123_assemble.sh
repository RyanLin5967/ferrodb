#!/bin/bash
# Assemble bench/d123_serial_attribution.txt from the committed section drafts plus the section
# written last (3c, the novelty verdict). Sections live in bench/d123_sections/ so that the
# artifact can be rebuilt and diffed rather than hand-edited.
set -eu
W=/Users/idide/wt/ferrodb-D123-serial-attribution
# ⛔ REFUSE rather than emit an artifact with an unfilled section. A placeholder that ships reads
# exactly like a finding nobody got round to, and this row has already produced two files that
# looked like results and were not (Amendment 8).
if grep -rq 'PENDING' "$W/bench/d123_sections/"; then
  echo "⛔ REFUSING: a section still says PENDING:"; grep -rln 'PENDING' "$W/bench/d123_sections/"
  exit 1
fi
S=$W/bench/d123_sections
OUT=$W/bench/d123_serial_attribution.txt
cat "$S/d123_head.txt" \
    "$S/d123_body1a.txt" "$S/d123_body1b.txt" "$S/d123_body1d.txt" \
    "$S/d123_body2.txt" "$S/d123_body_f5.txt" \
    "$S/d123_body3.txt" "$S/d123_body3c.txt" "$S/d123_body3b.txt" \
    "$S/d123_body4.txt" "$S/d123_body5.txt" > "$OUT"
echo "wrote $OUT ($(wc -l < "$OUT") lines)"
# Anti-vacuity: the artifact must not claim an arm that produced no result.
for f in "$W"/bench/d123_final/[1-9]*.txt; do
  [ -e "$f" ] || continue
  grep -q '^# harness_exit=0' "$f" || echo "⛔ $f has no '# harness_exit=0' sentinel - TRUNCATED, do not quote"
done
