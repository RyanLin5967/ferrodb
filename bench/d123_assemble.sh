#!/bin/bash
# Assemble bench/d123_serial_attribution.txt from the committed section drafts plus the section
# written last (3c, the novelty verdict). Sections live in bench/d123_sections/ so that the
# artifact can be rebuilt and diffed rather than hand-edited.
set -eu
W=/Users/idide/wt/ferrodb-D123-serial-attribution
# ⛔ REFUSE rather than emit an artifact with an unfilled section. A placeholder that ships reads
# exactly like a finding nobody got round to, and this row has already produced two files that
# looked like results and were not (Amendment 8).
# ⛔ ANCHORED TO LINE START, and that is not a nicety. The first version grepped for the bare word
# PENDING and refused on the HEADER — which merely DESCRIBES this guard. A detector that matches a
# MENTION rather than an OCCURRENCE is the same defect as `pgrep -f cargo` matching every shell
# whose PATH export contains the string (see bench/d123_final/00_PGREP_OVERREPORT.txt). A
# placeholder must therefore START its line with the marker; prose about placeholders cannot.
if grep -rq '^PENDING' "$W/bench/d123_sections/"; then
  echo "⛔ REFUSING: a section still carries a PENDING marker:"; grep -rln '^PENDING' "$W/bench/d123_sections/"
  exit 1
fi
S=$W/bench/d123_sections
OUT=$W/bench/d123_serial_attribution.txt
cat "$S/d123_head.txt" \
    "$S/d123_body1a.txt" "$S/d123_body1b.txt" "$S/d123_body1e.txt" "$S/d123_body1d.txt" \
    "$S/d123_body2.txt" "$S/d123_body_f5.txt" \
    "$S/d123_body3.txt" "$S/d123_body3c.txt" "$S/d123_body3b.txt" \
    "$S/d123_body4.txt" "$S/d123_body5.txt" > "$OUT"
echo "wrote $OUT ($(wc -l < "$OUT") lines)"
# Anti-vacuity: the artifact must not claim an arm that produced no result.
# Identify an ARM by CONTENT, not by filename: a run is a file that recorded a load stamp at its
# start. Selecting by name flagged lock files and prose notes, which is a detector matching the
# shape of a name rather than the thing itself -- the third instance of that defect in this row.
for f in "$W"/bench/d123_final/*.txt "$W"/bench/d123_rerun/*.txt "$W"/bench/d123_novelty/*.txt "$W"/bench/d123_raw/*.txt "$W"/bench/d123_raw_warm0/*.txt; do
  [ -e "$f" ] || continue
  grep -q '^# load_at_run_start:' "$f" || continue          # not an arm
  grep -q '^# harness_exit=0' "$f" || echo "⛔ $f is an ARM with no '# harness_exit=0' - TRUNCATED, do not quote"
done
