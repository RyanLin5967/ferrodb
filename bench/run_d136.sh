#!/bin/bash
# D136 — run the provenance-dictionary occupancy curve and stamp its own provenance into the
# artifact. Counts only; the two wall-clock stamps below are START/END markers, not measurements,
# and are labelled as such in the output.
#
#   bench/run_d136.sh <out-file>
set -u
export PATH="$HOME/.cargo/bin:$PATH"
OUT="${1:?usage: run_d136.sh <out-file>}"
WT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$WT" || exit 2

DONE="$(dirname "$OUT")/.$(basename "${OUT%.txt}")_done"
rm -f "$DONE"

{
  echo "### provenance of this run"
  echo "worktree: $WT"
  echo "branch:   $(git rev-parse --abbrev-ref HEAD)"
  echo "head:     $(git log -1 --format='%h %s')"
  echo "dirty:    $(git status --porcelain | wc -l | tr -d ' ') file(s)"
  echo "started:  $(date -u +%FT%TZ)   [a START MARKER, not a measurement]"
  echo "host load: $(uptime | sed 's/.*averages*: *//')"
  echo "suite lock: $( [ -d /tmp/ferrodb-suite.lock ] && cat /tmp/ferrodb-suite.lock/owner 2>/dev/null || echo 'free' )"
  echo "measure lock: $(/Users/idide/wt/logs/measure-lock.sh status 2>/dev/null)"
  echo
  echo "### binary identity — asked of the artifact, not of a pre-run echo"
  ls -l target/release/examples/d136_prov_dict_curve 2>&1 | sed 's/^/  /'
  echo
} > "$OUT"

timeout 3000 ./target/release/examples/d136_prov_dict_curve >> "$OUT" 2>&1
RC=$?

{
  echo
  echo "finished: $(date -u +%FT%TZ)   [an END MARKER, not a measurement]"
  echo "host load at end: $(uptime | sed 's/.*averages*: *//')"
  echo "RUN_DONE rc=$RC"
} >> "$OUT"

echo "RUN_DONE rc=$RC" > "$DONE"
echo "rc=$RC out=$OUT"
exit $RC
