# RUN 3 — THE RUN D40 ACTUALLY LANDED ON. head=c49e605.

The two runs already in this directory are NOT the landing evidence:
  run1  rc=101  head=240a8e5  RED on a known load-sensitive Raft flake
  RUNNER.txt (run 2)  rc=0  head=240a8e5  green, but against a tip EARLIER than the one landed

This one:
  D40-final: mode=per-target rc=0 passed=2048 failed=0 build_errors=0 head=c49e605
  D40-final: go rc=0 passed=97 failed=0
  RUNNER_EXIT=0

Re-derived from the raw log by the landing session rather than read off RUNNER.txt:
  verdicts            105   (104 + this branch's tests/d40_reserved_pages_return_to_baseline.rs)
  FAILED verdicts       0
  passed sum         2048
  ENOSPC                0
  rustc compile errs    0
  go PASS lines        99   go FAIL 0

head=c49e605 is the tip that was landed, and agent-isolation was an ancestor of it, so
`git merge-tree agent-isolation c49e605` produced tree 7b62b4549edd4f4c0a4543b485d3767981a8e7c8
-- byte-identical to c49e605's own tree. The certified tree and the landed tree are the same
git object. That is only true because agent-isolation was frozen while this branch certified;
one more commit on the base would have produced a third tree no suite had seen.

Copied out of /tmp/d40_verify_out3/ because verify-suite.sh defaults VERIFY_OUT to a mktemp
dir, and a finished run whose log lives only in /tmp is one reboot from being unciteable.
That happened earlier the same day with S22 and cost real time.
