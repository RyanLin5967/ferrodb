# D40 CERTIFICATION — THREE RUNS. THIS DIRECTORY'S CANONICAL NAMES CARRY THE LANDED ONE.

  RUNNER.txt / suite.log / suite.go.log      head=c49e605  <- WHAT THE LANDING RESTS ON
  prev-240a8e5-*                             head=240a8e5  green, but at a tip EARLIER than the landed one
  run1-RED-*                                 head=240a8e5  RED on a load-sensitive Raft flake

⚠ THIS LAYOUT IS A CORRECTION. As first landed, the canonical names held the SUPERSEDED 240a8e5
run, and the old README asserted "head=240a8e5 equals the merge commit" -- false for the tip that
was landed. A reader opening RUNNER.txt, the name you reach for first, would have concluded the
landing rested on a tip that is not the landed tip. That is exactly the shape refused twice in S22
(ccb9eea recorded head=2450722), sitting inside D40's own evidence directory.

Cause, recorded because it will recur: the landing session added its artifact under a `run3-LANDED-`
prefix ALONGSIDE the existing canonical files rather than replacing them, and the merge brought the
older canonically-named files in from the branch. Two naming schemes in one directory, and the
better-known name won the reader's attention while carrying the worse record. Found by the D40
builder reading what actually landed rather than the commit subject.

⇒ RULE: in an evidence directory, the UNPREFIXED name must always be the run the claim rests on.
Supersede by RENAMING the old one out of the way, never by adding a longer name beside it.

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
