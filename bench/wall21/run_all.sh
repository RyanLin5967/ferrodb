#!/usr/bin/env bash
# Wall #21 lane §8.22: everything in ONE lockrun acquisition -- green at the branch tip, every registered red at its
# base, then the mutants in a throwaway worktree. The red and mutant scripts are run from COPIES outside the
# worktree (the red script checks out older shas, which do not contain these files).
set -u
WT=/Users/idide/wt/ferrodb-wall21-reaped-subtree.noindex
SRC_TIP=$1
COPY=$(mktemp -d /tmp/wall21-run.XXXXXX)
cp "$WT/bench/wall21/run_red.sh" "$WT/bench/wall21/run_mutants.sh" "$COPY/"
bash "$WT/bench/wall21/run_green.sh"
bash "$COPY/run_red.sh"
bash "$COPY/run_mutants.sh" "$SRC_TIP"
rm -rf "$COPY"
