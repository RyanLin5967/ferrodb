#!/bin/bash
# logcommit.sh <branch> <message> <files...> : commit scratch logs under ops/push-all-20261010/ferrodb/ on <branch>
# (parent = branch tip if it exists, else origin/main) via a temp index; no worktree or real index touched.
set -eu
cd /Users/idide/projects/ferrodb
br="$1"; msg="$2"; shift 2
D=/private/tmp/claude-501/-Users-idide-projects-ferrodb/b2b44149-483d-42d1-b512-89bf5de5a135/scratchpad/push-all-20261010/ferrodb
parent=$(git rev-parse -q --verify "refs/heads/$br" || git rev-parse origin/main)
export GIT_INDEX_FILE=$D/log.idx; rm -f $GIT_INDEX_FILE
git read-tree "$parent"
for f in "$@"; do
  o=$(git hash-object -w "$D/$f")
  git update-index --add --cacheinfo 100644,$o,"ops/push-all-20261010/ferrodb/$f"
done
t=$(git write-tree); unset GIT_INDEX_FILE; rm -f $D/log.idx
c=$(printf '%s\n' "$msg" | git commit-tree "$t" -p "$parent")
old=$(git rev-parse -q --verify "refs/heads/$br" || true)
git update-ref "refs/heads/$br" "$c" "$old"
echo "$br $c"
