#!/bin/bash
# snap.sh <worktree> <refname>
# Quarantine a worktree's uncommitted TRACKED edits as a commit, without touching
# the worktree files or its real index. Tries `git stash create` first; if that
# refuses (unmerged entries during a merge), builds a tree from HEAD plus the
# working-tree contents of every path git status reports, via a temporary index.
# Parents: HEAD (+ MERGE_HEAD if a merge is in progress).
set -u
w="$1"; ref="$2"
if git -C "$w" show-ref --verify --quiet "refs/heads/$ref" 2>/dev/null || git -C /Users/idide/projects/ferrodb show-ref --verify --quiet "refs/heads/$ref"; then
  echo "EXISTS $ref $(git -C /Users/idide/projects/ferrodb rev-parse refs/heads/$ref)"; exit 0
fi
c=$(timeout 120 git -C "$w" stash create "wip quarantine $(basename "$w") 20261010" 2>/dev/null)
src=$?
how=stash
if [ $src -ne 0 ] || ! git -C "$w" cat-file -e "${c}^{commit}" 2>/dev/null; then
  c=""
  how=tmpindex
  gd=$(git -C "$w" rev-parse --absolute-git-dir)
  tmp=$(mktemp /private/tmp/claude-501/-Users-idide-projects-ferrodb/b2b44149-483d-42d1-b512-89bf5de5a135/scratchpad/push-all-20261010/ferrodb/idx.XXXXXX)
  export GIT_INDEX_FILE="$tmp"
  timeout 60 git -C "$w" read-tree HEAD || { echo "FAIL read-tree $w"; exit 1; }
  unset GIT_INDEX_FILE
  # paths from the REAL index vs HEAD and worktree (read-only status)
  list=$(mktemp "$tmp.list.XXXX")
  timeout 120 git -C "$w" diff --name-only -z HEAD > "$list" || { echo "FAIL diff $w"; exit 1; }
  timeout 120 git -C "$w" diff --cached --name-only -z HEAD >> "$list"
  export GIT_INDEX_FILE="$tmp"
  tr '\0' '\n' < "$list" | sort -u | while IFS= read -r p; do
    [ -z "$p" ] && continue
    if [ -e "$w/$p" ] || [ -L "$w/$p" ]; then
      git -C "$w" update-index --add -- "$p" || echo "WARN add $p"
    else
      git -C "$w" update-index --force-remove -- "$p" || echo "WARN rm $p"
    fi
  done
  t=$(git -C "$w" write-tree) || { echo "FAIL write-tree $w"; exit 1; }
  unset GIT_INDEX_FILE
  parents="-p $(git -C "$w" rev-parse HEAD)"
  [ -f "$gd/MERGE_HEAD" ] && for m in $(cat "$gd/MERGE_HEAD"); do parents="$parents -p $m"; done
  c=$(echo "wip quarantine (UNREVIEWED, mid-merge snapshot incl. conflict markers) of $w on $(git -C "$w" rev-parse --abbrev-ref HEAD), 20261010" | git -C "$w" commit-tree "$t" $parents) || { echo "FAIL commit-tree"; exit 1; }
  rm -f "$tmp" "$list"
fi
git -C /Users/idide/projects/ferrodb update-ref "refs/heads/$ref" "$c" "" || { echo "FAIL update-ref $ref"; exit 1; }
echo "OK $ref $c $how files=$(git -C /Users/idide/projects/ferrodb diff --name-only $c^1 $c | wc -l | tr -d ' ')"
