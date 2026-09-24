#!/bin/bash
# D231 — does the build stamp compiled into a ferrodb binary follow the tree it was built from?
#
# FAN WORK: this builds ferrodb (twice from scratch, then incrementally). Queue it; never run it
# under quiet mode. Run it once per commit under test, the D231 base and the D231 tip:
#
#     bench/d231_stamp_check.sh 9aa6968        # expected RED, see the lane report
#     bench/d231_stamp_check.sh <d231 tip>     # expected GREEN
#
# WHAT IT BUILDS, all under $WORK (default /Users/idide/wt/d231-check-<sha12>.noindex):
#   clone.noindex/   `git clone --shared` of the ferrodb repository, on a fresh branch at <commit>.
#                    Its `.git` is a DIRECTORY, like the main checkout's.
#   linked.noindex/  `git worktree add` of that clone, on its own branch at <commit>. Its `.git` is
#                    a FILE, like every lane worktree's.
#   foreign.git/     a bare clone with HEAD detached at <commit>^: a GIT_DIR that points elsewhere.
#   target-clone/, target-linked/  one cargo target dir per checkout, so neither sees the other.
#
# THE INSTRUMENT is the binary itself: an untracked example, examples/d231_stamp_probe.rs, written
# into each checkout before its first build, prints `ferrodb::build_provenance()`. That is the
# same call every bench harness prints. Beside it, two witnesses are read off cargo:
#   rerun=yes|no   whether this build rewrote the build script's `output` file, i.e. re-stamped;
#   lib_fresh      cargo's own `fresh` flag for the ferrodb lib, from --message-format=json.
# A step PASSES only if BOTH the printed stamp and `rerun` are what the step expects: a stamp that
# comes out right without the script having re-run proves nothing about the watch that step
# exists to exercise. Every build gets one line:
#   D231 commit=<sha12> arm=<clone|linked> step=<n-name> expect="<stamp>" got="<stamp>"
#        stamp_ok=<y|n> expect_rerun=<yes|no> rerun=<yes|no> lib_fresh=<true|false>
#        script_stamp=<commit>/<dirty> verdict=<PASS|FAIL>
#
# STEPS, per arm. Q0 is a second commit with S0's exact tree; foreign.git's HEAD is <commit>^.
# Each step is one change, then a build, then the probe:
#   1-fresh-build        nothing                                        at S0          rerun yes
#   2-no-change-rebuild  nothing, again: the re-run on EVERY build that
#                        a missing watched path causes shows up here    at S0          rerun no
#   3-unstaged-edit      a comment appended to src/lib.rs               at S0 +DIRTY   rerun yes
#   4-commit             `git add` + `git commit` of that edit          at S1          rerun yes
#   5-reset-soft         `git reset --soft HEAD~1`: moves the branch's
#                        ref file only; HEAD and the index untouched
#                        (a WITNESS line measures that)                 at S0 +DIRTY   rerun yes
#   6-foreign-git-dir    index mtime touched, build run with
#                        GIT_DIR=foreign.git as a git hook would        at S0 +DIRTY   rerun yes
#                        (NOT at <commit>^, foreign.git's HEAD)
#   7-head-only          `git symbolic-ref HEAD` to a branch at Q0:
#                        rewrites HEAD only (WITNESS line)              at Q0 +DIRTY   rerun yes
#   8-clean-again        `git reset --hard`                             at Q0          rerun yes
#   9-index-only         README.md (not a build input) edited and
#                        `git add`ed: rewrites the index only           at Q0 +DIRTY   rerun yes
#   10-stale-stat        README.md and src/lib.rs touched (contents
#                        unchanged) and the index touched, so the
#                        script re-runs with stale stat information     at Q0 +DIRTY   rerun yes
#   11-no-self-rerun     nothing: a script whose `git status` wrote the
#                        index back in step 10 re-runs itself here      at Q0 +DIRTY   rerun no
#
# EXIT: 0 every verdict PASS; 1 at least one FAIL (the expected result at the base); 2 the
# harness could not measure (a build failed, the probe printed nothing, a sha did not resolve, an
# mtime could not be read). A run that collected nothing has not passed, so 2 is never folded into
# 0 or 1. On exit 2, $WORK is left in place for diagnosis.
#
# CLEANUP: on exit 0 or 1, $WORK is removed unless D231_KEEP=1. The removal is guarded by a
# sentinel this script writes into a directory it created itself (`mkdir` without `-p`, so two
# runs cannot share one), and by the path's shape.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"

REPO=${D231_REPO:-/Users/idide/projects/ferrodb}
ARG=${1:?usage: d231_stamp_check.sh <commit>}
# The run must describe the checkouts it makes, not whatever repository a caller's environment
# names: the same recipe build.rs now follows (githooks(5), `git rev-parse --local-env-vars`).
unset $(git rev-parse --local-env-vars)

SHA=$(git -C "$REPO" rev-parse --verify --quiet "$ARG^{commit}") || { echo "D231 HARNESS: $ARG is not a commit in $REPO"; exit 2; }
SHA12=$(git -C "$REPO" rev-parse --short=12 "$SHA")
WORK=${D231_WORK:-/Users/idide/wt/d231-check-$SHA12.noindex}
case "$WORK" in
  */d231-check-*.noindex) ;;
  *) echo "D231 HARNESS: WORK=$WORK must match */d231-check-*.noindex (Spotlight, and the cleanup guard)"; exit 2 ;;
esac
mkdir -p "$(dirname "$WORK")" || exit 2
# No `-p`: creating the directory IS the check that nobody else is using it.
mkdir "$WORK" 2>/dev/null || { echo "D231 HARNESS: could not create $WORK (it exists, or its parent is unwritable)"; exit 2; }
: > "$WORK/.d231-stamp-check-sentinel"

SELF=$(git -C "$(dirname "$0")" rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
echo "D231 RUN commit_under_test=$SHA12 script_from=$SELF repo=$REPO work=$WORK git=$(git --version | awk '{print $3}') cargo=$(cargo --version 2>/dev/null | awk '{print $2}')"

CLONE=$WORK/clone.noindex
LINKED=$WORK/linked.noindex
FOREIGN=$WORK/foreign.git
PARSE=$WORK/d231_parse.py
PASS=0
FAIL=0

cat > "$PARSE" <<'PY'
# Reads `cargo build --message-format=json` on stdin. Prints: <out_dir or -> <lib fresh: true|false|?>
import json, sys
out_dir, fresh = "-", "?"
for line in sys.stdin:
    try:
        m = json.loads(line)
    except ValueError:
        continue
    pid = m.get("package_id", "")
    if "#ferrodb@" not in pid and not pid.startswith("ferrodb "):
        continue
    if m.get("reason") == "build-script-executed":
        out_dir = m.get("out_dir") or "-"
    elif m.get("reason") == "compiler-artifact":
        t = m.get("target", {})
        if t.get("name") == "ferrodb" and "lib" in t.get("kind", []):
            fresh = "true" if m.get("fresh") else "false"
print(out_dir, fresh)
PY

# mt <var> <path>: set <var> to the file's mtime in ns, or 0 when it does not exist. Runs in the
# calling shell rather than inside `$(...)`, so its `exit 2` ends the run. An unreadable mtime is a
# harness failure: an empty answer compared with another empty answer reads as "unchanged", which
# looks exactly like a measurement.
mt() {
  local v
  v=$(python3 -c 'import os,sys; p=sys.argv[1]; print(os.stat(p).st_mtime_ns if os.path.exists(p) else 0)' "$2")
  case "$v" in
    ''|*[!0-9]*) echo "D231 HARNESS: could not read the mtime of $2"; exit 2 ;;
  esac
  printf -v "$1" '%s' "$v"
}

script_stamp() {  # the env lines the build script handed cargo: <commit>/<dirty>
  local f=$1
  if [ ! -f "$f" ]; then echo "?/?"; return; fi
  local c d
  c=$(sed -n 's/^cargo:rustc-env=FERRODB_BUILD_COMMIT=//p' "$f")
  d=$(sed -n 's/^cargo:rustc-env=FERRODB_BUILD_DIRTY=//p' "$f")
  echo "${c:-?}/${d:-?}"
}

# The build script's `output` file for an arm, once a build has said where it is: written by one
# build's `check`, read by the next.
outfile_for() { cat "$WORK/outfile.$1" 2>/dev/null || echo "-"; }

# check <arm> <checkout> <target> <step> <expected stamp> <expected rerun: yes|no> [VAR=value ...]
check() {
  local arm=$1 ck=$2 tgt=$3 step=$4 expect=$5 expect_rerun=$6
  shift 6
  local of before json_line got after rerun fresh out_dir stamp_ok verdict
  of=$(outfile_for "$arm")
  before=0
  if [ "$of" != "-" ]; then mt before "$of"; fi

  # `${1+"$@"}`, not `"$@"`: bash 3.2 (macOS /bin/bash) calls an empty "$@" unbound under set -u.
  if ! ( cd "$ck" && env CARGO_TARGET_DIR="$tgt" ${1+"$@"} timeout 3600 cargo build --offline --quiet \
        --example d231_stamp_probe --message-format=json > "$WORK/build.$arm.$step.json" \
        2> "$WORK/build.$arm.$step.err" ); then
    echo "D231 HARNESS: build failed, arm=$arm step=$step; see $WORK/build.$arm.$step.err"
    tail -5 "$WORK/build.$arm.$step.err"
    exit 2
  fi
  json_line=$(python3 "$PARSE" < "$WORK/build.$arm.$step.json") || exit 2
  out_dir=${json_line% *}
  fresh=${json_line##* }
  if [ "$out_dir" != "-" ]; then
    of=$(dirname "$out_dir")/output
    echo "$of" > "$WORK/outfile.$arm"
  fi
  got=$("$tgt/debug/examples/d231_stamp_probe" 2>&1 | head -1)
  if [ -z "$got" ]; then echo "D231 HARNESS: the probe printed nothing, arm=$arm step=$step"; exit 2; fi
  if [ "$of" = "-" ]; then echo "D231 HARNESS: cargo never said where the build script's output is, arm=$arm step=$step"; exit 2; fi
  mt after "$of"
  if [ "$after" != "$before" ]; then rerun=yes; else rerun=no; fi

  case "$expect" in
    *" +DIRTY") case "$got" in "$expect "*) stamp_ok=y ;; *) stamp_ok=n ;; esac ;;
    *) if [ "$got" = "$expect" ]; then stamp_ok=y; else stamp_ok=n; fi ;;
  esac
  if [ "$stamp_ok" = y ] && [ "$rerun" = "$expect_rerun" ]; then verdict=PASS; else verdict=FAIL; fi
  if [ "$verdict" = PASS ]; then PASS=$((PASS + 1)); else FAIL=$((FAIL + 1)); fi
  echo "D231 commit=$SHA12 arm=$arm step=$step expect=\"$expect\" got=\"$got\" stamp_ok=$stamp_ok expect_rerun=$expect_rerun rerun=$rerun lib_fresh=$fresh script_stamp=$(script_stamp "$of") verdict=$verdict"
}

# witness <arm> <what> <index-before> <head-before> <ref-before> <index-file> <head-file> <ref-file>
witness() {
  local i h r
  mt i "$6"; mt h "$7"; mt r "$8"
  echo "D231 WITNESS arm=$1 $2 index_touched=$([ "$3" = "$i" ] && echo no || echo yes) head_touched=$([ "$4" = "$h" ] && echo no || echo yes) branch_ref_touched=$([ "$5" = "$r" ] && echo no || echo yes)"
}

probe_source() {
  cat > "$1/examples/d231_stamp_probe.rs" <<'RS'
//! D231 probe: print the build stamp this binary was compiled with. Written into a throwaway
//! checkout by bench/d231_stamp_check.sh and never committed.
fn main() {
    println!("{}", ferrodb::build_provenance());
}
RS
}

g() {  # git in a probe checkout: no hooks, no signing, a fixed identity
  git -C "$1" -c core.hooksPath=/dev/null -c commit.gpgsign=false \
      -c user.name=d231-probe -c user.email=d231@probe.invalid "${@:2}"
}

gpath() {  # an absolute git path in a checkout: gpath <checkout> <name>
  git -C "$1" rev-parse --path-format=absolute --git-path "$2"
}

# --- the checkouts ---------------------------------------------------------------------------
git clone -q --shared --no-checkout "$REPO" "$CLONE" || exit 2
g "$CLONE" checkout -q -b d231-probe "$SHA" || exit 2
g "$CLONE" worktree add -q -b d231-probe-linked "$LINKED" "$SHA" || exit 2
git clone -q --bare --shared "$REPO" "$FOREIGN" || exit 2
git --git-dir="$FOREIGN" update-ref --no-deref HEAD "$SHA^" || exit 2
# Q0: a second commit with EXACTLY S0's tree. Steps 7-11 move HEAD to it, so the stamp must move
# while every file, build.rs included, stays the one under test. (<commit>^ would not do: its
# build.rs is not necessarily the one being tested.)
Q0FULL=$(g "$CLONE" commit-tree "$SHA^{tree}" -p "$SHA" -m "d231 probe: S0's tree, another commit") || exit 2
S0=$(git -C "$CLONE" rev-parse --short=12 HEAD)
Q0=$(git -C "$CLONE" rev-parse --short=12 "$Q0FULL")
FOREIGN12=$(git --git-dir="$FOREIGN" rev-parse --short=12 HEAD)
if [ "$S0" = "$Q0" ] || [ "$S0" = "$FOREIGN12" ]; then
  echo "D231 HARNESS: S0=$S0 Q0=$Q0 foreign=$FOREIGN12; steps 6 and 7 need three different commits"; exit 2
fi
echo "D231 SHAS S0=$S0 Q0=$Q0 foreign=$FOREIGN12"

for arm in clone linked; do
  if [ "$arm" = clone ]; then ck=$CLONE; else ck=$LINKED; fi
  tgt=$WORK/target-$arm
  probe_source "$ck"
  # The checkout kind this arm claims to be, asserted rather than assumed.
  if [ "$arm" = clone ] && [ ! -d "$ck/.git" ]; then echo "D231 HARNESS: $ck/.git is not a directory"; exit 2; fi
  if [ "$arm" = linked ] && [ ! -f "$ck/.git" ]; then echo "D231 HARNESS: $ck/.git is not a file"; exit 2; fi

  check "$arm" "$ck" "$tgt" 1-fresh-build "at $S0" yes
  check "$arm" "$ck" "$tgt" 2-no-change-rebuild "at $S0" no

  printf '\n// d231 probe edit\n' >> "$ck/src/lib.rs"
  check "$arm" "$ck" "$tgt" 3-unstaged-edit "at $S0 +DIRTY" yes

  g "$ck" add src/lib.rs && g "$ck" commit -q -m "d231 probe commit" || exit 2
  S1=$(git -C "$ck" rev-parse --short=12 HEAD)
  check "$arm" "$ck" "$tgt" 4-commit "at $S1" yes

  idx=$(gpath "$ck" index); hf=$(gpath "$ck" HEAD); rf=$(gpath "$ck" "$(git -C "$ck" symbolic-ref -q HEAD)")
  mt i0 "$idx"; mt h0 "$hf"; mt r0 "$rf"
  g "$ck" reset -q --soft HEAD~1 || exit 2
  witness "$arm" reset-soft "$i0" "$h0" "$r0" "$idx" "$hf" "$rf"
  check "$arm" "$ck" "$tgt" 5-reset-soft "at $S0 +DIRTY" yes

  touch "$idx"
  check "$arm" "$ck" "$tgt" 6-foreign-git-dir "at $S0 +DIRTY" yes GIT_DIR="$FOREIGN"

  g "$ck" branch -q "d231-probe-$arm-q0" "$Q0FULL" || exit 2
  mt i0 "$idx"; mt h0 "$hf"; mt r0 "$rf"
  g "$ck" symbolic-ref HEAD "refs/heads/d231-probe-$arm-q0" || exit 2
  witness "$arm" head-only "$i0" "$h0" "$r0" "$idx" "$hf" "$rf"
  check "$arm" "$ck" "$tgt" 7-head-only "at $Q0 +DIRTY" yes

  g "$ck" reset -q --hard || exit 2
  check "$arm" "$ck" "$tgt" 8-clean-again "at $Q0" yes

  printf '\nd231 probe\n' >> "$ck/README.md"
  g "$ck" add README.md || exit 2
  check "$arm" "$ck" "$tgt" 9-index-only "at $Q0 +DIRTY" yes

  touch "$ck/README.md" "$ck/src/lib.rs" "$idx"
  check "$arm" "$ck" "$tgt" 10-stale-stat "at $Q0 +DIRTY" yes
  check "$arm" "$ck" "$tgt" 11-no-self-rerun "at $Q0 +DIRTY" no
done

echo "D231 SUMMARY commit=$SHA12 pass=$PASS fail=$FAIL"

if [ "${D231_KEEP:-0}" = 1 ]; then
  echo "D231 KEPT $WORK"
elif [ -f "$WORK/.d231-stamp-check-sentinel" ]; then
  case "$WORK" in */d231-check-*.noindex) rm -rf "$WORK" && echo "D231 REMOVED $WORK" ;; esac
fi

[ "$FAIL" -eq 0 ]
