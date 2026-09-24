#!/bin/bash
# D231 — does the build stamp compiled into a ferrodb binary follow the tree it was built from?
#
# FAN WORK: this builds ferrodb (twice from scratch, then incrementally). Queue it; never run it
# under quiet mode. Run it once per commit under test, the D231 base and the D231 tip:
#
#     bench/d231_stamp_check.sh 9aa6968        # expected RED: 27-35 PASS, exit 1
#     bench/d231_stamp_check.sh <d231 tip>     # expected GREEN: 50 PASS, exit 0
#
# THE PRE-REGISTRATION IS bench/d231_PREREG.md (its latest amendment): the per-step outcome at both
# commits, which base steps may flip with timing and in which direction, the mutant kill map, and
# what no step exercises. This header summarises it; where the two disagree, the PREREG wins.
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
# into each checkout before its first build, prints `ferrodb::build_provenance()`, the same call
# every bench harness prints. Two witnesses are read off cargo beside it:
#   rerun=yes|no   whether this build rewrote the build script's `output` file, i.e. re-stamped;
#   lib_fresh      cargo's own `fresh` flag for the ferrodb lib, from --message-format=json.
# A step PASSES only if the printed stamp AND `rerun` are both what the step expects, and, for
# steps 5, 7, 23 and 24, the files git touched are exactly the ones the step exists to isolate.
# Every build gets one line:
#   D231 commit=<sha12> arm=<clone|linked> step=<n-name> expect="<stamp>" got="<stamp>"
#        stamp_ok=<y|n> expect_rerun=<yes|no> rerun=<yes|no> [witness...] lib_fresh=<true|false>
#        script_stamp=<commit>/<dirty> verdict=<PASS|FAIL>
#
# WHY SOME STEPS SLEEP FIRST. A git operation that writes working-tree files and then the index
# in the same second leaves those entries racy, and a plain `git status` (the base's) rewrites such
# an index. Cargo sets the `output` file's mtime to the moment the script was invoked, so an index
# the script rewrote is newer than that and re-runs the script at the next build: a self-re-run
# (measured by D231 review 2, M3 and M4). The two-second sleep after each such operation (the
# checkout, `reset --hard`, `checkout --`) makes that write-back produce an index with nothing
# racy, so exactly ONE later build shows it, the SETTLE step, and the step after that starts
# clean at both commits. The tip never writes the index, so the sleeps change nothing there.
# The harness's own `git status` calls all pass `--no-optional-locks`, so it never writes the
# index either.
#
# STEPS, per arm. Q0 is a second commit with S0's exact tree; foreign.git's HEAD is <commit>^;
# $first is the arm's first branch (d231-probe, or d231-probe-linked).
#   n  name                change before the build                        expect         rerun
#   1  fresh-build         (checkout, then sleep)                         at S0          yes
#   2  no-change           nothing                                        at S0          no
#   3  src-edit            comment appended to src/lib.rs                 at S0 +DIRTY   yes
#   4  commit              `git add` + `git commit` of that edit          at S1          yes
#   5  reset-soft          `git reset --soft HEAD~1`; witness: only
#                          $first's ref file moved                        at S0 +DIRTY   yes
#   6  foreign-git-dir     index touched; built with GIT_DIR=foreign      at S0 +DIRTY   yes
#   7  head-only           `git symbolic-ref HEAD` to a branch at Q0;
#                          witness: only HEAD moved                       at Q0 +DIRTY   yes
#   8  reset-hard          `git reset --hard`, sleep                      at Q0          yes
#   9  settle              nothing                                        at Q0          no
#   10 example-edit        comment appended to a TRACKED examples/*.rs    at Q0 +DIRTY   yes
#   11 example-restore     `git checkout --` it, sleep                    at Q0          yes
#   12 settle              nothing                                        at Q0          no
#   13 test-edit           comment appended to a TRACKED tests/*.rs       at Q0 +DIRTY   yes
#   14 test-restore        `git checkout --` it, sleep                    at Q0          yes
#   15 settle              nothing                                        at Q0          no
#   16 manifest-edit       comment appended to Cargo.toml                 at Q0 +DIRTY   yes
#   17 manifest-restore    `git checkout --` it, sleep                    at Q0          yes
#   18 settle              nothing                                        at Q0          no
#   19 index-only          README.md (not an input) edited, `git add`     at Q0 +DIRTY   yes
#   20 stale-stat          README.md, src/lib.rs, the index touched       at Q0 +DIRTY   yes
#   21 no-self-rerun       nothing                                        at Q0 +DIRTY   no
#   22 chain-link          symref refs/heads/d231-link-<arm> -> the Q0
#                          branch; HEAD -> the link                       at Q0 +DIRTY   yes
#   23 chain-retarget      the link -> $first (at S0); witness: only the
#                          link's file moved                              at S0 +DIRTY   yes
#   24 chain-second-link   `git update-ref $first Q0`: moves the chain's
#                          SECOND link; witness: only $first's file moved at Q0 +DIRTY   yes
#   25 replace             `git replace Q0 X`, X = a commit of the current
#                          index tree (so honouring the replacement reads
#                          clean), index touched; `replace -d` after      at Q0 +DIRTY   yes
#
# WHICH STEP KILLS WHICH MUTANT OF build.rs (each removal fails at least the steps named):
#   HEAD watch 7, 22 · index watch 6, 19, 25 · every link 5, 23, 24 · links past the first
#   (`links.len() < 1`) 24 · a recursive `symbolic-ref -q HEAD` 23 · src/ 3 · examples/ 10 ·
#   tests/ 13 · Cargo.toml 16 · INPUTS = ["src","benches","build.rs"] 10, 13, 16 · env clearing 6
#   · --no-optional-locks 2, 9, 12, 15, 18, 21 · --no-replace-objects 25.
#   ⚠ SHARED INFERRED PREMISE: the Cargo.toml kill (step 16 at the tip) and base clone step 16's
#   FAIL both rest on cargo NOT re-running a rerun-if-changed script when only a comment is
#   appended to Cargo.toml. Base clone step 16 is its only measurement: if it comes out PASS with
#   rerun=yes, the Cargo.toml row is void at the tip too, not only the base cell.
#   NOT EXERCISED by any step: Cargo.lock (cargo may rewrite a lock it did not write); benches/
#   (absent); the reftable directories (ferrodb uses the files store); the fallback for a git
#   without `--no-recurse`; the refusal when git does not answer one absolute path per question;
#   the depth cap (git resolves 4 links and refuses 5; the walk collects at most 5).
#
# PRE-REGISTERED (bench/d231_PREREG.md, amendment 1): 2 arms x 25 steps = 50 verdicts.
#   AT THE D231 TIP: 50 PASS, exit 0. No step depends on timing.
#   AT 9aa6968: nominally 27 PASS / 23 FAIL, exit 1.
#     clone  PASS 1 4 7 8 11 14 17 19 20 22;   FAIL 2 3 5 6 9 10 12 13 15 16 18 21 23 24 25
#     linked PASS 1 3 4 5 7 8 10 11 13 14 16 17 19 20 22 23 24;   FAIL 2 6 9 12 15 18 21 25
#   TIMING-DEPENDENT base steps, and the only flips allowed:
#     clone 2, 9, 12, 15, 18  FAIL->PASS with rerun=no, only if the preceding git operation's file
#                             writes and index write straddled a second boundary;
#     clone 5, 23, 24         FAIL->PASS with rerun=yes, only if the preceding builds took under
#                             about 1 s (each recompiles the lib, so INFERRED improbable).
#   So a base run gives PASS 27..35, FAIL 15..23, exit 1; any other deviation is a MISMATCH.
#
# EXIT: 0 only if FAIL is 0 AND PASS is exactly 50; 1 any FAIL; 2 the harness could not measure:
# a build failed; the probe printed something that is not a stamp; cargo emitted no
# build-script-executed message for ferrodb (the Cargo book says it is emitted even when the
# script does not run); the script's `output` was missing before or after a build; a witnessed
# file was missing before or after its git operation; one of the harness's own edits did not take
# (an append `git status` cannot see, a restore it still sees, a `touch` that did not move the
# mtime); a git operation or sha lookup failed; or other than 50 verdicts were reached. A run that
# collected nothing has not passed, so 2 is never folded into 0 or 1.
#
# EVIDENCE: every D231 line is also written to $WORK/d231-result-<sha12>.txt, and each step's
# cargo JSON, cargo stderr and build-script `output` are copied to $WORK/steps/. On exit 0 or 1
# only the heavy directories (the checkouts, foreign.git, the target dirs) are removed, unless
# D231_KEEP=1; the evidence stays. On exit 2 everything stays. The removal is guarded by a
# sentinel this script writes into a directory it created itself (`mkdir` without `-p`, so two
# runs cannot share one), and by the path's shape.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"

REPO=${D231_REPO:-/Users/idide/projects/ferrodb}
ARG=${1:?usage: d231_stamp_check.sh <commit>}
# The run must describe the checkouts it makes, not whatever repository a caller's environment
# names: the same recipe build.rs now follows (githooks(5), `git rev-parse --local-env-vars`).
unset $(git rev-parse --local-env-vars)

harness() { echo "D231 HARNESS: $*"; exit 2; }

SHA=$(git -C "$REPO" rev-parse --verify --quiet "$ARG^{commit}") || harness "$ARG is not a commit in $REPO"
SHA12=$(git -C "$REPO" rev-parse --short=12 "$SHA") || harness "could not abbreviate $SHA"
WORK=${D231_WORK:-/Users/idide/wt/d231-check-$SHA12.noindex}
case "$WORK" in
  */d231-check-*.noindex) ;;
  *) harness "WORK=$WORK must match */d231-check-*.noindex (Spotlight, and the cleanup guard)" ;;
esac
mkdir -p "$(dirname "$WORK")" || exit 2
# No `-p`: creating the directory IS the check that nobody else is using it.
mkdir "$WORK" 2>/dev/null || harness "could not create $WORK (it exists, or its parent is unwritable)"
: > "$WORK/.d231-stamp-check-sentinel"
mkdir "$WORK/steps" || exit 2

# The harness's own provenance, with the mark D231 exists to make: an edited script says so.
SELF=$(git -C "$(dirname "$0")" rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
if [ -n "$(git -C "$(dirname "$0")" --no-optional-locks status --porcelain -- "$(basename "$0")" 2>/dev/null)" ]; then
  SELF="$SELF+DIRTY"
fi

CLONE=$WORK/clone.noindex
LINKED=$WORK/linked.noindex
FOREIGN=$WORK/foreign.git
PARSE=$WORK/d231_parse.py
RESULT=$WORK/d231-result-$SHA12.txt
PASS=0
FAIL=0
EXPECTED_VERDICTS=50
# What build_provenance() prints: `at <sha, 12 or more hex|unknown>`, then ` +DIRTY (...)`.
STAMP_RE='^at ([0-9a-f]{12,}|unknown)( \+DIRTY .*)?$'
# Set by `witness`, consumed and cleared by the next `check`.
WITNESS_NOTE=""
WITNESS_OK=y
SNAP_F=()
SNAP_M=()

say() { echo "$*" | tee -a "$RESULT"; }
say "D231 RUN commit_under_test=$SHA12 script_from=$SELF repo=$REPO work=$WORK git=$(git --version | awk '{print $3}') cargo=$(cargo --version 2>/dev/null | awk '{print $2}')"

cat > "$PARSE" <<'PY'
# Reads `cargo build --message-format=json` on stdin. Prints: <out_dir or -> <lib fresh: true|false|?>
import json, re, sys
out_dir, fresh = "-", "?"
# ferrodb's package id, in each spelling cargo has used: `path+file:///<dir>#ferrodb@<ver>`,
# `path+file:///<...>/ferrodb#<ver>` when the directory is named after the package, and the old
# `ferrodb <ver> (path+file:///...)`.
OURS = re.compile(r"(#ferrodb@|/ferrodb#[0-9]|^ferrodb )")
for line in sys.stdin:
    try:
        m = json.loads(line)
    except ValueError:
        continue
    if not OURS.search(m.get("package_id", "")):
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
# calling shell rather than inside `$(...)`, so its `exit 2` ends the run. Callers that need the
# file to exist check that themselves: 0 compared with 0 reads as "unchanged".
mt() {
  local v
  v=$(python3 -c 'import os,sys; p=sys.argv[1]; print(os.stat(p).st_mtime_ns if os.path.exists(p) else 0)' "$2")
  case "$v" in
    ''|*[!0-9]*) harness "could not read the mtime of $2" ;;
  esac
  printf -v "$1" '%s' "$v"
}

# sha12 <checkout> <rev>: the 12-or-more-hex abbreviation, or failure (callers exit 2 on it).
sha12() {
  local s
  s=$(git -C "$1" rev-parse --verify -q --short=12 "$2^{commit}") || return 1
  case "$s" in ''|*[!0-9a-f]*) return 1 ;; esac
  [ ${#s} -ge 12 ] || return 1
  echo "$s"
}

# The harness's own edits, each checked to have taken before anything is built on it: an edit
# that silently failed would otherwise be scored, and at the base a no-op edit reproduces exactly
# the FAILs the PREREG predicts.
porcelain() { git -C "$1" --no-optional-locks status --porcelain --untracked-files=no -- "$2"; }
append() {  # append <checkout> <path> <text>
  printf '%s' "$3" >> "$1/$2" || harness "could not append to $1/$2"
  [ -n "$(porcelain "$1" "$2")" ] || harness "an append to $2 is not visible to git status"
}
restore() {  # restore <checkout> <path>
  g "$1" checkout -q -- "$2" || harness "git checkout -- $2 failed"
  [ -z "$(porcelain "$1" "$2")" ] || harness "$2 still differs after git checkout --"
}
touch_moved() {  # touch_moved <file>...: every file must exist and its mtime must move
  local f b a
  for f in "$@"; do
    [ -f "$f" ] || harness "cannot touch $f: it does not exist"
    mt b "$f"
    touch "$f" || harness "touch $f failed"
    mt a "$f"
    [ "$a" != "$b" ] || harness "touch did not move the mtime of $f"
  done
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
  local of before json_line got after rerun fresh out_dir stamp_ok verdict wnote
  of=$(outfile_for "$arm")
  before=0
  if [ "$of" != "-" ]; then
    mt before "$of"
    [ "$before" != 0 ] || harness "the build script's output $of vanished before arm=$arm step=$step"
  fi

  # `${1+"$@"}`, not `"$@"`: bash 3.2 (macOS /bin/bash) calls an empty "$@" unbound under set -u.
  if ! ( cd "$ck" && env CARGO_TARGET_DIR="$tgt" ${1+"$@"} timeout 3600 cargo build --offline --quiet \
        --example d231_stamp_probe --message-format=json > "$WORK/steps/$arm.$step.json" \
        2> "$WORK/steps/$arm.$step.err" ); then
    tail -5 "$WORK/steps/$arm.$step.err"
    harness "build failed, arm=$arm step=$step; see $WORK/steps/$arm.$step.err"
  fi
  json_line=$(python3 "$PARSE" < "$WORK/steps/$arm.$step.json") || harness "could not parse cargo's JSON, arm=$arm step=$step"
  out_dir=${json_line% *}
  fresh=${json_line##* }
  # Emitted on every build, fresh or not (Cargo book, external-tools), so its absence is a fault
  # in this harness's reading, never a reason to reuse the last step's path.
  [ "$out_dir" != "-" ] || harness "cargo emitted no build-script-executed message for ferrodb, arm=$arm step=$step"
  of=$(dirname "$out_dir")/output
  echo "$of" > "$WORK/outfile.$arm"
  # stdout only: an exec error or a panic goes to stderr, and must not be read as a stamp.
  got=$("$tgt/debug/examples/d231_stamp_probe" | head -1)
  # Only a stamp is a measurement. Anything else would score as a FAIL, and a FAIL is what the
  # base run is expected to produce.
  [[ $got =~ $STAMP_RE ]] || harness "the probe printed something that is not a stamp, arm=$arm step=$step: '$got'"
  mt after "$of"
  [ "$after" != 0 ] || harness "the build script's output $of does not exist after a build, arm=$arm step=$step"
  cp "$of" "$WORK/steps/$arm.$step.output" || exit 2
  if [ "$after" != "$before" ]; then rerun=yes; else rerun=no; fi

  case "$expect" in
    *" +DIRTY") case "$got" in "$expect "*) stamp_ok=y ;; *) stamp_ok=n ;; esac ;;
    *) if [ "$got" = "$expect" ]; then stamp_ok=y; else stamp_ok=n; fi ;;
  esac
  if [ "$stamp_ok" = y ] && [ "$rerun" = "$expect_rerun" ] && [ "$WITNESS_OK" = y ]; then
    verdict=PASS; PASS=$((PASS + 1))
  else
    verdict=FAIL; FAIL=$((FAIL + 1))
  fi
  wnote=${WITNESS_NOTE:+ $WITNESS_NOTE}
  WITNESS_NOTE=""; WITNESS_OK=y
  say "D231 commit=$SHA12 arm=$arm step=$step expect=\"$expect\" got=\"$got\" stamp_ok=$stamp_ok expect_rerun=$expect_rerun rerun=$rerun$wnote lib_fresh=$fresh script_stamp=$(script_stamp "$of") verdict=$verdict"
}

# snap <file>...: record the mtimes of the files a git operation must or must not touch. Each
# must exist BEFORE the operation: `mt` reads a missing file as 0, and a file that appears reads
# as "touched" (a gc between steps can remove a loose ref that the next operation recreates).
snap() {
  local f m
  SNAP_F=(); SNAP_M=()
  for f in "$@"; do
    [ -f "$f" ] || harness "witnessed file $f does not exist before the operation"
    mt m "$f"
    SNAP_F+=("$f"); SNAP_M+=("$m")
  done
}

# witness <expected touched flags, one yes|no per snapped file, in order>
# Folded into the NEXT check's verdict: that step tests one watch only if exactly the expected
# files moved. A witnessed file missing after the operation is a harness failure, never a "no".
witness() {
  local i m seen=""
  for i in "${!SNAP_F[@]}"; do
    [ -f "${SNAP_F[$i]}" ] || harness "witnessed file ${SNAP_F[$i]} does not exist after the operation"
    mt m "${SNAP_F[$i]}"
    if [ "$m" = "${SNAP_M[$i]}" ]; then seen="$seen no"; else seen="$seen yes"; fi
  done
  seen=${seen# }
  if [ "$seen" = "$1" ]; then WITNESS_OK=y; else WITNESS_OK=n; fi
  WITNESS_NOTE="witness_touched=\"$seen\"(expect \"$1\")"
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
git clone -q --shared --no-checkout "$REPO" "$CLONE" || harness "git clone failed"
g "$CLONE" checkout -q -b d231-probe "$SHA" || harness "checkout of $SHA failed"
g "$CLONE" worktree add -q -b d231-probe-linked "$LINKED" "$SHA" || harness "git worktree add failed"
git clone -q --bare --shared "$REPO" "$FOREIGN" || harness "the bare clone failed"
git --git-dir="$FOREIGN" update-ref --no-deref HEAD "$SHA^" || harness "could not detach foreign.git at $SHA^"
# Q0: a second commit with EXACTLY S0's tree. Steps 7-25 sit on it, so the stamp moves while every
# file, build.rs included, stays the one under test. (<commit>^ would not do: its build.rs is not
# necessarily the one being tested.)
Q0FULL=$(g "$CLONE" commit-tree "$SHA^{tree}" -p "$SHA" -m "d231 probe: S0's tree, another commit") || harness "commit-tree for Q0 failed"
S0=$(sha12 "$CLONE" HEAD) || harness "S0 did not resolve"
Q0=$(sha12 "$CLONE" "$Q0FULL") || harness "Q0 did not resolve"
FOREIGN12=$(git --git-dir="$FOREIGN" rev-parse --verify -q --short=12 HEAD) || harness "foreign.git's HEAD did not resolve"
if [ "$S0" = "$Q0" ] || [ "$S0" = "$FOREIGN12" ]; then
  harness "S0=$S0 Q0=$Q0 foreign=$FOREIGN12; steps 6 and 7 need three different commits"
fi
say "D231 SHAS S0=$S0 Q0=$Q0 foreign=$FOREIGN12"

for arm in clone linked; do
  if [ "$arm" = clone ]; then ck=$CLONE; first="refs/heads/d231-probe"; else ck=$LINKED; first="refs/heads/d231-probe-linked"; fi
  tgt=$WORK/target-$arm
  probe_source "$ck"
  # The checkout kind this arm claims to be, asserted rather than assumed.
  if [ "$arm" = clone ] && [ ! -d "$ck/.git" ]; then harness "$ck/.git is not a directory"; fi
  if [ "$arm" = linked ] && [ ! -f "$ck/.git" ]; then harness "$ck/.git is not a file"; fi
  ex=$(git -C "$ck" ls-files 'examples/*.rs' | head -1)
  ts=$(git -C "$ck" ls-files 'tests/*.rs' | head -1)
  if [ -z "$ex" ] || [ -z "$ts" ]; then harness "no tracked example or test file to edit"; fi
  idx=$(gpath "$ck" index) || harness "no index path"
  hf=$(gpath "$ck" HEAD) || harness "no HEAD path"
  ff=$(gpath "$ck" "$first") || harness "no path for $first"

  sleep 2   # after the checkout: see WHY SOME STEPS SLEEP FIRST
  check "$arm" "$ck" "$tgt" 1-fresh-build "at $S0" yes
  check "$arm" "$ck" "$tgt" 2-no-change "at $S0" no

  append "$ck" src/lib.rs $'\n// d231 probe edit\n'
  check "$arm" "$ck" "$tgt" 3-src-edit "at $S0 +DIRTY" yes

  g "$ck" add src/lib.rs || harness "git add src/lib.rs failed"
  g "$ck" commit -q -m "d231 probe commit" || harness "git commit failed"
  S1=$(sha12 "$ck" HEAD) || harness "S1 did not resolve"
  [ "$S1" != "$S0" ] || harness "the probe commit did not move HEAD"
  check "$arm" "$ck" "$tgt" 4-commit "at $S1" yes

  snap "$idx" "$hf" "$ff"
  g "$ck" reset -q --soft HEAD~1 || harness "git reset --soft failed"
  witness "no no yes"
  check "$arm" "$ck" "$tgt" 5-reset-soft "at $S0 +DIRTY" yes

  touch_moved "$idx"
  check "$arm" "$ck" "$tgt" 6-foreign-git-dir "at $S0 +DIRTY" yes GIT_DIR="$FOREIGN"

  g "$ck" branch -q "d231-probe-$arm-q0" "$Q0FULL" || harness "git branch for Q0 failed"
  snap "$idx" "$hf" "$ff"
  g "$ck" symbolic-ref HEAD "refs/heads/d231-probe-$arm-q0" || harness "git symbolic-ref HEAD failed"
  witness "no yes no"
  check "$arm" "$ck" "$tgt" 7-head-only "at $Q0 +DIRTY" yes

  g "$ck" reset -q --hard || harness "git reset --hard failed"
  sleep 2
  check "$arm" "$ck" "$tgt" 8-reset-hard "at $Q0" yes
  check "$arm" "$ck" "$tgt" 9-settle "at $Q0" no

  append "$ck" "$ex" $'\n// d231 probe edit\n'
  check "$arm" "$ck" "$tgt" 10-example-edit "at $Q0 +DIRTY" yes
  restore "$ck" "$ex"
  sleep 2
  check "$arm" "$ck" "$tgt" 11-example-restore "at $Q0" yes
  check "$arm" "$ck" "$tgt" 12-settle "at $Q0" no

  append "$ck" "$ts" $'\n// d231 probe edit\n'
  check "$arm" "$ck" "$tgt" 13-test-edit "at $Q0 +DIRTY" yes
  restore "$ck" "$ts"
  sleep 2
  check "$arm" "$ck" "$tgt" 14-test-restore "at $Q0" yes
  check "$arm" "$ck" "$tgt" 15-settle "at $Q0" no

  append "$ck" Cargo.toml $'\n# d231 probe edit\n'
  check "$arm" "$ck" "$tgt" 16-manifest-edit "at $Q0 +DIRTY" yes
  restore "$ck" Cargo.toml
  sleep 2
  check "$arm" "$ck" "$tgt" 17-manifest-restore "at $Q0" yes
  check "$arm" "$ck" "$tgt" 18-settle "at $Q0" no

  append "$ck" README.md $'\nd231 probe\n'
  g "$ck" add README.md || harness "git add README.md failed"
  check "$arm" "$ck" "$tgt" 19-index-only "at $Q0 +DIRTY" yes

  touch_moved "$ck/README.md" "$ck/src/lib.rs" "$idx"
  check "$arm" "$ck" "$tgt" 20-stale-stat "at $Q0 +DIRTY" yes
  check "$arm" "$ck" "$tgt" 21-no-self-rerun "at $Q0 +DIRTY" no

  # A one-link symbolic-ref chain: HEAD -> link -> the Q0 branch.
  link="refs/heads/d231-link-$arm"
  g "$ck" symbolic-ref "$link" "refs/heads/d231-probe-$arm-q0" || harness "could not create $link"
  g "$ck" symbolic-ref HEAD "$link" || harness "could not point HEAD at $link"
  check "$arm" "$ck" "$tgt" 22-chain-link "at $Q0 +DIRTY" yes
  lf=$(gpath "$ck" "$link") || harness "no path for $link"

  # Retarget the FIRST link: HEAD -> link -> $first (at S0). Neither HEAD nor the index moves.
  snap "$idx" "$hf" "$lf"
  g "$ck" symbolic-ref "$link" "$first" || harness "could not retarget $link"
  witness "no no yes"
  check "$arm" "$ck" "$tgt" 23-chain-retarget "at $S0 +DIRTY" yes

  # Move the SECOND link: $first itself, from S0 to Q0. Only $first's file moves.
  snap "$idx" "$hf" "$lf" "$ff"
  g "$ck" update-ref "$first" "$Q0FULL" || harness "git update-ref $first failed"
  witness "no no no yes"
  check "$arm" "$ck" "$tgt" 24-chain-second-link "at $Q0 +DIRTY" yes

  # A replacement for Q0 whose tree is the current index tree (Q0 plus the staged README), so a
  # git that honours replacements reads the tree clean. Its parent is S0, not Q0, so the
  # replacement cannot form a cycle. Both premises are asserted before the build: honoured it
  # reads clean, and with --no-replace-objects it does not.
  xtree=$(git -C "$ck" write-tree) || harness "git write-tree failed"
  xcommit=$(g "$ck" commit-tree "$xtree" -p "$SHA" -m "d231 probe: replacement for Q0") || harness "commit-tree for the replacement failed"
  g "$ck" replace "$Q0FULL" "$xcommit" || harness "git replace failed"
  [ -z "$(git -C "$ck" --no-optional-locks status --porcelain --untracked-files=no)" ] \
    || harness "with the replacement honoured the tree does not read clean; step 25 would not discriminate"
  [ -n "$(git -C "$ck" --no-optional-locks --no-replace-objects status --porcelain --untracked-files=no)" ] \
    || harness "without the replacement the tree reads clean; step 25 would not discriminate"
  touch_moved "$idx"
  check "$arm" "$ck" "$tgt" 25-replace "at $Q0 +DIRTY" yes
  g "$ck" replace -d "$Q0FULL" > /dev/null || harness "git replace -d failed"
done

say "D231 SUMMARY commit=$SHA12 pass=$PASS fail=$FAIL verdicts=$((PASS + FAIL)) expected_verdicts=$EXPECTED_VERDICTS result=$RESULT"
if [ $((PASS + FAIL)) -ne "$EXPECTED_VERDICTS" ]; then
  harness "$((PASS + FAIL)) verdicts reached, not $EXPECTED_VERDICTS"
fi

if [ "${D231_KEEP:-0}" = 1 ]; then
  echo "D231 KEPT $WORK"
elif [ -f "$WORK/.d231-stamp-check-sentinel" ]; then
  case "$WORK" in
    */d231-check-*.noindex)
      rm -rf "$CLONE" "$LINKED" "$FOREIGN" "$WORK/target-clone" "$WORK/target-linked" \
        && echo "D231 REMOVED the checkouts and target dirs; evidence kept in $WORK" ;;
  esac
fi

[ "$FAIL" -eq 0 ] && [ "$PASS" -eq "$EXPECTED_VERDICTS" ]
