#!/bin/bash
# D231 — does the build stamp compiled into a ferrodb binary follow the tree it was built from?
#
# FAN WORK: this builds ferrodb (twice from scratch, then incrementally). Queue it; never run it
# under quiet mode. Run it once per commit under test, the D231 base and the D231 tip:
#
#     bench/d231_stamp_check.sh 9aa6968        # expected RED: 51-61 PASS, exit 1
#     bench/d231_stamp_check.sh <d231 tip>     # expected GREEN: 104 PASS, exit 0
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
#   probe-clone/, probe-linked/, probe-<arm>-export/  the probe crates (below).
#   target-clone/, target-linked/  one cargo target dir per checkout, so neither sees the other,
#                    and neither is shared with any other tree (a shared target can run one tree's
#                    code carrying another tree's stamp).
#   shim45-<arm>/, shim47-<arm>/  the one-build `git` wrappers of steps 45 and 47.
#
# THE INSTRUMENT is a binary built against the checkout: the probe crate $WORK/probe-<arm>, OUTSIDE
# the checkout, depends on it by path and prints `ferrodb::build_provenance()`, the same call every
# bench harness prints. (It used to be an untracked example inside the checkout, which the tip now
# counts as an untracked build input: PREREG amendment 3.) Each build is `cargo build` in the probe
# crate; the binary is $tgt/debug/d231_probe. Two witnesses are read off cargo beside it:
#   rerun=yes|no   whether this build rewrote ferrodb's build-script `output` file, i.e. re-stamped;
#   lib_fresh      cargo's own `fresh` flag for the ferrodb lib, from --message-format=json.
# A step PASSES only if the printed stamp AND `rerun` are both what the step expects, and, for
# steps 5, 7, 23, 24, 26 and 28, the files git touched are exactly the ones the step exists to
# isolate.
# Every build gets one line:
#   D231 commit=<sha12> arm=<clone|linked> step=<n-name> expect="<stamp>" got="<stamp>"
#        stamp_ok=<y|n> expect_rerun=<yes|no> rerun=<yes|no> [witness...] lib_fresh=<true|false>
#        script_stamp=<commit>/<dirty> verdict=<PASS|FAIL>
# and step 45 adds one `D231 NOTE` line: whether its shim refused an `ls-files -v` (the base never
# asks one).
#
# WHY SOME STEPS SLEEP FIRST. A git operation that writes working-tree files and then the index
# in the same second leaves those entries racy, and a plain `git status` (the base's) rewrites such
# an index. Cargo sets the `output` file's mtime to the moment the script was invoked, so an index
# the script rewrote is newer than that and re-runs the script at the next build: a self-re-run
# (measured by D231 review 2, M3 and M4). The two-second sleep after each such operation (the
# checkout, `reset --hard`, `checkout --`) makes that write-back produce an index with nothing
# racy, so exactly ONE later build shows it, the SETTLE step, and the step after that starts
# clean at both commits. The tip never writes the index, so the sleeps change nothing there.
# Steps 35-52 expect a re-run at every step, so a self-re-run cannot change their verdicts.
# The harness's own read-only questions never write the index either: its `git status` calls pass
# `--no-optional-locks`, and step 25's `write-tree`, which rewrites the index it reads even with
# that flag (review 4, MEASURED), runs on a COPY of the index. The index is written only by the
# git operations that ARE a step's change (add, commit, reset, checkout, update-index).
#
# STEPS, per arm. Q0 is a second commit with S0's exact tree; foreign.git's HEAD is <commit>^;
# $first is the arm's first branch (d231-probe, or d231-probe-linked). "touch" is `touch_moved`
# of the index, which forces a re-run.
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
#   26 unrelated-packed-ref an UNRELATED packed ref deleted (packed-refs
#                          rewritten); witness: only packed-refs moved    at Q0 +DIRTY   NO
#   27 chain-three-links   symref link2 -> link; HEAD -> link2             at Q0 +DIRTY   yes
#   28 third-link          `git update-ref $first S0`: the chain's THIRD
#                          link; witness: only $first's file moved        at S0 +DIRTY   yes
#   29 status-fails        status.aheadBehind=bogus (fails `git status`
#                          only), index touched; unset after              at unknown +DIRTY yes
#   30 status-recovers     index touched                                  at S0 +DIRTY   yes
#   31 assume-unchanged    src/lib.rs edited, then marked assume-unchanged
#                          (status hides it); cleared and restored after at unknown +DIRTY yes
#   32 export-inside-      `git archive S0` into the checkout's ignored
#      checkout            target/d231-export/; a probe crate built on it at unknown +DIRTY yes
#   33 reset-hard-2        `git reset --hard` (HEAD -> link2 -> link ->
#                          $first at S0), sleep                           at S0          yes
#   34 settle              nothing                                        at S0          no
#   35 untracked-input     untracked examples/d231_untracked.rs           at S0 +DIRTY   yes
#   36 untracked-removed   it removed; touch                              at S0          yes
#   37 ignored-rs-input    tests/d231_ignored.rs, excluded in info/exclude at S0 +DIRTY   yes
#   38 ignored-rs-removed  it and the exclude line removed; touch         at S0          yes
#   39 clean-filter        a same-size edit to a comment in src/lib.rs
#                          that a clean filter reverses, so `git status`
#                          reads it clean                                 at S0 +DIRTY   yes
#   40 filter-removed      filter and attribute removed, file restored;
#                          touch                                          at S0          yes
#   41 skip-worktree       src/lib.rs edited, marked skip-worktree        at unknown +DIRTY yes
#   42 skip-cleared        bit cleared, file restored; touch              at S0          yes
#   43 ignorestat          core.ignoreStat=true, no entry marked; touch   at unknown +DIRTY yes
#   44 ignorestat-cleared  unset; touch                                   at S0          yes
#   45 ls-files-fails      shim: `git ls-files -v` exits 1, every other
#                          call passes; touch                             at unknown +DIRTY yes
#   46 ls-files-recovers   touch                                          at S0          yes
#   47 head-moves-during-  shim: the first `git status` moves $first to
#      status              Q0 before answering; touch                     at unknown +DIRTY yes
#   48 head-moved-restamps nothing ($first moved during the last run)     at Q0          yes
#   49 packed-away         `pack-refs --include $first`: its loose file,
#                          a watched path, is gone                        at Q0          yes
#   50 packed-branch-moves `update-ref $first S0` creates the file again  at S0          yes
#   51 cargo-config        untracked .cargo/config.toml at the package
#                          root (not on the probe's config path); touch   at S0 +DIRTY   yes
#   52 cargo-config-removed .cargo removed; touch                         at S0          yes
#
# WHICH STEP KILLS WHICH MUTANT OF build.rs (each removal fails at least the steps named):
#   HEAD watch 7, 22 · index watch 6, 19, 25 · every link 5, 23, 24 · links past the first
#   (`links.len() < 1`) 24 · a recursive `symbolic-ref -q HEAD` 23 · src/ 3 · examples/ 10 ·
#   tests/ 13 · Cargo.toml 16 · INPUTS = ["src","benches","build.rs"] 10, 13, 16 · env clearing 6
#   · --no-optional-locks 2, 9, 12, 15, 18, 21, 34 · --no-replace-objects 25 · packed-refs
#   watched 26 · links.len() < 2 28 · a failed status keeping the sha 29 · no index-bit check 31
#   · no --show-toplevel check 32 · no untracked-file check 35 · no ignored-.rs check 37 · no
#   byte comparison 39 · the skip-worktree tag not refused (noS) 41 · core.ignoreStat not
#   refused (noignorestat) 43 · an `ls-files -v` that fails read as "nothing hidden"
#   (lsfailopen) 45 · the sha not re-read after the tree questions (non6) 47 · git paths watched
#   only if they exist (existsonly) 50 · .cargo not an input 51.
#   ⚠ SHARED INFERRED PREMISE: the Cargo.toml kill (step 16 at the tip) and base clone step 16's
#   FAIL both rest on cargo NOT re-running a rerun-if-changed script when only a comment is
#   appended to Cargo.toml. Base clone step 16 is its only measurement: if it comes out PASS with
#   rerun=yes, the Cargo.toml row is void at the tip too, not only the base cell.
#   NOT EXERCISED by any step: Cargo.lock (cargo may rewrite a lock it did not write); benches/
#   (absent); rust-toolchain* (a real one makes rustup switch or install toolchains); .cargo or
#   toolchain files in parent directories; a gitlink among the inputs (ferrodb has none; refused
#   as unknown); an fsmonitor hook (step 39 pins the byte comparison through the same gap, git's
#   opinion against the bytes); the reftable directories (ferrodb uses the files store); the
#   fallback for a git without `--no-recurse`; the refusal when git does not answer one absolute
#   path per question; the depth cap (git resolves 4 links and refuses 5; the walk collects at
#   most 5); core.preferSymlinkRefs, a known limit (review 4, N7).
#
# PRE-REGISTERED (bench/d231_PREREG.md, amendments 1, 2, 2a, 3, 3a and 3b): 2 arms x 52 steps = 104
# verdicts.
#   AT THE D231 TIP: 104 PASS, exit 0. No step depends on timing.
#   AT 9aa6968: nominally 51 PASS / 53 FAIL, exit 1.
#     clone  PASS 1 4 7 8 11 14 17 19 20 22 27 30 33 36 38 40 42 44 46 52
#            FAIL 2 3 5 6 9 10 12 13 15 16 18 21 23 24 25 26 28 29 31 32 34 35 37 39 41 43 45 47
#                 48 49 50 51
#     linked PASS 1 3 4 5 7 8 10 11 13 14 16 17 19 20 22 23 24 27 28 30 33 36 38 40 42 44 46 48
#                 49 50 52
#            FAIL 2 6 9 12 15 18 21 25 26 29 31 32 34 35 37 39 41 43 45 47 51
#   TIMING-DEPENDENT base steps, and the only flips allowed:
#     clone 2, 9, 12, 15, 18, 34  FAIL->PASS with rerun=no, only if the preceding git operation's
#                             file writes and index write straddled a second boundary;
#     clone 5, 23, 24, 28     FAIL->PASS with rerun=yes, only if the preceding builds took under
#                             about 1 s (each recompiles the lib, so INFERRED improbable).
#   So a base run gives PASS 51..61, FAIL 43..53, exit 1; any other deviation is a MISMATCH.
#
# EXIT: 0 only if FAIL is 0 AND PASS is exactly 104; 1 any FAIL; 2 the harness could not measure:
# a build failed; the probe printed something that is not a stamp; cargo emitted no
# build-script-executed message for ferrodb (the Cargo book says it is emitted even when the
# script does not run); the script's `output` was missing before or after a build; a witnessed
# file was missing before or after its git operation; one of the harness's own edits did not take
# (an append `git status` cannot see, a restore it still sees, a `touch` that did not move the
# mtime); a git operation, a `git status` or a sha lookup failed; a step's premise did not hold
# (steps 25, 26, 29, 31-33, 35, 37-39, 41, 43, 45, 47, 49-51 assert theirs); or other than 104
# verdicts were reached. A run that collected nothing has not passed, so 2 is never folded into
# 0 or 1.
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
EXPECTED_VERDICTS=104
# What build_provenance() prints: `at <sha, 12 or more hex|unknown>`, then ` +DIRTY (...)`.
STAMP_RE='^at ([0-9a-f]{12,}|unknown)( \+DIRTY .*)?$'
# Set by `witness`, consumed and cleared by the next `check`.
WITNESS_NOTE=""
WITNESS_OK=y
OUTKEY=""
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
# st <var> <checkout> [git flag...] [-- <path>]: `git status --porcelain` into <var>. A git error
# exits 2: a failing status prints nothing on stdout, and nothing reads exactly like "clean".
st() {
  local var=$1 ck=$2 out
  shift 2
  # `${1+"$@"}`: bash 3.2 calls an empty "$@" unbound under set -u.
  out=$(git -C "$ck" --no-optional-locks ${1+"$@"} status --porcelain --untracked-files=no 2>/dev/null) \
    || harness "git status failed in $ck"
  printf -v "$var" '%s' "$out"
}
# The same, for one path.
st_path() {
  local var=$1 ck=$2 path=$3 out
  out=$(git -C "$ck" --no-optional-locks status --porcelain --untracked-files=no -- "$path" 2>/dev/null) \
    || harness "git status -- $path failed in $ck"
  printf -v "$var" '%s' "$out"
}
append() {  # append <checkout> <path> <text>
  local seen
  printf '%s' "$3" >> "$1/$2" || harness "could not append to $1/$2"
  st_path seen "$1" "$2"
  [ -n "$seen" ] || harness "an append to $2 is not visible to git status"
}
restore() {  # restore <checkout> <path>
  local seen
  g "$1" checkout -q -- "$2" || harness "git checkout -- $2 failed"
  st_path seen "$1" "$2"
  [ -z "$seen" ] || harness "$2 still differs after git checkout --"
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

# check <arm> <probe crate> <target> <step> <expected stamp> <expected rerun: yes|no> [VAR=value ...]
check() {
  local arm=$1 probe_dir=$2 tgt=$3 step=$4 expect=$5 expect_rerun=$6
  shift 6
  local of before json_line got after rerun fresh out_dir stamp_ok verdict wnote key
  # The build script's output is tracked per package: step 32 builds a different one.
  key=${OUTKEY:-$arm}
  OUTKEY=""
  of=$(outfile_for "$key")
  before=0
  if [ "$of" != "-" ]; then
    mt before "$of"
    [ "$before" != 0 ] || harness "the build script's output $of vanished before arm=$arm step=$step"
  fi

  # `${1+"$@"}`, not `"$@"`: bash 3.2 (macOS /bin/bash) calls an empty "$@" unbound under set -u.
  if ! ( cd "$probe_dir" && env CARGO_TARGET_DIR="$tgt" ${1+"$@"} timeout 3600 cargo build --offline --quiet \
        --message-format=json > "$WORK/steps/$arm.$step.json" \
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
  echo "$of" > "$WORK/outfile.$key"
  # stdout only: an exec error or a panic goes to stderr, and must not be read as a stamp.
  got=$("$tgt/debug/d231_probe" | head -1)
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

# probe_crate <dir> <package root>: the probe, a crate OUTSIDE the checkout that depends on the
# package by path and prints its stamp. It used to be an untracked example inside the package,
# which is now itself an untracked build input and would stamp every build dirty (PREREG
# amendment 3). The empty [workspace] keeps it out of any workspace above it.
probe_crate() {
  mkdir "$1" "$1/src" || harness "could not create the probe crate at $1"
  cat > "$1/Cargo.toml" <<TOML || harness "could not write $1/Cargo.toml"
[package]
name = "d231_probe"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
ferrodb = { path = "$2" }

[workspace]
TOML
  cat > "$1/src/main.rs" <<'RS' || harness "could not write $1/src/main.rs"
//! D231 probe: print the build stamp of the ferrodb this binary was compiled against.
fn main() {
    println!("{}", ferrodb::build_provenance());
}
RS
}

# shim <dir> <body> [NAME=value ...]: a `git` put first on PATH for one build (PREREG amendments 3
# and 3a). It runs <body> (bash, with git's arguments as "$@", the real git as $REALGIT, its own
# directory as $SHIMDIR, and each NAME set), then hands the call to the real git.
REALGIT=$(command -v git) || harness "no git on PATH"
shim() {
  local dir=$1 body=$2 kv
  shift 2
  mkdir "$dir" || harness "could not create the shim directory $dir"
  {
    printf '#!/bin/bash\nREALGIT=%q\nSHIMDIR=%q\n' "$REALGIT" "$dir"
    for kv in ${1+"$@"}; do printf '%s=%q\n' "${kv%%=*}" "${kv#*=}"; done
    printf '%s\n' "$body"
    printf 'exec "$REALGIT" "$@"\n'
  } > "$dir/git" || harness "could not write the shim $dir/git"
  chmod +x "$dir/git" || harness "could not make $dir/git executable"
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
  probe=$WORK/probe-$arm
  probe_crate "$probe" "$ck"
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
  check "$arm" "$probe" "$tgt" 1-fresh-build "at $S0" yes
  check "$arm" "$probe" "$tgt" 2-no-change "at $S0" no

  append "$ck" src/lib.rs $'\n// d231 probe edit\n'
  check "$arm" "$probe" "$tgt" 3-src-edit "at $S0 +DIRTY" yes

  g "$ck" add src/lib.rs || harness "git add src/lib.rs failed"
  g "$ck" commit -q -m "d231 probe commit" || harness "git commit failed"
  S1=$(sha12 "$ck" HEAD) || harness "S1 did not resolve"
  [ "$S1" != "$S0" ] || harness "the probe commit did not move HEAD"
  check "$arm" "$probe" "$tgt" 4-commit "at $S1" yes

  snap "$idx" "$hf" "$ff"
  g "$ck" reset -q --soft HEAD~1 || harness "git reset --soft failed"
  witness "no no yes"
  check "$arm" "$probe" "$tgt" 5-reset-soft "at $S0 +DIRTY" yes

  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 6-foreign-git-dir "at $S0 +DIRTY" yes GIT_DIR="$FOREIGN"

  g "$ck" branch -q "d231-probe-$arm-q0" "$Q0FULL" || harness "git branch for Q0 failed"
  snap "$idx" "$hf" "$ff"
  g "$ck" symbolic-ref HEAD "refs/heads/d231-probe-$arm-q0" || harness "git symbolic-ref HEAD failed"
  witness "no yes no"
  check "$arm" "$probe" "$tgt" 7-head-only "at $Q0 +DIRTY" yes

  g "$ck" reset -q --hard || harness "git reset --hard failed"
  sleep 2
  check "$arm" "$probe" "$tgt" 8-reset-hard "at $Q0" yes
  check "$arm" "$probe" "$tgt" 9-settle "at $Q0" no

  append "$ck" "$ex" $'\n// d231 probe edit\n'
  check "$arm" "$probe" "$tgt" 10-example-edit "at $Q0 +DIRTY" yes
  restore "$ck" "$ex"
  sleep 2
  check "$arm" "$probe" "$tgt" 11-example-restore "at $Q0" yes
  check "$arm" "$probe" "$tgt" 12-settle "at $Q0" no

  append "$ck" "$ts" $'\n// d231 probe edit\n'
  check "$arm" "$probe" "$tgt" 13-test-edit "at $Q0 +DIRTY" yes
  restore "$ck" "$ts"
  sleep 2
  check "$arm" "$probe" "$tgt" 14-test-restore "at $Q0" yes
  check "$arm" "$probe" "$tgt" 15-settle "at $Q0" no

  append "$ck" Cargo.toml $'\n# d231 probe edit\n'
  check "$arm" "$probe" "$tgt" 16-manifest-edit "at $Q0 +DIRTY" yes
  restore "$ck" Cargo.toml
  sleep 2
  check "$arm" "$probe" "$tgt" 17-manifest-restore "at $Q0" yes
  check "$arm" "$probe" "$tgt" 18-settle "at $Q0" no

  append "$ck" README.md $'\nd231 probe\n'
  g "$ck" add README.md || harness "git add README.md failed"
  check "$arm" "$probe" "$tgt" 19-index-only "at $Q0 +DIRTY" yes

  touch_moved "$ck/README.md" "$ck/src/lib.rs" "$idx"
  check "$arm" "$probe" "$tgt" 20-stale-stat "at $Q0 +DIRTY" yes
  check "$arm" "$probe" "$tgt" 21-no-self-rerun "at $Q0 +DIRTY" no

  # A one-link symbolic-ref chain: HEAD -> link -> the Q0 branch.
  link="refs/heads/d231-link-$arm"
  g "$ck" symbolic-ref "$link" "refs/heads/d231-probe-$arm-q0" || harness "could not create $link"
  g "$ck" symbolic-ref HEAD "$link" || harness "could not point HEAD at $link"
  check "$arm" "$probe" "$tgt" 22-chain-link "at $Q0 +DIRTY" yes
  lf=$(gpath "$ck" "$link") || harness "no path for $link"

  # Retarget the FIRST link: HEAD -> link -> $first (at S0). Neither HEAD nor the index moves.
  snap "$idx" "$hf" "$lf"
  g "$ck" symbolic-ref "$link" "$first" || harness "could not retarget $link"
  witness "no no yes"
  check "$arm" "$probe" "$tgt" 23-chain-retarget "at $S0 +DIRTY" yes

  # Move the SECOND link: $first itself, from S0 to Q0. Only $first's file moves.
  snap "$idx" "$hf" "$lf" "$ff"
  g "$ck" update-ref "$first" "$Q0FULL" || harness "git update-ref $first failed"
  witness "no no no yes"
  check "$arm" "$probe" "$tgt" 24-chain-second-link "at $Q0 +DIRTY" yes

  # A replacement for Q0 whose tree is the current index tree (Q0 plus the staged README), so a
  # git that honours replacements reads the tree clean. Its parent is S0, not Q0, so the
  # replacement cannot form a cycle. Both premises are asserted before the build: honoured it
  # reads clean, and with --no-replace-objects it does not.
  cp "$idx" "$WORK/index.copy" || harness "could not copy the index"
  xtree=$(GIT_INDEX_FILE="$WORK/index.copy" git -C "$ck" write-tree) || harness "git write-tree failed"
  xcommit=$(g "$ck" commit-tree "$xtree" -p "$SHA" -m "d231 probe: replacement for Q0") || harness "commit-tree for the replacement failed"
  g "$ck" replace "$Q0FULL" "$xcommit" || harness "git replace failed"
  st honoured "$ck"
  [ -z "$honoured" ] || harness "with the replacement honoured the tree does not read clean; step 25 would not discriminate"
  st unhonoured "$ck" --no-replace-objects
  [ -n "$unhonoured" ] || harness "without the replacement the tree reads clean; step 25 would not discriminate"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 25-replace "at $Q0 +DIRTY" yes
  g "$ck" replace -d "$Q0FULL" > /dev/null || harness "git replace -d failed"

  # 26 (N3): an UNRELATED packed ref deleted. packed-refs is rewritten and nothing HEAD resolves
  # through moves, so the stamp must NOT re-run: the reason build.rs does not watch packed-refs.
  pr="$(git -C "$ck" rev-parse --path-format=absolute --git-common-dir)/packed-refs" || harness "no common dir"
  victim=$(git -C "$ck" for-each-ref --format='%(refname)' refs/remotes/ | grep -v '/HEAD$' | head -1)
  [ -n "$victim" ] || harness "no remote-tracking ref to delete"
  grep -q " $victim\$" "$pr" || harness "$victim is not in packed-refs, so deleting it would not rewrite packed-refs"
  snap "$idx" "$hf" "$lf" "$ff" "$pr"
  g "$ck" update-ref -d "$victim" || harness "git update-ref -d $victim failed"
  witness "no no no no yes"
  check "$arm" "$probe" "$tgt" 26-unrelated-packed-ref "at $Q0 +DIRTY" no

  # 27: a three-link chain, HEAD -> link2 -> link -> $first.
  link2="refs/heads/d231-link2-$arm"
  g "$ck" symbolic-ref "$link2" "$link" || harness "could not create $link2"
  g "$ck" symbolic-ref HEAD "$link2" || harness "could not point HEAD at $link2"
  check "$arm" "$probe" "$tgt" 27-chain-three-links "at $Q0 +DIRTY" yes
  l2f=$(gpath "$ck" "$link2") || harness "no path for $link2"

  # 28 (N5): move the THIRD link, $first itself, back to S0. Only $first's file moves.
  snap "$idx" "$hf" "$l2f" "$lf" "$ff"
  g "$ck" update-ref "$first" "$SHA" || harness "git update-ref $first failed"
  witness "no no no no yes"
  check "$arm" "$probe" "$tgt" 28-third-link "at $S0 +DIRTY" yes

  # 29 (N4): `git status` fails while every other question build.rs asks still works
  # (PREREG amendment 2a: status.aheadBehind=bogus fails status, not ls-files or rev-parse).
  g "$ck" config status.aheadBehind bogus || harness "could not set status.aheadBehind"
  if git -C "$ck" --no-optional-locks status --porcelain --untracked-files=no > /dev/null 2>&1; then
    harness "git status still succeeds with status.aheadBehind=bogus; step 29 would not discriminate"
  fi
  git -C "$ck" --no-optional-locks ls-files -v > /dev/null 2>&1 \
    || harness "ls-files fails too with status.aheadBehind=bogus; step 29 would not isolate the status rule"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 29-status-fails "at unknown +DIRTY" yes
  g "$ck" config --unset status.aheadBehind || harness "could not unset status.aheadBehind"

  # 30: status works again, and the `unknown` does not stick.
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 30-status-recovers "at $S0 +DIRTY" yes

  # 31 (N2): an edit hidden from `git status` by the assume-unchanged bit.
  append "$ck" src/lib.rs $'\n// d231 probe edit (assume-unchanged)\n'
  g "$ck" update-index --assume-unchanged src/lib.rs || harness "git update-index --assume-unchanged failed"
  tag=$(git -C "$ck" --no-optional-locks ls-files -v -- src/lib.rs) || harness "git ls-files -v failed"
  case "$tag" in "h "*) ;; *) harness "src/lib.rs is not tagged assume-unchanged ('$tag')" ;; esac
  st_path hidden "$ck" src/lib.rs
  [ -z "$hidden" ] || harness "git status still lists src/lib.rs; step 31 would not discriminate"
  check "$arm" "$probe" "$tgt" 31-assume-unchanged "at unknown +DIRTY" yes
  g "$ck" update-index --no-assume-unchanged src/lib.rs || harness "git update-index --no-assume-unchanged failed"
  restore "$ck" src/lib.rs

  # 32 (N1): a `git archive` copy of S0 inside this checkout's gitignored target/. git discovery
  # walks up from the copy and finds THIS checkout, whose HEAD the copy was not built from.
  ex_dir="$ck/target/d231-export"
  mkdir -p "$ex_dir" || harness "could not create $ex_dir"
  git -C "$ck" archive "$SHA" | tar -x -C "$ex_dir" || harness "git archive into $ex_dir failed"
  [ -f "$ex_dir/build.rs" ] && [ ! -e "$ex_dir/.git" ] || harness "$ex_dir is not a bare copy of the sources"
  top=$(git -C "$ex_dir" rev-parse --show-toplevel) || harness "git finds no repository above $ex_dir"
  # Both spelled physically: git answers with the real path, and $WORK may sit under a symlink.
  [ "$(cd "$top" && pwd -P)" != "$(cd "$ex_dir" && pwd -P)" ] \
    || harness "$ex_dir is its own repository; step 32 would not discriminate"
  git -C "$ck" check-ignore -q "target/d231-export/build.rs" \
    || harness "the export is not in an ignored directory of $ck"
  probe_crate "$WORK/probe-$arm-export" "$ex_dir"
  OUTKEY="$arm-export"
  check "$arm" "$WORK/probe-$arm-export" "$tgt" 32-export-inside-checkout "at unknown +DIRTY" yes

  # 33-34: a clean tree at S0 again (HEAD -> link2 -> link -> $first), then a settle.
  g "$ck" reset -q --hard || harness "git reset --hard failed"
  st now "$ck"
  [ -z "$now" ] || harness "the tree is not clean after reset --hard"
  [ "$(sha12 "$ck" HEAD)" = "$S0" ] || harness "HEAD does not resolve to S0 before step 33"
  sleep 2
  check "$arm" "$probe" "$tgt" 33-reset-hard-2 "at $S0" yes
  check "$arm" "$probe" "$tgt" 34-settle "at $S0" no

  # 35-36 (U1): an untracked, not-ignored file among the inputs.
  printf 'fn main() {}\n' > "$ck/examples/d231_untracked.rs" || harness "could not create an untracked example"
  others=$(git -C "$ck" --no-optional-locks ls-files --others --exclude-standard -- examples) \
    || harness "git ls-files --others failed"
  [ "$others" = "examples/d231_untracked.rs" ] || harness "the untracked examples are not exactly the one planted ('$others')"
  check "$arm" "$probe" "$tgt" 35-untracked-input "at $S0 +DIRTY" yes
  rm "$ck/examples/d231_untracked.rs" || harness "could not remove the untracked example"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 36-untracked-removed "at $S0" yes

  # 37-38: an IGNORED .rs among the inputs, which cargo's target discovery compiles whatever git
  # says. The exclude file is copied first and put back after.
  excl=$(gpath "$ck" info/exclude) || harness "no info/exclude path"
  excl_had=no
  if [ -f "$excl" ]; then cp "$excl" "$WORK/exclude.$arm" || harness "could not copy $excl"; excl_had=yes; fi
  mkdir -p "$(dirname "$excl")" || exit 2
  printf '%s\n' "/tests/d231_ignored.rs" >> "$excl" || harness "could not write $excl"
  printf '#[test]\nfn d231_ignored() {}\n' > "$ck/tests/d231_ignored.rs" || harness "could not create the ignored test"
  git -C "$ck" check-ignore -q tests/d231_ignored.rs || harness "tests/d231_ignored.rs is not ignored"
  others=$(git -C "$ck" --no-optional-locks ls-files --others --exclude-standard) || harness "git ls-files --others failed"
  [ -z "$others" ] || harness "untracked, not-ignored files exist ('$others'); step 37 would not isolate the ignored-.rs rule"
  check "$arm" "$probe" "$tgt" 37-ignored-rs-input "at $S0 +DIRTY" yes
  rm "$ck/tests/d231_ignored.rs" || harness "could not remove the ignored test"
  if [ "$excl_had" = yes ]; then
    cp "$WORK/exclude.$arm" "$excl" || harness "could not put $excl back"
  else
    rm "$excl" || harness "could not remove $excl"
  fi
  git -C "$ck" check-ignore -q tests/d231_ignored.rs && harness "tests/d231_ignored.rs is still ignored"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 38-ignored-rs-removed "at $S0" yes

  # 39-40 (U2): a clean filter hides an edit from `git status`; the bytes still differ from HEAD.
  # The edit keeps the file's size, because git reads a size change as a modification without
  # asking the filter (PREREG amendment 3b, MEASURED on a toy repository with git 2.50.1: an
  # appended line stayed visible through a filter that deletes it, and a same-size edit that the
  # filter reverses read clean). So one word of a comment in src/lib.rs changes case, and the
  # filter changes it back.
  attrs=$(gpath "$ck" info/attributes) || harness "no info/attributes path"
  [ ! -e "$attrs" ] || harness "$attrs exists already"
  mkdir -p "$(dirname "$attrs")" || exit 2
  printf '%s\n' "src/lib.rs filter=d231" > "$attrs" || harness "could not write $attrs"
  g "$ck" config filter.d231.clean "sed 's/EVERY harness in/Every harness in/'" \
    || harness "could not configure the clean filter"
  python3 - "$ck/src/lib.rs" <<'PY' || harness "could not make the same-size edit to src/lib.rs"
import sys
p = sys.argv[1]
b = open(p, "rb").read()
if b.count(b"Every harness in") != 1 or b"EVERY harness in" in b:
    sys.exit(1)
open(p, "wb").write(b.replace(b"Every harness in", b"EVERY harness in"))
PY
  st_path hidden "$ck" src/lib.rs
  [ -z "$hidden" ] || harness "git status sees the filtered edit; step 39 would not discriminate"
  head_blob=$(git -C "$ck" rev-parse HEAD:src/lib.rs) || harness "no HEAD blob for src/lib.rs"
  disk_blob=$(git -C "$ck" hash-object --no-filters src/lib.rs) || harness "git hash-object failed"
  [ "$head_blob" != "$disk_blob" ] || harness "src/lib.rs's bytes equal HEAD's; step 39 would not discriminate"
  check "$arm" "$probe" "$tgt" 39-clean-filter "at $S0 +DIRTY" yes
  rm "$attrs" || harness "could not remove $attrs"
  g "$ck" config --unset filter.d231.clean || harness "could not unset the clean filter"
  restore "$ck" src/lib.rs
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 40-filter-removed "at $S0" yes

  # 41-42 (noS): an edit hidden from `git status` by the skip-worktree bit.
  append "$ck" src/lib.rs $'\n// d231 probe edit (skip-worktree)\n'
  g "$ck" update-index --skip-worktree src/lib.rs || harness "git update-index --skip-worktree failed"
  tag=$(git -C "$ck" --no-optional-locks ls-files -v -- src/lib.rs) || harness "git ls-files -v failed"
  case "$tag" in "S "*) ;; *) harness "src/lib.rs is not tagged skip-worktree ('$tag')" ;; esac
  st_path hidden "$ck" src/lib.rs
  [ -z "$hidden" ] || harness "git status still lists src/lib.rs; step 41 would not discriminate"
  check "$arm" "$probe" "$tgt" 41-skip-worktree "at unknown +DIRTY" yes
  g "$ck" update-index --no-skip-worktree src/lib.rs || harness "git update-index --no-skip-worktree failed"
  restore "$ck" src/lib.rs
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 42-skip-cleared "at $S0" yes

  # 43-44 (noignorestat): core.ignoreStat, with no entry marked, so only the config rule can see it.
  g "$ck" config core.ignoreStat true || harness "could not set core.ignoreStat"
  lv=$(git -C "$ck" --no-optional-locks ls-files -v) || harness "git ls-files -v failed"
  # A here-string, not a pipe: under pipefail an early `grep -q` exit can SIGPIPE the writer and
  # turn a match into a failed pipeline.
  grep -q '^[a-zS]' <<< "$lv" && harness "an index entry is already marked; step 43 would not isolate core.ignoreStat"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 43-ignorestat "at unknown +DIRTY" yes
  g "$ck" config --unset core.ignoreStat || harness "could not unset core.ignoreStat"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 44-ignorestat-cleared "at $S0" yes

  # 45-46 (lsfailopen): `git ls-files -v` fails while every other question works, the byte
  # comparison's own `ls-files --others` included (PREREG amendment 3a).
  sh45=$WORK/shim45-$arm
  read -r -d '' body45 <<'SH' || true
l=; v=
for a in "$@"; do case $a in ls-files) l=1 ;; -v) v=1 ;; esac; done
if [ "$l$v" = 11 ]; then : > "$SHIMDIR/fired"; echo "d231 shim: ls-files -v refused" >&2; exit 1; fi
SH
  shim "$sh45" "$body45"
  env PATH="$sh45:$PATH" git -C "$ck" --no-optional-locks ls-files -v > /dev/null 2>&1 \
    && harness "the step-45 shim does not refuse ls-files -v"
  env PATH="$sh45:$PATH" git -C "$ck" --no-optional-locks ls-files --others --exclude-standard > /dev/null 2>&1 \
    || harness "the step-45 shim refuses ls-files --others too; step 45 would not isolate the N2 rule"
  env PATH="$sh45:$PATH" git -C "$ck" --no-optional-locks status --porcelain --untracked-files=no > /dev/null 2>&1 \
    || harness "the step-45 shim breaks git status"
  rm "$sh45/fired" || harness "the step-45 shim left no mark when it refused"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 45-ls-files-fails "at unknown +DIRTY" yes PATH="$sh45:$PATH"
  # An observation, not a premise: the base never asks `ls-files -v`.
  say "D231 NOTE commit=$SHA12 arm=$arm step=45 shim_refused_ls_files_v=$([ -e "$sh45/fired" ] && echo yes || echo no)"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 46-ls-files-recovers "at $S0" yes

  # 47-48 (non6): the build script's first `git status` moves $first to Q0 before it answers, so
  # HEAD moves between the script's two reads of the sha.
  sh47=$WORK/shim47-$arm
  read -r -d '' body47 <<'SH' || true
for a in "$@"; do
  if [ "$a" = status ] && [ ! -e "$SHIMDIR/fired" ]; then
    "$REALGIT" -C "$D231_CK" update-ref "$D231_FIRST" "$D231_TO" 2> "$SHIMDIR/fired.err"
    echo "rc=$?" > "$SHIMDIR/fired"
    break
  fi
done
SH
  shim "$sh47" "$body47" D231_CK="$ck" D231_FIRST="$first" D231_TO="$Q0FULL"
  [ "$(sha12 "$ck" HEAD)" = "$S0" ] || harness "HEAD does not resolve to S0 before step 47"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 47-head-moves-during-status "at unknown +DIRTY" yes PATH="$sh47:$PATH"
  [ "$(cat "$sh47/fired" 2>/dev/null)" = "rc=0" ] || harness "the step-47 shim did not move $first during the build"
  [ "$(sha12 "$ck" HEAD)" = "$Q0" ] || harness "HEAD does not resolve to Q0 after step 47"
  check "$arm" "$probe" "$tgt" 48-head-moved-restamps "at $Q0" yes

  # 49-50 (existsonly): packing $first removes its loose ref file, a watched path; the next write
  # to $first creates it again. `--include $first`, not `--all` (PREREG amendment 3a): `--all` in
  # the clone arm would also pack the linked arm's branch, whose missing file would then re-run
  # the linked arm's script on every build and fail its settle steps at the tip.
  other_ff=$(gpath "$ck" refs/heads/d231-probe-linked) || harness "no path for the linked arm's branch"
  [ -f "$ff" ] || harness "$first has no loose ref file before pack-refs"
  if [ "$arm" = clone ]; then [ -f "$other_ff" ] || harness "the linked arm's branch has no loose ref file"; fi
  g "$ck" pack-refs --include "$first" || harness "git pack-refs --include $first failed"
  [ ! -e "$ff" ] || harness "$first's loose ref file survived pack-refs"
  if [ "$arm" = clone ]; then [ -f "$other_ff" ] || harness "pack-refs packed the linked arm's branch too"; fi
  [ "$(sha12 "$ck" HEAD)" = "$Q0" ] || harness "HEAD does not resolve to Q0 after pack-refs"
  check "$arm" "$probe" "$tgt" 49-packed-away "at $Q0" yes
  g "$ck" update-ref "$first" "$SHA" || harness "git update-ref $first failed"
  [ -f "$ff" ] || harness "update-ref did not create $first's loose ref file again"
  check "$arm" "$probe" "$tgt" 50-packed-branch-moves "at $S0" yes

  # 51-52 (R1): an untracked .cargo/config.toml at the package root. The probe builds from its own
  # directory, whose config search path does not include the checkout, so the build is the same;
  # only the stamp can tell.
  case "$probe/" in "$ck"/*) harness "the probe crate is inside the checkout" ;; esac
  [ ! -e "$ck/.cargo" ] || harness "$ck already has a .cargo"
  mkdir "$ck/.cargo" || harness "could not create $ck/.cargo"
  printf '# d231 probe: an untracked cargo config at the package root\n' > "$ck/.cargo/config.toml" \
    || harness "could not write $ck/.cargo/config.toml"
  others=$(git -C "$ck" --no-optional-locks ls-files --others --exclude-standard -- .cargo) \
    || harness "git ls-files --others failed"
  [ "$others" = ".cargo/config.toml" ] || harness ".cargo/config.toml is not the one untracked file under .cargo ('$others')"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 51-cargo-config "at $S0 +DIRTY" yes
  rm "$ck/.cargo/config.toml" || harness "could not remove $ck/.cargo/config.toml"
  rmdir "$ck/.cargo" || harness "could not remove $ck/.cargo"
  touch_moved "$idx"
  check "$arm" "$probe" "$tgt" 52-cargo-config-removed "at $S0" yes
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
