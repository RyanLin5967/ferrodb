#!/bin/bash
# D231 — does the build stamp compiled into a ferrodb binary follow the tree it was built from?
#
# FAN WORK: this builds ferrodb (twice from scratch, then incrementally). Queue it; never run it
# under quiet mode. Run it once per commit under test, the D231 base and the D231 tip:
#
#     bench/d231_stamp_check.sh 9aa6968        # expected RED: 26 PASS / 20 FAIL, exit 1
#     bench/d231_stamp_check.sh <d231 tip>     # expected GREEN: 46 PASS, exit 0
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
# steps 5, 7 and 23, the files git touched are exactly the ones the step exists to isolate. A stamp
# that comes out right without the script having re-run proves nothing about the watch the step
# exercises. Every build gets one line:
#   D231 commit=<sha12> arm=<clone|linked> step=<n-name> expect="<stamp>" got="<stamp>"
#        stamp_ok=<y|n> expect_rerun=<yes|no> rerun=<yes|no> [witness=...] lib_fresh=<true|false>
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
#
# STEPS, per arm. Q0 is a second commit with S0's exact tree; foreign.git's HEAD is <commit>^.
#   n  name               change before the build                     expect         rerun
#   1  fresh-build        (checkout, then sleep)                      at S0          yes
#   2  no-change          nothing                                     at S0          no
#   3  src-edit           a comment appended to src/lib.rs            at S0 +DIRTY   yes
#   4  commit             `git add` + `git commit` of that edit       at S1          yes
#   5  reset-soft         `git reset --soft HEAD~1`; witness: only
#                         the branch ref file moved                   at S0 +DIRTY   yes
#   6  foreign-git-dir    index touched; built with GIT_DIR=foreign   at S0 +DIRTY   yes
#   7  head-only          `git symbolic-ref HEAD` to a branch at Q0;
#                         witness: only HEAD moved                    at Q0 +DIRTY   yes
#   8  reset-hard         `git reset --hard`, sleep                   at Q0          yes
#   9  settle             nothing                                     at Q0          no
#   10 example-edit       a comment appended to a TRACKED examples/*.rs  at Q0 +DIRTY yes
#   11 example-restore    `git checkout --` it, sleep                 at Q0          yes
#   12 settle             nothing                                     at Q0          no
#   13 test-edit          a comment appended to a TRACKED tests/*.rs  at Q0 +DIRTY   yes
#   14 test-restore       `git checkout --` it, sleep                 at Q0          yes
#   15 settle             nothing                                     at Q0          no
#   16 manifest-edit      a comment appended to Cargo.toml            at Q0 +DIRTY   yes
#   17 manifest-restore   `git checkout --` it, sleep                 at Q0          yes
#   18 settle             nothing                                     at Q0          no
#   19 index-only         README.md (not an input) edited, `git add`  at Q0 +DIRTY   yes
#   20 stale-stat         README.md, src/lib.rs, the index touched    at Q0 +DIRTY   yes
#   21 no-self-rerun      nothing                                     at Q0 +DIRTY   no
#   22 chain-link         symref refs/heads/d231-link-<arm> -> the Q0
#                         branch; HEAD -> the link                     at Q0 +DIRTY   yes
#   23 chain-retarget     the link -> the arm's first branch (at S0);
#                         witness: only the link's file moved         at S0 +DIRTY   yes
#
# WHICH STEP KILLS WHICH MUTANT OF build.rs (each removal fails at least the step named):
#   HEAD watch 7, 22 · index watch 19 · branch ref file 5 · src/ 3 · examples/ 10 · tests/ 13 ·
#   Cargo.toml 16 · INPUTS = ["src","benches","build.rs"] 10, 13, 16 · env clearing 6 ·
#   --no-optional-locks 2 (a fresh checkout's index is racy), 9, 12, 15, 18, 21 ·
#   the `--no-recurse` walk (a recursive `symbolic-ref -q HEAD` instead) 23.
#   Watched and NOT exercised: Cargo.lock (cargo may rewrite a lock it did not write), benches/
#   (absent), the reftable directories (ferrodb uses the files store).
#
# PRE-REGISTERED, before any run: 2 arms x 23 steps = 46 verdicts.
#   AT THE D231 TIP: 46 PASS, exit 0. No step depends on timing (the tip writes nothing it watches).
#   AT 9aa6968: 26 PASS, 20 FAIL, exit 1. The base watches `.git/HEAD` and `.git/index` relative to
#   the package, and its `git status` is the plain one.
#     clone arm: PASS 1 4 7 8 11 14 17 19 20 22; FAIL 2 3 5 6 9 10 12 13 15 16 18 21 23
#       2, 9, 12, 15, 18  re-run from the preceding git operation's racy index being written back by
#                         the base's plain `git status` (review 2, M3+M4), NOT from a missing path.
#                         Timing: each passes only if that operation's file writes and its index
#                         write straddled a second boundary.
#       3                 no re-run: stamp clean for an edited tree            (the D231 headline)
#       5                 no re-run: stamp still S1                             (branch ref unwatched)
#       6                 re-runs and stamps foreign.git's HEAD, +DIRTY         (GIT_DIR honoured)
#       10, 13            no re-run, stamp clean                                (inputs unwatched)
#       16                no re-run, stamp clean. INFERRED: a comment in Cargo.toml changes no
#                         fingerprint cargo keeps
#       21                re-runs: step 20's plain `git status` wrote the stale-stat index back
#       23                no re-run: the link's file is not watched; stamp still Q0
#       11, 14, 17 PASS because `git checkout -- <file>` rewrites the index (MEASURED, git 2.50.1),
#       which the base watches.
#     linked arm: PASS 1 3 4 5 7 8 10 11 13 14 16 17 19 20 22 23; FAIL 2 6 9 12 15 18 21
#       2, 9, 12, 15, 18, 21  re-run on every build: `.git/HEAD` and `.git/index` are missing
#                             paths in a linked worktree (Cargo FAQ)
#       6                     stamps foreign.git's HEAD
#   The witnesses in steps 5, 7 and 23 hold at both commits: they are facts about git, not build.rs.
#
# EXIT: 0 only if FAIL is 0 AND PASS is exactly 42; 1 any FAIL; 2 the harness could not measure:
# a build failed, the probe printed something that is not a stamp, a sha did not resolve, an
# mtime, a witnessed file or the script's `output` could not be read, or not every step ran. A run
# that collected nothing has not passed, so 2 is never folded into 0 or 1.
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
EXPECTED_VERDICTS=46
# What build_provenance() prints: `at <sha, 12 or more hex|unknown>`, then ` +DIRTY (...)`.
STAMP_RE='^at ([0-9a-f]{12,}|unknown)( \+DIRTY .*)?$'
# Set by `witness`, consumed and cleared by the next `check`.
WITNESS_NOTE=""
WITNESS_OK=y

say() { echo "$*" | tee -a "$RESULT"; }
say "D231 RUN commit_under_test=$SHA12 script_from=$SELF repo=$REPO work=$WORK git=$(git --version | awk '{print $3}') cargo=$(cargo --version 2>/dev/null | awk '{print $2}')"

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
# calling shell rather than inside `$(...)`, so its `exit 2` ends the run. Callers that need the
# file to exist check that themselves: 0 compared with 0 reads as "unchanged".
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
  local of before json_line got after rerun fresh out_dir stamp_ok verdict wnote
  of=$(outfile_for "$arm")
  before=0
  if [ "$of" != "-" ]; then mt before "$of"; fi

  # `${1+"$@"}`, not `"$@"`: bash 3.2 (macOS /bin/bash) calls an empty "$@" unbound under set -u.
  if ! ( cd "$ck" && env CARGO_TARGET_DIR="$tgt" ${1+"$@"} timeout 3600 cargo build --offline --quiet \
        --example d231_stamp_probe --message-format=json > "$WORK/steps/$arm.$step.json" \
        2> "$WORK/steps/$arm.$step.err" ); then
    echo "D231 HARNESS: build failed, arm=$arm step=$step; see $WORK/steps/$arm.$step.err"
    tail -5 "$WORK/steps/$arm.$step.err"
    exit 2
  fi
  json_line=$(python3 "$PARSE" < "$WORK/steps/$arm.$step.json") || exit 2
  out_dir=${json_line% *}
  fresh=${json_line##* }
  if [ "$out_dir" != "-" ]; then
    of=$(dirname "$out_dir")/output
    echo "$of" > "$WORK/outfile.$arm"
  fi
  # stdout only: an exec error or a panic goes to stderr, and must not be read as a stamp.
  got=$("$tgt/debug/examples/d231_stamp_probe" | head -1)
  # Only a stamp is a measurement. Anything else would score as a FAIL, and a FAIL is what the
  # base run is expected to produce.
  if ! [[ $got =~ $STAMP_RE ]]; then
    echo "D231 HARNESS: the probe printed something that is not a stamp, arm=$arm step=$step: '$got'"; exit 2
  fi
  if [ "$of" = "-" ]; then echo "D231 HARNESS: cargo never said where the build script's output is, arm=$arm step=$step"; exit 2; fi
  mt after "$of"
  if [ "$after" = 0 ]; then echo "D231 HARNESS: the build script's output $of does not exist after a build, arm=$arm step=$step"; exit 2; fi
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

# witness <expected "index head ref" as yes/no> <index-before> <head-before> <ref-before>
#         <index-file> <head-file> <ref-file>
# Folded into the NEXT check's verdict: that step tests one watch only if exactly the expected
# files moved. A witnessed file that does not exist is a harness failure, never a "no".
witness() {
  local f i h r seen
  for f in "$5" "$6" "$7"; do
    [ -f "$f" ] || { echo "D231 HARNESS: witnessed file $f does not exist"; exit 2; }
  done
  mt i "$5"; mt h "$6"; mt r "$7"
  seen="$([ "$2" = "$i" ] && echo no || echo yes) $([ "$3" = "$h" ] && echo no || echo yes) $([ "$4" = "$r" ] && echo no || echo yes)"
  if [ "$seen" = "$1" ]; then WITNESS_OK=y; else WITNESS_OK=n; fi
  WITNESS_NOTE="witness_index/head/ref_touched=\"$seen\"(expect \"$1\")"
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
# Q0: a second commit with EXACTLY S0's tree. Steps 7-21 sit on it, so the stamp moves while every
# file, build.rs included, stays the one under test. (<commit>^ would not do: its build.rs is not
# necessarily the one being tested.)
Q0FULL=$(g "$CLONE" commit-tree "$SHA^{tree}" -p "$SHA" -m "d231 probe: S0's tree, another commit") || exit 2
S0=$(git -C "$CLONE" rev-parse --short=12 HEAD)
Q0=$(git -C "$CLONE" rev-parse --short=12 "$Q0FULL")
FOREIGN12=$(git --git-dir="$FOREIGN" rev-parse --short=12 HEAD)
if [ "$S0" = "$Q0" ] || [ "$S0" = "$FOREIGN12" ]; then
  echo "D231 HARNESS: S0=$S0 Q0=$Q0 foreign=$FOREIGN12; steps 6 and 7 need three different commits"; exit 2
fi
say "D231 SHAS S0=$S0 Q0=$Q0 foreign=$FOREIGN12"

for arm in clone linked; do
  if [ "$arm" = clone ]; then ck=$CLONE; else ck=$LINKED; fi
  tgt=$WORK/target-$arm
  probe_source "$ck"
  # The checkout kind this arm claims to be, asserted rather than assumed.
  if [ "$arm" = clone ] && [ ! -d "$ck/.git" ]; then echo "D231 HARNESS: $ck/.git is not a directory"; exit 2; fi
  if [ "$arm" = linked ] && [ ! -f "$ck/.git" ]; then echo "D231 HARNESS: $ck/.git is not a file"; exit 2; fi
  ex=$(git -C "$ck" ls-files 'examples/*.rs' | head -1)
  ts=$(git -C "$ck" ls-files 'tests/*.rs' | head -1)
  if [ -z "$ex" ] || [ -z "$ts" ]; then echo "D231 HARNESS: no tracked example or test file to edit"; exit 2; fi

  sleep 2   # after the checkout: see WHY SOME STEPS SLEEP FIRST
  check "$arm" "$ck" "$tgt" 1-fresh-build "at $S0" yes
  check "$arm" "$ck" "$tgt" 2-no-change "at $S0" no

  printf '\n// d231 probe edit\n' >> "$ck/src/lib.rs"
  check "$arm" "$ck" "$tgt" 3-src-edit "at $S0 +DIRTY" yes

  g "$ck" add src/lib.rs && g "$ck" commit -q -m "d231 probe commit" || exit 2
  S1=$(git -C "$ck" rev-parse --short=12 HEAD)
  check "$arm" "$ck" "$tgt" 4-commit "at $S1" yes

  idx=$(gpath "$ck" index); hf=$(gpath "$ck" HEAD); rf=$(gpath "$ck" "$(git -C "$ck" symbolic-ref -q HEAD)")
  mt i0 "$idx"; mt h0 "$hf"; mt r0 "$rf"
  g "$ck" reset -q --soft HEAD~1 || exit 2
  witness "no no yes" "$i0" "$h0" "$r0" "$idx" "$hf" "$rf"
  check "$arm" "$ck" "$tgt" 5-reset-soft "at $S0 +DIRTY" yes

  touch "$idx"
  check "$arm" "$ck" "$tgt" 6-foreign-git-dir "at $S0 +DIRTY" yes GIT_DIR="$FOREIGN"

  g "$ck" branch -q "d231-probe-$arm-q0" "$Q0FULL" || exit 2
  mt i0 "$idx"; mt h0 "$hf"; mt r0 "$rf"
  g "$ck" symbolic-ref HEAD "refs/heads/d231-probe-$arm-q0" || exit 2
  witness "no yes no" "$i0" "$h0" "$r0" "$idx" "$hf" "$rf"
  check "$arm" "$ck" "$tgt" 7-head-only "at $Q0 +DIRTY" yes

  g "$ck" reset -q --hard || exit 2
  sleep 2
  check "$arm" "$ck" "$tgt" 8-reset-hard "at $Q0" yes
  check "$arm" "$ck" "$tgt" 9-settle "at $Q0" no

  printf '\n// d231 probe edit\n' >> "$ck/$ex"
  check "$arm" "$ck" "$tgt" 10-example-edit "at $Q0 +DIRTY" yes
  g "$ck" checkout -q -- "$ex" || exit 2
  sleep 2
  check "$arm" "$ck" "$tgt" 11-example-restore "at $Q0" yes
  check "$arm" "$ck" "$tgt" 12-settle "at $Q0" no

  printf '\n// d231 probe edit\n' >> "$ck/$ts"
  check "$arm" "$ck" "$tgt" 13-test-edit "at $Q0 +DIRTY" yes
  g "$ck" checkout -q -- "$ts" || exit 2
  sleep 2
  check "$arm" "$ck" "$tgt" 14-test-restore "at $Q0" yes
  check "$arm" "$ck" "$tgt" 15-settle "at $Q0" no

  printf '\n# d231 probe edit\n' >> "$ck/Cargo.toml"
  check "$arm" "$ck" "$tgt" 16-manifest-edit "at $Q0 +DIRTY" yes
  g "$ck" checkout -q -- Cargo.toml || exit 2
  sleep 2
  check "$arm" "$ck" "$tgt" 17-manifest-restore "at $Q0" yes
  check "$arm" "$ck" "$tgt" 18-settle "at $Q0" no

  printf '\nd231 probe\n' >> "$ck/README.md"
  g "$ck" add README.md || exit 2
  check "$arm" "$ck" "$tgt" 19-index-only "at $Q0 +DIRTY" yes

  touch "$ck/README.md" "$ck/src/lib.rs" "$idx"
  check "$arm" "$ck" "$tgt" 20-stale-stat "at $Q0 +DIRTY" yes
  check "$arm" "$ck" "$tgt" 21-no-self-rerun "at $Q0 +DIRTY" no

  # A one-link symbolic-ref chain: HEAD -> link -> the Q0 branch. Retargeting the link changes
  # what HEAD resolves to and touches neither HEAD nor the index.
  link="refs/heads/d231-link-$arm"
  if [ "$arm" = clone ]; then first="refs/heads/d231-probe"; else first="refs/heads/d231-probe-linked"; fi
  g "$ck" symbolic-ref "$link" "refs/heads/d231-probe-$arm-q0" || exit 2
  g "$ck" symbolic-ref HEAD "$link" || exit 2
  check "$arm" "$ck" "$tgt" 22-chain-link "at $Q0 +DIRTY" yes
  lf=$(gpath "$ck" "$link")
  mt i0 "$idx"; mt h0 "$hf"; mt r0 "$lf"
  g "$ck" symbolic-ref "$link" "$first" || exit 2
  witness "no no yes" "$i0" "$h0" "$r0" "$idx" "$hf" "$lf"
  check "$arm" "$ck" "$tgt" 23-chain-retarget "at $S0 +DIRTY" yes
done

say "D231 SUMMARY commit=$SHA12 pass=$PASS fail=$FAIL verdicts=$((PASS + FAIL)) expected_verdicts=$EXPECTED_VERDICTS result=$RESULT"
if [ $((PASS + FAIL)) -ne "$EXPECTED_VERDICTS" ]; then
  echo "D231 HARNESS: $((PASS + FAIL)) verdicts reached, not $EXPECTED_VERDICTS"; exit 2
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
