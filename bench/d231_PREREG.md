# D231 PREREG: the build-stamp check (`bench/d231_stamp_check.sh`)

Written BEFORE any run of the steps it predicts: no real cargo run, and no run of the stub-cargo model either.
Amendments are appended below and never edited in place. Where an amendment and the script's header disagree, the
latest amendment here is the pre-registration.

History:
* `fd7f0fa` predicted 32 verdicts (tip 32 PASS; base 21 PASS / 11 FAIL).
* `ffbddc3` predicted 46 verdicts (tip 46 PASS; base 26 PASS / 20 FAIL).
* Both predictions live in their own script headers. Amendment 1 supersedes both, for the script at the commit that
  follows this file on `d231-build-stamp`.

## Amendment 1 (D231 review 3, F1-F10; lead decisions 2026-09-24)

### Steps, per arm (25), and what each must show at the tip

Q0 is a second commit with S0's exact tree. `foreign.git`'s HEAD is `<commit>^`. The arm's first branch
(`d231-probe` in the clone, `d231-probe-linked` in the linked worktree) is `$first`.

| n | change before the build | tip stamp | tip rerun |
|---|---|---|---|
| 1 | checkout, sleep 2 | at S0 | yes |
| 2 | nothing | at S0 | no |
| 3 | comment appended to `src/lib.rs` | at S0 +DIRTY | yes |
| 4 | `git add` + `git commit` of it | at S1 | yes |
| 5 | `git reset --soft HEAD~1`. Witness: only `$first`'s ref file moved | at S0 +DIRTY | yes |
| 6 | index touched; built with `GIT_DIR=foreign.git` | at S0 +DIRTY | yes |
| 7 | `git symbolic-ref HEAD` to a branch at Q0. Witness: only HEAD moved | at Q0 +DIRTY | yes |
| 8 | `git reset --hard`, sleep 2 | at Q0 | yes |
| 9 | nothing (settle) | at Q0 | no |
| 10 | comment appended to a tracked `examples/*.rs` | at Q0 +DIRTY | yes |
| 11 | `git checkout --` it, sleep 2 | at Q0 | yes |
| 12 | nothing (settle) | at Q0 | no |
| 13 | comment appended to a tracked `tests/*.rs` | at Q0 +DIRTY | yes |
| 14 | `git checkout --` it, sleep 2 | at Q0 | yes |
| 15 | nothing (settle) | at Q0 | no |
| 16 | comment appended to `Cargo.toml` | at Q0 +DIRTY | yes |
| 17 | `git checkout --` it, sleep 2 | at Q0 | yes |
| 18 | nothing (settle) | at Q0 | no |
| 19 | `README.md` (not an input) edited and `git add`ed | at Q0 +DIRTY | yes |
| 20 | `README.md`, `src/lib.rs` and the index touched, contents unchanged | at Q0 +DIRTY | yes |
| 21 | nothing | at Q0 +DIRTY | no |
| 22 | symref `refs/heads/d231-link-<arm>` → the Q0 branch; HEAD → the link | at Q0 +DIRTY | yes |
| 23 | link retargeted to `$first` (at S0). Witness: only the link's file moved | at S0 +DIRTY | yes |
| **24** | **NEW (F1).** `git update-ref $first Q0`: moves the chain's SECOND link. Witness: only `$first`'s file moved (index, HEAD and the link unchanged) | at Q0 +DIRTY | yes |
| **25** | **NEW (F2).** `git replace Q0 X`, then the index touched. X = a commit of the current index tree (Q0 plus the staged README), so git with replacements honoured reports the tree clean. `git replace -d` afterwards | at Q0 +DIRTY | yes |

A step passes only if the stamp, the rerun, and (in steps 5, 7, 23 and 24) the witness all match.

### Predictions

**At the D231 tip: 50 PASS / 0 FAIL, exit 0.** No step depends on timing, because the tip writes nothing it watches.

**At `9aa6968`: nominally 27 PASS / 23 FAIL, exit 1.**

| arm | PASS | FAIL |
|---|---|---|
| clone | 1 4 7 8 11 14 17 19 20 22 | 2 3 5 6 9 10 12 13 15 16 18 21 23 24 25 |
| linked | 1 3 4 5 7 8 10 11 13 14 16 17 19 20 22 23 24 | 2 6 9 12 15 18 21 25 |

Why each base FAIL happens:

* **clone 2, 9, 12, 15, 18:** a self-re-run. The base's plain `git status` writes back an index that the preceding
  git operation left racy. Cargo sets `output`'s mtime to the moment the script was invoked, so the write-back is
  newer (review 2, M3 and M4).
* **clone 3, 10, 13:** no re-run, stamp clean. Those inputs are not watched.
* **clone 5:** no re-run, stamp still S1. The branch ref is not watched.
* **clone 6 and linked 6:** the stamp takes foreign.git's HEAD, +DIRTY. `GIT_DIR` is honoured.
* **clone 16:** no re-run, stamp clean. This one is INFERRED; see the premise below.
* **clone 21:** step 20's plain `git status` wrote back the index with stale stat information.
* **clone 23, 24:** no re-run. Neither link's file is watched.
* **clone 25 and linked 25:** the base honours `git replace`, so it stamps `at Q0` with no `+DIRTY`.
* **linked 2, 9, 12, 15, 18, 21:** a re-run on every build. `.git/HEAD` and `.git/index` are missing paths in a
  linked worktree (Cargo FAQ, READ).

### Base steps that depend on timing, and the only flips allowed

* **clone 2, 9, 12, 15, 18** may go FAIL→PASS with `rerun=no`. That happens only if the preceding git operation's
  file writes and its index write straddled a second boundary, so that nothing was left racy.
* **clone 5, 23, 24** may go FAIL→PASS with `rerun=yes`. That happens only if the preceding builds took under about
  1 s, so that a chain of racy write-backs is still pending. Each preceding build recompiles a 129,598-line lib
  (review 3, MEASURED line count), so a flip is INFERRED improbable.
* **Every other base step must match the table exactly.** Any other deviation is a MISMATCH, to be reported and not
  explained away.
* So a base run gives **PASS between 27 and 35, FAIL between 15 and 23, and exit 1 in every case**, and only the named
  steps may differ, only in the named direction.
* The linked arm has no timing-dependent step: it re-runs on every build.

### Which step kills which mutant of `build.rs` (at the tip)

| mutant | killed by step |
|---|---|
| HEAD watch removed | 7, 22 |
| index watch removed | 6, 19, 25 |
| every chain link removed | 5, 23, 24 |
| only the first link watched (`links.len() < 1`) | 24 |
| a recursive `symbolic-ref -q HEAD` | 23 |
| `src/` removed | 3 |
| `examples/` removed | 10 |
| `tests/` removed | 13 |
| `Cargo.toml` removed | 16 (see the premise) |
| `INPUTS = ["src","benches","build.rs"]` | 10, 13, 16 |
| env clearing removed | 6 |
| `--no-optional-locks` removed | 2, 9, 12, 15, 18, 21 |
| `--no-replace-objects` removed | 25 |

### Not exercised by any step

* `Cargo.lock`, because cargo may rewrite a lock file it did not write.
* `benches/`, which is absent.
* The reftable directories. ferrodb uses the files store.
* The fallback for a git without `--no-recurse`. git 2.50.1 has it.
* The refusal when git does not answer one absolute path per question. git 2.50.1 always answers.
* The depth cap. The walk collects at most 5 links. git resolves a chain of 4 links and refuses 5 (review 3,
  MEASURED), and past that `rev-parse HEAD` fails, so the stamp is `unknown`.

### The shared INFERRED premise (F8)

The Cargo.toml row of the kill list and base clone step 16's FAIL rest on the same unmeasured fact: cargo does NOT
re-run a build script that emits `rerun-if-changed` when only a comment is appended to `Cargo.toml`. Base clone step
16 is the only measurement of it. If that step comes out **PASS with `rerun=yes`**, the premise is false, and the
Cargo.toml mutant is not shown killed at the tip either: the whole row is void, not only the base cell.

Base clone steps 10 and 13 play the same role for `examples/` and `tests/`. They stand on firmer ground, because the
Cargo book says a script that emits `rerun-if-changed` is re-run only for the paths it names (READ).

### The harness refuses (exit 2), never scores

The harness exits 2, and does not score the step, when:
* a build fails;
* the probe's stdout is not a stamp;
* cargo emits no `build-script-executed` message for ferrodb. The Cargo book says it is "emitted even if the build
  script is not run" (READ, `reference/external-tools.html`).
* the build script's `output` is missing before or after a build;
* a witnessed file is missing before or after its git operation;
* one of the harness's own edits did not take: an append that `git status` does not see, a restore it still sees, a
  `touch` that did not move the mtime;
* any git operation or sha lookup fails;
* the run reaches other than 50 verdicts.

Exit 0 requires FAIL = 0 and PASS = 50.

## Amendment 2 (D231 review 4, N1-N7 and H1-H2; lead decisions 2026-09-24)

Written before steps 26-32 exist and before any run of them, stub model included. It supersedes amendment 1's totals,
for the script at the commit that follows this amendment. Steps 1-25 and their predictions are unchanged. The
`build.rs` behaviour these steps pin is new at that same commit:

* **N1.** The stamp is `unknown` unless `git rev-parse --show-toplevel` is the package root.
* **N2.** The stamp is `unknown` when any index entry carries assume-unchanged (a lowercase `ls-files -v` tag) or
  skip-worktree (`S`), or when `core.ignoreStat` is true.
* **N4.** A failed `git status` stamps `unknown`. It used to keep the sha and mark it dirty.
* **N6.** The sha is read, then `status` runs, then the sha is read again, and the stamp is `unknown` if it moved.

### New steps, per arm (32 in all), and what each must show at the tip

The first link of the chain is `refs/heads/d231-link-<arm>` ("link"). Step 27 adds `refs/heads/d231-link2-<arm>`
("link2") in front of it.

| n | change before the build | tip stamp | tip rerun |
|---|---|---|---|
| 26 | an UNRELATED packed ref deleted (`update-ref -d` of a packed remote-tracking ref). This rewrites `packed-refs`, and nothing HEAD resolves through moves. Witness: only `packed-refs` moved; index, HEAD, link and `$first` did not (N3) | at Q0 +DIRTY | **no** |
| 27 | symref link2 → link; HEAD → link2. HEAD now resolves through three links: link2 → link → `$first` | at Q0 +DIRTY | yes |
| 28 | `git update-ref $first S0` moves the chain's THIRD link. Witness: only `$first`'s file moved; index, HEAD, link2 and link did not (N5) | at S0 +DIRTY | yes |
| 29 | index touched, then made unreadable (`chmod 000`), so `git status` fails. The mode is restored after the build (N4) | at unknown +DIRTY | yes |
| 30 | index touched, now readable again: the `unknown` does not stick | at S0 +DIRTY | yes |
| 31 | a comment appended to `src/lib.rs`, then `update-index --assume-unchanged src/lib.rs` (premise asserted: `ls-files -v` tags it `h`). `git status` no longer lists the edit. The bit is cleared and the file restored after the build (N2) | at unknown +DIRTY | yes |
| 32 | `git archive S0` extracted into the checkout's gitignored `target/d231-export/`, probe written, built there with the arm's target dir. git discovers the ENCLOSING checkout (N1) | at unknown +DIRTY | yes |

### Predictions

**At the D231 tip: 64 PASS / 0 FAIL, exit 0.** No step depends on timing.

**At `9aa6968`: nominally 32 PASS / 32 FAIL, exit 1.**

| arm | PASS | FAIL |
|---|---|---|
| clone | 1 4 7 8 11 14 17 19 20 22 27 30 | 2 3 5 6 9 10 12 13 15 16 18 21 23 24 25 26 28 29 31 32 |
| linked | 1 3 4 5 7 8 10 11 13 14 16 17 19 20 22 23 24 27 28 30 | 2 6 9 12 15 18 21 25 26 29 31 32 |

Why each NEW base step comes out as it does:

* **clone 26:** FAIL. There is no re-run, so the stamp is still step 25's clean `at Q0`, because the base honoured the
  replacement. If a re-run happened it would still FAIL, on `rerun=yes`.
* **linked 26:** FAIL on `rerun=yes`. The base re-runs on every build.
* **27:** PASS in both arms. HEAD moved, and the base watches HEAD.
* **clone 28:** FAIL. There is no re-run (`$first`'s file is not watched), so the stamp is still `at Q0`.
* **linked 28:** PASS.
* **29:** FAIL in both arms. The base keeps the sha and marks it dirty: `at S0 +DIRTY`.
* **30:** PASS in both arms. The index moved, and the base watches it.
* **31:** FAIL in both arms. The base's status skips the assume-unchanged entry; the staged README still makes it
  `at S0 +DIRTY`, not `unknown`.
* **32:** FAIL in both arms. The export's base build script stamps the enclosing checkout's HEAD, `at S0 +DIRTY`.

### Base steps that depend on timing, and the only flips allowed

Amendment 1's list stands:
* clone 2, 9, 12, 15 and 18 may go FAIL→PASS with `rerun=no`;
* clone 5, 23 and 24 may go FAIL→PASS with `rerun=yes`.

Amendment 2 adds one:
* **clone 28 may go FAIL→PASS with `rerun=yes`**, only if a racy write-back is still pending from sub-second
  preceding builds. That is the same mechanism as clone 5, 23 and 24.
* Steps 26, 27 and 29-32 come out the same either way at the base.

So a base run gives **PASS between 32 and 41, FAIL between 23 and 32, and exit 1**. Only the named steps may differ, and
only in the named direction. Any other deviation is a MISMATCH.

### Mutants these steps must kill (at the tip)

| mutant | killed by step |
|---|---|
| `packed-refs` watched as well | 26 |
| only the first two links watched (`links.len() < 2`) | 28 |
| a failed `git status` keeps the sha (the old behaviour), or reads clean | 29 |
| no `--show-toplevel` check (N1) | 32 |
| no index-bit check (N2) | 31 |

Amendment 1's kill map stands for steps 1-25.

### Added to "not exercised by any step"

* the skip-worktree (`S`) tag and `core.ignoreStat`. The code refuses both; step 31 exercises only assume-unchanged.
* the N6 re-read of the sha. No step can move HEAD inside one build script run.
* `core.preferSymlinkRefs` (N7, a known limit). With it, HEAD is a symlink and `--git-path HEAD` names its target's
  realpath, so a HEAD retarget moves no watched file. The option is deprecated and not set on this machine (review 4,
  MEASURED).

The exit condition becomes: exit 0 requires FAIL = 0 and PASS = 64, and a run that reaches anything other than 64
verdicts exits 2.

## Amendment 2a (before any run): how step 29 makes `git status` fail

Amendment 2 said step 29 makes the index unreadable. That would not isolate the N4 rule. An unreadable index also
fails the `ls-files -v` of the N2 check, and N2 answers `unknown` too, so the mutant "a failed status keeps the sha"
would pass step 29 through N2. That is two guards, one of which cannot be tested.

Step 29 instead sets `status.aheadBehind = bogus` in the clone's repository config, then touches the index to force a
re-run. That config value makes `git status` fail and leaves every other call build.rs makes working. MEASURED on git
2.50.1, with the value set:

| call | exit code |
|---|---|
| `git status --porcelain --untracked-files=no` | 128 |
| `git ls-files -v` | 0 |
| `git rev-parse --short=12 HEAD` | 0 |
| `git config --bool core.ignoreStat` | 1, the normal answer for an unset key |

The value is unset right after the build. Step 29's predictions are unchanged:
* the tip stamps `at unknown +DIRTY`, with rerun yes;
* the base FAILs with `at S0 +DIRTY`;
* the N4 mutant FAILs the same way.
