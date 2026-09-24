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
