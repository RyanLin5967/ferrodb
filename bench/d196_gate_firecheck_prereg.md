# D196 — pre-registration, written BEFORE the runs it predicts

Subject: `tools/land-gate.sh` on branch `d196-gate-head`, base `main @ 9aa6968`. Amendments append only.

**The defect.** The gate resolves the commit being landed (`WANT_SHA`) and never asserts that the
checkout it runs in is that commit. Its step 3, `prepush.sh`, is the CI-parity build, and it builds
the checkout the gate's own file lives in: both scripts `cd` to their repository root, so the cwd is
irrelevant. Run from the main checkout, the gate asked steps 1 and 2 about the candidate and built
main in step 3, then printed one OK over the three answers.

**The fix.**
- **Step 0**, before the dirty-tree check and before every tool, refuses unless
  `git rev-parse HEAD^{commit}` equals `WANT_SHA`. The refusal names the tree examined, both full
  shas with their subjects, and the fix ("Run it from the candidate's worktree, using the copy of
  this script INSIDE it").
- **The closing block** asks the same question again after step 3. A clean checkout of another
  commit during step 3 leaves no dirt behind, so the existing dirty-after check cannot see it. This
  is the second way into the same state.
- Both compare shas (the resulting state), not the spelling of the call.

## Instrument

`bench/d196_gate_firecheck.sh <candidate> 9aa6968`, stdout captured to `bench/d196_gate_firecheck.txt`.
Every arm runs in a throwaway `git clone` under `$TMPDIR`. A fake `cargo` and `rustup` are first on
PATH: reaching either writes a sentinel and exits 97. The verify dir is empty. **No arm can build.**
Fixture commits exist only in the clone:

| name | what it is |
|---|---|
| C | the candidate (the D196 commit, real tools) |
| M | C plus one commit adding `D196_FIXTURE_MOVED`: "a checkout that is not the candidate" |
| S | C with `certify-head.sh`, `staleness.sh`, `prepush.sh` replaced by stubs. The stubs pass and print the HEAD a real prepush would build. `land-gate.sh` is byte-identical to C's |
| MS | S plus the same one-file commit |

The pre-D196 gate text (`9aa6968:tools/land-gate.sh`) is placed in the clone as the untracked file
`tools/land-gate.pre-d196.sh`, listed in the clone's `.git/info/exclude`. That file is the only
difference between the A1 and C1 states.

## Preconditions (they check the fixture, not the subject)

- C's gate text contains `this checkout is not the commit being landed` and `HEAD moved during this gate`; the pre-D196 text contains neither.
- M ≠ C. `S:tools/land-gate.sh` is the same blob as `C:tools/land-gate.sh`. C's `prepush.sh` is not a stub.
- **The no-build detector is forced to fire first.** Under the arms' PATH, `command -v cargo` is the
  fake. `timeout 10 cargo build --examples` exits 97 and leaves `FAKE cargo build --examples` in the
  sentinel. This runs in `$T`, which has no `Cargo.toml`, so even a failed interception builds nothing.
- Before each arm, `git status --porcelain` in the clone is empty.

## Arms, predicted before the first run

Every arm also asserts **the sentinel is absent**: nothing reached `cargo` or `rustup`.

| arm | gate text | HEAD | landing | predicted |
|---|---|---|---|---|
| **A1** (brief arm 1) | new | M | C (full sha) | **rc=1.** Has `land-gate: REFUSING — this checkout is not the commit being landed.`, `HEAD      <M>`, `landing   <C>`, `Run it from the candidate's worktree`. Lacks `[1/3]` and `land-gate: landing`, so it refuses before the summary lines and before any tool |
| **B1** (brief arm 2) | new | C | C (full sha) | **rc=1**, from certify-head, not step 0. Lacks the step-0 refusal. Has `HEAD is the commit being landed [step 0]`, `land-gate: [1/3] certify-head.sh`, `certify-head: REFUSING — no persisted suite summary`. Lacks `[2/3]` |
| B2 | new | C | C as a 7-char sha | as B1: step 0 passes. This rules out a comparison of the argument's text |
| B3 | new | C | the branch `d196-fc-candidate` at C | as B1 |
| **C1** (brief arm 3, anti-vacuity) | **pre-D196** | M | C | **rc=1**, from certify-head. Lacks the step-0 refusal. Has `land-gate: landing`, `land-gate: [1/3] certify-head.sh`, `certify-head: REFUSING — no persisted suite summary`. **The old text does not refuse at the point where A1 refuses** |
| D1 | new | S | S | **rc=0.** Has `[step 0]`, `STUB certify-head: OK`, `STUB prepush: would build HEAD=<S>`, `land-gate: OK — the evidence in`. Lacks `REFUSING`. The fix leaves the happy path open |
| D2 | **pre-D196** | MS | S | **rc=0**, the defect reproduced end to end: has `STUB certify-head: OK (dir=<V> want=<S>)`, **`STUB prepush: would build HEAD=<MS>`**, `land-gate: OK — the evidence in`. The old gate certifies S while its step 3 builds MS |
| D3 | new | MS | S | **rc=1**, the step-0 refusal. Lacks `STUB`, so no tool ran |
| E1 | new | S | S, and the stub prepush checks out MS | **rc=1.** Has `STUB prepush: moved HEAD to <MS>`, `land-gate: REFUSING — HEAD moved during this gate: <S> -> <MS>.`. Lacks `the tree became dirty`, which shows the new check fired and not the old dirty check. Lacks `land-gate: OK` |
| E2 | **pre-D196** | S | as E1 | **rc=0.** Has `STUB prepush: moved HEAD to <MS>` and `land-gate: OK — the evidence in`. This is anti-vacuity for the closing check |

The script refuses a PASS unless all **10** registered arms ran, at least one predicate was
evaluated, and none failed.

## What would falsify the fix, or the check itself

- A1 or D3 reaching `[1/3]`, or any tool output. Then the refusal is not the first step.
- B1, B2 or B3 printing the step-0 refusal. Then the equality is on text, not on shas, or HEAD is
  resolved in the wrong tree.
- C1 refusing at step 0's point, or D2/E2 **not** printing OK. Then the fixture does not reproduce
  the defect, and A1/D3/E1 prove nothing about the new text.
- E1 refusing with `the tree became dirty`. Then the stub's checkout left dirt, and the arm tests
  the old check, not the new one.
- Any sentinel. Then an arm reached a build, which is a defect in this instrument, and the run is void.

## What this fire-check CANNOT show, and what is queued for it (FAN-QUEUE #9)

The stubs replace the three real tools, so no arm here shows the REAL `prepush.sh` building the
candidate's checkout to OK. A gate change also needs a suite before it lands. Both build, so both
are queued in `artie-research/frontier/FAN-QUEUE.md` row 9. They are not run here:

1. **Suite at the D196 head**, per-target. Predicted **2579 passed, 0 failed**. That is main's
   measured count: `9aa6968`'s tree equals `9e76a4c`'s (`git rev-parse <c>^{tree}` gives
   `7a500e5` for both), and `9e76a4c` measured `mode=per-target rc=0 passed=2579 failed=0`
   (`~/wt/logs/lead-land-0924/verify-d187-9e76a4c/SUMMARY.txt`). The diff touches only `tools/`
   and `bench/` and adds no `#[test]`, so the change is +0. If main moves first, re-derive the count
   from main.
2. **The real gate, from the D196 worktree**, with that suite's verify dir. Predicted rc=0. The
   output has `HEAD is the commit being landed [step 0]`, then `[3/3] prepush.sh` with `PREPUSH: OK`,
   then `land-gate: OK`.
There is no "real gate from the MAIN checkout" arm, and it is left out on purpose. The gate
examines the checkout its own file lives in, so from the main checkout it runs **main's** copy of
the text, which is pre-D196 until this lands. Before the merge, that arm would test the old gate. A1
is the same state with the new text, in a clone. After the merge, the main checkout's gate refuses
any other candidate at step 0 without building. That can be checked then, for free.

---

## Amendment 1: four more arms, added after run 1 and before run 2

Run 1 (`bench/d196_gate_firecheck.txt`, committed `b4a5d48` before this amendment) went as predicted:
10/10 arms, 77 predicates, 0 failed. I then attacked it myself. Two properties were pinned by no
arm, although the brief or the refusal message claims them:

1. **Step 0 comes before the dirty-tree check.** Every run-1 arm starts from a clean tree. So a
   mutant that puts step 0 *after* the dirty check passes all ten. The order matters in practice.
   The 00:37Z d193 gate, run from main's checkout, refused with the dirty-tree message. That message
   points the operator at cleaning main's tree, which held another session's untracked `docs/`,
   when the real problem was the wrong checkout.
   - **A2**: new gate, HEAD = M, plus one untracked, NON-excluded file
     `docs/another-sessions-notes.md`, landing C. Predicted **rc=1** with the step-0 refusal and
     without `uncommitted change`. Precondition: the tree IS dirty.
   - **C2**: pre-D196 gate, the A2 state. Predicted **rc=1** with
     `the working tree has 1 uncommitted change(s)` and without the step-0 refusal. This reproduces
     the misdirection.
2. **"The gate examines the checkout its own file lives in, not your cwd."** The refusal says this,
   and it decides which instruction is right. No arm tested it, because every run-1 arm ran with
   cwd equal to the examined tree.
   - **A3**: cwd is a LINKED worktree of the clone, at C (the candidate). The gate is invoked by
     ABSOLUTE path to the main clone's copy, with the main clone at M, landing C. Predicted
     **rc=1** with the step-0 refusal. Its `checkout` line names the main clone, not the cwd.
   - **B4**: the production shape. The gate runs from a LINKED worktree at C, using that worktree's
     own copy, landing C. Predicted **rc=1** from certify-head. The `[step 0]` line names the
     linked worktree. Run 1 used only a standalone clone's primary worktree.

Registered arm count for run 2: **14**. Run 2 writes to `bench/d196_gate_firecheck_run2.txt`, and
run 1's file stays as it is.

---

## Results (appended after both runs; the raw files were committed first)

| run | candidate | gate blob (C / pre-D196) | arms | predicates | failed | raw |
|---|---|---|---|---|---|---|
| 1 | `37d964b` | `b528856` / `a618eb8` | 10/10 | 77 | 0 | `bench/d196_gate_firecheck.txt` (`b4a5d48`) |
| 2 | `ae5422c` | `b528856` / `a618eb8` | 14/14 | 100 | 0 | `bench/d196_gate_firecheck_run2.txt` (`2550b7d`) |

Both runs used the same gate text (blob `b528856`). Every arm came out as predicted: none printed an
unpredicted line, and none reached the fake `cargo`. The no-build detector fired on purpose before
the arms, with exit 97 and the sentinel written. I read the raw output arm by arm rather than the
PASS count:

- **The step-0 refusal fires, and fires first.** A1, A2, A3 and D3 refuse before any summary line.
  A2 does so on a dirty tree. **None of them built.**
- **It does not fire spuriously.** B1–B4 pass step 0 with the landing given as a full sha, a short
  sha, a branch name, and from a linked worktree. Each stops only at certify-head's empty verify dir.
- **The old text does not fire.** C1 reaches `[1/3]`. C2 sends the operator to clean the tree, which
  is the 00:37Z misdirection. D2 prints `OK` while its step 3 reports building `MS`, not the
  certified `S`. That is D196 reproduced end to end. E2 prints `OK` after HEAD moved mid-gate.
- **The closing check** fires on a clean checkout during step 3 (E1). The dirty-after check does not
  fire there, so E1 shows the new check fired and not the old one.
- **"Not your cwd" holds.** A3 ran from a checkout at the candidate and still refused, naming the
  tree the script lives in.

**Not shown here, and queued (FAN-QUEUE row 9):** the real `prepush.sh` building the candidate to OK,
and the suite. See "What this fire-check CANNOT show" above.
