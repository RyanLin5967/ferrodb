# Certification — ac6adb8..3e71a2b (D170)

| gate | result |
|---|---|
| `verify-suite.sh` (per-target) | `rc=0 passed=2529 failed=0 build_errors=0 head=3e71a2b` |
| `verify-suite.sh` (go) | `rc=0 passed=97 failed=0` |
| `certify-head.sh` | `OK — suite head=3e71a2b == landing 3e71a2b` |
| `staleness.sh` | `0 behind, 524 ahead — COVERED` |
| `prepush.sh` | `OK` |

**+0 @ ac6adb8, absolute 2529 per-target** (MODE: per-target; `whole` reads 2531). Four files, all
`bench/*.txt`, 358 insertions / 4 deletions. **No source change, no `#[test]` added or removed.**

✅ Acquired at **`load_at_acquire=3`** — the quietest window of the night (earlier runs held at
20–26). Acquire, build, suite and all three gates ran as **ONE command with `trap … EXIT`**, so the
lock could not survive a turn boundary; it released on exit. That is the fix written after holding
an invented lock path for 3h08m, and this is its fourth clean use.

## What this range is: a finding that was DESTROYED, and the corrections that followed

D170 re-ran D71 because `d71_pushdown_fix.txt` said *"THE RE-RUN HAS NOT HAPPENED"*. My reading of
the result was wrong in four ways; an adversary killed each one and every retraction is in-place:

1. **Causal claim false** — not stub-vs-arena but **before-fix vs after-fix**. `e6958d6` landed
   **53 minutes** after the stub artifact, and **27 commits** touched `src/agent_sql/` between runs.
   Settled by a 2×2 in one binary: across a row the class changes (13–15× → ~1×), down the
   stub/arena column it does not (12.91 → 13.17; 1.17 → 1.02). The config costs a **~4× CONSTANT**.
2. **My control was invalid AND did not hold** — PLAIN cannot see a `branch_update` change (the
   D101 banner says so verbatim), and "0.34 vs 0.35" was the LAST ROW ONLY; at 1,000 rows it is
   4.032 vs 3.027 and the ratio moved 1.34× → 1.85×. **My 3.027 was the outlier.**
3. **I cited D167 INVERTED** — O(W) is its DEMOTED arm; the probeable state is its FLAT CONTROL.
   `SET v = ?` never moves the PK, so `unprobeable_rows == 0` and the overlay is **O(1)**, measured
   in the stub itself with a **fire-checked** detector (all three arms shown firing on demand).
4. **The headline was already banked** three days earlier in a file I had opened and read only the
   banner of.

## ⛔ Known imperfections in this range, recorded rather than hidden

- **Commit `67bdac3`'s MESSAGE asserts "the staged path is O(staged rows)"** — a claim `c510c30`
  retracts two commits later. History is not rewritten, so a `git log` scan shows the retracted
  claim followed by its retraction. Concrete instance of why findings belong where the next reader
  greps, not in a commit message.
- ⚠ **`bench/d170_d71_rerun_RAW.txt` retracts my ~70 ns/staged row in favour of ~35 ns. That
  retraction is ITSELF WRONG and is corrected in the NEXT range**, by D171
  (`frontier/d171_session_length_wall.md`): the term is **per-STATEMENT, not per-staged-row**, and
  the ~35 ns was low by exactly 2× (median of a session growing 0→N samples at ~N/2). **My 70 was
  closer, for a reason neither of us had given.** Landed as written because it was an honest record
  of what was known; superseded rather than silently edited.

## Instruments that must never land

`D170_NOPUSHDOWN`, `D170_STUB`, and five arm counters (`probe_fired`, `walk_nopk`,
`walk_unprobeable`, `max_unprobeable_rows`, `max_overlay_len`) live on branch `d170-adversary`.
**Verified absent from main at this commit**, all seven, with a positive control (`unprobeable_rows`
— the real field — returns 11 hits in `runtime.rs`). ⛔ `D170_NOPUSHDOWN` restores an O(table) scan
on all three agent WRITE paths and **no test can see it**: D170 measured that removing the pushdown
leaves the answer CORRECT and changes only the complexity.
