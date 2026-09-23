# Certification — d2fb9a4..d1ddb04 (D169 artifact corrections)

| gate | result |
|---|---|
| `verify-suite.sh` (per-target) | `rc=0 passed=2529 failed=0 build_errors=0 head=d1ddb04` |
| `verify-suite.sh` (go) | `rc=0 passed=97 failed=0` |
| `certify-head.sh` | `OK — suite head=d1ddb04 == landing d1ddb04` |
| `staleness.sh` | `0 behind, 513 ahead — COVERED` |
| `prepush.sh` | `OK` |

**+0 @ d2fb9a4, absolute 2529 per-target** (MODE: per-target; `whole` reads 2531). This range is
`bench/*.txt` only — no source change, no `#[test]` added or removed.

## ⛔ THIS RANGE AMENDS THE `d2fb9a4` CERTIFICATION, WHICH OVERCLAIMED COMPLETENESS

`bench/d169-2e62bce_certified/CERTIFICATION.md` says *"**five corrections** were applied before
landing"* and lists five. **The adversary listed SIX required edits.** Edit 5 was applied only in
part and **edit 6 was not applied at all** when that certification was written. Both landed
afterwards, in `66965bc` and `d1ddb04`:

- **Edit 5 (completed here).** The per-fsync cost was already re-measured directly (3.03 ms mean,
  n=200, flat 1→64 dirty pages), but the **floor/fraction distinction was missing**: 4 × 3.03 =
  12.1 ms is ~90% of a quiet-box 13.5 ms/branch and only **54%** of the adversary's load-20 run at
  22.3 ms/branch, with durations there not even monotone in work. **The floor and the integer are
  load-immune; the 90% is not** and must not be quoted without its load.
- **Edit 6 (applied here).** The novelty claim was **too broad**. *"Reap is fsync-bound"* is
  already asserted in this tree three times — `lease_thread.rs:484-486` (*"a reap is dominated by
  a durable catalog write — measured at ~58 ms each on this machine, fsync-bound"*),
  `lease_thread.rs:696`, and `bench/land-2655e3b_certified/SUMMARY.txt:28` (*"This box is
  fsync-bound — a reap costs 16-38 ms in every shape"*). All three model a reap as **a** durable
  write, singular; D169's contribution is that it is **four** for the leaf shape, **which** four,
  and that the count is shape-dependent. The sync COUNT itself is new (positive-controlled).
- ⭐ **Why edit 6 was missed: a Rule Zero failure with a mechanical cause.** My prior-art sweep
  printed `grep -c` **counts** (`fsync.*reap  src=1 bench=4`); I read the one `src/` match and none
  of the four in `bench/`. **A count is the most compressed possible list**, so the standing rule
  *read the matches, never the list* was violated by construction while appearing to be obeyed.
  Fix recorded in `frontier/INVENTION-TRIGGER.md`: prior-art greps print `-nE -B6`, never counts.

## Also in this range

**The artifact's TITLE contradicted its own body.** Line 1 still read *"THE REAL WALL IS AN
INTEGER: 4.00"* — the universal §3 retracts. Corrected to the shape-dependent statement, with the
old header quoted in place. A title outlives the paragraph that corrects it.

## CI, and the D73 question it settled

CI on `d2fb9a4`: **all three platforms success** (ubuntu, macOS, windows). This push was **held**
until that run completed rather than cancelling it — a push cancels an in-flight run in the same
group, and this was D169's only three-platform sample.

⭐ **D73 did NOT reproduce**, and the `av-loop-counters` instrument proved it rather than being
silent: `LOOPCOUNT hold_leader want=2 turns=0 bound=100000 ms=0..3`, `settle_to` turns 0–2 of
20000, and the test itself `... ok`. Compare the failure signature: **turns=100,000 / ms=272,725**.
⚠ **An earlier attempt to read this was a false clean and was refused**: `gh run view --job --log`
returns **0 bytes** while the parent run is in progress, even for a completed job, so every grep
returns 0. A positive control (`test result`, which must appear) also returned 0, which is
impossible for a green run — that is what caught it. See `frontier/FLEET.md`.
