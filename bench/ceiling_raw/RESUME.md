# CEILING attack — resume state

Written before the long run, for a session that remembers nothing.

## Where the work is

* Measurement branch: `CEILING-resume`, worktree `/Users/idide/wt/ferrodb-CEILING-resume`.
  Off `CEILING-park-or-structure` (c00b26e). **MUST NEVER LAND** — it carries D123's
  `FERRODB_D123_STUB` capability escape in `src/branch/table_catalog.rs`.
* Quarantine of the paused session's uncommitted tree: branch
  `CEILING-QUARANTINE-uncommitted` (stash-create commit `540d5a5`). The old worktree
  `/Users/idide/wt/ferrodb-CEILING-park-or-structure` was **never modified**.
* Artifacts branch (no `src/`, no `examples/`): `CEILING-artifacts`, off current `main`.
  Verify with `git diff --name-only main...CEILING-artifacts -- src examples` → empty.

## Pre-registration

`/Users/idide/wt/artie-research/frontier/INVENTION-TRIGGER.md`, section
"IS THE 1.9× CEILING THE STRUCTURE, OR THE MUTEX?" plus Amendments 1 and 2.
Three outcomes, no fourth. "The counts cannot discriminate" is permitted and must be
labelled a result about the instrument.

## Done

1. `bench/ceiling_raw/00_…FAILED.txt` — the scaffold's own positive control refused
   (0.04688 against a ≥0.5 gate). Premise wrong, not instrument.
2. `01_model_inherited_PASSED.txt` — the paused session's version, re-run, passing.
3. `02_barrier_queuemax_63.txt` — my closed-form derivation of `queue_max` was wrong.
4. `03_model_closed_form.txt` — all three closed forms exact: 0.98438 / 32.50 / 64.
5. `10_paired_T64_N8000_warm0_reps5.txt` — the deliverable.

## Next action if this is a cold start

Read `10_…txt`'s CROSS-CONTROLS block first. If all eight ratios are inside 25% the
L0→L3 line is licensed; otherwise the answer is "instrument", not "hypothesis".
Then run the T=1 negative control and the pgwire layer, and write
`bench/ceiling_park_or_structure.txt`.

## Standing constraints

* Counts are the evidence. Durations are upper bounds, stamped with `load_at_acquire`.
* Every number names its LAYER: `TableBranchCatalog` (comparable to the recorded
  triple) vs pgwire (what production does). They are not the same experiment.
* `contended/op` UNDER-reports blocking under barging — measured, see the BARGE row in
  `03_model_closed_form.txt`. The comparator for "still contended" is the
  operating-point reference (b), not zero.
