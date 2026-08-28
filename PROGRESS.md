# Phase F — per-lane working notes

Each section below is one agent's own scratch record, kept verbatim from its lane branch at the
point that lane was merged into `agent-isolation`. They are HISTORY, not status: a "next action"
here describes what that agent was about to do inside its own worktree, and every lane named in
this file has since been merged. The authority for current state is `LEDGER.md`.

---

# F3 — transport

**Done and committed.**
- `94b0e50` — codec for every `Body`/`Command`/`BranchOp` variant + `Entry`/`Config`/`SnapshotMeta`;
  resumable `FrameReader`; thread-per-peer `Transport`; `REPL_VERSION` 1→2 and `CONSENSUS_TAG`
  added to `src/replication/mod.rs`. 30 tests.
- `4c85d22` — fd leak (`try_clone` dups the descriptor; registry never drained), zero-duration
  validation, and the M19 test rewritten to isolate the tag check. 34 tests.
- Mutant batch 1: 19 fired, 18 killed, **M19 SURVIVED** (fixed in `4c85d22`), **M17 was a false
  kill** (did not compile). Log: `~/wt/logs/F3-mutants.txt`.

**Uncommitted in the tree right now (waves 2 and 3 of the adversarial review):**
11 further defects found by a fresh-context review of `4c85d22`, all verified against the code
before fixing. Most serious: `decode_config` was O(learners×members) — one 8 MiB frame carries 2M
node ids, ≈10^12 comparisons, a remote DoS. Also: orphaned threads on a failed spawn, unbounded
inbound mpsc, uncounted write-failure loss, unbounded inbound connections, no idle deadline, `dial`
ignoring `stop`, concurrent `shutdown` not a barrier, `send`-after-stop silently discarding,
`try_clone` busy-loop, and `as u16` truncation in the Catalog encode path.

**Single next action.** Read the result of `cargo test --lib consensus::transport` (log:
`~/wt/logs/F3-t.log`; wave-3 tests were appended after it started, so re-run). Then: commit, run
mutant batch 2 (`/private/tmp/.../mutants2.py`, M17/M20–M23) plus `m04_evidence.py`, then
`VERIFY_MODE=per-target tools/verify-suite.sh` for the full number, then write
`scratchpad/F3-transport.md` and `/Users/idide/wt/artie-research/build-F/F3-transport.md`.

**Machine note.** Six or seven sibling agents run cargo concurrently; builds take minutes, not
seconds. Judge a run by its output advancing, never by elapsed time.

---

# F5 — membership changes

**Done:** read DISTRIBUTED.md §F5, mod.rs (frozen), config.rs, election.rs, F1's tests and mutant
driver. Design written to `scratchpad/F5-design.md` and committed.

**Now:** adversarial review of the design (the precondition's counting set is the whole row), then
implement `src/consensus/membership.rs`.

**Next action:** write `src/consensus/membership.rs` — `Change`, `plan_change`, `begin_membership`,
`note_config_in_log`, `note_config_ack`, `apply_committed_config`.

---

# F8 — deterministic simulator

**Done**
- `src/consensus/sim.rs` + `src/consensus/tests_sim.rs`. 24 tests, all green. One line added to the
  frozen `mod.rs`: `pub mod sim;`.
- Safety: 100 000 seeds per property in release — 346 982 elections, 3 013 990 committed rounds,
  410 028 crashes, zero violations. Default `cargo test` sweeps 2 000 seeds per property.
- Every detector fired by a deliberate defect and then shown quiet. Figure 8 and the lease are
  scripted, because 400 seeds of chaos measurably never produce them.
- Four real defects found, three in the reference machine and ONE IN THE SIMULATOR ITSELF: a node
  that crashed with a vote still queued behind an unfinished fsync had the vote delivered anyway,
  which is a lying fsync and reported two leaders in a term against a correct protocol. Seed
  1592682576 is pinned as a regression test.

**Doing now**
- Adversarial review in a fresh context, then the summary.

**Next action**
- Run the review workflow over sim.rs + tests_sim.rs; fix what it confirms; write
  `/Users/idide/wt/artie-research/build-F/F8-sim.md`.

---

# F4 — cluster-ifying node-local state

## Done
- `src/cluster/mod.rs` (new): `Authority`, `GrantError`, `GrantedCounter`, the process lease clock,
  `ClusterScope`. One consume path (`take`), two authorities; standalone self-grants, a member
  refuses. 23 rule tests in `src/cluster/tests.rs`, all green.
- `src/branch/arena.rs`: `next_extent_start` + `next_arena_id` -> `GrantedCounter`s.
  Checkpoint image format byte-identical (the issued watermark occupies the old u32 slots).
  `apply_arena_grant` added. Recycle stack epoch-stamped.
- `src/wal/txn.rs`: `next_txn_id: AtomicU64` -> `txn_ids: GrantedCounter`.
  `apply_txn_id_grant`, `raise_next_txn_id`, `next_txn_id()` accessor.
- `src/branch/types.rs`: `LeaseDeadline` reads cluster time; `try_now_millis`/`try_from_now`
  added; the callerless `is_expired()` replaced by fallible `is_expired_now()`.
- 3 one-line call-site fixes outside my files: `wal/recovery.rs`, `execution/executor.rs` (test),
  `replication/snapshot.rs` (test).

- `tests/integration_cluster_grants.rs`: 26 tests, all green. Single-node preservation, the three
  refusals, wrong-node/duplicate/standalone grant guards, two-node disjointness for pages and txn
  ids, the replicated clock, authority-change revocation, restart, and the byte-identical image.
- One real bug the tests caught: `GrantedCounter::remaining()` counted stale-epoch ranges the
  guard would refuse. Fixed to filter by epoch.

- Two defects found by re-reading my own code, both fixed and pinned:
  (a) `accepted_through` was not reset on an authority change, so the old membership's high-water
      kept clamping the new leader's grants -> a node could refuse space it legitimately held.
      Reset floor is `issued`, never lower, because that is the record of what was handed out.
  (b) `reserve()`'s comment stated the stranding direction backwards. Fixed, and the invariant that
      makes the order safe (one grant carries `page_count` ids vs `page_count/extent_pages`
      extents) is now pinned by a test instead of assumed.
- FOURTH node-local decision found by sweeping `fetch_add` across src/, not named in the brief:
  `txn.rs` `commits_since_checkpoint` fires `wal.truncate` on a node-local counter, which is exactly
  the hole `Command::Checkpoint`'s own doc names. Guarded (automatic trigger withheld on a member,
  deferred not dropped) with `apply_checkpoint()` as the replicated entry point.
- 25 rule tests + 31 integration tests green.
- Mutation run 1 stopped early (fleet CPU starvation, ~2.5 min/mutant); 5/5 verdicts KILLED, banked
  in scratchpad/f4-mutants-partial-run1.txt. The kill DID leave a mutated file, caught and restored.

## Doing now
Re-running the mutation sweep against final code, grouped by mutation to halve the rebuilds.

## Next action
Run the grouped sweep, then tools/verify-suite.sh, then read the adversary, then the summary.

---

# E82 — refused schema edit leaves partial state

**Done**
- Reproduced the I19-atk4 finding (`0dee647`) on HEAD `d65cbcd`: 4 passed, 5 failed.
- `src/catalog/alter.rs`: `alter_table` split into `plan_alters` (decides a SEQUENCE,
  writes nothing) + `apply_plan` (writes, refuses nothing).
- `src/agent_sql/runtime.rs`: the merge is plan → measure → apply → publish, schema first.
- Three-lens adversarial review in a fresh context; every finding closed or stated.
- 9 detectors in `tests/integration_merge_ddl_atomicity.rs`; 10 mutants, all fired
  (`scratchpad/E82/mutants-final.txt`).

**Doing now**
- Final full-suite run on the finished tree.

**Next single action**
- Write the summary to `/Users/idide/wt/artie-research/build-F/E82-ddl-atomicity.md`.

---

# E79b — a prompt to hash

**Done (committed)**
- `BEGIN AGENT SESSION AS 'a' [RUN 'r'] [MODEL 'm/v'] [PROMPT 'text']`. `PROMPT` is a *soft* keyword
  matched by lexeme, so `prompt` stays a usable column/table name (the `ADMIT ALL` idiom).
- `AgentRuntime::begin_session_as(RunIdentity, parent)` is the single body; it hashes with
  `prompt_digest` into `RunEntity::prompt_hash`. No clause => `[0u8; 32]`, which is NOT
  `prompt_digest("")`. `begin_session` / `begin_session_with_model` delegate.
- Tests: 4 unit rules (scanner/parser/binder), `tests/integration_prompt_clause.rs` (8), the SQL-path
  feed test, and `ferro_runs_reports_the_digest_of_a_declared_prompt`.
- 8 mutants, all fired, tree clean after — `scratchpad/E79b-mutants.txt`.
- README/DEMO documented; corrected a README claim E79 had falsified.

**Doing now**
- Per-target suite run (`scratchpad/run_suite.py` -> `scratchpad/E79b-suite.txt`), 81 targets.

**Known instrument problem, NOT a regression**
- `integration_base_backup`, `integration_cdc_diff`, `integration_cdc_duckdb_sink` (and likely other
  cdc/repl targets) `Command::new(example_bin(...))` for `repl_primary` / `repl_replica` /
  `cdc_feed` / `table_dump`, plus external `duckdb` / `go` / `sqlite`. `cargo test --test X` does not
  build examples, so they fail for want of binaries. Re-run them after `cargo build --examples`.
- `integration_alter_column` produced NO result line; `integration_alter_refusal_safety` exited
  non-zero with 16 passed / 0 failed. Both still need a raw-output look and a base-commit comparison.

**Next action**
- After the suite: `cargo build --examples`, re-run every failing target, then compare the
  still-failing ones against base commit d65cbcd in a separate worktree before claiming anything.
