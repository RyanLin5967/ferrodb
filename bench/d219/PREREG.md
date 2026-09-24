# D219 pre-registration — amendments

The original pre-registration lives in two places, both committed before any run:
- the headers of the tests it governs (`tests/d219_one_provenance_sync_per_merge.rs`, `src/catalog/alter.rs`'s `mod tests`);
- lane report §8 (`artie-research/frontier/lane_d219_provenance_sync.md`).

**This file only APPENDS to it.** An amendment never rewrites an earlier expectation; it quotes it, states the new one, and gives the reason.

## A1 — 2026-09-24T09:56:25Z — test 3 of `catalog::alter::tests`: the poisoned-store ALTER is refused, not installed

**Governs:** `catalog::alter::tests::a_poisoned_store_refusing_the_rewrites_stamps_leaves_the_table_consistently_altered`, added at `33e3d67`. It is this lane's own test and has never been on `main`: `9aa6968` has no `mod tests` in `src/catalog/alter.rs`.

**Decided by:** the lead, 2026-09-24. My ⚖ (lane report §6) proposed it. The lead ruled that it is not an owner decision, because the test is this lane's own and unlanded, so it changes through this PREREG, append-only.

**The expectation being replaced, quoted from `33e3d67`:** a plain `ALTER TABLE ... ADD COLUMN` of an attributed, packed table, on a durable provenance store poisoned by an earlier failed append,
> `expect_err("a poisoned store accepted the rewrite's stamps")`; the error contains `"refusing further writes"`; `schema.columns.len() == 3` ("a refused stamp left the rewritten rows under the OLD catalog: the I19 state"); `SELECT * FROM p` returns 41 rows of 3 cells; and some row's rid moved.

That is: **refused AFTER the install**, with the table consistently altered and the rows relocated.

**The new expectation:** the same ALTER is
- **refused** — `expect_err`, and the error contains `"refusing further writes"`;
- with the **DDL not installed** — `schema.columns.len() == 2`;
- with the **heap untouched** — every rid is where it was, and `SELECT * FROM p` returns 41 rows of 2 cells;
- and with **nothing logged** — the WAL's `next_lsn` is unchanged.

**Pre-registered outcomes:**
- **RED** at the commit that changes the test (on `fbfe038`'s code). The first failing assertion is `columns.len()`: 3 vs 2.
- **GREEN** once `plan_alters` probes the store.

**The reason.** At `fbfe038` the refusal comes after `finish`:
- the ALTER is installed and readable, and the call returns `Err`;
- both callers of `alter_table`/`apply_plan` log the DDL record only on `Ok` (`execution/executor.rs:376`, `agent_sql/runtime.rs` `publish_evaluation_as`);
- so the table's schema changed with no record in the log, and the change feed never hears of it (review 6, F1).

Refusing in `plan_alters` makes that state unrepresentable for a store that is already refusing writes:
- the plan phase asks the store (`ProvenanceStore::check_writable`) whenever the rewrite has an attributed row it may re-stamp;
- it refuses before the first heap byte moves, which is E82's "everything this merge can refuse, it refuses before the first byte is written".

**What this does NOT make unrepresentable, stated so the new expectation is not read as more:**
- **A sync that FAILS during the rewrite's own flush,** an I/O failure on a store that was healthy when planned, still returns `Err` after the install. That is `a_failed_flush_after_a_rewrite_leaves_the_table_consistently_altered`, whose expectation is unchanged. It is the same environmental class as the heap flush that follows `alter_table` in the executor.
- **A rewrite over attributed rows none of which move** is refused too on such a store: the plan cannot know which rows will move. That is the conservative direction, because the store is refusing every write anyway.

**The mutant that must kill it:** `M28_no_writable_probe` in `bench/d219/mutants.py` deletes the probe from `plan_alters`. With it deleted, this test must fail at `columns.len()` (3 vs 2), exactly as at the red commit.
