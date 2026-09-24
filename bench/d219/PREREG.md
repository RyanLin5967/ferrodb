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

## A2 (appended after `1c226d8`, before any run): review 7's findings on this lane's own tests, lead-decided

**Source:** review 7 (`artie-research frontier/d219_review7.md` @ `80b108b`), SOUND-WITH-CAVEATS. **Decided by:** the lead, 2026-09-24. F1 was lead-verified. F2–F6 concern only this lane's own unlanded tests and texts, so each goes through this PREREG.

### F1: M23 survives the registered run, which says KILLED

Since A1, test 3's poisoned store is refused by `check_writable` in `plan_alters`, so `apply_plan`, the function M23 mutates, never runs. No other test makes a stamp refuse after a healthy probe.

**New test:** `catalog::alter::tests::a_store_poisoned_between_plan_and_apply_leaves_the_table_consistently_altered`.
1. Attribute all 41 rows.
2. Call `plan_alters` on a HEALTHY store; it must return the plan.
3. Poison the store (`fail_next_append`, then `stamp_row`).
4. Call `apply_plan`.

**Expected at the fix:**
- `Err` containing "refusing further writes";
- `schema.columns.len() == 3`;
- `SELECT *` returns 41 rows of 3 cells;
- the epoch moved;
- premise: some rid moved.

This is `33e3d67`'s expectation, which A1 replaced for a store already poisoned at the probe. It is still the correct expectation for a refusal AFTER the probe.

**Kills:**
- **M23** (a swallowed refusal returns `Ok` with every moved row unattributed): the test fails at `expect_err`.
- **M21** (stamps before `finish`: refused with 2 columns): the test fails at the column count, 2 vs 3.

`run.sh` names this test for M23, and adds it to M21's.

### F2: the page-dictionary cap is a post-install refusal no text listed

The texts argued that re-stamping cannot fill a page's 255-run dictionary because a page holds about 135 live tuples. But the dictionary grows over the page's whole HISTORY:
- `intern_local` only appends;
- a re-stamped slot keeps its old run's entry;
- slots are never reused;
- a freed page id keeps its dictionary.

A restamp onto such a page is refused after the install, with the DDL unlogged. That is the same residue as the I/O case.

**A1's residue list gains it:**
- (i) a sync that fails during the rewrite's flush;
- (ii) a restamp refused by a page dictionary at `MAX_PAGE_DICT_ENTRIES`;
- (iii) as before, rows that turn out not to move.

`apply_plan`'s comment, `rewrite_heap`'s doc and lane §6 are corrected to match. Scope, per the lead: stated, not made unrepresentable. For a plain ALTER, the stamps queued before such a refusal stay pending. D246 A4 (N-5) closes that half on its branch.

### F3: test 3's `next_lsn` assertion is dropped, with this reason

**The assertion being removed, quoted from `1c226d8`:**
> `assert_eq!(f.wal.next_lsn.load(Ordering::SeqCst), logged_before, "the refused ALTER wrote to the log")`

Neither `plan_alters` nor `apply_plan` appends to the WAL:
- the rewrite opens the heap with `txn: None`;
- `catalog/` has no WAL call;
- the DDL record is written only by the callers (`executor.rs`, `publish_evaluation_as`), and only on `Ok`.

A refusal returns `Err` at every layer, so no mutant of the ALTER can make this assertion fire, at this layer or at the executor's. It witnessed nothing. It is removed together with the fixture's `wal` field, which only it read. A1's "nothing logged" is restated as a property of the callers (they log only on `Ok`), not as something this test measures.

### F4: test 4's premise asserts TWO moved rows

M24 (eager stamps) costs one sync per moved row. With a single moved row, one sync per ALTER is indistinguishable from one per row. The premise changes from `any(moved)` to `count(moved) >= 2`. The claim is unchanged.

### F5: pre-existing, recorded as known and not fixed here

A MERGE on a poisoned store whose altered tables carry no attributed row skips the probe. It then applies, flushes and DDL-logs its schema, and refuses at the first publish stamp, returning `Err`. The same happened before D219, with the eager stamp. This is recorded in lane §6.

### F6: nits

- **`run.sh`, M21:** the list now leads with test 2 (`a_failed_flush_after_a_rewrite…`), which kills it. Test 3 no longer does, because the probe refuses first. The new F1 test is added.
- **`check_writable`'s doc** no longer says "exactly when every write would be". With a poisoned file mutex, every write path but `flush` panics on `lock().unwrap()`, and the probe refuses instead.
- **Test 3's message** "it failed, but not at the probe" becomes "it failed, but not by the poisoned store". The probe and a post-install stamp return the same text; the column and rid assertions are the ones that tell them apart. This changes a message, not a condition.

### Pre-registered outcomes, after A2

- `catalog::alter::tests`: **6 passed** at the new fix, one more than A1's 5.
- b7 (A1's red at `e60c5be`) is unchanged: that commit's test 3 still carries its `next_lsn` line and still fails at the column count.
- Mutants: every one except M4 and M11 is KILLED, M23 by the F1 test.
- Lib: +19 over `9aa6968`, not +18.
