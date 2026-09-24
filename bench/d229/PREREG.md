# D229 pre-registration: crash-safe frees

Written BEFORE any build or run. Nothing on this branch has been compiled or executed (quiet mode).
Every expectation below is INFERRED from source. Amendments are appended below the line at the end;
nothing above it is edited once committed.

Decision: SCALE-DESIGN "D229" (09:13Z), with the lead's later inputs (lane file `frontier/lane_d229.md`
§2): no log-names deferral of a DROP's frees (D250 owns that hazard), D243 beside D242 as not covered.

## Commits (branch `d229-crash-safe-frees`)

| sha | what |
|---|---|
| `ac405c0` | base the brief named: D208's resolution on #16 `7cede54` |
| `a6e93ab` | merge #16 `71efc3b` (named `d208-root-cell-per-index-on-71efc3b`), clean |
| `8b329f7` | **RED phase.** Red tests, the guards, and `open_recovered`'s cfg(test) storage seam (no behaviour change) |
| `cc4268d` | fix part 1 (does not compile alone) |
| `37bab60`, `449a7e9` | #16 `8d492bf` merged into the D208 resolution (`d208-root-cell-per-index-on-8d492bf`) and into D229, clean |
| `f18283e` | fix part 2: `txn.rs` intents, `recovery.rs` reset and open sequence; F2 split into two sweeps |
| `eb079f1` | the fix's own guards, each red only under its mutant |
| `e1264b5` | restores `discard_releases_on`, deleted by accident in `f18283e` |
| `f3d91d4` | `table_pages` unit test. **GREEN phase and mutant base: `src/` here is what this file registers** |
| this commit | this file. `src/` and `tests/` are identical to `f3d91d4` |

## RED at `8b329f7`

`cargo test --lib -- wal::recovery::tests_crash_frees::` gives **7 run, 5 FAILED, 2 passed**:

| test | at `8b329f7` | why (INFERRED) |
|---|---|---|
| `a_drop_crashed_before_its_truncate_then_an_open_crashed_in_its_rebuild_still_opens` (R1) | FAILED | the DROP freed before its checkpoint; open #2's rebuild puts nodes on `t`'s pages; open #3's redo of `t`'s records meets them |
| `a_drop_whose_frees_are_refused_leaves_no_page_reached_twice` (R2) | FAILED | `drop_table` frees the heaps, is refused at the pinned root, `t` stays named over free pages, `u` takes them; the next open's rebuild walks `t`'s tree into `u`'s heap |
| `a_crash_at_every_operation_of_a_rebuilding_open_leaves_two_good_opens` (F1) | FAILED | the rebuild frees by walking; after a crash the next walk meets a zero page ("invalid page type header") |
| `a_crash_at_every_operation_of_drop_table_leaves_the_table_whole_or_gone` (F2) | FAILED | frees before the persist; and (e): the shrinking persist frees the tail before page 1 is durable |
| `an_open_crashed_after_the_rebuilt_catalog_page_reached_disk_still_opens` (F3) | FAILED | page 1 names fresh roots over zero pages; the next rebuild walks them |
| `a_quiet_database_has_no_page_allocated_and_unnamed` (F5) | passed | a guard; its planted leak fires |
| `the_oracle_fires_on_a_planted_alias_a_planted_free_and_a_missing_key` | passed | a guard |

`cargo test --lib -- catalog::catalog::tests::a_table_named_on_two_catalog_pages_loads_from_the_earlier_page`: **FAILED** (`t` loads with 600, the later page's copy).
`cargo test --test d229_allocation_sites`: **2 passed** (a guard).

## GREEN at the tip

- `cargo build --tests --examples` with CI's `RUSTFLAGS="-D duplicate_macro_attributes -D dead_code"` succeeds.
- `wal::recovery::tests_crash_frees::` with `FERRODB_D229_STRIDE=1`: **12 run, 12 passed** (the 7 above, plus
  `a_crash_at_every_operation_of_drop_table_with_nothing_of_it_in_the_log_leaves_it_whole_or_gone`,
  `a_drop_whose_frees_failed_after_clearing_bits_lets_recovery_take_none_of_its_pages`,
  `the_reset_keeps_an_unnamed_page_that_holds_no_index`, `the_reset_keeps_a_page_it_cannot_read`,
  `the_reset_keeps_a_page_a_directory_lists_whose_bytes_are_zeros`).
- `wal::free_intent::tests`: 3 passed.
- `catalog::catalog::tests`: all pass **except** `dropping_a_table_returns_its_pages_including_the_time_travel_heap`,
  which asserts that `Catalog::drop_table` frees pages. The decided design removes that (D229 (a)). It is a ⚖ for Ryan,
  with the proposed rewrite in `frontier/lane_d229_catalog_test.proposed.patch` (not committed): take `table_pages`
  first, drop, then `free_pages`, as `drop_checkpointed` does. Its E69 property is kept at the SQL level by F5 and at
  the catalog level by `table_pages_names_both_heaps_and_every_tree`.
- `d229_allocation_sites`, `open_path_allowlist`, `lock_order_allowlist`, `d53_private_root_allowlist`: all pass,
  files untouched except the new `d229_allocation_sites`.
- Collateral (`lane_d229_run.sh` COLL): all pass except `integration_fulltext_retrieval::a_rebuild_over_a_re_used_primary_key_does_not_double_the_row`
  (D197's premise test, failing since `d7891d5`).
- `root_cell_is_per_index` T13 stays green but is **no longer discriminating**: `rebuild_indexes` frees nothing now, so
  both arms leak the same pages. Its property moved to the reset, which only `open_recovered` runs.

## Mutants (`frontier/lane_d229_run.sh`; each expression matches one site at `f3d91d4`, PATTERNS_ONLY)

| mutant | what | killed by (INFERRED) |
|---|---|---|
| M1 | the reset frees every unnamed page it can read (no identity filter) | `the_reset_keeps_an_unnamed_page_that_holds_no_index` |
| M2 | the reset ignores the keep set | `the_reset_keeps_a_page_a_directory_lists_whose_bytes_are_zeros` |
| M3 | a DROP frees before its checkpoint | the unlogged F2 sweep |
| M4 | deciding an intent releases its quarantine | `a_drop_whose_frees_failed_after_clearing_bits_...` and F2's crash points inside the frees |
| M5 | intents adopted after `recover` (A1 removed) | `a_drop_whose_frees_failed_after_clearing_bits_...` |
| M6 | a read error frees the candidate | `the_reset_keeps_a_page_it_cannot_read` |
| M7 | the reset does not keep pending intents' pages (A2 removed) | **predicted to SURVIVE**: the quarantine (A1) keeps the rebuild off those pages, and a second free of a clear bit is a no-op. A2 is redundant here; recorded as a finding, not a gap to paper over |
| M9 | `load` keeps the later copy of a duplicated entry | `a_table_named_on_two_catalog_pages_loads_from_the_earlier_page` |
| M10 | `persist` stops at the last page with entries | `test_persist_orphan_removal` (the 190 dropped entries come back) |
| M11 | a recovered open does not rebuild (only the marker triggers) | F1 and F3 |

Falsifiers. Any one withdraws the row; none adjusts a number.
- A RED test above passes at `8b329f7`: the mechanism stated for it is wrong.
- Any GREEN test fails at the tip, other than the ⚖ test and D197's.
- A mutant other than M7 survives its named test, or M7 is killed (then A2 is independent after all, and this file's
  reason is wrong).
- A sweep's `fired_at` premise fails at any point: the run is not deterministic, and the sweep aimed nowhere.

---
Amendments (append only):

**Amendment 1 (after `652d7d0`).** `652d7d0` adds D237's `in_transit` check to `BufferPoolManager::free_pages`
(a page being faulted in is refused, and `in_transit` is held to the end), so the one `free_pages` that survives
a merge with `d237-pin-leak` has both halves. **The GREEN phase and mutant base is `652d7d0`**, not `f3d91d4`;
every expectation above is unchanged, and each mutant expression still matches one site there (PATTERNS_ONLY).

**Amendment 2 (after `6b70bbf`).** `6b70bbf` adds `a_drop_whose_mutation_fails_after_naming_its_pages_frees_none_of_them`
(the lead's D250 lesson: a record written before an irreversible step is a commit point unless durable state decides it;
here the durable catalog does). GREEN at the tip is now **13 run, 13 passed** for `wal::recovery::tests_crash_frees::`.
New mutant **M12**: the intent is marked decided before the mutation's result is looked at; killed by that test (the
checkpoint after the failed DROP frees pages the catalog still names). **The GREEN phase and mutant base is `6b70bbf`.**
Everything else is unchanged; 11 expressions, one site each, at `6b70bbf` (PATTERNS_ONLY).

**Amendment 3 (after the D250 merge `4d37508`).** `4d37508` merges `d250-drop-logged` @ `7869d4f` (on #16 @ `2c10f17`).
D250 is the one mechanism for DROP under a retained log. It makes the `DropTable` record durable before the mutation,
has recovery skip a dropped heap's records, poisons the log when the mutation fails after the record, and has the next
open complete a logged DROP. It also retires #16's pin refusal, pin fence and owed-elsewhere refusal. Resolved as:
- `drop_checkpointed(record, pages, f)`. The executor collects `table_pages` before the barrier. The intent is recorded
  BEFORE the `DropTable` record, so every crash state that has the record also has the intent, and a DROP the next open
  completes frees its pages. That closes D250's stated leak. `DropPages` is gone.
- `open_recovered` completes logged DROPs (D250), then decides intents by that catalog (D229).
- The frees run once, right after the checkpoint's sync, whatever happens to the truncation. `forget_dropped_table` is
  `drop_table`. `record_free_intent` supersedes an undecided intent for the same table.

**Tests that assumed the parent's refusals: none of D229's.** R2's pin is a PAGE pin (`fetch_page`), not a WAL pin;
no test relied on the owed-elsewhere refusal. **Restated:** `a_drop_whose_mutation_fails_after_naming_its_pages_frees_none_of_them`
asserted the table survives a DROP whose mutation fails; under D250 the next open completes that DROP. It is now
`a_drop_whose_mutation_fails_frees_nothing_until_the_next_open_completes_it`. In the failing process it asserts: nothing
freed, the checkpoint refused (poisoned log), the intent present. After the next open it asserts: `t` absent, none of
its pages allocated unless reached, the intent gone. **R1 is now a joint D229 + D250 test:** the frees run before the
truncate, so at crash #1 `t`'s pages are already free while the log still names them, and opens #2 and #3 stay correct
because D250's recovery skips those records.

D250's two `drop_checkpointed` test call sites (`txn.rs` review-4 finding-4 test, `recovery.rs` failed-mutation test)
pass the pages argument; their assertions are unchanged.

**Mutants M3 and M12 are RETIRED as equivalent by construction.** M3 (free before the checkpoint): the `DropTable` record
is durable before the mutation, so a crash before the unlink is durable is completed by the next open, and the early
free aliases nothing. M12 (decide before the mutation's result): a failed mutation poisons the log, so no checkpoint,
and therefore no in-process free, can follow. D229 (a)'s free-after-durable-unlink ordering is now a second guard behind
D250's completion: recorded as a finding, not removed. `lane_d229_run.sh` carries 9 mutants (M1, M2, M4-M7, M9-M11), one
site each at `4d37508`. GREEN: `wal::recovery::tests_crash_frees::` **13 run, 13 passed**, predicted. **The GREEN phase
and mutant base is `4d37508`**; `src/` at this amendment's commit is identical to it.

**Amendment 4 (after the D250 re-merge `9cc779f`).** `9cc779f` merges `d250-drop-logged` @ `f3f75af`: D250 review 1's
fixes and #16 review 5 (`ed6e901`, via `cb00606`). The merge keeps the following:
- `CheckpointOutcome` and `KEPT_LOG_DROPS` (a pin-kept DROP is counted), beside `FREE_FAILURES`.
- A failed append or flush of the `DropTable` record poisons the log (D250 F1).
- The open's completion skips a DROP whose root was written after it (D250 F2).
- `OpenedDatabase.completed_drops`.

D229's order is unchanged: intent, then the record, then the mutation, with the frees right after the checkpoint's sync.
D250's new review-5 F3 test passes the pages argument (`Vec::new()`; assertions unchanged).
No D229 test changes, and every expectation in amendment 3 stands.
**The GREEN phase and mutant base is `9cc779f`**: 9 mutants, one site each there (PATTERNS_ONLY).

**Amendment 5 (the lead: an intent left for a table that was re-created at its root; registered BEFORE the test).**
The case: a DROP whose `DropTable` record stays in a kept log, a table re-created at the dropped heap's root with heap
records above the DROP's LSN (so D250 F2 keeps it out of the open's completion), and an intent for the old table still
pending at the next open. D229's replay must free none of the re-created table's pages.
- **Reachability, stated:** D229's own code cannot produce this state. The intent is removed durably (A4) before its
  pages leave the quarantine, so no table can take the dropped root while the intent is pending. The test therefore
  PLANTS the intent, as a lost removal would leave it, and pins that the replay decides by identity.
- **New test** `a_stale_intent_for_a_dropped_root_frees_nothing_of_the_table_re_created_there`. Setup: a fresh database;
  `r` created and filled; `DROP TABLE r` under a WAL pin (D250: the DROP runs, and the pin keeps its record in the
  log); `r` re-created, which takes the dropped root (asserted), and filled again; the old intent planted; the crash.
  The open must:
  - complete no DROP (`completed_drops` empty);
  - keep the re-created `r`, whose rows are found by scan and by key;
  - reach no page twice and none while free;
  - leave no intent file.
- **Predicted GREEN at the tip** (INFERRED): `decide_free_intents` finds a table at the intent's root, so it removes the
  intent durably and releases its quarantine, freeing nothing.
- **New mutant M13:** `decide_free_intents` treats every intent as decided (ignores the table's presence). Predicted RED
  on this test: the open's checkpoint frees the old table's pages, the re-created table's directory root among them,
  so a page is reached while its bit is clear.
- **Blind spot:** the identity is the heap's first directory page. An intent whose OTHER pages a new table took, at a
  different root, would be freed under it. The quarantine and A4 are what exclude that; no check at replay does.

**Amendment 6 (after `cb648be`).** `cb648be` adds amendment 5's test, unchanged from its registration. The GREEN phase
and mutant base is `cb648be`: `wal::recovery::tests_crash_frees::` **14 run, 14 passed**, predicted. `lane_d229_run.sh`
carries 10 mutants (M1, M2, M4-M7, M9-M11, M13), one site each at `cb648be` (PATTERNS_ONLY).

**Amendment 7 (the lead's 10:47Z schedule, re-stated at 11:0xZ; registered BEFORE its test).** The schedule:
1. `DROP t` records an intent;
2. a crash comes while that intent is still pending;
3. before the crash, `t` is re-created, in a CREATE whose checkpoint sync succeeded or failed;
4. D250's completion skips the DROP;
5. D229 then decides the intent against the catalog.

**Where the re-created table's first directory page lands:** it is NOT the dropped one, and cannot be. Every page of a
pending intent is quarantined from the DROP until the intent is durably gone (A4), and the dropped first directory page
is one of them; A1 re-establishes the quarantine before `recover`. So D229's decision ("a table's first directory page
is the intent's root, so the intent is dropped") answers "absent" here, and the intent is carried out. That is correct
ONLY because no page of the intent can belong to the new table. The identity check does not protect a table at another
root that holds intent pages; the quarantine does. The same-root case is reachable only by planting (amendment 5's test).

**New test** `an_intent_left_pending_by_a_crash_frees_nothing_of_the_table_re_created_after_its_drop`, two arms (the
CREATE's checkpoint sync succeeds / fails; failing, the table exists and the statement reports failure, A8's shape).
Setup, on a fresh database:
- `t` is created with rows (1, 10), (2, 20), (3, 30), and `u` with one row of one page.
- `DROP t` runs with the sync after its frees failing once, so the bits are clear, the intent pending and the pages
  quarantined.
- One freed page is then pinned, so every later retry of the batch is refused and the intent is still pending at the
  crash.
- `u` gains two one-page rows (two new heap pages).
- `t` is re-created, with rows (1, 100), (2, 200), (3, 300).
- The crash.

The open must:
- complete no DROP;
- leave every page the new `t` and `u` reference allocated;
- read row 2 back by key (`SELECT v FROM t WHERE id = 2` gives 200), and every row by scan and by key;
- reach no page twice and none while free;
- carry the intent out (no old `t` page left allocated and unnamed);
- remove the intent file.

Premise, asserted: the re-created `t`'s first directory page is not the dropped one.

**Predicted GREEN at the tip.** **New mutant M14:** `allocate` ignores the quarantine. `u`'s new pages and the new `t`
then take the old `t`'s pages (the lowest clear bits), the open frees them under their owners, and the test goes red
(a page reached while free). Base and counts follow in the amendment after the test lands.

**Amendment 8 (after `1ef22d1`).** `1ef22d1` adds amendment 7's test, as registered, with one addition: it also asserts
that the intent was carried out (none of the old `t`'s pages is left allocated and unnamed), which amendment 7 listed.
The GREEN phase and mutant base is `1ef22d1`: `wal::recovery::tests_crash_frees::` **15 run, 15 passed**, predicted.
`lane_d229_run.sh` carries 11 mutants (M1, M2, M4-M7, M9-M11, M13, M14), one site each at `1ef22d1` (PATTERNS_ONLY).

**Amendment 9 (D229 review 1, `frontier/d229_review1.md` @ `78627e0`; the lead's decisions; registered BEFORE the code).**
The lead numbered this "amendment 7"; 7 and 8 were already taken, so it is 9.

- **R1, the fix:** when the intent file is already gone (`NotFound`), `free_intent::store` still fsyncs the directory. That
  fsync is what makes an earlier remove durable, and the quarantine is released only after `store` returns `Ok` (A4).
- **R7, the seam:**
  - `free_intent::store` and the stale marker's fsync in `RebuildOwed::made_durable` go through a `&dyn FileOps`
    (`storage::atomic_file`). `FileOps` gains `remove`, with a default that calls `std::fs::remove_file`.
  - `TxnManager` holds the ops: `OsFileOps` by default, or a double in this crate's tests through the open's cfg(test)
    seam.
  - `txn::sync_file_and_directory` goes (its one caller moves to the ops).
  - The release-quarantine file (#16's mismatch record) is a separate writer. It is not D229's and is left alone.
- **New tests, each predicted GREEN at the tip, and each a mutant-only red:**
  - `a_retried_intent_removal_syncs_the_directory_before_the_quarantine_is_released` (R1). The ops fail the directory
    fsync that follows the intent's removal, once. The intent stays pending and its pages stay quarantined. The retry
    meets `NotFound` and must still fsync the directory before the release. Mutant **M17**: `NotFound` returns before the
    directory fsync (R1's defect).
  - `an_intent_whose_removal_fails_keeps_its_pages_out_of_use_until_it_is_gone` (A4's order). The ops fail the intent's
    removal once; a new table fills; the retry succeeds; then the crash. The reopen must find the new table whole, the
    dropped table's pages not leaked, and no page reached while free. Mutant **M16**: the quarantine is released before
    `store`.
  - `a_rebuilding_open_makes_its_trigger_durable_before_its_first_free` (caveat 1). A planted stale marker and a log
    that owes a rebuild. One event log, shared by the ops and by wrappers around the page file and the log, must show
    the log's fsync and the marker's file and directory fsyncs before the open's first bitmap write. Mutants **M18**
    (drop the marker's fsyncs) and **M19** (drop the log's `sync_file`).
  - `a_page_the_log_relinks_after_its_bit_was_lost_has_its_bit_set_again` (caveat 2, the rebit). A `SyncOnly` crash
    loses a new heap page's bitmap bit while its insert's record is durable. The open relinks the page, and the oracle
    requires every reached page to be allocated. Mutant **M15**: drop the `set_allocated` call.
- **Not made killable, and why:** the pre-free `disk_manager.sync()` in `free_pending_frees` stays EQUIVALENT. Every
  caller has just synced: the checkpoint, or an open that checkpoints because D250's re-appended `DropTable` record makes
  its log non-empty. This harness has no model in which a kill -9's unsynced writes are later lost to a power loss.
- **R4:** a correction appended to `frontier/d229_design.md` at §5.1 row 3, in its own commit (artie-research).
- **R5:** the unlogged F2 sweep's doc stops stating the retired A3 premise. It is a guard with no mutant of its own (M3,
  its named killer, was retired in amendment 3). Had M3 stood, its killer was R1's two-crash test (the review's reading).
- **R6:** `table_pages_names_both_heaps_and_every_tree` loses its vacuous `distinct.len() == pages.len()`. It gains:
  - a test-side raw walk into a `Vec`, which must hold no page twice and must equal `table_pages` as a set;
  - a planted alias (the primary root listed in the heap's directory) that `table_pages` must refuse. That second check
    is the one that can fail: a `collect_pages` that stopped refusing would pass the first.
