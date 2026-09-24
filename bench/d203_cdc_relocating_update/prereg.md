# D203 — a relocating UPDATE reaches the change feed as DELETE + INSERT: PRE-REGISTERED

Written before the tests and before the rule. Quiet mode: nothing here has been compiled or run;
every commit on `cdc-relocating-update` is labelled UNBUILT. Base `9aa6968`.

## The claim, checked from source first (READ-FROM-SOURCE @ `9aa6968`)

1. **The relocation arm.** `HeapFileManager::update` (`src/storage/heap_file_manager.rs`) tries
   `Page::update`, which fails with `NotEnoughSpace` only when the new tuple is longer than the old
   one AND longer than the page's contiguous free span (`src/storage/heap_page.rs`, `Page::update`).
   On that error it:
   - reserves a destination page (`find_or_make_page`, before touching the source);
   - deletes the slot and logs `txn.log_delete(.., &old_bytes)`, a `HeapDelete` of the slot's
     CURRENT bytes. For an UPDATE those are the live head, `end_ts = 0`;
   - calls `insert_into(dest, ..)`, which logs `txn.log_insert(..)`, a `HeapInsert` of the new tuple.

   Nothing is logged between the two for that transaction. The UPDATE executor archives the old
   version to the time-travel heap BEFORE calling `heap.update` (`src/execution/update.rs`: `tt_heap.insert`
   then `heap.update`), so that `HeapInsert` on the time-travel root comes first and is `internal`.
2. **The decoder.** `LogicalDecoder::decode` (`src/replication/logical.rs`) maps `HeapInsert` →
   `ChangeOp::Insert` and `HeapDelete` → `ChangeOp::Delete`, one record at a time, and nothing pairs
   them. **Claim confirmed:** a relocating UPDATE decodes as `DELETE {old}` + `INSERT {new}`.
3. **Who else logs a `HeapDelete`** (all of `git grep log_delete`):
   - the relocation arm above;
   - `HeapFileManager::delete`, whose only callers are in `#[cfg(test)]` modules (`wal/txn.rs`, `wal/recovery.rs`, `heap_file_manager.rs`);
   - the abort path's CLR, which is a `RecKind::Clr` wrapping one, not a `HeapDelete` record, and belongs to an aborted transaction the decoder discards anyway.

   A SQL DELETE is a `HeapUpdate` stamping `end_ts` (`src/execution/delete.rs` via `heap.update`,
   same length, so it never relocates). ⇒ **In a production log at `9aa6968`, a `HeapDelete` of a
   live image is always the first half of a relocating UPDATE.**

## The rule (implemented in the commit after the tests)

A `HeapDelete` of a **live** image (`end_ts == 0`) marks its transaction. If that transaction's very
next record (skipping only time-travel records, which are not changes) is a `HeapInsert` on the
**same table** whose **primary key** (column 0, which UPDATE may not assign) equals the deleted
row's, the two become ONE `ChangeOp::Update { old, new }`. It is stamped with the `HeapDelete`'s LSN,
where the change began. Anything else clears the mark and both records decode exactly as before:
- any other record of that transaction;
- an insert into another table, or of another key;
- a `HeapDelete` of a dead image.

"Next record of the same transaction", not "next record in the log": other transactions' records may
interleave in the physical log, and pairing is per transaction.

## Tests and their predicted outcomes

### `tests/d203_cdc_relocating_update.rs` (new target, compiles against `9aa6968`: only existing APIs)

| test | at the tests commit (no rule) | after the rule |
|---|---|---|
| `a_relocating_update_is_one_update_event` | **FAILS** at the kinds assertion: key 1 reads `["INSERT", "DELETE", "INSERT"]`, not `["INSERT", "UPDATE"]`. Its fixture assertion (`rid_of(1)` changed) PASSES first, which proves the row relocated | PASSES |
| `an_update_that_fits_in_place_is_one_update_event_and_stays_put` | PASSES | PASSES |
| `a_delete_and_an_insert_adjacent_in_one_transaction_stay_two_events` | PASSES | PASSES |

### Decoder unit tests (appended to `logical.rs`'s `mod tests`, hand-built logs, compile against `9aa6968`)

| test | no rule | rule |
|---|---|---|
| U1 `a_live_heap_delete_then_the_same_row_inserted_decodes_as_one_update` | **FAILS** (two events) | PASSES |
| U2 `a_live_heap_delete_then_another_key_inserted_is_not_paired` | PASSES | PASSES |
| U3 `a_live_heap_delete_then_an_insert_into_another_table_is_not_paired` | PASSES | PASSES |
| U4 `a_record_between_the_halves_breaks_the_pair` | PASSES | PASSES |
| U5 `a_heap_delete_of_a_dead_image_is_never_paired` | PASSES | PASSES |
| U6 `a_time_travel_record_between_the_halves_does_not_break_the_pair` | **FAILS** | PASSES |
| U7 `pairing_is_per_transaction_across_interleaved_records` | **FAILS** | PASSES |

U5 asserts only "no UPDATE, and the insert decodes as an INSERT of the new row". That way it holds both
at `9aa6968` (`DELETE` + `INSERT`) and after `delete-insert-lookup` `4296723` merges, where a dead-image
`HeapDelete` becomes `internal` and only the `INSERT` remains.

## Mutants (each applied alone to the rule commit, whole lib + the new target run)

| id | edit | must fail |
|---|---|---|
| M1 | the `HeapInsert` arm ignores the mark (rule removed) | `a_relocating_update_is_one_update_event`, U1, U6, U7 |
| M2 | the key comparison removed | U2 |
| M3 | the table comparison removed | U3 |
| M4 | the mark also set by a `HeapUpdate` that kills its row (a SQL DELETE) | `a_delete_and_an_insert_adjacent_in_one_transaction_stay_two_events` |
| M5 | the mark not cleared by the transaction's next record | U4 |
| M6 | the liveness condition removed (dead `HeapDelete`s mark too) | U5 |
| M7 | the mark taken BEFORE the time-travel skip, so a time-travel record clears it | U6 |
| M8 | one global mark instead of one per transaction | U7 |

## Counts

New target `d203_cdc_relocating_update`: **3 passed**. Lib: **+7** unit tests, all passing.
`integration_cdc_key_reuse` stays green (its reuse is a `HeapUpdate` DELETE then a `HeapInsert`, never a
live `HeapDelete`). No existing test is edited.
