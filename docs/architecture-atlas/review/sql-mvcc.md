# SQL, ordinary storage, MVCC, and buffer-pool evidence

Scope: current source at `fc9556a742f61884a3011ccc28e78f160a8d57fe`.
All behavior below is verified by tracing the named code unless explicitly marked **inference**.
This review ran no tests and made no implementation changes. Test names below are suggested verification targets, not claims of execution.

## Diagram 1: entry points and execution routes

Use these exact node/edge labels:

```text
CLI input ── scan tokens + parse ──┐
                                 ├─ Statement dispatcher
PostgreSQL wire protocol ─────────┘
  └─ Parse/Bind/Execute or simple query; parameter conversion

Statement dispatcher
  ├─ agent commands / branch-qualified reads / agent-session DML → AgentRuntime
  ├─ BEGIN / COMMIT / ROLLBACK → TxnManager
  ├─ DDL → Catalog + checkpoint fence
  └─ ordinary SQL → SELECT plan or DML operator

SELECT → Binder → Logical plan → Predicate pushdown → Cost-based optimizer
       → Physical plan → Iterator executors → Rows
UPDATE / DELETE → bind expressions + predicate → same optimizer for row selection
                → drain selected rows → modification operator
INSERT → bind values → constraint checks → insertion operator
```

- CLI: `src/cli/cli.rs:228` scans/parses, locks catalog per statement, calls `run`.
- pgwire: `src/pgwire/extended.rs:216` normalizes transaction spellings/session commands, parses and infers parameter types; `:343` substitutes parameters and executes.
- Dispatcher: `src/execution/executor.rs:169`; intercepts system views, then agent statements, then agent DML, then ordinary transaction/DDL/DML cases.
- Ordinary SELECT: `src/planner/plan.rs:11`; **not every statement passes through a logical/physical plan**.
- DML row selection: `src/planner/plan.rs:183`; same optimizer as SELECT, including index selection.
- Operators implement `next() → (RecordId, values)` or `execute() → affected count`: `src/execution/executor.rs:26`.
- Optimizer chooses sequential/index scan, hash/nested-loop join, and inner-join order; LEFT joins retain their ordering boundary: `src/optimizer/optimizer.rs:5`, `:99`, `:240`.
- RIGHT/FULL joins are explicitly unsupported here (`optimizer.rs:23`); do not diagram all SQL features as implemented.

## Diagram 2: ordinary table structure and read routes

```text
SQL catalog: TableEntry
  ├─ schema
  ├─ first_directory_page_id → linked directory pages → heap pages → tuple slots
  ├─ time_travel_root        → separate old-version heap
  ├─ primary_index_root     → ordinary B+ tree: primary key → RecordId(page, slot)
  └─ secondary index roots  → ordinary B+ trees: (column value, primary key) → unit

Primary index lookup → RecordId → heap head tuple → MVCC visibility resolution → values
Secondary lookup → primary key → primary index → RecordId → heap → MVCC
                 → recheck visible column value against secondary entry → values
Sequential scan → directory pages → heap slots → MVCC → values
```

- SQL catalog root fields: `src/catalog/catalog_page.rs:19`; record IDs: `src/storage/heap_file_manager.rs:8`.
- Ordinary table managers share the live index root cell (`Arc<AtomicU32>`) so all statements see root splits: `src/planner/plan.rs:107`.
- Secondary mapping and final value recheck: `src/execution/sec_index_scan.rs:85`.
- A secondary index is **not column value → physical row address**; the primary-key hop matters.
- Primary key is column 0 in this implementation; direct UPDATE of that key is refused (`src/execution/update.rs:53`).
- Old secondary entries are retained when indexed values change, because older readers may need them; visible-value recheck rejects stale entries (`update.rs:128`).

## Diagram 3: actual ordinary page layouts

All pages are 4096 bytes; on disk, page ID `p` identifies offset `p * 4096`, not a memory address.
Source: `src/storage/disk_manager.rs:7`, `:142`, `:162`.

| Page | Header | Payload |
|---|---|---|
| Heap | 23 bytes: type, own ID, slot count, free-space boundaries, LSN, checksum field | 4-byte slots `(offset,length)` grow forward; serialized tuples grow backward |
| Ordinary B+ internal | 19 bytes: type, own ID, LSN, checksum field, key count | separator keys followed by `key_count+1` child page IDs |
| Ordinary B+ leaf | 27 bytes: internal fields plus `next` and `prev` leaf IDs | keys followed by values; PK values are RecordIds |
| Heap directory | 11 bytes: type, own ID, next-directory ID, entry count | `(heap_page_id, free_space)` entries, 6 bytes each |

Sources: `src/storage/heap_page.rs:10`, `:51`; `src/storage/index_page.rs:3`, `:42`, `:92`; `src/storage/page_directory.rs:4`, `:71`.

- Root is a role: a small tree's root can be a leaf. An internal root uses the same format as other internal nodes.
- Ordinary internal child IDs are in the **payload**; ordinary leaf sibling IDs are in the **header**.
- Heap directory chain is a linked list; ordinary index is a tree with an additional linked leaf level; row history is a version chain.
- The checksum fields in these ordinary structs should not be described as universally validated checksums; this trace verifies encoding, not comprehensive validation.
- **Do not reuse this layout for COW trees.** COW pages have a separate 24-byte ownership/epoch header and slotted payload; no leaf sibling links (`src/cow/node.rs:1`).

## Diagram 4: MVCC versions and snapshots

```text
Primary index → current heap tuple
                  header: begin_txn | end_txn | previous(page,slot)
                                              │
                                              ▼
                                     old-version heap tuple → older tuple → ...

ReadView = own transaction ID + Snapshot(high_water, active IDs)
  → creator visible AND ending transaction not visible?
       yes → return this tuple
       no  → follow previous version; none means no visible row
```

- Row version header is 24 bytes: 8-byte begin, 8-byte end, 4-byte previous page, 2-byte previous slot, 2 reserved bytes; then null bitmap and encoded values (`src/storage/tuple.rs:9`, `:22`).
- `begin_ts`/`end_ts` names hold transaction IDs, **not wall-clock times**. `end_ts=0` means not ended.
- Snapshot visibility: `id < high_water && id not in active`, plus own transaction ID; `src/wal/txn.rs:121`, `:137`, `:1113`.
- `BEGIN` atomically allocates/registers an ID while holding the active transaction table lock and stores its snapshot (`txn.rs:237`).
- Explicit transactions reuse the BEGIN snapshot; autocommit SELECT obtains the current committed snapshot (`executor.rs:115`, `:424`).
- Visibility resolution follows old versions in the time-travel heap (`src/wal/visibility.rs:3`).
- Write conflict check rejects a head created/ended by a transaction outside this writer's view (`visibility.rs:17`); this includes concurrent/unseen changes.
- Snapshot visibility and same-row conflict detection do **not** establish full serializability or prevent every write-skew pattern.
- Agent SQL read semantics must be explained separately by the branch reviewer; do not give all agent branches this BEGIN-time SQL snapshot.

## Diagram 5: ordinary writes

```text
INSERT: validate → serialize(begin=my_txn) → WAL-backed heap insert → PK upsert → secondary entries
UPDATE: select+materialize rows → head conflict check → copy old version into time-travel heap
        → new head(begin=my_txn, previous=old version) → heap update → maintain index mappings
DELETE: select+materialize rows → head conflict check → set head.end=my_txn → heap update
```

- INSERT rejects live/uncommitted duplicate keys; a deletion visible to the caller permits key reuse (`src/execution/insert.rs:85`).
- Reuse uses one primary-index upsert, avoiding a delete/insert gap (`insert.rs:146`; `src/storage/index.rs:234`).
- UPDATE records `old.end=my_txn`, inserts that old copy in the time-travel heap, and writes a new head with a previous pointer (`src/execution/update.rs:73`).
- If a larger tuple cannot fit, heap update relocates it and primary index is upserted to its new RecordId (`src/storage/heap_file_manager.rs:172`, `update.rs:100`).
- UPDATE and DELETE drain their child scan before mutating: avoids revisiting/skipping rows when an index changes (Halloween problem).
- SQL DELETE is a version-ending heap update, **not physical heap removal** (`src/execution/delete.rs:47`).
- Ordinary DML outside BEGIN gets an implicit transaction; success commits it, failure aborts; a DML error in an explicit transaction also aborts that transaction (`executor.rs:442`).
- Existing index entries/old versions accumulate. Ordinary B+ deletion has no rebalance; branch page reclamation is not MVCC vacuum (`src/storage/index.rs:829`; `tests/integration_index_debt.rs:158`).
- **Inference / unverified edge:** key reuse replaces the index pointer with a fresh row lacking a link to the deleted predecessor (`insert.rs:146`). Do not promise complete repeatable-read correctness for every reuse/index combination merely from the simple snapshot test; this review did not test that edge.

## Diagram 6: cached pages, synchronization, and durable storage

```text
Page request
  ├─ cached → verify frame label → pin → read/write under frame lock → unpin
  └─ cache miss → claim in-transit page → select unpinned victim → flush dirty victim
                  → disk read → publish frame and page-table mapping

Heap modification → append WAL record → store its LSN in heap page → mark frame dirty
Dirty-page flush/eviction → WAL flush through page LSN → disk page write
COMMIT → append commit record → flush WAL through commit → remove transaction from active set
```

- Pins prevent frame eviction; frame locks protect bytes; page latches protect compound ordinary-index operations. These are distinct jobs.
- Buffer pool has ARC replacement, deferred batched touches, page-table hints, per-frame locks and in-transit coordination; `src/buffer/buffer_pool.rs:747`.
- Cached hint must match the actual frame's page ID before pinning (`buffer_pool.rs:861`). A stale hint cannot name different bytes.
- Concurrent fetches of one absent page wait on the same in-transit marker; unrelated disk reads aren't held behind one global I/O lock (`buffer_pool.rs:776`).
- Ordinary index point lookup first tries version-validated, copied atomic shadow bytes, with bounded restart and latched fallback (`src/storage/index.rs:299`; `buffer_pool.rs:714`).
- These **buffer shadows are a concurrency optimization**, not branch shadow paging and not an MVCC version.
- Latched tree descent acquires child before releasing parent; writes latch a leaf or full root-to-leaf path on split (`index.rs:411`, `:451`, `:536`).
- Index split creates a sibling, propagates separator upward, and may publish a new shared root; this ordinary path mutates pages in place (`index.rs:600`, `:680`).
- Buffer pool never acquires a page latch from inside pool locks; debug instrumentation checks the lock-order rule (`src/storage/page_latch.rs:19`).
- Heap mutators append WAL before exposing dirty serialized bytes, and stamp the resulting page LSN (`src/storage/heap_file_manager.rs:154`, `:172`).
- WAL gate covers normal dirty eviction and explicit flushes (`buffer_pool.rs:1078`, `:1200`, `:1425`). Gate understands heap/index type offsets; other types produce zero.
- **Ordinary index writes do not themselves append physiological WAL records** (`src/storage/index.rs:651`). Recovery rebuilds ordinary indexes from recovered heap (`src/wal/recovery.rs:177`).
- Commit does not require every table page to be flushed: `src/wal/txn.rs:608`. Abort walks per-transaction WAL backward, writes CLRs and undoes heap changes (`:651`).
- Startup attaches WAL, recovers heap, opens SQL catalog, rebuilds indexes if recovery ran, then checkpoints (`src/cli/cli.rs:64`).

## Concurrency boundary to show, not hide

- pgwire permits shared read execution using a cached catalog snapshot and per-connection read slot (`src/pgwire/extended.rs:361`).
- Exclusive statements take a single catalog mutex; **every** exclusive catalog borrow announces a writer and drains shared readers (`src/pgwire/mod.rs:143`, `:261`).
- This includes ordinary DML and branch operations, not only DDL. CLI also locks per statement (`src/cli/cli.rs:244`).
- Therefore distinguish overlapping transaction lifetimes and concurrent internal components from unconstrained parallel SQL write execution.
- DDL is rejected inside explicit transactions and uses checkpoint fences (`src/execution/executor.rs:214`).
- Agent branch overhead should not be called cheaper than ordinary transactions without a scoped benchmark; they offer private candidate state and semantic merge machinery.

## Focused existing tests for parent verification

| Area | Existing test targets |
|---|---|
| SQL snapshot | `execution::executor::tests::test_snapshot_pins_at_begin`; `test_uncommitted_writes_invisible_across_sessions` |
| Conflict | `wal::visibility::tests::overwriting_a_version_an_in_flight_transaction_created_is_a_conflict`; analogous deleted-version test |
| Snapshot concurrency | `tests/integration_txn_concurrency.rs`; `tests/d59_snapshot_cache.rs` |
| DML/index agreement | `tests/d178_dml_index_correctness.rs` |
| Ordinary index concurrency | `tests/integration_btree_concurrency.rs`; `tests/d58_latch_free_descent.rs` |
| Buffer identity / pinning | `tests/integration_buffer_pool_concurrency.rs` |
| WAL-before-page and flush races | `tests/integration_flush_concurrency.rs`; inspect WAL unit tests for WAL-order assertions |
| Crash/rebuild | `wal::recovery::tests::sql_crash_recover_rebuild_query`; `test_insert_survives_crash`; `test_uncommited_rolled_back` |
| Retained index debt | `tests/integration_index_debt.rs`; `tests/integration_secondary_index_debt.rs` |

## Source coverage and limits

Traced live bodies in CLI, pgwire statement execution/server synchronization, executor, planner, optimizer, tuple/heap/index/directory layouts, insert/update/delete, secondary scans, transaction snapshots/commit/abort, visibility, ordinary B+ lookup/split, buffer fetch/optimistic read/flush and recovery index rebuild.
This is a mechanism map, not a proof that every combination of MVCC, rollback, key reuse, DDL and index access is correct. Do not turn test names or historical comments into verified universal guarantees.
