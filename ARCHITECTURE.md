# ferrodb / ferrobranch — architecture

A relational database written from scratch in Rust with **zero runtime dependencies**. `tempfile`
and `proptest` are dev-dependencies; the shipped library links nothing but `std`.

It has two identities sharing one storage stack:

- **ferrodb** — a conventional SQL engine: heap files, B+tree indexes, MVCC, ARIES write-ahead
  logging, a cost-based optimizer, and a PostgreSQL wire-protocol front end.
- **ferrobranch** — an *agent-isolation* layer where the unit of isolation is an agent task rather
  than a transaction: copy-on-write branches, a typed effect log, provenance, and a verification
  gate.

Every claim below is either enforced by a test or marked as a limitation. Where something is
demonstrated only under a condition, the condition is stated.

---

## 1. Layer map

```
                    ┌────────────────────────────────────────────┐
  clients           │  psql-compatible wire (src/pgwire)         │
                    │  simple query protocol, v3 subset          │
                    └───────────────────┬────────────────────────┘
                                        │
                    ┌───────────────────▼────────────────────────┐
  SQL               │ scanner → parser → binder → planner        │
                    │        → optimizer → executor              │
                    └───────┬───────────────────────┬────────────┘
                            │                       │
              ordinary DML  │                       │  agent-session DML
                            │                       │
        ┌───────────────────▼──────┐   ┌────────────▼─────────────────────┐
        │ heap files + B+tree      │   │ agent runtime (src/agent_sql)    │
        │ MVCC version chains      │   │ workspace, TEL capture, merge,   │
        │ (src/storage, src/catalog)│  │ gate, escrow, quarantine         │
        └───────────────┬──────────┘   └────────────┬─────────────────────┘
                        │                           │
                        │              ┌────────────▼─────────────────────┐
                        │              │ branch engine (src/branch,       │
                        │              │ src/cow): CoW B+tree, per-branch │
                        │              │ arenas, lease reaper, collapse   │
                        │              └────────────┬─────────────────────┘
                        │                           │
        ┌───────────────▼───────────────────────────▼─────────────────────┐
  store │ buffer pool (ARC replacement)  ·  WAL (ARIES)  ·  disk manager   │
        │ src/buffer                        src/wal        src/storage     │
        └───────────────────────────────┬──────────────────────────────────┘
                                        │
                    ┌───────────────────▼────────────────────────┐
  replication       │ WAL log shipping (src/replication)         │
                    │ primary streams durable frames to replicas │
                    └────────────────────────────────────────────┘
```

---

## 2. Storage substrate

### Disk manager (`src/storage/disk_manager.rs`)

Fixed 4KB pages in one file. Allocation is a chained bitmap, guarded by a mutex, with an **arena
floor**: everything at or above the floor belongs to the branch engine's arena allocator and is
never handed out by the bitmap. Without that floor the bitmap — whose bits are zero from page 0 —
hands out pages that already belong to an arena, and two writers silently overwrite each other.

Verified thread-safe: 8 threads allocating concurrently never receive a duplicate page id, and
removing the allocator's lock produces 1675 duplicates, so the test is known to be able to fail.

### Buffer pool (`src/buffer/buffer_pool.rs`)

1024 frames, ARC replacement, per-frame `RwLock`, a page table, and a WAL gate on eviction.

The operative rule in this file is that **a decision and the action taken on it must be one
critical section.** Three defects lived here, all the same shape — state read under one lock, acted
on under another:

- `fetch_page`'s no-evict path lost writes under 8-way concurrency.
- The eviction path *panicked* (`no entry found for key`), because the ARC cache's verdict was
  computed under a lock released before the page table was consulted.
- `flush_page` could write one page's bytes to disk under another page's id — durable corruption,
  not merely a lost write.

All three are fixed and fire-checked. `fetch_page` now holds the cache lock for its whole body,
which serialises fetches; that cost is deliberate and documented, because the alternative on offer
was a pool that crashes.

### Write-ahead log (`src/wal`)

ARIES: analysis → redo → undo, with CLRs for compensation and a torn-tail scanner
(`scan_valid_end`) that finds the last CRC-valid record.

**An LSN is a byte offset.** That one decision is why replication was cheap to add: the log is
already an ordered, self-describing, checksummed byte stream, and `read_record(lsn)` returns the
next LSN.

`flushed_lsn` is the durability frontier and is consulted by the buffer pool before any dirty page
is written. It used to over-report by ~117KB under concurrent flushes, because two flushes could
overlap and `fetch_max` advanced past a hole — which would let a data page reach disk before the
record describing it, the single rule write-ahead logging exists to enforce. Fixed by holding the
buffer lock across the file write.

### Heap and MVCC (`src/storage`, `src/catalog`)

Slotted pages, tuple version chains with `begin_ts`/`end_ts`, and snapshot isolation through
`TxnManager`. Snapshot correctness is tested directly: over 500 samples taken *during* concurrent
transaction churn, a running transaction was never missing from a snapshot — which is the property
that stops a reader treating uncommitted writes as committed.

---

## 3. Agent isolation (ferrobranch)

### Branch engine (`src/branch`, `src/cow`)

A branch **is a root pointer**. Forking sets `child.root_page_id = parent.root_page_id` and appends
a `fork_epoch` to the parent's sorted live-children array: one metadata record, **zero data pages
touched**. Measured flat at 10, 100 and 1000 branches.

Because the child's root *is* the parent's root, ordinary B+tree descent reaches parent data. The
read path never walks a parent chain — that is a hard rule, not an optimisation, because the
"not found here, ask my parent" overlay is what BranchBench measured degrading up to 5400×.

Reclamation is birth-epoch interval algebra rather than refcounts or content addressing:

> page `p` is reclaimable iff no live child has `fork_epoch` in `[birth(p), free(p))`

which is a range-emptiness query over a sorted array. Novel pages come from per-branch ~1MB
arenas, so reaping a childless branch is an extent-level free.

Every branch carries a lease, and a reaper hard-reaps expired branches **with no client
cooperation** — the thesis criterion. `BranchId` carries a generation counter, so a reaped id can
never be mistaken for a live one.

`CowTree::diff` compares two roots by **page identity**: an unchanged subtree is not merely equal to
its old self, it *is* the same page id, so it can be skipped unread. A one-row change in a
4000-key, 41-page tree decodes 4 pages; without the pruning it decodes 82.

### Typed Effect Log (`src/tel`)

Writes are logged as typed operations — `Assign`, `Add`, `Max`, `Min`, `SetInsert`, `SetRemove` —
alongside the **guards** that made them legal. Guards are the part that cannot be reconstructed
from a byte WAL: a numeric delta can be, `WHERE qty >= 5` cannot.

Merge is three-way against the fork point and reports four outcomes, the fourth being the point:

| Outcome | Meaning |
|---|---|
| `Clean` | main untouched |
| `Commuting` | both wrote and the ops compose (`Add`+`Add`, `SetInsert`∪`SetInsert`) |
| `Conflict` | contradictory, or a guard failed against merged state |
| `ResolvedWithLoss` | a policy succeeded **while discarding a write** |

The algebra is property-tested: composition/application coherence, commutativity, and the
**non-idempotence of `Add`** (two identical `qty -= 5` compose to −10; making that idempotent would
halve every concurrent decrement).

### Provenance (`src/provenance`)

The actor tuple — agent, run, model, model version, prompt hash — has *run-level* cardinality, so
it is interned once and referenced by a per-version slot rather than copied per row. Read-sets are
retained too, in a form chosen by **access shape**: point lookups keep exact versions, scans keep a
predicate summary (which is what gives phantom coverage). Retaining reads is what makes causal
`REVERT ... CASCADE` possible.

### Verification gate (`src/agent_sql/gate.rs`)

Tiers run in **cost ÷ rejection-probability** order — computed, not hard-coded, so the rule can
notice if its inputs change. Short-circuit happens *between* tiers but never *within* one, so an
agent learns all of a tier's defects in one round trip rather than one per round trip.

The outcome is chosen by the **epistemic status** of what fired, not its severity:

| Status | Outcome |
|---|---|
| sound and crisp | retry, with the violated predicate handed back |
| heuristic | quarantine — unmerged but still queryable |
| could not be evaluated | hard reject; not knowing is not passing |

The `write-set \ read-set` blind-write metric (rows an agent changed without ever reading) is a
heuristic tier: it reports, and does not decide.

### Escrow (`src/agent_sql/escrow.rs`)

Guards are *preconditions* evaluated against merged state **before** the composed ops apply, so an
invariant-shaped guard does not hold: starting at 20 with two agents each taking 12 under
`qty >= 0`, the second merge tests `8 >= 0`, passes, and the counter lands at **−4**. Measured, and
DESIGN.md was corrected at source because it used exactly that as a worked example.

Escrow is the answer, and it works by moving the failure earlier rather than making the merge
cleverer: slack is partitioned at claim time, and an overdraw is refused at **write** time while the
agent can still act on it. It charges the *change to the cell*, not the shape of the op, so
`Assign`, `Add` and float deltas are all covered.

---

## 4. Replication (`src/replication`)

Primary/replica **physical WAL log shipping**. A replica restores a base backup, connects, says how
far it has, and the primary streams frames onward; the replica CRC-checks each frame, verifies its
embedded LSN against the stream position, and applies it through the same `apply_redo` recovery
uses — inheriting ARIES idempotence, so a reconnect's re-sent overlap is absorbed rather than
double-applied.

Log shipping alone cannot start a replica, which is what the 2000-row case proved: the primary
truncates its WAL at every checkpoint, so there is no state for the surviving records to apply to.
`backup::take` copies pages **through the buffer pool**, so each page is atomic against a concurrent
writer. That is what makes the copy safe without full-page writes — redo skips by comparing against
a page's own LSN, so a torn page whose LSN read new while its bytes read old would make redo skip
exactly the records needed to repair it.

The rule the scheme rests on: **a primary never ships a record it has not durably written.** A
replica holding records the primary loses on a crash is *ahead* of its primary, which is divergence
rather than lag and nothing downstream can reconcile it.

---

## 5. What this does not do

Kept deliberately; a fabricated pass would be worse than an admitted gap.

- **No consensus.** No Raft, no leader election, no automatic failover, no split-brain protection.
  Two nodes that both believe they are primary would diverge and nothing here would notice.
- **A base backup holds the primary's WAL open.** Base backup now exists (`src/replication/backup.rs`),
  and the 2000-row case that used to fail now converges. The cost is a *pin*: a backup stops the
  next checkpoint discarding the log it points into, and since this log cannot be truncated
  part-way, honouring the pin means keeping all of it. **A backup handle nobody drops is a WAL that
  never shrinks** — the same hazard PostgreSQL replication slots have. Without the pin the failure
  is not subtle: a backup taken while the primary runs is refused the moment a replica uses it.
- **Only pages the WAL describes are replicated.** The catalog and the heap page directory are
  written outside the log. A base backup carries them *as of the instant it ran*, and nothing
  afterwards updates them. Measured, not assumed: after a backup taken while the primary was still
  inserting, every WAL-described page matched byte-for-byte and every page outside the log did not.
  So a backup taken against a running primary does **not** by itself give a usable replica.
- **Reconnect and catch-up is not demonstrated end to end.** The applier is idempotent by
  construction and unit-tested as such; no test yet kills a replica mid-stream and restarts it.
- **SQL statements do not write CoW pages.** `UPDATE`/`INSERT` inside an agent session stage into an
  in-memory workspace. A page-backed row path exists and criteria 1 and 8 are measured on it, but
  reading "criterion 2 is MET" as "isolation enforced by shadow paging" is wrong for the SQL
  surface. This is the largest remaining gap.
- **A guard must name the amount taken.** See escrow above.
- **Escrow governs agent-session writes only.** A plain `UPDATE` outside a session never reaches
  the capture point and is not charged.
- **Crash safety means process death, not power loss.** The crash tests kill with `abort()`; bytes
  already handed to `write()` survive in the OS page cache.
- **`psql` itself has not been run.** The wire protocol is verified by an independently written
  client speaking the same protocol, on a machine where psql is not installed.
- **Single node.** No sharding, no distributed query, no clustering.

---

## 6. Testing posture

638 tests, 0 failed, 0 ignored. Three habits matter more than the count:

1. **A test that has never failed is not evidence.** Fixes are fire-checked by reintroducing the
   defect and confirming the test catches it. Several "passing" tests this project has held were
   found incapable of failing — one checked a concurrency invariant only after every thread had
   joined, when the hole it looked for is necessarily filled.
2. **Guard against vacuous passes.** Measurements assert their own preconditions: a zero-copy claim
   asserts the tree occupied more than one page first, and a benchmark refuses to report if
   calibration shows its instrument cannot move.
3. **Judge from the artifact, not the reporter.** Replication convergence is decided by page bytes
   on disk; crash safety by re-reading the database; buffer-pool correctness by stamping each page
   with its own id — never by asking the component whether it thinks it succeeded.
