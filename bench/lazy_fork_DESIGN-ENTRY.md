⛔ THIS IS AN ENTRY FOR `SCALE-DESIGN.md`, PARKED HERE BECAUSE THAT FILE LIVES IN ANOTHER AGENT'S
WORKTREE (`~/wt/artie-research`) AND THIS ONE WAS SCOPED TO `~/wt/ferrodb-S14-lazy-durability`.
Append it there under the next free D-number and add its index row; nothing else needs editing.

## D11 — Durability at first write: the fork wall, removed rather than widened

**2026-09-17. Status: BUILT. This is D6 option 2, gated on its `kill -9` falsifier, which fired.**

### What it does

`TableBranchCatalog::fork` wrote six B+tree keys and waited on an fsync. It now writes its keys
into an in-memory stage and returns, touching no page and issuing no fsync. The keys land — through
the same `write_record_new`, byte for byte the same keys — at the branch's **first write**, sharing
that write's fsync.

### The measurement (bench/lazy_fork_first_write.txt, three alternating runs, medians)

| arm | threads | before | after | ratio | fsyncs |
|---|---|---|---|---|---|
| speculative (fork, abandon) | 1 | 222.1/s | 311,359.8/s | **×1402** | 640 → **0** |
| speculative | 64 | 3,830.4/s | 157,711.6/s | **×41** | 37 → **0** |
| fork+write (the durable path) | 1 | 73.0/s | 219.7/s | ×3.01 | 1280 → 640 |
| fork+write | 8 | 435.5/s | 846.4/s | ×1.94 | 320 → 166 |
| fork+write | 64 | 2,577.5/s | 2,552.3/s | ×0.99 | 61 → 59 |
| fork+reap (whole lifecycle) | 1 | 32.8/s | 137,202.9/s | **×4183** | 3200 → **0** |

⚠ **The ratio moves with machine load and the fsync column does not.** Three other agents shared
this machine; an earlier alternating session put the same BEFORE cell at 107.3/s and the same AFTER
cell at 383,357/s (×3573). The AFTER side is the same to within noise in both; the BEFORE side is
disk-bound and the machine was busier. Quote the fsync counts, not the multiple.

**It is a complexity-class change, not a constant**: a speculative fork went from `O(log N)` durable
tree writes plus one fsync to one in-memory insert and **zero disk operations**. The bar was a class
change or ×2; both are met. The durable path — the one that could have exposed this as deferral with
interest — is ×1.94–3.01 at 1 and 8 threads because the fsync count per written branch falls from 2
to 1, and **×0.99 at 64 threads, where group commit had already shared those fsyncs down to 61 for
640 branches and there was nothing left to take.** That last row is the honest shape of the result.

### The falsifier, which is the deliverable

`tests/integration_fork_lazy_durability.rs` SIGKILLs a process between fork and first write and
requires the catalog file to be **byte-identical**. Written first, run against the unmodified code,
and watched failing:

```
the catalog file GREW by 131072 bytes across 512 forks that wrote nothing
  left: 143360   right: 12288
```

It now passes, together with: no acknowledged id resolves, `has_live_children(trunk)` is false,
`live_count` is 1, and the first fork after the crash gets id 1 again. Its companion
`tests/integration_fork_kill9.rs` holds the other half — a fork that DID write must survive — and
its victim now publishes a root before printing an id, because that is what "acknowledged" means
under the new contract. Neither test is sufficient alone: the first passes a catalog that never
fsyncs, the second passes one that fsyncs on every fork.

### Why a KEY-level stage and not a record-level one

The obvious shape is `HashMap<BranchId, BranchRecord>` consulted by `get`. It is wrong: a record
overlay has to be re-implemented in each of seventeen trait methods — the state span, the deadline
span, the child span, the envelope span and the FREE_ID span each answer a different question — and
a method that forgets it does not fail loudly, it silently reports a live branch as absent. The
key-level stage is consulted in **two** places, `kv_search` and `kv_range`, and every query is built
out of those. **No invention**: `cow::WriteBuffer` is already a per-branch in-memory buffer of key
writes probed before descent, and its docstring already says why — "a branch that dies before
flushing allocates zero pages". This is that idea applied to the branch's metadata; the catalog was
the one place still paying eagerly. Its `WriteBufferEntry` is reused rather than respelled.

### The cost column, measured rather than asserted

A pending fork is **~950 resident bytes** (flat over 50k → 400k, so a real per-branch cost), against
~266 durable bytes the eager path wrote. That is the trade `LogBranchCatalog` was deleted for making
badly, so it owes a bound and has one: **400,000 complete fork+reap cycles leave a 409,600-byte RSS
delta in total** — a constant, not a slope — and the catalog file unchanged at 12,288 bytes.
Residency is bounded by live branches that have not written, which is the working set D1 asked for.

### Two things this corrects

- **D10 is subsumed on the path it was measured on.** D10 removed a wasted descent from `fork`'s
  upserts (×1.13). `fork` now performs no tree operation at all, so the saving became structural.
  `write_record_new` survives as the staging writer and its `tree.insert` path is now reached only
  through materialisation.
- **A pre-existing epoch window, widened by this change, is closed by it.** `write_header` was called
  only by `create` and `fork`, so the durable epoch counter advanced only when a fork did — and a
  branch stamping page births between two forks could already exceed it. With forks no longer
  writing the header that window would have become unbounded, so the refresh moved into `stage()`,
  which every durable operation calls. Fire-checked (M6).

### What would falsify this entry

- A workload where most forks do write: then the only saving is the second fsync (×1.74).
- A process that forks faster than it reaps: residency is bounded by live unwritten branches, so a
  reaper leak becomes a memory leak where it used to be a disk leak.
- A consumer that needs an acknowledged fork to survive without writing. There is none, and D6's
  premise check says why there cannot be one.
