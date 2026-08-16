# ferrobranch — end-to-end demonstration

ferrodb becomes an **agent-isolation database**: the unit of isolation is an agent task, not a
transaction. This document describes the runnable demonstration of the ten exit criteria, what it
genuinely proves, and — at least as importantly — what it does not.

Design authority for every decision referenced here is `DESIGN.md`.

---

## How to run it

```sh
export PATH="$HOME/.cargo/bin:$PATH"     # the toolchain is not on the default PATH
cargo run --example agent_isolation_demo
```

It prints a ~600-line transcript and **exits non-zero** if any criterion fails, or if fewer than
ten criteria report. The exit code is the gate; the transcript is the evidence.

Every number in the transcript is read out of the engine at the moment it is printed. Each `[ok]`
line is a live comparison, not a label. There are no hard-coded expected values printed as if they
were measurements.

```sh
cargo test                               # 430 tests, unchanged by this demo
```

`cargo test` **compiles** the example (so it cannot rot silently against the API) but does **not
run** it. The demo is a demonstration, not part of the test gate.

---

## The three layers, and why that matters

ferrobranch is built in layers that are wired together by trait but **do not yet share row
storage**. This is the single most important caveat in this document, and the demo prints it at
the top of its own output rather than burying it here.

| Layer | Code | Answers |
|---|---|---|
| page layer | `branch::ArenaPageStore` + `cow::CowTree` + `branch::LogBranchCatalog` + `branch::TwoTierReaper` | criteria **1, 8** |
| agent SQL | `agent_sql::AgentRuntime` behind the real scanner → parser → binder → executor | criteria **2–7, 10** |
| provenance | `provenance::MemProvenanceStore` over real heap `RecordId`s | criterion **9** |

**What is genuinely shared:** the agent-SQL surface forks its branches through the same
`LogBranchCatalog` the page layer uses, so branch identity, generation counters and leases are one
mechanism, not two.

**What is not shared:** rows written inside an agent session live in that branch's in-memory
workspace (`agent_sql::runtime::Workspace`), *not* on copy-on-write pages. Consequently:

> Criterion 8's "the page count returns to baseline" is proven for pages written through the CoW
> B+tree. It is **not** transitively proven for rows written through agent SQL, because those rows
> were never on those pages.

Anyone quoting the demo's summary table must carry that sentence with it.

---

## What the demo shows

### 1. Fork copies zero data pages — *page layer*

Trunk writes a real multi-level CoW B+tree (400 keys, 6 pages, 1 internal node). Then a branch is
forked and the same instruments are read again:

|  | before fork | after fork | delta |
|---|---|---|---|
| data pages allocated | 6 | 6 | **0** |
| pages reserved in arenas | 256 | 256 | **0** |
| pages reachable from the branch root | 6 | 6 | **0** |

The child's root page id *is* the parent's (`3` and `3`), its reachable page set is byte-identical,
it owns zero arenas, and all 400 trunk keys read back through ordinary descent — no parent-chain
walk.

**Negative control.** A zero delta is only meaningful if the instrument can move. One write on the
child takes the count 6 → 8 and gives it its first arena, and the parent's root still reads the old
value at that key. So the zero above is a measurement, not a dead gauge.

### 2. Branch writes invisible to main and to siblings — *agent SQL*

Three connections to one database. Agent A updates, inserts and deletes without merging. The same
`SELECT` from all three: writer sees `15`, main sees `20`, sibling branch sees `20`. Visible ids:
writer `[1, 3]`, main `[1, 2]`, sibling `[1, 2]`. After `MERGE`, main becomes `[1, 3]`.

### 3. `SELECT ... AS OF BRANCH` reads uncommitted state — *agent SQL*

A connection that never opened a session reads another branch's un-merged rows by name, including
its uncommitted inserts and deletes. `AS OF` composes with `WHERE` and projection. Naming an
unknown branch is an error, never an empty result.

### 4. `DIFF` returns a structured changeset — *agent SQL*

Fields are read off the struct, not parsed from text. For `UPDATE inventory SET qty = qty - 5 WHERE
qty >= 5 AND id = 1`:

- `.kind` = `Update`, `.before` = `[1, 20]`, `.after` = `[1, 15]`
- `.ops[0].kind` = **`Add(Int(-5))`** — the algebra element, not a scalar result
- `.ops[0].witness` = `Some(Integer(20))` — the value observed before the op applied
- `.guards[0]` = **`qty >= 5 AND id = 1`** — kept verbatim

### 5. `MERGE` reports all four outcomes — *agent SQL*

All four are produced by real merges in four separate scenarios, then checked to be four distinct
values — not enumerated from a list:

| scenario | outcome |
|---|---|
| one branch, nothing concurrent | `Clean` |
| two branches, both `Add` on one cell | `Commuting` |
| two branches, contradictory assignments, no declared policy | `Conflict` (publishes nothing) |
| same, with `inventory.qty` declared `LWW` | `ResolvedWithLoss` (names the discarded write) |

A column with no declared policy **forbids** concurrent updates rather than falling back to
last-writer-wins, and a lossy resolution is never reported as `Clean`.

### 6. Concurrent `qty -= n` compose arithmetically — *agent SQL*

`20 - 5 - 3 = 12`, which is neither branch's own answer (A alone gives 15, B alone 17). The second
merge reports `Commuting` and names the composed op `Add(Int(-3))`.

`Add` is correctly **not** idempotent: two identical `qty -= 5` inside one task give `10`, not
`15` — the Cassandra counter trap that makes retries double-count.

### 7. Guard violation rejected, violated predicate handed back — *agent SQL*

Two branches each take 12 from a starting 20. Each is individually legal (`20 >= 12`). Composed
they would be `-4`. The first merge is `Clean` (main → 8); the second is re-checked against the
**merged** state and returns:

```
outcome:            Conflict
conflict kind:      GuardFailed
violated predicate: id = 1 AND qty >= 12
qty on main:        8          (the counter never went negative)
```

The branch stays alive so the agent can retry with real feedback.

**The measured boundary.** The criterion names the invariant `qty >= 0`. The demo re-runs the same
scenario with that literal guard and prints what happens:

```
FINAL qty on main:  -4
```

ferrobranch enforces **the precondition the agent wrote**, re-evaluated against merged state — not
a declarative table constraint. `qty >= 0` is a pre-condition that passes against the merged state
(`8 >= 0`), after which the composed decrement drives the value below the bound. The correct guard
for a bounded counter is `qty >= <the amount being taken>`. This is a real limitation and it is
listed again below; it was found by running the case, not assumed.

### 8. *** THE THESIS *** — non-cooperative reaping — *page layer*

Baseline is trunk **plus one legitimately open agent session** with an hour-long lease. Including a
healthy branch in the baseline is deliberate: it means "returns to baseline" cannot be satisfied by
a reaper that simply deletes everything. Over-reaping fails the same check as under-reaping.

16 agent tasks then fork, each writes 12 keys, and each is killed. **Nothing is called on their
behalf** — no `ABANDON`, no `MERGE`, no `close()`, no `free()`, no rollback, no destructor of ours.
The `BranchId`s are dropped on the floor.

|  | before | during | after |
|---|---|---|---|
| data pages allocated | 8 | 40 | **8** |
| pages reserved in arenas | 512 | 4608 | **512** |
| live branches | 2 | 18 | **2** |

The lease scan reaped exactly the 16 expired branches and left the unexpired one alone. Reading a
reaped branch is a hard error (`branch b2@g0 has been reaped (id slot is now at generation 1)`),
never stale data. The surviving branch's 12 keys and trunk's 400 keys are all intact.

### 9. Provenance: which agent + run + model wrote a row — *agent SQL + provenance*

**Through SQL:** the effect log is asked which task carries an `Op` on a given `RowId`, and that
task's run is resolved. Row 1 → `restock-agent / run-42`; row 2 → `auditor-agent / run-99`.

**In the provenance store, on real heap rows:** rows are located by a real `SeqScan`, their
`begin_ts` read from the actual version header, and `who_wrote(rid)` returns the full tuple:

```
row 1 -> agent=restock-agent run=run-42 model=claude-opus-5/2026-05
row 2 -> agent=auditor-agent run=run-99 model=claude-sonnet-4/2026-02
```

**Interning is demonstrated, not asserted:** 32 rows on one page stamped by 2 runs produce **2**
dictionary entries — the dictionary tracks runs, not rows. 128 B stored versus 3296 B if the actor
tuple were written literally into every version. A row nobody claimed reports `unattributed`, never
the nearest run.

### 10. `REVERT ... CASCADE` over retained read-sets — *agent SQL*

Agent A updates row 1 and merges (`m_1`). Agent B **reads** row 1, then writes row 2 on the
strength of it, and merges (`m_2`).

- `REVERT MERGE m_1;` → **halts**. `blocked_by = [TxnId(2)]`, `cascade = []`, no data moves.
- `REVERT MERGE m_1 CASCADE;` → `cascade = [TxnId(2)]`; row 1 → 20 **and** row 2 → 5. Both the
  target write and the write that depended on it are undone.

**Negative control:** a merge nobody read reverts with an empty cascade, so the halt above is a
real finding rather than a constant. An unknown merge id is an error, not a silent no-op.

---

## Genuinely demonstrated vs. merely asserted

**Genuinely demonstrated** — a real mechanism runs and its output is measured:

- **1, 8** — real CoW B+tree on a real file, real arena allocator, real page counters, with
  negative controls in both directions.
- **2, 3, 4, 5, 6, 7, 10** — real SQL text through the real scanner, parser, binder and executor;
  the structures printed are the structures the engine returns.
- **9** — real heap `RecordId`s from a real scan; real interning ratio measured over 32 rows.

**Demonstrated with a stated substitution:**

- **9 (model half)** — the model is carried by the provenance layer, because `BEGIN AGENT SESSION`
  has no `MODEL` clause. From SQL, the model is recorded as the literal string `unspecified`.
- **9 (stamping)** — the demo calls `ProvenanceStore::stamp` where the agent-SQL write path would.
  The agent-SQL write path does not currently stamp a provenance store at all.
- **8 (time)** — the reaper takes `now_millis` as a parameter, so the demo advances the clock by
  passing a value rather than sleeping. The reaping is real; the passage of time is simulated.

**Asserted but NOT demonstrated by this demo** — believed on the strength of unit tests elsewhere
in the tree, not by anything printed here:

- Durability of the branch catalog across process restart (`LogBranchCatalog::open` exists; the
  demo uses `::in_memory`).
- Range/predicate read-sets. The demo only exercises point reads, which retain exact version ids.
  `tests/provenance_e2e.rs` covers the predicate form.
- `collapse` at the depth-8 guard. Covered by `tests/integration_cow_branch.rs`, not here.
- Crash recovery, WAL replay, and `resume_interrupted_reaps`.
- Any concurrency claim. The demo is single-threaded throughout.

---

## What this does not do yet

An honest list of every stub, gap and limitation known at the time of writing.

### Architecture

1. **Agent-SQL rows are not on CoW pages.** A branch's uncommitted rows live in an in-memory
   `BTreeMap` in `agent_sql::runtime::Workspace`. This is the design's "per-branch write buffer,
   probed before descent", expressed in rows rather than pages. It means criteria 1 and 8 (page
   counts) and criteria 2–7 (statement behaviour) are proven on **different substrates**, and the
   demo cannot show an abandoned *SQL session's* pages returning to baseline, because it has none.

2. **No background reaper.** There is no thread, timer or daemon anywhere in `src/branch/` or
   `src/agent_sql/` — verified by grep, not assumed. `Reaper::reap_expired(now_millis)` must be
   called explicitly. The non-cooperative *policy* is implemented and proven; the *scheduling* of
   it is not wired up.

3. **Merge targets the parent only.** `AgentRuntime::merge` computes its target as
   `parent_id.unwrap_or(TRUNK)`. There is no merge into an arbitrary branch.

4. **In-memory implementations on the demo path.** `AgentRuntime::new` uses
   `LogBranchCatalog::in_memory`, `MemEffectLog` and an in-process provenance map.
   `LogBranchCatalog::open` (durable, file-backed) exists and is not what a default session gets.

### Semantics

5. **No declarative constraints.** There is no `CHECK` clause. Guards are the `WHERE` clause of the
   statement that made the write, re-evaluated against merged state. **Measured consequence:** the
   literal guard `qty >= 0` does *not* protect the bound — two composed decrements reach `-4`. The
   invariant holds only if the agent writes `qty >= <amount being taken>`.

6. **`RowId` is derived from the primary key**, not minted at insert. `agent_sql::runtime::row_id_of`
   hashes the first column. The design is explicit that the PK is a *constraint*, not identity, so
   updating a primary key would currently look like a delete plus an insert. Every caller goes
   through that one function, which is where a real surrogate would be introduced.

7. **`BEGIN AGENT SESSION` has no `MODEL` clause.** Agent id and run id are captured from SQL; the
   model is recorded as the literal `unspecified` rather than guessed at. Criterion 9's model half
   is therefore carried only by the provenance layer.

8. **The agent-SQL write path does not stamp provenance.** `agent_sql::runtime` references `ProvId`
   and `RunEntity` but no `ProvenanceStore`. Per-row attribution through SQL is reconstructed from
   the effect log's ops; the page-local dictionary is exercised only via the provenance layer
   directly.

9. **No verification gate.** DESIGN.md §4 specifies a tiered gate, the `write-set \ read-set` blind
   -write metric, and quarantine for heuristic checks. `provenance::blind_writes` exists; the
   tiered gate, its ordering by cost ÷ rejection-probability, and the quarantine outcome are not
   built, and nothing in this demo exercises them.

10. **Escrow claims are unused.** `TxnFrame` carries a `claims: Vec<EscrowClaim>` field; nothing on
    the demo path populates it.

### Scope of the demo itself

11. **Single-threaded.** No concurrency, no interleaving, no torn-write or race behaviour is
    exercised. "Concurrent" throughout the transcript means *logically* concurrent branches merged
    in sequence.

12. **Not part of the test gate.** `cargo test` compiles the example but does not run it. Its
    guarantees hold only when someone runs it.

13. **Small scale.** 400 keys, 16 abandoned branches, 32 stamped rows. Nothing here is a
    performance result, and no claim about the 5400x overlay degradation BranchBench measured is
    tested — only that the read path is structurally incapable of the pattern, because the child's
    root *is* the parent's root.

### Stubs that exist in the tree

Nothing on the demo path returns a fake value: where a mechanism is missing it is missing entirely
and named above. Two **pre-existing** `todo!()`s do exist in the repository, neither added for this
demo, and both are worth knowing about:

14. **`Binder::bind` panics for `Insert` / `Update` / `Delete` / `CreateTable` / `CreateIndex` /
    `Join`** (`src/binder/binder.rs:139-154`). Those statements do not reach `bind` — they are
    dispatched elsewhere in `execution::executor::run` — which is why the demo executes well over a
    hundred of them and exits 0. It is a trap for a future caller who routes a write statement
    through the planner, not a live defect.

15. **`BPlusTreeManager::handle_underflow` is `todo!()`** (`src/storage/index.rs:280`). This is the
    legacy in-place B+tree, the one the CoW store replaces. A `DELETE` that drove an index node
    into underflow would panic. The demo issues deletes and never reaches it at this scale, so this
    is a latent panic under a larger delete workload, not something the demo proves safe.

---

## Files

| Path | What |
|---|---|
| `examples/agent_isolation_demo.rs` | the demo; one function per criterion, each self-contained with its own fresh database |
| `DESIGN.md` (external) | design authority |
| `tests/agent_sql_surface.rs` | the SQL surface's own test suite |
| `tests/integration_cow_branch.rs` | CoW B+tree on the arena store, and `collapse` |
| `tests/provenance_e2e.rs` | read-sets and causal rollback against the real engine |
| `src/branch/reaper.rs` | the two-tier reaper, including its own thesis test |
