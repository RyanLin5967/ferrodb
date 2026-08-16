# ferrodb agent-isolation — end-to-end demonstration

A runnable transcript that exercises the ten exit criteria in `DESIGN.md` section 5 and **prints
the evidence for each one** rather than asserting it silently.

```
export PATH="$HOME/.cargo/bin:$PATH"
cargo run --example agent_isolation_demo
```

It exits `0` when every self-check passes and `1` otherwise, printing
`N SELF-CHECK(S) FAILED — the transcript above is not trustworthy.`

## Result

| # | Criterion | Verdict |
|---|---|---|
| 1 | `BEGIN AGENT SESSION` forks a branch copying zero data pages | **MET** |
| 2 | Branch writes invisible to main and to siblings until merge | **MET** |
| 3 | `SELECT ... AS OF BRANCH` reads another branch's uncommitted state | **MET** |
| 4 | `DIFF` returns a structured changeset | **MET** |
| 5 | `MERGE` reports Clean / Commuting / Conflict / ResolvedWithLoss | **MET** |
| 6 | Two branches doing `qty -= n` compose arithmetically | **MET** |
| 7 | Guard violation rejected, violated predicate handed back | **MET** |
| 8 | **THE THESIS** — abandoned branches reaped on lease expiry, pages return to baseline | **MET** |
| 9 | Provenance: which agent + run + model wrote a given row | **PARTIAL** |
| 10 | `REVERT ... CASCADE` finds a downstream dependent via read-sets | **MET** |

**9 MET, 1 PARTIAL, 0 NOT MET.** Every verdict is computed from a check inside the demo, not
written into it. Read "What this does not do yet" before quoting any of them.

---

## The one structural fact that bounds everything below

**The demo is in two acts, because the system is two layers that are not wired to each other.**

- **Act I — the branch engine.** Real 4KB pages in a real file on disk. Criteria 1 and 8.
- **Act II — the agent SQL surface.** Real scanner, parser, binder, executor. Criteria 2–7, 9, 10.

**A row written by SQL in Act II does not live on a copy-on-write page from Act I.** The SQL
surface keeps a branch's uncommitted rows in an in-memory per-branch workspace
(`agent_sql::runtime::Workspace`). The copy-on-write page store, the arenas and the reaper are all
real, all tested, and none of them are underneath the SQL layer yet.

So Act I proves that forking copies zero pages and that abandoned branches give their pages back.
It does **not** prove that the rows you see in Act II are the things on those pages. Any reading of
"criterion 2 is MET" as "branch isolation is enforced by shadow paging" is wrong: in Act II it is
enforced by a `BTreeMap` in memory.

One genuine connection between the acts, worth stating because it is easy to assume the opposite:
`AgentRuntime::begin_session` really does call `BranchCatalog::fork`, and the runtime's default
catalog really is the `LogBranchCatalog` that Act I measures. Criterion 1 is therefore measuring
the same fork implementation the SQL statement invokes — it is not a re-enactment.

---

## What each criterion actually shows

### 1 — Fork copies zero data pages

Trunk is loaded with 400 rows through the real CoW B+tree, giving a multi-level tree of **6 pages**
— so a copying fork would have something to copy. Then one `BranchCatalog::fork`:

```
BEFORE FORK                          AFTER FORK
  pages reachable from root ... 6      child root page ........... 3   (identical)
  live (allocated) pages ...... 6      live (allocated) pages .... 6
  reserved (extent) pages ... 256      reserved (extent) pages . 256

  PAGES COPIED BY THE FORK .......... 0
```

The child's root page id **is** the parent's, and the demo then reads three keys spread across the
tree through the child's root to show the data is reached by ordinary descent — not by a
"not found here, ask my parent" overlay, which `DESIGN.md` rules out outright.

### 8 — The thesis

32 agent branches fork, take a 10-second lease, and each writes 7 novel pages. **Nothing is ever
closed**: no `close`, `commit`, `abort`, `rollback`, `free` or `ABANDON`. The handles are dropped,
exactly as if 32 agent processes had been killed. Then the lease scan runs.

```
                     BEFORE      DURING      AFTER
live pages              6          230          6
reserved pages        256         8448        256
live branches           1           33          1
branches reaped                                32 of 32
```

Two things make this stronger than the numbers alone:

- **The baseline is deliberately non-zero.** Trunk is seeded with 400 rows *first*. "Returns to
  baseline" is trivially satisfiable by freeing everything, so the demo re-reads trunk's data after
  the reap and shows it is still there. The reaper freed the right pages, not all pages.
- **A reaped branch id is a hard error**, printed verbatim:
  `branch b1@g0 has been reaped (id slot is now at generation 1)` — never stale data.

### 5 and 6 — merge outcomes and composition

All four outcomes are produced by real merges, not by constructing the enum:

| Scenario | Outcome |
|---|---|
| solo merge | `Clean` |
| `qty-5` and `qty-3` concurrently | `Commuting`, result **12** |
| `qty=1` and `qty=2` concurrently, no policy | `Conflict`, nothing published |
| same, with `qty` declared LWW | `ResolvedWithLoss`, 1 discarded write itemised |

Criterion 6 is the `Commuting` row: 20 − 5 − 3 = **12**, which is neither branch's own answer (15
and 17) and not what last-writer-wins would give (17). This works because `DIFF` shows the log
stored the *algebra element* `Add(Int(-5))` with `witness Some(Integer(20))`, not a before/after
image pair.

### 7 — the violated predicate comes back

Two agents each take 12 from a starting quantity of 20, each with the guard `qty >= 12`, which holds
on each branch alone. Composed, 20 − 12 − 12 = −4, so the guard is re-evaluated against the merged
state and fails. The agent gets back:

```
PREDICATES HANDED BACK TO THE AGENT:
  id = 1 AND qty >= 12
```

Nothing is published, and the counter never goes negative.

### 10 — causal revert through read-sets

Agent A changes row 1 and merges. Agent B **reads** row 1, then writes row 2 on the strength of what
it read. Reverting A's merge halts by default and names B as the blocker — an edge that exists only
because the read was retained. **B never wrote row 1.** Without read-sets there is no edge at all
and the revert would silently corrupt B's work. `CASCADE` then undoes B first, then A.

---

## What this does not do yet

Listed because each one bounds a verdict above.

### The layers are not wired together

1. **SQL rows do not live on CoW pages.** Act II's isolation is a per-branch `BTreeMap` in memory,
   not shadow paging. This is the largest gap in the system, and it is why criteria 1 and 8 had to
   be demonstrated separately from 2–7.
2. **`BEGIN AGENT SESSION` allocates no pages, but not because it is efficient** — the SQL layer
   has no pages to allocate. Criterion 1's zero is measured in Act I, where it is meaningful.
3. **The SQL surface's branch catalog and effect log are memory-backed by default.**
   `AgentRuntime::new` uses `LogBranchCatalog::in_memory` and `MemEffectLog`. The durable
   implementations exist and are used elsewhere; the default agent session does not get them.

### Criterion 8's mechanism is proven; its scheduling does not exist

4. **There is no background thread anywhere in `src/`.** `grep -rn 'thread::spawn' src/` returns
   nothing. `Reaper::reap_expired(now_millis)` is a method that something must call, and outside
   tests and this demo, nothing calls it. The non-cooperative *mechanism* is real and proven — the
   demo drives it exactly as a scheduler would — but no scheduler is shipped.
5. **The demo supplies the clock reading.** `reap_expired` takes `now_millis` explicitly, so the
   demo passes `now + 100s` rather than sleeping through a 10-second lease. The deadlines are real
   and really compared; only the clock reading is injected. This is the reaper's own design, not a
   demo shortcut, but it does mean the demo does not prove wall-clock expiry.
6. **An abandoned SQL session never frees pages.** `AgentRuntime::abandon` marks the branch reaped
   through the `BranchCatalog` trait only; it does not invoke `TwoTierReaper`. Since Act II holds no
   pages, there is nothing to free — but the two halves of abandonment are not connected either.

### Criterion 9 is PARTIAL, and this is why

7. **The executor does no provenance stamping at all.** `grep -rn 'provenance' src/execution/`
   returns **zero matches**. Attribution is recorded by the agent runtime when a `MERGE` publishes a
   row, not by the storage write path. An ordinary non-agent `INSERT`/`UPDATE` is never attributed.
8. **Two provenance paths exist and are not connected.** The storage-level, per-`RecordId` path
   (`MemProvenanceStore::who_wrote`, interned `RunEntity`, page-local dictionary) is real and tested
   in `tests/provenance_e2e.rs` — but nothing calls it from the executor, so the test stamps writes
   itself. The path the demo exercises is the agent runtime's, keyed by row id.
9. **Row identity is derived from the primary key.** `agent_sql::runtime::row_id_of` hashes the
   first column because no layer mints surrogate row ids yet. `DESIGN.md` is explicit that the PK is
   a *constraint*, not identity, so **updating a primary key would currently look like a delete plus
   an insert**. Every caller goes through that one function, so there is a single place to fix it.

### Smaller, but real

10. **A retained guard is the whole `WHERE` conjunction, not the failing conjunct.** Criterion 7
    hands back `id = 1 AND qty >= 12`, not `qty >= 12`. The agent gets the predicate it needs, but
    it also gets the row selector welded to it, and nothing decomposes the conjunction to report
    which half actually failed.
11. **Criterion 5's `ResolvedWithLoss` needs the policy set through a Rust call**
    (`runtime.set_policy`). `DESIGN.md` section 3 calls for per-column policy *declared in schema*;
    there is no DDL for it, so the demo sets it programmatically.
12. **`MODEL` is a new optional clause added for this demo.** `BEGIN AGENT SESSION ... MODEL
    'name/version'` did not exist; the runtime hardcoded model and model_version to `"unspecified"`,
    which made the "model" half of criterion 9 unanswerable. A bare `MODEL 'gpt-9'` leaves the
    version half explicitly `unspecified` rather than inventing one, and an empty or half-empty
    model is refused at bind time.
13. **Act II's criteria are demonstrated on one small table** (`inventory`, 2–3 rows). They are
    correctness demonstrations, not performance ones. **No benchmark in this demo measures the
    5400x read-degradation claim** that motivated the no-parent-chain rule; criterion 1 shows the
    child descends from a shared root, which is the structural precondition, not the measurement.

### Stubs: seven exist in the tree, none on the demo path

`grep -rn 'todo!()' src/` returns **7 matches**, all pre-existing and none written for this demo:

- `src/binder/binder.rs` × 6 — the *logical-plan* arm of `Binder::bind` for `Insert`, `Delete`,
  `Update`, `CreateIndex`, `CreateTable` and `Join`. The demo issues `CREATE TABLE`, `INSERT` and
  `UPDATE` successfully because those statements are executed directly rather than through this
  logical-plan path. `Join` is genuinely unimplemented; the demo uses no joins.
- `src/storage/index.rs:280` — `handle_underflow`, the B+tree deletion rebalance. The demo performs
  no `DELETE`.

The evidence that none is on the demo path is empirical rather than argued: a `todo!()` panics, and
the demo runs to completion and exits `0`.

Everything printed was produced by running the code. Two mutants were injected into the demo's own
checks to confirm they can fail — both were caught, criterion 6 flipped to NOT MET, and the process
exited non-zero — so "all self-checks passed" is a result, not a default.

---

## Where the criteria are also covered by tests

The demo is not the only evidence; it is the readable evidence. The suite behind it is
**430 passing, 0 failing** (`cargo test`).

| Area | Tests |
|---|---|
| SQL surface, criteria 2–7, 10 | `tests/agent_sql_surface.rs` |
| Criterion 8, page counts | `src/branch/reaper.rs` (unit tests, crate-internal harness) |
| Criterion 9 at the storage layer | `tests/provenance_e2e.rs` |
| CoW tree over arenas, collapse | `tests/integration_cow_branch.rs` |
| Durable branch engine under SQL | `tests/integration_sql_on_durable_branches.rs` |
| Effect log, merge agreement | `tests/integration_effect_log.rs`, `tests/integration_merge_agreement.rs` |
