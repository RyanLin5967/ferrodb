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
| 9 | Provenance: which agent + run + model wrote a given row | **MET** |
| 10 | `REVERT ... CASCADE` finds a downstream dependent via read-sets | **MET** |

**10 MET, 0 PARTIAL, 0 NOT MET.** Every verdict is computed from a check inside the demo, not
written into it. Read "What this does not do yet" before quoting any of them.

---

## The one structural fact that bounds everything below

**The demo is in two acts, because the system is two layers that are not wired to each other.**

- **Act I — the branch engine.** Real 4KB pages in a real file on disk. Criteria 1 and 8.
- **Act II — the agent SQL surface.** Real scanner, parser, binder, executor. Criteria 2–7, 9, 10.

**A row written by a SQL *statement* in Act II does not live on a copy-on-write page from Act I.**
`UPDATE` and `INSERT` stage into an in-memory per-branch workspace
(`agent_sql::runtime::Workspace`), and that is still where a statement's uncommitted rows go.

**This is no longer the whole story, and the correction runs in both directions.**

The runtime has a page-backed row path: `AgentRuntime::with_storage` builds it over a real
`PageStore`, and `put_row` / `get_row` / `scan_rows` put a branch's rows behind its own root page.
`tests/integration_zero_copy_fork.rs` measures criteria 1 and 8 through it — on pages the runtime
itself wrote. **This demo does not.** Criteria 1 and 8 as printed here are measured against the
branch engine directly: `criterion_1_fork_copies_zero_pages` builds its own `ArenaPageStore` and
calls `BranchCatalog::fork`, with no `AgentRuntime`, no `Session`, and no SQL statement executed.
The `sql>` line printed inside criterion 1 is an echo, not an executed statement.

**The statement path was never the thing that was unwired, and as of 2026-08-16 nothing is.**
`stage()` mirrors every staged row onto the branch's CoW tree whenever the runtime has a page store
(`src/agent_sql/runtime.rs:967-978`), and `tests/integration_branch_pages.rs` drives exactly that
through the real scanner, parser, binder and executor in 9 tests. What was missing was the
*constructor call*: nothing in `src/` built a storage-backed runtime. `src/cli/cli.rs` now does, and
this demo now does too, so Act II runs on pages.

**Reading "criterion 2 is MET" as "branch isolation is enforced by shadow paging" is now correct,
and the demo proves it rather than asking to be believed.** Criterion 2 reads the branch's own
copy-on-write tree and trunk's, and prints both counts — 1 row on the branch, 0 on trunk. Run the
demo against `AgentRuntime::new()` instead and criterion 2 reports **NOT MET**, with the reason
printed, which is what makes the MET a measurement rather than a default.

This section said the opposite until 2026-08-16. It was accurate when written — the constructor
genuinely had no caller — and it is corrected here rather than deleted, because a reader who saw the
earlier claim deserves to know which sentence changed and why.

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

The same claim is measured through the SQL runtime rather than the branch engine in
`tests/integration_zero_copy_fork.rs`: the trunk is populated with `AgentRuntime::put_row`, and
`begin_session` is checked to copy zero pages at 200, 400 and 1200 rows, with the forked branch
then asserted to still read all 400 rows — a fork that copied nothing and saw nothing would pass
a page count while failing the point.

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

Three things make this stronger than the numbers alone:

- **A control scan runs first, before the leases expire.** A reaper that simply freed every branch
  it was pointed at would produce exactly the same "returns to baseline" numbers, and would be
  catastrophic. The identical scan run at the current time reaps **0 branches** and leaves the page
  count at 230. So it is the *lease* that frees pages, not the act of scanning — the detector is
  shown not to fire spuriously before it is trusted when it does fire.

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

Nothing is published, and with **this** guard the counter never goes negative.

**The measured boundary — and it is the sharpest limitation in the system.** Read `qty >= 12` above
carefully: the guard names *the amount being taken*, not the invariant. Write the invariant the way
almost anyone would — `qty >= 0`, "never let stock go below zero" — and the same scenario ends at
**−4**:

```
agent-a:  UPDATE inventory SET qty = qty - 12 WHERE qty >= 0 AND id = 1;   -- merges, 20 -> 8
agent-b:  UPDATE inventory SET qty = qty - 12 WHERE qty >= 0 AND id = 1;   -- ALSO merges
FINAL qty on main:  -4
```

A guard is a **precondition**, re-evaluated against the merged state as it stands *before* this
branch's ops. `8 >= 0` is satisfied, so the merge is admitted, and only then does the composed
`−12` cross the bound. A precondition does not imply the postcondition, and ferrobranch has no
declarative `CHECK` constraints, so nothing else enforces the invariant either.

**ferrobranch enforces the predicate the agent wrote, not the invariant the schema means.** That is
a real limitation, not a phrasing quibble: the correct guard for a bounded counter is
`qty >= <amount being taken>`, and getting it wrong fails silently and publishes a wrong number.

It is pinned by
`tests/guard_precondition_probe.rs::a_floor_guard_does_not_protect_the_floor_and_the_counter_goes_negative`,
which asserts the `−4` so the boundary cannot move without someone deleting that test on purpose.

Worth stating plainly because it contradicts the design: `DESIGN.md` section 3 says bounded counters
"need no special merge logic — compose the `Add`s normally, then re-evaluate the guard against the
merged state. If `qty >= 0` now fails → `Conflict`." That is the exact guard above, and
re-evaluating it does **not** fail. The design's own worked example does not hold. The mechanism
that would fix it is escrow at fork (`DESIGN.md` section on bounded counters / ledger D5), which
converts this into a refusal at **write** time instead of a wrong number in main.

### 10 — causal revert through read-sets

Agent A changes row 1 and merges. Agent B **reads** row 1, then writes row 2 on the strength of what
it read. Reverting A's merge halts by default and names B as the blocker — an edge that exists only
because the read was retained. **B never wrote row 1.** Without read-sets there is no edge at all
and the revert would silently corrupt B's work. `CASCADE` then undoes B first, then A.

---

## What this does not do yet

Listed because each one bounds a verdict above.

### The layers are not wired together

1. **SQL *statements* do not write CoW pages.** `stage()` still writes a per-branch `BTreeMap`, so
   Act II's isolation is in-memory rather than shadow paging. This remains the largest gap in the
   system. A page-backed row path exists alongside it (`AgentRuntime::with_storage` + `put_row`),
   and criteria 1 and 8 are measured on it; what is missing is routing statements through it.
2. **`BEGIN AGENT SESSION` allocates no pages.** This used to hold only because the SQL layer had
   no pages to allocate, which made the zero uninteresting. That is no longer so: with
   `with_storage` the trunk holds real pages written through the runtime, and the fork is measured
   to copy zero of them at 200, 400 and 1200 rows — with the page counter proven to move while
   populating, so the zero is a property of forking rather than of an idle counter.
3. **The SQL surface's branch catalog and effect log are memory-backed by default** — but the two
   are memory-backed for different reasons, and the difference matters:
   - `AgentRuntime::new` uses `LogBranchCatalog::in_memory`. That *is* the durable branch-engine
     implementation (generation counters, append-only record log, id release on reap); it is simply
     constructed without a file, because `Session::new` carries no path. `LogBranchCatalog::open`
     plus `AgentRuntime::with_catalog` gives the same code a disk. `tests/integration_sql_on_durable_branches.rs`
     runs the surface that way.
   - The effect log is `MemEffectLog`, which is a memory implementation rather than a durable one
     configured in memory. Nothing persists captured frames today.

### Criterion 8's mechanism is proven; its scheduling does not exist

4. **No background thread runs in a shipped ferrodb process.** `grep -rn 'thread::spawn' src/`
   returns two matches, both inside a `#[cfg(test)]` module in `src/replication/sync.rs`, so they
   do not exist in a release build — this document previously said the grep returned nothing, which
   stopped being true when synchronous commit gained its tests. `Reaper::reap_expired(now_millis)`
   is a method that something must call, and outside tests and this demo, nothing calls it. The non-cooperative *mechanism* is real and proven — the
   demo drives it exactly as a scheduler would — but no scheduler is shipped.
5. **The demo supplies the clock reading.** `reap_expired` takes `now_millis` explicitly, so the
   demo passes `now + 100s` rather than sleeping through a 10-second lease. The deadlines are real
   and really compared; only the clock reading is injected. This is the reaper's own design, not a
   demo shortcut, but it does mean the demo does not prove wall-clock expiry.
6. **An abandoned SQL session never frees pages, and on the page-backed path that now leaks.**
   `AgentRuntime::abandon` marks the branch reaped through the `BranchCatalog` trait only; it does
   not invoke `TwoTierReaper`. On the map-backed runtime this demo uses there is nothing to free.
   On a `with_storage` runtime there is: staging allocates real arena pages, and nothing in `src/`
   calls `reap_expired`, so those pages stay allocated until some caller outside `src/` reaps them.
   This document used to say "since Act II holds no pages, there is nothing to free", which is true
   of this binary and not of the path the tests exercise.

### Criterion 9 was PARTIAL; what closed it, and what still bounds it

7. **The executor now stamps.** It previously did not: `grep -rn 'provenance' src/execution/`
   returned zero matches, so a row a `MERGE` published — a real tuple at a real `RecordId` — could
   not be attributed at all. The publish path now carries the authoring run down to
   `Modify::set_author`, and `Insert`/`Update`/`Delete` stamp the version they write. The demo asks
   the question **both** ways: of the runtime, keyed by row id, and of the stored version, keyed by
   `RecordId`. Removing the wiring drops the demo's own verdict back to `PARTIAL`, so the `MET` is
   computed rather than asserted.
8. **Two things stay deliberately unattributed, and both are checked.** A write made outside any
   agent session reads back as `ProvId::NONE`, and a `REVERT` is not attributed to the agent whose
   work it undoes — a revert is not that agent's write. Abstaining is the correct answer in both
   cases; a provenance system that over-claims is worse than one that admits it does not know. The
   demo asserts the unattributed case against a row that **exists** (it inserts one with no agent
   open), because asking about a missing row would satisfy the same check while proving nothing.
8b. **A run cannot be re-interned, so the run-level guarantee holds by refusal.**
   `RunEntity::same_actor` compares `started_at`, and two sessions for one run necessarily start at
   different instants — so a second `BEGIN AGENT SESSION` for the same `(agent, run)` is rejected
   rather than reusing the id. One run is therefore never split across two entities, but by the
   session failing loudly, not by the store returning the existing id as its trait doc describes.
   Relaxing `same_actor` would silently convert that refusal into a second entity for one run.
9. **Row identity is derived from the primary key.** `agent_sql::runtime::row_id_of` hashes the
   first column because no layer mints surrogate row ids yet. `DESIGN.md` is explicit that the PK is
   a *constraint*, not identity, so **updating a primary key would currently look like a delete plus
   an insert**. Every caller goes through that one function, so there is a single place to fix it.

### Not smaller: a floor guard does not protect the floor

9b. **A bounded counter can be driven past its bound by a guard that names the bound.** Two agents
    each taking 12 from 20 under `WHERE qty >= 0` both merge, and main ends at **−4** — measured,
    not reasoned about, and pinned by a test. Guards are preconditions re-evaluated against merged
    state; `8 >= 0` passes and the composed decrement then crosses zero. There are no declarative
    `CHECK` constraints, so nothing enforces the invariant the schema means. Full working in
    "7 — the violated predicate comes back" above, including where it contradicts `DESIGN.md`
    section 3. This is the concrete case for escrow claims at fork.

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
**699 passing, 0 failing** (`cargo test`), of which 5 were added here for criterion 9.

Those 5 were mutation-checked: deleting the one line that records authorship
(`state.row_author.insert(...)`) makes 4 of them fail. A test that cannot fail proves nothing, so
this was confirmed rather than assumed.

| Area | Tests |
|---|---|
| SQL surface, criteria 2–7, 10 | `tests/agent_sql_surface.rs` |
| Criterion 9 at the SQL surface | `tests/agent_sql_surface.rs` (authorship + `MODEL` clause) |
| Criterion 8, page counts | `src/branch/reaper.rs` (unit tests, crate-internal harness) |
| Criterion 9 at the storage layer | `tests/provenance_e2e.rs` |
| CoW tree over arenas, collapse | `tests/integration_cow_branch.rs` |
| Durable branch engine under SQL | `tests/integration_sql_on_durable_branches.rs` |
| Effect log, merge agreement | `tests/integration_effect_log.rs`, `tests/integration_merge_agreement.rs` |
