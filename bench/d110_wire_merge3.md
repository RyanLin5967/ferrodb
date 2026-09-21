# D110 — should `cow::merge3` be wired into the production merge path?

**Verdict: NO. The row is retired, and nothing was built to keep it alive.**

The production merge is already proportional to the diff, and — a stronger fact that only the
measurement found — it does not touch the CoW tree at all. Wiring `merge3` into `MERGE;` is not a
performance change; it is a storage migration wearing one.

`[HERE]` = measured by this session, in worktree `/Users/idide/wt/ferrodb-D110-wire-merge3`.

---

## ⛔ Two premises in the brief are false, and correcting them is most of the answer

The brief said: *"the real merge path is `agent_sql::runtime::merge_into` (`runtime.rs:3615`) and
`runtime::merge` (`:3120`), which go through `tel::engine::ThreeWayMerger` — a ROW-level merge."*

**1. `ThreeWayMerger` has no production caller either.** It is in exactly the same position as
`merge3`: a mechanism nothing calls.

| evidence | result |
|---|---|
| `grep ThreeWayMerger src/agent_sql/runtime.rs` | two hits, **both doc comments** (`:3567`, `:3579`). No code path. |
| only non-test construction in `src/` | `src/tel/capture.rs:476` — inside `#[cfg(test)] mod tests`, which opens at `capture.rs:253` |
| the other `Merger` impl, `agent_sql::SurfaceMerger` | constructed only at `merge_engine.rs:581` (a test) and `tel/tests_durable_log.rs:480` (a test) |

The `Merger` trait has two implementations and **zero production callers between them.** The real
merge composes inline inside `AgentRuntime::evaluate_merge` (`runtime.rs:4024`) using
`agent_sql::merge_engine::{resolve_cell, compose_ops}` directly.

So "wire merge3 in place of the row-level merger" was never the available move: there is no
`Merger` on the path to replace.

**2. D68's reading — that `evaluate_merge` scans every row of each touched table and
`fingerprint_tables` scans it again — was true when written and is not true now.** It was
re-checked rather than inherited, as instructed:

* `fingerprint_tables` (`runtime.rs:5027`) hashes `ctx.catalog.table_version(*t)` per table. It
  reads **no rows**. O(tables).
* `evaluate_merge` full-scans only tables an **assertion** ranges over (`if
  assertion_tables.contains(t)`). A merge with no assertions — the common case, and the one
  `MERGE;` takes — scans nothing.
* The repo already recorded both closures: `bench/d69_fsync_counted.txt` ("D69 CLOSES. The merge is
  O(delta)") and `bench/d86_merge_degrades_with_merge_count.txt` (cost per cell O(k) → O(log k)).

---

## Deflation question 1a — what does `ThreeWayMerger` cost, literally as asked?

**O(ops since the fork). Never O(rows in the table) — it never reads the table at all.** It is
*handed* the two frame lists (`ours: &[TxnFrame]`, `theirs: &[TxnFrame]`) and merges a history, so
the table cannot appear in its cost:

* `dedup_by_txn`, then one `StampedOp` pushed per op into each `SideIndex` — O(ops).
* `compose` iterates the **union of touched row keys** and the **union of touched cell keys** —
  O(rows touched + cells touched).
* Two further passes over the frames for guards and claims — O(ops).

⚠ One real wart, reported because it was looked for: on the **delete-vs-write path only**,
`compose_row` (`engine.rs:712-715`) calls `touches_row` / `last_op_on`, and each is a linear scan
of that side's whole cell map (`self.cells.iter().filter(...)`, `engine.rs:269-283`). That makes
the delete path **O(rows touched × cells touched)** — quadratic, but quadratic *in the delta*. The
table size is absent from every term. It is dead code, so this is recorded rather than fixed.

So even the merger the brief believed was on the path is already proportional to the diff. The
verdict does not depend on which of the two mergers you ask about.

## Deflation question 1 — what does the real merge cost per merge?

**O(delta · log N), where delta = rows the branch touched. Not O(rows in the table).** Read from
source at `e7588cc`, not guessed:

| phase | cost | where |
|---|---|---|
| enumerate the delta | **O(delta)** — iterates `snapshot.rows`, the workspace's own touched-rows map | `runtime.rs:4270` |
| per touched row, read the target's current image | **O(log N)** — `scan_table_where` with a PK equality | `runtime.rs:4236` |
| per changed cell, find the concurrent effect | **O(log k)** — `partition_point` over the per-cell index | `concurrent_op`, D86 |
| staleness fingerprint | **O(tables)** — hashes a version counter, not rows | `fingerprint_tables:5027` |
| full table scan | only for tables an **assertion** names; none for a plain `MERGE;` | `runtime.rs:4180` |

The PK point lookup is a descent, not a scan — verified through the planner rather than assumed:
`has_index(entry, col)` returns true for `col == 0` (`optimizer.rs:148`), `predicate_to_bounds`
turns `Equal` into the point range `[v, v]` (`plan.rs:165`), and `build_index_scan` cost-compares
that `IndexScan` against a `SeqScan` and takes the cheaper.

**The delta is never derived by walking a tree — it is maintained by the workspace as the agent
writes.** That is the whole reason merge3 has nothing to offer here: skipping identical subtrees is
an advantage over a merge that would otherwise walk the tree, and no advantage at all over one that
never walks it.

## Deflation question 2 — does merge3 + MerkleId give the production path a shape it lacks?

**No — and the measurement found something stronger than "no".**

`[HERE]` `examples/d110_merge_page_reads.rs`, committed at `9a69757`. Counts
`PageStore::read_page` across the `MERGE;` statement **and nothing else**. Integers, not durations:
this box runs a build fleet, and D68/D69 burned three rows on a wall clock that wrapped `BEGIN
AGENT SESSION` + N `UPDATE`s + `MERGE` and reported the total as merge latency.

```
  table rows   merges   writes reads   MERGE reads   reads per 1000 rows
        1000       25            0.0           0.0                0.0000
        2000       25            0.0           0.0                0.0000
        4000       25            0.0           0.0                0.0000
        8000       25            0.0           0.0                0.0000
       16000       25            0.0           0.0                0.0000

counter fire-check — a path that MUST read branch-engine COW pages:
  64 x AgentRuntime::put_row  -> 110 read_page calls
  64 x AgentRuntime::get_row  -> 128 read_page calls
  -> the counter COUNTS. A zero in the MERGE column is a fact about the merge.
```

**A MERGE reads ZERO branch-engine pages at every table size. Not flat — absent.**

⚠ **A zero is not a pass, so it was not taken as one.** Pre-registered in the harness: a zero
merge column is ambiguous between "the merge reads no branch pages" and "the counter is dead", and
the run **refuses with exit 2** unless `put_row`/`get_row` — which descend the same `CowTree`
through the same wrapped store — are seen to move it. They moved it by 110 and 128. It also refuses
if no merge applied, or if fewer than two sizes were reached.

The mechanism, confirmed in source: `MERGE;` never reaches `PagedRows`. `self.rows()` has exactly
five call sites in `runtime.rs` — `put_row`, `get_row`, `delete_row`, `scan_rows`,
`page_changeset_with_cost` — and `evaluate_merge` is not one of them. The SQL surface keeps base
tables in ordinary heap/index pages; the CoW tree is a parallel store. `runtime.rs:1786` says so
itself, in the conditional future tense: *"when base tables live in the tree as well, the fork root
will hold real rows and this same call will return the same answer for a better reason."*

⇒ **`merge3` cannot be given a caller on the merge path without first moving base tables into the
CoW tree.** That is a storage migration, not a merge change, and it is a different row.

### Why it would also be a semantic downgrade

Even with the storage in place, merge3 resolves per key over serialised leaf values, while the
production path resolves per **cell** over an effect algebra. `merge3.rs`'s own module doc argues
this against itself and is right: base 10, ours `Add(5)`, theirs `Add(3)` composes **clean at 18**
by op-replay, and is row 5 of merge3's truth table — **CONFLICT** — structurally, because a state
cannot distinguish "assigned 15" from "incremented by 5".

## Deflation question 3 — does anything already bridge them?

**Yes, and it is the precedent that shows this row was already taken.** `cow::diff` — merge3's
sibling — *does* have a production caller: `runtime.rs:1850`, `cow_diff(rows.tree(), fork_root,
current, &PageIdentity)`, reached by `DIFF <branch>`, with its cost banked in
`bench/d103_production_diff_curve.txt`. `agent_sql/paged_rows.rs` is the bridge both would use.

`DIFF` is the operation that genuinely has no delta in hand and must find it from two roots, so the
synchronised descent buys it a real shape — and the banked curve shows exactly the shape merge3
claims, already realised in production:

```
      N   nodes  depth | visited  skipped  |     walked
                       |    (the claim)    |  (control)
-------  ------  ----- | -------  -------  |  ---------
   1000      67      2 |      10       62  |        134
  256000   18234      4 |      14      141  |      36468

N grew 256x.  visited grew 1.4x, tracking DEPTH 2 -> 4.  control grew 272x, tracking N.
```

`MERGE` already holds the delta, so there is no equivalent control for it to beat. **The structural
mechanism was wired to the one operation that needed it.** merge3's zero callers is that fact, not
an oversight — and building a second structural descent for the operation that does not need one is
the shape of the pitch this project has killed 35 times.

---

## The curve, re-banked

`bench/d92_merge3_curve.txt` is cited twice by `src/cow/merge3.rs` (`:153`, `:396`) as the evidence
for its cost claim. **It was not in the tree.** It was committed on `D92-merge3` (`3c4ca6c`), which
is not an ancestor of `main` — the example landed and its evidence did not. It is re-banked here
from a clean tree.

`[HERE]` merge3's own complexity claim **survives D89**, which is worth recording since the harness
had to be repaired to show it:

| arm | pages 1k → 256k | nodes_read 1k → 256k |
|---|---|---|
| contested | 259 → 65,672 (**×253.6**) | 27 → 39 (**×1.44**) |
| separated | 259 → 65,672 (**×253.6**) | 15 → 27 (**×1.80**) |

If the descent were O(N), `nodes_read` would have grown by the ×254 the page count did. It grew
×1.44, tracking depth (3 → 4).

### The RED, and a duplicate fix

`examples/d92_merge3_curve.rs` was RED on `main`: `assert_eq!(con_theirs, 0)`.

**Cause.** D89 made leaf boundaries content-defined (`cow::chunker`, "chunk on the key alone"), so a
boundary can fall between two adjacent keys. The contested workload picked keys `i` and `i+1` and
assumed they share a leaf; when they do not, a subtree belongs to one side alone and rules 2 and 3
fire in the arm whose only job is to show they cannot. Measured at `e7588cc`:
`skip_theirs_unchanged` read **2, 2, 1, 1, 0** across the five sizes where the pre-D89 banked curve
had 0 at every one.

⚠ **`cdc-boundary` reached the identical diagnosis independently on `D89-curve` (`94bc632`),
including the same straddle counts 2,2,1,1,0 summing to 6 against rule totals of 6.** Two
independent derivations agreeing on the mechanism *and* the integers is the strongest evidence
available that the mechanism is right. This session's fix (`d7a6f64`) was committed before that
message arrived; it was not a knowing duplication, and the work is not redone here.

**The two fixes differ, and the choice is the lead's.** `cdc-boundary` treats it as a stale
assertion and corrects the premise. This one keeps the assertion untouched and makes the workload
genuinely contested: the pairs are read **out of the leaves themselves** (two entries of one leaf,
co-resident by construction under any chunker), and `arm()` re-reads that premise off the tree and
asserts it, so a future layout change fails naming the cause rather than tripping a counter
assertion four hundred lines away. Preferring this one preserves what the assertion is *for* — it
is what makes the separated arm's nonzero counters readable as "the counter works" rather than "the
counter is stuck". Never edit a test to make it pass; the workload was wrong, not the assertion.

---

## What this row does NOT claim

* **The heap side is unmeasured.** The counter sees the branch engine only. It proves the CoW tree
  is untouched by a merge; it does not measure the ordinary-table reads, because
  `BufferPoolManager` exposes no public fetch counter in this build. A column for it was drafted and
  **deleted rather than shipped returning a constant zero** — a counter that cannot fire is worse
  than no counter, because its zero reads as evidence. The `writes reads` column is the partial
  control: same planner, same buffer pool.
* **Not a comparison against Dolt, Postgres or any other engine.** A shape, in this engine.
* **`D92-merge3-fix` (`204b40e`) is unreviewed by this session.** It is 531 lines of `merge3.rs`
  hygiene — deleting a duplicate `H128`, scoping the memo, guarding `read_node`. It does not add a
  caller: merge3 still has zero `src/` callers on that branch and on `D92-merge3`, checked with
  `git grep -c merge3 <branch> -- src/`. It neither supports nor weakens this verdict.

## Recommendation

**Retire the row.** Do not wire `merge3` into `MERGE`. If the CoW tree ever becomes the base-table
store, re-open it then — and re-open it as a question about `DIFF`'s precedent, which is where the
structural descent has already paid.
