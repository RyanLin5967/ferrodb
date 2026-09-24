# D194 step 4 — `REBASE`: PRE-REGISTERED expected results (written before anything was built)

Quiet mode (Ryan, 03:26Z): no cargo in any form. **Everything in this lane after `cb41d29` is
UNBUILT** — written from source, committed with "UNBUILT" in the subject, and never compiled.
This file is the prediction; the run that checks it has not happened yet.

## Semantics chosen

`REBASE;` / `REBASE BRANCH b_n;` re-pins a live branch's `fork_snapshot` and `fork_seq` to main
as it stands **iff nothing the branch depends on moved** between its old pin and now:

1. every staged row's base image (`base_rows`) equals the row's image at the new instant;
2. every exact read premise's version equals the version visible at the new instant (the rule the
   read-premise gate applies at MERGE; scan premises are approximate there and are not checked here
   either);
3. every table's shape in `base_shapes` equals the catalog's current shape.

Otherwise it REFUSES atomically: pin, `fork_seq`, staged rows, premises all unchanged, and the report
names what moved. **It never rewrites a staged row.** A quarantined branch is refused outright (the hold
preserves the evidence the gate judged). Reasons are in `frontier/lane_d194_fork_snapshot.md` §8.

## Commit 1 — tests only (`tests/d194_rebase.rs`), no implementation

The target compiles against existing APIs only (it reads results by column name through
`AgentOutput::to_rows`). **All 7 tests FAIL**, each for a named reason:

| test | fails where | message contains |
|---|---|---|
| `rebase_refreshes_the_base_a_branch_reads` | first `REBASE;` (`db.ok` panics) | `REBASE; failed:` … `expected a statement` |
| `staged_edits_survive_a_rebase_and_still_merge` | first `REBASE;` | same |
| `a_staged_edit_whose_base_moved_is_reported_and_kept` | first `REBASE;` | same |
| `a_moved_read_premise_refuses_the_rebase_and_a_held_one_does_not` | first `REBASE;` (C's) | same |
| `a_childs_rebase_moves_the_child_and_never_its_parent` | first `REBASE;` (the child's) | same |
| `rebase_refuses_a_quarantined_branch` | the `contains("quarantined")` assertion | `refused for the wrong reason` (the error is the parse error) |
| `rebase_outside_a_session_names_the_missing_branch` | the `contains("no agent session")` assertion | the parse error text |

The assertions BEFORE the first `REBASE` in each test (fixture checks, and "the branch reads as of its
fork") PASS at this commit, because D194 is already in the tree. If any of them fails, the fixture is
wrong, not REBASE.

## Commit 2 — the implementation

- `tests/d194_rebase.rs`: **7 passed, 0 failed.**
- New lib unit tests: `parser::…::rebase_parses_with_and_without_a_branch_and_is_not_a_reserved_word`,
  `binder::…::test_bind_rebase_defaults_to_the_session_branch_and_refuses_without_one`,
  `agent_sql::dispatch::…::a_rebase_report_row_matches_its_declared_columns` — **all pass**.
- Lib + the 54 agent-session targets (+ `d194_rebase`, which now matches the grep, making 55):
  **56 result lines; the same 2 failures as the run of record and no others.**
  - `integration_capability_envelope::the_envelope_reads_one_funnel_while_three_reach_branch_state`:
    now reads **`workspaces.get_mut(` occurs 5 time(s) … expected 3**. The fifth is `State::repin`.
    Classified as before (a tripwire D194 trips on purpose; the allowlist is the lead's call).
  - `replication::sync::tests::a_concurrent_forget_does_not_take_the_ack_with_it`: under
    `taskpolicy -b` it is expected to fail as in the run of record (QoS), unrelated.
  - Passed count: 2135 (run of record) + 1 (§6.3 cherry test) + 7 (d194_rebase) + 3 (new unit tests)
    = **2146 passed**, 2 failed, 2 ignored.
- `cargo build --examples`: compiles (no example names a changed signature).

A compile error at commit 2 is a failure of this pre-registration, not a thing to fix silently: record
it here, then fix it in its own commit.

## Amendment 1 (append-only, before any run): the QoS the run is made at changes the expected count

`frontier/FAN-QUEUE.md` row #3 now says to run the D194 targets at **DEFAULT QoS, never
`taskpolicy -b`**, because `-b` fabricates the `sync.rs:402` failure (lane report §5.2). The counts
above assumed `-b`. Both, so neither can be picked after the fact:

| QoS | result lines | passed | failed | ignored | the failures |
|---|---|---|---|---|---|
| default (the queue's instruction) | 56 | **2147** | **1** | 2 | the envelope tripwire (`get_mut(` count 5, expected 3) |
| `taskpolicy -b` | 56 | 2146 | 2 | 2 | the envelope tripwire + `sync.rs:402` |

Arithmetic: 2135 passed at `2eeed40` (run of record, under `-b`) + 1 (`sync.rs:402`, default QoS only)
+ 1 (`a_pick_onto_a_row_the_target_deleted_is_refused_rather_than_resurrecting_it`) + 7
(`d194_rebase`) + 3 (parser, binder and dispatch unit tests) = 2147.

## Amendment 2 (append-only, written BEFORE the tests it describes): the three cases §8.5 listed as untested

Still quiet mode: every commit below is UNBUILT. Order: this amendment, then the seam, then each test,
then the one behaviour change (C2), each its own commit.

### A. The retryable error when the branch or main moves between validation and commit

**Seam: a function split, not a hook.** `AgentRuntime::rebase` becomes `rebase_validate` (quarantine
check, the first lock section, shapes, staged rows) followed by `rebase_commit` (the re-check, the
premises, the re-pin), both private, with `rebase` calling one then the other. That split is
PRODUCTION code, so no test-only code sits in the production path and nothing there needs compiling
out. The only test-only code is the new unit test, inside runtime.rs's existing `#[cfg(test)] mod
tests`. It calls the two phases directly and lands the change in between on the same thread, with no
timing.

New lib unit test `rebase_is_refused_retryably_when_the_branch_or_main_moves_between_its_phases`, three
cases plus a control, all in one test:

| between the phases | expected from `rebase_commit` |
|---|---|
| nothing (the control) | `Ok`, `rebased = true` |
| main's merge clock advances (`apply_seq += 1`, which is exactly what a publish's reservation does) | `Err` containing `moved while REBASE was validating`; the pin, `fork_seq` and staged set are unchanged |
| the branch stages a write (`AgentRuntime::write`, an UPDATE matching one row) | `Err` containing `moved while REBASE was validating`; unchanged |
| the branch is abandoned (`AgentRuntime::abandon`) | `Err` containing `was sealed while REBASE was validating` |

- **At the commit that adds it: PASSES.** The re-check exists since `be26d5f`, so there is no honest
  red state. The discriminating run is the fire-check instead: with the re-check's `unchanged` forced
  to `true`, the two `moved while` cases FAIL (commit returns `Ok`) and the `sealed` case still PASSES,
  because that one is the workspace-existence check, a separate line.
- **"Compiled out of release":** the test is under `#[cfg(test)]`, which only `cargo test --lib`
  sets. To confirm once fans are allowed:
  - `nm -a target/debug/deps/ferrodb-<hash> | grep -c rebase_is_refused_retryably` → ≥ 1 (the
    positive control, from the lib test binary);
  - after `cargo build --release`, `nm -a target/release/ferrodb | grep -c rebase_is_refused_retryably`
    → 0.

### B. `rebase_key`'s no-key path: an inherited insert-then-delete

`tests/d194_rebase_unkeyed.rs` (new file). The parent INSERTs `(7, 70)` and then DELETEs it, which
stages `(base None, Deleted)` with a `RowCreate` in the parent's frame. The child forked from the
parent inherits that row with an EMPTY frame, so no image names key 7: this is the no-key path.

- `a_child_that_inherited_an_insert_then_delete_rebases_when_main_left_the_key_alone`: main changes
  only row 2. Expected after C2: the child REBASEs (`rebased`), and so does the parent (through its own
  `RowCreate`).
- `a_child_that_inherited_an_insert_then_delete_is_refused_when_main_took_the_key`: main INSERTs
  `(7, 77)`. Child and parent are both refused with `moved_rows = 1`, and neither view changes. This is
  the control that stops C2 from becoming "a keyless row always holds".

**C2, the behaviour change:** as of `be26d5f`, a row with no recoverable key is counted as moved, so a
child holding an inherited insert-then-delete could NEVER rebase. After C2, such a row is looked up by
row id in ONE full scan of its table at the new instant, cached per table per REBASE. It holds iff the
row is absent there as well, and is moved iff present. This costs O(table) for this rare path only; the
keyed path stays a point lookup.

- **At the test commit (before C2):** the first test FAILS at the child's REBASE (`rebased = false`,
  `moved_rows = 1`), and the second PASSES.
- **After C2:** both PASS.

### C. `REBASE` over pgwire: extended protocol (Parse/Bind/Describe/Execute/Sync), next to simple query

`tests/d194_rebase_pgwire.rs` (new file), driving `pgwire::extended::dispatch` with real message bodies
and `Statement::parse_batch` + `execute` for the simple-query path, the harness `d54_as_of_in_session`
uses.

- `rebase_over_the_extended_protocol_describes_and_returns_one_row`:
  - Messages in order: ParseComplete, BindComplete, `RowDescription(branch, rebased, fork_seq_before,
    fork_seq_after, moved_rows, moved_premises, moved_shapes, detail)`, one DataRow, `CommandComplete(SELECT 1)`,
    ReadyForQuery.
  - The DataRow's text values from `rebased` on: `t, 0, 0, 0, 0, 0, NULL`.
  - Then the simple-query path returns the same 8-column row, with `rebased = true`, and the rebased view.
- `a_refused_rebase_over_the_extended_protocol_is_a_row_not_an_error`: a staged row whose base moved
  gives DataRow `f, 0, 0, 1, 0, 0, <detail naming inventory>` and `CommandComplete(SELECT 1)`, with no
  ErrorResponse.
- **At the commit that adds it: both PASS.** The describe arm exists since `be26d5f`, so again no red
  state. Fire-check: with `| Stmt::Rebase { .. }` removed from `describe_stmt`, Describe answers NoData
  and Execute returns `ErrorResponse(XX000 … described the statement as returning no rows and then
  produced some)`. Both tests FAIL.

### Counts, replacing Amendment 1's

| QoS | result lines | passed | failed | ignored |
|---|---|---|---|---|
| default | 58 (56 + the two new files, which match the agent grep) | **2152** (2147 + 1 unit + 2 + 2) | **1** (envelope tripwire, `get_mut(` count **5**) | 2 |
| `taskpolicy -b` | 58 | 2151 | 2 (+ `sync.rs:402`) | 2 |

The seam adds no `workspaces.get_mut(`, so the tripwire's count stays 5.

## Amendment 3 (append-only, still before any run): A's fire-check, stated as it can actually be observed

Amendment 2 A said that with the re-check forced to `true`, "the two `moved while` cases FAIL and the
`sealed` case still PASSES". The first half is right. The second half cannot be observed: all four
cases run inside ONE test function, which stops at its first failure. So under that mutant the
`sealed` case never runs, and saying it "passes" would be a claim nothing measured. Replacing it with
two mutants, each of which removes one half of the re-check:

| mutant | edit | expected |
|---|---|---|
| A1 | the workspace half: `unchanged` forced to `true` | the control and the main-moved case PASS (the `apply_seq` half still refuses); the test FAILS at the staged-write case, with `unwrap_err` on an `Ok` |
| A2 | the clock half: `|| state.apply_seq != new_seq` removed | the control PASSES; the test FAILS at the main-moved case, with `unwrap_err` on an `Ok` |

The `sealed` case is the last step. It passes only when neither half is broken, and its own guard (the
workspace-existence lookup) is not a mutant here.

`cargo test --lib rebase` now selects 4 tests (parser, binder, dispatch and the race test): **4 passed.**

## Amendment 4 (append-only, written BEFORE the tests and code it describes): the lead's new-wall audit

The lead's audit (workflow `wf_56f0b71c-a89`) confirmed three terms that `0570fe8` ADDS and that grow
on a branch-count axis. This amendment fixes, before any of it is written, what each change is and what
each run must print. Everything below is UNBUILT under quiet mode.

### Term 1: `State::version_history` is never pruned (runtime.rs:1031, written at :1143-1151)

**Retention rule.** For each row, keep every `begin_ts` at or above the newest one at or below the
OLDEST LIVE PIN. With no live pin, keep only the row's newest entry. A pin at `F` reads the newest
version `<= F`. Every pin is `>=` the oldest one, so everything below that entry is unreachable. The
newest-only case is safe because a pin created later is `>= apply_seq`, and that is `>=` every
published `begin_ts`. The one exception is a child that inherits a live parent's pin, and that value
is already in the set.

**Finding the oldest pin, O(log n) per publish.** A multiset `pins: BTreeMap<fork_seq, count>` over
the live workspaces whose `fork_snapshot` is `Some`. It is maintained at the four places a pin enters
or leaves:
- `insert_workspace`, including the slot-recycle eviction inside it;
- `remove_workspace`;
- `State::pin`, on a lazy pin's `None -> Some`;
- `State::repin`.

A debug-only brute-force re-derive runs at each of those places. This is the `audit_txn_refs`
pattern, capped by `AUDIT_FULL_MAX` in the same way. The oldest pin is `pins.first_key_value()`.

**Freeing when the oldest pin leaves.** Trimming at publish alone would strand the history of a row
that is never published again. A set `trimmable: BTreeSet<(second-oldest entry, tbl, row)>` indexes
every row holding 2 or more entries. When the oldest pin is removed or re-pinned, the set is drained
from its minimum while the key is `<=` the new horizon. Each drained row loses at least one entry, so
the work is O(log n) per entry freed, and each entry is freed once.

These three structures live in ONE new `State` field, `retention`. That field is one more line for the
envelope allowlist (Ryan's ⚖2).

**The race this opens, and its guard.** A statement takes its pin under one lock acquisition, scans
with no lock held, and records its read-set under a second acquisition (`visible_rows_where`, then
`record_read`). An agent's own-branch `SELECT` runs on the shared read path
(`executor::try_run_read`). So a `REBASE` or `ABANDON` of that branch from another connection can land
between the two acquisitions. After that, the read's `seen_through` names a pin that is no longer
live, and a trim can drop the entry it needs. `version_seen` would then answer "no version"
(`begin_ts 0`) for a row the read did see. The premise check would still catch that, because the row
has moved. The provenance graph would not: `REVERT` would lose the edge to the merge that published
the version the read saw.

The guard reads resulting state. `retention.exact_from` is the highest horizon any trim has used. It
never decreases, and every live pin is at or above it. An `Inspection` read whose
`seen_through < exact_from` is REFUSED, retryably. The refusal says the snapshot the read went through
"was released while this read ran" and asks for a retry.

A `RowTargeting` read records no versions and is not refused. The same interleaving cannot happen for a
WRITE statement: writes, `REBASE`, `MERGE` and `ABANDON` all take `&mut Catalog` through `ExecCtx`, so
the type system serialises them. The blind spot is two `Catalog` instances over one database.

**Tests.** They live in `src/agent_sql/runtime.rs` `mod tests`. The first five name only fields and
functions that exist at `0570fe8`, so they compile there. `cargo test --lib version_history` selects
all of them.

| test | at the tests-only commit | at the fix |
|---|---|---|
| `version_history_stays_flat_under_a_publish_loop_with_no_live_pin`: 4 rows × 500 publishes, no workspace | **FAILS**: held 2000, bound 4 | passes (held 4) |
| `version_history_keeps_what_a_live_pin_reads_and_nothing_older`: 3 publishes, pin at 3, 100 more | **FAILS** on the bound: len 103 > 101. The version it names (3) is right at both commits | passes: len 101, names 3 |
| `version_history_is_freed_when_the_oldest_pin_is_sealed`: as above, second pin at 103, first removed | **FAILS**: len 103 after the seal, want 1 | passes: len 1, the second pin names 103 |
| `version_history_stays_flat_under_a_merge_loop_through_sql`: 40 × (pinned fork, UPDATE 2 rows, MERGE) | **FAILS**: held after 10 merges ≠ held after 40 (both grow by 2 per merge) | passes: 2 == 2 <= `versions.len()` (2) |
| `version_history_never_names_a_version_a_read_did_not_see`: pin 3, publishes 4 and 5, `repin` to 5, then `record_read(.., Some(3))` | passes: `Ok`, names 3 (nothing is pruned) | passes: `Err`, "was released while this read ran" |
| `version_history_pin_audit_fires_on_a_desynchronised_index` (`should_panic`, debug only; names the new field, so it lands with the fix) | n/a (does not compile there) | passes |

Tests-only commit: **4 failed, 1 passed**. Fix: **6 passed** in debug.

**Mutants** (edit, run `cargo test --lib version_history`, then `git checkout -- src/agent_sql/runtime.rs`):

| id | edit | expected |
|---|---|---|
| M11 | the reclaim on pin removal does nothing (`reclaim_history` returns at once) | `..._is_freed_when_the_oldest_pin_is_sealed` FAILS (len 103). `..._merge_loop_through_sql` FAILS on the bound (held 4 > 2: each row keeps its previous entry). The other two bound tests pass |
| M12 | `publish_version` trims against horizon `0`, which keeps everything but still indexes the row | `..._publish_loop_with_no_live_pin` FAILS (2000). `..._keeps_what_a_live_pin_reads...` FAILS (103). `..._merge_loop_through_sql` PASSES, because the seal's reclaim does the work. Recorded, not a gap: the publish-loop test covers trimming at publish |
| M13 | `State::pin` does not index a lazy pin | debug: the pin audit panics with "pins disagree with a scan" at the next door a lazily pinned branch crosses. Instrument: `grep -c 'pins disagree'` over the lib + agent-target run output is `>= 1` |
| M14 | the `exact_from` refusal in `record_read` is removed | `..._never_names_a_version_a_read_did_not_see` FAILS: `Ok` naming `begin_ts 0`, want 3 |

### Term 2: every branch statement scans main through its pin (a cost of ⚖1, stated)

`visible_rows_where` -> `scan_table_where(at = fork_snapshot)` -> `resolve_visibility`
(`wal/visibility.rs:3-15`) walks newest-first down the row's `prev` chain. It does one `tt_heap.read`
per version the pinned view cannot see, and those are exactly the versions committed to the row since
the pin.

**Cost per statement** = Σ over the rows it scans of (versions of that row committed since the pin). A
row nobody touched since the pin costs 0. A read of main as it stands costs 0, as it did at the merge
base.

It is bounded only by the pin's age in commits, and nothing caps that:
- `REBASE` is voluntary;
- the lease caps wall-clock time, not merges;
- a child inherits its parent's pin under a fresh lease.

Nothing prunes the chain: there is no vacuum, which is pre-existing.

This is the price of snapshot isolation on a newest-first version chain, and it is paid by the OLD
reader alone.
- **InnoDB** is the same shape: a consistent read of a row changed since its read view follows the roll
  pointer through one undo record per newer version.
- **PostgreSQL's long-running snapshot** has the opposite shape. It does not slow its own reads. It
  holds back vacuum's `xmin` horizon, so dead tuples pile up and every session's scans pay for them.

Here, main's readers pay nothing.

These prior-art statements come from general knowledge of the two engines' documented MVCC. I did not
re-read the manuals for this lane.

**Instrument.**
- `wal::visibility::VISIBILITY_HOPS`, a new counter. It is flushed once per `resolve_visibility` call
  and only when that call hopped, so a read that sees the head pays nothing extra.
- `examples/d194_pinned_read_cost.rs`, a new harness.

The harness uses 256 rows. An OLD branch is pinned at k = 0. Then k merges run, each an
`UPDATE t SET v = v + 1 WHERE id >= 1` merged from a fresh branch, and the harness stops at
k ∈ {0, 8, 32, 128}. At each point it times, over 5 repetitions, the OLD branch's `SELECT id, v FROM t`,
its point read `WHERE id = 1`, and the same two reads on a FRESH branch forked at k (the control).

Expected, as integers from `VISIBILITY_HOPS` (control flow fixes them, so load cannot move them):
- OLD full scan: **256·k** per statement (0, 2048, 8192, 32768).
- FRESH full scan: **0**.
- OLD point read: **k** if the planner uses the primary index, **256·k** if it scans. The harness prints
  the `SEQ_SCAN_TUPLES` delta to say which.
- FRESH point read: **0**.

The harness REFUSES (exit 1, naming the cell) on any mismatch, and on 0 hops for the OLD branch at
k > 0, which would mean the pinned path never ran. Latency is reported, not certified: the expected
shape is a positive slope in k for OLD and a flat line for FRESH.

### Term 3: a fork retains an `Arc<Snapshot>` for the branch's life

**Memory** = Σ over DISTINCT `fork_snapshot` Arcs of `|active|` at their creation, at
8 bytes/id + `HashSet` overhead (not measured). That is at most live branches × max concurrent
`TxnManager` transactions. `|active|` counts SQL transactions in flight (one per connection at most),
plus a merge's publish txn. It does not count branches, because an agent's txn ids never enter `att`.

Sharing is already free in two cases:
- a child of a live branch holds its parent's Arc;
- forks on one thread with no `TxnManager` begin or end between them hit `read_snapshot_cached`'s
  thread-local cache.

**Not bounded further, and why.** No cheap change alters the class. The snapshot is the minimum a pin
needs: PostgreSQL's snapshot likewise carries an xip array sized by its in-progress transactions. A
cross-thread intern keyed by `att_version` would only merge Arcs whose content is already identical,
and that removes a constant in one traffic pattern.

**Instrument.** `AgentRuntime::fork_snapshot_census()` returns
`(pinned live branches, distinct snapshots, active ids retained)`. It is O(live branches) under the
state lock, so it is for measuring and is on no statement path. The harness's second part holds
N = 16 plain transactions open and forks B = 64 branches:
- **SHARED**, back to back: census `(64, 1, 16)`.
- **CHURN**, with a main autocommit `INSERT` between forks: census `(64, 64, 1024)`.

The harness refuses on any other triple.

### Counts, replacing Amendment 2's

- `cargo test --lib version_history`: **6 passed** at the fix (debug).
- `cargo test --lib rebase`: still **4** (no new name contains `rebase`).
- Run of record at default QoS: **58 result lines, 2158 passed (2152 + 6), 1 failed, 2 ignored**.
  - The failure is still the envelope tripwire at `get_mut(` **5**: this change adds no
    `workspaces.get_mut(`.
  - Its State field allowlist, which the count assertion hides today, now also needs `retention` beside
    `version_history` (⚖2).
- Under `-b`: 2157 / 2.
- `cargo build --examples`: compiles, including the new example.
