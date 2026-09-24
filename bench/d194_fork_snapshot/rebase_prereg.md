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

## Amendment 5 (append-only, before any run; written BEFORE the code and test it describes): a fresh-context review of `0570fe8..7c92d8d`

The review was read-only, in a fresh context, and its report is in the lane report §9. It found no wrong
answer: under Amendment 4's rule no live pin loses an entry it reads, and it re-derived every
Amendment 4 test outcome by hand. It did find five defects in the design and in this document. Each is
corrected here before the code that fixes it.

1. **The per-seal cost claim was false.** Amendment 4 said reclaim costs "O(log n) per entry freed".
   But `trim_history` drained a `Vec<u64>` from the front, which moves every entry that stays. A seal
   therefore cost O(entries kept) per row.
   - **Fix:** a row's unreachable prefix is dropped only once it is at least half the vector. A drain
     then never moves more entries than it frees, which is amortised O(1) moves per entry freed. The
     price is holding at most as many dead entries as live ones.
   - **Index key:** `trimmable` now keys a row by the entry just after its oldest REACHABLE one, not by
     its second-oldest entry, which the dead prefix would make wrong.
   - **Unindexing:** the key is recomputed at the current horizon. That is sound because the horizon
     only ever rises, except when it leaves "no pin" (`u64::MAX`), and at that horizon no row is indexed.

2. **Amendment 4's rule keeps entries no pin can read.** It keeps everything at or above the oldest
   pin's version. In the harness, an OLD pin taken before every publish would hold
   256 × 128 = 32,768 entries and could read none of them. The tightened rule below is not the lead's
   rule restated.
   - **Tightened rule:** when `v` supersedes a row's newest entry `h` in sequence order, `h` stays only
     if a live pin lies in `[h, v)`. The check is `pins.range(h..v)`, O(log n).
   - **Why it is safe:** a pin taken later is at or above every `begin_ts` already published. An entry
     no live pin reads when it is superseded can therefore never be read.
   - **Unchanged:** the oldest-pin reclaim stays, for the entries a sealed pin DID read.
   - **Still unbounded, stated here rather than found later:** an entry kept for a pin that is then
     sealed while an OLDER pin lives stays until the oldest pin passes it. That is at most one entry per
     row per such pin.

3. **The guard is now membership, not a floor.** `exact_from` was a scalar that assumed nothing below
   the oldest pin is kept. With (2), entries between pins are dropped too, so the history answers
   exactly only for LIVE PIN VALUES.
   - **Rule:** a read is refused iff `seen_through` is not a key of `retention.pins`, checked in
     O(log n).
   - **Not refused:** a read whose own pin moved, while another live branch still pins that value (a
     child that inherited it, say). It is answered exactly.
   - `exact_from` is removed.

4. **The race's route was misstated** in the `record_read` comment and in Amendment 4. They said "from
   another connection", and that `ABANDON` is serialised by `&mut Catalog`. Both were wrong.
   - **Over pgwire the race cannot happen.** Each connection registers a read slot
     (`pgwire/mod.rs:439`), and a shared-path `SELECT` holds its read pass across all of
     `try_run_read`. Every exclusive statement's `ServerContext::catalog()` first drains the registered
     readers (`drain_readers`).
   - **In-process it can.** `AgentRuntime::abandon(&self)` and `forget_reaped_branches(&self)` take no
     `ExecCtx`. A library caller that reads through a catalog clone can race them, or can race a
     `REBASE` made under a different catalog handle.
   - **The guard stays**, with the route named correctly.
   - **Writes are unaffected.** Writes and `REBASE` both take `&mut Catalog` through `ExecCtx`, so they
     are serialised with each other. A write whose branch is sealed mid-statement already refuses in
     `stage_all` ("no agent session").

5. **The harness was not d55's `exec`.** It made a fresh catalog cache and an unregistered read slot on
   every call, so every statement took the exclusive path and cloned the catalog.
   - **Fix:** each session now carries its own cache and registered slot, as `d55_agent_read_scaling`'s
     worker threads do.
   - **What it affected:** the hop integers never depended on this; the latencies did.
   - **Also added:** the harness now prints the `SEQ_SCAN_TUPLES` delta it names, and REFUSES a seqscan
     point read whose delta is not `ROWS`.

**Correction to Amendment 4's M11 integer:** under M11, `..._is_freed_when_the_oldest_pin_is_sealed`
held 101 entries, not 103, because publishes 1–3 still trim at publish time. The verdict (FAILS) was
right. The table below replaces Amendment 4's mutant table.

### Tests, replacing Amendment 4's table (all in `runtime.rs` `mod tests`)

Amendment 4's six tests stand, and one is added. The new test names only fields and functions that exist
at `0570fe8`, so its red state can be observed at `0570fe8` AND at `7c92d8d`, which carries Amendment 4's
rule alone.

| test | `0570fe8` | `7c92d8d` | fix |
|---|---|---|---|
| `version_history_holds_only_the_versions_live_pins_read`: row 1 gets 3 publishes, a pin at 3, then 100 more; row 2 gets 100 publishes after the pin | **FAILS** at row 1: len 103, bound 2 | **FAILS** at row 1: len 101, bound 2 | passes: row 1 is `[3, 103]` and names 3; row 2 is `[latest]` |

The other six have the same outcome at every commit. At the fix they derive as follows:
- keeps-what-a-pin-reads: len 2, names 3.
- freed-at-seal: len 1, names 103.
- flat publish loop: held 4.
- flat SQL loop: 2 == 2.
- never-names: refused, because 3 is not in pins `{5}`.
- pin audit: panics.

`cargo test --lib version_history` selects **7**, and all 7 pass at the fix in debug.

### Mutants, replacing Amendment 4's

| id | edit | expected |
|---|---|---|
| M11 | `reclaim_history` returns at once | `..._is_freed_when_the_oldest_pin_is_sealed` FAILS (len 2, want 1). `..._merge_loop_through_sql` FAILS on the bound, since every row keeps at least 2 entries and held is then ≥ 4 > 2. The other five pass. |
| M12 | the in-order supersession never pops (no `pins.range` check) | `..._holds_only_the_versions_live_pins_read` FAILS (row 1 len 101). The flat publish loop PASSES because the no-pin trim covers it: recorded, not a gap. |
| M13 | `State::pin` does not index a lazy pin | debug: "pins disagree with a scan" in at least one lib or agent-target test (`grep -c 'pins disagree'` ≥ 1). |
| M14 | the membership refusal in `record_read` is removed | `..._never_names_a_version_a_read_did_not_see` FAILS: `Ok`, naming `begin_ts 0`, want 3. |

### Counts, replacing Amendment 4's

- Run of record at default QoS: **58 result lines, 2159 passed (2152 + 7), 1 failed, 2 ignored**.
  - The failure is the envelope tripwire at `get_mut(` **5**. Its State allowlist also needs
    `version_history` and `retention`.
- Under `-b`: 2158 / 2.
- `cargo test --lib rebase`: 4.
- `cargo build --examples`: compiles.

## Amendment 6 (append-only, before any run; written BEFORE the code and tests it describes): a pin taken while a merge publishes

The same review found a D194 defect that already existed at `0570fe8` (its F6). I verified it from
source myself before writing this.

**The defect.** `merge` does four things, in this order:
1. It reserves its sequence numbers under the state lock, which moves `apply_seq`.
2. It releases the lock.
3. It begins and commits the publish transaction.
4. Only then does it record the versions (`record_applied`).

A pinned fork that lands between steps 1 and 3 takes `fork_seq = apply_seq`, so its seq already
includes the reserved numbers. But its snapshot was taken before the publish committed, so the
snapshot does not contain those versions. The branch then has two wrong beliefs:
- **At merge**, it treats the op at seq `b` as already in its base. `concurrent_op` filters on
  `seq > fork_seq`, so it never sees `b`. The branch's rows were derived from the image before `b`,
  so `b`'s change can be overwritten: a lost update.
- **At read time**, `version_seen(F >= b)` names version `b` for a read that saw the image before
  `b`. The premise check then compares `b` against itself and passes.

That is exactly the failure D194 exists to prevent.

**Reachability.**
- Not over pgwire. MERGE and BEGIN AGENT SESSION both hold the exclusive catalog for the whole
  statement.
- In-process, yes. `begin_session_pinned` needs only `&TxnManager`, so a library caller on another
  thread can fork inside another thread's merge.
- A lazy pin (`State::pin`) has the same window. REBASE's `rebase_validate` takes its new instant in
  the same way.

**The rule.** A pin never claims a version its snapshot cannot see.
- A new `State::publishing: BTreeMap<reserved.start, Option<publish txn>>` holds every merge that
  has reserved numbers but not yet recorded them:
  - the entry is inserted under the reservation's lock;
  - it gets its txn id once `begin` returns;
  - `record_applied` removes it, under the SAME lock acquisition that records the versions;
  - on every other exit, an RAII guard in `merge` removes it.
- A pin's seq is computed from the snapshot it pairs with:
  `min(reserved.start over entries whose txn the snapshot does not include)`, or `apply_seq` when
  there are none.
- An entry whose txn is still `None` is not committed. It is registered before `apply_in` and
  `commit`, and registering needs the lock the pinning code holds. So it counts as excluded.
- **The same code for every door.** It is one free function, called by the fork
  (`fork_session_staged`) and the lazy pin (`State::pin`). A child still inherits its parent's
  pair.
- **REBASE** refuses, retryably, while any merge is publishing. It checks at validation and again
  at commit. That is simpler than threading the computed seq through its commit re-check, and with
  one catalog it can never fire, because REBASE and MERGE both take `&mut Catalog`.

**Why it is exact with one catalog.** A merge holds `&mut Catalog` from its reservation to its
record, so at most one entry exists at a time. Every version already recorded is at or below that
entry's `reserved.start`. A pin at `reserved.start` therefore reads every recorded version as it
is, and treats the in-flight merge as "theirs". That matches its snapshot.

**The retention rules still hold:**
- A new pin is at or above every `begin_ts` already published.
- When the in-flight merge records `b` over `h`, the new pin lies in `[h, b)`, so `h` is kept.

**Blind spot, stated.** Two catalogs over one database could put two merges in flight at once.
- If they commit out of order, an INCLUDED entry can sit above an excluded one, and no single seq
  is consistent.
- The pin takes the lower value, which treats the included merge as "theirs" although it is in the
  base. A `debug_assert` names the case. It is not reachable with one catalog.

### Tests, both in `runtime.rs` `mod tests`

Both name `publishing`, so they cannot compile at `0570fe8`. They are MUTANT-ONLY red.

| test | expected at the fix |
|---|---|
| `a_pin_taken_while_a_merge_publishes_does_not_claim_its_versions` | passes. Setup: reserve 2 (`base` = the old `apply_seq`), register, and begin the publish txn `t` without committing it. A pinned fork then gets `fork_seq == base` with a snapshot that excludes `t`. A lazy pin of a ctx-less branch also gets `base`. After `commit(t)`, a new fork gets `base + 2` with a snapshot that includes `t`. |
| `rebase_is_refused_while_a_merge_publishes` | passes: `rebase_validate` returns `Err` containing "is publishing". With the entry removed, it validates. |

### Mutants

| id | edit | expected |
|---|---|---|
| M15 | `pin_seq` returns `apply_seq` | `a_pin_taken_while_a_merge_publishes...` FAILS: `fork_seq` is `base + 2`, want `base` |
| M16 | `merge` never inserts its `publishing` entry | debug: `record_applied`'s `debug_assert` ("recorded versions it never registered") fires in every merge test. Instrument: `grep -c 'never registered'` ≥ 1 over the lib and agent-target run |
| M17 | `rebase_validate`'s publishing check is removed | `rebase_is_refused_while_a_merge_publishes` FAILS: `Ok` where `Err` was expected |

### Counts, replacing Amendment 5's

- `cargo test --lib version_history`: 7.
- `cargo test --lib rebase`: **5**. That is the 4, plus `rebase_is_refused_while_a_merge_publishes`.
- Run of record at default QoS: **58 result lines, 2161 passed (2152 + 9), 1 failed, 2 ignored**.
  The failure is the envelope tripwire at `get_mut(` **5**. Its State allowlist now also needs
  `publishing`, next to `version_history` and `retention`.
- Under `-b`: 2160 / 2.

## Amendment 7 (append-only, before any run; written BEFORE the code it describes): second fresh-context review, of `7c92d8d..3c52476`

The review found no wrong answer, no deadlock and no compile error. It did find the following, and each
is corrected here.

1. **Registration came in two halves, and either could be deleted unseen (Medium).**
   - **The defect.** `merge` inserted `(start, None)` at the reservation and then `(start, Some(t))`
     after `begin`. M16 deleted both at once, so neither half alone was caught. The test also wrote the
     protocol out by hand instead of calling the code `merge` calls.
   - **Fix, part 1: one registration.** The publish transaction now begins BEFORE the reservation. The
     reservation registers `(start, t)` in one call, `PublishingEntry::register`, which also arms the
     cleanup, so registration and guard are one statement.
   - **Fix, part 2: the map loses its `None`.** It becomes `BTreeMap<start, txn>`, so there is no `None`
     case left to test.
   - **Order and visibility.** Beginning earlier changes nothing a snapshot can see, because visibility
     comes at commit. A fork between `begin` and the reservation sees neither the reservation nor the
     transaction, and pairs its snapshot with the old `apply_seq`: consistent.
   - **The refused reservation** (`fresh_reservation`) now aborts the transaction it began.
   - **The tests** register through `PublishingEntry::register` and release by dropping the guard, so
     a mutant in either is theirs. Their assertions are unchanged.
   - **Still unkilled, stated:** deleting the `_publishing` binding in `merge` (keeping the insert)
     would strand an entry only on a failed publish. No test fails a publish after its reservation
     in-process. With `Some(t)` always present, a stranded entry is harmless to pins, since an aborted
     txn is contained. It would, however, refuse every later `REBASE`. Recorded as a known unkilled
     mutant: M18.

2. **`pin_seq`'s `debug_assert` could fire on a legitimate path (Low).** A merge with zero ops reserves
   `base..base`. A pin between its commit and its record saw `start == seq` for a contained entry. The
   assertion now checks only what it names: when some entry is EXCLUDED, every contained entry lies
   below the lowest excluded start.

3. **The two-catalog detector covered less than claimed (Low).** A second check sits beside it: the
   last recorded version (`applied.last()`, O(1)) must be at or below the pin's seq. With one catalog
   this always holds. When B records while A is still in flight, it fires. Both callers pass it.

4. **A pin taken between a merge's commit and its record claims versions the history cannot name yet
   (Low, in-process only; already present at `0570fe8`).**
   - The snapshot contains the merge and `pin_seq` gives `E`, which is correct. But `versions` and
     `version_history` do not have the merge until `record_applied`.
   - A read recorded in that gap would name the superseded version.
   - **Fix:** `record_read` also refuses an Inspection read whose `seen_through` is ABOVE some
     `publishing` start. Its message says the merge "has not recorded its versions yet". That is
     exactly the gap:
     - A pin taken before the reservation is at or below `start`.
     - A pin taken inside the window is `start`.
     - Only a pin taken after the commit and before the record is above it.
   - A new test covers it.

5. **The Amendment 6 text overclaimed (Low).** It said REBASE "checks at validation and again at
   commit". The code checks only at validation. At commit, `apply_seq != new_seq` covers any merge that
   reserved after validation. A merge with zero ops moves nothing and claims nothing. The text is
   corrected here, and the code stays as it is: a second check would be a guard no mutant can tell
   apart from the first.

6. **Out of scope, stated.** An UNPINNED read (`seen_through = None`, a read of main as it stands)
   pairs a current snapshot with `versions`' latest, and it can name a version recorded after its scan
   ran. That is in-process only, and `observed_at`'s over-report direction is documented at the
   reservation.

### Tests, replacing Amendment 6's

- `a_pin_taken_while_a_merge_publishes_does_not_claim_its_versions`: same assertions. It registers
  through `PublishingEntry::register`. At the end it drops the guard and asserts
  `publishing.is_empty()`.
- `rebase_is_refused_while_a_merge_publishes`: same assertions, and registers the same way.
- NEW `version_history_is_not_asked_for_a_merge_it_has_not_recorded`, mutant-only red.
  - A pinned branch at `F`. A registered entry with `start < F` stands in for a merge that committed
    after the pin's snapshot and has not recorded yet.
  - `record_read(.., Some(F))` returns `Err` containing "has not recorded its versions yet".
  - After the guard drops, the same call returns `Ok`.

### Mutants, added to Amendment 6's

| id | edit | expected |
|---|---|---|
| M16 | `PublishingEntry::register`'s insert removed | debug: `record_applied`'s "never registered" fires in every merge test. `a_pin_taken_while_...` FAILS at its first `fork_seq` assertion |
| M18 | `merge`'s `_publishing` binding replaced by `let _ =` (the guard drops at once) | UNKILLED by any test here. With `Some(t)`, `record_applied`'s assert fires in every merge test, because the entry is gone before the record. So it is in fact killed in debug, by M16's instrument |
| M19 | `record_read`'s unrecorded-merge refusal removed | `version_history_is_not_asked_...` FAILS: `Ok` where `Err` was expected |

### Counts, replacing Amendment 6's

- `cargo test --lib version_history`: **8**.
- `cargo test --lib rebase`: 5.
- Run of record at default QoS: **58 result lines, 2162 passed (2152 + 10), 1 failed, 2 ignored**.
- Under `-b`: 2161 / 2.

**Amendment 7 correction to its own M18 row.** The row says both "UNKILLED" and "killed". The second
is right, and here is the exact spelling. The guard leaves the reservation block in a tuple:
`let (reserved, _publishing) = { .. }`. M18 replaces `_publishing` with `_`. The guard then drops at
the end of that `let`, after the block's state lock has been released, so it removes the entry before
`record_applied` runs. `record_applied`'s "never registered" assertion then fires in every merge test,
in debug builds. In release, M18 goes unkilled, and it is harmless there, as item 1 argues.

**A further correction, to the line above.** Strike "and it is harmless there". Under M18 the entry is
already gone during the publish window, so a pin taken inside it takes `apply_seq`: that is the
Amendment 6 defect itself. Item 1's "harmless" is about the opposite mutant, an entry STRANDED after a
failed publish. M18 is killed in debug, where the suite runs, and nowhere else.

## Amendment 8 (append-only, before any run; written BEFORE the code it describes): third fresh-context review, of `3c52476..fa1193a`

The review found nothing High or Medium, no wrong answer and no compile error, and it derived every test
and every mutant as pre-registered. Its Low findings are fixed here.

1. **Latent self-deadlock (Low).** `register` built the guard while the state lock was held, so a panic
   between `register` and the end of the reservation block would drop the guard first. Its `Drop` would
   then relock a mutex this thread still held. Production has no such exit, but test 3's fixture does.
   - **Fix:** `register` now CONSUMES the `MutexGuard`. It inserts, releases the lock, and only then
     returns the entry. No window exists in which the entry is alive and the lock is held, so the
     precondition is enforced by the signature instead of stated in a doc.
   - **Tests:** the three that register re-take the lock after `register` for any further setup.
     Their assertions are unchanged.

2. **Doc drift (Low/Info).**
   - The reservation comment still said freshness is checked "before the publish transaction opens".
   - The guard's doc said a stranded entry "would not harm pins". It would also refuse every
     exact-version read through a pin above it, for ever. Stranding is reachable only through a
     poisoned state mutex, and by then the runtime is already dead. Stated in the doc.
   - The docs on tests 1 and 3 are corrected: begin first, and test 3 names `PublishingEntry::register`.

3. **Both refusals in `record_read` ignored the access shape (Info).** A Range or FullScan inspection
   keeps a predicate and `observed_at`, not versions, and `observed_at = F + 1` is exact for it.
   - **Fix:** both refusals now fire only when the read names exact versions, i.e.
     `Inspection && shape.form() == ExactVersions`. That is the outcome at risk.

4. **Mutants that survive (Info).** No test caught any of these:
   - `start < f` → `start <= f`;
   - dropping the purpose conjunct;
   - either `pin_seq` `debug_assert`.

   Added:
   - Test 3 gains three reads that must NOT be refused:
     - one through a pin AT the entry's start (a pin taken inside the window), which kills `<=`;
     - a RowTargeting read through the pin above;
     - a FullScan inspection through it.

     The last two kill the dropped conjunct and the shape restriction's removal.
   - Two debug-only `should_panic` tests call `pin_seq` directly:
     - `pin_seq_names_a_contained_merge_above_an_excluded_one`: expected panic "no single fork_seq".
     - `pin_seq_names_a_recorded_version_above_its_seq`: expected panic "is recorded above".

5. **Found, NOT fixed here, and why.** This one existed before D194. At the merge's
   `ctx.txn.commit(publish_txn)?` a failed commit returns without an abort, and the txn stays in `att`.
   Aborting is not obviously right: `commit` can fail AFTER its `Commit` record is flushed (at
   `TxnEnd`), and there an abort would undo a durable commit. The fix needs `commit` to say which side
   of the flush it failed on. That is a change to the WAL API outside this lane, so it goes to the
   lead as a finding.

### Mutants, added

| id | edit | expected |
|---|---|---|
| M20 | the unrecorded refusal's `start < f` → `start <= f` | `version_history_is_not_asked_...` FAILS at the pin-at-start read (`Err` where `Ok` was expected) |
| M21 | `names_versions` loses its `shape.form() == ExactVersions` conjunct | the same test FAILS at the FullScan read |
| M22 | `names_versions` loses its `purpose == Inspection` conjunct | the same test FAILS at the RowTargeting read |

### Counts, replacing Amendment 7's

- `cargo test --lib version_history`: 8.
- `cargo test --lib rebase`: 5.
- `cargo test --lib pin_seq`: **2**, debug only.
- Run of record at default QoS: **58 result lines, 2164 passed (2152 + 12), 1 failed, 2 ignored**.
- Under `-b`: 2163 / 2.

## Amendment 9 (append-only, before any run; written BEFORE the test it describes): fourth fresh-context review, of `fa1193a..958a047`

The review found nothing High or Medium. It derived every assertion by hand and confirmed that
M19–M22 fail where Amendment 8 says they do. It left one Low finding and three Info notes.

1. **Low.** The shape scoping of the "released" refusal had no test. M21 and M22 are caught only
   through the "unrecorded" refusal. A mutant that changes only the `released` filter survived: for
   example, reverting it to the purpose-only check, which over-refuses.
   - **New test:** `version_history_released_pin_still_retains_reads_that_name_no_version`. It uses
     the fixture of `..._never_names_a_version_a_read_did_not_see`: pin 3, publishes 4 and 5, re-pin
     to 5.
   - It reads twice with `Some(3)`: a FullScan inspection, and an IndexLookup row-targeting read. Both
     must be `Ok`.
   - It names nothing new, so it compiles at `0570fe8` and passes there. It is mutant-only red.
   - **M23:** `released`'s filter uses `purpose == Inspection` in place of `names_versions`. Expected:
     the new test FAILS at the FullScan read.
2. **Info.** The gate goes by the shape's form, so an exact-shape read that matched no rows is still
   refused, although it records a predicate. That is over-refusal and safe. The comment now says
   "whose shape records exact versions" rather than "names versions".
3. **Info.** The doc of `version_history_is_not_asked_...` said its pin's snapshot "contains the
   merge". The `pinned()` fixture's snapshot does not include txn 99. `record_read` reads only the
   seq, so no assertion depends on it. The doc is corrected to say so.
4. **Info.** `register`'s "`locked` must come from `state`" cannot be enforced, because std offers no
   way to ask a guard for its mutex. Its doc now says that.

**Counts, replacing Amendment 8's:**
- `cargo test --lib version_history`: **9**.
- Run of record at default QoS: **58 result lines, 2165 passed (2152 + 13), 1 failed, 2 ignored**.
- Under `-b`: 2164 / 2.

## Amendment 10 (append-only; written BEFORE the tests and code it describes): the D194 cost review, and the lead's decisions

`frontier/d194_cost_review.md` @ `9ab4e83` found term 1 **narrowed, not closed**. The lead verified the
reasons at `4436e7f`:
- `record_applied` runs before `seal`, so the merging branch's own pin keeps every superseded entry it
  touches.
- A child inherits its parent's pin.
- Entries are freed only when the OLDEST pin rises.
- `drain` never returns capacity.

So an old live pin (W1), or a chain that inherits one pin (W2), makes the history grow with merges.
The flat SQL test passed only because it holds no live pin. The lead's decisions 1–7 are implemented
as follows.

### 1. Retention is O(rows × live pins), with no merge term

- **Invariant.** A retained entry `h` that is not a row's newest is kept only while a LIVE pin lies in
  `[h, s)`, where `s` is the version that superseded it.
- **Index.** `retention.readers: BTreeMap<h, (tbl, row, s)>` is keyed by the entry's value.
  `begin_ts` values are unique, because every op gets its own seq.
- **At supersession** (in sequence order), `h` gets an entry in `readers` if `pins.range(h..s)` is
  non-empty. Otherwise it is popped.
- **At ANY pin's departure** (its count reaching 0 in `drop_pin`), with `a` the next lower live pin
  (0 if there is none):
  - Only entries with `h ∈ (a, p]` can have lost their last reader. An entry with `h <= a` still
    contains `a`.
  - That range is added to `retention.pending`, and each entry in it is re-tested locally with
    `pins.range(h..s)`. The test is exact whatever pins came or went in between, because a new pin
    is at or above every published `begin_ts`.
- **Freeing.** A freed entry leaves `readers` and is counted in `retention.garbage[row]`. It stays in
  the row's `Vec` until that row's garbage reaches half the vector. The row is then compacted to its
  newest entry plus the entries still in `readers`.
  - A garbage entry never changes a live pin's answer: the newest version at or below a live pin is
    always retained, and nothing retained lies between it and the pin.
- **Removed:** the oldest-pin horizon, `trimmable`, and `reclaim_history`.
- **Bound per row:** the newest entry, plus one entry per distinct live pin value that reads an older
  version, plus garbage below half the row.
- **Not bounded, stated:**
  - Entries still waiting in `pending` (item 3).
  - One `version_history` key per row ever published, the same class as `versions`.

### 2. Capacity is returned

After a compaction, and after a pop at publish, a row's `Vec` is shrunk to fit whenever
`capacity > 2·len + 4`. The tests assert capacity as well as `len()`.

### 3. The departure stall is bounded, and chunked

- **Each departure** sweeps at most `DEPARTURE_SWEEP_BUDGET = 4096` entries from `pending`.
- **Each publish** sweeps `PUBLISH_SWEEP_BUDGET = 2`.
- A departure's own work is the entries in `(a, p]`. These are the entries the departing pin reads
  that its lower neighbour does not, plus garbage that is still pending. Anything beyond the budget
  waits in `pending`, whose intervals are merged, so the queue holds disjoint intervals only.
- **Stall under the state lock:** at most 4096 × O(log n) per departure, and 2 × O(log n) per
  publish, plus the amortised compactions.
- **Stated:** with no publishes and no departures, pending garbage waits.

### 4. `VISIBILITY_HOPS` follows D176's rule

- `resolve_visibility` no longer writes a global. `resolve_visibility_counted` adds hops into a
  per-scan `HopCount`, whose `Drop` adds ONE relaxed `fetch_add` to `VISIBILITY_HOPS`, and only when
  the count is non-zero.
- `SeqScan`, `IndexScan` and `SecondaryIndexScan` each hold one `HopCount`. Full-text search holds one
  local per call.
- The harness integers are unchanged: the flush happens when the plan root drops, inside the
  statement.

### 5. Corrections to the lane report

§9.1's "memory with live pins" bullet is corrected to item 1's bound. §9.2 now states the secondary-index
cost: a pinned range scan through a secondary index resolves visibility once per index ENTRY. That is
O(d_ever × u_since_pin) hops per row, against O(d_ever) checks for a fresh reader. `d_ever` is the
distinct indexed values the row ever held, and `u_since_pin` its versions since the pin.

### 6. C1 is D197's gap, so D194 lands after #16

A key that main deletes and reuses after a pin makes a pinned INDEX read miss the fork-time row. D197's
chain fix (`4296723` and `52b66d6`) is in #16's lineage: `git merge-base --is-ancestor` holds for both
against `2c10f17`.

**New test:** `tests/d194_pinned_key_reuse.rs`,
`a_pinned_index_read_after_main_deletes_and_reuses_the_key_returns_the_fork_image`.

| step | on this branch alone | after merging #16 |
|---|---|---|
| (a) the full scan returns `(1, 10)` and not `(1, 99)` | passes | passes |
| (b) premise: the point read took the index path (`INDEX_SCANS` delta ≥ 1) | passes | passes |
| (c) the point read returns `[(1, 10)]` | **FAILS**, returning `[]` | passes |

### 7. C2: a failed author stamp after the publish commits

**Choice: record every version BEFORE any fallible call.** `record_applied` becomes two passes:
1. The first pass is infallible, and completes all in-memory recording: `push_applied`,
   `publish_version`, the valued writes, the capture, and the merge record.
2. Only then does the second pass make the `stamp_row` calls. They are still under the lock, as before.

The alternative, marking the merge so exact reads refuse, would add a second state to maintain.

**Red test:** `record_applied_records_every_version_before_a_failed_stamp`.
- **Setup:** an `AgentRuntime` whose `prov_store` fails its 2nd `stamp_row`, and three fresh rows, each
  updated and merged.
- **Before the fix:** the merge returns the injected error after commit, and row 3's version is
  missing from `versions`, so the test FAILS.
- **At the fix:** all three versions are recorded, and the error still surfaces.
- **Premises:** the error is the injected one, and main shows all three updates, so the publish did
  commit.

### Tests: W1–W3 and C2 compile at `0570fe8` and at `4436e7f`; W4 is mutant-only

| test | at the tests-only commit (`4436e7f` code) | at the fix |
|---|---|---|
| W1 `version_history_stays_flat_under_merges_beside_an_old_pin`: OLD pinned before any publish, 40 × (pinned fork, UPDATE 2 rows, MERGE) | **FAILS**: held 20 after merge 10 vs 80 after merge 40 | passes: 2 == 2 == `versions.len()`, and Σcapacity ≤ Σ(2·len + 4) at both |
| W2 `version_history_stays_flat_under_a_chain_that_inherits_one_pin`: one merge, P0 pinned, then 40 rounds of (fork a child from the chain head, ABANDON the head, a merge from trunk) | **FAILS** the bound: held grows with the round | passes: held ≤ 3 × rows at rounds 10 and 40; the head's pin still names versions 1 and 2 |
| W3 `version_history_frees_an_entry_when_its_last_reader_leaves_and_returns_the_capacity` (State level): 50 pins each read one version of row 1; pins 50..2 depart | **FAILS**: len 51, bound 3 | passes: len ≤ 3 with cap ≤ 2·len + 4. After pin 1 departs, len 1 with cap ≤ 6 |
| W4 `version_history_departure_sweep_is_chunked_and_finishes_under_later_publishes` (mutant-only; names `DEPARTURE_SWEEP_BUDGET`): 4196 rows re-published under one pin, then the pin departs | n/a | passes: 100 entries left in `readers` and `pending` non-empty; after 50 publishes, both empty |
| C2 `record_applied_records_every_version_before_a_failed_stamp` | **FAILS**: row 3 has no version | passes |

**Prediction for the tests-only commit:** `cargo test --lib version_history_stays_flat_under_merges_beside version_history_stays_flat_under_a_chain version_history_frees_an_entry record_applied_records_every_version`
is the four names, run as separate filters: **4 FAILED**. The earlier nine `version_history` tests keep their Amendment 9 outcomes
at every commit (derived again for the new rules: B1 len 2, B2 len 1, the unrecorded and
released tests unchanged).

### Mutants

| id | edit | expected |
|---|---|---|
| M24 | `on_pin_departed` does nothing | W1, W2 and W3 FAIL; `..._is_freed_when_the_oldest_pin_is_sealed` FAILS (len 2) |
| M25 | the sweep frees only when the departing pin was the oldest (`below` computed but skipped whenever `a` exists) | W1 and W3 FAIL |
| M26 | `free_entry` never compacts | W3 FAILS (len 26, then more); `..._is_freed_when_the_oldest_pin_is_sealed` FAILS (len 2) |
| M27 | no `shrink_to_fit` | W3 FAILS on capacity (64 > 2·len + 4) |
| M28 | `sweep_pending` ignores its budget | W4 FAILS: 0 left in `readers` after the departure |
| M29 | `publish_version` does not sweep | W4 FAILS: 100 still in `readers` after the 50 publishes |
| M30 | the stamps moved back inside the first pass | C2 FAILS |

### Counts, replacing Amendment 9's

- `cargo test --lib version_history`: **13**, all pass (W4 included).
- Run of record at default QoS: **59 result lines** (the new `d194_pinned_key_reuse` matches the agent
  grep), **2170 passed** (2165 + 5 lib tests), **2 failed**, **2 ignored**. The failures are:
  - the envelope tripwire, `get_mut(` still **5**, whose State allowlist now needs `version_history`,
    `retention` and `publishing`;
  - `a_pinned_index_read_...`, which passes after merging #16.
- Under `-b`: 2169 / 3.
- The harness gains a history census (`version_history_census`: rows, entries, capacity). At every k,
  Part 1 must print `entries == 256` and `capacity ≤ 2·entries + 4·256`, and it refuses otherwise.

**Amendment 10 correction, written before the census code was committed.**
- **The harness census at k = 0.** Amendment 10 says part 1 must print `entries == 256` "at every k". At
  k = 0 no merge has published anything, so the history is empty. The pre-registered values are 0 at
  k = 0 and 256 at k ∈ {8, 32, 128}, with capacity ≤ 2·entries + 4·256 at every k.
- **The "Prediction for the tests-only commit" line.** It names four filters in one command line. It
  means: each of W1, W2, W3 and C2, run by its own name, FAILS at `24a3961`.

## Amendment 11 (append-only; written BEFORE the tests and code it describes): an unpinned read names what its scan saw

The cost review's Q7 (`frontier/d194_cost_review.md` §7 @ `a713506`) confirms, in-process only, at
`9aa6968` and at `4436e7f`, the item I had left unfixed.
- **The mechanism.** An UNPINNED read scans with no lock held. `record_read` then relocks and names
  each row's LATEST version, with `observed_at = apply_seq + 1` taken at record time rather than at
  the scan's snapshot.
- **Forward window** (a merge records between the scan and the record):
  - a missed premise conflict: the premise names the new version;
  - a false REVERT block: the predicate clock is too late;
  - a missed REVERT dependency: the exact edge goes to the wrong merge.
- **Reverse window** (the scan contains a merge that commits before it records): a false conflict,
  and a missed edge.
- **Reached at `4436e7f` by** a branch with no live workspace: a library `select` of TRUNK or of a
  sealed branch, or `AS OF BRANCH` sealing between bind and select. pgwire drains readers, so not
  over pgwire.

The lead's decision: an unpinned read takes a `pin_seq`-paired `seen_through` under the lock.

### The change

1. **`visible_rows_where`.** Its unpinned arms (a branch with no live workspace, and `branch = None`)
   now take the snapshot and `seen_through = Some(pin_seq(publishing, apply_seq, recorded, &snap))`
   under ONE lock acquisition. The seq and the snapshot then describe one instant, as a pin's do.
   Its signature is unchanged.

2. **`record_read`.** The rule for an exact-version read with `seen_through = Some(F)` is unified, so
   that it no longer matters whether `F` came from a pin:
   - **A matched row whose latest version is at or below `F`:** the scan saw that latest version, and
     it is named. No history is needed.
   - **A matched row whose latest version is above `F`:**
     - If `F` is a live pin, the history holds the version it read, and `version_seen` names it.
     - Otherwise the history may have dropped that version: this covers both a pin released
       mid-read and an unpinned read overtaken by a merge. The read is REFUSED, retryably. The
       message still says the snapshot "was released while this read ran, or was never a pin".
   - **Unchanged:** a read above an unrecorded merge refuses (Amendment 7), and a predicate or
     row-targeting read is never refused (Amendment 8).

   This relaxes Amendment 5's membership refusal: a released pin whose matched rows did not move is
   now answered exactly instead of refused. The released-pin test still refuses, because its row
   moved from 3 to 5.

3. **`observed_at = F + 1`** for every read that carries `Some(F)`, so an unpinned predicate read's
   clock is its scan's, not the record's.

Single-threaded behaviour is unchanged. With no interleaving, `F = apply_seq`, every latest version
is at or below `F`, and `observed_at = apply_seq + 1`, as before.

### Red tests, all in `runtime.rs` `mod tests`

These drive `visible_rows_where` on TRUNK, then commit a merge (or register an unrecorded one), then
call `record_read`, using the current signatures. So they compile at the current tip `b4cfce3`.

| test | at the tests-only commit | at the fix |
|---|---|---|
| `an_unpinned_read_never_names_a_version_published_after_its_scan`: rows 1 and 2, merge m0 on both, reader R; R scans TRUNK, merge W updates row 1, R's exact read of row 1 is recorded | **FAILS**: `Ok`, naming W's version | passes: `Err` ("was released while this read ran, or was never a pin"). An `Ok` naming m0's version would also pass. |
| `an_unpinned_read_whose_snapshot_holds_an_unrecorded_merge_refuses`: a committed txn registered as publishing with `apply_seq` moved past it, R scans TRUNK, then records an exact read | **FAILS**: `Ok` | passes: `Err` ("has not recorded its versions yet") |
| `an_unpinned_scan_is_not_a_dependent_of_a_merge_published_after_it`: R full-scans TRUNK, W merges, R's scan is recorded, then `REVERT MERGE <W> HALT`. Positive control: R2 scans and records AFTER W, and the same revert is blocked by R2 | **FAILS**: `blocked_by` names R | passes: `blocked_by` names R2 and not R |

### Mutants

| id | edit | expected |
|---|---|---|
| M31 | the unpinned arm returns `None` again | all three FAIL |
| M32 | the per-row refusal is removed (`F` not a live pin → never refuse) | the forward test FAILS (`Ok`, naming `begin_ts 0`); `version_history_never_names_...` FAILS |
| M33 | the unrecorded refusal is removed | the reverse test FAILS |

### Counts, replacing Amendment 10's

Run of record at default QoS: **59 result lines, 2173 passed (2170 + 3), 2 failed** (the envelope
tripwire, and `a_pinned_index_read_...` until #16), **2 ignored**. Under `-b`: 2172 / 3.
`cargo test --lib version_history`: 13. `cargo test --lib an_unpinned`: 3.

## Amendment 12 (append-only; written BEFORE the tests and code it describes): a fresh review of `4436e7f..ff971ee`

The review traced all seven new tests by hand. Each passes at `ff971ee` and fails where Amendments 10
and 11 say it fails. It found the retention scheme correct with one catalog and found no compile
error. Its findings:

1. **F1, Medium: a stamp failure left the merge half-finished.** `record_applied(...)?` returned
   before `attest_merge` and `seal`. The ordering already existed at `4436e7f`.
   - **What that left:** a merge that is committed and whose versions are recorded, with the branch
     still LIVE and not in `published_txns`. A second `MERGE` of it would publish again. A later
     ABANDON would drop the capture of a published merge, and REVERT would lose that merge's edges.
   - **Fix:** `publish_evaluation_as` keeps `record_applied`'s result, runs `attest_merge` and
     `seal`, and only then returns the error. The error reads "merge m_N was published and sealed,
     but recording who wrote its rows failed: …".
   - **Red test:** `a_merge_whose_author_stamp_fails_is_still_sealed`, the C2 fixture. It asserts:
     the branch has no live workspace; its txn is in `published_txns`; a second `MERGE` of it is
     refused; and main's values moved once (`v = 10·id + 1`).
     - At `ff971ee` it FAILS, because the workspace is live.
     - It names only existing items, so it compiles at `ff971ee`.

2. **F2, Medium: a finding for the lead, not fixed here.** `TxnManager::commit` removes the
   transaction from `att`, which makes its rows visible, and only then may run
   `self.checkpoint()?` (`wal/txn.rs:646` on this base). A checkpoint failure therefore returns `Err`
   after the publish is visible. `record_applied` is then skipped, and the guard removes the
   publishing entry, so this is the C2 state reached through another door.
   - **Why not here:**
     - The contract bug is in `commit`: a committed transaction must not report failure. `commit` is
       also the code #16 rewrote.
     - Patching around it in `merge`, by checking whether the txn is still active after an `Err`,
       would add a guard no test here can fire. The one WAL injection, `fail_next_append`, fails the
       NEXT append, which is `commit`'s own, not the checkpoint's.
   - Routed to the lead.

3. **F3, Low: a sweep's budget did not count empty intervals.** `continue` skipped past an interval
   with no `readers` key without spending budget, so one call could walk every queued interval.
   - **Fix:** each interval visited costs one unit of budget, found or not. The stall is then at most
     `budget` range probes and entry tests per call, plus amortised compactions.
   - **Test:** `version_history_sweep_budget_counts_empty_intervals`. It queues 10 disjoint empty
     intervals and sweeps with budget 2, then expects 8 still queued.
     - At `ff971ee` it FAILS: 0 are left.
     - Compiles at `ff971ee`.

4. **F4, Low: two mechanisms had no test.** Two mutant-only tests, each of which passes at `ff971ee`:
   - `version_history_pending_intervals_merge_when_they_overlap_or_touch`: `add_pending`'s interval
     algebra, covering disjoint, overlapping, touching and spanning several intervals.
   - `an_unpinned_scan_inside_a_publish_window_pairs_with_its_start`: with a registered reservation
     whose txn has NOT committed, an unpinned scan's `seen_through` is the reservation's `start`,
     not `apply_seq`. A mutant with `read_now` returning `apply_seq` fails it.

5. **F5, Low: stale comments.** Three are corrected:
   - "an exact-shape read that matched no rows is still refused" now applies to the `unrecorded`
     refusal only;
   - `visible_rows_where`'s second return value is now always `Some`;
   - `resolve_visibility` "counts nothing" but pays one drop.

6. **F6, Low: `None` was still representable.** `record_read` now REFUSES a read that carries no
   `seen_through`. Every read path pairs one, so a caller that passes `None` would bring Q7 back.
   - Narrowing the type to `u64` would edit committed tests that pass `Some(..)`, so the refusal is
     the change.
   - **Test:** `a_read_without_a_paired_seq_is_refused`.
     - At `ff971ee` it FAILS, returning `Ok`.
     - Compiles at `ff971ee`.

7. **F7, Low, two catalogs only.** The out-of-order arm counted garbage with no compaction check. It
   now compacts through the same check `free_entry` uses.

8. **F8, Low: corrections to this file.**
   - **Amendment 10's M26 row:** W3 fails with len **51**, not 26.
   - **M25:** W2 also fails.
   - **"W1–W3 and C2 compile at `0570fe8`":** this is literally false for the test HELPERS.
     `txn_fixture` arrived at `3c52476`, and `publish_next` and `pinned` at `9462d3b`. What is true
     is that they name no `src` item newer than `0570fe8`. The red commit that matters, `24a3961`,
     carries every helper.

### Counts, replacing Amendment 11's

- `cargo test --lib version_history`: **15**, all pass (13 + the sweep-budget test + the interval
  test).
- `cargo test --lib an_unpinned`: **4**.
- New lib tests: `a_merge_whose_author_stamp_fails_is_still_sealed` and
  `a_read_without_a_paired_seq_is_refused`.
- Run of record at default QoS: **59 result lines, 2178 passed (2173 + 5), 2 failed, 2 ignored**.
  Under `-b`: 2177 / 3.

### Mutants

| id | edit | expected |
|---|---|---|
| M34 | `seal` runs only on `Ok` again | `a_merge_whose_author_stamp_fails_is_still_sealed` FAILS |
| M35 | the sweep's empty-interval visit is free again | `version_history_sweep_budget_counts_empty_intervals` FAILS (0 left) |
| M36 | `add_pending` inserts without merging | the interval test FAILS |
| M37 | `read_now` returns `apply_seq` | `an_unpinned_scan_inside_a_publish_window_pairs_with_its_start` FAILS |
| M38 | the `None` refusal is removed | `a_read_without_a_paired_seq_is_refused` FAILS |

## Amendment 13 (append-only; written BEFORE the tests and code it describes): review 6's CONDITIONAL PASS, and the lead's decisions A–F

Review 6 is `frontier/d194_review6.md` @ `8a87787`, taken at `0fdcd81`. `0fdcd81` is not moved; the
commits below follow it.

### A (before landing): the SQL session unbinds by STATE after a MERGE or ABANDON

`dispatch.rs`'s MERGE arm cleared `session.agent` only on `Ok`. After Amendment 12's "published and
sealed" error, that left the connection bound to a reaped branch:
- BEGIN AGENT SESSION was refused as "already open";
- every read was refused as "no agent session";
- ABANDON failed, because the branch was already reaped.

**Fix.** Both the MERGE arm and the ABANDON arm keep the runtime's result. They clear the binding
whenever the runtime holds no live workspace for that branch
(`AgentRuntime::has_live_workspace`, new and O(log n)), and only then apply `?`.
- The decision is read from state, never from error text.
- The MERGE arm's old condition, `applied_to_target`, is equivalent on every `Ok`:
  - a published merge seals, so no workspace remains;
  - a conflict or a quarantine keeps the workspace.

**Red tests.** Both are in `runtime.rs` `mod tests`, go through `executor::run`, and compile at
`0fdcd81`.

| test | at `0fdcd81` | at the fix |
|---|---|---|
| `a_session_whose_merge_published_but_failed_is_unbound`: the MERGE's 2nd author stamp fails, then the same session begins a new agent session | **FAILS**: "an agent session is already open" | passes |
| `a_session_whose_branch_was_abandoned_elsewhere_is_unbound_by_its_own_abandon`: connection 2 runs `ABANDON BRANCH` on connection 1's branch, connection 1's own `ABANDON;` fails, then connection 1 begins a new session | **FAILS** (the same refusal) | passes |

### F (before landing): the attested half of Amendment 12's F1 gets a test

**Test:** `a_merge_whose_author_stamp_fails_is_attested_once`. After the failed-stamp merge:
- TRUNK's attested chain holds exactly one `Merge` entry;
- the branch's chain ends in `Reap`, and holds exactly one `Reap`.

It passes at `0fdcd81` (mutant-only). **M39** skips `attest_merge` on the authorship-error path and
FAILS it.

### B: a seal failure no longer hides the stamp failure

When both fail, the error names both: "… recording who wrote its rows failed (…), and sealing its
branch then failed too (…)".

**Untested, stated.** No injection point can make `seal` fail after a publish. Its fallible steps are
the reaper and branch-catalog calls, which in-memory tests cannot break. The change is error
reporting, and it has no branch that could admit or refuse anything.

### C: the cluster wrapper tells, from state, whether the rows landed

`cluster.rs`'s publish-failure wrapper now asks the runtime `published_merge_of(from)`: is there a
merge record for this branch?
- `record_applied` inserts that record after the publish commits and before any author stamp.
- **Record present:** the error says the rows LANDED here as merge `m_N`, and that finishing it failed.
- **Record absent:** it keeps the old meaning.

The lookup scans `merges` (O(merges ever), not pruned before this change) on the error path only.
The old text's runs of spaces, left by line continuations that an earlier edit lost, are removed at
the same time.

**Untested, stated.** No cluster test drives a publish whose stamp fails.

### D: a row the failed stamp missed never answers with its PREVIOUS author

**Confirmed from source at `0fdcd81`, so it had to change:**
- `DurableProvenanceStore::stamp_row` updates memory before its append, and poisons the store if the
  append fails.
- `row_author` reads memory WITHOUT checking the poison.
- `record_applied` stops at the first failed stamp.

So every row after the failed one answers the previous merge's author.

**Two changes:**
1. **`record_applied` stamps best-effort.**
   - A failed `stamp_row(row, run)` is followed by `stamp_row(row, NONE)`, which clears the row so it
     reads as "nobody on record" (unknown).
   - The loop then continues with the remaining rows.
   - The error it returns names the first failure, and lists the rows it could not stamp and could not
     clear.
2. **`DurableProvenanceStore::row_author` and `attributed_rows` REFUSE when the store is poisoned.**
   - A store whose log has stopped accepting writes cannot say who last wrote a row: the write it
     refused may have been exactly that.
   - `who_wrote_row` then answers `None` and `authors_of` answers empty. Neither names a stale author.
   - The physical `attribute` / `who_wrote(rid)` reads are unchanged. Their test asserts that they
     still answer after poisoning.

**Stated blind spot:** after a RESTART the reopened store is healthy and replays only what reached its
file, so a row whose new stamp never landed answers its previous author again. Closing that needs
authorship written atomically with the publish, which is D212 option (a) or D219's grouped stamp.

**Tests (both compile at `0fdcd81`).**

| test | at `0fdcd81` | at the fix |
|---|---|---|
| `a_row_the_failed_stamp_missed_never_names_its_previous_author` (runtime; `FailingStamps` fails the 5th call: run A's merge stamps rows 1–3, run B's merge fails its 2nd stamp) | **FAILS**: rows after B's failure name run A | passes: every row names B, or no one |
| `a_poisoned_store_refuses_to_say_who_wrote_a_row` (`durable.rs`: stamp a row, inject an append failure, then `row_author`) | **FAILS**: `Ok`, naming the old author | passes: `Err` |

### E: waiting on the lead's decision about retiring two tests

The design is already registered here, so the direction is fixed before any code.
- **Buckets.** A kept entry sits in the bucket of its HIGHEST live reader, the max pin in `[h, s)`.
- **Departure.** When pin `p` departs, every entry in its bucket either:
  - moves down to `a`, the next lower live pin, if `a ≥ h`; or
  - is freed.

  That frees exactly the rectangle `(a, p] × (p, b]`, and never re-tests an entry a higher pin reads.
- **The queue.** Departed pins whose buckets are not yet empty, swept at the Amendment 10 budgets. It
  never restarts.
- **New pins** never enter a kept entry's interval, because a new pin is at or above every recorded
  `s`, so arrivals touch no bucket.
- **Red test:** review 6's fixture (cold = B + 1000, hot = 100). After P0 and P1 depart, no hot entry
  is held. At `0fdcd81` all 100 are held.
- **Counter test:** after the two departures, sweeps have touched exactly 100 entries, and the queue is
  empty.

This retires the interval queue (`add_pending` / `sweep_pending` over `(lo, hi]`), so two mutant-only
tests from Amendment 12 no longer compile. Deleting them is a test deletion, and that is the lead's
decision. Until it is made, the `HistoryRetention` doc and lane §10.1 stop claiming the queue drains.

### Mutants

| id | edit | expected |
|---|---|---|
| M39 | `attest_merge` skipped when authorship failed | `..._is_attested_once` FAILS |
| M40 | MERGE unbinds only on `Ok` again | `a_session_whose_merge_published_but_failed_is_unbound` FAILS |
| M41 | ABANDON unbinds only on `Ok` again | `a_session_whose_branch_was_abandoned_elsewhere...` FAILS |
| M42 | no clear after a failed stamp | `a_row_the_failed_stamp_missed...` FAILS (the failed row names A) |
| M43 | the stamp loop stops at the first failure again | the same test FAILS (the later rows name A) |
| M44 | durable `row_author` stops checking the poison | `a_poisoned_store_refuses_...` FAILS |

### Counts, replacing Amendment 12's (before E)

Six new tests:
- lib: the two unbinding tests, attested-once, the previous-author test, and the poisoned-store test;
- and `version_history` is still 15.

Run of record at default QoS: **59 result lines, 2183 passed (2178 + 5 lib + 0), 2 failed, 2
ignored**.

Correction to the line above: six tests means 2178 + 6 = **2184 passed**. The durable-store test is
also a lib test.
