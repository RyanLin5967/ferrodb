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
