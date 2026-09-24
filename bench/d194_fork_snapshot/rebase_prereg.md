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
