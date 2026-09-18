# F9 — agent isolation on a cluster

Branch `F9-agents-cluster`, committed locally, never pushed. Files: `src/agent_sql/cluster.rs`
(new), `tests/integration_cluster_agents.rs` (new), one `pub mod` line in `src/agent_sql/mod.rs`,
and one allowlist entry in `tests/integration_server_stdout.rs` (below). **`src/agent_sql/runtime.rs`
was not touched** — everything this row needed was already public, so the file another agent may
want is untouched.

## What I built

### 1. The ordering rule, which is what the row was for

`Command::Branch { op: BranchOp::Merge { branch, base_round } }` at round `M` applies **iff no
round in `(base_round, M)` carries a command that can change what the gate read.** Otherwise it is
a no-op on every node and the proposer re-evaluates against the base as it now stands. Every node
applies the same entries in the same order, so every node computes the same verdict — which is what
makes this a cluster ordering rule rather than one node's optimisation.

`moves_base(&Command)` is that classification, public and exhaustively matched (a new `Command`
variant fails to compile rather than defaulting to "harmless"). The table:

| moves the base | does not |
|---|---|
| `WalBatch`, `Catalog`, `Branch{Merge}` *that applied* | `Branch{Fork/Abandon/Reap}`, `ArenaGrant`, `TxnIdRange`, `LeaseTick`, `Checkpoint`, `Membership`, `NoOp` |

The right-hand column is load-bearing for liveness, not an omission: `LeaseTick`, `NoOp` and
`ArenaGrant` are what a healthy idle cluster produces, and classifying any of them as base-moving
would re-evaluate every merge on every cluster for ever. `Branch{Merge}` is the one conditional
entry and `BranchLedger` owns the condition — a merge that was itself re-evaluated published
nothing, so it moved nothing, and only the ledger knows which happened.

The comparison is `last_base_move > base_round`, strictly. `base_round` is the committed head the
evaluation read, so a move *at* that round is one the gate saw; `>=` would re-evaluate every merge
proposed after any write.

This composes with, and does not replace, the node-local guard that already existed:
`MergeEvaluation`'s base fingerprint is **precise and local**, the round rule is **coarse and
agreed**. Neither subsumes the other.

### 2. The branch namespace, which the frozen contract forced me to answer

`BranchOp::Fork { child: u64, .. }` names a branch by number, and both branch catalogs mint from a
node-local `AtomicU64` — so two nodes both propose `Fork { child: 1 }` and the second silently
claims the first's arenas, which the reaper then frees. This is the same class of failure F4
removed from arena extents and txn ids.

Rather than a fourth leader-granted counter, `ClusterBranchId` **partitions** the namespace: 24 bits
of owning node, 40 bits of that node's own id, trunk pinned at 0 on every node. An extent and a txn
id are cluster resources and must be arbitrated; a branch's pages live on exactly one node and are
never shipped, so the namespace can be split instead. It costs no round trip, cannot be got wrong by
a node that has just been promoted and has not yet learned a watermark, and it carries the owner in
the id — which is precisely the question a promoted leader has to answer about every branch it
inherits. An id that does not fit is **refused, not truncated**.

### 3. `BranchLedger` — the cluster's branch metadata, from the log alone

`Live | Merged{at} | Abandoned{at} | Reaped{at, generation}` per branch, plus `last_base_move`,
built by `BranchApplier` (an `Applier`, chainable so a server's WAL applier sits behind it and the
ledger still sees *every* round — `moves_base` is a statement about the rounds *between* two branch
commands). This is what `DISTRIBUTED.md` says branch metadata is replicated *for*: reaping and
extent allocation are the destructive, unrecoverable decisions and must be agreed even though the
rows they concern are not.

### 4. `ClusterAgents` — fork without a quorum wait, merge with exactly one

`ConsensusCost { proposals, quorum_waits, reevaluations }` makes the performance claim a
measurement instead of a sentence. Fork: refuse if not leader, create locally (zero pages), send the
metadata and **do not wait**; a refused proposal abandons the local branch, because a branch the log
does not carry cannot be reaped by a replicated decision. Merge: evaluate against the committed
head, propose, wait for the round to be *applied* (not merely committed — the verdict is computed by
the applier), then publish or re-evaluate.

### 5. What a node's death does to a branch

Implemented as `DISTRIBUTED.md` decided it, not relitigated: a branch is a transaction. `orphans_of`
names the live branches a dead node was working on, `merges_owned_by` names what a promoted leader
inherits, `rows_are_on` answers "are the rows here" from the id alone, and `abandon_orphans_of`
disposes of the orphans **by a replicated decision** — because two nodes disagreeing about whether
one of those is live ends in a reap, and a reap is unrecoverable.

## Tests — 35, in `tests/integration_cluster_agents.rs`

Three harnesses, each where its property can actually fail:

- **Pure ledger** (20 tests) — the namespace, the `moves_base` table named variant by variant, and
  the ordering rule including three ledgers driving one log and being required to agree.
- **A real `AgentRuntime` against a scripted log** (11) — the race is *placed*, not waited for: a
  `WalBatch` lands between the gate's read and the merge's own round, every run. This is where the
  headline test lives.
- **Three real in-process `Node`s** (4) — real listeners, real elections, real round logs, real
  fsyncs. `Db` + the SQL surface on the leader.

```
test result: ok. 35 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.93s
```

The three that answer the brief directly:

- `a_merge_racing_a_committed_change_to_its_base_is_re_evaluated_rather_than_silently_applied` —
  asserts `reevaluations == 1`, that the merge then published, and at the log level that round 3
  was `ReEvaluate { moved_at: 2 }` and round 4 `Applied { base_round: 3 }`. Without both halves it
  would pass for a merge that was never raced.
- `a_fork_and_a_hundred_writes_cost_no_quorum_round_trip_and_the_merge_costs_one` — cost is exactly
  `{proposals: 1, quorum_waits: 0}` after a fork and a hundred writes, exactly
  `{proposals: 2, quorum_waits: 1}` after the merge, and the log holds two commands, both `Branch`.
- `a_followers_committed_log_holds_the_merge_and_not_one_agent_row` — read off a **follower's**
  applied entries on a real three-node cluster: one fork, one merge, zero `WalBatch`. The
  anti-vacuity halves come first, because a follower that applied nothing would pass the
  interesting assertion trivially.

## Mutants — 28, every one killed

Driver `bench/evidence/F9-mutants.py`, transcript `bench/evidence/F9-mutants.md`. Every named test was
**first shown to pass on the clean tree** in the same run, or a kill would prove nothing. Each
mutant printed `test result: FAILED. 0 passed; 1 failed` for the test named against it; the clean
tree printed `ok. 1 passed`.

| # | the rule broken | test that died |
|---|---|---|
| M1 | a WAL batch does not move the base | `a_merge_racing_a_committed_write_to_its_base_is_re_evaluated_not_applied` |
| M2 | the comparison is `>=` instead of `>` | `a_merge_whose_base_round_is_exactly_the_last_base_move_applies` |
| M3 | a re-evaluated merge moves the base too | `a_re_evaluated_merge_does_not_itself_move_the_base` |
| M4 | the cluster id drops the owning node | `two_nodes_forking_at_the_same_local_id_get_different_cluster_ids` |
| M5 | an oversized local id is packed, not refused | `a_local_branch_id_too_large_for_the_namespace_is_refused_not_truncated` |
| M6 | a fork waits for its own commit | `a_fork_and_a_hundred_writes_cost_no_quorum_round_trip_and_the_merge_costs_one` |
| M7 | the ledger has no idempotence guard | `a_re_delivered_round_is_a_no_op` |
| M8 | a colliding fork overwrites what is there | `a_fork_whose_child_id_is_already_taken_is_refused_not_overwritten` |
| M9 | the cluster merge does not check quarantine | `a_merge_the_gate_declines_costs_no_consensus_at_all` |
| M10 | a fork does not check leadership first | `a_fork_on_a_node_that_does_not_lead_is_refused_and_creates_nothing` |
| M11 | leadership is not re-checked while waiting | `a_merge_that_loses_the_leadership_while_waiting_refuses_rather_than_blocking` |
| M12 | a `LeaseTick` moves the base | `heartbeat_rounds_do_not_re_evaluate_a_merge` |
| M13 | a re-evaluated merge is published anyway | `a_merge_racing_a_committed_change_to_its_base_is_re_evaluated_rather_than_silently_applied` |
| M14 | a merge of a branch no fork created applies | `a_merge_for_a_branch_whose_fork_never_committed_is_refused` |
| M15 | a second reap is allowed | `a_reap_is_a_replicated_decision_and_is_refused_twice` |
| M16 | the verdict reads something not in the log | `every_node_reaches_the_same_verdict_for_every_merge_command` |
| M17 | the re-evaluation bound is 100x looser | `a_merge_whose_base_never_stops_moving_is_refused_with_the_round_that_moved` |
| M18 | a refused fork just times out | `a_merge_whose_fork_the_cluster_refused_says_so_rather_than_timing_out` |
| M19 | the orphan sweep is not filtered by owner | `a_promoted_leader_disposes_of_a_dead_nodes_branches_by_a_replicated_decision` |
| M20 | a node may declare itself dead | `a_node_cannot_sweep_its_own_branches_as_orphans` |
| M21 | a reap is decided locally | `a_reap_goes_through_the_log_and_carries_the_generation` |
| M22 | the verdict reads something not in the log (liveness half) | `no_two_nodes_disagree_about_whether_a_branch_is_live` |
| M23 | the applier swallows rounds instead of chaining | `a_followers_committed_log_holds_the_merge_and_not_one_agent_row` |
| M24 | the trunk is packed like any other branch | `the_trunk_is_the_same_branch_on_every_node_and_has_no_owner` |
| M25 | node 0 may own a branch | `node_zero_cannot_own_a_branch` |
| M26 | a merge that applied does not move the base | `a_merge_that_applied_moves_the_base_for_the_merge_behind_it` |
| M27 | an unknown fork parent is treated as the trunk | `a_fork_off_a_parent_the_cluster_does_not_hold_is_refused` |
| M28 | the orphan list is not filtered by the dead node | `a_branch_whose_owner_died_is_named_as_lost_work_and_its_merged_sibling_is_not` |

**The other half — shown to stay quiet.** `heartbeat_rounds_do_not_re_evaluate_a_merge` drives 100
rounds of `LeaseTick` and `ArenaGrant` past a pending merge and requires it to apply, so the
re-evaluation detector is not merely capable of firing but shown not to fire spuriously.

**One guard with no mutant, said rather than hidden:** `require_leader()` at the top of
`ClusterAgents::merge`. `propose` refuses on a follower too, so removing the line changes no
outcome and no mutant kills it. What it buys is that a node which cannot commit does not run a full
`evaluate_merge` — two scans of every touched table — to reach a verdict it can never act on. The
code says so at the call site.

## Two defects the tests found in my own code

1. **The cluster merge walked through quarantine.** `evaluate_merge` scores a branch without asking
   whether it may be merged at all, so an evaluate-then-publish coordinator published a held branch.
   `integration_quarantine` already pins that a hold a merge can walk through is not a hold. Fixed
   with a precondition check that delegates to `AgentRuntime::merge` for the refusal *and its
   reason*, so there is one copy of that wording; M9 now kills the regression.
2. **A merge waiting on a fork the cluster refused timed out saying nothing.** The ledger had
   recorded the refusal; the merge now carries it. M18.

## `cargo test` — real numbers

Per target, because a full `cargo test` on this machine is SIGTERMed mid-suite by the agent fleet
and an interrupted run reports as a pass. Durable, resumable results in `bench/evidence/F9-suite.txt`;
runner `bench/evidence/F9-run-suite.sh`.

```
LIB   test result: ok. 1092 passed; 0 failed; ...
... 85 integration targets ...
integration_cluster_agents   test result: ok. 35 passed; 0 failed; ...

86 targets, 1730 Rust tests passing.
cdc-consumer:  ok  github.com/RyanLin5967/ferrodb/cdc-consumer  17.379s   (97 Go tests)
```

Baseline before this row was 1695 Rust / 97 Go. **1695 + 35 = 1730**, which is the arithmetic that
says this row added its own tests and changed nothing else's count.

Built under CI's exact flags, clean:
`RUSTFLAGS="-D duplicate_macro_attributes -D dead_code" cargo build --lib`. No `allow(dead_code)`
was added, so nothing is owed to ledger row F-cleanup.

### Three targets failed in the loaded whole-suite pass. None is a regression, and each was
### diagnosed rather than retried until green.

- **`integration_base_backup`** — 5 failed in 0.00s: the example-staleness guard. `cargo test` does
  not rebuild examples and `src/` had changed. After `cargo build --examples`: **5 passed, 22.51s**.
- **`integration_server_stdout`** — **genuinely caused by this row.** See below.
- **`integration_consensus_failover`** — a 30s `recv_timeout` waiting for a spawned node process to
  print `READY`, exceeded while four agents were saturating this machine. Not reproduced: four
  consecutive re-runs at 3.20s, 2.78s, 2.88s, 2.83s.

## The one file outside my brief that I edited, and why

`tests/integration_server_stdout.rs` holds a repo-wide guard,
`no_test_picks_a_port_for_a_server_it_has_not_started_yet`, which scans `tests/` for any test that
binds its own socket. F9's three-node fleet does, so the guard caught it — correctly, because
binding a socket in a test is usually the discover-then-drop race of I16.

This is the case the guard's own message names as the exception: `Node::start` takes an
**already-bound** listener rather than an address, and its doc says why — every node's peer map
needs every other node's address, so binding A to discover its port and then constructing B is
circular. The listener is moved in still bound, so the port is owned continuously and there is no
window to lose. That is the opposite of discover-then-drop.

So the allowlist entry the guard asks for was added, with that reason, and the guard was then
**forced to fire with the entry in place**: a planted offender in `guard_precondition_probe.rs` was
caught by name, and the guard was shown quiet again once it was removed. The entry deliberately does
not spell the type, because the guard scans its own source and a reason that named it would make
that file an offender — which is how I found out, since my first wording did exactly that.

## Things I needed from the frozen contract and worked around, as the preamble asks

**`BranchOp::Merge` carries no changeset, and two log entries are not atomic.** The merge round is
the linearization point; the rows it publishes are ordinary redo that reaches the other nodes as
`Command::WalBatch` on a *later* round. If the owning node dies between applying its own merge round
and that redo reaching a quorum, a promoted leader holds a branch sealed as merged whose rows never
arrived. Nothing *acknowledged* is lost — `merge` returns only after the publish — but the branch
record and the trunk disagree, and no node can repair it, because the branch's rows were node-local
to the node that died.

Closing it needs either the merge command to carry the changeset, or a second variant confirming the
rows landed. `consensus/mod.rs` is frozen and carries neither, so this is **named in the module
header** rather than worked around silently, and the error on the reachable half of it
(`publish_evaluation` failing after the seal) says explicitly that the merge cannot be re-run. It is
the same boundary `BEGIN AGENT SESSION ... DURABLE` sits on: a branch whose frames are in the log
can be merged by whoever is leader, and one whose frames are not, cannot. That row is meaningless
until F10 exists, which is why `DISTRIBUTED.md` already schedules it after F10.

Narrow rather than merely unlikely, and stated with its reason: the only writer that could move the
base between this node's evaluate and its publish is this process, and it cannot — `ExecCtx` holds
`&mut Catalog` and `pgwire`'s catalog is one `Mutex<Catalog>` shared by every connection, so no
other statement runs at all while a merge holds it.

## Two properties I chose, stated rather than buried

- **Optimistic concurrency starves under sustained conflict.** A merge racing continuous writes to
  its own base is re-evaluated each time and, after a bound, **refused with the round that moved**
  rather than spun on. `DESIGN.md` §4 chose optimistic; this is that choice's cost, said out loud.
  The mechanism that removes it — ordering the merge ahead of concurrent proposals — belongs to
  whoever owns the leader's proposal loop, not to this file.
- **A fork is replicated but not awaited.** The claim "a fork costs no consensus round-trips" is
  about the agent's critical path, and `ConsensusCost` separates the two numbers so nobody has to
  guess which is meant: one proposal, zero quorum waits.

## Scope this row does not cover, by the design's own assignment

Exit criterion 10 — `BEGIN AGENT SESSION` / `MERGE` through pgwire against three nodes with a
follower redirecting — is `tools/demo-cluster.sh`'s in `DISTRIBUTED.md`'s proof table, and pgwire
and the server loop belong to other rows. The redirect itself is here and tested at this layer
(`FerroError::NotLeader` carrying the leader's client address, refusing before anything is created).
