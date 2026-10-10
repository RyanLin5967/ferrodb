# Agent SQL and merge evidence

Source reviewed: `fc9556a742f61884a3011ccc28e78f160a8d57fe`. This note reads implementation and existing tests; it does not claim newly executed tests.

## Scope and top-level diagram

Use these exact distinctions in the atlas:

```text
SQL statement
  ├─ no agent session → ordinary executor → shared heap + MVCC + WAL
  └─ agent session → AgentRuntime
       ├─ SELECT → current committed heap scan + private workspace overlay
       ├─ DML → validate → stage row state + capture effects → append TEL → mirror COW pages
       └─ MERGE → evaluate effects and checks → recheck base → publish rows in one WAL txn
                                                        → record history → seal branch
```

Dispatch: `src/execution/executor.rs:173`; agent DML dispatch: `src/agent_sql/dispatch.rs:545`.
Ordinary transactions and branch sessions are distinct; `BEGIN AGENT SESSION` refuses an open ordinary transaction (`dispatch.rs:439`).

## Branch state: exact box contents

| Node label | Meaning and evidence |
|---|---|
| Durable branch record | Branch identity/generation, parent, root page ID, lease, state, capability envelope. |
| Private workspace (RAM) | Persistent maps of `(table ID, row ID) → Present(row)/Deleted`, first-touch row witnesses, table names; task frame, schema edits, fork sequence. `runtime.rs:360`. |
| Typed effect frame | One task's ordered operations and captured guards; distinct from physical WAL. `runtime.rs:3107`. |
| Branch COW tree | Encoded staged row state mirrored onto the branch's pages. It is not the normal SQL read/merge authority. `runtime.rs:3131`. |
| Published operation history (RAM) | Per-cell index of applied operations, global apply sequence, version map, merge records, read captures. `runtime.rs:5308`, `5351`. |

`begin_session_as_staged` records `fork_seq = state.apply_seq`; it shares the parent's immutable workspace maps by cloning roots (`runtime.rs:1717`, `1737`). Later private parent edits are isolated.
The child starts its own new `TxnFrame`, rather than cloning the parent's effect frame (`runtime.rs:1770`). Inherited row state and task identities are separate fields.
`base_rows.insert_if_absent` captures each row's first-touch before image (`runtime.rs:3107`). Do not label this a full database snapshot at fork.

Reopen reads durable branch roots but constructs fresh runtime `State`, ancestry, and attestation maps (`runtime.rs:1381–1407`). Durable TEL and pages do not automatically reconstruct unfinished SQL workspaces or admission history.

## Read path: exact edges

1. `AgentRuntime::select` binds a single-table projection/predicate; joins are refused (`runtime.rs:2250–2267`).
2. `visible_rows_where` runs the ordinary planner's heap/index scan with a current committed read view (`runtime.rs:2152`, `6771–6780`).
3. Overlay staged rows by logical key: private Present replaces base; Deleted removes base; private inserts add rows. Re-evaluate predicate on private versions (`runtime.rs:2198`).
4. Record the reading session's dependencies; then project requested columns (`runtime.rs:2296`).

Diagram edge labels: **“current committed rows”** from heap; **“private overrides + deletions”** from workspace; combined output **“agent-visible rows”**.
Never draw SQL reads from the COW tree alone or label the whole SQL branch view “frozen at fork”.

Tests: `tests/integration_trunk_tree_authority.rs:122` and `:170` prove branch writes have pages, SQL publication reaches heap, and trunk's COW tree does not hold ordinary SQL rows.
Overlay predicate details: `tests/d57_overlay_probe.rs`; inherited private-state isolation: `tests/d27_fork_shares_without_leaking.rs`.

## Write path: effects, guards, staging, mirroring

`branch_update` binds expressions, scans visible rows, computes per-row after images, captures operation witnesses and WHERE guards (`runtime.rs:2595–2741`).

| SQL expression | Captured effect |
|---|---|
| `qty = qty + 5` | `Add(+5)` |
| `qty = qty - 3` | `Add(-3)` |
| `qty = 15` | `Assign(15)` |
| Other expressions, including equivalent unrecognized arithmetic shapes | Evaluated result as `Assign(value)` |

Recognition is specifically same-column on the left and numeric literal on the right (`runtime.rs:6856`). Do not label all arithmetic “automatically understood”.
The cell resolver supports Assign/Add/Min/Max; TEL contains other operation variants, but ordinary SQL capture does not expose all of them (`merge_engine.rs:145`).

`stage_all` order (`runtime.rs:2990–3156`):

```text
Check whole statement's capability envelope + escrow availability
  → charge durable row-write budget; charge escrow
  → update workspace rows + first-touch base_rows + cumulative task frame
  → append/extend effect-log frame
  → put/delete row in branch COW tree; publish changed root in branch catalog
```

Policy/escrow refusals are decided before staging rows. Later I/O failures are not rolled back across all these stores; no all-or-nothing durable staging claim.
Durable TEL stores one frame identity, extends new tails, treats identical retry as no-op, and refuses contradictory reuse (`src/tel/log.rs:1625`). Each nonempty append performs `sync_data` (`log.rs:1581`).
Frame deduplication prevents counting an Add twice; this is not a proof of exactly-once whole-merge retry after crashes.

## Production SQL MERGE: evaluation, decision, publication

Production calls `AgentRuntime::merge` (`runtime.rs:3376`), not `tel::Engine` or `cow::merge3`.

**Evaluate (no publication):**

1. Snapshot private maps, effects, guards, dependency captures and schema edits (`runtime.rs:4269`).
2. Resolve schema changes; get current target heap rows, conform row shapes as needed.
3. Form per-cell input: before-image witness, current target value, branch's composed effect, target's recorded effects since `fork_seq` (`runtime.rs:4520`, `4627`).
4. Resolve each changed cell and construct proposed row changes (`merge_engine.rs:199`).
5. Check captured guards against the **target before applying this branch's effects** (`runtime.rs:4562`, `4704`).
6. Validate supported read premises; evaluate supplied assertions against proposed resulting state; run gate (`runtime.rs:4741–4830`).
7. Carry proposed writes, outcomes, checks, and base fingerprint to publication (`runtime.rs:4834`).

**Decision:**

| Outcome | Production behavior |
|---|---|
| Clean | No incompatible concurrent effect; eligible for publication if gate passes. |
| Commuting | Compatible effects compose; still must pass checks. |
| ResolvedWithLoss | Explicit policy overwrote another effect; report discarded write. |
| Conflict | Publish no rows; branch remains available for correction. |
| Gate not Pass | Publish no rows; production MERGE quarantines branch for inspection. |

All row/schema conflicts prevent whole-row-batch publication (`runtime.rs:3408`). Gate outcome is separate from merge conflict outcome; draw separate boxes or clearly separate badges.

**Publish:**

1. Require passing gate and no conflicts (`runtime.rs:4900`).
2. Recompute base fingerprint; refuse stale evaluation (`runtime.rs:4916`).
3. Preflight complete schema edit plans and all row shapes; apply schema edits first (`runtime.rs:4960–5115`).
4. Reserve logical version sequence (`runtime.rs:5152`).
5. Begin ordinary transaction; bind author; apply all accepted rows; abort on row error; commit once (`runtime.rs:5168–5209`).
6. Record applied operations/provenance, attest merge, then seal source branch (`runtime.rs:5211–5231`).

Diagram: place **only row writes + WAL commit** inside the atomic transaction boundary. DDL rewrites, external provenance artifacts, post-commit history and branch seal are outside it.
Preflight handles predictable refusals, but I/O failures can leave schema changes without row publication; multi-table DDL is not crash-atomic (`runtime.rs:4995`). Post-commit failures cannot undo already committed rows.
Source `tests/integration_crash_safety.rs:162` tests killed row publication; it does not establish atomicity for all these side effects.

## Merge algebra example and necessary qualification

Use the existing SQL example, which captures commuting arithmetic without an earlier SELECT:

```text
Shared qty = 20
A: UPDATE inventory SET qty = qty - 5 WHERE id = 1;
B: UPDATE inventory SET qty = qty - 3 WHERE id = 1;
Merge A: Clean → shared 15
Merge B: Commuting → shared 12 (apply -3 to current 15)
```

Test: `tests/integration_sql_on_durable_branches.rs:146`.
Do not claim both merges always pass if they first explicitly SELECT that same row: B's exact read premise may become stale and quarantine B.

`resolve_cell` (`merge_engine.rs:199–271`): no target effect → apply branch op; commuting ops → apply branch op to current target; identical Assign values → clean; incompatible effects default Reject.
LWW is incoming-merge-wins, not timestamp comparison, and returns ResolvedWithLoss. MultiValue/Additive enum policies do not make arbitrary incompatible assignments merge successfully.
Concurrent inserts at an existing logical key conflict (`runtime.rs:4574`); deleting a row changed on the target conflicts (`runtime.rs:4593`).
An ordinary SQL write has no agent effect record. `concurrent_op` falls back to changed value as opaque Assign when no recorded effect exists (`runtime.rs:5339`). Do not claim all paths preserve arithmetic intent.

## Guards, assertions, and dependency limits

**Guard = precondition.** `qty >= 12` must hold before subtracting 12. Two such withdrawals from 20: first sees 20 and passes; second sees 8 and fails. `qty >= 0` would allow the second withdrawal and resulting -4.
**Assertion = postcondition.** Check `qty >= 0` against the proposed composed result. Production MERGE supplies no assertions; SIMULATE supplies explicit declarations (`runtime.rs:3386`, `simulate.rs:367`).
Tests: `tests/guard_precondition_probe.rs:118`, `:138`, `:288`. Old comments in `merge_engine.rs:3` and `runtime.rs:4703` incorrectly describe post-state guard checking; actual `admit_state` construction governs.

**Exact read premise:** point SELECT records runtime version references; compare against current runtime version map. That map is updated by merge publication, not every ordinary DML write (`runtime.rs:2375`, `4741`; `gate.rs:250`).
**Point UPDATE row targeting:** retained for causal history, excluded from inspection read-set admission. This deliberately permits Add composition (`runtime.rs:2437`, `6330`).
**Predicate reads:** record summaries for causal/dependency tracking; merge evaluation only marks them approximate. No moved predicate rows are computed there (`runtime.rs:4799`).
Crucial: `ReadPremiseCheck::evaluate` returns None if its exact moved list is empty (`gate.rs:278`), and gate reacts only to fired findings (`gate.rs:181`). **A predicate-only read does not automatically quarantine and has no complete phantom validation.**
Blind-write count is informational, explicitly excluded from admission (`runtime.rs:4809`).
Do not label agent isolation serializable, whole-fork snapshot isolation, or complete validation of all reads.

**Stale-evaluation check differs from prior-read validation.** Fingerprint includes change counters for involved target/assertion tables plus tracked exact premise version stamps (`runtime.rs:4495`, `5245`). Table counters catch ordinary DML changing an evaluated table between evaluate/publish, but don't retroactively close arbitrary read-premise gaps throughout the branch lifetime.
Test: `tests/integration_simulate.rs:342`; cross-table exact premise: `:1014`.

## Peripheral features: show as small side attachments

| Feature | Attach to | Boundary |
|---|---|---|
| DIFF | Private state/effects vs current target | Structured changes, not publication. `runtime.rs:3160`. |
| Sibling merge | Branch workspace → another branch workspace | Uses fork-point lookup and staged frames; does not publish to heap. Source/target remain live. Per-table staging lacks whole-operation failure atomicity. `runtime.rs:3871`, `4176`. |
| Cherry-pick | Published operation history → branch staging | Selects sequence-numbered effects; explicitly refuses selections spanning multiple tables. `runtime.rs:3488`; `tests/integration_cherry_pick.rs:340`. |
| Revert | Published effects + dependency graph → inverse ordinary SQL writes | Halt on dependents or explicit cascade; not root rewind; no single all-cascade transaction. `runtime.rs:5791`, `5831`. |
| SIMULATE | Fork → candidate writes → same evaluate/publish path | Evaluate candidates on common base; re-evaluate before each greedy admission. Assertions judge legality; no objective function ranks equally legal candidates, so declaration order decides ties. `simulate.rs:367`, `:523`, `:560`. |
| Provenance | Read captures + published images/author | Records task lineage and causal dependencies; its coverage is not equivalent to serializable admission. `runtime.rs:5351`, `6355`. |
| Capability envelope / escrow | Before branch staging | Limit allowed row-image changes and reserve bounded quantity. Ordinary SQL outside branch path bypasses these checks. `runtime.rs:2990`. |

## Misleading diagram labels to reject

- “Fork copies the SQL database” / “fork pins full committed snapshot”.
- “COW root is the authoritative source for every SQL read”.
- “MERGE replaces trunk's COW root”. It publishes accepted effects into ordinary heap.
- “A SQL branch is cheaper than an ordinary transaction”. Different workload/overhead; no such general claim.
- “Commutativity guarantees business invariants” / “WHERE guard checks final state”.
- “Predicate read always quarantines” / “all read-write conflicts detected”.
- “Durable TEL means restart automatically resumes an agent”.
- “Entire merge incl. DDL, provenance, and sealing is one atomic transaction”.
