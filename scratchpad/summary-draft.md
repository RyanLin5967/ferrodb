# F4 — cluster-ifying node-local state

Branch `F4-clusterstate`, worktree `/Users/idide/wt/ferrodb-F4-clusterstate`.

## What the row asked for, and what shipped

Three node-local counters had to become leader-granted, each behind a guard that **refuses** rather
than falling back, with single-node operation unchanged. All three are done, plus a fourth the brief
did not name and two defects in my own first attempt.

| what | was | now |
|---|---|---|
| `arena.rs` `next_extent_start` | `AtomicU32::fetch_add(extent_pages)` | `GrantedCounter`, fed by `Command::ArenaGrant` |
| `arena.rs` `next_arena_id` | `AtomicU32::fetch_add(1)` | `GrantedCounter`, fed by the **same** entry |
| `txn.rs` `next_txn_id` | `pub AtomicU64::fetch_add(1)` | `GrantedCounter`, fed by `Command::TxnIdRange` |
| `branch/types.rs` lease clock | `SystemTime::now()` | the last applied `Command::LeaseTick` |
| **`txn.rs` `commits_since_checkpoint`** | node-local `fetch_add` firing `wal.truncate` | withheld on a member; `Command::Checkpoint` drives it |

## The shape: one consume path, two authorities

<!-- FILL: keep -->

## Deviation from strict file ownership, and why

<!-- FILL -->

## Single-node operation

<!-- FILL: numbers -->

## Mutation evidence

<!-- FILL: final tally -->

## What the adversarial pass found

<!-- FILL -->

## Findings for other rows

<!-- FILL -->
