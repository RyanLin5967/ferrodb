# F5 — membership changes: the design, before it is written

Read with `src/consensus/mod.rs` (FROZEN), `src/consensus/config.rs`, `src/consensus/election.rs`.

## The constraint that shapes everything

`Consensus`'s field set is FROZEN. **There is no field for "the membership change I have
proposed but not yet committed".** Every rule below has to be checkable from:
`self_id, role, hard, leader, cfg, campaign, acked, behind, unjoined, progress, commit,
applied, durable, last_term, last_round, snapshot_round/term, rng`.

`replicate.rs` (F2) is still `unimplemented!()` on this branch — `on_propose`, `on_persisted`
and `on_append_msg` all panic. F5 is wave B and was dispatched before F2 merged.

## D1. A configuration takes effect when its entry COMMITS, not when it is appended

`election.rs` says so in the frozen-by-merge sense: "`apply_config` — the caller calls this
when a `Command::Membership` commits". So while a change is in flight, `self.cfg` is the
**previous** configuration — which is exactly "the set that created it" that DISTRIBUTED.md §F5
requires the acknowledgement to be counted in.

This differs from Raft, where a node uses the newest config in its log even uncommitted.
Claimed safe here because single-node changes plus the one-at-a-time precondition mean at most
two configurations are ever live and they differ by one node, so their majorities intersect.

## D2. `acked[n]` means "n is known to hold this configuration in its durable log"

Never expires: durability does not expire, which is why a `(version, term)` pair rather than a
version is safe to keep across terms. Two different configurations can both be version 2,
created by different leaders in different terms; an ack of `(2, term 3)` must not count toward
`(2, term 5)`. Comparison is `acked[n] >= wanted` on the PAIR (`CfgAt` derives Ord as
(version, term)).

`>=` rather than `==` deliberately: a node holding a LATER configuration necessarily held the
log prefix containing this one (log matching), and requiring equality would stall a leader for
ever against a member that jumped versions through a snapshot.

## D3. The precondition, stated exactly

A change may begin only when ALL of:
1. this node is Leader;
2. **no change is in flight**: `acked[self_id] <= cfg.at()` — the newest configuration in this
   node's own log is the one it has applied;
3. **the previous change is durable on a majority of the set that created it**: the voters of
   `self.cfg` for whom `acked[n] >= self.cfg.at()` are a quorum of `self.cfg`.
Counted over `self.cfg.members()` only — learners never, departed nodes never.

## D4. How the leader knows (2), given no field for it

One new seam, `pub fn note_config_in_log(&mut self, at: CfgAt)`: the caller, which owns the log,
reports the newest configuration in this node's durable log. It is the authority, so it covers
append, truncation, and a new leader that inherited an uncommitted membership entry — none of
which `Consensus` can see, because it holds no log. Stored as `acked[self_id]`.

Plus `pub(crate) fn begin_membership(&mut self, cfg) -> Result<(), FerroError>` — F2's
`on_propose` calls it before appending a `Command::Membership`; it re-checks D3 and records
`acked[self_id] = cfg.at()` so the window between proposing and fsyncing cannot admit a second
change. Recording before the fsync is deliberate and can only REFUSE a change, never permit one.

## D5. A peer's ack, given `AppendResp` carries no configuration field

`Body::AppendResp { success, matched, hint, digest }` is frozen and has no room for one. So the
leader derives it: it created the entry, so its caller's log knows which round holds it, and a
peer whose `matched` covers that round holds that entry (log matching). The caller reports it
with `note_config_ack(peer, at)`.

## D6. Change is an enum that cannot express a two-node change

```rust
pub enum Change { AddLearner(NodeId), Promote(NodeId), Remove(NodeId) }
```
Joint consensus and multi-node changes are unrepresentable rather than refused. `AddLearner`
then `Promote` is the only way to add a voter; `Promote` requires
`progress[n].matched >= self.commit`.

## D7. Refusals reuse existing `FerroError` variants

`error.rs` is not this row's file. Not-leader → `NotLeader { leader: None }` (the field is
documented as a dialable ADDRESS and `Consensus` holds `NodeId`s, so filling it would be a lie
the transport can correct). Every other refusal → `Constraint(String)`.

## The rules that get a named test and a mutant

R1 only a leader may begin a change
R2 no change while one is in flight (D3.2)
R3 the previous change must be durable on a majority of the CREATING set (D3.3)
R4 acks are counted on the (version, term) pair, never the version alone
R5 acks are counted over the current voters only — not learners, not departed nodes
R6 a node is added as a LEARNER first, never straight to voter
R7 a learner is promoted only once `matched >= commit`
R8 a learner is replicated to but never counted (quorum unchanged by an add)
R9 a change may not empty the voter set
R10 3 -> 5 has no two-leader window
R11 a removed node that keeps running cannot disrupt the cluster
