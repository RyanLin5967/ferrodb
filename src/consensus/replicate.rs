//! F2 — log replication: getting the leader's rounds onto a quorum, and deciding what is committed.
//!
//! **OWNER: agent F2.** The universal term rules are already applied in `mod.rs` before anything
//! here is called — do not re-implement them.
//!
//! # The four rules, and the data loss each one prevents
//!
//! 1. **Quorum is counted over [`Progress::matched`], never over [`Progress::next`].** `next` is
//!    optimism: it is where the leader *hopes* the peer is, and it is corrected by a refusal.
//!    Counting it commits a round nobody holds, so a leader change loses it while a client has
//!    already been told it committed.
//! 2. **A leader may commit a round of its OWN term directly; an inherited round commits only as a
//!    side effect of a later one** (Raft §5.4.2). This is the one that looks like an optimisation
//!    and is not: [`the figure-8 test`](tests_replicate) plays out the sequence where a round that
//!    a majority holds is still overwritable by a node that can still win an election, and
//!    committing it on the replica count alone loses acknowledged data.
//! 3. **A follower whose `(prev_round, prev_term)` does not match truncates its own conflicting
//!    suffix and answers with a `hint`,** which is the first round of the term run it was refused
//!    in. The leader backs up to that in ONE step instead of probing backwards a round at a time —
//!    the difference between a constant and a linear recovery after every leader change.
//! 4. **An ack is emitted on [`Event::Persisted`], never on receipt of the append.** A follower
//!    that acks a round it has not fsynced turns a correlated power loss — a rack, a data centre,
//!    a rolling deploy — into acknowledged data loss, because the quorum that answered was a
//!    quorum of volatile memory.
//!
//! [`Event::Persisted`]: super::Event::Persisted
//!
//! # The divergence detector
//!
//! Every successful `AppendResp` carries a rolling digest of the log through `matched`, and a
//! leader whose own digest at that round disagrees **latches the peer as diverged and stops
//! counting it toward any quorum**. `DISTRIBUTED.md` §F0 argues that a follower's WAL byte stream
//! must be a prefix of its leader's and then says the argument is an argument and not a
//! measurement, which is why it gets this. The digest is folded over the log entries, and a
//! [`Command::WalBatch`] *is* the WAL bytes, so a flipped byte in a follower's redo stream changes
//! the digest at that round and every round above it.
//!
//! Two limits, stated rather than left to be discovered:
//!
//! * It compares what was **agreed** — the entries — and not what a node's local storage engine
//!   did with them afterwards. Catching that would need [`Event::Persisted`] to carry the storage
//!   engine's own digest, and it carries only `(term, round)`.
//! * The latch is **in memory only**. [`crate::replication::ReplicaApplier`] learned this the hard
//!   way and grew a durable sibling (`ReplicaPosition`); `Consensus` touches no disk and the frozen
//!   `Action` set has no way to persist one, so a leader restart re-counts a diverged peer. Both
//!   are named in the summary as contract gaps, not designed around.
//!
//! # Where this node's own log lives, and why it is not in `Consensus`
//!
//! **`Consensus` holds no entries.** Its whole log surface is five scalars — `last_term`,
//! `last_round`, `durable`, `snapshot_round`, `snapshot_term` — and `Action::Persist` is
//! write-only: there is no event, action or accessor that reads an entry back. Rules 2 and 3 above
//! are both *term-at-an-arbitrary-round* questions, so neither is implementable against those
//! scalars, and the field set in `mod.rs` is frozen.
//!
//! [`Progress`] is the one type inside that frozen field set this file owns, so the node's own log
//! lives in [`LogTail`], in the `progress` entry keyed by the node's **own** id. That is a
//! workaround for a missing `Consensus::log` field and it is named as one. It is reached only
//! through [`Consensus::term_at`], [`Consensus::entries_from`] and their siblings, so moving it to
//! a real field is a change to this file alone.
//!
//! It has one sharp edge and therefore one detector: a `progress.clear()` anywhere else would
//! destroy the log silently. [`Consensus::ensure_log`] runs at the top of every entry point and
//! **panics, naming the cause**, if the tail no longer describes `last_round`. Use
//! [`Consensus::init_leader_progress`] to (re)initialise per-peer state; it preserves the log.

use super::config::Config;
use super::{
    Action, Body, BranchOp, Command, Consensus, Entry, HardState, Message, NodeId, Role, Round, Term,
};
use crate::catalog::column::DataType;
use crate::error::FerroError;
use crate::wal::log::{ColumnAlteration, DdlOp};

#[cfg(test)]
#[path = "tests_replicate.rs"]
mod tests_replicate;

/// How many entries one `Append` may carry.
///
/// A bound and not a "send everything": a leader whose follower is ten million rounds behind would
/// otherwise build one message the size of its log, and the peer at the other end does not get to
/// choose this process's memory usage — the same reasoning `transport.rs` applies to
/// `MAX_FRAME_BYTES`. Catch-up then takes several round trips, which is the correct trade: the
/// alternative is one round trip that cannot be delivered.
pub(crate) const MAX_ENTRIES_PER_APPEND: usize = 64;

/// Per-peer replication progress.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Progress {
    /// The next round to send this peer. Optimism — it may be wrong and is corrected by a refusal.
    pub next: Round,
    /// The highest round this node knows is on that peer **and agrees with its own log there**.
    /// Quorum is counted over this and never over `next`: counting optimism commits rounds nobody
    /// holds.
    ///
    /// In the entry keyed by the node's **own** id it means the same thing about this node: the
    /// highest round this node's log is confirmed to agree with its current leader's. A leader
    /// trivially agrees with itself, so there it is this node's `durable`; on a follower it is
    /// `prev_round + entries.len()` of the newest `Append` it ACCEPTED, and **not** its own log
    /// length. Those differ exactly when a follower holds a tail some other leader gave it, and
    /// reporting the log length there makes the leader count a replica of entries it never
    /// sent — see [`Consensus::acknowledgement`].
    pub matched: Round,
    /// Ticks since this peer last answered, feeding the leader's own lease.
    pub silent: u32,
    /// This peer needs a snapshot: the rounds it asked for are below our log start.
    pub needs_snapshot: bool,

    /// Why this peer stopped counting toward a quorum, once it has.
    ///
    /// Latched and deliberately never cleared, exactly as
    /// [`crate::replication::ReplicaApplier`]'s `diverged` is: the peer keeps answering, so
    /// without a latch it would be re-counted on the next ack that happened to agree, and an
    /// operator reading "it caught up again" would take the divergence for a transient. An
    /// `Option<String>` rather than a `bool` because the round it diverged at is the only trace of
    /// when a byte-level disagreement began.
    pub diverged: Option<String>,

    /// **Only in the entry keyed by the node's own id**: this node's own round log.
    ///
    /// See the module header. The frozen `Consensus` has no field for entries and this is the one
    /// type in its field set that `replicate.rs` owns.
    pub(crate) own_log: Option<LogTail>,

    /// F6: the snapshot this **leader** is streaming to this peer, if any.
    ///
    /// Subordinate to `needs_snapshot`, never a second authority. `on_append_resp` clears
    /// `needs_snapshot` on every success, so two independent flags would disagree the moment a
    /// peer answered mid-transfer; instead `send_append_to` reads the cursor only inside the
    /// `needs_snapshot` branch, and `on_append_resp` drops it wherever it clears the flag.
    pub(crate) sending: Option<super::snapshot::SendCursor>,

    /// **Only in the entry keyed by the node's own id**: F6's transfer *into* this node.
    ///
    /// Beside the log rather than in `Consensus` for the same reason [`LogTail`] is — the frozen
    /// field set has no room — and it is O(1) in the size of the snapshot by construction: see
    /// `snapshot.rs`, which owns every rule about it.
    pub(crate) receiving: Option<super::snapshot::RecvCursor>,
}

/// The node's own log above the snapshot floor: contiguous, ascending, and digested as it grows.
///
/// Rounds are contiguous from `base + 1`, so `entries[i].round == base + 1 + i` and a hole is a
/// panic rather than a silently accepted gap — which is the entire reason `DISTRIBUTED.md` §F0
/// chose a round over an LSN.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct LogTail {
    /// The round this tail follows — the snapshot floor. `0` before any snapshot.
    base: Round,
    /// The rolling digest through `base`, which a snapshot install supplies. `0` means "nothing
    /// below me", matching `AppendResp`'s reading of a zero digest.
    base_digest: u64,
    entries: Vec<Entry>,
    /// `digests[i]` is the rolling digest through `entries[i].round`. Held beside the entries
    /// rather than recomputed, so answering an ack is O(1) instead of O(log length).
    digests: Vec<u64>,

    /// The term this node held the last time a truncation **rewrote** these rounds.
    ///
    /// It exists to answer one question `Event::Persisted { term, round }` cannot answer on its
    /// own: a round number does not name an entry. A persist that was in flight when a truncation
    /// landed reports the round it made durable, and by then a *different* entry occupies that
    /// round — so taking it would ack, and let a leader commit, bytes that are not on this node's
    /// disk. That is rule 4's failure arriving through the mitigation written to prevent it.
    ///
    /// The comparison is sound because **within one term the entry at a round never changes once
    /// accepted**: a truncation happens only where the incoming term differs from the term this
    /// node already holds at that round, and one term has one leader, which never sends two
    /// different entries for one round. So a `Persisted` whose term is at least this one describes
    /// the log as it now stands; one from an earlier term may not, and is ignored.
    rewritten_in_term: Term,
}

impl LogTail {
    fn rebased(base: Round, base_digest: u64) -> Self {
        LogTail {
            base,
            base_digest,
            entries: Vec::new(),
            digests: Vec::new(),
            rewritten_in_term: 0,
        }
    }

    fn last_round(&self) -> Round {
        self.base + self.entries.len() as Round
    }

    /// The term of the entry at `round`, or `None` if this tail does not hold it — either because
    /// it is above the tail (a gap) or below `base` (compacted into a snapshot).
    fn term_at(&self, round: Round) -> Option<Term> {
        if round <= self.base || round > self.last_round() {
            return None;
        }
        Some(self.entries[(round - self.base - 1) as usize].term)
    }

    fn digest_at(&self, round: Round) -> Option<u64> {
        if round == self.base {
            return Some(self.base_digest);
        }
        if round < self.base || round > self.last_round() {
            return None;
        }
        Some(self.digests[(round - self.base - 1) as usize])
    }

    fn slice_from(&self, round: Round, limit: usize) -> Vec<Entry> {
        if round <= self.base || round > self.last_round() {
            return Vec::new();
        }
        let from = (round - self.base - 1) as usize;
        let to = (from + limit).min(self.entries.len());
        self.entries[from..to].to_vec()
    }

    fn push(&mut self, e: Entry) {
        assert_eq!(
            e.round,
            self.last_round() + 1,
            "the round log is contiguous by construction: entry {} was appended after {}, which \
             would leave a hole no arithmetic could later distinguish from a gap",
            e.round,
            self.last_round()
        );
        let prev = self.digests.last().copied().unwrap_or(self.base_digest);
        let d = fold_entry(prev, &e);
        self.entries.push(e);
        self.digests.push(d);
    }

    /// Drop everything at or below `base`, keeping the rest.
    ///
    /// The counterpart of [`LogTail::rebased`] for a node checkpointing its **own** log rather than
    /// installing somebody else's. The surviving digests are carried across unchanged, which is
    /// sound because a rolling digest is an absolute value at a round and not an offset into this
    /// vector — so a compaction cannot change what this node reports at any round it still holds.
    fn compact_to(&mut self, base: Round, base_digest: u64) {
        assert!(
            base > self.base && base <= self.last_round(),
            "compact_to({base}) on a tail covering ({}, {}]: the caller must have checked both              ends, because dropping the wrong prefix silently renumbers every entry above it",
            self.base,
            self.last_round()
        );
        let drop = (base - self.base) as usize;
        self.entries.drain(..drop);
        self.digests.drain(..drop);
        self.base = base;
        self.base_digest = base_digest;
    }

    /// Drop `from` and everything above it.
    fn truncate_from(&mut self, from: Round) {
        if from <= self.base {
            return;
        }
        let keep = (from - self.base - 1) as usize;
        if keep < self.entries.len() {
            self.entries.truncate(keep);
            self.digests.truncate(keep);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The entry digest.
// ---------------------------------------------------------------------------------------------

/// FNV-1a, 64-bit, resumable, so a digest can be folded over many pieces without concatenating
/// them.
///
/// A second copy of `agent_sql::runtime::fnv64_update`, which is module-private and in a file this
/// row does not own. The constants are the published FNV-1a-64 offset basis and prime, and
/// `tests_replicate` pins them against the published vectors rather than against the other copy,
/// so the two cannot drift into agreeing on a wrong answer.
///
/// Not a dependency: this crate carries zero runtime dependencies and that is a product claim.
pub(crate) fn fnv64_update(mut h: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

pub(crate) const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// Fold a variable-length piece in, length-prefixed.
///
/// **The length prefix is load-bearing.** Without it `("ab", "c")` and `("a", "bc")` fold to the
/// same digest, so two nodes that framed the same bytes differently would agree — which is
/// precisely the divergence this digest exists to catch.
fn fold_bytes(h: u64, bytes: &[u8]) -> u64 {
    let h = fnv64_update(h, &(bytes.len() as u64).to_be_bytes());
    fnv64_update(h, bytes)
}

fn fold_str(h: u64, s: &str) -> u64 {
    fold_bytes(h, s.as_bytes())
}

fn fold_u64(h: u64, v: u64) -> u64 {
    fnv64_update(h, &v.to_be_bytes())
}

fn fold_tag(h: u64, tag: u8) -> u64 {
    fnv64_update(h, &[tag])
}

/// The tag bytes match `wal::log::write_data_type`'s exactly, so the two descriptions of one type
/// cannot drift. Written as an exhaustive `match` rather than a `Debug` string for the same reason
/// that function gives: a type added to [`DataType`] must fail to compile here instead of
/// acquiring a second encoding.
fn fold_data_type(h: u64, ty: &DataType) -> u64 {
    match ty {
        DataType::Integer => fold_tag(h, 0),
        DataType::Float => fold_tag(h, 1),
        DataType::Boolean => fold_tag(h, 2),
        DataType::Varchar(n) => fold_u64(fold_tag(h, 3), *n as u64),
        DataType::BigInt => fold_tag(h, 4),
        DataType::Decimal => fold_tag(h, 5),
        DataType::Timestamp => fold_tag(h, 6),
    }
}

fn fold_ddl_op(h: u64, op: &DdlOp) -> u64 {
    match op {
        DdlOp::CreateTable => fold_tag(h, 0),
        DdlOp::DropTable => fold_tag(h, 1),
        DdlOp::AlterColumn(alt) => {
            let h = fold_tag(h, 2);
            match alt {
                ColumnAlteration::Add { column } => fold_str(fold_tag(h, 0), column),
                ColumnAlteration::Rename { from, to } => {
                    fold_str(fold_str(fold_tag(h, 1), from), to)
                }
                ColumnAlteration::Retype { column, from } => {
                    fold_data_type(fold_str(fold_tag(h, 2), column), from)
                }
            }
        }
    }
}

fn fold_config(h: u64, c: &Config) -> u64 {
    let mut h = fold_u64(fold_u64(h, c.version), c.term);
    h = fold_u64(h, c.members().len() as u64);
    for n in c.members() {
        h = fold_u64(h, n.0 as u64);
    }
    h = fold_u64(h, c.learners().len() as u64);
    for n in c.learners() {
        h = fold_u64(h, n.0 as u64);
    }
    h
}

fn fold_command(h: u64, c: &Command) -> u64 {
    match c {
        Command::WalBatch { start_lsn, bytes } => fold_bytes(fold_u64(fold_tag(h, 0), *start_lsn), bytes),
        Command::Catalog { op, table, columns } => {
            let mut h = fold_str(fold_ddl_op(fold_tag(h, 1), op), table);
            h = fold_u64(h, columns.len() as u64);
            for (name, ty, nullable) in columns {
                h = fold_tag(fold_data_type(fold_str(h, name), ty), *nullable as u8);
            }
            h
        }
        Command::Branch { op } => {
            let h = fold_tag(h, 2);
            match op {
                BranchOp::Fork { child, parent, fork_epoch, lease_millis } => fold_u64(
                    fold_u64(fold_u64(fold_u64(fold_tag(h, 0), *child), *parent), *fork_epoch),
                    *lease_millis,
                ),
                BranchOp::Merge { branch, base_round } => {
                    fold_u64(fold_u64(fold_tag(h, 1), *branch), *base_round)
                }
                BranchOp::Abandon { branch } => fold_u64(fold_tag(h, 2), *branch),
                BranchOp::Reap { branch, generation } => {
                    fold_u64(fold_u64(fold_tag(h, 3), *branch), *generation as u64)
                }
            }
        }
        Command::ArenaGrant { node, first_page, page_count } => fold_u64(
            fold_u64(fold_u64(fold_tag(h, 3), node.0 as u64), *first_page as u64),
            *page_count as u64,
        ),
        Command::TxnIdRange { node, lo, hi } => {
            fold_u64(fold_u64(fold_u64(fold_tag(h, 4), node.0 as u64), *lo), *hi)
        }
        Command::LeaseTick { unix_millis } => fold_u64(fold_tag(h, 5), *unix_millis),
        Command::Checkpoint => fold_tag(h, 6),
        Command::Membership { config } => fold_config(fold_tag(h, 7), config),
        Command::NoOp => fold_tag(h, 8),
    }
}

/// One entry folded into the running digest.
///
/// `(term, round)` are folded as well as the command, because the log-matching property is a claim
/// about the pair and not about the payload: two nodes holding the same command at the same round
/// under *different terms* are not the same log.
fn fold_entry(prev: u64, e: &Entry) -> u64 {
    never_zero(fold_command(fold_u64(fold_u64(prev, e.term), e.round), &e.command))
}

/// Zero is reserved by [`Body::AppendResp`] for "not claiming anything", so a real digest must
/// never be it.
///
/// Substituted rather than asserted: a 1-in-2^64 collision that silently disarmed the detector
/// would be indistinguishable from a refusal, and a panic on it would take a node down for an
/// arithmetic coincidence. A separate function because a branch folded into `fold_entry` can only
/// be tested by finding a preimage of zero, which is the whole difficulty — here it is testable
/// directly.
fn never_zero(h: u64) -> u64 {
    if h == 0 { FNV_OFFSET } else { h }
}

impl Consensus {
    // -----------------------------------------------------------------------------------------
    // The log, and the detector that guards its unusual home.
    // -----------------------------------------------------------------------------------------

    /// Make sure the self-keyed `Progress` holds a tail that describes `last_round`, and **panic,
    /// naming the cause, if it does not.**
    ///
    /// The log lives in `progress[self_id]` because the frozen `Consensus` has no field for it
    /// (module header). A `progress.clear()` elsewhere, or a `last_round` moved without going
    /// through [`Consensus::append_own_entry`], destroys or desynchronises it — and a state machine
    /// that silently forgets its log answers `term_at` with `None` and refuses every append
    /// forever, which reads like a network fault. Refusing loudly here converts an invisible
    /// corruption into one line of stack trace naming the fix.
    pub(crate) fn ensure_log(&mut self) {
        let (base, last, id) = (self.snapshot_round, self.last_round, self.self_id);
        let p = self.progress.entry(id).or_default();
        match &p.own_log {
            None => {
                assert!(
                    last <= base,
                    "this node's round log is gone but it claims rounds through {last} above the \
                     snapshot floor {base}: `Consensus::progress` was cleared by code that did not \
                     know the self-keyed entry holds the log. Use \
                     `Consensus::init_leader_progress`, which preserves it, instead of \
                     `progress.clear()`"
                );
                p.own_log = Some(LogTail::rebased(base, 0));
            }
            Some(t) => {
                assert!(
                    t.base == base && t.last_round() == last,
                    "this node's round log no longer describes its own tail: the log covers \
                     ({}, {}] but the node claims ({base}, {last}]. Something moved `last_round` \
                     or `snapshot_round` without going through `append_own_entry` / the append \
                     path, so every later `term_at` answers about a log that is not there",
                    t.base,
                    t.last_round()
                );
            }
        }
    }

    /// Restore a node's durable state on start, before any event is stepped.
    ///
    /// The driver (`node.rs`) owns the disk; this is the one seam through which what it read gets
    /// back into the state machine. It exists because `LogTail` is private to this module — the
    /// node cannot rebuild the tail itself without also owning the digest chain, and two places
    /// computing that chain is how they come to disagree.
    ///
    /// **This is not a general setter.** It is only sound on a freshly constructed `Consensus`,
    /// which is asserted rather than documented: applying it to a running node would install a log
    /// under live `progress` entries that describe a different one.
    ///
    /// `voted_for` is restored with the term, and that is the whole point of the hard state: a node
    /// that comes back having forgotten its vote can vote twice in one term, electing two leaders
    /// of that term.
    pub(crate) fn restore(
        &mut self,
        hard: HardState,
        snapshot_round: Round,
        snapshot_term: Term,
        base_digest: u64,
        entries: Vec<Entry>,
    ) {
        assert!(
            self.role == Role::Follower && self.last_round == 0 && self.snapshot_round == 0,
            "restore() is only sound on a freshly constructed Consensus; node {:?} is {:?} at \
             round {} with snapshot floor {}",
            self.self_id,
            self.role,
            self.last_round,
            self.snapshot_round
        );

        self.hard = hard;
        self.snapshot_round = snapshot_round;
        self.snapshot_term = snapshot_term;
        self.last_round = snapshot_round;
        self.last_term = snapshot_term;

        // The floor is where the log starts, so the tail is rebased there before anything is
        // pushed. `base_digest` is the rolling digest at the floor, which the driver read back from
        // the record it wrote when the snapshot was installed. **Zero is not a neutral default
        // here.** Every digest above the floor chains from this value, so restoring the wrong one
        // makes a healthy node disagree with its leader at every round and be latched as diverged;
        // zero is the one value `AppendResp` reads as "not claiming anything", so it degrades the
        // detector to silent instead of to wrong. `node.rs` supplies zero only when it has no
        // record to read, and says so.
        let tail = self.progress.entry(self.self_id).or_default();
        tail.own_log = Some(LogTail::rebased(snapshot_round, base_digest));

        for e in entries {
            if e.round <= snapshot_round {
                continue;
            }
            let (term, round) = (e.term, e.round);
            self.tail_mut().push(e);
            self.last_term = term;
            self.last_round = round;
        }

        // Everything on this node's disk is by definition durable. `commit` deliberately stays at
        // the *floor* and not at the tail: durability is a local fact, and whether a round in the
        // TAIL was COMMITTED is a fact about a quorum that only a leader's `Append` can
        // re-establish. Restoring a commit index from a local log tail is how a node applies a
        // round the cluster later truncated.
        //
        // **The floor is different in kind and must be restored.** A snapshot floor exists only
        // because a snapshot covering it was taken at or below a leader's `applied`, which is at or
        // below its `commit` — so it is committed state by construction, and the storage engine
        // already holds it. Leaving `applied` at zero would send the next `Action::Apply` walking
        // rounds that no longer exist anywhere on this node, which is exactly what `node.rs`
        // refused to do while F6 was unimplemented.
        self.durable = self.last_round;
        self.commit = snapshot_round;
        self.applied = snapshot_round;
    }

    /// Rebase this node's log onto an installed snapshot's floor, discarding every entry.
    ///
    /// The seam F6 installs through, here rather than in `snapshot.rs` because [`LogTail`] and its
    /// digest chain are private to this file and two places building that chain is how they come to
    /// disagree.
    ///
    /// **The whole tail goes, not just the prefix.** A node being re-seeded holds rounds the leader
    /// serving it cannot vouch for — that is why it is being re-seeded — and `RoundLog` does the
    /// same on the disk side: a `discard_prefix` above the end of the log empties it.
    ///
    /// The caller must have already moved `snapshot_round`, `snapshot_term` and `last_round`, or
    /// [`Consensus::ensure_log`] will refuse on the very next entry point, naming this call.
    pub(crate) fn install_snapshot_tail(&mut self, base: Round, base_digest: u64) {
        let id = self.self_id;
        self.progress.entry(id).or_default().own_log = Some(LogTail::rebased(base, base_digest));
    }

    /// Drop this node's log prefix at or below `base`, keeping everything above it.
    ///
    /// The other half of the seam above, for a node checkpointing its own log. Same rule about
    /// ordering: the caller moves `snapshot_round`/`snapshot_term` in the same step, or
    /// [`Consensus::ensure_log`] refuses on the next entry point.
    pub(crate) fn compact_tail(&mut self, base: Round, base_digest: u64) {
        self.tail_mut().compact_to(base, base_digest);
    }

    /// The highest round this node's log is confirmed to agree with its current leader's.
    ///
    /// Held in the self-keyed `Progress` (module header). It is reset to zero the moment this node
    /// adopts a different leader, because nothing has been established with that one yet.
    pub(crate) fn agreed(&self) -> Round {
        self.progress.get(&self.self_id).map(|p| p.matched).unwrap_or(0)
    }

    pub(crate) fn set_agreed(&mut self, r: Round) {
        let id = self.self_id;
        self.progress.entry(id).or_default().matched = r;
    }

    fn tail(&self) -> Option<&LogTail> {
        self.progress.get(&self.self_id)?.own_log.as_ref()
    }

    fn tail_mut(&mut self) -> &mut LogTail {
        let (base, id) = (self.snapshot_round, self.self_id);
        self.progress
            .entry(id)
            .or_default()
            .own_log
            .get_or_insert_with(|| LogTail::rebased(base, 0))
    }

    /// The term of the entry at `round`, or `None` if this node cannot answer for it.
    ///
    /// `None` has two causes and the callers care about both: the round is above this node's tail
    /// (a gap — the leader must back up), or it is at or below the snapshot floor (the entry is
    /// gone — the peer needs state transfer, not entries).
    pub(crate) fn term_at(&self, round: Round) -> Option<Term> {
        if round == self.snapshot_round {
            // Covers round 0 on a node that has never snapshotted: "before the log begins" is term
            // 0, which is what a leader sends as `prev_term` for the very first round.
            return Some(self.snapshot_term);
        }
        self.tail()?.term_at(round)
    }

    /// This node's rolling digest through `round`, or `None` if it does not hold that round.
    pub(crate) fn digest_at(&self, round: Round) -> Option<u64> {
        self.tail()?.digest_at(round)
    }

    /// Entries from `round` upward, capped at [`MAX_ENTRIES_PER_APPEND`].
    pub(crate) fn entries_from(&self, round: Round) -> Vec<Entry> {
        match self.tail() {
            Some(t) => t.slice_from(round, MAX_ENTRIES_PER_APPEND),
            None => Vec::new(),
        }
    }

    /// The first round of the term run containing `round`.
    ///
    /// This is the `hint`: the leader sets `next` to it and backs up past the whole conflicting
    /// run in **one** step. Probing backwards a round at a time instead is a round trip per
    /// diverged round, which after an ordinary leader change is a linear stall on every follower
    /// at once.
    fn first_round_of_term_run(&self, round: Round) -> Round {
        let Some(t) = self.term_at(round) else { return round };
        let floor = self.snapshot_round + 1;
        let mut lo = round;
        while lo > floor && self.term_at(lo - 1) == Some(t) {
            lo -= 1;
        }
        lo
    }

    // -----------------------------------------------------------------------------------------
    // Appending on this node's own behalf, and the per-peer state a leader keeps.
    // -----------------------------------------------------------------------------------------

    /// Append one command of this node's current term to its own log and ask for it to be made
    /// durable. Returns the round it took.
    ///
    /// The **only** sanctioned way to move `last_round` upward. Everything else — the leader's
    /// `NoOp` on election included — must come through here, or the log and the scalars that
    /// describe it part company and [`Consensus::ensure_log`] fires.
    pub(crate) fn append_own_entry(&mut self, command: Command, out: &mut Vec<Action>) -> Round {
        self.ensure_log();
        let e = Entry { term: self.hard.term, round: self.last_round + 1, command };
        self.tail_mut().push(e.clone());
        self.last_round = e.round;
        self.last_term = e.term;
        out.push(Action::Persist { entries: vec![e.clone()] });
        e.round
    }

    /// (Re)initialise per-peer replication state for a term this node has just won.
    ///
    /// **Call this instead of `self.progress.clear()`.** The self-keyed entry holds this node's
    /// log (module header), so clearing the map destroys it; this preserves it, and preserves the
    /// `diverged` latches, which are latches precisely so an election cannot clear them.
    pub(crate) fn init_leader_progress(&mut self) {
        self.ensure_log();
        let next = self.last_round + 1;
        let id = self.self_id;
        let known: Vec<NodeId> = self
            .cfg
            .members()
            .iter()
            .chain(self.cfg.learners().iter())
            .copied()
            .collect();

        // A node no longer in the configuration is no longer replicated to. The self entry stays
        // whatever the configuration says, because it is this node's log and not a peer record.
        self.progress.retain(|n, _| *n == id || known.contains(n));

        for n in known {
            let p = self.progress.entry(n).or_default();
            p.next = next;
            p.matched = 0;
            p.silent = 0;
            p.needs_snapshot = false;
            // A transfer belongs to the leader that armed it: its payload is that leader's image
            // at that leader's `applied`, and this node has just established a different one.
            // Reset field by field, like everything above it, so the log and the divergence
            // latches survive — see this function's own doc.
            p.sending = None;
        }
        // Written after the loop because `members()` contains this node: a leader's own replica is
        // exactly as far along as its own durability, and starting it at 0 would make a leader
        // wait for a quorum that includes a nonexistent copy of itself.
        let durable = self.durable;
        let p = self.progress.entry(id).or_default();
        p.next = next;
        p.matched = durable;
        p.needs_snapshot = false;
        p.sending = None;
    }

    /// Everything a node must do on winning an election: per-peer state, the term-establishing
    /// `NoOp`, and the first append to every peer.
    ///
    /// The `NoOp` is required and not decorative — see [`Command::NoOp`]. Without a round of its
    /// own term a leader can never satisfy rule 2 above, so it can never advance the commit index
    /// at all and its followers never learn what is committed.
    pub(crate) fn on_became_leader(&mut self, out: &mut Vec<Action>) {
        self.init_leader_progress();
        self.append_own_entry(Command::NoOp, out);
        let next = self.last_round + 1;
        for p in self.progress.values_mut() {
            p.next = next;
        }
        self.progress.entry(self.self_id).or_default().next = next;
        self.bcast_append(out);
    }

    /// Send one `Append` to every peer and learner. Also the heartbeat: with nothing to send the
    /// message carries no entries, and its `commit` is how a follower learns what to apply.
    pub(crate) fn bcast_append(&mut self, out: &mut Vec<Action>) {
        if self.role != Role::Leader {
            return;
        }
        let peers: Vec<NodeId> = self
            .cfg
            .members()
            .iter()
            .chain(self.cfg.learners().iter())
            .copied()
            .filter(|n| *n != self.self_id)
            .collect();
        for p in peers {
            self.send_append_to(p, out);
        }
    }

    /// Send one `Append` to `peer`, from wherever this leader believes that peer is.
    pub(crate) fn send_append_to(&mut self, peer: NodeId, out: &mut Vec<Action>) {
        if self.role != Role::Leader || peer == self.self_id || !self.cfg.is_known(peer) {
            return;
        }
        self.ensure_log();
        let last = self.last_round;
        let transferring = {
            let p = self.progress.entry(peer).or_default();
            if p.next == 0 {
                p.next = last + 1;
            }
            p.needs_snapshot
        };
        if transferring {
            // F6 owns the transfer. Sending entries at a round this leader no longer holds would
            // be a hole, not a catch-up. A bare `return` here was the stub, and it muted the peer
            // for ever: the only thing that cleared the flag was a successful `AppendResp`, which
            // can no longer arrive.
            self.send_snapshot_chunk_to(peer, out);
            return;
        }
        let next = self.progress[&peer].next;

        let prev_round = next.saturating_sub(1);
        let Some(prev_term) = self.term_at(prev_round) else {
            // The peer needs a round this leader has checkpointed away. There is no `prev_term` to
            // put in the message, and inventing one would claim a match that cannot be checked.
            self.progress.entry(peer).or_default().needs_snapshot = true;
            return;
        };
        let entries = self.entries_from(next);
        out.push(Action::Send(Message {
            from: self.self_id,
            to: peer,
            term: self.hard.term,
            body: Body::Append { prev_round, prev_term, entries, commit: self.commit },
        }));
    }

    // -----------------------------------------------------------------------------------------
    // Answers this node sends back.
    // -----------------------------------------------------------------------------------------

    /// A refusal, which carries **no claim about this node's log**.
    ///
    /// `matched: 0` and `digest: 0` for the reason `mod.rs` gives on the stale-term path: a
    /// refusal means no agreement was established, so a `matched` would be a statement about a log
    /// the leader has not matched and a digest would be a claim about a prefix neither side has
    /// agreed on. The `hint` is the only information a refusal carries.
    fn refusal(&self, to: NodeId, hint: Round) -> Action {
        Action::Send(Message {
            from: self.self_id,
            to,
            term: self.hard.term,
            body: Body::AppendResp { success: false, matched: 0, hint, digest: 0 },
        })
    }

    /// An acknowledgement, which claims exactly the prefix that is **both** durable here **and**
    /// established with this leader — and nothing else.
    ///
    /// Both halves are load-bearing and each was a real defect on its own:
    ///
    /// * **Durable**, not `last_round`: an entry appended but not yet fsynced is the one a power
    ///   loss removes after it was counted into a quorum (rule 4).
    /// * **Agreed**, not this node's whole durable log: a follower that was a leader a moment ago,
    ///   or that refused this leader's last append, holds a durable tail *this* leader never sent.
    ///   Claiming it makes the leader count a phantom replica, commit a round only it holds, and —
    ///   because the leader has no entry at that round to digest — bypass the divergence detector
    ///   at exactly the point it was needed. It also drives `next` past the leader's own tail and
    ///   live-locks the back-up exchange.
    fn acknowledgement(&self, to: NodeId) -> Action {
        let matched = self.durable.min(self.agreed());
        Action::Send(Message {
            from: self.self_id,
            to,
            term: self.hard.term,
            body: Body::AppendResp {
                success: true,
                matched,
                hint: 0,
                digest: self.digest_at(matched).unwrap_or(0),
            },
        })
    }

    // -----------------------------------------------------------------------------------------
    // Commit and apply.
    // -----------------------------------------------------------------------------------------

    /// The highest round a **majority of the current voter set** holds.
    ///
    /// Counted over `matched`, never `next`. This node counts for its own `durable` — not for its
    /// `last_round`, because an entry that is appended but not fsynced is exactly the entry a
    /// power loss removes from the quorum after it was counted into one. A `diverged` peer counts
    /// as holding nothing: it is answering, but it is not the same database.
    fn quorum_matched(&self) -> Round {
        let mut held: Vec<Round> = Vec::with_capacity(self.cfg.len());
        for n in self.cfg.members() {
            if *n == self.self_id {
                held.push(self.durable);
                continue;
            }
            held.push(match self.progress.get(n) {
                Some(p) if p.diverged.is_none() => p.matched,
                _ => 0,
            });
        }
        if held.is_empty() {
            return 0;
        }
        held.sort_unstable_by(|a, b| b.cmp(a));
        held[self.cfg.quorum() - 1]
    }

    /// Advance the commit watermark, under Raft §5.4.2.
    ///
    /// **The term check reads the term of the round being committed, not the log tail.** Those are
    /// the same number only while the leader's newest round is also the quorum's newest round, and
    /// the case where they differ is exactly the case §5.4.2 exists for: a leader that has
    /// inherited rounds from an earlier term and replicated them to a majority. Substituting
    /// `self.last_term` passes every ordinary test and commits an inherited round the moment the
    /// leader appends anything of its own — see the figure-8 tests.
    ///
    /// Returns whether the watermark moved, which also says whether every peer has just been sent
    /// an `Append` carrying it.
    fn advance_commit(&mut self, out: &mut Vec<Action>) -> bool {
        if self.role != Role::Leader {
            return false;
        }
        let q = self.quorum_matched();
        if q <= self.commit {
            return false;
        }
        if self.term_at(q) != Some(self.hard.term) {
            // An inherited round on a majority is *not* committed. It becomes committed — with
            // every round below it — as a side effect of the first round of this leader's own term
            // reaching a majority, because `commit` is a watermark and not a set.
            return false;
        }
        self.commit = q;
        self.advance_apply(out);
        // The followers cannot apply what they do not know is committed, and `commit` only travels
        // on an `Append`.
        self.bcast_append(out);
        true
    }

    /// Hand the storage engine everything that is both committed and durable **here**.
    ///
    /// Gated on `durable` and not on `commit` alone: a round is committed because a *quorum* holds
    /// it, which does not mean this node does. Applying ahead of this node's own durability would
    /// let a crash leave the storage engine holding work whose log record this node never had, and
    /// `applied <= commit` would then be true of a log that cannot prove it.
    fn advance_apply(&mut self, out: &mut Vec<Action>) {
        let through = self.commit.min(self.durable);
        if through > self.applied {
            self.applied = through;
            out.push(Action::Apply { through });
        }
    }

    // -----------------------------------------------------------------------------------------
    // The three entry points.
    // -----------------------------------------------------------------------------------------

    /// `Append`, `AppendResp`, `InstallSnapshot`, `InstallSnapshotResp`.
    pub(crate) fn on_append_msg(&mut self, m: Message, out: &mut Vec<Action>) {
        self.ensure_log();
        let from = m.from;
        match m.body {
            Body::Append { prev_round, prev_term, entries, commit } => {
                self.on_append(from, m.term, prev_round, prev_term, entries, commit, out)
            }
            Body::AppendResp { success, matched, hint, digest } => {
                self.on_append_resp(from, success, matched, hint, digest, out)
            }
            Body::InstallSnapshot { meta, offset, data, done } => {
                self.on_install_snapshot(from, m.term, meta, offset, data, done, out)
            }
            Body::InstallSnapshotResp { received_through } => {
                self.on_install_snapshot_resp(from, received_through, out)
            }
            other => unreachable!("on_append_msg received a vote body: {other:?}"),
        }
    }

    /// The follower half: match, truncate, append, and learn what is committed.
    #[allow(clippy::too_many_arguments)]
    fn on_append(
        &mut self,
        from: NodeId,
        term: Term,
        prev_round: Round,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Round,
        out: &mut Vec<Action>,
    ) {
        // **Validate the batch before anything acts on it, including this node's own office.**
        // Entries must be contiguous from `prev_round + 1`, and no entry may claim a term above
        // the envelope's. Nothing between the socket and here checks either: `mod.rs` states no
        // such invariant, F3 frames the message but does not parse entry rounds, and F7's own
        // header says it proves the sender holds the key and *not* that the message is new. An
        // unchecked hole reaches `LogTail::push`'s contiguity assertion, so one replayed or
        // spliced frame would abort any node in the cluster — and the panic text would blame the
        // local log. Refused whole rather than in part, the way `ReplicaApplier` refuses a batch
        // rather than applying a prefix of it.
        let malformed = entries
            .iter()
            .enumerate()
            .any(|(k, e)| e.round != prev_round + 1 + k as Round || e.term > term);
        if malformed {
            // `hint: 0` is "not claiming anything": a malformed message is no evidence about
            // where this node's log is, so it must not move the sender's cursor either way.
            out.push(self.refusal(from, 0));
            return;
        }

        // `mod.rs` has already refused anything from an earlier term, so this leader's term is at
        // least ours. Whatever this node was — follower, candidate, or (only if an election rule
        // is broken) a second leader of one term — it follows this one now. Stepping down here and
        // not only on a higher term is what stops a candidate from campaigning through a term that
        // already has a leader.
        //
        // Guarded rather than called unconditionally so that an ordinary heartbeat, which is by
        // far the most common message in a healthy cluster, does not emit a `RoleChanged` saying
        // nothing changed — a caller that starts and stops serving writes on that action would
        // thrash once per heartbeat.
        let adopting = self.leader != Some(from);
        if self.role != Role::Follower || adopting || self.hard.term != term {
            self.become_follower(term, Some(from), out);
        } else {
            self.since_heard = 0;
        }
        if adopting {
            // Nothing has been established with THIS leader yet, whatever was established with the
            // last one. Without this, a node that was itself a leader a moment ago goes on to
            // acknowledge its own unreplicated tail to its successor.
            self.set_agreed(0);
        }

        // Below the snapshot floor: this node cannot check a round it no longer holds. The hint
        // moves the leader FORWARD to the first round that can be checked, which is the one
        // direction a back-up hint never goes and therefore needs saying.
        if prev_round < self.snapshot_round {
            out.push(self.refusal(from, self.snapshot_round + 1));
            return;
        }

        match self.term_at(prev_round) {
            Some(t) if t == prev_term => {}
            Some(_) => {
                // A term disagreement at a round both sides hold. Back the leader up past the
                // whole run in one step.
                let hint = self.first_round_of_term_run(prev_round);
                out.push(self.refusal(from, hint));
                return;
            }
            None => {
                // A gap: this node does not hold `prev_round` at all. The first round it could
                // accept is the one after its own tail.
                out.push(self.refusal(from, self.last_round + 1));
                return;
            }
        }

        // Matched. Skip the leading entries this node already holds — that is what makes a
        // duplicate or a re-sent overlapping suffix idempotent instead of a truncation.
        let entries_len = entries.len();
        let newest_config = entries
            .iter()
            .filter_map(|e| match &e.command {
                Command::Membership { config } => Some(config.at()),
                _ => None,
            })
            .max();
        let mut i = 0usize;
        let mut conflict: Option<Round> = None;
        while i < entries.len() {
            match self.term_at(entries[i].round) {
                Some(t) if t == entries[i].term => i += 1,
                Some(_) => {
                    conflict = Some(entries[i].round);
                    break;
                }
                None => break,
            }
        }

        if let Some(r) = conflict {
            if r <= self.commit {
                // A committed round is held by a majority and every leader that can be elected
                // afterwards holds it, so a leader asking for one to be replaced is not one this
                // node can follow. Refusing is the only answer that does not unmake acknowledged
                // work; the hint points at the first round that is genuinely in question.
                out.push(self.refusal(from, self.commit + 1));
                return;
            }
            let rewriting_term = self.hard.term;
            let t = self.tail_mut();
            t.truncate_from(r);
            t.rewritten_in_term = rewriting_term;
            out.push(Action::Truncate { from: r });
            self.last_round = r - 1;
            self.last_term = self
                .term_at(r - 1)
                .expect("the round below a truncation point is either in the tail or the floor");
            // Durability cannot outlive the entries it was about.
            if self.durable > self.last_round {
                self.durable = self.last_round;
            }
        }

        // Everything through `prev_round + entries.len()` is now known to match this leader's log.
        // The clamp against the truncation point is what stops a stale, reordered `Append` from
        // re-claiming rounds this node has just deleted.
        let established = prev_round + entries_len as Round;
        let mut agreed = self.agreed();
        if let Some(r) = conflict {
            agreed = agreed.min(r - 1);
        }
        self.set_agreed(agreed.max(established));

        let fresh: Vec<Entry> = entries[i..].to_vec();
        if fresh.is_empty() {
            // Nothing to make durable means no `Persisted` will arrive, so an ack that waited for
            // one would never be sent. A heartbeat must still be answered or a healthy follower
            // starves its own leader's lease and causes the election it exists to prevent.
            out.push(self.acknowledgement(from));
        } else {
            for e in &fresh {
                self.tail_mut().push(e.clone());
            }
            let tail = fresh.last().expect("checked non-empty");
            self.last_round = tail.round;
            self.last_term = tail.term;
            // No ack here. It is emitted on `Event::Persisted` — rule 4.
            out.push(Action::Persist { entries: fresh });
        }

        // `min` with the round the append ESTABLISHED, not with this node's own tail. A follower
        // can hold a longer tail that some other leader gave it and that this one has never seen;
        // clamping to the tail commits and applies that stale suffix as though a quorum had agreed
        // to it. Raft states this as `min(leaderCommit, index of last new entry)` for the same
        // reason.
        let learned = leader_commit.min(self.agreed());
        if learned > self.commit {
            self.commit = learned;
        }

        // **The observable that clears `unjoined`, and this is its only call site.** The rule
        // itself is F1's (`election.rs::observe_quorum_watermark`) because F1 owns what may
        // campaign; the *evidence* only ever arrives here, in a leader's `commit`. Keeping a second
        // copy of the comparison in this file would be two rules that drift; not calling it at all
        // means no node added to a running cluster can ever campaign, and no test in either lane
        // can see that.
        //
        // The RAW `leader_commit` and not `self.commit`: the latter is already clamped to this
        // node's own tail, so a node holding nothing would compare zero against zero and clear the
        // flag it exists to hold.
        self.observe_quorum_watermark(leader_commit);

        // The other seam: a member has reported a configuration newer than the one this node
        // holds, so this node now *knows* it is stale and must not campaign until it has caught up.
        // A `Command::Membership` riding the log is the only form that report takes on this path.
        // Observing is not applying — `election.rs::apply_config` runs when the round commits, and
        // that is F5's row.
        if let Some(at) = newest_config {
            self.observe_config_at(at);
        }

        self.advance_apply(out);
    }

    /// The leader half: count the ack, check the digest, and repair a refusal in one step.
    fn on_append_resp(
        &mut self,
        from: NodeId,
        success: bool,
        matched: Round,
        hint: Round,
        digest: u64,
        out: &mut Vec<Action>,
    ) {
        if self.role != Role::Leader || from == self.self_id {
            return;
        }
        if !self.cfg.is_known(from) {
            // A removed node keeps running and keeps answering. Its ack must not count toward a
            // set it has left, and its `Progress` must not be resurrected here.
            return;
        }
        self.progress.entry(from).or_default().silent = 0;

        if !success {
            if hint == 0 {
                // "Not claiming anything" — `mod.rs`'s stale refusal shape. It carries no
                // information about where the peer is, so acting on it would be guessing.
                return;
            }
            let prev_next = self.progress[&from].next;
            let new_next = hint.max(1);
            if new_next == prev_next {
                // A refusal this leader has already acted on, delivered twice — F8 injects
                // duplication deliberately. A correct follower can never produce a hint equal to
                // the leader's current `next`: the gap and conflict hints are both at most
                // `prev_round`, which is `next - 1`, and the snapshot-floor hint is strictly
                // above `next`. So this is a replay, and the answer is to do nothing. Demanding a
                // snapshot here was the defect: it silenced a healthy peer permanently, because
                // `send_append_to` then refuses to send it anything and only a successful ack —
                // which can no longer arrive — clears the flag.
                return;
            }
            self.progress.entry(from).or_default().next = new_next;
            self.send_append_to(from, out);
            return;
        }

        // The divergence detector, before the ack is allowed to count toward anything.
        if digest != 0
            && let Some(mine) = self.digest_at(matched)
            && mine != digest
        {
            let why = format!(
                "peer {from} DIVERGED at round {matched}: it reports log digest {digest:#018x} \
                 where this leader's own log through the same round digests {mine:#018x}. The two \
                 nodes are not the same database, so counting this peer toward a quorum would \
                 report a round as replicated onto bytes nobody can name. Not counted again; \
                 re-seed it from a snapshot taken at or after round {matched}."
            );
            let p = self.progress.entry(from).or_default();
            if p.diverged.is_none() {
                p.diverged = Some(why);
            }
        }

        // **A peer cannot have matched a round this leader does not hold.** An ack that claims
        // one is not about this leader's log at all, so there is nothing here to count and nothing
        // to compare a digest against — `digest_at` returns `None` for it, which is precisely
        // where the divergence detector would otherwise be bypassed. Ignored rather than clamped:
        // clamping would count a prefix the peer never agreed to and compare the wrong digest.
        if matched > self.last_round {
            return;
        }

        let p = self.progress.entry(from).or_default();
        let before = p.matched;
        // Monotonic, for the reason `AckTracker::record` gives: a durability promise that can be
        // withdrawn after the fact is not a promise. Sound because an acknowledgement claims only
        // the prefix established with THIS leader (see `acknowledgement`) and `matched` is reset
        // to 0 on election, so it only ever describes rounds this leader itself replicated — and
        // this leader never sends a peer an entry conflicting with one it already sent.
        if matched > p.matched {
            p.matched = matched;
        }
        // Never backwards: `next` is where to send from, and a stale or zero-matched success
        // answer must not undo a back-up the leader has already made.
        p.next = p.next.max(p.matched + 1);
        p.needs_snapshot = false;
        // The cursor is subordinate to the flag (see `Progress::sending`): a peer that has
        // acknowledged an append is a peer this leader can serve with entries, so whatever transfer
        // was armed is finished or moot. Dropping it here and not only in `on_install_snapshot_resp`
        // is what keeps the two from ever disagreeing.
        p.sending = None;
        let advanced = p.matched > before;

        // **Keep sending while the peer is still behind.** One `Append` carries at most
        // `MAX_ENTRIES_PER_APPEND`, so a peer that is a thousand rounds behind needs many; waiting
        // for the next heartbeat between each turns a repair that should take milliseconds into
        // one heartbeat interval per batch. `advance_commit` has already sent every peer an
        // `Append` if the watermark moved, so this fires only when it did not.
        //
        // Gated on this acknowledgement having actually moved `matched`: re-sending on an ack that
        // told the leader nothing new would answer a duplicate with a duplicate, for ever.
        if !self.advance_commit(out) && advanced && self.progress[&from].matched < self.last_round {
            self.send_append_to(from, out);
        }
    }

    /// The caller made rounds durable. This is where an ack is emitted — never on receipt.
    pub(crate) fn on_persisted(&mut self, term: Term, round: Round, out: &mut Vec<Action>) {
        self.ensure_log();

        // **F6.** The driver reports a snapshot install with the round it covers, because that is
        // exactly what the event already means: everything through `round` is on this node's disk.
        // Reusing it rather than adding an event keeps the frozen contract intact and keeps the one
        // rule that matters — nothing moves until the fsync returned — in one place.
        if self.pending_install_round() == Some(round) {
            if let Err(why) = self.finish_install(out) {
                // A configuration this node cannot install is damage, and `note_config_in_log` has
                // already stepped this node down for it. Surfaced rather than swallowed: the
                // snapshot stays pending, so the transfer is retried rather than silently skipped.
                out.push(Action::Refuse { why });
                return;
            }
        }

        // Durability is a fact about bytes and not about office, so an ordinary step-down between
        // the `Persist` and this event does not invalidate the report — which is why it is bounded
        // by `last_round` rather than dropped.
        //
        // A **truncation** between the two is different in kind, and this is the one thing the
        // term on the event is for. `Persisted` names a round, and a round number does not name an
        // entry: after a truncate-and-refill a *different* entry occupies that round, and the
        // fsync that is now completing made the OLD one durable. Taking it would ack — and let a
        // leader commit — bytes that are not on this node's disk, which is rule 4's failure
        // arriving through the mitigation written to prevent it. Within one term the entry at a
        // round never changes once accepted (a truncation happens only where the incoming term
        // differs from the one already held, and one term has one leader), so a report from a term
        // at least as new as the last rewrite is trustworthy and an older one is not. Ignored
        // rather than partially trusted: it self-heals on the next in-term persist, which the
        // leader's term-establishing `NoOp` guarantees will come.
        let rewritten_in = self.tail().map(|t| t.rewritten_in_term).unwrap_or(0);
        if term >= rewritten_in {
            let bounded = round.min(self.last_round);
            if bounded > self.durable {
                self.durable = bounded;
            }
        }

        match self.role {
            Role::Leader => {
                let d = self.durable;
                self.progress.entry(self.self_id).or_default().matched = d;
                self.advance_commit(out);
            }
            _ => {
                if let Some(leader) = self.leader {
                    out.push(self.acknowledgement(leader));
                }
            }
        }
        self.advance_apply(out);
    }

    /// A client asked for a command to be committed.
    pub(crate) fn on_propose(&mut self, c: Command, out: &mut Vec<Action>) {
        if self.role != Role::Leader {
            // Refused, never dropped and never served locally: a follower that accepted a write
            // would be a second writer of the same database. `NotLeader.leader` is documented as
            // an address, and this state machine knows node ids and not addresses — it names the
            // node and the server above it, which owns the peer table, translates. Named in the
            // summary as a contract gap rather than papered over with `None`, which would tell a
            // client an election was in progress when this node knows exactly who leads.
            out.push(Action::Refuse {
                why: FerroError::NotLeader { leader: self.leader.map(|n| n.to_string()) },
            });
            return;
        }
        self.ensure_log();
        self.append_own_entry(c, out);
        self.bcast_append(out);
        // No commit advance here. This leader holds the round but has not made it durable, and a
        // round counted into a quorum before its own node fsynced it is the failure rule 4 names.
    }
}
