//! F2 — log replication: getting the leader's rounds onto a quorum, and deciding what is committed.
//!
//! **OWNER: agent F2.** The contract (`Progress`, and the entry points below) is fixed; the rules
//! are this file's job. The universal term rules are already applied in `mod.rs` before anything
//! here is called — do not re-implement them.

use super::{Action, Command, Consensus, Message, Round, Term};

/// Per-peer replication progress.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Progress {
    /// The next round to send this peer. Optimism — it may be wrong and is corrected by a refusal.
    pub next: Round,
    /// The highest round **known** to be on this peer. Quorum is counted over this and never over
    /// `next`: counting optimism commits rounds nobody holds.
    pub matched: Round,
    /// Ticks since this peer last answered, feeding the leader's own lease.
    pub silent: u32,
    /// This peer needs a snapshot: the rounds it asked for are below our log start.
    pub needs_snapshot: bool,
}

impl Consensus {
    /// `Append`, `AppendResp`, `InstallSnapshot`, `InstallSnapshotResp`.
    pub(crate) fn on_append_msg(&mut self, _m: Message, _out: &mut Vec<Action>) {
        unimplemented!("F2: append handling is not built yet")
    }

    pub(crate) fn on_persisted(&mut self, _term: Term, _round: Round, _out: &mut Vec<Action>) {
        unimplemented!("F2: durability acknowledgement is not built yet")
    }

    pub(crate) fn on_propose(&mut self, _c: Command, _out: &mut Vec<Action>) {
        unimplemented!("F2: proposal handling is not built yet")
    }
}
