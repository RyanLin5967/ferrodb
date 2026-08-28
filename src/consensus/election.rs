//! F1 — who leads. Terms, pre-vote, the election restriction, and the leader lease.
//!
//! **OWNER: agent F1.** Every rule in `DISTRIBUTED.md` §F1 gets a named test here, and a mutant
//! that kills it. The universal term rules are already applied in `mod.rs` before anything here is
//! called — in particular `PreVote` deliberately does NOT raise the receiver's term, and that
//! exception is already handled; do not re-implement it.

use super::{Action, Consensus, Message};

impl Consensus {
    pub(crate) fn on_tick(&mut self, _out: &mut Vec<Action>) {
        unimplemented!("F1: the election clock is not built yet")
    }

    /// `PreVote`, `PreVoteResp`, `RequestVote`, `RequestVoteResp`.
    pub(crate) fn on_vote_msg(&mut self, _m: Message, _out: &mut Vec<Action>) {
        unimplemented!("F1: vote handling is not built yet")
    }
}
