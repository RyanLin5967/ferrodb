//! F6 — state transfer for a follower whose rounds have been checkpointed away.
//!
//! **OWNER: agent F6.** A ferrodb snapshot is unusually cheap to name, because a branch *is* a root
//! pointer: `{ round, term, root_page_id, live arenas, branch catalog, catalog image }`.

use super::{Round, Term};

/// What a snapshot claims to be, sent ahead of its bytes.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SnapshotMeta {
    /// The last round included. The receiver's log begins after this.
    pub last_round: Round,
    /// The term of `last_round`, needed for the log-matching check on the entries that follow.
    pub last_term: Term,
    /// The configuration in force at that round — a snapshot that did not carry it would leave the
    /// receiver unable to count a majority.
    pub config: super::config::Config,
    /// Total bytes, so a receiver can refuse an implausible transfer before allocating for it.
    pub total_bytes: u64,
}
