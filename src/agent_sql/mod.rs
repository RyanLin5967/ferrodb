//! The SQL surface for agent sessions.
//!
//! Design authority: DESIGN.md sections 1-3 and exit criteria 2-7, 9, 10.
//!
//! ```text
//! BEGIN AGENT SESSION AS 'pricing-agent' RUN 'r_8fk2';   -- fork a branch for one agent task
//! UPDATE inventory SET qty = qty - 5 WHERE qty >= 5;     -- captured as Add(-5) + the guard
//! SELECT * FROM inventory AS OF BRANCH b_1;              -- another branch's uncommitted state
//! DIFF;                                                  -- structured changeset, not a blob
//! MERGE;                                                 -- Clean/Commuting/Conflict/WithLoss
//! ABANDON;                                               -- drop the branch and everything on it
//! REVERT MERGE m_1 CASCADE;                              -- causal rollback via read-sets
//! ```
//!
//! The statements are parsed by the existing recursive-descent parser, resolved by the existing
//! binder, and executed by [`runtime::AgentRuntime`] against the shared `BranchCatalog`,
//! `Merger`, `EffectLog` and provenance traits. Nothing in this module owns those contracts; it
//! calls them, so the branch engine, the typed effect log and the provenance store replace the
//! in-memory pieces here without the SQL layer changing.

pub mod changeset;
pub mod dispatch;
pub mod escrow;
pub mod gate;
pub mod mem_catalog;
pub mod merge_engine;
pub mod paged_rows;
pub mod runtime;
pub mod session;

pub use changeset::{ChangeOutcome, ChangeSet, MergeReport, RowChange, RowChangeKind, RowMergeOutcome};
pub use dispatch::{is_agent_stmt, run_agent_stmt, run_in_session, AgentOutput};
pub use escrow::{Cell, EscrowLedger};
pub use gate::{BlindWriteCheck, Check, CheckStatus, Finding, GateOutcome, ReadPremiseCheck, Tier, VerificationGate};
pub use mem_catalog::MemBranchCatalog;
pub use merge_engine::{CellState, PolicyTable, SurfaceMerger};
pub use paged_rows::PagedRows;
pub use runtime::{AgentRuntime, BranchResolver, ExecCtx};
pub use session::AgentSession;
