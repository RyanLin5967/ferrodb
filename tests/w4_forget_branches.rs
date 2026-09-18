//! W4 — the targeted sweep must not become a way to leak a branch.
//!
//! `scan_once` now forgets exactly the branches `reap_expired` reports, instead of re-deriving the
//! set by walking every open session and asking the catalog about each one. That is the whole
//! point: the walk was O(open sessions) and it ran inside the pgwire server's per-statement mutex,
//! so a background timer stopped every statement in the database for as long as it took.
//!
//! The risk that trade introduces is a leak, and it is not hypothetical. `TwoTierReaper::reap_expired`
//! accumulates the ids it reaps and then **discards that vector** if any later branch fails its
//! `reap` — the `Err(e) => return Err(e)` arm — and likewise if `sweep_empty_extents` fails after
//! an otherwise clean loop. Those branches are gone from the catalog and nothing will ever name
//! them again. A runtime that only forgets what it is told about would keep their workspaces,
//! names and escrow claims for the life of the process, which is precisely the unbounded growth
//! `forget_reaped_branches` was written to prevent.
//!
//! So the two must coexist, and these tests pin the division of labour: the fast path forgets what
//! it is handed and refuses what is still live, and the reconciliation still finds anything the
//! fast path was never told about.

use std::sync::Arc;

use ferrodb::agent_sql::runtime::{AgentRuntime, BranchResolver};
use ferrodb::branch::types::{BranchId, BranchState};

/// Reap a branch the way the lease reaper does — in the catalog, with nothing telling the runtime.
fn reap_behind_the_runtimes_back(rt: &AgentRuntime, branch: BranchId) {
    let rec = rt.branches().get(branch).expect("branch is live before being reaped");
    rt.branches()
        .set_state(branch, rec.state, BranchState::Reaped)
        .expect("mark the record reaped");
    assert!(
        rt.branches().get(branch).is_err(),
        "the catalog still answers for a reaped branch, so this fixture proves nothing"
    );
}

/// The fast path drops exactly what it is handed, and is idempotent.
#[test]
fn forget_branches_drops_the_branches_it_is_given() {
    let rt = Arc::new(AgentRuntime::new());
    let a = rt.begin_session("agent", Some("r_a"), BranchId::TRUNK).unwrap();
    let b = rt.begin_session("agent", Some("r_b"), BranchId::TRUNK).unwrap();

    reap_behind_the_runtimes_back(&rt, a.branch);
    reap_behind_the_runtimes_back(&rt, b.branch);
    // Non-vacuous: the runtime is still holding both, which is the state being fixed.
    assert!(rt.run_of(a.branch).is_some(), "fixture did not leave a workspace to forget");
    assert!(rt.run_of(b.branch).is_some(), "fixture did not leave a workspace to forget");

    assert_eq!(rt.forget_branches(&[a.branch, b.branch]), 2);
    assert!(rt.run_of(a.branch).is_none(), "a's workspace survived");
    assert!(rt.run_of(b.branch).is_none(), "b's workspace survived");

    assert_eq!(rt.forget_branches(&[a.branch, b.branch]), 0, "a second call must find nothing");
    // And the reconciliation agrees there is nothing left over.
    assert_eq!(rt.forget_reaped_branches(), 0, "the fast path left bookkeeping behind");
}

/// A LIVE branch handed to the fast path must be left completely alone. The catalog is what
/// decides, not the caller's say-so — otherwise one wrong id from a caller deletes a working
/// agent's session, its name and its escrow claim.
#[test]
fn forget_branches_refuses_a_branch_that_is_still_live() {
    let rt = Arc::new(AgentRuntime::new());
    let live = rt.begin_session("agent", Some("r_live"), BranchId::TRUNK).unwrap();

    assert_eq!(rt.forget_branches(&[live.branch]), 0, "a live branch was forgotten");
    assert!(rt.run_of(live.branch).is_some(), "a live agent's workspace was deleted");
    assert_eq!(
        rt.resolve_branch(&live.branch_name).expect("a live agent's branch name was unbound"),
        live.branch,
        "a live agent's branch name now points somewhere else"
    );
}

/// **The leak guard.** A branch reaped but never reported — exactly what `reap_expired` produces
/// when it fails partway — must still be forgotten by the reconciliation.
///
/// Told about `a` only, while both `a` and `b` are gone from the catalog. If the fast path were
/// allowed to replace the sweep, `b` would sit in the map forever.
#[test]
fn a_reaped_branch_nobody_reported_is_still_reconciled() {
    let rt = Arc::new(AgentRuntime::new());
    let a = rt.begin_session("agent", Some("r_a"), BranchId::TRUNK).unwrap();
    let b = rt.begin_session("agent", Some("r_b"), BranchId::TRUNK).unwrap();

    reap_behind_the_runtimes_back(&rt, a.branch);
    reap_behind_the_runtimes_back(&rt, b.branch);

    // The reaper failed after reaping both but only managed to report `a`.
    assert_eq!(rt.forget_branches(&[a.branch]), 1);
    assert!(rt.run_of(a.branch).is_none(), "the reported branch was not forgotten");
    assert!(
        rt.run_of(b.branch).is_some(),
        "b must still be held here, or this test is not measuring the leak it names"
    );

    // The backstop finds what the report omitted.
    assert_eq!(rt.forget_reaped_branches(), 1, "the unreported branch leaked");
    assert!(rt.run_of(b.branch).is_none(), "b's workspace survived the reconciliation");
}
