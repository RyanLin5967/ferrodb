//! **S20.** `live_count` is now a maintained counter instead of a scan. This is the test that it
//! did not become a faster WRONG answer.
//!
//! The old implementation iterated every record and filtered on `Live` — O(total branches, reaped
//! ones included), measured 0.664 -> 59.234 ms across 100x N. The new one reads an integer kept in
//! step by `CatalogState::install`, which is the only call that writes the record map.
//!
//! A maintained counter is a second source of truth, and the failure mode is DRIFT: it is right
//! when written and wrong three operations later, silently, with nothing to compare against. So
//! every case here does the same thing — run a sequence of real operations, then assert the
//! counter equals an independent recount of the map it claims to summarise.
use ferrodb::branch::BranchCatalog;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, BranchState, LeaseDeadline};

/// The independent answer: count the records, the way `live_count` used to.
fn recount(c: &LogBranchCatalog) -> usize {
    c.scan()
        .expect("scan")
        .filter_map(|r| r.ok())
        .filter(|r| r.state == BranchState::Live)
        .count()
}

fn agree(c: &LogBranchCatalog, what: &str) {
    let maintained = c.live_count();
    let scanned = recount(c);
    assert_eq!(
        maintained, scanned,
        "after {what}: live_count() says {maintained} but a scan of the records finds {scanned} — \
         the maintained counter has DRIFTED from the map it summarises"
    );
}

fn fresh() -> LogBranchCatalog {
    LogBranchCatalog::in_memory(1)
}

#[test]
fn the_counter_agrees_with_a_scan_after_forks() {
    let c = fresh();
    agree(&c, "create");
    for i in 0..32 {
        c.fork(BranchId::TRUNK, LeaseDeadline(1_000 + i)).expect("fork");
    }
    agree(&c, "32 forks");
}

#[test]
fn the_counter_agrees_with_a_scan_after_state_transitions() {
    let c = fresh();
    let mut ids = Vec::new();
    for i in 0..16 {
        ids.push(c.fork(BranchId::TRUNK, LeaseDeadline(1_000 + i)).expect("fork").branch_id);
    }
    agree(&c, "16 forks");

    // Live -> Reaping -> Reaped, the real sequence the reaper drives.
    for id in ids.iter().take(7) {
        c.set_state(*id, BranchState::Live, BranchState::Reaping).expect("to reaping");
    }
    agree(&c, "7 moved to Reaping");
    for id in ids.iter().take(7) {
        c.set_state(*id, BranchState::Reaping, BranchState::Reaped).expect("to reaped");
    }
    agree(&c, "7 moved to Reaped");
}

#[test]
fn an_idempotent_transition_does_not_move_the_counter() {
    // set_state returns early when expect == to. If the counter were adjusted before that check,
    // a resumed reap would decrement twice and under-report live branches forever.
    let c = fresh();
    let id = c.fork(BranchId::TRUNK, LeaseDeadline(1_000)).expect("fork").branch_id;
    c.set_state(id, BranchState::Live, BranchState::Reaping).expect("to reaping");
    let once = c.live_count();
    c.set_state(id, BranchState::Reaping, BranchState::Reaping).expect("idempotent");
    assert_eq!(c.live_count(), once, "an idempotent transition moved the counter");
    agree(&c, "an idempotent re-transition");
}

// ⚠ A reopen test belongs here and is NOT written, deliberately rather than forgotten: the
// counter is rebuilt by `CatalogState::recount` on the replay path, and an in-memory catalog has
// no replay path to exercise it. Asserting it against `in_memory` would test nothing while looking
// like coverage — the shape of a vacuous pass. It needs the file-backed constructor.
