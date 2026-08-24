//! A replica's divergence must outlive the process that discovered it — I20 / B6.
//!
//! `ReplicaApplier`'s latch is in memory, and `repl_replica` exits on divergence, so the latch dies
//! with it. The operator restarts the process; by then one more DDL statement on the primary has
//! checkpointed and truncated the log past the record that caused the halt, so the only position
//! the primary will still serve is its new base. Restarted there, the replica reports
//! `diverged=None` and `applied_lsn == durable_lsn` — caught up — over pages that do not match the
//! primary's. A halt that does not survive a restart defends only against the failure that does
//! not restart.

use ferrodb::replication::ReplicaState;

fn state_in(dir: &tempfile::TempDir) -> ReplicaState {
    ReplicaState::at(&dir.path().join("replica.db"))
}

#[test]
fn a_replica_that_never_ran_has_no_position() {
    let dir = tempfile::tempdir().unwrap();
    assert!(
        state_in(&dir).resume().expect("a missing state file is not an error").is_none(),
        "a replica with no state file must read as fresh, so the caller re-seeds"
    );
}

#[test]
fn an_ordinary_position_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_in(&dir);
    state.record_applied(4096).unwrap();
    assert_eq!(state.resume().unwrap(), Some(4096));
}

#[test]
fn a_recorded_divergence_refuses_to_resume_forever() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_in(&dir);
    state.record_applied(737).unwrap();
    state.record_divergence(737, "replica DIVERGED at lsn 800: ALTER TABLE on 'inv'").unwrap();

    let err = state
        .resume()
        .expect_err("a replica that recorded a divergence must refuse to resume, not report caught up");
    let msg = err.to_string();
    assert!(msg.contains("must not be resumed"), "the refusal must say so plainly: {msg}");
    assert!(msg.contains("737"), "the refusal must name where it stopped: {msg}");
    assert!(
        msg.contains("ALTER TABLE on 'inv'"),
        "the refusal must carry the ORIGINAL reason across the restart, or the operator learns \
         only that it broke and never why: {msg}"
    );

    // A SECOND restart must refuse identically. The whole defect was a latch that one restart
    // cleared, so refusing once and forgetting would reproduce it exactly.
    let reread = ReplicaState::at(&dir.path().join("replica.db"));
    assert!(reread.resume().is_err(), "the refusal did not survive being re-read from disk");
}

#[test]
fn recording_progress_cannot_erase_a_divergence() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_in(&dir);
    state.record_divergence(737, "diverged for a reason that must not be forgotten").unwrap();

    // The apply loop records progress after every batch. If that overwrote the latch, a diverged
    // replica would clear the evidence with its very next write — so the guard has to survive the
    // most ordinary thing the process does.
    state.record_applied(9999).unwrap();

    let err = state.resume().expect_err("recording progress erased the divergence latch");
    assert!(
        err.to_string().contains("must not be forgotten"),
        "the original reason was lost when progress was recorded: {err}"
    );
}

#[test]
fn a_bare_number_is_still_a_valid_state_file() {
    // Every state file written before the latch existed holds just a number. Those replicas are
    // healthy and must keep resuming, or this fix breaks every running deployment.
    let dir = tempfile::tempdir().unwrap();
    let state = state_in(&dir);
    std::fs::write(state.path(), "512").unwrap();
    assert_eq!(
        state.resume().unwrap(),
        Some(512),
        "a pre-existing bare-number state file must still resume"
    );
}
