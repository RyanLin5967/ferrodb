//! **D246 §6.1: a fork that brings a NEW run makes it durable after the caller's guard, not under
//! it.**
//!
//! Once pgserver opened the durable provenance store (D246), `BEGIN AGENT SESSION` paid
//! `DurableProvenanceStore::intern`'s fsync once per new run. It paid it inside
//! `begin_session_as_staged`, under the runtime's `state` lock and inside pgwire's
//! `ServerContext::catalog()` guard. Every statement on the server waited behind that fsync: the S5
//! shape D159 removed for the fork's own sync, back through the provenance store.
//!
//! D159's answer applies unchanged. Staging does everything but the disk, and the returned
//! `ForkDurability` carries the disk out past the guard. So the run's record now rides the same
//! value: staged with the fork, synced by `complete()`.
//!
//! # The instrument
//!
//! The guard is a stand-in `Mutex<()>`, held exactly where pgwire holds the catalog: across
//! `begin_session_as_staged` and released before `complete()` (`pgwire/extended.rs`, `run_staged`
//! then `drop(catalog)` then `d.complete()`). Each claim reads:
//! - `provenance().sync_counts()`, while the guard is held and again after `complete()`. Every sync
//!   between the first two readings happened under the guard, so "+0 while held" is the claim
//!   "no provenance fsync under the guard";
//! - `TableBranchCatalog::syncs_issued()`, the fork's own sync, so each test states the whole
//!   per-fork fsync count;
//! - a SECOND `DurableProvenanceStore::open` of the same file after `complete()`. The counter alone
//!   would pass a sync that wrote nothing; the reopen proves the record is in the file.
//!
//! # Pre-registered in `bench/d246/PREREG.md` A2, before this file was ever run
//!
//! | test | at `eca45fb` | with the fix |
//! |---|---|---|
//! | T1 staged under the guard: provenance +0 | **fails**: +1 | holds |
//! | T1 after `complete()`: provenance +1, booked `runs` | **fails**: +0, already paid | holds |
//! | T2 a known run's fork: +0 staged, +0 completed | holds | holds |
//! | T3 two new runs staged: +0; both completed: +1 in total | **fails**: +2, +2 | holds |
//! | T4 a dropped `ForkDurability` still makes its run durable | holds | holds |
//!
//! T1 and T3 collect their claims before asserting, so a red run lists every failed claim.

use std::sync::{Arc, Mutex};

use ferrodb::agent_sql::runtime::{AgentRuntime, RunIdentity};
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::provenance::{DurableProvenanceStore, SyncCounts};

struct Rig {
    cat: Arc<TableBranchCatalog>,
    rt: Arc<AgentRuntime>,
    prov: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

/// pgserver's shape: the table branch catalog, whose fork is staged and group-committed (D159),
/// and the durable provenance store (D246).
fn rig() -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let cat =
        Arc::new(TableBranchCatalog::open_sidecar(&dir.path().join("b.branchcat"), 1).unwrap());
    let prov = dir.path().join("d.provenance");
    let rt = Arc::new(
        AgentRuntime::with_catalog(cat.clone())
            .with_durable_provenance(&prov)
            .expect("open the durable provenance store"),
    );
    Rig { cat, rt, prov, _dir: dir }
}

impl Rig {
    fn prov_syncs(&self) -> SyncCounts {
        self.rt.provenance().sync_counts()
    }

    /// The run ids a second store opened on the same file recovers: what a crash right now keeps.
    fn run_ids_in_the_file(&self) -> Vec<String> {
        DurableProvenanceStore::open(&self.prov)
            .expect("reopen the provenance file")
            .runs()
            .expect("read the reopened runs")
            .into_iter()
            .map(|r| r.run_id)
            .collect()
    }
}

fn run(run_id: &'static str) -> RunIdentity<'static> {
    RunIdentity { agent_id: "d246", run_id: Some(run_id), ..RunIdentity::default() }
}

/// **T1: the seam.** Staging a new run's fork under the guard syncs nothing. `complete()`, after
/// the guard, syncs the run once, and the record is then in the file.
#[test]
fn a_new_runs_record_is_synced_after_the_guard_not_under_it() {
    let r = rig();
    let guard = Mutex::new(());
    let (p0, c0) = (r.prov_syncs(), r.cat.syncs_issued());

    let (under_guard, durability) = {
        let _catalog = guard.lock().unwrap();
        let (_session, durability) =
            r.rt.begin_session_as_staged(run("new-1"), BranchId::TRUNK).unwrap();
        ((r.prov_syncs(), r.cat.syncs_issued()), durability)
    };
    durability.complete().unwrap();
    let (p2, c2) = (r.prov_syncs(), r.cat.syncs_issued());
    let in_file = r.run_ids_in_the_file();

    let mut failures = Vec::new();
    if under_guard.0.total() != p0.total() {
        failures.push(format!(
            "{} provenance sync(s) ran while the guard was held (want 0): {:?} -> {:?}. Every \
             statement behind pgwire's catalog guard waits on that disk round-trip.",
            under_guard.0.total() - p0.total(),
            p0,
            under_guard.0
        ));
    }
    if under_guard.1 != c0 {
        failures.push(format!("the fork's own sync ran under the guard: {c0} -> {}", under_guard.1));
    }
    if p2.total() - under_guard.0.total() != 1 || p2.runs - under_guard.0.runs != 1 {
        failures.push(format!(
            "complete() issued {} provenance sync(s), {} booked under `runs` (want exactly 1 of \
             each): {:?} -> {:?}",
            p2.total() - under_guard.0.total(),
            p2.runs - under_guard.0.runs,
            under_guard.0,
            p2
        ));
    }
    if c2 - under_guard.1 != 1 {
        failures.push(format!("complete() issued {} fork syncs (want 1)", c2 - under_guard.1));
    }
    if !in_file.iter().any(|id| id == "new-1") {
        failures.push(format!(
            "the run is not in the provenance file after complete() returned: {in_file:?}. The \
             session was acknowledged with a run a crash would lose."
        ));
    }
    assert!(failures.is_empty(), "T1:\n  {}", failures.join("\n  "));
}

/// **T2: a known run stays free.** The second fork of a run already on disk issues no provenance
/// sync on either side of the guard. Non-discriminating at the base on purpose: a fix that synced
/// per FORK rather than per new run would fail here and nowhere else.
#[test]
fn a_known_runs_fork_issues_no_provenance_sync() {
    let r = rig();
    r.rt.begin_session_as(run("known"), BranchId::TRUNK).unwrap();
    assert!(
        r.run_ids_in_the_file().iter().any(|id| id == "known"),
        "premise: the first fork of the run did not leave it in the file"
    );

    let guard = Mutex::new(());
    let (p0, c0) = (r.prov_syncs(), r.cat.syncs_issued());
    let (staged, durability) = {
        let _catalog = guard.lock().unwrap();
        let (_session, durability) =
            r.rt.begin_session_as_staged(run("known"), BranchId::TRUNK).unwrap();
        (r.prov_syncs(), durability)
    };
    assert_eq!(staged.total(), p0.total(), "a known run's fork synced provenance under the guard");
    durability.complete().unwrap();
    assert_eq!(
        r.prov_syncs().total(),
        p0.total(),
        "a known run's fork synced provenance at complete(): a repeat must cost nothing"
    );
    assert_eq!(r.cat.syncs_issued(), c0 + 1, "the fork itself syncs exactly once");
}

/// **T3: group commit.** Two new runs staged before either completes: nothing is synced under the
/// guard, and one provenance sync covers both, the way D159's two staged forks share one.
#[test]
fn new_runs_staged_before_either_completes_share_one_provenance_sync() {
    let r = rig();
    let guard = Mutex::new(());
    let p0 = r.prov_syncs();

    let (da, db) = {
        let _catalog = guard.lock().unwrap();
        let (_a, da) = r.rt.begin_session_as_staged(run("pair-a"), BranchId::TRUNK).unwrap();
        let (_b, db) = r.rt.begin_session_as_staged(run("pair-b"), BranchId::TRUNK).unwrap();
        (da, db)
    };
    let staged = r.prov_syncs();
    da.complete().unwrap();
    db.complete().unwrap();
    let done = r.prov_syncs();
    let in_file = r.run_ids_in_the_file();

    let mut failures = Vec::new();
    if staged.total() != p0.total() {
        failures.push(format!(
            "{} provenance sync(s) while staging two forks under the guard (want 0)",
            staged.total() - p0.total()
        ));
    }
    if done.total() - p0.total() != 1 {
        failures.push(format!(
            "two new runs cost {} provenance syncs in all (want 1: the first complete() carries \
             both records): {:?} -> {:?}",
            done.total() - p0.total(),
            p0,
            done
        ));
    }
    for id in ["pair-a", "pair-b"] {
        if !in_file.iter().any(|x| x == id) {
            failures.push(format!("{id} is not in the provenance file: {in_file:?}"));
        }
    }
    assert!(failures.is_empty(), "T3:\n  {}", failures.join("\n  "));
}

/// **T4: the net.** A `ForkDurability` dropped without `complete()` still makes its run durable,
/// as it already makes its fork durable (D159): the debt is a value, so every exit discharges it.
#[test]
fn a_staged_fork_dropped_without_completing_still_makes_its_run_durable() {
    let r = rig();
    {
        let (_session, durability) =
            r.rt.begin_session_as_staged(run("dropped"), BranchId::TRUNK).unwrap();
        drop(durability); // the forgotten complete(), made explicit
    }
    assert!(
        r.run_ids_in_the_file().iter().any(|id| id == "dropped"),
        "a staged fork dropped without complete() left its run out of the file"
    );
}
