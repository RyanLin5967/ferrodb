//! D159 — `BEGIN AGENT SESSION`'s fsync is issuable OUTSIDE the caller's lock, and forks staged
//! before either completes SHARE one fsync.
//!
//! # What this is really testing, and why it is not a latency test
//!
//! `TableBranchCatalog` has implemented leader/follower group commit since D6a: the first forker
//! into `CommitGroup::wait_durable` issues one fsync and claims every ticket outstanding when it
//! started, so everyone else wakes already durable. D130 then measured that machinery **over the
//! wire** and found it completely inert — `f/sync` exactly **1.00 at every thread count from 1 to
//! 128**, against an in-process control on the same `Arc<TableBranchCatalog>` rising to **17.12**
//! (`bench/d130_batch_vs_threads.txt`). Nothing about the fsync was slow. pgwire holds
//! `ServerContext::catalog()` for the duration of a statement, so forkers serialised *in front of*
//! the commit group and never met inside it.
//!
//! So the property that had to change is not "the sync is faster" but "**the sync is reachable
//! from outside the caller's lock**". That is a structural property, and these tests assert it
//! structurally — **no threads, no timing, no rates**. A timing test here would be a fact about
//! this box; a sync counter is a fact about the code.
//!
//! The counter is `TableBranchCatalog::syncs_issued()` — `CommitGroup`'s own, the branch catalog's
//! only fsync. The attestation log and the provenance store sync their own files and never move it,
//! which is what makes these counts attributable to the fork and nothing else.

use std::sync::Arc;

use ferrodb::agent_sql::runtime::{AgentRuntime, RunIdentity};
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::BranchId;

fn rig() -> (Arc<TableBranchCatalog>, Arc<AgentRuntime>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cat =
        Arc::new(TableBranchCatalog::open_sidecar(&dir.path().join("b.branchcat"), 1).unwrap());
    let rt = Arc::new(AgentRuntime::with_catalog(cat.clone()));
    (cat, rt, dir)
}

/// One run id for every fork in a test. **Load-bearing, not tidiness:** `prov_store.intern` is
/// first-wins per `(agent, run)` and fsyncs its own file only when the run is new, so reusing one
/// run id keeps that second, ungrouped sync out of the way. It never touches `syncs_issued()`
/// either way — this just keeps the fixture honest about which syncs exist at all.
fn one_run() -> RunIdentity<'static> {
    RunIdentity { agent_id: "d159", run_id: Some("one-run"), ..RunIdentity::default() }
}

/// `fallback_syncs()` is a PROCESS-WIDE counter and cargo runs these tests on parallel threads, so
/// the two tests that read it must not interleave: one of them increments it, and the other
/// asserts it did not move. Without this the second would flake exactly when the first happened to
/// run beside it — a failure that looks like a real defect and is not.
///
/// Only these two tests need it. Every other test here completes its ticket explicitly and so never
/// touches the counter.
static COUNTER_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The seam itself: staging a fork does everything except the disk, and the returned ticket is
/// what carries the disk.
#[test]
fn a_staged_fork_has_not_synced_yet_and_completing_it_syncs_once() {
    let (cat, rt, _dir) = rig();
    let before = cat.syncs_issued();

    let (session, durability) = rt.begin_session_as_staged(one_run(), BranchId::TRUNK).unwrap();

    assert_eq!(
        cat.syncs_issued(),
        before,
        "staging a fork must not issue the branch catalog's fsync. The whole point of the split is \
         that the sync is issuable AFTER the caller drops its statement-wide guard; a sync in here \
         is a sync under that guard, which is the shape d130 measured at f/sync 1.00."
    );
    // The branch is real and usable before the sync — it is in the buffer pool, just not on disk.
    assert_ne!(session.branch, BranchId::TRUNK, "the fork must have produced a child branch");

    durability.complete().unwrap();

    assert_eq!(
        cat.syncs_issued(),
        before + 1,
        "completing a staged fork must issue exactly one sync"
    );
}

/// ⭐ **The property the whole row is about, made deterministic.**
///
/// Two forks are staged before either is completed. `wait_durable`'s leader reads
/// `target = st.requested` at the instant it begins its sync, so it claims BOTH tickets; the
/// second caller finds `durable >= seq` and returns without syncing. One fsync, two forks.
///
/// This is the same batching d130's arm D exercises with threads, expressed without them: the
/// batch is "whichever tickets exist when the leader looks", and staging both first is simply the
/// deterministic way to make two exist. If this reads 2, no group formed and the change bought
/// nothing.
#[test]
fn two_forks_staged_before_either_completes_share_one_fsync() {
    let (cat, rt, _dir) = rig();
    let before = cat.syncs_issued();

    let (_a, da) = rt.begin_session_as_staged(one_run(), BranchId::TRUNK).unwrap();
    let (_b, db) = rt.begin_session_as_staged(one_run(), BranchId::TRUNK).unwrap();

    assert_eq!(
        cat.syncs_issued(),
        before,
        "neither staged fork may have synced: if the first one did, tickets cannot accumulate and \
         there is nothing for a leader to batch"
    );

    da.complete().unwrap();
    db.complete().unwrap();

    assert_eq!(
        cat.syncs_issued(),
        before + 1,
        "two forks staged before either completed must SHARE one fsync. Two syncs here means the \
         leader claimed only its own ticket, so a group never forms and the wire-level f/sync of \
         1.00 that d130 measured survives this change untouched."
    );
}

/// The durable spelling must stay durable. `AgentRuntime::begin_session_as` is what every caller
/// that holds no wider lock uses — `cli.rs` among them — and the split must not have quietly
/// turned it into a staged fork nobody completes.
#[test]
fn the_durable_spelling_still_syncs_before_it_returns() {
    let (cat, rt, _dir) = rig();
    let before = cat.syncs_issued();

    rt.begin_session_as(one_run(), BranchId::TRUNK).unwrap();

    assert_eq!(
        cat.syncs_issued(),
        before + 1,
        "begin_session_as must be durable when it returns: it promises the fork is on disk, and a \
         caller with no wider lock has nowhere else to complete it"
    );
}

/// ⭐ **The safety net, FORCED TO FIRE.** A `ForkDurability` dropped without `complete()` must
/// still make the fork durable — the dangerous state is unrepresentable, not merely asserted
/// against.
///
/// This test exists because the first version of that guarantee was **two guards that could not
/// fire**: `#[must_use]` is satisfied by any binding (and every call site binds), and the
/// `debug_assert!` in `Drop` is compiled out under `--release`, which is the profile that serves
/// pgwire — this crate has no `[profile]` in `Cargo.toml` and no `.cargo/config.toml`. A guarantee
/// nobody has watched fire is not a guarantee.
///
/// Both halves are asserted: the sync **happened** (the counter the client depends on), and the
/// fallback **was used** (the counter an operator would watch). Asserting only the first would
/// pass even if `complete()` had been called somewhere unseen.
#[test]
fn dropping_a_staged_fork_without_completing_it_still_syncs_and_is_counted() {
    use ferrodb::agent_sql::session::fallback_syncs;

    let _serial = COUNTER_TESTS.lock().unwrap_or_else(|p| p.into_inner());
    let (cat, rt, _dir) = rig();
    let syncs_before = cat.syncs_issued();
    let fallbacks_before = fallback_syncs();

    {
        let (_session, durability) = rt.begin_session_as_staged(one_run(), BranchId::TRUNK).unwrap();
        assert_eq!(
            cat.syncs_issued(),
            syncs_before,
            "staging must not have synced, or this test is not exercising the net"
        );
        drop(durability); // the forgotten `complete()`, made explicit
    }

    assert_eq!(
        cat.syncs_issued(),
        syncs_before + 1,
        "a dropped ForkDurability must still issue the sync. If this reads +0 the branch is in the \
         buffer pool with nothing to make it durable, which is precisely the hole the earlier \
         debug_assert! pretended to guard and could not, being compiled out of release."
    );
    assert_eq!(
        fallback_syncs(),
        fallbacks_before + 1,
        "the fallback must be COUNTED as well as performed: it is correct but it forfeits the \
         batching, and an operator needs to be able to see that it happened in a release build"
    );
}

/// The mirror of the test above, and the reason it is not redundant: the normal path must **not**
/// touch the fallback counter. Without this, a `Drop` that always synced would pass the test above
/// while quietly double-syncing every well-behaved caller.
#[test]
fn completing_a_staged_fork_does_not_touch_the_fallback_counter() {
    use ferrodb::agent_sql::session::fallback_syncs;

    let _serial = COUNTER_TESTS.lock().unwrap_or_else(|p| p.into_inner());
    let (cat, rt, _dir) = rig();
    let syncs_before = cat.syncs_issued();
    let fallbacks_before = fallback_syncs();

    let (_session, durability) = rt.begin_session_as_staged(one_run(), BranchId::TRUNK).unwrap();
    durability.complete().unwrap();

    assert_eq!(cat.syncs_issued(), syncs_before + 1, "exactly one sync on the normal path");
    assert_eq!(
        fallback_syncs(),
        fallbacks_before,
        "an explicitly completed ticket must not also run the fallback: `complete()` takes the seq, \
         so the Drop that follows has nothing left to do"
    );
}

/// ⛔ **The basis of the whole "a staged fork cannot be lost" argument, asserted instead of
/// assumed — and it is COARSER than the per-seq accounting the design is written in terms of.**
///
/// `durable(seq)` calls `BufferPoolManager::flush_all`, which takes **no seq**: it writes every
/// dirty page in the pool. So a fork that is staged and whose ticket is never awaited directly is
/// still made durable by *any later* sync on the same catalog.
///
/// This test exists because that coarseness reads like dead work from inside `durable()` — it has a
/// `seq`, so flushing pages no ticket asked for looks like something to optimise away. Narrowing it
/// would convert a non-hazard into a real one, **and the ticket accounting would not notice**:
/// `CommitGroup` would still report the fork durable while its pages had never been written.
///
/// The assertion is therefore made against the FILE, by reopening it — not against the sync
/// counter, which would pass under exactly the mutation this is meant to catch.
#[test]
fn a_later_sync_makes_an_earlier_staged_fork_durable() {
    use ferrodb::branch::types::LeaseDeadline;
    use ferrodb::branch::BranchCatalog;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("b.branchcat");

    let staged = {
        let cat = TableBranchCatalog::open_sidecar(&path, 1).unwrap();

        // A is staged and its ticket is DELIBERATELY dropped on the floor — no `await_fork_durable`
        // for this one, ever. Nothing but the coarse flush can save it.
        let (a, _never_awaited) =
            cat.fork_staged(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();

        // An unrelated later fork, and ITS sync is the only one issued.
        let (_b, seq_b) =
            cat.fork_staged(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
        cat.await_fork_durable(seq_b).unwrap();

        a.branch_id
        // `cat` drops here. The pool does NOT flush on drop -- that is exactly how an earlier
        // version of this catalog shipped a database whose header page read `magic 0x00000000`
        // on reopen, so this drop is not doing the test's work for it.
    };

    let reopened = TableBranchCatalog::open_sidecar(&path, 1).unwrap();
    reopened.get(staged).unwrap_or_else(|e| {
        panic!(
            "a fork staged before an unrelated sync was NOT on disk after reopen: {e}. \
             `durable()` must flush the whole pool, not just the pages its `seq` covers -- if that \
             has been narrowed, every staged-but-not-yet-awaited fork can be lost while \
             CommitGroup still reports it durable."
        )
    });
}

/// `BranchCatalog::fork` keeps its contract on a store that does NOT implement the split. The
/// default `fork_staged` forks durably and reports nothing pending, so an implementor that never
/// heard of tickets is correct by default rather than silently non-durable.
#[test]
fn the_unsplit_default_forks_durably_and_defers_nothing() {
    use ferrodb::agent_sql::mem_catalog::MemBranchCatalog;
    use ferrodb::branch::types::LeaseDeadline;
    use ferrodb::branch::BranchCatalog;

    let cat = MemBranchCatalog::new();
    let (record, pending) = cat.fork_staged(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();

    assert_ne!(record.branch_id, BranchId::TRUNK, "the default must still actually fork");
    assert!(
        pending.is_none(),
        "a store with no stage/durable split must report nothing pending. Reporting a ticket it \
         cannot honour would make `await_fork_durable` a no-op that LOOKS like a sync."
    );
}
