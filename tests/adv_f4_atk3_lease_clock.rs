//! ATTACK 3 — make a reap decision come from a wall clock on a cluster member, make two members
//! disagree about a deadline, and make `LeaseDeadline::from_now` return a wrong value instead of
//! panicking.
//!
//! Reaping frees a branch's arenas and then bumps its `BranchId` generation, which exists precisely
//! so a reaped id can never be mistaken for a live one — so a wrong reap is **unrecoverable**. The
//! three ways to get one are: read the wrong clock, disagree with a peer about the same clock, or
//! compute the wrong number from the right clock. One test each, plus the availability question the
//! panic raises.
//!
//! Tests named `*_holds` assert a property the guard is supposed to have. A failure elsewhere is the
//! finding, and its panic message is the evidence.

use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cluster::{self, ClusterScope, GrantError};
use ferrodb::consensus::NodeId;
use ferrodb::storage::disk_manager::DiskManager;

const ARENA_BASE: u32 = 1024;
const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);

struct Store {
    store: Arc<ArenaPageStore>,
    catalog: Arc<LogBranchCatalog>,
    _dir: tempfile::TempDir,
}

fn store(tag: &str) -> Store {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(Arc::clone(&dm)));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let store =
        Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), ARENA_BASE).unwrap());
    Store { store, catalog, _dir: dir }
}

// =================================================================================================
// A3.1 — is there any infallible clock left that a reap can reach?
// =================================================================================================

/// `reap_expired` takes `now_millis` as a parameter and `is_expired_at` is a pure comparison, so
/// the reaper itself cannot read a clock. This pins that: a member with no tick has no way to
/// produce the argument.
#[test]
fn a3_1_a_member_with_no_tick_cannot_produce_a_reap_clock_holds() {
    let _scope = ClusterScope::standalone();
    cluster::join(N1);

    match LeaseDeadline::try_now_millis() {
        Err(GrantError::NoClusterTime { node }) => assert_eq!(node, N1),
        other => panic!("GUARD FELL: a member with no tick answered the time: {other:?}"),
    }
    assert!(LeaseDeadline::try_from_now(1).is_err());
    assert!(LeaseDeadline(0).is_expired_now().is_err(), "is_expired_now answered with no tick");
    assert!(LeaseDeadline(u64::MAX).is_expired_now().is_err());
}

// =================================================================================================
// A3.2 — the production paths that still call the PANICKING clock.
// =================================================================================================

/// `AgentRuntime::begin_session_with_model` — production code, not a test — calls
/// `LeaseDeadline::from_now(DEFAULT_LEASE_MILLIS)` (`src/agent_sql/runtime.rs:650`), and
/// `AgentRuntime::simulate` calls `LeaseDeadline::from_now(plan.lease_millis)`
/// (`src/agent_sql/simulate.rs:431`). Neither has a fallible variant on its path.
///
/// The claim is that the refusal is a refusal. This asks whether the *engine* refuses or aborts.
#[test]
fn a3_2_beginning_an_agent_session_on_a_member_with_no_tick_returns_an_error() {
    let _scope = ClusterScope::standalone();
    let rt = AgentRuntime::new();
    // Prove the fixture works before the join, so a failure after it is about the clock.
    rt.begin_session("probe-agent", None, BranchId::TRUNK)
        .expect("fixture: begin_session must work standalone");

    cluster::join(N1);

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.begin_session("probe-agent", None, BranchId::TRUNK)
    }));
    match outcome {
        Ok(Ok(_)) => panic!("GUARD FELL: a member with no tick minted a lease deadline anyway"),
        Ok(Err(_)) => {} // the good case: a refusal reached the caller
        Err(_) => panic!(
            "GUARD FELL (availability): AgentRuntime::begin_session PANICKED on a cluster member \
             with no applied LeaseTick instead of returning Err. This is production code \
             (src/agent_sql/runtime.rs:650, before the file's #[cfg(test)] at line 4291) on the \
             path of every agent session, FORK and SIMULATE. A node that joins a cluster before \
             its first LeaseTick aborts the process on the first agent statement rather than \
             refusing it."
        ),
    }
}

// =================================================================================================
// A3.3 — two members, the same ticks, delivered in different orders.
// =================================================================================================

/// `fold_tick` is `max`, which is commutative and idempotent, so a re-delivered or reordered suffix
/// of the same committed ticks lands on the same value everywhere.
#[test]
fn a3_3_the_same_ticks_in_any_order_give_the_same_time_holds() {
    let _scope = ClusterScope::standalone();
    let ticks = [1_700_000_000_000u64, 1_700_000_000_500, 1_700_000_001_000];

    let mut answers = Vec::new();
    // Every permutation, plus each one re-delivered.
    for perm in [[0, 1, 2], [2, 1, 0], [1, 2, 0], [0, 2, 1], [2, 0, 1], [1, 0, 2]] {
        cluster::leave();
        cluster::join(N1);
        for i in perm {
            cluster::apply_lease_tick(ticks[i]).unwrap();
            cluster::apply_lease_tick(ticks[i]).unwrap(); // re-delivery
        }
        answers.push(LeaseDeadline::try_from_now(60_000).unwrap());
    }
    let first = answers[0];
    assert!(
        answers.iter().all(|d| *d == first),
        "GUARD FELL: two members that applied the same ticks in different orders computed \
         different deadlines: {answers:?}"
    );
    assert_eq!(first.0, ticks[2] + 60_000, "the deadline is not tick + millis");
}

/// A node cannot be told the time by anything other than a tick, and a `leave` forgets it.
#[test]
fn a3_4_a_standalone_node_refuses_a_tick_and_a_leave_forgets_one_holds() {
    let _scope = ClusterScope::standalone();
    assert!(matches!(
        cluster::apply_lease_tick(1_700_000_000_000),
        Err(GrantError::NotClustered { .. })
    ));

    cluster::join(N1);
    cluster::apply_lease_tick(1_700_000_000_000).unwrap();
    assert_eq!(LeaseDeadline::try_now_millis().unwrap(), 1_700_000_000_000);

    cluster::leave();
    cluster::join(N2);
    assert!(
        LeaseDeadline::try_now_millis().is_err(),
        "GUARD FELL: a rejoin kept the previous authority's clock"
    );
}

// =================================================================================================
// A3.5 — one bad tick, and the clock can never come back.
// =================================================================================================

/// `apply_lease_tick` takes any `u64` with no upper bound and folds it with `max`, so a single
/// out-of-range tick sets the cluster clock to that value **for ever**: monotonicity means no later
/// correct tick can bring it back. Every lease in the cluster is then expired, and reaping is
/// destructive and unrecoverable.
#[test]
fn a3_5_one_absurd_tick_does_not_permanently_expire_every_lease() {
    let _scope = ClusterScope::standalone();
    let s = store("a3_5");
    let reaper = TwoTierReaper::new(Arc::clone(&s.catalog), Arc::clone(&s.store));

    cluster::join(N1);
    // A sane tick, and a branch with a long lease that is nowhere near expiry.
    let real = 1_700_000_000_000u64;
    cluster::apply_lease_tick(real).unwrap();
    let b = s.catalog.fork(BranchId::TRUNK, LeaseDeadline::try_from_now(86_400_000).unwrap()).unwrap();
    assert!(
        reaper.reap_expired(LeaseDeadline::try_now_millis().unwrap()).unwrap().is_empty(),
        "fixture: the branch was already expired"
    );

    // One tick from a buggy proposer, an unvalidated field, or a sign/units error.
    cluster::apply_lease_tick(u64::MAX).unwrap();

    // The cluster is told the correct time again, repeatedly.
    for _ in 0..5 {
        cluster::apply_lease_tick(real + 1_000).unwrap();
    }
    let now = LeaseDeadline::try_now_millis().unwrap();
    let reaped = reaper.reap_expired(now).unwrap();

    assert!(
        reaped.is_empty() && now != u64::MAX,
        "GUARD FELL: one tick of u64::MAX left the cluster clock at {now} and no later correct \
         tick could lower it (fold_tick is max). reap_expired then reaped {reaped:?} — the live \
         branch {:?} among them. Reaping bumps the BranchId generation, so this is unrecoverable, \
         and apply_lease_tick validates no bound on its input.",
        b.branch_id
    );
}

/// The mirror image: a tick far in the future makes two members disagree permanently, because the
/// one that saw it can never come back down to the one that did not.
#[test]
fn a3_6_a_spurious_future_tick_does_not_permanently_diverge_two_members() {
    let _scope = ClusterScope::standalone();
    let real = 1_700_000_000_000u64;

    // Member A applies the committed ticks plus one spurious future entry — an uncommitted suffix
    // a new leader later replaces, which consensus permits and this state machine cannot un-apply.
    cluster::join(N1);
    cluster::apply_lease_tick(real).unwrap();
    cluster::apply_lease_tick(real + 999_999_999).unwrap();
    cluster::apply_lease_tick(real + 1_000).unwrap();
    let a = LeaseDeadline::try_now_millis().unwrap();

    // Member B applies only the committed ticks.
    cluster::leave();
    cluster::join(N2);
    cluster::apply_lease_tick(real).unwrap();
    cluster::apply_lease_tick(real + 1_000).unwrap();
    let b = LeaseDeadline::try_now_millis().unwrap();

    assert_eq!(
        a, b,
        "GUARD FELL: after applying the same committed ticks, member A reads {a} and member B \
         reads {b} — a permanent {} ms divergence, because fold_tick is max and there is no way \
         to retract an applied tick. Exit criterion 9 is that two nodes never disagree here.",
        a - b
    );
}

// =================================================================================================
// A3.7 — a wrong value out of `from_now` rather than a panic.
// =================================================================================================

/// `from_now` is `now.saturating_add(millis)`. Saturation is not a refusal: it turns a finite lease
/// into one that can never expire, which "silently stops reaping and defeats exit criterion 8 with
/// no symptom" — the exact outcome the panic doc says it refuses to produce.
#[test]
fn a3_7_from_now_does_not_silently_saturate_into_a_lease_that_never_expires() {
    let _scope = ClusterScope::standalone();
    cluster::join(N1);
    cluster::apply_lease_tick(1_700_000_000_000).unwrap();

    let d = LeaseDeadline::try_from_now(u64::MAX).unwrap();
    assert_ne!(
        d.0,
        u64::MAX,
        "GUARD FELL: try_from_now(u64::MAX) saturated to a deadline of u64::MAX, which \
         is_expired_at can never report as expired for any reachable clock. from_now's own \
         documentation calls a value in the future the outcome that 'silently stops reaping and \
         defeats exit criterion 8 with no symptom', and returns it here rather than refusing."
    );
}

/// A lease minted while standalone is an absolute unix-millis deadline. The cluster clock is
/// whatever a `LeaseTick` said, and `apply_lease_tick` validates no relationship to unix time at
/// all — so a cluster ticking a logical counter evaluates pre-join leases against a different time
/// base.
#[test]
fn a3_8_a_lease_minted_standalone_is_still_evaluated_against_a_comparable_clock() {
    let _scope = ClusterScope::standalone();
    let s = store("a3_8");
    let reaper = TwoTierReaper::new(Arc::clone(&s.catalog), Arc::clone(&s.store));

    // Standalone: a branch whose lease expired an hour ago by the wall clock.
    let wall = LeaseDeadline::now_millis();
    let b = s.catalog.fork(BranchId::TRUNK, LeaseDeadline(wall - 3_600_000)).unwrap();
    assert_eq!(
        reaper.reap_expired(wall).unwrap(),
        vec![b.branch_id],
        "fixture: the expired branch was not reaped standalone"
    );

    // A second, identical branch, and this time the process joins a cluster whose leader ticks a
    // logical clock — nothing in `apply_lease_tick` says the argument must be unix millis.
    let s2 = store("a3_8b");
    let reaper2 = TwoTierReaper::new(Arc::clone(&s2.catalog), Arc::clone(&s2.store));
    let b2 = s2.catalog.fork(BranchId::TRUNK, LeaseDeadline(wall - 3_600_000)).unwrap();
    cluster::join(N1);
    cluster::apply_lease_tick(1).unwrap();
    cluster::apply_lease_tick(2).unwrap();
    let reaped = reaper2.reap_expired(LeaseDeadline::try_now_millis().unwrap()).unwrap();

    assert_eq!(
        reaped,
        vec![b2.branch_id],
        "GUARD FELL: a branch that expired an hour ago (deadline {}, wall clock {wall}) is not \
         reaped on a cluster member, because the cluster clock is {} — apply_lease_tick accepts \
         any u64 and never checks it against unix time. Exit criterion 8 is silently off for \
         every lease minted before the join.",
        wall - 3_600_000,
        LeaseDeadline::try_now_millis().unwrap()
    );
}
