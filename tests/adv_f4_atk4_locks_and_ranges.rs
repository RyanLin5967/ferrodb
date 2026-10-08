//! ATTACK 4 — the lock order, and the shape of a grant range.
//!
//! Two questions.
//!
//! **The lock order.** `cluster::authority_at()` takes a process mutex, `GrantedCounter::take`
//! takes a counter mutex, and `TxnManager::begin` holds the active-transaction table across both.
//! Three locks and a documented claim that the counter lock "is a leaf: nothing inside it calls
//! out". This hammers all three from many threads while the authority changes underneath them.
//!
//! **The range.** `ArenaPageStore::load_state` refuses an image whose `base_page` is not this
//! store's, because grafting another region's map on "would silently graft another arena's extents
//! onto this one's space". `apply_arena_grant` has no equivalent check, so these ask what a leader
//! can make a node allocate by proposing an odd range.
//!
//! Tests named `*_holds` assert a property the guard is supposed to have.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, ARENA_EXTENT_PAGES};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cluster::{self, ClusterScope, GrantError, GrantedCounter};
use ferrodb::consensus::NodeId;
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const ARENA_BASE: u32 = 1024;
const N1: NodeId = NodeId(1);

struct Store {
    store: Arc<ArenaPageStore>,
    _catalog: Arc<LogBranchCatalog>,
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
    Store { store, _catalog: catalog, _dir: dir }
}

fn txn_manager(tag: &str) -> (Arc<TxnManager>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(Arc::clone(&wal), Arc::clone(&bp)));
    bp.attach_wal(wal);
    (txn, dir)
}

// =================================================================================================
// A4.1 — the three-lock deadlock hunt.
// =================================================================================================

/// Every lock in the chain, contended, while `join`/`leave` change the authority underneath.
///
/// A deadlock here would hang the test binary rather than fail it, so progress is measured from
/// outside: each worker bumps a shared counter, and the test fails if the total stops advancing
/// while workers are still supposed to be running. That is the only signal that distinguishes
/// "wedged" from "slow" — a live thread count cannot.
#[test]
fn a4_1_hammering_authority_counter_and_att_locks_does_not_deadlock_holds() {
    let _scope = ClusterScope::standalone();
    let (txn, _dir) = txn_manager("a4_1");
    let s = store("a4_1_arena");
    let counter = Arc::new(GrantedCounter::new("probe", 0, 16));

    let stop = Arc::new(AtomicBool::new(false));
    let progress = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    // 1. begin(): holds `att`, then takes the process lock, then the counter lock.
    for _ in 0..4 {
        let (txn, stop, progress) = (Arc::clone(&txn), Arc::clone(&stop), Arc::clone(&progress));
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = txn.begin();
                let _ = txn.begin_snapshot_read();
                let _ = txn.next_txn_id();
                progress.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // 2. the bare counter: process lock then counter lock, no `att`.
    for _ in 0..4 {
        let (c, stop, progress) =
            (Arc::clone(&counter), Arc::clone(&stop), Arc::clone(&progress));
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = c.take(1);
                let _ = c.apply_grant(N1, 0, 4_096);
                let _ = c.remaining();
                let _ = c.issued_through();
                c.raise_issued_through(1);
                progress.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // 3. the arena store: free-extent mutex, state mutex, two counters.
    for _ in 0..4 {
        let (st, stop, progress) = (Arc::clone(&s.store), Arc::clone(&stop), Arc::clone(&progress));
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(a) = st.arena_for(BranchId::TRUNK) {
                    let _ = st.free_arena(a);
                }
                let _ = st.apply_arena_grant(N1, ARENA_BASE, 4 * ARENA_EXTENT_PAGES);
                let _ = st.grantable_extents();
                progress.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // 4. the authority itself, changing under all of the above.
    for _ in 0..2 {
        let (stop, progress) = (Arc::clone(&stop), Arc::clone(&progress));
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                cluster::join(N1);
                let _ = cluster::authority_at();
                let _ = cluster::apply_lease_tick(1_700_000_000_000);
                cluster::leave();
                let _ = cluster::epoch();
                progress.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Watch for a stall. Poll on the artifact — the progress counter — not on a timer alone.
    let mut last = 0u64;
    let mut stalls = 0;
    for round in 0..40 {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let now = progress.load(Ordering::Relaxed);
        if now == last {
            stalls += 1;
            assert!(
                stalls < 8,
                "GUARD FELL (deadlock): progress stopped at {now} operations after round {round} \
                 and did not advance for 8 consecutive 50 ms polls with 14 workers running."
            );
        } else {
            stalls = 0;
        }
        last = now;
    }
    stop.store(true, Ordering::Relaxed);

    // Every worker must be joinable. A worker still holding a lock never returns from join().
    for h in handles {
        h.join().expect("a worker panicked");
    }
    let total = progress.load(Ordering::Relaxed);
    assert!(total > 1_000, "the stress loop barely ran ({total} ops); this proves nothing");
    println!("a4_1: {total} operations across 14 threads, no stall");
}

// =================================================================================================
// A4.2 — what a leader can make a node allocate.
// =================================================================================================

/// A grant that reaches below the store's own region. `load_state` refuses a foreign `base_page`;
/// `apply_arena_grant` performs no such check, so the only thing standing in the way is that the
/// extent counter is *seeded* at `base_page`, which makes `Grants::apply_grant` clamp `lo` up.
#[test]
fn a4_2_a_grant_below_the_region_base_cannot_produce_an_extent_outside_it_holds() {
    let _scope = ClusterScope::standalone();
    let s = store("a4_2");
    cluster::join(N1);

    // A leader that thinks the file starts at page 0 — where the heap and index pages live. The
    // range must STRADDLE the base, or the whole grant is swallowed as a duplicate (the counter is
    // seeded at `base_page`, so `accepted_through` starts at 1024) and this test would pass by
    // allocating nothing at all.
    s.store.apply_arena_grant(N1, 0, 8 * ARENA_EXTENT_PAGES).unwrap();
    let mut produced = Vec::new();
    let mut outside = Vec::new();
    for _ in 0..8 {
        match s.store.arena_for(BranchId::TRUNK) {
            Ok(a) => {
                if let Some((start, count)) = s.store.extent_range(a) {
                    produced.push((a, start));
                    if start < ARENA_BASE {
                        outside.push((a, start, count));
                    }
                }
            }
            Err(_) => break,
        }
    }
    // Force the detector to fire: a pass is only meaningful if extents were actually allocated
    // from a grant that reached below the base.
    assert!(
        !produced.is_empty(),
        "DETECTOR DID NOT FIRE: the straddling grant produced no extent, so this test proves \
         nothing about containment"
    );
    assert!(
        outside.is_empty(),
        "GUARD FELL: a grant of [0, {}) produced extent(s) {outside:?} below the store's region \
         base {ARENA_BASE}. Those pages hold heap and index data. (all extents: {produced:?})",
        8 * ARENA_EXTENT_PAGES
    );
    println!("a4_2: straddling grant [0, {}) produced {produced:?}", 8 * ARENA_EXTENT_PAGES);
}

/// An empty and a backwards range.
#[test]
fn a4_3_empty_and_backwards_grants_are_refused_holds() {
    let _scope = ClusterScope::standalone();
    let c = GrantedCounter::new("probe", 0, 4);
    cluster::join(N1);

    assert!(matches!(c.apply_grant(N1, 500, 500), Err(GrantError::EmptyRange { .. })));
    assert!(matches!(c.apply_grant(N1, 900, 500), Err(GrantError::EmptyRange { .. })));
    assert_eq!(c.remaining(), 0, "a refused range left usable space");

    let s = store("a4_3");
    // page_count 0 through the real wiring.
    assert!(s.store.apply_arena_grant(N1, 4_096, 0).is_err(), "a zero-page grant was accepted");
    assert!(s.store.arena_for(BranchId::TRUNK).is_err(), "an empty grant produced an extent");
}

/// A grant whose top does not fit a `PageId`. `reserve` converts with `u32::try_from` and refuses,
/// rather than truncating and aliasing page 0.
#[test]
fn a4_4_a_grant_past_the_page_id_space_refuses_rather_than_truncates_holds() {
    let _scope = ClusterScope::standalone();
    let c = GrantedCounter::new("extent-start", u32::MAX as u64 - 10, ARENA_EXTENT_PAGES as u64);
    cluster::join(N1);

    // A range that starts inside u32 and ends outside it.
    c.apply_grant(N1, u32::MAX as u64 - 10, u32::MAX as u64 + 5_000).unwrap();
    let v = c.take(ARENA_EXTENT_PAGES as u64).unwrap();
    assert!(
        u32::try_from(v).is_err() || v <= u32::MAX as u64,
        "a value was issued that is neither a valid page id nor refused: {v}"
    );
    // And the real wiring must turn that into a refusal, not a truncation.
    let s = store("a4_4");
    // u32 arithmetic in apply_arena_grant: first_page + page_count as u64 cannot overflow u64, so
    // the widest possible grant is representable and the guard has to be the take-side conversion.
    let widest = s.store.apply_arena_grant(N1, u32::MAX - 1, u32::MAX);
    let got = s.store.arena_for(BranchId::TRUNK);
    if let Ok(a) = got {
        let range = s.store.extent_range(a);
        // The extent START fits a u32, so `reserve`'s try_from lets it through — but the extent is
        // ARENA_EXTENT_PAGES wide, so its pages run off the end of the page-id space. Find out what
        // allocating inside it does.
        let alloc = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            s.store.alloc_in_arena(a, ferrodb::cow::PageType::BTreeLeaf, ferrodb::branch::types::Epoch(1))
        }));
        panic!(
            "GUARD FELL: apply_arena_grant({N1}, {}, {}) returned {widest:?} and arena_for \
             produced arena {a} at {range:?}. reserve() checks only that the extent START fits a \
             PageId; the extent is {ARENA_EXTENT_PAGES} pages wide, so pages {}..{} run past \
             u32::MAX. Allocating in it: {}",
            u32::MAX - 1,
            u32::MAX,
            u32::MAX - 1,
            (u32::MAX as u64 - 1) + ARENA_EXTENT_PAGES as u64,
            match &alloc {
                Ok(r) => format!("{r:?}"),
                Err(_) => "PANICKED (arithmetic overflow on start_page + next_free)".to_string(),
            }
        );
    }
}

// =================================================================================================
// A4.5 — arena ids and page ids share one number space.
// =================================================================================================

/// `apply_arena_grant` applies the *same* `[lo, hi)` to both the extent-start counter and the
/// arena-id counter, so in a cluster an arena id is a page number. Standalone arena ids start at 1.
/// A2.9 found no collision only because the grant range was high; this asks what happens when it is
/// not.
#[test]
fn a4_5_a_low_page_grant_cannot_collide_with_standalone_arena_ids() {
    let _scope = ClusterScope::standalone();

    // Node A, its own leader. `free_arena` returns the extent start to the recycle stack and
    // `reserve` pops it, so pages are reused while arena ids keep climbing — which is how a node
    // reaches arena id 1024 without a 262144-page file.
    let a = store("a4_5_solo");
    let mut solo = Vec::new();
    for _ in 0..(ARENA_BASE as usize + 40) {
        let arena = a.store.arena_for(BranchId::TRUNK).unwrap();
        solo.push(arena);
        a.store.free_arena(arena).unwrap();
    }
    // Force the detector to fire: the collision is only reachable if standalone ids climbed into
    // the page-number range a leader would grant.
    let highest = solo.iter().map(|x| x.0).max().unwrap();
    assert!(
        highest >= ARENA_BASE,
        "DETECTOR DID NOT FIRE: standalone arena ids only reached {highest}, below the region \
         base {ARENA_BASE}, so no grant this test can make would overlap them"
    );

    // Node B, granted the region base onwards — pages [1024, 1536), hence arena ids [1024, 1536).
    cluster::join(N1);
    let b = store("a4_5_member");
    b.store.apply_arena_grant(N1, ARENA_BASE, 2 * ARENA_EXTENT_PAGES).unwrap();
    let member = b.store.arena_for(BranchId::TRUNK).expect(
        "DETECTOR DID NOT FIRE: the member could not allocate from its grant, so no arena id was \
         issued to compare",
    );

    assert!(
        !solo.contains(&member),
        "GUARD FELL: arena id {member} was issued both by node A's standalone self-grant \
         (standalone ids reached {highest}) and by a leader grant of pages \
         [{ARENA_BASE}, {}) to {N1}. apply_arena_grant feeds ONE range to BOTH counters, so a \
         page range doubles as an arena-id range and the two number spaces are the same. \
         BranchRecord::arenas then names one arena for two branches, and the reaper frees exactly \
         record.arenas.",
        ARENA_BASE + 2 * ARENA_EXTENT_PAGES
    );
}
