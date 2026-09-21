//! D59 — a cached snapshot must be indistinguishable from a freshly built one.
//!
//! `read_snapshot` took the active-transaction table's mutex and allocated a `HashSet` of every
//! open transaction, once per statement: 17.4k of ~80k thread-samples at 16 agent readers once
//! D58 had taken the page latch off the read path. `read_snapshot_cached` reuses the last
//! snapshot this thread built while the table's version has not moved — Postgres 14's
//! `xactCompletionCount`, and D54's catalog epoch in this repo.
//!
//! The whole correctness argument is "the version moves on every change, inside the critical
//! section that makes it". So these tests attack exactly that:
//!
//! 1. a transaction that BEGINS must appear in the next cached snapshot;
//! 2. a transaction that COMMITS must disappear from it, and become `includes`-visible;
//! 3. and under a writer churning begins and commits, a cached snapshot taken while the version
//!    did not move must equal the uncached one — which is what fails if the bump is published
//!    outside the lock, or if the snapshot is labelled with a version read before it.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn manager(dir: &tempfile::TempDir, name: &str) -> Arc<TxnManager> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{name}.db")))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{name}.wal"))).unwrap());
    bp.attach_wal(wal.clone());
    Arc::new(TxnManager::new(wal, bp))
}

#[test]
fn a_transaction_that_begins_is_in_the_next_cached_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "begin");

    // Warm the cache, so the next call is the one that must miss.
    let before = t.read_snapshot_cached();
    assert!(before.active.is_empty(), "the fixture started with an open transaction");
    let warm = t.read_snapshot_cached();
    assert!(warm.active.is_empty());

    let id = t.begin().unwrap();
    let after = t.read_snapshot_cached();
    assert!(
        after.active.contains(&id),
        "a cached snapshot taken after BEGIN {id} does not show it active ({:?}): the version did \
         not move, so every reader on this thread is looking at a snapshot from before it",
        after.active
    );
    // And the uncached truth agrees, which is what "indistinguishable" means.
    assert_eq!(after.active, t.read_snapshot().active);
    assert!(!after.includes(id), "an active transaction must not be included by a snapshot");
}

#[test]
fn a_transaction_that_commits_leaves_the_next_cached_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "commit");

    let id = t.begin().unwrap();
    let during = t.read_snapshot_cached();
    assert!(during.active.contains(&id));
    let _warm = t.read_snapshot_cached();

    t.commit(id).unwrap();
    let after = t.read_snapshot_cached();
    assert!(
        !after.active.contains(&id),
        "a cached snapshot taken after COMMIT {id} still lists it active: a committed \
         transaction stays invisible for ever on this thread"
    );
    assert!(
        after.includes(id),
        "after COMMIT {id} the snapshot does not include it, so its rows are invisible \
         (high_water {}, active {:?})",
        after.high_water,
        after.active
    );
    assert_eq!(after.active, t.read_snapshot().active);
}

#[test]
fn an_abort_also_moves_the_version() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "abort");
    let id = t.begin().unwrap();
    assert!(t.read_snapshot_cached().active.contains(&id));
    let _warm = t.read_snapshot_cached();
    t.abort(id).unwrap();
    let after = t.read_snapshot_cached();
    assert!(!after.active.contains(&id), "an aborted transaction is still listed active");
    assert_eq!(after.active, t.read_snapshot().active);
}

/// The ordering property, forced: a writer churns begins and commits while readers take cached
/// snapshots. **If the version did not move across a cached read, that snapshot must equal the
/// one the lock would have produced.** A bump published after the lock is released — or a
/// snapshot labelled with a version read before the table was — breaks exactly this.
#[test]
fn a_cached_snapshot_equals_the_locked_one_when_the_version_did_not_move() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "race");
    let stop = Arc::new(AtomicBool::new(false));
    let churned = Arc::new(AtomicU64::new(0));

    let writer = {
        let (t, stop, churned) = (t.clone(), stop.clone(), churned.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let id = t.begin().unwrap();
                churned.fetch_add(1, Ordering::Relaxed);
                if id % 2 == 0 {
                    t.commit(id).unwrap();
                } else {
                    t.abort(id).unwrap();
                }
                // **Also move the watermark WITHOUT touching the table** — recovery and a cluster
                // TxnIdRange grant do exactly this, and it is the only writer of `high_water` the
                // active set does not account for. Without it this detector cannot see the field.
                if id % 3 == 0 {
                    t.raise_next_txn_id(t.next_txn_id() + 1000);
                }
                churned.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (t, stop, churned) = (t.clone(), stop.clone(), churned.clone());
            std::thread::spawn(move || {
                let (mut checked, mut skipped) = (0u64, 0u64);
                let t0 = Instant::now();
                // Run until the detector has demonstrably FIRED, not for a fixed stretch of
                // clock. The assertions below require the writer to have churned and the readers
                // to have both checked and skipped; a wall-clock window makes those counts a
                // function of how busy the machine is, which is not a property of the code under
                // test. A fixed 4s window put the churn count right ON its own >1000 threshold
                // under fleet load — measured 736 and 752 in-target, and 980 / pass / pass when
                // run alone — so the test failed its vacuity guard rather than anything it was
                // testing, intermittently, for everyone.
                //
                // The assertions' thresholds are NOT lowered: they are the only thing keeping this
                // test from passing without exercising the race. These are the loop's EXIT
                // condition instead, which is a stronger guarantee than a margin — the loop cannot
                // end below them except by hitting `CAP`, so each is set to 2x its assertion
                // rather than to a number chosen for luck. `CAP` is a deadlock guard, not the
                // budget.
                const NEED_CHURN: u64 = 2_000;
                const NEED_CHECKED: u64 = 500;
                const CAP: Duration = Duration::from_secs(60);
                let fired = |checked: u64, skipped: u64, churned: &AtomicU64| {
                    churned.load(Ordering::Relaxed) >= NEED_CHURN
                        && checked >= NEED_CHECKED
                        && skipped >= 1
                };
                while !stop.load(Ordering::Relaxed)
                    && t0.elapsed() < CAP
                    && !fired(checked, skipped, &churned)
                {
                    let v1 = t.att_version();
                    let cached = t.read_snapshot_cached();
                    let locked = t.read_snapshot();
                    let v2 = t.att_version();
                    if v1 != v2 {
                        // The table changed under us; the two are allowed to differ.
                        skipped += 1;
                        continue;
                    }
                    assert_eq!(
                        cached.active, locked.active,
                        "the version did not move ({v1}) but the cached snapshot disagrees with \
                         the locked one: cached {:?}, locked {:?}",
                        cached.active, locked.active
                    );
                    // BOTH fields. The first version of this test compared only `active` — the
                    // half `att_version` obviously covers — so a `high_water` that went stale
                    // (the writer above now forces that case) was invisible to it.
                    assert_eq!(
                        cached.high_water, locked.high_water,
                        "the version did not move ({v1}) but the cached high_water is {} against \
                         the locked {}",
                        cached.high_water, locked.high_water
                    );
                    checked += 1;
                }
                (checked, skipped)
            })
        })
        .collect();

    let (mut checked, mut skipped) = (0u64, 0u64);
    for r in readers {
        let (c, s) = r.join().unwrap();
        checked += c;
        skipped += s;
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    // The detector must have been able to fire: the writer churned, and readers both checked and
    // were forced to skip — i.e. their reads really did interleave with the writer's changes.
    let n = churned.load(Ordering::Relaxed);
    assert!(n > 1000, "the writer churned only {n} times; the race was not exercised");
    assert!(checked > 1000, "only {checked} comparisons were made");
    assert!(skipped > 0, "no read ever overlapped a change ({checked} checked): not a race");
}

/// **`high_water` is the half of a snapshot the active-transaction table does not cover.**
/// `begin` issues an id and inserts into the table in one critical section, so ordinary operation
/// moves the version anyway — but recovery and a cluster `TxnIdRange` grant raise the watermark
/// with the table untouched. A cached snapshot would then keep an old `high_water` for as long as
/// no transaction began or ended, and `includes` would answer "not yet committed" for a
/// transaction whose rows are on disk. Found by a fresh-context review of the first version, which
/// argued this could not matter instead of enforcing it.
#[test]
fn raising_the_id_watermark_invalidates_a_cached_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "highwater");

    let before = t.read_snapshot_cached();
    let _warm = t.read_snapshot_cached();
    let raised = before.high_water + 500;
    t.raise_next_txn_id(raised);

    let after = t.read_snapshot_cached();
    assert!(
        after.high_water >= raised,
        "a cached snapshot kept high_water {} after the watermark was raised to {raised}: every \
         transaction id in between reads as 'not yet committed' on this thread",
        after.high_water
    );
    assert_eq!(after.high_water, t.read_snapshot().high_water);
    // The consequence the number stands for: an id below the new watermark, with nothing active,
    // must be included.
    assert!(after.includes(raised - 1), "id {} is below the watermark and not active", raised - 1);
}

/// The whole-snapshot comparison, not just the active set: a cached snapshot must match the
/// locked one in **both** fields. The first version of this file compared `active` only, which is
/// the field `att_version` obviously covers — the one it does not cover went untested.
#[test]
fn a_cached_snapshot_matches_the_locked_one_in_high_water_too() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "bothfields");
    for step in 0..8 {
        let id = t.begin().unwrap();
        if step % 2 == 0 {
            t.commit(id).unwrap();
        } else {
            t.abort(id).unwrap();
        }
        t.raise_next_txn_id(t.next_txn_id() + 7);
        let cached = t.read_snapshot_cached();
        let locked = t.read_snapshot();
        assert_eq!(cached.high_water, locked.high_water, "step {step}: high_water disagrees");
        assert_eq!(cached.active, locked.active, "step {step}: active set disagrees");
    }
}

/// Two managers on one thread must not share a cache entry. The entry is keyed by a per-manager
/// id precisely because a dropped manager's address is reused and every test builds several; the
/// check had no test, so it was an assertion rather than a property.
#[test]
fn two_managers_on_one_thread_do_not_share_a_cached_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let a = manager(&dir, "mgr_a");
    let b = manager(&dir, "mgr_b");

    // **Both managers must reach the SAME version with DIFFERENT content**, or the version check
    // alone separates them and the manager id is never load-bearing. (The first version of this
    // test left `a` at version 1 and `b` at 0, and the mutant "ignore the id" passed it.)
    //   a: begin, begin   -> version 2, active {x, y}
    //   b: begin, commit  -> version 2, active {}
    let x = a.begin().unwrap();
    let y = a.begin().unwrap();
    let z = b.begin().unwrap();
    b.commit(z).unwrap();
    assert_eq!(a.att_version(), b.att_version(), "the fixture failed to align the two versions");

    let sa = a.read_snapshot_cached();
    assert_eq!(sa.active.len(), 2, "manager a lost its own transactions: {:?}", sa.active);
    let sb = b.read_snapshot_cached();
    assert!(
        sb.active.is_empty(),
        "manager b was handed manager a's active set ({:?}) at the same version: the cache is not \
         keyed by manager",
        sb.active
    );
    // The other direction too, so a cache that always misses one way cannot pass.
    assert!(b.read_snapshot_cached().active.is_empty());
    assert_eq!(a.read_snapshot_cached().active.len(), 2);
    a.commit(x).unwrap();
    a.commit(y).unwrap();
}

/// **After recovery, a thread's cached snapshot must agree with the locked one.**
///
/// `recover` reinstates every loser transaction so the undo pass can abort it — an addition to the
/// active set from outside the ordinary begin/commit flow, and it used a raw `att.lock()` until a
/// fresh-context review found it. That is fixed (it goes through `att_write()` now), and the class
/// is prevented structurally: the field is private, `att_read` is `Deref`-only, and
/// `lock_order_allowlist::only_the_att_accessors_may_lock_the_active_transaction_table` refuses a
/// raw lock inside `txn.rs`.
///
/// ⚠ **This test does not fire-check that fix, and saying so is the point.** Planting the
/// unguarded insert back leaves it GREEN: recovery aborts each loser immediately, and the abort's
/// `remove` (and `raise_next_txn_id`) bump the version anyway, so the skipped bump is invisible
/// from outside — the window opens and closes inside `recover`. What this pins is the end state:
/// a thread that cached a snapshot before recovery must not keep it afterwards. A recovery that
/// left the version behind ALTOGETHER fails here.
#[test]
fn recovery_moves_the_version_for_a_thread_that_already_cached_a_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    // A WAL with an un-ended transaction in it: open a manager, write a record, and drop it
    // without committing. The next open's recovery has a loser to reinstate.
    let id = {
        let t = manager(&dir, "recov");
        let id = t.begin().unwrap();
        // A DDL record: it chains onto the transaction (so recovery sees the id) and recovery's
        // undo pass walks past it without touching a heap page, which keeps this test about the
        // ATT rather than about page replay. `TxnEnd` would be wrong — it ENDS the transaction,
        // so there would be no loser and this test would pass vacuously. It did, until the
        // mutant "insert without the guard" survived it.
        t.append_chained(
            id,
            &ferrodb::wal::log::RecKind::Ddl {
                op: ferrodb::wal::log::DdlOp::CreateTable,
                table: "t".into(),
                dir_root: 0,
                time_travel_root: 0,
                columns: Vec::new(),
            },
        )
        .unwrap();
        t.wal.flush().unwrap();
        id
    };

    // A second manager over the SAME wal file, and a snapshot cached on this thread BEFORE
    // recovery runs.
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.path().join("recov2.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("recov.wal")).unwrap());
    bp.attach_wal(wal.clone());
    let t2 = Arc::new(TxnManager::new(wal, bp));

    let before = t2.read_snapshot_cached();
    let _warm = t2.read_snapshot_cached();
    assert!(before.active.is_empty(), "the fresh manager started with active transactions");

    let before_version = t2.att_version();
    let _ = ferrodb::wal::recovery::recover(&t2);
    // **The fixture must have given recovery something to do**, or this test proves nothing: it
    // has to reinstate the loser (an ATT change) and raise the watermark past it.
    assert!(
        t2.att_version() > before_version,
        "recovery changed nothing observable (version still {before_version}): the fixture left \
         no loser transaction, so the path under test never ran"
    );

    // Whatever recovery did to the table, the cached read must agree with the locked one. That is
    // the property; the loser's presence is the fixture's business, and asserting it directly
    // would make this test depend on the undo pass's bookkeeping rather than on the cache.
    let after = t2.read_snapshot_cached();
    let locked = t2.read_snapshot();
    assert_eq!(
        after.active, locked.active,
        "after recovery the cached snapshot disagrees with the locked one (cached {:?}, locked \
         {:?}): recovery changed the active set without moving the version",
        after.active, locked.active
    );
    assert_eq!(after.high_water, locked.high_water, "recovery raised the watermark unseen");
    assert!(after.high_water > id, "the fixture's transaction {id} is not below the watermark");
}

/// **Deterministic companion to the race test above.** That test caught the unlocked
/// raise-then-bump ordering once in a full suite run, and when measured against the mutant it
/// fired in 3 of 8 runs — a detector that misses a real bug most of the time. The outcome it
/// guards is "(watermark, version) change as one fact", and the mechanism that makes that
/// certain is that a raise happens INSIDE the table's critical section. So: hold the table lock
/// on this thread, raise on another, and the watermark must not move until the lock is dropped.
/// The mutant (raise outside the lock) fails this every time.
#[test]
fn a_watermark_raise_waits_for_the_table_lock() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "raiselock");
    let before = t.next_txn_id();
    let target = before + 500;

    let held = t.att_read();
    let started = Arc::new(std::sync::Barrier::new(2));
    let raiser = {
        let (t, started) = (t.clone(), started.clone());
        std::thread::spawn(move || {
            started.wait();
            t.raise_next_txn_id(target);
        })
    };
    started.wait();
    // Give an unlocked raise every chance to complete. A locked one blocks until `held` drops,
    // however long this is; an unlocked one finishes in microseconds.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        t.next_txn_id(),
        before,
        "the id watermark moved from {before} while another thread held the transaction table's \
         lock: the raise is not inside the critical section that bumps the version, so a reader \
         can see the new watermark at the old version"
    );
    drop(held);
    raiser.join().unwrap();
    assert!(t.next_txn_id() >= target, "the raise never happened");
}
