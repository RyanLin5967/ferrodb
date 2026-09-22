//! **D126 — a rewritten key must never be transiently ABSENT.**
//!
//! `BPlusTreeManager` had no replace primitive, so every caller that rewrote a key open-coded
//! `delete` then `insert`. `delete` takes the leaf's write latch and **drops it on return**;
//! `insert` re-acquires it. Between the two the key does not exist, and the point-lookup path
//! (`search` -> `read_leaf_for` -> `descend_optimistic`) takes no latch at all. So a concurrent
//! reader sees the key vanish and come back on **every ordinary rewrite** of a live key.
//!
//! `TableBranchCatalog::upsert` was that shape, and `write_record` routes the branch RECORD key
//! through it -- reached from `set_state`, `set_root`, `renew_lease`, `reparent`,
//! `restrict_envelope` and `put`. `set_root` and `renew_lease` are hot-path writes. D124's page
//! guards are pre-positioned against exactly that absent-record state and their comments called it
//! impossible; it was not. D126 makes the premise true by giving the tree
//! [`BPlusTreeManager::upsert`], which applies the removal and the addition to one in-memory leaf
//! image and therefore to one page write.
//!
//! # Why this file is a probe and not an argument
//!
//! Every arm here has a **control in the same body that must FIRE**. The control performs the
//! delete-then-insert the fix removed, on the same tree, the same key, the same reader threads and
//! the same iteration count -- so `misses == 0` in the treatment arm is only admissible because
//! `misses > 0` was demonstrated on the identical harness one arm earlier. A probe whose control
//! comes back clean is not evidence that the defect is gone; it is evidence that the probe cannot
//! see it, and this file fails loudly in that case rather than passing.
//!
//! The reader's work is counted in both arms and asserted, for the same reason: a treatment arm
//! whose readers happened to run fewer probes would report a smaller, calmer zero.
//!
//! **Not reachable in production today** -- reader and writer are both inside the per-statement
//! mutex. W4 removes that mutex, which is why the window has to be gone before W4 and not after.

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::index::BPlusTreeManager;

type Tree = BPlusTreeManager<Vec<u8>, Vec<u8>>;

/// Keys loaded before the probe starts. Enough to push the tree past a single leaf so the target
/// key shares a leaf with neighbours and the descent is a real one.
const PREPOP: u32 = 600;
/// Rewrites the writer performs per arm.
const WRITES: usize = 4_000;
/// Readers spinning on the target key.
const READERS: usize = 3;

fn key(k: u32) -> Vec<u8> {
    k.to_be_bytes().to_vec()
}

/// A fixed-width value, so a rewrite is the same length as what it replaces. That is the shape
/// `set_root`/`renew_lease` produce (a serialised core record), and it is the shape that must stay
/// on the single-latch fast path.
fn val(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn fresh_tree(tag: &str) -> (tempfile::TempDir, Arc<Tree>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let tree = Arc::new(Tree::create(bp).expect("create index"));
    for k in 0..PREPOP {
        tree.insert(key(k), val(k as u64)).expect("prepop");
    }
    (dir, tree)
}

/// What one arm observed. `misses` is the signal; `reads` exists so a zero can be told apart from
/// a reader that never got to run.
#[derive(Debug, Default)]
struct Arm {
    misses: u64,
    reads: u64,
    errors: u64,
}

/// Run `writer` `WRITES` times on the target key while `READERS` threads point-look it up.
///
/// The writer closure is the ONLY difference between the control and the treatment arm.
fn probe<W>(tree: &Arc<Tree>, target: Vec<u8>, writer: W) -> Arm
where
    W: Fn(&Tree, Vec<u8>, Vec<u8>) + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let misses = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(READERS + 1));

    let mut readers = Vec::new();
    for _ in 0..READERS {
        let (t, k) = (Arc::clone(tree), target.clone());
        let (stop, misses, reads, errors, barrier) = (
            Arc::clone(&stop),
            Arc::clone(&misses),
            Arc::clone(&reads),
            Arc::clone(&errors),
            Arc::clone(&barrier),
        );
        readers.push(std::thread::spawn(move || {
            barrier.wait();
            let (mut m, mut r, mut e) = (0u64, 0u64, 0u64);
            while !stop.load(Ordering::Relaxed) {
                match t.search(&k) {
                    Ok(Some(_)) => {}
                    // THE SIGNAL. The key is present before the probe starts and nothing in
                    // either arm removes it permanently, so `None` can only be the rewrite
                    // window.
                    Ok(None) => m += 1,
                    Err(_) => e += 1,
                }
                r += 1;
            }
            misses.fetch_add(m, Ordering::Relaxed);
            reads.fetch_add(r, Ordering::Relaxed);
            errors.fetch_add(e, Ordering::Relaxed);
        }));
    }

    barrier.wait();
    for i in 0..WRITES {
        writer(tree, target.clone(), val(1_000_000 + i as u64));
    }
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().expect("reader thread");
    }

    Arm {
        misses: misses.load(Ordering::Relaxed),
        reads: reads.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
    }
}

/// The delete-then-insert the catalog used to open-code, reproduced exactly.
fn delete_then_insert(t: &Tree, k: Vec<u8>, v: Vec<u8>) {
    match t.delete(&k) {
        Ok(()) | Err(FerroError::KeyNotFound) => {}
        Err(e) => panic!("control delete failed: {e:?}"),
    }
    t.insert(k, v).expect("control insert");
}

fn atomic_upsert(t: &Tree, k: Vec<u8>, v: Vec<u8>) {
    t.upsert(k, v).expect("upsert");
}

#[test]
fn a_rewritten_key_is_never_absent_to_a_concurrent_reader() {
    let target = key(PREPOP / 2);

    // CONTROL FIRST, and it must fire. This is the delete-then-insert the fix removed.
    let (_d1, t1) = fresh_tree("d126-control");
    let control = probe(&t1, target.clone(), delete_then_insert);
    println!(
        "D126 control  (delete+insert): misses={} reads={} errors={}",
        control.misses, control.reads, control.errors
    );
    assert!(
        control.misses > 0,
        "THE PROBE IS NOT DISCRIMINATING. delete-then-insert leaves the key absent between the \
         two calls by construction, and {READERS} readers over {WRITES} rewrites saw it \
         {} times in {} reads. Either the readers did not overlap the writer or `search` is not \
         reaching the tree; the treatment arm's zero below would mean nothing until this fires.",
        control.misses, control.reads
    );

    // TREATMENT: identical harness, one atomic upsert in place of the two calls.
    let (_d2, t2) = fresh_tree("d126-atomic");
    let treatment = probe(&t2, target.clone(), atomic_upsert);
    println!(
        "D126 treatment (tree.upsert) : misses={} reads={} errors={}",
        treatment.misses, treatment.reads, treatment.errors
    );
    assert_eq!(
        treatment.misses, 0,
        "a concurrent reader observed the key ABSENT {} times in {} reads while `upsert` \
         rewrote it. The removal and the addition are supposed to share one page write.",
        treatment.misses, treatment.reads
    );
    assert_eq!(treatment.errors, 0, "reads failed during the atomic rewrite");
    assert!(
        treatment.reads >= control.reads / 4,
        "the treatment arm's readers did only {} reads against the control's {} -- too few for \
         its zero to be comparable. A smaller, calmer number from a smaller scope is the failure \
         this assertion exists to catch.",
        treatment.reads,
        control.reads
    );

    // And the key must still hold the LAST value written, not an earlier one and not two entries.
    let expected = val(1_000_000 + (WRITES - 1) as u64);
    assert_eq!(t2.search(&target).unwrap(), Some(expected), "final value after the upsert run");
    let dupes = occurrences(&t2, &target);
    assert_eq!(dupes, 1, "`upsert` left {dupes} entries for one key; it must leave exactly one");
}

/// How many entries the tree actually holds for `k`, counted by walking the **whole** leaf chain.
///
/// ⚠ NOT a bounded `range_scan` on `k` itself, which is what the first cut of this file used and
/// which cannot answer the question: `range_scan`'s lower bound lands via
/// `BPlusTreeLeafPage::binary_search`, and `slice::binary_search` on an array containing
/// duplicates returns *an* index of a match rather than the first, so a scan seeded that way can
/// begin after a duplicate and report 1 where there are 2. That is exactly how the duplicate
/// assertion in `insert_still_does_not_replace` passed while measuring nothing.
fn occurrences(tree: &Tree, k: &[u8]) -> usize {
    tree.range_scan(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .filter(|e| matches!(e, Ok((kk, _)) if kk.as_slice() == k))
        .count()
}

/// `upsert` must remain correct when the rewrite does NOT fit the leaf and has to split it.
///
/// The fast path returns "would split, nothing written" and the whole write is retried with write
/// latches on the root-to-leaf path. That retry is the arm where an implementation is most likely
/// to reintroduce a delete-then-insert, so it gets its own probe: values GROW on every rewrite,
/// which forces the leaf over the threshold repeatedly.
#[test]
fn a_growing_rewrite_that_splits_is_still_never_absent() {
    let (_d, tree) = fresh_tree("d126-split");
    let target = key(PREPOP / 3);
    let stop = Arc::new(AtomicBool::new(false));
    let misses = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(READERS + 1));

    let mut readers = Vec::new();
    for _ in 0..READERS {
        let (t, k) = (Arc::clone(&tree), target.clone());
        let (stop, misses, reads, barrier) = (
            Arc::clone(&stop),
            Arc::clone(&misses),
            Arc::clone(&reads),
            Arc::clone(&barrier),
        );
        readers.push(std::thread::spawn(move || {
            barrier.wait();
            let (mut m, mut r) = (0u64, 0u64);
            while !stop.load(Ordering::Relaxed) {
                if let Ok(None) = t.search(&k) {
                    m += 1;
                }
                r += 1;
            }
            misses.fetch_add(m, Ordering::Relaxed);
            reads.fetch_add(r, Ordering::Relaxed);
        }));
    }

    barrier.wait();
    // Cycle the value length so the leaf is driven over and back under the split threshold many
    // times rather than growing once. 1..=900 bytes: a 4 KB leaf holding ~600 short entries is
    // pushed into a split well before the top of that range.
    const ROUNDS: usize = 600;
    for i in 0..ROUNDS {
        let len = 1 + (i * 37) % 900;
        tree.upsert(target.clone(), vec![(i % 251) as u8; len]).expect("growing upsert");
    }
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().expect("reader");
    }

    let (m, r) = (misses.load(Ordering::Relaxed), reads.load(Ordering::Relaxed));
    println!("D126 split arm (growing upsert): misses={m} reads={r}");
    assert_eq!(m, 0, "the key was absent {m} times in {r} reads across splitting rewrites");
    assert!(r > 0, "the readers never ran; this arm proves nothing");

    let last = 1 + ((ROUNDS - 1) * 37) % 900;
    assert_eq!(
        tree.search(&target).unwrap(),
        Some(vec![((ROUNDS - 1) % 251) as u8; last]),
        "final value after the splitting run"
    );
    // Every pre-populated key must have survived the splits this arm forced.
    for k in 0..PREPOP {
        if key(k) == target {
            continue;
        }
        assert_eq!(
            tree.search(&key(k)).unwrap(),
            Some(val(k as u64)),
            "prepopulated key {k} lost during the splitting rewrites"
        );
    }
}

/// `insert` must NOT have become a replace. The two are different operations and the catalog
/// relies on the distinction: `fork` calls `tree.insert` directly for keys it knows are new, and
/// `write_record_new` does the same for four of them.
#[test]
fn insert_still_does_not_replace() {
    let (_d, tree) = fresh_tree("d126-insert-unchanged");
    let k = key(PREPOP + 1);
    tree.insert(k.clone(), val(1)).unwrap();
    tree.insert(k.clone(), val(2)).unwrap();
    let n = occurrences(&tree, &k);
    assert_eq!(
        n, 2,
        "`insert` left {n} entries for a key written twice. It has always left two -- that is \
         WHY the catalog needs `upsert` -- and a caller that relies on `insert` being cheap \
         because it knows the key is new would now be paying for a replace it does not need."
    );
}

// ==============================================================================================
// THE PRIMARY INDEX. Same defect, same fix, a different subsystem and different key/value types.
// ==============================================================================================

/// **`execution::update` and `execution::insert` open-coded the same pair, on a user table's
/// PRIMARY INDEX.** This is that, with the real types.
///
/// ```text
/// update.rs   if new_rid != rid { primary_index.delete(&pk)?;  primary_index.insert(pk, new_rid)?; }
/// insert.rs   primary_index.delete(&vals[0])?;  ...heap.insert(tuple)?...  primary_index.insert(vals[0], rid)?;
/// ```
///
/// The second is the worse of the two: a whole heap insert sits inside the window. Both are now
/// one `upsert`.
///
/// This arm exists because "the mechanism is the same, so the fix is the same" is an argument, and
/// the argument is cheap to replace with a measurement. `Value`/`RecordId` serialize differently
/// from `Vec<u8>`/`Vec<u8>` and land differently in a leaf, so the control is re-run for them
/// rather than assumed. It must fire, exactly as the control above must.
///
/// ⚠ **What this does NOT show.** It probes the index, not a concurrent `UPDATE`. Two statements
/// cannot reach the executor at once today — the pgwire server serialises whole statements behind
/// `ServerContext::catalog()`, which is the same exclusion that keeps the catalog's window latent
/// — so a two-thread probe at the SQL layer would need that lock bypassed, which is a harness of
/// its own. The window is in the index, the fix is in the index, and that is what is measured.
#[test]
fn the_primary_index_rewrite_is_never_absent_either() {
    use ferrodb::catalog::column::Value;
    use ferrodb::storage::heap_file_manager::RecordId;

    type PrimaryIndex = BPlusTreeManager<Value, RecordId>;

    fn fresh(tag: &str) -> (tempfile::TempDir, Arc<PrimaryIndex>) {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join(format!("{tag}.db")))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let ix = Arc::new(PrimaryIndex::create(bp).expect("create primary index"));
        for k in 0..PREPOP as i32 {
            ix.insert(Value::Integer(k), RecordId { page_id: 1, slot_num: k as u16 })
                .expect("prepop");
        }
        (dir, ix)
    }

    /// A plain `fn` pointer rather than a closure type, so the two arms are the same shape and the
    /// only difference between them is the body.
    fn run(
        ix: &Arc<PrimaryIndex>,
        pk: Value,
        rewrite: fn(&PrimaryIndex, &Value, RecordId),
    ) -> (u64, u64) {
        let stop = Arc::new(AtomicBool::new(false));
        let misses = Arc::new(AtomicU64::new(0));
        let reads = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(READERS + 1));
        let mut hs = Vec::new();
        for _ in 0..READERS {
            let (t, k, stop, misses, reads, barrier) = (
                Arc::clone(ix),
                pk.clone(),
                Arc::clone(&stop),
                Arc::clone(&misses),
                Arc::clone(&reads),
                Arc::clone(&barrier),
            );
            hs.push(std::thread::spawn(move || {
                barrier.wait();
                let (mut m, mut r) = (0u64, 0u64);
                while !stop.load(Ordering::Relaxed) {
                    if t.search(&k).expect("index search").is_none() {
                        m += 1;
                    }
                    r += 1;
                }
                misses.fetch_add(m, Ordering::Relaxed);
                reads.fetch_add(r, Ordering::Relaxed);
            }));
        }
        barrier.wait();
        for i in 0..WRITES {
            rewrite(ix, &pk, RecordId { page_id: 2, slot_num: (i % 60_000) as u16 });
        }
        stop.store(true, Ordering::Relaxed);
        for h in hs {
            h.join().expect("reader thread");
        }
        (misses.load(Ordering::Relaxed), reads.load(Ordering::Relaxed))
    }

    let pk = Value::Integer(PREPOP as i32 / 2);

    // CONTROL — what `update.rs` did. It must fire, or the zero below means nothing.
    let (_d1, ix1) = fresh("pk-control");
    let (cm, cr) = run(&ix1, pk.clone(), |t, k, rid| {
        match t.delete(k) {
            Ok(()) | Err(FerroError::KeyNotFound) => {}
            Err(e) => panic!("control delete: {e:?}"),
        }
        t.insert(k.clone(), rid).expect("control insert");
    });
    println!("D126 primary index CONTROL  (delete+insert): misses={cm} reads={cr}");

    // TREATMENT — what it does now.
    let (_d2, ix2) = fresh("pk-upsert");
    let (tm, tr) = run(&ix2, pk.clone(), |t, k, rid| {
        t.upsert(k.clone(), rid).expect("upsert");
    });
    println!("D126 primary index TREATMENT (upsert)      : misses={tm} reads={tr}");

    assert!(cr > 0 && tr > 0, "a reader never ran: control {cr}, treatment {tr}");
    assert!(
        cm > 0,
        "THE PROBE IS NOT DISCRIMINATING for Value/RecordId. delete-then-insert leaves the key \
         absent between the two calls by construction, and {READERS} readers over {WRITES} \
         rewrites saw it {cm} times in {cr} reads. The treatment's zero means nothing until this \
         fires."
    );
    assert_eq!(tm, 0, "the primary key was absent {tm} times in {tr} reads under `upsert`");
    assert!(
        tr >= cr / 4,
        "the treatment's readers did only {tr} reads against the control's {cr}; its zero is not \
         comparable"
    );
    // And it replaced rather than accumulating: one entry, holding the last value written.
    let last = RecordId { page_id: 2, slot_num: ((WRITES - 1) % 60_000) as u16 };
    assert_eq!(
        ix2.search(&pk).expect("search"),
        Some(last),
        "the last upsert is not readable back"
    );
    let dupes = ix2
        .range_scan(Bound::Included(pk.clone()), Bound::Included(pk.clone()))
        .expect("scan")
        .count();
    assert_eq!(dupes, 1, "`upsert` left {dupes} entries under one primary key");
}
