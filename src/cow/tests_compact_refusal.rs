//! D113: what a *refused* cell insert leaves behind.
//!
//! [`NodeMut::insert_cell_at`] and [`NodeMut::replace_cell_at`] compact the cell heap before they
//! give up, so the refusal path rewrites the payload. Its doc comment used to promise the node was
//! left "untouched", and both `btree` callers then drop the page's write guard on that path and
//! run a **fallible** step — `alloc_for`, once per cut — before anything restamps the checksum.
//!
//! The consequence is not cosmetic. `CowStore::read_page` verifies on every fetch and refuses a
//! page whose checksum disagrees with its bytes, so a failed split poisoned a page that was never
//! logically modified: the rows were all still there and still correct, and reading them returned
//! "refusing to return a torn page".
//!
//! **Which caller an ordinary workload actually reaches.** `internal_relink`'s — its level is
//! still byte-balanced, so internal nodes pack to capacity and refuse. `leaf_put`'s does not: a
//! leaf refuses only above 3046 of its 4060 bytes, and the D89 chunker targets 507, so that is a
//! ~6x tail run. Measured over builds of 2000-8000 keys at three value sizes, leaf occupancy
//! averaged 496-507 and peaked at 2268, and **no** leaf in any build could have refused even a
//! maximum-size entry. The leaf path is pinned at the node level below rather than end to end.
//!
//! Each test states its premises as assertions. A fixture that stops reproducing the setup — an
//! allocation that does not starve, a compaction that moves no bytes, a sweep that never reaches
//! a refusal — fails as a fixture failure rather than passing vacuously. That is not
//! hypothetical: the first end-to-end fixture here scattered its probe inserts and recorded 493
//! starved failures without reaching the refusal path once, and passed.

use std::fs::OpenOptions;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use crate::branch::types::{ArenaId, BranchId, Epoch, PageId};
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::cow::btree::CowTree;
use crate::cow::node::{self, Node, NodeMut};
use crate::cow::page_header::{stamp_checksum, verify_checksum, PageHeader, PageType};
use crate::cow::store::CowStore;
use crate::cow::{CowPage, PageHandle, PageStore};
use crate::error::FerroError;
use crate::storage::disk_manager::{DiskManager, PAGE_SIZE};

// ================================================================================================
// The starving store
// ================================================================================================

/// A [`PageStore`] that forwards everything to a real [`CowStore`] but can be armed to refuse
/// every subsequent **allocation**.
///
/// Only `alloc_in_arena` is intercepted. `alloc_for`'s default body routes through it, so arming
/// this reproduces exactly the exposure the split loops carry: the tree has already mutated a page
/// it holds a guard on, and the next page it asks for does not arrive. `cow_page` is delegated
/// whole and keeps allocating from the inner store, so shadowing a page still works while the
/// split loop starves — the failure lands on the step under test and nowhere else.
struct StarvingStore {
    inner: Arc<CowStore>,
    /// Allocations still permitted, or `-1` for "no limit".
    budget: AtomicI64,
    /// Every allocation this store has served or refused since [`StarvingStore::reset_log`],
    /// in order. `None` is a refusal. D125's instrument reads this to attribute a failure to a
    /// call site without putting a hook in production code: `alloc_for(BTreeLeaf)` is only
    /// reached from `write_leaf_chunked`, and `alloc_for(BTreeInternal)` only from
    /// `internal_relink` or the new-root tail of `relink_up`, so the type sequence says which
    /// stage the operation died in.
    log: Mutex<Vec<(Option<PageId>, PageType)>>,
}

impl StarvingStore {
    fn new(inner: Arc<CowStore>) -> StarvingStore {
        StarvingStore { inner, budget: AtomicI64::new(-1), log: Mutex::new(Vec::new()) }
    }

    fn reset_log(&self) {
        self.log.lock().unwrap().clear();
    }

    fn log_entries(&self) -> Vec<(Option<PageId>, PageType)> {
        self.log.lock().unwrap().clone()
    }

    /// Permit exactly `n` more allocations, then refuse every one after that. Sweeping `n`
    /// upwards walks the failure through the successive allocation sites of a single operation,
    /// which is how the internal-node path is reached: the leaf split below it has to be allowed
    /// to allocate before `internal_relink` is even called.
    fn allow(&self, n: i64) {
        self.budget.store(n, Ordering::SeqCst);
    }

    fn unlimited(&self) {
        self.budget.store(-1, Ordering::SeqCst);
    }
}

impl PageStore for StarvingStore {
    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        if self.budget.load(Ordering::SeqCst) >= 0
            && self.budget.fetch_sub(1, Ordering::SeqCst) <= 0
        {
            self.log.lock().unwrap().push((None, page_type));
            return Err(FerroError::Cow("starving store: allocation refused".into()));
        }
        let id = self.inner.alloc_in_arena(arena, page_type, birth_epoch)?;
        self.log.lock().unwrap().push((Some(id), page_type));
        Ok(id)
    }

    fn read_page(&self, page_id: PageId) -> Result<PageHandle, FerroError> {
        self.inner.read_page(page_id)
    }

    fn cow_page(
        &self,
        page_id: PageId,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<CowPage, FerroError> {
        self.inner.cow_page(page_id, branch, epoch)
    }

    fn free_page(&self, page_id: PageId, free_epoch: Epoch) -> Result<(), FerroError> {
        self.inner.free_page(page_id, free_epoch)
    }

    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        self.inner.alloc_arena(branch)
    }

    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        self.inner.arena_for(branch)
    }

    fn free_arena(&self, arena: ArenaId) -> Result<u32, FerroError> {
        self.inner.free_arena(arena)
    }

    fn live_page_count(&self) -> Result<u32, FerroError> {
        self.inner.live_page_count()
    }

    fn flush(&self) -> Result<(), FerroError> {
        self.inner.flush()
    }
}

// ================================================================================================
// Fixture
// ================================================================================================

struct Fixture {
    _dir: TempDir,
    store: Arc<StarvingStore>,
    inner: Arc<CowStore>,
    tree: CowTree,
    clock: AtomicU64,
}

impl Fixture {
    fn new() -> Fixture {
        let dir = TempDir::new().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("cow.db"))
            .unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let pool = Arc::new(BufferPoolManager::new(dm));
        let inner = Arc::new(CowStore::with_extent_pages(pool, 4096));
        let store = Arc::new(StarvingStore::new(inner.clone()));
        let tree = CowTree::new(store.clone() as Arc<dyn PageStore>);
        Fixture { _dir: dir, store, inner, tree, clock: AtomicU64::new(1) }
    }

    fn tick(&self) -> Epoch {
        Epoch(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    fn insert(&self, root: PageId, k: &[u8], v: &[u8]) -> Result<PageId, FerroError> {
        let e = self.tick();
        self.tree.insert(root, BranchId::TRUNK, e, k, v)
    }

    /// Raw page bytes, read through the buffer pool **without** the checksum gate that
    /// `read_page` applies. The test needs to look at a page the production reader refuses.
    fn raw(&self, id: PageId) -> [u8; PAGE_SIZE] {
        let h = PageHandle::fetch(self.inner.pool().clone(), id).unwrap();
        let f = h.read();
        f.data
    }

    /// Every key the tree can reach from `root`, and every page it reaches to get them.
    ///
    /// Reads pages through [`Fixture::raw`] rather than `read_page` so a tree that has been left
    /// with a bad checksum can still be enumerated — the question here is which rows are
    /// *reachable*, not whether the reader would hand them over.
    fn reachable(&self, root: PageId) -> (BTreeSet<Vec<u8>>, BTreeSet<PageId>) {
        let pages = self.leaves_and_internals(root);
        let mut keys = BTreeSet::new();
        for p in &pages {
            let page = self.raw(*p);
            if PageHeader::read_from(&page).unwrap().page_type != PageType::BTreeLeaf {
                continue;
            }
            for (k, _) in Node::new(&page).leaf_entries().unwrap() {
                keys.insert(k);
            }
        }
        (keys, pages.into_iter().collect())
    }

    /// Every key in the subtree rooted at `page_id`, whether or not anything points at it.
    ///
    /// Recursive, because an orphaned **internal** node strands every leaf beneath it, and those
    /// leaves are pages that existed long before the call that stranded them.
    fn keys_under(&self, page_id: PageId, depth: usize, out: &mut BTreeSet<Vec<u8>>) {
        if depth > 64 {
            return;
        }
        let page = self.raw(page_id);
        match PageHeader::read_from(&page) {
            Ok(h) if h.page_type == PageType::BTreeLeaf => {
                if let Ok(entries) = Node::new(&page).leaf_entries() {
                    out.extend(entries.into_iter().map(|(k, _)| k));
                }
            }
            Ok(h) if h.page_type == PageType::BTreeInternal => {
                if let Ok(kids) = Node::new(&page).all_children() {
                    for c in kids {
                        self.keys_under(c, depth + 1, out);
                    }
                }
            }
            _ => {}
        }
    }

    /// Every leaf page id, left to right.
    fn leaves(&self, root: PageId) -> Vec<PageId> {
        let mut out = Vec::new();
        self.visit(root, 0, false, &mut out);
        out
    }

    /// Every page the tree can reach, internal nodes included. The internal nodes are the point:
    /// they are where a starved split leaves a compacted page behind.
    fn leaves_and_internals(&self, root: PageId) -> Vec<PageId> {
        let mut out = Vec::new();
        self.visit(root, 0, true, &mut out);
        out
    }

    fn visit(&self, pid: PageId, depth: usize, internals: bool, out: &mut Vec<PageId>) {
        assert!(depth < 64, "descent guard");
        let page = self.raw(pid);
        let ty = PageHeader::read_from(&page).unwrap().page_type;
        match ty {
            PageType::BTreeLeaf => out.push(pid),
            PageType::BTreeInternal => {
                if internals {
                    out.push(pid);
                }
                for c in Node::new(&page).all_children().unwrap() {
                    self.visit(c, depth + 1, internals, out);
                }
            }
            other => panic!("page {} is a {:?}", pid, other),
        }
    }
}

fn key_of(i: u32) -> Vec<u8> {
    format!("key{:06}", i).into_bytes()
}

/// A long key, so that a **separator** is large and an internal node holds only ~19 of them.
///
/// This is the lever that makes `internal_relink`'s refusal path reachable in a test of sensible
/// size. With six-byte keys an internal node takes ~176 separators and overflows once per ~176
/// leaf splits, which a few hundred probe inserts essentially never reach — measured: 75 starved
/// failures, not one of them a refusal. At ~200 bytes it overflows every ~19.
fn long_key_of(i: u32) -> Vec<u8> {
    let mut k = format!("key{:06}", i).into_bytes();
    k.resize(400, b'k');
    k
}

/// A key that sorts strictly **between** `long_key_of(base)` and `long_key_of(base + 1)`.
///
/// Every probe insert goes into one gap, so every leaf split under it promotes into the same
/// parent and that parent fills in ~9 promotions. Scattering the probes instead leaves the
/// refusal to chance: measured over 400 scattered inserts, 493 starved failures and not one of
/// them a refusal, because a promotion has to arrive at a node that happens to be full.
fn gap_key_of(base: u32, j: u32) -> Vec<u8> {
    assert!(j < 14 * 14 * 14, "gap exhausted");
    let mut k = long_key_of(base);
    let n = k.len();
    // Three trailing bytes drawn from 'm'..'z', all above the 'k' the key is padded with, so the
    // result exceeds `base` and still differs from `base + 1` at the digit they disagree on.
    k[n - 3] = b'm' + (j / 196 % 14) as u8;
    k[n - 2] = b'm' + (j / 14 % 14) as u8;
    k[n - 1] = b'm' + (j % 14) as u8;
    k
}

/// Is this page's **logical** content untouched while its bytes moved? That is compaction's
/// signature, and it is what distinguishes a page the refusal path rewrote from one an ordinary
/// successful write changed.
fn compacted_in_place(before: &[u8; PAGE_SIZE], after: &[u8; PAGE_SIZE]) -> bool {
    let diff = before.iter().zip(after.iter()).filter(|(a, b)| a != b).count();
    if diff == 0 {
        return false;
    }
    let (b, a) = (Node::new(before), Node::new(after));
    let keys_before: Option<Vec<Vec<u8>>> =
        (0..b.count()).map(|i| b.key(i).ok().map(|k| k.to_vec())).collect();
    let keys_after: Option<Vec<Vec<u8>>> =
        (0..a.count()).map(|i| a.key(i).ok().map(|k| k.to_vec())).collect();
    // The slot array still names the same keys in the same order, yet more bytes than a child
    // pointer moved: the cell heap was rebuilt underneath it.
    keys_before.is_some() && keys_before == keys_after && diff > 8
}

// ================================================================================================
// The end-to-end falsifier: a starved allocation must not poison a page it only touched
// ================================================================================================

/// **The falsifier.** Walk a starved allocation through the successive allocation sites of a real
/// insert and assert, after every failure, that every page in the tree still verifies.
///
/// The budget sweep is what reaches the defect. `internal_relink` is only called once the leaf
/// split beneath it has already allocated, so refusing *every* allocation only ever exercises the
/// leaf path — which is careful to allocate before it overwrites and is therefore clean. Allowing
/// `b` allocations and refusing the rest, for b = 0, 1, 2, …, walks the failure down the operation
/// until it lands inside `internal_relink`, after that node's child pointer has moved and after a
/// refused separator insert has compacted it.
///
/// **Why the leaf path is not the one driven end-to-end here.** Measured on this fixture before
/// the sweep was written: over builds of 2000–8000 keys with 16-, 60- and 300-byte values, leaf
/// occupancy averaged 496–507 bytes of the 4060-byte capacity and peaked at 2268, and **no** leaf
/// in any of those builds could have refused even a maximum-size entry (that needs 3046). The
/// content-defined chunker targets `NODE_CAPACITY >> 3`, so a leaf that refuses an insert is a
/// ~6x tail run. `leaf_put`'s exposure is real in the code and is pinned by the node-level tests
/// below; it is `internal_relink`, whose level is still byte-balanced and does pack to capacity,
/// that an ordinary workload actually reaches.
#[test]
fn a_starved_allocation_never_leaves_an_unverifiable_page() {
    let f = Fixture::new();
    let mut root = f.tree.create(BranchId::TRUNK, f.tick()).unwrap();
    let value = vec![b'v'; 8];

    // Long keys, so separators are large and internal nodes hold only ~9. See `long_key_of`.
    for i in 0..600u32 {
        root = f.insert(root, &long_key_of(i), &value).unwrap();
    }
    assert!(f.leaves(root).len() > 200, "fixture: expected a deep tree");

    let mut starved_failures = 0usize;
    let mut mutating_failures = 0usize;
    let mut compaction_failures = 0usize;

    for j in 0..220u32 {
        let key = gap_key_of(300, j);
        let mut allowance = 0i64;
        loop {
            // Snapshot every page the tree can reach, so a failure can be checked against the
            // exact bytes that preceded it.
            let ids = f.leaves_and_internals(root);
            let before: Vec<(PageId, [u8; PAGE_SIZE])> =
                ids.iter().map(|&id| (id, f.raw(id))).collect();
            for (id, page) in &before {
                assert!(verify_checksum(page), "fixture: page {} was already invalid", id);
            }

            f.store.allow(allowance);
            let outcome = f.insert(root, &key, &value);
            f.store.unlimited();

            match outcome {
                Ok(new_root) => {
                    root = new_root;
                    break;
                }
                Err(e) => {
                    let msg = format!("{}", e);
                    assert!(
                        msg.contains("starving store"),
                        "fixture: the insert failed for the wrong reason: {}",
                        msg
                    );
                    starved_failures += 1;

                    // Did this failure leave anything rewritten? Those are the failures that can
                    // expose the defect; a failure that mutated nothing proves nothing.
                    let changed: usize = before
                        .iter()
                        .map(|(id, page)| {
                            let now = f.raw(*id);
                            page.iter().zip(now.iter()).filter(|(a, b)| a != b).count()
                        })
                        .sum();
                    if changed > 0 {
                        mutating_failures += 1;
                    }
                    if before.iter().any(|(id, page)| compacted_in_place(page, &f.raw(*id))) {
                        compaction_failures += 1;
                    }

                    // The conclusion: a failed allocation may abandon the operation, but it may
                    // not leave a page that the production reader refuses.
                    for (id, _) in &before {
                        let now = f.raw(*id);
                        assert!(
                            verify_checksum(&now),
                            "D113: after a starved allocation (key {:?}, {} allowed) page {} has \
                             a checksum that disagrees with its bytes; {} bytes were rewritten \
                             across the tree and `read_page` now refuses this page as torn",
                            String::from_utf8_lossy(&key[..12.min(key.len())]),
                            allowance,
                            id,
                            changed
                        );
                        f.store.read_page(*id).unwrap_or_else(|e| {
                            panic!("D113: page {} is no longer readable: {}", id, e)
                        });
                    }

                    allowance += 1;
                    assert!(allowance < 64, "fixture: insert never succeeded within 64 allocations");
                }
            }
        }
    }

    println!(
        "starved failures: {} ({} rewrote page bytes, {} left a page compacted in place)",
        starved_failures, mutating_failures, compaction_failures
    );

    // Premises. A sweep that never starved anything, or never starved anything that had already
    // rewritten a page's cell heap, cannot tell a fixed tree from a broken one — and a sweep that
    // only ever starved the clean paths is the vacuous green this fixture was rebuilt to avoid.
    assert!(starved_failures > 0, "fixture: no allocation was ever starved");
    assert!(
        mutating_failures > 0,
        "fixture: {} allocations were starved but none had written to a page first",
        starved_failures
    );
    assert!(
        compaction_failures > 0,
        "fixture: {} starved failures, {} of which rewrote bytes, but NONE left a page whose \
         cell heap had been rebuilt with its key list unchanged. The refusal path under test was \
         never reached, so this test proves nothing about it",
        starved_failures,
        mutating_failures
    );

    // Deliberately NOT asserted here: that the tree still holds every row. A starved insert
    // leaves whatever it had already applied applied — on the trunk every page is private, so
    // `cow_page` mutates in place and there is nothing to roll back to. That is a separate
    // question from this one, with its own test below; folding it in here would make a D113
    // regression and an unrelated atomicity gap fail the same assertion.
}

/// D125. **This test used to fail, and was `#[ignore]`d with its numbers in this comment so the
/// finding would survive.** It gates now; the history is kept because the numbers are the
/// evidence.
///
/// Measured at `d6771d8` and reproduced unchanged at `dc2a2cf`, both arms inserting the
/// identical 820 keys:
///
/// ```text
/// rows missing -- control: 0 base,  0 gap
///                 starved: 28 base, 165 gap
/// ```
///
/// The control is what makes that attributable. The alarming half is the gap figure: every one
/// of those 220 keys was *eventually* inserted by a later call that returned `Ok`, and 165 were
/// still missing afterwards — silent loss on the write path, not a refusal.
///
/// ⚠ **The mechanism this comment used to give was wrong, and is corrected here rather than
/// quietly dropped.** It read: "leaving a leaf truncated to its first piece while the other
/// pieces sit allocated with nothing pointing at them", blaming a split that fails part-way.
/// `d125_instrument_where_a_starved_insert_loses_rows` falsified that: of 300 starved failures,
/// the 203 refused a *leaf* page lost **zero** rows, because `write_leaf_chunked` allocates
/// every piece before it overwrites the first. All 97 losing failures were refused an
/// *internal* page, after the leaf split had already completed and committed. The leaf really
/// is left truncated and the pieces really are orphaned — but by a split that **succeeded**,
/// with the relink above it failing afterwards and nothing rolling it back.
///
/// The premise underneath was right, and is the whole defect: on the trunk every page is
/// private, so `cow_page` mutates in place and an operation has nothing to roll back to. The
/// same fixture with every page shadowed loses 0 and 0 against the same refusals.
///
/// Same family as D112's "free before the parent stops pointing at it": a correct happy path
/// with a broken error path. Fixed by [`WriteJournal`]; `bench/d125_starved_insert.txt`.
#[test]
fn a_starved_insert_loses_no_row_that_the_unstarved_control_keeps() {
    let probe_rows = |starve: bool| -> (usize, usize, usize) {
        let f = Fixture::new();
        let mut root = f.tree.create(BranchId::TRUNK, f.tick()).unwrap();
        let value = vec![b'v'; 8];
        for i in 0..600u32 {
            root = f.insert(root, &long_key_of(i), &value).unwrap();
        }
        let mut failures = 0usize;
        for j in 0..220u32 {
            let key = gap_key_of(300, j);
            if starve {
                // One starved attempt per key, then let it through, so both arms end up having
                // inserted exactly the same 220 keys.
                let mut allowance = 0i64;
                loop {
                    f.store.allow(allowance);
                    let outcome = f.insert(root, &key, &value);
                    f.store.unlimited();
                    match outcome {
                        Ok(new_root) => {
                            root = new_root;
                            break;
                        }
                        Err(_) => {
                            failures += 1;
                            allowance += 1;
                            assert!(allowance < 64, "fixture: never succeeded");
                        }
                    }
                }
            } else {
                root = f.insert(root, &key, &value).unwrap();
            }
        }
        let base_lost =
            (0..600u32).filter(|&i| f.tree.get(root, &long_key_of(i)).unwrap().is_none()).count();
        let gap_lost = (0..220u32)
            .filter(|&j| f.tree.get(root, &gap_key_of(300, j)).unwrap().is_none())
            .count();
        (base_lost, gap_lost, failures)
    };

    let (control_base, control_gap, control_failures) = probe_rows(false);
    let (starved_base, starved_gap, starved_failures) = probe_rows(true);
    println!(
        "rows missing -- control: {} base, {} gap;  starved: {} base, {} gap  \
         ({} starved failures)",
        control_base, control_gap, starved_base, starved_gap, starved_failures
    );

    // ⚠ The premise this test did NOT state while it was `#[ignore]`d, and the one that would
    // let it pass for the wrong reason now that it gates: a run in which nothing was ever
    // refused an allocation loses no rows trivially. Zero starved failures is not a pass.
    assert_eq!(control_failures, 0, "fixture: the unstarved control was starved");
    assert!(
        starved_failures > 0,
        "fixture: not one allocation was refused in the starved arm, so it is the control run \
         twice and proves nothing"
    );

    // The control is the premise: plain inserts of this sequence must lose nothing, or the
    // fixture is broken and the starved arm's losses are not attributable to the starvation.
    assert_eq!(
        (control_base, control_gap),
        (0, 0),
        "fixture: the unstarved control lost rows, so nothing here is attributable to starvation"
    );
    assert_eq!(
        (starved_base, starved_gap),
        (0, 0),
        "an insert that failed on a starved allocation lost {} of 600 base rows and {} of 220 \
         gap rows that a later successful insert of the same key did not restore; the identical \
         sequence without starvation loses none",
        starved_base,
        starved_gap
    );
}

// ================================================================================================
// D125 instrument: WHERE the rows go when a starved insert loses them
// ================================================================================================

/// The instrument behind D125, kept because the number without it is a mechanism-free fact.
///
/// It runs the probe's own sequence and, on **every** failure that loses a row, records three
/// things that together separate the two candidate mechanisms:
///
/// * the [`PageType`] of the **refused** allocation. `alloc_for(BTreeLeaf)` is reached only from
///   `write_leaf_chunked`; `alloc_for(BTreeInternal)` only from `internal_relink` or the
///   new-root tail of `relink_up`. So the type says which stage died.
/// * how many pages of each type the call had already been *served* before it was refused —
///   i.e. whether the leaf split had completed.
/// * for each lost key, whether it now sits on a page **allocated during this very call** and
///   unreachable from the root (a fresh split piece nobody points at), or has vanished from a
///   page that was there before.
///
/// Pre-fix, at `dc2a2cf`, it printed:
///
/// ```text
/// 300 starved failures, 97 of them lost rows (235 rows total).
/// refused allocation was a leaf page in 203 failures (0 of them losing),
///                             an internal page in  97 failures (97 losing).
/// leaf split had already completed in 97 of the 97 losing failures.
/// lost rows: 235 into a subtree the failing call ALLOCATED and left unreachable,
///              0 into a PRE-EXISTING subtree it detached, 0 unaccounted for.
/// ```
///
/// which is what falsified the hypothesis this row opened with — the leaf split, the stage it
/// blamed, is the one stage that never lost a row.
///
/// It now **gates**, because the table it prints is exactly the postcondition
/// [`WriteJournal`] has to establish: the same refusals still happen, and none of them loses
/// anything. The failure count falls from 300 to 220 with the fix in and that is the fix
/// working — a failed attempt no longer damages the tree, so the budget sweep no longer needs
/// extra rounds to get past its own wreckage.
#[test]
fn d125_instrument_where_a_starved_insert_loses_rows() {
    let f = Fixture::new();
    let mut root = f.tree.create(BranchId::TRUNK, f.tick()).unwrap();
    let value = vec![b'v'; 8];
    for i in 0..600u32 {
        root = f.insert(root, &long_key_of(i), &value).unwrap();
    }

    let mut failures = 0usize;
    let mut losing = 0usize;
    let mut refused_leaf = 0usize;
    let mut refused_internal = 0usize;
    let mut losing_refused_leaf = 0usize;
    let mut losing_refused_internal = 0usize;
    let mut lost_total = 0usize;
    let mut lost_on_fresh_orphan = 0usize;
    let mut lost_on_detached = 0usize;
    let mut lost_elsewhere = 0usize;
    let mut leaf_split_completed = 0usize;
    let mut printed = 0usize;

    for j in 0..220u32 {
        let key = gap_key_of(300, j);
        let mut allowance = 0i64;
        loop {
            let (before_keys, before_pages) = f.reachable(root);
            f.store.reset_log();
            f.store.allow(allowance);
            let outcome = f.insert(root, &key, &value);
            f.store.unlimited();
            let entries = f.store.log_entries();
            match outcome {
                Ok(new_root) => {
                    root = new_root;
                    break;
                }
                Err(_) => {
                    failures += 1;
                    let refused = entries.iter().find(|(id, _)| id.is_none()).map(|(_, t)| *t);
                    let served_leaf = entries
                        .iter()
                        .filter(|(id, t)| id.is_some() && *t == PageType::BTreeLeaf)
                        .count();
                    let served_internal = entries
                        .iter()
                        .filter(|(id, t)| id.is_some() && *t == PageType::BTreeInternal)
                        .count();
                    match refused {
                        Some(PageType::BTreeLeaf) => refused_leaf += 1,
                        Some(PageType::BTreeInternal) => refused_internal += 1,
                        other => panic!("unexpected refusal of {:?}", other),
                    }
                    let (after_keys, after_pages) = f.reachable(root);
                    let lost: Vec<Vec<u8>> =
                        before_keys.difference(&after_keys).cloned().collect();
                    if !lost.is_empty() {
                        losing += 1;
                        lost_total += lost.len();
                        match refused {
                            Some(PageType::BTreeLeaf) => losing_refused_leaf += 1,
                            Some(PageType::BTreeInternal) => losing_refused_internal += 1,
                            _ => unreachable!(),
                        }
                        // Did the leaf split finish before the call died? It allocates one page
                        // per cut past the first, so "served at least one leaf and was not
                        // refused a leaf" means the cut loop ran to completion.
                        if refused != Some(PageType::BTreeLeaf) && served_leaf > 0 {
                            leaf_split_completed += 1;
                        }
                        // Pages this call allocated and that nothing points at afterwards.
                        let fresh_orphans: Vec<PageId> = entries
                            .iter()
                            .filter_map(|(id, _)| *id)
                            .filter(|id| !after_pages.contains(id))
                            .collect();
                        let mut on_fresh = BTreeSet::new();
                        for p in &fresh_orphans {
                            f.keys_under(*p, 0, &mut on_fresh);
                        }
                        // Pages that WERE reachable before this call and are not now: a
                        // pre-existing subtree the failure detached.
                        let detached: Vec<PageId> =
                            before_pages.difference(&after_pages).copied().collect();
                        let mut on_detached = BTreeSet::new();
                        for p in &detached {
                            f.keys_under(*p, 0, &mut on_detached);
                        }
                        let here = lost.iter().filter(|k| on_fresh.contains(*k)).count();
                        let there = lost
                            .iter()
                            .filter(|k| !on_fresh.contains(*k) && on_detached.contains(*k))
                            .count();
                        lost_on_fresh_orphan += here;
                        lost_on_detached += there;
                        lost_elsewhere += lost.len() - here - there;
                        if printed < 8 {
                            printed += 1;
                            println!(
                                "  loss #{}: budget={} refused={:?} served(leaf={},int={}) \
                                 lost={} [fresh-orphan={} detached-preexisting={} other={}] \
                                 fresh_orphan_pages={} detached_pages={} \
                                 pages_before={} pages_after={}",
                                losing,
                                allowance,
                                refused.unwrap(),
                                served_leaf,
                                served_internal,
                                lost.len(),
                                here,
                                there,
                                lost.len() - here - there,
                                fresh_orphans.len(),
                                detached.len(),
                                before_pages.len(),
                                after_pages.len(),
                            );
                        }
                    }
                    allowance += 1;
                    assert!(allowance < 64, "fixture: never succeeded");
                }
            }
        }
    }

    println!(
        "D125 instrument: {} starved failures, {} of them lost rows ({} rows total).\n\
         refused allocation was a leaf page in {} failures ({} of them losing), an internal \
         page in {} ({} losing).\n\
         leaf split had already completed in {} of the {} losing failures.\n\
         lost rows by where they went: {} into a subtree the failing call ALLOCATED and left \
         unreachable, {} into a PRE-EXISTING subtree the failing call detached, {} unaccounted \
         for by either.",
        failures,
        losing,
        lost_total,
        refused_leaf,
        losing_refused_leaf,
        refused_internal,
        losing_refused_internal,
        leaf_split_completed,
        losing,
        lost_on_fresh_orphan,
        lost_on_detached,
        lost_elsewhere,
    );

    // Premises, in the order they can go vacuous. A sweep that never starved anything proves
    // nothing; one that starved only the leaf path proves nothing either, because the leaf path
    // was never the broken one — 203 of the 300 pre-fix failures were refused a leaf page and
    // all 203 were harmless. The stage that has to be reached is the internal-page refusal.
    assert!(failures > 0, "fixture: nothing was ever starved");
    assert!(
        refused_internal > 0,
        "fixture: {} starved failures, but not one of them was refused an INTERNAL page. That \
         is the only stage that ever lost a row (97 of 97 pre-fix), so this run exercised the \
         clean path only and proves nothing about the defect",
        failures
    );

    // The outcome. Stated over every failure, not just the probe's end state: a row that
    // disappears and is put back by a later insert of the same key would not show up in a
    // final count.
    assert_eq!(
        (losing, lost_total),
        (0, 0),
        "{} starved failures lost {} rows between them. Every failure must leave the tree \
         exactly as it found it; see `WriteJournal`",
        losing,
        lost_total
    );
}

/// The second half of D125's instrument: the **same** starvation on a tree whose pages are not
/// private, which is the arm that pins the mechanism to in-place mutation.
///
/// Registering a child branch that forked *after* the build makes every page of that build fail
/// `CowStore::privacy`, so `cow_page` shadows instead of handing the page back for in-place
/// mutation. Nothing else about the fixture changes: same 600 + 220 keys, same budget sweep,
/// same allocator refusals in the same places.
///
/// If the trunk's losses were caused by anything other than mutating a page the old root still
/// points at, this arm would lose rows too.
/// It gates as well as explains. The shadow path is the arm [`WriteJournal::record`]
/// deliberately does **not** record (`copied == true` leaves the old root's tree untouched, so
/// there is nothing to take back), which makes it the one path whose safety rests on the store
/// rather than on the journal. Nothing else in the suite pins it.
#[test]
fn d125_instrument_the_same_starvation_on_shadowed_pages() {
    let f = Fixture::new();
    let mut root = f.tree.create(BranchId::TRUNK, f.tick()).unwrap();
    let value = vec![b'v'; 8];
    for i in 0..600u32 {
        root = f.insert(root, &long_key_of(i), &value).unwrap();
    }

    // A live child forked immediately before every attempt below. `CowStore::privacy` refuses
    // in-place mutation of a page born before a live child's fork epoch, so this makes every
    // page the build produced non-private at the moment it is touched. One fork is not enough:
    // a shadow is born at the *current* epoch, so it is private again on the next write, which
    // is why an earlier cut of this arm shadowed 1 insert of 220 and reproduced the loss exactly.
    let mut next_branch = 1u64;

    let mut failures = 0usize;
    let mut losing = 0usize;
    let mut lost_total = 0usize;
    let mut shadowed = 0usize;
    for j in 0..220u32 {
        let key = gap_key_of(300, j);
        let mut allowance = 0i64;
        loop {
            let (before_keys, _) = f.reachable(root);
            f.inner
                .register_branch(
                    BranchId { id: next_branch, generation: 0 },
                    Some(BranchId::TRUNK),
                    f.tick(),
                )
                .unwrap();
            next_branch += 1;
            f.store.reset_log();
            f.store.allow(allowance);
            let outcome = f.insert(root, &key, &value);
            f.store.unlimited();
            match outcome {
                Ok(new_root) => {
                    if new_root != root {
                        shadowed += 1;
                    }
                    root = new_root;
                    break;
                }
                Err(_) => {
                    failures += 1;
                    let (after_keys, _) = f.reachable(root);
                    let lost = before_keys.difference(&after_keys).count();
                    if lost > 0 {
                        losing += 1;
                        lost_total += lost;
                    }
                    allowance += 1;
                    assert!(allowance < 64, "fixture: never succeeded");
                }
            }
        }
    }
    let base_lost =
        (0..600u32).filter(|&i| f.tree.get(root, &long_key_of(i)).unwrap().is_none()).count();
    let gap_lost =
        (0..220u32).filter(|&j| f.tree.get(root, &gap_key_of(300, j)).unwrap().is_none()).count();
    println!(
        "D125 shadowed arm: {} starved failures, {} of them lost rows ({} rows); \
         end state: {} of 600 base and {} of 220 gap rows missing; \
         {} of 220 inserts moved the root (i.e. actually shadowed).",
        failures, losing, lost_total, base_lost, gap_lost, shadowed,
    );

    // Premises: this arm has to reach the same failures, and it has to actually be shadowing.
    // The second one is not decoration — the first cut of this test registered a single child
    // before the loop, shadowed 1 insert of 220, and reproduced the trunk's 28/165 exactly
    // while reading as a confirming result.
    assert!(failures > 0, "fixture: nothing was ever starved in the shadowed arm");
    assert_eq!(
        shadowed, 220,
        "fixture: only {} of 220 inserts shadowed, so this is not the shadowed arm it claims \
         to be — a shadow page is born at the current epoch and is private again on the next \
         write, so every attempt needs its own live child",
        shadowed
    );
    assert_eq!(
        (losing, lost_total, base_lost, gap_lost),
        (0, 0, 0, 0),
        "a starved write on shadowed pages lost rows: {} failures lost {} rows, end state {} \
         base and {} gap missing",
        losing,
        lost_total,
        base_lost,
        gap_lost
    );
}

// ================================================================================================
// The node-level postcondition the fix establishes
// ================================================================================================

fn blank_leaf() -> [u8; PAGE_SIZE] {
    let mut page = [0u8; PAGE_SIZE];
    NodeMut::new(&mut page).init();
    PageHeader::new(Epoch(1), ArenaId(0), PageType::BTreeLeaf).write_to(&mut page);
    stamp_checksum(&mut page);
    page
}

/// Fill a leaf with `value_len`-byte values until it refuses one, returning how many landed.
fn fill_to_refusal(page: &mut [u8; PAGE_SIZE], value_len: usize) -> usize {
    let mut i = 0u32;
    loop {
        let cell = node::leaf_cell(&key_of(i), &vec![b'x'; value_len]);
        let ok = {
            let mut n = NodeMut::new(page);
            let at = n.count();
            n.insert_cell_at(at, &cell).unwrap()
        };
        if !ok {
            return i as usize;
        }
        i += 1;
        assert!(i < 10_000, "leaf never filled");
    }
}

/// Leave real garbage in a leaf's cell heap: remove entries from the **middle**, which only shifts
/// slots and abandons the cells. Returns the page, stamped and valid.
fn leaf_with_garbage() -> [u8; PAGE_SIZE] {
    let mut page = blank_leaf();
    let placed = fill_to_refusal(&mut page, 40);
    assert!(placed > 10, "fixture: too few entries ({})", placed);
    for _ in 0..6 {
        NodeMut::new(&mut page).remove_at(3).unwrap();
    }
    stamp_checksum(&mut page);
    assert!(verify_checksum(&page), "fixture page must start valid");
    page
}

/// A refused `insert_cell_at` must leave a page that verifies. It compacts before it refuses, so
/// "leaves the node untouched" was never available — the reachable guarantee is that whatever it
/// leaves behind is self-consistent.
#[test]
fn a_refused_insert_leaves_a_page_that_verifies() {
    let mut page = leaf_with_garbage();
    let before = page;
    let entries_before = Node::new(&page).leaf_entries().unwrap();

    // Too large to fit even after the garbage is reclaimed.
    let big = node::leaf_cell(b"zzzzzz", &vec![b'Z'; node::MAX_ENTRY_BYTES - 32]);
    let refused = {
        let mut n = NodeMut::new(&mut page);
        let at = n.count();
        !n.insert_cell_at(at, &big).unwrap()
    };
    assert!(refused, "fixture: the insert had to be refused");

    let changed = before.iter().zip(page.iter()).filter(|(a, b)| a != b).count();
    assert!(
        changed > 0,
        "fixture: the refusal moved no bytes, so compaction did not run and this test would pass \
         whether or not the defect is present"
    );
    assert_eq!(
        entries_before,
        Node::new(&page).leaf_entries().unwrap(),
        "a refused insert changed the node's logical contents"
    );
    println!("refused insert rewrote {} bytes", changed);
    assert!(
        verify_checksum(&page),
        "D113: a refused insert_cell_at rewrote {} bytes (compaction) and left the checksum stale",
        changed
    );
}

/// The same postcondition for `replace_cell_at`, which has the identical shape and was missed for
/// the identical reason.
#[test]
fn a_refused_replace_leaves_a_page_that_verifies() {
    let mut page = leaf_with_garbage();
    let before = page;
    let entries_before = Node::new(&page).leaf_entries().unwrap();

    let big = node::leaf_cell(&entries_before[0].0, &vec![b'Z'; node::MAX_ENTRY_BYTES - 32]);
    let refused = {
        let mut n = NodeMut::new(&mut page);
        !n.replace_cell_at(0, &big).unwrap()
    };
    assert!(refused, "fixture: the replace had to be refused");

    let changed = before.iter().zip(page.iter()).filter(|(a, b)| a != b).count();
    assert!(changed > 0, "fixture: the refusal moved no bytes, so compaction did not run");
    assert_eq!(
        entries_before,
        Node::new(&page).leaf_entries().unwrap(),
        "a refused replace changed the node's logical contents"
    );
    println!("refused replace rewrote {} bytes", changed);
    assert!(
        verify_checksum(&page),
        "D113: a refused replace_cell_at rewrote {} bytes (compaction) and left the checksum stale",
        changed
    );
}

/// `internal_relink`'s partial-promotion path, in shape: place separators until one is refused,
/// then look at the page the way the code does when it drops the guard.
///
/// This is the case that makes the node-level postcondition load-bearing rather than incidental,
/// and it is why this probe and the two above are **not** one bug. Here the separators that did
/// land mutated the page and nothing stamped after them — a different cause from a compaction on
/// the refusal path. The two share a fix only because the loop's single exit is a refusal, and a
/// refusal now restamps, which covers the earlier successful placements as a side effect.
///
/// That is a structural argument, not a coincidence, and it is the reason there is no second
/// `stamp_checksum` at the call site: one authority, in `restamp_after_refusal`.
#[test]
fn a_partial_promotion_leaves_a_page_that_verifies() {
    let mut page = [0u8; PAGE_SIZE];
    NodeMut::new(&mut page).init();
    PageHeader::new(Epoch(1), ArenaId(0), PageType::BTreeInternal).write_to(&mut page);
    NodeMut::new(&mut page).set_leftmost(7);

    let mut i = 0u32;
    loop {
        let cell = node::internal_cell(&format!("s{:08}", i).into_bytes(), 1000 + i);
        let ok = {
            let mut n = NodeMut::new(&mut page);
            let at = n.count();
            n.insert_cell_at(at, &cell).unwrap()
        };
        if !ok {
            break;
        }
        i += 1;
        assert!(i < 10_000, "internal node never filled");
    }
    // Free space for one long separator, not three.
    for _ in 0..3 {
        NodeMut::new(&mut page).remove_at(0).unwrap();
    }
    stamp_checksum(&mut page);
    assert!(verify_checksum(&page), "fixture must start valid");

    let promoted: Vec<(Vec<u8>, PageId)> = (0..3u32)
        .map(|j| (format!("s00000000{}", "z".repeat(40 + j as usize)).into_bytes(), 9000 + j))
        .collect();

    // `btree::internal_relink`'s loop, verbatim in shape.
    let mut placed = 0usize;
    {
        let mut n = NodeMut::new(&mut page);
        n.set_leftmost(77);
        for (j, (sep, right)) in promoted.iter().enumerate() {
            if !n.insert_cell_at(j, &node::internal_cell(sep, *right)).unwrap() {
                break;
            }
            placed = j + 1;
        }
    }
    // The guard drops here, and `alloc_for` runs before anything restamps.
    assert!(
        placed < promoted.len(),
        "fixture: all {} separators fit, so the partial-promotion path was never taken",
        promoted.len()
    );
    println!("partial promotion placed {}/{} separators", placed, promoted.len());
    assert!(
        verify_checksum(&page),
        "D113: {} of {} separators landed and the leftmost pointer moved, then the page was left \
         with a stale checksum across `alloc_for`",
        placed,
        promoted.len()
    );
}



