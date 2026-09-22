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
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

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
}

impl StarvingStore {
    fn new(inner: Arc<CowStore>) -> StarvingStore {
        StarvingStore { inner, budget: AtomicI64::new(-1) }
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
            return Err(FerroError::Cow("starving store: allocation refused".into()));
        }
        self.inner.alloc_in_arena(arena, page_type, birth_epoch)
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
                            String::from_utf8_lossy(&key),
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

/// Does an aborted insert lose rows, and if so is it the abort that loses them?
///
/// Split out from the D113 test because it asks a different question, and stated as a comparison
/// rather than a bare assertion because a bare "rows are lost" proves nothing about the cause:
/// the identical insert sequence run with the budget never armed is the control, and only the
/// difference between the two arms is attributable to the starvation.
#[test]
fn a_starved_insert_loses_no_row_that_the_unstarved_control_keeps() {
    let probe_rows = |starve: bool| -> (usize, usize) {
        let f = Fixture::new();
        let mut root = f.tree.create(BranchId::TRUNK, f.tick()).unwrap();
        let value = vec![b'v'; 8];
        for i in 0..600u32 {
            root = f.insert(root, &long_key_of(i), &value).unwrap();
        }
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
        (base_lost, gap_lost)
    };

    let (control_base, control_gap) = probe_rows(false);
    let (starved_base, starved_gap) = probe_rows(true);
    println!(
        "rows missing -- control: {} base, {} gap;  starved: {} base, {} gap",
        control_base, control_gap, starved_base, starved_gap
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



