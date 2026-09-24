//! D103 — `AgentRuntime::page_changeset` descends the two roots together, and it is really that
//! descent.
//!
//! ⚠ D193 (2026-09-23): the first line of this file used to read "the production `DIFF` path
//! descends the two roots together". `page_changeset` is NOT the production `DIFF` path and never
//! was: `DIFF <branch>` runs `AgentRuntime::diff`, which builds the changeset from the workspace's
//! touched-rows map and descends no page tree, and `page_changeset` has no caller in `src/`. The
//! file and test names keep the word "production" only because renaming them would change the
//! suite's target and test lists; read it as "the page-derived changeset". What these tests prove
//! is true of `page_changeset`; none of it is a statement about the cost of a `DIFF` statement.
//!
//! `cow::diff` had **zero external callers** before this row: a correct mechanism sitting beside a
//! database that did not use it. `AgentRuntime::page_changeset` now calls it (and `page_changeset`
//! itself has no production caller — see above). These tests exist to make three separate claims
//! falsifiable, because each of them can look true for a wrong reason:
//!
//! 1. **The wiring inside `page_changeset` is real.**
//!    `a_production_diff_reads_pages_in_proportion_to_depth` counts
//!    `PageStore::read_page` calls through a decorator, so it measures pages the engine actually
//!    pulled rather than a counter the code chose to report. Rewire `page_changeset` back to
//!    `CowTree::diff` and this fails: that path calls `walk_pages` on both roots first, so it
//!    reads O(N) pages, and it would keep reporting a small `pages_examined` the whole time. A
//!    test written against the reported counter alone would NOT catch it — which is the defect
//!    this row is fixing, one layer up.
//! 2. **The skip fires.** `two_identical_roots_read_nothing` forces the identical case: a diff of
//!    a root against itself must decode zero nodes. A skip mechanism that never fires is not a
//!    clean result.
//! 3. **The skip does not fire spuriously.** `every_leaf_differs_so_nothing_is_skipped` forces the
//!    opposite end: when every leaf changed there is nothing to skip, and the skip count must be
//!    zero while every change is still reported. Together with (2) this pins both directions; a
//!    provider that reported everything equal would pass (2) and lose every change in (3).
//!
//! `the_memoised_content_identity_agrees` covers the provider `page_changeset` deliberately does
//! NOT use, and states why in a runnable form rather than in a comment.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{ArenaId, BranchId, Epoch, PageId};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::cow::btree::CowTree;
use ferrodb::cow::diff::{diff, skipped_node_count, MemoIdentity, PageIdentity};
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{CowPage, PageHandle, PageStore};
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;

const ARENA_BASE: u32 = 1024;
const TABLE: &str = "inventory";

// -------------------------------------------------------------------------------------------
// A page store that counts reads
// -------------------------------------------------------------------------------------------

/// Delegates everything to the real store and counts `read_page`.
///
/// **This is the instrument, and it is deliberately not a counter the subject reports.** The
/// defect D103 fixes is exactly a counter that told the truth about the half it measured while an
/// O(N) traversal happened beside it; a test that trusted the subject's own number would inherit
/// that blind spot. Counting at the store boundary measures what the engine actually asked for.
struct CountingStore {
    inner: Arc<dyn PageStore>,
    reads: Arc<AtomicUsize>,
}

impl CountingStore {
    fn new(inner: Arc<dyn PageStore>) -> (Arc<dyn PageStore>, Arc<AtomicUsize>) {
        let reads = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn PageStore> =
            Arc::new(CountingStore { inner, reads: Arc::clone(&reads) });
        (store, reads)
    }
}

impl PageStore for CountingStore {
    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        self.inner.alloc_in_arena(arena, page_type, birth_epoch)
    }

    fn read_page(&self, page_id: PageId) -> Result<PageHandle, FerroError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
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

// -------------------------------------------------------------------------------------------
// Fixture
// -------------------------------------------------------------------------------------------

struct Fixture {
    runtime: AgentRuntime,
    reads: Arc<AtomicUsize>,
    _dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("d103.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let branches = Arc::new(LogBranchCatalog::in_memory(1));
        let arena = Arc::new(
            ArenaPageStore::new(
                bp.clone(),
                Arc::clone(&branches) as Arc<dyn ferrodb::branch::BranchCatalog>,
                ARENA_BASE,
            )
            .unwrap(),
        );
        let (store, reads) = CountingStore::new(Arc::clone(&arena) as Arc<dyn PageStore>);
        let runtime =
            AgentRuntime::with_storage(branches, Arc::new(MemEffectLog::new()), store).unwrap();
        Fixture { runtime, reads, _dir: dir }
    }

    /// `n` rows on trunk's own tree, so the fork point is a real tree rather than an empty one.
    fn seed_trunk(&self, n: usize) {
        for i in 0..n {
            let row = vec![Value::Integer(i as i32), Value::Integer(100)];
            self.runtime.put_row(BranchId::TRUNK, TABLE, i as u64, &row).unwrap();
        }
    }

    fn fork(&self, agent: &str) -> BranchId {
        self.runtime.begin_session(agent, Some("r"), BranchId::TRUNK).unwrap().branch
    }

    fn write(&self, branch: BranchId, id: u64, qty: i32) {
        let row = vec![Value::Integer(id as i32), Value::Integer(qty)];
        self.runtime.put_row(branch, TABLE, id, &row).unwrap();
    }

    fn take_reads(&self) -> usize {
        self.reads.swap(0, Ordering::Relaxed)
    }
}

// -------------------------------------------------------------------------------------------
// 1. The wiring is real
// -------------------------------------------------------------------------------------------

/// **Pages actually read by `page_changeset_with_cost` track DEPTH, not N.**
///
/// (D193: this line used to say "a production `DIFF`", and the test's name still does; the
/// function under test is not what `DIFF <branch>` runs — see the module doc.)
///
/// Four rows change in a tree of 8000. The old path had to enumerate both roots before it could
/// prune — more than 2·(tree nodes) reads — while reporting a `pages_examined` in the single
/// digits. This counts at the store boundary, so the rewiring is what is being tested and not the
/// counter.
///
/// The bound is stated against the TREE's own node count, read from the tree, so it scales with
/// the fixture instead of being a number someone has to remember to update.
#[test]
fn a_production_diff_reads_pages_in_proportion_to_depth() {
    let f = Fixture::new();
    f.seed_trunk(8_000);
    let branch = f.fork("agent-a");
    for id in [3u64, 17, 511, 4_000] {
        f.write(branch, id, 999);
    }

    let tree = f.runtime.storage().unwrap().tree();
    let head = f.runtime.root_of(branch).unwrap();
    let nodes = tree.walk_pages(head).unwrap().len();
    assert!(nodes > 200, "the fixture tree is too small to tell the two paths apart: {nodes}");

    f.take_reads();
    let (changes, cost) = f.runtime.page_changeset_with_cost(branch).unwrap();
    let reads = f.take_reads();

    assert_eq!(changes.len(), 4, "the diff must still report every change");
    assert_eq!(
        cost.visited, reads,
        "every page the diff read should be a node it decoded: visited={} reads={}",
        cost.visited, reads
    );
    assert!(
        reads * 10 < nodes,
        "a page-derived changeset of 4 rows read {reads} pages out of a {nodes}-node tree. \
         The old path reads more than 2x that many; this one is supposed to read \
         O(delta · depth)."
    );
    assert!(cost.skipped_subtrees > 0, "no subtree was skipped, so nothing was pruned");

    // The control, on the same two roots, in the same process: what the path this replaced has to
    // enumerate before it can prune either side. Asserted rather than described.
    let fork_root = {
        // The branch forked from trunk, and trunk has not moved since.
        f.runtime.root_of(BranchId::TRUNK).unwrap()
    };
    let old = tree.diff(fork_root, head).unwrap();
    assert_eq!(old.deltas.len(), 4, "the control must see the same changes");
    assert!(
        old.pages_walked > nodes,
        "the control walked {} pages of a {nodes}-node tree — that is not the O(N) enumeration \
         this comparison assumes, so the comparison would be meaningless",
        old.pages_walked
    );
    assert!(
        old.pages_examined < reads * 4,
        "pages_examined ({}) is supposed to be the SMALL half — the whole point is that it looks \
         cheap while pages_walked ({}) is what the path costs",
        old.pages_examined,
        old.pages_walked
    );
}

// -------------------------------------------------------------------------------------------
// 2. The skip fires
// -------------------------------------------------------------------------------------------

/// **Identical roots: zero nodes decoded, zero pages read, zero changes.**
///
/// A branch that read but never wrote. Its root IS trunk's root, so the descent's very first
/// identity test succeeds and nothing below it is touched.
///
/// ⚠ This is the forced-fire case. Without it, `skipped_subtrees > 0` elsewhere could be reported
/// by a mechanism that never actually declines to read a page.
#[test]
fn two_identical_roots_read_nothing() {
    let f = Fixture::new();
    f.seed_trunk(4_000);
    let branch = f.fork("agent-idle");

    let tree = f.runtime.storage().unwrap().tree();
    let nodes = tree.walk_pages(f.runtime.root_of(branch).unwrap()).unwrap().len();
    assert!(nodes > 50, "too small a tree for 'read nothing' to mean anything: {nodes}");

    f.take_reads();
    let (changes, cost) = f.runtime.page_changeset_with_cost(branch).unwrap();
    let reads = f.take_reads();

    assert!(changes.is_empty(), "an untouched branch changed {} rows", changes.len());
    assert_eq!(cost.visited, 0, "an untouched branch decoded {} nodes", cost.visited);
    assert_eq!(reads, 0, "an untouched branch read {reads} pages of a {nodes}-node tree");
    assert_eq!(cost.skipped_subtrees, 1, "the whole tree is one skip at the root");
}

// -------------------------------------------------------------------------------------------
// 3. The skip does not fire spuriously
// -------------------------------------------------------------------------------------------

/// **Every leaf differs: nothing is skipped, and every change is still reported.**
///
/// The branch rewrites every row, so no subtree is shared and there is no skip to take. The
/// assertion pair is the point: `skipped == 0` AND all N changes present. A provider that reported
/// every subtree equal would produce `skipped` large and `changes` empty — it would pass the
/// identical-roots test above and fail here, which is why both exist.
#[test]
fn every_leaf_differs_so_nothing_is_skipped() {
    const N: usize = 600;
    let f = Fixture::new();
    f.seed_trunk(N);
    let branch = f.fork("agent-rewrite");
    for id in 0..N as u64 {
        f.write(branch, id, 777);
    }

    let tree = f.runtime.storage().unwrap().tree();
    let head = f.runtime.root_of(branch).unwrap();
    let nodes = tree.walk_pages(head).unwrap().len();
    assert!(nodes > 10, "the tree must have internal structure for a skip to be possible");

    let (changes, cost) = f.runtime.page_changeset_with_cost(branch).unwrap();

    assert_eq!(changes.len(), N, "a full rewrite lost changes: {} of {N}", changes.len());
    assert_eq!(
        cost.skipped_subtrees, 0,
        "{} subtrees were skipped although every leaf changed — the skip is firing spuriously and \
         a change is being dropped somewhere",
        cost.skipped_subtrees
    );
    assert!(
        cost.visited >= nodes,
        "a full rewrite decoded {} nodes of a {nodes}-node tree; it has to read all of them",
        cost.visited
    );

    // Every key reported once, and every row genuinely moved.
    let mut seen = HashSet::new();
    for c in &changes {
        assert!(seen.insert(c.row), "row {:?} reported twice", c.row);
        assert!(c.before.is_some() && c.after.is_some(), "a rewrite is a modification, not an add");
    }
}

// -------------------------------------------------------------------------------------------
// The provider `page_changeset` deliberately does NOT use
// (D193: this heading used to say "NOT on the production path"; `page_changeset` is not the
// production `DIFF` path either — see the module doc.)
// -------------------------------------------------------------------------------------------

/// **`MemoIdentity` gives the same answer, must be warmed, and reports it when it was not.**
///
/// `page_changeset` uses `PageIdentity`: within one lineage a page that did not change IS the same
/// page, so page equality already decides every skip and a content digest can win no additional
/// ones. The content digest earns its keep across lineages, where page ids are not comparable.
///
/// The trap this pins is the one `cow::cid::subtree_cid` sets: it has no memo table and costs "the
/// whole subtree, every time", so handing it to `NodeIdentity::id_of` directly turns each O(1)
/// skip test into a full subtree walk — strictly more work than the O(N) path being replaced,
/// while still reporting skips. `MemoIdentity::warm` is what makes it affordable and
/// `MemoIdentity::misses` is what makes an unwarmed provider visible instead of silently slow.
/// Both halves are asserted here, including a deliberately unwarmed run so `misses` is proved to
/// fire rather than assumed to.
#[test]
fn the_memoised_content_identity_agrees() {
    let f = Fixture::new();
    f.seed_trunk(2_000);
    let branch = f.fork("agent-a");
    for id in [5u64, 99, 1_500] {
        f.write(branch, id, 42);
    }

    let tree = f.runtime.storage().unwrap().tree();
    let base = f.runtime.root_of(BranchId::TRUNK).unwrap();
    let head = f.runtime.root_of(branch).unwrap();

    let by_page = diff(tree, base, head, &PageIdentity).unwrap();
    assert_eq!(by_page.changes.len(), 3);

    // Warmed: no fallbacks, and the same answer.
    let warm = MemoIdentity::new(tree, |t: &CowTree, p| ferrodb::cow::cid::subtree_cid(t, p));
    warm.warm(base).unwrap();
    warm.warm(head).unwrap();
    let by_cid = diff(tree, base, head, &warm).unwrap();
    assert_eq!(
        warm.misses(),
        0,
        "a warmed provider fell back to page identity {} times",
        warm.misses()
    );
    assert_eq!(by_cid.changes, by_page.changes, "the two providers disagreed about what changed");

    // Unwarmed: still correct, because the fallback is page identity and that is sound here — but
    // `misses` is non-zero, which is the signal that the digest was never actually consulted.
    let cold = MemoIdentity::new(tree, |t: &CowTree, p| ferrodb::cow::cid::subtree_cid(t, p));
    let by_cold = diff(tree, base, head, &cold).unwrap();
    assert_eq!(by_cold.changes, by_page.changes, "the fallback changed the answer");
    assert!(
        cold.misses() > 0,
        "an unwarmed provider reported zero misses, so the counter that is supposed to make this \
         visible does not fire"
    );

    // And the audit: the nodes under the skipped subtrees are the ones the descent never read.
    let skipped_nodes = skipped_node_count(tree, &by_page).unwrap();
    assert!(
        skipped_nodes > by_page.visited,
        "the skip saved {skipped_nodes} nodes against {} visited, which is not a saving",
        by_page.visited
    );
}
