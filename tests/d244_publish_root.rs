//! D244 — **the branch catalog's header page must never be left naming a root the tree has left.**
//!
//! `TableBranchCatalog` keeps its tree's root in a fixed header page (page 1 of its file). A root
//! split moves the root in memory at once. `publish_root` writes the new root to page 1, and the
//! next open reads it from there. At `9aa6968` two things could leave page 1 stale while the split
//! pages still reached the disk:
//!
//! - **A2:** `publish_root` did `published_root.swap(root)` BEFORE fetching page 1. One failed fetch
//!   therefore marked the root published without writing it, every later call returned early, and
//!   each later `durable()` flushed the split tree and never the header.
//! - **A1:** a mutator published only through `stage()`, after its last `?`. A fault after a root
//!   split returned before publishing, and the next flush wrote the split pages without page 1.
//!
//! Either way, the next open reads the old root and searches for the header key `[0x07]`. At
//! `9aa6968` that is the tree's maximum key, so after a split it sits in the new right part, never
//! under the old root, and the open REFUSES: "branch catalog header key is missing". Every branch
//! becomes unreachable. (Branches that add keys above `[0x07]` keep this only while fewer keys lie
//! above the header than below it: D244 review 2, R2-8.)
//!
//! A3 covers a third route, found by D244 review 2 (R2-2): after a failed publish, ANY later sync
//! flushed the split pages without page 1, whatever exit followed. `durable()` now publishes first.
//!
//! # The fault injector, and why it is not `sim::SimStorage`
//!
//! `SimStorage` never faults a read, and any fault it fires ends the run. Both schedules here need
//! one failed READ and then more work on the same catalog. So `FlakyStorage` below is an in-memory
//! file that fails exactly the read an armed [`Rule`] picks and then carries on. It is handed to
//! `DiskManager::with_storage`, the same injection point.
//!
//! # Making page 1 non-resident
//!
//! Page 1 is only read from storage when it is not in the buffer pool. The production precondition
//! is a catalog over 1024 pages, where page 1 has been evicted. Here each mutation starts from a
//! cold pool instead: `flush_all` then `invalidate_all`. That is the same state for every page, not
//! only page 1.

use std::collections::HashSet;
use std::io;
use std::sync::{Arc, Mutex};

use ferrodb::branch::table_catalog::SIDECAR_HEADER_PAGE;
use ferrodb::branch::{BranchCatalog, BranchId, BranchState, LeaseDeadline, TableBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::BPlusTreePage;
use ferrodb::storage::storage::Storage;

/// Where page 1 starts in the file.
const PAGE_ONE: u64 = SIDECAR_HEADER_PAGE as u64 * PAGE_SIZE as u64;
/// Every child's lease: one value, so deadline keys arrive in id order.
const LEASE: LeaseDeadline = LeaseDeadline(4_000_000_000_000);
/// Trunk's data root lives in another file; any page id will do.
const TRUNK_ROOT: u32 = 7;
/// The text every injected failure carries, so a test can tell it from any other error.
const INJECTED: &str = "d244 injected read failure";

/// Which read to fail. One-shot: the first read it matches fails, and the rule turns itself off.
#[derive(Clone, Debug)]
enum Rule {
    Off,
    /// The next read at this offset.
    ReadAt { offset: u64 },
    /// The first read of a TREE page that existed before the rule was armed, once at least
    /// `appends_needed` pages have been appended since. Page 1 (the header) and page 0 (the
    /// allocator's bitmap, which every `new_page` reads) are never it, and neither are the new
    /// pages' own reads (`new_page` reads back the page it has just written).
    ///
    /// A root split of a two-level tree appends three pages (the leaf, the root's sibling, then
    /// the new root, published into the tree's cell right after it is written). One earlier leaf
    /// split in the same call can make it four, and then the third append is the sibling. Between
    /// that append and the new root's publication the only reads are page 0 and the new root's own,
    /// both skipped, so this fires only after the root has moved.
    ReadAfterAppends { appends_needed: usize, appended: HashSet<u64> },
}

/// An in-memory file that fails exactly the one read the armed [`Rule`] picks.
struct FlakyStorage {
    image: Mutex<Vec<u8>>,
    rule: Mutex<Rule>,
    fired: Mutex<Vec<String>>,
}

impl FlakyStorage {
    fn new() -> Arc<Self> {
        Arc::new(FlakyStorage { image: Mutex::new(Vec::new()), rule: Mutex::new(Rule::Off), fired: Mutex::new(Vec::new()) })
    }

    fn arm(&self, rule: Rule) {
        *self.rule.lock().unwrap() = rule;
    }

    fn disarm(&self) {
        *self.rule.lock().unwrap() = Rule::Off;
    }

    fn fired(&self) -> Vec<String> {
        self.fired.lock().unwrap().clone()
    }
}

impl Storage for FlakyStorage {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let mut image = self.image.lock().unwrap();
        if offset >= image.len() as u64 {
            if let Rule::ReadAfterAppends { appended, .. } = &mut *self.rule.lock().unwrap() {
                appended.insert(offset);
            }
        }
        let end = offset as usize + buf.len();
        if image.len() < end {
            image.resize(end, 0);
        }
        image[offset as usize..end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        {
            let mut rule = self.rule.lock().unwrap();
            let fire = match &*rule {
                Rule::Off => false,
                Rule::ReadAt { offset: at } => *at == offset,
                Rule::ReadAfterAppends { appends_needed, appended } => {
                    appended.len() >= *appends_needed
                        && !appended.contains(&offset)
                        && offset != PAGE_ONE
                        && offset != 0
                }
            };
            if fire {
                let what = format!("{INJECTED}: the read of page {} ({rule:?})", offset / PAGE_SIZE as u64);
                *rule = Rule::Off;
                self.fired.lock().unwrap().push(what.clone());
                return Err(io::Error::new(io::ErrorKind::Other, what));
            }
        }
        let image = self.image.lock().unwrap();
        let start = (offset as usize).min(image.len());
        let n = buf.len().min(image.len() - start);
        buf[..n].copy_from_slice(&image[start..start + n]);
        Ok(n)
    }

    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.image.lock().unwrap().resize(len as usize, 0);
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.image.lock().unwrap().len() as u64)
    }
}

/// A fresh catalog in `storage`, created the way `open_sidecar` creates one.
fn create(storage: &Arc<FlakyStorage>) -> TableBranchCatalog {
    let dm = DiskManager::with_storage(storage.clone() as Arc<dyn Storage>).expect("disk manager");
    let pool = Arc::new(BufferPoolManager::new(Arc::new(dm)));
    let (cat, header) = TableBranchCatalog::create_with_header(pool, TRUNK_ROOT).expect("create the catalog");
    assert_eq!(header, SIDECAR_HEADER_PAGE, "premise failed: the header must be page 1, where the injector aims");
    cat
}

/// Open what `storage` holds with a fresh pool, the way `open_sidecar` reopens a file.
fn reopen(storage: &Arc<FlakyStorage>) -> Result<TableBranchCatalog, FerroError> {
    let dm = DiskManager::with_storage(storage.clone() as Arc<dyn Storage>)?;
    let pool = Arc::new(BufferPoolManager::new(Arc::new(dm)));
    TableBranchCatalog::open_from_header(pool, SIDECAR_HEADER_PAGE)
}

/// Write every dirty page back, then drop every cached page, so the next access reads storage.
fn cold(cat: &TableBranchCatalog) {
    cat.pool_handle().flush_all().expect("flush the pool");
    cat.pool_handle().invalidate_all().expect("nothing is pinned between mutations");
}

/// Free bytes in the tree's root, if the root is an internal node. Internal pages are full once
/// `19 + keys + 4 * children >= PAGE_SIZE` (`index_page.rs`, `BPlusTreeInternalPage::is_full`).
fn internal_root_room(cat: &TableBranchCatalog) -> Option<usize> {
    let root = cat.root_page_id();
    let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::open(root, cat.pool_handle().clone());
    match tree.read_node(root).expect("read the root") {
        BPlusTreePage::Leaf(_) => None,
        BPlusTreePage::Internal(n) => {
            let used = 19 + n.key_arr.iter().map(|k| 4 + k.len()).sum::<usize>() + 4 * n.child_ptrs.len();
            Some(PAGE_SIZE.saturating_sub(used))
        }
    }
}

/// The reopen must succeed and find every branch this test forked.
fn assert_reopens_with(storage: &Arc<FlakyStorage>, ids: &[u64]) {
    let reopened = reopen(storage).unwrap_or_else(|e| {
        panic!("the catalog must reopen from its file after the fault, and it refused: {e}")
    });
    for id in ids {
        reopened
            .get_raw(*id)
            .unwrap_or_else(|e| panic!("branch {id} must still be in the reopened catalog: {e}"));
    }
}

/// A2: one failed read of page 1 at a root split's publish, then one more mutation.
///
/// At `9aa6968` the failed publish has already swapped the new root into `published_root`, so the
/// next mutation's publish returns early and its `durable()` flushes the split tree without page 1.
/// The reopen reads the old root and refuses. With the fix the failed publish records nothing, the
/// next mutation's publish retries and writes page 1, and the reopen finds every branch.
#[test]
fn a_failed_read_of_page_one_at_a_root_splits_publish_is_retried_by_the_next_mutation() {
    let storage = FlakyStorage::new();
    let cat = create(&storage);
    let root_before = cat.root_page_id();
    let mut ids = Vec::new();

    // Page 1 is read from storage only when a publish has a moved root to write, so the first
    // fork whose publish reads it is the first fork that split the root.
    storage.arm(Rule::ReadAt { offset: PAGE_ONE });
    let refused = loop {
        assert!(ids.len() < 5_000, "premise failed: {} forks never moved the root", ids.len());
        cold(&cat);
        match cat.fork(BranchId::TRUNK, LEASE) {
            Ok(rec) => ids.push(rec.branch_id.id),
            Err(e) => break e.to_string(),
        }
    };
    storage.disarm();
    let fired = storage.fired();
    assert!(
        fired.len() == 1 && fired[0].contains("the read of page 1 "),
        "premise failed: exactly one read of page 1 must have been failed; fired: {fired:?}"
    );
    assert!(refused.contains(INJECTED), "premise failed: the fork failed for another reason: {refused}");
    assert_ne!(cat.root_page_id(), root_before, "premise failed: the root did not split before the failed publish");

    cold(&cat);
    let rec = cat.fork(BranchId::TRUNK, LEASE).expect("the mutation after the failed publish must succeed");
    ids.push(rec.branch_id.id);
    cat.pool_handle().flush_all().expect("flush");
    drop(cat);

    assert_reopens_with(&storage, &ids);
}

/// A3 (D244 review 2, R2-2): a failed publish, no further mutation, then a sync, then no clean exit.
///
/// Forks are staged, not awaited, so no sync runs until the end. The first fork that splits the
/// root is the first whose publish reads page 1, and that read fails. Then the last GOOD fork's
/// ticket is awaited: its `durable()` is the next sync, and it flushes the split pages. The catalog
/// is then dropped with no exit path at all, which is what a kill, a panic or pgserver amounts to.
///
/// At `9aa6968` the failed publish had already swapped the new root in, so nothing was owed. Up to
/// the D244 exit fix, the failed publish left the root owed but `durable()` never paid it. Either
/// way the sync writes the split pages under the old header, and the reopen refuses. With
/// `durable()` publishing first, page 1 is written in that same flush.
#[test]
fn a_sync_after_a_failed_publish_writes_the_root_before_the_split_pages() {
    let storage = FlakyStorage::new();
    let cat = create(&storage);
    let root_before = cat.root_page_id();
    let mut ids = Vec::new();
    let mut last_ticket = None;

    storage.arm(Rule::ReadAt { offset: PAGE_ONE });
    let refused = loop {
        assert!(ids.len() < 5_000, "premise failed: {} forks never moved the root", ids.len());
        cold(&cat);
        match cat.fork_staged(BranchId::TRUNK, LEASE) {
            Ok((rec, seq)) => {
                ids.push(rec.branch_id.id);
                last_ticket = seq;
            }
            Err(e) => break e.to_string(),
        }
    };
    storage.disarm();
    let fired = storage.fired();
    assert!(
        fired.len() == 1 && fired[0].contains("the read of page 1 "),
        "premise failed: exactly one read of page 1 must have been failed; fired: {fired:?}"
    );
    assert!(refused.contains(INJECTED), "premise failed: the fork failed for another reason: {refused}");
    assert_ne!(cat.root_page_id(), root_before, "premise failed: the root did not split before the failed publish");
    let ticket = last_ticket.expect("premise failed: no fork was staged before the failed publish");

    // No further mutation. The earlier fork's sync: no sync has run in this test, so it flushes.
    cat.await_fork_durable(Some(ticket)).expect("the earlier fork's sync");
    drop(cat);

    assert_reopens_with(&storage, &ids);
}

/// The leaf an insert of `key` would land in, and its free bytes, found the way the tree finds it:
/// down from the root through `BPlusTreeInternalPage::find_child`. A leaf splits on an insert of `E`
/// bytes once `27 + keys + values + E >= PAGE_SIZE` (`index_page.rs`, `BPlusTreeLeafPage::is_full`),
/// and every key and value here is a `Vec<u8>` serialized behind a 4-byte length.
fn leaf_of(cat: &TableBranchCatalog, key: &[u8]) -> (u32, usize) {
    let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::open(cat.root_page_id(), cat.pool_handle().clone());
    let key = key.to_vec();
    let mut page = cat.root_page_id();
    loop {
        match tree.read_node(page).expect("read a tree page") {
            BPlusTreePage::Internal(n) => page = n.find_child(&key),
            BPlusTreePage::Leaf(l) => {
                let used = 27
                    + l.key_arr.iter().map(|k| 4 + k.len()).sum::<usize>()
                    + l.vals.iter().map(|v| 4 + v.len()).sum::<usize>();
                return (page, PAGE_SIZE.saturating_sub(used));
            }
        }
    }
}

/// The leaf to the right of leaf `page`: the one a split of `page` re-links, and the one a range
/// scan steps into when `page` holds no key at or above its start.
fn right_of(cat: &TableBranchCatalog, page: u32) -> Option<u32> {
    let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::open(cat.root_page_id(), cat.pool_handle().clone());
    match tree.read_node(page).expect("read a tree page") {
        BPlusTreePage::Leaf(l) => l.next,
        BPlusTreePage::Internal(_) => None,
    }
}

/// Bytes a fork's child entry and deadline entry take in a leaf: key and value, each behind a
/// 4-byte length (`tree_keys.rs`: a child key is 17 bytes with an 8-byte id value, a deadline key
/// 17 bytes with an empty value).
const CHILD_ENTRY: usize = 4 + 17 + 4 + 8;
const DEADLINE_ENTRY: usize = 4 + 17 + 4;

/// A1: a fork that faults AFTER it split the root, then a whole-pool flush that does NOT go through
/// `durable()`, then a reopen with no further mutation.
///
/// Why not a sync: since D244 review 2 (R2-2) `durable()` publishes an owed root before it flushes,
/// so a sync would heal a missing exit publish and this test could not see one. What the error
/// exit's publish still buys is page 1 current in the pool from the failure on, so a write-back
/// outside `durable()` carries it. The direct `flush_all` is that write-back.
///
/// A fork writes, in order: its record key, its Live state key, its deadline key, its child key in
/// the parent's live set, and the header key. The first two always append to the tails of their
/// groups (ids only grow). The next two are steered, by the parent and the lease each fork is
/// given, into leaves with room, so they never split. Those leaves must also be leaves the fork has
/// not read before a split can move the root:
/// - not the record-tail or state-tail leaf. The record group (tag 0) and the deadline group (tag 1)
///   are neighbours, so the record-tail leaf also holds deadline keys, and a deadline key steered
///   there would land in a leaf the fork has just read and grown (review void mode v1);
/// - not the leaf right of either tail, which a tail split re-links;
/// - not the free-id scan's leaf or the one right of it, and not the parent's record or envelope
///   leaf (review void mode v2).
///
/// The header rewrite is the same size. So a root split in a fork can only come from the record or
/// the state append, and each is followed by a write to a leaf this fork has not yet read: the state
/// tail after the record, the steered deadline leaf after the state. On a cold pool that write must
/// read from storage, and the injector fails that read. Its first reads (the parent's record, the
/// parent's envelope, the free-id span) are made before anything is appended, so the rule cannot
/// fire on them.
///
/// Phases:
/// 1. **Grow.** 200 parents forked from trunk, then forks round-robin across them, every fork with
///    its own lease, until the root is an internal node with under 150 bytes of room.
/// 2. **Steer.** Staged forks on a cold pool with the rule armed, until one faults. None is
///    awaited, so no sync runs in this phase.
///
/// Then one `flush_all` writes every dirty page, the split ones included. At `9aa6968` the failed
/// fork returned before `stage()`, page 1 still names the old root, and the reopen refuses. With
/// the fix the error exit published the new root into the pool, the flush carries it, and the
/// reopen finds every branch.
#[test]
fn a_fork_that_faults_after_splitting_the_root_still_publishes_it() {
    const PARENTS: u64 = 200;
    const LEASE_STEP: u64 = 1_000;
    let storage = FlakyStorage::new();
    let cat = create(&storage);
    let mut next_lease = 4_000_000_000_000u64;
    let mut lease = || {
        next_lease += LEASE_STEP;
        next_lease
    };

    // ---- 1. Grow a two-level tree whose root is nearly full -----------------------------------
    let mut ids: Vec<u64> = Vec::new();
    let mut leases: Vec<u64> = Vec::new();
    let mut parents: Vec<BranchId> = Vec::new();
    for _ in 0..PARENTS {
        let l = lease();
        let rec = cat.fork(BranchId::TRUNK, LeaseDeadline(l)).expect("fork a parent");
        ids.push(rec.branch_id.id);
        leases.push(l);
        parents.push(rec.branch_id);
    }
    let mut first_internal_root = None;
    for i in 0.. {
        assert!(i < 30_000, "premise failed: {i} forks never filled the root");
        let l = lease();
        let (rec, seq) = cat
            .fork_staged(parents[i % parents.len()], LeaseDeadline(l))
            .expect("fork a child");
        ids.push(rec.branch_id.id);
        leases.push(l);
        if let Some(room) = internal_root_room(&cat) {
            let root = *first_internal_root.get_or_insert(cat.root_page_id());
            assert_eq!(
                cat.root_page_id(),
                root,
                "premise failed: the internal root split while growing, so the tree is three levels \
                 deep and a leaf split can no longer reach its root"
            );
            // Under 150 bytes left. One fork adds at most four separators, 17 + 18 + 25 + 25 = 85
            // bytes (its record, state, deadline and child inserts can each split a leaf, while
            // the leases still rise), so the root is not full yet, and a few tail splits will fill it.
            if room < 150 {
                cat.await_fork_durable(seq).expect("sync the last growing fork");
                break;
            }
        }
        cat.await_fork_durable(seq).expect("sync a growing fork");
    }

    // ---- 2. Staged forks, steered, on a cold pool, until one faults ---------------------------
    let root_before = cat.root_page_id();
    let mut failure = None;
    for attempt in 0..5_000usize {
        let epoch = cat.current_epoch().0 + 1;
        let id = ids.last().copied().expect("ids") + 1;
        // Every leaf this fork reads before a split can move the root. A steered write must land
        // in none of them, so that it is still cold when it comes (review F2, void modes v1/v2):
        // - the two tails it appends to, and the leaf right of each, which a tail split re-links;
        // - the free-id scan's leaf, and the leaf right of it, where the scan finds its first key;
        // - the parent's record and envelope leaves, added once the parent is chosen.
        let record_tail = leaf_of(&cat, &ferrodb::branch::tree_keys::record(id)).0;
        let live_tail = leaf_of(&cat, &ferrodb::branch::tree_keys::state(BranchState::Live.as_u8(), id)).0;
        let free_ids = leaf_of(&cat, &ferrodb::branch::tree_keys::whole_group(ferrodb::branch::tree_keys::tag::FREE_ID).0).0;
        let mut warm = vec![record_tail, live_tail, free_ids];
        warm.extend([record_tail, live_tail, free_ids].iter().filter_map(|p| right_of(&cat, *p)));
        let parent_leaves = |p: BranchId| {
            [
                leaf_of(&cat, &ferrodb::branch::tree_keys::record(p.id)).0,
                leaf_of(&cat, &ferrodb::branch::tree_keys::envelope(p.id)).0,
            ]
        };
        let steered = |key: Vec<u8>, entry: usize, warm: &[u32]| {
            let (page, room) = leaf_of(&cat, &key);
            room > entry && !warm.contains(&page)
        };
        let parent = (0..parents.len())
            .map(|k| parents[(attempt + k) % parents.len()])
            .find(|p| {
                let mut w = warm.clone();
                w.extend(parent_leaves(*p));
                steered(ferrodb::branch::tree_keys::child(p.id, epoch), CHILD_ENTRY, w.as_slice())
            })
            .expect("premise failed: no parent's live set has a leaf with room that this fork does not read first");
        warm.extend(parent_leaves(parent));
        let deadline = (0..leases.len())
            .map(|k| leases[(attempt * 97 + k) % leases.len()] + 1)
            .find(|l| steered(ferrodb::branch::tree_keys::deadline(*l, id), DEADLINE_ENTRY, warm.as_slice()))
            .expect("premise failed: no deadline leaf has room that this fork does not read first");

        cold(&cat);
        storage.arm(Rule::ReadAfterAppends { appends_needed: 3, appended: HashSet::new() });
        let result = cat.fork_staged(parent, LeaseDeadline(deadline));
        let fired = storage.fired();
        storage.disarm();
        match (result, fired.is_empty()) {
            (Ok((rec, _)), true) => {
                assert_eq!(rec.branch_id.id, id, "premise failed: the fork did not take the id its deadline key was steered for");
                assert_eq!(
                    cat.root_page_id(),
                    root_before,
                    "premise failed: a fork split the root and read nothing from storage after the split"
                );
                ids.push(id);
            }
            (Err(e), false) => {
                failure = Some(e.to_string());
                break;
            }
            (Ok(_), false) => panic!("premise failed: the fault fired ({fired:?}) and the fork still succeeded"),
            (Err(e), true) => panic!("the fork failed without the injected fault: {e}"),
        }
    }
    let failure = failure.expect("premise failed: no fork split the root");
    assert!(failure.contains(INJECTED), "premise failed: the fork failed for another reason: {failure}");
    assert_ne!(cat.root_page_id(), root_before, "premise failed: the fault fired before the root moved");

    // A write-back that does not go through `durable()`: it carries page 1 only if the failed
    // fork's own exit published it.
    cat.pool_handle().flush_all().expect("flush the pool");
    drop(cat);

    assert_reopens_with(&storage, &ids);
}
