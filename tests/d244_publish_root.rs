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
//! Either way, the next open reads the old root and searches for the header key `[0x07]`. That is
//! the tree's maximum key, and after a split it never sits under the old root, so the open REFUSES:
//! "branch catalog header key is missing". Every branch becomes unreachable.
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
    /// The first read of a page that existed before the rule was armed, other than page 1, once at
    /// least `appends_needed` pages have been appended since. A root split of a two-level tree
    /// appends exactly three pages (the new leaf, the root's new sibling, the new root), and the
    /// new root is published into the tree's cell right after the third. So this fires only after
    /// the root has moved. The new pages' own reads are skipped: `new_page` reads back the page it
    /// has just written.
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
                    appended.len() >= *appends_needed && !appended.contains(&offset) && offset != PAGE_ONE
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

/// A1: a mutation that faults AFTER it split the root, with no successful mutation afterwards.
///
/// `set_state(Live -> Reaping)` grows the tree in exactly one place: it appends the branch's new
/// state key to the Reaping group's tail. So a root split can only happen there. Its later steps,
/// the envelope removal and the arena scan, descend into a region nothing earlier in the call
/// touched, so on a cold pool they must read from storage. The injector fails the first such read
/// after the new root exists.
///
/// Two phases:
/// 1. Fork until the root is an internal node with room for only a few more separators.
/// 2. Move children to Reaping, one per cold pool, until one splits the root.
///
/// Then the pool is flushed, as an eviction or another writer's `durable()` would flush it, and the
/// catalog is reopened with no further mutation. At `9aa6968` the fault returned before `stage()`,
/// page 1 still names the old root, and the reopen refuses. With the fix the error exit published
/// the new root, and the reopen finds every branch.
#[test]
fn a_mutation_that_faults_after_splitting_the_root_still_publishes_it() {
    let storage = FlakyStorage::new();
    let cat = create(&storage);

    // ---- 1. Grow a two-level tree whose root is nearly full --------------------------------------
    let mut children: Vec<BranchId> = Vec::new();
    let mut first_internal_root = None;
    loop {
        assert!(children.len() < 20_000, "premise failed: {} forks never filled the root", children.len());
        let rec = cat.fork(BranchId::TRUNK, LEASE).expect("fork");
        children.push(rec.branch_id);
        if let Some(room) = internal_root_room(&cat) {
            let root = *first_internal_root.get_or_insert(cat.root_page_id());
            assert_eq!(
                cat.root_page_id(),
                root,
                "premise failed: the internal root split while growing, so the tree is three levels \
                 deep and a Reaping split can no longer reach its root"
            );
            // Fewer than ~8 separators of 18 bytes left, and one fork adds at most a few, so the
            // root is not full yet and a few Reaping-tail splits will fill it.
            if room < 150 {
                break;
            }
        }
    }

    // ---- 2. Move children to Reaping until one splits the root --------------------------------
    let root_before = cat.root_page_id();
    let mut failure = None;
    for child in &children {
        cold(&cat);
        storage.arm(Rule::ReadAfterAppends { appends_needed: 3, appended: HashSet::new() });
        let result = cat.set_state(*child, BranchState::Live, BranchState::Reaping);
        let fired = storage.fired();
        storage.disarm();
        match (result, fired.is_empty()) {
            (Ok(()), true) => assert_eq!(
                cat.root_page_id(),
                root_before,
                "premise failed: a set_state split the root and read nothing from storage after the split"
            ),
            (Err(e), false) => {
                failure = Some(e.to_string());
                break;
            }
            (Ok(()), false) => panic!("premise failed: the fault fired ({fired:?}) and set_state still succeeded"),
            (Err(e), true) => panic!("set_state failed without the injected fault: {e}"),
        }
    }
    let failure = failure.expect("premise failed: no set_state split the root");
    assert!(failure.contains(INJECTED), "premise failed: set_state failed for another reason: {failure}");
    assert_ne!(cat.root_page_id(), root_before, "premise failed: the fault fired before the root moved");

    cat.pool_handle().flush_all().expect("flush the split pages, as eviction would");
    drop(cat);

    let ids: Vec<u64> = children.iter().map(|b| b.id).collect();
    assert_reopens_with(&storage, &ids);
}
