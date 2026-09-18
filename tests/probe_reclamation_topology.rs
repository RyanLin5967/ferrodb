//! S18 reclamation probe. Read-only instrumentation of the interval rule against the
//! PRODUCTION catalog (`TableBranchCatalog`), which every existing reaper test replaces with
//! `LogBranchCatalog`.

use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline, PageId};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::DiskManager;

const ARENA_BASE: u32 = 1024;

struct Rig {
    _dir: tempfile::TempDir,
    catalog: Arc<dyn BranchCatalog>,
    store: Arc<ArenaPageStore>,
}

fn rig(tag: &str) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join(format!("{tag}.db"));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&db)
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let cat = Arc::new(
        TableBranchCatalog::open_sidecar(&dir.path().join(format!("{tag}.branches")), 1).unwrap(),
    );
    let catalog: Arc<dyn BranchCatalog> = cat;
    let store = Arc::new(ArenaPageStore::new(pool, catalog.clone(), ARENA_BASE).unwrap());
    Rig { _dir: dir, catalog, store }
}

fn write_pages(r: &Rig, branch: BranchId, n: u32) -> Vec<PageId> {
    let arena = r.store.arena_for(branch).unwrap();
    let epoch = r.catalog.next_epoch();
    (0..n)
        .map(|i| {
            let p = r.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
            let h = r.store.read_page(p).unwrap();
            let mut f = h.write();
            f.data[PAGE_HEADER_SIZE] = (i & 0xff) as u8;
            stamp_checksum(&mut f.data);
            p
        })
        .collect()
}

/// FIRE-CHECK 1. The pinning half of the interval rule, against the production catalog.
/// This is `reaper::a_pinned_page_is_not_released_while_the_child_lives`, verbatim in shape,
/// with `LogBranchCatalog` swapped for the one the shipped binary wires up.
#[test]
fn pinned_pages_survive_a_reap_under_the_production_catalog() {
    let r = rig("pin");
    let reaper = TwoTierReaper::new(r.catalog.clone(), r.store.clone());

    let parent = r.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
    let pages = write_pages(&r, parent.branch_id, 6);
    let child = r.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
    assert!(r.catalog.get(child.branch_id).is_ok());

    let live_before = r.store.live_page_count().unwrap();
    let freed = reaper.reap(parent.branch_id).unwrap();

    eprintln!(
        "PROBE1 freed={freed} pending={} live_before={live_before} live_after={} pages={}",
        r.store.pending_len(),
        r.store.live_page_count().unwrap(),
        pages.len()
    );
    assert!(r.catalog.get(child.branch_id).is_ok(), "child must still be live");
    assert_eq!(freed, 0, "every page predates the fork -> the live child pins all six");
    assert_eq!(r.store.pending_len(), 6, "all six must stay parked");
}

/// FIRE-CHECK 2. Whether `get_raw` on the production catalog can answer the question
/// `drain_pending` asks it.
#[test]
fn get_raw_carries_the_live_children_the_rule_is_asked_about() {
    let r = rig("getraw");
    let parent = r.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
    let _c1 = r.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
    let _c2 = r.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();

    let raw = r.catalog.get_raw(parent.branch_id.id).unwrap();
    let indexed = r
        .catalog
        .live_child_in_epoch_range(parent.branch_id.id, ferrodb::branch::types::Epoch(0), ferrodb::branch::types::Epoch(u64::MAX))
        .unwrap();
    eprintln!(
        "PROBE2 get_raw.live_children.len()={} indexed_says_live_children={}",
        raw.live_children.len(),
        indexed
    );
    assert!(indexed, "the index knows there are two live children");
    assert_eq!(raw.live_children.len(), 2, "get_raw must carry them too");
}

/// FIRE-CHECK 3. A grandchild that outlives its parent's lease.
/// Reaping the middle branch detaches it from the grandparent, which is the only thing pinning
/// the grandparent's pages -- but the grandchild still reads through them.
#[test]
fn a_grandchild_keeps_its_grandparents_pages_when_the_middle_branch_is_reaped() {
    let r = rig("gc");
    let reaper = TwoTierReaper::new(r.catalog.clone(), r.store.clone());

    let gp = r.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
    let gp_pages = write_pages(&r, gp.branch_id, 5);
    // Root the grandparent ON one of its own pages, so the fork chain literally inherits it:
    // fork copies nothing, so mid.root == gp.root and grand.root == mid.root.
    r.catalog.set_root(gp.branch_id, gp_pages[0]).unwrap();
    let mid = r.catalog.fork(gp.branch_id, LeaseDeadline(0)).unwrap();
    let grand = r.catalog.fork(mid.branch_id, LeaseDeadline(0)).unwrap();
    assert_eq!(r.catalog.get(grand.branch_id).unwrap().root_page_id, gp_pages[0],
        "the grandchild is rooted on a page the grandparent allocated");

    // The grandparent's pages are parked: `mid` is live and forked after they were born.
    reaper.reap(gp.branch_id).unwrap();
    let parked_after_gp = r.store.pending_len();

    // `mid`'s lease expires first; the grandchild renewed. Reap only `mid`.
    reaper.reap(mid.branch_id).unwrap();

    eprintln!(
        "PROBE3 gp_pages={} parked_after_gp={} pending_after_mid={} grand_live={}",
        gp_pages.len(),
        parked_after_gp,
        r.store.pending_len(),
        r.catalog.get(grand.branch_id).is_ok()
    );
    assert!(r.catalog.get(grand.branch_id).is_ok(), "the grandchild is still live");
    assert_eq!(
        r.store.pending_len(),
        5,
        "the grandchild is ROOTED on one of these pages, so they must stay parked"
    );
}

/// MEASUREMENT. Cost of a lease-expiry storm as a function of the number of branches expiring
/// together. Each branch owns one arena, so `sweep_empty_extents` (called once per reap, via
/// `drain_pending`, and once more by the scan) walks every live arena every time.
#[test]
#[ignore]
fn storm_cost_curve() {
    for n in [64u32, 128, 256, 512, 1024] {
        let r = rig(&format!("storm{n}"));
        let reaper = TwoTierReaper::new(r.catalog.clone(), r.store.clone());
        for _ in 0..n {
            let b = r.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
            write_pages(&r, b.branch_id, 2);
        }
        let arenas = r.store.live_arenas().len();
        let t = Instant::now();
        let reaped = reaper.reap_expired(u64::MAX / 2).unwrap();
        let el = t.elapsed();
        println!(
            "STORM n={n} arenas={arenas} reaped={} total_ms={:.3} per_branch_us={:.1}",
            reaped.len(),
            el.as_secs_f64() * 1e3,
            el.as_secs_f64() * 1e6 / n as f64
        );
    }
}

/// MEASUREMENT. Slow-path reclamation cost vs the number of pages the branch wrote -- the rule
/// is applied per page and each page's birth epoch is read out of the page header.
#[test]
#[ignore]
fn slow_path_cost_per_page() {
    for pages in [64u32, 128, 256] {
        let r = rig(&format!("slow{pages}"));
        let reaper = TwoTierReaper::new(r.catalog.clone(), r.store.clone());
        let parent = r.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        write_pages(&r, parent.branch_id, pages);
        // one live child -> slow path
        let _child = r.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        let t = Instant::now();
        reaper.reap(parent.branch_id).unwrap();
        let el = t.elapsed();
        println!(
            "SLOWPATH pages={pages} total_ms={:.3} per_page_us={:.2} pending={}",
            el.as_secs_f64() * 1e3,
            el.as_secs_f64() * 1e6 / pages as f64,
            r.store.pending_len()
        );
    }
}

/// MEASUREMENT. Fast path (childless leaf) at the same page counts, for the ratio.
#[test]
#[ignore]
fn fast_path_cost_per_page() {
    for pages in [64u32, 128, 256] {
        let r = rig(&format!("fast{pages}"));
        let reaper = TwoTierReaper::new(r.catalog.clone(), r.store.clone());
        let b = r.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        write_pages(&r, b.branch_id, pages);
        let t = Instant::now();
        reaper.reap(b.branch_id).unwrap();
        let el = t.elapsed();
        println!(
            "FASTPATH pages={pages} total_ms={:.3} per_page_us={:.2}",
            el.as_secs_f64() * 1e3,
            el.as_secs_f64() * 1e6 / pages as f64
        );
    }
}

// ---------------------------------------------------------------------------------------
// FIRE-CHECK 4. The depth escape hatch's size ceiling.
// `MAX_BRANCH_DEPTH = 8`; the only way past it is `collapse`, which materialises the branch's
// whole reachable page graph into ONE arena. An arena is one `ARENA_EXTENT_PAGES` extent.
// ---------------------------------------------------------------------------------------

use ferrodb::branch::reaper::PageLinks;
use ferrodb::storage::disk_manager::PAGE_SIZE;

/// Children are a u16 count then u32 ids, immediately after the page header.
struct FanoutLinks;
impl PageLinks for FanoutLinks {
    fn child_pages(
        &self,
        _t: PageType,
        page: &[u8; PAGE_SIZE],
    ) -> Result<Vec<PageId>, ferrodb::error::FerroError> {
        let n = u16::from_be_bytes([page[PAGE_HEADER_SIZE], page[PAGE_HEADER_SIZE + 1]]) as usize;
        Ok((0..n)
            .map(|i| {
                let o = PAGE_HEADER_SIZE + 2 + i * 4;
                u32::from_be_bytes(page[o..o + 4].try_into().unwrap())
            })
            .collect())
    }
    fn rewrite_child(&self, page: &mut [u8; PAGE_SIZE], old: PageId, new: PageId) {
        let n = u16::from_be_bytes([page[PAGE_HEADER_SIZE], page[PAGE_HEADER_SIZE + 1]]) as usize;
        for i in 0..n {
            let o = PAGE_HEADER_SIZE + 2 + i * 4;
            if u32::from_be_bytes(page[o..o + 4].try_into().unwrap()) == old {
                page[o..o + 4].copy_from_slice(&new.to_be_bytes());
            }
        }
    }
}

fn collapse_at(leaves: u32) -> Result<(), String> {
    let r = rig(&format!("col{leaves}"));
    let reaper = TwoTierReaper::new(r.catalog.clone(), r.store.clone())
        .with_links(Arc::new(FanoutLinks));
    let b = r.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
    let epoch = r.catalog.next_epoch();
    // One `arena_for` per page, so the SOURCE branch rotates onto fresh extents freely.
    let kids: Vec<PageId> = (0..leaves)
        .map(|_| {
            let a = r.store.arena_for(b.branch_id).unwrap();
            r.store.alloc_in_arena(a, PageType::BTreeLeaf, epoch).unwrap()
        })
        .collect();
    let a = r.store.arena_for(b.branch_id).unwrap();
    let root = r.store.alloc_in_arena(a, PageType::BTreeInternal, epoch).unwrap();
    {
        let h = r.store.read_page(root).unwrap();
        let mut f = h.write();
        f.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 2]
            .copy_from_slice(&(kids.len() as u16).to_be_bytes());
        for (i, k) in kids.iter().enumerate() {
            let o = PAGE_HEADER_SIZE + 2 + i * 4;
            f.data[o..o + 4].copy_from_slice(&k.to_be_bytes());
        }
        stamp_checksum(&mut f.data);
    }
    r.catalog.set_root(b.branch_id, root).unwrap();
    reaper.collapse(b.branch_id).map(|_| ()).map_err(|e| e.to_string())
}

#[test]
fn collapse_survives_a_branch_bigger_than_one_extent() {
    for leaves in [64u32, 200, 255, 256, 300, 600] {
        let outcome = collapse_at(leaves);
        println!(
            "COLLAPSE leaves={leaves} pages={} -> {}",
            leaves + 1,
            match &outcome {
                Ok(()) => "ok".to_string(),
                Err(e) => format!("REFUSED: {e}"),
            }
        );
    }
    assert!(
        collapse_at(600).is_ok(),
        "a 601-page branch must be collapsible: collapse is the ONLY way past MAX_BRANCH_DEPTH"
    );
}

/// MEASUREMENT. Creation vs reclamation on ONE instrument, so the ratio is not cross-bench.
#[test]
#[ignore]
fn fork_versus_reap_rate() {
    for n in [128u32, 512] {
        let r = rig(&format!("ratio{n}"));
        let reaper = TwoTierReaper::new(r.catalog.clone(), r.store.clone());
        let t = Instant::now();
        let mut ids = Vec::new();
        for _ in 0..n {
            ids.push(r.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap().branch_id);
        }
        let fork_el = t.elapsed();
        for b in &ids {
            write_pages(&r, *b, 2);
        }
        let t = Instant::now();
        let reaped = reaper.reap_expired(u64::MAX / 2).unwrap();
        let reap_el = t.elapsed();
        println!(
            "RATIO n={n} fork_total_ms={:.1} fork_us_each={:.1} reap_total_ms={:.1} \
             reap_us_each={:.1} reap_over_fork={:.1}x reaped={}",
            fork_el.as_secs_f64() * 1e3,
            fork_el.as_secs_f64() * 1e6 / n as f64,
            reap_el.as_secs_f64() * 1e3,
            reap_el.as_secs_f64() * 1e6 / n as f64,
            reap_el.as_secs_f64() / fork_el.as_secs_f64(),
            reaped.len()
        );
    }
}
