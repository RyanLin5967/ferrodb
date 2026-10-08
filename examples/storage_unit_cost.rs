//! S18 storage probe: what does the PAGE as the unit of sharing cost per branch?
//!
//! Arms, all on real 4KB pages in a real file:
//!   1. how many pages a SINGLE-ROW write copies (the COW path), and how many payload bytes of
//!      each copied page actually differ;
//!   2. reserved vs allocated vs apparent-file-length vs on-disk blocks, for B branches that
//!      each write exactly one row;
//!   3. the size of the durable free-space map image (`state_bytes`), which `alloc_arena`
//!      rewrites in full and fsyncs on every extent claim.
//!
//! Usage: cargo run --release --example storage_unit_cost -- <trunk_rows> <branches>

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline, ARENA_EXTENT_PAGES};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;

fn key(i: u32) -> Vec<u8> {
    format!("k{:08}", i).into_bytes()
}
fn val(i: u32) -> Vec<u8> {
    format!("v{:08}", i).into_bytes()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let trunk_rows: u32 = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(2000);
    let branches: u32 = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(2000);

    let path = std::env::temp_dir().join(format!("s18-unit-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let file = OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(
            Arc::clone(&pool),
            Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
            base,
        )
        .unwrap(),
    );
    let tree = CowTree::new(store.clone() as Arc<dyn PageStore>);

    // ---- trunk ---------------------------------------------------------------------------
    let e0 = catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, e0).unwrap();
    for i in 0..trunk_rows {
        let e = catalog.next_epoch();
        root = tree.insert(root, BranchId::TRUNK, e, &key(i), &val(i)).unwrap();
    }
    catalog.set_root(BranchId::TRUNK, root).unwrap();
    let trunk_pages = tree.walk_pages(root).unwrap().len();
    let trunk_live = store.live_page_count().unwrap();
    println!("PAGE_SIZE=4096  ARENA_EXTENT_PAGES={}", ARENA_EXTENT_PAGES);
    println!("trunk: rows={} pages_reachable={} live_pages={}", trunk_rows, trunk_pages, trunk_live);

    // ---- arm 1: one fork, one single-row write --------------------------------------------
    let probe = catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
    let live_before = store.live_page_count().unwrap();
    let reserved_before = store.reserved_page_count();
    let e = catalog.next_epoch();
    let new_root = tree
        .insert(probe.root_page_id, probe.branch_id, e, &key(trunk_rows / 2), b"OVERWRITTEN")
        .unwrap();
    let live_after = store.live_page_count().unwrap();
    let reserved_after = store.reserved_page_count();
    println!(
        "ARM1 single-row write on a fresh fork: pages_allocated={} reserved_delta_pages={} \
         new_root={} old_root={}",
        live_after - live_before,
        reserved_after - reserved_before,
        new_root,
        probe.root_page_id
    );
    println!(
        "ARM1 bytes_of_new_physical_pages={}  bytes_of_logical_delta={}",
        (live_after - live_before) as usize * 4096,
        key(trunk_rows / 2).len() + b"OVERWRITTEN".len()
    );

    // a second single-row write on the SAME branch: how much is amortised once the path is private?
    let live_b2 = store.live_page_count().unwrap();
    let e = catalog.next_epoch();
    let r2 = tree
        .insert(new_root, probe.branch_id, e, &key(trunk_rows / 3), b"SECOND")
        .unwrap();
    println!(
        "ARM1b second single-row write, same branch: pages_allocated={} (root {} -> {})",
        store.live_page_count().unwrap() - live_b2,
        new_root,
        r2
    );

    // ---- arm 2: B branches, one row each ---------------------------------------------------
    let live_b = store.live_page_count().unwrap();
    let reserved_b = store.reserved_page_count();
    let image_b = store.state_bytes().len();
    let mut image_total_bytes: u128 = 0;
    let mut image_samples: Vec<(u32, usize)> = Vec::new();

    let t0 = std::time::Instant::now();
    for j in 0..branches {
        let rec = catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
        let e = catalog.next_epoch();
        let r = tree
            .insert(rec.root_page_id, rec.branch_id, e, &key(j % trunk_rows.max(1)), &val(j))
            .unwrap();
        catalog.set_root(rec.branch_id, r).unwrap();
        // What alloc_arena's persist_if_configured would have written, at this point in the run.
        // Sampled, then trapezoid-integrated, because calling it every iteration is itself O(N).
        if j % 64 == 0 || j == branches - 1 {
            let n = store.state_bytes().len();
            image_samples.push((j, n));
        }
    }
    let elapsed = t0.elapsed();

    let live_e = store.live_page_count().unwrap();
    let reserved_e = store.reserved_page_count();
    let image_e = store.state_bytes().len();
    store.flush().unwrap();

    let apparent = std::fs::metadata(&path).unwrap().len();
    let blocks = {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(&path).unwrap().blocks() * 512
    };

    // integrate the sampled image size over the run: total bytes alloc_arena would have
    // rewritten+fsynced, one full image per extent claim (one claim per branch here).
    for w in image_samples.windows(2) {
        let (j0, s0) = w[0];
        let (j1, s1) = w[1];
        image_total_bytes += ((s0 + s1) as u128 / 2) * (j1 - j0) as u128;
    }

    println!(
        "\nARM2 branches={} in {:.2?}  ({:.0} branch-writes/sec)",
        branches,
        elapsed,
        branches as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  allocated pages     {:>12}  ({:.2} per branch, {:.0} B/branch)",
        live_e - live_b,
        (live_e - live_b) as f64 / branches as f64,
        (live_e - live_b) as f64 * 4096.0 / branches as f64
    );
    println!(
        "  RESERVED pages      {:>12}  ({:.2} per branch, {:.0} B/branch)",
        reserved_e - reserved_b,
        (reserved_e - reserved_b) as f64 / branches as f64,
        (reserved_e - reserved_b) as f64 * 4096.0 / branches as f64
    );
    println!(
        "  extent utilisation  {:>11.3}%   (allocated / reserved)",
        100.0 * (live_e - live_b) as f64 / (reserved_e - reserved_b) as f64
    );
    println!("  apparent file len   {:>12} B  ({:.0} B/branch)", apparent, apparent as f64 / branches as f64);
    println!("  on-disk blocks      {:>12} B  ({:.0} B/branch)", blocks, blocks as f64 / branches as f64);
    println!(
        "  free-space map image {:>11} B -> {} B  ({:.1} B/branch marginal)",
        image_b,
        image_e,
        (image_e - image_b) as f64 / branches as f64
    );
    println!(
        "  TOTAL bytes alloc_arena would rewrite+fsync over the run: {} B ({:.1} MB) for {} \
         extent claims",
        image_total_bytes,
        image_total_bytes as f64 / 1e6,
        branches
    );

    if std::env::var("KEEP").is_ok() {
        println!("  file kept at {}", path.display());
    } else {
        let _ = std::fs::remove_file(&path);
    }
}
