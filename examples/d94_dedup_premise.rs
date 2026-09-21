//! D94 premise probe: how much DUPLICATE CONTENT does branching actually create?
//!
//! The frontier claim being tested is ForkBase's: *identical content anywhere in the store is one
//! chunk, so cross-branch dedup is a large space win.* ferrodb addresses pages by `PageId` and has
//! no content index, so the claim is structurally available here. Whether it is WORTH anything is
//! an entirely different question, and it is the one this file exists to answer BEFORE a chunk
//! index gets built.
//!
//! **The measurement has to separate two effects that a naive "how many duplicate pages" count
//! fuses together, because only one of them is dedup's to win:**
//!
//! 1. **Pages CoW already shares.** A branch that never writes a page does not copy it; parent and
//!    child name the SAME `PageId`. Those pages are already stored once. A probe that walked each
//!    branch's tree and counted repeated content would count every one of them as a "duplicate"
//!    and report an enormous, entirely fictional saving — the saving CoW already banked.
//! 2. **Pages independently WRITTEN to identical content.** Two branches that each allocate a page
//!    and fill it with the same bytes. This is the only set content addressing can collapse that
//!    CoW has not collapsed already, and it is therefore the whole of D94's budget.
//!
//! So the columns below are a chain, each one a strict subset of the one before it:
//!
//! ```text
//!   page refs        sum over branches of |walk_pages(root)|   — logical page references
//!   distinct pages   |union of those|                          — what is PHYSICALLY stored (CoW done)
//!   distinct whole   distinct sha256(page[0..4096])            — byte-identical WHOLE pages
//!   distinct payload distinct sha256(page[24..4096])           — byte-identical PAYLOADS
//! ```
//!
//! `page refs -> distinct pages` is CoW's saving and is **not** D94's. `distinct pages ->
//! distinct payload` is D94's entire ceiling. If that gap is zero the row is dead.
//!
//! **Whole pages and payloads are reported separately because the header makes them differ.**
//! `cow::page_header` puts `{birth_epoch, arena_id, checksum}` in bytes 0..16 of every page —
//! self-describing on purpose, so that liveness needs no side table. Two branches writing
//! identical rows therefore produce pages that are NOT byte-identical: different arena, different
//! birth epoch, and a crc32 over both. Any content index here has to key on the payload, and that
//! has a consequence the report spells out.
//!
//!   d94_dedup_premise [branches] [rows_per_branch] [dup_frac,comma,separated] [preload_rows]
//!
//! `dup_frac` is the fraction of each branch's rows whose VALUE is drawn from a shared pool rather
//! than being branch-unique. Every branch writes the same KEY set, so the tree shape is held fixed
//! and the only thing the knob moves is content. At 1.0 every branch's tree is payload-identical
//! and dedup's ceiling is maximal; at 0.0 no two branches agree on any row and it should be zero.
//! Both ends are run every time, because a dedup number without its own null case is unreadable.

use std::collections::HashSet;
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline, PageId};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::btree::CowTree;
use ferrodb::cow::{PageStore, PAGE_HEADER_SIZE};
use ferrodb::provenance::sha256_of;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

/// Stop if the filesystem drops below this.
///
/// **This is a LOWER floor than `branch_curve_writes.rs` uses (20 GiB), and lowering it is a
/// decision that has to be stated rather than quietly taken.** That harness is aimed at 10^6
/// branches and ~1 TiB, where 20 GiB is the margin that keeps a runaway from taking the machine
/// down. This one writes a few hundred MiB at its largest point and was run on a box that had
/// 3.8 GiB free, so a 20 GiB floor would refuse every configuration and report nothing — a refusal
/// that looks exactly like a measurement of zero. The floor still has to exist: it is set just
/// above this run's own measured high-water mark, so it refuses a runaway while admitting the run
/// it was written for.
const FREE_FLOOR: u64 = 1024 * 1024 * 1024;

fn free_bytes(path: &std::path::Path) -> Option<u64> {
    let out = std::process::Command::new("df").arg("-k").arg(path).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text.lines().nth(1)?;
    let avail_kb: u64 = line.split_whitespace().nth(3)?.parse().ok()?;
    Some(avail_kb * 1024)
}

/// One configuration's result. Every field is a count of pages, never a ratio: ratios are derived
/// in the report so the raw counts stay quotable.
struct Row {
    branches: usize,
    dup_frac: f64,
    page_refs: u64,
    distinct_pages: u64,
    distinct_whole: u64,
    distinct_payload: u64,
    store_live: u32,
    trunk_pages: u64,
}

fn run(branches: usize, rows: usize, dup_frac: f64, preload: usize) -> Row {
    let dir = std::env::temp_dir().join(format!("ferrodb-d94-{}-{}", std::process::id(), branches));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let main_path = dir.join("main.db");
    let mf = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&main_path)
        .unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(mf).unwrap())));
    let cat_path = dir.join("branches.branchcat");
    let cat_concrete =
        Arc::new(TableBranchCatalog::open_sidecar(&cat_path, 1).expect("open catalog"));
    let cat: Arc<dyn BranchCatalog> = cat_concrete.clone();
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&cat), base).unwrap());
    let tree = CowTree::new(Arc::clone(&store) as Arc<dyn PageStore>);
    let lease = LeaseDeadline(u64::MAX);

    // The trunk, optionally pre-populated. Pages written here are INHERITED by every branch and
    // are the part CoW already shares; they are in the walk of every branch's root and must
    // therefore be visible in `page refs` but counted once in `distinct pages`.
    let mut trunk_root = tree.create(BranchId::TRUNK, cat.next_epoch()).expect("create");
    for i in 0..preload {
        let k = format!("k{:08}", i);
        let v = format!("trunk-value-{:08}", i);
        trunk_root = tree
            .insert(trunk_root, BranchId::TRUNK, cat.next_epoch(), k.as_bytes(), v.as_bytes())
            .expect("trunk insert");
    }

    // How many of each branch's rows take the SHARED value. Computed as a count rather than by
    // sampling per row, so the configuration is exact and reproducible rather than approximately
    // `dup_frac` in expectation.
    let dup_rows = ((rows as f64) * dup_frac).round() as usize;

    let mut roots: Vec<PageId> = Vec::with_capacity(branches + 1);
    roots.push(trunk_root);
    for b in 0..branches {
        let rec = cat.fork(BranchId::TRUNK, lease).expect("fork");
        let mut root = trunk_root;
        for i in 0..rows {
            // **Spread the branch's writes across the trunk's key range, not over the first `rows`
            // keys.** Clustering them hits one leaf, so every branch would copy exactly one page
            // and the inherited-but-unwritten pages — the ones CoW shares and this probe exists to
            // hold apart from dedup's win — would never appear in the counts at all.
            let key_ix = if preload == 0 { i } else { (i * preload) / rows.max(1) };
            let k = format!("k{:08}", key_ix);
            let v = if i < dup_rows {
                // Shared across every branch: identical bytes, independently written.
                format!("shared-value-{:08}", key_ix)
            } else {
                format!("branch-{:08}-value-{:08}", b, key_ix)
            };
            root = tree
                .insert(root, rec.branch_id, cat.next_epoch(), k.as_bytes(), v.as_bytes())
                .expect("insert");
        }
        roots.push(root);
    }

    // The measurement. `walk_pages` is the tree's own reachability walk, so `distinct` is the set
    // of pages that are really on disk holding live data — not an arena high-water mark, which
    // would include pages freed and pending.
    let mut page_refs = 0u64;
    let mut distinct: HashSet<PageId> = HashSet::new();
    let mut trunk_pages = 0u64;
    for (ix, r) in roots.iter().enumerate() {
        let pages = tree.walk_pages(*r).expect("walk");
        if ix == 0 {
            trunk_pages = pages.len() as u64;
        }
        page_refs += pages.len() as u64;
        distinct.extend(pages);
    }

    let mut whole: HashSet<[u8; 32]> = HashSet::new();
    let mut payload: HashSet<[u8; 32]> = HashSet::new();
    for p in &distinct {
        let h = store.read_page(*p).expect("read");
        let f = h.read();
        whole.insert(sha256_of(&f.data[..]));
        payload.insert(sha256_of(&f.data[PAGE_HEADER_SIZE..PAGE_SIZE]));
    }

    let row = Row {
        branches,
        dup_frac,
        page_refs,
        distinct_pages: distinct.len() as u64,
        distinct_whole: whole.len() as u64,
        distinct_payload: payload.len() as u64,
        store_live: store.live_page_count().unwrap_or(0),
        trunk_pages,
    };
    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
    row
}

fn main() {
    println!("d94_dedup_premise {}", ferrodb::build_provenance());
    let branches: Vec<usize> = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "100,1000,10000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let rows: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    let fracs: Vec<f64> = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "0.0,0.5,1.0".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let preload: usize = std::env::args().nth(4).and_then(|s| s.parse().ok()).unwrap_or(5000);

    println!();
    println!("D94 PREMISE: duplicate content across branches, and who already owns the saving.");
    println!("rows/branch = {rows}, trunk preload = {preload} rows, page = {PAGE_SIZE} B, header = {PAGE_HEADER_SIZE} B");
    println!();
    println!("  'page refs' counts a page once PER BRANCH THAT REACHES IT; 'distinct pages' counts it");
    println!("  once. The first gap is COW SHARING and is already banked. The gap from 'distinct");
    println!("  pages' to 'distinct payload' is the ONLY space content dedup can still win.");
    println!();
    println!(
        "  {:>8} {:>6} {:>7} {:>10} {:>10} {:>10} {:>10} {:>9} {:>11} {:>11}",
        "branches",
        "dup",
        "trunk",
        "page refs",
        "distinct",
        "distinct",
        "distinct",
        "store",
        "cow saves",
        "dedup gains"
    );
    println!(
        "  {:>8} {:>6} {:>7} {:>10} {:>10} {:>10} {:>10} {:>9} {:>11} {:>11}",
        "", "frac", "pages", "", "pages", "whole", "payload", "live", "pages", "pages"
    );

    let mut rows_out: Vec<Row> = Vec::new();
    for &n in &branches {
        if let Some(free) = free_bytes(&std::env::temp_dir()) {
            if free < FREE_FLOOR {
                println!(
                    "  STOPPING: {:.1} GiB free, floor is {:.1} GiB. Reported as a stop, not a result.",
                    free as f64 / (1u64 << 30) as f64,
                    FREE_FLOOR as f64 / (1u64 << 30) as f64
                );
                break;
            }
        }
        for &f in &fracs {
            let r = run(n, rows, f, preload);
            println!(
                "  {:>8} {:>6.2} {:>7} {:>10} {:>10} {:>10} {:>10} {:>9} {:>11} {:>11}",
                r.branches,
                r.dup_frac,
                r.trunk_pages,
                r.page_refs,
                r.distinct_pages,
                r.distinct_whole,
                r.distinct_payload,
                r.store_live,
                r.page_refs - r.distinct_pages,
                r.distinct_pages - r.distinct_payload,
            );
            rows_out.push(r);
        }
    }

    println!();
    println!("BYTES, the two columns the row is decided on (pages x {PAGE_SIZE} B):");
    println!(
        "  {:>8} {:>6} {:>16} {:>16} {:>10}",
        "branches", "dup", "stored today B", "deduped B", "saving"
    );
    for r in &rows_out {
        let today = r.distinct_pages * PAGE_SIZE as u64;
        let deduped = r.distinct_payload * PAGE_SIZE as u64;
        println!(
            "  {:>8} {:>6.2} {:>16} {:>16} {:>9.1}%",
            r.branches,
            r.dup_frac,
            today,
            deduped,
            if today == 0 { 0.0 } else { 100.0 * (today - deduped) as f64 / today as f64 }
        );
    }

    println!();
    let any_whole_dupes = rows_out.iter().any(|r| r.distinct_pages != r.distinct_whole);
    if any_whole_dupes {
        println!("WHOLE-PAGE duplicates exist. A content index could key on the whole page.");
    } else {
        println!("NO WHOLE-PAGE DUPLICATE EXISTS IN ANY CONFIGURATION — 'distinct pages' equals");
        println!("'distinct whole' in every row above, including dup_frac = 1.00 where every branch");
        println!("wrote byte-identical ROWS. The self-describing header ({PAGE_HEADER_SIZE} B of");
        println!("birth_epoch + arena_id + crc32) makes two independently written pages differ even");
        println!("when their contents agree. A whole-page content index would find NOTHING here.");
    }
}
