//! D91 — does the subtree-skipping diff cost O(delta · log_m N), or O(N) wearing a skip counter?
//!
//! `cow::btree::CowTree::diff` already skips shared subtrees, but it finds them by enumerating
//! **both trees in full** into two `HashSet<PageId>`s before it can prune either against the
//! other. Its own doc comment concedes the point: "page *identity* traversal is proportional to
//! the tree". Its `pages_examined` counter reports the *decode* half, which is O(delta), so a
//! reader who trusts that counter reads an O(N) operation as cheap.
//!
//! `cow::diff::diff` descends the two roots **together**: children compared pairwise, an equal
//! pair skipped in O(1) without reading either page, and no side ever enumerated alone.
//!
//! PRE-REGISTERED, before the first run. Four rows change at every N, so delta is held fixed and
//! N is the only axis:
//!
//!   * NEW `visited` grows like log N (roughly +1 level per 4x in N, so a handful per step, and
//!     the 1k -> 256k ratio well under 10x)     -> the mechanism works.
//!   * NEW `visited` grows like N (ratio near the 256x growth in N)
//!                                              -> it does not, and the skip counter is decoration.
//!   * NEW `visited` flat at 0 with 4 changes still reported
//!                                              -> impossible; the harness is not diffing what it
//!                                                 thinks and the row is void.
//!
//! The CONTROL column is `old_pages_touched` = `walk_pages(base) + walk_pages(head)`, which is
//! what the existing path must read before it can prune. It is expected to track N exactly; if it
//! does not, the control is wrong and the comparison means nothing.
//!
//! ⚠ The buffer pool is 1024 frames and the largest tree here does not fit in it, so the wall
//! times include real eviction. That is deliberate — it is the cost the counter is a proxy for —
//! but the COUNTERS, not the times, are the claim.
//!
//! Run: `cargo run --release --example d91_diff_curve`

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::branch::types::{BranchId, Epoch, PageId};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::btree::CowTree;
use ferrodb::cow::diff::{diff, skipped_node_count, Change, PageIdentity, SubtreeHash};
use ferrodb::cow::node::Node;
use ferrodb::cow::page_header::{PageHeader, PageType};
use ferrodb::cow::store::CowStore;
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;

const B1: BranchId = BranchId::new(1, 0);

/// Fixed-width so an update rewrites a cell in place and can never split a node. A split would
/// change the tree's shape between the two versions and put a structural difference into a
/// measurement that is supposed to isolate four row changes.
fn key(i: usize) -> String {
    format!("k{:09}", i)
}

fn value(i: usize, ver: u32) -> String {
    format!("v{:09}-{:04}", i, ver)
}

struct Row {
    n: usize,
    tree_nodes: usize,
    depth: usize,
    new_visited_pageid: usize,
    new_visited_hash: usize,
    new_skipped_subtrees: usize,
    new_skipped_nodes: usize,
    old_pages_touched: usize,
    old_pages_examined: usize,
    t_new_pageid: Duration,
    t_new_hash: Duration,
    t_old: Duration,
    t_stamp: Duration,
}

fn depth_of(store: &Arc<dyn PageStore>, root: PageId) -> usize {
    let mut pid = root;
    let mut d = 1;
    loop {
        let h = store.read_page(pid).unwrap();
        let f = h.read();
        let ty = PageHeader::read_from(&f.data).unwrap().page_type;
        if ty == PageType::BTreeLeaf {
            return d;
        }
        let next = Node::new(&f.data).leftmost();
        drop(f);
        drop(h);
        pid = next;
        d += 1;
        assert!(d < 64, "depth walk ran away");
    }
}

fn measure(n: usize) -> Row {
    let dir = tempfile::TempDir::new().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("d91.db"))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let store = Arc::new(CowStore::new(pool));
    let dyn_store: Arc<dyn PageStore> = store.clone();
    let tree = CowTree::new(dyn_store.clone());

    let clock = AtomicU64::new(1);
    let tick = || Epoch(clock.fetch_add(1, Ordering::SeqCst));

    let e = tick();
    let mut base = tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..n {
        let e = tick();
        base = tree
            .insert(base, BranchId::TRUNK, e, key(i).as_bytes(), value(i, 0).as_bytes())
            .unwrap();
    }

    let e = tick();
    store.register_branch(B1, Some(BranchId::TRUNK), e).unwrap();
    let mut head = base;

    // Exactly four rows, spread across the key space so they land in four different leaves.
    let touched = [n / 7, n / 3, (n * 2) / 3, n - 1];
    for i in touched {
        let e = tick();
        head = tree.insert(head, B1, e, key(i).as_bytes(), value(i, 1).as_bytes()).unwrap();
    }
    assert_ne!(head, base, "the four updates did not shadow the root");

    let tree_nodes = tree.walk_pages(base).unwrap().len();
    let depth = depth_of(&dyn_store, base);

    // --- NEW, page identity: no precompute at all -------------------------------------------
    let t0 = Instant::now();
    let r_page = diff(&tree, base, head, &PageIdentity).unwrap();
    let t_new_pageid = t0.elapsed();

    // --- NEW, content hash: the stamp models ForkBase's write-time cid, so it is timed
    //     separately and kept OUT of the diff number rather than hidden inside it.
    let hash = SubtreeHash::new(dyn_store.clone());
    let t0 = Instant::now();
    hash.stamp(base).unwrap();
    hash.stamp(head).unwrap();
    let t_stamp = t0.elapsed();
    let t0 = Instant::now();
    let r_hash = diff(&tree, base, head, &hash).unwrap();
    let t_new_hash = t0.elapsed();

    // --- CONTROL: the existing path -----------------------------------------------------------
    let t0 = Instant::now();
    let old = tree.diff(base, head).unwrap();
    let t_old = t0.elapsed();
    let old_pages_touched =
        tree.walk_pages(base).unwrap().len() + tree.walk_pages(head).unwrap().len();

    // --- refuse rather than bank a row that measured the wrong thing -------------------------
    let check = |label: &str, changes: &[Change]| {
        assert_eq!(changes.len(), 4, "{label} at n={n} reported {} changes", changes.len());
        for (c, i) in changes.iter().zip(touched) {
            match c {
                Change::Modified { key: k, before, after } => {
                    assert_eq!(k, key(i).as_bytes(), "{label} wrong key at n={n}");
                    assert_eq!(before, value(i, 0).as_bytes(), "{label} wrong before at n={n}");
                    assert_eq!(after, value(i, 1).as_bytes(), "{label} wrong after at n={n}");
                }
                other => panic!("{label} at n={n}: expected Modified, got {other:?}"),
            }
        }
    };
    check("page-identity", &r_page.changes);
    check("subtree-hash", &r_hash.changes);
    assert_eq!(old.deltas.len(), 4, "the control found {} deltas at n={n}", old.deltas.len());
    assert!(
        r_page.skipped_subtrees > 0 && r_hash.skipped_subtrees > 0,
        "no subtree was skipped at n={n}; the mechanism did not fire and the row is void"
    );
    assert_eq!(
        hash.stamped_nodes(),
        tree.walk_pages(base)
            .unwrap()
            .into_iter()
            .chain(tree.walk_pages(head).unwrap())
            .collect::<std::collections::HashSet<_>>()
            .len(),
        "the stamp did not cover both trees at n={n}"
    );

    Row {
        n,
        tree_nodes,
        depth,
        new_visited_pageid: r_page.visited,
        new_visited_hash: r_hash.visited,
        new_skipped_subtrees: r_page.skipped_subtrees,
        new_skipped_nodes: skipped_node_count(&tree, &r_page).unwrap(),
        old_pages_touched,
        old_pages_examined: old.pages_examined,
        t_new_pageid,
        t_new_hash,
        t_old,
        t_stamp,
    }
}

fn main() {
    let provenance = ferrodb::build_provenance();
    println!("d91 subtree-skipping diff — curve, {provenance}");

    let sizes = [1_000usize, 4_000, 16_000, 64_000, 256_000];
    let mut rows = Vec::new();
    for n in sizes {
        eprintln!("  building n={n} ...");
        let r = measure(n);
        eprintln!(
            "    nodes={} depth={} new_visited={} old_touched={}",
            r.tree_nodes, r.depth, r.new_visited_pageid, r.old_pages_touched
        );
        rows.push(r);
    }

    let mut out = String::new();
    out.push_str(&format!("d91 — subtree-skipping structural diff, cost curve {provenance}\n"));
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    out.push_str(&format!("run at unix {unix}\n"));
    out.push('\n');
    out.push_str("Question: is cow::diff::diff O(delta · log_m N), or O(N) with a skip counter\n");
    out.push_str("bolted on? delta is held at exactly 4 modified rows; N is the only axis.\n");
    out.push('\n');
    out.push_str("Instruments:\n");
    out.push_str("  new_visited   cow::diff::DiffStats::visited — nodes whose payload was decoded\n");
    out.push_str("                by the synchronised descent. THE CLAIM.\n");
    out.push_str("  new_skipped   DiffReport::skipped_subtrees (events) and skipped_node_count()\n");
    out.push_str("                (exact nodes under them; an audit walk, not part of the diff).\n");
    out.push_str("  old_touched   walk_pages(base)+walk_pages(head) — what CowTree::diff must\n");
    out.push_str("                enumerate before it can prune. THE CONTROL.\n");
    out.push_str("  old_examined  CowTree::diff's own pages_examined. It reports the DECODE half\n");
    out.push_str("                only, which is why it looks cheap while old_touched tracks N.\n");
    out.push('\n');
    out.push_str("All three paths were asserted to return the same 4 Modified rows at every N;\n");
    out.push_str("a row that did not would have aborted the run rather than been banked.\n");
    out.push('\n');
    out.push_str(
        "      N   nodes  depth | new_visited  new_visited  skipped  skipped |  old_touched  old_examined\n",
    );
    out.push_str(
        "                       |    (pageid)       (hash)  subtree    nodes |     (control)              \n",
    );
    out.push_str(
        "-------  ------  ----- | -----------  -----------  -------  ------- |  -----------  ------------\n",
    );
    for r in &rows {
        out.push_str(&format!(
            "{:7}  {:6}  {:5} | {:11}  {:11}  {:7}  {:7} |  {:11}  {:12}\n",
            r.n,
            r.tree_nodes,
            r.depth,
            r.new_visited_pageid,
            r.new_visited_hash,
            r.new_skipped_subtrees,
            r.new_skipped_nodes,
            r.old_pages_touched,
            r.old_pages_examined,
        ));
    }
    out.push('\n');
    out.push_str("      N | t_new_pageid  t_new_hash      t_old | t_stamp (write-time cid, NOT diff cost)\n");
    out.push_str("------- | ------------  ----------  --------- | --------------------------------------\n");
    for r in &rows {
        out.push_str(&format!(
            "{:7} | {:12?}  {:10?}  {:9?} | {:?}\n",
            r.n, r.t_new_pageid, r.t_new_hash, r.t_old, r.t_stamp
        ));
    }

    let first = rows.first().unwrap();
    let last = rows.last().unwrap();
    let n_growth = last.n as f64 / first.n as f64;
    let visited_growth = last.new_visited_pageid as f64 / first.new_visited_pageid as f64;
    let control_growth = last.old_pages_touched as f64 / first.old_pages_touched as f64;
    out.push('\n');
    out.push_str(&format!(
        "SLOPE, {} -> {} ({:.0}x in N):\n  new_visited  {:>6} -> {:<6} = {:.2}x\n  old_touched  {:>6} -> {:<6} = {:.2}x\n",
        first.n, last.n, n_growth,
        first.new_visited_pageid, last.new_visited_pageid, visited_growth,
        first.old_pages_touched, last.old_pages_touched, control_growth,
    ));
    out.push('\n');
    let verdict = if visited_growth < n_growth.sqrt() && control_growth > n_growth / 2.0 {
        "VERDICT: the pre-registered 'mechanism works' branch. new_visited grew far slower than N\n\
         while the control tracked N, so the skip is doing structural work rather than counting.\n"
    } else if visited_growth > n_growth / 2.0 {
        "VERDICT: the pre-registered 'it does not' branch. new_visited tracks N — the skip counter\n\
         is decoration and the O(delta · log N) claim is NOT supported by this run.\n"
    } else {
        "VERDICT: neither pre-registered branch. new_visited is sub-linear but the control did not\n\
         track N, so the comparison is unsound. Reopen rather than quote this table.\n"
    };
    out.push_str(verdict);

    print!("{out}");
    let path = "bench/d91_diff_skip_curve.txt";
    let mut f = OpenOptions::new().write(true).create(true).truncate(true).open(path).unwrap();
    f.write_all(out.as_bytes()).unwrap();
    eprintln!("banked to {path}");
}
