//! Adversarial review probes for D89's content-defined chunker. Not part of the shipped suite.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tempfile::TempDir;

use crate::branch::types::{BranchId, Epoch, PageId};
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::cow::btree::CowTree;
use crate::cow::chunker;
use crate::cow::node::{self, Node, NodeMut};
use crate::cow::page_header::{verify_checksum, PageHeader, PageType};
use crate::cow::store::CowStore;
use crate::cow::PageStore;
use crate::storage::disk_manager::{DiskManager, PAGE_SIZE};

struct Fixture {
    _dir: TempDir,
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
        let store = Arc::new(CowStore::with_extent_pages(pool, 256));
        let tree = CowTree::new(store as Arc<dyn PageStore>);
        Fixture { _dir: dir, tree, clock: AtomicU64::new(1) }
    }

    fn tick(&self) -> Epoch {
        Epoch(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    fn build(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> PageId {
        let e = self.tick();
        let mut root = self.tree.create(BranchId::TRUNK, e).unwrap();
        for (k, v) in pairs {
            let e = self.tick();
            root = self.tree.insert(root, BranchId::TRUNK, e, k, v).unwrap();
        }
        root
    }

    fn leaf_partition(&self, root: PageId) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        self.visit(root, 0, &mut out);
        out
    }

    fn visit(&self, pid: PageId, depth: usize, out: &mut Vec<Vec<(Vec<u8>, Vec<u8>)>>) {
        assert!(depth < 64, "descent guard");
        let store = self.tree.store();
        let h = store.read_page(pid).unwrap();
        let (ty, children, entries) = {
            let f = h.read();
            let ty = PageHeader::read_from(&f.data).unwrap().page_type;
            let n = Node::new(&f.data);
            match ty {
                PageType::BTreeLeaf => (ty, Vec::new(), n.leaf_entries().unwrap()),
                PageType::BTreeInternal => (ty, n.all_children().unwrap(), Vec::new()),
                other => panic!("page {} is a {:?}", pid, other),
            }
        };
        drop(h);
        match ty {
            PageType::BTreeLeaf => out.push(entries),
            _ => {
                for c in children {
                    self.visit(c, depth + 1, out);
                }
            }
        }
    }
}

fn pairs(n: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| (format!("key{:06}", i).into_bytes(), format!("value-{}", i).into_bytes()))
        .collect()
}

/// The chunker's own partition of a sorted set, computed without any tree.
fn canonical(set: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let sizes: Vec<usize> = set.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).collect();
    let hashes: Vec<u32> = set.iter().map(|(k, _)| chunker::key_hash(k)).collect();
    let cuts = chunker::leaf_cuts(&sizes, &hashes, node::NODE_CAPACITY);
    let mut bounds = vec![0usize];
    bounds.extend_from_slice(&cuts);
    bounds.push(set.len());
    bounds.windows(2).map(|w| set[w[0]..w[1]].to_vec()).collect()
}

fn shuffled(n: u32, seed: u64) -> Vec<u32> {
    let mut idx: Vec<u32> = (0..n).collect();
    let mut s = seed;
    let mut next = || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    };
    for i in (1..idx.len()).rev() {
        let j = (next() % (i as u64 + 1)) as usize;
        idx.swap(i, j);
    }
    idx
}

fn permute(set: &[(Vec<u8>, Vec<u8>)], seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    shuffled(set.len() as u32, seed).iter().map(|&i| set[i as usize].clone()).collect()
}

fn sizes_of(part: &[Vec<(Vec<u8>, Vec<u8>)>]) -> Vec<usize> {
    part.iter().map(|l| l.len()).collect()
}

// ================================================================================================
// AXIS 2 — is the invariance claim true, or only true for insertion?
// ================================================================================================

/// A1: delete a key that terminates a leaf. The tree's partition must equal the chunker's
/// partition of the remaining content, or "identical content, identical boundaries" is false.
#[test]
fn a1_delete_leaves_a_partition_the_chunker_would_not_have_chosen() {
    let set = pairs(1500);
    let f = Fixture::new();
    let mut root = f.build(&set);
    let before = f.leaf_partition(root);
    assert!(before.len() > 8);

    // A key that is both a content boundary and the last entry of its leaf: deleting it is what
    // erases a boundary. Take the terminal key of leaf 5.
    let victim = before[5].last().unwrap().0.clone();
    assert!(
        chunker::is_boundary(&victim, &before[5].last().unwrap().1),
        "fixture: leaf 5 must end on a content boundary"
    );

    root = f.tree.delete(root, BranchId::TRUNK, f.tick(), &victim).unwrap();
    let after = f.leaf_partition(root);

    let remaining: Vec<(Vec<u8>, Vec<u8>)> =
        set.iter().filter(|(k, _)| k != &victim).cloned().collect();
    let want = canonical(&remaining);

    // The tree still holds the right rows.
    let flat: Vec<_> = after.iter().flatten().cloned().collect();
    assert_eq!(flat, remaining, "delete lost or reordered rows");

    println!("A1 victim = {:?}", String::from_utf8_lossy(&victim));
    println!("A1 tree leaves after delete : {}", after.len());
    println!("A1 chunker leaves for content: {}", want.len());
    println!("A1 tree    sizes 3..9 = {:?}", &sizes_of(&after)[3..9.min(after.len())]);
    println!("A1 chunker sizes 3..9 = {:?}", &sizes_of(&want)[3..9.min(want.len())]);

    // And an independent ascending build of the same content, for good measure.
    let g = Fixture::new();
    let fresh = g.leaf_partition(g.build(&remaining));
    println!("A1 fresh insert-only build   : {} leaves", fresh.len());
    assert_eq!(fresh, want, "control: an insert-only build must match the chunker");

    assert_eq!(
        after, want,
        "AXIS 2 CONFIRMED: after one delete the tree's partition is not the chunker's partition \
         of its own content; a branch that reaches this content by inserting instead gets {} \
         leaves against this tree's {}",
        want.len(),
        after.len()
    );
}

/// A2: overwrite the value of a leaf-terminal boundary key with a *different length*, chosen so
/// the key stops being a boundary. The boundary is erased and nothing repairs it.
#[test]
fn a2_a_value_length_change_erases_a_terminal_boundary() {
    let set = pairs(1500);
    let f = Fixture::new();
    let mut root = f.build(&set);
    let before = f.leaf_partition(root);

    // Find a leaf-terminal boundary key whose boundary can be destroyed by shrinking its value.
    let mut chosen: Option<(Vec<u8>, Vec<u8>)> = None;
    for leaf in before.iter().take(before.len() - 1) {
        let (k, v) = leaf.last().unwrap();
        if !chunker::is_boundary(k, v) {
            continue;
        }
        for len in 0..v.len() {
            let nv = vec![b'Q'; len];
            if !chunker::is_boundary(k, &nv) {
                chosen = Some((k.clone(), nv));
                break;
            }
        }
        if chosen.is_some() {
            break;
        }
    }
    let (k, nv) = chosen.expect("no terminal boundary could be destroyed by a length change");
    println!(
        "A2 key {:?}: value {} bytes -> {} bytes",
        String::from_utf8_lossy(&k),
        set.iter().find(|(kk, _)| kk == &k).unwrap().1.len(),
        nv.len()
    );

    root = f.tree.insert(root, BranchId::TRUNK, f.tick(), &k, &nv).unwrap();
    let after = f.leaf_partition(root);

    let updated: Vec<(Vec<u8>, Vec<u8>)> = set
        .iter()
        .map(|(kk, vv)| if kk == &k { (kk.clone(), nv.clone()) } else { (kk.clone(), vv.clone()) })
        .collect();
    let want = canonical(&updated);

    println!("A2 tree leaves    = {}", after.len());
    println!("A2 chunker leaves = {}", want.len());
    assert_eq!(
        after, want,
        "AXIS 2 CONFIRMED (overwrite): the tree has {} leaves where the chunker's partition of \
         the same content has {}",
        after.len(),
        want.len()
    );
}

/// A2b: can the divergence be repaired by later inserts, or is it permanent?
#[test]
fn a2b_the_erased_boundary_is_never_repaired_by_later_inserts() {
    let set = pairs(800);
    let f = Fixture::new();
    let mut root = f.build(&set);
    let before = f.leaf_partition(root);
    let victim = before[3].last().unwrap().0.clone();
    root = f.tree.delete(root, BranchId::TRUNK, f.tick(), &victim).unwrap();
    let damaged = f.leaf_partition(root);

    // Now insert 2000 fresh keys all over the key space and see whether the region heals.
    let extra: Vec<(Vec<u8>, Vec<u8>)> = (0..2000u32)
        .map(|i| (format!("key{:06}x{:04}", i % 800, i).into_bytes(), b"vv".to_vec()))
        .collect();
    for (k, v) in &extra {
        root = f.tree.insert(root, BranchId::TRUNK, f.tick(), k, v).unwrap();
    }
    let healed = f.leaf_partition(root);

    let mut content: Vec<(Vec<u8>, Vec<u8>)> =
        set.iter().filter(|(k, _)| k != &victim).cloned().collect();
    content.extend(extra.iter().cloned());
    content.sort_by(|a, b| a.0.cmp(&b.0));
    let want = canonical(&content);

    println!("A2b damaged leaves    = {}", damaged.len());
    println!("A2b after 2000 more   = {}", healed.len());
    println!("A2b chunker's partition = {}", want.len());
    let differing = healed
        .iter()
        .zip(want.iter())
        .position(|(a, b)| a != b)
        .map(|i| i.to_string())
        .unwrap_or_else(|| "none".into());
    println!("A2b first differing leaf index = {}", differing);
    assert_eq!(healed, want, "AXIS 2: 2000 later inserts did not repair the erased boundary");
}

// ================================================================================================
// AXIS 1 — can a chunk exceed a page, and what happens then?
// ================================================================================================

/// A3: an over-long chunk is reachable with ordinary keys, and when it is, insertion order
/// changes the partition. Keys are *selected* (not synthesised) so that a long contiguous run
/// contains no content boundary at the nominal target — exactly the tail the `e^-8` argument says
/// is rare, made deterministic.
#[test]
fn a3_an_over_long_chunk_makes_the_partition_order_dependent() {
    // Ordinary, sorted, printable keys with a short value.
    let value = b"v".to_vec();
    let mut run: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut i = 0u32;
    while run.len() < 400 && i < 4_000_000 {
        let k = format!("key{:08}", i).into_bytes();
        if !chunker::is_boundary(&k, &value) {
            run.push((k, value.clone()));
        }
        i += 1;
    }
    assert_eq!(run.len(), 400, "could not collect a boundary-free run");
    // Sorted? `key{:08}` is fixed width, so ascending i is ascending bytes.
    assert!(run.windows(2).all(|w| w[0].0 < w[1].0), "run is not sorted");

    let bytes: usize = run.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).sum();
    println!(
        "A3 run of {} entries, {} bytes, against a {}-byte page and a {}-byte target",
        run.len(),
        bytes,
        node::NODE_CAPACITY,
        chunker::TARGET_CHUNK_BYTES
    );
    assert!(bytes > node::NODE_CAPACITY, "the run must not fit a page");

    let asc = Fixture::new();
    let a = asc.leaf_partition(asc.build(&run));
    let want = canonical(&run);
    println!("A3 ascending build : {:?}", sizes_of(&a));
    println!("A3 chunker's answer: {:?}", sizes_of(&want));

    let mut divergences = Vec::new();
    for seed in [0x5DEE_CE66_D1F3_A7B9u64, 0x0123_4567_89AB_CDEF, 0xDEAD_BEEF_CAFE_F00D] {
        let g = Fixture::new();
        let b = g.leaf_partition(g.build(&permute(&run, seed)));
        println!("A3 shuffled {:#x}: {:?}", seed, sizes_of(&b));
        if b != a {
            divergences.push(seed);
        }
    }
    assert!(
        divergences.is_empty(),
        "AXIS 1/2 CONFIRMED: an over-long chunk makes the partition depend on insertion order; \
         seeds {:x?} produced a different partition from the ascending build",
        divergences
    );
}

/// A4: the entry-size cliff. Once one entry is as large as the mean chunk, every entry is a
/// boundary, so every leaf holds exactly one row. Legal row sizes, catastrophic packing.
#[test]
fn a4_rows_at_or_above_the_target_chunk_put_one_row_in_every_page() {
    println!(
        "NODE_CAPACITY={} TARGET_CHUNK_BYTES={} MAX_ENTRY_BYTES={} MAX_KEY_BYTES={}",
        node::NODE_CAPACITY,
        chunker::TARGET_CHUNK_BYTES,
        node::MAX_ENTRY_BYTES,
        node::MAX_KEY_BYTES
    );
    let mut cliff = None;
    for vlen in [200usize, 400, 480, 500, 520, 600, 800, 990] {
        let set: Vec<(Vec<u8>, Vec<u8>)> = (0..240u32)
            .map(|i| (format!("key{:06}", i).into_bytes(), vec![(i % 251) as u8; vlen]))
            .collect();
        let entry = node::leaf_entry_bytes(&set[0].0, &set[0].1);
        if entry > node::MAX_ENTRY_BYTES {
            println!("  vlen {:4}: entry {:5} B exceeds MAX_ENTRY_BYTES, skipped", vlen, entry);
            continue;
        }
        let f = Fixture::new();
        let part = f.leaf_partition(f.build(&set));
        let rows: Vec<usize> = sizes_of(&part);
        let mean_rows = rows.iter().sum::<usize>() as f64 / rows.len() as f64;
        let util = mean_rows * entry as f64 / node::NODE_CAPACITY as f64 * 100.0;
        println!(
            "  vlen {:4}: entry {:5} B  leaves {:4}  mean rows/leaf {:6.2}  page utilisation {:5.1}%",
            vlen,
            entry,
            part.len(),
            mean_rows,
            util
        );
        if mean_rows < 1.01 && cliff.is_none() {
            cliff = Some((vlen, entry, util));
        }
    }
    assert!(
        cliff.is_none(),
        "AXIS 1 CONFIRMED: at value length {:?} (entry {:?} B) every leaf holds exactly one row, \
         {:?}% page utilisation — the byte-balanced split this replaced packed the page",
        cliff.map(|c| c.0),
        cliff.map(|c| c.1),
        cliff.map(|c| c.2.round())
    );
}

/// A5: long keys. A key at or above the target is always a boundary, whatever its hash.
#[test]
fn a5_long_keys_are_unconditionally_boundaries() {
    let value = b"".to_vec();
    for klen in [100usize, 300, 490, 495, 500, 520, 700, node::MAX_KEY_BYTES] {
        let n = 200u32;
        let hits = (0..n)
            .filter(|i| {
                let mut k = format!("{:06}", i).into_bytes();
                k.resize(klen, b'p');
                chunker::is_boundary(&k, &value)
            })
            .count();
        println!(
            "  klen {:4}: entry {:5} B  boundary for {:3}/{} keys",
            klen,
            node::leaf_entry_bytes(&vec![b'p'; klen], &value),
            hits,
            n
        );
    }
    // The end-to-end consequence, at the largest legal key.
    let klen = node::MAX_KEY_BYTES;
    let set: Vec<(Vec<u8>, Vec<u8>)> = (0..120u32)
        .map(|i| {
            let mut k = format!("{:06}", i).into_bytes();
            k.resize(klen, b'p');
            (k, b"v".to_vec())
        })
        .collect();
    let f = Fixture::new();
    let part = f.leaf_partition(f.build(&set));
    let rows = sizes_of(&part);
    let mean = rows.iter().sum::<usize>() as f64 / rows.len() as f64;
    let entry = node::leaf_entry_bytes(&set[0].0, &set[0].1);
    println!(
        "A5 {}-byte keys: {} leaves, mean {:.2} rows/leaf, {:.1}% page utilisation",
        klen,
        part.len(),
        mean,
        mean * entry as f64 / node::NODE_CAPACITY as f64 * 100.0
    );
    assert!(mean > 1.5, "AXIS 1: max-length keys give {:.2} rows per leaf", mean);
}

// ================================================================================================
// AXIS 4 — the checksum restamp window
// ================================================================================================

fn blank_leaf() -> [u8; PAGE_SIZE] {
    let mut page = [0u8; PAGE_SIZE];
    NodeMut::new(&mut page).init();
    crate::cow::page_header::PageHeader::new(
        Epoch(1),
        crate::branch::types::ArenaId(0),
        PageType::BTreeLeaf,
    )
    .write_to(&mut page);
    crate::cow::page_header::stamp_checksum(&mut page);
    page
}

/// Fill a leaf to refusal with `vlen`-byte values. Returns the number of entries placed.
fn fill_to_refusal(page: &mut [u8; PAGE_SIZE], vlen: usize) -> u32 {
    let mut i = 0u32;
    loop {
        let k = format!("k{:06}", i).into_bytes();
        let cell = node::leaf_cell(&k, &vec![b'v'; vlen]);
        let ok = {
            let mut n = NodeMut::new(page);
            let at = n.count();
            n.insert_cell_at(at, &cell).unwrap()
        };
        if !ok {
            return i;
        }
        i += 1;
        assert!(i < 10_000, "leaf never filled");
    }
}

/// A6: `insert_cell_at`'s doc says it "leav[es] the node untouched" when it refuses. It does not —
/// it compacts first, which rewrites the whole cell area. That is the premise of axis 4:
/// `btree::leaf_put`'s overflow branch drops the write guard on this path **without restamping**,
/// and then calls `alloc_for` / `read_page`.
///
/// The fixture must leave real garbage in the heap and then refuse, or compaction is a byte-wise
/// no-op and the probe proves nothing (it did, on the first run).
#[test]
fn a6_a_refused_insert_has_already_mutated_the_page_bytes() {
    let mut page = blank_leaf();
    let placed = fill_to_refusal(&mut page, 40);
    assert!(placed > 10, "fixture: too few entries");

    // Remove entries from the MIDDLE. `remove_at` only shifts slots, so every removed cell stays
    // in the heap as garbage that nothing has compacted away yet.
    for _ in 0..6 {
        let mut n = NodeMut::new(&mut page);
        n.remove_at(3).unwrap();
    }
    crate::cow::page_header::stamp_checksum(&mut page);
    assert!(verify_checksum(&page), "fixture page must start valid");

    let before = page;
    let entries_before = Node::new(&page).leaf_entries().unwrap();

    // An insert far too large to fit even once the garbage is reclaimed: compaction runs, then the
    // retry still fails, so `insert_cell_at` returns false with the page already rewritten.
    let big = node::leaf_cell(b"zzzzzz", &vec![b'Z'; node::MAX_ENTRY_BYTES - 32]);
    let refused = {
        let mut n = NodeMut::new(&mut page);
        let at = n.count();
        n.insert_cell_at(at, &big).unwrap()
    };
    assert!(!refused, "fixture: the insert had to be refused");

    let entries_after = Node::new(&page).leaf_entries().unwrap();
    assert_eq!(entries_before, entries_after, "logical content must be unchanged");

    let changed = before.iter().zip(page.iter()).filter(|(a, b)| a != b).count();
    let valid = verify_checksum(&page);
    println!("A6 bytes changed by a REFUSED insert: {}", changed);
    println!("A6 checksum still valid after refusal: {}", valid);
    assert!(
        changed == 0 || valid,
        "AXIS 4 CONFIRMED (leaf_put): a refused insert rewrote {} page bytes (compaction) and \
         left the checksum stale. btree::leaf_put:489-496 drops the write guard on this path \
         without restamping, then calls alloc_for/read_page at :521-522 before rewriting it.",
        changed
    );
}

/// A7: the same window in `internal_relink`, where it needs no compaction argument at all. When a
/// k-way promotion only partly fits, the loop leaves `placed` separators **actually inserted** and
/// falls through to `entries`/`balanced_cuts` with the guard dropped and no restamp.
#[test]
fn a7_a_partial_k_way_promotion_leaves_the_internal_page_unstamped() {
    let mut page = [0u8; PAGE_SIZE];
    NodeMut::new(&mut page).init();
    crate::cow::page_header::PageHeader::new(
        Epoch(1),
        crate::branch::types::ArenaId(0),
        PageType::BTreeInternal,
    )
    .write_to(&mut page);
    NodeMut::new(&mut page).set_leftmost(7);

    // Fill the internal node to refusal.
    let mut i = 0u32;
    loop {
        let k = format!("s{:08}", i).into_bytes();
        let cell = node::internal_cell(&k, 1000 + i);
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
    // Free a little space — enough for one long separator, not three.
    for _ in 0..3 {
        let mut n = NodeMut::new(&mut page);
        n.remove_at(0).unwrap();
    }
    crate::cow::page_header::stamp_checksum(&mut page);
    assert!(verify_checksum(&page), "fixture must start valid");
    let count_before = Node::new(&page).count();

    // This is `internal_relink`'s loop, verbatim in shape. Long separators, so the node runs out
    // partway through the promotion.
    let promoted: Vec<(Vec<u8>, u32)> = (0..3u32)
        .map(|j| (format!("s{:08}{}", 0, "z".repeat(40 + j as usize)).into_bytes(), 9000 + j))
        .collect();
    let mut placed = 0usize;
    {
        let mut n = NodeMut::new(&mut page);
        n.set_leftmost(77); // the `set_child`/`set_leftmost` at btree.rs:578-579
        for (j, (sep, right)) in promoted.iter().enumerate() {
            if !n.insert_cell_at(j, &node::internal_cell(sep, *right)).unwrap() {
                break;
            }
            placed = j + 1;
        }
    }
    // btree.rs:599-604: the guard drops here. No stamp_checksum on this path.
    let count_after = Node::new(&page).count();
    let valid = verify_checksum(&page);
    println!(
        "A7 placed {}/{} separators; count {} -> {}; checksum valid: {}",
        placed,
        promoted.len(),
        count_before,
        count_after,
        valid
    );
    assert!(
        placed == promoted.len() || valid,
        "AXIS 4 CONFIRMED (internal_relink): {} of {} separators were written into the page and \
         the leftmost pointer moved, then btree.rs:604 drops the write guard with a stale \
         checksum; btree.rs:624-625 then calls alloc_for/read_page once per cut before \
         btree.rs:632-634 restamps.",
        placed,
        promoted.len()
    );
}

// ================================================================================================
// AXIS 5 — what the fanout change costs elsewhere
// ================================================================================================

/// A8: there is no leaf merge, so a delete-heavy branch keeps every leaf it ever created. At ~15
/// rows per leaf instead of ~68 that floor is 4.5x lower.
#[test]
fn a8_deletes_leave_the_leaf_count_where_it_was() {
    let set = pairs(1500);
    let f = Fixture::new();
    let mut root = f.build(&set);
    let full = f.leaf_partition(root);
    let entry = node::leaf_entry_bytes(&set[0].0, &set[0].1);
    println!(
        "A8 after build : {} leaves, {:.2} rows/leaf, {:.1}% page utilisation",
        full.len(),
        1500.0 / full.len() as f64,
        1500.0 * entry as f64 / (full.len() * node::NODE_CAPACITY) as f64 * 100.0
    );

    // Delete nine rows in ten.
    let mut kept = 0usize;
    for (i, (k, _)) in set.iter().enumerate() {
        if i % 10 == 0 {
            kept += 1;
            continue;
        }
        root = f.tree.delete(root, BranchId::TRUNK, f.tick(), k).unwrap();
    }
    let sparse = f.leaf_partition(root);
    let empty = sparse.iter().filter(|l| l.is_empty()).count();
    println!(
        "A8 after deleting 90% : {} leaves for {} rows, {:.2} rows/leaf, {:.2}% page utilisation, \
         {} empty leaves",
        sparse.len(),
        kept,
        kept as f64 / sparse.len() as f64,
        kept as f64 * entry as f64 / (sparse.len() * node::NODE_CAPACITY) as f64 * 100.0,
        empty
    );

    let remaining: Vec<(Vec<u8>, Vec<u8>)> =
        set.iter().enumerate().filter(|(i, _)| i % 10 == 0).map(|(_, p)| p.clone()).collect();
    let want = canonical(&remaining);
    println!("A8 chunker's partition of what is left: {} leaves", want.len());
    assert_eq!(
        sparse.len(),
        want.len(),
        "AXIS 5: after deleting 90% the tree holds {} leaves where the content needs {}",
        sparse.len(),
        want.len()
    );
}

// ================================================================================================
// A9 — reproducing cid.rs's delete diagnosis, then changing only the delete SHAPE
// ================================================================================================

/// `cid::tests::a_delete_leaves_empty_leaves_behind_and_breaks_the_partition` (main, 1758b3b)
/// asserts, as a load-bearing diagnosis, that after a delete "the live partition is exact, and the
/// whole difference is empty leaves":
///
/// ```text
///   assert_eq!(ordered_leaf_entries(churned).filter(!is_empty), ordered_leaf_entries(clean),
///       "the SURVIVING leaves must already agree — if they do not, the gap is in the boundary
///        rule and not merely in unreclaimed empty leaves, and the diagnosis above is wrong");
/// ```
///
/// Its fixture deletes keys 500..1000 — a contiguous **suffix**. This probe runs that shape
/// first (to reproduce their result), then changes nothing but the delete shape.
#[test]
fn a9_the_delete_diagnosis_holds_only_for_a_suffix_delete() {
    // cid.rs's own key/value shape.
    let ck = |n: u32| n.to_be_bytes().to_vec();
    let cv = |n: u32| format!("v{n}").into_bytes();
    let all: Vec<(Vec<u8>, Vec<u8>)> = (0..1000u32).map(|i| (ck(i), cv(i))).collect();

    // Their claim, stated as a closure so both shapes are judged by the identical predicate.
    let surviving_leaves_agree = |churned: &[Vec<(Vec<u8>, Vec<u8>)>],
                                  clean: &[Vec<(Vec<u8>, Vec<u8>)>]| {
        let live: Vec<_> = churned.iter().filter(|l| !l.is_empty()).cloned().collect();
        live == clean.to_vec()
    };

    // ---- SHAPE S: their fixture, a contiguous suffix -----------------------------------------
    let fs = Fixture::new();
    let mut churned_s = fs.build(&all);
    for i in 500..1000u32 {
        churned_s = fs.tree.delete(churned_s, BranchId::TRUNK, fs.tick(), &ck(i)).unwrap();
    }
    let churned_s_part = fs.leaf_partition(churned_s);
    let gs = Fixture::new();
    let clean_s: Vec<(Vec<u8>, Vec<u8>)> = (0..500u32).map(|i| (ck(i), cv(i))).collect();
    let clean_s_part = gs.leaf_partition(gs.build(&clean_s));
    let s_holds = surviving_leaves_agree(&churned_s_part, &clean_s_part);
    println!(
        "A9 SHAPE S (suffix 500..1000): churned {} leaves ({} empty), clean {} leaves -> \
         surviving leaves agree: {}",
        churned_s_part.len(),
        churned_s_part.iter().filter(|l| l.is_empty()).count(),
        clean_s_part.len(),
        s_holds
    );

    // ---- SHAPE I: one interior key that terminates a leaf ------------------------------------
    let fi = Fixture::new();
    let mut churned_i = fi.build(&all);
    let base = fi.leaf_partition(churned_i);
    // Pick a leaf well inside the tree whose last entry is a content boundary.
    let idx = base.len() / 2;
    let (vk, vv) = base[idx].last().unwrap().clone();
    assert!(chunker::is_boundary(&vk, &vv), "fixture: leaf {} must end on a boundary", idx);
    churned_i = fi.tree.delete(churned_i, BranchId::TRUNK, fi.tick(), &vk).unwrap();
    let churned_i_part = fi.leaf_partition(churned_i);

    let clean_i: Vec<(Vec<u8>, Vec<u8>)> =
        all.iter().filter(|(kk, _)| kk != &vk).cloned().collect();
    let gi = Fixture::new();
    let clean_i_part = gi.leaf_partition(gi.build(&clean_i));
    let i_holds = surviving_leaves_agree(&churned_i_part, &clean_i_part);
    println!(
        "A9 SHAPE I (one interior boundary key): churned {} leaves ({} empty), clean {} leaves \
         -> surviving leaves agree: {}",
        churned_i_part.len(),
        churned_i_part.iter().filter(|l| l.is_empty()).count(),
        clean_i_part.len(),
        i_holds
    );

    // Control: both sides really do hold the same rows, or nothing above measures shape.
    let flat_i: Vec<_> = churned_i_part.iter().flatten().cloned().collect();
    assert_eq!(flat_i, clean_i, "control: the two trees must hold the same rows");

    if !i_holds {
        let live: Vec<_> = churned_i_part.iter().filter(|l| !l.is_empty()).collect();
        let at = live
            .iter()
            .zip(clean_i_part.iter())
            .position(|(a, b)| **a != *b)
            .expect("they differ, so a first difference exists");
        println!(
            "A9 first disagreeing SURVIVING leaf is #{}: churned holds {} rows, clean holds {} \
             rows (neither is empty)",
            at,
            live[at].len(),
            clean_i_part[at].len()
        );
        println!(
            "A9   churned live leaf sizes {:?}",
            live.iter().skip(at.saturating_sub(1)).take(4).map(|l| l.len()).collect::<Vec<_>>()
        );
        println!(
            "A9   clean   leaf sizes {:?}",
            clean_i_part.iter().skip(at.saturating_sub(1)).take(4).map(|l| l.len()).collect::<Vec<_>>()
        );
    }

    assert!(s_holds, "could not reproduce cid.rs's result on its own suffix fixture");
    assert!(
        i_holds,
        "AXIS 2 — cid.rs's diagnosis is scope-limited: it holds for a suffix delete (reproduced \
         above) and fails for a single interior boundary key. The disagreeing leaves are NOT \
         empty, so by that test's own wording 'the gap is in the boundary rule and not merely in \
         unreclaimed empty leaves', and the pre-registered fix it points at (unlink emptied \
         leaves) cannot close it."
    );
}
