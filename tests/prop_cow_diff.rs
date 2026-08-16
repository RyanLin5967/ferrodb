//! D12 — `CowTree::diff` against a naive reference, over generated edit sets.
//!
//! `diff` is an optimisation: it skips whole subtrees the two roots share by page id, on the
//! reasoning that no content addressing and no refcounts means an unchanged subtree *is* the same
//! page. That reasoning is sound, and it is exactly the kind of reasoning that is sound right up
//! until an edit pattern nobody thought of.
//!
//! So the property is the one that matters for any optimisation: **it must agree with the obvious
//! implementation**. The reference here reads every entry from both roots with a full range scan
//! and compares them key by key, which is the answer `diff` is claiming to compute more cheaply.
//! Nothing about page identity, sharing, or pruning appears in the reference — that is the point.

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::*;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;

const ARENA_BASE: u32 = 1024;

/// One edit applied on the forked branch.
#[derive(Debug, Clone)]
enum Edit {
    Put(u32, u8),
    Delete(u32),
}

fn edits() -> impl Strategy<Value = Vec<Edit>> {
    prop::collection::vec(
        prop_oneof![
            (0u32..64, 0u8..255).prop_map(|(k, v)| Edit::Put(k, v)),
            (0u32..64).prop_map(Edit::Delete),
        ],
        0..24,
    )
}

fn key(n: u32) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// The obvious implementation: read everything from both sides and compare.
fn naive_diff(t: &CowTree, base: u32, head: u32) -> Vec<(Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>)> {
    let collect = |root: u32| -> BTreeMap<Vec<u8>, Vec<u8>> {
        t.range_scan(root, None, None).unwrap().into_iter().collect()
    };
    let (b, h) = (collect(base), collect(head));
    let mut keys: Vec<&Vec<u8>> = b.keys().chain(h.keys()).collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter_map(|k| {
            let (x, y) = (b.get(k), h.get(k));
            (x != y).then(|| (k.clone(), x.cloned(), y.cloned()))
        })
        .collect()
}

struct Fixture {
    _dir: tempfile::TempDir,
    catalog: Arc<LogBranchCatalog>,
    tree: CowTree,
}

fn fixture(tag: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), ARENA_BASE).unwrap());
    let tree = CowTree::new(store as Arc<dyn PageStore>);
    Fixture { _dir: dir, catalog, tree }
}

/// Build a trunk of `n` keys, fork, apply `edits` on the child, return both roots.
///
/// Edits must happen on a FORKED branch. `cow_page` mutates in place while a page is private to
/// the writing arena, so editing trunk straight after filling trunk rewrites one tree rather than
/// producing two versions — and every diff would then be trivially empty.
fn build(f: &Fixture, n: u32, edits: &[Edit]) -> (u32, u32) {
    let e0 = f.catalog.next_epoch();
    let mut base = f.tree.create(BranchId::TRUNK, e0).unwrap();
    for i in 0..n {
        base = f
            .tree
            .insert(base, BranchId::TRUNK, e0, &key(i), &[i as u8])
            .unwrap();
    }

    let child = f
        .catalog
        .fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000))
        .unwrap()
        .branch_id;
    let e1 = f.catalog.next_epoch();

    let mut head = base;
    for ed in edits {
        head = match ed {
            Edit::Put(k, v) => f.tree.insert(head, child, e1, &key(*k), &[*v]).unwrap(),
            Edit::Delete(k) => f.tree.delete(head, child, e1, &key(*k)).unwrap(),
        };
    }
    (base, head)
}

proptest! {
    // File-backed trees make each case relatively expensive, so the case count is lowered rather
    // than the trees being shrunk to a size where sharing cannot happen and the pruning path is
    // never taken.
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// The load-bearing property: the pruning diff equals the full-scan diff, always.
    #[test]
    fn diff_agrees_with_a_full_scan_comparison(n in 20u32..64, edits in edits()) {
        let f = fixture("agree");
        let (base, head) = build(&f, n, &edits);

        let fast = f.tree.diff(base, head).unwrap().deltas;
        let slow = naive_diff(&f.tree, base, head);

        prop_assert_eq!(
            fast, slow,
            "the pruning diff disagreed with a full scan after {:?}", edits
        );
    }

    /// Reversing the roots must invert every delta, not merely produce the same count.
    #[test]
    fn reversing_the_roots_inverts_every_delta(n in 20u32..48, edits in edits()) {
        let f = fixture("reverse");
        let (base, head) = build(&f, n, &edits);

        let fwd = f.tree.diff(base, head).unwrap().deltas;
        let rev = f.tree.diff(head, base).unwrap().deltas;

        prop_assert_eq!(fwd.len(), rev.len());
        for (k, before, after) in fwd {
            let found = rev.iter().find(|(rk, _, _)| *rk == k);
            prop_assert!(found.is_some(), "key missing when reversed");
            let (_, rb, ra) = found.unwrap();
            prop_assert_eq!(rb, &after, "reversed 'before' should be the forward 'after'");
            prop_assert_eq!(ra, &before, "reversed 'after' should be the forward 'before'");
        }
    }

    /// Applying the diff to the base must reconstruct the head exactly. A diff you cannot replay
    /// is not a changeset, and this is the property a merge actually depends on.
    #[test]
    fn applying_the_diff_to_the_base_reconstructs_the_head(n in 20u32..48, edits in edits()) {
        let f = fixture("apply");
        let (base, head) = build(&f, n, &edits);

        let mut rebuilt: BTreeMap<Vec<u8>, Vec<u8>> =
            f.tree.range_scan(base, None, None).unwrap().into_iter().collect();
        for (k, _before, after) in f.tree.diff(base, head).unwrap().deltas {
            match after {
                Some(v) => rebuilt.insert(k, v),
                None => rebuilt.remove(&k),
            };
        }

        let actual: BTreeMap<Vec<u8>, Vec<u8>> =
            f.tree.range_scan(head, None, None).unwrap().into_iter().collect();
        prop_assert_eq!(rebuilt, actual, "replaying the diff did not reproduce the head");
    }

    /// No edits means no deltas and, because the roots are identical, nothing read at all.
    #[test]
    fn an_unedited_branch_diffs_to_nothing_for_free(n in 20u32..64) {
        let f = fixture("noedit");
        let (base, head) = build(&f, n, &[]);
        prop_assert_eq!(base, head, "an edit-free branch moved its root");

        let d = f.tree.diff(base, head).unwrap();
        prop_assert!(d.deltas.is_empty());
        prop_assert_eq!(d.pages_examined, 0, "an unchanged branch cost pages to diff");
    }

    /// Pruning must actually prune. Without this the other properties would still pass on a diff
    /// that quietly read both trees in full — correct, and pointless.
    ///
    /// The key count is large deliberately. The first version used 40 keys, which fit on ONE page:
    /// with a single-page tree the root is the leaf, editing it copies it, and neither side shares
    /// anything, so the diff reads 2 pages out of 1 and the claim is not false but meaningless.
    /// A tree has to have interior structure before "skips shared subtrees" can be tested at all.
    #[test]
    fn a_small_edit_reads_less_than_reading_both_trees(n in 1200u32..2000, k in 0u32..1000) {
        let f = fixture("prune");
        // The value must differ from what is already there. Writing the literal 200 was a latent
        // flake: `build` stores `[i as u8]`, and 456 as u8 IS 200, so that "edit" rewrote the
        // existing value and correctly produced zero deltas. Derived from the key instead, so it
        // is different for every k.
        let (base, head) = build(&f, n, &[Edit::Put(k, (k as u8).wrapping_add(7))]);

        let total = f.tree.walk_pages(base).unwrap().len();
        prop_assume!(total >= 4); // below this there is no sharing to exploit

        let d = f.tree.diff(base, head).unwrap();
        prop_assert_eq!(d.deltas.len(), 1);
        prop_assert!(
            d.pages_examined < total,
            "examined {} pages for a one-row edit in a {total}-page tree; reading either tree in \
             full would already be {total}, so nothing was pruned",
            d.pages_examined
        );
    }
}
