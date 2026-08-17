//! E70 — the missing B+tree rebalance, measured: it cannot fire, and that is the actual problem.
//!
//! # The row's premise was half wrong
//!
//! `BPlusTreeManager::handle_underflow` is `todo!()` and has never been called. E62 recorded it as a
//! known limitation, and E70 was opened on the theory that E63 had made it urgent: the primary-key
//! uniqueness fix calls `primary_index.delete` on every key reuse, so deletes were said to be routine
//! now, and a `todo!()` on a routine path is a process abort waiting to happen.
//!
//! Measured, it is the reverse. **Nothing net-removes a key from a B+tree, so no leaf ever becomes
//! underfull and the `todo!()` is unreachable.** Every removal in the tree is one half of a pair:
//!
//! - `execution::delete` holds a `primary_index` field and never calls `delete` on it at all. A SQL
//!   `DELETE` stamps `end_ts` on the version and leaves the index entry pointing at it, because an
//!   older snapshot still has to find the row.
//! - `execution::insert` removes an entry only to put the same key straight back — that is what E63's
//!   key reuse *is*.
//! - `execution::update` removes and re-adds the same key when a row's `RecordId` moves.
//!
//! So the `todo!()` is safe, and it is safe for a reason nobody chose: it is protected by the fact that
//! the index never shrinks. That is not a rebalance being unnecessary — it is the same debt as E66's
//! secondary entries, on the primary index, and it is what this file measures instead.
//!
//! # What the debt actually costs
//!
//! An entry per deleted row, kept forever. A point lookup is unaffected: it still descends the tree and
//! the visibility check drops the dead version. A **range scan pays for every dead entry it walks**,
//! and that is the number worth having, because a delete-heavy table quietly makes every scan through
//! its index slower with no signal anywhere.

use std::ops::Bound;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("debt.db");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("debt.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: dir, catalog, bp, txn, session: Session::new() }
}

/// One leaf's worth of the primary index, as (leaf count, total entries, min/max keys per leaf).
struct LeafStats {
    leaves: usize,
    entries: usize,
    min_keys: u16,
    max_keys: u16,
}

impl Db {
    fn sql(&mut self, sql: &str) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn rows(&mut self, sql: &str) -> usize {
        match self.sql(sql) {
            Outcome::Rows(r) => r.len(),
            other => panic!("expected rows from `{sql}`, got {:?}", std::mem::discriminant(&other)),
        }
    }

    /// Walk the primary index's leaf chain and report what is in it.
    ///
    /// The chain rather than a range scan, because a range scan reports what it chose to yield and this
    /// has to report what is actually stored - including entries no query would ever return.
    fn leaf_stats(&self, table: &str) -> LeafStats {
        let entry = self.catalog.get_table(table).expect("table");
        let tree = BPlusTreeManager::<Value, RecordId>::open(
            entry.primary_index_root,
            self.bp.clone(),
        );
        let mut leaf = tree.leftmost_leaf().expect("leftmost leaf");
        let mut leaves = 0usize;
        let mut entries = 0usize;
        let mut min_keys = u16::MAX;
        let mut max_keys = 0u16;
        loop {
            leaves += 1;
            entries += leaf.num_keys as usize;
            min_keys = min_keys.min(leaf.num_keys);
            max_keys = max_keys.max(leaf.num_keys);
            match leaf.next {
                Some(next) => leaf = tree.read_leaf(next).expect("next leaf"),
                None => break,
            }
        }
        LeafStats { leaves, entries, min_keys, max_keys }
    }

    /// How many index entries a full scan of the primary index has to walk.
    fn entries_walked(&self, table: &str) -> usize {
        let entry = self.catalog.get_table(table).expect("table");
        let tree = BPlusTreeManager::<Value, RecordId>::open(
            entry.primary_index_root,
            self.bp.clone(),
        );
        tree.range_scan(Bound::Unbounded, Bound::Unbounded).expect("scan").count()
    }
}

fn seeded(n: i32) -> Db {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
    for i in 1..=n {
        d.sql(&format!("INSERT INTO t VALUES ({i}, {});", i * 10));
    }
    d
}

/// **The measurement: a DELETE removes no index entry, so the index never shrinks.**
#[test]
fn deleting_rows_does_not_shrink_the_primary_index() {
    // 400, because a leaf splits at ~370 entries and this must span more than one leaf for the leaf
    // accounting below to mean anything. Measured: n=400 -> 2 leaves (min 185, max 215); n=2000 -> 10.
    const N: i32 = 400;
    const KEPT: i32 = 50;

    let mut d = seeded(N);
    let before = d.leaf_stats("t");
    assert_eq!(before.entries, N as usize, "the seed did not index every row: {}", before.entries);
    assert!(before.leaves > 1, "the whole index fits in one leaf, so leaf accounting is vacuous");

    for i in (KEPT + 1)..=N {
        d.sql(&format!("DELETE FROM t WHERE id = {i};"));
    }

    // Anti-vacuity: the deletes really happened, as far as any query can tell.
    let live = d.rows("SELECT * FROM t;");
    assert_eq!(live, KEPT as usize, "the deletes did not take effect: {live} rows still visible");

    let after = d.leaf_stats("t");
    assert_eq!(
        after.entries, N as usize,
        "the index changed size across 350 deletes ({} -> {}). If it now shrinks, this file's premise \
         is out of date and `handle_underflow` may have become reachable.",
        before.entries, after.entries
    );
    assert_eq!(
        after.leaves, before.leaves,
        "the leaf count changed without any entry being removed"
    );
    assert_eq!(
        after.min_keys, before.min_keys,
        "a leaf's occupancy dropped, which is the condition `handle_underflow` exists for - and it is \
         `todo!()`, so reaching it aborts the process"
    );

    // **The cost, stated as a number.** A full index scan walks every dead entry.
    let walked = d.entries_walked("t");
    assert_eq!(walked, N as usize, "the scan did not walk the dead entries: {walked}");
    let amplification = walked / live;
    assert_eq!(
        amplification, 8,
        "400 entries walked to return 50 live rows is 8x read amplification; got {amplification}x from \
         {walked} entries and {live} rows"
    );
}

/// **Why `handle_underflow` cannot fire: every removal is half of a pair.**
///
/// The two production callers of `BPlusTreeManager::delete` both put the same key straight back - E63's
/// key reuse in `execution::insert`, and `execution::update` when a row's `RecordId` moves. This drives
/// both, hard, and requires the leaf occupancy never to dip.
///
/// If this test ever fails, the `todo!()` has become reachable and a rebalance is no longer optional.
#[test]
fn no_workload_drives_a_leaf_underfull() {
    const N: i32 = 400;
    let mut d = seeded(N);
    let before = d.leaf_stats("t");

    // Reuse every key: delete then insert the same id. Each pair calls `primary_index.delete` followed
    // by an insert of the identical key, which is the only removal path SQL can reach.
    for i in 1..=N {
        d.sql(&format!("DELETE FROM t WHERE id = {i};"));
        d.sql(&format!("INSERT INTO t VALUES ({i}, {});", i * 100));
    }
    // And move every row's version, which is `update`'s remove-and-re-add path.
    for i in 1..=N {
        d.sql(&format!("UPDATE t SET v = {} WHERE id = {i};", i * 1000));
    }

    let after = d.leaf_stats("t");
    assert_eq!(
        after.entries, before.entries,
        "the entry count moved ({} -> {}) under a reuse-heavy workload; a net removal is possible \
         after all and `handle_underflow` is reachable",
        before.entries, after.entries
    );
    assert!(
        after.min_keys >= before.min_keys,
        "leaf occupancy fell from {} to {} keys - `handle_underflow` is now reachable and it is \
         `todo!()`",
        before.min_keys,
        after.min_keys
    );

    // Anti-vacuity: the workload really ran and really changed the data, so the stability above is a
    // fact about the index rather than about nothing having happened.
    assert_eq!(d.rows("SELECT * FROM t;"), N as usize, "the reuse workload lost rows");
    match d.sql("SELECT * FROM t WHERE id = 7;") {
        Outcome::Rows(r) => assert_eq!(
            r[0][1],
            Value::Integer(7000),
            "row 7 does not carry the value the workload last wrote: {:?}",
            r[0]
        ),
        other => panic!("expected rows, got {:?}", std::mem::discriminant(&other)),
    }
    assert!(after.max_keys > 0 && after.leaves > 1, "the index is empty or single-leaf: vacuous");
}
