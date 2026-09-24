//! D230 — **a write statement that fails after moving a root must still record the new root.**
//!
//! # The defect, at `9aa6968`
//!
//! A B+tree root split is published into the tree's shared cell the moment it happens, and index
//! pages are not logged, so the split is permanent whatever the statement goes on to return.
//! `Insert::execute` and `Update::execute` ran `sync_roots` / `sync_fulltext_roots` only as their
//! last lines. Any `?` between the write that split a root and those lines skipped them:
//!
//! - INSERT: the primary `upsert` splits, then a secondary `insert` or `post_tokens` fails, e.g. the
//!   allocator refusing at the arena floor;
//! - UPDATE: an earlier row's secondary `insert` splits, then a LATER row is refused, e.g. a NOT
//!   NULL violation;
//! - `sync_roots` itself returned on the first failed `persist`, before recording the indexes after
//!   it.
//!
//! The in-memory record then lags the tree, and every later `persist` of ANY table writes the
//! lagging value to the catalog page. An open that does not rebuild reads it back. A clean restart
//! after a process that ran no DDL leaves an empty log, `recover` returns `false`, and `run_cli`
//! rebuilds nothing. The shared cell is then seeded with the pre-split root, which is now only the
//! left half of the tree, and lookups and writes descend from there (see "What D looks for").
//!
//! # The schedule each end-to-end test runs, one process per phase
//!
//! - **A.** DDL and rows, then a checkpoint. CREATE TABLE is logged and retained, so the log is
//!   not empty when A exits.
//! - **B.** Open (recovers, so it rebuilds), checkpoint, close. B ran no DDL, so its checkpoint
//!   leaves an EMPTY log.
//! - **C.** Open (premise: `recover` returns false, no rebuild). Run the statement that splits a
//!   root and then fails. Checkpoint.
//! - **D.** Open (premise again: no rebuild). The catalog record is whatever C persisted.
//!
//! Each open is `run_cli`'s own sequence at `9aa6968` (`recover`, `Catalog::open`, and
//! `rebuild_indexes` + checkpoint only if `recover` said so). `wal::recovery::open_recovered`,
//! which later lanes consolidate that sequence into, does not exist at `9aa6968`, and a red test
//! must compile there.
//!
//! # What D looks for, and why a lookup's answer depends on what is in memory
//!
//! At `9aa6968` the stale root D reads back is the LEFT LEAF of the split tree.
//!
//! - **The first lookup, on a cold pool.** `read_leaf_for` tries the lock-free descent first, and
//!   that reads only pages already in the buffer pool (`read_page_optimistic` returns `None`
//!   otherwise). D's pool is fresh, so all 16 restarts miss and the latched descent answers. It
//!   stops at the first leaf it reaches, the stale root, and does NOT walk right. The scan opens at
//!   the end of the left leaf, follows `next`, and yields every key from the right leaf's first up
//!   to the one asked for, because nothing re-checks an inclusive lower bound. `id = 369` answers
//!   184 rows. This is the first assertion to fire at the base.
//! - **The same lookup once the pages are in memory** would be answered right: the lock-free walk
//!   steps one leaf right. So D also WRITES and looks again, and that failure does not depend on
//!   residency. An INSERT descends from the stale root too, so a key larger than every other lands
//!   in the left leaf. That leaf then ends above everything in the right leaf, both descents stop
//!   there, and a key in the right half is neither found nor refused as a duplicate.
//!
//! # Fixture arithmetic, from source
//!
//! Primary entries are `Integer` + `RecordId` = 5 + 6 bytes. Secondary `('aaa', Integer)` entries
//! are 6 + 5. Both come to 11. A leaf is full once `27 + 11n >= 4096`: 369 entries (4086 bytes)
//! fit, and the 370th fills it, so row 370's INSERT splits both roots. The tests assert that
//! geometry before relying on it.

use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

use ferrodb::binder::binder::Binder;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, sync_fulltext_roots, sync_roots, Outcome};
use ferrodb::execution::index_handle::{FullTextHandle, IndexHandle};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::{explain_plan, optimize, pushdown};
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::{BPlusTreePage, BTreeSerialize};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::{rebuild_indexes, recover};
use ferrodb::wal::txn::TxnManager;

/// Rows before the trigger: each tree's single leaf is one entry short of full.
const FULL_LESS_ONE: i32 = 369;
/// The row whose INSERT splits the primary root and is then refused.
const TRIGGER: i32 = 370;
/// A key in the right half after the split: the split keeps `370 / 2 = 185` entries on the left.
const RIGHT_KEY: i32 = 369;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

/// Open `dir` exactly as `cli::run_cli` does at `9aa6968`, and say whether it rebuilt.
fn open(dir: &Path) -> (Db, bool) {
    let path = dir.join("d230.db");
    let existed = path.exists();
    let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.join("d230.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let recovered = recover(&txn).expect("recover");
    let mut catalog = if existed {
        Catalog::open(bp.clone(), 1).expect("open the catalog")
    } else {
        Catalog::create(bp.clone()).expect("create the catalog")
    };
    if recovered {
        rebuild_indexes(&mut catalog, &bp).expect("rebuild the indexes");
        txn.checkpoint().expect("checkpoint after the rebuild");
    }
    (Db { catalog, bp, txn, session: Session::new() }, recovered)
}

/// A clean exit: checkpoint, then drop every handle.
fn close(d: Db) {
    d.txn.checkpoint().expect("the clean exit's checkpoint");
    drop(d);
}

/// Phases A and B: build with `ddl_and_rows`, then restart once so the log A's DDL left behind is
/// replayed and rebuilt, and the NEXT open finds it empty.
fn built_then_restarted(dir: &Path, ddl_and_rows: impl FnOnce(&mut Db)) {
    let (mut a, _) = open(dir);
    ddl_and_rows(&mut a);
    close(a);
    let (b, _) = open(dir);
    close(b);
}

/// An open that must NOT rebuild, because the defect needs the record to be read back as written.
fn open_without_rebuild(dir: &Path, phase: &str) -> Db {
    let (d, recovered) = open(dir);
    assert!(
        !recovered,
        "premise failed: phase {phase}'s open found a log to replay and rebuilt every index from \
         the heap, which would hide a lagging record. The previous process must end with an \
         empty log (no DDL, a clean checkpoint)."
    );
    d
}

impl Db {
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        assert_eq!(stmts.len(), 1, "one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
    }

    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn ids(&mut self, sql: &str) -> Vec<i32> {
        match self.sql(sql) {
            Outcome::Rows(rows) => rows
                .into_iter()
                .map(|r| match r[0] {
                    Value::Integer(i) => i,
                    ref other => panic!("expected an Integer id from `{sql}`, got {other:?}"),
                })
                .collect(),
            other => panic!("expected rows from `{sql}`, got {:?}", std::mem::discriminant(&other)),
        }
    }

    /// The physical plan the optimizer chooses, rendered. Builds no executor.
    fn explain(&self, sql: &str) -> String {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        let logical = Binder::new(&self.catalog).bind(stmts.remove(0)).expect("bind");
        let physical = optimize(pushdown(logical), &self.catalog).expect("optimize");
        explain_plan(&physical, &self.catalog)
    }

    fn primary_root(&self, table: &str) -> u32 {
        self.catalog.get_table(table).expect("table").primary_index_root
    }

    fn secondary_root(&self, table: &str, column: &str) -> u32 {
        self.catalog.get_table(table).expect("table").indexes.iter()
            .find(|i| i.column_name == column).expect("a B-tree index").root_page_id
    }

    fn fulltext_root(&self, table: &str, column: &str) -> u32 {
        self.catalog.get_table(table).expect("table").fulltext_indexes.iter()
            .find(|i| i.column_name == column).expect("a full-text index").root_page_id
    }
}

/// The tree's leaves, left to right, as `(page id, entries)`: from `root` down the leftmost path,
/// back along `prev` to the head, then along `next`. The chain does not depend on `root` being the
/// real root, only on it being a page of the tree.
fn leaf_chain<K, V>(bp: &Arc<BufferPoolManager>, root: u32) -> Vec<(u32, usize)>
where
    K: Ord + Clone + BTreeSerialize,
    V: Clone + BTreeSerialize + Ord,
{
    let tree = BPlusTreeManager::<K, V>::open(root, bp.clone());
    let mut leaf = tree.leftmost_leaf().expect("descend to a leaf");
    let mut steps = 0;
    while let Some(prev) = leaf.prev {
        steps += 1;
        assert!(steps < 100_000, "the leaf chain's prev pointers loop");
        leaf = tree.read_leaf(prev).expect("read a leaf");
    }
    let mut chain = Vec::new();
    loop {
        chain.push((leaf.page_id, leaf.key_arr.len()));
        assert!(chain.len() < 100_000, "the leaf chain's next pointers loop");
        match leaf.next {
            Some(n) => leaf = tree.read_leaf(n).expect("read a leaf"),
            None => return chain,
        }
    }
}

/// The leaves a descent from `page` reaches, left to right.
fn leaves_under<K, V>(tree: &BPlusTreeManager<K, V>, page: u32, out: &mut Vec<u32>)
where
    K: Ord + Clone + BTreeSerialize,
    V: Clone + BTreeSerialize + Ord,
{
    match tree.read_node(page).expect("read a tree page") {
        BPlusTreePage::Leaf(_) => out.push(page),
        BPlusTreePage::Internal(node) => {
            for child in node.child_ptrs {
                leaves_under(tree, child, out);
            }
        }
    }
}

/// The recorded root must be the tree's real root: the one page whose descent reaches every leaf of
/// the chain, in chain order.
fn assert_record_is_the_root<K, V>(bp: &Arc<BufferPoolManager>, recorded: u32, what: &str)
where
    K: Ord + Clone + BTreeSerialize,
    V: Clone + BTreeSerialize + Ord,
{
    let chain: Vec<u32> = leaf_chain::<K, V>(bp, recorded).into_iter().map(|(p, _)| p).collect();
    let mut reached = Vec::new();
    leaves_under(&BPlusTreeManager::<K, V>::open(recorded, bp.clone()), recorded, &mut reached);
    assert!(
        reached == chain,
        "{what}: the persisted root, page {recorded}, is not the tree's root. A descent from it \
         reaches {} of the {} leaves{}.",
        reached.len(),
        chain.len(),
        if chain.first() == Some(&recorded) { "; it is the leftmost leaf, the pre-split root" } else { "" }
    );
}

/// Leave the page allocator exactly two pages: reserve an arena floor just above the high-water
/// mark, take every page below it, and give back two. Two is what a leaf root's split takes (the
/// new right leaf, then the new root). The next page anyone asks for is refused.
fn leave_exactly_two_free_pages(bp: &BufferPoolManager) {
    let floor = bp.disk_manager.high_water().expect("high-water mark") + 2;
    bp.disk_manager.reserve_from(floor).expect("reserve the arena floor");
    let mut taken = Vec::new();
    let refusal = loop {
        match bp.new_page() {
            Ok(p) => taken.push(p),
            Err(e) => break e.to_string(),
        }
        assert!(taken.len() < 100_000, "the allocator never refused below floor {floor}");
    };
    assert!(
        refusal.contains("the reserved arena region at page"),
        "premise failed: the allocator stopped for some other reason than the floor: {refusal}"
    );
    assert!(taken.len() >= 2, "premise failed: only {} page(s) below floor {floor}", taken.len());
    for page in taken.iter().rev().take(2) {
        bp.free_page(*page).expect("give a page back below the floor");
    }
}

/// An INSERT whose primary `upsert` splits the root and whose secondary `insert` is then refused
/// still records the primary's new root, so a later open that does not rebuild reads the real root.
///
/// At `9aa6968` phase D's first lookup of row 369 answers rows 186..=369 (see the module doc).
#[test]
fn an_insert_refused_after_its_primary_root_split_still_records_the_new_root() {
    let dir = tempfile::tempdir().unwrap();
    built_then_restarted(dir.path(), |a| {
        a.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(10));");
        a.sql("CREATE INDEX iv ON t (v);");
        for i in 1..=FULL_LESS_ONE {
            a.sql(&format!("INSERT INTO t VALUES ({i}, 'aaa');"));
        }
    });

    // ---- C: the INSERT that splits the primary root and is then refused ------------------------
    let mut c = open_without_rebuild(dir.path(), "C");
    for (what, chain) in [
        ("primary", leaf_chain::<Value, RecordId>(&c.bp, c.primary_root("t"))),
        ("t.v", leaf_chain::<(Value, Value), ()>(&c.bp, c.secondary_root("t", "v"))),
    ] {
        assert_eq!(
            chain.iter().map(|(_, n)| *n).collect::<Vec<_>>(),
            vec![FULL_LESS_ONE as usize],
            "premise failed: the {what} tree must be ONE leaf of {FULL_LESS_ONE} entries, so that \
             row {TRIGGER} splits its root"
        );
    }
    let root_before = c.primary_root("t");
    leave_exactly_two_free_pages(&c.bp);
    let refused = c
        .try_sql(&format!("INSERT INTO t VALUES ({TRIGGER}, 'aaa');"))
        .err()
        .expect("premise failed: the INSERT must be refused once its secondary split needs a page");
    assert!(
        refused.to_string().contains("the reserved arena region at page"),
        "premise failed: refused for another reason than the exhausted allocator: {refused}"
    );
    // The ROOT moved, not merely the leaf: if the heap had taken one of the two pages, the leaf
    // split would have taken the other and the new root's allocation would have been refused,
    // leaving two leaves under an unmoved root. The shared cell is where a published root lives.
    let root_now = c
        .catalog
        .root_cell("t", None)
        .expect("t's primary index has a shared root cell")
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        root_now != root_before
            && matches!(
                BPlusTreeManager::<Value, RecordId>::open(root_now, c.bp.clone()).read_node(root_now),
                Ok(BPlusTreePage::Internal(_))
            ),
        "premise failed: the primary root did not move to a new internal page before the refusal \
         (was {root_before}, now {root_now})"
    );
    let primary_after = leaf_chain::<Value, RecordId>(&c.bp, root_now);
    assert_eq!(primary_after.len(), 2, "premise failed: one root split makes two leaves: {primary_after:?}");
    assert_eq!(
        leaf_chain::<(Value, Value), ()>(&c.bp, c.secondary_root("t", "v")).len(),
        1,
        "premise failed: the secondary split was supposed to be refused before it wrote anything"
    );
    close(c);

    // ---- D: an open that reads the record back ------------------------------------------------
    let mut d = open_without_rebuild(dir.path(), "D");
    let lookup = format!("SELECT id FROM t WHERE id = {RIGHT_KEY};");
    let plan = d.explain(&lookup);
    assert!(
        plan.contains("Index scan on t (col 0, [") && !plan.contains("Filter") && !plan.contains("Sequential scan"),
        "premise failed: `{lookup}` must be a bare primary index scan. Plan:\n{plan}"
    );
    let got = d.ids(&lookup);
    assert!(
        got == vec![RIGHT_KEY],
        "right after the reopen, on a cold pool, `id = {RIGHT_KEY}` must answer exactly that row; it \
         answered {} rows, starting {:?}",
        got.len(),
        &got[..got.len().min(5)]
    );

    d.sql("INSERT INTO t VALUES (371, 'aaa');");
    assert_eq!(
        d.ids(&lookup),
        vec![RIGHT_KEY],
        "row {RIGHT_KEY}, in the right half of the split, must still be found by its key after an \
         INSERT of a larger key"
    );
    let dup = d.try_sql(&format!("INSERT INTO t VALUES ({RIGHT_KEY}, 'aaa');"));
    assert!(
        dup.as_ref().is_err_and(|e| e.to_string().contains("duplicate primary key")),
        "a second row with primary key {RIGHT_KEY} must be refused as a duplicate; got {:?}",
        dup.map(|_| "accepted")
    );
    assert_record_is_the_root::<Value, RecordId>(&d.bp, d.primary_root("t"), "t's primary index");
}

/// An UPDATE whose earlier rows split a secondary root, and whose later row is refused, still
/// records the secondary's new root.
///
/// `SET v = w` over rows 1..=250 in heap order: rows 1..=100 have a `w`, so each posts a new entry
/// `('bNNNN', id)` and the 63rd fills the leaf (250 + 63 = 313 entries of 13 bytes, 27 + 313·13 =
/// 4096) and splits the root, keeping `a0001..a0156` on the left. Row 101's `w` is NULL and `v` is
/// NOT NULL, so the statement is refused there, after the split and before any sync. At `9aa6968`
/// phase D's first lookup of `a0200` answers rows 157..=200 (see the module doc).
#[test]
fn an_update_refused_on_a_later_row_still_records_the_root_an_earlier_row_split() {
    const ROWS: i32 = 250;
    const WITH_W: i32 = 100;
    let dir = tempfile::tempdir().unwrap();
    built_then_restarted(dir.path(), |a| {
        // Declared 255 wide so the cost model prices the heap at 36 pages and prefers the index
        // for a point lookup on `v` (17 against 41); the values themselves are 5 characters.
        a.sql("CREATE TABLE u (id INTEGER NOT NULL, v VARCHAR(255) NOT NULL, w VARCHAR(255));");
        a.sql("CREATE INDEX iv ON u (v);");
        for i in 1..=ROWS {
            let w = if i <= WITH_W { format!("'b{i:04}'") } else { "NULL".to_string() };
            a.sql(&format!("INSERT INTO u VALUES ({i}, 'a{i:04}', {w});"));
        }
    });

    // ---- C -------------------------------------------------------------------------------------
    let mut c = open_without_rebuild(dir.path(), "C");
    assert_eq!(
        leaf_chain::<(Value, Value), ()>(&c.bp, c.secondary_root("u", "v")).len(),
        1,
        "premise failed: u.v must start as one leaf, so the UPDATE's split is a ROOT split"
    );
    let refused = c
        .try_sql("UPDATE u SET v = w;")
        .err()
        .expect("premise failed: the UPDATE must be refused at row 101, whose w is NULL");
    assert!(
        refused.to_string().contains("NOT NULL"),
        "premise failed: refused for another reason than row 101's NULL: {refused}"
    );
    let chain = leaf_chain::<(Value, Value), ()>(&c.bp, c.secondary_root("u", "v"));
    assert_eq!(chain.len(), 2, "premise failed: the earlier rows did not split u.v's root: {chain:?}");
    close(c);

    // ---- D -------------------------------------------------------------------------------------
    let mut d = open_without_rebuild(dir.path(), "D");
    d.sql("ANALYZE u;");
    let lookup = "SELECT id FROM u WHERE v = 'a0200';";
    let plan = d.explain(lookup);
    assert!(
        plan.contains("Index scan on u (col 1, [") && !plan.contains("Filter") && !plan.contains("Sequential scan"),
        "premise failed: `{lookup}` must be a bare secondary index scan. Plan:\n{plan}"
    );
    let got = d.ids(lookup);
    assert!(
        got == vec![200],
        "right after the reopen, on a cold pool, `v = 'a0200'` must answer exactly row 200; it \
         answered {} rows, starting {:?}",
        got.len(),
        &got[..got.len().min(5)]
    );

    d.sql("INSERT INTO u VALUES (251, 'zzzz', NULL);");
    assert_eq!(
        d.ids(lookup),
        vec![200],
        "row 200's entry, in the right half of the split, must still be found after an INSERT of a \
         larger value"
    );
    assert_record_is_the_root::<(Value, Value), ()>(&d.bp, d.secondary_root("u", "v"), "u.v");
}

/// `sync_roots` and `sync_fulltext_roots` record every moved root in memory even when the
/// catalog's `persist` fails part-way.
///
/// Each `update_*_root` sets the in-memory record and THEN persists. At `9aa6968` the first failed
/// persist returned, so every index after it kept its old record. Here the persist is made to fail by
/// pointing the catalog at a page that is not a catalog page, which `CatalogPage::deserialize`
/// refuses, and the handles are private ones whose roots were split by direct inserts.
#[test]
fn a_failed_persist_part_way_through_a_root_sync_still_records_every_root() {
    let dir = tempfile::tempdir().unwrap();
    let (mut d, _) = open(dir.path());
    d.sql("CREATE TABLE s (id INTEGER NOT NULL, v VARCHAR(10), b1 VARCHAR(10), b2 VARCHAR(10));");
    d.sql("CREATE INDEX iv ON s (v);");
    d.sql("CREATE FULLTEXT INDEX f1 ON s (b1);");
    d.sql("CREATE FULLTEXT INDEX f2 ON s (b2);");
    let schema = d.catalog.get_table("s").expect("table s").schema.clone();

    let primary = BPlusTreeManager::<Value, RecordId>::open(d.primary_root("s"), d.bp.clone());
    let sec = BPlusTreeManager::<(Value, Value), ()>::open(d.secondary_root("s", "v"), d.bp.clone());
    let ft1 = BPlusTreeManager::<(Value, Value), ()>::open(d.fulltext_root("s", "b1"), d.bp.clone());
    let ft2 = BPlusTreeManager::<(Value, Value), ()>::open(d.fulltext_root("s", "b2"), d.bp.clone());
    for i in 1..=400 {
        primary.insert(Value::Integer(i), RecordId::new(1, 0)).expect("primary insert");
        for t in [&sec, &ft1, &ft2] {
            t.insert((Value::Varchar("aaa".into()), Value::Integer(i)), ()).expect("index insert");
        }
    }
    let now = |t: &BPlusTreeManager<(Value, Value), ()>| t.root_page_id.load(std::sync::atomic::Ordering::SeqCst);
    let primary_now = primary.root_page_id.load(std::sync::atomic::Ordering::SeqCst);
    assert_ne!(primary_now, d.primary_root("s"), "premise failed: 400 entries did not split the primary root");
    for (what, t, recorded) in [
        ("s.v", &sec, d.secondary_root("s", "v")),
        ("s.b1", &ft1, d.fulltext_root("s", "b1")),
        ("s.b2", &ft2, d.fulltext_root("s", "b2")),
    ] {
        assert_ne!(now(t), recorded, "premise failed: 400 entries did not split {what}'s root");
    }

    // Every persist from here reads this page first and refuses it: a B+tree page is type 2 or 3,
    // a catalog page 4 or 5.
    d.catalog.first_catalog_page_id = primary_now;

    let handles = [IndexHandle { col_index: 1, tree: sec }];
    let synced = sync_roots("s", &schema, &primary, &handles, &mut d.catalog);
    assert!(synced.is_err(), "premise failed: persist was supposed to fail");
    assert_eq!(d.primary_root("s"), primary_now, "the primary record, set before its persist failed");
    assert_eq!(
        d.secondary_root("s", "v"),
        now(&handles[0].tree),
        "s.v's record must be set even though the persist before it failed"
    );

    let fulltext = [
        FullTextHandle { col_index: 2, column_name: "b1".into(), tree: ft1 },
        FullTextHandle { col_index: 3, column_name: "b2".into(), tree: ft2 },
    ];
    let synced = sync_fulltext_roots("s", &fulltext, &mut d.catalog);
    assert!(synced.is_err(), "premise failed: persist was supposed to fail");
    for h in &fulltext {
        assert_eq!(
            d.fulltext_root("s", &h.column_name),
            now(&h.tree),
            "s.{}'s record must be set even though an earlier persist failed",
            h.column_name
        );
    }
    d.catalog.first_catalog_page_id = 1;
}
