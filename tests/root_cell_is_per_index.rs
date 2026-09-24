//! A secondary index and a full-text index on ONE column share one D53 root cell.
//!
//! Found while porting D205's fix (artie-research `frontier/lane_rollback_index_orphan.md` §14).
//! INFERRED from source and never run; this file is the measurement. It was NOT fixed on the
//! branch that found it (`rollback-index-orphan`, `d7891d5`); D208 fixes it on `d208-root-cell-per-index`
//! by keying each cell by index KIND as well as column (`Catalog::root_cell`, `IndexTree`).
//!
//! The chain (READ-FROM-SOURCE):
//! - `Catalog::roots` is keyed `(table, Option<column>)`, with no index kind. `sync_root_cells`
//!   pushes a secondary index as `(t, Some(col))` and a full-text index on the same column as the
//!   same key. `or_insert_with` keeps whichever came first.
//! - `plan::open_table` opens the secondary tree AND the full-text tree through
//!   `root_cell(&entry.name, Some(&info.column_name))`, the same key. So an INSERT posts the
//!   full-text tokens into whichever tree the one cell names.
//! - `executor::sync_fulltext_roots` then sees that handle's root differ from the full-text RECORD
//!   and calls `update_fulltext_root`. The full-text record now names the secondary tree, and the
//!   real full-text tree, holding every posting `CREATE FULLTEXT INDEX` backfilled, is orphaned.
//! - `SEARCH` resolves its tree from the record, so after ONE insert every row that existed when
//!   the full-text index was built stops being findable.
//!
//! Secondary lookups survive, because `SecondaryIndexScan` drops an entry whose resolved value
//! differs from the key. That is why nothing noticed.
//!
//! # D208 — the same collision, three more routes (INFERRED, READ-FROM-SOURCE, not run)
//!
//! The sentence above holds only in the order the first test uses. Recorded here, not edited out:
//!
//! - **Full-text index FIRST.** It seeds the one cell, so the optimizer's secondary scan
//!   (`optimizer::lower`, `root_cell(&table, Some(&col_name))`) descends the POSTING tree, which
//!   holds tokens and not whole values: an equality lookup misses every backfilled row. The first
//!   write makes it durable, because `sync_roots` writes the posting tree's root into the B-tree
//!   record. Re-checking the value cannot help; the entry is absent, not wrong.
//! - **A rebuild, in either order.** `rebuild_indexes` stores each fresh root into "its" cell
//!   (D205's store-in-place). Both trees resolve to the one cell and the posting tree is stored
//!   last, so after any rebuild the secondary index reads the posting tree.
//! - **A renamed column (the same defect through another exit).** The cell is keyed by the column
//!   NAME, and `ALTER ... RENAME COLUMN` renames the records but leaves the cell under the old
//!   name. An index later built on a new column that takes the old name inherits the renamed
//!   index's tree through that cell, because `sync_root_cells` never overwrites an existing one.
//!
//! The lookups below are only meaningful on an index scan, and below a few hundred rows the
//! optimizer prefers a filtered sequential scan, which answers correctly whatever the cell names.
//! So each fixture is `N` rows, `ANALYZE`d, and each asserts its plan before trusting a lookup.

use std::fs::OpenOptions;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::{Catalog, IndexTree};
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::planner::plan::explain;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::rebuild_indexes;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("cells.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("cells.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _dir: dir }
    }

    fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        rows_on(&mut self.catalog, &self.bp, &self.txn, sql)
    }

    fn ids(&mut self, sql: &str) -> Vec<i32> {
        ids_of(self.rows(sql))
    }

    /// `ids`, run against another catalog over the same pages: a reader's cached snapshot.
    fn ids_against(&self, catalog: &mut Catalog, sql: &str) -> Vec<i32> {
        ids_of(rows_on(catalog, &self.bp, &self.txn, sql))
    }

    /// The plan the optimizer builds for a SELECT: the same `optimize` that `run` plans with.
    fn explain(&self, sql: &str) -> String {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        explain(stmts.remove(0), &self.catalog).unwrap_or_else(|e| panic!("EXPLAIN `{sql}` failed: {e}"))
    }

    /// Fail unless `sql` is planned as an index scan on column `col` of `t`.
    fn assert_index_scan(&self, sql: &str, col: usize) {
        let plan = self.explain(sql);
        assert!(
            plan.contains(&format!("Index scan on t (col {col},")),
            "premise failed: `{sql}` is not planned on the index over column {col}, so its lookup \
             would read the heap and could not see which tree the index opened. plan:\n{plan}"
        );
    }

    /// The shared root cell of one index on `t`, which must exist.
    fn cell(&self, index: IndexTree<&str>) -> Arc<std::sync::atomic::AtomicU32> {
        self.catalog.root_cell("t", Some(index)).unwrap_or_else(|| panic!("no shared root cell for {index:?} on t"))
    }

    /// The durable records' roots for the trees on `column` of `t`: (B-tree, full-text).
    fn record_roots(&self, column: &str) -> (Option<u32>, Option<u32>) {
        let e = self.catalog.get_table("t").expect("table t exists");
        (
            e.indexes.iter().find(|i| i.column_name == column).map(|i| i.root_page_id),
            e.fulltext_indexes.iter().find(|i| i.column_name == column).map(|i| i.root_page_id),
        )
    }
}

/// Run one statement against `catalog`.
fn rows_on(catalog: &mut Catalog, bp: &Arc<BufferPoolManager>, txn: &Arc<TxnManager>, sql: &str) -> Vec<Vec<Value>> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let mut stmts = p.parse();
    assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
    match run(stmts.remove(0), catalog, bp.clone(), txn.clone(), &mut Session::new()) {
        Ok(Outcome::Rows(r)) => r,
        Ok(_) => Vec::new(),
        Err(e) => panic!("`{sql}` failed: {e}"),
    }
}

/// The first column of every row, as the Integer primary key it must be, sorted.
fn ids_of(rows: Vec<Vec<Value>>) -> Vec<i32> {
    let mut ids: Vec<i32> = rows
        .into_iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            ref other => panic!("expected an Integer primary key, got {other:?}"),
        })
        .collect();
    ids.sort();
    ids
}

/// Rows in each index-scan fixture below.
///
/// Sized from `optimizer::cost_model` (READ; the EXPLAIN premise in each test is the measurement).
/// `t (id INTEGER, body VARCHAR(100))` costs 24 + 4 + 100 = 128 bytes a row, 32 to a 4096-byte
/// page. After `ANALYZE` with every value distinct, an equality scan on the secondary index
/// estimates 1 row: 2 levels x 4 + 1 leaf + 4 + 4 = 17. The filtered sequential scan costs
/// pages + 0.02 x rows: 26 at 500 rows, 16 at 300. So the index wins from about 340 rows, and 500
/// leaves a margin. Do not shrink it: a smaller table switches these tests off, it does not speed
/// them up.
const N: i32 = 500;

/// **The full-text index keeps answering for the rows it was built over, after the table is
/// written again.**
///
/// FAILS today (INFERRED) at the search for 'alpha': row 1 was backfilled into the real full-text
/// tree, and the insert of row 2 repoints the full-text record at the secondary tree.
#[test]
fn a_fulltext_index_beside_a_secondary_index_on_one_column_keeps_its_own_tree() {
    let mut d = Db::new();
    d.rows("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    d.rows("CREATE INDEX ib ON t (body);");
    d.rows("INSERT INTO t VALUES (1, 'alpha beta');");
    d.rows("CREATE FULLTEXT INDEX fb ON t (body);");
    // Premise and anti-vacuity: before any further write, the full-text index answers for row 1.
    assert_eq!(d.ids("SEARCH t (body) FOR 'alpha';"), vec![1], "premise failed: the backfill did not index row 1");

    d.rows("INSERT INTO t VALUES (2, 'gamma');");

    assert_eq!(
        d.ids("SEARCH t (body) FOR 'alpha';"),
        vec![1],
        "one insert later, the row the full-text index was built over is no longer found"
    );
    assert_eq!(d.ids("SEARCH t (body) FOR 'gamma';"), vec![2], "the new row is not found by its word");
    // Secondary lookups by whole value are expected to survive either way (their scan re-checks
    // the value), so this is a control, not the defect.
    assert_eq!(d.ids("SELECT id FROM t WHERE body = 'alpha beta';"), vec![1], "the secondary lookup broke");
}

/// **D208, the other order: a B-tree index built BESIDE an existing full-text index answers an
/// equality lookup for the rows it was built over, before and after the table is written again.**
///
/// The full-text index is created first, so it seeds the shared cell, and the secondary scan
/// descends the posting tree. FAILS today (INFERRED) at the FIRST lookup for `row7 alpha`, before
/// any write: the posting tree holds `row7` and `alpha`, never the whole value. If that assertion
/// were removed, the lookups after the write would fail too, and the record assertion at the end
/// would fail because `sync_roots` wrote the posting tree's root into the B-tree record.
#[test]
fn a_btree_index_beside_a_fulltext_index_on_one_column_keeps_its_own_tree() {
    let mut d = Db::new();
    d.rows("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    for i in 0..N {
        d.rows(&format!("INSERT INTO t VALUES ({i}, 'row{i} alpha');"));
    }
    d.rows("CREATE FULLTEXT INDEX fb ON t (body);");
    d.rows("CREATE INDEX ib ON t (body);");
    d.rows("ANALYZE t;");
    let lookup = "SELECT id FROM t WHERE body = 'row7 alpha';";
    d.assert_index_scan(lookup, 1);

    assert_eq!(d.ids(lookup), vec![7], "the B-tree index does not find a row it was built over");

    d.rows(&format!("INSERT INTO t VALUES ({N}, 'late entry');"));

    assert_eq!(d.ids(lookup), vec![7], "one insert later, the B-tree index lost a row it was built over");
    assert_eq!(
        d.ids("SELECT id FROM t WHERE body = 'late entry';"),
        vec![N],
        "the B-tree index does not find the row just inserted"
    );
    // The full-text side is a control in this order: it seeded the cell, so it reads its own tree.
    assert_eq!(d.ids("SEARCH t (body) FOR 'row7';"), vec![7], "the full-text control broke");
    assert_eq!(d.ids("SEARCH t (body) FOR 'late';"), vec![N], "the full-text control broke on the new row");
    // Structural: two trees cannot share a root page, so equal records mean one record names the
    // other index's tree and that index's own tree is orphaned.
    let (btree, fulltext) = d.record_roots("body");
    assert!(btree.is_some() && fulltext.is_some(), "premise failed: an index record is missing");
    assert_ne!(
        btree, fulltext,
        "the B-tree record and the full-text record name the same page, so one of the two trees is orphaned"
    );
}

/// **D208 through D205's store-in-place: a rebuild leaves the B-tree index on its own tree.**
///
/// The B-tree index is created FIRST here, the order in which the live path leaves B-tree lookups
/// correct, so the premise lookup before the rebuild passes today and the only thing that changes
/// is the rebuild. FAILS today (INFERRED) at the lookup right after `rebuild_indexes`: both fresh
/// roots are stored into the one cell, the posting tree's last.
#[test]
fn a_rebuild_leaves_a_btree_index_beside_a_fulltext_index_on_its_own_tree() {
    let mut d = Db::new();
    d.rows("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    for i in 0..N {
        d.rows(&format!("INSERT INTO t VALUES ({i}, 'row{i} alpha');"));
    }
    d.rows("CREATE INDEX ib ON t (body);");
    d.rows("CREATE FULLTEXT INDEX fb ON t (body);");
    d.rows("ANALYZE t;");
    let lookup = "SELECT id FROM t WHERE body = 'row7 alpha';";
    d.assert_index_scan(lookup, 1);
    assert_eq!(d.ids(lookup), vec![7], "premise failed: the B-tree lookup is wrong before any rebuild");

    rebuild_indexes(&mut d.catalog, &d.bp).unwrap();

    assert_eq!(d.ids(lookup), vec![7], "after a rebuild, the B-tree index lost a row");
    assert_eq!(d.ids("SEARCH t (body) FOR 'row7';"), vec![7], "after a rebuild, the full-text index lost a row");

    d.rows(&format!("INSERT INTO t VALUES ({N}, 'late entry');"));

    assert_eq!(d.ids(lookup), vec![7], "after a rebuild and a write, the B-tree index lost a row");
    assert_eq!(d.ids("SELECT id FROM t WHERE body = 'late entry';"), vec![N], "the new row is not found by value");
    assert_eq!(d.ids("SEARCH t (body) FOR 'late';"), vec![N], "the new row is not found by its word");
    let (btree, fulltext) = d.record_roots("body");
    assert!(btree.is_some() && fulltext.is_some(), "premise failed: an index record is missing");
    assert_ne!(btree, fulltext, "after a rebuild and a write, both index records name one tree");
}

/// **D208, the rename exit: an index built on a column that takes a renamed column's old name
/// does not inherit the renamed column's tree.**
///
/// The rows are inserted AFTER the two ALTERs, so the new column holds values of its own and the
/// two indexes' trees hold different keys. FAILS today (INFERRED) at the lookup on `a`: the cell
/// keyed `(t, a)` still names the tree the rename handed to `b`, so the scan finds `b` values only.
#[test]
fn an_index_on_a_reused_column_name_does_not_inherit_the_renamed_columns_tree() {
    let mut d = Db::new();
    d.rows("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(100));");
    d.rows("CREATE INDEX ia ON t (a);");
    d.rows("ALTER TABLE t RENAME COLUMN a TO b;");
    d.rows("ALTER TABLE t ADD COLUMN a VARCHAR(100);");
    for i in 0..N {
        d.rows(&format!("INSERT INTO t VALUES ({i}, 'b{i}', 'a{i}');"));
    }
    d.rows("CREATE INDEX ia2 ON t (a);");
    d.rows("ANALYZE t;");
    let on_a = "SELECT id FROM t WHERE a = 'a7';";
    let on_b = "SELECT id FROM t WHERE b = 'b7';";
    d.assert_index_scan(on_a, 2);
    d.assert_index_scan(on_b, 1);

    assert_eq!(d.ids(on_a), vec![7], "the index on the new column `a` does not find a row it was built over");
    assert_eq!(d.ids(on_b), vec![7], "the index that followed the rename to `b` lost a row");

    d.rows(&format!("INSERT INTO t VALUES ({N}, 'b-late', 'a-late');"));

    assert_eq!(d.ids(on_a), vec![7], "one insert later, the index on `a` lost a row");
    assert_eq!(d.ids(on_b), vec![7], "one insert later, the index on `b` lost a row");
    assert_eq!(d.ids("SELECT id FROM t WHERE a = 'a-late';"), vec![N], "the new row is not found through `a`");
    assert_eq!(d.ids("SELECT id FROM t WHERE b = 'b-late';"), vec![N], "the new row is not found through `b`");
    let (on_a_root, _) = d.record_roots("a");
    let (on_b_root, _) = d.record_roots("b");
    assert!(on_a_root.is_some() && on_b_root.is_some(), "premise failed: an index record is missing");
    assert_ne!(on_a_root, on_b_root, "the indexes on `a` and `b` record one tree between them");
}

/// **D215 — SEARCH descends the SHARED full-text cell, so a reader's cached catalog snapshot still
/// finds a row whose posting moved in a root split.**
///
/// D53's contract, in `Catalog::epoch`'s doc: a reader re-takes its snapshot only when the schema
/// epoch moves; a root move does not move it; and "a snapshot whose recorded root is stale is
/// still correct — `open_table` reads the cell, not the record". SEARCH read the RECORD
/// (`open_posting_tree(ft_root, ..)`, a private cell). After a root split the recorded page is the
/// LEFTMOST leaf, which keeps the left half, and `postings_for_token` scans from there, reaches the
/// next leaf, meets a smaller token and stops (`if tok != want { break }`).
///
/// ⚠ Not reachable through a server TODAY: `executor::try_run_read` serves only `EXPLAIN` and
/// `SELECT`, so SEARCH runs under the exclusive catalog lock on the live catalog, whose record is
/// current at every statement boundary. This drives SEARCH against a snapshot directly, which is
/// the position the shared read path puts every statement it serves in. FAILS before D215's fix
/// (INFERRED) at the snapshot's search, with `[]` against `[0]`.
#[test]
fn search_through_a_cached_snapshot_finds_a_row_after_a_posting_root_split() {
    let mut d = Db::new();
    d.rows("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    d.rows("CREATE FULLTEXT INDEX fb ON t (body);");
    let mut snapshot = d.catalog.clone();
    let recorded = d.record_roots("body").1.expect("premise failed: no full-text record");

    // `zulu` sorts after every `aNNNNN`, so the first split leaves it in the RIGHT half, away from
    // the page the snapshot recorded.
    d.rows("INSERT INTO t VALUES (0, 'zulu');");
    let mut id = 1;
    while d.record_roots("body").1 == Some(recorded) {
        assert!(id <= 5000, "premise failed: 5000 postings never split the posting tree's root");
        d.rows(&format!("INSERT INTO t VALUES ({id}, 'a{id:05}');"));
        id += 1;
    }
    assert_eq!(
        snapshot.epoch(),
        d.catalog.epoch(),
        "premise failed: a write moved the schema epoch, so a reader would have re-taken its snapshot"
    );
    assert_eq!(
        snapshot.get_table("t").unwrap().fulltext_indexes[0].root_page_id,
        recorded,
        "premise failed: the snapshot's record moved, so it is not stale"
    );
    assert_eq!(d.ids("SEARCH t (body) FOR 'zulu';"), vec![0], "premise failed: the live catalog's search misses the row");

    assert_eq!(
        d.ids_against(&mut snapshot, "SEARCH t (body) FOR 'zulu';"),
        vec![0],
        "a cached snapshot's SEARCH descended its recorded root, the pre-split leftmost leaf, and missed the row"
    );
}

// ---------------------------------------------------------------------------------------------
// The cells themselves (D208's fix). These use `IndexTree`, so they compile only from the fix on;
// the behavioural tests above are the red phase and compile against either API.
// ---------------------------------------------------------------------------------------------

/// **A B-tree index and a full-text index on one column hold two cells, each naming its own tree,
/// whichever was created first — and each root writer stores into its own kind's cell only.**
///
/// Structural, so it cannot pass by a fixture being too small to reach the index: two indexes are
/// two trees, and two trees need two cells. The two stores at the end are the root writers a split
/// reaches (`sync_roots` → `update_index_root`, `sync_fulltext_roots` → `update_fulltext_root`),
/// called directly because no fixture this size splits a root. 9001 and 9002 are page numbers
/// nothing allocates here; nothing descends them.
#[test]
fn each_index_kind_on_one_column_holds_its_own_cell_in_either_order() {
    for fulltext_first in [false, true] {
        let mut d = Db::new();
        d.rows("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
        d.rows("INSERT INTO t VALUES (1, 'alpha beta');");
        let (btree_sql, fulltext_sql) = ("CREATE INDEX ib ON t (body);", "CREATE FULLTEXT INDEX fb ON t (body);");
        if fulltext_first {
            d.rows(fulltext_sql);
            d.rows(btree_sql);
        } else {
            d.rows(btree_sql);
            d.rows(fulltext_sql);
        }
        let order = if fulltext_first { "full-text first" } else { "B-tree first" };

        let btree = d.cell(IndexTree::Secondary("body"));
        let fulltext = d.cell(IndexTree::FullText("body"));
        assert!(!Arc::ptr_eq(&btree, &fulltext), "{order}: the two indexes on `body` share one cell");
        let (btree_record, fulltext_record) = d.record_roots("body");
        assert!(btree_record.is_some() && fulltext_record.is_some(), "{order}: premise failed: an index record is missing");
        assert_ne!(btree_record, fulltext_record, "{order}: premise failed: both records name one page");
        assert_eq!(Some(btree.load(Ordering::SeqCst)), btree_record, "{order}: the B-tree cell does not name the B-tree index's tree");
        assert_eq!(Some(fulltext.load(Ordering::SeqCst)), fulltext_record, "{order}: the full-text cell does not name the posting tree");

        d.catalog.update_index_root("t", "body", 9001).unwrap();
        assert_eq!(btree.load(Ordering::SeqCst), 9001, "{order}: update_index_root did not store into the B-tree cell");
        assert_eq!(fulltext.load(Ordering::SeqCst), fulltext_record.unwrap(), "{order}: update_index_root moved the full-text cell");
        d.catalog.update_fulltext_root("t", "body", 9002).unwrap();
        assert_eq!(fulltext.load(Ordering::SeqCst), 9002, "{order}: update_fulltext_root did not store into the full-text cell");
        assert_eq!(btree.load(Ordering::SeqCst), 9001, "{order}: update_fulltext_root moved the B-tree cell");
    }
}

/// **A rebuild stores each fresh root into its own kind's cell, and keeps both cells.**
///
/// The structural twin of `a_rebuild_leaves_a_btree_index_beside_a_fulltext_index_on_its_own_tree`:
/// cell == record for each kind after `rebuild_indexes`, and each is still the `Arc` a statement
/// could have been holding (D205's store-in-place, per kind).
///
/// The DROP is load-bearing, as in D205's DROP schedule. It leaves free pages BELOW `t`'s trees,
/// so the rebuild's allocation puts every fresh root somewhere new. Without it a rebuilt tree can
/// land on its old page, and then a cell nobody repointed still reads equal to its record: a
/// mutant that stores the B-tree root into the wrong cell would pass. The two premises say so.
#[test]
fn a_rebuild_stores_each_fresh_root_into_its_own_kinds_cell() {
    let mut d = Db::new();
    d.rows("CREATE TABLE a (id INTEGER NOT NULL, v INTEGER);");
    d.rows("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    d.rows("INSERT INTO t VALUES (1, 'alpha beta');");
    d.rows("CREATE INDEX ib ON t (body);");
    d.rows("CREATE FULLTEXT INDEX fb ON t (body);");
    d.rows("DROP TABLE a;");
    let (btree_before, fulltext_before) = d.record_roots("body");
    let btree = d.cell(IndexTree::Secondary("body"));
    let fulltext = d.cell(IndexTree::FullText("body"));

    rebuild_indexes(&mut d.catalog, &d.bp).unwrap();

    let (btree_record, fulltext_record) = d.record_roots("body");
    assert!(btree_record.is_some() && fulltext_record.is_some(), "premise failed: an index record is missing");
    assert_ne!(btree_record, btree_before, "premise failed: the rebuilt B-tree is on its old page, so an unrepointed cell would still match");
    assert_ne!(fulltext_record, fulltext_before, "premise failed: the rebuilt posting tree is on its old page, so an unrepointed cell would still match");
    assert_eq!(Some(btree.load(Ordering::SeqCst)), btree_record, "after a rebuild, the B-tree cell does not name the rebuilt B-tree");
    assert_eq!(Some(fulltext.load(Ordering::SeqCst)), fulltext_record, "after a rebuild, the full-text cell does not name the rebuilt posting tree");
    assert!(Arc::ptr_eq(&btree, &d.cell(IndexTree::Secondary("body"))), "the rebuild REPLACED the B-tree cell");
    assert!(Arc::ptr_eq(&fulltext, &d.cell(IndexTree::FullText("body"))), "the rebuild REPLACED the full-text cell");
}

/// **One index leaving retires its own cell and keeps the other kind's.**
///
/// There is no `DROP INDEX` statement. This removes the record the way one would, then asks the
/// catalog to reconcile its cells, which every DDL path does after changing the set of trees. The
/// B-tree index's cell must go, or the next B-tree index on the column inherits the retired tree
/// through it (`sync_root_cells` never overwrites). The full-text cell must stay, as the same `Arc`.
#[test]
fn a_btree_index_leaving_retires_its_cell_and_keeps_the_fulltext_one() {
    let mut d = Db::new();
    d.rows("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    d.rows("INSERT INTO t VALUES (1, 'alpha beta');");
    d.rows("CREATE INDEX ib ON t (body);");
    d.rows("CREATE FULLTEXT INDEX fb ON t (body);");
    let fulltext = d.cell(IndexTree::FullText("body"));
    let fulltext_root = fulltext.load(Ordering::SeqCst);
    let retired_root = d.record_roots("body").0.expect("premise failed: no B-tree record");

    d.catalog.tables.get_mut("t").unwrap().indexes.clear();
    d.catalog.sync_root_cells();

    assert!(
        d.catalog.root_cell("t", Some(IndexTree::Secondary("body"))).is_none(),
        "the cell of a B-tree index the catalog no longer records survived it"
    );
    assert!(Arc::ptr_eq(&fulltext, &d.cell(IndexTree::FullText("body"))), "retiring the B-tree cell REPLACED the full-text one");
    assert_eq!(fulltext.load(Ordering::SeqCst), fulltext_root, "retiring the B-tree cell moved the full-text one");

    d.rows("CREATE INDEX ib2 ON t (body);");
    let btree_record = d.record_roots("body").0.expect("premise failed: the new B-tree index has no record");
    assert_ne!(
        btree_record, retired_root,
        "premise failed: the new index was built at the retired root's page, so this cannot tell the two trees apart"
    );
    assert_eq!(
        d.cell(IndexTree::Secondary("body")).load(Ordering::SeqCst),
        btree_record,
        "the new B-tree index's cell names the retired index's tree"
    );
}

/// **A rename carries both kinds' cells to the new name, as the same `Arc`s.**
///
/// The structural twin of `an_index_on_a_reused_column_name_does_not_inherit_the_renamed_columns_tree`,
/// with a full-text index as well, since both kinds are keyed by the renamed name.
#[test]
fn a_rename_carries_both_kinds_cells_to_the_new_name() {
    let mut d = Db::new();
    d.rows("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(100));");
    d.rows("INSERT INTO t VALUES (1, 'alpha beta');");
    d.rows("CREATE INDEX ia ON t (a);");
    d.rows("CREATE FULLTEXT INDEX fa ON t (a);");
    let btree = d.cell(IndexTree::Secondary("a"));
    let fulltext = d.cell(IndexTree::FullText("a"));

    d.rows("ALTER TABLE t RENAME COLUMN a TO b;");

    assert!(d.catalog.root_cell("t", Some(IndexTree::Secondary("a"))).is_none(), "the B-tree cell stayed under the old name");
    assert!(d.catalog.root_cell("t", Some(IndexTree::FullText("a"))).is_none(), "the full-text cell stayed under the old name");
    assert!(Arc::ptr_eq(&btree, &d.cell(IndexTree::Secondary("b"))), "the B-tree index's cell did not move to `b`");
    assert!(Arc::ptr_eq(&fulltext, &d.cell(IndexTree::FullText("b"))), "the full-text index's cell did not move to `b`");
    let (btree_record, fulltext_record) = d.record_roots("b");
    assert_eq!(Some(btree.load(Ordering::SeqCst)), btree_record, "the moved B-tree cell does not name the renamed index's tree");
    assert_eq!(Some(fulltext.load(Ordering::SeqCst)), fulltext_record, "the moved full-text cell does not name the renamed index's tree");
}
