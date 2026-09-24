//! A secondary index and a full-text index on ONE column share one D53 root cell.
//!
//! Found while porting D205's fix (artie-research `frontier/lane_rollback_index_orphan.md` §14).
//! INFERRED from source and never run; this file is the measurement. It is NOT fixed on this branch.
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

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
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
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        match run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut Session::new()) {
            Ok(Outcome::Rows(r)) => r,
            Ok(_) => Vec::new(),
            Err(e) => panic!("`{sql}` failed: {e}"),
        }
    }

    fn ids(&mut self, sql: &str) -> Vec<i32> {
        let mut ids: Vec<i32> = self
            .rows(sql)
            .into_iter()
            .map(|r| match r[0] {
                Value::Integer(i) => i,
                ref other => panic!("expected an Integer primary key, got {other:?}"),
            })
            .collect();
        ids.sort();
        ids
    }
}

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
