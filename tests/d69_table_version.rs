//! D69 — the per-table change counter that replaces a full-table rescan-and-hash.
//!
//! A merge answered "did the base move between scoring and publishing?" by SCANNING every row of
//! every touched table and hashing it — twice per merge. D68 measured that at 1.87 us/row:
//! ~1.9 seconds against a million-row table, to write four rows
//! (`bench/d68_merge_is_o_table.txt`). A monotone counter answers the same question in O(1).
//!
//! ⚠ THE RISK THIS FILE EXISTS TO PIN: the hash it replaces was computed by scanning the REAL
//! table, so it saw ordinary `INSERT`/`UPDATE`/`DELETE` as well as agent merges. A counter bumped
//! only on the agent path would miss direct writes and weaken the staleness check SILENTLY — a
//! regression that looks exactly like a speedup. Every test here is about that.
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::table_id;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
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
            .read(true).write(true).create(true).truncate(true)
            .open(dir.path().join("p.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _dir: dir }
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(FerroError::SqlParseError(format!("{:?}", p.errors)));
        }
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    fn version(&self, table: &str) -> u64 {
        self.catalog.table_version(Catalog::table_version_key(table))
    }
}

/// The counter and the merge path must agree on what identifies a table.
///
/// `Catalog::table_version_key` duplicates `agent_sql::runtime::table_id`'s FNV-1a rather than
/// importing it, because `catalog` must not depend on `agent_sql`. A duplicated constant that
/// nothing compares is a constant that drifts, so this compares them.
#[test]
fn the_counter_key_is_the_same_hash_the_merge_path_uses() {
    for name in ["t", "inventory", "a_much_longer_table_name", ""] {
        assert_eq!(
            Catalog::table_version_key(name),
            table_id(name).0,
            "the duplicated FNV-1a drifted for {name:?}"
        );
    }
}

/// ⚠ THE LOAD-BEARING TEST. Ordinary DML — no agent session anywhere — must move the counter.
#[test]
fn plain_dml_outside_any_agent_session_moves_the_counter() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut s);

    // Never written: 0 is correct, not merely convenient — a table nobody has written cannot have
    // moved, and two reads of 0 compare equal exactly as two reads of anything else do.
    assert_eq!(db.version("t"), 0, "an unwritten table should read 0");

    db.ok("INSERT INTO t VALUES (1, 10);", &mut s);
    let after_insert = db.version("t");
    assert!(after_insert > 0, "a plain INSERT must move the counter, got {after_insert}");

    db.ok("UPDATE t SET v = 11 WHERE id = 1;", &mut s);
    let after_update = db.version("t");
    assert!(
        after_update > after_insert,
        "a plain UPDATE must move the counter: {after_insert} -> {after_update}"
    );

    db.ok("DELETE FROM t WHERE id = 1;", &mut s);
    let after_delete = db.version("t");
    assert!(
        after_delete > after_update,
        "a plain DELETE must move the counter: {after_update} -> {after_delete}"
    );
}

/// Monotone, and never reused. A counter that could return to a previous value would reintroduce
/// exactly the blind spot the hash had: a change made and reverted between two observations.
#[test]
fn the_counter_never_goes_backwards() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut s);
    let mut seen = 0u64;
    for i in 1..=20 {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {i});"), &mut s);
        let now = db.version("t");
        assert!(now > seen, "counter did not advance at row {i}: {seen} -> {now}");
        seen = now;
    }
    // Write and revert: the value is back but the counter is not — the ABA case a hash misses.
    db.ok("UPDATE t SET v = 999 WHERE id = 1;", &mut s);
    let after_change = db.version("t");
    db.ok("UPDATE t SET v = 1 WHERE id = 1;", &mut s);
    assert!(
        db.version("t") > after_change,
        "reverting a value must still advance the counter; a fingerprint would read equal here"
    );
}

/// One table's writes must not move another's, or every merge would see every table as stale.
#[test]
fn tables_do_not_share_a_counter() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE a (id INTEGER NOT NULL, v INTEGER);", &mut s);
    db.ok("CREATE TABLE b (id INTEGER NOT NULL, v INTEGER);", &mut s);
    db.ok("INSERT INTO a VALUES (1, 1);", &mut s);
    let a1 = db.version("a");
    let b1 = db.version("b");
    db.ok("INSERT INTO b VALUES (1, 1);", &mut s);
    assert_eq!(db.version("a"), a1, "writing b moved a's counter");
    assert!(db.version("b") > b1, "writing b did not move b's counter");
}
