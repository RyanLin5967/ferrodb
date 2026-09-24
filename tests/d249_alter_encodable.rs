//! D249 — an `ALTER TABLE` whose new column name the catalog cannot hold, driven from SQL.
//!
//! D141 made the catalog encoder refuse a name too long for its one-byte length prefix, and made
//! `CREATE TABLE` undo its in-memory insert when that refusal comes back (`persist_or_undo`), so one
//! bad statement cannot wedge every later one. `ALTER TABLE` never got the same treatment:
//! `resulting_schema` checks a new column name for existence and duplicates only, and
//! `Catalog::finish` installs the new schema in memory and THEN persists, with no undo. So a
//! `RENAME COLUMN` or `ADD COLUMN` to a 300-byte name is refused at `finish` but leaves the long name
//! in the catalog map, and every later `persist` in the process re-serializes it and refuses too.
//! That is the D141 wedge, for ALTER.
//!
//! Each test checks the three things a clean refusal owes:
//!
//! - the statement is refused, by the encoder's own error, naming the column;
//! - the table keeps its old schema, and its row still reads back under it, so nothing was
//!   rewritten into a shape the catalog does not describe (I19);
//! - a later `CREATE TABLE`, which is a `persist`, still succeeds.
//!
//! The premise, asserted rather than assumed: the new name really is longer than the prefix holds.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
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
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("d249.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d249.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _dir: dir }
    }

    fn exec(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        let mut s = Session::new();
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut s)
    }

    /// A table with a row in it, so a rewrite into the wrong shape would have something to break.
    fn with_one_row() -> Self {
        let mut db = Db::new();
        db.exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);").unwrap();
        db.exec("INSERT INTO t VALUES (1, 7);").unwrap();
        db
    }
}

/// 300 bytes: D141's own example, and past the 255 a `u8` length prefix can express.
fn over_long() -> String {
    let name = "c".repeat(300);
    assert!(
        name.len() > u8::MAX as usize,
        "the fixture's name ({} bytes) fits the catalog's one-byte length prefix, so this tests nothing",
        name.len()
    );
    name
}

/// The refusal must be the encoder's, for this length, naming the column.
fn assert_refused_by_name(err: &FerroError, statement: &str) {
    assert!(
        matches!(err, FerroError::Unrepresentable { len: 300, limit: 255, .. }),
        "{statement}: expected the encoder's refusal naming the length it could not express, got: \
         {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("the name of column") && msg.contains("in table \"t\""),
        "{statement}: the refusal does not name the column and its table: {msg}"
    );
}

/// The table is exactly as it was: its old columns, and its row readable under them.
fn assert_unchanged(db: &mut Db, statement: &str) {
    let names: Vec<String> = db
        .catalog
        .get_table("t")
        .expect("the table is still there")
        .schema
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert_eq!(
        names,
        vec!["id".to_string(), "v".to_string()],
        "{statement} was refused, but the catalog in memory holds the new shape. Every later persist \
         re-serializes it, so one refused ALTER wedges every later DDL"
    );
    match db.exec("SELECT * FROM t;") {
        Ok(Outcome::Rows(rows)) => assert_eq!(
            rows,
            vec![vec![Value::Integer(1), Value::Integer(7)]],
            "{statement} was refused, but the row no longer reads back as (1, 7) under the old \
             schema: it was rewritten into a shape the catalog does not describe"
        ),
        Ok(_) => panic!("{statement}: SELECT after the refusal returned something other than rows"),
        Err(e) => panic!("{statement}: SELECT after the refusal failed: {e}"),
    }
}

/// A later `CREATE TABLE` is a `persist`, so it is the direct test for the wedge.
fn assert_not_wedged(db: &mut Db, statement: &str, next: &str) {
    if let Err(e) = db.exec(&format!("CREATE TABLE {next} (id INTEGER NOT NULL);")) {
        panic!(
            "after a refused {statement}, an ordinary CREATE TABLE refused too: the refused name is \
             still in the catalog, so every later persist fails ({e})"
        );
    }
    assert!(db.catalog.get_table(next).is_some(), "the CREATE TABLE after the refusal did not land");
}

#[test]
fn a_rename_to_a_column_name_the_catalog_cannot_hold_is_refused_and_wedges_nothing() {
    let mut db = Db::with_one_row();
    let long = over_long();

    let err = db
        .exec(&format!("ALTER TABLE t RENAME COLUMN v TO {long};"))
        .err()
        .expect("a rename to a 300-byte column name must be refused, not written truncated");
    assert_refused_by_name(&err, "RENAME COLUMN");
    assert_unchanged(&mut db, "RENAME COLUMN");
    assert_not_wedged(&mut db, "RENAME COLUMN", "after_rename");
}

#[test]
fn an_added_column_whose_name_the_catalog_cannot_hold_is_refused_and_wedges_nothing() {
    let mut db = Db::with_one_row();
    let long = over_long();

    let err = db
        .exec(&format!("ALTER TABLE t ADD COLUMN {long} INTEGER;"))
        .err()
        .expect("an added column with a 300-byte name must be refused, not written truncated");
    assert_refused_by_name(&err, "ADD COLUMN");
    assert_unchanged(&mut db, "ADD COLUMN");
    assert_not_wedged(&mut db, "ADD COLUMN", "after_add");
}

/// The ADD case again, on a table whose row layout an added column WOULD change.
///
/// A trailing NULL column occupies its type's width after the existing ones and, below nine
/// columns, leaves the null bitmap at one byte. So a two-column row rewritten to three still reads
/// back as its first two values, and the test above cannot tell "refused before the rewrite" from
/// "refused after it". At the ninth column the bitmap grows to two bytes, and every value after it
/// moves. A row rewritten under nine columns then does NOT read back under the old eight. This is
/// the test that pins where the refusal happens, and not only that it happens (I19).
#[test]
fn an_added_ninth_column_the_catalog_cannot_hold_is_refused_before_any_row_is_rewritten() {
    let mut db = Db::new();
    db.exec(
        "CREATE TABLE t (id INTEGER NOT NULL, c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER, \
         c5 INTEGER, c6 INTEGER, c7 INTEGER);",
    )
    .unwrap();
    db.exec("INSERT INTO t VALUES (1, 2, 3, 4, 5, 6, 7, 8);").unwrap();
    let long = over_long();

    let err = db
        .exec(&format!("ALTER TABLE t ADD COLUMN {long} INTEGER;"))
        .err()
        .expect("a ninth column with a 300-byte name must be refused, not written truncated");
    assert_refused_by_name(&err, "ADD COLUMN (ninth)");

    let columns = db.catalog.get_table("t").expect("the table is still there").schema.columns.len();
    assert_eq!(
        columns, 8,
        "ADD COLUMN (ninth) was refused, but the catalog in memory holds {columns} columns"
    );
    let want: Vec<Value> = (1..=8).map(Value::Integer).collect();
    match db.exec("SELECT * FROM t;") {
        Ok(Outcome::Rows(rows)) => assert_eq!(
            rows,
            vec![want],
            "ADD COLUMN (ninth) was refused, but the row no longer reads back under the old eight \
             columns: it was rewritten under nine, whose null bitmap is a byte wider (I19)"
        ),
        Ok(_) => panic!("SELECT after the refusal returned something other than rows"),
        Err(e) => panic!(
            "SELECT after the refused ADD COLUMN (ninth) failed, so the row on disk is no longer in \
             the shape the catalog describes: {e}"
        ),
    }
    assert_not_wedged(&mut db, "ADD COLUMN (ninth)", "after_add_ninth");
}
