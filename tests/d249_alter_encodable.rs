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
use ferrodb::catalog::catalog_page::CatalogPage;
use ferrodb::catalog::column::{Column, DataType, Value};
use ferrodb::catalog::schema::Schema;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::{AlterAction, Parser};
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::tuple::Tuple;
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
        let dm = Arc::new(DiskManager::new(file).unwrap());
        // A floor for the page allocator, so that a mutant reaching `persist`'s loop over an entry no
        // page can hold (D254) refuses at page 256 instead of writing zero pages until the disk is
        // full. No test here needs more than a few dozen pages.
        dm.reserve_region("test floor", 256, u32::MAX).unwrap();
        let bp = Arc::new(BufferPoolManager::new(dm));
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

/// The ADD case again, on an eight-column table: it pins the wedge, and NOT where the refusal
/// happens.
///
/// It was written to pin the placement too, on the premise that the ninth column's wider null
/// bitmap moves every value. That premise is false (D249 review, F1): every INTEGER is padded to a
/// 4-byte boundary after the 24-byte version header, so eight columns (24 + 1 bytes) and nine
/// (24 + 2) both put the first value at 28, and a row rewritten under nine reads back unchanged
/// under eight. `deserialize` checks no length either. The test that pins the placement is the
/// 33-column one below.
#[test]
fn an_added_ninth_column_the_catalog_cannot_hold_is_refused_and_wedges_nothing() {
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
             columns"
        ),
        Ok(_) => panic!("SELECT after the refusal returned something other than rows"),
        Err(e) => panic!(
            "SELECT after the refused ADD COLUMN (ninth) failed, so the row on disk is no longer in \
             the shape the catalog describes: {e}"
        ),
    }
    assert_not_wedged(&mut db, "ADD COLUMN (ninth)", "after_add_ninth");
}

/// Where the refusal happens: before any row is rewritten (I19), on a table where a rewrite would
/// show.
///
/// With 32 INTEGER columns the null bitmap is 4 bytes, so the first value sits at 24 + 4 = 28 with no
/// padding. With 33 it is 5 bytes, padded to 32, so every value moves. A row rewritten under 33
/// columns does not read back under the old 32. The premise is asserted through the tuple layer
/// itself, and is not used as the expected value.
#[test]
fn a_thirty_third_column_the_catalog_cannot_hold_is_refused_before_any_row_is_rewritten() {
    let names: Vec<String> = (0..32).map(|i| if i == 0 { "id".to_string() } else { format!("c{i}") }).collect();
    let want: Vec<Value> = (1..=32).map(Value::Integer).collect();

    // Premise: a 33-column row does NOT read back as its first 32 values under the 32-column schema.
    let cols32: Vec<Column> =
        names.iter().enumerate().map(|(i, n)| Column::new(n.clone(), DataType::Integer, i != 0)).collect();
    let mut cols33 = cols32.clone();
    cols33.push(Column::new("added".to_string(), DataType::Integer, true));
    let mut values33 = want.clone();
    values33.push(Value::Null);
    let rewritten = Tuple::serialize(&values33, &Schema::new(cols33), 1).unwrap();
    assert!(
        !matches!(rewritten.deserialize(&Schema::new(cols32)), Ok(ref v) if *v == want),
        "a 33-column row reads back unchanged under 32 columns, so this fixture cannot tell a refusal \
         before the rewrite from one after it"
    );

    let mut db = Db::new();
    let decl: Vec<String> = names
        .iter()
        .enumerate()
        .map(|(i, n)| if i == 0 { format!("{n} INTEGER NOT NULL") } else { format!("{n} INTEGER") })
        .collect();
    db.exec(&format!("CREATE TABLE t ({});", decl.join(", "))).unwrap();
    let row: Vec<String> = (1..=32).map(|i| i.to_string()).collect();
    db.exec(&format!("INSERT INTO t VALUES ({});", row.join(", "))).unwrap();
    let long = over_long();

    let err = db
        .exec(&format!("ALTER TABLE t ADD COLUMN {long} INTEGER;"))
        .err()
        .expect("a 33rd column with a 300-byte name must be refused, not written truncated");
    assert_refused_by_name(&err, "ADD COLUMN (33rd)");

    let columns = db.catalog.get_table("t").expect("the table is still there").schema.columns.len();
    assert_eq!(columns, 32, "ADD COLUMN (33rd) was refused, but the catalog in memory holds {columns} columns");
    match db.exec("SELECT * FROM t;") {
        Ok(Outcome::Rows(rows)) => assert_eq!(
            rows,
            vec![want],
            "ADD COLUMN (33rd) was refused, but the row no longer reads back under the old 32 columns: it \
             was rewritten under 33 before the refusal (I19)"
        ),
        Ok(_) => panic!("SELECT after the refusal returned something other than rows"),
        Err(e) => panic!("SELECT after the refused ADD COLUMN (33rd) failed: {e}"),
    }
    assert_not_wedged(&mut db, "ADD COLUMN (33rd)", "after_add_33rd");
}

/// The size arm, reached through an INDEX rename.
///
/// A rename grows the entry twice when the renamed column is indexed: once in the schema, once in the
/// index record, which also names the column. Here the schema-only growth still fits a page and the
/// full growth does not, so a pre-check that forgot the index renames would pass it. Both halves are
/// premises, asserted through the page's own placement question.
#[test]
fn a_rename_that_grows_an_indexed_column_past_a_page_is_refused_by_size() {
    let mut db = Db::new();
    let fillers: Vec<String> = (1..=14).map(|i| format!("f{i:02}{}", "x".repeat(252))).collect();
    assert!(fillers.iter().all(|f| f.len() == 255), "each filler name must be exactly 255 bytes");
    let decl: Vec<String> = fillers.iter().map(|f| format!("{f} INTEGER")).collect();
    db.exec(&format!("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, {});", decl.join(", "))).unwrap();
    db.exec("CREATE INDEX ix ON t (v);").unwrap();
    let to = "w".repeat(255);

    // Premises: renaming the schema alone would fit an empty catalog page; renaming the index too does
    // not.
    let before = db.catalog.get_table("t").expect("the table is there").clone();
    let mut schema_only = before.clone();
    for c in schema_only.schema.columns.iter_mut() {
        if c.name == "v" {
            c.name = to.clone();
        }
    }
    let mut full = schema_only.clone();
    for ix in full.indexes.iter_mut() {
        if ix.column_name == "v" {
            ix.column_name = to.clone();
        }
    }
    assert!(
        CatalogPage::new(0).has_space(&schema_only),
        "the schema-only rename already overflows a page, so this cannot see the index half"
    );
    assert!(!CatalogPage::new(0).has_space(&full), "the full rename fits a page, so this tests nothing");

    let err = db
        .exec(&format!("ALTER TABLE t RENAME COLUMN v TO {to};"))
        .err()
        .expect("a rename that grows the entry past a catalog page must be refused");
    let msg = err.to_string();
    assert!(
        matches!(err, FerroError::Constraint(_)) && msg.contains("\"t\"") && msg.contains("catalog page"),
        "expected the size refusal naming the table, got: {err:?}"
    );
    let after = db.catalog.get_table("t").expect("the table is still there");
    assert!(
        after.schema.columns.iter().any(|c| c.name == "v") && after.indexes.iter().any(|ix| ix.column_name == "v"),
        "the rename was refused, but the catalog in memory holds the renamed column or index"
    );
    assert_not_wedged(&mut db, "RENAME COLUMN (indexed, past a page)", "after_index_rename");
}

/// A plan decides against the entry it read. Held across a change to the same table, it must be
/// refused, not installed over an entry nobody checked.
#[test]
fn a_plan_held_across_a_change_to_its_own_table_is_refused() {
    let mut db = Db::with_one_row();
    let rename = AlterAction::RenameColumn { from: "v".to_string(), to: "w".to_string() };
    let plan = db.catalog.plan_alters("t", std::slice::from_ref(&rename), &db.txn, None).unwrap();

    // The table changes while the plan is held: an index on the column the plan renames.
    db.catalog.create_index("t", "v").unwrap();

    match db.catalog.apply_plan(plan, &db.txn) {
        Err(FerroError::Constraint(msg)) => assert!(
            msg.contains("changed since"),
            "the stale plan was refused, but not by the staleness check: {msg}"
        ),
        Err(e) => panic!("the stale plan was refused for some other reason: {e:?}"),
        Ok(_) => panic!("a plan made before the table changed was applied anyway"),
    }
    let entry = db.catalog.get_table("t").expect("the table is still there");
    assert!(
        entry.schema.columns.iter().any(|c| c.name == "v") && entry.indexes.iter().any(|ix| ix.column_name == "v"),
        "the stale plan was refused, but the table no longer has its column v with its index"
    );
}

/// The as-read check covers the SCHEMA: a plan held across another ALTER of the same table must be
/// refused, not laid over it.
///
/// ST's intervening change is an index, which leaves the schema alone, so ST cannot tell a
/// comparison that includes the schema from one that overwrites it first. Here the intervening
/// change is a schema change that moves no root and no index, and a plan that rewrites rows, so a
/// stale apply would install `[id, v, y]` over the `[id, v, x]` the table now has.
#[test]
fn a_plan_held_across_another_alter_of_its_own_table_is_refused() {
    let mut db = Db::with_one_row();
    let add_y = AlterAction::AddColumn(Column::new("y".to_string(), DataType::Integer, true));
    let plan = db.catalog.plan_alters("t", std::slice::from_ref(&add_y), &db.txn, None).unwrap();

    // The table changes while the plan is held: another column, by another statement.
    db.exec("ALTER TABLE t ADD COLUMN x INTEGER;").unwrap();

    match db.catalog.apply_plan(plan, &db.txn) {
        Err(FerroError::Constraint(msg)) => assert!(
            msg.contains("changed since"),
            "the stale plan was refused, but not by the staleness check: {msg}"
        ),
        Err(e) => panic!("the stale plan was refused for some other reason: {e:?}"),
        Ok(_) => panic!("a plan made before another ALTER of the same table was applied over it"),
    }
    let names: Vec<String> =
        db.catalog.get_table("t").expect("the table is still there").schema.columns.iter().map(|c| c.name.clone()).collect();
    assert_eq!(names, vec!["id".to_string(), "v".to_string(), "x".to_string()], "the table's shape moved");
    match db.exec("SELECT * FROM t;") {
        Ok(Outcome::Rows(rows)) => assert_eq!(
            rows,
            vec![vec![Value::Integer(1), Value::Integer(7), Value::Null]],
            "the row no longer reads back as (1, 7, NULL) under [id, v, x]"
        ),
        Ok(_) => panic!("SELECT after the refusal returned something other than rows"),
        Err(e) => panic!("SELECT after the refused stale plan failed: {e}"),
    }
}
