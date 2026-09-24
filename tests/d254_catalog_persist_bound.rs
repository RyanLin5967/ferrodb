//! D254 — a catalog entry larger than one catalog page, driven from ordinary DDL.
//!
//! `Catalog::persist` places each table's entry on a page with `has_space`, and `break`s at the first
//! entry that does not fit. An entry larger than an EMPTY page fits nowhere, so the loop links a new,
//! empty catalog page on every turn and never advances past it, until page allocation refuses. What
//! it leaves:
//!
//! - every free page below the region floor used up;
//! - a catalog image that has lost every table sorting after the refused one;
//! - and, from CREATE TABLE and CREATE INDEX, the pages each allocated before persisting, never
//!   freed.
//!
//! Ordinary identifiers reach it: 150 columns with 25-byte names are enough.
//!
//! Each test holds a table `zz`, which sorts after the one refused, and asserts three things:
//!
//! - the statement is refused by the size check, naming the table;
//! - the catalog is intact, in memory AND as its pages read back (`Catalog::open` over the same
//!   pool), with `zz` still there;
//! - no page was allocated. The fixture has freed nothing, so the bitmap has no holes, and ANY
//!   allocation during the statement raises `bitmap_high_water`. That is the blind spot: an
//!   allocation into a hole below the mark would not show, and there is no hole here to use.
//!
//! **Every harness bounds the allocator** (`reserve_region` at page 256) BEFORE the catalog exists,
//! so the unfixed loop, or a mutant of the fix, refuses at page 256 instead of filling the disk.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::catalog_page::{CatalogPage, FullTextIndexInfo, IndexInfo, TableEntry};
use ferrodb::catalog::column::{Column, DataType};
use ferrodb::catalog::schema::Schema;
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
    dm: Arc<DiskManager>,
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
            .open(dir.path().join("d254.db"))
            .unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        dm.reserve_region("test floor", 256, u32::MAX).unwrap();
        let bp = Arc::new(BufferPoolManager::new(dm.clone()));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d254.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let mut db = Db { catalog, bp, dm, txn, _dir: dir };
        // The table that sorts after every refused one, so a truncated catalog image loses it.
        db.exec("CREATE TABLE zz (id INTEGER NOT NULL);").unwrap();
        db
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

    fn high_water(&self) -> u32 {
        self.dm.bitmap_high_water().unwrap()
    }

    /// The tables the catalog's pages hold, read back through the same pool.
    fn tables_on_the_pages(&self) -> Vec<String> {
        let reread = Catalog::open(self.bp.clone(), self.catalog.first_catalog_page_id)
            .expect("the catalog pages must still read back");
        let mut names: Vec<String> = reread.tables.keys().cloned().collect();
        names.sort();
        names
    }
}

/// `n` distinct names of exactly `len` bytes.
fn names(prefix: char, n: usize, len: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            let head = format!("{prefix}{i:03}");
            format!("{head}{}", "x".repeat(len - head.len()))
        })
        .collect()
}

fn entry_of(name: &str, columns: Vec<Column>) -> TableEntry {
    TableEntry {
        name: name.to_string(),
        first_directory_page_id: 0,
        primary_index_root: 0,
        time_travel_root: 0,
        schema: Schema::new(columns),
        indexes: Vec::new(),
        fulltext_indexes: Vec::new(),
    }
}

fn assert_refused_by_size(err: &FerroError, table: &str, statement: &str) {
    let msg = err.to_string();
    assert!(
        matches!(err, FerroError::Constraint(_))
            && msg.contains(&format!("\"{table}\""))
            && msg.contains("catalog page"),
        "{statement}: expected the size refusal naming table {table}, got: {err:?}"
    );
}

fn assert_catalog_intact(db: &Db, present: &[&str], absent: &str, statement: &str) {
    for t in present {
        assert!(db.catalog.get_table(t).is_some(), "{statement}: table {t} is gone from the catalog in memory");
    }
    let on_pages = db.tables_on_the_pages();
    for t in present {
        assert!(
            on_pages.iter().any(|n| n == t),
            "{statement}: table {t} is gone from the catalog's pages, which list only {on_pages:?}. A \
             restart would lose it"
        );
    }
    assert!(
        !on_pages.iter().any(|n| n == absent) && db.catalog.get_table(absent).is_none(),
        "{statement}: the refused table {absent} is in the catalog"
    );
}

#[test]
fn a_create_table_whose_entry_exceeds_a_page_is_refused_and_leaves_the_catalog_and_the_pages_alone() {
    let mut db = Db::new();
    let cols = names('c', 150, 25);
    // Premise: the entry, with its ordinary 25-byte names, is larger than an empty catalog page.
    let columns: Vec<Column> =
        cols.iter().enumerate().map(|(i, n)| Column::new(n.clone(), DataType::Integer, i != 0)).collect();
    assert!(
        !CatalogPage::new(0).has_space(&entry_of("m", columns)),
        "150 columns of 25 bytes fit an empty catalog page, so this tests nothing"
    );

    let before = db.high_water();
    let decl: Vec<String> = cols
        .iter()
        .enumerate()
        .map(|(i, n)| if i == 0 { format!("{n} INTEGER NOT NULL") } else { format!("{n} INTEGER") })
        .collect();
    let err = db
        .exec(&format!("CREATE TABLE m ({});", decl.join(", ")))
        .err()
        .expect("a table whose catalog entry no page can hold must be refused");
    assert_refused_by_size(&err, "m", "CREATE TABLE");
    assert_eq!(db.high_water(), before, "the refused CREATE TABLE allocated pages it never freed");
    assert_catalog_intact(&db, &["zz"], "m", "CREATE TABLE");
    db.exec("CREATE TABLE after_table (id INTEGER NOT NULL);")
        .expect("an ordinary CREATE TABLE after the refusal must still work");
}

#[test]
fn a_create_index_that_grows_an_entry_past_a_page_is_refused_and_leaves_the_catalog_and_the_pages_alone() {
    let mut db = Db::new();
    let cols = names('c', 15, 255);
    let decl: Vec<String> = cols
        .iter()
        .enumerate()
        .map(|(i, n)| if i == 0 { format!("{n} INTEGER NOT NULL") } else { format!("{n} INTEGER") })
        .collect();
    db.exec(&format!("CREATE TABLE t ({});", decl.join(", "))).unwrap();

    // Premise: the table's entry fits a page, and the same entry with the index does not.
    let mut with_index = db.catalog.get_table("t").expect("t exists").clone();
    assert!(CatalogPage::new(0).has_space(&with_index), "the table itself does not fit, so this is not CREATE INDEX");
    with_index.indexes.push(IndexInfo { column_name: cols[1].clone(), root_page_id: 0 });
    assert!(!CatalogPage::new(0).has_space(&with_index), "the entry with its index still fits, so this tests nothing");

    let before = db.high_water();
    let err = db
        .exec(&format!("CREATE INDEX ix ON t ({});", cols[1]))
        .err()
        .expect("an index that grows the entry past a catalog page must be refused");
    assert_refused_by_size(&err, "t", "CREATE INDEX");
    assert_eq!(db.high_water(), before, "the refused CREATE INDEX allocated pages it never freed");
    assert_catalog_intact(&db, &["t", "zz"], "m", "CREATE INDEX");
    assert!(db.catalog.get_table("t").unwrap().indexes.is_empty(), "the refused index is on the table in memory");
    db.exec("CREATE TABLE after_index (id INTEGER NOT NULL);")
        .expect("an ordinary CREATE TABLE after the refusal must still work");
}

#[test]
fn a_create_fulltext_index_that_grows_an_entry_past_a_page_is_refused_and_leaves_the_catalog_and_the_pages_alone() {
    let mut db = Db::new();
    let cols = names('c', 15, 255);
    let decl: Vec<String> = cols
        .iter()
        .enumerate()
        .map(|(i, n)| match i {
            0 => format!("{n} INTEGER NOT NULL"),
            1 => format!("{n} VARCHAR(20)"),
            _ => format!("{n} INTEGER"),
        })
        .collect();
    db.exec(&format!("CREATE TABLE t ({});", decl.join(", "))).unwrap();

    let mut with_index = db.catalog.get_table("t").expect("t exists").clone();
    assert!(
        CatalogPage::new(0).has_space(&with_index),
        "the table itself does not fit, so this is not CREATE FULLTEXT INDEX"
    );
    with_index.fulltext_indexes.push(FullTextIndexInfo { column_name: cols[1].clone(), root_page_id: 0 });
    assert!(
        !CatalogPage::new(0).has_space(&with_index),
        "the entry with its full-text index still fits, so this tests nothing"
    );

    let before = db.high_water();
    let err = db
        .exec(&format!("CREATE FULLTEXT INDEX ix ON t ({});", cols[1]))
        .err()
        .expect("a full-text index that grows the entry past a catalog page must be refused");
    assert_refused_by_size(&err, "t", "CREATE FULLTEXT INDEX");
    assert_eq!(db.high_water(), before, "the refused CREATE FULLTEXT INDEX allocated pages it never freed");
    assert_catalog_intact(&db, &["t", "zz"], "m", "CREATE FULLTEXT INDEX");
    assert!(
        db.catalog.get_table("t").unwrap().fulltext_indexes.is_empty(),
        "the refused full-text index is on the table in memory"
    );
    db.exec("CREATE TABLE after_fulltext (id INTEGER NOT NULL);")
        .expect("an ordinary CREATE TABLE after the refusal must still work");
}

/// `persist` itself, for an entry that reached the catalog map by a path with no check of its own.
///
/// The DDL paths are checked before they allocate. That makes `persist`'s own check unreachable from
/// them, so it is driven here directly: an oversized entry is put into the map, and `persist` must
/// refuse it before it writes any page. Otherwise it truncates the image to the tables sorting
/// before it, and links empty pages until the floor.
#[test]
fn persist_refuses_an_entry_no_page_can_hold_before_writing_any_page() {
    let mut db = Db::new();
    let columns: Vec<Column> = names('c', 150, 25)
        .into_iter()
        .enumerate()
        .map(|(i, n)| Column::new(n, DataType::Integer, i != 0))
        .collect();
    let oversized = entry_of("m", columns);
    assert!(!CatalogPage::new(0).has_space(&oversized), "the entry fits an empty page, so this tests nothing");

    let before = db.high_water();
    db.catalog.tables.insert("m".to_string(), oversized);
    let err = db.catalog.persist().err().expect("persist must refuse an entry no catalog page can hold");
    db.catalog.tables.remove("m");

    assert_refused_by_size(&err, "m", "persist");
    assert_eq!(db.high_water(), before, "persist allocated pages for an entry it then refused");
    assert_catalog_intact(&db, &["zz"], "m", "persist");
}
