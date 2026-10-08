//! D270 — `Catalog::persist` fails part way through writing its chain, and leaves a mixed image.
//!
//! D254 made `persist` refuse an entry the ENCODER cannot write before any page is written. The other
//! failures still come mid-write, because `persist` reads, allocates and writes page by page: turn k
//! reads page k, allocates page k+1 only when page k is the old chain's end, then writes page k. So
//! an error at turn k ≥ 2 leaves pages 1..k-1 rewritten and the old chain after them (review 1 of
//! D254, R3). The image can then lose tables, and it can record the one whose statement was refused.
//!
//! Two of those routes, each driven from an ordinary CREATE TABLE on a catalog of two pages:
//!
//! - **allocation**: the table region has exactly the pages the statement itself needs, and none for
//!   the catalog's third page;
//! - **reading**: the second chain page does not read back. It is corrupted in the pool, which stands
//!   in for any failed fetch or decode of a later chain page.
//!
//! Each test asserts that the refusal leaves the catalog's pages exactly as they were.
//!
//! **The harness bounds the allocator** (`reserve_region` at page 256) before the catalog exists.

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::catalog_page::{CatalogPage, TableEntry};
use ferrodb::catalog::column::{Column, DataType};
use ferrodb::catalog::schema::Schema;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
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
            .open(dir.path().join("d270.db"))
            .unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        dm.reserve_region("test floor", 256, u32::MAX).unwrap();
        let bp = Arc::new(BufferPoolManager::new(dm.clone()));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d270.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, dm, txn, _dir: dir }
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

    /// A page's bytes as the pool holds them.
    fn page_bytes(&self, id: u32) -> [u8; PAGE_SIZE] {
        let i = self.bp.fetch_page(id).unwrap();
        let data = self.bp.frames[i].read().unwrap().data;
        self.bp.unpin_page(id, false);
        data
    }

    /// The catalog's page chain, walked from its first page.
    fn chain(&self) -> Vec<u32> {
        let mut ids = Vec::new();
        let mut id = self.catalog.first_catalog_page_id;
        while id != 0 {
            ids.push(id);
            id = CatalogPage::deserialize(self.page_bytes(id)).unwrap().next_catalog_page;
        }
        ids
    }

    /// The tables the catalog's pages hold, read back through the same pool.
    fn tables_on_the_pages(&self) -> BTreeSet<String> {
        Catalog::open(self.bp.clone(), self.catalog.first_catalog_page_id)
            .expect("the catalog pages must still read back")
            .tables
            .keys()
            .cloned()
            .collect()
    }
}

/// Four INTEGER columns with 240-byte names: 991 bytes of catalog entry for a two-byte table name.
/// Four such entries fit one catalog page and five do not.
fn wide_columns() -> Vec<Column> {
    (0..4)
        .map(|i| Column::new(format!("k{i}{}", "x".repeat(238)), DataType::Integer, i != 0))
        .collect()
}

fn wide_table(name: &str) -> String {
    let decl: Vec<String> = wide_columns()
        .iter()
        .map(|c| if c.nullable { format!("{} INTEGER", c.name) } else { format!("{} INTEGER NOT NULL", c.name) })
        .collect();
    format!("CREATE TABLE {name} ({});", decl.join(", "))
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

/// How many pages a greedy placement of `entries`, in name order, takes: the placement `persist`
/// makes. A premise, computed through the page's own `has_space`.
fn pages_for(mut entries: Vec<TableEntry>) -> usize {
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let mut pages = 1;
    let mut page = CatalogPage::new(0);
    for e in entries {
        if !page.has_space(&e) {
            pages += 1;
            page = CatalogPage::new(0);
        }
        page.add_entry(e).expect("each entry fits an empty page");
    }
    pages
}

/// Eight wide tables `b1`..`b8`: a catalog of exactly two pages.
fn two_page_catalog() -> Db {
    let mut db = Db::new();
    for i in 1..=8 {
        db.exec(&wide_table(&format!("b{i}"))).unwrap();
    }
    assert_eq!(db.chain().len(), 2, "the fixture's catalog is not two pages, so this tests nothing");
    db
}

#[test]
fn a_refused_allocation_for_the_catalog_leaves_its_pages_as_they_were() {
    let mut db = two_page_catalog();
    let mut with_new: Vec<TableEntry> =
        db.catalog.tables.values().cloned().collect();
    with_new.push(entry_of("aaa", wide_columns()));
    assert_eq!(pages_for(with_new), 3, "adding the table does not need a third catalog page, so this tests nothing");

    // Leave exactly the three pages CREATE TABLE allocates for itself (heap, primary tree,
    // time-travel heap), and none for the catalog's third page.
    let mut taken = Vec::new();
    while let Ok(id) = db.dm.allocate() {
        taken.push(id);
    }
    assert!(taken.len() >= 3, "the region had fewer than three free pages");
    for id in taken.split_off(taken.len() - 3) {
        db.dm.deallocate(id).unwrap();
    }

    let before = db.tables_on_the_pages();
    let err = db
        .exec(&wide_table("aaa"))
        .err()
        .expect("with no page for the catalog, the CREATE TABLE must be refused");
    assert!(
        matches!(&err, FerroError::Io(m) if m.contains("no free page below")),
        "expected the region's refusal, got: {err:?}"
    );
    assert!(db.catalog.get_table("aaa").is_none(), "the refused table is in the catalog in memory");
    assert_eq!(
        db.tables_on_the_pages(),
        before,
        "the refused statement changed the catalog's pages: a restart would read this image"
    );
}

#[test]
fn a_catalog_page_that_does_not_read_back_is_found_before_any_page_is_written() {
    let mut db = two_page_catalog();
    let chain = db.chain();
    let first_before = db.page_bytes(chain[0]);

    // The second chain page stops reading back as a catalog page.
    let i = db.bp.fetch_page(chain[1]).unwrap();
    {
        let mut frame = db.bp.frame_write(i);
        frame.data[0] = 0xEE;
    }
    db.bp.unpin_page(chain[1], true);
    assert!(
        CatalogPage::deserialize(db.page_bytes(chain[1])).is_err(),
        "the damaged page still reads back, so this tests nothing"
    );

    let err = db
        .exec("CREATE TABLE aaa (id INTEGER NOT NULL);")
        .err()
        .expect("a catalog whose second page does not read back cannot be rewritten");
    assert!(matches!(err, FerroError::Corruption(_)), "expected the page's refusal, got: {err:?}");
    assert!(db.catalog.get_table("aaa").is_none(), "the refused table is in the catalog in memory");
    assert!(
        db.page_bytes(chain[0]) == first_before,
        "the first catalog page was rewritten before the second was read, so the pages now mix two images"
    );
}
