//! D271 — **a CREATE [FULLTEXT] INDEX that is refused leaves nothing behind.**
//!
//! Until D271 the executor ran `create_index` (build the tree, push its record, persist, seed its
//! cell) and THEN a checkpoint that refused while ANY session's transaction was open. So with
//! another session inside `BEGIN`, the statement answered `Err` over an index that was already
//! attached; the next checkpoint, from anyone, made it durable. Had none run, the tree's pages
//! would have stayed allocated in the on-disk bitmap with nothing naming them.
//!
//! Now the tree is built unattached, and the attach runs with the checkpoint as one unit under
//! `TxnManager::ddl_checkpointed`. A unit refused before the attach persisted frees the tree. After
//! a failed persist the tree is KEPT, because a catalog page in the buffer pool may already name it.
//!
//! - **T1, T2:** another session's `BEGIN` is open, and CREATE [FULLTEXT] INDEX must fail. After
//!   that session ends and the database checkpoints, no page may be left allocated. After a reopen
//!   there must be no index.
//! - **T3, T4:** at the catalog level, a `create_index` / `create_fulltext_index` whose persist
//!   fails AFTER writing catalog page 1 into the pool must keep the tree that page names, and say
//!   so. Lane §9 withdrew the first T3/T4, which pinned the opposite.
//!
//! # The instrument
//!
//! `DiskManager::bitmap_high_water()`: one past the highest page the on-disk bitmap marks
//! allocated. `free_page` clears the bit on disk at once (`DiskManager::deallocate`), and
//! `allocate` hands out the lowest free bit, so a database that has freed nothing allocates in
//! order and a tree left allocated at the top reads as a higher mark. Blind spot, stated: an equal
//! mark proves every page came back only while no page below the mark is free, which holds here
//! because nothing below the table's pages is freed before the measurement. The same instrument as
//! `tests/d222_index_root_after_backfill.rs`.
//!
//! T1 and T2 read the mark BEFORE the reopen. A reopen after any DDL recovers and rebuilds every
//! index (D216: every checkpoint re-appends the process's DDL to the log), and the rebuild frees and
//! reallocates their pages, which moves the mark.
//!
//! # Red evidence
//!
//! Base API only (SQL, `Catalog`'s public fields and methods, the buffer pool's frames), so this
//! file compiles at the base `340fbf8` (#16 `fe2fd84` with D222 merged). There every test fails at
//! its named assertion: T1 and T2 at red 1, T3 and T4 at red 2.

use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::catalog_page::{CatalogPage, TableEntry};
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::BPlusTreePage;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::{rebuild_indexes, recover};
use ferrodb::wal::txn::TxnManager;

/// Rows in `t`: enough 200-byte keys that the index cannot fit one page.
const ROWS: i32 = 300;
/// Bytes of padding after the six-character prefix: 200-byte values.
const PAD: usize = 194;

#[derive(Clone, Copy, Debug)]
enum Kind {
    BTree,
    FullText,
}

impl Kind {
    fn create(self) -> &'static str {
        match self {
            Kind::BTree => "CREATE INDEX iv ON t (v);",
            Kind::FullText => "CREATE FULLTEXT INDEX fv ON t (v);",
        }
    }

    /// Row `i`'s value. For full-text, one 200-byte token: every character is alphanumeric.
    fn text(self, i: i32) -> String {
        match self {
            Kind::BTree => format!("k{i:05}{}", "x".repeat(PAD)),
            Kind::FullText => format!("w{i:05}{}", "x".repeat(PAD)),
        }
    }

    /// The root of this kind's index on `v`, as the table record `t` says, if there is one.
    fn root_in(self, t: &TableEntry) -> Option<u32> {
        match self {
            Kind::BTree => t.indexes.iter().find(|i| i.column_name == "v").map(|i| i.root_page_id),
            Kind::FullText => t.fulltext_indexes.iter().find(|i| i.column_name == "v").map(|i| i.root_page_id),
        }
    }

    /// The root of this kind's index on `t.v`, as `catalog` records it, if there is one.
    fn root(self, catalog: &Catalog) -> Option<u32> {
        self.root_in(catalog.get_table("t").expect("table t"))
    }

    /// `catalog.create_index` or `catalog.create_fulltext_index`, called directly.
    fn create_directly(self, catalog: &mut Catalog) -> Result<(), FerroError> {
        match self {
            Kind::BTree => catalog.create_index("t", "v"),
            Kind::FullText => catalog.create_fulltext_index("t", "v"),
        }
    }
}

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

/// Open `dir`'s database as `cli::run_cli` does at `9aa6968`.
fn open(dir: &Path) -> Db {
    let path = dir.join("d271.db");
    let existed = path.exists();
    let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.join("d271.db.wal")).unwrap());
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
    Db { catalog, bp, txn }
}

impl Db {
    /// Run one statement in `session`, as the executor runs it for that session.
    fn sql_in(&mut self, session: &mut Session, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        assert_eq!(stmts.len(), 1, "one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), session)
    }

    fn high_water(&self) -> u32 {
        self.bp.disk_manager.bitmap_high_water().expect("read the allocation bitmap")
    }

    /// Catalog page 1 as the BUFFER POOL holds it, which is what the next checkpoint writes.
    fn pool_catalog_page_one(&self) -> CatalogPage {
        let first = self.catalog.first_catalog_page_id;
        let i = self.bp.fetch_page(first).expect("fetch catalog page 1");
        let data = self.bp.frames[i].read().unwrap().data;
        self.bp.unpin_page(first, false);
        CatalogPage::deserialize(data).expect("catalog page 1 in the pool")
    }

    /// Point catalog page 1's `next_catalog_page`, in the pool, at `page`.
    fn point_the_catalog_tail_at(&self, page: u32) {
        let first = self.catalog.first_catalog_page_id;
        let i = self.bp.fetch_page(first).expect("fetch catalog page 1");
        let mut cat = CatalogPage::deserialize(self.bp.frames[i].read().unwrap().data).expect("catalog page 1");
        cat.next_catalog_page = page;
        self.bp.frame_write(i).data = cat.serialize().expect("serialize catalog page 1");
        self.bp.unpin_page(first, true);
    }
}

/// `t (id, v)` with `ROWS` rows of `kind`'s values, then a checkpoint.
fn table_t(d: &mut Db, kind: Kind) {
    let mut s = Session::new();
    d.sql_in(&mut s, "CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(255));").expect("create t");
    for i in 1..=ROWS {
        d.sql_in(&mut s, &format!("INSERT INTO t VALUES ({i}, '{}');", kind.text(i)))
            .unwrap_or_else(|e| panic!("insert row {i}: {e}"));
    }
    d.txn.checkpoint().expect("the checkpoint after filling t");
}

/// Premise, run last: with nothing refusing it, the same CREATE builds a tree whose root is an
/// internal page, so the build the test refused had more than one page to leak.
fn assert_the_build_has_pages_to_leak(d: &mut Db, kind: Kind) {
    d.sql_in(&mut Session::new(), kind.create())
        .unwrap_or_else(|e| panic!("premise failed: `{}` failed with nothing refusing it: {e}", kind.create()));
    let root = kind.root(&d.catalog).expect("premise failed: the CREATE left no record");
    assert!(
        matches!(
            BPlusTreeManager::<(Value, Value), ()>::open(root, d.bp.clone()).read_node(root),
            Ok(BPlusTreePage::Internal(_))
        ),
        "premise failed: {ROWS} rows built a one-page {kind:?} tree, so a refused build had little to leak"
    );
}

/// T1, T2: another session's `BEGIN` is open while CREATE [FULLTEXT] INDEX runs.
fn refused_under_another_sessions_transaction(kind: Kind) {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    table_t(&mut a, kind);
    let before = a.high_water();

    let mut other = Session::new();
    a.sql_in(&mut other, "BEGIN;").expect("the other session's BEGIN");
    let refused = a
        .sql_in(&mut Session::new(), kind.create())
        .err()
        .unwrap_or_else(|| panic!("premise failed: `{}` succeeded with another session's transaction open", kind.create()));
    assert!(
        refused.to_string().contains("active txns"),
        "premise failed: `{}` was refused for another reason: {refused}",
        kind.create()
    );
    a.sql_in(&mut other, "ROLLBACK;").expect("the other session's ROLLBACK");
    // The clean exit: any checkpoint after the refusal is what made the refused index durable.
    a.txn.checkpoint().expect("the checkpoint at exit");
    // Read before the reopen: the reopen recovers and rebuilds (D216), which moves the mark.
    let after = a.high_water();
    drop(a);

    let mut d = open(dir.path());
    assert_eq!(
        kind.root(&d.catalog),
        None,
        "the refused `{}` is in the reopened catalog: it answered Err over an index a later checkpoint made durable",
        kind.create()
    );
    assert_eq!(
        after,
        before,
        "the refused `{}` left {} page(s) allocated that nothing names",
        kind.create(),
        after.saturating_sub(before)
    );
    assert_the_build_has_pages_to_leak(&mut d, kind);
}

#[test]
fn a_create_index_refused_for_another_sessions_transaction_leaves_no_index_and_no_pages() {
    refused_under_another_sessions_transaction(Kind::BTree);
}

#[test]
fn a_create_fulltext_index_refused_for_another_sessions_transaction_leaves_no_index_and_no_pages() {
    refused_under_another_sessions_transaction(Kind::FullText);
}

/// T3, T4: the attach's persist fails AFTER it wrote catalog page 1 into the pool. Page 1's
/// `next_catalog_page` points at `t`'s primary root, so to `persist` that page is an orphan tail,
/// which it frees after writing page 1; `CatalogPage::deserialize` refuses the B+tree page there.
fn attach_persist_fails_after_writing(kind: Kind) {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    table_t(&mut a, kind);
    let before = a.high_water();
    assert_eq!(
        a.pool_catalog_page_one().next_catalog_page,
        0,
        "premise failed: the catalog is more than one page, so the injection would replace a live page"
    );

    a.point_the_catalog_tail_at(a.catalog.get_table("t").expect("t").primary_index_root);
    let refused = kind
        .create_directly(&mut a.catalog)
        .err()
        .unwrap_or_else(|| panic!("premise failed: the {kind:?} create succeeded although its persist cannot free the tail"))
        .to_string();
    assert!(
        refused.contains("catalog page format"),
        "premise failed: the {kind:?} create was refused for another reason than its persist: {refused}"
    );
    assert_eq!(
        kind.root(&a.catalog),
        None,
        "premise failed: the refused {kind:?} create left its record in memory"
    );
    let page_one = a.pool_catalog_page_one();
    let named = kind
        .root_in(page_one.entries.iter().find(|e| e.name == "t").expect("premise failed: t is not on catalog page 1"))
        .unwrap_or_else(|| {
            panic!("premise failed: the failed persist did not write catalog page 1 naming the {kind:?} index, so nothing names the tree")
        });
    assert!(
        named >= before,
        "premise failed: catalog page 1 names root {named}, below the mark {before} the build allocated from"
    );

    let mark = a.high_water();
    assert!(
        named < mark,
        "catalog page 1 in the pool names the {kind:?} root {named}, which the failed create freed (mark {mark}): the \
         next checkpoint makes a catalog naming free pages durable"
    );
    assert!(
        refused.contains("stays allocated"),
        "the {kind:?} create whose persist failed kept its tree without saying so: {refused}"
    );
    assert_the_build_has_pages_to_leak(&mut a, kind);
}

#[test]
fn a_create_index_whose_persist_fails_after_writing_the_catalog_keeps_the_tree_it_names() {
    attach_persist_fails_after_writing(Kind::BTree);
}

#[test]
fn a_create_fulltext_index_whose_persist_fails_after_writing_the_catalog_keeps_the_tree_it_names() {
    attach_persist_fails_after_writing(Kind::FullText);
}
