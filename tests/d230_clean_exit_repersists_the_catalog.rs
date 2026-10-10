//! D230, the clean exit — **a root whose persist failed is written by the exit, or the exit fails
//! loudly and leaves the log to rebuild from.**
//!
//! `sync_roots` records a moved root in the in-memory catalog even when its `persist` fails
//! (`tests/d230_root_sync_on_every_exit.rs`, T3). That leaves the catalog PAGE behind the tree, and
//! the next persist of any table would put it right. But a process can exit before one happens. The
//! clean exit used to only checkpoint, which flushes the stale page and truncates the log, so the
//! next open found an empty log, did not rebuild, and read the pre-split root. The exit now goes
//! through `cli::exit_sequence`, the function `cli::run_cli` calls, which runs
//! `catalog::clean_exit::checkpoint_for_exit` and then the arena's checkpoint. These tests call
//! `exit_sequence` itself, so a mutant of the CLI's exit wiring changes code they run (D230 review
//! 3, F3(b)). Its arena is a scratch one in its own files (`scratch_arena`):
//!
//! - **T4:** the fault that failed the statement's persist has cleared by the exit. The exit writes
//!   the new root, then checkpoints. The next open does not rebuild and reads the real root.
//! - **T5:** the fault is still there at the exit. The exit refuses, names the tree's root, and
//!   flushes the log without truncating it. The next open recovers and rebuilds. (Its injection makes
//!   the read-back unreadable too, so it reaches the branch that names every in-memory root; the
//!   comparison branch is pinned by the lib unit test. D230 review 3, F7.)
//! - **F2** (on the #16 resolution only, where `open_recovered` exists): a failed persist, then the
//!   AUTOMATIC checkpoint, then no exit at all. While a persist is owed every checkpoint keeps the
//!   log, so the next open rebuilds. Red at `0ba2295`, before the debt existed.
//!
//! # How the persist is made to fail
//!
//! `Catalog::first_catalog_page_id` is pointed at a B+tree leaf. Every `persist` reads that page
//! first, and `CatalogPage::deserialize` refuses a page whose type byte is a B+tree's (2 or 3)
//! rather than a catalog page's (4 or 5). The same injection as T3.
//!
//! # Red evidence
//!
//! These tests call `exit_sequence`, which does not exist at `9aa6968`, so this file cannot
//! compile there. It lives apart from `d230_root_sync_on_every_exit.rs` so that file's red tree
//! still builds. Its red is shown by mutants that remove each half of the exit (the lane report
//! lists them).
//!
//! # Fixture
//!
//! `t (id)` with 369 rows is one primary leaf of 369 × 11-byte entries (27 + 369·11 = 4086 bytes).
//! Row 370 splits the root, keeping keys 1..=185 on the left.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::binder::binder::Binder;
use ferrodb::branch::{ArenaPageStore, BranchCatalog, TableBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cli::cli::exit_sequence;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::{explain_plan, optimize, pushdown};
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::storage::index_page::{BPlusTreePage, BPLUS_INTERNAL_TYPE};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::{open_recovered, rebuild_indexes, recover, OpenedDatabase};
use ferrodb::wal::txn::TxnManager;

/// Rows before the split: the primary's single leaf is one entry short of full.
const FULL_LESS_ONE: i32 = 369;
/// A key in the right half after the split.
const RIGHT_KEY: i32 = 369;

type Primary = BPlusTreeManager<Value, RecordId>;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

/// Open `dir` exactly as `cli::run_cli` does at `9aa6968`, and say whether it rebuilt.
fn open(dir: &Path) -> (Db, bool) {
    let path = dir.join("d230x.db");
    let existed = path.exists();
    let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    // `<db>.wal`, as `run_cli` (`format!("{db_path}.wal")`) and `open_recovered` name it, so a phase
    // opened either way reads the same log (D230 review 8, R8-2).
    let wal = Arc::new(WalManager::new(dir.join("d230x.db.wal")).unwrap());
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

/// The arena `exit_sequence` checkpoints last: a scratch one, in its own files, over its own branch
/// catalog. T4 and T5 are about the catalog and the log. The arena step runs only because it is
/// part of the sequence the CLI runs, and a scratch arena keeps it off this fixture's pages.
///
/// Its branch catalog is returned too: the joined `exit_sequence` publishes the branch catalog's
/// root first (D244's step; merge resolve-recovery-16, a call-site port, no assertion changed).
fn scratch_arena() -> (tempfile::TempDir, Arc<TableBranchCatalog>, ArenaPageStore, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new().read(true).write(true).create(true).open(dir.path().join("arena.db")).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let branches = Arc::new(TableBranchCatalog::create(pool.clone(), 7).expect("a scratch branch catalog"));
    let base = pool.disk_manager.high_water().expect("high water") + 64;
    let store = ArenaPageStore::new(pool, branches.clone() as Arc<dyn BranchCatalog>, base).expect("a scratch arena");
    let path = dir.path().join("scratch.arena");
    (dir, branches, store, path)
}

/// The clean exit, through `cli::exit_sequence`, the function `cli::run_cli` calls.
fn exit_through_the_cli_sequence(d: &Db) -> Result<(), FerroError> {
    let (_dir, branches, arena, arena_path) = scratch_arena();
    exit_sequence(&branches, &d.catalog, &d.txn, &arena, &arena_path)
}

/// A clean exit through the function the CLI's exit calls, then every handle dropped.
fn exit_cleanly(d: Db) {
    exit_through_the_cli_sequence(&d).expect("the clean exit");
    drop(d);
}

fn open_without_rebuild(dir: &Path, phase: &str) -> Db {
    let (d, recovered) = open(dir);
    assert!(
        !recovered,
        "premise failed: phase {phase}'s open found a log to replay and rebuilt every index, which \
         would hide a stale catalog page. The previous process must end with an empty log."
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

    fn explain(&self, sql: &str) -> String {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        let logical = Binder::new(&self.catalog).bind(stmts.remove(0)).expect("bind");
        let physical = optimize(pushdown(logical), &self.catalog).expect("optimize");
        explain_plan(&physical, &self.catalog)
    }

    fn primary_root(&self) -> u32 {
        self.catalog.get_table("t").expect("table t").primary_index_root
    }

    /// The root the shared cell holds: where a published split puts the new root.
    fn primary_cell(&self) -> u32 {
        self.catalog
            .root_cell("t", None)
            .expect("t's primary index has a shared root cell")
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// The primary tree's leaves as `(page, entries)`, left to right, by the leaf chain from `root`.
fn leaf_chain(bp: &Arc<BufferPoolManager>, root: u32) -> Vec<(u32, usize)> {
    let tree = Primary::open(root, bp.clone());
    let mut leaf = tree.leftmost_leaf().expect("descend to a leaf");
    while let Some(prev) = leaf.prev {
        leaf = tree.read_leaf(prev).expect("read a leaf");
    }
    let mut chain = Vec::new();
    loop {
        chain.push((leaf.page_id, leaf.key_arr.len()));
        assert!(chain.len() < 100_000, "the leaf chain loops");
        match leaf.next {
            Some(n) => leaf = tree.read_leaf(n).expect("read a leaf"),
            None => return chain,
        }
    }
}

fn leaves_under(tree: &Primary, page: u32, out: &mut Vec<u32>) {
    match tree.read_node(page).expect("read a tree page") {
        BPlusTreePage::Leaf(_) => out.push(page),
        BPlusTreePage::Internal(node) => {
            for child in node.child_ptrs {
                leaves_under(tree, child, out);
            }
        }
    }
}

/// The recorded root must be the tree's root: its descent reaches every leaf of the chain, in order.
fn assert_record_is_the_root(d: &Db) {
    let recorded = d.primary_root();
    let chain: Vec<u32> = leaf_chain(&d.bp, recorded).into_iter().map(|(p, _)| p).collect();
    let mut reached = Vec::new();
    leaves_under(&Primary::open(recorded, d.bp.clone()), recorded, &mut reached);
    assert!(
        reached == chain,
        "the persisted root, page {recorded}, reaches {} of the {} leaves",
        reached.len(),
        chain.len()
    );
}

/// Phases A and B: `t` with 369 rows, then one restart so the log A's DDL left is rebuilt from and
/// the next open finds it empty.
fn built_then_restarted(dir: &Path) {
    built_then_restarted_with(dir, &[]);
}

/// [`built_then_restarted`], with `extra` statements run in A after `t` is filled.
fn built_then_restarted_with(dir: &Path, extra: &[&str]) {
    let (mut a, _) = open(dir);
    a.sql("CREATE TABLE t (id INTEGER NOT NULL);");
    for i in 1..=FULL_LESS_ONE {
        a.sql(&format!("INSERT INTO t VALUES ({i});"));
    }
    for stmt in extra {
        a.sql(stmt);
    }
    a.txn.checkpoint().expect("A's exit");
    drop(a);
    let (b, _) = open(dir);
    b.txn.checkpoint().expect("B's exit");
    drop(b);
}

/// Phase C up to the failed statement: row 370 splits the primary root while every `persist`
/// fails. The in-memory record then holds the new root and the catalog page does not.
///
/// Returns the real `first_catalog_page_id` and the new root. The injection stays in place; the
/// caller decides whether the fault clears before the exit.
fn split_the_root_with_persist_failing(c: &mut Db) -> (u32, u32) {
    let chain = leaf_chain(&c.bp, c.primary_root());
    assert_eq!(
        chain.iter().map(|(_, n)| *n).collect::<Vec<_>>(),
        vec![FULL_LESS_ONE as usize],
        "premise failed: t's primary must be ONE leaf of {FULL_LESS_ONE} entries, so row 370 splits \
         its root"
    );
    let root_before = c.primary_root();
    let page_one = c.catalog.first_catalog_page_id;
    // A B+tree leaf: every persist from here reads it as the first catalog page and refuses it.
    c.catalog.first_catalog_page_id = root_before;

    let refused = c
        .try_sql("INSERT INTO t VALUES (370);")
        .err()
        .expect("premise failed: the INSERT's root sync must fail on the persist");
    assert!(
        refused.to_string().contains("catalog page format"),
        "premise failed: the INSERT was refused for another reason than the persist: {refused}"
    );
    let root_now = c.primary_cell();
    assert!(
        root_now != root_before
            && matches!(Primary::open(root_now, c.bp.clone()).read_node(root_now), Ok(BPlusTreePage::Internal(_))),
        "premise failed: the primary root did not move to a new internal page (was {root_before}, \
         now {root_now})"
    );
    assert_eq!(
        c.primary_root(),
        root_now,
        "premise failed: the in-memory record must hold the new root although its persist failed \
         (D230's sync; T3 of d230_root_sync_on_every_exit)"
    );
    (page_one, root_now)
}

/// The lookup the stale page breaks, and its plan premise: a bare primary index scan.
fn assert_right_key_found(d: &mut Db) {
    let lookup = format!("SELECT id FROM t WHERE id = {RIGHT_KEY};");
    let plan = d.explain(&lookup);
    assert!(
        plan.contains("Index scan on t (col 0, [") && !plan.contains("Filter") && !plan.contains("Sequential scan"),
        "premise failed: `{lookup}` must be a bare primary index scan. Plan:\n{plan}"
    );
    let got = d.ids(&lookup);
    assert!(
        got == vec![RIGHT_KEY],
        "`id = {RIGHT_KEY}` must answer exactly that row; it answered {} rows, starting {:?}",
        got.len(),
        &got[..got.len().min(5)]
    );
}

/// The fault clears before the exit: the exit writes the new root, and an open that does not
/// rebuild reads the real root.
///
/// With the exit reduced to a checkpoint (mutant M6), the page keeps the pre-split root, the next
/// open's first lookup runs cold from that left leaf, and `id = 369` answers rows 186..=369.
#[test]
fn a_clean_exit_writes_a_root_whose_persist_failed_before_it_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    built_then_restarted(dir.path());

    let mut c = open_without_rebuild(dir.path(), "C");
    let (page_one, _) = split_the_root_with_persist_failing(&mut c);
    c.catalog.first_catalog_page_id = page_one; // the fault clears
    exit_cleanly(c);

    let mut d = open_without_rebuild(dir.path(), "D");
    assert_right_key_found(&mut d);
    assert_record_is_the_root(&d);
}

/// The fault is still there at the exit: the exit refuses and names the tree's root, and it leaves
/// the log flushed and untruncated, so the next open recovers and rebuilds.
///
/// Mutants: an exit that only checkpoints (M6) returns `Ok`; one that checkpoints anyway after the
/// failure (M7), or returns without flushing the log (M8), leaves the next open nothing to recover.
#[test]
fn a_clean_exit_that_cannot_persist_the_catalog_fails_loudly_and_leaves_the_log_to_rebuild_from() {
    let dir = tempfile::tempdir().unwrap();
    built_then_restarted(dir.path());

    let mut c = open_without_rebuild(dir.path(), "C");
    let (page_one, root_now) = split_the_root_with_persist_failing(&mut c);
    let refusal = exit_through_the_cli_sequence(&c)
        .err()
        .expect("an exit that cannot persist the catalog must fail, not report a clean exit")
        .to_string();
    assert!(
        refusal.contains(&format!("t (primary index): page {root_now}")),
        "the exit's error must name the tree's root: {refusal}"
    );
    assert!(
        refusal.contains("NOT truncated"),
        "the exit's error must say the log was kept: {refusal}"
    );
    c.catalog.first_catalog_page_id = page_one;
    drop(c); // the process ends on the error: no checkpoint

    let (d, recovered) = open(dir.path());
    assert!(
        recovered,
        "the refused exit must leave a flushed, untruncated log, so this open recovers and rebuilds \
         every index from the heap"
    );
    // A second, clean restart before looking: the rebuilding open's shared cells were seeded before
    // its rebuild ran (D205, fixed on other lanes), and this test is about the log, not about that.
    exit_cleanly(d);
    let mut e = open_without_rebuild(dir.path(), "E");
    assert_right_key_found(&mut e);
    assert_record_is_the_root(&e);
}

/// Open `dir`'s database as production does, through `open_recovered`, which gives the catalog the
/// transaction manager's persist debt (F2). The lock must outlive the handles.
fn open_as_production(dir: &Path) -> (Db, bool, DbLock) {
    let path = dir.join("d230x.db");
    let lock = DbLock::acquire(&path).expect("take the database lock");
    let OpenedDatabase { bp, wal: _, txn, catalog, recovered } = open_recovered(&path, &lock).expect("open_recovered");
    (Db { catalog, bp, txn, session: Session::new() }, recovered, lock)
}

/// Whether `page` holds a B+tree internal node ON DISK. A split's new root is written to disk as a
/// zero page by `new_page` and lives in the pool after that, so this turns true when a checkpoint's
/// flush writes it.
fn on_disk_internal(bp: &Arc<BufferPoolManager>, page: u32) -> bool {
    bp.disk_manager.read(page).expect("read a page from the disk")[0] == BPLUS_INTERNAL_TYPE
}

/// **F2 (D230 review 3, the lead's decision): a failed persist, then the AUTOMATIC checkpoint, then
/// no clean exit at all.**
///
/// The exit's own persist cannot help here, because the process never reaches it (a kill, a panic,
/// or pgserver, which has no reachable exit). What must hold instead: while a persist is owed,
/// every checkpoint keeps the log, so the next open recovers and rebuilds every index from the heap
/// rather than reading the stale catalog page.
///
/// - **A/B:** T4's, plus `u (k, v)` with one row, for commits that move no root.
/// - **C:** opened as production opens, so the catalog carries the debt. Row 370 splits `t`'s root
///   while every persist fails (T4's injection), then the fault clears. Same-width UPDATEs of `u`
///   commit, and move no root, so nothing persists again, until the automatic checkpoint has
///   flushed the split. Then the process drops.
/// - **D:** opened as production opens, then a clean exit. As T5, the lookup waits for one more
///   restart: a recovering open seeds its cells before its rebuild (D205).
/// - **E:** `id = 369` cold. At `0ba2295` the automatic checkpoint truncated the log over the stale
///   page, D did not rebuild, and E answers rows 186..=369.
#[test]
fn an_automatic_checkpoint_after_a_failed_persist_keeps_the_log_so_the_next_open_rebuilds() {
    let dir = tempfile::tempdir().unwrap();
    built_then_restarted_with(
        dir.path(),
        &["CREATE TABLE u (k INTEGER NOT NULL, v INTEGER);", "INSERT INTO u VALUES (1, 0);"],
    );

    let (mut c, recovered, lock) = open_as_production(dir.path());
    assert!(!recovered, "premise failed: C's open found a log to replay, so it rebuilt and nothing is stale");
    let root_before = c.primary_root();
    let (page_one, root_now) = split_the_root_with_persist_failing(&mut c);
    c.catalog.first_catalog_page_id = page_one; // the fault clears; nothing below moves a root
    assert!(
        !on_disk_internal(&c.bp, root_now),
        "premise failed: the new root reached the disk before any checkpoint"
    );
    let mut commits = 0;
    while !on_disk_internal(&c.bp, root_now) {
        commits += 1;
        assert!(commits <= 10_000, "premise failed: {commits} commits and no automatic checkpoint flushed the split");
        c.sql("UPDATE u SET v = 1 WHERE k = 1;");
    }
    // The central premise (review 7, F-C): the flush wrote a STALE catalog page, and it was the
    // automatic checkpoint's. Both hold at the red tree and at the fix: every checkpoint flushes, and
    // the automatic trigger resets its counter whether it truncated or kept the log.
    let on_disk = Catalog::read_entries(page_one, |p| c.bp.disk_manager.read(p))
        .expect("premise failed: the catalog chain on disk could not be read");
    assert_eq!(
        on_disk.iter().find(|e| e.name == "t").map(|e| e.primary_index_root),
        Some(root_before),
        "premise failed: a persist succeeded after the failed one, so the checkpoint flushed a current \
         catalog page and nothing is stale"
    );
    assert_eq!(
        c.txn.commits_since_checkpoint.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "premise failed: the flush was not the automatic checkpoint's"
    );
    drop(c);
    drop(lock); // no exit at all

    let (d, _, lock) = open_as_production(dir.path());
    exit_cleanly(d);
    drop(lock);
    let mut e = open_without_rebuild(dir.path(), "E");
    assert_right_key_found(&mut e);
    assert_record_is_the_root(&e);
}
