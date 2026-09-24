//! **D229 — crash-safe frees: the falsifier and the design review's two schedules.**
//!
//! SCALE-DESIGN "D229" decides the fix; `frontier/d229_design.md` §7 is the falsifier, and
//! `frontier/d229_design_review.md` §3.2 and §3.3 are the two schedules that broke the design as it
//! was first written. Every test here runs a database on a [`SimFabric`], puts a crash at a chosen
//! operation, and reopens it through [`open_recovered`], the one open path. The open takes the
//! fabric's page file and log through [`TEST_FILES`] instead of opening real files; that is the only
//! difference from production.
//!
//! **The oracle is written here, from the page formats, and never from the code under test:**
//! - every row the model holds is found by a scan, and by key through every index, descending the
//!   shared root cells a statement would descend;
//! - no page is reached from two structures;
//! - no reached page has its allocation bit clear;
//! - a table the model has dropped leaves none of its pages allocated unless a live structure
//!   reaches it.
//!
//! **Blind spots, stated:**
//! - The files beside the log (the stale-indexes marker, the release quarantine, D229's intent) are
//!   written through the real filesystem, and no sweep aims a fault at one of their operations, so a
//!   crash finds each durable the moment it returned. D229 review 1's tests reach them another way:
//!   a file-ops double ([`Ops`]) records every step of the intent file and the marker's fsyncs, or
//!   fails one step once, in a live process. That is how their ORDER is checked (A4; the rebuild's
//!   trigger before its first free); a power loss between two of those steps is still not modelled.
//! - `Durability::WriteThrough` (kill -9, where every write that returned is kept), except one test
//!   that needs a bitmap write lost under a durable log record and boots `SyncOnly`. A power loss that
//!   keeps an arbitrary subset of unsynced writes is not modelled here.
//! - Every fault is a lost write, a failed sync or a skipped `set_len`, and then the crash. Torn and
//!   garbled page writes are outside the engine's stated model (`tests/sim_durability.rs`,
//!   `fabric`).
//! - A run is aimed at an operation INDEX taken from a census run, so it is only meaningful while
//!   the run is deterministic. Every run asserts that its fault fired at exactly the planned index.
//! - `FERRODB_D229_STRIDE=k` samples every k-th point of each sweep. Unset, every point runs.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use super::{open_recovered, OpenedDatabase};
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::IndexTree;
use crate::catalog::catalog_page::CatalogPage;
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::execution::executor::{run, Outcome};
use crate::execution::session::Session;
use crate::parser::{parser::Parser, scanner::Scanner};
use crate::storage::atomic_file::{FileOps, OsFileOps};
use crate::storage::db_lock::DbLock;
use crate::storage::disk_manager::PAGE_SIZE;
use crate::storage::heap_file_manager::{HeapFileManager, RecordId};
use crate::storage::index::BPlusTreeManager;
use crate::storage::index_page::BPlusTreePage;
use crate::storage::heap_page::{Page, MAX_TUPLE_SIZE};
use crate::storage::page_directory::{PageDirectory, MAX_ENTRIES};
use crate::storage::tuple::Tuple;
use crate::storage::sim::{Durability, FaultPlan, OpKind, SimFabric, WriteShape};
use crate::storage::storage::Storage;
use crate::wal::free_intent::{self, FreeIntent};
use crate::wal::log::{DdlOp, RecKind, WalManager};
use crate::wal::txn::DdlRecord;

thread_local! {
    /// **Test-only: the storage the next [`open_recovered`] on this thread opens instead of its
    /// files**, as `(page file, log, whether the page file existed)`. The open takes it, once.
    /// Thread-local, so a test that hands storage over cannot redirect an open in a test beside it.
    pub(crate) static TEST_FILES: RefCell<Option<(Arc<dyn Storage>, Arc<dyn Storage>, bool)>> = const { RefCell::new(None) };

    /// **Test-only: the filesystem the next [`open_recovered`] on this thread hands its transaction
    /// manager** for D229's intent file and the stale-indexes marker's fsyncs, instead of the real
    /// one (D229 review 1's R7). Taken once, like [`TEST_FILES`].
    pub(crate) static TEST_FILE_OPS: RefCell<Option<Arc<dyn FileOps + Send + Sync>>> = const { RefCell::new(None) };
}

// ---------------------------------------------------------------------------------------------
// The machine: a fabric, and a directory for the files beside the log
// ---------------------------------------------------------------------------------------------

/// The fabric's names for the page file and the log.
const FAB_DB: &str = "sim.db";
const FAB_WAL: &str = "sim.wal";
/// The path `open_recovered` is given, inside the machine's directory. Its log's side files
/// (`<db>.wal.*`) are real files there.
const DB_NAME: &str = "d229.db";

/// What survives a crash: the fabric's durable bytes, and every file beside the log.
#[derive(Clone, Default)]
struct Snapshot {
    images: BTreeMap<String, Vec<u8>>,
    side: BTreeMap<String, Vec<u8>>,
}

/// One boot of a machine.
struct Machine {
    fabric: Arc<SimFabric>,
    dir: tempfile::TempDir,
    existed: bool,
}

/// An open database, and the single-writer lock the open was given. Dropping it is the crash:
/// nothing is flushed, and the fabric keeps only what was written.
struct Opened {
    o: OpenedDatabase,
    _lock: DbLock,
}

impl Machine {
    fn boot(s: &Snapshot, plan: Option<FaultPlan>) -> Machine {
        Machine::boot_as(s, plan, Durability::WriteThrough)
    }

    /// [`Machine::boot`] under `durability`. `SyncOnly` loses, at the crash, every write no sync
    /// covered: that is how a bitmap bit is lost while the log record that relinks its page is durable.
    fn boot_as(s: &Snapshot, plan: Option<FaultPlan>, durability: Durability) -> Machine {
        let dir = tempfile::tempdir().expect("a directory for the machine");
        for (name, bytes) in &s.side {
            std::fs::write(dir.path().join(name), bytes).expect("restore a file beside the log");
        }
        let existed = s.images.get(FAB_DB).is_some_and(|b| !b.is_empty());
        let fabric = SimFabric::from_images(s.images.clone(), plan, durability);
        // The engine's own model: a page write lands whole or not at all (`tests/sim_durability.rs`).
        fabric.set_write_atomicity(FAB_DB, PAGE_SIZE as u64);
        Machine { fabric, dir, existed }
    }

    fn open(&self) -> Result<Opened, FerroError> {
        self.open_on(self.fabric.open(FAB_DB))
    }

    /// Open with `db_file` as the page file: the fabric's, or a wrapper around it that misbehaves
    /// on purpose.
    fn open_on(&self, db_file: Arc<dyn Storage>) -> Result<Opened, FerroError> {
        self.open_with(db_file, self.fabric.open(FAB_WAL), None)
    }

    /// Open with `db_file` and `wal_file` as the page file and the log, and `ops` as the filesystem the
    /// transaction manager writes the intent file and fsyncs the marker through (the real one when
    /// `None`).
    fn open_with(
        &self,
        db_file: Arc<dyn Storage>,
        wal_file: Arc<dyn Storage>,
        ops: Option<Arc<dyn FileOps + Send + Sync>>,
    ) -> Result<Opened, FerroError> {
        let db = self.dir.path().join(DB_NAME);
        let lock = DbLock::acquire(&db)?;
        TEST_FILES.with(|f| *f.borrow_mut() = Some((db_file, wal_file, self.existed)));
        TEST_FILE_OPS.with(|f| *f.borrow_mut() = ops);
        let opened = open_recovered(&db, &lock);
        // Taken by the open unless it refused before reaching them.
        TEST_FILES.with(|f| f.borrow_mut().take());
        TEST_FILE_OPS.with(|f| f.borrow_mut().take());
        Ok(Opened { o: opened?, _lock: lock })
    }

    /// Open, turning a panic into an error: a crash can land anywhere, and an open that panics has
    /// failed as surely as one that returns `Err`.
    fn open_caught(&self) -> Result<Opened, String> {
        match catch_unwind(AssertUnwindSafe(|| self.open())) {
            Ok(Ok(d)) => Ok(d),
            Ok(Err(e)) => Err(format!("the open failed: {e}")),
            Err(p) => Err(format!("the open PANICKED: {}", panic_text(&p))),
        }
    }

    /// The crash image. Every `Opened` on this machine must already be dropped.
    fn snapshot(&self) -> Snapshot {
        let mut side = BTreeMap::new();
        for e in std::fs::read_dir(self.dir.path()).expect("list the machine's directory") {
            let e = e.expect("a directory entry");
            let name = e.file_name().to_string_lossy().to_string();
            if name.ends_with(".lock") {
                continue;
            }
            side.insert(name, std::fs::read(e.path()).expect("read a file beside the log"));
        }
        Snapshot { images: self.fabric.durable_image(), side }
    }
}

fn panic_text(p: &Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<String>()
        .cloned()
        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "a non-string panic".to_string())
}

// ---------------------------------------------------------------------------------------------
// The workload
// ---------------------------------------------------------------------------------------------

/// Rows in each indexed table of the fixture. 400 puts every tree two levels deep: about 370
/// integer keys fit a primary leaf, about 20 190-byte keys a secondary leaf, about 250 postings a
/// posting leaf (INFERRED from the page formats; asserted per tree by the fixture's premise).
const ROWS: i32 = 400;
/// Rows a stage adds after the fixture's checkpoint, so they are in the log when a phase runs.
const EXTRA: i32 = 30;
/// Rows the table built after a reopen gets, to show that later allocations alias nothing.
const FRESH: i32 = 60;
/// `t`'s padding columns: enough that dropping `t` lets the catalog's entries fit one page again.
const PADS: usize = 40;
/// Tables with nothing but columns, and their width: together with `t` they make the catalog span
/// two pages.
const WIDE: usize = 3;
const WIDE_COLS: usize = 45;

/// A row's indexed values, from its id alone. `v` is fixed-width so no leaf mixes key widths
/// (D225). `w` is one lowercase word, so its only full-text token is itself.
fn v_of(id: i32) -> String {
    format!("v{id:05}{}", "x".repeat(184))
}

fn w_of(id: i32) -> String {
    format!("w{id:05}")
}

fn pad_cols(n: usize) -> String {
    (0..n).map(|i| format!(", pad_column_number_{i:02} INTEGER")).collect()
}

fn create_indexed(table: &str, pads: usize) -> String {
    format!("CREATE TABLE {table} (id INTEGER NOT NULL, v VARCHAR(200), w VARCHAR(40){});", pad_cols(pads))
}

fn insert_sql(table: &str, id: i32) -> String {
    let pads = if table == "t" { PADS } else { 0 };
    format!("INSERT INTO {table} VALUES ({id}, '{}', '{}'{});", v_of(id), w_of(id), ", 0".repeat(pads))
}

fn sql(o: &mut OpenedDatabase, s: &mut Session, text: &str) -> Result<Outcome, FerroError> {
    let tokens = Scanner::new(text.chars().collect(), Vec::new()).scan_tokens()?;
    let mut p = Parser::new(tokens);
    let mut stmts = p.parse();
    assert!(p.errors.is_empty(), "parse errors in `{text}`: {:?}", p.errors);
    run(stmts.remove(0), &mut o.catalog, o.bp.clone(), o.txn.clone(), s)
}

fn must(o: &mut OpenedDatabase, s: &mut Session, text: &str) {
    if let Err(e) = sql(o, s, text) {
        panic!("`{}` failed: {e}", &text[..text.len().min(120)]);
    }
}

/// `ids` into `table`, in one transaction.
fn insert_rows(o: &mut OpenedDatabase, s: &mut Session, table: &str, ids: Range<i32>) -> Result<(), FerroError> {
    sql(o, s, "BEGIN;")?;
    for id in ids {
        sql(o, s, &insert_sql(table, id))?;
    }
    sql(o, s, "COMMIT;")?;
    Ok(())
}

/// The table every check builds after a reopen: its pages come from whatever the allocator hands
/// out then, so it aliases a page that some structure still names, if any is on offer.
fn fresh_table(o: &mut OpenedDatabase) -> Result<(), String> {
    let mut s = Session::new();
    for text in [create_indexed("z", 0), "CREATE INDEX iv_z ON z (v);".into(), "CREATE FULLTEXT INDEX fw_z ON z (w);".into()] {
        sql(o, &mut s, &text).map_err(|e| format!("`{text}` failed: {e}"))?;
    }
    insert_rows(o, &mut s, "z", 0..FRESH).map_err(|e| format!("inserting into z failed: {e}"))
}

/// **The fixture**, built once and shared: a table dropped early (`a`, its pages are holes), three
/// wide tables, and two indexed tables `t` and `u` of [`ROWS`] rows, each with a B-tree index on `v`
/// and a full-text index on `w`. Then a checkpoint, and the image is taken with nothing in flight.
fn fixture() -> &'static Snapshot {
    static FIXTURE: OnceLock<Snapshot> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let m = Machine::boot(&Snapshot::default(), None);
        {
            let mut d = m.open().expect("fixture: open a new database");
            let o = &mut d.o;
            let s = &mut Session::new();
            must(o, s, "CREATE TABLE a (id INTEGER NOT NULL, v INTEGER);");
            for id in 0..3 {
                must(o, s, &format!("INSERT INTO a VALUES ({id}, {id});"));
            }
            for k in 0..WIDE {
                must(o, s, &format!("CREATE TABLE wide_{k} (id INTEGER NOT NULL{});", pad_cols(WIDE_COLS)));
            }
            must(o, s, &create_indexed("t", PADS));
            must(o, s, &create_indexed("u", 0));
            for table in ["t", "u"] {
                must(o, s, &format!("CREATE INDEX iv_{table} ON {table} (v);"));
                must(o, s, &format!("CREATE FULLTEXT INDEX fw_{table} ON {table} (w);"));
            }
            for table in ["t", "u"] {
                insert_rows(o, s, table, 0..ROWS).unwrap_or_else(|e| panic!("fixture: inserting into {table} failed: {e}"));
            }
            must(o, s, "DROP TABLE a;");
            o.txn.checkpoint().expect("fixture: the checkpoint");

            let r = walk(o).unwrap_or_else(|e| panic!("fixture: {e}"));
            assert!(
                r.catalog_entries.iter().filter(|n| **n > 0).count() >= 2,
                "premise: the fixture's catalog entries fit one page ({:?} per page), so no crash can tear a \
                 multi-page catalog and no DROP can shrink it",
                r.catalog_entries
            );
            for table in ["t", "u"] {
                for tree in ["primary index", "index on v", "full-text index on w"] {
                    let label = format!("{table}'s {tree}");
                    assert!(
                        r.internal.get(&label).copied().unwrap_or(0) >= 1,
                        "premise: {label} is one level deep, so no crash can leave a node its parent does not name"
                    );
                }
            }
        }
        m.snapshot()
    })
}

/// The model after the fixture: `t` and `u` whole, the wide tables empty, `a` gone.
fn fixture_want() -> Want {
    let mut want = Want::default();
    for table in ["t", "u"] {
        want.present.insert(table.to_string(), (0..ROWS).collect());
    }
    for k in 0..WIDE {
        want.present.insert(format!("wide_{k}"), Vec::new());
    }
    want.absent.push("a".to_string());
    want
}

// ---------------------------------------------------------------------------------------------
// The oracle
// ---------------------------------------------------------------------------------------------

/// Bits per bitmap page: the page less its 4-byte next pointer (`storage/disk_manager.rs`).
const BITS: u32 = (PAGE_SIZE as u32 - 4) * 8;

/// A page's bytes as a statement would see them: through the pool.
fn page(bp: &BufferPoolManager, id: u32) -> Result<[u8; PAGE_SIZE], String> {
    let i = bp.fetch_page(id).map_err(|e| format!("page {id} does not read: {e}"))?;
    let bytes = bp.frames[i].read().unwrap().data;
    bp.unpin_page(id, false);
    Ok(bytes)
}

/// The bitmap chain's pages, from page 0. The allocator reads the bitmap from the file, not the
/// pool, so this does too.
fn bitmap_chain(bp: &BufferPoolManager) -> Result<Vec<(u32, [u8; PAGE_SIZE])>, String> {
    let mut out = Vec::new();
    let mut id = 0u32;
    loop {
        if out.iter().any(|(p, _)| *p == id) {
            return Err(format!("the bitmap chain cycles at page {id}"));
        }
        let b = bp.disk_manager.read(id).map_err(|e| format!("bitmap page {id} does not read: {e}"))?;
        let next = u32::from_le_bytes(b[0..4].try_into().unwrap());
        out.push((id, b));
        if next == 0 {
            return Ok(out);
        }
        id = next;
    }
}

/// Every page whose allocation bit is set.
fn allocated(bp: &BufferPoolManager) -> Result<BTreeSet<u32>, String> {
    let mut out = BTreeSet::new();
    for (k, (_, b)) in bitmap_chain(bp)?.into_iter().enumerate() {
        for local in 0..BITS {
            if b[(local / 8) as usize + 4] & (1 << (local % 8)) != 0 {
                out.insert(k as u32 * BITS + local);
            }
        }
    }
    Ok(out)
}

/// What the durable structures name.
#[derive(Default)]
struct Reach {
    /// Every page a structure names, with the first structure that named it.
    owner: BTreeMap<u32, String>,
    /// Pages named by two structures: the page, the first, the second.
    twice: Vec<(u32, String, String)>,
    /// Entries on each catalog page, in chain order.
    catalog_entries: Vec<usize>,
    /// Internal nodes per tree, by the tree's label.
    internal: BTreeMap<String, usize>,
}

impl Reach {
    fn claim(&mut self, page: u32, by: &str) {
        match self.owner.get(&page) {
            Some(first) => self.twice.push((page, first.clone(), by.to_string())),
            None => {
                self.owner.insert(page, by.to_string());
            }
        }
    }
}

/// A heap's directory chain and every page it lists. An all-zero directory page is the image a
/// crash leaves of a page allocated and not yet written, and it names nothing.
fn walk_heap(bp: &BufferPoolManager, first: u32, by: &str, r: &mut Reach) -> Result<(), String> {
    let mut id = first;
    let mut seen = BTreeSet::new();
    while id != 0 {
        if !seen.insert(id) {
            return Err(format!("{by}: the directory chain cycles at page {id}"));
        }
        r.claim(id, &format!("{by} directory"));
        let b = page(bp, id)?;
        if b.iter().all(|x| *x == 0) {
            return Ok(());
        }
        if b[0] != 1 {
            return Err(format!("{by}: directory page {id} holds a page of type {}", b[0]));
        }
        let dir = PageDirectory::deserialize(b);
        for e in &dir.entries {
            r.claim(e.page_id, by);
        }
        id = dir.next_page_directory;
    }
    Ok(())
}

/// A tree's nodes, through child pointers and the leaf chain. `children` decodes one node.
fn walk_tree(
    bp: &BufferPoolManager,
    root: u32,
    by: &str,
    children: fn([u8; PAGE_SIZE]) -> Result<(bool, Vec<u32>), FerroError>,
    r: &mut Reach,
) -> Result<(), String> {
    let mut mine = BTreeSet::new();
    let mut stack = vec![root];
    while let Some(id) = stack.pop() {
        if !mine.insert(id) {
            continue;
        }
        r.claim(id, by);
        let (internal, next) = children(page(bp, id)?).map_err(|e| format!("{by}: page {id} is not a tree node: {e}"))?;
        if internal {
            *r.internal.entry(by.to_string()).or_insert(0) += 1;
        }
        stack.extend(next);
    }
    Ok(())
}

fn primary_children(b: [u8; PAGE_SIZE]) -> Result<(bool, Vec<u32>), FerroError> {
    Ok(match BPlusTreePage::<Value, RecordId>::deserialize(b)? {
        BPlusTreePage::Internal(n) => (true, n.child_ptrs),
        BPlusTreePage::Leaf(l) => (false, l.next.into_iter().collect()),
    })
}

fn pair_children(b: [u8; PAGE_SIZE]) -> Result<(bool, Vec<u32>), FerroError> {
    Ok(match BPlusTreePage::<(Value, Value), ()>::deserialize(b)? {
        BPlusTreePage::Internal(n) => (true, n.child_ptrs),
        BPlusTreePage::Leaf(l) => (false, l.next.into_iter().collect()),
    })
}

/// Every page the bitmap chain, the catalog chain and each table's heaps and trees name. Trees are
/// walked from their shared cells, the roots a statement descends.
fn walk(o: &OpenedDatabase) -> Result<Reach, String> {
    let bp = &o.bp;
    let mut r = Reach::default();
    for (id, _) in bitmap_chain(bp)? {
        r.claim(id, "the bitmap chain");
    }
    let mut id = 1u32;
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(id) {
            return Err(format!("the catalog chain cycles at page {id}"));
        }
        r.claim(id, "the catalog chain");
        let p = CatalogPage::deserialize(page(bp, id)?).map_err(|e| format!("catalog page {id} does not parse: {e}"))?;
        r.catalog_entries.push(p.entries.len());
        if p.next_catalog_page == 0 {
            break;
        }
        id = p.next_catalog_page;
    }
    let mut names: Vec<&String> = o.catalog.tables.keys().collect();
    names.sort();
    for name in names {
        let e = &o.catalog.tables[name];
        let live = |index: Option<IndexTree<&str>>, recorded: u32| {
            o.catalog.root_cell(name, index).map_or(recorded, |c| c.load(Ordering::SeqCst))
        };
        walk_heap(bp, e.first_directory_page_id, &format!("{name}'s heap"), &mut r)?;
        walk_heap(bp, e.time_travel_root, &format!("{name}'s time-travel heap"), &mut r)?;
        walk_tree(bp, live(None, e.primary_index_root), &format!("{name}'s primary index"), primary_children, &mut r)?;
        for i in &e.indexes {
            let root = live(Some(IndexTree::Secondary(&i.column_name)), i.root_page_id);
            walk_tree(bp, root, &format!("{name}'s index on {}", i.column_name), pair_children, &mut r)?;
        }
        for i in &e.fulltext_indexes {
            let root = live(Some(IndexTree::FullText(&i.column_name)), i.root_page_id);
            walk_tree(bp, root, &format!("{name}'s full-text index on {}", i.column_name), pair_children, &mut r)?;
        }
    }
    Ok(r)
}

/// The pages `table`'s structures name.
fn pages_of(o: &OpenedDatabase, table: &str) -> Result<BTreeSet<u32>, String> {
    let prefix = format!("{table}'s ");
    Ok(walk(o)?.owner.into_iter().filter(|(_, by)| by.starts_with(&prefix)).map(|(p, _)| p).collect())
}

/// No page named twice, and no named page free.
fn structure(o: &OpenedDatabase) -> Result<Reach, String> {
    let r = walk(o)?;
    if !r.twice.is_empty() {
        return Err(format!(
            "{} page(s) reached from two structures, e.g. {:?}",
            r.twice.len(),
            &r.twice[..r.twice.len().min(4)]
        ));
    }
    let bits = allocated(&o.bp)?;
    let free: Vec<(&u32, &String)> = r.owner.iter().filter(|(p, _)| !bits.contains(p)).collect();
    if !free.is_empty() {
        return Err(format!(
            "{} page(s) reached while their allocation bit is clear, e.g. {:?}",
            free.len(),
            &free[..free.len().min(4)]
        ));
    }
    Ok(r)
}

/// Allocated pages no structure names.
fn leaked(o: &OpenedDatabase) -> Result<Vec<u32>, String> {
    let r = walk(o)?;
    Ok(allocated(&o.bp)?.into_iter().filter(|p| !r.owner.contains_key(p)).collect())
}

/// What the database must hold.
#[derive(Clone, Default)]
struct Want {
    /// Tables that must exist, each holding exactly the rows of these ids.
    present: BTreeMap<String, Vec<i32>>,
    /// Tables that must not exist.
    absent: Vec<String>,
    /// A table that is either wholly present with these rows, or wholly absent with none of these
    /// pages (its pages before the DROP) allocated unless a live structure reaches it.
    either: Option<(String, Vec<i32>, BTreeSet<u32>)>,
}

impl Want {
    fn with_fresh(&self) -> Want {
        let mut w = self.clone();
        w.present.insert("z".to_string(), (0..FRESH).collect());
        w
    }
}

/// `table` holds exactly the rows `ids`, by scan and by key through every index.
fn check_rows(o: &mut OpenedDatabase, table: &str, ids: &[i32]) -> Result<(), String> {
    let mut s = Session::new();
    let entry = o.catalog.get_table(table).ok_or_else(|| format!("table {table} is gone"))?.clone();
    if entry.indexes.is_empty() && entry.fulltext_indexes.is_empty() {
        // No `v`/`w` model: the ids, by a scan and by key through the primary index.
        let got: BTreeSet<i32> = match sql(o, &mut s, &format!("SELECT id FROM {table};")) {
            Ok(Outcome::Rows(rows)) => rows.iter().map(|r| if let Value::Integer(id) = r[0] { id } else { i32::MIN }).collect(),
            Ok(_) => return Err(format!("SELECT from {table} returned no rows")),
            Err(e) => return Err(format!("SELECT from {table} failed: {e}")),
        };
        let want: BTreeSet<i32> = ids.iter().copied().collect();
        if got != want {
            return Err(format!("a scan of {table} finds {} ids against {}", got.len(), want.len()));
        }
        let cell = o.catalog.root_cell(table, None).ok_or_else(|| format!("{table} has no primary cell"))?;
        let primary = BPlusTreeManager::<Value, RecordId>::open_shared(cell, o.bp.clone());
        let heap = HeapFileManager::open(entry.first_directory_page_id, o.bp.clone());
        for id in ids {
            let rid = primary
                .search(&Value::Integer(*id))
                .map_err(|e| format!("{table}: the primary index fails on row {id}: {e}"))?
                .ok_or_else(|| format!("{table}: row {id} is missing by key"))?;
            let vals = heap
                .read(rid)
                .and_then(|t| t.deserialize(&entry.schema))
                .map_err(|e| format!("{table}: row {id}'s key names {rid:?}, which does not read: {e}"))?;
            if vals.first() != Some(&Value::Integer(*id)) {
                return Err(format!("{table}: row {id} by key is row {:?}", vals.first()));
            }
        }
        return Ok(());
    }
    let want: BTreeSet<(i32, String, String)> = ids.iter().map(|id| (*id, v_of(*id), w_of(*id))).collect();
    let got: BTreeSet<(i32, String, String)> = match sql(o, &mut s, &format!("SELECT id, v, w FROM {table};")) {
        Ok(Outcome::Rows(rows)) => rows
            .into_iter()
            .map(|r| match (&r[0], &r[1], &r[2]) {
                (Value::Integer(id), Value::Varchar(v), Value::Varchar(w)) => (*id, v.clone(), w.clone()),
                _ => (i32::MIN, format!("{r:?}"), String::new()),
            })
            .collect(),
        Ok(_) => return Err(format!("a scan of {table} returned no rows")),
        Err(e) => return Err(format!("a scan of {table} failed: {e}")),
    };
    if got != want {
        let missing: Vec<i32> = want.difference(&got).map(|r| r.0).take(5).collect();
        let extra: Vec<i32> = got.difference(&want).map(|r| r.0).take(5).collect();
        return Err(format!(
            "a scan of {table} finds {} rows against {}: missing e.g. {missing:?}, extra e.g. {extra:?}",
            got.len(),
            want.len()
        ));
    }
    let cell = |index: Option<IndexTree<&str>>| {
        o.catalog.root_cell(table, index).ok_or_else(|| format!("{table} has no shared cell for {index:?}"))
    };
    let primary = BPlusTreeManager::<Value, RecordId>::open_shared(cell(None)?, o.bp.clone());
    let heap = HeapFileManager::open(entry.first_directory_page_id, o.bp.clone());
    for id in ids {
        let rid = primary
            .search(&Value::Integer(*id))
            .map_err(|e| format!("{table}: the primary index fails on row {id}: {e}"))?
            .ok_or_else(|| format!("{table}: row {id} is missing by key"))?;
        let tuple = heap.read(rid).map_err(|e| format!("{table}: row {id}'s key names {rid:?}, which does not read: {e}"))?;
        let ended = tuple.version_header().map_err(|e| format!("{table}: row {id}: {e}"))?.end_ts;
        let vals = tuple.deserialize(&entry.schema).map_err(|e| format!("{table}: row {id}: {e}"))?;
        if ended != 0 || vals.get(0..3) != Some(&[Value::Integer(*id), Value::Varchar(v_of(*id)), Value::Varchar(w_of(*id))][..]) {
            return Err(format!("{table}: row {id} by key is not the row (ended at {ended}): {:?}", &vals[..vals.len().min(3)]));
        }
    }
    for i in &entry.indexes {
        let tree = BPlusTreeManager::<(Value, Value), ()>::open_shared(cell(Some(IndexTree::Secondary(&i.column_name)))?, o.bp.clone());
        for id in ids {
            let key = match i.column_name.as_str() {
                "v" => Value::Varchar(v_of(*id)),
                other => return Err(format!("{table} has an index on {other}, which this model does not know")),
            };
            if tree.search(&(key, Value::Integer(*id))).map_err(|e| format!("{table}'s index on v fails on row {id}: {e}"))?.is_none() {
                return Err(format!("{table}: row {id} is missing through its index on v"));
            }
        }
    }
    for i in &entry.fulltext_indexes {
        let tree = BPlusTreeManager::<(Value, Value), ()>::open_shared(cell(Some(IndexTree::FullText(&i.column_name)))?, o.bp.clone());
        for id in ids {
            let token = match i.column_name.as_str() {
                "w" => Value::Varchar(w_of(*id)),
                other => return Err(format!("{table} has a full-text index on {other}, which this model does not know")),
            };
            if tree.search(&(token, Value::Integer(*id))).map_err(|e| format!("{table}'s full-text index fails on row {id}: {e}"))?.is_none() {
                return Err(format!("{table}: row {id} is missing through its full-text index on w"));
            }
        }
    }
    Ok(())
}

/// The whole oracle.
fn oracle(o: &mut OpenedDatabase, want: &Want) -> Result<(), String> {
    for (name, ids) in &want.present {
        check_rows(o, name, ids)?;
    }
    for name in &want.absent {
        if o.catalog.get_table(name).is_some() {
            return Err(format!("table {name} came back"));
        }
    }
    let known: BTreeSet<&String> = want.present.keys().chain(want.either.iter().map(|(n, _, _)| n)).collect();
    let strays: Vec<&String> = o.catalog.tables.keys().filter(|n| !known.contains(n)).collect();
    if !strays.is_empty() {
        return Err(format!("the catalog holds tables the model does not: {strays:?}"));
    }
    let r = structure(o)?;
    if let Some((name, ids, pages)) = &want.either {
        if o.catalog.get_table(name).is_some() {
            check_rows(o, name, ids).map_err(|e| format!("{name} is present but not whole: {e}"))?;
        } else {
            let bits = allocated(&o.bp)?;
            let kept: Vec<u32> = pages.iter().copied().filter(|p| bits.contains(p) && !r.owner.contains_key(p)).collect();
            if !kept.is_empty() {
                return Err(format!(
                    "{name} is gone, and {} of its pages are still allocated with nothing naming them, e.g. {:?}",
                    kept.len(),
                    &kept[..kept.len().min(5)]
                ));
            }
        }
    }
    Ok(())
}

/// The falsifier's two opens after a crash: open, check, build a fresh table, check again, then
/// crash that process and open once more, and check.
fn two_good_opens(s: &Snapshot, want: &Want) -> Result<(), String> {
    let m = Machine::boot(s, None);
    let after_fresh = want.with_fresh();
    {
        let mut d = m.open_caught().map_err(|e| format!("the first open after the crash: {e}"))?;
        oracle(&mut d.o, want).map_err(|e| format!("after the first open: {e}"))?;
        fresh_table(&mut d.o).map_err(|e| format!("after the first open: {e}"))?;
        oracle(&mut d.o, &after_fresh).map_err(|e| format!("after the first open and a fresh table: {e}"))?;
    }
    let m2 = Machine::boot(&m.snapshot(), None);
    let mut d = m2.open_caught().map_err(|e| format!("the second open after the crash: {e}"))?;
    oracle(&mut d.o, &after_fresh).map_err(|e| format!("after the second open: {e}"))
}

// ---------------------------------------------------------------------------------------------
// The sweep
// ---------------------------------------------------------------------------------------------

/// Every faultable operation in `[from, to)` of a census fabric, thinned by `FERRODB_D229_STRIDE`.
fn sweep_points(f: &SimFabric, from: u64, to: u64) -> Vec<u64> {
    let stride = std::env::var("FERRODB_D229_STRIDE").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(1).max(1);
    f.faultable_ops().into_iter().filter(|op| *op >= from && *op < to).step_by(stride).collect()
}

/// A lost write, a failed sync or a skipped `set_len` at `op`, then the crash.
fn plan(op: u64) -> FaultPlan {
    FaultPlan::at_shaped(op, 0xD229, WriteShape::Drop)
}

fn fired_at(m: &Machine, op: u64) -> Result<(), String> {
    match m.fabric.fired() {
        Some(f) if f.op_index == op => Ok(()),
        Some(f) => Err(format!(
            "the fault planned at operation {op} fired at {} ({:?} on {}), so this run is not the census run",
            f.op_index, f.kind, f.file
        )),
        None => Err(format!("the fault planned at operation {op} never fired")),
    }
}

fn report(what: &str, points: usize, failures: &[String]) {
    assert!(
        failures.is_empty(),
        "{what}: {} of {points} crash point(s) broke the falsifier (SCALE-DESIGN D229). First ones:\n  {}",
        failures.len(),
        failures.iter().take(8).cloned().collect::<Vec<_>>().join("\n  ")
    );
}

/// Open the fixture and add [`EXTRA`] rows to `table`, so its records are in the log.
fn stage_rows(m: &Machine, table: &str) -> Opened {
    let mut d = m.open().expect("stage: open the fixture");
    insert_rows(&mut d.o, &mut Session::new(), table, ROWS..ROWS + EXTRA).expect("stage: insert the extra rows");
    d
}

/// The fixture after `u` gained [`EXTRA`] rows and the process died before any checkpoint: an image
/// whose next open owes a rebuild.
fn owed_rebuild() -> Snapshot {
    let m = Machine::boot(fixture(), None);
    drop(stage_rows(&m, "u"));
    m.snapshot()
}

fn owed_rebuild_want() -> Want {
    let mut want = fixture_want();
    want.present.insert("u".to_string(), (0..ROWS + EXTRA).collect());
    want
}

// ---------------------------------------------------------------------------------------------
// The review's two schedules
// ---------------------------------------------------------------------------------------------

/// **Review §3.2: a DROP crashed after its checkpoint's sync and before its truncate, then an open
/// crashed inside its rebuild. The open after that must succeed.**
///
/// The first crash leaves the catalog without `t` on disk and `t`'s records still in the log.
/// If `t`'s pages are free by then (at `a6e93ab` the DROP frees them before its checkpoint), open
/// #2's rebuild puts B+tree nodes on them, and a crash before open #2 truncates leaves the log
/// naming them as heap pages. Open #3's redo then parses a B+tree node as a heap page (review:
/// a panic in `Page::deserialize`), at every open from then on. So every operation of open #2 is a
/// crash point here.
///
/// RED at `a6e93ab` (INFERRED): some open #2 crash point makes open #3 fail or panic.
#[test]
fn a_drop_crashed_before_its_truncate_then_an_open_crashed_in_its_rebuild_still_opens() {
    // Census: where the DROP's truncate writes the log header.
    let census = Machine::boot(fixture(), None);
    let truncate = {
        let mut d = stage_rows(&census, "t");
        let from = census.fabric.op_count();
        must(&mut d.o, &mut Session::new(), "DROP TABLE t;");
        let to = census.fabric.op_count();
        let trace = census.fabric.trace();
        let synced = trace
            .iter()
            .find(|op| op.index >= from && op.index < to && op.file == FAB_DB && matches!(op.kind, OpKind::SyncAll | OpKind::SyncData))
            .map(|op| op.index)
            .expect("premise: the DROP never synced the page file");
        trace
            .iter()
            .find(|op| op.index > synced && op.index < to && op.file == FAB_WAL && op.kind == OpKind::Pwrite && op.offset == 0)
            .map(|op| op.index)
            .expect("premise: the DROP's checkpoint never rewrote the log's header after its sync, so it never truncated")
    };

    // Crash #1, at the truncate.
    let first = Machine::boot(fixture(), Some(plan(truncate)));
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let mut d = stage_rows(&first, "t");
        let _ = sql(&mut d.o, &mut Session::new(), "DROP TABLE t;");
    }));
    fired_at(&first, truncate).expect("premise: crash #1");
    let after_first = first.snapshot();

    // Census of open #2.
    let census2 = Machine::boot(&after_first, None);
    drop(census2.open().expect("premise: open #2 fails on a clean machine, so the sweep below measures nothing"));
    let points = sweep_points(&census2.fabric, 0, census2.fabric.op_count());
    assert!(points.len() >= 20, "premise: open #2 has only {} faultable operations", points.len());

    let mut want = fixture_want();
    want.absent.push("t".to_string());
    want.present.remove("t");
    let mut failures = Vec::new();
    for op in &points {
        let second = Machine::boot(&after_first, Some(plan(*op)));
        let _ = catch_unwind(AssertUnwindSafe(|| second.open()));
        if let Err(e) = fired_at(&second, *op) {
            failures.push(e);
            continue;
        }
        if let Err(e) = two_good_opens(&second.snapshot(), &want) {
            failures.push(format!("crash at operation {op} of open #2: {e}"));
        }
    }
    report("a DROP crashed at its truncate, then open #2 crashed", points.len(), &failures);
}

/// **Review §3.3: a DROP whose frees fail part-way, more writes, then a crash.**
///
/// The frees fail here because one of `t`'s pages is PINNED, as a leaked pin leaves it (D237).
/// At `a6e93ab` `drop_table` frees `t`'s heaps and its primary tree's children on disk, is refused
/// at the pinned root, and returns `Err` with `t` still in the catalog over pages that are free.
/// `u`'s next rows take them. So `t`'s directory and `u`'s name the same pages, and the rebuild at
/// the next open walks `t`'s tree into `u`'s heap pages.
///
/// The model accepts either outcome of the DROP: `t` wholly present with every row, or wholly gone
/// with no page of it left allocated and unnamed.
///
/// RED at `a6e93ab` (INFERRED): the next open fails, or a page is reached twice.
#[test]
fn a_drop_whose_frees_are_refused_leaves_no_page_reached_twice() {
    let m = Machine::boot(fixture(), None);
    let pages;
    {
        let mut d = m.open().expect("open the fixture");
        pages = pages_of(&d.o, "t").expect("walk t");
        let root = d.o.catalog.root_cell("t", None).expect("t's primary cell").load(Ordering::SeqCst);
        d.o.bp.fetch_page(root).expect("pin t's primary root");
        let _ = sql(&mut d.o, &mut Session::new(), "DROP TABLE t;");
        d.o.bp.unpin_page(root, false);
        insert_rows(&mut d.o, &mut Session::new(), "u", ROWS..ROWS + 2 * EXTRA).expect("more rows for u");
        // The crash: no checkpoint.
    }
    let mut want = fixture_want();
    want.present.remove("t");
    want.present.insert("u".to_string(), (0..ROWS + 2 * EXTRA).collect());
    want.either = Some(("t".to_string(), (0..ROWS).collect(), pages));
    if let Err(e) = two_good_opens(&m.snapshot(), &want) {
        panic!("a DROP refused at a pinned page, more rows elsewhere, then a crash: {e}");
    }
}

// ---------------------------------------------------------------------------------------------
// The falsifier, §7
// ---------------------------------------------------------------------------------------------

/// **F1: a crash at every operation of an open that owes a rebuild, then two opens.**
///
/// The window is the whole open (recovery, the rebuild with whatever it frees, and the
/// checkpoint), a superset of "inside `rebuild_indexes` and the reset".
///
/// RED at `a6e93ab` (INFERRED, design §7 F1a): the rebuild frees each old tree by walking it and the
/// fresh trees take those pages, so after a crash the next rebuild walks a zero page ("invalid page
/// type header") and every open from then on fails; with holes below, some points alias instead.
#[test]
fn a_crash_at_every_operation_of_a_rebuilding_open_leaves_two_good_opens() {
    let owed = owed_rebuild();
    let census = Machine::boot(&owed, None);
    {
        let d = census.open().expect("premise: the owed open fails on a clean machine");
        assert!(d.o.recovered, "premise: the log held nothing to replay, so this open rebuilt nothing");
    }
    let points = sweep_points(&census.fabric, 0, census.fabric.op_count());
    assert!(points.len() >= 40, "premise: the rebuilding open has only {} faultable operations", points.len());

    let want = owed_rebuild_want();
    let mut failures = Vec::new();
    for op in &points {
        let m = Machine::boot(&owed, Some(plan(*op)));
        let _ = catch_unwind(AssertUnwindSafe(|| m.open()));
        if let Err(e) = fired_at(&m, *op) {
            failures.push(e);
            continue;
        }
        if let Err(e) = two_good_opens(&m.snapshot(), &want) {
            failures.push(format!("crash at operation {op} of the rebuilding open: {e}"));
        }
    }
    report("a crash inside a rebuilding open", points.len(), &failures);
}

/// **F2: a crash at every operation of `DROP TABLE t`, then two opens.** `t` must be wholly present
/// with every row found by key, or wholly gone with none of its pages left allocated and unnamed,
/// and a table built afterwards must alias nothing.
///
/// The fixture's catalog spans two pages and dropping `t` lets it fit one, so this is also the
/// candidates adversary's (e) (D238): a shrinking persist frees the tail before page 1 is durable.
///
/// Here `t`'s last rows are still in the log at the DROP; [`drop_sweep`] explains the other stage.
///
/// RED at `a6e93ab` (INFERRED, design §7 F2): `drop_table` frees before it persists, so a crash in
/// between leaves `t` named over free pages.
#[test]
fn a_crash_at_every_operation_of_drop_table_leaves_the_table_whole_or_gone() {
    drop_sweep(true);
}

/// **F2 with no row of `t` in the log at the DROP.** Recovery then has no heap record of `t` to skip,
/// so every crash point rests on the intent and on D250's completion alone; the logged sweep above
/// also exercises D250's skip.
///
/// Added with the fix for a premise that is retired: that D229 frees only once no retained record
/// names the pages, so that with `t`'s rows in the log the frees would wait for the truncation. Under
/// D250 the DROP's own `DropTable` record is always in the log, and the frees run right after the
/// checkpoint's sync whatever the truncation does (D229 review 1's R5; the lead's decision, 10:05Z).
/// A guard with no mutant of its own: M3, which it was added for, is registered as a predicted
/// survivor (PREREG amendments 3 and 10), and a kill here would falsify that prediction.
#[test]
fn a_crash_at_every_operation_of_drop_table_with_no_row_of_it_in_the_log_leaves_it_whole_or_gone() {
    drop_sweep(false);
}

/// Open the fixture; when `logged`, add [`EXTRA`] rows to `t` so its records are in the log.
fn stage_drop(m: &Machine, logged: bool) -> Opened {
    if logged {
        stage_rows(m, "t")
    } else {
        m.open().expect("stage: open the fixture")
    }
}

fn drop_sweep(logged: bool) {
    let rows = if logged { ROWS + EXTRA } else { ROWS };
    let census = Machine::boot(fixture(), None);
    let (from, to, pages) = {
        let mut d = stage_drop(&census, logged);
        let pages = pages_of(&d.o, "t").expect("walk t");
        let before = walk(&d.o).expect("walk before the DROP");
        assert!(
            before.catalog_entries.iter().filter(|n| **n > 0).count() >= 2,
            "premise: the catalog's entries fit one page before the DROP: {:?}",
            before.catalog_entries
        );
        let from = census.fabric.op_count();
        must(&mut d.o, &mut Session::new(), "DROP TABLE t;");
        let to = census.fabric.op_count();
        let after = walk(&d.o).expect("walk after the DROP");
        assert_eq!(
            after.catalog_entries.iter().filter(|n| **n > 0).count(),
            1,
            "premise: the DROP did not let the catalog's entries fit one page, so it shrank nothing: {:?}",
            after.catalog_entries
        );
        (from, to, pages)
    };
    let points = sweep_points(&census.fabric, from, to);
    assert!(points.len() >= 20, "premise: the DROP has only {} faultable operations", points.len());

    let mut want = fixture_want();
    want.present.remove("t");
    want.either = Some(("t".to_string(), (0..rows).collect(), pages));
    let mut failures = Vec::new();
    for op in &points {
        let m = Machine::boot(fixture(), Some(plan(*op)));
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let mut d = stage_drop(&m, logged);
            let _ = sql(&mut d.o, &mut Session::new(), "DROP TABLE t;");
        }));
        if let Err(e) = fired_at(&m, *op) {
            failures.push(e);
            continue;
        }
        if let Err(e) = two_good_opens(&m.snapshot(), &want) {
            failures.push(format!("crash at operation {op} of the DROP: {e}"));
        }
    }
    let what = if logged { "a crash inside DROP TABLE" } else { "a crash inside DROP TABLE, nothing of it in the log" };
    report(what, points.len(), &failures);
}

/// **F3: the zero-root brick, aimed directly.** The open's checkpoint writes pages in ascending id,
/// so the catalog's page 1, naming the rebuilt roots, reaches the disk before the rebuilt trees do.
/// A crash right after it leaves the catalog naming roots that are zero pages on disk.
///
/// RED at `a6e93ab` (INFERRED, design §7 F3): the next open's rebuild walks such a root from its
/// record and fails with "invalid page type header".
#[test]
fn an_open_crashed_after_the_rebuilt_catalog_page_reached_disk_still_opens() {
    let owed = owed_rebuild();
    let census = Machine::boot(&owed, None);
    drop(census.open().expect("premise: the owed open fails on a clean machine"));
    let trace = census.fabric.trace();
    let page_one = trace
        .iter()
        .filter(|op| op.file == FAB_DB && op.kind == OpKind::Pwrite && op.offset == PAGE_SIZE as u64)
        .map(|op| op.index)
        .last()
        .expect("premise: the open never wrote the catalog's page 1");
    let point = census
        .fabric
        .faultable_ops()
        .into_iter()
        .find(|op| *op > page_one)
        .expect("premise: nothing follows the catalog page's write");
    assert!(
        trace.iter().any(|op| op.index > page_one && op.file == FAB_DB && op.kind == OpKind::Pwrite),
        "premise: no page write follows the catalog page's, so the rebuilt trees were already on disk"
    );

    let m = Machine::boot(&owed, Some(plan(point)));
    let _ = catch_unwind(AssertUnwindSafe(|| m.open()));
    fired_at(&m, point).expect("premise: the crash");
    if let Err(e) = two_good_opens(&m.snapshot(), &owed_rebuild_want()) {
        panic!("a crash after the rebuilt catalog page reached the disk and before the rebuilt trees did: {e}");
    }
}

/// **F5: a quiet database has no page allocated and unnamed** (design §7 F5), after a workload
/// that allocated every kind of page this file has: bitmap and catalog chains, heap directories and
/// pages, B+tree nodes of all three kinds, a DROP, and the fresh trees of a rebuild. Checked on the
/// live database after a checkpoint and again after a clean reopen.
///
/// The detector is forced to fire first: a page taken with `new_page` and linked nowhere must be
/// the one page it names. GREEN at `a6e93ab` (INFERRED): this is a guard, not a red test.
#[test]
fn a_quiet_database_has_no_page_allocated_and_unnamed() {
    let m = Machine::boot(fixture(), None);
    {
        let mut d = m.open().expect("open the fixture");
        insert_rows(&mut d.o, &mut Session::new(), "t", ROWS..ROWS + EXTRA).expect("rows into t");
        must(&mut d.o, &mut Session::new(), "DROP TABLE u;");
        fresh_table(&mut d.o).expect("a fresh table");
        d.o.txn.checkpoint().expect("the checkpoint");
        assert_eq!(leaked(&d.o).expect("walk"), Vec::<u32>::new(), "a quiet database after a checkpoint");

        // The detector must be able to fire.
        let planted = d.o.bp.new_page().expect("plant a page");
        assert_eq!(leaked(&d.o).expect("walk"), vec![planted], "the leak detector did not name the one page planted");
        d.o.bp.delete_page(planted).expect("take the planted page back");
        d.o.txn.checkpoint().expect("the second checkpoint");
    }
    let m2 = Machine::boot(&m.snapshot(), None);
    let d = m2.open().expect("the clean reopen");
    assert_eq!(leaked(&d.o).expect("walk"), Vec::<u32>::new(), "a quiet database after a clean reopen");
}

/// **The oracle fires** on each thing it looks for, so a green sweep is not a detector that sees
/// nothing: a page named by two structures, a named page whose bit is clear, and a row missing by
/// key.
#[test]
fn the_oracle_fires_on_a_planted_alias_a_planted_free_and_a_missing_key() {
    let m = Machine::boot(fixture(), None);
    let mut d = m.open().expect("open the fixture");
    oracle(&mut d.o, &fixture_want()).expect("premise: the fixture passes the oracle");

    // A page of u's heap listed in t's directory too.
    let u_page = walk(&d.o)
        .expect("walk")
        .owner
        .into_iter()
        .find(|(_, by)| by == "u's heap")
        .map(|(p, _)| p)
        .expect("u has a heap page");
    let t_dir = d.o.catalog.get_table("t").expect("t").first_directory_page_id;
    HeapFileManager::open(t_dir, d.o.bp.clone()).add_to_directory(u_page, 0).expect("plant the alias");
    let e = structure(&d.o).err().expect("the oracle missed a page named by two structures");
    assert!(e.contains("two structures"), "the oracle fired, but not for the alias: {e}");

    // A named page whose bit is clear.
    let m2 = Machine::boot(fixture(), None);
    let d2 = m2.open().expect("open the fixture again");
    d2.o.bp.disk_manager.deallocate(u_page).expect("plant the free");
    let e = structure(&d2.o).err().expect("the oracle missed a named page whose bit is clear");
    assert!(e.contains("bit is clear"), "the oracle fired, but not for the free page: {e}");

    // A row missing by key: its primary entry removed.
    let m3 = Machine::boot(fixture(), None);
    let mut d3 = m3.open().expect("open the fixture a third time");
    let cell = d3.o.catalog.root_cell("u", None).expect("u's primary cell");
    BPlusTreeManager::<Value, RecordId>::open_shared(cell, d3.o.bp.clone())
        .delete(&Value::Integer(7))
        .expect("plant a missing key");
    let e = check_rows(&mut d3.o, "u", &(0..ROWS).collect::<Vec<_>>()).err().expect("the oracle missed a row missing by key");
    assert!(e.contains("missing by key"), "the oracle fired, but not for the missing key: {e}");
}

// ---------------------------------------------------------------------------------------------
// The fix's own guards: each is red only under the mutant named in it (added with the fix)
// ---------------------------------------------------------------------------------------------

/// A page file that fails ONE sync: once armed, the first sync after a write at offset 0, the
/// allocation bitmap's first page. Inside a DROP that is the sync its frees end with: nothing else a
/// DROP does writes the bitmap.
struct FailSyncAfterBitmap {
    inner: Arc<dyn Storage>,
    armed: AtomicBool,
    bitmap_written: AtomicBool,
    fired: AtomicBool,
}

impl Storage for FailSyncAfterBitmap {
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        if self.armed.load(Ordering::SeqCst) && offset == 0 {
            self.bitmap_written.store(true, Ordering::SeqCst);
        }
        self.inner.pwrite(buf, offset)
    }
    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        self.inner.pread(buf, offset)
    }
    fn sync_all(&self) -> std::io::Result<()> {
        if self.armed.load(Ordering::SeqCst) && self.bitmap_written.load(Ordering::SeqCst) && !self.fired.swap(true, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected: the sync after a DROP's frees fails, once"));
        }
        self.inner.sync_all()
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }
}

/// A page file whose every read of one page fails.
struct FailReadsOf {
    inner: Arc<dyn Storage>,
    page: u32,
}

impl Storage for FailReadsOf {
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        self.inner.pwrite(buf, offset)
    }
    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        if offset / PAGE_SIZE as u64 == self.page as u64 {
            return Err(std::io::Error::other(format!("injected: page {} does not read", self.page)));
        }
        self.inner.pread(buf, offset)
    }
    fn sync_all(&self) -> std::io::Result<()> {
        self.inner.sync_all()
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }
}

/// One `notes` row per heap page: a 3900-byte note leaves no room for a second.
fn note_sql(id: i32) -> String {
    format!("INSERT INTO notes VALUES ({id}, '{}');", "n".repeat(3900))
}

/// **Review §3.3 with the bits already clear: a DROP whose frees cleared their bits and then failed,
/// more writes that grow a heap's directory, then a crash.** Recovery must take none of the dropped
/// table's pages.
///
/// The frees fail at their sync, after the bitmap was written, so `t`'s bits are clear on disk and
/// the intent is still there. `notes` has exactly one directory page's worth of heap pages when the
/// DROP runs, and one more row after it, so the next open's directory repair must allocate a
/// directory page, and `allocate` hands out the lowest clear bit, which is one of `t`'s: `t` was
/// created first. Only the quarantine taken BEFORE `recover` (the review's A1) keeps it from
/// becoming `notes`' directory page, which the intent then frees under it.
///
/// Mutant-only red: M5 adopts the intents after `recover`.
#[test]
fn a_drop_whose_frees_failed_after_clearing_bits_lets_recovery_take_none_of_its_pages() {
    let m = Machine::boot(&Snapshot::default(), None);
    let flaky = Arc::new(FailSyncAfterBitmap {
        inner: m.fabric.open(FAB_DB),
        armed: AtomicBool::new(false),
        bitmap_written: AtomicBool::new(false),
        fired: AtomicBool::new(false),
    });
    let (pages, notes_dir);
    {
        let mut d = m.open_on(flaky.clone()).expect("open a new database");
        let s = &mut Session::new();
        must(&mut d.o, s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
        for id in 0..3 {
            must(&mut d.o, s, &format!("INSERT INTO t VALUES ({id}, {id});"));
        }
        must(&mut d.o, s, "CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));");
        must(&mut d.o, s, "BEGIN;");
        for id in 0..MAX_ENTRIES as i32 {
            must(&mut d.o, s, &note_sql(id));
        }
        must(&mut d.o, s, "COMMIT;");
        d.o.txn.checkpoint().expect("the checkpoint before the DROP");
        notes_dir = d.o.catalog.get_table("notes").expect("notes").first_directory_page_id;
        pages = pages_of(&d.o, "t").expect("walk t");
        flaky.armed.store(true, Ordering::SeqCst);
        must(&mut d.o, s, "DROP TABLE t;");
        assert!(flaky.fired.load(Ordering::SeqCst), "premise: the sync after the DROP's frees never failed, so the intent was carried out");
        must(&mut d.o, s, &note_sql(MAX_ENTRIES as i32));
        // The crash: no checkpoint.
    }
    let crash = m.snapshot();
    assert!(crash.side.keys().any(|n| n.ends_with(".drop-intent")), "premise: no intent survived the crash, so the open had nothing to protect");
    let db = &crash.images[FAB_DB];
    let bit = |p: u32| db[(p / 8) as usize + 4] & (1 << (p % 8)) != 0;
    assert!(pages.iter().any(|p| !bit(*p)), "premise: every page of t is still allocated on disk, so recovery could not take one");
    let at = notes_dir as usize * PAGE_SIZE;
    let dir = PageDirectory::deserialize(db[at..at + PAGE_SIZE].try_into().unwrap());
    assert!(
        dir.entries.len() == MAX_ENTRIES && dir.next_page_directory == 0,
        "premise: notes' directory on disk holds {} entries and links page {}, so recovery allocates no directory page",
        dir.entries.len(),
        dir.next_page_directory
    );
    let mut want = Want::default();
    want.present.insert("notes".to_string(), (0..=MAX_ENTRIES as i32).collect());
    want.either = Some(("t".to_string(), (0..3).collect(), pages));
    if let Err(e) = two_good_opens(&crash, &want) {
        panic!("frees that failed after clearing their bits, a directory that grew, then a crash: {e}");
    }
}

/// A page taken from the allocator and given a heap page's bytes, then linked from nothing: what
/// a table the catalog lost would leave, or a leak. Flushed, then rows go into `u` so the next open
/// owes a rebuild. Returns the page and its bytes.
fn plant_unnamed_heap_page(m: &Machine) -> (u32, [u8; PAGE_SIZE]) {
    let mut d = m.open().expect("open the fixture");
    let planted = d.o.bp.new_page().expect("take a page");
    let mut heap_page = Page::empty(planted);
    heap_page.insert(Tuple::new(vec![0xAB; 64])).expect("a tuple on the page");
    let image = heap_page.serialize().expect("serialize the page");
    let frame_i = d.o.bp.fetch_page(planted).expect("fetch the page");
    d.o.bp.frame_write(frame_i).data = image;
    d.o.bp.unpin_page(planted, true);
    d.o.bp.flush_all().expect("flush");
    insert_rows(&mut d.o, &mut Session::new(), "u", ROWS..ROWS + EXTRA).expect("rows, so the next open owes a rebuild");
    (planted, image)
}

/// **The reset keeps an allocated page nothing names when it holds a heap page, not an index**
/// (D229 (b)'s identity filter). Freeing every unnamed page is reachability alone, which frees
/// whatever a torn catalog forgets (the design's §5.3, rejected by the decision).
///
/// Mutant-only red: M1 frees every unnamed page it can read.
#[test]
fn the_reset_keeps_an_unnamed_page_that_holds_no_index() {
    let m = Machine::boot(fixture(), None);
    let (planted, image) = plant_unnamed_heap_page(&m);
    let m2 = Machine::boot(&m.snapshot(), None);
    let d = m2.open().expect("the rebuilding open");
    assert!(d.o.recovered, "premise: the open replayed nothing, so no reset ran");
    assert!(allocated(&d.o.bp).expect("read the bitmap").contains(&planted), "the reset freed page {planted}, which holds a heap page");
    assert_eq!(page(&d.o.bp, planted).expect("read the page"), image, "the reset changed page {planted}");
}

/// **The reset keeps a page it cannot read** (the D228 lesson: a read error is not evidence that
/// nothing lives there).
///
/// Mutant-only red: M6 frees a candidate whose read fails.
#[test]
fn the_reset_keeps_a_page_it_cannot_read() {
    let m = Machine::boot(fixture(), None);
    let (planted, _) = plant_unnamed_heap_page(&m);
    let m2 = Machine::boot(&m.snapshot(), None);
    let failing = Arc::new(FailReadsOf { inner: m2.fabric.open(FAB_DB), page: planted });
    let d = m2.open_on(failing).expect("the rebuilding open");
    assert!(d.o.recovered, "premise: the open replayed nothing, so no reset ran");
    assert!(allocated(&d.o.bp).expect("read the bitmap").contains(&planted), "the reset freed page {planted}, which it could not read");
}

/// **The reset keeps a page a directory lists even when its bytes are zeros** (D229 (b)'s keep
/// set). A heap page listed before its own image reached the disk was one: `new_page` zero-writes
/// it, the directory naming it is flushed, the page is not. Since the listed-zero-page fix
/// (`add_empty_page` writes the empty image before listing, PREREG amendment 12) this build no longer
/// leaves one, so the zero image is planted, as a crash under an earlier build left it (restated in
/// PREREG amendment 13; the assertions are unchanged).
///
/// It is put on `u`'s TIME-TRAVEL heap: on a primary heap an all-zero listed page makes the
/// rebuild's heap scan panic in `Page::deserialize` (a separate hazard, not this lane's), and the
/// rebuild never scans a time-travel heap.
///
/// Mutant-only red: M2 frees an index-shaped or zero page whether or not something names it.
#[test]
fn the_reset_keeps_a_page_a_directory_lists_whose_bytes_are_zeros() {
    let m = Machine::boot(fixture(), None);
    let listed;
    {
        let mut d = m.open().expect("open the fixture");
        let tt = d.o.catalog.get_table("u").expect("u").time_travel_root;
        listed = HeapFileManager::open(tt, d.o.bp.clone())
            .find_or_make_page(MAX_TUPLE_SIZE)
            .expect("a new page on u's time-travel heap");
        let mut dir_id = tt;
        loop {
            let dir = PageDirectory::deserialize(page(&d.o.bp, dir_id).expect("read the directory"));
            if dir.entries.iter().any(|e| e.page_id == listed) {
                break;
            }
            assert_ne!(dir.next_page_directory, 0, "premise: no directory page of u's time-travel heap lists page {listed}");
            dir_id = dir.next_page_directory;
        }
        d.o.bp.flush_page(dir_id).expect("flush the directory page alone");
        d.o.bp.disk_manager.write(listed, &[0u8; PAGE_SIZE]).expect("the zero image an earlier build's crash left");
        insert_rows(&mut d.o, &mut Session::new(), "t", ROWS..ROWS + EXTRA).expect("rows, so the next open owes a rebuild");
    }
    let crash = m.snapshot();
    let db = &crash.images[FAB_DB];
    let at = listed as usize * PAGE_SIZE;
    assert!(
        db.len() >= at + PAGE_SIZE && db[at..at + PAGE_SIZE].iter().all(|b| *b == 0),
        "premise: page {listed}'s own image reached the disk, so it is not a zero page"
    );
    let m2 = Machine::boot(&crash, None);
    let d = m2.open().expect("the rebuilding open");
    assert!(d.o.recovered, "premise: the open replayed nothing, so no reset ran");
    let r = structure(&d.o).unwrap_or_else(|e| panic!("after the rebuilding open: {e}"));
    assert_eq!(
        r.owner.get(&listed).map(String::as_str),
        Some("u's time-travel heap"),
        "page {listed}, listed by u's time-travel heap, is not that heap's page any more"
    );
}

/// **A DROP whose mutation fails frees nothing until the next open completes it** (restated after
/// the D250 merge, PREREG amendment 3; it had asserted the table survives such a DROP).
///
/// The intent is written before the `DropTable` record, and the record before the mutation, so a
/// mutation that fails leaves both. D250 then poisons the log: no checkpoint can run, so nothing is
/// freed in this process, while the log calls the table dropped. The next open completes the DROP
/// from the log, decides the intent by the catalog it has just completed, and frees the pages after
/// its checkpoint. So no page is freed while a durable structure still names it, and none leaks.
#[test]
fn a_drop_whose_mutation_fails_frees_nothing_until_the_next_open_completes_it() {
    let m = Machine::boot(fixture(), None);
    let pages;
    {
        let d = m.open().expect("open the fixture");
        pages = pages_of(&d.o, "t").expect("walk t");
        let e = d.o.catalog.get_table("t").expect("t").clone();
        let record = DdlRecord {
            op: DdlOp::DropTable,
            table: "t".into(),
            dir_root: e.first_directory_page_id,
            time_travel_root: e.time_travel_root,
            columns: Vec::new(),
        };
        let named = d.o.catalog.table_pages("t").expect("t's pages");
        let failed = d.o.txn.drop_checkpointed(record, named, || {
            Err::<(), FerroError>(FerroError::Internal("injected: the DROP's mutation fails after its record".into()))
        });
        assert!(failed.is_err(), "premise: the injected failure did not reach the caller");
        assert!(d.o.catalog.get_table("t").is_some(), "premise: the mutation that failed dropped the table anyway");
        assert!(d.o.txn.checkpoint().is_err(), "a checkpoint ran after a DROP that failed after its record: the log is not poisoned");
        let bits = allocated(&d.o.bp).expect("read the bitmap");
        let freed: Vec<u32> = pages.iter().copied().filter(|p| !bits.contains(p)).collect();
        assert!(freed.is_empty(), "a DROP that failed freed {} page(s) the catalog still names, e.g. {:?}", freed.len(), &freed[..freed.len().min(5)]);
        assert!(m.dir.path().join(format!("{DB_NAME}.wal.drop-intent")).exists(), "premise: the failed DROP left no intent, so the open decides nothing");
        // The crash: the process dies with the log poisoned and the intent undecided.
    }
    let mut want = fixture_want();
    want.present.remove("t");
    want.either = Some(("t".to_string(), (0..ROWS).collect(), pages));
    let m2 = Machine::boot(&m.snapshot(), None);
    {
        let mut d = m2.open().expect("the open after the failed DROP");
        assert!(d.o.catalog.get_table("t").is_none(), "the next open did not complete the DROP the log records (D250)");
        oracle(&mut d.o, &want).unwrap_or_else(|e| panic!("after the open that completed the DROP: {e}"));
    }
    assert!(!m2.dir.path().join(format!("{DB_NAME}.wal.drop-intent")).exists(), "the open left the completed DROP's intent in place");
}

/// **An intent left for a table that was re-created at its root frees none of the new table's
/// pages** (the lead, after D250 review 1's F2; PREREG amendment 5).
///
/// `r` is dropped under a WAL pin, so its `DropTable` record stays in the log, and re-created,
/// taking the dropped heap's root, with heap records above the DROP's LSN: D250 F2 keeps it out of
/// the open's completion. The old table's intent is then PLANTED, as a lost removal would leave it:
/// D229's own order removes an intent durably (A4) before its pages leave the quarantine, so no table
/// can take the root while the intent is pending, and this state is unreachable through it. The open
/// must decide the intent by identity (a table is at its root, so it is dropped, not carried out)
/// and free nothing of the new `r`.
///
/// Mutant-only red: M13 decides every intent as carried out.
#[test]
fn a_stale_intent_for_a_dropped_root_frees_nothing_of_the_table_re_created_there() {
    let m = Machine::boot(&Snapshot::default(), None);
    {
        let mut d = m.open().expect("open a new database");
        let s = &mut Session::new();
        must(&mut d.o, s, "CREATE TABLE r (id INTEGER NOT NULL, v INTEGER);");
        for id in 0..5 {
            must(&mut d.o, s, &format!("INSERT INTO r VALUES ({id}, {id});"));
        }
        let root = d.o.catalog.get_table("r").expect("r").first_directory_page_id;
        let old_pages = d.o.catalog.table_pages("r").expect("r's pages");
        let base = d.o.wal.base_lsn.load(Ordering::SeqCst);
        let pin = d.o.wal.pin(base).expect("pin the log at its base");
        must(&mut d.o, s, "DROP TABLE r;");
        assert_eq!(d.o.wal.base_lsn.load(Ordering::SeqCst), base, "premise: the pin did not keep the DROP's record in the log");
        must(&mut d.o, s, "CREATE TABLE r (id INTEGER NOT NULL, v INTEGER);");
        assert_eq!(
            d.o.catalog.get_table("r").expect("r again").first_directory_page_id,
            root,
            "premise: the re-created r is not at the dropped root, so D250 F2 has nothing to skip"
        );
        for id in 100..105 {
            must(&mut d.o, s, &format!("INSERT INTO r VALUES ({id}, {id});"));
        }
        let now = pages_of(&d.o, "r").expect("walk the new r");
        assert!(
            old_pages.iter().any(|p| now.contains(p)),
            "premise: the new r holds none of the old r's pages, so a wrong free could hit nothing of it"
        );
        free_intent::store(&OsFileOps, &d.o.wal.path, &[FreeIntent { table: "r".into(), dir_root: root, pages: old_pages }])
            .expect("plant the old r's intent");
        drop(pin);
        // The crash.
    }
    let m2 = Machine::boot(&m.snapshot(), None);
    {
        let mut d = m2.open().expect("the open with a stale intent");
        assert!(
            d.o.completed_drops.is_empty(),
            "the open completed the DROP of a table re-created at its root: {:?}",
            d.o.completed_drops
        );
        let mut want = Want::default();
        want.present.insert("r".to_string(), (100..105).collect());
        oracle(&mut d.o, &want).unwrap_or_else(|e| panic!("after the open with a stale intent for r's root: {e}"));
    }
    assert!(!m2.dir.path().join(format!("{DB_NAME}.wal.drop-intent")).exists(), "the open left the stale intent in place");
}

/// A page file whose next sync fails once, when armed.
struct FailNextSync {
    inner: Arc<dyn Storage>,
    armed: AtomicBool,
}

impl Storage for FailNextSync {
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        self.inner.pwrite(buf, offset)
    }
    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        self.inner.pread(buf, offset)
    }
    fn sync_all(&self) -> std::io::Result<()> {
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected: this sync fails, once"));
        }
        self.inner.sync_all()
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }
}

/// **An intent still pending at a crash frees nothing of the table re-created after its DROP**
/// (the lead's schedule; PREREG amendment 7). Two arms: the CREATE's checkpoint sync succeeds, or
/// fails (the table then exists while the statement reports failure, A8's shape).
///
/// `DROP t`'s frees clear their bits and then fail at their sync, so the intent stays pending with
/// its pages quarantined; a freed page is then pinned, so every later retry is refused and the intent
/// is still pending at the crash. `notes` takes two new pages and `t` is re-created. Its first
/// directory page CANNOT be the dropped one, which is quarantined, so at the open no table sits at
/// the intent's root and the intent is carried out. That is right only because the quarantine kept
/// every page of it away from `notes` and the new `t`: the identity check alone would not.
///
/// Mutant-only red: M14 makes `allocate` ignore the quarantine.
#[test]
fn an_intent_left_pending_by_a_crash_frees_nothing_of_the_table_re_created_after_its_drop() {
    for create_sync_fails in [false, true] {
        let arm = if create_sync_fails { "the CREATE's sync failed" } else { "the CREATE's sync succeeded" };
        let m = Machine::boot(&Snapshot::default(), None);
        let after_bitmap = Arc::new(FailSyncAfterBitmap {
            inner: m.fabric.open(FAB_DB),
            armed: AtomicBool::new(false),
            bitmap_written: AtomicBool::new(false),
            fired: AtomicBool::new(false),
        });
        let next_sync = Arc::new(FailNextSync { inner: after_bitmap.clone(), armed: AtomicBool::new(false) });
        let old_pages;
        {
            let mut d = m.open_on(next_sync.clone()).expect("open a new database");
            let s = &mut Session::new();
            must(&mut d.o, s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
            for id in 1..=3 {
                must(&mut d.o, s, &format!("INSERT INTO t VALUES ({id}, {});", id * 10));
            }
            must(&mut d.o, s, "CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));");
            must(&mut d.o, s, &note_sql(0));
            let old_root = d.o.catalog.get_table("t").expect("t").first_directory_page_id;
            old_pages = d.o.catalog.table_pages("t").expect("t's pages");
            after_bitmap.armed.store(true, Ordering::SeqCst);
            must(&mut d.o, s, "DROP TABLE t;");
            assert!(after_bitmap.fired.load(Ordering::SeqCst), "{arm}: premise: the sync after the DROP's frees never failed, so its intent was carried out");
            let bits = allocated(&d.o.bp).expect("read the bitmap");
            assert!(old_pages.iter().any(|p| !bits.contains(p)), "{arm}: premise: every page of the old t is still allocated, so the quarantine had nothing to keep");
            // Every later retry of the batch is refused while this pin is held, which is to the crash.
            d.o.bp.fetch_page(old_pages[0]).expect("pin a page of the old t");
            must(&mut d.o, s, &note_sql(1));
            must(&mut d.o, s, &note_sql(2));
            next_sync.armed.store(create_sync_fails, Ordering::SeqCst);
            let created = sql(&mut d.o, s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
            assert_eq!(created.is_err(), create_sync_fails, "{arm}: premise: the CREATE's outcome is not this arm's: {:?}", created.err());
            let new_root = d.o.catalog.get_table("t").expect("the re-created t").first_directory_page_id;
            assert_ne!(new_root, old_root, "{arm}: premise: the re-created t took the dropped first directory page, which the quarantine holds");
            for id in 1..=3 {
                must(&mut d.o, s, &format!("INSERT INTO t VALUES ({id}, {});", id * 100));
            }
            assert!(m.dir.path().join(format!("{DB_NAME}.wal.drop-intent")).exists(), "{arm}: premise: the intent is not pending at the crash");
            // The crash, with the pin and the intent still held.
        }
        let m2 = Machine::boot(&m.snapshot(), None);
        {
            let mut d = m2.open().unwrap_or_else(|e| panic!("{arm}: the open failed: {e}"));
            assert!(d.o.completed_drops.is_empty(), "{arm}: the open completed a DROP: {:?}", d.o.completed_drops);
            let bits = allocated(&d.o.bp).expect("read the bitmap");
            let mut owned = pages_of(&d.o, "t").expect("walk the new t");
            owned.extend(pages_of(&d.o, "notes").expect("walk notes"));
            let lost: Vec<u32> = owned.iter().copied().filter(|p| !bits.contains(p)).collect();
            assert!(lost.is_empty(), "{arm}: the open freed page(s) the re-created t or notes own: {lost:?}");
            match sql(&mut d.o, &mut Session::new(), "SELECT v FROM t WHERE id = 2;") {
                Ok(Outcome::Rows(rows)) => assert_eq!(rows, vec![vec![Value::Integer(200)]], "{arm}: row 2 of the re-created t"),
                other => panic!("{arm}: SELECT of row 2 returned no rows: {:?}", other.err()),
            }
            let mut want = Want::default();
            want.present.insert("t".to_string(), vec![1, 2, 3]);
            want.present.insert("notes".to_string(), vec![0, 1, 2]);
            oracle(&mut d.o, &want).unwrap_or_else(|e| panic!("{arm}: after the open: {e}"));
            let r = walk(&d.o).expect("walk");
            let left: Vec<u32> = old_pages.iter().copied().filter(|p| bits.contains(p) && !r.owner.contains_key(p)).collect();
            assert!(left.is_empty(), "{arm}: the intent was not carried out: old t pages still allocated and unnamed: {left:?}");
        }
        assert!(!m2.dir.path().join(format!("{DB_NAME}.wal.drop-intent")).exists(), "{arm}: the open left the intent in place");
    }
}

// ---------------------------------------------------------------------------------------------
// D229 review 1's R7 and the merge review's §4: the orders a crash sweep cannot aim at
// ---------------------------------------------------------------------------------------------

/// One step the open or a statement took, in the order taken.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Ev {
    /// A sync of the page file (`"db"`) or the log (`"wal"`) that succeeded.
    Synced(&'static str),
    /// Pages whose bits on the first bitmap page a write turned from set to clear: pages freed.
    Cleared(Vec<u32>),
    /// Pages whose bits on the first bitmap page a write turned from clear to set.
    Set(Vec<u32>),
    /// A step through the file-ops double: its name, its path, and how it ended.
    Fs(&'static str, PathBuf, Result<(), std::io::ErrorKind>),
}

/// The steps, shared by the page file, the log and the file-ops double of one test.
type Events = Arc<Mutex<Vec<Ev>>>;

/// A page file or a log that records its syncs into `events`, and, for the page file, every change
/// to the first bitmap page's bits. When armed, its next `sync_data` fails once: that is a log
/// flush's sync.
struct Watched {
    inner: Arc<dyn Storage>,
    name: &'static str,
    events: Events,
    fail_next_sync_data: AtomicBool,
}

impl Watched {
    fn new(inner: Arc<dyn Storage>, name: &'static str, events: &Events) -> Arc<Watched> {
        Arc::new(Watched { inner, name, events: events.clone(), fail_next_sync_data: AtomicBool::new(false) })
    }
}

impl Storage for Watched {
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        if self.name != "db" || offset != 0 || buf.len() != PAGE_SIZE {
            return self.inner.pwrite(buf, offset);
        }
        let mut old = [0u8; PAGE_SIZE];
        self.inner.pread(&mut old, 0)?;
        let wrote = self.inner.pwrite(buf, offset)?;
        let (mut cleared, mut set) = (Vec::new(), Vec::new());
        for byte in 4..PAGE_SIZE {
            for bit in 0..8 {
                let page = ((byte - 4) * 8 + bit) as u32;
                match ((old[byte] >> bit) & 1, (buf[byte] >> bit) & 1) {
                    (1, 0) => cleared.push(page),
                    (0, 1) => set.push(page),
                    _ => {}
                }
            }
        }
        let mut events = self.events.lock().unwrap();
        if !cleared.is_empty() {
            events.push(Ev::Cleared(cleared));
        }
        if !set.is_empty() {
            events.push(Ev::Set(set));
        }
        Ok(wrote)
    }
    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        self.inner.pread(buf, offset)
    }
    fn sync_all(&self) -> std::io::Result<()> {
        self.inner.sync_all()?;
        self.events.lock().unwrap().push(Ev::Synced(self.name));
        Ok(())
    }
    fn sync_data(&self) -> std::io::Result<()> {
        if self.fail_next_sync_data.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected: this sync_data fails, once"));
        }
        self.inner.sync_data()?;
        self.events.lock().unwrap().push(Ev::Synced(self.name));
        Ok(())
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }
}

/// **The file-ops double** (D229 review 1's R7): every step goes to the real filesystem
/// ([`OsFileOps`]) and is recorded into `events` with how it ended. Two faults can be armed, each
/// firing once: the next `remove` fails, or the next `sync_dir` after a successful `remove` fails.
struct Ops {
    events: Events,
    fail_next_remove: AtomicBool,
    fail_sync_dir_after_remove: AtomicBool,
    removed: AtomicBool,
}

impl Ops {
    fn new(events: &Events) -> Arc<Ops> {
        Arc::new(Ops {
            events: events.clone(),
            fail_next_remove: AtomicBool::new(false),
            fail_sync_dir_after_remove: AtomicBool::new(false),
            removed: AtomicBool::new(false),
        })
    }

    fn note<T>(&self, step: &'static str, path: &Path, r: &std::io::Result<T>) {
        let outcome = r.as_ref().map(|_| ()).map_err(|e| e.kind());
        self.events.lock().unwrap().push(Ev::Fs(step, path.to_path_buf(), outcome));
    }
}

impl FileOps for Ops {
    fn write(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        let r = OsFileOps.write(path, bytes);
        self.note("write", path, &r);
        r
    }
    fn append(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        let r = OsFileOps.append(path, bytes);
        self.note("append", path, &r);
        r
    }
    fn sync_file(&self, path: &Path) -> std::io::Result<()> {
        let r = OsFileOps.sync_file(path);
        self.note("sync_file", path, &r);
        r
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        let r = OsFileOps.rename(from, to);
        self.note("rename", to, &r);
        r
    }
    fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
        let r = if self.removed.load(Ordering::SeqCst) && self.fail_sync_dir_after_remove.swap(false, Ordering::SeqCst) {
            Err(std::io::Error::other("injected: the directory sync after a remove fails, once"))
        } else {
            OsFileOps.sync_dir(dir)
        };
        self.note("sync_dir", dir, &r);
        r
    }
    fn remove(&self, path: &Path) -> std::io::Result<()> {
        let r = if self.fail_next_remove.swap(false, Ordering::SeqCst) {
            Err(std::io::Error::other("injected: this remove fails, once"))
        } else {
            OsFileOps.remove(path)
        };
        if r.is_ok() {
            self.removed.store(true, Ordering::SeqCst);
        }
        self.note("remove", path, &r);
        r
    }
}

/// The DROP intent file of the database on `m`.
fn intent_file(m: &Machine) -> PathBuf {
    m.dir.path().join(format!("{DB_NAME}.wal.drop-intent"))
}

/// `t (id, v)` with rows 1, 2 and 3.
fn small_t(o: &mut OpenedDatabase, s: &mut Session) {
    must(o, s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
    for id in 1..=3 {
        must(o, s, &format!("INSERT INTO t VALUES ({id}, {id});"));
    }
}

/// **A retried removal of the intent file syncs its directory before the quarantine is released**
/// (D229 review 1's R1; PREREG amendment 9). The double fails the directory sync that follows the
/// intent's removal, once: the removal happened but is not durable, so the intent stays pending and its
/// pages stay quarantined. The retry (the next checkpoint) finds the file already gone, and only a
/// directory sync then makes the earlier removal survive a power cut. Returning at `NotFound` released
/// the quarantine over an intent a power loss could bring back, and the next open would free the pages
/// again under whatever had taken them.
///
/// Mutant-only red: M17 returns at `NotFound` before the directory sync.
#[test]
fn a_retried_intent_removal_syncs_the_directory_before_the_quarantine_is_released() {
    let m = Machine::boot(&Snapshot::default(), None);
    let events = Events::default();
    let ops = Ops::new(&events);
    let mut d = m.open_with(m.fabric.open(FAB_DB), m.fabric.open(FAB_WAL), Some(ops.clone())).expect("open a new database");
    let s = &mut Session::new();
    small_t(&mut d.o, s);
    let old_t = d.o.catalog.table_pages("t").expect("t's pages");
    ops.fail_sync_dir_after_remove.store(true, Ordering::SeqCst);
    must(&mut d.o, s, "DROP TABLE t;");
    assert!(
        events.lock().unwrap().iter().any(|e| matches!(e, Ev::Fs("sync_dir", _, Err(_)))),
        "premise: the directory sync after the intent's removal never failed"
    );
    assert!(!intent_file(&m).exists(), "premise: the intent file was not removed before its directory sync failed");
    let quarantined = d.o.bp.disk_manager.quarantined();
    let released: Vec<u32> = old_t.iter().copied().filter(|p| !quarantined.contains(p)).collect();
    assert!(released.is_empty(), "the dropped t's pages {released:?} left the quarantine while the removal of its intent was not durable");

    let mark = events.lock().unwrap().len();
    d.o.txn.checkpoint().expect("the checkpoint that retries the removal");
    let after: Vec<Ev> = events.lock().unwrap()[mark..].to_vec();
    let gone = after
        .iter()
        .position(|e| *e == Ev::Fs("remove", intent_file(&m), Err(std::io::ErrorKind::NotFound)))
        .unwrap_or_else(|| panic!("premise: the retry did not find the intent file already gone: {after:?}"));
    assert!(
        after[gone + 1..].iter().any(|e| matches!(e, Ev::Fs("sync_dir", dir, Ok(())) if dir.as_path() == m.dir.path())),
        "the retry found the intent file gone and did not sync its directory, so the earlier removal may not survive a power cut: {after:?}"
    );
    assert_eq!(d.o.bp.disk_manager.quarantined(), Vec::<u32>::new(), "premise: the retry did not release the quarantine");
    assert_eq!(d.o.txn.pending_free_pages(), Vec::<u32>::new(), "premise: the intent is still pending after the retry");
}

/// **A DROP's pages stay out of use until its intent is durably gone** (A4; D229 review 1's R7,
/// PREREG amendment 9). The double fails the intent's removal once, after the pages were freed. The
/// intent file still names them then, so a crash would free them again at the next open: no table may
/// take one until a retry has removed the intent. `notes` takes two new heap pages in between (no
/// checkpoint runs), the retry runs, and then the crash.
///
/// Mutant-only red: M16 releases the quarantine before the intent file is rewritten.
#[test]
fn an_intent_whose_removal_fails_keeps_its_pages_out_of_use_until_it_is_gone() {
    let m = Machine::boot(&Snapshot::default(), None);
    let events = Events::default();
    let ops = Ops::new(&events);
    let old_t;
    {
        let mut d = m.open_with(m.fabric.open(FAB_DB), m.fabric.open(FAB_WAL), Some(ops.clone())).expect("open a new database");
        let s = &mut Session::new();
        small_t(&mut d.o, s);
        must(&mut d.o, s, "CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));");
        must(&mut d.o, s, &note_sql(0));
        old_t = d.o.catalog.table_pages("t").expect("t's pages");
        ops.fail_next_remove.store(true, Ordering::SeqCst);
        must(&mut d.o, s, "DROP TABLE t;");
        assert!(
            events.lock().unwrap().iter().any(|e| matches!(e, Ev::Fs("remove", _, Err(_)))),
            "premise: the intent's removal never failed"
        );
        assert!(intent_file(&m).exists(), "premise: the intent file is gone although its removal failed");
        let bits = allocated(&d.o.bp).expect("read the bitmap");
        assert!(old_t.iter().all(|p| !bits.contains(p)), "premise: the DROP's frees did not run before the removal");
        let before = pages_of(&d.o, "notes").expect("walk notes");
        must(&mut d.o, s, &note_sql(1));
        must(&mut d.o, s, &note_sql(2));
        let taken: Vec<u32> = pages_of(&d.o, "notes")
            .expect("walk notes again")
            .difference(&before)
            .copied()
            .filter(|p| old_t.contains(p))
            .collect();
        assert!(taken.is_empty(), "notes took page(s) {taken:?} of the dropped t while t's intent file still named them");
        assert!(intent_file(&m).exists(), "premise: something removed the intent before the retry");
        d.o.txn.checkpoint().expect("the checkpoint that retries the removal");
        assert!(!intent_file(&m).exists(), "premise: the retry did not remove the intent");
        assert_eq!(d.o.bp.disk_manager.quarantined(), Vec::<u32>::new(), "the retry removed the intent and kept its pages quarantined");
        structure(&d.o).unwrap_or_else(|e| panic!("after the retry: {e}"));
        check_rows(&mut d.o, "notes", &[0, 1, 2]).unwrap_or_else(|e| panic!("notes after the retry: {e}"));
        // The crash.
    }
    let mut want = Want::default();
    want.present.insert("notes".to_string(), vec![0, 1, 2]);
    want.either = Some(("t".to_string(), vec![1, 2, 3], old_t.into_iter().collect()));
    let m2 = Machine::boot(&m.snapshot(), None);
    let mut d = m2.open().expect("the open after the retried removal");
    assert!(d.o.catalog.get_table("t").is_none(), "the dropped t came back");
    oracle(&mut d.o, &want).unwrap_or_else(|e| panic!("after the open: {e}"));
}

/// **A rebuilding open makes its trigger durable before its first free** (the design review's caveat
/// 1; D229 review 1's R7, PREREG amendment 9). The reset frees index pages the durable catalog still
/// names. That is safe only while every later open is certain to rebuild, so the log (whose records
/// are the trigger) and the stale-indexes marker with its directory must be synced first: a marker
/// or a log tail that an earlier process never synced could otherwise vanish in a power cut after the
/// frees, and the next open would walk freed trees. One event list, shared by the page file, the log
/// and the file-ops double, must show those three syncs before the open's first cleared bit.
///
/// Mutant-only red: M18 skips the marker's syncs; M19 skips the log's.
#[test]
fn a_rebuilding_open_makes_its_trigger_durable_before_its_first_free() {
    let mut owed = owed_rebuild();
    let marker_name = format!("{DB_NAME}.wal.stale-indexes");
    owed.side.insert(marker_name.clone(), b"planted: an earlier process's index undo failed\n".to_vec());
    assert_eq!(
        &owed.images[FAB_DB][0..4],
        &[0u8; 4][..],
        "premise: the bitmap chain runs past page 0, and only page 0's bits are watched"
    );
    let m = Machine::boot(&owed, None);
    let events = Events::default();
    let d = m
        .open_with(
            Watched::new(m.fabric.open(FAB_DB), "db", &events),
            Watched::new(m.fabric.open(FAB_WAL), "wal", &events),
            Some(Ops::new(&events)),
        )
        .expect("the rebuilding open");
    assert!(d.o.recovered, "premise: the open replayed nothing, so no reset ran");
    let ev = events.lock().unwrap().clone();
    let first_free = ev
        .iter()
        .position(|e| matches!(e, Ev::Cleared(_)))
        .unwrap_or_else(|| panic!("premise: the open freed no page, so there is no order to check: {ev:?}"));
    let before = &ev[..first_free];
    let marker = m.dir.path().join(&marker_name);
    assert!(
        before.contains(&Ev::Synced("wal")),
        "the open freed a page before it synced the log, whose records make every later open rebuild: {before:?}"
    );
    let synced = before
        .iter()
        .position(|e| *e == Ev::Fs("sync_file", marker.clone(), Ok(())))
        .unwrap_or_else(|| panic!("the open freed a page before it synced the stale-indexes marker: {before:?}"));
    assert!(
        before[synced + 1..].iter().any(|e| matches!(e, Ev::Fs("sync_dir", dir, Ok(())) if dir.as_path() == m.dir.path())),
        "the open freed a page before it synced the marker's directory: {before:?}"
    );
}

/// **A page the log relinks after its bit was lost has its bit set again** (the design review's
/// caveat 2; D229 review 1's R7, PREREG amendment 9). Under `SyncOnly` a crash loses every write no
/// sync covered. `notes` takes a page for its second row: the bitmap write that allocated it is lost,
/// while the row's log record is durable at its commit. The open's redo and directory repair put the
/// page back into `notes`' heap with its bit clear; left so, the rebuild could allocate it as a tree
/// node, under a live row.
///
/// The page is one the file already holds as zeros (taken, zero-written, freed and synced first), so
/// redo starts it from an empty page. A page holding a dropped table's stale bytes would be redone onto
/// them, which is outside this test (recorded in the lane as a finding). This doc used to say a page
/// past the durable end of the file would fail redo's read: false, since `recover` restores every page
/// the log names whose read fails as an empty page before redo (ledger D267, retracted; PREREG
/// amendment 16).
///
/// Mutant-only red: M15 skips the reset's `set_allocated`.
#[test]
fn a_page_the_log_relinks_after_its_bit_was_lost_has_its_bit_set_again() {
    let m = Machine::boot_as(&Snapshot::default(), None, Durability::SyncOnly);
    let lost;
    {
        let mut d = m.open().expect("open a new database");
        let s = &mut Session::new();
        must(&mut d.o, s, "CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));");
        must(&mut d.o, s, &note_sql(0));
        let spare = d.o.bp.new_page().expect("take a page");
        d.o.bp.delete_page(spare).expect("give it back");
        d.o.txn.checkpoint().expect("the checkpoint that makes everything so far durable");
        let before = pages_of(&d.o, "notes").expect("walk notes");
        must(&mut d.o, s, &note_sql(1));
        let new: Vec<u32> = pages_of(&d.o, "notes").expect("walk notes again").difference(&before).copied().collect();
        assert_eq!(new, vec![spare], "premise: the second note did not take exactly the page given back");
        lost = spare;
        // The crash: nothing has synced the page file since the checkpoint.
    }
    let crash = m.snapshot();
    let db = &crash.images[FAB_DB];
    assert!(
        db[(lost / 8) as usize + 4] & (1 << (lost % 8)) == 0,
        "premise: page {lost}'s allocation bit survived the crash, so there is no bit to set again"
    );
    let at = lost as usize * PAGE_SIZE;
    assert!(
        db.len() >= at + PAGE_SIZE && db[at..at + PAGE_SIZE].iter().all(|b| *b == 0),
        "premise: page {lost} is not a zero page inside the durable file, so redo does not start it empty"
    );
    let m2 = Machine::boot(&crash, None);
    let mut d = m2.open().expect("the open after the lost bit");
    assert!(d.o.recovered, "premise: the open replayed nothing, so its insert was not durable");
    assert_eq!(
        walk(&d.o).expect("walk").owner.get(&lost).map(String::as_str),
        Some("notes's heap"),
        "premise: recovery did not put page {lost} back into notes' heap"
    );
    assert!(allocated(&d.o.bp).expect("read the bitmap").contains(&lost), "page {lost}, which notes' heap lists after the open, is free");
    let mut want = Want::default();
    want.present.insert("notes".to_string(), vec![0, 1]);
    oracle(&mut d.o, &want).unwrap_or_else(|e| panic!("after the open that relinked page {lost}: {e}"));
}

/// **A DROP's intent is written before its `DropTable` record** (the D229 merge review's §4.1;
/// PREREG amendment 10). The log's flush of the record fails at its sync, after its bytes reached the
/// file: D250 poisons the log and the DROP reports failure, while the log on disk calls `t` dropped.
/// The intent must exist by then. Otherwise the next open completes the DROP with nothing naming its
/// pages, and they leak.
///
/// Mutant-only red: M20 records the intent after the record.
#[test]
fn a_drop_whose_record_flush_failed_left_its_intent_for_the_open_that_completes_it() {
    let m = Machine::boot(&Snapshot::default(), None);
    let events = Events::default();
    let wal = Watched::new(m.fabric.open(FAB_WAL), "wal", &events);
    let pages;
    {
        let mut d = m.open_with(m.fabric.open(FAB_DB), wal.clone(), None).expect("open a new database");
        let s = &mut Session::new();
        small_t(&mut d.o, s);
        must(&mut d.o, s, "CREATE TABLE u (id INTEGER NOT NULL, v INTEGER);");
        must(&mut d.o, s, "INSERT INTO u VALUES (0, 0);");
        pages = pages_of(&d.o, "t").expect("walk t");
        wal.fail_next_sync_data.store(true, Ordering::SeqCst);
        assert!(sql(&mut d.o, s, "DROP TABLE t;").is_err(), "premise: the DROP succeeded, so its record's flush did not fail");
        let why = d.o.wal.poisoned().unwrap_or_default();
        assert!(why.contains("could not make its record durable"), "premise: the log was not poisoned by the DROP's record flush: {why:?}");
        assert!(intent_file(&m).exists(), "the DROP's record reached the log's file, and no intent had been written before it");
        // The crash.
    }
    let m2 = Machine::boot(&m.snapshot(), None);
    {
        let mut d = m2.open().expect("the open after the failed DROP");
        assert_eq!(
            d.o.completed_drops,
            vec!["t".to_string()],
            "premise: the DropTable record did not reach the log's file, so the open had no DROP to complete"
        );
        let mut want = Want::default();
        want.present.insert("u".to_string(), vec![0]);
        want.either = Some(("t".to_string(), vec![1, 2, 3], pages));
        oracle(&mut d.o, &want).unwrap_or_else(|e| panic!("after the open that completed the DROP: {e}"));
    }
    assert!(!intent_file(&m2).exists(), "the open left the completed DROP's intent in place");
}

/// **A DROP frees its pages whether or not its checkpoint truncates** (the D229 merge review's §4.2,
/// FREEPOSm; PREREG amendment 10). A WAL pin cancels the truncation. The frees must still run in the
/// same process, right after the checkpoint's sync, and a table created next must be able to take the
/// pages without anything aliasing. D250's recovery skip is what makes the kept records harmless: the
/// crash with the pin still held, and the open after it, check that half.
///
/// Mutant-only red: M21 frees only on the truncated path.
#[test]
fn a_drop_under_a_wal_pin_frees_its_pages_before_any_truncation() {
    let m = Machine::boot(&Snapshot::default(), None);
    let old_t;
    {
        let mut d = m.open().expect("open a new database");
        let s = &mut Session::new();
        small_t(&mut d.o, s);
        old_t = d.o.catalog.table_pages("t").expect("t's pages");
        let base = d.o.wal.base_lsn.load(Ordering::SeqCst);
        let _pin = d.o.wal.pin(base).expect("pin the log at its base");
        must(&mut d.o, s, "DROP TABLE t;");
        assert_eq!(d.o.wal.base_lsn.load(Ordering::SeqCst), base, "premise: the pin did not keep the log, so the DROP's checkpoint truncated");
        let bits = allocated(&d.o.bp).expect("read the bitmap");
        let kept: Vec<u32> = old_t.iter().copied().filter(|p| bits.contains(p)).collect();
        assert!(kept.is_empty(), "a DROP whose truncation a pin cancelled left page(s) {kept:?} allocated");
        assert!(!intent_file(&m).exists(), "the DROP's intent is still on disk after its frees");
        assert_eq!(d.o.bp.disk_manager.quarantined(), Vec::<u32>::new(), "pages stay quarantined after the frees");
        must(&mut d.o, s, "CREATE TABLE u (id INTEGER NOT NULL, v INTEGER);");
        for id in 0..3 {
            must(&mut d.o, s, &format!("INSERT INTO u VALUES ({id}, {id});"));
        }
        let u_pages = pages_of(&d.o, "u").expect("walk u");
        assert!(old_t.iter().any(|p| u_pages.contains(p)), "premise: u took none of the old t's pages, so nothing could alias them");
        structure(&d.o).unwrap_or_else(|e| panic!("after u took the freed pages: {e}"));
        check_rows(&mut d.o, "u", &[0, 1, 2]).unwrap_or_else(|e| panic!("u, on the freed pages: {e}"));
        // The crash, with the pin still held.
    }
    let mut want = Want::default();
    want.present.insert("u".to_string(), vec![0, 1, 2]);
    want.either = Some(("t".to_string(), vec![1, 2, 3], old_t.into_iter().collect()));
    let m2 = Machine::boot(&m.snapshot(), None);
    let mut d = m2.open().expect("the open after a DROP under a pin");
    assert!(d.o.catalog.get_table("t").is_none(), "the dropped t came back");
    oracle(&mut d.o, &want).unwrap_or_else(|e| panic!("after the open: {e}"));
}

/// **A DROP of a table whose pages a pending intent already names is refused before anything
/// happens** (the D229 merge review's §4.3; PREREG amendment 10). No DROP in this build leaves such
/// an intent: its pages are quarantined until it is gone, and a failed DROP poisons the log. So one is
/// PLANTED, as a damaged or stale intent file would leave it, and adopted. Carrying out both intents
/// would free the pages twice, or free them under a live owner.
///
/// Mutant-only red: M22 never refuses.
#[test]
fn a_drop_of_a_table_a_pending_intent_already_names_is_refused_before_anything_happens() {
    let m = Machine::boot(&Snapshot::default(), None);
    let mut d = m.open().expect("open a new database");
    let s = &mut Session::new();
    small_t(&mut d.o, s);
    let root = d.o.catalog.get_table("t").expect("t").first_directory_page_id;
    let pages = d.o.catalog.table_pages("t").expect("t's pages");
    free_intent::store(&OsFileOps, &d.o.wal.path, &[FreeIntent { table: "t".into(), dir_root: root, pages: pages.clone() }])
        .expect("plant an intent naming t's pages");
    assert_eq!(d.o.txn.adopt_free_intents().expect("adopt the planted intent"), 1, "premise: the planted intent was not adopted");
    let planted = std::fs::read(intent_file(&m)).expect("read the planted intent");
    let why = match sql(&mut d.o, s, "DROP TABLE t;") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a DROP ran over a table whose pages a pending intent names"),
    };
    assert!(why.contains("already named by a pending DROP intent"), "the DROP failed, but not by the refusal: {why}");
    assert_eq!(d.o.wal.poisoned(), None, "the refusal poisoned the log");
    assert_eq!(std::fs::read(intent_file(&m)).expect("read the intent file"), planted, "the refused DROP rewrote the intent file");
    let bits = allocated(&d.o.bp).expect("read the bitmap");
    let freed: Vec<u32> = pages.iter().copied().filter(|p| !bits.contains(p)).collect();
    assert!(freed.is_empty(), "the refused DROP freed page(s) {freed:?}");
    check_rows(&mut d.o, "t", &[1, 2, 3]).unwrap_or_else(|e| panic!("t after the refused DROP: {e}"));
}

/// Every page a heap record in the durable log names (a CLR by the record it redoes), read from the
/// crash image `s` without opening the database.
fn logged_pages(s: &Snapshot) -> BTreeSet<u32> {
    let m = Machine::boot(s, None);
    let wal = WalManager::with_storage(m.fabric.open(FAB_WAL), m.dir.path().join(format!("{DB_NAME}.wal")))
        .expect("read the crash image's log");
    let mut out = BTreeSet::new();
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    while lsn < end {
        let (rec, next) = wal.read_record(lsn).expect("a record of the crash image's log");
        let kind = match &rec.kind {
            RecKind::Clr { redo, .. } => redo.as_ref(),
            other => other,
        };
        match kind {
            RecKind::HeapInsert { page_id, .. }
            | RecKind::HeapDelete { page_id, .. }
            | RecKind::HeapUpdate { page_id, .. }
            | RecKind::HeapRelease { page_id, .. } => {
                out.insert(*page_id);
            }
            _ => {}
        }
        lsn = next;
    }
    out
}

/// **A page its heap lists before its own image reached the disk opens as an empty page** (D256
/// review 1's R4, added to D229's exit by the lead; PREREG amendment 12). `add_empty_page` writes a
/// zero page, then lists it. Directory entries are not logged and a directory page carries no LSN, so
/// the directory can reach the disk (evicted, or first in `flush_all`'s ascending order) while the
/// page's own image does not, and when no log record names the page, redo does not initialise it.
/// Every scan of the heap then reads zeros as a heap page and `Page::deserialize` panics, and the
/// open's rebuild scans every primary heap. Here `notes`' directory is flushed alone, as an eviction
/// writes it, while the transaction whose INSERT took the page is still open.
///
/// RED at `60481bf` (INFERRED): the first open panics. Mutant-only red after the fix: M24.
#[test]
fn a_page_its_heap_lists_before_its_own_image_reached_the_disk_opens_as_an_empty_page() {
    let m = Machine::boot(fixture(), None);
    let (listed, dir_id);
    {
        let mut d = m.open().expect("open the fixture");
        let s = &mut Session::new();
        must(&mut d.o, s, "CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));");
        must(&mut d.o, s, &note_sql(0));
        let before = pages_of(&d.o, "notes").expect("walk notes");
        must(&mut d.o, s, "BEGIN;");
        must(&mut d.o, s, &note_sql(1));
        let new: Vec<u32> = pages_of(&d.o, "notes").expect("walk notes again").difference(&before).copied().collect();
        assert_eq!(new.len(), 1, "premise: the second note did not take exactly one new page: {new:?}");
        listed = new[0];
        dir_id = d.o.catalog.get_table("notes").expect("notes").first_directory_page_id;
        d.o.bp.flush_page(dir_id).expect("write notes' directory, as an eviction would");
        // The crash, with the INSERT's transaction still open.
    }
    let crash = m.snapshot();
    let db = &crash.images[FAB_DB];
    let at = dir_id as usize * PAGE_SIZE;
    let dir = PageDirectory::deserialize(db[at..at + PAGE_SIZE].try_into().unwrap());
    assert!(
        dir.entries.iter().any(|e| e.page_id == listed),
        "premise: notes' directory on disk does not list page {listed}, so no scan reaches it"
    );
    assert!(
        !logged_pages(&crash).contains(&listed),
        "premise: a durable log record names page {listed}, so redo would initialise it"
    );
    let mut want = fixture_want();
    want.present.insert("notes".to_string(), vec![0]);
    if let Err(e) = two_good_opens(&crash, &want) {
        panic!("a heap page its directory lists on disk before the page's own image: {e}");
    }
}

/// **`table_pages` of a FILLED table is what the oracle walks, and a heap-side alias is refused** (D229
/// review 2's R6; PREREG amendment 16). The catalog unit test uses an empty table, one page per
/// structure, so its equality cannot see a walk that skips directory entries, chain links, child
/// pointers or the leaf chain, and it plants only a tree-side alias. Here: the fixture's `t`, 400 rows,
/// every tree with internal nodes, and a time-travel heap given pages by an UPDATE. Then two plants, each
/// on a fresh boot of the same image:
/// 1. `t`'s time-travel ROOT listed in `t`'s heap directory: the time-travel walk then reaches a
///    directory page already named (`HeapFileManager::collect_pages`' first refusal; M25 disables it);
/// 2. one of `t`'s heap data pages listed a second time (its second refusal; M26 disables it).
///
/// Blind spot, stated: an alias ACROSS tables is not refused. `table_pages` walks one table, and no page
/// records its owner, so a check would need every other table walked.
#[test]
fn table_pages_of_a_filled_table_is_what_the_oracle_walks_and_refuses_a_heap_side_alias() {
    let m = Machine::boot(fixture(), None);
    {
        let mut d = m.open().expect("open the fixture");
        must(&mut d.o, &mut Session::new(), "UPDATE t SET w = 'changed' WHERE id < 40;");
        d.o.txn.checkpoint().expect("the checkpoint after the UPDATE");
        let r = walk(&d.o).expect("walk");
        assert!(r.twice.is_empty(), "premise: the oracle's walk reaches a page twice: {:?}", &r.twice[..r.twice.len().min(4)]);
        assert!(
            r.owner.values().any(|by| by == "t's time-travel heap"),
            "premise: t's time-travel heap lists no page after the UPDATE, so the equality below does not cover it"
        );
        let walked = pages_of(&d.o, "t").expect("walk t");
        let named: BTreeSet<u32> = d.o.catalog.table_pages("t").expect("t's pages").into_iter().collect();
        assert_eq!(
            named,
            walked,
            "table_pages of a filled table is not what the oracle walks: only table_pages names {:?}, only the walk {:?}",
            named.difference(&walked).take(5).collect::<Vec<_>>(),
            walked.difference(&named).take(5).collect::<Vec<_>>()
        );
    }
    let image = m.snapshot();

    let m1 = Machine::boot(&image, None);
    {
        let d = m1.open().expect("open the image for plant 1");
        let e = d.o.catalog.get_table("t").expect("t").clone();
        HeapFileManager::open(e.first_directory_page_id, d.o.bp.clone())
            .add_to_directory(e.time_travel_root, 0)
            .expect("plant t's time-travel root in t's heap directory");
        let refused = d.o.catalog.table_pages("t");
        assert!(
            refused.is_err(),
            "table_pages collected t's time-travel root once, although t's heap directory lists it as a data page too"
        );
    }

    let m2 = Machine::boot(&image, None);
    {
        let d = m2.open().expect("open the image for plant 2");
        let e = d.o.catalog.get_table("t").expect("t").clone();
        let data = walk(&d.o)
            .expect("walk")
            .owner
            .into_iter()
            .find(|(_, by)| by == "t's heap")
            .map(|(p, _)| p)
            .expect("premise: t's heap lists no data page");
        HeapFileManager::open(e.first_directory_page_id, d.o.bp.clone())
            .add_to_directory(data, 0)
            .expect("list one of t's data pages a second time");
        let refused = d.o.catalog.table_pages("t");
        assert!(refused.is_err(), "table_pages collected page {data} once, although t's heap directory lists it twice");
    }
}
