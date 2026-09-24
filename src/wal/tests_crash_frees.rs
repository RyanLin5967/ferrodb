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
//!   written through the real filesystem. No fault is ever aimed at one of their operations, so
//!   each is durable the moment it returns.
//! - `Durability::WriteThrough` only: kill -9, where every write that returned is kept. A power loss
//!   that keeps an arbitrary subset of unsynced writes is not modelled here.
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
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};

use super::{open_recovered, OpenedDatabase};
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::IndexTree;
use crate::catalog::catalog_page::CatalogPage;
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::execution::executor::{run, Outcome};
use crate::execution::session::Session;
use crate::parser::{parser::Parser, scanner::Scanner};
use crate::storage::db_lock::DbLock;
use crate::storage::disk_manager::PAGE_SIZE;
use crate::storage::heap_file_manager::{HeapFileManager, RecordId};
use crate::storage::index::BPlusTreeManager;
use crate::storage::index_page::BPlusTreePage;
use crate::storage::page_directory::PageDirectory;
use crate::storage::sim::{Durability, FaultPlan, OpKind, SimFabric, WriteShape};
use crate::storage::storage::Storage;

thread_local! {
    /// **Test-only: the storage the next [`open_recovered`] on this thread opens instead of its
    /// files**, as `(page file, log, whether the page file existed)`. The open takes it, once.
    /// Thread-local, so a test that hands storage over cannot redirect an open in a test beside it.
    pub(crate) static TEST_FILES: RefCell<Option<(Arc<dyn Storage>, Arc<dyn Storage>, bool)>> = const { RefCell::new(None) };
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
        let dir = tempfile::tempdir().expect("a directory for the machine");
        for (name, bytes) in &s.side {
            std::fs::write(dir.path().join(name), bytes).expect("restore a file beside the log");
        }
        let existed = s.images.get(FAB_DB).is_some_and(|b| !b.is_empty());
        let fabric = SimFabric::from_images(s.images.clone(), plan, Durability::WriteThrough);
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
        let db = self.dir.path().join(DB_NAME);
        let lock = DbLock::acquire(&db)?;
        let wal_file: Arc<dyn Storage> = self.fabric.open(FAB_WAL);
        let files = (db_file, wal_file, self.existed);
        TEST_FILES.with(|f| *f.borrow_mut() = Some(files));
        let opened = open_recovered(&db, &lock);
        // Taken by the open unless it refused before reaching its files.
        TEST_FILES.with(|f| f.borrow_mut().take());
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

/// **F2 with nothing of `t` in the log at the DROP.** D229 frees a DROP's pages once no retained
/// record names them, so with `t`'s records in the log the frees wait for the truncation, and a
/// free moved before the DROP's checkpoint (mutant M3) would still wait. With none in the log it
/// would run at once, before the unlink is durable. Added with the fix; its red is mutant-only.
#[test]
fn a_crash_at_every_operation_of_drop_table_with_nothing_of_it_in_the_log_leaves_it_whole_or_gone() {
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
