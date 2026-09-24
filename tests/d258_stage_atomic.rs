//! D258 — a branch write that `stage_all` refuses must leave nothing staged.
//!
//! # The defect
//!
//! `AgentRuntime::stage_all` decided the capability envelope and the escrow, then charged the
//! row-write budget, spent the escrow, rewrote the workspace's rows and frame — and only THEN
//! reached three steps that could still refuse:
//!
//! * `EffectLog::append`, which on `DurableEffectLog` refuses a string over its u16 length prefix, a
//!   guard nested past 256, and a record too large to replicate, and fails on I/O;
//! * `put_row`, whose copy-on-write tree refuses an entry over 1015 bytes — an encoded row value of
//!   992 bytes or more — and fails when its allocator is starved;
//! * the same tree's I/O.
//!
//! All under a comment reading "past this point nothing may fail", and no caller undid anything. So
//! the client got an error while `MERGE` — which reads the workspace — published the row, a retry
//! applied it a second time, the refused statement kept the budget it spent, and on the durable log
//! the refused op stayed in `ws.frame`, so every later statement on the branch re-encoded it and
//! was refused too.
//!
//! # What each test pins
//!
//! The PREREG is `frontier/lane_d258_stage_atomic.md` §2 in artie-research, written before this
//! file. T2 is the control that keeps T1 honest: a row one byte under the cap still stages, mirrors
//! and merges, so the refusal is about the boundary and not about long strings.
//!
//! Three fault injectors live here and nowhere in `src/`: [`FaultyStore`] fails the k-th
//! `PageStore::cow_page`, which is how a tree write fails AFTER every check has passed (allocator
//! starvation and I/O are not knowable in advance); [`FlakyFile`] fails the effect log's next
//! write; and [`FlakyRoots`] lets the catalog's next `set_root` land and then report failure, which
//! is a write that reached the tree while returning an error (T9, added by the PREREG's amendment
//! 2). Each is aimed at the one call it breaks and disarms itself after firing, so the restore that
//! follows a failure runs against a store that works.

use std::fs::{File, OpenOptions};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use ferrodb::agent_sql::runtime::{row_id_of, table_id, AgentRuntime, ExecCtx};
use ferrodb::agent_sql::AgentOutput;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::record::{BranchRecord, CoreRecord};
use ferrodb::branch::types::{ArenaId, BranchId, BranchState, Epoch, LeaseDeadline, PageId};
use ferrodb::branch::{BranchCatalog, CapabilityEnvelope, ColumnCapability, Verb};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{CowPage, PageHandle, PageStore};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::storage::Storage;
use ferrodb::tel::ids::ColId;
use ferrodb::tel::{DurableEffectLog, EffectLog, MemEffectLog};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Well above anything the catalog, heap and WAL reach, as in every page-backed fixture.
const ARENA_BASE: u32 = 1024;

/// `ledger (id, qty, note)`: the column the escrow and the envelope govern.
const QTY: ColId = ColId(1);

/// A note long enough to push a `ledger` row past the tree's entry cap: 2 (cell count) + 5 (id) +
/// 5 (qty) + 3 + 1000 (note) = 1015 bytes of value, an entry of 1039 against a cap of 1015.
const OVERSIZED_NOTE: usize = 1000;

// ---- fault injection ------------------------------------------------------------------------

/// The switches every injector reads. One per database, shared by the store, the log file and
/// the branch catalog.
#[derive(Default)]
struct Faults {
    /// `cow_page` calls seen since the last [`Faults::arm_cow`].
    cow_calls: AtomicUsize,
    /// The 1-based ordinal of the `cow_page` call to fail, once. Zero never matches.
    fail_cow_at: AtomicUsize,
    /// A second such ordinal, for a statement whose restore has to fail too (T10, T11).
    fail_cow_also: AtomicUsize,
    /// Fail the effect log's next `pwrite`, once.
    fail_log_write: AtomicBool,
    /// Let the catalog's next `set_root` land, then report it failed, once.
    fail_set_root: AtomicBool,
}

impl Faults {
    /// Start counting `cow_page` calls from zero, and fail call `k` (0: count only).
    fn arm_cow(&self, k: usize) {
        self.arm_cow_twice(k, 0);
    }

    /// As [`Faults::arm_cow`], failing calls `k` and `also`, each once.
    fn arm_cow_twice(&self, k: usize, also: usize) {
        self.cow_calls.store(0, Ordering::SeqCst);
        self.fail_cow_at.store(k, Ordering::SeqCst);
        self.fail_cow_also.store(also, Ordering::SeqCst);
    }
}

/// The real arena store, with `cow_page` counted and one call of it failed on demand.
///
/// `cow_page` is the call every tree write makes for every page it touches (`CowTree::shadow` is
/// the only path to it), so failing it fails a `put_row` exactly as a starved allocator or a dead
/// device would — after `stage_all` has decided the statement is admissible.
struct FaultyStore {
    inner: Arc<dyn PageStore>,
    faults: Arc<Faults>,
}

impl PageStore for FaultyStore {
    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        self.inner.alloc_in_arena(arena, page_type, birth_epoch)
    }
    fn read_page(&self, page_id: PageId) -> Result<PageHandle, FerroError> {
        self.inner.read_page(page_id)
    }
    fn alloc_for(
        &self,
        branch: BranchId,
        page_type: PageType,
        epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        self.inner.alloc_for(branch, page_type, epoch)
    }
    fn cow_page(
        &self,
        page_id: PageId,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<CowPage, FerroError> {
        let n = self.faults.cow_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.faults.fail_cow_at.load(Ordering::SeqCst) {
            self.faults.fail_cow_at.store(0, Ordering::SeqCst);
            return Err(FerroError::Io(format!("d258: injected failure of cow_page call {n}")));
        }
        if n == self.faults.fail_cow_also.load(Ordering::SeqCst) {
            self.faults.fail_cow_also.store(0, Ordering::SeqCst);
            return Err(FerroError::Io(format!("d258: injected failure of cow_page call {n}")));
        }
        self.inner.cow_page(page_id, branch, epoch)
    }
    fn free_page(&self, page_id: PageId, free_epoch: Epoch) -> Result<(), FerroError> {
        self.inner.free_page(page_id, free_epoch)
    }
    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        self.inner.alloc_arena(branch)
    }
    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        self.inner.arena_for(branch)
    }
    fn free_arena(&self, arena: ArenaId) -> Result<u32, FerroError> {
        self.inner.free_arena(arena)
    }
    fn live_page_count(&self) -> Result<u32, FerroError> {
        self.inner.live_page_count()
    }
    fn flush(&self) -> Result<(), FerroError> {
        self.inner.flush()
    }
}

/// The effect log's file, with its next write failed on demand. `DurableEffectLog::with_storage` is
/// the seam its own tests aim faults through.
struct FlakyFile {
    inner: File,
    faults: Arc<Faults>,
}

impl Storage for FlakyFile {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        if self.faults.fail_log_write.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("d258: injected effect-log write failure"));
        }
        Storage::pwrite(&self.inner, buf, offset)
    }
    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        Storage::pread(&self.inner, buf, offset)
    }
    fn sync_all(&self) -> io::Result<()> {
        Storage::sync_all(&self.inner)
    }
    fn sync_data(&self) -> io::Result<()> {
        Storage::sync_data(&self.inner)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        Storage::set_len(&self.inner, len)
    }
    fn len(&self) -> io::Result<u64> {
        Storage::len(&self.inner)
    }
}

/// The real branch catalog, with one `set_root` made to land and then report failure.
///
/// That is what both production catalogs do when the disk refuses the durability step:
/// `LogBranchCatalog::set_root` moves the record's root in memory and then fails its fsync'd
/// append, and `TableBranchCatalog::set_root` writes the record and then fails `durable`. So the
/// branch's root has MOVED — its tree holds the row — while `put_row` returns an error. Delegating
/// first and failing afterwards reproduces exactly that. Every other method, the defaulted ones
/// included, goes straight to the inner catalog, so nothing changes until the switch is armed.
struct FlakyRoots {
    inner: Arc<dyn BranchCatalog>,
    faults: Arc<Faults>,
}

impl BranchCatalog for FlakyRoots {
    fn set_root(&self, branch: BranchId, root: PageId) -> Result<(), FerroError> {
        let landed = self.inner.set_root(branch, root);
        if self.faults.fail_set_root.swap(false, Ordering::SeqCst) {
            landed?;
            return Err(FerroError::Io(format!(
                "d258: injected: the root of {branch} moved to page {root}, then its durability \
                 step failed"
            )));
        }
        landed
    }
    fn next_epoch(&self) -> Epoch {
        self.inner.next_epoch()
    }
    fn current_epoch(&self) -> Epoch {
        self.inner.current_epoch()
    }
    fn fork(&self, parent: BranchId, lease: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        self.inner.fork(parent, lease)
    }
    fn fork_staged(
        &self,
        parent: BranchId,
        lease: LeaseDeadline,
    ) -> Result<(BranchRecord, Option<u64>), FerroError> {
        self.inner.fork_staged(parent, lease)
    }
    fn await_fork_durable(&self, seq: Option<u64>) -> Result<(), FerroError> {
        self.inner.await_fork_durable(seq)
    }
    fn get(&self, branch: BranchId) -> Result<BranchRecord, FerroError> {
        self.inner.get(branch)
    }
    fn reparent(
        &self,
        branch: BranchId,
        parent: BranchId,
        fork_epoch: Epoch,
        root: PageId,
    ) -> Result<BranchRecord, FerroError> {
        self.inner.reparent(branch, parent, fork_epoch, root)
    }
    fn restrict_envelope(
        &self,
        branch: BranchId,
        envelope: CapabilityEnvelope,
    ) -> Result<(), FerroError> {
        self.inner.restrict_envelope(branch, envelope)
    }
    fn set_state(
        &self,
        branch: BranchId,
        expect: BranchState,
        to: BranchState,
    ) -> Result<(), FerroError> {
        self.inner.set_state(branch, expect, to)
    }
    fn expired_before(&self, now_millis: u64) -> Result<Vec<CoreRecord>, FerroError> {
        self.inner.expired_before(now_millis)
    }
    fn in_state(&self, state: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
        self.inner.in_state(state)
    }
    fn scan(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        self.inner.scan()
    }
    fn scan_ids(
        &self,
        lo: u64,
        hi: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        self.inner.scan_ids(lo, hi)
    }
    fn max_live_child(&self, parent_id: u64) -> Result<Option<Epoch>, FerroError> {
        self.inner.max_live_child(parent_id)
    }
    fn live_child_in_epoch_range(
        &self,
        parent_id: u64,
        lo: Epoch,
        hi: Epoch,
    ) -> Result<bool, FerroError> {
        self.inner.live_child_in_epoch_range(parent_id, lo, hi)
    }
    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        self.inner.has_live_children(parent_id)
    }
    fn live_count(&self) -> usize {
        self.inner.live_count()
    }
    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        self.inner.get_raw(id)
    }
    fn release_id(&self, id: u64) {
        self.inner.release_id(id)
    }
    fn attach_child(
        &self,
        parent_id: u64,
        fork_epoch: Epoch,
        child_id: u64,
    ) -> Result<(), FerroError> {
        self.inner.attach_child(parent_id, fork_epoch, child_id)
    }
    fn detach_child(&self, parent_id: u64, fork_epoch: Epoch) -> Result<bool, FerroError> {
        self.inner.detach_child(parent_id, fork_epoch)
    }
    fn add_arena(&self, branch: BranchId, arena: ArenaId) -> Result<(), FerroError> {
        self.inner.add_arena(branch, arena)
    }
    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        self.inner.renew_lease(branch, lease)
    }
    fn envelope_of(&self, branch: BranchId) -> Result<Option<CapabilityEnvelope>, FerroError> {
        self.inner.envelope_of(branch)
    }
    fn charge_row_writes(&self, branch: BranchId, n: u64) -> Result<(), FerroError> {
        self.inner.charge_row_writes(branch, n)
    }
}

// ---- the database ---------------------------------------------------------------------------

/// Which runtime a test runs against.
#[derive(Clone, Copy)]
enum Shape {
    /// Page-backed, in-memory effect log: `examples/pgserver.rs`'s wiring.
    PagedMem,
    /// Page-backed, durable effect log: the CLI's wiring (`cli.rs`).
    PagedDurable,
    /// No page store, durable effect log: the log is the only thing left that can refuse.
    MapDurable,
}

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    faults: Arc<Faults>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new(shape: Shape) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("d258.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d258.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);

        let faults = Arc::new(Faults::default());
        // Every consumer — the runtime and the page store — sees the catalog through the one
        // wrapper, so an armed `set_root` fault is the only difference from the real thing.
        let branches: Arc<dyn BranchCatalog> = Arc::new(FlakyRoots {
            inner: Arc::new(LogBranchCatalog::in_memory(1)),
            faults: Arc::clone(&faults),
        });
        let log: Arc<dyn EffectLog> = match shape {
            Shape::PagedMem => Arc::new(MemEffectLog::new()),
            Shape::PagedDurable | Shape::MapDurable => {
                let tel = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(dir.path().join("d258.tel"))
                    .unwrap();
                let file = FlakyFile { inner: tel, faults: Arc::clone(&faults) };
                Arc::new(DurableEffectLog::with_storage("d258.tel", Arc::new(file)).unwrap())
            }
        };
        let runtime = match shape {
            Shape::MapDurable => AgentRuntime::with_parts(branches, log),
            Shape::PagedMem | Shape::PagedDurable => {
                let arena =
                    ArenaPageStore::new(bp.clone(), Arc::clone(&branches), ARENA_BASE).unwrap();
                let store: Arc<dyn PageStore> = Arc::new(FaultyStore {
                    inner: Arc::new(arena) as Arc<dyn PageStore>,
                    faults: Arc::clone(&faults),
                });
                AgentRuntime::with_storage(branches, log, store).unwrap()
            }
        };
        Db { catalog, bp, txn, runtime: Arc::new(runtime), faults, _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{} failed: {e}", short(sql)))
    }

    /// The refusal text, or a panic naming the statement that was let through.
    fn refused(&mut self, sql: &str, s: &mut Session) -> String {
        match self.exec(sql, s) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("`{}` was accepted; it must be refused", short(sql)),
        }
    }

    fn rows(&mut self, sql: &str, s: &mut Session) -> Vec<Vec<Value>> {
        match self.ok(sql, s) {
            Outcome::Rows(rows) => rows,
            _ => panic!("`{}` did not return rows", short(sql)),
        }
    }

    /// Every `(id, qty)` of `table` that `s` can see, sorted by id.
    fn id_qty(&mut self, table: &str, s: &mut Session) -> Vec<(i32, i32)> {
        let mut out: Vec<(i32, i32)> = self
            .rows(&format!("SELECT id, qty FROM {table};"), s)
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                (Value::Integer(a), Value::Integer(b)) => (*a, *b),
                other => panic!("unexpected row: {other:?}"),
            })
            .collect();
        out.sort_unstable();
        out
    }

    /// Every id of `table` that `s` can see, sorted.
    fn ids(&mut self, table: &str, s: &mut Session) -> Vec<i32> {
        let mut out: Vec<i32> = self
            .rows(&format!("SELECT id FROM {table};"), s)
            .iter()
            .map(|r| match &r[0] {
                Value::Integer(a) => *a,
                other => panic!("unexpected id: {other:?}"),
            })
            .collect();
        out.sort_unstable();
        out
    }

    /// Rows the branch `s` is in would merge, as `DIFF` reports them.
    fn diff_len(&mut self, s: &mut Session) -> usize {
        match self.ok("DIFF;", s) {
            Outcome::Agent(AgentOutput::Diff(cs)) => cs.rows.len(),
            _ => panic!("DIFF did not return a changeset"),
        }
    }

    /// Open an agent session on `s` and return its branch.
    fn begin(&mut self, agent: &str, s: &mut Session) -> BranchId {
        self.ok(&format!("BEGIN AGENT SESSION AS '{agent}' RUN 'r_{agent}';"), s);
        s.agent.as_ref().expect("no agent session after BEGIN").branch
    }

    fn remaining(&self, branch: BranchId, table: &str, id: i32) -> Option<i64> {
        self.runtime.remaining_escrow(branch, table, row_id_of(&[Value::Integer(id)]), QTY)
    }

    fn row_writes(&self, branch: BranchId) -> u64 {
        self.runtime.envelope_of(branch).unwrap().expect("the branch lost its envelope").row_writes()
    }

    /// What the branch's own copy-on-write tree holds for one row.
    fn tree_row(&self, branch: BranchId, table: &str, id: i32) -> Option<Vec<Value>> {
        self.runtime.get_row(branch, table, rid(id)).unwrap()
    }

    /// `ledger` with two rows on trunk, both qty cells bounded with 20 of slack, and a session
    /// holding a claim of 5 on each. Returns the session and its branch.
    fn two_claimed_rows(&mut self) -> (Session, BranchId) {
        let mut setup = self.session();
        self.ok("CREATE TABLE ledger (id INTEGER NOT NULL, qty INTEGER, note VARCHAR(2000));", &mut setup);
        self.ok("INSERT INTO ledger VALUES (1, 20, 'a');", &mut setup);
        self.ok("INSERT INTO ledger VALUES (2, 20, 'b');", &mut setup);
        for id in [1, 2] {
            self.runtime.open_escrow("ledger", row_id_of(&[Value::Integer(id)]), QTY, 20).unwrap();
        }
        let mut s = self.session();
        let branch = self.begin("claimant", &mut s);
        for id in [1, 2] {
            self.runtime
                .claim_escrow(branch, "ledger", row_id_of(&[Value::Integer(id)]), QTY, 5)
                .unwrap();
        }
        (s, branch)
    }
}

fn rid(id: i32) -> u64 {
    row_id_of(&[Value::Integer(id)]).0
}

/// A statement for a panic message, without the kilobytes of padding some of them carry.
fn short(sql: &str) -> String {
    if sql.len() <= 120 { sql.to_string() } else { format!("{}… ({} bytes)", &sql[..120], sql.len()) }
}

// ---- T1, T2: the tree's entry cap ------------------------------------------------------------

/// **T1. A row the branch's tree cannot hold is refused, and MERGE publishes nothing from it.**
///
/// 2 (cell count) + 5 (INTEGER) + 3 + 982 (VARCHAR) = 992 bytes of value, a 1016-byte entry against
/// the tree's 1015. The heap takes it without complaint, which is why the phantom was publishable.
#[test]
fn an_oversized_row_is_refused_and_merge_publishes_nothing() {
    let mut db = Db::new(Shape::PagedMem);
    let mut setup = db.session();
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, body VARCHAR(2000));", &mut setup);
    db.ok("INSERT INTO notes VALUES (1, 'seed');", &mut setup);

    let mut a = db.session();
    let branch = db.begin("t1", &mut a);
    let err = db.refused(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(982)), &mut a);
    assert!(err.contains("1015"), "refused, but not by the entry cap: {err}");
    assert_eq!(db.tree_row(branch, "notes", 2), None, "the branch's tree holds the refused row");

    db.ok("MERGE;", &mut a);
    let mut trunk = db.session();
    assert_eq!(
        db.ids("notes", &mut trunk),
        vec![1],
        "the INSERT was refused with `{err}`, and MERGE published its row anyway"
    );
}

/// **T2 (control). One byte under the cap still stages, mirrors and merges.**
///
/// Without this, code that refused every long string — or every INSERT — would pass T1.
#[test]
fn a_row_at_the_entry_cap_still_stages_mirrors_and_merges() {
    let mut db = Db::new(Shape::PagedMem);
    let mut setup = db.session();
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, body VARCHAR(2000));", &mut setup);
    db.ok("INSERT INTO notes VALUES (1, 'seed');", &mut setup);

    let mut a = db.session();
    let branch = db.begin("t2", &mut a);
    let body = "x".repeat(981);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{body}');"), &mut a);
    assert_eq!(
        db.tree_row(branch, "notes", 2),
        Some(vec![Value::Integer(2), Value::Varchar(body.clone())]),
        "a 1015-byte entry is exactly the cap and must be mirrored"
    );

    db.ok("MERGE;", &mut a);
    let mut trunk = db.session();
    let rows = db.rows("SELECT id, body FROM notes WHERE id = 2;", &mut trunk);
    assert_eq!(rows, vec![vec![Value::Integer(2), Value::Varchar(body)]], "the row did not merge");
    assert_eq!(db.ids("notes", &mut trunk), vec![1, 2]);
}

// ---- T3, T4: no trace, applied once, nothing spent -------------------------------------------

/// **T3. A refused statement leaves no trace, and its corrected retry applies exactly once.**
///
/// The oversized statement is sent twice, the way a client that retries on error sends it. Before
/// the fix each refusal staged its decrement, so the corrected third statement read 10 and wrote 5.
#[test]
fn a_refused_statement_leaves_no_trace_and_its_retry_applies_once() {
    let mut db = Db::new(Shape::PagedMem);
    let mut setup = db.session();
    db.ok("CREATE TABLE ledger (id INTEGER NOT NULL, qty INTEGER, note VARCHAR(2000));", &mut setup);
    db.ok("INSERT INTO ledger VALUES (1, 20, 'n');", &mut setup);

    let mut a = db.session();
    let branch = db.begin("t3", &mut a);
    let oversized = format!(
        "UPDATE ledger SET qty = qty - 5, note = '{}' WHERE id = 1;",
        "y".repeat(OVERSIZED_NOTE)
    );
    for attempt in 1..=2 {
        let err = db.refused(&oversized, &mut a);
        let view = db.rows("SELECT qty, note FROM ledger WHERE id = 1;", &mut a);
        assert_eq!(
            view,
            vec![vec![Value::Integer(20), Value::Varchar("n".into())]],
            "refusal {attempt} (`{err}`) left the statement staged on the branch"
        );
    }
    assert_eq!(db.diff_len(&mut a), 0, "DIFF reports rows from statements that were refused");

    db.ok("UPDATE ledger SET qty = qty - 5, note = 'm' WHERE id = 1;", &mut a);
    assert_eq!(
        db.rows("SELECT qty, note FROM ledger WHERE id = 1;", &mut a),
        vec![vec![Value::Integer(15), Value::Varchar("m".into())]],
        "the corrected retry did not apply exactly once"
    );
    assert_eq!(
        db.tree_row(branch, "ledger", 1),
        Some(vec![Value::Integer(1), Value::Integer(15), Value::Varchar("m".into())]),
        "the branch's tree disagrees with its workspace"
    );

    db.ok("MERGE;", &mut a);
    let mut trunk = db.session();
    assert_eq!(
        db.rows("SELECT qty, note FROM ledger WHERE id = 1;", &mut trunk),
        vec![vec![Value::Integer(15), Value::Varchar("m".into())]],
        "MERGE did not publish exactly the one statement that was accepted"
    );
}

/// **T4. A refused statement spends neither its escrow nor its row-write budget.**
///
/// Budget 1 and a claim of 5 are exactly what the corrected retry needs, so a refusal that kept
/// either would make the retry fail — the caller could not recover from a refusal it was told about.
#[test]
fn a_refused_statement_spends_neither_escrow_nor_row_write_budget() {
    let mut db = Db::new(Shape::PagedMem);
    let mut setup = db.session();
    db.ok("CREATE TABLE ledger (id INTEGER NOT NULL, qty INTEGER, note VARCHAR(2000));", &mut setup);
    db.ok("INSERT INTO ledger VALUES (1, 20, 'n');", &mut setup);
    let one = CapabilityEnvelope::new(Verb::ALL, 1).allow(
        table_id("ledger").0,
        vec![ColumnCapability::open(0), ColumnCapability::open(1), ColumnCapability::open(2)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, one).unwrap();
    db.runtime.open_escrow("ledger", row_id_of(&[Value::Integer(1)]), QTY, 20).unwrap();

    let mut a = db.session();
    let branch = db.begin("t4", &mut a);
    db.runtime.claim_escrow(branch, "ledger", row_id_of(&[Value::Integer(1)]), QTY, 5).unwrap();

    let err = db.refused(
        &format!(
            "UPDATE ledger SET qty = qty - 5, note = '{}' WHERE id = 1;",
            "y".repeat(OVERSIZED_NOTE)
        ),
        &mut a,
    );
    assert_eq!(
        db.remaining(branch, "ledger", 1),
        Some(5),
        "the statement was refused with `{err}` and still spent the escrow claim"
    );
    assert_eq!(db.row_writes(branch), 0, "the refused statement was charged a row-write");

    db.exec("UPDATE ledger SET qty = qty - 5, note = 'm' WHERE id = 1;", &mut a)
        .unwrap_or_else(|e| panic!("the corrected retry was refused with `{e}`"));
    assert_eq!(db.remaining(branch, "ledger", 1), Some(0));
    assert_eq!(db.row_writes(branch), 1);

    db.ok("MERGE;", &mut a);
    let mut trunk = db.session();
    assert_eq!(db.id_qty("ledger", &mut trunk), vec![(1, 15)]);
}

// ---- T5: the durable log's own limit ---------------------------------------------------------

/// **T5. A statement the durable log refuses does not wedge the branch.**
///
/// A 65,536-byte string does not fit the log's u16 length prefix. Before the fix its op stayed in
/// `ws.frame` and every later append re-encoded it, so the branch could never write again. Map-backed
/// on purpose: with no page tree, the log is the only thing that can refuse.
///
/// The envelope pins the order as well as the outcome: the refusal must be decided before the
/// budget is charged, so the good statement is the only one that costs a row-write.
#[test]
fn a_log_limit_refusal_does_not_wedge_the_branch() {
    let mut db = Db::new(Shape::MapDurable);
    let mut setup = db.session();
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, body VARCHAR(65535));", &mut setup);
    db.ok("INSERT INTO notes VALUES (1, 'seed');", &mut setup);
    let ten = CapabilityEnvelope::new(Verb::ALL, 10).allow(
        table_id("notes").0,
        vec![ColumnCapability::open(0), ColumnCapability::open(1)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, ten).unwrap();

    let mut a = db.session();
    let branch = db.begin("t5", &mut a);
    let err =
        db.refused(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(65_536)), &mut a);
    assert!(err.contains("length prefix"), "refused, but not by the log's length prefix: {err}");

    db.exec("INSERT INTO notes VALUES (3, 'ok');", &mut a).unwrap_or_else(|e| {
        panic!("the branch is wedged: after `{err}`, an ordinary INSERT was refused with `{e}`")
    });
    assert_eq!(db.row_writes(branch), 1, "the refused statement was charged a row-write");
    assert_eq!(db.ids("notes", &mut a), vec![1, 3]);

    db.ok("MERGE;", &mut a);
    let mut trunk = db.session();
    assert_eq!(db.ids("notes", &mut trunk), vec![1, 3]);
}

// ---- T6, T7: failures after every check has passed -------------------------------------------

/// **T6. A tree write that fails part-way through a statement leaves no trace anywhere.**
///
/// Swept over EVERY `cow_page` call the statement makes, not sampled: the calibration run counts
/// them, and each ordinal is failed on its own fresh database. The last ordinal necessarily lands in
/// the second row's write, after the first row's write finished — which is the case that needs the
/// first row put back.
#[test]
fn a_page_tree_failure_mid_statement_leaves_no_trace() {
    const S: &str = "UPDATE ledger SET qty = qty - 5 WHERE qty >= 0;";

    let calls = {
        let mut db = Db::new(Shape::PagedMem);
        let (mut a, _) = db.two_claimed_rows();
        db.faults.arm_cow(0);
        db.ok(S, &mut a);
        db.faults.cow_calls.load(Ordering::SeqCst)
    };
    assert!(
        calls >= 2,
        "a two-row statement made {calls} cow_page call(s); each row's write makes at least one, so \
         the sweep below would not reach the second row"
    );

    for k in 1..=calls {
        let mut db = Db::new(Shape::PagedMem);
        let (mut a, branch) = db.two_claimed_rows();
        db.faults.arm_cow(k);
        let err = db.refused(S, &mut a);
        assert!(err.contains("injected"), "call {k}: refused for another reason: {err}");

        assert_eq!(
            db.id_qty("ledger", &mut a),
            vec![(1, 20), (2, 20)],
            "call {k} of {calls} failed with `{err}` and the statement is still staged"
        );
        for id in [1, 2] {
            assert_eq!(
                db.tree_row(branch, "ledger", id),
                None,
                "call {k} of {calls}: row {id} is still in the branch's tree"
            );
            assert_eq!(
                db.remaining(branch, "ledger", id),
                Some(5),
                "call {k} of {calls}: row {id}'s escrow was spent by a statement that failed"
            );
        }
        assert_eq!(db.diff_len(&mut a), 0, "call {k}: DIFF reports the failed statement");

        db.ok(S, &mut a);
        assert_eq!(db.id_qty("ledger", &mut a), vec![(1, 15), (2, 15)], "call {k}: the retry");
        for id in [1, 2] {
            assert_eq!(
                db.tree_row(branch, "ledger", id).map(|r| r[1].clone()),
                Some(Value::Integer(15)),
                "call {k}: row {id} of the retry is not in the branch's tree"
            );
            assert_eq!(db.remaining(branch, "ledger", id), Some(0));
        }
        db.ok("MERGE;", &mut a);
        let mut trunk = db.session();
        assert_eq!(db.id_qty("ledger", &mut trunk), vec![(1, 15), (2, 15)], "call {k}: MERGE");
    }
}

/// **T7. An effect-log write that fails leaves no trace, and puts the tree back as it was.**
///
/// Row 1 already holds an earlier statement's image in the tree, so putting it back means restoring
/// a VALUE, not deleting a key; row 2 had nothing, so it means deleting. Both halves are asserted.
#[test]
fn a_failed_log_write_leaves_no_trace_and_restores_the_page_tree() {
    const S: &str = "UPDATE ledger SET qty = qty - 5 WHERE qty >= 0;";
    let mut db = Db::new(Shape::PagedDurable);
    let (mut a, branch) = db.two_claimed_rows();
    db.ok("UPDATE ledger SET note = 'x' WHERE id = 1;", &mut a);
    let row1 = |qty: i32| Some(vec![Value::Integer(1), Value::Integer(qty), Value::Varchar("x".into())]);
    assert_eq!(db.tree_row(branch, "ledger", 1), row1(20), "the fixture's first write is not mirrored");

    db.faults.fail_log_write.store(true, Ordering::SeqCst);
    let err = db.refused(S, &mut a);
    assert!(err.contains("injected"), "refused for another reason: {err}");

    assert_eq!(
        db.id_qty("ledger", &mut a),
        vec![(1, 20), (2, 20)],
        "the log write failed with `{err}` and the statement is still staged"
    );
    assert_eq!(db.tree_row(branch, "ledger", 1), row1(20), "row 1's earlier image was not restored");
    assert_eq!(db.tree_row(branch, "ledger", 2), None, "row 2 was left in the tree");
    for id in [1, 2] {
        assert_eq!(db.remaining(branch, "ledger", id), Some(5), "row {id}'s escrow was spent");
    }
    assert_eq!(db.diff_len(&mut a), 1, "DIFF must report the earlier statement and only it");

    db.exec(S, &mut a).unwrap_or_else(|e| panic!("the retry after a failed log write: {e}"));
    assert_eq!(db.id_qty("ledger", &mut a), vec![(1, 15), (2, 15)]);
    assert_eq!(db.tree_row(branch, "ledger", 1), row1(15));
    assert_eq!(
        db.tree_row(branch, "ledger", 2),
        Some(vec![Value::Integer(2), Value::Integer(15), Value::Varchar("b".into())])
    );
    for id in [1, 2] {
        assert_eq!(db.remaining(branch, "ledger", id), Some(0));
    }

    db.ok("MERGE;", &mut a);
    let mut trunk = db.session();
    assert_eq!(
        db.rows("SELECT id, qty, note FROM ledger WHERE id = 1;", &mut trunk),
        vec![vec![Value::Integer(1), Value::Integer(15), Value::Varchar("x".into())]]
    );
    assert_eq!(db.id_qty("ledger", &mut trunk), vec![(1, 15), (2, 15)]);
}

// ---- T8: a sibling merge across two tables ---------------------------------------------------

/// **T8. A sibling merge refused on its second table stages nothing on the first.**
///
/// `merge_into` staged one table at a time, in table-id order, so the refusal has to be on the
/// table with the LARGER id for the old shape to have staged anything first. The names are chosen
/// here from the ids rather than assumed, because a table id is a hash of the name.
#[test]
fn a_sibling_merge_that_refuses_on_its_second_table_stages_nothing() {
    let (lo, hi) = if table_id("d258_a").0 < table_id("d258_b").0 {
        ("d258_a", "d258_b")
    } else {
        ("d258_b", "d258_a")
    };
    let mut db = Db::new(Shape::PagedMem);
    let mut setup = db.session();
    for t in [lo, hi] {
        db.ok(&format!("CREATE TABLE {t} (id INTEGER NOT NULL, qty INTEGER);"), &mut setup);
        db.ok(&format!("INSERT INTO {t} VALUES (1, 20);"), &mut setup);
    }
    let cell = row_id_of(&[Value::Integer(1)]);
    db.runtime.open_escrow(hi, cell, QTY, 20).unwrap();

    let mut src = db.session();
    let source = db.begin("t8_source", &mut src);
    db.runtime.claim_escrow(source, hi, cell, QTY, 5).unwrap();
    db.ok(&format!("UPDATE {lo} SET qty = 1 WHERE id = 1;"), &mut src);
    db.ok(&format!("UPDATE {hi} SET qty = qty - 5 WHERE id = 1;"), &mut src);

    let mut tgt = db.session();
    let target = db.begin("t8_target", &mut tgt);

    let rt = db.runtime.clone();
    let refused = {
        let mut ctx = ExecCtx { catalog: &mut db.catalog, bp: db.bp.clone(), txn: db.txn.clone() };
        rt.merge_into(&mut ctx, source, target)
    };
    let err = match refused {
        Err(e) => e.to_string(),
        Ok(r) => panic!("the target holds no claim on {hi}'s bounded cell, and the merge applied={}", r.applied),
    };
    assert_eq!(
        db.id_qty(lo, &mut tgt),
        vec![(1, 20)],
        "the merge was refused on {hi} with `{err}` and still staged {lo} on the target"
    );
    assert_eq!(db.id_qty(hi, &mut tgt), vec![(1, 20)]);
    assert_eq!(db.diff_len(&mut tgt), 0, "the target's DIFF reports a refused merge's rows");

    // Control: with the claim in place the same merge applies to BOTH tables.
    db.runtime.claim_escrow(target, hi, cell, QTY, 5).unwrap();
    let report = {
        let mut ctx = ExecCtx { catalog: &mut db.catalog, bp: db.bp.clone(), txn: db.txn.clone() };
        rt.merge_into(&mut ctx, source, target).unwrap()
    };
    assert!(report.applied, "a merge the target can afford was not applied");
    assert_eq!(db.id_qty(lo, &mut tgt), vec![(1, 1)]);
    assert_eq!(db.id_qty(hi, &mut tgt), vec![(1, 15)]);
}

// ---- T9: a write that lands and then fails ---------------------------------------------------

/// **T9. A row whose write LANDED before it reported failure is put back too.**
///
/// T6's faults are inside `cow_page`, where the tree's own journal rolls the failed operation back,
/// so nothing of the failing row ever reaches the tree. This one fails after the tree operation has
/// committed: the catalog moves the branch's root and then its durability step fails, which is what
/// both production catalogs do when the fsync is refused. The first row of a fresh branch is the row
/// that moves the root (its write copies the parent's shared root), so the failing row is row 1 —
/// and an undo list that records a row only once its write returned Ok has nothing for it.
#[test]
fn a_row_write_that_lands_and_then_fails_is_put_back_too() {
    const S: &str = "UPDATE ledger SET qty = qty - 5 WHERE qty >= 0;";
    let mut db = Db::new(Shape::PagedMem);
    let mut setup = db.session();
    db.ok("CREATE TABLE ledger (id INTEGER NOT NULL, qty INTEGER, note VARCHAR(2000));", &mut setup);
    db.ok("INSERT INTO ledger VALUES (1, 20, 'a');", &mut setup);
    db.ok("INSERT INTO ledger VALUES (2, 20, 'b');", &mut setup);
    let mut a = db.session();
    let branch = db.begin("t9", &mut a);

    db.faults.fail_set_root.store(true, Ordering::SeqCst);
    let err = db.refused(S, &mut a);
    assert!(err.contains("injected"), "refused for another reason: {err}");
    assert!(
        !db.faults.fail_set_root.load(Ordering::SeqCst),
        "the armed fault never fired, so this test exercised nothing"
    );

    assert_eq!(
        db.id_qty("ledger", &mut a),
        vec![(1, 20), (2, 20)],
        "the write failed with `{err}` and the statement is still staged"
    );
    for id in [1, 2] {
        assert_eq!(
            db.tree_row(branch, "ledger", id),
            None,
            "row {id} is in the branch's page tree after a statement that failed: its write landed \
             before the error, and nothing put it back"
        );
    }
    assert_eq!(db.diff_len(&mut a), 0, "DIFF reports the failed statement");

    db.exec(S, &mut a).unwrap_or_else(|e| panic!("the retry after a landed-then-failed write: {e}"));
    assert_eq!(db.id_qty("ledger", &mut a), vec![(1, 15), (2, 15)]);
    for id in [1, 2] {
        assert_eq!(
            db.tree_row(branch, "ledger", id).map(|r| r[1].clone()),
            Some(Value::Integer(15)),
            "row {id} of the retry is not in the branch's tree"
        );
    }
    db.ok("MERGE;", &mut a);
    let mut trunk = db.session();
    assert_eq!(db.id_qty("ledger", &mut trunk), vec![(1, 15), (2, 15)]);
}

// ---- T10, T11: when putting a row back fails (review 2, R2-1) ----------------------------------

/// **T10. A row whose write never landed is not rewritten, so its restore cannot fail.**
///
/// Row 1's prior image is `Some` — an earlier statement put it in the tree — and its write fails at
/// `cow_page` call 1, where the tree's own journal rolls it back: nothing landed. Putting back a
/// write that never landed is a rewrite of the same image, which asks the store for a page again;
/// the second armed fault (call 2) is there to make that rewrite fail if it is attempted. It must
/// not be: the tree already holds the prior, bit for bit. Before R2-1 it was attempted, failed, and
/// turned an ordinary storage error into an internal "double fault".
#[test]
fn a_restore_of_a_row_whose_write_never_landed_is_skipped() {
    let mut db = Db::new(Shape::PagedMem);
    let (mut a, branch) = db.two_claimed_rows();
    db.ok("UPDATE ledger SET note = 'x' WHERE id = 1;", &mut a);
    let row1 = |qty: i32| Some(vec![Value::Integer(1), Value::Integer(qty), Value::Varchar("x".into())]);
    assert_eq!(db.tree_row(branch, "ledger", 1), row1(20), "the fixture's first write is not mirrored");

    db.faults.arm_cow_twice(1, 2);
    let err = db.refused("UPDATE ledger SET qty = qty - 5 WHERE id = 1;", &mut a);
    assert!(err.contains("cow_page call 1"), "refused for another reason: {err}");
    assert!(
        !err.contains("failed too"),
        "a write that never landed was rewritten, the rewrite failed, and an ordinary storage \
         error came back as a double fault: {err}"
    );
    assert_eq!(
        db.faults.fail_cow_also.load(Ordering::SeqCst),
        2,
        "the restore asked the store for a page: the tree already held the prior, so nothing needed \
         writing"
    );
    assert_eq!(db.id_qty("ledger", &mut a), vec![(1, 20), (2, 20)]);
    assert_eq!(db.tree_row(branch, "ledger", 1), row1(20));
    assert_eq!(db.remaining(branch, "ledger", 1), Some(5), "the failed statement kept its escrow");
    assert_eq!(
        db.runtime.page_mirror_divergences(),
        0,
        "a row that was never changed was counted as a page mirror that may disagree"
    );

    db.faults.arm_cow(0);
    db.ok("UPDATE ledger SET qty = qty - 5 WHERE id = 1;", &mut a);
    assert_eq!(db.tree_row(branch, "ledger", 1), row1(15));
    db.ok("MERGE;", &mut a);
    let mut trunk = db.session();
    assert_eq!(db.id_qty("ledger", &mut trunk), vec![(1, 15), (2, 20)]);
}

/// **T11. A restore that really fails keeps the original error; the page mirror is left ahead.**
///
/// Row 1's write lands (call 1), row 2's fails (call 2, rolled back by the tree's journal), and
/// putting row 1 back fails too (call 3). Row 2 needs no restore — its image is already its prior —
/// so the only restore attempted is row 1's. The client must see the error that failed its
/// statement, not a different class of error about the page mirror; the mirror's disagreement with
/// the workspace is the stated residual, and it heals on the next write of that row.
#[test]
fn a_restore_that_fails_keeps_the_original_error() {
    const S: &str = "UPDATE ledger SET qty = qty - 5 WHERE qty >= 0;";
    let mut db = Db::new(Shape::PagedMem);
    let (mut a, branch) = db.two_claimed_rows();

    db.faults.arm_cow_twice(2, 3);
    let err = db.refused(S, &mut a);
    assert!(err.contains("cow_page call 2"), "refused for another reason: {err}");
    assert!(
        !err.contains("failed too"),
        "the statement failed at cow_page call 2, and the client was handed a different error: {err}"
    );
    assert_eq!(db.faults.fail_cow_also.load(Ordering::SeqCst), 0, "row 1's restore was never tried");
    assert_eq!(db.id_qty("ledger", &mut a), vec![(1, 20), (2, 20)], "the statement is staged");
    assert_eq!(
        db.tree_row(branch, "ledger", 1).map(|r| r[1].clone()),
        Some(Value::Integer(15)),
        "the fixture did not leave row 1's landed write in the tree, so no restore failed"
    );
    assert_eq!(db.tree_row(branch, "ledger", 2), None);
    for id in [1, 2] {
        assert_eq!(db.remaining(branch, "ledger", id), Some(5), "row {id}'s escrow was spent");
    }
    assert_eq!(
        db.runtime.page_mirror_divergences(),
        1,
        "row 1's failed restore left its page mirror ahead of the workspace, and it was not counted"
    );

    db.faults.arm_cow(0);
    db.exec(S, &mut a).unwrap_or_else(|e| panic!("the retry after a failed restore: {e}"));
    assert_eq!(db.id_qty("ledger", &mut a), vec![(1, 15), (2, 15)]);
    for id in [1, 2] {
        assert_eq!(
            db.tree_row(branch, "ledger", id).map(|r| r[1].clone()),
            Some(Value::Integer(15)),
            "row {id} of the retry is not in the branch's tree"
        );
    }
    db.ok("MERGE;", &mut a);
    let mut trunk = db.session();
    assert_eq!(db.id_qty("ledger", &mut trunk), vec![(1, 15), (2, 15)]);
}
