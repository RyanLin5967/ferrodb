//! **D212 (a'): the retention window, and the store's falsifiers.**
//!
//! SCALE-DESIGN "D212 (a') DECIDED" names the falsifier and "AMENDED 2" adds to it. Each test here
//! states which arm it is and the mutant that must turn it red:
//!
//! | test | arm | pre-registered mutant (in `src/wal/history.rs` / `src/wal/txn.rs`) |
//! |---|---|---|
//! | `exit_8_...` | exit test (8): REVERT refuses outside the window, naming `W` | `revert_merge` skips the in-memory ordinal check; or the store never prunes |
//! | `falsifier_1_...` | (1) the file's bytes flat in M, within one prune's worth, and ≤ (W + W/8)·max record | `drain`'s prune condition is never true |
//! | `falsifier_3_...` | (3) single-threaded: after a checkpoint the log holds no history record, and the store holds every one | the hook drains nothing; or `commit` queues nothing |
//! | `f5_...` | AMENDED 2 F5: an open with no store attached keeps the log. **SUPERSEDED by AMENDED 3 item 4**, which deleted the count: `recover` now REFUSES such a log, so this test fails as written; its replacement is a ⚖ in the lane report (not committed) | (none: the count is gone) |
//! | `falsifier_5_...` | AMENDED 2 F7: with an idle open transaction, queue bytes stay ≤ B, flat in M | `commit` skips the byte-bounded drain |
//! | `a_revert_after_ten_reverts_...` | the lead's new-wall audit: a REVERT reads the reverted set O(plan), not O(txns ever reverted) | a REVERT that asks the set about every txn it holds |
//! | `falsifier_5b_...` | AMENDED 3 item 8: with an idle open transaction, the FILE's bytes stay ≤ (W + W/8)·max record, flat in M | `drain`'s prune forced off (the one routine the hook and the commit path share) |
//!
//! (2) — no buffer-pool page and no history byte read by a prune — holds by construction:
//! `HistoryStore` has no buffer pool, and the window it rewrites is in memory
//! (`HistoryCounters::bytes_read_at_open` is the only read it does). (4) needs a WAL pin and a
//! process-wide counter, so it lives alone in `d212_history_pin.rs`.
//!
//! The window is passed to `HistoryStore::open` directly, so nothing here sets an environment
//! variable and the tests share one binary.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::history::{HistoryStore, IMAGE_OVERHEAD, QUEUE_DRAIN_BYTES, RECORD_FRAME};
use ferrodb::wal::log::{RecKind, WalManager};
use ferrodb::wal::recovery::{rebuild_indexes, recover};
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    store: Option<Arc<HistoryStore>>,
    dir: PathBuf,
    window: Option<u64>,
}

impl Db {
    /// `cli.rs`'s sequence, with the history store attached before `recover` when `window` is
    /// `Some(W)`, and not at all when it is `None`.
    fn open(dir: &Path, window: Option<u64>) -> Db {
        let path = dir.join("d212w.db");
        let existed = path.exists();
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.join("d212w.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let store = window.map(|w| HistoryStore::open(dir.join("d212w.db.history"), w).unwrap());
        if let Some(s) = &store {
            txn.attach_history_store(Arc::clone(s)).unwrap();
        }
        let recovered = recover(&txn).unwrap();
        let mut catalog = if existed {
            Catalog::open(bp.clone(), 1).unwrap()
        } else {
            Catalog::create(bp.clone()).unwrap()
        };
        if recovered {
            rebuild_indexes(&mut catalog, &bp).unwrap();
            txn.checkpoint().unwrap();
        }
        let branches = LogBranchCatalog::open(&dir.join("d212w.branches"), 1).unwrap();
        let runtime =
            Arc::new(AgentRuntime::with_catalog(Arc::new(branches) as Arc<dyn BranchCatalog>));
        Db { catalog, bp, txn, runtime, store, dir: dir.to_path_buf(), window }
    }

    /// A clean restart: checkpoint, drop everything, reopen from the files.
    fn restart(self) -> Db {
        self.txn.checkpoint().unwrap();
        let (dir, window) = (self.dir.clone(), self.window);
        drop(self);
        Db::open(&dir, window)
    }

    /// A crash: drop everything with no checkpoint, so the history queued in memory is lost and
    /// the log is its only copy. Reopened with `window`.
    fn crash_and_reopen(self, window: Option<u64>) -> Db {
        let dir = self.dir.clone();
        drop(self);
        Db::open(&dir, window)
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert!(parser.errors.is_empty(), "parse errors in {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn seed(&mut self, rows: i32) {
        let mut s = Session::with_runtime(self.runtime.clone());
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        for id in 1..=rows {
            self.ok(&format!("INSERT INTO inventory VALUES ({id}, 10);"), &mut s);
        }
    }

    /// One agent task running `sql`, merged; its merge id.
    fn merge_one(&mut self, name: &str, sql: &str) -> String {
        let mut a = Session::with_runtime(self.runtime.clone());
        self.ok(&format!("BEGIN AGENT SESSION AS '{name}' RUN 'r_{name}';"), &mut a);
        self.ok(sql, &mut a);
        match self.ok("MERGE;", &mut a) {
            Outcome::Agent(AgentOutput::Merge(m)) => {
                assert!(m.applied_to_target, "{m}");
                m.merge_id
            }
            _ => panic!("MERGE did not return a report"),
        }
    }

    fn revert(&mut self, id: &str) -> Result<Outcome, FerroError> {
        let mut s = Session::with_runtime(self.runtime.clone());
        self.exec(&format!("REVERT MERGE {id};"), &mut s)
    }

    fn revert_err(&mut self, id: &str) -> String {
        match self.revert(id) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("REVERT MERGE {id} was accepted"),
        }
    }

    fn revert_ok(&mut self, id: &str) {
        match self.revert(id) {
            Ok(Outcome::Agent(AgentOutput::Revert(p))) => {
                assert!(!p.is_blocked(), "{id} was blocked: {p:?}")
            }
            Ok(_) => panic!("REVERT MERGE {id} did not return a plan"),
            Err(e) => panic!("REVERT MERGE {id} failed: {e}"),
        }
    }

    fn qty_of(&mut self, id: i32) -> i32 {
        let mut s = Session::with_runtime(self.runtime.clone());
        let rows = match self.ok("SELECT id, qty FROM inventory;", &mut s) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT did not return rows"),
        };
        match rows.into_iter().find(|r| r[0] == Value::Integer(id)).map(|r| r[1].clone()) {
            Some(Value::Integer(q)) => q,
            other => panic!("row {id}: {other:?}"),
        }
    }

    /// How many history parts the retained log holds.
    fn history_parts_in_log(&self) -> usize {
        use std::sync::atomic::Ordering;
        let wal = &self.txn.wal;
        let (mut lsn, end) = (wal.base_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst));
        let mut n = 0;
        while lsn < end {
            let (rec, next) = wal.read_record(lsn).unwrap();
            if matches!(rec.kind, RecKind::RevertHistory { .. }) {
                n += 1;
            }
            lsn = next;
        }
        n
    }

    fn history_file_len(&self) -> u64 {
        std::fs::metadata(self.dir.join("d212w.db.history")).map_or(0, |m| m.len())
    }
}

/// **(8)** A REVERT older than the window is refused, and the refusal names the window — in memory
/// before a restart and from the store after one.
#[test]
fn exit_8_a_revert_older_than_the_window_is_refused_and_names_it() {
    const W: u64 = 2;
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path(), Some(W));
    db.seed(5);
    // Five independent merges, one row each, so no one of them depends on another.
    let ids: Vec<String> = (1..=5)
        .map(|id| db.merge_one(&format!("w{id}"), &format!("UPDATE inventory SET qty = qty + 1 WHERE id = {id};")))
        .collect();

    let msg = db.revert_err(&ids[0]);
    assert!(msg.contains("retention window"), "refused, but not for the window: {msg}");
    assert!(msg.contains(&format!("last {W} published merges")), "the refusal does not name W: {msg}");
    // Anti-vacuity: the newest merge is inside the window and reverts.
    db.revert_ok(&ids[4]);
    assert_eq!(db.qty_of(5), 10);

    // The same answer from the store alone.
    let mut db = db.restart();
    let msg = db.revert_err(&ids[1]);
    assert!(msg.contains("retention window"), "after a restart, refused but not for the window: {msg}");
    assert!(msg.contains(&format!("last {W} published merges")), "the refusal does not name W: {msg}");
    db.revert_ok(&ids[3]);
    assert_eq!(db.qty_of(4), 10);
}

/// **(1)** The file's bytes do not grow with the merges ever made: across M = W, 4W and 16W they
/// stay within one prune's worth of each other, and never exceed `(W + W/8)` records' worth.
#[test]
fn falsifier_1_the_history_file_is_flat_in_the_merges_ever_made() {
    const W: u64 = 8;
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path(), Some(W));
    db.seed(4);
    let mut sizes = Vec::new();
    let mut made = 0u64;
    for m in [W, 4 * W, 16 * W] {
        while made < m {
            made += 1;
            let row = made % 4 + 1;
            db.merge_one(&format!("t{made}"), &format!("UPDATE inventory SET qty = qty + 1 WHERE id = {row};"));
            db.txn.checkpoint().unwrap();
        }
        sizes.push(db.history_file_len());
    }
    let store = db.store.clone().unwrap();
    let largest = store.records().iter().map(|r| (RECORD_FRAME + r.body.len()) as u64).max().unwrap();
    let one_prune = (W / 8).max(1) * largest;
    let bound = IMAGE_OVERHEAD as u64 + (W + (W / 8).max(1)) * largest;
    assert!(store.counters().prunes > 0, "the fixture never pruned, so it tests nothing");
    for (m, size) in [W, 4 * W, 16 * W].iter().zip(&sizes) {
        assert!(*size <= bound, "at M = {m} the file is {size} bytes, over {bound}: {sizes:?}");
    }
    let (lo, hi) = (sizes.iter().min().unwrap(), sizes.iter().max().unwrap());
    assert!(hi - lo <= one_prune, "the file grew with M beyond one prune's worth ({one_prune}): {sizes:?}");
}

/// **(3)**, single-threaded: after a checkpoint the log holds no history record, and every record
/// committed is in the store.
#[test]
fn falsifier_3_after_a_checkpoint_the_history_is_in_the_store_and_not_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path(), Some(1024));
    db.seed(3);
    for i in 1..=5 {
        db.merge_one(&format!("t{i}"), &format!("UPDATE inventory SET qty = qty + 1 WHERE id = {};", i % 3 + 1));
    }
    assert!(db.history_parts_in_log() >= 5, "the fixture's log holds no history, so it tests nothing");
    db.txn.checkpoint().unwrap();
    assert_eq!(db.history_parts_in_log(), 0, "a checkpoint left history in the log");
    // What the FILE holds, read by a second handle: the running store's `records()` would also
    // count its in-memory queue.
    let on_disk = HistoryStore::open(dir.path().join("d212w.db.history"), 1024).unwrap();
    let publishes = on_disk.records().iter().filter(|r| r.ordinal > 0).count();
    assert_eq!(publishes, 5, "a committed publish record is not in the store's file");
}

/// **AMENDED 2, F5.** A process that opens the database without the store must not truncate away
/// history the store never received: the log is kept until an open WITH the store takes it.
#[test]
fn f5_an_open_without_the_store_keeps_the_history_the_store_lacks() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path(), Some(1024));
    db.seed(2);
    let id = db.merge_one("a", "UPDATE inventory SET qty = 11 WHERE id = 1;");
    // The history is in the log only: the queue died with the process.
    let db = db.crash_and_reopen(None);
    assert!(db.history_parts_in_log() > 0, "an open without the store truncated the only copy");
    // Anti-vacuity: an open WITH the store takes it, and the merge reverts.
    let mut db = db.crash_and_reopen(Some(1024));
    db.revert_ok(&id);
    assert_eq!(db.qty_of(1), 10);
}

/// **AMENDED 2, F7 — falsifier (5).** With an idle open transaction every checkpoint is refused,
/// so only the commit path's byte-bounded drain keeps the queue from growing with M.
#[test]
fn falsifier_5_the_queue_stays_bounded_while_checkpoints_are_blocked() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path(), Some(1024));
    db.seed(100);
    let mut idle = Session::with_runtime(db.runtime.clone());
    db.ok("BEGIN;", &mut idle);
    let store = db.store.clone().unwrap();
    let mut largest = 0usize;
    let mut peak = 0usize;
    // Every merge updates all 100 rows, so a record is tens of KiB and B is crossed within M.
    for i in 1..=120 {
        db.merge_one(&format!("t{i}"), "UPDATE inventory SET qty = qty + 1;");
        largest = largest.max(store.records().last().map_or(0, |r| RECORD_FRAME + r.body.len()));
        peak = peak.max(store.queued_bytes());
    }
    let c = store.counters();
    assert!(c.appends + c.rewrites > 0, "the queue never drained, so B was never crossed: {c:?}");
    assert!(
        peak <= QUEUE_DRAIN_BYTES + largest,
        "the queue reached {peak} bytes with checkpoints blocked; the bound is {} + one record",
        QUEUE_DRAIN_BYTES
    );
    db.ok("ROLLBACK;", &mut idle);
}

/// **AMENDED 3, item 8 — falsifier (5b).** With an idle open transaction every checkpoint is refused,
/// so only the commit path's drain writes the store. It is the SAME routine as the checkpoint hook's
/// (`HistoryStore::drain`), which prunes, so the FILE stays within `(W + W/8)` publishes whatever
/// M is, and not only the queue.
#[test]
fn falsifier_5b_store_bytes_stay_flat_with_an_idle_transaction() {
    const W: u64 = 8;
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path(), Some(W));
    db.seed(100);
    let mut idle = Session::with_runtime(db.runtime.clone());
    db.ok("BEGIN;", &mut idle);
    let store = db.store.clone().unwrap();
    let mut largest = 0u64;
    let mut sizes = Vec::new();
    let mut made = 0;
    for m in [60, 120] {
        while made < m {
            made += 1;
            db.merge_one(&format!("t{made}"), "UPDATE inventory SET qty = qty + 1;");
            let last = store.records().last().map_or(0, |r| (RECORD_FRAME + r.body.len()) as u64);
            largest = largest.max(last);
        }
        sizes.push(db.history_file_len());
    }
    let c = store.counters();
    assert!(c.drains >= 2, "the commit path drained fewer than twice, so M never crossed B twice: {c:?}");
    assert!(c.prunes > 0, "nothing pruned, so the bound tests nothing: {c:?}");
    let bound = IMAGE_OVERHEAD as u64 + (W + (W / 8).max(1)) * largest;
    for (m, size) in [60, 120].iter().zip(&sizes) {
        assert!(*size <= bound, "at M = {m} with checkpoints blocked the file is {size} bytes, over {bound}: {sizes:?}");
    }
    db.ok("ROLLBACK;", &mut idle);
}

/// **The lead's new-wall audit (08:23Z): a REVERT reads the reverted set only for the txns its plan
/// names.** Ten earlier REVERTs fill the set with ten txns; the eleventh REVERT, whose plan is its
/// target alone, asks the set about that one txn. At `b2269c9` every REVERT cloned the whole set.
#[test]
fn a_revert_after_ten_reverts_visits_only_its_plan() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path(), Some(1024));
    db.seed(12);
    for i in 1..=10 {
        let id = db.merge_one(&format!("t{i}"), &format!("UPDATE inventory SET qty = qty + 1 WHERE id = {i};"));
        db.revert_ok(&id);
    }
    let id = db.merge_one("last", "UPDATE inventory SET qty = qty + 1 WHERE id = 11;");
    let before = db.runtime.reverted_lookups();
    db.revert_ok(&id);
    let lookups = db.runtime.reverted_lookups() - before;
    assert!(lookups >= 1, "anti-vacuity: the REVERT never asked the set about its own target");
    assert!(
        lookups <= 3,
        "a REVERT whose plan is one txn made {lookups} lookups into a set of ten: it pays for the set, not the plan"
    );
    assert_eq!(db.qty_of(11), 10, "the last merge was not reverted");
}
