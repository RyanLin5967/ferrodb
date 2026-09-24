//! F4(i) of the adversary on `993145c..115f0b7` (`frontier/rollback_adversary.md` @ `4902c75`): a
//! COMMIT whose `Commit` record could not be flushed must not be rolled back.
//!
//! `TxnManager::commit` appends the `Commit` record, then flushes it. A failed flush puts the bytes
//! back in the log buffer for a later flush (`WalManager::flush`). The COMMIT returns `Err`, and the
//! session keeps its transaction (D211). Before this fix, ROLLBACK was then accepted. It logged an
//! Abort, CLRs and a `TxnEnd` AFTER that `Commit`. Once any later flush made the log durable,
//! recovery would treat the transaction as committed while its pages were rolled back, and the
//! change feed would already have shipped its rows at the `Commit`.
//!
//! The lead's decision: a flush failure after the `Commit` record fail-stops the log, as
//! PostgreSQL's fsync-failure PANIC does. The log is poisoned: every later write is refused, and the
//! database must be reopened, where recovery decides from what reached disk. Here the injected
//! failure keeps the `Commit` off disk, so the reopen finds the transaction a loser. INFERRED from
//! source and never run.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::storage::Storage;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::open_recovered;
use ferrodb::wal::txn::TxnManager;

/// A real file whose writes can be made to fail, as a full or failing disk does.
struct FailableFile {
    file: std::fs::File,
    fail: AtomicBool,
}

impl Storage for FailableFile {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected write failure"));
        }
        Storage::pwrite(&self.file, buf, offset)
    }
    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        Storage::pread(&self.file, buf, offset)
    }
    fn sync_all(&self) -> io::Result<()> {
        Storage::sync_all(&self.file)
    }
    fn sync_data(&self) -> io::Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected fsync failure"));
        }
        Storage::sync_data(&self.file)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        Storage::set_len(&self.file, len)
    }
    fn len(&self) -> io::Result<u64> {
        Storage::len(&self.file)
    }
}

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

impl Db {
    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }
}

fn wal_path(db: &Path) -> PathBuf {
    let mut p = db.as_os_str().to_os_string();
    p.push(".wal");
    PathBuf::from(p)
}

#[test]
fn a_commit_whose_flush_fails_poisons_the_log_and_cannot_be_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("poison.db");
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal_file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(wal_path(&path)).unwrap();
    let store = Arc::new(FailableFile { file: wal_file, fail: AtomicBool::new(false) });
    let wal = Arc::new(WalManager::with_storage(store.clone() as Arc<dyn Storage>, wal_path(&path)).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let catalog = Catalog::create(bp.clone()).unwrap();
    let mut db = Db { catalog, bp, txn };

    let mut main = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(8));", &mut main);
    db.ok("INSERT INTO t VALUES (1, 'a');", &mut main);

    let mut t1 = Session::new();
    db.ok("BEGIN;", &mut t1);
    let id = t1.current.expect("BEGIN opened a transaction");
    db.ok("INSERT INTO t VALUES (2, 'b');", &mut t1);

    store.fail.store(true, Ordering::SeqCst);
    assert!(db.exec("COMMIT;", &mut t1).is_err(), "premise: the COMMIT succeeded although its Commit record could not be flushed");
    assert_eq!(t1.current, Some(id), "the session forgot a transaction whose COMMIT is undecided");
    // The disk comes back. The log must not: its durable end is unknown, and only a reopen decides it.
    store.fail.store(false, Ordering::SeqCst);

    assert!(
        db.exec("ROLLBACK;", &mut t1).is_err(),
        "ROLLBACK after a failed COMMIT was accepted: it logs an Abort after a Commit that a later flush can still make durable"
    );
    assert!(db.exec("INSERT INTO t VALUES (3, 'c');", &mut main).is_err(), "the poisoned log accepted another transaction's write");
    assert!(db.txn.checkpoint().is_err(), "the poisoned log accepted a checkpoint, which would flush the undecided Commit");

    // The crash, and the reopen that decides. The injected failure kept the Commit off disk, so
    // transaction `id` is a loser: row 2 is gone, row 1 stays, and row 3 was never written.
    drop(db);
    drop(wal);
    drop(store);
    let lock = DbLock::acquire(&path).unwrap();
    let mut o = open_recovered(&path, &lock).expect("the database did not reopen after the poisoned log");
    let mut s = Session::new();
    let tokens = Scanner::new("SELECT id FROM t;".chars().collect(), Vec::new()).scan_tokens().unwrap();
    let stmt = Parser::new(tokens).parse().remove(0);
    let rows = match run(stmt, &mut o.catalog, o.bp.clone(), o.txn.clone(), &mut s).unwrap() {
        Outcome::Rows(r) => r,
        _ => panic!("SELECT did not return rows"),
    };
    assert_eq!(rows, vec![vec![Value::Integer(1)]], "after the reopen, the table is not exactly row 1");
}

/// **C3 and C6(a) of review 2** (`frontier/rollback_review2.md` @ artie-research `5f6b821`).
///
/// C3: poisoning must also mark the indexes stale. The reopen is the poison's whole contract, and
/// index pages are not logged: if the undecided transaction's records never reached disk and the log
/// holds nothing else, the reopen replays nothing and would not rebuild, so an index page flushed
/// with that transaction's entries would name slots the heap never got. The marker makes the reopen
/// rebuild. FAILS at `368d0e1` at "left no stale-indexes marker".
///
/// C6(a): once the disk works again, `flush` itself must still refuse. The first test's checkpoint
/// assertion is refused by the active-transaction table before the poison is consulted, so it cannot
/// tell whether `flush` checks the poison. Here the flush is called directly with bytes waiting in
/// the buffer, so only the poison check can refuse it.
#[test]
fn a_poisoned_log_marks_the_indexes_stale_and_refuses_a_direct_flush() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("poison_marker.db");
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal_file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(wal_path(&path)).unwrap();
    let store = Arc::new(FailableFile { file: wal_file, fail: AtomicBool::new(false) });
    let wal = Arc::new(WalManager::with_storage(store.clone() as Arc<dyn Storage>, wal_path(&path)).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let catalog = Catalog::create(bp.clone()).unwrap();
    let mut db = Db { catalog, bp, txn };

    let mut main = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(8));", &mut main);
    let mut t1 = Session::new();
    db.ok("BEGIN;", &mut t1);
    db.ok("INSERT INTO t VALUES (1, 'a');", &mut t1);

    let marker = ferrodb::wal::txn::stale_indexes_marker(&wal_path(&path));
    assert!(!marker.exists(), "premise: a stale-indexes marker exists before anything failed");
    store.fail.store(true, Ordering::SeqCst);
    assert!(db.exec("COMMIT;", &mut t1).is_err(), "premise: the COMMIT succeeded although its Commit record could not be flushed");
    store.fail.store(false, Ordering::SeqCst);

    assert!(marker.exists(), "the poisoned log left no stale-indexes marker, so a reopen that replays nothing would not rebuild");
    assert!(
        wal.flush().is_err(),
        "a direct flush of the poisoned log succeeded once the disk worked again: the undecided Commit reached disk"
    );

    drop(db);
    drop(wal);
    drop(store);
    let lock = DbLock::acquire(&path).unwrap();
    open_recovered(&path, &lock).expect("the database did not reopen after the poisoned log");
    assert!(!marker.exists(), "the reopen rebuilt, but did not consume the stale-indexes marker");
}
