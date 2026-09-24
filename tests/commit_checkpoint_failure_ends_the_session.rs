//! F4(ii) of the adversary on `993145c..115f0b7` (`frontier/rollback_adversary.md` @ `4902c75`): a
//! COMMIT that ended its transaction must release the session even if the automatic checkpoint
//! after it fails.
//!
//! `TxnManager::commit` writes `TxnEnd`, removes the transaction from `att`, and only then may run an
//! automatic checkpoint. When that checkpoint failed, `commit` returned `Err`, and D211's rule "keep
//! the id on any error" kept a DEAD id in the session. ROLLBACK then failed ("txn not active"), every
//! statement failed, and BEGIN was refused ("txn already started"): the session was wedged until it
//! disconnected.
//!
//! The lead's decision: once the transaction has ended, the session drops the id, even if the
//! checkpoint then fails. The error still says the transaction COMMITTED.
//!
//! This binary sets `FERRODB_CHECKPOINT_INTERVAL=1`, so every commit checkpoints. The variable is
//! read once per process (`wal::txn::checkpoint_interval`), which is why this test has a binary of
//! its own. The WAL's second write after arming fails: the first is the commit's own flush, the
//! second is the checkpoint's. INFERRED from source and never run.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::storage::Storage;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// A real file that fails exactly one write, the one after `passes` more succeed, once armed.
struct OneFailure {
    file: std::fs::File,
    armed: AtomicBool,
    passes: AtomicU64,
}

impl Storage for OneFailure {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        if self.armed.load(Ordering::SeqCst) {
            if self.passes.load(Ordering::SeqCst) == 0 {
                self.armed.store(false, Ordering::SeqCst);
                return Err(io::Error::other("injected write failure"));
            }
            self.passes.fetch_sub(1, Ordering::SeqCst);
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
fn a_commit_whose_checkpoint_fails_after_the_transaction_ended_releases_the_session() {
    // SAFETY: set before this binary's first commit reads it, and no other thread reads the
    // environment here.
    unsafe { std::env::set_var("FERRODB_CHECKPOINT_INTERVAL", "1") };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ended.db");
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal_file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(wal_path(&path)).unwrap();
    let store = Arc::new(OneFailure { file: wal_file, armed: AtomicBool::new(false), passes: AtomicU64::new(0) });
    let wal = Arc::new(WalManager::with_storage(store.clone() as Arc<dyn Storage>, wal_path(&path)).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let catalog = Catalog::create(bp.clone()).unwrap();
    let mut db = Db { catalog, bp, txn };

    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL);", &mut s);
    db.ok("BEGIN;", &mut s);
    db.ok("INSERT INTO t VALUES (1);", &mut s);

    // The commit's own flush passes; the checkpoint's flush, after `TxnEnd`, fails.
    store.passes.store(1, Ordering::SeqCst);
    store.armed.store(true, Ordering::SeqCst);
    let e = db.exec("COMMIT;", &mut s).err().expect("premise: the automatic checkpoint did not fail");
    assert!(!store.armed.load(Ordering::SeqCst), "premise: the injected failure never fired");
    assert!(e.to_string().contains("COMMITTED"), "the error does not say the transaction committed: {e}");
    assert_eq!(s.current, None, "the session kept a transaction that had ended");

    db.ok("BEGIN;", &mut s);
    db.ok("INSERT INTO t VALUES (2);", &mut s);
    db.ok("COMMIT;", &mut s);
    match db.ok("SELECT id FROM t;", &mut s) {
        Outcome::Rows(rows) => assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]], "a committed row is missing"),
        _ => panic!("SELECT did not return rows"),
    }
}
