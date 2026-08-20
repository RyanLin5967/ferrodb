//! ATTACK (I19, durability lens): a REFUSED `ALTER TABLE` must leave the table identical
//! *across a crash with no clean flush*, across a checkpoint + replay, in the WAL, in the
//! retained schema declaration, in the stats, and to a later successful ALTER.
//!
//! Written in a fresh context against 19b1de2 by an agent that did not write the guard.
//! Every test asserts it actually produced the refusal it is about, so a sweep that drifts
//! out of the refusing width band fails instead of passing vacuously.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::{DataType, Value};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::recover;
use ferrodb::wal::txn::TxnManager;

struct Db {
    dir: PathBuf,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

/// Open (or reopen) the database that lives in `dir`. Fresh `BufferPoolManager` and fresh
/// `Catalog` every time, so nothing a previous handle cached can answer for the disk.
fn open_at(dir: &Path) -> Db {
    let path = dir.join("alter.db");
    let fresh = !path.exists();
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog =
        if fresh { Catalog::create(bp.clone()).unwrap() } else { Catalog::open(bp.clone(), 1).unwrap() };
    let wal = Arc::new(WalManager::new(dir.join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { dir: dir.to_path_buf(), catalog, wal, bp, txn, runtime, session }
}

impl Db {
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                p.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        let mut session =
            std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out =
            run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut session);
        self.session = session;
        out
    }

    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.try_sql(sql)))
            .map_err(|_| format!("`{sql}` PANICKED the reading thread"))?;
        match out {
            Ok(Outcome::Rows(r)) => Ok(r),
            Ok(_) => Err(format!("`{sql}` did not return rows")),
            Err(e) => Err(format!("`{sql}` errored: {e}")),
        }
    }

    fn shape(&self, table: &str) -> Vec<(String, DataType)> {
        self.catalog
            .get_table(table)
            .unwrap_or_else(|| panic!("no table {table}"))
            .schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone()))
            .collect()
    }

    fn heap(&self, table: &str) -> Vec<(RecordId, Vec<u8>)> {
        let root = self.catalog.get_table(table).unwrap().first_directory_page_id;
        HeapFileManager::open(root, self.bp.clone())
            .scan()
            .map(|r| {
                let (rid, t) = r.unwrap();
                (rid, t.data)
            })
            .collect()
    }

    fn by_key(&self, table: &str, keys: &[Value]) -> Vec<(Value, Option<RecordId>)> {
        let root = self.catalog.get_table(table).unwrap().primary_index_root;
        let ix = BPlusTreeManager::<Value, RecordId>::open(root, self.bp.clone());
        keys.iter().map(|k| (k.clone(), ix.search(k).unwrap())).collect()
    }

    /// DDL records the change feed carries, `table:variant`.
    fn schema_changes(&self) -> Vec<String> {
        self.wal.flush().unwrap();
        ferrodb::replication::logical::LogicalDecoder::new(&self.catalog)
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode")
            .schema_changes
            .iter()
            .map(|(_, t, c)| format!("{t}:{c:?}"))
            .collect()
    }

    fn stats(&self, table: &str) -> String {
        format!("{:?}", self.catalog.table_stats(table))
    }

    /// Raw bytes of the data FILE, page by page, straight off the filesystem.
    /// Nothing in the buffer pool answers this.
    fn file_pages(&self) -> Vec<Vec<u8>> {
        let raw = std::fs::read(self.dir.join("alter.db")).unwrap();
        raw.chunks(PAGE_SIZE).map(|c| c.to_vec()).collect()
    }

    fn wal_bytes(&self) -> Vec<u8> {
        std::fs::read(self.dir.join("alter.wal")).unwrap_or_default()
    }
}

#[derive(Debug, PartialEq)]
struct Snap {
    shape: Vec<(String, DataType)>,
    heap: Vec<(RecordId, Vec<u8>)>,
    rows: Result<Vec<Vec<Value>>, String>,
    by_key: Vec<(Value, Option<RecordId>)>,
    stats: String,
}

fn snap(d: &mut Db) -> Snap {
    Snap {
        shape: d.shape("t"),
        heap: d.heap("t"),
        rows: d.rows("SELECT id, a FROM t;"),
        by_key: d.by_key("t", &[Value::Integer(1), Value::Integer(2), Value::Integer(3)]),
        stats: d.stats("t"),
    }
}

fn assert_is_the_size_refusal(e: &FerroError) {
    let msg = e.to_string();
    for needle in ["would widen", "bytes a tuple can occupy", "Nothing has been written"] {
        assert!(msg.contains(needle), "refused, but not by the size precheck: {msg}");
    }
}

/// Seed a three-row table whose widest row is `pad` bytes of VARCHAR.
/// Returns false when the seed itself does not fit a page (that width is not usable).
fn seed(d: &mut Db, pad: usize) -> bool {
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(4100));");
    d.sql("INSERT INTO t VALUES (1, 'small');");
    d.sql("INSERT INTO t VALUES (2, 'medium-sized-value');");
    let big = "x".repeat(pad);
    d.try_sql(&format!("INSERT INTO t VALUES (3, '{big}');")).is_ok()
}

fn diff_pages(before: &[Vec<u8>], after: &[Vec<u8>]) -> Vec<usize> {
    let n = before.len().max(after.len());
    (0..n)
        .filter(|i| before.get(*i).map(|v| v.as_slice()) != after.get(*i).map(|v| v.as_slice()))
        .collect()
}

// -------------------------------------------------------------------------------------------
// 1. Crash with NO clean flush, then recover.
// -------------------------------------------------------------------------------------------

/// Refuse an ALTER and then lose the process: every handle is dropped without `flush_all`,
/// without `sync` and without a checkpoint (`BufferPoolManager` has no `Drop`, so dirty frames
/// are simply gone), then the database is reopened and `recover()` is run — the CLI's sequence.
///
/// Also measures whether the refusal itself wrote to the data file at all, since a rewrite that
/// is deliberately unlogged means any byte it lands on disk is unrecoverable by design.
#[test]
fn refused_alter_then_crash_without_flush_then_recover_is_identical() {
    let mut refusals = 0;
    for pad in 4020usize..=4040 {
        let tmp = tempfile::tempdir().unwrap();
        let mut d = open_at(tmp.path());
        if !seed(&mut d, pad) {
            continue;
        }
        // A durable baseline, so "identical after the crash" is a statement about the disk.
        d.txn.checkpoint().unwrap();
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();

        let before = snap(&mut d);
        let pages_before = d.file_pages();
        let wal_before = d.wal_bytes();

        let Err(e) = d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);") else { continue };
        assert_is_the_size_refusal(&e);
        refusals += 1;

        // What the refusal put on the filesystem, with nothing flushed on purpose.
        let pages_after_refusal = d.file_pages();
        let wal_after_refusal = d.wal_bytes();
        assert_eq!(
            diff_pages(&pages_before, &pages_after_refusal),
            Vec::<usize>::new(),
            "pad={pad}: a refused ALTER wrote data-file pages. The rewrite is unlogged, so any \
             byte it lands is unrecoverable. Refusal was: {e}"
        );
        assert_eq!(
            wal_before, wal_after_refusal,
            "pad={pad}: a refused ALTER changed the WAL file"
        );

        // CRASH: drop every handle with nothing flushed, then reopen and recover.
        let dir = d.dir.clone();
        drop(d);
        let mut d = open_at(&dir);
        recover(&d.txn).expect("recover after the crash");

        let after = snap(&mut d);
        assert_eq!(
            before, after,
            "pad={pad}: a REFUSED ALTER did not survive a crash-without-flush plus recovery. \
             The statement reported failure ({e})."
        );
        assert_eq!(
            diff_pages(&pages_before, &d.file_pages()),
            Vec::<usize>::new(),
            "pad={pad}: recovery after a refused ALTER changed data-file pages"
        );
        drop(d);
        drop(tmp);
    }
    assert!(refusals > 0, "no width in 4020..=4040 produced the size refusal; fixture is vacuous");
    eprintln!("[T1] widths that refused: {refusals}");
}

// -------------------------------------------------------------------------------------------
// 2. Checkpoint, reopen, replay.
// -------------------------------------------------------------------------------------------

/// Refuse an ALTER, then checkpoint (which truncates the log), then reopen into a fresh buffer
/// pool and replay. Page-level file comparison as well as the tuple-level snapshot, because the
/// snapshot only sees live tuples and a half-rewritten page shows up in neither row counts nor
/// values.
#[test]
fn refused_alter_then_checkpoint_then_reopen_and_replay_is_identical() {
    let mut refusals = 0;
    for pad in 4020usize..=4040 {
        let tmp = tempfile::tempdir().unwrap();
        let mut d = open_at(tmp.path());
        if !seed(&mut d, pad) {
            continue;
        }
        d.txn.checkpoint().unwrap();
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
        let before = snap(&mut d);
        let pages_before = d.file_pages();

        let Err(e) = d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);") else { continue };
        assert_is_the_size_refusal(&e);
        refusals += 1;

        d.txn.checkpoint().unwrap();
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
        let dir = d.dir.clone();
        drop(d);

        let mut d = open_at(&dir);
        recover(&d.txn).expect("replay");
        assert_eq!(
            before,
            snap(&mut d),
            "pad={pad}: refusal + checkpoint + reopen + replay changed the table ({e})"
        );
        assert_eq!(
            diff_pages(&pages_before, &d.file_pages()),
            Vec::<usize>::new(),
            "pad={pad}: refusal + checkpoint changed data-file pages"
        );
        drop(d);
        drop(tmp);
    }
    assert!(refusals > 0, "no refusing width; fixture is vacuous");
    eprintln!("[T2] widths that refused: {refusals}");
}

// -------------------------------------------------------------------------------------------
// 3. WAL / retained declaration / stats.
// -------------------------------------------------------------------------------------------

/// No checkpoint anywhere, so the whole log — including the `CREATE TABLE` DDL record and every
/// insert — is still there to be decoded. A refusal must add nothing to it: not a DDL record
/// (which becomes the table's retained declaration), not a row change, not a stats update.
#[test]
fn a_refused_alter_adds_nothing_to_the_wal_the_declaration_or_the_stats() {
    let mut refusals = 0;
    for pad in 4020usize..=4040 {
        let tmp = tempfile::tempdir().unwrap();
        let mut d = open_at(tmp.path());
        if !seed(&mut d, pad) {
            continue;
        }
        d.sql("ANALYZE t;");
        let feed_before = d.schema_changes();
        let stats_before = d.stats("t");
        let wal_before = d.wal_bytes();
        let next_lsn_before = d.wal.next_lsn.load(Ordering::SeqCst);

        let Err(e) = d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);") else { continue };
        assert_is_the_size_refusal(&e);
        refusals += 1;

        assert_eq!(feed_before, d.schema_changes(), "pad={pad}: refusal changed the DDL feed ({e})");
        assert_eq!(stats_before, d.stats("t"), "pad={pad}: refusal changed the stats");
        assert_eq!(
            next_lsn_before,
            d.wal.next_lsn.load(Ordering::SeqCst),
            "pad={pad}: refusal advanced next_lsn — it appended WAL records"
        );
        let wal_after = d.wal_bytes();
        assert_eq!(
            wal_before.len(),
            wal_after.len(),
            "pad={pad}: refusal changed the WAL file length"
        );
        assert_eq!(wal_before, wal_after, "pad={pad}: refusal changed WAL bytes");
        drop(d);
        drop(tmp);
    }
    assert!(refusals > 0, "no refusing width; fixture is vacuous");
    eprintln!("[T3] widths that refused: {refusals}");
}

// -------------------------------------------------------------------------------------------
// 4. A refusal must not poison the next, successful ALTER.
// -------------------------------------------------------------------------------------------

/// Refuse a widening ALTER, then run one that fits, then checkpoint + flush + reopen into a
/// fresh buffer pool. The successful alter must be complete and correct — every row present,
/// every value right, every primary-index answer resolving — and the feed must carry exactly
/// ONE `AlterColumn` record, not two and not zero.
#[test]
fn a_refused_alter_does_not_poison_the_next_successful_one() {
    let mut cases = 0;
    for pad in 4020usize..=4040 {
        let tmp = tempfile::tempdir().unwrap();
        let mut d = open_at(tmp.path());
        if !seed(&mut d, pad) {
            continue;
        }
        let big = "x".repeat(pad);
        let Err(e) = d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);") else { continue };
        assert_is_the_size_refusal(&e);
        cases += 1;

        // A rename moves no bytes at all, so it must succeed even on this table.
        d.sql("ALTER TABLE t RENAME COLUMN a TO body;");

        d.txn.checkpoint().unwrap();
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
        let dir = d.dir.clone();
        drop(d);
        let mut d = open_at(&dir);
        recover(&d.txn).expect("replay");

        assert_eq!(
            d.shape("t"),
            vec![
                ("id".to_string(), DataType::Integer),
                ("body".to_string(), DataType::Varchar(4100))
            ],
            "pad={pad}: the successful rename after a refusal did not survive the reopen"
        );
        let rows = d.rows("SELECT id, body FROM t;").expect("read after rename");
        assert_eq!(rows.len(), 3, "pad={pad}: rows lost across refusal + rename + reopen");
        let mut got: Vec<(i64, usize)> = rows
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                (Value::Integer(i), Value::Varchar(s)) => (*i as i64, s.len()),
                other => panic!("pad={pad}: unexpected row {other:?}"),
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![(1, 5), (2, 18), (3, big.len())],
            "pad={pad}: values changed across refusal + rename + reopen"
        );
        for (k, hit) in d.by_key("t", &[Value::Integer(1), Value::Integer(2), Value::Integer(3)]) {
            assert!(hit.is_some(), "pad={pad}: primary index lost key {k:?}");
        }
        let feed = d.schema_changes();
        assert_eq!(
            feed.iter().filter(|s| s.contains("AlterColumn")).count(),
            1,
            "pad={pad}: expected exactly one AlterColumn DDL record after one refusal and one \
             success, got {feed:?}"
        );
        drop(d);
        drop(tmp);
    }
    assert!(cases > 0, "no refusing width; fixture is vacuous");
    eprintln!("[T4] widths that refused then renamed: {cases}");
}

// -------------------------------------------------------------------------------------------
// 5. A physical replica following the primary.
// -------------------------------------------------------------------------------------------

/// A physical replica is the primary's data file plus the primary's WAL, redone through the same
/// `apply_redo` path `recover` uses (see `replication::ReplicaApplier`'s doc). Build one after
/// the refusal and compare its heap page bytes with the primary's.
#[test]
fn a_physical_replica_built_after_a_refusal_matches_the_primary_byte_for_byte() {
    let mut refusals = 0;
    for pad in 4020usize..=4040 {
        let tmp = tempfile::tempdir().unwrap();
        let mut d = open_at(tmp.path());
        if !seed(&mut d, pad) {
            continue;
        }
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
        d.wal.flush().unwrap();

        let Err(e) = d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);") else { continue };
        assert_is_the_size_refusal(&e);
        refusals += 1;

        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
        d.wal.flush().unwrap();
        let primary_heap = d.heap("t");
        let primary_pages = d.file_pages();

        // Ship: the data file and the log, exactly as they stand.
        let rep = tempfile::tempdir().unwrap();
        std::fs::copy(tmp.path().join("alter.db"), rep.path().join("alter.db")).unwrap();
        std::fs::copy(tmp.path().join("alter.wal"), rep.path().join("alter.wal")).unwrap();
        let mut r = open_at(rep.path());
        recover(&r.txn).expect("replica redo");

        assert_eq!(
            primary_heap,
            r.heap("t"),
            "pad={pad}: the replica's heap bytes differ from the primary's after a refused ALTER \
             ({e})"
        );
        assert_eq!(
            diff_pages(&primary_pages, &r.file_pages()),
            Vec::<usize>::new(),
            "pad={pad}: the replica's data-file pages differ from the primary's"
        );
        drop(r);
        drop(d);
    }
    assert!(refusals > 0, "no refusing width; fixture is vacuous");
    eprintln!("[T5] widths that refused: {refusals}");
}
