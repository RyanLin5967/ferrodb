//! ATK3 — durability and recovery around a REFUSED `ALTER TABLE`.
//!
//! Attacking the claim: "an ALTER that is refused leaves the table EXACTLY as it was ... and that
//! holds after a checkpoint, a flush and a reopen into a fresh buffer pool."
//!
//! The angles here are the ones the committed suite does NOT cover: a crash with no clean flush
//! (drop the handles, reopen, run `recover`), a checkpoint that TRUNCATES the WAL, a whole-file
//! byte comparison of the database and the WAL, the retained schema declaration, the statistics,
//! the change feed, a physical replica following the primary, and a successful ALTER run after a
//! refused one.

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
use ferrodb::replication::{ReplicaApplier, ReplicationSource};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::storage::heap_page::Page;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    path: PathBuf,
    wal_path: PathBuf,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

/// Open (or create) the database in `dir`. `do_recover` runs `wal::recovery::recover` the way a
/// restart after a crash does.
fn open(dir: &Path, do_recover: bool) -> Db {
    let path = dir.join("atk3.db");
    let wal_path = dir.join("atk3.wal");
    let fresh = !path.exists();
    let file =
        std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = if fresh {
        Catalog::create(bp.clone()).unwrap()
    } else {
        Catalog::open(bp.clone(), 1).unwrap()
    };
    let wal = Arc::new(WalManager::new(wal_path.clone()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    if do_recover {
        let did = ferrodb::wal::recovery::recover(&txn).expect("recover");
        eprintln!("  recover() -> {did}");
    }
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { path, wal_path, catalog, wal, bp, txn, runtime, session }
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

    fn dir_root(&self, table: &str) -> u32 {
        self.catalog.get_table(table).unwrap().first_directory_page_id
    }

    fn heap(&self, table: &str) -> Vec<(RecordId, Vec<u8>)> {
        HeapFileManager::open(self.dir_root(table), self.bp.clone())
            .scan()
            .map(|r| {
                let (rid, tuple) = r.unwrap();
                (rid, tuple.data)
            })
            .collect()
    }

    fn by_key(&self, table: &str, keys: &[Value]) -> Vec<(Value, Option<RecordId>)> {
        let root = self.catalog.get_table(table).unwrap().primary_index_root;
        let ix = BPlusTreeManager::<Value, RecordId>::open(root, self.bp.clone());
        keys.iter().map(|k| (k.clone(), ix.search(k).unwrap())).collect()
    }

    /// The whole change feed, as a debug string: events, schema changes, and the report fields.
    fn feed(&self) -> String {
        self.wal.flush().unwrap();
        let d = ferrodb::replication::logical::LogicalDecoder::new(&self.catalog)
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode");
        format!("{d:?}")
    }

    fn stats(&self, table: &str) -> String {
        format!("{:?}", self.catalog.table_stats(table))
    }

    /// Everything durable, made durable first: flush, sync, then the raw bytes of both files.
    fn durable_bytes(&self) -> (Vec<u8>, Vec<u8>) {
        self.bp.flush_all().unwrap();
        self.bp.disk_manager.sync().unwrap();
        self.wal.flush().unwrap();
        (std::fs::read(&self.path).unwrap(), std::fs::read(&self.wal_path).unwrap())
    }
}

/// Raw bytes of the given pages, straight out of whichever buffer pool is handed in.
fn page_bytes(bp: &Arc<BufferPoolManager>, ids: &[u32]) -> Vec<(u32, Vec<u8>)> {
    ids.iter()
        .map(|&id| {
            let idx = bp.fetch_page(id).unwrap_or_else(|e| panic!("fetch {id}: {e}"));
            let data = bp.frames[idx].read().unwrap().data.to_vec();
            bp.unpin_page(id, false);
            (id, data)
        })
        .collect()
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    shape: Vec<(String, DataType)>,
    heap: Vec<(RecordId, Vec<u8>)>,
    rows: Result<Vec<Vec<Value>>, String>,
    by_key: Vec<(Value, Option<RecordId>)>,
}

fn snapshot(d: &mut Db, table: &str, select: &str, keys: &[Value]) -> Snapshot {
    Snapshot {
        shape: d.shape(table),
        heap: d.heap(table),
        rows: d.rows(select),
        by_key: d.by_key(table, keys),
    }
}

/// Values only — safe to compare across two different databases, where a `RecordId` or a version
/// header need not match.
fn values_only(d: &mut Db, table: &str, select: &str) -> (Vec<(String, DataType)>, Vec<Vec<Value>>) {
    let mut rows = d.rows(select).expect("the table must still read");
    rows.sort_by_key(|r| format!("{:?}", r[0]));
    (d.shape(table), rows)
}

fn assert_is_the_size_refusal(e: &FerroError, table: &str) {
    let msg = e.to_string();
    for needle in ["would widen", "bytes a tuple can occupy", "Nothing has been written", table] {
        assert!(msg.contains(needle), "refused, but not by the size precheck: {msg}");
    }
}

const SELECT: &str = "SELECT id, n FROM t;";

/// The fixture. One row comfortably small, one within a few bytes of the tuple limit, so
/// `n INTEGER -> BIGINT` pushes the wide row past `MAX_TUPLE_SIZE`.
fn build(d: &mut Db) {
    let big = "x".repeat(2014);
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));
    d.sql("INSERT INTO t VALUES (3, 300, 'third', 'third');");
    d.sql("ANALYZE t;");
}

fn refuse(d: &mut Db) {
    let e = d
        .try_sql("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;")
        .err()
        .expect("the fixture must produce a REFUSED alter, or it measures nothing");
    assert_is_the_size_refusal(&e, "t");
    eprintln!("  refusal: {e}");
}

// -------------------------------------------------------------------------------------------
// 1. A crash with NO clean flush: drop the handles, reopen, run recovery.
// -------------------------------------------------------------------------------------------

#[test]
fn a_refused_alter_survives_a_crash_with_no_clean_flush() {
    let keys = [Value::Integer(1), Value::Integer(2), Value::Integer(3)];

    // The run that attempts the ALTER.
    let attempt_dir = tempfile::tempdir().unwrap();
    let before = {
        let mut d = open(attempt_dir.path(), false);
        build(&mut d);
        let before = snapshot(&mut d, "t", SELECT, &keys);
        refuse(&mut d);
        // NO checkpoint, NO flush_all, NO sync. Every dirty page in this buffer pool is discarded
        // when `d` goes out of scope, which is what a crash does.
        before
    };
    let mut r = open(attempt_dir.path(), true);
    let after = snapshot(&mut r, "t", SELECT, &keys);
    assert_eq!(
        before, after,
        "a REFUSED ALTER changed the table across a crash with no clean flush"
    );

    // Control: the identical script with the ALTER never attempted. Compared on values and shape,
    // not raw bytes, because two databases need not agree on record ids or version headers.
    let control_dir = tempfile::tempdir().unwrap();
    {
        let mut c = open(control_dir.path(), false);
        build(&mut c);
    }
    let mut cr = open(control_dir.path(), true);
    assert_eq!(
        values_only(&mut cr, "t", SELECT),
        values_only(&mut r, "t", SELECT),
        "after a crash, the database that attempted a refused ALTER does not read back the same \
         as one that never attempted it"
    );
}

// -------------------------------------------------------------------------------------------
// 2. A checkpoint TRUNCATES the WAL. If the refusal left a page dirty-flag clear that should not
//    be, the rows are gone and the log no longer holds them.
// -------------------------------------------------------------------------------------------

#[test]
fn a_refused_alter_survives_a_checkpoint_that_truncates_the_log() {
    let keys = [Value::Integer(1), Value::Integer(2), Value::Integer(3)];
    let dir = tempfile::tempdir().unwrap();
    let (before, retained_before, stats_before) = {
        let mut d = open(dir.path(), false);
        build(&mut d);
        let before = snapshot(&mut d, "t", SELECT, &keys);
        let retained_before = d.txn.retained_shape(d.dir_root("t"));
        let stats_before = d.stats("t");
        refuse(&mut d);
        assert_eq!(
            d.txn.retained_shape(d.dir_root("t")),
            retained_before,
            "a REFUSED ALTER changed the table's retained schema declaration"
        );
        assert_eq!(d.stats("t"), stats_before, "a REFUSED ALTER changed the statistics");
        d.txn.checkpoint().expect("checkpoint");
        (before, retained_before, stats_before)
    };
    let mut r = open(dir.path(), true);
    assert_eq!(
        before,
        snapshot(&mut r, "t", SELECT, &keys),
        "a REFUSED ALTER changed the table across checkpoint + crash + replay"
    );
    assert_eq!(
        r.txn.retained_shape(r.dir_root("t")),
        retained_before,
        "the retained declaration is different after the checkpoint replayed it"
    );
    // Statistics live in the catalog, so they must survive the reopen too.
    assert_eq!(r.stats("t"), stats_before, "the statistics did not survive the reopen unchanged");
}

// -------------------------------------------------------------------------------------------
// 3. Whole-file byte comparison of the database AND the WAL across a refusal.
// -------------------------------------------------------------------------------------------

#[test]
fn a_refused_alter_changes_no_byte_of_the_database_file_or_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let mut d = open(dir.path(), false);
    build(&mut d);
    let feed_before = d.feed();
    let (db_before, wal_before) = d.durable_bytes();

    refuse(&mut d);

    let (db_after, wal_after) = d.durable_bytes();
    let feed_after = d.feed();

    assert_eq!(db_before.len(), db_after.len(), "the database FILE changed size across a refusal");
    let diff: Vec<usize> = db_before
        .iter()
        .zip(db_after.iter())
        .enumerate()
        .filter(|(_, (a, b))| a != b)
        .map(|(i, _)| i)
        .collect();
    assert!(
        diff.is_empty(),
        "a REFUSED ALTER changed {} byte(s) of the database file, first at offset {} (page {})",
        diff.len(),
        diff[0],
        diff[0] / 4096
    );
    assert_eq!(
        wal_before.len(),
        wal_after.len(),
        "a REFUSED ALTER appended {} byte(s) to the WAL",
        wal_after.len().saturating_sub(wal_before.len())
    );
    assert!(wal_before == wal_after, "a REFUSED ALTER rewrote WAL bytes in place");
    assert_eq!(feed_before, feed_after, "a REFUSED ALTER changed what the change feed carries");
}

// -------------------------------------------------------------------------------------------
// 4. A refused ALTER followed by ALTERs that must succeed.
// -------------------------------------------------------------------------------------------

#[test]
fn a_refused_alter_does_not_poison_the_alters_that_follow_it() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut d = open(dir.path(), false);
        build(&mut d);
        refuse(&mut d);

        // (a) A rename persists the catalog on its own, with no heap work at all.
        d.sql("ALTER TABLE t RENAME COLUMN n TO m;");
        assert_eq!(d.shape("t")[1].0, "m", "the rename after a refusal did not land");

        // (b) An ADD COLUMN whose widening every row survives.
        d.sql("ALTER TABLE t ADD COLUMN tag INTEGER;");

        // (c) The refused retype, once the row that blocked it is narrowed.
        d.sql("UPDATE t SET b = 'narrow' WHERE id = 2;");
        d.sql("ALTER TABLE t ALTER COLUMN m TYPE BIGINT;");
        d.txn.checkpoint().expect("checkpoint");
    }
    let mut r = open(dir.path(), true);
    assert_eq!(
        r.shape("t"),
        vec![
            ("id".to_string(), DataType::Integer),
            ("m".to_string(), DataType::BigInt),
            ("a".to_string(), DataType::Varchar(2100)),
            ("b".to_string(), DataType::Varchar(2100)),
            ("tag".to_string(), DataType::Integer),
        ],
        "the shape after refusal + rename + add + retype is not what the three statements asked for"
    );
    let mut got = r.rows("SELECT id, m, tag FROM t;").expect("the table must still read");
    got.sort_by_key(|row| format!("{:?}", row[0]));
    assert_eq!(
        got,
        vec![
            vec![Value::Integer(1), Value::BigInt(100), Value::Null],
            vec![Value::Integer(2), Value::BigInt(200), Value::Null],
            vec![Value::Integer(3), Value::BigInt(300), Value::Null],
        ],
        "the ALTERs that ran after the refusal did not carry every row across"
    );
    assert_eq!(
        r.by_key("t", &[Value::Integer(1), Value::Integer(2), Value::Integer(3)])
            .iter()
            .filter(|(_, rid)| rid.is_some())
            .count(),
        3,
        "the primary index lost a key across the ALTERs that followed the refusal"
    );
}

// -------------------------------------------------------------------------------------------
// 5. A physical replica following the primary.
// -------------------------------------------------------------------------------------------

/// The replica has its own file and its own buffer pool, and receives only shipped WAL frames.
///
/// The refusal must ship nothing and change nothing. The positive control at the end is the
/// anti-vacuity check: a SUCCESSFUL alter must be visible to this instrument — and it is visible
/// as primary pages that changed with no frames shipped, which is the documented consequence of
/// the rewrite being unlogged.
#[test]
fn a_refused_alter_ships_nothing_to_a_physical_replica() {
    let dir = tempfile::tempdir().unwrap();
    let rep_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("replica.db"))
        .unwrap();
    let replica_bp =
        Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(rep_file).unwrap())));

    let mut d = open(dir.path(), false);
    let src_start = ReplicationSource::new(&d.wal).start_lsn();
    let applier = ReplicaApplier::new(Arc::clone(&replica_bp), src_start);

    build(&mut d);

    let ship = |applier: &ReplicaApplier, wal: &Arc<WalManager>| -> usize {
        wal.flush().unwrap();
        let src = ReplicationSource::new(wal);
        let (bytes, next) = src.read_from(applier.applied_lsn(), 1 << 20).expect("read");
        if bytes.is_empty() {
            return 0;
        }
        applier.apply(next - bytes.len() as u64, &bytes).expect("apply");
        bytes.len()
    };
    let n = ship(&applier, &d.wal);
    assert!(n > 0, "the fixture shipped nothing at all, so it measures nothing");

    // The heap's data pages, named from the primary and read on both sides.
    let heap_pages: Vec<u32> = {
        let mut ids: Vec<u32> = d.heap("t").iter().map(|(rid, _)| rid.page_id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };
    assert!(!heap_pages.is_empty(), "no heap pages to compare");
    eprintln!("  heap data pages: {heap_pages:?}");

    let primary_before = page_bytes(&d.bp, &heap_pages);
    let replica_before = page_bytes(&replica_bp, &heap_pages);
    let lsn_before = applier.applied_lsn();

    refuse(&mut d);

    let shipped = ship(&applier, &d.wal);
    assert_eq!(shipped, 0, "a REFUSED ALTER shipped {shipped} byte(s) of WAL to the replica");
    assert_eq!(applier.applied_lsn(), lsn_before, "the replica advanced across a refusal");
    assert_eq!(
        primary_before,
        page_bytes(&d.bp, &heap_pages),
        "a REFUSED ALTER changed the primary's heap page bytes"
    );
    assert_eq!(
        replica_before,
        page_bytes(&replica_bp, &heap_pages),
        "a REFUSED ALTER changed the replica's heap page bytes"
    );

    // Anti-vacuity: the instrument must be able to see a change. A SUCCESSFUL alter is unlogged,
    // so the primary's pages move and the replica is shipped nothing.
    d.sql("UPDATE t SET b = 'narrow' WHERE id = 2;");
    let _ = ship(&applier, &d.wal);
    let primary_mid = page_bytes(&d.bp, &heap_pages);
    let replica_mid = page_bytes(&replica_bp, &heap_pages);
    d.sql("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;");
    let shipped_by_success = ship(&applier, &d.wal);
    let primary_end = page_bytes(&d.bp, &heap_pages);
    let replica_end = page_bytes(&replica_bp, &heap_pages);
    assert_ne!(
        primary_mid, primary_end,
        "a SUCCESSFUL alter changed no primary page byte either — the instrument is blind, so the \
         refusal comparisons above prove nothing"
    );
    eprintln!(
        "  successful alter: primary pages changed, {shipped_by_success} WAL byte(s) shipped, \
         replica pages changed = {}",
        replica_mid != replica_end
    );
}

// -------------------------------------------------------------------------------------------
// 6. Reading the table through the change feed after a refusal, then again after a reopen.
// -------------------------------------------------------------------------------------------

#[test]
fn the_change_feed_reads_the_same_table_before_and_after_a_refusal_and_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let feed_before;
    let feed_after_refusal;
    {
        let mut d = open(dir.path(), false);
        build(&mut d);
        feed_before = d.feed();
        refuse(&mut d);
        feed_after_refusal = d.feed();
        assert_eq!(feed_before, feed_after_refusal, "the refusal changed the feed in-process");
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
    }
    let r = open(dir.path(), true);
    // Not the same string as before — recovery re-reads the same log through a fresh decoder — so
    // the assertion is that the *decoded content* is the same, with the log window unchanged.
    let feed_reopened = r.feed();
    assert_eq!(
        feed_before, feed_reopened,
        "the change feed decodes a different table after a refusal and a reopen"
    );
    // And no DDL record for a column change anywhere in it.
    assert!(
        !feed_reopened.contains("Retype") && !feed_reopened.contains("AlterColumn"),
        "a REFUSED ALTER left a column-level DDL record on the feed: {feed_reopened}"
    );
}
