//! LENS: is a MERGE that is refused for tuple size atomic on the target?
//!
//! Three snapshots per fixture, not two: T0 before the branch opens, T1 after the branch has
//! written rows and staged the edit but before `MERGE`, T2 after the refused `MERGE`. T0 vs T1
//! says whether branch isolation held; T1 vs T2 says what the refused statement did.

use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::{DataType, Value};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn db() -> Db {
    open_at(tempfile::tempdir().unwrap())
}

fn open_at(dir: tempfile::TempDir) -> Db {
    let path = dir.path().join("alter.db");
    let fresh = !path.exists();
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let catalog =
        if fresh { Catalog::create(bp.clone()).unwrap() } else { Catalog::open(bp.clone(), 1).unwrap() };
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { dir, catalog, wal, bp, txn, runtime, session }
}

/// Write everything through and reopen into a brand-new buffer pool and catalog.
fn reopen(d: Db) -> Db {
    d.txn.checkpoint().expect("checkpoint");
    d.bp.flush_all().unwrap();
    d.bp.disk_manager.sync().unwrap();
    let Db { dir, .. } = d;
    open_at(dir)
}

impl Db {
    fn exec(&mut self, sql: &str, session: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                p.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), session)
    }
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let mut s = std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out = self.exec(sql, &mut s);
        self.session = s;
        out
    }
    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }
    fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.try_sql(sql)))
            .map_err(|_| format!("`{sql}` PANICKED"))?
        {
            Ok(Outcome::Rows(r)) => Ok(r),
            Ok(_) => Err(format!("`{sql}` returned no rows")),
            Err(e) => Err(format!("`{sql}` errored: {e}")),
        }
    }
    fn shape(&self, t: &str) -> Vec<(String, DataType)> {
        self.catalog.get_table(t).unwrap().schema.columns.iter()
            .map(|c| (c.name.clone(), c.data_type.clone())).collect()
    }
    fn heap(&self, t: &str) -> Vec<(RecordId, Vec<u8>)> {
        let root = self.catalog.get_table(t).unwrap().first_directory_page_id;
        HeapFileManager::open(root, self.bp.clone()).scan()
            .map(|r| { let (rid, tup) = r.unwrap(); (rid, tup.data) }).collect()
    }
    fn by_key(&self, t: &str, keys: &[Value]) -> Vec<(Value, Option<RecordId>)> {
        let root = self.catalog.get_table(t).unwrap().primary_index_root;
        let ix = BPlusTreeManager::<Value, RecordId>::open(root, self.bp.clone());
        keys.iter().map(|k| (k.clone(), ix.search(k).unwrap())).collect()
    }
    fn ddl(&self) -> Vec<String> {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        ferrodb::replication::logical::LogicalDecoder::new(&self.catalog)
            .decode(&self.wal, self.wal.base_lsn.load(Ordering::SeqCst),
                    self.wal.next_lsn.load(Ordering::SeqCst))
            .expect("decode").schema_changes.iter().map(|(_, t, c)| format!("{t}:{c:?}")).collect()
    }
}

#[derive(Debug, PartialEq, Clone)]
struct Snap {
    shape: Vec<(String, DataType)>,
    heap: Vec<(RecordId, Vec<u8>)>,
    rows: Result<Vec<Vec<Value>>, String>,
    by_key: Vec<(Value, Option<RecordId>)>,
    ddl: Vec<String>,
}

fn snap(d: &mut Db) -> Snap {
    Snap {
        shape: d.shape("t"),
        heap: d.heap("t"),
        rows: d.rows(SEL),
        by_key: d.by_key("t", &keys()),
        ddl: d.ddl(),
    }
}

fn keys() -> Vec<Value> { vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)] }
const SEL: &str = "SELECT id, n FROM t;";
const REFUSED: &str = "ALTER TABLE t ALTER COLUMN n TYPE BIGINT;";

fn seed(d: &mut Db) {
    let big = "x".repeat(2014);
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));
}

fn is_size_refusal(e: &FerroError) -> bool {
    let m = e.to_string();
    ["would widen", "bytes a tuple can occupy", "Nothing has been written", "t"]
        .iter().all(|n| m.contains(n))
}

fn brief(s: &Snap) -> String {
    format!("rows={:?} slots={:?} by_key={:?} shape={:?} ddl={:?}",
        s.rows,
        s.heap.iter().map(|(r, b)| (r.page_id, r.slot_num, b.len())).collect::<Vec<_>>(),
        s.by_key, s.shape.iter().map(|(n, t)| format!("{n}:{t:?}")).collect::<Vec<_>>(), s.ddl)
}

// =============================================================================================
// 1. The core measurement. Does the target change across a MERGE that reports failure?
// =============================================================================================
#[test]
fn lens1_refused_merge_three_point() {
    let mut d = db();
    seed(&mut d);
    let t0 = snap(&mut d);

    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'lens-1';", &mut agent).expect("branch");
    d.exec("UPDATE t SET n = 999 WHERE id = 1;", &mut agent).expect("branch update");
    d.exec("INSERT INTO t VALUES (3, 300, 'small', 'small');", &mut agent).expect("branch insert");
    d.exec(REFUSED, &mut agent).expect("branch accepts the edit");
    let t1 = snap(&mut d);

    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
    assert!(is_size_refusal(&e), "refused for the wrong reason: {e}");
    let t2 = snap(&mut d);

    eprintln!("LENS1 refusal   = {e}");
    eprintln!("LENS1 T0 (pre-branch) = {}", brief(&t0));
    eprintln!("LENS1 T1 (pre-merge)  = {}", brief(&t1));
    eprintln!("LENS1 T2 (post-fail)  = {}", brief(&t2));
    eprintln!("LENS1 isolation held (T0==T1)? {}", t0 == t1);
    eprintln!("LENS1 target unchanged across the refused MERGE (T1==T2)? {}", t1 == t2);
    eprintln!("LENS1 shape unchanged? {}  ddl unchanged? {}", t1.shape == t2.shape, t1.ddl == t2.ddl);
    // Discriminator (b): is every pre-existing tuple still present, byte for byte, at the same rid?
    let lost: Vec<_> = t1.heap.iter().filter(|x| !t2.heap.contains(x)).collect();
    let gained: Vec<_> = t2.heap.iter().filter(|x| !t1.heap.contains(x))
        .map(|(r, b)| (r.page_id, r.slot_num, b.len())).collect();
    eprintln!("LENS1 pre-existing tuples LOST or moved = {:?}",
        lost.iter().map(|(r, b)| (r.page_id, r.slot_num, b.len())).collect::<Vec<_>>());
    eprintln!("LENS1 tuples GAINED = {gained:?}");
    eprintln!("LENS1 full rows after = {:?}", d.rows("SELECT id, n, a FROM t;"));

    assert_eq!(t1, t2, "MERGE reported failure ({e}) but the target changed");
}

// =============================================================================================
// 2. Control: the same branch writes with NO staged ALTER. Are these rows the branch's own?
// =============================================================================================
#[test]
fn lens2_control_same_writes_without_the_alter() {
    let mut d = db();
    seed(&mut d);
    let t1 = snap(&mut d);

    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'lens-2';", &mut agent).expect("branch");
    d.exec("UPDATE t SET n = 999 WHERE id = 1;", &mut agent).expect("branch update");
    d.exec("INSERT INTO t VALUES (3, 300, 'small', 'small');", &mut agent).expect("branch insert");
    let ok = d.exec("MERGE;", &mut agent);
    let t2 = snap(&mut d);
    eprintln!("LENS2 merge ok?       = {:?}", ok.is_ok());
    eprintln!("LENS2 T1 (pre-merge)  = {}", brief(&t1));
    eprintln!("LENS2 T2 (post-merge) = {}", brief(&t2));
    eprintln!("LENS2 full rows after = {:?}", d.rows("SELECT id, n, a FROM t;"));
}

// =============================================================================================
// 3. Control: the CLAIM under attack. A plain refused ALTER, no MERGE anywhere.
// =============================================================================================
#[test]
fn lens3_control_plain_refused_alter_changes_nothing() {
    let mut d = db();
    seed(&mut d);
    let before = snap(&mut d);
    let e = d.try_sql(REFUSED).err().expect("plain ALTER must be refused");
    assert!(is_size_refusal(&e), "wrong refusal: {e}");
    let after = snap(&mut d);
    eprintln!("LENS3 before = {}", brief(&before));
    eprintln!("LENS3 after  = {}", brief(&after));
    assert_eq!(before, after, "a plain refused ALTER changed the table ({e})");

    let mut r = reopen(d);
    let reopened = snap(&mut r);
    eprintln!("LENS3 reopened = {}", brief(&reopened));
    assert_eq!(before, reopened, "a plain refused ALTER changed the table durably ({e})");
}

// =============================================================================================
// 4. Is whatever the refused MERGE left durable? Reopen into a fresh buffer pool.
// =============================================================================================
#[test]
fn lens4_refused_merge_durability_and_retry() {
    let mut d = db();
    seed(&mut d);
    let t1 = snap(&mut d);

    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'lens-4';", &mut agent).expect("branch");
    d.exec("INSERT INTO t VALUES (3, 300, 'small', 'small');", &mut agent).expect("branch insert");
    d.exec(REFUSED, &mut agent).expect("branch accepts the edit");
    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
    assert!(is_size_refusal(&e), "wrong refusal: {e}");
    let t2 = snap(&mut d);

    // A retry of the same MERGE: does it double-apply the rows already on the target?
    let e2 = d.exec("MERGE;", &mut agent);
    let t3 = snap(&mut d);
    eprintln!("LENS4 retry MERGE = {:?}", e2.as_ref().err().map(|x| x.to_string()));
    eprintln!("LENS4 T1 pre-merge   = {}", brief(&t1));
    eprintln!("LENS4 T2 post-fail   = {}", brief(&t2));
    eprintln!("LENS4 T3 post-retry  = {}", brief(&t3));

    let mut r = reopen(d);
    let t4 = snap(&mut r);
    eprintln!("LENS4 T4 reopened    = {}", brief(&t4));
    eprintln!("LENS4 durable change across the refused MERGE (T1 vs T4 rows)? before={:?} after={:?}",
        t1.rows, t4.rows);
    assert_eq!(t1.rows, t4.rows, "a refused MERGE changed the target durably");
}
