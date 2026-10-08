//! LENS: skeptical re-derivation of the ATK4 "E" claim.
//!
//! Claim: a MERGE that reports failure leaves every live tuple's raw heap bytes changed,
//! because the first staged edit (ADD COLUMN) rewrites the heap and is committed before the
//! second (retype) is refused.
//!
//! What this file measures, on the committed tree, with its own fixture:
//!   1. does the MERGE really report failure, and does the shape/heap really change?
//!   2. is the resulting state EXACTLY the state that "ALTER ADD (ok); ALTER RETYPE (refused)"
//!      as two plain statements produces?  (i.e. is anything LOST, or is it only that MERGE
//!      is not atomic across two DDL edits?)
//!   3. does the *refused ALTER statement itself* change one byte of the heap?
//!   4. does every value survive a flush + reopen into a FRESH buffer pool?

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
    #[allow(dead_code)]
    dir: tempfile::TempDir,
    path: std::path::PathBuf,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alter.db");
    let file =
        std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let catalog = Catalog::create(bp.clone()).unwrap();
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { dir, path, catalog, wal, bp, txn, runtime, session }
}

impl Db {
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let mut session =
            std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out = self.exec(sql, &mut session);
        self.session = session;
        out
    }
    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }
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
    fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        match self.try_sql(sql) {
            Ok(Outcome::Rows(r)) => Ok(r),
            Ok(_) => Err(format!("`{sql}` did not return rows")),
            Err(e) => Err(format!("`{sql}` errored: {e}")),
        }
    }
    fn shape(&self, table: &str) -> Vec<(String, DataType)> {
        self.catalog.get_table(table).unwrap().schema.columns.iter()
            .map(|c| (c.name.clone(), c.data_type.clone())).collect()
    }
    fn heap(&self, table: &str) -> Vec<(RecordId, Vec<u8>)> {
        let root = self.catalog.get_table(table).unwrap().first_directory_page_id;
        HeapFileManager::open(root, self.bp.clone())
            .scan().map(|r| { let (rid, t) = r.unwrap(); (rid, t.data) }).collect()
    }
    fn ddl(&self) -> Vec<String> {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        ferrodb::replication::logical::LogicalDecoder::new(&self.catalog)
            .decode(&self.wal, self.wal.base_lsn.load(Ordering::SeqCst), self.wal.next_lsn.load(Ordering::SeqCst))
            .expect("decode").schema_changes.iter().map(|(_, t, c)| format!("{t}:{c:?}")).collect()
    }
    fn by_key(&self, table: &str, keys: &[Value]) -> Vec<(Value, Option<RecordId>)> {
        let root = self.catalog.get_table(table).unwrap().primary_index_root;
        let ix = BPlusTreeManager::<Value, RecordId>::open(root, self.bp.clone());
        keys.iter().map(|k| (k.clone(), ix.search(k).unwrap())).collect()
    }
    /// Flush, sync, and reopen the SAME file through a brand-new buffer pool + catalog.
    fn reopen_fresh(&mut self) -> (Vec<String>, Vec<(RecordId, Vec<u8>)>) {
        self.bp.flush_all().unwrap();
        self.bp.disk_manager.sync().unwrap();
        let file = std::fs::OpenOptions::new().read(true).write(true).open(&self.path).unwrap();
        let bp2 = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let cat2 = Catalog::open(bp2.clone(), 1).unwrap();
        let e = cat2.get_table("t").unwrap();
        let shape = e.schema.columns.iter().map(|c| format!("{}:{:?}", c.name, c.data_type)).collect();
        let heap = HeapFileManager::open(e.first_directory_page_id, bp2.clone())
            .scan().map(|r| { let (rid, t) = r.unwrap(); (rid, t.data) }).collect();
        (shape, heap)
    }
}

#[derive(Debug, PartialEq)]
struct Snap {
    shape: Vec<(String, DataType)>,
    heap: Vec<(RecordId, Vec<u8>)>,
    rows: Result<Vec<Vec<Value>>, String>,
    by_key: Vec<(Value, Option<RecordId>)>,
}
fn snap(d: &mut Db) -> Snap {
    Snap {
        shape: d.shape("t"),
        heap: d.heap("t"),
        rows: d.rows(SEL),
        by_key: d.by_key("t", &KEYS),
    }
}

const ADD: &str = "ALTER TABLE t ADD COLUMN c1 VARCHAR(20);";
const RETYPE: &str = "ALTER TABLE t ALTER COLUMN n TYPE BIGINT;";
const SEL: &str = "SELECT id, n, a, b FROM t;";
const KEYS: [Value; 2] = [Value::Integer(1), Value::Integer(2)];

fn try_seed(d: &mut Db, w: usize) -> Result<(), FerroError> {
    let big = "x".repeat(w);
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    d.try_sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');")).map(|_| ())
}
fn seed(d: &mut Db, w: usize) {
    try_seed(d, w).unwrap_or_else(|e| panic!("seed at w={w} failed: {e}"));
}

/// Find the widths where ADD fits and RETYPE behind it does not. Loud if none.
fn probe_widths() -> Vec<usize> {
    let mut hit = Vec::new();
    for w in 1990..2020usize {
        let mut p = db();
        if try_seed(&mut p, w).is_err() { eprintln!("=== w={w}: seed itself refused, skipping"); continue; }
        if p.try_sql(ADD).is_err() { continue; }
        if p.try_sql(RETYPE).is_ok() { continue; }
        hit.push(w);
    }
    hit
}

#[test]
fn lens_the_refused_merge_state_equals_the_plain_sequential_state() {
    let widths = probe_widths();
    eprintln!("=== widths where ADD fits and RETYPE is refused: {widths:?}");
    assert!(!widths.is_empty(), "the sweep never reached the condition");

    for w in &widths {
        let w = *w;

        // ---- Route 1: through MERGE, two staged edits, second refused.
        let mut m = db();
        seed(&mut m, w);
        let before = snap(&mut m);
        let mut agent = Session::with_runtime(m.runtime.clone());
        m.exec("BEGIN AGENT SESSION AS 'agent-e';", &mut agent).expect("branch");
        m.exec(ADD, &mut agent).expect("stage add");
        m.exec(RETYPE, &mut agent).expect("stage retype");
        let merr = m.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
        let after_merge = snap(&mut m);

        // ---- Route 2: the same two edits as PLAIN statements, in order.
        let mut s = db();
        seed(&mut s, w);
        let s_add = s.try_sql(ADD);
        let s_ret = s.try_sql(RETYPE);
        let after_seq = snap(&mut s);

        eprintln!("--- w={w} MERGE err        = {merr}");
        eprintln!("--- w={w} plain ADD        = {:?}", s_add.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
        eprintln!("--- w={w} plain RETYPE     = {:?}", s_ret.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
        eprintln!("--- w={w} shape before     = {:?}", before.shape.iter().map(|c| c.0.clone()).collect::<Vec<_>>());
        eprintln!("--- w={w} shape after MERGE= {:?}", after_merge.shape.iter().map(|c| c.0.clone()).collect::<Vec<_>>());
        eprintln!("--- w={w} shape after SEQ  = {:?}", after_seq.shape.iter().map(|c| c.0.clone()).collect::<Vec<_>>());
        eprintln!("--- w={w} heap lens before = {:?}", before.heap.iter().map(|(_, b)| b.len()).collect::<Vec<_>>());
        eprintln!("--- w={w} heap lens MERGE  = {:?}", after_merge.heap.iter().map(|(_, b)| b.len()).collect::<Vec<_>>());
        eprintln!("--- w={w} heap lens SEQ    = {:?}", after_seq.heap.iter().map(|(_, b)| b.len()).collect::<Vec<_>>());
        eprintln!("--- w={w} rows after MERGE = {:?}", after_merge.rows.as_ref().map(|r| r.iter().map(|row| format!("{:?}|{:?}|len{}|len{}", row[0], row[1],
            match &row[2] { Value::Varchar(s) => s.len(), o => format!("{o:?}").len() },
            match &row[3] { Value::Varchar(s) => s.len(), o => format!("{o:?}").len() })).collect::<Vec<_>>()));
        eprintln!("--- w={w} rows after SEQ   = {:?}", after_seq.rows.as_ref().map(|r| r.len()));
        eprintln!("--- w={w} by_key MERGE     = {:?}", after_merge.by_key);
        let ddl_merge = m.ddl();
        let ddl_seq = s.ddl();
        eprintln!("--- w={w} ddl after MERGE  = {ddl_merge:?}");
        eprintln!("--- w={w} ddl after SEQ    = {ddl_seq:?}");
        eprintln!("--- w={w} ddl identical    = {}", ddl_merge == ddl_seq);
        eprintln!("--- w={w} MERGE state == plain-sequential state? {}", after_merge == after_seq);
        assert_eq!(ddl_merge, ddl_seq, "the two routes emitted different change-feed DDL (w={w})");

        assert!(s_add.is_ok(), "the plain ADD must succeed at w={w}");
        assert!(s_ret.is_err(), "the plain RETYPE must be refused at w={w}");
        // The decisive question: is anything LOST relative to "first edit landed, second refused"?
        assert_eq!(after_merge, after_seq,
            "the refused MERGE left a state that plain sequential statements do not produce (w={w})");
    }
}

#[test]
fn lens_the_refused_alter_statement_itself_changes_nothing() {
    let widths = probe_widths();
    assert!(!widths.is_empty(), "the sweep never reached the condition");
    for w in &widths {
        let w = *w;
        let mut d = db();
        seed(&mut d, w);
        d.sql(ADD); // the heap rewrite that legitimately succeeds
        let before = snap(&mut d);
        let (disk_shape_b, disk_heap_b) = d.reopen_fresh();
        let e = d.try_sql(RETYPE).err().expect("RETYPE must be refused");
        let after = snap(&mut d);
        let (disk_shape_a, disk_heap_a) = d.reopen_fresh();
        eprintln!("--- w={w} refusal = {e}");
        eprintln!("--- w={w} in-mem identical  = {}", before == after);
        eprintln!("--- w={w} on-disk shape identical = {}", disk_shape_b == disk_shape_a);
        eprintln!("--- w={w} on-disk heap  identical = {}", disk_heap_b == disk_heap_a);
        eprintln!("--- w={w} on-disk shape after refusal = {disk_shape_a:?}");
        eprintln!("--- w={w} on-disk heap lens after refusal = {:?}", disk_heap_a.iter().map(|(_, b)| b.len()).collect::<Vec<_>>());
        assert_eq!(before, after, "the REFUSED ALTER changed the table in memory (w={w})");
        assert_eq!(disk_shape_b, disk_shape_a, "the REFUSED ALTER changed the on-disk shape (w={w})");
        assert_eq!(disk_heap_b, disk_heap_a, "the REFUSED ALTER changed the on-disk heap (w={w})");
    }
}

#[test]
fn lens_every_value_survives_the_refused_merge_and_a_fresh_reopen() {
    let widths = probe_widths();
    assert!(!widths.is_empty(), "the sweep never reached the condition");
    for w in &widths {
        let w = *w;
        let big = "x".repeat(w);
        let mut d = db();
        seed(&mut d, w);
        let mut agent = Session::with_runtime(d.runtime.clone());
        d.exec("BEGIN AGENT SESSION AS 'agent-v';", &mut agent).expect("branch");
        d.exec(ADD, &mut agent).expect("stage add");
        d.exec(RETYPE, &mut agent).expect("stage retype");
        let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
        eprintln!("--- w={w} refusal = {e}");
        let rows = d.rows("SELECT id, n, a, b, c1 FROM t;").expect("table still readable");
        eprintln!("--- w={w} row count = {}", rows.len());
        assert_eq!(rows.len(), 2, "a row was lost (w={w})");
        assert_eq!(rows[0][0], Value::Integer(1));
        assert_eq!(rows[0][1], Value::Integer(100));
        assert_eq!(rows[0][2], Value::Varchar("small".into()));
        assert_eq!(rows[0][3], Value::Varchar("small".into()));
        assert_eq!(rows[0][4], Value::Null, "the added column is not NULL");
        assert_eq!(rows[1][0], Value::Integer(2));
        assert_eq!(rows[1][1], Value::Integer(200));
        assert_eq!(rows[1][2], Value::Varchar(big.clone()), "the wide value changed");
        assert_eq!(rows[1][3], Value::Varchar(big.clone()), "the wide value changed");
        assert_eq!(rows[1][4], Value::Null);
        let (disk_shape, disk_heap) = d.reopen_fresh();
        eprintln!("--- w={w} fresh-pool shape = {disk_shape:?}");
        eprintln!("--- w={w} fresh-pool heap lens = {:?}", disk_heap.iter().map(|(_, b)| b.len()).collect::<Vec<_>>());
        assert_eq!(disk_heap.len(), 2, "a row was lost after reopen (w={w})");
        // The refused retype must NOT be on disk.
        assert!(disk_shape.contains(&"n:Integer".to_string()),
            "the refused retype reached disk: {disk_shape:?}");
    }
}
