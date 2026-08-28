//! ATK4: routes into the heap rewrite other than a plain `ALTER TABLE`.
//!
//! The claim under attack: "an ALTER that is refused leaves the table EXACTLY as it was".
//! These fixtures reach `Catalog::alter_table` through `AgentRuntime::merge` instead.

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
    Db { dir, catalog, wal, bp, txn, runtime, session }
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
                let (rid, tuple) = r.unwrap();
                (rid, tuple.data)
            })
            .collect()
    }
    fn schema_changes(&self) -> Vec<String> {
        use std::sync::atomic::Ordering;
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
    fn by_key(&self, table: &str, keys: &[Value]) -> Vec<(Value, Option<RecordId>)> {
        let root = self.catalog.get_table(table).unwrap().primary_index_root;
        let ix = BPlusTreeManager::<Value, RecordId>::open(root, self.bp.clone());
        keys.iter().map(|k| (k.clone(), ix.search(k).unwrap())).collect()
    }
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    shape: Vec<(String, DataType)>,
    heap: Vec<(RecordId, Vec<u8>)>,
    rows: Result<Vec<Vec<Value>>, String>,
    by_key: Vec<(Value, Option<RecordId>)>,
    ddl: Vec<String>,
}

fn snapshot(d: &mut Db, table: &str, select: &str, keys: &[Value]) -> Snapshot {
    Snapshot {
        shape: d.shape(table),
        heap: d.heap(table),
        rows: d.rows(select),
        by_key: d.by_key(table, keys),
        ddl: d.schema_changes(),
    }
}

fn assert_is_the_size_refusal(e: &FerroError, table: &str) {
    let msg = e.to_string();
    for needle in ["would widen", "bytes a tuple can occupy", "Nothing has been written", table] {
        assert!(
            msg.contains(needle),
            "refused, but not by the size precheck — no {needle:?} in: {msg}"
        );
    }
}

/// The fixture the existing suite proved reaches the size refusal.
fn seed(d: &mut Db) {
    let big = "x".repeat(2014);
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));
}

const REFUSED: &str = "ALTER TABLE t ALTER COLUMN n TYPE BIGINT;";
const KEYS: [Value; 3] = [Value::Integer(1), Value::Integer(2), Value::Integer(3)];
const SEL: &str = "SELECT id, n FROM t;";

// -------------------------------------------------------------------------------------------
// A. Rows a branch wrote are published and COMMITTED before the schema edit is even attempted.
// -------------------------------------------------------------------------------------------
#[test]
fn a_merge_refused_for_size_still_publishes_the_branch_rows() {
    let mut d = db();
    seed(&mut d);
    let before = snapshot(&mut d, "t", SEL, &KEYS);

    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut agent).expect("branch");
    d.exec("UPDATE t SET n = 999 WHERE id = 1;", &mut agent).expect("update on branch");
    d.exec("INSERT INTO t VALUES (3, 300, 'small', 'small');", &mut agent).expect("insert");
    d.exec(REFUSED, &mut agent).expect("the branch accepts the edit");

    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
    assert_is_the_size_refusal(&e, "t");

    let after = snapshot(&mut d, "t", SEL, &KEYS);
    eprintln!("--- A: refusal = {e}");
    eprintln!("--- A: rows BEFORE merge = {:?}", before.rows);
    eprintln!("--- A: rows AFTER refused merge = {:?}", after.rows);
    eprintln!("--- A: heap slots before = {:?}", before.heap.iter().map(|(r, b)| (*r, b.len())).collect::<Vec<_>>());
    eprintln!("--- A: heap slots after  = {:?}", after.heap.iter().map(|(r, b)| (*r, b.len())).collect::<Vec<_>>());
    eprintln!("--- A: by_key before = {:?}", before.by_key);
    eprintln!("--- A: by_key after  = {:?}", after.by_key);
    assert_eq!(before, after, "MERGE reported failure ({e}) but the target changed");
}

// -------------------------------------------------------------------------------------------
// B. Two staged edits, the second refused: is the first left applied to the SHARED catalog?
// -------------------------------------------------------------------------------------------
#[test]
fn a_merge_refused_on_the_second_edit_does_not_leave_the_first_applied() {
    let mut d = db();
    seed(&mut d);
    let before = snapshot(&mut d, "t", SEL, &KEYS);

    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-b';", &mut agent).expect("branch");
    // A rename does no heap work at all, so it cannot fail for size: it is the edit that will
    // already have been committed when the retype behind it is refused.
    d.exec("ALTER TABLE t RENAME COLUMN a TO a2;", &mut agent).expect("stage rename");
    d.exec(REFUSED, &mut agent).expect("stage retype");
    eprintln!("--- B: staged = {:?}", d.runtime.pending_schema_edits(
        match agent.agent.as_ref() { Some(a) => a.branch, None => panic!("no branch") }));

    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
    assert_is_the_size_refusal(&e, "t");

    let after = snapshot(&mut d, "t", SEL, &KEYS);
    eprintln!("--- B: refusal = {e}");
    eprintln!("--- B: shape BEFORE = {:?}", before.shape);
    eprintln!("--- B: shape AFTER  = {:?}", after.shape);
    eprintln!("--- B: ddl BEFORE = {:?}", before.ddl);
    eprintln!("--- B: ddl AFTER  = {:?}", after.ddl);
    assert_eq!(before.shape, after.shape, "the refused MERGE renamed a column anyway ({e})");
    assert_eq!(before.ddl, after.ddl, "the refused MERGE emitted a DDL record anyway ({e})");
    assert_eq!(before, after, "MERGE reported failure ({e}) but the target changed");
}

// -------------------------------------------------------------------------------------------
// C. What is left of the BRANCH: is it sealed, and does a retry double-apply?
// -------------------------------------------------------------------------------------------
#[test]
fn a_refused_merge_leaves_a_branch_that_does_not_double_apply_on_retry() {
    let mut d = db();
    seed(&mut d);

    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-c';", &mut agent).expect("branch");
    d.exec("INSERT INTO t VALUES (3, 300, 'small', 'small');", &mut agent).expect("insert");
    d.exec(REFUSED, &mut agent).expect("stage");
    let e1 = d.exec("MERGE;", &mut agent).err().expect("first MERGE refused");
    let after1 = d.rows(SEL);
    let e2 = d.exec("MERGE;", &mut agent);
    let after2 = d.rows(SEL);
    eprintln!("--- C: first refusal  = {e1}");
    eprintln!("--- C: rows after 1st = {after1:?}");
    eprintln!("--- C: second MERGE   = {:?}", e2.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    eprintln!("--- C: rows after 2nd = {after2:?}");
    assert_eq!(after1, after2, "retrying a refused MERGE changed the target again");
}

// -------------------------------------------------------------------------------------------
// D. Two branches, the second refused. Does A's success survive, and is B's refusal clean?
//    Also: is the runtime still usable (no poisoned mutex) after a refusal?
// -------------------------------------------------------------------------------------------
#[test]
fn a_second_branch_refused_leaves_the_first_branchs_merge_intact() {
    let mut d = db();
    seed(&mut d);

    let mut a = Session::with_runtime(d.runtime.clone());
    let mut b = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-1';", &mut a).expect("branch a");
    d.exec("BEGIN AGENT SESSION AS 'agent-2';", &mut b).expect("branch b");
    d.exec("ALTER TABLE t RENAME COLUMN b TO b2;", &mut a).expect("stage rename on a");
    d.exec(REFUSED, &mut b).expect("stage retype on b");

    let ra = d.exec("MERGE;", &mut a);
    eprintln!("--- D: branch a MERGE = {:?}", ra.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    let before = snapshot(&mut d, "t", SEL, &KEYS);

    let e = d.exec("MERGE;", &mut b).err().expect("branch b MERGE must be refused");
    assert_is_the_size_refusal(&e, "t");
    let after = snapshot(&mut d, "t", SEL, &KEYS);
    eprintln!("--- D: refusal = {e}");
    eprintln!("--- D: shape before b = {:?}", before.shape);
    eprintln!("--- D: shape after  b = {:?}", after.shape);
    assert_eq!(before, after, "branch b's refused MERGE changed the target ({e})");

    // The runtime must still work: a poisoned mutex would take every later statement with it.
    let mut c = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-3';", &mut c).expect("runtime still usable after refusal");
    d.exec("UPDATE t SET n = 7 WHERE id = 1;", &mut c).expect("branch write still works");
    let rc = d.exec("MERGE;", &mut c);
    eprintln!("--- D: later branch MERGE = {:?}", rc.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    rc.expect("a later merge must still succeed");
    eprintln!("--- D: rows at the end = {:?}", d.rows(SEL));
}

// -------------------------------------------------------------------------------------------
// CONTROL. The same branch shape, with a table whose rows leave room, must MERGE successfully —
// otherwise every assertion above is measuring a merge that could never have landed.
// -------------------------------------------------------------------------------------------
#[test]
fn control_the_same_merge_lands_when_the_rows_leave_room() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    d.sql("INSERT INTO t VALUES (2, 200, 'small', 'small');");

    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-ok';", &mut agent).expect("branch");
    d.exec("UPDATE t SET n = 999 WHERE id = 1;", &mut agent).expect("update");
    d.exec("INSERT INTO t VALUES (3, 300, 'small', 'small');", &mut agent).expect("insert");
    d.exec("ALTER TABLE t RENAME COLUMN a TO a2;", &mut agent).expect("stage rename");
    d.exec(REFUSED, &mut agent).expect("stage retype");
    d.exec("MERGE;", &mut agent).expect("the control MERGE must land");
    eprintln!("--- CONTROL: shape = {:?}", d.shape("t"));
    eprintln!("--- CONTROL: rows  = {:?}", d.rows(SEL));
    eprintln!("--- CONTROL: ddl   = {:?}", d.schema_changes());
}

// -------------------------------------------------------------------------------------------
// E. Same as B, but the edit that lands FIRST rewrites the heap. Sweeps to find a row width
//    where `ADD COLUMN` fits and the retype behind it does not.
// -------------------------------------------------------------------------------------------
#[test]
fn a_merge_refused_on_the_second_edit_does_not_leave_the_heap_rewritten() {
    let mut reached = 0usize;
    for w in 2000..2014usize {
        let big = "x".repeat(w);
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
        d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
        d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));

        // Does the ADD fit and the retype not? Decide it on a throwaway copy of the fixture.
        let mut probe = db();
        probe.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
        probe.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
        probe.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));
        if probe.try_sql("ALTER TABLE t ADD COLUMN c1 VARCHAR(20);").is_err() { continue; }
        if probe.try_sql(REFUSED).is_ok() { continue; }
        reached += 1;

        let before = snapshot(&mut d, "t", SEL, &KEYS);
        let mut agent = Session::with_runtime(d.runtime.clone());
        d.exec("BEGIN AGENT SESSION AS 'agent-e';", &mut agent).expect("branch");
        d.exec("ALTER TABLE t ADD COLUMN c1 VARCHAR(20);", &mut agent).expect("stage add");
        d.exec(REFUSED, &mut agent).expect("stage retype");
        let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
        assert_is_the_size_refusal(&e, "t");
        let after = snapshot(&mut d, "t", SEL, &KEYS);

        eprintln!("--- E(w={w}): shape BEFORE = {:?}", before.shape);
        eprintln!("--- E(w={w}): shape AFTER  = {:?}", after.shape);
        eprintln!("--- E(w={w}): ddl AFTER    = {:?}", after.ddl);
        eprintln!("--- E(w={w}): heap len before = {:?}", before.heap.iter().map(|(_, b)| b.len()).collect::<Vec<_>>());
        eprintln!("--- E(w={w}): heap len after  = {:?}", after.heap.iter().map(|(_, b)| b.len()).collect::<Vec<_>>());
        eprintln!("--- E(w={w}): heap bytes identical = {}", before.heap == after.heap);
        // Was it persisted, or only in-memory? Read the catalog off the disk again.
        d.bp.flush_all().unwrap();
        let reread = Catalog::open(d.bp.clone(), 1).unwrap();
        let disk_shape: Vec<String> = reread.get_table("t").unwrap().schema.columns.iter()
            .map(|c| format!("{}:{:?}", c.name, c.data_type)).collect();
        eprintln!("--- E(w={w}): shape RE-READ FROM DISK = {disk_shape:?}");
        assert_eq!(before, after, "the refused MERGE half-applied its schema edits ({e})");
    }
    assert!(reached > 0, "the sweep never reached the condition: no width where ADD fits and retype does not");
}

// -------------------------------------------------------------------------------------------
// F. B's rename, re-read off disk after a flush: in-memory only, or committed?
// -------------------------------------------------------------------------------------------
#[test]
fn the_half_applied_rename_is_on_disk_not_just_in_memory() {
    let mut d = db();
    seed(&mut d);
    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-f';", &mut agent).expect("branch");
    d.exec("ALTER TABLE t RENAME COLUMN a TO a2;", &mut agent).expect("stage rename");
    d.exec(REFUSED, &mut agent).expect("stage retype");
    let e = d.exec("MERGE;", &mut agent).err().expect("refused");
    assert_is_the_size_refusal(&e, "t");
    d.bp.flush_all().unwrap();
    d.bp.disk_manager.sync().unwrap();
    let reread = Catalog::open(d.bp.clone(), 1).unwrap();
    let disk_shape: Vec<String> = reread.get_table("t").unwrap().schema.columns.iter()
        .map(|c| format!("{}:{:?}", c.name, c.data_type)).collect();
    eprintln!("--- F: shape RE-READ FROM DISK after the refused MERGE = {disk_shape:?}");
    eprintln!("--- F: change-feed DDL = {:?}", d.schema_changes());
    assert!(disk_shape.iter().any(|c| c.starts_with("a:")),
        "the refused MERGE persisted a rename to disk: {disk_shape:?}");
}

// -------------------------------------------------------------------------------------------
// G. The retry in C reported Ok. Did it APPLY the retype, or silently drop it?
// -------------------------------------------------------------------------------------------
#[test]
fn the_retry_after_a_refused_merge_does_not_report_success_while_dropping_the_edit() {
    let mut d = db();
    seed(&mut d);
    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-g';", &mut agent).expect("branch");
    let br = agent.agent.as_ref().unwrap().branch;
    d.exec(REFUSED, &mut agent).expect("stage retype");
    eprintln!("--- G: staged before 1st MERGE = {:?}", d.runtime.pending_schema_edits(br));
    let e1 = d.exec("MERGE;", &mut agent).err().expect("1st MERGE refused");
    assert_is_the_size_refusal(&e1, "t");
    eprintln!("--- G: staged after refusal    = {:?}", d.runtime.pending_schema_edits(br));
    eprintln!("--- G: shape after refusal     = {:?}", d.shape("t"));

    let r2 = d.exec("MERGE;", &mut agent);
    eprintln!("--- G: 2nd MERGE = {:?}", r2.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    eprintln!("--- G: shape after 2nd MERGE   = {:?}", d.shape("t"));
    eprintln!("--- G: staged after 2nd MERGE  = {:?}", d.runtime.pending_schema_edits(br));
    eprintln!("--- G: ddl = {:?}", d.schema_changes());
    if r2.is_ok() {
        assert_eq!(
            d.shape("t")[1].1, DataType::BigInt,
            "the retry MERGE reported success but the staged retype was never applied"
        );
    }
}

// -------------------------------------------------------------------------------------------
// H. C's retry returned Ok. G's (no row writes) did not. Did C's retry apply the retype, or
//    report a landed merge while dropping it?
// -------------------------------------------------------------------------------------------
#[test]
fn a_retry_that_reports_ok_must_have_applied_the_staged_retype() {
    let mut d = db();
    seed(&mut d);
    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-h';", &mut agent).expect("branch");
    let br = agent.agent.as_ref().unwrap().branch;
    d.exec("INSERT INTO t VALUES (3, 300, 'small', 'small');", &mut agent).expect("insert");
    d.exec(REFUSED, &mut agent).expect("stage retype");
    let e1 = d.exec("MERGE;", &mut agent).err().expect("1st MERGE refused");
    assert_is_the_size_refusal(&e1, "t");
    eprintln!("--- H: staged after refusal = {:?}", d.runtime.pending_schema_edits(br));

    let r2 = d.exec("MERGE;", &mut agent);
    let ok = r2.is_ok();
    eprintln!("--- H: 2nd MERGE ok? {ok}  err={:?}", r2.as_ref().err().map(|e| e.to_string()));
    if let Ok(ferrodb::execution::executor::Outcome::Agent(ferrodb::agent_sql::dispatch::AgentOutput::Merge(m))) = &r2 {
        eprintln!("--- H: MergeReport applied_to_target = {} schema = {:?}", m.applied_to_target, m.schema);
    }
    eprintln!("--- H: shape after 2nd MERGE  = {:?}", d.shape("t"));
    eprintln!("--- H: staged after 2nd MERGE = {:?}", d.runtime.pending_schema_edits(br));
    eprintln!("--- H: rows  = {:?}", d.rows(SEL));
    eprintln!("--- H: ddl   = {:?}", d.schema_changes());
    if ok {
        assert_eq!(d.shape("t")[1].1, DataType::BigInt,
            "MERGE reported success but the staged retype was silently dropped");
    }
}
