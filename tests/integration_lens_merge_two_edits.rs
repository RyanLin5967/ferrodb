//! LENS: is the "a refused ALTER leaves the table exactly as it was" claim broken by a MERGE
//! that carries TWO schema edits, the second of which the size guard refuses?
//!
//! Built from scratch, against committed 58804c8, with no edits to src/.

use std::path::PathBuf;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    #[allow(dead_code)]
    dir: tempfile::TempDir,
    path: PathBuf,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lens.db");
    let file =
        std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("lens.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let catalog = Catalog::create(bp.clone()).unwrap();
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { dir, path, catalog, wal, bp, txn, runtime, session }
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
        let mut s =
            std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out = self.exec(sql, &mut s);
        self.session = s;
        out
    }
    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }
    /// A query's answer as a string, error included: the SEMANTIC probe, not a snapshot field.
    fn ask(&mut self, sql: &str) -> String {
        match self.try_sql(sql) {
            Ok(Outcome::Rows(r)) => format!("Ok({r:?})"),
            Ok(_) => "Ok(non-rows)".to_string(),
            Err(e) => format!("Err({e})"),
        }
    }
    fn shape(&self, table: &str) -> Vec<String> {
        self.catalog
            .get_table(table)
            .unwrap_or_else(|| panic!("no table {table}"))
            .schema
            .columns
            .iter()
            .map(|c| format!("{}:{:?}", c.name, c.data_type))
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
    fn ddl(&self) -> Vec<String> {
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
    /// Flush, then reopen the SAME FILE through a brand-new DiskManager and a brand-new buffer
    /// pool. Nothing of the live pool survives, so what this returns is what is on disk.
    fn shape_from_a_fresh_pool(&mut self, table: &str) -> Vec<String> {
        self.bp.flush_all().unwrap();
        self.bp.disk_manager.sync().unwrap();
        let file = std::fs::OpenOptions::new().read(true).write(true).open(&self.path).unwrap();
        let bp2 = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let cat2 = Catalog::open(bp2.clone(), 1).unwrap();
        cat2.get_table(table)
            .unwrap_or_else(|| panic!("no table {table} on disk"))
            .schema
            .columns
            .iter()
            .map(|c| format!("{}:{:?}", c.name, c.data_type))
            .collect()
    }
}

const WIDE: usize = 2014;
const RETYPE: &str = "ALTER TABLE t ALTER COLUMN n TYPE BIGINT;";
const RENAME: &str = "ALTER TABLE t RENAME COLUMN a TO a2;";

fn seed(d: &mut Db) {
    let big = "x".repeat(WIDE);
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));
}

fn is_size_refusal(e: &FerroError) -> bool {
    let m = e.to_string();
    m.contains("would widen") && m.contains("bytes a tuple can occupy")
}

// ---------------------------------------------------------------------------------------------
// CONTROL 0. The guard is the guard: a PLAIN `ALTER TABLE` retype on this fixture is refused,
// and refusing it changes nothing. If this fails, every test below is measuring the wrong thing.
// ---------------------------------------------------------------------------------------------
#[test]
fn lens_control_plain_alter_is_refused_and_changes_nothing() {
    let mut d = db();
    seed(&mut d);
    let shape0 = d.shape("t");
    let heap0 = d.heap("t");
    let ddl0 = d.ddl();
    let e = d.try_sql(RETYPE).err().expect("the plain ALTER must be refused");
    eprintln!("--- C0: plain ALTER refusal = {e}");
    assert!(is_size_refusal(&e), "refused by something other than the size guard: {e}");
    assert_eq!(shape0, d.shape("t"), "plain refused ALTER changed the shape");
    assert_eq!(heap0, d.heap("t"), "plain refused ALTER changed the heap bytes");
    assert_eq!(ddl0, d.ddl(), "plain refused ALTER emitted a DDL record");
    eprintln!("--- C0: shape/heap/ddl all unchanged after the plain refusal");
}

// ---------------------------------------------------------------------------------------------
// CONTROL 1. The same two-edit MERGE must LAND on a table whose rows leave room. Without this,
// test 1 could be measuring a merge that could never have applied anything at all.
// ---------------------------------------------------------------------------------------------
#[test]
fn lens_control_the_two_edit_merge_lands_when_the_rows_are_narrow() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'lens-ok';", &mut agent).expect("branch");
    d.exec(RENAME, &mut agent).expect("stage rename");
    d.exec(RETYPE, &mut agent).expect("stage retype");
    d.exec("MERGE;", &mut agent).expect("the control MERGE must land");
    eprintln!("--- C1: shape after a LANDED two-edit merge = {:?}", d.shape("t"));
    eprintln!("--- C1: ddl   after a LANDED two-edit merge = {:?}", d.ddl());
    assert!(d.shape("t").iter().any(|c| c.starts_with("a2:")), "control did not rename");
    assert!(d.shape("t").iter().any(|c| c == "n:BigInt"), "control did not retype");
}

// ---------------------------------------------------------------------------------------------
// 1. THE CLAIM UNDER TEST. Stage rename-then-retype; the retype is refused. Does the rename
//    survive in the shared catalog, on disk, in the change feed, and in what SQL can see?
// ---------------------------------------------------------------------------------------------
#[test]
fn lens_merge_refused_on_the_second_edit_leaves_the_first_applied() {
    let mut d = db();
    seed(&mut d);

    let shape_before = d.shape("t");
    let heap_before = d.heap("t");
    let ddl_before = d.ddl();
    let ask_a_before = d.ask("SELECT a FROM t WHERE id = 1;");
    let ask_a2_before = d.ask("SELECT a2 FROM t WHERE id = 1;");
    let disk_before = d.shape_from_a_fresh_pool("t");

    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'lens-1';", &mut agent).expect("branch");
    d.exec(RENAME, &mut agent).expect("stage rename");
    d.exec(RETYPE, &mut agent).expect("stage retype");

    let r = d.exec("MERGE;", &mut agent);
    eprintln!("--- 1: MERGE returned = {:?}", r.as_ref().map(|_| "Ok(MergeReport)").map_err(|e| e.to_string()));
    let e = r.err().expect("(a) did the statement actually FAIL? -- it must return Err");
    assert!(is_size_refusal(&e), "refused, but not by the size guard: {e}");

    let shape_after = d.shape("t");
    let heap_after = d.heap("t");
    let ddl_after = d.ddl();
    let ask_a_after = d.ask("SELECT a FROM t WHERE id = 1;");
    let ask_a2_after = d.ask("SELECT a2 FROM t WHERE id = 1;");
    let disk_after = d.shape_from_a_fresh_pool("t");

    eprintln!("--- 1: refusal        = {e}");
    eprintln!("--- 1: shape BEFORE   = {shape_before:?}");
    eprintln!("--- 1: shape AFTER    = {shape_after:?}");
    eprintln!("--- 1: DISK  BEFORE   = {disk_before:?}");
    eprintln!("--- 1: DISK  AFTER    = {disk_after:?}   <- fresh DiskManager + fresh buffer pool");
    eprintln!("--- 1: ddl   BEFORE   = {ddl_before:?}");
    eprintln!("--- 1: ddl   AFTER    = {ddl_after:?}");
    eprintln!("--- 1: `SELECT a`  before = {ask_a_before}");
    eprintln!("--- 1: `SELECT a`  after  = {ask_a_after}");
    eprintln!("--- 1: `SELECT a2` before = {ask_a2_before}");
    eprintln!("--- 1: `SELECT a2` after  = {ask_a2_after}");
    eprintln!("--- 1: heap bytes identical = {}", heap_before == heap_after);
    eprintln!("--- 1: heap lens before = {:?}", heap_before.iter().map(|(_, b)| b.len()).collect::<Vec<_>>());
    eprintln!("--- 1: heap lens after  = {:?}", heap_after.iter().map(|(_, b)| b.len()).collect::<Vec<_>>());

    // The heap half of the claim is expected to HOLD. Assert it separately so a failure of the
    // catalog half cannot be confused with a failure of the heap half.
    assert_eq!(heap_before, heap_after, "the refused MERGE changed raw heap bytes");
    assert_eq!(shape_before, shape_after, "the refused MERGE renamed a column in the catalog");
    assert_eq!(disk_before, disk_after, "the refused MERGE persisted a rename to DISK");
    assert_eq!(ddl_before, ddl_after, "the refused MERGE emitted a DDL record");
    assert_eq!(ask_a_before, ask_a_after, "`SELECT a` answers differently after the refusal");
}

// ---------------------------------------------------------------------------------------------
// 2. ORDER. Same two edits, retype FIRST. If the refusal is genuinely "edit 1 commits, edit 2
//    is refused", then with the refused edit first NOTHING should be applied.
// ---------------------------------------------------------------------------------------------
#[test]
fn lens_the_refused_edit_first_leaves_nothing_applied() {
    let mut d = db();
    seed(&mut d);
    let shape_before = d.shape("t");
    let ddl_before = d.ddl();

    let mut agent = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'lens-2';", &mut agent).expect("branch");
    d.exec(RETYPE, &mut agent).expect("stage retype FIRST");
    d.exec(RENAME, &mut agent).expect("stage rename SECOND");
    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
    eprintln!("--- 2: refusal      = {e}");
    eprintln!("--- 2: shape BEFORE = {shape_before:?}");
    eprintln!("--- 2: shape AFTER  = {:?}", d.shape("t"));
    eprintln!("--- 2: ddl   BEFORE = {ddl_before:?}");
    eprintln!("--- 2: ddl   AFTER  = {:?}", d.ddl());
    assert_eq!(shape_before, d.shape("t"), "refused-first MERGE still changed the shape");
}
