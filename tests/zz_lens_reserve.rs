//! LENS (adversarial, fresh context): does `reserve_free_space` — guard (3) of 58804c8 —
//! terminate? Written from scratch; nothing copied from the claimant's file except the
//! decision to run the ALTER on a worker thread so a hang is an assertion, not a wedged suite.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;

use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::storage::heap_page::{HEADER_SIZE, MAX_TUPLE_SIZE, SLOT_ENTRY_SIZE};
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lens.db");
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("lens.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let catalog = Catalog::create(bp.clone()).unwrap();
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { _dir: dir, catalog, bp, txn, runtime, session }
}

impl Db {
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(FerroError::SqlParseError(format!("{:?}", p.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>())));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        let mut session = std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out = run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut session);
        self.session = session;
        out
    }
    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }
    fn heap(&self, table: &str) -> HeapFileManager {
        HeapFileManager::open(self.catalog.get_table(table).unwrap().first_directory_page_id, self.bp.clone())
    }
    /// (rows, distinct data pages) as the heap itself reports them.
    fn shape_of_heap(&self, table: &str) -> (usize, usize) {
        let mut pages: Vec<u32> = Vec::new();
        let mut n = 0usize;
        for r in self.heap(table).scan() {
            let (rid, _t) = r.unwrap();
            n += 1;
            if !pages.contains(&rid.page_id) { pages.push(rid.page_id); }
        }
        (n, pages.len())
    }
}

/// Arithmetic the mechanism rests on, printed so the numbers are not taken on trust.
#[test]
fn t0_the_constants() {
    println!("PAGE_SIZE={PAGE_SIZE} HEADER_SIZE={HEADER_SIZE} SLOT_ENTRY_SIZE={SLOT_ENTRY_SIZE} MAX_TUPLE_SIZE={MAX_TUPLE_SIZE}");
    println!("reserve_free_space asks find_or_make_page for usable={}", PAGE_SIZE - HEADER_SIZE - SLOT_ENTRY_SIZE);
    println!("find_or_make_page then searches for free_space >= {}", PAGE_SIZE - HEADER_SIZE - SLOT_ENTRY_SIZE + SLOT_ENTRY_SIZE);
    println!("a brand-new page is registered with free_space = {}", PAGE_SIZE - HEADER_SIZE);
}

/// Direct mechanism: three consecutive `find_or_make_page(MAX_TUPLE_SIZE)` calls — exactly what
/// `reserve_free_space`'s loop body does — must each add space, or that loop cannot terminate.
#[test]
fn t1_find_or_make_page_adds_space_on_every_call() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, s VARCHAR(200));");
    for i in 0..40 {
        d.sql(&format!("INSERT INTO t VALUES ({i}, '{}');", "x".repeat(150)));
    }
    let heap = d.heap("t");
    let mut trace = vec![heap.free_space().unwrap()];
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(heap.find_or_make_page(MAX_TUPLE_SIZE).unwrap());
        trace.push(heap.free_space().unwrap());
    }
    println!("free_space trace: {trace:?}");
    println!("find_or_make_page returned page ids: {ids:?}");
    assert!(
        trace[2] > trace[1] && trace[3] > trace[2],
        "find_or_make_page(MAX_TUPLE_SIZE) stopped adding space after the first call: \
         free_space went {trace:?} and it returned page ids {ids:?}. \
         `while free_space() < bytes {{ find_or_make_page(usable) }}` therefore cannot terminate."
    );
}

/// Termination of `reserve_free_space` itself, at the raw heap layer, for a request bigger than
/// one page. Bounded: the call runs on a worker thread and the test asserts on a timeout.
#[test]
fn t2_reserve_free_space_terminates_for_a_multi_page_request() {
    for &want in &[100usize, 2_000, 4_000, 4_073, 4_100, 20_000] {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut d = db();
            d.sql("CREATE TABLE t (id INTEGER NOT NULL, s VARCHAR(200));");
            for i in 0..40 {
                d.sql(&format!("INSERT INTO t VALUES ({i}, '{}');", "x".repeat(150)));
            }
            let heap = d.heap("t");
            let before = heap.free_space().unwrap();
            let r = heap.reserve_free_space(want);
            let _ = tx.send((before, format!("{r:?}"), heap.free_space().unwrap()));
        });
        let t = Instant::now();
        match rx.recv_timeout(Duration::from_secs(15)) {
            Ok((before, r, after)) => {
                println!("reserve_free_space({want}): returned {r} in {:?}; free_space {before} -> {after}", t.elapsed());
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("worker for reserve_free_space({want}) died before sending — fixture bug, not a hang"),
            Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                "reserve_free_space({want}) had not returned — neither Ok nor Err — 15s after it \
                 was called on a 40-row heap. Guard (3) cannot refuse and cannot succeed."
            ),
        }
    }
}

/// Impact: the statement a user types. `ADD COLUMN` on tables of growing size must each either
/// succeed or be refused. Bounded per table the same way.
#[test]
fn t3_add_column_returns_on_tables_of_growing_size() {
    for &rows in &[10usize, 40, 90, 200] {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut d = db();
            d.sql("CREATE TABLE t (id INTEGER NOT NULL, s VARCHAR(200));");
            for i in 0..rows {
                d.sql(&format!("INSERT INTO t VALUES ({i}, '{}');", "x".repeat(150)));
            }
            let (n, pages) = d.shape_of_heap("t");
            let _ = tx.send(format!("filled: {n} rows over {pages} data pages"));
            let r = d.try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);");
            let verdict = match &r {
                Ok(_) => {
                    let (n2, p2) = d.shape_of_heap("t");
                    let cols = d.catalog.get_table("t").unwrap().schema.columns.len();
                    let sel = d.try_sql("SELECT id FROM t;").map(|o| match o { Outcome::Rows(v) => v.len(), _ => 0 }).unwrap_or(usize::MAX);
                    format!("Ok — {n2} rows over {p2} pages, {cols} columns, SELECT returned {sel}")
                }
                Err(e) => format!("REFUSED: {e}"),
            };
            let _ = tx.send(verdict);
        });
        let fill = rx.recv_timeout(Duration::from_secs(120)).expect("fill timed out");
        println!("[{rows} rows] {fill}");
        let t = Instant::now();
        match rx.recv_timeout(Duration::from_secs(40)) {
            Ok(v) => println!("[{rows} rows] ALTER -> {v}  (in {:?})", t.elapsed()),
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("worker for the {rows}-row ALTER died before sending — fixture bug, not a hang"),
            Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                "`ALTER TABLE t ADD COLUMN w VARCHAR(10)` on a table of {rows} rows ({fill}) had \
                 not returned — neither succeeded nor been refused — 40s after the rows were in \
                 place. Nothing to compare before/after: the statement never finished."
            ),
        }
    }
}

/// Control on the raw layer: the empty page `find_or_make_page` hands back really is usable, so
/// t1's failure is not an artefact of asking for an impossible tuple length.
#[test]
fn t4_control_the_page_it_returns_can_hold_a_max_size_tuple() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, s VARCHAR(200));");
    d.sql("INSERT INTO t VALUES (1, 'a');");
    let heap = d.heap("t");
    let p = heap.find_or_make_page(MAX_TUPLE_SIZE).unwrap();
    let rid = heap.insert(Tuple::new(vec![7u8; MAX_TUPLE_SIZE])).unwrap();
    println!("find_or_make_page(MAX_TUPLE_SIZE) -> page {p}; inserting a {MAX_TUPLE_SIZE}-byte tuple landed at {rid:?}");
    assert_eq!(rid.page_id, p, "the reserved page is not where a max-size tuple goes");

}
