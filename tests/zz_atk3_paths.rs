//! ATK3 — routes into `rewrite_heap` other than a plain `ALTER TABLE` at the SQL surface.
//!
//! Claim under attack: "An ALTER TABLE that is refused leaves the table EXACTLY as it was."
//! These fixtures reach the same size refusal through `AgentRuntime::merge` (the `MERGE;` surface),
//! where the statement that carries the ALTER has already committed other work.

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
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { dir, catalog, wal, bp, txn, runtime, session }
}

impl Db {
    fn try_exec(&mut self, sql: &str, session: &mut Session) -> Result<Outcome, FerroError> {
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

    fn exec(&mut self, sql: &str, session: &mut Session) -> Outcome {
        self.try_exec(sql, session).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let mut s = std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out = self.try_exec(sql, &mut s);
        self.session = s;
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

fn is_the_size_refusal(e: &FerroError) -> bool {
    let m = e.to_string();
    m.contains("would widen") && m.contains("bytes a tuple can occupy")
}

fn short(s: &Snapshot) -> String {
    format!(
        "shape={:?} heap_slots={:?} heap_lens={:?} rows={:?} by_key={:?} ddl={:?}",
        s.shape.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
        s.heap.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
        s.heap.iter().map(|(_, b)| b.len()).collect::<Vec<_>>(),
        s.rows,
        s.by_key,
        s.ddl
    )
}

// ---------------------------------------------------------------------------------------------
// Probe: which widths insert at all, and which of those the plain ALTER refuses for size.
// ---------------------------------------------------------------------------------------------

#[test]
fn probe_the_refusing_window() {
    let mut refusing = Vec::new();
    let mut widest_insert = 0usize;
    for pad in 3900usize..=4069 {
        let mut d = db();
        d.sql("CREATE TABLE inv (id INTEGER NOT NULL, a VARCHAR(4100));");
        if d.try_sql(&format!("INSERT INTO inv VALUES (1, '{}');", "y".repeat(pad))).is_err() {
            continue;
        }
        widest_insert = widest_insert.max(pad);
        match d.try_sql("ALTER TABLE inv ADD COLUMN n1 VARCHAR(20);") {
            Ok(_) => {}
            Err(e) if is_the_size_refusal(&e) => refusing.push(pad),
            Err(e) => println!("pad={pad}: ALTER refused for another reason: {e}"),
        }
    }
    println!("widest pad that inserts = {widest_insert}");
    println!("pads whose ADD COLUMN hits the size refusal = {refusing:?}");
    assert!(!refusing.is_empty(), "no width reached the size refusal on the plain path");
}

// ---------------------------------------------------------------------------------------------
// Path 1: a branch whose ROW writes publish and whose staged ALTER is then refused at MERGE.
// ---------------------------------------------------------------------------------------------

#[test]
fn merge_that_is_refused_for_size_leaves_the_target_unchanged() {
    let mut reached = 0;
    let mut skipped: Vec<String> = Vec::new();
    for pad in 3900usize..=4069 {
        let mut d = db();
        d.sql("CREATE TABLE inv (id INTEGER NOT NULL, a VARCHAR(4100));");
        if d.try_sql("INSERT INTO inv VALUES (1, 'small');").is_err() {
            continue;
        }
        let mut a = Session::with_runtime(d.runtime.clone());
        d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut a);
        let big = "x".repeat(pad);
        if let Err(e) = d.try_exec(&format!("INSERT INTO inv VALUES (2, '{big}');"), &mut a) {
            skipped.push(format!("pad={pad} branch-insert-err={e}"));
            continue;
        }
        if let Err(e) = d.try_exec("ALTER TABLE inv ADD COLUMN note VARCHAR(20);", &mut a) {
            skipped.push(format!("pad={pad} stage-err={e}"));
            continue;
        }

        let keys = vec![Value::Integer(1), Value::Integer(2)];
        let before = snapshot(&mut d, "inv", "SELECT id FROM inv;", &keys);
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            d.try_exec("MERGE;", &mut a)
        }));
        let err = match out {
            Err(_) => panic!("pad={pad}: MERGE PANICKED"),
            Ok(Ok(_)) => {
                skipped.push(format!("pad={pad} merge-ok"));
                continue;
            }
            Ok(Err(e)) => e,
        };
        if !is_the_size_refusal(&err) {
            skipped.push(format!("pad={pad} merge-err={err}"));
            continue;
        }
        reached += 1;
        let after = snapshot(&mut d, "inv", "SELECT id FROM inv;", &keys);
        println!("pad={pad} MERGE REFUSED: {err}");
        println!("  before: {}", short(&before));
        println!("  after : {}", short(&after));

        let still_staged = d.runtime.pending_schema_edits(a.agent.as_ref().unwrap().branch);
        println!("  still staged on the branch after the refusal: {still_staged:?}");
        let second = d.try_exec("MERGE;", &mut a);
        let after2 = snapshot(&mut d, "inv", "SELECT id FROM inv;", &keys);
        println!("  second MERGE: {:?}", second.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
        println!("  after2: {}", short(&after2));

        assert_eq!(
            before, after,
            "pad={pad}: a MERGE that reported the size refusal changed the target table"
        );
        break;
    }
    if reached == 0 {
        for s in &skipped {
            println!("{s}");
        }
    }
    assert!(reached > 0, "no pad width reached the size refusal through MERGE — fixture vacuous");
}

// ---------------------------------------------------------------------------------------------
// Path 2: two staged edits on ONE branch, the second refused.
// ---------------------------------------------------------------------------------------------

#[test]
fn merge_with_two_staged_edits_the_second_refused() {
    let mut reached = 0;
    let mut skipped: Vec<String> = Vec::new();
    for pad in 3900usize..=4069 {
        let mut d = db();
        d.sql("CREATE TABLE inv (id INTEGER NOT NULL, a VARCHAR(4100));");
        if d.try_sql(&format!("INSERT INTO inv VALUES (1, '{}');", "y".repeat(pad))).is_err() {
            continue;
        }
        let mut a = Session::with_runtime(d.runtime.clone());
        d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut a);
        if d.try_exec("ALTER TABLE inv ADD COLUMN n1 VARCHAR(20);", &mut a).is_err() {
            continue;
        }
        if d.try_exec("ALTER TABLE inv ADD COLUMN n2 VARCHAR(20);", &mut a).is_err() {
            continue;
        }

        let keys = vec![Value::Integer(1)];
        let before = snapshot(&mut d, "inv", "SELECT id FROM inv;", &keys);
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            d.try_exec("MERGE;", &mut a)
        }));
        let err = match out {
            Err(_) => panic!("pad={pad}: MERGE PANICKED"),
            Ok(Ok(_)) => {
                skipped.push(format!("pad={pad} merge-ok"));
                continue;
            }
            Ok(Err(e)) => e,
        };
        if !is_the_size_refusal(&err) {
            skipped.push(format!("pad={pad} merge-err={err}"));
            continue;
        }
        reached += 1;
        let after = snapshot(&mut d, "inv", "SELECT id FROM inv;", &keys);
        println!("pad={pad} MERGE REFUSED (second staged edit): {err}");
        println!("  before: {}", short(&before));
        println!("  after : {}", short(&after));
        assert_eq!(
            before, after,
            "pad={pad}: a MERGE that reported the size refusal changed the target table"
        );
        break;
    }
    if reached == 0 {
        for s in &skipped {
            println!("{s}");
        }
    }
    assert!(reached > 0, "no pad width reached the refusal with two staged edits — fixture vacuous");
}

// ---------------------------------------------------------------------------------------------
// Path 3: two branches, the SECOND one's schema edit refused.
// ---------------------------------------------------------------------------------------------

#[test]
fn second_branch_schema_edit_refused_leaves_the_target_unchanged() {
    let mut reached = 0;
    let mut skipped: Vec<String> = Vec::new();
    for pad in 3900usize..=4069 {
        let mut d = db();
        d.sql("CREATE TABLE inv (id INTEGER NOT NULL, a VARCHAR(4100));");
        if d.try_sql(&format!("INSERT INTO inv VALUES (1, '{}');", "z".repeat(pad))).is_err() {
            continue;
        }
        let mut a = Session::with_runtime(d.runtime.clone());
        let mut b = Session::with_runtime(d.runtime.clone());
        d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut a);
        d.exec("BEGIN AGENT SESSION AS 'agent-b';", &mut b);
        if d.try_exec("ALTER TABLE inv ADD COLUMN n1 VARCHAR(20);", &mut a).is_err() {
            continue;
        }
        if d.try_exec("ALTER TABLE inv ADD COLUMN n2 VARCHAR(20);", &mut b).is_err() {
            continue;
        }
        if let Err(e) = d.try_exec("MERGE;", &mut a) {
            skipped.push(format!("pad={pad} A-merge-err={e}"));
            continue;
        }

        let keys = vec![Value::Integer(1)];
        let before = snapshot(&mut d, "inv", "SELECT id FROM inv;", &keys);
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            d.try_exec("MERGE;", &mut b)
        }));
        let err = match out {
            Err(_) => panic!("pad={pad}: second MERGE PANICKED"),
            Ok(Ok(_)) => {
                skipped.push(format!("pad={pad} B-merge-ok"));
                continue;
            }
            Ok(Err(e)) => e,
        };
        if !is_the_size_refusal(&err) {
            skipped.push(format!("pad={pad} B-merge-err={err}"));
            continue;
        }
        reached += 1;
        let after = snapshot(&mut d, "inv", "SELECT id FROM inv;", &keys);
        println!("pad={pad} B's MERGE REFUSED: {err}");
        println!("  before: {}", short(&before));
        println!("  after : {}", short(&after));
        assert_eq!(before, after, "pad={pad}: B's refused MERGE changed the target");
        break;
    }
    if reached == 0 {
        for s in &skipped {
            println!("{s}");
        }
    }
    assert!(reached > 0, "no pad width reached the refusal on the second branch — fixture vacuous");
}
