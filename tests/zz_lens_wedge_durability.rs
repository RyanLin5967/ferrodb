//! LENS pass: is the "wedged ALTER makes a durable unlogged change" claim a defect of the
//! refusal guard, or a property of the page allocator that every statement shares?
//!
//! Three measurements, one harness:
//!   1. a REFUSED ALTER (width refusal) -> are the raw file bytes identical?
//!   2. a plain, SUCCESSFUL INSERT that needs a new page -> does it make the same class of
//!      unlogged change to alter.db (grow by a page, byte 4 of page 0 change) with no flush?
//!   3. the wedging ALTER -> did the statement FAIL, or is it still running? and does a fresh
//!      reader lose a row, a column or a heap byte?

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn open(dir: &Path, recover: bool) -> Db {
    let path = dir.join("alter.db");
    let fresh = !path.exists();
    let file =
        std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    if recover {
        ferrodb::wal::recovery::recover(&txn).expect("recover");
    }
    let catalog = if fresh {
        Catalog::create(bp.clone()).unwrap()
    } else {
        Catalog::open(bp.clone(), 1).unwrap()
    };
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { catalog, wal, bp, txn, runtime, session }
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
    fn rows(&mut self, sql: &str) -> Result<usize, String> {
        match self.try_sql(sql) {
            Ok(Outcome::Rows(r)) => Ok(r.len()),
            Ok(_) => Err("not rows".into()),
            Err(e) => Err(e.to_string()),
        }
    }
    fn shape(&self, t: &str) -> Vec<(String, DataType)> {
        self.catalog
            .get_table(t)
            .unwrap()
            .schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone()))
            .collect()
    }
    fn heap(&self, t: &str) -> Vec<(RecordId, Vec<u8>)> {
        let root = self.catalog.get_table(t).unwrap().first_directory_page_id;
        HeapFileManager::open(root, self.bp.clone())
            .scan()
            .map(|r| {
                let (rid, tup) = r.unwrap();
                (rid, tup.data)
            })
            .collect()
    }
    fn by_key(&self, t: &str, keys: &[Value]) -> Vec<(Value, Option<RecordId>)> {
        let root = self.catalog.get_table(t).unwrap().primary_index_root;
        let ix = BPlusTreeManager::<Value, RecordId>::open(root, self.bp.clone());
        keys.iter().map(|k| (k.clone(), ix.search(k).unwrap())).collect()
    }
    fn flush(&self) {
        self.bp.flush_all().unwrap();
        self.bp.disk_manager.sync().unwrap();
        self.wal.flush().unwrap();
    }
}

/// Read both files through FRESH handles, so measuring flushes nothing.
fn files(dir: &Path) -> (Vec<u8>, Vec<u8>) {
    (
        std::fs::read(dir.join("alter.db")).unwrap_or_default(),
        std::fs::read(dir.join("alter.wal")).unwrap_or_default(),
    )
}

fn describe(name: &str, a: &[u8], b: &[u8]) -> String {
    if a == b {
        return format!("{name}: identical ({} bytes)", a.len());
    }
    let first = (0..a.len().min(b.len())).find(|&i| a[i] != b[i]);
    let diffs = (0..a.len().min(b.len())).filter(|&i| a[i] != b[i]).count();
    format!(
        "{name}: DIFFERS — was {} bytes, now {} bytes, {diffs} differing byte(s) in the common \
         prefix, first at {first:?}",
        a.len(),
        b.len()
    )
}

/// Which 4096-byte pages differ, and by how many bytes.
fn diff_pages(a: &[u8], b: &[u8]) -> Vec<(usize, usize)> {
    let n = a.len().min(b.len()) / 4096;
    (0..n)
        .filter_map(|p| {
            let (x, y) = (&a[p * 4096..(p + 1) * 4096], &b[p * 4096..(p + 1) * 4096]);
            let c = (0..4096).filter(|&i| x[i] != y[i]).count();
            if c == 0 { None } else { Some((p, c)) }
        })
        .collect()
}

fn build(d: &mut Db, rows: i32, pad_len: usize) {
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));");
    let pad = "y".repeat(pad_len);
    for i in 1..=rows {
        d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
    }
}

// ---------------------------------------------------------------------------------------------
// 1. The claim actually under attack: a REFUSED ALTER and the raw file bytes.
// ---------------------------------------------------------------------------------------------
#[test]
fn a_refused_alter_leaves_the_raw_file_bytes_identical() {
    let dir = tempfile::tempdir().unwrap();
    let keys: Vec<Value> = (1..=6).map(Value::Integer).collect();
    let (before_heap, before_ix, before_shape, before_rows);
    let (bdb, bwal);
    {
        let mut d = open(dir.path(), false);
        // Rows already near MAX_TUPLE_SIZE so any widening ALTER must be refused.
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(4100));");
        let pad = "z".repeat(4034);
        for i in 1..=6 {
            d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
        }
        d.flush();
        (bdb, bwal) = files(dir.path());
        before_heap = d.heap("t");
        before_ix = d.by_key("t", &keys);
        before_shape = d.shape("t");
        before_rows = d.rows("SELECT id FROM t;");

        let e = d
            .try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);")
            .err()
            .expect("a widening ALTER on 4034-byte rows must be REFUSED");
        println!("REFUSAL: {e}");

        let (mdb, mwal) = files(dir.path());
        println!("  mid  {}", describe("alter.db", &bdb, &mdb));
        println!("  mid  {}", describe("alter.wal", &bwal, &mwal));
        assert_eq!(bdb, mdb, "a REFUSED ALTER changed alter.db. {}", describe("alter.db", &bdb, &mdb));
        assert_eq!(bwal, mwal, "a REFUSED ALTER changed alter.wal");

        assert_eq!(before_heap, d.heap("t"), "refusal changed the live heap bytes");
        assert_eq!(before_ix, d.by_key("t", &keys), "refusal changed the primary index");
        assert_eq!(before_shape, d.shape("t"), "refusal changed the catalog shape");
        d.flush();
    } // crash / drop every handle

    let (cdb, cwal) = files(dir.path());
    println!("  post-drop {}", describe("alter.db", &bdb, &cdb));
    println!("  post-drop {}", describe("alter.wal", &bwal, &cwal));
    assert_eq!(bdb, cdb, "the file changed after refusal + checkpoint + drop");
    assert_eq!(bwal, cwal, "the wal changed after refusal + checkpoint + drop");

    let mut r = open(dir.path(), true);
    assert_eq!(before_shape, r.shape("t"), "reopen: shape");
    assert_eq!(before_heap, r.heap("t"), "reopen: heap bytes");
    assert_eq!(before_ix, r.by_key("t", &keys), "reopen: primary index");
    assert_eq!(before_rows, r.rows("SELECT id FROM t;"), "reopen: row count");
    println!(
        "  reopen: {} cols, {} heap tuples, SELECT -> {:?}",
        r.shape("t").len(),
        r.heap("t").len(),
        r.rows("SELECT id FROM t;")
    );
}

// ---------------------------------------------------------------------------------------------
// 2. CONTROL. Is "grew alter.db by a page, changed page 0, nothing in the WAL describes it"
//    peculiar to the ALTER guard, or does every allocation in the engine do it?
// ---------------------------------------------------------------------------------------------
#[test]
fn an_ordinary_successful_insert_makes_the_same_unlogged_file_change() {
    let dir = tempfile::tempdir().unwrap();
    let mut d = open(dir.path(), false);
    build(&mut d, 30, 60);
    d.flush();
    let (bdb, _bwal) = files(dir.path());
    let pages_before: BTreeSet<u32> = d.heap("t").iter().map(|(r, _)| r.page_id).collect();

    // Plain INSERTs, no flush of any kind afterwards, until the heap needs a new page.
    let pad = "y".repeat(60);
    let mut i = 1000;
    loop {
        d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
        i += 1;
        let now: BTreeSet<u32> = d.heap("t").iter().map(|(r, _)| r.page_id).collect();
        if now != pages_before {
            break;
        }
        assert!(i < 1200, "never allocated a page");
    }
    let (ndb, _nwal) = files(dir.path());
    println!("  a SUCCESSFUL insert, with NO flush: {}", describe("alter.db", &bdb, &ndb));
    println!("  differing pages (page, bytes): {:?}", diff_pages(&bdb, &ndb));
    assert_ne!(
        bdb, ndb,
        "control failed: an ordinary insert that allocates a page did NOT change the file, so \
         the wedge's file change would be peculiar to the ALTER path"
    );
    let first = (0..bdb.len().min(ndb.len())).find(|&k| bdb[k] != ndb[k]);
    println!("  first differing byte: {first:?} (page 0 allocation bitmap starts at byte 4)");
    assert_eq!(
        first,
        Some(4),
        "control: expected the allocation bitmap in page 0 to be what changed"
    );
    assert!(ndb.len() > bdb.len(), "control: expected the file to have grown");
}

// ---------------------------------------------------------------------------------------------
// 3. The claim as stated: while the ALTER spins, did the STATEMENT FAIL, and is anything lost?
// ---------------------------------------------------------------------------------------------
#[test]
fn the_spinning_alter_neither_failed_nor_lost_anything() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().to_path_buf();
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        let mut d = open(&path, false);
        build(&mut d, 82, 60);
        d.flush();
        let _ = tx.send("baseline flushed".into());
        match d.try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);") {
            Ok(_) => {
                let _ = tx.send("the ALTER SUCCEEDED".into());
            }
            Err(e) => {
                let _ = tx.send(format!("the ALTER was REFUSED: {e}"));
            }
        }
    });
    assert_eq!(rx.recv_timeout(Duration::from_secs(180)).unwrap(), "baseline flushed");
    let (bdb, bwal) = files(dir.path());

    let outcome = rx.recv_timeout(Duration::from_secs(30));
    let (ndb, nwal) = files(dir.path());
    println!("  statement outcome after 30s: {outcome:?}");
    println!("  {}", describe("alter.db", &bdb, &ndb));
    println!("  {}", describe("alter.wal", &bwal, &nwal));
    println!("  differing pages (page, bytes): {:?}", diff_pages(&bdb, &ndb));

    // (a) Did the statement report failure? The claim is about "a refused ALTER".
    match &outcome {
        Ok(msg) => println!("  the statement RETURNED: {msg}"),
        Err(_) => println!("  the statement has NEITHER succeeded NOR been refused after 30s"),
    }

    // (b)/(c) Does a fresh reader, recovering as the CLI does after a crash, lose anything?
    let mut r = open(dir.path(), true);
    let pages: BTreeSet<u32> = r.heap("t").iter().map(|(x, _)| x.page_id).collect();
    let n_rows = r.rows("SELECT id, v FROM t;");
    let keys: Vec<Value> = (1..=82).map(Value::Integer).collect();
    let ix = r.by_key("t", &keys);
    let missing: Vec<&Value> = ix.iter().filter(|(_, v)| v.is_none()).map(|(k, _)| k).collect();
    println!(
        "  fresh reader after recover: {} cols {:?}, {} heap tuples over pages {:?}, SELECT -> \
         {:?}, index misses: {:?}",
        r.shape("t").len(),
        r.shape("t").iter().map(|c| c.0.clone()).collect::<Vec<_>>(),
        r.heap("t").len(),
        pages,
        n_rows,
        missing
    );
    assert_eq!(r.shape("t").len(), 2, "the spinning ALTER installed its new schema");
    assert_eq!(n_rows, Ok(82), "the spinning ALTER lost rows");
    assert_eq!(r.heap("t").len(), 82, "the spinning ALTER lost heap tuples");
    assert!(missing.is_empty(), "the spinning ALTER lost index entries: {missing:?}");
}
