//! LENS: independent attack on "a refused ALTER leaves the table EXACTLY as it was".
//!
//! Written from scratch. Deliberately compares LOGICAL content (the set of id->v pairs SELECT
//! returns) as well as physical bytes, so that a row merely RELOCATING to a new RecordId cannot be
//! mistaken for a row being lost.

use std::collections::BTreeMap;
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
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;
use ferrodb::storage::index::BPlusTreeManager;

struct Db {
    dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn db() -> Db { open_at(tempfile::tempdir().unwrap()) }

fn open_at(dir: tempfile::TempDir) -> Db {
    let path = dir.path().join("lens.db");
    let fresh = !path.exists();
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = if fresh { Catalog::create(bp.clone()).unwrap() } else { Catalog::open(bp.clone(), 1).unwrap() };
    let wal = Arc::new(WalManager::new(dir.path().join("lens.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { dir, catalog, bp, txn, runtime, session }
}

impl Db {
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(FerroError::SqlParseError(p.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; ")));
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
}

/// LOGICAL: id -> the v column, read back through SELECT. `None` for the whole thing means the
/// table could not be read at all.
type Logical = Result<BTreeMap<i64, String>, String>;

#[derive(Debug, PartialEq, Eq)]
enum IdxRead { NoEntry, Readable(usize), Dangling(String) }

struct Snap {
    shape: Vec<(String, DataType)>,
    phys: BTreeMap<RecordId, Vec<u8>>,
    logical: Logical,
    idx: BTreeMap<i64, (Option<RecordId>, IdxRead)>,
}

fn as_i64(v: &Value) -> Option<i64> {
    match v { Value::Integer(i) => Some(*i as i64), _ => None }
}
fn as_s(v: &Value) -> String { format!("{v:?}") }

fn snap(d: &mut Db, table: &str, ids: &[i64]) -> Snap {
    let t = d.catalog.get_table(table).unwrap().clone();
    let shape = t.schema.columns.iter().map(|c| (c.name.clone(), c.data_type.clone())).collect();
    let heap = HeapFileManager::open(t.first_directory_page_id, d.bp.clone());
    let phys: BTreeMap<RecordId, Vec<u8>> =
        heap.scan().map(|r| { let (rid, tp) = r.unwrap(); (rid, tp.data) }).collect();

    let logical: Logical = {
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            d.try_sql(&format!("SELECT id, v FROM {table};"))
        }));
        match out {
            Err(_) => Err("SELECT PANICKED".to_string()),
            Ok(Err(e)) => Err(format!("SELECT errored: {e}")),
            Ok(Ok(Outcome::Rows(rows))) => {
                let mut m = BTreeMap::new();
                for r in rows {
                    let k = as_i64(&r[0]).unwrap_or(-1);
                    m.insert(k, as_s(&r[1]));
                }
                Ok(m)
            }
            Ok(Ok(_)) => Err("SELECT returned no rows outcome".to_string()),
        }
    };

    let ix = BPlusTreeManager::<Value, RecordId>::open(
        d.catalog.get_table(table).unwrap().primary_index_root, d.bp.clone());
    let heap2 = HeapFileManager::open(t.first_directory_page_id, d.bp.clone());
    let mut idx = BTreeMap::new();
    for id in ids {
        let rid = ix.search(&Value::Integer(*id as i32)).unwrap();
        let st = match rid {
            None => IdxRead::NoEntry,
            Some(r) => match heap2.read(r) {
                Ok(tp) => IdxRead::Readable(tp.data.len()),
                Err(e) => IdxRead::Dangling(e.to_string()),
            },
        };
        idx.insert(*id, (rid, st));
    }
    Snap { shape, phys, logical, idx }
}

/// Build a heap that is ONE packed data page, so any widening must relocate. Returns the ids.
fn packed_one_page(d: &mut Db) -> Vec<i64> {
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));");
    let v = "y".repeat(60);
    d.sql(&format!("INSERT INTO t VALUES (1, '{v}');"));
    let t = d.catalog.get_table("t").unwrap().clone();
    let l = HeapFileManager::open(t.first_directory_page_id, d.bp.clone())
        .scan().next().unwrap().unwrap().1.data.len();
    let k = 4073 / (l + 4);
    for i in 2..=k { d.sql(&format!("INSERT INTO t VALUES ({i}, '{v}');")); }
    let pages: std::collections::BTreeSet<u32> =
        HeapFileManager::open(t.first_directory_page_id, d.bp.clone())
            .scan().map(|r| r.unwrap().0.page_id).collect();
    println!("  fixture: tuple={l} bytes, {k} rows, data pages={pages:?}");
    assert_eq!(pages.len(), 1, "fixture spilled to a 2nd page; widening would find room there");
    (1..=k as i64).collect()
}

fn report(tag: &str, before: &Snap, after: &Snap) {
    println!("{tag}: heap tuples before={} after={}", before.phys.len(), after.phys.len());
    println!("{tag}: shape same = {}", before.shape == after.shape);
    let (lb, la) = (&before.logical, &after.logical);
    match (lb, la) {
        (Ok(b), Ok(a)) => {
            println!("{tag}: SELECT rows before={} after={}", b.len(), a.len());
            let lost: Vec<i64> = b.keys().filter(|k| !a.contains_key(k)).cloned().collect();
            let gained: Vec<i64> = a.keys().filter(|k| !b.contains_key(k)).cloned().collect();
            let mutated: Vec<i64> = b.iter().filter(|(k, v)| a.get(k).map_or(false, |v2| v2 != *v)).map(|(k, _)| *k).collect();
            println!("{tag}: ids LOST from SELECT: {lost:?}");
            println!("{tag}: ids GAINED: {gained:?}");
            println!("{tag}: ids whose v VALUE changed: {mutated:?}");
        }
        _ => println!("{tag}: SELECT before={lb:?} after={la:?}"),
    }
    let moved: Vec<i64> = before.idx.iter()
        .filter(|(k, (r, _))| after.idx.get(k).map(|(r2, _)| r2 != r).unwrap_or(true))
        .map(|(k, _)| *k).collect();
    println!("{tag}: index answers that MOVED (relocation): {moved:?}");
    let dangling: Vec<(i64, &IdxRead)> = after.idx.iter()
        .filter(|(_, (_, s))| matches!(s, IdxRead::Dangling(_)))
        .map(|(k, (_, s))| (*k, s)).collect();
    println!("{tag}: index entries now DANGLING: {dangling:?}");
    let vanished: Vec<i64> = after.idx.iter().filter(|(_, (r, _))| r.is_none()).map(|(k, _)| *k).collect();
    println!("{tag}: index entries now ABSENT: {vanished:?}");
}

fn durable(d: &mut Db) { d.txn.checkpoint().unwrap(); d.bp.flush_all().unwrap(); d.bp.disk_manager.sync().unwrap(); }

// ------------------------------------------------------------------ lane A: control, no bound

#[test]
fn lane_a_the_same_alter_with_no_arena_bound_succeeds_and_keeps_every_row() {
    let mut d = db();
    let ids = packed_one_page(&mut d);
    durable(&mut d);
    let before = snap(&mut d, "t", &ids);
    let res = d.try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);");
    println!("A: ALTER -> {:?}", res.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    let after = snap(&mut d, "t", &ids);
    report("A", &before, &after);
    assert!(res.is_ok(), "control lane: the ALTER must succeed here, else the fixture proves nothing");
    let (b, a) = (before.logical.as_ref().unwrap(), after.logical.as_ref().unwrap());
    assert_eq!(b.len(), a.len(), "control: a SUCCESSFUL alter lost rows");
    assert!(b.keys().all(|k| a.contains_key(k)), "control: a SUCCESSFUL alter lost an id");
}

// ------------------------------------------------------------------ lane B: the size guard itself

#[test]
fn lane_b_the_size_refusal_leaves_the_table_untouched() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(4100));");
    // Find the widest v that INSERTs, so the row is legal now but ADD COLUMN pushes it over.
    let mut chosen = 0usize;
    for l in (3900..4100usize).rev() {
        let mut probe = db();
        probe.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(4100));");
        if probe.try_sql(&format!("INSERT INTO t VALUES (1, '{}');", "z".repeat(l))).is_ok() {
            chosen = l; break;
        }
    }
    assert!(chosen > 0, "no v width in 3900..4100 could be inserted");
    println!("B: widest insertable v = {chosen} chars");
    d.sql(&format!("INSERT INTO t VALUES (1, '{}');", "z".repeat(chosen)));
    d.sql(&format!("INSERT INTO t VALUES (2, '{}');", "q".repeat(10)));
    let ids = vec![1i64, 2];
    durable(&mut d);
    let before = snap(&mut d, "t", &ids);
    let res = d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(60);");
    println!("B: ALTER -> {:?}", res.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    let after = snap(&mut d, "t", &ids);
    report("B", &before, &after);
    assert!(res.is_err(), "lane B never reached the size refusal - it measures nothing");
    assert_eq!(before.phys, after.phys, "size-refused ALTER changed the raw heap bytes");
    assert_eq!(before.logical, after.logical, "size-refused ALTER changed what SELECT returns");
    assert_eq!(before.shape, after.shape, "size-refused ALTER changed the catalog shape");
    durable(&mut d);
    let dir = d.dir; let mut r = open_at(dir);
    let re = snap(&mut r, "t", &ids);
    println!("B: after checkpoint+flush+reopen: phys_same={} logical_same={}",
        re.phys == before.phys, re.logical == before.logical);
    assert_eq!(re.phys, before.phys, "size-refused ALTER differed after reopen");
    assert_eq!(re.logical, before.logical, "size-refused ALTER differed after reopen (logical)");
}

// ------------------------------------------------------------------ lane C: bound only, no ALTER

#[test]
fn lane_c_setting_the_arena_floor_alone_changes_nothing() {
    let mut d = db();
    let ids = packed_one_page(&mut d);
    durable(&mut d);
    let before = snap(&mut d, "t", &ids);
    let probe = d.bp.new_page().unwrap();
    d.bp.free_page(probe).unwrap();
    d.bp.disk_manager.reserve_from(probe).unwrap();
    println!("C: floor at {probe}; new_page -> {:?}", d.bp.new_page().map_err(|e| e.to_string().lines().next().unwrap().to_string()));
    let after = snap(&mut d, "t", &ids);
    report("C", &before, &after);
    assert_eq!(before.phys, after.phys, "bounding the arena ALONE changed the heap");
    assert_eq!(before.logical, after.logical, "bounding the arena ALONE changed SELECT");
}

// ------------------------------------------------------------------ lane D: the claim

#[test]
fn lane_d_refused_alter_when_pass2_cannot_allocate() {
    let mut d = db();
    let ids = packed_one_page(&mut d);
    durable(&mut d);
    let before = snap(&mut d, "t", &ids);

    let probe = d.bp.new_page().unwrap();
    d.bp.free_page(probe).unwrap();
    d.bp.disk_manager.reserve_from(probe).unwrap();
    let na = d.bp.new_page().map_err(|e| e.to_string().lines().next().unwrap().to_string());
    println!("D: floor at {probe}; new_page -> {na:?}");
    assert!(na.is_err(), "the bound did not take - page space is not exhausted");

    let res = d.try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);");
    let err = res.as_ref().err().map(|e| e.to_string().lines().next().unwrap().to_string());
    println!("D: ALTER -> {:?}", res.as_ref().map(|_| "Ok").map_err(|_| err.clone().unwrap()));
    assert!(res.is_err(), "the ALTER SUCCEEDED - there is no refusal to attack");

    let after = snap(&mut d, "t", &ids);
    report("D", &before, &after);

    durable(&mut d);
    let dir = d.dir; let mut r = open_at(dir);
    let re = snap(&mut r, "t", &ids);
    println!("D: --- after checkpoint + flush + sync + reopen into a FRESH buffer pool ---");
    report("D-reopen", &before, &re);

    let b = before.logical.as_ref().expect("baseline SELECT must work");
    match &re.logical {
        Err(e) => panic!("REFUTATION FAILED: after a REFUSED alter the table no longer reads at all: {e}"),
        Ok(a) => {
            let lost: Vec<i64> = b.keys().filter(|k| !a.contains_key(k)).cloned().collect();
            assert!(lost.is_empty(),
                "REFUTATION FAILED: a REFUSED ALTER durably lost ids {lost:?}: {} rows became {}", b.len(), a.len());
        }
    }
    assert_eq!(re.shape, before.shape, "a REFUSED alter changed the catalog shape");
}
