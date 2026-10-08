//! ATK2: attack the premise under I19's ALTER-refusal guard.
//!
//! The guard rests on: "`HeapFileManager::insert` can always place any tuple of `MAX_TUPLE_SIZE`
//! bytes or fewer." Four fixtures:
//!   t1 boundary agreement between MAX_TUPLE_SIZE, Page::insert and find_page_with_space
//!   t2 the premise after heavy churn on a heap whose page DIRECTORY had to chain
//!   t3 pass 2 failing AFTER pass 1 passed, via a starved buffer pool (the stated blind spot)
//!   t4 over-refusal: the widest ADD COLUMN that succeeds vs the narrowest that is refused

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
use ferrodb::storage::heap_page::{Page, MAX_TUPLE_SIZE};
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::page_directory::PageDirectory;
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

// ---------------------------------------------------------------- raw storage harness

fn raw_pool() -> (tempfile::TempDir, Arc<BufferPoolManager>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).open(dir.path().join("raw.db")).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    (dir, bp)
}

fn body(n: usize, seed: u8) -> Tuple {
    Tuple::new((0..n).map(|i| (i as u8).wrapping_add(seed)).collect())
}

/// (page_id, what the directory claims is free, what the page actually has free)
fn dir_vs_pages(bp: &Arc<BufferPoolManager>, first_dir: u32) -> Vec<(u32, i64, i64)> {
    let mut out = Vec::new();
    let mut dir_id = first_dir;
    loop {
        let fi = bp.fetch_page(dir_id).unwrap();
        let dir = PageDirectory::deserialize(bp.frames[fi].read().unwrap().data);
        bp.unpin_page(dir_id, false);
        for e in &dir.entries {
            let pi = bp.fetch_page(e.page_id).unwrap();
            let page = Page::deserialize(bp.frames[pi].read().unwrap().data).unwrap();
            bp.unpin_page(e.page_id, false);
            let actual = page.get_free_space_end() as i64 - page.get_free_space_start() as i64;
            out.push((e.page_id, e.free_space as i64, actual));
        }
        if dir.next_page_directory == 0 { return out }
        dir_id = dir.next_page_directory;
    }
}

fn count_dir_pages(bp: &Arc<BufferPoolManager>, first_dir: u32) -> usize {
    let mut n = 0;
    let mut dir_id = first_dir;
    loop {
        n += 1;
        let fi = bp.fetch_page(dir_id).unwrap();
        let dir = PageDirectory::deserialize(bp.frames[fi].read().unwrap().data);
        bp.unpin_page(dir_id, false);
        if dir.next_page_directory == 0 { return n }
        dir_id = dir.next_page_directory;
    }
}

/// The premise, stated as a probe: every size up to MAX_TUPLE_SIZE must be insertable, whatever
/// state the heap is in, because `insert` allocates a fresh page when no existing page has room.
fn probe_premise(heap: &HeapFileManager, label: &str) -> Vec<(usize, String)> {
    let mut broken = Vec::new();
    for n in [MAX_TUPLE_SIZE, MAX_TUPLE_SIZE - 1, 4000, 3000, 2048, 1360, 1024, 100, 10, 1] {
        match heap.insert(body(n, 0xAB)) {
            Ok(rid) => {
                let got = heap.read(rid).unwrap().data.len();
                if got != n { broken.push((n, format!("{label}: read back {got} bytes"))) }
            }
            Err(e) => broken.push((n, format!("{label}: insert({n}) -> {e}"))),
        }
    }
    broken
}

// ---------------------------------------------------------------- SQL harness (ported)

struct Db {
    dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn db() -> Db { open_at(tempfile::tempdir().unwrap()) }

fn open_at(dir: tempfile::TempDir) -> Db {
    let path = dir.path().join("alter.db");
    let fresh = !path.exists();
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = if fresh { Catalog::create(bp.clone()).unwrap() } else { Catalog::open(bp.clone(), 1).unwrap() };
    let wal = Arc::new(WalManager::new(dir.path().join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { dir, catalog, wal, bp, txn, runtime, session }
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
        self.catalog.get_table(table).unwrap().schema.columns.iter()
            .map(|c| (c.name.clone(), c.data_type.clone())).collect()
    }
    fn heap(&self, table: &str) -> Vec<(RecordId, Vec<u8>)> {
        let root = self.catalog.get_table(table).unwrap().first_directory_page_id;
        HeapFileManager::open(root, self.bp.clone()).scan()
            .map(|r| { let (rid, t) = r.unwrap(); (rid, t.data) }).collect()
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
    primary_root: u32,
}

fn snapshot(d: &mut Db, table: &str, select: &str, keys: &[Value]) -> Snapshot {
    Snapshot {
        shape: d.shape(table),
        heap: d.heap(table),
        rows: d.rows(select),
        by_key: d.by_key(table, keys),
        primary_root: d.catalog.get_table(table).unwrap().primary_index_root,
    }
}

// ---------------------------------------------------------------- t1

#[test]
fn t1_max_tuple_size_agrees_with_every_gate_it_has_to_agree_with() {
    // Page::insert, both directions, on a genuinely empty page.
    let mut p = Page::empty(9);
    assert!(p.insert(body(MAX_TUPLE_SIZE, 1)).is_ok(), "Page::insert refused MAX_TUPLE_SIZE");
    let mut p = Page::empty(9);
    match p.insert(body(MAX_TUPLE_SIZE + 1, 1)) {
        Err(FerroError::NotEnoughSpace) => {}
        other => panic!("Page::insert accepted MAX_TUPLE_SIZE+1: {other:?}"),
    }

    // Page::update's in-place grow branch checks `>= new_len` with NO slot-entry term. Prove it
    // still cannot exceed MAX_TUPLE_SIZE: put one tuple on a page, then grow it as far as it goes.
    let mut p = Page::empty(9);
    p.insert(body(1, 2)).unwrap();
    let mut widest_in_place = 0usize;
    for n in 1..=(MAX_TUPLE_SIZE + 8) {
        let mut q = Page::empty(9);
        q.insert(body(1, 2)).unwrap();
        if q.update(0, body(n, 3)).is_ok() { widest_in_place = n } 
    }
    println!("t1: widest tuple Page::update can grow into a 1-tuple page = {widest_in_place} (MAX_TUPLE_SIZE={MAX_TUPLE_SIZE})");
    assert!(widest_in_place <= MAX_TUPLE_SIZE,
        "Page::update stored {widest_in_place} bytes, above MAX_TUPLE_SIZE {MAX_TUPLE_SIZE}");

    // HeapFileManager::insert, fresh heap: the premise itself.
    let (_d, bp) = raw_pool();
    let heap = HeapFileManager::new(bp.clone()).unwrap();
    let dir_entries_before = dir_vs_pages(&bp, heap.first_directory_page_id).len();
    assert_eq!(heap.find_page_with_space(MAX_TUPLE_SIZE as u16 + 4).unwrap(), None,
        "a heap with no data pages claimed to have room");
    let broken = probe_premise(&heap, "fresh heap");
    assert!(broken.is_empty(), "premise broken on a fresh heap: {broken:?}");

    // One over: refused, and note whether the refusal leaks the page it allocated.
    let before = dir_vs_pages(&bp, heap.first_directory_page_id);
    let over = heap.insert(body(MAX_TUPLE_SIZE + 1, 4));
    let after = dir_vs_pages(&bp, heap.first_directory_page_id);
    println!("t1: insert(MAX_TUPLE_SIZE+1) -> {over:?}");
    println!("t1: directory pages entries before={} after={} (start {})",
        before.len(), after.len(), dir_entries_before);
    assert!(over.is_err(), "heap accepted a tuple of MAX_TUPLE_SIZE+1 bytes");

    // Directory must never claim more free space than the page has.
    let over_states: Vec<_> = after.iter().filter(|(_, d, a)| d > a).collect();
    println!("t1: directory entries (page, claimed, actual) = {after:?}");
    assert!(over_states.is_empty(), "directory OVERSTATES free space: {over_states:?}");
}

// ---------------------------------------------------------------- t2

#[test]
fn t2_premise_survives_churn_on_a_heap_whose_directory_had_to_chain() {
    let (_d, bp) = raw_pool();
    let heap = HeapFileManager::new(bp.clone()).unwrap();

    // 1350-byte tuples pack 3 to a page, leaving 11 bytes: growing any of them relocates.
    let n = 2200usize;
    let mut rids: Vec<RecordId> = Vec::with_capacity(n);
    for i in 0..n { rids.push(heap.insert(body(1350, i as u8)).unwrap()) }
    let dir_pages = count_dir_pages(&bp, heap.first_directory_page_id);
    let entries = dir_vs_pages(&bp, heap.first_directory_page_id);
    println!("t2: {n} tuples -> {} data pages across {dir_pages} directory page(s)", entries.len());
    assert!(dir_pages > 1, "fixture never chained the directory ({dir_pages} page) - it measures nothing");

    // Grow every tuple. Every one of these must relocate (page free space is 11 bytes).
    let mut relocated = 0usize;
    for i in 0..n {
        let new_rid = heap.update(rids[i], body(1360, i as u8))
            .unwrap_or_else(|e| panic!("update #{i} failed: {e}"));
        if new_rid != rids[i] { relocated += 1 }
        rids[i] = new_rid;
    }
    println!("t2: {relocated}/{n} tuples relocated");
    assert!(relocated > n / 2, "fixture forced almost no relocations ({relocated}) - it measures nothing");

    // Every tuple still readable and correct.
    for i in 0..n {
        let got = heap.read(rids[i]).unwrap().data;
        assert_eq!(got, body(1360, i as u8).data, "tuple {i} came back wrong after relocation");
    }

    let entries = dir_vs_pages(&bp, heap.first_directory_page_id);
    let overs: Vec<_> = entries.iter().filter(|(_, d, a)| d > a).collect();
    let unders = entries.iter().filter(|(_, d, a)| d < a).count();
    println!("t2: {} data pages, {} directory entries OVERSTATE free space, {} understate",
        entries.len(), overs.len(), unders);
    if !overs.is_empty() {
        println!("t2: worst overstatements: {:?}", &overs[..overs.len().min(10)]);
    }

    // The premise, after all that churn.
    let broken = probe_premise(&heap, "churned heap");
    println!("t2: premise probe -> {broken:?}");
    assert!(broken.is_empty(), "premise broken after churn: {broken:?}");
    assert!(overs.is_empty(), "directory OVERSTATES free space after churn: {:?}", &overs[..overs.len().min(10)]);
}

// ---------------------------------------------------------------- t3

/// Pin every frame but `leave`, using only public API. Returns the pages still pinned.
fn starve(bp: &Arc<BufferPoolManager>, leave: usize) -> Vec<u32> {
    let mut spare = Vec::new();
    for _ in 0..1200 {
        match bp.new_page() { Ok(p) => spare.push(p), Err(_) => break }
    }
    let mut pinned = Vec::new();
    for p in spare {
        match bp.fetch_page(p) { Ok(_) => pinned.push(p), Err(_) => break }
    }
    for _ in 0..leave {
        if let Some(p) = pinned.pop() { bp.unpin_page(p, false) }
    }
    pinned
}

fn release(bp: &Arc<BufferPoolManager>, pinned: Vec<u32>) {
    for p in pinned { bp.unpin_page(p, false) }
}

#[test]
fn t3_pass2_failing_after_pass1_passed_under_a_starved_buffer_pool() {
    let select = "SELECT id FROM t;";
    let keys: Vec<Value> = (1..=8).map(Value::Integer).collect();
    let mut any_pass2_failure = false;

    for leave in [1usize, 2, 3, 4, 6, 8] {
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(60));");
        for i in 1..=400 { d.sql(&format!("INSERT INTO t VALUES ({i}, 'abcdefghijklmnopqrstuvwxyz0123');")); }
        d.txn.checkpoint().unwrap();
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();

        let before = snapshot(&mut d, "t", select, &keys);
        let pinned = starve(&d.bp, leave);
        let n_pinned = pinned.len();
        let res = d.try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);");
        release(&d.bp, pinned);

        let after = snapshot(&mut d, "t", select, &keys);
        let heap_changed = before.heap != after.heap;
        let shape_changed = before.shape != after.shape;
        println!("t3: leave={leave} pinned={n_pinned} alter={:?} heap_changed={heap_changed} shape_changed={shape_changed}",
            res.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));

        if res.is_err() {
            if heap_changed {
                any_pass2_failure = true;
                let diff = before.heap.iter().zip(after.heap.iter()).filter(|(a, b)| a != b).count();
                println!("t3: !!! REFUSED ALTER CHANGED THE HEAP. rows before={} after={} differing_positions={diff}",
                    before.heap.len(), after.heap.len());
                println!("t3: shape before={:?} after={:?}", before.shape, after.shape);
                println!("t3: rows before ok={} after ok={}", before.rows.is_ok(), after.rows.is_ok());
                if before.rows.is_ok() != after.rows.is_ok() { println!("t3: after rows = {:?}", after.rows); }
                let bad_keys: Vec<_> = before.by_key.iter().zip(after.by_key.iter())
                    .filter(|(a, b)| a != b).take(5).collect();
                println!("t3: primary-index answers that changed: {bad_keys:?}");

                // Durability: does it survive checkpoint + flush + reopen into a fresh pool?
                d.txn.checkpoint().unwrap();
                d.bp.flush_all().unwrap();
                d.bp.disk_manager.sync().unwrap();
                let dir = d.dir;
                let mut r = open_at(dir);
                let reopened = snapshot(&mut r, "t", select, &keys);
                println!("t3: after checkpoint+flush+reopen: heap_still_differs={} rows_ok={} len={}",
                    reopened.heap != before.heap, reopened.rows.is_ok(), reopened.heap.len());
            } else {
                println!("t3: leave={leave}: failed with the heap untouched (pass 1 or earlier) - safe");
            }
        }
    }
    println!("t3: any pass-2 failure observed = {any_pass2_failure}");
}

// ---------------------------------------------------------------- t4

#[test]
fn t4_the_refusal_boundary_is_not_an_over_refusal() {
    let mut widest_ok: Option<(usize, usize)> = None; // (varchar len, resulting max tuple bytes)
    let mut narrowest_refused: Option<(usize, String)> = None;

    for l in 4020..4080usize {
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(4100));");
        let big = "x".repeat(l);
        if d.try_sql(&format!("INSERT INTO t VALUES (1, '{big}');")).is_err() { continue }
        match d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);") {
            Ok(_) => {
                let widest = d.heap("t").iter().map(|(_, b)| b.len()).max().unwrap();
                widest_ok = Some((l, widest));
            }
            Err(e) => {
                if narrowest_refused.is_none() { narrowest_refused = Some((l, e.to_string())) }
            }
        }
    }

    let (ok_l, ok_bytes) = widest_ok.expect("no width in the sweep produced a SUCCESSFUL alter");
    let (bad_l, msg) = narrowest_refused.expect("no width in the sweep produced a REFUSED alter");
    println!("t4: widest accepted VARCHAR len={ok_l} -> heap tuple {ok_bytes} bytes (MAX_TUPLE_SIZE={MAX_TUPLE_SIZE})");
    println!("t4: narrowest refused VARCHAR len={bad_l}; message: {msg}");

    // The reported byte count in the refusal must really be unplaceable.
    let reported: usize = msg.split("would become ").nth(1)
        .and_then(|s| s.split(' ').next()).and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("refusal message has no 'would become N bytes': {msg}"));
    println!("t4: refusal claims the row would become {reported} bytes");
    assert!(reported > MAX_TUPLE_SIZE,
        "OVER-REFUSAL: refused a row of {reported} bytes, which is <= MAX_TUPLE_SIZE {MAX_TUPLE_SIZE}");
    let mut p = Page::empty(1);
    assert!(p.insert(body(reported, 0)).is_err(),
        "OVER-REFUSAL: a fresh page accepts {reported} bytes but the ALTER refused it");
    assert_eq!(bad_l, ok_l + 1, "the accept/refuse boundary is not contiguous: ok={ok_l} refused={bad_l}");
}

// ---------------------------------------------------------------- t5

/// Pass 2 failing AFTER pass 1 passed, with nothing about the DATA out of range.
///
/// The heap is one page, packed. Every widened row therefore has to relocate, and the first
/// relocation needs a page the heap does not have yet — so `insert` calls `new_page`. The page
/// space is then bounded with the public `DiskManager::reserve_from`, which is what the arena
/// store does, so `allocate` refuses. `HeapFileManager::update` has already deleted the slot by
/// then.
#[test]
fn t5_a_refused_alter_destroys_a_row_when_pass2_cannot_allocate() {
    let select = "SELECT id FROM t;";
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));");
    let v = "y".repeat(60);
    d.sql(&format!("INSERT INTO t VALUES (1, '{v}');"));
    let l = d.heap("t")[0].1.len();
    let k = 4073 / (l + 4);
    for i in 2..=k { d.sql(&format!("INSERT INTO t VALUES ({i}, '{v}');")); }
    let pages: std::collections::BTreeSet<u32> = d.heap("t").iter().map(|(r, _)| r.page_id).collect();
    println!("t5: tuple bytes={l}, {k} rows, heap spans {} data page(s): {pages:?}", pages.len());
    assert_eq!(pages.len(), 1, "fixture spilled onto a second page - the first relocation would find room");

    let keys: Vec<Value> = (1..=k as i32).map(Value::Integer).collect();
    d.txn.checkpoint().unwrap();
    d.bp.flush_all().unwrap();
    d.bp.disk_manager.sync().unwrap();
    let before = snapshot(&mut d, "t", select, &keys);

    // Bound the page space at the very next page the allocator would hand out.
    let probe = d.bp.new_page().unwrap();
    d.bp.free_page(probe).unwrap();
    d.bp.disk_manager.reserve_from(probe).unwrap();
    println!("t5: arena floor set to {probe}; new_page now -> {:?}", d.bp.new_page().map_err(|e| e.to_string()));

    let res = d.try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);");
    println!("t5: ALTER -> {:?}", res.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    assert!(res.is_err(), "the fixture never produced a failing ALTER - it measures nothing");

    let after = snapshot(&mut d, "t", select, &keys);
    println!("t5: rows in heap before={} after={}", before.heap.len(), after.heap.len());
    println!("t5: shape before={:?}", before.shape);
    println!("t5: shape after ={:?}", after.shape);
    let lost: Vec<_> = before.heap.iter().filter(|(r, _)| !after.heap.iter().any(|(r2, _)| r2 == r)).map(|(r, _)| *r).collect();
    let changed: Vec<_> = before.heap.iter()
        .filter(|(r, b)| after.heap.iter().any(|(r2, b2)| r2 == r && b2 != b)).map(|(r, _)| *r).collect();
    println!("t5: slots that VANISHED: {lost:?}");
    println!("t5: slots whose BYTES changed: {changed:?}");
    println!("t5: SELECT after = {:?}", after.rows.as_ref().map(|r| r.len()));
    let idx_lost: Vec<_> = before.by_key.iter().zip(after.by_key.iter()).filter(|(a, b)| a != b).collect();
    println!("t5: primary-index answers that changed: {idx_lost:?}");

    // Durability.
    d.txn.checkpoint().unwrap();
    d.bp.flush_all().unwrap();
    d.bp.disk_manager.sync().unwrap();
    let dir = d.dir;
    let mut r = open_at(dir);
    let re = snapshot(&mut r, "t", select, &keys);
    println!("t5: after checkpoint+flush+reopen into a fresh pool: heap rows={} (was {}), differs_from_before={}",
        re.heap.len(), before.heap.len(), re.heap != before.heap);
    println!("t5: reopened SELECT = {:?}", re.rows.as_ref().map(|x| x.len()));

    assert_eq!(re.heap, before.heap,
        "a FAILED ALTER changed the table durably: {} live tuples became {}", before.heap.len(), re.heap.len());
}
