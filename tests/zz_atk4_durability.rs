//! I19 adversarial pass 4 — durability and recovery around a REFUSED `ALTER TABLE`.
//!
//! Claim under attack: "an ALTER that is refused leaves the table exactly as it was ... and that
//! holds after a checkpoint, a flush and a reopen into a fresh buffer pool."
//! These fixtures attack the halves the committed suite does not measure: the raw bytes of the
//! database FILE (not just the live tuples), a crash with no clean flush at all, a refusal
//! followed by a real ALTER, and whether `reserve_free_space` — the guard added by 58804c8 —
//! terminates.

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
use ferrodb::storage::heap_page::MAX_TUPLE_SIZE;
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

/// Open (or create) the database in `dir`. The directory is NOT owned here, so the caller can drop
/// every handle — the crash — and still read the files afterwards.
fn open(dir: &Path, recover: bool) -> Db {
    let path = dir.join("alter.db");
    let fresh = !path.exists();
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    if recover {
        ferrodb::wal::recovery::recover(&txn).expect("recover");
    }
    let catalog =
        if fresh { Catalog::create(bp.clone()).unwrap() } else { Catalog::open(bp.clone(), 1).unwrap() };
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

    fn by_key(&self, table: &str, keys: &[Value]) -> Vec<(Value, Option<RecordId>)> {
        let root = self.catalog.get_table(table).unwrap().primary_index_root;
        let ix = BPlusTreeManager::<Value, RecordId>::open(root, self.bp.clone());
        keys.iter().map(|k| (k.clone(), ix.search(k).unwrap())).collect()
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

    fn stats_of(&self, table: &str) -> String {
        format!("{:?}", self.catalog.stats.get(table))
    }
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    shape: Vec<(String, DataType)>,
    heap: Vec<(RecordId, Vec<u8>)>,
    rows: Result<Vec<Vec<Value>>, String>,
    by_key: Vec<(Value, Option<RecordId>)>,
}

fn snapshot(d: &mut Db, table: &str, select: &str, keys: &[Value]) -> Snapshot {
    Snapshot { shape: d.shape(table), heap: d.heap(table), rows: d.rows(select), by_key: d.by_key(table, keys) }
}

/// Every byte of both files, and a short digest for messages.
fn files(dir: &Path) -> (Vec<u8>, Vec<u8>) {
    let db = std::fs::read(dir.join("alter.db")).unwrap_or_default();
    let wal = std::fs::read(dir.join("alter.wal")).unwrap_or_default();
    (db, wal)
}

fn describe(name: &str, a: &[u8], b: &[u8]) -> String {
    if a == b {
        return format!("{name}: identical ({} bytes)", a.len());
    }
    let first = a.iter().zip(b.iter()).position(|(x, y)| x != y);
    format!(
        "{name}: DIFFERS — was {} bytes, now {} bytes, first differing byte at {:?}",
        a.len(),
        b.len(),
        first
    )
}

// ---------------------------------------------------------------------------------------------
// 1. Does the guard added by 58804c8 terminate?
// ---------------------------------------------------------------------------------------------

/// `reserve_free_space` loops `while free_space() < bytes { find_or_make_page(usable) }` with
/// `usable = PAGE_SIZE - HEADER_SIZE - SLOT_ENTRY_SIZE`, and `find_or_make_page` asks the directory
/// for a page with `usable + SLOT_ENTRY_SIZE` free — which is exactly what an EMPTY page reports.
/// So the second iteration finds the page the first one made and makes nothing.
#[test]
fn find_or_make_page_makes_progress_on_consecutive_calls() {
    let dir = tempfile::tempdir().unwrap();
    let mut d = open(dir.path(), false);
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));");
    let pad = "y".repeat(60);
    for i in 1..=41 {
        d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
    }
    let root = d.catalog.get_table("t").unwrap().first_directory_page_id;
    let heap = HeapFileManager::open(root, d.bp.clone());

    let f0 = heap.free_space().unwrap();
    let p1 = heap.find_or_make_page(MAX_TUPLE_SIZE).unwrap();
    let f1 = heap.free_space().unwrap();
    let p2 = heap.find_or_make_page(MAX_TUPLE_SIZE).unwrap();
    let f2 = heap.free_space().unwrap();
    let p3 = heap.find_or_make_page(MAX_TUPLE_SIZE).unwrap();
    let f3 = heap.free_space().unwrap();
    println!("free_space: {f0} -> {f1} -> {f2} -> {f3}; pages: {p1}, {p2}, {p3}");
    assert!(
        f3 > f2 && f2 > f1,
        "reserve_free_space's loop body stopped adding space after the first page: free_space went \
         {f0} -> {f1} -> {f2} -> {f3} and find_or_make_page returned {p1}, {p2}, {p3}. The loop \
         `while free_space() < bytes` therefore cannot terminate once one empty page exists."
    );
}

/// The impact: an ordinary `ADD COLUMN` on a table whose rows need more than one page of room.
/// Run in a thread so a non-terminating rewrite is a failed assertion rather than a wedged suite.
#[test]
fn add_column_on_a_two_page_table_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().to_path_buf();
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        let mut d = open(&path, false);
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));");
        let pad = "y".repeat(60);
        for i in 1..=82 {
            d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
        }
        let pages: std::collections::BTreeSet<u32> =
            d.heap("t").iter().map(|(rid, _)| rid.page_id).collect();
        let _ = tx.send(format!("filled: {} rows over {} pages", d.heap("t").len(), pages.len()));
        let r = d.try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);");
        let _ = tx.send(match r { Ok(_) => "ALTER returned: Ok".to_string(), Err(e) => format!("ALTER refused: {e}") });
    });
    let filled = rx.recv_timeout(Duration::from_secs(120)).expect("the fixture never finished loading rows");
    println!("{filled}");
    match rx.recv_timeout(Duration::from_secs(45)) {
        Ok(s) => println!("{s}"),
        Err(_) => panic!(
            "`ALTER TABLE t ADD COLUMN w VARCHAR(10)` on a two-page table ({filled}) had not \
             returned — neither succeeded nor been refused — 45s after the rows were in place."
        ),
    }
}

// ---------------------------------------------------------------------------------------------
// 2. A refusal, then a crash with no clean flush of any kind
// ---------------------------------------------------------------------------------------------

#[test]
fn a_refused_add_column_leaves_both_files_byte_identical_across_a_crash() {
    let mut refusals = 0;
    for pad in 4028usize..=4040 {
        let dir = tempfile::tempdir().unwrap();
        let keys = [Value::Integer(1), Value::Integer(2)];
        let select = "SELECT id, a FROM t;";
        let (base_db, base_wal, before) = {
            let mut d = open(dir.path(), false);
            d.sql("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(4100));");
            d.sql("INSERT INTO t VALUES (1, 'small');");
            let big = "x".repeat(pad);
            if d.try_sql(&format!("INSERT INTO t VALUES (2, '{big}');")).is_err() {
                continue;
            }
            // A clean baseline on disk: everything before the ALTER is durable.
            d.bp.flush_all().unwrap();
            d.bp.disk_manager.sync().unwrap();
            d.wal.flush().unwrap();
            let (bdb, bwal) = files(dir.path());
            let before = snapshot(&mut d, "t", select, &keys);

            let Err(e) = d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);") else { continue };
            refusals += 1;

            // No flush, no checkpoint, no sync: the process dies here.
            let (mid_db, mid_wal) = files(dir.path());
            println!("pad={pad} refused with: {e}");
            println!("  in-place  {}", describe("alter.db", &bdb, &mid_db));
            println!("  in-place  {}", describe("alter.wal", &bwal, &mid_wal));
            (bdb, bwal, before)
        }; // every handle dropped — the crash

        let (crash_db, crash_wal) = files(dir.path());
        assert_eq!(
            base_db, crash_db,
            "pad={pad}: a REFUSED ALTER changed the database file across a crash with no clean \
             flush. {}",
            describe("alter.db", &base_db, &crash_db)
        );
        assert_eq!(
            base_wal, crash_wal,
            "pad={pad}: a REFUSED ALTER changed the WAL. {}",
            describe("alter.wal", &base_wal, &crash_wal)
        );

        let mut r = open(dir.path(), true);
        let after = snapshot(&mut r, "t", select, &keys);
        assert_eq!(
            before, after,
            "pad={pad}: after a refusal, a crash with no clean flush and a recovery, the table is \
             not what it was"
        );
    }
    assert!(refusals > 0, "no width refused; the fixture measured nothing (MAX_TUPLE_SIZE={MAX_TUPLE_SIZE})");
}

// ---------------------------------------------------------------------------------------------
// 3. A refusal must not poison the ALTER that comes after it
// ---------------------------------------------------------------------------------------------

#[test]
fn a_refusal_then_a_real_alter_is_durable_and_correct() {
    let mut refusals = 0;
    for pad in 4028usize..=4040 {
        let dir = tempfile::tempdir().unwrap();
        let keys = [Value::Integer(1), Value::Integer(2)];
        {
            let mut d = open(dir.path(), false);
            d.sql("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(4100));");
            d.sql("INSERT INTO t VALUES (1, 'small');");
            let big = "x".repeat(pad);
            if d.try_sql(&format!("INSERT INTO t VALUES (2, '{big}');")).is_err() {
                continue;
            }
            let Err(_) = d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);") else { continue };
            refusals += 1;

            // Narrow the offending row and run the same statement, which must now work.
            d.sql("UPDATE t SET a = 'narrow' WHERE id = 2;");
            d.sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);");
            d.sql("UPDATE t SET note = 'after' WHERE id = 1;");
            d.txn.checkpoint().expect("checkpoint");
            d.bp.flush_all().unwrap();
            d.bp.disk_manager.sync().unwrap();
        }
        let mut r = open(dir.path(), true);
        assert_eq!(r.shape("t").len(), 3, "pad={pad}: the successful ALTER did not survive the reopen");
        let rows = r.rows("SELECT id, a, note FROM t;").unwrap_or_else(|e| panic!("pad={pad}: {e}"));
        let mut got: Vec<String> = rows.iter().map(|r| format!("{r:?}")).collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                format!("{:?}", vec![Value::Integer(1), Value::Varchar("small".into()), Value::Varchar("after".into())]),
                format!("{:?}", vec![Value::Integer(2), Value::Varchar("narrow".into()), Value::Null]),
            ],
            "pad={pad}: after a refusal + a successful ALTER, a reopen reads back the wrong table"
        );
        let ix = r.by_key("t", &keys);
        assert!(ix.iter().all(|(_, rid)| rid.is_some()), "pad={pad}: primary index lost a key: {ix:?}");
    }
    assert!(refusals > 0, "no width refused; the fixture measured nothing");
}

// ---------------------------------------------------------------------------------------------
// 4. What a refusal leaves in the WAL, the retained DDL declaration and the statistics
// ---------------------------------------------------------------------------------------------

#[test]
fn a_refused_alter_leaves_the_wal_the_ddl_declaration_and_the_stats_alone() {
    let mut refusals = 0;
    let mut analyzed = 0;
    for pad in 4028usize..=4040 {
        let dir = tempfile::tempdir().unwrap();
        let mut d = open(dir.path(), false);
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(4100));");
        d.sql("INSERT INTO t VALUES (1, 'small');");
        let big = "x".repeat(pad);
        if d.try_sql(&format!("INSERT INTO t VALUES (2, '{big}');")).is_err() {
            continue;
        }
        if d.try_sql("ANALYZE t;").is_ok() {
            analyzed += 1;
        }
        d.wal.flush().unwrap();
        let (_, wal_before) = files(dir.path());
        let ddl_before = d.schema_changes();
        let stats_before = d.stats_of("t");

        let Err(e) = d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);") else { continue };
        refusals += 1;

        d.wal.flush().unwrap();
        let (_, wal_after) = files(dir.path());
        assert_eq!(
            wal_before, wal_after,
            "pad={pad}: a REFUSED ALTER appended to the WAL ({e}). {}",
            describe("alter.wal", &wal_before, &wal_after)
        );
        assert_eq!(ddl_before, d.schema_changes(), "pad={pad}: a REFUSED ALTER changed the retained DDL declaration");
        assert_eq!(stats_before, d.stats_of("t"), "pad={pad}: a REFUSED ALTER changed the statistics");
    }
    assert!(refusals > 0, "no width refused; the fixture measured nothing");
    println!("widths where ANALYZE ran: {analyzed}");
}

// ---------------------------------------------------------------------------------------------
// 5. Differential: two identical databases, one of which is handed a refusal, then both crash
//    with NO clean flush at any point. Recovery has to make them the same database.
// ---------------------------------------------------------------------------------------------

/// The crash test above flushes a clean baseline first, so the only writes it can see are the
/// refusal's own. This one never flushes at all: whatever the refused statement did to the buffer
/// pool — dirtying a page, forcing an eviction while it scans, moving the WAL — has to come out in
/// the wash of `recover`. The control arm runs every other statement in the same order.
#[test]
fn a_refusal_before_an_unflushed_crash_recovers_to_the_same_database() {
    // Large enough that pass 1's scan walks many pages, so an eviction during the refused
    // statement is possible rather than hypothetical.
    const ROWS: i32 = 150;
    let pad = "z".repeat(600);
    let select = "SELECT id, v FROM t;";
    let keys: Vec<Value> = (1..=ROWS).map(Value::Integer).collect();

    let build = |dir: &Path, refuse: bool| -> String {
        let mut d = open(dir, false);
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(4100));");
        for i in 1..=ROWS {
            d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
        }
        // One row that no ADD COLUMN can widen, so the refusal is the size precheck's.
        // 4034 is the width measured above at which the widened row becomes 4070 bytes,
        // i.e. one byte past MAX_TUPLE_SIZE, so pass 1 refuses before it reserves anything.
        let huge = "w".repeat(4034);
        d.sql(&format!("INSERT INTO t VALUES ({}, '{huge}');", ROWS + 1));
        let mut note = String::from("no alter attempted");
        if refuse {
            match d.try_sql("ALTER TABLE t ADD COLUMN c VARCHAR(20);") {
                Err(e) => note = format!("refused: {e}"),
                Ok(_) => panic!("the fixture's ALTER succeeded, so it measures nothing"),
            }
        }
        note
        // dropped here: no flush_all, no sync, no checkpoint — the crash.
    };

    let control = tempfile::tempdir().unwrap();
    let victim = tempfile::tempdir().unwrap();
    let note = build(control.path(), false);
    println!("control: {note}");
    let note = build(victim.path(), true);
    println!("victim: {}", &note[..note.len().min(120)]);
    assert!(note.starts_with("refused:"), "the victim arm was not refused");

    let mut keys_all = keys.clone();
    keys_all.push(Value::Integer(ROWS + 1));
    let mut c = open(control.path(), true);
    let mut v = open(victim.path(), true);
    let cs = snapshot(&mut c, "t", select, &keys_all);
    let vs = snapshot(&mut v, "t", select, &keys_all);
    assert_eq!(
        cs.shape, vs.shape,
        "after an unflushed crash and recovery, the refused arm has a different schema"
    );
    assert_eq!(
        cs.rows.as_ref().map(|r| r.len()),
        vs.rows.as_ref().map(|r| r.len()),
        "after an unflushed crash and recovery, the refused arm holds a different number of rows \
         (control {:?} vs refused {:?})",
        cs.rows.as_ref().map(|r| r.len()),
        vs.rows.as_ref().map(|r| r.len())
    );
    assert_eq!(cs.heap, vs.heap, "after an unflushed crash and recovery, the refused arm's heap bytes differ");
    assert_eq!(cs.by_key, vs.by_key, "after an unflushed crash and recovery, the primary index differs");
}

/// The exhausted-allocator refusal, crashed instead of checkpointed.
///
/// The committed suite checkpoints, flushes and syncs before it reopens. `reserve_free_space` can
/// add empty pages and update the page directory before it discovers the allocator is closed, and
/// none of that is logged — so if it leaves anything behind, a crash is where it shows.
#[test]
fn an_exhausted_allocator_refusal_leaves_nothing_behind_across_a_crash() {
    const ROWS: i32 = 41;
    let pad = "y".repeat(60);
    let select = "SELECT id, v FROM t;";
    let keys: Vec<Value> = (1..=ROWS).map(Value::Integer).collect();
    let dir = tempfile::tempdir().unwrap();

    let (base_db, base_wal, before) = {
        let mut d = open(dir.path(), false);
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));");
        for i in 1..=ROWS {
            d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
        }
        let pages: std::collections::BTreeSet<u32> =
            d.heap("t").iter().map(|(rid, _)| rid.page_id).collect();
        assert_eq!(pages.len(), 1, "the fixture must pack one page: {pages:?}");
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
        d.wal.flush().unwrap();
        let (bdb, bwal) = files(dir.path());
        let before = snapshot(&mut d, "t", select, &keys);

        let floor = d.bp.disk_manager.high_water().unwrap();
        d.bp.disk_manager.reserve_from(floor).unwrap();
        assert!(d.bp.new_page().is_err(), "the fixture did not close the allocator");

        let e = d
            .try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);")
            .err()
            .expect("an ALTER that cannot obtain a page must be refused");
        println!("exhaustion refusal: {e}");
        let (mid_db, mid_wal) = files(dir.path());
        println!("  {}", describe("alter.db", &bdb, &mid_db));
        println!("  {}", describe("alter.wal", &bwal, &mid_wal));
        (bdb, bwal, before)
    }; // crash: no flush, no sync, no checkpoint

    let (crash_db, crash_wal) = files(dir.path());
    assert_eq!(
        base_db, crash_db,
        "a REFUSED (allocator-exhausted) ALTER changed the database file across a crash. {}",
        describe("alter.db", &base_db, &crash_db)
    );
    assert_eq!(base_wal, crash_wal, "{}", describe("alter.wal", &base_wal, &crash_wal));

    let mut r = open(dir.path(), true);
    let after = snapshot(&mut r, "t", select, &keys);
    assert_eq!(before, after, "the table is not what it was after refusal + crash + recovery");
}

/// Durability of the non-terminating ALTER: while it spins, has anything reached the disk?
///
/// `reserve_free_space`'s one productive iteration allocates a page and writes a directory entry,
/// and the rewrite is unlogged, so anything the buffer pool wrote through before the loop wedged
/// would be a change on disk made by a statement that has neither succeeded nor been refused —
/// and one recovery cannot undo. The files are read through a fresh `File`, not through the
/// database's own handles, so nothing is flushed by the act of measuring.
#[test]
fn the_non_terminating_alter_writes_nothing_through_while_it_spins() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().to_path_buf();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        let mut d = open(&path, false);
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));");
        let pad = "y".repeat(60);
        for i in 1..=82 {
            d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
        }
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
        d.wal.flush().unwrap();
        let _ = ready_tx.send("baseline flushed".to_string());
        let _ = d.try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);");
        let _ = ready_tx.send("the ALTER returned after all".to_string());
    });
    assert_eq!(ready_rx.recv_timeout(Duration::from_secs(120)).unwrap(), "baseline flushed");
    let (base_db, base_wal) = files(dir.path());
    // Give the wedged loop time to write through if it is going to.
    assert!(
        ready_rx.recv_timeout(Duration::from_secs(20)).is_err(),
        "the ALTER completed, so this fixture is not measuring the wedged loop"
    );
    let (now_db, now_wal) = files(dir.path());
    println!("  {}", describe("alter.db", &base_db, &now_db));
    println!("  {}", describe("alter.wal", &base_wal, &now_wal));
    // What a reader that opened the database after the wedged process was killed would see. A
    // fresh handle on the same files, recovering the way the CLI does after a crash. (With
    // `recover: false` the same reader sees the 82 heap tuples but `SELECT` returns 0 rows: the
    // commit state of the inserts lives in the WAL, so that is an artifact of skipping recovery,
    // not of the wedged ALTER.)
    let mut r = open(dir.path(), true);
    let pages: std::collections::BTreeSet<u32> =
        r.heap("t").iter().map(|(rid, _)| rid.page_id).collect();
    let rows = r.rows("SELECT id, v FROM t;");
    println!(
        "  after the wedge, a fresh reader sees: {} columns, {} heap tuples over pages {:?}, SELECT -> {:?}",
        r.shape("t").len(),
        r.heap("t").len(),
        pages,
        rows.as_ref().map(|v| v.len())
    );
    assert_eq!(r.shape("t").len(), 2, "the wedged ALTER installed its schema");
    assert_eq!(rows.as_ref().map(|v| v.len()), Ok(82), "the wedged ALTER lost rows: {rows:?}");
    assert_eq!(
        base_db, now_db,
        "an ALTER that has neither succeeded nor been refused has already changed the database \
         file, and the rewrite is unlogged so recovery cannot undo it. {}",
        describe("alter.db", &base_db, &now_db)
    );
}
