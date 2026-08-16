//! Provenance and causal rollback, end to end against the real engine.
//!
//! Design authority: DESIGN.md section 2, exit criteria 9 and 10.
//!
//! Nothing here is a mock. Rows go in through the SQL front end, reads come out of real scan
//! operators over a real buffer pool, and every `begin_ts` in a retained read-set is read from
//! the actual 24-byte version header. Two agent runs are simulated: `restock-agent` updates a
//! row, `auditor-agent` reads that row and writes a derived one. Reverting the first must find
//! the second.

use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Executor};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::lower;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::planner::physical_plan::PhysicalPlan;
use ferrodb::provenance::capture::{
    CapturingScan, ProvenanceLog, SurrogateColumn, TxnCapture, VersionSource, WriteRecord,
};
use ferrodb::provenance::readset::{AccessShape, Bound, PredicateSummary, ReadSetForm, VersionRef};
use ferrodb::provenance::revert::RevertMode;
use ferrodb::provenance::store::MemProvenanceStore;
use ferrodb::provenance::{ProvId, ProvenanceStore, RunEntity};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::tel::ids::{ColId, RowId, TableId, TxnId};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::{ReadView, TxnManager};

use ferrodb::branch::types::BranchId;

const INVENTORY: TableId = TableId(1);
const QTY: ColId = ColId(1);

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn sql(&mut self, sql: &str) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let stmts = Parser::new(tokens).parse();
        let mut session = Session::new();
        for stmt in stmts {
            run(stmt, &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut session).unwrap();
        }
    }

    fn view(&self) -> Arc<ReadView> {
        Arc::new(ReadView { snapshot: self.txn.read_snapshot(), txn_id: 0 })
    }

    fn heap(&self, table: &str) -> HeapFileManager {
        let entry = self.catalog.get_table(table).unwrap();
        HeapFileManager::open(entry.first_directory_page_id, self.bp.clone())
    }

    fn scan(&self, table: &str) -> Box<dyn Executor> {
        lower(
            PhysicalPlan::SeqScan { table: table.into() },
            &self.catalog,
            self.bp.clone(),
            self.view(),
        )
        .unwrap()
    }

    /// The physical slot currently holding the row with this surrogate id.
    fn rid_of(&self, table: &str, id: i32) -> RecordId {
        let mut exec = self.scan(table);
        while let Some(row) = exec.next() {
            let (rid, values) = row.unwrap();
            if values[0] == Value::Integer(id) {
                return rid;
            }
        }
        panic!("row {} not found in {}", id, table);
    }
}

fn setup() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prov.db");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("prov.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal, bp.clone()));
    let mut db = Db { catalog, bp, txn, _dir: dir };
    db.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");
    for s in [
        "INSERT INTO inventory VALUES (1, 10);",
        "INSERT INTO inventory VALUES (2, 20);",
        "INSERT INTO inventory VALUES (3, 30);",
    ] {
        db.sql(s);
    }
    db
}

fn agent_run(store: &MemProvenanceStore, agent: &str, run_id: &str, branch: u64) -> ProvId {
    store
        .intern(&RunEntity::new(
            ProvId::NONE,
            agent,
            run_id,
            "claude-opus",
            "2026-05",
            [0xab; 32],
            1_700_000_000_000,
            BranchId::new(branch, 0),
        ))
        .unwrap()
}

/// Read row `id` through a real scan operator, retaining the exact version that was read.
fn point_read(db: &Db, capture: &Arc<Mutex<TxnCapture>>, id: i32) -> Value {
    let heap = Arc::new(db.heap("inventory"));
    let observed_at = db.txn.read_snapshot().high_water;
    let inner = lower(
        PhysicalPlan::IndexScan {
            table: "inventory".into(),
            column: 0,
            lower: std::ops::Bound::Included(Value::Integer(id)),
            upper: std::ops::Bound::Included(Value::Integer(id)),
        },
        &db.catalog,
        db.bp.clone(),
        db.view(),
    )
    .unwrap();
    let mut scan = CapturingScan::new(
        inner,
        capture.clone(),
        INVENTORY,
        AccessShape::IndexLookup,
        Arc::new(SurrogateColumn(0)),
        heap,
        None,
        observed_at,
    );
    let mut qty = Value::Null;
    while let Some(row) = scan.next() {
        let (_, values) = row.unwrap();
        qty = values[1].clone();
    }
    assert_ne!(qty, Value::Null, "point read of row {} returned nothing", id);
    qty
}

/// Record (and attribute) a write this run just made through SQL, reading the real version
/// header of the slot it landed in.
fn record_write(
    db: &Db,
    store: &MemProvenanceStore,
    capture: &Arc<Mutex<TxnCapture>>,
    id: i32,
    value: Value,
) -> VersionRef {
    let rid = db.rid_of("inventory", id);
    let begin_ts = db.heap("inventory").begin_ts(rid).unwrap();
    let v = VersionRef { tbl: INVENTORY, row: RowId(id as u64), rid, begin_ts };
    capture
        .lock()
        .unwrap()
        .on_write_stamped(store, WriteRecord::new(v, Some(QTY), Some(value)))
        .unwrap();
    v
}

/// Criterion 9 and criterion 10 in one run: two agents, a real read-after-write, a halted revert.
#[test]
fn exact_read_set_makes_the_downstream_agent_visible_to_revert() {
    let mut db = setup();
    let store = MemProvenanceStore::new();
    let restock = agent_run(&store, "restock-agent", "run-42", 1);
    let auditor = agent_run(&store, "auditor-agent", "run-99", 2);
    let mut log = ProvenanceLog::new();

    // --- run A: restock-agent reads row 2, then raises its qty -------------------------------
    let a = Arc::new(Mutex::new(TxnCapture::new(TxnId(1), restock, BranchId::new(1, 0))));
    let before = point_read(&db, &a, 2);
    assert_eq!(before, Value::Integer(20));
    db.sql("UPDATE inventory SET qty = 40 WHERE id = 2;");
    let written_by_a = record_write(&db, &store, &a, 2, Value::Integer(40));
    let a = Arc::try_unwrap(a).unwrap().into_inner().unwrap().finish();

    // The read-set really is exact-version form, and really carries the storage layer's begin_ts.
    assert_eq!(a.read_sets.len(), 1);
    assert_eq!(a.read_sets[0].form(), ReadSetForm::ExactVersions);
    assert_eq!(a.read_sets[0].exact_rows(), vec![(INVENTORY, RowId(2))]);

    // --- criterion 9: which agent + run + model wrote row 2 -----------------------------------
    let who = store.who_wrote(written_by_a.rid).unwrap();
    assert_eq!(who.agent_id, "restock-agent");
    assert_eq!(who.run_id, "run-42");
    assert_eq!(who.model, "claude-opus");
    assert_eq!(who.model_version, "2026-05");
    assert_eq!(who.parent_branch, BranchId::new(1, 0));
    assert_eq!(store.attribute(written_by_a.rid).unwrap(), restock);
    // Attribution is run-level: one dictionary entry on that page, not one actor tuple per row.
    assert_eq!(store.page_dictionary_len(written_by_a.rid.page_id), 1);

    // --- run B: auditor-agent reads what A wrote, then writes a derived row -------------------
    let b = Arc::new(Mutex::new(TxnCapture::new(TxnId(2), auditor, BranchId::new(2, 0))));
    let seen = point_read(&db, &b, 2);
    assert_eq!(seen, Value::Integer(40), "auditor must observe A's write");
    db.sql("INSERT INTO inventory VALUES (9, 80);");
    let written_by_b = record_write(&db, &store, &b, 9, Value::Integer(80));
    let b = Arc::try_unwrap(b).unwrap().into_inner().unwrap().finish();

    // B read exactly the version A produced — the causal edge is exact, not inferred.
    assert!(b.read_sets[0].contains_version(&written_by_a));
    // ...and B never looked at the row it created, which is the write-set \ read-set metric.
    assert_eq!(b.blind_writes(), vec![(INVENTORY, RowId(9), Some(QTY))]);
    assert_eq!(store.who_wrote(written_by_b.rid).unwrap().agent_id, "auditor-agent");

    log.record(a);
    log.record(b);

    // --- criterion 10: reverting A surfaces B, and halts ---------------------------------------
    let plan = log.plan_revert(TxnId(1), RevertMode::Halt);
    assert!(plan.is_blocked(), "revert of A must not proceed silently");
    assert_eq!(plan.blocked_by, vec![TxnId(2)]);
    assert!(plan.cascade.is_empty(), "halt reverts nothing");

    let report = log.revert_report(TxnId(1), RevertMode::Halt, &store);
    assert!(report.contains("HALTED"), "{}", report);
    assert!(report.contains("txn2 read row2@"), "{}", report);
    assert!(report.contains("auditor-agent"), "{}", report);

    // Cascade only on explicit request, and dependents first.
    let cascade = log.plan_revert(TxnId(1), RevertMode::Cascade);
    assert!(!cascade.is_blocked());
    assert_eq!(cascade.cascade, vec![TxnId(2)]);
}

/// A range read retains a predicate, so a write that lands inside the scanned region is a
/// dependency even though no exact version was retained for it.
#[test]
fn a_range_read_set_still_finds_the_write_it_consumed() {
    let mut db = setup();
    let store = MemProvenanceStore::new();
    let restock = agent_run(&store, "restock-agent", "run-42", 1);
    let reporter = agent_run(&store, "reporting-agent", "run-7", 3);
    let mut log = ProvenanceLog::new();

    // A writes qty = 25 into row 1.
    let a = Arc::new(Mutex::new(TxnCapture::new(TxnId(1), restock, BranchId::new(1, 0))));
    db.sql("UPDATE inventory SET qty = 25 WHERE id = 1;");
    record_write(&db, &store, &a, 1, Value::Integer(25));
    let a = Arc::try_unwrap(a).unwrap().into_inner().unwrap().finish();

    // B scans every row with a real SeqScan, retaining a predicate over qty in [20, 50).
    let b = Arc::new(Mutex::new(TxnCapture::new(TxnId(2), reporter, BranchId::new(3, 0))));
    let observed_at = db.txn.read_snapshot().high_water;
    let mut scan = CapturingScan::new(
        db.scan("inventory"),
        b.clone(),
        INVENTORY,
        AccessShape::Range,
        Arc::new(SurrogateColumn(0)),
        Arc::new(db.heap("inventory")),
        Some(PredicateSummary {
            tbl: INVENTORY,
            col: Some(QTY),
            lo: Bound::Included(Value::Integer(20)),
            hi: Bound::Excluded(Value::Integer(50)),
            residual: Some("qty >= 20 AND qty < 50".into()),
            rows_observed: 0,
        }),
        observed_at,
    );
    let mut rows = 0u64;
    while let Some(r) = scan.next() {
        r.unwrap();
        rows += 1;
    }
    assert_eq!(rows, 3);
    drop(scan);
    let b = Arc::try_unwrap(b).unwrap().into_inner().unwrap().finish();

    // Predicate form, with the row count observed by the real scan, and no exact versions.
    assert_eq!(b.read_sets.len(), 1);
    assert_eq!(b.read_sets[0].form(), ReadSetForm::Predicate);
    assert!(b.read_sets[0].exact_rows().is_empty());
    assert_eq!(b.predicate_reads[0].summary.rows_observed, rows);

    log.record(a);
    log.record(b);

    let plan = log.plan_revert(TxnId(1), RevertMode::Halt);
    assert!(plan.is_blocked(), "the scan consumed A's write");
    assert_eq!(plan.blocked_by, vec![TxnId(2)]);
}

/// The trap the design names explicitly: scattered point reads are never coarsened into an
/// enclosing interval, however many of them there are. Proven against real scans.
#[test]
fn scattered_point_reads_stay_exact_through_the_executor() {
    let mut db = setup();
    for id in 4..40 {
        db.sql(&format!("INSERT INTO inventory VALUES ({}, {});", id, id * 2));
    }
    let store = MemProvenanceStore::new();
    let prov = agent_run(&store, "picker-agent", "run-3", 1);
    let capture = Arc::new(Mutex::new(TxnCapture::new(TxnId(1), prov, BranchId::new(1, 0))));

    // Deliberately scattered: 1, 13, 27, 39. An enclosing interval would cover the whole table.
    for id in [1, 13, 27, 39] {
        point_read(&db, &capture, id);
    }
    let t = Arc::try_unwrap(capture).unwrap().into_inner().unwrap().finish();

    assert_eq!(t.read_sets.len(), 1);
    assert_eq!(t.read_sets[0].form(), ReadSetForm::ExactVersions);
    assert!(t.predicate_reads.is_empty(), "no interval may be synthesised");
    let mut rows = t.read_sets[0].exact_rows();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (INVENTORY, RowId(1)),
            (INVENTORY, RowId(13)),
            (INVENTORY, RowId(27)),
            (INVENTORY, RowId(39)),
        ]
    );
}

/// A read view is not a dependency: a scan that ran before the write cannot depend on it.
#[test]
fn a_snapshot_taken_before_the_write_creates_no_edge() {
    let mut db = setup();
    let store = MemProvenanceStore::new();
    let restock = agent_run(&store, "restock-agent", "run-42", 1);
    let reporter = agent_run(&store, "reporting-agent", "run-7", 3);

    // B scans first, at an older snapshot.
    let b = Arc::new(Mutex::new(TxnCapture::new(TxnId(2), reporter, BranchId::new(3, 0))));
    let observed_at = db.txn.read_snapshot().high_water;
    let mut scan = CapturingScan::new(
        db.scan("inventory"),
        b.clone(),
        INVENTORY,
        AccessShape::FullScan,
        Arc::new(SurrogateColumn(0)),
        Arc::new(db.heap("inventory")),
        Some(PredicateSummary::full_scan(INVENTORY, 0)),
        observed_at,
    );
    while let Some(r) = scan.next() {
        r.unwrap();
    }
    drop(scan);
    let b = Arc::try_unwrap(b).unwrap().into_inner().unwrap().finish();

    // Only afterwards does A write.
    let a = Arc::new(Mutex::new(TxnCapture::new(TxnId(1), restock, BranchId::new(1, 0))));
    db.sql("UPDATE inventory SET qty = 25 WHERE id = 1;");
    let written = record_write(&db, &store, &a, 1, Value::Integer(25));
    let a = Arc::try_unwrap(a).unwrap().into_inner().unwrap().finish();
    // The engine's own visibility rule is `begin_ts < snapshot.high_water`, so a write stamped at
    // the mark itself was invisible to the scan too.
    assert!(
        written.begin_ts >= observed_at,
        "write ts {} should not have been visible at snapshot {}",
        written.begin_ts,
        observed_at
    );

    let mut log = ProvenanceLog::new();
    log.record(a);
    log.record(b);
    let plan = log.plan_revert(TxnId(1), RevertMode::Halt);
    assert!(!plan.is_blocked(), "blocked by {:?}", plan.blocked_by);
    assert!(log.dependency_graph().edges.is_empty());
}

/// A version written before the agent layer existed is reported as unattributed, never guessed.
#[test]
fn unattributed_versions_are_reported_as_unattributed() {
    let db = setup();
    let store = MemProvenanceStore::new();
    let rid = db.rid_of("inventory", 3);
    assert_eq!(store.attribute(rid).unwrap(), ProvId::NONE);
    assert_eq!(store.describe_row(rid), "unattributed");
}
