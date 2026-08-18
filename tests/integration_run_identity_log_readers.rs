//! B5 — the other readers of the log, against the new tag.
//!
//! `RecKind::RunIdentity` is tag 10, and `log.rs` already proves the *record* round-trips and that
//! tags 0..9 have not moved. That is not the same claim as **every component that walks this log
//! still works when tag 10 is in it**, and there are three such components besides the change feed:
//! crash recovery, the replica applier, and the checkpoint path that puts declarations in the log in
//! the first place.
//!
//! Each of them handles unknown records through a `_ => {}` arm, which is exactly the shape that
//! looks correct and is untested — and the cost of being wrong is not a decode error. A replica that
//! refused a batch containing an identity record would stop replicating; a recovery pass that
//! mishandled one would treat a live transaction as a loser and undo committed work. Neither
//! announces itself.
//!
//! **Breaking shape for both tests below:** a log that contains identity records at all. Every test
//! in this repo written before this lane produces logs with none, so both components pass their
//! whole existing suite without ever seeing tag 10.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::provenance::sha256::prompt_digest;
use ferrodb::provenance::{ProvId, RunEntity};
use ferrodb::replication::{ReplicaApplier, ReplicationSource};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_page::Page;
use ferrodb::wal::log::{RecKind, WalManager};
use ferrodb::wal::recovery::recover;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;

fn a_run(prov: u32, agent: &str) -> RunEntity {
    RunEntity::new(
        ProvId(prov),
        agent,
        "run-42",
        "claude-opus",
        "2026-05",
        prompt_digest("top up everything below reorder"),
        1_700_000_000_000,
        BranchId::new(prov as u64, 0),
    )
}

/// Every record in a WAL, in order.
fn walk(wal: &WalManager) -> Vec<(u64, RecKind)> {
    let mut out = Vec::new();
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    let end = wal.next_lsn.load(Ordering::SeqCst);
    while lsn < end {
        let (rec, next) = wal.read_record(lsn).expect("read record");
        out.push((rec.txn_id, rec.kind));
        lsn = next;
    }
    out
}

/// **A replica applies a batch containing an identity record, and ignores the record.**
///
/// The applier CRC-checks every frame and validates each embedded LSN against where the walk places
/// it, then redoes the heap records. An identity record is neither a heap record nor an error: it
/// must pass validation, advance the walk, and change no page.
#[test]
fn a_replica_applies_a_batch_containing_run_identity_records() {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("primary.wal")).unwrap());
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("replica.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));

    let src_start = ReplicationSource::new(&wal).start_lsn();
    let applier = ReplicaApplier::new(Arc::clone(&bp), src_start);

    // A whole transaction, with the identity record where `TxnManager::commit` puts it: immediately
    // before the `Commit`.
    let mut tuple = vec![0u8; 40];
    tuple[0] = 0xAB;
    wal.append(1, 0, &RecKind::Begin).unwrap();
    wal.append(1, 0, &RecKind::HeapInsert { dir_root: 1, page_id: 10, slot: 0, tuple }).unwrap();
    wal.append(1, 0, &RecKind::RunIdentity { run: a_run(1, "restock-agent") }).unwrap();
    wal.append(1, 0, &RecKind::Commit).unwrap();
    wal.append(1, 0, &RecKind::TxnEnd).unwrap();
    wal.flush().unwrap();

    let src = ReplicationSource::new(&wal);
    let (bytes, next) = src.read_from(src_start, 1 << 20).expect("read");
    assert!(!bytes.is_empty(), "the primary shipped nothing; the test would be vacuous");
    let after = applier
        .apply(next - bytes.len() as u64, &bytes)
        .expect("a batch containing a run identity record was refused");

    assert_eq!(after, next, "the applier did not consume the whole batch");
    assert!(after > src_start, "the applier did not advance");

    // The row really landed, judged from the page rather than from the applier's own number.
    let idx = bp.fetch_page(10).expect("fetch");
    let page = Page::deserialize(bp.frames[idx].read().unwrap().data).expect("page");
    bp.unpin_page(10, false);
    assert!(page.lsn > 0, "the replica's page carries no LSN, so nothing was applied");

    // Anti-vacuity: the batch really did carry an identity record, so the pass above is about
    // tolerating one rather than about there being none.
    let identities = walk(&wal)
        .iter()
        .filter(|(_, k)| matches!(k, RecKind::RunIdentity { .. }))
        .count();
    assert_eq!(identities, 1, "no identity record was in the shipped log");
}

struct Db {
    dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("recover.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("recover.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { dir, catalog, wal, bp, txn, session: Session::new() }
}

impl Db {
    fn sql(&mut self, sql: &str) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn agent_txn(&mut self, entity: &RunEntity, statements: &[&str]) {
        self.sql("BEGIN;");
        let txn_id = self.session.current.expect("BEGIN did not open a transaction");
        self.txn.bind_run(txn_id, entity.clone()).expect("bind_run");
        for s in statements {
            self.sql(s);
        }
        self.sql("COMMIT;");
    }
}

/// **Crash recovery over a log holding both a binding and a declaration.**
///
/// Both shapes are in the log on purpose, because they exercise different parts of the analysis
/// pass. A **binding** rides inside a transaction's record chain, so a recovery pass that
/// mishandled it would corrupt that transaction's `last_lsn` and undo committed work. A
/// **declaration** carries transaction id 0, which never commits — so it lands in the loser set,
/// exactly as a `Ddl` record already does, and its chain must terminate rather than walk off.
#[test]
fn recovery_replays_a_log_holding_both_a_binding_and_a_declaration() {
    let mut d = db();
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");
    let restock = a_run(1, "restock-agent");

    d.agent_txn(&restock, &[
        "INSERT INTO inventory VALUES (1, 10);",
        "INSERT INTO inventory VALUES (2, 20);",
    ]);

    // A checkpoint puts DECLARATIONS at the head of the new log — that is what retention across
    // truncation means — and then one more transaction adds a BINDING above them.
    d.txn.checkpoint().expect("checkpoint");
    d.agent_txn(&restock, &["INSERT INTO inventory VALUES (3, 30);"]);
    d.wal.flush().unwrap();

    // Anti-vacuity, before anything is recovered: both shapes really are in the log.
    let records = walk(&d.wal);
    let declarations = records
        .iter()
        .filter(|(txn, k)| *txn == 0 && matches!(k, RecKind::RunIdentity { .. }))
        .count();
    let bindings = records
        .iter()
        .filter(|(txn, k)| *txn != 0 && matches!(k, RecKind::RunIdentity { .. }))
        .count();
    assert_eq!(declarations, 1, "the checkpoint wrote no run declaration: {records:?}");
    assert_eq!(bindings, 1, "the post-checkpoint transaction wrote no binding");

    let expected = match d.sql("SELECT id, qty FROM inventory;") {
        Outcome::Rows(r) => r.len(),
        _ => panic!("expected rows from the select"),
    };
    assert_eq!(expected, 3, "the workload did not land three rows");

    // Destructured rather than `drop(d)`: the `TempDir` must OUTLIVE the database handles, or the
    // directory is removed and the reopen below fails with ENOENT instead of testing recovery.
    let Db { dir, catalog, wal, bp, txn, session } = d;
    drop((catalog, wal, bp, txn, session));
    let db_path = dir.path().join("recover.db");
    let wal_path = dir.path().join("recover.wal");

    // Reopen exactly the way the CLI does: a fresh buffer pool over the same file, `recover`, then
    // `Catalog::open`.
    let file = std::fs::OpenOptions::new().read(true).write(true).open(&db_path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(wal_path).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    recover(&txn).expect("recovery refused a log containing run identity records");

    let mut catalog = Catalog::open(bp.clone(), FIRST_CATALOG_PAGE_ID).expect("reopen catalog");
    let mut session = Session::new();
    let tokens = Scanner::new("SELECT id, qty FROM inventory;".chars().collect(), Vec::new())
        .scan_tokens()
        .unwrap();
    let stmt = Parser::new(tokens).parse().remove(0);
    let rows = match run(stmt, &mut catalog, bp.clone(), txn.clone(), &mut session)
        .expect("select after recovery")
    {
        Outcome::Rows(r) => r,
        _ => panic!("expected rows from the select after recovery"),
    };

    let mut ids: Vec<i32> = rows
        .iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            ref other => panic!("unexpected id {other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "recovery over a log containing run identity records lost or resurrected rows"
    );
    drop(dir);
}
