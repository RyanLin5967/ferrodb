//! E3 — a replica applying a primary's shipped WAL.
//!
//! The applier's job is to be *suspicious of the wire*. Every frame is CRC-checked against the
//! bytes that actually arrived and its embedded LSN is checked against where the walk places it,
//! because the two failures that matter here are silent: corrupted bytes applied as if valid, and
//! records applied out of order.
//!
//! It must also be **idempotent**. A reconnect asks from the replica's own LSN, and the primary
//! will happily re-send a record the replica already has. Redo goes through the same
//! `apply_redo` recovery uses, which skips any record at or below the page's own LSN — so an
//! overlap is absorbed rather than applied twice.
//!
//! A batch that fails validation is refused **whole**. Applying a prefix and then erroring would
//! leave the replica at an LSN it cannot justify, which is worse than not advancing at all.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::replication::{ReplicaApplier, ReplicationSource};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_page::Page;
use ferrodb::wal::log::{RecKind, WalManager};

/// A primary with a WAL, and a replica with its own pages.
struct Pair {
    _dir: tempfile::TempDir,
    primary_wal: Arc<WalManager>,
    replica_bp: Arc<BufferPoolManager>,
}

fn pair(tag: &str) -> Pair {
    let dir = tempfile::tempdir().unwrap();
    let primary_wal =
        Arc::new(WalManager::new(dir.path().join(format!("{tag}-primary.wal"))).unwrap());

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}-replica.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let replica_bp = Arc::new(BufferPoolManager::new(dm));
    Pair { _dir: dir, primary_wal, replica_bp }
}

/// A tuple big enough to be recognisable, tagged with `n`.
fn tuple(n: u8) -> Vec<u8> {
    let mut t = vec![0u8; 40];
    t[0] = n;
    t
}

/// Write an insert on the primary and make it durable.
fn primary_insert(wal: &WalManager, txn: u64, page_id: u32, slot: u16, n: u8) -> u64 {
    let lsn = wal
        .append(
            txn,
            0,
            &RecKind::HeapInsert { dir_root: 1, page_id, slot, tuple: tuple(n) },
        )
        .expect("append");
    wal.flush().expect("flush");
    lsn
}

fn ship_all(p: &Pair, applier: &ReplicaApplier) -> u64 {
    let src = ReplicationSource::new(&p.primary_wal);
    let (bytes, next) = src.read_from(applier.applied_lsn(), 1 << 20).expect("read");
    if bytes.is_empty() {
        return applier.applied_lsn();
    }
    applier.apply(next - bytes.len() as u64, &bytes).expect("apply")
}

#[test]
fn a_replica_applies_a_shipped_insert_and_advances() {
    let p = pair("basic");
    let src_start = ReplicationSource::new(&p.primary_wal).start_lsn();
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src_start);

    primary_insert(&p.primary_wal, 1, 10, 0, 0xAB);
    let after = ship_all(&p, &applier);

    assert!(after > src_start, "the replica did not advance past the log's start");
    assert_eq!(applier.applied_lsn(), after);

    // The page must actually hold the row, judged from the page rather than from the applier.
    let idx = p.replica_bp.fetch_page(10).expect("fetch");
    let page = Page::deserialize(p.replica_bp.frames[idx].read().unwrap().data).expect("page");
    p.replica_bp.unpin_page(10, false);
    assert!(page.lsn > 0, "the replica's page carries no LSN, so nothing was applied");
}

/// **Idempotence.** Re-sending a batch the replica already has must change nothing.
#[test]
fn re_applying_the_same_batch_is_a_no_op() {
    let p = pair("idem");
    let src = ReplicationSource::new(&p.primary_wal);
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src.start_lsn());

    primary_insert(&p.primary_wal, 1, 20, 0, 0x11);
    let (bytes, next) = src.read_from(src.start_lsn(), 1 << 20).unwrap();
    let start = next - bytes.len() as u64;

    let first = applier.apply(start, &bytes).expect("first apply");
    let page_lsn_after_first = {
        let idx = p.replica_bp.fetch_page(20).unwrap();
        let l = Page::deserialize(p.replica_bp.frames[idx].read().unwrap().data).unwrap().lsn;
        p.replica_bp.unpin_page(20, false);
        l
    };

    // Exactly what a reconnect does: the primary re-sends from a point the replica already passed.
    let second = applier.apply(start, &bytes).expect("re-apply must be accepted, not rejected");
    assert_eq!(second, first, "the frontier moved on a duplicate batch");

    let page_lsn_after_second = {
        let idx = p.replica_bp.fetch_page(20).unwrap();
        let l = Page::deserialize(p.replica_bp.frames[idx].read().unwrap().data).unwrap().lsn;
        p.replica_bp.unpin_page(20, false);
        l
    };
    assert_eq!(
        page_lsn_after_second, page_lsn_after_first,
        "re-applying the same records changed the page; redo is not idempotent"
    );
}

/// A corrupted byte on the wire must be refused, not applied.
#[test]
fn a_frame_whose_crc_fails_is_refused() {
    let p = pair("crc");
    let src = ReplicationSource::new(&p.primary_wal);
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src.start_lsn());

    primary_insert(&p.primary_wal, 1, 30, 0, 0x22);
    let (mut bytes, next) = src.read_from(src.start_lsn(), 1 << 20).unwrap();
    let start = next - bytes.len() as u64;

    // Flip a byte in the record body, leaving the length prefix intact.
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;

    let err = applier.apply(start, &bytes).expect_err("a corrupted frame was accepted");
    assert!(format!("{err}").contains("CRC"), "refused for the wrong reason: {err}");
    assert_eq!(
        applier.applied_lsn(),
        src.start_lsn(),
        "the replica advanced despite refusing the batch"
    );
}

/// A frame claiming an LSN other than its position in the stream must be refused — that is the
/// signature of records arriving out of order, which would corrupt the replica silently.
#[test]
fn a_frame_at_the_wrong_lsn_is_refused() {
    let p = pair("order");
    let src = ReplicationSource::new(&p.primary_wal);
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src.start_lsn());

    primary_insert(&p.primary_wal, 1, 40, 0, 0x33);
    let (bytes, next) = src.read_from(src.start_lsn(), 1 << 20).unwrap();
    let start = next - bytes.len() as u64;

    // Same bytes, told they begin somewhere else.
    let err = applier
        .apply(start + 8, &bytes)
        .expect_err("a frame was applied at an lsn it does not claim");
    assert!(format!("{err}").contains("out of order"), "refused for the wrong reason: {err}");
}

/// A batch is all-or-nothing: a bad frame at the end must prevent the good ones before it from
/// being applied, or the replica ends up at an LSN it cannot account for.
#[test]
fn a_batch_with_one_bad_frame_applies_none_of_it() {
    let p = pair("atomic");
    let src = ReplicationSource::new(&p.primary_wal);
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src.start_lsn());

    primary_insert(&p.primary_wal, 1, 50, 0, 0x44);
    primary_insert(&p.primary_wal, 1, 51, 0, 0x55);
    let (mut bytes, next) = src.read_from(src.start_lsn(), 1 << 20).unwrap();
    let start = next - bytes.len() as u64;

    // Corrupt the LAST byte, which is inside the final frame's CRC.
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;

    assert!(applier.apply(start, &bytes).is_err(), "a batch with a bad frame was accepted");
    assert_eq!(applier.applied_lsn(), src.start_lsn(), "the replica advanced on a refused batch");

    // And the FIRST record, which was perfectly valid, must not have been applied either.
    //
    // The page is expected to be ABSENT, not merely empty: the batch was refused before anything
    // was materialised, so the replica's file never grew. That is stronger evidence than a zero
    // LSN would be. The first version of this assertion fetched the page and unwrapped, which
    // failed on the EOF that absence produces — the code was more correct than the test.
    match p.replica_bp.fetch_page(50) {
        Err(_) => { /* never materialised: nothing from the batch was applied */ }
        Ok(idx) => {
            let page = Page::deserialize(p.replica_bp.frames[idx].read().unwrap().data).unwrap();
            p.replica_bp.unpin_page(50, false);
            assert_eq!(
                page.lsn, 0,
                "a valid frame from a refused batch was applied; the batch is not all-or-nothing"
            );
        }
    }
}

/// Several batches in sequence must land the replica exactly where the primary's durable frontier
/// is — the convergence property the whole scheme exists for.
#[test]
fn streaming_in_batches_converges_on_the_primarys_frontier() {
    let p = pair("converge");
    let src = ReplicationSource::new(&p.primary_wal);
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src.start_lsn());

    for i in 0..25u8 {
        primary_insert(&p.primary_wal, 1, 100 + i as u32, 0, i);
    }

    // Small batches, so the stream is split many times.
    let mut rounds = 0;
    while applier.applied_lsn() < src.durable_lsn() {
        let (bytes, next) = src.read_from(applier.applied_lsn(), 128).unwrap();
        assert!(!bytes.is_empty(), "a batch below the frontier returned nothing");
        applier.apply(next - bytes.len() as u64, &bytes).expect("apply");
        rounds += 1;
        assert!(rounds < 500, "streaming did not terminate");
    }

    assert!(rounds > 1, "the stream was never split, so batching was not exercised");
    assert_eq!(
        applier.applied_lsn(),
        src.durable_lsn(),
        "the replica did not converge on the primary's durable frontier"
    );
}

// ---------------------------------------------------------------------------------------------
// I20 — an ALTER on the primary, review finding 11.
// ---------------------------------------------------------------------------------------------

/// A primary with a catalog and the real SQL surface, so an `ALTER` can actually happen.
///
/// The `Pair` above drives a bare `WalManager` with hand-built records, which is right for the
/// wire-level properties it tests and useless here: an `ALTER` exists only through
/// `Catalog::alter_table`, and the whole point of this finding is what that function does to pages
/// *without* writing a record.
struct RealPrimary {
    _dir: tempfile::TempDir,
    catalog: ferrodb::catalog::catalog::Catalog,
    bp: Arc<BufferPoolManager>,
    wal: Arc<WalManager>,
    txn: Arc<ferrodb::wal::txn::TxnManager>,
    session: ferrodb::execution::session::Session,
    replica_bp: Arc<BufferPoolManager>,
}

fn real_primary(tag: &str) -> RealPrimary {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(dir.path().join(format!("{tag}-primary.db"))).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = ferrodb::catalog::catalog::Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}-primary.wal"))).unwrap());
    let txn = Arc::new(ferrodb::wal::txn::TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());

    let rfile = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(dir.path().join(format!("{tag}-replica.db"))).unwrap();
    let replica_bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(rfile).unwrap())));

    RealPrimary {
        _dir: dir,
        catalog,
        bp,
        wal,
        txn,
        session: ferrodb::execution::session::Session::new(),
        replica_bp,
    }
}

impl RealPrimary {
    fn sql(&mut self, sql: &str) {
        use ferrodb::parser::parser::Parser;
        use ferrodb::parser::scanner::Scanner;
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        ferrodb::execution::executor::run(
            stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session,
        )
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
        self.wal.flush().unwrap();
    }

    /// Ship everything the primary has durably logged to `applier`, in one batch.
    fn ship(&self, applier: &ReplicaApplier) -> Result<u64, ferrodb::error::FerroError> {
        let src = ReplicationSource::new(&self.wal);
        let (bytes, next) = src.read_from(applier.applied_lsn(), 1 << 20).expect("read");
        if bytes.is_empty() {
            return Ok(applier.applied_lsn());
        }
        applier.apply(next - bytes.len() as u64, &bytes)
    }
}

/// The `dir_root` of a table, which is what a heap record names.
fn dir_root_of(p: &RealPrimary, table: &str) -> u32 {
    p.catalog.get_table(table).unwrap_or_else(|| panic!("no table {table}")).first_directory_page_id
}

/// **A replica must not walk past an ALTER it cannot replay — I20, review finding 11.**
///
/// The rewrite `ALTER TABLE` performs is unlogged: `Catalog::alter_table` goes through a
/// `HeapFileManager::open`, whose `txn` is `None`, so every write path in it is gated off and no
/// record describes the rewrite. The `Ddl` record that follows is the only trace, and the applier's
/// redo loop used to drop it into `_ => {}` and then advance `applied_lsn` regardless.
///
/// So a replica silently kept the pre-ALTER tuple layout, took post-ALTER `HeapInsert` frames into
/// the same pages, and reported itself caught up over pages holding two incompatible layouts. There
/// is nothing in this file that can repair that — logging the rewrite is a change to `alter.rs` —
/// so the replica STOPS and says why.
#[test]
fn a_replica_refuses_a_stream_carrying_an_alter_rather_than_advancing_past_it() {
    let mut p = real_primary("alter");
    p.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    p.sql("INSERT INTO inv VALUES (1, 10);");
    p.sql("INSERT INTO inv VALUES (2, 20);");

    let start = ReplicationSource::new(&p.wal).start_lsn();
    let applier = ReplicaApplier::new(p.replica_bp.clone(), start);

    // Everything before the ALTER ships and converges. Without this the test could pass because
    // replication never worked at all.
    p.ship(&applier).expect("the pre-ALTER stream must apply cleanly");
    assert!(applier.diverged().is_none(), "diverged before an ALTER was ever issued");
    let root = dir_root_of(&p, "inv");
    let before = applier.applied_lsn();
    assert!(before > start, "the replica applied nothing at all; the test would be vacuous");

    // Now the ALTER, and a row written under the NEW shape after it.
    p.sql("ALTER TABLE inv ADD COLUMN note VARCHAR(20);");
    p.sql("INSERT INTO inv VALUES (3, 30, 'after');");

    let err = p
        .ship(&applier)
        .expect_err("the replica applied a stream carrying an ALTER and reported itself caught up");
    let text = format!("{err}");
    assert!(text.contains("DIVERGED"), "refused, but not by this guard: {text}");
    assert!(text.contains("inv"), "the refusal does not name the table: {text}");

    // It stopped where it was, rather than reporting a position it cannot justify.
    assert_eq!(
        applier.applied_lsn(),
        before,
        "the replica moved its applied_lsn while refusing the batch"
    );

    // **Latched.** A reconnect re-sends from `applied_lsn`, so without the latch the same record
    // would be met, refused, and an operator restarting the replica would see it "catch up" as far
    // as the ALTER again and again.
    assert!(applier.diverged().is_some(), "the divergence was not recorded");
    let second = p.ship(&applier).expect_err("a reconnect walked past the divergence");
    assert!(format!("{second}").contains("DIVERGED"), "{second}");

    // And the divergence is real, not theoretical: the primary's page for this table is not the
    // replica's. This is the state the applier used to reach while claiming to be caught up.
    let primary_page = p.bp.disk_manager.read(root).expect("primary page");
    let replica_page = p.replica_bp.disk_manager.read(root).expect("replica page");
    assert_ne!(
        primary_page.to_vec(),
        replica_page.to_vec(),
        "the pages agree, so this test is not exercising the divergence it claims to"
    );
}

/// The guard must not fire on the DDL that is in **every** stream.
///
/// A `CREATE TABLE` record is re-declared at every checkpoint of the source and a `DROP TABLE`
/// record is ordinary traffic; neither touches a heap page, so halting on either would stop every
/// replica in existence. Driven through hand-built frames on the bare `Pair` rather than through
/// the SQL surface, because `DROP TABLE` checkpoints — which truncates the log out from under a
/// replica streaming without a base backup, a separate limitation that would make this test about
/// something else entirely.
#[test]
fn a_replica_still_applies_a_stream_carrying_create_and_drop_table() {
    use ferrodb::catalog::column::DataType;
    use ferrodb::wal::log::DdlOp;

    let p = pair("createdrop");
    let start = ferrodb::replication::ReplicationSource::new(&p.primary_wal).start_lsn();
    let applier = ReplicaApplier::new(p.replica_bp.clone(), start);

    let columns = vec![("id".to_string(), DataType::Integer, false)];
    for op in [DdlOp::CreateTable, DdlOp::DropTable] {
        p.primary_wal
            .append(
                0,
                0,
                &RecKind::Ddl {
                    op,
                    table: "inv".into(),
                    dir_root: 1,
                    time_travel_root: 2,
                    columns: columns.clone(),
                },
            )
            .expect("append");
    }
    // A real row on either side of the DDL, so "applied" is not vacuously true.
    primary_insert(&p.primary_wal, 1, 9, 0, 7);
    p.primary_wal.append(1, 0, &RecKind::Commit).expect("append");
    p.primary_wal.flush().unwrap();

    ship_all(&p, &applier);
    assert!(
        applier.diverged().is_none(),
        "the guard halted on whole-table DDL: {:?}",
        applier.diverged()
    );
    assert!(applier.applied_lsn() > start, "the replica applied nothing");
}

/// The guard fires on an `AlterColumn` record **whatever the alteration is** — a rename and a
/// retype rewrite the heap exactly as an add does, and each was verified separately rather than
/// assumed from the one the end-to-end test happens to use.
#[test]
fn every_column_alteration_halts_the_replica() {
    use ferrodb::catalog::column::DataType;
    use ferrodb::wal::log::{ColumnAlteration, DdlOp};

    let alterations = [
        ColumnAlteration::Add { column: "note".into() },
        ColumnAlteration::Rename { from: "qty".into(), to: "quantity".into() },
        ColumnAlteration::Retype { column: "qty".into(), from: DataType::Integer },
    ];
    for alteration in alterations {
        let p = pair("altkinds");
        let start = ferrodb::replication::ReplicationSource::new(&p.primary_wal).start_lsn();
        let applier = ReplicaApplier::new(p.replica_bp.clone(), start);
        p.primary_wal
            .append(
                0,
                0,
                &RecKind::Ddl {
                    op: DdlOp::AlterColumn(alteration.clone()),
                    table: "inv".into(),
                    dir_root: 1,
                    time_travel_root: 2,
                    columns: vec![("id".to_string(), DataType::Integer, false)],
                },
            )
            .expect("append");
        p.primary_wal.flush().unwrap();

        let src = ferrodb::replication::ReplicationSource::new(&p.primary_wal);
        let (bytes, next) = src.read_from(applier.applied_lsn(), 1 << 20).expect("read");
        let err = applier
            .apply(next - bytes.len() as u64, &bytes)
            .expect_err(&format!("{alteration:?} was applied rather than halting the replica"));
        assert!(format!("{err}").contains("DIVERGED"), "{alteration:?}: {err}");
        assert!(applier.diverged().is_some(), "{alteration:?} did not latch");
    }
}

/// The halt unwraps a `Clr` — I20, adversarial pass.
///
/// Nothing in this codebase can produce a `Clr` wrapping a `Ddl`: DDL is refused inside a
/// transaction and a `Clr` is only written while rolling one back. The arm exists anyway, and this
/// test hand-builds the record to prove it, because a guard over what may be applied should be an
/// allowlist rather than a list of the shapes somebody happened to think of — and the cost of being
/// wrong about reachability is a silently diverged replica.
#[test]
fn an_alter_wrapped_in_a_clr_still_halts_the_replica() {
    use ferrodb::catalog::column::DataType;
    use ferrodb::wal::log::{ColumnAlteration, DdlOp};

    let p = pair("clralter");
    let start = ferrodb::replication::ReplicationSource::new(&p.primary_wal).start_lsn();
    let applier = ReplicaApplier::new(p.replica_bp.clone(), start);

    let inner = RecKind::Ddl {
        op: DdlOp::AlterColumn(ColumnAlteration::Add { column: "note".into() }),
        table: "inv".into(),
        dir_root: 1,
        time_travel_root: 2,
        columns: vec![("id".to_string(), DataType::Integer, false)],
    };
    p.primary_wal
        .append(1, 0, &RecKind::Clr { undone_lsn: 0, undo_next: 0, redo: Box::new(inner) })
        .expect("append");
    p.primary_wal.flush().unwrap();

    let src = ferrodb::replication::ReplicationSource::new(&p.primary_wal);
    let (bytes, next) = src.read_from(applier.applied_lsn(), 1 << 20).expect("read");
    let err = applier
        .apply(next - bytes.len() as u64, &bytes)
        .expect_err("an AlterColumn wrapped in a Clr walked straight past the guard");
    assert!(format!("{err}").contains("DIVERGED"), "{err}");
    assert!(applier.diverged().is_some(), "the divergence was not latched");
}
