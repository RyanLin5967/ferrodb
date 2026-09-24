//! D234 with D227 — a run an entry point declares at open is in the log at once, so a pin that keeps
//! every later checkpoint from truncating cannot keep it out.
//!
//! D234 made a checkpoint re-declare only after a real truncation, because a kept log already holds
//! every declaration it had. That is true only if every declaration reaches the log when it is
//! made. `log_ddl` writes DDL as it runs, and a merge writes its run in the binding before its
//! `Commit`. `TxnManager::declare_runs_of`, which the CLI and pgserver call once their provenance
//! store is open (D227), used to only RETAIN the runs, leaving them for the next checkpoint to
//! write; under a pin that checkpoint writes nothing.
//!
//! Green-only: `declare_runs_of` does not exist at `00f4c39`, so this cannot compile there. It
//! fails under the mutant that makes `declare_runs_of` retain without writing.

use std::fs::OpenOptions;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::provenance::{MemProvenanceStore, ProvId, ProvenanceStore, RunEntity};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::{RecKind, WalManager};
use ferrodb::wal::txn::TxnManager;

#[test]
fn a_run_declared_at_open_reaches_the_log_while_every_checkpoint_is_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new().read(true).write(true).create(true).open(dir.path().join("d234.db")).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("d234.wal")).unwrap());
    let txn = TxnManager::new(wal.clone(), bp.clone());
    bp.attach_wal(wal.clone());

    let store = MemProvenanceStore::new();
    let slot = store
        .intern(&RunEntity::new(ProvId(0), "restock-agent", "run-1", "a-model", "v1", [0u8; 32], 1_700_000_000_000, BranchId::new(1, 0)))
        .unwrap();

    // A change stream subscribed before the runtime was built, lagging from here on.
    let pin = wal.pin_durable();
    let base = wal.base_lsn.load(Ordering::SeqCst);
    txn.declare_runs_of(&store).unwrap();
    for _ in 0..3 {
        txn.checkpoint().unwrap();
    }
    assert_eq!(wal.base_lsn.load(Ordering::SeqCst), base, "premise failed: a checkpoint truncated past a held pin");

    let mut declared = Vec::new();
    let (mut lsn, end) = (base, wal.next_lsn.load(Ordering::SeqCst));
    while lsn < end {
        let (rec, next) = wal.read_record(lsn).unwrap();
        if let (0, RecKind::RunIdentity { run }) = (rec.txn_id, rec.kind) {
            declared.push(run.prov_id);
        }
        lsn = next;
    }
    assert_eq!(
        declared,
        vec![slot],
        "the run declared at open is not in the log exactly once while every checkpoint was kept by a pin"
    );
    drop(pin);
}
