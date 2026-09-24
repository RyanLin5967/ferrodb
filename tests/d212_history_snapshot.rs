//! **D212 (a') AMENDED 2, F6 — the pin on the one gap left open by decision:** a snapshot install
//! does not carry `<db>.history`.
//!
//! `StorePaths` names the page file, the arena image and the branch catalog, and nothing else, so a
//! follower re-seeded by `InstallSnapshot` (or a node restored from its payload) keeps whatever
//! history it had — or none — beside a database whose merges it never saw. A REVERT of one of those
//! merges is then refused as an earlier run's that the history does not hold: the safe direction,
//! and still a gap. The lead's decision: single-node lands first; shipping the store in the snapshot
//! (and resetting the queue on install) is REQUIRED before D212 is enabled on a clustered member.
//!
//! So this test is written RED and `#[ignore]`d, naming the decision, rather than left unwritten:
//! it is the exit test of that later work. Run it with `--ignored`; it fails until the install
//! carries the sender's history.

use std::path::Path;
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::consensus::config::Config;
use ferrodb::consensus::snapshot::{
    PageStoreSnapshots, SnapshotPoint, SnapshotStore, StorePaths,
};
use ferrodb::consensus::NodeId;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::history::{HistoryRecord, HistoryStore};
use ferrodb::wal::log::WalManager;

const ARENA_BASE: u32 = 1024;
const CATALOG_PAGE: u32 = 1;
const IDS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

fn engine_at(dir: &Path, tag: &str) -> Box<PageStoreSnapshots> {
    let page_file = dir.join(format!("{tag}.db"));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&page_file)
        .unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.join(format!("{tag}.wal"))).unwrap());
    pool.attach_wal(Arc::clone(&wal));
    let branch_catalog = dir.join(format!("{tag}.branches"));
    let branches = Arc::new(LogBranchCatalog::open(&branch_catalog, 1).unwrap());
    let arenas = Arc::new(
        ArenaPageStore::new(
            Arc::clone(&pool),
            Arc::clone(&branches) as Arc<dyn ferrodb::branch::BranchCatalog>,
            ARENA_BASE,
        )
        .unwrap(),
    );
    let paths =
        StorePaths { page_file, arena_image: dir.join(format!("{tag}.arena")), branch_catalog };
    Box::new(PageStoreSnapshots::new(
        pool,
        wal,
        arenas,
        branches,
        CATALOG_PAGE,
        paths,
        dir.join("scratch"),
    ))
}

#[test]
#[ignore = "D212 (a') AMENDED 2 F6: a snapshot does not ship <db>.history yet; required before D212 \
            is enabled on a clustered member (single-node lands first)"]
fn an_install_carries_the_senders_revert_history() {
    let src = tempfile::tempdir().unwrap();
    let mut from = engine_at(src.path(), "src");
    let sent = HistoryStore::open(src.path().join("src.db.history"), 8).unwrap();
    sent.enqueue(vec![HistoryRecord { hseq: 1, ordinal: 1, body: b"a publish".to_vec() }]);
    sent.drain().unwrap();

    let dst = tempfile::tempdir().unwrap();
    let mut to = engine_at(dst.path(), "dst");
    let at = SnapshotPoint { last_round: 1, last_term: 1, config: Config::new(IDS, 1, 0), base_digest: 0 };
    let snap = from.capture(&at).expect("capture failed");
    let spool = dst.path().join("payload");
    std::fs::write(&spool, snap.payload()).unwrap();
    to.install(&snap.meta, &spool).expect("install failed");

    let received = HistoryStore::open(dst.path().join("dst.db.history"), 8).unwrap();
    assert_eq!(
        received.records(),
        sent.records(),
        "the follower's REVERT history is not the leader's after an install"
    );
}
