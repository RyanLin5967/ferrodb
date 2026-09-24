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
    sent.enqueue(vec![HistoryRecord { hseq: 1, ordinal: 1, commit_lsn: 1, body: b"a publish".to_vec() }]);
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

/// [`engine_at`], with a REVERT history store attached and one page written, so a capture has an
/// image to take (`backup::take` refuses a zero-page one).
fn engine_with_history(dir: &Path, tag: &str, history: Arc<HistoryStore>) -> Box<PageStoreSnapshots> {
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
    // `new_page` writes the page and leaves it unpinned.
    pool.new_page().unwrap();
    pool.flush_all().unwrap();
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
    Box::new(
        PageStoreSnapshots::new(pool, wal, arenas, branches, CATALOG_PAGE, paths, dir.join("scratch"))
            .with_history(history),
    )
}

/// **AMENDED 3, item 2.** A capture ships the sender's REVERT history, a record still queued at
/// capture included, and the install keeps exactly the records committed below the image's
/// `end_lsn`, replacing what the receiver held and emptying its queue. Durable when it returns.
///
/// Mutants: `capture` copies the window without draining (record 2 is missing); the install keeps
/// records past `end_lsn` (record 3 arrives); the install merges into the receiver's own history
/// (records 7 and 8 survive).
#[test]
fn a_capture_ships_the_queued_history_and_nothing_past_its_end() {
    let rec = |hseq: u64, commit_lsn: u64| HistoryRecord {
        hseq,
        ordinal: hseq,
        commit_lsn,
        body: format!("publish {hseq}").into_bytes(),
    };
    let src = tempfile::tempdir().unwrap();
    let sent = HistoryStore::open(src.path().join("src.db.history"), 8).unwrap();
    sent.enqueue(vec![rec(1, 0)]);
    sent.drain().unwrap();
    // Committed and still queued when the capture runs.
    sent.enqueue(vec![rec(2, 0)]);
    // Committed after any `end_lsn` this capture can read: its rows are not in the image.
    sent.enqueue(vec![rec(3, u64::MAX - 1)]);
    let mut from = engine_with_history(src.path(), "src", Arc::clone(&sent));

    let dst = tempfile::tempdir().unwrap();
    let held = HistoryStore::open(dst.path().join("dst.db.history"), 8).unwrap();
    held.enqueue(vec![rec(7, 0)]);
    held.drain().unwrap();
    held.enqueue(vec![rec(8, 0)]);
    let mut to = engine_with_history(dst.path(), "dst", Arc::clone(&held));

    let at = SnapshotPoint { last_round: 1, last_term: 1, config: Config::new(IDS, 1, 0), base_digest: 0 };
    let snap = from.capture(&at).expect("capture failed");
    assert!(snap.header.history_len > 0, "premise: the capture shipped no history");
    let spool = dst.path().join("payload");
    std::fs::write(&spool, snap.payload()).unwrap();
    to.install(&snap.meta, &spool).expect("install failed");

    let hseqs = |s: &HistoryStore| s.records().iter().map(|r| r.hseq).collect::<Vec<u64>>();
    assert_eq!(hseqs(&held), [1, 2], "the receiver's history is not the sender's cut at end_lsn");
    let reopened = HistoryStore::open(dst.path().join("dst.db.history"), 8).unwrap();
    assert_eq!(hseqs(&reopened), [1, 2], "the installed history is not durable");
}

/// **AMENDED 3, item 2.** The install re-queues, by membership, the committed history its redo window
/// carries: a record committed between the sender's drain and `end_lsn` is in the window and not in
/// the shipped copy. A transaction in the window with no `Commit` contributes nothing.
///
/// Forced rather than raced, as `integration_cluster_snapshot.rs` forces its window: a real capture's
/// sections are rebuilt around a redo window written for the purpose.
///
/// Mutants: the install skips the window (record 5 is missing); it takes uncommitted parts (record 6
/// arrives).
#[test]
fn an_install_requeues_the_redo_windows_committed_history() {
    use ferrodb::consensus::snapshot::Snapshot;
    use ferrodb::replication::backup::BackupLabel;
    use ferrodb::replication::ReplicationSource;
    use ferrodb::wal::log::RecKind;

    let src = tempfile::tempdir().unwrap();
    let sent = HistoryStore::open(src.path().join("src.db.history"), 8).unwrap();
    let mut from = engine_with_history(src.path(), "src", Arc::clone(&sent));
    let at = SnapshotPoint { last_round: 1, last_term: 1, config: Config::new(IDS, 1, 0), base_digest: 0 };
    let taken = from.capture(&at).expect("capture failed");

    // The window: txn 7 commits history record 5; txn 8 writes record 6 and never commits.
    let wal = WalManager::new(src.path().join("window.wal")).unwrap();
    let part = |hseq: u64| RecKind::RevertHistory { hseq, ordinal: hseq, part: 0, last: true, bytes: format!("publish {hseq}").into_bytes() };
    let b7 = wal.append(7, 0, &RecKind::Begin).unwrap();
    let h7 = wal.append(7, b7, &part(5)).unwrap();
    let commit = wal.append(7, h7, &RecKind::Commit).unwrap();
    let b8 = wal.append(8, 0, &RecKind::Begin).unwrap();
    wal.append(8, b8, &part(6)).unwrap();
    wal.flush().unwrap();
    let wal = Arc::new(wal);
    let source = ReplicationSource::new(&wal);
    let base = source.start_lsn();
    let (redo, end) = source.read_from(base, 1 << 20).unwrap();
    assert!(commit < end, "premise: the Commit is inside the window");

    let h = &taken.header;
    let body = &taken.payload()[ferrodb::consensus::snapshot::PayloadHeader::BYTES..];
    let (a, b, w, hl) = (h.arena_len as usize, h.branches_len as usize, h.wal_len as usize, h.history_len as usize);
    let history = &body[a + b + w..a + b + w + hl];
    let image = body[a + b + w + hl..].to_vec();
    assert_eq!(image.len() as u64, h.image_len, "premise: the sections were sliced where the header says");
    let with_window = Snapshot::build_with_history(
        &at,
        h.root_page_id,
        h.catalog_page_id,
        BackupLabel { start_lsn: base, end_lsn: end, page_count: h.page_count },
        &body[..a],
        &body[a..a + b],
        &redo,
        history,
        image,
    )
    .expect("a well-formed payload was refused");

    let dst = tempfile::tempdir().unwrap();
    let held = HistoryStore::open(dst.path().join("dst.db.history"), 8).unwrap();
    let mut to = engine_with_history(dst.path(), "dst", Arc::clone(&held));
    let spool = dst.path().join("payload");
    std::fs::write(&spool, with_window.payload()).unwrap();
    to.install(&with_window.meta, &spool).expect("install failed");

    let got: Vec<(u64, u64)> = held.records().iter().map(|r| (r.hseq, r.commit_lsn)).collect();
    assert_eq!(got, [(5, commit)], "the install did not take exactly the window's committed history");
}
