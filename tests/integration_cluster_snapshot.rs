//! F6 — **exit criterion 3**: a follower partitioned for longer than the log retention rejoins via
//! `InstallSnapshot` and converges.
//!
//! `src/consensus/tests_snapshot.rs` pins the *rules* — it steps the state machine by hand, and
//! nothing in it touches a disk. This file pins the *wiring*: that the driver actually captures a
//! snapshot from a real `BufferPoolManager`, streams it over a real socket, spools it, installs it
//! through `replication::backup`, and moves the follower's floor to it.
//!
//! # The anti-vacuity half, which is the whole point
//!
//! A cluster that converged by ordinary replication passes any convergence check you write, so
//! "the follower caught up" proves nothing about F6. Two things are asserted instead:
//!
//! 1. **The path was taken.** `Node::snapshots_installed` counts completed installs and
//!    `Node::snapshots_sent` counts armed transfers. A run in which neither moves has not tested
//!    state transfer, and the test says so by name rather than passing.
//! 2. **The bytes moved.** The leader's page file is seeded with pages the follower has never had,
//!    and the follower's page file is compared against it afterwards. Seeded *after* the cluster
//!    forms, so the two files are genuinely different beforehand — a comparison of two empty files
//!    would pass without a single byte crossing the wire.
//!
//! And the retention is shrunk to zero (`retain_rounds: 0`), which is what makes a follower one
//! round behind fall below the leader's floor at all. Without it the snapshot path is code a
//! passing test never enters.
//!
//! # What "partitioned" means here
//!
//! One node is left out of the poll loop. Its transport thread keeps accepting frames into a
//! bounded inbox that drops the oldest, which is what a partition looks like from inside the node:
//! time passes, nothing arrives that it can act on, and the cluster moves on without it. Nothing is
//! faked at the protocol level — the other two nodes go on sending it messages the whole time.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::consensus::config::Config;
use ferrodb::consensus::node::{Node, NodeOptions, RecordingApplier};
use ferrodb::consensus::transport::{Transport, TransportOptions};
use ferrodb::consensus::snapshot::{PageStoreSnapshots, SnapshotMeta, StorePaths};
use ferrodb::consensus::{Body, Command, Message, NodeId, Role};
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::wal::log::WalManager;

/// Deliberately above anything these tests allocate, matching `tests/integration_cluster_grants.rs`.
const ARENA_BASE: u32 = 1024;
/// The catalog's first page on a ferrodb database. Passed rather than assumed by the module under
/// test; restated here because a test that read it from the code could not catch it changing.
const CATALOG_PAGE: u32 = 1;

const IDS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

/// Generous: these run on a loaded machine beside several other cargo processes, and a flaky
/// timeout here would be indistinguishable from the bug the test exists to catch.
const BUDGET: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------- one node's engine

struct Engine {
    pool: Arc<BufferPoolManager>,
    page_file: PathBuf,
}

/// A node: its storage engine, its consensus driver, and the directory both live in.
struct Member {
    node: Node<RecordingApplier>,
    engine: Engine,
    _dir: tempfile::TempDir,
}

/// `fresh` truncates the page file. A restart must NOT: the whole question a restart answers is
/// what a node reads off its own disk.
fn engine_at(dir: &Path, tag: &str, fresh: bool) -> (Engine, Box<PageStoreSnapshots>) {
    let page_file = dir.join(format!("{tag}.db"));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(fresh)
        .open(&page_file)
        .unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.join(format!("{tag}.wal"))).unwrap());
    pool.attach_wal(Arc::clone(&wal));

    let branch_catalog = dir.join(format!("{tag}.branches"));
    let branches = Arc::new(LogBranchCatalog::open(&branch_catalog, 1).unwrap());
    let arenas =
        Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&branches), ARENA_BASE).unwrap());

    let paths = StorePaths {
        page_file: page_file.clone(),
        arena_image: dir.join(format!("{tag}.arena")),
        branch_catalog,
    };
    let store = Box::new(PageStoreSnapshots::new(
        Arc::clone(&pool),
        wal,
        arenas,
        branches,
        CATALOG_PAGE,
        paths,
        dir.join("scratch"),
    ));
    (Engine { pool, page_file }, store)
}

/// Write `n` pages whose bytes are a function of `(mark, page)`, so two engines' files are equal
/// only if the bytes actually crossed.
fn seed_pages(e: &Engine, n: usize, mark: u8) {
    for i in 0..n {
        let id = e.pool.new_page().unwrap();
        let frame_i = e.pool.fetch_page(id).unwrap();
        {
            let mut f = e.pool.frames[frame_i].write().unwrap();
            for (k, b) in f.data.iter_mut().enumerate() {
                *b = mark ^ (i as u8) ^ (k as u8);
            }
        }
        e.pool.unpin_page(id, true);
    }
    e.pool.flush_all().unwrap();
}

/// Three nodes, three listeners bound before anything is told about anything.
///
/// Binding first is not tidiness: every node's peer map needs every other node's address, so
/// binding A to discover its port and then constructing B is circular, and picking free ports and
/// closing them leaves a window in which a port is published but unowned.
fn cluster(retain_rounds: u64) -> Vec<Member> {
    let listeners: Vec<TcpListener> =
        (0..3).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    let addrs: Vec<SocketAddr> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    let cfg = Config::new(IDS, 1, 0);

    listeners
        .into_iter()
        .enumerate()
        .map(|(i, listener)| {
            let dir = tempfile::tempdir().unwrap();
            let (engine, store) = engine_at(dir.path(), &format!("n{}", i + 1), true);
            let mut peers = BTreeMap::new();
            for (j, a) in addrs.iter().enumerate() {
                if j != i {
                    peers.insert(IDS[j], *a);
                }
            }
            // The default tick. A shorter one was tried and is the wrong knob to turn here: the
            // leader's lease is eight ticks, and a test loop that spends longer than that between
            // polls costs the leader its office for reasons that have nothing to do with F6.
            let opts = NodeOptions::new(dir.path().join("consensus"), peers, (i as u64) + 1)
                .with_snapshots(store, retain_rounds);
            let node = Node::start(IDS[i], cfg.clone(), listener, opts, RecordingApplier::default())
                .unwrap();
            Member { node, engine, _dir: dir }
        })
        .collect()
}

/// Poll only the members named in `live`, so the others are partitioned by omission.
fn poll(members: &mut [Member], live: &[usize]) {
    for &i in live {
        members[i].node.poll(Duration::from_millis(2)).expect("a node's driver failed");
    }
}

fn pump_until(
    members: &mut [Member],
    live: &[usize],
    what: &str,
    mut done: impl FnMut(&[Member]) -> bool,
) {
    let deadline = Instant::now() + BUDGET;
    while Instant::now() < deadline {
        poll(members, live);
        if done(members) {
            return;
        }
    }
    let state: Vec<String> = members
        .iter()
        .map(|m| {
            format!(
                "{}={:?} term={} commit={} floor={} sent={} installed={}",
                m.node.id(),
                m.node.role(),
                m.node.term(),
                m.node.commit_round(),
                m.node.snapshot_round(),
                m.node.snapshots_sent(),
                m.node.snapshots_installed(),
            )
        })
        .collect();
    panic!("timed out waiting for {what}. Nodes: {state:#?}");
}

/// Propose `n` rounds to whoever currently leads among `live`, polling between each.
///
/// Re-resolving the leader every round rather than assuming the first one keeps its office: a
/// leader change is legitimate and says nothing about state transfer, and a test that proposed into
/// a demoted node would fail with a timeout that named the wrong cause. (It did, before this.)
fn propose_rounds(members: &mut [Member], live: &[usize], n: u64) {
    for k in 0..n {
        let mut at = None;
        let deadline = Instant::now() + BUDGET;
        while Instant::now() < deadline {
            if let Some(l) = leader_of(members).filter(|l| live.contains(l)) {
                at = Some(l);
                break;
            }
            poll(members, live);
        }
        let l = at.unwrap_or_else(|| panic!("no leader among the live nodes at round {k}"));
        members[l]
            .node
            .propose(Command::WalBatch { start_lsn: k, bytes: vec![k as u8; 64] })
            .expect("the leader refused its own proposal");
        for _ in 0..4 {
            poll(members, live);
        }
    }
}

fn leader_of(members: &[Member]) -> Option<usize> {
    let leaders: Vec<usize> =
        (0..members.len()).filter(|&i| members[i].node.role() == Role::Leader).collect();
    match leaders.len() {
        1 => Some(leaders[0]),
        0 => None,
        _ => panic!("two leaders at once: {leaders:?}"),
    }
}

fn shutdown(members: &[Member]) {
    for m in members {
        m.node.shutdown();
    }
}

// ---------------------------------------------------------------------------- the criterion

/// **Exit criterion 3.** A follower partitioned past the log retention rejoins via
/// `InstallSnapshot` and converges — and the transfer is proven to have happened, not inferred from
/// the fact that it caught up.
#[test]
fn a_follower_partitioned_past_log_retention_rejoins_by_snapshot_and_converges() {
    // Retention zero: the leader checkpoints to what its engine has applied on every turn, so a
    // follower that misses even a few rounds falls below its floor. Without this the snapshot path
    // is never entered and every assertion below would pass on ordinary replication.
    let mut m = cluster(0);
    pump_until(&mut m, &[0, 1, 2], "a leader", |m| leader_of(m).is_some());
    let leader = leader_of(&m).expect("checked above");
    let cut = (0..3).find(|&i| i != leader).expect("three nodes, one leader");
    let live: Vec<usize> = (0..3).filter(|&i| i != cut).collect();

    // The state that has to cross the wire. Seeded on every live node, because which of them ends
    // up leading is not this test's business — and NOT on the cut node, so the comparison at the
    // end is about bytes that moved rather than about two files that were always equal.
    for &i in &live {
        seed_pages(&m[i].engine, 6, 0xA7);
    }
    let leader_pages = std::fs::read(&m[leader].engine.page_file).unwrap();
    let cut_before = std::fs::read(&m[cut].engine.page_file).unwrap();
    assert_ne!(
        leader_pages, cut_before,
        "the two page files already agree, so comparing them afterwards proves nothing"
    );

    // Enough rounds, with the follower out, that its `next` is far below the leader's floor.
    propose_rounds(&mut m, &live, 24);
    pump_until(&mut m, &live, "the surviving two to commit", |m| {
        leader_of(m).is_some_and(|l| m[l].node.commit_round() >= 24)
    });
    let leader = leader_of(&m).expect("a leader among the live nodes");
    let floor = m[leader].node.snapshot_round();
    assert!(
        floor > 0,
        "the leader never checkpointed, so no follower can be below its floor and this test cannot \
         reach the snapshot path at all"
    );

    // The partition heals.
    pump_until(&mut m, &[0, 1, 2], "the cut node to install a snapshot and converge", |m| {
        m[cut].node.snapshots_installed() > 0 && m[cut].node.commit_round() >= m[leader].node.commit_round()
    });

    // 1. The path was taken.
    assert!(
        m[cut].node.snapshots_installed() > 0,
        "the follower converged without ever installing a snapshot, so this run tested ordinary \
         replication and not state transfer"
    );
    assert!(
        m[leader].node.snapshots_sent() > 0,
        "no InstallSnapshot was ever armed by the leader"
    );

    // 2. The bytes moved.
    let cut_after = std::fs::read(&m[cut].engine.page_file).unwrap();
    assert_eq!(
        &cut_after[..leader_pages.len().min(cut_after.len())],
        &leader_pages[..leader_pages.len().min(cut_after.len())],
        "the follower installed a snapshot and its page file is not the leader's"
    );
    assert!(cut_after.len() >= leader_pages.len(), "the follower's image is short");

    // 3. It converged, on the round watermark and on the floor.
    assert!(
        m[cut].node.snapshot_round() >= floor,
        "the follower's log still begins below the floor it was re-seeded at"
    );
    assert!(
        m[cut].node.commit_round() >= m[leader].node.commit_round(),
        "the follower did not catch up to the leader's commit"
    );
    assert_eq!(m[cut].node.term(), m[leader].node.term(), "the transfer cost an election");

    shutdown(&m);
}

/// **The anti-vacuity half of the criterion above, run as its own case.**
///
/// The same scenario with a retention that keeps the whole log: the follower must converge, and it
/// must do so **without** a snapshot. If this passes and the test above also passes, the
/// `snapshots_installed` counter distinguishes the two paths — which is the only thing that makes
/// the assertion above evidence rather than a tautology.
#[test]
fn with_the_log_retained_the_same_follower_converges_without_a_snapshot() {
    let mut m = cluster(u64::MAX);
    pump_until(&mut m, &[0, 1, 2], "a leader", |m| leader_of(m).is_some());
    let leader = leader_of(&m).expect("checked above");
    let cut = (0..3).find(|&i| i != leader).expect("three nodes, one leader");
    let live: Vec<usize> = (0..3).filter(|&i| i != cut).collect();

    propose_rounds(&mut m, &live, 24);
    pump_until(&mut m, &live, "the surviving two to commit", |m| {
        leader_of(m).is_some_and(|l| m[l].node.commit_round() >= 24)
    });
    let leader = leader_of(&m).expect("a leader among the live nodes");
    assert_eq!(
        m[leader].node.snapshot_round(),
        0,
        "the log was checkpointed despite an infinite retention"
    );

    pump_until(&mut m, &[0, 1, 2], "the cut node to converge", |m| {
        m[cut].node.commit_round() >= m[leader].node.commit_round()
    });
    assert_eq!(
        m[cut].node.snapshots_installed(),
        0,
        "a snapshot was installed on a cluster whose leader still holds the whole log, so the \
         counter cannot tell the two paths apart and the criterion's proof is vacuous"
    );
    shutdown(&m);
}

/// A snapshot claiming an implausible size is refused **before anything is allocated for it** —
/// and, at the driver, before anything is written for it either.
///
/// The state machine's half is `tests_snapshot.rs`; this is the half that can be checked from
/// outside: no spool file appears on the receiving node's disk.
#[test]
fn a_snapshot_claiming_an_implausible_size_leaves_nothing_on_the_receivers_disk() {
    let mut m = cluster(0);
    pump_until(&mut m, &[0, 1, 2], "a leader", |m| leader_of(m).is_some());
    let leader = leader_of(&m).expect("checked above");
    let victim = (0..3).find(|&i| i != leader).expect("three nodes, one leader");

    let spool = m[victim]
        ._dir
        .path()
        .join("consensus")
        .join("snapshot.incoming");
    assert!(!spool.exists(), "the fixture started with a spool file already present");

    let lie = Message {
        from: IDS[leader],
        to: IDS[victim],
        term: m[leader].node.term(),
        body: Body::InstallSnapshot {
            meta: SnapshotMeta {
                last_round: 1,
                last_term: 1,
                config: Config::new(IDS, 1, 0),
                total_bytes: u64::MAX,
            },
            offset: 0,
            data: vec![0u8; 1024],
            done: false,
        },
    };
    // Sent through a real `Transport` rather than through a hole cut in the driver: the frame goes
    // over a socket, through the handshake and the framing, and arrives at the victim exactly as
    // one from its leader would. A leader will never build this message, which is the point — the
    // rule under test is what a receiver does with one that should not exist.
    let liar = Transport::from_listener(
        IDS[leader],
        TcpListener::bind("127.0.0.1:0").unwrap(),
        BTreeMap::from([(IDS[victim], m[victim].node.local_addr())]),
        TransportOptions::default(),
    )
    .unwrap();
    liar.send(&lie).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        poll(&mut m, &[0, 1, 2]);
        assert!(
            !spool.exists(),
            "a transfer of u64::MAX bytes was spooled to disk, so `total_bytes` was acted on \
             before it was refused"
        );
    }
    assert_eq!(m[victim].node.snapshots_installed(), 0);
    liar.shutdown();
    shutdown(&m);
}

/// A node restarted above a snapshot floor comes back on it: it does not try to apply rounds that
/// no longer exist, and it does not forget the digest its floor is anchored at.
///
/// The second half is the one that is invisible until it costs something: every digest a node
/// reports above its floor is chained from that value, so a restart that anchored at zero would
/// disagree with its leader at every round and be latched as diverged — a healthy node,
/// permanently out of the quorum.
#[test]
fn a_node_restarted_above_a_snapshot_floor_comes_back_on_it() {
    let mut m = cluster(0);
    pump_until(&mut m, &[0, 1, 2], "a leader", |m| leader_of(m).is_some());
    let seeded = leader_of(&m).expect("checked above");
    seed_pages(&m[seeded].engine, 3, 0x5C);

    propose_rounds(&mut m, &[0, 1, 2], 12);
    pump_until(&mut m, &[0, 1, 2], "a checkpoint", |m| {
        leader_of(m).is_some_and(|l| m[l].node.snapshot_round() > 0)
    });

    let leader = leader_of(&m).expect("a leader");
    let floor = m[leader].node.snapshot_round();
    let dir = m[leader]._dir.path().to_path_buf();
    let tag = format!("n{}", leader + 1);
    let id = IDS[leader];
    let cfg = Config::new(IDS, 1, 0);
    shutdown(&m);
    // Keep the tempdirs alive across the restart — dropping `m` would delete the disk the restart
    // is supposed to read.
    let keep: Vec<tempfile::TempDir> = m.into_iter().map(|x| x._dir).collect();

    // Reopened alone, with no peers: the question is what a restart reads off its own disk.
    let (_engine, store) = engine_at(&dir, &tag, false);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let opts = NodeOptions::new(dir.join("consensus"), BTreeMap::new(), 9)
        .with_snapshots(store, 0);
    let mut back = Node::start(id, cfg, listener, opts, RecordingApplier::default()).unwrap();

    assert_eq!(back.snapshot_round(), floor, "the restart forgot where its log begins");
    assert_eq!(
        back.applied(),
        floor,
        "the applied cursor came back at zero, so the first `Apply` would walk rounds that no \
         longer exist on this node"
    );
    assert!(
        back.take_refusals().is_empty(),
        "the restart could not recover the digest at its floor, so the divergence detector is now \
         silent for this node"
    );
    back.shutdown();
    drop(keep);
}

/// The payload really is a base backup: what `PageStoreSnapshots` captures restores, through
/// `replication::backup`, to the same bytes.
///
/// This is the seam the brief asked about — F6 streams `backup.rs`'s image rather than walking
/// pages a second time — checked directly rather than only through a cluster.
#[test]
fn a_captured_payload_installs_back_to_the_same_pages() {
    let src = tempfile::tempdir().unwrap();
    let (from_engine, mut from) = engine_at(src.path(), "src", true);
    seed_pages(&from_engine, 5, 0x3B);

    let dst = tempfile::tempdir().unwrap();
    let (to_engine, mut to) = engine_at(dst.path(), "dst", true);
    seed_pages(&to_engine, 2, 0xF0);
    assert_ne!(
        std::fs::read(&from_engine.page_file).unwrap(),
        std::fs::read(&to_engine.page_file).unwrap(),
        "the two engines already agree, so the comparison below proves nothing"
    );

    let at = ferrodb::consensus::snapshot::SnapshotPoint {
        last_round: 9,
        last_term: 2,
        config: Config::new(IDS, 1, 0),
        base_digest: 0x1234_5678_9ABC_DEF0,
    };
    let snap = {
        use ferrodb::consensus::snapshot::SnapshotStore;
        from.capture(&at).expect("capture failed")
    };
    assert_eq!(snap.meta.last_round, 9);
    assert!(snap.header.page_count >= 5, "the capture missed pages the engine holds");

    let spool = dst.path().join("payload");
    std::fs::write(&spool, snap.payload()).unwrap();
    {
        use ferrodb::consensus::snapshot::SnapshotStore;
        to.install(&snap.meta, &spool).expect("install failed");
    }

    let a = std::fs::read(&from_engine.page_file).unwrap();
    let b = std::fs::read(&to_engine.page_file).unwrap();
    let common = a.len().min(b.len());
    assert!(common >= 5 * PAGE_SIZE, "the restored image is shorter than the pages it carried");
    assert_eq!(a[..common], b[..common], "a captured payload did not install back to the same bytes");
}
