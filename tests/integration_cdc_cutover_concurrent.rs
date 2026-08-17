//! E20 — the exactly-once cutover under **real threads**, not a scripted interleaving.
//!
//! `integration_cdc_cutover.rs` drives the scenario statement by statement from one thread. That
//! proves the rule and proves nothing about the thing the rule depends on: that the in-flight set,
//! the high water mark and the oldest open `Begin` are sampled *consistently* while other threads
//! are starting and committing transactions. Reading the code says they are — they are taken under
//! the one lock every transactional append also takes — and reading the code is how the causes of
//! several defects in this repo were originally misattributed. So it is run.
//!
//! # What is deliberately deterministic, and what is deliberately not
//!
//! Writers race freely: `begin`, the log appends and `commit` are unsynchronised, which is the
//! window `begin_snapshot_read` has to sample correctly. Only the heap page mutation is serialised,
//! because a torn directory would be a storage bug reported as a cutover bug and this test is not
//! about that.
//!
//! One writer is coordinated: it holds a transaction open across the snapshot, and the snapshot is
//! not taken until several other transactions have committed *after* that one began. Without that
//! the test would still usually exercise the interesting case and would sometimes quietly not —
//! and a concurrency test that is sometimes vacuous is worse than one that is never vacuous,
//! because the green run tells you nothing about which kind you got. The anti-vacuity assertions at
//! the end fail the test rather than let it pass having checked nothing.

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::{Column, DataType, Value};
use ferrodb::catalog::schema::Schema;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::replication::snapshot::SnapshotBoundaryBuilder;
use ferrodb::replication::stream::{FeedStreamer, Subscription};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::{ReadView, TxnManager};

fn schema() -> Schema {
    Schema::new(vec![
        Column { name: "id".into(), data_type: DataType::Integer, nullable: false },
        Column { name: "qty".into(), data_type: DataType::Integer, nullable: true },
    ])
}

/// Ids carried by a JSONL feed's `after` images, with multiplicity.
fn stream_ids(feed: &str) -> Vec<i32> {
    let key = "\"after\":{\"id\":";
    let mut out = Vec::new();
    for line in feed.lines() {
        let Some(i) = line.find(key) else {
            panic!("a feed line carried no row id: {line}");
        };
        let rest = &line[i + key.len()..];
        let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
        out.push(rest[..end].parse::<i32>().unwrap());
    }
    out
}

#[test]
fn every_committed_row_arrives_exactly_once_with_writers_running_throughout() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("c.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("c.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());

    let dir_root = HeapFileManager::new(bp.clone()).unwrap().first_directory_page_id;
    // Serialises page mutation only. Transaction lifecycle stays unsynchronised.
    let heap_lock = Arc::new(Mutex::new(()));

    let next_id = Arc::new(AtomicI32::new(1));
    let committed = Arc::new(Mutex::new(Vec::<i32>::new()));
    let commits = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let slow_open = Arc::new(AtomicBool::new(false));
    let snapshot_taken = Arc::new(AtomicBool::new(false));

    let write_row = {
        let bp = Arc::clone(&bp);
        let txn = Arc::clone(&txn);
        let heap_lock = Arc::clone(&heap_lock);
        move |txn_id: u64, id: i32| {
            let _g = heap_lock.lock().unwrap();
            let mut heap = HeapFileManager::open(dir_root, Arc::clone(&bp));
            heap.set_transaction(Arc::clone(&txn), txn_id);
            let t = Tuple::serialize(
                &[Value::Integer(id), Value::Integer(id * 10)],
                &schema(),
                txn_id,
            )
            .unwrap();
            heap.insert(t).unwrap();
        }
    };

    let (snap_ids, handoff, slow_id) = std::thread::scope(|s| {
        // Racing writers: unsynchronised begin/commit for the whole run.
        for _ in 0..4 {
            let (txn, next_id, committed, commits, stop, write_row) = (
                Arc::clone(&txn),
                Arc::clone(&next_id),
                Arc::clone(&committed),
                Arc::clone(&commits),
                Arc::clone(&stop),
                write_row.clone(),
            );
            s.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let id = next_id.fetch_add(1, Ordering::SeqCst);
                    let t = txn.begin().unwrap();
                    write_row(t, id);
                    txn.commit(t).unwrap();
                    committed.lock().unwrap().push(id);
                    commits.fetch_add(1, Ordering::SeqCst);
                }
            });
        }

        // One writer holds a transaction open across the snapshot. Its records are below the
        // resume point's target and its work is NOT in the snapshot, so the stream owes it.
        let slow_id = next_id.fetch_add(1, Ordering::SeqCst);
        {
            let (txn, committed, slow_open, snapshot_taken, write_row) = (
                Arc::clone(&txn),
                Arc::clone(&committed),
                Arc::clone(&slow_open),
                Arc::clone(&snapshot_taken),
                write_row.clone(),
            );
            s.spawn(move || {
                let t = txn.begin().unwrap();
                write_row(t, slow_id);
                slow_open.store(true, Ordering::SeqCst);
                while !snapshot_taken.load(Ordering::SeqCst) {
                    std::hint::spin_loop();
                }
                txn.commit(t).unwrap();
                committed.lock().unwrap().push(slow_id);
            });
        }

        // Wait for the slow transaction to be open, then for several commits to land *after* it —
        // those are in the snapshot and above the resume point, so they are what the boundary has
        // to suppress.
        while !slow_open.load(Ordering::SeqCst) {
            std::hint::spin_loop();
        }
        let baseline = commits.load(Ordering::SeqCst);
        while commits.load(Ordering::SeqCst) < baseline + 8 {
            std::hint::spin_loop();
        }

        // **The cutover**, taken while all four writers are still running. The handoff arrives
        // already pinned, so there is no separate pin to take here.
        let handoff = txn.begin_snapshot_read().unwrap();
        let view = ReadView { snapshot: handoff.snapshot.clone(), txn_id: handoff.txn_id };
        let snap_ids: Vec<i32> = {
            let _g = heap_lock.lock().unwrap();
            HeapFileManager::open(dir_root, Arc::clone(&bp))
                .scan()
                .map(|r| r.unwrap().1)
                .filter(|t| view.visible(&t.version_header().unwrap()))
                .map(|t| match t.deserialize(&schema()).unwrap()[0] {
                    Value::Integer(i) => i,
                    _ => panic!("id is not an integer"),
                })
                .collect()
        };
        txn.end_read_only(handoff.txn_id).unwrap();
        snapshot_taken.store(true, Ordering::SeqCst);

        // Let the writers run on past the cutover, then stop them.
        let after = commits.load(Ordering::SeqCst);
        while commits.load(Ordering::SeqCst) < after + 8 {
            std::hint::spin_loop();
        }
        stop.store(true, Ordering::SeqCst);

        // **The pin travels out with the results**, and dropping it here instead is what this test
        // used to do. The writers below run on for another eight commits and the checkpointer runs
        // with them, so at FERRODB_CHECKPOINT_INTERVAL=1 the resume point is truncated away long
        // before the subscription is built - "cannot pin lsn 133: the log has already been
        // truncated to base 5163". It survived only because the default interval is 256.
        (snap_ids, handoff, slow_id)
    });

    wal.flush().unwrap();

    let _resume_lsn = handoff.resume_lsn;

    // Stream from the handoff, skipping what the snapshot already had. This snapshot read `t` by
    // scanning the heap directly rather than by writing a feed, so the table is asserted through
    // the builder's escape hatch - which is the point of it having a name that says so.
    let mut boundary = SnapshotBoundaryBuilder::new(handoff);
    boundary.delivered_elsewhere("t");
    let (boundary, handoff_pin) = boundary.finish();

    let streamer = FeedStreamer::new(LogicalDecoder::for_table(dir_root, "t", schema(), u32::MAX))
        .resuming_after_snapshot(boundary);
    let mut sub = Subscription::following(&wal, &streamer).expect("subscribe at the handoff");
    // The subscription holds its own claim from here, so the handoff's can go. Released explicitly
    // rather than at end of scope, so that what keeps the log alive is never ambiguous.
    drop(handoff_pin);
    let mut feed: Vec<u8> = Vec::new();
    let mut suppressed = 0;
    for round in 0..2000 {
        let before = sub.cursor();
        let p = sub.pump(&streamer, &mut feed).unwrap();
        suppressed += p.suppressed;
        assert!(p.is_clean(), "the stream could not decode records: {p:?}");
        if p.emitted == 0 && p.cursor == before {
            break;
        }
        assert!(round < 1999, "the stream did not terminate");
    }
    let streamed = stream_ids(&String::from_utf8(feed).unwrap());

    let committed = committed.lock().unwrap().clone();

    // Anti-vacuity first: a green run that exercised neither half of the join proves nothing.
    assert!(committed.len() >= 20, "only {} rows were written", committed.len());
    assert!(!snap_ids.is_empty(), "the snapshot was empty, so its half of the join is untested");
    assert!(!streamed.is_empty(), "the stream was empty, so its half of the join is untested");
    // The gap direction, asserted before the scaffolding guard below so a resume point that failed
    // to reach back is diagnosed as the data loss it is rather than as an unexercised test.
    assert!(
        !snap_ids.contains(&slow_id),
        "the snapshot contained a transaction that was still open when it was taken"
    );
    assert!(
        streamed.contains(&slow_id),
        "the transaction held open across the cutover never reached the stream: its records sit \
         below the resume point"
    );
    assert!(
        suppressed > 0,
        "nothing was suppressed, so the resume point never reached back over commits the snapshot \
         already had and the overlap was never exercised"
    );

    // **The assertion.** Every committed row exactly once across the two feeds; nothing else.
    let mut counts = std::collections::BTreeMap::<i32, usize>::new();
    for id in snap_ids.iter().chain(streamed.iter()) {
        *counts.entry(*id).or_insert(0) += 1;
    }
    let mut expected = committed.clone();
    expected.sort_unstable();
    for id in &expected {
        match counts.get(id) {
            None => panic!("committed id {id} reached neither feed"),
            Some(1) => {}
            Some(n) => panic!("committed id {id} was delivered {n} times"),
        }
    }
    let seen: Vec<i32> = counts.keys().copied().collect();
    assert_eq!(seen, expected, "the feeds carried ids that were never committed");
}
