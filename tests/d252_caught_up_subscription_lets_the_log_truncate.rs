//! D252 — a change-feed `Subscription` that has read everything must let a checkpoint truncate the
//! log.
//!
//! A subscription pins the log at its cursor, and `truncate` keeps the whole log while any pin is
//! below its end. At `00f4c39` a caught-up cursor never reached the end, for two reasons:
//!
//! 1. the pump's cursor stopped at the last event's `commit_end_lsn`, the end of its `Commit`, and
//!    never passed the records after it that yield no event (the `TxnEnd` every commit appends, a
//!    rollback, a run declaration);
//! 2. `commit` appended its `TxnEnd` AFTER the flush that made the `Commit` durable, and a pump reads
//!    only what is durable, so the `TxnEnd` stayed invisible until the next flush, usually the
//!    checkpoint's own.
//!
//! So no checkpoint truncated while any subscription lived. The first test needs both halves fixed;
//! the second isolates the first half, with a rollback's durable tail after the last commit.
//!
//! The outcome is read from `base_lsn`, which a truncation moves to the log's end and a kept log
//! leaves alone. Each test first shows a subscription that has NOT read the log keeps it, so a
//! checkpoint that ignored pins could not pass.
//!
//! Pre-registered from source, UNBUILT: both FAIL at `00f4c39` at the `base_lsn` assertion after the
//! drain.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::{Column, DataType, Value};
use ferrodb::catalog::schema::Schema;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::replication::publication::Publication;
use ferrodb::replication::stream::{FeedStreamer, Subscription};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::{RecKind, WalManager};
use ferrodb::wal::txn::TxnManager;

fn schema() -> Schema {
    Schema::new(vec![
        Column { name: "id".into(), data_type: DataType::Integer, nullable: false },
        Column { name: "qty".into(), data_type: DataType::Integer, nullable: true },
    ])
}

struct Db {
    _dir: tempfile::TempDir,
    wal: Arc<WalManager>,
    txn: Arc<TxnManager>,
    heap: HeapFileManager,
    feed: FeedStreamer,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("d252.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("d252.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let heap = HeapFileManager::new(bp.clone()).unwrap();
    // The decoder is told the table out of band, so every committed row is an event and the cursor
    // has a `Commit` to stop at. The time-travel root is one no page can have.
    let feed = FeedStreamer::new(
        LogicalDecoder::for_table(heap.first_directory_page_id, "inventory", schema(), u32::MAX),
        Publication::unrestricted(),
    );
    Db { _dir: dir, wal, txn, heap, feed }
}

impl Db {
    /// One transaction inserting one row, committed or rolled back.
    fn write(&mut self, id: i32, commit: bool) {
        let t = self.txn.begin().unwrap();
        self.heap.set_transaction(self.txn.clone(), t);
        let row = Tuple::serialize(&[Value::Integer(id), Value::Integer(id * 10)], &schema(), 0).unwrap();
        self.heap.insert(row).unwrap();
        if commit {
            self.txn.commit(t).unwrap();
        } else {
            self.txn.abort(t).unwrap();
        }
    }

    /// Pump until a pump emits nothing; the events emitted in all.
    fn drain(&self, sub: &mut Subscription) -> usize {
        let mut emitted = 0;
        for _ in 0..16 {
            let p = sub.pump(&self.feed, &mut Vec::<u8>::new()).unwrap();
            assert!(p.is_clean(), "premise failed: the feed is not clean, so its cursor proves nothing: {p:?}");
            if p.emitted == 0 {
                return emitted;
            }
            emitted += p.emitted;
        }
        panic!("the feed was still emitting after 16 pumps");
    }

    fn base(&self) -> u64 {
        self.wal.base_lsn.load(Ordering::SeqCst)
    }

    /// The control: a subscription that has not read the log keeps it through a checkpoint.
    fn a_lagging_subscription_keeps_the_log(&mut self) -> Subscription {
        let sub = Subscription::from_start(&self.wal).unwrap();
        self.write(1, true);
        let base = self.base();
        self.txn.checkpoint().unwrap();
        assert_eq!(
            self.base(),
            base,
            "premise failed: a checkpoint truncated records a subscription had not read, so a \
             truncation below could not be credited to its cursor"
        );
        sub
    }
}

/// **Subscribe, commit, drain, checkpoint: the log truncates.** Both halves: the cursor passes the
/// commit's `TxnEnd`, and the `TxnEnd` is durable by the time the drain reads.
#[test]
fn a_subscription_that_has_read_every_commit_lets_the_checkpoint_truncate() {
    let mut d = db();
    let mut sub = d.a_lagging_subscription_keeps_the_log();
    d.write(2, true);
    assert_eq!(d.drain(&mut sub), 2, "premise failed: the drain did not deliver both committed rows");
    let base = d.base();
    d.txn.checkpoint().unwrap();
    assert!(
        d.base() > base,
        "a subscription that had delivered every commit kept the log through a checkpoint: its \
         cursor {} stopped below the log's end {} (base still {base})",
        sub.cursor(),
        d.wal.next_lsn.load(Ordering::SeqCst)
    );

    // The truncation did not strand it: the next commit still reaches the feed.
    d.write(3, true);
    assert_eq!(d.drain(&mut sub), 1, "the subscription lost the commit made after the truncation");
}

/// **A rollback after the last commit is passed over once it is durable.** The first half alone:
/// the flush here is any later one (a page write-back, a snapshot read), which writes the rolled-back
/// transaction's records, and none of them yields an event.
#[test]
fn a_subscription_passes_a_durable_rollback_after_the_last_commit() {
    let mut d = db();
    let mut sub = d.a_lagging_subscription_keeps_the_log();
    d.write(2, false);
    d.wal.flush().unwrap();
    assert_eq!(d.drain(&mut sub), 1, "premise failed: the drain did not deliver exactly the committed row");

    // Premise: the log after the last `Commit` holds the rollback, so the cursor had something to pass.
    let mut lsn = d.base();
    let (mut last_commit_end, mut aborted_after) = (0, false);
    while lsn < d.wal.next_lsn.load(Ordering::SeqCst) {
        let (rec, next) = d.wal.read_record(lsn).unwrap();
        match rec.kind {
            RecKind::Commit => (last_commit_end, aborted_after) = (next, false),
            RecKind::Abort => aborted_after = true,
            _ => {}
        }
        lsn = next;
    }
    assert!(
        aborted_after && last_commit_end < d.wal.flushed_lsn.load(Ordering::SeqCst),
        "premise failed: no durable rollback follows the last commit"
    );

    let base = d.base();
    d.txn.checkpoint().unwrap();
    assert!(
        d.base() > base,
        "a subscription that had delivered every commit kept the log because a rolled-back \
         transaction followed it: cursor {}, log end {}",
        sub.cursor(),
        d.wal.next_lsn.load(Ordering::SeqCst)
    );
}
