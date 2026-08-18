//! Deterministic crash simulation: aim a fault at operation N, reboot, recover, check what survived.
//!
//! # Why this file exists
//!
//! Recovery is the least-exercised code in a database and the code with the worst failure mode. What
//! this suite had before was three hand-staged crashes — `torn_tail_trimmed_on_open` truncates a
//! closed file by three bytes, `interrupted_truncation_self_heals_on_open` rewrites a header — each
//! of which stages the *aftermath* of a crash at a point a person chose. Two classes of fault could
//! not be expressed at all: a write interrupted **part-way through**, and a fault at an operation
//! nobody thought to pick.
//!
//! With [`SimFabric`] behind [`ferrodb::storage::storage::Storage`], every durable operation is
//! counted and any one of them can be broken. So the question stops being "does recovery survive the
//! crash I imagined?" and becomes "does it survive **every** crash this workload can have?", which is
//! a sweep, and a sweep needs the write order to be a function of the data rather than of a hash seed
//! — see `BufferPoolManager::flush_all` and `wal::recovery::recover`.
//!
//! # The breaking shape
//!
//! Every workload here commits **five rows in one transaction**, and every invariant is all-or-nothing
//! over those five. That is deliberate. Two silent data-loss bugs survived in this repository because
//! the generators only ever produced one row per commit: with a single row, "lost all but the last
//! one" and "wrote every row" are the same observation. A partial survival — 1, 2, 3 or 4 rows out of
//! 5 — is the shape that fails here and could not fail before.
//!
//! A second transaction inserts three more rows and never commits, so undo must run during recovery
//! and its rows must be gone afterwards. Without it a recovery that did nothing at all would pass.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::catalog_page::CatalogPage;
use ferrodb::catalog::column::{Column, DataType, Value};
use ferrodb::catalog::schema::Schema;
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::storage::heap_page::Page;
use ferrodb::storage::sim::{
    Durability, FaultKind, FaultPlan, OpKind, SimFabric, TraceOp, WriteShape,
};
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::{crc32, RecKind, WalManager};
use ferrodb::wal::recovery::{rebuild_indexes, recover};
use ferrodb::wal::txn::TxnManager;

const DB: &str = "sim.db";
const WAL: &str = "sim.wal";

/// Rows in the first committed transaction. A hundred and fifty, not one, and wide enough to span a
/// dozen heap pages: see the module note on the breaking shape. A single-page, single-row commit is the shape
/// that hid two data-loss bugs in this repository.
const ROWS: usize = 150;
/// Rows in the second committed transaction, the one followed by a checkpoint.
const ROWS2: usize = 60;
/// Transactions that never commit, and rows in each.
///
/// **Five, not one, and the count is load-bearing.** `recover`'s undo loop took its losers straight
/// from `last_lsn.keys()`, a `HashMap`, and undo writes — a compensation record per action, plus the
/// page it repairs. With one loser there is no order to get wrong; with two, reverting the sort is
/// detected only about half the time, and a fire-check that fails half the time certifies nothing.
/// Measured: with two losers the reverted sort SURVIVED a three-run comparison. Five gives 120
/// possible orders, of which exactly one is the sorted one.
const LOSER_TXNS: usize = 5;
const LOSER_ROWS: usize = 3;

/// Distinct length *and* distinct content per row, so a row that survives can be identified and a
/// row whose bytes were spliced from two different writes cannot pass for a whole one. ~200 bytes, so
/// forty rows do not fit on one 4 KiB page and `flush_all` has an order to get wrong.
fn committed_payload(i: usize) -> Vec<u8> {
    let mut v = vec![0xA0u8; 190 + (i % 7) * 4];
    v[0] = 0xC0;
    v[1] = i as u8;
    v[2] = (i >> 8) as u8;
    v
}

fn committed2_payload(i: usize) -> Vec<u8> {
    let mut v = vec![0xB0u8; 150 + (i % 5) * 6];
    v[0] = 0xD0;
    v[1] = i as u8;
    v
}

/// Recognisably from uncommitted transaction `txn`, row `i`. Distinct content and distinct length per
/// pair, and no length shared with a committed payload, so a row that survives can be attributed.
fn loser_payload(txn: usize, i: usize) -> Vec<u8> {
    let mut v = vec![0x50 | (txn as u8 & 0x0F); 80 + txn * 4 + i];
    v[0] = 0x5A;
    v[1] = txn as u8;
    v[2] = i as u8;
    v
}

/// Every row any uncommitted transaction wrote. None of these may survive recovery.
fn all_loser_rows() -> BTreeSet<Vec<u8>> {
    (0..LOSER_TXNS)
        .flat_map(|t| (0..LOSER_ROWS).map(move |i| loser_payload(t, i)))
        .collect()
}

/// Every fabric in this file is built here, so the fault model is stated once.
///
/// **The database file is given page-atomic writes; the WAL is not.** That asymmetry is the engine's
/// own, not a convenience: the WAL puts a CRC32 on every frame and `scan_valid_end` walks the chain
/// and stops at the first frame that fails it, so a byte-granular torn tail is a fault the log is
/// built to survive and worth sweeping. A table page has no such thing —
/// `heap_page::Page::checksum` is written verbatim, read back verbatim, and computed by nothing —
/// so a torn table page is undetectable by construction and the engine's durability already rests
/// on a page write being all-or-nothing. Sweeping byte-granular tears of table pages would
/// therefore re-derive one known gap at forty offsets instead of testing recovery.
///
/// That gap is not swept under the carpet: `a_torn_table_page_is_served_as_a_row_that_was_never_written`
/// sets the unit back to one byte and pins the consequence.
fn fabric(plan: Option<FaultPlan>, durability: Durability) -> Arc<SimFabric> {
    let f = match plan {
        Some(p) => SimFabric::with_fault(p, durability),
        None => SimFabric::clean(durability),
    };
    f.set_write_atomicity(DB, PAGE_SIZE as u64);
    // The log's records are checksummed and may be garbled; its header is not, and garbling that is a
    // restatement of a gap rather than a test — see
    // `a_garbled_wal_header_takes_the_database_down_with_its_data_intact`.
    f.set_verified_from(WAL, wal_header_len());
    f
}

/// The length of the WAL header, taken from the log itself rather than hardcoded: a brand-new log is
/// exactly its header, so this cannot drift if `HEADER_SIZE` changes. It is a *model parameter*, not
/// an expected value, which is why deriving it from the subject is the right thing to do here.
fn wal_header_len() -> u64 {
    let f = SimFabric::clean(Durability::WriteThrough);
    let _ = WalManager::with_storage(f.open(WAL), WAL.into()).expect("create a log");
    let len = f.durable_image().get(WAL).map(|v| v.len() as u64).unwrap_or(0);
    assert!(len >= 8 && len < PAGE_SIZE as u64, "a fresh WAL is {len} bytes, which is not a header");
    len
}

struct Db {
    bp: Arc<BufferPoolManager>,
    wal: Arc<WalManager>,
    txn: Arc<TxnManager>,
}

/// Open the database and its log on one fabric, exactly as `run_cli` does on real files.
fn open(fabric: &Arc<SimFabric>) -> Result<Db, FerroError> {
    let dm = Arc::new(DiskManager::with_storage(fabric.open(DB))?);
    let bp = Arc::new(BufferPoolManager::new(dm));
    let wal = Arc::new(WalManager::with_storage(fabric.open(WAL), WAL.into())?);
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Ok(Db { bp, wal, txn })
}

struct Written {
    dir_root: u32,
    committed_txn: u64,
}

/// Rows a fully-successful run leaves behind: both committed transactions, and neither loser row.
fn all_committed_rows() -> Vec<Vec<u8>> {
    (0..ROWS)
        .map(committed_payload)
        .chain((0..ROWS2).map(committed2_payload))
        .collect()
}

/// The workload the crash lands in the middle of.
///
/// Every step can fail, because after the fault fires every operation on the fabric returns an
/// error — that is what "the process is gone" means. So this returns `Result` and the caller does not
/// care where it stopped.
fn workload(fabric: &Arc<SimFabric>) -> Result<Written, FerroError> {
    workload_inner(fabric, true)
}

/// `do_checkpoint == false` keeps the log intact, which is the only way `commit_is_durable` can ever
/// answer yes for the first transaction — a checkpoint throws the log away and starts it again. Used
/// to force that detector to fire; see `the_durable_commit_detector_fires_and_stays_quiet`.
fn workload_inner(fabric: &Arc<SimFabric>, do_checkpoint: bool) -> Result<Written, FerroError> {
    let db = open(fabric)?;

    let t1 = db.txn.begin()?;
    let mut heap = HeapFileManager::new(db.bp.clone())?;
    let dir_root = heap.first_directory_page_id;
    heap.set_transaction(db.txn.clone(), t1);
    for i in 0..ROWS {
        heap.insert(Tuple::new(committed_payload(i)))?;
    }
    db.txn.commit(t1)?;

    // The durability discipline `TxnManager::checkpoint` uses: pages out, then made durable. Without
    // the sync every page write is still in a volatile cache under `Durability::SyncOnly`.
    db.bp.flush_all()?;
    db.bp.disk_manager.sync()?;

    // A second commit followed by a real checkpoint, which is where the log is thrown away and
    // restarted: `set_len`, `sync_data` and `sync_all` on the WAL, in that order. Those are the
    // operations `interrupted_truncation_self_heals_on_open` stages by hand, one point at a time;
    // here they are inside the swept range like everything else.
    let t2 = db.txn.begin()?;
    heap.set_transaction(db.txn.clone(), t2);
    for i in 0..ROWS2 {
        heap.insert(Tuple::new(committed2_payload(i)))?;
    }
    db.txn.commit(t2)?;
    if do_checkpoint {
        db.txn.checkpoint()?;
    }

    // Five losers, interleaved in the same pages. Undo has to run for every one of them at recovery
    // and remove every one of these rows, and the ORDER it runs them in is what the sort in `recover`
    // fixes. See `undo_compensates_the_losers_in_ascending_transaction_id_order`.
    for t in 0..LOSER_TXNS {
        let loser = db.txn.begin()?;
        heap.set_transaction(db.txn.clone(), loser);
        for i in 0..LOSER_ROWS {
            heap.insert(Tuple::new(loser_payload(t, i)))?;
        }
        db.wal.flush()?;
    }

    Ok(Written { dir_root, committed_txn: t1 })
}

/// Is there a durable `Commit` for `txn_id` in the log as it stands? Asked **before** `recover`,
/// because recovery appends its own records.
fn commit_is_durable(wal: &WalManager, txn_id: u64) -> bool {
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    while lsn < end {
        match wal.read_record(lsn) {
            Ok((rec, next)) => {
                if rec.txn_id == txn_id && matches!(rec.kind, RecKind::Commit) {
                    return true;
                }
                if next <= lsn {
                    break;
                }
                lsn = next;
            }
            Err(_) => break,
        }
    }
    false
}

/// What the database looked like after the reboot.
#[derive(Debug)]
struct AfterRecovery {
    /// `Err` when reopening or recovering refused. A refusal is an acceptable outcome; a wrong
    /// database is not — and it is only acceptable under the conditions invariant 6 names.
    outcome: Result<Vec<Vec<u8>>, String>,
    commit_durable: bool,
    /// Could the surviving database file even contain the table's directory page? When the crash
    /// landed before anything about the file was durable, the answer is no and the honest report is
    /// "there is no such table" — which arrives as an error from the scan.
    table_root_could_exist: bool,
}

fn reboot_and_recover(surviving: &Arc<SimFabric>, dir_root: u32) -> AfterRecovery {
    // Measured from the surviving bytes, before anything reopens and extends the file.
    let db_bytes = surviving.durable_image().get(DB).map(|v| v.len() as u64).unwrap_or(0);
    let table_root_could_exist = db_bytes >= (dir_root as u64 + 1) * PAGE_SIZE as u64;
    let db = match open(surviving) {
        Ok(d) => d,
        Err(e) => {
            return AfterRecovery {
                outcome: Err(format!("open: {e:?}")),
                commit_durable: false,
                table_root_could_exist,
            };
        }
    };
    // Asked before `recover`, which appends records of its own.
    let commit_durable = commit_is_durable(&db.wal, first_txn_id());
    let outcome = recover(&db.txn)
        .map_err(|e| format!("recover: {e:?}"))
        .and_then(|recovered| {
            // What `run_cli` does when recovery ran: a checkpoint, which is what pushes the pages
            // recovery rebuilt in memory out to the disk. Without it the reboot leaves everything
            // recovery did in a buffer pool that is about to be dropped, and a second reboot would
            // have to redo it all over again from the log.
            if recovered {
                db.txn.checkpoint().map_err(|e| format!("checkpoint after recovery: {e:?}"))?;
            }
            Ok(())
        })
        .and_then(|()| {
            let heap = HeapFileManager::open(dir_root, db.bp.clone());
            heap.scan()
                .collect::<Result<Vec<_>, _>>()
                .map(|rows| rows.into_iter().map(|(_, t)| t.data).collect())
                .map_err(|e| format!("scan: {e:?}"))
        });
    AfterRecovery { outcome, commit_durable, table_root_could_exist }
}

/// The first transaction id this engine hands out. `transaction_ids_never_start_at_zero` pins it at
/// 1, and the workload's committed transaction is the first one begun, so it is that.
fn first_txn_id() -> u64 {
    1
}

/// Every invariant, checked against one recovered database. Returns a one-line description of which
/// branch was taken, so the sweep can prove it saw more than one.
fn check_invariants(after: &AfterRecovery, what: &str) -> &'static str {
    let rows = match &after.outcome {
        Ok(r) => r,
        Err(e) => {
            // 6. **A refusal is allowed only where there was nothing to recover.**
            //
            // Recovery that cannot make sense of what survived must say so rather than open a
            // database it has half understood. But "it refused" is also how a lost-data bug would
            // look from the outside, so the licence is narrow: no `Commit` record for the
            // transaction survived, AND the surviving file is too short to hold the table's own
            // directory page — the database was never created. Anything else is an acknowledged
            // commit that recovery cannot open, which is data loss with an error message on it.
            assert!(
                !e.is_empty(),
                "{what}: recovery refused with an empty message, which tells an operator nothing"
            );
            assert!(
                !after.commit_durable,
                "{what}: recovery REFUSED, and the Commit record for transaction 1 is durable in \
                 the log — an acknowledged commit that cannot be reopened. {e}"
            );
            assert!(
                !after.table_root_could_exist,
                "{what}: recovery refused even though the surviving file is long enough to hold \
                 the table's directory page, so there was something there to recover. {e}"
            );
            return "refused";
        }
    };

    let expected: BTreeSet<Vec<u8>> = all_committed_rows().into_iter().collect();
    let losers = all_loser_rows();
    let first_txn: BTreeSet<Vec<u8>> = (0..ROWS).map(committed_payload).collect();

    // 1. No row from either transaction that never committed. Undo must have run, for both.
    for r in rows {
        assert!(
            !losers.contains(r),
            "{what}: a row from one of the uncommitted transactions survived recovery: {r:?}"
        );
    }

    // 2. Every surviving row is one that was actually written, whole. A torn write that spliced half
    //    of one row onto half of another lands here, because the payloads differ in length AND content.
    for r in rows {
        assert!(
            expected.contains(r),
            "{what}: recovered a row that was never written: len {} {:?}",
            r.len(),
            &r[..r.len().min(8)]
        );
    }

    // 3. No duplicates. Redo is idempotent by page LSN; a redo applied twice would show up here.
    let unique: BTreeSet<&Vec<u8>> = rows.iter().collect();
    assert_eq!(
        unique.len(),
        rows.len(),
        "{what}: a row was recovered twice ({} rows, {} distinct)",
        rows.len(),
        unique.len()
    );

    // 4. **All or nothing, per transaction.** This is the breaking shape: anything from 1 to
    //    ROWS - 1 rows of the first transaction's ROWS is the failure two silent data-loss bugs in
    //    this repository would have produced, and it could not be seen while every generator
    //    committed a single row. Written against the constants rather than spelled out, because the
    //    two numbers that were spelled out here both went stale as the workload grew.
    let from_first = rows.iter().filter(|r| first_txn.contains(*r)).count();
    assert!(
        from_first == 0 || from_first == ROWS,
        "{what}: a {ROWS}-row commit came back in pieces — {from_first} rows survived"
    );
    let from_second = rows.len() - from_first;
    assert!(
        from_second == 0 || from_second == ROWS2,
        "{what}: the second commit came back in pieces — {from_second} of {ROWS2} rows survived"
    );
    // The second transaction commits after the first, so the first cannot be the one that vanished.
    assert!(
        !(from_second > 0 && from_first == 0),
        "{what}: the later commit survived and the earlier one did not, which no crash can produce \
         from a log that is replayed in order"
    );

    // 5. A durable commit record means the rows are there. This is the durability promise itself:
    //    the commit returned to the caller only after its log records were flushed.
    if after.commit_durable {
        assert_eq!(
            from_first, ROWS,
            "{what}: the commit record for transaction 1 is durable in the log, but {from_first} of \
             {ROWS} rows came back — a commit that was acknowledged and then lost"
        );
    }

    match (from_first, from_second) {
        (0, 0) => "empty",
        (f, s) if f == ROWS && s == ROWS2 => "all rows",
        _ => "first only",
    }
}

// ---------------------------------------------------------------------------------------------
// Exit criterion 1: the same seed reproduces the same byte sequence and the same fault point.
// ---------------------------------------------------------------------------------------------

/// **Breaking shape: a write order that is a function of anything but the data.**
///
/// `flush_all` iterated a `HashMap` and `recover` a `HashSet`, so the same workload wrote the same
/// pages in a different order on every run of the same binary. Everything below — replay, bisection,
/// comparing a faulted run against a clean one — needs the sequence to be reproducible, and it is
/// this assertion that says so. Revert either sort and this test fails on the trace comparison.
#[test]
fn the_same_seed_reproduces_the_same_byte_sequence_and_the_same_fault_point() {
    let census = fabric(None, Durability::WriteThrough);
    workload(&census).expect("the fault-free workload must succeed");
    let ops = census.op_count();
    println!(
        "census: {ops} operations, {} faultable",
        census.faultable_ops().len()
    );
    assert!(
        ops >= 80,
        "only {ops} operations recorded; a sequence this short is not evidence of anything"
    );

    let seed = 0x0B10_5EED_0000_0001;
    let plan = FaultPlan::from_seed(seed, ops);

    let a = fabric(Some(plan), Durability::WriteThrough);
    let _ = workload(&a);
    let b = fabric(Some(plan), Durability::WriteThrough);
    let _ = workload(&b);

    // Anti-vacuity: a fault really did fire, so the comparison is over a faulted run.
    let fired_a = a.fired().expect("the plan named a faultable operation but nothing fired");
    let fired_b = b.fired().expect("second run fired nothing");
    assert_eq!(fired_a, fired_b, "the same seed crashed in two different places");

    // The byte sequence: what was written, where, in what order.
    assert_eq!(a.trace_digest(), b.trace_digest(), "same seed, different byte sequence");
    assert_eq!(a.trace(), b.trace(), "same seed, different operation trace");
    assert_eq!(a.image_digest(), b.image_digest(), "same seed, different surviving image");

    // Anti-vacuity for the digest itself: it distinguishes runs that really differ. A digest that
    // returned a constant would satisfy every equality above.
    assert_ne!(
        a.trace_digest(),
        census.trace_digest(),
        "a crashed run and a clean run share a trace digest, so the digest distinguishes nothing"
    );
    assert_ne!(
        a.image_digest(),
        census.image_digest(),
        "a crashed run and a clean run share an image digest"
    );

    // And the clean run is reproducible too, which is the part the sorts in `flush_all` and
    // `recover` are responsible for.
    let census2 = fabric(None, Durability::WriteThrough);
    workload(&census2).expect("second census must succeed");
    assert_eq!(
        census.trace(),
        census2.trace(),
        "two fault-free runs of one workload wrote different sequences, so nothing here is replayable"
    );
    assert_eq!(census.image_digest(), census2.image_digest(), "same workload, different image");
}

/// Recovery is a sequence of durable writes too — the one most likely to be interrupted, since it
/// only runs after something has already gone wrong. It has to be replayable for the same reason
/// everything else here does.
///
/// **Breaking shape: a crash that leaves more than one page to rebuild.** `recover`'s `touched` set is
/// a `HashSet` and its `losers` came straight from `last_lsn.keys()`, a `HashMap`; both loops write.
/// With one page and one loser the order is a single element and nothing can differ, which is why this
/// test hunts for an image whose recovery issues several writes and refuses to run on one that does
/// not. The same surviving bytes are then recovered three times, in three separate buffer pools with
/// three separately-seeded hash maps.
#[test]
fn recovery_itself_writes_the_same_sequence_three_times() {
    // `SyncOnly` is what leaves work for recovery to do: page writes sit in a volatile cache, so a
    // crash before the checkpoint's fsync means recovery has to rebuild them from the log.
    let census = fabric(None, Durability::SyncOnly);
    let base = workload(&census).expect("census");

    // **Score every crash point and take the best, rather than the first that writes enough pages.**
    //
    // Taking the first hit was a mistake worth recording: the earliest points leave an empty database
    // file, so recovery writes plenty of pages — but they are also *before* the uncommitted
    // transactions exist, so recovery has nothing to undo and the loser ORDER cannot differ. Measured:
    // reverting `recover`'s `losers` sort survived this test 5 times out of 5 while it picked such a
    // point. The image this test needs is one where recovery both rebuilds pages AND compensates
    // several transactions.
    let mut best: Option<(usize, usize, BTreeMap<String, Vec<u8>>)> = None;
    for &n in census.faultable_ops().iter() {
        let live = fabric(Some(FaultPlan::at(n, 0x0B10_5EED)), Durability::SyncOnly);
        let _ = workload(&live);
        let image = live.durable_image();

        let probe = SimFabric::from_images(image.clone(), None, Durability::SyncOnly);
        probe.set_write_atomicity(DB, PAGE_SIZE as u64);
        let Ok(db) = open(&probe) else { continue };
        let first_new_lsn = db.wal.next_lsn.load(Ordering::SeqCst);
        let before = probe.op_count();
        if recover(&db.txn).is_err() {
            continue;
        }
        let undone = compensated_transactions(&db.wal, first_new_lsn).len();
        let writes = probe
            .trace()
            .into_iter()
            .filter(|t| t.index >= before && t.kind == OpKind::Pwrite)
            .count();
        if best.as_ref().is_none_or(|(u, w, _)| (undone, writes) > (*u, *w)) {
            best = Some((undone, writes, image));
        }
    }
    let (undone, _, image) = best.expect("no crash point produced a recoverable image at all");
    assert!(
        undone >= 2,
        "the best crash point left recovery only {undone} transaction(s) to compensate; with fewer \
         than two there is no undo order to get wrong and this test cannot see one"
    );

    let mut sequences = Vec::new();
    let mut db_write_counts = Vec::new();
    for _ in 0..3 {
        let rebooted = SimFabric::from_images(image.clone(), None, Durability::SyncOnly);
        rebooted.set_write_atomicity(DB, PAGE_SIZE as u64);
        let before = rebooted.op_count();
        let after = reboot_and_recover(&rebooted, base.dir_root);
        assert!(after.outcome.is_ok(), "recovery of a fixed image failed: {:?}", after.outcome);
        check_invariants(&after, "fixed image");
        // Every operation, both files. Filtering to the database file alone would miss the undo
        // ordering entirely: the losers' compensation records go to the WAL, and it is the WAL bytes
        // that differ when the loser order does.
        let seq: Vec<(String, OpKind, u64, usize, u32)> = rebooted
            .trace()
            .into_iter()
            .filter(|t| t.index >= before)
            .map(|t| (t.file, t.kind, t.offset, t.len, t.data_crc))
            .collect();
        db_write_counts
            .push(seq.iter().filter(|(f, k, ..)| f == DB && *k == OpKind::Pwrite).count());
        sequences.push(seq);
    }
    assert!(
        db_write_counts[0] >= 3,
        "recovery wrote only {} page(s), so a page ordering bug could not show either: {db_write_counts:?}",
        db_write_counts[0]
    );
    assert_eq!(sequences[0], sequences[1], "recovery wrote a different sequence on run 2");
    assert_eq!(sequences[1], sequences[2], "recovery wrote a different sequence on run 3");
}

/// The transactions recovery compensated, in the order its records appear in the log, starting at
/// `from_lsn`. Consecutive records for one transaction collapse to a single entry, so the result is
/// the order undo ran them in.
fn compensated_transactions(wal: &WalManager, from_lsn: u64) -> Vec<u64> {
    let mut groups: Vec<u64> = Vec::new();
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = from_lsn;
    while lsn < end {
        let Ok((rec, next)) = wal.read_record(lsn) else { break };
        if matches!(rec.kind, RecKind::Clr { .. } | RecKind::Abort) && groups.last() != Some(&rec.txn_id) {
            groups.push(rec.txn_id);
        }
        if next <= lsn {
            break;
        }
        lsn = next;
    }
    groups
}

// ---------------------------------------------------------------------------------------------
// Exit criterion 2: two different seeds produce different fault points.
// ---------------------------------------------------------------------------------------------

/// **Proves the seed is read.**
///
/// Breaking shape: a `SimStorage` that ignores its seed. Every determinism assertion above would
/// still pass — identical runs are trivially identical — so "reproducible" would mean nothing more
/// than "always crashes in the same one place", and a sweep over one point is not a sweep.
#[test]
fn two_different_seeds_crash_in_different_places() {
    let census = fabric(None, Durability::WriteThrough);
    workload(&census).expect("census");
    let ops = census.op_count();

    let mut points: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    let mut kinds: BTreeSet<FaultKind> = BTreeSet::new();
    for seed in 1u64..=24 {
        let plan = FaultPlan::from_seed(seed, ops);
        let f = fabric(Some(plan), Durability::WriteThrough);
        let _ = workload(&f);
        let fired = f.fired().unwrap_or_else(|| panic!("seed {seed} fired nothing"));
        points.entry(fired.op_index).or_default().push(seed);
        kinds.insert(fired.kind);
    }

    // Two claims, because they can fail separately. First: the *plan* the seed produces is almost
    // always a different operation — this is the seed being read at all, independent of what the
    // workload does with it.
    let planned: BTreeSet<u64> =
        (1u64..=24).map(|seed| FaultPlan::from_seed(seed, ops).at_op).collect();
    assert!(
        planned.len() >= 20,
        "24 seeds planned only {} distinct operations out of {ops}; the seed is barely being read",
        planned.len()
    );
    // Second: those plans really did break different operations. Fewer than `planned`, because a
    // plan aimed at a read fires at the next write instead.
    assert!(
        points.len() >= 12,
        "24 seeds produced only {} distinct crash points; the seed is barely being read: {:?}",
        points.len(),
        points
    );
    assert!(
        kinds.len() >= 2,
        "every seed chose the same kind of break ({kinds:?}), so the seed does not pick the shape"
    );

    // The other half of the claim, and the reason this is not just noise: one seed, twice, is the
    // same point. "Different seeds differ" would otherwise be satisfied by a random crash point.
    let plan = FaultPlan::from_seed(7, ops);
    let x = fabric(Some(plan), Durability::WriteThrough);
    let _ = workload(&x);
    let y = fabric(Some(plan), Durability::WriteThrough);
    let _ = workload(&y);
    assert_eq!(x.fired(), y.fired(), "seed 7 crashed in two different places");
}

// ---------------------------------------------------------------------------------------------
// Exit criterion 3: recovery holds for every injected fault point across the swept range.
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct Tally {
    all_rows: usize,
    /// Points at which the first transaction's `Commit` record survived, so invariant 5 — an
    /// acknowledged commit must still be there — was actually evaluated rather than skipped.
    commit_durable: usize,
    /// Every refusal, with the operation and the reason. A refusal is an acceptable outcome and an
    /// unexplained one is not, so they are carried out of the sweep rather than counted and dropped.
    /// Kept out of the `Debug` line because a hundred identical strings buries the counts.
    refusals: Vec<(u64, FaultKind, String)>,
    /// The first transaction's rows survived and the second's did not — the crash landed between the
    /// two commits.
    first_only: usize,
    empty: usize,
    refused: usize,
    torn: usize,
    corrupted: usize,
    dropped: usize,
    failed_sync: usize,
    dropped_set_len: usize,
}

fn sweep(durability: Durability, seed: u64) -> Tally {
    let census = fabric(None, durability);
    let base = workload(&census).expect("the fault-free workload must succeed");
    let points = census.faultable_ops();
    println!("sweeping {} faultable operations ({durability:?})", points.len());
    assert!(
        points.len() >= 50,
        "only {} faultable operations; that is not a sweep",
        points.len()
    );

    let mut tally = Tally::default();
    // **Every shape at every point**, not whichever one the seed happened to pick. With a single
    // shape per point a 60-point sweep produced 50 dropped writes and zero torn ones, so the fault
    // kind that most needs testing — a write that landed halfway — was absent from a sweep that
    // reported success. `Corrupt` is the third because a tear and a drop both leave a *short* record,
    // which a bounds check catches on its own: with only those two, removing every CRC check from the
    // WAL read path left this sweep green.
    //
    // **The run count is not the coverage number.** A `Tear` or a `Corrupt` aimed at the database
    // file degrades to a `Drop`, because that file is page-atomic in this model — so three runs at
    // one database write stage one fault, not three. Measured: 68 faultable operations x 3 shapes =
    // 204 runs per durability model, staging **86 distinct faults**. The duplicate runs are kept
    // because they cost microseconds and keep the loop uniform, but a claim about what this sweep
    // covers is a claim about the 86.
    for &n in &points {
        for shape in [WriteShape::Drop, WriteShape::Tear, WriteShape::Corrupt] {
            let plan = FaultPlan::at_shaped(n, seed, shape);
            let live = fabric(Some(plan), durability);
            let ran = std::panic::catch_unwind(AssertUnwindSafe(|| workload(&live)));
            assert!(
                ran.is_ok(),
                "the workload PANICKED with a fault at operation {n} ({:?}). A durable-IO error \
                 must come back as an error, not as a process that stops existing.",
                live.fired()
            );
            let fired = live.fired().unwrap_or_else(|| {
                panic!("operation {n} was listed as faultable but nothing fired")
            });
            // A durable-IO fault must reach the caller. Every operation in the workload is behind a
            // `?`, so a run that reports success while the fabric refused an operation means someone
            // dropped an error on the floor — which is how an acknowledged commit loses data.
            if let Ok(Ok(_)) = &ran {
                panic!(
                    "the workload reported SUCCESS with a fault at operation {n} ({fired:?}); a \
                     durable-IO error was swallowed"
                );
            }
            match fired.kind {
                FaultKind::TearWrite => {
                    tally.torn += 1;
                    assert!(
                        fired.kept > 0 && fired.kept < fired.len,
                        "a tear at operation {n} kept {} of {} bytes, which is not a tear",
                        fired.kept,
                        fired.len
                    );
                }
                FaultKind::CorruptWrite => {
                    tally.corrupted += 1;
                    assert!(
                        fired.kept < fired.len,
                        "a garbled write at operation {n} claims to have altered byte {} of {}",
                        fired.kept,
                        fired.len
                    );
                }
                FaultKind::DropWrite => tally.dropped += 1,
                FaultKind::FailSync => tally.failed_sync += 1,
                FaultKind::DropSetLen => tally.dropped_set_len += 1,
            }

            // The machine reboots with only what survived, and nothing faults from here on.
            let rebooted = live.restart();
            let what = format!("fault at op {n} ({shape:?}): {fired:?}");
            let after = match std::panic::catch_unwind(AssertUnwindSafe(|| {
                reboot_and_recover(&rebooted, base.dir_root)
            })) {
                Ok(a) => a,
                Err(_) => panic!("recovery PANICKED after {what}"),
            };
            if after.commit_durable {
                tally.commit_durable += 1;
            }
            match check_invariants(&after, &what) {
                "all rows" => tally.all_rows += 1,
                "first only" => tally.first_only += 1,
                "empty" => tally.empty += 1,
                _ => {
                    tally.refused += 1;
                    if let Err(e) = &after.outcome {
                        tally.refusals.push((n, fired.kind, e.clone()));
                    }
                }
            }

            // **Recovery must be idempotent**, because a crash during recovery means it runs again
            // at the next boot. Compared row by row, not by count: two databases with the same
            // number of different rows is the failure a length check cannot see.
            if after.outcome.is_ok() {
                let twice = reboot_and_recover(&rebooted, base.dir_root);
                check_invariants(&twice, &format!("{what}, recovered twice"));
                assert_eq!(
                    after.outcome.as_ref().ok(),
                    twice.outcome.as_ref().ok(),
                    "recovery run twice gave two different databases after {what}"
                );
            }
        }
    }

    let mut by_reason: BTreeMap<(FaultKind, String), Vec<u64>> = BTreeMap::new();
    for (n, kind, why) in &tally.refusals {
        by_reason.entry((*kind, why.clone())).or_default().push(*n);
    }
    for ((kind, why), ops) in &by_reason {
        println!("  refused ({kind:?}) at ops {ops:?}: {why}");
    }
    // No ceiling on the refusal count, deliberately. A ceiling would be an arbitrary number
    // standing in for the real property, and the real property is checked at every point by
    // invariant 6: a refusal is only licensed where no commit was durable AND the file was too
    // short to hold the table at all. Refusing everything is impossible under that rule without
    // also failing `tally.all_rows > 0` below.
    assert!(tally.torn > 0, "no write was ever torn: {tally:?}");
    assert!(
        tally.corrupted > 0,
        "no write was ever garbled, so nothing here needed a checksum to catch it: {tally:?}"
    );
    assert!(tally.dropped > 0, "no write was ever dropped: {tally:?}");
    assert!(tally.failed_sync > 0, "no sync ever failed: {tally:?}");
    assert!(
        tally.commit_durable > 0,
        "at no fault point did a Commit record survive, so invariant 5 — an acknowledged commit is \
         still there after the crash — was never evaluated: {tally:?}"
    );
    assert!(
        tally.all_rows > 0,
        "not one fault point recovered the committed rows, so 'all or nothing' was only ever \
         checked against nothing: {tally:?}"
    );
    assert!(
        tally.empty + tally.refused + tally.first_only > 0,
        "every fault point recovered everything, so the fault may not be reaching durable state: \
         {tally:?}"
    );
    tally
}

/// The sweep. Every faultable operation in the workload, one at a time, then reboot and recover.
///
/// `WriteThrough`: a completed write is durable at once, so the injected fault is the only thing the
/// crash lost. That is what makes a failure at operation N attributable to the fault at N.
///
/// Breaking shape: any commit whose rows come back in pieces, any row from the transaction that never
/// committed, any duplicate, any panic out of a durable-IO error, any acknowledged commit that
/// vanished. All five are checked at every point.
#[test]
fn recovery_holds_at_every_fault_point_write_through() {
    let tally = sweep(Durability::WriteThrough, 0x0B10_5EED);
    println!("write-through sweep: {tally:?}");
}

/// The same sweep against the harsher model: writes sit in a volatile cache and a crash loses
/// everything not flushed. Strictly more is lost at every point than under `WriteThrough`.
///
/// Breaking shape: a crash between a commit and the next checkpoint's fsync. The rows are in the log
/// and nowhere else, so recovery has to rebuild every page of them from the log — including pages the
/// file never grew far enough to contain. Under `WriteThrough` that path is barely taken, because the
/// page writes are already durable.
///
/// This one is not redundant. It is the model in which `checkpoint`'s ordering — flush the log, write
/// the pages, **fsync the pages**, only then discard the log — is the difference between a recoverable
/// database and a lost one. Under `WriteThrough` that fsync is decoration.
#[test]
fn recovery_holds_at_every_fault_point_sync_only() {
    let tally = sweep(Durability::SyncOnly, 0x0B10_5EED);
    println!("sync-only sweep: {tally:?}");
}

// ---------------------------------------------------------------------------------------------
// Build item 3, asserted directly: deterministic write ordering.
// ---------------------------------------------------------------------------------------------

/// **Breaking shape: two dirty pages.** `flush_all` iterated `page_table`, a `HashMap` whose order is
/// seeded per instance, so with two or more dirty pages the offsets it wrote came out in a different
/// order on every run. One dirty page cannot show it — which is why this test insists on at least
/// four, and says so rather than assuming.
#[test]
fn flush_all_writes_pages_in_ascending_page_id_order() {
    let mut sequences = Vec::new();
    for _ in 0..4 {
        let f = fabric(None, Durability::WriteThrough);
        let db = open(&f).expect("open");
        let t = db.txn.begin().expect("begin");
        let mut heap = HeapFileManager::new(db.bp.clone()).expect("heap");
        heap.set_transaction(db.txn.clone(), t);
        // Enough rows to spill across several heap pages, so there is an order to get wrong.
        for i in 0..400 {
            heap.insert(Tuple::new(vec![(i % 251) as u8; 60])).expect("insert");
        }
        db.txn.commit(t).expect("commit");

        let before = f.op_count();
        db.bp.flush_all().expect("flush_all");
        let after = f.op_count();

        let offsets: Vec<u64> = f
            .trace()
            .into_iter()
            .filter(|o| o.index >= before && o.index < after && o.file == DB && o.kind == OpKind::Pwrite)
            .map(|o| o.offset)
            .collect();

        assert!(
            offsets.len() >= 4,
            "flush_all wrote only {} pages; with fewer than 4 dirty pages an ordering bug cannot \
             show, so this assertion would be vacuous",
            offsets.len()
        );
        for w in offsets.windows(2) {
            assert!(
                w[0] < w[1],
                "flush_all wrote offset {} before {}, so page order is not ascending: {offsets:?}",
                w[0],
                w[1]
            );
        }
        for o in &offsets {
            assert_eq!(o % PAGE_SIZE as u64, 0, "a page write at a non-page offset {o}");
        }
        sequences.push(offsets);
    }
    // Four separate `BufferPoolManager`s, so four separately-seeded `HashMap`s. Identical order
    // across all four is what a sort gives and hash iteration does not.
    for (i, s) in sequences.iter().enumerate().skip(1) {
        assert_eq!(
            &sequences[0], s,
            "run {i} flushed the same pages in a different order from run 0"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// The fabric's own guards. A harness nobody has checked is not evidence.
// ---------------------------------------------------------------------------------------------

/// A torn write must land a *prefix* and nothing more, and the run must be over.
///
/// Breaking shape: a fabric that keeps serving operations after the fault. It would be simulating a
/// disk that dropped one write and carried on — a bad disk, not a dead process — and every later
/// write in the workload would reach the image, so the surviving bytes would be a state no crash can
/// produce. Every "recovery held" result would then be about a database that never existed.
#[test]
fn a_torn_write_lands_a_prefix_and_ends_the_run() {
    let census = fabric(None, Durability::WriteThrough);
    workload(&census).expect("census");
    let points = census.faultable_ops();

    // Find a seed/point pair that actually tears rather than drops.
    let mut found = None;
    'outer: for &n in &points {
        for seed in 0u64..8 {
            let plan = FaultPlan::at(n, seed);
            let f = fabric(Some(plan), Durability::WriteThrough);
            let _ = workload(&f);
            if let Some(fired) = f.fired() {
                if fired.kind == FaultKind::TearWrite {
                    found = Some((f, fired));
                    break 'outer;
                }
            }
        }
    }
    let (f, fired) = found.expect("no seed produced a torn write at any point");
    assert!(fired.kept > 0, "a tear that kept nothing is a drop");
    assert!(fired.kept < fired.len, "a tear that kept everything is not a tear");
    assert!(f.crashed(), "the fabric kept running after a tear");

    // Nothing reached the image after the fault: every later operation is refused.
    let trace = f.trace();
    let after_fault: Vec<&TraceOp> =
        trace.iter().filter(|o| o.index > fired.op_index && o.ok).collect();
    assert!(
        after_fault.is_empty(),
        "{} operations succeeded after the crash: {:?}",
        after_fault.len(),
        &after_fault[..after_fault.len().min(3)]
    );
}

/// Anti-vacuity for the whole file: with **no** fault the workload completes, recovery finds every
/// row, and nothing was refused.
///
/// Breaking shape: a workload or a fabric that fails for a reason unrelated to any injected fault. If
/// this failed, every "recovery held" above would be the harness declining to do anything and calling
/// the refusal an acceptable outcome.
#[test]
fn with_no_fault_the_workload_completes_and_every_row_comes_back() {
    let f = fabric(None, Durability::WriteThrough);
    let w = workload(&f).expect("a fault-free workload must succeed");
    assert!(f.fired().is_none(), "a clean fabric injected a fault");
    assert!(!f.crashed(), "a clean fabric crashed");
    assert!(f.trace().iter().all(|o| o.ok), "a clean fabric refused an operation");

    let rebooted = f.restart();
    let after = reboot_and_recover(&rebooted, w.dir_root);
    let rows = after.outcome.as_ref().expect("clean recovery must succeed");
    assert_eq!(
        rows.len(),
        ROWS + ROWS2,
        "clean recovery came back with {} of {} rows",
        rows.len(),
        ROWS + ROWS2
    );
    // The workload ends with a checkpoint, and a checkpoint in this engine does not trim the log —
    // it throws it away and restarts it at the current end. So the commit record is legitimately
    // gone while every row it committed is on disk, which is exactly why invariant 5 is stated one
    // way round: a durable commit implies the rows, not the reverse.
    assert!(
        !after.commit_durable,
        "the commit record survived a checkpoint, so this engine no longer discards the log there \
         and `the_durable_commit_detector_fires_and_stays_quiet` needs rewriting"
    );
    assert_eq!(check_invariants(&after, "no fault"), "all rows");
    assert_eq!(w.committed_txn, first_txn_id(), "the committed transaction is not txn 1");
}

/// `SyncOnly` must be the harsher model, not a differently-worded `WriteThrough`. Forced on purpose:
/// write without syncing, and the bytes must not be in the surviving image.
///
/// Breaking shape: a `Durability` enum that both arms treat the same. The `SyncOnly` sweep would then
/// be a second copy of the `WriteThrough` one, reported as twice the coverage.
#[test]
fn under_sync_only_an_unflushed_write_does_not_survive() {
    let f = fabric(None, Durability::SyncOnly);
    let s = f.open("probe");
    use ferrodb::storage::storage::Storage;
    s.pwrite(&[7u8; 32], 0).expect("write");
    assert_eq!(
        f.durable_image().get("probe").map(|v| v.len()),
        Some(0),
        "an unflushed write survived under SyncOnly"
    );
    s.sync_data().expect("sync");
    assert_eq!(
        f.durable_image().get("probe").map(|v| v.as_slice()),
        Some(&[7u8; 32][..]),
        "a flushed write did not survive under SyncOnly"
    );

    // The other half: WriteThrough keeps it without a sync, so the two models really differ.
    let g = fabric(None, Durability::WriteThrough);
    let t = g.open("probe");
    t.pwrite(&[7u8; 32], 0).expect("write");
    assert_eq!(
        g.durable_image().get("probe").map(|v| v.len()),
        Some(32),
        "WriteThrough lost a completed write"
    );
}

/// **Forcing the durable-commit detector to fire, and then to stay quiet.**
///
/// `commit_is_durable` is what makes invariant 5 — an acknowledged commit is still there after the
/// crash — anything other than a skipped branch. A detector that has never been seen to fire is not
/// evidence, so this makes it fire on purpose (a run with no checkpoint keeps the log, and the commit
/// record is found) and then makes it stay quiet on purpose (a checkpoint throws the log away, so the
/// record is gone even though every row it committed is on disk).
#[test]
fn the_durable_commit_detector_fires_and_stays_quiet() {
    // Fires: the log still holds the commit.
    let kept = fabric(None, Durability::WriteThrough);
    let w = workload_inner(&kept, false).expect("workload without a checkpoint");
    let rebooted = kept.restart();
    let db = open(&rebooted).expect("reopen");
    assert!(
        commit_is_durable(&db.wal, w.committed_txn),
        "the commit record is missing from a log that was never truncated, so the detector cannot \
         fire at all and invariant 5 is dead code"
    );

    // Quiet: the checkpoint discarded the log, and the rows are still there.
    let ckpt = fabric(None, Durability::WriteThrough);
    let w2 = workload(&ckpt).expect("workload with a checkpoint");
    let rebooted2 = ckpt.restart();
    let after = reboot_and_recover(&rebooted2, w2.dir_root);
    assert!(
        !after.commit_durable,
        "the commit record survived a checkpoint that is supposed to discard the whole log"
    );
    assert_eq!(
        after.outcome.as_ref().map(|r| r.len()).unwrap_or(0),
        ROWS + ROWS2,
        "the rows did not survive the checkpoint that discarded their commit records"
    );
}


impl std::fmt::Debug for Tally {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Tally {{ all_rows: {}, first_only: {}, empty: {}, refused: {}, commit_durable: {}, \
             torn: {}, corrupted: {}, dropped: {}, failed_sync: {}, dropped_set_len: {} }}",
            self.all_rows,
            self.first_only,
            self.empty,
            self.refused,
            self.commit_durable,
            self.torn,
            self.corrupted,
            self.dropped,
            self.failed_sync,
            self.dropped_set_len
        )
    }
}

/// **A known gap, pinned so it cannot be forgotten: a torn table page is served as a row that was
/// never written.**
///
/// The WAL protects itself from a torn write — every frame carries a CRC32 and `scan_valid_end` stops
/// at the first frame that fails it. Ordinary table pages have no such protection.
/// `heap_page::Page` *has* a `checksum` field, in the header, at bytes 19..23, and **nothing in the
/// crate ever computes it**: `serialize` copies whatever is in the struct and `deserialize` reads it
/// back into the struct, and no caller compares the two. (Contrast `cow::page_header::stamp_checksum`
/// and `verify_checksum`, which the branch arena really does apply and really does verify —
/// `ArenaPageStore::read_page` refuses a page that fails.) So a 4 KiB table-page write that lands
/// partially leaves a page whose slot array is new and whose tuple bytes are stale, and a `SELECT`
/// returns that as a row.
///
/// This is why `fabric()` gives the database file page-atomic writes: the engine's durability already
/// rests on that assumption, and sweeping byte-granular tears of table pages would re-derive this one
/// gap at forty different offsets instead of testing recovery. This test is the record of it. It sets
/// the unit back to one byte on purpose.
///
/// **If this test ever fails**, someone has given table pages a real checksum — which is the fix —
/// and this test should become the assertion that the torn page is *refused* instead.
#[test]
fn a_torn_table_page_is_served_as_a_row_that_was_never_written() {
    // Half of the finding, provable on its own with no crash at all: the checksum field is not a
    // checksum. Serialise a page with a nonsense value in it and the nonsense is what lands.
    let mut page = Page::empty(9);
    page.checksum = 0xDEAD_BEEF;
    let bytes = page.serialize().expect("serialize");
    assert_eq!(
        &bytes[19..23],
        &0xDEAD_BEEFu32.to_be_bytes(),
        "the checksum field is computed from the page after all, so the rest of this test is stale"
    );
    assert!(
        Page::deserialize(bytes).is_ok(),
        "a page carrying a checksum that cannot possibly match its contents was accepted — if this \
         now fails, verification has been added and this test should be rewritten"
    );

    // The other half: a torn table page, and a scan that returns a row nobody wrote.
    let census = SimFabric::clean(Durability::WriteThrough); // unit 1: bytes, not pages
    let base = workload(&census).expect("census");
    let known: BTreeSet<Vec<u8>> = all_committed_rows().into_iter().collect();
    let losers = all_loser_rows();

    let mut corrupted: Option<(u64, usize)> = None;
    for &n in census.faultable_ops().iter() {
        let live = SimFabric::with_fault(FaultPlan::at_shaped(n, 0x0B10_5EED, WriteShape::Tear), Durability::WriteThrough);
        let _ = workload(&live);
        let Some(fired) = live.fired() else { continue };
        if fired.kind != FaultKind::TearWrite || fired.file != DB {
            continue;
        }
        let rebooted = live.restart();
        let after = reboot_and_recover(&rebooted, base.dir_root);
        if let Ok(rows) = &after.outcome {
            let bogus = rows
                .iter()
                .filter(|r| !known.contains(*r) && !losers.contains(*r))
                .count();
            if bogus > 0 {
                corrupted = Some((n, bogus));
                break;
            }
        }
    }
    let (op, bogus) = corrupted.expect(
        "no torn table page produced a bogus row. Either table pages are now checksummed — in which \
         case this test should assert the refusal instead — or the tear model stopped tearing table \
         pages, in which case it is no longer testing what it claims.",
    );
    println!(
        "known gap: a table page torn at operation {op} put {bogus} row(s) that were never written \
         into a successful scan"
    );
}

fn three_column_schema() -> Schema {
    Schema::new(vec![
        Column { name: "id".to_string(), data_type: DataType::Integer, nullable: false },
        Column { name: "age".to_string(), data_type: DataType::Integer, nullable: false },
    ])
}

/// **Breaking shape: several tables whose index trees are different sizes.**
///
/// `Catalog::persist` iterated `tables.values()` and `rebuild_indexes` iterated `tables.values_mut()`,
/// both `HashMap`s. Three *empty* tables were not enough to catch the second one, and the reason is
/// worth writing down: `rebuild_indexes` frees each table's old index tree and immediately allocates a
/// new one, and when every tree is a single page the allocator hands the same page straight back —
/// free-then-allocate is order-invariant, so no order is observable. Measured: the reverted sort
/// SURVIVED that version of this test. With row counts of 3, 250, 40, 7, 120 and 1 the trees are
/// different sizes, the page ids each one lands on depend on the order, and the order becomes visible
/// in what is written.
///
/// The catalog half is asserted directly rather than by comparison: the entries on the persisted page
/// must be in ascending name order, which is a property of one run and does not rely on two runs
/// disagreeing.
#[test]
fn the_catalog_and_its_rebuilt_indexes_land_in_the_same_place_every_run() {
    const TABLES: [(&str, usize); 6] =
        [("zulu", 3), ("alpha", 250), ("mike", 40), ("delta", 7), ("papa", 120), ("bravo", 1)];

    let mut traces = Vec::new();
    let mut digests = Vec::new();
    for run in 0..4 {
        let f = fabric(None, Durability::WriteThrough);
        let first_catalog_page = {
            let db = open(&f).expect("open");
            let mut catalog = Catalog::create(db.bp.clone()).expect("catalog");
            // Deliberately not in name order, so a sort has something to do.
            for (name, _) in TABLES {
                catalog
                    .create_table(name.to_string(), three_column_schema())
                    .expect("create table");
            }
            for (name, rows) in TABLES {
                let entry = catalog.require_table(name).expect("table").clone();
                let heap = HeapFileManager::open(entry.first_directory_page_id, db.bp.clone());
                for i in 0..rows {
                    let t = Tuple::serialize(
                        &[Value::Integer(i as i32), Value::Integer((i as i32) * 2)],
                        &entry.schema,
                        1,
                    )
                    .expect("serialize");
                    heap.insert(t).expect("insert");
                }
            }
            catalog.create_index("alpha", "age").expect("index on alpha");
            catalog.create_index("mike", "age").expect("index on mike");
            catalog.persist().expect("persist");
            db.bp.flush_all().expect("flush");
            db.bp.disk_manager.sync().expect("sync");

            // Direct, single-run assertion for `persist`: the page lists its entries in name order.
            let frame_i = db.bp.fetch_page(catalog.first_catalog_page_id).expect("fetch");
            let page = {
                let frame = db.bp.frames[frame_i].read().unwrap();
                CatalogPage::deserialize(frame.data).expect("deserialize catalog page")
            };
            db.bp.unpin_page(catalog.first_catalog_page_id, false);
            let names: Vec<String> = page.entries.iter().map(|e| e.name.clone()).collect();
            assert!(
                names.len() >= 2,
                "run {run}: the catalog page holds {} entries, so an ordering bug could not show",
                names.len()
            );
            let mut sorted = names.clone();
            sorted.sort();
            assert_eq!(
                names, sorted,
                "run {run}: the persisted catalog page lists its tables out of order: {names:?}"
            );

            catalog.first_catalog_page_id
        };

        // Reboot and rebuild, which is what recovery does.
        let rebooted = f.restart();
        let db = open(&rebooted).expect("reopen");
        let before = rebooted.op_count();
        let mut catalog = Catalog::open(db.bp.clone(), first_catalog_page).expect("reopen catalog");
        assert_eq!(
            catalog.tables.len(),
            TABLES.len(),
            "the catalog did not come back with all its tables"
        );
        rebuild_indexes(&mut catalog, &db.bp).expect("rebuild_indexes");
        db.bp.flush_all().expect("flush");
        db.bp.disk_manager.sync().expect("sync");

        let seq: Vec<(String, u64, usize, u32)> = rebooted
            .trace()
            .into_iter()
            .filter(|t| t.index >= before && t.kind == OpKind::Pwrite)
            .map(|t| (t.file, t.offset, t.len, t.data_crc))
            .collect();
        assert!(
            seq.len() >= 20,
            "run {run}: rebuilding six tables' indexes wrote only {} pages, so an ordering bug \
             could not show and this assertion would be vacuous",
            seq.len()
        );
        traces.push(seq);
        // Which page each table's index landed on. With the sort this is a function of the names and
        // the row counts; without it, of a hash seed.
        let mut roots: Vec<(String, u32, Vec<u32>)> = catalog
            .tables
            .values()
            .map(|e| {
                (
                    e.name.clone(),
                    e.primary_index_root,
                    e.indexes.iter().map(|i| i.root_page_id).collect(),
                )
            })
            .collect();
        roots.sort();
        digests.push((rebooted.image_digest(), roots));
    }
    for run in 1..traces.len() {
        assert_eq!(
            traces[0], traces[run],
            "run {run} wrote the rebuilt catalog and indexes in a different order from run 0"
        );
        assert_eq!(
            digests[0], digests[run],
            "run {run} put the rebuilt indexes on different pages from run 0"
        );
    }
}

/// **Undo runs the losers in ascending transaction id, and the log itself says so.**
///
/// Breaking shape: more than one uncommitted transaction at the crash. `recover` took its losers
/// straight from `last_lsn.keys()` — a `HashMap` — and undo *writes*: an `Abort` and a compensation
/// record per action, flushed per transaction. So two recoveries of one crash appended the same
/// records in different orders and produced different WAL bytes.
///
/// This asserts the order **directly**, from the records recovery appended, rather than by comparing
/// two runs. That matters: comparing runs only catches the bug when the hash order happens to differ,
/// and with two losers that is about half the time — measured, and it is why the reverted sort survived
/// a three-run comparison. With five losers, exactly one of the 120 possible orders passes this
/// assertion, so a reverted sort has one chance in 120 of getting through, per run.
#[test]
fn undo_compensates_the_losers_in_ascending_transaction_id_order() {
    let f = fabric(None, Durability::WriteThrough);
    let _ = workload(&f).expect("workload");
    let rebooted = f.restart();
    let db = open(&rebooted).expect("reopen");

    // Everything from here on is what recovery appended.
    let first_new_lsn = db.wal.next_lsn.load(Ordering::SeqCst);
    assert!(recover(&db.txn).expect("recover"), "recovery found nothing to do");

    assert!(
        db.wal.next_lsn.load(Ordering::SeqCst) > first_new_lsn,
        "recovery appended no records, so there is no order to check"
    );
    let groups = compensated_transactions(&db.wal, first_new_lsn);

    // Anti-vacuity: every loser really was undone, so this is an assertion about all five and not
    // about whichever one happened to be first.
    assert_eq!(
        groups.len(),
        LOSER_TXNS,
        "recovery compensated {} transactions, expected the {LOSER_TXNS} that never committed: \
         {groups:?}",
        groups.len()
    );
    let mut ascending = groups.clone();
    ascending.sort_unstable();
    assert_eq!(
        groups, ascending,
        "undo ran the uncommitted transactions in the order {groups:?}, not ascending transaction \
         id. Recovery is a sequence of durable writes and this is the sequence."
    );
}

/// **A known gap, pinned: 24 unchecksummed bytes can take the whole database down while every row it
/// ever committed is safe on disk.**
///
/// Breaking shape: a garbled — not lost, not truncated — write to the WAL's header. The header holds
/// magic, version, base LSN and next transaction id, it is rewritten in place by
/// `WalManager::truncate` on every checkpoint, and it carries **no checksum**. One wrong byte inside
/// the version field and `WalManager::new` refuses with `incorrect wal version`; `run_cli` cannot get
/// past it, so the database will not open at all.
///
/// What makes it worth pinning rather than shrugging at is the ordering. A checkpoint flushes the log,
/// writes the pages, **fsyncs the pages**, and only then rewrites this header — so at the moment the
/// header is damaged every committed row is already durable. This test proves that: it drops a
/// brand-new empty log next to the surviving database file and gets all 210 rows back.
///
/// The fix is a checksum over the header and a way to rebuild or refuse-and-continue when it fails;
/// that is a WAL format change, and it is not in this branch. **If this test ever fails**, someone has
/// made the header verifiable, and it should become the assertion that the database still opens.
#[test]
fn a_garbled_wal_header_takes_the_database_down_with_its_data_intact() {
    let census = fabric(None, Durability::WriteThrough);
    let base = workload(&census).expect("census");
    let clean_db_len = census.durable_image().get(DB).map(|v| v.len()).unwrap_or(0);
    assert!(clean_db_len > 0, "the census wrote no database file");

    // A fabric that WILL garble the header: no `verified_from` floor at all.
    let mut found = None;
    for &n in census.faultable_ops().iter() {
        let live = SimFabric::with_fault(
            FaultPlan::at_shaped(n, 0x0B10_5EED, WriteShape::Corrupt),
            Durability::WriteThrough,
        );
        live.set_write_atomicity(DB, PAGE_SIZE as u64);
        let _ = workload(&live);
        let Some(fired) = live.fired() else { continue };
        if fired.kind != FaultKind::CorruptWrite
            || fired.file != WAL
            || fired.offset >= wal_header_len()
        {
            continue;
        }
        // Does the log now refuse to open?
        let rebooted = live.restart();
        if open(&rebooted).is_err() {
            found = Some((n, fired, live.durable_image()));
            break;
        }
    }
    let (op, fired, image) = found.expect(
        "no garbled header byte stopped the log from opening. Either the header is verified now — in \
         which case this test should assert that the database still opens — or the corruption model \
         stopped reaching offset 0 of the log.",
    );

    // The database file is untouched and still holds everything.
    let db_len = image.get(DB).map(|v| v.len()).unwrap_or(0);
    assert_eq!(
        db_len, clean_db_len,
        "the database file is a different length from the fault-free run, so this is not purely a \
         log-header problem"
    );

    // Same pages, brand-new empty log: every committed row comes back. So the data was never at risk
    // and 24 bytes were.
    let mut repaired = image.clone();
    repaired.insert(WAL.to_string(), Vec::new());
    let fresh = SimFabric::from_images(repaired, None, Durability::WriteThrough);
    fresh.set_write_atomicity(DB, PAGE_SIZE as u64);
    let after = reboot_and_recover(&fresh, base.dir_root);
    let rows = after.outcome.as_ref().expect("the pages alone must open with a fresh log");
    assert_eq!(
        rows.len(),
        ROWS + ROWS2,
        "with a fresh log the pages gave back {} of {} rows, so the data was NOT already durable and \
         this test is describing the wrong failure",
        rows.len(),
        ROWS + ROWS2
    );
    println!(
        "known gap: byte {} of the WAL header, garbled at operation {op} ({fired:?}), makes the \
         database unopenable while all {} rows are durable in the data file",
        fired.kept,
        ROWS + ROWS2
    );
}

/// **Every page the log names and the file does not hold gets its own empty page, in ascending order.**
///
/// Breaking shape: a crash that leaves several pages named by the log missing from the file, at
/// *scattered* page ids. `recover` walks its `touched` set — a `HashSet` — and writes an empty page for
/// each id it cannot read:
///
/// ```text
/// for (_, page_id) in &touched {
///     if bp.disk_manager.read(*page_id).is_err() { write Page::empty(*page_id) }
/// }
/// ```
///
/// and writing page 10 **extends the file past pages 3 and 7**, so a later `read(3)` succeeds on a
/// zero-filled gap and page 3 never gets its own empty page. A zero-filled gap deserialises as a page
/// whose recorded id is 0 and whose free space is 0, so the directory entry rebuilt from it claims the
/// page is full. Visiting the ids in ascending order is what stops a high page from hiding the low
/// ones — the sort is not only about reproducibility here, it changes what the database ends up
/// holding.
///
/// So this asserts the exact set: the empty pages recovery wrote must be precisely the missing ids, in
/// ascending order. The missing set is computed **independently**, by walking the surviving log for the
/// pages it names and comparing against the surviving file's length — not by asking `recover` what it
/// thought. Each repair write is identified in the trace by its content digest, because it is exactly
/// `Page::empty(page_id).serialize()`.
#[test]
fn recovery_gives_every_missing_page_its_own_empty_page_in_ascending_order() {
    // What an empty page for each id looks like on the wire, so a repair write is identifiable.
    const MAX_PAGE: u32 = 512;
    let page_of_crc: BTreeMap<u32, u32> = (0..MAX_PAGE)
        .map(|pid| (crc32(&Page::empty(pid).serialize().expect("serialize an empty page")), pid))
        .collect();

    // `SyncOnly` leaves the most for recovery to rebuild: page writes sit in a volatile cache, so a
    // crash before the checkpoint's fsync means the pages the log describes are simply not there.
    let census = fabric(None, Durability::SyncOnly);
    workload(&census).expect("census");

    // Take the crash point whose surviving image is missing the most pages the log names.
    let mut best: Option<(usize, BTreeMap<String, Vec<u8>>)> = None;
    for &n in census.faultable_ops().iter() {
        let live = fabric(Some(FaultPlan::at(n, 0x0B10_5EED)), Durability::SyncOnly);
        let _ = workload(&live);
        let image = live.durable_image();
        let probe = SimFabric::from_images(image.clone(), None, Durability::SyncOnly);
        probe.set_write_atomicity(DB, PAGE_SIZE as u64);
        probe.set_verified_from(WAL, wal_header_len());
        let Ok(db) = open(&probe) else { continue };
        let missing = missing_pages(&db.wal, image.get(DB).map(|v| v.len() as u64).unwrap_or(0));
        if best.as_ref().is_none_or(|(m, _)| missing.len() > *m) {
            best = Some((missing.len(), image));
        }
    }
    let (_, image) = best.expect("no crash point produced an openable image at all");

    let rebooted = SimFabric::from_images(image.clone(), None, Durability::SyncOnly);
    rebooted.set_write_atomicity(DB, PAGE_SIZE as u64);
    rebooted.set_verified_from(WAL, wal_header_len());
    let db = open(&rebooted).expect("reopen");
    let db_len = image.get(DB).map(|v| v.len() as u64).unwrap_or(0);
    let missing = missing_pages(&db.wal, db_len);
    assert!(
        missing.len() >= 3,
        "the best crash point is missing only {} of the pages its log names; with fewer than three, \
         scattered ids cannot show a high page hiding a low one and this test would be vacuous",
        missing.len()
    );

    let before = rebooted.op_count();
    recover(&db.txn).expect("recover");
    let repaired: Vec<u32> = rebooted
        .trace()
        .into_iter()
        .filter(|t| t.index >= before && t.file == DB && t.kind == OpKind::Pwrite)
        .filter_map(|t| page_of_crc.get(&t.data_crc).copied().filter(|p| *p as u64 * PAGE_SIZE as u64 == t.offset))
        .collect();

    assert_eq!(
        repaired, missing,
        "recovery rebuilt pages {repaired:?}; the log names {missing:?} as missing from a {db_len}-byte \
         file. A high page repaired first extends the file past the lower ones, so they read back as a \
         zero-filled gap and never get an empty page of their own."
    );
    println!("recovery rebuilt {} missing pages, in ascending order: {repaired:?}", repaired.len());
}

/// Pages the surviving log names that a `db_len`-byte file cannot hold. Computed from the log and the
/// file length alone, so it is an independent answer rather than `recover`'s own.
fn missing_pages(wal: &WalManager, db_len: u64) -> Vec<u32> {
    let mut named: BTreeSet<u32> = BTreeSet::new();
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    while lsn < end {
        let Ok((rec, next)) = wal.read_record(lsn) else { break };
        let mut note = |k: &RecKind| match k {
            RecKind::HeapInsert { page_id, .. }
            | RecKind::HeapDelete { page_id, .. }
            | RecKind::HeapUpdate { page_id, .. } => {
                named.insert(*page_id);
            }
            _ => {}
        };
        match &rec.kind {
            RecKind::Clr { redo, .. } => note(redo),
            other => note(other),
        }
        if next <= lsn {
            break;
        }
        lsn = next;
    }
    named
        .into_iter()
        .filter(|p| (*p as u64 + 1) * PAGE_SIZE as u64 > db_len)
        .collect()
}
