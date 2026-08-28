use std::{collections::{HashMap, HashSet}, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}};

use crate::catalog::column::DataType;
use crate::provenance::RunEntity;
use crate::{buffer::buffer_pool::BufferPoolManager, error::FerroError, storage::{heap_page::Page, tuple::{Tuple, VersionHeader}}, wal::{log::{DdlOp, RecKind, WalManager, WalPin}}};

/// Commits between automatic checkpoints.
///
/// Overridable by `FERRODB_CHECKPOINT_INTERVAL` **so that tests can make truncation constant
/// instead of rare.** That is not a convenience knob: three separate features shipped with a test
/// that passed only because it stayed under this threshold, and past it each one failed for real —
/// a base backup dead on arrival, a table's schema erased from the log, a live consumer whose
/// cursor had been truncated away. A condition that almost never happens is a condition nothing is
/// tested against, so this exists to let a test set it to 1 and make every commit truncate.
///
/// Read once, because a value that changes underneath a running database would make checkpoint
/// timing depend on when the environment was last read rather than on how much work has happened.
fn checkpoint_interval() -> u64 {
    use std::sync::OnceLock;
    static INTERVAL: OnceLock<u64> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        std::env::var("FERRODB_CHECKPOINT_INTERVAL")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            // Zero would mean "checkpoint before every commit has happened", which is not a
            // meaningful setting; treat it as 1 rather than dividing the world by it.
            .map(|v| v.max(1))
            .unwrap_or(256)
    })
}


pub struct TxnManager {
    pub wal: Arc<WalManager>,
    pub bp: Arc<BufferPoolManager>,
    pub next_txn_id: AtomicU64,
    pub att: Mutex<HashMap<u64, TxnEntry>>,
    pub commits_since_checkpoint: AtomicU64,
    /// Every table's DDL, retained so a checkpoint can re-establish it at the head of the new log.
    ///
    /// A checkpoint truncates the WAL, which would otherwise discard the `CREATE TABLE` records a
    /// log reader needs — and `CREATE TABLE` itself checkpoints, so creating a second table wiped
    /// the first one's schema. Replaying them after each truncation keeps the log self-describing
    /// from its own base, which is the same reason an ARIES checkpoint re-records the dirty page
    /// table rather than assuming a reader saw the original entries.
    schema_log: Mutex<Vec<DdlRecord>>,
    /// Every agent run this database has been told about, retained for exactly the reason
    /// `schema_log` is: [`WalManager::truncate`] discards the log **whole** rather than by prefix,
    /// so a checkpoint erases every identity record in it. Replayed as declarations at the head of
    /// the new log by [`TxnManager::replay_runs`], so a reader starting at the new base can still
    /// name the database's writers.
    ///
    /// **Stated cost:** this grows with the number of distinct runs and is never pruned, and every
    /// checkpoint rewrites all of it — the same unbounded shape `schema_log` has for tables, where
    /// the bound is the schema and here it is the agent history. A database with a very large
    /// number of runs pays for that at each checkpoint. [`TxnManager::retained_runs`] is how a
    /// caller sees the size; nothing here caps it, because dropping declarations would silently
    /// make some writers unnameable and that is the failure this record exists to prevent.
    run_log: Mutex<Vec<RunEntity>>,
    /// Open transaction -> the run that will be named immediately before its `Commit`.
    ///
    /// Held here rather than written when it is bound, and that is the whole correctness property.
    /// See [`TxnManager::bind_run`].
    run_bindings: Mutex<HashMap<u64, RunEntity>>,
}

/// A retained DDL record, replayed into the log after every checkpoint.
#[derive(Debug, Clone)]
pub struct DdlRecord {
    pub op: DdlOp,
    pub table: String,
    pub dir_root: u32,
    pub time_travel_root: u32,
    pub columns: Vec<(String, DataType, bool)>,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub high_water: u64,
    pub active: HashSet<u64>,
}

impl Snapshot {
    /// Whether a transaction's work is **already reflected in this snapshot**.
    ///
    /// The same rule [`ReadView::visible`] applies to a row version, stated over transaction ids
    /// instead. That restatement is what a change feed needs: an event carries the id of the
    /// transaction that produced it, not a version header, and the only question a snapshot-to-
    /// stream cutover has to answer is "did the snapshot already contain this transaction".
    ///
    /// Below the high water mark and not in flight means committed before the snapshot was taken,
    /// so every row that transaction wrote is in the snapshot's rows. In flight, or numbered at or
    /// above the high water mark, means it was not — and the stream owes the consumer those rows.
    pub fn includes(&self, txn_id: u64) -> bool {
        txn_id < self.high_water && !self.active.contains(&txn_id)
    }

    /// Whether a **change-feed event** attributed to `txn_id` was already delivered by this
    /// snapshot, and therefore must not be delivered again by the stream.
    ///
    /// This differs from [`Snapshot::includes`] in exactly one case, and that case is the reason it
    /// exists rather than callers being trusted to remember it. **Id 0 is not a transaction.** DDL
    /// is logged under it and `begin` never hands it out, but the arithmetic in `includes` is about
    /// timestamps and answers "yes, contained" for 0 — it is below every high water mark and in no
    /// active set. A stream that asked `includes` would suppress every schema declaration and leave
    /// a consumer without the shape of any table created after the cutover.
    ///
    /// The two questions are not the same question with a special case bolted on. `includes` asks
    /// about a *version timestamp*, where 0 is a legitimate value meaning "not written by any
    /// transaction this snapshot has to reason about", and MVCC visibility depends on it staying
    /// that way. This asks about an *event's author*, where 0 means there is no author to compare.
    /// Sharing the arithmetic is right; sharing the answer for 0 is not.
    pub fn already_delivered(&self, txn_id: u64) -> bool {
        txn_id != 0 && self.includes(txn_id)
    }
}

/// A snapshot taken so a change-feed consumer can cut over to the live stream.
///
/// The two fields together are what makes the cutover exact, and neither is sufficient alone:
///
/// - `resume_lsn` is early enough that **no** excluded transaction's records sit below it, so
///   nothing can be missed. It is not "the LSN when the scan started" — a transaction that was
///   already in flight then has records *earlier* than that, and it is excluded from the snapshot,
///   so a stream starting at the scan's LSN would see its `Commit` with none of its changes and
///   drop them silently. So the resume point is pulled back to the oldest in-flight transaction's
///   `Begin`.
/// - `snapshot` says exactly which transactions the rows already cover, so the stream can drop
///   their events instead of re-delivering them.
///
/// Resume position alone gives at-least-once; the transaction set is what removes the overlap.
///
/// **The pin is the third part, and it is here rather than left to the caller because leaving it to
/// the caller did not work.** A recipe documented in `replication::snapshot` said to call this,
/// read every table under the one reader, close it, and stream from `resume_lsn` — and omitted the
/// pin. At the default checkpoint interval of 256 commits nothing truncates in between, so it
/// passed; at `FERRODB_CHECKPOINT_INTERVAL=1` the very next commit truncates the log out from under
/// the resume point and the stream is refused with *"cannot pin lsn 2597: the log has already been
/// truncated to base 4260"*. Handing the claim back with the position it protects is the only shape
/// in which a caller cannot forget it.
#[derive(Debug)]
pub struct SnapshotHandoff {
    /// The reading transaction. Reads performed under it see exactly `snapshot`.
    pub txn_id: u64,
    /// The transactions whose work the read already contains.
    pub snapshot: Snapshot,
    /// Where a stream must resume. Durable by the time this is returned.
    pub resume_lsn: u64,
    /// A claim on the log at `resume_lsn`, held so a checkpoint cannot discard the records the
    /// stream is about to ask for. Released when this handoff is dropped, so a caller that intends
    /// to stream later must keep it alive until the subscription has taken its own.
    pub pin: WalPin,
}

pub struct TxnEntry {
    pub status: TxnStatus,
    pub last_lsn: u64,
    /// LSN of this transaction's `Begin` record — its earliest record, and therefore the earliest
    /// point a reader would have to resume from in order to see everything it did.
    pub begin_lsn: u64,
    pub snapshot: Option<Snapshot>,
}

pub enum TxnStatus {
    Running, 
    Commiting,
    Aborting
}

#[derive(Debug, Clone)]
pub struct ReadView {
    pub snapshot: Snapshot,
    pub txn_id: u64,
}

impl TxnManager {
    pub fn new(wal: Arc<WalManager>, bp: Arc<BufferPoolManager>) -> Self {
        let start = wal.header_txn_id;
        Self { wal, bp, next_txn_id: AtomicU64::new(start), att: Mutex::new(HashMap::new()), commits_since_checkpoint: AtomicU64::new(0), schema_log: Mutex::new(Vec::new()), run_log: Mutex::new(Vec::new()), run_bindings: Mutex::new(HashMap::new()) }
    }

    pub fn begin(&self) -> Result<u64, FerroError> {
        let mut att = self.att.lock().unwrap();
        Self::begin_locked(&self.next_txn_id, &self.wal, &mut att)
    }

    /// The body of `begin`, with the active-transaction table already locked.
    ///
    /// Factored out so [`TxnManager::begin_snapshot_read`] can observe the table in the *same*
    /// critical section that allocates the id and writes the `Begin` — see the comment there for
    /// why doing it in two steps would be a race rather than a tidiness question.
    fn begin_locked(
        next_txn_id: &AtomicU64,
        wal: &WalManager,
        att: &mut HashMap<u64, TxnEntry>,
    ) -> Result<u64, FerroError> {
        let txn_id = next_txn_id.fetch_add(1, Ordering::SeqCst);
        let lsn = wal.append(txn_id, 0, &RecKind::Begin)?;
        let snapshot = Snapshot { high_water: txn_id, active: att.keys().copied().collect() };
        att.insert(
            txn_id,
            TxnEntry {
                status: TxnStatus::Running,
                last_lsn: lsn,
                begin_lsn: lsn,
                snapshot: Some(snapshot),
            },
        );
        Ok(txn_id)
    }

    /// Open a transaction to take a consistent snapshot from, and report where a stream must
    /// resume so the two meet **with no gap and no overlap**.
    ///
    /// # Why the whole thing is one critical section
    ///
    /// Three facts have to be sampled together or the cutover is wrong:
    ///
    /// 1. which transactions are in flight (they are excluded from the snapshot, so the stream owes
    ///    them),
    /// 2. the high water mark (everything numbered above it is excluded too),
    /// 3. the earliest `Begin` still in flight (the stream has to start at or before it, or the
    ///    excluded transactions' changes sit below the resume point and are lost).
    ///
    /// Sampling them separately loses rows. Read the in-flight set, then have one of them commit,
    /// then compute the earliest `Begin` from what is left: the resume point jumps forward past the
    /// records of a transaction the snapshot does not contain, and its rows reach nobody. Every
    /// transactional record in this engine is appended under this same lock, so holding it makes
    /// all three consistent with each other and freezes the log while they are taken.
    ///
    /// The `Begin`s of transactions that start *after* this call are necessarily above
    /// `resume_lsn`, for the same reason: they cannot be appended until this lock is released.
    ///
    /// The caller must finish with [`TxnManager::end_read_only`].
    pub fn begin_snapshot_read(&self) -> Result<SnapshotHandoff, FerroError> {
        let (txn_id, snapshot, resume_lsn) = {
            let mut att = self.att.lock().unwrap();
            let txn_id = Self::begin_locked(&self.next_txn_id, &self.wal, &mut att)?;
            // Includes this reader's own `Begin`, which is the answer when nothing else is in
            // flight: there is then nothing below it that the snapshot does not already contain.
            let resume_lsn = att
                .values()
                .map(|e| e.begin_lsn)
                .min()
                .expect("the reader was just inserted, so the table cannot be empty");
            let snapshot = att[&txn_id]
                .snapshot
                .clone()
                .expect("begin_locked always records a snapshot");
            (txn_id, snapshot, resume_lsn)
        };

        // **The reader is in `att` from here on, and the caller does not have its id yet.** So a
        // `?` on either step below would leak it with nobody able to clean it up: `checkpoint`
        // refuses while `att` is non-empty, which means every later CREATE TABLE, CREATE INDEX and
        // CLI shutdown fails with "checkpoint with active txns" for the life of the process. One
        // failed snapshot would take the database with it. Both steps therefore report through
        // `abandon_reader` rather than through `?`.
        let prepared = (|| {
            // A resume point the log has not durably written is not a position anyone can resume
            // from: a crash moves the frontier backwards underneath it and the consumer is left
            // pointing into a range that will be rewritten by different records. `flush` drains
            // everything buffered, and `resume_lsn` is at or below the reader's own `Begin`, so
            // this covers it.
            self.wal.flush()?;
            // Claimed before this returns, so the resume point is never *published* unclaimed. See
            // `SnapshotHandoff::pin` for what leaving this to the caller cost.
            //
            // **It is not claimed from the moment it exists, and the gap is real rather than
            // theoretical.** The `Begin` above went in under the `att` lock, which was then
            // released; the pin is taken here, without it. A concurrent `checkpoint` samples
            // `att.is_empty()` and *drops that lock before truncating*, so one that found the table
            // empty a moment before this reader was inserted can truncate in between - and then
            // this `pin` fails. It is tempting to write that a checkpoint cannot truncate while a
            // transaction is open; it is not true as written, because the check and the truncation
            // are not one critical section.
            //
            // Which is precisely why the failure below closes the reader instead of returning `?`.
            // The race is narrow, its outcome is a clean refusal the caller can retry, and the
            // alternative - holding `att` across a checkpoint's flush, page writes and fsync - buys
            // atomicity here at the cost of blocking every begin and commit on disk IO. The error
            // path is the cheaper correct answer; it just has to not leak.
            self.wal.pin(resume_lsn)
        })();

        match prepared {
            Ok(pin) => Ok(SnapshotHandoff { txn_id, snapshot, resume_lsn, pin }),
            Err(e) => {
                self.abandon_reader(txn_id);
                Err(e)
            }
        }
    }

    /// Drop a snapshot reader nobody can close, because nobody else knows it exists.
    ///
    /// **The entry must leave `att`, and that is not negotiable** — a checkpoint refuses while any
    /// transaction is active, so an entry left behind here blocks every checkpoint for the life of
    /// the process. It is therefore removed directly if the ordinary close cannot write its
    /// `TxnEnd`, which is the likely case: the reason this is being called at all is usually that
    /// the WAL just refused a write.
    ///
    /// Removing it without a `TxnEnd` record is safe, and specifically because this is a *reader*.
    /// Recovery treats a transaction with no `Commit` or `TxnEnd` as a loser and aborts it
    /// (`wal::recovery`), and an abort walks the undo chain back from its last record — for a
    /// transaction whose `last_lsn` is still its `begin_lsn` there is nothing on that chain, so the
    /// abort is a no-op. A reader that had written would not reach this path: `end_read_only`
    /// refuses and rolls it back.
    fn abandon_reader(&self, txn_id: u64) {
        // The ordinary close first, so the common case still records its `TxnEnd` in the log.
        let _ = self.end_read_only(txn_id);
        self.att.lock().unwrap().remove(&txn_id);
    }

    /// Close a transaction that only read.
    ///
    /// Not `commit`, and not `abort`, and neither is a stylistic preference:
    ///
    /// - `commit` advances the checkpoint counter, and a checkpoint truncates the whole WAL. Ending
    ///   a snapshot read that way could discard the very records the handoff LSN points at, turning
    ///   a successful cutover into "cursor is below the log's base" on the consumer's first pump.
    /// - `abort` would write an `Abort` record, telling every log reader that a transaction rolled
    ///   back when nothing happened at all.
    ///
    /// A reader that turns out to have written is **refused and rolled back** rather than quietly
    /// ended, because its writes would be invisible to its own snapshot and it is not a snapshot
    /// reader at all.
    pub fn end_read_only(&self, txn_id: u64) -> Result<(), FerroError> {
        let wrote = {
            let att = self.att.lock().unwrap();
            let entry = att
                .get(&txn_id)
                .ok_or_else(|| FerroError::Txn(format!("txn {txn_id} is not active")))?;
            entry.last_lsn != entry.begin_lsn
        };
        if wrote {
            self.abort(txn_id)?;
            return Err(FerroError::Txn(format!(
                "transaction {txn_id} was opened to take a snapshot and then wrote to the \
                 database; it has been rolled back"
            )));
        }
        // **The entry leaves `att` whether or not the `TxnEnd` could be written.** Ordering these
        // the other way round - append with `?`, then remove - returns an error to a caller who has
        // already handed over responsibility for this transaction and now has nothing useful to do
        // with it. Retrying calls back into a WAL that just failed; not retrying leaves the reader
        // active for ever, and an active transaction blocks every checkpoint. The error is still
        // reported; what changes is that reporting it no longer wedges the database. See
        // `abandon_reader` for why a reader with no `TxnEnd` record is safe for recovery.
        //
        // **Not reachable in production today, which is why the test forces it.** `append_chained`
        // fails two ways: the entry is missing from `att`, which cannot happen here because it was
        // just checked and would mean no leak anyway; or `WalManager::append` returns `Err`, which
        // it never does today - it extends an in-memory buffer and ends in `Ok(lsn)`. Rather than
        // leave the guard unproven, `WalManager::fail_next_append` (test-only) fires it on demand;
        // see `a_reader_is_not_leaked_when_its_txn_end_cannot_be_written`. The guard earns its
        // place the day `append` does any IO - a bounded buffer that flushes when full, a direct
        // write - because then the failure is real and its cost is the whole database.
        let appended = self.append_chained(txn_id, &RecKind::TxnEnd);
        self.att.lock().unwrap().remove(&txn_id);
        appended?;
        Ok(())
    }

    pub fn log_insert(&self, txn_id: u64, dir_root: u32, page_id: u32, slot: u16, tuple: &[u8]) -> Result<u64, FerroError> {
        self.append_chained(txn_id, &RecKind::HeapInsert { dir_root, page_id, slot, tuple: tuple.to_vec() })
    }

    pub fn log_delete(&self, txn_id: u64, dir_root: u32, page_id: u32, slot: u16, old: &[u8]) -> Result<u64, FerroError> {
        self.append_chained(txn_id, &RecKind::HeapDelete { dir_root, page_id, slot, old: old.to_vec() })
    }

    pub fn log_update(&self, txn_id: u64, dir_root: u32, page_id: u32, slot: u16, old: &[u8], new: &[u8]) -> Result<u64, FerroError> {
        self.append_chained(txn_id, &RecKind::HeapUpdate { dir_root, page_id, slot, old: old.to_vec(), new: new.to_vec() })
    }

    pub fn append_chained(&self, txn_id: u64, kind: &RecKind) -> Result<u64, FerroError> {
        let mut att = self.att.lock().unwrap();
        let entry = att.get_mut(&txn_id).ok_or_else(|| FerroError::Wal("txn not active".into()))?;
        let lsn = self.wal.append(txn_id,entry.last_lsn, kind)?;
        entry.last_lsn = lsn;
        Ok(lsn)
    }

    /// Name the run that wrote this transaction, so its `Commit` is attributable.
    ///
    /// # The record is written at COMMIT, and nowhere else
    ///
    /// This method records the binding and writes nothing. The identity record goes into the log in
    /// [`TxnManager::commit`], in the append immediately before the `Commit` record, and that
    /// position is a correctness property rather than a tidy choice.
    ///
    /// A change feed's cursor may not advance past the earliest **staged** record of any still-open
    /// transaction — `Decoded::open_from` — or that transaction's rows are stepped over and lost
    /// when it finally commits. An identity record written when the session begins sits *below*
    /// that point: it stages nothing, so it does not hold the cursor back, and the next pump starts
    /// above it. The record is then never read again, and every row of that transaction ships
    /// attributed to nobody while the feed reports a clean run.
    ///
    /// Written immediately before the `Commit` instead, the record cannot be separated from the
    /// commit it describes. If a batch boundary falls between the two, the transaction is still
    /// open at the end of that batch, so the cursor is clamped back below its first staged row —
    /// which is below the identity record — and the next batch reads both together.
    /// `tests/integration_run_identity_feed.rs` runs exactly that workload against both placements.
    ///
    /// **The precise claim is "in this transaction, after every row it staged, before its
    /// `Commit`" — not physical adjacency in the log.** The two appends are separate calls and
    /// nothing holds a lock across them, so another transaction's record, or a `Ddl` record (which
    /// bypasses the active-transaction table entirely), can land between them. That is harmless: the
    /// decoder keys both the staging and the binding by `txn_id`, and the cursor argument above needs
    /// only that the identity record sit *above* this transaction's earliest staged record and
    /// *below* its commit. An earlier version of this doc asserted adjacency, which would have made
    /// any test of it flaky under a concurrent workload.
    ///
    /// # Guards
    ///
    /// Refuses an unknown transaction (a binding nothing would ever write), `ProvId::NONE` (a
    /// record claiming a writer must name one), and a second, *different* run for one transaction —
    /// one transaction has one writer, and quietly keeping either of two would attribute rows to an
    /// actor that did not write them. Re-binding the identical run is a no-op.
    pub fn bind_run(&self, txn_id: u64, run: RunEntity) -> Result<(), FerroError> {
        if run.prov_id.is_none() {
            return Err(FerroError::Provenance(format!(
                "refusing to bind run {}/{} to txn {txn_id} with ProvId::NONE: that is the value \
                 meaning 'unattributed', so the identity record would claim a writer and name none. \
                 Intern the run first and bind the id the store assigned.",
                run.agent_id, run.run_id
            )));
        }
        // **Every named field must be named.**
        //
        // Refused here rather than counted downstream, because the consumer's contract is stricter
        // than this type: `cdc-consumer`'s `checkWriter` REFUSES a writer object with an empty
        // agent, run, model or model_version, and it refuses the whole LINE — so one such run would
        // make `validate`, `sink`, `diff` and `follow` all abort at the first row it wrote and land
        // nothing at all. `model_version` is also the field `retract` keys on, and an empty one is
        // indistinguishable from the NULL a row with no writer carries.
        //
        // A producer that can emit what its consumer must reject is a contract whose halves
        // disagree, and the half to fix is the one that can still refuse cheaply.
        for (field, value) in [
            ("agent_id", &run.agent_id),
            ("run_id", &run.run_id),
            ("model", &run.model),
            ("model_version", &run.model_version),
        ] {
            if value.trim().is_empty() {
                return Err(FerroError::Provenance(format!(
                    "refusing to bind run {} to txn {txn_id}: its {field} is empty. Every field of \
                     an identity record is part of the answer to 'who wrote this row', and the \
                     change-feed consumer refuses a writer object carrying an empty one - so this \
                     run would make the whole feed unreadable rather than merely vague. Use an \
                     explicit placeholder such as \"unspecified\" if there is genuinely nothing to \
                     name.",
                    run.describe()
                )));
            }
        }
        if !self.att.lock().unwrap().contains_key(&txn_id) {
            return Err(FerroError::Txn(format!(
                "cannot bind a run to txn {txn_id}: it is not active, so no identity record would \
                 ever be written for it"
            )));
        }
        {
            let bindings = self.run_bindings.lock().unwrap();
            match bindings.get(&txn_id) {
                // Compared with `same_actor`, NOT `==`: `started_at` is when a particular session
                // began and is deliberately not part of who the actor is, so two sessions of one run
                // legitimately differ in it. `MemProvenanceStore::intern` hands them the same
                // `ProvId` for exactly that reason, and a full-equality test here would refuse a
                // second session of the same run because the clock had moved.
                Some(existing) if !existing.same_actor(&run) => {
                    return Err(FerroError::Provenance(format!(
                        "txn {txn_id} is already bound to {}; refusing to rebind it to {}. One \
                         transaction has one writer.",
                        existing.describe(),
                        run.describe()
                    )));
                }
                Some(_) => return Ok(()),
                None => {}
            }
        }
        // **Declared BEFORE the binding is installed, and that order is the fix for a real defect.**
        //
        // It used to insert first. When `declare_run` then refused a slot collision, `bind_run`
        // returned `Err` and the binding STOOD — so the next `commit` appended an identity record
        // for that slot anyway, putting two different actors under one `prov_id` in the log. That is
        // precisely the state `LogicalDecoder` refuses outright, which would make the whole log
        // range undecodable: the guard's own failure path produced the disaster the guard names.
        self.declare_run(run.clone())?;
        self.run_bindings.lock().unwrap().insert(txn_id, run);
        Ok(())
    }

    /// Remember a run so a checkpoint can re-declare it, without binding it to a transaction.
    ///
    /// Refuses to hold two different actors under one `prov_id`: the slot is the reference every
    /// stamped version carries, so two meanings for it would make every attribution ambiguous.
    pub fn declare_run(&self, run: RunEntity) -> Result<(), FerroError> {
        let mut log = self.run_log.lock().unwrap();
        if let Some(existing) = log.iter().find(|r| r.prov_id == run.prov_id) {
            // `same_actor`, not `==`. Full equality compares `started_at`, which is when a session
            // began rather than part of who the actor is — so a second session of the same run
            // carries a different one and was being refused for having a later clock reading. The
            // in-memory store already draws the line here and hands both sessions one `ProvId`; two
            // definitions of "the same actor" in one system is how that becomes a bug.
            if !existing.same_actor(&run) {
                return Err(FerroError::Provenance(format!(
                    "provenance slot {} is already declared as {}; refusing to redeclare it as {}",
                    run.prov_id,
                    existing.describe(),
                    run.describe()
                )));
            }
            return Ok(());
        }
        log.push(run);
        Ok(())
    }

    /// How many run declarations a checkpoint would replay. See [`TxnManager::run_log`].
    pub fn retained_runs(&self) -> usize {
        self.run_log.lock().unwrap().len()
    }

    /// Re-declare every known run at the head of the log, after a truncation discarded them.
    ///
    /// Transaction id 0, matching [`TxnManager::append_ddl`]: a declaration says "this run exists",
    /// and transaction 0 never commits, so it binds nothing. `LogicalDecoder` relies on exactly
    /// that to tell a declaration from a binding.
    fn replay_runs(&self) -> Result<(), FerroError> {
        let runs = self.run_log.lock().unwrap().clone();
        if runs.is_empty() {
            return Ok(());
        }
        for run in runs {
            self.wal.append(0, 0, &RecKind::RunIdentity { run })?;
        }
        self.wal.flush()
    }

    pub fn commit(&self, txn_id: u64) -> Result<(), FerroError> {
        // **Immediately before the `Commit`, with no append between them.** See `bind_run` for what
        // any other position costs. Read rather than removed, so a failed append leaves the binding
        // intact for the abort that follows.
        let bound = self.run_bindings.lock().unwrap().get(&txn_id).cloned();
        if let Some(run) = bound {
            self.append_chained(txn_id, &RecKind::RunIdentity { run })?;
        }
        let commit_lsn = self.append_chained(txn_id, &RecKind::Commit)?;
        self.wal.flush_up_to(commit_lsn)?;
        let _ = self.append_chained(txn_id, &RecKind::TxnEnd)?;
        self.att.lock().unwrap().remove(&txn_id);
        self.run_bindings.lock().unwrap().remove(&txn_id);
        if self.commits_since_checkpoint.fetch_add(1, Ordering::SeqCst) + 1 >= checkpoint_interval() && self.att.lock().unwrap().is_empty() {
            self.checkpoint()?;
        }
        Ok(())
    }

    pub fn abort(&self, txn_id: u64) -> Result<(), FerroError> {
        let abort_lsn = self.append_chained(txn_id, &RecKind::Abort)?;
        let _ = abort_lsn;
        {
            self.att.lock().unwrap().get_mut(&txn_id).unwrap().status = TxnStatus::Aborting;
        }
        let mut lsn = {
            self.att.lock().unwrap().get(&txn_id).unwrap().last_lsn
        };
        loop {
            let (rec, _) = self.wal.read_record(lsn)?;
            match rec.kind {
                RecKind::Begin => break,
                RecKind::HeapInsert { dir_root, page_id, slot, .. } => {
                    let clr = RecKind::Clr { undone_lsn: rec.lsn , undo_next: rec.prev_lsn, 
                        redo: Box::new(RecKind::HeapDelete{ dir_root, page_id, slot, old: Vec::new() })
                    };
                    let clr_lsn = self.append_chained(txn_id, &clr)?;
                    undo_insert(&self.bp, page_id, slot, clr_lsn)?;
                }
                RecKind::HeapDelete { dir_root, page_id, slot, old } => {
                    let clr = RecKind::Clr { undone_lsn: rec.lsn, undo_next: rec.prev_lsn, 
                        redo: Box::new(RecKind::HeapInsert { dir_root, page_id, slot, tuple: old.to_vec() })
                    };
                    let clr_lsn = self.append_chained(txn_id, &clr)?;
                    undo_delete(&self.bp, page_id, slot, &old, clr_lsn)?;
                }
                RecKind::HeapUpdate { dir_root, page_id, slot, old, new } => {
                    let clr = RecKind::Clr { undone_lsn: rec.lsn, undo_next: rec.prev_lsn, 
                        redo: Box::new(RecKind::HeapUpdate { dir_root, page_id, slot, old: new.clone(), new: old.clone() })
                    }; 
                    let clr_lsn = self.append_chained(txn_id, &clr)?;
                    undo_update(&self.bp, page_id, slot, &old, clr_lsn)?;
                }
                RecKind::Clr {undo_next, .. } => {
                    if undo_next == 0 {
                        break;
                    }
                    lsn = undo_next;
                    continue;
                }
                _ => {}
            }
            if rec.prev_lsn == 0 {
                break;
            }
            lsn = rec.prev_lsn;
        }
        let _ = self.append_chained(txn_id, &RecKind::TxnEnd)?;
        self.att.lock().unwrap().remove(&txn_id);
        // The run bound to this transaction described work that has been rolled back. No identity
        // record was written — they are only written at commit — so there is nothing in the log to
        // retract, only a binding that must not outlive its transaction id.
        self.run_bindings.lock().unwrap().remove(&txn_id);
        Ok(())
    }

    /// Remember a schema change and write it to the log.
    ///
    /// Retained rather than merely written, because the next checkpoint truncates whatever is
    /// there. A `DropTable` removes the table from the retained set as well as being logged, so a
    /// replay after truncation does not resurrect a table that no longer exists.
    ///
    /// # An `AlterColumn` is retained as a re-declaration, not as itself — B11
    ///
    /// This is what makes a column-level change survive a restart, and it is the one part of the
    /// design that is not obvious.
    ///
    /// The retained set is replayed at the head of the log after every truncation
    /// ([`Self::replay_schema`]), so whatever is in it is re-emitted to every consumer, repeatedly,
    /// forever. `CREATE_TABLE` is safe there because the feed documents it as a **declaration** —
    /// "this table has this shape" — which a consumer may apply any number of times. An `ALTER` is
    /// **news**: "this column just changed". Retaining the alter itself would re-deliver that news
    /// at every checkpoint, and a consumer applying a rename twice renames a column that no longer
    /// has the old name.
    ///
    /// So an alter updates the *declaration*: the retained record for this table becomes a
    /// `CreateTable` carrying the shape the alter produced. The alter is still appended to the log
    /// in its own right, in log order, exactly once. After a truncation the log re-declares the
    /// table with its **new** shape, which is precisely "the schema survives a restart".
    ///
    /// The record's `columns` must therefore be the table's FULL shape after the change. It is,
    /// for every op: `CreateTable` and `AlterColumn` both carry it, and `DropTable` carries none
    /// because there is no shape left to declare.
    pub fn log_ddl(&self, rec: DdlRecord) -> Result<(), FerroError> {
        {
            let mut log = self.schema_log.lock().unwrap();
            match &rec.op {
                DdlOp::CreateTable => {
                    log.retain(|r| r.dir_root != rec.dir_root);
                    log.push(rec.clone());
                }
                DdlOp::DropTable => log.retain(|r| r.dir_root != rec.dir_root),
                DdlOp::AlterColumn(_) => {
                    log.retain(|r| r.dir_root != rec.dir_root);
                    log.push(DdlRecord {
                        op: DdlOp::CreateTable,
                        table: rec.table.clone(),
                        dir_root: rec.dir_root,
                        time_travel_root: rec.time_travel_root,
                        columns: rec.columns.clone(),
                    });
                }
            }
        }
        self.append_ddl(&rec)?;
        self.wal.flush()
    }

    /// The shape the log would re-declare for `dir_root` after a truncation, if any.
    ///
    /// Exposed so a test can ask what survives a restart without having to truncate to find out.
    pub fn retained_shape(&self, dir_root: u32) -> Option<Vec<(String, DataType, bool)>> {
        self.schema_log
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.dir_root == dir_root)
            .map(|r| r.columns.clone())
    }

    fn append_ddl(&self, r: &DdlRecord) -> Result<(), FerroError> {
        self.wal.append(
            0,
            0,
            &RecKind::Ddl {
                op: r.op.clone(),
                table: r.table.clone(),
                dir_root: r.dir_root,
                time_travel_root: r.time_travel_root,
                columns: r.columns.clone(),
            },
        )?;
        Ok(())
    }

    /// Re-establish every known table's schema at the head of the log.
    ///
    /// Called immediately after a truncation. Without it the log is self-describing only until the
    /// first checkpoint, which is to say almost never.
    fn replay_schema(&self) -> Result<(), FerroError> {
        let records = self.schema_log.lock().unwrap().clone();
        if records.is_empty() {
            return Ok(());
        }
        for r in &records {
            self.append_ddl(r)?;
        }
        self.wal.flush()
    }

    pub fn checkpoint(&self) -> Result<(), FerroError> {
        // Deliberately a SHORT hold — the guard is a temporary and is released before the body
        // runs, which is exactly what this function did before `ddl_checkpointed` existed. Every
        // existing caller therefore keeps its old concurrency behaviour; only the DDL path below
        // needs the answer to stay true while it is acted on, and only it pays for that.
        if !self.att.lock().unwrap().is_empty() {
            return Err(FerroError::Wal("checkpoint with active txns".into()));
        }
        self.checkpoint_locked()
    }

    /// Do a DDL statement's irreversible catalog mutation and its checkpoint as ONE unit, with the
    /// attach table held shut for the whole of it.
    ///
    /// **A8: without this, a refused `CREATE TABLE` had already created the table.** The executor
    /// mutated the catalog and only then called `checkpoint`, which refuses while any transaction
    /// is attached — so the statement returned `Err` over a table that existed, was queryable, and
    /// accepted inserts. Worse, the `Ddl` record is logged *after* the checkpoint, and `log_ddl` is
    /// what puts a table into the retained `schema_log`, so no later checkpoint ever re-declared
    /// it: the table was invisible to every self-describing consumer PERMANENTLY, with its rows
    /// arriving as `unresolved`. The session's own `DDL not allowed in txn` guard does not catch
    /// this, because that guard is per-SESSION while checkpoint admissibility is global — the
    /// transaction that blocks the checkpoint can belong to any other session.
    ///
    /// Taking the decision before the mutation is what I19 did for the same class of defect (a
    /// refused `ALTER` that had already destroyed rows). Merely *asking* first would leave a window:
    /// `begin` takes this same lock, so a transaction starting between the question and the
    /// checkpoint would put the statement right back into the half-done state. Holding the guard
    /// across both closes it — the answer cannot go stale while it is being acted on.
    ///
    /// Nothing reachable from `f` or from `checkpoint_locked` takes `att`, so this cannot deadlock
    /// on itself, and the lock order here (`att`, then the buffer pool) is the order `checkpoint`
    /// already used.
    pub fn ddl_checkpointed<T>(
        &self,
        f: impl FnOnce() -> Result<T, FerroError>,
    ) -> Result<T, FerroError> {
        let att = self.att.lock().unwrap();
        if !att.is_empty() {
            return Err(FerroError::Wal("checkpoint with active txns".into()));
        }
        let out = f()?;
        self.checkpoint_locked()?;
        Ok(out)
    }

    /// The body of `checkpoint`, with the attach table ALREADY held shut by the caller.
    ///
    /// Must not take `att` — the callers above hold it, and `Mutex` is not re-entrant.
    fn checkpoint_locked(&self) -> Result<(), FerroError> {
        self.wal.flush()?;
        self.bp.flush_all()?;
        self.bp.disk_manager.sync()?;
        self.wal.truncate(self.next_txn_id.load(Ordering::SeqCst))?;
        self.commits_since_checkpoint.store(0, Ordering::SeqCst);
        // The truncation just discarded every DDL record. Put them back, or a log reader starting
        // at the new base has no way to know what any table is.
        self.replay_schema()?;
        // And every run declaration, for the same reason: a reader starting at the new base would
        // otherwise have no way to name the database's writers.
        self.replay_runs()?;
        Ok(())
    }

    pub fn snapshot_of(&self, txn_id: u64) -> Result<Snapshot, FerroError> {
        self.att.lock().unwrap().get(&txn_id).and_then(|e| e.snapshot.clone()).ok_or_else(|| FerroError::Txn("no snapshot for txn".into()))
    }

    pub fn read_snapshot(&self) -> Snapshot {
        let att = self.att.lock().unwrap();
        Snapshot { high_water: self.next_txn_id.load(Ordering::SeqCst), active: att.keys().copied().collect() }
    }
}

pub fn undo_insert(bp: &BufferPoolManager, page_id: u32, slot: u16, clr_lsn: u64) -> Result<(), FerroError> {
    with_page(bp, page_id, clr_lsn, |page| page.delete(slot as usize))
}

pub fn undo_delete(bp: &BufferPoolManager, page_id: u32, slot: u16, old: &[u8], clr_lsn: u64) -> Result<(), FerroError> {
    with_page(bp, page_id, clr_lsn, |page| page.restore_at(slot as usize, old))
}

pub fn undo_update(bp: &BufferPoolManager, page_id: u32, slot: u16, old: &[u8], clr_lsn: u64) -> Result<(), FerroError> {
    with_page(bp, page_id, clr_lsn, |page| page.update(slot as usize, Tuple::new(old.to_vec())))
}

pub fn stamp_page_lsn(bp: &BufferPoolManager, page_id: u32, lsn: u64) -> Result<(), FerroError> {
    with_page(bp, page_id, lsn, |_| Ok(()))
}

pub fn with_page<F>(bp: &BufferPoolManager, page_id: u32, lsn: u64, f: F) -> Result<(), FerroError> 
where F: FnOnce(&mut Page) -> Result<(), FerroError> {
    let frame_i = bp.fetch_page(page_id)?;
    let mut frame = bp.frames[frame_i].write().unwrap();
    let mut page = Page::deserialize(frame.data)?;
    f(&mut page)?;
    page.lsn = lsn;
    frame.data = page.serialize()?;
    drop(frame);
    bp.unpin_page(page_id, true);
    Ok(())
}

impl ReadView {
    pub fn visible(&self, h: &VersionHeader) -> bool {
        if !self.is_commited_for_me(h.begin_ts) {
            return false;
        }
        let ended = h.end_ts != 0 && self.is_commited_for_me(h.end_ts);
        !ended
    }

    /// Stated once, and used by [`ReadView::visible`] and by
    /// [`Snapshot::includes`] rather than spelled out again in either. A cutover that decides
    /// "the snapshot already has this transaction" by a *different* rule than the one the snapshot
    /// was read with is a duplicate or a hole, depending on which way the two drift.
    pub fn is_commited_for_me(&self, ts: u64) -> bool {
        ts == self.txn_id || self.snapshot.includes(ts)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{OpenOptions, metadata};

use crate:: {catalog::catalog::Catalog, execution::{executor::{Outcome, run}, session::Session}, parser::{parser::Parser, scanner::Scanner}, storage::{disk_manager::DiskManager, heap_file_manager::HeapFileManager}, wal::log::{LogRecord, pwrite_all}};

use super::*;

    fn setup() -> (Arc<BufferPoolManager>, Arc<WalManager>, Arc<TxnManager>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.path().join("txn.db")).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        let wal = Arc::new(WalManager::new(dir.path().join("txn.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal.clone());
        (bp, wal, txn, dir)
    }

    fn walk_log(wal: &WalManager) -> Vec<LogRecord> {
        let mut out = Vec::new();
        let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
        let end = wal.next_lsn.load(Ordering::SeqCst);
        while lsn < end {
            let (rec, next) = wal.read_record(lsn).unwrap();
            out.push(rec);
            lsn = next;
        }
        out
    }

    fn a_run(prov: u32, agent: &str) -> RunEntity {
        RunEntity::new(
            crate::provenance::ProvId(prov),
            agent,
            "run-1",
            "claude-opus",
            "2026-05",
            [0xcd; 32],
            1_700_000_000_000,
            crate::branch::types::BranchId::new(1, 0),
        )
    }

    /// **The identity record is this transaction's last record before its `Commit`.**
    ///
    /// Its position is a correctness property of the change feed rather than a matter of taste —
    /// see [`TxnManager::bind_run`] — so it is asserted on the log's own record order and not only
    /// through the decoder that reads it.
    ///
    /// Filtered to **this transaction's** records on purpose. Nothing holds a lock across the two
    /// appends, so a concurrent transaction's record, or a `Ddl` (which bypasses the active-txn
    /// table), can land between them in the byte stream. Asserting raw adjacency would be asserting
    /// single-threadedness; what the feed needs is that this record follow every row this transaction
    /// staged and precede its commit.
    #[test]
    fn the_run_identity_record_is_the_last_record_before_its_own_commit() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        txn.bind_run(t1, a_run(1, "restock-agent")).unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![1, 2, 3, 4])).unwrap();
        txn.commit(t1).unwrap();

        let all = walk_log(&wal);
        let mine: Vec<&LogRecord> = all.iter().filter(|r| r.txn_id == t1).collect();
        let commit_at = mine
            .iter()
            .position(|r| matches!(r.kind, RecKind::Commit))
            .expect("no commit record for this transaction");
        assert!(commit_at > 0, "the commit is this transaction's first record");
        match &mine[commit_at - 1].kind {
            RecKind::RunIdentity { run } => assert_eq!(run.agent_id, "restock-agent"),
            other => panic!(
                "this transaction's record before its commit is {other:?}, not the run identity. \
                 Anything of this transaction's between them can be separated from the commit by a \
                 batch boundary, and the feed then ships the rows attributed to nobody."
            ),
        }
        // And it is AFTER every row this transaction staged, which is the other half of the cursor
        // argument: below the earliest staged record it would be stepped over.
        let last_row = mine
            .iter()
            .rposition(|r| matches!(r.kind, RecKind::HeapInsert { .. }))
            .expect("no row record");
        assert!(
            last_row < commit_at - 1,
            "the identity record sits at or below a row this transaction staged"
        );

        // Anti-vacuity: an unbound transaction writes no identity record at all, so the assertion
        // above is about the binding and not about some record that is always there.
        let t2 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t2);
        heap.insert(Tuple::new(vec![5, 6])).unwrap();
        txn.commit(t2).unwrap();
        let identities = walk_log(&wal)
            .iter()
            .filter(|r| matches!(r.kind, RecKind::RunIdentity { .. }))
            .count();
        assert_eq!(identities, 1, "an unbound transaction wrote an identity record");
    }

    /// **A second session of the same run is not a different actor, and the clock must not decide.**
    ///
    /// `started_at` is when a particular session began. `MemProvenanceStore::intern` deliberately
    /// excludes it from `same_actor` — its doc records that including it was a real bug CI caught,
    /// where the same input was refused or accepted depending on whether the system clock had ticked
    /// — and hands two sessions of one run the SAME `ProvId`.
    ///
    /// `declare_run` and `bind_run` reintroduced that bug by comparing with derived `==`, which does
    /// compare `started_at`. Two definitions of "the same actor" in one system is how this becomes a
    /// defect, and it is worse here than in the store: a refused declaration meant a legitimate
    /// second session could not commit at all.
    ///
    /// **Breaking shape:** the same run bound twice with a later `started_at` — that is, any agent
    /// that opens a second session, which is the ordinary case. A workload where each run commits
    /// exactly once never produces it.
    #[test]
    fn a_second_session_of_one_run_is_the_same_actor_however_the_clock_moved() {
        let (bp, _wal, txn, _dir) = setup();
        let first = a_run(1, "restock-agent");
        let mut later = a_run(1, "restock-agent");
        later.started_at = first.started_at + 5_000;
        assert_ne!(first, later, "the two entities must differ, or this test proves nothing");
        assert!(first.same_actor(&later), "same_actor changed meaning; this test is measuring it");

        txn.declare_run(first.clone()).unwrap();
        txn.declare_run(later.clone())
            .expect("a second session of the same run was refused for having a later clock reading");
        assert_eq!(txn.retained_runs(), 1, "the second session became a second declaration");

        // And it can actually commit, which is what the refusal was blocking.
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        let t1 = txn.begin().unwrap();
        txn.bind_run(t1, later.clone()).expect("bind_run refused a second session");
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![1, 2, 3, 4])).unwrap();
        txn.commit(t1).unwrap();

        // Anti-vacuity: a genuinely different actor under the same slot is still refused.
        let err = txn
            .declare_run(a_run(1, "auditor-agent"))
            .expect_err("a different agent under one slot was accepted");
        assert!(format!("{err}").contains("already declared"), "{err}");
    }

    /// **A refused `bind_run` must leave NO binding, or the refusal causes the thing it prevents.**
    ///
    /// The insert used to happen before `declare_run` could refuse. On a slot collision `bind_run`
    /// returned `Err` and the binding stood, so the next `commit` appended an identity record for
    /// that slot anyway — putting two different actors under one `prov_id` in the log, which is
    /// exactly what `LogicalDecoder` refuses outright. The guard's failure path produced the disaster
    /// the guard is named after.
    ///
    /// **Breaking shape:** a caller that ignores `bind_run`'s error and commits anyway — which is
    /// what any `?`-less call site does, and what a caller that logs and continues does deliberately.
    #[test]
    fn a_refused_binding_leaves_nothing_behind_for_the_commit_to_write() {
        let (bp, wal, txn, _dir) = setup();
        // Slot 1 already means `restock-agent`.
        txn.declare_run(a_run(1, "restock-agent")).unwrap();

        let t1 = txn.begin().unwrap();
        let err = txn
            .bind_run(t1, a_run(1, "auditor-agent"))
            .expect_err("a colliding slot was bound");
        assert!(format!("{err}").contains("already declared"), "{err}");

        // Commit anyway, as a caller that ignored the error would. No identity record may appear.
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![9, 9])).unwrap();
        txn.commit(t1).unwrap();

        let records = walk_log(&wal);
        let identities: Vec<&RecKind> = records
            .iter()
            .map(|r| &r.kind)
            .filter(|k| matches!(k, RecKind::RunIdentity { .. }))
            .collect();
        assert!(
            identities.is_empty(),
            "a refused binding still put an identity record in the log: {identities:?}"
        );

        // Anti-vacuity: an accepted binding does write one.
        let t2 = txn.begin().unwrap();
        txn.bind_run(t2, a_run(1, "restock-agent")).unwrap();
        heap.set_transaction(txn.clone(), t2);
        heap.insert(Tuple::new(vec![8, 8])).unwrap();
        txn.commit(t2).unwrap();
        let n = walk_log(&wal)
            .iter()
            .filter(|r| matches!(r.kind, RecKind::RunIdentity { .. }))
            .count();
        assert_eq!(n, 1, "an accepted binding wrote no identity record either");
    }

    /// **An identity record with an empty field is refused at the producer.**
    ///
    /// The consumer's contract is stricter than `RunEntity`: `cdc-consumer`'s `checkWriter` refuses a
    /// writer object with an empty agent, run, model or model_version, and it refuses the whole LINE
    /// — so one such run makes `validate`, `sink`, `diff` and `follow` abort at the first row it
    /// wrote and land nothing. `model_version` is also the field `retract` keys on, where an empty
    /// string is indistinguishable from the NULL a row with no writer carries.
    ///
    /// **Breaking shape:** any caller that defaults a field to `""` rather than to an explicit
    /// placeholder. `begin_session_with_model` already defaults to the literal `unspecified`, which is
    /// why this was reachable but not yet reached.
    #[test]
    fn binding_a_run_with_an_empty_named_field_is_refused() {
        let (_bp, _wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mutations: [(&str, fn(&mut RunEntity)); 4] = [
            ("agent_id", |r| r.agent_id = String::new()),
            ("run_id", |r| r.run_id = String::new()),
            ("model", |r| r.model = String::new()),
            // Whitespace only, because "   " is not a name and `trim` is what decides that.
            ("model_version", |r| r.model_version = "   ".into()),
        ];
        for (field, mutate) in mutations {
            let mut run = a_run(1, "restock-agent");
            mutate(&mut run);
            let err = txn
                .bind_run(t1, run)
                .unwrap_err();
            assert!(
                format!("{err}").contains(&format!("its {field} is empty")),
                "an empty {field} was not refused by this guard: {err}"
            );
            assert_eq!(txn.retained_runs(), 0, "a refused binding declared the run anyway");
        }
        // Anti-vacuity: a fully named run binds.
        txn.bind_run(t1, a_run(1, "restock-agent")).expect("a fully named run was refused");
    }

    /// One provenance slot cannot mean two actors. The slot is the reference every stamped version
    /// carries, so two meanings for it make every attribution ambiguous, and a checkpoint would
    /// replay both declarations into the log for a decoder to choose between.
    #[test]
    fn declaring_one_slot_as_two_actors_is_refused() {
        let (_bp, _wal, txn, _dir) = setup();
        txn.declare_run(a_run(1, "restock-agent")).unwrap();
        // Anti-vacuity: the same declaration again is a no-op, which is what a checkpoint replay
        // and a repeated session both produce.
        txn.declare_run(a_run(1, "restock-agent")).unwrap();
        assert_eq!(txn.retained_runs(), 1);

        let err = txn
            .declare_run(a_run(1, "auditor-agent"))
            .expect_err("one slot was declared as two actors");
        assert!(format!("{err}").contains("already declared"), "{err}");
        assert_eq!(txn.retained_runs(), 1, "the refused declaration was retained anyway");

        // A different slot is fine, so the refusal is about the collision and not about declaring.
        txn.declare_run(a_run(2, "auditor-agent")).unwrap();
        assert_eq!(txn.retained_runs(), 2);
    }

    #[test]
    fn test_commit_writes_chain_and_flushes() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![1,2,3,4])).unwrap();
        txn.commit(t1).unwrap();
        let recs = walk_log(&wal);
        assert_eq!(recs.len(), 4);
        assert!(matches!(recs[0].kind, RecKind::Begin));
        assert!(matches!(recs[1].kind, RecKind::HeapInsert { .. }));
        assert!(matches!(recs[2].kind, RecKind::Commit));
        assert!(matches!(recs[3].kind, RecKind::TxnEnd));
        assert_eq!(recs[1].prev_lsn, recs[0].lsn);
        assert_eq!(recs[2].prev_lsn, recs[1].lsn);
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) > recs[2].lsn);
        if let RecKind::HeapInsert { dir_root, tuple, .. } = &recs[1].kind {
            assert_eq!(*dir_root, heap.first_directory_page_id);
            assert_eq!(tuple, &vec![1, 2, 3, 4]);
        }

        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_abort_insert_removes_rows() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);

        for i in 0..3u8 {
            heap.insert(Tuple::new(vec![i,i,i])).unwrap();
        }
        txn.abort(t1).unwrap();
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        // begin, hi, hi, hi, abort, clr, clr, clr, txnend
        let recs = walk_log(&wal);
        let undone: Vec<u64> = recs[5..8].iter().map(|r| match &r.kind {
            RecKind::Clr { undone_lsn, .. } => *undone_lsn,
            _ => panic!()
        }).collect();
        assert!(rows.is_empty());
        assert_eq!(recs.len(), 9);
        assert!(matches!(recs[4].kind, RecKind::Abort));
        assert!(matches!(recs[8].kind, RecKind::TxnEnd));
        assert_eq!(undone, vec![recs[3].lsn, recs[2].lsn, recs[1].lsn]);
    }

    #[test]
    fn test_abort_delete_restores_row() {
        let (bp, _wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        let rid = heap.insert(Tuple::new(vec![8,8,8])).unwrap();
        txn.commit(t1).unwrap();

        let t2 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t2);
        heap.delete(rid).unwrap();
        assert!(heap.read(rid).is_err());
        txn.abort(t2).unwrap();
        
        assert_eq!(heap.read(rid).unwrap().data, vec![8,8,8]);
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn sql_insert_commits_through_run() {
        let (bp, wal, txn, _dir) = setup();
        let mut catalog = Catalog::create(bp.clone()).unwrap();
        let exec = |sql: &str, catalog: &mut Catalog| -> Outcome {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty());
            let mut session = Session::new();
            run(stmts.remove(0), catalog, bp.clone(), txn.clone(), &mut session).unwrap()
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL, name VARCHAR(20));", &mut catalog);
        let out = exec("INSERT INTO t VALUES (1, 'a');", &mut catalog);
        assert!(matches!(out, Outcome::Affected(1)));
        let recs = walk_log(&wal);
        assert!(recs.iter().any(|r| matches!(r.kind, RecKind::HeapInsert { .. })));
        assert!(recs.iter().any(|r| matches!(r.kind, RecKind::Commit)));
    }

    #[test]
    fn gate_flushed_wal_before_page_write() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        let rid = heap.insert(Tuple::new(vec![1, 2, 3])).unwrap();
        let target = wal.next_lsn.load(Ordering::SeqCst);
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) < target);
        bp.flush_page(rid.page_id).unwrap();
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) >= target);
        txn.abort(t1).unwrap();
    }

    #[test]
    fn gate_covers_flush_all() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![9,9])).unwrap();

        let target = wal.next_lsn.load(Ordering::SeqCst);
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) < target);
        bp.flush_all().unwrap();
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) >= target);
        txn.abort(t1).unwrap();
    }

    #[test]
    fn checkpoint_truncates_and_preserves_data() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        let rid = heap.insert(Tuple::new(vec![4,5,6])).unwrap();
        txn.commit(t1).unwrap();
        txn.checkpoint().unwrap();
        let next = wal.next_lsn.load(Ordering::SeqCst);
        assert!(walk_log(&wal).is_empty());
        assert_eq!(wal.base_lsn.load(Ordering::SeqCst), next);
        assert_eq!(metadata(&wal.path).unwrap().len(), 24);
        assert_eq!(heap.read(rid).unwrap().data, vec![4,5,6]);

        let t2 = txn.begin().unwrap();
        let recs = walk_log(&wal);
        assert_eq!(recs[0].lsn, next);
        txn.abort(t2).unwrap();
    }

    #[test]
    fn interrupted_truncation_self_heals_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.wal");
        let new_base;
        {
            let wal = WalManager::new(path.clone()).unwrap();
            wal.append(1, 0, &RecKind::Begin).unwrap();
            wal.append(1, 0, &RecKind::Commit).unwrap();
            wal.flush().unwrap();
            new_base = wal.next_lsn.load(Ordering::SeqCst);

            let mut header = [0u8; 24];
            header[0..4].copy_from_slice(&0xF3_EE_DB_01u32.to_be_bytes());
            header[4..8].copy_from_slice(&2u32.to_be_bytes());
            header[8..16].copy_from_slice(&new_base.to_be_bytes());
            header[16..24].copy_from_slice(&1u64.to_be_bytes());
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            pwrite_all(&f, &mut header, 0).unwrap();
        }
        let wal = WalManager::new(path.clone()).unwrap();
        assert_eq!(metadata(&path).unwrap().len(), 24);
        assert_eq!(wal.next_lsn.load(Ordering::SeqCst), new_base);
        let lsn = wal.append(2, 0, &RecKind::Begin).unwrap();
        assert_eq!(lsn, new_base);
    }

    #[test]
    fn torn_tail_trimmed_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path =  dir.path().join("torn.wal");
        let (l0, l1);
        {
            let wal = WalManager::new(path.clone()).unwrap();
            l0 = wal.append(1, 0, &RecKind::Begin).unwrap();
            l1 = wal.append(1, l0, &RecKind::HeapInsert { dir_root: 1, page_id: 1, slot: 0, tuple: vec![1,2,3,4,5,6] }).unwrap();
            wal.flush().unwrap();
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            let len = f.metadata().unwrap().len();
            f.set_len(len - 3).unwrap();
        }
        let wal = WalManager::new(path.clone()).unwrap();
        assert_eq!(wal.next_lsn.load(Ordering::SeqCst), l1);
        assert!(wal.read_record(l0).is_ok());
    }

    #[test]
    fn test_begin_creates_snapshot() {
        let (_bp, _wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let t2 = txn.begin().unwrap();
        let att = txn.att.lock().unwrap();
        let snapshot = att[&t2].snapshot.as_ref().unwrap();
        assert_eq!(snapshot.high_water, t2);
        assert!(snapshot.active.contains(&t1));
        assert!(!snapshot.active.contains(&t2));
    }

    /// **The resume point must reach back over a transaction that was already in flight.**
    ///
    /// Its records sit below the point the read was taken at, and its work is *not* in the
    /// snapshot, so a stream told to start at the read would meet its `Commit` having never seen
    /// its changes and drop them without a trace.
    #[test]
    fn a_snapshot_read_resumes_at_the_oldest_transaction_it_excluded() {
        let (bp, wal, txn, _dir) = setup();

        // Committed before anything else: in the snapshot, and behind the resume point.
        let early = txn.begin().unwrap();
        txn.commit(early).unwrap();

        // Still open when the snapshot is taken: excluded from it, and its records are older than
        // the read.
        let open = txn.begin().unwrap();
        let open_begin = txn.att.lock().unwrap()[&open].begin_lsn;
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), open);
        heap.insert(Tuple::new(vec![1, 2, 3])).unwrap();

        let handoff = txn.begin_snapshot_read().unwrap();

        assert_eq!(
            handoff.resume_lsn, open_begin,
            "the resume point did not reach back to the open transaction's Begin, so its changes \
             sit below where the stream would start"
        );
        assert!(
            !handoff.snapshot.includes(open),
            "an in-flight transaction was reported as already in the snapshot"
        );
        assert!(
            handoff.snapshot.includes(early),
            "a transaction that committed before the snapshot was not reported as in it"
        );
        assert!(
            !handoff.snapshot.includes(handoff.txn_id + 1),
            "a transaction that has not started yet was reported as already in the snapshot"
        );
        assert!(
            wal.flushed_lsn.load(Ordering::SeqCst) >= handoff.resume_lsn,
            "the resume point is not durable, so a crash would move the frontier back over it"
        );

        txn.end_read_only(handoff.txn_id).unwrap();
        txn.abort(open).unwrap();
    }

    /// With nothing in flight there is nothing to reach back for, and the resume point is the
    /// reader's own `Begin`. Without this, the test above would pass just as well against a resume
    /// point that always ran to the start of the log.
    #[test]
    fn a_quiet_database_resumes_at_the_readers_own_begin() {
        let (_bp, wal, txn, _dir) = setup();
        let before = wal.next_lsn.load(Ordering::SeqCst);
        let handoff = txn.begin_snapshot_read().unwrap();
        assert_eq!(
            handoff.resume_lsn, before,
            "a quiet database resumed somewhere other than the reader's own Begin"
        );
        assert!(handoff.snapshot.active.is_empty());
        txn.end_read_only(handoff.txn_id).unwrap();
    }

    /// A snapshot reader that writes is refused, and rolled back rather than left open. Its writes
    /// would be invisible to its own snapshot, so the rows it wrote out would not be the state it
    /// claims to describe.
    #[test]
    fn a_snapshot_reader_that_writes_is_refused_and_rolled_back() {
        let (bp, _wal, txn, _dir) = setup();
        let handoff = txn.begin_snapshot_read().unwrap();

        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), handoff.txn_id);
        heap.insert(Tuple::new(vec![7, 7])).unwrap();

        let err = txn.end_read_only(handoff.txn_id).expect_err("a writing reader was accepted");
        assert!(format!("{err}").contains("rolled back"), "wrong reason: {err}");
        assert!(
            !txn.att.lock().unwrap().contains_key(&handoff.txn_id),
            "the refused reader was left open, which blocks every future checkpoint"
        );
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert!(rows.is_empty(), "the reader's write survived its rollback");
    }

    /// **A snapshot read that fails must not leave its reader behind.**
    ///
    /// This is the expensive one. The reader is inserted into `att` before anything that can fail,
    /// and its id is not handed out until the call succeeds — so a failure after the insert leaves
    /// an active transaction nobody can name, let alone close. `checkpoint` refuses while `att` is
    /// non-empty, so the database never checkpoints again: every later CREATE TABLE, CREATE INDEX
    /// and CLI shutdown fails with "checkpoint with active txns" for the life of the process. One
    /// failed snapshot takes the whole database with it.
    ///
    /// Reaching the failure needs the log truncated out from under an open transaction, which
    /// `checkpoint` will not do — defending `att` is precisely its job — so the WAL is told
    /// directly. That is fault injection rather than a scenario, and it is the point: this path is
    /// rare, and a rare path that wedges the process permanently is worth a deliberate test.
    #[test]
    fn a_snapshot_reader_is_not_leaked_when_its_handoff_cannot_be_pinned() {
        let (_bp, wal, txn, _dir) = setup();

        // Its `Begin` is what `begin_snapshot_read` will choose as the resume point, being the
        // oldest still in flight.
        let open = txn.begin().unwrap();

        // The log's base moves to `next_lsn`, which is already past that `Begin`, so pinning the
        // resume point must fail.
        wal.truncate(txn.next_txn_id.load(Ordering::SeqCst)).unwrap();
        assert!(
            wal.base_lsn.load(Ordering::SeqCst) > txn.att.lock().unwrap()[&open].begin_lsn,
            "the log was not truncated past the open transaction, so this test proves nothing"
        );

        let err = txn.begin_snapshot_read().expect_err("a handoff below the log's base was given out");
        assert!(format!("{err}").contains("truncated"), "wrong reason: {err}");

        // The failed reader is gone; only the transaction this test opened is still active.
        let live: Vec<u64> = txn.att.lock().unwrap().keys().copied().collect();
        assert_eq!(
            live,
            vec![open],
            "the failed snapshot left its reader in the active transaction table"
        );

        // And the consequence, asserted as a consequence rather than as internal state: with the
        // reader leaked this refuses for ever. Committed rather than aborted only because an abort
        // walks its undo chain, and this test just truncated that chain away.
        txn.commit(open).unwrap();
        txn.checkpoint()
            .expect("a failed snapshot left a reader open, so checkpoints are blocked for good");
    }

    /// **The other half of the same leak: closing the reader fails, and it still has to go.**
    ///
    /// `end_read_only` used to append its `TxnEnd` with `?` and only then remove the entry, so a
    /// failed append returned an error to a caller who had already handed the transaction over.
    /// Retrying means calling back into a WAL that just refused; not retrying leaves the reader
    /// active for ever. Either way `checkpoint` refuses from then on and the database never
    /// truncates its log again.
    ///
    /// `WalManager::append` cannot fail on its own — it extends a buffer and returns `Ok` — so this
    /// drives it through the test-only one-shot lever rather than pretending the path is reachable
    /// by ordinary means. Without the lever the guard could not be shown to work at all.
    #[test]
    fn a_reader_is_not_leaked_when_its_txn_end_cannot_be_written() {
        let (_bp, wal, txn, _dir) = setup();
        let handoff = txn.begin_snapshot_read().unwrap();
        let reader = handoff.txn_id;

        // Arm the failure for exactly the `TxnEnd` this close is about to write.
        wal.fail_next_append.store(true, Ordering::SeqCst);

        let err = txn.end_read_only(reader).expect_err("a failed TxnEnd was reported as success");
        assert!(format!("{err}").contains("injected"), "wrong reason: {err}");

        // **The error is reported AND the reader is gone.** Reporting it is not the hard part.
        assert!(
            !txn.att.lock().unwrap().contains_key(&reader),
            "the reader survived a failed close, so every later checkpoint is refused"
        );

        // The consequence that makes it matter. The handoff still holds a pin at the resume point,
        // so it is dropped first - otherwise this fails for the unrelated reason that the log is
        // legitimately claimed, and would pass just as well with the reader leaked.
        drop(handoff);
        txn.checkpoint().expect("a leaked reader is blocking every checkpoint");
    }

    /// **Transaction id 0 is never handed out, and something now depends on that.**
    ///
    /// The change feed treats id 0 as "not attributed to a transaction" — DDL is logged under it —
    /// and the snapshot-to-stream filter exempts those events, because a snapshot's transaction set
    /// says nothing about a schema declaration. If `begin` ever returned 0, that exemption would
    /// silently start applying to a real transaction's rows and re-deliver them after a cutover.
    /// The invariant comes from the WAL header's initial `next_txn_id` of 1, which is a long way
    /// from here, so it is asserted here rather than assumed.
    #[test]
    fn transaction_ids_never_start_at_zero() {
        let (_bp, _wal, txn, _dir) = setup();
        let first = txn.begin().unwrap();
        assert_ne!(first, 0, "the first transaction was given the id the feed reserves for DDL");
        txn.commit(first).unwrap();

        let handoff = txn.begin_snapshot_read().unwrap();
        assert_ne!(handoff.txn_id, 0);

        // The raw rule genuinely does say "contained" for 0 — it is arithmetic over timestamps, and
        // MVCC visibility needs it to keep saying that. The cutover asks a different question.
        assert!(handoff.snapshot.includes(0));
        assert!(
            !handoff.snapshot.already_delivered(0),
            "an event with no transaction author was treated as already delivered by the snapshot; \
             a stream would suppress every schema declaration"
        );
        assert!(
            handoff.snapshot.already_delivered(first),
            "a transaction that committed before the snapshot was not treated as delivered by it"
        );
        txn.end_read_only(handoff.txn_id).unwrap();
    }

    /// **The high water mark is exclusive, and nothing in the matrix below pins that.**
    ///
    /// Every case there that sits exactly on the mark is also the view's own transaction, so it
    /// passes through the `ts == txn_id` branch and an off-by-one in the comparison goes unseen.
    /// A statement-level view has `txn_id` 0 and `high_water` set to the *next* id to be handed
    /// out, so making the bound inclusive would show a reader the work of a transaction that had
    /// not even begun, let alone committed.
    #[test]
    fn the_high_water_mark_is_exclusive() {
        let snapshot = Snapshot { high_water: 10, active: HashSet::new() };
        assert!(!snapshot.includes(10), "the transaction at the mark was reported as included");
        assert!(snapshot.includes(9), "the transaction below the mark was reported as excluded");

        // Through a view whose own id is not the mark, so the `ts == txn_id` branch cannot mask it.
        let view = ReadView { snapshot, txn_id: 0 };
        let h = |b, e| VersionHeader { begin_ts: b, end_ts: e, prev_page: 0, prev_slot: 0 };
        assert!(
            !view.visible(&h(10, 0)),
            "a version written by the next transaction to begin was visible before it existed"
        );
        assert!(view.visible(&h(9, 0)));
    }

    #[test]
    fn test_visibility_matrix() {
        let view = ReadView { snapshot: Snapshot { high_water: 10, active: HashSet::from([7])}, txn_id: 10};
        let h = |b, e| VersionHeader { begin_ts: b, end_ts: e, prev_page: 0, prev_slot: 0};
        assert!(view.visible(&h(10, 0)));
        assert!(view.visible(&h(5, 0)));
        assert!(!view.visible(&h(7, 0)));
        assert!(!view.visible(&h(12, 0)));
        assert!(!view.visible(&h(5,6)));
        assert!(view.visible(&h(5, 12)));
        assert!(view.visible(&h(5, 7)));
        assert!(!view.visible(&h(5, 10)));
    }
}