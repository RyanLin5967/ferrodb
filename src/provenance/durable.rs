//! A [`ProvenanceStore`] that survives the process.
//!
//! # The gap this closes
//!
//! [`MemProvenanceStore`] was the only implementation, and all three `AgentRuntime` constructors
//! built one — including `reopen_with_storage`, whose entire job is to attach to a tree another
//! process wrote. So a database could be reopened with every row intact and `who_wrote_row`
//! answering *nothing* about any of them: exit criterion 9 held for as long as one process stayed
//! alive and not one moment longer. Attribution that evaporates on restart is not attribution; it
//! is a cache of it.
//!
//! **Status, corrected 2026-08-28.** This header used to say "nothing constructs it yet" and to
//! give three line numbers to wire it at; both statements are stale and the line numbers have
//! moved. What is true now:
//!
//! * `AgentRuntime::with_durable_provenance` exists (E79) and `src/cli/cli.rs` uses it, so the CLI
//!   really does get a durable store. So does `examples/pgserver.rs` since D246: until then the
//!   server ran in memory, reissued slots from 1 after every restart, and left a log that declared
//!   one slot for two actors.
//! * The three plain constructors still default to `MemProvenanceStore`. That is deliberate — a
//!   constructor that takes page stores is not given a database's name, so the layer that owns the
//!   path applies it — and it means a runtime built any other way is still in-memory.
//! * **The half criterion 9 is written against is now closed too (row E79c, 2026-09-03).**
//!   `AgentRuntime::who_wrote_row` used to read `State::row_author` and `State::runs`, two
//!   in-memory maps, and so did `authors_of` and the `ferro_row_authors` view — so the stamps
//!   below survived a restart while the *question* "who wrote this row" did not. Those two maps
//!   are gone; all three read this store. `State` no longer keeps a copy of anything answered
//!   here, so there is nothing left that has to be kept in step.
//!
//! There was a key-space mismatch behind that gap, and resolving it was the real work in E79c
//! rather than an oversight: `Stamp` is keyed by the **physical** `(page_id, slot_num)` because
//! that is what the executor knows when it writes, while row authorship is asked about the
//! **logical** `(table, row)`. `DESIGN.md` is explicit that `RowId` is the immutable surrogate and
//! physical position is not identity, so the logical key is the one attribution survives on — and
//! `RowAuthor` below is a record kind of its own rather than a reinterpretation of `Stamp`,
//! because neither key derives from the other and a row that moves pages must keep its author.
//!
//! # Shape: an append-only log, replayed on open
//!
//! Four record kinds, all small:
//!
//! * **`Run`** — one interned [`RunEntity`], written the first time a run is interned. One per
//!   *run*, never per row. A fork's run is interned PENDING since D246 §6.1 and its record is
//!   written and group-synced by the fork's `complete()`, after pgwire's catalog guard; see
//!   `intern_pending` and `await_run`.
//! * **`Stamp`** — `(page_id, slot_num) -> ProvId`, ten bytes of payload, written once per stamped
//!   version.
//! * **`RowAuthor`** — `(table_id, row_id) -> ProvId`, sixteen bytes, written once per op a merge
//!   applies (so a row updated in two columns gets two identical records), and all of one merge's
//!   in ONE append and ONE fsync since D219. This is the record `who_wrote_row` answers from.
//! * **`ForgetTable`** — one `table_id`, written on `DROP TABLE`. Recorded rather than applied only
//!   in memory because `table_id` hashes the table's NAME: without it, a reopen replays a dropped
//!   table's authorship and hands it to whatever table next takes that name.
//!
//! Only `Run` records are applied in id order on open; everything else is replayed in **file**
//! order, because a `ForgetTable` must erase the `RowAuthor` records before it and leave the ones
//! after it alone.
//!
//! That is the interning claim made durable rather than re-argued: the fat actor tuple is written
//! once per run and every version costs a fixed ten-byte reference to it. The claim is *measured*
//! with the pair that already exists for it — [`MemProvenanceStore::footprint_bytes`] against
//! [`MemProvenanceStore::literal_footprint_bytes`] — because this store keeps a `MemProvenanceStore`
//! as its in-memory index rather than reimplementing one. A second density counter written here
//! would be a second thing to keep true.
//!
//! **Append-only, not a rewritten snapshot.** A snapshot file has to be written completely or the
//! previous one is lost, which is a torn-write hazard on every stamp; an append that is torn loses
//! only its own tail, and the reader stops there. The cost, stated rather than discovered later, is
//! that the file grows with re-stamps of the same version and is never compacted. Nothing here
//! compacts it.
//!
//! # What a torn tail does
//!
//! Exactly what the WAL does with one: the scan stops at the first record whose length or CRC does
//! not hold up, the file is truncated back to the last good byte, and **the number of discarded
//! bytes is reported** through [`DurableProvenanceStore::discarded_tail_bytes`]. A store that
//! silently swallowed a partial write would answer "unattributed" for rows it had been told about,
//! which is the failure this module exists to prevent, arriving by another door.
//!
//! Damage that is *not* at the tail is a different thing and is refused rather than healed: a
//! `Stamp` naming a run the file never declared means the file disagrees with itself, and guessing
//! which half is right would produce confident wrong attribution.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::branch::group_commit::CommitGroup;
use crate::branch::types::BranchId;
use crate::error::FerroError;
use crate::provenance::store::MemProvenanceStore;
use crate::provenance::{ProvId, ProvenanceStore, RunEntity, SyncCounts};
use crate::storage::heap_file_manager::RecordId;
use crate::wal::log::{
    crc32, pread_all, pwrite_all, take_array, take_str, take_u16, take_u32, take_u64, take_u8,
    write_str,
};

const MAGIC: u32 = 0xF3_EE_50_01;
const VERSION: u32 = 1;
const HEADER_SIZE: u64 = 8;
/// `total_len(4) + tag(1) + crc(4)`: the smallest frame that can exist.
const MIN_FRAME: usize = 9;

const TAG_RUN: u8 = 1;
const TAG_STAMP: u8 = 2;
/// A LOGICAL row attribution, `(table_id, row_id) -> ProvId`. Sixteen bytes of payload.
///
/// A third record kind rather than a reinterpretation of `Stamp`, because the two answer different
/// questions and neither derives from the other: `Stamp` is keyed by physical `(page, slot)` and a
/// row that moves pages gets a new one, while this is keyed by the immutable surrogate and must
/// follow the row. See the row-attribution section of the `ProvenanceStore` trait.
const TAG_ROW_AUTHOR: u8 = 3;
/// A whole table's row attributions forgotten, because the table was dropped.
///
/// Recorded rather than applied only in memory: without it a reopen replays every `RowAuthor` the
/// dropped table ever had, and since `table_id` hashes the table NAME, a table recreated under the
/// same name inherits an author that never touched it. That is the precise defect
/// `AgentRuntime::forget_table` was added for, arriving by way of the file instead of the map.
const TAG_FORGET_TABLE: u8 = 4;

/// What opening the file recovered, and what it threw away.
///
/// Returned rather than logged: "the store opened" and "the store opened and discarded a partial
/// write" are different facts, and only the caller knows whether the second one matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoveryReport {
    pub runs: usize,
    pub stamps: usize,
    /// Logical `(table, row) -> run` records replayed. Counted apart from `stamps` because they
    /// are a different question about a different key space, and a total would hide one behind the
    /// other — a file full of physical stamps and no row authorship replays as "healthy" while
    /// `who_wrote_row` answers nothing, which is the exact shape E79c closed.
    pub row_authors: usize,
    /// `DROP TABLE` records replayed.
    pub forgets: usize,
    /// Bytes of a torn tail that were discarded. Non-zero means the process that wrote this file
    /// died mid-append.
    pub discarded_tail_bytes: u64,
}

/// A provenance store backed by a file, so attribution outlives the process that recorded it.
#[derive(Debug)]
pub struct DurableProvenanceStore {
    /// The in-memory index. Deliberately the existing, tested implementation rather than a second
    /// one: every guard it enforces (a page dictionary that refuses to widen past a byte, a
    /// re-intern with a different actor tuple, a stamp with `ProvId::NONE`) applies here unchanged,
    /// and the density instruments are the same ones.
    mem: MemProvenanceStore,
    /// Serialises appends, and is held across `mem`'s mutation so the file and the index cannot
    /// disagree about which runs exist. Lock order is file -> mem, everywhere, without exception.
    ///
    /// It also guards the PENDING records (D219): records already in `mem` whose frames have not
    /// been written yet. One lock over both is what keeps file order equal to index order — see
    /// [`Appender`].
    file: Mutex<Appender>,
    path: PathBuf,
    recovery: RecoveryReport,
    /// Set when an append failed after the in-memory index had already been changed.
    ///
    /// At that instant the store knows something its file does not, and every later write would
    /// deepen the disagreement: a stamp appended for a run whose record never landed produces a
    /// file the next `open` must refuse outright. So the store stops accepting writes and says why,
    /// rather than continuing to look healthy while building a file that cannot be reopened. Reads
    /// keep working — what is already known is still true.
    poisoned: AtomicBool,
    /// fsyncs issued, by record kind: the instrument behind [`ProvenanceStore::sync_counts`].
    syncs: SyncCounters,
    /// **D246: the group commit behind [`ProvenanceStore::await_run`]**, `branch/group_commit.rs`'s
    /// own, reused rather than copied.
    ///
    /// Its tickets are RECORD NUMBERS: every pending record takes one when it is written, under
    /// `file`'s lock, so `requested` is always the number of queued records written so far. A
    /// ticket being durable says that record and every one before it are on disk. A synchronous
    /// append marks everything it wrote durable (`covered_all`), so a fork whose run record a
    /// MERGE's sync already carried pays nothing more.
    group: CommitGroup,
    /// A second open of the same file, for the sync `await_run` issues OUTSIDE `file`'s lock. An
    /// fsync is per file, not per descriptor, so it covers every write made through `file`; and as
    /// its own open file description it is told of a writeback error independently of `file`
    /// (Linux ≥ 4.13 reports one to every description open when it happened).
    sync_handle: File,
    /// **Test-only: hold `await_run`'s group sync in flight until the test releases it.** The first
    /// sender is told the sync has been reached; the sync waits on the receiver (up to 30 s). One-shot.
    ///
    /// Without it, "the sync is not issued under the lock every staged write needs" cannot be
    /// observed: an fsync of a few bytes returns before any other thread can be seen waiting on it.
    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    pub(crate) sync_gate: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    /// **Test-only: make the next append fail once.**
    ///
    /// `append_locked` cannot fail on its own in any test — it writes a few dozen bytes to a temp
    /// file and fsyncs — which makes the poison guard above unreachable, and an unreachable guard is
    /// one nobody can show works. Its cost if it were wrong is the whole point of this module: a
    /// store that kept accepting stamps after an append failed would build a file whose next `open`
    /// must refuse outright, so every row it had attributed would come back unattributed.
    ///
    /// The same lever exists on `WalManager` for the same reason, with the same `#[cfg(test)]`
    /// scope: it does not exist in any shipped build, nor in integration tests.
    #[cfg(test)]
    pub(crate) fail_next_append: AtomicBool,
}

impl DurableProvenanceStore {
    /// Open a store at `path`, creating it if absent and replaying whatever is there.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FerroError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| FerroError::Provenance(format!("open {}: {e}", path.display())))?;
        let len = file
            .metadata()
            .map_err(|e| FerroError::Provenance(e.to_string()))?
            .len();

        // Its OWN open file description, not `try_clone()` (PREREG A4c, R2-D1): a clone shares
        // one error cursor with the append descriptor, so an fsync error could be reported to the
        // group leader and never to an in-lock writer whose pages it was, or the reverse. Write
        // access, because Windows flushes only a handle opened for writing.
        let sync_handle = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| FerroError::Provenance(format!("open {}: {e}", path.display())))?;
        let mem = MemProvenanceStore::new();
        let recovery = if len == 0 {
            let mut header = [0u8; HEADER_SIZE as usize];
            header[0..4].copy_from_slice(&MAGIC.to_be_bytes());
            header[4..8].copy_from_slice(&VERSION.to_be_bytes());
            pwrite_all(&file, &header, 0)
                .map_err(|e| FerroError::Provenance(format!("write header: {e}")))?;
            file.sync_all()
                .map_err(|e| FerroError::Provenance(e.to_string()))?;
            RecoveryReport::default()
        } else {
            Self::replay(&file, len, &mem, &path)?
        };

        Ok(DurableProvenanceStore {
            mem,
            file: Mutex::new(Appender {
                file,
                pending: Vec::new(),
                queued: 0,
                run_seqs: HashMap::new(),
            }),
            path,
            recovery,
            poisoned: AtomicBool::new(false),
            syncs: SyncCounters::default(),
            group: CommitGroup::default(),
            sync_handle,
            #[cfg(test)]
            sync_gate: Mutex::new(None),
            #[cfg(test)]
            fail_next_append: AtomicBool::new(false),
        })
    }

    /// Replay the file into `mem`, healing a torn tail and refusing internal disagreement.
    fn replay(
        file: &File,
        len: u64,
        mem: &MemProvenanceStore,
        path: &Path,
    ) -> Result<RecoveryReport, FerroError> {
        if len < HEADER_SIZE {
            return Err(FerroError::Provenance(format!(
                "{} is {len} bytes, too short to hold a provenance file header",
                path.display()
            )));
        }
        let mut header = [0u8; HEADER_SIZE as usize];
        pread_all(file, &mut header, 0)
            .map_err(|e| FerroError::Provenance(format!("read header: {e}")))?;
        if u32::from_be_bytes(header[0..4].try_into().unwrap()) != MAGIC {
            return Err(FerroError::Provenance(format!(
                "{} does not begin with the provenance magic; refusing to read it as one",
                path.display()
            )));
        }
        let version = u32::from_be_bytes(header[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(FerroError::Provenance(format!(
                "{} is provenance format version {version}; this build reads version {VERSION}",
                path.display()
            )));
        }

        // Pass one: read every intact frame, stopping at the first that is not.
        //
        // Runs are separated out because they must be applied in ID order (pass two). Everything
        // else keeps FILE order in ONE list, and that is load-bearing rather than tidy: a
        // `ForgetTable` has to erase the `RowAuthor` records before it and leave the ones after it
        // alone, so sorting these into per-kind buckets and applying kind by kind would either
        // resurrect a dropped table's attribution or drop a recreated table's.
        let mut runs: Vec<(u64, RunEntity)> = Vec::new();
        let mut after: Vec<(u64, Frame)> = Vec::new();
        let mut offset = HEADER_SIZE;
        let good_end = loop {
            if offset + 4 > len {
                break offset;
            }
            let mut len_buf = [0u8; 4];
            if pread_all(file, &mut len_buf, offset).is_err() {
                break offset;
            }
            let total = u32::from_be_bytes(len_buf) as u64;
            if (total as usize) < MIN_FRAME || offset + total > len {
                break offset;
            }
            let mut frame = vec![0u8; total as usize];
            if pread_all(file, &mut frame, offset).is_err() {
                break offset;
            }
            let stored = u32::from_be_bytes(frame[total as usize - 4..].try_into().unwrap());
            if crc32(&frame[..total as usize - 4]) != stored {
                break offset;
            }
            let body = &frame[4..total as usize - 4];
            match Self::decode(body, offset)? {
                Frame::Run(run) => runs.push((offset, run)),
                other => after.push((offset, other)),
            }
            offset += total;
        };

        let discarded_tail_bytes = len - good_end;
        if discarded_tail_bytes > 0 {
            // Heal it the way the WAL heals a torn tail, so the next append does not write past
            // garbage and produce a file that stops being readable at the same point for ever.
            file.set_len(good_end)
                .map_err(|e| FerroError::Provenance(format!("truncate torn tail: {e}")))?;
            file.sync_all()
                .map_err(|e| FerroError::Provenance(e.to_string()))?;
        }

        // Pass two: intern in `prov_id` order.
        //
        // File order is NOT id order — two threads may be assigned 1 and 2 and reach the file in
        // the other order — and `MemProvenanceStore` hands out ids sequentially, so replaying in
        // file order would silently re-number every run and every stamp would then name the wrong
        // one. Sorting makes the ids reproduce exactly, and the check below proves they did rather
        // than assuming it.
        let run_count = runs.len();
        runs.sort_by_key(|(_, r)| r.prov_id.0);
        for (at, run) in runs {
            let got = mem.intern(&run)?;
            if got != run.prov_id {
                return Err(FerroError::Provenance(format!(
                    "{}: the run at offset {at} was recorded as {} but replays as {got}. The file \
                     is missing an earlier run record, so every stamp below this point would be \
                     attributed to the wrong run; refusing to open it.",
                    path.display(),
                    run.prov_id
                )));
            }
        }

        // Pass three: everything else, in FILE order. A version stamped twice must end on the
        // later value, a row re-published must end on its later author, and a `DROP TABLE` must
        // erase what came before it and nothing after — only file order says which is which.
        let mut stamp_count = 0usize;
        let mut row_author_count = 0usize;
        let mut forget_count = 0usize;
        for (at, frame) in after {
            let (rid, id) = match frame {
                Frame::Stamp(rid, id) => {
                    stamp_count += 1;
                    (rid, id)
                }
                Frame::RowAuthor { table, row, id } => {
                    row_author_count += 1;
                    if let Err(e) = mem.stamp_row(table, row, id) {
                        // Told apart the same way the stamp arm below does, and for the same
                        // reason: `stamp_row` refuses an id that was never interned, which means
                        // the file disagrees with itself, and that is a different report from any
                        // other refusal. `ProvId::NONE` is not a refusal here — it CLEARS — so a
                        // cleared row replays as a clear rather than as damage.
                        let known = mem.lookup(id).is_ok();
                        return Err(FerroError::Provenance(if known {
                            format!(
                                "{}: the row attribution at offset {at} names {id}, which this \
                                 file DID declare, so the file is intact — the store refused it \
                                 for another reason: {e}",
                                path.display()
                            )
                        } else {
                            format!(
                                "{}: the row attribution at offset {at} names {id}, which this \
                                 file never declared: {e}",
                                path.display()
                            )
                        }));
                    }
                    continue;
                }
                Frame::ForgetTable(table) => {
                    forget_count += 1;
                    mem.forget_table(table)?;
                    continue;
                }
                // Runs went to pass two; pass one never puts one in this list.
                Frame::Run(_) => unreachable!("a run record reached pass three"),
            };
            if let Err(e) = mem.stamp(rid, id) {
                // **Two different failures, told apart rather than both blamed on the file.**
                //
                // `stamp` refuses an id that was never interned AND a page whose dictionary has hit
                // its 255-entry cap. The first means the file disagrees with itself; the second says
                // nothing about the file at all, and reporting it as "this file never declared that
                // run" would send the reader looking for corruption that is not there.
                //
                // The second branch is **defensive rather than reachable through this store's own
                // API**: the cap is enforced at write time, so a file this build wrote cannot carry
                // a 256th run for one page. It exists for a file written by a build with a different
                // cap, or one edited by hand — and it is here rather than absent because a wrong
                // diagnosis costs more than an unused branch. That reachability is stated rather
                // than tested, because there is no honest way to test it from here.
                let known = mem.lookup(id).is_ok();
                return Err(FerroError::Provenance(if known {
                    format!(
                        "{}: the stamp at offset {at} names {id}, which this file DID declare, so \
                         the file is intact — the store refused the stamp for another reason: {e}",
                        path.display()
                    )
                } else {
                    format!(
                        "{}: the stamp at offset {at} names {id}, which this file never declared: {e}",
                        path.display()
                    )
                }));
            }
        }

        Ok(RecoveryReport {
            runs: run_count,
            stamps: stamp_count,
            row_authors: row_author_count,
            forgets: forget_count,
            discarded_tail_bytes,
        })
    }

    fn decode(body: &[u8], at_offset: u64) -> Result<Frame, FerroError> {
        let mut at = 0usize;
        let tag = take_u8(body, &mut at)?;
        match tag {
            TAG_RUN => {
                let prov_id = ProvId(take_u32(body, &mut at)?);
                if prov_id.is_none() {
                    return Err(FerroError::Provenance(format!(
                        "the run record at offset {at_offset} names ProvId::NONE, which means \
                         'unattributed' and cannot name a run"
                    )));
                }
                let agent_id = take_str(body, &mut at)?;
                let run_id = take_str(body, &mut at)?;
                let model = take_str(body, &mut at)?;
                let model_version = take_str(body, &mut at)?;
                let prompt_hash = take_array::<32>(body, &mut at)?;
                let started_at = take_u64(body, &mut at)?;
                let branch_id = take_u64(body, &mut at)?;
                let generation = take_u32(body, &mut at)?;
                Ok(Frame::Run(RunEntity::new(
                    prov_id,
                    agent_id,
                    run_id,
                    model,
                    model_version,
                    prompt_hash,
                    started_at,
                    BranchId::new(branch_id, generation),
                )))
            }
            TAG_STAMP => {
                let page_id = take_u32(body, &mut at)?;
                let slot_num = take_u16(body, &mut at)?;
                let prov_id = ProvId(take_u32(body, &mut at)?);
                Ok(Frame::Stamp(RecordId { page_id, slot_num }, prov_id))
            }
            TAG_ROW_AUTHOR => {
                let table = take_u32(body, &mut at)?;
                let row = take_u64(body, &mut at)?;
                // `ProvId::NONE` is legal here and is NOT rejected the way the run record rejects
                // it: this record kind carries "no run is on record for this row any more", and
                // refusing it would make a clear unreplayable — so a reopen would resurrect the
                // author the writer had deliberately removed.
                let id = ProvId(take_u32(body, &mut at)?);
                Ok(Frame::RowAuthor { table, row, id })
            }
            TAG_FORGET_TABLE => {
                let table = take_u32(body, &mut at)?;
                Ok(Frame::ForgetTable(table))
            }
            other => Err(FerroError::Provenance(format!(
                "unknown provenance record tag {other} at offset {at_offset}"
            ))),
        }
    }

    /// Append one framed record and fsync it.
    ///
    /// **Synchronous on every call, deliberately.** The whole claim of this module is that a stamp
    /// survives the process that made it; a buffered write that has not reached the disk survives
    /// a clean exit and nothing else, and the difference is invisible until the crash that matters.
    /// The cost is one fsync per append — per run interned by `intern`, per stamped version, per
    /// forget, per BATCH of row authorship, and per flush of pending stamps (D219) — which is the
    /// price of the guarantee rather than an oversight. A fork's run is the exception since D246: it
    /// is interned pending and synced by `await_run`, group-committed outside the lock.
    ///
    /// `issued_by` is the counter of the write path that issued the sync; see [`SyncCounters`].
    fn append_locked(
        &self,
        out: &mut Appender,
        body: &[u8],
        issued_by: &AtomicU64,
    ) -> Result<(), FerroError> {
        self.append_all_locked(out, &[body], issued_by)
    }

    /// Append every PENDING record and then every body, each as its own framed record, with ONE
    /// write and ONE fsync for all of them.
    ///
    /// This is `branch/group_commit.rs`'s rule with a single writer: every record of the group is
    /// written first and the sync is issued after the LAST of them, so the sync covers them all.
    /// A sync issued before the last write would acknowledge a record a crash can still lose, which
    /// is the one ordering that module calls its whole correctness argument.
    ///
    /// **Pending records go FIRST, whatever this append is for.** They reached the index before
    /// anything this call carries, so writing them first is what keeps file order equal to index
    /// order — and replay is in file order, so a pending stamp overtaken by a later one for the same
    /// slot would reopen as the earlier author.
    ///
    /// Each frame is byte-for-byte the frame a one-body call writes, and they are laid down in
    /// order, so replay, the torn-tail heal and the format are untouched: a torn batch is a torn
    /// tail like any other, and the reader keeps the intact prefix of it. Nothing to write is not a
    /// write: no sync, and nothing counted.
    fn append_all_locked(
        &self,
        out: &mut Appender,
        bodies: &[&[u8]],
        issued_by: &AtomicU64,
    ) -> Result<(), FerroError> {
        let written_pending = out.pending.len();
        if !self.write_locked(out, written_pending, bodies)? {
            return Ok(());
        }
        out.file
            .sync_data()
            .map_err(|e| FerroError::Provenance(e.to_string()))?;
        // Cleared only once the sync has RETURNED OK, and counted only then: a failed sync made
        // nothing durable. A failure leaves the store poisoned by the caller, so nothing can append
        // after the records it lost.
        out.pending.clear();
        issued_by.fetch_add(1, Ordering::Relaxed);
        // D246: the pending records just written take their numbers now, and this sync covered
        // them and every record written before them, including any an `await_run` wrote whose own
        // group sync is still in flight. Sound because every ticket is taken under this lock, which
        // is held from the write above to here, so no ticket can be handed out during the sync.
        self.group.tickets(written_pending as u64);
        self.group.covered_all();
        Ok(())
    }

    /// Write the first `n` PENDING records and then every body, each as its own framed record, in
    /// ONE write and with NO sync. Returns whether anything was written.
    ///
    /// Neither `pending` nor the group is touched: whether the write is covered by a sync issued
    /// here under the lock (`append_all_locked`) or by a group sync outside it (`await_run`) is the
    /// caller's decision, and so is when the records leave `pending`.
    fn write_locked(&self, out: &Appender, n: usize, bodies: &[&[u8]]) -> Result<bool, FerroError> {
        // One-shot, and it disarms itself, so a test can fail exactly the append it means to.
        #[cfg(test)]
        if self.fail_next_append.swap(false, Ordering::SeqCst) {
            return Err(FerroError::Provenance("injected provenance append failure".into()));
        }
        let all: Vec<&[u8]> =
            out.pending[..n].iter().map(Vec::as_slice).chain(bodies.iter().copied()).collect();
        if all.is_empty() {
            return Ok(false);
        }
        // Checked BEFORE the write, so a body this file's reader could not decode is refused rather
        // than appended: replay stops the whole open on an unknown tag.
        for body in &all {
            known_tag(body)?;
        }
        let end = out
            .file
            .metadata()
            .map_err(|e| FerroError::Provenance(e.to_string()))?
            .len();
        let mut frames = Vec::with_capacity(all.iter().map(|b| 4 + b.len() + 4).sum());
        for body in &all {
            let start = frames.len();
            let total = (4 + body.len() + 4) as u32;
            frames.extend_from_slice(&total.to_be_bytes());
            frames.extend_from_slice(body);
            let crc = crc32(&frames[start..]);
            frames.extend_from_slice(&crc.to_be_bytes());
        }
        pwrite_all(&out.file, &frames, end)
            .map_err(|e| FerroError::Provenance(format!("append to {}: {e}", self.path.display())))?;
        Ok(true)
    }

    /// The group sync behind `await_run`, issued by `CommitGroup::wait_durable`'s leader OUTSIDE the
    /// file lock, and booked under `runs`: everything `await_run` writes is run records.
    fn sync_runs(&self) -> Result<(), FerroError> {
        #[cfg(test)]
        {
            let gate = self.sync_gate.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.recv_timeout(std::time::Duration::from_secs(30));
            }
        }
        if let Err(e) = self.sync_handle.sync_data() {
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(FerroError::Provenance(format!("sync {}: {e}", self.path.display())));
        }
        // **Checked AFTER the fsync, under the file lock (A4, review D-1).** `sync_handle` shares
        // one open file description with the append descriptor, so on Linux an fsync error is
        // reported to whichever of them syncs first, once: an in-lock writer whose own fsync failed
        // can consume the error for pages this leader's records were in, and this fsync then
        // returns 0. Every in-lock writer holds the lock from its fsync to its `poisoned.store`,
        // so once this lock is taken, any error that writer consumed is visible here. The same
        // check refuses a follower the group makes leader after a failed sync, rather than trust a
        // second fsync the kernel may answer with success for pages it already dropped.
        let _file = self.file.lock().map_err(|_| {
            FerroError::Provenance(format!(
                "{}: the provenance lock was poisoned by a panicking writer; refusing to sync",
                self.path.display()
            ))
        })?;
        self.refuse_if_poisoned()?;
        // Booked only once it vouches for what it covered (PREREG A4c).
        self.syncs.runs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// `intern` and `intern_pending` as far as the record: the in-memory intern, and the new run's
    /// record body, or `None` for a repeat. **Call it with the file lock held and the poison flag
    /// checked**, so the index and the file cannot disagree about which runs exist.
    fn intern_locked(&self, run: &RunEntity) -> Result<(ProvId, Option<Vec<u8>>), FerroError> {
        // ⛔ EVERYTHING THAT CAN REFUSE IS ENCODED BEFORE ANYTHING IS MUTATED, and the order is
        // the correctness argument rather than a style choice.
        //
        // D154 gave `wal::log::write_str` a length guard, which made this function able to return
        // early for the first time. `self.mem.intern` below is a MUTATION. With the encode after
        // it — where it used to sit, and where a comment of mine claimed "the refusal costs
        // nothing here" — a refused run stayed in memory and never reached the file, so
        // `run_count`/`runs` answered for a run that disappears on the next `open`. Measured, not
        // reasoned about: `tests/d154_length_prefix_refusal.rs
        // ::the_durable_provenance_store_refuses_a_run_it_cannot_encode` failed 1 != 0 on exactly
        // that, and the claim that it cost nothing was wrong.
        //
        // Moving the encode earlier is NOT a second guard — it is the same guard, ahead of the
        // state it would otherwise have to undo. Nothing needs rolling back if nothing changed.
        //
        // ⚠ It costs one ~96-byte encode on the REPEAT path, which previously allocated nothing.
        // That is per transaction, not per row, behind a file lock and a mutex that both dwarf it.
        // Stated rather than hidden, because it is a real if small regression on the common path.
        let mut tail = Vec::with_capacity(96);
        write_str(&mut tail, &run.agent_id, "an agent id")?;
        write_str(&mut tail, &run.run_id, "a run id")?;
        write_str(&mut tail, &run.model, "a model name")?;
        write_str(&mut tail, &run.model_version, "a model version")?;
        tail.extend_from_slice(&run.prompt_hash);
        tail.extend_from_slice(&run.started_at.to_be_bytes());
        tail.extend_from_slice(&run.parent_branch.id.to_be_bytes());
        tail.extend_from_slice(&run.parent_branch.generation.to_be_bytes());

        let before = self.mem.run_count();
        let id = self.mem.intern(run)?;
        if self.mem.run_count() == before {
            // A repeat. `intern` is a lookup in that case, so there is nothing new to record.
            return Ok((id, None));
        }

        let mut body = Vec::with_capacity(tail.len() + 5);
        body.push(TAG_RUN);
        body.extend_from_slice(&id.0.to_be_bytes());
        body.extend_from_slice(&tail);
        Ok((id, Some(body)))
    }

    /// Refuse further writes once the file has fallen behind the index. See [`Self::poisoned`].
    fn refuse_if_poisoned(&self) -> Result<(), FerroError> {
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(FerroError::Provenance(format!(
                "{} could not be appended to, so this store now knows attribution its file does \
                 not; refusing further writes rather than building a file that cannot be reopened",
                self.path.display()
            )));
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// What opening this store recovered from the file, and what it discarded.
    pub fn recovery(&self) -> RecoveryReport {
        self.recovery
    }

    /// Bytes of a torn tail thrown away when this store was opened. Zero on a clean file.
    pub fn discarded_tail_bytes(&self) -> u64 {
        self.recovery.discarded_tail_bytes
    }

    // ---- the reads criterion 9 is stated in, delegated to the in-memory index ------------------

    /// Which agent + run + model wrote this version. The exit-criterion-9 answer, and the one that
    /// now survives a reopen.
    pub fn who_wrote(&self, rid: RecordId) -> Result<RunEntity, FerroError> {
        self.mem.who_wrote(rid)
    }

    pub fn describe_row(&self, rid: RecordId) -> String {
        self.mem.describe_row(rid)
    }

    pub fn rows_written_by(&self, id: ProvId) -> Result<Vec<RecordId>, FerroError> {
        self.mem.rows_written_by(id)
    }

    pub fn run_count(&self) -> usize {
        self.mem.run_count()
    }

    pub fn runs(&self) -> Result<Vec<RunEntity>, FerroError> {
        self.mem.runs()
    }

    pub fn page_dictionary_len(&self, page_id: u32) -> usize {
        self.mem.page_dictionary_len(page_id)
    }

    /// Bytes actually spent on provenance across every page — the interned side of the instrument
    /// pair. Not a new counter: this is [`MemProvenanceStore::footprint_bytes`].
    pub fn footprint_bytes(&self) -> usize {
        self.mem.footprint_bytes()
    }

    /// What the same attribution would have cost stored literally in every version header — the
    /// other half of the pair, likewise reused rather than reinvented.
    pub fn literal_footprint_bytes(&self) -> Result<usize, FerroError> {
        self.mem.literal_footprint_bytes()
    }
}

enum Frame {
    Run(RunEntity),
    Stamp(RecordId, ProvId),
    RowAuthor { table: u32, row: u64, id: ProvId },
    ForgetTable(u32),
}

/// The file, and the records the index already holds whose frames have not been written to it.
///
/// Behind ONE mutex, and that is the ordering argument rather than tidiness: a record is applied to
/// the index and queued here under this lock, and every append writes the queue before anything it
/// was called for, so no record can reach the file ahead of one that reached the index first.
#[derive(Debug)]
struct Appender {
    file: File,
    /// Bodies `stamp_pending` and `intern_pending` applied to the index and nobody has written yet.
    /// In index order.
    pending: Vec<Vec<u8>>,
    /// Records ever queued in `pending`. `pending[i]` is record number
    /// `queued - pending.len() + 1 + i`, so every record numbered at or below
    /// `queued - pending.len()` has been written. That count is also the group's `requested`.
    queued: u64,
    /// D246: the record number of each run `intern_pending` queued whose durability `await_run`
    /// has not yet seen. Removed once seen, so it holds the forks in flight, not every run.
    run_seqs: HashMap<ProvId, u64>,
}

/// Refuse a body whose tag this file's reader cannot decode: replay would stop the whole file there.
fn known_tag(body: &[u8]) -> Result<(), FerroError> {
    match body.first() {
        Some(&TAG_RUN) | Some(&TAG_STAMP) | Some(&TAG_ROW_AUTHOR) | Some(&TAG_FORGET_TABLE) => Ok(()),
        other => Err(FerroError::Provenance(format!(
            "refusing to append a provenance record with tag {other:?}: replay would stop the whole \
             file at it"
        ))),
    }
}

/// One counter per WRITE PATH, bumped once per sync that path issued.
///
/// Booked by the issuing call rather than by the frames a sync carried, so every sync is counted
/// exactly once and the fields sum to the syncs issued. The distinction matters since D219: a sync
/// carries every pending record along with its own, so a MERGE's physical stamps ride in the one
/// sync its row authorship issues and are booked under `row_authors` — or, for a merge with no row
/// to attribute, in the flush, booked under `stamps`.
#[derive(Debug, Default)]
struct SyncCounters {
    runs: AtomicU64,
    stamps: AtomicU64,
    row_authors: AtomicU64,
    forgets: AtomicU64,
}

impl SyncCounters {

    fn snapshot(&self) -> SyncCounts {
        SyncCounts {
            runs: self.runs.load(Ordering::Relaxed),
            stamps: self.stamps.load(Ordering::Relaxed),
            row_authors: self.row_authors.load(Ordering::Relaxed),
            forgets: self.forgets.load(Ordering::Relaxed),
        }
    }
}

impl ProvenanceStore for DurableProvenanceStore {
    fn intern(&self, run: &RunEntity) -> Result<ProvId, FerroError> {
        let id = {
            // The file lock is taken FIRST and held across BOTH the in-memory intern and the
            // append, so two threads cannot both observe "this run is new" and both write it, and
            // no stamp can slip between a run being interned and its record reaching the file. Lock
            // order is file -> mem, everywhere.
            let mut file = self.file.lock().unwrap();
            // Checked UNDER the lock, and that is the fix for a race rather than tidiness. Checked
            // before taking it, a writer that had already passed the check could be sitting on
            // `file.lock()` while another thread's append failed and poisoned the store; it would
            // then acquire the lock and append anyway, producing exactly the file the poison flag
            // exists to prevent — a `Stamp` naming a run whose `Run` record never landed, which the
            // next `open` must refuse whole. Check-then-act on shared state, which is the shape
            // that has produced six separate defects in this codebase.
            self.refuse_if_poisoned()?;
            let (id, body) = self.intern_locked(run)?;
            match body {
                Some(body) => {
                    if let Err(e) = self.append_locked(&mut file, &body, &self.syncs.runs) {
                        self.poisoned.store(true, Ordering::SeqCst);
                        return Err(e);
                    }
                    return Ok(id);
                }
                None if !file.run_seqs.contains_key(&id) => return Ok(id),
                None => id,
            }
        };
        // D246: a repeat of a run an `intern_pending` queued and nobody has awaited yet. This
        // method returns a DURABLE run, so it awaits the record, outside the file lock, which
        // `await_run` takes itself.
        self.await_run(id)?;
        Ok(id)
    }

    /// **D246: a run interned in the index NOW and written LATER, by `await_run`.**
    ///
    /// Everything `intern` refuses is refused here, at the same moment. Only the record waits: it
    /// joins `pending` with a number, and `run_seqs` remembers the number until `await_run` has
    /// seen it durable. Any durable write before that carries it anyway, in order: the pending
    /// records go first in every append.
    fn intern_pending(&self, run: &RunEntity) -> Result<ProvId, FerroError> {
        let mut file = self.file.lock().unwrap();
        // Under the lock; see `intern` for the race that checking it first opened.
        self.refuse_if_poisoned()?;
        let (id, body) = self.intern_locked(run)?;
        if let Some(body) = body {
            file.pending.push(body);
            file.queued += 1;
            let seq = file.queued;
            file.run_seqs.insert(id, seq);
        }
        Ok(id)
    }

    /// **D246: make a run's record durable with a group-committed sync issued outside the lock.**
    ///
    /// Under the file lock: if the record is still pending, write it, with every pending record
    /// ahead of it, and take their tickets. No sync is issued there. Then, with the lock RELEASED,
    /// wait in `CommitGroup::wait_durable`: one waiter leads and syncs, and every record written
    /// before its sync began is covered. So forks completing together share one sync, and a fork
    /// being staged meanwhile (which needs this lock under pgwire's catalog guard) never waits on
    /// the disk.
    ///
    /// **Only RUN records are ever left written-but-unsynced**, and that is what keeps `flush`'s
    /// "nothing pending, nothing to do" true for the stamps it answers for. The records ahead of
    /// this run's are written here only while they are all run records, which is every case the
    /// SQL shapes produce: a MERGE queues its stamps under the catalog guard and makes them durable
    /// before releasing it (its `ProvenanceFlush` on every exit, and a plain ALTER flushes what it
    /// queued even when a restamp is refused, A4), so no stamp is pending when a fork completes
    /// behind the guard. When a
    /// stamp IS queued ahead (the embedded API, with no guard serialising writers), the pending
    /// records are written and synced here under the lock instead, as any other append would.
    fn await_run(&self, id: ProvId) -> Result<(), FerroError> {
        let seq = {
            // Not `unwrap`: `ForkDurability`'s `Drop` calls this, and a panic in a drop that runs
            // during unwinding aborts the process.
            let mut file = self.file.lock().map_err(|_| {
                FerroError::Provenance(format!(
                    "{}: the provenance lock was poisoned by a panicking writer; refusing to sync",
                    self.path.display()
                ))
            })?;
            let Some(&seq) = file.run_seqs.get(&id) else {
                // Interned by `intern`, recovered from the file, or already seen durable here. A
                // durable run is vouched for even on a poisoned store (PREREG A4c, R2-D5): refusing
                // it refused every MERGE there, including ones that write no provenance, the shape
                // D219 F1 ruled out for ALTER. A failed `intern` returned `Err`, so no caller holds
                // the id of a run whose record never landed.
                return Ok(());
            };
            let written = file.queued - file.pending.len() as u64;
            if seq > written {
                // Before the write: appending after a failed append is what the poison flag exists
                // to stop, since a torn frame mid-file makes the reader drop everything after it.
                // A record written but not yet synced is refused by `sync_runs`' own check.
                self.refuse_if_poisoned()?;
                let runs_ahead =
                    file.pending.iter().take_while(|b| b.first() == Some(&TAG_RUN)).count();
                if runs_ahead as u64 >= seq - written {
                    // Every pending run record at the head of the queue, not just this one: a fork
                    // staged behind this one rides the same sync instead of paying its own.
                    if let Err(e) = self.write_locked(&file, runs_ahead, &[]) {
                        self.poisoned.store(true, Ordering::SeqCst);
                        return Err(e);
                    }
                    file.pending.drain(..runs_ahead).for_each(drop);
                    self.group.tickets(runs_ahead as u64);
                } else if let Err(e) = self.append_all_locked(&mut file, &[], &self.syncs.runs) {
                    self.poisoned.store(true, Ordering::SeqCst);
                    return Err(e);
                }
            }
            seq
        };
        self.group.wait_durable(seq, || self.sync_runs())?;
        // Durable, so the number is no longer needed. Removed here, which is what keeps this map
        // the size of the forks in flight rather than of every run ever interned.
        if let Ok(mut file) = self.file.lock() {
            file.run_seqs.remove(&id);
        }
        Ok(())
    }

    fn lookup(&self, id: ProvId) -> Result<RunEntity, FerroError> {
        self.mem.lookup(id)
    }

    fn attribute(&self, rid: RecordId) -> Result<ProvId, FerroError> {
        self.mem.attribute(rid)
    }

    /// Delegated, like every other read: `mem` is not a cache of the page dictionaries, it *is*
    /// them. `stamp` above writes memory first and the file second, so the two cannot disagree
    /// about which runs a page carries without the store already being poisoned.
    fn page_dictionary_lens(&self) -> Result<Vec<(u32, usize)>, FerroError> {
        self.refuse_if_poisoned()?;
        self.mem.page_dictionary_lens()
    }

    fn stamp(&self, rid: RecordId, id: ProvId) -> Result<(), FerroError> {
        let mut file = self.file.lock().unwrap();
        // Under the lock; see `intern` for the race that checking it first opened.
        self.refuse_if_poisoned()?;
        // In memory first: it holds the guards (an uninterned id, `ProvId::NONE`, a page dictionary
        // at its cap), and a refused stamp must not reach the file.
        self.mem.stamp(rid, id)?;
        if let Err(e) = self.append_locked(&mut file, &stamp_body(rid, id), &self.syncs.stamps) {
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(e);
        }
        Ok(())
    }

    /// **D219: a physical stamp applied to the index NOW and written LATER, with the next sync.**
    ///
    /// Everything `stamp` refuses is refused here, at the same moment — the guards live in `mem` and
    /// run before anything is queued — so a MERGE whose publish loop hits a full page dictionary
    /// still aborts its publish transaction, as it did when each stamp synced. Only the frame waits:
    /// it joins `pending`, and the next append of any kind writes it ahead of its own records.
    ///
    /// Until then the index knows a stamp the file does not, which is exactly the state the poison
    /// flag refuses to let an append FAILURE leave behind. Here it is transient by construction: the
    /// caller holds a `ProvenanceFlush` that writes it on every exit, and `Drop` writes whatever a
    /// caller left behind.
    fn stamp_pending(&self, rid: RecordId, id: ProvId) -> Result<(), FerroError> {
        let mut file = self.file.lock().unwrap();
        self.refuse_if_poisoned()?;
        self.mem.stamp(rid, id)?;
        file.pending.push(stamp_body(rid, id));
        file.queued += 1;
        Ok(())
    }

    /// Write and sync every pending record: one append, one sync, booked under `stamps` because
    /// pending records are physical stamps. Nothing pending is not a write — no sync, and no
    /// refusal even from a poisoned store. A lock poisoned by a PANIC is refused either way.
    fn flush(&self) -> Result<(), FerroError> {
        // Not `unwrap`, unlike every other write path: this one is called from two `Drop`s, and a
        // panic inside a drop that runs during unwinding aborts the process. A lock poisoned by a
        // panicking writer is refused like a poisoned store.
        let mut file = self.file.lock().map_err(|_| {
            FerroError::Provenance(format!(
                "{}: the provenance lock was poisoned by a panicking writer; refusing to flush",
                self.path.display()
            ))
        })?;
        if file.pending.is_empty() {
            return Ok(());
        }
        self.refuse_if_poisoned()?;
        if let Err(e) = self.append_all_locked(&mut file, &[], &self.syncs.stamps) {
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(e);
        }
        Ok(())
    }

    // ── Logical row attribution, appended and fsynced like a stamp ───────────────────────────────
    //
    // Same lock order (file -> mem), same poison-on-append-failure, same synchronous fsync — one
    // per call to `stamp_rows`, however many rows it carries (D219), where a stamp's is one per
    // version. The write paths differ only in what they encode and how many records share a sync,
    // and that is deliberate: a second durability strategy for the record kind that answers
    // criterion 9 would be a second thing to keep true.

    /// A batch of one: the same lock, guards, frame and single sync `stamp_row` always had.
    fn stamp_row(&self, table: u32, row: u64, id: ProvId) -> Result<(), FerroError> {
        self.stamp_rows(&[(table, row)], id)
    }

    /// **D219: a whole merge's authorship in ONE append and ONE fsync.**
    ///
    /// The records are exactly those δ `stamp_row` calls wrote — one per entry, repeats kept, in
    /// order — so what a reopen replays is unchanged. Only the number of syncs they share moved,
    /// from one per record to one per batch.
    ///
    /// **Crash semantics, before and after, for a crash after the publish transaction committed
    /// and before this returns.** The rows are durable either way (the WAL commit came first), and
    /// a reopen replays a PREFIX of the batch's records — possibly none, possibly all — because the
    /// reader stops at the first frame whose length or CRC does not hold. Before, each record had
    /// its own sync, so a crash mid-merge most likely left a nonempty proper prefix; now the one
    /// sync makes "none" and "all" the likely outcomes, with a proper prefix still possible when
    /// power fails during write-back of the one write. Neither is atomic per merge and neither
    /// ever was; the window in which a committed merge's rows can reopen under their previous
    /// author shrank from δ fsyncs to one.
    fn stamp_rows(&self, rows: &[(u32, u64)], id: ProvId) -> Result<(), FerroError> {
        if rows.is_empty() {
            // Nothing to record is not a write: not refused by a poisoned store, and no sync.
            return Ok(());
        }
        let mut file = self.file.lock().unwrap();
        self.refuse_if_poisoned()?;
        // In memory first, so a refused attribution (an id this store never interned) does not
        // reach the file and make the next `open` refuse the whole thing. `mem.stamp_rows` refuses
        // before touching any row, so a refusal leaves the index and the file agreeing.
        self.mem.stamp_rows(rows, id)?;
        let bodies: Vec<Vec<u8>> = rows
            .iter()
            .map(|&(table, row)| {
                let mut body = Vec::with_capacity(17);
                body.push(TAG_ROW_AUTHOR);
                body.extend_from_slice(&table.to_be_bytes());
                body.extend_from_slice(&row.to_be_bytes());
                body.extend_from_slice(&id.0.to_be_bytes());
                body
            })
            .collect();
        let bodies: Vec<&[u8]> = bodies.iter().map(Vec::as_slice).collect();
        if let Err(e) = self.append_all_locked(&mut file, &bodies, &self.syncs.row_authors) {
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(e);
        }
        Ok(())
    }

    fn row_author(&self, table: u32, row: u64) -> Result<ProvId, FerroError> {
        self.mem.row_author(table, row)
    }

    fn attributed_rows(&self, table: u32) -> Result<Vec<(u64, ProvId)>, FerroError> {
        self.mem.attributed_rows(table)
    }

    fn forget_table(&self, table: u32) -> Result<(), FerroError> {
        let mut file = self.file.lock().unwrap();
        self.refuse_if_poisoned()?;
        self.mem.forget_table(table)?;
        let mut body = Vec::with_capacity(5);
        body.push(TAG_FORGET_TABLE);
        body.extend_from_slice(&table.to_be_bytes());
        if let Err(e) = self.append_locked(&mut file, &body, &self.syncs.forgets) {
            // **The in-memory forget has already happened and cannot be undone**, so this store
            // now knows LESS than its file — the mirror image of the poison case `intern` and
            // `stamp` guard, and poisoned for the same reason. Reopening would replay the
            // attributions this call dropped and hand them to whatever table next takes the name.
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(e);
        }
        Ok(())
    }

    fn sync_counts(&self) -> SyncCounts {
        self.syncs.snapshot()
    }

    /// Refused when a write would not succeed: a store poisoned by a failed append, or a file lock
    /// poisoned by a panicking writer (where every write path but `flush` would panic on
    /// `lock().unwrap()` rather than refuse, so refusing here is the safer answer). Read without
    /// taking the lock — the trait says why the probe is advisory.
    fn check_writable(&self) -> Result<(), FerroError> {
        if self.file.is_poisoned() {
            return Err(FerroError::Provenance(format!(
                "{}: the provenance lock was poisoned by a panicking writer; refusing further writes",
                self.path.display()
            )));
        }
        self.refuse_if_poisoned()
    }
}

impl Drop for DurableProvenanceStore {
    /// The last chance for a pending record on a clean shutdown. A `ProvenanceFlush` is meant to
    /// have written it long before; one that did not left the index holding a stamp the file lacks,
    /// and dropping it silently would lose attribution the process had already reported. A failure
    /// here cannot be returned, and there is no later write for the poison flag to protect.
    fn drop(&mut self) {
        // `flush` refuses a poisoned lock rather than panicking on it, so this cannot abort an
        // unwinding process.
        let _ = self.flush();
    }
}

/// One physical `Stamp` record's body: tag, page, slot, run. The same bytes on both paths.
fn stamp_body(rid: RecordId, id: ProvId) -> Vec<u8> {
    let mut body = Vec::with_capacity(11);
    body.push(TAG_STAMP);
    body.extend_from_slice(&rid.page_id.to_be_bytes());
    body.extend_from_slice(&rid.slot_num.to_be_bytes());
    body.extend_from_slice(&id.0.to_be_bytes());
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provenance::sha256::prompt_digest;

    fn run(agent: &str, run_id: &str) -> RunEntity {
        RunEntity::new(
            ProvId::NONE,
            agent,
            run_id,
            "claude-opus",
            "2026-05",
            prompt_digest("restock everything below the reorder point"),
            1_700_000_000_000,
            BranchId::new(4, 0),
        )
    }

    fn rid(page: u32, slot: u16) -> RecordId {
        RecordId { page_id: page, slot_num: slot }
    }

    /// **Exit criterion: `who_wrote_row` answers after a reopen.**
    ///
    /// Breaking shape: any workload at all, as long as the process that wrote it is not the process
    /// that asks. `MemProvenanceStore` passes every provenance test in this repo and fails this
    /// one, because every one of those tests interns, stamps and asks inside a single store object.
    #[test]
    fn who_wrote_a_row_still_answers_after_a_close_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");

        let id = {
            let s = DurableProvenanceStore::open(&path).unwrap();
            let id = s.intern(&run("restock-agent", "run-42")).unwrap();
            s.stamp(rid(9, 3), id).unwrap();
            s.stamp(rid(9, 4), id).unwrap();
            // It answers before the reopen too — otherwise the assertion below would pass for a
            // store that simply never worked.
            assert_eq!(s.who_wrote(rid(9, 3)).unwrap().agent_id, "restock-agent");
            id
        };

        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(s.recovery().runs, 1, "the run record was not replayed");
        assert_eq!(s.recovery().stamps, 2, "the stamps were not replayed");
        assert_eq!(s.discarded_tail_bytes(), 0, "a clean file discarded bytes");

        let who = s.who_wrote(rid(9, 3)).unwrap();
        assert_eq!(who.agent_id, "restock-agent");
        assert_eq!(who.run_id, "run-42");
        assert_eq!(who.model, "claude-opus");
        assert_eq!(who.model_version, "2026-05");
        assert_eq!(who.prov_id, id);
        assert_eq!(
            who.prompt_hash,
            prompt_digest("restock everything below the reorder point"),
            "the prompt digest did not survive the round trip"
        );
        assert_eq!(s.attribute(rid(9, 4)).unwrap(), id);
        assert_eq!(s.rows_written_by(id).unwrap(), vec![rid(9, 3), rid(9, 4)]);

        // Anti-vacuity: a version nobody stamped is still unattributed after a reopen, so the
        // answers above are attribution and not a store that says yes to everything.
        assert_eq!(s.attribute(rid(9, 5)).unwrap(), ProvId::NONE);
        assert_eq!(s.describe_row(rid(9, 5)), "unattributed");
    }

    /// **Row E79c's exit criterion at the store layer**: the LOGICAL `(table, row) -> run` answer
    /// survives a reopen, not just the physical stamp.
    ///
    /// The two are genuinely different questions and the sibling test above cannot cover this one:
    /// a file can replay every `Stamp` perfectly and still answer nothing about a row, which is the
    /// exact state this repo shipped in until E79c.
    #[test]
    fn logical_row_authorship_survives_a_close_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");

        let id = {
            let s = DurableProvenanceStore::open(&path).unwrap();
            let id = s.intern(&run("restock-agent", "run-42")).unwrap();
            s.stamp_row(7, 1, id).unwrap();
            s.stamp_row(7, 2, id).unwrap();
            // Answers before the reopen, so the assertions after it are about durability and not
            // about a store that never worked.
            assert_eq!(s.row_author(7, 1).unwrap(), id);
            id
        };

        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(s.recovery().row_authors, 2, "the row attributions were not replayed");
        assert_eq!(s.recovery().forgets, 0);
        assert_eq!(s.discarded_tail_bytes(), 0, "a clean file discarded bytes");

        assert_eq!(s.row_author(7, 1).unwrap(), id);
        assert_eq!(s.lookup(s.row_author(7, 2).unwrap()).unwrap().agent_id, "restock-agent");
        assert_eq!(s.attributed_rows(7).unwrap(), vec![(1, id), (2, id)]);

        // Anti-vacuity: a row nobody published is still unattributed after the reopen.
        assert_eq!(s.row_author(7, 3).unwrap(), ProvId::NONE);
        assert_eq!(s.row_author(8, 1).unwrap(), ProvId::NONE);
    }

    /// A cleared attribution replays as a CLEAR, not as the author it replaced.
    ///
    /// `ProvId::NONE` is legal in a `RowAuthor` record precisely so this is expressible; decoding it
    /// as a damaged frame, or refusing it on the write path, would make a reopen resurrect an
    /// author the writer had deliberately removed.
    #[test]
    fn clearing_a_rows_author_survives_the_reopen_as_a_clear() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        {
            let s = DurableProvenanceStore::open(&path).unwrap();
            let id = s.intern(&run("restock-agent", "run-42")).unwrap();
            s.stamp_row(7, 1, id).unwrap();
            s.stamp_row(7, 2, id).unwrap();
            s.stamp_row(7, 1, ProvId::NONE).unwrap();
        }
        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(s.row_author(7, 1).unwrap(), ProvId::NONE, "a cleared author came back");
        assert_ne!(s.row_author(7, 2).unwrap(), ProvId::NONE, "the clear took the wrong row");
    }

    /// **The file-order claim, which is the subtle half of the replay.**
    ///
    /// A `ForgetTable` must erase the `RowAuthor` records BEFORE it and leave the ones AFTER it
    /// alone. Both ways of getting this wrong are caught here, and neither is caught by any other
    /// test: replaying all attributions and then all forgets loses row 2 (the recreated table's),
    /// and replaying all forgets and then all attributions resurrects row 1 (the dropped table's).
    /// `table_id` hashes the table NAME, so this is exactly a table dropped and recreated under the
    /// same name inheriting an author that never touched it.
    #[test]
    fn a_dropped_table_forgets_only_what_preceded_the_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let (old, new) = {
            let s = DurableProvenanceStore::open(&path).unwrap();
            let old = s.intern(&run("restock-agent", "run-42")).unwrap();
            s.stamp_row(7, 1, old).unwrap();
            s.stamp_row(8, 1, old).unwrap();
            s.forget_table(7).unwrap();
            let new = s.intern(&run("auditor", "run-99")).unwrap();
            s.stamp_row(7, 2, new).unwrap();
            (old, new)
        };

        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(s.recovery().forgets, 1, "the drop was not replayed");
        assert_eq!(s.recovery().row_authors, 3);
        assert_eq!(
            s.row_author(7, 1).unwrap(),
            ProvId::NONE,
            "the dropped table's authorship came back and now belongs to a table that reused its name"
        );
        assert_eq!(
            s.row_author(7, 2).unwrap(),
            new,
            "the drop erased an attribution written after it"
        );
        assert_eq!(s.row_author(8, 1).unwrap(), old, "an untouched table lost its authorship");
        assert_eq!(s.attributed_rows(7).unwrap(), vec![(2, new)]);
    }

    /// A run interned again after a reopen keeps its id. Attribution is run-level: a second session
    /// for one run that got a second id would split that run's rows across two entities.
    #[test]
    fn re_interning_after_a_reopen_returns_the_same_slot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let (a, b) = {
            let s = DurableProvenanceStore::open(&path).unwrap();
            (
                s.intern(&run("restock", "run-1")).unwrap(),
                s.intern(&run("auditor", "run-9")).unwrap(),
            )
        };
        assert_ne!(a, b);

        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(s.run_count(), 2);
        assert_eq!(s.intern(&run("restock", "run-1")).unwrap(), a);
        assert_eq!(s.intern(&run("auditor", "run-9")).unwrap(), b);
        assert_eq!(s.run_count(), 2, "a repeat intern created a second entity");

        // A third run appends and takes the next slot, so the id counter really was restored and
        // not merely happened to match.
        let c = s.intern(&run("planner", "run-3")).unwrap();
        assert_eq!(c, ProvId(3));
        drop(s);
        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(s.lookup(ProvId(3)).unwrap().agent_id, "planner");
    }

    /// The guards `MemProvenanceStore` enforces are the guards this store enforces, because it *is*
    /// that store underneath. A durable copy that quietly dropped one of them would be a second,
    /// weaker implementation wearing the same name.
    #[test]
    fn the_in_memory_guards_still_hold_and_a_refused_stamp_never_reaches_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let s = DurableProvenanceStore::open(&path).unwrap();
        let id = s.intern(&run("restock", "run-1")).unwrap();

        assert!(s.stamp(rid(1, 0), ProvId::NONE).is_err(), "NONE was stamped");
        assert!(s.stamp(rid(1, 0), ProvId(99)).is_err(), "an uninterned id was stamped");
        let mut lying = run("restock", "run-1");
        lying.model = "some-other-model".into();
        assert!(s.intern(&lying).is_err(), "a disagreeing actor tuple was interned");

        // Anti-vacuity: the legal case still lands, and lands durably.
        s.stamp(rid(1, 0), id).unwrap();
        drop(s);

        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(
            s.recovery().stamps,
            1,
            "a refused stamp was written to the file anyway; the reopened store would carry it"
        );
        assert_eq!(s.attribute(rid(1, 0)).unwrap(), id);
    }

    /// A version stamped twice ends on the LATER value after a reopen, so stamps must replay in
    /// file order. Sorting them the way run records are sorted would make the winner arbitrary.
    #[test]
    fn a_restamped_version_replays_to_the_later_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let (first, second) = {
            let s = DurableProvenanceStore::open(&path).unwrap();
            let a = s.intern(&run("restock", "run-1")).unwrap();
            let b = s.intern(&run("auditor", "run-2")).unwrap();
            s.stamp(rid(3, 7), a).unwrap();
            s.stamp(rid(3, 7), b).unwrap();
            assert_eq!(s.attribute(rid(3, 7)).unwrap(), b);
            (a, b)
        };
        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(
            s.attribute(rid(3, 7)).unwrap(),
            second,
            "the reopened store attributes the row to {first}, the run that was overwritten"
        );
    }

    /// **A torn tail is healed and REPORTED, never silently swallowed.**
    ///
    /// Breaking shape: a process killed between `pwrite` and its completion, leaving a partial
    /// frame. A store that stopped reading and said nothing would answer "unattributed" for rows it
    /// had been told about, which is the exact failure this module exists to prevent.
    #[test]
    fn a_partial_append_is_discarded_reported_and_then_written_over() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let id = {
            let s = DurableProvenanceStore::open(&path).unwrap();
            let id = s.intern(&run("restock", "run-1")).unwrap();
            s.stamp(rid(2, 2), id).unwrap();
            id
        };
        let clean_len = std::fs::metadata(&path).unwrap().len();

        // Half of another frame: a length prefix promising more bytes than are there.
        let mut torn = std::fs::read(&path).unwrap();
        torn.extend_from_slice(&999u32.to_be_bytes());
        torn.extend_from_slice(&[TAG_STAMP, 0, 0, 0, 5]);
        std::fs::write(&path, &torn).unwrap();

        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(
            s.discarded_tail_bytes(),
            torn.len() as u64 - clean_len,
            "the torn tail was not reported"
        );
        // Everything before the tear survived.
        assert_eq!(s.attribute(rid(2, 2)).unwrap(), id);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), clean_len, "the tail was not healed");

        // And the healed file accepts new appends that survive their own reopen — a truncation that
        // left the offset wrong would corrupt the next record instead of the last one.
        s.stamp(rid(2, 3), id).unwrap();
        drop(s);
        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(s.discarded_tail_bytes(), 0);
        assert_eq!(s.attribute(rid(2, 3)).unwrap(), id);
    }

    /// A stamp naming a run the file never declared is refused, not guessed at. This is damage in
    /// the MIDDLE of the file rather than at its tail, and healing it would mean inventing an
    /// attribution.
    #[test]
    fn a_stamp_naming_an_undeclared_run_is_refused_rather_than_healed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        {
            let s = DurableProvenanceStore::open(&path).unwrap();
            let id = s.intern(&run("restock", "run-1")).unwrap();
            s.stamp(rid(1, 1), id).unwrap();
        }
        // Rewrite the file's single stamp so it names ProvId(7), which was never interned. The
        // frame is re-CRCed so the reader believes the bytes.
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        // The last frame is the stamp: total(4) | tag(1) | page(4) | slot(2) | prov(4) | crc(4).
        let frame_len = 4 + 1 + 4 + 2 + 4 + 4;
        let start = n - frame_len;
        bytes[n - 8..n - 4].copy_from_slice(&7u32.to_be_bytes());
        let crc = crc32(&bytes[start..n - 4]);
        bytes[n - 4..].copy_from_slice(&crc.to_be_bytes());
        std::fs::write(&path, &bytes).unwrap();

        let err = DurableProvenanceStore::open(&path)
            .expect_err("a stamp naming an undeclared run was accepted");
        assert!(
            format!("{err}").contains("never declared"),
            "it failed, but not by this guard: {err}"
        );
    }

    /// A file that is not a provenance file is refused rather than read as an empty one — an empty
    /// store and a misidentified file both answer "unattributed" for everything, and only one of
    /// them is a fact about the database.
    #[test]
    fn a_foreign_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not-provenance");
        std::fs::write(&path, b"this is somebody else's file, quite a long way from empty").unwrap();
        let err = DurableProvenanceStore::open(&path).expect_err("a foreign file was opened");
        assert!(format!("{err}").contains("magic"), "{err}");

        // Anti-vacuity: an absent file is created, not refused.
        let fresh = dir.path().join("fresh.log");
        let s = DurableProvenanceStore::open(&fresh).expect("a fresh store was refused");
        assert_eq!(s.run_count(), 0);
    }

    /// **A store whose file has fallen behind it refuses further writes rather than looking healthy.**
    ///
    /// When an append fails, the in-memory index has already changed: the store knows an
    /// attribution its file does not. Every later write deepens the disagreement, and a stamp
    /// appended for a run whose record never landed produces a file the next `open` must refuse
    /// outright — so every row this store had attributed comes back unattributed. Refusing now, and
    /// saying why, is the only outcome that does not turn one failed write into a whole store.
    ///
    /// Breaking shape: an I/O failure on the append path. It cannot happen against a temp file,
    /// which is exactly why the failure is injected — the guard is otherwise unreachable and
    /// therefore unverifiable.
    #[test]
    fn a_failed_append_poisons_the_store_instead_of_leaving_it_looking_healthy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let s = DurableProvenanceStore::open(&path).unwrap();
        let id = s.intern(&run("restock", "run-1")).unwrap();
        s.stamp(rid(1, 0), id).unwrap();

        s.fail_next_append.store(true, Ordering::SeqCst);
        let err = s.stamp(rid(1, 1), id).expect_err("the injected append failure was swallowed");
        assert!(format!("{err}").contains("injected"), "{err}");

        // Every later write refuses, and names the reason rather than failing obscurely.
        let err = s.stamp(rid(1, 2), id).expect_err("a poisoned store accepted another stamp");
        assert!(
            format!("{err}").contains("refusing further writes"),
            "it refused, but not by this guard: {err}"
        );
        let err = s
            .intern(&run("auditor", "run-2"))
            .expect_err("a poisoned store interned another run");
        assert!(format!("{err}").contains("refusing further writes"), "{err}");

        // Reads still answer: what was already recorded is still true.
        assert_eq!(s.attribute(rid(1, 0)).unwrap(), id);
        assert_eq!(s.who_wrote(rid(1, 0)).unwrap().agent_id, "restock");

        // And the file is still one a later process can open — which is the outcome the refusal
        // bought. The stamp that failed is simply absent, rather than present without its run.
        drop(s);
        let s = DurableProvenanceStore::open(&path).expect("the file became unopenable anyway");
        assert_eq!(s.attribute(rid(1, 0)).unwrap(), id);
        assert_eq!(s.attribute(rid(1, 1)).unwrap(), ProvId::NONE);

        // Anti-vacuity: the reopened store is NOT poisoned, so the refusals above were about the
        // failure and not about this store never having accepted anything.
        s.stamp(rid(1, 1), id).expect("a freshly opened store refused a stamp");
    }

    /// **File order is not id order, and replaying in file order would renumber every run.**
    ///
    /// `MemProvenanceStore` hands out ids sequentially, so replay has to present the runs in the
    /// order that reproduces them. Two threads can be assigned ids 1 and 2 and reach the file in the
    /// other order — the id is assigned under the lock, the `pwrite` is not ordered with respect to
    /// another thread's — so the file genuinely can hold them backwards.
    ///
    /// Breaking shape: exactly that, a file whose run records are out of `prov_id` order. Every file
    /// written by a single-threaded caller is already in order and passes either way, which is why
    /// this is built by hand rather than hoped for from a concurrent workload.
    #[test]
    fn run_records_out_of_order_in_the_file_still_replay_to_their_recorded_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        // Equal-length names, so the two run frames are byte-identical in length and can be swapped
        // without re-framing anything.
        let first = RunEntity::new(ProvId::NONE, "aaa", "r1", "claude-opus", "2026-05",
            prompt_digest("one"), 1, BranchId::new(1, 0));
        let second = RunEntity::new(ProvId::NONE, "bbb", "r2", "claude-opus", "2026-05",
            prompt_digest("two"), 1, BranchId::new(1, 0));
        {
            let s = DurableProvenanceStore::open(&path).unwrap();
            let a = s.intern(&first).unwrap();
            let b = s.intern(&second).unwrap();
            assert_eq!((a, b), (ProvId(1), ProvId(2)));
            s.stamp(rid(1, 1), a).unwrap();
            s.stamp(rid(1, 2), b).unwrap();
        }

        // Swap the two run frames, which are the first two after the header.
        let mut bytes = std::fs::read(&path).unwrap();
        let h = HEADER_SIZE as usize;
        let len_a = u32::from_be_bytes(bytes[h..h + 4].try_into().unwrap()) as usize;
        let len_b =
            u32::from_be_bytes(bytes[h + len_a..h + len_a + 4].try_into().unwrap()) as usize;
        assert_eq!(len_a, len_b, "the two run frames must be the same length to swap them");
        let frame_a = bytes[h..h + len_a].to_vec();
        let frame_b = bytes[h + len_a..h + len_a + len_b].to_vec();
        bytes[h..h + len_b].copy_from_slice(&frame_b);
        bytes[h + len_b..h + len_b + len_a].copy_from_slice(&frame_a);
        std::fs::write(&path, &bytes).unwrap();

        let s = DurableProvenanceStore::open(&path)
            .expect("a file whose run records are out of order was refused");
        assert_eq!(s.run_count(), 2);
        assert_eq!(s.lookup(ProvId(1)).unwrap().agent_id, "aaa", "the ids were renumbered");
        assert_eq!(s.lookup(ProvId(2)).unwrap().agent_id, "bbb");
        // And the stamps still resolve to the runs that made them, which is the point.
        assert_eq!(s.who_wrote(rid(1, 1)).unwrap().agent_id, "aaa");
        assert_eq!(s.who_wrote(rid(1, 2)).unwrap().agent_id, "bbb");
    }

    /// **The sync counter counts one sync per append, under the kind the append carried, and
    /// nothing for an append that failed.**
    ///
    /// D219's red test reads this instrument through a whole `MERGE`, so the instrument is proven
    /// here first, one write path at a time: every kind is forced to fire, a repeat intern (a
    /// lookup, which writes nothing) is shown NOT to fire, and an injected append failure is shown
    /// not to count as a durable sync.
    #[test]
    fn the_sync_counter_fires_once_per_append_by_kind_and_not_for_a_failed_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(s.sync_counts(), SyncCounts::default(), "a fresh store has synced nothing");

        let id = s.intern(&run("restock", "run-1")).unwrap();
        s.intern(&run("restock", "run-1")).unwrap();
        s.stamp(rid(1, 0), id).unwrap();
        s.stamp_row(7, 1, id).unwrap();
        s.stamp_row(7, 2, id).unwrap();
        s.forget_table(8).unwrap();
        assert_eq!(
            s.sync_counts(),
            SyncCounts { runs: 1, stamps: 1, row_authors: 2, forgets: 1 },
            "one sync per append, by kind; the repeat intern is a lookup and must not count"
        );
        assert_eq!(s.sync_counts().total(), 5);

        s.fail_next_append.store(true, Ordering::SeqCst);
        assert!(s.stamp_row(7, 3, id).is_err(), "the injected failure was swallowed");
        assert_eq!(
            s.sync_counts().row_authors,
            2,
            "a failed append was counted as a durable sync"
        );
    }

    /// **D219: a batch of row authorship is ONE sync, and the file it leaves is byte-for-byte the
    /// file the same records written one call at a time leave.**
    ///
    /// The byte comparison is what "preserve the crash semantics" rests on: the replay reads frames,
    /// so identical bytes in identical order mean every reopen — clean, torn or healed — sees what
    /// it saw before. A repeated row is in the batch deliberately: `record_applied` stamps once per
    /// OP, so a two-column update stamps its row twice, and a batch that deduplicated would write a
    /// different file.
    #[test]
    fn a_batch_of_row_authors_is_one_sync_and_the_same_bytes_as_one_call_per_row() {
        let dir = tempfile::tempdir().unwrap();
        let singles = dir.path().join("singles.log");
        let batched = dir.path().join("batched.log");
        let rows = [(7u32, 1u64), (7, 2), (7, 2), (9, 40)];
        {
            let s = DurableProvenanceStore::open(&singles).unwrap();
            let id = s.intern(&run("restock", "run-1")).unwrap();
            for (table, row) in rows {
                s.stamp_row(table, row, id).unwrap();
            }
            assert_eq!(s.sync_counts().row_authors, 4, "the control is one sync per call");
        }
        {
            let s = DurableProvenanceStore::open(&batched).unwrap();
            let id = s.intern(&run("restock", "run-1")).unwrap();
            s.stamp_rows(&rows, id).unwrap();
            assert_eq!(s.sync_counts().row_authors, 1, "a batch of 4 rows took more than one sync");
            assert_eq!(s.row_author(9, 40).unwrap(), id, "the batch did not reach the index");
        }
        assert_eq!(
            std::fs::read(&batched).unwrap(),
            std::fs::read(&singles).unwrap(),
            "the batch wrote a different file from the same records written one at a time"
        );
        let s = DurableProvenanceStore::open(&batched).unwrap();
        assert_eq!(s.recovery().row_authors, 4, "the repeated row's second record was dropped");
        assert_eq!(s.attributed_rows(7).unwrap().len(), 2);
        assert_eq!(s.discarded_tail_bytes(), 0);
    }

    /// An empty batch records nothing: no sync, and no refusal even from a store refusing writes.
    #[test]
    fn an_empty_batch_is_not_a_write() {
        let dir = tempfile::tempdir().unwrap();
        let s = DurableProvenanceStore::open(dir.path().join("prov.log")).unwrap();
        let id = s.intern(&run("restock", "run-1")).unwrap();
        s.stamp_rows(&[], id).unwrap();
        assert_eq!(s.sync_counts().row_authors, 0, "an empty batch issued a sync");

        s.fail_next_append.store(true, Ordering::SeqCst);
        assert!(s.stamp_row(1, 1, id).is_err(), "the injected failure was swallowed");
        s.stamp_rows(&[], id).expect("a poisoned store refused a batch that writes nothing");
        // Anti-vacuity: the store IS refusing writes, so the success above is about emptiness.
        let err = s.stamp_rows(&[(1, 2)], id).expect_err("a poisoned store accepted a batch");
        assert!(format!("{err}").contains("refusing further writes"), "{err}");
    }

    /// A batch whose append fails poisons the store and leaves NONE of its rows in the file.
    #[test]
    fn a_failed_batch_poisons_the_store_and_lands_none_of_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let s = DurableProvenanceStore::open(&path).unwrap();
        let id = s.intern(&run("restock", "run-1")).unwrap();
        s.stamp_row(1, 1, id).unwrap();

        s.fail_next_append.store(true, Ordering::SeqCst);
        let err = s.stamp_rows(&[(1, 2), (1, 3)], id).expect_err("the injected failure was swallowed");
        assert!(format!("{err}").contains("injected"), "{err}");
        let err = s.stamp_rows(&[(1, 4)], id).expect_err("a poisoned store accepted a batch");
        assert!(format!("{err}").contains("refusing further writes"), "{err}");
        drop(s);

        let s = DurableProvenanceStore::open(&path).expect("the file became unopenable");
        assert_eq!(s.row_author(1, 1).unwrap(), id, "a record from before the failure was lost");
        assert_eq!(s.row_author(1, 2).unwrap(), ProvId::NONE, "part of a failed batch landed");
        assert_eq!(s.row_author(1, 3).unwrap(), ProvId::NONE, "part of a failed batch landed");
    }

    /// **A store dropped with pending stamps writes them.** The flush guard is meant to have done
    /// it already; this is the clean-shutdown backstop for a caller that did not, so a stamp the
    /// index reported is not silently missing from the next process.
    #[test]
    fn a_store_dropped_with_pending_stamps_writes_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let id = {
            let s = DurableProvenanceStore::open(&path).unwrap();
            let id = s.intern(&run("restock", "run-1")).unwrap();
            s.stamp_pending(rid(4, 0), id).unwrap();
            s.stamp_pending(rid(4, 1), id).unwrap();
            assert_eq!(
                DurableProvenanceStore::open(&path).unwrap().recovery().stamps,
                0,
                "a pending stamp reached the file before anything wrote it"
            );
            id
        };
        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(s.recovery().stamps, 2, "dropping the store lost its pending stamps");
        assert_eq!(s.attribute(rid(4, 1)).unwrap(), id);
    }

    /// A poisoned store refuses a pending stamp, AT the stamp: queueing it would put a record in
    /// the index that no later write can ever carry to the file.
    #[test]
    fn a_poisoned_store_refuses_a_pending_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let s = DurableProvenanceStore::open(dir.path().join("prov.log")).unwrap();
        let id = s.intern(&run("restock", "run-1")).unwrap();
        s.stamp_pending(rid(6, 0), id).expect("a healthy store refused a pending stamp");
        s.fail_next_append.store(true, Ordering::SeqCst);
        assert!(s.stamp_row(1, 1, id).is_err(), "the injected failure was swallowed");
        let err = s.stamp_pending(rid(6, 1), id).expect_err("a poisoned store queued a stamp");
        assert!(format!("{err}").contains("refusing further writes"), "{err}");
        assert_eq!(
            s.attribute(rid(6, 1)).unwrap(),
            ProvId::NONE,
            "the refused stamp reached the index"
        );
    }

    /// A flush with nothing pending is not a write: it succeeds even on a poisoned store, as an
    /// empty batch does. With something pending, a poisoned store refuses it.
    #[test]
    fn an_empty_flush_is_not_a_write_even_on_a_poisoned_store() {
        let dir = tempfile::tempdir().unwrap();
        let s = DurableProvenanceStore::open(dir.path().join("prov.log")).unwrap();
        let id = s.intern(&run("restock", "run-1")).unwrap();
        s.stamp_pending(rid(7, 0), id).unwrap();
        s.fail_next_append.store(true, Ordering::SeqCst);
        assert!(s.flush().is_err(), "the injected failure was swallowed");
        let err = s.flush().expect_err("a poisoned store flushed its pending stamps");
        assert!(format!("{err}").contains("refusing further writes"), "{err}");
        let empty = DurableProvenanceStore::open(dir.path().join("empty.log")).unwrap();
        let empty_id = empty.intern(&run("restock", "run-1")).unwrap();
        empty.fail_next_append.store(true, Ordering::SeqCst);
        assert!(empty.stamp_row(1, 1, empty_id).is_err(), "the injected failure was swallowed");
        empty.flush().expect("a poisoned store refused a flush with nothing pending");
    }

    /// A batch naming a run this store never interned attributes none of its rows, writes nothing,
    /// and — because nothing changed — does not poison the store.
    #[test]
    fn a_batch_naming_an_uninterned_run_attributes_none_of_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let s = DurableProvenanceStore::open(dir.path().join("prov.log")).unwrap();
        let err = s
            .stamp_rows(&[(7, 1), (7, 2)], ProvId(9))
            .expect_err("a batch naming an uninterned run was accepted");
        assert!(format!("{err}").contains("not interned"), "refused, but not by this guard: {err}");
        assert_eq!(s.row_author(7, 1).unwrap(), ProvId::NONE);
        assert_eq!(s.row_author(7, 2).unwrap(), ProvId::NONE);
        assert_eq!(s.sync_counts().row_authors, 0, "a refused batch reached the file");

        // Anti-vacuity: the same batch under a real run lands whole.
        let id = s.intern(&run("restock", "run-1")).unwrap();
        s.stamp_rows(&[(7, 1), (7, 2)], id).expect("a batch under a real run was refused");
        assert_eq!(s.attributed_rows(7).unwrap(), vec![(1, id), (2, id)]);
    }

    /// **The density claim, measured with the instrument that already exists for it.**
    ///
    /// `footprint_bytes` against `literal_footprint_bytes` — the pair
    /// `store::tests::the_density_numbers_the_docs_quote_are_the_numbers_this_computes` pins — is
    /// asked of the store *after a reopen*, so the claim is about what the file preserved rather
    /// than about what a live process happens to hold.
    #[test]
    fn the_interning_claim_still_holds_after_a_reopen_on_the_existing_instrument() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        {
            let s = DurableProvenanceStore::open(&path).unwrap();
            let id = s.intern(&run("restock-agent", "run-42")).unwrap();
            for slot in 0..200u16 {
                s.stamp(rid(1, slot), id).unwrap();
            }
        }
        let s = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(s.run_count(), 1);
        assert_eq!(s.page_dictionary_len(1), 1, "200 versions, one run, one dictionary entry");
        let interned = s.footprint_bytes();
        let literal = s.literal_footprint_bytes().unwrap();
        assert_eq!(interned, 204, "1 byte per version plus a one-entry dictionary");
        assert!(
            literal > interned * 20,
            "literal {literal} vs interned {interned} — the interning claim did not survive"
        );
    }

    // ---- D246 §6.1: a fork's run record, group-committed outside the lock ----------------------
    //
    // Pre-registered in `bench/d246/PREREG.md` A2 (U1-U4). They name `intern_pending`,
    // `await_run` and `sync_gate`, which do not exist before the fix, so they can only go red under
    // a mutant (M7-M10), never at the red commit.

    /// The run a fork's staging queued, awaited on another thread with its sync held in flight.
    /// Returns the store, the awaiting thread, and the sender that releases the sync.
    #[allow(clippy::type_complexity)]
    fn a_run_sync_in_flight(
        path: &Path,
        run_id: &str,
    ) -> (
        std::sync::Arc<DurableProvenanceStore>,
        ProvId,
        std::thread::JoinHandle<Result<(), FerroError>>,
        std::sync::mpsc::Sender<()>,
    ) {
        use std::sync::{mpsc, Arc};
        let s = Arc::new(DurableProvenanceStore::open(path).unwrap());
        let a = s.intern_pending(&run("fork", run_id)).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *s.sync_gate.lock().unwrap() = Some((entered_tx, release_rx));
        let awaiting = {
            let s = Arc::clone(&s);
            std::thread::spawn(move || s.await_run(a))
        };
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("await_run never reached its sync");
        (s, a, awaiting, release_tx)
    }

    /// **U1: the sync is not issued under the lock every staged write needs.** Under pgwire's
    /// catalog guard, a fork's staging takes this store's file lock (`intern_pending`), and so does
    /// a MERGE's publish loop (`stamp_pending`). If `await_run` held that lock across its fsync,
    /// every statement behind the guard would wait on another fork's disk round-trip: S5 again, one
    /// lock further down.
    #[test]
    fn a_runs_group_sync_does_not_hold_the_lock_every_staged_write_needs() {
        use std::sync::{mpsc, Arc};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let (s, _a, awaiting, release) = a_run_sync_in_flight(&path, "run-a");

        let (done_tx, done_rx) = mpsc::channel();
        let stager = {
            let s = Arc::clone(&s);
            std::thread::spawn(move || {
                let b = s.intern_pending(&run("fork", "run-b"));
                let stamped = b.as_ref().ok().map(|b| s.stamp_pending(rid(1, 0), *b));
                let _ = done_tx.send(());
                (b, stamped)
            })
        };
        let staged_meanwhile = done_rx.recv_timeout(std::time::Duration::from_secs(10)).is_ok();
        let _ = release.send(()); // a signal: a dropped receiver means the sync went ahead
        awaiting.join().unwrap().expect("await_run of the first run");
        let (b, stamped) = stager.join().unwrap();
        let b = b.expect("intern_pending of the second run");
        stamped.expect("the stamp was not attempted").expect("stamp_pending");
        assert!(
            staged_meanwhile,
            "staging a run and queueing a stamp waited for another run's fsync to finish: the sync \
             holds the lock every staged write needs"
        );

        s.await_run(b).expect("await_run of the second run");
        s.flush().expect("flush the stamp");
        assert_eq!(DurableProvenanceStore::open(&path).unwrap().run_count(), 2);
    }

    /// **U2: a record written but not yet synced is not durable.** A repeat run's fork awaits a run
    /// whose record another fork has already WRITTEN, with that fork's sync still in flight. It must
    /// wait for that sync, not return because nothing is pending, and it must share it rather than
    /// issue its own.
    #[test]
    fn a_second_await_of_a_run_already_written_waits_for_its_sync() {
        use std::sync::{mpsc, Arc};
        let dir = tempfile::tempdir().unwrap();
        let (s, a, first, release) = a_run_sync_in_flight(&dir.path().join("prov.log"), "run-a");

        let (done_tx, done_rx) = mpsc::channel();
        let second = {
            let s = Arc::clone(&s);
            std::thread::spawn(move || {
                let r = s.await_run(a);
                let _ = done_tx.send(());
                r
            })
        };
        // Structurally safe: the right code cannot return until `release`, which is sent after this
        // window, so a loaded box can only make a wrong implementation look right, never the reverse.
        let returned_early = done_rx.recv_timeout(std::time::Duration::from_secs(2)).is_ok();
        let _ = release.send(()); // a signal: a dropped receiver means the sync went ahead
        first.join().unwrap().expect("the first await_run");
        second.join().unwrap().expect("the second await_run");
        assert!(
            !returned_early,
            "a second await_run returned while the sync covering the run was still in flight: that \
             fork would be acknowledged with a run a crash could lose"
        );
        assert_eq!(
            s.sync_counts().runs,
            1,
            "the second await issued a sync of its own instead of sharing the one in flight"
        );
    }

    /// **U3: a run a synchronous append already carried costs its await nothing.** A MERGE's
    /// row-author sync writes every pending record first, so a fork staged before it has its run
    /// record synced by the merge. The fork's `complete()` must see that, not issue another sync.
    #[test]
    fn a_run_a_synchronous_append_already_carried_costs_its_await_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let s = DurableProvenanceStore::open(&path).unwrap();
        let a = s.intern_pending(&run("fork", "run-a")).unwrap();
        s.stamp_row(7, 1, a).expect("the synchronous append");
        assert_eq!(
            DurableProvenanceStore::open(&path).unwrap().run_count(),
            1,
            "premise: the synchronous append did not carry the pending run record"
        );
        let before = s.sync_counts();
        s.await_run(a).expect("await_run");
        assert_eq!(
            s.sync_counts(),
            before,
            "await_run synced a run record a synchronous append had already made durable"
        );
    }

    /// **U4: `intern` still returns a DURABLE run**, including a repeat of a run whose record an
    /// `intern_pending` queued and nobody has awaited yet.
    #[test]
    fn interning_a_run_whose_record_is_pending_makes_it_durable_first() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let s = DurableProvenanceStore::open(&path).unwrap();
        let a = s.intern_pending(&run("fork", "run-a")).unwrap();
        assert_eq!(
            DurableProvenanceStore::open(&path).unwrap().run_count(),
            0,
            "premise: intern_pending wrote the record at once, so this test proves nothing"
        );
        assert_eq!(s.intern(&run("fork", "run-a")).unwrap(), a, "a repeat intern is a lookup");
        assert_eq!(
            DurableProvenanceStore::open(&path).unwrap().run_count(),
            1,
            "intern returned a run whose record was still pending"
        );
    }

    // ---- D246 A4: the fresh review's D-1, D-3 and N-9, pre-registered in `bench/d246/PREREG.md` --

    /// **U5 (D-1): a run sync that overlaps a failed append is not acknowledged.** The sync handle
    /// is a duplicate of the append descriptor, so on Linux an fsync error reported to an in-lock
    /// writer is NOT reported to the group leader: the leader's own fsync can return 0 for pages
    /// the kernel dropped. The injected failure below stands in for that writer's EIO, and the
    /// poison flag it leaves is the only trace the leader can see.
    #[test]
    fn a_run_sync_overlapping_a_failed_append_is_not_acknowledged() {
        let dir = tempfile::tempdir().unwrap();
        let (s, a, awaiting, release) = a_run_sync_in_flight(&dir.path().join("prov.log"), "run-a");
        s.fail_next_append.store(true, Ordering::SeqCst);
        assert!(s.stamp_row(7, 1, a).is_err(), "the injected failure was swallowed");
        let _ = release.send(()); // a signal: a dropped receiver means the sync went ahead
        let err = awaiting.join().unwrap().expect_err(
            "a run sync that overlapped a failed append was acknowledged: the fork would be told its \
             run is durable by a sync whose error another writer consumed",
        );
        assert!(format!("{err}").contains("refusing further writes"), "{err}");
    }

    /// **U6 (D-3b): only RUN records are ever left written but unsynced.** `flush` answers for
    /// stamps with "nothing pending, nothing to do", so a stamp queued behind a run whose group
    /// sync is in flight must still be synced by `flush` itself, not written by `await_run` and
    /// left for a sync that has not happened.
    #[test]
    fn a_stamp_queued_behind_a_run_is_never_left_written_but_unsynced() {
        use std::sync::{mpsc, Arc};
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(DurableProvenanceStore::open(dir.path().join("prov.log")).unwrap());
        let a = s.intern_pending(&run("fork", "run-a")).unwrap();
        s.stamp_pending(rid(3, 0), a).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *s.sync_gate.lock().unwrap() = Some((entered_tx, release_rx));
        let awaiting = {
            let s = Arc::clone(&s);
            std::thread::spawn(move || s.await_run(a))
        };
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("await_run never reached its sync");

        let before = s.sync_counts();
        s.flush().expect("flush the stamp");
        let flushed = s.sync_counts();
        let _ = release_tx.send(()); // a signal: a dropped receiver means the sync went ahead
        awaiting.join().unwrap().expect("await_run");
        assert_eq!(
            flushed.stamps - before.stamps,
            1,
            "flush returned without syncing a stamp queued behind a run whose sync is still in \
             flight: the stamp was written by await_run and is durable only if that sync succeeds"
        );
    }

    /// **U7 (D-3e): a poisoned store refuses a pending intern AT the intern**, before the index
    /// holds a run no later write could carry to the file.
    #[test]
    fn a_poisoned_store_refuses_a_pending_intern_and_leaves_the_index_alone() {
        let dir = tempfile::tempdir().unwrap();
        let s = DurableProvenanceStore::open(dir.path().join("prov.log")).unwrap();
        let id = s.intern(&run("restock", "run-1")).unwrap();
        s.fail_next_append.store(true, Ordering::SeqCst);
        assert!(s.stamp_row(1, 1, id).is_err(), "the injected failure was swallowed");
        let before = s.run_count();
        let err = s.intern_pending(&run("fork", "run-b")).expect_err("a poisoned store queued a run");
        assert!(format!("{err}").contains("refusing further writes"), "{err}");
        assert_eq!(s.run_count(), before, "the refused run reached the index");
    }

    /// **U8 (D-3e; second half per A4c R2-D5): a poisoned store refuses to write a pending run and
    /// writes nothing, and still vouches for a run that is already durable.** Appending after a
    /// failed append is what the poison flag exists to stop: a torn frame in the middle of the file
    /// makes the reader drop every record after it.
    #[test]
    fn a_poisoned_store_refuses_await_run_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let s = DurableProvenanceStore::open(&path).unwrap();
        let synced = s.intern(&run("restock", "run-1")).unwrap();
        let a = s.intern_pending(&run("fork", "run-a")).unwrap();
        s.fail_next_append.store(true, Ordering::SeqCst);
        assert!(s.stamp_row(1, 1, synced).is_err(), "the injected failure was swallowed");
        let len = std::fs::metadata(&path).unwrap().len();

        let err = s.await_run(a).expect_err("a poisoned store acknowledged a pending run");
        assert!(format!("{err}").contains("refusing further writes"), "{err}");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            len,
            "a poisoned store appended a run record"
        );
        // A DURABLE run is still vouched for (PREREG A4c, R2-D5): refusing it would refuse every
        // MERGE on a poisoned store, including ones that write no provenance at all.
        s.await_run(synced)
            .expect("a poisoned store refused to vouch for a run that is already durable");
    }
}
