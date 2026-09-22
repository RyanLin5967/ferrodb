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
//!   really does get a durable store.
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
//!   *run*, never per row.
//! * **`Stamp`** — `(page_id, slot_num) -> ProvId`, ten bytes of payload, written once per stamped
//!   version.
//! * **`RowAuthor`** — `(table_id, row_id) -> ProvId`, sixteen bytes, written once per row a merge
//!   publishes. This is the record `who_wrote_row` answers from.
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

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::branch::types::BranchId;
use crate::error::FerroError;
use crate::provenance::store::MemProvenanceStore;
use crate::provenance::{ProvId, ProvenanceStore, RunEntity};
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
    file: Mutex<File>,
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
            file: Mutex::new(file),
            path,
            recovery,
            poisoned: AtomicBool::new(false),
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
    /// The cost is one fsync per interned run and per stamped version, which is the price of the
    /// guarantee rather than an oversight.
    fn append_locked(&self, file: &File, body: &[u8]) -> Result<(), FerroError> {
        // One-shot, and it disarms itself, so a test can fail exactly the append it means to.
        #[cfg(test)]
        if self.fail_next_append.swap(false, Ordering::SeqCst) {
            return Err(FerroError::Provenance("injected provenance append failure".into()));
        }
        let end = file
            .metadata()
            .map_err(|e| FerroError::Provenance(e.to_string()))?
            .len();
        let total = (4 + body.len() + 4) as u32;
        let mut frame = Vec::with_capacity(total as usize);
        frame.extend_from_slice(&total.to_be_bytes());
        frame.extend_from_slice(body);
        let crc = crc32(&frame);
        frame.extend_from_slice(&crc.to_be_bytes());
        pwrite_all(file, &frame, end)
            .map_err(|e| FerroError::Provenance(format!("append to {}: {e}", self.path.display())))?;
        file.sync_data()
            .map_err(|e| FerroError::Provenance(e.to_string()))?;
        Ok(())
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

impl ProvenanceStore for DurableProvenanceStore {
    fn intern(&self, run: &RunEntity) -> Result<ProvId, FerroError> {
        // The file lock is taken FIRST and held across BOTH the in-memory intern and the append,
        // so two threads cannot both observe "this run is new" and both write it, and no stamp can
        // slip between a run being interned and its record reaching the file. Lock order is
        // file -> mem, everywhere.
        let file = self.file.lock().unwrap();
        // Checked UNDER the lock, and that is the fix for a race rather than tidiness. Checked
        // before taking it, a writer that had already passed the check could be sitting on
        // `file.lock()` while another thread's append failed and poisoned the store; it would then
        // acquire the lock and append anyway, producing exactly the file the poison flag exists to
        // prevent — a `Stamp` naming a run whose `Run` record never landed, which the next `open`
        // must refuse whole. Check-then-act on shared state, which is the shape that has produced
        // six separate defects in this codebase.
        self.refuse_if_poisoned()?;

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
            return Ok(id);
        }

        let mut body = Vec::with_capacity(tail.len() + 5);
        body.push(TAG_RUN);
        body.extend_from_slice(&id.0.to_be_bytes());
        body.extend_from_slice(&tail);
        if let Err(e) = self.append_locked(&file, &body) {
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(e);
        }
        Ok(id)
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
        let file = self.file.lock().unwrap();
        // Under the lock; see `intern` for the race that checking it first opened.
        self.refuse_if_poisoned()?;
        // In memory first: it holds the guards (an uninterned id, `ProvId::NONE`, a page dictionary
        // at its cap), and a refused stamp must not reach the file.
        self.mem.stamp(rid, id)?;
        let mut body = Vec::with_capacity(11);
        body.push(TAG_STAMP);
        body.extend_from_slice(&rid.page_id.to_be_bytes());
        body.extend_from_slice(&rid.slot_num.to_be_bytes());
        body.extend_from_slice(&id.0.to_be_bytes());
        if let Err(e) = self.append_locked(&file, &body) {
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(e);
        }
        Ok(())
    }

    // ── Logical row attribution, appended and fsynced exactly like a stamp ──────────────────────
    //
    // Same lock order (file -> mem), same poison-on-append-failure, same synchronous fsync. The
    // three write paths differ only in what they encode, and that is deliberate: a second
    // durability strategy for the record kind that answers criterion 9 would be a second thing to
    // keep true.

    fn stamp_row(&self, table: u32, row: u64, id: ProvId) -> Result<(), FerroError> {
        let file = self.file.lock().unwrap();
        self.refuse_if_poisoned()?;
        // In memory first, so a refused attribution (an id this store never interned) does not
        // reach the file and make the next `open` refuse the whole thing.
        self.mem.stamp_row(table, row, id)?;
        let mut body = Vec::with_capacity(17);
        body.push(TAG_ROW_AUTHOR);
        body.extend_from_slice(&table.to_be_bytes());
        body.extend_from_slice(&row.to_be_bytes());
        body.extend_from_slice(&id.0.to_be_bytes());
        if let Err(e) = self.append_locked(&file, &body) {
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
        let file = self.file.lock().unwrap();
        self.refuse_if_poisoned()?;
        self.mem.forget_table(table)?;
        let mut body = Vec::with_capacity(5);
        body.push(TAG_FORGET_TABLE);
        body.extend_from_slice(&table.to_be_bytes());
        if let Err(e) = self.append_locked(&file, &body) {
            // **The in-memory forget has already happened and cannot be undone**, so this store
            // now knows LESS than its file — the mirror image of the poison case `intern` and
            // `stamp` guard, and poisoned for the same reason. Reopening would replay the
            // attributions this call dropped and hand them to whatever table next takes the name.
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(e);
        }
        Ok(())
    }
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
}
