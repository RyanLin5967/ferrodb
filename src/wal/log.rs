use std::{fs::OpenOptions, mem::take, path::PathBuf, sync::{Arc, Mutex, OnceLock, atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering}}};

use crate::{
    branch::types::BranchId,
    catalog::column::DataType,
    error::FerroError,
    provenance::{ProvId, RunEntity},
    storage::storage::Storage,
};

const HEADER_SIZE: usize = 24;
const MAGIC: u32 = 0xF3_EE_DB_01;
/// **The log format this binary writes: 3 since D213.** In it a forward `HeapDelete` RETIRES its
/// slot and a `HeapRelease` (tag 11) frees it after the commit (`storage::heap_page::RETIRED`).
///
/// Version 2 is every log written before D213. The records are the same bytes, but a forward
/// `HeapDelete` FREED its slot at once, and there is no `HeapRelease`. Replaying such a log with
/// version-3 meaning failed the open whenever a committed insert had landed in a committed
/// relocation's bytes (the adversary's F1 on `115f0b7`, `frontier/rollback_adversary.md`). So
/// the version travels with the log. A version-2 log is replayed with version-2 meaning, then
/// checkpointed (`wal::recovery::open_recovered`), which rewrites its header as version 3. Until
/// then no transaction may begin on it (`TxnManager::begin`), and the log itself refuses the two
/// records whose meaning changed, a top-level `HeapDelete` and a `HeapRelease` ([`WalManager::append`],
/// review 2's Q5), so no record with version-3 meaning can be written into a log labelled version 2.
///
/// A binary before D213 refuses a version-3 log ("incorrect wal version") instead of misreading
/// it, which also makes a downgrade a clean refusal (the adversary's F6).
const VERSION: u32 = 3;
/// The one older format this binary still opens. See [`VERSION`].
const LEGACY_VERSION: u32 = 2;
const INITIAL_LSN: u64 = 1;
const MIN_FRAME: usize = 33;

// need next_txn_id for mvcc, and then multi txn statements before mvcc
/// **D69-REOPEN INSTRUMENT: count the fsyncs, do not infer them from a profile.**
///
/// The merge-phase profile put 98.2% of samples in `WalManager::flush` and read that as "the fsync
/// itself scales with database volume". That is an inference from an aggregate, and an aggregate
/// cannot separate "one fsync that got slower" from "more fsyncs of the same speed" — they produce
/// the identical profile. A counter can, so this is a counter.
///
/// Process-wide rather than per-`WalManager` so that no constructor has to change; the D68 harness
/// is one thread and one manager, which is the only configuration these are read in. `Relaxed` is
/// sufficient — nothing orders anything against these, and they are read after the work is done.
/// ⚠ Two uncontended relaxed increments per fsync sit inside the timed region. That is the harness
/// measuring itself, so it is bounded deliberately: an fsync is microseconds at best and these are
/// nanoseconds, and they are present in EVERY arm, so they cannot create a slope across arms.
pub static FSYNC_CALLS: AtomicU64 = AtomicU64::new(0);
pub static FSYNC_BYTES: AtomicU64 = AtomicU64::new(0);

/// `(calls, bytes)` since process start. Read it twice and subtract to scope it to a phase.
pub fn fsync_counters() -> (u64, u64) {
    (FSYNC_CALLS.load(Ordering::Relaxed), FSYNC_BYTES.load(Ordering::Relaxed))
}

/// **D212 (a') instrument: records and bytes [`WalManager::read_record`] has returned.**
///
/// The checkpoint hook that makes REVERT's history durable must read NO log: it drains an
/// in-memory queue. A hook that re-read the retained log instead would cost O(pin lag) per
/// checkpoint under a WAL pin (SCALE-DESIGN "D212 (a') AMENDED" §6), and only a counter here can
/// show it does not. Observation only; nothing reads these to decide anything.
pub static WAL_RECORDS_READ: AtomicU64 = AtomicU64::new(0);
pub static WAL_BYTES_READ: AtomicU64 = AtomicU64::new(0);

/// `(records, bytes)` read from any log since process start.
pub fn wal_read_counters() -> (u64, u64) {
    (WAL_RECORDS_READ.load(Ordering::Relaxed), WAL_BYTES_READ.load(Ordering::Relaxed))
}

/// The most history bytes one tag-12 record carries. A larger history record is split across
/// consecutive parts of its transaction, so every frame stays far below the 8 MiB replication
/// frame (`replication::MAX_FRAME`) whatever the size of the merge.
pub const REVERT_HISTORY_PART_BYTES: usize = 1 << 20;

pub struct WalManager {
    /// The log's bytes. Was a concrete `File`; it is a [`Storage`] so that a crash can be aimed at
    /// this log — a torn frame, a lost frame, a flush that reports success it did not achieve. Those
    /// are the faults `scan_valid_end` and the CRC exist to survive, and until this seam existed the
    /// only way to stage one was to edit the file after closing it. `impl Storage for File` keeps the
    /// production path on the same syscalls.
    pub file: Mutex<Arc<dyn Storage>>,
    pub buffer: Mutex<WalBuffer>,
    pub next_lsn: AtomicU64,
    pub flushed_lsn: AtomicU64,
    pub path: PathBuf,
    pub base_lsn: AtomicU64,
    pub header_txn_id: u64,
    /// LSNs some reader still needs, so a checkpoint may not discard them. See [`WalManager::pin`].
    pins: Mutex<std::collections::BTreeMap<u64, u64>>,
    next_pin_id: AtomicU64,
    /// **Test-only: make the next [`WalManager::append`] fail once.**
    ///
    /// `append` cannot fail on its own — it extends an in-memory buffer and ends in `Ok`. That
    /// makes several error paths that guard against a failed append unreachable, and an unreachable
    /// guard is one nobody can show works. `end_read_only` has one whose cost is the whole database
    /// (a reader left in `att` blocks every checkpoint for the life of the process), so it is worth
    /// being able to fire deliberately.
    ///
    /// `#[cfg(test)]` so it does not exist in any shipped build, nor in integration tests: this is
    /// a lever for the unit tests in this crate and nothing else.
    #[cfg(test)]
    pub(crate) fail_next_append: std::sync::atomic::AtomicBool,
    /// The format this log was written in: [`VERSION`], or [`LEGACY_VERSION`] until a truncation
    /// rewrites the header.
    format: AtomicU32,
    /// **Set once, never cleared: the log refuses every write** ([`WalManager::poison`]). The
    /// reason is kept for the refusal's message.
    poisoned: AtomicBool,
    poison_reason: Mutex<Option<String>>,
}

/// A claim on the log from `lsn` onwards. Released on drop.
///
/// This is a minimal **replication slot**. It exists because a base backup taken while the primary
/// is running was found to be dead on arrival: the copy recorded a start LSN, the next checkpoint
/// truncated the whole log, and the replica was refused with *"lsn 183 is below the log's base"*
/// before it applied a single record. A backup is only usable if the log it points into survives.
pub struct WalPin {
    wal: std::sync::Arc<WalManager>,
    id: u64,
    /// The LSN this pin holds. Exposed because a caller that pins "wherever the log is now" has no
    /// other way to learn where that turned out to be.
    pub lsn: u64,
}

impl WalPin {
    pub fn lsn(&self) -> u64 {
        self.lsn
    }
}

impl Drop for WalPin {
    fn drop(&mut self) {
        self.wal.pins.lock().unwrap().remove(&self.id);
    }
}

impl std::fmt::Debug for WalPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WalPin({} @ lsn {})", self.id, self.lsn)
    }
}

pub struct WalBuffer {
    pub bytes: Vec<u8>,
    pub start_lsn: u64,
}

/// What a [`RecKind::Ddl`] record describes.
///
/// **No longer `Copy`, on purpose.** `AlterColumn` carries the column names the change is about,
/// and those cannot be reconstructed from the record's `columns` list: a rename's *old* name is
/// gone from the new shape, and a retype's *old* type is gone from it too. A reader handed only
/// the resulting shape sees a rename as one column vanishing and another appearing, which a sink
/// would apply as DROP + ADD — losing the column's data. So the alteration travels with the op.
#[derive(Debug, PartialEq, Clone)]
pub enum DdlOp {
    CreateTable,
    DropTable,
    /// A column-level change. The record's `columns` still carries the table's **full shape after
    /// the change**, exactly as `CreateTable` does, so one retained record per table remains a
    /// complete description of it.
    AlterColumn(ColumnAlteration),
}

/// Which column-level change a [`DdlOp::AlterColumn`] describes, and the half of it that the
/// resulting shape does not record.
#[derive(Debug, PartialEq, Clone)]
pub enum ColumnAlteration {
    /// A column appended at the end. Its type and nullability are in the record's `columns`.
    Add { column: String },
    /// A column renamed. `to` is in the record's `columns`; `from` is nowhere else.
    Rename { from: String, to: String },
    /// A column's type changed. The new type is in the record's `columns`; `from` is nowhere else.
    Retype { column: String, from: DataType },
}

impl ColumnAlteration {
    /// The column the change is about, named as it is **after** the change.
    pub fn column(&self) -> &str {
        match self {
            ColumnAlteration::Add { column } => column,
            ColumnAlteration::Rename { to, .. } => to,
            ColumnAlteration::Retype { column, .. } => column,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum RecKind {
    Begin, Commit, Abort, TxnEnd, 
    HeapInsert { dir_root: u32, page_id: u32, slot: u16, tuple: Vec<u8> },
    HeapDelete { dir_root: u32, page_id: u32, slot: u16, old: Vec<u8> }, 
    HeapUpdate { dir_root: u32, page_id: u32, slot: u16, old: Vec<u8>, new: Vec<u8> },
    /// **D213: a committed transaction frees a slot its delete RETIRED.** Tag 11, the next free
    /// number, so every older record keeps its meaning.
    ///
    /// A logged `HeapDelete` no longer frees its slot: it retires it, so the bytes stay occupied
    /// and a rollback restores the tuple in place (`storage::heap_page::RETIRED`). This record is
    /// the commit's release of those bytes, written by `TxnManager::commit` after the `Commit`
    /// record is durable, in the same transaction. Redo and a replica apply it like any other page
    /// change, gated by the page's LSN. Without it a replica, or recovery after a crash, would keep
    /// the slot retired while the primary's later inserts used its bytes, and replaying those
    /// inserts would find no room. It carries no row, so it is not a change-feed event, and nothing
    /// ever undoes it: its transaction has committed.
    HeapRelease { dir_root: u32, page_id: u32, slot: u16 },
    Clr { undone_lsn: u64, undo_next: u64, redo: Box<RecKind> },
    Checkpoint,
    /// A schema change, logged so the change feed can carry it.
    ///
    /// The catalog itself is written outside the WAL, so this record does not *drive* DDL — recovery
    /// does not replay it and the catalog is authoritative for the running database. It exists so
    /// that a **reader of the log** can know what the tables were at each point in it. Without it a
    /// decoder can only assume today's catalog describes yesterday's rows, which is wrong the moment
    /// anyone runs `CREATE TABLE` or `DROP TABLE`, and wrong silently.
    Ddl {
        op: DdlOp,
        table: String,
        dir_root: u32,
        time_travel_root: u32,
        /// `(name, type, nullable)` per column. Empty for a drop.
        columns: Vec<(String, DataType, bool)>,
    },
    /// **Who wrote this transaction**: one interned [`RunEntity`], bound to the record's `txn_id`.
    ///
    /// Tag 10. Tags 0..9 were taken when this arrived, and it takes the next free number rather
    /// than reusing one, so a WAL written before this variant existed still replays — the same
    /// additive discipline the wide column types followed at `DataType::BigInt`.
    ///
    /// # It carries the whole entity, and that is not redundancy
    ///
    /// A bare `prov_id` would be a reference into a table the log does not contain. Provenance is
    /// interned *in memory*, so a reader of an archived log — or of a log whose database is gone —
    /// would hold a number naming nothing. Carrying the tuple makes the log self-describing about
    /// its writers for exactly the reason [`RecKind::Ddl`] carries the columns rather than a table
    /// id: a decoder must be able to answer from the log alone.
    ///
    /// The density argument in `provenance` is untouched by this. Interning is about the cost per
    /// *row version*, and this is one record per **transaction**, not per row.
    ///
    /// # Two jobs, one variant, told apart by `txn_id`
    ///
    /// * `txn_id != 0` — a **binding**: this transaction's changes were written by this run. It is
    ///   appended immediately before the `Commit`, and [`crate::replication::logical`] documents
    ///   at length why anywhere else loses it.
    /// * `txn_id == 0` — a **declaration**: this run exists. Written by `TxnManager` after every
    ///   checkpoint, because a checkpoint discards the log whole, exactly as `replay_schema` does
    ///   for DDL. Transaction 0 never commits, so a declaration binds nothing.
    RunIdentity { run: RunEntity },
    /// **D212 (a') — one part of a REVERT history record, written inside the transaction whose
    /// history it is** (tag 12; #16 took 11 for `HeapRelease`).
    ///
    /// Appended among the transaction's own records, so the transaction's `Commit` decides for the
    /// rows and their history together (`wal::history`). Recovery does not redo it — it describes
    /// no page — and the change feed and a physical replica skip it. What it does is let the open
    /// put a committed record back into `<db>.history` when a crash beat the checkpoint that would
    /// have written it there. `bytes` are opaque here; a record over
    /// [`REVERT_HISTORY_PART_BYTES`] arrives as parts `0, 1, ..`, the last one marked.
    RevertHistory { hseq: u64, ordinal: u64, part: u32, last: bool, bytes: Vec<u8> },
}

pub struct LogRecord {
    pub lsn: u64,
    pub prev_lsn: u64,
    pub txn_id: u64,
    pub kind: RecKind
}

/// Length-prefixed string, **refusing** any string the `u16` prefix cannot express.
///
/// `pub(crate)` rather than private because the durable provenance store writes the same
/// length-prefixed strings into its own file. One encoder means the two formats cannot disagree
/// about what a string is, and a second hand-written one is a second place for the same
/// off-by-two to live.
///
/// ⛔ **THE GUARD IS HERE AND DELIBERATELY NOWHERE ELSE.** Until D154 this wrote `s.len() as u16`
/// unchecked, while the line above claimed *"so a name containing anything at all cannot desync
/// the reader"* — false for any string of 65536 bytes or more, which landed with a length prefix
/// of **zero** followed by its full bytes: durable, correctly checksummed over exactly the bytes
/// intended, and permanently undecodable. Nothing downstream can catch that, because everything
/// downstream checks the bytes against a checksum of themselves.
///
/// ⭐ **THREE separate callers had already found this and each fixed it LOCALLY** —
/// `tel::log::put_str`, `consensus::log::put_str`, and `consensus::transport`'s re-encode check —
/// while the root stayed unguarded and two more formats grew on top of it. That is the shape:
/// a local fix is always cheaper than a root fix, so local fixes accumulate and the root never
/// gets done. More than one hand-rolled wrapper around one primitive means the primitive is
/// broken, and each wrapper is a datapoint proving somebody already knew.
///
/// The two `put_str`s are now error-type ADAPTERS, not guards — they exist so each format keeps
/// its own error vocabulary. They must not re-check the length: a second check in front of this
/// one would mask every mutant of it.
pub(crate) fn write_str(buffer: &mut Vec<u8>, s: &str, what: &'static str) -> Result<(), FerroError> {
    let len = u16::try_from(s.len()).map_err(|_| FerroError::Unrepresentable {
        what: what.to_string(),
        len: s.len(),
        limit: u16::MAX as usize,
    })?;
    buffer.extend_from_slice(&len.to_be_bytes());
    buffer.extend_from_slice(s.as_bytes());
    Ok(())
}

// Bounds-checked readers. These bytes arrive from a disk or a socket, so every read has to be able
// to refuse: indexing past the end of a truncated record panics the whole process, which is a
// denial of service triggered by a corrupt log rather than a parse error.
pub(crate) fn take_u8(bytes: &[u8], at: &mut usize) -> Result<u8, FerroError> {
    let v = *bytes.get(*at).ok_or_else(|| short(*at, 1, bytes.len()))?;
    *at += 1;
    Ok(v)
}

pub(crate) fn take_u16(bytes: &[u8], at: &mut usize) -> Result<u16, FerroError> {
    let end = *at + 2;
    let slice = bytes.get(*at..end).ok_or_else(|| short(*at, 2, bytes.len()))?;
    *at = end;
    Ok(u16::from_be_bytes(slice.try_into().unwrap()))
}

pub(crate) fn take_u32(bytes: &[u8], at: &mut usize) -> Result<u32, FerroError> {
    let end = *at + 4;
    let slice = bytes.get(*at..end).ok_or_else(|| short(*at, 4, bytes.len()))?;
    *at = end;
    Ok(u32::from_be_bytes(slice.try_into().unwrap()))
}

pub(crate) fn take_u64(bytes: &[u8], at: &mut usize) -> Result<u64, FerroError> {
    let end = *at + 8;
    let slice = bytes.get(*at..end).ok_or_else(|| short(*at, 8, bytes.len()))?;
    *at = end;
    Ok(u64::from_be_bytes(slice.try_into().unwrap()))
}

/// Exactly `N` bytes, refusing a record too short to hold them.
pub(crate) fn take_array<const N: usize>(
    bytes: &[u8],
    at: &mut usize,
) -> Result<[u8; N], FerroError> {
    let end = *at + N;
    let slice = bytes.get(*at..end).ok_or_else(|| short(*at, N, bytes.len()))?;
    *at = end;
    Ok(slice.try_into().unwrap())
}

pub(crate) fn take_str(bytes: &[u8], at: &mut usize) -> Result<String, FerroError> {
    let len = take_u16(bytes, at)? as usize;
    let end = *at + len;
    let slice = bytes.get(*at..end).ok_or_else(|| short(*at, len, bytes.len()))?;
    *at = end;
    String::from_utf8(slice.to_vec())
        .map_err(|e| FerroError::Wal(format!("log record holds a non-utf8 string: {e}")))
}

/// A [`DataType`] as one tag byte plus whatever payload the type carries.
///
/// One definition, two call sites: a column in the record's shape list, and the *old* type inside
/// a [`ColumnAlteration::Retype`]. Written as a function rather than twice inline so a type added
/// to `DataType` fails to compile here instead of acquiring two different tags.
fn write_data_type(buffer: &mut Vec<u8>, ty: &DataType) {
    // Varchar's length is part of the type, so a consumer that recreates the column gets the same
    // width. Tags 0..3 are fixed by every DDL record already in a log; the wide types took the
    // next free numbers so an existing WAL still replays.
    match ty {
        DataType::Integer => buffer.push(0),
        DataType::Float => buffer.push(1),
        DataType::Boolean => buffer.push(2),
        DataType::Varchar(n) => {
            buffer.push(3);
            buffer.extend_from_slice(&n.to_be_bytes());
        }
        DataType::BigInt => buffer.push(4),
        DataType::Decimal => buffer.push(5),
        DataType::Timestamp => buffer.push(6),
    }
}

fn read_data_type(bytes: &[u8], at: &mut usize) -> Result<DataType, FerroError> {
    Ok(match take_u8(bytes, at)? {
        0 => DataType::Integer,
        1 => DataType::Float,
        2 => DataType::Boolean,
        3 => DataType::Varchar(take_u16(bytes, at)?),
        4 => DataType::BigInt,
        5 => DataType::Decimal,
        6 => DataType::Timestamp,
        other => return Err(FerroError::Wal(format!("unknown column type tag {other}"))),
    })
}

// `pub(crate)`, which is HEAD's visibility: B10's `storage::sim`, `branch::arena`,
// `branch::record` and `cow::page_header` all read from this module, and B11's private `fn short`
// would have made this file's own error helper unreachable from them.
pub(crate) fn short(at: usize, want: usize, have: usize) -> FerroError {
    FerroError::Wal(format!(
        "log record is truncated: wanted {want} byte(s) at offset {at} but the record is {have} bytes"
    ))
}

impl RecKind {
    pub fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), FerroError> {
        match self {
            RecKind::Begin => buffer.push(0),
            RecKind::Commit => buffer.push(1),
            RecKind::Abort => buffer.push(2),
            RecKind::TxnEnd => buffer.push(3),
            RecKind::Checkpoint => buffer.push(4),
            RecKind::HeapInsert { dir_root, page_id, slot, tuple } => {
                buffer.push(5);
                buffer.extend_from_slice(&dir_root.to_be_bytes());
                buffer.extend_from_slice(&page_id.to_be_bytes());
                buffer.extend_from_slice(&slot.to_be_bytes());
                buffer.extend_from_slice(&(tuple.len() as u32).to_be_bytes());
                buffer.extend_from_slice(tuple);
            }
            RecKind::HeapDelete { dir_root, page_id, slot, old } => {
                buffer.push(6);
                buffer.extend_from_slice(&dir_root.to_be_bytes());
                buffer.extend_from_slice(&page_id.to_be_bytes());
                buffer.extend_from_slice(&slot.to_be_bytes());
                buffer.extend_from_slice(&(old.len() as u32).to_be_bytes());
                buffer.extend_from_slice(old);
            }
            RecKind::HeapUpdate { dir_root, page_id, slot, old, new } => {
                buffer.push(7);
                buffer.extend_from_slice(&dir_root.to_be_bytes());
                buffer.extend_from_slice(&page_id.to_be_bytes());
                buffer.extend_from_slice(&slot.to_be_bytes());
                buffer.extend_from_slice(&(old.len() as u32).to_be_bytes());
                buffer.extend_from_slice(old);
                buffer.extend_from_slice(&(new.len() as u32).to_be_bytes());
                buffer.extend_from_slice(new);
            }
            RecKind::HeapRelease { dir_root, page_id, slot } => {
                buffer.push(11);
                buffer.extend_from_slice(&dir_root.to_be_bytes());
                buffer.extend_from_slice(&page_id.to_be_bytes());
                buffer.extend_from_slice(&slot.to_be_bytes());
            }
            RecKind::Ddl { op, table, dir_root, time_travel_root, columns } => {
                buffer.push(9);
                // Tags 0 and 1 keep their meaning and their position, so every DDL record already
                // in a log still deserializes byte for byte. The alteration's payload is written
                // immediately after the op byte and only for tag 2, so nothing that reads an older
                // record is offset by it.
                match op {
                    DdlOp::CreateTable => buffer.push(0),
                    DdlOp::DropTable => buffer.push(1),
                    DdlOp::AlterColumn(alt) => {
                        buffer.push(2);
                        match alt {
                            ColumnAlteration::Add { column } => {
                                buffer.push(0);
                                write_str(buffer, column, "an added column's name")?;
                            }
                            ColumnAlteration::Rename { from, to } => {
                                buffer.push(1);
                                write_str(buffer, from, "a renamed column's old name")?;
                                write_str(buffer, to, "a renamed column's new name")?;
                            }
                            ColumnAlteration::Retype { column, from } => {
                                buffer.push(2);
                                write_str(buffer, column, "a retyped column's name")?;
                                write_data_type(buffer, from);
                            }
                        }
                    }
                }
                buffer.extend_from_slice(&dir_root.to_be_bytes());
                buffer.extend_from_slice(&time_travel_root.to_be_bytes());
                write_str(buffer, table, "a table name")?;
                // Same hazard as the names, one field over: a DDL record with 65536 columns would
                // carry a column count of ZERO and then all of them.
                let n_cols = u16::try_from(columns.len()).map_err(|_| FerroError::Unrepresentable {
                    what: "a DDL record's column count".to_string(),
                    len: columns.len(),
                    limit: u16::MAX as usize,
                })?;
                buffer.extend_from_slice(&n_cols.to_be_bytes());
                for (name, ty, nullable) in columns {
                    write_str(buffer, name, "a column name")?;
                    write_data_type(buffer, ty);
                    buffer.push(if *nullable { 1 } else { 0 });
                }
            }
            RecKind::RunIdentity { run } => {
                buffer.push(10);
                buffer.extend_from_slice(&run.prov_id.0.to_be_bytes());
                write_str(buffer, &run.agent_id, "an agent id")?;
                write_str(buffer, &run.run_id, "a run id")?;
                write_str(buffer, &run.model, "a model name")?;
                write_str(buffer, &run.model_version, "a model version")?;
                buffer.extend_from_slice(&run.prompt_hash);
                buffer.extend_from_slice(&run.started_at.to_be_bytes());
                buffer.extend_from_slice(&run.parent_branch.id.to_be_bytes());
                buffer.extend_from_slice(&run.parent_branch.generation.to_be_bytes());
            }
            RecKind::RevertHistory { hseq, ordinal, part, last, bytes } => {
                buffer.push(12);
                buffer.extend_from_slice(&hseq.to_be_bytes());
                buffer.extend_from_slice(&ordinal.to_be_bytes());
                buffer.extend_from_slice(&part.to_be_bytes());
                buffer.push(u8::from(*last));
                let len = u32::try_from(bytes.len()).map_err(|_| FerroError::Unrepresentable {
                    what: "a REVERT history record part".to_string(),
                    len: bytes.len(),
                    limit: u32::MAX as usize,
                })?;
                buffer.extend_from_slice(&len.to_be_bytes());
                buffer.extend_from_slice(bytes);
            }
            RecKind::Clr { undone_lsn, undo_next, redo } => {
                buffer.push(8);
                buffer.extend_from_slice(&undone_lsn.to_be_bytes());
                buffer.extend_from_slice(&undo_next.to_be_bytes());
                redo.serialize(buffer)?;
            }
        }
        Ok(())
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, FerroError> {
        if bytes.is_empty() {
            return Err(FerroError::Wal("empty log record".into()))
        }
        match bytes[0] {
            0 => Ok(RecKind::Begin),
            1 => Ok(RecKind::Commit),
            2 => Ok(RecKind::Abort),
            3 => Ok(RecKind::TxnEnd),
            4 => Ok(RecKind::Checkpoint),
            5 => {
                let (dir_root, page_id, slot, length) = read_heap(bytes)?;
                let tuple = bytes[15..15+length].to_vec();
                Ok(RecKind::HeapInsert { dir_root, page_id, slot, tuple })
            }
            6 => {
                let (dir_root, page_id, slot, length) = read_heap(bytes)?;
                let old = bytes[15..15 + length].to_vec();
                Ok(RecKind::HeapDelete { dir_root, page_id, slot, old })
            }
            7 => {
                let (dir_root, page_id, slot, length) = read_heap(bytes)?;
                let old = bytes[15..15 + length].to_vec();
                let new_len = u32::from_be_bytes(bytes[15 + length..19 + length].try_into().unwrap()) as usize;
                let new = bytes[19 + length.. 19 + length + new_len].to_vec();
                Ok(RecKind::HeapUpdate { dir_root, page_id, slot, old, new })
            }
            9 => {
                // Every read is bounds-checked against the record's own length: these bytes came
                // off a disk or a socket, and `deserialize` must refuse a truncated record rather
                // than index past it and panic the process.
                let mut at = 1usize;
                let op = match take_u8(bytes, &mut at)? {
                    0 => DdlOp::CreateTable,
                    1 => DdlOp::DropTable,
                    2 => DdlOp::AlterColumn(match take_u8(bytes, &mut at)? {
                        0 => ColumnAlteration::Add { column: take_str(bytes, &mut at)? },
                        1 => ColumnAlteration::Rename {
                            from: take_str(bytes, &mut at)?,
                            to: take_str(bytes, &mut at)?,
                        },
                        2 => ColumnAlteration::Retype {
                            column: take_str(bytes, &mut at)?,
                            from: read_data_type(bytes, &mut at)?,
                        },
                        other => {
                            return Err(FerroError::Wal(format!(
                                "unknown column alteration tag {other}"
                            )))
                        }
                    }),
                    other => return Err(FerroError::Wal(format!("unknown ddl op {other}"))),
                };
                let dir_root = take_u32(bytes, &mut at)?;
                let time_travel_root = take_u32(bytes, &mut at)?;
                let table = take_str(bytes, &mut at)?;
                let count = take_u16(bytes, &mut at)? as usize;
                let mut columns = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    let name = take_str(bytes, &mut at)?;
                    let ty = read_data_type(bytes, &mut at)?;
                    let nullable = take_u8(bytes, &mut at)? != 0;
                    columns.push((name, ty, nullable));
                }
                Ok(RecKind::Ddl { op, table, dir_root, time_travel_root, columns })
            }
            10 => {
                // Bounds-checked throughout, for the same reason the DDL arm is: these bytes came
                // off a disk, and a truncated record must be refused rather than indexed past.
                let mut at = 1usize;
                let prov_id = ProvId(take_u32(bytes, &mut at)?);
                if prov_id.is_none() {
                    return Err(FerroError::Wal(
                        "run identity record names ProvId::NONE, which is the value meaning \
                         'unattributed'; a record claiming a writer must name one"
                            .into(),
                    ));
                }
                let agent_id = take_str(bytes, &mut at)?;
                let run_id = take_str(bytes, &mut at)?;
                let model = take_str(bytes, &mut at)?;
                let model_version = take_str(bytes, &mut at)?;
                let prompt_hash = take_array::<32>(bytes, &mut at)?;
                let started_at = take_u64(bytes, &mut at)?;
                let branch_id = take_u64(bytes, &mut at)?;
                let generation = take_u32(bytes, &mut at)?;
                Ok(RecKind::RunIdentity {
                    run: RunEntity::new(
                        prov_id,
                        agent_id,
                        run_id,
                        model,
                        model_version,
                        prompt_hash,
                        started_at,
                        BranchId::new(branch_id, generation),
                    ),
                })
            }
            11 => {
                let mut at = 1usize;
                let dir_root = take_u32(bytes, &mut at)?;
                let page_id = take_u32(bytes, &mut at)?;
                let slot = take_u16(bytes, &mut at)?;
                Ok(RecKind::HeapRelease { dir_root, page_id, slot })
            }
            12 => {
                let mut at = 1usize;
                let hseq = take_u64(bytes, &mut at)?;
                let ordinal = take_u64(bytes, &mut at)?;
                let part = take_u32(bytes, &mut at)?;
                let last = take_u8(bytes, &mut at)? != 0;
                let len = take_u32(bytes, &mut at)? as usize;
                let body = at
                    .checked_add(len)
                    .and_then(|end| bytes.get(at..end))
                    .ok_or_else(|| short(at, len, bytes.len()))?;
                Ok(RecKind::RevertHistory { hseq, ordinal, part, last, bytes: body.to_vec() })
            }
            8 => {
                let undone_lsn = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
                let undo_next = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
                let redo = RecKind::deserialize(&bytes[17..])?;
                Ok(RecKind::Clr { undone_lsn, undo_next, redo: Box::new(redo) })
            }
            t => Err(FerroError::Wal(format!("unknown tag: {}", t)))
        }
    }
}

impl WalManager {
    /// Open the log on a real file. The production entry point.
    pub fn new(path: PathBuf) -> Result<Self, FerroError> {
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path).map_err(|e| FerroError::Wal(e.to_string()))?;
        Self::with_storage(Arc::new(file), path)
    }

    /// Open the log on any [`Storage`]. `path` is carried because callers report it and
    /// `WalManager::path` is public. The log itself is never opened through it, but a FRESH log (length
    /// 0) moves the release quarantine beside `path` aside first (`txn::start_fresh_quarantine`), so
    /// even a log on simulated storage does real filesystem I/O next to `path`, and a failed move fails
    /// the log's creation.
    pub fn with_storage(file: Arc<dyn Storage>, path: PathBuf) -> Result<Self, FerroError> {
        let len = file.len().map_err(|e| FerroError::Wal(e.to_string()))?;

        let (base_lsn, header_txn_id, format) = if len == 0 {
            // A fresh log is a new database at this path: its quarantine starts fresh too (review 5's
            // F6). BEFORE the header (review 6's caveat 2): until the move succeeds the log stays
            // empty, so every later open retries it rather than taking the old file as current.
            crate::wal::txn::start_fresh_quarantine(&path).map_err(|e| {
                FerroError::Wal(format!("a fresh log could not move the earlier release quarantine aside ({e})"))
            })?;
            let mut header = [0u8; HEADER_SIZE];
            header[0..4].copy_from_slice(&MAGIC.to_be_bytes());
            header[4..8].copy_from_slice(&VERSION.to_be_bytes());
            header[8..16].copy_from_slice(&INITIAL_LSN.to_be_bytes());
            header[16..24].copy_from_slice(&1u64.to_be_bytes());
            pwrite_all(&*file, &header, 0)?;
            file.sync_all().map_err(|e| FerroError::Wal(e.to_string()))?;
            (INITIAL_LSN, 1u64, VERSION)
        } else {
            let mut header = [0u8; HEADER_SIZE];
            pread_all(&*file, &mut header, 0)?;
            if u32::from_be_bytes(header[0..4].try_into().unwrap()) != MAGIC {
                return Err(FerroError::Wal("incorrect magic".into()));
            }
            let version = u32::from_be_bytes(header[4..8].try_into().unwrap());
            if version != VERSION && version != LEGACY_VERSION {
                return Err(FerroError::Wal("incorrect wal version".into()));
            }
            let base = u64::from_be_bytes(header[8..16].try_into().unwrap());
            let txn_hwn = u64::from_be_bytes(header[16..24].try_into().unwrap());
            (base, txn_hwn, version)
        };
        let valid_end = scan_valid_end(&*file, base_lsn, len)?;
        let file_end = HEADER_SIZE as u64 + (valid_end - base_lsn);
        if file_end < len {
            file.set_len(file_end).map_err(|e| FerroError::Wal(e.to_string()))?;
            file.sync_all().map_err(|e| FerroError::Wal(e.to_string()))?;
        }
        Ok(Self {file: Mutex::new(file), buffer: Mutex::new(WalBuffer { bytes: Vec::new(), start_lsn: valid_end }), next_lsn: AtomicU64::new(valid_end), flushed_lsn: AtomicU64::new(valid_end), base_lsn: AtomicU64::new(base_lsn), path, header_txn_id, pins: Mutex::new(std::collections::BTreeMap::new()), next_pin_id: AtomicU64::new(1),
            #[cfg(test)]
            fail_next_append: std::sync::atomic::AtomicBool::new(false),
            format: AtomicU32::new(format), poisoned: AtomicBool::new(false), poison_reason: Mutex::new(None)})
    }

    /// The format this log is in: 3, or 2 for a log written before D213 that has not been
    /// checkpointed since. See [`VERSION`].
    pub fn format_version(&self) -> u32 {
        self.format.load(Ordering::SeqCst)
    }

    /// Whether this log was written before D213: a forward `HeapDelete` in it FREED its slot.
    pub fn is_legacy(&self) -> bool {
        self.format_version() == LEGACY_VERSION
    }

    /// **Fail-stop: from now on the log refuses every append and every flush.** The adversary's F4(i)
    /// on `115f0b7`, and the lead's decision.
    ///
    /// Called when a transaction's `Commit` record could not be flushed (`TxnManager::commit`). The
    /// bytes may or may not be on disk, and a failed flush puts them back in the buffer, so a later
    /// flush could still make that `Commit` durable. Anything written after it is then built on an
    /// outcome nobody knows: a ROLLBACK would log an Abort after a `Commit` that later lands, and
    /// recovery would redo both. So nothing is written after it. The database must be reopened, and
    /// recovery decides from what actually reached disk. This is PostgreSQL's answer to an fsync
    /// failure (it PANICs), for the same reason.
    ///
    /// Reads are not refused while the pages they need are resident. The undecided transaction is
    /// still in the active set, so no other snapshot sees its rows. A page write is refused only if it
    /// needs a flush, because `flush_up_to` does not flush for an LSN that is already durable; so once
    /// eviction picks a dirty page past the durable end, the fetch that needed the frame fails too.
    ///
    /// **It also marks every index stale** (review 2's C3, the lead's decision): the reopen is this
    /// function's whole contract, and index pages are not logged. If the undecided transaction's
    /// records never reached disk and the log holds nothing else, the reopen replays nothing and would
    /// not rebuild, so an index page flushed with that transaction's entries would name slots the heap
    /// never got. The marker makes `open_recovered` rebuild every tree.
    pub fn poison(&self, why: &str) {
        use std::io::Write;
        {
            let mut reason = self.poison_reason.lock().unwrap();
            if reason.is_some() {
                return;
            }
            *reason = Some(why.to_string());
        }
        self.poisoned.store(true, Ordering::SeqCst);
        let marked = crate::wal::txn::write_stale_indexes_marker(&self.path, &format!("the log was poisoned: {why}"));
        let _ = writeln!(
            std::io::stderr(),
            "ferrodb: the log refuses every write from now on ({why}); reopen the database{}",
            match marked {
                Ok(()) => ", and every index is rebuilt then".to_string(),
                Err(e) => format!(
                    "; the marker that makes that open rebuild every index could not be written ({e})"
                ),
            }
        );
    }

    /// Why this log refuses writes, once it does. See [`WalManager::poison`].
    pub fn poisoned(&self) -> Option<String> {
        if !self.poisoned.load(Ordering::SeqCst) {
            return None;
        }
        self.poison_reason.lock().unwrap().clone()
    }

    fn refuse_if_poisoned(&self) -> Result<(), FerroError> {
        match self.poisoned() {
            None => Ok(()),
            Some(why) => Err(FerroError::Wal(format!(
                "the log refuses every write since {why}; reopen the database, and recovery decides \
                 from what reached disk"
            ))),
        }
    }

    /// Pin the log at its current durable frontier, and return where that turned out to be.
    ///
    /// **Reading the LSN and registering the claim happen under one lock, and that is the whole
    /// point.** Doing it in two steps — read `flushed_lsn`, then pin it — is the check-then-act
    /// shape that has produced six separate defects in this codebase: a truncation landing in the
    /// gap leaves a pin on an LSN that has already been discarded, which is exactly the bug the
    /// pin was added to prevent, reintroduced by the fix for it. [`WalManager::truncate`] takes
    /// the same lock, so there is no gap to land in.
    ///
    /// Lock order is pins -> buffer -> file, matching `truncate`. (D250 removed the pin fence that #16
    /// put first.)
    pub fn pin_durable(self: &std::sync::Arc<Self>) -> WalPin {
        let mut pins = self.pins.lock().unwrap();
        let lsn = self.flushed_lsn.load(Ordering::SeqCst);
        let id = self.next_pin_id.fetch_add(1, Ordering::SeqCst);
        pins.insert(id, lsn);
        WalPin { wal: std::sync::Arc::clone(self), id, lsn }
    }

    /// Pin a specific LSN, refusing if it has already been truncated away.
    ///
    /// Refuses rather than clamping: a caller asking for an LSN the log no longer holds has state
    /// built on records that are gone, and silently moving the pin forward would hand it a
    /// plausible-looking claim over the wrong range.
    pub fn pin(self: &std::sync::Arc<Self>, lsn: u64) -> Result<WalPin, FerroError> {
        let mut pins = self.pins.lock().unwrap();
        let base = self.base_lsn.load(Ordering::SeqCst);
        if lsn < base {
            return Err(FerroError::Wal(format!(
                "cannot pin lsn {lsn}: the log has already been truncated to base {base}"
            )));
        }
        let id = self.next_pin_id.fetch_add(1, Ordering::SeqCst);
        pins.insert(id, lsn);
        Ok(WalPin { wal: std::sync::Arc::clone(self), id, lsn })
    }

    /// The oldest LSN any pin still needs, if there are any.
    pub fn min_pinned_lsn(&self) -> Option<u64> {
        self.pins.lock().unwrap().values().min().copied()
    }

    pub fn read_record(&self, lsn: u64) -> Result<(LogRecord, u64), FerroError> {
        // Whether a record is still in the buffer is decided UNDER the buffer lock, using the
        // buffer's own `start_lsn`, and read in the same critical section.
        //
        // It used to branch on the `flushed_lsn` atomic and only then take the lock. Between those
        // two steps another thread could flush, advancing `start_lsn` past `lsn`, and
        // `lsn - buffer.start_lsn` then UNDERFLOWED — a subtract-with-overflow panic that poisoned
        // the WAL mutexes, so one aborting transaction took down every other thread in the process
        // with `PoisonError`. Reproduced by concurrent commit/abort, which walks the record chain.
        let buffered = {
            let buffer = self.buffer.lock().unwrap();
            if lsn >= buffer.start_lsn {
                let rel = (lsn - buffer.start_lsn) as usize; // safe: guarded above, same lock
                if rel + 4 > buffer.bytes.len() {
                    return Err(FerroError::Wal("lsn past end of buffer".into()));
                }
                let total =
                    u32::from_be_bytes(buffer.bytes[rel..rel + 4].try_into().unwrap()) as usize;
                if total < MIN_FRAME || rel + total > buffer.bytes.len() {
                    return Err(FerroError::Wal("record goes past buffer".into()));
                }
                Some(buffer.bytes[rel..rel + total].to_vec())
            } else {
                None
            }
        };

        let frame = if let Some(f) = buffered {
            f
        } else {
            let file = self.file.lock().unwrap();
            // `truncate` can advance `base_lsn` past an lsn a caller still holds, which underflowed
            // here for the same reason. Refuse with a description instead of panicking.
            let rel = lsn.checked_sub(self.base_lsn.load(Ordering::SeqCst)).ok_or_else(|| {
                FerroError::Wal(format!(
                    "lsn {lsn} is below the log's base; it was truncated away"
                ))
            })?;
            let offset = HEADER_SIZE as u64 + rel;
            let mut len_buf = [0u8; 4];
            pread_all(&**file, &mut len_buf, offset)?;
            let total = u32::from_be_bytes(len_buf) as usize;
            if total < MIN_FRAME {
                return Err(FerroError::Wal("incorrect record length".into()));
            }
            let mut buf = vec![0u8; total];
            pread_all(&**file, &mut buf, offset)?;
            buf
        };
        let total = frame.len();
        if total < MIN_FRAME {
            return Err(FerroError::Wal("record too short".into()));
        }
        let rec_lsn = u64::from_be_bytes(frame[4..12].try_into().unwrap());
        let stored = u32::from_be_bytes(frame[total - 4..total].try_into().unwrap());
        if crc32(&frame[..total - 4]) != stored {
            return Err(FerroError::Wal("crc doesn't match".into()));
        }
        let prev_lsn = u64::from_be_bytes(frame[12..20].try_into().unwrap());
        let txn_id = u64::from_be_bytes(frame[20..28].try_into().unwrap());
        let kind = RecKind::deserialize(&frame[28..total-4])?;
        WAL_RECORDS_READ.fetch_add(1, Ordering::Relaxed);
        WAL_BYTES_READ.fetch_add(total as u64, Ordering::Relaxed);
        Ok((LogRecord {lsn: rec_lsn, prev_lsn, txn_id, kind}, lsn + total as u64))
    }

    // |total_len: u32|lsn: u64|prev_lsn: u64|txn_id: u64|tag: u8|payload: ...|crc32: u32|
    pub fn append(&self, txn_id: u64, prev_lsn: u64, kind: &RecKind) -> Result<u64, FerroError> {
        // One-shot, and it disarms itself so a test can fail exactly the append it means to and let
        // the cleanup that follows succeed. Compiled out entirely outside this crate's unit tests.
        #[cfg(test)]
        if self.fail_next_append.swap(false, Ordering::SeqCst) {
            return Err(FerroError::Wal("injected append failure".into()));
        }
        self.refuse_if_poisoned()?;
        // **Review 2's Q5: the log itself refuses the two records whose meaning changed, while it
        // is version 2.** A top-level `HeapDelete` RETIRES its slot since D213 and FREED it before,
        // and a `HeapRelease` did not exist. `TxnManager::begin_locked` already refuses a transaction
        // on such a log, but that guard cannot see a direct `append`. Everything else means the same
        // in both formats, including a `Clr` carrying a `HeapDelete` (an undone insert frees in both),
        // so recovery's undo of a version-2 log's losers is unaffected.
        if self.is_legacy() && matches!(kind, RecKind::HeapDelete { .. } | RecKind::HeapRelease { .. }) {
            return Err(FerroError::Wal(format!(
                "this log was written before D213 (format {LEGACY_VERSION}), where this record means \
                 something else; it takes one only after open_recovered has replayed and upgraded it"
            )));
        }
        let mut buffer = self.buffer.lock().unwrap();
        let lsn = self.next_lsn.load(Ordering::SeqCst);
        let mut body = Vec::new();
        body.extend_from_slice(&lsn.to_be_bytes());
        body.extend_from_slice(&prev_lsn.to_be_bytes());
        body.extend_from_slice(&txn_id.to_be_bytes());
        kind.serialize(&mut body)?;
        let total_len = (4 + body.len() + 4) as u32;

        let start = buffer.bytes.len();
        buffer.bytes.extend_from_slice(&total_len.to_be_bytes());
        buffer.bytes.extend_from_slice(&body);
        let crc = crc32(&buffer.bytes[start..]);
        buffer.bytes.extend_from_slice(&crc.to_be_bytes());
        self.next_lsn.fetch_add(total_len as u64, Ordering::SeqCst);
        Ok(lsn)
    }

    /// The raw bytes of the frame at `lsn`, for shipping to a replica verbatim.
    ///
    /// Replication sends the log's own bytes rather than re-serialising a parsed record: the CRC
    /// already in the frame then covers what actually crosses the wire, so a replica validates the
    /// primary's bytes rather than trusting that two encoders agree.
    ///
    /// Same lock discipline as `read_record`: the buffer-or-file decision is made under the buffer
    /// lock and the read happens in that same critical section, because deciding from the
    /// `flushed_lsn` atomic and then locking is exactly what underflowed in D23.
    pub fn raw_frame(&self, lsn: u64, len: usize) -> Result<Vec<u8>, FerroError> {
        let buffered = {
            let buffer = self.buffer.lock().unwrap();
            if lsn >= buffer.start_lsn {
                let rel = (lsn - buffer.start_lsn) as usize;
                if rel + len > buffer.bytes.len() {
                    return Err(FerroError::Wal("frame runs past the buffer".into()));
                }
                Some(buffer.bytes[rel..rel + len].to_vec())
            } else {
                None
            }
        };
        if let Some(b) = buffered {
            return Ok(b);
        }
        let file = self.file.lock().unwrap();
        let rel = lsn.checked_sub(self.base_lsn.load(Ordering::SeqCst)).ok_or_else(|| {
            FerroError::Wal(format!("lsn {lsn} is below the log's base; it was truncated away"))
        })?;
        let mut buf = vec![0u8; len];
        pread_all(&**file, &mut buf, HEADER_SIZE as u64 + rel)?;
        Ok(buf)
    }

    /// Discard the log and restart it at the current end.
    ///
    /// **A pin below that point cancels the truncation.** This log cannot be truncated part-way —
    /// it is thrown away whole and restarted — so honouring a pin means keeping everything. The
    /// checkpoint still succeeds; it simply reclaims nothing this time.
    ///
    /// The cost is the same one PostgreSQL replication slots have: a pin nobody releases makes the
    /// WAL grow without bound. That is a real hazard and it is not guarded here beyond
    /// [`WalManager::min_pinned_lsn`] being available to look at. It is the right trade against the
    /// alternative, which is discarding records a replica has been promised and only finding out
    /// when the replica is refused.
    pub fn truncate(&self, next_txn_id: u64) -> Result<(), FerroError> {
        self.flush()?;
        // Taken first and held across the decision, so a pin cannot be registered against a range
        // this call is in the middle of discarding. `pin_durable` reads the frontier under this
        // same lock for the same reason.
        let pins = self.pins.lock().unwrap();
        let mut buffer = self.buffer.lock().unwrap();
        let file = self.file.lock().unwrap();
        let next = self.next_lsn.load(Ordering::SeqCst);

        if let Some(&oldest) = pins.values().min() {
            if oldest < next {
                // Something still needs records below the new base. Keep the log.
                return Ok(());
            }
        }

        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        header[4..8].copy_from_slice(&VERSION.to_be_bytes());
        header[8..16].copy_from_slice(&next.to_be_bytes());
        header[16..24].copy_from_slice(&next_txn_id.to_be_bytes());
        pwrite_all(&**file, &mut header, 0)?;
        file.sync_data().map_err(|e| FerroError::Wal(e.to_string()))?;
        file.set_len(HEADER_SIZE as u64).map_err(|e| FerroError::Wal(e.to_string()))?;
        file.sync_all().map_err(|e| FerroError::Wal(e.to_string()))?;
        // Every record of the older format is gone with the truncation, so the log is version 3 now.
        self.format.store(VERSION, Ordering::SeqCst);
        
        self.base_lsn.store(next, Ordering::SeqCst);
        buffer.bytes.clear();
        buffer.start_lsn = next;
        self.flushed_lsn.store(next, Ordering::SeqCst);
        Ok(())

    }


    pub fn flush(&self) -> Result<(), FerroError> {
        // The buffer lock is held across the file write, and that is the correctness fix rather
        // than caution.
        //
        // It used to be released after draining, so two flushes could overlap. Thread A drains
        // [100,200) and thread B drains [200,300); if B reaches the file first, `fetch_max` puts
        // `flushed_lsn` at 300 while [100,200) is still only in memory. `fetch_max` cannot express
        // "durable to 300 except for a hole", and `flush_up_to` reads that number as a guarantee —
        // so the buffer pool would write a data page to disk before the log record describing it,
        // which is the single rule write-ahead logging exists to enforce. Measured before the fix:
        // over-reporting by up to ~117,000 bytes across 400 samples taken during the race.
        //
        // Order is buffer -> file, matching `truncate` and `read_record`; taking the file lock
        // first here would invert against them.
        //
        // The cost is that appends block for the duration of an fsync, because `append` also takes
        // the buffer lock. That is the honest price of a single-buffer WAL, and a faster log that
        // lies about durability is not a better one.
        self.refuse_if_poisoned()?;
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.bytes.is_empty() {
            return Ok(());
        }
        let start_lsn = buffer.start_lsn;
        let bytes = take(&mut buffer.bytes);
        buffer.start_lsn = bytes.len() as u64 + start_lsn;

        let offset = HEADER_SIZE as u64 + (start_lsn - self.base_lsn.load(Ordering::SeqCst));
        let wrote = {
            let file = self.file.lock().unwrap();
            pwrite_all(&**file, &bytes, offset).and_then(|()| {
                FSYNC_CALLS.fetch_add(1, Ordering::Relaxed);
                FSYNC_BYTES.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                file.sync_data().map_err(|e| FerroError::Wal(e.to_string()))
            })
        };
        if let Err(e) = wrote {
            // The bytes were drained but never reached disk. Putting them back keeps them
            // recoverable by a later flush; dropping them would lose committed log records on an
            // error path, which is a worse outcome than the error itself.
            buffer.start_lsn = start_lsn;
            let mut restored = bytes;
            restored.append(&mut buffer.bytes);
            buffer.bytes = restored;
            return Err(e);
        }
        self.flushed_lsn.fetch_max(start_lsn + bytes.len() as u64, Ordering::SeqCst);
        Ok(())
    }

    pub fn flush_up_to(&self, lsn: u64) -> Result<(), FerroError> {
        if self.flushed_lsn.load(Ordering::SeqCst) >= lsn {
            return Ok(());
        }
        self.flush()
    }
}

pub fn scan_valid_end(file: &dyn Storage, base_lsn: u64, file_len: u64) -> Result<u64, FerroError>{
    let mut offset = HEADER_SIZE as u64;
    loop {
        if offset + 4 > file_len {
            break;
        }
        let mut len_buf = [0u8; 4];
        pread_all(file, &mut len_buf, offset)?;
        let total = u32::from_be_bytes(len_buf) as u64;
        if total < MIN_FRAME as u64 || offset + total > file_len {
            break;
        }
        let mut frame = vec![0u8; total as usize];
        pread_all(file, &mut frame, offset)?;
        let stored = u32::from_be_bytes(frame[total as usize - 4..].try_into().unwrap());
        if crc32(&frame[..total as usize - 4]) != stored {
            break;
        }
        let expected_lsn = base_lsn + (offset - HEADER_SIZE as u64);
        let embedded = u64::from_be_bytes(frame[4..12].try_into().unwrap());
        if embedded != expected_lsn {
            break;
        }
        offset += total;
    }
    Ok(base_lsn + (offset - HEADER_SIZE as u64))
}

fn read_heap(bytes: &[u8]) -> Result<(u32, u32, u16, usize), FerroError> {
    let dir_root = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
    let page_id = u32::from_be_bytes(bytes[5..9].try_into().unwrap());
    let slot = u16::from_be_bytes(bytes[9..11].try_into().unwrap());
    let length = u32::from_be_bytes(bytes[11..15].try_into().unwrap()) as usize;
    Ok((dir_root, page_id, slot, length))
}

fn crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 == 1 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

pub fn crc32(data: &[u8]) -> u32 {
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(crc32_table);

    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ table[idx];
    }
    crc ^ 0xFFFF_FFFF
}

pub fn pwrite_all(file: &dyn Storage, mut buf: &[u8], mut offset: u64) -> Result<(), FerroError> {
    while !buf.is_empty() {
        match file.pwrite(buf, offset) {
            Ok(0) => return Err(FerroError::Wal("wrote 0 bytes".into())),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) => return Err(FerroError::Wal(e.to_string()))
        }
    }
    Ok(())
}

pub fn pread_all(file: &dyn Storage, buf: &mut [u8], mut offset: u64) -> Result<(), FerroError>{
    let mut total_read = 0;
    while total_read < buf.len() {
        match file.pread(&mut buf[total_read..], offset) {
            Ok(0) => return Err(FerroError::Wal("eof before finished record".into())),
            Ok(n) => {
                total_read += n;
                offset += n as u64;
            }
            Err(e) => return Err(FerroError::Wal(e.to_string()))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (WalManager, tempfile::TempDir){
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        (WalManager::new(path).unwrap(), dir)
    }

    /// **A corrupted record is refused, not returned.**
    ///
    /// Found by a mutation sweep: replacing this CRC check with `if false` left all 821 tests green.
    /// The page checksum's equivalent mutation was caught, so the gap was specifically the WAL — the
    /// substrate durability rests on. Nothing in the suite had ever handed the reader a damaged
    /// record, so the database's ability to notice damage at all was unverified.
    #[test]
    fn a_record_whose_bytes_were_corrupted_is_refused() {
        let (w, dir) = setup();
        let lsn = w.append(1, 0, &RecKind::Begin).unwrap();
        w.flush().unwrap();

        // Anti-vacuity: it reads back cleanly before the corruption, so the refusal below is about
        // the damage and not about this record being unreadable in the first place.
        w.read_record(lsn).expect("an intact record was refused");

        // Flip one bit inside the frame, leaving its length prefix intact so the reader still
        // believes it has a whole record — which is precisely the case a checksum exists for.
        let path = dir.path().join("test.wal");
        let mut bytes = std::fs::read(&path).unwrap();
        let at = HEADER_SIZE + 24;
        assert!(at < bytes.len(), "the wal is too small to corrupt at a fixed offset");
        bytes[at] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        // The SAME manager, deliberately: after `flush` its buffer is drained and `start_lsn` has
        // advanced past this record, so `read_record` takes the file path — which is the one the
        // CRC check lives on. Reopening instead lands in the buffer branch and fails for an
        // unrelated reason, which is how the first version of this test "passed" the wrong way.
        let err = match w.read_record(lsn) {
            Err(e) => e,
            Ok(_) => panic!("a record with a corrupted body was returned as if it were valid"),
        };
        assert!(
            format!("{err}").contains("crc"),
            "it failed, but not because the checksum caught it: {err}"
        );
    }

    /// **A torn tail truncates the log rather than being replayed.**
    ///
    /// The other survivor of the same sweep. `scan_valid_end` decides how much of the log recovery
    /// is allowed to trust; if its CRC check stops working, recovery replays whatever bytes happen
    /// to be there — which after a crash is exactly the half-written record the check exists to
    /// stop.
    #[test]
    fn the_recovery_scan_stops_at_a_corrupted_record() {
        let (w, dir) = setup();
        for _ in 0..4 {
            w.append(1, 0, &RecKind::Begin).unwrap();
        }
        w.flush().unwrap();

        let path = dir.path().join("test.wal");
        let base = w.base_lsn.load(Ordering::SeqCst);
        let file = std::fs::File::open(&path).unwrap();
        let len = file.metadata().unwrap().len();
        let clean_end = scan_valid_end(&file, base, len).unwrap();
        assert!(clean_end > HEADER_SIZE as u64, "the undamaged scan trusted no records at all");
        drop(file);

        // Corrupt a record in the middle. Everything after it must be treated as untrustworthy,
        // because a log is only meaningful as a prefix.
        let mut bytes = std::fs::read(&path).unwrap();
        let at = HEADER_SIZE + 24;
        bytes[at] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let torn_end = scan_valid_end(&file, base, len).unwrap();
        assert!(
            torn_end < clean_end,
            "the scan trusted {torn_end} bytes of a log corrupted at {at}; it trusted the same \
             {clean_end} as an undamaged one, so recovery would replay the damage"
        );
    }

    #[test]
    fn test_crc32_val() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_ne!(crc32(b"123456789"), crc32(b"123456780"));
    }

    #[test]
    fn test_reckind_roundtrip() {
        let cases = vec![
            RecKind::Begin, 
            RecKind::Commit, 
            RecKind::Abort,
            RecKind::TxnEnd,
            RecKind::Checkpoint,
            RecKind::HeapInsert { dir_root: 1, page_id: 2, slot: 3, tuple: vec![4, 5, 6] },
            RecKind::HeapDelete { dir_root: 1, page_id: 2, slot: 4, old: vec![7, 8, 9] },
            RecKind::HeapUpdate { dir_root: 1, page_id: 3, slot: 4, old: vec![1], new: vec![4,5] },
            RecKind::Clr { undone_lsn: 2, undo_next: 4, redo: Box::new(RecKind::HeapUpdate { dir_root: 1, page_id: 3, slot: 4, old: vec![4, 5], new: vec![1] }) },
            RecKind::RunIdentity { run: crate::provenance::RunEntity::new(
                crate::provenance::ProvId(7),
                "restock-agent",
                "run-42",
                "claude-opus",
                "2026-05",
                [0xab; 32],
                1_700_000_000_000,
                crate::branch::types::BranchId::new(4, 3),
            ) },
        ];

        for case in cases {
            let mut buf = Vec::new();
            case.serialize(&mut buf).unwrap();
            assert_eq!(RecKind::deserialize(&buf).unwrap(), case);
        }
    }

    #[test]
    fn test_survives_flush_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.wal");
        let (l0, l1, l2);
        {
            let wal = WalManager::new(path.clone()).unwrap();
            l0 = wal.append(1, 0, &RecKind::Begin).unwrap();
            l1 = wal.append(1, l0, &RecKind::HeapInsert {
                dir_root: 5, page_id: 10, slot: 2, tuple: vec![0xAA, 0xBB],
            }).unwrap();
            l2 = wal.append(1, l1, &RecKind::Commit).unwrap();
            wal.flush().unwrap();
        }
        let wal = WalManager::new(path).unwrap();
        let (r0, _) = wal.read_record(l0).unwrap();
        let (r1, _) = wal.read_record(l1).unwrap();
        let (r2, _) = wal.read_record(l2).unwrap();

        assert_eq!(&r0.kind, &RecKind::Begin);
        assert_eq!(&r1.kind, &RecKind::HeapInsert {
            dir_root: 5, page_id: 10, slot: 2, tuple: vec![0xAA, 0xBB],
        });
        assert_eq!(&r2.kind, &RecKind::Commit);
        assert_eq!(r1.prev_lsn, l0);
        assert_eq!(r2.prev_lsn, l1);
        assert_eq!(r1.txn_id, 1);
    }

    #[test]
    fn test_corrupted_last_record_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("torn.wal");

        let l1;
        {
            let wal = WalManager::new(path.clone()).unwrap();
            let l0 = wal.append(1, 0, &RecKind::Begin).unwrap();
            l1 = wal.append(1, l0, &RecKind::HeapInsert { dir_root: 1, page_id: 1, slot: 0, tuple: vec![1,2,3,4,5,6] }).unwrap();
            wal.flush().unwrap();
        }

        let f = OpenOptions::new().write(true).open(&path).unwrap();
        let len = f.metadata().unwrap().len();
        f.set_len(len - 3).unwrap();
        drop(f);

        let wal = WalManager::new(path).unwrap();
        assert!(wal.read_record(l1).is_err());
    }

    #[test]
    fn test_flush_up_to_stops_when_durable() {
        let (wal, _dir) = setup();
        let l0 = wal.append(1, 0, &RecKind::Begin).unwrap();
        wal.flush().unwrap();
        let flushed = wal.flushed_lsn.load(Ordering::SeqCst);
        wal.flush_up_to(l0).unwrap();
        assert_eq!(wal.flushed_lsn.load(Ordering::SeqCst), flushed);
    }

    #[test]
    fn test_read_buffer_before_flush() {
        let (wal, _dir) = setup();
        let l0 = wal.append(3, 0, &RecKind::Begin).unwrap();
        let l1 = wal.append(3, l0, &RecKind::Abort).unwrap();
        assert_eq!(wal.read_record(l0).unwrap().0.kind, RecKind::Begin);
        assert_eq!(wal.read_record(l1).unwrap().0.kind, RecKind::Abort);
    }

    #[test]
    fn test_lsns_are_monotonic() {
        let (wal, _dir) = setup();
        let l0 = wal.append(1, 0, &RecKind::Begin).unwrap();
        let l1 = wal.append(1, l0, &RecKind::Abort).unwrap();
        assert_eq!(l0, INITIAL_LSN);
        assert!(l1 > l0);
        assert_eq!(l1, wal.read_record(l0).unwrap().1);
    }

    #[test]
    fn deserialize_rejects_empty_and_unknown_tag() {
        assert!(RecKind::deserialize(&[]).is_err());
        assert!(RecKind::deserialize(&[99]).is_err());
    }

    /// **D213: `HeapRelease` round-trips under tag 11, and a truncated one is refused.** Tag 11 is
    /// the next free number, so the older tags checked below keep their meaning.
    #[test]
    fn a_heap_release_round_trips_under_its_own_tag() {
        let kind = RecKind::HeapRelease { dir_root: 7, page_id: 42, slot: 3 };
        let mut buf = Vec::new();
        kind.serialize(&mut buf).unwrap();
        assert_eq!(buf[0], 11, "HeapRelease is not tag 11");
        assert_eq!(buf.len(), 11, "a HeapRelease is its tag plus a u32, a u32 and a u16");
        assert_eq!(RecKind::deserialize(&buf).unwrap(), kind);
        assert!(RecKind::deserialize(&buf[..10]).is_err(), "a truncated HeapRelease decoded");
    }

    /// **The additive-tag discipline, for tag 10.**
    ///
    /// Tags 0..9 were taken when `RunIdentity` arrived, so it took the next free number. A log
    /// written before it existed contains none of them, and every record in it must still decode —
    /// which is the property that lets this variant be added to a running database at all. Checked
    /// by decoding one of every older tag after the new one was introduced.
    ///
    /// Breaking shape: reusing an existing tag, or renumbering. Either produces a build in which
    /// yesterday's log decodes into today's record type with the wrong fields, which is far worse
    /// than a decode error because it succeeds.
    #[test]
    fn an_older_logs_records_still_decode_after_the_new_tag_was_added() {
        for (tag, kind) in [
            (0u8, RecKind::Begin),
            (1, RecKind::Commit),
            (2, RecKind::Abort),
            (3, RecKind::TxnEnd),
            (4, RecKind::Checkpoint),
        ] {
            let mut buf = Vec::new();
            kind.serialize(&mut buf).unwrap();
            assert_eq!(buf[0], tag, "the tag of {kind:?} moved; an existing log now misdecodes");
            assert_eq!(RecKind::deserialize(&buf).unwrap(), kind);
        }
        let mut buf = Vec::new();
        RecKind::Ddl {
            op: DdlOp::CreateTable,
            table: "t".into(),
            dir_root: 1,
            time_travel_root: 2,
            columns: vec![("c".into(), DataType::Integer, true)],
        }
        .serialize(&mut buf).unwrap();
        assert_eq!(buf[0], 9, "the DDL tag moved");

        let mut buf = Vec::new();
        RecKind::RunIdentity {
            run: crate::provenance::RunEntity::new(
                crate::provenance::ProvId(1),
                "a",
                "r",
                "m",
                "v",
                [0u8; 32],
                0,
                crate::branch::types::BranchId::TRUNK,
            ),
        }
        .serialize(&mut buf).unwrap();
        assert_eq!(buf[0], 10, "run identity must be tag 10; 0..9 are taken by existing logs");
    }

    /// A run identity record must name a run. `ProvId::NONE` is the value meaning *unattributed*,
    /// so a record carrying it would claim a writer and name none — and every row of that commit
    /// would then be attributed to a slot that resolves to nothing.
    #[test]
    fn a_run_identity_record_that_names_no_run_is_refused() {
        let run = crate::provenance::RunEntity::new(
            crate::provenance::ProvId(3),
            "restock-agent",
            "run-42",
            "claude-opus",
            "2026-05",
            [1u8; 32],
            5,
            crate::branch::types::BranchId::new(2, 0),
        );
        let mut buf = Vec::new();
        RecKind::RunIdentity { run }.serialize(&mut buf).unwrap();
        // Anti-vacuity: it decodes as written, so the refusal below is about the id and not about
        // the record being unreadable.
        RecKind::deserialize(&buf).expect("a well-formed run identity record was refused");

        // prov_id occupies bytes 1..5.
        buf[1..5].copy_from_slice(&0u32.to_be_bytes());
        let err = RecKind::deserialize(&buf).expect_err("a record naming ProvId::NONE was accepted");
        assert!(format!("{err}").contains("ProvId::NONE"), "{err}");
    }

    /// A truncated run identity record is refused rather than indexed past. These bytes arrive from
    /// a disk; reading off the end of one panics the whole process.
    #[test]
    fn a_truncated_run_identity_record_is_refused_rather_than_panicking() {
        let mut buf = Vec::new();
        RecKind::RunIdentity {
            run: crate::provenance::RunEntity::new(
                crate::provenance::ProvId(1),
                "restock-agent",
                "run-42",
                "claude-opus",
                "2026-05",
                [9u8; 32],
                77,
                crate::branch::types::BranchId::new(1, 0),
            ),
        }
        .serialize(&mut buf).unwrap();
        assert!(RecKind::deserialize(&buf).is_ok(), "the intact record was refused");
        for cut in 1..buf.len() {
            let err = RecKind::deserialize(&buf[..cut]);
            assert!(err.is_err(), "a record truncated to {cut} bytes decoded as if it were whole");
        }
    }

    // ---- B11: column-level DDL records ---------------------------------------------------------

    fn ddl(op: DdlOp, columns: Vec<(String, DataType, bool)>) -> RecKind {
        RecKind::Ddl { op, table: "inventory".into(), dir_root: 7, time_travel_root: 8, columns }
    }

    fn round_trip(rec: &RecKind) -> RecKind {
        let mut bytes = Vec::new();
        rec.serialize(&mut bytes).unwrap();
        RecKind::deserialize(&bytes).expect("a record this code just wrote did not read back")
    }

    /// **D212 (a'): a REVERT history part round-trips, and a truncated one is refused.**
    #[test]
    fn a_revert_history_part_round_trips_and_a_truncated_one_is_refused() {
        let rec = RecKind::RevertHistory {
            hseq: 7,
            ordinal: 3,
            part: 1,
            last: true,
            bytes: b"opaque history bytes".to_vec(),
        };
        assert_eq!(round_trip(&rec), rec);
        let mut bytes = Vec::new();
        rec.serialize(&mut bytes).unwrap();
        assert_eq!(bytes[0], 12, "tag 12, the next free number after HeapRelease's 11");
        assert!(RecKind::deserialize(&bytes[..bytes.len() - 1]).is_err());
    }

    /// **Breaking shape: an alteration whose payload is not recoverable from the resulting shape.**
    ///
    /// A rename's old name and a retype's old type exist nowhere except in the op itself. If the
    /// op byte were written without its payload — as tags 0 and 1 are — these three records would
    /// all deserialize as the same thing, and a consumer would see a rename as a drop plus an add.
    #[test]
    fn every_column_alteration_survives_the_log_round_trip() {
        let shape = vec![
            ("id".to_string(), DataType::Integer, false),
            ("note".to_string(), DataType::Varchar(20), true),
        ];
        for op in [
            DdlOp::AlterColumn(ColumnAlteration::Add { column: "note".into() }),
            DdlOp::AlterColumn(ColumnAlteration::Rename {
                from: "memo".into(),
                to: "note".into(),
            }),
            DdlOp::AlterColumn(ColumnAlteration::Retype {
                column: "note".into(),
                from: DataType::Varchar(4),
            }),
        ] {
            let rec = ddl(op.clone(), shape.clone());
            assert_eq!(round_trip(&rec), rec, "{op:?} did not survive serialization");
        }
    }

    /// **Anti-vacuity, and the compatibility claim in `serialize`'s comment, measured.**
    ///
    /// Tags 0 and 1 must still lay down the exact bytes they laid down before the alteration
    /// payload existed, or every DDL record already in a log stops replaying. Breaking shape: an
    /// alteration payload written unconditionally after the op byte.
    #[test]
    fn create_and_drop_records_keep_their_byte_layout() {
        let shape = vec![("id".to_string(), DataType::Integer, false)];
        for (op, tag) in [(DdlOp::CreateTable, 0u8), (DdlOp::DropTable, 1u8)] {
            let rec = ddl(op.clone(), shape.clone());
            let mut bytes = Vec::new();
            rec.serialize(&mut bytes).unwrap();
            assert_eq!(bytes[0], 9, "the record kind tag moved");
            assert_eq!(bytes[1], tag, "the ddl op tag moved");
            // dir_root is the next four bytes, exactly as before: nothing was inserted between.
            assert_eq!(u32::from_be_bytes(bytes[2..6].try_into().unwrap()), 7);
            assert_eq!(round_trip(&rec), rec);
        }
    }

    /// A truncated alteration payload must be refused, not indexed past. These bytes arrive off a
    /// disk. Breaking shape: a record cut anywhere inside the alteration's strings.
    #[test]
    fn a_truncated_alteration_is_refused_rather_than_panicking() {
        let rec = ddl(
            DdlOp::AlterColumn(ColumnAlteration::Rename {
                from: "memo".into(),
                to: "note".into(),
            }),
            vec![("note".to_string(), DataType::Varchar(20), true)],
        );
        let mut bytes = Vec::new();
        rec.serialize(&mut bytes).unwrap();
        for cut in 2..bytes.len() {
            // Every prefix is either a clean refusal or, for a prefix that happens to be a valid
            // shorter record, something that is not this record. Never a panic.
            let _ = RecKind::deserialize(&bytes[..cut]);
        }
        assert!(RecKind::deserialize(&bytes[..4]).is_err(), "a 4-byte alteration record parsed");
    }

    /// An alteration tag this build does not know is refused by name, not silently read as one it
    /// does know. Breaking shape: a log written by a newer build.
    #[test]
    fn an_unknown_alteration_tag_is_refused() {
        let rec = ddl(
            DdlOp::AlterColumn(ColumnAlteration::Add { column: "note".into() }),
            vec![("note".to_string(), DataType::Varchar(20), true)],
        );
        let mut bytes = Vec::new();
        rec.serialize(&mut bytes).unwrap();
        bytes[2] = 99; // the alteration sub-tag
        let err = RecKind::deserialize(&bytes).expect_err("an unknown alteration tag was accepted");
        assert!(format!("{err}").contains("column alteration tag"), "{err}");
    }
}
