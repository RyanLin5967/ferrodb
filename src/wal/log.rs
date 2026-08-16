use std::{fs::{File, OpenOptions}, mem::take, path::PathBuf, sync::{Mutex, OnceLock, atomic::{AtomicU64, Ordering}}};

use crate::{catalog::column::DataType, error::FerroError, storage::disk_manager::{pread, pwrite}};

const HEADER_SIZE: usize = 24;
const MAGIC: u32 = 0xF3_EE_DB_01;
const VERSION: u32 = 2;
const INITIAL_LSN: u64 = 1;
const MIN_FRAME: usize = 33;

// need next_txn_id for mvcc, and then multi txn statements before mvcc
pub struct WalManager {
    pub file: Mutex<File>,
    pub buffer: Mutex<WalBuffer>,
    pub next_lsn: AtomicU64,
    pub flushed_lsn: AtomicU64,
    pub path: PathBuf,
    pub base_lsn: AtomicU64,
    pub header_txn_id: u64,
    /// LSNs some reader still needs, so a checkpoint may not discard them. See [`WalManager::pin`].
    pins: Mutex<std::collections::BTreeMap<u64, u64>>,
    next_pin_id: AtomicU64,
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
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DdlOp {
    CreateTable,
    DropTable,
}

#[derive(Debug, PartialEq)]
pub enum RecKind {
    Begin, Commit, Abort, TxnEnd, 
    HeapInsert { dir_root: u32, page_id: u32, slot: u16, tuple: Vec<u8> },
    HeapDelete { dir_root: u32, page_id: u32, slot: u16, old: Vec<u8> }, 
    HeapUpdate { dir_root: u32, page_id: u32, slot: u16, old: Vec<u8>, new: Vec<u8> },
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
}

pub struct LogRecord {
    pub lsn: u64,
    pub prev_lsn: u64,
    pub txn_id: u64,
    pub kind: RecKind
}

/// Length-prefixed string, so a name containing anything at all cannot desync the reader.
fn write_str(buffer: &mut Vec<u8>, s: &str) {
    buffer.extend_from_slice(&(s.len() as u16).to_be_bytes());
    buffer.extend_from_slice(s.as_bytes());
}

// Bounds-checked readers. These bytes arrive from a disk or a socket, so every read has to be able
// to refuse: indexing past the end of a truncated record panics the whole process, which is a
// denial of service triggered by a corrupt log rather than a parse error.
fn take_u8(bytes: &[u8], at: &mut usize) -> Result<u8, FerroError> {
    let v = *bytes.get(*at).ok_or_else(|| short(*at, 1, bytes.len()))?;
    *at += 1;
    Ok(v)
}

fn take_u16(bytes: &[u8], at: &mut usize) -> Result<u16, FerroError> {
    let end = *at + 2;
    let slice = bytes.get(*at..end).ok_or_else(|| short(*at, 2, bytes.len()))?;
    *at = end;
    Ok(u16::from_be_bytes(slice.try_into().unwrap()))
}

fn take_u32(bytes: &[u8], at: &mut usize) -> Result<u32, FerroError> {
    let end = *at + 4;
    let slice = bytes.get(*at..end).ok_or_else(|| short(*at, 4, bytes.len()))?;
    *at = end;
    Ok(u32::from_be_bytes(slice.try_into().unwrap()))
}

fn take_str(bytes: &[u8], at: &mut usize) -> Result<String, FerroError> {
    let len = take_u16(bytes, at)? as usize;
    let end = *at + len;
    let slice = bytes.get(*at..end).ok_or_else(|| short(*at, len, bytes.len()))?;
    *at = end;
    String::from_utf8(slice.to_vec())
        .map_err(|e| FerroError::Wal(format!("ddl record holds a non-utf8 name: {e}")))
}

fn short(at: usize, want: usize, have: usize) -> FerroError {
    FerroError::Wal(format!(
        "log record is truncated: wanted {want} byte(s) at offset {at} but the record is {have} bytes"
    ))
}

impl RecKind {
    pub fn serialize(&self, buffer: &mut Vec<u8>) {
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
            RecKind::Ddl { op, table, dir_root, time_travel_root, columns } => {
                buffer.push(9);
                buffer.push(match op { DdlOp::CreateTable => 0, DdlOp::DropTable => 1 });
                buffer.extend_from_slice(&dir_root.to_be_bytes());
                buffer.extend_from_slice(&time_travel_root.to_be_bytes());
                write_str(buffer, table);
                buffer.extend_from_slice(&(columns.len() as u16).to_be_bytes());
                for (name, ty, nullable) in columns {
                    write_str(buffer, name);
                    // Type tag, then any payload the type carries. Varchar's length is part of the
                    // type, so a consumer that recreates the column gets the same width.
                    match ty {
                        DataType::Integer => buffer.push(0),
                        DataType::Float => buffer.push(1),
                        DataType::Boolean => buffer.push(2),
                        DataType::Varchar(n) => {
                            buffer.push(3);
                            buffer.extend_from_slice(&n.to_be_bytes());
                        }
                    }
                    buffer.push(if *nullable { 1 } else { 0 });
                }
            }
            RecKind::Clr { undone_lsn, undo_next, redo } => {
                buffer.push(8);
                buffer.extend_from_slice(&undone_lsn.to_be_bytes());
                buffer.extend_from_slice(&undo_next.to_be_bytes());
                redo.serialize(buffer);
            }
        }
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
                    other => return Err(FerroError::Wal(format!("unknown ddl op {other}"))),
                };
                let dir_root = take_u32(bytes, &mut at)?;
                let time_travel_root = take_u32(bytes, &mut at)?;
                let table = take_str(bytes, &mut at)?;
                let count = take_u16(bytes, &mut at)? as usize;
                let mut columns = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    let name = take_str(bytes, &mut at)?;
                    let ty = match take_u8(bytes, &mut at)? {
                        0 => DataType::Integer,
                        1 => DataType::Float,
                        2 => DataType::Boolean,
                        3 => DataType::Varchar(take_u16(bytes, &mut at)?),
                        other => {
                            return Err(FerroError::Wal(format!("unknown column type tag {other}")))
                        }
                    };
                    let nullable = take_u8(bytes, &mut at)? != 0;
                    columns.push((name, ty, nullable));
                }
                Ok(RecKind::Ddl { op, table, dir_root, time_travel_root, columns })
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
    pub fn new(path: PathBuf) -> Result<Self, FerroError> {
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path).map_err(|e| FerroError::Wal(e.to_string()))?;
        let len = file.metadata().map_err(|e| FerroError::Wal(e.to_string()))?.len();

        let (base_lsn, header_txn_id) = if len == 0 {
            let mut header = [0u8; HEADER_SIZE];
            header[0..4].copy_from_slice(&MAGIC.to_be_bytes());
            header[4..8].copy_from_slice(&VERSION.to_be_bytes());
            header[8..16].copy_from_slice(&INITIAL_LSN.to_be_bytes());
            header[16..24].copy_from_slice(&1u64.to_be_bytes());
            pwrite_all(&file, &header, 0)?;
            file.sync_all().map_err(|e| FerroError::Wal(e.to_string()))?;
            (INITIAL_LSN, 1u64)
        } else {
            let mut header = [0u8; HEADER_SIZE];
            pread_all(&file, &mut header, 0)?;
            if u32::from_be_bytes(header[0..4].try_into().unwrap()) != MAGIC {
                return Err(FerroError::Wal("incorrect magic".into()));
            }
            if u32::from_be_bytes(header[4..8].try_into().unwrap()) != VERSION {
                return Err(FerroError::Wal("incorrect wal version".into()));
            }
            let base = u64::from_be_bytes(header[8..16].try_into().unwrap());
            let txn_hwn = u64::from_be_bytes(header[16..24].try_into().unwrap());
            (base, txn_hwn)
        };
        let valid_end = scan_valid_end(&file, base_lsn, len)?;
        let file_end = HEADER_SIZE as u64 + (valid_end - base_lsn);
        if file_end < len {
            file.set_len(file_end).map_err(|e| FerroError::Wal(e.to_string()))?;
            file.sync_all().map_err(|e| FerroError::Wal(e.to_string()))?;
        }
        Ok(Self {file: Mutex::new(file), buffer: Mutex::new(WalBuffer { bytes: Vec::new(), start_lsn: valid_end }), next_lsn: AtomicU64::new(valid_end), flushed_lsn: AtomicU64::new(valid_end), base_lsn: AtomicU64::new(base_lsn), path, header_txn_id, pins: Mutex::new(std::collections::BTreeMap::new()), next_pin_id: AtomicU64::new(1)})
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
    /// Lock order is pins -> buffer -> file, matching `truncate`.
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
            pread_all(&file, &mut len_buf, offset)?;
            let total = u32::from_be_bytes(len_buf) as usize;
            if total < MIN_FRAME {
                return Err(FerroError::Wal("incorrect record length".into()));
            }
            let mut buf = vec![0u8; total];
            pread_all(&file, &mut buf, offset)?;
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
        Ok((LogRecord {lsn: rec_lsn, prev_lsn, txn_id, kind}, lsn + total as u64))
    }

    // |total_len: u32|lsn: u64|prev_lsn: u64|txn_id: u64|tag: u8|payload: ...|crc32: u32|
    pub fn append(&self, txn_id: u64, prev_lsn: u64, kind: &RecKind) -> Result<u64, FerroError> {
        let mut buffer = self.buffer.lock().unwrap();
        let lsn = self.next_lsn.load(Ordering::SeqCst);
        let mut body = Vec::new();
        body.extend_from_slice(&lsn.to_be_bytes());
        body.extend_from_slice(&prev_lsn.to_be_bytes());
        body.extend_from_slice(&txn_id.to_be_bytes());
        kind.serialize(&mut body);
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
        pread_all(&file, &mut buf, HEADER_SIZE as u64 + rel)?;
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
        pwrite_all(&file, &mut header, 0)?;
        file.sync_data().map_err(|e| FerroError::Wal(e.to_string()))?;
        file.set_len(HEADER_SIZE as u64).map_err(|e| FerroError::Wal(e.to_string()))?;
        file.sync_all().map_err(|e| FerroError::Wal(e.to_string()))?;
        
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
            pwrite_all(&file, &bytes, offset)
                .and_then(|()| file.sync_data().map_err(|e| FerroError::Wal(e.to_string())))
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

pub fn scan_valid_end(file: &File, base_lsn: u64, file_len: u64) -> Result<u64, FerroError>{
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

pub fn pwrite_all(file: &File, mut buf: &[u8], mut offset: u64) -> Result<(), FerroError> {
    while !buf.is_empty() {
        match pwrite(file, buf, offset) {
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

pub fn pread_all(file: &File, buf: &mut [u8], mut offset: u64) -> Result<(), FerroError>{
    let mut total_read = 0;
    while total_read < buf.len() {
        match pread(file, &mut buf[total_read..], offset) {
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
            RecKind::Clr { undone_lsn: 2, undo_next: 4, redo: Box::new(RecKind::HeapUpdate { dir_root: 1, page_id: 3, slot: 4, old: vec![4, 5], new: vec![1] }) }
        ];

        for case in cases {
            let mut buf = Vec::new();
            case.serialize(&mut buf);
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
}