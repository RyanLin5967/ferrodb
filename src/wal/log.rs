use std::{fs::{File, OpenOptions}, path::PathBuf, sync::{Mutex, OnceLock, atomic::{AtomicU64, Ordering}}};

use crate::{error::FerroError, storage::disk_manager::{pread, pwrite}};

const HEADER_SIZE: usize = 16;
const MAGIC: u32 = 0xF3_EE_DB_01;
const VERSION: u32 = 1;
const INITIAL_LSN: u64 = 1;

pub struct WalManager {
    pub file: Mutex<File>,
    pub buffer: Mutex<WalBuffer>,
    pub next_lsn: AtomicU64,
    pub flushed_lsn: AtomicU64,
    pub path: PathBuf,
    pub base_lsn: u64,
}

pub struct WalBuffer {
    pub bytes: Vec<u8>,
    pub start_lsn: u64,
}

pub enum RecKind {
    Begin, Commit, Abort, TxnEnd, 
    HeapInsert { dir_root: u32, page_id: u32, slot: u16, tuple: Vec<u8> },
    HeapDelete { dir_root: u32, page_id: u32, slot: u16, old: Vec<u8> }, 
    HeapUpdate { dir_root: u32, page_id: u32, slot: u16, old: Vec<u8>, new: Vec<u8> },
    Clr { undone_lsn: u64, undo_next: u64, redo: Box<RecKind> },
    Checkpoint
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
            RecKind::Clr { undone_lsn, undo_next, redo } => {
                buffer.push(8);
                buffer.extend_from_slice(&undone_lsn.to_be_bytes());
                buffer.extend_from_slice(&undo_next.to_be_bytes());
                redo.serialize(buffer);
            }
        }
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, FerroError> {
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

        let base_lsn = if len == 0 {
            let mut header = [0u8; HEADER_SIZE];
            header[0..4].copy_from_slice(&MAGIC.to_be_bytes());
            header[4..8].copy_from_slice(&VERSION.to_be_bytes());
            header[8..16].copy_from_slice(&INITIAL_LSN.to_be_bytes());
            pwrite_all(&file, &header, 0)?;
            file.sync_all().map_err(|e| FerroError::Wal(e.to_string()))?;
            INITIAL_LSN
        } else {
            let mut header = [0u8; HEADER_SIZE];
            pread_all(&file, &mut header, 0)?;
            if u32::from_be_bytes(header[0..4].try_into().unwrap()) != MAGIC {
                return Err(FerroError::Wal("incorrect magic".into()));
            }
            if u32::from_be_bytes(header[4..8].try_into().unwrap()) != VERSION {
                return Err(FerroError::Wal("incorrect wal version".into()));
            }
            u64::from_be_bytes(header[8..16].try_into().unwrap())
        };
        let next_lsn = base_lsn + len.saturating_sub(HEADER_SIZE as u64);
        Ok(Self {file: Mutex::new(file), buffer: Mutex::new(WalBuffer { bytes: Vec::new(), start_lsn: next_lsn }), next_lsn: AtomicU64::new(next_lsn), flushed_lsn: AtomicU64::new(next_lsn), base_lsn, path})
    }

    pub fn read_record(&self, lsn: u64) -> Result<RecKind, FerroError> {

        todo!()
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
        buffer.bytes.extend_from_slice(&total_len.to_be_bytes());
        buffer.bytes.extend_from_slice(&body);
        let crc = crc32(&buffer.bytes[buffer.bytes.len()..]);
        buffer.bytes.extend_from_slice(&crc.to_be_bytes());
        self.next_lsn.fetch_add(total_len as u64, Ordering::SeqCst);
        Ok(lsn)
    }

    pub fn flush(&self) -> Result<(), FerroError> {

        Ok(())
    }

    pub fn flush_up_to(&self, lsn: u64) -> Result<(), FerroError> {

        Ok(())
    }
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

}