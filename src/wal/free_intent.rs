//! **D229 (a): the durable record of pages a DROP has given up and not yet freed.**
//!
//! A DROP frees its table's pages only after its checkpoint has flushed and synced the unlink,
//! whatever that checkpoint then does with the log (a WAL pin can keep it; D250's recovery skips the
//! dropped table's records). Between the unlink and the frees, and across a crash anywhere in
//! between, this file is what says which pages those are. It sits beside the log,
//! `<wal>.drop-intent`, and holds every pending intent at once, rewritten whole.
//!
//! The rules it serves live in `wal::txn` (`TxnManager::adopt_free_intents` and what follows):
//! - an intent is written durably BEFORE the DROP's `DropTable` record, and so before the unlink,
//!   so every crash state in which the next open completes the DROP from the log has its intent;
//! - at open, its pages are quarantined BEFORE recovery can allocate (the review's A1);
//! - a table the durable catalog still names means the DROP never took effect, and the intent is
//!   dropped; a table that is gone is rolled forward after the open's checkpoint has synced. A
//!   record of the dropped table still in a kept log is D250's to skip at redo, not this file's to
//!   wait for (the review's A3, re-decided by the lead);
//! - the frees are synced before the intent goes, and the intent's removal is synced before its
//!   quarantine is released (A4).
//!
//! **Page ids, not roots.** Index pages are not logged, so after a crash a tree cannot be trusted to
//! be walked; the process that trusted it named every page while it still could.
//!
//! Format, big-endian: `D229` magic, version `u32` = 1, count `u32`, then per intent the dropped
//! table's first directory page `u32`, its name (`u16` length, bytes), and its pages as runs
//! (`u32` count, then `u32` start and `u32` length per run); a CRC32 of everything before it last.
//! A file that fails any check is refused, never guessed at: freeing the wrong pages is an alias.

use std::path::{Path, PathBuf};

use crate::error::FerroError;
use crate::storage::atomic_file::{parent_dir, replace_atomically, FileOps};
use crate::wal::log::crc32;

const MAGIC: &[u8; 4] = b"D229";
const VERSION: u32 = 1;

/// The pages of one dropped table, and the identity the open decides by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreeIntent {
    /// The table's name, for the messages a person reads.
    pub table: String,
    /// The table's first heap directory page: its identity in the durable catalog. A page of the
    /// dropped table, so no table created while the intent is pending can take it (the quarantine).
    pub dir_root: u32,
    /// Every page the table held, ascending.
    pub pages: Vec<u32>,
}

/// Where the intents live: beside the log, `<wal path>.drop-intent`.
pub fn intent_path(wal_path: &Path) -> PathBuf {
    let mut path = wal_path.as_os_str().to_os_string();
    path.push(".drop-intent");
    PathBuf::from(path)
}

fn runs(pages: &[u32]) -> Vec<(u32, u32)> {
    let mut sorted = pages.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut out: Vec<(u32, u32)> = Vec::new();
    for page in sorted {
        match out.last_mut() {
            Some((start, len)) if start.checked_add(*len) == Some(page) => *len += 1,
            _ => out.push((page, 1)),
        }
    }
    out
}

pub fn encode(intents: &[FreeIntent]) -> Result<Vec<u8>, FerroError> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_be_bytes());
    out.extend_from_slice(&(intents.len() as u32).to_be_bytes());
    for intent in intents {
        let name = intent.table.as_bytes();
        let name_len = u16::try_from(name.len()).map_err(|_| FerroError::Unrepresentable {
            what: format!("the name of dropped table {:?}", intent.table),
            len: name.len(),
            limit: u16::MAX as usize,
        })?;
        out.extend_from_slice(&intent.dir_root.to_be_bytes());
        out.extend_from_slice(&name_len.to_be_bytes());
        out.extend_from_slice(name);
        let runs = runs(&intent.pages);
        out.extend_from_slice(&(runs.len() as u32).to_be_bytes());
        for (start, len) in runs {
            out.extend_from_slice(&start.to_be_bytes());
            out.extend_from_slice(&len.to_be_bytes());
        }
    }
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    Ok(out)
}

/// A bounds-checked reader over the file's bytes.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], String> {
        let end = self.at.checked_add(n).filter(|end| *end <= self.bytes.len()).ok_or("it ends early")?;
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
}

pub fn decode(bytes: &[u8]) -> Result<Vec<FreeIntent>, String> {
    if bytes.len() < MAGIC.len() + 12 {
        return Err(format!("it is {} bytes, shorter than an empty intent file", bytes.len()));
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if crc32(body).to_be_bytes() != crc {
        return Err("its checksum does not match".into());
    }
    let mut c = Cursor { bytes: body, at: 0 };
    if c.take(4)? != MAGIC {
        return Err("it does not start with the intent magic".into());
    }
    let version = c.u32()?;
    if version != VERSION {
        return Err(format!("it is version {version}; this build reads version {VERSION}"));
    }
    let count = c.u32()?;
    let mut out = Vec::new();
    for _ in 0..count {
        let dir_root = c.u32()?;
        let name_len = c.u16()? as usize;
        let table = String::from_utf8(c.take(name_len)?.to_vec()).map_err(|_| "a table name is not UTF-8".to_string())?;
        let run_count = c.u32()?;
        let mut pages = Vec::new();
        for _ in 0..run_count {
            let (start, len) = (c.u32()?, c.u32()?);
            let end = start.checked_add(len).ok_or("a page run overflows")?;
            pages.extend(start..end);
        }
        out.push(FreeIntent { table, dir_root, pages });
    }
    if c.at != body.len() {
        return Err(format!("{} bytes follow the last intent", body.len() - c.at));
    }
    Ok(out)
}

/// The pending intents, or none when the file does not exist. A file that does not decode is
/// refused with its path, and the open with it: guessing would free pages nobody named.
pub fn load(wal_path: &Path) -> Result<Vec<FreeIntent>, FerroError> {
    let path = intent_path(wal_path);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(FerroError::Io(format!("read {}: {e}", path.display()))),
    };
    decode(&bytes).map_err(|why| {
        FerroError::Io(format!(
            "{} does not read as a DROP's page intent ({why}). It names the pages of a dropped table \
             that were not yet freed, and without it they cannot be told apart from live ones. Moving it \
             aside lets the database open, and those pages then stay allocated for good",
            path.display()
        ))
    })
}

/// Make `intents` the durable content of the intent file, through `ops` (`OsFileOps` in production,
/// a faulting double in tests): replaced atomically, or, when there are none, removed with its
/// directory synced (A4), so a power loss cannot bring back an intent whose pages have since been
/// handed out again.
///
/// **An intent file already gone still gets its directory synced** (D229 review 1's R1). A remove
/// that succeeded and whose directory sync then failed left the removal undurable; the retry finds
/// the file gone, and only this sync makes that earlier removal survive a power cut. Returning
/// before it let the caller release the quarantine over an intent a power loss could bring back.
pub fn store(ops: &dyn FileOps, wal_path: &Path, intents: &[FreeIntent]) -> Result<(), FerroError> {
    let path = intent_path(wal_path);
    if intents.is_empty() {
        match ops.remove(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(FerroError::Io(format!("remove {}: {e}", path.display()))),
        }
        return ops
            .sync_dir(parent_dir(&path))
            .map_err(|e| FerroError::Io(format!("sync the directory of {}: {e}", path.display())));
    }
    replace_atomically(ops, &path, &encode(intents)?)
        .map_err(|e| FerroError::Io(format!("write {}: {e}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::atomic_file::OsFileOps;

    fn intent(table: &str, dir_root: u32, pages: &[u32]) -> FreeIntent {
        FreeIntent { table: table.to_string(), dir_root, pages: pages.to_vec() }
    }

    /// Every page comes back, in runs and out of them, and the order within an intent is ascending.
    #[test]
    fn intents_round_trip_through_their_encoding() {
        let intents = vec![intent("t", 7, &[9, 7, 8, 12, 40, 41, 3]), intent("u", 50, &[50]), intent("empty", 60, &[])];
        let back = decode(&encode(&intents).unwrap()).unwrap();
        let mut want = intents.clone();
        for i in want.iter_mut() {
            i.pages.sort_unstable();
        }
        assert_eq!(back, want);
        assert_eq!(runs(&[3, 7, 8, 9, 12, 40, 41]), vec![(3, 1), (7, 3), (12, 1), (40, 2)], "runs are not maximal");
    }

    /// A flipped byte anywhere, a cut tail, or a trailing byte is refused, never decoded into a
    /// different list of pages.
    #[test]
    fn a_damaged_intent_file_is_refused() {
        let good = encode(&[intent("t", 7, &[7, 8, 9, 100])]).unwrap();
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0x40;
            assert!(decode(&bad).is_err(), "a flip at byte {i} decoded");
        }
        assert!(decode(&good[..good.len() - 1]).is_err(), "a cut tail decoded");
        let mut long = good.clone();
        long.push(0);
        assert!(decode(&long).is_err(), "a trailing byte decoded");
    }

    /// Storing none removes the file, and a missing file loads as none.
    #[test]
    fn storing_no_intents_removes_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join("x.wal");
        store(&OsFileOps, &wal, &[intent("t", 7, &[7])]).unwrap();
        assert!(intent_path(&wal).exists(), "the intent was not written");
        assert_eq!(load(&wal).unwrap(), vec![intent("t", 7, &[7])]);
        store(&OsFileOps, &wal, &[]).unwrap();
        assert!(!intent_path(&wal).exists(), "no intents left, and the file is still there");
        assert_eq!(load(&wal).unwrap(), Vec::new());
    }
}
