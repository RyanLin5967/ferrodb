//! The durable-IO seam.
//!
//! Every byte this database makes durable leaves through one of six operations. Before this trait
//! existed there was no way to make any of them misbehave: [`crate::storage::disk_manager::DiskManager`]
//! owned a concrete `File` and called the free `pwrite`/`pread` helpers directly, and
//! [`crate::wal::log::WalManager`] did the same. So the recovery code — the whole point of a write-ahead
//! log — could only ever be tested against faults a test could stage by *hand-editing the file after
//! the fact* (see `torn_tail_trimmed_on_open`). A fault that happens *during* the write, at an
//! arbitrary operation, could not be expressed at all.
//!
//! The trait is deliberately tiny: exactly the operations the storage layer already performs, at the
//! same granularity, with the same signatures. It adds no policy. `impl Storage for File` is the
//! production implementation and it forwards to the same free functions the code called before, so the
//! production path is the same syscalls in the same order.
//!
//! `pwrite`/`pread` keep their short-count return, because a real `write_at` is allowed to be short
//! and both callers already loop. A simulated *torn* write is therefore NOT modelled as a short count
//! — a short count is a retry, not a loss. See [`crate::storage::sim`].

use std::fs::File;
use std::io;

/// A file's worth of durable storage: positional reads and writes, two flavours of flush, truncate,
/// and its length.
///
/// `&self` throughout, matching `File`: positional IO needs no seek pointer, so callers share one
/// handle across threads without a lock of their own.
pub trait Storage: Send + Sync {
    /// Write at an absolute offset. May write fewer bytes than asked, exactly like `write_at`.
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize>;

    /// Read at an absolute offset. `Ok(0)` means end of file.
    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    /// Flush data and metadata.
    fn sync_all(&self) -> io::Result<()>;

    /// Flush data only. Distinct from [`Storage::sync_all`] because the WAL uses both and the
    /// difference is load-bearing there: `truncate` syncs the new header's *data* before shortening
    /// the file, then syncs *all* so the new length is durable too.
    fn sync_data(&self) -> io::Result<()>;

    /// Truncate or extend to `len`.
    fn set_len(&self, len: u64) -> io::Result<()>;

    /// Current length in bytes.
    fn len(&self) -> io::Result<u64>;
}

impl Storage for File {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        crate::storage::disk_manager::pwrite(self, buf, offset)
    }

    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        crate::storage::disk_manager::pread(self, buf, offset)
    }

    fn sync_all(&self) -> io::Result<()> {
        File::sync_all(self)
    }

    fn sync_data(&self) -> io::Result<()> {
        File::sync_data(self)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        File::set_len(self, len)
    }

    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|m| m.len())
    }
}
