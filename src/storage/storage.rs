//! The durable-IO seam.
//!
//! Every byte the **page store and the write-ahead log** make durable leaves through one of six
//! operations. Before this trait existed there was no way to make any of them misbehave: [`crate::storage::disk_manager::DiskManager`]
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
//!
//! # What is NOT behind this seam, stated because the first sentence used to overclaim it
//!
//! It said "every byte this database makes durable", and that was false. `run_cli` durably writes five
//! files and only two of them are here. A fault cannot reach:
//!
//! * `<db>.arena` — [`crate::branch::arena::ArenaPageStore::checkpoint`] uses `std::fs::write` plus
//!   `std::fs::rename`, and fsyncs neither the temporary file nor the directory.
//! * `<db>.branches` — `branch::catalog::LogBranchCatalog` appends with `write_all` + `sync_data` and
//!   replays with `read_to_end`. Its `replay` has an explicit torn-tail branch, which is exactly the
//!   fault class [`crate::storage::sim`] exists to inject and cannot reach there.
//! * the base backup image and its label — `replication::backup` calls the free `pwrite` on a concrete
//!   `File`; that module was out of bounds for the change that introduced this trait.
//! * `<db>.lock` — `storage::db_lock`, which is process coordination rather than recoverable state.
//!
//! Converting the first two is the next increment, and it is what would let a crash be aimed at the
//! branch arena or the branch catalog at all.

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
