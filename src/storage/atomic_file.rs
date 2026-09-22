//! Replacing a file's contents durably: the four-step write-temporary-then-rename.
//!
//! `std::fs::write` followed by `std::fs::rename` looks atomic and is not. The rename is atomic
//! with respect to a concurrent *reader* — nobody ever opens a half-written file — but neither call
//! makes anything durable. After a power cut the directory entry can be the new one while the bytes
//! it names never left the page cache, which is precisely the half-written image the idiom is
//! supposed to make impossible.
//!
//! Four steps, all load-bearing:
//!
//! 1. write the temporary;
//! 2. **fsync the temporary**, so its bytes are on the device before anything points at them;
//! 3. rename it over the target;
//! 4. **fsync the directory**, so the rename itself survives.
//!
//! Steps 2 and 4 are invisible when they are missing. Every test that reopens the file in the same
//! process passes without them, because the page cache answers the read; only a power cut can tell
//! the difference. That is why the steps are a recordable seam ([`FileOps`]) rather than four inline
//! calls at a call site: the *order of the four* is the entire guarantee, and this way a test can
//! assert it.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// The filesystem operations a durable replace is made of.
///
/// Split at exactly this granularity on purpose. A coarser seam — one `write_durably` — would let
/// step 2 or step 4 be deleted with no test able to notice, which is the state this trait was
/// introduced to end.
pub trait FileOps {
    /// Create or truncate `path` and write every byte. Makes nothing durable.
    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()>;

    /// Append every byte to the **end of an existing** `path`. Makes nothing durable.
    ///
    /// Deliberately refuses a path that does not exist rather than creating it. An append is only
    /// ever meaningful after the thing it extends — see [`append_durably`], whose whole contract is
    /// that the reader can find a complete image in front of the appended bytes. Creating the file
    /// here would produce a headless tail that no reader can interpret.
    fn append(&self, path: &Path, bytes: &[u8]) -> io::Result<()>;

    /// Flush `path`'s data and metadata to the device.
    fn sync_file(&self, path: &Path) -> io::Result<()>;

    /// Rename `from` onto `to`, replacing whatever `to` was.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Flush `dir`'s own entries, so a rename into it survives a power cut.
    ///
    /// **Stated blind spot:** on Windows [`OsFileOps`] cannot do this at all (see its
    /// implementation) and does nothing. Every other step runs on every platform.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
}

/// The real filesystem.
pub struct OsFileOps;

impl FileOps for OsFileOps {
    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        std::fs::write(path, bytes)
    }

    /// `append(true)` **without** `create(true)`: a missing target is `ENOENT`, which is the
    /// refusal the trait's doc comment promises rather than a silently headless file.
    fn append(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        use std::io::Write;
        OpenOptions::new().append(true).open(path)?.write_all(bytes)
    }

    /// Opened **for writing** rather than with `File::open`: Windows' `FlushFileBuffers` refuses a
    /// handle that has no write access, so the read-only spelling of this would compile everywhere
    /// and fail on one of the three platforms in CI.
    fn sync_file(&self, path: &Path) -> io::Result<()> {
        OpenOptions::new().write(true).open(path)?.sync_all()
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    /// Measured before being relied on: `File::open(dir)?.sync_all()` returns `Ok` on macOS/APFS
    /// (where `sync_all` is `F_FULLFSYNC`) and on Linux, which are two of the three CI platforms.
    #[cfg(not(windows))]
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        std::fs::File::open(dir)?.sync_all()
    }

    /// A directory cannot be opened as a `File` on Windows without `FILE_FLAG_BACKUP_SEMANTICS`,
    /// which needs a winapi dependency this crate does not have. So this is a no-op **there only**,
    /// and it is a real gap rather than a claim of parity: NTFS journals a rename as metadata, so
    /// the entry is far less exposed than on a POSIX filesystem, but "less exposed" is not "fsynced".
    /// Step 2 — the temporary's own fsync, which is the step that protects the *bytes* — runs on
    /// every platform including this one.
    #[cfg(windows)]
    fn sync_dir(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }
}

/// Replace `path`'s contents with `bytes` so that a crash leaves either the previous contents or
/// these, never a mixture and never an entry naming bytes that are not there.
///
/// A failure part-way through leaves the temporary behind. That is deliberate: removing it would
/// need a fifth operation whose own failure would then have to be handled, and the next successful
/// replace overwrites it anyway.
/// **D81 instrument: count the checkpoints and the bytes, separately.**
///
/// The D81 design turns on one question that a latency number cannot answer: is the free-space
/// map's cost the BYTES it rewrites or the FSYNCS it performs? Both grow together in wall-clock,
/// and only one of them a delta scheme would fix — appending 24 bytes instead of 24 KB still costs
/// one fsync. Two counters separate them; a profile cannot.
pub static ATOMIC_REPLACES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static ATOMIC_REPLACE_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// `(replacements, bytes)` since process start. Read twice and subtract to scope to a phase.
pub fn atomic_replace_counters() -> (u64, u64) {
    use std::sync::atomic::Ordering;
    (ATOMIC_REPLACES.load(Ordering::Relaxed), ATOMIC_REPLACE_BYTES.load(Ordering::Relaxed))
}

/// The same two counters for [`append_durably`], kept SEPARATE from the replace pair.
///
/// Separate because the point of D81 is the difference between the two: a replace is two fsyncs
/// over the whole image, an append is one fsync over one record. Summing them would hide exactly
/// the quantity the row is about.
pub static DURABLE_APPENDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static DURABLE_APPEND_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// `(appends, bytes)` since process start.
pub fn durable_append_counters() -> (u64, u64) {
    use std::sync::atomic::Ordering;
    (DURABLE_APPENDS.load(Ordering::Relaxed), DURABLE_APPEND_BYTES.load(Ordering::Relaxed))
}

/// Extend `path` with `bytes` and make the extension durable, without rewriting what is in front
/// of it.
///
/// **Two steps, not four, and the two that are missing are the point.** A replace has to fsync a
/// temporary *and* the directory, because it publishes a new inode under an existing name. An
/// append changes neither the name nor the inode: the directory entry is already durable, so there
/// is nothing for `sync_dir` to do. What is left is write-then-fsync — **one fsync where
/// [`replace_atomically`] pays two**, over one record instead of the whole file.
///
/// `sync_file` rather than a data-only flush: the file's LENGTH is metadata, and a tail whose bytes
/// are on the device under a length that is not is a tail no reader will look at.
///
/// **Under the same [`REPLACE_LOCK`] as a replace, and that is load-bearing.** An append that
/// interleaved with a replace of the same target would land on the inode being replaced and vanish
/// with it — a durably-acknowledged claim that is not in the file afterwards, which is the one
/// outcome worse than a slow checkpoint.
///
/// ⚠ **What this does NOT give you, and the caller must:** an append is atomic against other
/// writers here, never against a power cut. A torn append leaves a partial record at the end of
/// the file, so whatever the caller appends must be self-delimiting and self-checked, and its
/// reader must be able to discard an incomplete final record. `ArenaPageStore`'s tail records
/// carry a length and a CRC32 for exactly this reason.
pub fn append_durably(ops: &dyn FileOps, path: &Path, bytes: &[u8]) -> io::Result<()> {
    {
        use std::sync::atomic::Ordering;
        DURABLE_APPENDS.fetch_add(1, Ordering::Relaxed);
        DURABLE_APPEND_BYTES.fetch_add(bytes.len() as u64, Ordering::Relaxed);
    }
    let _serialised = REPLACE_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    ops.append(path, bytes)?;
    ops.sync_file(path)
}

pub fn replace_atomically(ops: &dyn FileOps, path: &Path, bytes: &[u8]) -> io::Result<()> {
    {
        use std::sync::atomic::Ordering;
        ATOMIC_REPLACES.fetch_add(1, Ordering::Relaxed);
        ATOMIC_REPLACE_BYTES.fetch_add(bytes.len() as u64, Ordering::Relaxed);
    }
    let _serialised = REPLACE_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let tmp = temp_path(path)?;
    ops.write(&tmp, bytes)?;
    ops.sync_file(&tmp)?;
    ops.rename(&tmp, path)?;
    ops.sync_dir(parent_dir(path))
}

/// Serialises durable replaces process-wide.
///
/// The temporary's name is derived from the target, so two threads replacing one file interleave:
/// T1 writes the temporary, T2 truncates it, T1 fsyncs and renames a half-written image into place.
/// That is reachable rather than theoretical — `ArenaPageStore` is `Sync`, both of its checkpoint
/// triggers deliberately drop the state lock before persisting, and `reopen_from_checkpoint` arms
/// persisting on every open — and the outcome is a CRC-refusing `<db>.arena`, which is the exact
/// failure this module exists to prevent.
///
/// Process-wide rather than per-path: a replace happens once per extent claimed or freed, so
/// contention costs nothing worth measuring, and a per-path map would have to be pruned. Two
/// *processes* over one database are refused earlier, by `storage::db_lock`.
///
/// Poisoning is ignored deliberately. What this guards is a path on a filesystem, not an invariant
/// in memory: a panicking replace leaves at worst a stale temporary, whereas refusing every later
/// checkpoint because an earlier one panicked would turn a recoverable leak into a database that
/// cannot record where its arena starts.
static REPLACE_LOCK: Mutex<()> = Mutex::new(());

/// The temporary that `path` is staged through: its **whole file name** plus `.tmp`.
///
/// Appended rather than substituted. `Path::with_extension("tmp")` *replaces* the extension, so
/// `db.arena` and `db.branches` would both stage through `db.tmp` and two concurrent replaces in
/// one directory would write over each other's half-finished image.
pub fn temp_path(path: &Path) -> io::Result<PathBuf> {
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} names no file to replace", path.display()),
        )
    })?;
    let mut tmp = name.to_os_string();
    tmp.push(".tmp");
    Ok(path.with_file_name(tmp))
}

/// The directory whose entry the rename changes.
///
/// A bare relative target such as `db.arena` has an **empty** parent, and opening `""` fails with
/// `ENOENT`, so it resolves to the current directory instead. The CLI builds its arena path as
/// `format!("{db_path}.arena")`, so a bare name is what it passes for `ferrodb mydb.db`.
pub fn parent_dir(path: &Path) -> &Path {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    }
}

/// One recorded filesystem operation, for tests that need to see the *order* of the four.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Op {
    Write(PathBuf, Vec<u8>),
    Append(PathBuf, Vec<u8>),
    SyncFile(PathBuf),
    Rename(PathBuf, PathBuf),
    SyncDir(PathBuf),
}

/// A [`FileOps`] that records and touches nothing.
///
/// The point is that an fsync is unobservable from user space — no assertion over a real filesystem
/// can distinguish a durable replace from a `write` plus a `rename`. This can.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct RecordingOps {
    ops: std::sync::Mutex<Vec<Op>>,
}

#[cfg(test)]
impl RecordingOps {
    pub(crate) fn new() -> RecordingOps {
        RecordingOps::default()
    }

    pub(crate) fn ops(&self) -> Vec<Op> {
        self.ops.lock().unwrap().clone()
    }

    /// The operations as `(kind, first path)` pairs, which is the shape assertions care about.
    pub(crate) fn shape(&self) -> Vec<(&'static str, PathBuf)> {
        self.ops()
            .into_iter()
            .map(|op| match op {
                Op::Write(p, _) => ("write", p),
                Op::Append(p, _) => ("append", p),
                Op::SyncFile(p) => ("sync_file", p),
                Op::Rename(from, _) => ("rename", from),
                Op::SyncDir(p) => ("sync_dir", p),
            })
            .collect()
    }
}

#[cfg(test)]
impl FileOps for RecordingOps {
    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.ops.lock().unwrap().push(Op::Write(path.to_path_buf(), bytes.to_vec()));
        Ok(())
    }

    fn append(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.ops.lock().unwrap().push(Op::Append(path.to_path_buf(), bytes.to_vec()));
        Ok(())
    }

    fn sync_file(&self, path: &Path) -> io::Result<()> {
        self.ops.lock().unwrap().push(Op::SyncFile(path.to_path_buf()));
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.ops.lock().unwrap().push(Op::Rename(from.to_path_buf(), to.to_path_buf()));
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.ops.lock().unwrap().push(Op::SyncDir(dir.to_path_buf()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole guarantee, spelled out as an order: the bytes are on the device before anything
    /// names them, and the name is on the device after it does.
    #[test]
    fn a_durable_replace_syncs_the_temporary_before_the_rename_and_the_directory_after_it() {
        let ops = RecordingOps::new();
        let target = Path::new("/var/db/state.arena");
        replace_atomically(&ops, target, b"payload").unwrap();

        let tmp = PathBuf::from("/var/db/state.arena.tmp");
        assert_eq!(
            ops.shape(),
            vec![
                ("write", tmp.clone()),
                ("sync_file", tmp.clone()),
                ("rename", tmp.clone()),
                ("sync_dir", PathBuf::from("/var/db")),
            ],
            "a replace that skips either fsync is not durable, and only the order shows it"
        );
        assert_eq!(
            ops.ops()[0],
            Op::Write(tmp.clone(), b"payload".to_vec()),
            "the temporary must receive the payload, not the target"
        );
        assert_eq!(
            ops.ops()[2],
            Op::Rename(tmp, target.to_path_buf()),
            "the rename must move the temporary onto the target"
        );
    }

    #[test]
    fn the_temporary_appends_tmp_instead_of_replacing_the_extension() {
        // `with_extension("tmp")` maps both of these to `db.tmp`, and this database writes both
        // files into one directory.
        assert_eq!(temp_path(Path::new("db.arena")).unwrap(), PathBuf::from("db.arena.tmp"));
        assert_eq!(
            temp_path(Path::new("/x/db.branches")).unwrap(),
            PathBuf::from("/x/db.branches.tmp")
        );
        assert_ne!(
            temp_path(Path::new("/x/db.arena")).unwrap(),
            temp_path(Path::new("/x/db.branches")).unwrap()
        );
    }

    #[test]
    fn a_path_that_names_no_file_is_refused_rather_than_staged_through_dot_tmp() {
        assert!(temp_path(Path::new("/")).is_err());
        assert!(temp_path(Path::new("..")).is_err());
    }

    #[test]
    fn a_bare_relative_target_syncs_the_current_directory_not_the_empty_path() {
        // `Path::new("db.arena").parent()` is `Some("")`, and `File::open("")` is ENOENT — so
        // without this the CLI's own `format!("{db_path}.arena")` would fail every checkpoint.
        assert_eq!(parent_dir(Path::new("db.arena")), Path::new("."));
        assert_eq!(parent_dir(Path::new("/x/db.arena")), Path::new("/x"));
        assert_eq!(parent_dir(Path::new("x/db.arena")), Path::new("x"));
    }

    /// The recorder cannot prove the real implementation works, so this runs the real one against a
    /// real directory: every step must succeed on this platform, the target must hold the new bytes
    /// and the temporary must be gone.
    #[test]
    fn the_real_filesystem_replace_lands_the_bytes_and_leaves_no_temporary() {
        let dir = std::env::temp_dir().join(format!("ferro-atomic-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("state.arena");

        replace_atomically(&OsFileOps, &target, b"first").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"first");
        replace_atomically(&OsFileOps, &target, b"second").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"second", "a replace overwrites");
        assert!(
            !temp_path(&target).unwrap().exists(),
            "the temporary must not survive a successful replace"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **Two threads replacing one file must not interleave into a half-written image.**
    ///
    /// The temporary's name comes from the target, so without serialisation T2's `File::create`
    /// truncates the temporary T1 is about to fsync and rename — publishing a short or mixed image
    /// under a name that promises neither. `ArenaPageStore` is `Sync` and persists with its state
    /// lock dropped, so this is the arena's own shape, not a hypothetical.
    ///
    /// Measured with `REPLACE_LOCK` removed: this fails, reporting a published length of 0 rather
    /// than 262144.
    #[test]
    fn concurrent_replaces_of_one_target_never_publish_a_mixture() {
        const LEN: usize = 256 * 1024;
        let dir = std::env::temp_dir().join(format!("ferro-atomic-race-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("state.arena");
        replace_atomically(&OsFileOps, &target, &vec![b'z'; LEN]).unwrap();

        std::thread::scope(|s| {
            for tag in [b'a', b'b', b'c', b'd'] {
                let target = target.clone();
                s.spawn(move || {
                    let payload = vec![tag; LEN];
                    for _ in 0..25 {
                        replace_atomically(&OsFileOps, &target, &payload).unwrap();
                        let got = std::fs::read(&target).unwrap();
                        assert_eq!(
                            got.len(),
                            LEN,
                            "a replace published {} bytes: the temporary was truncated under it",
                            got.len()
                        );
                        assert!(
                            got.iter().all(|b| *b == got[0]),
                            "a replace published a mixture of two writers' images"
                        );
                    }
                });
            }
        });

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **The saving D81 is built on, stated as an order rather than argued about.** A replace is
    /// four operations and TWO fsyncs; an append is two operations and ONE, and neither of the
    /// missing ones is the directory's by accident — an append does not change the directory.
    #[test]
    fn a_durable_append_is_one_fsync_where_a_replace_is_two() {
        let target = Path::new("/var/db/state.arena");

        let replacing = RecordingOps::new();
        replace_atomically(&replacing, target, b"image").unwrap();
        let appending = RecordingOps::new();
        append_durably(&appending, target, b"rec").unwrap();

        assert_eq!(
            appending.shape(),
            vec![("append", target.to_path_buf()), ("sync_file", target.to_path_buf())],
            "an append writes the record and flushes it, and touches nothing else"
        );
        assert_eq!(
            appending.ops()[0],
            Op::Append(target.to_path_buf(), b"rec".to_vec()),
            "the record must go to the target itself, not through a temporary"
        );

        let fsyncs = |ops: &RecordingOps| {
            ops.shape().iter().filter(|(kind, _)| *kind == "sync_file" || *kind == "sync_dir").count()
        };
        assert_eq!(fsyncs(&replacing), 2, "a replace fsyncs the temporary and the directory");
        assert_eq!(fsyncs(&appending), 1, "an append fsyncs only the file it extended");
        assert!(
            !appending.shape().iter().any(|(kind, _)| *kind == "sync_dir" || *kind == "rename"),
            "an append changes no directory entry, so it must pay for neither"
        );
    }

    /// An append onto a path that does not exist must REFUSE, not create one.
    ///
    /// A created file would hold tail records with no image in front of them, and the reader's
    /// first act is to parse an image. Measured against the real filesystem, because this is a
    /// property of `OpenOptions`, not of anything this module decides.
    #[test]
    fn appending_to_a_missing_file_is_refused_rather_than_creating_a_headless_tail() {
        let dir = std::env::temp_dir().join(format!("ferro-append-missing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("state.arena");

        let err = append_durably(&OsFileOps, &target, b"rec").expect_err(
            "an append with nothing to append to must fail, not invent a file",
        );
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(!target.exists(), "the refusal must leave no file behind");

        // ...and it works the moment there is something in front of it.
        replace_atomically(&OsFileOps, &target, b"image").unwrap();
        append_durably(&OsFileOps, &target, b"rec").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"imagerec", "the append must EXTEND, not replace");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A replace after appends must leave the target holding the image ALONE.
    ///
    /// This is what makes compaction a compaction: `replace_atomically` renames a fresh inode over
    /// the target, so the old tail goes with the old inode. If it truncated in place instead, a
    /// shorter image would leave the previous tail's bytes stranded after it and the next reader
    /// would replay records that had already been folded in.
    #[test]
    fn a_replace_after_appends_drops_the_tail_instead_of_stranding_it() {
        let dir = std::env::temp_dir().join(format!("ferro-append-compact-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("state.arena");

        replace_atomically(&OsFileOps, &target, b"LONG-IMAGE-V1").unwrap();
        append_durably(&OsFileOps, &target, b"tail-a").unwrap();
        append_durably(&OsFileOps, &target, b"tail-b").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"LONG-IMAGE-V1tail-atail-b");

        replace_atomically(&OsFileOps, &target, b"V2").unwrap();
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"V2",
            "a shorter image must not leave the old tail visible after it"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `sync_dir` on a real directory is the step most likely to be unsupported somewhere, and a
    /// checkpoint that fails is worse than one that is not durable. Asserted rather than assumed.
    #[test]
    fn syncing_a_real_directory_succeeds_on_this_platform() {
        let dir = std::env::temp_dir().join(format!("ferro-dirsync-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        OsFileOps.sync_dir(&dir).expect("fsync of a directory must work wherever this runs");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
