//! E38 — one writer per database, enforced rather than documented.
//!
//! # The failure this exists to make impossible
//!
//! Two processes opening the same database file both build an [`ArenaPageStore`] from the same
//! checkpoint. Both therefore read the same `next_extent_start`, both claim extents from it, and
//! both hand out the same page ids to different branches. Neither notices: each write leaves a
//! structurally valid, correctly checksummed page behind, so nothing downstream can tell that two
//! writers are interleaving on it. Whichever process exits last writes its free-space map over the
//! other's, and the surviving map describes allocations that no longer match the file.
//!
//! Measured before it was fixed, on 2026-08-16: with `pgserver` listening on a database, the CLI
//! opened the same path, read from it and exited cleanly, checkpointing its arena over the server's
//! live view. Neither process printed anything.
//!
//! This became reachable when the CLI (E31) and the pgwire server (E36) were wired to the branch
//! engine. Before that neither binary built an arena, so a second opener could corrupt the heap but
//! not the branch region — a smaller hole, and one this lock also closes.
//!
//! # Why a lock *file* and not `flock`
//!
//! This database has no runtime dependencies, and `flock(2)` is not in `std`. Reaching for `libc`
//! to get advisory locking would trade the project's central constraint for a nicety. So the lock
//! is an `O_EXCL` create — [`std::fs::OpenOptions::create_new`], which is atomic at the filesystem
//! level and is exactly how Postgres's `postmaster.pid` works.
//!
//! # The stale-lock trade, stated plainly
//!
//! A process killed with `SIGKILL` leaves its lock file behind, and the next open refuses until
//! somebody removes it. That is the deliberate direction. `std` cannot ask whether a pid is still
//! alive without shelling out, and a liveness check that guesses wrong in the permissive direction
//! reintroduces exactly the corruption this prevents — while guessing wrong in the strict direction
//! costs one `rm`. The refusal therefore names the file and the pid that wrote it, so the operator
//! has what they need to decide, rather than being told to try again.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::FerroError;

/// An exclusive claim on one database, released when this value drops.
///
/// Hold it for as long as the database is open. Dropping it early re-opens the window it exists to
/// close, which is why nothing here hands out a way to release it without dropping the value.
#[derive(Debug)]
pub struct DbLock {
    path: PathBuf,
}

impl DbLock {
    /// Claim `db_path`, or fail because somebody else holds it.
    ///
    /// The lock file is `<db_path>.lock`. It carries the pid of the holder so a refusal can say who
    /// to look for, not merely that the door is shut.
    pub fn acquire(db_path: &Path) -> Result<DbLock, FerroError> {
        let path = lock_path(db_path);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut f) => {
                // Best-effort contents: the claim is the file's existence, which `create_new`
                // already established. A failure to write the pid must not be reported as a failure
                // to lock, because by then the lock IS held and returning an error here would drop
                // it on the floor and leave the file behind.
                let _ = writeln!(f, "{}", std::process::id());
                Ok(DbLock { path })
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let holder = std::fs::read_to_string(&path)
                    .ok()
                    .and_then(|s| s.trim().lines().next().map(|l| l.to_string()))
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "unknown".to_string());
                Err(FerroError::Io(format!(
                    "database {} is already open by process {} (lock file {}).\n\
                     Two processes writing one database hand out the same pages to different \
                     branches, and every such page still passes its checksum, so the damage is not \
                     detectable afterwards.\n\
                     If no such process is running, this lock is stale - delete {} and open again.",
                    db_path.display(),
                    holder,
                    path.display(),
                    path.display()
                )))
            }
            Err(e) => Err(FerroError::Io(format!(
                "could not create the lock file {}: {e}",
                path.display()
            ))),
        }
    }

    /// Where this lock lives, for a caller that wants to name it in its own diagnostics.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DbLock {
    fn drop(&mut self) {
        // A failure to remove leaves a stale lock, which refuses the next open. That is noisy and
        // recoverable; ignoring the error and carrying on is correct here because there is nothing
        // useful a panic in `drop` could do about it.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// The lock file that belongs to `db_path`.
pub fn lock_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".lock");
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db(tag: &str) -> (tempfile::TempDir, PathBuf) {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join(format!("{tag}.db"));
        (d, p)
    }

    #[test]
    fn a_second_claim_on_one_database_is_refused() {
        let (_d, p) = db("dup");
        let first = DbLock::acquire(&p).expect("the first claim was refused");
        let second = DbLock::acquire(&p);
        let err = second.expect_err("a second process claimed a database that was already open");
        let msg = format!("{err}");
        assert!(msg.contains("already open"), "the refusal does not say why: {msg}");
        assert!(
            msg.contains(&std::process::id().to_string()),
            "the refusal does not name the holding pid, so an operator cannot check whether it is \
             stale: {msg}"
        );
        drop(first);
    }

    /// The lock must not outlive the process that took it, or every clean shutdown would leave a
    /// database that refuses to open.
    #[test]
    fn dropping_the_lock_lets_the_next_open_through() {
        let (_d, p) = db("release");
        {
            let _held = DbLock::acquire(&p).expect("first");
            assert!(lock_path(&p).exists(), "no lock file was created, so nothing was claimed");
        }
        assert!(!lock_path(&p).exists(), "the lock file outlived the lock");
        DbLock::acquire(&p).expect("a released database was still refused");
    }

    /// Two different databases in one directory must not contend. Without this a single global lock
    /// would pass every test above and make the whole thing useless.
    #[test]
    fn two_different_databases_do_not_contend() {
        let d = tempfile::tempdir().unwrap();
        let a = DbLock::acquire(&d.path().join("a.db")).expect("a");
        let b = DbLock::acquire(&d.path().join("b.db")).expect("b was refused because of a");
        drop((a, b));
    }

    /// A stale lock is refused, and the message says which file to remove. This is the path an
    /// operator meets after a crash, so its text is the feature.
    #[test]
    fn a_stale_lock_is_refused_and_names_the_file_to_remove() {
        let (_d, p) = db("stale");
        std::fs::write(lock_path(&p), "424242\n").unwrap();
        let err = DbLock::acquire(&p).expect_err("a stale lock was silently taken over");
        let msg = format!("{err}");
        assert!(msg.contains("424242"), "the recorded pid is not reported: {msg}");
        assert!(
            msg.contains(&lock_path(&p).display().to_string()),
            "the message does not name the file to delete: {msg}"
        );
        assert!(msg.contains("stale"), "the message does not mention the stale case: {msg}");
    }

    /// An unreadable or empty lock file must still refuse. A guard that cannot parse its own input
    /// and falls through to allow is not a guard.
    #[test]
    fn an_empty_lock_file_still_refuses() {
        let (_d, p) = db("empty");
        std::fs::write(lock_path(&p), "").unwrap();
        let err = DbLock::acquire(&p).expect_err("an empty lock file was treated as no lock");
        assert!(format!("{err}").contains("unknown"), "the holder should read as unknown: {err}");
    }
}
