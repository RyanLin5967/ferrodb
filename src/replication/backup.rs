//! E8 — base backup: the thing replication cannot start without.
//!
//! E4 shipped a replica that converged at 40 rows and blew up at 2000. The reason was not a bug in
//! the applier: the primary checkpoints every 256 commits and **truncates the WAL**, so a replica
//! starting with an empty file against a truncated log has missed every record before the new base
//! and is applying "insert at slot 75" to a page that has no slots. Physical replication carries
//! *changes*; it cannot carry a history that has been thrown away.
//!
//! A base backup is the missing half: a copy of the data pages plus **the LSN they correspond
//! to**. The replica restores that copy, starts its applier at that LSN, and streams forward.
//! `pg_basebackup` exists for exactly this reason.
//!
//! # Why this is allowed to copy pages while the primary is running
//!
//! The backup does not stop the world, and the file it produces is *not* an instant snapshot —
//! page 3 may be copied a millisecond after page 2 and reflect later work. It is still usable, and
//! the argument has two legs:
//!
//! 1. **Each page is copied atomically.** Pages are read through the buffer pool under the frame
//!    lock, never by reading the file underneath a concurrent writer. So no copied page is torn:
//!    its bytes and its page-LSN header agree with each other. This matters more than it sounds.
//!    Redo decides whether to skip a record by comparing it against the page's own LSN, so a torn
//!    page whose LSN reads new while its contents read old would cause redo to skip exactly the
//!    records needed to repair it. That is the failure full-page writes exist to prevent, and
//!    copying atomically avoids needing them.
//!
//! 2. **Redo is idempotent per page.** `apply_redo` skips any record at or below the page's LSN.
//!    So a page copied early is missing records and gets them replayed; a page copied late already
//!    has them and skips them. Replaying `[start_lsn, end_lsn]` therefore brings *every* page to a
//!    state consistent as of `end_lsn`, regardless of the order pages were copied in.
//!
//! Hence the label carries both ends. `start_lsn` is where the replica must begin, and `end_lsn`
//! is the point before which the restored file **must not be treated as consistent** — it is a
//! smear of states until the replica has replayed that far.
//!
//! # What this deliberately does not do
//!
//! - **The backup is transferred out of band.** `take` writes a directory; the replica reads one.
//!   Streaming it over the replication socket is a separate concern and is not implemented, so a
//!   backup taken on another machine has to be copied there by other means.
//! - **It holds the primary's WAL, and that has a cost.** A [`BackupHandle`] pins the log, so a
//!   checkpoint will not discard records the backup points into. This log cannot be truncated
//!   part-way — it is discarded whole — so honouring the pin means the WAL keeps growing until the
//!   handle drops. That is the same hazard PostgreSQL replication slots have, and it is the right
//!   trade: the alternative was measured, and it is a backup that is unusable the instant it
//!   finishes. A handle that is never dropped is a WAL that never shrinks.
//! - **A backup whose handle has been dropped can still go stale.** Once the pin is gone the next
//!   checkpoint reclaims the log, and a replica starting from that backup is refused.
//!   [`BackupLabel::assert_usable_with`] turns that refusal into a message naming the real cause
//!   rather than the generic "lsn N is below the log's base" from inside the source.
//! - **It copies the data file only**, which is the same boundary physical WAL replication already
//!   has. The catalog lives outside both.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::buffer::buffer_pool::BufferPoolManager;
use crate::error::FerroError;
use crate::storage::disk_manager::{pwrite, PAGE_SIZE};
use crate::wal::log::WalManager;

/// The file names inside a backup directory.
pub const BASE_IMAGE: &str = "base.db";
pub const BACKUP_LABEL: &str = "backup_label";

/// Where a restored copy sits in the primary's log.
///
/// Both ends are needed and they mean different things. Starting a replica anywhere other than
/// `start_lsn` is wrong in both directions: later skips records the copy is missing, earlier is
/// refused by the primary once it has truncated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupLabel {
    /// Replay must begin here. Taken *before* the first page is copied, so no change that could
    /// have landed in the copy is ahead of it.
    pub start_lsn: u64,
    /// The copy is consistent once replay has reached at least here. Taken *after* the last page,
    /// so it covers every change that could have raced the copy.
    pub end_lsn: u64,
    /// Pages written to the image, so a truncated transfer is caught rather than restored.
    pub page_count: u32,
}

impl BackupLabel {
    fn encode(&self) -> String {
        // Deliberately text: a backup that cannot be read without the tool that wrote it is a
        // worse backup, and this file is the first thing anyone looks at when a restore goes wrong.
        format!(
            "ferrodb backup_label v1\nstart_lsn {}\nend_lsn {}\npage_count {}\n",
            self.start_lsn, self.end_lsn, self.page_count
        )
    }

    fn decode(text: &str) -> Result<Self, FerroError> {
        let field = |name: &str| -> Result<u64, FerroError> {
            text.lines()
                .find_map(|l| l.strip_prefix(name).map(str::trim))
                .ok_or_else(|| {
                    FerroError::Io(format!("backup_label has no `{name}` field; it is not a ferrodb backup label"))
                })?
                .parse::<u64>()
                .map_err(|e| FerroError::Io(format!("backup_label field `{name}` is not a number: {e}")))
        };
        if !text.starts_with("ferrodb backup_label v1") {
            return Err(FerroError::Io(
                "backup_label is missing its version header; refusing to guess its format".into(),
            ));
        }
        let label = BackupLabel {
            start_lsn: field("start_lsn")?,
            end_lsn: field("end_lsn")?,
            page_count: field("page_count")? as u32,
        };
        if label.end_lsn < label.start_lsn {
            return Err(FerroError::Io(format!(
                "backup_label is impossible: end_lsn {} is before start_lsn {}",
                label.end_lsn, label.start_lsn
            )));
        }
        Ok(label)
    }

    /// Refuse a backup the primary can no longer serve, and say why.
    ///
    /// Without this the failure surfaces as `lsn N is below the log's base` from deep inside the
    /// source — a true statement that tells the operator nothing about the actual mistake, which
    /// is that the backup is older than the primary's log.
    pub fn assert_usable_with(&self, source_start_lsn: u64) -> Result<(), FerroError> {
        if self.start_lsn < source_start_lsn {
            return Err(FerroError::Wal(format!(
                "this base backup is stale: it starts at LSN {} but the primary has truncated its \
                 WAL up to LSN {}, so the records between them are gone. Take a new base backup. \
                 (A live `BackupHandle` pins the log and prevents this; a handle that has been \
                 dropped no longer does.)",
                self.start_lsn, source_start_lsn
            )));
        }
        Ok(())
    }
}

/// A completed backup, and the claim on the primary's log that keeps it usable.
///
/// The pin is held for as long as this value lives and released when it drops. That is deliberate
/// rather than an ownership detail: a backup whose log has been truncated away is indistinguishable
/// from a good one until a replica tries to use it and is refused, so the thing that keeps it valid
/// should not be something a caller has to remember to do.
///
/// Dropping it says "no replica will start from this backup any more", which is also what allows
/// the next checkpoint to reclaim the log.
pub struct BackupHandle {
    pub label: BackupLabel,
    _pin: crate::wal::log::WalPin,
}

impl std::ops::Deref for BackupHandle {
    type Target = BackupLabel;
    fn deref(&self) -> &BackupLabel {
        &self.label
    }
}

/// Copy the primary's data pages into `dir`, and record the LSN they correspond to.
///
/// Ordering is the whole of the correctness argument, so it is written out rather than left to be
/// re-derived: flush the WAL, read `start_lsn`, copy, flush again, read `end_lsn`. Reading
/// `start_lsn` *after* copying would let a change land in a copied page and sit below the replay
/// point, and it would never be replayed.
pub fn take(
    bp: &Arc<BufferPoolManager>,
    wal: &Arc<WalManager>,
    dir: &Path,
) -> Result<BackupHandle, FerroError> {
    std::fs::create_dir_all(dir).map_err(|e| FerroError::Io(format!("create backup dir: {e}")))?;

    // Everything already written must be durable before it can be a replay floor.
    wal.flush()?;

    // Pin BEFORE anything else reads the frontier. `pin_durable` reads it and registers the claim
    // under one lock, so a checkpoint cannot slip between the two and truncate the range this
    // backup is about to declare itself to start at. That failure is not hypothetical: the hot
    // test caught it as `lsn 183 is below the log's base`, with the backup unusable the moment it
    // finished.
    let pin = wal.pin_durable();
    let start_lsn = pin.lsn();

    // Not required for correctness — the copy reads the pool, not the file — but it keeps the
    // image close to the file the primary would recover from, which makes a restored copy easier
    // to compare by hand when something is wrong.
    bp.flush_all()?;

    let page_count = bp.disk_manager.high_water()?;
    if page_count == 0 {
        // Zero pages copied is not a successful backup of an empty database; it is a backup that
        // collected nothing, and restoring it would silently produce an empty replica.
        return Err(FerroError::Io(
            "base backup would contain 0 pages, so there is nothing to restore from; refusing to \
             write an empty backup that would look like a successful one"
                .into(),
        ));
    }

    let image_path = dir.join(BASE_IMAGE);
    let image = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&image_path)
        .map_err(|e| FerroError::Io(format!("create {}: {e}", image_path.display())))?;

    let mut copied = 0u32;
    for page_id in 0..page_count {
        // Read through the pool so the frame lock makes each page atomic. A page the pool cannot
        // produce is one the file does not yet hold; it is written as zeros so offsets stay aligned
        // and the applier can materialise it from the WAL later, exactly as recovery does.
        let buf = match bp.fetch_page(page_id) {
            Ok(frame_i) => {
                let data = bp.frames[frame_i].read().unwrap().data;
                bp.unpin_page(page_id, false);
                data
            }
            Err(_) => [0u8; PAGE_SIZE],
        };
        pwrite(&image, &buf, page_id as u64 * PAGE_SIZE as u64)
            .map_err(|e| FerroError::Io(format!("write backup page {page_id}: {e}")))?;
        copied += 1;
    }

    image.sync_all().map_err(|e| FerroError::Io(format!("fsync backup image: {e}")))?;

    wal.flush()?;
    let end_lsn = wal.flushed_lsn.load(std::sync::atomic::Ordering::SeqCst);

    let label = BackupLabel { start_lsn, end_lsn, page_count: copied };
    let label_path = dir.join(BACKUP_LABEL);
    let mut f = std::fs::File::create(&label_path)
        .map_err(|e| FerroError::Io(format!("create {}: {e}", label_path.display())))?;
    f.write_all(label.encode().as_bytes())
        .map_err(|e| FerroError::Io(format!("write backup label: {e}")))?;
    f.sync_all().map_err(|e| FerroError::Io(format!("fsync backup label: {e}")))?;

    Ok(BackupHandle { label, _pin: pin })
}

/// Restore a backup directory into `dest`, returning the label the replica must start from.
///
/// The page count in the label is checked against the image's actual size, so a transfer that was
/// cut short is refused here rather than discovered later as a replica that is quietly missing its
/// last pages.
pub fn restore(dir: &Path, dest: &Path) -> Result<BackupLabel, FerroError> {
    let label_path = dir.join(BACKUP_LABEL);
    let text = std::fs::read_to_string(&label_path)
        .map_err(|e| FerroError::Io(format!("read {}: {e}", label_path.display())))?;
    let label = BackupLabel::decode(&text)?;

    let image_path: PathBuf = dir.join(BASE_IMAGE);
    let meta = std::fs::metadata(&image_path)
        .map_err(|e| FerroError::Io(format!("stat {}: {e}", image_path.display())))?;
    let expected = label.page_count as u64 * PAGE_SIZE as u64;
    if meta.len() != expected {
        return Err(FerroError::Io(format!(
            "base backup is truncated or padded: the label says {} pages ({expected} bytes) but \
             {} is {} bytes. Restoring it would produce a replica silently missing pages.",
            label.page_count,
            image_path.display(),
            meta.len()
        )));
    }

    std::fs::copy(&image_path, dest)
        .map_err(|e| FerroError::Io(format!("copy backup image to {}: {e}", dest.display())))?;
    Ok(label)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk_manager::DiskManager;
    use crate::wal::log::RecKind;

    struct Primary {
        _dir: tempfile::TempDir,
        dir: PathBuf,
        bp: Arc<BufferPoolManager>,
        wal: Arc<WalManager>,
    }

    fn primary(tag: &str) -> Primary {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().to_path_buf();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.join(format!("{tag}.db")))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.join(format!("{tag}.wal"))).unwrap());
        bp.attach_wal(Arc::clone(&wal));
        Primary { _dir: d, dir, bp, wal }
    }

    /// Put some real pages under the pool so a backup has something to copy.
    fn seed(p: &Primary, pages: usize) {
        for i in 0..pages {
            let id = p.bp.new_page().unwrap();
            let frame_i = p.bp.fetch_page(id).unwrap();
            p.bp.frames[frame_i].write().unwrap().data[0] = i as u8 + 1;
            p.bp.unpin_page(id, true);
        }
        p.bp.flush_all().unwrap();
    }

    #[test]
    fn a_backup_records_the_lsn_window_its_pages_sit_in() {
        let p = primary("window");
        seed(&p, 4);
        p.wal
            .append(1, 0, &RecKind::HeapInsert { dir_root: 1, page_id: 2, slot: 0, tuple: vec![7; 8] })
            .unwrap();

        let out = p.dir.join("bk");
        let label = *take(&p.bp, &p.wal, &out).unwrap();

        assert!(label.page_count > 0, "a backup of zero pages is not a backup");
        assert!(
            label.end_lsn >= label.start_lsn,
            "the window is inverted: {} .. {}",
            label.start_lsn,
            label.end_lsn
        );
        assert!(out.join(BASE_IMAGE).exists(), "no image was written");
        assert!(out.join(BACKUP_LABEL).exists(), "no label was written");
    }

    /// The image must hold the primary's actual bytes, judged from the file rather than from the
    /// return value.
    #[test]
    fn the_image_holds_the_primarys_pages() {
        let p = primary("bytes");
        seed(&p, 6);
        let out = p.dir.join("bk");
        let label = *take(&p.bp, &p.wal, &out).unwrap();

        let dest = p.dir.join("restored.db");
        let restored = restore(&out, &dest).unwrap();
        assert_eq!(restored, label, "the label did not survive the round trip");

        let src = std::fs::read(p.dir.join("bytes.db")).unwrap();
        let dst = std::fs::read(&dest).unwrap();
        // The restored file covers every page the primary had at backup time.
        assert!(
            dst.len() >= src.len(),
            "the restore is smaller than the primary: {} < {}",
            dst.len(),
            src.len()
        );
        let common = src.len().min(dst.len());
        assert_eq!(
            src[..common],
            dst[..common],
            "restored bytes differ from the primary's"
        );
    }

    /// A label round trip must be exact. This is the file an operator reads at 3am.
    #[test]
    fn a_label_round_trips_through_text() {
        let l = BackupLabel { start_lsn: 326145, end_lsn: 401920, page_count: 97 };
        assert_eq!(BackupLabel::decode(&l.encode()).unwrap(), l);
    }

    /// Garbage must be refused, not guessed at.
    #[test]
    fn a_label_that_is_not_one_is_refused() {
        assert!(BackupLabel::decode("hello").is_err(), "a non-label was accepted");
        assert!(
            BackupLabel::decode("ferrodb backup_label v1\nstart_lsn 5\n").is_err(),
            "a label missing end_lsn was accepted"
        );
        assert!(
            BackupLabel::decode("ferrodb backup_label v1\nstart_lsn 9\nend_lsn 4\npage_count 1\n")
                .is_err(),
            "a label whose window runs backwards was accepted"
        );
    }

    /// A transfer cut short must be caught at restore time.
    #[test]
    fn a_truncated_image_is_refused_rather_than_restored() {
        let p = primary("short");
        seed(&p, 5);
        let out = p.dir.join("bk");
        let _h = take(&p.bp, &p.wal, &out).unwrap();

        // Lose the last page in transit.
        let img = out.join(BASE_IMAGE);
        let len = std::fs::metadata(&img).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&img).unwrap();
        f.set_len(len - PAGE_SIZE as u64).unwrap();

        let err = restore(&out, &p.dir.join("dest.db"))
            .expect_err("a truncated backup image was restored as if whole");
        assert!(
            format!("{err}").contains("truncated"),
            "refused for the wrong reason: {err}"
        );
    }

    /// **The pin, which is the fix the hot test forced.**
    ///
    /// A checkpoint must not discard a range a backup still points into. Before this existed, a
    /// backup taken while the primary was running was refused the moment a replica tried to use
    /// it: `lsn 183 is below the log's base`.
    #[test]
    fn a_checkpoint_may_not_discard_a_pinned_range() {
        let p = primary("pinned");
        p.wal
            .append(1, 0, &RecKind::HeapInsert { dir_root: 1, page_id: 2, slot: 0, tuple: vec![1; 8] })
            .unwrap();
        p.wal.flush().unwrap();

        let pin = p.wal.pin_durable();
        let base_before = p.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(pin.lsn(), p.wal.flushed_lsn.load(std::sync::atomic::Ordering::SeqCst));

        // More work, then a checkpoint. The pinned range must survive it.
        p.wal
            .append(2, 0, &RecKind::HeapInsert { dir_root: 1, page_id: 3, slot: 0, tuple: vec![2; 8] })
            .unwrap();
        p.wal.flush().unwrap();
        p.wal.truncate(9).unwrap();
        assert_eq!(
            p.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst),
            base_before,
            "a checkpoint discarded the log while a pin still needed it"
        );
        // And the pinned LSN is still readable, which is the property that actually matters.
        assert!(p.wal.read_record(pin.lsn()).is_ok() || pin.lsn() >= p.wal.next_lsn.load(std::sync::atomic::Ordering::SeqCst));

        // Releasing it lets the next checkpoint reclaim. Without this the test would pass just as
        // well against a `truncate` that never truncates anything.
        drop(pin);
        p.wal.truncate(10).unwrap();
        assert!(
            p.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst) > base_before,
            "the log was never reclaimed even after the pin was dropped, so this test cannot tell \
             a working pin from a broken truncate"
        );
    }

    /// Asking to pin something already gone is refused, not quietly clamped forward.
    #[test]
    fn pinning_an_lsn_the_log_has_already_dropped_is_refused() {
        let p = primary("gone");
        p.wal
            .append(1, 0, &RecKind::HeapInsert { dir_root: 1, page_id: 2, slot: 0, tuple: vec![3; 8] })
            .unwrap();
        p.wal.flush().unwrap();
        p.wal.truncate(5).unwrap();
        let base = p.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
        assert!(base > 1, "the log did not advance, so there is nothing to have lost");

        let err = p.wal.pin(base - 1).expect_err("a pin below the base was accepted");
        assert!(format!("{err}").contains("truncated"), "wrong reason: {err}");
        assert!(p.wal.pin(base).is_ok(), "a pin at the base was refused");
    }

    /// A backup older than the primary's log must be refused with a message about *that*, not with
    /// a generic complaint from inside the source.
    #[test]
    fn a_backup_older_than_the_primarys_log_is_refused_by_name() {
        let stale = BackupLabel { start_lsn: 100, end_lsn: 200, page_count: 1 };
        let err = stale
            .assert_usable_with(326145)
            .expect_err("a stale backup was accepted");
        let msg = format!("{err}");
        assert!(msg.contains("stale"), "message does not name the problem: {msg}");
        assert!(msg.contains("326145"), "message does not say where the primary is: {msg}");

        // And a current one is accepted, or the check above would pass by always failing.
        let fresh = BackupLabel { start_lsn: 326145, end_lsn: 400000, page_count: 1 };
        assert!(fresh.assert_usable_with(326145).is_ok(), "a usable backup was refused");
    }
}
