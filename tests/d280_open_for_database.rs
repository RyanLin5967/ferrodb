//! D280 — `WalManager::open_for_database`, the one open path the CLI and `pgserver` share, asked
//! directly. `d280_orphaned_wal_binaries.rs` proves the binaries refuse and lose nothing. This file
//! pins each part of the rule, so that a mutant of any one part fails a test named for it:
//! - which cause is named;
//! - when the pages are read at all;
//! - which pages count;
//! - which page is reported.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::branch::types::{ArenaId, Epoch};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::page_header::{stamp_checksum, PageHeader, PageType};
use ferrodb::error::{FerroError, LogBehindPagesCause};
use ferrodb::replication::{backup, ReplicaState};
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::storage::heap_page::Page;
use ferrodb::storage::storage::Storage;
use ferrodb::wal::log::WalManager;

const HEADROOM: u32 = 256;
const ROWS: i64 = 20;
const DEADLINE: Duration = Duration::from_secs(120);

fn wal_of(db: &Path) -> PathBuf {
    let mut p = db.as_os_str().to_os_string();
    p.push(".wal");
    PathBuf::from(p)
}

fn lock_of(db: &Path) -> PathBuf {
    let mut p = db.as_os_str().to_os_string();
    p.push(".lock");
    PathBuf::from(p)
}

fn open_rw(db: &Path) -> std::fs::File {
    std::fs::OpenOptions::new().read(true).write(true).create(true).open(db).unwrap()
}

/// Counts every `pread`, so "a normal open reads no page" is a number and not a claim.
struct CountingStorage {
    inner: std::fs::File,
    reads: Arc<AtomicU64>,
}

impl Storage for CountingStorage {
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        Storage::pwrite(&self.inner, buf, offset)
    }
    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Storage::pread(&self.inner, buf, offset)
    }
    fn sync_all(&self) -> std::io::Result<()> {
        Storage::sync_all(&self.inner)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        Storage::sync_data(&self.inner)
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        Storage::set_len(&self.inner, len)
    }
    fn len(&self) -> std::io::Result<u64> {
        Storage::len(&self.inner)
    }
}

/// `open_for_database` over a `DiskManager` whose reads are counted: the result, and how many
/// `pread`s the open itself issued.
fn counted_open(db: &Path) -> (Result<WalManager, FerroError>, u64) {
    let reads = Arc::new(AtomicU64::new(0));
    let dm = DiskManager::with_storage(Arc::new(CountingStorage { inner: open_rw(db), reads: reads.clone() }))
        .unwrap();
    let before = reads.load(Ordering::SeqCst);
    let out = WalManager::open_for_database(db, &dm);
    (out, reads.load(Ordering::SeqCst) - before)
}

/// The refusal's fields, or a panic saying what came back instead.
fn refusal(r: Result<WalManager, FerroError>) -> (String, LogBehindPagesCause, u32, u64) {
    match r {
        Err(FerroError::LogBehindPages { db, cause, page_id, page_lsn }) => (db, cause, page_id, page_lsn),
        Err(e) => panic!("refused, but not as LogBehindPages: {e:?}"),
        Ok(_) => panic!("opened a data file whose pages carry LSNs the log never issued"),
    }
}

// ---- the CLI-built primary and its restored copy -----------------------------------------------

fn cli(db: &Path) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_ferrodb"));
    c.arg(db)
        .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
        .env("FERRODB_LEASE_SCAN_MILLIS", "3600000")
        .env("FERRODB_CHECKPOINT_INTERVAL", "100000")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    c
}

fn session(db: &Path, sql: &str) -> (ExitStatus, String) {
    let mut child = cli(db).spawn().expect("spawn ferrodb");
    child.stdin.take().unwrap().write_all(sql.as_bytes()).expect("write sql");
    let out = child.wait_with_output().expect("wait for ferrodb");
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    (out.status, text)
}

fn cli_primary(dir: &Path) -> PathBuf {
    let db = dir.join("p.db");
    let mut sql = String::from("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n");
    for i in 1..=ROWS {
        sql.push_str(&format!("INSERT INTO inv VALUES ({i}, {i});\n"));
    }
    let (status, out) = session(&db, &sql);
    assert!(status.success() && !out.contains("error:"), "building the primary failed:\n{out}");
    db
}

/// The page holding the primary's rows, and the LSN on it. Found through the catalog and the heap,
/// which is a different route from the page scan under test, so the expectation does not come from
/// the subject.
fn rows_page(db: &Path) -> (u32, u64) {
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(open_rw(db)).unwrap())));
    let catalog = Catalog::open(bp.clone(), 1).expect("open the primary's catalog");
    let root = catalog.get_table("inv").expect("the table").first_directory_page_id;
    let mut pages: Vec<u32> = HeapFileManager::open(root, bp.clone())
        .scan()
        .map(|r| r.expect("scan the primary's heap").0.page_id)
        .collect();
    pages.dedup();
    assert_eq!(pages.len(), 1, "fixture: the primary's rows span {pages:?}, not one page");
    let lsn = Page::deserialize(bp.disk_manager.read(pages[0]).unwrap()).unwrap().lsn;
    assert!(lsn > 0, "fixture: the primary's rows page carries no LSN");
    (pages[0], lsn)
}

/// `backup::take` then `backup::restore` to `<dir>/r.db`, with the label the replica starts from.
fn restored_copy(primary: &Path, dir: &Path) -> (PathBuf, backup::BackupLabel) {
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(open_rw(primary)).unwrap())));
    let wal = Arc::new(WalManager::new(wal_of(primary)).unwrap());
    bp.attach_wal(wal.clone());
    let backup_dir = dir.join("backup");
    drop(backup::take(&bp, &wal, &backup_dir).expect("take a base backup"));
    let dest = dir.join("r.db");
    let label = backup::restore(&backup_dir, &dest).expect("restore the backup");
    assert!(!wal_of(&dest).exists(), "fixture: restore wrote a WAL");
    (dest, label)
}

// ---- a database written page by page, with no log at all ----------------------------------------

/// A data file built with the `DiskManager` alone: `n` heap pages at LSN 0, allocated through the
/// bitmap. No log has ever existed for it, which is a legitimate database that has logged nothing.
fn unlogged(dir: &Path, n: u32) -> PathBuf {
    let db = dir.join("n.db");
    let dm = DiskManager::new(open_rw(&db)).unwrap();
    for _ in 0..n {
        let id = dm.allocate().unwrap();
        dm.write(id, &Page::empty(id).serialize().unwrap()).unwrap();
    }
    dm.sync().unwrap();
    db
}

fn write_page(db: &Path, id: u32, bytes: &[u8; PAGE_SIZE]) {
    let dm = DiskManager::new(open_rw(db)).unwrap();
    dm.write(id, bytes).unwrap();
    dm.sync().unwrap();
}

fn heap_page_at(id: u32, lsn: u64) -> [u8; PAGE_SIZE] {
    let mut p = Page::empty(id);
    p.lsn = lsn;
    p.serialize().unwrap()
}

fn file_pages(db: &Path) -> u32 {
    (std::fs::metadata(db).unwrap().len() / PAGE_SIZE as u64) as u32
}

// ---- tests ---------------------------------------------------------------------------------------

/// T3. A restored base backup: no `<db>.wal`, pages at the primary's LSNs. Refused as a missing log,
/// naming the page the primary's rows live on and its LSN. The refused open creates no log.
#[test]
fn a_restored_backup_is_refused_as_a_missing_log() {
    let dir = tempfile::tempdir().unwrap();
    let primary = cli_primary(dir.path());
    let (rows_id, rows_lsn) = rows_page(&primary);
    let (db, _) = restored_copy(&primary, dir.path());
    let copied = DiskManager::new(open_rw(&db)).unwrap().read(rows_id).unwrap();
    let original = DiskManager::new(open_rw(&primary)).unwrap().read(rows_id).unwrap();
    assert!(copied == original, "fixture: restore did not copy the rows page verbatim");

    let (named, cause, page_id, page_lsn) = refusal(counted_open(&db).0);
    assert_eq!(cause, LogBehindPagesCause::MissingLog);
    assert_eq!((page_id, page_lsn), (rows_id, rows_lsn), "the refusal names the wrong page");
    assert_eq!(named, db.display().to_string());
    assert!(!wal_of(&db).exists(), "the refused open created {}", wal_of(&db).display());
}

/// T4. The same copy, after `repl_replica`'s own record of progress. A replica has no log of its
/// own, and the refusal says it is a replica's file rather than a lost log.
#[test]
fn a_replicas_file_is_refused_as_a_replica() {
    let dir = tempfile::tempdir().unwrap();
    let primary = cli_primary(dir.path());
    let (db, label) = restored_copy(&primary, dir.path());
    ReplicaState::at(&db).record_applied(label.end_lsn).expect("record the replica's position");
    assert!(ReplicaState::at(&db).path().exists(), "fixture: no .replstate was written");

    let (_, cause, _, page_lsn) = refusal(counted_open(&db).0);
    assert_eq!(cause, LogBehindPagesCause::ReplicaFile);
    assert!(page_lsn > 0);
    assert!(!wal_of(&db).exists(), "the refused open created {}", wal_of(&db).display());
}

/// T5. The same copy with an empty log beside it — one some earlier open created and never wrote
/// to. It exists, so "missing" is not the test; it has never issued an LSN, which is.
#[test]
fn a_log_that_never_issued_an_lsn_is_refused_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    let primary = cli_primary(dir.path());
    let (db, _) = restored_copy(&primary, dir.path());
    drop(WalManager::new(wal_of(&db)).unwrap());
    let len = std::fs::metadata(wal_of(&db)).unwrap().len();

    let (_, cause, _, page_lsn) = refusal(counted_open(&db).0);
    assert_eq!(cause, LogBehindPagesCause::EmptyLog);
    assert!(page_lsn > 0);
    assert_eq!(std::fs::metadata(wal_of(&db)).unwrap().len(), len, "the refused open wrote to the log");
}

/// T6. An ordinary database reads no page at open, both after a clean exit and after a crash that
/// left committed records in the log. The positive control is the same instrument on the restored
/// copy, which does read, so a counter that never counts cannot pass this.
#[test]
fn a_normal_reopen_reads_no_page() {
    let dir = tempfile::tempdir().unwrap();

    // Cleanly closed by the CLI.
    let clean = cli_primary(dir.path());
    let (opened, reads) = counted_open(&clean);
    let wal = opened.expect("an ordinary database was refused");
    assert!(wal.base_lsn.load(Ordering::SeqCst) > 1, "fixture: the clean database's log never issued anything");
    assert_eq!(reads, 0, "opening a cleanly closed database read {reads} times");
    drop(wal);

    // Killed right after a commit, so its log still holds that transaction.
    let crashed = dir.path().join("c.db");
    let (status, out) = session(&crashed, "CREATE TABLE t (id INTEGER NOT NULL);\n");
    assert!(status.success() && !out.contains("error:"), "{out}");
    let mut child = Reaped(cli(&crashed).spawn().unwrap());
    let stdout = lines_of(child.0.stdout.take().unwrap());
    let mut seen = Vec::new();
    wait_for(&stdout, "type .exit", "banner", &mut seen).expect("the CLI did not open c.db");
    let mut stdin = child.0.stdin.take().unwrap();
    stdin.write_all(b"INSERT INTO t VALUES (7);\n").unwrap();
    stdin.flush().unwrap();
    wait_for(&stdout, "row affected", "commit acknowledgement", &mut seen).expect("no commit");
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    std::fs::remove_file(lock_of(&crashed)).unwrap();
    let (opened, reads) = counted_open(&crashed);
    let wal = opened.expect("a crashed ordinary database was refused");
    assert!(
        wal.next_lsn.load(Ordering::SeqCst) > wal.base_lsn.load(Ordering::SeqCst),
        "fixture: the crashed database's log holds no records"
    );
    assert_eq!(reads, 0, "opening a crashed database read {reads} times");
    drop(wal);

    // Positive control: the instrument does count when the scan runs.
    let sub = dir.path().join("control");
    std::fs::create_dir(&sub).unwrap();
    let (db, _) = restored_copy(&clean, &sub);
    let (opened, reads) = counted_open(&db);
    refusal(opened);
    assert!(reads >= file_pages(&db) as u64, "CONTROL: the scan of {} pages counted {reads} reads", file_pages(&db));
}

/// T7. A database that never logged anything, holding the two kinds of page the WAL gate's
/// classifier misreads: the bitmap page (enough pages allocated that its bytes 11..19 are set) and
/// a COW arena page (`data[0]` is the top byte of a small epoch). Neither is a page LSN, and
/// the database opens.
#[test]
fn a_never_logged_database_with_bitmap_and_cow_pages_opens() {
    let dir = tempfile::tempdir().unwrap();
    let db = unlogged(dir.path(), 70);
    let cow_id = file_pages(&db);
    let mut cow = [0u8; PAGE_SIZE];
    PageHeader::new(Epoch(3), ArenaId(0x0102_0304), PageType::BTreeLeaf).write_to(&mut cow);
    stamp_checksum(&mut cow);
    write_page(&db, cow_id, &cow);

    // Premises: without the self-naming rule, both pages would read as heap pages with an LSN.
    let bitmap = DiskManager::new(open_rw(&db)).unwrap().read(0).unwrap();
    assert_eq!(bitmap[0], 0, "fixture: the bitmap page has a chained pointer; its type byte is not 0");
    assert_ne!(bitmap[11..19], [0u8; 8], "fixture: 70 pages did not set the bitmap's bytes 11..19");
    assert_eq!(cow[0], 0, "fixture: the COW page's first byte is not 0");
    assert_ne!(cow[11..19], [0u8; 8], "fixture: the COW page's bytes 11..19 are zero");

    let (opened, _) = counted_open(&db);
    opened.expect("a database that never logged anything was refused");
    assert!(wal_of(&db).exists(), "the accepted open did not create the log");
}

/// T8. One heap page at exactly the first LSN a log issues, planted as the file's last page, above
/// anything the bitmap ever allocated — where a replica's applier puts a page the primary allocated
/// after the backup. It is found, and named.
#[test]
fn a_page_above_the_bitmap_at_the_first_lsn_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = unlogged(dir.path(), 70);
    let planted = file_pages(&db);
    write_page(&db, planted, &heap_page_at(planted, 1));

    let dm = DiskManager::new(open_rw(&db)).unwrap();
    assert!(dm.bitmap_high_water().unwrap() <= planted, "fixture: the bitmap covers the planted page");
    assert_eq!(file_pages(&db) - 1, planted, "fixture: the planted page is not the last page");
    drop(dm);

    let (_, cause, page_id, page_lsn) = refusal(counted_open(&db).0);
    assert_eq!(cause, LogBehindPagesCause::MissingLog);
    assert_eq!((page_id, page_lsn), (planted, 1));
    assert!(!wal_of(&db).exists(), "the refused open created {}", wal_of(&db).display());
}

/// T9. Several stamped pages: the refusal names the highest LSN, which is how far a log would have
/// to start to be above every page. It sits between a lower first page and a lower last one, so
/// neither "first found" nor "last found" gets it.
#[test]
fn the_refusal_names_the_page_with_the_highest_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let db = unlogged(dir.path(), 14);
    for (id, lsn) in [(5u32, 500u64), (9, 900), (12, 700)] {
        write_page(&db, id, &heap_page_at(id, lsn));
    }
    let (_, _, page_id, page_lsn) = refusal(counted_open(&db).0);
    assert_eq!((page_id, page_lsn), (9, 900));
}

/// T10. A brand-new database opens and gets a log at the first LSN. Reopened before anything is
/// logged, the log exists and has issued nothing, so the scan runs, finds no stamped page, and the
/// open succeeds.
#[test]
fn a_fresh_database_opens_and_gets_a_log_at_the_first_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("f.db");
    let dm = DiskManager::new(open_rw(&db)).unwrap();
    let wal = WalManager::open_for_database(&db, &dm).expect("a brand-new database was refused");
    assert_eq!(wal.base_lsn.load(Ordering::SeqCst), 1);
    assert_eq!(wal.next_lsn.load(Ordering::SeqCst), 1);
    assert!(wal_of(&db).exists());
    drop(wal);
    drop(dm);

    let (opened, reads) = counted_open(&db);
    opened.expect("a brand-new database was refused on its second open");
    assert!(reads > 0, "the second open of a never-logged database did not scan");
}

// ---- child-process plumbing ----------------------------------------------------------------------

struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn lines_of(pipe: impl Read + Send + 'static) -> Receiver<String> {
    let (tx, rx) = channel();
    std::thread::spawn(move || {
        for line in BufReader::new(pipe).lines() {
            match line {
                Ok(l) => {
                    if tx.send(l).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
    rx
}

fn wait_for(rx: &Receiver<String>, needle: &str, what: &str, seen: &mut Vec<String>) -> Option<String> {
    let until = Instant::now() + DEADLINE;
    loop {
        let left = until.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(line) => {
                seen.push(line.clone());
                if line.contains(needle) {
                    return Some(line);
                }
            }
            Err(RecvTimeoutError::Disconnected) => return None,
            Err(RecvTimeoutError::Timeout) => {
                panic!("no {what} within {DEADLINE:?}; stdout so far:\n{}", seen.join("\n"))
            }
        }
    }
}
