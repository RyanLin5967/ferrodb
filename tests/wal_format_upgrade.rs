//! F1 of the adversary on `993145c..115f0b7` (`frontier/rollback_adversary.md` @ `4902c75`): a log
//! written before D213 must still open.
//!
//! Before D213, redo FREED a forward `HeapDelete`, and a committed relocation's bytes were reusable at
//! once, so a later committed insert could land in them. D213 made redo RETIRE a forward delete until
//! a `HeapRelease`, which no older log contains. Replaying such a log with D213's semantics refuses
//! that insert (`NotEnoughSpace`), and the database does not open. The WAL `VERSION` was 2 before
//! and after D213, so nothing told the two apart.
//!
//! The lead's decision: version the WAL. New logs are version 3. A version-2 log is replayed with
//! version-2 semantics (a forward delete frees), then checkpointed, which rewrites it as version 3.
//! Until then no transaction may begin on it; recovery's own compensation needs none.
//!
//! The version-2 log here is built by hand, record by record, as the older binary wrote it, and
//! then labelled version 2 in its header. INFERRED from source and never run.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::{RecKind, WalManager};
use ferrodb::wal::recovery::{open_recovered, OpenedDatabase};
use ferrodb::wal::txn::TxnManager;

struct Db {
    o: OpenedDatabase,
    _lock: DbLock,
}

impl Db {
    fn open(path: &Path) -> Result<Db, FerroError> {
        let lock = DbLock::acquire(path)?;
        let o = open_recovered(path, &lock)?;
        Ok(Db { o, _lock: lock })
    }

    fn ok(&mut self, sql: &str) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        let mut s = Session::new();
        run(stmts.remove(0), &mut self.o.catalog, self.o.bp.clone(), self.o.txn.clone(), &mut s)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        match self.ok(sql) {
            Outcome::Rows(r) => r,
            _ => panic!("`{sql}` did not return rows"),
        }
    }
}

fn wal_path(db: &Path) -> PathBuf {
    let mut p = db.as_os_str().to_os_string();
    p.push(".wal");
    PathBuf::from(p)
}

/// The version field of a WAL's header: bytes 4..8, big-endian (`wal::log`, `HEADER_SIZE` 24).
fn wal_version(wal: &Path) -> u32 {
    let mut f = std::fs::File::open(wal).unwrap();
    let mut header = [0u8; 8];
    f.read_exact(&mut header).unwrap();
    u32::from_be_bytes(header[4..8].try_into().unwrap())
}

/// Label a closed WAL as version 2, the format every binary before D213 wrote.
fn label_as_version_2(wal: &Path) {
    let mut f = std::fs::OpenOptions::new().write(true).open(wal).unwrap();
    f.seek(SeekFrom::Start(4)).unwrap();
    f.write_all(&2u32.to_be_bytes()).unwrap();
    f.sync_all().unwrap();
}

fn note(id: i32, text: &str) -> Vec<Value> {
    vec![Value::Integer(id), Value::Varchar(text.to_string())]
}

/// **The adversary's schedule, from a log the older binary wrote.** Transaction 101 relocates row 1,
/// which is the LOWEST tuple on page P, so under version-2 semantics its 35 B are free at once.
/// Transaction 102 then commits a 114 B row into P, which fits only in those bytes (96 B were free
/// before, 131 B after). FAILS at `56f6752` at the open: redo retires the delete and refuses 102's
/// insert.
#[test]
fn a_pre_d213_log_with_an_insert_into_a_relocations_bytes_opens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");

    // A database with the table on disk (CREATE TABLE checkpoints), and two data pages allocated,
    // as the older binary had allocated them before it crashed.
    let (dir_root, schema, p, q) = {
        let mut db = Db::open(&path).unwrap();
        db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));");
        let entry = db.o.catalog.get_table("notes").expect("table");
        let (dir_root, schema) = (entry.first_directory_page_id, entry.schema.clone());
        let p = db.o.bp.new_page().unwrap();
        let q = db.o.bp.new_page().unwrap();
        (dir_root, schema, p, q)
    };

    // The older binary's records. No `HeapRelease` exists in this format.
    {
        let wal = WalManager::new(wal_path(&path)).unwrap();
        let row = |id: i32, text: &str, ts: u64| {
            Tuple::serialize(&[Value::Integer(id), Value::Varchar(text.to_string())], &schema, ts).unwrap().data
        };
        let log = |txn: u64, kind: RecKind| {
            wal.append(txn, 0, &kind).unwrap();
        };
        log(100, RecKind::Begin);
        log(100, RecKind::HeapInsert { dir_root, page_id: p, slot: 0, tuple: row(2, &"x".repeat(3900), 100) });
        log(100, RecKind::HeapInsert { dir_root, page_id: p, slot: 1, tuple: row(1, "a", 100) });
        log(100, RecKind::Commit);
        log(100, RecKind::TxnEnd);
        log(101, RecKind::Begin);
        log(101, RecKind::HeapDelete { dir_root, page_id: p, slot: 1, old: row(1, "a", 100) });
        log(101, RecKind::HeapInsert { dir_root, page_id: q, slot: 0, tuple: row(1, &"y".repeat(200), 101) });
        log(101, RecKind::Commit);
        log(101, RecKind::TxnEnd);
        log(102, RecKind::Begin);
        log(102, RecKind::HeapInsert { dir_root, page_id: p, slot: 2, tuple: row(3, &"z".repeat(80), 102) });
        log(102, RecKind::Commit);
        log(102, RecKind::TxnEnd);
        wal.flush().unwrap();
    }
    label_as_version_2(&wal_path(&path));

    let mut db = Db::open(&path).unwrap_or_else(|e| panic!("the pre-D213 log did not open: {e}"));
    assert_eq!(db.rows("SELECT id, note FROM notes WHERE id = 1;"), vec![note(1, &"y".repeat(200))], "row 1 is not its relocated self");
    assert_eq!(db.rows("SELECT id, note FROM notes WHERE id = 2;"), vec![note(2, &"x".repeat(3900))], "row 2 is missing");
    assert_eq!(db.rows("SELECT id, note FROM notes WHERE id = 3;"), vec![note(3, &"z".repeat(80))], "row 3, the insert into the freed bytes, is missing");
    assert_eq!(db.rows("SELECT id FROM notes;").len(), 3, "the table does not hold exactly rows 1, 2 and 3");
    drop(db);
    assert_eq!(wal_version(&wal_path(&path)), 3, "the open did not rewrite the replayed log as version 3");
    let mut db = Db::open(&path).expect("the upgraded database did not reopen");
    assert_eq!(db.rows("SELECT id FROM notes;").len(), 3, "the second open lost rows");
}

/// **A version-2 log accepts no new transaction until it has been replayed and upgraded.** Only
/// `open_recovered` does that, so a writer that opened the log some other way cannot append records
/// with D213's meaning to a log labelled with the older one. FAILS at `56f6752` at "accepted a new
/// transaction", where every log is version 2 and nothing is refused.
#[test]
fn a_version_2_log_refuses_new_transactions_until_a_checkpoint_upgrades_it() {
    let dir = tempfile::tempdir().unwrap();
    let wal_file = dir.path().join("legacy.wal");
    drop(WalManager::new(wal_file.clone()).unwrap());
    label_as_version_2(&wal_file);

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("legacy.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(wal_file.clone()).unwrap());
    let txn = TxnManager::new(wal.clone(), bp.clone());
    bp.attach_wal(wal);

    assert!(txn.begin().is_err(), "a version-2 log accepted a new transaction before it was upgraded");
    txn.checkpoint().expect("the checkpoint that upgrades a replayed log was refused");
    assert_eq!(wal_version(&wal_file), 3, "the checkpoint did not rewrite the header as version 3");
    txn.begin().expect("an upgraded log refused a transaction");
}

/// **Guard: an EMPTY version-2 log is upgraded at open too, so writes work after it.** `recover`
/// replays nothing and returns `false` for an empty log, so the checkpoint that upgrades it runs
/// only because the log is version 2. Without that, the first transaction would be refused. A guard
/// for the fix, added with it: killed by the runner's F1c, which stops `open_recovered` checkpointing
/// a legacy log. At `56f6752` every log was version 2, so only its final version check would fail.
///
/// The empty log is made the way a real one gets that way. The first open creates the table, and
/// that DDL leaves its records in the log. The second open replays them and checkpoints, and a
/// process that ran no DDL re-declares nothing, so the log it leaves is empty. Then it is labelled
/// version 2.
#[test]
fn an_empty_version_2_log_is_upgraded_at_open_so_writes_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty_legacy.db");
    Db::open(&path).unwrap().ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(40));");
    drop(Db::open(&path).unwrap());
    let len = std::fs::metadata(wal_path(&path)).unwrap().len();
    assert_eq!(len, 24, "premise: the log is not empty (header only) after the second open");
    label_as_version_2(&wal_path(&path));

    let mut db = Db::open(&path).expect("an empty version-2 log did not open");
    db.ok("INSERT INTO notes VALUES (1, 'a');");
    assert_eq!(db.rows("SELECT id, note FROM notes;"), vec![note(1, "a")], "the insert after the upgrade is missing");
    drop(db);
    assert_eq!(wal_version(&wal_path(&path)), 3, "the open did not rewrite the empty log as version 3");
}

/// **Q5 of review 2: the log itself refuses the two records whose meaning changed**, while it is
/// version 2. `begin` already refuses a transaction on such a log, but that guard cannot see a
/// direct `WalManager::append`. A top-level `HeapDelete` (it RETIRES now, and FREED then) and a
/// `HeapRelease` (it did not exist) are refused. Everything that means the same in both formats is
/// accepted: `Begin`, `Abort`, a `Clr` (even one carrying a `HeapDelete`), `TxnEnd`. FAILS at
/// `368d0e1` at the first refusal: `append` checked only the poison.
#[test]
fn a_version_2_log_refuses_the_records_whose_meaning_changed() {
    let dir = tempfile::tempdir().unwrap();
    let wal_file = dir.path().join("legacy_append.wal");
    drop(WalManager::new(wal_file.clone()).unwrap());
    label_as_version_2(&wal_file);
    let wal = WalManager::new(wal_file).unwrap();

    let delete = RecKind::HeapDelete { dir_root: 1, page_id: 2, slot: 0, old: vec![0; 24] };
    let release = RecKind::HeapRelease { dir_root: 1, page_id: 2, slot: 0 };
    assert!(wal.append(1, 0, &RecKind::Begin).is_ok(), "a version-2 log refused a Begin");
    assert!(wal.append(1, 0, &delete).is_err(), "a version-2 log accepted a forward HeapDelete, which means something else in it");
    assert!(wal.append(1, 0, &release).is_err(), "a version-2 log accepted a HeapRelease, which it cannot contain");
    assert!(wal.append(1, 0, &RecKind::Abort).is_ok(), "a version-2 log refused an Abort");
    let clr = RecKind::Clr { undone_lsn: 1, undo_next: 0, redo: Box::new(delete) };
    assert!(wal.append(1, 0, &clr).is_ok(), "a version-2 log refused a CLR, which recovery's undo of its losers writes");
    assert!(wal.append(1, 0, &RecKind::TxnEnd).is_ok(), "a version-2 log refused a TxnEnd");
}

/// **C6 of review 2: the state a crash during the upgrade actually leaves.** `truncate` writes the
/// version-3 header, with its base set to the log's end, BEFORE it shortens the file. A crash in
/// between leaves a version-3 header over version-2 frames. The frames' embedded LSNs no longer match
/// the new base, so `scan_valid_end` drops them all, and the open must see an empty version-3 log
/// that accepts a transaction. A guard: it passes at `368d0e1` too, because that path does not
/// depend on the version.
#[test]
fn a_crash_between_the_upgrade_header_and_the_truncation_leaves_an_empty_version_3_log() {
    let dir = tempfile::tempdir().unwrap();
    let wal_file = dir.path().join("half_upgraded.wal");
    let end = {
        let wal = WalManager::new(wal_file.clone()).unwrap();
        wal.append(1, 0, &RecKind::Begin).unwrap();
        wal.append(1, 0, &RecKind::HeapDelete { dir_root: 1, page_id: 2, slot: 0, old: vec![0; 24] }).unwrap();
        wal.append(1, 0, &RecKind::Commit).unwrap();
        wal.flush().unwrap();
        wal.next_lsn.load(std::sync::atomic::Ordering::SeqCst)
    };
    label_as_version_2(&wal_file);
    // The new header, exactly as `truncate` writes it: magic, version 3, base = the old end, and a
    // transaction high-water. The file is NOT shortened.
    {
        let mut header = [0u8; 24];
        header[0..4].copy_from_slice(&0xF3_EE_DB_01u32.to_be_bytes());
        header[4..8].copy_from_slice(&3u32.to_be_bytes());
        header[8..16].copy_from_slice(&end.to_be_bytes());
        header[16..24].copy_from_slice(&2u64.to_be_bytes());
        let mut f = std::fs::OpenOptions::new().write(true).open(&wal_file).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&header).unwrap();
        f.sync_all().unwrap();
    }
    assert!(std::fs::metadata(&wal_file).unwrap().len() > 24, "premise: the old frames are not still in the file");

    let wal = Arc::new(WalManager::new(wal_file.clone()).unwrap());
    assert_eq!(wal_version(&wal_file), 3, "premise: the header is not version 3");
    assert_eq!(wal.next_lsn.load(std::sync::atomic::Ordering::SeqCst), end, "the old version-2 frames were read as part of the version-3 log");
    assert_eq!(std::fs::metadata(&wal_file).unwrap().len(), 24, "the open did not trim the dropped version-2 frames");
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.path().join("half_upgraded.db")).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let txn = TxnManager::new(wal.clone(), bp.clone());
    bp.attach_wal(wal);
    txn.begin().expect("the half-upgraded log, now an empty version-3 log, refused a transaction");
}
