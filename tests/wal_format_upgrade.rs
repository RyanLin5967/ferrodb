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
//! Until then it accepts nothing but the compensation recovery itself writes.
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
