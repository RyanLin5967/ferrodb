//! D202, reached mid-statement: a multi-row UPDATE relocates row 1, then fails on row 2.
//!
//! Found by `update-relocation-adversary` (artie-research `frontier/update_relocation_adversary.md`
//! @ `71ec74e`); pinned at the lead's request (lane `lane_rollback_index_orphan.md` §21.4).
//!
//! # The defect at `9aa6968` (READ there by the adversary; INFERRED here, not run)
//!
//! `Update::execute` drains its child, then writes the rows one at a time. Row 1's new tuple does
//! not fit on its page, so `HeapFileManager::update` relocates it: a `HeapDelete` of its home slot,
//! a `HeapInsert` on another page, and then `primary_index.upsert(1, new_rid)`. Row 2 then fails
//! (`10 / q` with `q = 0`), and the failed statement aborts its transaction. The abort undoes the
//! heap records only: it frees `new_rid` and restores row 1 at home. The primary entry still names
//! `new_rid`, a freed slot, so every lookup of key 1 fails with "the slot is delted", while a scan
//! finds the row.
//!
//! No failing row is needed for that (`BEGIN; UPDATE; ROLLBACK;` is enough). That shape is pinned by
//! `rollback_index_undo::a_rolled_back_relocating_update_keeps_its_row_findable_by_key_and_its_key_taken`.
//! This file pins the one no test covered: the failure inside the statement, after the relocation.
//!
//! # The fix (`c21eaff`, D202)
//!
//! The relocation arm records the primary write (`TxnManager::record_primary_write`) before its
//! upsert, and `abort` puts every recorded entry back before it undoes the heap.
//!
//! # Pre-registered (lane §21.4)
//!
//! At `9aa6968` (a copy of this file in a detached checkout): FAILS at the lookup by key, after its
//! premises pass. At the branch tip: passes. Under mutant N4 (no `record_primary_write` in the
//! relocation arm): FAILS at the lookup by key. Uses only API that exists at `9aa6968`. INFERRED from
//! source and never run.

use std::fs::OpenOptions;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::log::{RecKind, WalManager};
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    wal: Arc<WalManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("midstmt.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("midstmt.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal.clone());
        Db { catalog, bp, wal, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn rows(&mut self, sql: &str, s: &mut Session) -> Vec<Vec<Value>> {
        match self.ok(sql, s) {
            Outcome::Rows(mut r) => {
                r.sort_by_key(|row| match row.first() {
                    Some(Value::Integer(id)) => *id,
                    _ => i32::MAX,
                });
                r
            }
            _ => panic!("`{sql}` did not return rows"),
        }
    }

    fn assert_plan(&mut self, select: &str, needle: &str, s: &mut Session) {
        let sql = format!("EXPLAIN {select}");
        match self.ok(&sql, s) {
            Outcome::Explain(plan) => assert!(
                plan.contains(needle),
                "premise failed: `{select}` does not plan as `{needle}`:\n{plan}"
            ),
            _ => panic!("`{sql}` did not explain"),
        }
    }

    fn primary_rid(&self, key: i32) -> Option<RecordId> {
        let entry = self.catalog.get_table("notes").expect("table");
        BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, self.bp.clone())
            .search(&Value::Integer(key))
            .expect("primary search")
    }

    /// The heap writes logged from `from` up to the first CLR, where the abort began, in order:
    /// `(is_delete, dir_root, page, slot)`.
    fn writes_before_the_abort(&self, from: u64) -> Vec<(bool, u32, u32, u16)> {
        let mut out = Vec::new();
        let mut lsn = from;
        let end = self.wal.next_lsn.load(Ordering::SeqCst);
        while lsn < end {
            let (rec, next) = self.wal.read_record(lsn).expect("read a log record");
            match rec.kind {
                RecKind::Clr { .. } => break,
                RecKind::HeapDelete { dir_root, page_id, slot, .. } => out.push((true, dir_root, page_id, slot)),
                RecKind::HeapInsert { dir_root, page_id, slot, .. } => out.push((false, dir_root, page_id, slot)),
                _ => {}
            }
            lsn = next;
        }
        out
    }
}

fn row(id: i32, note: &str, q: i32) -> Vec<Value> {
    vec![Value::Integer(id), Value::Varchar(note.to_string()), Value::Integer(q)]
}

const BY_KEY: &str = "SELECT id, note, q FROM notes WHERE id = 1;";

#[test]
fn a_multi_row_update_that_fails_after_relocating_row_1_rolls_back_its_primary_entry() {
    let mut db = Db::new();
    let mut s = db.session();
    // Row 1 first (slot 0), then a 3900-byte row 2 (slot 1) on the same page, so a 200-character
    // note does not fit beside them and row 1 must relocate. The UPDATE writes rows in scan order,
    // so row 1 is written, and relocated, before row 2 fails.
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000), q INTEGER);", &mut s);
    db.ok("INSERT INTO notes VALUES (1, 'a', 1);", &mut s);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}', 0);", "x".repeat(3900)), &mut s);
    let home = db.primary_rid(1).expect("row 1 is indexed");
    let two = db.primary_rid(2).expect("row 2 is indexed");
    assert_eq!(home.page_id, two.page_id, "premise failed: rows 1 and 2 are not on one page");
    db.assert_plan(BY_KEY, "Index scan on notes (col 0", &mut s);
    let heap = db.catalog.get_table("notes").expect("table").first_directory_page_id;

    // Row 1 grows and relocates; row 2 divides by zero. Autocommit, so the failure aborts the
    // statement's own transaction.
    let from = db.wal.next_lsn.load(Ordering::SeqCst);
    let update = format!("UPDATE notes SET note = '{}', q = 10 / q;", "y".repeat(200));
    let err = match db.exec(&update, &mut s) {
        Err(e) => e,
        Ok(_) => panic!("premise failed: the UPDATE did not fail on row 2"),
    };
    assert!(err.to_string().contains("division by zero"), "premise failed: the UPDATE failed, but not on row 2's division: {err}");
    let writes = db.writes_before_the_abort(from);
    let deleted_home = writes
        .iter()
        .position(|&(del, root, page, slot)| del && root == heap && page == home.page_id && slot == home.slot_num);
    let inserted_elsewhere = writes.iter().position(|&(del, root, page, _)| !del && root == heap && page != home.page_id);
    assert!(
        matches!((deleted_home, inserted_elsewhere), (Some(d), Some(i)) if d < i),
        "premise failed: row 1 did not relocate before the failure, so this is not the relocation case: {writes:?}"
    );

    // The rollback must have put the key back where the row is.
    assert_eq!(db.rows(BY_KEY, &mut s), vec![row(1, "a", 1)], "after the failed UPDATE, row 1 is not found by key at its old values");
    assert_eq!(
        db.rows("SELECT id, note, q FROM notes;", &mut s),
        vec![row(1, "a", 1), row(2, &"x".repeat(3900), 0)],
        "after the failed UPDATE, a scan does not return both rows unchanged"
    );
    match db.exec("INSERT INTO notes VALUES (1, 'dup', 5);", &mut s) {
        Err(FerroError::Constraint(m)) if m.contains("duplicate primary key") => {}
        Err(e) => panic!("re-INSERT of live key 1: refused, but not as a duplicate key: {e}"),
        Ok(_) => panic!("re-INSERT of live key 1: ADMITTED. Two live rows now share one primary key"),
    }
    assert_eq!(db.primary_rid(1), Some(home), "the key does not point back at the slot the rollback restored row 1 to");
}
