//! E66 — what dead secondary-index entries actually cost, measured rather than assumed.
//!
//! # The claim being tested
//!
//! No write path removes a secondary-index entry. `DELETE` stamps `end_ts` on the version and stops.
//! `UPDATE` of an indexed column *inserts* the new `(value, pk)` entry and leaves the old one:
//!
//! ```text
//! for handle in &self.secondary_indexes {
//!     if old_v != new_v { handle.tree.insert((new_v.clone(), pk.clone()), ())?; }
//! }
//! ```
//!
//! So a secondary index grows by one entry per delete and one per update of the indexed column, and
//! never shrinks. That was recorded in E66 as an unquantified suspicion. This file measures it.
//!
//! # It was a wrong-results bug, and the growth is what made it one
//!
//! The row this file opened with - "not a wrong-results bug, just space and reads" - was measured and
//! is **false**. `SecondaryIndexScan` does re-check the value against the visible row:
//!
//! ```text
//! if vals[self.col_index] != sec { continue; }
//! ```
//!
//! so an entry whose row has moved on, or has been deleted, is correctly skipped. That much held. But
//! the re-check compares a value, and it cannot tell one entry from an identical one. `insert_entry`
//! appends at the binary-search position rather than overwriting, and no write path removed anything,
//! so two ways of arriving at the same `(value, key)` pair put two copies in the index and the scan
//! yielded a row per copy:
//!
//! - `UPDATE s SET v=999 WHERE id=4; UPDATE s SET v=40 WHERE id=4;` then a lookup for 40 returned
//!   `[[4, 40], [4, 40]]` - one row, reported twice, from write history alone.
//! - and since E63, `DELETE FROM s WHERE id=4; INSERT INTO s VALUES (4, 40);` did the same, on a
//!   separate code path, so fixing one would have left the other duplicating.
//!
//! Fixed by de-duplicating at both write sites rather than by deleting entries or de-duplicating in
//! the scan. **The old entry has to stay**: a secondary entry is how a reader finds a row by value,
//! and a transaction whose snapshot predates an update must still find the row under its old value -
//! the scan resolves the entry through the primary index and `resolve_visibility` returns the version
//! that reader can see. Removing it turns a stale answer into a lost row, which is worse. And
//! de-duplicating in the scan would hide the accounting while paying for it on every query.
//!
//! What remains, and is genuinely only cost: an entry per deleted row and per superseded value, never
//! reclaimed. Measured below - 25 entries for 15 live rows after 5 deletes and 5 updates.
//!
//! # An instrument note, because the obvious probe does not work
//!
//! Driving this from SQL does not reach `SecondaryIndexScan` at these sizes: `build_index_scan` costs
//! the index candidate against a filtered sequential scan and the sequential scan wins, correctly, up
//! to a few thousand rows. Measured through the CLI on a 400-row table with fresh `ANALYZE`, the plan
//! was `Sequential scan on s (rows=400 cost=8.00)`, so a probe that ran SQL and read the answers
//! would have been reporting on a heap filter while claiming to test an index. The plan is therefore
//! built directly, which is what the existing `test_index_scan_secondary_*` tests in `planner::plan`
//! do for the same reason.

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::lower;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::planner::physical_plan::PhysicalPlan;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::{ReadView, Snapshot, TxnManager};

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sec.db");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("sec.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: dir, catalog, bp, txn, session: Session::new() }
}

impl Db {
    fn sql(&mut self, sql: &str) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }

    /// Every entry in the table's one secondary index, dead ones included.
    fn secondary_entries(&self, table: &str) -> Vec<(Value, Value)> {
        let entry = self.catalog.get_table(table).expect("table");
        let info = entry.indexes.first().expect("the table has no secondary index");
        let tree =
            BPlusTreeManager::<(Value, Value), ()>::open(info.root_page_id, self.bp.clone());
        let scanner = tree.range_scan(Bound::Unbounded, Bound::Unbounded).expect("range scan");
        scanner.map(|r| r.expect("entry").0).collect()
    }

    fn live_rows(&mut self, table: &str) -> usize {
        let sql = format!("SELECT * FROM {table};");
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        match run(
            stmts.remove(0),
            &mut self.catalog,
            self.bp.clone(),
            self.txn.clone(),
            &mut self.session,
        )
        .unwrap()
        {
            ferrodb::execution::executor::Outcome::Rows(r) => r.len(),
            other => panic!("expected rows, got {:?}", std::mem::discriminant(&other)),
        }
    }

    /// Run a point lookup through `SecondaryIndexScan`, bypassing the cost model.
    fn secondary_lookup(&self, table: &str, v: i32) -> Vec<Vec<Value>> {
        let plan = PhysicalPlan::IndexScan {
            table: table.into(),
            column: 1,
            lower: Bound::Included(Value::Integer(v)),
            upper: Bound::Included(Value::Integer(v)),
        };
        let view = Arc::new(ReadView {
            snapshot: Snapshot { high_water: u64::MAX, active: HashSet::new() },
            txn_id: 0,
        });
        let mut exec = lower(plan, &self.catalog, self.bp.clone(), view).expect("lower the plan");
        let mut out = Vec::new();
        while let Some(r) = exec.next() {
            out.push(r.expect("scan").1);
        }
        out
    }
}

/// A table of `n` rows with a secondary index on `v`, built after the inserts so the index backfills.
fn seeded(n: i32) -> Db {
    let mut d = db();
    d.sql("CREATE TABLE s (id INTEGER NOT NULL, v INTEGER);");
    for i in 1..=n {
        d.sql(&format!("INSERT INTO s VALUES ({i}, {});", i * 10));
    }
    d.sql("CREATE INDEX si ON s (v);");
    d
}

/// **The measurement: how far the index drifts from the table.**
#[test]
fn a_secondary_index_grows_by_one_entry_per_delete_and_per_update() {
    let mut d = seeded(20);
    let baseline = d.secondary_entries("s").len();
    assert_eq!(baseline, 20, "the backfill did not index every row: {baseline}");

    // 5 deletes and 5 updates of the indexed column. Both are the operations under test; the
    // remaining 10 rows are untouched, so drift cannot be blamed on the whole table having moved.
    for i in 1..=5 {
        d.sql(&format!("DELETE FROM s WHERE id = {i};"));
    }
    for i in 6..=10 {
        d.sql(&format!("UPDATE s SET v = {} WHERE id = {i};", 100_000 + i));
    }

    let entries = d.secondary_entries("s").len();
    let live = d.live_rows("s");
    assert_eq!(live, 15, "the workload did not land: {live} live rows");

    // 20 backfilled + 5 new entries from the updates, and not one removal.
    assert_eq!(
        entries, 25,
        "expected 25 entries for 15 live rows - 20 backfilled, +5 from the updates, 0 removed. \
         Got {entries}, so the accounting in this file's header is wrong."
    );

    // Stated as the ratio an operator would care about, so the number is in the record rather than
    // implied by the arithmetic above.
    let dead = entries - live;
    assert_eq!(
        dead, 10,
        "10 dead entries for 15 live rows - 5 from deletes, 5 superseded by updates"
    );
}

/// **Not a wrong-results bug: the scan re-checks the value, so dead entries are invisible.**
#[test]
fn dead_entries_never_reach_the_caller() {
    let mut d = seeded(20);

    // id 3 is deleted; id 7's indexed value moves away. Both leave an entry behind.
    d.sql("DELETE FROM s WHERE id = 3;");
    d.sql("UPDATE s SET v = 777 WHERE id = 7;");

    let entries = d.secondary_entries("s");
    assert!(
        entries.contains(&(Value::Integer(30), Value::Integer(3))),
        "the deleted row's entry is gone, so this test no longer probes a dead entry: {entries:?}"
    );
    assert!(
        entries.contains(&(Value::Integer(70), Value::Integer(7))),
        "the superseded entry is gone, so this test no longer probes a stale entry"
    );

    // A lookup for the deleted row's value: the entry is there, the row is not visible.
    assert!(
        d.secondary_lookup("s", 30).is_empty(),
        "a deleted row was returned through its surviving index entry"
    );
    // A lookup for the superseded value: the entry is there, the row no longer holds that value.
    assert!(
        d.secondary_lookup("s", 70).is_empty(),
        "a row was returned for a value it no longer has - the scan is not re-checking the column"
    );
    // Anti-vacuity: the new value is found, so the scan is not simply returning nothing.
    let hit = d.secondary_lookup("s", 777);
    assert_eq!(
        hit,
        vec![vec![Value::Integer(7), Value::Integer(777)]],
        "the updated row is not reachable by its new value, so the two assertions above prove \
         nothing about filtering"
    );
    // And an untouched row still resolves, which rules out the index being broken generally.
    assert_eq!(d.secondary_lookup("s", 120), vec![vec![Value::Integer(12), Value::Integer(120)]]);
}

/// **The case the growth makes reachable: one row, two identical entries.**
///
/// `insert_entry` appends at the binary-search position rather than overwriting, and `UPDATE` only
/// ever inserts. So moving a value away and back gives the index two identical `(v, pk)` entries for
/// one row. If the scan returned a row per matching entry, a `SELECT` would report the same row
/// twice - a wrong answer produced by nothing but write history, and one no existing test covers,
/// since every other test in this file leaves at most one live entry per row.
#[test]
fn a_value_moved_away_and_back_does_not_return_its_row_twice() {
    let mut d = seeded(20);

    d.sql("UPDATE s SET v = 999 WHERE id = 4;");
    d.sql("UPDATE s SET v = 40 WHERE id = 4;");

    // The premise, in two parts, so a pass cannot come from the workload not having run.
    //
    // First: the away-and-back history really happened, evidenced by the entry the detour left
    // behind. Entries are never removed, so `(999, 4)` surviving is proof the value did move.
    let entries = d.secondary_entries("s");
    assert!(
        entries.contains(&(Value::Integer(999), Value::Integer(4))),
        "no entry from the intermediate value, so the value never moved and there was no way for a \
         duplicate to arise: {entries:?}"
    );
    // Second: the returned pair is present exactly once. This is the fixed state; before the guard
    // in `execution::update` it was 2, and the lookup below returned the row twice.
    let copies = entries.iter().filter(|e| **e == (Value::Integer(40), Value::Integer(4))).count();
    assert_eq!(
        copies, 1,
        "a value moved away and back left {copies} entries for one pair; `insert_entry` appends, so \
         each one yields a row and the lookup below duplicates the row: {entries:?}"
    );

    let hit = d.secondary_lookup("s", 40);
    assert_eq!(
        hit,
        vec![vec![Value::Integer(4), Value::Integer(40)]],
        "a row was returned once per index entry rather than once per row; a value moved away and \
         back duplicates it in every query that uses this index"
    );
}

/// The same duplicate, reached through E63's new path rather than through UPDATE.
///
/// Deleting a row leaves its `(v, pk)` entry behind. Since E63 the primary key can be used again, and
/// if the new row happens to carry the same indexed value the index gains a second identical entry -
/// no UPDATE involved. Worth its own test because the two write paths are separate code, and fixing
/// one would leave the other returning duplicates.
#[test]
fn reusing_a_key_with_the_same_indexed_value_does_not_duplicate_it() {
    let mut d = seeded(20);

    d.sql("DELETE FROM s WHERE id = 4;");
    d.sql("INSERT INTO s VALUES (4, 40);");

    let entries = d.secondary_entries("s");
    let n = entries.iter().filter(|e| **e == (Value::Integer(40), Value::Integer(4))).count();
    assert_eq!(n, 1, "the deleted row's entry was not reused, it was duplicated: {n} entries");

    assert_eq!(
        d.secondary_lookup("s", 40),
        vec![vec![Value::Integer(4), Value::Integer(40)]],
        "a reused primary key with an unchanged indexed value returns its row once per index entry"
    );
}
