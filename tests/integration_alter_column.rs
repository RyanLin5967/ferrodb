//! B11 — column-level schema evolution, end to end.
//!
//! # The gap this closes
//!
//! `SchemaChange` had exactly two variants, `CreateTable` and `DropTable`, both whole-table, and
//! `ALTER` was a word the parser refused by name. So the system had no representation at all for
//! the single most common real-world CDC break — a column added, renamed or retyped mid-stream —
//! and no representation for two agents evolving a schema concurrently, which is the
//! agent-isolation form of the same problem.
//!
//! # What is measured here rather than asserted
//!
//! Every claim in this file goes through the real path: the shipped SQL surface, the real WAL, the
//! real decoder, and — for the destination — the **Go consumer as a separate process**, read back
//! with the `sqlite3` CLI rather than with the driver that wrote it. E69's lesson is the reason:
//! the producer and the consumer had disagreed about a schema event's shape for four increments,
//! and neither side's own tests could see it, because each was validated against its own idea of
//! the format.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::{DataType, Value};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::write_feed;
use ferrodb::replication::logical::{ChangeOp, LogicalDecoder, SchemaChange};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::tel::merge::{ConflictKind, MergeOutcome};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn go_bin() -> String {
    for c in ["go", "/opt/homebrew/bin/go", "/usr/local/go/bin/go"] {
        if Command::new(c).arg("version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("Go is required to drive the independent CDC consumer");
}

fn sqlite_bin() -> String {
    for c in ["sqlite3", "/usr/bin/sqlite3", "/opt/homebrew/bin/sqlite3"] {
        if Command::new(c).arg("--version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("sqlite3 is required to read the destination independently of the driver that wrote it");
}

struct Db {
    dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    open_at(dir)
}

fn open_at(dir: tempfile::TempDir) -> Db {
    let path = dir.path().join("alter.db");
    let fresh = !path.exists();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog =
        if fresh { Catalog::create(bp.clone()).unwrap() } else { Catalog::open(bp.clone(), 1).unwrap() };
    let wal = Arc::new(WalManager::new(dir.path().join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { dir, catalog, wal, bp, txn, runtime, session }
}

impl Db {
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                p.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        let mut session = std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out = run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut session);
        self.session = session;
        out
    }

    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        match self.sql(sql) {
            Outcome::Rows(r) => r,
            other => panic!("`{sql}` did not return rows: {}", outcome_name(&other)),
        }
    }

    fn decode(&self) -> ferrodb::replication::logical::Decoded {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        LogicalDecoder::new(&self.catalog)
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode")
    }

    fn shape(&self, table: &str) -> Vec<(String, DataType)> {
        self.catalog
            .get_table(table)
            .unwrap_or_else(|| panic!("no table {table}"))
            .schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone()))
            .collect()
    }
}

/// `Outcome` has no `Debug`, so a panic message names the variant rather than dumping it.
fn outcome_name(o: &Outcome) -> &'static str {
    match o {
        Outcome::Rows(_) => "Rows",
        Outcome::Affected(_) => "Affected",
        Outcome::Explain(_) => "Explain",
        Outcome::Agent(_) => "Agent",
        Outcome::Ok => "Ok",
    }
}

/// The workload every feed test uses: rows on **both sides** of the alteration.
///
/// That is the breaking shape and it is the whole point of the fixture. A feed whose rows all
/// arrive before the ALTER passes with a sink that applies the DDL and then stops writing; one
/// whose rows all arrive after passes with a sink that drops and recreates the table. Only rows on
/// both sides distinguish "the destination evolved" from either failure.
fn mid_stream_workload(d: &mut Db) {
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");
    d.sql("INSERT INTO inv VALUES (2, 20);");
    d.sql("ALTER TABLE inv ADD COLUMN note VARCHAR(20);");
    d.sql("INSERT INTO inv VALUES (3, 30, 'after');");
}

// ---------------------------------------------------------------------------------------------
// The source itself
// ---------------------------------------------------------------------------------------------

/// **The rows written BEFORE the column existed must still be readable.**
///
/// This is what forces the heap rewrite, and it is the claim a metadata-only `ALTER` would fail.
/// A tuple is laid out positionally against the schema that wrote it and records nothing about its
/// own shape: a null bitmap sized `(ncols + 7) / 8` and then each column at an offset recomputed
/// from the schema on every read. A two-column row is therefore not a prefix of a three-column
/// one, and reading it under the wider schema walks off the end of its bytes.
///
/// Breaking shape: rows that predate the ALTER. A table altered while empty proves nothing.
#[test]
fn rows_written_before_the_column_are_still_readable_after_it() {
    let mut d = db();
    mid_stream_workload(&mut d);

    let rows = d.rows("SELECT * FROM inv;");
    assert_eq!(rows.len(), 3, "the ALTER lost rows: {rows:?}");
    assert_eq!(rows[0], vec![Value::Integer(1), Value::Integer(10), Value::Null]);
    assert_eq!(rows[1], vec![Value::Integer(2), Value::Integer(20), Value::Null]);
    assert_eq!(
        rows[2],
        vec![Value::Integer(3), Value::Integer(30), Value::Varchar("after".into())]
    );
}

/// The rewrite must not disturb the primary index, which maps a key to a **physical slot** the
/// rewrite can move.
///
/// Breaking shape, and every clause of it is load-bearing — a fire-check caught the first version
/// of this test proving nothing:
///
/// - **rows big enough to fill pages.** Growing a tuple that no longer fits its page makes
///   `HeapFileManager::update` delete the slot and re-insert elsewhere, which changes its
///   `RecordId`. `Page::update` also does not reclaim the bytes it grew out of, so a full page
///   spills after the first row on it grows, not after the last.
/// - **a query that actually uses the index.** With no statistics the cost model picks a
///   sequential scan, which finds the row whatever the index says — so the first version of this
///   test passed with the repointing deleted. `ANALYZE` before the alter and an `EXPLAIN`
///   assertion after it are what make the index the thing being measured.
#[test]
fn a_lookup_by_key_still_finds_a_row_the_rewrite_moved() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, pad VARCHAR(200));");
    let pad = "x".repeat(180);
    for i in 1..=200 {
        d.sql(&format!("INSERT INTO inv VALUES ({i}, '{pad}');"));
    }
    d.sql("ANALYZE inv;");
    d.sql("ALTER TABLE inv ADD COLUMN note VARCHAR(20);");

    let plan = explain(&mut d, "EXPLAIN SELECT id FROM inv WHERE id = 137;");
    assert!(
        plan.contains("Index scan"),
        "this test only measures the primary index if the plan uses it, and it does not:\n{plan}"
    );

    for i in [1, 17, 137, 200] {
        let rows = d.rows(&format!("SELECT id FROM inv WHERE id = {i};"));
        assert_eq!(rows.len(), 1, "row {i} is unreachable by key after the rewrite: {rows:?}");
        assert_eq!(rows[0][0], Value::Integer(i));
    }
    let all = d.rows("SELECT id FROM inv;");
    assert_eq!(all.len(), 200, "the rewrite lost rows: {}", all.len());
}

/// `EXPLAIN` text, so a test can prove which plan it is actually measuring.
fn explain(d: &mut Db, sql: &str) -> String {
    match d.sql(sql) {
        Outcome::Explain(t) => t,
        other => panic!("`{sql}` did not explain: {}", outcome_name(&other)),
    }
}

/// **A secondary index over a retyped column keeps answering, and this test pins the reason.**
///
/// The reason is not that anything rebuilds the index — nothing does, deliberately. It is that
/// `Value::cmp` compares the whole numeric band by VALUE rather than by type rank, so the
/// `Integer` key an entry was written with is *equal* to the `BigInt` the column became, and the
/// tree stays ordered across the change. Every conversion in the widening allowlist stays inside
/// that band or leaves the type alone.
///
/// This is worth a test of its own precisely because it is load-bearing and invisible: an earlier
/// version of `alter_table` rebuilt the index on the opposite belief, doing unnecessary work and
/// silently discarding the historical `(old value, key)` entries E66 keeps on purpose. If
/// `Value::cmp` ever stopped comparing across the numeric band, nothing else in the suite would
/// notice that every pre-retype index entry had become unreachable.
///
/// Breaking shape: entries written BEFORE the retype, looked up by the NEW type. Measured against
/// the index structure rather than through a query, because this cost model does not choose a
/// secondary index for a table this size — measured, before and after the alter — so a
/// query-level assertion would fall back to a sequential scan and prove only that the rows exist.
#[test]
fn a_secondary_index_over_a_retyped_column_still_answers() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER, pad VARCHAR(200));");
    let pad = "x".repeat(180);
    for i in 1..=200 {
        d.sql(&format!("INSERT INTO inv VALUES ({i}, {}, '{pad}');", i * 10));
    }
    d.sql("CREATE INDEX ix ON inv (qty);");
    d.sql("ANALYZE inv;");
    d.sql("ALTER TABLE inv ALTER COLUMN qty TYPE BIGINT;");

    let root = d.catalog.get_table("inv").unwrap().indexes[0].root_page_id;
    let ix = BPlusTreeManager::<(Value, Value), ()>::open(root, d.bp.clone());
    assert!(
        ix.search(&(Value::BigInt(1370), Value::Integer(137))).unwrap().is_some(),
        "an entry written before the retype is unreachable by the column's new type"
    );

    // A row inserted AFTER the retype writes a BIGINT key into the same tree, and both are
    // findable — which is the whole claim: one tree, two spellings of the same values, correctly
    // ordered.
    d.sql(&format!("INSERT INTO inv VALUES (201, 7, '{pad}');"));
    let root = d.catalog.get_table("inv").unwrap().indexes[0].root_page_id;
    let ix = BPlusTreeManager::<(Value, Value), ()>::open(root, d.bp.clone());
    assert!(
        ix.search(&(Value::BigInt(7), Value::Integer(201))).unwrap().is_some(),
        "an entry written after the retype is unreachable"
    );
    assert!(
        ix.search(&(Value::BigInt(1370), Value::Integer(137))).unwrap().is_some(),
        "writing a new-typed key made the old-typed entries unreachable"
    );

    // The rows themselves are intact and hold the new type.
    let rows = d.rows("SELECT id, qty FROM inv WHERE qty = 1370;");
    assert_eq!(rows.len(), 1, "the retype lost the row: {rows:?}");
    assert_eq!(rows[0], vec![Value::Integer(137), Value::BigInt(1370)]);

    // And the widening is real: a value no INTEGER could hold now stores and reads back.
    d.sql(&format!("INSERT INTO inv VALUES (202, 9223372036854775807, '{pad}');"));
    let wide = d.rows("SELECT qty FROM inv WHERE id = 202;");
    assert_eq!(wide[0][0], Value::BigInt(i64::MAX));
}

/// A table's statistics must survive an alteration, or every `ALTER` silently de-optimises every
/// query against the table until somebody runs `ANALYZE`.
///
/// Breaking shape: a plan that used an index before the alter and stops using it after. That is
/// exactly how two of the tests above first passed with their guards deleted.
#[test]
fn statistics_survive_an_alteration_exactly() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER, pad VARCHAR(200));");
    let pad = "x".repeat(180);
    for i in 1..=200 {
        d.sql(&format!("INSERT INTO inv VALUES ({i}, {}, '{pad}');", i * 10));
    }
    d.sql("ANALYZE inv;");
    let before = explain(&mut d, "EXPLAIN SELECT id FROM inv WHERE id = 137;");
    assert!(before.contains("Index scan"), "the fixture does not use an index to begin with:\n{before}");

    d.sql("ALTER TABLE inv ADD COLUMN note VARCHAR(20);");
    let after = explain(&mut d, "EXPLAIN SELECT id FROM inv WHERE id = 137;");
    assert!(after.contains("Index scan"), "the ALTER de-optimised the plan:\n{after}");

    // The added column's statistics are the exact ones ANALYZE would compute for an all-null
    // column, not a guess and not an absence.
    let stats = d.catalog.stats.get("inv").expect("the ALTER dropped the statistics");
    assert_eq!(stats.row_count, 200);
    assert_eq!(stats.columns.len(), 4, "the statistics are not one per column");
    assert_eq!(stats.columns[3].nulls, 200, "the added column is NULL in every existing row");
    assert_eq!(stats.columns[3].distinct, 0);
    assert!(stats.columns[3].min.is_none() && stats.columns[3].max.is_none());

    // A retype carries min/max through the same widening the rows went through.
    d.sql("ALTER TABLE inv ALTER COLUMN qty TYPE BIGINT;");

    // **"Carried" has to mean "equal to what ANALYZE would compute", and it is compared by Debug
    // rendering rather than by `==`.**
    //
    // `Value`'s equality is `cmp`, which compares the whole numeric band by VALUE — so
    // `Integer(10)` and `BigInt(10)` are equal, and an assertion written with `assert_eq!` cannot
    // tell a statistic that was converted from one that was left in the old type. A fire-check
    // caught precisely that: deleting the min/max conversion left this test green.
    let carried = format!("{:?}", d.catalog.stats.get("inv").expect("the retype dropped the stats"));
    d.sql("ANALYZE inv;");
    let recomputed = format!("{:?}", d.catalog.stats.get("inv").unwrap());
    assert_eq!(
        carried, recomputed,
        "the statistics carried across the ALTER are not the ones ANALYZE computes for the \
         altered table"
    );
    assert!(carried.contains("BigInt(2000)"), "the retyped column's bounds kept the old type: {carried}");
}

/// A rename must move the name everywhere it is recorded, and an index records it BY NAME
/// (`IndexInfo.column_name`), re-resolved to an ordinal on every statement. Miss it and the index
/// is not stale, it is unfindable — the planner's lookup fails and every query against the table
/// stops working.
///
/// Breaking shape: an indexed column, renamed, then the table queried at all.
#[test]
fn renaming_an_indexed_column_leaves_the_table_queryable() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");
    d.sql("CREATE INDEX ix ON inv (qty);");
    d.sql("ALTER TABLE inv RENAME COLUMN qty TO quantity;");

    let rows = d.rows("SELECT id, quantity FROM inv WHERE quantity = 10;");
    assert_eq!(rows.len(), 1, "the renamed column is unreachable: {rows:?}");
    assert_eq!(rows[0], vec![Value::Integer(1), Value::Integer(10)]);

    // **A WRITE is what proves the index metadata followed the rename**, and a read is not.
    // `open_table` resolves `IndexInfo.column_name` to an ordinal with `position()` on every
    // write and turns a miss into `KeyNotFound`; the read path merely declines to use an index it
    // cannot resolve and falls back to a scan, which succeeds either way. A fire-check caught this
    // test passing with the rename of `IndexInfo.column_name` deleted.
    d.sql("UPDATE inv SET quantity = 11 WHERE id = 1;");
    assert_eq!(d.rows("SELECT quantity FROM inv WHERE id = 1;")[0][0], Value::Integer(11));
    d.sql("INSERT INTO inv VALUES (2, 20);");
    d.sql("DELETE FROM inv WHERE id = 2;");
    assert_eq!(
        d.shape("inv"),
        vec![("id".into(), DataType::Integer), ("quantity".into(), DataType::Integer)]
    );
}

// ---------------------------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------------------------

/// **A deleted row must stay deleted across the rewrite.**
///
/// A tombstone is a version with a non-zero `end_ts` sitting in the main heap — `DELETE` stamps the
/// live version rather than removing it — and the rewrite scans every slot, tombstones included.
/// `Tuple::serialize` writes a fresh version header, so the rewrite has to carry `begin_ts` **and**
/// `end_ts` across from the old bytes. Carry only `begin_ts` and every row the table has ever
/// deleted comes back to life at the next `ALTER`.
///
/// Breaking shape: a row deleted BEFORE the alter. A table whose rows are all live proves nothing.
#[test]
fn deleted_rows_stay_deleted_across_a_rewrite() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");
    d.sql("INSERT INTO inv VALUES (2, 20);");
    d.sql("INSERT INTO inv VALUES (3, 30);");
    d.sql("DELETE FROM inv WHERE id = 2;");
    assert_eq!(d.rows("SELECT id FROM inv;").len(), 2, "the fixture did not delete anything");

    d.sql("ALTER TABLE inv ADD COLUMN note VARCHAR(20);");

    let ids: Vec<Value> = d.rows("SELECT id FROM inv;").into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        ids,
        vec![Value::Integer(1), Value::Integer(3)],
        "the rewrite resurrected a deleted row"
    );
}

/// **Exit criterion: the schema survives a restart.**
///
/// A checkpoint truncates the log, so the only thing a reader starting at the new base can learn a
/// table's shape from is the *retained* schema declaration, which `replay_schema` re-appends. An
/// `ALTER` that was merely logged would be truncated away and the log would go on re-declaring the
/// table's OLD shape forever — a consumer restarting after any checkpoint would rebuild the
/// pre-alter table and never be told otherwise.
///
/// Breaking shape: a checkpoint AFTER the alter. Without one the alter record is still in the log
/// and the test passes for the wrong reason.
#[test]
fn the_altered_shape_survives_a_truncation_and_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dir = {
        let mut d = open_at(dir);
        mid_stream_workload(&mut d);
        let root = d.catalog.get_table("inv").unwrap().first_directory_page_id;

        // The retained declaration already carries the new column.
        let retained = d.txn.retained_shape(root).expect("no retained declaration for inv");
        assert_eq!(
            retained.iter().map(|(n, _, _)| n.as_str()).collect::<Vec<_>>(),
            vec!["id", "qty", "note"],
            "the retained declaration is still the pre-alter shape"
        );

        // Force a truncation, which discards every record including the ALTER itself.
        d.txn.checkpoint().expect("checkpoint");
        let after = d.decode();
        let declared: Vec<&SchemaChange> = after
            .schema_changes
            .iter()
            .filter(|(_, t, _)| t == "inv")
            .map(|(_, _, c)| c)
            .collect();
        assert!(
            declared.iter().all(|c| **c == SchemaChange::CreateTable),
            "an ALTER was re-emitted after truncation; a consumer would apply it twice: {declared:?}"
        );
        let shape = after
            .events
            .iter()
            .find_map(|e| match &e.op {
                ChangeOp::Schema { change: SchemaChange::CreateTable, columns } if e.table == "inv" => {
                    Some(columns.clone())
                }
                _ => None,
            })
            .expect("the truncated log re-declares nothing for inv");
        assert_eq!(
            shape.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["id", "qty", "note"],
            "the log re-declares the PRE-alter shape after truncation"
        );
        d.bp.flush_all().unwrap();
        d.dir
    };

    // A whole new process's worth of state: fresh buffer pool, catalog re-read from its pages.
    let mut reopened = open_at(dir);
    assert_eq!(
        reopened.shape("inv"),
        vec![
            ("id".into(), DataType::Integer),
            ("qty".into(), DataType::Integer),
            ("note".into(), DataType::Varchar(20))
        ],
        "the catalog came back without the added column"
    );
    let rows = reopened.rows("SELECT * FROM inv;");
    assert_eq!(rows.len(), 3, "the rows did not survive the reopen: {rows:?}");
    assert_eq!(rows[2][2], Value::Varchar("after".into()));
    // And the reopened database can still write the new shape.
    reopened.sql("INSERT INTO inv VALUES (4, 40, 'later');");
    assert_eq!(reopened.rows("SELECT * FROM inv;").len(), 4);
}

// ---------------------------------------------------------------------------------------------
// The feed, and a real consumer
// ---------------------------------------------------------------------------------------------

/// The event reaches the feed in band and in log order, with both halves of the contract.
#[test]
fn the_feed_carries_the_column_change_in_log_order() {
    let mut d = db();
    mid_stream_workload(&mut d);
    let out = d.decode();

    let names: Vec<&str> = out.events.iter().map(|e| e.op.name()).collect();
    let alter_at = names
        .iter()
        .position(|n| *n == "ADD_COLUMN")
        .unwrap_or_else(|| panic!("no ADD_COLUMN in the feed: {names:?}"));
    let last_insert = names.iter().rposition(|n| *n == "INSERT").unwrap();
    assert!(
        alter_at < last_insert,
        "the ALTER did not arrive before the row that uses the new column: {names:?}"
    );
    // Rows on both sides, decoded against the shape in force where they sit.
    let before: Vec<&ChangeOp> = out.events[..alter_at].iter().map(|e| &e.op).collect();
    assert!(
        before.iter().any(|op| matches!(op, ChangeOp::Insert { new } if new.len() == 2)),
        "a row from before the ALTER was not decoded against the two-column shape"
    );
    assert!(
        out.events[alter_at..]
            .iter()
            .any(|e| matches!(&e.op, ChangeOp::Insert { new } if new.len() == 3)),
        "a row from after the ALTER was not decoded against the three-column shape"
    );

    match &out.events[alter_at].op {
        ChangeOp::Schema { change, columns } => {
            assert_eq!(*change, SchemaChange::AddColumn { column: "note".into() });
            assert_eq!(
                columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
                vec!["id", "qty", "note"],
                "the event does not carry the full shape a sink reconciles against"
            );
            assert_eq!(columns[2].sql_type, "VARCHAR(20)");
        }
        other => panic!("not a schema event: {other:?}"),
    }
}

fn feed_file(dir: &Path, d: &Db) -> PathBuf {
    let out = d.decode();
    let path = dir.join("feed.jsonl");
    let mut buf: Vec<u8> = Vec::new();
    let n = write_feed(&out.events, &mut buf).expect("write feed");
    assert!(n > 0, "the feed is empty; everything downstream would be vacuous");
    std::fs::write(&path, &buf).unwrap();
    path
}

fn go(args: &[&str], feed: &Path, extra: &[&str]) -> std::process::Output {
    Command::new(go_bin())
        .current_dir("cdc-consumer")
        .args(["run", "."])
        .args(args)
        .arg(feed)
        .args(extra)
        .output()
        .expect("run the Go consumer")
}

fn query(db_path: &Path, sql: &str) -> String {
    let out = Command::new(sqlite_bin()).arg(db_path).arg(sql).output().expect("run sqlite3");
    assert!(out.status.success(), "sqlite3 failed: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n").trim().to_string()
}

/// **THE exit criterion: a column added mid-stream reaches the destination and the sink keeps
/// landing rows.**
///
/// Driven through the Go binary in a separate process and read back with the `sqlite3` CLI, so
/// neither side of the claim is made by the code that produced it.
#[test]
fn a_column_added_mid_stream_reaches_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    let mut d = db();
    mid_stream_workload(&mut d);
    let feed = feed_file(dir.path(), &d);

    // The independent validator first: an op it has not been taught refuses the whole feed, which
    // is exactly what happened to `CREATE_TABLE` the day the producer started emitting one.
    let v = go(&["validate"], &feed, &[]);
    assert!(
        v.status.success(),
        "the independent validator refused the feed:\n{}",
        String::from_utf8_lossy(&v.stderr)
    );

    let dest = dir.path().join("out.sqlite");
    let s = go(&["sink"], &feed, &["-db", dest.to_str().unwrap(), "-key", "id"]);
    assert!(
        s.status.success(),
        "the sink failed on a feed containing an ADD_COLUMN:\n{}\n{}",
        String::from_utf8_lossy(&s.stderr),
        String::from_utf8_lossy(&s.stdout)
    );

    // The destination has the column...
    let cols = query(&dest, "SELECT name FROM pragma_table_info('inv') ORDER BY cid;");
    assert!(cols.split('\n').any(|c| c == "note"), "the destination never gained the column: {cols}");
    // ...the rows from before it are there with it null...
    let landed = query(&dest, "SELECT id, qty, COALESCE(note,'<null>') FROM inv ORDER BY id;");
    assert_eq!(
        landed, "1|10|<null>\n2|20|<null>\n3|30|after",
        "the destination is not what the source holds"
    );
    // ...and "keeps landing rows" is the third line: a row written AFTER the ALTER, with a value in
    // the column the ALTER created.
}

/// The other two alterations reach a real destination too, and a rename must move the column
/// rather than dropping one and adding another — which is what a sink reconciling from the shape
/// alone would do, and which loses the column's data.
#[test]
fn a_rename_and_a_retype_reach_the_destination_without_losing_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");
    d.sql("INSERT INTO inv VALUES (2, 20);");
    d.sql("ALTER TABLE inv RENAME COLUMN qty TO quantity;");
    d.sql("INSERT INTO inv VALUES (3, 30);");
    d.sql("ALTER TABLE inv ALTER COLUMN quantity TYPE BIGINT;");
    d.sql("INSERT INTO inv VALUES (4, 9223372036854775807);");

    let feed = feed_file(dir.path(), &d);
    let v = go(&["validate"], &feed, &[]);
    assert!(v.status.success(), "validator: {}", String::from_utf8_lossy(&v.stderr));

    let dest = dir.path().join("out.sqlite");
    let s = go(&["sink"], &feed, &["-db", dest.to_str().unwrap(), "-key", "id"]);
    assert!(
        s.status.success(),
        "the sink failed:\n{}\n{}",
        String::from_utf8_lossy(&s.stderr),
        String::from_utf8_lossy(&s.stdout)
    );

    // The rename moved the data rather than dropping it: rows 1 and 2 were written under the OLD
    // name and their values are under the new one.
    let landed = query(&dest, "SELECT id, quantity FROM inv ORDER BY id;");
    assert_eq!(
        landed, "1|10\n2|20\n3|30\n4|9223372036854775807",
        "the destination lost or mis-keyed data across the rename and retype"
    );
}

// ---------------------------------------------------------------------------------------------
// Refusals. Each one has an allowed case beside it.
// ---------------------------------------------------------------------------------------------

fn refuses(d: &mut Db, sql: &str, needle: &str) {
    let err = d
        .try_sql(sql)
        .err()
        .unwrap_or_else(|| panic!("`{sql}` was accepted; it must be refused"));
    let text = format!("{err}");
    assert!(text.contains(needle), "`{sql}` was refused, but not by this guard: {text}");
}

/// Every refusal, each with the case that is **allowed** beside it. A guard that refused
/// everything would pass the first half of each pair and break the database.
#[test]
fn the_refusals_refuse_and_the_permitted_cases_are_permitted() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER, name VARCHAR(4));");
    d.sql("INSERT INTO inv VALUES (1, 10, 'ab');");

    // NOT NULL with no DEFAULT: every existing row would violate it on arrival.
    refuses(&mut d, "ALTER TABLE inv ADD COLUMN c INTEGER NOT NULL;", "NOT NULL");
    d.sql("ALTER TABLE inv ADD COLUMN c INTEGER;");

    // A name already in use.
    refuses(&mut d, "ALTER TABLE inv ADD COLUMN qty INTEGER;", "already has a column");

    // The primary key's type is the primary index's key. Retyping a non-key column is fine.
    refuses(&mut d, "ALTER TABLE inv ALTER COLUMN id TYPE BIGINT;", "primary key");
    d.sql("ALTER TABLE inv ALTER COLUMN qty TYPE BIGINT;");

    // Narrowing, and any conversion outside the allowlist.
    refuses(&mut d, "ALTER TABLE inv ALTER COLUMN qty TYPE INTEGER;", "prove total");
    refuses(&mut d, "ALTER TABLE inv ALTER COLUMN name TYPE VARCHAR(2);", "prove total");
    refuses(&mut d, "ALTER TABLE inv ALTER COLUMN name TYPE INTEGER;", "prove total");
    d.sql("ALTER TABLE inv ALTER COLUMN name TYPE VARCHAR(40);");

    // A no-op alter would still be published as though something changed.
    refuses(&mut d, "ALTER TABLE inv ALTER COLUMN qty TYPE BIGINT;", "already");

    // Columns that do not exist.
    refuses(&mut d, "ALTER TABLE inv RENAME COLUMN nosuch TO x;", "no column 'nosuch'");
    refuses(&mut d, "ALTER TABLE inv RENAME COLUMN qty TO id;", "already has a column");
    d.sql("ALTER TABLE inv RENAME COLUMN qty TO quantity;");

    // And the table has to exist at all.
    refuses(&mut d, "ALTER TABLE nosuch ADD COLUMN c INTEGER;", "unknown table");

    // The whole sequence above actually took effect, so none of the permitted cases was a no-op.
    assert_eq!(
        d.shape("inv"),
        vec![
            ("id".into(), DataType::Integer),
            ("quantity".into(), DataType::BigInt),
            ("name".into(), DataType::Varchar(40)),
            ("c".into(), DataType::Integer),
        ]
    );
    assert_eq!(
        d.rows("SELECT * FROM inv;")[0],
        vec![
            Value::Integer(1),
            Value::BigInt(10),
            Value::Varchar("ab".into()),
            Value::Null
        ]
    );
}

/// The rewrite reads and writes every tuple of a table in place. A reader mid-flight would be
/// handed rows in a shape its plan was built against and no longer matches, and the rewrite
/// truncates version chains on the assumption that no snapshot older than it can exist.
///
/// Breaking shape: an open transaction on another connection, not on this one — the in-transaction
/// check `session.current.is_some()` cannot see that.
#[test]
fn an_alter_is_refused_while_any_transaction_is_open() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");

    let other = d.txn.begin().expect("begin");
    refuses(&mut d, "ALTER TABLE inv ADD COLUMN note VARCHAR(4);", "cannot run while a transaction is open");
    d.txn.commit(other).expect("commit");

    // Anti-vacuity: with nothing in flight the same statement is accepted.
    d.sql("ALTER TABLE inv ADD COLUMN note VARCHAR(4);");
    assert_eq!(d.shape("inv").len(), 3);
}

// ---------------------------------------------------------------------------------------------
// Two agents evolving one schema
// ---------------------------------------------------------------------------------------------

fn merge_report(out: Outcome) -> ferrodb::agent_sql::MergeReport {
    match out {
        Outcome::Agent(AgentOutput::Merge(r)) => r,
        other => panic!("not a merge report: {}", outcome_name(&other)),
    }
}

/// **Exit criterion, first half: two branches adding different columns merge.**
///
/// Breaking shape: the second agent forks BEFORE the first one merges, so its edit is checked
/// against a shape that has since moved. An implementation that validated an edit against the
/// branch's fork point would let both through — and would let two adds of the *same* column
/// through as well, leaving a table with two columns of one name.
#[test]
fn two_branches_adding_different_columns_compose() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");

    let mut a = Session::with_runtime(d.runtime.clone());
    let mut b = Session::with_runtime(d.runtime.clone());
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-a';", &mut a);
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-b';", &mut b);

    // Both fork first, so neither saw the other's change.
    exec(&mut d, "ALTER TABLE inv ADD COLUMN note VARCHAR(20);", &mut a);
    exec(&mut d, "ALTER TABLE inv ADD COLUMN sku VARCHAR(8);", &mut b);

    let ra = merge_report(exec(&mut d, "MERGE;", &mut a));
    assert!(ra.applied_to_target, "the first agent's schema change did not land: {ra}");
    assert_eq!(d.shape("inv").len(), 3, "the first add did not reach the shared catalog");

    let rb = merge_report(exec(&mut d, "MERGE;", &mut b));
    assert!(rb.applied_to_target, "the second agent's schema change did not land: {rb}");
    assert_eq!(
        rb.outcome.name(),
        "Commuting",
        "both sides changed the shape and it composed, which is not Clean: {rb}"
    );
    assert_eq!(
        d.shape("inv").iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["id", "qty", "note", "sku"],
        "the two adds did not compose"
    );
    // And the row that predates both is still there, now with two null columns.
    assert_eq!(
        d.rows("SELECT * FROM inv;")[0],
        vec![Value::Integer(1), Value::Integer(10), Value::Null, Value::Null]
    );
}

/// **Exit criterion, second half: two branches retyping one column conflict, with the violated
/// predicate returned.**
///
/// Breaking shape: two *different* target types. Identical retypes are not contradictory — the
/// second agent's intent is already satisfied — and refusing those would force a retry with nothing
/// to do, which is why the companion case below is part of the same claim.
#[test]
fn two_branches_retyping_one_column_conflict_with_the_predicate() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");

    let mut a = Session::with_runtime(d.runtime.clone());
    let mut b = Session::with_runtime(d.runtime.clone());
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-a';", &mut a);
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-b';", &mut b);
    exec(&mut d, "ALTER TABLE inv ALTER COLUMN qty TYPE BIGINT;", &mut a);
    exec(&mut d, "ALTER TABLE inv ALTER COLUMN qty TYPE DECIMAL;", &mut b);

    let ra = merge_report(exec(&mut d, "MERGE;", &mut a));
    assert!(ra.applied_to_target, "{ra}");
    assert_eq!(d.shape("inv")[1].1, DataType::BigInt);

    let rb = merge_report(exec(&mut d, "MERGE;", &mut b));
    assert!(!rb.applied_to_target, "a contradictory retype was published: {rb}");
    assert!(rb.outcome.is_conflict(), "{rb}");
    assert_eq!(rb.outcome.conflicts()[0].kind, ConflictKind::SchemaMismatch);

    // Exit criterion 7's shape: the predicate itself, not a boolean.
    let predicate = rb.outcome.conflicts()[0]
        .violated_guard
        .as_ref()
        .expect("no predicate handed back")
        .violated_predicate();
    assert_eq!(predicate, "typeof(inv.qty) = INTEGER");
    assert!(
        rb.outcome.conflicts()[0].detail.contains("BIGINT"),
        "the conflict does not say what it found: {}",
        rb.outcome.conflicts()[0].detail
    );

    // Target untouched by the loser.
    assert_eq!(d.shape("inv")[1].1, DataType::BigInt);
}

/// The anti-vacuity companion: two branches making the **same** change do not conflict. Without
/// this, a schema merge that refused every second edit would pass the test above.
#[test]
fn two_branches_making_the_same_change_do_not_conflict() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");

    let mut a = Session::with_runtime(d.runtime.clone());
    let mut b = Session::with_runtime(d.runtime.clone());
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-a';", &mut a);
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-b';", &mut b);
    exec(&mut d, "ALTER TABLE inv ADD COLUMN note VARCHAR(20);", &mut a);
    exec(&mut d, "ALTER TABLE inv ADD COLUMN note VARCHAR(20);", &mut b);

    assert!(merge_report(exec(&mut d, "MERGE;", &mut a)).applied_to_target);
    let rb = merge_report(exec(&mut d, "MERGE;", &mut b));
    assert!(rb.applied_to_target, "an identical schema change was refused: {rb}");
    assert_eq!(
        d.shape("inv").iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["id", "qty", "note"],
        "the absorbed edit was applied twice"
    );
}

/// A branch that adds a column and a branch that only writes rows must both land — and the row
/// branch's rows were written one value short of the shape they end up in.
///
/// Breaking shape: agent B forks, writes a row, and merges AFTER agent A widened the table. Without
/// conforming the row on the way out, publishing it fails from inside `Tuple::serialize` with a
/// message about value counts.
#[test]
fn a_row_written_before_a_sibling_widened_the_table_still_publishes() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");

    let mut a = Session::with_runtime(d.runtime.clone());
    let mut b = Session::with_runtime(d.runtime.clone());
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-a';", &mut a);
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-b';", &mut b);
    // An INSERT, not an UPDATE, and that is the whole breaking shape. An updated row already
    // exists on the target, so the merge starts from the target's image and is the right width by
    // accident; a NEW row exists only on the branch, in the branch's fork-point shape, and is the
    // image that has to be conformed on the way out. A fire-check caught the first version of this
    // test — written with an UPDATE — passing with the conforming deleted.
    exec(&mut d, "INSERT INTO inv VALUES (2, 22);", &mut b);
    exec(&mut d, "UPDATE inv SET qty = 99 WHERE id = 1;", &mut b);
    exec(&mut d, "ALTER TABLE inv ADD COLUMN note VARCHAR(20);", &mut a);

    assert!(merge_report(exec(&mut d, "MERGE;", &mut a)).applied_to_target);
    let rb = merge_report(exec(&mut d, "MERGE;", &mut b));
    assert!(rb.applied_to_target, "the row branch could not publish into the widened table: {rb}");
    let rows = d.rows("SELECT * FROM inv;");
    assert_eq!(
        rows[0],
        vec![Value::Integer(1), Value::Integer(99), Value::Null],
        "the updated row did not land in the widened shape"
    );
    assert_eq!(
        rows[1],
        vec![Value::Integer(2), Value::Integer(22), Value::Null],
        "the inserted row did not land in the widened shape"
    );
}

/// A branch's schema edit is **pending**, not applied: it must be invisible to everyone else until
/// `MERGE`, and abandoning the branch must take it away.
#[test]
fn a_branchs_schema_change_is_invisible_until_it_merges() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");

    let mut a = Session::with_runtime(d.runtime.clone());
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-a';", &mut a);
    exec(&mut d, "ALTER TABLE inv ADD COLUMN note VARCHAR(20);", &mut a);
    assert_eq!(d.shape("inv").len(), 2, "a branch's ALTER reached the shared catalog immediately");

    exec(&mut d, "ABANDON;", &mut a);
    assert_eq!(d.shape("inv").len(), 2, "an abandoned branch's ALTER survived it");

    // Anti-vacuity: the same edit on a branch that merges does reach the shared catalog.
    let mut b = Session::with_runtime(d.runtime.clone());
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-b';", &mut b);
    exec(&mut d, "ALTER TABLE inv ADD COLUMN note VARCHAR(20);", &mut b);
    assert!(merge_report(exec(&mut d, "MERGE;", &mut b)).applied_to_target);
    assert_eq!(d.shape("inv").len(), 3);
}

/// A column an agent added must reach the change feed by the same path a column a human added
/// takes — one producer of schema events, not two.
#[test]
fn a_column_an_agent_added_reaches_the_feed() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");

    let mut a = Session::with_runtime(d.runtime.clone());
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-a';", &mut a);
    exec(&mut d, "ALTER TABLE inv ADD COLUMN note VARCHAR(20);", &mut a);
    assert!(merge_report(exec(&mut d, "MERGE;", &mut a)).applied_to_target);

    let out = d.decode();
    let found = out.events.iter().any(|e| {
        e.table == "inv"
            && matches!(&e.op, ChangeOp::Schema { change, .. }
                        if *change == SchemaChange::AddColumn { column: "note".into() })
    });
    assert!(
        found,
        "the agent's column never reached the feed: {:?}",
        out.events.iter().map(|e| e.op.name()).collect::<Vec<_>>()
    );
}

fn exec(d: &mut Db, sql: &str, session: &mut Session) -> Outcome {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let mut stmts = p.parse();
    assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
    run(stmts.remove(0), &mut d.catalog, d.bp.clone(), d.txn.clone(), session)
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
}

/// Guard against the merge outcome being reported without its schema half.
#[test]
fn a_merge_report_names_the_shape_it_produced() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    let mut a = Session::with_runtime(d.runtime.clone());
    exec(&mut d, "BEGIN AGENT SESSION AS 'agent-a';", &mut a);
    exec(&mut d, "ALTER TABLE inv ADD COLUMN note VARCHAR(20);", &mut a);
    let r = merge_report(exec(&mut d, "MERGE;", &mut a));
    let text = format!("{r}");
    assert!(text.contains("schema inv"), "the report does not mention the shape: {text}");
    assert!(text.contains("note"), "the report does not name the new column: {text}");
    assert!(matches!(r.outcome, MergeOutcome::Clean), "a one-sided change is Clean: {r}");
}
