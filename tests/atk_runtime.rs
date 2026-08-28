//! Attack probes on the RUNTIME egress paths: snapshot, hand-built shapes, DDL reuse.
use std::path::Path;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::{to_json_line, write_table_json};
use ferrodb::replication::logical::{ChangeEvent, ChangeOp, ColumnSpec, LogicalDecoder, SchemaChange};
use ferrodb::replication::publication::Publication;
use ferrodb::replication::snapshot::{snapshot_table, snapshot_table_exact};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn policy() -> Publication {
    Publication::parse("publication analytics\npatients: id, name\n").unwrap()
}

fn wal(dir: &Path, tag: &str) -> Arc<WalManager> {
    Arc::new(WalManager::new(dir.join(format!("{tag}.wal"))).unwrap())
}

// ---- 1. the SNAPSHOT path ---------------------------------------------------------------------

#[test]
fn snapshot_probes() {
    let dir = tempfile::tempdir().unwrap();
    let w = wal(dir.path(), "snap");

    // (a) a denied column in a normal backfill
    let mut buf = Vec::new();
    let r = snapshot_table("patients", &w, &policy(), &mut buf, || {
        Ok((
            vec!["id".into(), "name".into(), "ssn".into()],
            vec![vec![Value::Integer(1), Value::Varchar("ada".into()), Value::Varchar("000-11-2222".into())]],
        ))
    });
    println!("[snap-denied-col] {:?}\n  bytes = {}", r.map(|s| s.rows), String::from_utf8_lossy(&buf));

    // (b) MORE values than column names
    let mut buf = Vec::new();
    let r = snapshot_table("patients", &w, &policy(), &mut buf, || {
        Ok((
            vec!["id".into(), "name".into()],
            vec![vec![Value::Integer(1), Value::Varchar("ada".into()), Value::Varchar("000-11-2222".into())]],
        ))
    });
    println!("[snap-surplus-value] {:?}\n  bytes = {}", r.map(|s| s.rows), String::from_utf8_lossy(&buf));

    // (c) FEWER values than column names
    let mut buf = Vec::new();
    let r = snapshot_table("patients", &w, &policy(), &mut buf, || {
        Ok((
            vec!["id".into(), "name".into(), "ssn".into()],
            vec![vec![Value::Integer(1)]],
        ))
    });
    println!("[snap-short-row] {:?}\n  bytes = {}", r.map(|s| s.rows), String::from_utf8_lossy(&buf));

    // (d) undecided table
    let mut buf = Vec::new();
    let r = snapshot_table("secrets", &w, &policy(), &mut buf, || {
        Ok((vec!["id".into(), "ssn".into()], vec![vec![Value::Integer(1), Value::Varchar("000-11-2222".into())]]))
    });
    println!("[snap-undecided-table] err={:?}\n  bytes = {:?}", r.err().map(|e| e.to_string()), String::from_utf8_lossy(&buf));

    // (e) DUPLICATE column names, the second one denied-by-intent but the name shadows a published one
    let mut buf = Vec::new();
    let r = snapshot_table("patients", &w, &policy(), &mut buf, || {
        Ok((
            vec!["id".into(), "name".into(), "name".into()],
            vec![vec![Value::Integer(1), Value::Varchar("ada".into()), Value::Varchar("000-11-2222".into())]],
        ))
    });
    println!("[snap-duplicate-name] {:?}\n  bytes = {}", r.map(|s| s.rows), String::from_utf8_lossy(&buf));

    // (f) exact snapshot, denied column
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true)
        .open(dir.path().join("x.db")).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let w2 = wal(dir.path(), "exact");
    let txn = TxnManager::new(w2.clone(), bp.clone());
    bp.attach_wal(w2.clone());
    let mut buf = Vec::new();
    let r = snapshot_table_exact("patients", &txn, &policy(), &mut buf, |_r| {
        Ok((
            vec!["id".into(), "name".into(), "ssn".into()],
            vec![vec![Value::Integer(7), Value::Varchar("grace".into()), Value::Varchar("999-88-7777".into())]],
        ))
    });
    println!("[snap-exact-denied] rows={:?}\n  bytes = {}", r.map(|s| s.rows), String::from_utf8_lossy(&buf));
}

// ---- 2. hand-built adversarial event shapes ---------------------------------------------------

fn ev(table: &str, columns: &[&str], op: ChangeOp) -> ChangeEvent {
    ChangeEvent {
        txn_id: 1, lsn: 10, commit_lsn: 10, commit_end_lsn: 20,
        table: table.to_string(),
        columns: Arc::new(columns.iter().map(|c| c.to_string()).collect()),
        op,
    }
}

#[test]
fn shape_probes() {
    let p = policy();

    // (a) a column name needing JSON escaping, published
    let odd = Publication::parse("publication analytics\npatients: id, na\"me\n").unwrap();
    let e = ev("patients", &["id", "na\"me", "ssn"], ChangeOp::Insert {
        new: vec![Value::Integer(1), Value::Varchar("ada".into()), Value::Varchar("000-11-2222".into())],
    });
    println!("[escaped-name] {:?}", to_json_line(&e, &odd).map_err(|r| r.to_string()));

    // (b) non-ASCII column name
    let uni = Publication::parse("publication analytics\npatients: id, naïve\n").unwrap();
    let e = ev("patients", &["id", "naïve", "ssn"], ChangeOp::Insert {
        new: vec![Value::Integer(1), Value::Varchar("ada".into()), Value::Varchar("000-11-2222".into())],
    });
    println!("[non-ascii-name] {:?}", to_json_line(&e, &uni).map_err(|r| r.to_string()));

    // (c) a column name that is a JSON-injection attempt, DENIED
    let e = ev("patients", &["id", "name", "x\":\"leak"], ChangeOp::Insert {
        new: vec![Value::Integer(1), Value::Varchar("ada".into()), Value::Varchar("000-11-2222".into())],
    });
    println!("[denied-injection-name] {:?}", to_json_line(&e, &p).map_err(|r| r.to_string()));

    // (d) a PUBLISHED column name that injects JSON structure
    let inj = Publication::parse("publication analytics\npatients: id, a\":\"b\n").unwrap();
    let e = ev("patients", &["id", "a\":\"b"], ChangeOp::Insert {
        new: vec![Value::Integer(1), Value::Varchar("000-11-2222".into())],
    });
    println!("[published-injection-name] {:?}", to_json_line(&e, &inj).map_err(|r| r.to_string()));

    // (e) fewer values than columns
    let e = ev("patients", &["id", "name", "ssn"], ChangeOp::Insert { new: vec![Value::Integer(1)] });
    println!("[short-row] {:?}", to_json_line(&e, &p).map_err(|r| r.to_string()));

    // (f) more values than columns
    let e = ev("patients", &["id"], ChangeOp::Insert {
        new: vec![Value::Integer(1), Value::Varchar("000-11-2222".into())],
    });
    println!("[surplus-row] {:?}", to_json_line(&e, &p).map_err(|r| r.to_string()));

    // (g) CREATE_TABLE whose declared shape disagrees with e.columns: check() looks at e.columns,
    //     line_with_mask projects the specs. Does write_feed's up-front decision still hold?
    let e = ev("patients", &["id"], ChangeOp::Schema {
        change: SchemaChange::CreateTable,
        columns: vec![ColumnSpec { name: "ssn".into(), sql_type: "VARCHAR".into(), nullable: true }],
    });
    println!("[shape-disagrees] {:?}", to_json_line(&e, &p).map_err(|r| r.to_string()));

    // (h) DROP_TABLE of an undecided table
    let e = ev("secrets", &["id", "ssn"], ChangeOp::Schema {
        change: SchemaChange::DropTable, columns: vec![],
    });
    println!("[drop-undecided] {:?}", to_json_line(&e, &p).map_err(|r| r.to_string()));

    // (i) DROP_TABLE of a published table whose spec list still names the denied column
    let e = ev("patients", &["id", "ssn"], ChangeOp::Schema {
        change: SchemaChange::DropTable,
        columns: vec![ColumnSpec { name: "ssn".into(), sql_type: "VARCHAR".into(), nullable: true }],
    });
    println!("[drop-published-with-specs] {:?}", to_json_line(&e, &p).map_err(|r| r.to_string()));

    // (j) a Read op (snapshot shape) via to_json_line
    let e = ev("patients", &["id", "name", "ssn"], ChangeOp::Read {
        row: vec![Value::Integer(1), Value::Varchar("ada".into()), Value::Varchar("000-11-2222".into())],
    });
    println!("[read-denied] {:?}", to_json_line(&e, &p).map_err(|r| r.to_string()));

    // (k) write_table_json: table dump with an EMPTY column list but rows present
    let mut buf = Vec::new();
    let r = write_table_json("patients", &[], &[vec![Value::Varchar("000-11-2222".into())]], &p, &mut buf);
    println!("[dump-empty-cols] {:?}\n  bytes = {:?}", r.map_err(|e| e.to_string()), String::from_utf8_lossy(&buf));

    // (l) write_table_json: PARTIAL write before a refusal on a later row?
    let mut buf = Vec::new();
    let r = write_table_json("patients", &["id".into(), "name".into()],
        &[vec![Value::Integer(1), Value::Varchar("ada".into())],
          vec![Value::Integer(2), Value::Varchar("grace".into()), Value::Varchar("000-11-2222".into())]],
        &p, &mut buf);
    println!("[dump-partial] {:?}\n  bytes = {:?}", r.map_err(|e| e.to_string()), String::from_utf8_lossy(&buf));
}

// ---- 3. DROP TABLE then CREATE TABLE reusing the name -----------------------------------------

struct Db { catalog: Catalog, wal: Arc<WalManager>, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>, session: Session }

fn db(dir: &Path, tag: &str) -> Db {
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true)
        .open(dir.join(format!("{tag}.db"))).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = wal(dir, tag);
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { catalog, wal, bp, txn, session: Session::new() }
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
}

#[test]
fn drop_and_recreate_reusing_the_name() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let mut d = db(dir.path(), "reuse");
    // v1 carries the denied column SECOND; v2 carries a PUBLISHED column second.
    d.sql("CREATE TABLE patients (id INTEGER NOT NULL, ssn VARCHAR(32));");
    d.sql("INSERT INTO patients VALUES (1, '000-11-2222');");
    d.sql("DROP TABLE patients;");
    d.sql("CREATE TABLE patients (id INTEGER NOT NULL, name VARCHAR(32));");
    d.sql("INSERT INTO patients VALUES (2, 'ada');");
    d.wal.flush().unwrap();

    let decoder = LogicalDecoder::new(&d.catalog);
    let out = decoder.decode(&d.wal, d.wal.base_lsn.load(Ordering::SeqCst), d.wal.next_lsn.load(Ordering::SeqCst)).unwrap();
    println!("[reuse] {} decoded event(s)", out.events.len());
    for e in &out.events {
        println!("   {} {} cols={:?}", e.table, e.op.name(), e.columns);
    }
    let mut buf = Vec::new();
    let r = ferrodb::replication::jsonl::write_feed(&out.events, &policy(), &mut buf);
    println!("[reuse] write_feed = {:?}", r.as_ref().map_err(|e| e.to_string()));
    let feed = String::from_utf8_lossy(&buf).to_string();
    println!("[reuse] feed:\n{feed}");
    println!("[reuse] LEAKED 000-11-2222 ? {}", feed.contains("000-11-2222"));
    println!("[reuse] LEAKED name 'ssn'  ? {}", feed.contains("ssn"));
}

// ---- 4. write_feed's "or write none of them" ---------------------------------------------------

#[test]
fn write_feed_atomicity_probe() {
    let p = policy();
    let good = ev("patients", &["id", "name"], ChangeOp::Insert {
        new: vec![Value::Integer(1), Value::Varchar("ada".into())],
    });
    // A CREATE_TABLE whose declared shape projects to nothing. `check` cannot see it: it decides
    // from e.columns, and the refusal is minted inside line_with_mask, i.e. inside the write loop.
    let bad = ev("patients", &["id"], ChangeOp::Schema {
        change: SchemaChange::CreateTable,
        columns: vec![ColumnSpec { name: "ssn".into(), sql_type: "VARCHAR".into(), nullable: true }],
    });
    let mut buf = Vec::new();
    let r = ferrodb::replication::jsonl::write_feed(&[good, bad], &p, &mut buf);
    println!("[write_feed-partial] result = {:?}", r.map_err(|e| e.to_string()));
    println!("[write_feed-partial] BYTES WRITTEN DESPITE THE ERROR = {:?}", String::from_utf8_lossy(&buf));
}

// ---- 5. duplicate column names, straight from SQL ---------------------------------------------

#[test]
fn duplicate_column_names_from_sql() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let mut d = db(dir.path(), "dup");
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        d.sql("CREATE TABLE patients (id INTEGER NOT NULL, name VARCHAR(32), name VARCHAR(32));");
        d.sql("INSERT INTO patients VALUES (1, 'ada', '000-11-2222');");
    }));
    println!("[sql-dup-cols] accepted = {}", r.is_ok());
    if r.is_err() { return; }
    d.wal.flush().unwrap();
    let decoder = LogicalDecoder::new(&d.catalog);
    let out = decoder.decode(&d.wal, d.wal.base_lsn.load(Ordering::SeqCst), d.wal.next_lsn.load(Ordering::SeqCst)).unwrap();
    let mut buf = Vec::new();
    let r = ferrodb::replication::jsonl::write_feed(&out.events, &policy(), &mut buf);
    let feed = String::from_utf8_lossy(&buf).to_string();
    println!("[sql-dup-cols] write_feed = {:?}\n{feed}", r.map_err(|e| e.to_string()));
    println!("[sql-dup-cols] LEAKED 000-11-2222 ? {}", feed.contains("000-11-2222"));
    std::fs::write("/tmp/claude-501/-Users-idide-wt-ferrodb-B7/5409a51e-27bb-4fa1-b9b3-e590577d7cfd/scratchpad/run/dup_feed.jsonl", &feed).ok();
}

// ---- 6. the same duplicate-name table, left on disk for table_dump ------------------------------

#[test]
fn duplicate_columns_on_disk_for_table_dump() {
    let base = Path::new("/tmp/claude-501/-Users-idide-wt-ferrodb-B7/5409a51e-27bb-4fa1-b9b3-e590577d7cfd/scratchpad/run/dupdb");
    let _ = std::fs::remove_dir_all(base);
    std::fs::create_dir_all(base).unwrap();
    let mut d = db(base, "dup");
    d.sql("CREATE TABLE patients (id INTEGER NOT NULL, name VARCHAR(32), name VARCHAR(32));");
    d.sql("INSERT INTO patients VALUES (1, 'ada', '000-11-2222');");
    d.sql("INSERT INTO patients VALUES (2, 'grace', '999-88-7777');");
    d.wal.flush().unwrap();
    d.bp.flush_all().unwrap();
    println!("[dupdb] wrote {}", base.join("dup.db").display());
    // and the dump renderer, in process
    let mut buf = Vec::new();
    let r = write_table_json(
        "patients",
        &["id".into(), "name".into(), "name".into()],
        &[vec![Value::Integer(1), Value::Varchar("ada".into()), Value::Varchar("000-11-2222".into())]],
        &policy(), &mut buf);
    println!("[dupdb] write_table_json = {:?}\n  {}", r.map_err(|e| e.to_string()), String::from_utf8_lossy(&buf));
}

// ---- 7. the same leak through FeedStreamer::pump (the stream, not write_feed directly) ----------

#[test]
fn duplicate_names_leak_through_pump() {
    use ferrodb::replication::stream::FeedStreamer;
    let dir = tempfile::tempdir().unwrap();
    let mut d = db(dir.path(), "pumpdup");
    d.sql("CREATE TABLE patients (id INTEGER NOT NULL, name VARCHAR(32), name VARCHAR(32));");
    d.sql("INSERT INTO patients VALUES (1, 'ada', '000-11-2222');");
    d.wal.flush().unwrap();

    let streamer = FeedStreamer::new(LogicalDecoder::new(&d.catalog), policy());
    let mut buf = Vec::new();
    let cursor = FeedStreamer::start_cursor(&d.wal);
    let p = streamer.pump(&d.wal, cursor, 0, &mut buf).expect("pump");
    let feed = String::from_utf8_lossy(&buf).to_string();
    println!("[pump-dup] emitted={} refused={} clean={} refusal={:?}", p.emitted, p.refused, p.is_clean(), p.refusal.as_ref().map(|r| r.to_string()));
    println!("[pump-dup] feed:\n{feed}");
    println!("[pump-dup] LEAKED 000-11-2222 ? {}", feed.contains("000-11-2222"));
}

#[test]
fn can_an_operator_deny_the_duplicate() {
    println!("[deny-attempt-1] `patients: id, name, name` -> {:?}",
        Publication::parse("publication analytics\npatients: id, name, name\n").err().map(|e| e.to_string()));
    let only_id = Publication::parse("publication analytics\npatients: id\n").unwrap();
    let e = ev("patients", &["id", "name", "name"], ChangeOp::Insert {
        new: vec![Value::Integer(1), Value::Varchar("ada".into()), Value::Varchar("000-11-2222".into())],
    });
    println!("[deny-attempt-2] `patients: id` (name dropped entirely) -> {:?}", to_json_line(&e, &only_id).map_err(|r| r.to_string()));
    // and the leaky case again for contrast
    println!("[for-contrast]   `patients: id, name` -> {:?}", to_json_line(&e, &policy()).map_err(|r| r.to_string()));
}

#[test]
fn is_the_duplicate_a_real_column() {
    let dir = tempfile::tempdir().unwrap();
    let mut d = db(dir.path(), "sel");
    d.sql("CREATE TABLE patients (id INTEGER NOT NULL, name VARCHAR(32), name VARCHAR(32));");
    d.sql("INSERT INTO patients VALUES (1, 'ada', '000-11-2222');");
    for q in ["SELECT * FROM patients;", "SELECT name FROM patients;"] {
        let tokens = Scanner::new(q.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        let r = run(stmts.remove(0), &mut d.catalog, d.bp.clone(), d.txn.clone(), &mut d.session);
        match r {
            Err(e) => println!("[select] {q} -> ERR {e}"),
            Ok(ferrodb::execution::executor::Outcome::Rows(rows)) => println!("[select] {q} -> ROWS {rows:?}"),
            Ok(_) => println!("[select] {q} -> ok (no rows)"),
        }
    }
}
