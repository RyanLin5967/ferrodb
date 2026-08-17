//! E69 — the `DROP_TABLE` half of the feed, which nothing could produce until now.
//!
//! # What was wrong
//!
//! `DdlOp::DropTable` has been a WAL record type since the log had DDL records. It serialises (tag 1),
//! deserialises, drives the retained-record logic in `wal::txn`, and `replication::logical` turns it
//! into a `DROP_TABLE` event. The Go consumer validates it, the SQLite sink runs
//! `DROP TABLE IF EXISTS`, the DuckDB sink has its own branch, and the README states that
//! "`CREATE_TABLE` and `DROP_TABLE` are" carried while `ALTER TABLE` is not.
//!
//! **Nothing could ever write one.** `DdlOp::CreateTable` was the only op any code path logged, because
//! `DROP TABLE` was not in the SQL surface at all — E67 measured it as
//! `1 at ' DROP ' expected a statement`. So every one of those branches was reachable only from a
//! hand-written fixture, the decoder arm was dead, and half a README sentence was false.
//!
//! It also made E67's own new message wrong: creating a table that exists said "DROP it first",
//! recommending a statement that did not exist — the same defect as E63's "use UPDATE to change the
//! existing row" when there was no row to update.
//!
//! # What this file pins
//!
//! The statement exists, the event reaches a real consumer, and the destination table actually goes
//! away. Plus the question the drop raises for a change feed and which nothing else asks: a table's
//! earlier changes must stay decodable after it is dropped, or dropping a table would retroactively
//! corrupt the feed that already reported its rows.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::write_feed;
use ferrodb::replication::logical::{ChangeOp, LogicalDecoder, SchemaChange};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn go_bin() -> String {
    for c in ["go", "/opt/homebrew/bin/go", "/usr/local/go/bin/go"] {
        if Command::new(c).arg("version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("Go is required to drive the CDC sink");
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
    _dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("drop.db");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("drop.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: dir, catalog, wal, bp, txn, session: Session::new() }
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

    fn decode(&self) -> ferrodb::replication::logical::Decoded {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        let decoder = LogicalDecoder::new(&self.catalog);
        decoder
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode")
    }
}

/// Create a table, change it, drop it. `keep` stays behind so the feed is not empty afterwards and a
/// claim about the dropped table is measured against a table that survived.
fn drop_workload(d: &mut Db) {
    d.sql("CREATE TABLE keep (id INTEGER NOT NULL, v INTEGER);");
    d.sql("INSERT INTO keep VALUES (1, 100);");
    d.sql("CREATE TABLE doomed (id INTEGER NOT NULL, v INTEGER);");
    d.sql("INSERT INTO doomed VALUES (1, 10);");
    d.sql("INSERT INTO doomed VALUES (2, 20);");
    d.sql("DROP TABLE doomed;");
    // AFTER the drop, deliberately. The drop checkpoints, so everything above is truncated out of the
    // window a consumer reads next - a feed built from this workload would otherwise carry DDL and no
    // data at all, and "the sink dropped a table" could not be told apart from "the sink applied
    // nothing".
    d.sql("INSERT INTO keep VALUES (2, 200);");
}

/// **The event exists in a feed produced by real SQL.**
#[test]
fn dropping_a_table_emits_a_drop_table_event() {
    let mut d = db();
    drop_workload(&mut d);
    let out = d.decode();

    let drops: Vec<&str> = out
        .events
        .iter()
        .filter(|e| matches!(&e.op, ChangeOp::Schema { change: SchemaChange::DropTable, .. }))
        .map(|e| e.table.as_str())
        .collect();
    assert_eq!(
        drops,
        vec!["doomed"],
        "a DROP TABLE produced no DROP_TABLE event, so the decoder arm and every consumer branch for \
         it are still unreachable from SQL"
    );

    // Anti-vacuity: the surviving table is not reported as dropped, so the event is about the
    // statement rather than emitted for every table.
    assert!(
        !out.events.iter().any(|e| e.table == "keep"
            && matches!(&e.op, ChangeOp::Schema { change: SchemaChange::DropTable, .. })),
        "a table that was never dropped was reported as dropped"
    );
}

/// **What a DROP does to the changes that came before it — measured, not assumed.**
///
/// My first version of this test asserted the dropped table's two inserts were still in the feed
/// afterwards, and it failed: 0 events, not 2. The reason is not that a DROP rewrites history. It is
/// that `DROP TABLE` checkpoints, exactly as `CREATE TABLE` does, and a checkpoint truncates the log.
/// Measured: `base_lsn` moved 244 -> 732 across the statement, putting both inserts below the new
/// base.
///
/// So the claim worth pinning is not "the inserts survive" - they are checkpointed away like any other
/// pre-checkpoint change - but that **the loss is not silent**. The decode reports `is_complete()` and
/// an empty `unresolved` set: the dropped table's `dir_root` still resolves, so nothing is quietly
/// attributed to the wrong table or dropped on the floor. A consumer that had not yet read past the
/// drop is refused by the existing `base_lsn` guard rather than handed a feed with a hole in it, which
/// is what `integration_truncation_race` covers.
///
/// The distinction matters because "the rows are gone" and "the rows are gone and nobody was told" are
/// different failures, and only the second is a defect.
#[test]
fn a_drop_truncates_like_any_checkpoint_and_says_so() {
    let mut d = db();

    d.sql("CREATE TABLE keep (id INTEGER NOT NULL, v INTEGER);");
    d.sql("INSERT INTO keep VALUES (1, 100);");
    d.sql("CREATE TABLE doomed (id INTEGER NOT NULL, v INTEGER);");
    d.sql("INSERT INTO doomed VALUES (1, 10);");
    d.sql("INSERT INTO doomed VALUES (2, 20);");

    // Before the drop the inserts are there. Without this the assertion after the drop proves
    // nothing - a feed that never carried them would look the same.
    let before = d.decode();
    let inserts_before = before
        .events
        .iter()
        .filter(|e| e.table == "doomed" && matches!(&e.op, ChangeOp::Insert { .. }))
        .count();
    assert_eq!(inserts_before, 2, "the workload did not land its inserts: {inserts_before}");

    let base_before = d.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
    d.sql("DROP TABLE doomed;");
    let base_after = d.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        base_after > base_before,
        "the DROP did not checkpoint, so this test is not measuring what it claims: {base_before} -> \
         {base_after}"
    );

    let after = d.decode();
    // The point: honest, not lossless.
    assert!(after.is_complete(), "the feed after a DROP reports itself incomplete");
    assert!(
        after.unresolved.is_empty(),
        "a change could not be attributed to a table after the DROP: {:?}",
        after.unresolved
    );
    assert!(after.undecodable.is_empty(), "undecodable tuples after a DROP: {:?}", after.undecodable);
    // And the drop itself is in the window a consumer reads next.
    assert!(
        after.events.iter().any(|e| e.table == "doomed"
            && matches!(&e.op, ChangeOp::Schema { change: SchemaChange::DropTable, .. })),
        "the DROP_TABLE event is not in the feed after the checkpoint that carried it"
    );
}

/// **A later checkpoint must not resurrect the dropped table.**
///
/// `log_ddl` removes a dropped table from the retained schema set rather than adding the drop to it -
/// written in anticipation, and dead until now, because a checkpoint re-emits every retained
/// `CREATE_TABLE` so the log stays self-describing at its new base. If the drop had been retained
/// instead, every future checkpoint would re-emit `DROP_TABLE` forever; if the CREATE had been left in,
/// every future checkpoint would re-declare a table that no longer exists.
///
/// The anticipation turns out to be right, and this is the first thing to check it.
#[test]
fn a_checkpoint_after_a_drop_does_not_redeclare_the_dropped_table() {
    let mut d = db();
    drop_workload(&mut d);

    // Force another checkpoint by creating a table, which is what re-emits the retained set.
    d.sql("CREATE TABLE later (id INTEGER NOT NULL);");
    let out = d.decode();

    let redeclared = out.events.iter().any(|e| e.table == "doomed"
        && matches!(&e.op, ChangeOp::Schema { change: SchemaChange::CreateTable, .. }));
    assert!(
        !redeclared,
        "a checkpoint after the DROP re-declared the dropped table, so a consumer would recreate a \
         table the source does not have"
    );
    // The surviving tables ARE re-declared, which is what makes the line above about the drop rather
    // than about the retained set having been emptied.
    for t in ["keep", "later"] {
        assert!(
            out.events.iter().any(|e| e.table == t
                && matches!(&e.op, ChangeOp::Schema { change: SchemaChange::CreateTable, .. })),
            "table '{t}' was not re-declared at the checkpoint, so the retained set is empty and this \
             test is vacuous"
        );
    }
    // And the drop is not re-emitted either.
    let drops = out.events.iter().filter(|e| e.table == "doomed"
        && matches!(&e.op, ChangeOp::Schema { change: SchemaChange::DropTable, .. })).count();
    assert!(drops <= 1, "the DROP_TABLE was re-emitted by a later checkpoint {drops} times");
}

fn run_sink(feed: &Path, db_path: &Path) -> String {
    let out = Command::new(go_bin())
        .current_dir("cdc-consumer")
        .args(["run", ".", "sink"])
        .arg(feed)
        .arg("-db")
        .arg(db_path)
        .args(["-key", "id"])
        .output()
        .expect("run the Go sink");
    assert!(
        out.status.success(),
        "the sink failed:\nstderr: {}\nstdout: {}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn query(db_path: &Path, sql: &str) -> String {
    let out = Command::new(sqlite_bin()).arg(db_path).arg(sql).output().expect("run sqlite3");
    assert!(out.status.success(), "sqlite3 failed: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n").trim().to_string()
}

fn feed_file(dir: &Path) -> PathBuf {
    let mut d = db();
    drop_workload(&mut d);
    let out = d.decode();
    let path = dir.join("feed.jsonl");
    let mut buf: Vec<u8> = Vec::new();
    let n = write_feed(&out.events, &mut buf).expect("write feed");
    assert!(n > 0, "the feed is empty; everything downstream would be vacuous");
    assert!(
        String::from_utf8_lossy(&buf).contains("\"op\":\"DROP_TABLE\""),
        "the serialised feed carries no DROP_TABLE line, so the sink cannot be tested on one"
    );
    std::fs::write(&path, &buf).unwrap();
    path
}

/// **The destination table actually goes away**, through the Go binary and verified with the sqlite3
/// CLI rather than the driver that wrote it.
#[test]
fn the_sink_drops_the_destination_table() {
    let dir = tempfile::tempdir().unwrap();
    let feed = feed_file(dir.path());
    let out = dir.path().join("out.sqlite");

    let summary = run_sink(&feed, &out);
    assert!(summary.contains("APPLIED"), "no summary from the sink: {summary}");

    // The dropped table is gone from the destination...
    let tables = query(&out, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;");
    assert!(
        !tables.split('\n').any(|t| t == "doomed"),
        "the sink left the dropped table in the destination: {tables}"
    );
    // ...and the surviving one is still there with its row, which is what makes the line above mean
    // "dropped this table" rather than "landed nothing at all".
    assert!(
        tables.split('\n').any(|t| t == "keep"),
        "the surviving table is missing too, so the sink dropped everything or applied nothing: \
         {tables}"
    );
    // A row written after the drop lands, so this run applied data as well as DDL. Row 1 is NOT here
    // and must not be asserted: the drop's checkpoint truncated it out of this window, which
    // `a_drop_truncates_like_any_checkpoint_and_says_so` measures.
    assert_eq!(
        query(&out, "SELECT id, v FROM keep ORDER BY id;"),
        "2|200",
        "no row landed in the surviving table, so the sink applied DDL and no data"
    );
}
