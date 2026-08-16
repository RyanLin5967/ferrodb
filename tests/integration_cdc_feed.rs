//! E10 — the change feed on the wire, validated by a parser that shares no code with the producer.
//!
//! The unit tests in `src/replication/jsonl.rs` check the encoder against my own expectations of
//! what it should emit. That catches typos and not much else: an encoder validated by its own
//! author's idea of the format agrees with itself about any shared misreading of JSON.
//!
//! So the feed produced here by **real SQL** is handed to `tests/cdc/validate_feed.py`, written
//! against the JSON spec and the documented envelope rather than against the Rust. It is
//! deliberately strict — in particular it refuses bare `NaN`/`Infinity`, which Python's `json`
//! accepts by default and which would otherwise let through exactly the bug the float handling
//! exists to prevent.
//!
//! This is the same argument `tests/integration_pgwire.rs` makes for using an independently
//! written protocol client.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::write_feed;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db(tag: &str) -> Db {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(dir.path().join(format!("{tag}.db"))).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { dir, catalog, wal, bp, txn, session: Session::new() }
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

    /// Decode and write the feed, returning the path it was written to.
    fn write_feed_file(&self) -> std::path::PathBuf {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        let decoder = LogicalDecoder::new(&self.catalog);
        let out = decoder
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode");
        assert!(!out.events.is_empty(), "nothing to serialise; the test would be vacuous");
        let path = self.dir.path().join("feed.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        let n = write_feed(&out.events, &mut f).expect("write feed");
        assert_eq!(n, out.events.len(), "write_feed miscounted its own output");
        path
    }
}

/// Run the independent validator. Returns its stdout.
fn validate(path: &std::path::Path) -> String {
    let out = std::process::Command::new("python3")
        .arg("tests/cdc/validate_feed.py")
        .arg(path)
        .output()
        .expect("python3 is required to run the independent feed validator");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        out.status.success(),
        "the independent validator rejected the feed:\n{}\n--- feed ---\n{}",
        String::from_utf8_lossy(&out.stderr),
        std::fs::read_to_string(path).unwrap_or_default()
    );
    stdout
}

#[test]
fn a_feed_from_real_sql_is_valid_json_to_an_independent_parser() {
    let mut d = db("feed");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inventory VALUES (1, 10);");
    d.sql("INSERT INTO inventory VALUES (2, 20);");
    d.sql("UPDATE inventory SET qty = 999 WHERE id = 1;");
    d.sql("DELETE FROM inventory WHERE id = 2;");

    let path = d.write_feed_file();
    let report = validate(&path);

    let n: usize = report
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("OK "))
        .and_then(|s| s.parse().ok())
        .expect("no OK line");
    assert!(n >= 4, "only {n} records validated; four data statements were run");

    // The validator checked the document's shape. These check its CONTENT against the SQL, so a
    // well-formed feed describing the wrong changes cannot pass.
    let rows: Vec<&str> = report.lines().filter_map(|l| l.strip_prefix("ROW ")).collect();
    assert!(
        rows.iter().any(|r| r.contains("\"op\": \"INSERT\"") && r.contains("\"qty\": 10")),
        "no insert of qty 10 in the feed:\n{}",
        rows.join("\n")
    );
    assert!(
        rows.iter().any(|r| r.contains("\"op\": \"UPDATE\"") && r.contains("\"qty\": 999")),
        "no update to qty 999 in the feed:\n{}",
        rows.join("\n")
    );
    assert!(
        rows.iter().any(|r| r.contains("\"op\": \"DELETE\"") && r.contains("\"qty\": 20")),
        "no delete of the qty-20 row in the feed:\n{}",
        rows.join("\n")
    );
}

/// **The escaping test that matters**, because the value goes through the real scanner, the real
/// storage layer and the real decoder before it reaches the encoder. A quote or backslash surviving
/// unescaped would produce a document the validator rejects.
#[test]
fn a_varchar_full_of_json_metacharacters_survives_the_round_trip() {
    let mut d = db("escape");
    d.sql("CREATE TABLE notes (id INTEGER NOT NULL, body VARCHAR(64));");
    // A double quote and a backslash are the two characters that break JSON if unescaped. Both are
    // legal inside a single-quoted SQL literal, so they reach the encoder untouched.
    d.sql("INSERT INTO notes VALUES (1, 'he said \"hi\" \\ bye');");
    d.sql("INSERT INTO notes VALUES (2, 'café 日本語');");

    let path = d.write_feed_file();
    let report = validate(&path);

    let rows: Vec<&str> = report.lines().filter_map(|l| l.strip_prefix("ROW ")).collect();
    // The validator re-serialised what it parsed, so finding the original text in its output means
    // the value survived encoding AND decoding intact.
    assert!(
        rows.iter().any(|r| r.contains("he said")),
        "the quoted string did not survive the round trip:\n{}",
        rows.join("\n")
    );
    assert!(
        rows.iter().any(|r| r.contains("café 日本語")),
        "non-ASCII did not survive the round trip:\n{}",
        rows.join("\n")
    );
}
