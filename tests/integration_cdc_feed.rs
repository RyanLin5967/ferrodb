//! E10 — the change feed on the wire, validated by a parser that shares no code with the producer.
//!
//! The unit tests in `src/replication/jsonl.rs` check the encoder against my own expectations of
//! what it should emit. That catches typos and not much else: an encoder validated by its own
//! author's idea of the format agrees with itself about any shared misreading of JSON.
//!
//! So the feed produced here by **real SQL** is handed to `cdc-consumer`, a separate program in a
//! separate language (Go), written against the JSON spec and the documented envelope rather than
//! against the Rust. Go's `encoding/json` rejects bare `NaN`/`Infinity` outright, so the strictness
//! that matters most here is the standard library's rather than something this project asserts
//! about itself.
//!
//! This is the same argument `tests/integration_pgwire.rs` makes for using an independently written
//! protocol client.

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

/// Locate the Go toolchain. `cargo test` does not necessarily inherit an interactive shell's PATH,
/// so a bare `go` can fail on a machine where Go is installed and working.
pub fn go_bin() -> String {
    for candidate in ["go", "/opt/homebrew/bin/go", "/usr/local/go/bin/go"] {
        if std::process::Command::new(candidate)
            .arg("version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            return candidate.to_string();
        }
    }
    panic!("Go is required to run the independent feed consumer; none of go, /opt/homebrew/bin/go, /usr/local/go/bin/go worked");
}

/// Run the independent Go validator. Returns its stdout.
fn validate(path: &std::path::Path) -> String {
    // Run from inside the Go module: `cdc-consumer` has its own go.mod, and the repo root is not
    // a Go module, so invoking it from here fails with "cannot find main module".
    let out = std::process::Command::new(go_bin())
        .current_dir("cdc-consumer")
        .args(["run", ".", "validate"])
        .arg(path)
        .output()
        .expect("failed to run the Go feed consumer");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        out.status.success(),
        "the independent Go validator rejected the feed (exit {:?}):\nstderr: {}\nstdout: {}\n\
         --- feed ---\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr),
        stdout,
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

    // Go checked the document's shape. These check its CONTENT against the SQL, so a well-formed
    // feed describing the wrong changes cannot pass.
    let feed = std::fs::read_to_string(&path).unwrap();
    assert!(
        feed.lines().any(|l| l.contains("\"op\":\"INSERT\"") && l.contains("\"qty\":10")),
        "no insert of qty 10 in the feed:\n{feed}"
    );
    assert!(
        feed.lines().any(|l| l.contains("\"op\":\"UPDATE\"") && l.contains("\"qty\":999")),
        "no update to qty 999 in the feed:\n{feed}"
    );
    assert!(
        feed.lines().any(|l| l.contains("\"op\":\"DELETE\"") && l.contains("\"qty\":20")),
        "no delete of the qty-20 row in the feed:\n{feed}"
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
    // Go parsing it at all is the assertion: an unescaped quote or backslash makes `encoding/json`
    // reject the document outright, and `validate` panics on a non-zero exit.
    validate(&path);

    let feed = std::fs::read_to_string(&path).unwrap();
    assert!(
        feed.contains("he said"),
        "the quoted string did not reach the feed:\n{feed}"
    );
    assert!(
        feed.contains("café 日本語"),
        "non-ASCII did not reach the feed:\n{feed}"
    );
}
