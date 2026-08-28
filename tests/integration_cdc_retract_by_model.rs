//! B5 — retract by model version, end to end, judged by a consumer that shares no code with us.
//!
//! Every other test of this lane asks the producer what it emitted. This one runs the real pipeline
//! — SQL into ferrodb, the WAL decoded into a JSONL feed, the feed landed in SQLite by the Go
//! consumer — and then asks the **destination** which rows a given model version wrote.
//!
//! That is the question provenance exists for, and it is asked in the shape it is really asked in:
//! days later, at the destination, by somebody who does not have the source database and cannot
//! replay a log that has been checkpointed since.
//!
//! Ground truth is `cdc-consumer scan`, a full table scan, not the count `retract` reports about
//! its own work. A predicate that matched the wrong set reports a confident count of exactly the
//! wrong rows.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::provenance::sha256::prompt_digest;
use ferrodb::provenance::{ProvId, RunEntity};
use ferrodb::replication::jsonl::write_feed;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Locate the Go toolchain. `cargo test` does not necessarily inherit an interactive shell's PATH.
fn go_bin() -> String {
    for candidate in ["go", "/opt/homebrew/bin/go", "/usr/local/go/bin/go"] {
        if Command::new(candidate)
            .arg("version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            return candidate.to_string();
        }
    }
    panic!("Go is required to run the independent CDC consumer");
}

/// Run one `cdc-consumer` subcommand, returning stdout. Panics with both streams on failure, so a
/// broken pipeline reports what the consumer actually said rather than an exit code.
fn consumer(args: &[&str]) -> String {
    let out = Command::new(go_bin())
        // `cdc-consumer` has its own go.mod and the repo root is not a Go module.
        .current_dir("cdc-consumer")
        .arg("run")
        .arg(".")
        .args(args)
        .output()
        .expect("failed to run the Go CDC consumer");
    assert!(
        out.status.success(),
        "cdc-consumer {args:?} failed ({}):\n--- stdout ---\n{}\n--- stderr ---\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// Run one that is expected to be REFUSED, returning stderr.
fn consumer_refuses(args: &[&str]) -> String {
    let out = Command::new(go_bin())
        .current_dir("cdc-consumer")
        .arg("run")
        .arg(".")
        .args(args)
        .output()
        .expect("failed to run the Go CDC consumer");
    assert!(
        !out.status.success(),
        "cdc-consumer {args:?} was expected to refuse and exited 0:\n{}",
        String::from_utf8_lossy(&out.stdout)
    );
    String::from_utf8_lossy(&out.stderr).to_string()
}

struct Db {
    dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("retract.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("retract.wal")).unwrap());
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

    /// `BEGIN`, bind the run, run the statements, `COMMIT`.
    fn agent_txn(&mut self, run_entity: &RunEntity, statements: &[&str]) {
        self.sql("BEGIN;");
        let txn_id = self.session.current.expect("BEGIN did not open a transaction");
        self.txn.bind_run(txn_id, run_entity.clone()).expect("bind_run");
        for s in statements {
            self.sql(s);
        }
        self.sql("COMMIT;");
    }

    fn write_feed_file(&self) -> std::path::PathBuf {
        self.wal.flush().unwrap();
        let decoder = LogicalDecoder::new(&self.catalog);
        let decoded = decoder
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode");
        assert!(!decoded.events.is_empty(), "nothing to serialise; the test would be vacuous");
        let path = self.dir.path().join("feed.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        // B7 gave the render path a publication. This test is about attribution and retraction,
        // not egress policy, so it publishes everything.
        write_feed(
            &decoded.events,
            &ferrodb::replication::publication::Publication::unrestricted(),
            &mut f,
        )
        .expect("write feed");
        path
    }
}

fn a_run(prov: u32, agent: &str, run_id: &str, model_version: &str, prompt: &str) -> RunEntity {
    RunEntity::new(
        ProvId(prov),
        agent,
        run_id,
        "claude-opus",
        model_version,
        prompt_digest(prompt),
        1_700_000_000_000,
        BranchId::new(prov as u64, 0),
    )
}

/// One row of `cdc-consumer scan` output.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScannedRow {
    model_version: String,
    retracted: bool,
    deleted: bool,
}

/// Parse `scan` output into `id -> row`. This is the ground truth: a full table scan of the
/// destination, produced by a program that did not do the retracting.
fn scan(db_path: &Path, table: &str) -> BTreeMap<i64, ScannedRow> {
    let out = consumer(&["scan", db_path.to_str().unwrap(), "-table", table, "-key", "id"]);
    let mut rows = BTreeMap::new();
    let mut scanned = None;
    for line in out.lines() {
        if let Some(rest) = line.strip_prefix("SCANNED ") {
            scanned = rest.split_whitespace().next().and_then(|n| n.parse::<usize>().ok());
            continue;
        }
        let Some(rest) = line.strip_prefix("ROW ") else { continue };
        let mut fields = BTreeMap::new();
        for pair in rest.split_whitespace() {
            if let Some((k, v)) = pair.split_once('=') {
                fields.insert(k.to_string(), v.to_string());
            }
        }
        let id: i64 = fields["id"].parse().expect("id");
        rows.insert(
            id,
            ScannedRow {
                model_version: fields["model_version"].clone(),
                retracted: fields["retracted"] == "1",
                deleted: fields["deleted"] == "1",
            },
        );
    }
    assert_eq!(
        scanned,
        Some(rows.len()),
        "the scan's own count disagrees with the rows it printed: {out}"
    );
    assert!(!rows.is_empty(), "the scan returned no rows; nothing below would be tested");
    rows
}

/// **The exit criterion, end to end: 100% of one model version's rows and 0% of any other.**
///
/// Breaking shape: a destination holding MORE THAN ONE model version *and* rows written by no agent
/// at all. A single-version workload passes a retraction that ignores its predicate and takes the
/// whole table; a workload with no unattributed rows passes one whose comparison treats a missing
/// writer as a match. Both defects ship a destination that is silently wrong and self-consistent.
#[test]
fn retract_by_model_touches_one_model_versions_rows_and_no_others() {
    let mut d = db();
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");

    let trusted = a_run(1, "restock-agent", "run-42", "2026-05", "top up everything below reorder");
    let suspect = a_run(2, "restock-agent", "run-99", "2026-07", "top up everything below reorder");

    d.agent_txn(&trusted, &[
        "INSERT INTO inventory VALUES (1, 10);",
        "INSERT INTO inventory VALUES (3, 30);",
    ]);
    d.agent_txn(&suspect, &[
        "INSERT INTO inventory VALUES (2, 20);",
        "INSERT INTO inventory VALUES (4, 40);",
        "INSERT INTO inventory VALUES (5, 50);",
    ]);
    // An ordinary transaction with no agent behind it: the rows a retraction must never touch,
    // whatever string it is given.
    d.sql("INSERT INTO inventory VALUES (6, 60);");

    let feed = d.write_feed_file();
    let feed_s = feed.to_str().unwrap();

    // The consumer's own census of the feed, before anything is landed.
    let validated = consumer(&["validate", feed_s]);
    let census = validated
        .lines()
        .find(|l| l.starts_with("WRITERS "))
        .unwrap_or_else(|| panic!("no WRITERS line:\n{validated}"));
    assert!(
        census.contains("attributed=5"),
        "five row changes came from an agent run: {census}"
    );
    assert!(
        census.contains("unattributed=1"),
        "one row change came from ordinary SQL, and must be reported rather than assumed away: \
         {census}"
    );

    let dest = d.dir.path().join("dest.sqlite");
    let dest_s = dest.to_str().unwrap();
    let applied = consumer(&["sink", feed_s, "-db", dest_s, "-key", "id"]);
    assert!(applied.contains("APPLIED "), "the sink landed nothing: {applied}");

    // Before: nothing is retracted, and both model versions really are present. Without this the
    // assertions after the retraction could all hold on an empty or single-version table.
    let before = scan(&dest, "inventory");
    assert_eq!(before.len(), 6, "expected six landed rows, got {before:?}");
    assert!(before.values().all(|r| !r.retracted), "something was already retracted: {before:?}");
    assert_eq!(before[&1].model_version, "2026-05");
    assert_eq!(before[&2].model_version, "2026-07");
    assert_eq!(before[&6].model_version, "<unattributed>");

    let report = consumer(&[
        "retract", dest_s, "-table", "inventory", "-model-version", "2026-07",
    ]);
    assert!(
        report.contains("RETRACTED 3 OF 6"),
        "the retraction reported the wrong blast radius: {report}"
    );

    // Ground truth: a full scan, by a program that did not do the retracting.
    let after = scan(&dest, "inventory");
    let (mut hit, mut spared, mut unattributed) = (0, 0, 0);
    for (id, row) in &after {
        match row.model_version.as_str() {
            "2026-07" => {
                hit += 1;
                assert!(row.retracted, "row {id} was written by 2026-07 and was not retracted");
            }
            "<unattributed>" => {
                unattributed += 1;
                assert!(!row.retracted, "row {id} has no writer at all and was retracted");
            }
            other => {
                spared += 1;
                assert!(!row.retracted, "row {id} was written by {other} and was retracted anyway");
            }
        }
    }
    assert_eq!(hit, 3, "100% of the suspect model's rows: {after:?}");
    assert_eq!(spared, 2, "0% of the trusted model's rows: {after:?}");
    assert_eq!(unattributed, 1, "0% of the rows with no writer: {after:?}");

    // And the other half of the anti-vacuity pair: retracting the OTHER version now marks exactly
    // the rows the first retraction spared, so the predicate really is reading the column.
    let report = consumer(&[
        "retract", dest_s, "-table", "inventory", "-model-version", "2026-05",
    ]);
    assert!(report.contains("RETRACTED 2 OF 6"), "{report}");
    let after = scan(&dest, "inventory");
    assert!(after[&1].retracted && after[&3].retracted, "the trusted model's rows: {after:?}");
    assert!(!after[&6].retracted, "the unattributed row was swept up by the second pass: {after:?}");
}

/// A retraction naming a model version nothing wrote is **refused**, not reported as a clean run of
/// zero rows. The likely cause is a typo, and a zero here is indistinguishable from success.
#[test]
fn a_retraction_that_matches_nothing_is_refused_end_to_end() {
    let mut d = db();
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");
    let trusted = a_run(1, "restock-agent", "run-42", "2026-05", "top up everything below reorder");
    d.agent_txn(&trusted, &["INSERT INTO inventory VALUES (1, 10);"]);

    let feed = d.write_feed_file();
    let dest = d.dir.path().join("dest.sqlite");
    let dest_s = dest.to_str().unwrap();
    consumer(&["sink", feed.to_str().unwrap(), "-db", dest_s, "-key", "id"]);

    let err = consumer_refuses(&[
        "retract", dest_s, "-table", "inventory", "-model-version", "2026-99",
    ]);
    assert!(err.contains("nothing was retracted"), "refused, but not by this guard: {err}");
    assert!(err.contains("2026-05"), "the error does not say what versions are present: {err}");

    // Anti-vacuity: the version that IS there is accepted.
    let ok = consumer(&["retract", dest_s, "-table", "inventory", "-model-version", "2026-05"]);
    assert!(ok.contains("RETRACTED 1 OF 1"), "{ok}");
}

/// The prompt reaches the consumer as a digest and never as text — checked on the actual bytes that
/// crossed the boundary, not on what the encoder believes it wrote.
#[test]
fn the_feed_carries_a_prompt_digest_and_never_the_prompt() {
    let mut d = db();
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER NOT NULL);");
    let prompt = "refund the customer at 4471 Elm Street, card ending 9021";
    let agent = a_run(1, "refund-agent", "run-7", "2026-05", prompt);
    d.agent_txn(&agent, &["INSERT INTO inventory VALUES (1, 10);"]);

    let feed = d.write_feed_file();
    let text = std::fs::read_to_string(&feed).unwrap();

    assert!(
        !text.contains("4471 Elm Street"),
        "the prompt itself is in the feed:\n{text}"
    );
    let digest = ferrodb::provenance::sha256::to_hex(&prompt_digest(prompt));
    assert_eq!(digest.len(), 64);
    assert!(
        text.contains(&format!("\"prompt_sha256\":\"{digest}\"")),
        "the feed does not carry the prompt digest:\n{text}"
    );
    // Non-zero, which is the whole difference from the `[0u8; 32]` this field used to hold.
    assert_ne!(digest, "0".repeat(64));

    // The independent consumer agrees the line is well formed, including the writer allowlist that
    // would have refused a leaked prompt field.
    let out = consumer(&["validate", feed.to_str().unwrap()]);
    assert!(out.starts_with("OK "), "{out}");
    assert!(out.contains("attributed=1"), "{out}");
    assert!(out.contains("unattributed=0"), "{out}");
}
