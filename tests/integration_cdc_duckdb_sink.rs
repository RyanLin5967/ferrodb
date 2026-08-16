//! E16 — landing the change feed in **DuckDB**, and surviving replay.
//!
//! The sibling of `integration_cdc_sink.rs`, aimed at the other kind of destination. SQLite is where
//! an operational replica goes; DuckDB is where the analysts' copy goes. A change feed that can only
//! reach the first of those is half a pipeline, and the half it is missing is the one people build
//! change feeds for.
//!
//! Everything that made the SQLite sink correct has to hold here too, because it is a property of
//! the FEED, not of the destination. The feed is at-least-once by design, so a sink will be handed
//! the same event twice and can be handed a stale one after a newer one. Applying either naively is
//! not a small bug: a re-applied old `UPDATE` overwrites current data with a previous value, and a
//! re-applied `INSERT` after a `DELETE` resurrects a row the source no longer has. Both leave the
//! destination silently wrong *and self-consistent*, which is the worst failure a pipeline can have.
//!
//! **The destination is inspected with the `duckdb` CLI**, a different binary and a different build
//! of DuckDB from the one the Go driver links, for the same reason the feed is validated by a
//! separate program: a writer checked with its own reader agrees with itself about any shared
//! misreading. Where no CLI is installed the tests fall back to a second process through the Go
//! driver — [`Reader`] below records which one ran, and the fallback is explicitly the weaker of the
//! two rather than an equal. When both are present, `both_readers_agree` checks them against each
//! other, which is what stops the fallback rotting unnoticed.
//!
//! CI installs the CLI on all three runners and sets `FERRODB_REQUIRE_DUCKDB_CLI=1`, so there the
//! fallback is not merely discouraged — taking it is a failure. That variable is what closes the
//! hole this file used to have: the comparison was written, and then ran nowhere but a developer's
//! laptop, because every CI runner quietly took the other branch and still reported green.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

fn go_bin() -> String {
    for c in ["go", "/opt/homebrew/bin/go", "/usr/local/go/bin/go"] {
        if Command::new(c).arg("version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("Go is required for the CDC sink");
}

/// The consumer, compiled **once** for the whole test binary.
///
/// This suite launches it about ten times across five tests. `go run .` costs ~0.10s per launch
/// against a warm build cache versus ~0.01s for an already-built binary (measured on this machine,
/// `/usr/bin/time -p`, three runs each) — the cache means it is usually *not* relinking DuckDB, so
/// the cost is the toolchain's own startup rather than a heavy cgo link. On a cold cache, or the
/// first call after any edit to the module, it *is* the full static DuckDB link.
///
/// Either way the CPU lands on whatever else `cargo test` is running in parallel, and
/// `integration_truncation_race` refuses to report a pass when its race barely ran — so a test file
/// that quietly loads the machine can make an unrelated, correct suite fail. Build once, exec many.
fn consumer() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        // Into cargo's own target directory, not `$TMPDIR`, and under a FIXED name.
        //
        // A pid-stamped path in `$TMPDIR` is never the same twice, so nothing ever overwrites and
        // nothing ever cleans up: this binary is 60MB+ because DuckDB is linked statically, and a
        // few dozen `cargo test` runs quietly leave gigabytes behind on the developer's machine.
        // A fixed name under `target/` is overwritten by the next run and removed by `cargo clean`.
        //
        // `EXE_SUFFIX` is not decoration — `go build -o` writes exactly the name it is given, and on
        // Windows a file without `.exe` cannot be executed by `Command::new`.
        let mut out = target_dir();
        out.push(format!("cdc-consumer-test{}", std::env::consts::EXE_SUFFIX));
        let st = Command::new(go_bin())
            .current_dir("cdc-consumer")
            .args(["build", "-o"])
            .arg(&out)
            .arg(".")
            .output()
            .expect("build the Go consumer");
        assert!(
            st.status.success(),
            "building cdc-consumer failed (needs Go with cgo enabled): {}",
            String::from_utf8_lossy(&st.stderr)
        );
        out
    })
}

/// The directory holding this test binary — `target/debug/deps` — walked up to `target/debug`.
///
/// Derived from `current_exe` rather than from `CARGO_TARGET_DIR` or a hardcoded `target/`, so it
/// still lands in the right place when the target directory has been relocated.
fn target_dir() -> PathBuf {
    let mut p = std::env::current_exe().expect("test exe");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    p
}

/// Whether this environment has DECLARED that the `duckdb` CLI must be present.
///
/// Unset means "a developer machine": fall back to the Go reader and say so. CI sets it to `1`,
/// which turns that fallback from a documented degradation into a hard failure. Without this the
/// two outcomes are indistinguishable — a CI run whose CLI install silently did nothing reports
/// exactly the same green as one that compared against the CLI, and the independence this file
/// claims would be unverified on every platform at once.
///
/// An unrecognised value is REFUSED rather than read as "not required". A guard that falls through
/// to allow when it cannot parse its own input is not a guard, and the failure it would wave
/// through here is precisely the one it exists to catch.
fn cli_required() -> bool {
    match std::env::var("FERRODB_REQUIRE_DUCKDB_CLI") {
        Err(_) => false,
        Ok(v) => match v.trim() {
            "1" => true,
            "0" | "" => false,
            other => panic!(
                "FERRODB_REQUIRE_DUCKDB_CLI is {other:?}; it takes 1 or 0. Refusing to guess, \
                 because guessing \"not required\" would silently drop the CLI comparison."
            ),
        },
    }
}

/// The `duckdb` CLI, if this machine has one. Unlike `sqlite3` it is not shipped with macOS, so on
/// a developer machine its absence is a fact about the machine rather than a broken checkout —
/// hence an Option and a documented fallback, not a panic.
///
/// Where `FERRODB_REQUIRE_DUCKDB_CLI=1` says otherwise, that same absence is a broken environment
/// and this refuses. The check lives HERE rather than in each caller so that every path which SELECTS
/// a reader inherits it — `Reader::get` is the only such path, so no test can be handed the fallback
/// by accident.
///
/// It does not, and cannot, stop code from naming `Reader::Fallback` directly: that is a variant of
/// a plain enum. `both_readers_agree` does exactly that on purpose, because comparing the two
/// readers means constructing both. The guard's claim is about which reader a test gets when it
/// asks for one, not about what an author can write deliberately.
fn duckdb_cli() -> Option<String> {
    // Parsed FIRST, and on every call, rather than only in the not-found branch below. A typo like
    // `FERRODB_REQUIRE_DUCKDB_CLI=true` would otherwise sit unnoticed on every machine that happens
    // to have a CLI, and stop being a requirement on the exact day one went missing — which is the
    // day it was supposed to speak up.
    let required = cli_required();
    for c in ["duckdb", "/opt/homebrew/bin/duckdb", "/usr/local/bin/duckdb"] {
        if Command::new(c).arg("--version").output().map(|o| o.status.success()).unwrap_or(false) {
            return Some(c.to_string());
        }
    }
    assert!(
        !required,
        "FERRODB_REQUIRE_DUCKDB_CLI=1, but no `duckdb` CLI is on PATH.\n\
         These tests would otherwise read the destination back through the same Go driver that \
         wrote it, which agrees with itself about any shared misreading — so the independence they \
         claim would be unverified, and the run would still be green.\n\
         Install the CLI, or unset the variable to accept the weaker reader knowingly."
    );
    None
}

fn example_bin(name: &str) -> PathBuf {
    // `EXE_SUFFIX` is "" on unix and ".exe" on Windows, and leaving it out is not a portability
    // nicety — it is the difference between this file running on Windows and not running at all.
    // Nine sibling helpers hardcoded the unix name, all failed the Windows runner with "the system
    // cannot find the file specified" on a binary `cargo build --examples` had just built, and were
    // fixed together. This file was written in parallel with that fix and did not inherit it, so
    // every test here panicked before reaching a single DuckDB assertion.
    let bin = target_dir().join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    let bin_time = std::fs::metadata(&bin)
        .unwrap_or_else(|e| panic!("{} missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .unwrap();
    if let Some(src) = walk_newest(Path::new("src")) {
        assert!(bin_time >= src, "{} is older than src/; run: cargo build --examples", bin.display());
    }
    bin
}

fn walk_newest(dir: &Path) -> Option<std::time::SystemTime> {
    let mut newest = None;
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
        let t = if p.is_dir() { walk_newest(&p) } else { std::fs::metadata(&p).ok().and_then(|m| m.modified().ok()) };
        if let Some(t) = t {
            newest = Some(match newest { Some(c) if c > t => c, _ => t });
        }
    }
    newest
}

/// How the destination is being read back.
///
/// `Cli` is the real check: a separate binary, a separately built DuckDB. `Fallback` is the same Go
/// driver that did the writing, reached in a fresh process — it catches a sink that never committed
/// but cannot catch a driver that encodes and decodes a value wrongly in a matching pair.
enum Reader {
    Cli(String),
    Fallback,
}

impl Reader {
    fn get() -> Reader {
        match duckdb_cli() {
            Some(bin) => Reader::Cli(bin),
            None => Reader::Fallback,
        }
    }

    /// Run one statement, returning rows as `col|col|col` lines — the format both readers emit.
    fn sql(&self, db: &Path, sql: &str) -> String {
        let out = match self {
            Reader::Cli(bin) => Command::new(bin)
                .arg(db)
                .args(["-noheader", "-list", "-c"])
                .arg(sql)
                .output()
                .expect("run the duckdb CLI"),
            Reader::Fallback => Command::new(consumer())
                .arg("duckdb-sql")
                .arg(db)
                .arg(sql)
                .output()
                .expect("run the Go fallback reader"),
        };
        assert!(
            out.status.success(),
            "reading the destination failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        // The duckdb CLI terminates rows with CRLF on Windows and LF elsewhere, exactly as the
        // sqlite3 CLI does — and the Go fallback joins rows with `\n` on every platform. Without
        // this, the two readers differ on Windows by line endings alone while every byte of DATA
        // matches, and `both_readers_agree` reports it as a disagreement about the data. That is
        // the failure this file's own comments warn about: a rendering difference that sends the
        // reader after a corruption which is not there.
        //
        // Only the row SEPARATOR is normalised, deliberately. A bare `\r` inside a VARCHAR is data
        // and must still register as a difference; stripping every `\r` would be shorter and would
        // hide precisely the case worth catching.
        String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n").trim().to_string()
    }
}

/// Produce a real feed from real SQL.
fn make_feed(dir: &Path) -> PathBuf {
    let feed = dir.join("feed.jsonl");
    let out = Command::new(example_bin("cdc_feed"))
        .arg(dir.join("src.db"))
        .output()
        .expect("run cdc_feed");
    assert!(out.status.success(), "cdc_feed failed: {}", String::from_utf8_lossy(&out.stderr));
    std::fs::write(&feed, &out.stdout).unwrap();
    assert!(!out.stdout.is_empty(), "the feed is empty; the test would be vacuous");
    feed
}

/// Run the Go sink against DuckDB. Returns its stdout summary line.
fn run_sink(feed: &Path, db: &Path) -> String {
    let out = Command::new(consumer())
        .arg("sink")
        .arg(feed)
        .args(["-db"])
        .arg(db)
        .args(["-key", "id", "-engine", "duckdb"])
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

const ROWS: &str = "SELECT id,item,qty,_deleted FROM inventory ORDER BY id;";

#[test]
fn the_feed_lands_in_duckdb_matching_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let r = Reader::get();
    let feed = make_feed(dir.path());
    let db = dir.path().join("out.duckdb");

    let summary = run_sink(&feed, &db);
    assert!(summary.contains("APPLIED"), "no summary from the sink: {summary}");

    // The workload cdc_feed runs: insert ids 1..3, update id 1 to qty 999, delete id 2.
    let rows = r.sql(&db, ROWS);
    assert_eq!(
        rows,
        "1|widget|999|false\n2|gadget|20|true\n3|doohickey|30|false",
        "the destination does not match the source workload"
    );

    // The delete is a TOMBSTONE, not a missing row. That is what lets a stale re-insert be
    // rejected later; a hard delete would throw away the LSN that does the rejecting.
    assert_eq!(
        r.sql(&db, "SELECT COUNT(*) FROM inventory WHERE _deleted;"),
        "1",
        "the delete did not leave a tombstone"
    );
    // And every row records the commit that last wrote it.
    assert_eq!(r.sql(&db, "SELECT COUNT(*) FROM inventory WHERE _commit_lsn <= 0;"), "0");
    // Not one shared LSN stamped over everything: each row carries the commit that last touched IT.
    assert_eq!(
        r.sql(&db, "SELECT COUNT(DISTINCT _commit_lsn) FROM inventory;"),
        "3",
        "rows do not carry their own commit_lsn"
    );
}

/// The DDL comes from the `CREATE_TABLE` event, and lands as real DuckDB types.
///
/// This is the half of the claim that SQLite cannot test. SQLite has storage classes, so a sink that
/// declared every column `TEXT` would still store and return integers and every assertion above
/// would pass. DuckDB is typed: `qty` being `BIGINT` rather than `VARCHAR` is only true if the
/// schema event actually drove the `CREATE TABLE`, and a wrong guess would have failed the insert
/// rather than degrading quietly.
#[test]
fn the_destination_ddl_comes_from_the_create_table_event() {
    let dir = tempfile::tempdir().unwrap();
    let r = Reader::get();
    let feed = make_feed(dir.path());
    let db = dir.path().join("out.duckdb");
    run_sink(&feed, &db);

    let schema = r.sql(
        &db,
        "SELECT column_name, data_type FROM duckdb_columns() \
         WHERE table_name='inventory' ORDER BY column_index;",
    );
    assert_eq!(
        schema,
        "id|BIGINT\nitem|VARCHAR\nqty|BIGINT\n_commit_lsn|BIGINT\n_deleted|BOOLEAN",
        "the destination DDL does not match the CREATE_TABLE event"
    );

    // The key column is the conflict target, and in DuckDB `ON CONFLICT` has nothing to attach to
    // without an index on it. No primary key means no ordering guard — not a degraded one, an
    // absent one — so this is checked rather than assumed.
    assert_eq!(
        r.sql(
            &db,
            "SELECT COUNT(*) FROM duckdb_constraints() \
             WHERE table_name='inventory' AND constraint_type='PRIMARY KEY';"
        ),
        "1",
        "the key column is not a PRIMARY KEY, so ON CONFLICT has no conflict target"
    );
}

/// **Replay must be a no-op.** The feed is at-least-once, so this is the normal case, not an edge.
#[test]
fn replaying_the_whole_feed_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let r = Reader::get();
    let feed = make_feed(dir.path());
    let db = dir.path().join("out.duckdb");

    run_sink(&feed, &db);
    let all = "SELECT id,item,qty,_commit_lsn,_deleted FROM inventory ORDER BY id;";
    let after_first = r.sql(&db, all);
    assert!(!after_first.is_empty(), "nothing landed, so idempotence would be vacuous");

    let summary = run_sink(&feed, &db);
    let after_second = r.sql(&db, all);

    assert_eq!(after_first, after_second, "replaying the feed changed the destination");
    // And it must have *recognised* the replay rather than coincidentally rewriting identical rows.
    assert!(
        summary.contains("SKIPPED") && !summary.contains("SKIPPED 0"),
        "the sink did not report skipping any re-delivered events: {summary}"
    );
}

/// **The guard is in the SQL, not in the control flow — tested with genuinely out-of-order
/// delivery.**
///
/// Replaying a feed *in order* is naturally idempotent, because each row's last write is still its
/// newest one; a test that only does that passes with the ordering clause deleted and is therefore
/// testing nothing. The guard only earns its place when a stale event arrives AFTER a newer one.
///
/// So this replays a single early event — the original `INSERT` of id 1 at qty 10, which was later
/// updated to 999, and the `INSERT` of id 2, which was later deleted. The destination's cursor is
/// wiped first, so nothing above the SQL statement is in a position to reject anything: the two
/// events are handed to `apply` and the `WHERE excluded._commit_lsn > inventory._commit_lsn` clause
/// is the only thing standing between them and the data. Without it, id 1 reverts to qty 10 and id
/// 2's tombstone is overwritten by a live row. That is the silent corruption a CDC sink exists to
/// prevent.
#[test]
fn a_stale_event_arriving_after_a_newer_one_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let r = Reader::get();
    let feed = make_feed(dir.path());
    let db = dir.path().join("out.duckdb");
    run_sink(&feed, &db);

    let before = r.sql(&db, ROWS);
    assert!(before.contains("1|widget|999|false"), "the update did not land: {before}");
    assert!(before.contains("2|gadget|20|true"), "the delete did not land: {before}");

    // Build a feed of ONLY the early inserts — stale relative to what the destination now holds.
    let text = std::fs::read_to_string(&feed).unwrap();
    let stale_lines: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("\"op\":\"INSERT\"") && (l.contains("\"id\":1,") || l.contains("\"id\":2,")))
        .collect();
    assert_eq!(
        stale_lines.len(),
        2,
        "expected the two early inserts to replay; got {}:\n{text}",
        stale_lines.len()
    );
    let stale = dir.path().join("stale.jsonl");
    std::fs::write(&stale, format!("{}\n", stale_lines.join("\n"))).unwrap();

    // Lose the cursor, so nothing above the SQL statement can reject anything.
    r.sql(&db, "DELETE FROM _cdc_checkpoint;");
    assert_eq!(r.sql(&db, "SELECT COUNT(*) FROM _cdc_checkpoint;"), "0");

    // The sink must have really been made to try: if it skipped these as re-deliveries the guard
    // would never have been reached and this test would pass without testing anything.
    let summary = run_sink(&stale, &db);
    assert!(
        summary.contains("APPLIED 2") && summary.contains("SKIPPED 0"),
        "the stale events never reached the SQL guard, so this proves nothing: {summary}"
    );

    let after = r.sql(&db, ROWS);
    assert!(
        after.contains("1|widget|999|false"),
        "a stale INSERT reverted id 1 from qty 999 to its original value — the ordering guard is \
         not in the statement.\n  before: {before}\n  after:  {after}"
    );
    assert!(
        after.contains("2|gadget|20|true"),
        "a stale INSERT resurrected a deleted row — the tombstone was overwritten.\n  \
         before: {before}\n  after:  {after}"
    );
    assert_eq!(before, after, "stale events changed the destination");
}

/// The fallback reader must agree with the CLI, on any machine that has both.
///
/// Otherwise the fallback rots: a machine without the CLI would run every test above through a
/// reader nobody had ever compared against anything, and a disagreement in rendering would look
/// exactly like a disagreement in the data.
#[test]
fn both_readers_agree() {
    let Some(bin) = duckdb_cli() else {
        // Nothing to compare against. Not a pass and not a silent skip: say which reader ran, so a
        // green suite here is not mistaken for one that exercised the CLI.
        //
        // Written straight to `io::stderr()` rather than with `eprintln!`, and that is the whole
        // point of the line. libtest captures the `eprintln!`/`println!` macros and only replays
        // them for FAILING tests, so this notice — emitted by a passing one — would be swallowed
        // and the run would print `5 passed` and nothing else, defeating the guard it exists to be.
        // A direct write to the stderr handle bypasses that capture (verified on this machine).
        let _ = writeln!(
            std::io::stderr(),
            "NOTE: no duckdb CLI on this machine; the other tests in this file ran on the WEAKER \
             fallback reader (the same Go driver that did the writing). Set \
             FERRODB_REQUIRE_DUCKDB_CLI=1 to make that a failure instead of a notice — CI does."
        );
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let feed = make_feed(dir.path());
    let db = dir.path().join("out.duckdb");
    run_sink(&feed, &db);

    let cli = Reader::Cli(bin);
    let fallback = Reader::Fallback;
    for sql in [
        ROWS,
        "SELECT id,_commit_lsn,_deleted FROM inventory ORDER BY id;",
        "SELECT table_name, \"cursor\" FROM _cdc_checkpoint ORDER BY table_name;",
        // The renderings that actually differ between the two readers, and therefore the only ones
        // that make this test worth running. Every query above returns non-null scalars, on which
        // any two readers agree by accident — this one is chosen so a mismatch is REACHABLE:
        //   - NULL, which the CLI prints as the four characters `NULL` and Go's zero value does not;
        //   - a DOUBLE holding an integral value, printed `2.0` by the CLI and `2` by fmt.Sprint;
        //   - a TIMESTAMP, which Go's stringer decorates with a zone the CLI never shows.
        // Without a case like this the fallback could drift arbitrarily far and still look green on
        // every machine that has the CLI, while silently failing on every machine that does not.
        "SELECT NULL, CAST(2 AS DOUBLE), CAST(1.5 AS DOUBLE), \
         CAST('2024-01-02 03:04:05' AS TIMESTAMP), CAST('2024-01-02 03:04:05.123' AS TIMESTAMP);",
    ] {
        let a = cli.sql(&db, sql);
        let b = fallback.sql(&db, sql);
        assert!(!a.is_empty(), "the CLI read nothing back for {sql}; the comparison would be vacuous");
        assert_eq!(a, b, "the duckdb CLI and the Go fallback disagree about `{sql}`");
    }
}
