//! Wide numeric and temporal types end to end, through **real SQL**.
//!
//! The unit tests in `src/replication/jsonl.rs` hand a `Value` straight to the encoder. That skips
//! the scanner, the binder, the tuple encoding, the WAL and the logical decoder — every stage where
//! an `i64` could actually be narrowed on its way to the feed. A test that constructs
//! `Value::BigInt(i64::MAX)` in memory and asserts the encoder stringifies it proves nothing about
//! whether `INSERT INTO t VALUES (9223372036854775807)` can even reach that point intact.
//!
//! So everything here starts as SQL text and ends as bytes in a `.jsonl` file, and the assertions
//! are on exact bytes rather than on "contains the digits" — a bare JSON number contains the digits
//! too, so a `contains` check would pass on the very encoding these tests exist to forbid.
//!
//! The last two tests hand the feed to the Go consumer's `precision` subcommand, which decodes with
//! `encoding/json` into `map[string]any` — the shape where every JSON number becomes a float64, and
//! precisely the consumer behaviour the string encoding exists to protect. One asserts the real
//! feed comes through with zero lossy columns; the other feeds that same checker a hand-built feed
//! carrying the wide values as **bare numbers** and asserts it reports the corruption, so a green
//! result from it means the detector works rather than that it never fires.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::{DataType, Value};
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::write_feed;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// The three literals the resume claim is about, plus the boundary just past a double's exact
/// integer range. Written once so the SQL, the expected feed bytes and the expected read-back
/// cannot drift apart.
const BIG_MAX: &str = "9223372036854775807"; // i64::MAX
const BIG_MIN: &str = "-9223372036854775808"; // i64::MIN
const BIG_PAST_2_53: &str = "9007199254740993"; // 2^53 + 1, the first integer a double cannot hold
const DEC_MANY_DIGITS: &str = "123456789012345678901234567890.12345678901234567890";
const TS_MILLIS: &str = "1700000000123";

/// Ordinary `FLOAT` values, and the reason the seeded table carries a FLOAT column at all.
///
/// `lossy=0` from the Go precision checker was a vacuous pass while every JSON *number* in the feed
/// was a small `INTEGER`: the one type whose digits a float64 always holds exactly. The checker's
/// number path was never reached, so the gate could not have failed however wrong that path was —
/// and it WAS wrong, reporting every ordinary float as corrupted.
///
/// None of these three is a dyadic rational, so none equals its own double exactly. That is the
/// point: they are precisely the values a naive exact-rational comparison flags as LOSSY, and
/// precisely the values that in fact survive the round trip perfectly, because Rust prints an f64
/// in shortest-round-trip form.
const F_TENTH: &str = "0.1";
const F_NEG: &str = "-0.3";
const F_ROUND: &str = "2.675";

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
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { dir, catalog, wal, bp, txn, session: Session::new() }
}

impl Db {
    fn exec(&mut self, sql: &str) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        assert_eq!(stmts.len(), 1, "expected exactly one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn sql(&mut self, sql: &str) {
        self.exec(sql);
    }

    /// Like `sql`, but hands back the failure instead of panicking. Scan and parse errors are
    /// folded into the same `Err` so a refusal at any stage counts as a refusal.
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, ferrodb::error::FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(ferrodb::error::FerroError::SqlParseError(format!("{:?}", p.errors)));
        }
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
    }

    fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        match self.exec(sql) {
            Outcome::Rows(r) => r,
            _ => panic!("`{sql}` did not return rows"),
        }
    }

    /// Decode the WAL into a change feed and write it, returning the path.
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

/// Populate a table holding one of every type, with the extreme literals above.
fn seeded(tag: &str) -> Db {
    let mut d = db(tag);
    d.sql(
        "CREATE TABLE wide (id INTEGER NOT NULL, big BIGINT, dec DECIMAL, ts TIMESTAMP, note VARCHAR(16), f FLOAT);",
    );
    d.sql(&format!(
        "INSERT INTO wide VALUES (1, {BIG_MAX}, {DEC_MANY_DIGITS}, {TS_MILLIS}, 'max', {F_TENTH});"
    ));
    d.sql(&format!(
        "INSERT INTO wide VALUES (2, {BIG_MIN}, -0.00000000000000000001, -1, 'min', {F_NEG});"
    ));
    d.sql(&format!(
        "INSERT INTO wide VALUES (3, {BIG_PAST_2_53}, 1.50, 0, 'edge', {F_ROUND});"
    ));
    d
}

fn go_bin() -> String {
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

/// Run a `cdc-consumer` subcommand against a feed, returning its stdout. Panics with the feed
/// attached if it exits non-zero.
fn consumer(subcommand: &str, path: &std::path::Path) -> String {
    let out = std::process::Command::new(go_bin())
        .current_dir("cdc-consumer")
        .args(["run", ".", subcommand])
        .arg(path)
        .output()
        .expect("failed to run the Go feed consumer");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        out.status.success(),
        "the independent Go consumer failed `{subcommand}` (exit {:?}):\nstderr: {}\nstdout: {}\n--- feed ---\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr),
        stdout,
        std::fs::read_to_string(path).unwrap_or_default()
    );
    stdout
}

/// **Storage round trip through real SQL.** Before saying anything about the feed, the values have
/// to survive `INSERT` -> tuple bytes -> heap -> `SELECT`. If `i64::MAX` were narrowed anywhere in
/// there, every feed assertion below would be checking a value that was already wrong.
#[test]
fn wide_literals_survive_insert_and_select_exactly() {
    let mut d = seeded("wide_select");
    let rows = d.rows("SELECT id, big, dec, ts FROM wide;");
    assert_eq!(rows.len(), 3, "expected the three inserted rows, got {rows:?}");

    let by_id = |id: i32| -> Vec<Value> {
        rows.iter()
            .find(|r| r[0] == Value::Integer(id))
            .unwrap_or_else(|| panic!("no row with id {id} in {rows:?}"))
            .clone()
    };

    // The exact variants matter as much as the values: an `i64` that arrived as `Value::Float`
    // would compare equal to plenty of things while having already lost its low bits.
    let r1 = by_id(1);
    assert!(matches!(r1[1], Value::BigInt(v) if v == i64::MAX), "id 1 big: {:?}", r1[1]);
    assert!(matches!(&r1[2], Value::Decimal(d) if d == DEC_MANY_DIGITS), "id 1 dec: {:?}", r1[2]);
    assert!(matches!(r1[3], Value::Timestamp(v) if v == 1_700_000_000_123), "id 1 ts: {:?}", r1[3]);

    let r2 = by_id(2);
    assert!(matches!(r2[1], Value::BigInt(v) if v == i64::MIN), "id 2 big: {:?}", r2[1]);
    assert!(matches!(r2[3], Value::Timestamp(v) if v == -1), "id 2 ts: {:?}", r2[3]);

    let r3 = by_id(3);
    assert!(
        matches!(r3[1], Value::BigInt(v) if v == 9007199254740993),
        "2^53+1 did not survive: {:?}",
        r3[1]
    );
    // Trailing zeros are significant to a consumer reading a price, so `1.50` must not become `1.5`.
    assert!(
        matches!(&r3[2], Value::Decimal(d) if d == "1.50"),
        "decimal scale was not preserved: {:?}",
        r3[2]
    );
}

/// **The feed ships them as JSON strings, quotes included.**
///
/// Every assertion is on the exact bytes. The paired negative assertion — that the bare-number
/// spelling is *absent* — is what makes this test fail if the encoder is switched back to emitting
/// JSON numbers; without it, a `contains` on the digits alone would pass either way.
#[test]
fn the_feed_ships_wide_types_as_quoted_json_strings() {
    let d = seeded("wide_feed");
    let path = d.write_feed_file();
    let feed = std::fs::read_to_string(&path).unwrap();

    for (field, text) in [
        ("big", BIG_MAX),
        ("big", BIG_MIN),
        ("big", BIG_PAST_2_53),
        ("dec", DEC_MANY_DIGITS),
        ("dec", "1.50"),
        ("dec", "-0.00000000000000000001"),
        ("ts", TS_MILLIS),
        ("ts", "-1"),
        ("ts", "0"),
    ] {
        let quoted = format!("\"{field}\":\"{text}\"");
        assert!(
            feed.contains(&quoted),
            "expected the exact bytes {quoted} in the feed, which would be absent if {field} \
             shipped as a bare JSON number:\n{feed}"
        );
        let bare = format!("\"{field}\":{text}");
        assert!(
            !feed.contains(&bare),
            "{field} was emitted as the bare JSON number {bare}; a consumer parsing that into a \
             double loses it with no error raised:\n{feed}"
        );
    }

    // INTEGER is i32 and exactly representable, so it must stay a bare number: stringifying it
    // would be a regression for every consumer reading that column today.
    assert!(feed.contains("\"id\":1"), "INTEGER must stay a bare JSON number:\n{feed}");
    assert!(!feed.contains("\"id\":\"1\""), "INTEGER must not be stringified:\n{feed}");
}

/// **A literal that cannot be represented is refused, loudly, at bind or write time.**
///
/// The alternative is what this whole feature exists to remove: a value that is quietly turned
/// into a different value. `serialize` picks its width from the VALUE and `deserialize` picks it
/// from the SCHEMA, so a mismatch is not a type-checking nicety — it shifts every column after it
/// in the row. Each of these must come back as an error, never as a stored row.
#[test]
fn a_literal_that_does_not_fit_its_column_is_refused_not_truncated() {
    let mut d = db("wide_refuse");
    d.sql("CREATE TABLE wide (id INTEGER NOT NULL, big BIGINT, dec DECIMAL, ts TIMESTAMP);");

    for sql in [
        // Past i64 in both directions.
        "INSERT INTO wide VALUES (1, 9223372036854775808, 1.0, 0);",
        "INSERT INTO wide VALUES (1, -9223372036854775809, 1.0, 0);",
        // A fractional literal is not an integer, in either integral column.
        "INSERT INTO wide VALUES (1, 1.5, 1.0, 0);",
        "INSERT INTO wide VALUES (1, 1, 1.0, 1.5);",
        // A string is not a number.
        "INSERT INTO wide VALUES (1, 'nope', 1.0, 0);",
    ] {
        assert!(d.try_sql(sql).is_err(), "`{sql}` was accepted; it must be refused");
    }

    // Nothing was stored by any of them.
    assert!(d.rows("SELECT id FROM wide;").is_empty(), "a refused INSERT still wrote a row");

    // And the representable extremes still go in, so the refusals above are not just "everything
    // fails".
    d.sql(&format!("INSERT INTO wide VALUES (1, {BIG_MAX}, {DEC_MANY_DIGITS}, {BIG_MIN});"));
    assert_eq!(d.rows("SELECT id FROM wide;").len(), 1);
}

/// A whole-numbered literal in a `FLOAT` column is ordinary SQL and must work. It reads as
/// `Integer` on its own, and an `Integer` written to a `FLOAT` column is refused by the width
/// guard — so without type-directed binding this plain statement is a hard error. Widening an i32
/// to f64 is exact, which is why this coercion is safe where a narrowing one would not be.
#[test]
fn a_whole_numbered_literal_lands_in_a_float_column() {
    let mut d = db("wide_float");
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, f FLOAT);");
    d.sql("INSERT INTO t VALUES (1, 5);");
    d.sql("INSERT INTO t VALUES (2, -3);");
    let rows = d.rows("SELECT id, f FROM t;");
    assert!(rows.contains(&vec![Value::Integer(1), Value::Float(5.0)]), "{rows:?}");
    assert!(rows.contains(&vec![Value::Integer(2), Value::Float(-3.0)]), "{rows:?}");

    // INTEGER columns are untouched: a whole literal there is still an i32, not a float.
    let ids = d.rows("SELECT id FROM t WHERE id = 1;");
    assert_eq!(ids, vec![vec![Value::Integer(1)]]);
}

/// **A comparison against a FLOAT column must not round the literal.**
///
/// This is the exactness the wide types exist for, aimed at the one column type that cannot hold
/// them. `Value::cmp` compares an i64 against an f64 exactly, via `cmp_i64_f64`, precisely so that
/// a question like this has a right answer. 2^53+1 is not representable as any f64, so NO stored
/// float can equal it, and the honest result is the empty set.
///
/// Getting there requires the literal to still BE 2^53+1 when the comparison happens. Bind it
/// through the FLOAT widening — `text.parse::<f64>()` — and it silently becomes 2^53 before
/// `cmp_i64_f64` is ever consulted, and the query returns the row holding 2^53: a row that is
/// provably not equal to the value asked for. No error is raised, which is what makes it the
/// dangerous kind of wrong.
///
/// The widening itself is not the bug and must stay — see the write test below. The bug was
/// SHARING it with the comparison path, where there is no storage width to satisfy and nothing to
/// gain by rounding early.
#[test]
fn a_comparison_against_a_float_column_does_not_round_the_literal() {
    let mut d = db("float_exact_cmp");
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, f FLOAT);");
    // 2^53 exactly — the largest integer with an f64 neighbour-free representation.
    d.sql("INSERT INTO t VALUES (1, 9007199254740992);");

    // The row is really there, so an empty result below is about the predicate and not the table.
    let all = d.rows("SELECT id FROM t;");
    assert_eq!(all, vec![vec![Value::Integer(1)]], "the fixture row is missing: {all:?}");

    let exact = d.rows("SELECT id FROM t WHERE f = 9007199254740992;");
    assert_eq!(exact, vec![vec![Value::Integer(1)]], "2^53 is an f64 and must match: {exact:?}");

    let past = d.rows(&format!("SELECT id FROM t WHERE f = {BIG_PAST_2_53};"));
    assert!(
        past.is_empty(),
        "`WHERE f = {BIG_PAST_2_53}` matched {past:?}. 2^53+1 is not representable as an f64, so \
         no stored float equals it — the literal was rounded down to 2^53 at bind time and the \
         query returned a row it can prove is unequal."
    );

    // The same statement, spelled as an inequality, must agree: 2^53+1 is strictly above 2^53.
    let above = d.rows(&format!("SELECT id FROM t WHERE f < {BIG_PAST_2_53};"));
    assert_eq!(
        above,
        vec![vec![Value::Integer(1)]],
        "2^53 is below 2^53+1; a rounded literal makes them equal and this empty: {above:?}"
    );
}

/// **BIGINT has to work as a counter**, which is most of what a 64-bit integer column is for.
///
/// `arithmetic` matched only Integer/Float pairs, so every one of these was
/// `Err("can't add non numbers")` — for two operands that are unambiguously numbers. A column type
/// that cannot be incremented is not a usable integer type, and the failure is not subtle: it is a
/// hard error on `SET big = big + 1`.
///
/// The values are chosen past 2^53 on purpose. If the fix routed BIGINT arithmetic through f64 the
/// statement would stop erroring and start returning the wrong number, which is a worse outcome
/// than the error it replaced — so the assertions are on exact i64 results.
#[test]
fn bigint_arithmetic_works_and_stays_exact() {
    let mut d = db("bigint_arith");
    d.sql("CREATE TABLE c (id INTEGER NOT NULL, big BIGINT);");
    d.sql(&format!("INSERT INTO c VALUES (1, {BIG_PAST_2_53});")); // 2^53 + 1

    // SELECT with arithmetic on a BIGINT.
    let sum = d.rows("SELECT big + 1 FROM c;");
    assert!(
        matches!(sum[0][0], Value::BigInt(v) if v == 9007199254740994),
        "`SELECT big + 1` gave {:?}; an f64 detour would land on 9007199254740994 by luck here, \
         so the variant matters as much as the value",
        sum[0][0]
    );

    for (expr, want) in [
        ("big - 1", 9007199254740992i64),
        ("big * 2", 18014398509481986),
        ("big / 2", 4503599627370496),
    ] {
        let got = d.rows(&format!("SELECT {expr} FROM c;"));
        assert!(
            matches!(got[0][0], Value::BigInt(v) if v == want),
            "`SELECT {expr}` gave {:?}, want BigInt({want})",
            got[0][0]
        );
    }

    // UPDATE as a counter: the read-modify-write that makes it a counter rather than a constant.
    d.sql("UPDATE c SET big = big + 1 WHERE id = 1;");
    let after = d.rows("SELECT big FROM c;");
    assert!(
        matches!(after[0][0], Value::BigInt(v) if v == 9007199254740994),
        "the counter did not advance exactly: {:?}",
        after[0][0]
    );

    // Increment again, to show it is not a one-off that happens to survive a single round trip.
    d.sql("UPDATE c SET big = big + 1 WHERE id = 1;");
    let twice = d.rows("SELECT big FROM c;");
    assert!(
        matches!(twice[0][0], Value::BigInt(v) if v == 9007199254740995),
        "two increments past 2^53 must be distinguishable, which f64 cannot do: {:?}",
        twice[0][0]
    );

    // Mixing a BIGINT with a narrow INTEGER column is ordinary SQL and must widen, not refuse.
    let mixed = d.rows("SELECT big + id FROM c;");
    assert!(
        matches!(mixed[0][0], Value::BigInt(v) if v == 9007199254740996),
        "BIGINT + INTEGER should widen to BIGINT: {:?}",
        mixed[0][0]
    );
}

/// TIMESTAMP is an i64 too, and epoch millis are a thing you add to.
#[test]
fn timestamp_arithmetic_advances_by_milliseconds() {
    let mut d = db("ts_arith");
    d.sql("CREATE TABLE e (id INTEGER NOT NULL, ts TIMESTAMP);");
    d.sql(&format!("INSERT INTO e VALUES (1, {TS_MILLIS});"));

    let plus = d.rows("SELECT ts + 1000 FROM e;");
    assert!(
        matches!(plus[0][0], Value::Timestamp(v) if v == 1_700_000_001_123),
        "a second past the timestamp is still a timestamp: {:?}",
        plus[0][0]
    );

    d.sql("UPDATE e SET ts = ts - 123 WHERE id = 1;");
    let back = d.rows("SELECT ts FROM e;");
    assert!(
        matches!(back[0][0], Value::Timestamp(v) if v == 1_700_000_000_000),
        "{:?}",
        back[0][0]
    );
}

/// **The declared types survive a catalog reload.**
///
/// `DataType` is written to the catalog page as a one-byte tag, and the wide types took the next
/// three free numbers. If a tag were wrong or unhandled, a reopened database would either refuse
/// the page or — far worse — read the column back as a *different* type, at which point
/// `Tuple::deserialize` takes the wrong width and every column after it in the row shifts.
///
/// Reading the rows back rather than just the schema is what makes that second failure detectable:
/// a wrong width is invisible in the column list and obvious in the values.
#[test]
fn the_declared_types_and_their_rows_survive_a_catalog_reload() {
    let mut d = seeded("wide_reload");
    let first_page = d.catalog.first_catalog_page_id;

    // Reopen the catalog from the pages, as recovery and the CLI do.
    d.catalog = Catalog::open(d.bp.clone(), first_page).expect("reopen catalog");

    let entry = d.catalog.get_table("wide").expect("table missing after reload");
    let declared: Vec<(&str, &DataType)> =
        entry.schema.columns.iter().map(|c| (c.name.as_str(), &c.data_type)).collect();
    assert_eq!(
        declared,
        vec![
            ("id", &DataType::Integer),
            ("big", &DataType::BigInt),
            ("dec", &DataType::Decimal),
            ("ts", &DataType::Timestamp),
            ("note", &DataType::Varchar(16)),
            ("f", &DataType::Float),
        ],
        "the reloaded schema is not the declared one"
    );

    // And the rows still decode at the right widths through that reloaded schema.
    let rows = d.rows("SELECT id, big, dec, ts, note FROM wide;");
    assert_eq!(rows.len(), 3);
    let r1 = rows
        .iter()
        .find(|r| r[0] == Value::Integer(1))
        .unwrap_or_else(|| panic!("row 1 missing after reload: {rows:?}"));
    assert!(matches!(r1[1], Value::BigInt(v) if v == i64::MAX), "{:?}", r1[1]);
    assert!(matches!(&r1[2], Value::Decimal(d) if d == DEC_MANY_DIGITS), "{:?}", r1[2]);
    assert!(matches!(r1[3], Value::Timestamp(v) if v == 1_700_000_000_123), "{:?}", r1[3]);
    // The column AFTER the wide ones is the one a wrong width corrupts, so check it explicitly.
    assert_eq!(r1[4], Value::Varchar("max".into()), "the column after the wide ones shifted");
}

/// **A predicate against a wide column finds the row.**
///
/// This is the query-side half of type-directed literal binding, and it is the one that fails
/// silently rather than loudly if it is missing: a `WHERE` comparing a `Timestamp` cell against a
/// literal bound as some other numeric variant falls through to the cross-type rank fallback,
/// which is a fixed answer independent of the values. The query then returns **no rows** and
/// reports no error, which is a wrong answer wearing an empty result's clothes.
#[test]
fn a_where_clause_against_a_wide_column_matches_by_value() {
    let mut d = seeded("wide_where");

    // Past 2^53, so an f64-mediated comparison would also match id 1's neighbour if it existed.
    let big = d.rows(&format!("SELECT id FROM wide WHERE big = {BIG_MAX};"));
    assert_eq!(big, vec![vec![Value::Integer(1)]], "BIGINT equality found {big:?}");

    let ts = d.rows(&format!("SELECT id FROM wide WHERE ts = {TS_MILLIS};"));
    assert_eq!(ts, vec![vec![Value::Integer(1)]], "TIMESTAMP equality found {ts:?}");

    let dec = d.rows("SELECT id FROM wide WHERE dec = 1.50;");
    assert_eq!(dec, vec![vec![Value::Integer(3)]], "DECIMAL equality found {dec:?}");

    // Range predicates too, and the negative bound.
    let neg = d.rows("SELECT id FROM wide WHERE ts < 0;");
    assert_eq!(neg, vec![vec![Value::Integer(2)]], "TIMESTAMP range found {neg:?}");

    let past = d.rows(&format!("SELECT id FROM wide WHERE big > {BIG_PAST_2_53};"));
    assert_eq!(past, vec![vec![Value::Integer(1)]], "only i64::MAX is above 2^53+1: {past:?}");
}

/// The schema event names the wide types, so a consumer learns **in band** that a column is a
/// BIGINT rather than having to infer it from a string that happens to hold digits. Without this,
/// a sink could not tell `BIGINT` from `VARCHAR` — both arrive as JSON strings by design.
#[test]
fn the_schema_event_declares_the_wide_types_by_name() {
    let d = seeded("wide_schema");
    let feed = std::fs::read_to_string(d.write_feed_file()).unwrap();
    let create = feed
        .lines()
        .find(|l| l.contains("\"op\":\"CREATE_TABLE\""))
        .unwrap_or_else(|| panic!("no CREATE_TABLE event in the feed:\n{feed}"));
    for (col, ty) in [("big", "BIGINT"), ("dec", "DECIMAL"), ("ts", "TIMESTAMP")] {
        assert!(
            create.contains(&format!("\"name\":\"{col}\",\"type\":\"{ty}\"")),
            "the schema event does not declare {col} as {ty}: {create}"
        );
    }
    // The pre-existing types must still be spelled the way they always were.
    assert!(create.contains("\"type\":\"INTEGER\""), "{create}");
    assert!(create.contains("\"type\":\"VARCHAR(16)\""), "{create}");
}

/// A NULL in a wide column is JSON `null`, not the four characters `"null"`. A consumer that cannot
/// tell a missing amount from the string "null" has a worse problem than precision.
#[test]
fn a_null_in_a_wide_column_stays_json_null() {
    let mut d = db("wide_null");
    d.sql("CREATE TABLE wide (id INTEGER NOT NULL, big BIGINT, dec DECIMAL, ts TIMESTAMP);");
    d.sql("INSERT INTO wide VALUES (1, NULL, NULL, NULL);");

    let rows = d.rows("SELECT big, dec, ts FROM wide;");
    assert_eq!(rows[0], vec![Value::Null, Value::Null, Value::Null], "NULLs did not survive storage");

    let feed = std::fs::read_to_string(d.write_feed_file()).unwrap();
    for f in ["big", "dec", "ts"] {
        assert!(feed.contains(&format!("\"{f}\":null")), "{f} was not JSON null:\n{feed}");
        assert!(!feed.contains(&format!("\"{f}\":\"null\"")), "{f} was a quoted null:\n{feed}");
    }
}

/// An UPDATE reaches the wide columns with the same type-directed binding as INSERT, and the
/// before/after images both carry the exact digits.
#[test]
fn an_update_to_a_wide_column_ships_both_images_as_strings() {
    let mut d = seeded("wide_update");
    d.sql(&format!("UPDATE wide SET big = {BIG_PAST_2_53} WHERE id = 1;"));

    let rows = d.rows("SELECT big FROM wide WHERE id = 1;");
    assert!(matches!(rows[0][0], Value::BigInt(v) if v == 9007199254740993), "{:?}", rows[0][0]);

    let feed = std::fs::read_to_string(d.write_feed_file()).unwrap();
    let update = feed
        .lines()
        .find(|l| l.contains("\"op\":\"UPDATE\""))
        .unwrap_or_else(|| panic!("no UPDATE in the feed:\n{feed}"));
    assert!(
        update.contains(&format!("\"big\":\"{BIG_MAX}\"")),
        "the before image lost the old i64::MAX: {update}"
    );
    assert!(
        update.contains(&format!("\"big\":\"{BIG_PAST_2_53}\"")),
        "the after image lost the new value: {update}"
    );
}

/// **The independent half.** Go's `encoding/json` decoding into `map[string]any` is the consumer
/// shape the resume claim is about: every JSON number becomes a float64. Running it over the real
/// feed must find the wide columns arriving as **strings**, with the exact digits, and zero columns
/// corrupted.
///
/// This shares no code with the producer, so it cannot agree with the Rust about a misreading of
/// JSON the way the encoder's own unit tests can.
#[test]
fn an_independent_go_consumer_receives_the_wide_columns_as_exact_strings() {
    let d = seeded("wide_go");
    let path = d.write_feed_file();

    // It is a legal feed first; `validate` exits non-zero otherwise.
    consumer("validate", &path);

    let report = consumer("precision", &path);
    for text in [BIG_MAX, BIG_MIN, BIG_PAST_2_53, DEC_MANY_DIGITS, TS_MILLIS] {
        assert!(
            report.lines().any(|l| l.ends_with(&format!(" string {text}"))),
            "Go did not receive `{text}` as an exact JSON string:\n{report}"
        );
    }
    // `id` is INTEGER, so Go must see it as a number — the narrow type is deliberately untouched.
    assert!(
        report.lines().any(|l| l.contains(" id number ")),
        "INTEGER should still arrive as a JSON number:\n{report}"
    );

    // **The FLOAT column is what makes `lossy=0` below mean anything.**
    //
    // While every JSON number in this feed was a small INTEGER, the checker's number path handled
    // only values a float64 holds exactly, so `lossy=0` was true no matter how that path behaved.
    // A FLOAT is the case it is actually there to judge: it ships as a bare number by design,
    // because f64 is exactly what a float64 decode produces.
    //
    // One line per seeded row, and NOT ONE of them flagged. `precision` prints the exact value the
    // double holds rather than the wire text — `0.1` is reported as
    // 0.1000000000000000055511151231257827021181583404541015625, which is genuinely what a float64
    // contains — so the assertion is on the absence of the LOSSY marker, not on the digits.
    let float_lines: Vec<&str> =
        report.lines().filter(|l| l.contains(" f number ")).collect();
    assert_eq!(
        float_lines.len(),
        3,
        "expected the three seeded FLOAT cells to be inspected as numbers:\n{report}"
    );
    for line in &float_lines {
        assert!(
            !line.contains("LOSSY"),
            "an ordinary FLOAT was reported as a precision loss, which is a false alarm — the \
             digits on the wire are exactly the double's shortest round-trip form: {line}"
        );
    }
    // And it must be a *number*, not quoted: FLOAT deliberately does not take the string encoding.
    assert!(
        !report.lines().any(|l| l.contains(&format!(" f string {F_TENTH}"))),
        "FLOAT should ship as a bare JSON number, not a string:\n{report}"
    );

    let summary = report
        .lines()
        .find(|l| l.starts_with("SUMMARY "))
        .unwrap_or_else(|| panic!("no SUMMARY line:\n{report}"));
    assert!(
        summary.contains("lossy=0"),
        "a default float64 decode corrupted a column of the real feed: {summary}\n{report}"
    );
    // A run that inspected nothing is not a pass.
    assert!(!summary.contains("strings=0"), "no string columns were inspected: {summary}");
    assert!(!summary.contains("numbers=0"), "no number columns were inspected: {summary}");
}

/// **The detector is forced to fire.** `lossy=0` above is only evidence if `precision` is capable
/// of reporting anything else. This hands it the same values spelled as **bare JSON numbers** —
/// exactly what the encoder would produce if the string encoding were removed — and requires it to
/// report the corruption.
///
/// The numbers it prints are the concrete cost of the alternative: `i64::MAX` comes back as
/// 9223372036854775808, one larger than the type it came from, and Go raised no error doing it.
#[test]
fn the_same_values_as_bare_json_numbers_are_silently_corrupted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bare.jsonl");
    // Hand-built rather than produced by this codebase: the point is to model the encoder we do
    // NOT have, so it cannot be generated by the encoder we do.
    let line = format!(
        "{{\"table\":\"wide\",\"op\":\"INSERT\",\"txn\":1,\"lsn\":1,\"commit_lsn\":1,\
         \"commit_end_lsn\":1,\"after\":{{\"id\":1,\"big\":{BIG_MAX},\"edge\":{BIG_PAST_2_53},\
         \"dec\":{DEC_MANY_DIGITS},\"ts\":{TS_MILLIS}}}}}\n"
    );
    std::fs::write(&path, &line).unwrap();

    let report = consumer("precision", &path);
    let summary = report
        .lines()
        .find(|l| l.starts_with("SUMMARY "))
        .unwrap_or_else(|| panic!("no SUMMARY line:\n{report}"));
    assert!(
        summary.contains("strings=0"),
        "this fixture must contain no strings, or it is not modelling a bare-number encoder: {summary}"
    );
    assert!(
        !summary.contains("lossy=0"),
        "the precision checker reported nothing lost on a feed that provably loses i64::MAX — so \
         its `lossy=0` on the real feed would have meant nothing:\n{report}"
    );

    // Name the specific corruptions, so this test documents the cost rather than just a count.
    assert!(
        report.lines().any(|l| l.contains(&format!(" big number 9223372036854775808 LOSSY {BIG_MAX}"))),
        "i64::MAX should come back one larger than it went in:\n{report}"
    );
    assert!(
        report.lines().any(|l| l.contains(&format!(" edge number 9007199254740992 LOSSY {BIG_PAST_2_53}"))),
        "2^53+1 should come back as 2^53:\n{report}"
    );
    assert!(
        report.lines().any(|l| l.contains(" dec number ") && l.contains(" LOSSY ")),
        "the many-digit decimal should not survive a float64:\n{report}"
    );

    // And the timestamp used here is small enough to survive, which is the honest half of the
    // claim: TIMESTAMP ships as a string because the *type* is i64, not because every epoch-millis
    // value overflows a double.
    assert!(
        report.lines().any(|l| l.contains(&format!(" ts number {TS_MILLIS}")) && !l.contains("LOSSY")),
        "epoch millis at this magnitude does fit a double; say so rather than overclaiming:\n{report}"
    );
}
