//! D69 — the point lookups must work for EVERY primary-key type, not just INTEGER.
//!
//! # Why this file exists, and why the performance curve cannot replace it
//!
//! D69 replaced a full-table scan in `evaluate_merge` with point lookups. It cannot look a row up
//! by its `RowId`, because `row_id_of` is a ONE-WAY FNV for Varchar, Boolean, Float and Decimal
//! keys — so it goes through the key VALUE instead, building `WHERE pk = <literal>` with
//! `value_expr` and re-binding that literal against the column's declared type.
//!
//! ⚠ That makes the non-integer key types the ones the mechanism leans on HARDEST, and
//! `bench/d68_merge_is_o_table.txt` — the curve that verifies D69 — uses INTEGER keys ONLY. A
//! green curve proves integers and says nothing about anything else.
//!
//! Two distinct failures are possible for a key type whose literal does not round-trip exactly:
//!   * WRONG: the lookup finds nothing, `current` has no entry, and the merge compares against a
//!     missing base — a silent correctness bug.
//!   * SLOW: the predicate stops being a recognisable equality on column 0, so the planner picks a
//!     SEQ SCAN per lookup and the merge becomes O(delta x table) — WORSE than the scan D69
//!     removed, and invisible to a test that only checks the answer.
//!
//! These tests pin the first. The second needs the curve run per key type.
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(dir.path().join("p.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _dir: dir }
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(FerroError::SqlParseError(format!("{:?}", p.errors)));
        }
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    fn rows(&mut self, sql: &str, s: &mut Session) -> Vec<Vec<Value>> {
        match self.ok(sql, s) {
            Outcome::Rows(r) => r,
            Outcome::Table(t) => t.rows,
            _ => Vec::new(),
        }
    }
}

/// The lookup D69 performs, exercised directly: write a row with a given key type, then fetch it
/// back by `WHERE pk = <literal>` exactly as `evaluate_merge` now does.
///
/// If the literal does not round-trip, this returns nothing — which inside a merge means comparing
/// against a base that appears absent.
fn round_trips(decl: &str, literal: &str) -> bool {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok(&format!("CREATE TABLE t (id {decl} NOT NULL, v INTEGER);"), &mut s);
    db.ok(&format!("INSERT INTO t VALUES ({literal}, 1);"), &mut s);
    let found = db.rows(&format!("SELECT v FROM t WHERE id = {literal};"), &mut s);
    !found.is_empty()
}

#[test]
fn a_varchar_primary_key_round_trips_through_the_lookup() {
    assert!(round_trips("VARCHAR(32)", "'agent-7'"), "plain varchar key did not round-trip");
}

#[test]
fn a_varchar_key_with_awkward_content_round_trips() {
    // Spaces and non-ASCII. ⚠ A doubled-quote key ('with''quote') is NOT tested here and the
    // omission is deliberate: ferrodb's scanner does not implement SQL's doubled-quote escape, so
    // `INSERT ... VALUES ('with''quote', 1)` fails to PARSE. That is a parser limitation, not a
    // D69 defect, and it is recorded rather than worked around — see the note below.
    for lit in ["'a b'", "'ünïcode'"] {
        assert!(round_trips("VARCHAR(32)", lit), "varchar key {lit} did not round-trip");
    }
}

/// ⚠ **THE TEST ABOVE EXERCISES THE WRONG PATH, AND THIS ONE EXERCISES THE RIGHT ONE.**
///
/// `round_trips` goes through SQL TEXT — it renders a literal into a query string and re-parses
/// it. D69's point lookups do NOT do that. They build the predicate as an `Expr` AST directly:
///
/// ```ignore
/// Expr::BinaryOp {
///     left:  Expr::ColumnRef { table: None, column: pk_col },
///     operator: TokenType::Equal,
///     right: value_expr(&key),          // -> Expr::Literal { .. }, never text
/// }
/// ```
///
/// So no rendering, no re-parsing, and no escaping hazard: a key containing a quote cannot break
/// the lookup the way it breaks a hand-built query string. Discovering that is what the failure of
/// the first version of this test was actually worth — it looked like a D69 bug and was a fact
/// about how D69 differs from SQL text.
///
/// What this test pins is the property that MATTERS: a merge on a table with a VARCHAR primary key
/// finds its base row and decides correctly. That runs the real path end to end.
#[test]
fn a_merge_on_a_varchar_keyed_table_finds_its_base_row() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id VARCHAR(32) NOT NULL, v INTEGER);", &mut s);
    db.ok("INSERT INTO t VALUES ('agent-7', 100);", &mut s);
    db.ok("INSERT INTO t VALUES ('agent-8', 200);", &mut s);

    let mut a = Session::new();
    db.ok("BEGIN AGENT SESSION AS 'keytest';", &mut a);
    db.ok("UPDATE t SET v = 111 WHERE id = 'agent-7';", &mut a);
    let out = db.ok("MERGE;", &mut a);

    // The merge must APPLY. If the point lookup failed to find 'agent-7', the base would look
    // absent and the verdict would differ — which is exactly the silent failure mode this file
    // exists to catch.
    let applied = match out {
        Outcome::Agent(ref ag) => format!("{ag:?}").contains("applied_to_target: true"),
        _ => false,
    };
    assert!(applied, "a merge on a VARCHAR-keyed table did not apply — the base row was not found");

    let rows = db.rows("SELECT v FROM t WHERE id = 'agent-7';", &mut s);
    assert_eq!(rows.len(), 1, "the merged row is missing");
    assert_eq!(rows[0][0], Value::Integer(111), "the merge did not publish its value");
}

#[test]
fn a_bigint_primary_key_round_trips() {
    // Past 2^53, where a float rendering would collapse distinct keys onto one.
    assert!(round_trips("BIGINT", "9007199254740993"), "bigint key past 2^53 did not round-trip");
}

#[test]
fn a_negative_integer_primary_key_round_trips() {
    // value_expr renders negatives as UnaryOp(Minus, literal) rather than a signed literal.
    assert!(round_trips("INTEGER", "-42"), "negative integer key did not round-trip");
}

/// The end-to-end property, parameterised over the key type: a merge on a table keyed by `decl`
/// must FIND its base row and publish its value.
///
/// ⚠ **`a_merge_on_a_varchar_keyed_table_finds_its_base_row` above covered VARCHAR and nothing
/// else, and this file's own premise is that Varchar, Boolean, Float AND Decimal are all one-way
/// through `row_id_of` (`runtime.rs:158-171`) — so all four lean on the value-lookup equally.
/// Testing one of the four and calling the risk covered is the shape of a detector that fails in
/// the direction that looks like success.** Float and Decimal are the two the integer cases say
/// least about: a float key that does not compare bit-exactly, or a decimal whose digit text is
/// re-rendered, misses its base and the merge decides against a base that appears absent.
fn merge_finds_base(decl: &str, key: &str, other: &str) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok(&format!("CREATE TABLE t (id {decl} NOT NULL, v INTEGER);"), &mut s);
    db.ok(&format!("INSERT INTO t VALUES ({key}, 100);"), &mut s);
    db.ok(&format!("INSERT INTO t VALUES ({other}, 200);"), &mut s);

    let mut a = Session::new();
    db.ok("BEGIN AGENT SESSION AS 'keytest';", &mut a);
    db.ok(&format!("UPDATE t SET v = 111 WHERE id = {key};"), &mut a);
    let out = db.ok("MERGE;", &mut a);

    let applied = match out {
        Outcome::Agent(ref ag) => format!("{ag:?}").contains("applied_to_target: true"),
        _ => false,
    };
    assert!(
        applied,
        "a merge on a {decl}-keyed table did not apply — the point lookup did not find key {key}"
    );

    let rows = db.rows(&format!("SELECT v FROM t WHERE id = {key};"), &mut s);
    assert_eq!(rows.len(), 1, "{decl} key {key}: the merged row is missing");
    assert_eq!(
        rows[0][0],
        Value::Integer(111),
        "{decl} key {key}: the merge did not publish its value"
    );

    // The row the branch never touched must be untouched. Without this the test would pass if the
    // merge published over EVERY row — which is the failure mode a lookup that matches too much
    // produces, and it is invisible to an assertion that only reads the key it wrote.
    let untouched = db.rows(&format!("SELECT v FROM t WHERE id = {other};"), &mut s);
    assert_eq!(untouched.len(), 1, "{decl}: the untouched row vanished");
    assert_eq!(
        untouched[0][0],
        Value::Integer(200),
        "{decl}: the merge published over a row the branch never wrote"
    );
}

#[test]
fn a_merge_on_a_float_keyed_table_finds_its_base_row() {
    // Not a round number: 0.1 has no exact binary representation, so any path that re-renders the
    // key through text and back is a candidate to miss.
    merge_finds_base("FLOAT", "0.1", "0.2");
}

#[test]
fn a_merge_on_a_decimal_keyed_table_finds_its_base_row() {
    // `row_id_of` hashes a Decimal's DIGIT TEXT, so trailing zeros are identity-bearing: the
    // lookup must reproduce the literal exactly, not a normalised form of it.
    merge_finds_base("DECIMAL", "10.50", "10.51");
}

#[test]
fn a_merge_on_a_boolean_keyed_table_finds_its_base_row() {
    // A two-valued key is a degenerate primary key and that is exactly why it is here: with only
    // two possible rows, a lookup that matches too much still finds *a* row, so the untouched-row
    // assertion in the helper is doing the real work.
    merge_finds_base("BOOLEAN", "TRUE", "FALSE");
}

#[test]
fn a_merge_on_a_bigint_keyed_table_finds_its_base_row() {
    // Past 2^53, where a float rendering collapses distinct keys onto one.
    merge_finds_base("BIGINT", "9007199254740993", "9007199254740995");
}

#[test]
fn a_merge_on_a_negative_integer_keyed_table_finds_its_base_row() {
    // `value_expr` renders negatives as UnaryOp(Minus, literal), not as a signed literal, so this
    // is a different AST shape from every other integer case here.
    merge_finds_base("INTEGER", "-42", "-43");
}

/// **THE DANGEROUS DIRECTION, and the one the tests above do not cover.**
///
/// Mutation-testing the tests above turned up an asymmetry worth a test of its own. Breaking the
/// point lookup in `evaluate_merge` ALONE (leaving the apply path in `into_stmt` intact) made the
/// VARCHAR and BOOLEAN cases fail — and FLOAT, DECIMAL, BIGINT and INTEGER still passed. They
/// passed because "the lookup found nothing" and "there is no base row" are the same observation
/// to a caller that only asks whether the merge APPLIED: with no conflicting base in the fixture,
/// both readings admit, and the assertion never sees the difference.
///
/// That is the silent failure this file's header warns about, stated as a test instead of a
/// comment: a merge that must be REFUSED because its base moved will instead be ADMITTED if the
/// point lookup misses, and it will look exactly like a healthy merge on the way through.
///
/// So this stages a write in a branch, moves the same row underneath it in the trunk, and asserts
/// the merge is refused. A lookup that misses reports an unchanged base and admits — publishing
/// over a concurrent write, which is the one outcome the whole merge gate exists to prevent.
fn a_stale_merge_is_refused(decl: &str, key: &str) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok(&format!("CREATE TABLE t (id {decl} NOT NULL, v INTEGER);"), &mut s);
    db.ok(&format!("INSERT INTO t VALUES ({key}, 100);"), &mut s);

    let mut a = Session::new();
    db.ok("BEGIN AGENT SESSION AS 'stale';", &mut a);
    db.ok(&format!("UPDATE t SET v = 111 WHERE id = {key};"), &mut a);

    // The trunk moves the SAME row after the branch read it. The branch's view of the base is now
    // stale, and the gate must say so.
    db.ok(&format!("UPDATE t SET v = 222 WHERE id = {key};"), &mut s);

    let out = db.ok("MERGE;", &mut a);
    let applied = match out {
        Outcome::Agent(ref ag) => format!("{ag:?}").contains("applied_to_target: true"),
        _ => false,
    };
    assert!(
        !applied,
        "{decl} key {key}: a merge whose base moved underneath it was ADMITTED. Either the point \
         lookup missed the base row and reported it unchanged, or the staleness check did not \
         consult it. Both publish over a concurrent write."
    );

    // And the trunk's write must still be standing. A refusal that still mutated the row would be
    // the same data loss wearing an error message.
    let rows = db.rows(&format!("SELECT v FROM t WHERE id = {key};"), &mut s);
    assert_eq!(rows.len(), 1, "{decl}: the row vanished");
    assert_eq!(
        rows[0][0],
        Value::Integer(222),
        "{decl}: the merge was refused but the trunk's concurrent write was overwritten anyway"
    );
}

#[test]
fn a_stale_merge_is_refused_for_a_float_key() { a_stale_merge_is_refused("FLOAT", "0.1"); }

#[test]
fn a_stale_merge_is_refused_for_a_decimal_key() { a_stale_merge_is_refused("DECIMAL", "10.50"); }

#[test]
fn a_stale_merge_is_refused_for_a_varchar_key() { a_stale_merge_is_refused("VARCHAR(32)", "'agent-7'"); }

#[test]
fn a_stale_merge_is_refused_for_a_bigint_key() { a_stale_merge_is_refused("BIGINT", "9007199254740993"); }

/// **D71: a duplicate primary key must still be REFUSED when the check is a point lookup.**
///
/// `branch_insert`'s duplicate-key check stopped materialising the table and now probes
/// `pk = <literal>` built from the row being inserted. If that probe MISSES, the table looks empty
/// at that key and the duplicate is ADMITTED — a silent constraint violation that leaves two rows
/// sharing a primary key, and no error anywhere to say so.
///
/// That is the same failure direction as the stale-merge tests above and it needs the same
/// treatment: one case per key type that `row_id_of` maps one-way, because those are the ones the
/// probe leans on hardest.
fn a_duplicate_key_is_refused(decl: &str, key: &str) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok(&format!("CREATE TABLE t (id {decl} NOT NULL, v INTEGER);"), &mut s);
    db.ok(&format!("INSERT INTO t VALUES ({key}, 1);"), &mut s);

    let mut a = Session::new();
    db.ok("BEGIN AGENT SESSION AS 'dup';", &mut a);
    // `Outcome` does not implement Debug, so this matches rather than using expect_err.
    let err = match db.exec(&format!("INSERT INTO t VALUES ({key}, 2);"), &mut a) {
        Err(e) => e,
        Ok(_) => panic!(
            "{decl} key {key}: a duplicate primary key was ADMITTED — the point lookup missed \
             the existing row, so the table looked empty at that key"
        ),
    };
    assert!(
        err.to_string().contains("duplicate primary key"),
        "{decl} key {key}: refused, but not as a duplicate: {err}"
    );
}

#[test]
fn a_duplicate_varchar_key_is_refused_in_a_branch() {
    a_duplicate_key_is_refused("VARCHAR(32)", "'agent-7'");
}

#[test]
fn a_duplicate_float_key_is_refused_in_a_branch() {
    a_duplicate_key_is_refused("FLOAT", "0.1");
}

#[test]
fn a_duplicate_decimal_key_is_refused_in_a_branch() {
    a_duplicate_key_is_refused("DECIMAL", "10.50");
}

#[test]
fn a_duplicate_bigint_key_is_refused_in_a_branch() {
    a_duplicate_key_is_refused("BIGINT", "9007199254740993");
}

#[test]
fn a_distinct_key_is_still_admitted_in_a_branch() {
    // The control. Without it, a probe that matched EVERYTHING would refuse every insert and pass
    // all four tests above — a constraint that always fires is not a constraint, it is an outage.
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id VARCHAR(32) NOT NULL, v INTEGER);", &mut s);
    db.ok("INSERT INTO t VALUES ('agent-7', 1);", &mut s);
    let mut a = Session::new();
    db.ok("BEGIN AGENT SESSION AS 'dup';", &mut a);
    db.ok("INSERT INTO t VALUES ('agent-8', 2);", &mut a);
    db.ok("MERGE;", &mut a);
    let rows = db.rows("SELECT v FROM t WHERE id = 'agent-8';", &mut s);
    assert_eq!(rows.len(), 1, "a distinct key was refused or lost");
}
