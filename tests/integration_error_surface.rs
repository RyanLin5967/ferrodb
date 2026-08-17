//! E67 — the errors a user can actually reach, read as a set.
//!
//! # Why as a set
//!
//! E63 and E65 were both partly message defects, and both were found by accident while working on
//! something else: one told the reader to `UPDATE` a row that did not exist, the other refused without
//! naming a way forward. Nothing had ever read this surface deliberately, and reading it one site at a
//! time is what let it drift - the same condition got a good message in one statement and a
//! contentless one in another, because the good message lived in the binder and the planner had its own
//! literal.
//!
//! So this drives a battery of reachable statements and judges what comes back, with one property over
//! the whole set: **no refusal may be contentless.** That is the check that catches a site nobody
//! thought about, which per-case assertions cannot do.
//!
//! # What it found, measured through the shipped binary on 2026-08-17
//!
//! ```text
//! SELECT nosuchcol FROM t;      -> binding error: not found
//! INSERT INTO nosuch VALUES(1); -> parsing error: table not found       (SELECT: "known tables are: t")
//! UPDATE nosuch SET v = 1;      -> parsing error: table not found
//! DELETE FROM nosuch;           -> parsing error: table not found
//! CREATE TABLE t (...);         -> key wasn't found                     (t already exists)
//! CREATE INDEX ix ON t (nope);  -> key wasn't found
//! CREATE INDEX ix ON nosuch(v); -> key wasn't found
//! DROP TABLE t;                 -> 1 at ' DROP ' expected a statement   (E69 implemented it)
//! SELECT COUNT(id) FROM t;      -> 1 at ' ( ' expected FROM
//! SELECT * FROM t ORDER BY v;   -> 1 at ' BY ' expected ;
//! SELECT * FROM t LIMIT 1;      -> 1 at ' 1 ' expected ;
//! ```
//!
//! `key wasn't found` is the worst of them: it is `FerroError::KeyNotFound`, a storage-layer variant,
//! answering three unrelated user mistakes - and it tells the reader something is MISSING in the one
//! case where the problem is that something is PRESENT.
//!
//! The last four are valid SQL this database does not implement, reported as though the reader
//! mistyped. `expected ;` after `ORDER` was pointing at `BY`, which was never the problem: `FROM t
//! ORDER` had parsed as "table `t`, aliased `ORDER`", because `parse_table_ref` kept its own denylist
//! of words that must not become an alias and `ORDER`/`LIMIT`/`GROUP` were not on it. One list now
//! serves both the alias guard and the refusal.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("err.db");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("err.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: dir, catalog, bp, txn, session: Session::new() }
}

impl Db {
    /// Run one statement and report what a user would see: `Ok` or the rendered error.
    ///
    /// Parse failures and execution failures both count, because a user cannot tell them apart and
    /// this file is about what they read.
    fn try_sql(&mut self, sql: &str) -> Result<(), FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new())
            .scan_tokens()
            .map_err(|e| FerroError::SqlParseError(format!("{e:?}")))?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if let Some(e) = p.errors.first() {
            return Err(FerroError::SqlParseError(format!("{e}")));
        }
        assert!(!stmts.is_empty(), "`{sql}` produced neither a statement nor an error");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .map(|_| ())
    }

    fn sql(&mut self, sql: &str) {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }

    /// The rendered message for a statement that must be refused.
    fn refusal(&mut self, sql: &str) -> String {
        match self.try_sql(sql) {
            Err(e) => format!("{e}"),
            Ok(()) => panic!("`{sql}` was accepted, so there is no message to judge"),
        }
    }
}

fn seeded() -> Db {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
    d.sql("INSERT INTO t VALUES (1, 10);");
    d.sql("CREATE INDEX ix ON t (v);");
    d
}

/// Every reachable refusal, with the words its message must contain.
///
/// Substrings rather than whole messages on purpose: pinning exact text makes rewording a test
/// failure, and the claim here is about content - does it name what was refused, and what to do.
const CASES: &[(&str, &[&str])] = &[
    // Unknown table, from all four statements that can hit it. These four were the drift: SELECT
    // answered well and the other three answered `parsing error: table not found`.
    ("SELECT * FROM nosuch;", &["unknown table", "nosuch", "known tables"]),
    ("INSERT INTO nosuch VALUES (1);", &["unknown table", "nosuch", "known tables"]),
    ("UPDATE nosuch SET v = 1;", &["unknown table", "nosuch", "known tables"]),
    ("DELETE FROM nosuch;", &["unknown table", "nosuch", "known tables"]),
    // Unknown column: name it, and say what is in scope.
    ("SELECT nosuchcol FROM t;", &["unknown column", "nosuchcol", "t.id", "t.v"]),
    // Something is PRESENT, not missing.
    ("CREATE TABLE t (id INTEGER NOT NULL);", &["already exists", "'t'", "DROP TABLE t"]),
    // Two different mistakes that used to share one contentless message.
    ("CREATE INDEX ix2 ON t (nosuchcol);", &["nosuchcol", "no such column", "id", "v"]),
    ("CREATE INDEX ix2 ON nosuch (v);", &["unknown table", "nosuch"]),
    // `DROP TABLE` used to be here, asserting `["DROP", "not supported"]`. E69 implemented it - the
    // WAL, the decoder and the Go sink had carried a DROP_TABLE path all along that nothing could
    // produce - so the case moved to what a DROP can still get wrong: an unknown table, answered by
    // the same shared refusal as every other statement.
    ("DROP TABLE nosuch;", &["unknown table", "nosuch", "known tables"]),
    // Valid SQL this database does not implement. Each must say so and name the feature.
    ("SELECT COUNT(id) FROM t;", &["COUNT", "not supported"]),
    ("SELECT * FROM t ORDER BY v;", &["ORDER BY", "not supported"]),
    ("SELECT * FROM t LIMIT 1;", &["LIMIT", "not supported"]),
    ("SELECT * FROM t GROUP BY v;", &["GROUP BY", "not supported"]),
    // Data-model refusals that were already good; here so a regression is caught with the rest.
    ("INSERT INTO t VALUES (1, 10);", &["duplicate primary key", "'t'"]),
    ("INSERT INTO t VALUES (NULL, 5);", &["NOT NULL", "'id'"]),
    ("INSERT INTO t VALUES (1);", &["2 column", "1 value"]),
    ("UPDATE t SET id = 9 WHERE id = 1;", &["primary key", "DELETE", "INSERT"]),
];

#[test]
fn every_reachable_refusal_says_what_was_refused() {
    for (sql, required) in CASES {
        let mut d = seeded();
        let msg = d.refusal(sql);
        for want in *required {
            assert!(
                msg.contains(want),
                "`{sql}`\n  message: {msg}\n  is missing: {want:?}"
            );
        }
    }
}

/// **The property, and the part that can catch a site nobody listed.**
///
/// Each of these strings was a real message on this surface this morning. A refusal that renders as
/// one of them has told the reader nothing, so none may appear anywhere in the battery.
///
/// **What it cannot catch, stated because a guard that hides its blind spot invites being trusted too
/// far.** It is a denylist of renderings already seen to be bad, so a NEW contentless message in a
/// wording nobody has met yet passes. In particular it does not catch a regression to a bare
/// `expected ;` - measured: removing `ORDER`/`LIMIT`/`GROUP` from the parser's keyword list leaves this
/// test green and is caught only by the per-case list above. `expected ;` deliberately stays off the
/// list, because it is the correct message for a genuinely missing semicolon and a guard that fires on
/// correct behaviour gets switched off.
#[test]
fn no_refusal_is_contentless() {
    // Exact renderings, not fragments: "not found" as a fragment appears inside the perfectly good
    // "cannot index 't.nosuchcol': no such column", and a check that cannot tell those apart would
    // have to be switched off.
    const CONTENTLESS: &[&str] = &[
        "binding error: not found",
        "parsing error: table not found",
        "key wasn't found",
        "binding error: unknown column",  // the bare form, with no name after it
        "constraint error: can't update primary key",
        "expected a statement",
    ];

    for (sql, _) in CASES {
        let mut d = seeded();
        let msg = d.refusal(sql);
        for bad in CONTENTLESS {
            assert!(
                !msg.trim_end().ends_with(bad) && msg != *bad,
                "`{sql}` answered with a message that names nothing: {msg}"
            );
        }
        // An unformatted `{}` means a `format!` was left off - the exact mistake that made
        // `unknown table: {}` print braces instead of the table name.
        assert!(
            !msg.contains("{}"),
            "`{sql}` prints an unformatted placeholder, so a value never made it in: {msg}"
        );
        // A message this short cannot be naming a subject and a reason.
        assert!(
            msg.len() > 24,
            "`{sql}` answered in {} characters, which cannot say what was refused and why: {msg}",
            msg.len()
        );
    }
}

/// Anti-vacuity for the two guards above: the statements they refuse are refused for their own
/// reasons, not because this parser or planner rejects everything.
///
/// Without this, deleting the alias handling in `parse_table_ref` - or making every statement fail -
/// would leave both tests above perfectly green.
#[test]
fn the_statements_next_to_the_refusals_still_work() {
    let mut d = seeded();

    // Aliases, which the unsupported-keyword guard sits inside. A bare alias and an AS alias.
    d.sql("SELECT * FROM t alias1;");
    d.sql("SELECT * FROM t AS a WHERE a.v = 10;");
    // The good sibling of every "unknown" case: the real names resolve.
    d.sql("SELECT id FROM t;");
    d.sql("SELECT t.v FROM t;");
    d.sql("CREATE INDEX ix3 ON t (id);");
    d.sql("CREATE TABLE t2 (id INTEGER NOT NULL);");
    // E69: DROP is a statement now, and the duplicate-table message above recommends it - so the
    // recommendation is executed here rather than merely asserted, the same way E65's DELETE-then-
    // INSERT remedy is.
    d.sql("DROP TABLE t2;");
    d.sql("CREATE TABLE t2 (id INTEGER NOT NULL);");
    // And the writes.
    d.sql("INSERT INTO t VALUES (2, 20);");
    d.sql("UPDATE t SET v = 21 WHERE id = 2;");
    d.sql("DELETE FROM t WHERE id = 2;");
}

/// A catalog lookup is not a parse failure, and an operator triaging by error kind depends on that.
///
/// Three of the four unknown-table statements used to return `FerroError::Parse`, which renders as
/// "parsing error" and files a perfectly well-formed statement with malformed SQL.
#[test]
fn a_missing_table_is_not_reported_as_a_parse_error() {
    for sql in [
        "SELECT * FROM nosuch;",
        "INSERT INTO nosuch VALUES (1);",
        "UPDATE nosuch SET v = 1;",
        "DELETE FROM nosuch;",
    ] {
        let mut d = seeded();
        let err = d.try_sql(sql).expect_err("a missing table was accepted");
        assert!(
            matches!(err, FerroError::Bind(_)),
            "`{sql}` reports a catalog lookup as {err:?} - a statement that parses fine is filed \
             under malformed SQL"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// E68 — a comparison that cannot match is refused, not answered with an empty result.
// ---------------------------------------------------------------------------------------------
//
// `Value::cmp` is a TOTAL order across types, because `Value` is a B+tree key and an index needs
// one. Its cross-type arm is `type_rank(a).cmp(&type_rank(b))` - a fixed answer that never looks at
// the values - so `Integer(10) == Varchar("abc")` is simply false. Measured before the fix:
//
//   SELECT * FROM t WHERE v = 'abc';   -> (0 rows)
//   INSERT INTO t VALUES ('abc', 1);   -> column 'id' is declared Integer but was given Varchar("abc")
//
// The type system enforced on write and ignored on read, and the read path is the one where getting
// it wrong is silent. An operator asking "why is this query empty" has no signal at all.
//
// Refused at BIND time, not in `compare`: a point lookup the optimizer routes through an index has no
// `Filter` node - `build_index_scan` folds the conjunct into bounds - so `compare` is never called on
// exactly the plans most likely to be chosen.

/// Mismatches that must be refused, and the pairs that must still work.
#[test]
fn a_comparison_that_cannot_match_is_refused() {
    let mut d = db();
    d.sql(
        "CREATE TABLE w (id INTEGER NOT NULL, f FLOAT, dec DECIMAL, s VARCHAR(20), b BIGINT, \
         ts TIMESTAMP);",
    );
    d.sql("INSERT INTO w VALUES (1, 1.5, 2.5, 'hi', 9007199254740993, 1700000000123);");

    for (sql, want) in [
        // number column, text literal - the original report.
        ("SELECT * FROM w WHERE id = 'abc';", "w.id"),
        // text column, number literal - the mirror image.
        ("SELECT * FROM w WHERE s = 5;", "w.s"),
        // a timestamp is not a string.
        ("SELECT * FROM w WHERE ts = 'yesterday';", "w.ts"),
        // it is a predicate, so DELETE and UPDATE inherit it.
        ("DELETE FROM w WHERE id = 'abc';", "w.id"),
        ("UPDATE w SET f = 1 WHERE id = 'abc';", "w.id"),
        // **Column against column.** Found by re-auditing the fix, not by writing it: the
        // literal-only check left `WHERE s = id` answering (0 rows), one substitution away from the
        // defect being fixed and just as silent.
        ("SELECT * FROM w WHERE s = id;", "w.s"),
        ("SELECT * FROM w WHERE ts = id;", "w.ts"),
    ] {
        let msg = d.refusal(sql);
        assert!(
            msg.contains("cannot compare") && msg.contains(want),
            "`{sql}` was not refused with a message naming {want}: {msg}"
        );
    }
}

/// **Anti-vacuity, and it is the more important half.**
///
/// A check that refused every comparison would satisfy the test above completely. These are the pairs
/// that must keep working, including the two the wide-type work exists for.
#[test]
fn comparisons_that_can_match_still_do() {
    let mut d = db();
    d.sql(
        "CREATE TABLE w (id INTEGER NOT NULL, f FLOAT, dec DECIMAL, s VARCHAR(20), b BIGINT, \
         ts TIMESTAMP);",
    );
    d.sql("INSERT INTO w VALUES (1, 1.5, 2.5, 'hi', 9007199254740993, 1700000000123);");

    // Same category, and the whole numeric band is one category: INTEGER against FLOAT, BIGINT and
    // DECIMAL all compare exactly rather than being refused or rounded.
    for sql in [
        "SELECT * FROM w WHERE id = 1;",
        "SELECT * FROM w WHERE f = 1.5;",
        "SELECT * FROM w WHERE dec = 2.5;",
        "SELECT * FROM w WHERE b = 9007199254740993;",
        "SELECT * FROM w WHERE ts = 1700000000123;",
        "SELECT * FROM w WHERE s = 'hi';",
        "SELECT * FROM w WHERE id = f;",
        "SELECT * FROM w WHERE id = b;",
        "SELECT * FROM w WHERE id < b;",
        // NULL is comparable with anything; three-valued logic handles it in `compare`, which
        // returns Value::Null rather than a boolean.
        "SELECT * FROM w WHERE id = NULL;",
        "SELECT * FROM w WHERE s = NULL;",
    ] {
        d.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` was refused but can match: {e}"));
    }

    // And the exactness the wide types exist for is untouched: 2^53+1 is not representable as an f64,
    // so no stored FLOAT can equal it and the answer is an empty result rather than a refusal - the
    // one case where "no rows" is the correct answer to a cross-type comparison.
    d.try_sql("SELECT * FROM w WHERE f = 9007199254740993;")
        .expect("a FLOAT compared against an exact i64 must be answered, not refused");
}

/// **A deliberate incompatibility, recorded as one rather than described as an improvement.**
///
/// `WHERE dec = '2.5'` used to return `(0 rows)` and is now refused. Several SQL dialects accept a
/// quoted numeric against a numeric column, so this is stricter than they are - a choice, not a
/// bug fix. It is the right side to err on here: the alternative is the silent empty answer this whole
/// row exists to remove, and the message names the column and says to use a number.
#[test]
fn a_quoted_number_against_a_numeric_column_is_refused_deliberately() {
    let mut d = db();
    d.sql("CREATE TABLE n (id INTEGER NOT NULL, dec DECIMAL);");
    d.sql("INSERT INTO n VALUES (1, 2.5);");
    let msg = d.refusal("SELECT * FROM n WHERE dec = '2.5';");
    assert!(msg.contains("cannot compare"), "expected the E68 refusal: {msg}");
    // The unquoted form is the way through, and it works.
    d.try_sql("SELECT * FROM n WHERE dec = 2.5;").expect("the unquoted literal must still match");
}
