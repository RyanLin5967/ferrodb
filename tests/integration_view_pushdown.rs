//! D28 — the system-view generator hint, and the proof it cannot change an answer.
//!
//! # What is under test, and why this file exists before the optimisation did
//!
//! `catalog::system_views` materialises a whole view and then runs the query's `WHERE` over it
//! through the engine's own `Filter`. Its header says why: reuse gets "the engine's comparison and
//! evaluation semantics, not a second implementation of them that can drift". At 10⁶ branches
//! `SELECT * FROM ferro_branches WHERE branch_id = 1` therefore built 10⁶ rows to return one, and
//! took **22.2 s** doing it on this machine (`bench/d28_before.txt`; S15 measured 31.5 s on
//! theirs).
//!
//! [`ViewHint`] narrows what the *generator* builds without letting it decide what a row means.
//! That is only true if it is tested, and there is exactly one test that establishes it: **run every
//! query both ways and compare.** `run_select` uses the hint; `run_select_unhinted` withholds it and
//! materialises everything, which is precisely what the code did before D28. If the two ever
//! disagree, the hint has narrowed away a row the filter would have kept.
//!
//! # The half that makes the other half mean something
//!
//! A hint that never fires passes the drift comparison perfectly. So every narrowing assertion here
//! is paired with a **count**: the hinted generator must produce strictly fewer rows than the
//! unhinted one for a query that should narrow, and exactly as many for a query that must not.
//!
//! **Both halves were forced to fire before being trusted**, which is the only reason to believe
//! either of them:
//!
//! * `ViewHint::for_select` made to return `ViewHint::All` unconditionally — the hint switched off.
//!   Three of these eight tests go red (`the_hint_narrows_what_the_generator_builds`,
//!   `the_extracted_hint_is_the_one_the_predicate_licenses`,
//!   `a_literal_the_binder_refuses_never_reaches_the_hint`) and the drift test stays **green**,
//!   because withholding the hint is exactly what the drift test's other side already does.
//! * `id_hint` made to return `lo + 1` — a hint that loses one row, the only failure mode this
//!   design is supposed to make impossible. The drift test goes red on `SELECT generation, state
//!   FROM ferro_branches WHERE branch_id = 3`.
//!
//! So the two are not redundant: one catches a hint that stopped working, the other catches a hint
//! that started lying. Neither catches the other's case.
//!
//! # Both catalogs, because they narrow by different means
//!
//! `BranchCatalog::scan_ids` has a default implementation that filters a full scan, and
//! `TableBranchCatalog` overrides it with a B+tree range descent. Those are two different pieces of
//! code that must return the same records, so the whole matrix runs against both. `LogBranchCatalog`
//! keeps records in a `HashMap`; the table catalog keeps them on pages under
//! `[0x00][id big-endian]` keys.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, BranchState, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::system_views::{
    run_select, run_select_unhinted, NamedRows, SystemView, ViewHint,
};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::{Parser, Stmt};
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Which `BranchCatalog` the fixture is built over. Both are exercised by every test in this file,
/// because `scan_ids` is two different implementations and only one of them is the fast one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Under {
    /// `HashMap` records; `scan_ids` is the trait's default, a filtered full scan.
    Log,
    /// B+tree records keyed by id; `scan_ids` is a range descent.
    Table,
}

impl Under {
    const BOTH: [Under; 2] = [Under::Log, Under::Table];
}

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new(under: Under) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("views.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("views.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);

        let branches: Arc<dyn BranchCatalog> = match under {
            Under::Log => Arc::new(LogBranchCatalog::in_memory(1)),
            Under::Table => {
                Arc::new(TableBranchCatalog::open_sidecar(&dir.path().join("b.branchcat"), 1).unwrap())
            }
        };
        let runtime = Arc::new(AgentRuntime::with_catalog(branches));
        Db { catalog, bp, txn, runtime, _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        run(parse(sql), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }
}

fn parse(sql: &str) -> Stmt {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let mut stmts = p.parse();
    assert!(p.errors.is_empty(), "{sql}: {:?}", p.errors);
    assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
    stmts.remove(0)
}

/// How many branches the fixture forks on top of the ones its sessions create.
///
/// Large enough that a narrowed generator is unambiguously narrower than a whole one — a fixture of
/// three branches would let a hint that returned two rows instead of three "pass" a `<` assertion
/// while narrowing nothing real.
const BULK: u64 = 200;

/// The fixture's quarantined branch still has a live run, so it is a `ferro_runs` row on top of the
/// sessions the fixture hands back.
///
/// Found by this file rather than assumed: the first version asserted `ferro_runs` held exactly one
/// row per session in `live` and it held one more, `b_4` / agent `b`. `seal` drops a workspace on
/// merge or abandon, and a merge the premise check REFUSES does neither — the branch is held, and a
/// held branch's work is exactly the work an operator reading `ferro_runs` is looking for. Written
/// down here because it is the correct behaviour and the test was wrong about it.
const QUARANTINE_HOLDS_ITS_WORKSPACE: usize = 1;

/// A database with something in every view, and with the live sessions **scattered** through the id
/// space rather than clustered at the end.
///
/// The scattering is deliberate. `runs_rows` merges the live-run list against one record scan over
/// the span those runs occupy, and a fixture where every session sat at the high end would never
/// walk the cursor past a non-matching record — which is the only interesting line in that merge.
fn fixture(under: Under) -> (Db, Vec<Session>) {
    let mut db = Db::new(under);
    let mut boot = db.session();
    db.ok("CREATE TABLE oncall (id INTEGER NOT NULL, qty INTEGER);", &mut boot);
    db.ok("CREATE TABLE ledger (id INTEGER NOT NULL, qty INTEGER);", &mut boot);
    db.ok("CREATE TABLE audit (id INTEGER NOT NULL, qty INTEGER);", &mut boot);
    for t in ["oncall", "ledger", "audit"] {
        for i in 1..=4 {
            db.ok(&format!("INSERT INTO {t} VALUES ({i}, {});", i * 10), &mut boot);
        }
    }
    drop(boot);

    // Published rows in two of the three tables, so `ferro_row_authors` has more than one
    // `table_name` to narrow between and one table that must never appear under a narrowed hint.
    for (n, t) in [("oncall", "oncall"), ("ledger", "ledger")] {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'writer_{n}' RUN 'r_{n}';"), &mut s);
        db.ok(&format!("UPDATE {t} SET qty = 99 WHERE id = 1;"), &mut s);
        db.ok(&format!("UPDATE {t} SET qty = 98 WHERE id = 2;"), &mut s);
        db.ok("MERGE;", &mut s);
    }

    // A held branch, reached through the merge-admission check rather than by calling
    // `quarantine` directly — the same route `integration_system_views` uses, for the same reason.
    {
        let mut a = db.session();
        db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r_qa';", &mut a);
        db.ok("SELECT qty FROM audit WHERE id = 1;", &mut a);
        let mut b = db.session();
        db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r_qb';", &mut b);
        db.ok("SELECT qty FROM audit WHERE id = 1;", &mut b);
        let held = b.agent.as_ref().unwrap().branch;
        db.ok("UPDATE audit SET qty = 111 WHERE id = 1;", &mut a);
        db.ok("UPDATE audit SET qty = 222 WHERE id = 2;", &mut b);
        db.ok("MERGE;", &mut a);
        db.ok("MERGE;", &mut b);
        assert_eq!(
            db.runtime.branches().get(held).expect("record").state,
            BranchState::Quarantined,
            "the premise check did not hold b, so ferro_quarantine has nothing to show"
        );
    }

    // Bulk branches with live sessions interleaved through them.
    let lease = LeaseDeadline(u64::MAX);
    let mut live = Vec::new();
    for i in 0..BULK {
        db.runtime.branches().fork(BranchId::TRUNK, lease).expect("fork");
        if i % 50 == 17 {
            let mut s = db.session();
            db.ok(&format!("BEGIN AGENT SESSION AS 'agent_{i}' RUN 'r_live_{i}';"), &mut s);
            live.push(s);
        }
    }
    assert!(live.len() >= 3, "fixture opened {} live sessions", live.len());
    (db, live)
}

/// Every query this file runs both ways, against every view whose columns it mentions.
///
/// Each entry is `(view, sql, must_narrow)`. `must_narrow` is the claim that the generator builds
/// strictly fewer rows under the hint — asserted as a count, so a hint that quietly stopped firing
/// fails here rather than passing the comparison.
fn matrix() -> Vec<(SystemView, &'static str, bool)> {
    use SystemView::*;
    vec![
        // --- ferro_branches: the view W1 measured.
        (Branches, "SELECT * FROM ferro_branches;", false),
        (Branches, "SELECT generation, state FROM ferro_branches WHERE branch_id = 3;", true),
        (Branches, "SELECT * FROM ferro_branches WHERE branch_id = 0;", true),
        // The id written on the left of the operator and on the right: the same question typed two
        // ways must narrow the same amount.
        (Branches, "SELECT * FROM ferro_branches WHERE 3 = branch_id;", true),
        (Branches, "SELECT * FROM ferro_branches WHERE branch_id >= 190;", true),
        (Branches, "SELECT * FROM ferro_branches WHERE 190 <= branch_id;", true),
        (Branches, "SELECT * FROM ferro_branches WHERE branch_id > 190;", true),
        (
            Branches,
            "SELECT * FROM ferro_branches WHERE branch_id >= 10 AND branch_id <= 20;",
            true,
        ),
        // Three-way AND, and a conjunct on a column the hint knows nothing about. The unknown side
        // must neither widen nor narrow the known one.
        (
            Branches,
            "SELECT * FROM ferro_branches WHERE branch_id >= 10 AND branch_id <= 20 AND state = 'Live';",
            true,
        ),
        // Empty by intersection. The honest answer is no rows, not every row.
        (Branches, "SELECT * FROM ferro_branches WHERE branch_id >= 9 AND branch_id <= 3;", true),
        // --- shapes that must NOT narrow.
        (Branches, "SELECT * FROM ferro_branches WHERE branch_id = 3 OR branch_id = 9;", false),
        (Branches, "SELECT * FROM ferro_branches WHERE branch_id != 3;", false),
        (Branches, "SELECT * FROM ferro_branches WHERE state = 'Live';", false),
        (Branches, "SELECT * FROM ferro_branches WHERE depth = 1;", false),
        // A bare upper bound leaves the lower bound negative, and negative ids are exactly the
        // `u64` values above `i64::MAX` — two spans, not one. Declined.
        (Branches, "SELECT * FROM ferro_branches WHERE branch_id <= 20;", false),
        (Branches, "SELECT * FROM ferro_branches WHERE branch_id < 20;", false),
        (Branches, "SELECT * FROM ferro_branches WHERE branch_id = -1;", false),
        // An alias moves nothing: the hint reads bound column offsets, not spellings.
        (Branches, "SELECT b.state FROM ferro_branches AS b WHERE b.branch_id = 3;", true),
        // --- ferro_runs: the view W2 measured. Its row source is now the workspace map, so the
        // generator is already short and `must_narrow` is false even for a keyed lookup.
        (Runs, "SELECT * FROM ferro_runs;", false),
        // Trunk never has a run, so this narrows the live-run list to nothing before a single
        // record is read.
        (Runs, "SELECT * FROM ferro_runs WHERE branch_id = 0;", true),
        (Runs, "SELECT agent_id FROM ferro_runs WHERE branch_id >= 100;", true),
        (Runs, "SELECT * FROM ferro_runs WHERE agent_id = 'agent_17';", false),
        (Runs, "SELECT * FROM ferro_runs WHERE branch_id = 1 OR branch_id = 2;", false),
        // --- ferro_row_authors: keyed on a name, not an id.
        (RowAuthors, "SELECT * FROM ferro_row_authors;", false),
        (RowAuthors, "SELECT * FROM ferro_row_authors WHERE table_name = 'oncall';", true),
        (RowAuthors, "SELECT * FROM ferro_row_authors WHERE 'oncall' = table_name;", true),
        (
            RowAuthors,
            // `OR` narrows here and does not narrow for `branch_id`, which is the whole difference
            // between the two lattices: a union of name sets is a name set, a union of id intervals
            // is not an interval. `audit` also has an attributed row, so this genuinely excludes one.
            "SELECT * FROM ferro_row_authors WHERE table_name = 'oncall' OR table_name = 'ledger';",
            true,
        ),
        (
            RowAuthors,
            "SELECT * FROM ferro_row_authors WHERE table_name = 'oncall' AND agent_id = 'writer_oncall';",
            true,
        ),
        // A name no table has. Narrowed to nothing, which is what the filter would have concluded.
        (RowAuthors, "SELECT * FROM ferro_row_authors WHERE table_name = 'nosuch';", true),
        (RowAuthors, "SELECT * FROM ferro_row_authors WHERE table_name != 'oncall';", false),
        (RowAuthors, "SELECT * FROM ferro_row_authors WHERE agent_id = 'writer_oncall';", false),
        // --- the two views that decline the hint. They must still answer correctly.
        (Quarantine, "SELECT * FROM ferro_quarantine;", false),
        (Quarantine, "SELECT * FROM ferro_quarantine WHERE branch_id = 3;", false),
        (RunActivity, "SELECT * FROM ferro_run_activity;", false),
        (RunActivity, "SELECT * FROM ferro_run_activity WHERE branch_id = 0;", false),
    ]
}

/// **The drift test.** Every query, answered with the hint and without it, must be identical.
///
/// Identical rows in identical order with identical column metadata — `NamedRows` compares all
/// three. This is the whole safety argument for D28 reduced to an assertion: a hint that can only
/// widen what the generator produces cannot change what `Filter` concludes.
#[test]
fn every_view_query_answers_identically_with_and_without_the_hint() {
    for under in Under::BOTH {
        let (db, _live) = fixture(under);
        let mut non_empty = 0usize;
        for (view, sql, _) in matrix() {
            let stmt = parse(sql);
            let hinted = run_select(view, &stmt, &db.catalog, &db.runtime)
                .unwrap_or_else(|e| panic!("{under:?} {sql}: {e}"));
            let plain = run_select_unhinted(view, &stmt, &db.catalog, &db.runtime)
                .unwrap_or_else(|e| panic!("{under:?} {sql}: {e}"));
            assert_eq!(
                hinted, plain,
                "{under:?}: the hint changed the answer to `{sql}`\n  with hint: {:?}\n  without:   {:?}",
                hinted.rows, plain.rows
            );
            if !hinted.is_empty() {
                non_empty += 1;
            }
        }
        // Anti-vacuity for the comparison itself: two empty results are equal, and a fixture that
        // populated nothing would make every assertion above trivially true.
        assert!(
            non_empty >= 20,
            "{under:?}: only {non_empty} of {} queries returned a row, so the comparison above is \
             mostly comparing emptiness",
            matrix().len()
        );
    }
}

/// **The fire check.** A hint that never fires passes the drift test perfectly.
///
/// So this asserts the generator really builds less: fewer rows under the hint than without it, for
/// every query the matrix marks `must_narrow`. Replacing `ViewHint::for_select` with
/// `Ok(ViewHint::All)` turns this test red and leaves the drift test green, which is the property
/// that makes the pair worth having.
#[test]
fn the_hint_narrows_what_the_generator_builds() {
    for under in Under::BOTH {
        let (db, _live) = fixture(under);
        let mut fired = 0usize;
        for (view, sql, must_narrow) in matrix() {
            let stmt = parse(sql);
            let hint = ViewHint::for_select(view, &stmt, &db.catalog).expect("hint");
            let narrow = view
                .materialise_hinted(&db.catalog, &db.runtime, &hint)
                .unwrap_or_else(|e| panic!("{under:?} {sql}: {e}"))
                .len();
            let whole = view
                .materialise(&db.catalog, &db.runtime)
                .unwrap_or_else(|e| panic!("{under:?} {sql}: {e}"))
                .len();
            if must_narrow {
                assert!(
                    narrow < whole,
                    "{under:?}: `{sql}` generated {narrow} rows of {whole} — the hint {hint:?} did \
                     not narrow anything"
                );
                fired += 1;
            } else {
                // The other direction, and it is not the same assertion. A hint that narrowed here
                // would be narrowing on a predicate it cannot read — the shape that loses rows.
                assert_eq!(
                    narrow, whole,
                    "{under:?}: `{sql}` narrowed to {narrow} of {whole} rows under {hint:?}, and \
                     nothing about that predicate permits narrowing"
                );
            }
        }
        assert!(fired >= 10, "{under:?}: only {fired} queries narrowed; the matrix expects more");
    }
}

/// The hint read off each predicate, spelled out.
///
/// The count assertions above prove the generator builds less; this proves it builds less *for the
/// right reason*. A hint of `BranchIds { lo: 0, hi: 20 }` and one of `BranchIds { lo: 10, hi: 20 }`
/// both narrow, and only one of them is what `branch_id >= 10 AND branch_id <= 20` means.
#[test]
fn the_extracted_hint_is_the_one_the_predicate_licenses() {
    use ferrodb::catalog::system_views::ViewHint::*;
    let (db, _live) = fixture(Under::Table);
    let tables = |names: &[&str]| Tables(names.iter().map(|s| s.to_string()).collect());

    let cases: Vec<(SystemView, &str, ViewHint)> = vec![
        (SystemView::Branches, "SELECT * FROM ferro_branches;", All),
        (
            SystemView::Branches,
            "SELECT * FROM ferro_branches WHERE branch_id = 3;",
            BranchIds { lo: 3, hi: 3 },
        ),
        (
            SystemView::Branches,
            "SELECT * FROM ferro_branches WHERE 3 = branch_id;",
            BranchIds { lo: 3, hi: 3 },
        ),
        (
            SystemView::Branches,
            "SELECT * FROM ferro_branches WHERE branch_id >= 10 AND branch_id <= 20;",
            BranchIds { lo: 10, hi: 20 },
        ),
        (
            SystemView::Branches,
            "SELECT * FROM ferro_branches WHERE 10 <= branch_id AND 20 >= branch_id;",
            BranchIds { lo: 10, hi: 20 },
        ),
        (
            SystemView::Branches,
            "SELECT * FROM ferro_branches WHERE branch_id > 190;",
            BranchIds { lo: 191, hi: i64::MAX as u64 },
        ),
        // The upper bound stops at `i64::MAX` and not at `u64::MAX`, because an id above it renders
        // as a NEGATIVE `BIGINT` and would not satisfy `>= 190` anyway. Including it would be a
        // superset, which is safe; excluding it is exact, which is better.
        (
            SystemView::Branches,
            "SELECT * FROM ferro_branches WHERE branch_id >= 190;",
            BranchIds { lo: 190, hi: i64::MAX as u64 },
        ),
        // Empty, spelled the one way an inclusive range can be: lo above hi.
        (
            SystemView::Branches,
            "SELECT * FROM ferro_branches WHERE branch_id >= 9 AND branch_id <= 3;",
            BranchIds { lo: 1, hi: 0 },
        ),
        // A conjunct the hint cannot read constrains nothing and must not disturb the one it can.
        (
            SystemView::Branches,
            "SELECT * FROM ferro_branches WHERE state = 'Live' AND branch_id = 7;",
            BranchIds { lo: 7, hi: 7 },
        ),
        // Declined, each for its own reason: a union of intervals is not an interval; `!=` is a
        // hole rather than a bound; a bare upper bound admits the wrapped high half of the `u64`
        // range; a negative literal is entirely in that half; and a `DECIMAL` literal would need a
        // second copy of the engine's coercion rules to read.
        (SystemView::Branches, "SELECT * FROM ferro_branches WHERE branch_id = 3 OR branch_id = 9;", All),
        (SystemView::Branches, "SELECT * FROM ferro_branches WHERE branch_id != 3;", All),
        (SystemView::Branches, "SELECT * FROM ferro_branches WHERE branch_id <= 20;", All),
        (SystemView::Branches, "SELECT * FROM ferro_branches WHERE branch_id = -1;", All),
        (SystemView::Branches, "SELECT * FROM ferro_branches WHERE depth = 1;", All),
        // `ferro_runs` shares the key column, so it shares the lattice.
        (
            SystemView::Runs,
            "SELECT * FROM ferro_runs WHERE branch_id = 4;",
            BranchIds { lo: 4, hi: 4 },
        ),
        // Names are a set, so `OR` IS expressible here — a union of name sets is a name set.
        (
            SystemView::RowAuthors,
            "SELECT * FROM ferro_row_authors WHERE table_name = 'oncall';",
            tables(&["oncall"]),
        ),
        (
            SystemView::RowAuthors,
            "SELECT * FROM ferro_row_authors WHERE table_name = 'oncall' OR table_name = 'ledger';",
            tables(&["ledger", "oncall"]),
        ),
        (
            SystemView::RowAuthors,
            "SELECT * FROM ferro_row_authors WHERE table_name = 'oncall' AND agent_id = 'x';",
            tables(&["oncall"]),
        ),
        // Two incompatible equalities intersect to nothing, and nothing is the right answer.
        (
            SystemView::RowAuthors,
            "SELECT * FROM ferro_row_authors WHERE table_name = 'oncall' AND table_name = 'ledger';",
            tables(&[]),
        ),
        (
            SystemView::RowAuthors,
            "SELECT * FROM ferro_row_authors WHERE table_name != 'oncall';",
            All,
        ),
        (SystemView::RowAuthors, "SELECT * FROM ferro_row_authors WHERE table_name > 'a';", All),
        // The two views that decline the hint decline it whatever they are asked.
        (SystemView::Quarantine, "SELECT * FROM ferro_quarantine WHERE branch_id = 3;", All),
        (SystemView::RunActivity, "SELECT * FROM ferro_run_activity WHERE branch_id = 3;", All),
    ];

    for (view, sql, want) in cases {
        let got = ViewHint::for_select(view, &parse(sql), &db.catalog).expect("hint");
        assert_eq!(got, want, "`{sql}` yielded the wrong hint");
    }
}

/// A literal the binder will not accept against the key column never reaches the hint at all — and
/// when it does not, the **query** does not run either.
///
/// `comparison_bounds` declines every literal that is not an `Integer` or a `BigInt`, which reads
/// like a defensive arm until you ask what SQL produces one. This says what actually happens:
/// `branch_id = 3.0` and `branch_id = '3'` are refused by the binder, so no such comparison is ever
/// bound. Worth pinning because `ViewHint::for_select` binds the `WHERE` clause a **second time**,
/// independently of the one `run_select` binds — the two must refuse the same statements, or a
/// query could fail with one error while the hint had already failed with another.
///
/// Found by this file: the case table above originally expected `All` for both of these and got a
/// `Bind` error instead.
#[test]
fn a_literal_the_binder_refuses_never_reaches_the_hint() {
    let (db, _live) = fixture(Under::Table);
    let mut refused = 0usize;
    for sql in [
        "SELECT * FROM ferro_branches WHERE branch_id = 3.0;",
        "SELECT * FROM ferro_branches WHERE branch_id = '3';",
    ] {
        let stmt = parse(sql);
        let hint = ViewHint::for_select(SystemView::Branches, &stmt, &db.catalog);
        let query = run_select(SystemView::Branches, &stmt, &db.catalog, &db.runtime);
        assert!(hint.is_err(), "`{sql}`: the hint bound a literal the query cannot");
        assert!(query.is_err(), "`{sql}`: the query ran a literal the hint refused");
        assert_eq!(
            hint.unwrap_err().to_string(),
            query.unwrap_err().to_string(),
            "`{sql}`: the hint and the query refuse it for different stated reasons"
        );
        refused += 1;
    }
    assert_eq!(refused, 2, "the loop did not run");
}

/// `scan_ids` is two implementations — a filtered scan and a range descent — and they must return
/// the same records.
///
/// Asserted against `scan()` filtered in the test itself rather than against each other, so a bug
/// duplicated in both would still fail. The expected value comes from the full scan, never from
/// calling `scan_ids`.
#[test]
fn a_bounded_record_scan_returns_exactly_the_records_in_range() {
    for under in Under::BOTH {
        let (db, _live) = fixture(under);
        let cat = db.runtime.branches();
        let all: Vec<u64> = cat
            .scan()
            .unwrap()
            .map(|r| r.unwrap().branch_id.id)
            .collect();
        assert!(all.len() as u64 > BULK, "{under:?}: fixture holds only {} records", all.len());
        assert!(all.windows(2).all(|w| w[0] < w[1]), "{under:?}: scan is not in branch-id order");

        for (lo, hi) in [
            (0u64, 0u64),
            (3, 3),
            (0, u64::MAX),
            (10, 20),
            (190, 10_000),
            (1, 0),          // empty by inversion
            (100_000, 200_000), // empty because nothing is there
            (0, 1),
        ] {
            let want: Vec<u64> =
                all.iter().copied().filter(|id| *id >= lo && *id <= hi).collect();
            let got: Vec<u64> =
                cat.scan_ids(lo, hi).unwrap().map(|r| r.unwrap().branch_id.id).collect();
            assert_eq!(got, want, "{under:?}: scan_ids({lo}, {hi}) disagreed with a filtered scan");
        }

        // Anti-vacuity: the ranges above must not all be empty, or the loop proves nothing.
        assert_eq!(
            cat.scan_ids(10, 20).unwrap().count(),
            all.iter().filter(|id| (10u64..=20).contains(*id)).count(),
        );
        assert!(cat.scan_ids(10, 20).unwrap().count() > 0, "{under:?}: [10, 20] held no records");
    }
}

/// `ferro_runs` stopped enumerating branches to find its rows, so its row source changed. The rows
/// themselves must not have.
///
/// Checked against the shape the old code had — for every branch record, does a live run exist —
/// computed here from `run_of`, which is the function `runs_rows` used to call per record. The
/// expected value is built by a different route than the one under test.
#[test]
fn ferro_runs_reports_exactly_the_branches_with_a_live_run() {
    for under in Under::BOTH {
        let (db, live) = fixture(under);
        let want: Vec<u64> = db
            .runtime
            .branches()
            .scan()
            .unwrap()
            .filter_map(|r| {
                let rec = r.unwrap();
                db.runtime.run_of(rec.branch_id).map(|_| rec.branch_id.id)
            })
            .collect();
        assert_eq!(
            want.len(),
            live.len() + QUARANTINE_HOLDS_ITS_WORKSPACE,
            "{under:?}: the fixture holds {} live sessions but {} branches answer run_of",
            live.len(),
            want.len()
        );

        let rows = run_select(
            SystemView::Runs,
            &parse("SELECT branch_id FROM ferro_runs;"),
            &db.catalog,
            &db.runtime,
        )
        .expect("ferro_runs");
        let got: Vec<u64> = rows
            .rows
            .iter()
            .map(|r| match &r[0] {
                ferrodb::catalog::column::Value::BigInt(i) => *i as u64,
                other => panic!("branch_id came back as {other:?}"),
            })
            .collect();
        assert_eq!(got, want, "{under:?}: ferro_runs and run_of disagree about which branches have a run");
    }
}

/// A live run whose branch id sits between other branches must still be found, and the records
/// between the live ones must not become rows.
///
/// This is the merge cursor in `runs_rows`. A cursor that advanced wrongly would either drop the
/// last live run or emit a branch that has none, and with the sessions clustered at one end of the
/// id space neither mistake would show.
#[test]
fn a_scattered_set_of_live_runs_is_neither_widened_nor_truncated() {
    for under in Under::BOTH {
        let (db, live) = fixture(under);
        let rows = run_select(
            SystemView::Runs,
            &parse("SELECT branch_id, agent_id FROM ferro_runs;"),
            &db.catalog,
            &db.runtime,
        )
        .expect("ferro_runs");
        assert_eq!(
            rows.len(),
            live.len() + QUARANTINE_HOLDS_ITS_WORKSPACE,
            "{under:?}: {:?}",
            rows.rows
        );

        let ids: Vec<i64> = rows
            .rows
            .iter()
            .map(|r| match &r[0] {
                ferrodb::catalog::column::Value::BigInt(i) => *i,
                other => panic!("{other:?}"),
            })
            .collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "{under:?}: not in branch-id order: {ids:?}");
        // The point of the fixture: they are spread out, not adjacent.
        assert!(
            ids.last().unwrap() - ids.first().unwrap() > 40,
            "{under:?}: the live runs are clustered at {ids:?}, so the cursor was never exercised"
        );
    }
}

/// `NamedRows` equality covers the column list too, so a narrowed generator that somehow produced a
/// differently-shaped row would be caught — but only if a projection is exercised.
#[test]
fn a_narrowed_read_still_announces_the_projections_columns() {
    let (db, _live) = fixture(Under::Table);
    let got: NamedRows = run_select(
        SystemView::Branches,
        &parse("SELECT state, generation FROM ferro_branches WHERE branch_id = 3;"),
        &db.catalog,
        &db.runtime,
    )
    .expect("ferro_branches");
    assert_eq!(got.header(), vec!["state".to_string(), "generation".to_string()]);
    assert_eq!(got.len(), 1, "{:?}", got.rows);
}
