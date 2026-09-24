//! D255 — an INNER JOIN applied an ON conjunct only when it linked two relations.
//!
//! `optimizer::search_algorithm::reorder_inner_joins` splits every ON into conjuncts and tags each
//! with the bitmask of relations it reads. A conjunct was then used in exactly one way: as a
//! `bridge` on a split `(l, r)`, which needs its mask to meet BOTH sides. A one-bit mask cannot meet
//! two disjoint sides and a zero mask meets nothing, so:
//!
//! * `a JOIN b ON a.id = b.id AND b.v = 5` ignored `b.v = 5` — at every size. A WRONG ANSWER.
//! * `ON a.id = b.id AND 1 = 0` ignored `1 = 0` and returned every match.
//! * At 12 relations or fewer, a query no bridged split could cover was refused ("disconnected
//!   join graph"): `ON 1 = 1`, `ON TRUE`, `ON b.v = 5`, and a join linked only by a conjunct over
//!   three relations. Past 12 the left-deep fallback planned the same queries as cross products,
//!   so whether a query ran depended on how many tables it joined.
//!
//! The parser has no `CROSS JOIN` and no comma FROM list, so `ON TRUE` is the only way to write a
//! cross product here; refusing it made a cross product unwritable below 13 relations.
//!
//! Every assertion is on the ANSWER, compared as a sorted row set, except where a test says it is
//! pinning a plan and why. `tests/d64_expression_depth.rs` also writes `ON 1` and `ON 1 = 1`, but it
//! only parses — it never reaches the planner — so it is not a control for any of this. The
//! planner-level positive controls are the two tests marked POSITIVE CONTROL, which pass at
//! `9aa6968` and must keep passing.
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::search_algorithm::MAX_DP_RELATIONS;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// d66's fixture, copied rather than shared: each integration test file is its own crate.
struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    s: Session,
    _dir: tempfile::TempDir,
}

type Rows = Vec<Vec<Option<i32>>>;

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("p.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, s: Session::new(), _dir: dir }
    }

    fn exec(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(format!("{:?}", parser.errors)));
        }
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.s)
    }

    /// `Outcome` has no `Debug`, so nothing here can `unwrap` or `expect` a `Result<Outcome, _>`.
    fn ok(&mut self, sql: &str) -> Outcome {
        self.exec(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    /// The answer as a SORTED row set, so the assertion is on which rows came back and not on the
    /// order a plan happened to produce them in. `None` is SQL NULL.
    fn rows(&mut self, sql: &str) -> Rows {
        let mut rows: Rows = match self.ok(sql) {
            Outcome::Rows(rows) => rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|v| match v {
                            Value::Integer(i) => Some(i),
                            Value::Null => None,
                            other => panic!("`{sql}`: this file only stores INTEGERs, got {other:?}"),
                        })
                        .collect()
                })
                .collect(),
            _ => panic!("`{sql}`: expected Outcome::Rows"),
        };
        rows.sort();
        rows
    }

    fn explain(&mut self, sql: &str) -> String {
        match self.ok(sql) {
            Outcome::Explain(text) => text,
            _ => panic!("`{sql}`: expected Outcome::Explain"),
        }
    }

    fn table(&mut self, name: &str, rows: &[(i32, i32)]) {
        self.ok(&format!("CREATE TABLE {name} (id INTEGER NOT NULL, v INTEGER);"));
        for (id, v) in rows {
            self.ok(&format!("INSERT INTO {name} VALUES ({id}, {v});"));
        }
    }
}

fn r(rows: &[&[i32]]) -> Rows {
    let mut out: Rows = rows.iter().map(|row| row.iter().map(|&v| Some(v)).collect()).collect();
    out.sort();
    out
}

/// a = (1,10) (2,20) (3,30); b = (1,5) (2,6) (3,5). Every id matches exactly one row across.
fn ab() -> Db {
    let mut db = Db::new();
    db.table("a", &[(1, 10), (2, 20), (3, 30)]);
    db.table("b", &[(1, 5), (2, 6), (3, 5)]);
    db
}

/// Every (a.id, b.id) pair: the cross product of `ab()`.
fn all_ab_pairs() -> Rows {
    let mut out = Rows::new();
    for x in 1..=3 {
        for y in 1..=3 {
            out.push(vec![Some(x), Some(y)]);
        }
    }
    out.sort();
    out
}

/// `n` tables `r0..r{n-1}`, each holding (1,10) and (2,20).
///
/// TWO rows each, on purpose: with one row per table (d66's fixture) a cross product is one row and
/// a dropped predicate is invisible. Here a single dropped link doubles the answer.
fn chain_db(n: usize) -> Db {
    let mut db = Db::new();
    for i in 0..n {
        db.table(&format!("r{i}"), &[(1, 10), (2, 20)]);
    }
    db
}

/// `SELECT <cols> FROM r0 JOIN r1 ON <on(1)> ... JOIN r{n-1} ON <on(n-1)>;`
fn chain_sql(n: usize, cols: &str, on: impl Fn(usize) -> String) -> String {
    let mut sql = format!("SELECT {cols} FROM r0");
    for i in 1..n {
        sql.push_str(&format!(" JOIN r{i} ON {}", on(i)));
    }
    sql.push(';');
    sql
}

// ---- one-relation conjuncts --------------------------------------------------------------------

#[test]
fn a_one_relation_conjunct_on_the_joined_side_filters_the_join() {
    let mut db = ab();
    assert_eq!(
        db.rows("SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND b.v = 5;"),
        r(&[&[1, 1], &[3, 3]]),
        "`b.v = 5` reads only b; it was dropped, so b's row with v = 6 joined too"
    );
}

#[test]
fn a_one_relation_conjunct_on_the_accumulated_side_filters_the_join() {
    let mut db = ab();
    assert_eq!(
        db.rows("SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND a.v = 20;"),
        r(&[&[2, 2]]),
        "`a.v = 20` reads only a; it was dropped"
    );
    // The same conjuncts in the other order: placement is by the relations a conjunct reads, not
    // by where it was typed.
    assert_eq!(
        db.rows("SELECT a.id, b.id FROM a JOIN b ON a.v = 20 AND a.id = b.id;"),
        r(&[&[2, 2]]),
    );
}

// ---- zero-relation conjuncts -------------------------------------------------------------------

#[test]
fn a_zero_relation_conjunct_filters_the_whole_join() {
    let mut db = ab();
    // Premise, and it passes at 9aa6968: the join itself is reached and answers. So a failure
    // below is the `1 = 0`, not a broken fixture.
    assert_eq!(
        db.rows("SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND 1 = 1;"),
        r(&[&[1, 1], &[2, 2], &[3, 3]]),
    );
    assert_eq!(
        db.rows("SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND 1 = 0;"),
        Rows::new(),
        "`1 = 0` reads no relation; it was dropped and every match came back"
    );
}

#[test]
fn on_one_equals_zero_alone_is_an_empty_join_not_a_refusal() {
    let mut db = ab();
    assert_eq!(db.rows("SELECT a.id, b.id FROM a JOIN b ON 1 = 0;"), Rows::new());
}

// ---- no linking conjunct: a cross product, not a refusal -------------------------------------

#[test]
fn a_join_with_no_linking_conjunct_is_a_cross_product() {
    let mut db = ab();
    assert_eq!(db.rows("SELECT a.id, b.id FROM a JOIN b ON 1 = 1;"), all_ab_pairs());
    assert_eq!(db.rows("SELECT a.id, b.id FROM a JOIN b ON TRUE;"), all_ab_pairs());
    // A one-relation conjunct and nothing linking: b filtered, then crossed with a.
    let filtered: Rows = all_ab_pairs().into_iter().filter(|p| p[1] != Some(2)).collect();
    assert_eq!(filtered.len(), 6, "fixture: b holds v = 5 at ids 1 and 3");
    assert_eq!(db.rows("SELECT a.id, b.id FROM a JOIN b ON b.v = 5;"), filtered);
}

/// `Filter`, `NestedLoopJoin` and `HashJoin` all keep a row only when its predicate is
/// `Boolean(true)`, so a bare `1` keeps nothing, in WHERE and in ON alike. The law pinned is that
/// `a JOIN b ON p` answers exactly as `a JOIN b ON TRUE WHERE p` does.
///
/// ⚠ Past 12 relations this CHANGES an answer that planned at 9aa6968: the `1` was dropped there and
/// the cross product came back.
#[test]
fn a_non_boolean_on_answers_like_the_same_where() {
    let mut db = ab();
    // Premise: the cross product itself answers, so the equality below is not two refusals, or two
    // empty results for an unrelated reason.
    assert_eq!(db.rows("SELECT a.id, b.id FROM a JOIN b ON TRUE;"), all_ab_pairs());
    let on = db.rows("SELECT a.id, b.id FROM a JOIN b ON 1;");
    let where_ = db.rows("SELECT a.id, b.id FROM a JOIN b ON TRUE WHERE 1;");
    assert_eq!(on, where_, "ON 1 and WHERE 1 must be the same predicate");
    assert_eq!(on, Rows::new());
}

// ---- the same shapes on both sides of MAX_DP_RELATIONS --------------------------------------------

/// The exhaustive DP runs at `MAX_DP_RELATIONS` relations and the left-deep fallback one past it.
/// Each conjunct goes on the LAST join, so on the left-deep path the one-relation conjunct is on the
/// relation being joined (`r{n-1}`) in one arm and on the accumulated side (`r0`) in the other.
#[test]
fn the_same_conjuncts_filter_at_and_past_the_dp_limit() {
    for n in [MAX_DP_RELATIONS, MAX_DP_RELATIONS + 1] {
        let mut db = chain_db(n);
        let last = n - 1;
        let with = |extra: &str| {
            chain_sql(n, "r0.id", |i| {
                if i == last {
                    format!("r0.id = r{i}.id {extra}")
                } else {
                    format!("r0.id = r{i}.id")
                }
            })
        };
        // Premise, passing at 9aa6968: the whole chain joins, two rows.
        assert_eq!(db.rows(&with("")), r(&[&[1], &[2]]), "n = {n}: premise");
        assert_eq!(
            db.rows(&with(&format!("AND r{last}.v = 20"))),
            r(&[&[2]]),
            "n = {n}: a conjunct on the joined relation alone was dropped"
        );
        assert_eq!(
            db.rows(&with("AND r0.v = 10")),
            r(&[&[1]]),
            "n = {n}: a conjunct on the accumulated side alone was dropped"
        );
        assert_eq!(db.rows(&with("AND 1 = 0")), Rows::new(), "n = {n}: `1 = 0` was dropped");
    }
}

/// r1 is joined `ON 1 = 1` and linked to nothing; every other relation links to r0.
fn crossed_chain(n: usize) -> String {
    chain_sql(n, "r0.id, r1.id", |i| if i == 1 { "1 = 1".into() } else { format!("r0.id = r{i}.id") })
}

/// POSITIVE CONTROL — passes at 9aa6968 and must keep passing. The left-deep path always planned a
/// cross product, with `true`, where no conjunct linked the next relation.
#[test]
fn past_the_dp_limit_a_cross_joined_relation_still_joins() {
    let n = MAX_DP_RELATIONS + 1;
    let mut db = chain_db(n);
    assert_eq!(db.rows(&crossed_chain(n)), r(&[&[1, 1], &[1, 2], &[2, 1], &[2, 2]]));
}

/// The same query one relation smaller, on the DP path, where 9aa6968 refused it.
#[test]
fn at_the_dp_limit_a_cross_joined_relation_joins_the_same_way() {
    let n = MAX_DP_RELATIONS;
    let mut db = chain_db(n);
    assert_eq!(db.rows(&crossed_chain(n)), r(&[&[1, 1], &[1, 2], &[2, 1], &[2, 2]]));
}

/// Connected, but only by a conjunct over THREE relations: no pair is ever linked, so no split of
/// two relations has a bridge. The DP must cross two of them first and apply the conjunct at the
/// third.
#[test]
fn a_conjunct_over_three_relations_is_applied_when_no_pair_is_linked() {
    let mut db = ab();
    db.table("c", &[(1, 15), (2, 26)]);
    // a.v + b.v over all nine pairs: 15 16 15 / 25 26 25 / 35 36 35. c.v = 15 matches (a1,b1) and
    // (a1,b3); c.v = 26 matches (a2,b2).
    assert_eq!(
        db.rows("SELECT a.id, b.id, c.id FROM a JOIN b ON 1 = 1 JOIN c ON a.v + b.v = c.v;"),
        r(&[&[1, 1, 1], &[1, 3, 1], &[2, 2, 2]]),
    );
}

/// Two components, each linked inside and not to the other. The cheapest plan joins each component
/// and crosses the two results last; this pins that the search can SEE that plan.
///
/// This is the one assertion here on a plan rather than an answer, and it rests on the cost model,
/// worked by hand from `cost_model.rs` (50 rows a table, ids unique, ANALYZEd): the bushy root
/// `(w⋈x)×(y⋈z)` costs 4.5 + 4.5 + 25 = 34, and every root that is a join on a predicate costs 83
/// (`((w⋈x)×y)⋈z` = 31 + 1.5 + 50.5). A search that only crosses when a whole level is empty never
/// builds the bushy root.
#[test]
fn two_unlinked_components_are_joined_first_and_crossed_last() {
    let mut db = Db::new();
    let rows: Vec<(i32, i32)> = (0..50).map(|i| (i, i)).collect();
    for t in ["w", "x", "y", "z"] {
        db.table(t, &rows);
        db.ok(&format!("ANALYZE {t};"));
    }
    let q = "SELECT w.id, y.id FROM w JOIN x ON w.id = x.id JOIN y ON TRUE JOIN z ON y.id = z.id;";
    let mut every_pair = Rows::new();
    for i in 0..50 {
        for j in 0..50 {
            every_pair.push(vec![Some(i), Some(j)]);
        }
    }
    every_pair.sort();
    assert_eq!(db.rows(q), every_pair);

    let plan = db.explain(&format!("EXPLAIN {q}"));
    let root_join = plan
        .lines()
        .find(|l| l.contains(" join "))
        .unwrap_or_else(|| panic!("no join in the plan:\n{plan}"));
    assert!(
        root_join.contains("(on true)"),
        "the root join should be the cross product of the two components:\n{plan}"
    );
}

/// A one-relation ON conjunct is a filter on that relation BEFORE its access path is chosen, so a
/// primary-key equality in ON reaches the index exactly as it does in WHERE. D56: a unique-key
/// equality wins over a sequential scan at every table width, statistics or not.
#[test]
fn a_one_relation_conjunct_reaches_index_selection() {
    let mut db = ab();
    let q = "SELECT a.id FROM a JOIN b ON a.id = b.id AND b.id = 3;";
    assert_eq!(db.rows(q), r(&[&[3]]));
    let plan = db.explain(&format!("EXPLAIN {q}"));
    assert!(plan.contains("Index scan on b"), "`b.id = 3` did not reach b's access path:\n{plan}");

    // With a WHERE conjunct on b as well, pushed down onto b's scan first: the two must become ONE
    // filter, or the planner sees a filter over a filter and never considers the index.
    let q = "SELECT a.id FROM a JOIN b ON a.id = b.id AND b.id = 3 WHERE b.v > 0;";
    assert_eq!(db.rows(q), r(&[&[3]]));
    let plan = db.explain(&format!("EXPLAIN {q}"));
    assert!(plan.contains("Index scan on b"), "the ON and WHERE filters on b were not merged:\n{plan}");
}

/// The leaf a one-relation conjunct reads can be a LEFT JOIN. The filter goes OVER it: pushed into
/// the nullable side it would stop excluding the NULL-extended row, which is (2, NULL, 2) here.
#[test]
fn a_one_relation_conjunct_over_a_left_join_filters_after_the_left_join() {
    let mut db = Db::new();
    db.table("la", &[(1, 0), (2, 0)]);
    db.table("lb", &[(1, 5), (2, 6)]);
    db.table("lc", &[(1, 0), (2, 0)]);
    assert_eq!(
        db.rows(
            "SELECT la.id, lb.id, lc.id FROM la LEFT JOIN lb ON la.id = lb.id \
             JOIN lc ON la.id = lc.id AND lb.v = 5;"
        ),
        r(&[&[1, 1, 1]]),
    );
}

/// POSITIVE CONTROL — passes at 9aa6968 and must keep passing. The left-deep path at d66's width
/// (relation 32 is past `u32`), with two rows a table, so that a dropped or aliased link would double
/// the answer instead of hiding in a one-row cross product.
#[test]
fn thirty_three_two_row_relations_chain_to_exactly_two_rows() {
    let n = 33;
    let mut db = chain_db(n);
    assert_eq!(db.rows(&chain_sql(n, "r0.id", |i| format!("r0.id = r{i}.id"))), r(&[&[1], &[2]]));
}
