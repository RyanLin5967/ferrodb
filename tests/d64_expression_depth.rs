//! D64 / D64b — a client cannot nest or chain its way into taking the server down.
//!
//! ⚠ These tests CANNOT reproduce the bug and must not be read as though they did. The failure is
//! a `SIGABRT`, which would take the whole test process with it rather than fail an assertion — a
//! test that reproduced it would destroy every other test's result. The proof is in
//! `bench/d64_nesting_probe.txt`, measured one depth per process on a 2 MiB thread.
//!
//! What these tests pin is the BOUNDARY, the REFUSAL, and that a shape just under the limit still
//! produces a real parse tree. The boundary numbers below are measured, not derived: see the
//! comment on `PARENS_MAX`.
use ferrodb::parser::parser::{Parser, Stmt, MAX_EXPR_DEPTH, MAX_TREE_DEPTH};
use ferrodb::parser::scanner::Scanner;

/// The four shapes that reach the parser by DIFFERENT routes. Each was, at some point in D64's
/// history, a shape an earlier version of the guard did not see.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// `((((1))))` — re-enters `Parser::expression`.
    Parens,
    /// `NOT NOT NOT 1` — `Parser::not` recurses into itself.
    Not,
    /// `- - - 1` — `Parser::unary` recurses into itself.
    Neg,
    /// `EXPLAIN EXPLAIN … SELECT` — `parse_explain` re-enters `parse_statement`. STATEMENT-level
    /// recursion, which the first two versions of this guard did not charge at all.
    Explain,
    /// `1 + 1 + 1 + …` — parsed by a `while` loop, so the PARSER never recurses; the tree is
    /// left-deep and the recursion is in whatever walks or drops it.
    Chain,
}

fn sql_for(levels: usize, shape: Shape) -> String {
    let mut sql = String::with_capacity(levels * 8 + 32);
    match shape {
        Shape::Explain => {
            for _ in 0..levels {
                sql.push_str("EXPLAIN ");
            }
            sql.push_str("SELECT 1 FROM t;");
            return sql;
        }
        _ => sql.push_str("SELECT "),
    }
    match shape {
        Shape::Not => {
            for _ in 0..levels {
                sql.push_str("NOT ");
            }
            sql.push('1');
        }
        Shape::Neg => {
            for _ in 0..levels {
                sql.push_str("- ");
            }
            sql.push('1');
        }
        Shape::Chain => {
            sql.push('1');
            for _ in 0..levels {
                sql.push_str(" + 1");
            }
        }
        Shape::Parens => {
            for _ in 0..levels {
                sql.push('(');
            }
            sql.push('1');
            for _ in 0..levels {
                sql.push(')');
            }
        }
        Shape::Explain => unreachable!("handled above"),
    }
    sql.push_str(" FROM t;");
    sql
}

/// Returns the parsed statements, NOT a count.
///
/// ⚠ An earlier version of this helper returned `stmts.len()` and dropped the tree, so `Ok(1)`
/// pinned "no error, exactly one statement" and NOTHING about what was built — it would have
/// passed against a parser that returned an empty or wrong statement. Returning the tree is what
/// lets the assertions below check that a shape under the limit really parsed.
fn parse_shape(levels: usize, shape: Shape) -> Result<Vec<Stmt>, String> {
    let sql = sql_for(levels, shape);
    let tokens = Scanner::new(sql.chars().collect(), Vec::new())
        .scan_tokens()
        .map_err(|e| e.to_string())?;
    let mut p = Parser::new(tokens);
    let stmts = p.parse();
    match p.errors.first() {
        Some(e) => Err(e.to_string()),
        None => Ok(stmts),
    }
}

/// The deepest nesting each shape actually accepts, MEASURED against the built binary rather than
/// derived from the constant.
///
/// It is not `MAX_EXPR_DEPTH - 1`, and the difference is the point: `parse_statement` charges a
/// frame too (D64b), so a `SELECT` spends one level before its expression starts. A test that
/// computed these from the constant would encode the same arithmetic the code does and could not
/// catch an off-by-one in it.
///
/// ⚠ `EXPLAIN_MAX` coinciding with `PARENS_MAX` is a MEASURED fact, not an assumption. An earlier
/// version of this constant said 57, taken from a probe whose EXPLAIN generator appended a stray
/// second `FROM t` — so the boundary it found was that syntax error, not the guard. The probe was
/// fixed (`examples/d64_nesting_probe.rs`, `whole_statement`) and both shapes re-measured at 62.
const PARENS_MAX: usize = 62;
const EXPLAIN_MAX: usize = 62;

#[test]
fn a_shape_just_under_the_limit_parses_and_produces_a_real_statement() {
    for (shape, max) in [
        (Shape::Parens, PARENS_MAX),
        (Shape::Not, PARENS_MAX),
        (Shape::Neg, PARENS_MAX),
        (Shape::Explain, EXPLAIN_MAX),
        (Shape::Chain, MAX_TREE_DEPTH),
    ] {
        let stmts = parse_shape(max, shape)
            .unwrap_or_else(|e| panic!("{shape:?} at {max} should parse, got: {e}"));
        assert_eq!(stmts.len(), 1, "{shape:?} at {max} produced {} statements", stmts.len());
        // Not just "one statement": the RIGHT KIND of statement, so an empty or wrong parse
        // cannot satisfy this.
        match (shape, &stmts[0]) {
            (Shape::Explain, Stmt::Explain(_)) => {}
            (Shape::Explain, other) => panic!("EXPLAIN nesting produced {other:?}"),
            (_, Stmt::Select { .. }) => {}
            (_, other) => panic!("{shape:?} produced {other:?}, expected a Select"),
        }
    }
}

#[test]
fn one_level_past_the_limit_is_refused_and_the_message_names_the_limit() {
    for (shape, max, expect) in [
        (Shape::Parens, PARENS_MAX, "nests deeper"),
        (Shape::Not, PARENS_MAX, "nests deeper"),
        (Shape::Neg, PARENS_MAX, "nests deeper"),
        (Shape::Explain, EXPLAIN_MAX, "nests deeper"),
        (Shape::Chain, MAX_TREE_DEPTH, "chains more than"),
    ] {
        let err = parse_shape(max + 1, shape)
            .expect_err("{shape:?} one past the limit must be refused");
        assert!(
            err.contains(expect),
            "{shape:?} at {} refused with the wrong message: {err}",
            max + 1
        );
    }
}

/// The depths that ABORTED THE PROCESS before the guard existed, from bench/d64_nesting_probe.txt.
/// Each must now be an ordinary error.
#[test]
fn depths_that_used_to_abort_the_process_are_now_errors() {
    let cases = [
        (Shape::Parens, 2_000usize),
        (Shape::Not, 50_000),
        (Shape::Neg, 50_000),
        (Shape::Explain, 5_000),
        (Shape::Chain, 25_600),
    ];
    for (shape, levels) in cases {
        let err = parse_shape(levels, shape).expect_err("must refuse");
        assert!(
            err.contains("nests deeper") || err.contains("chains more than"),
            "{shape:?} at {levels}: {err}"
        );
    }
}

/// The two limits bound DIFFERENT quantities and must not be collapsed into one.
///
/// If a future change makes the binary loops charge the frame counter instead, a chain of 100
/// operators starts failing — which is legal SQL a generator emits — and this test says so.
#[test]
fn a_long_operator_chain_is_not_bounded_by_the_frame_limit() {
    let levels = MAX_EXPR_DEPTH * 4;
    assert!(levels < MAX_TREE_DEPTH, "fixture must sit between the two limits");
    let stmts = parse_shape(levels, Shape::Chain)
        .unwrap_or_else(|e| panic!("a {levels}-operator chain must still parse, got: {e}"));
    assert_eq!(stmts.len(), 1);
}

/// Neither counter may leak across statements in one batch.
///
/// ⚠ **Uses an OPERATOR shape on purpose.** The first version used nested parens, which charge the
/// FRAME counter and never touch the chain counter — so `parse`'s `self.chain = 0` reset, the
/// other line D64c added, was pinned by nothing and could be deleted silently. Each statement here
/// spends most of the tree budget, so a missing reset makes the second statement fail.
#[test]
fn neither_counter_leaks_across_statements_in_a_batch() {
    let per_statement = MAX_TREE_DEPTH - 1;
    let mut one = String::from("SELECT 1");
    for _ in 0..per_statement {
        one.push_str(" + 1");
    }
    one.push_str(" FROM t;");
    let many = one.repeat(20);
    assert!(
        per_statement * 20 > MAX_TREE_DEPTH,
        "the batch must exceed the budget in total, or a missing reset would not show"
    );

    let tokens = Scanner::new(many.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let stmts = p.parse();
    assert!(
        p.errors.is_empty(),
        "20 statements each just under the budget must all parse; first error: {:?}",
        p.errors.first().map(|e| e.to_string())
    );
    assert_eq!(stmts.len(), 20);

    // And the frame counter too, with the shape that charges it.
    let nested = sql_for(PARENS_MAX, Shape::Parens).repeat(20);
    let tokens = Scanner::new(nested.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let stmts = p.parse();
    assert!(p.errors.is_empty(), "frame counter leaked: {:?}", p.errors.first().map(|e| e.to_string()));
    assert_eq!(stmts.len(), 20);
}

/// **The hole three review passes took to find: COMPOSING two shapes.**
///
/// Every test above exercises one shape in isolation — `(((1)))` has no operators, `1 + 1 + 1` has
/// no parens — and that is exactly why they were all green while the guard was walked around. In
/// v3 the chain budget was released when an inner `expression()` returned, and every binary loop
/// parses its LEFT operand before charging, so a left-position `(` re-entered with the counter at
/// zero. Each nesting level got a fresh full budget while costing 1 against the frame limit, so the
/// two limits MULTIPLIED: 62 levels x 800 operators built a ~49,600-deep tree that parsed happily
/// and then aborted the process on a 2 MiB connection thread.
///
/// The budget is per-STATEMENT now, so nesting cannot renew it.
#[test]
fn nesting_cannot_renew_the_chain_budget() {
    // Well under both limits on their own: 8 parens (limit 64) and 400 operators (limit 1024).
    // Composed, v3 allowed 8 x 400 = 3,200 levels of tree. v4 must refuse.
    let nest = 8usize;
    let inner = 400usize;
    assert!(nest < MAX_EXPR_DEPTH, "fixture must be legal as pure nesting");
    assert!(inner < MAX_TREE_DEPTH, "each chain must be legal on its own");
    assert!(nest * inner > MAX_TREE_DEPTH, "composed, it must exceed the tree budget");

    let mut sql = String::from("SELECT ");
    for _ in 0..nest {
        sql.push('(');
    }
    sql.push('1');
    for _ in 0..nest {
        for _ in 0..inner {
            sql.push_str(" + 1");
        }
        sql.push(')');
    }
    sql.push_str(" FROM t;");

    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let _ = p.parse();
    let err = p
        .errors
        .first()
        .map(|e| e.to_string())
        .expect("composing nesting with chains must be refused, not multiplied");
    assert!(
        err.contains("chains more than"),
        "must be refused by the TREE budget specifically; got: {err}"
    );
}

/// Joins are charged too: the parser keeps them in a flat `Vec`, but the planner folds them into
/// an N-deep `LogicalPlan::Join` tree that several walkers descend recursively.
///
/// ⚠ **`ON 1`, NOT `ON 1 = 1`, and that is the whole test.** The first version of this used
/// `ON 1 = 1`, whose `=` spends a charge in `equality()` all by itself — so the budget ran out
/// either way and the test passed with the join charge DELETED. It could not tell the guard from
/// its absence. `ON 1` is a bare literal costing zero, so the only thing that can refuse this is
/// the charge in the join loop.
#[test]
fn a_join_list_longer_than_the_tree_budget_is_refused() {
    let mut sql = String::from("SELECT 1 FROM t0");
    for i in 1..=(MAX_TREE_DEPTH + 1) {
        sql.push_str(&format!(" JOIN t{i} ON 1"));
    }
    sql.push(';');
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let _ = p.parse();
    let err = p.errors.first().map(|e| e.to_string()).expect("must be refused");
    assert!(err.contains("chains more than"), "got: {err}");
}

/// The other side of that boundary: exactly the budget in zero-cost joins must still parse, so an
/// off-by-one in the join charge is caught as well as its deletion.
#[test]
fn exactly_the_tree_budget_in_zero_cost_joins_still_parses() {
    let mut sql = String::from("SELECT 1 FROM t0");
    for i in 1..=MAX_TREE_DEPTH {
        sql.push_str(&format!(" JOIN t{i} ON 1"));
    }
    sql.push(';');
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let stmts = p.parse();
    assert!(
        p.errors.is_empty(),
        "exactly {MAX_TREE_DEPTH} joins must parse; got {:?}",
        p.errors.first().map(|e| e.to_string())
    );
    assert_eq!(stmts.len(), 1);
}

/// ...and a join list a real query might contain still parses.
#[test]
fn an_ordinary_join_list_still_parses() {
    let mut sql = String::from("SELECT 1 FROM t0");
    for i in 1..=25 {
        sql.push_str(&format!(" JOIN t{i} ON 1 = 1"));
    }
    sql.push(';');
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let stmts = p.parse();
    assert!(p.errors.is_empty(), "25 joins must parse; got {:?}", p.errors.first().map(|e| e.to_string()));
    assert_eq!(stmts.len(), 1);
}
