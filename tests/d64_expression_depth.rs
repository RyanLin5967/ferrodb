//! D64 — a client cannot nest an expression deep enough to take the server down.
//!
//! ⚠ These tests CANNOT prove the bug, and must not be read as though they did. The failure they
//! guard against is a `SIGABRT`, which would take the whole test process with it rather than fail
//! an assertion — a test that reproduced it would destroy every other test's result. The proof is
//! in `bench/d64_nesting_probe.txt`, measured one depth per process: before the guard, release
//! aborted at 8,000 levels and debug at 1,000.
//!
//! What these tests DO pin is the boundary and the refusal, which is what a regression would move.
use ferrodb::parser::parser::{Parser, MAX_EXPR_DEPTH};
use ferrodb::parser::scanner::Scanner;

/// The three shapes that each add a level of tree depth by a DIFFERENT recursion. `Not` and `Neg`
/// recurse into themselves and never re-enter `Parser::expression`, so a guard kept only there does
/// not see them — which is exactly how the first version of this guard was walked around.
#[derive(Clone, Copy)]
enum Shape {
    Parens,
    Not,
    Neg,
}

/// The paren shape, which is the one the original probe measured.
fn parse_nested(levels: usize) -> Result<usize, String> {
    parse_nested_shape(levels, Shape::Parens)
}

fn parse_nested_shape(levels: usize, shape: Shape) -> Result<usize, String> {
    let mut sql = String::with_capacity(levels * 6 + 24);
    sql.push_str("SELECT ");
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
        Shape::Parens => {
            for _ in 0..levels {
                sql.push('(');
            }
            sql.push('1');
            for _ in 0..levels {
                sql.push(')');
            }
        }
    }
    sql.push_str(" FROM t;");
    let tokens = Scanner::new(sql.chars().collect(), Vec::new())
        .scan_tokens()
        .map_err(|e| e.to_string())?;
    let mut p = Parser::new(tokens);
    let stmts = p.parse();
    match p.errors.first() {
        Some(e) => Err(e.to_string()),
        None => Ok(stmts.len()),
    }
}

#[test]
fn an_expression_just_under_the_limit_still_parses() {
    // One `expression()` frame is spent on the top-level expression itself, so `MAX_EXPR_DEPTH`
    // frames means `MAX_EXPR_DEPTH - 1` nested parentheses. Pinning the boundary from BOTH sides
    // is the point: a guard that refuses everything would pass a one-sided test.
    assert_eq!(parse_nested(MAX_EXPR_DEPTH - 1), Ok(1));
}

#[test]
fn an_expression_past_the_limit_is_refused_and_says_so() {
    let err = parse_nested(MAX_EXPR_DEPTH).expect_err("must refuse");
    assert!(
        err.contains("nests deeper") && err.contains(&MAX_EXPR_DEPTH.to_string()),
        "the refusal must name the limit so a client can act on it; got: {err}"
    );
}

#[test]
fn a_depth_that_used_to_abort_the_process_is_now_just_an_error() {
    // 2,000 is where a 2 MiB connection thread aborted in release with no guard; 50,000 is where
    // the self-recursive NOT/unary shapes did. See bench/d64_nesting_probe.txt.
    for shape in [Shape::Parens, Shape::Not, Shape::Neg] {
        for levels in [2_000, 50_000] {
            let err = parse_nested_shape(levels, shape).expect_err("must refuse");
            assert!(err.contains("nests deeper"), "at {levels} levels, got: {err}");
        }
    }
}

/// The guard must cover every recursion that adds a level, not just the one in `expression`.
#[test]
fn every_recursion_shape_is_bounded_at_the_same_depth() {
    for shape in [Shape::Parens, Shape::Not, Shape::Neg] {
        assert_eq!(
            parse_nested_shape(MAX_EXPR_DEPTH - 1, shape),
            Ok(1),
            "one level under the limit must still parse"
        );
        let err = parse_nested_shape(MAX_EXPR_DEPTH, shape).expect_err("must refuse at the limit");
        assert!(err.contains("nests deeper"), "got: {err}");
    }
}
