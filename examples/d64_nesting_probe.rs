//! D64 — does a deeply nested SQL expression recurse past the stack?
//!
//! Usage: `d64_nesting_probe <depth> [stage]`, stage = `scan` | `parse` (default `parse`).
//!
//! **One depth per PROCESS, on purpose.** A stack overflow aborts the process, so a loop inside
//! one binary would report only the first depth that dies and nothing after it. The caller runs
//! this once per depth and reads the exit status, which makes the threshold a measurement rather
//! than an inference.
//!
//! The premise being tested is that `parser::not`/`unary`, `binder::bind_expr` and
//! `executor::evaluate` recurse once per level of nesting with no depth limit anywhere, while
//! `tel::log`'s `MAX_GUARD_DEPTH = 256` guards the same shape for records read off a disk. If this
//! probe exits cleanly at every depth, the premise is WRONG and D64 gets closed saying so.
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;

fn main() {
    // `thread` mode runs the parse on a DEFAULT-STACK spawned thread, which is what
    // pgwire/mod.rs:290 gives every connection (std::thread::spawn, no stack_size anywhere in
    // src/). The main thread's 8 MB is NOT the server's shape and measuring there overstates
    // what survives by roughly 4x.
    if std::env::args().nth(2).as_deref() == Some("thread") {
        let d: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
        let h = std::thread::spawn(move || run(d, "parse"));
        h.join().unwrap();
        return;
    }
    let depth: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
    let stage = std::env::args().nth(2).unwrap_or_else(|| "parse".to_string());
    run(depth, &stage);
}

fn run(depth: usize, stage: &str) {

    // Three SHAPES, because the parser has three distinct ways to add a level and a guard that
    // covers one of them is walked around by the other two:
    //   parens -> Parser::expression (grouping)
    //   not    -> Parser::not,   which recurses into ITSELF
    //   neg    -> Parser::unary, which recurses into ITSELF
    let shape = std::env::var("D64_SHAPE").unwrap_or_else(|_| "parens".to_string());
    let mut sql = String::with_capacity(depth * 6 + 24);
    sql.push_str("SELECT ");
    match shape.as_str() {
        "not" => {
            for _ in 0..depth {
                sql.push_str("NOT ");
            }
            sql.push('1');
        }
        "neg" => {
            for _ in 0..depth {
                sql.push_str("- ");
            }
            sql.push('1');
        }
        _ => {
            for _ in 0..depth {
                sql.push('(');
            }
            sql.push('1');
            for _ in 0..depth {
                sql.push(')');
            }
        }
    }
    sql.push_str(" FROM t;");

    let tokens = match Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens() {
        Ok(t) => t,
        Err(e) => {
            println!("depth={depth} stage=scan REFUSED_CLEANLY: {e}");
            return;
        }
    };
    if stage == "scan" {
        println!("depth={depth} stage=scan OK ({} tokens)", tokens.len());
        return;
    }

    let mut p = Parser::new(tokens);
    let stmts = p.parse();
    if !p.errors.is_empty() {
        println!("depth={depth} stage=parse REFUSED_CLEANLY: {} error(s): {:?}", p.errors.len(),
                 p.errors.first().map(|e| format!("{e}")).unwrap_or_default());
        return;
    }
    println!("depth={depth} stage=parse OK ({} stmt)", stmts.len());
}
