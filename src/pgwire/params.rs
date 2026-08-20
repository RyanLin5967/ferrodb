//! Query parameters: `$1`, and how a value gets into a statement without ever becoming SQL text.
//!
//! ## Why the value is substituted into the *parsed* statement
//!
//! The obvious implementation is to splice the parameter's text into the SQL and parse the
//! result, quoting strings on the way in. It is also unsafe here, and not in the abstract:
//! `src/parser/scanner.rs` has no escape handling inside a string literal at all — `string()`
//! reads to the next `'` and stops — so the SQL-standard `''` doubling that every quoting routine
//! emits does not mean "one quote" to this scanner. It means "end of one string, start of
//! another". A parameter carrying `'; DROP TABLE t; --` would be quoted correctly by any textbook
//! routine and would still reach the parser as tokens.
//!
//! So no parameter value is ever scanned. `$n` is rewritten to a sentinel *identifier* before
//! parsing, and [`substitute`] then replaces that identifier node with a literal node carrying the
//! decoded value. A value can therefore only ever be a value; there is no path from a parameter's
//! bytes to the tokeniser.
//!
//! The literal is built in exactly the shape the parser would have produced for a typed-in
//! constant, which is what lets the binder's type-directed literal reading (`literal_for_column`,
//! the reason `WHERE ts = 1700000000000` binds as a TIMESTAMP rather than a BIGINT) apply to
//! parameters unchanged.

use crate::binder::binder::Scope;
use crate::catalog::catalog::Catalog;
use crate::parser::parser::{Expr, Stmt};
use crate::pgwire::types::{oid, oid_of_type, ParamLiteral};

/// The protocol counts parameters in an `int16`, so this is the ceiling a client can express.
const MAX_PARAMS: usize = 32_767;

/// The identifier a `$n` becomes before parsing. Deliberately ugly, and checked for below.
fn sentinel(n: usize) -> String {
    format!("__ferro_param_{n}__")
}

fn sentinel_index(name: &str) -> Option<usize> {
    name.strip_prefix("__ferro_param_")
        .and_then(|rest| rest.strip_suffix("__"))
        .and_then(|digits| digits.parse::<usize>().ok())
}

/// Rewrite every `$n` outside a string literal into a sentinel identifier.
///
/// Returns the rewritten SQL and the highest `n` seen — which is the parameter count, exactly as
/// Postgres defines it: `$1, $3` declares three parameters, the second one simply unused.
pub fn rewrite(sql: &str) -> Result<(String, usize), String> {
    // A statement that already contains the sentinel would have its own identifier replaced by a
    // parameter value. Refusing is the only safe answer, and it is unreachable for anyone not
    // trying: `__ferro_param_1__` is not a name a schema arrives with.
    if sql.contains("__ferro_param_") {
        return Err(
            "`__ferro_param_` is reserved by this server's parameter handling and cannot appear \
             in a statement"
                .into(),
        );
    }
    let mut out = String::with_capacity(sql.len());
    let mut max = 0usize;
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    // The scanner has exactly two quoting forms and no escapes, so tracking them is this short.
    // `'a''b'` is two adjacent strings to that scanner and the toggle below agrees with it: either
    // reading leaves the `$` inside quotes untouched, which is the property that matters.
    let mut in_string = false;
    let mut in_ident = false;
    while i < chars.len() {
        let c = chars[i];
        if in_string {
            out.push(c);
            if c == '\'' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if in_ident {
            out.push(c);
            if c == '"' {
                in_ident = false;
            }
            i += 1;
            continue;
        }
        match c {
            '\'' => {
                in_string = true;
                out.push(c);
                i += 1;
            }
            '"' => {
                in_ident = true;
                out.push(c);
                i += 1;
            }
            '$' if i + 1 < chars.len() && chars[i + 1].is_ascii_digit() => {
                let start = i + 1;
                let mut end = start;
                while end < chars.len() && chars[end].is_ascii_digit() {
                    end += 1;
                }
                let digits: String = chars[start..end].iter().collect();
                let n: usize = digits
                    .parse()
                    .map_err(|_| format!("parameter number `${digits}` is out of range"))?;
                if n == 0 {
                    return Err("there is no parameter $0; they are numbered from 1".into());
                }
                if n > MAX_PARAMS {
                    return Err(format!("parameter ${n} exceeds the protocol's limit of {MAX_PARAMS}"));
                }
                max = max.max(n);
                out.push_str(&sentinel(n));
                i = end;
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    if in_string {
        // Left for the scanner to report in its own words rather than pre-empted here; the point
        // of noticing is only that the `$` handling above stayed inside the quotes.
    }
    Ok((out, max))
}

/// Replace every sentinel identifier with the literal for that parameter.
///
/// Errors if the statement refers to a parameter the client did not bind, or if a sentinel appears
/// somewhere this walk does not reach — a parameter left in place would otherwise be executed as a
/// column reference and fail with "unknown column `__ferro_param_1__`", which says nothing.
pub fn substitute(stmt: &Stmt, params: &[ParamLiteral]) -> Result<Stmt, String> {
    let mut out = stmt.clone();
    let mut err = None;
    walk_exprs(&mut out, &mut |e| {
        if let Expr::ColumnRef { table: None, column } = e {
            if let Some(n) = sentinel_index(column) {
                match params.get(n - 1) {
                    Some(p) => {
                        *e = Expr::Literal { value_type: p.token, value: p.text.clone() };
                    }
                    None => {
                        err.get_or_insert(format!(
                            "the statement uses ${n} but only {} parameter{} {} bound",
                            params.len(),
                            if params.len() == 1 { "" } else { "s" },
                            if params.len() == 1 { "was" } else { "were" }
                        ));
                    }
                }
            }
        }
    });
    if let Some(e) = err {
        return Err(e);
    }
    if let Some(name) = first_sentinel(&out) {
        return Err(format!(
            "parameter `{name}` appears somewhere this server cannot substitute it; parameters are \
             supported in the value and predicate positions of SELECT, INSERT, UPDATE and DELETE"
        ));
    }
    Ok(out)
}

/// Any sentinel still standing after substitution, if there is one.
fn first_sentinel(stmt: &Stmt) -> Option<String> {
    let mut found = None;
    let mut copy = stmt.clone();
    walk_exprs(&mut copy, &mut |e| {
        if let Expr::ColumnRef { column, .. } = e {
            if sentinel_index(column).is_some() && found.is_none() {
                found = Some(column.clone());
            }
        }
    });
    // The statement kinds `walk_exprs` does not descend into carry no expressions at all, so a
    // sentinel in one of them can only be in a name — where it would have been a parse error.
    found
}

/// Visit every expression node of a statement, in place.
fn walk_exprs(stmt: &mut Stmt, f: &mut impl FnMut(&mut Expr)) {
    match stmt {
        Stmt::Select { columns, where_clause, joins, .. } => {
            for c in columns {
                walk_expr(c, f);
            }
            if let Some(w) = where_clause {
                walk_expr(w, f);
            }
            for j in joins {
                walk_expr(&mut j.on, f);
            }
        }
        Stmt::Insert { values, .. } => {
            for v in values {
                walk_expr(v, f);
            }
        }
        Stmt::Update { assignments, where_clause, .. } => {
            for (_, e) in assignments {
                walk_expr(e, f);
            }
            if let Some(w) = where_clause {
                walk_expr(w, f);
            }
        }
        Stmt::Delete { where_clause, .. } => {
            if let Some(w) = where_clause {
                walk_expr(w, f);
            }
        }
        Stmt::Explain(inner) => walk_exprs(inner, f),
        Stmt::Join { on, .. } => walk_expr(on, f),
        // DDL, transaction control and the agent-session statements carry no expressions.
        _ => {}
    }
}

fn walk_expr(expr: &mut Expr, f: &mut impl FnMut(&mut Expr)) {
    f(expr);
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            walk_expr(left, f);
            walk_expr(right, f);
        }
        Expr::UnaryOp { right, .. } => walk_expr(right, f),
        Expr::Grouping(inner) => walk_expr(inner, f),
        Expr::Literal { .. } | Expr::ColumnRef { .. } => {}
    }
}

/// Work out what type each parameter should arrive as.
///
/// A client cannot encode an argument until the server says what it expects, so
/// `ParameterDescription` is a promise this has to make *before* any value exists. There is no
/// type inference in this engine, so the types come from the one place that knows them: the
/// column each parameter is compared against or written into.
///
/// `declared` is what the client asked for in `Parse` — a client may pin a parameter's type, and
/// where it does, that wins. Postgres resolves anything left as 0 itself, which is what this does.
///
/// Anything unresolved falls back to `text`, because text is the one type every value has a
/// representation in; a wrong *guess* at a narrower type would be refused at Bind, and refusing a
/// statement that would have worked is worse than shipping a string.
pub fn infer_types(stmt: &Stmt, nparams: usize, declared: &[i32], catalog: &Catalog) -> Vec<i32> {
    let mut oids = vec![0i32; nparams];
    for (i, d) in declared.iter().enumerate() {
        if i < nparams {
            oids[i] = *d;
        }
    }
    infer_into(stmt, &mut oids, catalog);
    for o in oids.iter_mut() {
        if *o == 0 {
            *o = oid::TEXT;
        }
    }
    oids
}

fn infer_into(stmt: &Stmt, oids: &mut [i32], catalog: &Catalog) {
    match stmt {
        Stmt::Insert { table, values } => {
            let Some(entry) = catalog.get_table(table) else { return };
            for (i, v) in values.iter().enumerate() {
                let Some(col) = entry.schema.columns.get(i) else { continue };
                if let Some(n) = as_param(v) {
                    set(oids, n, oid_of_type(&col.data_type));
                }
            }
        }
        Stmt::Update { table, assignments, where_clause } => {
            let Some(entry) = catalog.get_table(table) else { return };
            for (name, e) in assignments {
                if let Some(n) = as_param(e) {
                    if let Some(col) = entry.schema.columns.iter().find(|c| &c.name == name) {
                        set(oids, n, oid_of_type(&col.data_type));
                    }
                }
            }
            if let (Some(w), Ok(scope)) = (where_clause, scope_for_table(catalog, table, None)) {
                infer_from_predicate(w, &scope, oids);
            }
        }
        Stmt::Delete { table, where_clause } => {
            if let (Some(w), Ok(scope)) = (where_clause, scope_for_table(catalog, table, None)) {
                infer_from_predicate(w, &scope, oids);
            }
        }
        Stmt::Select { from, where_clause, joins, columns } => {
            let mut scope = Scope::new();
            let add = |t: &crate::parser::parser::TableRef, scope: &mut Scope| {
                if let Some(entry) = catalog.get_table(&t.name) {
                    let qualifier = t.alias.clone().unwrap_or_else(|| t.name.clone());
                    let _ = scope.add_table(&qualifier, &entry.schema);
                }
            };
            add(from, &mut scope);
            for j in joins {
                add(&j.table, &mut scope);
            }
            if let Some(w) = where_clause {
                infer_from_predicate(w, &scope, oids);
            }
            for j in joins {
                infer_from_predicate(&j.on, &scope, oids);
            }
            for c in columns {
                infer_from_predicate(c, &scope, oids);
            }
        }
        Stmt::Explain(inner) => infer_into(inner, oids, catalog),
        _ => {}
    }
}

/// A comparison is the only place a predicate says anything about a parameter's type: one side
/// names a column, the other is the parameter, and the column's declared type is the answer.
fn infer_from_predicate(expr: &Expr, scope: &Scope, oids: &mut [i32]) {
    match expr {
        Expr::BinaryOp { left, operator: _, right } => {
            match (as_param(left), column_type(right, scope)) {
                (Some(n), Some(t)) => set(oids, n, t),
                _ => {}
            }
            match (as_param(right), column_type(left, scope)) {
                (Some(n), Some(t)) => set(oids, n, t),
                _ => {}
            }
            infer_from_predicate(left, scope, oids);
            infer_from_predicate(right, scope, oids);
        }
        Expr::UnaryOp { right, .. } => infer_from_predicate(right, scope, oids),
        Expr::Grouping(inner) => infer_from_predicate(inner, scope, oids),
        _ => {}
    }
}

fn column_type(expr: &Expr, scope: &Scope) -> Option<i32> {
    match unwrap_grouping(expr) {
        Expr::ColumnRef { table, column } if sentinel_index(column).is_none() => scope
            .resolve(table.as_deref(), column)
            .ok()
            .map(|i| oid_of_type(&scope.columns[i].data_type)),
        _ => None,
    }
}

fn as_param(expr: &Expr) -> Option<usize> {
    match unwrap_grouping(expr) {
        Expr::ColumnRef { table: None, column } => sentinel_index(column),
        _ => None,
    }
}

fn unwrap_grouping(expr: &Expr) -> &Expr {
    match expr {
        Expr::Grouping(inner) => unwrap_grouping(inner),
        other => other,
    }
}

/// Only fills a slot that is still unresolved, so a type the client pinned in `Parse` is never
/// overwritten by a guess made here.
fn set(oids: &mut [i32], n: usize, oid: i32) {
    if let Some(slot) = oids.get_mut(n - 1) {
        if *slot == 0 {
            *slot = oid;
        }
    }
}

pub fn scope_for_table(
    catalog: &Catalog,
    table: &str,
    alias: Option<&str>,
) -> Result<Scope, crate::error::FerroError> {
    let entry = catalog.require_table(table)?;
    let mut scope = Scope::new();
    scope.add_table(alias.unwrap_or(table), &entry.schema)?;
    Ok(scope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parser::Parser;
    use crate::parser::scanner::{Scanner, TokenType};

    fn parse(sql: &str) -> Stmt {
        let (rewritten, _) = rewrite(sql).unwrap();
        let tokens = Scanner::new(rewritten.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "{sql} -> {:?}", p.errors);
        stmts.remove(0)
    }

    #[test]
    fn placeholders_become_identifiers_and_are_counted() {
        let (sql, n) = rewrite("SELECT * FROM t WHERE a = $1 AND b = $2;").unwrap();
        assert_eq!(n, 2);
        assert!(sql.contains("__ferro_param_1__") && sql.contains("__ferro_param_2__"));
        assert!(!sql.contains('$'));
    }

    /// Breaking shape: `$10`. Reading one digit turns it into `$1` followed by a literal `0`,
    /// which parses cleanly and binds the wrong argument — a wrong answer, not an error.
    #[test]
    fn a_two_digit_parameter_is_one_parameter() {
        let (sql, n) = rewrite("SELECT * FROM t WHERE a = $10;").unwrap();
        assert_eq!(n, 10);
        assert!(sql.contains("__ferro_param_10__"), "{sql}");
        assert!(!sql.contains("__ferro_param_1__0"), "{sql}");
    }

    /// Breaking shape: a `$1` inside a string literal. Substituting it would rewrite the user's
    /// data, and the count would claim a parameter the client is not going to send.
    #[test]
    fn a_dollar_inside_a_string_literal_is_data() {
        let (sql, n) = rewrite("INSERT INTO t VALUES ('costs $1 today', $1);").unwrap();
        assert_eq!(n, 1, "only the one outside the quotes is a parameter");
        assert!(sql.contains("'costs $1 today'"), "{sql}");
        assert!(sql.contains("__ferro_param_1__"), "{sql}");
    }

    #[test]
    fn a_quoted_identifier_is_not_scanned_for_parameters() {
        let (sql, n) = rewrite(r#"SELECT "weird$1col" FROM t;"#).unwrap();
        assert_eq!(n, 0);
        assert!(sql.contains(r#""weird$1col""#), "{sql}");
    }

    #[test]
    fn zero_and_out_of_range_parameter_numbers_are_refused() {
        assert!(rewrite("SELECT $0;").is_err());
        assert!(rewrite("SELECT $99999;").is_err());
        assert!(
            rewrite("SELECT __ferro_param_1__ FROM t;").is_err(),
            "a statement that already contains the sentinel must be refused, not rewritten"
        );
    }

    #[test]
    fn a_bare_dollar_is_left_for_the_scanner_to_complain_about() {
        let (sql, n) = rewrite("SELECT $ FROM t;").unwrap();
        assert_eq!(n, 0);
        assert!(sql.contains('$'));
    }

    #[test]
    fn substitution_puts_a_literal_where_the_parameter_was() {
        let stmt = parse("SELECT * FROM t WHERE id = $1;");
        let out = substitute(
            &stmt,
            &[ParamLiteral { token: TokenType::Number, text: "7".into() }],
        )
        .unwrap();
        match out {
            Stmt::Select { where_clause: Some(Expr::BinaryOp { right, .. }), .. } => match *right {
                Expr::Literal { value_type, value } => {
                    assert_eq!(value_type, TokenType::Number);
                    assert_eq!(value, "7");
                }
                other => panic!("expected a literal, got {other:?}"),
            },
            other => panic!("expected a select with a predicate, got {other:?}"),
        }
    }

    /// The breaking shape this whole module exists for: a text parameter that is itself SQL.
    /// It must arrive as one string value, and must not tokenise.
    #[test]
    fn a_parameter_carrying_sql_stays_one_value() {
        let stmt = parse("INSERT INTO t VALUES (1, $1);");
        let hostile = "'; DROP TABLE t; --";
        let out = substitute(
            &stmt,
            &[ParamLiteral { token: TokenType::String, text: hostile.into() }],
        )
        .unwrap();
        match out {
            Stmt::Insert { values, .. } => {
                assert_eq!(values.len(), 2, "the statement still has exactly two values");
                match &values[1] {
                    Expr::Literal { value_type, value } => {
                        assert_eq!(*value_type, TokenType::String);
                        assert_eq!(value, hostile, "the value is intact and is still a value");
                    }
                    other => panic!("expected a literal, got {other:?}"),
                }
            }
            other => panic!("expected an insert, got {other:?}"),
        }
    }

    #[test]
    fn a_parameter_the_client_did_not_bind_is_refused() {
        let stmt = parse("SELECT * FROM t WHERE id = $2;");
        let e = substitute(&stmt, &[ParamLiteral { token: TokenType::Number, text: "1".into() }])
            .unwrap_err();
        assert!(e.contains("$2"), "{e}");
    }

    #[test]
    fn substitution_reaches_every_expression_position() {
        for sql in [
            "SELECT $1 FROM t;",
            "SELECT * FROM t WHERE a = $1;",
            "UPDATE t SET a = $1 WHERE b = 2;",
            "UPDATE t SET a = 1 WHERE b = $1;",
            "DELETE FROM t WHERE a = $1;",
            "INSERT INTO t VALUES ($1);",
            "EXPLAIN SELECT * FROM t WHERE a = $1;",
            "SELECT * FROM t WHERE a = ($1);",
            "SELECT * FROM t WHERE NOT a = $1;",
            "SELECT * FROM t WHERE a = $1 AND b = $1;",
        ] {
            let stmt = parse(sql);
            let out = substitute(
                &stmt,
                &[ParamLiteral { token: TokenType::Number, text: "1".into() }],
            )
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert!(first_sentinel(&out).is_none(), "{sql} left a parameter unsubstituted");
        }
    }
}
