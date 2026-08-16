//! Guards: the predicates that made a write legal.
//!
//! Design authority: DESIGN.md section 3 ("The single most important correction").
//!
//! The premise "you need an intent log because deltas can't be recovered from a byte WAL" is true
//! but for the wrong reason. Numeric deltas *are* recoverable from before/after images. What
//! genuinely cannot be reconstructed from any log of values is:
//!
//! - **the guard** — the `WHERE qty >= 5` that made the write legal
//! - reads that produced no write
//! - which algebra element was meant when two shapes produce identical images
//!
//! So the query layer must cooperate to capture **guards**, not deltas. Guards are first-class
//! log records, separate from ops. This is Bayou's dependency check (1995): a guard is a query
//! plus the answer it must still give.
//!
//! At merge time, a guard is re-evaluated against the **merged** state. This is also why bounded
//! counters need no special merge logic: compose the `Add`s, then re-check `qty >= 0`. If it now
//! fails the outcome is `Conflict`, and the violated predicate is returned so the agent can retry
//! with real feedback (exit criterion 7).

use std::fmt::{Display, Formatter};

use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::tel::ids::{ColId, RowId, TableId};

/// Comparison operators available inside a guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    pub fn apply(&self, l: &Value, r: &Value) -> bool {
        use std::cmp::Ordering;
        let o = l.cmp(r);
        match self {
            CmpOp::Eq => o == Ordering::Equal,
            CmpOp::Ne => o != Ordering::Equal,
            CmpOp::Lt => o == Ordering::Less,
            CmpOp::Le => o != Ordering::Greater,
            CmpOp::Gt => o == Ordering::Greater,
            CmpOp::Ge => o != Ordering::Less,
        }
    }
}

impl Display for CmpOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CmpOp::Eq => "=",
            CmpOp::Ne => "<>",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        })
    }
}

/// Arithmetic operators available inside a guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl Display for ArithOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
        })
    }
}

/// A self-contained, re-evaluable predicate expression.
///
/// Deliberately **not** `BoundExpr`: a `BoundExpr::Column(usize)` is an offset into one
/// particular plan's output row and means nothing once the branch it was bound in is gone. A
/// guard has to survive into a merge that happens later, on a different branch, against a state
/// nobody had at capture time, so every column reference names `(table, row, column)` explicitly.
#[derive(Debug, Clone, PartialEq)]
pub enum GuardExpr {
    Literal(Value),
    /// The value of one cell, read from whatever state the guard is being evaluated against.
    Col { tbl: TableId, row: RowId, col: ColId },
    Compare { left: Box<GuardExpr>, op: CmpOp, right: Box<GuardExpr> },
    Arith { left: Box<GuardExpr>, op: ArithOp, right: Box<GuardExpr> },
    And(Vec<GuardExpr>),
    Or(Vec<GuardExpr>),
    Not(Box<GuardExpr>),
    IsNull(Box<GuardExpr>),
}

impl GuardExpr {
    pub fn col(tbl: TableId, row: RowId, col: ColId) -> Self {
        GuardExpr::Col { tbl, row, col }
    }

    pub fn cmp(left: GuardExpr, op: CmpOp, right: GuardExpr) -> Self {
        GuardExpr::Compare { left: Box::new(left), op, right: Box::new(right) }
    }

    pub fn arith(left: GuardExpr, op: ArithOp, right: GuardExpr) -> Self {
        GuardExpr::Arith { left: Box::new(left), op, right: Box::new(right) }
    }

    /// Every cell this expression reads. The verification gate uses this to build the read half
    /// of `write-set \ read-set`.
    pub fn referenced_cells(&self) -> Vec<(TableId, RowId, ColId)> {
        let mut out = Vec::new();
        self.collect_cells(&mut out);
        out
    }

    fn collect_cells(&self, out: &mut Vec<(TableId, RowId, ColId)>) {
        match self {
            GuardExpr::Literal(_) => {}
            GuardExpr::Col { tbl, row, col } => out.push((*tbl, *row, *col)),
            GuardExpr::Compare { left, right, .. } | GuardExpr::Arith { left, right, .. } => {
                left.collect_cells(out);
                right.collect_cells(out);
            }
            GuardExpr::And(v) | GuardExpr::Or(v) => {
                for e in v {
                    e.collect_cells(out);
                }
            }
            GuardExpr::Not(e) | GuardExpr::IsNull(e) => e.collect_cells(out),
        }
    }

    /// Evaluate against a concrete state.
    ///
    /// Returns a `Value`; boolean-shaped nodes return `Value::Boolean`. A cell the context cannot
    /// supply is an **error**, never a false — "could not be evaluated" is a distinct outcome
    /// from "evaluated false" and the gate treats them differently (hard reject vs. retry).
    pub fn eval(&self, ctx: &dyn GuardContext) -> Result<Value, FerroError> {
        Ok(match self {
            GuardExpr::Literal(v) => v.clone(),
            GuardExpr::Col { tbl, row, col } => ctx.column(*tbl, *row, *col)?,
            GuardExpr::Compare { left, op, right } => {
                let l = left.eval(ctx)?;
                let r = right.eval(ctx)?;
                // SQL three-valued logic: any NULL operand makes the comparison unknown.
                if matches!(l, Value::Null) || matches!(r, Value::Null) {
                    Value::Null
                } else {
                    Value::Boolean(op.apply(&l, &r))
                }
            }
            GuardExpr::Arith { left, op, right } => {
                let l = left.eval(ctx)?;
                let r = right.eval(ctx)?;
                arith(&l, *op, &r)?
            }
            GuardExpr::And(v) => {
                let mut saw_null = false;
                for e in v {
                    match truthy(&e.eval(ctx)?) {
                        Some(false) => return Ok(Value::Boolean(false)),
                        Some(true) => {}
                        None => saw_null = true,
                    }
                }
                if saw_null { Value::Null } else { Value::Boolean(true) }
            }
            GuardExpr::Or(v) => {
                let mut saw_null = false;
                for e in v {
                    match truthy(&e.eval(ctx)?) {
                        Some(true) => return Ok(Value::Boolean(true)),
                        Some(false) => {}
                        None => saw_null = true,
                    }
                }
                if saw_null { Value::Null } else { Value::Boolean(false) }
            }
            GuardExpr::Not(e) => match truthy(&e.eval(ctx)?) {
                Some(b) => Value::Boolean(!b),
                None => Value::Null,
            },
            GuardExpr::IsNull(e) => Value::Boolean(matches!(e.eval(ctx)?, Value::Null)),
        })
    }
}

fn truthy(v: &Value) -> Option<bool> {
    match v {
        Value::Boolean(b) => Some(*b),
        Value::Null => None,
        // Any other value in a boolean position is a bind-time bug; treat as unknown so the
        // gate reports "could not be evaluated" rather than silently passing.
        _ => None,
    }
}

fn arith(l: &Value, op: ArithOp, r: &Value) -> Result<Value, FerroError> {
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Ok(Value::Null);
    }
    match (l, r) {
        (Value::Integer(a), Value::Integer(b)) => {
            let (a, b) = (*a as i64, *b as i64);
            let out = match op {
                ArithOp::Add => a.checked_add(b),
                ArithOp::Sub => a.checked_sub(b),
                ArithOp::Mul => a.checked_mul(b),
                ArithOp::Div => {
                    if b == 0 {
                        return Err(FerroError::Merge("guard divides by zero".into()));
                    }
                    a.checked_div(b)
                }
            }
            .ok_or_else(|| FerroError::Merge("guard arithmetic overflowed".into()))?;
            if out > i32::MAX as i64 || out < i32::MIN as i64 {
                return Err(FerroError::Merge("guard arithmetic left INTEGER range".into()));
            }
            Ok(Value::Integer(out as i32))
        }
        _ => {
            let a = as_f64(l)?;
            let b = as_f64(r)?;
            Ok(Value::Float(match op {
                ArithOp::Add => a + b,
                ArithOp::Sub => a - b,
                ArithOp::Mul => a * b,
                ArithOp::Div => {
                    if b == 0.0 {
                        return Err(FerroError::Merge("guard divides by zero".into()));
                    }
                    a / b
                }
            }))
        }
    }
}

fn as_f64(v: &Value) -> Result<f64, FerroError> {
    match v {
        Value::Integer(i) => Ok(*i as f64),
        Value::Float(f) => Ok(*f),
        other => Err(FerroError::Merge(format!(
            "guard arithmetic on non-numeric value {:?}",
            other
        ))),
    }
}

impl Display for GuardExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GuardExpr::Literal(v) => write_value(f, v),
            GuardExpr::Col { tbl, row, col } => write!(f, "{}.{}[{}]", tbl, col, row),
            GuardExpr::Compare { left, op, right } => write!(f, "({} {} {})", left, op, right),
            GuardExpr::Arith { left, op, right } => write!(f, "({} {} {})", left, op, right),
            GuardExpr::And(v) => write_joined(f, v, " AND "),
            GuardExpr::Or(v) => write_joined(f, v, " OR "),
            GuardExpr::Not(e) => write!(f, "NOT {}", e),
            GuardExpr::IsNull(e) => write!(f, "{} IS NULL", e),
        }
    }
}

fn write_joined(f: &mut Formatter<'_>, v: &[GuardExpr], sep: &str) -> std::fmt::Result {
    write!(f, "(")?;
    for (i, e) in v.iter().enumerate() {
        if i > 0 {
            write!(f, "{}", sep)?;
        }
        write!(f, "{}", e)?;
    }
    write!(f, ")")
}

fn write_value(f: &mut Formatter<'_>, v: &Value) -> std::fmt::Result {
    match v {
        Value::Integer(i) => write!(f, "{}", i),
        Value::Float(x) => write!(f, "{}", x),
        Value::Varchar(s) => write!(f, "'{}'", s),
        Value::Boolean(b) => write!(f, "{}", b),
        Value::Null => write!(f, "NULL"),
    }
}

/// Whatever a guard is being evaluated against: the base snapshot at capture time, or the merged
/// state at merge time.
///
/// `column` returning `Err` means the cell could not be read at all. That is materially different
/// from returning `Value::Null`, and implementors must not collapse the two — the verification
/// gate maps "could not be evaluated" to a hard reject and "evaluated false" to a retry.
pub trait GuardContext {
    fn column(&self, tbl: TableId, row: RowId, col: ColId) -> Result<Value, FerroError>;
}

/// A first-class log record: a predicate plus the answer it gave when the write was admitted.
///
/// Separate from [`crate::tel::op::Op`] on purpose. An op says what changed; a guard says what
/// had to be true for that change to be legal. Only the second one is unrecoverable from images.
#[derive(Debug, Clone, PartialEq)]
pub struct Guard {
    pub expr: GuardExpr,
    /// The value `expr` produced when the transaction ran. Re-evaluating against merged state
    /// must reproduce this or the guard has been violated. Normally `Value::Boolean(true)`.
    pub expected: Value,
    /// The original SQL fragment, verbatim, for handing back to the agent on violation.
    /// `None` when the guard was synthesised rather than parsed.
    pub source_text: Option<String>,
}

impl Guard {
    pub fn new(expr: GuardExpr, expected: Value) -> Self {
        Guard { expr, expected, source_text: None }
    }

    /// The common case: a WHERE clause that must remain true.
    pub fn holds(expr: GuardExpr) -> Self {
        Guard { expr, expected: Value::Boolean(true), source_text: None }
    }

    pub fn with_source(mut self, text: impl Into<String>) -> Self {
        self.source_text = Some(text.into());
        self
    }

    /// Re-evaluate against `ctx`. `Ok(true)` means the guard still holds.
    ///
    /// An error here is **not** a violation: it means the guard could not be evaluated, which the
    /// gate treats as a hard reject rather than something the agent can retry.
    pub fn check(&self, ctx: &dyn GuardContext) -> Result<bool, FerroError> {
        Ok(self.expr.eval(ctx)? == self.expected)
    }

    /// What gets returned to the agent when the guard fails. Exit criterion 7 requires the
    /// violated predicate itself, not a generic conflict message.
    pub fn violated_predicate(&self) -> String {
        match &self.source_text {
            Some(t) => t.clone(),
            None => self.expr.to_string(),
        }
    }
}

impl Display for Guard {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} == ", self.violated_predicate())?;
        write_value(f, &self.expected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(Vec<((TableId, RowId, ColId), Value)>);

    impl GuardContext for Fixed {
        fn column(&self, tbl: TableId, row: RowId, col: ColId) -> Result<Value, FerroError> {
            self.0
                .iter()
                .find(|(k, _)| *k == (tbl, row, col))
                .map(|(_, v)| v.clone())
                .ok_or_else(|| FerroError::Merge(format!("no such cell {}.{}[{}]", tbl, col, row)))
        }
    }

    fn qty(v: i32) -> Fixed {
        Fixed(vec![((TableId(1), RowId(1), ColId(2)), Value::Integer(v))])
    }

    fn qty_ge_5() -> Guard {
        Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TableId(1), RowId(1), ColId(2)),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(5)),
        ))
        .with_source("qty >= 5")
    }

    #[test]
    fn guard_is_rechecked_against_whatever_state_it_is_given() {
        let g = qty_ge_5();
        assert!(g.check(&qty(7)).unwrap());
        assert!(!g.check(&qty(3)).unwrap());
    }

    #[test]
    fn violated_predicate_is_returned_verbatim() {
        assert_eq!(qty_ge_5().violated_predicate(), "qty >= 5");
        // and is still legible when no source text was captured
        let synth = Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TableId(1), RowId(1), ColId(2)),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(0)),
        ));
        assert_eq!(synth.violated_predicate(), "(tbl1.col2[row1] >= 0)");
    }

    #[test]
    fn unreadable_cell_is_an_error_not_a_false() {
        let g = qty_ge_5();
        let empty = Fixed(vec![]);
        // "could not be evaluated" must not masquerade as "evaluated false"
        assert!(g.check(&empty).is_err());
    }

    #[test]
    fn null_operand_makes_the_guard_unknown_not_true() {
        let ctx = Fixed(vec![((TableId(1), RowId(1), ColId(2)), Value::Null)]);
        let g = qty_ge_5();
        assert_eq!(g.expr.eval(&ctx).unwrap(), Value::Null);
        assert!(!g.check(&ctx).unwrap());
    }

    #[test]
    fn bounded_counter_guard_composes_with_arithmetic() {
        // qty - 5 >= 0, the classic bounded decrement
        let g = Guard::holds(GuardExpr::cmp(
            GuardExpr::arith(
                GuardExpr::col(TableId(1), RowId(1), ColId(2)),
                ArithOp::Sub,
                GuardExpr::Literal(Value::Integer(5)),
            ),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(0)),
        ));
        assert!(g.check(&qty(5)).unwrap());
        assert!(!g.check(&qty(4)).unwrap());
    }

    #[test]
    fn referenced_cells_finds_every_read() {
        let g = GuardExpr::And(vec![
            GuardExpr::cmp(
                GuardExpr::col(TableId(1), RowId(1), ColId(2)),
                CmpOp::Ge,
                GuardExpr::Literal(Value::Integer(5)),
            ),
            GuardExpr::cmp(
                GuardExpr::col(TableId(1), RowId(1), ColId(3)),
                CmpOp::Eq,
                GuardExpr::Literal(Value::Boolean(true)),
            ),
        ]);
        let cells = g.referenced_cells();
        assert_eq!(cells.len(), 2);
        assert!(cells.contains(&(TableId(1), RowId(1), ColId(3))));
    }
}
