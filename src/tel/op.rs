//! Typed effects: what a transaction *did*, in algebra terms rather than as images.
//!
//! Design authority: DESIGN.md section 3.
//!
//! Note the correction the research made: numeric deltas *are* recoverable from ordinary
//! before/after images (Oracle GoldenGate's `USEDELTA` does exactly this in production). Ops are
//! recorded anyway because they name **which algebra element was meant** when two shapes produce
//! identical images — but they are not the reason the query layer must cooperate. Guards are.
//! See [`crate::tel::guard`].

use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::tel::ids::{ColId, Dot, RowId, TableId};

/// A numeric increment. Kept separate from `Value` so that composing two `Add`s is total and
/// type-directed rather than a match over every `Value` variant.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Delta {
    Int(i64),
    Float(f64),
}

impl Delta {
    pub fn is_zero(&self) -> bool {
        match self {
            Delta::Int(i) => *i == 0,
            Delta::Float(f) => *f == 0.0,
        }
    }

    /// Compose two increments. `Add` is associative and commutative, which is the entire reason
    /// concurrent `qty -= n` merges arithmetically (exit criterion 6). Mixing an integer and a
    /// float promotes to float.
    pub fn compose(&self, other: &Delta) -> Result<Delta, FerroError> {
        Ok(match (self, other) {
            (Delta::Int(a), Delta::Int(b)) => Delta::Int(a.checked_add(*b).ok_or_else(|| {
                FerroError::Merge(format!("integer delta overflow composing {} and {}", a, b))
            })?),
            (Delta::Float(a), Delta::Float(b)) => Delta::Float(a + b),
            (Delta::Int(a), Delta::Float(b)) => Delta::Float(*a as f64 + b),
            (Delta::Float(a), Delta::Int(b)) => Delta::Float(a + *b as f64),
        })
    }

    /// Apply this increment to a base value.
    pub fn apply(&self, base: &Value) -> Result<Value, FerroError> {
        Ok(match (base, self) {
            (Value::Integer(b), Delta::Int(d)) => {
                let sum = (*b as i64).checked_add(*d).ok_or_else(|| {
                    FerroError::Merge(format!("integer overflow applying delta {} to {}", d, b))
                })?;
                if sum > i32::MAX as i64 || sum < i32::MIN as i64 {
                    return Err(FerroError::Merge(format!(
                        "delta {} pushes {} outside INTEGER range",
                        d, b
                    )));
                }
                Value::Integer(sum as i32)
            }
            (Value::Integer(b), Delta::Float(d)) => Value::Float(*b as f64 + d),
            (Value::Float(b), Delta::Int(d)) => Value::Float(b + *d as f64),
            (Value::Float(b), Delta::Float(d)) => Value::Float(b + d),
            (other, _) => {
                return Err(FerroError::Merge(format!(
                    "cannot apply a numeric delta to {:?}",
                    other
                )));
            }
        })
    }

    pub fn negate(&self) -> Delta {
        match self {
            Delta::Int(i) => Delta::Int(-i),
            Delta::Float(f) => Delta::Float(-f),
        }
    }
}

/// The effect algebra.
///
/// Idempotence is **not** uniform across these variants, and treating it as though it were is the
/// single most common way a merge engine corrupts a counter:
/// - idempotent: `Assign`, `Max`, `Min`, `SetInsert`, `RowCreate`, `RowDelete`
/// - **not** idempotent: `Add` — de-duplicate by `TxnId` before composing
#[derive(Debug, Clone, PartialEq)]
pub enum OpKind {
    /// The row came into existence. Carries the full initial image so a merge can materialise
    /// the row on a branch that never saw it.
    RowCreate(Vec<Value>),
    /// The row was removed.
    RowDelete,
    /// Last write wins on this column, subject to the column's merge policy.
    Assign(Value),
    /// Increment. Composes with other `Add`s. Not idempotent.
    Add(Delta),
    /// Monotone upper bound: result is `max(current, v)`. Idempotent and commutative.
    Max(Value),
    /// Monotone lower bound: result is `min(current, v)`. Idempotent and commutative.
    Min(Value),
    /// Add `elem` to a set-valued column, tagged with the dot that identifies this insertion.
    SetInsert { elem: Value, dot: Dot },
    /// Remove `elem`, naming the dots of the insertions this transaction actually observed.
    /// Observed-remove semantics: an insert this transaction did not see survives the remove.
    SetRemove { elem: Value, dots: Vec<Dot> },
}

impl OpKind {
    /// Whether replaying this op twice is the same as replaying it once. `Add` is the odd one
    /// out and the reason `TxnId` de-duplication exists.
    pub fn is_idempotent(&self) -> bool {
        !matches!(self, OpKind::Add(_))
    }

    /// Whether two ops of this kind on the same cell compose without a policy decision.
    pub fn commutes_with(&self, other: &OpKind) -> bool {
        match (self, other) {
            (OpKind::Add(_), OpKind::Add(_)) => true,
            (OpKind::Max(_), OpKind::Max(_)) => true,
            (OpKind::Min(_), OpKind::Min(_)) => true,
            (OpKind::SetInsert { .. }, OpKind::SetInsert { .. }) => true,
            (OpKind::SetInsert { .. }, OpKind::SetRemove { .. }) => true,
            (OpKind::SetRemove { .. }, OpKind::SetInsert { .. }) => true,
            (OpKind::SetRemove { .. }, OpKind::SetRemove { .. }) => true,
            // Two Assigns to the same cell contradict unless the values are equal; that is a
            // policy question, not a commutation one.
            _ => false,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            OpKind::RowCreate(_) => "RowCreate",
            OpKind::RowDelete => "RowDelete",
            OpKind::Assign(_) => "Assign",
            OpKind::Add(_) => "Add",
            OpKind::Max(_) => "Max",
            OpKind::Min(_) => "Min",
            OpKind::SetInsert { .. } => "SetInsert",
            OpKind::SetRemove { .. } => "SetRemove",
        }
    }
}

/// One typed effect on one cell.
#[derive(Debug, Clone, PartialEq)]
pub struct Op {
    pub tbl: TableId,
    /// Immutable surrogate. Not the primary key.
    pub row: RowId,
    /// `None` for whole-row ops (`RowCreate`, `RowDelete`).
    pub col: Option<ColId>,
    pub kind: OpKind,
    /// The value observed immediately before the op was applied.
    ///
    /// Enables LWW fallback, equality detection (two branches that wrote the *same* value are
    /// not in conflict), and audit. It is an observation, never the justification for the write
    /// — that is the guard's job.
    pub witness: Option<Value>,
}

impl Op {
    pub fn new(tbl: TableId, row: RowId, col: Option<ColId>, kind: OpKind) -> Self {
        Op { tbl, row, col, kind, witness: None }
    }

    pub fn with_witness(mut self, witness: Value) -> Self {
        self.witness = Some(witness);
        self
    }

    /// Two ops touch the same cell.
    pub fn same_cell(&self, other: &Op) -> bool {
        self.tbl == other.tbl && self.row == other.row && self.col == other.col
    }
}

/// A reservation taken against a bounded resource.
///
/// A reservation of headroom in a bounded resource, taken **before** the writes that spend it.
///
/// This comment used to say bounded counters need no special merge logic — compose the `Add`s and
/// re-evaluate the guard against the merged state. That is measurably false and DESIGN.md section
/// 3 has been corrected: guards are *preconditions*, evaluated against merged state before the
/// ops apply, so with a start of 20 and two takes of 12 the second merge tests `8 >= 0`, passes,
/// and the counter lands at -4. A precondition cannot see a post-op violation.
///
/// Escrow is the answer to that, and it works by moving the failure earlier rather than by making
/// the merge cleverer: the slack is partitioned at claim time, so an agent that would overdraw is
/// refused when it *writes*, while it can still do something about it.
#[derive(Debug, Clone, PartialEq)]
pub struct EscrowClaim {
    pub tbl: TableId,
    pub row: RowId,
    pub col: ColId,
    /// How much of the resource this transaction reserved.
    pub amount: Delta,
    /// Inclusive lower bound the resource must respect, if any.
    pub floor: Option<Value>,
    /// Inclusive upper bound the resource must respect, if any.
    pub ceiling: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_identical_decrements_compose_to_double() {
        // The Cassandra counter trap, asserted so nobody "fixes" it into idempotence.
        let a = Delta::Int(-5);
        assert_eq!(a.compose(&a).unwrap(), Delta::Int(-10));
        assert!(!OpKind::Add(a).is_idempotent());
    }

    #[test]
    fn assign_is_idempotent_but_does_not_commute() {
        let a = OpKind::Assign(Value::Integer(1));
        let b = OpKind::Assign(Value::Integer(2));
        assert!(a.is_idempotent());
        assert!(!a.commutes_with(&b));
    }

    #[test]
    fn delta_applies_and_refuses_non_numeric() {
        assert_eq!(
            Delta::Int(-5).apply(&Value::Integer(12)).unwrap(),
            Value::Integer(7)
        );
        assert!(Delta::Int(1).apply(&Value::Varchar("x".into())).is_err());
    }

    #[test]
    fn integer_delta_overflow_is_an_error_not_a_wrap() {
        assert!(Delta::Int(1).apply(&Value::Integer(i32::MAX)).is_err());
        assert!(Delta::Int(i64::MAX).compose(&Delta::Int(1)).is_err());
    }
}
