//! Capturing typed effects and guards from the query layer.
//!
//! Design authority: DESIGN.md section 3 ("The single most important correction") and section 6
//! (`src/execution/update.rs:48` — `assignments: Vec<(usize, BoundExpr)>` already holds the
//! *operation shape*, currently evaluated to a scalar and discarded).
//!
//! This is the "query layer must cooperate" hook, and what it exists to capture is the **guard**.
//! An `Op` could in principle be recovered from before/after images — Oracle GoldenGate's
//! `USEDELTA` recovers numeric deltas in production. The `WHERE qty >= 5` that made the write
//! legal cannot be recovered from any log of values, so if it is not captured here it is gone.
//!
//! Consequently [`capture_guard`] **refuses** an expression it cannot translate rather than
//! returning a weaker predicate. A guard that silently degrades to something trivially true is
//! worse than no guard at all: the merge would then report `Clean` for a write whose precondition
//! nobody ever re-checked.

use crate::binder::binder::BoundExpr;
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::parser::scanner::TokenType;
use crate::tel::guard::{ArithOp, CmpOp, Guard, GuardContext, GuardExpr};
use crate::tel::ids::{ColId, RowId, TableId};
use crate::tel::op::{Delta, Op, OpKind};

/// One row image, addressed the way a [`GuardExpr`] addresses cells.
///
/// `BoundExpr::Column(i)` is an offset into one particular plan's output row and means nothing
/// once that plan is gone, which is why guards name `(table, row, column)` instead. This adapter
/// is the bridge in the one place where the offset is still meaningful: at capture time, against
/// the row actually being written.
pub struct RowSnapshot<'a> {
    pub tbl: TableId,
    pub row: RowId,
    pub values: &'a [Value],
}

impl GuardContext for RowSnapshot<'_> {
    fn column(&self, tbl: TableId, row: RowId, col: ColId) -> Result<Value, FerroError> {
        if tbl != self.tbl || row != self.row {
            return Err(FerroError::Merge(format!(
                "row snapshot holds {}[{}], not {}[{}]",
                self.tbl, self.row, tbl, row
            )));
        }
        self.values.get(col.0 as usize).cloned().ok_or_else(|| {
            FerroError::CellAbsent(format!("column {} outside the row image for {}", col, row))
        })
    }
}

/// Where the target table's columns sit inside the plan's output row.
///
/// `BoundExpr::Column(i)` is an offset into the plan's **combined** row — `Scope.columns` in the
/// binder documents it as "index = offset in combined row" — not an ordinal in the target table's
/// schema. Those coincide only for a single-table plan. For an `UPDATE`/`DELETE` over a join,
/// treating them as identical silently renames the guard onto a different column and re-evaluates
/// a predicate nobody wrote, which is exactly the silent degradation this module refuses to do.
#[derive(Debug, Clone, Copy)]
pub struct ColMap {
    /// Plan offset at which the target table's columns begin.
    pub base: usize,
    /// How many columns the target table has.
    pub count: usize,
}

impl ColMap {
    /// The single-table case, where plan offset and schema ordinal do coincide.
    pub fn single_table(count: usize) -> Self {
        ColMap { base: 0, count }
    }

    /// `None` when the offset falls outside the target table — i.e. the guard reads a column of
    /// some other relation in the plan, which cannot be expressed as `(tbl, row, col)`.
    pub fn col_of(&self, offset: usize) -> Option<ColId> {
        if offset < self.base || offset >= self.base + self.count {
            return None;
        }
        Some(ColId((offset - self.base) as u32))
    }
}

/// Translate a bound expression into a re-evaluable [`GuardExpr`] over `(tbl, row, col)`.
///
/// Returns `Err` for anything not representable. Callers must propagate that: a write whose guard
/// could not be captured has to fail at capture time, because by merge time the predicate is
/// unrecoverable.
pub fn to_guard_expr(
    tbl: TableId,
    row: RowId,
    map: ColMap,
    e: &BoundExpr,
) -> Result<GuardExpr, FerroError> {
    Ok(match e {
        BoundExpr::Literal(v) => GuardExpr::Literal(v.clone()),
        BoundExpr::Column(i) => match map.col_of(*i) {
            Some(col) => GuardExpr::col(tbl, row, col),
            None => {
                return Err(FerroError::Merge(format!(
                    "guard reads plan offset {}, which is outside target table {}'s columns \
                     [{}, {}); capturing it would rename the predicate onto a different column",
                    i, tbl, map.base, map.base + map.count
                )))
            }
        },
        BoundExpr::BinaryOp { left, operator, right } => {
            let l = to_guard_expr(tbl, row, map, left)?;
            let r = to_guard_expr(tbl, row, map, right)?;
            match operator {
                TokenType::And => GuardExpr::And(vec![l, r]),
                TokenType::Or => GuardExpr::Or(vec![l, r]),
                _ => match cmp_of(operator) {
                    Some(op) => GuardExpr::cmp(l, op, r),
                    None => match arith_of(operator) {
                        Some(op) => GuardExpr::arith(l, op, r),
                        None => {
                            return Err(unrepresentable(operator));
                        }
                    },
                },
            }
        }
        BoundExpr::UnaryOp { operator, right } => {
            let r = to_guard_expr(tbl, row, map, right)?;
            match operator {
                TokenType::Not | TokenType::Bang => GuardExpr::Not(Box::new(r)),
                TokenType::Minus => GuardExpr::arith(
                    GuardExpr::Literal(Value::Integer(0)),
                    ArithOp::Sub,
                    r,
                ),
                other => return Err(unrepresentable(other)),
            }
        }
    })
}

fn unrepresentable(op: &TokenType) -> FerroError {
    FerroError::Merge(format!(
        "cannot capture a guard containing {:?}: refusing to record a weaker predicate than the \
         one that admitted the write",
        op
    ))
}

fn cmp_of(t: &TokenType) -> Option<CmpOp> {
    Some(match t {
        TokenType::Equal => CmpOp::Eq,
        TokenType::BangEqual => CmpOp::Ne,
        TokenType::Less => CmpOp::Lt,
        TokenType::LessEqual => CmpOp::Le,
        TokenType::Greater => CmpOp::Gt,
        TokenType::GreaterEqual => CmpOp::Ge,
        _ => return None,
    })
}

fn arith_of(t: &TokenType) -> Option<ArithOp> {
    Some(match t {
        TokenType::Plus => ArithOp::Add,
        TokenType::Minus => ArithOp::Sub,
        TokenType::Star => ArithOp::Mul,
        TokenType::Slash => ArithOp::Div,
        _ => return None,
    })
}

/// Capture a WHERE clause as a first-class guard: the predicate, plus the answer it must still
/// give against the merged state.
///
/// `source_text` should be the SQL fragment verbatim — exit criterion 7 hands it back to the
/// agent, and the original text is more useful to a retry than a rendered expression tree.
pub fn capture_guard(
    tbl: TableId,
    row: RowId,
    map: ColMap,
    predicate: &BoundExpr,
    source_text: Option<&str>,
) -> Result<Guard, FerroError> {
    let g = Guard::holds(to_guard_expr(tbl, row, map, predicate)?);
    Ok(match source_text {
        Some(t) => g.with_source(t),
        None => g,
    })
}

/// Capture one `SET col = <expr>` assignment as a typed op, preserving the *shape* rather than
/// collapsing it to a scalar.
///
/// The shape is what distinguishes `qty = qty - 5` (an `Add`, which composes with a concurrent
/// decrement) from `qty = 15` (an `Assign`, which contradicts one). Both produce the same
/// after-image on a given row, and that ambiguity is exactly what a value log cannot resolve.
///
/// `old_values` is the row image immediately before the write; it supplies the witness and lets
/// non-relative expressions be folded to an absolute value.
pub fn capture_assignment(
    tbl: TableId,
    row: RowId,
    col: ColId,
    expr: &BoundExpr,
    old_values: &[Value],
) -> Result<Op, FerroError> {
    let witness = old_values.get(col.0 as usize).cloned();
    let kind = match relative_delta(col, expr) {
        Some(d) => OpKind::Add(d),
        None => {
            let snap = RowSnapshot { tbl, row, values: old_values };
            let map = ColMap::single_table(old_values.len());
            OpKind::Assign(to_guard_expr(tbl, row, map, expr)?.eval(&snap)?)
        }
    };
    let op = Op::new(tbl, row, Some(col), kind);
    Ok(match witness {
        Some(w) => op.with_witness(w),
        None => op,
    })
}

/// Recognise `col ± literal` (and `literal + col`) as a relative move on that same column.
///
/// Deliberately narrow. A shape this does not recognise becomes an `Assign`, which is the
/// conservative answer: an `Assign` conflicts where an `Add` would have composed, so mis-reading
/// the shape costs a spurious conflict rather than a silently doubled counter.
fn relative_delta(col: ColId, e: &BoundExpr) -> Option<Delta> {
    let BoundExpr::BinaryOp { left, operator, right } = e else {
        return None;
    };
    let is_self = |b: &BoundExpr| matches!(b, BoundExpr::Column(i) if ColId::from(*i) == col);
    match operator {
        TokenType::Plus => {
            if is_self(left) {
                literal_delta(right, false)
            } else if is_self(right) {
                literal_delta(left, false)
            } else {
                None
            }
        }
        // Subtraction does not commute: only `col - literal` is a relative move.
        TokenType::Minus if is_self(left) => literal_delta(right, true),
        _ => None,
    }
}

fn literal_delta(e: &BoundExpr, negate: bool) -> Option<Delta> {
    let d = match e {
        BoundExpr::Literal(Value::Integer(i)) => Delta::Int(*i as i64),
        BoundExpr::Literal(Value::Float(f)) => Delta::Float(*f),
        _ => return None,
    };
    Some(if negate { d.negate() } else { d })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TBL: TableId = TableId(1);
    const QTY: ColId = ColId(2);
    const R1: RowId = RowId(1);

    fn row() -> Vec<Value> {
        vec![
            Value::Integer(1),
            Value::Varchar("widget".into()),
            Value::Integer(20),
        ]
    }

    fn bin(l: BoundExpr, op: TokenType, r: BoundExpr) -> BoundExpr {
        BoundExpr::BinaryOp { left: Box::new(l), operator: op, right: Box::new(r) }
    }

    #[test]
    fn qty_minus_five_is_captured_as_an_add_not_an_assign() {
        // The shape is the point: `qty = qty - 5` composes with a concurrent decrement,
        // `qty = 15` does not, and both leave the same after-image.
        let e = bin(
            BoundExpr::Column(2),
            TokenType::Minus,
            BoundExpr::Literal(Value::Integer(5)),
        );
        let op = capture_assignment(TBL, R1, QTY, &e, &row()).unwrap();
        assert_eq!(op.kind, OpKind::Add(Delta::Int(-5)));
        assert_eq!(op.witness, Some(Value::Integer(20)));
    }

    #[test]
    fn a_literal_assignment_stays_an_assign() {
        let e = BoundExpr::Literal(Value::Integer(15));
        let op = capture_assignment(TBL, R1, QTY, &e, &row()).unwrap();
        assert_eq!(op.kind, OpKind::Assign(Value::Integer(15)));
    }

    #[test]
    fn five_plus_qty_is_still_an_add() {
        let e = bin(
            BoundExpr::Literal(Value::Integer(5)),
            TokenType::Plus,
            BoundExpr::Column(2),
        );
        assert_eq!(
            capture_assignment(TBL, R1, QTY, &e, &row()).unwrap().kind,
            OpKind::Add(Delta::Int(5))
        );
    }

    #[test]
    fn five_minus_qty_is_not_an_add_because_subtraction_does_not_commute() {
        // 5 - qty is an absolute expression, not a relative move. Recording it as Add(5) would
        // compose two of them into nonsense.
        let e = bin(
            BoundExpr::Literal(Value::Integer(5)),
            TokenType::Minus,
            BoundExpr::Column(2),
        );
        assert_eq!(
            capture_assignment(TBL, R1, QTY, &e, &row()).unwrap().kind,
            OpKind::Assign(Value::Integer(-15))
        );
    }

    #[test]
    fn a_delta_against_a_different_column_is_not_a_relative_move() {
        // `qty = other + 5` is absolute with respect to qty.
        let e = bin(
            BoundExpr::Column(0),
            TokenType::Plus,
            BoundExpr::Literal(Value::Integer(5)),
        );
        assert_eq!(
            capture_assignment(TBL, R1, QTY, &e, &row()).unwrap().kind,
            OpKind::Assign(Value::Integer(6))
        );
    }

    #[test]
    fn a_where_clause_becomes_a_guard_that_can_be_rechecked_later() {
        let e = bin(
            BoundExpr::Column(2),
            TokenType::GreaterEqual,
            BoundExpr::Literal(Value::Integer(5)),
        );
        let g = capture_guard(TBL, R1, ColMap::single_table(row().len()), &e, Some("qty >= 5")).unwrap();
        assert_eq!(g.violated_predicate(), "qty >= 5");

        // and it is re-evaluable against a state nobody had at capture time
        let plenty = RowSnapshot { tbl: TBL, row: R1, values: &row() };
        assert!(g.check(&plenty).unwrap());
        let scarce = vec![Value::Integer(1), Value::Null, Value::Integer(2)];
        let short = RowSnapshot { tbl: TBL, row: R1, values: &scarce };
        assert!(!g.check(&short).unwrap());
    }

    #[test]
    fn conjunctions_and_negations_survive_capture() {
        let e = BoundExpr::UnaryOp {
            operator: TokenType::Not,
            right: Box::new(bin(
                bin(
                    BoundExpr::Column(2),
                    TokenType::GreaterEqual,
                    BoundExpr::Literal(Value::Integer(5)),
                ),
                TokenType::And,
                bin(
                    BoundExpr::Column(0),
                    TokenType::Equal,
                    BoundExpr::Literal(Value::Integer(1)),
                ),
            )),
        };
        let g = capture_guard(TBL, R1, ColMap::single_table(row().len()), &e, None).unwrap();
        assert_eq!(g.expr.referenced_cells().len(), 2);
        let snap = RowSnapshot { tbl: TBL, row: R1, values: &row() };
        // qty >= 5 AND id = 1 holds, so NOT(...) is false
        assert!(!g.check(&snap).unwrap());
    }

    #[test]
    fn an_untranslatable_predicate_is_refused_rather_than_weakened() {
        // A guard that silently degrades to something trivially true is worse than no guard:
        // the merge would report Clean for a write whose precondition nobody re-checked.
        let e = bin(
            BoundExpr::Column(2),
            TokenType::Select, // not an operator this translation knows
            BoundExpr::Literal(Value::Integer(5)),
        );
        let err = capture_guard(TBL, R1, ColMap::single_table(row().len()), &e, None).unwrap_err();
        assert!(format!("{}", err).contains("refusing"));
    }

    #[test]
    fn arithmetic_inside_a_guard_survives_capture() {
        // `qty - 5 >= 0`, the bounded decrement, captured whole.
        let e = bin(
            bin(
                BoundExpr::Column(2),
                TokenType::Minus,
                BoundExpr::Literal(Value::Integer(5)),
            ),
            TokenType::GreaterEqual,
            BoundExpr::Literal(Value::Integer(0)),
        );
        let g = capture_guard(TBL, R1, ColMap::single_table(row().len()), &e, None).unwrap();
        let snap = RowSnapshot { tbl: TBL, row: R1, values: &row() };
        assert!(g.check(&snap).unwrap());
        let scarce = vec![Value::Integer(1), Value::Null, Value::Integer(4)];
        assert!(!g
            .check(&RowSnapshot { tbl: TBL, row: R1, values: &scarce })
            .unwrap());
    }

    /// Capture → log → merge, end to end, over the shape the SQL layer actually produces:
    /// `UPDATE inv SET qty = qty - 5 WHERE qty >= 5`.
    mod end_to_end {
        use super::*;
        use crate::branch::record::BranchRecord;
        use crate::branch::types::{BranchId, CommitHash, LeaseDeadline};
        use crate::tel::engine::ThreeWayMerger;
        use crate::tel::frame::TxnFrame;
        use crate::tel::ids::TxnId;
        use crate::tel::log::MemEffectLog;
        use crate::tel::merge::{
            ColumnPolicyLookup, ConflictKind, MergeOutcome, MergePolicy, Merger,
        };
        use crate::tel::op::Delta;
        use crate::tel::EffectLog;

        struct AllReject;
        impl ColumnPolicyLookup for AllReject {
            fn policy(&self, _t: TableId, _c: ColId) -> MergePolicy {
                MergePolicy::Reject
            }
        }

        fn stock(qty: i32) -> Vec<Value> {
            vec![Value::Integer(1), Value::Varchar("widget".into()), Value::Integer(qty)]
        }

        /// What one agent's `UPDATE inv SET qty = qty - n WHERE qty >= n` records.
        fn take(txn: u64, branch: u64, n: i32, seen: &[Value]) -> TxnFrame {
            let assignment = bin(
                BoundExpr::Column(2),
                TokenType::Minus,
                BoundExpr::Literal(Value::Integer(n)),
            );
            let predicate = bin(
                BoundExpr::Column(2),
                TokenType::GreaterEqual,
                BoundExpr::Literal(Value::Integer(n)),
            );
            let mut f = TxnFrame::new(
                TxnId(txn),
                BranchId::new(branch, 0),
                CommitHash::ZERO,
                0,
                1,
            );
            f.push_op(capture_assignment(TBL, R1, QTY, &assignment, seen).unwrap());
            f.push_guard(
                capture_guard(TBL, R1, ColMap::single_table(row().len()), &predicate, Some(&format!("qty >= {}", n))).unwrap(),
            );
            f
        }

        fn merge_two(on_hand: i32, a: i32, b: i32) -> MergeOutcome {
            let seen = stock(on_hand);
            let log = MemEffectLog::new();
            log.append(&take(1, 1, a, &seen)).unwrap();
            log.append(&take(2, 2, b, &seen)).unwrap();

            let ours = log.frames_for(BranchId::new(1, 0), 0).unwrap();
            let theirs = log.frames_for(BranchId::new(2, 0), 0).unwrap();
            let lca = BranchRecord::trunk(0, LeaseDeadline(u64::MAX));
            let base = RowSnapshot { tbl: TBL, row: R1, values: &seen };
            ThreeWayMerger::new()
                .merge(&lca, &ours, &theirs, &AllReject, &base)
                .unwrap()
        }

        #[test]
        fn concurrent_decrements_compose_arithmetically() {
            // Exit criterion 6, from captured SQL rather than hand-built ops.
            let out = merge_two(20, 5, 3);
            match &out {
                MergeOutcome::Commuting { composed } => {
                    assert_eq!(composed.len(), 1);
                    assert_eq!(composed[0].kind, OpKind::Add(Delta::Int(-8)));
                }
                other => panic!("expected Commuting, got {}", other),
            }
        }

        #[test]
        fn overselling_is_rejected_with_the_violated_predicate_returned() {
            // Exit criterion 7. Eight on hand; each agent legally takes five against its own
            // snapshot; the composition would take ten.
            let out = merge_two(8, 5, 5);
            assert!(out.is_conflict(), "{}", out);
            let r = &out.conflicts()[0];
            assert_eq!(r.kind, ConflictKind::GuardFailed);
            assert_eq!(
                r.violated_guard.as_ref().unwrap().violated_predicate(),
                "qty >= 5"
            );
            assert!(r.feedback().contains("qty >= 5"));
        }

        #[test]
        fn the_rejection_is_not_unconditional() {
            // Same two takes against enough stock must pass, or the check above proves nothing.
            assert_eq!(merge_two(20, 5, 5).name(), "Commuting");
        }
    }

    #[test]
    fn a_snapshot_refuses_a_row_it_does_not_hold() {
        let snap = RowSnapshot { tbl: TBL, row: R1, values: &row() };
        assert!(snap.column(TBL, RowId(9), QTY).is_err());
        assert!(snap.column(TBL, R1, ColId(99)).is_err());
    }

    /// R7: a plan offset outside the target table must be refused, not silently renamed.
    ///
    /// `BoundExpr::Column(i)` indexes the plan's combined output row. For a single-table plan that
    /// equals the schema ordinal; for an UPDATE/DELETE over a join it does not. Mapping it as
    /// identity would point the guard at a different column of the target table and re-evaluate a
    /// predicate nobody wrote — the silent degradation this module's header forbids.
    #[test]
    fn a_guard_reading_outside_the_target_table_is_refused() {
        // Target table has 2 columns at plan offsets 0..2. Offset 5 belongs to some other relation.
        let map = ColMap::single_table(2);
        let e = BoundExpr::Column(5);
        let err = to_guard_expr(TBL, R1, map, &e).unwrap_err();
        assert!(
            format!("{}", err).contains("outside target table"),
            "the refusal did not name the problem: {}",
            err
        );
    }

    /// The join case the identity mapping got wrong: the target sits on the RIGHT of the join, so
    /// its columns start at offset 2. Offset 2 is the target's column 0; offset 0 belongs to the
    /// left relation and must be refused. A width-only check would have accepted offset 0 and
    /// silently renamed it, which is why the map carries a base and not just a count.
    #[test]
    fn a_join_shifts_the_target_tables_offsets() {
        let map = ColMap { base: 2, count: 2 };
        assert_eq!(map.col_of(2), Some(ColId(0)));
        assert_eq!(map.col_of(3), Some(ColId(1)));
        assert_eq!(map.col_of(0), None, "left relation's column was accepted as the target's");
        assert_eq!(map.col_of(4), None);

        let ok = to_guard_expr(TBL, R1, map, &BoundExpr::Column(2)).unwrap();
        assert_eq!(ok, GuardExpr::col(TBL, R1, ColId(0)));
        assert!(to_guard_expr(TBL, R1, map, &BoundExpr::Column(0)).is_err());
    }

    /// Control: the single-table case must still map straight through, or the refusal above would
    /// just be breaking every guard.
    #[test]
    fn a_single_table_plan_still_maps_offsets_straight_through() {
        let map = ColMap::single_table(3);
        assert_eq!(map.col_of(0), Some(ColId(0)));
        assert_eq!(map.col_of(2), Some(ColId(2)));
        assert_eq!(map.col_of(3), None);
    }
}
