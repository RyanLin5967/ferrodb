//! The logical plan, and the output schema every consumer of a result needs.
//!
//! # The output schema was write-only until B9
//!
//! [`LogicalPlan::output_schema`] has always returned `Vec<BoundColumn>` — a name, a qualifier, a
//! declared type and a nullability per output column — and until B9 **every call site used it only
//! for `.len()`**: two arity computations in `optimizer::optimize`, one in `optimizer::push`, one in
//! `search_algorithm`. The names and types were dead weight, `PhysicalPlan::Projection` dropped the
//! `output` list on lowering, and so pgwire had nothing to advertise and named every column
//! `column1..N` with a comment saying it was being honest about not knowing.
//!
//! B9 threads the schema through to the wire for results that have no table behind them (system
//! views), which turns one latent inaccuracy into a reachable one and is why
//! [`is_computed_column`] exists: for a projection expression that is not a bare column reference,
//! `Binder::bind_projection` emits a placeholder whose NAME is honest (`?column?`) and whose TYPE is
//! a hardcoded `Integer` guess. Announcing that guess as `int4` on the wire would send `1.5` or
//! `alpha` under an `int4` column and break a conforming client's parser — the same failure mode
//! `pgwire::oid_of` documents for `timestamp`. A caller that puts these types on the wire must ask.

use crate::{binder::binder::{BoundColumn, BoundExpr}, parser::parser::JoinType};

#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        alias: Option<String>,
        output: Vec<BoundColumn>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        on: BoundExpr,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: BoundExpr,
    },
    Projection {
        input: Box<LogicalPlan>,
        exprs: Vec<BoundExpr>,
        output: Vec<BoundColumn>,
    }
}

/// The name `Binder::bind_projection` gives a projected expression that is not a bare column
/// reference — `SELECT qty * 2`, `SELECT 1`. Postgres uses the same spelling.
pub const COMPUTED_COLUMN: &str = "?column?";

/// Is this output column a computed expression whose declared type is a placeholder rather than a
/// fact?
///
/// `Binder::bind_projection` pushes `BoundColumn { qualifier: "", name: "?column?", data_type:
/// DataType::Integer, nullable: true }` for every projected expression it cannot resolve to a
/// column of a scanned relation. The name is deliberate and correct; the type is not derived from
/// the expression at all. A caller that needs a *type* for such a column has to get it from the
/// value it actually produced — and it has to ask this question first, because the placeholder is
/// indistinguishable from a real `INTEGER` column by looking at the `data_type` alone.
///
/// Keyed on the empty qualifier as well as the name: a real column may legitimately be *called*
/// `?column?` if someone quotes it into existence, but a real column always has a qualifier, since
/// `bind_scan` sets one for every scanned relation (the alias, or the table name).
pub fn is_computed_column(col: &BoundColumn) -> bool {
    col.qualifier.is_empty() && col.name == COMPUTED_COLUMN
}

impl LogicalPlan {
    // combine columns 
    pub fn output_schema(&self) -> Vec<BoundColumn> {
        match self {
            LogicalPlan::Filter { input, .. } => {
                input.output_schema()
            }
            LogicalPlan::Join { left, right, .. } => {
                let mut cols = left.output_schema();
                cols.extend(right.output_schema());
                cols
            }
            LogicalPlan::Projection {  output, .. } => {
                output.clone()
            }  
            LogicalPlan::Scan { output, .. } => {
                output.clone()
            }
        }
    }
}