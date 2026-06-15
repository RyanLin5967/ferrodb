use crate::{binder::binder::BoundExpr, parser::parser::JoinType};

pub enum PhysicalPlan {
    SeqScan {table: String},
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        on: BoundExpr,
        join_type: JoinType,
        right_width: usize,
    },
    Filter { 
        input: Box<PhysicalPlan>, 
        predicate: BoundExpr,
    },
    Projection { input: Box<PhysicalPlan>, exprs: Vec<BoundExpr>},
}

