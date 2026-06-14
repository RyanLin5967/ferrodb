use crate::{binder::binder::{BoundColumn, BoundExpr}, parser::parser::JoinType};

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