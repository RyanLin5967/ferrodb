use std::collections::HashMap;

use crate::{binder::binder::BoundExpr, catalog::{catalog::Catalog, column::Value}, error::FerroError, optimizer::{cost_model::cost, optimizer::{build_join, combine_and, optimize, split_and}}, parser::parser::JoinType, planner::{logical_plan::LogicalPlan, physical_plan::PhysicalPlan}};

pub const MAX_DP_RELATIONS: usize = 12;

pub struct Sub {
    pub plan: PhysicalPlan,
    pub order: Vec<usize>,
    pub cost: f64,
}

pub fn reorder_inner_joins(plan: LogicalPlan, catalog: &Catalog) -> Result<PhysicalPlan, FerroError> {
    let mut leaves = Vec::new();
    let mut preds = Vec::new();
    flatten(plan, &mut leaves, &mut preds);
    let n = leaves.len();

    let widths: Vec<usize> = leaves.iter().map(|l| l.output_schema().len()).collect();
    let mut orig_offset = vec![0usize; n];
    for r in 1..n {
        orig_offset[r] = orig_offset[r - 1] + widths[r - 1];
    }

    let mut base_plans = Vec::with_capacity(n);
    for leaf in leaves {
        base_plans.push(optimize(leaf, catalog)?)
    }

    let mut conjuncts: Vec<(u32, BoundExpr)> = Vec::new();
    for pred in preds {
        let mut parts = Vec::new();
        split_and(pred, &mut parts);
        for part in parts {
            let mut rel = 0u32;
            relations_of(&part, &orig_offset, &mut rel, &widths);
            conjuncts.push((rel, part));
        }
    }

    if n > MAX_DP_RELATIONS {
        return Ok(left_deep(base_plans, &conjuncts, &orig_offset, &widths, catalog))
    }

    let mut best = HashMap::new();
    for (r, plan) in base_plans.into_iter().enumerate() {
        let cost = cost(&plan, catalog).cost;
        best.insert(1 << r, Sub { plan, order: vec![r], cost});
    }

    // build up subsets by increasing size
    for size in 2..=n {
        let masks: Vec<u32> = (1u32..(1 << n)).filter(|m| m.count_ones() as usize == size).collect();
        for mask in masks {
            let mut sub = (mask - 1) & mask;
            while sub > 0 {
                let l_mask = sub;
                let r_mask = mask & !sub;
                sub = (sub.wrapping_sub(1)) & mask;
                let (Some(l), Some(r)) = (best.get(&l_mask), best.get(&r_mask)) else {continue;};

                let bridge: Vec<&BoundExpr> = conjuncts.iter()
                    .filter(|(rel, _)| rel & mask == *rel && rel & l_mask != 0 && rel & r_mask != 0).map(|(_, e)| e).collect();
                if bridge.is_empty() {
                    continue;
                }

                let mut order = l.order.clone();
                order.extend(&r.order);
                let map = build_remap(&order, &orig_offset, &widths);
                let on = combine_and(bridge.iter().map(|e| remap(e, &map)).collect());
                let left_width: usize = l.order.iter().map(|&x| widths[x]).sum();
                let right_width: usize = r.order.iter().map(|&x| widths[x]).sum();
                let candidate = build_join(l.plan.clone(), r.plan.clone(), on, JoinType::Inner, left_width, right_width, catalog);
                let cost = cost(&candidate, catalog).cost;
                if best.get(&mask).map_or(true, |s| cost < s.cost) {
                    best.insert(mask, Sub { plan: candidate, order, cost });
                }
            }
        }
    }

    let full = (1u32 << n) - 1;
    let best_full = best.remove(&full).ok_or_else(|| FerroError::Bind("disconnected join graph".into()))?;
    
    let idendity: Vec<usize> = (0..n).collect();
    if best_full.order == idendity {
        return Ok(best_full.plan)
    } else {
        let map = build_remap(&best_full.order, &orig_offset, &widths);
        let total: usize = widths.iter().sum();
        let exprs: Vec<BoundExpr> = (0..total).map(|k| BoundExpr::Column(*map.get(&k).unwrap())).collect();
        Ok(PhysicalPlan::Projection { input: Box::new(best_full.plan), exprs })
    }
}

pub fn flatten(plan: LogicalPlan, leaves: &mut Vec<LogicalPlan>, preds: &mut Vec<BoundExpr>) {
    match plan {
        LogicalPlan::Join { left, right, join_type: JoinType::Inner, on } => {
            flatten(*left, leaves, preds);
            flatten(*right, leaves, preds);
            preds.push(on);
        }
        other => leaves.push(other),
    }
}

pub fn relation_of(idx: usize, orig_offset: &[usize], widths: &[usize]) -> usize {
    (0..widths.len()).find(|&r| idx >= orig_offset[r] && idx < orig_offset[r] + widths[r]).unwrap_or(0)
}   

pub fn relations_of(expr: &BoundExpr, orig_offset: &[usize], out: &mut u32, widths: &[usize]) {
    match expr {
        BoundExpr::BinaryOp { left, right, .. } => {
            relations_of(left, orig_offset, out, widths);
            relations_of(right, orig_offset, out, widths);
        }
        BoundExpr::Column(i) => *out |= 1 << relation_of(*i, orig_offset, widths),
        BoundExpr::Literal(_) => {}
        BoundExpr::UnaryOp {right, .. } => relations_of(right, orig_offset, out, widths),
    }
}

fn left_deep(base: Vec<PhysicalPlan>, conjuncts: &[(u32, BoundExpr)], orig_offset: &[usize], widths: &[usize], catalog: &Catalog) -> PhysicalPlan {
    let mut iter = base.into_iter();
    let mut acc = iter.next().unwrap();
    let mut acc_order = vec![0usize];
    let mut covered = 1u32;
    for r in 1..widths.len() {
        let right = iter.next().unwrap();
        let r_mask = 1 << r;
        let mask = covered | r_mask;
        let mut order = acc_order.clone();
        order.push(r);
        let map = build_remap(&order, orig_offset, widths);
        let bridge: Vec<BoundExpr> = conjuncts.iter()
            .filter(|(rel, _)| rel & mask == *rel && rel & covered != 0 && rel & r_mask != 0)
            .map(|(_, e)| remap(e, &map))
            .collect();
        let on = if bridge.is_empty() {
            BoundExpr::Literal(Value::Boolean(true))
        } else {
            combine_and(bridge)
        };
        let left_width: usize = acc_order.iter().map(|&x| widths[x]).sum();
        acc = build_join(acc, right, on, JoinType::Inner, left_width, widths[r], catalog);
        acc_order = order;
        covered = mask;
    }
    acc
}

fn build_remap(order: &[usize], orig_offset: &[usize], widths: &[usize]) -> HashMap<usize, usize>{
    let mut map = HashMap::new();
    let mut new_offset = 0;
    for &r in order {
        for j in 0..widths[r] {
            map.insert(orig_offset[r] + j, new_offset + j);
        }
        new_offset += widths[r];
    }
    map
}

fn remap(expr: &BoundExpr, map: &HashMap<usize, usize>) -> BoundExpr{
    match expr {
        BoundExpr::BinaryOp { left, operator, right } => BoundExpr::BinaryOp{ left: Box::new(remap(left, map)), operator: *operator, right: Box::new(remap(right, map))},
        BoundExpr::Column(i) => BoundExpr::Column(*map.get(i).unwrap_or(i)),
        BoundExpr::Literal(v) => BoundExpr::Literal(v.clone()),
        BoundExpr::UnaryOp { operator, right } => BoundExpr::UnaryOp { operator: *operator, right: Box::new(remap(right, map)) }
    }
}