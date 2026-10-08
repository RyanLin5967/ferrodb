use std::collections::HashMap;

use crate::{binder::binder::BoundExpr, catalog::{catalog::Catalog, column::Value}, error::FerroError, optimizer::{cost_model::cost, optimizer::{build_join, combine_and, optimize, push, split_and}}, parser::parser::JoinType, planner::{logical_plan::LogicalPlan, physical_plan::PhysicalPlan}};

pub const MAX_DP_RELATIONS: usize = 12;

/// How many relations one query may join before the planner refuses it — **D66**.
///
/// The relation sets here are BITMASKS in a `u64` (`1 << relation_of(..)`), so relation 64 would
/// alias onto relation 0. That is not a crash in release, it is a WRONG ANSWER: two different
/// relations share a mask bit, the join-order search treats them as one, and the query returns a
/// cross product or drops a predicate silently. In a debug build the same shift panics with
/// "attempt to shift left with overflow" instead.
///
/// This is bounded rather than widened because widening only moves the cliff: any fixed-width mask
/// has one. A refusal that names the limit is a fact the caller can act on; an aliased bit is not.
/// `MAX_DP_RELATIONS` (12) is a different limit for a different reason — past it the exhaustive
/// DP is abandoned for a left-deep plan, which is a QUALITY decision, not a correctness one.
pub const MAX_JOIN_RELATIONS: usize = 63;

pub struct Sub {
    pub plan: PhysicalPlan,
    pub order: Vec<usize>,
    pub cost: f64,
}

/// Plan a tree of INNER joins, with the WHERE filters over it: flatten it, place every conjunct,
/// and search for a join order.
///
/// # D255 — every ON conjunct is placed by the relations it reads
///
/// A conjunct used to be applied only as a `bridge`: on a split `(l, r)` whose two sides it both
/// reads. A conjunct reading ONE relation has a one-bit mask, which cannot meet two disjoint sides,
/// and one reading NONE meets nothing — so both were silently dropped, at every size.
/// `a JOIN b ON a.id = b.id AND b.v = 5` ignored `b.v = 5`, and `ON a.id = b.id AND 1 = 0`
/// returned every match. Now, as in any planner that does predicate placement:
///
/// * **one relation** — a filter on that relation's LOGICAL leaf, placed by the same
///   [`push`] WHERE uses and before `optimize` chooses an access path, so `ON b.id = 3` reaches the
///   index as `WHERE b.id = 3` does. See [`filter_leaf`].
/// * **no relation** — a filter over the whole join, unless it is the literal TRUE, when it is
///   dropped: it keeps every row, and `ON TRUE` then plans exactly as it did past 12 relations
///   before D255.
/// * **two or more** — a bridge, unchanged.
///
/// A WHERE conjunct that reads two or more of these relations is placed the same way: over an
/// INNER join it is the same predicate as an ON conjunct, and [`flatten`] collects it with them.
///
/// The other half of D255 is that a join no predicate links now plans as a cross product at every
/// size instead of being refused at 12 relations or fewer — see [`dynamic_program`].
/// `tests/d255_join_on_conjuncts.rs` pins both halves on both sides of `MAX_DP_RELATIONS`.
pub fn reorder_inner_joins(plan: LogicalPlan, catalog: &Catalog) -> Result<PhysicalPlan, FerroError> {
    let mut leaves = Vec::new();
    let mut preds = Vec::new();
    flatten(plan, &mut leaves, &mut preds);
    let n = leaves.len();
    if n > MAX_JOIN_RELATIONS {
        return Err(FerroError::SqlParseError(format!(
            "query joins {} relations; the planner supports at most {}",
            n, MAX_JOIN_RELATIONS
        )));
    }

    let widths: Vec<usize> = leaves.iter().map(|l| l.output_schema().len()).collect();
    let mut orig_offset = vec![0usize; n];
    for r in 1..n {
        orig_offset[r] = orig_offset[r - 1] + widths[r - 1];
    }

    // Only conjuncts reading two or more relations go in `conjuncts`: every other reader of that
    // list — the bridges, `closed` in the DP, `left_deep` — is about linking relations.
    let mut conjuncts: Vec<(u64, BoundExpr)> = Vec::new();
    let mut local: Vec<Vec<BoundExpr>> = vec![Vec::new(); n];
    let mut constant: Vec<BoundExpr> = Vec::new();
    for pred in preds {
        let mut parts = Vec::new();
        split_and(pred, &mut parts);
        for part in parts {
            let mut rel = 0u64;
            relations_of(&part, &orig_offset, &mut rel, &widths);
            match rel.count_ones() {
                // A conjunct that reads no column has one value for every row. Only the literal
                // TRUE is dropped, and it is recognised by its shape, never evaluated: planning must
                // not run a user's expression (`i32::MIN / -1` panics in `evaluate`, and EXPLAIN
                // would then panic too). Everything else stays a per-row filter over the join, as a
                // column-free WHERE conjunct always has.
                0 => if part != BoundExpr::Literal(Value::Boolean(true)) {
                    constant.push(part)
                },
                1 => {
                    // Global column numbers → the leaf's own, which start at 0.
                    let r = rel.trailing_zeros() as usize;
                    local[r].push(remap(&part, &build_remap(&[r], &orig_offset, &widths)));
                }
                _ => conjuncts.push((rel, part)),
            }
        }
    }

    let mut base_plans = Vec::with_capacity(n);
    for (leaf, filters) in leaves.into_iter().zip(local) {
        base_plans.push(optimize(filter_leaf(leaf, filters), catalog)?)
    }

    let joined = if n > MAX_DP_RELATIONS {
        left_deep(base_plans, &conjuncts, &orig_offset, &widths, catalog)
    } else {
        dynamic_program(base_plans, &conjuncts, &orig_offset, &widths, catalog)?
    };
    // It reads no column, so it means the same over any column order and goes over whichever plan
    // came back, including the DP's reordering projection.
    Ok(if constant.is_empty() {
        joined
    } else {
        PhysicalPlan::Filter { input: Box::new(joined), predicate: combine_and(constant) }
    })
}

/// Put `filters` — conjuncts that read only this leaf, in its own column numbers — on the leaf.
///
/// * A leaf with nothing to add is returned UNTOUCHED, so no plan that had no such conjunct has its
///   predicate tree rebuilt: re-splitting a WHERE filter and recombining it reassociates its ANDs.
/// * Otherwise [`push`] places them, exactly as it placed the WHERE conjuncts already on the leaf:
///   - into ONE filter with a WHERE filter already there. Two stacked filters are the same rows,
///     but `optimize` only considers an index for a `Filter` directly over a `Scan`;
///   - through a LEFT JOIN leaf into its preserved side, which is sound and reaches that table's
///     index — and never into its nullable side, where a conjunct stops excluding the
///     NULL-extended rows this INNER join's ON must exclude. It stays above the left join.
fn filter_leaf(leaf: LogicalPlan, filters: Vec<BoundExpr>) -> LogicalPlan {
    if filters.is_empty() {
        return leaf;
    }
    push(leaf, filters)
}

/// The exhaustive join-order search, for at most `MAX_DP_RELATIONS` relations.
///
/// Subsets are built by increasing size. For every split `(l, r)` of a subset whose two sides both
/// have a plan, `bridge` is the conjuncts the split's join applies: those inside the subset that
/// read both sides.
///
/// # D255 — when a split with no bridge is a candidate
///
/// Before D255 it never was, so a query no chain of bridged splits could cover was refused with
/// "disconnected join graph": `a JOIN b ON TRUE`, and `a JOIN b ON TRUE JOIN c ON a.v + b.v = c.v`,
/// which is connected but only by a conjunct over three relations. Past `MAX_DP_RELATIONS`,
/// `left_deep` planned the same queries as cross products. And the parser has no `CROSS JOIN` and
/// no comma FROM list, so `ON TRUE` is the only way to write a cross product at all.
///
/// A split with no bridge is now a cross-product candidate, joined `ON true`, under two rules from
/// PostgreSQL's `join_search_one_level`:
///
/// * **R1, a closed side.** One side is CLOSED: no multi-relation conjunct reads both a relation in
///   it and one outside it. It has no predicate left to wait for, so crossing it now loses none.
///   PostgreSQL's `make_rels_by_clauseless_joins`, for a rel with no join clauses.
/// * **R3, a linked split.** Some conjunct reads both sides without yet lying inside them — it
///   reads a third relation too — so joining them is progress towards applying it. PostgreSQL's
///   `have_relevant_joinclause`. A conjunct over at most two relations that reads both sides is a
///   bridge, so R3 is only ever about conjuncts over three or more.
///
/// What those two rules guarantee:
///
/// * **The full set always gets a plan.** Take any planned proper subset S: if it is closed, R1
///   joins it with any relation outside it; if not, some conjunct reads S and a relation x outside
///   it, and `(S, {x})` is a bridge or R3. So every size has a plan. The `Internal` error below is
///   that argument failing, not a refusal of a query. PostgreSQL's third rule, the one-sided "last
///   ditch", is for a sub-problem whose rels have clauses to rels OUTSIDE it, and none exists here:
///   every conjunct reads only relations of its own flattened unit.
/// * **Every multi-relation conjunct is applied exactly once, in any tree.** At the lowest node
///   whose subset contains it, it is inside neither child but inside their union, so it reads both
///   sides; above that node it is inside one child.
/// * **Queries that planned before D255 see the same candidates**, when their conjuncts each read at
///   most two relations. R3 cannot fire for them (above). R1 cannot fire once every relation is
///   bridge-reachable: take the lowest node with relations both inside and outside a closed set —
///   its children split the set from the rest, and its bridge reads both.
///
/// **Where this differs from PostgreSQL, on purpose:** its clauseless joins are one-sided — a rel
/// against a single base relation — "to avoid unreasonable growth of planning time". R1 here also
/// admits a split whose sides are both larger, which is how two linked components are each joined
/// and then crossed ONCE: `(w⋈x)×(y⋈z)` instead of `((w⋈x)×y)⋈z`, whose intermediate is a full
/// cross product. This search visits every split anyway, so an admitted one costs its `build_join`.
/// With no join predicate at all every subset is closed and all 523,250 splits of 12 relations are
/// built — the count a 12-relation clique already built before D255, the load `MAX_DP_RELATIONS`
/// bounds.
fn dynamic_program(base: Vec<PhysicalPlan>, conjuncts: &[(u64, BoundExpr)], orig_offset: &[usize], widths: &[usize], catalog: &Catalog) -> Result<PhysicalPlan, FerroError> {
    let n = widths.len();
    let mut best = HashMap::new();
    for (r, plan) in base.into_iter().enumerate() {
        let cost = cost(&plan, catalog).cost;
        best.insert(1u64 << r, Sub { plan, order: vec![r], cost});
    }

    let closed = |side: u64| conjuncts.iter().all(|(rel, _)| rel & side == 0 || rel & side == *rel);

    // build up subsets by increasing size
    for size in 2..=n {
        let masks: Vec<u64> = (1u64..(1u64 << n)).filter(|m| m.count_ones() as usize == size).collect();
        for mask in masks {
            let mut sub = (mask - 1) & mask;
            while sub > 0 {
                let l_mask = sub;
                let r_mask = mask & !sub;
                sub = (sub.wrapping_sub(1)) & mask;
                let (Some(l), Some(r)) = (best.get(&l_mask), best.get(&r_mask)) else {continue;};

                // A bridge, or R3: some conjunct reads both sides. Otherwise only R1.
                let linked = conjuncts.iter().any(|(rel, _)| rel & l_mask != 0 && rel & r_mask != 0);
                if !(linked || closed(l_mask) || closed(r_mask)) {
                    continue;
                }
                let bridge: Vec<&BoundExpr> = conjuncts.iter()
                    .filter(|(rel, _)| rel & mask == *rel && rel & l_mask != 0 && rel & r_mask != 0).map(|(_, e)| e).collect();

                let mut order = l.order.clone();
                order.extend(&r.order);
                let map = build_remap(&order, orig_offset, widths);
                let on = if bridge.is_empty() {
                    BoundExpr::Literal(Value::Boolean(true))
                } else {
                    combine_and(bridge.iter().map(|e| remap(e, &map)).collect())
                };
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

    let full = (1u64 << n) - 1;
    let best_full = best.remove(&full).ok_or_else(|| FerroError::Internal(format!(
        "the join search planned no subset covering all {n} relations; R1 and R3 in `dynamic_program` are meant to make that impossible"
    )))?;

    let idendity: Vec<usize> = (0..n).collect();
    if best_full.order == idendity {
        return Ok(best_full.plan)
    } else {
        let map = build_remap(&best_full.order, orig_offset, widths);
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
        // D255 — a WHERE conjunct `push` left over an INNER join (it reads both inputs) is the same
        // predicate as an ON conjunct, so it joins the ONs. A filter over any other join stays a
        // leaf: `push` kept it above a join that NULL-extends, and it must stay there.
        LogicalPlan::Filter { input, predicate } if is_inner_join(&input) => {
            flatten(*input, leaves, preds);
            preds.push(predicate);
        }
        other => leaves.push(other),
    }
}

fn is_inner_join(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Join { join_type: JoinType::Inner, .. })
}

pub fn relation_of(idx: usize, orig_offset: &[usize], widths: &[usize]) -> usize {
    (0..widths.len()).find(|&r| idx >= orig_offset[r] && idx < orig_offset[r] + widths[r]).unwrap_or(0)
}   

pub fn relations_of(expr: &BoundExpr, orig_offset: &[usize], out: &mut u64, widths: &[usize]) {
    match expr {
        BoundExpr::BinaryOp { left, right, .. } => {
            relations_of(left, orig_offset, out, widths);
            relations_of(right, orig_offset, out, widths);
        }
        BoundExpr::Column(i) => *out |= 1u64 << relation_of(*i, orig_offset, widths),
        BoundExpr::Literal(_) => {}
        BoundExpr::UnaryOp {right, .. } => relations_of(right, orig_offset, out, widths),
    }
}

/// Past `MAX_DP_RELATIONS`: join in the order written, crossing `ON true` where no bridge applies.
///
/// `conjuncts` holds only conjuncts over two or more relations (D255 — the others are already on
/// the base plans or over the whole join). Each one, with highest relation `m`, becomes a bridge at
/// exactly one step, the one that joins relation `m`: before it the conjunct is not yet inside
/// `mask`, and after it the conjunct no longer reads the relation being added.
fn left_deep(base: Vec<PhysicalPlan>, conjuncts: &[(u64, BoundExpr)], orig_offset: &[usize], widths: &[usize], catalog: &Catalog) -> PhysicalPlan {
    let mut iter = base.into_iter();
    let mut acc = iter.next().unwrap();
    let mut acc_order = vec![0usize];
    let mut covered = 1u64;
    for r in 1..widths.len() {
        let right = iter.next().unwrap();
        let r_mask = 1u64 << r;
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