use std::{cmp::Ordering, collections::HashSet, ops::Bound, sync::Arc};

use crate::{binder::binder::BoundExpr, buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, catalog_page::TableEntry, column::Value}, error::FerroError, execution::{executor::Executor, filter::Filter, hash_join::HashJoin, index_scan::IndexScan, nested_loop_join::NestedLoopJoin, projection::Projection, sec_index_scan::SecondaryIndexScan, seq_scan::SeqScan}, optimizer::{cost_model::{DEFAULT_CPU_TUPLE_COST, cost, equi_pairs, join_cardinality}, search_algorithm::reorder_inner_joins}, parser::{parser::JoinType, scanner::TokenType}, planner::{logical_plan::LogicalPlan, physical_plan::PhysicalPlan, plan::predicate_to_bounds}, storage::{heap_file_manager::{HeapFileManager, RecordId}, index::BPlusTreeManager}, wal::txn::ReadView};

pub fn optimize(lp: LogicalPlan, catalog: &Catalog) -> Result<PhysicalPlan, FerroError> {
    match lp {
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::Scan { table, .. } = input.as_ref() {
                if let Some(physical) = build_index_scan(table, &predicate, catalog) {
                    return Ok(physical)
                }
            }
            Ok(PhysicalPlan::Filter { input: Box::new(optimize(*input, catalog)?), predicate })
        }
        LogicalPlan::Join { left, right, join_type, on } => match join_type {
            JoinType::Left => {
                let right_width = right.output_schema().len();
                let left_width = left.output_schema().len();
                let pl = optimize(*left, catalog)?;
                let pr = optimize(*right, catalog)?;
                Ok(build_join(pl, pr, on, join_type, left_width, right_width, catalog))
            }
            JoinType::Inner => reorder_inner_joins(LogicalPlan::Join { left, right, join_type, on }, catalog),
            _ => Err(FerroError::Bind("right/full not implemented".into()))
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            Ok(PhysicalPlan::Projection { input: Box::new(optimize(*input, catalog)?), exprs })
        }
        LogicalPlan::Scan { table, .. } => {
            Ok(PhysicalPlan::SeqScan { table })
        }
    }
}

// physical -> executors
pub fn lower(plan: PhysicalPlan, catalog: &Catalog, bp: Arc<BufferPoolManager>, view: Arc<ReadView>) -> Result<Box<dyn Executor>, FerroError> {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let child = lower(*input, catalog, bp, view)?;
            Ok(Box::new(Filter{child, predicate}))
        }
        PhysicalPlan::SeqScan { table } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Bind(format!("unknown table: {}", table)))?;
            let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
            let tt_heap = HeapFileManager::open(entry.time_travel_root, bp);
            Ok(Box::new(SeqScan::new(heap.scan(), entry.schema.clone(), view, tt_heap)))
        }
        PhysicalPlan::Projection { input, exprs, .. } => {
            let child = lower(*input, catalog, bp, view)?;
            Ok(Box::new(Projection {child, exprs}))
        }
        PhysicalPlan::NestedLoopJoin { left, right, on, join_type, right_width } => {
            let left_exec = lower(*left, catalog, bp.clone(), view.clone())?;
            let right_exec = lower(*right, catalog, bp, view)?;
            Ok(Box::new(NestedLoopJoin::new(left_exec, right_exec, on, join_type, right_width)))
        }
        PhysicalPlan::IndexScan { table, column, lower, upper } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Bind(format!("unknown table: {}", table)))?;
            let schema = entry.schema.clone();
            let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
            let tt_heap = HeapFileManager::open(entry.time_travel_root, bp.clone());
            // D187 — does this scan compare anything against a bound? If it does, an entry whose
            // indexed value is NULL is not in the answer, because that comparison is UNKNOWN and an
            // UNKNOWN row is excluded. If it does not, there is nothing to be UNKNOWN about and the
            // NULL entries belong. Computed here, before either bound is moved into the scanner,
            // and handed to whichever scan is built — the rule is one sentence and both scans get
            // the same one. See `IndexScan::skip_nulls`.
            let skip_nulls =
                !matches!(lower, Bound::Unbounded) || !matches!(upper, Bound::Unbounded);
            if column == 0 {
                // SHARED root cell (D53). The point-lookup path every indexed read takes was
                // missed when D53 wired plan::open_table: a private cell here means an index scan
                // could descend from a root a concurrent split had already moved.
                let tree = match catalog.root_cell(&table, None) {
                    Some(cell) => BPlusTreeManager::<Value, RecordId>::open_shared(cell, bp),
                    None => BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp),
                };
                let scanner = tree.range_scan(lower, upper)?;
                return Ok(Box::new(IndexScan::new(heap, scanner, schema, view, tt_heap, skip_nulls)))
            }
            let col_name = schema.columns.get(column).ok_or(FerroError::Bind("unknown column".into()))?.name.clone();
            let sec_root = entry.indexes.iter().find(|i| i.column_name == col_name).ok_or(FerroError::Bind("no index found".into()))?.root_page_id;
            let sec_tree = match catalog.root_cell(&table, Some(&col_name)) {
                Some(cell) => BPlusTreeManager::<(Value, Value), ()>::open_shared(cell, bp.clone()),
                None => BPlusTreeManager::<(Value, Value), ()>::open(sec_root, bp.clone()),
            };
            let primary_index = match catalog.root_cell(&table, None) {
                Some(cell) => BPlusTreeManager::<Value, RecordId>::open_shared(cell, bp.clone()),
                None => BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp.clone()),
            };

            // ⭐ D179 SUPERSEDES D187 HERE, and this is not a merge of two equals: d187 called
            // `secondary_scan_lower(&lower)` and had to `ok_or_else` on an unsupported bound. D179
            // replaced that with `secondary_scan_start`, which cannot fail. d187 keeps only its
            // `skip_nulls` argument; its scanner-opening line is deliberately dropped.
            // D179 — the scanner is opened at the START KEY, which is not the bound. `Excluded(v)`
            // opens at `(v, Null)` like `Included(v)` does and the exclusion is applied by
            // `SecondaryIndexScan::next`, which skips the leading `sec == v` run. Both bounds are
            // handed to the executor unchanged for that reason.
            let scanner = sec_tree.range_scan(secondary_scan_start(&lower), Bound::Unbounded)?;
            Ok(Box::new(SecondaryIndexScan::new(heap, scanner, primary_index, schema, lower, upper, view, tt_heap, column, skip_nulls)))
        }
        PhysicalPlan::HashJoin { left, right, on, join_type, left_keys, right_keys, right_width } => {
            let left_exec = lower(*left, catalog, bp.clone(), view.clone())?;
            let right_exec = lower(*right, catalog, bp, view)?;
            Ok(Box::new(HashJoin::new(left_exec, right_exec, on, join_type, left_keys, right_keys, right_width)))
        }
    }
}

pub fn build_join(left: PhysicalPlan, right: PhysicalPlan, on: BoundExpr, join_type: JoinType, left_width: usize, right_width: usize, catalog: &Catalog) -> PhysicalPlan{
    let mut pairs = Vec::new();
    equi_pairs(&on, &mut pairs);
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    for (a, b) in &pairs {
        if *a < left_width && *b >= left_width {
            left_keys.push(*a);
            right_keys.push(*b - left_width);
        } else if *b < left_width && *a >= left_width {
            left_keys.push(*b);
            right_keys.push(*a - left_width);
        }
    }

    let lc = cost(&left, catalog);
    let rc = cost(&right, catalog);
    let jstats = join_cardinality(&lc.stats, &rc.stats, &on, &join_type);
    let nlj_marginal = lc.stats.rows * rc.stats.rows * DEFAULT_CPU_TUPLE_COST;
    let hash_marginal = (lc.stats.rows + rc.stats.rows + jstats.rows) * DEFAULT_CPU_TUPLE_COST;
    if !left_keys.is_empty() && hash_marginal < nlj_marginal {
        PhysicalPlan::HashJoin { left: Box::new(left), right: Box::new(right), on, join_type, left_keys, right_keys, right_width }
    } else {
        PhysicalPlan::NestedLoopJoin { left: Box::new(left), right: Box::new(right), on, join_type, right_width }
    }
}

/// Where a scan of the `(value, pk)` secondary tree must START, for a bound in the column's own
/// value space.
///
/// `Included(v)` and `Excluded(v)` both open at `(v, Value::Null)`, and that is not an oversight.
/// `Null` sorts below every pk (type rank 0, and column 0 is `NOT NULL`), so `(v, Null)` is exactly
/// the first key whose value is `v` — right for `Included`. For `Excluded` it is the first key that
/// bound **excludes**, and no key is right: the scan would have to start just past the LAST key
/// with value `v`, and there is no maximum pk to write down.
///
/// # D179 — so the start key does not carry the exclusion, and the executor does
///
/// `SecondaryIndexScan::next` skips the leading run of entries whose value is still `v` — the exact
/// mirror of the `sec_upper` check it has always carried, for the same reason: a bound in value
/// space is not a key in `(value, pk)` space. That is why this function is total and why it returns
/// a start key rather than a bound. **It is not a predicate about what is supported**, and a reader
/// who turns it back into one will reintroduce D178's defect from the other side.
///
/// Before D179 this returned `Option` with `None` for `Excluded`, and `lower` turned that `None`
/// into `FerroError::Bind("lower bound sec index isn't supported")`. `WHERE sec > v` therefore
/// could not use the index at all: D178 made the optimizer stop CHOOSING such a plan (so the error
/// became unreachable and the query fell back to a correct sequential scan) but could not make the
/// plan buildable. Measured at `fe40276` + D176's counters, in `bench/d178_run1_BEFORE_RAW.txt`, on
/// a 1,000-row table with an index on `v` and `ANALYZE` run:
///
/// ```text
///   SELECT id FROM h WHERE v > 9980 ;  ERROR: binding error: lower bound sec index isn't supported
///   SELECT id FROM h WHERE v > 9900 ;  OK, 9 rows
/// ```
///
/// Both of those now build, and both use the index.
///
/// ## What went away with the `Option`, and why nothing replaced it
///
/// `index_scan_lowerable` — D178's gate in `build_index_scan`, which passed over a conjunct `lower`
/// could not build and considered the next one. Every bound is now buildable against every tree
/// shape, so the gate could only ever return `true`: a branch that cannot be false is one no test
/// can hold to account, and keeping it would have left a guard that looks like a check and is not.
/// `lower`'s matching `ok_or_else` went the same way and for the same reason. `lower` still refuses
/// a hand-built `PhysicalPlan` that names a secondary column carrying no index
/// (`plan::tests::test_index_scan_rejects_unindexed_secondary_column` pins it) — that refusal is
/// about a tree that does not exist, which is a fact about the catalog and stays true.
fn secondary_scan_start(lower: &Bound<Value>) -> Bound<(Value, Value)> {
    match lower {
        Bound::Included(v) | Bound::Excluded(v) => Bound::Included((v.clone(), Value::Null)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Choose an access path for a single-relation `Filter`: the cheapest of the sequential scan and
/// one `IndexScan` per usable conjunct.
///
/// # D181 — this used to cost ONE candidate, and which one was decided by the user's typing
///
/// The selection was `conjuncts.iter().position(..)`, and `position` returns the FIRST match.
/// `split_and` preserves source order, so with two usable indexed conjuncts exactly one candidate
/// was ever built — the leftmost — and it was costed against the sequential scan and against
/// nothing else. The second index was never built, never costed, and could not win. **Index
/// selection was decided by the order the predicate was typed**, and the cost model was only ever
/// asked to ratify that choice against a full scan.
///
/// Measured in `bench/d181_conjunct_order_BEFORE_RAW.txt` (`examples/d181_conjunct_order.rs`), on
/// a table with two indexed secondary columns — `sel` unique, `broad` two-valued — after `ANALYZE`,
/// in rows examined (heap tuples pulled + index entries walked) for the SAME predicate:
///
/// ```text
///     n                                  400      800     1600
///     SELECT .. WHERE sel = k AND broad = b     400        2        2
///     SELECT .. WHERE broad = b AND sel = k     400      800     1600
///     gap                                         0      798     1598      <- n - 2, not a constant
/// ```
///
/// The fire-check arm (`pad = 'zz'`, no index) read exactly `n` at every size in the same process,
/// so the zeros above are readings and not a dead counter, and every arm returned its one row.
/// Typing `broad` first did not merely pick the worse index: the broad candidate loses to the
/// sequential scan on cost, so the statement reached NO index at all and read the whole table,
/// while the other spelling of the identical predicate read two index entries.
///
/// Note the `ANALYZE: no` half of that run shows no gap. Without statistics both columns get
/// `DEFAULT_DISTINCT`, so the two candidates cost the same and both lose to the sequential scan —
/// the defect needs statistics to become visible, which is exactly the case where the engine has
/// the information to choose correctly and throws it away.
///
/// # What this does instead
///
/// Every conjunct that `predicate_to_bounds` can read AND whose column carries an index becomes a
/// candidate plan, each with the remaining conjuncts as a residual `Filter`; all of them are costed
/// and the cheapest wins. The sequential scan is the starting incumbent rather than a special case,
/// so it still wins ties — the same `<` comparison as before, and the same result for every
/// predicate with at most one usable conjunct.
///
/// # The tie, and why it is broken on the candidate rather than on where it was typed
///
/// A strict `<` alone leaves the defect's residue: on an EXACT cost tie the incumbent stands, so
/// between two index candidates the earliest-typed would win and typing order would still decide.
/// Cost is an estimate, so two plans the model scores identically can examine wildly different
/// numbers of rows — which means the residue is not merely cosmetic, and a test asserting that the
/// two typing orders examine the same rows would fail intermittently rather than never.
///
/// So a tie is broken on [`candidate_key`] — `(column, lower bound, upper bound)`, a property of
/// the candidate and not of its position in the predicate. Because the update rule keeps the
/// running MINIMUM key among equal-cost candidates, the result is the same whichever order the
/// conjuncts arrive in, for any number of them. Plan choice is then a function of the predicate
/// SET, which is the law D181 is actually about.
///
/// A tie with the SEQUENTIAL incumbent still keeps the sequential scan, exactly as `<` did: the
/// incumbent carries no key, and that case is the first thing the match arm below rules out.
///
/// What remains order-dependent is the residual `Filter`'s own conjunct order —
/// `Filter(#2 = 1 AND #1 = 801)` against `Filter(#1 = 801 AND #2 = 1)`. Both evaluate both
/// conjuncts over the same rows, so no access path and no row count depends on it. Tests assert
/// COUNTERS across a typing swap, not plan-string equality, for that reason.
///
/// This is one candidate per conjunct, not a subset search: an `IndexScan` here reads one tree and
/// a multi-index intersection is a different physical operator this engine does not have.
fn build_index_scan(table: &str, predicate: &BoundExpr, catalog: &Catalog) -> Option<PhysicalPlan> {
    let mut conjuncts = Vec::new();
    let entry = catalog.get_table(table)?;
    split_and(predicate.clone(), &mut conjuncts);

    // The incumbent. `optimize` builds exactly this tree when we return `None`, so starting here
    // rather than returning early keeps the no-usable-conjunct case identical to what it was.
    // `best_key` is `None` for it, which is what makes a tie against the sequential scan keep the
    // sequential scan.
    let mut best = PhysicalPlan::Filter { input: Box::new(PhysicalPlan::SeqScan { table: table.into() }), predicate: predicate.clone() };
    let mut best_cost = cost(&best, catalog).cost;
    let mut best_key: Option<CandidateKey> = None;

    for (i, conjunct) in conjuncts.iter().enumerate() {
        let Some((column, lower, upper)) = predicate_to_bounds(conjunct) else { continue };
        if !has_index(entry, column) { continue }
        let key = candidate_key(column, &lower, &upper);
        let scan = PhysicalPlan::IndexScan { table: table.into(), column, lower, upper };
        let residual: Vec<BoundExpr> = conjuncts.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, c)| c.clone()).collect();
        let candidate = if residual.is_empty() {
            scan
        } else {
            PhysicalPlan::Filter { input: Box::new(scan), predicate: combine_and(residual) }
        };
        let candidate_cost = cost(&candidate, catalog).cost;
        let better = match (candidate_cost.partial_cmp(&best_cost), &best_key) {
            (Some(Ordering::Less), _) => true,
            // A tie between two INDEX candidates, broken on the candidate itself. See the doc.
            (Some(Ordering::Equal), Some(incumbent)) => &key < incumbent,
            // A tie with the sequential incumbent, or a NaN cost: keep what we have.
            _ => false,
        };
        if better {
            best_cost = candidate_cost;
            best_key = Some(key);
            best = candidate;
        }
    }
    Some(best)
}

/// The tie-break key for an `IndexScan` candidate — see [`build_index_scan`].
///
/// `Bound` does not implement `Ord` (std derives only `Clone, Copy, Debug, Hash, PartialEq, Eq`),
/// so the bounds are ranked explicitly. The ranks are arbitrary and that is fine: the key exists to
/// be TOTAL and to depend on nothing but the candidate, not to express a preference. What it must
/// not do is read the conjunct's position, because that is the defect.
type CandidateKey = (usize, (u8, Option<Value>), (u8, Option<Value>));

fn candidate_key(column: usize, lower: &Bound<Value>, upper: &Bound<Value>) -> CandidateKey {
    (column, bound_key(lower), bound_key(upper))
}

fn bound_key(bound: &Bound<Value>) -> (u8, Option<Value>) {
    match bound {
        Bound::Unbounded => (0, None),
        Bound::Included(v) => (1, Some(v.clone())),
        Bound::Excluded(v) => (2, Some(v.clone())),
    }
}

fn has_index(entry: &TableEntry, col: usize) -> bool {
    col == 0 || entry.schema.columns.get(col).is_some_and(|c| entry.indexes.iter().any(|i| i.column_name == c.name))
}

pub fn split_and(expr: BoundExpr, output: &mut Vec<BoundExpr>) {
    match expr {
        BoundExpr::BinaryOp { left, operator:TokenType::And, right } => {
            split_and(*left, output);
            split_and(*right, output);
        }
        other => output.push(other),
    }
}

pub fn combine_and(mut conjuncts: Vec<BoundExpr>) -> BoundExpr {
    let mut combined = conjuncts.remove(0);
    for conjunct in conjuncts {
        combined = BoundExpr::BinaryOp { left: Box::new(combined), operator: TokenType::And, right: Box::new(conjunct)
        }
    }
    combined
}

pub fn collect_columns(expr: &BoundExpr, output: &mut HashSet<usize>) {
    match expr {
        BoundExpr::BinaryOp { left, right, .. } => {
            collect_columns(left, output);
            collect_columns(right, output);
        }
        BoundExpr::UnaryOp { right, .. } => collect_columns(right, output),
        
        BoundExpr::Column(i) => {output.insert(*i);}
        BoundExpr::Literal(_) => {}
    }
}

pub fn remap(expr: BoundExpr, offset: usize) -> BoundExpr {
    match expr {
        BoundExpr::BinaryOp { left, operator, right } => { BoundExpr::BinaryOp { left: Box::new(remap(*left, offset)), operator, right: Box::new(remap(*right, offset))} }
        BoundExpr::UnaryOp { operator, right } => { BoundExpr::UnaryOp { operator, right: Box::new(remap(*right, offset)) } }
        BoundExpr::Literal(v) => BoundExpr::Literal(v),
        BoundExpr::Column(i) => BoundExpr::Column(i-offset)
    }
}

pub fn wrap_filter(plan: LogicalPlan, conjuncts: Vec<BoundExpr>) -> LogicalPlan {
    if conjuncts.is_empty() {
        plan
    } else {
        LogicalPlan::Filter { input: Box::new(plan), predicate: combine_and(conjuncts) }
    }
}

pub fn push(plan: LogicalPlan, carried: Vec<BoundExpr>) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let mut c = carried;
            split_and(predicate, &mut c);
            push(*input, c)
        }
        LogicalPlan::Join { left, right, join_type, on } => {
            let left_width = left.output_schema().len();
            let (mut go_left, mut go_right, mut stay) = (Vec::new(), Vec::new(), Vec::new());
            for expr in carried {
                let mut cols = HashSet::new();
                collect_columns(&expr, &mut cols);
                if cols.is_empty() {
                    stay.push(expr);
                } else if cols.iter().all(|&c| c < left_width) {
                    go_left.push(expr);
                } else if cols.iter().all(|&c| c >= left_width) {
                    go_right.push(remap(expr, left_width));
                } else {
                    stay.push(expr);
                }
            }

            let joined = LogicalPlan::Join { left: Box::new(push(*left, go_left)), right: Box::new(push(*right, go_right)), join_type, on };
            wrap_filter(joined, stay)
        }
        LogicalPlan::Projection { input, exprs, output } => {
            let inner = push(*input, Vec::new());
            let proj = LogicalPlan::Projection { input: Box::new(inner), exprs, output };
            wrap_filter(proj, carried)
        }
        LogicalPlan::Scan { .. } => wrap_filter(plan, carried)
    }
}

pub fn pushdown(plan: LogicalPlan) -> LogicalPlan {
    push(plan, Vec::new())
}

pub fn explain_plan(plan: &PhysicalPlan, catalog: &Catalog) -> String {
    let mut out = String::new();
    format_node(plan, catalog, 0, &mut out);
    out
}    

fn format_node(plan: &PhysicalPlan, catalog: &Catalog, indent: usize, out: &mut String) {
    let pad = "  ".repeat(indent);
    let text = match plan {
        PhysicalPlan::Filter { predicate, .. } => format!("Filter ({})", format_expr(predicate)),
        PhysicalPlan::IndexScan { table, column, lower, upper } => format!("Index scan on {} (col {}, {})", table, column, format_bounds(lower,upper)),
        PhysicalPlan::NestedLoopJoin {  on, join_type, .. } => format!("Nested loop join {:?} (on {})", join_type, format_expr(on)),
        PhysicalPlan::Projection { exprs, .. } => format!("Projection [{}]", exprs.iter().map(|e| format_expr(e)).collect::<Vec<_>>().join(", ")),
        PhysicalPlan::SeqScan { table } => format!("Sequential scan on {}", table),
        PhysicalPlan::HashJoin { join_type, on, .. } => format!("Hash join {:?} (on {})", join_type, format_expr(on)),
    };
    let costed = cost(plan, catalog);
    out.push_str(&format!("{}{} (rows={:.0} cost={:.2})\n", pad, text, costed.stats.rows, costed.cost));
    match plan {
        PhysicalPlan::Filter { input, .. } => format_node(input, catalog, indent + 1, out),
        PhysicalPlan::IndexScan { .. } => {}
        PhysicalPlan::NestedLoopJoin { left, right, .. }  | PhysicalPlan::HashJoin { left, right,.. } => {
            format_node(left, catalog, indent + 1, out);
            format_node(right, catalog, indent + 1, out);
        }
        PhysicalPlan::Projection { input, .. } => format_node(input, catalog, indent + 1, out),
        PhysicalPlan::SeqScan { .. } => {},
    }
}

fn format_expr(e: &BoundExpr) -> String {
    match e {
        BoundExpr::BinaryOp { left, operator, right } => {
            format!("{} {} {}", format_expr(left), op_symbol(*operator), format_expr(right))
        }
        BoundExpr::Column(i) => format!("#{}", i),
        BoundExpr::UnaryOp { operator, right } => {
            format!("{} {}", op_symbol(*operator), format_expr(right))
        }
        BoundExpr::Literal(v) => format_value(v)
    }
}

fn op_symbol(op: TokenType) -> &'static str {
    match op {
        TokenType::Equal => "=",
        TokenType::BangEqual => "!=",
        TokenType::Less => "<",
        TokenType::LessEqual => "<=",
        TokenType::Greater => ">",
        TokenType::GreaterEqual => ">=",
        TokenType::And => "AND",
        TokenType::Or => "OR",
        TokenType::Not => "NOT",
        _ => "?"
    }
}

fn format_value(v: &Value) -> String {
    match v {
        Value::Boolean(b) => b.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::Timestamp(ms) => ms.to_string(),
        Value::Null => "NULL".into(),
        Value::Varchar(s) => format!("'{}'", s)
    }
}

fn format_bounds(lower: &Bound<Value>, upper: &Bound<Value>) -> String {
    if let (Bound::Included(l), Bound::Excluded(h)) = (lower, upper) {
        if l == h { return format!("= {}", format_value(l));}
    }
    let l = match lower {
        Bound::Excluded(v) => format!("({}", format_value(v)),
        Bound::Included(v) => format!("[{}", format_value(v)),
        Bound::Unbounded => "(-inf".into()
    };
    let u = match upper {
        Bound::Excluded(v) => format!("{})", format_value(v)),
        Bound::Included(v) => format!("{}]", format_value(v)),
        Bound::Unbounded => "inf)".into()
    };
    format!("{}, {}", l, u)
}

#[cfg(test)]
mod tests {
    use crate::{binder::binder::BoundColumn, catalog::column::DataType};
    use super::*;

    #[test]
    fn test_split_and_roundtrip() {
        let expr = BoundExpr::BinaryOp { left: Box::new(
            BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(1)), operator: TokenType::And, right: Box::new(BoundExpr::Column(3)) }
        ), operator: TokenType::And, right: Box::new(BoundExpr::Column(2)) };
        let mut output: Vec<BoundExpr> = Vec::new();
        split_and(expr, &mut output);
        assert_eq!(output, vec![BoundExpr::Column(1), BoundExpr::Column(3), BoundExpr::Column(2) ]);
    }

    #[test]
    fn test_combine_column() {
        let conjuncts = vec![BoundExpr::Column(1), BoundExpr::Column(3), BoundExpr::Column(2)];
        let expr = BoundExpr::BinaryOp { left: Box::new(
            BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(1)), operator: TokenType::And, right: Box::new(BoundExpr::Column(3)) }
        ), operator: TokenType::And, right: Box::new(BoundExpr::Column(2)) };
        let combined = combine_and(conjuncts);
        assert_eq!(combined, expr);
    }

    #[test]
    fn test_collect_columns() {
        let mut output = HashSet::new();
        let expr = BoundExpr::BinaryOp { 
            left: Box::new(BoundExpr::UnaryOp { 
                operator: TokenType::Not, 
                right: Box::new(BoundExpr::Literal(Value::Varchar("idk".into()))) 
            }), 
            operator: TokenType::Or, 
            right: Box::new(BoundExpr::BinaryOp { 
                left: Box::new(BoundExpr::Column(1)), 
                operator: TokenType::And, 
                right: Box::new(BoundExpr::Column(3)), 
            }), 
        };
        collect_columns(&expr, &mut output);
        assert_eq!(output, HashSet::from([1, 3]))
    }

    #[test]
    fn test_remap() {
        let expr = BoundExpr::UnaryOp { operator: TokenType::Not, right: Box::new(BoundExpr::Column(3)) };
        assert_eq!(remap(expr, 2), BoundExpr::UnaryOp { operator: TokenType::Not, right: Box::new(BoundExpr::Column(1)) });
        let nested = BoundExpr::BinaryOp { 
            left: Box::new(BoundExpr::UnaryOp { operator: TokenType::Not, right: Box::new(BoundExpr::Column(5))}), 
            operator: TokenType::Not, 
            right: Box::new(BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(4)), operator: TokenType::And, right: Box::new(BoundExpr::Column(3)) })
        };
        assert_eq!(remap(nested, 2), BoundExpr::BinaryOp { 
            left: Box::new(BoundExpr::UnaryOp { operator: TokenType::Not, right: Box::new(BoundExpr::Column(3))}), 
            operator: TokenType::Not, 
            right: Box::new(BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(2)), operator: TokenType::And, right: Box::new(BoundExpr::Column(1)) })
        })
    }

    #[test]
    fn test_wrap_filter_empty_conjuncts() {
        let plan = LogicalPlan::Projection { input: 
            Box::new(LogicalPlan::Scan { table: "users".into(), alias: Some("u".into()), output: vec![]}), 
            exprs: vec![BoundExpr::Column(1)], 
            output: vec![BoundColumn {qualifier: "p".into(), name: "p".into(), data_type: DataType::Integer, nullable: true}]
        };
        let res = wrap_filter(plan.clone(), vec![]);
        assert_eq!(res, plan);
    }

    #[test]
    fn test_wrap_filter_non_empty_conjuncts() {
        let plan = LogicalPlan::Projection { input: 
            Box::new(LogicalPlan::Scan { table: "users".into(), alias: Some("u".into()), output: vec![]}), 
            exprs: vec![BoundExpr::Column(1)], 
            output: vec![BoundColumn {qualifier: "p".into(), name: "p".into(), data_type: DataType::Integer, nullable: true}]
        };
        let res = wrap_filter(plan.clone(), vec![BoundExpr::Column(1), BoundExpr::Column(2)]);
        assert_eq!(res, LogicalPlan::Filter { input: Box::new(plan), predicate: BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(1)), operator: TokenType::And, right: Box::new(BoundExpr::Column(2)) } })
    }

    #[test]
    fn test_push_split_remap() {
        let c = BoundColumn { qualifier: "t".into(), name: "x".into(), data_type: DataType::Integer, nullable: true};
        let plan = LogicalPlan::Filter { 
            input: Box::new(LogicalPlan::Join { 
                left: Box::new(LogicalPlan::Scan { table: "users".into(), alias: None, output: vec![c.clone(), c.clone()]}),  
                right: Box::new(LogicalPlan::Scan { table: "posts".into(), alias: None, output: vec![c.clone(), c.clone(), c.clone()] }), 
                join_type: JoinType::Inner, 
                on: BoundExpr::Literal(Value::Boolean(true))
            }), 
            predicate: BoundExpr::BinaryOp { 
                left: Box::new(BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(0)), operator: TokenType::Equal, right: Box::new(BoundExpr::Literal(Value::Integer(5))) }), 
                operator: TokenType::And, 
                right: Box::new(BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(3)), operator: TokenType::Equal, right: Box::new(BoundExpr::Literal(Value::Integer(7))) })
            }
        };
        match pushdown(plan) {
            LogicalPlan::Join { left, right, .. } => match (*left, *right) {
                (LogicalPlan::Filter { input: li, predicate: lp }, LogicalPlan::Filter { input: ri, predicate: rp }) => {
                    assert_eq!(lp, BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(0)), operator: TokenType::Equal, right: Box::new(BoundExpr::Literal(Value::Integer(5))) });
                    assert!(matches!(*li, LogicalPlan::Scan { table, .. } if table == "users"));
                    assert_eq!(rp, BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(1)), operator: TokenType::Equal, right: Box::new(BoundExpr::Literal(Value::Integer(7))) });
                    assert!(matches!(*ri, LogicalPlan::Scan { table, ..} if table == "posts"));
                }   
                _ => panic!()
            }
            _ => panic!()
        }
    }

    #[test]
    fn test_push_spanning_predicate_stays() {
        let c = BoundColumn { qualifier: "t".into(), name: "x".into(), data_type: DataType::Integer, nullable: true};
        let spanning = BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(0)), operator: TokenType::Greater, right: Box::new(BoundExpr::Column(3)) };
        let plan = LogicalPlan::Join { 
            left: Box::new(LogicalPlan::Scan { table: "users".into(), alias: None, output: vec![c.clone(), c.clone()] }), 
            right: Box::new(LogicalPlan::Scan { table: "posts".into(), alias: None, output: vec![c.clone(), c.clone(), c.clone()] }), 
            join_type: JoinType::Inner, 
            on: BoundExpr::Literal(Value::Boolean(true)) 
        };
        match push(plan, vec![spanning.clone()]) {
            LogicalPlan::Filter { input, predicate } => {
                assert_eq!(predicate, spanning);
                assert!(matches!(*input, LogicalPlan::Join{..}));
            }
            _ => panic!()
        }
    }
}