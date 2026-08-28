import subprocess, sys, os
MUT = sys.argv[1]
p_alter = 'src/catalog/alter.rs'
p_rt    = 'src/agent_sql/runtime.rs'

def sub(path, old, new):
    s = open(path).read()
    assert s.count(old) == 1, (path, s.count(old), old[:80])
    open(path,'w').write(s.replace(old, new))

if MUT == 'M1':
    # the publish precheck: stop measuring a row against the shape it lands in
    sub(p_rt,
"""                let which = format!("the row this merge inserts with key {:?}", row.first());
                refuse_if_the_row_cannot_land(&table, to, &row, &which)?;""",
"""                let _ = &row;""")
    sub(p_rt,
"""                let which = format!("the row this merge updates with key {key:?}");
                refuse_if_the_row_cannot_land(&table, to, &row, &which)?;""",
"""                let _ = &key;""")
elif MUT == 'M2':
    # measure only the LAST step of a chain instead of every step
    sub(p_alter,
"""            let (bytes, fits) = serialize_and_measure(&values, &shapes[step])?;
            if !fits {""",
"""            let (bytes, fits) = serialize_and_measure(&values, &shapes[step])?;
            if !fits && step == shapes.len() - 1 {""")
elif MUT == 'M3':
    # group atomicity: plan nothing, apply the edits one alter_table call at a time (the pre-fix
    # shape of the schema half, kept BEFORE the publish so this isolates "the group is atomic"
    # from "the schema goes first").
    sub(p_rt,
"""        let mut plans: Vec<(usize, AlterPlan)> = Vec::new();
        for (i, report) in schema_reports.iter().enumerate() {
            if report.to_apply.is_empty() {
                continue;
            }
            let actions: Vec<AlterAction> = report.to_apply.iter().map(|e| e.as_action()).collect();
            let plan = ctx
                .catalog
                .plan_alters(&report.table, &actions, &ctx.txn, Some(&prov))
                .map_err(in_a_merge_the_narrowing_comes_first)?;
            plans.push((i, plan));
        }""",
"""        let plans: Vec<(usize, AlterPlan)> = Vec::new();
        let _ = in_a_merge_the_narrowing_comes_first;
        for report in schema_reports.iter() {
            for edit in &report.to_apply {
                let action = edit.as_action();
                let (dir_root, tt_root, alteration) = {
                    let entry = ctx.catalog.require_table(&report.table)?;
                    (
                        entry.first_directory_page_id,
                        entry.time_travel_root,
                        alteration_of(&action, &entry.schema)?,
                    )
                };
                let columns =
                    ctx.catalog.alter_table(&report.table, &action, &ctx.txn, Some(&prov))?;
                ctx.bp.flush_all()?;
                ctx.bp.disk_manager.sync()?;
                ctx.txn.log_ddl(crate::wal::txn::DdlRecord {
                    op: crate::wal::log::DdlOp::AlterColumn(alteration),
                    table: report.table.clone(),
                    dir_root,
                    time_travel_root: tt_root,
                    columns,
                })?;
            }
        }""")
elif MUT == 'M4':
    # rows no longer carried into the shape the merge's own edits produced
    sub(p_rt,
"""            PendingWrite::Insert { table, row } => {
                let row = conform_row(&row, from, to)?;""",
"""            PendingWrite::Insert { table, row } => {
                let _ = (from, to);""")
elif MUT == 'M5':
    # an index stops following its column across a rename
    sub(p_alter,
"""                for ind in entry.indexes.iter_mut() {
                    if &ind.column_name == from {
                        ind.column_name = to.clone();
                    }
                }
                for ind in entry.fulltext_indexes.iter_mut() {""",
"""                for ind in entry.fulltext_indexes.iter_mut() {""")
elif MUT == 'M6':
    # the width refusal stops being told what it means inside a merge
    sub(p_rt,
"""                .plan_alters(&report.table, &actions, &ctx.txn, Some(&prov))
                .map_err(in_a_merge_the_narrowing_comes_first)?;""",
"""                .plan_alters(&report.table, &actions, &ctx.txn, Some(&prov))?;
            let _ = in_a_merge_the_narrowing_comes_first;""")
elif MUT == 'M7':
    # apply_plan installs the second-to-last shape: the catalog then disagrees with the heap
    sub(p_alter,
"""        let new_schema = shapes
            .last()
            .cloned()""",
"""        let new_schema = shapes
            .get(shapes.len().saturating_sub(2))
            .cloned()""")
elif MUT == 'M8':
    # the publish precheck stops asking about NOT NULL
    sub(p_alter,
"""    for (i, c) in schema.columns.iter().enumerate() {
        if c.nullable || !matches!(values.get(i), None | Some(Value::Null)) {
            continue;
        }""",
"""    for (i, c) in schema.columns.iter().enumerate() {
        if true || c.nullable || !matches!(values.get(i), None | Some(Value::Null)) {
            continue;
        }""")
elif MUT == 'M9':
    # a rename stops following its column into the full-text index list
    sub(p_alter,
"""                for ind in entry.fulltext_indexes.iter_mut() {
                    if &ind.column_name == from {
                        ind.column_name = to.clone();
                    }
                }""",
"""                let _ = &entry.fulltext_indexes;""")
elif MUT == 'M10':
    # the reservation goes back into the planning half, where it writes and then refuses
    sub(p_alter,
"""        let growth: usize = prepared
            .iter()
            .filter(|p| p.tuple.data.len() > p.was)
            .map(|p| p.tuple.data.len() + SLOT_ENTRY_SIZE)
            .sum();
        if growth > 0 {
            HeapFileManager::open(dir_root, self.buffer_pool.clone()).reserve_free_space(growth)?;
        }

        let primary_root_now =""",
"""        let primary_root_now =""")
    sub(p_alter,
"""        let prepared = prepare_rewrite(&self.buffer_pool, table, dir_root, &shapes, actions, prov)?;

        Ok(AlterPlan {""",
"""        let prepared = prepare_rewrite(&self.buffer_pool, table, dir_root, &shapes, actions, prov)?;

        let growth: usize = prepared
            .iter()
            .filter(|p| p.tuple.data.len() > p.was)
            .map(|p| p.tuple.data.len() + SLOT_ENTRY_SIZE)
            .sum();
        if growth > 0 {
            HeapFileManager::open(dir_root, self.buffer_pool.clone()).reserve_free_space(growth)?;
        }

        Ok(AlterPlan {""")
else:
    sys.exit("unknown mutant " + MUT)
print("applied " + MUT)
