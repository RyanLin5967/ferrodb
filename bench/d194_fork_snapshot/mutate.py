#!/usr/bin/env python3
"""Fire-check each D194 mechanism: apply one mutation, run the 4 targets, record FAILED tests, restore from HEAD."""
import subprocess, sys, pathlib, datetime
WT = pathlib.Path('/Users/idide/wt/ferrodb-d194-fork-snapshot')
OUT = WT / 'bench/d194_fork_snapshot'
LOCKED = '/private/tmp/claude-501/-Users-idide-projects-ferrodb/b2b44149-483d-42d1-b512-89bf5de5a135/scratchpad/locked.sh'
TARGETS = ['agent_sql_surface', 'integration_read_premise', 'integration_cherry_pick', 'integration_sibling_merge']
RT = 'src/agent_sql/runtime.rs'
M = [
 ('M01_lazy_pin_only', 'src/agent_sql/dispatch.rs',
  """            let (s, durability) = runtime.begin_session_pinned_staged(
                crate::agent_sql::runtime::RunIdentity {
                    agent_id: &agent_id,
                    run_id: run_id.as_deref(),
                    model,
                    prompt: prompt.as_deref(),
                },
                parent,
                &ctx.txn,
            )?;""",
  """            let (s, durability) = runtime.begin_session_as_staged(
                crate::agent_sql::runtime::RunIdentity {
                    agent_id: &agent_id,
                    run_id: run_id.as_deref(),
                    model,
                    prompt: prompt.as_deref(),
                },
                parent,
            )?;"""),
 ('M02_child_fresh_fork_seq', RT,
  "            Some(p) => (p.fork_seq, p.fork_snapshot.clone()),",
  "            Some(p) => (state.apply_seq, p.fork_snapshot.clone()),"),
 ('M03_version_seen_latest', RT,
  "        if latest.begin_ts <= through {\n            return Some(latest);",
  "        if latest.begin_ts <= through || true {\n            return Some(latest);"),
 ('M04_version_seen_no_history', RT,
  "        (at > 0).then(|| VersionRef { begin_ts: history[at - 1], ..latest })",
  "        (at > usize::MAX - 1).then(|| VersionRef { begin_ts: history[at - 1], ..latest })"),
 ('M05_observed_at_now', RT,
  "        let observed_at = seen_through.unwrap_or(state.apply_seq) + 1;",
  "        let observed_at = state.apply_seq + 1;"),
 ('M06_cherry_reads_current', RT,
  "Some(&pred), &ctx.read(), Arc::clone(&at))? {",
  "Some(&pred), &ctx.read(), ctx.txn.read_snapshot_cached())? {"),
 ('M07_sibling_same_view_forced', RT,
  "                None if same_view => base.clone(),",
  "                None if same_view || true => base.clone(),"),
 ('M08_sibling_op_staged_only', RT,
  "        match on_target {\n            Some(v) if v.get(idx) != base.get(idx) => v.get(idx).cloned().map(OpKind::Assign),",
  "        match tgt.rows.get(&(tbl.0, row.0)).and_then(|s| match s { RowState::Present(v) => Some(v), RowState::Deleted => None }).filter(|_| on_target.is_some() || true) {\n            Some(v) if v.get(idx) != base.get(idx) => v.get(idx).cloned().map(OpKind::Assign),"),
 ('M09_merge_reads_the_pin', RT,
  "                    let now = ctx.txn.read_snapshot_cached();",
  "                    let now = self.state.lock().unwrap().workspaces.get(&branch).and_then(|w| w.fork_snapshot.clone()).unwrap_or_else(|| ctx.txn.read_snapshot_cached());"),
 ('M10_branch_reads_now', RT,
  "        let base = scan_table_where(table, alias, raw, ctx, at)?;",
  "        let base = scan_table_where(table, alias, raw, ctx, { let _ = at; ctx.txn.read_snapshot_cached() })?;"),
]
only = set(sys.argv[1:])
def git(*a):
    return subprocess.run(['git', '-C', str(WT), *a], capture_output=True, text=True)
assert git('status', '--porcelain', '--', 'src').stdout.strip() == '', 'src dirty before fire-check'
summary = []
for name, f, old, new in M:
    if only and name not in only:
        continue
    p = WT / f
    src = p.read_text()
    n = src.count(old)
    if n != 1:
        summary.append(f'{name}: MUTATION NOT APPLIED (anchor count {n})')
        continue
    p.write_text(src.replace(old, new))
    out = OUT / f'firecheck_{name}.txt'
    args = [LOCKED, str(out), 'test', '--no-fail-fast']
    for t in TARGETS:
        args += ['--test', t]
    rc = subprocess.run(args, capture_output=True, text=True).returncode
    git('checkout', 'HEAD', '--', f)
    assert git('status', '--porcelain', '--', 'src').stdout.strip() == '', f'restore failed after {name}'
    text = out.read_text()
    failed = [l.split()[1] for l in text.splitlines() if l.startswith('test ') and l.endswith('FAILED')]
    build_err = 'error[' in text or 'could not compile' in text
    results = sum(1 for l in text.splitlines() if l.startswith('test result'))
    summary.append(f'{name}: rc={rc} build_error={build_err} result_lines={results} FAILED={failed}')
    print(summary[-1], flush=True)
(OUT / 'firecheck_summary.txt').write_text(f'# {datetime.datetime.utcnow().isoformat()}Z HEAD={git("rev-parse","--short","HEAD").stdout.strip()}\n' + '\n'.join(summary) + '\n')
