# F11 — the server must actually reap

## Done
- Read DISTRIBUTED.md, `src/consensus/mod.rs` (frozen), the brief, ledger row S2b.
- Established the design (see DESIGN NOTES below).

## Doing right now
- Writing `src/branch/lease_thread.rs`.

## Single next action
- `src/branch/lease_thread.rs`: `RuntimeLock` trait + `LeaseThread::start/stop` + unit tests.

## DESIGN NOTES (do not re-derive)
- S2b is stale as the brief says: `src/cli/cli.rs:88` and `examples/pgserver.rs:83` both build
  `ArenaPageStore`, and `pgwire::ServerContext` holds a storage-backed `AgentRuntime`. There IS a
  branch engine to reap from in both shipped entry points.
- Nothing wires `TwoTierReaper` into either binary, so:
  * no lease scan at all (criterion 8 / "THE THESIS" is component-level only), and
  * `AgentRuntime::seal` takes its no-reaper branch, which marks a merged/abandoned branch
    `Reaped` through the `BranchCatalog` trait and **never frees its extents**. Pages leak on
    every merge and every ABANDON in the shipped binary.
- The "runtime lock" a merge holds is the pgwire **catalog mutex**: `pgwire::serve`'s doc says it
  is taken OUTERMOST for the duration of one statement, and `Statement::execute`
  (`src/pgwire/extended.rs:309`) is the single place both the simple and extended paths run SQL.
  So `MERGE` is strictly inside it. The lease scan takes that same lock => no new lock, no new
  lock order. Trait `RuntimeLock` in lease_thread.rs, impl for `ServerContext` and for the CLI's
  `Arc<Mutex<Catalog>>`.
- `resume_interrupted_reaps` is an inherent method on `TwoTierReaper`, NOT on the `Reaper` trait,
  so the lease thread holds `Arc<TwoTierReaper>`.
- `AgentRuntime::forget_reaped_branches` exists for exactly this caller ("A branch reclaimed by
  the lease reaper ... nothing here is told, and its workspace stays in the map for the life of
  the process"). The scan must call it after a reap.
- Clock: `LeaseDeadline::try_now_millis()` -> `cluster::lease_now_millis()`. Standalone = local
  wall clock; a cluster member with no applied tick REFUSES. The scan must refuse to reap rather
  than substitute a reading.
- Test fixture levers: `LeaseDeadline(0)` = expired, no clock read needed. `ClusterScope::joined`
  arms the process for the refusal test. `DbLock` is NOT released by SIGKILL, so a multi-phase
  server test must delete `<db>.lock` between phases (the lock's own doc says that is the
  operator action).
- `store.checkpoint_to(path)` is already set in both binaries, and `free_arena` ->
  `persist_if_configured`, so a reap's frees are durable without reaching the exit checkpoint.
