//! D9 — serve the Postgres wire subset on a TCP port.
//!
//! `cargo run --release --example pgserver -- <db-path> <addr>`
//!
//! Prints the bound address on stdout before accepting, so a test can wait for readiness instead
//! of sleeping and hoping.

use std::path::Path;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::TableBranchCatalog;
use ferrodb::branch::lease_thread::{scan_interval_from_env, LeaseThread, RuntimeLock};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::cow::PageStore;
use ferrodb::pgwire::{serve, ServerContext};
use ferrodb::storage::db_lock::DbLock;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::recovery::{open_recovered, OpenedDatabase};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "ferro.db".into());
    let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:0".into());

    // Read BEFORE the lock, because the refusal path below is `process::exit`, which does not run
    // destructors. Parsed after `DbLock::acquire`, a misconfigured `FERRODB_LEASE_SCAN_MILLIS`
    // would exit past the lock's `Drop` and leave `<db>.lock` behind, so the operator's next
    // attempt — with the variable corrected — would be refused as "already open by process N".
    // Nothing here touches a file, so there is no reason for it to happen after anything.
    let interval = scan_interval_from_env().unwrap_or_else(|e| {
        eprintln!("pgserver: {e}");
        std::process::exit(1);
    });

    // Before any file is opened: a second writer on one database aliases arena pages, and every
    // aliased page still checksums correctly, so refusing here is the only detection point.
    let _lock = match DbLock::acquire(Path::new(&db)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("pgserver: {e}");
            std::process::exit(1);
        }
    };
    // **D204: the one open path.** Recovery; every index rebuilt from the recovered heap if
    // recovery replayed a data record or the stale-indexes marker asks; then a checkpoint after a
    // rebuild or whenever the log holds records. Through the same function the CLI calls. This
    // file used to spell the sequence out itself, and from D9 until D202 its copy omitted the
    // rebuild. Index pages are not logged, so a
    // row committed after the last checkpoint came back in the heap but not in its primary index: a
    // lookup by key missed it, and an INSERT of its key was admitted as a second live row
    // (`tests/pgserver_crash_rebuilds_indexes.rs`). `tests/open_path_allowlist.rs` keeps it from
    // drifting again. Before the arena below, for the reason given there: the rebuild allocates pages.
    // It also holds D280's refusal (`WalManager::open_for_database`, inside `open_recovered`): a data
    // file whose pages carry LSNs its log never issued (a restored backup, a replica's file, a lost
    // `.wal`) is refused before the log is created, the same refusal as the CLI's.
    //
    // On failure: the message, the lock released BY HAND, then `process::exit(1)`. This runs after
    // the lock, and `process::exit` runs no destructors, so exiting without the `drop` would strand
    // `<db>.lock` and the next open would refuse it as stale (the reason D204 chose a panic here, and
    // D280 the explicit drop; the drop keeps D280's exit status and leaves no lock behind either way).
    let OpenedDatabase { bp, txn, catalog, .. } = match open_recovered(Path::new(&db), &_lock) {
        Ok(opened) => opened,
        Err(e) => {
            eprintln!("pgserver: {e}");
            drop(_lock);
            std::process::exit(1);
        }
    };

    let listener = std::net::TcpListener::bind(&addr).expect("bind");
    // Readiness, not a guess: the test reads this line rather than sleeping.
    // Writes that tolerate a closed pipe. `println!` PANICS on EPIPE, and a harness that reads the
    // readiness line and then drops its reader closes this pipe underneath us. Proven on
    // `cdc_server`: closing stdout before the first write kills it with `failed printing to stdout:
    // Broken pipe (os error 32)` and exit 101, which is the status an intermittent CI failure
    // reported. A server has no business dying because nobody is reading its log.
    use std::io::Write;
    let mut out = std::io::stdout();
    let _ = writeln!(out, "LISTENING {}", listener.local_addr().unwrap());
    let _ = out.flush();

    // Built AFTER the catalog, for the reason `src/cli/cli.rs` spells out: the arena floor has to
    // sit above what the catalog has already allocated, or the ordinary allocator and the arena hand
    // out the same page. The floor is persisted in the checkpoint, so a reopen reattaches to the
    // region it left rather than inventing a new one on top of live pages.
    let arena_path = format!("{db}.arena");
    let branches = Arc::new(
        TableBranchCatalog::default_for_database(&db, 1).expect("branch catalog"),
    );
    let arena_exists = Path::new(&arena_path).exists();
    let store: Arc<ArenaPageStore> = Arc::new(if arena_exists {
        ArenaPageStore::reopen_from_checkpoint(bp.clone(), branches.clone() as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>, Path::new(&arena_path))
            .expect("reattach to the arena")
    } else {
        let base = bp.disk_manager.high_water().expect("high water") + 32_736;
        ArenaPageStore::new(bp.clone(), branches.clone() as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>, base).expect("arena")
    });
    store.checkpoint_to(std::path::PathBuf::from(&arena_path));

    // F11 — the reaper, built over the SAME catalog and page store as the runtime, which is the
    // contract `with_reaper` states and cannot check.
    let reaper = Arc::new(TwoTierReaper::new(
        branches.clone() as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>,
        store.clone(),
    ));

    let runtime = Arc::new(
        if arena_exists {
            AgentRuntime::reopen_with_storage(
                branches.clone() as Arc<dyn BranchCatalog>,
                Arc::new(MemEffectLog::new()),
                store.clone() as Arc<dyn PageStore>,
            )
            .expect("reattach the runtime")
        } else {
            AgentRuntime::with_storage(
                branches.clone() as Arc<dyn BranchCatalog>,
                Arc::new(MemEffectLog::new()),
                store.clone() as Arc<dyn PageStore>,
            )
            .expect("storage-backed runtime")
        }
        // Retiring a branch now reclaims it. Without this, `seal` takes its no-reaper branch: a
        // merged or abandoned branch is marked `Reaped` and its extents are never freed, so every
        // `MERGE` and every `ABANDON` this server served leaked the branch's pages.
        .with_reaper(reaper.clone() as Arc<dyn Reaper>),
    );

    // One `Arc` shared by every connection thread; the catalog inside it is behind a mutex.
    let ctx = Arc::new(ServerContext::new(catalog, bp, txn, runtime.clone()));

    // THE LEASE SCAN. Started before the first connection is handled, and holding `ctx`'s catalog
    // mutex — the same outermost lock a statement takes — so a scan can never run inside a `MERGE`.
    // See `branch::lease_thread` for all three rules and why this is the right lock.
    //
    // A failure here PANICS rather than exiting, matching the other post-lock failures in this
    // file (`.expect("bind")`, `.expect("arena")`): a panic unwinds and drops the `DbLock`, and
    // `process::exit` would strand the lock file on a database this process is not holding. The
    // one post-lock exit, the open's refusal above, drops the lock by hand before it exits.
    let lease =
        LeaseThread::start(reaper, runtime, ctx.clone() as Arc<dyn RuntimeLock>, interval)
            .unwrap_or_else(|e| panic!("pgserver: {e}"));
    let _ = writeln!(
        std::io::stderr(),
        "pgserver: lease scan every {}ms; {} interrupted reap(s) finished on startup",
        interval.as_millis(),
        lease.resumed().len()
    );

    serve(listener, ctx).unwrap();

    // Stop the scan before the exit checkpoint: a scan that freed an extent after the map was
    // written would leave a durable map that still charges pages nothing owns.
    let stats = lease.stop();
    let _ = writeln!(std::io::stderr(), "pgserver: lease scan stopped after {stats:?}");
    store.checkpoint(Path::new(&arena_path)).expect("checkpoint the arena");
}
