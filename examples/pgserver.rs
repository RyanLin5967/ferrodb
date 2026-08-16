//! D9 — serve the Postgres wire subset on a TCP port.
//!
//! `cargo run --release --example pgserver -- <db-path> <addr>`
//!
//! Prints the bound address on stdout before accepting, so a test can wait for readiness instead
//! of sleeping and hoping.

use std::path::Path;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::{serve, ServerContext};
use ferrodb::storage::db_lock::DbLock;
use ferrodb::tel::MemEffectLog;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::recover;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "ferro.db".into());
    let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:0".into());

    // Before any file is opened: a second writer on one database aliases arena pages, and every
    // aliased page still checksums correctly, so refusing here is the only detection point.
    let _lock = match DbLock::acquire(Path::new(&db)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("pgserver: {e}");
            std::process::exit(1);
        }
    };
    let existed = Path::new(&db).exists();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&db)
        .expect("open db");
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let wal = Arc::new(WalManager::new(format!("{db}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    recover(&txn).unwrap();
    let catalog = if existed {
        Catalog::open(bp.clone(), FIRST_CATALOG_PAGE_ID).unwrap()
    } else {
        Catalog::create(bp.clone()).unwrap()
    };

    let listener = std::net::TcpListener::bind(&addr).expect("bind");
    // Readiness, not a guess: the test reads this line rather than sleeping.
    println!("LISTENING {}", listener.local_addr().unwrap());
    use std::io::Write;
    std::io::stdout().flush().unwrap();

    // Built AFTER the catalog, for the reason `src/cli/cli.rs` spells out: the arena floor has to
    // sit above what the catalog has already allocated, or the ordinary allocator and the arena hand
    // out the same page. The floor is persisted in the checkpoint, so a reopen reattaches to the
    // region it left rather than inventing a new one on top of live pages.
    let branches_path = format!("{db}.branches");
    let arena_path = format!("{db}.arena");
    let branches = Arc::new(
        LogBranchCatalog::open(Path::new(&branches_path), 1).expect("branch catalog"),
    );
    let arena_exists = Path::new(&arena_path).exists();
    let store: Arc<ArenaPageStore> = Arc::new(if arena_exists {
        ArenaPageStore::reopen_from_checkpoint(bp.clone(), branches.clone(), Path::new(&arena_path))
            .expect("reattach to the arena")
    } else {
        let base = bp.disk_manager.high_water().expect("high water") + 32_736;
        ArenaPageStore::new(bp.clone(), branches.clone(), base).expect("arena")
    });
    store.checkpoint_to(std::path::PathBuf::from(&arena_path));

    let runtime = Arc::new(if arena_exists {
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
    });

    let mut ctx = ServerContext { catalog, bp, txn, runtime };
    serve(listener, &mut ctx).unwrap();
    store.checkpoint(Path::new(&arena_path)).expect("checkpoint the arena");
}
