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
use ferrodb::pgwire::{serve, ServerContext};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::recover;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "ferro.db".into());
    let addr = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:0".into());

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

    let mut ctx = ServerContext { catalog, bp, txn };
    serve(listener, &mut ctx).unwrap();
}
