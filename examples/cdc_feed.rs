//! E10 — a runnable CDC source. `cdc_feed [db-path]`
//!
//! Runs a short workload of real SQL, then prints the change feed as JSON Lines on stdout. Pipe it
//! anywhere a JSONL consumer lives:
//!
//! ```text
//! cargo run --example cdc_feed | jq -c '{op, table, after}'
//! ```
//!
//! Everything on stdout is the feed and nothing else, so it composes. Counts and anything that was
//! NOT emitted go to stderr, because a feed with a summary line in the middle of it is not a feed.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::write_feed;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "cdc_demo.db".into());

    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&db).expect("open db");
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let mut catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(format!("{db}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());

    let mut session = Session::new();
    let mut exec = |sql: &str, cat: &mut Catalog, s: &mut Session| {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in: {sql}");
        run(stmts.remove(0), cat, bp.clone(), txn.clone(), s).unwrap_or_else(|e| {
            eprintln!("{sql} failed: {e}");
            std::process::exit(3);
        })
    };

    exec("CREATE TABLE inventory (id INTEGER NOT NULL, item VARCHAR(32), qty INTEGER);",
         &mut catalog, &mut session);
    exec("INSERT INTO inventory VALUES (1, 'widget', 10);", &mut catalog, &mut session);
    exec("INSERT INTO inventory VALUES (2, 'gadget', 20);", &mut catalog, &mut session);
    exec("INSERT INTO inventory VALUES (3, 'doohickey', 30);", &mut catalog, &mut session);
    exec("UPDATE inventory SET qty = 999 WHERE id = 1;", &mut catalog, &mut session);
    exec("DELETE FROM inventory WHERE id = 2;", &mut catalog, &mut session);
    wal.flush().expect("flush wal");

    use std::sync::atomic::Ordering;
    let decoder = LogicalDecoder::new(&catalog);
    let out = decoder
        .decode(&wal, wal.base_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst))
        .expect("decode");

    let mut stdout = std::io::stdout().lock();
    let n = write_feed(&out.events, &mut stdout).expect("write feed");

    // Everything that did NOT become an event, on stderr. A consumer that only reads stdout gets a
    // clean feed; an operator watching the terminal still learns what was skipped and why.
    eprintln!("--- {n} change event(s) emitted");
    eprintln!("--- {} internal MVCC record(s) skipped (time-travel archive)", out.internal);
    if !out.aborted.is_empty() {
        eprintln!("--- {} aborted transaction(s) withheld: {:?}", out.aborted.len(), out.aborted);
    }
    if !out.open.is_empty() {
        eprintln!("--- {} transaction(s) still open, withheld: {:?}", out.open.len(), out.open);
    }
    if !out.unresolved.is_empty() {
        eprintln!("--- WARNING unresolved dir_roots (no such table): {:?}", out.unresolved);
    }
    if !out.undecodable.is_empty() {
        eprintln!("--- WARNING undecodable tuples: {:?}", out.undecodable);
    }
    if !out.is_complete() {
        eprintln!("--- this feed is INCOMPLETE; see the warnings above");
    }
}
