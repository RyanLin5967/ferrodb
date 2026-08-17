//! `table_dump <db-path> <table>` — the source side of a CDC diff.
//!
//! Prints one table's **live rows** as a JSON array of objects on stdout, and nothing else, so it
//! composes: `cdc-consumer diff <feed.jsonl> <source.json>`.
//!
//! # Why this exists
//!
//! The Go consumer re-materializes a table from the change events and can dump what it built. Nothing
//! produced the other half, so the only way to check the two agreed was a test harness holding a
//! hardcoded expectation — which verifies the pipeline against what somebody typed, not against the
//! database. This prints what the database actually holds.
//!
//! # It asks the query path, not the heap
//!
//! The rows come from running `SELECT * FROM <table>;` through the real executor, rather than scanning
//! the heap and filtering by hand. That is deliberate: "the source" means what the source database
//! answers, MVCC visibility and all. A dump that walked the heap itself could disagree with every
//! query the database would answer and still call itself correct — and it would have to reimplement
//! version resolution to do it, which is the part most likely to be wrong.
//!
//! Values are rendered by `replication::jsonl::write_table_json`, which shares [`value_into`] with the
//! feed writer, so a `BigInt` past 2^53, a `DECIMAL` and a `TIMESTAMP` are strings on both sides
//! without this file knowing that.

use std::path::Path;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::write_table_json;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::recover;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (db, table) = match (args.get(1), args.get(2)) {
        (Some(d), Some(t)) => (d.clone(), t.clone()),
        _ => {
            eprintln!("usage: table_dump <db-path> <table>");
            std::process::exit(2);
        }
    };

    // Single-writer lock, as every binary that opens a user-named database takes (E38/E45). Held for
    // the whole run; released on the way out, including on an early return.
    let _lock = DbLock::acquire(Path::new(&db)).unwrap_or_else(|e| {
        eprintln!("table_dump: {e}");
        std::process::exit(1);
    });

    // **`create(false)`**: this reads an existing database. Creating one on a typo would print an
    // empty array and exit zero, which reads exactly like "the source table is empty" - the one
    // answer a diff must never be handed by mistake.
    if !Path::new(&db).exists() {
        eprintln!("table_dump: {db} does not exist");
        std::process::exit(1);
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&db)
        .unwrap_or_else(|e| {
            eprintln!("table_dump: open {db}: {e}");
            std::process::exit(1);
        });
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(format!("{db}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    // Recovery before reading: an unclean shutdown leaves committed work in the log and not yet in the
    // pages, and dumping without replaying it would report rows the source considers written as
    // missing.
    recover(&txn).unwrap_or_else(|e| {
        eprintln!("table_dump: recovery failed: {e}");
        std::process::exit(1);
    });
    let mut catalog = Catalog::open(bp.clone(), FIRST_CATALOG_PAGE_ID).unwrap_or_else(|e| {
        eprintln!("table_dump: open catalog: {e}");
        std::process::exit(1);
    });

    let columns: Vec<String> = match catalog.get_table(&table) {
        Some(entry) => entry.schema.columns.iter().map(|c| c.name.clone()).collect(),
        None => {
            // Refuse rather than print `[]`. An unknown table and an empty table are different facts
            // and a diff that cannot tell them apart is worse than no diff.
            eprintln!("table_dump: {}", catalog.unknown_table(&table));
            std::process::exit(1);
        }
    };

    let sql = format!("SELECT * FROM {table};");
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().expect("scan");
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    if !parser.errors.is_empty() {
        eprintln!("table_dump: {:?}", parser.errors);
        std::process::exit(1);
    }
    let mut session = Session::new();
    let rows = match run(stmts.remove(0), &mut catalog, bp.clone(), txn.clone(), &mut session) {
        Ok(Outcome::Rows(r)) => r,
        Ok(_) => {
            eprintln!("table_dump: `{sql}` did not return rows");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("table_dump: {e}");
            std::process::exit(1);
        }
    };

    let mut stdout = std::io::stdout().lock();
    let n = write_table_json(&columns, &rows, &mut stdout).expect("write dump");
    // Counts on stderr, so stdout stays a single JSON document a consumer can read whole.
    eprintln!("--- {n} live row(s) in '{table}'");
}
