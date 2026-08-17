//! D8 harness — a process that merges, and can be killed part-way through publishing.
//!
//! Run by `tests/integration_crash_safety.rs`, which spawns it with `FERRODB_CRASH_AFTER_ROWS`
//! set to a row index. The merge publishes every row in one transaction; the crash point sits
//! inside that loop, so the process dies with the transaction open and some rows written.
//!
//! Usage: `crash_mid_merge <db-path> <phase>` where phase is `seed` or `merge`.

use std::path::Path;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::recover;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;

/// The three rows a merge will publish. Row 1 is the sentinel the test reads back.
const ROWS: [(i32, i32); 3] = [(1, 100), (2, 200), (3, 300)];
/// What the agent sets each row to. Distinct from the seed so a partial apply is visible.
const MERGED: i32 = 999;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: crash_mid_merge <db-path> <seed|merge>");
        std::process::exit(2);
    }
    let path = args[1].clone();
    let phase = args[2].clone();

    let existed = Path::new(&path).exists();
    // Single-writer lock, taken before the file is opened. Two processes on one database both build
    // an ArenaPageStore from the same checkpoint and hand the same pages to different branches, and
    // every such page still passes its checksum - so refusing here is the only detection point.
    //
    // Held for the whole run: `_db_lock` releases on the way out, including on an early return.
    let _db_lock = ferrodb::storage::db_lock::DbLock::acquire(std::path::Path::new(&path))
        .unwrap_or_else(|e| { eprintln!("crash_mid_merge: {e}"); std::process::exit(1); });

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let wal = Arc::new(WalManager::new(format!("{path}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());

    // Same opening sequence the CLI uses, so recovery is the real one.
    let recovered = recover(&txn).unwrap();
    let mut catalog = if existed {
        Catalog::open(bp.clone(), FIRST_CATALOG_PAGE_ID).unwrap()
    } else {
        Catalog::create(bp.clone()).unwrap()
    };
    let _ = recovered;

    let runtime = Arc::new(AgentRuntime::new());
    let mut session = Session::with_runtime(runtime);

    let exec = |sql: &str, s: &mut Session, cat: &mut Catalog| -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert!(parser.errors.is_empty(), "parse errors in: {sql}");
        run(stmts.remove(0), cat, bp.clone(), txn.clone(), s).unwrap_or_else(|e| {
            eprintln!("{sql} failed: {e}");
            std::process::exit(3);
        })
    };

    if phase == "seed" {
        exec("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut session, &mut catalog);
        for (id, qty) in ROWS {
            exec(
                &format!("INSERT INTO inventory VALUES ({id}, {qty});"),
                &mut session,
                &mut catalog,
            );
        }
        bp.flush_all().unwrap();
        println!("seeded");
        return;
    }

    if phase == "read" {
        // Read every row back after recovery and print them for the test to parse.
        let out = exec("SELECT id, qty FROM inventory;", &mut session, &mut catalog);
        let mut got: Vec<(i32, i32)> = match out {
            Outcome::Rows(rows) => rows
                .iter()
                .filter_map(|r| match (r.first(), r.get(1)) {
                    (
                        Some(ferrodb::catalog::column::Value::Integer(a)),
                        Some(ferrodb::catalog::column::Value::Integer(b)),
                    ) => Some((*a, *b)),
                    _ => None,
                })
                .collect(),
            _ => {
                eprintln!("expected rows");
                std::process::exit(4);
            }
        };
        got.sort_unstable();
        let rendered: Vec<String> = got.iter().map(|(a, b)| format!("{a}:{b}")).collect();
        println!("STATE {}", rendered.join(","));
        return;
    }

    // phase == "merge": one agent changes every row, then merges. The crash point is inside the
    // publish loop, so the process can die with some rows applied and the transaction open.
    exec("BEGIN AGENT SESSION AS 'crash-agent' RUN 'r_c';", &mut session, &mut catalog);
    for (id, _) in ROWS {
        exec(
            &format!("UPDATE inventory SET qty = {MERGED} WHERE id = {id};"),
            &mut session,
            &mut catalog,
        );
    }
    exec("MERGE;", &mut session, &mut catalog);
    bp.flush_all().unwrap();
    println!("merged");
}
