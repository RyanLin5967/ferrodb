//! **D212 — what REVERT's durable history costs per MERGE, in WAL bytes and fsyncs.**
//!
//! The lead's decision says the per-merge overhead "must be measured, not assumed". This measures it
//! with two integer instruments that do not move when the machine is loaded:
//!
//! * **WAL bytes per `MERGE`** — `WalManager::next_lsn` before and after the statement. An LSN is a
//!   byte offset into the log, so the difference is exactly what the statement appended. The
//!   automatic checkpoint is pushed out of the window (`FERRODB_CHECKPOINT_INTERVAL`), because a
//!   truncation re-appends the retained schema, which is not the merge's write.
//! * **fsyncs per `MERGE`** — `wal::log::fsync_counters()`.
//!
//! It uses only API that exists both before and after D212 option (a), so the SAME program runs at
//! `b2269c9` (Step 0: no history written) and at the option (a) commit; the overhead is the
//! difference. Heap page writes are not counted: they reach disk at flush and checkpoint, and the
//! WAL is what a commit waits on.
//!
//! The workload is `d212_design.md` §1's worked example: one agent task per merge, on a table of four
//! INTEGER columns, runs three range scans with WHERE clauses of ~30 characters and one point lookup,
//! then updates ONE row by key (δ = ρ = 1). The first `ROWS` merges each touch a row no merge has
//! touched (a `versions` insert under (a)); the rest re-touch rows (a `versions` update).
//!
//! Usage: `cargo run --release --example d212_merge_overhead -- [merges]` (default 200).

use std::fs::OpenOptions;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::{fsync_counters, WalManager};
use ferrodb::wal::txn::TxnManager;

/// Distinct rows the merges cycle through.
const ROWS: usize = 50;

fn main() {
    // SAFETY: set before anything reads it, on the only thread there is.
    unsafe { std::env::set_var("FERRODB_CHECKPOINT_INTERVAL", "1000000000") };
    let merges: usize = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(200);
    assert!(merges > ROWS, "run more than {ROWS} merges so both the insert and update paths are seen");

    let dir = tempfile::tempdir().expect("tempdir");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("d212o.db"))
        .expect("db file");
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).expect("disk"))));
    let mut catalog = Catalog::create(bp.clone()).expect("catalog");
    let wal = Arc::new(WalManager::new(dir.path().join("d212o.wal")).expect("wal"));
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let runtime = Arc::new(AgentRuntime::new());

    let mut exec = |sql: &str, s: &mut Session| -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().expect("scan");
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert!(parser.errors.is_empty(), "parse errors in {sql}");
        run(stmts.remove(0), &mut catalog, bp.clone(), txn.clone(), s)
            .unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    };

    let mut plain = Session::with_runtime(runtime.clone());
    exec("CREATE TABLE inv (id INTEGER NOT NULL, a INTEGER, b INTEGER, c INTEGER);", &mut plain);
    for id in 1..=ROWS {
        exec(&format!("INSERT INTO inv VALUES ({id}, 100, 200, 300);"), &mut plain);
    }

    let mut bytes: Vec<u64> = Vec::with_capacity(merges);
    let mut syncs: Vec<u64> = Vec::with_capacity(merges);
    for i in 0..merges {
        let row = i % ROWS + 1;
        let mut s = Session::with_runtime(runtime.clone());
        exec(&format!("BEGIN AGENT SESSION AS 'overhead' RUN 'r{i}';"), &mut s);
        exec("SELECT id, a FROM inv WHERE a >= 0 AND a < 1000000;", &mut s);
        exec("SELECT id, b FROM inv WHERE b >= 0 AND b < 1000000;", &mut s);
        exec("SELECT id, c FROM inv WHERE c >= 0 AND c < 1000000;", &mut s);
        exec(&format!("SELECT a FROM inv WHERE id = {row};"), &mut s);
        exec(&format!("UPDATE inv SET a = a + 1 WHERE id = {row};"), &mut s);
        let (lsn0, (f0, _)) = (wal.next_lsn.load(Ordering::SeqCst), fsync_counters());
        match exec("MERGE;", &mut s) {
            Outcome::Agent(AgentOutput::Merge(m)) => {
                assert!(m.applied_to_target, "merge {i} did not land: {m}")
            }
            _ => panic!("MERGE did not return a report"),
        }
        let (lsn1, (f1, _)) = (wal.next_lsn.load(Ordering::SeqCst), fsync_counters());
        bytes.push(lsn1 - lsn0);
        syncs.push(f1 - f0);
    }

    let median = |v: &[u64]| {
        let mut v = v.to_vec();
        v.sort_unstable();
        v[v.len() / 2]
    };
    println!("d212_merge_overhead: {merges} merges, one row each, {ROWS} distinct rows");
    println!("  merge 0 (creates the history tables under option (a)): {} WAL bytes, {} fsyncs", bytes[0], syncs[0]);
    println!(
        "  merges 1..{ROWS} (first touch of their row): median {} WAL bytes, min {}, max {}",
        median(&bytes[1..ROWS]),
        bytes[1..ROWS].iter().min().unwrap(),
        bytes[1..ROWS].iter().max().unwrap()
    );
    println!(
        "  merges {ROWS}..{merges} (row touched before): median {} WAL bytes, min {}, max {}",
        median(&bytes[ROWS..]),
        bytes[ROWS..].iter().min().unwrap(),
        bytes[ROWS..].iter().max().unwrap()
    );
    println!(
        "  fsyncs: {} over merges 1..{merges} ({:.3} per merge)",
        syncs[1..].iter().sum::<u64>(),
        syncs[1..].iter().sum::<u64>() as f64 / (merges - 1) as f64
    );
    println!("  raw WAL bytes per merge: {bytes:?}");
    println!("  raw fsyncs per merge: {syncs:?}");
}
