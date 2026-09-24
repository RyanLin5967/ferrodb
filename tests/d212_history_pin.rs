//! **D212 (a') falsifier (4), as restated by "AMENDED 2" F10: under a held WAL pin, the checkpoint
//! hook reads no log, and writes the store at most once per checkpoint — ≤ 1 fsync when it appends,
//! ≤ 2 when it prunes — flat in M.**
//!
//! In its own test binary, with one test in it, because `wal_read_counters` is process-wide and any
//! other test's recovery would move it.
//!
//! A pin (a lagging subscriber, D252) makes `truncate` keep the whole log, so a hook that caught up
//! by re-reading the retained log would read O(pin lag) bytes per checkpoint — the new term on the
//! programme's axis "AMENDED" §6 names. The queue design reads none.
//!
//! Pre-registered mutants (in `src/wal/history.rs` / `src/wal/txn.rs`), each RED here:
//! - the hook re-reads the retained log for committed history (the report's design) — WAL bytes
//!   read per checkpoint are non-zero and grow with M;
//! - `drain` writes one record per call instead of the queue at once — two merges precede each
//!   checkpoint, so that is two store writes in one checkpoint.

use std::fs::OpenOptions;
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
use ferrodb::wal::history::HistoryStore;
use ferrodb::wal::log::{wal_read_counters, WalManager};
use ferrodb::wal::txn::TxnManager;

#[test]
fn falsifier_4_under_a_pin_the_hook_reads_no_log_and_writes_once_per_checkpoint() {
    const W: u64 = 8;
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("d212p.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let mut catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("d212p.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let store = HistoryStore::open(dir.path().join("d212p.db.history"), W).unwrap();
    txn.attach_history_store(Arc::clone(&store)).unwrap();
    let runtime = Arc::new(AgentRuntime::new());

    let mut exec = |sql: &str, s: &mut Session| -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert!(parser.errors.is_empty(), "parse errors in {sql}");
        run(stmts.remove(0), &mut catalog, bp.clone(), txn.clone(), s)
            .unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    };
    let mut plain = Session::with_runtime(runtime.clone());
    exec("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut plain);
    for id in 1..=4 {
        exec(&format!("INSERT INTO inventory VALUES ({id}, 10);"), &mut plain);
    }

    // The pin: every checkpoint from here keeps the whole log.
    let _pin = wal.pin_durable();

    // Per checkpoint: (WAL bytes read, store appends, store rewrites), for M = W and M = 4W.
    let mut worst: Vec<(u64, u64, u64)> = Vec::new();
    let mut made = 0u64;
    for m in [W, 4 * W] {
        let mut phase = (0u64, 0u64, 0u64);
        while made < m {
            // TWO merges per checkpoint, so a drain that wrote one record per call would show as
            // two store writes in one checkpoint.
            for _ in 0..2 {
                made += 1;
                let mut a = Session::with_runtime(runtime.clone());
                exec(&format!("BEGIN AGENT SESSION AS 't{made}' RUN 'r{made}';"), &mut a);
                exec(&format!("UPDATE inventory SET qty = qty + 1 WHERE id = {};", made % 4 + 1), &mut a);
                match exec("MERGE;", &mut a) {
                    Outcome::Agent(AgentOutput::Merge(r)) => assert!(r.applied_to_target, "{r}"),
                    _ => panic!("MERGE did not return a report"),
                }
            }
            let (read0, c0) = (wal_read_counters().1, store.counters());
            txn.checkpoint().unwrap();
            let (read1, c1) = (wal_read_counters().1, store.counters());
            phase.0 = phase.0.max(read1 - read0);
            phase.1 = phase.1.max(c1.appends - c0.appends);
            phase.2 = phase.2.max(c1.rewrites - c0.rewrites);
            assert!(
                (c1.appends - c0.appends) + (c1.rewrites - c0.rewrites) <= 1,
                "checkpoint {made} wrote the store more than once"
            );
        }
        worst.push(phase);
    }
    assert!(store.counters().prunes > 0, "the fixture never pruned, so the ≤ 2 arm tests nothing");
    // The pin did keep the log (read only now, after every measured checkpoint).
    let kept = {
        use std::sync::atomic::Ordering;
        let (mut lsn, end) = (wal.base_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst));
        let mut n = 0;
        while lsn < end {
            let (r, next) = wal.read_record(lsn).unwrap();
            n += usize::from(matches!(r.kind, ferrodb::wal::log::RecKind::RevertHistory { .. }));
            lsn = next;
        }
        n
    };
    assert!(kept as u64 >= 4 * W, "the pin did not keep the log, so the hook had nothing to re-read");
    for (m, (read, appends, rewrites)) in [W, 4 * W].iter().zip(&worst) {
        assert_eq!(*read, 0, "at M = {m} a checkpoint read {read} bytes of the pinned log");
        // ≤ 1 fsync for an append, ≤ 2 (the temporary, then the directory) for a prune's rewrite.
        assert!(*appends <= 1 && *rewrites <= 1, "at M = {m}: {appends} appends, {rewrites} rewrites");
    }
}
