//! D55 — what plan does `WHERE id = k` actually get, on a table with and without ANALYZE?
//!
//! The D55 profile put ~80% of an agent read under `SeqScan::next`, after the predicate had been
//! pushed into the planner. So either the plain path seq-scans too (and the "33x" gap was mostly
//! the 200-vs-5000 row difference between the D54 and D55 tables), or something about how the
//! agent path calls the planner disables index selection. EXPLAIN settles it in a minute.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn main() {
    let dir = std::env::temp_dir().join(format!("ferrodb-d55x-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.join("x.db")).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let mut catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.join("x.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let mut s = Session::new();
    let mut exec = |sql: &str, catalog: &mut Catalog| -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut st = p.parse();
        assert!(p.errors.is_empty(), "{sql}: {:?}", p.errors);
        run(st.remove(0), catalog, bp.clone(), txn.clone(), &mut s).unwrap_or_else(|e| panic!("{sql}: {e}"))
    };
    exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog);
    for i in 1..=5000 {
        exec(&format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut catalog);
    }
    let show = |label: &str, o: Outcome| {
        if let Outcome::Explain(text) = o { println!("--- {label} ---\n{text}"); } else { println!("--- {label}: not an EXPLAIN outcome"); }
    };
    show("BEFORE ANALYZE: WHERE id = 7", exec("EXPLAIN SELECT v FROM t WHERE id = 7;", &mut catalog));
    exec("ANALYZE t;", &mut catalog);
    show("AFTER ANALYZE:  WHERE id = 7", exec("EXPLAIN SELECT v FROM t WHERE id = 7;", &mut catalog));
    let _ = std::fs::remove_dir_all(&dir);
}
