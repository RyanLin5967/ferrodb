//! D101, the spurious-fire half: a process that designates nothing must not be refused anything.
//!
//! This is the arm that decides whether the guard in `agent_sql::designated` is safe to keep. A
//! refusal that also fires on correct code gets switched off within a week, and then it guards
//! nothing — so the permitting direction is tested as deliberately as the refusing one.
//!
//! The population it stands for is large: `Session::new()` has ~110 call sites across `src/`,
//! `tests/` and `examples/`, and the overwhelming majority are processes that never build a
//! `ServerContext` at all — unit tests, the CLI's own fixtures, single-purpose harnesses. Every
//! one of them must keep working, and none of them is doing anything wrong by holding a private
//! in-memory runtime: that is the correct configuration when it is the ONLY runtime in the process.
//!
//! ⚠ Its own binary, and it must stay that way. The registry is process-wide, so a sibling test
//! that built a `ServerContext` would designate a runtime and turn this file's premise false — it
//! would then fail loudly rather than silently passing, but the failure would be the harness's,
//! not the guard's. Keeping the binary empty of `ServerContext` keeps the premise readable.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// A whole agent workflow on a plain `Session::new()`, with no `ServerContext` anywhere in the
/// process: fork, staged write, diff, merge. Not one statement may be refused.
#[test]
fn nothing_is_designated_so_nothing_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("p.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let mut catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);

    let mut sess = Session::new();
    let mut exec = |sql: &str, sess: &mut Session| -> Result<(), String> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "{sql}: {:?}", p.errors);
        run(stmts.remove(0), &mut catalog, bp.clone(), txn.clone(), sess)
            .map(|_| ())
            .map_err(|e| e.to_string())
    };

    exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    for i in 1..=20 {
        exec(&format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess).unwrap();
    }
    for sql in [
        "BEGIN AGENT SESSION AS 'd101' RUN 'r0';",
        "UPDATE t SET v = 99 WHERE id = 3;",
        "DIFF;",
        "MERGE;",
    ] {
        if let Err(e) = exec(sql, &mut sess) {
            panic!(
                "the guard fired in a process that designated nothing — this is the \
                 spurious-fire case, and it would break ~110 correct callers. {sql}: {e}"
            );
        }
    }
}
