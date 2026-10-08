//! Was the two-page `ADD COLUMN` hang introduced by 58804c8, or was it always there?
//! Self-contained so it compiles against the parent commit's `src/`, which has no
//! `find_or_make_page` and no `reserve_free_space`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn go(dir: &Path) -> (String, String) {
    let path = dir.join("alter.db");
    let file = std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let mut catalog = Catalog::create(bp.clone()).unwrap();
    let runtime = Arc::new(AgentRuntime::new());
    let mut session = Session::with_runtime(runtime.clone());
    let mut go1 = |catalog: &mut Catalog, sql: &str| -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "`{sql}` did not parse");
        run(stmts.remove(0), catalog, bp.clone(), txn.clone(), &mut session)
    };
    go1(&mut catalog, "CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));").unwrap();
    let pad = "y".repeat(60);
    for i in 1..=82 {
        go1(&mut catalog, &format!("INSERT INTO t VALUES ({i}, '{pad}');")).unwrap();
    }
    let root = catalog.get_table("t").unwrap().first_directory_page_id;
    let heap = HeapFileManager::open(root, bp.clone());
    let rows: Vec<_> = heap.scan().map(|r| r.unwrap()).collect();
    let pages: std::collections::BTreeSet<u32> = rows.iter().map(|(rid, _)| rid.page_id).collect();
    let filled = format!("{} rows over {} pages", rows.len(), pages.len());
    let out = match go1(&mut catalog, "ALTER TABLE t ADD COLUMN w VARCHAR(10);") {
        Ok(_) => {
            let root = catalog.get_table("t").unwrap().first_directory_page_id;
            let n = HeapFileManager::open(root, bp.clone()).scan().count();
            format!("Ok, {n} rows still in the heap, {} columns", catalog.get_table("t").unwrap().schema.columns.len())
        }
        Err(e) => format!("refused: {e}"),
    };
    (filled, out)
}

#[test]
fn add_column_on_a_two_page_table_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().to_path_buf();
    let (tx, rx) = std::sync::mpsc::channel::<(String, String)>();
    std::thread::spawn(move || {
        let r = go(&path);
        let _ = tx.send(r);
    });
    match rx.recv_timeout(Duration::from_secs(90)) {
        Ok((filled, out)) => println!("filled: {filled}\nALTER: {out}"),
        Err(_) => panic!("ALTER TABLE t ADD COLUMN on a two-page table never returned (90s)"),
    }
}
