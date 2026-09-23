use std::fs::OpenOptions;
use std::sync::Arc;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db { catalog: Catalog, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>, _d: tempfile::TempDir }
impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.path().join("p.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _d: dir }
    }
    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let t = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(t); let mut st = p.parse();
        if !p.errors.is_empty() { return Err(FerroError::SqlParseError(format!("{:?}", p.errors))); }
        run(st.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }
    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome { self.exec(sql, s).unwrap_or_else(|e| panic!("{sql}: {e}")) }
}

#[test]
fn scope_probe() {
    for n in [60i64, 100, 200, 400, 800] {
        for analyze in [false, true] {
            let mut db = Db::new();
            let mut s = Session::new();
            db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, label VARCHAR(16));", &mut s);
            for i in 0..n { db.ok(&format!("INSERT INTO t VALUES ({i}, {}, 'row');", i*10), &mut s); }
            db.ok("CREATE INDEX ix ON t (v);", &mut s);
            if analyze { db.ok("ANALYZE t;", &mut s); }
            let cutoff = (n-2)*10;
            let r = db.exec(&format!("SELECT id FROM t WHERE v > {cutoff};"), &mut s);
            let verdict = match r { Ok(Outcome::Rows(x)) => format!("OK {} rows", x.len()), Ok(_) => "OK other".into(), Err(e) => format!("ERROR: {e}") };
            println!("n={n:<5} analyze={analyze:<5} v>{cutoff:<6} -> {verdict}");
        }
    }
}
