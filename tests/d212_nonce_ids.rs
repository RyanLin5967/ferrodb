//! **D212 addendum (07:41Z): a merge id carries a per-open random nonce, `m_<nonce>_<n>`.**
//!
//! The three falsifier tests SCALE-DESIGN's addendum names, plus the format refusal it decides:
//! - a fresh database's first merge is not `m_1` — a database written before D217 has no ceiling,
//!   so under the ceiling its first merge after the upgrade is `m_1` again;
//! - a restored copy of an earlier database file mints ids that do not collide with ids the original
//!   issued after the copy was taken — a copy carries the ceiling of the moment it was taken;
//! - an old-format id is refused by its FORMAT, naming the upgrade.
//!
//! Merge, reopen, merge (ids differ) is `d212_step0_revert_identity.rs`'s first test.
//!
//! All three compile against `b2269c9` (Step 0 with the durable ceiling) and name no new API. Each
//! is RED there, at the assertion its doc names.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::{rebuild_indexes, recover};
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    dir: PathBuf,
}

impl Db {
    fn open(dir: &Path) -> Db {
        let path = dir.join("d212n.db");
        let existed = path.exists();
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.join("d212n.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let recovered = recover(&txn).unwrap();
        let mut catalog = if existed {
            Catalog::open(bp.clone(), 1).unwrap()
        } else {
            Catalog::create(bp.clone()).unwrap()
        };
        if recovered {
            rebuild_indexes(&mut catalog, &bp).unwrap();
            txn.checkpoint().unwrap();
        }
        let branches = LogBranchCatalog::open(&dir.join("d212n.branches"), 1).unwrap();
        let runtime =
            Arc::new(AgentRuntime::with_catalog(Arc::new(branches) as Arc<dyn BranchCatalog>));
        Db { catalog, bp, txn, runtime, dir: dir.to_path_buf() }
    }

    fn restart(self) -> Db {
        self.txn.checkpoint().unwrap();
        let dir = self.dir.clone();
        drop(self);
        Db::open(&dir)
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert!(parser.errors.is_empty(), "parse errors in {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn seed(&mut self) {
        let mut s = Session::with_runtime(self.runtime.clone());
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        for id in 1..=3 {
            self.ok(&format!("INSERT INTO inventory VALUES ({id}, 10);"), &mut s);
        }
    }

    /// One agent task that sets row `id`'s qty, then its merge id.
    fn merge_one(&mut self, name: &str, id: i32, qty: i32) -> String {
        let mut a = Session::with_runtime(self.runtime.clone());
        self.ok(&format!("BEGIN AGENT SESSION AS '{name}' RUN 'r_{name}';"), &mut a);
        self.ok(&format!("UPDATE inventory SET qty = {qty} WHERE id = {id};"), &mut a);
        match self.ok("MERGE;", &mut a) {
            Outcome::Agent(AgentOutput::Merge(m)) => {
                assert!(m.applied_to_target, "{name}'s merge did not land: {m}");
                m.merge_id
            }
            _ => panic!("MERGE did not return a report"),
        }
    }
}

/// `m_<16 lowercase hex digits>_<n>`, the format the addendum decides.
fn nonce_format(id: &str) -> Option<(String, u64)> {
    let (nonce, n) = id.strip_prefix("m_")?.split_once('_')?;
    let hex = nonce.len() == 16
        && nonce.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if !hex {
        return None;
    }
    Some((nonce.to_string(), n.parse().ok()?))
}

/// A database written before D217 has no ceiling, so under the ceiling its first merge after the
/// upgrade is `m_1` — the very id that database's earlier runs issued. A fresh database stands in
/// for it here: both have no ceiling, and the upgrade case differs only in what `m_1` used to mean.
///
/// RED at `b2269c9`: the first id is `m_1`.
#[test]
fn a_first_merge_is_named_by_this_runs_nonce_not_by_a_counter_from_one() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed();
    let first = db.merge_one("a", 1, 11);
    assert_ne!(first, "m_1", "a first merge reused the id every earlier ceiling-less run issued");
    let (_, n) = nonce_format(&first)
        .unwrap_or_else(|| panic!("{first} is not m_<16 hex digits>_<n>"));
    assert_eq!(n, 1, "the counter within a run starts at 1");
}

/// A restored copy carries the counters of the moment it was taken, so ids the original issued
/// after that moment are issued again by the copy — unless the id names the run that minted it.
///
/// RED at `b2269c9`: after the restart the original's next merge is `m_65`, and so is the copy's.
#[test]
fn a_restored_copy_never_reissues_an_id_the_original_issued_after_the_copy() {
    let original_dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(original_dir.path());
    db.seed();
    db.merge_one("a", 1, 11);
    // A consistent image on disk, then the copy — the backup.
    db.txn.checkpoint().unwrap();
    let copy_dir = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(original_dir.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), copy_dir.path().join(entry.file_name())).unwrap();
        }
    }

    // The original goes on: a restart, and a merge.
    let mut db = db.restart();
    let after_copy = db.merge_one("b", 2, 22);

    // The copy is restored, and merges.
    let mut restored = Db::open(copy_dir.path());
    let from_copy = restored.merge_one("c", 2, 33);

    assert_ne!(
        from_copy, after_copy,
        "the restored copy issued {from_copy}, an id the original had already issued after the copy"
    );
}

/// An id without a nonce was minted by a build before the addendum, and nothing can say which merge
/// it named now. It is refused by its format, and the refusal names the upgrade.
///
/// RED at `b2269c9`: the refusal says the id was published by nothing, not that its format is old.
#[test]
fn an_old_format_merge_id_is_refused_by_its_format_naming_the_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed();
    let mut s = Session::with_runtime(db.runtime.clone());
    let msg = match db.exec("REVERT MERGE m_1;", &mut s) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("REVERT MERGE m_1 returned a plan on a database with no merges"),
    };
    assert!(msg.contains("old-format"), "not refused for its format: {msg}");
    assert!(msg.contains("m_<nonce>_<n>"), "the refusal does not name the new format: {msg}");
}
