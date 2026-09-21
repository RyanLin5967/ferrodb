//! D103 — CHERRY-PICK reaches the production op log.
//!
//! `branch::cherry` shipped as an engine with no caller, and said so in its own header: "Nothing
//! wires this to `runtime.rs` ... none of it is evidence about the runtime until that impl exists
//! and is tested, and `commit_all`'s all-or-nothing contract in particular is the part the runtime
//! will owe." This is that impl and these are those tests.
//!
//! # The operation, and why `MERGE` is not it
//!
//! With one branch per agent the natural request is *"agent A found the right fix — put just that
//! change onto agent B's branch, without agent A's other edits."* A merge's unit is a whole
//! branch: every effect the source recorded, or none. Merging and then reverting the unwanted
//! edits is a different operation — it publishes the unwanted work to every reader in the window,
//! and `REVERT` refuses any op whose before-image was not recorded.
//!
//! # What is asserted
//!
//! * `one_op_of_several_lands_and_the_others_do_not` — the capability. It is the test that fails
//!   if the wiring is removed, and it checks the negative half (the unpicked cells did NOT move)
//!   as well as the positive one, because a pick that applied everything would satisfy the
//!   positive half alone.
//! * `a_pick_whose_cell_the_target_already_moved_is_refused_and_writes_nothing` — the divergence
//!   read, through D86's by-cell index. The forced-fire case for the refusal.
//! * `a_pick_spanning_two_tables_is_refused_rather_than_half_applied` — the atomicity boundary,
//!   made a refusal rather than a caveat.
//! * `an_empty_selection_is_refused` and `a_selector_naming_no_op_is_refused` — the two malformed
//!   requests, which must come back as answers rather than as silent no-ops.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::{row_id_of, AgentRuntime, ExecCtx};
use ferrodb::tel::ids::ColId;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::cherry::CherryConflictKind;
use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const ARENA_BASE: u32 = 1024;

fn rid(id: i32) -> u64 {
    row_id_of(&[Value::Integer(id)]).0
}

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("pages.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("pages.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);

        let branches = Arc::new(LogBranchCatalog::in_memory(1));
        let store = Arc::new(
            ArenaPageStore::new(
                bp.clone(),
                Arc::clone(&branches) as Arc<dyn ferrodb::branch::BranchCatalog>,
                ARENA_BASE,
            )
            .unwrap(),
        );
        let runtime = Arc::new(
            AgentRuntime::with_storage(
                branches,
                Arc::new(MemEffectLog::new()),
                Arc::clone(&store) as Arc<dyn PageStore>,
            )
            .unwrap(),
        );
        Db { catalog, bp, txn, runtime, _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {}", sql);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn rt(&self) -> Arc<AgentRuntime> {
        self.runtime.clone()
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok(
            "CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER, price INTEGER);",
            &mut s,
        );
        self.ok("CREATE TABLE audit (id INTEGER NOT NULL, n INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 100, 10);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 200, 20);", &mut s);
        self.ok("INSERT INTO audit VALUES (1, 0);", &mut s);
    }

    /// Open an agent session and return its branch and session.
    fn agent(&mut self, name: &str) -> (BranchId, Session) {
        let mut s = self.session();
        self.ok(&format!("BEGIN AGENT SESSION AS '{name}' RUN 'r_{name}';"), &mut s);
        let b = s.agent.as_ref().unwrap().branch;
        (b, s)
    }

    fn merge(&mut self, branch: BranchId) {
        let rt = self.rt();
        let (bp, txn) = (self.bp.clone(), self.txn.clone());
        let mut ctx = ExecCtx { catalog: &mut self.catalog, bp, txn };
        let r = rt.merge(&mut ctx, branch).unwrap();
        assert!(!r.outcome.is_conflict(), "merge conflicted: {:?}", r.outcome);
        assert!(r.applied_to_target, "merge did not publish");
    }

    fn cell(&mut self, table: &str, col: &str, branch: Option<BranchId>, id: i32) -> Option<i32> {
        let mut s = self.session();
        let sql = match branch {
            Some(b) => format!(
                "SELECT {col} FROM {table} AS OF BRANCH b_{} WHERE id = {id};",
                b.id
            ),
            None => format!("SELECT {col} FROM {table} WHERE id = {id};"),
        };
        match self.ok(&sql, &mut s) {
            Outcome::Rows(rows) => rows.first().and_then(|r| match r.first() {
                Some(Value::Integer(i)) => Some(*i),
                _ => None,
            }),
            _ => panic!("expected rows from {sql}"),
        }
    }

    /// The seq of the op that wrote `table.col` on the row with primary key `id`.
    ///
    /// Read out of `pickable_ops`, which is the catalogue an agent selects from — so the test
    /// names an op exactly the way a caller has to.
    fn seq_of(&self, table: &str, id: i32, col: u32) -> u64 {
        let row = row_id_of(&[Value::Integer(id)]);
        let ops = self.runtime.pickable_ops();
        ops.iter()
            .filter(|o| o.table == table && o.row == row && o.col == Some(ColId(col)))
            .map(|o| o.seq)
            .next_back()
            .unwrap_or_else(|| {
                panic!("no recorded op on {table}.col{col} for id {id}; log = {ops:?}")
            })
    }
}

// -------------------------------------------------------------------------------------------
// The capability
// -------------------------------------------------------------------------------------------

/// **One op of several lands, and the others do not.**
///
/// Agent A writes three cells and merges, so all three are in the applied-op log. Agent B picks
/// exactly one of them. The other two must be absent from B — that negative half is what makes
/// this a cherry-pick rather than a merge, and a wiring that applied everything would satisfy the
/// positive half on its own.
#[test]
fn one_op_of_several_lands_and_the_others_do_not() {
    let mut db = Db::new();
    db.seed();

    let (a, mut sa) = db.agent("agent-a");
    db.ok("UPDATE inventory SET qty = 111 WHERE id = 1;", &mut sa);
    db.ok("UPDATE inventory SET price = 99 WHERE id = 1;", &mut sa);
    db.ok("UPDATE inventory SET qty = 222 WHERE id = 2;", &mut sa);
    db.merge(a);

    // Everything A wrote is now on trunk and in the op log.
    assert_eq!(db.cell("inventory", "qty", None, 1), Some(111));
    assert_eq!(db.cell("inventory", "price", None, 1), Some(99));
    assert_eq!(db.cell("inventory", "qty", None, 2), Some(222));

    assert!(
        db.runtime.pickable_ops().len() >= 3,
        "expected at least three recorded ops, got {:?}",
        db.runtime.pickable_ops()
    );
    // The one op we want: `price` on row 1, named out of the same catalogue a caller reads.
    let price_seq = db.seq_of("inventory", 1, 2);

    let (b, _sb) = db.agent("agent-b");
    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let result = rt.cherry_pick(&mut ctx, a, &[price_seq], b).unwrap();
    assert!(
        result.is_applied(),
        "the pick was refused: {:?}",
        result.refusal().map(|r| r.kinds())
    );
    assert_eq!(result.applied().unwrap().picked, 1);

    // What B STAGED is the discriminator, not what B can see: a branch reads the shared table
    // overlaid with its own buffer, and A already merged, so B can see all three values whether or
    // not it picked anything. The staged set is what the pick actually did.
    let changes = db.runtime.page_changeset(b).unwrap();
    let rows: Vec<u64> = changes.iter().map(|c| c.row).collect();
    assert_eq!(
        rows.len(),
        1,
        "a pick of ONE op staged {} rows — the other two ops came along: {rows:?}",
        rows.len()
    );
    let staged = &changes[0];
    assert_eq!(staged.row, rid(1), "the pick staged the wrong row");
    let after = staged.after.as_ref().expect("the pick staged a deletion");
    assert_eq!(after[2], Value::Integer(99), "the picked cell did not land");

    // **The sharpest discriminator available: the OPS B now carries.** A branch reads the shared
    // table overlaid with its own buffer, so after A merged, B can SEE all three values whether or
    // not it picked anything — reading values back proves nothing here. What B will republish when
    // it merges is its op list, and a pick of one op must have produced exactly one.
    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let cs = rt.diff(&mut ctx, b).unwrap();
    assert_eq!(cs.rows.len(), 1, "one op picked, {} rows in B's changeset", cs.rows.len());
    let ops = &cs.rows[0].ops;
    assert_eq!(ops.len(), 1, "one op picked, {} ops staged: {ops:?}", ops.len());
    assert_eq!(
        ops[0].col,
        Some(ColId(2)),
        "the staged op names column {:?}, not the picked `price` column",
        ops[0].col
    );
}

// -------------------------------------------------------------------------------------------
// The refusals, each forced to fire
// -------------------------------------------------------------------------------------------

/// **A cell the target already moved is refused, and nothing is written.**
///
/// This is the divergence read, and it goes through D86's by-cell index — the same index
/// `concurrent_op` uses on the merge path. Forced to fire: B writes the very cell the pick would
/// land on, so the pick must refuse rather than clobber it.
#[test]
fn a_pick_whose_cell_the_target_already_moved_is_refused_and_writes_nothing() {
    let mut db = Db::new();
    db.seed();

    let (a, mut sa) = db.agent("agent-a");
    db.ok("UPDATE inventory SET price = 99 WHERE id = 1;", &mut sa);
    db.merge(a);
    let price_seq = db.seq_of("inventory", 1, 2);

    // A second agent moves the same cell and publishes it, so the target has diverged on it.
    let (c, mut sc) = db.agent("agent-c");
    db.ok("UPDATE inventory SET price = 55 WHERE id = 1;", &mut sc);
    db.merge(c);
    assert_eq!(db.cell("inventory", "price", None, 1), Some(55));

    let (b, _sb) = db.agent("agent-b");
    let before = db.runtime.page_changeset(b).unwrap().len();
    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let result = rt.cherry_pick(&mut ctx, a, &[price_seq], b).unwrap();

    assert!(
        !result.is_applied(),
        "a pick onto a cell that has since moved was applied, which is the silent-clobber case"
    );
    assert_eq!(
        db.runtime.page_changeset(b).unwrap().len(),
        before,
        "a refused pick staged rows on the target"
    );
    // The reason matters: it must be the DIVERGENCE finding, not some unrelated refusal that
    // happens to make this test green. A refusal for the wrong reason is a detector that fired by
    // accident.
    let kinds = result.refusal().unwrap().kinds();
    assert!(
        kinds.contains(&CherryConflictKind::TargetCellMoved),
        "expected the divergence refusal, got {kinds:?}"
    );
}

/// **A selection spanning two tables is refused rather than half-applied.**
///
/// The staging door is atomic per table, so across tables there is nothing holding the
/// all-or-nothing contract `cherry.rs` says the runtime owes. The dangerous state is removed by
/// refusing, not documented.
#[test]
fn a_pick_spanning_two_tables_is_refused_rather_than_half_applied() {
    let mut db = Db::new();
    db.seed();

    let (a, mut sa) = db.agent("agent-a");
    db.ok("UPDATE inventory SET price = 99 WHERE id = 1;", &mut sa);
    db.ok("UPDATE audit SET n = 7 WHERE id = 1;", &mut sa);
    db.merge(a);

    let inv = db.seq_of("inventory", 1, 2);
    let aud = db.seq_of("audit", 1, 1);

    let (b, _sb) = db.agent("agent-b");
    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let err = rt.cherry_pick(&mut ctx, a, &[inv, aud], b).unwrap_err().to_string();
    assert!(
        err.contains("spans") && err.contains("tables"),
        "a two-table pick must be refused with a reason, got: {err}"
    );
    assert!(
        db.runtime.page_changeset(b).unwrap().is_empty(),
        "a refused two-table pick staged something"
    );
}

/// **An empty selection is refused.** A pick that picked nothing has not picked, and an empty plan
/// is indistinguishable from a satisfied one at the call site.
#[test]
fn an_empty_selection_is_refused() {
    let mut db = Db::new();
    db.seed();
    let (a, mut sa) = db.agent("agent-a");
    db.ok("UPDATE inventory SET price = 99 WHERE id = 1;", &mut sa);
    db.merge(a);
    let (b, _sb) = db.agent("agent-b");

    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let result = rt.cherry_pick(&mut ctx, a, &[], b).unwrap();
    assert!(!result.is_applied());
    assert!(
        result.refusal().unwrap().has(CherryConflictKind::EmptySelection),
        "an empty selection must say so: {:?}",
        result.refusal().unwrap().kinds()
    );
}

/// **A selector naming no op is refused**, rather than silently picking the ops that do resolve.
#[test]
fn a_selector_naming_no_op_is_refused() {
    let mut db = Db::new();
    db.seed();
    let (a, mut sa) = db.agent("agent-a");
    db.ok("UPDATE inventory SET price = 99 WHERE id = 1;", &mut sa);
    db.merge(a);
    let good = db.seq_of("inventory", 1, 2);
    let (b, _sb) = db.agent("agent-b");

    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let result = rt.cherry_pick(&mut ctx, a, &[good, 9_999_999], b).unwrap();
    assert!(
        !result.is_applied(),
        "a selection containing an unresolvable selector was applied, so the good half landed \
         from a request that could not be honoured in full"
    );
    assert!(
        db.runtime.page_changeset(b).unwrap().is_empty(),
        "a refused pick staged the half that did resolve"
    );
}
