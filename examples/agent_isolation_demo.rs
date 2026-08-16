//! ferrobranch — end-to-end demonstration of the ten exit criteria.
//!
//! Run with:
//!
//! ```text
//! cargo run --example agent_isolation_demo
//! ```
//!
//! Every number printed below is read back out of the engine at the moment it is printed. There
//! are no hard-coded expected values in the transcript: each `[ok]` line is a live comparison,
//! and the process exits non-zero if any of them fails or if fewer than ten criteria report.
//!
//! **Two substrates, stated up front because it is the honest limitation of this demo.**
//! ferrobranch is built in layers that are wired by trait but not yet by data:
//!
//! - The **page layer** (`branch::ArenaPageStore` + `cow::CowTree` + `branch::LogBranchCatalog` +
//!   `branch::TwoTierReaper`) owns pages. Criteria 1 and 8 are page-count claims, so they are
//!   demonstrated here, against a real copy-on-write B+tree on a real file.
//! - The **agent-SQL surface** (`agent_sql::AgentRuntime`, reached through the ordinary scanner,
//!   parser, binder and executor) owns statements. Criteria 2-7 and 10 are statement-level
//!   claims, so they are demonstrated there.
//! - The **provenance layer** (`provenance::MemProvenanceStore`) owns per-row attribution, which
//!   is criterion 9.
//!
//! The agent-SQL surface forks its branches through the same `LogBranchCatalog` the page layer
//! uses, so branch identity, generations and leases are genuinely shared. What is *not* shared is
//! row storage: rows written inside an agent session live in that branch's in-memory workspace,
//! not on CoW pages. So "the page count returns to baseline" is proven for pages written through
//! the CoW tree, and is *not* transitively proven for rows written through SQL. DEMO.md says the
//! same thing at greater length; nothing here should be read as claiming more.

use std::collections::HashSet;
use std::fmt::Display;
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::{ChangeSet, MergeReport};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline, PageId, ARENA_EXTENT_PAGES};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::{CowPageLinks, CowTree, PageStore, PageType};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Executor, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::lower;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::planner::physical_plan::PhysicalPlan;
use ferrodb::provenance::capture::VersionSource;
use ferrodb::provenance::store::MemProvenanceStore;
use ferrodb::provenance::{ProvId, ProvenanceStore, RunEntity};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::tel::ids::{ColId, RowId};
use ferrodb::tel::merge::{MergeOutcome, MergePolicy};
use ferrodb::tel::op::OpKind;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::{ReadView, TxnManager};

// =================================================================================================
// transcript plumbing
// =================================================================================================

const RULE: &str = "\
────────────────────────────────────────────────────────────────────────────────";

#[derive(Clone, Copy, PartialEq)]
enum Verdict {
    Met,
    NotMet,
}

impl Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Verdict::Met => write!(f, "MET"),
            Verdict::NotMet => write!(f, "NOT MET"),
        }
    }
}

struct Entry {
    n: u8,
    title: &'static str,
    layer: &'static str,
    verdict: Verdict,
    scope: String,
}

struct Ledger {
    entries: Vec<Entry>,
}

impl Ledger {
    fn new() -> Ledger {
        Ledger { entries: Vec::new() }
    }

    /// Close out a criterion. `scope` is the honest qualifier that goes in the summary table —
    /// what this run did *not* establish, even when every check passed.
    fn record(
        &mut self,
        n: u8,
        title: &'static str,
        layer: &'static str,
        checks: Checks,
        scope: &str,
    ) {
        let verdict = if checks.fails.is_empty() { Verdict::Met } else { Verdict::NotMet };
        println!();
        println!("  ==> CRITERION {} {}", n, verdict);
        if !checks.fails.is_empty() {
            for f in &checks.fails {
                println!("      failed check: {}", f);
            }
        }
        if !scope.is_empty() {
            println!("      scope: {}", scope);
        }
        self.entries.push(Entry { n, title, layer, verdict, scope: scope.to_string() });
    }

    /// Print the summary and exit. Non-zero if anything failed, and non-zero if the demo did not
    /// report on all ten criteria — a run that collected nothing has not passed.
    fn finish(self) -> ! {
        println!();
        println!("{}", RULE);
        println!("SUMMARY");
        println!("{}", RULE);
        println!();
        println!("  {:<3} {:<7} {:<13} {}", "#", "VERDICT", "LAYER", "CRITERION");
        println!("  {:<3} {:<7} {:<13} {}", "-", "-------", "-----", "---------");
        for e in &self.entries {
            println!(
                "  {:<3} {:<7} {:<13} {}",
                e.n,
                e.verdict.to_string(),
                e.layer,
                e.title
            );
        }

        let met = self.entries.iter().filter(|e| e.verdict == Verdict::Met).count();
        let failed = self.entries.iter().filter(|e| e.verdict == Verdict::NotMet).count();

        println!();
        println!("  reported: {} of 10    met: {}    not met: {}", self.entries.len(), met, failed);

        let scoped: Vec<&Entry> = self.entries.iter().filter(|e| !e.scope.is_empty()).collect();
        if !scoped.is_empty() {
            println!();
            println!("  Qualifiers on criteria that passed — read these before quoting the table:");
            for e in scoped {
                println!("    {:>2}. {}", e.n, e.scope);
            }
        }

        println!();
        if self.entries.len() != 10 {
            println!(
                "  DEMO FAILED: reported on {} criteria, expected 10.",
                self.entries.len()
            );
            std::process::exit(1);
        }
        if failed > 0 {
            println!("  DEMO FAILED: {} criteria NOT MET.", failed);
            std::process::exit(1);
        }
        println!("  All ten criteria met, within the scope qualifiers listed above.");
        std::process::exit(0);
    }
}

/// A live check. Prints its own verdict so the transcript shows the comparison, not just the
/// conclusion, and collects failures instead of panicking so one bad criterion cannot hide the
/// nine after it.
struct Checks {
    fails: Vec<String>,
}

impl Checks {
    fn new() -> Checks {
        Checks { fails: Vec::new() }
    }

    fn that(&mut self, ok: bool, what: impl Into<String>) {
        let what = what.into();
        println!("    [{}] {}", if ok { "ok  " } else { "FAIL" }, what);
        if !ok {
            self.fails.push(what);
        }
    }
}

fn criterion(n: u8, title: &str, layer: &str) {
    println!();
    println!();
    println!("{}", RULE);
    println!("CRITERION {} — {}", n, title);
    println!("  layer under test: {}", layer);
    println!("{}", RULE);
}

fn step(s: &str) {
    println!();
    println!("  {}", s);
}

fn kv(label: &str, value: impl Display) {
    println!("    {:<46} {}", format!("{}:", label), value);
}

fn note(s: &str) {
    println!("    · {}", s);
}

/// Echo a statement as a session transcript line, then run it.
fn sql_echo(who: &str, stmt: &str) {
    println!("    {:<9} {}", format!("[{}]", who), stmt);
}

// =================================================================================================
// Act I — the page layer: ArenaPageStore + CowTree + LogBranchCatalog + TwoTierReaper
// =================================================================================================

struct PageEnv {
    catalog: Arc<LogBranchCatalog>,
    store: Arc<ArenaPageStore>,
    tree: CowTree,
    _dir: tempfile::TempDir,
}

fn page_env() -> PageEnv {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("pages.db"))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    // The arena region must start at or above the bitmap allocator's real high-water mark, or
    // the two allocators hand out the same pages. `next_page_id` is NOT that mark.
    let base = pool.disk_manager.high_water().unwrap();
    let store =
        Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), base).unwrap());
    let tree = CowTree::new(Arc::clone(&store) as Arc<dyn PageStore>);
    PageEnv { catalog, store, tree, _dir: dir }
}

impl PageEnv {
    fn arenas_of(&self, b: BranchId) -> usize {
        self.store.live_arenas().iter().filter(|(_, owner)| *owner == b).count()
    }

    fn internal_nodes(&self, root: PageId) -> usize {
        self.tree
            .walk_pages(root)
            .unwrap()
            .iter()
            .filter(|p| {
                self.store.read_page(**p).unwrap().header().unwrap().page_type
                    == PageType::BTreeInternal
            })
            .count()
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("k{:06}", i).into_bytes()
}

fn val(i: u32) -> Vec<u8> {
    format!("v{:06}", i).into_bytes()
}

/// Keys enough to force a multi-level tree, so "the child's tree is the parent's tree" is a claim
/// about a real internal-node structure and not about one leaf.
const TRUNK_KEYS: u32 = 400;

// ---- criterion 1 --------------------------------------------------------------------------------

fn criterion_1(led: &mut Ledger) {
    criterion(
        1,
        "BEGIN AGENT SESSION forks a branch copying ZERO data pages",
        "page layer",
    );
    let mut c = Checks::new();
    let e = page_env();

    step("Trunk writes a real copy-on-write B+tree.");
    let ep = e.catalog.next_epoch();
    let mut root = e.tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..TRUNK_KEYS {
        root = e.tree.insert(root, BranchId::TRUNK, ep, &key(i), &val(i)).unwrap();
    }
    e.catalog.set_root(BranchId::TRUNK, root).unwrap();

    let live_before = e.store.live_page_count().unwrap();
    let reserved_before = e.store.reserved_page_count();
    let tree_before: Vec<PageId> = e.tree.walk_pages(root).unwrap();
    let internal_before = e.internal_nodes(root);

    kv("keys written on trunk", TRUNK_KEYS);
    kv("data pages allocated (live_page_count)", live_before);
    kv("pages reserved in arenas", reserved_before);
    kv("pages reachable from the trunk root", tree_before.len());
    kv("of which internal (non-leaf) nodes", internal_before);
    kv("trunk root page id", root);

    c.that(
        internal_before > 0,
        "the trunk tree is multi-level, so the fork claim is about a real page graph",
    );

    step("Now fork an agent branch. This is what BEGIN AGENT SESSION does underneath.");
    let child = e.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
    let live_after = e.store.live_page_count().unwrap();
    let reserved_after = e.store.reserved_page_count();
    let tree_after: Vec<PageId> = e.tree.walk_pages(child.root_page_id).unwrap();

    println!();
    println!("    {:<46} {:>10} {:>10} {:>8}", "", "BEFORE", "AFTER", "DELTA");
    println!(
        "    {:<46} {:>10} {:>10} {:>8}",
        "data pages allocated",
        live_before,
        live_after,
        live_after as i64 - live_before as i64
    );
    println!(
        "    {:<46} {:>10} {:>10} {:>8}",
        "pages reserved in arenas",
        reserved_before,
        reserved_after,
        reserved_after as i64 - reserved_before as i64
    );
    println!(
        "    {:<46} {:>10} {:>10} {:>8}",
        "pages reachable from the branch root",
        tree_before.len(),
        tree_after.len(),
        tree_after.len() as i64 - tree_before.len() as i64
    );
    println!();
    kv("trunk root page id", root);
    kv("forked branch root page id", child.root_page_id);
    kv("arenas owned by the new branch", e.arenas_of(child.branch_id));

    c.that(live_after == live_before, "fork allocated ZERO data pages");
    c.that(reserved_after == reserved_before, "fork reserved ZERO additional pages");
    c.that(
        child.root_page_id == root,
        "the child's root IS the parent's root — the fork is one pointer",
    );
    c.that(
        tree_after == tree_before,
        "the child's page set is physically identical to the parent's",
    );
    c.that(
        e.arenas_of(child.branch_id) == 0,
        "a branch that has not written owns no arena",
    );

    step("The child reads the parent's data by ordinary descent — no parent-chain walk.");
    let mut readable = 0u32;
    for i in 0..TRUNK_KEYS {
        if e.tree.get(child.root_page_id, &key(i)).unwrap().as_deref() == Some(val(i).as_slice()) {
            readable += 1;
        }
    }
    kv("keys readable from the child root", format!("{} / {}", readable, TRUNK_KEYS));
    c.that(readable == TRUNK_KEYS, "every trunk key is visible from the child with no copy");

    step("Negative control — prove the page counter is not simply stuck at zero.");
    let ep2 = e.catalog.next_epoch();
    let new_root = e
        .tree
        .insert(child.root_page_id, child.branch_id, ep2, b"k000007", b"the-child-wrote-this")
        .unwrap();
    let live_child_wrote = e.store.live_page_count().unwrap();
    kv("data pages after ONE write on the child", live_child_wrote);
    kv("delta from the fork-time count", live_child_wrote as i64 - live_after as i64);
    kv("arenas owned by the branch now", e.arenas_of(child.branch_id));
    c.that(
        live_child_wrote > live_after,
        "writing DOES move the counter, so the zero above is a measurement and not a dead gauge",
    );
    c.that(
        new_root != child.root_page_id,
        "the write shadowed a new root rather than mutating the parent's",
    );
    c.that(
        e.tree.get(root, b"k000007").unwrap().as_deref() == Some(val(7).as_slice()),
        "and the parent's own root still reads the OLD value at that key",
    );

    led.record(
        1,
        "Fork copies zero data pages",
        "page layer",
        c,
        "measured on the CoW page store; the agent-SQL surface forks the same catalog but does \
         not yet store its rows on these pages",
    );
}

// ---- criterion 8 --------------------------------------------------------------------------------

/// Branches for the thesis run. Each takes one ~1MB arena extent when it first writes.
const ABANDONED: usize = 16;
const KEYS_PER_BRANCH: u32 = 12;
const LEASE_MS: u64 = 10_000;

fn criterion_8(led: &mut Ledger) {
    criterion(
        8,
        "*** THE THESIS *** branches abandoned with NO client cooperation are reaped on lease \
         expiry and the page count returns to baseline",
        "page layer",
    );
    let mut c = Checks::new();
    let e = page_env();
    let reaper = TwoTierReaper::new(Arc::clone(&e.catalog), Arc::clone(&e.store))
        .with_links(Arc::new(CowPageLinks));

    step("Trunk holds a real tree.");
    let ep = e.catalog.next_epoch();
    let mut root = e.tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..TRUNK_KEYS {
        root = e.tree.insert(root, BranchId::TRUNK, ep, &key(i), &val(i)).unwrap();
    }
    e.catalog.set_root(BranchId::TRUNK, root).unwrap();

    step(
        "One agent session opens and stays legitimately open, with an hour-long lease. It is the \
         negative control: reaping must not touch it.",
    );
    note("this branch is part of the BASELINE, so 'returns to baseline' cannot be satisfied by");
    note("simply deleting everything — a reaper that over-reaps fails the same check as one that");
    note("under-reaps");
    let survivor = e.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(3_600_000)).unwrap();
    let sep = e.catalog.next_epoch();
    let mut sr = survivor.root_page_id;
    for i in 0..KEYS_PER_BRANCH {
        let k = format!("survivor-key{:03}", i).into_bytes();
        sr = e.tree.insert(sr, survivor.branch_id, sep, &k, b"still-working").unwrap();
    }
    e.catalog.set_root(survivor.branch_id, sr).unwrap();

    step("BASELINE: trunk plus one healthy open agent session.");
    let base_live = e.store.live_page_count().unwrap();
    let base_reserved = e.store.reserved_page_count();
    let base_branches = e.catalog.live_count();
    kv("data pages allocated", base_live);
    kv("pages reserved in arenas", base_reserved);
    kv("live branches", base_branches);
    kv("arena extent size (pages)", ARENA_EXTENT_PAGES);

    step(&format!(
        "Now {} agent tasks each fork a branch and write {} keys — then are killed.",
        ABANDONED, KEYS_PER_BRANCH
    ));
    let mut abandoned = Vec::new();
    for n in 0..ABANDONED {
        let rec = e.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
        let bep = e.catalog.next_epoch();
        let mut r = rec.root_page_id;
        for i in 0..KEYS_PER_BRANCH {
            let k = format!("agent{:03}-key{:03}", n, i).into_bytes();
            r = e.tree.insert(r, rec.branch_id, bep, &k, b"written-by-an-agent-that-then-died").unwrap();
        }
        e.catalog.set_root(rec.branch_id, r).unwrap();
        abandoned.push(rec.branch_id);
    }

    let peak_live = e.store.live_page_count().unwrap();
    let peak_reserved = e.store.reserved_page_count();
    let peak_branches = e.catalog.live_count();
    kv("data pages allocated", peak_live);
    kv("pages reserved in arenas", peak_reserved);
    kv("live branches", peak_branches);
    c.that(peak_live > base_live, "the abandoned branches really did allocate pages");
    c.that(peak_reserved > base_reserved, "and really did take arena extents");

    step("The agents now die. NOTHING is called on their behalf:");
    note("no ABANDON, no MERGE, no close(), no free(), no rollback, no destructor of ours");
    note("the BranchIds are simply dropped on the floor, as a killed agent process would");
    drop(abandoned.clone());

    step("A background lease scan runs. It is given a clock reading past the leases.");
    note(&format!(
        "leases were {} ms; the scan is called with now = now + {} ms",
        LEASE_MS,
        10 * LEASE_MS
    ));
    note("the reaper takes `now_millis` as a parameter, so the demo advances time instead of sleeping");
    let far_future = LeaseDeadline::now_millis() + 10 * LEASE_MS;
    let reaped = reaper.reap_expired(far_future).unwrap();
    let _ = reaper.drain_pending().unwrap();

    let after_live = e.store.live_page_count().unwrap();
    let after_reserved = e.store.reserved_page_count();
    let after_branches = e.catalog.live_count();

    println!();
    kv("branches reaped by the scan", reaped.len());
    println!();
    println!(
        "    {:<38} {:>10} {:>10} {:>10}",
        "", "BEFORE", "DURING", "AFTER"
    );
    println!(
        "    {:<38} {:>10} {:>10} {:>10}",
        "data pages allocated", base_live, peak_live, after_live
    );
    println!(
        "    {:<38} {:>10} {:>10} {:>10}",
        "pages reserved in arenas", base_reserved, peak_reserved, after_reserved
    );
    println!(
        "    {:<38} {:>10} {:>10} {:>10}",
        "live branches", base_branches, peak_branches, after_branches
    );
    println!();

    c.that(
        reaped.len() == ABANDONED,
        format!("exactly the {} abandoned branches were reaped", ABANDONED),
    );
    c.that(
        abandoned.iter().all(|b| reaped.contains(b)),
        "every abandoned branch is in the reaped set",
    );
    c.that(
        !reaped.contains(&survivor.branch_id),
        "the branch whose lease had NOT expired was left alone (no spurious reaping)",
    );
    c.that(
        after_live == base_live,
        format!("allocated page count returned to baseline ({} -> {} -> {})", base_live, peak_live, after_live),
    );
    c.that(
        after_reserved == base_reserved,
        "every arena extent went back to the free space map, not merely stopped growing",
    );

    step("A reaped branch is a hard error on read, never stale data.");
    let dead = abandoned[0];
    let read_back = e.catalog.get(dead);
    kv("reading a reaped branch", match &read_back {
        Ok(_) => "returned a record (WRONG)".to_string(),
        Err(err) => format!("Err({})", err),
    });
    c.that(read_back.is_err(), "reading a reaped branch is refused");

    step("The negative control still works, and its data is intact.");
    let alive = e.catalog.get(survivor.branch_id);
    c.that(alive.is_ok(), "the unexpired branch is still readable");
    let mut survivor_keys = 0u32;
    for i in 0..KEYS_PER_BRANCH {
        let k = format!("survivor-key{:03}", i).into_bytes();
        if e.tree.get(sr, &k).unwrap().is_some() {
            survivor_keys += 1;
        }
    }
    kv("survivor keys still readable", format!("{} / {}", survivor_keys, KEYS_PER_BRANCH));
    c.that(
        survivor_keys == KEYS_PER_BRANCH,
        "reaping its siblings did not damage the surviving branch's pages",
    );

    step("Trunk is untouched by any of it.");
    let mut trunk_keys = 0u32;
    for i in 0..TRUNK_KEYS {
        if e.tree.get(root, &key(i)).unwrap().as_deref() == Some(val(i).as_slice()) {
            trunk_keys += 1;
        }
    }
    kv("trunk keys still readable", format!("{} / {}", trunk_keys, TRUNK_KEYS));
    c.that(trunk_keys == TRUNK_KEYS, "trunk survived the reaping intact");

    led.record(
        8,
        "Abandoned branches reaped on lease expiry, pages return to baseline",
        "page layer",
        c,
        "time is advanced by passing a clock value to the scan rather than by a real background \
         thread; no daemon is wired up yet, and SQL-session rows are not on these pages",
    );
}

// =================================================================================================
// Act II — the agent-SQL surface, driven through the real scanner/parser/binder/executor
// =================================================================================================

struct Sql {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Sql {
    fn new() -> Sql {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("agent.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("agent.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Sql { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    /// A connection sharing this database's agent runtime, so branches are mutually visible.
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
        assert_eq!(stmts.len(), 1, "expected exactly one statement: {}", sql);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        match self.exec(sql, s) {
            Ok(o) => o,
            Err(e) => panic!("{} failed: {}", sql, e),
        }
    }

    /// Run a statement and echo it as a transcript line.
    fn run_as(&mut self, who: &str, sql: &str, s: &mut Session) -> Outcome {
        sql_echo(who, sql);
        self.ok(sql, s)
    }

    fn seed(&mut self) {
        let mut s = self.session();
        for stmt in [
            "CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);",
            "INSERT INTO inventory VALUES (1, 20);",
            "INSERT INTO inventory VALUES (2, 5);",
        ] {
            self.ok(stmt, &mut s);
        }
    }

    fn heap(&self, table: &str) -> HeapFileManager {
        let entry = self.catalog.get_table(table).unwrap();
        HeapFileManager::open(entry.first_directory_page_id, self.bp.clone())
    }

    /// The physical slot currently holding the row with this id, found by a real scan.
    fn rid_of(&self, table: &str, id: i32) -> RecordId {
        let view = Arc::new(ReadView { snapshot: self.txn.read_snapshot(), txn_id: 0 });
        let mut exec: Box<dyn Executor> = lower(
            PhysicalPlan::SeqScan { table: table.into() },
            &self.catalog,
            self.bp.clone(),
            view,
        )
        .unwrap();
        while let Some(row) = exec.next() {
            let (rid, values) = row.unwrap();
            if values[0] == Value::Integer(id) {
                return rid;
            }
        }
        panic!("row {} not found in {}", id, table);
    }

    /// qty of a row as main sees it.
    fn qty(&mut self, id: i32) -> i32 {
        let mut s = self.session();
        let r = rows(self.ok(&format!("SELECT qty FROM inventory WHERE id = {};", id), &mut s));
        match r.first().map(|row| &row[0]) {
            Some(Value::Integer(i)) => *i,
            other => panic!("row {} qty unreadable: {:?}", id, other),
        }
    }
}

fn rows(out: Outcome) -> Vec<Vec<Value>> {
    match out {
        Outcome::Rows(r) => r,
        other => panic!("expected rows, got {:?}", std::mem::discriminant(&other)),
    }
}

fn agent_out(out: Outcome) -> AgentOutput {
    match out {
        Outcome::Agent(a) => a,
        _ => panic!("expected an agent output"),
    }
}

fn changeset(out: Outcome) -> ChangeSet {
    match agent_out(out) {
        AgentOutput::Diff(d) => d,
        other => panic!("expected a changeset, got {}", other),
    }
}

fn report(out: Outcome) -> MergeReport {
    match agent_out(out) {
        AgentOutput::Merge(m) => m,
        other => panic!("expected a merge report, got {}", other),
    }
}

fn ints(rs: &[Vec<Value>], col: usize) -> Vec<i32> {
    let mut v: Vec<i32> = rs
        .iter()
        .map(|r| match r[col] {
            Value::Integer(i) => i,
            ref other => panic!("not an integer: {:?}", other),
        })
        .collect();
    v.sort();
    v
}

fn one_int(rs: &[Vec<Value>]) -> i32 {
    assert_eq!(rs.len(), 1, "expected exactly one row, got {}", rs.len());
    match rs[0][0] {
        Value::Integer(i) => i,
        ref other => panic!("not an integer: {:?}", other),
    }
}

// ---- criterion 2 --------------------------------------------------------------------------------

fn criterion_2(led: &mut Ledger) {
    criterion(
        2,
        "Branch writes are invisible to main and to sibling branches until merge",
        "agent SQL",
    );
    let mut c = Checks::new();
    let mut db = Sql::new();
    db.seed();

    step("Three connections to one database. Two open agent sessions; the third stays on main.");
    let mut a = db.session();
    let mut b = db.session();
    let mut main = db.session();
    db.run_as("agent-a", "BEGIN AGENT SESSION AS 'restock' RUN 'r1';", &mut a);
    db.run_as("agent-b", "BEGIN AGENT SESSION AS 'pricing' RUN 'r2';", &mut b);

    step("Agent A writes three different shapes. Nobody merges.");
    db.run_as("agent-a", "UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    db.run_as("agent-a", "INSERT INTO inventory VALUES (3, 7);", &mut a);
    db.run_as("agent-a", "DELETE FROM inventory WHERE id = 2;", &mut a);

    step("Now the same SELECT from all three connections.");
    let q = "SELECT qty FROM inventory WHERE id = 1;";
    sql_echo("agent-a", q);
    let seen_a = one_int(&rows(db.ok(q, &mut a)));
    sql_echo("agent-b", q);
    let seen_b = one_int(&rows(db.ok(q, &mut b)));
    sql_echo("main", q);
    let seen_main = one_int(&rows(db.ok(q, &mut main)));

    println!();
    kv("qty of row 1 as the WRITER sees it", seen_a);
    kv("qty of row 1 as the SIBLING branch sees it", seen_b);
    kv("qty of row 1 as MAIN sees it", seen_main);

    c.that(seen_a == 15, "the writing branch sees its own uncommitted write (20 - 5 = 15)");
    c.that(seen_main == 20, "main does NOT see it");
    c.that(seen_b == 20, "the sibling branch does NOT see it either");

    step("The insert and the delete are equally invisible.");
    let all = "SELECT id FROM inventory;";
    sql_echo("agent-a", all);
    let ids_a = ints(&rows(db.ok(all, &mut a)), 0);
    sql_echo("main", all);
    let ids_main = ints(&rows(db.ok(all, &mut main)), 0);
    sql_echo("agent-b", all);
    let ids_b = ints(&rows(db.ok(all, &mut b)), 0);
    kv("ids visible on the writing branch", format!("{:?}", ids_a));
    kv("ids visible on main", format!("{:?}", ids_main));
    kv("ids visible on the sibling branch", format!("{:?}", ids_b));
    c.that(ids_a == vec![1, 3], "the writer sees its insert and not its deleted row");
    c.that(ids_main == vec![1, 2], "main sees neither the insert nor the delete");
    c.that(ids_b == vec![1, 2], "and neither does the sibling");

    step("MERGE publishes. Only now does main move.");
    sql_echo("agent-a", "MERGE;");
    let r = report(db.ok("MERGE;", &mut a));
    kv("merge outcome", r.outcome.name());
    kv("applied to target", r.applied_to_target);
    sql_echo("main", all);
    let ids_after = ints(&rows(db.ok(all, &mut main)), 0);
    kv("ids visible on main AFTER the merge", format!("{:?}", ids_after));
    c.that(r.applied_to_target, "the merge published");
    c.that(ids_after == vec![1, 3], "main now sees exactly what the branch did");

    led.record(
        2,
        "Branch writes invisible to main and siblings until merge",
        "agent SQL",
        c,
        "isolation is enforced by the per-branch workspace in the agent runtime, not yet by page \
         visibility in the CoW store",
    );
}

// ---- criterion 3 --------------------------------------------------------------------------------

fn criterion_3(led: &mut Ledger) {
    criterion(
        3,
        "SELECT ... AS OF BRANCH reads another branch's UNCOMMITTED state",
        "agent SQL",
    );
    let mut c = Checks::new();
    let mut db = Sql::new();
    db.seed();

    let mut a = db.session();
    let mut observer = db.session();

    step("An agent session writes and does not merge.");
    let started = agent_out(db.run_as(
        "agent-a",
        "BEGIN AGENT SESSION AS 'restock' RUN 'r_8fk2';",
        &mut a,
    ));
    let branch_name = match started {
        AgentOutput::SessionStarted(s) => {
            kv("session", format!("{}", s));
            s.branch_name
        }
        other => panic!("expected a session, got {}", other),
    };
    db.run_as("agent-a", "UPDATE inventory SET qty = qty + 30 WHERE id = 1;", &mut a);

    step("A DIFFERENT connection, which never opened a session, asks for that branch by name.");
    let as_of = format!("SELECT qty FROM inventory AS OF BRANCH {};", branch_name);
    sql_echo("observer", &as_of);
    let seen = ints(&rows(db.ok(&as_of, &mut observer)), 0);
    kv("qty values on that branch", format!("{:?}", seen));

    let plain = "SELECT qty FROM inventory;";
    sql_echo("observer", plain);
    let seen_main = ints(&rows(db.ok(plain, &mut observer)), 0);
    kv("qty values without AS OF (i.e. main)", format!("{:?}", seen_main));

    c.that(seen == vec![5, 50], "AS OF BRANCH sees the branch's uncommitted 20 + 30 = 50");
    c.that(seen_main == vec![5, 20], "the same connection without AS OF still sees main's 20");

    step("AS OF composes with WHERE and projection rather than being a special whole-table mode.");
    let filtered =
        format!("SELECT qty FROM inventory AS OF BRANCH {} WHERE id = 1;", branch_name);
    sql_echo("observer", &filtered);
    let one = one_int(&rows(db.ok(&filtered, &mut observer)));
    kv("qty of row 1 on that branch", one);
    c.that(one == 50, "AS OF composes with a predicate");

    step("Uncommitted inserts and deletes are reachable the same way.");
    db.run_as("agent-a", "INSERT INTO inventory VALUES (9, 99);", &mut a);
    db.run_as("agent-a", "DELETE FROM inventory WHERE id = 2;", &mut a);
    let ids_sql = format!("SELECT id FROM inventory AS OF BRANCH {};", branch_name);
    sql_echo("observer", &ids_sql);
    let ids = ints(&rows(db.ok(&ids_sql, &mut observer)), 0);
    kv("ids on that branch", format!("{:?}", ids));
    sql_echo("observer", "SELECT id FROM inventory;");
    let ids_main = ints(&rows(db.ok("SELECT id FROM inventory;", &mut observer)), 0);
    kv("ids on main", format!("{:?}", ids_main));
    c.that(ids == vec![1, 9], "the observer sees the branch's uncommitted insert and delete");
    c.that(ids_main == vec![1, 2], "main is still untouched");

    step("Naming a branch that does not exist is an error, never an empty answer.");
    let bad = db.exec("SELECT qty FROM inventory AS OF BRANCH b_99;", &mut observer);
    kv("SELECT ... AS OF BRANCH b_99", match &bad {
        Ok(_) => "succeeded (WRONG)".to_string(),
        Err(e) => format!("Err({})", e),
    });
    c.that(bad.is_err(), "an unknown branch is refused rather than silently returning nothing");

    led.record(
        3,
        "SELECT ... AS OF BRANCH reads another branch's uncommitted state",
        "agent SQL",
        c,
        "",
    );
}

// ---- criterion 4 --------------------------------------------------------------------------------

fn criterion_4(led: &mut Ledger) {
    criterion(4, "DIFF returns a structured changeset", "agent SQL");
    let mut c = Checks::new();
    let mut db = Sql::new();
    db.seed();

    let mut a = db.session();
    db.run_as("agent-a", "BEGIN AGENT SESSION AS 'pricing' RUN 'r1';", &mut a);
    db.run_as(
        "agent-a",
        "UPDATE inventory SET qty = qty - 5 WHERE qty >= 5 AND id = 1;",
        &mut a,
    );
    db.run_as("agent-a", "INSERT INTO inventory VALUES (4, 1);", &mut a);

    step("DIFF. What comes back is data, not rendered text — every field below is read off the struct.");
    sql_echo("agent-a", "DIFF;");
    let d = changeset(db.ok("DIFF;", &mut a));

    kv("changeset.from (branch)", format!("{}", d.from));
    kv("changeset.to (branch)", format!("{}", d.to));
    kv("changeset.rows.len()", d.rows.len());

    for (i, rc) in d.rows.iter().enumerate() {
        println!();
        println!("    row change [{}]", i);
        kv("  .table", &rc.table);
        kv("  .row (RowId)", format!("{:?}", rc.row));
        kv("  .kind", format!("{:?}", rc.kind));
        kv("  .before", format!("{:?}", rc.before));
        kv("  .after", format!("{:?}", rc.after));
        kv("  .outcome", format!("{:?}", rc.outcome));
        for (j, op) in rc.ops.iter().enumerate() {
            kv(
                &format!("  .ops[{}].kind", j),
                format!("{:?}", op.kind),
            );
            kv(
                &format!("  .ops[{}].witness (pre-op observed value)", j),
                format!("{:?}", op.witness),
            );
        }
        for (j, g) in rc.guards.iter().enumerate() {
            kv(
                &format!("  .guards[{}]", j),
                g.violated_predicate(),
            );
        }
    }

    c.that(d.rows.len() == 2, "the changeset has one entry per changed row");

    let update = d.rows.iter().find(|r| format!("{:?}", r.kind) == "Update");
    match update {
        Some(u) => {
            c.that(u.before.is_some() && u.after.is_some(), "an update carries before AND after");
            c.that(
                matches!(u.ops.first().map(|o| &o.kind), Some(OpKind::Add(_))),
                "`qty = qty - 5` is retained as the algebra element Add(-5), not as a scalar",
            );
            c.that(
                u.ops.first().and_then(|o| o.witness.as_ref()).is_some(),
                "the op carries its witness (the value observed before it applied)",
            );
            c.that(
                u.guards.iter().any(|g| g.violated_predicate().contains("qty >= 5")),
                "the guard `qty >= 5` that made the write legal is kept verbatim",
            );
        }
        None => c.that(false, "the changeset contains an Update row"),
    }

    let insert = d.rows.iter().find(|r| format!("{:?}", r.kind) == "Insert");
    match insert {
        Some(ins) => {
            c.that(ins.before.is_none(), "an insert has no before-image");
            c.that(
                matches!(ins.ops.first().map(|o| &o.kind), Some(OpKind::RowCreate(_))),
                "and is recorded as RowCreate",
            );
        }
        None => c.that(false, "the changeset contains an Insert row"),
    }

    step("An untouched branch still returns a changeset, not an error and not nothing.");
    let mut idle = db.session();
    db.run_as("agent-c", "BEGIN AGENT SESSION AS 'idle' RUN 'r9';", &mut idle);
    sql_echo("agent-c", "DIFF;");
    let empty = changeset(db.ok("DIFF;", &mut idle));
    kv("empty changeset rows", empty.rows.len());
    kv("is_empty()", empty.is_empty());
    c.that(empty.is_empty(), "an idle branch diffs to an empty changeset");

    led.record(4, "DIFF returns a structured changeset", "agent SQL", c, "");
}

// ---- criterion 5 --------------------------------------------------------------------------------

fn criterion_5(led: &mut Ledger) {
    criterion(
        5,
        "MERGE reports CLEAN / COMMUTING / CONFLICT / RESOLVED-WITH-LOSS",
        "agent SQL",
    );
    let mut c = Checks::new();
    let mut seen: Vec<String> = Vec::new();

    // -- Clean ------------------------------------------------------------------------------
    step("(a) One branch, nothing concurrent  ->  expect Clean");
    {
        let mut db = Sql::new();
        db.seed();
        let mut a = db.session();
        db.run_as("agent-a", "BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
        db.run_as("agent-a", "UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
        sql_echo("agent-a", "MERGE;");
        let r = report(db.ok("MERGE;", &mut a));
        kv("outcome", r.outcome.name());
        kv("applied_to_target", r.applied_to_target);
        kv("qty on main afterwards", db.qty(1));
        seen.push(r.outcome.name().to_string());
        c.that(r.outcome == MergeOutcome::Clean, "a solo merge reports Clean");
        c.that(db.qty(1) == 15, "and main really moved to 15");
    }

    // -- Commuting --------------------------------------------------------------------------
    step("(b) Two branches, both Add on the same cell  ->  expect Commuting");
    {
        let mut db = Sql::new();
        db.seed();
        let mut a = db.session();
        let mut b = db.session();
        db.run_as("agent-a", "BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
        db.run_as("agent-b", "BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
        db.run_as("agent-a", "UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
        db.run_as("agent-b", "UPDATE inventory SET qty = qty - 3 WHERE id = 1;", &mut b);
        sql_echo("agent-a", "MERGE;");
        let first = report(db.ok("MERGE;", &mut a));
        kv("first merge outcome", first.outcome.name());
        sql_echo("agent-b", "MERGE;");
        let second = report(db.ok("MERGE;", &mut b));
        kv("second merge outcome", second.outcome.name());
        kv("qty on main afterwards", db.qty(1));
        seen.push(second.outcome.name().to_string());
        c.that(
            matches!(second.outcome, MergeOutcome::Commuting { .. }),
            "the second merge, against a target that moved, reports Commuting",
        );
    }

    // -- Conflict ---------------------------------------------------------------------------
    step("(c) Two branches, contradictory assignments, no declared policy  ->  expect Conflict");
    note("a column with no declared merge policy forbids concurrent updates; it does NOT fall back to last-writer-wins");
    {
        let mut db = Sql::new();
        db.seed();
        let mut a = db.session();
        let mut b = db.session();
        db.run_as("agent-a", "BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
        db.run_as("agent-b", "BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
        db.run_as("agent-a", "UPDATE inventory SET qty = 1 WHERE id = 1;", &mut a);
        db.run_as("agent-b", "UPDATE inventory SET qty = 2 WHERE id = 1;", &mut b);
        sql_echo("agent-a", "MERGE;");
        let first = report(db.ok("MERGE;", &mut a));
        kv("first merge outcome", first.outcome.name());
        sql_echo("agent-b", "MERGE;");
        let second = report(db.ok("MERGE;", &mut b));
        kv("second merge outcome", second.outcome.name());
        kv("applied_to_target", second.applied_to_target);
        kv("qty on main afterwards", db.qty(1));
        seen.push(second.outcome.name().to_string());
        c.that(second.outcome.is_conflict(), "contradictory assignments report Conflict");
        c.that(!second.applied_to_target, "and publish nothing");
        c.that(db.qty(1) == 1, "main still holds the first writer's value");
        let still_there = db.exec("DIFF;", &mut b);
        c.that(still_there.is_ok(), "the conflicting branch stays alive so the agent can retry");
    }

    // -- ResolvedWithLoss -------------------------------------------------------------------
    step("(d) Same, but qty is declared LWW  ->  expect ResolvedWithLoss, never Clean");
    {
        let mut db = Sql::new();
        db.seed();
        db.runtime.set_policy("inventory", ColId(1), MergePolicy::Lww);
        note("policy declared: inventory.qty = LWW");
        let mut a = db.session();
        let mut b = db.session();
        db.run_as("agent-a", "BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
        db.run_as("agent-b", "BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
        db.run_as("agent-a", "UPDATE inventory SET qty = 111 WHERE id = 1;", &mut a);
        db.run_as("agent-b", "UPDATE inventory SET qty = 222 WHERE id = 1;", &mut b);
        sql_echo("agent-a", "MERGE;");
        let first = report(db.ok("MERGE;", &mut a));
        kv("first merge outcome", first.outcome.name());
        kv("qty on main after the first merge", db.qty(1));
        sql_echo("agent-b", "MERGE;");
        let second = report(db.ok("MERGE;", &mut b));
        kv("second merge outcome", second.outcome.name());
        kv("applied_to_target", second.applied_to_target);
        kv("writes DISCARDED by the policy", second.rows[0].discarded.len());
        for dw in &second.rows[0].discarded {
            kv("  discarded under policy", format!("{:?}", dw.policy));
        }
        kv("qty on main afterwards", db.qty(1));
        seen.push(second.outcome.name().to_string());
        c.that(second.outcome.lost_a_write(), "a lossy resolution reports ResolvedWithLoss");
        c.that(
            second.outcome.name() != "Clean",
            "and is NEVER reported as Clean — the agent is told a write was thrown away",
        );
        c.that(!second.rows[0].discarded.is_empty(), "the discarded write is named, not just counted");
    }

    println!();
    kv("distinct outcomes observed", format!("{:?}", seen));
    let unique: HashSet<&String> = seen.iter().collect();
    c.that(unique.len() == 4, "all four outcomes were produced by real merges, not enumerated");

    led.record(
        5,
        "MERGE reports Clean / Commuting / Conflict / ResolvedWithLoss",
        "agent SQL",
        c,
        "",
    );
}

// ---- criterion 6 --------------------------------------------------------------------------------

fn criterion_6(led: &mut Ledger) {
    criterion(
        6,
        "Two branches both doing `qty -= n` COMPOSE arithmetically instead of conflicting",
        "agent SQL",
    );
    let mut c = Checks::new();
    let mut db = Sql::new();
    db.seed();

    let start = db.qty(1);
    kv("starting qty on main", start);

    let mut a = db.session();
    let mut b = db.session();
    db.run_as("agent-a", "BEGIN AGENT SESSION AS 'picker-a' RUN 'r1';", &mut a);
    db.run_as("agent-b", "BEGIN AGENT SESSION AS 'picker-b' RUN 'r2';", &mut b);

    step("Both branches decrement the SAME cell, concurrently, by different amounts.");
    db.run_as("agent-a", "UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    db.run_as("agent-b", "UPDATE inventory SET qty = qty - 3 WHERE id = 1;", &mut b);

    kv("agent A's own view", one_int(&rows(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut a))));
    kv("agent B's own view", one_int(&rows(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b))));

    step("Merge both.");
    sql_echo("agent-a", "MERGE;");
    let first = report(db.ok("MERGE;", &mut a));
    let after_first = db.qty(1);
    kv("outcome", first.outcome.name());
    kv("qty on main", after_first);

    sql_echo("agent-b", "MERGE;");
    let second = report(db.ok("MERGE;", &mut b));
    let after_second = db.qty(1);
    kv("outcome", second.outcome.name());
    kv("qty on main", after_second);
    if let MergeOutcome::Commuting { composed } = &second.outcome {
        for op in composed {
            kv("composed op", format!("{:?}", op.kind));
        }
    }

    println!();
    kv("arithmetic", format!("{} - 5 - 3 = {}", start, after_second));
    note("12 is neither branch's own answer (A alone would give 15, B alone 17) — the two");
    note("decrements composed rather than one overwriting the other");

    c.that(second.applied_to_target, "the second merge published rather than conflicting");
    c.that(
        after_second == start - 8,
        format!("both decrements survive: {} - 5 - 3 = {}", start, after_second),
    );
    c.that(
        matches!(second.outcome, MergeOutcome::Commuting { .. }),
        "and the outcome names the composition explicitly",
    );

    step("Add is NOT idempotent: two identical decrements inside ONE task are -10, not -5.");
    note("this is the Cassandra counter trap — retries must not silently collapse");
    let mut db2 = Sql::new();
    db2.seed();
    let mut t = db2.session();
    db2.run_as("agent-t", "BEGIN AGENT SESSION AS 't' RUN 'r1';", &mut t);
    db2.run_as("agent-t", "UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut t);
    db2.run_as("agent-t", "UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut t);
    sql_echo("agent-t", "MERGE;");
    let r = report(db2.ok("MERGE;", &mut t));
    let q = db2.qty(1);
    kv("outcome", r.outcome.name());
    kv("qty on main", q);
    c.that(q == 10, "20 - 5 - 5 = 10, not 15");

    led.record(
        6,
        "Concurrent `qty -= n` compose arithmetically",
        "agent SQL",
        c,
        "",
    );
}

// ---- criterion 7 --------------------------------------------------------------------------------

fn criterion_7(led: &mut Ledger) {
    criterion(
        7,
        "A branch violating `qty >= 0` is rejected and THE VIOLATED PREDICATE is handed back",
        "agent SQL",
    );
    let mut c = Checks::new();
    let mut db = Sql::new();
    db.seed();

    kv("starting qty on main", db.qty(1));
    note("each branch takes 12, and each is individually legal: 20 >= 12");
    note("but composed they are 20 - 12 - 12 = -4, which the guard forbids");

    let mut a = db.session();
    let mut b = db.session();
    db.run_as("agent-a", "BEGIN AGENT SESSION AS 'picker-a' RUN 'r1';", &mut a);
    db.run_as("agent-b", "BEGIN AGENT SESSION AS 'picker-b' RUN 'r2';", &mut b);
    db.run_as(
        "agent-a",
        "UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 12;",
        &mut a,
    );
    db.run_as(
        "agent-b",
        "UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 12;",
        &mut b,
    );

    step("The first merge is fine.");
    sql_echo("agent-a", "MERGE;");
    let first = report(db.ok("MERGE;", &mut a));
    kv("outcome", first.outcome.name());
    kv("qty on main", db.qty(1));

    step("The second is re-checked against the MERGED state, and the guard now fails.");
    sql_echo("agent-b", "MERGE;");
    let second = report(db.ok("MERGE;", &mut b));
    kv("outcome", second.outcome.name());
    kv("applied_to_target", second.applied_to_target);

    let predicates = second.violated_predicates();
    println!();
    println!("    what the agent is handed back:");
    for p in &predicates {
        println!("        violated predicate: {}", p);
    }
    for conflict in second.outcome.conflicts() {
        kv("  conflict kind", format!("{:?}", conflict.kind));
    }
    println!();
    kv("qty on main after the rejected merge", db.qty(1));

    c.that(second.outcome.is_conflict(), "the merge is rejected");
    c.that(!second.applied_to_target, "nothing is published");
    c.that(db.qty(1) == 8, "the counter never went negative — main still reads 8");
    c.that(!predicates.is_empty(), "a violated predicate is returned, not just a failure flag");
    c.that(
        predicates.iter().any(|p| p.contains("qty >= 12")),
        "and it is the ACTUAL predicate text the agent wrote, so the retry can be informed",
    );

    step("The branch is still alive, so the agent can adjust and try again.");
    let alive = db.exec("DIFF;", &mut b);
    kv("DIFF on the rejected branch", if alive.is_ok() { "still works" } else { "failed" });
    c.that(alive.is_ok(), "a rejected merge does not destroy the agent's work");

    step("WHERE THE BOUNDARY ACTUALLY IS — measured, not assumed.");
    note("the criterion names the invariant `qty >= 0`. What the engine enforces is the guard the");
    note("agent WROTE, re-evaluated against merged state — not a declarative table constraint.");
    note("So the same scenario is re-run below with the WEAKER but more literal guard `qty >= 0`,");
    note("and whatever it does is printed as fact.");
    let mut weak = Sql::new();
    weak.seed();
    let mut wa = weak.session();
    let mut wb = weak.session();
    weak.run_as("agent-a", "BEGIN AGENT SESSION AS 'wa' RUN 'r1';", &mut wa);
    weak.run_as("agent-b", "BEGIN AGENT SESSION AS 'wb' RUN 'r2';", &mut wb);
    weak.run_as(
        "agent-a",
        "UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 0;",
        &mut wa,
    );
    weak.run_as(
        "agent-b",
        "UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 0;",
        &mut wb,
    );
    sql_echo("agent-a", "MERGE;");
    let w1 = report(weak.ok("MERGE;", &mut wa));
    sql_echo("agent-b", "MERGE;");
    let w2 = report(weak.ok("MERGE;", &mut wb));
    let final_qty = weak.qty(1);
    kv("first merge outcome", w1.outcome.name());
    kv("second merge outcome", w2.outcome.name());
    kv("second merge applied_to_target", w2.applied_to_target);
    kv("FINAL qty on main", final_qty);
    if final_qty < 0 {
        note("NEGATIVE. `qty >= 0` is a PRE-condition: it passes against the merged state (8 >= 0)");
        note("and the composed decrement then drives the value below the bound. The correct guard");
        note("for a bounded counter is `qty >= <the amount being taken>`, which is the case proven");
        note("above. This is a real limitation, recorded in DEMO.md: ferrobranch enforces");
        note("agent-supplied preconditions, NOT declarative CHECK constraints.");
    } else {
        note("The bound held even with the weak guard.");
    }
    c.that(
        w1.outcome.name() == "Clean",
        "the first weak-guard merge is unaffected — only the second is at issue",
    );
    // Deliberately NOT asserting the final value: this step exists to REPORT where the boundary
    // is. An assertion in either direction would turn a measurement into a claim.

    led.record(
        7,
        "Guard violation rejected with the violated predicate returned",
        "agent SQL",
        c,
        "the guard is the statement's own WHERE clause re-evaluated against merged state; there \
         are no declarative CHECK constraints, so a weak precondition does not protect the bound \
         — see the measured boundary case in the transcript",
    );
}

// ---- criterion 9 --------------------------------------------------------------------------------

fn criterion_9(led: &mut Ledger) {
    criterion(
        9,
        "Provenance: query which agent + run + model wrote a given row",
        "agent SQL + provenance",
    );
    let mut c = Checks::new();
    let mut db = Sql::new();
    db.seed();

    step("Part 1 — attribution through the agent-SQL surface.");
    let mut a = db.session();
    let mut b = db.session();
    let sa = match agent_out(db.run_as("agent-a", "BEGIN AGENT SESSION AS 'restock-agent' RUN 'run-42';", &mut a)) {
        AgentOutput::SessionStarted(s) => s,
        other => panic!("expected a session, got {}", other),
    };
    let sb = match agent_out(db.run_as("agent-b", "BEGIN AGENT SESSION AS 'auditor-agent' RUN 'run-99';", &mut b)) {
        AgentOutput::SessionStarted(s) => s,
        other => panic!("expected a session, got {}", other),
    };
    db.run_as("agent-a", "UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    db.run_as("agent-b", "UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut b);

    step("Ask the effect log which task touched a given row, then resolve that task's run.");
    for (row, want_agent) in [(RowId(1), "restock-agent"), (RowId(2), "auditor-agent")] {
        let mut answer: Option<RunEntity> = None;
        for branch in [sa.branch, sb.branch] {
            let frames = db.runtime.log().frames_for(branch, 0).unwrap();
            let touched = frames.iter().any(|f| f.ops.iter().any(|o| o.row == row));
            if touched {
                answer = db.runtime.run_of(branch);
            }
        }
        match &answer {
            Some(run) => {
                kv(&format!("who wrote row {}", row.0), run.describe());
                c.that(
                    run.agent_id == want_agent,
                    format!("row {} is attributed to {}", row.0, want_agent),
                );
            }
            None => c.that(false, format!("row {} has an attributable writer", row.0)),
        }
    }

    let model_at_sql = db.runtime.run_of(sa.branch).map(|r| r.model).unwrap_or_default();
    println!();
    kv("model recorded by the SQL surface", &model_at_sql);
    if model_at_sql == "unspecified" {
        note("the BEGIN AGENT SESSION grammar has no MODEL clause, so the agent and run halves of");
        note("criterion 9 are carried end-to-end from SQL but the model half is not — it is");
        note("recorded as the literal 'unspecified' rather than guessed at. Part 2 shows the model");
        note("carried for real one layer down, where the write path stamps it.");
    }
    c.that(
        !model_at_sql.is_empty(),
        "the model field is populated with something explicit rather than left blank",
    );

    step("Part 2 — per-row attribution in the provenance store, on real heap rows.");
    note("this is the layer the write path stamps: one u32 slot per version, resolved through a");
    note("page-local dictionary to one reified run entity");

    let store = MemProvenanceStore::new();
    let restock = store
        .intern(&RunEntity::new(
            ProvId::NONE,
            "restock-agent",
            "run-42",
            "claude-opus-5",
            "2026-05",
            [0xab; 32],
            1_700_000_000_000,
            BranchId::new(1, 0),
        ))
        .unwrap();
    let auditor = store
        .intern(&RunEntity::new(
            ProvId::NONE,
            "auditor-agent",
            "run-99",
            "claude-sonnet-4",
            "2026-02",
            [0xcd; 32],
            1_700_000_500_000,
            BranchId::new(2, 0),
        ))
        .unwrap();
    kv("interned run slot for restock-agent", format!("{}", restock));
    kv("interned run slot for auditor-agent", format!("{}", auditor));

    let rid1 = db.rid_of("inventory", 1);
    let rid2 = db.rid_of("inventory", 2);
    kv("physical slot of row 1", format!("page {} slot {}", rid1.page_id, rid1.slot_num));
    kv("physical slot of row 2", format!("page {} slot {}", rid2.page_id, rid2.slot_num));
    kv("begin_ts of row 1 from the real version header", db.heap("inventory").begin_ts(rid1).unwrap());

    store.stamp(rid1, restock).unwrap();
    store.stamp(rid2, auditor).unwrap();

    step("Now the query criterion 9 actually asks for.");
    let who1 = store.who_wrote(rid1).unwrap();
    let who2 = store.who_wrote(rid2).unwrap();
    println!();
    println!("    row 1 -> {}", who1.describe());
    kv("  agent_id", &who1.agent_id);
    kv("  run_id", &who1.run_id);
    kv("  model", format!("{}/{}", who1.model, who1.model_version));
    println!("    row 2 -> {}", who2.describe());
    kv("  agent_id", &who2.agent_id);
    kv("  run_id", &who2.run_id);
    kv("  model", format!("{}/{}", who2.model, who2.model_version));

    c.that(who1.agent_id == "restock-agent" && who1.run_id == "run-42", "row 1 names its agent and run");
    c.that(who1.model == "claude-opus-5" && who1.model_version == "2026-05", "row 1 names its model and version");
    c.that(who2.agent_id == "auditor-agent" && who2.model == "claude-sonnet-4", "row 2 names a DIFFERENT agent and model");
    c.that(store.attribute(rid1).unwrap() == restock, "attribution resolves to the interned slot");

    step("Attribution is interned: the dictionary tracks the number of RUNS, not of ROWS.");
    note("many more rows are now written and stamped, alternating between the SAME two runs.");
    note("if provenance were stored per row the dictionary would grow with the row count");
    let mut plain = db.session();
    let mut stamped_on_page = 2usize;
    for id in 10..40 {
        db.ok(&format!("INSERT INTO inventory VALUES ({}, {});", id, id), &mut plain);
        let rid = db.rid_of("inventory", id);
        if rid.page_id != rid1.page_id {
            continue;
        }
        store.stamp(rid, if id % 2 == 0 { restock } else { auditor }).unwrap();
        stamped_on_page += 1;
    }
    let dict_len = store.page_dictionary_len(rid1.page_id);
    let distinct_runs = 2usize;
    let literal = who1.literal_footprint();
    kv("rows stamped on page", format!("{} (page {})", stamped_on_page, rid1.page_id));
    kv("distinct runs among them", distinct_runs);
    kv("dictionary entries on that page", dict_len);
    kv("bytes per version actually stored", std::mem::size_of::<u32>());
    kv("bytes the actor tuple would cost if stored literally", literal);
    kv(
        "stored this way vs. literally, for these rows",
        format!(
            "{} B vs {} B",
            stamped_on_page * std::mem::size_of::<u32>(),
            stamped_on_page * literal
        ),
    );
    c.that(
        stamped_on_page > distinct_runs * 2,
        format!(
            "the page really does hold many more rows ({}) than runs ({})",
            stamped_on_page, distinct_runs
        ),
    );
    c.that(
        dict_len == distinct_runs,
        format!(
            "the dictionary holds {} entries for {} stamped rows — one per RUN, not per row",
            dict_len, stamped_on_page
        ),
    );
    c.that(
        literal > std::mem::size_of::<u32>() * 4,
        "the literal tuple is many times the interned slot, which is why interning is the design",
    );

    step("A row nobody claimed is reported as unattributed, never guessed.");
    let mut plain = db.session();
    db.ok("INSERT INTO inventory VALUES (77, 3);", &mut plain);
    let rid_unclaimed = db.rid_of("inventory", 77);
    kv("describe_row for an unstamped row", store.describe_row(rid_unclaimed));
    c.that(
        store.attribute(rid_unclaimed).unwrap() == ProvId::NONE,
        "an unattributed row reports ProvId::NONE rather than the nearest run",
    );

    led.record(
        9,
        "Provenance: which agent + run + model wrote a row",
        "agent SQL + prov",
        c,
        "agent and run are carried end-to-end from SQL; the MODEL is carried by the provenance \
         layer only, because BEGIN AGENT SESSION has no MODEL clause yet. Part 2 stamps rows \
         directly where the write path would call stamp()",
    );
}

// ---- criterion 10 -------------------------------------------------------------------------------

fn criterion_10(led: &mut Ledger) {
    criterion(
        10,
        "REVERT ... CASCADE uses retained read-sets to find a downstream dependent write",
        "agent SQL",
    );
    let mut c = Checks::new();
    let mut db = Sql::new();
    db.seed();

    step("Agent A changes row 1 and merges.");
    let mut a = db.session();
    db.run_as("agent-a", "BEGIN AGENT SESSION AS 'restock' RUN 'r1';", &mut a);
    db.run_as("agent-a", "UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    sql_echo("agent-a", "MERGE;");
    let first = report(db.ok("MERGE;", &mut a));
    kv("merge id", &first.merge_id);
    kv("qty of row 1", db.qty(1));

    step("Agent B READS what A wrote, then writes somewhere else on the strength of it.");
    note("the read is a point lookup, so the read-set is retained as exact version ids");
    let mut b = db.session();
    db.run_as("agent-b", "BEGIN AGENT SESSION AS 'auditor' RUN 'r2';", &mut b);
    sql_echo("agent-b", "SELECT qty FROM inventory WHERE id = 1;");
    let seen = one_int(&rows(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b)));
    kv("what B observed", seen);
    db.run_as("agent-b", "UPDATE inventory SET qty = qty + 2 WHERE id = 2;", &mut b);
    sql_echo("agent-b", "MERGE;");
    let second = report(db.ok("MERGE;", &mut b));
    kv("merge id", &second.merge_id);
    kv("qty of row 2", db.qty(2));

    step("Now revert A's merge. HALT is the default: the dependent is found and nothing moves.");
    let mut main = db.session();
    sql_echo("main", &format!("REVERT MERGE {};", first.merge_id));
    let plan = match agent_out(db.ok(&format!("REVERT MERGE {};", first.merge_id), &mut main)) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    };
    kv("plan.target", format!("{:?}", plan.target));
    kv("plan.mode", format!("{:?}", plan.mode));
    kv("plan.is_blocked()", plan.is_blocked());
    kv("plan.blocked_by (downstream dependents)", format!("{:?}", plan.blocked_by));
    kv("plan.cascade", format!("{:?}", plan.cascade));
    kv("qty of row 1 (unchanged)", db.qty(1));
    kv("qty of row 2 (unchanged)", db.qty(2));

    c.that(plan.is_blocked(), "the revert HALTS rather than silently discarding a dependent write");
    c.that(
        plan.blocked_by.len() == 1,
        "and names exactly the transaction that read the version being reverted",
    );
    c.that(plan.cascade.is_empty(), "a halted revert undoes nothing");
    c.that(db.qty(1) == 15 && db.qty(2) == 7, "no data moved");

    step("CASCADE is explicit, and undoes the dependent FIRST, then the target.");
    sql_echo("main", &format!("REVERT MERGE {} CASCADE;", first.merge_id));
    let plan = match agent_out(db.ok(&format!("REVERT MERGE {} CASCADE;", first.merge_id), &mut main)) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    };
    kv("plan.is_blocked()", plan.is_blocked());
    kv("plan.cascade (undone, in order)", format!("{:?}", plan.cascade));
    kv("qty of row 1", db.qty(1));
    kv("qty of row 2", db.qty(2));

    c.that(!plan.is_blocked(), "cascade proceeds");
    c.that(plan.cascade.len() == 1, "the downstream dependent is in the cascade set");
    c.that(
        db.qty(1) == 20 && db.qty(2) == 5,
        "BOTH the target write and the write that depended on it are undone",
    );

    step("Negative control — a merge nobody read reverts with no cascade at all.");
    let mut db2 = Sql::new();
    db2.seed();
    let mut s = db2.session();
    db2.run_as("agent-x", "BEGIN AGENT SESSION AS 'x' RUN 'r1';", &mut s);
    db2.run_as("agent-x", "UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut s);
    let r = report(db2.ok("MERGE;", &mut s));
    let mut m = db2.session();
    sql_echo("main", &format!("REVERT MERGE {};", r.merge_id));
    let plan = match agent_out(db2.ok(&format!("REVERT MERGE {};", r.merge_id), &mut m)) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    };
    kv("plan.is_blocked()", plan.is_blocked());
    kv("plan.cascade", format!("{:?}", plan.cascade));
    kv("qty of row 1", db2.qty(1));
    c.that(
        !plan.is_blocked() && plan.cascade.is_empty(),
        "no reader means no dependency, so the halt above was a real finding and not a constant",
    );

    step("Reverting a merge that never happened is an error, not a silent no-op.");
    let bad = db2.exec("REVERT MERGE m_99 CASCADE;", &mut m);
    kv("REVERT MERGE m_99", match &bad {
        Ok(_) => "succeeded (WRONG)".to_string(),
        Err(e) => format!("Err({})", e),
    });
    c.that(bad.is_err(), "an unknown merge id is refused");

    led.record(
        10,
        "REVERT ... CASCADE finds downstream dependents via read-sets",
        "agent SQL",
        c,
        "the dependency edge here comes from a point read inside an agent session; range reads \
         retain a predicate summary instead, which this demo does not exercise",
    );
}

// =================================================================================================

fn main() {
    println!("{}", RULE);
    println!("ferrobranch — end-to-end demonstration of the ten exit criteria");
    println!("{}", RULE);
    println!();
    println!("  Every number below is read out of the engine as it is printed. Each [ok] line is a");
    println!("  live comparison; the process exits non-zero if any fails or if fewer than ten");
    println!("  criteria report.");
    println!();
    println!("  THREE LAYERS, and they do not yet share row storage:");
    println!("    · page layer   — ArenaPageStore + CowTree + LogBranchCatalog + TwoTierReaper.");
    println!("                     Owns pages, so it answers the page-count criteria (1 and 8).");
    println!("    · agent SQL    — AgentRuntime behind the real scanner/parser/binder/executor.");
    println!("                     Owns statements, so it answers criteria 2-7 and 10.");
    println!("    · provenance   — MemProvenanceStore over real heap RecordIds; criterion 9.");
    println!();
    println!("  The SQL surface forks its branches through the SAME LogBranchCatalog the page");
    println!("  layer uses, so branch identity, generations and leases really are shared. Row");
    println!("  storage is NOT: rows written in an agent session live in that branch's in-memory");
    println!("  workspace, not on CoW pages. So criterion 8's 'pages return to baseline' is proven");
    println!("  for pages written through the CoW tree and is NOT transitively proven for rows");
    println!("  written through SQL. See DEMO.md.");

    let mut led = Ledger::new();
    criterion_1(&mut led);
    criterion_2(&mut led);
    criterion_3(&mut led);
    criterion_4(&mut led);
    criterion_5(&mut led);
    criterion_6(&mut led);
    criterion_7(&mut led);
    criterion_8(&mut led);
    criterion_9(&mut led);
    criterion_10(&mut led);
    led.finish();
}
