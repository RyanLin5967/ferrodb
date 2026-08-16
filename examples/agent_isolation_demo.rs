//! End-to-end demonstration of the ten exit criteria in DESIGN.md section 5.
//!
//! Run with:  `cargo run --example agent_isolation_demo`
//!
//! This prints evidence rather than asserting silently: every criterion shows the numbers,
//! structures and predicates it rests on, so a reader can disagree with the verdict. Each
//! criterion also *checks* itself, and the process exits non-zero if any criterion that claims
//! MET fails its own check — a fabricated pass cannot survive running the thing.
//!
//! **The demo is in two acts because the system is in two layers, and they are not yet wired to
//! each other.** This is the single most important thing to understand about what follows:
//!
//! - **Act I — the branch engine**, which owns pages. Criteria 1 and 8 are page-count claims and
//!   are demonstrated here, on real 4KB pages in a real file.
//! - **Act II — the agent SQL surface**, which owns statements. Criteria 2-7, 9 and 10 are
//!   demonstrated here, through the real scanner, parser, binder and executor.
//!
//! A row written by SQL in Act II does **not** live on a copy-on-write page from Act I. The SQL
//! surface keeps a branch's uncommitted rows in an in-memory per-branch workspace. Both layers
//! are real and both are tested; the seam between them is not closed. Act I therefore proves
//! that forking copies zero pages and that abandoned branches return their pages; it does not
//! prove that the rows in Act II are the things on those pages. DEMO.md says this again, at
//! length, because a demo that blurred it would be claiming a finished system.

use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::process::ExitCode;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::{ChangeSet, MergeReport};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline, ARENA_EXTENT_PAGES};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::{stamp_checksum, CowPageLinks, CowTree, PageStore, PageType, PAGE_HEADER_SIZE};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::ids::{ColId, RowId};
use ferrodb::tel::merge::{MergeOutcome, MergePolicy};

// =================================================================================================
// transcript plumbing
// =================================================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// Demonstrated by the evidence printed directly above it.
    Met,
    /// The criterion's core claim is demonstrated, but something it names is not.
    Partial,
    /// Not demonstrated. The reason is printed.
    NotMet,
}

impl Verdict {
    fn tag(&self) -> &'static str {
        match self {
            Verdict::Met => "MET",
            Verdict::Partial => "PARTIAL",
            Verdict::NotMet => "NOT MET",
        }
    }
}

struct Ledger {
    rows: Vec<(u8, &'static str, Verdict, String)>,
    /// Set when a criterion's self-check fails, which is different from an honest NOT MET.
    broken: Vec<String>,
}

impl Ledger {
    fn new() -> Self {
        Ledger { rows: Vec::new(), broken: Vec::new() }
    }

    fn record(&mut self, n: u8, title: &'static str, v: Verdict, note: impl Into<String>) {
        println!("\n  VERDICT {}: {}", n, v.tag());
        let note = note.into();
        if !note.is_empty() {
            println!("  {}", note);
        }
        self.rows.push((n, title, v, note));
    }

    /// A claim the demo makes about itself. Failing one is a bug in the system or in the demo,
    /// and must not be reportable as a pass.
    fn check(&mut self, criterion: u8, claim: &str, holds: bool) {
        if !holds {
            self.broken.push(format!("criterion {}: {}", criterion, claim));
            println!("  !! SELF-CHECK FAILED: {}", claim);
        }
    }

    fn summary(&self) {
        rule('=');
        println!("SUMMARY — ten exit criteria (DESIGN.md section 5)");
        rule('=');
        for (n, title, v, _) in &self.rows {
            println!("  {:>2}. {:<70} {}", n, title, v.tag());
        }
        let met = self.rows.iter().filter(|r| r.2 == Verdict::Met).count();
        let partial = self.rows.iter().filter(|r| r.2 == Verdict::Partial).count();
        let unmet = self.rows.iter().filter(|r| r.2 == Verdict::NotMet).count();
        println!("\n  {} MET, {} PARTIAL, {} NOT MET, of {} criteria", met, partial, unmet, self.rows.len());
        if !self.broken.is_empty() {
            println!("\n  SELF-CHECKS FAILED ({}):", self.broken.len());
            for b in &self.broken {
                println!("    - {}", b);
            }
        }
        println!("\n  Read the 'What this does not do yet' section of DEMO.md before quoting any");
        println!("  of the above. Two boundaries in particular travel with these numbers, and a");
        println!("  reader who stops at the table above will not have them:");
        println!("    · SQL STATEMENTS still stage into an in-memory workspace, which bounds what");
        println!("      a MET in Act II can mean about pages. A page-backed row path does now");
        println!("      exist (AgentRuntime::with_storage), and criteria 1 and 8 are measured on");
        println!("      it in tests/integration_zero_copy_fork.rs; statements do not use it yet.");
        println!("    · Criterion 7 holds for a guard naming the amount taken (`qty >= 12`).");
        println!("      Written as the invariant (`qty >= 0`) the same case is not refused and the");
        println!("      counter reaches -4 — measured above, not argued.");
    }
}

fn rule(c: char) {
    println!("{}", std::iter::repeat(c).take(96).collect::<String>());
}

fn act(title: &str) {
    println!();
    rule('=');
    println!("{}", title);
    rule('=');
}

fn criterion(n: u8, title: &str) {
    println!();
    rule('-');
    println!("CRITERION {} — {}", n, title);
    rule('-');
}

fn note(s: &str) {
    println!("  · {}", s);
}

fn sql_echo(s: &str) {
    println!("    sql> {}", s);
}

// =================================================================================================
// ACT I — the branch engine. Pages are real here.
// =================================================================================================

struct PageEnv {
    catalog: Arc<LogBranchCatalog>,
    store: Arc<ArenaPageStore>,
    path: std::path::PathBuf,
}

impl Drop for PageEnv {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn page_env(tag: &str) -> PageEnv {
    let path = std::env::temp_dir().join(format!("ferro-demo-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .expect("open demo database file");
    let dm = Arc::new(DiskManager::new(file).expect("disk manager"));
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    // The arena region must start at or above everything the legacy bitmap allocator owns,
    // otherwise both allocators hand out the same pages. Ask the bitmap, don't guess.
    let base = pool.disk_manager.high_water().expect("high water mark");
    let store = Arc::new(
        ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), base).expect("arena store"),
    );
    println!(
        "  page store ready: 4KB pages, arena extents of {} pages, arena floor at page {}",
        ARENA_EXTENT_PAGES, base
    );
    PageEnv { catalog, store, path }
}

fn key(i: u32) -> Vec<u8> {
    format!("k{:06}", i).into_bytes()
}
fn val(i: u32) -> Vec<u8> {
    format!("v{:06}", i).into_bytes()
}

/// Criterion 1 — forking an agent session copies zero data pages.
fn criterion_1_fork_copies_zero_pages(led: &mut Ledger) {
    criterion(1, "BEGIN AGENT SESSION forks a branch copying ZERO data pages");
    let env = page_env("c1");
    let tree = CowTree::new(env.store.clone() as Arc<dyn PageStore>);

    // Build a genuinely multi-level tree on trunk, so there is something substantial to copy.
    // "Zero copied" is only interesting when the alternative is expensive.
    note("populating trunk with a real multi-level B+tree, so a copying fork would be costly");
    let e0 = env.catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, e0).expect("create trunk tree");
    const N: u32 = 400;
    for i in 0..N {
        let e = env.catalog.next_epoch();
        root = tree
            .insert(root, BranchId::TRUNK, e, &key(i), &val(i))
            .expect("insert into trunk");
    }
    env.catalog.set_root(BranchId::TRUNK, root).expect("publish trunk root");

    let tree_pages = tree.walk_pages(root).expect("walk trunk tree").len();
    let live_before = env.store.live_page_count().expect("live count");
    let reserved_before = env.store.reserved_page_count();

    println!("\n  BEFORE FORK");
    println!("    rows in trunk tree ................ {}", N);
    println!("    pages reachable from trunk root ... {}", tree_pages);
    println!("    live (allocated) pages ............ {}", live_before);
    println!("    reserved (extent) pages ........... {}", reserved_before);
    println!("    trunk root page ................... {}", root);

    // The fork itself. Worth being precise about what is shared with Act II: this is not a
    // re-enactment of what the SQL surface does, it is the same call. AgentRuntime::begin_session
    // calls BranchCatalog::fork, and the runtime's default catalog is a LogBranchCatalog — the
    // implementation being measured here.
    sql_echo("BEGIN AGENT SESSION AS 'pricing-agent' RUN 'r_8fk2';   -- at the page layer:");
    note("catalog.fork(TRUNK, lease) — one metadata record, one epoch appended to the parent");
    let child = env
        .catalog
        .fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000))
        .expect("fork");

    let live_after = env.store.live_page_count().expect("live count");
    let reserved_after = env.store.reserved_page_count();

    println!("\n  AFTER FORK");
    println!("    child branch ...................... {}", child.branch_id);
    println!("    child root page ................... {}", child.root_page_id);
    println!("    live (allocated) pages ............ {}", live_after);
    println!("    reserved (extent) pages ........... {}", reserved_after);
    println!(
        "\n    PAGES COPIED BY THE FORK .......... {}   <-- the criterion",
        live_after - live_before
    );

    let copied_zero = live_after == live_before && reserved_after == reserved_before;
    let root_shared = child.root_page_id == root;
    led.check(1, "fork allocated no pages", copied_zero);
    led.check(1, "the child's root IS the parent's root", root_shared);

    // And the child reads the parent's data by ordinary descent — no parent-chain walk.
    note("the child now reads parent data by ordinary B+tree descent from that shared root");
    let mut found = 0usize;
    for i in [0u32, 199, 399] {
        let got = tree.get(child.root_page_id, &key(i)).expect("read from child");
        let ok = got.as_deref() == Some(val(i).as_slice());
        println!(
            "      child.get({}) -> {}  [{}]",
            String::from_utf8_lossy(&key(i)),
            got.as_deref().map(|v| String::from_utf8_lossy(v).into_owned()).unwrap_or_else(|| "<missing>".into()),
            if ok { "ok" } else { "WRONG" }
        );
        if ok {
            found += 1;
        }
    }
    led.check(1, "the child reads parent rows through the shared root", found == 3);

    led.record(
        1,
        "Fork copies zero data pages",
        if copied_zero && root_shared && found == 3 { Verdict::Met } else { Verdict::NotMet },
        format!(
            "{} pages were reachable from the trunk root; the fork copied {} of them and the child's \
root page id is the parent's. Measured with PageStore::live_page_count on a real file.",
            tree_pages,
            live_after - live_before
        ),
    );
}

/// Criterion 8 — THE THESIS. Abandoned branches are reaped with no client cooperation.
fn criterion_8_lease_reaping(led: &mut Ledger) {
    criterion(8, "*** THE THESIS *** branches abandoned with NO client cooperation are reaped");
    let env = page_env("c8");
    let reaper = TwoTierReaper::new(Arc::clone(&env.catalog), Arc::clone(&env.store))
        .with_links(Arc::new(CowPageLinks));

    // The baseline must NOT be an empty database. "Page count returns to baseline" is trivially
    // satisfied by freeing everything, so trunk is given real data first: the reaper then has to
    // free exactly the abandoned branches' pages and leave trunk's alone, and the demo checks
    // trunk is still readable afterwards rather than only checking a number.
    let tree = CowTree::new(env.store.clone() as Arc<dyn PageStore>);
    note("seeding trunk with real data FIRST, so 'returns to baseline' cannot be satisfied");
    note("by simply freeing everything — the reaper has to free the right pages, not all pages");
    let e0 = env.catalog.next_epoch();
    let mut trunk_root = tree.create(BranchId::TRUNK, e0).expect("create trunk tree");
    const TRUNK_ROWS: u32 = 400;
    for i in 0..TRUNK_ROWS {
        let e = env.catalog.next_epoch();
        trunk_root = tree
            .insert(trunk_root, BranchId::TRUNK, e, &key(i), &val(i))
            .expect("insert into trunk");
    }
    env.catalog.set_root(BranchId::TRUNK, trunk_root).expect("publish trunk root");

    let baseline_live = env.store.live_page_count().expect("live");
    let baseline_reserved = env.store.reserved_page_count();
    let baseline_branches = env.catalog.live_count();

    println!("\n  BEFORE — trunk holds {} rows; no agent has run yet", TRUNK_ROWS);
    println!("    live pages ........................ {}", baseline_live);
    println!("    reserved (extent) pages ........... {}", baseline_reserved);
    println!("    live branches ..................... {}", baseline_branches);

    const AGENTS: usize = 32;
    const PAGES_EACH: u32 = 7;
    const LEASE_MS: u64 = 10_000;

    note(&format!(
        "starting {} agent tasks; each takes a lease of {}ms and writes {} novel pages",
        AGENTS, LEASE_MS, PAGES_EACH
    ));
    let mut abandoned = Vec::new();
    for _ in 0..AGENTS {
        let rec = env
            .catalog
            .fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS))
            .expect("fork agent branch");
        let arena = env.store.arena_for(rec.branch_id).expect("arena");
        let epoch = env.catalog.next_epoch();
        for i in 0..PAGES_EACH {
            let p = env
                .store
                .alloc_in_arena(arena, PageType::BTreeLeaf, epoch)
                .expect("alloc");
            let handle = env.store.read_page(p).expect("read back");
            let mut frame = handle.write();
            frame.data[PAGE_HEADER_SIZE] = (i & 0xff) as u8;
            stamp_checksum(&mut frame.data);
        }
        abandoned.push(rec.branch_id);
    }

    let during_live = env.store.live_page_count().expect("live");
    let during_reserved = env.store.reserved_page_count();
    println!("\n  DURING — {} agent branches alive, all of them having written", AGENTS);
    println!(
        "    live pages ........................ {}   (+{} over baseline)",
        during_live,
        during_live - baseline_live
    );
    println!(
        "    reserved (extent) pages ........... {}   (+{} over baseline)",
        during_reserved,
        during_reserved - baseline_reserved
    );
    println!("    live branches ..................... {}", env.catalog.live_count());
    led.check(
        8,
        "the abandoned branches really did allocate pages",
        during_live == baseline_live + AGENTS as u32 * PAGES_EACH,
    );

    println!("\n  ABANDONMENT — this is the part that matters:");
    note("no client calls close, commit, abort, rollback, free or ABANDON");
    note("the handles are simply dropped, exactly as if 32 agent processes were killed");
    // `abandoned` is kept only so the reap can be checked against it afterwards. Nothing in this
    // demo ever calls anything on those branches again before the reaper runs.

    // FIRST: prove the lease is load-bearing. A reaper that simply frees every branch it is
    // pointed at would also make the numbers below return to baseline, and would be catastrophic.
    // So run the identical scan while the leases are still valid and show it takes nothing.
    println!("\n  CONTROL — the same scan, run BEFORE the leases expire:");
    let early = reaper
        .reap_expired(LeaseDeadline::now_millis())
        .expect("early lease scan");
    let control_live = env.store.live_page_count().expect("live");
    println!("    branches reaped ................... {}  (must be 0)", early.len());
    println!("    live pages ........................ {}  (unchanged from {})", control_live, during_live);
    note("so the reaper is honouring the deadline, not simply freeing whatever it is shown");
    led.check(
        8,
        "a scan before expiry reaps nothing — the lease, not the scan, is what frees pages",
        early.is_empty() && control_live == during_live,
    );

    // THEN advance the clock past every lease. The reaper takes `now_millis` explicitly so the
    // demo does not have to sleep through a real lease. The deadlines are real and really
    // compared; only the clock reading is supplied.
    let now = LeaseDeadline::now_millis() + 10 * LEASE_MS;
    note(&format!(
        "background lease scan runs again with the clock at now+{}ms, past every lease",
        10 * LEASE_MS
    ));
    let reaped = reaper.reap_expired(now).expect("lease scan");

    let after_live = env.store.live_page_count().expect("live");
    let after_reserved = env.store.reserved_page_count();
    println!("\n  AFTER — the lease scan has run");
    println!("    branches reaped ................... {} of {}", reaped.len(), AGENTS);
    println!(
        "    live pages ........................ {}   (baseline was {})",
        after_live, baseline_live
    );
    println!(
        "    reserved (extent) pages ........... {}   (baseline was {})",
        after_reserved, baseline_reserved
    );
    println!("    live branches ..................... {}", env.catalog.live_count());

    let all_reaped = reaped.len() == AGENTS && abandoned.iter().all(|b| reaped.contains(b));
    let live_back = after_live == baseline_live;
    let reserved_back = after_reserved == baseline_reserved;
    led.check(8, "every abandoned branch was reaped", all_reaped);
    led.check(8, "allocated page count returned to baseline", live_back);
    led.check(8, "extents went back to the free space map, not merely stopped growing", reserved_back);

    // Reading a reaped branch must be a hard error, never stale data.
    let stale = env.catalog.get(abandoned[0]);
    println!(
        "\n    reading a reaped branch ({}) -> {}",
        abandoned[0],
        match &stale {
            Ok(_) => "RETURNED DATA (wrong)".to_string(),
            Err(e) => format!("hard error: {}", e),
        }
    );
    led.check(8, "a reaped branch id is a hard error, not stale data", stale.is_err());

    // The number returning to baseline is necessary but not sufficient: freeing the whole file
    // would also do that. Trunk's data must still be there and still be readable.
    println!("\n    trunk survived the reap? re-reading it through the same root:");
    let mut intact = 0usize;
    for i in [0u32, 199, 399] {
        let got = tree.get(trunk_root, &key(i)).expect("read trunk after reap");
        let ok = got.as_deref() == Some(val(i).as_slice());
        println!(
            "      trunk.get({}) -> {}  [{}]",
            String::from_utf8_lossy(&key(i)),
            got.as_deref()
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_else(|| "<missing>".into()),
            if ok { "ok" } else { "WRONG" }
        );
        if ok {
            intact += 1;
        }
    }
    led.check(
        8,
        "the reaper freed the abandoned branches' pages and left trunk's data intact",
        intact == 3 && after_live == baseline_live && baseline_live > 0,
    );

    led.record(
        8,
        "Abandoned branches reaped on lease expiry, pages return to baseline",
        if all_reaped && live_back && reserved_back && stale.is_err() && intact == 3 {
            Verdict::Met
        } else {
            Verdict::NotMet
        },
        format!(
            "{} branches abandoned with zero client cooperation; all {} reaped by the lease scan; \
live pages {} -> {} -> {} and reserved pages {} -> {} -> {} (before/during/after). The baseline \
is non-zero and trunk's {} rows were still readable afterwards, so the count returned by freeing \
the right pages rather than by freeing everything.",
            AGENTS, reaped.len(), baseline_live, during_live, after_live,
            baseline_reserved, during_reserved, after_reserved, TRUNK_ROWS
        ),
    );
}

// =================================================================================================
// ACT II — the agent SQL surface. Statements are real here; pages are not involved.
// =================================================================================================

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<ferrodb::wal::txn::TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("agent.db");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("open db");
        let bp = Arc::new(BufferPoolManager::new(Arc::new(
            DiskManager::new(file).expect("disk manager"),
        )));
        let catalog = Catalog::create(bp.clone()).expect("catalog");
        let wal = Arc::new(
            ferrodb::wal::log::WalManager::new(dir.path().join("agent.wal")).expect("wal"),
        );
        let txn = Arc::new(ferrodb::wal::txn::TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
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
        if stmts.len() != 1 {
            return Err(FerroError::SqlParseError(format!("expected one statement: {}", sql)));
        }
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    /// Run and echo. Panics on error, so the transcript can never show a step that did not run.
    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        sql_echo(sql);
        match self.exec(sql, s) {
            Ok(o) => o,
            Err(e) => panic!("{} failed: {}", sql, e),
        }
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 5);", &mut s);
    }

    fn qty(&mut self, id: i32) -> i32 {
        let mut s = self.session();
        let sql = format!("SELECT qty FROM inventory WHERE id = {};", id);
        match self.exec(&sql, &mut s).expect("read qty") {
            Outcome::Rows(r) if !r.is_empty() => match r[0][0] {
                Value::Integer(i) => i,
                ref v => panic!("qty is not an integer: {:?}", v),
            },
            _ => panic!("row {} missing", id),
        }
    }
}

fn rows_of(o: Outcome) -> Vec<Vec<Value>> {
    match o {
        Outcome::Rows(r) => r,
        _ => panic!("expected rows"),
    }
}

fn agent_out(o: Outcome) -> AgentOutput {
    match o {
        Outcome::Agent(a) => a,
        _ => panic!("expected an agent output"),
    }
}

fn changeset_of(o: Outcome) -> ChangeSet {
    match agent_out(o) {
        AgentOutput::Diff(d) => d,
        other => panic!("expected a changeset, got {}", other),
    }
}

fn report_of(o: Outcome) -> MergeReport {
    match agent_out(o) {
        AgentOutput::Merge(m) => m,
        other => panic!("expected a merge report, got {}", other),
    }
}

fn ints(rows: &[Vec<Value>]) -> Vec<i32> {
    let mut v: Vec<i32> = rows
        .iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            ref o => panic!("not an integer: {:?}", o),
        })
        .collect();
    v.sort();
    v
}

/// Criterion 2 — branch writes are invisible to main and to siblings until merge.
fn criterion_2_isolation(led: &mut Ledger) {
    criterion(2, "Branch writes are invisible to main and to sibling branches until merge");
    let mut db = Db::new();
    db.seed();
    let (mut a, mut b, mut main) = (db.session(), db.session(), db.session());

    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'agent-b' RUN 'r2';", &mut b);
    println!("    (two sibling agent branches now open on the same table)");
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);

    let seen_a = rows_of(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut a));
    let seen_main = rows_of(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut main));
    let seen_b = rows_of(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b));

    println!("\n    row 1 qty, started at 20, agent-a applied -5:");
    println!("      as agent-a (the writer) sees it .... {:?}", seen_a[0][0]);
    println!("      as main sees it .................... {:?}", seen_main[0][0]);
    println!("      as agent-b (a sibling) sees it ..... {:?}", seen_b[0][0]);

    let ok = seen_a[0][0] == Value::Integer(15)
        && seen_main[0][0] == Value::Integer(20)
        && seen_b[0][0] == Value::Integer(20);
    led.check(2, "writer sees 15 while main and sibling both still see 20", ok);

    // And after merge it becomes visible — otherwise "until merge" is unproven.
    let r = report_of(db.ok("MERGE;", &mut a));
    let after_main = rows_of(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut main));
    println!("\n    after agent-a merges ({}):", r.outcome.name());
    println!("      as main sees it .................... {:?}", after_main[0][0]);
    let became_visible = after_main[0][0] == Value::Integer(15);
    led.check(2, "the write becomes visible to main on merge", became_visible);

    led.record(
        2,
        "Branch writes invisible to main and siblings until merge",
        if ok && became_visible { Verdict::Met } else { Verdict::NotMet },
        "Isolation is at the row level in the SQL surface's per-branch workspace, not at the page \
level — see 'What this does not do yet' in DEMO.md.",
    );
}

/// Criterion 3 — SELECT ... AS OF BRANCH reads another branch's *uncommitted* state.
fn criterion_3_as_of_branch(led: &mut Ledger) {
    criterion(3, "SELECT ... AS OF BRANCH reads another branch's UNCOMMITTED state");
    let mut db = Db::new();
    db.seed();
    let (mut a, mut observer) = (db.session(), db.session());

    let started = agent_out(db.ok("BEGIN AGENT SESSION AS 'restock' RUN 'r1';", &mut a));
    let branch_name = match started {
        AgentOutput::SessionStarted(s) => s.branch_name,
        other => panic!("expected a session, got {}", other),
    };
    db.ok("UPDATE inventory SET qty = qty + 30 WHERE id = 1;", &mut a);
    note("that UPDATE has not been merged and never will be, in this scenario");

    let sql = format!("SELECT qty FROM inventory AS OF BRANCH {};", branch_name);
    let seen = ints(&rows_of(db.ok(&sql, &mut observer)));
    let plain = ints(&rows_of(db.ok("SELECT qty FROM inventory;", &mut observer)));

    println!("\n    a DIFFERENT connection, reading {}:", branch_name);
    println!("      AS OF BRANCH {} ..... {:?}", branch_name, seen);
    println!("      without AS OF (main) . {:?}", plain);

    let ok = seen == vec![5, 50] && plain == vec![5, 20];
    led.check(3, "the observer sees 50 on the branch and 20 on main", ok);

    led.record(
        3,
        "AS OF BRANCH reads another branch's uncommitted state",
        if ok { Verdict::Met } else { Verdict::NotMet },
        "A second connection read a value that exists only in an open agent task's workspace.",
    );
}

/// Criterion 4 — DIFF returns a structured changeset.
fn criterion_4_diff(led: &mut Ledger) {
    criterion(4, "DIFF returns a structured changeset");
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'pricing' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE qty >= 5 AND id = 1;", &mut a);
    db.ok("INSERT INTO inventory VALUES (4, 1);", &mut a);

    let d = changeset_of(db.ok("DIFF;", &mut a));
    println!("\n    the changeset is data, not rendered text. Field by field:");
    for (i, r) in d.rows.iter().enumerate() {
        println!("      [{}] table={} kind={} outcome={}", i, r.table, r.kind, r.outcome);
        println!("          before ..... {:?}", r.before);
        println!("          after ...... {:?}", r.after);
        println!("          ops ........ {:?}", r.ops.iter().map(|o| &o.kind).collect::<Vec<_>>());
        println!(
            "          witness .... {:?}",
            r.ops.iter().map(|o| &o.witness).collect::<Vec<_>>()
        );
        println!(
            "          guards ..... {:?}",
            r.guards.iter().map(|g| g.violated_predicate()).collect::<Vec<_>>()
        );
    }
    note("note the op is the ALGEBRA ELEMENT (Add(-5)), not a before/after pair —");
    note("that is what lets criterion 6 compose instead of conflicting");

    let has_two = d.rows.len() == 2;
    let has_add = d.rows.iter().any(|r| {
        r.ops.iter().any(|o| format!("{:?}", o.kind).contains("Add"))
    });
    let has_guard = d
        .rows
        .iter()
        .any(|r| r.guards.iter().any(|g| g.violated_predicate().contains("qty >= 5")));
    led.check(4, "the changeset has one row per changed row", has_two);
    led.check(4, "the update is recorded as Add, not as an opaque assignment", has_add);
    led.check(4, "the guard that admitted the write is retained verbatim", has_guard);

    led.record(
        4,
        "DIFF returns a structured changeset",
        if has_two && has_add && has_guard { Verdict::Met } else { Verdict::NotMet },
        "Typed fields: table, kind, before, after, ops (with witness), guards, outcome.",
    );
}

/// Criteria 5 and 6 — all four merge outcomes, and arithmetic composition.
fn criteria_5_and_6_merge_outcomes(led: &mut Ledger) {
    criterion(5, "MERGE reports CLEAN / COMMUTING / CONFLICT / RESOLVED-WITH-LOSS");
    let mut seen: Vec<String> = Vec::new();

    // --- Clean: one branch, nothing concurrent ---------------------------------------------
    println!("\n  (a) CLEAN — a solo merge, main untouched since the fork");
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    let r = report_of(db.ok("MERGE;", &mut a));
    println!("      outcome = {}   applied={}   qty(1) = {}", r.outcome.name(), r.applied_to_target, db.qty(1));
    seen.push(r.outcome.name().to_string());
    led.check(5, "a solo merge reports Clean", r.outcome == MergeOutcome::Clean);

    // --- Commuting: this is also criterion 6 ------------------------------------------------
    println!("\n  (b) COMMUTING — both branches wrote, and the ops compose");
    let mut db = Db::new();
    db.seed();
    let (mut a, mut b) = (db.session(), db.session());
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 3 WHERE id = 1;", &mut b);
    let first = report_of(db.ok("MERGE;", &mut a));
    println!("      after a merges: outcome={}  qty(1) = {}", first.outcome.name(), db.qty(1));
    let second = report_of(db.ok("MERGE;", &mut b));
    let final_qty = db.qty(1);
    println!("      after b merges: outcome={}  qty(1) = {}", second.outcome.name(), final_qty);
    seen.push(second.outcome.name().to_string());
    let commuting = matches!(second.outcome, MergeOutcome::Commuting { .. });
    led.check(5, "concurrent Adds report Commuting", commuting);

    // --- Conflict ---------------------------------------------------------------------------
    println!("\n  (c) CONFLICT — two concurrent assignments, no declared policy");
    let mut db = Db::new();
    db.seed();
    let (mut a, mut b) = (db.session(), db.session());
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    db.ok("UPDATE inventory SET qty = 1 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = 2 WHERE id = 1;", &mut b);
    report_of(db.ok("MERGE;", &mut a));
    let second = report_of(db.ok("MERGE;", &mut b));
    println!(
        "      outcome = {}   applied={}   qty(1) = {} (the first writer's value, kept)",
        second.outcome.name(),
        second.applied_to_target,
        db.qty(1)
    );
    note("AntidoteSQL's default, and DESIGN.md's: an undeclared column FORBIDS concurrent writes");
    note("rather than silently picking one. The branch stays alive so the agent can retry.");
    seen.push(second.outcome.name().to_string());
    led.check(5, "concurrent assignments conflict rather than picking a winner", second.outcome.is_conflict());
    led.check(5, "a conflicting merge publishes nothing", !second.applied_to_target);

    // --- ResolvedWithLoss --------------------------------------------------------------------
    println!("\n  (d) RESOLVED-WITH-LOSS — the same shape, but the column declares LWW");
    let mut db = Db::new();
    db.seed();
    db.runtime.set_policy("inventory", ColId(1), MergePolicy::Lww);
    note("policy for inventory.qty set to LWW");
    let (mut a, mut b) = (db.session(), db.session());
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    db.ok("UPDATE inventory SET qty = 111 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = 222 WHERE id = 1;", &mut b);
    report_of(db.ok("MERGE;", &mut a));
    let second = report_of(db.ok("MERGE;", &mut b));
    println!(
        "      outcome = {}   applied={}   qty(1) = {}",
        second.outcome.name(),
        second.applied_to_target,
        db.qty(1)
    );
    println!("      writes DISCARDED and reported .... {}", second.rows[0].discarded.len());
    for d in &second.rows[0].discarded {
        println!("        discarded by policy {:?}", d.policy);
    }
    note("this is the outcome that must never be reported as Clean: the merge succeeded");
    note("*while throwing away* agent-a's write, and the agent is told so.");
    seen.push(second.outcome.name().to_string());
    led.check(5, "an LWW resolution reports loss", second.outcome.lost_a_write());
    led.check(5, "a lossy resolution is not reported as Clean", second.outcome.name() != "Clean");
    led.check(5, "the discarded write is itemised", second.rows[0].discarded.len() == 1);

    println!("\n    outcomes observed: {:?}", seen);
    let all_four = seen.iter().any(|s| s == "Clean")
        && seen.iter().any(|s| s.contains("Commuting"))
        && seen.iter().any(|s| s.contains("Conflict"))
        && seen.iter().any(|s| s.contains("Loss"));
    led.check(5, "all four outcomes were produced by real merges", all_four);

    led.record(
        5,
        "MERGE reports all four outcomes",
        if all_four { Verdict::Met } else { Verdict::NotMet },
        format!("Observed: {:?}", seen),
    );

    // --- criterion 6, called out on its own ---------------------------------------------------
    criterion(6, "Two branches both doing `qty -= n` COMPOSE arithmetically");
    println!("    from scenario (b) above:");
    println!("      base at fork ............ 20");
    println!("      agent-a applied ......... qty = qty - 5   (its own answer would be 15)");
    println!("      agent-b applied ......... qty = qty - 3   (its own answer would be 17)");
    println!("      last-writer-wins would give .......... 17");
    println!("      merged result ........................ {}", final_qty);
    println!("      20 - 5 - 3 = {}   <-- neither branch's own answer", 20 - 5 - 3);
    let composed = final_qty == 12;
    led.check(6, "the two decrements composed to 12", composed);
    led.record(
        6,
        "Concurrent decrements compose arithmetically",
        if composed && commuting { Verdict::Met } else { Verdict::NotMet },
        "Composition happens because the log stored Add(-5) and Add(-3) as algebra elements, not \
as before/after images. Reported as Commuting, not Clean, because both sides wrote.",
    );
}

/// Criterion 7 — a guard violation is rejected and the predicate is handed back.
fn criterion_7_guard(led: &mut Ledger) {
    // The guard below is `qty >= 12`, not `qty >= 0`. That is not a cosmetic choice and the
    // criterion must not claim otherwise: the `qty >= 0` phrasing is the one this system CANNOT
    // hold, and it is measured at the end of this function.
    criterion(7, "A branch violating its own guard is rejected and THE VIOLATED PREDICATE returned");
    let mut db = Db::new();
    db.seed();
    let (mut a, mut b) = (db.session(), db.session());
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    note("row 1 starts at qty=20. Each agent takes 12, and each guard holds on its own branch.");
    db.ok("UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 12;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 12;", &mut b);

    let first = report_of(db.ok("MERGE;", &mut a));
    println!("\n      agent-a merges: {}  qty(1) = {}", first.outcome.name(), db.qty(1));
    note("now compose: 20 - 12 - 12 = -4. The guard is re-evaluated against the MERGED state.");
    let second = report_of(db.ok("MERGE;", &mut b));
    println!("      agent-b merges: {}  applied={}", second.outcome.name(), second.applied_to_target);

    let predicates = second.violated_predicates();
    println!("\n    PREDICATES HANDED BACK TO THE AGENT:");
    for p in &predicates {
        println!("      {}", p);
    }
    println!("\n      qty(1) after the rejected merge = {}  (never went negative)", db.qty(1));

    let conflicted = second.outcome.is_conflict();
    let got_predicate = predicates.len() == 1 && predicates[0].contains("qty >= 12");
    let nothing_published = !second.applied_to_target && db.qty(1) == 8;
    led.check(7, "the merge is rejected", conflicted);
    led.check(7, "the violated predicate itself comes back, not just a failure code", got_predicate);
    led.check(7, "nothing was published, so the counter never went negative", nothing_published);
    note("the agent can now retry with real feedback rather than guessing why it failed");

    // The boundary. Same scenario, but with the guard written as the INVARIANT rather than as the
    // amount being taken — which is how almost anyone would write "never let stock go below zero".
    println!("\n    THE BOUNDARY — the same case with the guard written as `qty >= 0`:");
    let mut db2 = Db::new();
    db2.seed();
    let (mut c, mut d) = (db2.session(), db2.session());
    db2.ok("BEGIN AGENT SESSION AS 'c' RUN 'r3';", &mut c);
    db2.ok("BEGIN AGENT SESSION AS 'd' RUN 'r4';", &mut d);
    db2.ok("UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 0;", &mut c);
    db2.ok("UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 0;", &mut d);
    let m1 = report_of(db2.ok("MERGE;", &mut c));
    println!("      agent-c merges: {}  qty(1) = {}", m1.outcome.name(), db2.qty(1));
    let m2 = report_of(db2.ok("MERGE;", &mut d));
    let floor_qty = db2.qty(1);
    println!("      agent-d merges: {}  qty(1) = {}", m2.outcome.name(), floor_qty);
    println!(
        "\n      A guard is a PRECONDITION, re-evaluated against the merged state BEFORE this\n      \
         branch's ops: 8 >= 0 passes, and only then does the composed -12 cross the bound.\n      \
         ferrobranch enforces the predicate the agent wrote, not the invariant the schema means,\n      \
         and there are no declarative CHECK constraints, so nothing else enforces it either."
    );

    led.record(
        7,
        "Guard violation rejected with the violated predicate returned",
        if conflicted && got_predicate && nothing_published { Verdict::Met } else { Verdict::NotMet },
        format!(
            "Returned predicate: {:?}. BOUNDARY: this holds for a guard naming the amount taken \
             (`qty >= 12`). Written as the invariant (`qty >= 0`) the same case is NOT refused and \
             main ends at {}. DESIGN.md section 3 uses `qty >= 0` as its worked example and says \
             re-evaluation yields Conflict; it does not. See DEMO.md criterion 7.",
            predicates, floor_qty
        ),
    );
}

/// Criterion 9 — provenance: which agent + run + model wrote a given row.
fn criterion_9_provenance(led: &mut Ledger) {
    criterion(9, "Provenance: query which agent + run + model wrote a given row");
    let mut db = Db::new();
    db.seed();

    let (mut a, mut b) = (db.session(), db.session());
    db.ok(
        "BEGIN AGENT SESSION AS 'restock-agent' RUN 'run-42' MODEL 'claude-opus-5/2026-05';",
        &mut a,
    );
    db.ok("UPDATE inventory SET qty = qty + 30 WHERE id = 1;", &mut a);
    let ra = report_of(db.ok("MERGE;", &mut a));
    println!("      merged: {}", ra.outcome.name());

    db.ok(
        "BEGIN AGENT SESSION AS 'audit-agent' RUN 'run-99' MODEL 'gpt-9/turbo';",
        &mut b,
    );
    db.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 2;", &mut b);
    let rb = report_of(db.ok("MERGE;", &mut b));
    println!("      merged: {}", rb.outcome.name());

    println!("\n    both branches are now GONE — merged and sealed. Asking about the rows:");
    let who1 = db.runtime.who_wrote_row("inventory", RowId(1));
    let who2 = db.runtime.who_wrote_row("inventory", RowId(2));
    println!("      who wrote inventory row 1? {}", describe(&who1));
    println!("      who wrote inventory row 2? {}", describe(&who2));

    // A row nobody attributed must answer honestly rather than guessing.
    let unknown = db.runtime.who_wrote_row("inventory", RowId(999));
    println!("      who wrote inventory row 999 (never written)? {}", describe(&unknown));

    let full_table = db.runtime.authors_of("inventory");
    println!("\n    full attribution table for `inventory`:");
    for (row, run) in &full_table {
        println!("      row {:<4} <- {}", row.0, run.describe());
    }

    let r1_ok = who1.as_ref().map(|r| {
        r.agent_id == "restock-agent" && r.run_id == "run-42" && r.model == "claude-opus-5"
            && r.model_version == "2026-05"
    }) == Some(true);
    let r2_ok = who2.as_ref().map(|r| r.agent_id == "audit-agent" && r.model == "gpt-9") == Some(true);
    led.check(9, "row 1 is attributed to restock-agent/run-42/claude-opus-5", r1_ok);
    led.check(9, "row 2 is attributed to a different agent, run and model", r2_ok);
    led.check(9, "an unwritten row reports no author rather than guessing", unknown.is_none());

    note("attribution is interned once per RUN, not copied per row — the actor tuple has");
    note("run-level cardinality, so storing it literally per version costs ~3.4x for nothing.");

    led.record(
        9,
        "Provenance: which agent + run + model wrote a given row",
        if r1_ok && r2_ok && unknown.is_none() { Verdict::Partial } else { Verdict::NotMet },
        "PARTIAL, and the boundary matters: attribution is recorded by the agent runtime when a \
merge publishes a row, NOT by the storage write path. `src/execution` contains zero provenance \
references, so an ordinary non-agent INSERT or UPDATE is never attributed. The storage-level \
per-RecordId path (MemProvenanceStore::who_wrote) is real and tested in tests/provenance_e2e.rs, \
but nothing calls it from the executor.",
    );
}

fn describe(r: &Option<ferrodb::provenance::RunEntity>) -> String {
    match r {
        Some(e) => e.describe(),
        None => "<unattributed>".to_string(),
    }
}

/// Criterion 10 — REVERT ... CASCADE uses retained read-sets.
fn criterion_10_revert_cascade(led: &mut Ledger) {
    criterion(10, "REVERT ... CASCADE uses retained read-sets to find a downstream dependent");
    let mut db = Db::new();
    db.seed();

    println!("\n  (a) agent-a changes row 1 and merges");
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    report_of(db.ok("MERGE;", &mut a));
    println!("      qty(1) = {}", db.qty(1));

    println!("\n  (b) agent-b READS row 1, then writes row 2 on the strength of what it read");
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    let seen = rows_of(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b));
    println!("      agent-b observed qty(1) = {:?}   <-- this read is retained", seen[0][0]);
    note("a point lookup on the primary key, so the read-set is retained as EXACT versions");
    note("(a scan would retain a predicate summary instead — chosen by access shape, not size)");
    db.ok("UPDATE inventory SET qty = qty + 2 WHERE id = 2;", &mut b);
    report_of(db.ok("MERGE;", &mut b));
    println!("      qty(2) = {}", db.qty(2));

    println!("\n  (c) now revert agent-a's merge. HALT is the default.");
    let mut main = db.session();
    let plan = match agent_out(db.ok("REVERT MERGE m_1;", &mut main)) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    };
    println!("      blocked ......... {}", plan.is_blocked());
    println!("      blocked by ...... {:?}", plan.blocked_by);
    println!("      qty(1)={}  qty(2)={}   (nothing moved)", db.qty(1), db.qty(2));
    note("agent-b was found through its RETAINED READ-SET — it never wrote row 1, it only read it.");
    note("Without read-sets there is no edge here at all and the revert would silently corrupt b's work.");
    let halted = plan.is_blocked() && plan.blocked_by.len() == 1 && db.qty(1) == 15;

    println!("\n  (d) CASCADE, on explicit request only. Dependents are undone first.");
    let plan = match agent_out(db.ok("REVERT MERGE m_1 CASCADE;", &mut main)) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    };
    println!("      cascaded ........ {:?}", plan.cascade);
    println!("      qty(1)={}  qty(2)={}   (both back to their seeded values)", db.qty(1), db.qty(2));

    let cascaded = !plan.is_blocked() && plan.cascade.len() == 1 && db.qty(1) == 20 && db.qty(2) == 5;
    led.check(10, "the default halts and shows the dependency", halted);
    led.check(10, "cascade undoes the dependent and then the target", cascaded);

    led.record(
        10,
        "REVERT CASCADE finds downstream dependents via read-sets",
        if halted && cascaded { Verdict::Met } else { Verdict::NotMet },
        "The dependency is a read-write edge, not a write-write one: agent-b never wrote row 1.",
    );
}

// =================================================================================================

fn main() -> ExitCode {
    let mut banner = String::new();
    let _ = writeln!(banner, "ferrodb — agent-isolation database");
    let _ = writeln!(banner, "End-to-end demonstration of the ten exit criteria (DESIGN.md section 5)");
    rule('=');
    print!("{}", banner);
    rule('=');
    println!(
        "\nTwo acts, because the SQL statement path and the page layer are not yet joined:\n\
         \n  ACT I  — branch engine. Real 4KB pages in a real file. Criteria 1 and 8.\n\
         ACT II — agent SQL surface. Real scanner/parser/binder/executor. Criteria 2-7, 9, 10.\n\
         \nA row written by a SQL STATEMENT in Act II does NOT live on a page from Act I: it is\n\
         staged in an in-memory workspace. The runtime does now have a page-backed row path\n\
         (AgentRuntime::with_storage + put_row), and criteria 1 and 8 are measured through it in\n\
         tests/integration_zero_copy_fork.rs — but statements do not route through it yet.\n\
         That seam is what remains open, and DEMO.md documents exactly what it costs."
    );

    let mut led = Ledger::new();

    act("ACT I — THE BRANCH ENGINE (pages are real here)");
    criterion_1_fork_copies_zero_pages(&mut led);
    criterion_8_lease_reaping(&mut led);

    act("ACT II — THE AGENT SQL SURFACE (statements are real here)");
    criterion_2_isolation(&mut led);
    criterion_3_as_of_branch(&mut led);
    criterion_4_diff(&mut led);
    criteria_5_and_6_merge_outcomes(&mut led);
    criterion_7_guard(&mut led);
    criterion_9_provenance(&mut led);
    criterion_10_revert_cascade(&mut led);

    led.summary();

    if led.broken.is_empty() {
        println!("\nAll self-checks passed.");
        ExitCode::SUCCESS
    } else {
        println!("\n{} SELF-CHECK(S) FAILED — the transcript above is not trustworthy.", led.broken.len());
        ExitCode::FAILURE
    }
}
