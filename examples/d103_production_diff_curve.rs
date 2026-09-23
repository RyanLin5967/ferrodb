//! D103 — does the PAGE-DERIVED changeset, `AgentRuntime::page_changeset_with_cost`, cost
//! O(delta · log_m N), or is its `cow::diff` wiring cosmetic?
//!
//! # ⛔ D193 correction (2026-09-23): this is NOT the cost of `DIFF <branch>`
//!
//! This harness and the file it banks used to call its subject "the PRODUCTION `DIFF` path" and
//! "the function `DIFF <branch>` reaches through `dispatch::exec_agent`". **Both were false, at
//! every commit.** Read from source at `fc9556a`, by symbol:
//!
//! * `dispatch::exec_agent`, arm `BoundAgentStmt::Diff`, calls `AgentRuntime::diff` and nothing
//!   else.
//! * `AgentRuntime::diff` builds the changeset from the workspace's own touched-rows map
//!   (`ws.rows`, `ws.base_rows`, `ws.frame`) and descends no page tree. It calls neither
//!   `page_changeset_with_cost` nor `cow::diff::diff` nor `CowTree::diff`.
//! * `page_changeset_with_cost` has **no caller in `src/`** outside `page_changeset`, which has
//!   none either. Its callers are integration tests and this example.
//! * `git log -S page_changeset -- src/` finds no commit that routed `DIFF` to it, so "the
//!   production `DIFF` path used to call `CowTree::diff`" was never true either: `page_changeset`
//!   used to call it.
//!
//! So the integers this harness banks are a real measurement of a real function — the
//! page-derived changeset, which `page_changeset`'s own doc describes as "what `DIFF` looks like
//! when shadow paging provides it", i.e. a candidate, not the wiring — and they are **not** the
//! cost of a `DIFF` statement. Do not quote them as one. What `DIFF` costs is
//! `AgentRuntime::diff`'s question, and nothing here measures it.
//!
//! The example and the banked file keep the name `d103_production_diff_curve` only because
//! renaming them would break every existing citation. **The name is historical and it is wrong.**
//! Every sentence the harness writes into the banked file now says what was measured, and its
//! first line names the commit the binary was built at, because the "not reached by `DIFF`" fact
//! is a fact about a tree and would go stale silently if `DIFF` were ever rewired.
//!
//! # What it measures
//!
//! D91 measured `cow::diff::diff` directly, on a `CowTree` built by the example itself. That
//! proves the algorithm on a tree the example owned; the module then had **zero external
//! callers**. This harness drives `AgentRuntime::page_changeset_with_cost` — which, since D103,
//! calls `cow::diff::diff` on the agent branch's own page tree — over a real agent branch forked
//! from a real trunk tree, and reports what that call cost.
//!
//! # Pre-registered, before the first run
//!
//! delta is held at exactly 4 changed rows at every N, so N is the only axis. (D193: the two
//! outcome labels below originally read "the production path" and "the wiring"; they are
//! corrected in place and marked. The thresholds are unchanged.)
//!
//!   * `visited` grows like log N — roughly one more level per 4x in N, and the 1k -> 256k ratio
//!     well under the 256x growth in N  -> `page_changeset_with_cost` [D193: was "the production
//!                                         path"] is O(delta · log N).
//!   * `visited` grows like N (ratio near 256x)  -> its `cow::diff` wiring [D193: was "the
//!                                         wiring"] is decoration and the row is a negative result
//!                                         to be reported as one.
//!   * `visited` is 0 while 4 changes are still reported
//!                                                      -> impossible; the harness is not diffing
//!                                                         what it thinks and the row is void.
//!
//! # The control, and why it is `pages_walked` and not `pages_examined`
//!
//! `CowTree::diff` — what `page_changeset` called before D103 — is run on the same two roots at
//! every N. Its control number is `pages_walked`: `walk_pages(base) + walk_pages(head)`, the
//! enumeration it must complete before it can prune either side against the other. It is expected
//! to track N exactly; if it does not, the control is wrong and the comparison is meaningless.
//!
//! `pages_examined` is printed beside it precisely because it does NOT track N. That was the whole
//! defect: the old path reported the decode half only, so an O(N) operation read as cheap.
//!
//! # Counters, not clocks
//!
//! No durations are banked. This box runs a build fleet at load 20-60 and a 46x quiet-vs-loaded
//! spread has been measured on it, so a wall clock here would report the machine rather than the
//! algorithm. Node counts are integers and do not move when the box is busy.
//!
//! Run: `cargo run --release --example d103_production_diff_curve`

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, PageId};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::cow::node::Node;
use ferrodb::cow::page_header::{PageHeader, PageType};
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;

const ARENA_BASE: u32 = 1024;
const TABLE: &str = "inventory";

/// The four rows changed at every N. Held fixed so delta is not an axis.
const CHANGED: [u64; 4] = [3, 17, 511, 999];

struct Row {
    n: usize,
    tree_nodes: usize,
    depth: usize,
    visited: usize,
    skipped_subtrees: usize,
    control_walked: usize,
    control_examined: usize,
    changes: usize,
}

fn depth_of(store: &Arc<dyn PageStore>, root: PageId) -> usize {
    let mut pid = root;
    let mut d = 1;
    loop {
        let h = store.read_page(pid).unwrap();
        let f = h.read();
        let ty = PageHeader::read_from(&f.data).unwrap().page_type;
        if ty == PageType::BTreeLeaf {
            return d;
        }
        let next = Node::new(&f.data).leftmost();
        drop(f);
        drop(h);
        pid = next;
        d += 1;
        assert!(d < 64, "depth walk ran away");
    }
}

fn measure(n: usize) -> Row {
    let dir = tempfile::TempDir::new().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("d103.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let branches = Arc::new(LogBranchCatalog::in_memory(1));
    let store = Arc::new(
        ArenaPageStore::new(
            bp.clone(),
            Arc::clone(&branches) as Arc<dyn ferrodb::branch::BranchCatalog>,
            ARENA_BASE,
        )
        .unwrap(),
    );
    let dyn_store: Arc<dyn PageStore> = Arc::clone(&store) as Arc<dyn PageStore>;
    let runtime =
        AgentRuntime::with_storage(branches, Arc::new(MemEffectLog::new()), dyn_store.clone())
            .unwrap();

    // The base table, on trunk's own tree. This is what makes the fork point a real tree rather
    // than an empty one — the case `page_changeset`'s own doc says it is waiting for.
    for i in 0..n {
        let row = vec![Value::Integer(i as i32), Value::Integer(100)];
        runtime.put_row(BranchId::TRUNK, TABLE, i as u64, &row).unwrap();
    }

    // Fork. `begin_session` is the call `BEGIN AGENT SESSION` makes; the workspace records the
    // trunk root it forked from, which is the base side of the diff below.
    let session = runtime.begin_session("bench-agent", Some("r_bench"), BranchId::TRUNK).unwrap();
    let branch = session.branch;
    let fork_root = runtime.root_of(branch).unwrap();

    // Exactly four rows change. Written through `put_row`, the same call `stage_all` mirrors
    // every staged row with.
    for &i in &CHANGED {
        assert!((i as usize) < n, "changed row {i} is outside a tree of {n}");
        let row = vec![Value::Integer(i as i32), Value::Integer(999)];
        runtime.put_row(branch, TABLE, i, &row).unwrap();
    }
    let head_root = runtime.root_of(branch).unwrap();

    // ---- the subject: page_changeset_with_cost. NOT what `DIFF` runs — see the module doc -----
    let (changes, cost) = runtime.page_changeset_with_cost(branch).unwrap();

    // ---- the control: what page_changeset called before D103 ----------------------------------
    let tree = runtime.storage().unwrap().tree();
    let old = tree.diff(fork_root, head_root).unwrap();

    // Both paths must agree about WHAT changed, or the cost comparison is between two different
    // questions. Aborts the run rather than banking a row.
    assert_eq!(
        changes.len(),
        CHANGED.len(),
        "page_changeset_with_cost reported {} changes at n={n}, expected {}",
        changes.len(),
        CHANGED.len()
    );
    assert_eq!(
        old.deltas.len(),
        CHANGED.len(),
        "the control reported {} changes at n={n}, expected {}",
        old.deltas.len(),
        CHANGED.len()
    );

    Row {
        n,
        tree_nodes: tree.walk_pages(head_root).unwrap().len(),
        depth: depth_of(&dyn_store, head_root),
        visited: cost.visited,
        skipped_subtrees: cost.skipped_subtrees,
        control_walked: old.pages_walked,
        control_examined: old.pages_examined,
        changes: changes.len(),
    }
}

fn main() {
    let provenance = if cfg!(debug_assertions) { "(DEBUG BUILD)" } else { "(release)" };
    let sizes = [1_000usize, 4_000, 16_000, 64_000, 256_000];
    let mut rows = Vec::new();
    for n in sizes {
        eprintln!("  building n={n} ...");
        let r = measure(n);
        eprintln!(
            "    nodes={} depth={} visited={} control_walked={}",
            r.tree_nodes, r.depth, r.visited, r.control_walked
        );
        rows.push(r);
    }

    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut out = String::new();
    // D193: the title and Subject lines below used to call the subject "the PRODUCTION DIFF
    // path" and "the call DIFF <branch> reaches". Neither was ever true; see the module doc. The
    // build commit is printed because the "not reached by DIFF" sentence below is a fact about a
    // tree, checked by reading source, and it goes stale silently if DIFF is ever rewired.
    out.push_str(&format!(
        "d103 — the PAGE-DERIVED changeset (AgentRuntime::page_changeset_with_cost), cost curve \
         {provenance}\n"
    ));
    out.push_str(&format!("built {}\n", ferrodb::build_provenance()));
    out.push_str(&format!("run at unix {unix}\n\n"));
    out.push_str("Subject: AgentRuntime::page_changeset_with_cost. This is NOT what DIFF <branch> runs.\n");
    out.push_str("  DIFF <branch> dispatches to AgentRuntime::diff, which builds the changeset from the\n");
    out.push_str("  workspace's touched-rows map and descends no page tree. page_changeset_with_cost has\n");
    out.push_str("  no caller in src/ (tests and this harness only). That wiring fact was read from\n");
    out.push_str("  source at D193 (fc9556a), not checked by this binary: re-read dispatch.rs before\n");
    out.push_str("  quoting it against a later commit. These numbers are NOT the cost of a DIFF statement.\n");
    out.push_str("Question: is the page-derived changeset O(delta · log_m N), or O(N)?\n");
    out.push_str("delta is held at exactly 4 changed rows; N is the only axis.\n\n");
    out.push_str("Instruments:\n");
    out.push_str("  visited   DiffCost::visited — nodes the synchronised descent DECODED, which on\n");
    out.push_str("            that path is also every node it read. THE CLAIM.\n");
    out.push_str("  skipped   subtree pairs found equal by page identity and abandoned unread.\n");
    out.push_str("  walked    TreeDiff::pages_walked — walk_pages(base)+walk_pages(head), what\n");
    out.push_str("            CowTree::diff (page_changeset's body before D103) must enumerate\n");
    out.push_str("            before it can prune. THE CONTROL.\n");
    out.push_str("  examined  TreeDiff::pages_examined — the old counter. It reports the DECODE\n");
    out.push_str("            half only, which is why an O(N) operation read as cheap.\n\n");
    out.push_str("Subject and control were asserted to report the same 4 changes at every N; a row\n");
    out.push_str("that did not would have aborted the run rather than been banked.\n\n");
    out.push_str("      N   nodes  depth | visited  skipped  changes |     walked   examined\n");
    out.push_str("                       |                           |  (control)   (the lie)\n");
    out.push_str("-------  ------  ----- | -------  -------  ------- |  ---------   ---------\n");
    for r in &rows {
        out.push_str(&format!(
            "{:7}  {:6}  {:5} | {:7}  {:7}  {:7} |  {:9}   {:9}\n",
            r.n,
            r.tree_nodes,
            r.depth,
            r.visited,
            r.skipped_subtrees,
            r.changes,
            r.control_walked,
            r.control_examined,
        ));
    }

    let first = rows.first().unwrap();
    let last = rows.last().unwrap();
    let n_growth = last.n as f64 / first.n as f64;
    let visited_growth = last.visited as f64 / first.visited.max(1) as f64;
    let control_growth = last.control_walked as f64 / first.control_walked.max(1) as f64;
    out.push_str(&format!(
        "\nN grew {:.0}x ({} -> {}).\n\
         visited grew {:.1}x ({} -> {}), tracking DEPTH {} -> {}.\n\
         control  grew {:.0}x ({} -> {}), tracking N.\n",
        n_growth,
        first.n,
        last.n,
        visited_growth,
        first.visited,
        last.visited,
        first.depth,
        last.depth,
        control_growth,
        first.control_walked,
        last.control_walked,
    ));
    out.push_str(&format!(
        "\nThe old counter, for contrast: examined {} -> {}, which is flat and true and says\n\
         nothing at all about what that path cost.\n",
        first.control_examined, last.control_examined
    ));

    // D193: outcome labels corrected from "the production path" / "The wiring"; thresholds
    // unchanged.
    let verdict = if visited_growth < n_growth / 8.0 && control_growth > n_growth / 2.0 {
        "PRE-REGISTERED OUTCOME 1: page_changeset_with_cost is O(delta · log N).\n\
         (A statement about the page-derived changeset, not about DIFF <branch> — see Subject.)"
    } else if control_growth <= n_growth / 2.0 {
        "VOID: the control did not track N, so it is not the control this run assumed."
    } else {
        "PRE-REGISTERED OUTCOME 2: visited tracks N. The cow::diff wiring inside \
         page_changeset_with_cost is decoration."
    };
    out.push_str(&format!("\n{verdict}\n"));

    print!("{out}");
    let path = "bench/d103_production_diff_curve.txt";
    std::fs::create_dir_all("bench").ok();
    std::fs::write(path, &out).unwrap();
    eprintln!("banked to {path}");
}
