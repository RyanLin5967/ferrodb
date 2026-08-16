//! Exit criterion 1, measured through the shipped API instead of asserted about a component.
//!
//! The criterion is "`BEGIN AGENT SESSION` forks a branch copying ZERO data pages (prove via page
//! count before/after)". Until now it could only be shown against the branch engine directly,
//! because agent-session rows lived in a `BTreeMap` and the SQL surface never touched a page
//! store — so there were no data pages for the claim to be about.
//!
//! **The trap this file is built around:** a page count that does not change is meaningless if
//! nothing ever writes a page. That is a fact about the test's scope, not about the database, and
//! it would read as a pass. So every test here first asserts the trunk actually occupies pages,
//! and the fork is only credited with copying zero once there was something it could have copied.

use std::sync::Arc;

use ferrodb::agent_sql::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;

/// Above anything the ordinary heap/index allocator will want; the partition is deliberate.
const ARENA_BASE: u32 = 1024;

fn runtime(tag: &str) -> (tempfile::TempDir, Arc<ArenaPageStore>, AgentRuntime) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(Arc::clone(&dm)));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), ARENA_BASE).unwrap());
    let rt = AgentRuntime::with_storage(
        catalog,
        Arc::new(MemEffectLog::new()),
        Arc::clone(&store) as Arc<dyn PageStore>,
    )
    .unwrap();
    (dir, store, rt)
}

/// Fill the trunk with enough rows to occupy several pages, so a fork has something to copy.
fn populate(rt: &AgentRuntime, rows: u64) {
    for r in 0..rows {
        rt.put_row(
            BranchId::TRUNK,
            "inventory",
            r,
            &[Value::Integer(r as i32), Value::Varchar(format!("widget-{r}"))],
        )
        .unwrap();
    }
}

#[test]
fn beginning_an_agent_session_copies_zero_data_pages() {
    let (_d, store, rt) = runtime("zerocopy");
    let empty = store.live_page_count().unwrap();
    populate(&rt, 400);

    let before = store.live_page_count().unwrap();
    // "Unchanged" is trivially true of a counter that never moves, so prove the instrument
    // responds before reading anything into its silence.
    assert!(
        before > empty,
        "live_page_count did not move while writing 400 rows ({empty} -> {before}); it is not \
         measuring anything and 'the fork copied zero' would be a fact about the counter"
    );
    // The guard that keeps the measurement honest: a fork copying zero pages proves nothing if
    // there were no pages. 400 two-cell rows do not fit on one 4KB page.
    assert!(
        before > 1,
        "the trunk occupies {before} page(s), so 'the fork copied zero' would be vacuous"
    );

    let session = rt.begin_session("pricing-agent", Some("r_1"), BranchId::TRUNK).unwrap();

    let after = store.live_page_count().unwrap();
    assert_eq!(
        after, before,
        "BEGIN AGENT SESSION copied {} page(s); the criterion is zero",
        after as i64 - before as i64
    );

    // Zero-copy is only correct if the child can still read the data. A fork that copies nothing
    // *and* sees nothing would satisfy the count while failing the point.
    assert_eq!(
        rt.get_row(session.branch, "inventory", 399).unwrap(),
        Some(vec![Value::Integer(399), Value::Varchar("widget-399".into())]),
        "the forked branch must see the trunk's rows without copying them"
    );
    assert_eq!(rt.scan_rows(session.branch, "inventory").unwrap().len(), 400);
}

#[test]
fn the_fork_stays_zero_copy_as_the_trunk_grows() {
    // One measurement at one size can be a coincidence of allocation. The claim is O(1) in the
    // size of the branch, so it is measured at three sizes.
    //
    // The smallest is 200 and not 50 because the vacuity guard below rejected 50: that many rows
    // fit on a single page, so "the fork copied zero" would have been a fact about the row count
    // rather than about forking. The guard caught that on its first run.
    for rows in [200u64, 400, 1200] {
        let (_d, store, rt) = runtime(&format!("scale{rows}"));
        populate(&rt, rows);
        let before = store.live_page_count().unwrap();
        assert!(before > 1, "{rows} rows occupied {before} page(s); measurement would be vacuous");
        rt.begin_session("a", Some("r"), BranchId::TRUNK).unwrap();
        assert_eq!(
            store.live_page_count().unwrap(),
            before,
            "fork was not zero-copy at {rows} rows ({before} pages)"
        );
    }
}

#[test]
fn a_write_on_the_branch_is_invisible_to_the_trunk_and_to_a_sibling() {
    let (_d, _store, rt) = runtime("isolation");
    populate(&rt, 100);

    let a = rt.begin_session("agent-a", Some("r_a"), BranchId::TRUNK).unwrap();
    let b = rt.begin_session("agent-b", Some("r_b"), BranchId::TRUNK).unwrap();

    rt.put_row(a.branch, "inventory", 7, &[Value::Integer(-1), Value::Varchar("A".into())])
        .unwrap();

    assert_eq!(
        rt.get_row(a.branch, "inventory", 7).unwrap(),
        Some(vec![Value::Integer(-1), Value::Varchar("A".into())]),
        "the writing branch must see its own write"
    );
    let untouched = Some(vec![Value::Integer(7), Value::Varchar("widget-7".into())]);
    assert_eq!(
        rt.get_row(BranchId::TRUNK, "inventory", 7).unwrap(),
        untouched,
        "the trunk must not see an uncommitted branch write"
    );
    assert_eq!(
        rt.get_row(b.branch, "inventory", 7).unwrap(),
        untouched,
        "a sibling branch must not see another branch's uncommitted write"
    );
}

#[test]
fn a_branch_write_costs_pages_only_on_the_path_it_touches() {
    // The other half of copy-on-write: writing does allocate, but proportional to the path
    // copied, not to the branch. If this were a full copy the delta would track the trunk size.
    let (_d, store, rt) = runtime("cowcost");
    populate(&rt, 800);
    let trunk_pages = store.live_page_count().unwrap();

    let s = rt.begin_session("agent", Some("r"), BranchId::TRUNK).unwrap();
    let before = store.live_page_count().unwrap();
    rt.put_row(s.branch, "inventory", 5, &[Value::Integer(0), Value::Varchar("x".into())])
        .unwrap();
    let delta = store.live_page_count().unwrap() - before;

    assert!(delta > 0, "a copy-on-write update must actually shadow a page");
    assert!(
        delta < trunk_pages,
        "one row update shadowed {delta} page(s) against a {trunk_pages}-page trunk, which is a \
         copy, not copy-on-write"
    );
}

#[test]
fn a_runtime_without_a_page_store_refuses_row_calls_instead_of_pretending() {
    // The map-backed runtime is still the default. It must say so rather than silently no-op,
    // which would make a caller believe rows were stored.
    let rt = AgentRuntime::new();
    let e = rt.put_row(BranchId::TRUNK, "inventory", 1, &[Value::Integer(1)]).unwrap_err();
    assert!(format!("{e}").contains("no page store"), "got {e}");
    assert!(rt.storage().is_none());
}
