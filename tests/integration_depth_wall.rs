//! D13b: the branch-depth wall, through the production path.
//!
//! `MAX_BRANCH_DEPTH` is 8 and `BranchRecord::fork_child_parts` refuses past it. The only thing in
//! the system that can get past that refusal is `TwoTierReaper::collapse`, which materialises a
//! branch to a fresh root and re-parents it to trunk at depth 1 — and until this file existed
//! `collapse` had **no production caller at all**: every call site in the tree was under
//! `#[cfg(test)]`. So a 9th agent session forked from an 8-deep chain did not collapse and
//! continue, it returned an error and the chain was over. That is a correctness hole, not a
//! tuning question: an agent that forks a sub-agent that forks a sub-agent hits a hard floor
//! after eight hops with no way to proceed.
//!
//! These tests drive `AgentRuntime::begin_session`, which is the funnel every agent session in
//! the system goes through, rather than the catalog underneath it.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, MAX_BRANCH_DEPTH};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowPageLinks, PageStore};
use ferrodb::catalog::column::Value;
use ferrodb::storage::disk_manager::DiskManager;

struct Db {
    runtime: AgentRuntime,
    branches: Arc<LogBranchCatalog>,
    _dir: tempfile::TempDir,
}

/// `links` decides whether the reaper can collapse at all: without a page-layout walker
/// `collapse` refuses rather than re-parent a branch onto ancestor-owned pages. Both arms are
/// built here so the negative control is the same code with one thing removed.
fn db(links: bool, reaper_attached: bool) -> Db {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("pages.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let branches = Arc::new(LogBranchCatalog::in_memory(1));
    let base = bp.disk_manager.high_water().unwrap() + 64;
    let store = Arc::new(
        ArenaPageStore::new(bp.clone(), Arc::clone(&branches) as Arc<dyn BranchCatalog>, base)
            .unwrap(),
    );
    let mut reaper = TwoTierReaper::new(
        Arc::clone(&branches) as Arc<dyn BranchCatalog>,
        Arc::clone(&store),
    );
    if links {
        reaper = reaper.with_links(Arc::new(CowPageLinks));
    }
    let runtime = AgentRuntime::with_storage(
        Arc::clone(&branches) as Arc<dyn BranchCatalog>,
        Arc::new(ferrodb::tel::MemEffectLog::new()),
        Arc::clone(&store) as Arc<dyn PageStore>,
    )
    .unwrap();
    let runtime = if reaper_attached {
        runtime.with_reaper(Arc::new(reaper) as Arc<dyn Reaper>)
    } else {
        runtime
    };
    Db { runtime, branches, _dir: dir }
}

/// Fork `n` sessions, each from the previous one, returning every branch in order.
fn chain(db: &Db, n: usize) -> Vec<BranchId> {
    let mut out = Vec::new();
    let mut parent = BranchId::TRUNK;
    for i in 0..n {
        let s = db
            .runtime
            .begin_session("agent", Some(&format!("run{i}")), parent)
            .unwrap_or_else(|e| panic!("fork {} of {} failed: {}", i + 1, n, e));
        parent = s.branch;
        out.push(parent);
    }
    out
}

/// The wall itself: a chain one longer than `MAX_BRANCH_DEPTH` must go through.
///
/// This is the test that failed before `begin_session_as` learned to collapse, with
/// `branch b_8 is at ancestry depth 9, max is 8`.
#[test]
fn a_fork_past_the_depth_wall_collapses_instead_of_failing() {
    let db = db(true, true);
    let deep = chain(&db, MAX_BRANCH_DEPTH as usize);
    let last = *deep.last().unwrap();
    assert_eq!(
        db.branches.get(last).unwrap().depth,
        MAX_BRANCH_DEPTH,
        "the chain did not actually reach the wall"
    );

    // The branch at the wall holds a row of its own, so the collapse has something to lose.
    db.runtime.put_row(last, "t", 1, &[Value::Integer(1), Value::Integer(41)]).unwrap();

    // The fork that used to fail.
    let ninth = db
        .runtime
        .begin_session("agent", Some("run9"), last)
        .expect("a 9th fork must collapse the parent and proceed, not fail");

    let parent_now = db.branches.get(last).unwrap();
    assert_eq!(parent_now.depth, 1, "the parent was not collapsed");
    assert_eq!(
        parent_now.parent_id,
        Some(BranchId::TRUNK),
        "a collapsed branch must be re-parented to trunk"
    );
    assert_eq!(
        db.branches.get(ninth.branch).unwrap().depth,
        2,
        "the new child must sit at depth 2 under the collapsed parent"
    );

    // Collapse materialises a new root, so the row has to survive the copy — for the parent and
    // for the child that forked off it afterwards. A depth wall that is broken by losing data is
    // not broken.
    assert_eq!(
        db.runtime.get_row(last, "t", 1).unwrap(),
        Some(vec![Value::Integer(1), Value::Integer(41)]),
        "the collapsed branch lost its own row"
    );
    assert_eq!(
        db.runtime.get_row(ninth.branch, "t", 1).unwrap(),
        Some(vec![Value::Integer(1), Value::Integer(41)]),
        "the child forked after the collapse cannot see its parent's row"
    );

    // And the chain keeps going: depth is a renewable resource now, not a budget of eight.
    let mut parent = ninth.branch;
    for i in 0..MAX_BRANCH_DEPTH as usize * 2 {
        parent = db
            .runtime
            .begin_session("agent", Some(&format!("more{i}")), parent)
            .unwrap_or_else(|e| panic!("fork {} past the wall failed: {}", i, e))
            .branch;
    }
    assert_eq!(
        db.runtime.get_row(parent, "t", 1).unwrap(),
        Some(vec![Value::Integer(1), Value::Integer(41)]),
        "the row did not survive repeated collapses down the chain"
    );
}

/// Negative control 1: with no reaper attached there is nothing that *can* collapse, so the wall
/// must still refuse — and say so, naming the depth rather than reporting some unrelated failure.
///
/// Without this the test above would pass just as well if `begin_session` had simply stopped
/// checking depth.
#[test]
fn without_a_reaper_the_wall_still_refuses() {
    let db = db(true, false);
    let deep = chain(&db, MAX_BRANCH_DEPTH as usize);
    let err = db
        .runtime
        .begin_session("agent", Some("run9"), *deep.last().unwrap())
        .expect_err("with no reaper attached there is nothing to collapse with");
    let msg = err.to_string();
    assert!(
        msg.contains("ancestry depth") && msg.contains("reaper"),
        "the refusal must name the depth AND why nothing collapsed; got: {msg}"
    );
}

/// Negative control 2: a reaper with no page-layout walker cannot collapse either. The wall must
/// refuse, and the message must carry `collapse`'s own reason rather than swallowing it — an
/// operator who sees only "depth exceeded" has no way to find out that the fix is a walker.
#[test]
fn without_a_page_walker_the_wall_refuses_and_says_why() {
    let db = db(false, true);
    let deep = chain(&db, MAX_BRANCH_DEPTH as usize);
    let err = db
        .runtime
        .begin_session("agent", Some("run9"), *deep.last().unwrap())
        .expect_err("a reaper with no walker must not be able to break the wall");
    let msg = err.to_string();
    assert!(
        msg.contains("ancestry depth") && msg.contains("refusing to re-parent"),
        "the refusal must carry collapse's own reason; got: {msg}"
    );
}

/// The hazard the wiring introduces, pinned down: **a changeset taken after a collapse must still
/// be the branch's own writes, not its whole tree.**
///
/// `page_changeset` diffs the branch's current root against the `fork_root` cached in its
/// workspace, and `CowTree::diff` prunes on page IDENTITY — a subtree with the same page id on
/// both sides is skipped unread. Collapse materialises the branch onto entirely fresh pages, so
/// after it fires nothing is shared and the pruning saves nothing: both trees are decoded in full.
/// That is a cost, and the doc on `fork_through_the_depth_wall` states it.
///
/// What must NOT happen is the cost turning into a wrong answer — every key reported as changed
/// because every page id changed. `diff` compares VALUES after the prune, so it does not; this
/// test is what makes that a checked fact rather than a reading of a comment. If it ever became
/// false, a MERGE after a collapse would publish rows the branch never touched.
#[test]
fn a_changeset_taken_after_a_collapse_is_still_only_what_the_branch_wrote() {
    let db = db(true, true);

    // Trunk holds rows the branch does not touch. These are the ones a page-identity answer
    // would wrongly report.
    for id in 1..=40u64 {
        db.runtime
            .put_row(BranchId::TRUNK, "t", id, &[Value::Integer(id as i32), Value::Integer(0)])
            .unwrap();
    }

    let deep = chain(&db, MAX_BRANCH_DEPTH as usize);
    let last = *deep.last().unwrap();
    // One write of its own, on top of the trunk rows it inherited.
    db.runtime.put_row(last, "t", 7, &[Value::Integer(7), Value::Integer(99)]).unwrap();

    let before: Vec<_> = db.runtime.page_changeset(last).unwrap();
    assert_eq!(before.len(), 1, "expected one staged change before the collapse, got {before:?}");
    let root_before = db.branches.get(last).unwrap().root_page_id;

    // The fork that collapses `last`.
    db.runtime.begin_session("agent", Some("run9"), last).unwrap();

    // Prove the collapse actually fired, or everything below passes for the wrong reason: an
    // assertion that the diff is unchanged is worth nothing if the tree was never renumbered.
    let rec = db.branches.get(last).unwrap();
    assert_eq!(rec.depth, 1, "the collapse did not fire, so this test proves nothing");
    assert_ne!(
        rec.root_page_id, root_before,
        "the root was not renumbered, so the identity pruning was never defeated"
    );

    let after: Vec<_> = db.runtime.page_changeset(last).unwrap();
    assert_eq!(
        after.len(),
        1,
        "the collapse renumbered every page and the diff reported {} changes instead of the one \
         row this branch wrote: {:?}",
        after.len(),
        after
    );
    assert_eq!(after[0].row, 7, "the wrong row survived as the branch's change");
}
