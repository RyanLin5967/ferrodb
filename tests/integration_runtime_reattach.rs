//! E19 — a page-backed runtime can open a database that already has a trunk tree.
//!
//! `AgentRuntime::with_storage` always created a fresh trunk root. That is right for a new
//! database and silently destructive for an existing one: the old tree becomes unreachable, every
//! row in it disappears, and the database reports itself healthy and empty. It was the single
//! blocker between the shipped binaries and the page path — wiring the CLI to `with_storage` would
//! have discarded the tree on every open.
//!
//! There are now two constructors, and **they refuse each other's case rather than guessing**. A
//! single "create it if missing" call has to decide from an ambiguous page whether it is looking at
//! a fresh database or an existing one, and the cost of guessing wrong is unrecoverable and quiet.
//!
//! The probe is the page **checksum**, not the page type. `PageHeader::read_from` parses a type
//! byte out of arbitrary bytes and can succeed on a page belonging to something else — in the CLI,
//! page 1 is the first catalog page and `TRUNK_ROOT_PAGE` is 1. The last test here is that exact
//! collision.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;

const ARENA_BASE: u32 = 1024;

/// Everything a page-backed runtime needs, kept so a second runtime can be built over the same
/// store and catalog — which is what "reopen" means when the process has not actually exited.
struct Env {
    bp: Arc<BufferPoolManager>,
    branches: Arc<LogBranchCatalog>,
    store: Arc<ArenaPageStore>,
    _dir: tempfile::TempDir,
}

fn env(tag: &str) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let branches = Arc::new(LogBranchCatalog::in_memory(1));
    let store =
        Arc::new(ArenaPageStore::new(bp.clone(), Arc::clone(&branches), ARENA_BASE).unwrap());
    Env { bp, branches, store, _dir: dir }
}

impl Env {
    fn create(&self) -> AgentRuntime {
        AgentRuntime::with_storage(
            Arc::clone(&self.branches) as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&self.store) as Arc<dyn PageStore>,
        )
        .expect("with_storage on a fresh database")
    }

    fn reopen(&self) -> Result<AgentRuntime, ferrodb::error::FerroError> {
        AgentRuntime::reopen_with_storage(
            Arc::clone(&self.branches) as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&self.store) as Arc<dyn PageStore>,
        )
    }

    fn trunk_root(&self) -> u32 {
        self.branches.get(BranchId::TRUNK).unwrap().root_page_id
    }
}

/// **The row.** Rows written by one runtime are readable by a second one that reattached.
#[test]
fn a_reopened_runtime_reads_the_rows_the_first_one_wrote() {
    let e = env("roundtrip");
    let first = e.create();

    let root_after_create = e.trunk_root();
    assert!(
        root_after_create >= ARENA_BASE,
        "trunk's root is {root_after_create}, below the arena floor — it is still the placeholder, \
         so no tree was created and reattaching would prove nothing"
    );

    first
        .put_row(BranchId::TRUNK, "inventory", 7, &[Value::Integer(7), Value::Integer(70)])
        .expect("put_row");

    // Deliberately NOT asserting that trunk's root moved. It does not, and that is correct: the
    // root page is already private to trunk's own arena, so `cow_page` mutates it in place rather
    // than shadowing. Shadowing is for pages a parent or sibling can still see. An earlier version
    // of this test asserted the root moved and failed with `1024 != 1024` — the assumption was
    // wrong, not the engine.
    let root_after_write = e.trunk_root();
    assert_eq!(
        first.get_row(BranchId::TRUNK, "inventory", 7).unwrap(),
        Some(vec![Value::Integer(7), Value::Integer(70)]),
        "the first runtime cannot read back its own write, so there is nothing to reattach to"
    );
    drop(first);

    // A second runtime over the same store and catalog — what a restart looks like from here.
    let second = e.reopen().expect("reopen_with_storage on a database that has a trunk tree");
    let got = second
        .get_row(BranchId::TRUNK, "inventory", 7)
        .expect("get_row")
        .expect("the row is missing after reattaching");
    assert_eq!(
        got,
        vec![Value::Integer(7), Value::Integer(70)],
        "the reattached runtime read different values than were written"
    );
    assert_eq!(e.trunk_root(), root_after_write, "reattaching moved trunk's root");
}

/// Reopening a database that has no trunk tree must refuse, not invent one.
#[test]
fn reopening_a_database_with_no_trunk_tree_is_refused_by_name() {
    let e = env("fresh");
    let msg = match e.reopen() {
        Ok(_) => panic!("reopen invented a trunk tree on a fresh database"),
        Err(e) => format!("{e}"),
    };
    assert!(msg.contains("nothing to reopen"), "wrong reason: {msg}");
    assert!(msg.contains("with_storage"), "the message does not say what to do instead: {msg}");

    // And creating then works, so the refusal is about state rather than a permanent block.
    let _rt = e.create();
    assert!(e.reopen().is_ok(), "reopen still refused after the tree was created");
}

/// **Creating a second trunk tree over an existing one must be refused.** This is the destructive
/// case: it succeeds silently, and every row in the first tree is gone with the database looking
/// healthy and empty.
#[test]
fn creating_over_an_existing_trunk_tree_is_refused() {
    let e = env("clobber");
    let first = e.create();
    first
        .put_row(BranchId::TRUNK, "inventory", 1, &[Value::Integer(1), Value::Integer(10)])
        .expect("put_row");
    let root = e.trunk_root();

    let msg = match AgentRuntime::with_storage(
        Arc::clone(&e.branches) as Arc<dyn BranchCatalog>,
        Arc::new(MemEffectLog::new()),
        Arc::clone(&e.store) as Arc<dyn PageStore>,
    ) {
        Ok(_) => panic!("with_storage created a second trunk tree and orphaned the first"),
        Err(e) => format!("{e}"),
    };
    assert!(msg.contains("already has a trunk tree"), "wrong reason: {msg}");
    assert!(msg.contains("reopen_with_storage"), "the message does not say what to do: {msg}");

    // The refusal must leave the tree exactly as it was, not half-replaced.
    assert_eq!(e.trunk_root(), root, "the refused call still moved trunk's root");
    let still_there = e
        .reopen()
        .expect("reopen")
        .get_row(BranchId::TRUNK, "inventory", 1)
        .expect("get_row");
    assert_eq!(still_there, Some(vec![Value::Integer(1), Value::Integer(10)]));
}

/// **The collision the checksum probe exists for.**
///
/// `TRUNK_ROOT_PAGE` is 1, and in the CLI page 1 is the first catalog page. A probe that only
/// parsed the page header could read a type byte out of unrelated bytes and conclude a tree is
/// there — then descend into someone else's data. Here trunk's root is pointed at a real, readable
/// page that this branch engine did not write, and reopening must still refuse.
#[test]
fn a_root_pointing_at_a_page_this_engine_did_not_write_is_refused() {
    let e = env("collision");

    // A real page, written through the buffer pool, containing bytes that are not a CoW node.
    let victim = e.bp.new_page().expect("allocate a non-arena page");
    {
        let idx = e.bp.fetch_page(victim).expect("fetch");
        let mut frame = e.bp.frames[idx].write().unwrap();
        // Deliberately not zeroes: an all-zero page could checksum to zero and pass by accident.
        for (i, b) in frame.data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        // **Byte 16 is the page type, and it is set to a VALID one (2 = BTreeLeaf) on purpose.**
        // Without this the page is rejected by the type parser alone and the test cannot tell a
        // checksum probe from a type probe — verified by weakening the probe to type-only and
        // watching this test still pass. A page that parses as a header but was written by
        // something else is the whole hazard: in the CLI, page 1 is the first catalog page.
        frame.data[16] = 2;
        drop(frame);
        e.bp.unpin_page(victim, true);
        e.bp.flush_page(victim).expect("flush");
    }
    e.branches.set_root(BranchId::TRUNK, victim).expect("point trunk at it");
    assert_eq!(e.trunk_root(), victim, "the test did not manage to point trunk at the page");

    let msg = match e.reopen() {
        Ok(_) => panic!("reopen accepted a page this engine never wrote as a trunk tree"),
        Err(e) => format!("{e}"),
    };
    assert!(
        msg.contains("fails the checksum"),
        "refused, but not for a reason that names how the page was rejected: {msg}"
    );

    // And `with_storage` must NOT think a tree is there either: on this database it is correct to
    // create one, because what is at that page is not ours.
    assert!(
        AgentRuntime::with_storage(
            Arc::clone(&e.branches) as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&e.store) as Arc<dyn PageStore>,
        )
        .is_ok(),
        "with_storage refused to create on a database whose trunk root is not a tree"
    );
}
