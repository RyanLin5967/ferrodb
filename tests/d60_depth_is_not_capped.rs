//! D60 — a branch chain may be any depth, and the things depth used to protect still hold.
//!
//! `MAX_BRANCH_DEPTH = 8` refused the ninth fork of any chain, and nothing in production called
//! `collapse`, so a tree-search agent (BranchBench's own workload: fork from your own children)
//! hit a wall at eight. Measured before removing it (`bench/d60_depth_premise.txt`): fork and read
//! are FLAT from depth 1 to 250, because a branch's root is its parent's root at fork and a read
//! never walks ancestry.
//!
//! Three things had to survive the removal, and each is a test here:
//!
//! 1. a chain far past the old cap forks, and every level reads what it inherited;
//! 2. reclamation over a chain of REAPED interior nodes — D16's rule that such a node is a pin —
//!    must still answer correctly, and must not recurse per level (it was mutually recursive with
//!    `live_child_at`, bounded only by the cap);
//! 3. the `depth` field widened from `u8` to `u32`, so a core record written by an older build is
//!    three bytes short and must still load.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::record::{BranchRecord, CORE_BYTES};
use ferrodb::branch::types::{BranchId, BranchState, Epoch, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

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
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true)
            .open(dir.path().join("d60.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d60.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }
    fn exec(&mut self, sql: &str, s: &mut Session) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "{sql}: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }
}

/// A chain 64 deep — eight times the old cap — where every level writes, and the leaf still sees
/// what the root wrote. Before D60 the ninth `begin_session` failed with "is at ancestry depth 9".
#[test]
fn a_chain_far_past_the_old_cap_forks_and_still_reads_what_the_root_wrote() {
    const DEPTH: usize = 64;
    let mut db = Db::new();
    let mut setup = Session::with_runtime(db.runtime.clone());
    db.exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut setup);
    for i in 1..=80 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, {});", i * 10), &mut setup);
    }

    let mut parent = BranchId::TRUNK;
    let mut sessions = Vec::new();
    for level in 1..=DEPTH {
        let agent = db
            .runtime
            .begin_session("d60", Some(&format!("r{level}")), parent)
            .unwrap_or_else(|e| panic!("fork at depth {level} refused: {e}"));
        parent = agent.branch;
        let mut s = Session::with_runtime(db.runtime.clone());
        s.agent = Some(agent);
        // Each level writes its own row, so every level has state of its own to lose.
        db.exec(&format!("UPDATE t SET v = {} WHERE id = {level};", 900000 + level), &mut s);
        sessions.push(s);
    }

    let deepest = sessions.last_mut().expect("a chain");
    // Row 80 is written by the root and by nobody below it.
    match db.exec("SELECT v FROM t WHERE id = 80;", deepest) {
        Outcome::Rows(r) => assert_eq!(r[0][0], Value::Integer(800), "the leaf lost the root's row"),
        _ => panic!("expected rows"),
    }
    // ...and the leaf sees its own write, and the write of the level above it.
    match db.exec(&format!("SELECT v FROM t WHERE id = {DEPTH};"), deepest) {
        Outcome::Rows(r) => assert_eq!(r[0][0], Value::Integer((900000 + DEPTH) as i32)),
        _ => panic!("expected rows"),
    }
    match db.exec("SELECT v FROM t WHERE id = 1;", deepest) {
        Outcome::Rows(r) => assert_eq!(r[0][0], Value::Integer(900001), "level 1's write is not inherited"),
        _ => panic!("expected rows"),
    }

    // **Isolation, which every assertion above would pass without.** A review pointed out that
    // this test never looked outside the chain: an implementation with no branch isolation at all
    // — every write landing on trunk — satisfies "the leaf sees everything". So: trunk must see
    // NONE of the 64 levels' writes, and a sibling forked from trunk must not either.
    let mut trunk = Session::with_runtime(db.runtime.clone());
    for level in [1usize, DEPTH / 2, DEPTH] {
        match db.exec(&format!("SELECT v FROM t WHERE id = {level};"), &mut trunk) {
            Outcome::Rows(r) => assert_eq!(
                r[0][0],
                Value::Integer((level * 10) as i32),
                "trunk sees level {level}'s write: the chain is not isolated from main"
            ),
            _ => panic!("expected rows"),
        }
    }
    let sibling = db.runtime.begin_session("sibling", Some("s1"), BranchId::TRUNK).unwrap();
    let mut sib = Session::with_runtime(db.runtime.clone());
    sib.agent = Some(sibling);
    match db.exec(&format!("SELECT v FROM t WHERE id = {DEPTH};"), &mut sib) {
        Outcome::Rows(r) => assert_eq!(
            r[0][0],
            Value::Integer((DEPTH * 10) as i32),
            "a sibling of the chain's root sees the chain's writes"
        ),
        _ => panic!("expected rows"),
    }
}

/// D16's rule — a reaped interior node with a live descendant is a PIN — over a chain far longer
/// than any recursion should carry. In `TableBranchCatalog`, `has_live_children` resolves each
/// child's RECORD and, for a reaped one, explores its subtree; that was mutual recursion with
/// `live_child_at`, so its call depth was this chain's length and the cap was the only thing
/// keeping it to 8. It is iterative now, and this is what says so.
///
/// (Deliberately the TABLE catalog, not the log one: `LogBranchCatalog::has_live_children` answers
/// from the parent's stored `live_children` array without resolving anything, so it walks nothing
/// and would pass this test no matter how the walk were written.)
#[test]
fn reclamation_walks_a_long_reaped_chain_without_recursing_per_level() {
    // **2,000 levels, chosen by measurement rather than by feel.** With the pre-D60 recursive walk
    // restored, this test PASSES at depth 500 and dies with `fatal runtime error: stack overflow`
    // at 1,000 — so 2,000 is double the measured death point, and still an eighth of the 5,000 the
    // first version used, which cost 99 s of every suite run to prove the same thing.
    const DEPTH: usize = 2_000;
    let dir = tempfile::tempdir().unwrap();
    let c = TableBranchCatalog::open_sidecar(&dir.path().join("b.branchcat"), 1).unwrap();
    let mut chain = Vec::with_capacity(DEPTH);
    let mut cur = BranchId::TRUNK;
    for level in 1..=DEPTH {
        cur = c
            .fork(cur, LeaseDeadline(u64::MAX))
            .unwrap_or_else(|e| panic!("fork at depth {level} refused: {e}"))
            .branch_id;
        chain.push(cur);
    }
    assert_eq!(c.get(cur).unwrap().depth, DEPTH as u32, "depth stopped counting");

    // Reap every INTERIOR node and leave the leaf alive: MCTS pruning, and the shape that makes a
    // long chain of reaped nodes with one live descendant at the bottom.
    let leaf = *chain.last().unwrap();
    for b in &chain[..chain.len() - 1] {
        c.set_state(*b, BranchState::Live, BranchState::Reaping).unwrap();
        c.set_state(*b, BranchState::Reaping, BranchState::Reaped).unwrap();
    }
    assert_eq!(c.get(leaf).unwrap().state, BranchState::Live, "the leaf must still be live");

    // The question the reaper asks of the root: is anything under me still alive? Every level in
    // between is reaped, so the answer has to come from the leaf, 5,000 levels down.
    assert!(
        c.has_live_children(BranchId::TRUNK.id).unwrap(),
        "a live branch {DEPTH} levels down was not seen through {} reaped interior nodes: D16's \
         pin rule does not survive a deep chain",
        DEPTH - 1
    );

    // And when the leaf goes too, nothing under trunk is alive any more.
    c.set_state(leaf, BranchState::Live, BranchState::Reaping).unwrap();
    c.set_state(leaf, BranchState::Reaping, BranchState::Reaped).unwrap();
    assert!(
        !c.has_live_children(BranchId::TRUNK.id).unwrap(),
        "with every branch reaped the chain still reads as pinning trunk"
    );
}

/// The depth field went from `u8` to `u32`, so a core record written by an older build is three
/// bytes short. It must still load, with its byte read as the depth — the same tolerant-read
/// discipline this module already uses for envelopes.
#[test]
fn a_core_record_written_with_a_one_byte_depth_still_loads() {
    let mut r = BranchRecord::trunk(1, LeaseDeadline(7));
    r.depth = 5;
    r.parent_id = Some(BranchId::new(3, 0));
    r.fork_epoch = Epoch(9);
    let current = r.serialize_core();
    assert_eq!(current.len(), CORE_BYTES);

    // The old shape: everything up to `state`, then ONE byte of depth.
    let mut old = current[..CORE_BYTES - 4].to_vec();
    old.push(5);
    assert_eq!(old.len(), CORE_BYTES - 3, "the old core record was three bytes shorter");

    let decoded = BranchRecord::deserialize_core(&old).expect("an older core record must load");
    assert_eq!(decoded.depth(), 5, "the one-byte depth was not read");
    assert_eq!(decoded.branch_id(), r.branch_id);
    assert_eq!(decoded.fork_epoch(), Epoch(9));

    // And the new shape round-trips a depth no byte could hold.
    r.depth = 100_000;
    let wide = BranchRecord::deserialize_core(&r.serialize_core()).expect("round trip");
    assert_eq!(wide.depth(), 100_000);

    // A length that is neither is refused rather than read as one of them. 53 bytes is the
    // interesting case and the one a review asked for: it is one byte short of the new record and
    // two long for the old, i.e. exactly the shape a half-migrated writer would produce.
    for short_by in [1usize, 2] {
        assert!(
            BranchRecord::deserialize_core(&current[..CORE_BYTES - short_by]).is_err(),
            "a record {short_by} byte(s) short of the current core was accepted"
        );
    }
    // And one byte longer than the current core, which no writer produces either.
    let mut too_long = current.clone();
    too_long.push(0);
    assert!(BranchRecord::deserialize_core(&too_long).is_err(), "an over-long core was accepted");
}

/// **A FULL record written by a pre-D60 build must decode exactly — and D60's first version broke
/// this while its checksum still passed.**
///
/// `depth` sits in the middle of the variable-length record, before `arena_len`, `child_len` and
/// the envelope. Writing it as four bytes shifted every one of those. A fresh-context review
/// traced the result byte by byte: a depth-3 branch decoded as depth 50,331,648, the arena count
/// came from bytes that were not it, and `child_len` was read out of the CHECKSUM — so the record
/// either failed with a bogus "truncated" error or drove a `Vec::with_capacity` sized from noise
/// (8.86 GB for a branch owning 900 arenas, an allocation abort rather than an error). The CRC
/// covers the body's bytes, which were unchanged, so it could not catch any of it.
///
/// That format is reachable: `LogBranchCatalog::replay` reads it, and the CLI and pgserver reach
/// it through `TableBranchCatalog::default_for_database`, the one-time migration of a legacy
/// `{db}.branches` log — a file that by construction only an older build wrote.
///
/// The fix is the module's own additive discipline: the byte stays where it was, and the exact
/// depth is appended after every field an older build knows. This test builds the old bytes.
#[test]
fn a_full_record_written_before_the_depth_widened_still_decodes() {
    fn old_bytes(r: &BranchRecord) -> Vec<u8> {
        let mut b: Vec<u8> = Vec::new();
        b.extend_from_slice(&r.branch_id.id.to_be_bytes());
        b.extend_from_slice(&r.branch_id.generation.to_be_bytes());
        b.extend_from_slice(&r.generation.to_be_bytes());
        match r.parent_id {
            Some(p) => {
                b.push(1);
                b.extend_from_slice(&p.id.to_be_bytes());
                b.extend_from_slice(&p.generation.to_be_bytes());
            }
            None => {
                b.push(0);
                b.extend_from_slice(&0u64.to_be_bytes());
                b.extend_from_slice(&0u32.to_be_bytes());
            }
        }
        b.extend_from_slice(&r.fork_epoch.0.to_be_bytes());
        b.extend_from_slice(&r.root_page_id.to_be_bytes());
        b.extend_from_slice(&r.lease_deadline.0.to_be_bytes());
        b.push(r.state.as_u8());
        b.push(r.depth as u8); // ONE byte: the whole point
        b.extend_from_slice(&(r.arenas.len() as u32).to_be_bytes());
        for a in &r.arenas {
            b.extend_from_slice(&a.0.to_be_bytes());
        }
        b.extend_from_slice(&(r.live_children.len() as u32).to_be_bytes());
        for c in &r.live_children {
            b.extend_from_slice(&c.0.to_be_bytes());
        }
        b.push(0); // envelope tag: none. Old builds that had envelopes wrote this too.
        let crc = ferrodb::wal::log::crc32(&b);
        b.extend_from_slice(&crc.to_be_bytes());
        b
    }

    // The two shapes the review named: a plain child, and one owning arenas and live children —
    // the second is where a shifted `arena_len` turned into a multi-gigabyte allocation.
    let mut plain = BranchRecord::trunk(1, LeaseDeadline(77));
    plain.parent_id = Some(BranchId::new(4, 0));
    plain.fork_epoch = Epoch(9);
    plain.depth = 3;

    let mut heavy = plain.clone();
    heavy.depth = 8;
    heavy.arenas = (1..=900).map(ferrodb::branch::types::ArenaId).collect();
    heavy.live_children = (1..=17).map(Epoch).collect();

    for (name, r) in [("plain", &plain), ("with arenas and children", &heavy)] {
        let decoded = BranchRecord::deserialize(&old_bytes(r))
            .unwrap_or_else(|e| panic!("a pre-D60 {name} record did not load: {e}"));
        assert_eq!(decoded.depth, r.depth, "{name}: depth misread");
        assert_eq!(decoded.arenas.len(), r.arenas.len(), "{name}: arena count misread");
        assert_eq!(decoded.live_children, r.live_children, "{name}: live children misread");
        assert_eq!(decoded.branch_id, r.branch_id);
        assert_eq!(decoded.parent_id, r.parent_id);
        assert_eq!(decoded.lease_deadline, r.lease_deadline);
    }

    // And today's format round-trips a depth no byte could hold.
    let mut deep = plain.clone();
    deep.depth = 100_000;
    let back = BranchRecord::deserialize(&deep.serialize()).expect("round trip");
    assert_eq!(back.depth, 100_000, "the full record cannot carry a depth past a byte");
    // An old READER of that record sees the saturated byte rather than nonsense: byte at the
    // offset it has always been at, value clamped, everything after it where it expects.
    let wire = deep.serialize();
    assert_eq!(wire[50], u8::MAX, "the compatibility byte is not where an older build reads it");
}

/// **The REAPER down a deep chain — the path the test above does not take.**
///
/// `reclamation_walks_a_long_reaped_chain_without_recursing_per_level` asks the catalog directly.
/// `TwoTierReaper::detach_from_parent` has its own walk up the parent chain, and its termination
/// argument was, verbatim, "whose length is capped at `MAX_BRANCH_DEPTH`" — the cap D60 deleted.
/// It was still recursive; three fresh-context reviewers found it in one pass, and no test here
/// could have, because none of them ran a reap over a deep chain. This one does.
#[test]
fn the_reaper_cascades_up_a_deep_chain_of_reaped_ancestors() {
    use ferrodb::branch::arena::ArenaPageStore;
    use ferrodb::branch::reaper::TwoTierReaper;
    use ferrodb::branch::Reaper;
    use ferrodb::storage::disk_manager::DiskManager as Dm;

    // Same sizing argument as the catalog test: double the depth at which the recursive form was
    // measured to die (500 survives, 1,000 stack-overflows).
    const DEPTH: usize = 2_000;
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(true)
        .open(dir.path().join("reap.db")).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(Dm::new(file).unwrap())));
    let catalog: Arc<dyn BranchCatalog> =
        Arc::new(TableBranchCatalog::open_sidecar(&dir.path().join("reap.branchcat"), 1).unwrap());
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), base).unwrap());
    let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));

    let mut chain = Vec::with_capacity(DEPTH);
    let mut cur = BranchId::TRUNK;
    for level in 1..=DEPTH {
        cur = catalog
            .fork(cur, LeaseDeadline::from_now(600_000))
            .unwrap_or_else(|e| panic!("fork at depth {level} refused: {e}"))
            .branch_id;
        chain.push(cur);
    }

    // Reap top-down: every level but the last finds its own children still live, so it is marked
    // Reaped and stays attached. The LAST reap then has to cascade through all 1,999 reaped
    // ancestors above it in one call — which is the walk under test.
    //
    // **Run on a 512 KB stack, deliberately.** The recursive form does not overflow at 2,000 —
    // measured: it survives 2,000 on a normal test stack and dies at 12,000 — and a 12,000-level
    // chain costs minutes to build, which is not a price every suite run should pay to prove
    // this. A small stack amplifies the hazard the way oversubscription amplifies a race: the
    // iterative walk uses O(1) stack and does not care, the recursive one dies. Measured both
    // ways before this was written.
    let worker = std::thread::Builder::new()
        .stack_size(512 * 1024)
        .spawn(move || {
            for (i, b) in chain.iter().enumerate() {
                reaper
                    .reap(*b)
                    .unwrap_or_else(|e| panic!("reap of level {} failed: {e}", i + 1));
            }
        })
        .expect("spawn");
    worker.join().expect("the reap cascade died — a stack overflow aborts, so a failure here is a panic");

    // Everything is gone, and trunk is no longer pinned by any of it — which is what the cascade
    // exists to make true.
    assert!(
        !catalog.has_live_children(BranchId::TRUNK.id).unwrap(),
        "after reaping all {DEPTH} levels, trunk still reads as having a live child: the cascade \
         stopped short"
    );
}
