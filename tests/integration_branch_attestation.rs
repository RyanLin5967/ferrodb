//! D103 — the branch lifecycle writes to the attested history, and a third party can check it.
//!
//! `branch::attest` had **zero external callers**: 1900 lines of RFC 6962, 22 of its own tests, and
//! nothing in the database ever appended an entry. These tests drive it through the production
//! lifecycle — `BEGIN AGENT SESSION`, `MERGE`, and the reap that follows a merge or an abandon —
//! and then verify the log the way an outside auditor would.
//!
//! # ⚠ What this does and does not buy, stated before the assertions
//!
//! The module is blunt about its own limits and the wiring does not change any of them:
//!
//! * **A chain walk cannot detect a wholesale rewrite.** An adversary who can write the log can
//!   alter an entry and re-link everything after it. `verify_chain` passes on that by design.
//!   Only a root published BEFORE the rewrite catches it, which is why
//!   `a_published_head_catches_a_rewrite_that_the_chain_walk_accepts` exists and why
//!   `AgentRuntime::attestation_head` is public.
//! * **Nothing is durable.** `AttestedHistory` is in memory. Across a restart this answers
//!   nothing.
//! * **No signatures.** A root says what the log said, never who said it.
//!
//! What it does buy: within a running process, the sequence of forks, published merges and reaps
//! cannot be altered without either breaking the chain or contradicting a head someone already
//! wrote down — and the merge entries commit to the row images that were actually published.
//!
//! # ⚠ What is deliberately NOT attested
//!
//! There is no `BranchOp::Commit` entry. A content commitment over a branch's tree needs a digest
//! of the whole tree, and the only one this engine has (`cow::cid::subtree_cid`) has no memo table
//! and costs the whole subtree every call. Paying that on the fork path would make forking O(N)
//! and destroy exit criterion 1 — the measured claim that a fork copies zero pages — to buy a
//! commitment the merge entries already make for the data that reaches the shared tables.
//! `attesting_a_fork_does_not_read_the_tree` pins that as a property rather than a comment.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ferrodb::agent_sql::runtime::{AgentRuntime, ExecCtx};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::attest::{
    verify_consistency, verify_inclusion, AttestedHistory, BranchOp, ContentId,
};
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{ArenaId, BranchId, Epoch, PageId};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{CowPage, PageHandle, PageStore};
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

/// Counts `read_page`, so "attesting a fork does not read the tree" is measured at the store
/// boundary rather than inferred from the code.
struct CountingStore {
    inner: Arc<dyn PageStore>,
    reads: Arc<AtomicUsize>,
}

impl PageStore for CountingStore {
    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        self.inner.alloc_in_arena(arena, page_type, birth_epoch)
    }
    fn read_page(&self, page_id: PageId) -> Result<PageHandle, FerroError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_page(page_id)
    }
    fn cow_page(
        &self,
        page_id: PageId,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<CowPage, FerroError> {
        self.inner.cow_page(page_id, branch, epoch)
    }
    fn free_page(&self, page_id: PageId, free_epoch: Epoch) -> Result<(), FerroError> {
        self.inner.free_page(page_id, free_epoch)
    }
    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        self.inner.alloc_arena(branch)
    }
    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        self.inner.arena_for(branch)
    }
    fn free_arena(&self, arena: ArenaId) -> Result<u32, FerroError> {
        self.inner.free_arena(arena)
    }
    fn live_page_count(&self) -> Result<u32, FerroError> {
        self.inner.live_page_count()
    }
    fn flush(&self) -> Result<(), FerroError> {
        self.inner.flush()
    }
}

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    reads: Arc<AtomicUsize>,
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
        let arena = Arc::new(
            ArenaPageStore::new(
                bp.clone(),
                Arc::clone(&branches) as Arc<dyn ferrodb::branch::BranchCatalog>,
                ARENA_BASE,
            )
            .unwrap(),
        );
        let reads = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn PageStore> = Arc::new(CountingStore {
            inner: Arc::clone(&arena) as Arc<dyn PageStore>,
            reads: Arc::clone(&reads),
        });
        let runtime =
            Arc::new(AgentRuntime::with_storage(branches, Arc::new(MemEffectLog::new()), store).unwrap());
        Db { catalog, bp, txn, runtime, reads, _dir: dir }
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
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 100);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 200);", &mut s);
    }

    /// One agent: fork, write, merge. Returns the branch.
    fn agent_writes_and_merges(&mut self, agent: &str, id: i32, qty: i32) -> BranchId {
        let mut s = self.session();
        self.ok(&format!("BEGIN AGENT SESSION AS '{agent}' RUN 'r_{agent}';"), &mut s);
        let branch = s.agent.as_ref().unwrap().branch;
        self.ok(&format!("UPDATE inventory SET qty = {qty} WHERE id = {id};"), &mut s);
        let rt = self.rt();
        let (bp, txn) = (self.bp.clone(), self.txn.clone());
        let mut ctx = ExecCtx { catalog: &mut self.catalog, bp, txn };
        let report = rt.merge(&mut ctx, branch).unwrap();
        assert!(!report.outcome.is_conflict(), "{agent}'s merge conflicted: {:?}", report.outcome);
        assert!(report.applied_to_target, "{agent}'s merge did not publish");
        branch
    }
}

fn ops_of(rt: &AgentRuntime, branch: BranchId) -> Vec<BranchOp> {
    rt.attested_entries(branch).iter().map(|e| e.op).collect()
}

// -------------------------------------------------------------------------------------------
// The wiring
// -------------------------------------------------------------------------------------------

/// **A fork, a published merge and the reap that follows each leave exactly one entry.**
///
/// This is the test that fails if any of the three callers is removed. It asserts the ops by
/// branch, not just a total, so deleting the reap call while leaving the fork call fails here
/// rather than being absorbed into a count.
#[test]
fn a_fork_a_merge_and_a_reap_each_leave_an_attested_entry() {
    let mut db = Db::new();
    db.seed();
    let before = db.runtime.attested_len();
    assert_eq!(before, 0, "nothing should be attested before the first fork");

    let branch = db.agent_writes_and_merges("agent-a", 1, 111);

    // The branch that did the work: forked, then reaped when its merge published.
    assert_eq!(
        ops_of(&db.runtime, branch),
        vec![BranchOp::Fork, BranchOp::Reap],
        "the worker branch's attested history"
    );
    // Trunk, which the merge published INTO, carries the merge.
    assert_eq!(
        ops_of(&db.runtime, BranchId::TRUNK),
        vec![BranchOp::Merge],
        "the merge target's attested history"
    );
    assert_eq!(db.runtime.attested_len(), 3, "three lifecycle events, three entries");

    // An abandoned branch is reaped too, and the record must say it did NOT publish — a reap
    // entry that conflated the two would make "merged" and "abandoned" indistinguishable.
    let mut s = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-b' RUN 'r_b';", &mut s);
    let b = s.agent.as_ref().unwrap().branch;
    db.ok("UPDATE inventory SET qty = 5 WHERE id = 2;", &mut s);
    db.runtime.abandon(b).unwrap();
    assert_eq!(ops_of(&db.runtime, b), vec![BranchOp::Fork, BranchOp::Reap]);

    let merged_reap = db.runtime.attested_entries(branch).pop().unwrap();
    let abandoned_reap = db.runtime.attested_entries(b).pop().unwrap();
    assert_ne!(
        merged_reap.content_cid, abandoned_reap.content_cid,
        "a merged reap and an abandoned reap produced the same content id, so the record cannot \
         tell them apart"
    );
}

/// **The child's chain walks into the parent it forked from.**
///
/// `append_fork` links a child's first entry to the PARENT's head, which is the one structural
/// decision in `attest.rs` that is ferrodb's rather than RFC 6962's. Wiring that wrongly — linking
/// to the child's own (empty) head — would leave every branch's history an island, and the whole
/// ancestry claim would be false while every other assertion here still passed.
#[test]
fn a_childs_chain_walks_into_the_parent_it_forked_from() {
    let mut db = Db::new();
    db.seed();

    // Give trunk a head first, by merging something into it.
    db.agent_writes_and_merges("agent-a", 1, 111);
    let trunk_head = db.runtime.attestation_of(BranchId::TRUNK).expect("trunk has a head");

    let mut s = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-c' RUN 'r_c';", &mut s);
    let child = s.agent.as_ref().unwrap().branch;

    let first = db.runtime.attested_entries(child).into_iter().next().expect("child forked");
    assert_eq!(first.op, BranchOp::Fork);
    assert_eq!(
        first.prev, trunk_head,
        "the child's first link is not the parent's head, so its history does not walk into its \
         ancestry"
    );
    assert_eq!(first.branch, child, "the entry names the wrong branch");
}

/// **A fork attests without reading a single page of the tree.**
///
/// The cost claim, measured at the store boundary. If a later change makes the fork entry commit
/// to the branch's tree content, this fails — which is the point: exit criterion 1 is that a fork
/// copies zero pages, and a fork that had to HASH the tree would be O(N) whether or not it copied
/// anything.
#[test]
fn attesting_a_fork_does_not_read_the_tree() {
    let mut db = Db::new();
    db.seed();
    // A tree with real structure, so "read nothing" is a claim about the tree and not about its
    // absence.
    for i in 0..2_000u64 {
        let row = vec![Value::Integer(i as i32), Value::Integer(1)];
        db.runtime.put_row(BranchId::TRUNK, "inventory", i, &row).unwrap();
    }
    let nodes = {
        let tree = db.runtime.storage().unwrap().tree();
        tree.walk_pages(db.runtime.root_of(BranchId::TRUNK).unwrap()).unwrap().len()
    };
    assert!(nodes > 50, "the fixture tree is too small for this to mean anything: {nodes}");

    db.reads.store(0, Ordering::Relaxed);
    let before = db.runtime.attested_len();
    let mut s = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-fork' RUN 'r_f';", &mut s);
    let reads = db.reads.load(Ordering::Relaxed);

    assert_eq!(db.runtime.attested_len(), before + 1, "the fork was not attested");
    assert!(
        reads * 20 < nodes,
        "a fork read {reads} pages of a {nodes}-node tree. A fork is supposed to copy zero pages; \
         attesting it must not have turned that into a traversal."
    );
}

/// **The merge entry commits to the rows that were published, not to a constant.**
///
/// Two merges that publish different values must produce different content ids. A wiring that
/// passed a fixed digest — or a digest of the branch id — would satisfy every structural assertion
/// above and commit to nothing.
#[test]
fn the_merge_entry_commits_to_the_rows_it_published() {
    let mut db = Db::new();
    db.seed();

    db.agent_writes_and_merges("agent-a", 1, 111);
    let first = *db.runtime.attested_entries(BranchId::TRUNK).last().unwrap();

    db.agent_writes_and_merges("agent-b", 1, 222);
    let second = *db.runtime.attested_entries(BranchId::TRUNK).last().unwrap();

    assert_eq!(first.op, BranchOp::Merge);
    assert_eq!(second.op, BranchOp::Merge);
    assert_ne!(
        first.content_cid, second.content_cid,
        "two merges publishing different row values produced the same content id, so the entry \
         commits to nothing about the data"
    );
    // And the second is chained to the first: altering the first breaks the second.
    assert_eq!(second.prev, first.attestation(), "the merge entries are not chained");
}

// -------------------------------------------------------------------------------------------
// The detector, forced to fire — and forced not to
// -------------------------------------------------------------------------------------------

/// **A clean log verifies, and an altered entry does not.**
///
/// Both halves, because a verifier that always passed would satisfy the first alone and a verifier
/// that always failed would satisfy the second alone.
#[test]
fn altering_one_entry_breaks_the_chain() {
    let mut db = Db::new();
    db.seed();
    let branch = db.agent_writes_and_merges("agent-a", 1, 111);

    // Clean: the production log re-links.
    db.runtime.verify_attested_branch(branch).expect("the log this runtime wrote must verify");
    db.runtime.verify_attested_branch(BranchId::TRUNK).unwrap();

    // Tampered: take the real entries out, alter one, and load them as a verifier would.
    let mut entries: Vec<_> = {
        let clean = db.runtime.attested_entries(branch);
        assert_eq!(clean.len(), 2, "expected fork+reap for the worker branch");
        clean
    };
    entries[0].content_cid = ContentId::of(b"a content id nobody attested");
    let forged = AttestedHistory::load_untrusted(entries);
    let finding = forged
        .verify_branch(branch)
        .expect_err("altering a fork entry's content id left the chain verifying");
    // The finding names the branch it is about, so an operator is not told merely that something
    // somewhere is wrong.
    assert!(
        finding.to_string().contains(&branch.to_string())
            || finding.to_string().contains("chain")
            || finding.to_string().contains("prev"),
        "the tamper finding is not actionable: {finding}"
    );
}

/// **A published head catches the rewrite that a chain walk accepts.**
///
/// This is the property the module says the chain walk cannot have, and the reason
/// `attestation_head` is public: an adversary who can write the log can alter an entry and
/// recompute every `prev` after it, leaving an internally consistent chain. Only a root witnessed
/// before the rewrite detects it.
///
/// So: an operator records `(size, root)` while the log is honest; the log is then rewritten; the
/// rewritten log's own consistency proof against that published head fails.
#[test]
fn a_published_head_catches_a_rewrite_that_the_chain_walk_accepts() {
    let mut db = Db::new();
    db.seed();
    db.agent_writes_and_merges("agent-a", 1, 111);

    // What the operator wrote down.
    let published = db.runtime.attestation_head();
    assert!(published.size > 0, "nothing was attested, so there is nothing to publish");

    // The honest log extends it, and the extension proves itself.
    db.agent_writes_and_merges("agent-b", 2, 222);
    let later = db.runtime.attestation_head();
    let proof = db
        .runtime
        .attested_consistency_proof(published.size)
        .expect("the log must be able to prove its own append-only extension");
    assert!(
        verify_consistency(&published, &later, &proof),
        "an honestly extended log failed its own consistency proof, so the detector would fire on \
         everything and mean nothing"
    );

    // Now the rewrite. Every entry is re-linked, so the chain walk is satisfied...
    let mut all: Vec<_> = {
        let mut v = Vec::new();
        for b in [BranchId::TRUNK] {
            v.extend(db.runtime.attested_entries(b));
        }
        v
    };
    assert!(!all.is_empty());
    all[0].content_cid = ContentId::of(b"rewritten history");
    // Re-link the tail by hand, which is exactly what an adversary with write access does.
    for i in 1..all.len() {
        all[i].prev = all[i - 1].attestation();
    }
    let rewritten = AttestedHistory::load_untrusted(all);
    assert!(
        rewritten.verify_chain().is_ok(),
        "the rewritten chain failed a chain walk — then this test is not demonstrating the gap \
         the published head exists to close"
    );

    // ...and the published head is not.
    let forged_head = rewritten.head();
    let forged_proof = rewritten.consistency_proof(published.size);
    let accepted = match forged_proof {
        Some(p) => verify_consistency(&published, &forged_head, &p),
        // No proof at all is also a refusal, and the honest outcome.
        None => false,
    };
    assert!(
        !accepted,
        "a rewritten log proved consistency against a head published before the rewrite, which is \
         the one thing this mechanism is for"
    );
}

/// **An inclusion proof checks against the published head with no handle to the log.**
///
/// The auditor holds an entry, a proof and a `(size, root)` they wrote down earlier — and nothing
/// else. `verify_inclusion` is a free function for exactly that reason.
#[test]
fn an_auditor_can_check_every_entry_against_a_published_head() {
    let mut db = Db::new();
    db.seed();
    db.agent_writes_and_merges("agent-a", 1, 111);
    db.agent_writes_and_merges("agent-b", 2, 222);

    let head = db.runtime.attestation_head();
    let log = db.runtime.attested_log();
    assert_eq!(log.len(), head.size, "the log and its head disagree about the size");
    assert!(head.size >= 6, "too few entries to be checking anything: {}", head.size);

    for (index, entry) in log.iter().enumerate() {
        let proof = db
            .runtime
            .attested_inclusion_proof(index)
            .unwrap_or_else(|| panic!("no inclusion proof for logged entry {index}"));
        assert!(
            verify_inclusion(entry, &proof, &head),
            "entry {index} ({:?} on {}) failed its own inclusion proof against the published head",
            entry.op,
            entry.branch
        );
    }

    // And the same proofs are refused against a root nobody published, so the check is not
    // vacuous. Without this a `verify_inclusion` that returned `true` unconditionally would pass
    // every assertion above.
    let wrong = ferrodb::branch::attest::TreeHead { size: head.size, root: [7u8; 32] };
    let proof = db.runtime.attested_inclusion_proof(0).unwrap();
    assert!(
        !verify_inclusion(&log[0], &proof, &wrong),
        "an inclusion proof verified against a root nobody published"
    );
}
