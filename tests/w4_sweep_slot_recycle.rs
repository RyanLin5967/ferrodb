//! W4 — `forget_reaped_branches` must not delete a LIVE agent's workspace.
//!
//! The sweep asks the catalog about each candidate with the state lock **released**, because the
//! catalog takes its own lock and the two are acquired in the other order elsewhere. That window
//! is not free: the catalog recycles a reaped branch's id SLOT (`release_id` pushes it back and
//! `fork` pops it), and both `State::workspaces` and `State::names` are keyed by the slot alone —
//! a session forking into slot 5 takes the same map key and the same `b_5` name as the branch that
//! just died there. Only the generation tells them apart.
//!
//! So a sweep that acts on what the catalog said, without re-reading, removes the workspace of a
//! branch it never asked about. The agent holding that session then gets "no agent session on
//! branch b_5" for a session it opened successfully and never closed.
//!
//! **Why a seam and not a stress test.** The window is microseconds wide. A test that forked in a
//! loop and hoped to land inside it would report luck, and would go quiet the moment the timing
//! shifted — the failure mode this file exists to prevent. `BranchCatalog::get` is called from
//! exactly one place in the sweep, phase 2, so a catalog that forks when it is asked about a dead
//! branch puts the new session in the window every time, by construction.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::session::AgentSession;
use ferrodb::branch::record::{BranchRecord, CoreRecord};
use ferrodb::branch::types::{BranchId, BranchState, Epoch, LeaseDeadline, PageId};
use ferrodb::agent_sql::runtime::BranchResolver;
use ferrodb::branch::{BranchCatalog, LogBranchCatalog};
use ferrodb::error::FerroError;
use ferrodb::tel::ids::{ColId, RowId};

const QTY: ColId = ColId(1);

/// A catalog that forks one replacement session the first time the sweep asks about a branch the
/// catalog no longer has — i.e. exactly inside phase 2's lock-free window.
struct ForkInTheWindow {
    inner: Arc<LogBranchCatalog>,
    /// Set after the runtime exists; the runtime owns this catalog, so the edge has to be weak.
    runtime: OnceLock<Weak<AgentRuntime>>,
    armed: AtomicBool,
    /// The session forked inside the window, for the test to assert on afterwards.
    forked: Mutex<Option<AgentSession>>,
}

impl ForkInTheWindow {
    fn new(inner: Arc<LogBranchCatalog>) -> ForkInTheWindow {
        ForkInTheWindow {
            inner,
            runtime: OnceLock::new(),
            armed: AtomicBool::new(false),
            forked: Mutex::new(None),
        }
    }
}

impl BranchCatalog for ForkInTheWindow {
    fn get(&self, branch: BranchId) -> Result<BranchRecord, FerroError> {
        // The guard inside `inner` is dropped before anything below runs: forking re-enters this
        // catalog and would deadlock against a read guard still held here.
        let answer = self.inner.get(branch);
        if answer.is_err() && self.armed.swap(false, Ordering::SeqCst) {
            let rt = self
                .runtime
                .get()
                .and_then(Weak::upgrade)
                .expect("runtime wired before the sweep runs");
            // This lands between phase 2 and phase 3 with the state lock free, which is where a
            // real client's `BEGIN` lands when the lease thread is sweeping.
            let sess = rt.begin_session("racer", Some("in-the-window"), BranchId::TRUNK).unwrap();
            // The reborn branch takes a claim of its own INSIDE the window. Releasing the dead
            // branch's escrow must not touch this one -- that is what keying the ledger by the
            // whole `BranchId` buys, and asserting only the release would not notice if it did.
            rt.claim_escrow(sess.branch, "inventory", RowId(1), QTY, 3).expect("racer claims");
            *self.forked.lock().unwrap() = Some(sess);
        }
        answer
    }

    fn next_epoch(&self) -> Epoch {
        self.inner.next_epoch()
    }
    fn current_epoch(&self) -> Epoch {
        self.inner.current_epoch()
    }
    fn fork(&self, parent: BranchId, lease: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        self.inner.fork(parent, lease)
    }
    fn put(&self, record: &BranchRecord) -> Result<(), FerroError> {
        self.inner.put(record)
    }
    fn set_root(&self, branch: BranchId, root: PageId) -> Result<(), FerroError> {
        self.inner.set_root(branch, root)
    }
    fn expired_before(&self, now_millis: u64) -> Result<Vec<CoreRecord>, FerroError> {
        self.inner.expired_before(now_millis)
    }
    fn in_state(&self, state: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
        self.inner.in_state(state)
    }
    fn scan(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        self.inner.scan()
    }
    fn max_live_child(&self, parent_id: u64) -> Result<Option<Epoch>, FerroError> {
        self.inner.max_live_child(parent_id)
    }
    fn live_child_in_epoch_range(
        &self,
        parent_id: u64,
        lo: Epoch,
        hi: Epoch,
    ) -> Result<bool, FerroError> {
        self.inner.live_child_in_epoch_range(parent_id, lo, hi)
    }
    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        self.inner.has_live_children(parent_id)
    }
    fn live_count(&self) -> usize {
        self.inner.live_count()
    }
    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        self.inner.get_raw(id)
    }
    fn release_id(&self, id: u64) {
        self.inner.release_id(id)
    }
    fn attach_child(
        &self,
        parent_id: u64,
        fork_epoch: Epoch,
        child_id: u64,
    ) -> Result<(), FerroError> {
        self.inner.attach_child(parent_id, fork_epoch, child_id)
    }
    fn detach_child(&self, parent_id: u64, fork_epoch: Epoch) -> Result<bool, FerroError> {
        self.inner.detach_child(parent_id, fork_epoch)
    }
    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        self.inner.renew_lease(branch, lease)
    }
    fn charge_row_writes(&self, branch: BranchId, rows: u64) -> Result<(), FerroError> {
        self.inner.charge_row_writes(branch, rows)
    }
}

/// **The one that would lose data.** A session forked into a recycled slot while the sweep was
/// mid-flight must survive the sweep.
///
/// The assertion is on the NEW session being usable afterwards, not on the sweep's return count:
/// a count is satisfied by removing the wrong thing, and what this is about is which workspace
/// went. Forcing it to fire is a two-line experiment — delete the `still_ours` re-validation in
/// `forget_reaped_branches` and this test reports the new session's branch as having no workspace.
#[test]
fn a_session_that_recycled_a_reaped_slot_survives_a_sweep_already_in_flight() {
    let inner = Arc::new(LogBranchCatalog::in_memory(1));
    let catalog = Arc::new(ForkInTheWindow::new(Arc::clone(&inner)));
    let rt = Arc::new(AgentRuntime::with_catalog(Arc::clone(&catalog) as Arc<dyn BranchCatalog>));
    catalog.runtime.set(Arc::downgrade(&rt)).ok().expect("wire the runtime once");

    // One doomed session. Its workspace stays behind when the branch leaves the catalog, which is
    // the whole reason `forget_reaped_branches` exists.
    let doomed = rt.begin_session("doomed", Some("r0"), BranchId::TRUNK).unwrap();
    let slot = doomed.branch.id;
    assert_eq!(doomed.branch.generation, 0, "a fresh slot starts at generation 0");

    // A pool with the doomed branch holding part of it. A reaped branch that never gives its
    // claim back strands that headroom for everyone else, for the life of the process.
    rt.open_escrow("inventory", RowId(1), QTY, 20).unwrap();
    rt.claim_escrow(doomed.branch, "inventory", RowId(1), QTY, 12).unwrap();
    assert_eq!(rt.unclaimed_escrow("inventory", RowId(1), QTY), Some(8));

    // Reap it behind the runtime's back and hand the slot back to the allocator, exactly as the
    // lease reaper does with no client cooperation at all.
    let mut rec = rt.branches().get(doomed.branch).unwrap();
    rec.mark_reaped();
    rt.branches().put(&rec).unwrap();
    rt.branches().release_id(slot);
    assert!(rt.branches().get(doomed.branch).is_err(), "the doomed branch is gone from the catalog");

    // Arm the seam: the sweep's phase-2 lookup for `doomed` will fork a new session, and the
    // allocator pops the slot it just released.
    catalog.armed.store(true, Ordering::SeqCst);
    let forgotten = rt.forget_reaped_branches();

    let racer = catalog.forked.lock().unwrap().take().expect("the seam forked inside the window");
    assert_eq!(racer.branch.id, slot, "the test needs the SLOT recycled; it was not");
    assert_eq!(racer.branch.generation, 1, "a recycled slot must come back at a new generation");

    // The property. `blind_writes` is a plain read that needs the branch's workspace, so it is
    // the shortest statement that answers "does this agent still have its session".
    assert!(
        rt.blind_writes(racer.branch).is_ok(),
        "the sweep deleted the workspace of a LIVE session that recycled slot {slot}: it acted on \
         what the catalog said about generation 0 without re-reading, and generation 1 was there \
         by then"
    );
    assert_eq!(
        rt.resolve_branch(&format!("b_{slot}")).ok(),
        Some(racer.branch),
        "b_{slot} must name the live branch, not be unbound by the sweep"
    );

    // **The dead branch's claim went back to the pool, and the reborn branch's did not.** The
    // sweep refuses to remove the live workspace, and the first version of that refusal skipped
    // the escrow release along with everything else -- leaving 12 units held by a branch that no
    // longer exists and that nothing alive could ever release. 20 - 3 = 17: the doomed branch's 12
    // returned, the racer's own 3 (claimed inside the window) untouched.
    assert_eq!(
        rt.unclaimed_escrow("inventory", RowId(1), QTY),
        Some(17),
        "dead branch's 12 units must return to the pool and the racer's 3 must not"
    );
    assert_eq!(
        rt.remaining_escrow(racer.branch, "inventory", RowId(1), QTY),
        Some(3),
        "the reborn branch must keep its own claim"
    );

    // And the sweep did not quietly remove something else instead. The slot now belongs to the
    // racer at generation 1 and to nobody else; the doomed branch's identity is gone even though
    // its map KEY was immediately taken over.
    assert!(forgotten <= 1, "a one-branch fixture cannot forget more than one, got {forgotten}");
    let live: Vec<BranchId> = rt.run_activity().into_iter().map(|a| a.branch).collect();
    assert_eq!(live, vec![racer.branch], "exactly the racer's branch should be live");

    // **A neighbouring defect this test deliberately does NOT assert away.** `blind_writes` and
    // every other `workspaces.get(&branch.id)` lookup is keyed by the id SLOT alone, so a caller
    // holding the STALE `BranchId` (generation 0) is answered about the slot's new occupant
    // instead of being refused:
    //
    //     rt.blind_writes(doomed.branch).is_ok()   // true, and it is the racer's workspace
    //
    // The catalog refuses a stale generation (`BranchError::Reaped`); the runtime's workspace map
    // has no such check. That is a pre-existing generation-blindness, not something the sweep
    // introduced, and closing it means a generation check at every workspace lookup rather than a
    // change here. Recorded at the point that measured it. The assertion above is written against
    // `run_activity`, which derives its branch from `names` and so does carry the generation.
}
