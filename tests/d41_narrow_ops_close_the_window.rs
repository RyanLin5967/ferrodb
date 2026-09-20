//! **D41 — the two reproductions for the sites that had none.**
//!
//! `collapse`'s window (ledger row D29) was reproduced in-crate by
//! `branch::reaper::tests::*::collapse_discards_a_lease_renewal_that_lands_on_its_re_read`, which
//! D41 fixed and D63 then deleted along with `collapse` itself.
//!
//! ⚠ **Nothing in THIS file exercises `reparent`** — site 1's narrow op. The `reparent` below is a
//! bare forward that exists to satisfy `BranchCatalog`; the two reproductions here drive
//! `restrict_envelope` and `charge_row_writes`. What covers `reparent` is
//! `branch::catalog::tests::reparent_moves_the_four_position_fields_and_nothing_else` and one case
//! in `branch::table_catalog::tests`. Said explicitly because a comment in `branch/mod.rs` used to
//! claim this file pinned it, and it never did.
//!
//! The other two exposed read-modify-writes on a branch record had nothing driving them:
//!
//! * **Site 2 — `AgentRuntime::restrict_branch`.** `get` -> [`BranchRecord::restrict`] -> `put`
//!   compared the new envelope against a SNAPSHOT and then wrote the whole record back. Two
//!   restrictions racing therefore left the LATER WRITER's envelope standing whether or not it was
//!   narrower than what had already been accepted — a widening reached by losing a write rather
//!   than by being granted one, which is the direction a capability system must not fail in.
//!
//! * **Site 3 — `AgentRuntime::quarantine`.** Same shape, and its window contains the write
//!   funnel: a `charge_row_writes` landing between the `get` and the `put` was written over with
//!   a record whose envelope had not been charged, so a governed branch got those row-writes for
//!   free.
//!
//! # How these are kept from passing vacuously, which is most of the design
//!
//! **A catalog DECORATOR, never `thread::spawn`.** A racing test fires sometimes, and a test that
//! fires sometimes reports green when the interleaving did not happen. The decorator makes the
//! interleaving a *fact* of the test; the `fired` flag makes it refuse to pass without one.
//!
//! **It intercepts reads, never the write under test.** Intercepting the write would observe the
//! clobber instead of causing it, which tests the test.
//!
//! **Each case has a DISARMED CONTROL ARM.** Without one, a test that does not fire cannot tell
//! "the code is already safe" from "my harness is broken": both look like a green line. The
//! control runs the identical sequence with the racer switched off and asserts the opposite
//! outcome — the second restriction IS in force, the envelope has NOT been charged — so the armed
//! arm's result can only have come from the interleaving.
//!
//! **Site 2 asserts the VERB MASK, not that the envelope changed.** The defect is a widening
//! relative to what was granted; an assertion of equality with the outer envelope would also hold
//! if the code were correct-but-different. The budgets are held equal across all three envelopes
//! here precisely so that the refusal can only be about verbs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::record::{BranchRecord, CapabilityEnvelope, CoreRecord, Verb};
use ferrodb::branch::types::{
    ArenaId, BranchId, BranchState, Epoch, LeaseDeadline, PageId,
};
use ferrodb::branch::BranchCatalog;
use ferrodb::error::FerroError;

/// What the racer does when it lands inside the operation under test.
enum Landing {
    /// The write funnel spends part of the branch's row-write budget. Site 3.
    Charge(u64),
    /// Somebody else narrows the same envelope first. Site 2.
    Restrict(CapabilityEnvelope),
}

/// A catalog that performs one concurrent mutation from inside the operation under test.
///
/// It fires **once**, on the first read of the subject the operation makes — `get` for the code
/// that reads a whole record, `restrict_envelope` for the code that does not read one at all. One
/// decorator serving both spellings is what lets the same test be run against the code before and
/// after D41: it fires in exactly one place either way, and the place is the window.
struct Racer {
    inner: Arc<dyn BranchCatalog>,
    subject: Mutex<Option<BranchId>>,
    armed: AtomicBool,
    fired: AtomicBool,
    landing: Landing,
}

impl Racer {
    fn new(inner: Arc<dyn BranchCatalog>, landing: Landing) -> Racer {
        Racer {
            inner,
            subject: Mutex::new(None),
            armed: AtomicBool::new(false),
            fired: AtomicBool::new(false),
            landing,
        }
    }

    /// Arm for one landing on `branch`. Until this is called the decorator is a pass-through, which
    /// is what the control arm runs on.
    fn arm(&self, branch: BranchId) {
        *self.subject.lock().unwrap() = Some(branch);
        self.armed.store(true, Ordering::SeqCst);
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    /// One shot, and the swap is what makes it one: a second read of the same branch inside the
    /// same operation must not land a second mutation, or what the test drove stops being one
    /// interleaving.
    fn maybe_land(&self, b: BranchId) {
        if *self.subject.lock().unwrap() != Some(b) {
            return;
        }
        if !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        match &self.landing {
            Landing::Charge(n) => self.inner.charge_row_writes(b, *n).expect("the racing charge"),
            Landing::Restrict(env) => self
                .inner
                .restrict_envelope(b, env.clone())
                .expect("the racing restriction"),
        }
        self.fired.store(true, Ordering::SeqCst);
    }
}

impl BranchCatalog for Racer {
    fn get(&self, b: BranchId) -> Result<BranchRecord, FerroError> {
        // The answer is taken FIRST and the landing happens after, so what the caller holds is the
        // pre-landing record. That is the defect's precondition, and producing it here is the
        // whole job: a decorator that landed first would hand the caller fresh data and prove
        // nothing.
        let answer = self.inner.get(b)?;
        self.maybe_land(b);
        Ok(answer)
    }

    fn restrict_envelope(
        &self,
        b: BranchId,
        envelope: CapabilityEnvelope,
    ) -> Result<(), FerroError> {
        self.maybe_land(b);
        self.inner.restrict_envelope(b, envelope)
    }

    fn next_epoch(&self) -> Epoch {
        self.inner.next_epoch()
    }
    fn current_epoch(&self) -> Epoch {
        self.inner.current_epoch()
    }
    fn fork(&self, p: BranchId, l: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        self.inner.fork(p, l)
    }
    fn reparent(
        &self,
        b: BranchId,
        p: BranchId,
        e: Epoch,
        r: PageId,
    ) -> Result<BranchRecord, FerroError> {
        self.inner.reparent(b, p, e, r)
    }
    fn set_state(
        &self,
        b: BranchId,
        expect: BranchState,
        to: BranchState,
    ) -> Result<(), FerroError> {
        self.inner.set_state(b, expect, to)
    }
    fn set_root(&self, b: BranchId, r: PageId) -> Result<(), FerroError> {
        self.inner.set_root(b, r)
    }
    fn expired_before(&self, n: u64) -> Result<Vec<CoreRecord>, FerroError> {
        self.inner.expired_before(n)
    }
    fn in_state(&self, s: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
        self.inner.in_state(s)
    }
    fn scan(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        self.inner.scan()
    }
    fn max_live_child(&self, p: u64) -> Result<Option<Epoch>, FerroError> {
        self.inner.max_live_child(p)
    }
    fn live_child_in_epoch_range(
        &self,
        p: u64,
        lo: Epoch,
        hi: Epoch,
    ) -> Result<bool, FerroError> {
        self.inner.live_child_in_epoch_range(p, lo, hi)
    }
    fn has_live_children(&self, p: u64) -> Result<bool, FerroError> {
        self.inner.has_live_children(p)
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
    fn attach_child(&self, p: u64, e: Epoch, c: u64) -> Result<(), FerroError> {
        self.inner.attach_child(p, e, c)
    }
    fn detach_child(&self, p: u64, e: Epoch) -> Result<bool, FerroError> {
        self.inner.detach_child(p, e)
    }
    fn add_arena(&self, b: BranchId, a: ArenaId) -> Result<(), FerroError> {
        self.inner.add_arena(b, a)
    }
    fn renew_lease(&self, b: BranchId, l: LeaseDeadline) -> Result<(), FerroError> {
        self.inner.renew_lease(b, l)
    }
    /// Delegated rather than inherited. The trait's default is `self.get(branch)?.envelope`, which
    /// would route the test's own assertions back through the racing `get` above.
    fn envelope_of(&self, b: BranchId) -> Result<Option<CapabilityEnvelope>, FerroError> {
        self.inner.envelope_of(b)
    }
    fn charge_row_writes(&self, b: BranchId, n: u64) -> Result<(), FerroError> {
        self.inner.charge_row_writes(b, n)
    }
}

/// A runtime over a racing catalog, plus one governed live branch.
fn fixture(landing: Landing) -> (Arc<AgentRuntime>, Arc<Racer>, BranchId) {
    let inner: Arc<dyn BranchCatalog> = Arc::new(LogBranchCatalog::in_memory(1));
    let racer = Arc::new(Racer::new(inner, landing));
    let rt = Arc::new(AgentRuntime::with_catalog(Arc::clone(&racer) as Arc<dyn BranchCatalog>));
    let branch = rt
        .branches()
        .fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000))
        .expect("fork")
        .branch_id;
    // The branch starts GOVERNED, with every verb and a budget. Both cases need something in the
    // envelope for the window to be able to lose.
    rt.restrict_branch(branch, CapabilityEnvelope::new(Verb::ALL, 1_000))
        .expect("install the first envelope");
    (rt, racer, branch)
}

// ---- SITE 3: quarantine -------------------------------------------------------------------

/// **A row-write charge that lands while a branch is being quarantined must survive it.**
///
/// The worst arm of site 3, and the reason it is the worst: the window contains the write funnel.
/// `charge_row_writes` is the only thing that spends a governed branch's budget, and a whole-record
/// `put` over the top of it hands the branch those row-writes back — the branch is billed for
/// nothing and may write again. `quarantine` is reachable by any verification gate, so this is not
/// a rare interleaving.
#[test]
fn a_row_write_charge_is_not_discarded_by_quarantine() {
    let (rt, racer, branch) = fixture(Landing::Charge(7));
    racer.arm(branch);

    rt.quarantine(branch, "the gate declined this branch").expect("quarantine");

    assert!(
        racer.fired(),
        "the charge never landed inside quarantine's window, so this test proves nothing either way"
    );
    let env = rt.envelope_of(branch).expect("envelope_of").expect("the branch is governed");
    assert_eq!(
        env.row_writes(),
        7,
        "quarantine wrote the branch's whole record back and discarded a row-write charge that \
         landed in its window. The branch has spent 7 row-writes and its envelope says 0, so a \
         governed branch got them for free — the direction a capability system must not fail in."
    );
    assert_eq!(
        rt.branches().get(branch).expect("get").state,
        BranchState::Quarantined,
        "the hold itself did not happen, so the assertion above is about the wrong thing"
    );
}

/// **The disarmed control.** Same sequence, racer off: the charge is the only possible source of a
/// non-zero spend, and the hold still happens. Without this, a green line above could equally mean
/// the harness never ran.
#[test]
fn control_quarantine_with_no_racer_leaves_the_budget_unspent() {
    let (rt, racer, branch) = fixture(Landing::Charge(7));
    // deliberately not armed

    rt.quarantine(branch, "the gate declined this branch").expect("quarantine");

    assert!(!racer.fired(), "the racer fired without being armed; the control arm is not a control");
    let env = rt.envelope_of(branch).expect("envelope_of").expect("the branch is governed");
    assert_eq!(env.row_writes(), 0, "something other than the racer spent the branch's budget");
    assert_eq!(
        rt.branches().get(branch).expect("get").state,
        BranchState::Quarantined,
        "quarantine did not hold the branch even with nothing racing it"
    );
}

// ---- SITE 2: restrict_branch --------------------------------------------------------------

/// **A narrowing must be measured against the envelope IN FORCE, not against the one its caller
/// read.**
///
/// Two restrictions race. The one that lands first cuts the branch to INSERT only; the one under
/// test would cut it to INSERT|UPDATE, which is narrower than what *it* read (every verb) and
/// WIDER than what is now in force. Through `get`/`restrict`/`put` the comparison was against the
/// snapshot, so the second write won and the branch could UPDATE again — authority reinstated by
/// losing a write.
///
/// **The assertion is the verb mask.** Every envelope here carries the same budget, so the only
/// thing that can differ is which verbs are granted, and the only thing that can refuse the second
/// restriction is the first one still being in force.
#[test]
fn a_restriction_may_not_be_widened_by_one_that_raced_it() {
    let insert_only = CapabilityEnvelope::new(Verb::INSERT, 1_000);
    let (rt, racer, branch) = fixture(Landing::Restrict(insert_only));
    racer.arm(branch);

    // Narrower than what this caller read (ALL), wider than what lands inside the window.
    let outer = rt.restrict_branch(branch, CapabilityEnvelope::new(
        Verb::INSERT | Verb::UPDATE,
        1_000,
    ));

    assert!(
        racer.fired(),
        "the competing restriction never landed, so this test proves nothing either way"
    );
    let env = rt.envelope_of(branch).expect("envelope_of").expect("the branch is governed");
    assert_eq!(
        env.verbs() & Verb::UPDATE,
        0,
        "a restriction to INSERT-only was already in force, and the branch may UPDATE again. Its \
         authority was widened by a write that lost a race rather than by anyone granting it; \
         verbs are now {:#05b}",
        env.verbs()
    );
    assert!(
        outer.is_err(),
        "the widening was applied and reported as success, so no caller could even find out"
    );
    assert!(
        outer.unwrap_err().to_string().contains("UPDATE"),
        "the refusal must name the verb it refused, or an operator cannot act on it"
    );
}

/// **The disarmed control.** Same sequence, racer off: the second restriction is accepted and IS in
/// force. This is what tells the arm above apart from a harness that silently did nothing — and
/// from a `restrict_branch` that refuses everything, which would also make the armed assertion
/// pass.
#[test]
fn control_a_restriction_with_no_racer_is_applied() {
    let insert_only = CapabilityEnvelope::new(Verb::INSERT, 1_000);
    let (rt, racer, branch) = fixture(Landing::Restrict(insert_only));
    // deliberately not armed

    rt.restrict_branch(branch, CapabilityEnvelope::new(Verb::INSERT | Verb::UPDATE, 1_000))
        .expect("an ordinary narrowing must be accepted");

    assert!(!racer.fired(), "the racer fired without being armed; the control arm is not a control");
    let env = rt.envelope_of(branch).expect("envelope_of").expect("the branch is governed");
    assert_eq!(
        env.verbs(),
        Verb::INSERT | Verb::UPDATE,
        "the second restriction was not in force with nothing racing it, so the armed arm above \
         cannot distinguish the fix from a `restrict_branch` that refuses everything"
    );
}
