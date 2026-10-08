//! Test-only: a [`BranchCatalog`] that forwards every call to a real catalog, except where a test
//! arms a fault or a hook.
//!
//! **D232.** The reaper's and the store's tests each grew private wrappers that forward twenty-odd
//! methods to change one. This one is shared. It carries the two injections D228/D232 need:
//!
//! * `get_raw` failing for one branch id with an I/O error, the way a storage fault or a corrupt
//!   catalog page makes it fail for a branch that is alive;
//! * a hook run just after a successful `add_arena`, which is the moment the catalog's half of a
//!   claim becomes durable, so a test can capture what else is durable at that instant.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::branch::record::{BranchRecord, CapabilityEnvelope, CoreRecord};
use crate::branch::types::{ArenaId, BranchId, BranchState, Epoch, LeaseDeadline, PageId};
use crate::branch::BranchCatalog;
use crate::error::FerroError;

type AddArenaHook = Box<dyn FnMut(BranchId, ArenaId) + Send>;

pub(crate) struct FaultyCatalog {
    inner: Arc<dyn BranchCatalog>,
    /// `get_raw` of this id fails. `u64::MAX` arms nothing: ids are minted from 0 up.
    fail_get_raw: AtomicU64,
    /// Run after every `add_arena` the inner catalog accepted.
    after_add_arena: Mutex<Option<AddArenaHook>>,
}

impl FaultyCatalog {
    pub(crate) fn new(inner: Arc<dyn BranchCatalog>) -> Arc<FaultyCatalog> {
        Arc::new(FaultyCatalog {
            inner,
            fail_get_raw: AtomicU64::new(u64::MAX),
            after_add_arena: Mutex::new(None),
        })
    }

    /// From now on, `get_raw(id)` fails with an I/O error.
    pub(crate) fn fail_get_raw_for(&self, id: u64) {
        self.fail_get_raw.store(id, Ordering::SeqCst);
    }

    /// Run `hook` with each claim the inner catalog has just made durable.
    pub(crate) fn after_add_arena(&self, hook: impl FnMut(BranchId, ArenaId) + Send + 'static) {
        *self.after_add_arena.lock().unwrap() = Some(Box::new(hook));
    }
}

impl BranchCatalog for FaultyCatalog {
    fn next_epoch(&self) -> Epoch {
        self.inner.next_epoch()
    }
    fn current_epoch(&self) -> Epoch {
        self.inner.current_epoch()
    }
    fn fork(&self, p: BranchId, l: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        self.inner.fork(p, l)
    }
    fn fork_staged(
        &self,
        p: BranchId,
        l: LeaseDeadline,
    ) -> Result<(BranchRecord, Option<u64>), FerroError> {
        self.inner.fork_staged(p, l)
    }
    fn await_fork_durable(&self, seq: Option<u64>) -> Result<(), FerroError> {
        self.inner.await_fork_durable(seq)
    }
    fn get(&self, b: BranchId) -> Result<BranchRecord, FerroError> {
        self.inner.get(b)
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
    fn restrict_envelope(&self, b: BranchId, env: CapabilityEnvelope) -> Result<(), FerroError> {
        self.inner.restrict_envelope(b, env)
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
    fn expired_before(&self, now_millis: u64) -> Result<Vec<CoreRecord>, FerroError> {
        self.inner.expired_before(now_millis)
    }
    fn in_state(&self, s: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
        self.inner.in_state(s)
    }
    fn scan(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        self.inner.scan()
    }
    fn scan_ids(
        &self,
        lo: u64,
        hi: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        self.inner.scan_ids(lo, hi)
    }
    fn max_live_child(&self, p: u64) -> Result<Option<Epoch>, FerroError> {
        self.inner.max_live_child(p)
    }
    fn live_child_in_epoch_range(&self, p: u64, lo: Epoch, hi: Epoch) -> Result<bool, FerroError> {
        self.inner.live_child_in_epoch_range(p, lo, hi)
    }
    fn has_live_children(&self, p: u64) -> Result<bool, FerroError> {
        self.inner.has_live_children(p)
    }
    fn live_count(&self) -> usize {
        self.inner.live_count()
    }
    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        if id == self.fail_get_raw.load(Ordering::SeqCst) {
            return Err(FerroError::Io(format!("injected: get_raw({id}) could not be read")));
        }
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
        self.inner.add_arena(b, a)?;
        if let Some(hook) = self.after_add_arena.lock().unwrap().as_mut() {
            hook(b, a);
        }
        Ok(())
    }
    fn renew_lease(&self, b: BranchId, l: LeaseDeadline) -> Result<(), FerroError> {
        self.inner.renew_lease(b, l)
    }
    fn envelope_of(&self, b: BranchId) -> Result<Option<CapabilityEnvelope>, FerroError> {
        self.inner.envelope_of(b)
    }
    fn charge_row_writes(&self, b: BranchId, n: u64) -> Result<(), FerroError> {
        self.inner.charge_row_writes(b, n)
    }
}
