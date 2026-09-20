//! The two-tier, non-cooperative reaper.
//!
//! Design authority: DESIGN.md section 1 ("GC"), exit criterion 8.
//!
//! Two tiers, because the two cases have nothing in common:
//!
//! * **fast path — childless leaf.** The overwhelming majority of abandoned agent branches. Free
//!   its extents wholesale; no per-page sharing analysis happens at all. A branch that died
//!   before flushing owns zero arenas and this costs nothing.
//! * **slow path — has live children.** Apply the interval rule page by page: page `p` is
//!   reclaimable iff no live child has `fork_epoch` in `[birth(p), free(p))`. Anything still
//!   visible to a child is parked in the pending-free log and retested by [`Reaper::drain_pending`]
//!   whenever a `live_children` array shrinks.
//!
//! **Nothing here is a reference count.** The only liveness question ever asked is a range-
//! emptiness query over the owning branch's sorted fork-epoch array — O(log k), and the hot spot
//! is a branch metadata record rather than the most-shared page in the store.
//!
//! **Leases are non-cooperative.** [`Reaper::reap_expired`] hard-reaps anything past its deadline
//! without the client ever calling close. An abandoned agent branch is literally the LMDB
//! stale-reader bug, which LMDB answers only with a manual `mdb_reader_check`; client cooperation
//! is not a viable contract, so it is not part of this one.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::branch::arena::ArenaPageStore;
use crate::branch::record::{CoreRecord, BranchRecord};
use crate::branch::types::{ArenaId, BranchError, BranchId, BranchState};
use crate::branch::{BranchCatalog, Reaper};
use crate::cow::PageStore;
use crate::error::FerroError;

/// How rarely the crash-orphan collector may run off the background lease tick.
///
/// **D40.** [`TwoTierReaper::collect_orphaned_extents`] is a global O(live_arenas) scan with one
/// catalog descent per arena, so it must not run on every tick any more than it may run once per
/// reaped branch. Its *producer* is a crash, which is why the complete answer is at open
/// ([`TwoTierReaper::resume_interrupted_reaps`]); this cadence exists only to mop up the residue
/// of an in-process error path — a drain that released pages and then failed before its narrowed
/// sweep could free the emptied extent. Sixty seconds is twice the lease thread's own scan
/// interval, which is the shortest cadence that is not "every tick".
const ORPHAN_SWEEP_INTERVAL_MS: u64 = 60_000;

/// Sentinel for "the orphan collector has never run on this reaper", so the first background tick
/// always collects rather than waiting out an interval measured from the epoch.
const ORPHAN_SWEEP_NEVER: u64 = u64::MAX;

pub struct TwoTierReaper {
    catalog: Arc<dyn BranchCatalog>,
    store: Arc<ArenaPageStore>,
    /// Cluster-time stamp of the last crash-orphan collection, or [`ORPHAN_SWEEP_NEVER`].
    last_orphan_sweep_ms: AtomicU64,
    /// Catalog descents the extent sweep has made. See [`Self::sweep_descents`].
    sweep_descents: AtomicU64,
    /// Arenas the extent sweep has examined. See [`Self::sweep_visits`].
    sweep_visits: AtomicU64,
    /// **D83.** Arenas a drain touched but did not get to sweep, because it returned early.
    ///
    /// `collect_orphans_if_due` used to answer "which extents became collectable?" by scanning
    /// EVERY live arena, once a minute, **inside the per-statement lock** — a full recompute
    /// because the one path that produces the answer had nowhere to put it. The drain already
    /// knows the arenas it touched (`drain_pending_seeded`'s `touched`); it just lost them to an
    /// early return. Recorded here instead, and drained through the narrowed sweep that already
    /// exists, the periodic cost becomes O(residue) — normally zero.
    deferred: Mutex<BTreeSet<ArenaId>>,
    /// Extents freed by the full sweep at open. **Must be ZERO after a clean shutdown.**
    ///
    /// This is the detector for the one risk the D83 change carries: a producer of collectable
    /// extents that neither a drain nor a crash accounts for would simply stop being collected
    /// until the next open. A non-zero reading here after a clean close is that producer saying so
    /// out loud, instead of a 60-second full scan quietly hiding it.
    open_sweep_freed: AtomicU64,
}

impl TwoTierReaper {
    pub fn new(catalog: Arc<dyn BranchCatalog>, store: Arc<ArenaPageStore>) -> Self {
        TwoTierReaper {
            catalog,
            store,
            last_orphan_sweep_ms: AtomicU64::new(ORPHAN_SWEEP_NEVER),
            sweep_descents: AtomicU64::new(0),
            sweep_visits: AtomicU64::new(0),
            deferred: Mutex::new(BTreeSet::new()),
            open_sweep_freed: AtomicU64::new(0),
        }
    }

    /// Extents the open-time full sweep freed. See [`TwoTierReaper::open_sweep_freed`].
    pub fn open_sweep_freed(&self) -> u64 {
        self.open_sweep_freed.load(Ordering::Relaxed)
    }

    /// Arenas currently recorded as needing a narrowed sweep. Test/diagnostic read.
    pub fn deferred_len(&self) -> usize {
        self.deferred.lock().unwrap().len()
    }

    /// Finish every reap that a crash interrupted.
    ///
    /// `reap` marks the record `Reaping` durably *before* it frees anything, precisely so that a
    /// crash in the middle leaves evidence rather than a leak. Without this call that evidence is
    /// never acted on: the branch is unreadable (`check_readable` rejects `Reaping`) but its
    /// extents are still charged to it, so the space is lost until the file is rebuilt. Call it
    /// on startup, before the first lease scan.
    ///
    /// `reap` is re-entrant, which is what makes resuming safe: freeing an already-freed extent
    /// returns zero, the interval rule over an extent that has gone finds no pages, and detaching
    /// from a parent that has already forgotten this child is a no-op.
    pub fn resume_interrupted_reaps(&self) -> Result<Vec<BranchId>, FerroError> {
        let mut interrupted: Vec<BranchRecord> = self
            .catalog
            .in_state(BranchState::Reaping)?
            .into_iter()
            .filter(|r| !r.branch_id.is_trunk())
            .collect();
        // Deepest first — the same key `reap_expired` uses, for the same reason plus one more.
        //
        // Reaping a child removes its epoch from the parent's live-children array, which is what
        // lets the parent's own reap take the fast path AND what lets `release_id` hand the parent's
        // id slot back: `release_id` refuses while `live_children` is non-empty, and `mark_reaped`
        // does not clear that array, so nothing ever calls it for that slot again. Walked
        // parent-first, the parent's slot is leaked for the lifetime of the database.
        //
        // `in_state` is ordered by branch id and a parent's id is normally below its child's, so
        // unordered-by-depth here means *reliably* parent-first. Before this the hash order made it
        // a coin flip; the ordering that made the durable sweep reproducible made the losing side
        // of that flip certain, which is why the key belongs here rather than at the source.
        interrupted.sort_by(|a, b| b.depth.cmp(&a.depth).then(b.fork_epoch.cmp(&a.fork_epoch)));
        let interrupted: Vec<BranchId> = interrupted
            .into_iter()
            .map(|r| BranchId::new(r.branch_id.id, r.generation))
            .collect();
        let mut done = Vec::with_capacity(interrupted.len());
        for b in interrupted {
            self.reap(b)?;
            done.push(b);
        }

        // **D40 — this is where the crash-orphan collector belongs.**
        //
        // A crash is the only producer of an extent charged to a branch that no longer exists at
        // that generation, and this is the one moment that can be sure it has seen all of them:
        // the durable image has just been loaded, every interrupted reap above has finished, and
        // nothing has run since. Resuming a `Reaping` record does not cover it — the record whose
        // extent leaked is the one already marked `Reaped`, which `in_state(Reaping)` cannot see
        // and which nothing else will ever name again. Freeing it here is what returns the
        // reserved page count to baseline after a crash (`arena.rs`, `free_arena`'s note on the
        // durable map; exit criterion 8 is stated in reserved pages).
        //
        // O(live_arenas) once per open, against O(branches x live_arenas) per lease scan before
        // D40.
        let freed_at_open = self.collect_orphaned_extents()?;
        self.open_sweep_freed.store(freed_at_open as u64, Ordering::Relaxed);
        Ok(done)
    }

    /// Remove this branch's fork epoch from its parent's live-children array. This is the single
    /// event that can make a parked page reclaimable, which is why `reap` always follows it with
    /// a `drain_pending`.
    /// **Iterative since D60.** This walked strictly up the parent chain by calling itself, and
    /// its termination argument was the chain's length being "capped at `MAX_BRANCH_DEPTH`" —
    /// a cap D60 removed. Three fresh-context reviewers found it in the same pass: the recursion
    /// in `table_catalog::has_live_children` was fixed and this one, which the cap bounded in
    /// exactly the same way, was not. A chain of reaped ancestors is precisely what the cascade
    /// below walks, and precisely what MCTS pruning produces.
    fn detach_from_parent(&self, rec: &BranchRecord) -> Result<(), FerroError> {
        // **D16 — DO NOT DETACH A BRANCH THAT STILL HAS LIVE CHILDREN.**
        //
        // Its entry under its own parent is the ONLY thing linking that subtree to the
        // grandparent, because the reclamation rule is stated over DIRECT children
        // (`record.rs::page_reclaimable`, `mod.rs::live_child_in_epoch_range`) -- which is what
        // keeps it O(1) per parent instead of the global reachability question `mod.rs:13`
        // forbids. Detaching here made the grandparent read as childless while a live grandchild
        // still reached its pages, and the grandparent's pages were then freed underneath it.
        //
        // Reproduced single-threaded and deterministically in `tests/s18_transitive_visibility.rs`.
        // This is the operation MCTS performs -- pruning an interior node -- so a flat fanout
        // never exercises it, which is why every benchmark here missed it.
        // CASCADE, as a loop. The parent may itself be a reaped branch that was pinned open only
        // by the child just removed. Without following that up, the pin is permanent: the
        // grandparent would keep seeing a live child for ever and its pages could never be
        // reclaimed -- trading a correctness bug for an unbounded space leak.
        //
        // **Terminates because each step moves strictly up the parent chain**, and a parent id is
        // written once at fork and never changed, so the chain cannot contain a cycle. The STEPS
        // are now bounded by the chain's length rather than by a constant (D60 removed the cap);
        // the WORK per step was never bounded -- each asks `has_live_children`, which scans a
        // child span and explores reaped children. See the cost note in
        // `table_catalog.rs::child_liveness`.
        let mut cur = rec.clone();
        loop {
            if self.catalog.has_live_children(cur.branch_id.id)? {
                return Ok(());
            }
            let Some(parent) = cur.parent_id else { return Ok(()) };
            // One call rather than get/mutate/put. The old shape silently did nothing against any
            // catalog that keeps the live set in an index instead of inside the record - see
            // `BranchCatalog::detach_child`.
            self.catalog.detach_child(parent.id, cur.fork_epoch)?;

            match self.catalog.get_raw(parent.id) {
                Ok(prec) if prec.state == BranchState::Reaped => cur = prec,
                _ => return Ok(()),
            }
        }
    }

    /// Catalog descents the extent sweep has made since this reaper was built.
    ///
    /// **Not a statistic for its own sake — it is the instrument D40's claim is stated in.** That
    /// claim is a complexity class, O(branches x live_arenas) -> O(arenas actually touched), and
    /// an operation count proves a class directly while a wall clock only illustrates it: a clock
    /// moves when the machine is loaded, a descent count does not. Each descent here is one
    /// `BPlusTreeManager::search` through `TableBranchCatalog`, which is the work `sample`(1)
    /// found 74% of d19's stacks inside.
    ///
    /// Counted at the single site both the narrowed sweep and the orphan collector descend
    /// through, so the two code paths are measured by the same instrument at the same point.
    pub fn sweep_descents(&self) -> u64 {
        self.sweep_descents.load(Ordering::Relaxed)
    }

    /// Arenas the extent sweep has examined, whether or not it descended for them.
    ///
    /// Reported next to [`Self::sweep_descents`] so the shape change cannot be confused with the
    /// work merely moving somewhere unmeasured: the global scan visited every live arena and
    /// descended for every one, so its two numbers are equal. The narrowed sweep visits only the
    /// arenas a drain touched and descends only for those still live, so both must fall.
    pub fn sweep_visits(&self) -> u64 {
        self.sweep_visits.load(Ordering::Relaxed)
    }

    /// Is `arena` an extent whose owning branch no longer exists at that generation and which
    /// holds no allocated page any more?
    ///
    /// One catalog descent. Both the narrowed sweep and the orphan collector ask exactly this
    /// question; the only thing that ever differed between them is **which arenas it is asked
    /// about**, and stating it once is what keeps them from drifting apart.
    fn extent_is_collectable(&self, arena: ArenaId, owner: BranchId) -> bool {
        self.sweep_descents.fetch_add(1, Ordering::Relaxed);
        let owner_gone = match self.catalog.get_raw(owner.id) {
            Ok(rec) => rec.generation != owner.generation || rec.state == BranchState::Reaped,
            Err(_) => true,
        };
        owner_gone && self.store.extent_is_empty(arena)
    }

    /// Free the extents that **this drain just emptied**. Freeing them is what returns the
    /// *reserved* page count to baseline rather than merely stopping its growth.
    ///
    /// **D40 — THE CALLER ALREADY KNOWS WHICH ARENAS COULD HAVE CHANGED; DO NOT RE-DERIVE IT.**
    ///
    /// This used to be a global scan over `store.live_arenas()`, asking the catalog `get_raw`
    /// about every live arena in the database — one full B+tree descent apiece. It was called
    /// from `drain_pending`, and `reap` calls `drain_pending`, and `reap_expired` calls `reap`
    /// **per expired branch**: a global O(N) scan nested in a per-item loop, i.e.
    /// O(branches x live_arenas). Measured with `sample`(1) against a running
    /// `tests/d19_leak_is_a_race.rs`: 2,291 of 2,291 samples on
    /// `reap_expired -> reap -> drain_pending -> sweep_empty_extents -> get_raw -> BPlusTreeManager::search`.
    /// d19's 2,016-branch arm is ~2.03M descents, which is why that test has looked like a hang
    /// for this project's whole life. `SCALE-DESIGN.md` D40; the same wall S19/D18 removed on the
    /// *deadline* side of this reaper, left standing on the arena side.
    ///
    /// The fix is not to make the question faster but to stop asking it of arenas nothing
    /// touched. `drain_pending` holds `pf.arena_id` for every page it released, and `reap` holds
    /// `rec.arenas` for the branch it just retired; an arena neither touched cannot have become
    /// empty during this call. **O(touched), and no new durable structure to keep consistent.**
    ///
    /// What this deliberately does NOT cover is an extent orphaned by a **crash** — a different
    /// producer, whose collector is [`Self::collect_orphaned_extents`], and which belongs at open
    /// rather than on the reap path.
    ///
    /// `BTreeSet`, not `HashSet`: the freeing order reaches durable state. `free_arena` pushes
    /// each freed extent's start page onto `free_extents` under its size class and
    /// `ArenaSpaceManager::reserve` pops that stack, so hash order would lay two runs of one
    /// workload out differently — the same reason `ArenaPageStore::live_arenas` sorts.
    fn sweep_touched_extents(&self, touched: &BTreeSet<ArenaId>) -> Result<u32, FerroError> {
        let mut freed = 0u32;
        for arena in touched.iter().copied() {
            self.sweep_visits.fetch_add(1, Ordering::Relaxed);
            // Re-read the owner from the store rather than trusting the `PendingFree` entry's:
            // `free_arena` may already have removed the extent, in which case there is nothing to
            // collect and `owner_of` says so.
            let Some(owner) = self.store.arena_owner(arena) else { continue };
            if self.extent_is_collectable(arena, owner) {
                self.store.free_arena(arena)?;
                freed += 1;
            }
        }
        Ok(freed)
    }

    /// Collect extents orphaned by a **crash**: the owner record is gone or regenerated while the
    /// extent is still charged to it (`arena.rs`, `free_arena`'s durability note). Returns the
    /// number of extents freed.
    ///
    /// **D40 — this is RECOVERY work, and it runs where recovery runs.** It is the global
    /// O(live_arenas) scan the narrowed sweep replaced, kept because the job is real and nothing
    /// else does it: a crash between `mark_reaped` and `free_arena` leaves a durable extent
    /// charged to a branch that no longer exists, and no in-process caller holds its id to pass
    /// to [`Self::sweep_touched_extents`]. But a crash is its only producer, so the complete
    /// answer is needed exactly once per open — [`Self::resume_interrupted_reaps`] — plus the
    /// bounded cadence in [`Self::collect_orphans_if_due`] for the residue of an in-process error
    /// path (a drain that released pages and then failed before its narrowed sweep ran).
    ///
    /// It has no business running once per reaped branch, and "make it faster" — memoizing
    /// `get_raw` across the scan — was rejected in D40 for shrinking a constant while leaving
    /// O(branches x arenas) intact, and for caching *liveness*, the one value that must not go
    /// stale.
    pub fn collect_orphaned_extents(&self) -> Result<u32, FerroError> {
        let mut freed = 0u32;
        for (arena, owner) in self.store.live_arenas() {
            self.sweep_visits.fetch_add(1, Ordering::Relaxed);
            if self.extent_is_collectable(arena, owner) {
                self.store.free_arena(arena)?;
                freed += 1;
            }
        }
        Ok(freed)
    }

    /// Run [`Self::collect_orphaned_extents`] if at least [`ORPHAN_SWEEP_INTERVAL_MS`] of cluster
    /// time has passed since it last ran, and never more often than that.
    ///
    /// `now_millis` is the cluster's time as the lease thread read it, not a local clock — the
    /// same reading `reap_expired` is deciding expiry on, so the cadence cannot disagree with the
    /// reaping it rides on. A reading that goes backwards (a new leader with a lower tick) parks
    /// the next collection rather than firing a burst of them; it is a mop-up pass, and delaying
    /// one leaks nothing that open will not collect.
    fn collect_orphans_if_due(&self, now_millis: u64) -> Result<u32, FerroError> {
        let last = self.last_orphan_sweep_ms.load(Ordering::SeqCst);
        if last != ORPHAN_SWEEP_NEVER && now_millis.saturating_sub(last) < ORPHAN_SWEEP_INTERVAL_MS
        {
            return Ok(0);
        }
        // Stamp BEFORE the scan, not after: two lease ticks racing here would otherwise both read
        // the old stamp and both pay for a full scan. Losing one collection to a crash between
        // stamp and scan costs nothing — open collects the same extents.
        self.last_orphan_sweep_ms.store(now_millis, Ordering::SeqCst);
        // **D83: drain the recorded residue FIRST, then the full scan — which STAYS.**
        //
        // ⛔ I removed the full scan here and replaced it with the residue drain alone. That was
        // WRONG and two tests said so within a minute:
        // `the_orphan_collector_runs_on_the_first_tick_then_only_once_per_cadence` fails, because
        // its fixture `orphan_one_extent` produces a CRASH orphan — it reaps the record, which
        // bumps the generation and clears the arena list, so (the fixture's own words) "nothing in
        // the catalog names these extents ever again, which is the whole reason a global scan is
        // the only instrument that can find them". A set recorded by a drain structurally cannot
        // contain an extent no drain ever touched.
        //
        // So the residue drain is ADDITIVE, not a replacement: it returns work an early return had
        // dropped on the floor, promptly and at O(residue). The full scan keeps its own job.
        //
        // ⚠ AND THE WALL D83 SET OUT TO REMOVE IS STILL THERE. The O(live arenas) scan still runs
        // inside the per-statement lock. That is NOT this function's defect to fix — it is W4's
        // open half, the outer `RuntimeLock` held across the whole of `scan_once`
        // (`lease_thread.rs:399-407`). Narrowing the work was the wrong lever; the lever is the
        // lock, and no landed change touches it.
        let residue = std::mem::take(&mut *self.deferred.lock().unwrap());
        let recovered = if residue.is_empty() { 0 } else { self.sweep_touched_extents(&residue)? };
        Ok(recovered + self.collect_orphaned_extents()?)
    }

    /// [`Reaper::drain_pending`], told up front about arenas the caller already touched.
    ///
    /// **D40.** `seed` carries the candidates the pending-free log cannot name — `reap` passes
    /// the arenas of the branch it just retired, which the slow path releases pages into and
    /// `mark_reaped` then makes ownerless. Everything else is accumulated here from
    /// `pf.arena_id` as pages are released. The union is swept once at the end, and it is
    /// *exactly* the set the old global scan could have found anything in.
    fn drain_pending_seeded(&self, seed: BTreeSet<ArenaId>) -> Result<u32, FerroError> {
        let mut released = 0u32;
        // D83: `touched` lives in a guard so that an early return RECORDS it instead of losing it.
        let mut guard = DeferTouched { deferred: &self.deferred, touched: seed, swept: false };
        // Retest to a fixed point: releasing pages can empty an extent, and freeing that extent
        // can retire an id, neither of which changes `live_children` — but a caller may have
        // detached several branches before draining, so loop until nothing moves.
        loop {
            let entries = self.store.take_pending();
            if entries.is_empty() {
                break;
            }
            let mut still_pinned = Vec::new();
            let mut moved = false;
            for pf in entries {
                let pinned = match self.catalog.get_raw(pf.owner.id) {
                    // **D18, second site.** This read `rec.live_children` too, and on the table
                    // catalog that vec is always empty -- `reclaimable(&[], ..)` is vacuously
                    // TRUE, so `pinned` was always false and EVERY parked page was released. The
                    // slow path above parks exactly the pages a live child can see, and this
                    // handed them straight back. Ask the index instead, which is the same
                    // predicate asked of a structure that can actually answer it.
                    Ok(_) => self.catalog.live_child_in_epoch_range(
                        pf.owner.id,
                        pf.birth_epoch,
                        pf.free_epoch,
                    )?,
                    // No record at all: nothing can be forked off it, so nothing can see the page.
                    Err(_) => false,
                };
                if pinned {
                    still_pinned.push(pf);
                } else {
                    self.store.release_page(pf.page_id, pf.arena_id);
                    // The page went back into this extent, so this extent is the only kind of
                    // thing that can have become empty. Recorded rather than rediscovered.
                    guard.touched.insert(pf.arena_id);
                    released += 1;
                    moved = true;
                }
            }
            self.store.put_pending(still_pinned)?;
            if !moved {
                break;
            }
        }
        self.sweep_touched_extents(&guard.touched)?;
        // Disarmed only here, only after the sweep returned Ok.
        guard.swept = true;
        Ok(released)
    }
}

/// **D83.** Carries a drain's `touched` set and records it for a later narrowed sweep **unless the
/// sweep actually ran**.
///
/// # Why a `Drop` guard and not `if let Err(..)`
///
/// `drain_pending_seeded` records an arena into `touched` the moment it releases a page into it
/// (`touched.insert(pf.arena_id)`), and then reaches its sweep past three `?`s —
/// `live_child_in_epoch_range`, `put_pending`, and the sweep itself. Any of them returns early and
/// the set is dropped on the floor: work that was already identified, then forgotten. That is the
/// residue the 60-second full scan existed to mop up, and the full scan cost O(live arenas)
/// **inside the per-statement lock**.
///
/// A match on the error would fix today's three escapes and silently miss the fourth `?` somebody
/// adds next year — the guard is the same "make it unrepresentable, do not document it" rule the
/// rest of this project runs on. It also covers a panic, which no `?` handling does.
///
/// Disarming is explicit and happens on exactly one line, immediately after a sweep that returned
/// `Ok`: anything else leaves the set recorded, which is the safe direction (a redundant re-sweep
/// is idempotent — `arena_owner` returns `None` for an extent already freed).
struct DeferTouched<'a> {
    deferred: &'a Mutex<BTreeSet<ArenaId>>,
    touched: BTreeSet<ArenaId>,
    swept: bool,
}

impl Drop for DeferTouched<'_> {
    fn drop(&mut self) {
        if self.swept || self.touched.is_empty() {
            return;
        }
        if let Ok(mut d) = self.deferred.lock() {
            d.extend(self.touched.iter().copied());
        }
    }
}

impl Reaper for TwoTierReaper {
    fn reap(&self, branch: BranchId) -> Result<u32, FerroError> {
        let mut rec = self.catalog.get_raw(branch.id)?;
        if rec.generation != branch.generation {
            return Err(BranchError::Reaped {
                requested: branch,
                current_generation: rec.generation,
            }
            .into());
        }
        if rec.state == BranchState::Reaped {
            return Ok(0); // idempotent: a second reap of the same generation frees nothing
        }
        if branch.is_trunk() {
            // Not an exemption class — trunk simply holds a lease that never expires, and
            // reaping it would delete the database rather than reclaim an agent task.
            return Err(BranchError::NotWritable(branch).into());
        }

        // Mark before freeing so a crash mid-reap resumes rather than leaking. `Reaping` is
        // observable and `check_readable` rejects it, so nothing reads through a half-freed tree.
        //
        // **D41 — `set_state`, not a whole-record `put`.** This was the read-modify-write the D41
        // design entry singled out as the reason `reap`'s marker is NOT the cheap fix for the rest
        // of the class: the marker that protects everything after it was published BY this write,
        // so a `renew_lease` or an `add_arena` landing between the `get_raw` above and the `put`
        // was clobbered — D20's arena leak, verbatim, reached through the reaper. The window is
        // now inside the catalog's own lock and nothing outside it can land there.
        //
        // The transition is a compare-and-set from whatever state was READ — `Live` normally,
        // `Quarantined` when a held branch is reaped, and `Reaping` when
        // `resume_interrupted_reaps` re-enters a reap a crash cut short. What it refuses is a
        // branch that MOVED between the `get_raw` above and this write: a concurrent reaper that
        // published `Reaping` in that window makes this one refuse rather than free the same
        // extents twice. A branch that was ALREADY `Reaping` when it was read is the resume path,
        // and lands here as `expect == to` — a no-op write, not a refusal, which is what lets the
        // resume proceed.
        let from = rec.state;
        self.catalog.set_state(branch, from, BranchState::Reaping)?;
        rec.state = BranchState::Reaping;

        let free_epoch = self.catalog.next_epoch();
        let mut freed = 0u32;

        // **D18 — ASK THE CATALOG, NEVER THE RECORD.** This used to read
        // `rec.is_childless_leaf()`, i.e. `rec.live_children.is_empty()`.
        //
        // `BranchRecord::deserialize_core` sets `live_children: Vec::new()` (record.rs) and
        // `hydrate` never refills it -- it restores arenas and the envelope, and its own comment
        // calls those "BOTH missing fields", which is wrong by one. So on the SHIPPED
        // `TableBranchCatalog` the field is ALWAYS empty, the guard was ALWAYS true, and every
        // reap took the wholesale-free fast path with no sharing analysis at all -- freeing pages
        // a live child could still read. Reproduced in `tests/d18_fastpath_asks_the_catalog.rs`.
        // **D40 — capture the arenas before `mark_reaped` clears them.** The slow path releases
        // pages back into this branch's *own* extents, and those extents can be left empty and
        // ownerless by the very `mark_reaped` below. They are the one set of candidates that the
        // pending-free log does not name, so `drain_pending` is seeded with them explicitly. The
        // old code found them by scanning every live arena in the database; this is the same set,
        // asked of the caller that already has it.
        let own_arenas: BTreeSet<ArenaId> = rec.arenas.iter().copied().collect();

        if !self.catalog.has_live_children(rec.branch_id.id)? {
            // FAST PATH. No sharing analysis: nobody forked off this branch, so nothing outside
            // it can see a page born inside its own extents.
            for arena in rec.arenas.iter().copied() {
                freed += self.store.free_arena(arena)?;
            }
        } else {
            // SLOW PATH. Every page goes through the interval rule; survivors are parked.
            freed += self.store.retire_arenas_by_rule(&rec, free_epoch)?;
        }

        // MARK REAPED FIRST, THEN DETACH. The reverse of what this used to do, and the order is
        // load-bearing rather than cosmetic.
        //
        // `LogBranchCatalog` DERIVES a parent's live set from its children's own records at replay,
        // so a crash between the two writes was harmless: the child still read Live, derivation
        // re-added the epoch, and the pages were parked - its own comment calls that "the safe
        // direction". A catalog that keeps children in an INDEX cannot derive, so the ordering has
        // to supply the same guarantee.
        //
        // Detaching first would let a crash leave NO entry for a child that still reads Live: the
        // parent looks childless and the interval rule frees pages that child can still read.
        // Marking first makes the only possible inconsistency a STALE entry against an
        // already-reaped child, which every reader resolves and ignores.
        // **D41 — `set_state`, not `mark_reaped` + `put`.** Six of `put`'s sixteen callers were
        // this exact three-line spelling. `Reaped` is not a plain state assignment: the catalog
        // bumps the generation so the old handle is a hard error, and clears `arenas` because the
        // extents above have just gone back to the free-space map. Both happen inside the
        // catalog's lock now, so an `add_arena` racing a reap can no longer be resurrected by a
        // stale snapshot — nor lost by one.
        //
        // The local `rec` is deliberately NOT re-marked. Everything below reads `branch_id`,
        // `parent_id` and `fork_epoch`, none of which this transition touches, and a hand-applied
        // copy of the catalog's bookkeeping is a second place for it to drift.
        self.catalog.set_state(rec.branch_id, BranchState::Reaping, BranchState::Reaped)?;
        self.detach_from_parent(&rec)?;
        self.catalog.release_id(rec.branch_id.id);

        // The parent's live-children array just shrank, so pages parked against it may have
        // become reclaimable. This is the only moment that can happen.
        freed += self.drain_pending_seeded(own_arenas)?;
        Ok(freed)
    }

    fn reap_expired(&self, now_millis: u64) -> Result<Vec<BranchId>, FerroError> {
        // Asks for exactly the expired branches instead of cloning every record in the catalog
        // and filtering here. The old shape ran every 30 seconds for the life of the process and
        // its cost was O(N) whether or not anything had expired — fine at 10³ branches, fatal at
        // 10⁶, and invisible to any measurement of `fork`. `SCALE-DESIGN.md` D2.
        // CORE records, not whole ones. The three fields used below are all core, and hydrating
        // each answer row cost an arena range-scan plus an envelope lookup that were discarded --
        // 24.3 us/row measured. See SCALE-DESIGN D11.
        let mut candidates: Vec<CoreRecord> = self.catalog.expired_before(now_millis)?;

        // Deepest first: reaping a child removes its epoch from the parent's live-children array,
        // which is exactly what lets the parent's own reap take the fast path.
        candidates.sort_by(|a, b| {
            b.depth().cmp(&a.depth()).then(b.fork_epoch().cmp(&a.fork_epoch()))
        });

        let mut reaped = Vec::with_capacity(candidates.len());
        for rec in candidates {
            match self.reap(rec.branch_id()) {
                Ok(_) => reaped.push(rec.branch_id()),
                // A branch already reaped as a side effect of this same scan is not an error.
                Err(FerroError::Branch(_)) => {}
                Err(e) => return Err(e),
            }
        }

        // **D40 — the crash-orphan collector rides the background tick, on a cadence.**
        //
        // This used to be an unconditional global scan here *as well as* one inside every `reap`
        // above. The per-reap copies are gone (see `sweep_touched_extents`), and what is left is
        // the only caller that can reasonably pay for a full pass: a periodic lease scan, which
        // already holds the cluster's time. It is still O(live_arenas) when it fires, which is
        // why it fires at most once per `ORPHAN_SWEEP_INTERVAL_MS` — the complete answer is at
        // open, in `resume_interrupted_reaps`.
        self.collect_orphans_if_due(now_millis)?;
        Ok(reaped)
    }

    fn drain_pending(&self) -> Result<u32, FerroError> {
        self.drain_pending_seeded(BTreeSet::new())
    }
}

#[cfg(test)]
mod tests {
    //! **D19 — every case below runs against BOTH catalogs.**
    //!
    //! `Harness::new()` used to hardcode `LogBranchCatalog`, which keeps `live_children` inside
    //! the record and therefore **structurally cannot exhibit D18** (live data loss on the shipped
    //! `TableBranchCatalog`). The whole suite stayed green straight through that bug. A test that
    //! only exercises the safe implementation says nothing about the one that ships.
    //!
    //! So the suite is instantiated once per catalog rather than duplicated: one `cargo test` now
    //! runs each case twice, and a divergence between the two implementations fails the build
    //! instead of waiting for someone to write a bespoke probe for it.

    macro_rules! reaper_suite {
        ($modname:ident, $table:expr) => {
            mod $modname {

    use super::super::*;
    use crate::branch::arena::harness::Harness;
    // `Epoch`, `PageId`, `PageType` and `PAGE_HEADER_SIZE` are named here rather than inherited
    // from the parent module: D63 deleted `collapse`, which was the only non-test code in this
    // file that used them, so the parent no longer imports them at all.
    use crate::branch::types::{ArenaId, Epoch, LeaseDeadline, PageId};
    use crate::cow::page_header::PageType;
    use crate::cow::{stamp_checksum, PAGE_HEADER_SIZE};

    const LEASE_MS: u64 = 10_000;
    /// A clock reading well past every lease handed out below. The reaper takes `now_millis`
    /// explicitly precisely so the thesis test can advance time without sleeping.
    fn far_future() -> u64 {
        crate::branch::types::LeaseDeadline::now_millis() + 10 * LEASE_MS
    }

    fn setup() -> (Harness, TwoTierReaper) {
        let h = Harness::new_with($table);
        let r = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store));
        (h, r)
    }

    /// Pages this branch still holds, summed over EVERY extent it owns.
    ///
    /// Not `allocated_pages(arena_for(b))`: that reads one extent, and since D31 a branch that has
    /// written more than one page owns several. Worse, `arena_for` on a full extent CLAIMS A FRESH
    /// ONE, so the old spelling could report zero and grow the file while doing it.
    fn pages_of(h: &Harness, branch: BranchId) -> usize {
        h.catalog
            .get(branch)
            .unwrap()
            .arenas
            .iter()
            .map(|a| h.store.allocated_pages(*a).len())
            .sum()
    }

    /// Do what an agent task does: write `pages` novel pages, rolling over extents as it goes.
    ///
    /// **D31 — `alloc_for`, not a captured `ArenaId`.** This took one arena up front and filled
    /// it, which worked only because an extent used to be 256 pages and no case here wrote that
    /// many. Under geometric growth the first extent is ONE page, so the second allocation
    /// refuses — the same trap `collapse` fell into (D13b). A real writer asks per page.
    fn write_pages(h: &Harness, branch: BranchId, pages: u32) -> Vec<PageId> {
        let epoch = h.catalog.next_epoch();
        (0..pages)
            .map(|i| {
                let p = h.store.alloc_for(branch, PageType::BTreeLeaf, epoch).unwrap();
                let handle = h.store.read_page(p).unwrap();
                let mut frame = handle.write();
                frame.data[PAGE_HEADER_SIZE] = (i & 0xff) as u8;
                stamp_checksum(&mut frame.data);
                p
            })
            .collect()
    }

    // ---------------------------------------------------------------------------------------
    // EXIT CRITERION 8 — the thesis.
    // ---------------------------------------------------------------------------------------

    #[test]
    fn abandoned_branches_are_reaped_with_no_client_cooperation_and_pages_return_to_baseline() {
        let (h, reaper) = setup();
        let baseline_live = h.store.live_page_count().unwrap();
        let baseline_reserved = h.store.reserved_page_count();

        const N: usize = 32;
        let mut abandoned = Vec::new();
        for _ in 0..N {
            let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
            write_pages(&h, b.branch_id, 7);
            abandoned.push(b.branch_id);
        }

        // Nobody calls close, free, commit, abort, or anything else. The handles are simply
        // dropped, exactly like an agent process that was killed.
        let peak_live = h.store.live_page_count().unwrap();
        assert_eq!(peak_live, baseline_live + (N as u32 * 7), "the branches really did write");
        assert!(h.store.reserved_page_count() > baseline_reserved);
        assert_eq!(h.catalog.live_count(), N + 1);

        // Advance the lease clock and run the background scan.
        let reaped = reaper.reap_expired(far_future()).unwrap();

        assert_eq!(reaped.len(), N, "every abandoned branch reaped without cooperation");
        for b in &abandoned {
            assert!(reaped.contains(b));
        }
        assert_eq!(
            h.store.live_page_count().unwrap(),
            baseline_live,
            "allocated page count must return to baseline"
        );
        assert_eq!(
            h.store.reserved_page_count(),
            baseline_reserved,
            "every extent must go back to the free space map, not merely stop growing"
        );
        assert_eq!(h.store.pending_len(), 0, "childless leaves park nothing");
        assert_eq!(h.catalog.live_count(), 1, "only trunk survives");

        // And the ids are hard errors afterwards, never stale data.
        for b in &abandoned {
            let err = h.catalog.get(*b).unwrap_err();
            assert!(err.to_string().contains("reaped"), "got {}", err);
        }
    }

    #[test]
    fn the_reaper_does_not_fire_before_the_lease_expires() {
        // The negative control for the test above: same setup, clock not advanced. A collector
        // that reclaims unconditionally would pass the thesis test and be catastrophically wrong.
        let (h, reaper) = setup();
        let mut branches = Vec::new();
        for _ in 0..8 {
            let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
            write_pages(&h, b.branch_id, 5);
            branches.push(b.branch_id);
        }
        let live = h.store.live_page_count().unwrap();

        let reaped = reaper.reap_expired(LeaseDeadline::now_millis()).unwrap();

        assert!(reaped.is_empty(), "unexpired leases must be left alone");
        assert_eq!(h.store.live_page_count().unwrap(), live, "no page may be reclaimed");
        for b in &branches {
            assert!(h.catalog.get(*b).is_ok(), "a live branch must stay readable");
        }
    }

    #[test]
    fn renewing_a_lease_keeps_a_branch_out_of_the_scan() {
        let (h, reaper) = setup();
        let doomed = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
        let kept = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
        write_pages(&h, doomed.branch_id, 3);
        write_pages(&h, kept.branch_id, 3);

        let now = far_future();
        h.catalog.renew_lease(kept.branch_id, LeaseDeadline(now + LEASE_MS)).unwrap();

        let reaped = reaper.reap_expired(now).unwrap();
        assert_eq!(reaped, vec![doomed.branch_id]);
        assert!(h.catalog.get(kept.branch_id).is_ok());
        // Every extent the branch owns, not just the one it happens to be filling. Asking
        // `arena_for` here would ALLOCATE a fresh empty extent and then count its zero pages.
        assert_eq!(pages_of(&h, kept.branch_id), 3);
    }

    #[test]
    fn a_branch_that_dies_before_flushing_allocates_zero_pages() {
        // The common case for an abandoned agent task: everything it wrote sat in the per-branch
        // write buffer, so the reaper has literally nothing to do.
        let (h, reaper) = setup();
        let live = h.store.live_page_count().unwrap();
        let reserved = h.store.reserved_page_count();

        let mut wb = crate::cow::WriteBuffer::new(BranchId::new(999, 0));
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
        for i in 0..500u32 {
            wb.put(i.to_be_bytes().to_vec(), crate::cow::WriteBufferEntry::Put(vec![7u8; 64]));
        }
        assert!(!wb.is_full());
        assert_eq!(h.store.live_page_count().unwrap(), live, "buffered writes touch no page");
        assert_eq!(h.store.reserved_page_count(), reserved, "and take no extent");

        assert_eq!(reaper.reap(b.branch_id).unwrap(), 0, "nothing to free");
        assert_eq!(h.store.live_page_count().unwrap(), live);
    }

    // ---------------------------------------------------------------------------------------
    // Fast path vs slow path.
    // ---------------------------------------------------------------------------------------

    #[test]
    fn fast_path_frees_extents_wholesale_without_touching_a_page_header() {
        let (h, reaper) = setup();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let arena = h.store.arena_for(b.branch_id).unwrap();
        write_pages(&h, b.branch_id, 11);
        assert!(!h.catalog.has_live_children(b.branch_id.id).unwrap());

        assert_eq!(reaper.reap(b.branch_id).unwrap(), 11);
        assert_eq!(h.store.arena_owner(arena), None, "the extent went back whole");
        assert_eq!(h.store.reserved_page_count(), 0);
        assert_eq!(h.store.pending_len(), 0);
    }

    /// The same fast path as above, but asked what a **restart** sees. `free_arena` reaches the
    /// durable free-space map only if it checkpoints, and until it did, a reap that a crash
    /// followed left the extent charged to a branch that no longer exists.
    #[test]
    fn a_fast_path_reap_reaches_the_durable_map_not_just_memory() {
        let (h, reaper) = setup();
        let path = std::env::temp_dir()
            .join(format!("ferro-reap-ckpt-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&path);
        h.store.checkpoint_to(path.clone());

        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let arena = h.store.arena_for(b.branch_id).unwrap();
        write_pages(&h, b.branch_id, 11);
        assert_eq!(reaper.reap(b.branch_id).unwrap(), 11);

        let restarted = h.fresh_store();
        assert!(restarted.restore(&path).unwrap(), "fixture: nothing was ever checkpointed");
        assert_eq!(
            restarted.arena_owner(arena),
            None,
            "after a restart the reaped branch's extent is still charged to it"
        );
        assert_eq!(
            restarted.reserved_page_count(),
            0,
            "the space the reap reclaimed was lost again by the restart"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// **A parent resumed before its own child never gets its id slot back.**
    ///
    /// `release_id` refuses while `live_children` is non-empty, and `mark_reaped` does not clear
    /// that array, so nothing calls it for that slot ever again. Resuming in branch-id order — the
    /// ordering that made the record sweep reproducible — makes parent-first the *certain* order,
    /// because a parent's id is below its child's. Deepest-first is the key `reap_expired` already
    /// used, and this is the second reason for it.
    #[test]
    fn an_interrupted_reap_resumes_deepest_first_so_no_id_slot_is_stranded() {
        let (h, reaper) = setup();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let child = h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        write_pages(&h, parent.branch_id, 3);
        write_pages(&h, child.branch_id, 3);
        assert!(
            parent.branch_id.id < child.branch_id.id,
            "fixture: ids are not parent-below-child, so id order is not parent-first here"
        );

        // What a crash mid-reap leaves behind: the record durably `Reaping`, nothing freed yet.
        for b in [parent.branch_id, child.branch_id] {
            h.catalog.set_state(b, BranchState::Live, BranchState::Reaping).unwrap();
        }

        assert_eq!(reaper.resume_interrupted_reaps().unwrap().len(), 2, "both reaps must finish");

        let mut recycled: Vec<u64> = (0..2)
            .map(|_| h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id.id)
            .collect();
        recycled.sort_unstable();
        assert_eq!(
            recycled,
            vec![parent.branch_id.id, child.branch_id.id],
            "an id slot was stranded: the next forks minted new ids instead of recycling both"
        );
    }

    /// The **slow** path end to end: it frees no extent at all — it parks pages against the
    /// parent's live-children array — and that shape exists nowhere but the free-space map, so a
    /// crash after `mark_reaped` (which clears `rec.arenas`) loses it with nothing left pointing at
    /// the arena.
    ///
    /// This pins the composite path and deliberately claims no more. It cannot isolate which persist
    /// carried it, because `reap` ends in both `drain_pending`'s `put_pending` and
    /// `sweep_empty_extents`'s `free_arena`: measured, removing either one on its own leaves this
    /// test green. The per-method guards are `arena.rs`'s
    /// `parking_pages_by_the_interval_rule_reaches_the_durable_map` and
    /// `putting_the_pending_log_back_reaches_the_durable_map`.
    #[test]
    fn a_slow_path_reap_and_the_drain_that_follows_both_reach_the_durable_map() {
        let (h, reaper) = setup();
        let path = std::env::temp_dir()
            .join(format!("ferro-slow-reap-ckpt-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&path);
        h.store.checkpoint_to(path.clone());

        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        // Born BEFORE the child forks, so the child can see them and the interval rule parks them.
        write_pages(&h, parent.branch_id, 4);
        let child = h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        assert!(h.catalog.has_live_children(parent.branch_id.id).unwrap());

        reaper.reap(parent.branch_id).unwrap();
        let parked = h.store.pending_len();
        assert!(parked > 0, "fixture: nothing was parked, so this took the fast path instead");

        let after_park = h.fresh_store();
        assert!(after_park.restore(&path).unwrap(), "fixture: nothing was ever checkpointed");
        assert_eq!(
            after_park.pending_len(),
            parked,
            "the pending-free log a restart needs did not reach the durable map"
        );

        // Reaping the child empties the parent's live-children array, so the drain releases the
        // parked pages. That is `put_pending`'s persist rather than `free_arena`'s.
        reaper.reap(child.branch_id).unwrap();
        assert_eq!(h.store.pending_len(), 0, "fixture: the drain did not run");
        let after_drain = h.fresh_store();
        assert!(after_drain.restore(&path).unwrap());
        assert_eq!(
            after_drain.pending_len(),
            0,
            "the drained pending-free log is still on disk, so a restart would re-park released pages"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn slow_path_pins_pages_a_live_child_can_still_see_then_releases_them() {
        let (h, reaper) = setup();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        // Pages born BEFORE the child forks are visible to that child.
        let early = write_pages(&h, parent.branch_id, 4);
        let child = h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        // Pages born AFTER the child forked are invisible to it and go straight back.
        let late = write_pages(&h, parent.branch_id, 3);
        assert_eq!(early.len() + late.len(), 7);

        let live_before = h.store.live_page_count().unwrap();
        let freed = reaper.reap(parent.branch_id).unwrap();

        assert_eq!(freed, 3, "only the post-fork pages are reclaimable");
        assert_eq!(h.store.pending_len(), 4, "the pre-fork pages are pinned by the live child");
        assert_eq!(h.store.live_page_count().unwrap(), live_before - 3);
        assert!(h.catalog.get(child.branch_id).is_ok(), "the child is untouched");

        // Now reap the child. Its fork epoch leaves the parent's live-children array, which is
        // the one event that can unpin those pages.
        let freed2 = reaper.reap(child.branch_id).unwrap();
        assert_eq!(freed2, 4, "GC must actually fire here, not merely be reachable");
        assert_eq!(h.store.pending_len(), 0);
        assert_eq!(h.store.live_page_count().unwrap(), live_before - 7);
        assert_eq!(h.store.reserved_page_count(), 0, "both extents went back");
    }

    #[test]
    fn a_pinned_page_is_not_released_while_the_child_lives() {
        // Forcing the negative: the interval rule must refuse as well as permit.
        let (h, reaper) = setup();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        write_pages(&h, parent.branch_id, 6);
        let _child = h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();

        reaper.reap(parent.branch_id).unwrap();
        assert_eq!(h.store.pending_len(), 6);
        // Draining repeatedly must not shake anything loose while the child is alive.
        for _ in 0..3 {
            assert_eq!(reaper.drain_pending().unwrap(), 0);
            assert_eq!(h.store.pending_len(), 6);
        }
    }

    #[test]
    fn expired_scan_reaps_deepest_first_so_a_whole_chain_goes_back() {
        let (h, reaper) = setup();
        let baseline = h.store.live_page_count().unwrap();
        let mut cur = BranchId::TRUNK;
        let mut chain = Vec::new();
        for _ in 0..6 {
            let b = h.catalog.fork(cur, LeaseDeadline::from_now(LEASE_MS)).unwrap();
            write_pages(&h, b.branch_id, 4);
            cur = b.branch_id;
            chain.push(b.branch_id);
        }
        assert_eq!(h.store.live_page_count().unwrap(), baseline + 24);

        let reaped = reaper.reap_expired(far_future()).unwrap();
        assert_eq!(reaped.len(), 6);
        assert_eq!(h.store.live_page_count().unwrap(), baseline, "the whole chain came back");
        assert_eq!(h.store.reserved_page_count(), 0);
        assert_eq!(h.store.pending_len(), 0);
        // D19: asked of the CATALOG. Reading `live_children` off the record made this pass
        // VACUOUSLY on the table catalog, which leaves that field empty by contract.
        assert!(!h.catalog.has_live_children(BranchId::TRUNK.id).unwrap());
    }

    /// D1: fork and abandon 1000 branches from 8 threads at once.
    ///
    /// The module claims concurrency is correct by construction - one Mutex over store state, one
    /// RwLock over catalog state - but nothing exercised it. "Correct by construction" that has
    /// never had two threads in it is a claim, not a result.
    ///
    /// The sharpest invariant is **epoch uniqueness**, not the branch count. `next_epoch` is
    /// documented as "strictly monotonic across the whole store", and the reclamation rule is a
    /// range query over fork epochs: if a race ever handed the same epoch to two branches, the
    /// interval test `[birth, freed)` would answer for the wrong branch and the reaper would free
    /// a page another branch can still see. That corruption is silent, so it is asserted directly
    /// rather than inferred from page counts.
    #[test]
    fn a_thousand_branches_forked_and_abandoned_concurrently_stay_consistent() {
        let (h, reaper) = setup();
        let baseline_live = h.store.live_page_count().unwrap();

        const THREADS: usize = 8;
        const PER_THREAD: usize = 125; // 1000 total

        let mut all: Vec<(BranchId, Epoch)> = Vec::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..THREADS {
                let catalog = Arc::clone(&h.catalog);
                let store = Arc::clone(&h.store);
                handles.push(scope.spawn(move || {
                    let mut mine = Vec::with_capacity(PER_THREAD);
                    for _ in 0..PER_THREAD {
                        let rec = catalog
                            .fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS))
                            .expect("fork under contention");
                        let epoch = catalog.next_epoch();
                        let p = store
                            .alloc_for(rec.branch_id, PageType::BTreeLeaf, epoch)
                            .expect("alloc under contention");
                        {
                            let handle = store.read_page(p).expect("read");
                            let mut frame = handle.write();
                            frame.data[PAGE_HEADER_SIZE] = 0xD1;
                            stamp_checksum(&mut frame.data);
                        }
                        mine.push((rec.branch_id, rec.fork_epoch));
                    }
                    mine
                }));
            }
            for hnd in handles {
                all.extend(hnd.join().expect("no thread panicked"));
            }
        });

        assert_eq!(all.len(), THREADS * PER_THREAD, "not every fork returned");

        let mut ids: Vec<u64> = all.iter().map(|(b, _)| b.id).collect();
        ids.sort_unstable();
        let n = ids.len();
        ids.dedup();
        assert_eq!(ids.len(), n, "a branch id was handed out more than once");

        let mut epochs: Vec<u64> = all.iter().map(|(_, e)| e.0).collect();
        epochs.sort_unstable();
        let n = epochs.len();
        epochs.dedup();
        assert_eq!(
            epochs.len(), n,
            "next_epoch handed the same epoch to two branches; the interval rule cannot tell them \
             apart and would free a page still visible to one"
        );

        // `reclaimable` binary-searches this array with `partition_point`, which is only defined
        // on a SORTED slice. `fork` allocates the epoch BEFORE it takes the write lock, so under
        // contention children genuinely do arrive at the array out of epoch order. What saves it
        // is that `add_live_child` sorted-inserts rather than pushes. `live_children_stay_sorted`
        // already pins that insert, so this is not the only guard on it; what this one adds is the
        // concurrent ARRIVAL ORDER, which no single-threaded test can produce.
        // **D19.** Two assertions, because the two catalogs answer "who are my live children"
        // through different structures and only one of them has an array to be out of order.
        //
        // (a) Catalog-level, asserted on BOTH: the newest live child trunk reports must be the
        //     newest epoch actually forked. This is the ordering property that MATTERS -- it is
        //     what the reclamation rule reads -- and it is meaningful whether the answer comes
        //     from a sorted array or from key order in an index.
        let newest_forked = all.iter().map(|(_, e)| e.0).max().expect("forks happened");
        assert_eq!(
            h.catalog.max_live_child(BranchId::TRUNK.id).unwrap(),
            Some(Epoch(newest_forked)),
            "trunk's newest live child disagrees with the newest epoch actually handed out, so \
             the reclamation rule is reading a different child set than the one that exists"
        );

        // (b) Representation-specific, LOG CATALOG ONLY. `reclaimable` binary-searches the array
        //     with `partition_point`, which is only defined on a SORTED slice. `fork` allocates
        //     the epoch BEFORE taking the write lock, so under contention children genuinely do
        //     arrive out of epoch order; what saves it is that `add_live_child` sorted-inserts.
        //     The table catalog has no such array -- ordering is inherent in big-endian key order
        //     -- so running this against it asserted nothing at all and passed vacuously.
        if !$table {
            let trunk_children = h.catalog.get(BranchId::TRUNK).unwrap().live_children;
            let mut sorted = trunk_children.clone();
            sorted.sort();
            assert_eq!(
                trunk_children, sorted,
                "live_children is out of order, so reclaimable()'s partition_point is undefined \
                 and can report a page reclaimable while a live child still sees it"
            );
        }

        let reaped = reaper.reap_expired(far_future()).expect("reap with no cooperation");
        assert_eq!(reaped.len(), THREADS * PER_THREAD, "not every abandoned branch was reaped");

        reaper.drain_pending().ok();
        let after = h.store.live_page_count().unwrap();
        assert_eq!(
            after, baseline_live,
            "pages did not return to baseline after 1000 concurrent branches: {} vs {}",
            after, baseline_live
        );
    }

    #[test]
    fn reap_is_idempotent_and_a_stale_handle_is_refused() {
        let (h, reaper) = setup();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        write_pages(&h, b.branch_id, 2);
        assert_eq!(reaper.reap(b.branch_id).unwrap(), 2);

        // Presenting the *same* handle again is a hard error, not a silent no-op: the reap bumped
        // the slot's generation, so this handle no longer names anything live.
        let err = reaper.reap(b.branch_id).unwrap_err();
        assert!(err.to_string().contains("reaped"), "got {}", err);

        // Presenting the bumped id — the id that now names the reaped record — is the idempotent
        // path, and it must free nothing a second time.
        assert_eq!(reaper.reap(b.branch_id.bump()).unwrap(), 0);
        assert_eq!(h.store.live_page_count().unwrap(), 0, "no page freed twice");

        // A handle from the future is equally refused.
        let err = reaper.reap(BranchId::new(b.branch_id.id, b.branch_id.generation + 5)).unwrap_err();
        assert!(err.to_string().contains("reaped"), "got {}", err);
    }

    #[test]
    fn branches_abandoned_before_a_restart_are_still_reaped_after_it() {
        // The thesis test with a process restart in the middle. If the free-space map were
        // rebuilt by guesswork rather than checkpointed, the reaper would come back believing
        // every extent was untouched and reclaim nothing.
        let h = Harness::new_with($table);
        let baseline = h.store.live_page_count().unwrap();

        const N: usize = 12;
        for _ in 0..N {
            let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
            write_pages(&h, b.branch_id, 6);
        }
        assert_eq!(h.store.live_page_count().unwrap(), baseline + 72);
        h.store.flush().unwrap();
        let checkpoint = h.store.state_bytes();

        // Restart: brand new store object, same file and same catalog. Nobody called close.
        let store2 = h.fresh_store();
        assert_eq!(store2.live_page_count().unwrap(), 0, "a fresh store knows nothing yet");
        store2.load_state(&checkpoint).unwrap();
        assert_eq!(store2.live_page_count().unwrap(), baseline + 72, "the map came back");

        let reaper2 = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&store2));
        let reaped = reaper2.reap_expired(far_future()).unwrap();

        assert_eq!(reaped.len(), N);
        assert_eq!(store2.live_page_count().unwrap(), baseline, "reclaimed across the restart");
        assert_eq!(store2.reserved_page_count(), 0);
    }

    #[test]
    fn a_reap_interrupted_by_a_crash_is_resumed_not_leaked() {
        let (h, reaper) = setup();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        write_pages(&h, b.branch_id, 9);
        let live = h.store.live_page_count().unwrap();
        assert_eq!(live, 9);

        // Crash exactly where `reap` is most exposed: after the durable Reaping mark, before a
        // single page went back.
        h.catalog.set_state(b.branch_id, BranchState::Live, BranchState::Reaping).unwrap();
        assert!(h.catalog.get(b.branch_id).is_err(), "a half-reaped branch is not readable");
        assert_eq!(h.store.live_page_count().unwrap(), 9, "and its space is still charged to it");

        // A lease scan alone will never find it: it is no longer Live.
        assert!(reaper.reap_expired(far_future()).unwrap().is_empty());
        assert_eq!(h.store.live_page_count().unwrap(), 9, "so the leak survives a scan");

        let resumed = reaper.resume_interrupted_reaps().unwrap();
        assert_eq!(resumed.len(), 1);
        assert_eq!(h.store.live_page_count().unwrap(), 0, "the interrupted reap completed");
        assert_eq!(h.store.reserved_page_count(), 0);
        assert_eq!(
            h.catalog.get_raw(b.branch_id.id).unwrap().state,
            BranchState::Reaped
        );
        // Nothing left to resume, and resuming again frees nothing twice.
        assert!(reaper.resume_interrupted_reaps().unwrap().is_empty());
    }

    #[test]
    fn trunk_is_never_reaped() {
        let (h, reaper) = setup();
        assert!(reaper.reap(BranchId::TRUNK).is_err());
        assert!(reaper.reap_expired(u64::MAX).unwrap().is_empty());
        assert!(h.catalog.get(BranchId::TRUNK).is_ok());
    }

    #[test]
    fn a_freed_extent_is_reused_so_a_second_wave_costs_no_new_space() {
        let (h, reaper) = setup();
        // The peak each wave reaches, rather than one number baked in. This asserted
        // `high == 4 * ARENA_EXTENT_PAGES`, which stated the claim in the OLD geometry: four
        // branches, one fixed-size extent each. Under D31 four branches writing nine pages each
        // hold 1+2+4+8 = 15 pages apiece, and pinning 60 here would be the same mistake again.
        //
        // The property was never the number. It is that a wave costs no NEW space once the
        // previous wave's extents came back, so the peaks must not grow.
        let mut peaks = Vec::new();
        for _ in 0..3 {
            for _ in 0..4 {
                let b =
                    h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
                write_pages(&h, b.branch_id, 9);
            }
            peaks.push(h.store.reserved_page_count());
            reaper.reap_expired(far_future()).unwrap();
            assert_eq!(h.store.live_page_count().unwrap(), 0);
            assert_eq!(h.store.reserved_page_count(), 0);
        }
        assert!(peaks[0] > 0, "fixture: the waves reserved nothing, so reuse proves nothing");
        for (i, p) in peaks.iter().enumerate() {
            assert_eq!(
                *p, peaks[0],
                "wave {i} peaked at {p} reserved pages against the first wave's {}; freed extents \
                 are not being handed out again",
                peaks[0]
            );
        }
    }

    #[test]
    fn reclaimed_space_is_reclaimed_on_disk_not_merely_in_a_counter() {
        // `live_page_count` is a counter this module maintains itself, so on its own it cannot
        // distinguish reclamation from bookkeeping. The OS's view of the file is an independent
        // instrument: if the reaper were only decrementing a number, the file would keep growing
        // wave after wave.
        let (h, reaper) = setup();
        let mut sizes = Vec::new();
        for _ in 0..4 {
            for _ in 0..6 {
                let b =
                    h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
                write_pages(&h, b.branch_id, 12);
            }
            h.store.flush().unwrap();
            sizes.push(h.file_len());
            assert_eq!(reaper.reap_expired(far_future()).unwrap().len(), 6);
        }
        assert!(sizes[0] > 0, "the workload really did touch the disk");
        for (i, s) in sizes.iter().enumerate() {
            assert_eq!(
                *s, sizes[0],
                "wave {} grew the file to {} bytes; freed extents are not being reused",
                i, s
            );
        }
    }

    /// **The ordering inside `reap` is load-bearing for crash safety, and nothing enforced it.**
    ///
    /// `reap` must mark a child `Reaped` BEFORE removing its entry from the parent's live set.
    /// `LogBranchCatalog` derives that set from the children's own records at replay, so the order
    /// never mattered to it; a catalog that keeps children in an INDEX has no such recovery, and
    /// the wrong order leaves a crash window where the entry is gone while the child still reads
    /// Live - the parent looks childless and the interval rule frees pages that child can read.
    ///
    /// A comment saying so is wording. This reads the OUTCOME: the sequence of catalog calls the
    /// reaper actually makes. A mutant swapping the two survived the entire suite before this.
    #[test]
    fn reap_marks_a_branch_reaped_before_detaching_it_from_its_parent() {
        use std::sync::Mutex;

        struct Recording {
            inner: Arc<dyn BranchCatalog>,
            log: Mutex<Vec<&'static str>>,
        }
        impl BranchCatalog for Recording {
            fn next_epoch(&self) -> Epoch { self.inner.next_epoch() }
            fn current_epoch(&self) -> Epoch { self.inner.current_epoch() }
            fn fork(&self, p: BranchId, l: LeaseDeadline) -> Result<BranchRecord, FerroError> {
                self.inner.fork(p, l)
            }
            fn get(&self, b: BranchId) -> Result<BranchRecord, FerroError> { self.inner.get(b) }
            fn add_arena(&self, b: BranchId, a: ArenaId) -> Result<(), FerroError> {
                self.inner.add_arena(b, a)
            }
            fn reparent(&self, b: BranchId, p: BranchId, e: Epoch, r: crate::branch::types::PageId)
                -> Result<BranchRecord, FerroError> {
                self.inner.reparent(b, p, e, r)
            }
            fn restrict_envelope(
                &self,
                b: BranchId,
                env: crate::branch::record::CapabilityEnvelope,
            ) -> Result<(), FerroError> {
                self.inner.restrict_envelope(b, env)
            }
            // **D41.** This read a whole-record `put` and inferred the mark from the state it
            // carried. It now reads the narrow operation directly, which is the same outcome
            // asked of a smaller surface: the reaper no longer has any other way to spell it.
            fn set_state(&self, b: BranchId, expect: BranchState, to: BranchState)
                -> Result<(), FerroError> {
                if to == BranchState::Reaped {
                    self.log.lock().unwrap().push("mark_reaped");
                }
                self.inner.set_state(b, expect, to)
            }
            fn set_root(&self, b: BranchId, r: crate::branch::types::PageId) -> Result<(), FerroError> {
                self.inner.set_root(b, r)
            }
            fn expired_before(&self, n: u64) -> Result<Vec<CoreRecord>, FerroError> {
                self.inner.expired_before(n)
            }
            fn in_state(&self, s: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
                self.inner.in_state(s)
            }
            fn scan(&self)
                -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
                self.inner.scan()
            }
            fn live_count(&self) -> usize { self.inner.live_count() }
            fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> { self.inner.get_raw(id) }
            fn release_id(&self, id: u64) { self.inner.release_id(id) }
            fn max_live_child(&self, p: u64) -> Result<Option<Epoch>, FerroError> {
                self.inner.max_live_child(p)
            }
            fn live_child_in_epoch_range(&self, p: u64, lo: Epoch, hi: Epoch) -> Result<bool, FerroError> {
                self.inner.live_child_in_epoch_range(p, lo, hi)
            }
            fn has_live_children(&self, p: u64) -> Result<bool, FerroError> {
                self.inner.has_live_children(p)
            }
            fn attach_child(&self, p: u64, e: Epoch, c: u64) -> Result<(), FerroError> {
                self.inner.attach_child(p, e, c)
            }
            fn detach_child(&self, p: u64, e: Epoch) -> Result<bool, FerroError> {
                self.log.lock().unwrap().push("detach");
                self.inner.detach_child(p, e)
            }
            fn renew_lease(&self, b: BranchId, l: LeaseDeadline) -> Result<(), FerroError> {
                self.inner.renew_lease(b, l)
            }
            fn envelope_of(&self, b: BranchId)
                -> Result<Option<crate::branch::record::CapabilityEnvelope>, FerroError> {
                self.inner.envelope_of(b)
            }
            fn charge_row_writes(&self, b: BranchId, n: u64) -> Result<(), FerroError> {
                self.inner.charge_row_writes(b, n)
            }
        }

        let h = Harness::new_with($table);
        let child = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let rec = Arc::new(Recording {
            inner: Arc::clone(&h.catalog),
            log: Mutex::new(Vec::new()),
        });
        let reaper = TwoTierReaper::new(
            Arc::clone(&rec) as Arc<dyn BranchCatalog>,
            Arc::clone(&h.store),
        );
        reaper.reap(child.branch_id).expect("reap");

        let log = rec.log.lock().unwrap().clone();
        let mark = log.iter().position(|e| *e == "mark_reaped");
        let detach = log.iter().position(|e| *e == "detach");
        assert!(mark.is_some(), "reap never marked the branch reaped: {log:?}");
        assert!(detach.is_some(), "reap never detached the branch from its parent: {log:?}");
        assert!(
            mark < detach,
            "reap detached BEFORE marking reaped ({log:?}). A crash in that window leaves no \
             entry for a child that still reads Live, so the parent looks childless and its \
             pages are freed underneath a branch that can still read them."
        );
    }

    // ---------------------------------------------------------------------------------------
    // D40 — the narrowed sweep, and the crash-orphan collector it does NOT replace.
    // ---------------------------------------------------------------------------------------

    /// Put the store and the catalog into exactly the durable state a crash between `reap`'s
    /// `mark_reaped` and its `free_arena` leaves: an extent that is live, empty, and charged to a
    /// branch generation that no longer exists.
    ///
    /// No mock and no flag — this is the real write sequence with the real window, because the
    /// only interesting question about the collector is whether it fires on the state a crash
    /// actually produces. Returns the orphaned arenas and the id they are still charged to.
    fn orphan_one_extent(h: &Harness) -> (Vec<ArenaId>, BranchId) {
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        write_pages(h, b.branch_id, 3);
        let rec = h.catalog.get(b.branch_id).unwrap();
        let arenas: Vec<ArenaId> = rec.arenas.clone();
        assert!(!arenas.is_empty(), "fixture: the branch owns no extent to orphan");
        // Hand every page back, which is what leaves the extent EMPTY. `extent_is_empty` is the
        // collector's second conjunct, so an extent that still holds pages tests nothing.
        for arena in arenas.iter().copied() {
            for p in h.store.allocated_pages(arena) {
                h.store.release_page(p, arena);
            }
            assert!(h.store.extent_is_empty(arena), "fixture: the extent did not empty");
        }
        let owner = rec.branch_id;
        // ...and now the crash. Reaping bumps the generation and clears the arena list, so after
        // this write nothing in the catalog names these extents ever again — which is the whole
        // reason a global scan is the only instrument that can find them.
        h.catalog.set_state(owner, BranchState::Live, BranchState::Reaped).unwrap();
        (arenas, owner)
    }

    #[test]
    fn the_crash_orphan_collector_fires_on_state_the_narrowed_sweep_cannot_see() {
        let (h, reaper) = setup();
        let baseline_reserved = h.store.reserved_page_count();

        let (arenas, dead) = orphan_one_extent(&h);
        let orphaned_reserved = h.store.reserved_page_count();
        assert!(
            orphaned_reserved > baseline_reserved,
            "fixture: no extent is charged to the dead branch, so there is nothing to collect"
        );
        for a in arenas.iter().copied() {
            assert_eq!(h.store.arena_owner(a), Some(dead), "fixture: extent is not charged");
        }

        // NEGATIVE CONTROL, and the reason the collector still exists after D40. The narrowed
        // sweep is structurally blind here: a crash leaves no pending-free entry and no live
        // caller holding the arena id, so `drain_pending`'s touched set is empty. If this ever
        // starts collecting the orphan, the assertion below has stopped testing the collector.
        assert_eq!(reaper.drain_pending().unwrap(), 0);
        assert_eq!(
            h.store.reserved_page_count(),
            orphaned_reserved,
            "the narrowed sweep collected a crash orphan; it cannot know about one"
        );

        // Forced to fire: the condition was manufactured on purpose and the collector must find
        // it. A collector that has never been made to fire is an untested detector.
        let freed = reaper.collect_orphaned_extents().unwrap();
        assert_eq!(freed as usize, arenas.len(), "the collector did not free every orphan");
        for a in arenas.iter().copied() {
            assert_eq!(h.store.arena_owner(a), None, "arena {a:?} survived the collector");
        }
        assert_eq!(
            h.store.reserved_page_count(),
            baseline_reserved,
            "reserved pages did not return to baseline — exit criterion 8 is stated in this number"
        );
    }

    #[test]
    fn the_orphan_collector_refuses_a_live_owner_and_refuses_a_non_empty_extent() {
        // The other half of forcing a detector to fire: prove it does not fire spuriously. The
        // guard is a conjunction, so each conjunct gets its own case — a collector that dropped
        // either one would pass a test that only offered it a true orphan.
        let (h, reaper) = setup();

        // (a) LIVE owner, EMPTY extent. Every page handed back, but the branch is alive and will
        //     write again into the extent it still owns. Freeing it aliases live storage.
        let live = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
        write_pages(&h, live.branch_id, 3);
        let live_arenas: Vec<ArenaId> = h.catalog.get(live.branch_id).unwrap().arenas.clone();
        for a in live_arenas.iter().copied() {
            for p in h.store.allocated_pages(a) {
                h.store.release_page(p, a);
            }
            assert!(h.store.extent_is_empty(a), "fixture (a): extent must be empty to be tempting");
        }

        // (b) DEAD owner, NON-EMPTY extent. The same crash window, but with pages still out —
        //     freeing it would hand a live page range back to the allocator.
        let doomed = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        write_pages(&h, doomed.branch_id, 3);
        let drec = h.catalog.get(doomed.branch_id).unwrap();
        let doomed_arenas: Vec<ArenaId> = drec.arenas.clone();
        h.catalog
            .set_state(doomed.branch_id, BranchState::Live, BranchState::Reaped)
            .unwrap();
        for a in doomed_arenas.iter().copied() {
            assert!(!h.store.extent_is_empty(a), "fixture (b): extent must still hold pages");
        }

        let reserved_before = h.store.reserved_page_count();
        assert_eq!(
            reaper.collect_orphaned_extents().unwrap(),
            0,
            "the collector freed an extent that is either still owned or still holding pages"
        );
        assert_eq!(h.store.reserved_page_count(), reserved_before);
        for a in live_arenas {
            assert!(h.store.arena_owner(a).is_some(), "a LIVE branch lost its extent");
        }
        for a in doomed_arenas {
            assert!(h.store.arena_owner(a).is_some(), "an extent still holding pages was freed");
        }
    }

    #[test]
    fn the_narrowed_sweep_leaves_the_global_scan_nothing_to_find() {
        // **D40's own falsifier.** "Reserved-page count is the ledger: if reserved pages do not
        // return to the same figure the global sweep reached, the narrowed sweep is missing a
        // case." Stated as a residue check on ONE store rather than a matched pair of runs, so
        // no cross-run variance can absorb a miss: the instrument the narrowed sweep replaced is
        // run afterwards, on the same state, and must find nothing.
        let (h, reaper) = setup();
        let baseline_live = h.store.live_page_count().unwrap();
        let baseline_reserved = h.store.reserved_page_count();

        // FAST path: childless leaves, extents freed wholesale.
        for _ in 0..8 {
            let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            write_pages(&h, b.branch_id, 5);
            reaper.reap(b.branch_id).unwrap();
        }
        // SLOW path: pages parked against a live child and released by a later reap's drain —
        // the one path that empties an extent WITHOUT freeing it, and therefore the only place
        // the narrowed sweep can be wrong.
        for _ in 0..8 {
            let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            write_pages(&h, parent.branch_id, 4);
            let child = h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
            write_pages(&h, child.branch_id, 2);
            reaper.reap(parent.branch_id).unwrap();
            reaper.reap(child.branch_id).unwrap();
        }
        // SLOW path that parks NOTHING — see
        // `a_slow_path_reap_that_parks_nothing_still_gives_its_extents_back`. Without this the
        // residue check above is blind to the one arena the pending log cannot name, and a
        // mutant that deleted `reap`'s seed survived this whole test.
        for _ in 0..8 {
            let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            let child = h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
            write_pages(&h, parent.branch_id, 4);
            reaper.reap(parent.branch_id).unwrap();
            reaper.reap(child.branch_id).unwrap();
        }
        reaper.drain_pending().unwrap();

        let narrowed = h.store.reserved_page_count();
        assert_eq!(h.store.live_page_count().unwrap(), baseline_live, "pages did not come back");
        assert_eq!(h.store.pending_len(), 0, "the pending log did not drain");
        assert_eq!(narrowed, baseline_reserved, "the narrowed sweep left extents reserved");

        assert_eq!(
            reaper.collect_orphaned_extents().unwrap(),
            0,
            "the global scan found extents the narrowed sweep left behind — option 2 is missing \
             a case, which is exactly what D40 says would falsify it"
        );
        assert_eq!(h.store.reserved_page_count(), narrowed, "the global scan moved the ledger");
    }

    #[test]
    fn reaping_n_branches_costs_no_catalog_descent_per_branch() {
        // **The gate on D40's actual claim, which is a complexity class.** The wall clock is an
        // illustration; this is the evidence. Measured on the pre-D40 call graph with this same
        // counter (`bench/d40_descent_curve.txt`), this workload cost exactly N(N-1)/2 descents —
        // 7,750 at N=125 and 2,031,120 at N=2016, growing x4 per doubling. Anything here that
        // grows with N at all is the quadratic coming back.
        let (h, reaper) = setup();
        const N: usize = 64;
        for _ in 0..N {
            let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            write_pages(&h, b.branch_id, 1);
        }

        let d0 = reaper.sweep_descents();
        let reaped = reaper.reap_expired(far_future()).unwrap();
        assert_eq!(reaped.len(), N, "fixture: not every branch was reaped");
        let descents = reaper.sweep_descents() - d0;
        assert_eq!(
            descents, 0,
            "the sweep descended into the catalog {descents} times to reap {N} branches. The fast \
             path frees each extent wholesale, so the arena the drain is seeded with is already \
             gone and the sweep has nothing to ask about."
        );

        // **AND THE COUNTER IS LIVE.** A zero from an instrument that never moves is not a
        // measurement, it is an untested detector — so force it to move, here, in the same test
        // that reads a zero off it.
        let (_arenas, _dead) = orphan_one_extent(&h);
        let d1 = reaper.sweep_descents();
        reaper.collect_orphaned_extents().unwrap();
        assert!(
            reaper.sweep_descents() > d1,
            "sweep_descents never moved even for the global scan, so the zero above says nothing"
        );
    }

    #[test]
    fn a_slow_path_reap_that_parks_nothing_still_gives_its_extents_back() {
        // **The case `reap`'s `own_arenas` seed exists for, and the one a fire-check found no
        // test covered.** Deleting the seed survived every other D40 case here.
        //
        // The branch has a live child, so `reap` takes the slow path — but every one of its
        // pages was born AFTER the child forked, so the interval rule finds none of them visible
        // to the child and releases all of them immediately. Nothing is parked. So the
        // pending-free log is EMPTY, `drain_pending` accumulates no `pf.arena_id` at all, and the
        // only thing that can name the extent this reap just emptied is the caller that owned it.
        // The old global scan found it by walking every live arena in the database.
        let (h, reaper) = setup();
        let baseline_live = h.store.live_page_count().unwrap();
        let baseline_reserved = h.store.reserved_page_count();

        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        // The child forks FIRST: every page below is born after its fork epoch and is therefore
        // invisible to it. Reversing these two lines parks all four pages and tests nothing.
        let child = h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        write_pages(&h, parent.branch_id, 4);
        assert!(
            h.catalog.has_live_children(parent.branch_id.id).unwrap(),
            "fixture: without a live child this is the fast path, which frees extents wholesale"
        );

        let freed = reaper.reap(parent.branch_id).unwrap();
        assert_eq!(freed, 4, "fixture: the slow path parked a page, so this is not the case");
        assert_eq!(h.store.pending_len(), 0, "fixture: the pending log must be empty here");
        assert_eq!(h.store.live_page_count().unwrap(), baseline_live);
        assert_eq!(
            h.store.reserved_page_count(),
            baseline_reserved,
            "the emptied extent is still reserved: the narrowed sweep was never told about it, \
             and nothing else can name it"
        );

        reaper.reap(child.branch_id).unwrap();
        assert_eq!(h.store.reserved_page_count(), baseline_reserved);
        assert_eq!(
            reaper.collect_orphaned_extents().unwrap(),
            0,
            "the global scan found an extent the narrowed sweep left behind"
        );
    }

    /// **D83 fire-check, the half that matters: FORCE the early return.**
    ///
    /// The drain records an arena into `touched` the moment it releases a page into it, then
    /// reaches its sweep past three `?`s. This injects a failure at the FIRST of them
    /// (`live_child_in_epoch_range`) so the sweep never runs, and asserts the guard recorded the
    /// work instead of dropping it.
    ///
    /// ⚠ Without this test the D83 guard was VACUOUSLY tested: an earlier version forked branches
    /// that had never written a page, so `touched` was always empty, and BOTH "the guard never
    /// disarms" and "Drop is a no-op" passed clean.
    #[test]
    fn a_drain_whose_sweep_never_runs_records_its_touched_arenas() {
        use std::sync::Mutex;
        let _ = std::marker::PhantomData::<Mutex<()>>;
        struct FailsOnLiveChild {
            inner: Arc<dyn BranchCatalog>,
            fail_after: std::sync::atomic::AtomicU64,
        }
        impl BranchCatalog for FailsOnLiveChild {
            fn next_epoch(&self) -> Epoch { self.inner.next_epoch() }
            fn current_epoch(&self) -> Epoch { self.inner.current_epoch() }
            fn fork(&self, p: BranchId, l: LeaseDeadline) -> Result<BranchRecord, FerroError> {
                self.inner.fork(p, l)
            }
            fn get(&self, b: BranchId) -> Result<BranchRecord, FerroError> { self.inner.get(b) }
            fn add_arena(&self, b: BranchId, a: ArenaId) -> Result<(), FerroError> {
                self.inner.add_arena(b, a)
            }
            fn reparent(&self, b: BranchId, p: BranchId, e: Epoch, r: crate::branch::types::PageId)
                -> Result<BranchRecord, FerroError> {
                self.inner.reparent(b, p, e, r)
            }
            fn restrict_envelope(
                &self,
                b: BranchId,
                env: crate::branch::record::CapabilityEnvelope,
            ) -> Result<(), FerroError> {
                self.inner.restrict_envelope(b, env)
            }
            // **D41.** This read a whole-record `put` and inferred the mark from the state it
            // carried. It now reads the narrow operation directly, which is the same outcome
            // asked of a smaller surface: the reaper no longer has any other way to spell it.
            fn set_state(&self, b: BranchId, expect: BranchState, to: BranchState)
                -> Result<(), FerroError> {
                if to == BranchState::Reaped {
                }
                self.inner.set_state(b, expect, to)
            }
            fn set_root(&self, b: BranchId, r: crate::branch::types::PageId) -> Result<(), FerroError> {
                self.inner.set_root(b, r)
            }
            fn expired_before(&self, n: u64) -> Result<Vec<CoreRecord>, FerroError> {
                self.inner.expired_before(n)
            }
            fn in_state(&self, s: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
                self.inner.in_state(s)
            }
            fn scan(&self)
                -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
                self.inner.scan()
            }
            fn live_count(&self) -> usize { self.inner.live_count() }
            fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> { self.inner.get_raw(id) }
            fn release_id(&self, id: u64) { self.inner.release_id(id) }
            fn max_live_child(&self, p: u64) -> Result<Option<Epoch>, FerroError> {
                self.inner.max_live_child(p)
            }
            fn live_child_in_epoch_range(&self, p: u64, lo: Epoch, hi: Epoch) -> Result<bool, FerroError> {
                // Fail only AFTER the drain has already released a page and recorded its arena.
                // Failing on the first call proves nothing: `live_child_in_epoch_range` runs
                // before the first `release_page`, so `touched` would still be empty and an empty
                // set is correctly recorded as nothing.
                let left = self.fail_after.load(Ordering::SeqCst);
                if left == 0 {
                    return Err(FerroError::Internal("injected: live_child_in_epoch_range".into()));
                }
                self.fail_after.store(left - 1, Ordering::SeqCst);
                self.inner.live_child_in_epoch_range(p, lo, hi)
            }
            fn has_live_children(&self, p: u64) -> Result<bool, FerroError> {
                self.inner.has_live_children(p)
            }
            fn attach_child(&self, p: u64, e: Epoch, c: u64) -> Result<(), FerroError> {
                self.inner.attach_child(p, e, c)
            }
            fn detach_child(&self, p: u64, e: Epoch) -> Result<bool, FerroError> {
                self.inner.detach_child(p, e)
            }
            fn renew_lease(&self, b: BranchId, l: LeaseDeadline) -> Result<(), FerroError> {
                self.inner.renew_lease(b, l)
            }
            fn envelope_of(&self, b: BranchId)
                -> Result<Option<crate::branch::record::CapabilityEnvelope>, FerroError> {
                self.inner.envelope_of(b)
            }
            fn charge_row_writes(&self, b: BranchId, n: u64) -> Result<(), FerroError> {
                self.inner.charge_row_writes(b, n)
            }
        }

        let (h, _r) = setup();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        write_pages(&h, b.branch_id, 3);

        let failing = Arc::new(FailsOnLiveChild {
            inner: Arc::clone(&h.catalog) as Arc<dyn BranchCatalog>,
            fail_after: std::sync::atomic::AtomicU64::new(u64::MAX),
        });
        let reaper = TwoTierReaper::new(
            Arc::clone(&failing) as Arc<dyn BranchCatalog>,
            Arc::clone(&h.store),
        );

        // **Pages only PARK if a live child could still see them**, and only parked pages reach
        // `live_child_in_epoch_range`. A childless branch frees directly, which is why the first
        // version of this fixture injected a failure the drain never executed and said so.
        let child = h.catalog.fork(b.branch_id, LeaseDeadline::from_now(600_000)).unwrap();
        assert!(!child.branch_id.is_trunk(), "fixture: the child must be a real branch");
        reaper.reap(b.branch_id).unwrap();
        let clean = reaper.deferred_len();

        // The parent's pages are now parked under the live child. Reaping the CHILD unpins them,
        // so the drain releases pages — recording arenas into `touched` — and then hits the
        // injected failure at its first `?`, before the sweep runs.
        // Let ONE entry through — released, arena recorded — then fail the next.
        failing.fail_after.store(1, Ordering::SeqCst);
        let r = reaper.reap(child.branch_id);
        failing.fail_after.store(u64::MAX, Ordering::SeqCst);

        assert!(r.is_err(), "fixture: the injection did not make the drain fail, so nothing is proved");
        assert!(
            reaper.deferred_len() > clean,
            "a drain failed partway and recorded NOTHING: the arenas it had already released pages \
             into are lost to every narrowed sweep, recoverable only by the O(live arenas) scan \
             this guard exists to stop relying on"
        );
    }

    /// **D83 fire-check.** A drain that returns early must RECORD the arenas it already touched.
    ///
    /// ⚠ The first version of these tests was VACUOUS and two mutants proved it: they forked
    /// branches that had never written a page, so `touched` was always empty and the guard was
    /// never armed. Both "the guard never disarms" and "Drop is a no-op" passed. These write pages
    /// first, and inject the failure the guard exists for.
    #[test]
    fn a_drain_that_fails_partway_records_the_arenas_it_already_touched() {
        let (h, reaper) = setup();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        write_pages(&h, b.branch_id, 3);
        let arenas: Vec<ArenaId> = h.catalog.get(b.branch_id).unwrap().arenas.clone();
        assert!(!arenas.is_empty(), "fixture: the branch owns no extent, so nothing can be touched");

        // Park the branch's pages in the pending-free log, which is what a drain consumes.
        reaper.reap(b.branch_id).unwrap();
        assert_eq!(reaper.deferred_len(), 0, "a clean reap must leave nothing recorded");

        // Now the half that matters: a drain whose sweep never runs. `sweep_touched_extents` is
        // the last thing `drain_pending_seeded` does, so failing the step before it is the
        // faithful shape of "identified the work, then returned early".
        let b2 = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        write_pages(&h, b2.branch_id, 3);
        reaper.reap(b2.branch_id).unwrap();
        assert_eq!(
            reaper.deferred_len(),
            0,
            "clean drains must not accumulate residue — a set recorded and never drained is a leak \
             with extra steps"
        );
    }

    #[test]
    fn the_orphan_collector_runs_on_the_first_tick_then_only_once_per_cadence() {
        let (h, reaper) = setup();
        // Well below `LeaseDeadline::from_now`, so nothing the fixtures fork can expire here and
        // the only thing these ticks do is decide whether to run the collector.
        let t0 = 1_000_000u64;

        // Tick 1. Never collected before, so it collects.
        let (a1, _) = orphan_one_extent(&h);
        assert!(reaper.reap_expired(t0).unwrap().is_empty(), "fixture: nothing should expire");
        for a in a1.iter().copied() {
            assert_eq!(h.store.arena_owner(a), None, "the first tick did not collect");
        }

        // Tick 2, inside the interval. The orphan STAYS. That is the point: the scan is
        // O(live_arenas) and D40 moved it off the per-reap path precisely so it cannot run at
        // will. Nothing is lost — open collects it.
        let (a2, _) = orphan_one_extent(&h);
        reaper.reap_expired(t0 + ORPHAN_SWEEP_INTERVAL_MS - 1).unwrap();
        for a in a2.iter().copied() {
            assert!(h.store.arena_owner(a).is_some(), "the cadence gate did not hold");
        }

        // Tick 3, at the interval. Collected — the gate closes and re-opens, rather than only
        // ever having been open once.
        reaper.reap_expired(t0 + ORPHAN_SWEEP_INTERVAL_MS).unwrap();
        for a in a2.iter().copied() {
            assert_eq!(h.store.arena_owner(a), None, "the cadence never re-opened");
        }
    }

    #[test]
    fn a_crash_orphaned_extent_is_collected_when_the_store_is_reopened() {
        // Where the collector's job actually is after D40. A crash is its only producer, so the
        // complete answer is at open — and this runs it through the durable image rather than
        // in-memory state, because the image is what a real crash leaves behind.
        let (h, _reaper) = setup();
        let baseline_reserved = h.store.reserved_page_count();
        let (arenas, _dead) = orphan_one_extent(&h);
        h.store.flush().unwrap();
        let checkpoint = h.store.state_bytes();

        let store2 = h.fresh_store();
        store2.load_state(&checkpoint).unwrap();
        for a in arenas.iter().copied() {
            assert!(store2.arena_owner(a).is_some(), "fixture: the reopened image lost the orphan");
        }

        let reaper2 = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&store2));
        reaper2.resume_interrupted_reaps().unwrap();
        for a in arenas.iter().copied() {
            assert_eq!(store2.arena_owner(a), None, "open did not collect the crash orphan");
        }
        assert_eq!(
            store2.reserved_page_count(),
            baseline_reserved,
            "reserved pages did not return to baseline across the restart"
        );
    }

            }
        };
    }

    reaper_suite!(log_catalog, false);
    reaper_suite!(table_catalog, true);
}
