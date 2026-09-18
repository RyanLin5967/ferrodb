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

use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::branch::arena::ArenaPageStore;
use crate::branch::record::{CoreRecord, BranchRecord};
use crate::branch::types::{ArenaId, BranchError, BranchId, BranchState, Epoch, PageId};
use crate::branch::{BranchCatalog, Reaper};
use crate::cow::page_header::PageType;
use crate::cow::{PageStore, PAGE_HEADER_SIZE};
use crate::error::FerroError;
use crate::storage::disk_manager::PAGE_SIZE;

/// How `collapse` learns which pages a page points at, and how to repoint it.
///
/// Collapse must genuinely **materialise** the branch's visible state into its own arena before
/// re-parenting to trunk. Re-parenting without copying would strand the branch on pages owned by
/// ancestors that no longer list it in `live_children`, and the interval rule would then declare
/// those pages reclaimable while the branch is still reading them. Page layout belongs to the
/// B+tree module, so the walker is injected rather than guessed at — and when no walker is
/// supplied, [`TwoTierReaper::collapse`] **refuses** instead of silently corrupting the tree.
pub trait PageLinks: Send + Sync {
    /// Page ids this page points at. Must return an empty vector for leaves.
    ///
    /// Returns `Result` deliberately. A page whose links cannot be decoded is NOT a page with no
    /// links, and collapsing the two is silent corruption: `deep_copy` would copy the internal
    /// node without its subtree, then `collapse` re-parents the branch to trunk and detaches it
    /// from its old parent, leaving it rooted on ancestor-owned pages the interval rule is free
    /// to reclaim. That is precisely the corruption an injected walker exists to refuse, so it
    /// must be able to say "I cannot read this".
    fn child_pages(
        &self,
        page_type: PageType,
        page: &[u8; PAGE_SIZE],
    ) -> Result<Vec<PageId>, FerroError>;
    /// Repoint one link. Called once per child during the post-order copy.
    fn rewrite_child(&self, page: &mut [u8; PAGE_SIZE], old: PageId, new: PageId);
}

/// Guard against a cyclic or pathologically deep page graph during collapse.
const MAX_COLLAPSE_PAGES: usize = 1 << 16;

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
    links: Option<Arc<dyn PageLinks>>,
    /// Cluster-time stamp of the last crash-orphan collection, or [`ORPHAN_SWEEP_NEVER`].
    last_orphan_sweep_ms: AtomicU64,
}

impl TwoTierReaper {
    pub fn new(catalog: Arc<dyn BranchCatalog>, store: Arc<ArenaPageStore>) -> Self {
        TwoTierReaper {
            catalog,
            store,
            links: None,
            last_orphan_sweep_ms: AtomicU64::new(ORPHAN_SWEEP_NEVER),
        }
    }

    /// Supply the page-layout walker that `collapse` needs.
    pub fn with_links(mut self, links: Arc<dyn PageLinks>) -> Self {
        self.links = Some(links);
        self
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
        self.collect_orphaned_extents()?;
        Ok(done)
    }

    /// Remove this branch's fork epoch from its parent's live-children array. This is the single
    /// event that can make a parked page reclaimable, which is why `reap` always follows it with
    /// a `drain_pending`.
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
        if self.catalog.has_live_children(rec.branch_id.id)? {
            return Ok(());
        }
        let Some(parent) = rec.parent_id else { return Ok(()) };
        // One call rather than get/mutate/put. The old shape silently did nothing against any
        // catalog that keeps the live set in an index instead of inside the record - see
        // `BranchCatalog::detach_child`.
        self.catalog.detach_child(parent.id, rec.fork_epoch)?;

        // CASCADE. The parent may itself be a reaped branch that was pinned open only by the
        // child just removed. Without this the pin is permanent: the grandparent would keep
        // seeing a live child for ever and its pages could never be reclaimed -- trading a
        // correctness bug for an unbounded space leak, which is not a trade worth making.
        //
        // Terminates because each step moves strictly up the parent chain, whose length is capped
        // at `MAX_BRANCH_DEPTH`. Note the STEPS are bounded (<= 8); the WORK per step is not --
        // each one asks `has_live_children`, which scans a CHILD span and recurses into reaped
        // children. See the cost note in `table_catalog.rs::live_child_at`, which corrects an
        // earlier "O(1) in N" claim of mine that was simply wrong.
        if let Ok(prec) = self.catalog.get_raw(parent.id) {
            if prec.state == BranchState::Reaped {
                self.detach_from_parent(&prec)?;
            }
        }
        Ok(())
    }

    /// Is `arena` an extent whose owning branch no longer exists at that generation and which
    /// holds no allocated page any more?
    ///
    /// One catalog descent. Both the narrowed sweep and the orphan collector ask exactly this
    /// question; the only thing that ever differed between them is **which arenas it is asked
    /// about**, and stating it once is what keeps them from drifting apart.
    fn extent_is_collectable(&self, arena: ArenaId, owner: BranchId) -> bool {
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
    fn sweep_touched_extents(&self, touched: &BTreeSet<ArenaId>) -> Result<(), FerroError> {
        for arena in touched.iter().copied() {
            // Re-read the owner from the store rather than trusting the `PendingFree` entry's:
            // `free_arena` may already have removed the extent, in which case there is nothing to
            // collect and `owner_of` says so.
            let Some(owner) = self.store.arena_owner(arena) else { continue };
            if self.extent_is_collectable(arena, owner) {
                self.store.free_arena(arena)?;
            }
        }
        Ok(())
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
        self.collect_orphaned_extents()
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
        let mut touched = seed;
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
                    touched.insert(pf.arena_id);
                    released += 1;
                    moved = true;
                }
            }
            self.store.put_pending(still_pinned)?;
            if !moved {
                break;
            }
        }
        self.sweep_touched_extents(&touched)?;
        Ok(released)
    }

    /// Post-order copy of the page graph rooted at `page` into `branch`'s own extents, stamping
    /// every copy with `epoch` and repointing parents at their new children.
    ///
    /// **D13b — the arena is asked for PER PAGE, never captured once.** This took a single
    /// `ArenaId` and handed it to every `alloc_in_arena`, which is the one call in the store that
    /// deliberately refuses to grow an extent: its error says "ask `arena_for` for a fresh extent"
    /// and nothing here ever did. A tree larger than `ARENA_EXTENT_PAGES` (256 pages, ~1MB)
    /// therefore could not be collapsed at all -- and since `collapse` is the only escape from
    /// `MAX_BRANCH_DEPTH`, the ninth fork of any database over ~1MiB was a dead end.
    ///
    /// `arena_for` is what every other page allocator in the engine already uses (`cow_page`, all
    /// four B+tree split paths): it returns the branch's current extent while it has room and
    /// claims a fresh one when it does not, recording each fresh one against the branch inside
    /// `alloc_arena`'s atomic `add_arena`. That last part is why `collapse` must RE-READ the
    /// record before its final write; see there.
    fn deep_copy(
        &self,
        page: PageId,
        branch: BranchId,
        epoch: Epoch,
        links: &dyn PageLinks,
        seen: &mut HashSet<PageId>,
        budget: &mut usize,
    ) -> Result<PageId, FerroError> {
        if *budget == 0 {
            return Err(BranchError::Arena(format!(
                "collapse exceeded {} pages; the page graph is cyclic or larger than a branch",
                MAX_COLLAPSE_PAGES
            ))
            .into());
        }
        *budget -= 1;
        if !seen.insert(page) {
            return Err(BranchError::Arena(format!(
                "collapse revisited page {}; the page graph is not a tree",
                page
            ))
            .into());
        }

        let (page_type, data) = {
            let handle = self.store.read_page(page)?;
            (handle.header()?.page_type, handle.read().data)
        };

        let children = links.child_pages(page_type, &data)?;
        let mut rewrites = Vec::with_capacity(children.len());
        for child in children {
            let new_child = self.deep_copy(child, branch, epoch, links, seen, budget)?;
            rewrites.push((child, new_child));
        }

        let new_id = self.store.alloc_for(branch, page_type, epoch)?;
        let handle = self.store.read_page(new_id)?;
        {
            let mut frame = handle.write();
            frame.data[PAGE_HEADER_SIZE..].copy_from_slice(&data[PAGE_HEADER_SIZE..]);
            for (old, new) in rewrites {
                links.rewrite_child(&mut frame.data, old, new);
            }
            crate::cow::stamp_checksum(&mut frame.data);
        }
        Ok(new_id)
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
        rec.state = BranchState::Reaping;
        self.catalog.put(&rec)?;

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
        rec.mark_reaped(); // state = Reaped, generation += 1, arenas cleared
        self.catalog.put(&rec)?;
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

    fn collapse(&self, branch: BranchId) -> Result<BranchRecord, FerroError> {
        let Some(links) = self.links.clone() else {
            // Refuse rather than warn. Re-parenting without materialising would strand this
            // branch on ancestor-owned pages that the interval rule is then free to reclaim.
            return Err(BranchError::Arena(
                "collapse needs a PageLinks walker to materialise the branch before re-parenting; \
                 refusing to re-parent without copying"
                    .into(),
            )
            .into());
        };

        let rec = self.catalog.get(branch)?; // generation-guarded: never collapse a stale id
        if rec.branch_id.is_trunk() {
            return Err(BranchError::NotWritable(branch).into());
        }

        let new_fork_epoch = self.catalog.next_epoch();
        // One fresh extent to start in, so the materialised tree is physically clustered and does
        // not begin halfway through whatever the branch was last writing. `deep_copy` asks
        // `arena_for` from here on, which rolls over into further fresh extents as it fills them.
        self.store.alloc_arena(branch)?;
        let mut seen = HashSet::new();
        let mut budget = MAX_COLLAPSE_PAGES;
        let new_root = self.deep_copy(
            rec.root_page_id,
            branch,
            new_fork_epoch,
            &*links,
            &mut seen,
            &mut budget,
        )?;

        // Detach from the old parent only after the copy succeeded.
        self.detach_from_parent(&rec)?;

        // `attach_child`, not `get_raw`/`add_live_child`/`put`. The old shape mutated the parent's
        // RECORD, which writes nothing at all against a catalog that keeps children in an index:
        // the re-parented branch would be absent from trunk's live set and trunk's pages would look
        // unreferenced by it. Same failure `detach_child` was introduced for, in the other
        // direction.
        self.catalog.attach_child(BranchId::TRUNK.id, new_fork_epoch, rec.branch_id.id)?;

        // RE-READ before the final write, do not write back the snapshot taken above.
        //
        // The copy may have claimed SEVERAL extents, each recorded against this branch inside
        // `alloc_arena`'s atomic `add_arena`. Writing back `rec` as it stood before the copy would
        // drop every one of them from `record.arenas` -- and the reaper frees exactly
        // `record.arenas`, so those extents could never be reclaimed by anything. That is D20's
        // read-modify-write, arriving through `collapse` instead of through `alloc_arena`.
        //
        // `put` is still a whole-record write because `parent_id`, `depth` and `fork_epoch` have
        // no narrower setter and must move together with the root. The re-read narrows the window
        // to the four lines below rather than to the whole page copy.
        //
        // Measured by mutation: restore the pre-copy snapshot here and
        // `collapse_rolls_over_extents_for_a_tree_larger_than_one` leaves 664 of 664 copied pages
        // still allocated after the branch is reaped -- leaked for the life of the file.
        let mut rec = self.catalog.get(branch)?;
        rec.parent_id = Some(BranchId::TRUNK);
        rec.fork_epoch = new_fork_epoch;
        rec.depth = 1;
        rec.root_page_id = new_root;
        self.catalog.put(&rec)?;

        // The old ancestors just lost a child, so their parked pages may now be free.
        self.drain_pending()?;
        Ok(rec)
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
    use crate::branch::types::{ArenaId, LeaseDeadline, ARENA_EXTENT_PAGES};
    use crate::cow::stamp_checksum;

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
            let mut rec = h.catalog.get_raw(b.id).unwrap();
            rec.state = BranchState::Reaping;
            h.catalog.put(&rec).unwrap();
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
        let mut rec = h.catalog.get(b.branch_id).unwrap();
        rec.state = BranchState::Reaping;
        h.catalog.put(&rec).unwrap();
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

    // ---------------------------------------------------------------------------------------
    // Collapse.
    // ---------------------------------------------------------------------------------------

    /// A toy page layout for exercising collapse: byte 0 of the payload is the child count,
    /// followed by that many big-endian u32 page ids.
    struct ToyLinks;

    impl ToyLinks {
        fn write(handle_data: &mut [u8; PAGE_SIZE], children: &[PageId]) {
            handle_data[PAGE_HEADER_SIZE] = children.len() as u8;
            for (i, c) in children.iter().enumerate() {
                let at = PAGE_HEADER_SIZE + 1 + i * 4;
                handle_data[at..at + 4].copy_from_slice(&c.to_be_bytes());
            }
        }
    }

    impl PageLinks for ToyLinks {
        fn child_pages(
            &self,
            page_type: PageType,
            page: &[u8; PAGE_SIZE],
        ) -> Result<Vec<PageId>, FerroError> {
            if page_type != PageType::BTreeInternal {
                return Ok(Vec::new());
            }
            let n = page[PAGE_HEADER_SIZE] as usize;
            let collected = (0..n)
                .map(|i| {
                    let at = PAGE_HEADER_SIZE + 1 + i * 4;
                    u32::from_be_bytes(page[at..at + 4].try_into().unwrap())
                })
                .collect::<Vec<_>>();
            Ok(collected)
        }

        fn rewrite_child(&self, page: &mut [u8; PAGE_SIZE], old: PageId, new: PageId) {
            let n = page[PAGE_HEADER_SIZE] as usize;
            for i in 0..n {
                let at = PAGE_HEADER_SIZE + 1 + i * 4;
                if u32::from_be_bytes(page[at..at + 4].try_into().unwrap()) == old {
                    page[at..at + 4].copy_from_slice(&new.to_be_bytes());
                }
            }
        }
    }

    #[test]
    fn collapse_refuses_without_a_page_walker() {
        let (h, reaper) = setup();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let err = reaper.collapse(b.branch_id).unwrap_err();
        assert!(err.to_string().contains("refusing to re-parent"), "got {}", err);
        assert_eq!(h.catalog.get(b.branch_id).unwrap().depth, 1, "nothing was changed");
    }

    #[test]
    fn collapse_materialises_the_whole_reachable_tree_and_reparents_to_trunk() {
        let h = Harness::new_with($table);
        let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store))
            .with_links(Arc::new(ToyLinks));

        // Build a 3-page tree owned by an ancestor, then a deep chain that inherits it.
        let anc = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let e = h.catalog.next_epoch();
        let leaf_a = h.store.alloc_for(anc.branch_id, PageType::BTreeLeaf, e).unwrap();
        let leaf_b = h.store.alloc_for(anc.branch_id, PageType::BTreeLeaf, e).unwrap();
        for (p, tag) in [(leaf_a, 0xA1u8), (leaf_b, 0xB2)] {
            let handle = h.store.read_page(p).unwrap();
            let mut f = handle.write();
            f.data[PAGE_HEADER_SIZE + 32] = tag;
            stamp_checksum(&mut f.data);
        }
        let root = h.store.alloc_for(anc.branch_id, PageType::BTreeInternal, e).unwrap();
        {
            let handle = h.store.read_page(root).unwrap();
            let mut f = handle.write();
            ToyLinks::write(&mut f.data, &[leaf_a, leaf_b]);
            stamp_checksum(&mut f.data);
        }

        let mut cur = anc.branch_id;
        for _ in 0..6 {
            cur = h.catalog.fork(cur, LeaseDeadline(0)).unwrap().branch_id;
        }
        h.catalog.set_root(cur, root).unwrap();
        let deep = h.catalog.get(cur).unwrap();
        assert_eq!(deep.depth, 7);
        let old_parent = deep.parent_id.unwrap();
        let live_before = h.store.live_page_count().unwrap();

        let collapsed = reaper.collapse(cur).unwrap();

        assert_eq!(collapsed.depth, 1);
        assert_eq!(collapsed.parent_id, Some(BranchId::TRUNK));
        assert_ne!(collapsed.root_page_id, root, "the root was materialised, not aliased");
        assert_eq!(
            h.store.live_page_count().unwrap(),
            live_before + 3,
            "every reachable page was copied"
        );
        // D19: asked of the CATALOG -- the record's `live_children` is empty by contract on
        // the table catalog, so the original form passed vacuously there.
        assert!(
            !h.catalog
                .live_child_in_epoch_range(
                    old_parent.id,
                    deep.fork_epoch,
                    Epoch(deep.fork_epoch.0 + 1)
                )
                .unwrap(),
            "the old parent no longer pins anything for this branch"
        );
        // D19: asked of the CATALOG. This one FAILED on the table catalog rather than passing
        // vacuously, because it asserts PRESENCE in a field that catalog never populates.
        assert!(h
            .catalog
            .live_child_in_epoch_range(
                BranchId::TRUNK.id,
                collapsed.fork_epoch,
                Epoch(collapsed.fork_epoch.0 + 1)
            )
            .unwrap());

        // The copy is a real copy: same payload, new ids, all in the branch's own arena.
        let new_root_handle = h.store.read_page(collapsed.root_page_id).unwrap();
        let new_children = ToyLinks
            .child_pages(PageType::BTreeInternal, &new_root_handle.read().data)
            .expect("toy links decode");
        assert_eq!(new_children.len(), 2);
        assert_ne!(new_children[0], leaf_a);
        for (p, tag) in new_children.iter().zip([0xA1u8, 0xB2]) {
            let handle = h.store.read_page(*p).unwrap();
            assert_eq!(handle.read().data[PAGE_HEADER_SIZE + 32], tag);
            // The branch's OWN extents, not one specific extent. This used to read
            // `== *collapsed.arenas.last().unwrap()`, which was the same statement only while a
            // branch had exactly one extent; since D31 a three-page copy spans two.
            let landed = handle.header().unwrap().arena_id;
            assert!(
                collapsed.arenas.contains(&landed),
                "a materialised page landed in {landed}, which the branch does not own: {:?}",
                collapsed.arenas
            );
        }
        // And the ancestor's originals are untouched.
        assert_eq!(pages_of(&h, anc.branch_id), 3);
        let _ = ArenaId(0);
    }

    /// D6's actual claim: the depth guard and collapse are one mechanism, not two features.
    ///
    /// The guard refusing the ninth fork is only useful if there is a way forward afterwards, and
    /// collapse is only motivated by the guard. Each was tested alone; neither test showed that a
    /// chain which has hit the ceiling can carry on working.
    #[test]
    fn a_chain_at_max_depth_can_only_fork_again_after_collapsing() {
        let h = Harness::new_with($table);
        let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store))
            .with_links(Arc::new(ToyLinks));

        let mut cur = BranchId::TRUNK;
        use crate::branch::types::MAX_BRANCH_DEPTH;
        for _ in 0..MAX_BRANCH_DEPTH {
            cur = h.catalog.fork(cur, LeaseDeadline::from_now(LEASE_MS)).unwrap().branch_id;
        }
        assert_eq!(h.catalog.get(cur).unwrap().depth, MAX_BRANCH_DEPTH);

        let err = h
            .catalog
            .fork(cur, LeaseDeadline::from_now(LEASE_MS))
            .expect_err("the ninth fork must be refused");
        assert!(err.to_string().contains("depth"), "got {err}");

        // Give the chain a real root before collapsing. A branch that has never written still
        // carries the trunk's placeholder root id, which is not an allocated page — collapse then
        // fails while trying to copy it, with an IO error rather than anything explanatory.
        let ep = h.catalog.next_epoch();
        let root = h.store.alloc_for(cur, PageType::BTreeLeaf, ep).unwrap();
        {
            let handle = h.store.read_page(root).unwrap();
            let mut f = handle.write();
            stamp_checksum(&mut f.data);
        }
        h.catalog.set_root(cur, root).unwrap();

        let collapsed = reaper.collapse(cur).unwrap();
        assert_eq!(collapsed.depth, 1, "collapse must reset the chain, not just move it");
        assert_eq!(collapsed.parent_id, Some(BranchId::TRUNK));

        // The whole point: the branch is usable again.
        let child = h
            .catalog
            .fork(cur, LeaseDeadline::from_now(LEASE_MS))
            .expect("forking after a collapse must succeed, or the ceiling is permanent");
        assert_eq!(h.catalog.get(child.branch_id).unwrap().depth, 2);
    }

    /// Why collapse **materialises** instead of merely re-pointing the parent.
    ///
    /// A re-parent alone would leave the branch reading pages its ancestors own. Those ancestors
    /// are exactly what the collapse exists to let go of, and the interval rule would then be free
    /// to reclaim pages still under the collapsed branch's root — silent corruption, visible only
    /// later as a checksum failure or a wrong answer. So the test does the dangerous thing on
    /// purpose: reap the entire ancestor chain afterwards and read the data back.
    #[test]
    fn after_collapse_the_ancestor_chain_can_be_reaped_and_the_data_survives() {
        let h = Harness::new_with($table);
        let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store))
            .with_links(Arc::new(ToyLinks));

        // An ancestor owns the pages; a deep chain inherits them.
        let anc = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
        let e = h.catalog.next_epoch();
        let leaf = h.store.alloc_for(anc.branch_id, PageType::BTreeLeaf, e).unwrap();
        {
            let handle = h.store.read_page(leaf).unwrap();
            let mut f = handle.write();
            f.data[PAGE_HEADER_SIZE + 32] = 0xD6;
            stamp_checksum(&mut f.data);
        }
        let root = h.store.alloc_for(anc.branch_id, PageType::BTreeInternal, e).unwrap();
        {
            let handle = h.store.read_page(root).unwrap();
            let mut f = handle.write();
            ToyLinks::write(&mut f.data, &[leaf]);
            stamp_checksum(&mut f.data);
        }

        let mut cur = anc.branch_id;
        for _ in 0..6 {
            cur = h.catalog.fork(cur, LeaseDeadline::from_now(LEASE_MS)).unwrap().branch_id;
        }
        h.catalog.set_root(cur, root).unwrap();

        let collapsed = reaper.collapse(cur).unwrap();
        let own_root = collapsed.root_page_id;
        assert_ne!(own_root, root, "collapse aliased the ancestor's page instead of copying it");

        // Keep the collapsed branch alive; `far_future()` expires every lease, including its own,
        // and reaping the subject along with the ancestors would prove nothing about either.
        h.catalog.renew_lease(cur, LeaseDeadline(u64::MAX)).unwrap();

        // Now let go of every ancestor. Before the collapse this would have been unsafe.
        let reaped = reaper.reap_expired(far_future()).unwrap();
        assert!(
            reaped.len() >= 6,
            "the ancestor chain did not go away, so this proves nothing: {reaped:?}"
        );
        assert!(
            !reaped.contains(&cur),
            "the collapsed branch was reaped along with its old ancestors"
        );
        reaper.drain_pending().ok();

        // The collapsed branch still answers, from pages it owns.
        let children = ToyLinks
            .child_pages(PageType::BTreeInternal, &h.store.read_page(own_root).unwrap().read().data)
            .unwrap();
        assert_eq!(children.len(), 1, "the materialised root lost its child");
        let handle = h.store.read_page(children[0]).unwrap();
        assert_eq!(
            handle.read().data[PAGE_HEADER_SIZE + 32],
            0xD6,
            "the collapsed branch's data did not survive its ancestors being reclaimed"
        );
    }

    #[test]
    fn collapse_refuses_a_cyclic_page_graph() {
        let h = Harness::new_with($table);
        let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store))
            .with_links(Arc::new(ToyLinks));
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let e = h.catalog.next_epoch();
        let p1 = h.store.alloc_for(b.branch_id, PageType::BTreeInternal, e).unwrap();
        let p2 = h.store.alloc_for(b.branch_id, PageType::BTreeInternal, e).unwrap();
        for (a, c) in [(p1, p2), (p2, p1)] {
            let handle = h.store.read_page(a).unwrap();
            let mut f = handle.write();
            ToyLinks::write(&mut f.data, &[c]);
            stamp_checksum(&mut f.data);
        }
        h.catalog.set_root(b.branch_id, p1).unwrap();
        let err = reaper.collapse(b.branch_id).unwrap_err();
        assert!(err.to_string().contains("not a tree"), "got {}", err);
    }

    /// **D13b — `collapse` must roll over extents, or it cannot collapse a real database.**
    ///
    /// `collapse` allocated ONE arena and threaded that single id through every `deep_copy`.
    /// `alloc_in_arena` deliberately refuses to grow an extent — its own error says "ask
    /// `arena_for` for a fresh extent" — so the copy died the moment the materialised tree
    /// crossed `ARENA_EXTENT_PAGES` (256 pages, ~1MB at 4KB pages).
    ///
    /// That is not a corner: `collapse` is the ONLY escape from `MAX_BRANCH_DEPTH`, so the ninth
    /// fork of any database over ~1MiB was a dead end — the fork is refused, and the one operation
    /// that clears the refusal cannot run. Every existing collapse test copies a 1–3 page tree,
    /// which is why the whole suite stayed green through it.
    ///
    /// The tree below is bigger than TWO extents on purpose, so a fix that rolls over exactly once
    /// does not pass either. And the record's arena list is checked by OUTCOME — the branch is
    /// reaped and the reserved count must return to baseline — because the reaper frees exactly
    /// `record.arenas`: a rollover that allocates extents the record never learns about trades an
    /// exhaustion error for a permanent space leak (D20, in the other direction).
    #[test]
    fn collapse_rolls_over_extents_for_a_tree_larger_than_one() {
        const FANOUT: usize = 3;
        const LEAVES_PER: usize = 220;
        // 1 root + 3 internal + 660 leaves = 664 pages = 2.6 extents.
        const TREE_PAGES: u32 = (1 + FANOUT + FANOUT * LEAVES_PER) as u32;
        const EXTENTS: usize =
            ((TREE_PAGES + ARENA_EXTENT_PAGES - 1) / ARENA_EXTENT_PAGES) as usize;

        let h = Harness::new_with($table);
        let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store))
            .with_links(Arc::new(ToyLinks));

        let baseline_live = h.store.live_page_count().unwrap();
        let baseline_reserved = h.store.reserved_page_count();

        // A chain sitting exactly where the depth guard leaves one.
        let mut cur = BranchId::TRUNK;
        for _ in 0..crate::branch::types::MAX_BRANCH_DEPTH {
            cur = h.catalog.fork(cur, LeaseDeadline::from_now(LEASE_MS)).unwrap().branch_id;
        }
        assert!(
            h.catalog.fork(cur, LeaseDeadline::from_now(LEASE_MS)).is_err(),
            "the chain is not at the ceiling, so collapse is not the only way forward"
        );

        // Build its tree. Note `arena_for` per page rather than one captured `alloc_arena` id:
        // the store could ALREADY roll over, and a normal writer gets it for free. Collapse was
        // the one caller that did not ask.
        let epoch = h.catalog.next_epoch();
        let mut leaves_all: Vec<PageId> = Vec::with_capacity(FANOUT * LEAVES_PER);
        let mut internals: Vec<PageId> = Vec::with_capacity(FANOUT);
        for _ in 0..FANOUT {
            let mut leaves = Vec::with_capacity(LEAVES_PER);
            for _ in 0..LEAVES_PER {
                let p = h.store.alloc_for(cur, PageType::BTreeLeaf, epoch).unwrap();
                let handle = h.store.read_page(p).unwrap();
                let mut f = handle.write();
                f.data[PAGE_HEADER_SIZE + 32] = (leaves_all.len() % 256) as u8;
                stamp_checksum(&mut f.data);
                drop(f);
                leaves.push(p);
                leaves_all.push(p);
            }
            let node = h.store.alloc_for(cur, PageType::BTreeInternal, epoch).unwrap();
            let handle = h.store.read_page(node).unwrap();
            let mut f = handle.write();
            ToyLinks::write(&mut f.data, &leaves);
            stamp_checksum(&mut f.data);
            drop(f);
            internals.push(node);
        }
        let root = h.store.alloc_for(cur, PageType::BTreeInternal, epoch).unwrap();
        {
            let handle = h.store.read_page(root).unwrap();
            let mut f = handle.write();
            ToyLinks::write(&mut f.data, &internals);
            stamp_checksum(&mut f.data);
        }
        h.catalog.set_root(cur, root).unwrap();

        assert!(
            TREE_PAGES > 2 * ARENA_EXTENT_PAGES,
            "a tree that fits in two extents does not test rollover"
        );
        assert_eq!(
            h.store.live_page_count().unwrap(),
            baseline_live + TREE_PAGES,
            "the tree was not built"
        );

        let collapsed = reaper
            .collapse(cur)
            .expect("collapse must materialise a tree larger than one extent");

        assert_eq!(collapsed.depth, 1);
        assert_eq!(collapsed.parent_id, Some(BranchId::TRUNK));
        assert_eq!(
            h.store.live_page_count().unwrap(),
            baseline_live + 2 * TREE_PAGES,
            "every reachable page must be copied exactly once"
        );

        // Walk the materialised tree: right shape, right payloads, no original id reused, and
        // spread across as many extents as it needs.
        let originals: HashSet<PageId> =
            leaves_all.iter().chain(internals.iter()).copied().chain([root]).collect();
        let mut copy_arenas: HashSet<ArenaId> = HashSet::new();
        let mut tags: Vec<u8> = Vec::with_capacity(FANOUT * LEAVES_PER);
        let mut copied = 0u32;
        let mut stack = vec![collapsed.root_page_id];
        while let Some(p) = stack.pop() {
            assert!(
                !originals.contains(&p),
                "collapse aliased original page {p} instead of copying it"
            );
            let (page_type, arena_id, data) = {
                let handle = h.store.read_page(p).unwrap();
                let hd = handle.header().unwrap();
                (hd.page_type, hd.arena_id, handle.read().data)
            };
            copy_arenas.insert(arena_id);
            copied += 1;
            let kids = ToyLinks.child_pages(page_type, &data).expect("toy links decode");
            if kids.is_empty() {
                tags.push(data[PAGE_HEADER_SIZE + 32]);
            }
            stack.extend(kids);
        }
        assert_eq!(copied, TREE_PAGES, "the materialised tree is the wrong size");
        tags.sort_unstable();
        let mut want: Vec<u8> = (0..FANOUT * LEAVES_PER).map(|i| (i % 256) as u8).collect();
        want.sort_unstable();
        assert_eq!(tags, want, "the copied leaves lost their payloads");

        assert_eq!(
            copy_arenas.len(),
            EXTENTS,
            "a {TREE_PAGES}-page copy landed in {} extent(s) of {ARENA_EXTENT_PAGES}; \
             collapse did not roll over",
            copy_arenas.len()
        );
        for a in &copy_arenas {
            assert!(
                collapsed.arenas.contains(a),
                "arena {a} holds copied pages but is absent from the record. The reaper frees \
                 exactly `record.arenas`, so this extent could never be reclaimed."
            );
        }
        // The two trees are reserved under DIFFERENT geometries, which is the point of D31.
        //
        //   source: the branch starts with nothing, so it climbs 1+2+4+...+128+256+256 = 767
        //           pages across ten extents to hold 664.
        //   copy:   by then the branch is already at the 256-page cap, so `collapse` claims
        //           3 x 256 = 768.
        //
        // Spelled as arithmetic rather than as `2 * EXTENTS * ARENA_EXTENT_PAGES`, which was the
        // uniform-extent statement and is now wrong by exactly the one page the climb saves.
        const SOURCE_RESERVED: u32 = 1 + 2 + 4 + 8 + 16 + 32 + 64 + 128 + 256 + 256;
        const COPY_RESERVED: u32 = 3 * ARENA_EXTENT_PAGES;
        assert_eq!(SOURCE_RESERVED, 767);
        assert_eq!(
            h.store.reserved_page_count(),
            baseline_reserved + SOURCE_RESERVED + COPY_RESERVED,
            "the original tree and its copy do not hold the extents the growth rule predicts"
        );

        // THE OUTCOME, not the field. The reaper frees exactly `record.arenas`; if collapse
        // forgot a rolled-over extent, the space never comes back.
        reaper.reap(cur).expect("reap the collapsed branch");
        assert_eq!(
            h.store.live_page_count().unwrap(),
            baseline_live,
            "pages survived the reap: collapse allocated extents the record does not list"
        );
        assert_eq!(
            h.store.reserved_page_count(),
            baseline_reserved,
            "extents survived the reap: collapse allocated extents the record does not list"
        );
    }

    /// **Known-open, ledger row D29: `collapse`'s final `put` is still a whole-record write.**
    ///
    /// D13b narrowed the window — `collapse` re-reads the record immediately before writing it,
    /// instead of writing back the snapshot it took before copying the whole tree — but it did not
    /// close it, because `parent_id`, `depth` and `fork_epoch` have no narrower setter and must
    /// move together with the new root. Anything that mutates the record between that re-read and
    /// that `put` is silently discarded.
    ///
    /// **WHICH DIRECTION D13b AND D31 MOVED THE WINDOW, because the D29 fix needs to know what it
    /// is aiming at.** Both NARROWED it; neither widened it.
    ///
    /// * D13b: before it, `collapse` took its snapshot ABOVE `deep_copy` and wrote it back after,
    ///   so the window was the whole page copy — unbounded in the size of the tree. It is now the
    ///   four field assignments below the re-read.
    /// * D31 (geometric extents): the extra `alloc_arena`/`add_arena` round-trips a collapse now
    ///   makes all happen DURING the copy, i.e. above the re-read and outside the window. What
    ///   D31 does change is the COST of removing the re-read: a collapse now claims several
    ///   extents instead of one, so writing back a pre-copy snapshot would drop all of them from
    ///   `record.arenas` and leak the lot. Mutant D in the D13b fire-check measured exactly that —
    ///   664 of 664 copied pages unreclaimable after the branch was reaped.
    ///
    /// **Why `reap` has the same shape and is safe, which is the clue to the cheap fix.** `reap`
    /// publishes `Reaping` durably BEFORE it frees anything, and `check_readable` rejects that
    /// state — so nothing can land in its window, because every writer is already refused.
    /// `renew_lease` goes through `check_readable` on both catalogs, so a `collapse` that
    /// published such a marker would make the keepalive below REFUSE rather than be silently lost.
    /// By inspection, not measured, and not free either: `reap` marks branches that are dying,
    /// while `collapse` runs on a LIVE branch, so making it briefly unreadable is a real
    /// behaviour change and not a drop-in swap. Recorded as a lead worth costing, not a
    /// recommendation.
    ///
    /// This drives the smallest of those: a lease renewal. The wrapper below renews the lease to
    /// `u64::MAX` at the exact instant of collapse's re-read, which is what a live client's
    /// keepalive does. Collapse then writes the pre-renewal deadline back on top, and the branch is
    /// reapable while its holder believes the lease is good — `reap_expired` needs no cooperation
    /// at all, so nothing else stands between that and the branch being reclaimed underneath it.
    ///
    /// `renew_lease` is the cheapest field to demonstrate with, not the worst case. `envelope`
    /// (a `charge_row_writes` spend) and `state` travel in the same record.
    ///
    /// **This is NOT the D13b rollover defect and is NOT fixed here.** Closing it needs exclusion
    /// against a concurrent writer on the branch being collapsed, or a narrow
    /// `reparent(branch, parent, epoch, root)` catalog operation that each implementation makes
    /// atomic — the shape `add_arena` and `detach_child` already took, for this same reason. That
    /// is a catalog-trait change and belongs with the latching work, not inside a page-copy fix.
    #[test]
    #[ignore = "known-open defect: collapse's whole-record `put` discards a concurrent write. \
                Needs a narrow reparent() catalog op or exclusion; run with --ignored"]
    fn collapse_discards_a_lease_renewal_that_lands_on_its_re_read() {
        use std::sync::Mutex;

        /// Renews the subject's lease at the instant of collapse's SECOND `get` — the re-read.
        struct RacingKeepalive {
            inner: Arc<dyn BranchCatalog>,
            subject: Mutex<Option<BranchId>>,
            gets: Mutex<u32>,
            fired: Mutex<bool>,
        }
        impl BranchCatalog for RacingKeepalive {
            fn get(&self, b: BranchId) -> Result<BranchRecord, FerroError> {
                let rec = self.inner.get(b)?;
                let subject = *self.subject.lock().unwrap();
                if subject == Some(b) {
                    let mut n = self.gets.lock().unwrap();
                    *n += 1;
                    if *n == 2 {
                        // A live client's keepalive, landing in the window.
                        self.inner.renew_lease(b, LeaseDeadline(u64::MAX)).unwrap();
                        *self.fired.lock().unwrap() = true;
                    }
                }
                Ok(rec)
            }
            fn next_epoch(&self) -> Epoch { self.inner.next_epoch() }
            fn current_epoch(&self) -> Epoch { self.inner.current_epoch() }
            fn fork(&self, p: BranchId, l: LeaseDeadline) -> Result<BranchRecord, FerroError> {
                self.inner.fork(p, l)
            }
            fn add_arena(&self, b: BranchId, a: ArenaId) -> Result<(), FerroError> {
                self.inner.add_arena(b, a)
            }
            fn put(&self, r: &BranchRecord) -> Result<(), FerroError> { self.inner.put(r) }
            fn set_root(&self, b: BranchId, r: PageId) -> Result<(), FerroError> {
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
        let racer = Arc::new(RacingKeepalive {
            inner: Arc::clone(&h.catalog),
            subject: Mutex::new(None),
            gets: Mutex::new(0),
            fired: Mutex::new(false),
        });
        let reaper =
            TwoTierReaper::new(Arc::clone(&racer) as Arc<dyn BranchCatalog>, Arc::clone(&h.store))
                .with_links(Arc::new(ToyLinks));

        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
        let e = h.catalog.next_epoch();
        let root = h.store.alloc_for(b.branch_id, PageType::BTreeLeaf, e).unwrap();
        {
            let handle = h.store.read_page(root).unwrap();
            let mut f = handle.write();
            stamp_checksum(&mut f.data);
        }
        h.catalog.set_root(b.branch_id, root).unwrap();
        *racer.subject.lock().unwrap() = Some(b.branch_id);

        reaper.collapse(b.branch_id).expect("collapse");

        // Never let this pass vacuously: if the interleaving did not happen there is nothing to
        // assert about, and a green line would be a lie about a defect that is still there.
        assert!(
            *racer.fired.lock().unwrap(),
            "the keepalive never fired, so this test proves nothing either way"
        );
        assert_eq!(
            h.catalog.get(b.branch_id).unwrap().lease_deadline,
            LeaseDeadline(u64::MAX),
            "collapse's whole-record `put` overwrote a lease renewal that landed after its \
             re-read. The branch is now reapable while its holder believes the lease is live, and \
             `reap_expired` needs no cooperation from that holder."
        );
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
            fn put(&self, r: &BranchRecord) -> Result<(), FerroError> {
                if r.state == BranchState::Reaped {
                    self.log.lock().unwrap().push("mark_reaped");
                }
                self.inner.put(r)
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
        let mut rec = h.catalog.get(b.branch_id).unwrap();
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
        // ...and now the crash. `mark_reaped` bumps the generation and clears `rec.arenas`, so
        // after this write nothing in the catalog names these extents ever again — which is the
        // whole reason a global scan is the only instrument that can find them.
        rec.mark_reaped();
        h.catalog.put(&rec).unwrap();
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
        let mut drec = h.catalog.get(doomed.branch_id).unwrap();
        let doomed_arenas: Vec<ArenaId> = drec.arenas.clone();
        drec.mark_reaped();
        h.catalog.put(&drec).unwrap();
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
