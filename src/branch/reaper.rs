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

use std::collections::HashSet;
use std::sync::Arc;

use crate::branch::arena::ArenaPageStore;
use crate::branch::catalog::LogBranchCatalog;
use crate::branch::record::{reclaimable, BranchRecord};
use crate::branch::types::{BranchError, BranchId, BranchState, Epoch, PageId};
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
    fn child_pages(&self, page_type: PageType, page: &[u8; PAGE_SIZE]) -> Vec<PageId>;
    /// Repoint one link. Called once per child during the post-order copy.
    fn rewrite_child(&self, page: &mut [u8; PAGE_SIZE], old: PageId, new: PageId);
}

/// Guard against a cyclic or pathologically deep page graph during collapse.
const MAX_COLLAPSE_PAGES: usize = 1 << 16;

pub struct TwoTierReaper {
    catalog: Arc<LogBranchCatalog>,
    store: Arc<ArenaPageStore>,
    links: Option<Arc<dyn PageLinks>>,
}

impl TwoTierReaper {
    pub fn new(catalog: Arc<LogBranchCatalog>, store: Arc<ArenaPageStore>) -> Self {
        TwoTierReaper { catalog, store, links: None }
    }

    /// Supply the page-layout walker that `collapse` needs.
    pub fn with_links(mut self, links: Arc<dyn PageLinks>) -> Self {
        self.links = Some(links);
        self
    }

    /// Remove this branch's fork epoch from its parent's live-children array. This is the single
    /// event that can make a parked page reclaimable, which is why `reap` always follows it with
    /// a `drain_pending`.
    fn detach_from_parent(&self, rec: &BranchRecord) -> Result<(), FerroError> {
        let Some(parent) = rec.parent_id else { return Ok(()) };
        let Ok(mut prec) = self.catalog.get_raw(parent.id) else { return Ok(()) };
        if prec.remove_live_child(rec.fork_epoch) {
            self.catalog.put(&prec)?;
        }
        Ok(())
    }

    /// Extents whose owning branch no longer exists at that generation and which hold no
    /// allocated page any more. Freeing them is what returns the *reserved* page count to
    /// baseline rather than merely stopping its growth.
    fn sweep_empty_extents(&self) -> Result<(), FerroError> {
        for (arena, owner) in self.store.live_arenas() {
            let owner_gone = match self.catalog.get_raw(owner.id) {
                Ok(rec) => rec.generation != owner.generation || rec.state == BranchState::Reaped,
                Err(_) => true,
            };
            if owner_gone && self.store.extent_is_empty(arena) {
                self.store.free_arena(arena)?;
            }
        }
        Ok(())
    }

    /// Post-order copy of the page graph rooted at `page` into `arena`, stamping every copy with
    /// `epoch` and repointing parents at their new children.
    fn deep_copy(
        &self,
        page: PageId,
        arena: crate::branch::types::ArenaId,
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

        let children = links.child_pages(page_type, &data);
        let mut rewrites = Vec::with_capacity(children.len());
        for child in children {
            let new_child = self.deep_copy(child, arena, epoch, links, seen, budget)?;
            rewrites.push((child, new_child));
        }

        let new_id = self.store.alloc_in_arena(arena, page_type, epoch)?;
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

        if rec.is_childless_leaf() {
            // FAST PATH. No sharing analysis: nobody forked off this branch, so nothing outside
            // it can see a page born inside its own extents.
            for arena in rec.arenas.iter().copied() {
                freed += self.store.free_arena(arena)?;
            }
        } else {
            // SLOW PATH. Every page goes through the interval rule; survivors are parked.
            freed += self.store.retire_arenas_by_rule(&rec, free_epoch)?;
        }

        self.detach_from_parent(&rec)?;

        rec.mark_reaped(); // state = Reaped, generation += 1, arenas cleared
        self.catalog.put(&rec)?;
        self.catalog.release_id(rec.branch_id.id);

        // The parent's live-children array just shrank, so pages parked against it may have
        // become reclaimable. This is the only moment that can happen.
        freed += self.drain_pending()?;
        Ok(freed)
    }

    fn reap_expired(&self, now_millis: u64) -> Result<Vec<BranchId>, FerroError> {
        let mut candidates: Vec<BranchRecord> = self
            .catalog
            .live_branches()?
            .into_iter()
            .filter(|r| !r.branch_id.is_trunk() && r.lease_deadline.is_expired_at(now_millis))
            .collect();

        // Deepest first: reaping a child removes its epoch from the parent's live-children array,
        // which is exactly what lets the parent's own reap take the fast path.
        candidates.sort_by(|a, b| b.depth.cmp(&a.depth).then(b.fork_epoch.cmp(&a.fork_epoch)));

        let mut reaped = Vec::with_capacity(candidates.len());
        for rec in candidates {
            match self.reap(rec.branch_id) {
                Ok(_) => reaped.push(rec.branch_id),
                // A branch already reaped as a side effect of this same scan is not an error.
                Err(FerroError::Branch(_)) => {}
                Err(e) => return Err(e),
            }
        }

        // Anything whose pages all went back but whose extent was still registered.
        self.sweep_empty_extents()?;
        Ok(reaped)
    }

    fn drain_pending(&self) -> Result<u32, FerroError> {
        let mut released = 0u32;
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
                    Ok(rec) => !reclaimable(&rec.live_children, pf.birth_epoch, pf.free_epoch),
                    // No record at all: nothing can be forked off it, so nothing can see the page.
                    Err(_) => false,
                };
                if pinned {
                    still_pinned.push(pf);
                } else {
                    self.store.release_page(pf.page_id, pf.arena_id);
                    released += 1;
                    moved = true;
                }
            }
            self.store.put_pending(still_pinned);
            if !moved {
                break;
            }
        }
        self.sweep_empty_extents()?;
        Ok(released)
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

        let mut rec = self.catalog.get(branch)?; // generation-guarded: never collapse a stale id
        if rec.branch_id.is_trunk() {
            return Err(BranchError::NotWritable(branch).into());
        }

        let new_fork_epoch = self.catalog.next_epoch();
        let arena = self.store.alloc_arena(branch)?;
        let mut seen = HashSet::new();
        let mut budget = MAX_COLLAPSE_PAGES;
        let new_root = self.deep_copy(
            rec.root_page_id,
            arena,
            new_fork_epoch,
            &*links,
            &mut seen,
            &mut budget,
        )?;

        // Detach from the old parent only after the copy succeeded.
        self.detach_from_parent(&rec)?;

        let mut trunk = self.catalog.get_raw(BranchId::TRUNK.id)?;
        trunk.add_live_child(new_fork_epoch);
        self.catalog.put(&trunk)?;

        rec.parent_id = Some(trunk.branch_id);
        rec.fork_epoch = new_fork_epoch;
        rec.depth = 1;
        rec.root_page_id = new_root;
        if !rec.arenas.contains(&arena) {
            rec.arenas.push(arena);
        }
        self.catalog.put(&rec)?;

        // The old ancestors just lost a child, so their parked pages may now be free.
        self.drain_pending()?;
        Ok(rec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let h = Harness::new();
        let r = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store));
        (h, r)
    }

    /// Do what an agent task does: take an arena and write `pages` novel pages into it.
    fn write_pages(h: &Harness, branch: BranchId, pages: u32) -> Vec<PageId> {
        let arena = h.store.arena_for(branch).unwrap();
        let epoch = h.catalog.next_epoch();
        (0..pages)
            .map(|i| {
                let p = h.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
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
        assert_eq!(h.store.allocated_pages(h.store.arena_for(kept.branch_id).unwrap()).len(), 3);
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
        assert!(h.catalog.get(b.branch_id).unwrap().is_childless_leaf());

        assert_eq!(reaper.reap(b.branch_id).unwrap(), 11);
        assert_eq!(h.store.arena_owner(arena), None, "the extent went back whole");
        assert_eq!(h.store.reserved_page_count(), 0);
        assert_eq!(h.store.pending_len(), 0);
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
        assert_eq!(h.catalog.get(BranchId::TRUNK).unwrap().live_children, Vec::new());
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
    fn trunk_is_never_reaped() {
        let (h, reaper) = setup();
        assert!(reaper.reap(BranchId::TRUNK).is_err());
        assert!(reaper.reap_expired(u64::MAX).unwrap().is_empty());
        assert!(h.catalog.get(BranchId::TRUNK).is_ok());
    }

    #[test]
    fn a_freed_extent_is_reused_so_a_second_wave_costs_no_new_space() {
        let (h, reaper) = setup();
        let mut high = 0;
        for _ in 0..3 {
            for _ in 0..4 {
                let b =
                    h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(LEASE_MS)).unwrap();
                write_pages(&h, b.branch_id, 9);
            }
            high = high.max(h.store.reserved_page_count());
            reaper.reap_expired(far_future()).unwrap();
            assert_eq!(h.store.live_page_count().unwrap(), 0);
            assert_eq!(h.store.reserved_page_count(), 0);
        }
        assert_eq!(high, 4 * ARENA_EXTENT_PAGES, "three waves never exceeded four extents");
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
        fn child_pages(&self, page_type: PageType, page: &[u8; PAGE_SIZE]) -> Vec<PageId> {
            if page_type != PageType::BTreeInternal {
                return Vec::new();
            }
            let n = page[PAGE_HEADER_SIZE] as usize;
            (0..n)
                .map(|i| {
                    let at = PAGE_HEADER_SIZE + 1 + i * 4;
                    u32::from_be_bytes(page[at..at + 4].try_into().unwrap())
                })
                .collect()
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
        let h = Harness::new();
        let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store))
            .with_links(Arc::new(ToyLinks));

        // Build a 3-page tree owned by an ancestor, then a deep chain that inherits it.
        let anc = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a_arena = h.store.arena_for(anc.branch_id).unwrap();
        let e = h.catalog.next_epoch();
        let leaf_a = h.store.alloc_in_arena(a_arena, PageType::BTreeLeaf, e).unwrap();
        let leaf_b = h.store.alloc_in_arena(a_arena, PageType::BTreeLeaf, e).unwrap();
        for (p, tag) in [(leaf_a, 0xA1u8), (leaf_b, 0xB2)] {
            let handle = h.store.read_page(p).unwrap();
            let mut f = handle.write();
            f.data[PAGE_HEADER_SIZE + 32] = tag;
            stamp_checksum(&mut f.data);
        }
        let root = h.store.alloc_in_arena(a_arena, PageType::BTreeInternal, e).unwrap();
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
        assert!(
            !h.catalog
                .get_raw(old_parent.id)
                .unwrap()
                .live_children
                .contains(&deep.fork_epoch),
            "the old parent no longer pins anything for this branch"
        );
        assert!(h
            .catalog
            .get(BranchId::TRUNK)
            .unwrap()
            .live_children
            .contains(&collapsed.fork_epoch));

        // The copy is a real copy: same payload, new ids, all in the branch's own arena.
        let new_root_handle = h.store.read_page(collapsed.root_page_id).unwrap();
        let new_children = ToyLinks.child_pages(PageType::BTreeInternal, &new_root_handle.read().data);
        assert_eq!(new_children.len(), 2);
        assert_ne!(new_children[0], leaf_a);
        let owner_arena = *collapsed.arenas.last().unwrap();
        for (p, tag) in new_children.iter().zip([0xA1u8, 0xB2]) {
            let handle = h.store.read_page(*p).unwrap();
            assert_eq!(handle.read().data[PAGE_HEADER_SIZE + 32], tag);
            assert_eq!(handle.header().unwrap().arena_id, owner_arena);
        }
        // And the ancestor's originals are untouched.
        assert_eq!(h.store.allocated_pages(a_arena).len(), 3);
        let _ = ArenaId(0);
    }

    #[test]
    fn collapse_refuses_a_cyclic_page_graph() {
        let h = Harness::new();
        let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store))
            .with_links(Arc::new(ToyLinks));
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let arena = h.store.arena_for(b.branch_id).unwrap();
        let e = h.catalog.next_epoch();
        let p1 = h.store.alloc_in_arena(arena, PageType::BTreeInternal, e).unwrap();
        let p2 = h.store.alloc_in_arena(arena, PageType::BTreeInternal, e).unwrap();
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
}
