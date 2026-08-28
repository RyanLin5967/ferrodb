#!/usr/bin/env python3
"""The hole the adversarial pass pointed at, confirmed by reading the code: an extent claimed under
one authority went on being FILLED under the next one.

`ArenaSpaceManager::reserve` is guarded, so a member cannot CLAIM an extent without a grant. But
neither of the two paths that put pages *inside* an already-claimed extent consulted the authority
at all:

  * `arena_for` returns `state.current[branch]` on its fast path whenever the extent has pages left,
    without going near the grant book;
  * `alloc_in_arena` takes an `ArenaId` directly, pops from `state.recycled[arena]` or bumps the
    extent's `next_free`, and never asks whose extent it is.

That fast path is the right design and must stay: pages inside a granted extent are node-local, and
making each one cost a consensus round is what would make agent isolation expensive. It is only
wrong for an extent granted under an authority this node has left — those pages are unknown to the
current leader, which will hand them to somebody else, and then "every such page still passes its
checksum, so refusing here is the only detection point."

The fix follows a precedent already in this file. `load_state` deliberately does `current.clear()`
under the heading "**Never resume filling a restored extent**", for the same reason in a different
guise: a session that died may have handed out pages past the recorded mark. An authority change is
the same hazard with a different cause, so it gets the same answer plus a per-arena stamp, because
`alloc_in_arena` is reachable with an id the caller already holds and `current.clear()` alone does
not stop it.
"""
import pathlib, sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

EDITS = [
    # ---- StoreState carries the authority each live extent was claimed under -------------------
    ("src/branch/arena.rs",
     """    /// Pages logically freed but still visible to some live child. Slow-path reaping parks here.
    pending: Vec<PendingFree>,
}""",
     """    /// Pages logically freed but still visible to some live child. Slow-path reaping parks here.
    pending: Vec<PendingFree>,
    /// The authority epoch each live extent was **claimed** under.
    ///
    /// Kept beside `extents` rather than inside `ArenaExtent`, because that type is a durable
    /// record (`branch/record.rs`) and the authority is not a durable fact about an extent — it is
    /// a fact about this process's relationship to a cluster. Putting it in the record would change
    /// the on-disk format for something a restart cannot verify anyway.
    ///
    /// An arena missing from this map is one whose authority is not known, and is therefore not
    /// fillable. That is the safe direction: an unfillable extent is still accounted for in
    /// `extents` and still freeable by the reaper, so nothing leaks permanently.
    claim_epoch: HashMap<ArenaId, u64>,
}"""),

    ("src/branch/arena.rs",
     """            state: Mutex::new(StoreState {
                extents: HashMap::new(),
                recycled: HashMap::new(),
                current: HashMap::new(),
                pending: Vec::new(),
            }),""",
     """            state: Mutex::new(StoreState {
                extents: HashMap::new(),
                recycled: HashMap::new(),
                current: HashMap::new(),
                pending: Vec::new(),
                claim_epoch: HashMap::new(),
            }),"""),

    ("src/branch/arena.rs",
     """        current.clear();
        *self.state.lock().unwrap() = StoreState { extents, recycled, current, pending };""",
     """        current.clear();
        // Restored extents are stamped with the authority in force NOW, which preserves today's
        // single-node behaviour exactly: `current` is cleared just above, so `arena_for` goes to
        // `alloc_arena` for a fresh extent and a restored extent's tail is given up either way.
        //
        // **What this cannot do is verify the authority the image was written under**, because the
        // image has no field for one and cannot grow one: `load_state` refuses an unknown version
        // (`arena.rs`), `key_order_in_image` hard-codes a 21-byte header, and
        // `two_stores_in_the_same_state_checkpoint_byte_identical_images` pins the bytes. So a
        // database that ran standalone, was converted to a cluster member, and then reopened from
        // its old image could reuse recycled pages the new leader does not know about. No path in
        // this repository performs that conversion, and closing it needs a decision above this row
        // — either bump `STATE_VERSION` and migrate every `<db>.arena` on disk, or make a grant
        // remember its range after it is consumed so a restored page can be checked against it.
        // Named in this row's summary rather than left to be discovered.
        let claim_epoch = extents.keys().map(|a| (*a, crate::cluster::epoch())).collect();
        *self.state.lock().unwrap() =
            StoreState { extents, recycled, current, pending, claim_epoch };"""),

    # ---- the revocation itself ------------------------------------------------------------------
    ("src/branch/arena.rs",
     """    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        {
            let st = self.state.lock().unwrap();
            if let Some(&arena) = st.current.get(&branch) {
                if let Some(ext) = st.extents.get(&arena) {
                    let has_recycled = st.recycled.get(&arena).map(|v| !v.is_empty()).unwrap_or(false);
                    if ext.remaining() > 0 || has_recycled {
                        return Ok(arena);
                    }
                }
            }
        }
        self.alloc_arena(branch)
    }""",
     """    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        let epoch = self.revoke_stale_authority();
        {
            let st = self.state.lock().unwrap();
            if let Some(&arena) = st.current.get(&branch) {
                // The fast path additionally requires that THIS authority claimed the extent.
                // Without it, a branch that claimed an extent while standalone goes on filling it
                // after the process joins a cluster, and those pages are ones the leader believes
                // are free.
                if st.claim_epoch.get(&arena) == Some(&epoch) {
                    if let Some(ext) = st.extents.get(&arena) {
                        let has_recycled =
                            st.recycled.get(&arena).map(|v| !v.is_empty()).unwrap_or(false);
                        if ext.remaining() > 0 || has_recycled {
                            return Ok(arena);
                        }
                    }
                }
            }
        }
        self.alloc_arena(branch)
    }

    /// Notice an authority change and take back the right to **fill** anything claimed under the
    /// old one. Returns the epoch now in force.
    ///
    /// Only the right to fill is withdrawn. The extents stay in `extents` so their pages remain
    /// accounted for and the reaper can still free them whole — dropping them would leak the space
    /// permanently, which is the one outcome worse than refusing to use it.
    fn revoke_stale_authority(&self) -> u64 {
        let epoch = crate::cluster::epoch();
        if self.authority_epoch.swap(epoch, Ordering::SeqCst) != epoch {
            let mut st = self.state.lock().unwrap();
            // Same rule and same reason as `load_state`'s `current.clear()`: never resume filling
            // an extent whose provenance this process can no longer vouch for.
            st.current.clear();
            st.claim_epoch.clear();
            st.recycled.clear();
        }
        epoch
    }"""),

    ("src/branch/arena.rs",
     """    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        let page_id = {
            let mut st = self.state.lock().unwrap();""",
     """    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        // Reached with an `ArenaId` the caller already holds, so `arena_for`'s check is not enough:
        // a caller that obtained the id before the authority changed would otherwise keep writing
        // into an extent the current leader knows nothing about.
        let epoch = self.revoke_stale_authority();
        let page_id = {
            let mut st = self.state.lock().unwrap();
            if st.claim_epoch.get(&arena) != Some(&epoch) {
                return Err(BranchError::Arena(format!(
                    "arena {arena} was claimed under a superseded authority and will not be \\
                     filled: its pages are not known to the current leader, and a page written \\
                     twice still passes its own checksum"
                ))
                .into());
            }"""),

    ("src/branch/arena.rs",
     """            st.recycled.insert(arena, Vec::new());
            st.current.insert(branch, arena);
        }""",
     """            st.recycled.insert(arena, Vec::new());
            st.current.insert(branch, arena);
            st.claim_epoch.insert(arena, epoch);
        }"""),

    ("src/branch/arena.rs",
     """    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        let (arena, start) = self.space.reserve()?;""",
     """    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        let epoch = self.revoke_stale_authority();
        let (arena, start) = self.space.reserve()?;"""),

    ("src/branch/arena.rs",
     """        st.pending.retain(|p| p.arena_id != arena);""",
     """        st.pending.retain(|p| p.arena_id != arena);
        st.claim_epoch.remove(&arena);"""),

    # ---- the store remembers which authority its state belongs to -------------------------------
    ("src/branch/arena.rs",
     """    /// Where to persist the free-space map when an extent is claimed or freed, if anywhere.""",
     """    /// The authority epoch this store's in-memory state belongs to.
    ///
    /// Compared against [`crate::cluster::epoch`] on every allocation path; a change revokes the
    /// right to fill anything claimed before it. See [`ArenaPageStore::revoke_stale_authority`].
    authority_epoch: AtomicU64,
    /// Where to persist the free-space map when an extent is claimed or freed, if anywhere."""),

    ("src/branch/arena.rs",
     """            live_pages: AtomicU32::new(0),
            reserved_pages: AtomicU32::new(0),
            checkpoint_path: Mutex::new(None),""",
     """            live_pages: AtomicU32::new(0),
            reserved_pages: AtomicU32::new(0),
            authority_epoch: AtomicU64::new(crate::cluster::epoch()),
            checkpoint_path: Mutex::new(None),"""),

    # ---- M3b: make the applier report idempotence so a test can actually detect it --------------
    ("src/branch/arena.rs",
     """    pub fn apply_arena_grant(
        &self,
        node: crate::consensus::NodeId,
        first_page: u32,
        page_count: u32,
    ) -> Result<(), FerroError> {
        let lo = first_page as u64;
        let hi = lo + page_count as u64;
        self.space.extent_starts.apply_grant(node, lo, hi)?;
        self.space.arena_ids.apply_grant(node, lo, hi)?;
        Ok(())
    }""",
     """    /// Returns whether the grant was new or had already been applied.
    ///
    /// Reporting it is not decoration. The mutation sweep showed that removing the duplicate check
    /// in `Grants::apply_grant` did NOT let a re-delivered grant hand out a second extent — the
    /// clamp to `max(accepted_through, issued)` already prevents that — so an integration test that
    /// only asserted "no second extent" was pinning a rule it could not detect. The two mechanisms
    /// are defence in depth, and the outcome is what makes the idempotence itself observable.
    ///
    /// Refuses if the two counters disagree about whether the grant was new. They are fed from one
    /// entry and are stamped from the same numbers, so a disagreement means their watermarks have
    /// diverged — which would eventually issue an arena id for an extent range that was never
    /// granted, and there is no correct way to carry on from it.
    pub fn apply_arena_grant(
        &self,
        node: crate::consensus::NodeId,
        first_page: u32,
        page_count: u32,
    ) -> Result<crate::cluster::Applied, FerroError> {
        let lo = first_page as u64;
        let hi = lo + page_count as u64;
        let pages = self.space.extent_starts.apply_grant(node, lo, hi)?;
        let ids = self.space.arena_ids.apply_grant(node, lo, hi)?;
        if std::mem::discriminant(&pages) != std::mem::discriminant(&ids) {
            return Err(BranchError::Arena(format!(
                "an ArenaGrant of [{lo}, {hi}) was {pages:?} for extent pages but {ids:?} for \\
                 arena ids; the two watermarks have diverged and cannot both be trusted"
            ))
            .into());
        }
        Ok(pages)
    }"""),
]


def main():
    for path, old, new in EDITS:
        p = ROOT / path
        s = p.read_text()
        if old not in s:
            print(f"REFUSING: anchor not found in {path}:\n---\n{old[:220]}\n---")
            return 1
        p.write_text(s.replace(old, new, 1))
    print("applied", len(EDITS), "edits")
    return 0


if __name__ == "__main__":
    sys.exit(main())
