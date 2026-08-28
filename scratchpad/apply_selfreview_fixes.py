#!/usr/bin/env python3
"""Two defects found by re-reading my own code, applied after the mutation sweep freed the tree.

DEFECT 1 (liveness) — `accepted_through` was not reset when the authority changed. It is a monotone
"highest hi ever accepted", used for two things: recognising a re-delivered grant, and clamping a
grant that reaches back below what this node has already issued. Both are statements about the
authority that issued those ranges. Carrying the value across a `leave()`/`join()` means the
previous membership's high-water keeps clamping the NEW leader's grants, so a node refuses a range
it legitimately holds and never allocates again. Safety was never at risk — it only ever refuses —
but a guard that deadlocks liveness is still a defect.

The reset floor is `issued`, not zero: safety here is "never issue a value twice", and `issued` is
the record of which values those are. Lowering to `issued` keeps that and restores liveness.

Per-range epochs then become redundant, so `Held` loses its epoch field and the epoch lives once on
`Grants`. One place to get it wrong instead of two.

DEFECT 2 (a comment that stated the opposite of what the code does) — `reserve()` takes the extent
pages first and the arena id second, and the comment claimed that order stops a refusal from
stranding a page range. It is the other way round: a refusal on the *id* after the pages were
consumed is what would strand them. It cannot actually happen, because `apply_arena_grant` grants
`page_count` arena ids alongside `page_count` pages while a reserve consumes `extent_pages` pages
per single id — ids outnumber extents by `extent_pages` to one. That invariant was load-bearing and
unstated; it is now stated and pinned by a test.
"""
import pathlib, sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

EDITS = [
    # ---- DEFECT 1 -------------------------------------------------------------------------------
    ("src/cluster/mod.rs",
     """/// A half-open range of values this node may issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Held {
    lo: u64,
    hi: u64,
    /// The authority epoch this range was granted under. A range from a stale epoch is not this
    /// node's to issue from — see the module header.
    epoch: AuthorityEpoch,
}""",
     """/// A half-open range of values this node may issue.
///
/// Carries no epoch: the epoch lives once on [`Grants`], because every range in `held` was granted
/// under the same authority — a change of authority clears the whole vector. Stamping each range
/// separately was two places to get one fact right.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Held {
    lo: u64,
    hi: u64,
}"""),

    ("src/cluster/mod.rs",
     """    /// How much a standalone node takes for itself when it runs dry.
    chunk: u64,
}""",
     """    /// How much a standalone node takes for itself when it runs dry.
    chunk: u64,
    /// The authority epoch every range in `held` was granted under, and that `accepted_through`
    /// describes. See [`Grants::observe_epoch`].
    epoch: AuthorityEpoch,
}"""),

    ("src/cluster/mod.rs",
     """        Grants { counter, held: Vec::new(), accepted_through: start, issued: start, chunk }
    }

    /// Drop ranges granted under a superseded authority. See the module header.
    fn evict_stale(&mut self, now_epoch: AuthorityEpoch) {
        self.held.retain(|h| h.epoch == now_epoch);
    }""",
     """        Grants { counter, held: Vec::new(), accepted_through: start, issued: start, chunk, epoch: 0 }
    }

    /// Notice an authority change, and forget everything that belonged to the old one.
    ///
    /// Two things are dropped, for two different reasons.
    ///
    /// **`held`**, because a range granted under a superseded authority is not this node's to issue
    /// from: a store that self-granted pages `[256, 512)` while standalone, in a process that then
    /// joins a cluster, is sitting on space no leader knows it has and will hand to somebody else.
    ///
    /// **`accepted_through` down to `issued`**, which is the subtler half and was a defect for a
    /// while. That field is the highest `hi` ever accepted, and it does two jobs — recognising a
    /// re-delivered grant, and clamping a grant that reaches below what this node already issued.
    /// Both are statements *about the authority that issued those ranges*. Carried across a
    /// `leave()`/`join()` it keeps clamping the NEW leader's grants against the old membership's
    /// high-water, so the node refuses a range it legitimately holds and then never allocates
    /// again. Safety was never at risk — it only ever refuses — but a guard that deadlocks
    /// liveness is still a guard that is wrong.
    ///
    /// `issued` is the floor and is never lowered: safety here is "never issue a value twice", and
    /// `issued` is the record of which values those are.
    fn observe_epoch(&mut self, now_epoch: AuthorityEpoch) {
        if self.epoch != now_epoch {
            self.epoch = now_epoch;
            self.held.clear();
            self.accepted_through = self.issued;
        }
    }"""),

    ("src/cluster/mod.rs",
     """    fn take(&mut self, n: u64, auth: Authority, now_epoch: AuthorityEpoch) -> Result<u64, GrantError> {
        self.evict_stale(now_epoch);""",
     """    fn take(&mut self, n: u64, auth: Authority, now_epoch: AuthorityEpoch) -> Result<u64, GrantError> {
        self.observe_epoch(now_epoch);"""),

    ("src/cluster/mod.rs",
     """            Authority::Standalone => {
                let lo = self.accepted_through.max(self.issued);
                let hi = lo.saturating_add(self.chunk.max(n));
                self.push_range(lo, hi, now_epoch);""",
     """            Authority::Standalone => {
                let lo = self.accepted_through.max(self.issued);
                let hi = lo.saturating_add(self.chunk.max(n));
                self.push_range(lo, hi);"""),

    ("src/cluster/mod.rs",
     """    /// How many values are held and not yet issued **under `now_epoch`**.
    ///
    /// The epoch filter is not decoration. Without it this reports space that [`Grants::take`]
    /// would refuse — a diagnostic that disagrees with the guard, which is the worst kind: a
    /// leader loop reading it would see a node as well supplied and never grant it anything, and
    /// the node would refuse every allocation for ever.
    fn remaining_values(&self, now_epoch: AuthorityEpoch) -> u64 {
        self.held.iter().filter(|h| h.epoch == now_epoch).map(|h| h.hi - h.lo).sum()
    }""",
     """    /// How many values are held and not yet issued **under `now_epoch`**.
    ///
    /// Takes the epoch and applies it rather than reading `held` raw. Without that this reports
    /// space [`Grants::take`] would refuse — a diagnostic that disagrees with the guard, which is
    /// the worst kind: a leader loop reading it would see a node as well supplied and never grant
    /// it anything, and the node would refuse every allocation for ever. Caught by
    /// `space_a_node_self_granted_while_standalone_is_revoked_when_it_joins`.
    fn remaining_values(&mut self, now_epoch: AuthorityEpoch) -> u64 {
        self.observe_epoch(now_epoch);
        self.held.iter().map(|h| h.hi - h.lo).sum()
    }"""),

    ("src/cluster/mod.rs",
     """    fn push_range(&mut self, lo: u64, hi: u64, epoch: AuthorityEpoch) {
        if hi > lo {
            self.held.push(Held { lo, hi, epoch });
            self.held.sort_unstable_by_key(|h| h.lo);
        }
        self.accepted_through = self.accepted_through.max(hi);
    }""",
     """    fn push_range(&mut self, lo: u64, hi: u64) {
        if hi > lo {
            self.held.push(Held { lo, hi });
            self.held.sort_unstable_by_key(|h| h.lo);
        }
        self.accepted_through = self.accepted_through.max(hi);
    }"""),

    ("src/cluster/mod.rs",
     """        if hi <= lo {
            return Err(GrantError::EmptyRange { counter: self.counter, lo, hi });
        }
        self.evict_stale(now_epoch);""",
     """        if hi <= lo {
            return Err(GrantError::EmptyRange { counter: self.counter, lo, hi });
        }
        self.observe_epoch(now_epoch);"""),

    ("src/cluster/mod.rs",
     """        let lo = lo.max(self.accepted_through).max(self.issued);
        let usable = hi.saturating_sub(lo);
        self.push_range(lo, hi, now_epoch);
        Ok(Applied::Accepted { usable })""",
     """        let lo = lo.max(self.accepted_through).max(self.issued);
        let usable = hi.saturating_sub(lo);
        self.push_range(lo, hi);
        Ok(Applied::Accepted { usable })"""),

    ("src/cluster/mod.rs",
     """    pub fn remaining(&self) -> u64 {
        let (_, ep) = authority_at();
        self.inner().remaining_values(ep)
    }""",
     """    pub fn remaining(&self) -> u64 {
        let (_, ep) = authority_at();
        self.inner().remaining_values(ep)
    }

    /// The authority epoch this counter last acted under. Diagnostic, and how a test observes that
    /// an authority change was noticed at all rather than merely not mattering yet.
    pub fn observed_epoch(&self) -> AuthorityEpoch {
        self.inner().epoch
    }"""),

    # ---- DEFECT 2 -------------------------------------------------------------------------------
    ("src/branch/arena.rs",
     """    /// Take one extent's worth of pages and one arena id, or refuse.
    ///
    /// Both takes can refuse and neither is retried against a local counter. The arena id is taken
    /// **after** the pages so that a refusal on the id does not strand a page range: an unused
    /// grant range is still this node's, but a page range consumed for an arena that was never
    /// created would be a durable leak the leader cannot see.""",
     """    /// Take one extent's worth of pages and one arena id, or refuse.
    ///
    /// Both takes can refuse and neither is retried against a local counter.
    ///
    /// # The order, and the invariant that makes it safe
    ///
    /// The pages are consumed first and the id second, so in principle a refusal on the *id* would
    /// strand a page range: an unused arena id costs one number, but pages consumed for an arena
    /// that was never created are a durable leak the leader cannot see and will not re-grant.
    ///
    /// That cannot happen, and the reason is an invariant worth stating rather than relying on.
    /// [`ArenaPageStore::apply_arena_grant`] grants `page_count` arena ids alongside `page_count`
    /// pages, while one reserve consumes `extent_pages` pages against a single id — so ids
    /// outnumber the extents they can name by `extent_pages` to one, and the page counter is always
    /// the binding constraint. Pinned by
    /// `an_arena_grant_always_carries_more_ids_than_the_extents_it_can_name`.""")
]


def main():
    for path, old, new in EDITS:
        p = ROOT / path
        s = p.read_text()
        if old not in s:
            print(f"REFUSING: anchor not found in {path}:\n---\n{old[:200]}\n---")
            return 1
        p.write_text(s.replace(old, new, 1))
    print("applied all", len(EDITS), "edits")
    return 0


if __name__ == "__main__":
    sys.exit(main())
