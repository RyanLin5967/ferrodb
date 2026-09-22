//! The key space of the branch catalog's copy-on-write B+tree.
//!
//! **Why one tree and not several.** The records and every index over them live in a single tree,
//! so one root-pointer swap commits a record *and* all of its index entries atomically. The
//! append-only log could not have that: it held one record per append and derived the indexes at
//! replay, which is exactly why `open` was O(operations ever written) and why a reaped child's
//! index entry could not be removed in the same durable act that marked it reaped.
//!
//! **Why big-endian.** The tree compares keys as byte strings. Big-endian encoding is the standard
//! trick that makes lexicographic byte order equal numeric order, which is what turns "every lease
//! expiring at or before `now`" into a single range descent instead of a walk. Little-endian would
//! silently produce a tree that is sorted by the *last* byte of the deadline and answers range
//! queries with garbage, so `encoded_order_matches_numeric_order` below is not a nicety — it is the
//! load-bearing invariant of the whole design.
//!
//! **Why the index keys carry no value.** The record is the single copy of the truth. An index
//! entry that duplicated any field would be a second copy to keep in step, and the two would
//! disagree after the first crash between them.
//!
//! See `SCALE-DESIGN.md` D1 and D2.

/// Tag byte, and therefore the sort group. Ordering between groups is the tag's own order, so each
/// group occupies one contiguous span and `[tag] .. [tag + 1]` is exactly that span.
pub mod tag {
    /// `[0x00][branch id]` → the serialized `BranchRecord`. The only copy of the truth.
    pub const RECORD: u8 = 0x00;
    /// `[0x01][lease deadline][branch id]` → empty. **`Live`, non-trunk branches only.**
    ///
    /// The restriction is not tidiness. A quarantined branch holds its record for as long as an
    /// operator wants to look at it, and its lease expired long ago — left in this index it would
    /// sit at the head of the range for ever, and *every* 30-second reap scan would walk over it
    /// before reaching anything real. That is an unbounded walk in the hot path reintroduced
    /// through the back door, which is the one thing D2 exists to prevent.
    pub const DEADLINE: u8 = 0x01;
    /// `[0x02][state][branch id]` → empty. Every branch, including trunk.
    pub const STATE: u8 = 0x02;
    /// `[0x03][parent id][fork epoch]` → empty. **Live children only.**
    ///
    /// Replaces the `live_children` array that used to live inside the parent's record and forced
    /// `fork` to rewrite the parent. The reclamation rule was already a range-emptiness question
    /// over that array; here it is a range-emptiness question over the tree, which is the same
    /// question asked of a structure that can answer it without holding every parent resident.
    pub const CHILD: u8 = 0x03;
    /// `[0x05][branch id]` → the serialized `CapabilityEnvelope`. **Sparse**: only governed
    /// branches have one.
    ///
    /// Out of the record because it is variable-length and because `envelope_of` is already a
    /// separate trait method — it exists so the write funnel, which asks on every statement, does
    /// not clone the rest of the record. Splitting the storage the same way the query is already
    /// split costs nothing and keeps the core record fixed-size.
    pub const ENVELOPE: u8 = 0x05;
    /// `[0x06][branch id][arena id]` → empty. The arenas a branch allocates novel pages from.
    ///
    /// **Not optional.** A leaf page holds about 2 KB of entries in total, and a branch that has
    /// written ~230 MB owns ~900 arena ids — around 3.6 KB on its own. A record that can outgrow a
    /// page is a wall with no error message, so the unbounded field becomes a key span, where the
    /// only question anyone asks of it ("which arenas does this branch own?") is a range scan.
    pub const ARENA: u8 = 0x06;
    /// `[0x07]` → the catalog header: `next_id` and the epoch counter.
    ///
    /// Inside the tree rather than on a page of its own, so that `open` is one descent and so that
    /// a fork's id allocation commits in the same structure as the record it allocated. The only
    /// value that must live outside the tree is the tree's own root page id, which the caller
    /// persists — that is the irreducible bootstrap, and it is one `u32`.
    pub const HEADER: u8 = 0x07;
    /// `[0x04][branch id]` → empty. Ids released by a reap and available for reuse.
    ///
    /// In the tree rather than in the header on purpose: a free-id *list* in a fixed header is
    /// unbounded in the one direction that matters — a workload that creates and reaps 10⁶
    /// branches would need every released id in the header, and the header must stay O(1) or
    /// `open` is O(N) again by another route. "Lowest free id" is the first key in this span.
    pub const FREE_ID: u8 = 0x04;
}

/// `[0x00][id]`
pub fn record(id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(9);
    k.push(tag::RECORD);
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// `[0x01][deadline][id]`
pub fn deadline(deadline_millis: u64, id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(17);
    k.push(tag::DEADLINE);
    k.extend_from_slice(&deadline_millis.to_be_bytes());
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// `[0x02][state][id]`
pub fn state(state_tag: u8, id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(tag::STATE);
    k.push(state_tag);
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// `[0x03][parent][u64::MAX - fork epoch]` — **the epoch is stored complemented.**
///
/// So the span is ordered newest-child-first and `max_live_child` is the FIRST key, reachable in
/// one descent. Ascending would put the maximum last, and reading it would mean consuming the whole
/// span — O(N) for trunk, which is the parent of everything and the exact cost being removed.
///
/// This is the standard descending-index trick, how any forward-only B+tree serves `ORDER BY x
/// DESC`. It costs nothing because no caller iterates children in ascending order: the three that
/// exist want the maximum, a range-emptiness test, and a non-empty test.
pub fn child(parent_id: u64, fork_epoch: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(17);
    k.push(tag::CHILD);
    k.extend_from_slice(&parent_id.to_be_bytes());
    k.extend_from_slice(&(u64::MAX - fork_epoch).to_be_bytes());
    k
}

/// Recover the fork epoch from a `CHILD` key's trailing eight bytes.
pub fn child_epoch_from_key(key: &[u8]) -> Option<u64> {
    if key.len() != 17 || key[0] != tag::CHILD {
        return None;
    }
    let c = u64::from_be_bytes(key[9..17].try_into().ok()?);
    Some(u64::MAX - c)
}

/// Recover the parent id from a `CHILD` key's middle eight bytes. NOT complemented — only the
/// epoch is, so that the span reads newest-first.
///
/// The sibling of [`child_epoch_from_key`], and it exists for one reason: a resolver that refuses
/// an entry holds only the key, and an error that cannot say WHICH entry is unactionable.
pub fn child_parent_from_key(key: &[u8]) -> Option<u64> {
    if key.len() != 17 || key[0] != tag::CHILD {
        return None;
    }
    Some(u64::from_be_bytes(key[1..9].try_into().ok()?))
}

/// `[0x04][u64::MAX - id]` — **complemented**, so the HIGHEST free id is the first key.
///
/// Not an aesthetic choice: `LogBranchCatalog` keeps free ids in a sorted `Vec` and `fork` calls
/// `pop()`, which takes the highest. A catalog that recycled the LOWEST would hand out a different
/// id for the same sequence of operations, and then no single test could cover both
/// implementations — which is the whole reason the table catalog lands behind the same trait.
/// Complemented, "the id `pop()` would have returned" is the first key in the span, one descent.
pub fn free_id(id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(9);
    k.push(tag::FREE_ID);
    k.extend_from_slice(&(u64::MAX - id).to_be_bytes());
    k
}

/// Recover the id from a `FREE_ID` key.
pub fn free_id_from_key(key: &[u8]) -> Option<u64> {
    if key.len() != 9 || key[0] != tag::FREE_ID {
        return None;
    }
    Some(u64::MAX - u64::from_be_bytes(key[1..9].try_into().ok()?))
}

/// `[0x07]` — the single header key.
pub fn header() -> Vec<u8> {
    vec![tag::HEADER]
}

/// The half-open span `[lo, hi)` covering an entire tag group.
pub fn whole_group(t: u8) -> (Vec<u8>, Vec<u8>) {
    (vec![t], vec![t + 1])
}

/// The half-open span covering every `DEADLINE` key with a deadline **at or before** `now`.
///
/// Inclusive of `now` because `LeaseDeadline::is_expired_at` is inclusive, and a range query that
/// disagreed with the predicate it replaces would reap a branch a millisecond early or a
/// millisecond late depending on which one a caller happened to ask.
///
/// `now == u64::MAX` is the case that would panic on `now + 1` in debug and wrap to an empty range
/// in release — the worse of the two, because it would silently report nothing expired when in
/// fact everything had. It is handled by widening to the end of the group.
pub fn expired_at_or_before(now_millis: u64) -> (Vec<u8>, Vec<u8>) {
    let lo = vec![tag::DEADLINE];
    let hi = match now_millis.checked_add(1) {
        Some(next) => {
            let mut h = Vec::with_capacity(9);
            h.push(tag::DEADLINE);
            h.extend_from_slice(&next.to_be_bytes());
            h
        }
        None => vec![tag::DEADLINE + 1],
    };
    (lo, hi)
}

/// The half-open span covering every branch in one state.
pub fn whole_state(state_tag: u8) -> (Vec<u8>, Vec<u8>) {
    (vec![tag::STATE, state_tag], {
        // `state_tag + 1` cannot overflow for any real state, but a `u8` that reached 0xFF would
        // wrap to 0x00 and produce an EMPTY range that reads as "no branches in this state" —
        // a silent wrong answer. Widen to the next tag group instead.
        match state_tag.checked_add(1) {
            Some(next) => vec![tag::STATE, next],
            None => vec![tag::STATE + 1],
        }
    })
}

/// The half-open span covering every live child of `parent`.
pub fn children_of(parent_id: u64) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(9);
    lo.push(tag::CHILD);
    lo.extend_from_slice(&parent_id.to_be_bytes());
    let hi = match parent_id.checked_add(1) {
        Some(next) => {
            let mut h = Vec::with_capacity(9);
            h.push(tag::CHILD);
            h.extend_from_slice(&next.to_be_bytes());
            h
        }
        None => vec![tag::CHILD + 1],
    };
    (lo, hi)
}

/// The half-open span covering live children of `parent` forked in `[lo_epoch, hi_epoch)`.
///
/// This is the reclamation rule itself: a page born at epoch `b` and freed at epoch `f` is
/// reclaimable exactly when no live child forked in `[b, f)`. Asking the tree directly means the
/// rule stops depending on an array held inside the parent's record.
/// Returns an **inclusive** `[lo_key, hi_key]` pair, because complement turns a half-open epoch
/// window into a closed key window and the alternative arithmetic overflows: epochs `[lo, hi)`
/// are keys `[MAX-hi+1, MAX-lo]`, and computing `MAX-lo+1` for an exclusive upper bound overflows
/// whenever `lo == 0` — which is trunk's own fork epoch, so it is the common case, not an edge one.
///
/// An empty or inverted window (`lo >= hi`) returns `None`: there is no key range to scan, and
/// synthesising one would make a reversed pair that silently matches everything.
pub fn children_in_epoch_range(
    parent_id: u64,
    lo_epoch: u64,
    hi_epoch: u64,
) -> Option<(Vec<u8>, Vec<u8>)> {
    if lo_epoch >= hi_epoch {
        return None;
    }
    // hi >= 1 here, so `MAX - hi + 1` cannot overflow.
    Some((child(parent_id, hi_epoch - 1), child(parent_id, lo_epoch)))
}

/// `[0x05][id]`
pub fn envelope(id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(9);
    k.push(tag::ENVELOPE);
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// `[0x06][branch][arena]`
pub fn arena(branch_id: u64, arena_id: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(13);
    k.push(tag::ARENA);
    k.extend_from_slice(&branch_id.to_be_bytes());
    k.extend_from_slice(&arena_id.to_be_bytes());
    k
}

/// The half-open span covering every arena owned by `branch`.
pub fn arenas_of(branch_id: u64) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(9);
    lo.push(tag::ARENA);
    lo.extend_from_slice(&branch_id.to_be_bytes());
    let hi = match branch_id.checked_add(1) {
        Some(next) => {
            let mut h = Vec::with_capacity(9);
            h.push(tag::ARENA);
            h.extend_from_slice(&next.to_be_bytes());
            h
        }
        None => vec![tag::ARENA + 1],
    };
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **The load-bearing invariant.** Every range query in the catalog is a byte-string comparison
    /// standing in for a numeric one. If that correspondence fails anywhere, the tree is sorted by
    /// something other than what callers think and the reap scan silently returns wrong branches.
    #[test]
    fn encoded_order_matches_numeric_order() {
        let interesting = [
            0u64, 1, 2, 127, 128, 129, 254, 255, 256, 257, 511, 512, 65_535, 65_536, 65_537,
            16_777_215, 16_777_216, u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX - 1, u64::MAX,
        ];
        for a in interesting {
            for b in interesting {
                assert_eq!(
                    record(a).cmp(&record(b)),
                    a.cmp(&b),
                    "record keys disagree with numeric order at {a} vs {b}"
                );
                // DESCENDING, so the highest free id is the first key — the id `Vec::pop()`
                // would have returned in the log catalog.
                assert_eq!(
                    free_id(a).cmp(&free_id(b)),
                    b.cmp(&a),
                    "free-id keys are not descending at {a} vs {b}"
                );
                assert_eq!(free_id_from_key(&free_id(a)), Some(a), "free id does not round-trip");
                // The deadline key is the one the 30-second scan descends. Its PRIMARY component
                // must dominate: a later deadline sorts after an earlier one whatever the ids are.
                assert_eq!(
                    deadline(a, 7).cmp(&deadline(b, 3)),
                    a.cmp(&b).then(7u64.cmp(&3)),
                    "deadline keys disagree with (deadline, id) order at {a} vs {b}"
                );
                assert_eq!(
                    child(a, 9).cmp(&child(b, 9)),
                    a.cmp(&b),
                    "child keys disagree with parent order at {a} vs {b}"
                );
                // **Descending in the epoch**: a LATER fork must sort EARLIER, so the newest live
                // child is the first key in the span and `max_live_child` is one descent.
                assert_eq!(
                    child(7, a).cmp(&child(7, b)),
                    b.cmp(&a),
                    "child keys are not descending in the fork epoch at {a} vs {b}"
                );
                assert_eq!(child_epoch_from_key(&child(7, a)), Some(a), "epoch does not round-trip");
                // Both halves, from the SAME key: an error that names the wrong entry is worse
                // than one that names none, and the epoch is complemented while the parent is not.
                assert_eq!(
                    child_parent_from_key(&child(a, b)),
                    Some(a),
                    "parent does not round-trip out of a CHILD key at {a}/{b}"
                );
                assert_eq!(
                    child_epoch_from_key(&child(a, b)),
                    Some(b),
                    "epoch does not round-trip out of a CHILD key at {a}/{b}"
                );
            }
        }
    }

    /// A decoder that answers for a key it was not given is worse than one that refuses: both
    /// callers put the answer into an error message that a human uses to find the entry.
    #[test]
    fn the_child_key_decoders_refuse_anything_that_is_not_a_child_key() {
        for (name, k) in [
            ("record", record(7)),
            ("deadline", deadline(7, 7)),
            ("state", state(1, 7)),
            ("free_id", free_id(7)),
            ("arena", arena(7, 7)),
            ("truncated child", child(7, 7)[..16].to_vec()),
        ] {
            assert!(child_parent_from_key(&k).is_none(), "parent decoded out of a {name} key");
            assert!(child_epoch_from_key(&k).is_none(), "epoch decoded out of a {name} key");
        }
    }

    /// Groups must not overlap, or a scan of one would return entries of another — and because
    /// every group's value encoding differs, that surfaces as a deserialization error at best and
    /// a wrong branch at worst.
    #[test]
    fn tag_groups_are_disjoint_and_each_span_contains_only_its_own() {
        let keys = vec![
            ("record", record(u64::MAX)),
            ("deadline", deadline(u64::MAX, u64::MAX)),
            ("state", state(0xFF, u64::MAX)),
            ("child", child(u64::MAX, u64::MAX)),
            ("free_id", free_id(u64::MAX)),
            ("envelope", envelope(u64::MAX)),
            ("arena", arena(u64::MAX, u32::MAX)),
        ];
        for (name, t) in
            [("record", tag::RECORD), ("deadline", tag::DEADLINE), ("state", tag::STATE),
             ("child", tag::CHILD), ("free_id", tag::FREE_ID), ("envelope", tag::ENVELOPE),
             ("arena", tag::ARENA)]
        {
            let (lo, hi) = whole_group(t);
            for (kname, k) in &keys {
                let inside = *k >= lo && *k < hi;
                assert_eq!(
                    inside,
                    kname == &name,
                    "{kname} key {} the {name} span, which is wrong",
                    if inside { "fell inside" } else { "fell outside" }
                );
            }
        }
    }

    /// `is_expired_at` is inclusive, so the range must be too — and the boundary is where an
    /// off-by-one reaps a branch that still holds a valid lease.
    #[test]
    fn expired_range_is_inclusive_of_now_and_excludes_the_next_millisecond() {
        let (lo, hi) = expired_at_or_before(1_000);
        assert!(deadline(999, 0) >= lo && deadline(999, 0) < hi, "999 must be expired at 1000");
        assert!(deadline(1_000, 0) >= lo && deadline(1_000, 0) < hi, "1000 must be expired at 1000");
        assert!(
            deadline(1_000, u64::MAX) < hi,
            "every id at the boundary deadline must be inside, not just id 0"
        );
        assert!(!(deadline(1_001, 0) < hi), "1001 must NOT be expired at 1000");
    }

    /// `u64::MAX` is the case that wraps to an EMPTY range — which reads as "nothing expired" at
    /// the exact moment everything has. Silent and in the wrong direction, so it gets its own test.
    #[test]
    fn expired_range_at_max_contains_every_deadline_and_no_other_group() {
        let (lo, hi) = expired_at_or_before(u64::MAX);
        for d in [0u64, 1, 1_000, u64::MAX - 1, u64::MAX] {
            let k = deadline(d, 42);
            assert!(k >= lo && k < hi, "deadline {d} must be inside the u64::MAX range");
        }
        assert!(!(state(0, 0) >= lo && state(0, 0) < hi), "the range must not reach the next group");
    }

    #[test]
    fn a_state_span_holds_that_state_alone() {
        let (lo, hi) = whole_state(3);
        assert!(state(3, 0) >= lo && state(3, 0) < hi);
        assert!(state(3, u64::MAX) >= lo && state(3, u64::MAX) < hi);
        assert!(!(state(2, u64::MAX) >= lo), "a lower state must sort below the span");
        assert!(!(state(4, 0) < hi), "a higher state must sort above the span");
        // 0xFF is the wrap case: it must still be a non-empty span that excludes the next group.
        let (lo, hi) = whole_state(0xFF);
        assert!(state(0xFF, 7) >= lo && state(0xFF, 7) < hi);
        assert!(!(child(0, 0) >= lo && child(0, 0) < hi));
    }

    #[test]
    fn an_arena_span_holds_one_branchs_arenas_and_survives_the_wrap() {
        let (lo, hi) = arenas_of(5);
        assert!(arena(5, 0) >= lo && arena(5, 0) < hi);
        assert!(arena(5, u32::MAX) >= lo && arena(5, u32::MAX) < hi);
        assert!(!(arena(4, u32::MAX) >= lo), "branch 4 must sort below branch 5's span");
        assert!(!(arena(6, 0) < hi), "branch 6 must sort above branch 5's span");

        let (lo, hi) = arenas_of(u64::MAX);
        assert!(arena(u64::MAX, 7) >= lo && arena(u64::MAX, 7) < hi, "wrap emptied the span");
        assert!(!(envelope(0) >= lo && envelope(0) < hi), "must not reach another group");
    }

    #[test]
    fn a_parents_child_span_excludes_the_adjacent_parent() {
        let (lo, hi) = children_of(5);
        assert!(child(5, 0) >= lo && child(5, 0) < hi);
        assert!(child(5, u64::MAX) >= lo && child(5, u64::MAX) < hi);
        assert!(!(child(4, u64::MAX) >= lo), "parent 4 must sort below parent 5's span");
        assert!(!(child(6, 0) < hi), "parent 6 must sort above parent 5's span");
        // The wrap case, which a mutant survived before this was here: `parent_id + 1` at
        // `u64::MAX` wraps to 0, making `hi` sort BELOW `lo` and the span empty — so every live
        // child of that parent becomes invisible and its pages look reclaimable. Silent, and in
        // the direction that loses data.
        let (lo, hi) = children_of(u64::MAX);
        assert!(child(u64::MAX, 0) >= lo && child(u64::MAX, 0) < hi, "wrap emptied the span");
        assert!(
            child(u64::MAX, u64::MAX) >= lo && child(u64::MAX, u64::MAX) < hi,
            "the last epoch of the last parent must still be inside"
        );
        assert!(!(state(0, 0) >= lo && state(0, 0) < hi), "must not reach into the next group");

        // The reclamation rule's own query. Complemented keys make this a CLOSED key range over a
        // HALF-OPEN epoch window, so the assertions are on epochs, not on key comparisons.
        let (lo, hi) = children_in_epoch_range(5, 10, 20).expect("non-empty window");
        assert!(child(5, 10) >= lo && child(5, 10) <= hi, "the birth epoch is inside");
        assert!(child(5, 19) >= lo && child(5, 19) <= hi, "the last epoch before freed is inside");
        assert!(!(child(5, 20) >= lo), "the freed epoch is NOT inside — the window is half-open");
        assert!(!(child(5, 9) <= hi), "an epoch before birth is outside");
        assert!(children_in_epoch_range(5, 10, 10).is_none(), "an empty window has no key range");
        assert!(children_in_epoch_range(5, 20, 10).is_none(), "an inverted window has no key range");
        // The case that overflows if the upper bound is computed exclusively. Trunk forks at 0.
        let (lo, hi) = children_in_epoch_range(5, 0, 3).expect("window from epoch zero");
        assert!(child(5, 0) >= lo && child(5, 0) <= hi, "epoch 0 must be inside [0, 3)");
        assert!(child(5, 2) >= lo && child(5, 2) <= hi);
        assert!(!(child(5, 3) >= lo), "epoch 3 is outside [0, 3)");
    }
}
