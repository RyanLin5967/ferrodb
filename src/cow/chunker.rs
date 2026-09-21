//! Content-defined chunking for the copy-on-write B+tree's leaf level.
//!
//! # The property this buys
//!
//! A textbook B+tree splits a node when the node happens to be full, and cuts it where the bytes
//! happen to balance. Both facts are about **when** keys arrived, not about what they are: insert
//! the same thousand rows ascending and shuffled and the two trees hold the same data behind
//! different page boundaries. That is fatal to the thing ferrodb is built to do. Its diff
//! (`CowTree::diff`) prunes a subtree the moment two roots name the *same page id*, which is only
//! ever true for pages one side inherited from the other; two branches that independently
//! converge on identical content share nothing, because they do not agree on where a leaf ends.
//!
//! A prolly tree / POS-Tree (ForkBase, VLDB'18; Noms; Dolt) fixes this by deriving the boundary
//! from the content: a hash over each entry decides whether a chunk ends there, so the partition
//! of a sorted sequence is a function of the sequence and nothing else. This module is that
//! decision function; `cow::btree` is the code that obeys it.
//!
//! # Why the boundary predicate reads one entry and not a sliding window
//!
//! Classic CDC (rsync, LBFS, FastCDC) slides a `W`-byte window along a byte stream and cuts where
//! the window's hash hits a pattern. Applied to a B+tree that costs more than it gives:
//!
//! - The window spans entries, so whether entry `i` ends a chunk depends on the bytes before it.
//!   Inserting a key would then invalidate boundary decisions to its right, and a tree with no
//!   leaf sibling pointers (see `cow::node`'s module doc for why it has none) cannot cheaply walk
//!   right to repair them.
//! - Evaluated over a *single* entry, a window is worse than useless: it sees only the last `W`
//!   bytes, so a table whose keys share a long common prefix and differ in a short suffix would
//!   have every entry decide alike — every entry a boundary, or none of them.
//!
//! So the predicate hashes **one whole key**, with the cyclic polynomial restarted at each entry.
//! Every byte of the key contributes, the decision is local, and an insert can only *add* a
//! boundary — never move or destroy one. That last property is what makes the maintenance in
//! `btree::leaf_put` a two-way split instead of a rightward cascade. [`key_hash`] carries why the
//! value is left out, and what was measured on the sorted keys that are said to defeat buzhash.
//!
//! [`Buzhash::roll`] keeps the real sliding-window operation available and tested, because the
//! window is the right tool the moment a *value* is chunked rather than a key.
//!
//! # Where the guarantee stops, and what it cost to put it there
//!
//! A hash boundary cannot be forced to appear, so a chunk can come out longer than the 4 KB page
//! that has to hold it. [`leaf_cuts`] then re-cuts that chunk at a finer target — still by
//! content, but by content *of the whole chunk*, and a leaf holding part of an over-long chunk
//! cannot see the rest of it without the sibling pointer this layout does not have. That is the
//! one case where two insertion orders can still disagree.
//!
//! It is designed out rather than hoped away: [`CHUNK_SHIFT`] puts the mean chunk at an eighth of
//! a page, which makes an over-long chunk an `e^-8` event, and pays about four times the fanout
//! for it. The numbers behind that trade — including what was measured at the setting that kept
//! the old fanout instead — are on [`CHUNK_SHIFT`].
//!
//! Two narrower gaps were predicted here on the paths that *remove* content rather than add it:
//! overwriting a value and deleting a key can each erase a boundary. Both are now repaired by
//! [`crate::cow::btree::CowTree`]'s merge with the right-hand neighbour, so a tree is exact
//! however it was reached, not only one built by insertion.
//!
//! **That repair was called impossible here, and the reasoning was wrong in a way worth keeping
//! visible.** It read "which is sibling access again", treating sibling access as ruled out by
//! `cow::node`'s refusal of sibling POINTERS. Those are different things. A stored `next_leaf`
//! link is rejected for a reason specific to shadow paging — shadowing leaf N+1 forces shadowing
//! N to update its pointer, which cascades leftward along the whole leaf level. Reaching the
//! right-hand neighbour stores nothing and cascades nothing: climb the descent path to the
//! shallowest ancestor with a child further right and take that subtree's leftmost leaf, which is
//! exactly what `ScanCursor` already does for an ordered scan. A merge then shadows two leaves
//! and their ancestors up to the common one — bounded by twice the depth, not by the leaf level.
//!
//! What remains is the over-long chunk above, and the delete path can now reach it deliberately:
//! a join has only ONE boundary, the neighbour's last entry, so deleting terminators repeatedly
//! in one region grows a single run until it no longer fits a page. Measured, 40 such deletes
//! leave 205 leaves with 40 of them cut by the byte cap rather than by content. Every leaf still
//! fits; that guarantee has no exception.

/// How many times the mean chunk divides into a page: the page capacity over
/// [`TARGET_CHUNK_BYTES`].
///
/// This is the whole tuning decision, and it is a straight trade against fanout. Chunk lengths
/// are geometric, so with a mean of `capacity / r` bytes the chance a chunk does not fit a page
/// is about `e^-r`, while the mean fanout is `r` times smaller than a full page would hold:
///
/// ```text
///   r    P(chunk exceeds a page)   mean entries per leaf, ~30-byte rows
///   2            13.5 %                          68     <- what the byte-balanced split gave
///   4             1.8 %                          34
///   6             0.25 %                         22
///   8             0.03 %                         17
/// ```
///
/// A chunk that does not fit is the one case this design cannot keep invariant — [`leaf_cuts`]
/// re-cuts it finer, and a leaf holding part of an over-long chunk has no way to see the rest of
/// that chunk, so which cuts it ends up with depends on when it overflowed.
///
/// Measured, on the 1500-row fixture in `cow::tests_chunking`, by printing the chunker's cuts for
/// the whole sorted set beside the cuts each build actually produced:
///
/// ```text
///   mean chunk ~ half a page (r ~ 2)   27 cuts, 9 of them from the re-cut path;
///                                      the shuffled build differed on 16 of them
///                                      (12 cuts it added, 4 it never made)
///   mean chunk ~ an eighth of a page   99 cuts, none from the re-cut path;
///   (r = 8, this setting)              ascending and shuffled both matched exactly
/// ```
///
/// So the fanout is spent on the property the change exists to deliver, deliberately and with the
/// number written down: about 15 rows per leaf here against the 68 the byte-balanced split gave,
/// and a 10^6-row tree one level deeper.
pub const CHUNK_SHIFT: u32 = 3;

/// Mean chunk size, in bytes of slot-plus-cell.
pub const TARGET_CHUNK_BYTES: usize = crate::cow::node::NODE_CAPACITY >> CHUNK_SHIFT;

/// The cyclic polynomial's per-byte table.
///
/// Built at compile time from a fixed splitmix64 sequence rather than shipped as a literal: the
/// generator is auditable, the values are reproducible, and nothing here reads a clock or an RNG.
/// It must never change — the table *is* the partition, so a new table silently re-chunks every
/// tree ever written by this code.
const TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut i = 0;
    while i < 256 {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // The high half: splitmix64's low bits are the weakest part of its avalanche.
        t[i] = (z >> 32) as u32;
        i += 1;
    }
    t
}

/// A 32-bit cyclic polynomial (buzhash) state.
///
/// `h` after pushing `b_0..b_{n-1}` is `XOR_i rotl(TABLE[b_i], n-1-i)`. Rotation is what makes it
/// order-sensitive — a plain XOR of table entries would hash every permutation of an entry alike.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Buzhash {
    h: u32,
}

impl Buzhash {
    pub const fn new() -> Self {
        Buzhash { h: 0 }
    }

    /// Absorb one byte.
    #[inline]
    pub fn push(&mut self, b: u8) {
        self.h = self.h.rotate_left(1) ^ TABLE[b as usize];
    }

    /// Absorb a slice.
    #[inline]
    pub fn push_all(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.push(b);
        }
    }

    /// Slide a `window`-byte window one byte right: drop `out`, absorb `inp`.
    ///
    /// `out` must be the byte that is `window` positions back, and the state must already hold
    /// exactly `window` bytes; `rolling_window_matches_a_fresh_hash` below is the proof of the
    /// eviction term rather than an assertion about it.
    ///
    /// The chunker does not use this — its window is one whole entry, so there is nothing to
    /// evict. It exists because a value-level chunker needs exactly this operation, and an
    /// eviction term derived at that point without a test is a coin flip.
    #[inline]
    pub fn roll(&mut self, out: u8, inp: u8, window: usize) {
        self.h = self.h.rotate_left(1)
            ^ TABLE[out as usize].rotate_left(window as u32)
            ^ TABLE[inp as usize];
    }

    #[inline]
    pub fn finish(&self) -> u32 {
        self.h
    }
}

/// The chunking hash of one entry: every byte of its **key**, and nothing else.
///
/// # Why the value is not hashed
///
/// Hashing key and value together makes a boundary move whenever a value is rewritten, so
/// updating one column re-chunks the region around that row and the diff stops being small —
/// which is the property this whole module exists to protect. With a key-only hash, a value
/// edit that keeps the row's length cannot move any boundary at all, including its own.
///
/// Dolt reaches the same conclusion — its shipped `keySplitter` hashes the key alone — but pairs
/// it with a *cumulative* size threshold, so in their construction a change in value **length**
/// still shifts every boundary downstream of it. [`boundary_from`] compares against one entry's
/// own size instead of a running total, so a length change can only flip the one row it happened
/// to, and never its neighbours.
///
/// # Why buzhash, on sorted keys, when Noms abandoned it
///
/// Noms chunked with buzhash over a 67-byte sliding window and a static pattern, and Dolt
/// replaced it, reporting poor output on sorted or low-entropy keys — massive chunks on exactly
/// the auto-increment and timestamp keys a database is full of. Two things differ here, and the
/// distribution was measured rather than assumed (`degenerate_key_shapes_do_not_produce_massive_chunks`):
///
/// - The window is one whole key and the state restarts at every entry, so consecutive keys do
///   not share 67 bytes of window the way consecutive positions in a byte stream do.
/// - The threshold is proportional to the entry's size, so the boundary *rate* is pinned to
///   bytes no matter how little the keys vary. A static pattern has no such feedback, which is
///   what lets a low-entropy stream drift into one enormous chunk.
///
/// Measured over 200k rows, mean chunk against a 507-byte target, worst chunk seen:
///
/// ```text
///   ascending key{:06}            mean 507 B   max 2,132 B    0 chunks over a page
///   big-endian u64 counter        mean 505 B   max 3,480 B    0
///   big-endian u32 counter        mean 514 B   max 3,204 B    0
///   monotonic timestamps          mean 510 B   max 5,920 B    7 of 15,691  (0.04 %)
///   32-byte common prefix         mean 513 B   max 4,032 B    0
///   random 16-byte keys (control) mean 505 B   max 4,464 B    3 of 18,999  (0.02 %)
/// ```
///
/// The control is the point: the sorted shapes are not worse than random keys, so the reported
/// failure does not reproduce against this construction.
pub fn key_hash(key: &[u8]) -> u32 {
    let mut h = Buzhash::new();
    h.push_all(key);
    h.finish()
}

/// Does a chunk end after this entry, at a mean chunk of `target` bytes?
///
/// The entry ends a chunk when its hash falls in the lowest `size/target` of the hash space, so
/// each **byte** carries the same `1/target` chance of ending a chunk however the bytes are
/// divided into entries. A plain `hash & ((1<<q)-1) == 0` counts entries instead, which sets the
/// mean chunk in *rows*: the same `q` then means a quarter-page chunk for 30-byte rows and a
/// four-page chunk for 500-byte rows, and the second of those cannot fit in a leaf at all. The
/// tuning that matters is against the page, so the predicate is stated against the page.
///
/// It stays a pure function of the entry itself — its key's bytes and its own size. It
/// deliberately does **not** see the entry's position, its neighbours, or the page it currently
/// sits on; those are the three things that differ between two insertion orders.
///
/// The size term is what keeps a chunk inside a page, and it is the reason value *length* stays
/// in the predicate when value *content* does not. Dropping it would denominate the chunk in key
/// bytes, so a table with 4 KB values would put sixty rows in a chunk and hand a 4 KB page a
/// quarter-megabyte of them — trading a guarantee the page layout enforces for one it cannot.
///
/// Halving `target` can only *add* boundaries, never move one, so the finer partition contains
/// the coarser. That nesting is what lets [`leaf_cuts`] refine an over-long run one piece at a
/// time and still land where refining the whole run would have.
pub fn is_boundary_at(key: &[u8], value: &[u8], target: usize) -> bool {
    let h = key_hash(key) as u64;
    let size = crate::cow::node::leaf_entry_bytes(key, value);
    boundary_from(h, size, target)
}

#[inline]
fn boundary_from(hash: u64, size: usize, target: usize) -> bool {
    // hash / 2^32 <= size / target, in integers and without overflow: hash < 2^32 and
    // target <= a page, so the left side stays under 2^44.
    hash * target.max(1) as u64 <= (size as u64) << 32
}

/// Does a chunk end after this entry, at the tree's nominal chunk size?
pub fn is_boundary(key: &[u8], value: &[u8]) -> bool {
    is_boundary_at(key, value, TARGET_CHUNK_BYTES)
}

/// Cut positions for a leaf's entries: each returned index `i` means "a new leaf starts at entry
/// `i`". The result is strictly increasing and never contains `0` or `sizes.len()`, so no cut
/// leaves either side empty.
///
/// The rule is recursive, and every step of it reads content only:
/// 1. Cut after every entry the boundary predicate accepts at [`TARGET_CHUNK_BYTES`].
/// 2. Any piece that still does not fit `capacity` is re-cut at half the target, and so on. Since
///    halving only adds boundaries, refining a piece in isolation lands on exactly the cuts that
///    refining the whole run would have produced there.
/// 3. At a one-byte target every entry is a boundary, so every piece is a single entry and the
///    recursion cannot fail to terminate; the byte-balanced floor below that is unreachable and
///    kept only so the "a page can never overflow" guarantee has no hole in it.
///
/// Step 2 is the case the invariance guarantee does not cover — see [`CHUNK_SHIFT`] for why it is
/// tuned to essentially never happen, and what it costs to tune it that way.
///
/// `hashes[i]` is [`key_hash`] of entry `i`'s key; `sizes[i]` is its slot-plus-cell cost.
pub fn leaf_cuts(sizes: &[usize], hashes: &[u32], capacity: usize) -> Vec<usize> {
    debug_assert_eq!(sizes.len(), hashes.len());
    let mut cuts = Vec::new();
    cut_at_target(TARGET_CHUNK_BYTES, sizes, hashes, capacity, 0, &mut cuts);
    cuts
}

fn cut_at_target(
    target: usize,
    sizes: &[usize],
    hashes: &[u32],
    capacity: usize,
    base: usize,
    out: &mut Vec<usize>,
) {
    let n = sizes.len();
    if n < 2 {
        return;
    }
    let mut starts: Vec<usize> = vec![0];
    for i in 0..n - 1 {
        if boundary_from(hashes[i] as u64, sizes[i], target) {
            starts.push(i + 1);
        }
    }
    for (j, &s) in starts.iter().enumerate() {
        let e = starts.get(j + 1).copied().unwrap_or(n);
        if j > 0 {
            out.push(base + s);
        }
        let total: usize = sizes[s..e].iter().sum();
        if total <= capacity {
            continue;
        }
        if target > 1 {
            cut_at_target(target / 2, &sizes[s..e], &hashes[s..e], capacity, base + s, out);
        } else {
            balanced_cuts(&sizes[s..e], capacity, base + s, out);
        }
    }
}

/// Byte-balanced cuts that bring every piece of `sizes` under `capacity`, offset by `base`.
///
/// Recursing through [`crate::cow::node::split_point`] rather than packing greedily from the left
/// keeps the pieces even: a greedy pack fills the first piece to the brim and leaves the last one
/// nearly empty, so the next insert into it splits again immediately. When one cut is enough this
/// is exactly `split_point`, which is what the internal level has always done.
pub fn balanced_cuts(sizes: &[usize], capacity: usize, base: usize, out: &mut Vec<usize>) {
    if sizes.len() < 2 {
        return;
    }
    let total: usize = sizes.iter().sum();
    if total <= capacity {
        return;
    }
    let s = crate::cow::node::split_point(sizes);
    balanced_cuts(&sizes[..s], capacity, base, out);
    out.push(base + s);
    balanced_cuts(&sizes[s..], capacity, base + s, out);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cow::node::{leaf_entry_bytes, NODE_CAPACITY};

    fn rows(n: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                (
                    format!("key{:06}", i).into_bytes(),
                    format!("value-{}", i).into_bytes(),
                )
            })
            .collect()
    }

    fn sizes_and_hashes(rows: &[(Vec<u8>, Vec<u8>)]) -> (Vec<usize>, Vec<u32>) {
        (
            rows.iter().map(|(k, v)| leaf_entry_bytes(k, v)).collect(),
            rows.iter().map(|(k, _)| key_hash(k)).collect(),
        )
    }

    /// Chunk sizes in bytes under the nominal target, with no refinement — the refinement is what
    /// hides a long tail, so a distribution measured through it would not show the thing being
    /// asked about.
    fn chunk_bytes(rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<usize> {
        let mut out = Vec::new();
        let mut acc = 0usize;
        for (k, v) in rows {
            let size = leaf_entry_bytes(k, v);
            acc += size;
            if boundary_from(key_hash(k) as u64, size, TARGET_CHUNK_BYTES) {
                out.push(acc);
                acc = 0;
            }
        }
        if acc > 0 {
            out.push(acc);
        }
        out
    }

    fn pieces_of(cuts: &[usize], n: usize) -> Vec<(usize, usize)> {
        let mut bounds = vec![0usize];
        bounds.extend_from_slice(cuts);
        bounds.push(n);
        bounds.windows(2).map(|w| (w[0], w[1])).collect()
    }

    /// The eviction term is the only part of a rolling hash that can be silently wrong: a hash
    /// with a bad roll still looks random, it just stops matching the window it claims to cover.
    /// Rolling a window across a buffer must therefore agree, byte for byte, with hashing that
    /// window from scratch.
    #[test]
    fn rolling_window_matches_a_fresh_hash() {
        let buf: Vec<u8> = (0..200u32).map(|i| (i * 37 + 11) as u8).collect();
        for window in [1usize, 2, 7, 16, 32, 33, 64] {
            let mut rolling = Buzhash::new();
            rolling.push_all(&buf[..window]);
            for i in window..buf.len() {
                rolling.roll(buf[i - window], buf[i], window);
                let mut fresh = Buzhash::new();
                fresh.push_all(&buf[i + 1 - window..=i]);
                assert_eq!(
                    rolling.finish(),
                    fresh.finish(),
                    "window {} desynchronised at byte {}",
                    window,
                    i
                );
            }
        }
    }

    /// Rotation is what separates this from a XOR checksum. Without it every anagram of an entry
    /// would chunk identically, and keys in a sorted tree are near-anagrams of each other.
    #[test]
    fn the_hash_is_order_sensitive() {
        assert_ne!(key_hash(b"ab"), key_hash(b"ba"));
        assert_ne!(key_hash(b"key000012"), key_hash(b"key000021"));
        // Adjacent keys in a sorted tree differ in one byte near the end; that has to be enough.
        assert_ne!(key_hash(b"key000012"), key_hash(b"key000013"));
    }

    /// The property the key-only hash exists for: rewriting a value cannot move a boundary.
    ///
    /// Same-length edits are the strong case and must never change a decision. A length change
    /// moves the entry's own size, so its own decision may flip — but that is the whole blast
    /// radius, because the predicate compares against one entry's size and not a running total.
    #[test]
    fn rewriting_a_value_does_not_move_a_boundary() {
        let keys: Vec<Vec<u8>> = (0..4000u32).map(|i| format!("key{:06}", i).into_bytes()).collect();
        let before: Vec<bool> = keys.iter().map(|k| is_boundary(k, b"aaaaaaaaaa")).collect();

        let same_len: Vec<bool> = keys.iter().map(|k| is_boundary(k, b"zzzzzzzzzz")).collect();
        assert_eq!(before, same_len, "a same-length value rewrite moved a boundary");

        // And the fixture has to contain boundaries, or the equality above is vacuous.
        assert!(before.iter().any(|&b| b), "no boundary in the fixture at all");

        // A length change is allowed to flip the row it changed, and nothing else: compare the
        // decisions for every *other* row, whose size did not move.
        let longer: Vec<bool> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                if i == 1234 {
                    is_boundary(k, b"aaaaaaaaaaaaaaaaaaaa")
                } else {
                    is_boundary(k, b"aaaaaaaaaa")
                }
            })
            .collect();
        for i in 0..keys.len() {
            if i != 1234 {
                assert_eq!(before[i], longer[i], "row {} moved when row 1234 grew", i);
            }
        }
    }

    /// Buzhash is reported to chunk sorted and low-entropy keys badly — Dolt replaced Noms'
    /// buzhash splitter for producing "massive chunks" on exactly the auto-increment and
    /// timestamp keys a database is full of. That claim is about a 67-byte sliding window over a
    /// concatenated byte stream with a static pattern; this chunker restarts the hash per key and
    /// scales its threshold by entry size, and the difference has to be demonstrated rather than
    /// argued. The random-key arm is the control: the sorted shapes must not be worse than it.
    #[test]
    fn degenerate_key_shapes_do_not_produce_massive_chunks() {
        const N: u32 = 40_000;
        let fixed = vec![b'x'; 20];
        let shapes: Vec<(&str, Vec<(Vec<u8>, Vec<u8>)>)> = vec![
            (
                "ascending key{:06}",
                (0..N).map(|i| (format!("key{:06}", i).into_bytes(), fixed.clone())).collect(),
            ),
            (
                "big-endian u64 counter",
                (0..N).map(|i| ((i as u64).to_be_bytes().to_vec(), fixed.clone())).collect(),
            ),
            (
                "monotonic timestamps",
                (0..N)
                    .map(|i| ((1_700_000_000_000u64 + i as u64 * 1000).to_be_bytes().to_vec(), fixed.clone()))
                    .collect(),
            ),
            (
                "32-byte common prefix",
                (0..N)
                    .map(|i| {
                        let mut k = b"tenant-00000000000000000001-row-".to_vec();
                        k.extend_from_slice(&(i as u64).to_be_bytes());
                        (k, fixed.clone())
                    })
                    .collect(),
            ),
            (
                "random-ish 16-byte keys (control)",
                (0..N)
                    .map(|i| {
                        // splitmix64 of i, twice, so the control owns no RNG the test cannot repeat
                        let mut z = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                        let a = z ^ (z >> 31);
                        let b = a.wrapping_mul(0x94D0_49BB_1331_11EB) ^ 0x5DEE_CE66_D1F3_A7B9;
                        let mut k = a.to_be_bytes().to_vec();
                        k.extend_from_slice(&b.to_be_bytes());
                        (k, fixed.clone())
                    })
                    .collect(),
            ),
        ];

        for (name, rows) in &shapes {
            let cs = chunk_bytes(rows);
            assert!(cs.len() > 100, "{}: only {} chunks - the predicate barely fired", name, cs.len());
            let total: usize = cs.iter().sum();
            let mean = total as f64 / cs.len() as f64;
            let max = *cs.iter().max().unwrap();
            let over = cs.iter().filter(|&&c| c > NODE_CAPACITY).count();
            assert!(
                mean > TARGET_CHUNK_BYTES as f64 * 0.75 && mean < TARGET_CHUNK_BYTES as f64 * 1.25,
                "{}: mean chunk {:.0} B against a {} B target",
                name,
                mean,
                TARGET_CHUNK_BYTES
            );
            // "Massive" is the claim under test. Twenty times the target is five pages.
            assert!(
                max < TARGET_CHUNK_BYTES * 20,
                "{}: largest chunk {} B, over {} B - the sorted-key degeneration reproduced",
                name,
                max,
                TARGET_CHUNK_BYTES * 20
            );
            assert!(
                over * 200 < cs.len(),
                "{}: {} of {} chunks exceed a page, over the 0.5% the refinement path is budgeted",
                name,
                over,
                cs.len()
            );
        }
    }

    /// The predicate has to actually fire, and land near the chunk size it advertises. A chunker
    /// whose boundary never triggers degrades to "split when full" without saying so, and one
    /// that triggers constantly turns every row into a page.
    #[test]
    fn the_mean_chunk_lands_near_the_target() {
        let set = rows(200_000);
        let (sizes, hashes) = sizes_and_hashes(&set);
        let hits: Vec<usize> = (0..set.len())
            .filter(|&i| boundary_from(hashes[i] as u64, sizes[i], TARGET_CHUNK_BYTES))
            .collect();
        assert!(hits.len() > 1, "the boundary predicate fired {} times", hits.len());
        let total: usize = sizes.iter().sum();
        let mean = total as f64 / hits.len() as f64;
        assert!(
            mean > TARGET_CHUNK_BYTES as f64 * 0.8 && mean < TARGET_CHUNK_BYTES as f64 * 1.2,
            "mean chunk is {:.0} bytes against a {}-byte target",
            mean,
            TARGET_CHUNK_BYTES
        );
        // And the target is the page divided by the tuning constant, not an unrelated number.
        assert_eq!(TARGET_CHUNK_BYTES, NODE_CAPACITY >> CHUNK_SHIFT);
    }

    /// The point of measuring the target in bytes: the *same* setting has to mean the same chunk
    /// size when the rows change size. Counting rows instead would make a 500-byte row chunk
    /// sixteen times larger than a 30-byte one, which is how a chunk stops fitting in a page.
    #[test]
    fn the_chunk_size_tracks_bytes_not_row_count() {
        for pad in [0usize, 64, 200, 400] {
            let set: Vec<(Vec<u8>, Vec<u8>)> = (0..40_000u32)
                .map(|i| (format!("key{:06}", i).into_bytes(), vec![(i % 251) as u8; pad + 1]))
                .collect();
            let (sizes, hashes) = sizes_and_hashes(&set);
            let hits = (0..set.len())
                .filter(|&i| boundary_from(hashes[i] as u64, sizes[i], TARGET_CHUNK_BYTES))
                .count();
            assert!(hits > 0, "no boundary at all for {}-byte values", pad + 1);
            let mean = sizes.iter().sum::<usize>() as f64 / hits as f64;
            assert!(
                mean > TARGET_CHUNK_BYTES as f64 * 0.75 && mean < TARGET_CHUNK_BYTES as f64 * 1.25,
                "{}-byte values gave a mean chunk of {:.0} bytes, target {}",
                pad + 1,
                mean,
                TARGET_CHUNK_BYTES
            );
        }
    }

    /// The decision must not drift with where an entry sits, only with what it is.
    #[test]
    fn the_boundary_decision_ignores_everything_but_the_entry() {
        let a = is_boundary(b"key000042", b"value-42");
        for _ in 0..8 {
            assert_eq!(is_boundary(b"key000042", b"value-42"), a);
        }
        // Different keys must not all decide alike, or the predicate is a constant.
        let set: std::collections::BTreeSet<bool> = (0..5000u32)
            .map(|i| is_boundary(format!("key{:06}", i).as_bytes(), b"value-42"))
            .collect();
        assert_eq!(set.len(), 2, "the key never affected the boundary decision");
    }

    #[test]
    fn cuts_never_leave_a_side_empty() {
        let sizes = vec![40usize; 300];
        // Hash zero is a boundary at every target: the rule must still refuse 0 and len.
        let cuts = leaf_cuts(&sizes, &vec![0u32; 300], NODE_CAPACITY);
        assert_eq!(cuts.first(), Some(&1));
        assert_eq!(cuts.last(), Some(&299));
        assert_eq!(cuts.len(), 299);
        assert!(leaf_cuts(&[40], &[0], NODE_CAPACITY).is_empty(), "one entry has nowhere to cut");
    }

    /// The hard guarantee: whatever the content, no piece is ever handed to a page that cannot
    /// hold it. Driven with the most hostile input there is — a hash that refuses to be a
    /// boundary at the nominal target — so the refinement, not luck, is what produces the fit.
    #[test]
    fn no_piece_ever_exceeds_the_page() {
        let sizes = vec![40usize; 400];
        let cuts = leaf_cuts(&sizes, &vec![u32::MAX; 400], NODE_CAPACITY);
        assert!(!cuts.is_empty(), "400 entries of 40 bytes must not fit one page");
        for (a, b) in pieces_of(&cuts, 400) {
            assert!(b > a, "empty piece {}..{}", a, b);
            let bytes: usize = sizes[a..b].iter().sum();
            assert!(bytes <= NODE_CAPACITY, "piece {}..{} is {} bytes", a, b, bytes);
        }
        assert!(cuts.windows(2).all(|w| w[0] < w[1]), "cuts are not increasing: {:?}", cuts);
    }

    #[test]
    fn a_sequence_that_fits_is_never_cut_without_a_boundary() {
        assert!(leaf_cuts(&[40usize; 10], &[u32::MAX; 10], NODE_CAPACITY).is_empty());
    }

    /// Refinement is the capacity rule, and the reason it is not the order-dependent thing a
    /// byte-balanced cut would be: a run too long for a page is re-cut at half the target, and
    /// because halving only adds boundaries, the finer cuts **contain** the coarser ones. That
    /// containment is what makes it legal to refine one leaf without touching the rest of its run.
    #[test]
    fn refining_an_over_long_run_keeps_every_cut_it_already_made() {
        let n = 400usize;
        let size = 40usize;
        let sizes = vec![size; n];
        // Thresholds straight from the predicate, so the fixture cannot drift from the rule.
        let at_target = ((size as u64) << 32) / TARGET_CHUNK_BYTES as u64;
        let at_half = ((size as u64) << 32) / (TARGET_CHUNK_BYTES / 2) as u64;
        let mut hashes = vec![u32::MAX; n];
        for (i, h) in hashes.iter_mut().enumerate() {
            if (i + 1) % 200 == 0 {
                *h = at_target as u32; // a boundary at the nominal target
            } else if (i + 1) % 25 == 0 {
                *h = at_half as u32; // only once the target has been halved
            }
        }
        let coarse: Vec<usize> =
            (0..n - 1).filter(|&i| boundary_from(hashes[i] as u64, size, TARGET_CHUNK_BYTES)).map(|i| i + 1).collect();
        assert!(!coarse.is_empty(), "fixture has no nominal boundary");
        let cuts = leaf_cuts(&sizes, &hashes, NODE_CAPACITY);

        for c in &coarse {
            assert!(cuts.contains(c), "the nominal cut at {} was dropped by refinement", c);
        }
        for &c in &cuts {
            assert!(
                boundary_from(hashes[c - 1] as u64, size, TARGET_CHUNK_BYTES / 2),
                "cut at {} does not sit on a boundary entry",
                c
            );
        }
        for (a, b) in pieces_of(&cuts, n) {
            let bytes: usize = sizes[a..b].iter().sum();
            assert!(bytes <= NODE_CAPACITY, "piece {}..{} is {} bytes", a, b, bytes);
        }
    }

    /// The partition is a function of the sorted sequence, so scrambling the order the entries
    /// are *handed to* the chunker cannot move a cut. Stated on the decision function directly,
    /// this is the algebraic half of the end-to-end test in `cow::tests_chunking`.
    #[test]
    fn cuts_depend_only_on_the_sorted_sequence() {
        let set = rows(400);
        let (sizes, hashes) = sizes_and_hashes(&set);
        let want = leaf_cuts(&sizes, &hashes, NODE_CAPACITY);
        assert!(!want.is_empty(), "fixture produced no cuts at all");

        let mut scrambled: Vec<usize> = (0..set.len()).collect();
        scrambled.rotate_left(137);
        scrambled.sort_by_key(|&i| set[i].0.clone());
        let resorted: Vec<(Vec<u8>, Vec<u8>)> = scrambled.iter().map(|&i| set[i].clone()).collect();
        let (sizes2, hashes2) = sizes_and_hashes(&resorted);
        assert_eq!(want, leaf_cuts(&sizes2, &hashes2, NODE_CAPACITY));
    }

    /// `balanced_cuts` is the internal level's splitter and the unreachable floor under
    /// refinement. It is exercised here directly, because a floor nothing can reach is a floor
    /// nothing has ever run.
    #[test]
    fn balanced_cuts_fit_every_piece_and_reduce_to_split_point_for_one_cut() {
        let sizes = vec![40usize; 300];
        let mut cuts = Vec::new();
        balanced_cuts(&sizes, NODE_CAPACITY, 0, &mut cuts);
        for (a, b) in pieces_of(&cuts, 300) {
            let bytes: usize = sizes[a..b].iter().sum();
            assert!(b > a && bytes <= NODE_CAPACITY, "piece {}..{} is {} bytes", a, b, bytes);
        }
        // Exactly one cut is needed here, and it must be the one `split_point` names.
        let two = vec![40usize; 150];
        let mut one = Vec::new();
        balanced_cuts(&two, 40 * 100, 0, &mut one);
        assert_eq!(one, vec![crate::cow::node::split_point(&two)]);
    }
}
