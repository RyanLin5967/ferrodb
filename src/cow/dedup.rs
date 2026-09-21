//! Content-addressed chunk index with reference counts — D94.
//!
//! **This module is deliberately not wired into the write path, and the reason is a measurement.**
//! `examples/d94_dedup_premise.rs` (banked as `bench/d94_chunk_dedup.txt`) measures what cross-branch
//! dedup could win here, and the answer is *entirely* workload-dependent: exactly zero pages when
//! branches write different content, and up to ~79% of stored bytes when every branch writes
//! byte-identical rows. Read the bench file before adopting this; the premise section of it argues
//! that most identical-by-construction content is better hoisted to the common ancestor, where
//! `cow`'s existing sharing stores it once for free and needs no index and no refcounts at all.
//!
//! What this module IS: the mechanism, built and tested, so the decision is about evidence rather
//! than about whether it can be done.
//!
//! # Why this cannot key on a whole page
//!
//! [`crate::cow::page_header`] puts `{birth_epoch, arena_id, checksum}` in the first 16 bytes of
//! every page, self-describing on purpose so that liveness needs no side table. Two branches that
//! independently write identical rows therefore produce pages that are **not** byte-identical:
//! different arena, different birth epoch, and a crc32 over both. Measured — the premise harness
//! reports `distinct pages == distinct whole` in every configuration it runs, including the one
//! where every branch writes the same rows. A whole-page content index finds nothing here.
//!
//! So the unit of identity is the **payload**, `page[PAGE_HEADER_SIZE..]`, and callers pass that
//! slice. That has a consequence which is properly a design decision and not this module's to
//! take: a payload shared by two branches lives in one physical page, and that page carries one
//! `arena_id` and one `birth_epoch` — so it belongs to one branch, while two branches reference
//! it. The epoch-interval liveness rule in [`crate::branch::record::reclaimable`] is stated per
//! owner and cannot answer for such a page. **That is exactly why `cow::mod`'s brief lists
//! refcounts and content addressing as deliberate non-goals.** This index supplies the refcount
//! half; adopting it means retiring the epoch rule for shared pages, which is a bigger change than
//! this file, and the bench file says so.
//!
//! # Refcount correctness
//!
//! A dropped decrement leaks forever; an extra one is silent data loss. Three mechanisms, each
//! against a specific way that has already happened in this repo:
//!
//! 1. **A hash hit is never trusted on its own.** 128 bits is not a proof. Every candidate's bytes
//!    are compared before its refcount is raised, and a bucket holds a *list* of entries precisely
//!    so two different payloads that share a content id each keep their own page. See
//!    [`ChunkIndex::with_hasher`] — the collision path is tested by forcing every input to one id,
//!    because a collision branch that has never executed is not a tested branch.
//! 2. **The increment is provisional until the caller records it.** [`ChunkIndex::intern`] hands
//!    back a [`RefTicket`] holding a +1 that no durable record owns yet. Dropping it without
//!    [`RefTicket::commit`] rolls the increment back, so an early return, a `?`, or a panic between
//!    "bumped the refcount" and "wrote the reference down" cannot leak.
//! 3. **A process crash is a different failure and gets a different mechanism.** `Drop` does not
//!    run when the process dies, so the provisional increments are also journalled in `pending`;
//!    [`ChunkIndex::recover`] rolls back every ticket that was never committed. This is the
//!    increment-then-crash window D85 needed fault injection to find, and
//!    [`tests::a_crash_between_the_increment_and_the_record_leaks_nothing`] simulates it directly
//!    with `std::mem::forget`, which is what a dead process looks like to this data structure.
//!
//! An over-release is **refused**, not saturated: [`ChunkIndex::release`] on a page at zero returns
//! an error rather than wrapping or clamping, because a clamp turns the second decrement — the one
//! that would delete live data — into a silent no-op that looks like success.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::branch::types::PageId;
use crate::error::FerroError;
use crate::provenance::sha256_of;

/// Width of a content id. 128 bits, and [`ChunkIndex`] treats it as a *hint* rather than an
/// identity — see the byte comparison in [`ChunkIndex::intern`].
pub const CONTENT_ID_BYTES: usize = 16;

/// A content id: the first [`CONTENT_ID_BYTES`] of the SHA-256 of a payload.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct ContentId(pub [u8; CONTENT_ID_BYTES]);

impl ContentId {
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(CONTENT_ID_BYTES * 2);
        for b in self.0.iter() {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }
}

/// The default content id: SHA-256 truncated to 128 bits.
///
/// Truncation is safe *here* only because equality is decided by the byte comparison and not by
/// this value. Reuses [`crate::provenance::sha256_of`] rather than adding a hash to the crate:
/// ferrodb has no runtime dependencies and that implementation is already the tested one.
pub fn content_id(payload: &[u8]) -> ContentId {
    let full = sha256_of(payload);
    let mut id = [0u8; CONTENT_ID_BYTES];
    id.copy_from_slice(&full[..CONTENT_ID_BYTES]);
    ContentId(id)
}

/// How a payload is turned into a content id. Swappable so the collision path can be *forced* in a
/// test rather than waited for.
pub type Hasher = fn(&[u8]) -> ContentId;

/// One stored payload: the page holding it and the number of references to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    page_id: PageId,
    refs: u32,
}

#[derive(Default)]
struct Inner {
    /// Content id -> the distinct payloads that hash to it. A `Vec` rather than a single entry
    /// because a 128-bit id is not a proof of equality: two different payloads that collide each
    /// keep their own page, and the list is what makes that representable.
    buckets: HashMap<ContentId, Vec<Entry>>,
    /// Page -> the content id it is filed under, so [`ChunkIndex::release`] needs neither a rehash
    /// nor a page read. Kept in lockstep with `buckets`; [`ChunkIndex::audit`] proves it.
    owner: HashMap<PageId, ContentId>,
    /// Provisional references: incremented, not yet recorded by the caller. Survives a `Drop` that
    /// never ran, which is the only thing a dead process leaves behind. See
    /// [`ChunkIndex::recover`].
    pending: HashMap<u64, PageId>,
    next_ticket: u64,
}

/// What [`ChunkIndex::intern`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interned {
    /// The payload was already stored. No page was allocated; `page_id` is the existing one.
    Shared { page_id: PageId },
    /// The payload is new. `page_id` is the page the caller's `alloc` returned, now at refcount 1.
    Fresh { page_id: PageId },
    /// Another thread published the same payload while this caller was allocating. `page_id` is
    /// the winner to use; `surplus` is the page this caller allocated and **must now free** — it
    /// is handed back rather than dropped, because a page allocated and forgotten is a leak with
    /// no owner to find it.
    SharedAfterRace { page_id: PageId, surplus: PageId },
}

impl Interned {
    /// The page the caller should reference, whichever way it went.
    pub fn page_id(&self) -> PageId {
        match self {
            Interned::Shared { page_id }
            | Interned::Fresh { page_id }
            | Interned::SharedAfterRace { page_id, .. } => *page_id,
        }
    }

    /// The page the caller allocated and no longer needs, if any.
    pub fn surplus(&self) -> Option<PageId> {
        match self {
            Interned::SharedAfterRace { surplus, .. } => Some(*surplus),
            _ => None,
        }
    }
}

/// A reference that has been counted but not yet recorded anywhere durable.
///
/// Drop rolls it back. That is the whole point: the window between raising a refcount and writing
/// down who owns the reference is where a leak is born, and a guard closes it for every in-process
/// exit — `?`, an early `return`, a panic. A *process* crash is not an in-process exit and Drop
/// does not run, which is what [`ChunkIndex::recover`] is for.
#[must_use = "an uncommitted RefTicket rolls its reference back on drop; bind it and call commit() \
              once the reference is recorded, or drop it deliberately to abandon the write"]
pub struct RefTicket<'a> {
    index: &'a ChunkIndex,
    page_id: PageId,
    ticket: u64,
    committed: bool,
}

impl RefTicket<'_> {
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// The caller has recorded the reference. The +1 is now owned by that record.
    pub fn commit(mut self) {
        self.committed = true;
        let mut inner = self.index.inner.lock().unwrap();
        inner.pending.remove(&self.ticket);
    }
}

impl Drop for RefTicket<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        // Roll back. Errors are impossible here by construction (the entry was present when the
        // ticket was issued and only this ticket can retire it), and Drop cannot report one
        // anyway, so a newly freed page goes on the `freed` queue for `take_freed`.
        let mut inner = self.index.inner.lock().unwrap();
        inner.pending.remove(&self.ticket);
        ChunkIndex::decrement(&mut inner, self.page_id, &mut self.index.freed.lock().unwrap());
    }
}

/// A content-addressed index over page payloads, with reference counts.
pub struct ChunkIndex {
    hasher: Hasher,
    inner: Mutex<Inner>,
    /// Pages whose refcount reached zero. Drained by [`ChunkIndex::take_freed`]. A queue rather
    /// than a callback because the only path that can free a page without returning a `Result` is
    /// `Drop`, and a `Drop` that calls back into a page store is a lock-order hazard.
    freed: Mutex<Vec<PageId>>,
}

impl Default for ChunkIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkIndex {
    pub fn new() -> Self {
        Self::with_hasher(content_id)
    }

    /// Build an index over a chosen hash.
    ///
    /// Exists so a test can make every payload collide and drive the comparison path, which is
    /// otherwise unreachable in any run short of astronomical. A branch no test has executed is
    /// not a tested branch, and this is the branch whose failure is silent data corruption.
    pub fn with_hasher(hasher: Hasher) -> Self {
        ChunkIndex { hasher, inner: Mutex::new(Inner::default()), freed: Mutex::new(Vec::new()) }
    }

    /// Store `payload`, sharing an existing page when one already holds exactly these bytes.
    ///
    /// `read_payload` reads back a stored page's payload so a candidate can be compared byte for
    /// byte; `alloc` allocates and fills a new page and is called **only** on a genuine miss.
    ///
    /// # Locking
    ///
    /// The comparison runs while the index lock is held and `alloc` runs while it is not. That
    /// split is deliberate. A page read goes through the buffer pool and takes no page-store lock,
    /// so holding across it is safe; allocation *does* take the store's arena lock, and calling it
    /// under this one would set up the classic two-lock inversion. The cost is a race window — two
    /// threads can miss, both allocate, and both come back — which is closed below by re-checking
    /// the bucket and reporting the loser's page as [`Interned::SharedAfterRace`] surplus rather
    /// than leaking it.
    ///
    /// `read_payload` must not call back into this index; it is handed no way to, and a Mutex is
    /// not reentrant, so an attempt deadlocks loudly instead of corrupting a refcount quietly.
    pub fn intern<R, A>(
        &self,
        payload: &[u8],
        read_payload: R,
        alloc: A,
    ) -> Result<(Interned, RefTicket<'_>), FerroError>
    where
        R: Fn(PageId) -> Result<Vec<u8>, FerroError>,
        A: FnOnce() -> Result<PageId, FerroError>,
    {
        let cid = (self.hasher)(payload);

        // Phase 1 — look for an existing page holding exactly these bytes.
        let seen = {
            let mut inner = self.inner.lock().unwrap();
            let candidates: Vec<PageId> =
                inner.buckets.get(&cid).map(|b| b.iter().map(|e| e.page_id).collect())
                    .unwrap_or_default();
            if let Some(hit) = Self::compare_all(&candidates, payload, &read_payload)? {
                let ticket = Self::increment(&mut inner, cid, hit)?;
                return Ok((
                    Interned::Shared { page_id: hit },
                    RefTicket { index: self, page_id: hit, ticket, committed: false },
                ));
            }
            candidates.len()
        };

        // Phase 2 — a genuine miss. Allocate OUTSIDE the lock; see the locking note above.
        let page_id = alloc()?;

        // Phase 3 — publish, re-checking only the entries that appeared while the lock was down.
        let mut inner = self.inner.lock().unwrap();
        let newcomers: Vec<PageId> = inner
            .buckets
            .get(&cid)
            .map(|b| b.iter().skip(seen).map(|e| e.page_id).collect())
            .unwrap_or_default();
        if let Some(winner) = Self::compare_all(&newcomers, payload, &read_payload)? {
            let ticket = Self::increment(&mut inner, cid, winner)?;
            return Ok((
                Interned::SharedAfterRace { page_id: winner, surplus: page_id },
                RefTicket { index: self, page_id: winner, ticket, committed: false },
            ));
        }
        if inner.owner.contains_key(&page_id) {
            return Err(FerroError::Cow(format!(
                "chunk index: page {} is already interned; alloc returned a page the index \
                 already owns",
                page_id
            )));
        }
        inner.buckets.entry(cid).or_default().push(Entry { page_id, refs: 1 });
        inner.owner.insert(page_id, cid);
        let ticket = inner.next_ticket;
        inner.next_ticket += 1;
        inner.pending.insert(ticket, page_id);
        Ok((
            Interned::Fresh { page_id },
            RefTicket { index: self, page_id, ticket, committed: false },
        ))
    }

    /// Compare `payload` against each candidate's stored bytes. Returns the first exact match.
    ///
    /// **This is the guard that makes a truncated hash safe**, so it compares the bytes and never
    /// the ids. A candidate the reader cannot read is an error, not a miss: treating an unreadable
    /// page as "not a match" would allocate a second copy and, worse, hide a corrupt page.
    fn compare_all<R>(
        candidates: &[PageId],
        payload: &[u8],
        read_payload: &R,
    ) -> Result<Option<PageId>, FerroError>
    where
        R: Fn(PageId) -> Result<Vec<u8>, FerroError>,
    {
        for pid in candidates {
            let stored = read_payload(*pid)?;
            if stored.as_slice() == payload {
                return Ok(Some(*pid));
            }
        }
        Ok(None)
    }

    /// Raise the refcount of `page_id` under `cid` and journal a provisional ticket.
    fn increment(inner: &mut Inner, cid: ContentId, page_id: PageId) -> Result<u64, FerroError> {
        let bucket = inner.buckets.get_mut(&cid).ok_or_else(|| {
            FerroError::Cow(format!("chunk index: bucket {} vanished", cid.to_hex()))
        })?;
        let e = bucket.iter_mut().find(|e| e.page_id == page_id).ok_or_else(|| {
            FerroError::Cow(format!("chunk index: page {} vanished from its bucket", page_id))
        })?;
        e.refs = e.refs.checked_add(1).ok_or_else(|| {
            FerroError::Cow(format!("chunk index: refcount overflow on page {}", page_id))
        })?;
        let ticket = inner.next_ticket;
        inner.next_ticket += 1;
        inner.pending.insert(ticket, page_id);
        Ok(ticket)
    }

    /// Lower the refcount of `page_id`, pushing it onto `freed` if it reaches zero.
    ///
    /// Infallible on purpose — `Drop` calls it and cannot report. Every caller that *can* report
    /// validates first; see [`ChunkIndex::release`].
    fn decrement(inner: &mut Inner, page_id: PageId, freed: &mut Vec<PageId>) {
        let Some(cid) = inner.owner.get(&page_id).copied() else {
            return;
        };
        let Some(bucket) = inner.buckets.get_mut(&cid) else {
            return;
        };
        let Some(ix) = bucket.iter().position(|e| e.page_id == page_id) else {
            return;
        };
        let e = &mut bucket[ix];
        e.refs = e.refs.saturating_sub(1);
        if e.refs == 0 {
            bucket.remove(ix);
            if bucket.is_empty() {
                inner.buckets.remove(&cid);
            }
            inner.owner.remove(&page_id);
            freed.push(page_id);
        }
    }

    /// Drop one recorded reference to `page_id`.
    ///
    /// **Refuses rather than saturates.** A release of a page the index does not hold, or of one
    /// already at zero, is the decrement that deletes live data; clamping it to zero would make
    /// that indistinguishable from success. The caller gets an error and the index is unchanged.
    pub fn release(&self, page_id: PageId) -> Result<(), FerroError> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.owner.contains_key(&page_id) {
            return Err(FerroError::Cow(format!(
                "chunk index: release of page {} which the index does not hold — an extra \
                 decrement, which is silent data loss if it is allowed through",
                page_id
            )));
        }
        let mut freed = self.freed.lock().unwrap();
        Self::decrement(&mut inner, page_id, &mut freed);
        Ok(())
    }

    /// Roll back every provisional reference that was never committed.
    ///
    /// **This is the crash-recovery half of the guard.** [`RefTicket`]'s `Drop` covers every
    /// in-process exit; a process that dies runs no destructor at all and leaves its increments
    /// behind, looking exactly like live references. Run this at startup, before anything trusts a
    /// refcount. Returns the pages that reached zero as a result.
    pub fn recover(&self) -> Vec<PageId> {
        let mut inner = self.inner.lock().unwrap();
        let orphans: Vec<(u64, PageId)> = inner.pending.iter().map(|(t, p)| (*t, *p)).collect();
        let mut freed = self.freed.lock().unwrap();
        for (ticket, page_id) in orphans {
            inner.pending.remove(&ticket);
            Self::decrement(&mut inner, page_id, &mut freed);
        }
        std::mem::take(&mut *freed)
    }

    /// Take the pages whose refcount has reached zero. The caller frees them in its page store.
    pub fn take_freed(&self) -> Vec<PageId> {
        std::mem::take(&mut *self.freed.lock().unwrap())
    }

    /// The refcount of `page_id`, or `None` if the index does not hold it.
    pub fn refcount(&self, page_id: PageId) -> Option<u32> {
        let inner = self.inner.lock().unwrap();
        let cid = inner.owner.get(&page_id)?;
        inner.buckets.get(cid)?.iter().find(|e| e.page_id == page_id).map(|e| e.refs)
    }

    /// Distinct payloads currently stored.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().buckets.values().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Provisional references outstanding. Non-zero at shutdown means [`Self::recover`] has work.
    pub fn pending_len(&self) -> usize {
        self.inner.lock().unwrap().pending.len()
    }

    /// Check the index against itself: `owner` and `buckets` name the same pages, no refcount is
    /// zero, and no page appears twice. Returns the first inconsistency found.
    ///
    /// Two structures holding the same fact will disagree eventually; this is what turns that into
    /// a test failure rather than a wrong answer.
    pub fn audit(&self) -> Result<(), FerroError> {
        let inner = self.inner.lock().unwrap();
        let mut seen: HashMap<PageId, ContentId> = HashMap::new();
        for (cid, bucket) in inner.buckets.iter() {
            for e in bucket {
                if e.refs == 0 {
                    return Err(FerroError::Cow(format!(
                        "chunk index audit: page {} is present at refcount 0",
                        e.page_id
                    )));
                }
                if let Some(prev) = seen.insert(e.page_id, *cid) {
                    return Err(FerroError::Cow(format!(
                        "chunk index audit: page {} appears under two content ids {} and {}",
                        e.page_id,
                        prev.to_hex(),
                        cid.to_hex()
                    )));
                }
                match inner.owner.get(&e.page_id) {
                    Some(o) if o == cid => {}
                    Some(o) => {
                        return Err(FerroError::Cow(format!(
                            "chunk index audit: page {} is filed under {} but owner says {}",
                            e.page_id,
                            cid.to_hex(),
                            o.to_hex()
                        )));
                    }
                    None => {
                        return Err(FerroError::Cow(format!(
                            "chunk index audit: page {} is in a bucket but has no owner entry",
                            e.page_id
                        )));
                    }
                }
            }
        }
        for page_id in inner.owner.keys() {
            if !seen.contains_key(page_id) {
                return Err(FerroError::Cow(format!(
                    "chunk index audit: owner names page {} which is in no bucket",
                    page_id
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as Map;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex as StdMutex;

    /// A stand-in page store: page id -> payload. The index never touches a real buffer pool, so
    /// these tests are about refcount arithmetic and nothing else.
    #[derive(Default)]
    struct Pages {
        map: StdMutex<Map<PageId, Vec<u8>>>,
        next: AtomicU32,
        allocs: AtomicU32,
    }

    impl Pages {
        fn new() -> Pages {
            Pages { map: StdMutex::new(Map::new()), next: AtomicU32::new(1), allocs: AtomicU32::new(0) }
        }
        fn reader(&self) -> impl Fn(PageId) -> Result<Vec<u8>, FerroError> + '_ {
            move |p| {
                self.map
                    .lock()
                    .unwrap()
                    .get(&p)
                    .cloned()
                    .ok_or_else(|| FerroError::Cow(format!("no page {}", p)))
            }
        }
        /// Allocate a page holding `payload`.
        fn alloc(&self, payload: &[u8]) -> Result<PageId, FerroError> {
            let id = self.next.fetch_add(1, Ordering::SeqCst);
            self.allocs.fetch_add(1, Ordering::SeqCst);
            self.map.lock().unwrap().insert(id, payload.to_vec());
            Ok(id)
        }
        fn alloc_count(&self) -> u32 {
            self.allocs.load(Ordering::SeqCst)
        }
    }

    /// Intern `payload` and commit the reference, the ordinary caller flow.
    fn put(ix: &ChunkIndex, pages: &Pages, payload: &[u8]) -> Interned {
        let (outcome, ticket) = ix
            .intern(payload, pages.reader(), || pages.alloc(payload))
            .expect("intern");
        ticket.commit();
        outcome
    }

    #[test]
    fn identical_payloads_share_one_page_and_allocate_once() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let a = put(&ix, &pages, b"the same bytes");
        let b = put(&ix, &pages, b"the same bytes");
        assert!(matches!(a, Interned::Fresh { .. }), "first is fresh, got {a:?}");
        assert!(matches!(b, Interned::Shared { .. }), "second shares, got {b:?}");
        assert_eq!(a.page_id(), b.page_id(), "both callers reference one page");
        assert_eq!(pages.alloc_count(), 1, "the second write allocated nothing");
        assert_eq!(ix.refcount(a.page_id()), Some(2));
        assert_eq!(ix.len(), 1);
        ix.audit().expect("audit");
    }

    #[test]
    fn different_payloads_never_share() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let a = put(&ix, &pages, b"alpha");
        let b = put(&ix, &pages, b"beta");
        assert_ne!(a.page_id(), b.page_id());
        assert_eq!(pages.alloc_count(), 2);
        assert_eq!(ix.len(), 2);
        ix.audit().expect("audit");
    }

    /// Every payload is forced to one content id, so the comparison path — the guard that makes a
    /// truncated hash safe — is the only thing standing between these two payloads.
    ///
    /// This is the fire-check for that guard. Without the byte comparison the second payload would
    /// be handed the first one's page and its contents would be silently replaced by different
    /// bytes, which is the corruption the brief names.
    #[test]
    fn a_content_id_collision_does_not_share_pages() {
        fn always_collide(_: &[u8]) -> ContentId {
            ContentId([0u8; CONTENT_ID_BYTES])
        }
        let ix = ChunkIndex::with_hasher(always_collide);
        let pages = Pages::new();

        let a = put(&ix, &pages, b"payload one");
        let b = put(&ix, &pages, b"payload two -- different bytes, same content id");
        assert_eq!(
            (always_collide(b"payload one"), always_collide(b"payload two")),
            (ContentId([0; 16]), ContentId([0; 16])),
            "the test's premise: these really do collide",
        );
        assert_ne!(a.page_id(), b.page_id(), "colliding payloads must not share a page");
        assert_eq!(pages.alloc_count(), 2);
        assert_eq!(ix.len(), 2, "one bucket, two entries");

        // And a genuine repeat still shares, so the collision handling has not simply disabled
        // dedup for the whole bucket.
        let again = put(&ix, &pages, b"payload one");
        assert_eq!(again.page_id(), a.page_id());
        assert_eq!(pages.alloc_count(), 2, "the repeat allocated nothing");
        assert_eq!(ix.refcount(a.page_id()), Some(2));
        ix.audit().expect("audit");
    }

    #[test]
    fn releasing_the_last_reference_frees_the_page() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let a = put(&ix, &pages, b"content");
        put(&ix, &pages, b"content");
        assert_eq!(ix.refcount(a.page_id()), Some(2));

        ix.release(a.page_id()).expect("first release");
        assert_eq!(ix.refcount(a.page_id()), Some(1));
        assert!(ix.take_freed().is_empty(), "still referenced, nothing freed");

        ix.release(a.page_id()).expect("second release");
        assert_eq!(ix.refcount(a.page_id()), None);
        assert_eq!(ix.take_freed(), vec![a.page_id()], "the page is now the caller's to free");
        assert!(ix.is_empty());
        ix.audit().expect("audit");
    }

    /// An extra decrement is silent data loss, so it is refused rather than clamped.
    #[test]
    fn an_over_release_is_refused_and_changes_nothing() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let a = put(&ix, &pages, b"content");
        ix.release(a.page_id()).expect("the one real release");

        let err = ix.release(a.page_id()).expect_err("the second release must be refused");
        assert!(
            format!("{err:?}").contains("extra decrement"),
            "the error should name what it is guarding: {err:?}"
        );
        assert!(ix.release(999).is_err(), "a page the index never held is refused too");
        ix.audit().expect("audit");
    }

    /// The in-process half of the guard: the caller took a reference and then failed before
    /// recording it. `?`, an early return and a panic all land here.
    #[test]
    fn an_uncommitted_ticket_rolls_its_increment_back_on_drop() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let a = put(&ix, &pages, b"content");
        assert_eq!(ix.refcount(a.page_id()), Some(1));

        {
            let (outcome, _ticket) =
                ix.intern(b"content", pages.reader(), || pages.alloc(b"content")).expect("intern");
            assert_eq!(outcome.page_id(), a.page_id());
            assert_eq!(ix.refcount(a.page_id()), Some(2), "provisionally 2 inside the scope");
            // `_ticket` drops here without commit — the caller never recorded the reference.
        }

        assert_eq!(
            ix.refcount(a.page_id()),
            Some(1),
            "the provisional increment was rolled back, so nothing leaked"
        );
        assert_eq!(ix.pending_len(), 0);
        ix.audit().expect("audit");
    }

    /// A ticket dropped uncommitted on a *fresh* page takes the page's only reference with it, so
    /// the page comes back to the caller to free rather than leaking at refcount 0.
    #[test]
    fn an_uncommitted_ticket_on_a_fresh_page_hands_the_page_back() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        {
            let (outcome, _ticket) =
                ix.intern(b"only", pages.reader(), || pages.alloc(b"only")).expect("intern");
            assert!(matches!(outcome, Interned::Fresh { .. }));
        }
        assert!(ix.is_empty(), "the entry went with its only reference");
        assert_eq!(ix.take_freed().len(), 1, "the allocated page is handed back, not leaked");
        ix.audit().expect("audit");
    }

    /// **The crash window the brief names, tested directly.**
    ///
    /// `std::mem::forget` is precisely what a dead process looks like to this data structure: the
    /// increment happened, the caller never recorded it, and `Drop` never ran. Without `recover`
    /// the refcount stays permanently one too high and the page can never be freed — a leak
    /// forever, which is D85's failure mode.
    ///
    /// The assertion is against the refcount *before* the forgotten ticket, read from the index,
    /// not against a constant recomputed from the subject.
    #[test]
    fn a_crash_between_the_increment_and_the_record_leaks_nothing() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let a = put(&ix, &pages, b"content");
        let before = ix.refcount(a.page_id()).expect("held");

        let (outcome, ticket) =
            ix.intern(b"content", pages.reader(), || pages.alloc(b"content")).expect("intern");
        assert_eq!(outcome.page_id(), a.page_id());
        assert_eq!(ix.refcount(a.page_id()), Some(before + 1), "the increment is live");
        assert_eq!(ix.pending_len(), 1, "and journalled as provisional");

        std::mem::forget(ticket); // the process dies here: no destructor runs.
        assert_eq!(
            ix.refcount(a.page_id()),
            Some(before + 1),
            "the increment survives the crash, which is exactly the leak"
        );

        let freed = ix.recover();
        assert_eq!(
            ix.refcount(a.page_id()),
            Some(before),
            "recover rolled the orphaned increment back to the pre-crash refcount"
        );
        assert!(freed.is_empty(), "the page was still referenced, so nothing was freed");
        assert_eq!(ix.pending_len(), 0);
        ix.audit().expect("audit");
    }

    /// The same crash, on a page whose only reference was the forgotten one. Recovery must free
    /// the page rather than leave it stranded at refcount 1 with no owner.
    #[test]
    fn recovery_frees_a_page_whose_only_reference_was_lost_to_the_crash() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let (outcome, ticket) =
            ix.intern(b"orphan", pages.reader(), || pages.alloc(b"orphan")).expect("intern");
        let page = outcome.page_id();
        std::mem::forget(ticket);
        assert_eq!(ix.refcount(page), Some(1));

        let freed = ix.recover();
        assert_eq!(freed, vec![page], "recovery hands the stranded page back to be freed");
        assert_eq!(ix.refcount(page), None);
        assert!(ix.is_empty());
        ix.audit().expect("audit");
    }

    /// A committed ticket is NOT rolled back by recovery. Without this, `recover` would be a
    /// mass over-decrement — the silent-data-loss direction — and every test above would still
    /// pass, because they all check the leak direction only.
    #[test]
    fn recovery_leaves_committed_references_alone() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let a = put(&ix, &pages, b"content");
        put(&ix, &pages, b"content");
        assert_eq!(ix.refcount(a.page_id()), Some(2));
        assert_eq!(ix.pending_len(), 0, "commit retires the journal entry");

        let freed = ix.recover();
        assert!(freed.is_empty());
        assert_eq!(
            ix.refcount(a.page_id()),
            Some(2),
            "recovery must not touch references that were properly recorded"
        );
        ix.audit().expect("audit");
    }

    /// Interning under concurrency must not double-count or lose a reference. The final refcount
    /// is read from the index and compared against the number of committed tickets, which is
    /// counted by the test rather than reported by the subject.
    #[test]
    fn concurrent_interning_of_one_payload_counts_every_reference_once() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let threads = 8;
        let per = 16;

        std::thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    for _ in 0..per {
                        let (_, ticket) = ix
                            .intern(b"hot payload", pages.reader(), || pages.alloc(b"hot payload"))
                            .expect("intern");
                        ticket.commit();
                    }
                });
            }
        });

        // Every allocation beyond the first is a page the loser of a race handed back as surplus,
        // so the index still holds exactly one entry.
        assert_eq!(ix.len(), 1, "one payload, one entry");
        let page = {
            let inner = ix.inner.lock().unwrap();
            inner.owner.keys().copied().next().expect("one page")
        };
        assert_eq!(
            ix.refcount(page),
            Some((threads * per) as u32),
            "one reference per committed ticket, no more and no fewer"
        );
        assert_eq!(ix.pending_len(), 0);
        ix.audit().expect("audit");
    }

    /// Releasing every reference taken concurrently must land exactly on zero and free the page
    /// exactly once — the arithmetic that a leak or an over-free both break.
    #[test]
    fn concurrent_release_frees_the_page_exactly_once() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let n = 64;
        let mut page = 0;
        for _ in 0..n {
            page = put(&ix, &pages, b"shared").page_id();
        }
        assert_eq!(ix.refcount(page), Some(n));

        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for _ in 0..(n / 8) {
                        ix.release(page).expect("release");
                    }
                });
            }
        });

        assert_eq!(ix.refcount(page), None, "every reference was dropped");
        assert_eq!(ix.take_freed(), vec![page], "freed once, not 8 times");
        assert!(ix.is_empty());
        ix.audit().expect("audit");
    }

    /// The audit is only worth running if it can fail. Corrupt the index on purpose and prove it
    /// reports — otherwise every `audit().expect(..)` above is decorative.
    #[test]
    fn the_audit_detects_an_inconsistency_it_is_supposed_to_catch() {
        let ix = ChunkIndex::new();
        let pages = Pages::new();
        let a = put(&ix, &pages, b"content");
        ix.audit().expect("clean to start with");

        // A page named by `owner` but present in no bucket: the shape a half-finished free leaves.
        ix.inner.lock().unwrap().owner.insert(4242, ContentId([7u8; CONTENT_ID_BYTES]));
        let err = ix.audit().expect_err("the audit must see this");
        assert!(format!("{err:?}").contains("4242"), "and must name the page: {err:?}");

        ix.inner.lock().unwrap().owner.remove(&4242);
        ix.audit().expect("clean again");

        // A refcount of zero left in a bucket: the shape a missed removal leaves.
        {
            let mut inner = ix.inner.lock().unwrap();
            let cid = *inner.owner.get(&a.page_id()).unwrap();
            inner.buckets.get_mut(&cid).unwrap()[0].refs = 0;
        }
        let err = ix.audit().expect_err("a zero refcount is an inconsistency");
        assert!(format!("{err:?}").contains("refcount 0"), "{err:?}");
    }
}
