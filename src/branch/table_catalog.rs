//! The branch catalog stored the way every other database stores a catalog: **as a table.**
//!
//! `pg_class` is a heap table with B+tree indexes. SQLite's `sqlite_schema` is a table on page 1.
//! InnoDB moved `.frm` files into data-dictionary tables in 8.0 specifically to stop having a
//! second storage mechanism. This is that, over `BPlusTreeManager`, which already backs every
//! user table's primary index in this repo.
//!
//! **What it replaces and why.** `LogBranchCatalog` is an append-only file replayed at open into a
//! `HashMap<u64, BranchRecord>` of every record. Measured (`bench/o1_residency_marginal.txt`), that
//! map costs ~685 resident bytes per branch and rises, because a doubling table's freed
//! predecessors are retained by the allocator. Open is O(operations ever written) and the log never
//! compacts, so a branch created and reaped leaves records for ever. Here the records live in tree
//! pages under a bounded buffer pool, and open is one descent to read a header key.
//!
//! **One tree, not seven.** Records and every index over them share a single tree, separated by the
//! leading tag byte in [`crate::branch::tree_keys`]. One root page id to persist rather than seven,
//! and therefore one thing to keep crash-consistent instead of seven that can disagree.
//!
//! **Unbounded fields are key spans, not record fields.** A leaf page holds about 2 KB of entries
//! in total. `live_children` at 10⁶ branches is 8 MB, `arenas` reaches ~900 ids for a branch that
//! has written ~230 MB, and an envelope carries a table list. A record that can outgrow a page is
//! a wall with no error message, so all three live in their own spans.
//!
//! See `SCALE-DESIGN.md` D2b.

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::branch::group_commit::CommitGroup;
use crate::branch::record::{BranchRecord, CapabilityEnvelope, CoreRecord};
use crate::branch::tree_keys as keys;
use crate::branch::types::{
    ArenaId, BranchError, BranchId, BranchState, Epoch, LeaseDeadline, PageId,
};
use crate::branch::BranchCatalog;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::error::FerroError;
use crate::storage::index::BPlusTreeManager;

/// Header payload: `next_id` then `epoch`, both big-endian.
const HEADER_BYTES: usize = 16;

pub struct TableBranchCatalog {
    tree: BPlusTreeManager<Vec<u8>, Vec<u8>>,
    /// Serialises **logical** operations, each of which touches several keys.
    ///
    /// The tree's own locking protects a single key. A fork writes a core record, a deadline key, a
    /// state key and a child key; without this, two concurrent forks can interleave and a reader
    /// can see a branch that exists but is not yet in its parent's live set — which is the GC
    /// correctness hole the log catalog closed by putting both halves in one append.
    logical: Mutex<()>,
    next_id: AtomicU64,
    epoch: AtomicU64,
    /// The buffer pool, kept so the header page can be written without threading it through.
    pool: Arc<BufferPoolManager>,
    /// The fixed page naming the tree root. `0` means "none" - `create` builds a catalog with no
    /// durable bootstrap, which is what the bench and the tests want.
    header_page: std::sync::atomic::AtomicU32,
    /// The root page id last written to the header page, so a mutation that did not split the root
    /// costs one atomic compare rather than a page write.
    published_root: std::sync::atomic::AtomicU32,
    /// One fsync shared by every writer waiting on it. See `group_commit`: forks used to hold
    /// `logical` across the fsync, so 64 concurrent forkers produced no more throughput than one
    /// (measured x0.92, `bench/fork_concurrency_before.txt`).
    commit_group: CommitGroup,
}

/// Magic in the first four bytes of the header page, so opening the wrong page id is an error
/// rather than a plausible-looking root taken from whatever was there.
const HEADER_PAGE_MAGIC: u32 = 0xFE44_0B01;

/// Where the header lives in a dedicated catalog file: the first page after the bitmap, which is
/// what `BufferPoolManager::new_page` hands out first on a fresh file. Fixed rather than recorded
/// elsewhere, because a file holding only the catalog needs no second place to look.
pub const SIDECAR_HEADER_PAGE: u32 = 1;

impl TableBranchCatalog {
    /// Create a catalog and a **header page whose own id never changes**, returning that id.
    ///
    /// The tree's root page id cannot live in the tree (reading the tree needs it) and is not
    /// stable (it moves whenever the root splits). The caller therefore has to persist *something*
    /// across restarts, and the choice matters: writing the moving root to a sidecar on every split
    /// means a crash between the tree write and the sidecar write leaves a database pointing at a
    /// stale root — silently, because a stale root is a perfectly valid B+tree of an older state.
    ///
    /// A fixed indirection page moves the volatile value inside the database, so there is ONE
    /// write to order instead of two files to keep in step, and leaves the caller holding an id
    /// that is written once and never changes. This is the superblock-pointer arrangement: a known
    /// location naming a moving root, the same shape as SQLite finding `sqlite_schema` at page 1.
    ///
    /// ⚠ **This used to claim the page lands "where the buffer pool's WAL gate already orders
    /// writes". THAT IS FALSE, and the replacement is stricter than the claim it removes — the
    /// gate is a provable no-op here, so anything built on it was resting on nothing.**
    ///
    /// `BufferPoolManager::wal_gate` flushes only `if plsn > 0`, and `plsn` comes from
    /// `page_lsn_of`, which is `match data[0] { 0 => .., 2 | 3 => .., _ => 0 }` — an LSN exists
    /// only for a heap page (0) and a B+tree internal/leaf page (2, 3). This page's first four
    /// bytes are [`HEADER_PAGE_MAGIC`] (`0xFE44_0B01`), so `data[0]` is `0xFE`, which takes the
    /// `_ => 0` arm: **the gate does nothing on it.** Nor does it help the tree this page names —
    /// B+tree pages never set an LSN either, and index structure is not logged at all, because
    /// `wal::recovery::rebuild_indexes` frees every index tree and builds a fresh one from the
    /// heap. There is no ordering here for a gate to provide.
    ///
    /// ⇒ What the fixed page buys is the single-write sentence above, and nothing more.
    /// **Do not build a durability argument on the WAL gate.**
    pub fn create_with_header(
        pool: Arc<BufferPoolManager>,
        trunk_root: PageId,
    ) -> Result<(Self, u32), FerroError> {
        let header_page = pool.new_page()?;
        pool.unpin_page(header_page, false);
        let cat = Self::create(pool, trunk_root)?;
        cat.header_page.store(header_page, Ordering::SeqCst);
        cat.publish_root()?;
        Ok((cat, header_page))
    }

    /// Reopen from the header page id the caller persisted at creation.
    pub fn open_from_header(
        pool: Arc<BufferPoolManager>,
        header_page: u32,
    ) -> Result<Self, FerroError> {
        let frame_i = pool.fetch_page(header_page)?;
        let (magic, root) = {
            let f = pool.frames[frame_i].read().unwrap();
            (
                u32::from_be_bytes(f.data[0..4].try_into().unwrap()),
                u32::from_be_bytes(f.data[4..8].try_into().unwrap()),
            )
        };
        pool.unpin_page(header_page, false);
        if magic != HEADER_PAGE_MAGIC {
            return Err(FerroError::Branch(format!(
                "page {header_page} is not a branch-catalog header (magic {magic:#010x}, expected \
                 {HEADER_PAGE_MAGIC:#010x}); refusing to read a root page id out of whatever this \
                 page actually holds"
            )));
        }
        let cat = Self::open(pool, root)?;
        cat.header_page.store(header_page, Ordering::SeqCst);
        cat.published_root.store(root, Ordering::SeqCst);
        Ok(cat)
    }

    /// Open — or create — the branch catalog **in its own file**.
    ///
    /// The catalog was briefly put in the main database's pages, and that inherited a ceiling it
    /// had no reason to: the ordinary allocator is confined below the arena floor, a budget of
    /// `DEFAULT_ARENA_HEADROOM` (32,736 pages, ~134 MB) shared with every user table and fixed when
    /// the database is created. At the measured 250 bytes per branch, 10⁶ branches want ~61,000
    /// pages — 1.87x that entire budget, before one user row.
    ///
    /// `LogBranchCatalog`, which this replaces, is a **sidecar** (`{db}.branches`) and never
    /// competed for it. Keeping that arrangement and changing only the format inside the file
    /// removes the ceiling without a region table, without bounding the arena, and without touching
    /// the consensus grant protocol that decides arena page addresses. A sidecar replaces a
    /// sidecar; `{db}.arena` and `{db}.provenance` are the same pattern.
    ///
    /// The second buffer pool is the honest cost: a second frame budget, real memory. It is also
    /// what keeps the residency win — a bounded pool is bounded whichever file it is over.
    pub fn open_sidecar(path: &std::path::Path, trunk_root: PageId) -> Result<Self, FerroError> {
        // Decided BEFORE the file is touched: `DiskManager::new` writes the bitmap into an empty
        // file, so afterwards the length no longer distinguishes a new catalog from an existing
        // one, and a fresh `create` over a populated file would silently orphan every branch.
        let fresh = std::fs::metadata(path).map(|m| m.len() == 0).unwrap_or(true);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| {
                FerroError::Io(format!("open branch catalog {}: {e}", path.display()))
            })?;
        let pool = Arc::new(BufferPoolManager::new(Arc::new(
            crate::storage::disk_manager::DiskManager::new(file)?,
        )));

        if fresh {
            let (cat, header) = Self::create_with_header(pool, trunk_root)?;
            // Not an assumption - a check. `open_sidecar` reopens at a FIXED page id, so if page
            // allocation ever stops making the header the first page after the bitmap, the next
            // open would read a tree page as a header. Failing here is recoverable; failing there
            // is a database that cannot find its own branches.
            if header != SIDECAR_HEADER_PAGE {
                return Err(FerroError::Branch(format!(
                    "branch catalog header landed on page {header}, not the expected \
                     {SIDECAR_HEADER_PAGE}; reopening would read the wrong page as a header"
                )));
            }
            Ok(cat)
        } else {
            Self::open_from_header(pool, SIDECAR_HEADER_PAGE)
        }
    }

    /// The catalog for a database, migrating a legacy `{db}.branches` log if one is present.
    ///
    /// Named after `DurableEffectLog::default_for_database`, and for the reason `cli.rs` gives
    /// there: the naming convention and the choice of implementation belong beside the format, and
    /// an entry point wiring a runtime should make no decision.
    ///
    /// **Crash safety comes from an atomic rename, not from a journal.** The migration writes
    /// `{db}.branchcat.tmp` and renames it into place only once it is complete and flushed, so the
    /// PRESENCE of `{db}.branchcat` means "finished". Migrating straight into the final name would
    /// leave a half-built catalog that the next open would happily use.
    ///
    /// The legacy log is **renamed aside, never deleted**: a conversion that turns out to be wrong
    /// is recoverable only if its source survives it, and renaming costs nothing.
    pub fn default_for_database(db_path: &str, trunk_root: PageId) -> Result<Self, FerroError> {
        use std::path::Path;
        let cat_path = format!("{db_path}.branchcat");
        let legacy_path = format!("{db_path}.branches");
        let tmp_path = format!("{db_path}.branchcat.tmp");
        let retired_path = format!("{db_path}.branches.pre-table");

        let has = |p: &str| std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false);

        if has(&cat_path) {
            // Finished, whatever else is lying around. If a crash landed between the two renames
            // the legacy log is still here and is now stale; retire it rather than leave two
            // catalogs where a later reader might pick the wrong one.
            if has(&legacy_path) {
                let _ = std::fs::rename(&legacy_path, &retired_path);
            }
            return Self::open_sidecar(Path::new(&cat_path), trunk_root);
        }

        if has(&legacy_path) {
            // A previous attempt may have died partway; its tmp is garbage by construction.
            let _ = std::fs::remove_file(&tmp_path);
            {
                let source = crate::branch::catalog::LogBranchCatalog::open(
                    Path::new(&legacy_path),
                    trunk_root,
                )?;
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&tmp_path)
                    .map_err(|e| FerroError::Io(format!("create {tmp_path}: {e}")))?;
                let pool = Arc::new(BufferPoolManager::new(Arc::new(
                    crate::storage::disk_manager::DiskManager::new(file)?,
                )));
                let (cat, header) = Self::migrate_from(pool, &source, trunk_root)?;
                if header != SIDECAR_HEADER_PAGE {
                    return Err(FerroError::Branch(format!(
                        "migrated catalog header landed on page {header}, not \
                         {SIDECAR_HEADER_PAGE}; refusing to rename it into place"
                    )));
                }
                // Everything must be on disk BEFORE the rename publishes it.
                cat.pool.flush_all()?;
            }
            std::fs::rename(&tmp_path, &cat_path)
                .map_err(|e| FerroError::Io(format!("publish {cat_path}: {e}")))?;
            // Only now is the log redundant.
            std::fs::rename(&legacy_path, &retired_path)
                .map_err(|e| FerroError::Io(format!("retire {legacy_path}: {e}")))?;
            return Self::open_sidecar(Path::new(&cat_path), trunk_root);
        }

        Self::open_sidecar(Path::new(&cat_path), trunk_root)
    }

    /// Build a catalog from an existing one, record for record.
    ///
    /// Used to convert a `{db}.branches` log into `{db}.branchcat` on first open. It reads the
    /// source through the **trait**, so it is not specific to `LogBranchCatalog` and the
    /// equivalence test can drive it with either.
    ///
    /// Child entries are rebuilt from the children themselves — every non-`Reaped` record that
    /// names a parent contributes one — which is exactly how `LogBranchCatalog::index` derives the
    /// live set at replay. Rebuilding from the PARENT's `live_children` array instead would copy a
    /// representation rather than re-derive the truth, and would carry across any staleness the
    /// source happened to hold.
    pub fn migrate_from(
        pool: Arc<BufferPoolManager>,
        source: &dyn BranchCatalog,
        trunk_root: PageId,
    ) -> Result<(Self, u32), FerroError> {
        let (cat, header) = Self::create_with_header(pool, trunk_root)?;

        // Records first: `attach_child` and `release_id` both RESOLVE a child's record, so they
        // cannot run before the records exist.
        let mut max_id = 0u64;
        let mut reaped: Vec<u64> = Vec::new();
        let mut children: Vec<(u64, Epoch, u64)> = Vec::new();
        for rec in source.scan()? {
            let rec = rec?;
            // `scan` yields core records; the unbounded fields come from the source's own `get`,
            // which is where arenas and the envelope live.
            let full = source.get_raw(rec.branch_id.id).unwrap_or(rec);
            max_id = max_id.max(full.branch_id.id);
            if full.state == BranchState::Reaped {
                reaped.push(full.branch_id.id);
            }
            if full.state != BranchState::Reaped {
                if let Some(p) = full.parent_id {
                    children.push((p.id, full.fork_epoch, full.branch_id.id));
                }
            }
            let old = cat.core(full.branch_id.id)?;
            cat.write_record(&full, old.as_ref())?;
        }
        for (parent, epoch, child) in children {
            cat.attach_child(parent, epoch, child)?;
        }

        // Counters, matching what `LogBranchCatalog::open` derives: the next id is one past the
        // highest, and the epoch counter holds the LAST epoch issued (see `next_epoch`).
        cat.next_id.store(max_id + 1, Ordering::SeqCst);
        cat.epoch.store(source.current_epoch().0, Ordering::SeqCst);

        // Free ids last: `release_id` refuses while a slot still has live children, so it has to
        // see the child entries that were just attached.
        for id in reaped {
            cat.release_id(id);
        }
        cat.write_header()?;
        cat.publish_root()?;
        Ok((cat, header))
    }

    /// Publish the root **and make everything this operation wrote durable.**
    ///
    /// The log catalog fsyncs every append, which is why forking is fsync-bound at ~270/sec. This
    /// catalog writes through a buffer pool, and without this it is not durable at all: the first
    /// version of the switchover created a database, wrote the header page into a frame, exited,
    /// and the next open found `magic 0x00000000` because the page had never reached the disk.
    /// Every branch the process created was gone.
    ///
    /// **That also means the earlier benchmark's "6x less total time" was partly the cost of not
    /// being durable.** The bench artifact said time was not comparable between the two catalogs;
    /// it is now comparable, and the honest number is measured rather than assumed.
    /// fsyncs issued by this catalog. Exposed so a benchmark can report forks-per-fsync, which is
    /// the only direct evidence that group commit is batching rather than merely not-regressing.
    pub fn syncs_issued(&self) -> u64 {
        self.commit_group.syncs()
    }

    /// Publish the root and take a commit ticket. **Call under the logical lock, after the LAST
    /// mutation** — the ticket's meaning is "everything up to here is in the pool", and taking it
    /// earlier would let the group's leader mark work durable whose pages were never written.
    fn stage(&self) -> Result<u64, FerroError> {
        self.publish_root()?;
        Ok(self.commit_group.ticket())
    }

    /// Wait until an fsync covering `seq` has completed. **Call after RELEASING the logical lock.**
    /// One waiter issues the sync and the rest share it, which is the entire point: holding the
    /// lock here would put the serialization straight back.
    fn durable(&self, seq: u64) -> Result<(), FerroError> {
        self.commit_group.wait_durable(seq, || {
            self.pool.flush_all()?;
            self.pool.disk_manager.sync()
        })
    }

    // `commit()` (stage + durable in one call) was DELETED once every caller had been migrated.
    // It was the only remaining way to fsync while still holding `logical`, which is exactly the
    // serialization group commit exists to remove -- so leaving it would have left a second, worse
    // way to do the same thing, and the next method added would have reached for the shorter name.

    /// Write the current tree root into the header page, if it moved.
    ///
    /// Called after every mutation. The comparison is an atomic load, so the common case — no root
    /// split — costs nothing and the page is written only when a split actually happened.
    fn publish_root(&self) -> Result<(), FerroError> {
        let header_page = self.header_page.load(Ordering::SeqCst);
        if header_page == 0 {
            // No header page: this catalog was built with `create`, which is the test and bench
            // path. Nothing to publish, and silently skipping is correct rather than an error -
            // `create` does not promise a durable bootstrap, `create_with_header` does.
            return Ok(());
        }
        let root = self.tree.root_page_id.load(Ordering::SeqCst);
        if self.published_root.swap(root, Ordering::SeqCst) == root {
            return Ok(());
        }
        let frame_i = self.pool.fetch_page(header_page)?;
        {
            let mut f = self.pool.frame_write(frame_i);
            f.data = [0u8; crate::storage::disk_manager::PAGE_SIZE];
            f.data[0..4].copy_from_slice(&HEADER_PAGE_MAGIC.to_be_bytes());
            f.data[4..8].copy_from_slice(&root.to_be_bytes());
        }
        self.pool.unpin_page(header_page, true);
        Ok(())
    }

    /// Create an empty catalog containing only trunk, and return it with the tree's root page id.
    ///
    /// The root page id is the **one** value that cannot live in the tree, because reading the tree
    /// requires it. Everything else — `next_id`, the epoch counter — is a key, so `open` is one
    /// descent rather than a replay.
    pub fn create(pool: Arc<BufferPoolManager>, trunk_root: PageId) -> Result<Self, FerroError> {
        let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::create(Arc::clone(&pool))?;
        let cat = TableBranchCatalog {
            tree,
            logical: Mutex::new(()),
            next_id: AtomicU64::new(1),
            // 0, not 1: a fresh `LogBranchCatalog` derives `max_epoch` from trunk's fork epoch,
            // which is 0, and seeds its counter with exactly that. Seeding 1 here would make the
            // first epoch this catalog hands out differ from the first the log hands out.
            epoch: AtomicU64::new(0),
            pool,
            header_page: std::sync::atomic::AtomicU32::new(0),
            commit_group: CommitGroup::default(),
            published_root: std::sync::atomic::AtomicU32::new(0),
        };
        let trunk = BranchRecord::trunk(trunk_root, crate::branch::TRUNK_LEASE);
        cat.tree.insert(keys::record(trunk.branch_id.id), trunk.serialize_core())?;
        cat.tree.insert(keys::state(trunk.state.as_u8(), trunk.branch_id.id), Vec::new())?;
        // Trunk is deliberately absent from the DEADLINE span: it is excluded from every reap
        // query, so an entry for it would sit at the head of the range for ever and every scan
        // would step over it.
        cat.write_header()?;
        Ok(cat)
    }

    /// Reopen an existing catalog from its tree root. One descent, no replay.
    pub fn open(pool: Arc<BufferPoolManager>, root_page_id: u32) -> Result<Self, FerroError> {
        let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::open(root_page_id, Arc::clone(&pool));
        let cat = TableBranchCatalog {
            tree,
            logical: Mutex::new(()),
            next_id: AtomicU64::new(1),
            epoch: AtomicU64::new(1),
            pool,
            header_page: std::sync::atomic::AtomicU32::new(0),
            commit_group: CommitGroup::default(),
            published_root: std::sync::atomic::AtomicU32::new(0),
        };
        let bytes = cat.tree.search(&keys::header())?.ok_or_else(|| {
            FerroError::Branch("branch catalog header key is missing; the tree root is wrong \
                                or the catalog was never created".into())
        })?;
        if bytes.len() != HEADER_BYTES {
            return Err(BranchError::Corrupt(format!(
                "catalog header must be {HEADER_BYTES} bytes, got {}",
                bytes.len()
            ))
            .into());
        }
        cat.next_id.store(u64::from_be_bytes(bytes[0..8].try_into().unwrap()), Ordering::SeqCst);
        cat.epoch.store(u64::from_be_bytes(bytes[8..16].try_into().unwrap()), Ordering::SeqCst);
        Ok(cat)
    }

    /// The pool this catalog is over. For a harness that needs to flush it; the catalog owns its
    /// own pool when opened as a sidecar.
    pub fn pool_handle(&self) -> &Arc<BufferPoolManager> {
        &self.pool
    }

    /// The tree's current root page id. **The caller must persist this**: it moves whenever a root
    /// split happens, and it is the only value `open` cannot find for itself.
    pub fn root_page_id(&self) -> u32 {
        self.tree.root_page_id.load(Ordering::SeqCst)
    }

    fn write_header(&self) -> Result<(), FerroError> {
        let mut v = Vec::with_capacity(HEADER_BYTES);
        v.extend_from_slice(&self.next_id.load(Ordering::SeqCst).to_be_bytes());
        v.extend_from_slice(&self.epoch.load(Ordering::SeqCst).to_be_bytes());
        self.upsert(keys::header(), v)
    }

    /// Replace a key's value, **atomically** — the key is never absent to a concurrent reader.
    ///
    /// `BPlusTreeManager::insert` still does not replace: `insert_entry` always inserts, so
    /// writing an existing key through it a second time leaves TWO entries and `search` returns
    /// whichever the binary search lands on. For a catalog that would mean a branch with two
    /// records that disagree, chosen nondeterministically.
    ///
    /// ⛔ This used to be `tree.delete` then `tree.insert`, with **nothing held across the two**.
    /// `delete` drops the leaf write latch on return and `insert` re-acquires it, so the key did
    /// not exist for the length of a tree descent on **every ordinary rewrite** — and `core`,
    /// `get_raw`, `has_live_children`, `max_live_child` and `live_child_in_epoch_range` all read
    /// it with no lock at all (`logical` is writers-only). D124 is pre-positioned against exactly
    /// that state on the page paths, and its comments described it as impossible; it was not.
    /// [`BPlusTreeManager::upsert`] closes the window at the layer that owns the latch.
    /// SCALE-DESIGN D126; probe in `tests/d126_atomic_upsert.rs` and `mod d126_record_key_probe`.
    fn upsert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), FerroError> {
        self.tree.upsert(key, value)
    }

    fn remove_if_present(&self, key: &Vec<u8>) -> Result<bool, FerroError> {
        match self.tree.delete(key) {
            Ok(()) => Ok(true),
            Err(FerroError::KeyNotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// A record with its unbounded fields filled in from their spans.
    ///
    /// `arenas` is populated because `ArenaPageStore` still mutates it and calls `put`, and a
    /// record that came back empty would delete every arena on the next write. `live_children` is
    /// deliberately left EMPTY — it is unbounded, and every caller was moved onto the indexed
    /// queries first so that nothing reads it.
    fn hydrate(&self, core: CoreRecord) -> Result<BranchRecord, FerroError> {
        let id = core.branch_id().id;
        let mut arenas = Vec::new();
        let (lo, hi) = keys::arenas_of(id);
        for entry in self.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi))? {
            let (k, _) = entry?;
            if k.len() == 13 {
                arenas.push(ArenaId(u32::from_be_bytes(k[9..13].try_into().unwrap())));
            }
        }
        // The only call to `into_hydrated` in the codebase, and it is handed BOTH missing fields.
        Ok(core.into_hydrated(arenas, self.envelope_bytes(id)?))
    }

    fn envelope_bytes(&self, id: u64) -> Result<Option<CapabilityEnvelope>, FerroError> {
        match self.tree.search(&keys::envelope(id))? {
            Some(b) => Ok(Some(CapabilityEnvelope::deserialize(&b).map_err(FerroError::from)?)),
            None => Ok(None),
        }
    }

    fn core(&self, id: u64) -> Result<Option<CoreRecord>, FerroError> {
        match self.tree.search(&keys::record(id))? {
            Some(b) => Ok(Some(BranchRecord::deserialize_core(&b)?)),
            None => Ok(None),
        }
    }

    /// Write a record's core plus the index entries derived from it, removing the entries derived
    /// from `old` first. The CHILD span is **not** touched here: children are inserted by `fork`
    /// and removed by `detach_child`, because `put` receives records whose `live_children` is
    /// empty by construction and diffing against that would delete every child.
    /// `write_record` for a branch whose keys are **provably new**, which on the fork path means a
    /// freshly minted id.
    ///
    /// ⛔ **D126 RETIRED MOST OF THIS METHOD'S ORIGINAL JUSTIFICATION. Stated rather than left to
    /// be discovered, because the retired half is the half that carried a number.**
    ///
    /// The argument was: `upsert` is delete-then-insert, so for a key that cannot exist the delete
    /// is a guaranteed miss — a full B+tree descent whose only possible outcome is `KeyNotFound`.
    /// Measured on the child key, same key space and same run, `upsert` 0.02280 ms against
    /// `tree.insert` 0.01003 ms, so the wasted delete was 56% of an upsert
    /// (`bench/serial_section_profile.txt`). **`upsert` is no longer delete-then-insert.** It is
    /// one descent and one page write, the same shape as `insert`, and the delete it used to pay
    /// for does not happen: `bench/d126_upsert_cost.txt` measures the replacement at 0.52-0.54x
    /// the pair, which is that wasted descent going away.
    ///
    /// ⇒ What is left of the justification is the part that never depended on the delete: this
    /// skips the arena-span `range_scan` that `write_record` performs to reconcile arenas, which
    /// for a brand-new child can only ever find an empty span. **That saving is UNMEASURED** — the
    /// only number this method ever had was the one D126 removed — so anyone pricing `fork`'s
    /// serial section should re-take it rather than reusing 56% of anything.
    ///
    /// SAFETY OF "PROVABLY NEW", checked rather than assumed: no path anywhere deletes a `RECORD`
    /// key -- the only operations on `keys::record` are insert, search and upsert -- so a reaped
    /// branch's record SURVIVES with `state = Reaped`, and a **recycled** id's keys DO exist. This
    /// must therefore be called only for an id taken from `next_id`, never one taken from
    /// `FREE_ID`. `fork` knows which it chose because it chose.
    ///
    /// The precedent is eight lines away: `fork` already calls `self.tree.insert` directly for the
    /// child key, on exactly this reasoning. This extends it to the four keys that are new for the
    /// identical reason.
    fn write_record_new(&self, rec: &BranchRecord) -> Result<(), FerroError> {
        // A real assert, not debug_assert. A record carrying arenas would have them silently
        // dropped here, and "silently dropped arenas" is precisely the defect that leaked pages
        // permanently once already (6e28372) while the obvious assertion passed. One `is_empty()`
        // is nanoseconds; a repeat of that bug is not.
        assert!(
            rec.arenas.is_empty(),
            "write_record_new was handed a record with {} arenas; it does not reconcile the arena \
             span, so they would be silently dropped. Use write_record.",
            rec.arenas.len()
        );
        self.tree.insert(keys::record(rec.branch_id.id), rec.serialize_core())?;
        self.tree.insert(keys::state(rec.state.as_u8(), rec.branch_id.id), Vec::new())?;
        if Self::in_deadline_index(rec.state, rec.branch_id) {
            self.tree.insert(keys::deadline(rec.lease_deadline.0, rec.branch_id.id), Vec::new())?;
        }
        if let Some(e) = &rec.envelope {
            self.tree.insert(keys::envelope(rec.branch_id.id), e.serialize())?;
        }
        Ok(())
    }

    /// `rec` must be WHOLE -- its arena span is rewritten to match it. `old` is deliberately a
    /// [`CoreRecord`]: the only things ever read from it are the state and deadline index keys, and
    /// typing it that way means a caller can pass the cheap read it already has without the
    /// signature implying the expensive one would be safer.
    ///
    /// ⚠ **D126 made the RECORD key's rewrite atomic. It did NOT make this method atomic, and the
    /// difference is worth stating so the guarantee is not over-read.** The state and deadline
    /// index keys are not rewritten in place, they MOVE: `remove_if_present(old)` below, then
    /// `upsert(new)` a few lines later. Between those two a branch is in NEITHER state span and
    /// neither deadline span, and `live_count`, `in_state` and `expired_before` scan exactly those
    /// spans without the `logical` lock. No per-key primitive can close that window — the two keys
    /// are different keys, in general on different pages — so it needs multi-key exclusion or an
    /// ordering argument, and it is a separate row. The direction it fails in is benign for the
    /// reaper (`expired_before` missing an entry means "not expired yet", and
    /// `reap_if_still_expired` re-reads the record anyway), which is why it is noted here rather
    /// than treated as the same defect.
    fn write_record(
        &self,
        rec: &BranchRecord,
        old: Option<&CoreRecord>,
    ) -> Result<(), FerroError> {
        if let Some(prev) = old {
            self.remove_if_present(&keys::state(prev.state().as_u8(), prev.branch_id().id))?;
            if Self::in_deadline_index(prev.state(), prev.branch_id()) {
                self.remove_if_present(&keys::deadline(
                    prev.lease_deadline().0,
                    prev.branch_id().id,
                ))?;
            }
        }
        self.upsert(keys::record(rec.branch_id.id), rec.serialize_core())?;
        self.upsert(keys::state(rec.state.as_u8(), rec.branch_id.id), Vec::new())?;
        if Self::in_deadline_index(rec.state, rec.branch_id) {
            self.upsert(keys::deadline(rec.lease_deadline.0, rec.branch_id.id), Vec::new())?;
        }
        match &rec.envelope {
            Some(e) => self.upsert(keys::envelope(rec.branch_id.id), e.serialize())?,
            None => {
                self.remove_if_present(&keys::envelope(rec.branch_id.id))?;
            }
        }
        // Arenas: the record is the authority, so the span is made to match it.
        let (lo, hi) = keys::arenas_of(rec.branch_id.id);
        let existing: Vec<Vec<u8>> = self
            .tree
            .range_scan(Bound::Included(lo), Bound::Excluded(hi))?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for k in existing {
            if k.len() == 13 {
                let a = ArenaId(u32::from_be_bytes(k[9..13].try_into().unwrap()));
                if !rec.arenas.contains(&a) {
                    self.remove_if_present(&k)?;
                }
            }
        }
        for a in &rec.arenas {
            self.upsert(keys::arena(rec.branch_id.id, a.0), Vec::new())?;
        }
        Ok(())
    }

    /// Resolve one CHILD entry to the fork epoch of a child that is **still live**, or `None`.
    ///
    /// **A CHILD entry is a hint, not an answer.** `reap` marks a child `Reaped` and then removes
    /// its entry, and a crash between those leaves the entry behind. `LogBranchCatalog` never had
    /// this problem because it DERIVES the live set from each child's own record at replay; an
    /// index cannot derive, so the child's record is consulted here instead. That keeps the
    /// authority in the same place the log catalog kept it — *a live child's own record says so* —
    /// while the lookup stays a descent rather than a scan.
    ///
    /// The same pattern, with the same reasoning, is already in this repo:
    /// `index_fulltext::postings_for_token` returns "candidates, not answers" and makes the caller
    /// re-check the row. It is also how a Postgres index scan treats a dead tuple.
    ///
    /// A stale entry is harmless BECAUSE of the ordering in `reap`: the entry can only outlive a
    /// child that is already `Reaped`, never precede one that is still live. Reversing those two
    /// writes would make a MISSING entry possible for a live child, which is the unsafe direction —
    /// the parent would look childless and its pages would be freed underneath a branch that can
    /// still read them.
    /// What a child entry means for its parent's liveness, WITHOUT deciding it recursively.
    ///
    /// **D60 split this out of `live_child_at`.** The recursion used to live in the arm below:
    /// a reaped child asked `has_live_children(child)`, so the call depth was the length of a
    /// reaped chain. This returns the reaped child's id instead and lets the caller's explicit
    /// stack explore it, which is the same answer with no stack growth. `live_child_at` is kept
    /// as the one-entry answer its other callers want, now written in terms of this.
    /// A CHILD entry whose value names a branch with **no record**. Refused, never resolved.
    ///
    /// ⛔ **A missing record cannot mean "the child is gone."** Nothing ever leaves a record
    /// deleted: retirement is a state FLIP to `Reaped`, the id-reuse path OVERWRITES the recycled
    /// slot, and `keys::record` is only ever inserted, searched and upserted — the same fact
    /// `write_record_new`'s "provably new" argument rests on.
    ///
    /// # What D126 changed here, and what it did not
    ///
    /// An earlier version of this comment called the state impossible. It was not, and the reason
    /// was `upsert`: with no replace primitive on the tree it was DELETE-THEN-INSERT, `delete`
    /// dropped its leaf write latch on return and `insert` re-acquired, and `write_record` routed
    /// the RECORD key through the pair. Every `set_root`, `renew_lease`, `set_state`, `reparent`,
    /// `restrict_envelope` and `put` — two of them hot-path writes — left the key briefly ABSENT,
    /// and every reader here is lockless because `logical` is writers-only. **A reader could see
    /// "no record" for a perfectly healthy, live branch.**
    ///
    /// **D126 closed that window** ([`crate::storage::index::BPlusTreeManager::upsert`]). Measured
    /// on the RECORD key itself in `mod d126_record_key_probe`: the delete-then-insert control
    /// misses 18,515 times in 156,409 lockless reads, the same rewrite through `upsert` misses 0
    /// in 140,113, and a 300-iteration `set_root` loop misses 0 in 9,401,816.
    ///
    /// ⇒ This arm is no longer reachable from an ordinary write. It is still refused, for a
    /// different reason than before: what remains is a branch that was never published — which
    /// `attach_child` and `add_arena` refuse to create, so it means an older database or a bug —
    /// or genuine corruption. Freeing is irreversible and refusing is retryable, so guessing the
    /// destructive direction is wrong whichever of the two it is.
    ///
    /// ⚠ **That makes this `Corrupt` a REAL signal rather than an expected artefact of a hot-path
    /// write — which makes D127, the reaper swallowing it, matter more rather than less.**
    ///
    /// **D124 — both resolvers used to answer "not a pin" here, which is the DESTRUCTIVE
    /// direction.** The parent then reads as childless, `reap_expired` frees its pages, and a
    /// branch that can still reach them through the root it inherited loses them: silent data
    /// loss, not a leak. The `value.len() != 8` arm above already takes the parking (safe)
    /// direction for an entry that cannot be verified at all. There is no parking direction left
    /// for an entry that CAN be verified and fails — "assume live" would pin the parent's pages
    /// for ever on a genuinely corrupt entry — so this refuses and names the entry instead, which
    /// is the one answer that neither frees nor leaks.
    fn dangling_child(key: &[u8], child_id: u64) -> FerroError {
        let which = match (keys::child_parent_from_key(key), keys::child_epoch_from_key(key)) {
            (Some(p), Some(e)) => format!("parent {p}, fork epoch {e}"),
            _ => format!("malformed CHILD key {key:02x?}"),
        };
        BranchError::Corrupt(format!(
            "CHILD entry ({which}) names branch {child_id}, which has no record. Nothing ever \
             deletes a record — retirement is a state flip to Reaped — and since D126 there is \
             no rewrite window either: upsert replaces a value under one hold of the leaf latch, \
             so an ordinary set_root/renew_lease/set_state no longer makes the key momentarily \
             absent. What is left is a branch that was never published, or a corrupt entry. This \
             refuses instead of resolving it to \"not a live child\", which would let the \
             parent's pages be freed underneath a branch that may still be reading them. \
             Refusing leaks at worst and is retryable; see D124/D126."
        ))
        .into()
    }

    fn child_liveness(&self, key: &[u8], value: &[u8]) -> Result<ChildLiveness, FerroError> {
        if value.len() != 8 {
            // An entry written before the value carried an id. There is no id to resolve, so it
            // cannot be verified; treat it as LIVE, which is the parking (safe) direction.
            return Ok(ChildLiveness::Live);
        }
        let child_id = u64::from_be_bytes(value[0..8].try_into().unwrap());
        Ok(match self.core(child_id)? {
            Some(rec) if rec.state() != BranchState::Reaped => ChildLiveness::Live,
            // **D16 — a reaped branch that still has live children is a PIN, not a stale hint.**
            // Its subtree is explored by the caller (see `has_live_children`), not from here.
            Some(_) => ChildLiveness::ReapedWithSubtree(child_id),
            // **D124.** This used to be `ChildLiveness::Gone`, which `has_live_children` then
            // skipped entirely — so the entry pinned nothing and the parent became reclaimable.
            // `dangling_child` has the reason. It used to be "a concurrent `set_root` on this
            // very child removes its RECORD key for the length of an upsert"; D126 closed that
            // window, and the arm stays because what is left — never published, or corrupt — is
            // still not something to resolve in the direction that FREES pages.
            None => return Err(Self::dangling_child(key, child_id)),
        })
    }

    fn live_child_at(&self, key: &[u8], value: &[u8]) -> Result<Option<Epoch>, FerroError> {
        if value.len() != 8 {
            // An entry written before the value carried an id. There is no id to resolve, so it
            // cannot be verified; treat it as LIVE, which is the parking (safe) direction.
            return Ok(keys::child_epoch_from_key(key).map(Epoch));
        }
        let child_id = u64::from_be_bytes(value[0..8].try_into().unwrap());
        match self.core(child_id)? {
            Some(rec) if rec.state() != BranchState::Reaped => {
                Ok(keys::child_epoch_from_key(key).map(Epoch))
            }
            // **D16 — a reaped branch that still has live children is a PIN, not a stale hint.**
            //
            // Without this, pruning an INTERIOR branch loses its whole subtree: the grandparent
            // consults only its DIRECT children, the reaped interior node resolves to "not live",
            // and the grandparent reads as childless while a live grandchild still reaches its
            // pages through the root it inherited. Reproduced in
            // `tests/s18_transitive_visibility.rs`; MCTS prunes interior nodes, so this is the
            // workload BranchBench names, not a corner case.
            //
            // The epoch reported is THIS entry's -- the reaped node's own fork epoch -- and that
            // is the semantically correct one: a grandchild's root is the interior node's root at
            // fork time, which is the grandparent's root at *that* epoch.
            //
            // ⛔ **COST, STATED CORRECTLY — an earlier version of this comment said "O(1) in N"
            // and that was WRONG.** Recursion depth WAS bounded by the cap D60 removed, but the
            // WORK is not: `has_live_children` scans a node's whole CHILD span and recurses into
            // every REAPED child, so this explores the reaped subtree breadth-first with an early
            // exit on the first live descendant. A parent with 10^6 reaped children and one live
            // child at the end of the span scans all 10^6.
            //
            // It is therefore O(explored reaped subtree), cheap in the common case (few reaped
            // children, early exit) and NOT bounded by 8. It is still not the global reachability
            // walk `mod.rs:13` forbids -- it never leaves this branch's own subtree -- but the
            // honest bound is the subtree, not a constant.
            // The subtree walk is `has_live_children`'s now (iterative, D60), and this arm asks it
            // for the one entry it was handed — so a single-entry answer keeps the D16 rule while
            // the depth-unbounded exploration happens on a heap stack, not the call stack.
            Some(_) if BranchCatalog::has_live_children(self, child_id)? => {
                Ok(keys::child_epoch_from_key(key).map(Epoch))
            }
            // Reaped with nothing under it: a stale hint, and LEGITIMATELY not a live child. This
            // is the state `reap` leaves behind when it marks a child `Reaped` and crashes before
            // removing the entry, and it must keep answering exactly this.
            Some(_) => Ok(None),
            // **D124 — split out of the catch-all these two arms used to share.** That arm's own
            // comment admitted it lumped "reaped with nothing under it" (legitimate, above)
            // together with "gone entirely", and answered the destructive way for both. "Gone
            // entirely" is not impossible, just unresolvable: see `dangling_child`.
            None => Err(Self::dangling_child(key, child_id)),
        }
    }

    /// Only `Live`, non-trunk branches are in the deadline index.
    ///
    /// A quarantined branch keeps its record for as long as an operator wants to look at it and its
    /// lease expired long ago; left in the index it would sit at the head of the range for ever and
    /// every 30-second reap scan would step over it before reaching anything real. That is an
    /// unbounded walk reintroduced into the hot path through the back door.
    /// Takes the two fields it reads rather than a record, so it serves a `CoreRecord` and a whole
    /// `BranchRecord` alike without either having to be converted to satisfy it.
    fn in_deadline_index(state: BranchState, branch_id: BranchId) -> bool {
        state == BranchState::Live && !branch_id.is_trunk()
    }

    /// Ids of every entry in a span whose key ends with an 8-byte branch id.
    fn ids_in_span(&self, lo: Vec<u8>, hi: Vec<u8>) -> Result<Vec<u64>, FerroError> {
        let mut out = Vec::new();
        for entry in self.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi))? {
            let (k, _) = entry?;
            let n = k.len();
            if n >= 8 {
                out.push(u64::from_be_bytes(k[n - 8..].try_into().unwrap()));
            }
        }
        Ok(out)
    }

    /// Live branches, for parity with `LogBranchCatalog::live_count`.
    pub fn live_count(&self) -> Result<usize, FerroError> {
        let (lo, hi) = keys::whole_state(BranchState::Live.as_u8());
        Ok(self.ids_in_span(lo, hi)?.len())
    }

    /// Durably replace a whole record. **Test-only — D41 removed this from `BranchCatalog`.**
    ///
    /// No engine path calls it; the narrow operations do. It exists because the format, index and
    /// replay tests in this file have to write records the engine would never produce on purpose
    /// — the same record twice to prove `insert` does not leave two of it, a `Reaped` state with
    /// its CHILD entry deliberately left behind to prove readers resolve the entry rather than
    /// trust it, an arena appended the way the page store used to do it.
    ///
    /// `#[cfg(test)]` rather than private, because private is not enough: `tests/` compiles
    /// against the library without `cfg(test)`, so this gate is what stops an integration test
    /// from reaching for a whole-record write instead of naming the field it means.
    #[cfg(test)]
    fn put(&self, record: &BranchRecord) -> Result<(), FerroError> {
        let _g = self.logical.lock().unwrap();
        let old = self.core(record.branch_id.id)?;
        self.write_record(record, old.as_ref())?;
        // Lock dropped BEFORE the fsync, so this joins the commit group rather than
        // holding every other writer out for a disk round-trip. See `group_commit`.
        let seq = self.stage()?;
        drop(_g);
        self.durable(seq)
    }
}

impl BranchCatalog for TableBranchCatalog {
    /// POST-increment, matching `LogBranchCatalog::next_epoch` exactly.
    ///
    /// This returned the PRE-increment value until the migration work forced the question. The
    /// sequence of epochs handed out was identical either way, which is why every test passed -
    /// but `current_epoch()` meant "the NEXT epoch to issue" here and "the LAST epoch issued"
    /// there. Two implementations of one trait method meaning different things, invisible because
    /// the only assertion on it was an inequality that holds under both.
    ///
    /// It would have surfaced in the worst possible place: a migration whose job is to carry the
    /// counter across. Picking the wrong reading either reuses an epoch - which for this catalog is
    /// a duplicate CHILD **key**, not merely a duplicate array entry - or silently skips one.
    fn next_epoch(&self) -> Epoch {
        Epoch(self.epoch.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn current_epoch(&self) -> Epoch {
        Epoch(self.epoch.load(Ordering::SeqCst))
    }

    fn fork(&self, parent: BranchId, lease: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        // The durable spelling, defined in terms of the staged one so there is exactly one fork
        // body. A caller that holds no wider lock should use this and not think about tickets.
        let (record, seq) = self.fork_staged(parent, lease)?;
        self.await_fork_durable(seq)?;
        Ok(record)
    }

    /// Wait for the shared sync covering `seq`. See [`BranchCatalog::fork_staged`] for why this is
    /// a separate call: it must be reachable *after* the caller has released its own lock.
    fn await_fork_durable(&self, seq: Option<u64>) -> Result<(), FerroError> {
        match seq {
            Some(seq) => self.durable(seq),
            None => Ok(()),
        }
    }

    fn fork_staged(
        &self,
        parent: BranchId,
        lease: LeaseDeadline,
    ) -> Result<(BranchRecord, Option<u64>), FerroError> {
        let fork_epoch = self.next_epoch();
        // The lock covers every TREE MUTATION and nothing else. It is dropped before the fsync, so
        // concurrent forkers share one disk round-trip instead of queueing for private ones. See
        // `group_commit` for why the ticket is taken last.
        let (child, seq) = {
            let _g = self.logical.lock().unwrap();

        // HYDRATED, and this is a security property, not an optimisation. `fork_child` does
        // `parent.envelope.as_ref().map(CapabilityEnvelope::inherited)`, so a parent read WITHOUT
        // its envelope hands the child `None` - which is the UNGOVERNED default. The child of a
        // governed branch would then be free to write anything: a capability escape.
        //
        // `core()` deliberately leaves the envelope empty because it lives in its own key span.
        // That is exactly why reading a parent through it here was wrong.
        let parent_core = self.core(parent.id)?.ok_or(BranchError::NotFound(parent))?;
        parent_core.check_readable(parent)?;
        // ONE POINT LOOKUP, not `hydrate`. `hydrate` also range-scans the parent's whole arena
        // span, and `fork_child` never reads `arenas` -- measured at ~42ns per arena of the parent,
        // x0.72 throughput at 2000 (`bench/fork_parent_arena_scan.txt`). The envelope is still
        // loaded, and that is not optional: a parent read without it hands the child `None`, which
        // is the UNGOVERNED default and was a shipped capability escape (339e405).
        let parent_envelope = self.envelope_bytes(parent.id)?;

        // Recycle a retired slot if one is free, otherwise mint a new one. Either way the
        // generation comes from the slot's history, never from zero — a reused id whose generation
        // restarted would make a stale handle look current.
        let (lo, hi) = keys::whole_group(keys::tag::FREE_ID);
        let recycled = self
            .tree
            .range_scan(Bound::Included(lo), Bound::Excluded(hi))?
            .next()
            .transpose()?
            .and_then(|(k, _)| keys::free_id_from_key(&k));

        // `reused` decides which writer runs below, and it is the whole safety condition for
        // `write_record_new`: a recycled slot still holds the reaped branch's record, state and
        // deadline keys, so its keys are NOT new.
        let (child_num, generation, reused) = match recycled {
            Some(id) => {
                self.remove_if_present(&keys::free_id(id))?;
                let slot_gen = self.core(id)?.map(|r| r.generation()).unwrap_or(0);
                (id, slot_gen, true)
            }
            None => (self.next_id.fetch_add(1, Ordering::SeqCst), 0, false),
        };
        let child_id = BranchId::new(child_num, generation);
            let child = BranchRecord::fork_child_from_core(
                &parent_core,
                parent_envelope.as_ref(),
                child_id,
                fork_epoch,
                lease,
            )?;

            if reused {
                self.write_record(&child, None)?;
            } else {
                self.write_record_new(&child)?;
            }
            // The child's entry in its parent's live set. A child that exists but is not listed in its
        // parent is a GC correctness hole, which is why both happen under one logical lock.
        // The VALUE is the child's branch id, so a reader can resolve the child and check
        // whether it is still live. See `live_child_at` for why the entry is only a hint.
            self.tree
                .insert(keys::child(parent.id, fork_epoch.0), child_num.to_be_bytes().to_vec())?;
            self.write_header()?;
            // Ticket LAST: every mutation above is now in the pool, so an fsync issued after this
            // point necessarily covers this fork.
            (child, self.stage()?)
        };
        // ⛔ NOT durable yet. The sync that covers `seq` is the caller's to await, and the whole
        // point of handing it back rather than doing it here is that the caller may be holding a
        // lock WIDER than `logical` -- over pgwire it holds `ServerContext::catalog()` for the
        // whole statement. Syncing under that lock is what made `f/sync` exactly 1.00 at every
        // thread count (`bench/d130_batch_vs_threads.txt`) while this catalog's own group commit
        // batched x17 one layer down: the forkers never met inside `wait_durable` because they
        // were still serialised in front of it. See `BranchCatalog::fork_staged`.
        Ok((child, Some(seq)))
    }

    fn get(&self, branch: BranchId) -> Result<BranchRecord, FerroError> {
        let rec = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        self.hydrate(rec)
    }

    fn reparent(
        &self,
        branch: BranchId,
        parent: BranchId,
        fork_epoch: Epoch,
        root: PageId,
    ) -> Result<BranchRecord, FerroError> {
        // **D41.** Under `logical`, the lock every mutator here already takes, so no new lock and
        // no new lock-order edge. The whole read-modify-write is inside it: what this writes back
        // is a record read microseconds ago under the same lock, not the snapshot its caller took
        // before copying up to 256 MiB of pages.
        let _g = self.logical.lock().unwrap();
        let core = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        core.check_readable(branch)?;
        let depth = self
            .core(parent.id)?
            .ok_or(BranchError::NotFound(parent))?
            .depth()
            .saturating_add(1);
        let old = core.clone();
        // HYDRATED, for `set_root`'s reason: `write_record` makes the arena span match the record
        // it is given, so writing back a core record would delete every extent the branch owns —
        // including any a concurrent writer claimed, which is the leak D13b's re-read existed to
        // prevent and this method inherits the duty of. (D13b's caller was `collapse`, deleted by
        // D63; the duty is a property of this write, not of that caller.)
        let mut rec = self.hydrate(core)?;
        rec.parent_id = Some(parent);
        rec.fork_epoch = fork_epoch;
        rec.depth = depth;
        rec.root_page_id = root;
        self.write_record(&rec, Some(&old))?;
        // Lock dropped BEFORE the fsync, so this joins the commit group rather than
        // holding every other writer out for a disk round-trip. See `group_commit`.
        let seq = self.stage()?;
        drop(_g);
        self.durable(seq)?;
        Ok(rec)
    }

    fn restrict_envelope(
        &self,
        branch: BranchId,
        envelope: CapabilityEnvelope,
    ) -> Result<(), FerroError> {
        // **D41.** One key, and no record read at all beyond the readability check: the envelope
        // lives in its own span, which is why `charge_row_writes` can already spend against it
        // without touching the record. The narrowing is compared against what is IN FORCE under
        // the lock, not against a snapshot the caller read — two restrictions racing now leave the
        // narrower one standing instead of whichever wrote last.
        let _g = self.logical.lock().unwrap();
        let core = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        core.check_readable(branch)?;
        if let Some(current) = self.envelope_bytes(branch.id)? {
            current.permits(&envelope)?;
        }
        self.upsert(keys::envelope(branch.id), envelope.serialize())?;
        // Lock dropped BEFORE the fsync, so this joins the commit group rather than
        // holding every other writer out for a disk round-trip. See `group_commit`.
        let seq = self.stage()?;
        drop(_g);
        self.durable(seq)
    }

    fn set_state(
        &self,
        branch: BranchId,
        expect: BranchState,
        to: BranchState,
    ) -> Result<(), FerroError> {
        let _g = self.logical.lock().unwrap();
        let core = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        // Generation-checked, not `check_readable`-checked: the transition OUT of `Reaping` is the
        // second half of every reap, and `check_readable` refuses `Reaping` outright.
        if core.generation() != branch.generation {
            return Err(BranchError::Reaped {
                requested: branch,
                current_generation: core.generation(),
            }
            .into());
        }
        if core.state() != expect {
            return Err(BranchError::UnexpectedState {
                branch,
                expected: expect,
                actual: core.state(),
            }
            .into());
        }
        if expect == to {
            // Nothing to write, and it keeps a second `set_state(.., Reaped)` from bumping the
            // generation twice.
            return Ok(());
        }
        let old = core.clone();
        // HYDRATED: `write_record` rewrites the arena span from the record it is handed, so a core
        // record would silently drop every extent this branch owns.
        let mut rec = self.hydrate(core)?;
        if to == BranchState::Reaped {
            // Generation bumped, arenas cleared. The cleared list is load-bearing here rather than
            // cosmetic: `write_record` reconciles the arena span against the record, so this is
            // what removes the ARENA keys of a branch whose extents the reaper has just returned.
            rec.mark_reaped();
        } else {
            rec.state = to;
        }
        self.write_record(&rec, Some(&old))?;
        // Lock dropped BEFORE the fsync, so this joins the commit group rather than
        // holding every other writer out for a disk round-trip. See `group_commit`.
        let seq = self.stage()?;
        drop(_g);
        self.durable(seq)
    }

    fn set_root(&self, branch: BranchId, root: PageId) -> Result<(), FerroError> {
        let _g = self.logical.lock().unwrap();
        // HYDRATED, not core. `core` returns a record whose `arenas` is empty, and `write_record`
        // makes the arena span match the record it is given - so writing back a core record
        // DELETES every arena the branch owns. The page store records an arena by appending to
        // this field and calling `put`; a `set_root` afterwards then silently threw it away, and
        // the reaper frees exactly `record.arenas`, so nothing was ever returned.
        //
        // `LogBranchCatalog::set_root` reads through `get`, which returns a whole record. Two
        // implementations of one trait method must do the same thing.
        // The core is read FIRST and kept as `old`: `write_record` only ever consults an old
        // record for its state and deadline index keys, so cloning the hydrated record -- arenas
        // and all -- to hand it over was copying a vector nobody read.
        let core = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        core.check_readable(branch)?;
        let old = core.clone();
        let mut rec = self.hydrate(core)?;
        rec.root_page_id = root;
        self.write_record(&rec, Some(&old))?;
        // Lock dropped BEFORE the fsync, so this joins the commit group rather than
        // holding every other writer out for a disk round-trip. See `group_commit`.
        let seq = self.stage()?;
        drop(_g);
        self.durable(seq)
    }

    fn expired_before(&self, now_millis: u64) -> Result<Vec<CoreRecord>, FerroError> {
        let (lo, hi) = keys::expired_at_or_before(now_millis);
        let mut out = Vec::new();
        for id in self.ids_in_span(lo, hi)? {
            if let Some(rec) = self.core(id)? {
                // The index holds only Live non-trunk branches, so this re-check is belt and
                // braces against an index entry that outlived its record rather than a filter the
                // query depends on.
                if Self::in_deadline_index(rec.state(), rec.branch_id())
                    && rec.lease_deadline().is_expired_at(now_millis)
                {
                    // NOT hydrated. The reaper reads branch_id, depth and fork_epoch -- all core.
                    // Hydrating here cost an arena range-scan plus an envelope lookup PER ANSWER
                    // ROW, both discarded: 24.3 us/row measured, against ~5 us for this descent.
                    out.push(rec);
                }
            }
        }
        out.sort_unstable_by_key(|r| r.branch_id().id);
        Ok(out)
    }

    fn in_state(&self, state: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
        let (lo, hi) = keys::whole_state(state.as_u8());
        let mut out = Vec::new();
        for id in self.ids_in_span(lo, hi)? {
            if let Some(rec) = self.core(id)? {
                out.push(self.hydrate(rec)?);
            }
        }
        out.sort_unstable_by_key(|r| r.branch_id.id);
        Ok(out)
    }

    fn scan(&self)
        -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        let (lo, hi) = keys::whole_group(keys::tag::RECORD);
        let it = self.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi))?;
        // Streaming: the scanner walks leaf by leaf, so a full system view or snapshot holds one
        // record at a time rather than a second copy of the catalog.
        //
        // HYDRATED, not core. `LogBranchCatalog::scan` returns whole records, and two
        // implementations of one trait method must mean the same thing by it - the third time that
        // has bitten in this series. It is not cosmetic here:
        //   - `consensus::snapshot::branch_image` SERIALIZES these records, so a core-only scan
        //     would ship a follower every branch with its arenas dropped;
        //   - the `ferro_branches` system view reports `arenas.len()`, which would read 0 for
        //     every branch.
        // `live_children` stays empty, as it does from `get`: it is unbounded and no caller reads
        // it any more - they use the three indexed queries.
        Ok(Box::new(it.map(move |e| {
            let (_, v) = e?;
            let rec = BranchRecord::deserialize_core(&v).map_err(FerroError::from)?;
            self.hydrate(rec)
        })))
    }

    /// The narrowing form, served by the same descent machinery as [`Self::scan`] — literally the
    /// same `range_scan` with tighter bounds and the same hydration, so the two cannot answer
    /// differently about a record they both reach.
    ///
    /// `RECORD` keys are `[0x00][id big-endian]` and the tree compares keys as byte strings, so a
    /// contiguous id range is a contiguous key range. See `tree_keys`' "why big-endian".
    fn scan_ids(
        &self,
        lo: u64,
        hi: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        if lo > hi {
            // Never handed to the tree. An inverted range is an empty answer here, and asking a
            // range scan for one is asking a question whose handling is a property of the scanner
            // rather than of this range.
            return Ok(Box::new(std::iter::empty()));
        }
        let it = self
            .tree
            .range_scan(Bound::Included(keys::record(lo)), Bound::Included(keys::record(hi)))?;
        Ok(Box::new(it.map(move |e| {
            let (_, v) = e?;
            let rec = BranchRecord::deserialize_core(&v).map_err(FerroError::from)?;
            self.hydrate(rec)
        })))
    }

    fn max_live_child(&self, parent_id: u64) -> Result<Option<Epoch>, FerroError> {
        let (lo, hi) = keys::children_of(parent_id);
        // One descent: the CHILD key stores the complement of the fork epoch, so the newest child
        // is the FIRST key in the span. Ascending would put it last and reading it would mean
        // consuming every child trunk has.
        // Entries are newest-first, so the first LIVE one is the maximum. Stale entries are
        // skipped rather than trusted; they are bounded by crashes and removed by the next reap.
        for entry in self.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi))? {
            let (k, v) = entry?;
            if let Some(e) = self.live_child_at(&k, &v)? {
                return Ok(Some(e));
            }
        }
        Ok(None)
    }

    fn live_child_in_epoch_range(
        &self,
        parent_id: u64,
        lo: Epoch,
        hi: Epoch,
    ) -> Result<bool, FerroError> {
        let Some((klo, khi)) = keys::children_in_epoch_range(parent_id, lo.0, hi.0) else {
            // An empty window pins nothing.
            return Ok(false);
        };
        for entry in self.tree.range_scan(Bound::Included(klo), Bound::Included(khi))? {
            let (k, v) = entry?;
            if self.live_child_at(&k, &v)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Does `parent_id` have a live descendant reachable through reaped interior nodes?
    ///
    /// **Iterative, with an explicit stack — D60.** This was mutually recursive with
    /// `live_child_at`: a reaped child sent it back into `has_live_children`, so its RECURSION
    /// depth was the length of a chain of reaped interior nodes. That was bounded only by
    /// `MAX_BRANCH_DEPTH = 8`, and removing the cap (SCALE-DESIGN D60 — fork and read are flat
    /// across depth 1..250, so the cap protects nothing on either path) makes that chain
    /// unbounded. MCTS prunes interior nodes, which is exactly how a long reaped chain forms, so
    /// the recursion had to go before the cap could.
    ///
    /// The cost is unchanged and is still not O(1): `live_child_at` returns `None` for a reaped
    /// node so that its own children are explored here instead, and the walk is breadth-first over
    /// the reaped subtree with an early exit on the first live descendant.
    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        let mut pending = vec![parent_id];
        while let Some(id) = pending.pop() {
            let (lo, hi) = keys::children_of(id);
            for entry in self.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi))? {
                let (k, v) = entry?;
                match self.child_liveness(&k, &v)? {
                    ChildLiveness::Live => return Ok(true),
                    // A reaped child is not itself a pin, but anything live BELOW it is — D16.
                    // Explored here rather than by recursing, so the stack cannot grow with depth.
                    ChildLiveness::ReapedWithSubtree(child) => pending.push(child),
                }
            }
        }
        Ok(false)
    }

    /// Counts the `Live` state span. Unlike the log catalog's, this is a scan of that span rather
    /// than of every record, so it is proportional to the answer.
    fn live_count(&self) -> usize {
        TableBranchCatalog::live_count(self).unwrap_or(0)
    }

    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        // No `check_readable`: that is the whole point. A branch mid-reap or already reaped still
        // owns the children that decide the fate of its pages.
        let rec = self.core(id)?.ok_or(BranchError::NotFound(BranchId::new(id, 0)))?;
        self.hydrate(rec)
    }

    fn release_id(&self, id: u64) {
        if id == 0 {
            return;
        }
        let _g = self.logical.lock().unwrap();
        // Refuse while the slot still has live children - that set decides the fate of pages
        // parked under this branch's name. Errors are swallowed to match the inherent method's
        // signature on the log catalog, which returns nothing: a failure here leaks an id slot,
        // which is recoverable, while propagating it would abort a reap midway, which is not.
        let reusable = match (self.core(id), self.has_live_children(id)) {
            (Ok(Some(rec)), Ok(false)) => rec.state() == BranchState::Reaped,
            _ => false,
        };
        if reusable {
            let _ = self.upsert(keys::free_id(id), Vec::new());
            // Same as the others: release the lock before the fsync. Errors stay swallowed
            // to match the log catalog's signature -- a failure here leaks an id slot,
            // which is recoverable, while propagating would abort a reap midway.
            if let Ok(seq) = self.stage() {
                drop(_g);
                let _ = self.durable(seq);
            }
        }
    }

    fn attach_child(
        &self,
        parent_id: u64,
        fork_epoch: Epoch,
        child_id: u64,
    ) -> Result<(), FerroError> {
        let _g = self.logical.lock().unwrap();
        // **D124 — refuse to CREATE an entry that can never resolve.** The value written here is
        // a child branch id, and both resolvers (`child_liveness`, `live_child_at`) later ask
        // that child's own record whether the entry is a pin. An entry naming a branch that was
        // never published can never answer, so it is refused at the only place in the API that
        // can create one; `fork` writes the record before the child key under this same lock, so
        // it cannot create one either.
        //
        // ⚠ This removes the PERMANENT form. The transient one — a record missing for the length
        // of an `upsert` on a perfectly healthy branch — was never preventable here, and is now
        // gone at its source: D126 made `upsert` an in-place replace under one latch hold. The
        // resolvers still refuse a dangling entry, for the residual reasons in `dangling_child`.
        //
        // One extra point lookup, and it is free where it matters: `migrate_from` is the only
        // production caller (D63 deleted `collapse`) and it already writes every record before it
        // attaches anything, precisely so this resolution can succeed.
        if self.core(child_id)?.is_none() {
            return Err(BranchError::NotFound(BranchId::new(child_id, 0)).into());
        }
        self.upsert(keys::child(parent_id, fork_epoch.0), child_id.to_be_bytes().to_vec())?;
        // Lock dropped BEFORE the fsync, so this joins the commit group rather than
        // holding every other writer out for a disk round-trip. See `group_commit`.
        let seq = self.stage()?;
        drop(_g);
        self.durable(seq)
    }

    fn detach_child(&self, parent_id: u64, fork_epoch: Epoch) -> Result<bool, FerroError> {
        let _g = self.logical.lock().unwrap();
        let removed = self.remove_if_present(&keys::child(parent_id, fork_epoch.0))?;
        // Lock dropped BEFORE the fsync, so this joins the commit group rather than
        // holding every other writer out for a disk round-trip. See `group_commit`.
        let seq = self.stage()?;
        drop(_g);
        self.durable(seq)?;
        Ok(removed)
    }

    fn add_arena(&self, branch: BranchId, arena: ArenaId) -> Result<(), FerroError> {
        // **D20.** No read-modify-write at all: `hydrate` DERIVES `arenas` from exactly these
        // ARENA keys, so owning an arena is one key. The racy shape this replaces was
        // `get_raw` (unlocked) -> push -> `put` (locked) in `ArenaPageStore::alloc_arena`, where
        // the unlocked read could traverse a tree another thread was splitting and come back with
        // the wrong arena list -- which was then written back as truth.
        //
        // Under `logical` so it cannot interleave with the multi-key writers (`put`, `fork`,
        // `attach_child`), all of which also rewrite this span.
        //
        // **D33.** The first version of this wrote the key and returned. Every other mutating
        // method here ends `let seq = self.stage()?; drop(_g); self.durable(seq)`, and skipping it
        // cost two things, not one. The ARENA key was never fsynced -- and `stage()` is the ONLY
        // caller of `publish_root()` (:331-333), so an `upsert` that happened to split the tree's
        // root left the header page naming the OLD root, after which a reopen came back on a
        // perfectly valid B+tree of an older state. Measured: 0 of 64 arenas survived a reopen.
        //
        // The generation check is the same story: `get_mut`-by-id alone let a STALE handle whose
        // slot had been recycled attach an arena to the slot's new occupant.
        let _g = self.logical.lock().unwrap();
        let core = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        core.check_readable(branch)?;
        self.upsert(keys::arena(branch.id, arena.0), Vec::new())?;
        // Lock dropped BEFORE the fsync, so this joins the commit group rather than
        // holding every other writer out for a disk round-trip. See `group_commit`.
        let seq = self.stage()?;
        drop(_g);
        self.durable(seq)
    }

    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        let _g = self.logical.lock().unwrap();
        // Hydrated for the same reason as `set_root`: a core record has no arenas, and writing it
        // back would delete the branch's.
        let core = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        core.check_readable(branch)?;
        let old = core.clone();
        let mut rec = self.hydrate(core)?;
        rec.lease_deadline = lease;
        self.write_record(&rec, Some(&old))?;
        // Lock dropped BEFORE the fsync, so this joins the commit group rather than
        // holding every other writer out for a disk round-trip. See `group_commit`.
        let seq = self.stage()?;
        drop(_g);
        self.durable(seq)
    }

    fn envelope_of(&self, branch: BranchId) -> Result<Option<CapabilityEnvelope>, FerroError> {
        // A point lookup on its own key, which is the reason the envelope is stored separately:
        // the write funnel asks this on every statement and must not pay for the rest of a record.
        let rec = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        self.envelope_bytes(branch.id)
    }

    fn charge_row_writes(&self, branch: BranchId, n: u64) -> Result<(), FerroError> {
        // Under the logical lock so the read-modify-write cannot lose a concurrent charge, and so
        // it cannot discard a `set_root` or `renew_lease` that landed in the window: only the
        // envelope key is written back, never a whole stale record.
        let _g = self.logical.lock().unwrap();
        let rec = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        let mut env = self.envelope_bytes(branch.id)?.ok_or_else(|| {
            FerroError::Branch(format!(
                "{branch} has no capability envelope; refusing to charge {n} row-writes against a \
                 policy that no longer exists"
            ))
        })?;
        env.charge(n)?;
        self.upsert(keys::envelope(branch.id), env.serialize())?;
        // Lock dropped BEFORE the fsync, so this joins the commit group rather than
        // holding every other writer out for a disk round-trip. See `group_commit`.
        let seq = self.stage()?;
        drop(_g);
        self.durable(seq)
    }
}

#[cfg(test)]
mod d126_record_key_probe {
    //! **D126 at the key that matters: the branch RECORD key must never be transiently absent.**
    //!
    //! `tests/d126_atomic_upsert.rs` proves the tree primitive. This proves the catalog uses it,
    //! on the exact key D124's page guards read, and it lives here rather than in `tests/` for one
    //! reason: the CONTROL has to reproduce the old `upsert` on the real RECORD key, which means
    //! reaching `self.tree` — private, and deliberately so.
    //!
    //! Three arms, in this order, because the later ones are only admissible after the first:
    //!
    //! 1. **CONTROL — `tree.delete` then `tree.insert` on `keys::record(id)`.** Exactly what
    //!    `upsert` was. It MUST make `get_raw` miss, or the probe cannot see the defect and the
    //!    two zeros below are worthless.
    //! 2. **TREATMENT — `cat.upsert(keys::record(id), ..)`.** Same key, same readers, same write
    //!    count, one call in place of two. Must be zero, with at least a comparable number of
    //!    reads behind that zero.
    //! 3. **REALISM — `set_root` in a loop.** The actual hot-path caller, fsync and all. Zero.
    //!
    //! The reader is `get_raw`, which is what `arena::free_page` and
    //! `reaper::drain_pending_seeded` call, and it takes no lock: `logical` is writers-only.
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64};

    const READERS: usize = 3;

    fn fresh(tag: &str) -> (Arc<TableBranchCatalog>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("ferrodb-d126-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{tag}.branchcat"));
        let _ = std::fs::remove_file(&path);
        (Arc::new(TableBranchCatalog::open_sidecar(&path, 1).expect("open")), path)
    }

    #[derive(Debug, Default)]
    struct Arm {
        misses: u64,
        reads: u64,
    }

    /// Spin `READERS` threads on `get_raw(id)` while the caller rewrites that branch's record.
    fn probe<W: FnOnce()>(cat: &Arc<TableBranchCatalog>, id: u64, write: W) -> Arm {
        let stop = Arc::new(AtomicBool::new(false));
        let misses = Arc::new(AtomicU64::new(0));
        let reads = Arc::new(AtomicU64::new(0));
        let mut hs = Vec::new();
        for _ in 0..READERS {
            let (c, stop, misses, reads) =
                (Arc::clone(cat), Arc::clone(&stop), Arc::clone(&misses), Arc::clone(&reads));
            hs.push(std::thread::spawn(move || {
                let (mut m, mut r) = (0u64, 0u64);
                while !stop.load(Ordering::Relaxed) {
                    // `core`, not `get_raw`: `get_raw` hydrates, which adds an arena range_scan
                    // between the record read and the answer and would let a miss be masked by
                    // timing rather than by correctness. The record read is the thing on trial.
                    if c.core(id).expect("core read").is_none() {
                        m += 1;
                    }
                    r += 1;
                }
                misses.fetch_add(m, Ordering::Relaxed);
                reads.fetch_add(r, Ordering::Relaxed);
            }));
        }
        // Let the readers get going before the rewrites start; otherwise a fast writer can finish
        // inside thread spawn and the arm reports a zero it never earned. The `reads > 0` assert
        // at each call site is what actually enforces this.
        std::thread::sleep(std::time::Duration::from_millis(20));
        write();
        stop.store(true, Ordering::Relaxed);
        for h in hs {
            h.join().expect("reader");
        }
        Arm { misses: misses.load(Ordering::Relaxed), reads: reads.load(Ordering::Relaxed) }
    }

    /// Rewrites per arm. Arms 1 and 2 are raw key writes; arm 3 pays an fsync each, so it gets a
    /// smaller count and its own `reads` assertion rather than a shared one.
    const RAW_WRITES: usize = 4_000;
    const SET_ROOT_WRITES: usize = 300;

    #[test]
    fn the_record_key_is_never_absent_while_it_is_rewritten() {
        let lease = LeaseDeadline(u64::MAX);

        // ---- 1. CONTROL: the delete-then-insert `upsert` used to be. It must fire. ----
        let (c1, _p1) = fresh("control");
        let child = c1.fork(BranchId::TRUNK, lease).expect("fork");
        let id = child.branch_id.id;
        let bytes = child.serialize_core();
        let c1w = Arc::clone(&c1);
        let control = probe(&c1, id, move || {
            for _ in 0..RAW_WRITES {
                match c1w.tree.delete(&keys::record(id)) {
                    Ok(()) | Err(FerroError::KeyNotFound) => {}
                    Err(e) => panic!("control delete: {e:?}"),
                }
                c1w.tree.insert(keys::record(id), bytes.clone()).expect("control insert");
            }
        });
        println!("D126 catalog CONTROL  (delete+insert): misses={} reads={}", control.misses, control.reads);

        // ---- 2. TREATMENT: the same rewrite through the catalog's own `upsert`. ----
        let (c2, _p2) = fresh("treatment");
        let child2 = c2.fork(BranchId::TRUNK, lease).expect("fork");
        let id2 = child2.branch_id.id;
        let bytes2 = child2.serialize_core();
        let c2w = Arc::clone(&c2);
        let treatment = probe(&c2, id2, move || {
            for _ in 0..RAW_WRITES {
                c2w.upsert(keys::record(id2), bytes2.clone()).expect("upsert");
            }
        });
        println!("D126 catalog TREATMENT (upsert)      : misses={} reads={}", treatment.misses, treatment.reads);

        // ---- 3. REALISM: the hot-path caller, whole. ----
        let (c3, _p3) = fresh("set_root");
        let child3 = c3.fork(BranchId::TRUNK, lease).expect("fork");
        let bid = child3.branch_id;
        let c3w = Arc::clone(&c3);
        let real = probe(&c3, bid.id, move || {
            for i in 0..SET_ROOT_WRITES {
                c3w.set_root(bid, 900_000 + i as u32).expect("set_root");
            }
        });
        println!("D126 catalog set_root                : misses={} reads={}", real.misses, real.reads);

        // ---- Every arm has REPORTED before any assertion fires. ----
        //
        // Deliberate: the first cut asserted arm by arm, and the before-the-fix run therefore
        // died on arm 2 and never printed arm 3 at all. The number that shows `set_root` -- the
        // hot-path caller, the one that makes this more than a storage-layer curiosity -- was
        // the one the ordering threw away. A probe should not hide its own evidence.
        assert!(control.reads > 0, "the control's readers never ran");
        assert!(
            control.misses > 0,
            "THE PROBE IS NOT DISCRIMINATING. delete-then-insert on the RECORD key leaves it \
             absent between the two calls by construction, and {READERS} readers over \
             {RAW_WRITES} rewrites saw it {} times in {} reads. The zeros below mean nothing \
             until this fires.",
            control.misses,
            control.reads
        );
        assert_eq!(
            treatment.misses, 0,
            "the RECORD key was absent {} times in {} reads while `upsert` rewrote it",
            treatment.misses, treatment.reads
        );
        assert!(
            treatment.reads >= control.reads / 4,
            "the treatment arm's readers did only {} reads against the control's {}; its zero is \
             not comparable to the control's positive count",
            treatment.reads,
            control.reads
        );
        assert!(real.reads > 0, "the set_root arm's readers never ran");
        assert_eq!(
            real.misses, 0,
            "`set_root` un-read a live branch's record {} times in {} reads",
            real.misses, real.reads
        );
        // The rewrites actually landed, so the zero is not the zero of a writer that did nothing.
        assert_eq!(
            c3.get_raw(bid.id).expect("record").root_page_id,
            900_000 + (SET_ROOT_WRITES - 1) as u32,
            "set_root did not write what the arm counted"
        );
    }

    /// The other hot-path rewrite, read through the method D124's page guards actually call —
    /// and with a **NEIGHBOUR record that is never rewritten**, read on the same schedule by the
    /// same threads.
    ///
    /// Two things the arms above do not say, which this one does:
    ///
    /// * `renew_lease` is the second hot-path caller named in SCALE-DESIGN and reaches `upsert`
    ///   through `write_record` exactly as `set_root` does. Covered by argument is not covered.
    /// * The reader here is `get_raw`, which is what `arena::free_page` and
    ///   `reaper::drain_pending_seeded` call. `core` (above) is the tighter instrument; `get_raw`
    ///   is the one whose answer the page paths act on, and it hydrates — so a zero from `core`
    ///   does not by itself say the callers are safe.
    /// * The neighbour is the negative control **inside the treatment arm**. It shares a leaf with
    ///   the target, so every page write the rewriter performs passes straight over it. If the
    ///   lockless reader were unsound in some way that had nothing to do with delete-then-insert —
    ///   a torn snapshot, a descent that loses a page mid-write — the neighbour would miss too.
    ///   It must not, and the target's zero is only worth something alongside it.
    ///
    /// **Fire-checked, and it fires.** With `upsert` put back to `tree.delete` then `tree.insert`
    /// and nothing else changed, this arm reports `target_miss=255 neighbour_miss=0` in 125,170
    /// reads — the target vanishes, the neighbour does not, which is exactly the discrimination
    /// this arm claims. The miss count is a race and varies run to run (214 and 255 on two
    /// consecutive runs); the neighbour's zero did not. Log: `bench/d126_probe_before.txt`.
    #[test]
    fn renew_lease_never_un_reads_the_record_and_the_neighbour_never_moves() {
        let lease = LeaseDeadline(u64::MAX);
        let (cat, _p) = fresh("renew");
        let target = cat.fork(BranchId::TRUNK, lease).expect("fork").branch_id;
        let neighbour = cat.fork(BranchId::TRUNK, lease).expect("fork").branch_id;
        cat.get_raw(target.id).expect("target readable before the probe");
        cat.get_raw(neighbour.id).expect("neighbour readable before the probe");

        let stop = Arc::new(AtomicBool::new(false));
        let target_miss = Arc::new(AtomicU64::new(0));
        let neighbour_miss = Arc::new(AtomicU64::new(0));
        let reads = Arc::new(AtomicU64::new(0));
        let mut hs = Vec::new();
        for _ in 0..READERS {
            let (c, stop, tm, nm, reads) = (
                Arc::clone(&cat),
                Arc::clone(&stop),
                Arc::clone(&target_miss),
                Arc::clone(&neighbour_miss),
                Arc::clone(&reads),
            );
            hs.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if c.get_raw(target.id).is_err() {
                        tm.fetch_add(1, Ordering::Relaxed);
                    }
                    if c.get_raw(neighbour.id).is_err() {
                        nm.fetch_add(1, Ordering::Relaxed);
                    }
                    reads.fetch_add(2, Ordering::Relaxed);
                }
            }));
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        for i in 0..SET_ROOT_WRITES {
            cat.renew_lease(target, LeaseDeadline(u64::MAX - i as u64)).expect("renew_lease");
        }
        stop.store(true, Ordering::Relaxed);
        for h in hs {
            h.join().expect("reader");
        }
        let (tm, nm, r) = (
            target_miss.load(Ordering::Relaxed),
            neighbour_miss.load(Ordering::Relaxed),
            reads.load(Ordering::Relaxed),
        );
        println!("D126 catalog renew_lease/get_raw     : target_miss={tm} neighbour_miss={nm} reads={r}");
        assert!(r > 0, "the readers never ran; this arm proves nothing");
        assert_eq!(
            nm, 0,
            "the NEVER-REWRITTEN neighbour record went missing {nm} times in {r} reads. That is \
             not the D126 window — nothing rewrites it — so it is an unsound reader or an \
             unsound page write, and it would invalidate the target's zero."
        );
        assert_eq!(
            tm, 0,
            "`renew_lease` made `get_raw` miss a live branch's record {tm} times in {r} reads"
        );
    }
}

#[cfg(test)]
mod d10_guard {
    //! The assert in `write_record_new` is a guard, so it is forced to fire here.
    //!
    //! `write_record_new` does not reconcile the arena span. Handing it a record that owns arenas
    //! would drop them silently, and the reaper frees precisely `record.arenas` -- so the pages
    //! would leak permanently, which is defect `6e28372` exactly, and that one shipped while the
    //! obvious assertion (`populated.reserved > baseline.reserved`) PASSED. A guard against a
    //! defect that has already happened once is worth a real assert and a test that fires it.
    use super::*;

    fn fresh_catalog() -> (TableBranchCatalog, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("ferrodb-d10-{}-{:?}",
            std::process::id(), std::thread::current().id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("g.branchcat");
        let _ = std::fs::remove_file(&path);
        (TableBranchCatalog::open_sidecar(&path, 1).expect("open"), path)
    }

    #[test]
    #[should_panic(expected = "write_record_new was handed a record with")]
    fn write_record_new_refuses_a_record_carrying_arenas() {
        let (cat, _p) = fresh_catalog();
        let mut rec = cat.get_raw(BranchId::TRUNK.id).expect("trunk");
        rec.arenas.push(ArenaId(3));
        cat.write_record_new(&rec).unwrap();
    }

    /// And it must NOT fire on the shape fork actually produces, or it is a guard that refuses the
    /// only caller it has.
    #[test]
    fn write_record_new_accepts_a_freshly_forked_child() {
        let (cat, _p) = fresh_catalog();
        let child = cat.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).expect("fork");
        assert!(child.arenas.is_empty(), "a fresh child must own no arenas");
        // The record it wrote must be readable back, with its envelope and state intact.
        let back = cat.get_raw(child.branch_id.id).expect("child record");
        assert_eq!(back.branch_id, child.branch_id);
        assert_eq!(back.state, BranchState::Live);
    }
}

#[cfg(test)]
mod serial_section_profile {
    //! Where does the ~0.22 ms a fork holds `logical` actually go? (S8 / D8)
    //!
    //! D8 established that fork throughput is `1 / serial_time` -- the device's flush duration
    //! cancels out, so the only lever is the work done under the lock. Four measurements matched
    //! that model within 0.15%. What nobody had measured is WHICH of the operations in that
    //! section costs anything, and this project has been wrong three times about where a wall was.
    //!
    //! This lives as an #[ignore]d test rather than as production instrumentation or an example,
    //! for two reasons: it needs the PRIVATE methods (`core`, `write_record`, `upsert`, ...), and
    //! a profiler compiled into the write path is overhead in the hot loop it is measuring.
    //!
    //!   cargo test --release serial_section_profile -- --ignored --nocapture
    use super::*;
    use std::time::Instant;

    fn timed<F: FnMut()>(iters: usize, mut f: F) -> f64 {
        let t = Instant::now();
        for _ in 0..iters {
            f();
        }
        t.elapsed().as_secs_f64() * 1000.0 / iters as f64
    }

    #[test]
    #[ignore = "profiling, not a correctness test; run with --ignored --nocapture"]
    fn where_the_serial_section_goes() {
        let dir = std::env::temp_dir().join(format!("ferrodb-s8-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("p.branchcat");
        let _ = std::fs::remove_file(&path);
        let cat = TableBranchCatalog::open_sidecar(&path, 1).expect("open");
        let lease = LeaseDeadline(u64::MAX);

        // A tree with real depth. A profile taken on an empty tree measures the best case of every
        // descent and would understate every one of them.
        const WARM: usize = 20_000;
        for _ in 0..WARM {
            cat.fork(BranchId::TRUNK, lease).expect("warm fork");
        }

        const N: usize = 2_000;
        let trunk_id = BranchId::TRUNK.id;
        let child = cat.get_raw(1).expect("some child");

        let t_core = timed(N, || {
            std::hint::black_box(cat.core(trunk_id).unwrap());
        });
        let t_env = timed(N, || {
            std::hint::black_box(cat.envelope_bytes(trunk_id).unwrap());
        });
        let t_free = timed(N, || {
            let (lo, hi) = keys::whole_group(keys::tag::FREE_ID);
            std::hint::black_box(
                cat.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi)).unwrap().next(),
            );
        });
        let t_header = timed(N, || {
            cat.write_header().unwrap();
        });
        let t_publish = timed(N, || {
            cat.publish_root().unwrap();
        });
        // One upsert on its own, for scale against the 3 that `write_record` performs.
        // ⚠ Since D126 this is an in-place replace, not the delete-then-insert it was when this
        // profile was first taken, so the 0.01344 ms in `bench/serial_section_profile.txt` is a
        // PRE-D126 number. `bench/d126_upsert_cost.txt` prices the change at 0.52-0.54x; re-run
        // this profiler before quoting its upsert row again.
        let t_upsert = timed(N, || {
            cat.upsert(keys::state(child.state.as_u8(), child.branch_id.id), Vec::new()).unwrap();
        });
        let t_write_record = timed(N, || {
            cat.write_record(&child, None).unwrap();
        });
        // ⛔ `tree.insert`, NOT `upsert`. The first version of this profiler measured an upsert
        // here and reported 0.0228 ms / 19.8% for the child key -- but `fork` calls
        // `self.tree.insert` directly for it, with no delete. The row was pricing an operation
        // fork does not perform, and overstated it by a whole wasted descent. Measuring the
        // convenient call instead of the real one is the same error as benchmarking the wrong
        // catalog, which this project already has on record.
        let mut childk = 0u64;
        let t_childkey = timed(N, || {
            childk += 1;
            cat.tree
                .insert(keys::child(trunk_id, 900_000 + childk), 7u64.to_be_bytes().to_vec())
                .unwrap();
        });

        // SAME-INSTRUMENT SUBTRACTION. Comparing the 1-thread per-fork number from one harness
        // against the F_FULLFSYNC probe from another left ~0.014 ms for everything else, which
        // contradicts the 0.115 ms the operations above cost. Two instruments on two files cannot
        // be subtracted. These two are taken in THIS process, on THIS catalog's own file, so they
        // can be.
        let t_fork = timed(200, || {
            std::hint::black_box(cat.fork(BranchId::TRUNK, lease).unwrap());
        });
        // ⛔ THE FLUSH MUST HAVE SOMETHING TO FLUSH. Timing flush_all+sync in a loop with no
        // dirty pages gave 0.010 ms -- macOS short-circuits F_FULLFSYNC when the file has nothing
        // pending -- and subtracting THAT from fork() attributed 3.32 ms to "the serial section",
        // which is absurd on its face and disagreed with the 0.115 ms the operations actually cost.
        // A flush is priced by dirtying a page first and then subtracting the dirtying.
        let t_dirty_and_flush = timed(200, || {
            cat.upsert(keys::child(trunk_id, 888_888), 1u64.to_be_bytes().to_vec()).unwrap();
            cat.pool.flush_all().unwrap();
            cat.pool.disk_manager.sync().unwrap();
        });
        let t_dirty_only = timed(200, || {
            cat.upsert(keys::child(trunk_id, 888_888), 1u64.to_be_bytes().to_vec()).unwrap();
        });
        let t_flush = t_dirty_and_flush - t_dirty_only;

        let sum = t_core + t_env + t_free + t_write_record + t_childkey + t_header + t_publish;
        println!();
        println!("S8: where the serial section goes. {WARM} branches resident, {N} iters each.");
        println!("  operation                         ms/op     share");
        for (name, v) in [
            ("core(parent)            lookup", t_core),
            ("envelope_bytes(parent)  lookup", t_env),
            ("FREE_ID first-key       scan  ", t_free),
            ("write_record (3 upserts)      ", t_write_record),
            ("child-key tree.insert (no del)", t_childkey),
            ("write_header (1 upsert)       ", t_header),
            ("publish_root                  ", t_publish),
        ] {
            println!("  {name}  {v:8.5}  {:6.1}%", 100.0 * v / sum);
        }
        println!("  {:32}  {sum:8.5}", "SUM");
        println!("  {:32}  {t_upsert:8.5}   <- one upsert alone, for scale", "(upsert)");
        println!();
        println!();
        println!("  uncontended fork() total        {t_fork:8.5} ms   (1 thread, includes its flush)");
        println!("  flush_all + sync, page dirtied  {t_flush:8.5} ms   (same process, same file)");
        println!("     (dirty+flush {t_dirty_and_flush:8.5} minus dirty-only {t_dirty_only:8.5})");
        println!("  => serial section by subtraction {:8.5} ms", t_fork - t_flush);
        println!("  => sum of the seven operations   {sum:8.5} ms");
        println!("  If those two disagree, the operations above are NOT what fork actually does,");
        println!("  or the profile perturbs the tree in a way the real path does not.");
        println!();
        println!("Compare with the EFFECTIVE serial time under 64-thread contention: ~0.22 ms (D8).");
        println!("If SUM is far below that, the cost is NOT this work -- it is lock handoff/convoy,");
        println!("and D8's options 1 and 4 are aimed at the wrong thing. That is the single most");
        println!("useful thing this measurement can say, so it is printed either way.");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk_manager::DiskManager;
    use std::fs::OpenOptions;

    fn cat(tag: &str) -> (TableBranchCatalog, std::path::PathBuf, Arc<BufferPoolManager>) {
        let path = std::env::temp_dir()
            .join(format!("ferro-tablecat-{}-{}.db", std::process::id(), tag));
        let _ = std::fs::remove_file(&path);
        let f = OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));
        let c = TableBranchCatalog::create(Arc::clone(&pool), 1).unwrap();
        (c, path, pool)
    }

    #[test]
    fn a_fresh_catalog_holds_trunk_and_nothing_else() {
        let (c, p, _pool) = cat("fresh");
        let trunk = c.get(BranchId::TRUNK).unwrap();
        assert_eq!(trunk.branch_id, BranchId::TRUNK);
        assert_eq!(trunk.root_page_id, 1);
        assert_eq!(c.live_count().unwrap(), 1);
        let all: Vec<u64> =
            c.scan().unwrap().map(|r| r.unwrap().branch_id.id).collect();
        assert_eq!(all, vec![0], "scan must return exactly trunk");
        let _ = std::fs::remove_file(p);
    }

    /// **The duplicate-key hazard.** `BPlusTreeManager::insert` never replaces, so a second write
    /// to one key leaves TWO entries and `search` returns whichever the binary search lands on.
    /// For a catalog that is one branch with two disagreeing records, chosen nondeterministically.
    #[test]
    fn writing_a_record_twice_leaves_exactly_one_of_it() {
        let (c, p, _pool) = cat("dup");
        let child = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();

        let mut rec = c.get(child.branch_id).unwrap();
        rec.root_page_id = 42;
        c.put(&rec).unwrap();
        rec.root_page_id = 99;
        c.put(&rec).unwrap();

        let ids: Vec<u64> = c.scan().unwrap().map(|r| r.unwrap().branch_id.id).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(ids.len(), sorted.len(), "a branch appears twice in the record span: {ids:?}");
        assert_eq!(c.get(child.branch_id).unwrap().root_page_id, 99, "read the stale copy");
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn fork_records_the_child_in_its_parents_live_set() {
        let (c, p, _pool) = cat("fork");
        let a = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        let b = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        assert!(a.fork_epoch < b.fork_epoch, "fixture ordering");

        assert!(c.has_live_children(BranchId::TRUNK.id).unwrap());
        // The LATEST, which is what the privacy barrier depends on, and the reason the key stores
        // a complemented epoch: it is the first entry in the span, one descent away.
        assert_eq!(c.max_live_child(BranchId::TRUNK.id).unwrap(), Some(b.fork_epoch));
        assert_eq!(c.max_live_child(a.branch_id.id).unwrap(), None, "a leaf has no children");

        // The reclamation rule, half-open.
        let t = BranchId::TRUNK.id;
        assert!(c.live_child_in_epoch_range(t, a.fork_epoch, Epoch(a.fork_epoch.0 + 1)).unwrap());
        assert!(!c.live_child_in_epoch_range(t, a.fork_epoch, a.fork_epoch).unwrap(), "empty window");
        assert!(!c
            .live_child_in_epoch_range(t, Epoch(b.fork_epoch.0 + 1), Epoch(b.fork_epoch.0 + 9))
            .unwrap());

        // detach removes exactly one child and reports it.
        assert!(c.detach_child(t, a.fork_epoch).unwrap(), "detach reported nothing removed");
        assert!(!c.detach_child(t, a.fork_epoch).unwrap(), "detach removed the same child twice");
        assert_eq!(c.max_live_child(t).unwrap(), Some(b.fork_epoch));
        assert!(c.detach_child(t, b.fork_epoch).unwrap());
        assert!(!c.has_live_children(t).unwrap(), "trunk still reports children after both left");
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn expired_before_selects_only_expired_live_non_trunk_branches() {
        let (c, p, _pool) = cat("expired");
        let early = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        let late = c.fork(BranchId::TRUNK, LeaseDeadline(9_000)).unwrap();
        let held = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        c.set_state(held.branch_id, BranchState::Live, BranchState::Quarantined).unwrap();

        let ids: Vec<u64> =
            c.expired_before(1_000).unwrap().iter().map(|r| r.branch_id().id).collect();
        assert_eq!(ids, vec![early.branch_id.id], "expected only the expired Live non-trunk branch");
        assert!(!ids.contains(&late.branch_id.id), "an unexpired lease is not a candidate");
        assert!(!ids.contains(&held.branch_id.id), "a quarantined branch is not a candidate");
        assert!(!ids.contains(&BranchId::TRUNK.id), "trunk must never be a reap candidate");

        // A quarantined branch must have LEFT the deadline index, or it sits at the head of the
        // range for ever and every scan steps over it.
        assert!(c.in_state(BranchState::Quarantined).unwrap().len() == 1);
        assert!(
            c.expired_before(u64::MAX).unwrap().iter().all(|r| r.branch_id().id != held.branch_id.id),
            "the quarantined branch is still in the deadline index"
        );
        let _ = std::fs::remove_file(p);
    }

    /// Open must be a descent, not a replay: the header carries `next_id` and the epoch, so a
    /// reopened catalog cannot mint an id that collides with a live branch.
    #[test]
    fn reopening_recovers_the_counters_without_replaying_anything() {
        let (c, p, pool) = cat("reopen");
        for _ in 0..8 {
            c.fork(BranchId::TRUNK, LeaseDeadline(50)).unwrap();
        }
        let root = c.root_page_id();
        let next_id = c.next_id.load(Ordering::SeqCst);
        let epoch = c.current_epoch();
        drop(c);

        let re = TableBranchCatalog::open(pool, root).unwrap();
        assert_eq!(re.next_id.load(Ordering::SeqCst), next_id, "next_id did not survive");
        assert_eq!(re.current_epoch(), epoch, "epoch did not survive");
        assert_eq!(re.live_count().unwrap(), 9, "trunk plus eight children");

        // The id it mints next must not collide with one already handed out.
        let fresh = re.fork(BranchId::TRUNK, LeaseDeadline(50)).unwrap();
        assert_eq!(fresh.branch_id.id, next_id, "a reopened catalog reused a live id");
        let _ = std::fs::remove_file(p);
    }

    /// **The indirection page earns its keep only if the root actually moves.** This forks enough
    /// branches to split the root several times and asserts the root id CHANGED before reopening -
    /// otherwise the test would pass against a catalog that never republished anything.
    #[test]
    fn a_reopen_through_the_header_page_follows_a_root_that_moved() {
        let path = std::env::temp_dir()
            .join(format!("ferro-tablecat-hdr-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let f = OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));

        let (c, header_page) = TableBranchCatalog::create_with_header(Arc::clone(&pool), 1).unwrap();
        let first_root = c.root_page_id();
        for _ in 0..2000 {
            c.fork(BranchId::TRUNK, LeaseDeadline(50)).unwrap();
        }
        let moved_root = c.root_page_id();
        assert_ne!(
            first_root, moved_root,
            "the root never split, so this fixture cannot tell a working header page from a \
             constant one - raise the fork count"
        );
        let next_id = c.next_id.load(Ordering::SeqCst);
        drop(c);

        // Reopened knowing ONLY the header page id, which never changed.
        let re = TableBranchCatalog::open_from_header(Arc::clone(&pool), header_page).unwrap();
        assert_eq!(re.root_page_id(), moved_root, "the header page named a stale root");
        assert_eq!(re.live_count().unwrap(), 2001, "trunk plus two thousand");
        assert_eq!(re.next_id.load(Ordering::SeqCst), next_id, "counters did not survive");
        let fresh = re.fork(BranchId::TRUNK, LeaseDeadline(50)).unwrap();
        assert_eq!(fresh.branch_id.id, next_id, "a reopened catalog reused a live id");

        // Opening a page that is not a header must refuse rather than read a root id out of
        // whatever bytes happen to be there.
        match TableBranchCatalog::open_from_header(Arc::clone(&pool), moved_root) {
            Ok(_) => panic!("a tree page was accepted as a header page"),
            Err(e) => assert!(
                format!("{e}").contains("not a branch-catalog header"),
                "refused for the wrong reason: {e}"
            ),
        }
        let _ = std::fs::remove_file(&path);
    }

    /// **The crash window between `mark_reaped` and `detach_child`.**
    ///
    /// `reap` marks the child reaped and then removes its CHILD entry. A crash in between leaves a
    /// STALE entry: present, but naming a branch that is already `Reaped`. Every reader must
    /// resolve it and ignore it, or the parent's pages stay pinned for ever by a branch that no
    /// longer exists.
    ///
    /// The opposite window — entry removed while the child still reads Live — is the unsafe one,
    /// and the ordering in `reap` is what rules it out. This test covers the half that ordering
    /// leaves behind.
    #[test]
    fn a_stale_child_entry_against_a_reaped_child_is_resolved_and_ignored() {
        let (c, p, _pool) = cat("stalechild");
        let doomed = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        let survivor = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        let t = BranchId::TRUNK.id;

        // Precondition: both are live and the newest is the survivor.
        assert_eq!(c.max_live_child(t).unwrap(), Some(survivor.fork_epoch));
        assert!(c.live_child_in_epoch_range(t, doomed.fork_epoch, Epoch(doomed.fork_epoch.0 + 1)).unwrap());

        // THE CRASH WINDOW: mark reaped, do NOT detach. Spelled the way `reap` spells it, so a
        // change to what `Reaped` means reaches this fixture instead of leaving it behind.
        c.set_state(doomed.branch_id, BranchState::Live, BranchState::Reaped).unwrap();

        // The entry is still there...
        let (lo, hi) = keys::children_of(t);
        let raw: Vec<_> = c
            .tree
            .range_scan(Bound::Included(lo), Bound::Excluded(hi))
            .unwrap()
            .map(|e| e.unwrap())
            .collect();
        assert_eq!(raw.len(), 2, "fixture: the stale entry must still be present");

        // ...and every reader must see through it.
        assert!(
            !c.live_child_in_epoch_range(t, doomed.fork_epoch, Epoch(doomed.fork_epoch.0 + 1))
                .unwrap(),
            "a reaped child still pins its parent's pages - the entry was trusted, not resolved"
        );
        assert_eq!(
            c.max_live_child(t).unwrap(),
            Some(survivor.fork_epoch),
            "max_live_child returned a reaped child's epoch"
        );

        // A LIVE child must still be seen - otherwise the resolver could just always say None and
        // this whole test would pass while the reclamation rule freed everything.
        assert!(
            c.live_child_in_epoch_range(t, survivor.fork_epoch, Epoch(survivor.fork_epoch.0 + 1))
                .unwrap(),
            "a live child was ignored"
        );
        assert!(c.has_live_children(t).unwrap(), "trunk still has a live child");

        // Reap the survivor too: now every entry is stale and the parent is genuinely childless.
        c.set_state(survivor.branch_id, BranchState::Live, BranchState::Reaped).unwrap();
        assert!(
            !c.has_live_children(t).unwrap(),
            "trunk reports live children when both are reaped - its pages would never be reclaimed"
        );
        assert_eq!(c.max_live_child(t).unwrap(), None);
        let _ = std::fs::remove_file(p);
    }

    /// `attach_child` is how a branch enters a parent's live set. It had NO test against this
    /// catalog, and a mutant that made it a no-op survived the whole suite: an attached branch
    /// would simply be absent from its new parent's live set, and that parent's pages would look
    /// unreferenced by it. (The case that first forced it was `collapse` re-parenting onto trunk;
    /// D63 deleted `collapse`, leaving `migrate_from` as the only production caller. `fork` does
    /// not come through here — it writes the child entry inside its own path.)
    #[test]
    fn attach_child_puts_a_branch_into_a_parents_live_set_and_detach_takes_it_out() {
        let (c, p, _pool) = cat("attach");
        let child = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        let t = BranchId::TRUNK.id;

        assert!(c.detach_child(t, child.fork_epoch).unwrap(), "fixture: detach removed nothing");
        assert!(!c.has_live_children(t).unwrap(), "fixture: trunk should now look childless");

        // Re-attach at a NEW epoch — the shape a re-parent takes.
        let new_epoch = c.next_epoch();
        c.attach_child(t, new_epoch, child.branch_id.id).unwrap();
        assert!(c.has_live_children(t).unwrap(), "attach_child wrote nothing");
        assert_eq!(c.max_live_child(t).unwrap(), Some(new_epoch), "attached child not the newest");
        assert!(
            c.live_child_in_epoch_range(t, new_epoch, Epoch(new_epoch.0 + 1)).unwrap(),
            "the reclamation rule cannot see the re-parented child, so trunk's pages at that \
             epoch would be freed underneath it"
        );

        // And the entry it wrote is still a HINT: reap the child and it stops counting.
        c.set_state(child.branch_id, BranchState::Live, BranchState::Reaped).unwrap();
        assert!(!c.has_live_children(t).unwrap(), "attach_child wrote an unverifiable entry");
        let _ = std::fs::remove_file(p);
    }

    /// ⛔ **D124 — the guard, made to fire, and made not to fire spuriously.**
    ///
    /// A CHILD entry's value is a child branch id, and both resolvers used to resolve a MISSING
    /// record in the DESTRUCTIVE direction: `child_liveness` answered `Gone`, which
    /// `has_live_children` then skipped entirely, so the entry pinned nothing and the parent
    /// became reclaimable; `live_child_at` swept the same case into a catch-all it shared with
    /// the legitimate "reaped with nothing under it". A missing record is never a licence to
    /// free: it is either a branch that was never published or one whose record is mid-`upsert`
    /// on a live, healthy branch (`dangling_child` has the mechanism), and freeing on either is
    /// the pages of a live branch.
    ///
    /// Four parts, and the last two are what make the first two mean anything. A guard that
    /// refuses everything is not a guard, and a guard whose only caller it refuses is worse.
    #[test]
    fn a_child_entry_naming_a_branch_with_no_record_is_refused_not_resolved_away() {
        let (c, p, _pool) = cat("dangling");
        let t = BranchId::TRUNK.id;
        let ghost = 4242u64;
        assert!(c.core(ghost).unwrap().is_none(), "fixture: the ghost must have no record");

        // 1. UNREPRESENTABLE AT THE ENTRY POINT. `attach_child` is the only API that can write a
        //    CHILD entry naming an arbitrary id, and it now refuses one with no record.
        let ghost_epoch = c.next_epoch();
        let err = c.attach_child(t, ghost_epoch, ghost).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not found"), "attach_child refused with the wrong error: {msg}");
        assert!(
            c.tree.search(&keys::child(t, ghost_epoch.0)).unwrap().is_none(),
            "attach_child refused and wrote the entry anyway"
        );
        assert!(!c.has_live_children(t).unwrap(), "the refused attach still pinned trunk");

        // 2. WRITTEN PAST THE ENTRY POINT, straight into the tree, exactly as a catalog corrupted
        //    by some other means would hold it. Every reader must refuse rather than answer
        //    "not a pin" — that answer is what frees the parent's pages.
        c.upsert(keys::child(t, ghost_epoch.0), ghost.to_be_bytes().to_vec()).unwrap();
        for (name, e) in [
            ("has_live_children", c.has_live_children(t).map(|b| b.to_string())),
            ("max_live_child", c.max_live_child(t).map(|m| format!("{m:?}"))),
            (
                "live_child_in_epoch_range",
                c.live_child_in_epoch_range(t, ghost_epoch, Epoch(ghost_epoch.0 + 1))
                    .map(|b| b.to_string()),
            ),
        ] {
            let e = e.expect_err(&format!(
                "{name} resolved a CHILD entry naming an unpublished branch instead of refusing \
                 it — trunk reads as childless and its pages are freed underneath b{ghost}"
            ));
            let m = e.to_string();
            // The error has to name the entry, or nobody can find it.
            assert!(m.contains("parent 0"), "{name}: error does not name the parent: {m}");
            assert!(
                m.contains(&format!("fork epoch {}", ghost_epoch.0)),
                "{name}: error does not name the fork epoch: {m}"
            );
            assert!(m.contains(&ghost.to_string()), "{name}: error does not name the child: {m}");
        }

        // 3. AND IT MUST NOT FIRE ON THE LEGITIMATE CASE. `reap` marks a child `Reaped` and then
        //    removes its entry; a crash between those leaves a reaped child with nothing under it,
        //    which is genuinely not a live child and must keep answering exactly that. This is the
        //    arm `live_child_at`'s old catch-all shared with the unresolvable one.
        assert!(c.detach_child(t, ghost_epoch).unwrap(), "fixture: the ghost entry is gone");
        let doomed = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        c.set_state(doomed.branch_id, BranchState::Live, BranchState::Reaped).unwrap();
        let (lo, hi) = keys::children_of(t);
        assert_eq!(
            c.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi)).unwrap().count(),
            1,
            "fixture: reap's crash window leaves the entry behind, or this proves nothing"
        );
        assert!(!c.has_live_children(t).unwrap(), "a reaped childless child now refuses");
        assert_eq!(c.max_live_child(t).unwrap(), None, "a reaped childless child now refuses");
        assert!(
            !c.live_child_in_epoch_range(t, doomed.fork_epoch, Epoch(doomed.fork_epoch.0 + 1))
                .unwrap(),
            "a reaped childless child now refuses"
        );

        // 4. AND IT MUST STILL ACCEPT ITS ONLY CALLER'S SHAPE. `migrate_from` attaches a child
        //    whose record it has just written; a guard that refused that would refuse production.
        let real = c.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
        assert!(c.detach_child(t, real.fork_epoch).unwrap(), "fixture: detach removed nothing");
        let e2 = c.next_epoch();
        c.attach_child(t, e2, real.branch_id.id).expect("the guard refused a legitimate attach");
        assert!(c.has_live_children(t).unwrap(), "a live re-attached child stopped pinning");
        assert_eq!(c.max_live_child(t).unwrap(), Some(e2));
        let _ = std::fs::remove_file(p);
    }

    /// A genuine close-and-reopen: the catalog is dropped, its pool with it, and the next open
    /// starts from nothing but the file. Everything before this used one long-lived pool, which
    /// cannot tell a durable write from a page still sitting in a frame.
    #[test]
    fn a_sidecar_catalog_survives_being_closed_and_reopened_from_the_file_alone() {
        let path = std::env::temp_dir()
            .join(format!("ferro-sidecar-{}.branchcat", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let ids: Vec<u64> = {
            let c = TableBranchCatalog::open_sidecar(&path, 7).unwrap();
            assert_eq!(c.get(BranchId::TRUNK).unwrap().root_page_id, 7, "trunk root not stored");
            let mut ids = Vec::new();
            // Enough to split the root, so the header page has to have tracked it.
            for _ in 0..2000 {
                ids.push(c.fork(BranchId::TRUNK, LeaseDeadline(500)).unwrap().branch_id.id);
            }
            c.pool.flush_all().expect("flush");
            ids
        };

        let re = TableBranchCatalog::open_sidecar(&path, 7).unwrap();
        assert_eq!(re.live_count().unwrap(), 2001, "trunk plus two thousand");
        for id in &ids {
            let rec = re.get(BranchId::new(*id, 0)).expect("branch lost across reopen");
            assert_eq!(rec.parent_id, Some(BranchId::TRUNK));
        }
        // The id it mints next must not collide with one it just handed back.
        let fresh = re.fork(BranchId::TRUNK, LeaseDeadline(500)).unwrap();
        assert!(!ids.contains(&fresh.branch_id.id), "a reopened catalog reused a live id");
        let _ = std::fs::remove_file(&path);
    }

    /// Opening a NON-empty file must never create a fresh catalog over it - that would orphan
    /// every branch silently. The decision is made from the file length BEFORE `DiskManager::new`
    /// writes a bitmap into it, which is the only moment the two cases are distinguishable.
    #[test]
    fn opening_an_existing_catalog_never_creates_a_fresh_one_over_it() {
        let path = std::env::temp_dir()
            .join(format!("ferro-sidecar-noclobber-{}.branchcat", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let c = TableBranchCatalog::open_sidecar(&path, 1).unwrap();
            for _ in 0..5 {
                c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
            }
            c.pool.flush_all().unwrap();
        }
        for _ in 0..3 {
            let c = TableBranchCatalog::open_sidecar(&path, 1).unwrap();
            assert_eq!(c.live_count().unwrap(), 6, "a reopen clobbered the catalog");
            c.pool.flush_all().unwrap();
        }
        let _ = std::fs::remove_file(&path);
    }

    /// **Two implementations of one trait must mean the same thing by it.**
    ///
    /// `next_epoch` hands out the same SEQUENCE under either pre- or post-increment, so a test
    /// that only checks the forks succeed cannot tell them apart. What differs is
    /// `current_epoch()`: "the last epoch issued" versus "the next one to issue". The existing
    /// assertion elsewhere is `current_epoch() >= fork_epoch`, which holds under both readings —
    /// exactly the kind of check that lets a divergence live.
    ///
    /// This runs the same operations against the log catalog and the table catalog and compares at
    /// EVERY step, not at the end.
    #[test]
    fn the_two_catalogs_agree_about_what_current_epoch_means() {
        use crate::branch::catalog::LogBranchCatalog;

        let (table, p, _pool) = cat("epochsem");
        let log = LogBranchCatalog::in_memory(1);

        assert_eq!(
            table.current_epoch(),
            log.current_epoch(),
            "a fresh catalog disagrees about the starting epoch"
        );

        for step in 0..12 {
            let t = table.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
            let l = log.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
            assert_eq!(
                t.fork_epoch, l.fork_epoch,
                "step {step}: the two catalogs handed out different fork epochs"
            );
            assert_eq!(
                table.current_epoch(),
                log.current_epoch(),
                "step {step}: current_epoch diverged - one means the last epoch issued and the \
                 other means the next one, which makes 'carry the counter' ambiguous for any \
                 migration between them"
            );
            // And the ids must match too, for the same reason.
            assert_eq!(t.branch_id.id, l.branch_id.id, "step {step}: different ids minted");
        }
        let _ = std::fs::remove_file(p);
    }

    /// **The migration falsifier: equivalence of ANSWERS, not of representation.**
    ///
    /// A record-level `assert_eq!` fails for a CORRECT migration, because `live_children` is
    /// populated by the log catalog and deliberately empty here — the field stopped being the
    /// authority in D2b. So this compares every field EXCEPT that one, and compares the three
    /// live-child QUERIES separately, which is where the information now lives. Comparing the
    /// field would assert a representation was preserved; a migration that dropped every child
    /// entry passes that if both sides return empty arrays, and fails this immediately.
    ///
    /// The population has to reach every case or the comparison is vacuous over what it misses.
    #[test]
    fn a_migrated_catalog_answers_every_query_the_way_its_source_does() {
        use crate::branch::catalog::LogBranchCatalog;
        use crate::branch::record::CapabilityEnvelope;

        let src = LogBranchCatalog::in_memory(9);

        // -- a parent with several live children
        let kids: Vec<_> =
            (0..4).map(|_| src.fork(BranchId::TRUNK, LeaseDeadline(5_000)).unwrap()).collect();
        // -- a parent whose only child gets reaped
        let lone_parent = src.fork(BranchId::TRUNK, LeaseDeadline(5_000)).unwrap();
        let doomed = src.fork(lone_parent.branch_id, LeaseDeadline(5_000)).unwrap();
        // -- a quarantined branch
        let held = src.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        // -- a branch with arenas and a spent envelope
        let rich = src.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();

        // D41: the fixture is built with the catalog's own operations rather than by assembling a
        // record and writing it whole. That is not only because `put` is gone from the trait — a
        // state the engine cannot reach is a state the migration is not obliged to carry, so a
        // hand-built record could make this test fail for a migration that is in fact correct.
        for a in [ArenaId(3), ArenaId(11)] {
            src.add_arena(rich.branch_id, a).unwrap();
        }
        src.restrict_envelope(rich.branch_id, CapabilityEnvelope::new(0b111, 1000)).unwrap();
        src.charge_row_writes(rich.branch_id, 37).unwrap();

        src.set_state(held.branch_id, BranchState::Live, BranchState::Quarantined).unwrap();

        // -- a reaped branch, and its parent detached, so a slot is recyclable
        let d = src.get(doomed.branch_id).unwrap();
        src.detach_child(lone_parent.branch_id.id, d.fork_epoch).unwrap();
        src.set_state(doomed.branch_id, BranchState::Live, BranchState::Reaped).unwrap();
        src.release_id(d.branch_id.id);
        // -- and recycle it, so a slot carries a non-zero generation
        let recycled = src.fork(BranchId::TRUNK, LeaseDeadline(5_000)).unwrap();
        assert_ne!(recycled.branch_id.generation, 0, "fixture: no recycled slot was produced");

        // -- and leave a slot STILL FREE at migration time. Without this the free list is empty
        // when the migration runs, so a migration that skipped recycling entirely was
        // indistinguishable from a correct one - a mutant proved exactly that. Having a recycled
        // slot is not the same as having a free one.
        let spare = src.fork(BranchId::TRUNK, LeaseDeadline(5_000)).unwrap();
        let sp = src.get(spare.branch_id).unwrap();
        src.detach_child(BranchId::TRUNK.id, sp.fork_epoch).unwrap();
        src.set_state(spare.branch_id, BranchState::Live, BranchState::Reaped).unwrap();
        src.release_id(sp.branch_id.id);

        // ---- migrate -------------------------------------------------------------------------
        let path = std::env::temp_dir().join(format!("ferro-migrate-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let f = OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));
        let (dst, _hdr) = TableBranchCatalog::migrate_from(pool, &src, 9).unwrap();

        // ---- scan: same ids, same order ------------------------------------------------------
        let sids: Vec<u64> = src.scan().unwrap().map(|r| r.unwrap().branch_id.id).collect();
        let dids: Vec<u64> = dst.scan().unwrap().map(|r| r.unwrap().branch_id.id).collect();
        assert_eq!(sids, dids, "scan disagrees");
        assert!(sids.len() >= 8, "fixture too small to be meaningful: {}", sids.len());

        // ---- every record field EXCEPT live_children -----------------------------------------
        for id in &sids {
            let a = src.get_raw(*id).unwrap();
            let b = dst.get_raw(*id).unwrap();
            assert_eq!(a.branch_id, b.branch_id, "branch_id for {id}");
            assert_eq!(a.generation, b.generation, "generation for {id}");
            assert_eq!(a.parent_id, b.parent_id, "parent_id for {id}");
            assert_eq!(a.fork_epoch, b.fork_epoch, "fork_epoch for {id}");
            assert_eq!(a.root_page_id, b.root_page_id, "root_page_id for {id}");
            assert_eq!(a.lease_deadline, b.lease_deadline, "lease_deadline for {id}");
            assert_eq!(a.state, b.state, "state for {id}");
            assert_eq!(a.depth, b.depth, "depth for {id}");
            assert_eq!(a.arenas, b.arenas, "arenas for {id}");
            assert_eq!(a.envelope, b.envelope, "envelope for {id} - a dropped envelope un-governs it");
        }

        // ---- the three live-child queries, which is where live_children now lives -------------
        for id in &sids {
            assert_eq!(src.max_live_child(*id).unwrap(), dst.max_live_child(*id).unwrap(),
                       "max_live_child for {id}");
            assert_eq!(src.has_live_children(*id).unwrap(), dst.has_live_children(*id).unwrap(),
                       "has_live_children for {id}");
            // Windows that include and exclude each child, plus the whole range.
            for k in &kids {
                let lo = k.fork_epoch;
                let hi = Epoch(k.fork_epoch.0 + 1);
                assert_eq!(
                    src.live_child_in_epoch_range(*id, lo, hi).unwrap(),
                    dst.live_child_in_epoch_range(*id, lo, hi).unwrap(),
                    "live_child_in_epoch_range({id}, {lo:?}, {hi:?})"
                );
            }
            assert_eq!(
                src.live_child_in_epoch_range(*id, Epoch(0), Epoch(u64::MAX)).unwrap(),
                dst.live_child_in_epoch_range(*id, Epoch(0), Epoch(u64::MAX)).unwrap(),
                "live_child_in_epoch_range over everything, for {id}"
            );
        }

        // ---- the remaining queries ------------------------------------------------------------
        for st in [BranchState::Live, BranchState::Reaping, BranchState::Reaped,
                   BranchState::Quarantined] {
            let a: Vec<u64> =
                src.in_state(st).unwrap().iter().map(|r| r.branch_id.id).collect();
            let b: Vec<u64> =
                dst.in_state(st).unwrap().iter().map(|r| r.branch_id.id).collect();
            assert_eq!(a, b, "in_state({st:?})");
        }
        for now in [0u64, 99, 100, 101, 4_999, 5_000, 5_001, u64::MAX] {
            let a: Vec<u64> =
                src.expired_before(now).unwrap().iter().map(|r| r.branch_id().id).collect();
            let b: Vec<u64> =
                dst.expired_before(now).unwrap().iter().map(|r| r.branch_id().id).collect();
            assert_eq!(a, b, "expired_before({now})");
        }
        for id in &sids {
            let bid = BranchId::new(*id, src.get_raw(*id).unwrap().branch_id.generation);
            assert_eq!(src.envelope_of(bid).ok(), dst.envelope_of(bid).ok(), "envelope_of {id}");
        }
        // Through the TRAIT on both sides: the inherent `TableBranchCatalog::live_count` returns
        // a Result and the trait method returns usize, and Rust picks the inherent one for a
        // concrete type. Comparing the trait's answers is the point.
        assert_eq!(
            BranchCatalog::live_count(&src),
            BranchCatalog::live_count(&dst),
            "live_count"
        );
        assert_eq!(src.current_epoch(), dst.current_epoch(), "current_epoch");

        // ---- and the next id minted must agree, or a migration reuses a live slot -------------
        let sn = src.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let dn = dst.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        assert_eq!(sn.branch_id, dn.branch_id, "the next branch minted differs after migrating");
        assert_eq!(sn.fork_epoch, dn.fork_epoch, "the next fork epoch differs after migrating");
        let _ = std::fs::remove_file(&path);
    }

    /// The open-or-migrate policy end to end, on real files.
    #[test]
    fn a_legacy_log_is_migrated_once_and_retired_not_deleted() {
        use crate::branch::catalog::LogBranchCatalog;
        let dir = std::env::temp_dir().join(format!("ferro-dfd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("x");
        let db_s = db.to_str().unwrap().to_string();

        // A legacy log with a population.
        let ids: Vec<u64> = {
            let log = LogBranchCatalog::open(&dir.join("x.branches"), 1).unwrap();
            (0..25).map(|_| log.fork(BranchId::TRUNK, LeaseDeadline(9_000)).unwrap().branch_id.id)
                .collect()
        };

        let cat = TableBranchCatalog::default_for_database(&db_s, 1).unwrap();
        assert_eq!(BranchCatalog::live_count(&cat), 26, "trunk plus twenty-five");
        for id in &ids {
            cat.get(BranchId::new(*id, 0)).expect("branch lost in migration");
        }

        assert!(dir.join("x.branchcat").exists(), "the tree catalog was not published");
        assert!(!dir.join("x.branches").exists(), "the legacy log was left in place");
        assert!(
            dir.join("x.branches.pre-table").exists(),
            "the legacy log was DELETED - a conversion that turns out wrong is only recoverable \
             if its source survives it"
        );
        assert!(!dir.join("x.branchcat.tmp").exists(), "the staging file was left behind");
        cat.pool.flush_all().unwrap();
        drop(cat);

        // Second open: uses the tree, does not migrate again, and does not lose anything.
        let again = TableBranchCatalog::default_for_database(&db_s, 1).unwrap();
        assert_eq!(BranchCatalog::live_count(&again), 26, "a second open lost branches");
        for id in &ids {
            again.get(BranchId::new(*id, 0)).expect("branch lost on reopen");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A crash between publishing the tree and retiring the log leaves BOTH files. The next open
    /// must use the tree - it is complete by construction, since only a finished migration is ever
    /// renamed into place - and retire the stale log rather than leave two catalogs around.
    #[test]
    fn an_interrupted_switchover_prefers_the_tree_and_retires_the_stale_log() {
        use crate::branch::catalog::LogBranchCatalog;
        let dir = std::env::temp_dir().join(format!("ferro-dfd-int-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("y");
        let db_s = db.to_str().unwrap().to_string();

        {
            let log = LogBranchCatalog::open(&dir.join("y.branches"), 1).unwrap();
            for _ in 0..5 {
                log.fork(BranchId::TRUNK, LeaseDeadline(9_000)).unwrap();
            }
        }
        let cat = TableBranchCatalog::default_for_database(&db_s, 1).unwrap();
        cat.pool.flush_all().unwrap();
        drop(cat);
        // Simulate the crash window: put the legacy log back alongside a finished tree.
        std::fs::rename(dir.join("y.branches.pre-table"), dir.join("y.branches")).unwrap();

        let again = TableBranchCatalog::default_for_database(&db_s, 1).unwrap();
        assert_eq!(BranchCatalog::live_count(&again), 6, "the stale log was migrated a second time");
        assert!(!dir.join("y.branches").exists(), "the stale log was left beside the tree");
        assert!(dir.join("y.branches.pre-table").exists(), "the stale log was deleted");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Exactly what `ArenaPageStore::alloc_arena` does: claim an extent through `add_arena`. If it
    /// does not round-trip, the reaper frees nothing, because it frees precisely `record.arenas`.
    ///
    /// **And then: every OTHER mutator must leave the span alone.** This is a whole-file hazard
    /// rather than one method's, because `write_record` makes the arena span match the record it is
    /// handed — so any mutator that reads a CORE record (whose `arenas` is empty by construction)
    /// and writes it back DELETES every extent the branch owns, silently, and the pages are lost
    /// for the life of the file. It has happened: the page store claimed an extent, recorded it,
    /// and the next `set_root` threw it away. Each mutator here is a separate arm because each has
    /// its own chance to make that mistake, and **D41 added three more of them** — `reparent`,
    /// `set_state` and `restrict_envelope` all hydrate for exactly this reason.
    #[test]
    fn an_arena_appended_the_way_the_page_store_does_it_survives_a_round_trip() {
        use crate::branch::record::CapabilityEnvelope;
        let (c, p, _pool) = cat("arenart");
        let child = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();

        assert!(
            c.get_raw(child.branch_id.id).expect("get_raw").arenas.is_empty(),
            "fixture: a fresh branch owns no arena"
        );
        c.add_arena(child.branch_id, ArenaId(7)).expect("add_arena");

        let back = c.get_raw(child.branch_id.id).expect("get_raw after add_arena");
        assert_eq!(back.arenas, vec![ArenaId(7)], "the arena did not survive add_arena/get_raw");

        // and a second arena appends rather than replaces
        c.add_arena(child.branch_id, ArenaId(9)).unwrap();
        assert_eq!(
            c.get_raw(child.branch_id.id).unwrap().arenas,
            vec![ArenaId(7), ArenaId(9)],
            "appending a second arena lost the first"
        );

        let owned = vec![ArenaId(7), ArenaId(9)];
        c.set_root(child.branch_id, 123).expect("set_root");
        assert_eq!(
            c.get_raw(child.branch_id.id).unwrap().arenas, owned,
            "set_root deleted the branch's arenas"
        );
        c.renew_lease(child.branch_id, LeaseDeadline(5_000)).expect("renew_lease");
        assert_eq!(
            c.get_raw(child.branch_id.id).unwrap().arenas, owned,
            "renew_lease deleted the branch's arenas"
        );
        assert_eq!(c.get_raw(child.branch_id.id).unwrap().root_page_id, 123, "set_root lost");

        // ---- D41's three, each with its own chance to write a core record back ---------------
        c.restrict_envelope(child.branch_id, CapabilityEnvelope::new(0b001, 500))
            .expect("restrict_envelope");
        assert_eq!(
            c.get_raw(child.branch_id.id).unwrap().arenas, owned,
            "restrict_envelope deleted the branch's arenas"
        );
        c.set_state(child.branch_id, BranchState::Live, BranchState::Quarantined)
            .expect("set_state");
        assert_eq!(
            c.get_raw(child.branch_id.id).unwrap().arenas, owned,
            "set_state deleted the branch's arenas"
        );
        c.set_state(child.branch_id, BranchState::Quarantined, BranchState::Live).unwrap();
        // `reparent` is the one whose predecessor leaked 664 of 664 copied pages by writing back a
        // record that had lost its extents (D13b mutant D). Re-parenting onto trunk was what
        // `collapse` did before D63 deleted it; the write itself still has to preserve arenas.
        let moved = c
            .reparent(child.branch_id, BranchId::TRUNK, c.next_epoch(), 321)
            .expect("reparent");
        assert_eq!(moved.arenas, owned, "reparent returned a record with no arenas");
        assert_eq!(
            c.get_raw(child.branch_id.id).unwrap().arenas, owned,
            "reparent deleted the branch's arenas — a branch's extents are exactly what is at risk \
             in a whole-record write, and nothing else would ever free them"
        );
        assert_eq!(c.get_raw(child.branch_id.id).unwrap().root_page_id, 321, "reparent lost root");

        // and `scan` must show them too - snapshot serializes what scan yields
        let scanned = c.scan().unwrap()
            .map(|r| r.unwrap())
            .find(|r| r.branch_id.id == child.branch_id.id)
            .expect("branch missing from scan");
        assert_eq!(scanned.arenas, owned, "scan dropped the arenas");

        // LAST, because it is the one transition that MUST clear the span: the reaper has already
        // handed those extents back, so a record that still names them names free space. It runs
        // after every "must not wipe" arm above, which is why those arms can assert a non-empty
        // span at all.
        c.set_state(child.branch_id, BranchState::Live, BranchState::Reaped).unwrap();
        assert!(
            c.get_raw(child.branch_id.id).unwrap().arenas.is_empty(),
            "a reaped branch still names extents that are back in the free-space map"
        );
        let _ = std::fs::remove_file(p);
    }

    /// **A child must inherit its parent's capability envelope.** `fork_child` does
    /// `parent.envelope.as_ref().map(inherited)`, so a parent record read WITHOUT its envelope
    /// hands every child `None` - which is the ungoverned default. That is a capability escape:
    /// an agent forks a branch off a governed one and the child may write anything.
    #[test]
    fn a_child_inherits_its_parents_capability_envelope() {
        use crate::branch::record::CapabilityEnvelope;
        let (c, p, _pool) = cat("inherit");

        let parent = c.fork(BranchId::TRUNK, LeaseDeadline(5_000)).unwrap();
        c.restrict_envelope(parent.branch_id, CapabilityEnvelope::new(0b001, 500)).unwrap();
        assert!(c.envelope_of(parent.branch_id).unwrap().is_some(), "fixture: parent is governed");

        let child = c.fork(parent.branch_id, LeaseDeadline(5_000)).unwrap();
        assert!(
            child.envelope.is_some(),
            "the child of a GOVERNED branch came back ungoverned - a capability escape"
        );
        assert!(
            c.envelope_of(child.branch_id).unwrap().is_some(),
            "the child's inherited envelope was not persisted"
        );
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn scan_returns_every_record_in_branch_id_order() {
        let (c, p, _pool) = cat("scan");
        for _ in 0..12 {
            c.fork(BranchId::TRUNK, LeaseDeadline(50)).unwrap();
        }
        let ids: Vec<u64> = c.scan().unwrap().map(|r| r.unwrap().branch_id.id).collect();
        assert_eq!(ids.len(), 13, "trunk plus twelve");
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(ids, sorted, "scan came back out of branch-id order");
        let _ = std::fs::remove_file(p);
    }
}

/// See [`TableBranchCatalog::child_liveness`].
enum ChildLiveness {
    /// A live child: its parent is pinned by it.
    Live,
    /// A reaped child that may still have live descendants (D16). Carries its id so the caller
    /// can explore it without recursing.
    ReapedWithSubtree(u64),
    // There is deliberately no `Gone` variant. It existed until D124 and meant "no record at all
    // — a stale hint, pinning nothing", and `has_live_children` skipped it, so a child whose
    // record could not be read let the parent be reclaimed. A missing record is now an ERROR
    // rather than a value, because it is not a fact about the child: it is equally the signature
    // of a record being rewritten right now (`dangling_child` has the mechanism). Keeping the
    // variant unconstructed would only invite the next reader to resolve into it again.
}

