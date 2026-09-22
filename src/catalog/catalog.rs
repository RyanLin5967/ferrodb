use std::collections::HashMap;
use std::sync::Arc;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog_page::{CatalogPage, FullTextIndexInfo, IndexInfo, TableEntry};
use crate::catalog::stats::{ColumnStats, TableStats};
use crate::error::FerroError;
use crate::storage::heap_file_manager::HeapFileManager;
use crate::storage::index::BPlusTreeManager;
use crate::storage::heap_file_manager::RecordId;
use crate::catalog::column::{DataType, Value};
use crate::storage::index_fulltext::{indexed_text, post_tokens};
use std::sync::atomic::{AtomicU32, Ordering};
use crate::catalog::schema::Schema;

#[derive(Clone)]
pub struct Catalog {
    pub tables: HashMap<String, TableEntry>,
    /// **Monotone change counter per table — D69. Deliberately NOT serialized**, like
    /// `root_cells` below: it exists to detect movement WITHIN a process, between the moment a
    /// merge is scored and the moment it is published, and a value that survived a restart would
    /// be comparing against a base that no longer exists anyway.
    ///
    /// # Why a counter and not the fingerprint it replaces
    ///
    /// `agent_sql::runtime` used to answer "did the base move?" by SCANNING every row of every
    /// touched table and hashing it — twice per merge, once at scoring and once at publication
    /// (`evaluate_merge` and `fingerprint_tables`). Measured at 1.87 us/row (D68,
    /// `bench/d68_merge_is_o_table.txt`): ~1.9 SECONDS per merge against a million-row table, to
    /// write four rows. Comparing two `u64`s is O(1) and answers the same question.
    ///
    /// It is also STRICTLY STRONGER than the hash it replaces. A fingerprint cannot see a change
    /// that was reverted before it looked; a monotone counter can, because it never goes back.
    ///
    /// ⚠ **Every committed row change must bump it, not only agent merges.** The hash it replaces
    /// was computed by scanning the real table, so it saw ordinary `INSERT`/`UPDATE`/`DELETE` as
    /// well. A counter bumped only on the agent path would miss direct writes and weaken the
    /// staleness check silently — which is worse than the cost it removes. Keyed by
    /// `agent_sql::runtime::table_id`'s FNV-1a hash of the name, so both sides agree without the
    /// catalog minting ids.
    table_versions: HashMap<u32, u64>,
    pub buffer_pool: Arc<BufferPoolManager>,
    pub first_catalog_page_id: u32,
    pub stats: HashMap<String, TableStats>,
    /// **Shared root cells, one per tree.** Deliberately NOT serialized — it is derived from
    /// `tables` and rebuilt by [`Catalog::sync_root_cells`].
    ///
    /// # Why it exists
    ///
    /// `BPlusTreeManager::open` wraps the catalog's recorded root in a *private* atomic, so two
    /// statements over one table held two independent root pointers and the root-split retry in
    /// `read_leaf_for` compared a private value against itself — it could never fire. Handing
    /// every statement the SAME cell is what makes that guard live. See `SCALE-DESIGN` D53.
    ///
    /// Keyed `(table, None)` for the primary index and `(table, Some(column))` for a secondary or
    /// full-text one.
    roots: HashMap<(String, Option<String>), Arc<AtomicU32>>,
    /// Bumped by every change to the SCHEMA — and deliberately **not** by a root move.
    ///
    /// A reader caches a snapshot of this catalog and re-takes it only when this number changes.
    /// Before D53 a root move had to invalidate every snapshot, because `primary_index_root` was
    /// the authoritative pointer a reader descended from; now the registry above hands out a
    /// SHARED cell that a split updates in place, so a snapshot whose recorded root is stale is
    /// still correct — `open_table` reads the cell, not the record. That is the whole reason a
    /// write statement no longer invalidates every reader's cache.
    epoch: u64,
}

impl Catalog {
    pub fn create(buffer_pool: Arc<BufferPoolManager>) -> Result<Self, FerroError> {
        let page_id = buffer_pool.new_page()?; // = 1 on a fresh DB
        let frame_i = buffer_pool.fetch_page(page_id)?;
        let mut frame = buffer_pool.frame_write(frame_i);
        let page = CatalogPage::new(page_id);
        frame.data = page.serialize()?;
        drop(frame);
        buffer_pool.unpin_page(page_id, true);
        Ok(Self {tables: HashMap::new(), buffer_pool: buffer_pool.clone(), first_catalog_page_id: 1, stats: HashMap::new(), table_versions: HashMap::new(), roots: HashMap::new(), epoch: 0})
    }

    pub fn open(buffer_pool: Arc<BufferPoolManager>, first_catalog_page_id: u32) -> Result<Self, FerroError>{
        let mut catalog = Self {tables: HashMap::new(), buffer_pool, first_catalog_page_id, stats: HashMap::new(), table_versions: HashMap::new(), roots: HashMap::new(), epoch: 0};
        catalog.load()?;
        Ok(catalog)
    }

    /// The SHARED root cell for a tree, or `None` if this catalog has never seen it.
    ///
    /// Read-only and lock-free: the map is populated by [`Catalog::sync_root_cells`] from the
    /// `&mut self` paths, so a reader holding only `&Catalog` does a plain hash lookup.
    pub fn root_cell(&self, table: &str, column: Option<&str>) -> Option<Arc<AtomicU32>> {
        self.roots.get(&(table.to_string(), column.map(|c| c.to_string()))).cloned()
    }

    /// The schema epoch. A reader compares this with one relaxed load and re-snapshots only when
    /// it moves. See the field's own doc for why a root move does not move it.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Bump the schema epoch from another module in this crate (the ALTER path lives in
    /// `catalog::alter`). `pub(crate)` on purpose: nothing outside the catalog may invalidate
    /// every reader's snapshot.
    pub(crate) fn epoch_bump(&mut self) {
        self.epoch += 1;
    }

    /// Ensure every tree named in `tables` has a shared root cell.
    ///
    /// **Creates missing cells; never overwrites an existing one.** That asymmetry is the whole
    /// correctness argument: an existing cell may already be shared with a live handle whose
    /// split has advanced it past the value recorded on the catalog page, and clobbering it would
    /// hand the next reader a root that has moved. A missing cell has no such history, so seeding
    /// it from the durable record is right.
    pub fn sync_root_cells(&mut self) {
        let mut want: Vec<((String, Option<String>), u32)> = Vec::new();
        for (name, entry) in self.tables.iter() {
            want.push(((name.clone(), None), entry.primary_index_root));
            for idx in entry.indexes.iter() {
                want.push(((name.clone(), Some(idx.column_name.clone())), idx.root_page_id));
            }
            for ft in entry.fulltext_indexes.iter() {
                want.push(((name.clone(), Some(ft.column_name.clone())), ft.root_page_id));
            }
        }
        for (key, root) in want {
            self.roots.entry(key).or_insert_with(|| Arc::new(AtomicU32::new(root)));
        }
        // Drop cells for tables this catalog no longer holds, so a DROP+CREATE of the same name
        // cannot inherit the old tree's pointer.
        let live: std::collections::HashSet<&String> = self.tables.keys().collect();
        self.roots.retain(|(t, _), _| live.contains(t));
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<(), FerroError> {
        // E67: this was `FerroError::KeyNotFound`, so creating a table that already exists answered
        // `error: key wasn't found` - a storage-layer message, for a name collision, telling the
        // reader that something is missing when the problem is that something is present.
        if self.tables.contains_key(&name) {
            return Err(FerroError::Constraint(format!(
                "table '{name}' already exists; DROP TABLE {name} first, or choose another name"
            )));
        }
        // B9: a table may not take a system view's name. Checked here — in `catalog`, because
        // `Catalog::tables` is where every path that creates a table converges, and the parser is
        // not; a guard in the parser is walked around by any caller that builds a `CreateTable`
        // without going through SQL text.
        //
        // The state being made unrepresentable: `executor::run` recognises a view name before any
        // other route, so a table called `ferro_quarantine` would accept writes and answer every
        // read from the view. Rows in, nothing out, no error anywhere.
        //
        // **Ordered AFTER the already-exists check, and that order is a correctness fix.** On a
        // database written before these views existed, a table CAN already hold one of these names
        // (`Catalog::load` rebuilds `tables` from the catalog pages and never comes through here).
        // Refusing such a `CREATE TABLE` with the view message told the reader that every SELECT
        // would answer from the view and the table's rows were unreachable — and for that database
        // both halves are false, because `system_views::view_for` yields to the real table. The
        // honest answer there is the one above: the table already exists.
        crate::catalog::system_views::reject_view_name_collision(&name)?;
        let hfm = HeapFileManager::new(self.buffer_pool.clone())?;
        let primary = BPlusTreeManager::<Value, RecordId>::create(self.buffer_pool.clone())?;
        let tt_heap = HeapFileManager::new(self.buffer_pool.clone())?;
        let entry = TableEntry {
            name: name.clone(),
            first_directory_page_id: hfm.first_directory_page_id,
            schema,
            primary_index_root: primary.root_page_id.load(Ordering::Relaxed),
            time_travel_root: tt_heap.first_directory_page_id, 
            indexes: Vec::new(),
            fulltext_indexes: Vec::new()
        };
        self.tables.insert(name.clone(), entry);
        // Undo: the table never existed if it could not be written down.
        self.persist_or_undo(|c| {
            c.tables.remove(&name);
        })?;
        // The set of trees changed, so seed (or retire) their shared root cells, and tell
        // every cached reader snapshot that the schema moved.
        self.sync_root_cells();
        self.epoch += 1;
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&TableEntry> {
        self.tables.get(name)
    }

    /// The one refusal for "there is no such table", so every statement gives the same answer.
    ///
    /// E67 measured the alternative. `SELECT * FROM nosuch` said
    /// `unknown table 'nosuch'; known tables are: t`, while `INSERT INTO nosuch`, `UPDATE nosuch` and
    /// `DELETE FROM nosuch` all said `parsing error: table not found` - wrong error class for a
    /// catalog lookup, and naming nothing. One condition, two qualities of answer, because the good
    /// message lived in the binder and the planner had its own literal. This is that message, moved
    /// to where the table list actually is so there is nothing left to reimplement.
    pub fn unknown_table(&self, name: &str) -> FerroError {
        let mut known: Vec<&str> = self.tables.keys().map(|s| s.as_str()).collect();
        known.sort_unstable();
        FerroError::Bind(if known.is_empty() {
            format!("unknown table '{name}'; this database has no tables yet — CREATE TABLE one first")
        } else {
            format!("unknown table '{name}'; known tables are: {}", known.join(", "))
        })
    }

    /// `table` if it exists, or the shared refusal above.
    pub fn require_table(&self, name: &str) -> Result<&TableEntry, FerroError> {
        self.get_table(name).ok_or_else(|| self.unknown_table(name))
    }

    // create a secondary B+ tree, push an IndexInfo onto the table, persist
    pub fn create_index(&mut self, table: &str, column: &str) -> Result<(), FerroError> {
        let (schema, first_dir_page_id, col_index) = {
            // E67: both of these were bare `KeyNotFound`, so `CREATE INDEX ix ON nosuch (v)` and
            // `CREATE INDEX ix ON t (nosuchcol)` - two different mistakes - produced the identical
            // contentless `error: key wasn't found`. The table case now reuses the shared refusal, so
            // it lists the tables that do exist exactly as SELECT does.
            let entry = self.tables.get(table).ok_or_else(|| self.unknown_table(table))?;

            if entry.indexes.iter().any(|ind| ind.column_name == column) {
                return Err(FerroError::IndexAlreadyExists);
            }
            let col_index = entry.schema.columns.iter()
                .position(|c| c.name == column)
                .ok_or_else(|| FerroError::Bind(format!(
                    "cannot index '{table}.{column}': no such column. '{table}' has: {}",
                    entry.schema.columns.iter()
                        .map(|c| c.name.as_str()).collect::<Vec<_>>().join(", ")
                )))?;

            (entry.schema.clone(), entry.first_directory_page_id, col_index)
        };
        let sec_tree = BPlusTreeManager::<(Value, Value), ()>::create(self.buffer_pool.clone())?;
        let new_root_id = sec_tree.root_page_id.load(Ordering::Relaxed);

        let hfm = HeapFileManager::open(first_dir_page_id, self.buffer_pool.clone());
        for item in hfm.scan() {
            let (_, tuple) = item?;
            let values = tuple.deserialize(&schema)?;
            let sec_value = values[col_index].clone();
            let primary_key = values[0].clone();   // first column = primary key
            sec_tree.insert((sec_value, primary_key), ())?;
        }

        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.indexes.push(IndexInfo { column_name: column.to_string(), root_page_id: new_root_id });

        // Undo: pop the index this call pushed. A column name too long for its length prefix is
        // refused by the encoder exactly as a table name is.
        let table = table.to_string();
        self.persist_or_undo(|c| {
            if let Some(e) = c.tables.get_mut(&table) {
                e.indexes.pop();
            }
        })?;
        // The set of trees changed, so seed (or retire) their shared root cells, and tell
        // every cached reader snapshot that the schema moved.
        self.sync_root_cells();
        self.epoch += 1;
        Ok(())
    }

    /// B8 — create a full-text index on one `VARCHAR` column and backfill it from the heap.
    ///
    /// Same shape as `create_index` above, and deliberately so: the tree is the same type, the
    /// backfill is the same scan, and the only differences are the three that matter.
    ///
    /// 1. **The column must be `VARCHAR`.** Refused, not coerced. Tokenizing an `Integer` would
    ///    mean picking a rendering for it, and every choice there is a silent one — so the
    ///    maintenance paths are allowed to assume `Varchar | Null` and report anything else as a
    ///    bug in this refusal (`index_fulltext::indexed_text`).
    /// 2. **The backfill de-duplicates.** `create_index`'s does not, and gets away with it because
    ///    the main heap holds one live version per primary key. That is not enough here: `DELETE`
    ///    stamps `end_ts` and leaves the slot, so `DELETE FROM t WHERE id = 4` followed by
    ///    `INSERT INTO t VALUES (4, ...)` leaves **two** slots with pk 4 in the same heap. Building
    ///    an index over that emits every token they share twice, `insert_entry` appends rather than
    ///    overwrites, and the search then returns that row twice. `post_tokens` probes first.
    /// 3. **It lands in `fulltext_indexes`**, not `indexes` — see `FullTextIndexInfo`.
    pub fn create_fulltext_index(&mut self, table: &str, column: &str) -> Result<(), FerroError> {
        let (schema, first_dir_page_id, col_index) = {
            let entry = self.tables.get(table).ok_or_else(|| self.unknown_table(table))?;

            if entry.fulltext_indexes.iter().any(|ind| ind.column_name == column) {
                return Err(FerroError::IndexAlreadyExists);
            }
            let col_index = entry.schema.columns.iter()
                .position(|c| c.name == column)
                .ok_or_else(|| FerroError::Bind(format!(
                    "cannot index '{table}.{column}': no such column. '{table}' has: {}",
                    entry.schema.columns.iter()
                        .map(|c| c.name.as_str()).collect::<Vec<_>>().join(", ")
                )))?;
            if !matches!(entry.schema.columns[col_index].data_type, DataType::Varchar(_)) {
                return Err(FerroError::Bind(format!(
                    "cannot build a full-text index on '{table}.{column}': it is {:?}, and only \
                     VARCHAR columns have text to tokenize",
                    entry.schema.columns[col_index].data_type
                )));
            }

            (entry.schema.clone(), entry.first_directory_page_id, col_index)
        };
        let ft_tree = BPlusTreeManager::<(Value, Value), ()>::create(self.buffer_pool.clone())?;
        let new_root_id = ft_tree.root_page_id.load(Ordering::Relaxed);

        let hfm = HeapFileManager::open(first_dir_page_id, self.buffer_pool.clone());
        for item in hfm.scan() {
            let (_, tuple) = item?;
            let values = tuple.deserialize(&schema)?;
            let primary_key = values[0].clone();   // first column = primary key
            if let Some(text) = indexed_text(&values[col_index])? {
                post_tokens(&ft_tree, text, &primary_key)?;
            }
        }

        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.fulltext_indexes.push(FullTextIndexInfo { column_name: column.to_string(), root_page_id: new_root_id });

        // Undo: pop the full-text index this call pushed. Same reason as `create_index`.
        let table = table.to_string();
        self.persist_or_undo(|c| {
            if let Some(e) = c.tables.get_mut(&table) {
                e.fulltext_indexes.pop();
            }
        })?;
        // The set of trees changed, so seed (or retire) their shared root cells, and tell
        // every cached reader snapshot that the schema moved.
        self.sync_root_cells();
        self.epoch += 1;
        Ok(())
    }

    /// Remove a table and give back every page it allocated.
    ///
    /// **E69 found three gaps in this, all of them because nothing had ever called it.** It has existed
    /// since the catalog did, and until `DROP TABLE` reached the SQL surface there was no caller at
    /// all - so a page leak and a stale-stats bug sat here unexercised:
    ///
    /// - the **time-travel heap was never freed**. Every table has one (`time_travel_root`), `UPDATE`
    ///   and `DELETE` push old versions into it, and dropping the table left all of it allocated. On a
    ///   table with any update history that is the larger leak of the two heaps.
    /// - `self.stats` kept the dropped table's row counts, so a table recreated under the same name
    ///   inherited the old table's statistics and the optimizer planned against a stranger's data.
    /// - the missing-table error was a bare `KeyNotFound`, rendering as "key wasn't found" - the exact
    ///   contentless message E67 removed everywhere else.
    ///
    /// Roots are read out of the entry BEFORE it is removed, because the entry is the only record of
    /// where those pages are.
    pub fn drop_table(&mut self, name: &str) -> Result<(), FerroError> {
        let (heap_dir, tt_root, primary_root, sec_roots) = {
            let entry = self.require_table(name)?;
            (
                entry.first_directory_page_id,
                entry.time_travel_root,
                entry.primary_index_root,
                // B8: the full-text roots go in the SAME list because the trees are the same type,
                // and leaving them out would leak one tree per full-text index on every DROP TABLE
                // — the E69 gap, re-opened by a second index list.
                entry.indexes.iter().map(|i| i.root_page_id)
                    .chain(entry.fulltext_indexes.iter().map(|i| i.root_page_id))
                    .collect::<Vec<_>>(),
            )
        };
        HeapFileManager::open(heap_dir, self.buffer_pool.clone()).free_all()?;
        HeapFileManager::open(tt_root, self.buffer_pool.clone()).free_all()?;
        BPlusTreeManager::<Value, RecordId>::open(primary_root, self.buffer_pool.clone()).free_all()?;
        for root in sec_roots {
            BPlusTreeManager::<(Value, Value), ()>::open(root, self.buffer_pool.clone()).free_all()?;
        }
        self.tables.remove(name);
        self.stats.remove(name);
        self.persist()?;
        // The set of trees changed, so seed (or retire) their shared root cells, and tell
        // every cached reader snapshot that the schema moved.
        self.sync_root_cells();
        self.epoch += 1;
        Ok(())
    }

    // root split propagation called when a tree's root changes
    pub fn update_primary_root(&mut self, table: &str, new_root: u32) -> Result<(), FerroError> {
        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.primary_index_root = new_root;
        self.persist()?;
        // Keep the SHARED cell in step with the durable record. A split stores into the cell and
        // then calls this, so the two usually already agree; a caller that sets the root directly
        // (tests, ALTER) would otherwise leave the cell pointing at the old tree.
        if let Some(cell) = self.roots.get(&(table.to_string(), None)) {
            cell.store(new_root, Ordering::Release);
        }
        Ok(())
    }

    pub fn update_index_root(&mut self, table: &str, column: &str, new_root: u32) -> Result<(), FerroError> {
        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.indexes.iter_mut().find(|ind| ind.column_name == column).ok_or(FerroError::KeyNotFound)?.root_page_id = new_root;
        self.persist()?;
        if let Some(cell) = self.roots.get(&(table.to_string(), Some(column.to_string()))) {
            cell.store(new_root, Ordering::Release);
        }
        Ok(())
    }

    /// B8 — the `update_index_root` of the full-text list. Separate because the two lists are
    /// separate: resolving a full-text column name inside `indexes` would either miss (and leave a
    /// split tree's new root unrecorded, losing every posting added since) or hit a same-named
    /// B-tree index and overwrite ITS root with a token tree's.
    pub fn update_fulltext_root(&mut self, table: &str, column: &str, new_root: u32) -> Result<(), FerroError> {
        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.fulltext_indexes.iter_mut().find(|ind| ind.column_name == column).ok_or(FerroError::KeyNotFound)?.root_page_id = new_root;
        self.persist()?;
        if let Some(cell) = self.roots.get(&(table.to_string(), Some(column.to_string()))) {
            cell.store(new_root, Ordering::Release);
        }
        Ok(())
    }

    /// Record that `table` changed. Called from every committed write path.
    ///
    /// Takes the table NAME and hashes it the same way `agent_sql::runtime::table_id` does, so the
    /// merge path can look the counter up without the catalog minting ids.
    pub fn bump_table_version(&mut self, table: &str) {
        let id = Self::table_version_key(table);
        *self.table_versions.entry(id).or_insert(0) += 1;
    }

    /// The current change counter for a table id, or 0 if it has never been written.
    ///
    /// 0 for "never written" is correct rather than convenient: a table nobody has written cannot
    /// have moved, and two reads of 0 compare equal exactly as two reads of any other value do.
    pub fn table_version(&self, id: u32) -> u64 {
        self.table_versions.get(&id).copied().unwrap_or(0)
    }

    /// FNV-1a over the name — byte for byte what `agent_sql::runtime::table_id` computes.
    ///
    /// Duplicated rather than imported because `catalog` must not depend on `agent_sql`; the
    /// duplication is pinned by a test that asserts the two agree, so it cannot drift silently.
    pub fn table_version_key(table: &str) -> u32 {
        let mut h: u32 = 0x811c_9dc5;
        for b in table.as_bytes() {
            h ^= *b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
        h
    }

    /// Make an in-memory catalog change durable, and **undo it if it cannot be made durable**.
    ///
    /// `Catalog::tables` is the authority every reader and every later `persist` works from, and
    /// the mutators here insert into it *before* persisting. That was harmless only while `persist`
    /// could not refuse for a reason the caller had just created. D141 made it refuse: a name too
    /// long for a catalog page's length prefix is now caught at the encoder instead of being
    /// written truncated, and nothing upstream bounds identifier length.
    ///
    /// Without the undo, the refused entry stays in the map and **every later DDL re-serializes it
    /// and fails too** — one over-long `CREATE TABLE` takes out every subsequent one until restart.
    /// That is a wedge, not a refusal. Measured before this existed, by
    /// `tests/d141_long_identifier.rs::a_refused_create_table_does_not_wedge_the_next_one`.
    ///
    /// It is not a second length check — the encoder remains the only authority on what fits. This
    /// makes the *mutation* atomic, so it covers every reason `persist` can fail, an I/O error
    /// included, and not just the one D141 added.
    fn persist_or_undo(&mut self, undo: impl FnOnce(&mut Self)) -> Result<(), FerroError> {
        match self.persist() {
            Ok(()) => Ok(()),
            Err(e) => {
                undo(self);
                Err(e)
            }
        }
    }

    pub fn persist(&self) -> Result<(), FerroError> {
        let mut curr_page_id = self.first_catalog_page_id;
        // **By name, not by `HashMap` order.** Which table lands on which catalog page, and therefore
        // which bytes are written where, used to depend on a per-process hash seed: `persist()` on the
        // same two tables produced a different on-disk layout on every run. Nothing can rely on the
        // old order because the old order was random, so sorting is safe; what it buys is a catalog
        // whose image is a function of its contents, which is what makes a crash during `persist`
        // reproducible at all.
        let mut sorted: Vec<&TableEntry> = self.tables.values().collect();
        sorted.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        let mut iter = sorted.into_iter().peekable();

        loop {
            let frame_i = self.buffer_pool.fetch_page(curr_page_id)?;

            let mut page = {
                let frame = self.buffer_pool.frames[frame_i].read().unwrap();
                CatalogPage::deserialize(frame.data)?
            };

            page.entries.clear();
            page.num_entries = 0;

            while let Some(entry) = iter.peek() {
                if page.has_space(entry) {
                    page.add_entry(iter.next().unwrap().clone())?;
                } else {
                    break;
                }
            }

            let has_more = iter.peek().is_some();
            let mut orphan_head = 0;
            if has_more {
                if page.next_catalog_page == 0 {
                    let new_id = self.buffer_pool.new_page()?;
                    // Stamp it as an empty catalog page before linking it. `new_page` hands back a
                    // zero-filled page, and the next turn of this loop deserializes whatever is at
                    // `next_catalog_page` — so an unstamped page arrives at `CatalogPage::deserialize`
                    // with format byte 0. That used to parse as an accidentally-empty page because
                    // byte 0 was never read; now that the byte is the format stamp (B8), the choice
                    // is between initialising the page here and teaching the format allowlist to
                    // accept all-zeroes, which would let a genuinely corrupt page through.
                    let frame_i = self.buffer_pool.fetch_page(new_id)?;
                    {
                        let mut frame = self.buffer_pool.frame_write(frame_i);
                        frame.data = CatalogPage::new(new_id).serialize()?;
                    }
                    self.buffer_pool.unpin_page(new_id, true);
                    page.next_catalog_page = new_id;
                }
            } else {
                orphan_head = page.next_catalog_page;
                page.next_catalog_page = 0;
            }

            let next = page.next_catalog_page;

            {
                let mut frame = self.buffer_pool.frame_write(frame_i);
                frame.data = page.serialize()?;
            }
            self.buffer_pool.unpin_page(curr_page_id, true);

            if !has_more {
                let mut free_id = orphan_head;
                while free_id != 0 {
                    let frame_i = self.buffer_pool.fetch_page(free_id)?;
                    let next_orphan = {
                        let frame = self.buffer_pool.frames[frame_i].read().unwrap();
                        CatalogPage::deserialize(frame.data)?.next_catalog_page
                    };

                    self.buffer_pool.unpin_page(free_id, false);
                    self.buffer_pool.delete_page(free_id)?;
                    free_id = next_orphan;
                }
                break;
            }
            curr_page_id = next;
        }
        Ok(())
    }

    // traverses catalog pages and loads into hashmap
    pub fn load(&mut self) -> Result<(), FerroError> {
        let mut curr_page_id = self.first_catalog_page_id;
        loop{
            let frame_i = self.buffer_pool.fetch_page(curr_page_id)?;
            let cat_page = {
                let frame = self.buffer_pool.frames[frame_i].read().unwrap();
                CatalogPage::deserialize(frame.data)?
            };
            self.buffer_pool.unpin_page(curr_page_id, false);
            for entry in cat_page.entries {
                self.tables.insert(entry.name.clone(), entry);
            }
            if cat_page.next_catalog_page == 0 {
                break;
            }
            curr_page_id = cat_page.next_catalog_page;
        }
        // Seed the shared root cells from the records just loaded.
        self.sync_root_cells();
        // `load` replaces `tables` wholesale, so anything cached against this catalog is stale.
        self.epoch += 1;
        Ok(())
    }

    pub fn analyze(&mut self, table: &str) -> Result<(), FerroError> {
        let entry = self.tables.get(table).ok_or(FerroError::KeyNotFound)?;
        let num_cols = entry.schema.columns.len();
        let hfm = HeapFileManager::open(entry.first_directory_page_id, self.buffer_pool.clone());
        let mut row_count: usize = 0;
        let mut per_col: Vec<Vec<Value>> = vec![Vec::new(); num_cols];
        let mut nulls = vec![0usize; num_cols];

        for item in hfm.scan() {
            let (_, tuple) = item?;
            let vals = tuple.deserialize(&entry.schema)?;
            row_count += 1;
            for (i, v) in vals.into_iter().enumerate() {
                if matches!(v, Value::Null) {
                    nulls[i] += 1;
                } else {
                    per_col[i].push(v);
                }
            }
        }

        let columns: Vec<ColumnStats> = per_col.into_iter().enumerate().map(|(i, mut vals)| {
            vals.sort();
            let min = vals.first().cloned();
            let max = vals.last().cloned();
            vals.dedup();
            ColumnStats {distinct: vals.len(), nulls: nulls[i], min, max}
        }).collect();
        self.stats.insert(table.to_string(), TableStats { row_count, columns});
        // Stats feed the planner, so a reader's cached snapshot must be told.
        self.epoch += 1;
        Ok(())
    }

    pub fn table_stats(&self, table: &str) -> Option<&TableStats> {
        self.stats.get(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;
    use std::sync::Arc;
    use crate::storage::disk_manager::DiskManager;
    use crate::catalog::column::Column;
    use crate::catalog::column::DataType;

    fn setup_catalog() -> Catalog {
        setup_catalog_with_disk().0
    }

    /// As [`setup_catalog`], keeping the `DiskManager` so a test can watch the high-water mark.
    fn setup_catalog_with_disk() -> (Catalog, Arc<DiskManager>) {
        let file = tempfile().expect("Failed to create temporary test file");
        let disk_manager = Arc::new(DiskManager::new(file).expect("Failed to create DiskManager"));
        let bp = Arc::new(BufferPoolManager::new(disk_manager.clone()));
        (Catalog::create(bp).unwrap(), disk_manager)
    }
    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Integer, // Adjust to match your exact DataType enum variant
                nullable: false,
            },
            Column {
                name: "age".to_string(),
                data_type: DataType::Integer, // Adjust to match your exact DataType enum variant
                nullable: false,
            },
        ])
    }

    #[test]
    fn test_catalog_create_open() {
        let mut catalog = setup_catalog();
        assert_eq!(catalog.first_catalog_page_id, 1);
        
        catalog.tables.insert("test_table".to_string(), TableEntry { name: "test_table".to_string(), first_directory_page_id: 2, primary_index_root: 3, schema: create_test_schema(), indexes: vec![], fulltext_indexes: vec![], time_travel_root: 1});
        catalog.persist().unwrap();
        let opened_catalog = Catalog::open(catalog.buffer_pool, catalog.first_catalog_page_id).unwrap();
        assert_eq!(opened_catalog.tables.len(), 1);
        assert!(opened_catalog.tables.get("test_table").is_some());
    }

    #[test]
    fn test_get_table() {
        let mut catalog = setup_catalog();
        let schema = create_test_schema();
        catalog.create_table("users".to_string(), schema).unwrap();

        let table = catalog.get_table("users").expect("");
        assert_eq!(table.name, "users");
        assert!(table.first_directory_page_id > 0);
        assert!(table.primary_index_root > 0);
        assert!(table.indexes.is_empty());
        // E67 changed this variant deliberately: creating a table that exists used to answer
        // `KeyNotFound`, which renders as "key wasn't found" - something is MISSING - for a condition
        // where something is present. The assertion is stronger than it was, not weaker: it now pins
        // the class AND requires the message to name the table the caller collided with.
        let duplicate_res = catalog.create_table("users".to_string(), create_test_schema());
        let err = duplicate_res.expect_err("a duplicate table name was accepted");
        assert!(
            matches!(err, FerroError::Constraint(_)),
            "a name collision must not be reported as a missing key: {err:?}"
        );
        let msg = format!("{err}");
        assert!(msg.contains("'users'"), "the refusal does not name the table: {msg}");
        assert!(msg.contains("already exists"), "the refusal does not say what is wrong: {msg}");
    }

    #[test]
    fn test_drop_table(){
        let mut catalog = setup_catalog();
        catalog.create_table("users".to_string(), create_test_schema()).unwrap();
        catalog.drop_table("users").unwrap();
        assert!(catalog.get_table("users").is_none());
    }

    /// **A dropped table gives its pages back — including the time-travel heap.**
    ///
    /// E69: `drop_table` freed the data heap, the primary tree and every secondary tree, and **not the
    /// time-travel heap**. Every table has one, `UPDATE` and `DELETE` push old versions into it, and
    /// dropping the table leaked all of it. Nothing had noticed because nothing called `drop_table`
    /// until `DROP TABLE` reached the SQL surface: it had no caller outside its own unit test, and that
    /// test only asserted the catalog entry was gone.
    ///
    /// **The instrument is file growth, not a counter**, because freeing a page does not lower the
    /// high-water mark - `high_water_does_not_regress_after_a_free` in `disk_manager` pins that
    /// deliberately. What a leak changes is whether the NEXT table can reuse those pages. So this
    /// builds a table, drops it, builds an identical one, and requires the file not to have grown: on
    /// the second pass every page it needs is one the first pass returned.
    #[test]
    fn dropping_a_table_returns_its_pages_including_the_time_travel_heap() {
        let (mut catalog, disk) = setup_catalog_with_disk();

        // Cycle once to absorb any one-off growth (bitmap pages, the catalog's own page), so the
        // comparison below is between two steady-state cycles rather than against a cold file.
        catalog.create_table("t".to_string(), create_test_schema()).unwrap();
        catalog.create_index("t", "age").unwrap();
        catalog.drop_table("t").unwrap();

        let baseline = disk.bitmap_high_water().unwrap();
        assert!(baseline > 0, "no pages were ever allocated, so this measures nothing");

        catalog.create_table("t".to_string(), create_test_schema()).unwrap();
        catalog.create_index("t", "age").unwrap();
        let peak = disk.bitmap_high_water().unwrap();
        catalog.drop_table("t").unwrap();

        catalog.create_table("t".to_string(), create_test_schema()).unwrap();
        catalog.create_index("t", "age").unwrap();
        let after = disk.bitmap_high_water().unwrap();

        assert_eq!(
            after, peak,
            "rebuilding an identical table after a DROP pushed the high-water mark from {peak} to \
             {after}, so the drop did not return every page it took. The time-travel heap is the one \
             that used to be missed."
        );
    }

    /// A dropped table must not leave its statistics behind for the next table of the same name.
    ///
    /// E69: `drop_table` removed the entry from `self.tables` and left `self.stats` alone, so a table
    /// recreated under the same name inherited a stranger's row counts and the optimizer planned
    /// against them. `build_index_scan` chooses between an index and a sequential scan on exactly those
    /// numbers.
    #[test]
    fn dropping_a_table_forgets_its_statistics() {
        let mut catalog = setup_catalog();
        catalog.create_table("t".to_string(), create_test_schema()).unwrap();
        catalog.analyze("t").unwrap();
        assert!(catalog.stats.contains_key("t"), "ANALYZE recorded nothing, so this is vacuous");

        catalog.drop_table("t").unwrap();
        assert!(
            !catalog.stats.contains_key("t"),
            "the dropped table's statistics survived it; a table recreated under this name would be \
             planned against the old table's row counts"
        );
    }

    #[test]
    fn test_create_index() {
        let mut catalog = setup_catalog();
        catalog.create_table("users".to_string(), create_test_schema()).unwrap();
        catalog.create_index("users", "age").expect("");
        let table = catalog.get_table("users").unwrap();
        assert_eq!(table.indexes.len(), 1);
        assert_eq!(table.indexes[0].column_name, "age");
        assert!(table.indexes[0].root_page_id > 0);

        let dup_res = catalog.create_index("users", "age");
        assert!(matches!(dup_res, Err(FerroError::IndexAlreadyExists)));
    }

    #[test]
    fn test_update_roots() {
        let mut catalog = setup_catalog();
        catalog.create_table("users".to_string(), create_test_schema()).unwrap();
        catalog.create_index("users", "age").expect("");
        catalog.update_primary_root("users", 999).unwrap();
        assert_eq!(catalog.get_table("users").unwrap().primary_index_root, 999);

        catalog.update_index_root("users", "age", 888).unwrap();
        let index_info = catalog.get_table("users").unwrap().indexes.iter().find(|i| i.column_name == "age").unwrap();
        assert_eq!(index_info.root_page_id, 888);
    }

    #[test]
    fn test_persist_orphan_removal() {
        let mut catalog = setup_catalog();
        
        for i in 0..200 {
            catalog.tables.insert(
                format!("table_{}", i),
                TableEntry { name: format!("table_{}", i), first_directory_page_id: i, primary_index_root: i + 1, schema: create_test_schema(), indexes: vec![], fulltext_indexes: vec![], time_travel_root: 1 }
            );
        }
        catalog.persist().unwrap();
        let mut loaded_catalog = Catalog::open(catalog.buffer_pool.clone(), 1).unwrap();
        assert_eq!(loaded_catalog.tables.len(), 200);

        for i in 10..200 {
            loaded_catalog.tables.remove(&format!("table_{}", i));
        }
        loaded_catalog.persist().unwrap();
        let final_catalog = Catalog::open(catalog.buffer_pool.clone(), 1).unwrap();
        assert_eq!(final_catalog.tables.len(), 10);
    }

    #[test]
    fn test_create_table_adds_time_travel_root() {
        let mut catalog = setup_catalog();
        catalog.create_table("t".to_string(), create_test_schema()).unwrap();
        let e = catalog.get_table("t").unwrap();
        assert_ne!(e.time_travel_root, 0);
        assert_ne!(e.time_travel_root, e.first_directory_page_id);
        catalog.persist().unwrap();
        let f_c = Catalog::open(catalog.buffer_pool.clone(), 1).unwrap();
        assert_ne!(f_c.get_table("t").unwrap().time_travel_root, 0);
    }
}

