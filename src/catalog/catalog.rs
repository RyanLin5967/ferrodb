use std::collections::{BTreeSet, HashMap};
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

/// **Which index of a table a shared root cell belongs to: its KIND and its column** (D208).
///
/// A B-tree index and a full-text index may cover the same column (each `create_*` refuses only a
/// duplicate of its own kind), and they are two different trees. Keyed by the column alone they
/// shared ONE cell. The first index created seeded it, the other opened the first one's tree
/// through it, and that index's next write repointed its durable record there as well, orphaning
/// its own tree. A full-text index created second lost every posting it was built with. A B-tree
/// index created second lost every row it was built over, and so did any B-tree index after a
/// rebuild, which stored the posting tree's root into the one cell last
/// (`tests/root_cell_is_per_index.rs`).
///
/// The column is generic so that one definition serves both the lookup (`IndexTree<&str>`, no
/// allocation at the caller) and the map key (`IndexTree<String>`). The primary index is the
/// `None` beside it in [`Catalog::root_cell`], so a primary index "on a column" cannot be written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IndexTree<C> {
    /// A B-tree index, in `TableEntry::indexes`: keyed `(value, pk)`.
    Secondary(C),
    /// A full-text index, in `TableEntry::fulltext_indexes`: keyed `(token, pk)`.
    FullText(C),
}

impl<'a> IndexTree<&'a str> {
    fn owned(self) -> IndexTree<String> {
        match self {
            IndexTree::Secondary(c) => IndexTree::Secondary(c.to_string()),
            IndexTree::FullText(c) => IndexTree::FullText(c.to_string()),
        }
    }
}

impl IndexTree<String> {
    /// The borrowed form [`Catalog::root_cell`] takes.
    pub fn borrowed(&self) -> IndexTree<&str> {
        match self {
            IndexTree::Secondary(c) => IndexTree::Secondary(c.as_str()),
            IndexTree::FullText(c) => IndexTree::FullText(c.as_str()),
        }
    }
}

/// A shared root cell's key: the table, then `None` for its primary index or the index's identity.
type RootKey = (String, Option<IndexTree<String>>);

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
    /// Keyed `(table, None)` for the primary index and `(table, Some(IndexTree::..(column)))` for
    /// a secondary or full-text one: by index KIND as well as column, because the two kinds on one
    /// column are two trees. See [`IndexTree`] (D208).
    roots: HashMap<RootKey, Arc<AtomicU32>>,
    /// Bumped by every change to the SCHEMA — and deliberately **not** by a root move.
    ///
    /// A reader caches a snapshot of this catalog and re-takes it only when this number changes.
    /// Before D53 a root move had to invalidate every snapshot, because `primary_index_root` was
    /// the authoritative pointer a reader descended from; now the registry above hands out a
    /// SHARED cell that a split updates in place, so a snapshot whose recorded root is stale is
    /// still correct — `open_table` reads the cell, not the record. That is the whole reason a
    /// write statement no longer invalidates every reader's cache.
    epoch: u64,
    /// **How many catalog pages, from the first, held an entry at the last persist or load**
    /// (D229 (c)). The chain never shrinks: `persist` keeps every page linked, writes an empty image
    /// to a page that has just stopped holding entries, and stops there, so it rewrites the used
    /// pages and not the chain's peak length (the review's repair of R0).
    ///
    /// Shared by clones, as the chain it counts is: a clone that persisted would write the same
    /// pages. Deliberately not serialized; `load` recounts it from the chain.
    catalog_pages_used: Arc<AtomicU32>,
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
        Ok(Self {tables: HashMap::new(), buffer_pool: buffer_pool.clone(), first_catalog_page_id: 1, stats: HashMap::new(), table_versions: HashMap::new(), roots: HashMap::new(), epoch: 0, catalog_pages_used: Arc::new(AtomicU32::new(1))})
    }

    pub fn open(buffer_pool: Arc<BufferPoolManager>, first_catalog_page_id: u32) -> Result<Self, FerroError>{
        let mut catalog = Self {tables: HashMap::new(), buffer_pool, first_catalog_page_id, stats: HashMap::new(), table_versions: HashMap::new(), roots: HashMap::new(), epoch: 0, catalog_pages_used: Arc::new(AtomicU32::new(1))};
        catalog.load()?;
        Ok(catalog)
    }

    /// The SHARED root cell for a tree, or `None` if this catalog has never seen it.
    ///
    /// `index` is `None` for the table's primary index, and otherwise names the index by kind and
    /// column: a B-tree index and a full-text index on one column are two trees with two cells.
    ///
    /// Read-only and lock-free: the map is populated by [`Catalog::sync_root_cells`] from the
    /// `&mut self` paths, so a reader holding only `&Catalog` does a plain hash lookup.
    pub fn root_cell(&self, table: &str, index: Option<IndexTree<&str>>) -> Option<Arc<AtomicU32>> {
        self.roots.get(&(table.to_string(), index.map(|i| i.owned()))).cloned()
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
    ///
    /// **Retires the cell of every tree the catalog no longer records, one index at a time.** A
    /// cell kept for a tree that is gone would be inherited by the next tree created under the same
    /// key, since the step above never overwrites. Retiring by whole table covered `DROP TABLE`
    /// only. By key it also covers one index leaving while the other kind on the same column stays:
    /// that one's cell is still wanted, so it is kept, `Arc` and all (D208).
    ///
    /// ⚠ **The retire runs only when this does.** A DROP that fails at its persist returns before
    /// its sync, and the dead key keeps its cell. The CREATE side is what closes that
    /// (`install_fresh_cell`, F4 of the D208 review), not this.
    pub fn sync_root_cells(&mut self) {
        let mut want: Vec<(RootKey, u32)> = Vec::new();
        for (name, entry) in self.tables.iter() {
            want.push(((name.clone(), None), entry.primary_index_root));
            for idx in entry.indexes.iter() {
                want.push(((name.clone(), Some(IndexTree::Secondary(idx.column_name.clone()))), idx.root_page_id));
            }
            for ft in entry.fulltext_indexes.iter() {
                want.push(((name.clone(), Some(IndexTree::FullText(ft.column_name.clone()))), ft.root_page_id));
            }
        }
        let live: std::collections::HashSet<&RootKey> = want.iter().map(|(key, _)| key).collect();
        self.roots.retain(|key, _| live.contains(key));
        for (key, root) in want {
            self.roots.entry(key).or_insert_with(|| Arc::new(AtomicU32::new(root)));
        }
    }

    /// The root a tree IS at: its shared cell's value, or the record for a tree the catalog never
    /// gave a cell. **One source of truth** (D208 review 2, C4): every statement descends the cell,
    /// and a record can LAG it. An INSERT that split a root through the shared handle and then
    /// failed before its root sync leaves the record on the pre-split page. That page is now the new
    /// root's LEFT CHILD: a leaf if the tree was one level deep, an internal node if it was deeper
    /// (D208 review 3, C3). Anything that reads a whole tree from its root must read it from here.
    pub(crate) fn live_root(&self, table: &str, index: Option<IndexTree<&str>>, recorded: u32) -> u32 {
        self.root_cell(table, index).map_or(recorded, |cell| cell.load(Ordering::Acquire))
    }

    /// Bring every INDEX record of `table` up to its shared cell (D208 review 2, C4). This is the rule
    /// ALTER's `finish` follows for the primary index (D214), applied to the B-tree and full-text
    /// indexes.
    ///
    /// ALTER's one persist writes every record of the table, so a record that lagged its cell was
    /// written as it was. In memory that is harmless, because statements descend the cell. But the
    /// persisted record is what an open that does not rebuild would seed its cell from. `pub(crate)`
    /// for `catalog::alter`. It never writes a cell. (The recovery rebuild used it too, to free each
    /// old tree from a current record; since D229 it frees no tree at all.)
    pub(crate) fn catch_up_index_records(&mut self, table: &str) {
        let Some(entry) = self.tables.get_mut(table) else { return };
        for idx in entry.indexes.iter_mut() {
            if let Some(cell) = self.roots.get(&(table.to_string(), Some(IndexTree::Secondary(idx.column_name.clone())))) {
                idx.root_page_id = cell.load(Ordering::Acquire);
            }
        }
        for ft in entry.fulltext_indexes.iter_mut() {
            if let Some(cell) = self.roots.get(&(table.to_string(), Some(IndexTree::FullText(ft.column_name.clone())))) {
                ft.root_page_id = cell.load(Ordering::Acquire);
            }
        }
    }

    /// Install a FRESH shared cell for a tree the caller has just created and made durable (F4 of
    /// the D208 review).
    ///
    /// `sync_root_cells` never overwrites, and it retires a dead key only when it runs. A DROP
    /// whose persist fails returns before its sync, and so leaves the dead tree's cell under the
    /// key. A CREATE of the same key then inherited that cell and descended a FREED tree. A
    /// brand-new tree has no legitimate holder, so replacing whatever is there is always right,
    /// and it makes the inheritance unrepresentable instead of depending on a retire having run.
    ///
    /// The root is read from the RECORD the caller has just written, so the cell cannot disagree
    /// with it, however the caller computed it. A missing record installs nothing:
    /// `sync_root_cells` then retires the key.
    fn install_fresh_cell(&mut self, table: &str, index: Option<IndexTree<&str>>) {
        let Some(entry) = self.tables.get(table) else { return };
        let root = match index {
            None => Some(entry.primary_index_root),
            Some(IndexTree::Secondary(c)) => entry.indexes.iter().find(|i| i.column_name == c).map(|i| i.root_page_id),
            Some(IndexTree::FullText(c)) => entry.fulltext_indexes.iter().find(|i| i.column_name == c).map(|i| i.root_page_id),
        };
        if let Some(root) = root {
            self.roots.insert((table.to_string(), index.map(|i| i.owned())), Arc::new(AtomicU32::new(root)));
        }
    }

    /// Carry a renamed column's index cells to its new name — D208, the rename exit.
    ///
    /// The cell is keyed by column NAME, so a rename that left it behind broke the key's claim to
    /// be the index's identity, twice over. The renamed index found no cell and opened a private
    /// root (the D53 hazard) until the next `sync_root_cells` seeded a second cell for the same
    /// tree. And an index later built on a new column taking the old name INHERITED the renamed
    /// index's tree through the old cell, because `sync_root_cells` never overwrites
    /// (`tests/root_cell_is_per_index.rs::an_index_on_a_reused_column_name_does_not_inherit_the_renamed_columns_tree`).
    ///
    /// Moved, not re-created: a statement holding the `Arc` keeps following the same tree, and
    /// there stays one cell per tree, which is D205's reason for storing into cells rather than
    /// replacing them. `pub(crate)` for `catalog::alter`, which calls it under the exclusive
    /// catalog lock in the rename chain's own order, so `a -> b` then `b -> c` ends at `c`.
    pub(crate) fn rename_root_cells(&mut self, table: &str, from: &str, to: &str) {
        for (old, new) in [
            (IndexTree::Secondary(from), IndexTree::Secondary(to)),
            (IndexTree::FullText(from), IndexTree::FullText(to)),
        ] {
            if let Some(cell) = self.roots.remove(&(table.to_string(), Some(old.owned()))) {
                self.roots.insert((table.to_string(), Some(new.owned())), cell);
            }
        }
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
        // A brand-new tree gets a brand-new cell, whatever a dead tree left under the key.
        self.install_fresh_cell(&name, None);
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

        let hfm = HeapFileManager::open(first_dir_page_id, self.buffer_pool.clone());
        for item in hfm.scan() {
            let (_, tuple) = item?;
            let values = tuple.deserialize(&schema)?;
            let sec_value = values[col_index].clone();
            let primary_key = values[0].clone();   // first column = primary key
            sec_tree.insert((sec_value, primary_key), ())?;
        }
        // D222 — the root is read AFTER the backfill. A backfill that splits the root moves it to a
        // new page, and the page `create` returned is left as the leftmost leaf: recording that
        // seeded the shared cell with one leaf, so a lookup past the leaf walk's 64 hops missed and
        // an INSERT landed in that leaf whatever its key (`tests/d222_index_root_after_backfill.rs`).
        let new_root_id = sec_tree.root_page_id.load(Ordering::Relaxed);

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
        self.install_fresh_cell(&table, Some(IndexTree::Secondary(column)));
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
    ///    ⚠ Since the reused-key fix (`execution::insert`), SQL writes the new version into the
    ///    dead one's slot, so a heap written after it holds one slot per key. A heap written
    ///    before it still holds two, and the probe is what keeps this correct over one.
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

        let hfm = HeapFileManager::open(first_dir_page_id, self.buffer_pool.clone());
        for item in hfm.scan() {
            let (_, tuple) = item?;
            let values = tuple.deserialize(&schema)?;
            let primary_key = values[0].clone();   // first column = primary key
            if let Some(text) = indexed_text(&values[col_index])? {
                post_tokens(&ft_tree, text, &primary_key)?;
            }
        }
        // D222 — after the backfill, for the reason `create_index` gives.
        let new_root_id = ft_tree.root_page_id.load(Ordering::Relaxed);

        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.fulltext_indexes.push(FullTextIndexInfo { column_name: column.to_string(), root_page_id: new_root_id });

        // Undo: pop the full-text index this call pushed. Same reason as `create_index`.
        let table = table.to_string();
        self.persist_or_undo(|c| {
            if let Some(e) = c.tables.get_mut(&table) {
                e.fulltext_indexes.pop();
            }
        })?;
        self.install_fresh_cell(&table, Some(IndexTree::FullText(column)));
        // The set of trees changed, so seed (or retire) their shared root cells, and tell
        // every cached reader snapshot that the schema moved.
        self.sync_root_cells();
        self.epoch += 1;
        Ok(())
    }

    /// Every page a table holds, ascending: both heaps (directory pages and the pages they list)
    /// and every node of its primary, B-tree and full-text trees. Reads only.
    ///
    /// This is what a DROP names in its durable intent BEFORE it unlinks the table (D229), because
    /// the entry is the only record of where those pages are, and the process that trusts the
    /// trees is this one.
    ///
    /// **E69's gaps stay closed here:** the time-travel heap is a table's larger heap once it has
    /// update history (E69), and the full-text trees are listed beside the B-tree ones (B8). Every
    /// tree is read from its LIVE root, its shared cell, and not from the record (D208 review 2,
    /// C4). A record that lags its cell names the pre-split page, which is now the new root's left
    /// child (a leaf if the tree was one level deep, an internal node otherwise; D208 review 3,
    /// C3), and reading from it misses the rest of the tree (`tests/root_cell_is_per_index.rs`, T12).
    ///
    /// Refuses a page two of the table's structures name: freeing it for this table would free it
    /// under its other owner too.
    ///
    /// **Blind spot, stated (D229 review 2's R6): an alias ACROSS tables is not refused.** Only this
    /// table's structures are walked, and no page records its owner (a heap page stores its own id, a
    /// tree node nothing of its table). A check would need every other table walked, O(database pages),
    /// or an owner stamp in every page header, a format change. Neither fits within a DROP's O(table
    /// pages).
    pub fn table_pages(&self, name: &str) -> Result<Vec<u32>, FerroError> {
        let entry = self.require_table(name)?;
        let mut pages = BTreeSet::new();
        HeapFileManager::open(entry.first_directory_page_id, self.buffer_pool.clone()).collect_pages(&mut pages)?;
        HeapFileManager::open(entry.time_travel_root, self.buffer_pool.clone()).collect_pages(&mut pages)?;
        let primary = self.live_root(name, None, entry.primary_index_root);
        BPlusTreeManager::<Value, RecordId>::open(primary, self.buffer_pool.clone()).collect_pages(&mut pages)?;
        let index_roots = entry
            .indexes
            .iter()
            .map(|i| self.live_root(name, Some(IndexTree::Secondary(&i.column_name)), i.root_page_id))
            .chain(entry.fulltext_indexes.iter().map(|i| self.live_root(name, Some(IndexTree::FullText(&i.column_name)), i.root_page_id)));
        for root in index_roots {
            BPlusTreeManager::<(Value, Value), ()>::open(root, self.buffer_pool.clone()).collect_pages(&mut pages)?;
        }
        Ok(pages.into_iter().collect())
    }

    /// Remove a table from the catalog. **Frees nothing** (D229).
    ///
    /// Its pages are freed by the DROP that calls this, and only after the DDL's checkpoint has
    /// flushed and synced the removal, whether or not that checkpoint then truncates the log (a WAL
    /// pin can cancel the truncation, and D250's recovery skips the dropped table's records):
    /// `TxnManager::drop_checkpointed`, with the pages from [`Catalog::table_pages`] named in a
    /// durable intent BEFORE this runs. Freeing here, before the removal was durable, was what let a
    /// crash leave a table named over free pages, and a free that failed part-way leave one in the
    /// same process (`frontier/d229_design.md` §2 (b), (f)).
    ///
    /// E69, which found the time-travel heap never freed, also found two gaps that stay closed here:
    /// `self.stats` kept the dropped table's row counts, so a table recreated under the same name
    /// was planned against a stranger's data; and the missing-table error was a bare `KeyNotFound`.
    pub fn drop_table(&mut self, name: &str) -> Result<(), FerroError> {
        self.require_table(name)?;
        self.tables.remove(name);
        self.stats.remove(name);
        self.persist()?;
        // The set of trees changed, so seed (or retire) their shared root cells, and tell
        // every cached reader snapshot that the schema moved.
        self.sync_root_cells();
        self.epoch += 1;
        Ok(())
    }

    /// **D250: finish a DROP that the log records but the catalog on disk does not**, without freeing
    /// anything. Called only by `wal::recovery::open_recovered`, for a table whose `DropTable` record
    /// is durable (it is written before the DROP's mutation) while the catalog change never reached
    /// disk. Recovery has skipped the table's records, so it must not stay in the catalog.
    ///
    /// Its pages are not freed here. With D229 none of them was freed before the crash either: a
    /// DROP frees only after the checkpoint that makes this very removal durable. If the DROP recorded
    /// its intent, `open_recovered` decides it after this (the table is now absent) and the intent frees
    /// the pages after the open's checkpoint; they were quarantined before recovery, so recovery took
    /// none. The intent is recorded before the `DropTable` record, so every DROP this build logs has
    /// one, and D250's stated cost (a completed DROP leaks its pages) is closed for them. Only a DROP
    /// logged by a build without D229 leaks its pages here. The rest is what [`Catalog::drop_table`]
    /// does.
    pub fn forget_dropped_table(&mut self, name: &str) -> Result<(), FerroError> {
        self.drop_table(name)
    }

    // root split propagation called when a tree's root changes
    pub fn update_primary_root(&mut self, table: &str, new_root: u32) -> Result<(), FerroError> {
        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.primary_index_root = new_root;
        self.persist()?;
        // Keep the SHARED cell in step with the durable record. A split stores into the cell and
        // then calls this, so the two usually already agree; a caller that sets the root directly
        // (tests) would otherwise leave the cell pointing at the old tree. ALTER does not come
        // through here: it plans from the cell, rewrites through it, and its `finish` writes the
        // record FROM the cell, in its one persist (D214).
        if let Some(cell) = self.roots.get(&(table.to_string(), None)) {
            cell.store(new_root, Ordering::Release);
        }
        Ok(())
    }

    pub fn update_index_root(&mut self, table: &str, column: &str, new_root: u32) -> Result<(), FerroError> {
        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.indexes.iter_mut().find(|ind| ind.column_name == column).ok_or(FerroError::KeyNotFound)?.root_page_id = new_root;
        self.persist()?;
        if let Some(cell) = self.roots.get(&(table.to_string(), Some(IndexTree::Secondary(column.to_string())))) {
            cell.store(new_root, Ordering::Release);
        }
        Ok(())
    }

    /// B8 — the `update_index_root` of the full-text list. Separate because the two lists are
    /// separate: resolving a full-text column name inside `indexes` would either miss (and leave a
    /// split tree's new root unrecorded, losing every posting added since) or hit a same-named
    /// B-tree index and overwrite ITS root with a token tree's. The cell is the full-text one for
    /// the same reason (D208): keyed by column alone, this store landed in the B-tree index's cell
    /// whenever that index had been created first.
    pub fn update_fulltext_root(&mut self, table: &str, column: &str, new_root: u32) -> Result<(), FerroError> {
        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.fulltext_indexes.iter_mut().find(|ind| ind.column_name == column).ok_or(FerroError::KeyNotFound)?.root_page_id = new_root;
        self.persist()?;
        if let Some(cell) = self.roots.get(&(table.to_string(), Some(IndexTree::FullText(column.to_string())))) {
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

    /// Write every entry into the catalog chain, in the pool.
    ///
    /// **The chain never shrinks (D229 (c), closes D238).** Entries are packed from the first page
    /// on, in name order. A page that stops holding entries is written as an empty catalog page and
    /// KEPT in the chain, linked; nothing here frees a page. The chain used to be cut after the
    /// last page with entries and the rest freed at once, before the shortened chain was durable,
    /// so a crash could leave page 1 on disk still linking a page `allocate` then handed to someone
    /// else and zero-wrote, and the next `load` refused it: the database stopped opening
    /// (`frontier/d229_candidates_adversary.md` (e)).
    ///
    /// **It walks only as far as it must:** past every page that holds an entry now, and past every
    /// page that held one at the last persist or load (`catalog_pages_used`), so a page that has just
    /// become empty is written empty. Pages beyond both are empty already and are not rewritten, so
    /// a persist after a mass DROP costs the pages in use, not the chain's peak length (the review's
    /// repair of R0).
    ///
    /// ⚠ **Not crash-atomic across pages**, and not made so here: a persist that moves entries
    /// between pages, cut off part-way, can leave an entry on two pages or on none. `load` resolves
    /// the first (the earlier page's copy wins); the second is D240, a separate row.
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
        let was_used = self.catalog_pages_used.load(Ordering::Acquire).max(1);
        let mut written = 0u32;
        let mut used = 1u32;

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
            if has_more && page.next_catalog_page == 0 {
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

            let next = page.next_catalog_page;
            written += 1;
            if !page.entries.is_empty() {
                used = written;
            }

            {
                let mut frame = self.buffer_pool.frame_write(frame_i);
                frame.data = page.serialize()?;
            }
            self.buffer_pool.unpin_page(curr_page_id, true);
            // Raised as each page is written, so a persist that fails part-way still leaves the
            // count covering every page it put entries on, and the next one rewrites them.
            self.catalog_pages_used.fetch_max(used, Ordering::AcqRel);

            if next == 0 || (!has_more && written >= was_used) {
                break;
            }
            curr_page_id = next;
        }
        self.catalog_pages_used.store(used, Ordering::Release);
        Ok(())
    }

    /// Read the catalog chain into `tables`.
    ///
    /// **A table named on two pages is loaded from the EARLIER page** (D229 (c); the candidates
    /// adversary's caveat on R0). Now that the chain never shrinks, a persist that moves an entry
    /// to an earlier page leaves its old copy on a later page until that page is rewritten.
    /// `flush_all` writes in ascending page id and page 1 is the lowest, so a crash in between
    /// leaves page 1 new and the tail old, and the earlier copy is the newer one. The later copy
    /// used to win (`HashMap::insert` in chain order), root and all.
    ///
    /// It also recounts `catalog_pages_used`: up to the last page with an entry, which is where
    /// the next persist must write to.
    pub fn load(&mut self) -> Result<(), FerroError> {
        let mut curr_page_id = self.first_catalog_page_id;
        let mut loaded: HashMap<String, TableEntry> = HashMap::new();
        let mut position = 0u32;
        let mut used = 1u32;
        loop{
            let frame_i = self.buffer_pool.fetch_page(curr_page_id)?;
            let cat_page = {
                let frame = self.buffer_pool.frames[frame_i].read().unwrap();
                CatalogPage::deserialize(frame.data)?
            };
            self.buffer_pool.unpin_page(curr_page_id, false);
            position += 1;
            if !cat_page.entries.is_empty() {
                used = position;
            }
            for entry in cat_page.entries {
                loaded.entry(entry.name.clone()).or_insert(entry);
            }
            if cat_page.next_catalog_page == 0 {
                break;
            }
            curr_page_id = cat_page.next_catalog_page;
        }
        self.tables.extend(loaded);
        self.catalog_pages_used.store(used, Ordering::Release);
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

    /// Every page `name`'s structures name, walked here from the page formats into a `Vec` with one
    /// entry per structure that names the page. Nothing de-duplicates ACROSS structures, so a page two
    /// of them name is in it twice. Within one tree a node reached twice (a leaf by its parent and by
    /// its left sibling) is one node, as it is for `collect_pages`. Record roots, not shared cells:
    /// the callers split no tree.
    fn raw_pages(catalog: &Catalog, name: &str) -> Vec<u32> {
        use crate::storage::index_page::BPlusTreePage;
        use crate::storage::page_directory::PageDirectory;
        let bp = catalog.buffer_pool.clone();
        let e = catalog.get_table(name).unwrap().clone();
        let mut out = Vec::new();
        for first in [e.first_directory_page_id, e.time_travel_root] {
            let mut id = first;
            while id != 0 {
                out.push(id);
                let frame_i = bp.fetch_page(id).unwrap();
                let dir = PageDirectory::deserialize(bp.frames[frame_i].read().unwrap().data);
                bp.unpin_page(id, false);
                out.extend(dir.entries.iter().map(|x| x.page_id));
                id = dir.next_page_directory;
            }
        }
        let primary = BPlusTreeManager::<Value, RecordId>::open(e.primary_index_root, bp.clone());
        let (mut mine, mut stack) = (BTreeSet::new(), vec![e.primary_index_root]);
        while let Some(id) = stack.pop() {
            if mine.insert(id) {
                out.push(id);
                match primary.read_node(id).unwrap() {
                    BPlusTreePage::Internal(n) => stack.extend(n.child_ptrs),
                    BPlusTreePage::Leaf(l) => stack.extend(l.next),
                }
            }
        }
        for root in e.indexes.iter().map(|i| i.root_page_id).chain(e.fulltext_indexes.iter().map(|i| i.root_page_id)) {
            let tree = BPlusTreeManager::<(Value, Value), ()>::open(root, bp.clone());
            let (mut mine, mut stack) = (BTreeSet::new(), vec![root]);
            while let Some(id) = stack.pop() {
                if mine.insert(id) {
                    out.push(id);
                    match tree.read_node(id).unwrap() {
                        BPlusTreePage::Internal(n) => stack.extend(n.child_ptrs),
                        BPlusTreePage::Leaf(l) => stack.extend(l.next),
                    }
                }
            }
        }
        out
    }

    /// **`table_pages` names both heaps and every tree** (E69's gap, carried into D229: a DROP frees
    /// what this returns and nothing else, so a structure left out here is a structure leaked). The
    /// time-travel heap's directory, the primary root and the B-tree index's root are checked by
    /// id, and the list is exactly what a walk written here from the page formats finds, which
    /// names no page twice.
    ///
    /// **And a page two structures name is refused, not collected once** (D229 review 1's R6: this
    /// test's earlier no-page-twice check ran on `table_pages`' own output, which is built in a set
    /// and so could not fail). The primary root is planted in the heap's directory; the raw walk
    /// then holds it twice, and `table_pages` must refuse. Mutant-only red: M23.
    #[test]
    fn table_pages_names_both_heaps_and_every_tree() {
        let mut catalog = setup_catalog();
        catalog.create_table("t".to_string(), create_test_schema()).unwrap();
        catalog.create_index("t", "age").unwrap();
        let e = catalog.get_table("t").unwrap().clone();
        let pages = catalog.table_pages("t").unwrap();
        for (what, page) in [
            ("heap directory", e.first_directory_page_id),
            ("time-travel heap directory", e.time_travel_root),
            ("primary index root", e.primary_index_root),
            ("B-tree index root", e.indexes[0].root_page_id),
        ] {
            assert!(pages.contains(&page), "table_pages leaves out the {what}, page {page}: {pages:?}");
        }
        // The raw walk, checked before anything puts it in a set.
        let mut raw = raw_pages(&catalog, "t");
        raw.sort_unstable();
        let twice: Vec<u32> = raw.windows(2).filter(|w| w[0] == w[1]).map(|w| w[0]).collect();
        assert!(twice.is_empty(), "two of t's structures name page(s) {twice:?}: {raw:?}");
        assert_eq!(raw, pages, "table_pages is not the pages t's structures name");

        HeapFileManager::open(e.first_directory_page_id, catalog.buffer_pool.clone())
            .add_to_directory(e.primary_index_root, 0)
            .unwrap();
        let raw = raw_pages(&catalog, "t");
        assert_eq!(
            raw.iter().filter(|p| **p == e.primary_index_root).count(),
            2,
            "premise: the planted alias is not named twice in the raw walk: {raw:?}"
        );
        let refused = catalog.table_pages("t");
        assert!(refused.is_err(), "table_pages collected a page two of t's structures name, once: {refused:?}");
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

    /// **A table named on two catalog pages loads from the EARLIER page** (D229 (c); the candidates
    /// adversary's caveat on R0, `frontier/d229_candidates_adversary.md` (e)).
    ///
    /// Once the chain never shrinks, a persist that moves an entry to an earlier page leaves the
    /// entry's old copy on a later page until that page is written too. `flush_all` writes in
    /// ascending page id and page 1 is the lowest, so a crash in between leaves page 1 new and the
    /// tail old: the same table on both, the earlier copy the newer one. `load` kept the LAST copy
    /// it met, which is the stale one, root and all.
    ///
    /// FAILS at `a6e93ab` (INFERRED): `t` loads with the tail's root, 600.
    #[test]
    fn a_table_named_on_two_catalog_pages_loads_from_the_earlier_page() {
        let catalog = setup_catalog();
        let bp = catalog.buffer_pool.clone();
        let tail_id = bp.new_page().unwrap();
        let entry = |root: u32| TableEntry {
            name: "t".to_string(),
            first_directory_page_id: 2,
            primary_index_root: root,
            schema: create_test_schema(),
            indexes: vec![],
            fulltext_indexes: vec![],
            time_travel_root: 3,
        };
        let mut first = CatalogPage::new(1);
        first.add_entry(entry(700)).unwrap();
        first.next_catalog_page = tail_id;
        let mut tail = CatalogPage::new(tail_id);
        tail.add_entry(entry(600)).unwrap();
        for (id, page) in [(1, first), (tail_id, tail)] {
            let frame_i = bp.fetch_page(id).unwrap();
            bp.frame_write(frame_i).data = page.serialize().unwrap();
            bp.unpin_page(id, true);
        }

        let opened = Catalog::open(bp, 1).unwrap();
        assert_eq!(
            opened.get_table("t").expect("t did not load").primary_index_root,
            700,
            "a table on two catalog pages loaded from the later page, the copy a torn persist left stale"
        );
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

