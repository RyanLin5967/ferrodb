//! E63 — primary-key uniqueness is a question about the heap, not about the index.
//!
//! # The defect
//!
//! A deleted primary key could never be used again. Three statements through the shipped binary,
//! measured 2026-08-17:
//!
//! ```text
//! INSERT INTO t VALUES (1,10);  DELETE FROM t WHERE id = 1;  INSERT INTO t VALUES (1,99);
//! → error: duplicate primary key Integer(1) in 't': ... use UPDATE to change the existing row
//! → SELECT * FROM t;  →  (no row 1)
//! ```
//!
//! The error told the operator to UPDATE a row that `SELECT` said was not there, and no sequence of
//! statements could recover the key.
//!
//! # Why
//!
//! The check was a pure index lookup: `search(key).is_some()` ⇒ refuse. But an index entry outlives
//! the row it points at. DELETE is MVCC — it stamps `end_ts` on the version in place (see
//! `execution::delete`) and never touches the index, because the old version must stay reachable for
//! readers whose snapshot predates the delete. So the index answers "was this key ever used", and
//! uniqueness needs "is it used *now, for me*".
//!
//! # The fix, and the two things it has to get right
//!
//! Read the version the entry points at and ask the caller's [`ReadView`] whether its deletion is
//! committed *for this reader*: `end_ts != 0 && view.is_commited_for_me(end_ts)`. Both conjuncts are
//! load-bearing, and dropping either one is caught by a test:
//!
//! - Without the visibility half, a delete that has not committed — or that goes on to roll back —
//!   frees the key for everybody, and two live rows end up sharing it.
//! - The stale entry must be **removed**, not shadowed. `insert_entry` appends at the binary-search
//!   position rather than overwriting, so leaving the dead entry puts two entries for one key in a
//!   unique index. `search` then returns whichever binary search lands on — the dead one — and the
//!   *next* insert of that key reads `end_ts != 0`, concludes the row is gone, and admits a genuine
//!   duplicate. Measured with the removal commented out: `INSERT (1,10); DELETE id=1; INSERT (1,99);
//!   INSERT (1,777)` left both `1 | 99` and `1 | 777` live.
//!
//! ⛔ **SUPERSEDED — this paragraph said:** *"Removing the entry orphans nothing. The deleted version
//! stays in the heap where a sequential scan still finds it and `ReadView::visible` still filters
//! it, and nothing needs the index to reach an old version: there is no temporal `AS OF
//! <timestamp>` in this SQL surface, only `AS OF BRANCH`."* The last clause was false. An explicit
//! transaction's snapshot IS a temporal read. See the next section.
//!
//! # The reused key must keep its old version reachable from the index
//!
//! The E63 fix wrote the new row into a NEW slot, repointed the index at it, and gave it no `prev`.
//! A reader whose snapshot predates the reuse then disagreed with itself: its sequential scan found
//! the dead version's slot, still live for it, and its lookup by key landed on the new slot, which
//! it cannot see, with nowhere to go. `WHERE id = k` returned nothing while `SELECT *` returned
//! the row. `UPDATE`/`DELETE ... WHERE id = k` affected 0 rows where the same statement by scan
//! raised a write conflict. `SecondaryIndexScan` resolves through the primary index and lost the
//! row the same way. Found by the D194 lane (artie-research
//! `frontier/lane_d194_fork_snapshot.md` §6.1). Pinned by `tests/reused_key_old_snapshot.rs`.
//!
//! So the new version now goes INTO the dead version's slot, with `prev` pointing at a
//! time-travel copy of the dead version. That is UPDATE's mechanism, and the index entry stays
//! where it was. A lookup lands on the new head and walks `prev` to the old version exactly as it
//! does after an UPDATE. The heap holds one slot per key, so a scan cannot yield the old version
//! twice.
//!
//! One line differs from UPDATE, and it is load-bearing. UPDATE stamps the archived version's
//! `end_ts` with its own id, because that version was live. A reused key's dead version is already
//! ended by its DELETE, and it is archived **verbatim**. Re-stamping it with the inserter's id
//! would make it live again for any reader that saw the DELETE commit but not the INSERT.
//! `a_reader_that_saw_the_delete_does_not_see_the_old_row_come_back` fails if that is done.
//!
//! The WAL record changes, and the change feed must not. The table's record for a reuse is now a
//! `HeapUpdate` whose old image is dead and whose new image is live. When the new row does not fit
//! and relocates, it is a `HeapDelete` of the dead image followed by a `HeapInsert`.
//! `replication::logical` decodes the `HeapUpdate` as an INSERT and counts the dead `HeapDelete` as
//! bookkeeping. Both rules read the version headers, as the DELETE rule already does.
//!
//! The B+tree does not rebalance on delete (`handle_underflow` is unimplemented and never called), so
//! this can leave a sparse leaf. Sparse is correct; the alternative was a key that could not be
//! reused.

use crate::binder::binder::BoundExpr;
use crate::catalog::catalog::Catalog;
use crate::error::FerroError;
use crate::execution::executor::{Modify, evaluate, sync_fulltext_roots, sync_roots};
use crate::storage::tuple::Tuple;
use crate::storage::heap_file_manager::HeapFileManager;
use crate::catalog::schema::Schema;
use crate::catalog::column::Value;
use crate::storage::index::BPlusTreeManager;
use crate::storage::heap_file_manager::RecordId;
use crate::execution::index_handle::{FullTextHandle, IndexHandle};
use crate::storage::index_fulltext::{indexed_text, post_tokens};
use crate::storage::index_page::{entry_refusal, first_entry_over_bound};
use crate::provenance::{ProvId, ProvenanceStore};
use std::sync::Arc;

pub struct Insert {
    pub table: String,
    pub values: Vec<BoundExpr>,
    pub heap: HeapFileManager,
    pub schema: Schema,
    pub primary_index: BPlusTreeManager<Value, RecordId>,
    pub secondary_indexes: Vec<IndexHandle>,
    /// B8 — full-text indexes on this table, maintained one posting per distinct token.
    pub fulltext_indexes: Vec<FullTextHandle>,
    /// Who to attribute the inserted version to. `None` means unattributed.
    pub author: Option<(Arc<dyn ProvenanceStore>, ProvId)>,
    /// Needed to answer "is the row this index entry points at still there?".
    ///
    /// Without it the uniqueness check could only ask the index whether a key existed, and an index
    /// entry outlives the row: DELETE stamps `end_ts` on the version in place and leaves the entry
    /// pointing at it. So a deleted primary key could never be reused.
    pub view: std::sync::Arc<crate::wal::txn::ReadView>,
    /// Where a reused key's dead version is archived, so the new version's `prev` can reach it.
    /// Opened and logged exactly as `Update::tt_heap` is.
    pub tt_heap: HeapFileManager,
}

impl Modify for Insert {
    fn set_author(&mut self, prov: Arc<dyn ProvenanceStore>, id: ProvId) {
        self.author = Some((prov, id));
    }

    /// Write the row, then record every root it moved **on every exit, not only on success** — D230.
    ///
    /// A root split is published into the shared cell the moment it happens and is not logged, so it
    /// is permanent whatever this statement returns. The primary `upsert` can split and a later
    /// secondary `insert` or `post_tokens` can still fail (the allocator refusing at the arena floor,
    /// an I/O error), and that exit used to skip `sync_roots`. The in-memory record then lagged the
    /// tree, every later `persist` of any table wrote the lagging value, and an open that does not
    /// rebuild — a clean restart after a process that ran no DDL leaves an empty log, so `recover`
    /// returns false — seeded the cell from a root that is now only the left part of the tree
    /// (`tests/d230_root_sync_on_every_exit.rs`).
    ///
    /// When both fail, the write's error is the one returned: it is why the statement failed.
    fn execute(&mut self, catalog: &mut Catalog) -> Result<usize, FerroError> {
        let written = self.write_row();
        let primary = sync_roots(&self.table, &self.schema, &self.primary_index, &self.secondary_indexes, catalog);
        let fulltext = sync_fulltext_roots(&self.table, &self.fulltext_indexes, catalog);
        let count = written?;
        primary?;
        fulltext?;
        // D69 — record that this table changed, on the SAME path as the write that
        // changed it. The merge staleness check reads this counter instead of
        // rescanning and rehashing every row (see Catalog::bump_table_version). It must
        // be bumped here and not only on the agent-merge path: the hash it replaces was
        // computed by scanning the real table, so it saw ordinary DML too.
        catalog.bump_table_version(&self.table);
        Ok(count)
    }
}

impl Insert {
    /// Everything [`Modify::execute`] does except record the roots, which `execute` does on every
    /// exit from here.
    fn write_row(&mut self) -> Result<usize, FerroError>{
        let mut vals = Vec::with_capacity(self.values.len());
        for expr in &self.values {
            vals.push(evaluate(expr, &[])?);
        }
        if vals.len() != self.schema.columns.len() {
            return Err(FerroError::Constraint(format!(
                "table '{}' has {} column(s) but {} value(s) were given; list a value for each \
                 column, in declared order",
                self.table,
                self.schema.columns.len(),
                vals.len()
            )))
        }
        for (i, col) in self.schema.columns.iter().enumerate() {
            if !col.nullable && matches!(vals[i], Value::Null) {
                return Err(FerroError::Constraint(format!(
                    "column '{}' of '{}' is declared NOT NULL, so it needs a value",
                    col.name, self.table
                )))
            }
        }
        // **D225 — every index entry this row will add must be one the trees admit, asked BEFORE
        // any of the row is written.**
        //
        // Each tree refuses an entry over `MAX_ENTRY_BYTES` by name (`admit_entry`), but it would
        // refuse too late: by the secondary and full-text writes below, the heap row and the
        // primary entry are already written. The abort undoes only the heap, so the primary entry
        // would be left pointing at a deleted slot, and every later INSERT of that key would fail
        // reading it (`SlotDeleted`) — a key refused once could never be used again. So the same
        // bound is asked here, for every entry the writes below will make, through the one builder
        // of entry shapes (`index_page::row_entry_sizes`, review 7 K9): the primary entry, each
        // secondary entry, and each posting of each distinct token.
        let secondary: Vec<usize> = self.secondary_indexes.iter().map(|h| h.col_index).collect();
        let fulltext: Vec<usize> = self.fulltext_indexes.iter().map(|h| h.col_index).collect();
        if let Some((_, len)) = first_entry_over_bound(&vals, true, &secondary, &fulltext)? {
            return Err(entry_refusal(len));
        }
        // **An index entry outlives the row it points at.** DELETE stamps `end_ts` on the version
        // in place and leaves the entry alone, so `search` finding a key does NOT mean the key is
        // taken. Asking the index alone made a deleted primary key unusable forever, and said so
        // with "use UPDATE to change the existing row" when there was no row to update.
        let mut reused: Option<(RecordId, Tuple)> = None;
        if let Some(existing) = self.primary_index.search(&vals[0])? {
            let head = self.heap.read(existing)?;
            let h = head.version_header()?;
            // Free only if a transaction that has COMMITTED FOR ME deleted it. Deliberately not
            // `ReadView::visible`: that also returns false for a row another transaction has
            // inserted but not yet committed, and treating THAT key as free would let two
            // transactions both claim it and clobber each other's index entry.
            let deleted_for_me = h.end_ts != 0 && self.view.is_commited_for_me(h.end_ts);
            if !deleted_for_me {
                return Err(FerroError::Constraint(format!(
                    "duplicate primary key {:?} in '{}': column '{}' already has that value; use \
                     UPDATE to change the existing row",
                    vals[0],
                    self.table,
                    self.schema.columns.first().map(|c| c.name.as_str()).unwrap_or("?")
                )))
            }
            // The key is free. Its dead version is where the new one goes: into the same slot,
            // with `prev` linking back to it, so the index entry that already points here reaches
            // both (module doc, "The reused key must keep its old version reachable"). Writing a
            // NEW slot instead, as this did until the reused-key fix, left the entry pointing at a
            // version no older snapshot can see, with nothing behind it.
            reused = Some((existing, head));
        }
        let mut tuple = Tuple::serialize(&vals, &self.schema, self.heap.txn_id)?;
        // What the primary entry holds before this statement: nothing for a new key, the dead
        // version's slot for a reuse. It is what a rollback must put back (D202).
        let entry_before = reused.as_ref().map(|(dead_rid, _)| *dead_rid);
        let (rid, index_moved) = match reused {
            Some((dead_rid, dead)) => {
                // Archived VERBATIM: its `end_ts` is its deleter's and must stay so. UPDATE
                // re-stamps the version it archives because that version was live; this one is
                // not, and re-stamping it with this transaction's id would revive it for every
                // reader that saw the DELETE commit and not this INSERT.
                let tt_rid = self.tt_heap.insert(dead)?;
                tuple.data[16..20].copy_from_slice(&tt_rid.page_id.to_be_bytes());
                tuple.data[20..22].copy_from_slice(&tt_rid.slot_num.to_be_bytes());
                // In place when the new row fits the dead row's page, which leaves the index
                // untouched. Otherwise `update` relocates it and the entry has to follow, exactly
                // as it does for a relocated UPDATE.
                let rid = self.heap.update(dead_rid, tuple)?;
                (rid, rid != dead_rid)
            }
            None => (self.heap.insert(tuple)?, true),
        };
        if let Some((prov, id)) = &self.author {
            prov.stamp(rid, *id)?;
        }
        // `upsert`, not `insert`, and only when the row is somewhere the index does not already
        // say. A brand-new key needs its entry written. A reused key whose new row relocated needs
        // its entry REPLACED: `insert` cannot do that because it appends, and delete-then-insert
        // could, but only through a window in which the key is absent to every lockless reader.
        //
        // ⛔ **D126 — the `delete` that used to sit in the reuse branch above MOVED INTO this
        // `upsert`.** It was `primary_index.delete(&vals[0])` there and `primary_index.insert(..)`
        // after the heap write, with `delete` dropping the leaf write latch on return: the primary
        // key was absent from the index for the whole of a heap insert, and `search` descends with
        // no latch at all. A concurrent point lookup on that key got "no such row" for a row that
        // exists. Same defect the branch catalog had, same fix: one `upsert`, one page write, no
        // window. See `BPlusTreeManager::upsert`.
        //
        // A reuse written in place writes no index page at all. The relocating case keeps the
        // window a relocated UPDATE already has: `HeapFileManager::update` frees the old slot
        // before this line repoints the entry.
        if index_moved {
            // **D202 — recorded before the write, so a rollback can take it back.** Index pages are
            // not logged, and before this a rolled-back INSERT left its key pointing at the slot
            // `undo_insert` freed: the key then failed every lookup and every INSERT with
            // `SlotDeleted`. See `TxnManager::record_primary_write`.
            if let Some(txn) = &self.heap.txn {
                txn.record_primary_write(self.heap.txn_id, self.primary_index.root_cell(), vals[0].clone(), entry_before);
            }
            self.primary_index.upsert(vals[0].clone(), rid)?;
        }
        // **E66 — the same de-duplication UPDATE needs, on the path E63 opened.**
        //
        // DELETE leaves a secondary entry behind on purpose (see `execution::update`: an older
        // snapshot still has to find the row by its value). Reusing the primary key with the SAME
        // indexed value therefore lands on an entry that already exists, and `insert_entry` appends
        // rather than overwrites - so the index gained a second identical pair and every lookup
        // through it returned the row twice. Measured before this guard: `DELETE id=4; INSERT (4,40)`
        // gave two `(40, 4)` entries and a lookup for 40 returned `[[4, 40], [4, 40]]`.
        for sec_idx in &self.secondary_indexes {
            let key = (vals[sec_idx.col_index].clone(), vals[0].clone());
            if sec_idx.tree.search(&key)?.is_none() {
                sec_idx.tree.insert(key, ())?;
            }
        }
        // **B8 — the same rule, one level finer.** A secondary index posts one entry per row; a
        // full-text index posts one per *distinct token of the value*, so a value that repeats a
        // word would post that pair twice from a single INSERT, before any DELETE or UPDATE is
        // involved. `post_tokens` carries both halves of the guard: distinct tokens, and the
        // search-before-insert probe that the re-used-primary-key case above needs.
        for ft in &self.fulltext_indexes {
            if let Some(text) = indexed_text(&vals[ft.col_index])? {
                post_tokens(&ft.tree, text, &vals[0])?;
            }
        }
        Ok(1)
    }
}
