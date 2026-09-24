use std::sync::Arc;

use crate::binder::binder::BoundExpr;
use crate::catalog::catalog::Catalog;
use crate::error::FerroError;
use crate::execution::executor::{Modify, evaluate, sync_fulltext_roots, sync_roots};
use crate::storage::tuple::Tuple;
use crate::wal::txn::ReadView;
use crate::wal::visibility::check_write_conflict;
use crate::{catalog::schema::Schema, execution::executor::Executor, storage::heap_file_manager::HeapFileManager};
use crate::storage::index::BPlusTreeManager;
use crate::storage::heap_file_manager::RecordId;
use crate::catalog::column::Value;
use crate::execution::index_handle::{FullTextHandle, IndexHandle};
use crate::storage::index_fulltext::{distinct_tokens, indexed_text, post_tokens, posting_key};
use crate::storage::index_page::{admit_entry, entry_over_bound, entry_too_large, OPEN_TABLE_REMEDY};
use crate::provenance::{ProvId, ProvenanceStore};

pub struct Update {
    pub table: String,
    pub child: Box<dyn Executor>,
    pub schema: Schema,
    pub assignments: Vec<(usize, BoundExpr)>, // col idx -> new value expr
    pub heap: HeapFileManager,
    pub primary_index: BPlusTreeManager<Value, RecordId>,
    pub secondary_indexes: Vec<IndexHandle>,
    /// B8 — full-text indexes on this table. Postings for the NEW text are added; postings for the
    /// old text stay, exactly as the secondary entries do.
    pub fulltext_indexes: Vec<FullTextHandle>,
    pub view: Arc<ReadView>,
    pub tt_heap: HeapFileManager,
    /// Who to attribute each new version to. `None` means unattributed.
    pub author: Option<(std::sync::Arc<dyn ProvenanceStore>, ProvId)>,
}

impl Modify for Update {
    fn set_author(&mut self, prov: std::sync::Arc<dyn ProvenanceStore>, id: ProvId) {
        self.author = Some((prov, id));
    }

    fn execute(&mut self, catalog: &mut Catalog) -> Result<usize, FerroError>{
        // **E65 — a semantic refusal, reported as one, with a way forward.**
        //
        // This said `FerroError::Parse("can't update primary key")`, which was wrong twice. The
        // statement parses fine and is refused on a rule about the data model, so anyone triaging
        // logs by error kind filed it with malformed SQL; and the message named no alternative.
        //
        // The restriction itself stays. Rewriting a primary key means moving every index entry that
        // points at the row and checking uniqueness against two keys at once - the old one being
        // vacated and the new one being claimed - and this executor does neither. Refusing is the
        // correct trade. Refusing without saying what to do instead is not, especially now that
        // there IS something to do: as of E63 a deleted key can be used again, so DELETE-then-INSERT
        // works rather than being advice that would have failed.
        //
        // **With one caveat (D225, review 6):** the deleted tuple is never purged. Every entry an
        // index over its values would need, including a CREATE INDEX added later, is still asked of
        // it, so a row whose value is too long for an index entry leaves a tuple that refuses that
        // index for good. The remedy then is the table copy, `index_page::OPEN_TABLE_REMEDY`.
        if let Some((col, _)) = self.assignments.iter().find(|(col, _)| *col == 0) {
            return Err(FerroError::Constraint(format!(
                "column '{}' of '{}' is the primary key and cannot be updated: moving a key means \
                 moving every index entry that points at the row and checking uniqueness against \
                 both the old and the new key at once. DELETE the row and INSERT it under the new \
                 key instead.",
                self.schema.columns.get(*col).map(|c| c.name.as_str()).unwrap_or("?"),
                self.table
            )));
        }
        let mut res = Vec::new();
        loop {
            let (rid, values) = match self.child.next() {
                Some(Ok((r, t))) => (r, t),
                Some(Err(e)) => return Err(e),
                None => break
            };
            res.push((rid, values));
        }
        // **D225 — every row's new values, and the entry bound over every index entry they will
        // add, BEFORE the first row is written.**
        //
        // Each tree refuses an entry over `MAX_ENTRY_BYTES` by name, but it would refuse after the
        // heap update, and an abort undoes only the heap: a row the heap moved would keep a primary
        // entry pointing at a deleted slot (`execution::insert` gives the same argument). Asking
        // per row inside the write loop is not enough either, because a later row's refusal lands
        // after the earlier rows are written. So all of them are asked first, here, for exactly
        // the entries the loop below may add: the primary re-point, a changed secondary value,
        // and the tokens of changed text. The assignments are evaluated here too, still once per row, and the NOT
        // NULL check moves with them: it reads only the new values, and inside the loop it had
        // the same late-refusal shape.
        let mut planned = Vec::with_capacity(res.len());
        for (rid, old_values) in res {
            let mut new_values = old_values.clone();
            for (col_idx, expr) in &self.assignments {
                new_values[*col_idx] = evaluate(expr, &old_values)?;
            }
            for (i, col) in self.schema.columns.iter().enumerate() {
                if !col.nullable && matches!(new_values[i], Value::Null) {
                    return Err(FerroError::Constraint(format!(
                        "column '{}' of '{}' is declared NOT NULL, so it cannot be set to NULL",
                        col.name, self.table
                    )))
                }
            }
            let pk = &old_values[0];
            // The primary re-point, `upsert(pk, new_rid)`, happens only if the heap moves the row,
            // which is not known until it is written, so it is asked for every row. This build
            // cannot write a key over the bound; an earlier build could. The refusal names the one
            // remedy that works for a key (review 5): a primary key cannot be UPDATEd, and DELETE
            // then INSERT would leave the deleted tuple under the key for good.
            if let Some(len) = entry_over_bound(pk, &RecordId::new(0, 0)) {
                let shown: String = format!("{pk:?}").chars().take(60).collect();
                return Err(FerroError::Constraint(format!(
                    "this UPDATE would re-point the primary-index entry of the row whose key is \
                     {shown}: {}. A build before D225 could store such a key; this one cannot \
                     re-point it. Nothing has been written. {OPEN_TABLE_REMEDY}",
                    entry_too_large(len)
                )));
            }
            for handle in &self.secondary_indexes {
                let new_v = &new_values[handle.col_index];
                if &old_values[handle.col_index] != new_v {
                    admit_entry(&(new_v.clone(), pk.clone()), &())?;
                }
            }
            for ft in &self.fulltext_indexes {
                let new_text = indexed_text(&new_values[ft.col_index])?;
                if indexed_text(&old_values[ft.col_index])? != new_text {
                    if let Some(text) = new_text {
                        for token in distinct_tokens(text) {
                            admit_entry(&posting_key(&token, pk), &())?;
                        }
                    }
                }
            }
            planned.push((rid, old_values, new_values));
        }
        let mut count = 0;
        for (rid, old_values, new_values) in planned {
            let head_h = self.heap.read(rid)?.version_header()?;
            check_write_conflict(&self.view, &head_h)?;
            let pk = old_values[0].clone();
            let mut old_ver = self.heap.read(rid)?;
            old_ver.data[8..16].copy_from_slice(&self.heap.txn_id.to_be_bytes());
            let mut tuple = Tuple::serialize(&new_values, &self.schema, self.heap.txn_id)?;
            let tt_rid = self.tt_heap.insert(old_ver)?;
            tuple.data[16..20].copy_from_slice(&tt_rid.page_id.to_be_bytes());
            tuple.data[20..22].copy_from_slice(&tt_rid.slot_num.to_be_bytes());
            let new_rid = self.heap.update(rid, tuple)?;
            if let Some((prov, id)) = &self.author {
                prov.stamp(new_rid, *id)?;
            }
            if new_rid != rid {
                // ⛔ **D126 — this was `delete(&pk)` then `insert(pk, new_rid)`.** `delete` drops
                // the leaf write latch when it returns and `insert` re-acquires it, so the primary
                // key did not exist in the index between the two, and `search` descends with no
                // latch at all: a concurrent point lookup on this key got "no such row" for a row
                // that exists and was merely being moved. Same defect the branch catalog had on
                // its RECORD key, same fix -- one page write, no window.
                //
                // It also removes an error path that could only fire AFTER the heap was already
                // updated: `delete` returns `KeyNotFound` for a missing entry, and propagating
                // that here aborted the statement with the index left disagreeing with the heap.
                // `upsert` repairs the entry instead, which is the outcome that was wanted.
                //
                // **D202 — and a rollback must put it back.** The rollback frees `new_rid`
                // (`undo_insert`) and restores the row at `rid` (`undo_delete`). Without this record
                // the key was left on the freed slot while the row lived at `rid`: every lookup
                // of it and every INSERT of it then failed with `SlotDeleted`.
                if let Some(txn) = &self.heap.txn {
                    txn.record_primary_write(self.heap.txn_id, self.primary_index.root_cell(), pk.clone(), Some(rid));
                }
                self.primary_index.upsert(pk.clone(), new_rid)?;
            }

            // **E66 — one entry per (value, key) pair, however many times history visits it.**
            //
            // The old entry deliberately STAYS. A secondary entry is how a reader finds a row by
            // value, and a transaction whose snapshot predates this update must still find this row
            // under its old value - `SecondaryIndexScan` resolves the entry through the primary index
            // and `resolve_visibility` hands back the version that reader can see. Delete the old
            // entry and that lookup finds nothing, which is a lost row rather than a stale one.
            //
            // What must not happen is a SECOND identical entry. `insert_entry` appends at the
            // binary-search position rather than overwriting, so moving a value away and back gave
            // the index two copies of one pair, and the scan yields a row per entry: measured before
            // this guard, `UPDATE v=999 WHERE id=4; UPDATE v=40 WHERE id=4;` then a lookup for 40
            // returned `[[4, 40], [4, 40]]` - the same row twice, from write history alone.
            for handle in &self.secondary_indexes {
                let old_v = &old_values[handle.col_index];
                let new_v = &new_values[handle.col_index];
                if old_v != new_v {
                    let key = (new_v.clone(), pk.clone());
                    if handle.tree.search(&key)?.is_none() {
                        handle.tree.insert(key, ())?;
                    }
                }
            }
            // **B8 — the same shape for postings, and the same reason the old ones stay.**
            //
            // A posting is how a reader finds a row by a word in it, and a snapshot older than this
            // update must still find this row by a word that used to be in it. So the old tokens
            // are left alone and the new ones are posted, deduped.
            //
            // The breaking shape is the one E66 names, and a full-text index reaches it more
            // easily: `UPDATE body='...' ; UPDATE body='<the original text>' ;` re-posts every
            // token of the original text over postings that are still there. Without the probe
            // inside `post_tokens`, `insert_entry` appends and the search returns the row once per
            // copy. That the value moved away and back is invisible to the index - only the probe
            // sees it.
            //
            // Nothing here removes a posting for a token the new text dropped. `FullTextSearch`
            // re-tokenizes the version it resolves and drops a candidate whose text no longer holds
            // any query term, which is what makes a left-behind posting harmless rather than a
            // wrong answer.
            for ft in &self.fulltext_indexes {
                let old_text = indexed_text(&old_values[ft.col_index])?;
                let new_text = indexed_text(&new_values[ft.col_index])?;
                if old_text != new_text {
                    if let Some(text) = new_text {
                        post_tokens(&ft.tree, text, &pk)?;
                    }
                }
            }
            count += 1;
        }
        sync_roots(&self.table, &self.schema, &self.primary_index, &self.secondary_indexes, catalog)?;
        sync_fulltext_roots(&self.table, &self.fulltext_indexes, catalog)?;
        // D69 — record that this table changed, on the SAME path as the write that
        // changed it. The merge staleness check reads this counter instead of
        // rescanning and rehashing every row (see Catalog::bump_table_version). It must
        // be bumped here and not only on the agent-merge path: the hash it replaces was
        // computed by scanning the real table, so it saw ordinary DML too.
        catalog.bump_table_version(&self.table);

        Ok(count)
    }
}