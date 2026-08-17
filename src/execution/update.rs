use std::sync::Arc;

use crate::binder::binder::BoundExpr;
use crate::catalog::catalog::Catalog;
use crate::error::FerroError;
use crate::execution::executor::{Modify, evaluate, sync_roots};
use crate::storage::tuple::Tuple;
use crate::wal::txn::ReadView;
use crate::wal::visibility::check_write_conflict;
use crate::{catalog::schema::Schema, execution::executor::Executor, storage::heap_file_manager::HeapFileManager};
use crate::storage::index::BPlusTreeManager;
use crate::storage::heap_file_manager::RecordId;
use crate::catalog::column::Value;
use crate::execution::index_handle::IndexHandle;
use crate::provenance::{ProvId, ProvenanceStore};

pub struct Update {
    pub table: String,
    pub child: Box<dyn Executor>,
    pub schema: Schema,
    pub assignments: Vec<(usize, BoundExpr)>, // col idx -> new value expr
    pub heap: HeapFileManager,
    pub primary_index: BPlusTreeManager<Value, RecordId>,
    pub secondary_indexes: Vec<IndexHandle>,
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
        // is a real remedy rather than advice that would have failed.
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
        let mut count = 0;
        for (rid, old_values) in res {
            let head_h = self.heap.read(rid)?.version_header()?;
            check_write_conflict(&self.view, &head_h)?;
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
                self.primary_index.delete(&pk)?;
                self.primary_index.insert(pk.clone(), new_rid)?;
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
            count += 1;
        }
        sync_roots(&self.table, &self.schema, &self.primary_index, &self.secondary_indexes, catalog)?;
        Ok(count)
    }
}