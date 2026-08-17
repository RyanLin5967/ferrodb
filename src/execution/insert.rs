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
//! Removing the entry orphans nothing. The deleted version stays in the heap where a sequential scan
//! still finds it and `ReadView::visible` still filters it, and nothing needs the index to reach an
//! old version: there is no temporal `AS OF <timestamp>` in this SQL surface, only `AS OF BRANCH`.
//!
//! The B+tree does not rebalance on delete (`handle_underflow` is unimplemented and never called), so
//! this can leave a sparse leaf. Sparse is correct; the alternative was a key that could not be
//! reused.

use crate::binder::binder::BoundExpr;
use crate::catalog::catalog::Catalog;
use crate::error::FerroError;
use crate::execution::executor::{Modify, evaluate, sync_roots};
use crate::storage::tuple::Tuple;
use crate::storage::heap_file_manager::HeapFileManager;
use crate::catalog::schema::Schema;
use crate::catalog::column::Value;
use crate::storage::index::BPlusTreeManager;
use crate::storage::heap_file_manager::RecordId;
use crate::execution::index_handle::IndexHandle;
use crate::provenance::{ProvId, ProvenanceStore};
use std::sync::Arc;

pub struct Insert {
    pub table: String,
    pub values: Vec<BoundExpr>,
    pub heap: HeapFileManager,
    pub schema: Schema,
    pub primary_index: BPlusTreeManager<Value, RecordId>,
    pub secondary_indexes: Vec<IndexHandle>,
    /// Who to attribute the inserted version to. `None` means unattributed.
    pub author: Option<(Arc<dyn ProvenanceStore>, ProvId)>,
    /// Needed to answer "is the row this index entry points at still there?".
    ///
    /// Without it the uniqueness check could only ask the index whether a key existed, and an index
    /// entry outlives the row: DELETE stamps `end_ts` on the version in place and leaves the entry
    /// pointing at it. So a deleted primary key could never be reused.
    pub view: std::sync::Arc<crate::wal::txn::ReadView>,
}

impl Modify for Insert {
    fn set_author(&mut self, prov: Arc<dyn ProvenanceStore>, id: ProvId) {
        self.author = Some((prov, id));
    }

    fn execute(&mut self, catalog: &mut Catalog) -> Result<usize, FerroError>{
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
        // **An index entry outlives the row it points at.** DELETE stamps `end_ts` on the version
        // in place and leaves the entry alone, so `search` finding a key does NOT mean the key is
        // taken. Asking the index alone made a deleted primary key unusable forever, and said so
        // with "use UPDATE to change the existing row" when there was no row to update.
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
            // The key is free, but the stale entry has to GO rather than be shadowed:
            // `insert_entry` appends, it does not overwrite, so leaving it would put two entries
            // for one key in a unique index and `search` would return whichever binary search
            // landed on. The deleted version itself stays in the heap, where a sequential scan
            // still finds it and resolves it as invisible - nothing needs the index to reach it,
            // because there is no temporal `AS OF` in the SQL surface, only `AS OF BRANCH`.
            self.primary_index.delete(&vals[0])?;
        }
        let tuple = Tuple::serialize(&vals, &self.schema, self.heap.txn_id)?;
        let rid = self.heap.insert(tuple)?;
        if let Some((prov, id)) = &self.author {
            prov.stamp(rid, *id)?;
        }
        self.primary_index.insert(vals[0].clone(), rid)?;
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
        sync_roots(&self.table, &self.schema, &self.primary_index, &self.secondary_indexes, catalog)?;
        Ok(1)
    }
}
