use std::sync::Arc;

use crate::catalog::catalog::Catalog;
use crate::execution::executor::{Modify, sync_roots};
use crate::wal::txn::ReadView;
use crate::wal::visibility::check_write_conflict;
use crate::{error::FerroError, execution::executor::Executor, storage::heap_file_manager::HeapFileManager};
use crate::catalog::schema::Schema;
use crate::storage::index::BPlusTreeManager;
use crate::catalog::column::Value;
use crate::storage::heap_file_manager::RecordId;
use crate::execution::index_handle::IndexHandle;
use crate::provenance::{ProvId, ProvenanceStore};

/// `DELETE` — and, for B8, the statement that maintains **no** posting on purpose.
///
/// There is no `fulltext_indexes` field here, and that is the whole of full-text delete
/// maintenance. A `DELETE` stamps `end_ts` on the version in place and leaves every index entry
/// alone, because an entry is how an older snapshot still reaches the row it can still see. Removing
/// the postings of a deleted row would lose it for those readers, not tidy up after it.
///
/// A dead posting is made harmless on the read side instead: `FullTextSearch` resolves each posting
/// through the primary index, applies `resolve_visibility`, and drops the candidate when no version
/// is visible — the same three steps `SecondaryIndexScan` takes for the same reason. The cost is the
/// scan amplification `tests/integration_index_debt.rs` measures at 8x, which is space, not a wrong
/// answer.
///
/// So this statement writes no tree and no full-text root can move, which is why `plan()` drops the
/// full-text handles when it builds a `Delete`.
pub struct Delete {
    pub table: String,
    pub child: Box<dyn Executor>,
    pub heap: HeapFileManager,
    pub schema: Schema,
    pub primary_index: BPlusTreeManager<Value, RecordId>,
    pub secondary_indexes: Vec<IndexHandle>,
    pub view: Arc<ReadView>,
    /// Who to attribute the tombstone to. `None` means unattributed.
    pub author: Option<(std::sync::Arc<dyn ProvenanceStore>, ProvId)>,
}

impl Modify for Delete {
    fn set_author(&mut self, prov: std::sync::Arc<dyn ProvenanceStore>, id: ProvId) {
        self.author = Some((prov, id));
    }

    fn execute(&mut self, catalog: &mut Catalog) -> Result<usize, FerroError> {
        let mut res = Vec::new();
        let mut count = 0;
        loop {
            let (rid, values) = match self.child.next() {
                Some(Ok((r, t))) => (r, t),
                Some(Err(e)) => return Err(e),
                None => break
            };
            res.push((rid, values));
        }
        for (rid, _values) in res {
            let mut head = self.heap.read(rid)?;
            let head_h = head.version_header()?;
            check_write_conflict(&self.view, &head_h)?;
            head.data[8..16].copy_from_slice(&self.heap.txn_id.to_be_bytes());
            self.heap.update(rid, head)?;
            if let Some((prov, id)) = &self.author {
                prov.stamp(rid, *id)?;
            }
            count += 1;
        }
        sync_roots(&self.table, &self.schema, &self.primary_index, &self.secondary_indexes, catalog)?;
        // D69 — record that this table changed, on the SAME path as the write that
        // changed it. The merge staleness check reads this counter instead of
        // rescanning and rehashing every row (see Catalog::bump_table_version). It must
        // be bumped here and not only on the agent-merge path: the hash it replaces was
        // computed by scanning the real table, so it saw ordinary DML too.
        catalog.bump_table_version(&self.table);

        Ok(count)
    }
}