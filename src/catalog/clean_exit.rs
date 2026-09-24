//! D230 — **a clean exit writes the catalog from its in-memory records before it checkpoints.**
//!
//! `sync_roots` records every moved root in the in-memory catalog and then persists it. When that
//! persist fails, the in-memory records are right and the catalog PAGE is not, and nothing made sure
//! a later persist happened before the process exited. A clean exit only checkpointed: it flushed the
//! stale page and truncated the log. The next open found an empty log, `recover` returned false,
//! nothing was rebuilt, and the shared root cell was seeded from the stale page.
//!
//! So the clean exit persists the catalog first, and checkpoints only if that worked
//! (`tests/d230_clean_exit_repersists_the_catalog.rs`).

use std::collections::BTreeMap;

use crate::catalog::catalog::Catalog;
use crate::catalog::catalog_page::TableEntry;
use crate::error::FerroError;
use crate::wal::txn::TxnManager;

/// The clean exit's catalog-and-log step: persist the catalog from its in-memory records, then
/// checkpoint the log. `cli::exit_sequence` runs it, then the arena's checkpoint.
///
/// **If the persist fails, the log is flushed but NOT truncated, and the error names the roots the
/// catalog on disk does not record.** Truncating is what would make the next open skip its rebuild and read the
/// stale page. Flushed and left in place, a log holding any record makes the next open recover and
/// rebuild every index from the heap, which writes every root afresh. A root can only have moved in
/// a statement that wrote heap records, so such a log is not empty unless a checkpoint has run since.
///
/// Blind spot, stated: if a checkpoint HAS truncated the log since the root moved and nothing has
/// been written after it, the next open does not rebuild, and this error is the only signal.
pub fn checkpoint_for_exit(catalog: &Catalog, txn: &TxnManager) -> Result<(), FerroError> {
    if let Err(e) = catalog.persist() {
        let flushed = match txn.wal.flush() {
            Ok(()) => "The write-ahead log was flushed and NOT truncated, so the next open recovers \
                       and rebuilds every index if it holds any record."
                .to_string(),
            Err(f) => format!(
                "The write-ahead log could not be flushed either ({f}), and it was NOT truncated."
            ),
        };
        let _ = txn.checkpoint(); // D230 MUTANT M7: truncates the log although the persist failed
        return Err(FerroError::Io(format!(
            "the clean exit could not persist the catalog: {e}. {} {flushed}",
            unpersisted_roots(catalog)
        )));
    }
    txn.checkpoint()
}

/// Every tree's root as `entries` record it, keyed by a name an operator can read.
fn roots_of<'a>(entries: impl IntoIterator<Item = &'a TableEntry>) -> BTreeMap<String, u32> {
    let mut roots = BTreeMap::new();
    for entry in entries {
        let name = &entry.name;
        roots.insert(format!("{name} (primary index)"), entry.primary_index_root);
        for idx in &entry.indexes {
            roots.insert(format!("{name}.{} (index)", idx.column_name), idx.root_page_id);
        }
        for ft in &entry.fulltext_indexes {
            roots.insert(format!("{name}.{} (full-text index)", ft.column_name), ft.root_page_id);
        }
    }
    roots
}

/// The roots the catalog ON DISK does not record as `catalog` does, for the exit's error.
///
/// Reads the page chain from the `DiskManager`, not the buffer pool: the next open reads the disk,
/// and a persist rewrites the pool's pages one at a time, so the pool's copy can already hold a root
/// the disk does not (D230 review 3, F5). It decodes with [`Catalog::read_entries`], which constructs
/// no `Catalog` and opens nothing (F1). If the chain cannot be read, every in-memory root is named,
/// since none of them can be confirmed.
fn unpersisted_roots(catalog: &Catalog) -> String {
    let in_memory = roots_of(catalog.tables.values());
    let disk = &catalog.buffer_pool.disk_manager;
    match Catalog::read_entries(catalog.first_catalog_page_id, |page_id| disk.read(page_id)) {
        Ok(on_disk) => {
            let recorded = roots_of(&on_disk);
            let stale: Vec<String> = in_memory
                .iter()
                .filter(|(tree, root)| recorded.get(*tree) != Some(*root))
                .map(|(tree, root)| match recorded.get(tree) {
                    Some(old) => format!("{tree}: the catalog on disk records page {old}, the tree's root is page {root}"),
                    None => format!("{tree}: the catalog on disk does not record it, the tree's root is page {root}"),
                })
                .collect();
            if stale.is_empty() {
                "The catalog on disk already records every root the in-memory catalog does.".to_string()
            } else {
                format!("Roots the catalog on disk does not record: {}.", stale.join("; "))
            }
        }
        Err(read) => format!(
            "The catalog on disk could not be read back to compare ({read}); the roots it must record \
             are: {}.",
            in_memory
                .iter()
                .map(|(tree, root)| format!("{tree}: page {root}"))
                .collect::<Vec<_>>()
                .join("; ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::sync::Arc;

    use super::*;
    use crate::buffer::buffer_pool::BufferPoolManager;
    use crate::catalog::column::{Column, DataType};
    use crate::catalog::schema::Schema;
    use crate::storage::disk_manager::DiskManager;

    /// The readable-page branch names the stale root by what the catalog ON DISK records and what
    /// the tree's root is. The failing-persist branch is driven end to end by the integration test
    /// named in the module doc; this pins the comparison it reports.
    ///
    /// D230 review 3 (F1/F5, the lead's decision) moved the comparison from the buffer pool's copy to
    /// the disk, so the control flushes first, and the last step pins F5: a persist that has not
    /// reached the disk is still named.
    #[test]
    fn a_root_the_page_does_not_record_is_named_with_both_pages() {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new().read(true).write(true).create(true).open(dir.path().join("c.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let mut catalog = Catalog::create(bp.clone()).unwrap();
        let schema = Schema { columns: vec![Column { name: "id".into(), data_type: DataType::Integer, nullable: false }] };
        catalog.create_table("t".to_string(), schema).unwrap();
        let on_page = catalog.get_table("t").unwrap().primary_index_root;
        bp.flush_all().unwrap();

        assert_eq!(
            unpersisted_roots(&catalog),
            "The catalog on disk already records every root the in-memory catalog does.",
            "control: straight after a persist that reached the disk, nothing is stale"
        );

        let moved = on_page + 1000;
        catalog.tables.get_mut("t").unwrap().primary_index_root = moved;
        let named = unpersisted_roots(&catalog);
        assert!(
            named.contains(&format!("t (primary index): the catalog on disk records page {on_page}, the tree's root is page {moved}")),
            "the message must name the stale root and the real one: {named}"
        );

        // F5: the persist rewrites the pool's copy, and the disk still holds the old root until a
        // flush. The next open reads the disk, so the root is still stale.
        catalog.persist().unwrap();
        let buffered = unpersisted_roots(&catalog);
        assert!(
            buffered.contains(&format!("t (primary index): the catalog on disk records page {on_page}, the tree's root is page {moved}")),
            "a persist that has not reached the disk must still be named, because the next open reads \
             the disk, not the pool: {buffered}"
        );
    }
}
