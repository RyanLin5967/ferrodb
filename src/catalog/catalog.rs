use std::collections::HashMap;
use std::sync::Arc;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog_page::{CatalogPage, IndexInfo, TableEntry};
use crate::catalog::stats::{ColumnStats, TableStats};
use crate::error::FerroError;
use crate::storage::heap_file_manager::HeapFileManager;
use crate::storage::index::BPlusTreeManager;
use crate::storage::heap_file_manager::RecordId;
use crate::catalog::column::Value;
use std::sync::atomic::Ordering;
use crate::catalog::schema::Schema;

pub struct Catalog {
    pub tables: HashMap<String, TableEntry>,
    pub buffer_pool: Arc<BufferPoolManager>,
    pub first_catalog_page_id: u32,
    pub stats: HashMap<String, TableStats>,
}

impl Catalog {
    pub fn create(buffer_pool: Arc<BufferPoolManager>) -> Result<Self, FerroError> {
        let page_id = buffer_pool.new_page()?; // = 1 on a fresh DB
        let frame_i = buffer_pool.fetch_page(page_id)?;
        let mut frame = buffer_pool.frames[frame_i].write().unwrap();
        let page = CatalogPage::new(page_id);
        frame.data = page.serialize()?;
        drop(frame);
        buffer_pool.unpin_page(page_id, true);
        Ok(Self {tables: HashMap::new(), buffer_pool: buffer_pool.clone(), first_catalog_page_id: 1, stats: HashMap::new()})
    }

    pub fn open(buffer_pool: Arc<BufferPoolManager>, first_catalog_page_id: u32) -> Result<Self, FerroError>{
        let mut catalog = Self {tables: HashMap::new(), buffer_pool, first_catalog_page_id, stats: HashMap::new()};
        catalog.load()?;
        Ok(catalog)
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<(), FerroError> {
        // E67: this was `FerroError::KeyNotFound`, so creating a table that already exists answered
        // `error: key wasn't found` - a storage-layer message, for a name collision, telling the
        // reader that something is missing when the problem is that something is present.
        if self.tables.contains_key(&name) {
            return Err(FerroError::Constraint(format!(
                "table '{name}' already exists; DROP it first or choose another name"
            )));
        }
        let hfm = HeapFileManager::new(self.buffer_pool.clone())?;
        let primary = BPlusTreeManager::<Value, RecordId>::create(self.buffer_pool.clone())?;
        let tt_heap = HeapFileManager::new(self.buffer_pool.clone())?;
        let entry = TableEntry {
            name: name.clone(),
            first_directory_page_id: hfm.first_directory_page_id,
            schema,
            primary_index_root: primary.root_page_id.load(Ordering::Relaxed),
            time_travel_root: tt_heap.first_directory_page_id, 
            indexes: Vec::new()
        };
        self.tables.insert(name, entry);
        self.persist()?;
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

        self.persist()?;
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), FerroError> {
        let (heap_dir, primary_root, sec_roots) = {
            let entry = self.tables.get(name).ok_or(FerroError::KeyNotFound)?;
            (entry.first_directory_page_id, entry.primary_index_root, entry.indexes.iter().map(|i| i.root_page_id).collect::<Vec<_>>())
        };
        HeapFileManager::open(heap_dir, self.buffer_pool.clone()).free_all()?;
        BPlusTreeManager::<Value, RecordId>::open(primary_root, self.buffer_pool.clone()).free_all()?;
        for root in sec_roots {
            BPlusTreeManager::<(Value, Value), ()>::open(root, self.buffer_pool.clone()).free_all()?;
        }
        self.tables.remove(name);
        self.persist()?;
        Ok(())
    }

    // root split propagation called when a tree's root changes
    pub fn update_primary_root(&mut self, table: &str, new_root: u32) -> Result<(), FerroError> {
        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.primary_index_root = new_root;
        self.persist()?;
        Ok(())
    }

    pub fn update_index_root(&mut self, table: &str, column: &str, new_root: u32) -> Result<(), FerroError> {
        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.indexes.iter_mut().find(|ind| ind.column_name == column).ok_or(FerroError::KeyNotFound)?.root_page_id = new_root;
        self.persist()?;
        Ok(())
    }

    pub fn persist(&self) -> Result<(), FerroError> {
        let mut curr_page_id = self.first_catalog_page_id;
        let mut iter = self.tables.values().peekable();

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
                    page.next_catalog_page = new_id;
                }
            } else {
                orphan_head = page.next_catalog_page;
                page.next_catalog_page = 0;
            }

            let next = page.next_catalog_page;

            {
                let mut frame = self.buffer_pool.frames[frame_i].write().unwrap();
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
        let file = tempfile().expect("Failed to create temporary test file");
        let disk_manager = Arc::new(DiskManager::new(file).expect("Failed to create DiskManager"));
        let bp = Arc::new(BufferPoolManager::new(disk_manager));
        Catalog::create(bp).unwrap()
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
        
        catalog.tables.insert("test_table".to_string(), TableEntry { name: "test_table".to_string(), first_directory_page_id: 2, primary_index_root: 3, schema: create_test_schema(), indexes: vec![] , time_travel_root: 1});
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
                TableEntry { name: format!("table_{}", i), first_directory_page_id: i, primary_index_root: i + 1, schema: create_test_schema(), indexes: vec![], time_travel_root: 1 }
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

