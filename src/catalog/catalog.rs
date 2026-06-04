use std::collections::HashMap;
use std::sync::Arc;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog_page::{CatalogPage, IndexInfo, TableEntry};
use crate::error::FerroError;
use crate::storage::heap_file_manager::HeapFileManager;
use crate::storage::index::BPlusTreeManager;
use crate::storage::index_page::{BPlusTreeInternalPage, BPlusTreeLeafPage};
use crate::storage::heap_file_manager::RecordId;
use crate::catalog::column::Value;
use std::sync::atomic::Ordering;
use crate::catalog::schema::Schema;

pub struct Catalog {
    pub tables: HashMap<String, TableEntry>,
    pub buffer_pool: Arc<BufferPoolManager>,
    pub first_catalog_page_id: u32,
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
        Ok(Self {tables: HashMap::new(), buffer_pool: buffer_pool.clone(), first_catalog_page_id: 1})
    }

    pub fn open(buffer_pool: Arc<BufferPoolManager>, first_catalog_page_id: u32) -> Result<Self, FerroError>{
        let mut catalog = Self {tables: HashMap::new(), buffer_pool, first_catalog_page_id};
        catalog.load()?;
        Ok(catalog)
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<(), FerroError> {
        if self.tables.contains_key(&name) {
            return Err(FerroError::KeyNotFound);
        }
        let hfm = HeapFileManager::new(self.buffer_pool.clone())?;
        let primary = BPlusTreeManager::<Value, RecordId>::create(self.buffer_pool.clone())?;
        let entry = TableEntry {
            name: name.clone(),
            first_directory_page_id: hfm.first_directory_page_id,
            schema,
            primary_index_root: primary.root_page_id.load(Ordering::Relaxed),
            indexes: Vec::new()
        };
        self.tables.insert(name, entry);
        self.persist()?;
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&TableEntry> {
        self.tables.get(name)
    }

    // create a secondary B+ tree, push an IndexInfo onto the table, persist
    pub fn create_index(&mut self, table: &str, column: &str) -> Result<(), FerroError> {
        let (schema, first_dir_page_id, col_index) = {
            let entry = self.tables.get(table).ok_or(FerroError::KeyNotFound)?;

            if entry.indexes.iter().any(|ind| ind.column_name == column) {
                return Err(FerroError::IndexAlreadyExists);
            }
            let col_index = entry.schema.columns.iter()
                .position(|c| c.name == column)
                .ok_or(FerroError::KeyNotFound)?;  // column must exist

            (entry.schema.clone(), entry.first_directory_page_id, col_index)
        };
        let sec_tree = BPlusTreeManager::<(Value, Value), ()>::create(self.buffer_pool.clone())?;
        let new_root_id = sec_tree.root_page_id.load(Ordering::Relaxed);

        let hfm = HeapFileManager::open(first_dir_page_id, self.buffer_pool.clone());
        for tuple in hfm.scan()? {
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
        if self.tables.remove(name).is_none() {
            return Err(FerroError::KeyNotFound);
        }
        self.persist()?;
        // TODO: page reclamation
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

        // TODO: page reclamation
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
}