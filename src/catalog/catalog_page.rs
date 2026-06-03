use std::collections::HashMap;
use std::sync::Arc;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::schema::Schema;
use crate::error::FerroError;
use crate::storage::heap_page::Page;
use crate::storage::disk_manager::PAGE_SIZE;
use crate::catalog::column::DataType;
use crate::catalog::column::Column;

#[derive(PartialEq, Debug)]
pub struct CatalogPage {
    pub page_type: u8,
    pub page_id: u32,
    pub next_catalog_page: u32,
    pub num_entries: u16,
    pub lsn: u64,
    pub checksum: u32,
    pub entries: Vec<TableEntry>
}

#[derive(Debug, PartialEq, Clone)]
pub struct TableEntry {
    pub name: String,
    pub first_directory_page_id: u32,
    pub primary_index_root: u32,
    pub schema: Schema,
    pub indexes: Vec<IndexInfo>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct IndexInfo {
    pub column_name: String,
    pub root_page_id: u32,
}

const HEADER_SIZE: usize = 23;
const CATALOG_PAGE_TYPE: u8 = 4;
// header: |page_type (1)|page_id (4)|next_catalog_page (4)|num_entries (2)|lsn (8)|checksum (4)|
impl CatalogPage {

    pub fn new(page_id: u32) -> Self {
        Self { page_type: CATALOG_PAGE_TYPE, page_id, lsn: 0, checksum: 0, next_catalog_page: 0, num_entries: 0, entries: Vec::new() }
    }

    // header -> entries (table name -> page id -> schema (column name -> datatype tag & null tag) -> index)
    // add space checking later
    pub fn serialize(&self) -> Result<[u8; PAGE_SIZE], FerroError>{
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0] = self.page_type;
        bytes[1..5].copy_from_slice(&self.page_id.to_be_bytes());
        bytes[5..9].copy_from_slice(&self.next_catalog_page.to_be_bytes());
        bytes[9..11].copy_from_slice(&(self.entries.len() as u16).to_be_bytes());
        bytes[11..19].copy_from_slice(&self.lsn.to_be_bytes());
        bytes[19..23].copy_from_slice(&self.checksum.to_be_bytes());

        let mut offset = HEADER_SIZE;
        for entry in &self.entries{
            let name_bytes = entry.name.as_bytes();
            bytes[offset] = name_bytes.len() as u8;
            offset += 1;
            bytes[offset..offset + name_bytes.len()].copy_from_slice(name_bytes);
            offset += name_bytes.len();
            
            bytes[offset..offset + 4].copy_from_slice(&entry.first_directory_page_id.to_be_bytes());
            offset += 4;
            bytes[offset..offset + 4].copy_from_slice(&entry.primary_index_root.to_be_bytes());
            offset += 4;

            let num_columns = entry.schema.columns.len() as u16;
            bytes[offset..offset + 2].copy_from_slice(&num_columns.to_be_bytes());
            offset+= 2;

            for col in &entry.schema.columns {
                let col_name_bytes = col.name.as_bytes();
                bytes[offset] = col.name.bytes().len() as u8;
                offset += 1;
                bytes[offset..offset + col_name_bytes.len()].copy_from_slice(col_name_bytes);
                offset += col_name_bytes.len();

                match col.data_type {
                    DataType::Integer => {
                        bytes[offset] = 0;
                        offset += 1;
                    }
                    DataType::Varchar(n) => {
                        bytes[offset] = 1;
                        offset += 1;
                        bytes[offset..offset + 2].copy_from_slice(&n.to_be_bytes());
                        offset += 2;
                    }
                    DataType::Float => {
                        bytes[offset] = 2;
                        offset += 1;
                    }
                    DataType::Boolean => {
                        bytes[offset] = 3;
                        offset += 1;
                    }
                }
                bytes[offset] = if col.nullable {1} else {0};
                offset += 1
            }

            let num_indexes = entry.indexes.len() as u8;
            bytes[offset] = num_indexes;
            offset += 1;

            for ind in &entry.indexes {
                let ind_name_bytes = ind.column_name.as_bytes();
                bytes[offset] = ind_name_bytes.len() as u8;
                offset += 1;
                bytes[offset..offset + ind_name_bytes.len()].copy_from_slice(ind_name_bytes);
                offset += ind_name_bytes.len();
                bytes[offset..offset + 4].copy_from_slice(&ind.root_page_id.to_be_bytes());
                offset += 4;
            }
        }
        Ok(bytes)
    }

    // header -> entries (table name -> page id -> schema (column name -> datatype tag & null tag) -> index)
    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        let page_id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
        let next_catalog_page = u32::from_be_bytes(bytes[5..9].try_into().unwrap());
        let num_entries = u16::from_be_bytes(bytes[9..11].try_into().unwrap());
        let lsn = u64::from_be_bytes(bytes[11..19].try_into().unwrap());
        let checksum = u32::from_be_bytes(bytes[19..23].try_into().unwrap());

        let mut entries: Vec<TableEntry> = Vec::new();
        let mut offset = HEADER_SIZE;

        for _ in 0..num_entries {
            let name_len = bytes[offset] as usize;
            offset += 1;
            let name = std::str::from_utf8(&bytes[offset..offset + name_len]).map_err(|_| FerroError::Parse(String::from("deserializing error")))?.to_string();
            offset += name_len;

            let first_directory_page_id = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let primary_index_root = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let num_columns = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
            offset += 2;
            let mut columns: Vec<Column> = Vec::new();

            for _ in 0..num_columns {
                let col_name_len = bytes[offset] as usize;
                offset += 1;
                let col_name = std::str::from_utf8(&bytes[offset..offset + col_name_len]).map_err(|_| FerroError::Parse(String::from("deserializing error")))?;
                offset += col_name_len;

                let tag = bytes[offset];
                offset += 1;
                let data_type = match tag {
                    0 => DataType::Integer,
                    1 => {
                        let size = u16::from_be_bytes(bytes[offset..offset+ 2].try_into().unwrap());
                        offset += 2;
                        DataType:: Varchar(size)
                    }
                    2 => DataType::Float,
                    3 => DataType::Boolean,
                    _ => return Err(FerroError::Parse(String::from("invalid tag")))
                };
                let nullable = bytes[offset] != 0;
                offset += 1;
                columns.push(Column { name: col_name.to_string(), data_type, nullable });
            }

            let num_indexes = bytes[offset] as usize;
            offset += 1;
            let mut indexes = Vec::new();

            for _ in 0..num_indexes {
                let ind_name_len = bytes[offset] as usize;
                offset += 1;
                let column_name = std::str::from_utf8(&bytes[offset..offset + ind_name_len]).map_err(|_| FerroError::Parse(String::from("deserialization error")))?.to_string();
                offset += ind_name_len;
                let root_page_id = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
                offset += 4;

                indexes.push(IndexInfo {column_name,root_page_id});
            }
            entries.push(TableEntry { name, first_directory_page_id, primary_index_root, schema: Schema{columns}, indexes });
        }
        Ok(Self { page_type: CATALOG_PAGE_TYPE, page_id, next_catalog_page, num_entries, lsn, checksum, entries })
    }

    pub fn add_entry(&mut self, entry: TableEntry) -> Result<(), FerroError> {
        if !self.has_space(&entry) {
            return Err(FerroError::NotEnoughSpace);
        }
        self.entries.push(entry);
        self.num_entries += 1;
        Ok(())
    }

    pub fn remove_entry(&mut self, name: &str) -> Result<(), FerroError> {
        for i in 0..self.entries.len() {
            if self.entries[i as usize].name == name {
                self.entries.remove(i.into());
                self.num_entries -= 1;
                return Ok(())
            }
        }
        Err(FerroError::KeyNotFound)
    }

    pub fn has_space(&self, entry: &TableEntry) -> bool {
        let entries_len: usize = self.entries.iter().map(|e| e.length()).sum();
        HEADER_SIZE + entry.length() + entries_len <= PAGE_SIZE
    }
    
}

impl TableEntry {
    pub fn length(&self)  -> usize{
        let mut length = 8;
        length += 1 + self.name.len();

        length += 1;
        for index in &self.indexes {
            length += 5 + index.column_name.len()
        }

        length += 2;
        for column in &self.schema.columns {
            length += 3 + column.name.len();
            if let DataType::Varchar(_) = column.data_type {
                length += 2;
            }
        }
        length
    }
}

#[cfg(test)]
mod tests {

use super::*;

    fn mock_entry(name: &str) -> TableEntry {
        TableEntry { name: name.to_string(), first_directory_page_id: 1, primary_index_root: 2, 
            schema: Schema {
                columns: vec![
                    Column::new("id".to_string(), DataType::Integer, false),
                    Column::new("name".to_string(), DataType::Varchar(255), true),
                    Column::new("active".to_string(), DataType::Boolean, false),
                ],
            },
            indexes: vec![
                IndexInfo { column_name: "id".to_string(), root_page_id: 20 },
                IndexInfo { column_name: "name".to_string(), root_page_id: 21 },
            ],  
        }
    }
    #[test]
    fn test_basic_roundtrip() {
        let mut catalog_page = CatalogPage::new(1);
        let table_entry = vec![TableEntry {name: "s".into(), first_directory_page_id: 1, primary_index_root: 2, schema: Schema { columns: Vec::new() }, indexes: Vec::new()}];
        catalog_page.entries = table_entry;
        catalog_page.num_entries = 1;

        let serialized = catalog_page.serialize().unwrap();
        let deserialized = CatalogPage::deserialize(serialized).unwrap();
        assert_eq!(deserialized, catalog_page);
    }

    #[test]
    fn test_remove_entry() {
        let mut page = CatalogPage::new(1);
        page.add_entry(mock_entry("users")).unwrap();
        page.add_entry(mock_entry("orders")).unwrap();

        assert!(page.remove_entry("users").is_ok());
        assert_eq!(page.num_entries, 1);
        assert_eq!(page.entries[0].name, "orders");
        let err = page.remove_entry("invalid").unwrap_err();
        assert!(matches!(err, FerroError::KeyNotFound));
        assert_eq!(page.num_entries, 1);
    }

    #[test]
    fn test_add_space_management() {
        let mut page = CatalogPage::new(1);
        let entry = mock_entry("users");
        let entry_len = entry.length();
        assert!(page.add_entry(entry.clone()).is_ok());
        assert_eq!(page.num_entries, 1);
        let max_entries = (PAGE_SIZE - HEADER_SIZE) / entry_len;

        for _ in 1..max_entries {
            assert!(page.add_entry(entry.clone()).is_ok())
        }

        let result = page.add_entry(mock_entry("overflow pls"));
        assert!(matches!(result, Err(FerroError::NotEnoughSpace)));
    }
}