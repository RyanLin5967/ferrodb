use std::collections::HashMap;
use std::sync::Arc;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::schema::Schema;
use crate::error::FerroError;
use crate::storage::heap_page::Page;
use crate::storage::disk_manager::PAGE_SIZE;
use crate::catalog::column::DataType;
use crate::catalog::column::Column;

pub struct CatalogPage {
    pub page_type: u8,
    pub page_id: u32,
    pub next_catalog_page: u32,
    pub num_entries: u16,
    pub lsn: u64,
    pub checksum: u32,
    pub entries: Vec<TableEntry>
}

pub struct TableEntry {
    pub name: String,
    pub first_directory_page_id: u32,
    pub primary_index_root: u32,
    pub schema: Schema,
    pub indexes: Vec<IndexInfo>,
}

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
        todo!()
    }

    pub fn remove_entry(&mut self, name: &str) -> Result<(), FerroError> {
        todo!()
    }

    pub fn has_space(&self, entry: &TableEntry) -> bool {
        todo!()
    }
    
}