use crate::catalog::schema::Schema;
use crate::error::FerroError;
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
    pub time_travel_root: u32,
    pub schema: Schema,
    pub indexes: Vec<IndexInfo>,
    /// Full-text indexes, kept in their own list — see `FullTextIndexInfo`.
    pub fulltext_indexes: Vec<FullTextIndexInfo>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct IndexInfo {
    pub column_name: String,
    pub root_page_id: u32,
}

/// A full-text index on one `VARCHAR` column (B8). Same two fields as `IndexInfo`, and the tree it
/// names is literally the same type — `BPlusTreeManager<(Value, Value), ()>` — but keyed
/// `(token, primary_key)` instead of `(column_value, primary_key)`.
///
/// **It is a separate list and a separate type on purpose.** `optimizer::has_index` treats any
/// column named in `TableEntry::indexes` as range-scannable and hands it to `SecondaryIndexScan`,
/// which would then compare a *token* against a whole-column bound and silently return the wrong
/// rows. A distinct type also stops a token tree being passed to `sync_roots` or
/// `update_index_root`, both of which resolve a root by column name inside `indexes`.
#[derive(Debug, PartialEq, Clone)]
pub struct FullTextIndexInfo {
    pub column_name: String,
    pub root_page_id: u32,
}

const HEADER_SIZE: usize = 23;
/// Catalog page written before B8: no full-text block after a table entry's index list.
/// Still read, never written.
const CATALOG_PAGE_TYPE_V1: u8 = 4;
/// Catalog page as written now: each table entry's index list is followed by a full-text index
/// list in the same shape (`u8` count, then `u8` name length + name + `u32` root per index).
///
/// Byte 0 of a catalog page was written and never read back, which is the only reason this
/// extension is possible at all: there is no version field in the 23-byte header, and inserting a
/// byte anywhere in an entry body would misparse every page an earlier build wrote. So byte 0
/// becomes the format stamp, `deserialize` branches on it, and an unknown value is refused rather
/// than guessed at. A v1 page is upgraded in memory on read and lands as v2 the next time the
/// catalog is persisted.
const CATALOG_PAGE_TYPE: u8 = 5;
// header: |page_type (1)|page_id (4)|next_catalog_page (4)|num_entries (2)|lsn (8)|checksum (4)|
impl CatalogPage {

    pub fn new(page_id: u32) -> Self {
        Self { page_type: CATALOG_PAGE_TYPE, page_id, lsn: 0, checksum: 0, next_catalog_page: 0, num_entries: 0, entries: Vec::new() }
    }

    // header -> entries (table name -> page id -> schema (column name -> datatype tag & null tag) -> index)
    // add space checking later
    pub fn serialize(&self) -> Result<[u8; PAGE_SIZE], FerroError>{
        let mut bytes = [0u8; PAGE_SIZE];
        // Always the current format, never `self.page_type`: a page read as v1 carries 4 in that
        // field, and writing 4 back out alongside a v2 body is exactly the misparse the stamp
        // exists to prevent.
        bytes[0] = CATALOG_PAGE_TYPE;
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
            bytes[offset..offset + 4].copy_from_slice(&entry.time_travel_root.to_be_bytes());
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
                    // Tags 0..3 are fixed by every catalog page already on disk. The wide types
                    // take the next free numbers so an existing file keeps deserialising.
                    DataType::BigInt => {
                        bytes[offset] = 4;
                        offset += 1;
                    }
                    DataType::Decimal => {
                        bytes[offset] = 5;
                        offset += 1;
                    }
                    DataType::Timestamp => {
                        bytes[offset] = 6;
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

            let num_fulltext = entry.fulltext_indexes.len() as u8;
            bytes[offset] = num_fulltext;
            offset += 1;

            for ft in &entry.fulltext_indexes {
                let ft_name_bytes = ft.column_name.as_bytes();
                bytes[offset] = ft_name_bytes.len() as u8;
                offset += 1;
                bytes[offset..offset + ft_name_bytes.len()].copy_from_slice(ft_name_bytes);
                offset += ft_name_bytes.len();
                bytes[offset..offset + 4].copy_from_slice(&ft.root_page_id.to_be_bytes());
                offset += 4;
            }
        }
        Ok(bytes)
    }

    // header -> entries (table name -> page id -> schema (column name -> datatype tag & null tag) -> index)
    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        // An allowlist, and it has to be: byte 0 was previously written and never read, so a page
        // that is neither format is a page this build cannot parse. Falling through to "assume the
        // newest" would read a v1 entry's next name-length byte as a full-text count.
        let has_fulltext_block = match bytes[0] {
            CATALOG_PAGE_TYPE => true,
            CATALOG_PAGE_TYPE_V1 => false,
            other => {
                return Err(FerroError::Corruption(format!(
                    "catalog page format {other} is not one this build can read (expected \
                     {CATALOG_PAGE_TYPE_V1} or {CATALOG_PAGE_TYPE})"
                )))
            }
        };
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
            let name = std::str::from_utf8(&bytes[offset..offset + name_len]).map_err(|_| FerroError::Corruption(String::from("deserializing error")))?.to_string();
            offset += name_len;

            let first_directory_page_id = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let primary_index_root = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let time_travel_root = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let num_columns = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
            offset += 2;
            let mut columns: Vec<Column> = Vec::new();

            for _ in 0..num_columns {
                let col_name_len = bytes[offset] as usize;
                offset += 1;
                let col_name = std::str::from_utf8(&bytes[offset..offset + col_name_len]).map_err(|_| FerroError::Corruption(String::from("deserializing error")))?;
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
                    4 => DataType::BigInt,
                    5 => DataType::Decimal,
                    6 => DataType::Timestamp,
                    _ => return Err(FerroError::Corruption(String::from("invalid tag")))
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
                let column_name = std::str::from_utf8(&bytes[offset..offset + ind_name_len]).map_err(|_| FerroError::Corruption(String::from("deserialization error")))?.to_string();
                offset += ind_name_len;
                let root_page_id = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
                offset += 4;

                indexes.push(IndexInfo {column_name,root_page_id});
            }

            let mut fulltext_indexes = Vec::new();
            if has_fulltext_block {
                let num_fulltext = bytes[offset] as usize;
                offset += 1;
                for _ in 0..num_fulltext {
                    let ft_name_len = bytes[offset] as usize;
                    offset += 1;
                    let column_name = std::str::from_utf8(&bytes[offset..offset + ft_name_len]).map_err(|_| FerroError::Corruption(String::from("deserialization error")))?.to_string();
                    offset += ft_name_len;
                    let root_page_id = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;

                    fulltext_indexes.push(FullTextIndexInfo {column_name, root_page_id});
                }
            }
            entries.push(TableEntry { name, first_directory_page_id, primary_index_root, schema: Schema{columns}, indexes , fulltext_indexes, time_travel_root});
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
        let mut length = 12;
        length += 1 + self.name.len();

        length += 1;
        for index in &self.indexes {
            length += 5 + index.column_name.len()
        }

        // The full-text block, counted whether or not it is empty: the count byte is always
        // written. `serialize` has no bounds check, so an under-count here is a slice-index panic
        // at 4096 rather than `NotEnoughSpace`.
        length += 1;
        for index in &self.fulltext_indexes {
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
        TableEntry { name: name.to_string(), first_directory_page_id: 1, primary_index_root: 2, time_travel_root: 1,
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
            fulltext_indexes: vec![
                FullTextIndexInfo { column_name: "name".to_string(), root_page_id: 22 },
            ],
        }
    }
    #[test]
    fn test_basic_roundtrip() {
        let mut catalog_page = CatalogPage::new(1);
        let table_entry = vec![TableEntry {name: "s".into(), first_directory_page_id: 1, primary_index_root: 2, schema: Schema { columns: Vec::new() }, indexes: Vec::new(), fulltext_indexes: Vec::new(), time_travel_root: 1}];
        catalog_page.entries = table_entry;
        catalog_page.num_entries = 1;

        let serialized = catalog_page.serialize().unwrap();
        let deserialized = CatalogPage::deserialize(serialized).unwrap();
        assert_eq!(deserialized, catalog_page);
    }

    /// B8 — the full-text list survives a round trip, and is not confused with the B-tree list.
    ///
    /// Breaking shape: a table with **both** kinds of index on the same column. `mock_entry` has
    /// `name` in `indexes` and in `fulltext_indexes` with different roots; if `serialize` and
    /// `deserialize` disagree about the order or the presence of the second block, the two roots
    /// swap or the names run together, and the reader gets a token tree for a range scan.
    #[test]
    fn fulltext_indexes_round_trip_separately_from_btree_indexes() {
        let mut page = CatalogPage::new(1);
        page.add_entry(mock_entry("docs")).unwrap();

        let back = CatalogPage::deserialize(page.serialize().unwrap()).unwrap();
        assert_eq!(back, page);

        let entry = &back.entries[0];
        assert_eq!(
            entry.indexes,
            vec![
                IndexInfo { column_name: "id".to_string(), root_page_id: 20 },
                IndexInfo { column_name: "name".to_string(), root_page_id: 21 },
            ]
        );
        assert_eq!(
            entry.fulltext_indexes,
            vec![FullTextIndexInfo { column_name: "name".to_string(), root_page_id: 22 }]
        );
    }

    /// Two entries, so the full-text block of the first sits in the middle of the page rather than
    /// at the end. Breaking shape: an offset that is only right for the last entry — the first
    /// entry's trailing block would then be read as the second entry's name length.
    #[test]
    fn a_middle_entry_s_fulltext_block_does_not_shift_the_next_entry() {
        let mut page = CatalogPage::new(1);
        page.add_entry(mock_entry("docs")).unwrap();
        page.add_entry(mock_entry("notes")).unwrap();

        let back = CatalogPage::deserialize(page.serialize().unwrap()).unwrap();
        assert_eq!(back, page);
        assert_eq!(back.entries[1].name, "notes");
        assert_eq!(back.entries[1].fulltext_indexes.len(), 1);
    }

    /// A catalog page written before B8 still parses, and reports no full-text indexes.
    ///
    /// Hand-encoded rather than produced by `serialize`, because `serialize` no longer writes this
    /// layout — building the fixture from the code under test would only prove the code agrees with
    /// itself. Breaking shape: any pre-B8 `.db` file. Without the format branch, the `u8` read for
    /// the full-text count is really the *next* byte of the page, and a page holding one table
    /// reports a full-text index it does not have.
    #[test]
    fn a_pre_b8_catalog_page_still_parses_with_no_fulltext_indexes() {
        // **TWO entries, and that is what makes this test able to fail.** With one entry the page is
        // zero-filled after its last field, so a build that wrongly read a full-text count byte
        // would read a 0 and produce the same answer — measured: the single-entry version of this
        // fixture SURVIVED the mutant that deletes the format branch. A second entry puts real bytes
        // where the spurious count would be read, so the one-byte shift corrupts everything after
        // it.
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0] = CATALOG_PAGE_TYPE_V1;
        bytes[1..5].copy_from_slice(&7u32.to_be_bytes()); // page_id
        bytes[5..9].copy_from_slice(&0u32.to_be_bytes()); // next_catalog_page
        bytes[9..11].copy_from_slice(&2u16.to_be_bytes()); // num_entries
        let mut o = HEADER_SIZE;

        // one v1 table entry: name, three roots, one INTEGER column, one B-tree index, and NO
        // full-text count byte at all — that byte is what v2 added.
        let mut write_entry = |bytes: &mut [u8; PAGE_SIZE], o: &mut usize, name: &[u8], roots: [u32; 3], index_root: u32| {
            bytes[*o] = name.len() as u8;
            *o += 1;
            bytes[*o..*o + name.len()].copy_from_slice(name);
            *o += name.len();
            for r in roots {
                bytes[*o..*o + 4].copy_from_slice(&r.to_be_bytes());
                *o += 4;
            }
            bytes[*o..*o + 2].copy_from_slice(&1u16.to_be_bytes()); // num_columns
            *o += 2;
            bytes[*o] = 2; // column name length
            *o += 1;
            bytes[*o..*o + 2].copy_from_slice(b"id");
            *o += 2;
            bytes[*o] = 0; // DataType::Integer
            *o += 1;
            bytes[*o] = 0; // not nullable
            *o += 1;
            bytes[*o] = 1; // one B-tree index
            *o += 1;
            bytes[*o] = 2; // index column name length
            *o += 1;
            bytes[*o..*o + 2].copy_from_slice(b"id");
            *o += 2;
            bytes[*o..*o + 4].copy_from_slice(&index_root.to_be_bytes());
            *o += 4;
        };
        write_entry(&mut bytes, &mut o, b"t", [11, 12, 13], 14);
        write_entry(&mut bytes, &mut o, b"u", [21, 22, 23], 24);

        let page = CatalogPage::deserialize(bytes).expect("a v1 page must still parse");
        assert_eq!(page.page_id, 7);
        assert_eq!(page.entries.len(), 2);

        let first = &page.entries[0];
        assert_eq!(first.name, "t");
        assert_eq!(first.primary_index_root, 12);
        assert_eq!(
            first.indexes,
            vec![IndexInfo { column_name: "id".to_string(), root_page_id: 14 }]
        );
        assert!(
            first.fulltext_indexes.is_empty(),
            "a v1 page cannot carry full-text indexes; got {:?}",
            first.fulltext_indexes
        );

        // The second entry is the one that catches an off-by-one in the first entry's tail.
        let second = &page.entries[1];
        assert_eq!(second.name, "u", "the entry after a v1 entry must still start where it should");
        assert_eq!(second.first_directory_page_id, 21);
        assert_eq!(second.primary_index_root, 22);
        assert_eq!(second.time_travel_root, 23);
        assert_eq!(
            second.indexes,
            vec![IndexInfo { column_name: "id".to_string(), root_page_id: 24 }]
        );
        assert!(second.fulltext_indexes.is_empty());

        // Read as the current format, upgraded in memory, so the next persist writes v2.
        assert_eq!(page.page_type, CATALOG_PAGE_TYPE);
    }

    /// The anti-vacuity half of the format allowlist: 4 and 5 are read, everything else is refused
    /// rather than parsed as the newest layout. Breaking shape: a page whose byte 0 was clobbered,
    /// or a future format read by this build — both used to parse as "whatever this build expects".
    #[test]
    fn an_unknown_catalog_page_format_is_refused() {
        let mut page = CatalogPage::new(1);
        page.add_entry(mock_entry("docs")).unwrap();
        let good = page.serialize().unwrap();
        assert!(CatalogPage::deserialize(good).is_ok(), "format 5 must be accepted");

        for stamp in [0u8, 3, 6, 255] {
            let mut bad = good;
            bad[0] = stamp;
            match CatalogPage::deserialize(bad) {
                Err(FerroError::Corruption(msg)) => {
                    assert!(msg.contains(&stamp.to_string()), "the refusal must name the format it saw: {msg}")
                }
                other => panic!("format {stamp} should be refused, got {other:?}"),
            }
        }
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