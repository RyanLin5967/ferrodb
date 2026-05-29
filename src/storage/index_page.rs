use crate::{catalog::column::Value, error::FerroError, storage::{disk_manager::PAGE_SIZE, heap_file_manager::RecordId}};
pub struct BPlusTreeInternalPage<K> {
    pub page_type: u8,
    pub page_id: u32,
    pub lsn: u64,
    pub checksum: u32,
    pub num_keys: u16,
    pub key_arr: Vec<K>, // prim: Value, sec: (Value, Value)
    pub child_ptrs: Vec<u32>,
}

pub struct BPlusTreeLeafPage<K, V> {
    pub page_type: u8,
    pub page_id: u32,
    pub lsn: u64,
    pub checksum: u32,
    pub num_keys: u16,
    pub next: Option<u32>,
    pub prev: Option<u32>,
    pub key_arr: Vec<K>, // prim: Value, sec: (Value, Value)
    pub vals: Vec<V> // prim: RecordId, sec: 0
}

pub enum BPlusTreePage<K, V> {
    Internal(BPlusTreeInternalPage<K>),
    Leaf(BPlusTreeLeafPage<K, V>)
}
pub const BPLUS_INTERNAL_TYPE: u8 = 2;
pub const BPLUS_LEAF_TYPE: u8 = 3;
const INTERNAL_HEADER_SIZE: usize = 21;
const LEAF_HEADER_SIZE: usize = 29;

// HEADER: |page_type (1)|page_id (4)|lsn (8)|checksum (4)|num_keys (4)|
impl<K> BPlusTreeInternalPage<K> {

    pub fn new(page_id: u32) -> Self {
        BPlusTreeInternalPage { page_id, page_type: BPLUS_INTERNAL_TYPE, lsn: 0, checksum: 0, num_keys: 0, key_arr: Vec::new(), child_ptrs: Vec::new() }
    }

    pub fn serialize(&self) -> Result<[u8; PAGE_SIZE], FerroError>{
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0] = BPLUS_INTERNAL_TYPE;
        todo!()
    }
    
    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        todo!()
    }
}

// HEADER: |page_type (1)|page_id (4)|lsn (8)|checksum (4)|num_keys (4)|next (4)|prev (4)|
impl<K: BTreeSerialize, V: BTreeSerialize> BPlusTreeLeafPage<K, V> {

    pub fn new(page_id: u32) -> Self {
        BPlusTreeLeafPage { page_id, page_type: BPLUS_LEAF_TYPE, lsn: 0, checksum: 0, num_keys: 0, next: None, prev: None, key_arr: Vec::new(), vals: Vec::new() }
    }

    pub fn serialize(&self) -> Result<[u8; PAGE_SIZE], FerroError>{
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0] = BPLUS_LEAF_TYPE;
        bytes[1..5].copy_from_slice(&self.page_id.to_be_bytes());
        bytes[5..13].copy_from_slice(&self.lsn.to_be_bytes());
        bytes[13..17].copy_from_slice(&self.checksum.to_be_bytes());
        bytes[17..21].copy_from_slice(&self.num_keys.to_be_bytes());

        

        Ok(bytes)
    }

    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        todo!()
    }
}

pub trait BTreeSerialize {
    fn serialize(&self, buf: &mut Vec<u8>);
    fn deserialize(bytes: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized;
}
impl BTreeSerialize for Value { // primary
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Value::Integer(i) => { // tag 0
                buf.push(0);
                buf.extend_from_slice(&i.to_be_bytes());
            }
            Value::Varchar(s) => { // tag 1, etc...
                buf.push(1);
                buf.push(s.len() as u8);
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Float(f) => {
                buf.push(2); 
                buf.extend_from_slice(&f.to_be_bytes());
            }
            Value::Boolean(b) => {
                buf.push(3);
                buf.push(*b as u8);
            }
            Value::Null => {
                buf.push(4);
            }
        }
    }

    fn deserialize(bytes: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized { // (Value, consumed)
        let tag = bytes[0];
        match tag {
            0 => {
                let i = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
                Ok((Value::Integer(i), 5))
            }
            1 => {
                let len = bytes[1] as usize;
                let s = String::from_utf8(bytes[2..2+len].try_into().unwrap()).unwrap();
                Ok((Value::Varchar(s), 2 + len))
            }
            2 => {
                let f = f64::from_be_bytes(bytes[1..9].try_into().unwrap());
                Ok((Value::Float(f), 9))
            }
            3 => {
                let b = bytes[1] != 0;
                Ok((Value::Boolean(b), 2))
            }
            4 => {
                Ok((Value::Null, 2))
            }
            _ => Err(FerroError::Io(String::from("invalid tag value")))
        }
    }
}

impl BTreeSerialize for (Value, Value) { // secondary
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf);
        self.1.serialize(buf);
    }

    fn deserialize(bytes: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized {    
        let (first, len1) = Value::deserialize(bytes)?;
        let (second, len2) = Value::deserialize(&bytes[len1..])?;
        Ok(((first, second), len1+len2))
    }
}

impl <K: BTreeSerialize, V: BTreeSerialize> BPlusTreePage<K, V> {
    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        match bytes[0] {
            BPLUS_INTERNAL_TYPE => Ok(BPlusTreePage::Internal(BPlusTreeInternalPage::deserialize(bytes)?)),
            BPLUS_LEAF_TYPE => Ok(BPlusTreePage::Leaf(BPlusTreeLeafPage::deserialize(bytes)?)),
            _ => Err(FerroError::Io(String::from("invalid page type header")))
        }
    }
}