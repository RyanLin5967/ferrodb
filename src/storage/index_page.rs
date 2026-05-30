use crate::{catalog::column::Value, error::FerroError, storage::{disk_manager::PAGE_SIZE, heap_file_manager::RecordId}};
#[derive(PartialEq, Debug)]
pub struct BPlusTreeInternalPage<K> {
    pub page_type: u8,
    pub page_id: u32,
    pub lsn: u64,
    pub checksum: u32,
    pub num_keys: u16,
    pub key_arr: Vec<K>, // prim: Value, sec: (Value, Value)
    pub child_ptrs: Vec<u32>,
}

#[derive(PartialEq, Debug)]
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
const INTERNAL_HEADER_SIZE: usize = 19;
const LEAF_HEADER_SIZE: usize = 27;
// const CHILD_POINTER_SIZE: usize = 4;
// HEADER: |page_type (1)|page_id (4)|lsn (8)|checksum (4)|num_keys (2)|
impl<K: BTreeSerialize> BPlusTreeInternalPage<K> {

    pub fn new(page_id: u32) -> Self {
        BPlusTreeInternalPage { page_id, page_type: BPLUS_INTERNAL_TYPE, lsn: 0, checksum: 0, num_keys: 0, key_arr: Vec::new(), child_ptrs: Vec::new() }
    }

    pub fn serialize(&self) -> Result<[u8; PAGE_SIZE], FerroError>{
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0] = BPLUS_INTERNAL_TYPE;
        bytes[1..5].copy_from_slice(&self.page_id.to_be_bytes());
        bytes[5..13].copy_from_slice(&self.lsn.to_be_bytes());
        bytes[13..17].copy_from_slice(&self.checksum.to_be_bytes());
        bytes[17..19].copy_from_slice(&self.num_keys.to_be_bytes());

        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        bytes[INTERNAL_HEADER_SIZE..INTERNAL_HEADER_SIZE + buf.len()].copy_from_slice(&buf);
        for (i, child_ptr) in self.child_ptrs.iter().enumerate() {
            bytes[INTERNAL_HEADER_SIZE + buf.len() + i*4..INTERNAL_HEADER_SIZE + buf.len() + i*4 +4].copy_from_slice(&child_ptr.to_be_bytes());
        }
        
        Ok(bytes)
    }
    
    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        let page_type = u8::from_be_bytes(bytes[0..1].try_into().unwrap());
        let page_id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
        let lsn = u64::from_be_bytes(bytes[5..13].try_into().unwrap());
        let checksum = u32::from_be_bytes(bytes[13..17].try_into().unwrap());
        let num_keys = u16::from_be_bytes(bytes[17..19].try_into().unwrap());

        let mut offset = INTERNAL_HEADER_SIZE;
        let mut key_arr = Vec::new();
        for _ in 0..num_keys {
            let (key, consumed) = K::deserialize(&bytes[offset..])?;
            key_arr.push(key);
            offset += consumed;
        }
        let mut child_ptrs = Vec::new();
        for i  in 0..num_keys + 1 {
            child_ptrs.push(u32::from_be_bytes(bytes[offset+ i as usize*4..offset+i as usize*4+ 4].try_into().unwrap()));
        }

        Ok(Self { page_type, page_id, lsn, checksum, num_keys, key_arr, child_ptrs })
    }
}

// HEADER: |page_type (1)|page_id (4)|lsn (8)|checksum (4)|num_keys (2)|next (4)|prev (4)|
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
        bytes[17..19].copy_from_slice(&self.num_keys.to_be_bytes());
        match self.next {
            Some(next) => bytes[19..23].copy_from_slice(&next.to_be_bytes()),
            None => bytes[19..23].copy_from_slice(&[0u8; 4]),
        } 
        match self.prev {
            Some(prev) => bytes[23..27].copy_from_slice(&prev.to_be_bytes()),
            None => bytes[23..27].copy_from_slice(&[0u8; 4]),
        }

        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        bytes[LEAF_HEADER_SIZE..LEAF_HEADER_SIZE + buf.len()].copy_from_slice(&buf);
        let mut buff = Vec::new();
        for v in &self.vals {
            v.serialize(&mut buff);
        }
        bytes[LEAF_HEADER_SIZE + buf.len()..LEAF_HEADER_SIZE + buf.len() + buff.len()].copy_from_slice(&buff);

        Ok(bytes)
    }

    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        let page_type = u8::from_be_bytes(bytes[0..1].try_into().unwrap());
        let page_id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
        let lsn = u64::from_be_bytes(bytes[5..13].try_into().unwrap());
        let checksum = u32::from_be_bytes(bytes[13..17].try_into().unwrap());
        let num_keys = u16::from_be_bytes(bytes[17..19].try_into().unwrap());
        let next = match u32::from_be_bytes(bytes[19..23].try_into().unwrap()) {
            0 => None,
            n => Some(n)
        };
        let prev = match u32::from_be_bytes(bytes[23..27].try_into().unwrap()) {
            0 => None,
            p => Some(p)
        };

        let mut key_arr = Vec::new();
        let mut offset = LEAF_HEADER_SIZE;
        for _ in 0..num_keys {
            let (key, consumed) = K::deserialize(&bytes[offset..])?;
            key_arr.push(key);
            offset += consumed;
        }

        let mut vals = Vec::new();
        for _ in 0..num_keys {
            let (val, consumed) = V::deserialize(&bytes[offset..])?;
            vals.push(val);
            offset += consumed;
        }
        Ok(Self { page_type, page_id, lsn, checksum, num_keys, next, prev, key_arr, vals })
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
                Ok((Value::Null, 1))
            }
            _ => Err(FerroError::Io(String::from("invalid tag value")))
        }
    }
}

impl BTreeSerialize for RecordId {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.page_id.to_be_bytes());
        buf.extend_from_slice(&self.slot_num.to_be_bytes());
    }

    fn deserialize(bytes: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized {
        if bytes.len() < 6 {
            return Err(FerroError::NotEnoughSpace)
        }
        let page_id = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let slot_num = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
        Ok ((RecordId::new(page_id, slot_num), 6))
    }
}

impl BTreeSerialize for () {
    fn serialize(&self, _: &mut Vec<u8>) {}
    fn deserialize(_: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized { Ok(((), 0)) }
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

// |      K1       |       K2       |       K3       |
// |    k < K1  |K1 <= k < K2 | K2 <= k < K3 |k >= K4|
impl <K: Ord + Clone + BTreeSerialize> BPlusTreeInternalPage<K> {

    // find position of a key (or where it should go) returns index (search and insert use)
    pub fn binary_search(&self, key: &K) -> usize {
        match self.key_arr.binary_search(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        }
    }

    // binary search the separator keys, retrn matching child ptr index
    pub fn find_child(&self, key: &K) -> u32{
        let index = match self.key_arr.binary_search(key) {
            Ok(pos) => pos+ 1,
            Err(pos) => pos,
        };
        self.child_ptrs[index]
    }
    // insert a separator key and its chidl pointer at a position (for child splits and pushes key up)
    pub fn insert_key_child(&mut self, index: usize, key: K, child_ptr: u32){
        self.key_arr.insert(index, key);
        self.child_ptrs.insert(index+1, child_ptr);
        self.num_keys += 1;
    }
    // divide keys/children between current and new node, return middle key to push up, new node
    pub fn split(&mut self, new_page_id: u32) -> (K, Self) {
        let mid = self.key_arr.len() /2;
        let mid_key = self.key_arr.remove(mid);

        let mut new_node = Self {
            page_id: new_page_id,
            page_type: BPLUS_INTERNAL_TYPE,
            lsn: 0,
            checksum: 0,
            num_keys: 0,
            key_arr: self.key_arr.split_off(mid),
            child_ptrs: self.child_ptrs.split_off(mid + 1)
        };
        new_node.num_keys = new_node.key_arr.len() as u16;
        self.num_keys = mid as u16;
        (mid_key, new_node)
    }
    // does adding one more entry exceed capacity? triggers splits
    pub fn is_full(&self) -> bool { 
        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        let len = buf.len();
        let child_ptrs_size = self.child_ptrs.len() * 4;
        return INTERNAL_HEADER_SIZE + len + child_ptrs_size>= PAGE_SIZE;
    }
    // fewer than min entries (capacity/2) triggers merge/redistribute
    pub fn is_underfull(&self) -> bool {
        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        let child_ptrs_size = self.child_ptrs.len() * 4;
        INTERNAL_HEADER_SIZE + buf.len() + child_ptrs_size < (PAGE_SIZE - INTERNAL_HEADER_SIZE)/2
    }
}

impl<K: BTreeSerialize + Ord + Clone, V: Clone + BTreeSerialize + Ord> BPlusTreeLeafPage<K,V> {
    pub fn is_full(&self) -> bool {
        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        for val in &self.vals {
            val.serialize(&mut buf);
        }
        return buf.len() + LEAF_HEADER_SIZE >= PAGE_SIZE;
    } 
    pub fn is_underfull(&self) -> bool{
        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        for val in &self.vals {
            val.serialize(&mut buf);
        }
        LEAF_HEADER_SIZE + buf.len() < (PAGE_SIZE - LEAF_HEADER_SIZE)/2
    }
    pub fn binary_search(&self, key: &K) -> usize {
        match self.key_arr.binary_search(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        }
    }

    // insert a key,value at the correct sorted position
    pub fn insert_entry(&mut self, key: K, value: V){ 
        let index = self.binary_search(&key);
        self.key_arr.insert(index, key);
        self.vals.insert(index, value);
        self.num_keys += 1;
    }

    // find and remove a key/value
    pub fn remove_entry(&mut self, key: &K) -> Result<(), FerroError>{ 
        let index = match self.key_arr.binary_search(key) {
            Ok(i) => i,
            Err(_) => return Err(FerroError::KeyNotFound)
        };
        
        self.key_arr.remove(index);
        self.vals.remove(index);
        self.num_keys -= 1;
        Ok(())
    }

    // return value for a key, or None
    pub fn get(&self, key: &K) -> Result<Option<&V>, FerroError> { 
        let index = match self.key_arr.binary_search(key) {
            Ok(i) => i,
            Err(_) => return Ok(None)
        };
        return Ok(Some(&self.vals[index]));
    }

    // divide entries between this leaf and a new leaf, copy the middle key up (stays in leaf), fix sibling pointers
    pub fn split(&mut self, new_page_id: u32) -> (K, Self) {
        let mid = self.key_arr.len() /2;
        let mid_key = self.key_arr[mid].clone();

        let mut new_node = Self {
            page_id: new_page_id,
            page_type: BPLUS_LEAF_TYPE,
            lsn: 0,
            checksum: 0,
            num_keys: 0,
            key_arr: self.key_arr.split_off(mid),
            vals: self.vals.split_off(mid),
            next: self.next, // new node points to old next
            prev: Some(self.page_id) // new node points back to self
        };
        self.next = Some(new_node.page_id); // self now points to new node
        new_node.num_keys = new_node.key_arr.len() as u16;
        self.num_keys = self.key_arr.len() as u16;
        // ALSO NEED TO UPDATE OLD SIBLING POINTER POINTING TO new_node INSTEAD OF SELF
        (mid_key, new_node)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_i_p() -> Result<(), FerroError> {
        let mut internal = BPlusTreeInternalPage::<Value>::new(1);
        internal.key_arr = vec![Value::Integer(10), Value::Integer(20)];
        internal.child_ptrs = vec![100, 200, 300]; // N keys -> N+1 ptrs
        internal.num_keys = 2;

        let bytes = internal.serialize()?;
        let de = BPlusTreeInternalPage::<Value>::deserialize(bytes)?;
        assert_eq!(internal, de);
        Ok(())
    }

    #[test]
    fn test_roundtrip_i_s() -> Result<(), FerroError> {
        let mut internal = BPlusTreeInternalPage::<(Value, Value)>::new(1);
        internal.key_arr = vec![
            (Value::Integer(10), Value::Integer(1)),
            (Value::Integer(20), Value::Integer(2)),
        ];
        internal.child_ptrs = vec![100, 200, 300];
        internal.num_keys = 2;

        let bytes = internal.serialize()?;
        let de = BPlusTreeInternalPage::<(Value, Value)>::deserialize(bytes)?;
        assert_eq!(internal, de);
        Ok(())
    }

    #[test]
    fn test_roundtrip_l_p() -> Result<(), FerroError> {
        let mut leaf = BPlusTreeLeafPage::<Value, RecordId>::new(2);
        leaf.key_arr = vec![Value::Integer(5), Value::Integer(15)];
        leaf.vals = vec![RecordId::new(7, 3), RecordId::new(8, 4)];
        leaf.num_keys = 2;
        leaf.next = Some(3);
        leaf.prev = None;

        let bytes = leaf.serialize()?;
        let de = BPlusTreeLeafPage::<Value, RecordId>::deserialize(bytes)?;
        assert_eq!(leaf, de);
        Ok(())
    }

    #[test]
    fn test_roundtrip_l_s() -> Result<(), FerroError> {
        let mut leaf = BPlusTreeLeafPage::<(Value, Value), ()>::new(2);
        leaf.key_arr = vec![
            (Value::Varchar("toronto".into()), Value::Integer(1)),
            (Value::Varchar("toronto".into()), Value::Integer(2)),
        ];
        leaf.vals = vec![(), ()];
        leaf.num_keys = 2;
        leaf.next = Some(5);
        leaf.prev = Some(1);

        let bytes = leaf.serialize()?;
        let de = BPlusTreeLeafPage::<(Value, Value), ()>::deserialize(bytes)?;
        assert_eq!(leaf, de);
        Ok(())
    }

    #[test]
    fn test_internal_find_child() {
        let mut node = BPlusTreeInternalPage::<Value>::new(1);
        node.key_arr = vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)];
        node.child_ptrs = vec![100, 200, 300, 400];
        node.num_keys = 3;

        assert_eq!(node.find_child(&Value::Integer(5)), 100);
        assert_eq!(node.find_child(&Value::Integer(10)), 200);
        assert_eq!(node.find_child(&Value::Integer(15)), 200);
        assert_eq!(node.find_child(&Value::Integer(30)), 400);
        assert_eq!(node.find_child(&Value::Integer(99)), 400);
    }

    #[test]
    fn test_internal_insert_key_child() {
        let mut node = BPlusTreeInternalPage::<Value>::new(1);
        node.key_arr = vec![Value::Integer(10), Value::Integer(30)];
        node.child_ptrs = vec![100, 200, 300];
        node.num_keys = 2;

        node.insert_key_child(1, Value::Integer(20), 250);
        assert_eq!(node.key_arr, vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)]);
        assert_eq!(node.child_ptrs, vec![100, 200, 250, 300]);
        assert_eq!(node.num_keys, 3);
    }

    #[test]
    fn test_internal_split() {
        let mut node = BPlusTreeInternalPage::<Value>::new(1);
        node.key_arr = vec![Value::Integer(10), Value::Integer(20), Value::Integer(30), Value::Integer(40)];
        node.child_ptrs = vec![1,2,3,4,5];
        node.num_keys = 4;

        let (mid_key, new_node) = node.split(2);
        assert_eq!(mid_key, Value::Integer(30));
        assert_eq!(node.key_arr, vec![Value::Integer(10), Value::Integer(20)]);
        assert_eq!(node.child_ptrs, vec![1,2,3]);
        assert_eq!(node.num_keys, 2);
        assert_eq!(new_node.key_arr, vec![Value::Integer(40)]);
        assert_eq!(new_node.child_ptrs, vec![4,5]);
        assert_eq!(new_node.num_keys, 1);
        assert_eq!(node.child_ptrs.len(), node.key_arr.len() + 1);
        assert_eq!(new_node.child_ptrs.len(), new_node.key_arr.len() + 1);
    }
}