use std::{fs::File, marker::PhantomData, sync::{Arc, atomic::AtomicU32}};

use crate::{buffer::buffer_pool::BufferPoolManager, error::FerroError, storage::{disk_manager::DiskManager, index_page::{BPlusTreeInternalPage, BPlusTreeLeafPage, BTreeSerialize}}};
use crate::storage::index_page::BPlusTreePage;
use std::sync::atomic::Ordering;
pub enum OperationType {
    Search,
    Insert,
    Delete,
}
pub struct BPlusTreeManager<K, V> {
    root_page_id: AtomicU32,
    buffer_pool: Arc<BufferPoolManager>,
    marker: PhantomData<(K, V)>
}

impl<K: Ord + Clone + BTreeSerialize,V: Clone + BTreeSerialize + Ord> BPlusTreeManager<K,V> {

    pub fn new(root_page_id: AtomicU32, buffer_pool: Arc<BufferPoolManager>) -> Self{
        BPlusTreeManager {root_page_id, buffer_pool, marker: PhantomData}
    }

    // allocates empty root leaf
    pub fn create(buffer_pool: Arc<BufferPoolManager>) -> Result<Self, FerroError> {
        let root_page_id = buffer_pool.new_page()?;
        let root_node = BPlusTreeLeafPage::<K, V>::new(root_page_id);
        let frame_i = buffer_pool.fetch_page(root_page_id)?;
        let mut frame = buffer_pool.frames[frame_i].write().unwrap();
        frame.data = root_node.serialize()?;
        drop(frame);
        buffer_pool.unpin_page(root_page_id, true);
        Ok(Self {root_page_id: AtomicU32::new(root_page_id), buffer_pool, marker: PhantomData})
    }

    // calls find_leaf, read it, call leaf.get and return value or None
    pub fn search(&self, key: &K) -> Result<Option<V>, FerroError> {
        let (page_id, _) = self.find_leaf(key.clone())?;
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        let frame = self.buffer_pool.frames[frame_i].read().unwrap();
        let leaf: BPlusTreeLeafPage<K, V> = BPlusTreeLeafPage::<K, V>::deserialize(frame.data)?;
        self.buffer_pool.unpin_page(page_id, false);
        match leaf.get(key) {
            Ok(Some(v)) => Ok(Some(v.clone())),
            Ok(None) => return Ok(None),
            Err(_) => return Err(FerroError::KeyNotFound)
        }
    }

    // find leaf then remove entry, if underfull, call handle_underfull
    pub fn delete(&mut self, key: &K) -> Result<(), FerroError> {
        Ok(())
    }

    // find_leaf to get the leaf, deserialize, try to insert, if not full, done. else, have to split and do stuff
    pub fn insert(&mut self, key: K, value: V) -> Result<(), FerroError> {
        Ok(())
    }

    // uses sibling pointers to traverse leaves and does range scan from start -> end
    pub fn range_scan(&self, start: &K, end: &K) -> Result<Vec<(K, V)>, FerroError> {
        todo!()
    }

    // HELPERS
    // traverse tree to leaf, remember to push to path stack if it's internal
    pub fn find_leaf(&self, key: K) -> Result<(u32, Vec<u32>), FerroError> { // (leaf_page_id, path_stack)
        let mut curr = self.root_page_id.load(Ordering::Relaxed);
        let mut stack: Vec<u32> = Vec::new();
        loop {
            let frame_i = self.buffer_pool.fetch_page(curr)?;
            let frame = self.buffer_pool.frames[frame_i].read().unwrap();
            let node = BPlusTreePage::<K, V>::deserialize(frame.data)?;
            drop(frame);
            self.buffer_pool.unpin_page(curr, false);

            match node {
                BPlusTreePage::Internal(n) => {
                    stack.push(curr);
                    curr = n.child_ptrs[n.find_child(&key) as usize];
                }
                BPlusTreePage::Leaf(_) => return Ok((curr, stack))
            }
        }
    }

    // fetch page, deserialize to BPlusTreePage, unpin
    pub fn read_node(&self, page_id: u32) -> Result<BPlusTreePage<K, V>, FerroError> {
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        let frame = self.buffer_pool.frames[frame_i].read().unwrap();
        let node = BPlusTreePage::<K, V>::deserialize(frame.data)?;
        drop(frame);
        self.buffer_pool.unpin_page(page_id, false);
        Ok(node)
    }

    //
    pub fn write_node_internal(&self, page_id: u32, node: &BPlusTreeInternalPage<K>) -> Result<(), FerroError> {
        todo!()
    }

    //
    pub fn write_node_leaf(&self, page_id: u32, node: &BPlusTreeLeafPage<K, V>) -> Result<(), FerroError> {
        todo!()
    }

    // prob have to use recursion, too complex to write here
    pub fn insert_into_parent(&mut self, path: &mut Vec<u32>, left_id: u32, mid_key: K, right_id: u32) -> Result<(), FerroError> {
        todo!()
    }

    // try to borrow from sibling else merge with a sibling and remove separator key from parent, recursing up. if the root is internal
    // and drops to one child, make that child the new root
    pub fn handle_underflow(&self, path: &mut Vec<u32>, node_id: u32) -> Result<(), FerroError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::fs::OpenOptions;
    use crate::{buffer, catalog::column::Value};
    
    fn setup() -> BPlusTreeManager::<Value, Value>{
        let _ = std::fs::remove_file("test_index.db");
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open("test_index.db").unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        BPlusTreeManager::<Value, Value>::create(bp.clone()).unwrap()
    }

    #[test]
    fn test_create_init_empty_leaf() {
        let tree = setup();
        let root_id = tree.root_page_id.load(Ordering::Relaxed);
        let root_node = tree.read_node(root_id).unwrap();
        match root_node {
            BPlusTreePage::Leaf(leaf) => {
                assert_eq!(leaf.page_id, root_id);
                assert_eq!(leaf.num_keys, 0);
                assert_eq!(leaf.key_arr.len(), 0);
                assert_eq!(leaf.vals.len(), 0);
            }
            BPlusTreePage::Internal(_) => unreachable!()
        }
    }

    #[test]
    fn test_search_empty_tree_returns_none() {
        let tree = setup();
        let search_key = Value::Integer(67);
        let result = tree.search(&search_key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_search_finds_key() {
        let tree = setup();
        let root_id = tree.root_page_id.load(Ordering::Relaxed);

        let frame_i = tree.buffer_pool.fetch_page(root_id).unwrap();
        let mut frame = tree.buffer_pool.frames[frame_i].write().unwrap();
        let mut leaf = BPlusTreeLeafPage::<Value, Value>::deserialize(frame.data).unwrap();
        let key = Value::Integer(69);
        let val = Value::Integer(6767);
        leaf.insert_entry(key.clone(), val.clone());
        leaf.insert_entry(Value::Integer(67), Value::Integer(6969));

        frame.data = leaf.serialize().unwrap();
        drop(frame);
        tree.buffer_pool.unpin_page(root_id, true);
        let result = tree.search(&key).unwrap();
        let bad_result = tree.search(&Value::Integer(76)).unwrap();
        assert_eq!(result, Some(val));
        assert_eq!(bad_result, None);
    }
}