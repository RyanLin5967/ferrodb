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
    pub fn delete(&self, key: &K) -> Result<(), FerroError> {
        Ok(())
    }

    // find_leaf to get the leaf, deserialize, try to insert, if not full, done. else, have to split and do stuff
    pub fn insert(&self, key: K, value: V) -> Result<(), FerroError> {
        let (page_id, mut stack) = self.find_leaf(key.clone())?;
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        let mut frame = self.buffer_pool.frames[frame_i].write().unwrap();
        let mut leaf = BPlusTreeLeafPage::<K, V>::deserialize(frame.data)?;
        leaf.insert_entry(key, value);
        if !leaf.is_full() { 
            frame.data = leaf.serialize()?;
            drop(frame);
            self.buffer_pool.unpin_page(page_id, true);
            return Ok(())
        }
        let new_page_id = self.buffer_pool.new_page()?;
        let (split_key, new_leaf) = leaf.split(new_page_id);

        if let Some(old_next_id) = new_leaf.next {
            let next_frame_i = self.buffer_pool.fetch_page(old_next_id)?;
            let mut next_frame = self.buffer_pool.frames[next_frame_i].write().unwrap();
            let mut next_leaf = BPlusTreeLeafPage::<K, V>::deserialize(next_frame.data)?;
            next_leaf.prev = Some(new_page_id);
            next_frame.data = next_leaf.serialize()?;
            drop(next_frame);
            self.buffer_pool.unpin_page(old_next_id, true);
        }
        frame.data = leaf.serialize()?;
        drop(frame);

        let new_frame_i = self.buffer_pool.fetch_page(new_page_id)?;
        let mut new_frame = self.buffer_pool.frames[new_frame_i].write().unwrap();
        new_frame.data = new_leaf.serialize()?;
        drop(new_frame);

        self.buffer_pool.unpin_page(new_page_id, true);
        self.buffer_pool.unpin_page(page_id, true);
        self.insert_into_parent(&mut stack, page_id, split_key, new_page_id)?;
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
    pub fn insert_into_parent(&self, stack: &mut Vec<u32>, left_id: u32, mid_key: K, right_id: u32) -> Result<(), FerroError> {
        // root was split, so need to allocate new root, make an internal node with one key (mid_key) and two children (left, right id)
        // update root_page_id, tree height grew
        if stack.is_empty() { 
            let new_page_id = self.buffer_pool.new_page()?;
            let mut new_root = BPlusTreeInternalPage::<K>::new(new_page_id);
            new_root.key_arr.push(mid_key);
            new_root.child_ptrs.push(left_id);
            new_root.child_ptrs.push(right_id);

            let frame_i = self.buffer_pool.fetch_page(new_page_id)?;
            let mut frame = self.buffer_pool.frames[frame_i].write().unwrap();
            frame.data = new_root.serialize()?;
            drop(frame);

            self.buffer_pool.unpin_page(new_page_id, true);
            self.root_page_id.store(new_page_id, Ordering::Relaxed);
            return Ok(())
            // later store the page id in catalog
        }
        // else, have to pop parent id from stack, read it, insert_key_child at right position, if parent not full, done, if it's full,
        // have to do internal split (where middle key moves up), write back both, recurse  with parent's middle key
        let parent_id: u32 = stack.pop().expect("");
        let frame_i = self.buffer_pool.fetch_page(parent_id)?;
        let mut parent_frame = self.buffer_pool.frames[frame_i].write().unwrap();
        let mut parent_node = BPlusTreeInternalPage::<K>::deserialize(parent_frame.data)?;
        if parent_node.is_full() {
            let new_parent_id = self.buffer_pool.new_page()?;
            let frame_i = self.buffer_pool.fetch_page(new_parent_id)?;
            let mut frame = self.buffer_pool.frames[frame_i].write().unwrap();
            let (up_key, mut new_parent) = parent_node.split(new_parent_id);

            if mid_key >= up_key {
                let index = match new_parent.key_arr.binary_search(&mid_key) {
                    Ok(i) => i,
                    Err(i) => i
                };
                new_parent.insert_key_child(index, mid_key, right_id);
            }else {
                let index = match parent_node.key_arr.binary_search(&mid_key) {
                    Ok(i) => i,
                    Err(i) => i,
                };
                parent_node.insert_key_child(index, mid_key, right_id);
            }
            frame.data = new_parent.serialize()?;
            drop(frame);
            
            parent_frame.data = parent_node.serialize()?;
            drop(parent_frame);

            self.buffer_pool.unpin_page(new_parent_id, true);
            self.buffer_pool.unpin_page(parent_id, true);
            self.insert_into_parent(stack, parent_id, up_key, new_parent_id)?;
            return Ok(())
        } else {
            let index = match parent_node.key_arr.binary_search(&mid_key) {
                Ok(i) => i,
                Err(i) => i
            };
            parent_node.insert_key_child(index, mid_key, right_id);
            parent_frame.data = parent_node.serialize()?;
            drop(parent_frame);

            self.buffer_pool.unpin_page(parent_id, true);
            return Ok(())
        }
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