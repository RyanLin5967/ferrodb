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
        drop(frame);

        self.buffer_pool.unpin_page(page_id, false);
        match leaf.get(key) {
            Ok(Some(v)) => Ok(Some(v.clone())),
            Ok(None) => return Ok(None),
            Err(_) => return Err(FerroError::KeyNotFound)
        }
    }

    // find leaf then remove entry, if underfull, call handle_underfull
    pub fn delete(&self, key: &K) -> Result<(), FerroError> {
        let (page_id, _) = self.find_leaf(key.clone())?;
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        let mut frame = self.buffer_pool.frames[frame_i].write().unwrap();
        let mut leaf = BPlusTreeLeafPage::<K, V>::deserialize(frame.data)?;
        match leaf.remove_entry(key) {
            Ok(_) => {
                frame.data = leaf.serialize()?;
                drop(frame);
                self.buffer_pool.unpin_page(page_id, true);
            }
            Err(e) => {
                drop(frame);
                self.buffer_pool.unpin_page(page_id, false);
                return Err(e)
            }
        };
        // if underfull: handle_underflow() ... later
        Ok(())
    }

    // find_leaf to get the leaf, deserialize, try to insert, if not full, done. else, have to split and do stuff
    pub fn insert(&self, key: K, value: V) -> Result<(), FerroError> {
        let (page_id, mut stack) = self.find_leaf(key.clone())?;
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        let frame = self.buffer_pool.frames[frame_i].write().unwrap();
        let mut leaf = BPlusTreeLeafPage::<K, V>::deserialize(frame.data)?;
        drop(frame);
        leaf.insert_entry(key, value);
        if !leaf.is_full() { 
            let mut frame = self.buffer_pool.frames[frame_i].write().unwrap();
            frame.data = leaf.serialize()?;
            drop(frame);
            self.buffer_pool.unpin_page(page_id, true);
            return Ok(())
        }
        let new_page_id = self.buffer_pool.new_page()?;
        let (split_key, new_leaf) = leaf.split(new_page_id);

        if let Some(old_next_id) = new_leaf.next {
            let next_frame_i = self.buffer_pool.fetch_page(old_next_id)?;
            let next_frame = self.buffer_pool.frames[next_frame_i].write().unwrap();
            let mut next_leaf = BPlusTreeLeafPage::<K, V>::deserialize(next_frame.data)?;
            drop(next_frame);
            let mut next_frame = self.buffer_pool.frames[next_frame_i].write().unwrap();
            next_leaf.prev = Some(new_page_id);
            next_frame.data = next_leaf.serialize()?;
            drop(next_frame);
            self.buffer_pool.unpin_page(old_next_id, true);
        }
        let mut frame = self.buffer_pool.frames[frame_i].write().unwrap();
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

    // uses sibling pointers to traverse leaves and does range scan from start -> end (inclusive)
    // should later return an iterator to load results lazily cuz memory could overflow
    pub fn range_scan(&self, start: &K, end: &K) -> Result<Vec<(K, V)>, FerroError> {
        let mut keys = Vec::new();
        let (mut leaf_page_id, _) = self.find_leaf(start.clone())?;
        loop {
            let frame_i = self.buffer_pool.fetch_page(leaf_page_id)?;
            let frame = self.buffer_pool.frames[frame_i].read().unwrap();
            let curr_leaf_page = BPlusTreeLeafPage::<K,V>::deserialize(frame.data)?;
            drop(frame);
            self.buffer_pool.unpin_page(leaf_page_id, false);
            for i in 0..curr_leaf_page.key_arr.len() {
                if &curr_leaf_page.key_arr[i] > end {
                    return Ok(keys)
                }
                if &curr_leaf_page.key_arr[i] >= start {
                    keys.push((curr_leaf_page.key_arr[i].clone(), curr_leaf_page.vals[i].clone()));
                }
            }
            match curr_leaf_page.next {
                Some(next_id) => leaf_page_id = next_id,
                None => return Ok(keys)
            }
        }
    }

    // HELPERS
    
    // traverse tree to leaf, push to path stack if it's internal
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
                    curr = n.find_child(&key);
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

    // have to use recursion
    pub fn insert_into_parent(&self, stack: &mut Vec<u32>, left_id: u32, mid_key: K, right_id: u32) -> Result<(), FerroError> {
        // root was split, so need to allocate new root, make an internal node with one key (mid_key) and two children (left, right id)
        // update root_page_id, tree height grew
        if stack.is_empty() { 
            let new_page_id = self.buffer_pool.new_page()?;
            let mut new_root = BPlusTreeInternalPage::<K>::new(new_page_id);
            new_root.key_arr.push(mid_key);
            new_root.child_ptrs.push(left_id);
            new_root.child_ptrs.push(right_id);
            new_root.num_keys = 1;

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
        let parent_id = stack.pop().expect("non-empty");
        let frame_i = self.buffer_pool.fetch_page(parent_id)?;
        let mut parent_node = {
            let pf = self.buffer_pool.frames[frame_i].read().unwrap();
            BPlusTreeInternalPage::<K>::deserialize(pf.data)?
        }; // lock dropped

        if parent_node.is_full() {
            let new_parent_id = self.buffer_pool.new_page()?;   // no lock held
            let (up_key, mut new_parent) = parent_node.split(new_parent_id);

            if mid_key >= up_key {
                let index = new_parent.key_arr.binary_search(&mid_key).unwrap_or_else(|i| i);
                new_parent.insert_key_child(index, mid_key, right_id);
            } else {
                let index = parent_node.key_arr.binary_search(&mid_key).unwrap_or_else(|i| i);
                parent_node.insert_key_child(index, mid_key, right_id);
            }

            {
                let mut pf = self.buffer_pool.frames[frame_i].write().unwrap();
                pf.data = parent_node.serialize()?;
            }
            {
                let nf_i = self.buffer_pool.fetch_page(new_parent_id)?;
                let mut nf = self.buffer_pool.frames[nf_i].write().unwrap();
                nf.data = new_parent.serialize()?;
            }
            self.buffer_pool.unpin_page(new_parent_id, true);
            self.buffer_pool.unpin_page(parent_id, true);
            self.insert_into_parent(stack, parent_id, up_key, new_parent_id)?;
        } else {
            let index = parent_node.key_arr.binary_search(&mid_key).unwrap_or_else(|i| i);
            parent_node.insert_key_child(index, mid_key, right_id);
            {
                let mut pf = self.buffer_pool.frames[frame_i].write().unwrap();
                pf.data = parent_node.serialize()?;
            }
            self.buffer_pool.unpin_page(parent_id, true);
        }
        Ok(())
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
    use crate::{catalog::column::Value, storage::heap_file_manager::RecordId};
    
    fn setup() -> BPlusTreeManager::<Value, Value>{
        let _ = std::fs::remove_file("test_index.db");
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open("test_index.db").unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        BPlusTreeManager::<Value, Value>::create(bp.clone()).unwrap()
    }

    fn setup_leaf() -> BPlusTreeManager<Value, RecordId> {
        let _ = std::fs::remove_file("test_leaf.db");
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open("test_leaf.db").unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        BPlusTreeManager::<Value, RecordId>::create(bp.clone()).unwrap()
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

    #[test]
    fn test_simple_insert() {
        let tree = setup();
        let result = tree.insert(Value::Integer(67), Value::Integer(69));
        assert!(result.is_ok());
    }

    #[test]
    fn test_internal_node_split_a_lot() {
        let tree = setup();
        for i in 0..400 {
            let result = tree.insert(Value::Integer(i), Value::Integer(i));
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_many_level_insert_and_search() {
        let tree = setup();
        for i in 0..2000 {
            let _ = tree.insert(Value::Integer(i), Value::Integer(i *2)).unwrap_or_else(|e| panic!("insert {} failed: {:?}", i, e));
        }
        for i in 0..2000 {
            let res = tree.search(&Value::Integer(i)).unwrap();
            assert_eq!(res, Some(Value::Integer(i*2)));
        }
    }

    #[test]
    fn test_many_level_reverse() {
        let tree = setup();
        for i in (0..2000).rev() {
            tree.insert(Value::Integer(i), Value::Integer(i)).unwrap();
        }
        for i in 0..2000 {
            assert_eq!(tree.search(&Value::Integer(i)).unwrap(), Some(Value::Integer(i)));
        }
    }

    #[test]
    fn test_delete_basic() {
        let tree: BPlusTreeManager<Value, Value> = setup();
        let leaf = setup_leaf();
        tree.insert(Value::Integer(67),Value::Integer(6)).unwrap();
        tree.insert(Value::Integer(20), Value::Integer(7)).unwrap();

        leaf.insert(Value::Integer(67),RecordId::new(6, 1)).unwrap();
        leaf.insert(Value::Integer(20), RecordId::new(7, 2)).unwrap();
        tree.delete(&Value::Integer(67)).unwrap();
        leaf.delete(&Value::Integer(67)).unwrap();

        assert!(tree.search(&Value::Integer(67)).unwrap().is_none());
        assert!(leaf.search(&Value::Integer(67)).unwrap().is_none());
        assert_eq!(tree.search(&Value::Integer(20)).unwrap(), Some(Value::Integer(7)));
        assert_eq!(leaf.search(&Value::Integer(20)).unwrap(), Some(RecordId::new(7, 2)));
    }

    #[test]
    fn test_delete_not_real_key() {
        let tree = setup();
        assert!(tree.delete(&Value::Integer(934857)).is_err());
    }
}