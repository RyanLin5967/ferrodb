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

impl<K: Ord + Clone + BTreeSerialize,V: Clone + BTreeSerialize> BPlusTreeManager<K,V> {

    pub fn new(root_page_id: AtomicU32, buffer_pool: Arc<BufferPoolManager>) -> Self{
        BPlusTreeManager {root_page_id, buffer_pool, marker: PhantomData}
    }

    // allocates empty root leaf
    pub fn create(buffer_pool: Arc<BufferPoolManager>) -> Result<Self, FerroError> {
        let root_page_id = buffer_pool.disk_manager.allocate()?;
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
        let (page_id, stack) = self.find_leaf(key.clone())?;
        
        todo!()
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

}