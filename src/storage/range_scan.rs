use crate::error::FerroError;
use crate::{buffer::buffer_pool::BufferPoolManager, storage::index_page::BPlusTreeLeafPage};
use std::sync::Arc;
use std::ops::Bound;
use crate::storage::index_page::{ BPlusTreePage, BTreeSerialize};

pub struct RangeScanner<K, V> {
    pub buffer_pool: Arc<BufferPoolManager>,
    pub leaf: Option<BPlusTreeLeafPage<K, V>>,
    pub idx: usize,
    pub upper: Bound<K>,
}

impl<K: Ord + Clone + BTreeSerialize, V: Clone + BTreeSerialize> RangeScanner<K, V> {
    /// Copy one leaf out **under its page read latch**.
    ///
    /// Without the latch this walked the `next` chain straight through a concurrent split: D23
    /// measured a full scan returning 1649 of 2200 entries at 8 threads. The latch is released
    /// when this returns — the scan is not a repeatable read — but no page is read while a writer
    /// is part-way through rewriting it, and `src/storage/index.rs` writes a new leaf before
    /// publishing the `next` pointer to it, so this never arrives at an unwritten page.
    pub fn load_leaf(&self, page_id: u32) -> Result<BPlusTreeLeafPage<K, V>, FerroError> {
        let _latch = self.buffer_pool.page_latches.read(page_id);
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        let node = {
            let frame = self.buffer_pool.frames[frame_i].read().unwrap();
            BPlusTreePage::<K, V>::deserialize(frame.data)?
        };
        self.buffer_pool.unpin_page(page_id, false);
        match node {
            BPlusTreePage::Leaf(leaf) => Ok(leaf),
            _ => Err(FerroError::Io(String::from("expected leaf")))
        }
    }
}

impl<K: Clone + BTreeSerialize + Ord, V: Clone + BTreeSerialize> Iterator for RangeScanner<K, V> {
    type Item = Result<(K, V), FerroError>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf = self.leaf.as_ref()?;

            if self.idx < leaf.key_arr.len() {
                let key = leaf.key_arr[self.idx].clone();
                let past = match &self.upper {
                    Bound::Included(u) => &key > u,
                    Bound::Excluded(u) => &key >= u,
                    Bound::Unbounded => false
                };
                if past {
                    self.leaf = None;
                    return None;
                }

                let val = leaf.vals[self.idx].clone();
                self.idx += 1;
                return Some(Ok((key, val)));
            }

            match leaf.next {
                Some(next_id) => match self.load_leaf(next_id) {
                    Ok(l) => {
                        self.leaf = Some(l);
                        self.idx = 0;
                    }
                    Err(e) => {
                        self.leaf = None;
                        return Some(Err(e));
                    }
                }
                None => {
                    self.leaf = None;
                    return None;
                }
            }
        }
    }
}