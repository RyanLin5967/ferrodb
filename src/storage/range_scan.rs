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
    /// Copy one leaf out under its page read latch.
    ///
    /// **This latch is defence in depth. Nothing currently demonstrates that it is load-bearing,
    /// and an earlier version of this comment claimed otherwise.** Removing it —
    /// `bench/d23_fire_check.txt`, BREAK C — leaves all four arms of
    /// `tests/integration_btree_concurrency.rs` green, including a scanner running concurrently
    /// with 32 writers over 49-98 full scans per round.
    ///
    /// The retracted claim was that "a full scan returned 1649 of 2200 entries at 8 threads
    /// without this latch". That measurement is real but it is **not** evidence for this latch: it
    /// was taken before the fix, when the split's write ordering was broken too, and the short
    /// scan was the write ordering. Attributing it here made a redundant guard look load-bearing.
    ///
    /// Two properties make the latch redundant *today*, and it is kept precisely because it is the
    /// cheap guard if either stops holding:
    ///
    /// - `index.rs::write_page` assigns `frame.data` under the frame WRITE lock while this takes
    ///   the READ lock, so the 4096-byte page is already copied atomically — a scanner cannot see
    ///   a half-rewritten leaf.
    /// - a split writes the new leaf **before** publishing the `next` pointer to it, so the chain
    ///   never points at an unwritten page.
    ///
    /// The latch is released when this returns, so a scan is not a repeatable read: entries
    /// inserted while it runs may or may not appear. Entries present when it started always do.
    pub fn load_leaf(&self, page_id: u32) -> Result<BPlusTreeLeafPage<K, V>, FerroError> {
        let _latch = self.buffer_pool.page_latches.read(page_id);
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        let node = {
            // Tracked accessor: the read latch above is held across this, so an inverted order
            // here must fail loudly rather than deadlock. See src/storage/page_latch.rs.
            let frame = self.buffer_pool.frame_read(frame_i);
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