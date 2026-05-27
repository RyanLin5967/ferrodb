use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::{Mutex, atomic::AtomicU16};
use std::collections::HashMap;
use crate::error::FerroError;
use crate::storage::disk_manager::{DiskManager, PAGE_SIZE};
use crate::buffer::arc::ArcCache;
use std::sync::RwLock;
use std::sync::atomic::Ordering;
use crate::buffer::arc::ArcResult;

pub struct Frame {
    data: [u8; PAGE_SIZE],
    page_id: Option<u32>,
    pin_counter: AtomicU16,
    dirty_flag: AtomicBool,
    frame_latch: RwLock<()>,
}

pub struct BufferPool {
    frames: Vec<RwLock<Frame>>,
    page_table: RwLock<HashMap<u32, usize>>, // page_id -> frame index
    disk_manager: Arc<DiskManager>,
    arc_cache: Mutex<ArcCache>,
}

const MAX_BUFFER_POOL_PAGES: usize = 1024;
impl BufferPool {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self{
        let frames: Vec<RwLock<Frame>> = (0..MAX_BUFFER_POOL_PAGES).map(|_| RwLock::new(Frame::new())).collect();
        BufferPool {frames, page_table: RwLock::new(HashMap::new()), disk_manager, arc_cache: Mutex::new(ArcCache::new(MAX_BUFFER_POOL_PAGES))}
    }

    // if cached, return page. else, load from disk into a frame (and evicting if all frames are full), then pin
    pub fn fetch_page(&mut self, page_id: u32) -> Result<usize, FerroError>{
        let result = self.arc_cache.lock().unwrap().request(page_id, &|id| {
            let pt = self.page_table.read().unwrap();
            let frame_i = self.page_table.read().unwrap()[&id];
            let frame = self.frames[frame_i].read().unwrap();
            frame.pin_counter.load(Ordering::Relaxed) > 0
        });

        match result {
            ArcResult::Hit => { // page was already cached 
                let pt = self.page_table.read().unwrap();
                let frame_i = self.page_table.read().unwrap()[&page_id];
                let frame = self.frames[frame_i].read().unwrap();
                frame.pin_counter.fetch_add(1, Ordering::Relaxed);
                return Ok(frame_i)
            }
            ArcResult::MissEvict(evicted_id) => { // page not cached and pool is full (victim eviction)
                let pt = self.page_table.read().unwrap();
                let frame_i = pt[&evicted_id];
                drop(pt);

                self.flush_page(evicted_id)?;
                let new_page_data = self.disk_manager.read(page_id)?;
                let mut frame = self.frames[frame_i].write().unwrap();
                frame.data = new_page_data;
                frame.page_id = Some(page_id);
                frame.pin_counter = AtomicU16::new(1);
                frame.dirty_flag = AtomicBool::new(false);
                drop(frame);

                let mut pt = self.page_table.write().unwrap();
                pt.remove(&evicted_id);
                pt.insert(page_id, frame_i);
                return Ok(frame_i)
            }
            ArcResult::MissNoEvict => { // page not cached, pool not full 
                let data = self.disk_manager.read(page_id)?;
                for i in 0..self.frames.len() {
                    let frame = self.frames[i].read().unwrap();
                    if frame.page_id.is_none() {
                        drop(frame);
                        let mut frame = self.frames[i].write().unwrap();
                        frame.data = data;
                        frame.page_id = Some(page_id);
                        frame.pin_counter = AtomicU16::new(1);
                        frame.dirty_flag = AtomicBool::new(false);
                        self.page_table.write().unwrap().insert(page_id, i);
                        return Ok(i);
                    }
                }
                unreachable!()
            }
            ArcResult::PoolFull => { // page not cached, pool is full, everything is pinned
                return Err(FerroError::NotEnoughSpace)
            }
        }
    }

    // decrement pin count, if page was modified, add dirty flag
    pub fn unpin_page(page_id: u32, is_dirty: bool) {

    }

    // allocate new page on disk using disk manager, load into a frame, return page id
    pub fn new_page() {

    }

    // writes a dirty page to disk
    pub fn flush_page(&self, page_id: u32) -> Result<(), FerroError>{
        Ok(())
    }

    // write all dirty pages to disk
    pub fn flush_all(&self) {

    }

    // remove from buffer pool, deallocate on disk
    pub fn delete_page(&self, page_id: u32) {

    }

}

impl Frame {
    pub fn new() -> Self {
        Frame {data: [0u8; PAGE_SIZE], page_id: None, pin_counter: AtomicU16::new(0), dirty_flag: AtomicBool::new(false), frame_latch: RwLock::new(())}
    }
}