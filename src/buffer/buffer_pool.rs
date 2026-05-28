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
    pub data: [u8; PAGE_SIZE],
    pub page_id: Option<u32>,
    pub pin_counter: AtomicU16,
    pub dirty_flag: AtomicBool,
}

pub struct BufferPoolManager {
    pub frames: Vec<RwLock<Frame>>,
    pub page_table: RwLock<HashMap<u32, usize>>, // page_id -> frame index
    pub disk_manager: Arc<DiskManager>,
    pub arc_cache: Mutex<ArcCache>,
}

const MAX_BUFFER_POOL_PAGES: usize = 1024;
impl BufferPoolManager {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self{
        let frames: Vec<RwLock<Frame>> = (0..MAX_BUFFER_POOL_PAGES).map(|_| RwLock::new(Frame::new())).collect();
        BufferPoolManager {frames, page_table: RwLock::new(HashMap::new()), disk_manager, arc_cache: Mutex::new(ArcCache::new(MAX_BUFFER_POOL_PAGES))}
    }

    // if cached, return page. else, load from disk into a frame (and evicting if all frames are full), then pin
    pub fn fetch_page(&self, page_id: u32) -> Result<usize, FerroError>{
        let result = self.arc_cache.lock().unwrap().request(page_id, &|id| {
            let pt = self.page_table.read().unwrap();
            let frame_i = pt[&id];
            let frame = self.frames[frame_i].read().unwrap();
            frame.pin_counter.load(Ordering::Relaxed) > 0
        });

        match result {
            ArcResult::Hit => { // page was already cached 
                let pt = self.page_table.read().unwrap();
                let frame_i = pt[&page_id];
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
    pub fn unpin_page(&self, page_id: u32, is_dirty: bool) {
        let pt = self.page_table.read().unwrap();
        let frame_i = pt[&page_id];
        drop(pt);

        let frame = self.frames[frame_i].read().unwrap();
        if is_dirty {
            frame.dirty_flag.store(true, Ordering::Relaxed);
        }
        let _ = frame.pin_counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
            if val > 0 {Some(val-1)} else {None}
        });
    }

    // allocate new page on disk using disk manager, load into a frame, return page id
    pub fn new_page(&self) -> Result<u32, FerroError>{
        let page_id = self.disk_manager.allocate()?;
        self.disk_manager.write(page_id, &[0u8; PAGE_SIZE])?;
        self.fetch_page(page_id)?;
        Ok(page_id)
    }

    // writes a dirty page to disk
    pub fn flush_page(&self, page_id: u32) -> Result<(), FerroError>{
        let pt = self.page_table.read().unwrap();
        let frame_i = pt[&page_id];
        drop(pt);

        let frame = self.frames[frame_i].read().unwrap();
        if frame.dirty_flag.load(Ordering::Relaxed) {
            self.disk_manager.write(page_id, &frame.data)?;
            frame.dirty_flag.store(false, Ordering::Relaxed);
            return Ok(());
        }
        Ok(())
    }

    // write all dirty pages to disk
    pub fn flush_all(&self) -> Result<(), FerroError>{
        let pt = self.page_table.read().unwrap();

        for (&page_id, &frame_i) in pt.iter() {
            let frame = self.frames[frame_i].read().unwrap();
            if frame.dirty_flag.load(Ordering::Relaxed) {
                self.disk_manager.write(page_id, &frame.data)?;
                frame.dirty_flag.store(false,Ordering::Relaxed);
            }
        }
        Ok(())
    }

    // remove from buffer pool, deallocate on disk
    pub fn delete_page(&self, page_id: u32) -> Result<(), FerroError>{
        let mut pt = self.page_table.write().unwrap();
        
        let frame_i = match pt.get(&page_id){
            Some(&i) => i,
            None => return Err(FerroError::KeyNotFound)
        };

        if self.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0 {
            return Err(FerroError::PagePinned);
        }
        pt.remove(&page_id);
        drop(pt);

        let mut frame = self.frames[frame_i].write().unwrap();
        frame.page_id = None;
        frame.data = [0u8; PAGE_SIZE];
        frame.pin_counter = AtomicU16::new(0);
        frame.dirty_flag = AtomicBool::new(false);
        drop(frame);

        self.arc_cache.lock().unwrap().remove(page_id)?;
        self.disk_manager.deallocate(page_id)?;
        Ok(())
    }
}

impl Frame {
    pub fn new() -> Self {
        Frame {data: [0u8; PAGE_SIZE], page_id: None, pin_counter: AtomicU16::new(0), dirty_flag: AtomicBool::new(false)}
    }
}