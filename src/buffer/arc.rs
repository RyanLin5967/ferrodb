use crate::buffer::linked_hash_set::LinkedHashSet;

pub struct ArcCache {
    pub capacity: usize,
    pub p: usize,
    pub t1: LinkedHashSet,
    pub t2: LinkedHashSet,
    pub b1: LinkedHashSet,
    pub b2: LinkedHashSet,
}

pub enum ArcResult {
    Hit, 
    MissEvict(u32), // page id is evicted, have to load new page into that frame
    MissNoEvict // not full, just load into empty frame
}

impl ArcCache {
    pub fn new (capacity: usize) -> Self {
        ArcCache {t1: LinkedHashSet::new(), t2: LinkedHashSet::new(), b1: LinkedHashSet::new(), b2: LinkedHashSet::new(), p:0, capacity}
    }

    pub fn request(&mut self, page_id: u32) -> ArcResult {
        // case 1: hit in t1 or t2
        if self.t1.contains(page_id) {
            self.t1.remove(page_id).unwrap();
            self.t2.insert(page_id).unwrap();
            return ArcResult::Hit;
        }

        if self.t2.contains(page_id) {
            self.t2.move_to_front(page_id).unwrap();
            return ArcResult::Hit;
        }

        // case 2: ghost hit on b1
        if self.b1.contains(page_id) {
            let delta = std::cmp::max(1, self.b2.len()/std::cmp::max(self.b1.len(),1));
            self.p = std::cmp::min(self.p + delta, self.capacity);
            let evicted = self.replace(page_id);
            self.b1.remove(page_id).unwrap();
            self.t2.insert(page_id).unwrap();

            return match evicted {
                Some(id) => ArcResult::MissEvict(id),
                None => ArcResult::MissNoEvict
            }
        }
        // case 3: ghost hit in b2
        if self.b2.contains(page_id) {
            let delta = std::cmp::max(1, self.b1.len() /std::cmp::max(1, self.b2.len()));
            self.p = self.p.saturating_sub(delta);
            let evicted = self.replace(page_id);
            self.b2.remove(page_id).unwrap();
            self.t2.insert(page_id).unwrap();

            return match evicted {
                Some(id) => ArcResult::MissEvict(id),
                None => ArcResult::MissNoEvict
            }
        }

        // case 4: complete miss
        let evicted = if self.b1.len() + self.t1.len() == self.capacity {
            if self.t1.len() < self.capacity {
                self.b1.pop_back().ok();
                self.replace(page_id)
            } else { // t1 is full, so no ghost entries, thus evict directly from t1
                let victim = self.t1.pop_back().ok();
                victim
            }
        } else if self.t1.len() + self.t2.len() + self.b1.len() + self.b2.len() >= self.capacity {
            if self.t1.len() + self.t2.len() + self.b1.len() + self.b2.len() == 2*self.capacity {
                self.b2.pop_back().ok();
            }
            self.replace(page_id)
        } else {
            None
        };

        self.t1.insert(page_id).unwrap();

        return match evicted {
            Some(id) => ArcResult::MissEvict(id),
            None => ArcResult::MissNoEvict
        }
    }   

    pub fn replace(&mut self, incoming: u32) -> Option<u32> {
        if self.t1.len() + self.t2.len() == 0 {
            return None;
        }
        if self.t1.len() > 0 && (self.t1.len() > self.p || (self.b2.contains(incoming) && self.t1.len() == self.p)){
            let victim = self.t1.pop_back().unwrap();
            self.b1.insert(victim).unwrap();
            Some(victim)
        } else if self.t2.len() > 0 {
            let victim = self.t2.pop_back().unwrap();
            self.b2.insert(victim).unwrap();
            Some(victim)
        } else {
            let victim = self.t1.pop_back().unwrap();
            self.b1.insert(victim).unwrap();
            Some(victim)
        }
    }   
}