//! Test-only seams in the buffer pool's fault path and in `BufferPoolManager::free_pages` (D237
//! review 2, N1). They let a unit test park a fault between its replacement-policy verdict and its
//! frame claim, and park `free_pages` between its pass 1 and pass 2, so the race N1 describes can be
//! driven deterministically instead of by timing. Two more (D237 review 3, R1) park `evict_into`
//! between its table lookup and its frame latch, and report when it reaches phase 2.
//!
//! In a non-test build every function here is an empty `#[inline(always)]` stub, so the pool pays
//! nothing. They live in their own file on purpose: `tests/lock_order_allowlist.rs` cuts
//! `buffer_pool.rs` at its first `#[cfg(test)]` text and inspects only what comes before it, so a
//! test-only item written inline there would silently shrink that guard's scope.

#[cfg(not(test))]
mod imp {
    use crate::buffer::buffer_pool::BufferPoolManager;

    /// What a parked fault carries to its second hook. Nothing, outside tests.
    pub(crate) struct Parked;

    #[inline(always)]
    pub(crate) fn park_before_claim(_pool: &BufferPoolManager, _page: u32) -> Parked {
        Parked
    }

    #[inline(always)]
    pub(crate) fn signal_filled(_parked: Parked) {}

    #[inline(always)]
    pub(crate) fn between_pass_1_and_2(_pool: &BufferPoolManager) {}

    #[inline(always)]
    pub(crate) fn between_evict_lookup_and_latch(_pool: &BufferPoolManager, _victim: u32) {}

    #[inline(always)]
    pub(crate) fn before_evict_phase_2(_pool: &BufferPoolManager, _victim: u32) {}
}

#[cfg(test)]
mod imp {
    use std::sync::Mutex;
    use std::sync::mpsc::{Receiver, Sender};

    use crate::buffer::buffer_pool::BufferPoolManager;

    /// Parks the fault of `page` in the pool at address `pool`: it sends `arrived` once it holds its
    /// verdict, waits on `release`, and sends `filled` once its frame holds the page's bytes.
    pub(crate) struct FaultPark {
        pub(crate) pool: usize,
        pub(crate) page: u32,
        pub(crate) arrived: Sender<()>,
        pub(crate) release: Receiver<()>,
        pub(crate) filled: Sender<()>,
    }

    /// Parks `free_pages` on the pool at address `pool` between pass 1 and pass 2: it sends
    /// `window_open` and waits on `resume`.
    pub(crate) struct FreePark {
        pub(crate) pool: usize,
        pub(crate) window_open: Sender<()>,
        pub(crate) resume: Receiver<()>,
    }

    /// Parks `evict_into(victim, ..)` on the pool at address `pool` between its `page_table` lookup
    /// and its frame latch (D237 review 3, R1): it sends `arrived` and waits on `release`.
    pub(crate) struct EvictPark {
        pub(crate) pool: usize,
        pub(crate) victim: u32,
        pub(crate) arrived: Sender<()>,
        pub(crate) release: Receiver<()>,
    }

    /// Tells the test that `evict_into(victim, ..)` on the pool at address `pool` got past phase 1,
    /// that is, it is about to take `page_table` for phase 2. It does not park.
    pub(crate) struct EvictPast {
        pub(crate) pool: usize,
        pub(crate) victim: u32,
        pub(crate) past: Sender<()>,
    }

    pub(crate) static FAULT: Mutex<Option<FaultPark>> = Mutex::new(None);
    pub(crate) static FREE: Mutex<Option<FreePark>> = Mutex::new(None);
    pub(crate) static EVICT: Mutex<Option<EvictPark>> = Mutex::new(None);
    pub(crate) static EVICT_PAST: Mutex<Option<EvictPast>> = Mutex::new(None);

    pub(crate) fn address(pool: &BufferPoolManager) -> usize {
        pool as *const BufferPoolManager as usize
    }

    pub(crate) struct Parked(Option<FaultPark>);

    pub(crate) fn park_before_claim(pool: &BufferPoolManager, page: u32) -> Parked {
        let mine = {
            let mut armed = FAULT.lock().unwrap();
            let hit = armed.as_ref().is_some_and(|f| f.pool == address(pool) && f.page == page);
            if hit { armed.take() } else { None }
        };
        if let Some(f) = &mine {
            let _ = f.arrived.send(());
            let _ = f.release.recv();
        }
        Parked(mine)
    }

    pub(crate) fn signal_filled(parked: Parked) {
        if let Some(f) = parked.0 {
            let _ = f.filled.send(());
        }
    }

    pub(crate) fn between_pass_1_and_2(pool: &BufferPoolManager) {
        let mine = {
            let mut armed = FREE.lock().unwrap();
            let hit = armed.as_ref().is_some_and(|f| f.pool == address(pool));
            if hit { armed.take() } else { None }
        };
        if let Some(f) = mine {
            let _ = f.window_open.send(());
            let _ = f.resume.recv();
        }
    }

    pub(crate) fn between_evict_lookup_and_latch(pool: &BufferPoolManager, victim: u32) {
        let mine = {
            let mut armed = EVICT.lock().unwrap();
            let hit = armed.as_ref().is_some_and(|e| e.pool == address(pool) && e.victim == victim);
            if hit { armed.take() } else { None }
        };
        if let Some(e) = mine {
            let _ = e.arrived.send(());
            let _ = e.release.recv();
        }
    }

    pub(crate) fn before_evict_phase_2(pool: &BufferPoolManager, victim: u32) {
        let mine = {
            let mut armed = EVICT_PAST.lock().unwrap();
            let hit = armed.as_ref().is_some_and(|e| e.pool == address(pool) && e.victim == victim);
            if hit { armed.take() } else { None }
        };
        if let Some(e) = mine {
            let _ = e.past.send(());
        }
    }
}

pub(crate) use imp::*;
