//! **Test instrument (D233), observing only:** the pages THIS THREAD asked the buffer pool for,
//! through either read path, `BufferPoolManager::fetch_page` (latched) or
//! `BufferPoolManager::read_page_optimistic` (latch-free). A caller reads it before and after an
//! operation, and the difference is the pages that operation read.
//!
//! Thread-local on purpose: a counter shared across threads on the read path is the one word per
//! read that D51 measured as a contention wall and D58 removed. In a non-test build `count` is an
//! empty `#[inline(always)]` function, so the pool pays nothing.
//!
//! **Its own file on purpose.** `tests/lock_order_allowlist.rs::every_pool_method_that_locks_opens_a_pool_section`
//! cuts `buffer_pool.rs` at its first `#[cfg(test)]` text and inspects only what comes before it.
//! This counter used to live near the top of that file, which moved the cut above every method and
//! failed the test (D233 amendment 5).

#[cfg(not(test))]
mod imp {
    /// Count one page read on this thread. Nothing, outside tests.
    #[inline(always)]
    pub(crate) fn count() {}
}

#[cfg(test)]
mod imp {
    thread_local! {
        static PAGE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    /// Count one page read on this thread.
    pub(crate) fn count() {
        PAGE_READS.with(|c| c.set(c.get() + 1));
    }

    /// Pages this thread has read through any pool.
    pub(crate) fn on_this_thread() -> u64 {
        PAGE_READS.with(|c| c.get())
    }
}

pub(crate) use imp::*;
