//! D23 — the lock-order detector, fired through the **real** API rather than its internals.
//!
//! `src/storage/page_latch.rs`'s own unit tests drive the detector with `enter_pool()` directly,
//! which proves the assertion works but not that anything is *wired to it*. These go through
//! `BufferPoolManager` exactly as `src/storage/index.rs` does, so they fail if the tracked frame
//! accessors stop tracking — which is the way this guard would realistically rot.
//!
//! **Why these cannot hang.** Each uses a page nothing else has latched, so without the assertion
//! the inverted acquisition would simply succeed. The test exercises the *detector*, not a live
//! deadlock; a test that needed a real deadlock to prove a point would hang CI when it regressed
//! rather than fail it.
//!
//! This file lives in `tests/` and not beside the pool on purpose: `tests/lock_order_allowlist.rs`
//! forbids page latches anywhere in `src/` outside the tree, and these deliberately take one.
//!
//! **Two of these are `#[cfg(debug_assertions)]`, and that is a gating fix rather than a weakened
//! test.** The detector does not exist in release: `page_latch.rs`'s `mod order` is
//! `#[cfg(not(debug_assertions))]` there, `pool_depth()` const-folds to 0, and its module doc says
//! so in its own words. Asserting that an inversion panics is asserting the behaviour of a
//! debug-only tool, so it is scoped to debug. The release half is pinned by
//! `release_does_not_flag_the_inversion` below — neither profile is left with an unasserted claim,
//! and the two profiles' tests fail in opposite directions if the `cfg` ever moves.
//!
//! The two tests that are NOT gated — `the_correct_order_latch_then_frame_is_not_flagged` and
//! `pool_sections_do_not_leak_across_calls` — are profile-independent and must keep running in
//! both: they exercise the real acquisition order and guard-drop behaviour, which are production
//! code in every profile.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;

fn pool(tag: &str) -> (tempfile::TempDir, Arc<BufferPoolManager>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    (dir, bp)
}

/// The inversion the whole layer exists to prevent: hold a frame, then ask for a page latch.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "LOCK-ORDER INVERSION")]
fn a_page_latch_taken_while_holding_a_frame_write_lock_is_caught() {
    let (_dir, bp) = pool("inv_w");
    let page = bp.new_page().unwrap();
    let frame_i = bp.fetch_page(page).unwrap();

    let _frame = bp.frame_write(frame_i); // pool lock held ...
    let _latch = bp.page_latches.read(page); // ... and now reaching UP. Must panic.
}

/// Same for a shared frame lock — a reader can close the cycle just as well as a writer.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "LOCK-ORDER INVERSION")]
fn a_page_latch_taken_while_holding_a_frame_read_lock_is_caught() {
    let (_dir, bp) = pool("inv_r");
    let page = bp.new_page().unwrap();
    let frame_i = bp.fetch_page(page).unwrap();

    let _frame = bp.frame_read(frame_i);
    let _latch = bp.page_latches.write(page);
}

/// **The other direction must stay silent.** This is the shape every descent in `index.rs` has —
/// latch the page, then fetch and lock its frame — and it is legal. A detector that fired here
/// would fail every correct operation in the tree, so this is the test that proves the assertion
/// is discriminating rather than merely loud.
#[test]
fn the_correct_order_latch_then_frame_is_not_flagged() {
    let (_dir, bp) = pool("ok");
    let page = bp.new_page().unwrap();

    let _latch = bp.page_latches.write(page); // page latch FIRST ...
    let frame_i = bp.fetch_page(page).unwrap(); // ... then down into the pool ...
    let mut frame = bp.frame_write(frame_i); // ... and onto the frame.
    frame.data[0] = 0xAB;
    drop(frame);
    bp.unpin_page(page, true);
}

/// A pool section must close when its method returns, or the first `fetch_page` of an operation
/// would poison every later latch acquisition in the same operation. `index.rs` interleaves the
/// two constantly — fetch a node, latch its child, fetch that — so a leaked section would make the
/// tree panic on its second step.
#[test]
fn pool_sections_do_not_leak_across_calls() {
    let (_dir, bp) = pool("leak");
    let a = bp.new_page().unwrap();
    let b = bp.new_page().unwrap();

    for _ in 0..3 {
        let latch_a = bp.page_latches.read(a);
        let fi = bp.fetch_page(a).unwrap();
        drop(bp.frame_read(fi));
        bp.unpin_page(a, false);
        // If any of the calls above leaked a section, this second latch panics.
        let latch_b = bp.page_latches.read(b);
        drop(latch_b);
        drop(latch_a);
    }

    assert_eq!(bp.page_latches.outstanding(), 0, "a page latch guard leaked");
}

/// The RELEASE half, through the same real API. In debug the two `#[cfg(debug_assertions)]` tests
/// above require this exact sequence to panic; in release the tracker is compiled out and it must
/// simply succeed. Reaching the end of this function is the assertion.
///
/// If someone makes the detector real in release, this test FAILS — which is correct. Turning a
/// debug-time detector into a production guard that panics trades a rare latent hang for a hard
/// crash in a live database. That is a decision to make on purpose, with the module doc changed in
/// the same commit, not one to arrive as a side effect of deleting a `cfg`.
#[cfg(not(debug_assertions))]
#[test]
fn release_does_not_flag_the_inversion() {
    let (_dir, bp) = pool("release_noop");
    let page = bp.new_page().unwrap();
    let frame_i = bp.fetch_page(page).unwrap();

    {
        let _frame = bp.frame_write(frame_i); // pool lock held ...
        let _latch = bp.page_latches.read(page); // ... and reaching UP. Debug panics here.
    }

    bp.unpin_page(page, false);
    assert_eq!(bp.page_latches.outstanding(), 0, "a page latch guard leaked");
}
