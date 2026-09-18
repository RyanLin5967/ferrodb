//! **D45 — `LogBranchCatalog::set_root` and `renew_lease` are read-modify-writes with no lock
//! across the two halves, and this is the catalog an agent session gets by default.**
//!
//! D41 deleted `put` from the `BranchCatalog` trait and converted three sites to narrow atomic
//! operations. It did not convert these two, which were already narrow in their *signature* and
//! still whole-record in their *implementation*:
//!
//! ```ignore
//! fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
//!     let mut rec = self.get(branch)?;   // read lock ... released here
//!     rec.lease_deadline = lease;        // ... window ...
//!     self.put(&rec)                     // write lock: the WHOLE record goes back
//! }
//! ```
//!
//! `set_root` publishes a copy-on-write root — the commit point of shadow paging. `renew_lease` is
//! the keepalive, so a lost renewal leaves the deadline at its pre-renewal value and
//! `reap_expired` then reclaims a branch whose holder believes its lease is live. That is D29's
//! failure reached without `collapse` being involved at all.
//!
//! **Why this is two threads and not D41's decorator.** `tests/d41_narrow_ops_close_the_window.rs`
//! injects through a catalog DECORATOR, deliberately, because a racing test that fires sometimes
//! is not a gate. That technique cannot reach here: the window is between `LogBranchCatalog`'s own
//! `self.get` and `self.put`, inside the type, where no decorator sits. So the race is real — and
//! the assertion is made deterministic instead, by asserting the STATE THAT MUST HOLD AFTERWARDS
//! rather than catching the interleaving in the act.
//!
//! **The post-condition is exact in both directions, which is what makes it a gate rather than a
//! probability.** One thread advances only the root, the other only the deadline. Whatever order
//! they interleave in, if each write touches only its own field then the final record must carry
//! the LAST value of BOTH. It can only carry a stale one if some write put back a field it had
//! read earlier and never owned — which is precisely the defect. With the fix the assertion holds
//! always; without it, a losing interleave is what the loop count is for.

use std::sync::Arc;
use std::thread;

use ferrodb::branch::BranchCatalog;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline, PageId};

const ROUNDS: PageId = 4000;

fn subject() -> (Arc<LogBranchCatalog>, BranchId) {
    let cat = Arc::new(LogBranchCatalog::in_memory(1));
    let rec = cat.fork(BranchId::TRUNK, LeaseDeadline(1)).expect("fork");
    (cat, rec.branch_id)
}

/// Site 1: a published root must not be undone by a concurrent lease renewal.
#[test]
fn a_concurrent_renewal_does_not_discard_a_published_root() {
    let (cat, b) = subject();

    let a = {
        let cat = Arc::clone(&cat);
        thread::spawn(move || {
            for i in 1..=ROUNDS {
                cat.set_root(b, i).expect("set_root");
            }
        })
    };
    let c = {
        let cat = Arc::clone(&cat);
        thread::spawn(move || {
            for i in 1..=ROUNDS {
                cat.renew_lease(b, LeaseDeadline(i as u64)).expect("renew_lease");
            }
        })
    };
    a.join().unwrap();
    c.join().unwrap();

    let rec = cat.get(b).expect("get");
    assert_eq!(
        rec.root_page_id,
        ROUNDS,
        "the last published root was discarded by a concurrent renew_lease: \
         a whole-record write put back a `root_page_id` it had read before that publish. \
         This is the commit point of shadow paging, so the pages it published became invisible."
    );
    assert_eq!(
        rec.lease_deadline,
        LeaseDeadline(ROUNDS as u64),
        "the last lease renewal was discarded by a concurrent set_root. The holder believes its \
         lease runs to {}, the catalog says {}, and `reap_expired` reads the catalog.",
        ROUNDS,
        rec.lease_deadline.0
    );
}

/// The control. The same two fields, written by ONE thread in the same order and the same number
/// of times, must reach the same end state — so a failure above is the interleaving and not the
/// loop, the fork, or an off-by-one in the assertion itself.
#[test]
fn the_same_writes_serially_reach_the_same_state() {
    let (cat, b) = subject();
    for i in 1..=ROUNDS {
        cat.set_root(b, i).expect("set_root");
        cat.renew_lease(b, LeaseDeadline(i as u64)).expect("renew_lease");
    }
    let rec = cat.get(b).expect("get");
    assert_eq!(rec.root_page_id, ROUNDS);
    assert_eq!(rec.lease_deadline, LeaseDeadline(ROUNDS as u64));
}
