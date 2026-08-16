//! D14 — the escrow safety invariant, over arbitrary interleavings.
//!
//! Escrow exists to make one statement true: **a bounded resource cannot be overdrawn.** Example
//! tests cover the interleavings someone imagined. These generate them, because the failure this
//! row was written to look for is precisely the ordering nobody pictured.
//!
//! That is not hypothetical here. Writing this row found a real hole first: `seal` released the
//! claim on merge as well as on abandon, so a *published* branch handed its spend back as though
//! the resource had never been consumed. Five sequential agents each took 12 from a pool of 20 and
//! drove the counter to −40 — escrow bounded concurrent agents and not sequential ones, which is
//! not a bound. Merge now settles; abandon still releases. These properties pin the distinction.

use std::collections::BTreeMap;

use proptest::prelude::*;

use ferrodb::agent_sql::escrow::{Cell, EscrowLedger};
use ferrodb::branch::types::BranchId;
use ferrodb::tel::ids::{ColId, RowId, TableId};

const CELL: Cell = (TableId(1), RowId(1), ColId(1));

fn branch(n: u64) -> BranchId {
    BranchId::new(n, 0)
}

/// What an agent does with the pool. `Settle` models a merge, `Release` an abandon.
#[derive(Debug, Clone)]
enum Act {
    Claim(u64, i64),
    Spend(u64, i64),
    Settle(u64),
    Release(u64),
}

fn acts() -> impl Strategy<Value = Vec<Act>> {
    prop::collection::vec(
        prop_oneof![
            (0u64..4, 1i64..15).prop_map(|(b, n)| Act::Claim(b, n)),
            (0u64..4, 1i64..15).prop_map(|(b, n)| Act::Spend(b, n)),
            (0u64..4).prop_map(Act::Settle),
            (0u64..4).prop_map(Act::Release),
        ],
        0..40,
    )
}

proptest! {
    /// **The invariant escrow exists for.** However agents interleave, the total that is actually
    /// *consumed* — spent and then settled, i.e. published — can never exceed the declared slack.
    ///
    /// Spends that are released instead of settled do not count, and must not: an abandoned
    /// branch's writes never reached the shared tables, so the resource was never used.
    #[test]
    fn consumed_never_exceeds_the_declared_slack(slack in 1i64..40, acts in acts()) {
        let mut led = EscrowLedger::new();
        led.open(CELL, slack).unwrap();

        // Model of what each branch has spent but not yet resolved.
        let mut unresolved: BTreeMap<u64, i64> = BTreeMap::new();
        let mut consumed = 0i64;

        for a in &acts {
            match *a {
                Act::Claim(b, n) => {
                    let _ = led.claim(branch(b), CELL, n);
                }
                Act::Spend(b, n) => {
                    if led.spend(branch(b), CELL, n).is_ok() {
                        *unresolved.entry(b).or_insert(0) += n;
                    }
                }
                Act::Settle(b) => {
                    // A merge: whatever this branch spent has really left the resource.
                    consumed += unresolved.remove(&b).unwrap_or(0);
                    led.settle_all(branch(b));
                }
                Act::Release(b) => {
                    // An abandon: the writes never landed, so nothing was consumed.
                    unresolved.remove(&b);
                    led.release(branch(b));
                }
            }
            prop_assert!(
                consumed <= slack,
                "consumed {consumed} of a {slack}-unit pool after {:?}", acts
            );
        }

        // Anything still outstanding would also have to fit if it settled.
        let outstanding: i64 = unresolved.values().sum();
        prop_assert!(
            consumed + outstanding <= slack,
            "consumed {consumed} + outstanding {outstanding} exceeds {slack} after {:?}", acts
        );
    }

    /// Outstanding claims can never exceed what the pool has left, at any point.
    #[test]
    fn claims_outstanding_never_exceed_the_remaining_pool(slack in 1i64..40, acts in acts()) {
        let mut led = EscrowLedger::new();
        led.open(CELL, slack).unwrap();

        for a in &acts {
            match *a {
                Act::Claim(b, n) => { let _ = led.claim(branch(b), CELL, n); }
                Act::Spend(b, n) => { let _ = led.spend(branch(b), CELL, n); }
                Act::Settle(b) => led.settle_all(branch(b)),
                Act::Release(b) => led.release(branch(b)),
            }
            let unclaimed = led.unclaimed(&CELL).unwrap();
            prop_assert!(
                unclaimed >= 0,
                "the pool went negative ({unclaimed}), so more was reserved than exists: {:?}", acts
            );
        }
    }

    /// A branch can never spend more than it reserved, whatever else is happening around it.
    #[test]
    fn a_branch_never_spends_beyond_its_own_claim(slack in 5i64..40, acts in acts()) {
        let mut led = EscrowLedger::new();
        led.open(CELL, slack).unwrap();

        let mut claimed: BTreeMap<u64, i64> = BTreeMap::new();
        let mut spent: BTreeMap<u64, i64> = BTreeMap::new();

        for a in &acts {
            match *a {
                Act::Claim(b, n) => {
                    if led.claim(branch(b), CELL, n).is_ok() {
                        *claimed.entry(b).or_insert(0) += n;
                    }
                }
                Act::Spend(b, n) => {
                    if led.spend(branch(b), CELL, n).is_ok() {
                        *spent.entry(b).or_insert(0) += n;
                        prop_assert!(
                            spent[&b] <= claimed.get(&b).copied().unwrap_or(0),
                            "branch {b} spent {} against a claim of {}",
                            spent[&b],
                            claimed.get(&b).copied().unwrap_or(0)
                        );
                    }
                }
                // The ledger has to be told too. Updating only the model here made the two
                // diverge, and the ledger then correctly allowed a spend the model had already
                // cleared — a counterexample about the test, not about escrow.
                Act::Settle(b) => {
                    led.settle_all(branch(b));
                    claimed.remove(&b);
                    spent.remove(&b);
                }
                Act::Release(b) => {
                    led.release(branch(b));
                    claimed.remove(&b);
                    spent.remove(&b);
                }
            }
        }
    }

    /// Releasing must return everything, so a crashed agent cannot shrink the resource. Settling
    /// must not, or the resource would never be consumed at all.
    #[test]
    fn release_restores_the_pool_and_settle_consumes_it(slack in 5i64..40, take in 1i64..5) {
        prop_assume!(take <= slack);
        let mut led = EscrowLedger::new();
        led.open(CELL, slack).unwrap();

        led.claim(branch(1), CELL, take).unwrap();
        led.spend(branch(1), CELL, take).unwrap();
        led.release(branch(1));
        prop_assert_eq!(
            led.unclaimed(&CELL), Some(slack),
            "an abandoned branch did not return its headroom"
        );

        led.claim(branch(2), CELL, take).unwrap();
        led.spend(branch(2), CELL, take).unwrap();
        led.settle_all(branch(2));
        prop_assert_eq!(
            led.unclaimed(&CELL), Some(slack - take),
            "a merged branch's spend was handed back, which is the -40 bug"
        );
    }
}
