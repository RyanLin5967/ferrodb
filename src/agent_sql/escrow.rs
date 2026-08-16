//! Escrow: partition a bounded counter's slack at fork, so overdraw fails at WRITE time.
//!
//! Design authority: DESIGN.md section 3, as corrected. The section used to claim bounded counters
//! need no special merge logic — compose the `Add`s and re-evaluate the guard against merged
//! state. That is measurably false. Guards are **preconditions**, evaluated against the merged
//! state *before* the ops apply, so with a start of 20 and two agents each taking 12 under
//! `qty >= 0`, the second merge tests `8 >= 0`, passes, and the counter lands at **−4**. The demo
//! prints exactly that. A precondition cannot see a post-op violation, so no amount of care at
//! merge time fixes it.
//!
//! Escrow does not make the merge cleverer. It moves the failure **earlier**, which is the only
//! place it can be both correct and useful:
//!
//! - at claim time the slack is partitioned, so two agents cannot each reserve 12 out of 20;
//! - at write time an agent that would exceed its own claim is refused, while it still has the
//!   context to do something about it — retry smaller, or ask for more.
//!
//! By merge time the arithmetic is already guaranteed to fit, so the merge has nothing to enforce.
//! That is the difference between a bound that holds and a bound that is checked too late to.

use std::collections::BTreeMap;

use crate::branch::types::BranchId;
use crate::error::FerroError;
use crate::tel::ids::{ColId, RowId, TableId};

/// One bounded cell: which table, row and column the resource lives in.
pub type Cell = (TableId, RowId, ColId);

/// A cell's slack and who is holding it.
#[derive(Debug, Clone)]
struct Pool {
    /// Total headroom above the floor when the pool was opened.
    slack: i64,
    /// Reserved but not yet spent, per branch.
    claimed: BTreeMap<u64, i64>,
    /// Spent against a claim, per branch.
    spent: BTreeMap<u64, i64>,
}

impl Pool {
    fn outstanding(&self) -> i64 {
        self.claimed.values().sum()
    }
    fn unclaimed(&self) -> i64 {
        self.slack - self.outstanding()
    }
}

/// Tracks claims over bounded cells.
#[derive(Debug, Default)]
pub struct EscrowLedger {
    pools: BTreeMap<Cell, Pool>,
}

impl EscrowLedger {
    pub fn new() -> Self {
        EscrowLedger { pools: BTreeMap::new() }
    }

    /// Declare a cell bounded, with `slack` units available above its floor.
    ///
    /// Re-opening an existing pool is refused rather than silently resizing it: changing the slack
    /// under live claims would let the sum of outstanding reservations exceed what exists, which is
    /// the exact failure escrow is here to prevent.
    pub fn open(&mut self, cell: Cell, slack: i64) -> Result<(), FerroError> {
        if slack < 0 {
            return Err(FerroError::Bind(format!("escrow slack must not be negative, got {slack}")));
        }
        if self.pools.contains_key(&cell) {
            return Err(FerroError::Bind(format!(
                "escrow pool for {cell:?} is already open; resizing it under live claims could \
                 let outstanding reservations exceed the resource"
            )));
        }
        self.pools.insert(cell, Pool { slack, claimed: BTreeMap::new(), spent: BTreeMap::new() });
        Ok(())
    }

    pub fn is_bounded(&self, cell: &Cell) -> bool {
        self.pools.contains_key(cell)
    }

    /// Headroom nobody has reserved yet.
    pub fn unclaimed(&self, cell: &Cell) -> Option<i64> {
        self.pools.get(cell).map(|p| p.unclaimed())
    }

    /// What `branch` has reserved and not yet spent.
    pub fn remaining(&self, branch: BranchId, cell: &Cell) -> Option<i64> {
        self.pools.get(cell).map(|p| {
            p.claimed.get(&branch.id).copied().unwrap_or(0)
                - p.spent.get(&branch.id).copied().unwrap_or(0)
        })
    }

    /// Reserve `amount` of a cell's slack for `branch`.
    ///
    /// This is the step that makes two agents unable to both take 12 out of 20: the second claim
    /// sees 8 unclaimed and is refused, at a point where the agent can still act on it.
    pub fn claim(&mut self, branch: BranchId, cell: Cell, amount: i64) -> Result<(), FerroError> {
        if amount <= 0 {
            return Err(FerroError::Bind(format!("an escrow claim must be positive, got {amount}")));
        }
        let pool = self.pools.get_mut(&cell).ok_or_else(|| {
            FerroError::Bind(format!("{cell:?} is not a bounded resource; nothing to claim"))
        })?;
        let free = pool.unclaimed();
        if amount > free {
            return Err(FerroError::Contraint(format!(
                "escrow claim of {amount} exceeds the {free} unit(s) still unclaimed on {cell:?}"
            )));
        }
        *pool.claimed.entry(branch.id).or_insert(0) += amount;
        Ok(())
    }

    /// Spend `amount` against `branch`'s claim. **This is the write-time check.**
    ///
    /// An unbounded cell is not this module's business and passes through — escrow applies only
    /// where a bound was declared.
    pub fn spend(&mut self, branch: BranchId, cell: Cell, amount: i64) -> Result<(), FerroError> {
        let Some(pool) = self.pools.get_mut(&cell) else {
            return Ok(());
        };
        if amount <= 0 {
            return Ok(()); // giving headroom back is always safe
        }
        let claimed = pool.claimed.get(&branch.id).copied().unwrap_or(0);
        let spent = pool.spent.get(&branch.id).copied().unwrap_or(0);
        let left = claimed - spent;
        if amount > left {
            return Err(FerroError::Contraint(format!(
                "write of {amount} exceeds this branch's remaining escrow of {left} on {cell:?} \
                 (claimed {claimed}, already spent {spent}); claim more before writing"
            )));
        }
        *pool.spent.entry(branch.id).or_insert(0) += amount;
        Ok(())
    }

    /// Return everything `branch` holds. Called when a branch merges or is abandoned — an agent
    /// that dies holding a claim must not strand the resource, which is the failure mode that
    /// makes reservation schemes unusable in practice.
    pub fn release(&mut self, branch: BranchId) {
        for pool in self.pools.values_mut() {
            pool.claimed.remove(&branch.id);
            pool.spent.remove(&branch.id);
        }
    }

    /// Permanently reduce a cell's slack by what `branch` actually spent, then drop its claim.
    /// Used when a branch's writes are published: the resource really was consumed.
    pub fn settle(&mut self, branch: BranchId, cell: &Cell) {
        if let Some(pool) = self.pools.get_mut(cell) {
            let spent = pool.spent.get(&branch.id).copied().unwrap_or(0);
            pool.slack -= spent;
            pool.claimed.remove(&branch.id);
            pool.spent.remove(&branch.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CELL: Cell = (TableId(1), RowId(1), ColId(1));
    fn b(n: u64) -> BranchId {
        BranchId::new(n, 0)
    }

    /// DESIGN.md section 3's worked example, which without escrow lands at -4.
    #[test]
    fn two_agents_cannot_both_reserve_twelve_out_of_twenty() {
        let mut e = EscrowLedger::new();
        e.open(CELL, 20).unwrap();

        e.claim(b(1), CELL, 12).expect("the first claim fits");
        let err = e
            .claim(b(2), CELL, 12)
            .expect_err("both agents reserved 12 out of 20, which is how the counter reached -4");
        assert!(format!("{err}").contains("exceeds the 8"), "got {err}");

        // The second agent can still get what actually exists, which is the point of failing here
        // rather than at merge: there is something useful to do about it.
        e.claim(b(2), CELL, 8).expect("the remaining slack must still be claimable");
        assert_eq!(e.unclaimed(&CELL), Some(0));
    }

    /// The write-time half. A claim that is not enforced on write is a suggestion.
    #[test]
    fn a_write_beyond_the_branchs_claim_is_refused_at_write_time() {
        let mut e = EscrowLedger::new();
        e.open(CELL, 20).unwrap();
        e.claim(b(1), CELL, 10).unwrap();

        e.spend(b(1), CELL, 6).expect("within the claim");
        assert_eq!(e.remaining(b(1), &CELL), Some(4));

        let err = e.spend(b(1), CELL, 6).expect_err("spent 12 against a claim of 10");
        let msg = format!("{err}");
        assert!(msg.contains("remaining escrow of 4"), "the error must say how much is left: {msg}");
        assert_eq!(e.remaining(b(1), &CELL), Some(4), "a refused write still consumed the claim");
    }

    #[test]
    fn a_branch_cannot_spend_against_a_claim_it_never_made() {
        let mut e = EscrowLedger::new();
        e.open(CELL, 20).unwrap();
        assert!(e.spend(b(1), CELL, 1).is_err(), "spent with no claim at all");
    }

    /// Escrow governs declared resources only. Silently policing every column would make the
    /// mechanism impossible to reason about.
    #[test]
    fn an_unbounded_cell_is_not_escrowed_and_writes_pass_through() {
        let mut e = EscrowLedger::new();
        assert!(!e.is_bounded(&CELL));
        e.spend(b(1), CELL, 1_000_000).expect("an undeclared cell must not be governed");
        assert_eq!(e.remaining(b(1), &CELL), None);
    }

    #[test]
    fn releasing_a_branch_returns_its_unspent_claim_to_the_pool() {
        let mut e = EscrowLedger::new();
        e.open(CELL, 20).unwrap();
        e.claim(b(1), CELL, 12).unwrap();
        assert_eq!(e.unclaimed(&CELL), Some(8));

        // An agent that dies holding a claim must not strand the resource.
        e.release(b(1));
        assert_eq!(e.unclaimed(&CELL), Some(20), "an abandoned claim stranded the resource");
        e.claim(b(2), CELL, 20).expect("the whole pool must be claimable again");
    }

    #[test]
    fn settling_consumes_only_what_was_actually_spent() {
        let mut e = EscrowLedger::new();
        e.open(CELL, 20).unwrap();
        e.claim(b(1), CELL, 12).unwrap();
        e.spend(b(1), CELL, 5).unwrap();

        e.settle(b(1), &CELL);
        // 5 really left the resource; the other 7 were never spent and go back.
        assert_eq!(e.unclaimed(&CELL), Some(15), "settling charged the claim instead of the spend");
    }

    #[test]
    fn reopening_a_pool_is_refused_rather_than_resizing_it() {
        let mut e = EscrowLedger::new();
        e.open(CELL, 20).unwrap();
        e.claim(b(1), CELL, 20).unwrap();
        let err = e.open(CELL, 5).expect_err("resizing under live claims must be refused");
        assert!(format!("{err}").contains("already open"), "got {err}");
        assert_eq!(e.unclaimed(&CELL), Some(0), "the pool was resized anyway");
    }

    #[test]
    fn a_negative_or_zero_claim_is_refused() {
        let mut e = EscrowLedger::new();
        e.open(CELL, 20).unwrap();
        assert!(e.claim(b(1), CELL, 0).is_err());
        assert!(e.claim(b(1), CELL, -5).is_err(), "a negative claim would mint headroom");
        assert_eq!(e.unclaimed(&CELL), Some(20));
    }

    /// The end-to-end statement: with escrow the counter cannot go below its floor, whatever
    /// order the agents interleave in.
    #[test]
    fn the_counter_cannot_be_driven_below_its_floor() {
        let mut e = EscrowLedger::new();
        e.open(CELL, 20).unwrap();

        // Every agent that gets a claim spends all of it; the pool is what bounds the total.
        let mut total_spent = 0i64;
        for id in 1..=5u64 {
            if e.claim(b(id), CELL, 12).is_ok() {
                e.spend(b(id), CELL, 12).unwrap();
                total_spent += 12;
            }
        }
        assert!(
            total_spent <= 20,
            "agents spent {total_spent} out of 20; the counter would be at {}",
            20 - total_spent
        );
    }
}
