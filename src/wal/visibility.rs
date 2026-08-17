use crate::{error::FerroError, storage::{heap_file_manager::{HeapFileManager, RecordId}, tuple::{Tuple, VersionHeader}}, wal::txn::ReadView};

pub fn resolve_visibility(view: &ReadView, tt_heap: &HeapFileManager, head: Tuple) -> Result<Option<Tuple>, FerroError> {
    let mut current = head;
    loop {
        let h = current.version_header()?;
        if view.visible(&h) {
            return Ok(Some(current));
        }
        match h.prev() {
            Some((page, slot)) => current = tt_heap.read(RecordId::new(page, slot))?,
            None => return Ok(None),
        }
    }
}

pub fn check_write_conflict(view: &ReadView, h: &VersionHeader) -> Result<(), FerroError> {
    if !view.is_commited_for_me(h.begin_ts) {
        return Err(FerroError::Txn("double write conflict".into()));
    }
    if h.end_ts != 0 && !view.is_commited_for_me(h.end_ts) {
        return Err(FerroError::Txn("double write conflict".into()))
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::txn::Snapshot;
    use std::collections::HashSet;

    /// A view for transaction `me`, with `active` still in flight and everything below
    /// `high_water` otherwise committed.
    fn view(me: u64, active: &[u64], high_water: u64) -> ReadView {
        ReadView {
            snapshot: Snapshot { high_water, active: active.iter().copied().collect::<HashSet<_>>() },
            txn_id: me,
        }
    }

    fn header(begin_ts: u64, end_ts: u64) -> VersionHeader {
        VersionHeader { begin_ts, end_ts, prev_page: 0, prev_slot: 0 }
    }

    /// **Write-write conflict detection was entirely unforced.**
    ///
    /// Found by a mutation sweep: BOTH arms of `check_write_conflict` could be replaced with
    /// `if false` and all 825 tests stayed green, while `update.rs` and `delete.rs` both call it on
    /// every row they touch. The string "double write conflict" appeared in no test — the other
    /// tests with `conflict` in their names are about MERGE conflicts in the effect log, which is a
    /// different mechanism entirely.
    ///
    /// Each arm is a lost update. The first: overwriting a version whose *creator* has not committed
    /// for me. The second: overwriting one whose *deleter* has not — two transactions both believing
    /// they own the row, and the later commit silently discarding the earlier one's work.
    #[test]
    fn overwriting_a_version_an_in_flight_transaction_created_is_a_conflict() {
        // Transaction 9 is still active; it created this version.
        let me = view(10, &[9], 20);
        let err = check_write_conflict(&me, &header(9, 0))
            .expect_err("a version created by an in-flight transaction was overwritten silently");
        assert!(
            format!("{err}").contains("conflict"),
            "it failed, but not as a write conflict: {err}"
        );
    }

    #[test]
    fn overwriting_a_version_an_in_flight_transaction_deleted_is_a_conflict() {
        // Created by 5 (committed for me), deleted by 9 (still active).
        let me = view(10, &[9], 20);
        let err = check_write_conflict(&me, &header(5, 9))
            .expect_err("a version deleted by an in-flight transaction was overwritten silently");
        assert!(
            format!("{err}").contains("conflict"),
            "it failed, but not as a write conflict: {err}"
        );
    }

    /// Anti-vacuity for both. Without this, a `check_write_conflict` that refused every write would
    /// pass the two tests above and break the database.
    #[test]
    fn an_uncontended_version_and_my_own_write_are_both_writable() {
        let me = view(10, &[9], 20);

        // Created by a committed transaction, never deleted.
        check_write_conflict(&me, &header(5, 0)).expect("an uncontended version was refused");

        // My own uncommitted write: `is_commited_for_me` is true for my own txn id, which is what
        // lets a transaction update a row it just wrote.
        check_write_conflict(&me, &header(10, 0)).expect("my own write was refused as a conflict");

        // Deleted by a transaction that HAS committed for me: the row is gone, not contended.
        check_write_conflict(&me, &header(5, 6)).expect("a committed delete was reported as a conflict");
    }
}
