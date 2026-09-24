//! **D33 — the D20 fix recorded arena ownership but never made it DURABLE.**
//!
//! D20 replaced `ArenaPageStore::alloc_arena`'s racy `get_raw` (unlocked) -> push -> `put`
//! (locked) with one atomic `BranchCatalog::add_arena`. That removed the race and the leak it
//! caused, and it was verified against a leak counter **inside one process**. Nothing in that
//! verification survived a restart, so nothing caught what the new method DID NOT do:
//!
//!   * `TableBranchCatalog::add_arena` wrote its key and returned. Every sibling writer
//!     (`put`, `set_root`, `fork`, `renew_lease`, `detach_child`, `release_id`) ends
//!     `let seq = self.stage()?; drop(_g); self.durable(seq)`. Two things follow. The ARENA key
//!     is never fsynced -- and `stage()` is the ONLY caller of `publish_root()`, so if the
//!     `upsert` is the write that splits the tree's root, the header page still names the OLD
//!     root and a reopen silently comes back on a valid B+tree of an older state.
//!   * `LogBranchCatalog::add_arena` mutated `state.records` and never called `self.append(..)`.
//!     That catalog rebuilds every record by replaying its log, so the arena was gone on reopen.
//!
//! The reaper frees exactly `record.arenas`. An extent that is reserved in the free-space map but
//! absent from the durable record after a restart is a **permanent leak** -- the same failure D20
//! set out to remove, moved from a race to a crash.
//!
//! Both arms reopen FROM DISK, which is the only thing that can see this.

use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{ArenaId, BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;

#[test]
fn table_catalog_add_arena_survives_a_reopen() {
    let path = std::env::temp_dir().join(format!("d33-table-{}.cat", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let (branch, root) = {
        let cat = TableBranchCatalog::open_sidecar(&path, 1).unwrap();
        let rec = cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
        // Enough arenas that at least one `upsert` is overwhelmingly likely to have split a node,
        // which is the half of this that loses UNRELATED records rather than just the arena.
        for i in 0..64u32 {
            cat.add_arena(rec.branch_id, ArenaId(i)).unwrap();
        }
        (rec.branch_id, cat.root_page_id())
    }; // dropped: no more in-memory state, exactly as a process exit leaves it

    let re = TableBranchCatalog::open_sidecar(&path, 1).unwrap();
    let back = re.get_raw(branch.id).expect("the branch itself must survive");
    let _ = root;
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        back.arenas.len(),
        64,
        "add_arena wrote {} of 64 arenas durably. The reaper frees exactly `record.arenas`, so \
         every arena missing here is an extent reserved in the free-space map that no branch \
         owns and nothing will ever free -- a permanent leak across a restart. `add_arena` is \
         the only mutating method in TableBranchCatalog that never calls stage()/durable().",
        back.arenas.len()
    );
}

#[test]
fn log_catalog_add_arena_survives_a_reopen() {
    let path = std::env::temp_dir().join(format!("d33-log-{}.branches", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let branch = {
        let cat = LogBranchCatalog::open(&path, 1).unwrap();
        let rec = cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
        for i in 0..8u32 {
            cat.add_arena(rec.branch_id, ArenaId(i)).unwrap();
        }
        rec.branch_id
    };

    let re = LogBranchCatalog::open(&path, 1).unwrap();
    let back = re.get_raw(branch.id).expect("the branch itself must survive");
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        back.arenas.len(),
        8,
        "add_arena recorded {} of 8 arenas in the LOG. It mutated the in-memory record and never \
         appended, and this catalog rebuilds every record by replaying its log -- so the ownership \
         is gone at restart and the reaper can never free those extents.",
        back.arenas.len()
    );
}

/// The arena must belong to the branch that asked for it, at the generation that asked.
/// `mem_catalog` and the log catalog both `get_mut(&branch.id)` and ignore `branch.generation`,
/// so a stale handle whose slot has been recycled attaches an arena to the slot's NEW occupant.
#[test]
fn add_arena_refuses_a_stale_generation() {
    let path = std::env::temp_dir().join(format!("d33-gen-{}.cat", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let cat = TableBranchCatalog::open_sidecar(&path, 1).unwrap();

    let first = cat.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
    let stale = first.branch_id;
    let raw = cat.get_raw(stale.id).unwrap();
    // state = Reaped, generation += 1, arenas cleared — all three are what `Reaped` MEANS, and
    // `set_state` is where that meaning lives now (D41).
    cat.set_state(stale, raw.state, ferrodb::branch::types::BranchState::Reaped).unwrap();
    cat.release_id(stale.id);

    let reused = cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
    let res = cat.add_arena(stale, ArenaId(77));
    let landed_on_reused = cat
        .get_raw(reused.branch_id.id)
        .map(|r| r.arenas.contains(&ArenaId(77)))
        .unwrap_or(false);
    let _ = std::fs::remove_file(&path);

    if reused.branch_id.id == stale.id {
        assert!(
            res.is_err() || !landed_on_reused,
            "add_arena accepted a STALE BranchId (generation {} against current {}) and attached \
             arena 77 to the slot's new occupant. Every other method routes through a generation \
             check; this one takes the id alone.",
            stale.generation, reused.branch_id.generation
        );
    }
}
