//! D32: the same curve as `branch_curve`, but **every branch writes one page**.
//!
//! `branch_curve` reached 10⁶ branches and is honest about what it measured — the CATALOG. Its hot
//! loop calls exactly one operation, `cat.fork(..)`, and it builds a `BufferPoolManager` it binds to
//! `_main_pool` and never uses. **No branch in that run ever wrote a page**, so its `cat MB` column
//! is catalog bytes and the data file stays empty.
//!
//! That matters because of D31: `arena_for` gives each writing branch its own extent, and
//! `alloc_arena` reserves `ARENA_EXTENT_PAGES` (256) of them at once. So a branch's FIRST 4 KB page
//! costs 1 MiB. Fork-only workloads never pay it; every real agent workload does.
//!
//! This harness adds the three calls `branch_curve` leaves out — `arena_for`, `next_epoch`,
//! `alloc_in_arena` — and reports **DATA FILE bytes**, because reporting the catalog column here
//! would reproduce exactly the blindness the repo already paid for: `branch_scaling_bench.rs` used
//! an in-memory catalog and therefore could not see an O(N²) durable cost at all. The instrument has
//! to exercise the path being claimed about.
//!
//!   branch_curve_writes [checkpoints,comma,separated] [threads] [byte_budget_gb]
//!
//! **READ-VS-N** (`bench/read_vs_n/PREREG.md`): `CURVE_ARMS=read,merge,restart` (any subset)
//! switches to the PRODUCTION layout (the database is opened by the shipped binary's own
//! `cli::open_database`, and each branch writes one row through `AgentRuntime::put_row`) and adds,
//! at every checkpoint, arm 1 (point-read cost against live N, with per-read census counters),
//! arm 2 axis (i) (K SQL `MERGE`s right after an open, so M is fixed while N grows) and arm 3 (the
//! full production open, timed in a fresh child process: this binary re-run as
//! `--open-only <db>`), then arm 2 axis (ii) after the last checkpoint (N fixed, M grows).
//! `CURVE_READ_K` sets reads per arm per thread count (default 16,384); `CURVE_MERGE_K` merges per
//! checkpoint (64); `CURVE_MERGE_M` axis (ii)'s M targets (256,1024,4096,16384);
//! `CURVE_FIRECHECK=<mode>` forces one guard to fire. Without `CURVE_ARMS` every line below runs
//! the historical D61/D65 path unchanged.
//!
//! **The byte budget is a refusal, not a tuning knob.** At 1 MiB/branch the 10⁶ point would need
//! ~1 TiB, which no machine here has, and a benchmark that fills the disk takes the machine down
//! with it. It stops at the budget and SAYS SO, naming the N it reached. "Stopped early on space" is
//! the result, not a failure of the run — and if it does NOT stop early, that kills D31, which is
//! the outcome D31's own falsifier asks for.
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::{merge_log_counters, table_id, AgentRuntime, StateSizes};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::lease_thread::{scan_interval_from_env, CatalogLock, LeaseStats};
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, BranchState, LeaseDeadline, PageId, ARENA_EXTENT_PAGES};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::buffer::read_census::{this_thread, ReadCensus};
use ferrodb::catalog::column::Value;
use ferrodb::cli::cli::{open_database, OpenDatabase};
use ferrodb::cluster::ClusterScope;
use ferrodb::consensus::NodeId;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::index_scan::index_scan_counters;
use ferrodb::execution::seq_scan::seq_scan_counters;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::wal::log::FSYNC_CALLS;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::BPlusTreePage;

/// Say which of the two worlds this number came from, and NAME THE HYPOTHESIS.
///
/// **This function exists because the line it replaces read backwards after the fix landed.**
/// The old text was *"If that is far below 1048576 B, the first-page amplification is NOT binding
/// here and D31 should be CLOSED rather than built"*. That reading holds only while geometric
/// extent growth is ABSENT: then a run that completes the budget really does falsify D31. Once the
/// growth rule is BUILT, the same branch prints for the opposite reason — the run completes
/// *because the fix worked* — so an unconditioned verdict tells its reader to close the row at the
/// exact moment the row succeeded, and a reader with no context believes the line rather than
/// inferring the inversion.
///
/// A conditional verdict has to name the hypothesis it is conditioned on. The two reference values
/// below are what distinguishes the cases, so both are printed in every outcome, including the one
/// where neither fits.
fn verdict(bytes_per_branch: f64) {
    /// One page: what a branch's first page costs when extents grow geometrically from one.
    const GROWN: f64 = PAGE_SIZE as f64;
    /// One whole extent: what it cost when every extent was `ARENA_EXTENT_PAGES` long.
    const FLAT: f64 = (ARENA_EXTENT_PAGES as usize * PAGE_SIZE) as f64;
    /// "Within a small factor of". Generous on purpose — the catalog, the trunk's own pages and
    /// the partially filled last extent all land in the gap, and the two references are 256x
    /// apart, so nothing can be near both.
    const NEAR: f64 = 8.0;

    println!();
    println!("  reference: one page = {GROWN:.0} B  ·  one full extent = {FLAT:.0} B  ({}x apart)",
             ARENA_EXTENT_PAGES);
    if bytes_per_branch <= GROWN * NEAR {
        println!("VERDICT — AMPLIFICATION GONE. {bytes_per_branch:.0} B/branch is within {NEAR:.0}x of a");
        println!("single page, so a branch's first page costs about a page. This is the tree WITH");
        println!("geometric extent growth (D31) built; without it this number is ~{FLAT:.0}.");
    } else if bytes_per_branch >= FLAT / NEAR {
        println!("VERDICT — AMPLIFICATION PRESENT. {bytes_per_branch:.0} B/branch is within {NEAR:.0}x of a");
        println!("whole extent, so every branch pays {ARENA_EXTENT_PAGES} pages for its first one. This is the");
        println!("tree WITHOUT geometric extent growth, and it is what D31 exists to remove.");
    } else {
        println!("VERDICT — AMBIGUOUS. {bytes_per_branch:.0} B/branch sits between the two references above,");
        println!("near neither. Do NOT read this as either result: say which tree it was taken on,");
        println!("and look at `pages live` and the allocated-blocks column before concluding.");
    }
}


/// Bytes actually ALLOCATED to a file, as opposed to its addressed length -- `None` where the
/// platform cannot answer.
///
/// This is the whole instrument of D31: the gap between `len()` and allocation is the 256x space
/// amplification, so a fabricated number here would fabricate the finding. `std` exposes
/// `MetadataExt::blocks()` on unix only and has no Windows equivalent, so Windows gets `None` and
/// the caller prints `NaN` rather than a zero that reads as "no amplification".
///
/// Gated the way `storage::disk_manager::pwrite` already gates its platform split.
fn allocated_bytes(path: &std::path::Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path).ok().map(|m| m.blocks() * 512)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}


/// Free bytes on the filesystem holding `path`, via `df -k`.
///
/// **A shared-machine guard, not a tuning knob — D61.** The byte budget below refuses to let this
/// run's own database grow past a size; it says nothing about what else is on the disk. Another
/// session on this machine tripped a disk monitor twice on 2026-09-19 while this repo held three
/// worktree targets, and an ENOSPC in someone else's lane reads exactly like a real test failure.
/// So the run also stops when the DISK is low, whatever its own database weighs.
fn free_bytes(path: &std::path::Path) -> Option<u64> {
    let out = std::process::Command::new("df").arg("-k").arg(path).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text.lines().nth(1)?;
    let avail_kb: u64 = line.split_whitespace().nth(3)?.parse().ok()?;
    Some(avail_kb * 1024)
}

/// Stop if the filesystem drops below this, whatever this run's own budget says. See
/// [`free_bytes`]. 20 GiB leaves a working margin for every other lane on a shared machine.
const FREE_FLOOR: u64 = 20 * (1u64 << 30);

/// Removes the run directory when dropped — including during a panic's unwind.
struct RemoveOnDrop(std::path::PathBuf);
impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn be_u32(b: &[u8], at: usize) -> Option<usize> {
    b.get(at..at.checked_add(4)?).map(|s| u32::from_be_bytes(s.try_into().unwrap()) as usize)
}

/// Byte offset of the `current` section in a v3 arena image, walking the layout
/// `ArenaPageStore::image_len` walks: 21-byte header, counted free list of 8-byte entries, counted
/// extents of 28 bytes plus a counted recycled list of 4-byte ids. `None` for anything else.
///
/// A mis-walk cannot produce a false pass: [`reload_control`] requires the count found at this
/// offset to equal the run's branch count, which a wrong offset does not reproduce.
fn current_section_offset(img: &[u8]) -> Option<usize> {
    if img.first() != Some(&3) {
        return None;
    }
    let mut at = 21usize;
    let n_free = be_u32(img, at)?;
    at = at.checked_add(n_free.checked_mul(8)?.checked_add(4)?)?;
    let n_ext = be_u32(img, at)?;
    at = at.checked_add(4)?;
    for _ in 0..n_ext {
        at = at.checked_add(28)?;
        let rec = be_u32(img, at)?;
        at = at.checked_add(rec.checked_mul(4)?.checked_add(4)?)?;
    }
    Some(at)
}

/// D65 run 2's POSITIVE CONTROL on the arena column: did the timed reopen really load the map?
///
/// ⛔ **Run 2 first shipped this as `reopened.state_bytes() == img`, which can NEVER hold for
/// N > 0**: `ArenaPageStore::load_state` deserialises `current` and then deliberately clears it
/// ("Never resume filling a restored extent"), so the re-serialised state is `16 * N` bytes
/// shorter than the image. Found by review before the first run; the column would have read "NO"
/// at every checkpoint and voided itself under its own pre-registration.
///
/// The control now states the round trip `load_state` actually promises. Relative to `img`, the
/// re-serialisation `reload` must have: every byte before `current` (header, free list, EVERY
/// extent with its owner, start, length, fill mark and recycled list) identical; `current` present
/// in `img` with exactly `n` entries and EMPTY in `reload`; the `pending` section identical; and a
/// length exactly `16 * n` shorter. The CRC is not compared — `state_bytes` recomputes it.
///
/// What it proves: the extents section — the O(extents) part this column exists to time — was
/// deserialised in full, because a store that skipped it re-serialises an empty extent list.
/// What it does not prove: that the timer contained only that work.
fn reload_control(img: &[u8], reload: &[u8], n: usize) -> Result<(), &'static str> {
    let cur = current_section_offset(img).ok_or("walk")?;
    let n_cur = be_u32(img, cur).ok_or("walk")?;
    if n_cur != n {
        return Err("walk");
    }
    let removed = 16 * n_cur;
    if img.len() < cur + 4 + removed + 4 || reload.len() + removed != img.len() {
        return Err("len");
    }
    if reload[..cur] != img[..cur] {
        return Err("ext");
    }
    if reload[cur..cur + 4] != [0u8; 4] {
        return Err("cur");
    }
    if reload[cur + 4..reload.len() - 4] != img[cur + 4 + removed..img.len() - 4] {
        return Err("pend");
    }
    Ok(())
}

// =================================================================================================
// READ-VS-N — `bench/read_vs_n/PREREG.md`. Everything below this line and above `main` exists for
// `CURVE_ARMS`, and none of it runs without it.
// =================================================================================================

/// The table and row every branch writes, and trunk's value for it.
const TABLE: &str = "t";
const ROW: u64 = 1;
const TRUNK_VALUE: i64 = -1;
/// Reads per timed block. Arms alternate block by block, so both see the same box.
const BLOCK: usize = 64;
/// FIXED thread counts for the read arm, per the brief: one, then eight.
const READ_THREADS: [usize; 2] = [1, 8];
/// The parent's lease thread runs its first pass and then sleeps for this long. The leases in this
/// workload never expire, so its only other work would be the 60 s orphan sweep — a full catalog
/// walk dropped into a timed window at a random moment. The CHILD, which is the restart being
/// measured, uses the production interval.
const PARENT_SCAN_INTERVAL: Duration = Duration::from_secs(86_400);

/// Which PREREG arms this invocation runs. `None` from [`Arms::from_env`] means the historical
/// D61/D65 path, unchanged.
#[derive(Clone, Copy, Debug)]
struct Arms {
    read: bool,
    merge: bool,
    restart: bool,
}

impl Arms {
    /// `CURVE_ARMS=read,merge,restart`, any subset. Refuses a name it does not know rather than
    /// running less than was asked for.
    fn from_env() -> Option<Arms> {
        let raw = std::env::var("CURVE_ARMS").ok()?;
        let mut arms = Arms { read: false, merge: false, restart: false };
        for name in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            match name {
                "read" => arms.read = true,
                "merge" => arms.merge = true,
                "restart" => arms.restart = true,
                other => panic!("CURVE_ARMS: unknown arm {other:?}; the arms are read, merge, restart"),
            }
        }
        assert!(arms.read || arms.merge || arms.restart, "CURVE_ARMS={raw:?} names no arm");
        Some(arms)
    }
}

/// `CURVE_FIRECHECK=<mode>`: break exactly one guard's premise on purpose, so the guard is seen to
/// fire (PREREG section 4). Four kinds (A11.5, A13.1), labelled on each variant:
///
/// * COMPARISON: offsets or swaps the expected value, or drops a measured delta, at the harness's
///   own comparison. It proves that comparison and its NOT A RESULT line work, and nothing more.
/// * CONTROL: changes what the timed CONTROL arm does (arm 1), so the guard watching the control
///   sees real work move. The measured branch path is untouched.
/// * IN PATH (A7.5, A8, A11.6): a real state change that the measured code then sees.
/// * JUDGE (A13.1): a real state change that moves a JUDGED integer, which the verdict script
///   checks against the pre-registration. No harness guard reads it, so the run exits 0.
///
/// Every mode needs one arm, so a mode whose arm is not in `CURVE_ARMS` is refused at startup
/// (A11.4, A12.3): it could only print "every guard held" having injected nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fire {
    None,
    /// G2 (comparison): the branch arm expects its NEIGHBOUR's value.
    WrongPage,
    /// G3 (comparison): the branch arm's census deltas are discarded before the comparison.
    CensusOff,
    /// G4 (control): the control also asks the catalog.
    ControlCatalog,
    /// G5 (control): the control reads a random branch's page by id, which misses at large N.
    ControlCold,
    /// G6 (comparison): the identity is checked against height + 1.
    WrongHeight,
    /// G7 (control): the control spins 10·2^checkpoint µs per read, so it grows with N.
    ControlDrift,
    /// H2 (comparison): the parent expects one arena more than it counted.
    WrongArenas,
    /// H3 (comparison): the parent expects one live branch more than it counted.
    WrongLive,
    /// M1 (in path): every merge's branch is really quarantined first, so `MERGE` returns `Ok`
    /// having published nothing.
    MergeQuarantined,
    /// M2 (comparison): axis (i) expects its batch to start at one applied entry, not zero.
    WrongStart,
    /// M3 (comparison): the identity expects one visit more than the log held.
    WrongVisits,
    /// M4 (comparison): the constant-delta check expects one op more than the first merge appended.
    WrongDelta,
    /// M5 (comparison): the parent expects one live branch more after a merge batch than before it.
    WrongLiveMerge,
    /// M6 (comparison check, A9.4): the counter-derived `checkpointed` flag is inverted before it is
    /// compared with `wal.base_lsn`.
    WrongCkptFlag,
    // ---- IN PATH (PREREG A7 item 5), besides M1 above: a real state change the measured code sees
    /// G1: a real fork at the first checkpoint that `branches` never records.
    ExtraBranch,
    /// H1: the parent HOLDS `{db}.lock` while the child opens, so the child's `DbLock::acquire`
    /// refuses and it exits non-zero.
    ChildLocked,
    /// H2 (PREREG A8): after the parent counts live arenas, it claims ONE more real extent, for
    /// trunk, so the child's OPEN sweep visits one arena the parent did not count. It replaces
    /// `wrong-sweep-read`, which read the shared counter after the first lease pass and so could not
    /// fire once D209 stops that pass from sweeping again. The open sweep, which this touches, is
    /// the same with and without D209.
    ExtraExtent,
    /// H4: before each restart, a real fork claims an empty extent and is then marked `Reaped` —
    /// the D40 crash-orphan shape — so the child's open sweep frees it.
    OrphanExtent,
    /// H5: at the LAST checkpoint the child is a cluster member with no applied `LeaseTick`, so its
    /// lease passes refuse before the orphan sweep and `LeaseStats::finished` never rises.
    NoClusterTime,
    /// M6 (A11.6): axis (ii) runs holding a real WAL pin (`pin_durable`), so each auto-checkpoint
    /// resets the commit counter while `truncate` keeps the log: A10.2's one legitimate
    /// disagreement, produced for real. Needs an M target of at least one checkpoint interval.
    PinnedCheckpoint,
    /// H6 (A12.2): before the FIRST restart the parent leaves the stale-index marker beside the log,
    /// the file a failed index undo leaves (`TxnManager::mark_indexes_stale`), so the child's open
    /// finds it and rebuilds for it.
    StaleMarker,
    /// A12.1's replay bytes (judge, A13.1): one real `CREATE TABLE` right after axis (ii)'s reopen
    /// puts one `Ddl` record in `schema_log`, so every axis-(ii) checkpoint re-appends it and the
    /// bytes line gains an intercept of exactly that record's size.
    CkptDdl,
}

/// The arm a fire mode needs (A12.3): the one holding the guard it breaks, or for `ckpt-ddl` the
/// integer it moves. `Any` is G1, which every arm set checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NeedsArm {
    Any,
    Read,
    Merge,
    Restart,
}

impl Fire {
    fn from_env() -> Fire {
        match std::env::var("CURVE_FIRECHECK") {
            Err(_) => Fire::None,
            Ok(mode) => Fire::parse(&mode),
        }
    }

    /// The arm this mode needs; `None` for no fire. Exhaustive, with no `_` arm, so a
    /// new mode does not compile until it names its arm (A12.3).
    fn needs(self) -> Option<NeedsArm> {
        Some(match self {
            Fire::None => return None,
            Fire::WrongPage
            | Fire::CensusOff
            | Fire::ControlCatalog
            | Fire::ControlCold
            | Fire::WrongHeight
            | Fire::ControlDrift => NeedsArm::Read,
            Fire::MergeQuarantined
            | Fire::WrongStart
            | Fire::WrongVisits
            | Fire::WrongDelta
            | Fire::WrongLiveMerge
            | Fire::WrongCkptFlag
            | Fire::PinnedCheckpoint
            | Fire::CkptDdl => NeedsArm::Merge,
            Fire::WrongArenas
            | Fire::WrongLive
            | Fire::ChildLocked
            | Fire::ExtraExtent
            | Fire::OrphanExtent
            | Fire::NoClusterTime
            | Fire::StaleMarker => NeedsArm::Restart,
            Fire::ExtraBranch => NeedsArm::Any,
        })
    }

    /// What this mode must fire, and what the pre-registration allows beside it, by guard id
    /// (A20.1). Exhaustive, with no `_` arm, so a new mode must declare both. The FIRECHECK line
    /// prints them, and the verdict script cross-checks them against its own table.
    fn expects(self) -> (&'static [&'static str], &'static [&'static str]) {
        match self {
            Fire::None => (&[], &[]),
            Fire::WrongPage => (&["G2"], &[]),
            Fire::CensusOff => (&["G3"], &[]),
            Fire::ControlCatalog => (&["G4"], &[]),
            Fire::ControlCold => (&["G5"], &["G7"]),
            Fire::WrongHeight => (&["G6"], &[]),
            Fire::ControlDrift => (&["G7"], &[]),
            Fire::WrongArenas => (&["H2"], &[]),
            Fire::WrongLive => (&["H3"], &[]),
            Fire::MergeQuarantined => (&["M1"], &["M5", "G1"]),
            Fire::WrongStart => (&["M2"], &[]),
            Fire::WrongVisits => (&["M3"], &[]),
            Fire::WrongDelta => (&["M4"], &[]),
            Fire::WrongLiveMerge => (&["M5"], &[]),
            Fire::WrongCkptFlag => (&["M6"], &[]),
            Fire::ExtraBranch => (&["G1"], &[]),
            Fire::ChildLocked => (&["H1"], &["ARM3"]),
            Fire::ExtraExtent => (&["H2"], &[]),
            Fire::OrphanExtent => (&["H4"], &[]),
            Fire::NoClusterTime => (&["H5"], &[]),
            Fire::PinnedCheckpoint => (&["M6"], &[]),
            Fire::StaleMarker => (&["H6"], &["ARM3"]),
            // A JUDGE fire: no guard; its own FIRECHECK line names what may print (A20.3).
            Fire::CkptDdl => (&[], &[]),
        }
    }

    /// Why this mode cannot run with these arms, if it cannot. A refusal, not a warning: each case
    /// would otherwise end in "every guard held" having tested nothing (A9.2, A11.4, A12.3).
    fn refusal(self, arms: Option<Arms>) -> Option<String> {
        let need = self.needs()?;
        let Some(a) = arms else {
            return Some(format!(
                "CURVE_FIRECHECK={:?} is refused without CURVE_ARMS: every mode breaks a READ-VS-N guard",
                self
            ));
        };
        let on = match need {
            NeedsArm::Any => true,
            NeedsArm::Read => a.read,
            NeedsArm::Merge => a.merge,
            NeedsArm::Restart => a.restart,
        };
        if !on {
            return Some(format!(
                "CURVE_FIRECHECK={:?} is refused: it needs the {need:?} arm, which \
                 CURVE_ARMS does not run, so it could only pass unseen (PREREG A12.3)",
                self
            ));
        }
        // Merges free extents, and a recycled extent can hold a stale checksummed page that the
        // open sweep's fill probe counts, so it would rightly not free the orphan and H4 would stay
        // silent.
        if self == Fire::OrphanExtent && a.merge {
            return Some(
                "CURVE_FIRECHECK=orphan-extent is refused with the merge arm on: its orphan could land \
                 in a recycled extent that still holds a readable page (PREREG A9.2). Run it with \
                 CURVE_ARMS=restart."
                    .into(),
            );
        }
        None
    }

    /// One mode by name. Refuses a name it does not know rather than running unguarded.
    fn parse(mode: &str) -> Fire {
        match mode {
            "" | "none" => Fire::None,
            "wrong-page" => Fire::WrongPage,
            "census-off" => Fire::CensusOff,
            "control-catalog" => Fire::ControlCatalog,
            "control-cold" => Fire::ControlCold,
            "wrong-height" => Fire::WrongHeight,
            "control-drift" => Fire::ControlDrift,
            "wrong-arenas" => Fire::WrongArenas,
            "wrong-live" => Fire::WrongLive,
            "merge-quarantined" => Fire::MergeQuarantined,
            "wrong-start" => Fire::WrongStart,
            "wrong-visits" => Fire::WrongVisits,
            "wrong-delta" => Fire::WrongDelta,
            "wrong-live-merge" => Fire::WrongLiveMerge,
            "wrong-ckpt-flag" => Fire::WrongCkptFlag,
            "extra-branch" => Fire::ExtraBranch,
            "child-locked" => Fire::ChildLocked,
            "extra-extent" => Fire::ExtraExtent,
            "orphan-extent" => Fire::OrphanExtent,
            "no-cluster-time" => Fire::NoClusterTime,
            "pinned-checkpoint" => Fire::PinnedCheckpoint,
            "stale-marker" => Fire::StaleMarker,
            "ckpt-ddl" => Fire::CkptDdl,
            other => panic!(
                "CURVE_FIRECHECK: unknown mode {other:?}; the modes are wrong-page, census-off, \
                 control-catalog, control-cold, wrong-height, control-drift, wrong-arenas, \
                 wrong-live, merge-quarantined, wrong-start, wrong-visits, wrong-delta, \
                 wrong-live-merge, wrong-ckpt-flag, extra-branch, child-locked, extra-extent, \
                 orphan-extent, no-cluster-time, pinned-checkpoint, stale-marker, ckpt-ddl"
            ),
        }
    }
}

/// The three read arms. `TrunkId` is the control: it cannot depend on N.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arm {
    Branch,
    TrunkId,
    TrunkCat,
}
const ARMS: [Arm; 3] = [Arm::Branch, Arm::TrunkId, Arm::TrunkCat];

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Branch => "branch",
            Arm::TrunkId => "trunk-id",
            Arm::TrunkCat => "trunk-cat",
        }
    }
}

/// One arm's totals at one (N, threads): the catalog phase (`cat`) and the data phase (`data`) of
/// every read, counted separately.
#[derive(Clone, Copy, Debug, Default)]
struct ArmTotals {
    reads: u64,
    nanos: u128,
    cat: ReadCensus,
    data: ReadCensus,
    /// Reads that did not return what that branch wrote (G2).
    wrong: u64,
}

impl ArmTotals {
    fn add(&mut self, o: &ArmTotals) {
        self.reads += o.reads;
        self.nanos += o.nanos;
        self.cat.add(&o.cat);
        self.data.add(&o.data);
        self.wrong += o.wrong;
    }
    fn ns_per_read(&self) -> f64 {
        self.nanos as f64 / self.reads.max(1) as f64
    }
    fn per(&self, count: u64) -> f64 {
        count as f64 / self.reads.max(1) as f64
    }
}

/// One printed READ block: an (N, threads) point, all three arms.
struct ReadRow {
    n: usize,
    /// Height of the branch catalog's tree, walked from its root — independent of the census.
    h: u64,
    threads: usize,
    arms: [ArmTotals; 3],
}

/// splitmix64: a named, seeded generator, so a run's branch choices are reproducible from the seed
/// rule printed in its header.
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn read_seed(n: usize, threads: usize, t: usize) -> u64 {
    0xD197_0000_0000_0000 ^ ((n as u64) << 16) ^ ((threads as u64) << 8) ^ t as u64
}

/// Levels from the branch catalog's root to a leaf, walked through `read_node`. This is the
/// independent instrument PREREG P2/G6 checks the census against, so it must not come from the
/// census. It fetches pages and moves this thread's counts: call it outside every bracket.
fn catalog_height(cat: &TableBranchCatalog) -> u64 {
    let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::open(
        cat.root_page_id(),
        Arc::clone(cat.pool_handle()),
    );
    let mut page = cat.root_page_id();
    let mut h = 1;
    loop {
        match tree.read_node(page).expect("walk the branch catalog") {
            BPlusTreePage::Leaf(_) => return h,
            BPlusTreePage::Internal(n) => {
                page = n.child_ptrs[0];
                h += 1;
            }
        }
    }
}

/// Everything the loop in `main` reads or writes through. In the production layout these are
/// clones out of an [`OpenDatabase`] and are REPLACED after every restart.
struct Handles {
    cat_concrete: Arc<TableBranchCatalog>,
    cat: Arc<dyn BranchCatalog>,
    store: Arc<ArenaPageStore>,
    /// `Some` only in the production layout.
    runtime: Option<Arc<AgentRuntime>>,
    statement_lock: Option<Arc<CatalogLock>>,
}

impl Handles {
    fn of(db: &OpenDatabase) -> Handles {
        Handles {
            cat_concrete: Arc::clone(&db.branches),
            cat: db.branches.clone() as Arc<dyn BranchCatalog>,
            store: Arc::clone(&db.store),
            runtime: Some(Arc::clone(&db.runtime)),
            statement_lock: Some(Arc::clone(&db.catalog)),
        }
    }
}

/// Wait for the lease thread's first pass to reach its end — the second full sweep, PREREG R3.
/// `None` if it did not within `bound`.
fn wait_first_pass(db: &OpenDatabase, bound: Duration) -> Option<Duration> {
    let t = Instant::now();
    while db.lease.stats().finished == 0 {
        if t.elapsed() > bound {
            return None;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Some(t.elapsed())
}

/// Open the production database, wait out its first lease pass, and hand it back. Every open the
/// PARENT does goes through here, so no timed window can overlap a sweep it started.
fn parent_open(db_path: &str, failures: &mut Vec<String>) -> OpenDatabase {
    let db = open_database(db_path, PARENT_SCAN_INTERVAL).expect("open the production database");
    let bound = Duration::from_secs(60) + db.timings.lease_start * 10;
    if wait_first_pass(&db, bound).is_none() {
        failures.push(format!(
            "the parent's first lease pass did not finish within {bound:?}; a sweep may overlap a \
             timed window"
        ));
    }
    db
}

/// Untimed: every branch once in order (up to 65,536), then 16,384 random reads. For N below the
/// pools this makes everything resident; above them it leaves the pools in the random steady state
/// the timed reads then sample.
fn warm_up(runtime: &AgentRuntime, branches: &[BranchId]) {
    let rows = runtime.storage().expect("the production runtime is page-backed");
    let tid = table_id(TABLE).0;
    let read = |b: BranchId| {
        let root = runtime.root_of(b).expect("root_of");
        let _ = rows.get(root, tid, ROW).expect("get");
    };
    for &b in branches.iter().take(65_536) {
        read(b);
    }
    let mut rng = read_seed(branches.len(), 0, 0xFF);
    for _ in 0..16_384 {
        read(branches[(splitmix(&mut rng) % branches.len() as u64) as usize]);
    }
}

/// PREREG arm 1 at one (N, threads): K reads per arm, in blocks of [`BLOCK`], the arm order rotated
/// per block and every thread on the same arm at once.
#[allow(clippy::too_many_arguments)]
fn read_phase(
    runtime: &AgentRuntime,
    store: &ArenaPageStore,
    branches: &[BranchId],
    trunk_root: PageId,
    cold_pages: &[PageId],
    threads: usize,
    k: usize,
    checkpoint_index: usize,
    fire: Fire,
) -> [ArmTotals; 3] {
    let blocks = (k / threads / BLOCK).max(1);
    let barrier = Barrier::new(threads);
    let rows = runtime.storage().expect("the production runtime is page-backed");
    let tid = table_id(TABLE).0;
    let n = branches.len() as u64;
    // 10 µs doubling per checkpoint: already 2x between the first two, far past G7's 1.5 band even
    // if the control read itself costs as much as the spin.
    let drift = Duration::from_micros(10u64 << checkpoint_index.min(16));
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let barrier = &barrier;
                s.spawn(move || {
                    let mut rng = read_seed(branches.len(), threads, t);
                    let mut tot = [ArmTotals::default(); 3];
                    for block in 0..blocks {
                        for j in 0..ARMS.len() {
                            let a = (block + j) % ARMS.len();
                            let arm = ARMS[a];
                            barrier.wait();
                            let t0 = Instant::now();
                            for _ in 0..BLOCK {
                                // Drawn on every arm, so every arm does the same arithmetic.
                                let pick = (splitmix(&mut rng) % n) as usize;
                                let c0 = this_thread();
                                let (ok, c1) = match arm {
                                    Arm::Branch | Arm::TrunkCat => {
                                        let (b, want) = match arm {
                                            Arm::Branch => {
                                                let writer = if fire == Fire::WrongPage {
                                                    branches[(pick + 1) % branches.len()]
                                                } else {
                                                    branches[pick]
                                                };
                                                (branches[pick], writer.id as i64)
                                            }
                                            _ => (BranchId::TRUNK, TRUNK_VALUE),
                                        };
                                        let root = runtime.root_of(b).expect("root_of");
                                        let c1 = this_thread();
                                        let got = rows.get(root, tid, ROW).expect("get");
                                        (got == Some(vec![Value::BigInt(want)]), c1)
                                    }
                                    Arm::TrunkId => {
                                        if fire == Fire::ControlCatalog {
                                            let _ = runtime.root_of(BranchId::TRUNK);
                                        }
                                        let c1 = this_thread();
                                        let page = if fire == Fire::ControlCold {
                                            cold_pages[pick]
                                        } else {
                                            trunk_root
                                        };
                                        if fire == Fire::ControlDrift {
                                            let until = Instant::now() + drift;
                                            while Instant::now() < until {}
                                        }
                                        (store.read_page(page).is_ok(), c1)
                                    }
                                };
                                let c2 = this_thread();
                                let r = &mut tot[a];
                                r.reads += 1;
                                if !ok {
                                    r.wrong += 1;
                                }
                                if !(fire == Fire::CensusOff && arm == Arm::Branch) {
                                    r.cat.add(&c1.since(&c0));
                                    r.data.add(&c2.since(&c1));
                                }
                            }
                            tot[a].nanos += t0.elapsed().as_nanos();
                        }
                    }
                    tot
                })
            })
            .collect();
        let mut sum = [ArmTotals::default(); 3];
        for h in handles {
            let tot = h.join().expect("reader thread");
            for (s, t) in sum.iter_mut().zip(tot.iter()) {
                s.add(t);
            }
        }
        sum
    })
}

fn print_read_header() {
    println!(
        "  READ          N   h   T  arm          reads     ns/read   c.desc   c.att    c.opt  c.omiss   \
         c.walk    c.lat  c.fetch  c.fault    c.hop  d.fetch  d.fault  wrong"
    );
}

fn print_read_row(row: &ReadRow) {
    for (i, arm) in ARMS.iter().enumerate() {
        let r = &row.arms[i];
        println!(
            "  READ   {:>8} {:>3} {:>3}  {:<10} {:>7} {:>11.1} {:>8.3} {:>7.3} {:>8.3} {:>8.3} {:>8.3} \
             {:>8.3} {:>8.3} {:>8.3} {:>8.3} {:>8.3} {:>8.3} {:>6}",
            row.n,
            row.h,
            row.threads,
            arm.name(),
            r.reads,
            r.ns_per_read(),
            r.per(r.cat.descents),
            r.per(r.cat.attempts),
            r.per(r.cat.optimistic),
            r.per(r.cat.optimistic_misses),
            r.per(r.cat.right_walks),
            r.per(r.cat.latched),
            r.per(r.cat.fetches),
            r.per(r.cat.faults),
            r.per(r.cat.scan_leaves),
            r.per(r.data.fetches),
            r.per(r.data.faults),
            r.wrong,
        );
    }
}

/// The integer guards on one READ block (PREREG G2, G3, G4, G6). G5 and G7 are about the ns
/// columns and are judged in the summary.
fn read_guards(row: &ReadRow, fire: Fire, failures: &mut Vec<String>) {
    let at = format!("N={} T={}", row.n, row.threads);
    let [branch, control, trunk_cat] = &row.arms;
    for (i, arm) in ARMS.iter().enumerate() {
        if row.arms[i].wrong > 0 {
            failures.push(format!(
                "G2 {at} {}: {} of {} reads did not return what was written",
                arm.name(),
                row.arms[i].wrong,
                row.arms[i].reads
            ));
        }
    }
    if branch.cat.descents == 0 || branch.data.fetches == 0 {
        failures.push(format!(
            "G3 {at}: the census did not fire on the branch arm (c.desc {}, d.fetch {})",
            branch.cat.descents, branch.data.fetches
        ));
    }
    if control.cat != ReadCensus::default() || control.data.fetches != control.reads {
        failures.push(format!(
            "G4 {at}: the control touched the catalog or did not fetch exactly once per read \
             (c = {:?}, d.fetch {} over {} reads)",
            control.cat, control.data.fetches, control.reads
        ));
    }
    let h = if fire == Fire::WrongHeight { row.h + 1 } else { row.h };
    for (arm, r) in [(Arm::Branch, branch), (Arm::TrunkCat, trunk_cat)] {
        if r.cat.attempts == r.cat.descents
            && r.cat.optimistic != r.cat.descents * h + r.cat.right_walks
        {
            failures.push(format!(
                "G6 {at} {}: no restarts, yet c.opt {} != c.desc {} x h {} + c.walk {}",
                arm.name(),
                r.cat.optimistic,
                r.cat.descents,
                h,
                r.cat.right_walks
            ));
        }
    }
}

/// What one restart measured. Microseconds, as the child printed them.
#[derive(Default)]
struct RestartRow {
    n: usize,
    expect_arenas: u64,
    expect_live: u64,
    child: std::collections::HashMap<String, u64>,
    /// The child's `RESTART_RESULT` line, verbatim, so nothing it measured can be dropped (A7.2).
    raw: String,
    /// What the child is about to replay, sized by the parent after its clean close (A7.1, A7.3).
    wal_bytes: u64,
    tel_bytes: u64,
    prov_bytes: u64,
    /// Rows arm 2 has published into table `m` so far: the heap `open_recovered` rebuilds over.
    m_rows: u64,
    parent_total_us: u64,
    parent_lease_us: u64,
}

impl RestartRow {
    fn get(&self, key: &str) -> u64 {
        self.child.get(key).copied().unwrap_or(u64::MAX)
    }
}

/// The 1-minute load average ×100 (PREREG A14.2), or `u64::MAX` when the box will not say.
/// `/proc/loadavg` where it exists, else `sysctl -n vm.loadavg` (`{ 1.23 1.45 1.67 }`), the reader
/// `examples/d97_attestation.rs` uses. The first number in either is the 1-minute average.
fn load_1min_centi() -> u64 {
    let text = std::fs::read_to_string("/proc/loadavg").ok().or_else(|| {
        std::process::Command::new("sysctl")
            .args(["-n", "vm.loadavg"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
    });
    text.as_deref()
        .and_then(|t| t.split_whitespace().find_map(|w| w.parse::<f64>().ok()))
        .map(|l| (l * 100.0).round() as u64)
        .unwrap_or(u64::MAX)
}

/// The stale-index marker `open_recovered` honours, beside `{db}.wal`: the engine's own path helper.
fn stale_marker(db_path: &str) -> std::path::PathBuf {
    ferrodb::wal::txn::stale_indexes_marker(Path::new(&format!("{db_path}.wal")))
}

/// The CHILD: a fresh process that opens the database exactly as the shipped binary does, times
/// it, waits for the lease thread's first pass, closes cleanly, and prints ONE tagged line.
///
/// A fresh process because a real restart is one (D65 adversary, finding e): the parent has a
/// large live heap and warm allocator. The OS page cache is warm either way — purging it needs
/// root — so this is a WARM-CACHE restart, and PREREG says so.
fn open_only_child(db_path: &str, fire: Fire) -> ! {
    // H5's in-path fire (PREREG A7.5): a cluster member with no applied `LeaseTick`, held from
    // before the open to after the close. Its lease passes refuse before the orphan sweep.
    let _cluster = (fire == Fire::NoClusterTime).then(|| ClusterScope::joined(NodeId(1)));
    // H6 (A12.2): read BEFORE the open, which removes the marker once it has rebuilt for it.
    let stale = stale_marker(db_path).exists();
    // A14.2's load flag: the box's load around this one open, read outside every timer.
    let load_start = load_1min_centi();
    let interval = scan_interval_from_env().expect("lease scan interval");
    let c0 = this_thread();
    let db = open_database(db_path, interval).expect("open the database");
    let c = this_thread().since(&c0);
    let t = db.timings;
    let open_visits = db.reaper.open_sweep_visits();
    let freed = db.reaper.open_sweep_freed();
    let bound = Duration::from_secs(60) + t.lease_start * 10;
    let first = wait_first_pass(&db, bound);
    let visits_total = db.reaper.sweep_visits();
    let descents_total = db.reaper.sweep_descents();
    // Untimed positive control (PREREG H3): the database that opened is the populated one.
    let live = db.branches.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
    // A9.3/A12.2: D216's judge. Did `open_recovered` find records to replay? It is also the gate:
    // the open rebuilds only when this is true or the marker was present (`stale`, H6).
    let recovered = db.recovered;
    // A20.4: the close's lease stats and its result go on the line, and the parent judges them
    // (`restart_guards`). A failed close no longer panics the child, which the parent would have
    // read as H1.
    let (lease_stats, closed) = db.close();
    if let Err(e) = &closed {
        eprintln!("the child's close failed: {e}");
    }
    let close_ok = closed.is_ok();
    let load_end = load_1min_centi();
    let us = |d: Duration| d.as_micros() as u64;
    println!(
        "RESTART_RESULT recovered={} stale={} total_us={} lock_us={} files_us={} floor_us={} recover_us={} sql_catalog_us={} \
         rebuild_us={} branch_catalog_us={} arena_us={} effect_log_us={} runtime_us={} \
         provenance_us={} lease_start_us={} \
         open_visits={} freed={} first_pass_done={} first_pass_us={} visits_total={} \
         descents_total={} live={} c_desc={} c_att={} c_opt={} c_omiss={} c_lat={} c_fetch={} \
         c_fault={} c_hop={} load_start_centi={} load_end_centi={} lease_panicked={} lease_failed={} \
         lease_refused_branches={} close_ok={}",
        recovered as u64,
        stale as u64,
        us(t.total),
        us(t.lock),
        us(t.boot.files),
        us(t.boot.floor),
        us(t.boot.recover),
        us(t.boot.catalog),
        us(t.boot.rebuild),
        us(t.branch_catalog),
        us(t.arena),
        us(t.effect_log),
        us(t.runtime),
        us(t.provenance),
        us(t.lease_start),
        open_visits,
        freed,
        first.is_some() as u64,
        first.map(us).unwrap_or(0),
        visits_total,
        descents_total,
        live,
        c.descents,
        c.attempts,
        c.optimistic,
        c.optimistic_misses,
        c.latched,
        c.fetches,
        c.faults,
        c.scan_leaves,
        load_start,
        load_end,
        lease_stats.panicked as u64,
        lease_stats.failed,
        lease_stats.refused_branches,
        close_ok as u64,
    );
    std::process::exit(0);
}

/// Run the child on `db_path` with the fire mode it should apply (`"none"` for none), and parse its
/// line. Returns the fields and the line itself, verbatim. `Err` names what went wrong (PREREG H1).
fn run_child(
    db_path: &str,
    child_fire: &str,
) -> Result<(std::collections::HashMap<String, u64>, String), String> {
    let exe = std::env::current_exe().map_err(|e| format!("own path: {e}"))?;
    let out = std::process::Command::new(exe)
        .arg("--open-only")
        .arg(db_path)
        .arg(child_fire)
        .stderr(std::process::Stdio::inherit())
        .output()
        .map_err(|e| format!("spawn: {e}"))?;
    if !out.status.success() {
        return Err(format!("child exited {:?}", out.status.code()));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = text.lines().filter(|l| l.starts_with("RESTART_RESULT ")).collect();
    if lines.len() != 1 {
        return Err(format!("child printed {} RESTART_RESULT lines, not 1", lines.len()));
    }
    let mut map = std::collections::HashMap::new();
    for kv in lines[0].split_whitespace().skip(1) {
        let (k, v) = kv.split_once('=').ok_or_else(|| format!("unparseable field {kv:?}"))?;
        let v: u64 = v.parse().map_err(|e| format!("field {k}={v:?}: {e}"))?;
        map.insert(k.to_string(), v);
    }
    Ok((map, lines[0].to_string()))
}

fn print_restart_header() {
    println!(
        "  RESTART       N   arenas   total ms  lease_st ms   arena ms  br_cat ms  recover ms  \
         tel ms  prov ms  open visits  1st-pass visits  1st-pass ms  c.desc/v  c.att/v  c.fault/v  \
         freed  live  parent ms  lock ms  files ms  floor ms  sqlcat ms  rebuild ms  runtime ms  descents  \
         wal B  tel B  prov B  m rows  recovered  stale"
    );
    println!("  RESTART-RAW N=<n> <the child's RESTART_RESULT line, verbatim: every field it measured>");
}

fn print_restart_row(r: &RestartRow) {
    let ms = |k: &str| r.get(k) as f64 / 1000.0;
    let visits = r.get("open_visits");
    let per_visit = |k: &str| r.get(k) as f64 / visits.max(1) as f64;
    let first_pass_visits = r.get("visits_total").saturating_sub(visits);
    println!(
        "  RESTART {:>8} {:>8} {:>10.3} {:>12.3} {:>10.3} {:>10.3} {:>11.3} {:>7.3} {:>8.3} {:>12} \
         {:>16} {:>12.3} {:>9.3} {:>8.3} {:>10.4} {:>6} {:>5} {:>10.3} {:>8.3} {:>9.3} {:>9.3} {:>9.3} \
         {:>11.3} {:>11.3} {:>9} {:>6} {:>6} {:>7} {:>7} {:>10} {:>6}",
        r.n,
        r.expect_arenas,
        ms("total_us"),
        ms("lease_start_us"),
        ms("arena_us"),
        ms("branch_catalog_us"),
        ms("recover_us"),
        ms("effect_log_us"),
        ms("provenance_us"),
        visits,
        first_pass_visits,
        ms("first_pass_us"),
        per_visit("c_desc"),
        per_visit("c_att"),
        per_visit("c_fault"),
        r.get("freed"),
        r.get("live"),
        r.parent_total_us as f64 / 1000.0,
        ms("lock_us"),
        ms("files_us"),
        ms("floor_us"),
        ms("sql_catalog_us"),
        ms("rebuild_us"),
        ms("runtime_us"),
        r.get("descents_total"),
        r.wal_bytes,
        r.tel_bytes,
        r.prov_bytes,
        r.m_rows,
        r.get("recovered"),
        r.get("stale"),
    );
    // A7.2: every field the child measured, verbatim, whether or not a column above shows it.
    println!("  RESTART-RAW N={} {}", r.n, r.raw);
}

/// PREREG H2-H5 on one restart.
fn restart_guards(r: &RestartRow, fire: Fire, failures: &mut Vec<String>) {
    let at = format!("N={}", r.n);
    let want_arenas = r.expect_arenas + (fire == Fire::WrongArenas) as u64;
    let want_live = r.expect_live + (fire == Fire::WrongLive) as u64;
    if r.get("open_visits") != want_arenas {
        failures.push(format!(
            "H2 {at}: the open sweep visited {} arenas; the parent counted {want_arenas} live",
            r.get("open_visits")
        ));
    }
    if r.get("live") != want_live {
        failures.push(format!(
            "H3 {at}: the reopened catalog has {} live branches; the parent counted {want_live}",
            r.get("live")
        ));
    }
    if r.get("freed") != 0 {
        failures.push(format!(
            "H4 {at}: the open sweep freed {} extents from a cleanly closed database",
            r.get("freed")
        ));
    }
    if r.get("first_pass_done") != 1 {
        failures.push(format!("H5 {at}: the lease thread's first pass did not finish in its bound"));
    }
    // H6 (A12.2): the one thing besides `recovered` that makes the child's open rebuild. Nothing in
    // this harness rolls back, so a marker means an index undo failed in the parent, and the row's
    // `rebuild` is D205's, not a clean restart's.
    // A20.4: the child's own close, judged here. A missing field reads u64::MAX, so it fires.
    let (lp, lf, lr) = (r.get("lease_panicked"), r.get("lease_failed"), r.get("lease_refused_branches"));
    if lp != 0 || lf != 0 || lr != 0 {
        failures.push(format!(
            "LEASE {at} (the child's close): the lease thread ended with panicked={lp}, failed={lf}, \
             refused_branches={lr}"
        ));
    }
    if r.get("close_ok") != 1 {
        failures.push(format!("CLOSE {at} (the child's close): the child's database did not close cleanly"));
    }
    if r.get("stale") != 0 {
        failures.push(format!(
            "H6 {at}: the stale-index marker was present at the child's open, so its rebuild is not a \
             clean restart's"
        ));
    }
}

// ---- ARM 2: MERGE against N and against M (PREREG amendment A3) --------------------------------

/// The SQL table every merge publishes one NEW row into, created once on trunk.
const MERGE_TABLE: &str = "m";
/// `ckpt-ddl`'s table (A13.1). PREREG derives its one `Ddl` record's encoded size by hand from this
/// name and its single column, so neither may change without amending A13.1.
const CKPT_DDL_SQL: &str = "CREATE TABLE ckpt_ddl (c INTEGER);";
/// Merges each axis-(ii) report averages: the last this many before its M target.
const MERGE_BLOCK: usize = 64;

/// One SQL statement, the way `run_cli`'s `execute_sql` runs it: parsed, then run under the
/// statement lock with the database's own pool and transaction manager.
fn exec_sql(db: &OpenDatabase, sess: &mut Session, sql: &str) -> Result<Outcome, String> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new())
        .scan_tokens()
        .map_err(|e| format!("{sql}: {e:?}"))?;
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    if !parser.errors.is_empty() || stmts.is_empty() {
        return Err(format!("{sql}: {:?}", parser.errors));
    }
    let mut cat = db.catalog.lock();
    run(stmts.remove(0), &mut cat, db.bp.clone(), db.txn.clone(), sess).map_err(|e| format!("{sql}: {e}"))
}

/// What one `MERGE;` cost and did. Everything is bracketed around that statement ALONE: the
/// session's `BEGIN` and `INSERT` are outside, and so are the `state_sizes` reads, which take the
/// lock every statement takes.
#[derive(Clone, Copy, Debug, Default)]
struct MergeOne {
    nanos: u128,
    applied_to_target: bool,
    before: StateSizes,
    after: StateSizes,
    attested_after: u64,
    /// `highest_applied_seq`'s entries (Q1).
    v_hi: u64,
    /// `concurrent_op`'s reads through D86's index (Q3).
    v_cell: u64,
    seq_tuples: u64,
    index_scans: u64,
    fsyncs: u64,
    census: ReadCensus,
    /// `TxnManager::retained_runs` before and after: the run declarations every checkpoint
    /// re-appends (A7.4).
    runs_before: u64,
    runs_after: u64,
    /// The publish commit's automatic checkpoint ran inside this `MERGE;` (A7.4): the commit counter
    /// did not rise, which only a checkpoint's reset explains. Exact on APPLIED merges only; a merge
    /// that committed nothing also leaves the counter where it was (A9.4).
    checkpointed: bool,
    /// The state cross-check for `checkpointed` (A9.4, guard M6): the log's `base_lsn` moved, which
    /// only a checkpoint's truncation does.
    base_lsn_moved: bool,
    /// When the log's base moved (a real truncation): the WAL bytes appended after it, i.e. the
    /// checkpoint's re-appended declarations (A12.1). `None` otherwise.
    replay_bytes: Option<u64>,
    /// On an applied merge that checkpointed: the merge cycles since the previous checkpoint (or
    /// the open), this one included. The period the checkpoint amortizes over, counted in the unit
    /// being amortized, so a deferred trigger or a cycle that commits twice shows here (A10.1).
    period: Option<u64>,
}

/// One agent task: `BEGIN AGENT SESSION`, one INSERT of a row nobody has written, `MERGE;`.
/// `since_ckpt` counts cycles since the last checkpoint; the caller zeroes it at every open.
///
/// The agent id is fixed-width (`mc` + 10 digits) so every run declaration encodes to the same
/// size, which is what makes A12.1's replay bytes an exact line in the retained runs.
fn merge_cycle(db: &OpenDatabase, id: i64, fire: Fire, since_ckpt: &mut u64) -> MergeOne {
    let mut sess = Session::with_runtime(Arc::clone(&db.runtime));
    exec_sql(db, &mut sess, &format!("BEGIN AGENT SESSION AS 'mc{id:010}';")).expect("begin");
    exec_sql(db, &mut sess, &format!("INSERT INTO {MERGE_TABLE} VALUES ({id}, {id});"))
        .expect("insert");
    if fire == Fire::MergeQuarantined {
        let b = sess.agent.as_ref().expect("an open agent session").branch;
        db.runtime.quarantine(b, "READ-VS-N merge-quarantined fire check").expect("quarantine");
    }
    let before = db.runtime.state_sizes();
    let runs_before = db.txn.retained_runs() as u64;
    let commits0 = db.txn.commits_since_checkpoint.load(std::sync::atomic::Ordering::SeqCst);
    let base0 = db.txn.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
    let (v0, c0) = merge_log_counters();
    let seq0 = seq_scan_counters().1;
    let ix0 = index_scan_counters().0;
    let f0 = FSYNC_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let census0 = this_thread();
    let t = Instant::now();
    let out = exec_sql(db, &mut sess, "MERGE;");
    let nanos = t.elapsed().as_nanos();
    let census = this_thread().since(&census0);
    let fsyncs = FSYNC_CALLS.load(std::sync::atomic::Ordering::Relaxed) - f0;
    let index_scans = index_scan_counters().0 - ix0;
    let seq_tuples = seq_scan_counters().1 - seq0;
    let (v1, c1) = merge_log_counters();
    let commits1 = db.txn.commits_since_checkpoint.load(std::sync::atomic::Ordering::SeqCst);
    let base1 = db.txn.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
    let next1 = db.txn.wal.next_lsn.load(std::sync::atomic::Ordering::SeqCst);
    let runs_after = db.txn.retained_runs() as u64;
    let after = db.runtime.state_sizes();
    let applied_to_target =
        matches!(&out, Ok(Outcome::Agent(AgentOutput::Merge(r))) if r.applied_to_target);
    let checkpointed = commits1 <= commits0;
    *since_ckpt += 1;
    let period = (applied_to_target && checkpointed).then(|| std::mem::take(since_ckpt));
    MergeOne {
        nanos,
        applied_to_target,
        before,
        after,
        attested_after: db.runtime.attested_len() as u64,
        v_hi: v1 - v0,
        v_cell: c1 - c0,
        seq_tuples,
        index_scans,
        fsyncs,
        census,
        runs_before,
        runs_after,
        checkpointed,
        base_lsn_moved: base1 != base0,
        replay_bytes: (base1 != base0).then(|| next1 - base1),
        period,
    }
}

/// The per-merge guards (PREREG A3: M1, M3, M4; A9.4: M6). `delta` holds the first merge's append
/// count for the whole run, so M4 compares every merge against the same number.
fn merge_guards(at: &str, one: &MergeOne, fire: Fire, delta: &mut Option<u64>, failures: &mut Vec<String>) {
    if !one.applied_to_target {
        failures.push(format!("M1 {at}: a MERGE did not reach the target, so it measured nothing"));
        return;
    }
    let want_visits = one.before.applied as u64 + (fire == Fire::WrongVisits) as u64;
    if one.v_hi != want_visits {
        failures.push(format!(
            "M3 {at}: highest_applied_seq visited {} entries of a {}-entry log",
            one.v_hi, want_visits
        ));
    }
    let appended = (one.after.applied - one.before.applied) as u64;
    let first = *delta.get_or_insert(appended);
    let want = first + (fire == Fire::WrongDelta) as u64;
    if appended != want {
        failures.push(format!("M4 {at}: this merge appended {appended} ops; the run's first appended {want}"));
    }
    // M6: the counter-derived flag against state. On an applied merge, a checkpoint and only a
    // checkpoint both resets the commit counter and truncates the log.
    let flag = one.checkpointed != (fire == Fire::WrongCkptFlag);
    if flag != one.base_lsn_moved {
        failures.push(format!(
            "M6 {at}: the commit counter says checkpointed={flag}, but the log's base_lsn {} moved",
            if one.base_lsn_moved { "DID" } else { "did NOT" }
        ));
    }
}

/// One axis-(ii) auto-checkpoint (A11.3, A12.1): the merge that paid it. EVERY checkpoint is kept,
/// not only those in a printed block.
#[derive(Clone, Copy, Debug)]
struct CkptPoint {
    /// Merges since the open when it ran.
    m: usize,
    /// `retained_runs` after it: the run declarations its checkpoint re-appended, which is the
    /// replay's input and so both fits' x (A12.4). It equals `m` only while nothing refills
    /// `run_log` at open (ledger D227).
    runs: u64,
    /// A12.1's JUDGED instrument, an integer: the WAL bytes appended after the truncation
    /// (`next_lsn - base_lsn` once the merge returns), which is everything the checkpoint
    /// re-appended and nothing else. `None` when the log was not truncated (a WAL pin, M6's case).
    replay_bytes: Option<u64>,
    ns: u128,
    /// The median ns of the applied merges since the previous checkpoint that did not checkpoint.
    /// `None` when there were none (reachable only at a checkpoint interval of 1): that point still
    /// counts for the JUDGED bytes line and is left out of the time fit alone (A13.7).
    median: Option<f64>,
    period: u64,
}

impl CkptPoint {
    fn excess(&self) -> Option<f64> {
        self.median.map(|m| self.ns as f64 - m)
    }
}

/// A least-squares line `y = c + a·x`, with what a reader needs to judge it.
#[derive(Clone, Copy, Debug)]
struct Line {
    a: f64,
    c: f64,
    /// Standard error of `a`; `None` below three points (n − 2 degrees of freedom).
    se_a: Option<f64>,
    resid_sd: Option<f64>,
    max_abs_resid: f64,
}

/// `None` below two points, or when every x is the same.
fn fit_line(pts: &[(f64, f64)]) -> Option<Line> {
    if pts.len() < 2 {
        return None;
    }
    let n = pts.len() as f64;
    let mx = pts.iter().map(|p| p.0).sum::<f64>() / n;
    let my = pts.iter().map(|p| p.1).sum::<f64>() / n;
    let sxx: f64 = pts.iter().map(|p| (p.0 - mx).powi(2)).sum();
    if sxx == 0.0 {
        return None;
    }
    let sxy: f64 = pts.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum();
    let a = sxy / sxx;
    let c = my - a * mx;
    let resid: Vec<f64> = pts.iter().map(|p| p.1 - (c + a * p.0)).collect();
    let max_abs_resid = resid.iter().fold(0.0f64, |m, r| m.max(r.abs()));
    let (se_a, resid_sd) = if pts.len() > 2 {
        let s2 = resid.iter().map(|r| r * r).sum::<f64>() / (n - 2.0);
        (Some((s2 / sxx).sqrt()), Some(s2.sqrt()))
    } else {
        (None, None)
    };
    Some(Line { a, c, se_a, resid_sd, max_abs_resid })
}

/// Every axis-(ii) checkpoint, raw, then two fits over them.
///
/// * **Judged (A12.1): the replay's BYTES against `runs`.** `replay_runs` appends one fixed-size
///   record per retained run, so control flow fixes the count and no load can move it. The line is
///   printed as fitted, not graded: PREREG A12.1 pre-registers its slope, intercept and residual.
/// * **Reported, never judged: the replay's TIME.** A11.3's verdict on it had no noise floor
///   (review 4 R1), so it prints each slope with its standard error and nothing more.
fn print_ckpt_fit(points: &[CkptPoint]) {
    println!("arm 2, axis (ii): every auto-checkpoint (A11.3, A12.1); excess = ns - the median of its own period");
    let whole = |v: Option<f64>| v.map(|x| format!("{x:.0}")).unwrap_or_else(|| "-".into());
    for p in points {
        println!(
            "  CKPT M={} runs={} replay_bytes={} ns={} period_median_ns={} excess_ns={} period={}",
            p.m,
            p.runs,
            p.replay_bytes.map(|b| b.to_string()).unwrap_or_else(|| "-".into()),
            p.ns,
            whole(p.median),
            whole(p.excess()),
            p.period
        );
    }
    let bytes: Vec<(f64, f64)> =
        points.iter().filter_map(|p| p.replay_bytes.map(|b| (p.runs as f64, b as f64))).collect();
    match fit_line(&bytes) {
        Some(l) => println!(
            "  replay bytes vs runs (JUDGED, A12.1), {} truncating checkpoints: slope = {:.3} bytes per run, \
             intercept = {:.1} bytes, max |residual| = {:.1} bytes",
            bytes.len(),
            l.a,
            l.c,
            l.max_abs_resid
        ),
        None => println!(
            "  replay bytes vs runs (JUDGED, A12.1): not computable ({} truncating checkpoints)",
            bytes.len()
        ),
    }
    let or_dash = |v: Option<f64>, prec: usize| v.map(|x| format!("{x:.prec$}")).unwrap_or_else(|| "-".into());
    let show = |l: Option<Line>| {
        l.map(|l| {
            format!(
                "a = {:.3} ± {} ns per retained run, c = {:.0} ns, residual sd = {} ns",
                l.a,
                or_dash(l.se_a, 3),
                l.c,
                or_dash(l.resid_sd, 0)
            )
        })
        .unwrap_or_else(|| "not computable".into())
    };
    // Only points with a period median (A13.7); the bytes line above takes every point.
    let excess = |pts: &[CkptPoint]| -> Vec<(f64, f64)> {
        pts.iter().filter_map(|p| p.excess().map(|e| (p.runs as f64, e))).collect()
    };
    let range = |pts: &[CkptPoint]| match (pts.first(), pts.last()) {
        (Some(f), Some(l)) => format!(
            "runs {}..={}, {} points, {} with a period median",
            f.runs,
            l.runs,
            pts.len(),
            pts.iter().filter(|p| p.median.is_some()).count()
        ),
        _ => "no points".into(),
    };
    // The lower half takes floor(n/2) points; for an odd n the middle point is the upper half's.
    let (lo, hi) = points.split_at(points.len() / 2);
    println!("  excess vs runs (REPORTED, never judged), all ({}): {}", range(points), show(fit_line(&excess(points))));
    println!("  excess vs runs, lower half ({}): {}", range(lo), show(fit_line(&excess(lo))));
    println!("  excess vs runs, upper half ({}): {}", range(hi), show(fit_line(&excess(hi))));
}

/// One printed MERGE row: the per-merge MEANS of a batch (axis i) or of a block (axis ii).
struct MergeRow {
    axis: &'static str,
    n: usize,
    /// Merges published since the last open, at the end of the block.
    m: usize,
    ones: Vec<MergeOne>,
}

impl MergeRow {
    fn mean(&self, f: impl Fn(&MergeOne) -> u64) -> f64 {
        self.ones.iter().map(|o| f(o) as f64).sum::<f64>() / self.ones.len().max(1) as f64
    }
    /// The block's APPLIED merges that paid the auto-checkpoint (A9.4: `ckpts` counts applied ones
    /// only), and their mean ns and fsyncs — the O(M) replay the median leaves out (A9.1).
    fn ckpt(&self) -> (usize, Option<f64>, Option<f64>) {
        let paid: Vec<&MergeOne> =
            self.ones.iter().filter(|o| o.applied_to_target && o.checkpointed).collect();
        if paid.is_empty() {
            return (0, None, None);
        }
        let n = paid.len() as f64;
        let ns = paid.iter().map(|o| o.nanos as f64).sum::<f64>() / n;
        let fs = paid.iter().map(|o| o.fsyncs as f64).sum::<f64>() / n;
        (paid.len(), Some(ns), Some(fs))
    }
    fn ns_median(&self) -> f64 {
        let mut v: Vec<u128> = self.ones.iter().map(|o| o.nanos).collect();
        v.sort_unstable();
        v.get(v.len() / 2).copied().unwrap_or(0) as f64
    }
    /// The typical merge's ns (A13.3): mean and standard error (sd / √n) over the block's applied
    /// merges that did not checkpoint. `None` below two such merges.
    fn typ_mean_se(&self) -> Option<(f64, f64)> {
        let v: Vec<f64> =
            self.ones.iter().filter(|o| o.applied_to_target && !o.checkpointed).map(|o| o.nanos as f64).collect();
        if v.len() < 2 {
            return None;
        }
        let n = v.len() as f64;
        let mean = v.iter().sum::<f64>() / n;
        let var = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
        Some((mean, (var / n).sqrt()))
    }
}

fn print_merge_header() {
    println!(
        "  MERGE axis        N        M  merges   ns/merge  ns median     V_hi   V_cell  d.applied  \
         applied  captures   merges  versions  wkspaces  attested  seq tup  ix scans   fsyncs   \
         c.desc  c.fault    c.att  ret runs  d.runs  ckpts    ckpt ns  ckpt fsync"
    );
}

fn print_merge_row(r: &MergeRow) {
    let last = r.ones.last().copied().unwrap_or_default();
    let (ckpts, ckpt_ns, ckpt_fs) = r.ckpt();
    let or_dash = |v: Option<f64>, prec: usize| v.map(|x| format!("{x:.prec$}")).unwrap_or_else(|| "-".into());
    println!(
        "  MERGE {:>4} {:>8} {:>8} {:>7} {:>10.0} {:>10.0} {:>8.1} {:>8.2} {:>10.2} {:>8} {:>9} \
         {:>8} {:>9} {:>9} {:>9} {:>8.2} {:>9.2} {:>8.2} {:>8.2} {:>8.3} {:>8.2} {:>9} {:>7.2} {:>6} \
         {:>10} {:>11}",
        r.axis,
        r.n,
        r.m,
        r.ones.len(),
        r.mean(|o| o.nanos as u64),
        r.ns_median(),
        r.mean(|o| o.v_hi),
        r.mean(|o| o.v_cell),
        r.mean(|o| (o.after.applied - o.before.applied) as u64),
        last.after.applied,
        last.after.captures,
        last.after.merges,
        last.after.versions,
        last.after.workspaces,
        last.attested_after,
        r.mean(|o| o.seq_tuples),
        r.mean(|o| o.index_scans),
        r.mean(|o| o.fsyncs),
        r.mean(|o| o.census.descents),
        r.mean(|o| o.census.faults),
        r.mean(|o| o.census.attempts),
        last.runs_after,
        r.mean(|o| o.runs_after - o.runs_before),
        ckpts,
        or_dash(ckpt_ns, 0),
        or_dash(ckpt_fs, 2),
    );
}

/// A20.4: every close's lease stats are read. A scan thread that died (D265), failed a reap, or
/// declined to decide about a branch leaves a failure, whatever the close's own result, because a
/// run whose lease thread stopped reaping or refused a branch did not run the workload it reports.
fn lease_end(stats: &LeaseStats, at: &str, failures: &mut Vec<String>) {
    if stats.panicked || stats.failed > 0 || stats.refused_branches > 0 {
        failures.push(format!(
            "LEASE {at}: the lease thread ended with panicked={}, failed={}, refused_branches={}",
            stats.panicked, stats.failed, stats.refused_branches
        ));
    }
}

/// Close cleanly and reopen, so `State` — which lives in memory (`reopen_with_storage` builds
/// `State::default()`) — starts empty: M = 0. Returns the new handles; the caller swaps them in.
fn reopen_for_merges(db: OpenDatabase, db_path: &str, failures: &mut Vec<String>) -> OpenDatabase {
    let (lease_stats, closed) = db.close();
    lease_end(&lease_stats, "(the close before a merge batch)", failures);
    // A21.1: returned to `main` through `failures`, not a panic, so the summary prints it.
    if let Err(e) = closed {
        failures.push(format!("CLOSE (the close before a merge batch): {e}"));
    }
    parent_open(db_path, failures)
}

/// `ln(b/a) / ln(nb/na)`: the local log-log slope between two checkpoints.
fn slope(a: f64, b: f64, na: usize, nb: usize) -> f64 {
    (b / a).ln() / (nb as f64 / na as f64).ln()
}

fn main() {
    // READ-VS-N arm 3: this binary re-executes itself as the restart being measured. Decided
    // before anything else is parsed, so a child cannot mistake its database path for checkpoints.
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("--open-only") {
        open_only_child(
            argv.get(2).expect("usage: --open-only <database path> <fire mode>"),
            Fire::parse(argv.get(3).map(String::as_str).unwrap_or("none")),
        );
    }
    let arms = Arms::from_env();
    let fire = Fire::from_env();
    // Before anything is created: a mode that cannot inject with these arms is refused.
    if let Some(why) = fire.refusal(arms) {
        panic!("{why}");
    }
    let read_k: usize = std::env::var("CURVE_READ_K").ok().and_then(|v| v.parse().ok()).unwrap_or(16_384);
    // Arm 2 (PREREG A3): K merges per checkpoint on axis (i), and the M targets of axis (ii).
    let merge_k: usize = std::env::var("CURVE_MERGE_K").ok().and_then(|v| v.parse().ok()).unwrap_or(64);
    let merge_targets: Vec<usize> = std::env::var("CURVE_MERGE_M")
        .unwrap_or_else(|_| "256,1024,4096,16384".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let checkpoints: Vec<usize> = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "1000,2000,4000,8000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let threads: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    let budget_gb: f64 = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(8.0);
    let budget = (budget_gb * 1e9) as u64;

    let dir = std::env::temp_dir().join(format!("ferrodb-wcurve-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    // Declared first so it drops LAST, after every handle below. A panic anywhere in this run
    // (every `expect` here is one) used to skip the old trailing `remove_dir_all` and leave a
    // multi-GB database in the temp dir of a disk this harness is guarding with FREE_FLOOR.
    let _cleanup = RemoveOnDrop(dir.clone());
    // A13.2: the (f0) refusal checks assert this line is ABSENT. The directory itself cannot say,
    // because `RemoveOnDrop` deletes it on unwind too. stderr, so no stdout table moves. After
    // `_cleanup` is armed (A14.5), so a failed write cannot leak the directory.
    eprintln!("RUN DIR {}", dir.display());
    // READ-VS-N: guard failures, and failures that void only the ns columns. Printed at the end,
    // and either one makes the run exit 2.
    let mut failures: Vec<String> = Vec::new();
    let mut ns_void: Vec<String> = Vec::new();
    // READ-VS-N's PRODUCTION LAYOUT (`CURVE_ARMS` set): the database is `curve.db`, opened by the
    // shipped binary's own `open_database`. Otherwise the historical D61/D65 files, built by hand
    // exactly as before.
    let db_path_str = dir.join("curve.db").to_str().expect("a UTF-8 temp path").to_string();
    let mut db: Option<OpenDatabase> = None;
    let (main_path, cat_path) = if arms.is_some() {
        (dir.join("curve.db"), dir.join("curve.db.branchcat"))
    } else {
        (dir.join("main.db"), dir.join("branches.branchcat"))
    };
    let mut hd = if arms.is_some() {
        let opened = parent_open(&db_path_str, &mut failures);
        let hd = Handles::of(&opened);
        db = Some(opened);
        hd
    } else {
        let mf = std::fs::OpenOptions::new()
            .create(true).read(true).write(true).open(&main_path).unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(mf).unwrap())));
        let _ = std::fs::remove_file(&cat_path);
        // Two handles to ONE catalog, deliberately: `root_page_id` is on the concrete type and not on
        // the `BranchCatalog` trait, and D65's reopen needs the CURRENT root rather than the 1 this
        // was opened with — reopening at a stale root would time the wrong thing.
        // ⛔ CORRECTED 2026-09-24 (D65 run 4 adversary, `artie-research frontier/d65_run4_adversary.md` §3; lead-verified in
        // `TableBranchCatalog::open_sidecar` at 9aa6968): `trunk_root` is used ONLY when the file is fresh (`create_with_header`).
        // A populated sidecar reopens from its header page, so no reopen can time a stale root. The concern above cannot arise,
        // and the error, if any, was in the measurement's favour. Comment only; the timed code is unchanged.
        let cat_concrete = Arc::new(TableBranchCatalog::open_sidecar(&cat_path, 1).expect("open catalog"));
        let cat: Arc<dyn BranchCatalog> = cat_concrete.clone();
        let base = pool.disk_manager.high_water().unwrap();
        let store = Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&cat), base).unwrap());
        Handles { cat_concrete, cat, store, runtime: None, statement_lock: None }
    };
    // The arena floor: fixed at creation and carried in the checkpoint, so it survives a restart.
    let base = hd.store.base_page();
    // **D79: this harness did NOT persist the free-space map, and the shipped binary does.**
    //
    // `ArenaPageStore` writes the map only if `checkpoint_to` has been called (see
    // `ArenaPageStore::checkpoint_to`; cited as `arena.rs:914` when D79 was written, and the line
    // has since moved — grep the name, not the number). `cli.rs` calls it; this file never did —
    // so D61's published 10^6 curve, and every other 10^6 result in this repo, measured a
    // configuration production does not run. THAT HALF STILL STANDS.
    //
    // ⛔ **THE NEXT SENTENCE OF D79 IS SUPERSEDED — BANDED, NOT DELETED, SO THE REVERSAL IS
    // VISIBLE WHERE THE CLAIM WAS MADE.** D79 said the map "is re-serialised and re-fsynced IN
    // FULL on every new branch's first page write, which is `sum(48i) = 24N^2` bytes over a run:
    // ~24 TB at 10^6." **That was true when written and is FALSE AT HEAD.** D81 (`53a6b66`) made
    // a claim append a 45-byte tail record instead, so the write volume is O(N); the three
    // remaining full-rewrite sites fire once per REAP, not once per branch created.
    //
    // D168 re-derived the consequence in THIS harness: the OFF/ON penalty is **flat in N**
    // (1.36x -> 1.36x across N=500..4000) where D80 measured it GROWING 1.88x -> 3.29x before
    // D81 landed. See `bench/d168_persistence_penalty_at_head.txt`. A flat ~1.4x remains and is
    // the per-claim fsync, which D81 never claimed to remove.
    //
    // `CURVE_PERSIST=1` turns it on so the two curves can be compared. It is OFF by default so
    // that re-running this file reproduces the historical numbers rather than silently replacing
    // them with different ones under the same name.
    let persist = std::env::var("CURVE_PERSIST").map(|v| v == "1").unwrap_or(false);
    if persist && arms.is_none() {
        hd.store.checkpoint_to(dir.join("main.db.arena"));
    }
    println!(
        "free-space map persistence: {}",
        if arms.is_some() {
            "ON (the production open arms it; CURVE_PERSIST is not consulted)"
        } else if persist {
            "ON (as cli.rs:120 ships)"
        } else {
            "OFF (historical default)"
        }
    );
    let lease = LeaseDeadline(u64::MAX);

    // READ-VS-N: every branch this run created, in creation order, and trunk's root page.
    let mut branches: Vec<BranchId> = Vec::new();
    let mut trunk_root: PageId = 0;
    let mut read_rows: Vec<ReadRow> = Vec::new();
    let mut restart_rows: Vec<RestartRow> = Vec::new();
    // H5's fire injects only at the LAST checkpoint; a run that stops before it never fired (A9.6).
    let mut no_cluster_time_injected = false;
    let mut checkpoint_index = 0usize;
    // Arm 2: every printed MERGE row, the next never-used row id, and the first merge's append
    // count, which every later merge is held to (M4).
    let mut merge_rows: Vec<MergeRow> = Vec::new();
    let mut ckpt_points: Vec<CkptPoint> = Vec::new();
    let mut next_merge_row: i64 = 1;
    // Rows published into table `m`: the heap every recovering open rebuilds over (A7.1).
    let mut m_rows: u64 = 0;
    let mut merge_delta: Option<u64> = None;
    if let (Some(arms), Some(rt), Some(statement_lock)) =
        (arms, hd.runtime.clone(), hd.statement_lock.clone())
    {
        // Trunk's row, written BEFORE any fork so every branch inherits the page holding it.
        {
            let _statement = statement_lock.lock();
            rt.put_row(BranchId::TRUNK, TABLE, ROW, &[Value::BigInt(TRUNK_VALUE)])
                .expect("seed trunk's row");
        }
        trunk_root = rt.root_of(BranchId::TRUNK).expect("trunk's root");
        if arms.merge {
            // Arm 2's target table, on trunk, through an ordinary session. Every merge publishes
            // one new row into it (PREREG A3: an INSERT, not an UPDATE, so D178's scan is not on
            // the path).
            let mut plain = Session::with_runtime(Arc::clone(&rt));
            exec_sql(
                db.as_ref().expect("the production database is open"),
                &mut plain,
                &format!("CREATE TABLE {MERGE_TABLE} (id INTEGER NOT NULL, v INTEGER);"),
            )
            .expect("create the merge table");
        }
        println!("READ-VS-N (bench/read_vs_n/PREREG.md). Built {}", ferrodb::build_provenance());
        println!(
            "  arms: read={} merge={} restart={}   firecheck: {fire:?}   K = {read_k} reads per arm \
             per thread count   read threads {READ_THREADS:?}   block {BLOCK}",
            arms.read, arms.merge, arms.restart
        );
        println!(
            "  merge arm: axis (i) {merge_k} merges per checkpoint, each after an open (M starts at 0); \
             axis (ii) after the last checkpoint, M targets {merge_targets:?}, each reporting the last \
             {MERGE_BLOCK} merges before it. One new row of table {MERGE_TABLE} per merge."
        );
        println!(
            "  layout: PRODUCTION, via ferrodb::cli::cli::open_database at {db_path_str}. Parent lease \
             interval {PARENT_SCAN_INTERVAL:?}, first pass awaited before anything is timed; the \
             restart child is a fresh process running the same open at the production interval."
        );
        println!(
            "  checkpoint interval: {} commits, the engine's value in force (A14.3; FERRODB_CHECKPOINT_INTERVAL={}). \
             Each CKPT line's `period` reads against it (A13.7).",
            ferrodb::wal::txn::checkpoint_interval(),
            std::env::var("FERRODB_CHECKPOINT_INTERVAL").unwrap_or_else(|_| "unset".into())
        );
        println!(
            "  workload: trunk row {ROW} = BigInt({TRUNK_VALUE}); each branch forks from trunk and \
             writes row {ROW} = BigInt(its id) with put_row, both under the statement lock."
        );
        println!("  seed rule: splitmix64 from 0xD197<<48 ^ N<<16 ^ threads<<8 ^ thread");
        print_read_header();
        print_restart_header();
        print_merge_header();
    }

    println!("D32: the curve to 10^6 WITH ONE PAGE WRITTEN PER BRANCH. {threads} threads.");
    println!("ARENA_EXTENT_PAGES = {ARENA_EXTENT_PAGES}, so D31 predicts ~{} KiB of DATA file per branch.",
             ARENA_EXTENT_PAGES * 4);
    println!("Budget: {budget_gb} GB. The run REFUSES to exceed it rather than fill the disk.");
    println!();
    // Two space columns on purpose. `data MB` is FILE LENGTH; `alloc MB` is blocks*512, what the
    // filesystem actually gave out. A reservation scheme can inflate length far past allocation, and
    // quoting only length would overstate the wall. Both are reported so neither can be cherry-picked.
    // The first nine columns are the historical ones, byte-for-byte in format, so every earlier
    // artifact of this harness still lines up. The five after `reopen ms` are D65 run 2's; see the
    // block that computes them for what each one proves.
    println!("         N   forks/sec   data MB   alloc MB   len B/branch   alloc B/branch   cat B/branch   pages live   reopen ms   cat live@re   img B/branch   arena re ms   arena re pg    reload ctl");

    let mut done = 0usize;
    let mut stopped_early: Option<(usize, u64)> = None;
    let mut negatives_checked = false;

    for &target in &checkpoints {
        if target <= done || stopped_early.is_some() {
            continue;
        }
        let seg = target - done;
        let per = seg / threads.max(1);
        let actually = per * threads;
        if actually == 0 {
            continue;
        }

        let t0 = Instant::now();
        let mut created: Vec<BranchId> = Vec::new();
        std::thread::scope(|s| {
            let writers: Vec<_> = (0..threads).map(|_| {
                let cat = Arc::clone(&hd.cat);
                let store = Arc::clone(&hd.store);
                let prod = hd.runtime.clone().zip(hd.statement_lock.clone());
                s.spawn(move || {
                    let mut mine: Vec<BranchId> = Vec::new();
                    for _ in 0..per {
                        if let Some((rt, statement_lock)) = &prod {
                            // READ-VS-N (PREREG section 1): the fork and the row write under ONE
                            // hold of the statement lock, as the CLI and pgwire run statements.
                            // `put_row` copies trunk's leaf into this branch's arena and publishes
                            // it with `set_root`, so the branch owns exactly one page.
                            let _statement = statement_lock.lock();
                            let rec = cat.fork(BranchId::TRUNK, lease).expect("fork");
                            rt.put_row(rec.branch_id, TABLE, ROW, &[Value::BigInt(rec.branch_id.id as i64)])
                                .expect("put_row");
                            mine.push(rec.branch_id);
                            continue;
                        }
                        let rec = cat.fork(BranchId::TRUNK, lease).expect("fork");
                        // THE THREE CALLS `branch_curve` LEAVES OUT. This is the whole difference.
                        let arena = store.arena_for(rec.branch_id).expect("arena");
                        let ep = cat.next_epoch();
                        let p = store
                            .alloc_in_arena(arena, PageType::BTreeLeaf, ep)
                            .expect("alloc");
                        let h = store.read_page(p).expect("read");
                        let mut f = h.write();
                        f.data[PAGE_HEADER_SIZE] = 0xD3;
                        stamp_checksum(&mut f.data);
                    }
                    mine
                })
            }).collect();
            for w in writers {
                created.extend(w.join().expect("writer thread"));
            }
        });
        let secs = t0.elapsed().as_secs_f64();
        branches.extend(created);
        done += actually;

        let md = std::fs::metadata(&main_path).ok();
        let data = md.as_ref().map(|m| m.len()).unwrap_or(0);
        let alloc = allocated_bytes(&main_path).unwrap_or(0);
        let cbytes = std::fs::metadata(&cat_path).map(|m| m.len()).unwrap_or(0);

        // D65 — reopen the catalog from disk, WITH DATA PRESENT.
        //
        // S4's O(1)-reopen claim has only ever been checked by `examples/branch_curve.rs`, which is
        // FORK-ONLY: no branch in it calls `arena_for` or `alloc_in_arena`, so it reopens a catalog
        // whose branches own no pages. This harness is the one that writes, and it did not measure
        // reopen at all — and it deletes its database at the end, so D61 could not answer this
        // after the fact. The timing block is `branch_curve.rs`'s own, reused rather than rewritten.
        //
        // Reported PER CHECKPOINT on purpose: one reopen number at 10^6 cannot separate O(1) from
        // O(log N) from a small O(N). The column across the decade is the measurement; a single
        // cell is an anecdote.
        let root = hd.cat_concrete.root_page_id();
        let t_reopen = Instant::now();
        let re = TableBranchCatalog::open_sidecar(&cat_path, root).expect("reopen");
        let reopen_ms = t_reopen.elapsed().as_secs_f64() * 1000.0;

        // D65 run 2 — POSITIVE CONTROL for the column above. A flat `reopen ms` is only evidence if
        // the catalog it opened is the populated one: an open that found an empty or stale tree
        // would be flat for the wrong reason. So ask the REOPENED handle how many branches are
        // live. Expected N + 1 (the trunk is Live too). Untimed — this is O(N) by construction
        // (`TableBranchCatalog::live_count` walks the Live span) and is not part of an open.
        let re_live = re.live_count().map(|n| n.to_string()).unwrap_or_else(|e| format!("ERR:{e:?}"));
        let re: Arc<dyn BranchCatalog> = Arc::new(re);

        // D65 run 2 — THE ARENA'S reopen, which the catalog column above does NOT measure.
        //
        // SCALE-DESIGN D189's addendum routed this question to D65: `<db>.arena`'s image holds every
        // extent, `current` entry and `pending` entry, and `reopen_from_checkpoint` deserialises
        // all of it in counted loops, so by source the arena's open is O(extents), once, at open.
        // D189 says in so many words not to quote the catalog's O(1) reopen for the arena. The
        // block above times `TableBranchCatalog::open_sidecar` only — a different file and a
        // different structure — so without this block a flat catalog column would be read as
        // closing a question it never asked.
        //
        // This is the production restart's CODE PATH: `cli.rs` writes `store.checkpoint` at clean
        // exit and opens with `ArenaPageStore::reopen_from_checkpoint`. ⚠ It is timed WARM-CACHE:
        // the probe was written and fsynced moments before and is read once below, so the OS page
        // cache holds it. A cold restart (after a reboot) additionally pays reading the image off
        // the SSD; that arm is not measured here — purging the cache needs root. The catalog
        // column above is warm for the same reason. What IS inside the timer, by source:
        // `base_page_in_state` (a structural walk + CRC32 via `image_len`), `reopen` (bitmap scan
        // of `main.db`, small), `load_file` (`image_len` AGAIN: a second walk + CRC32), and
        // `load_state` (a third CRC32 plus the deserialisation: per extent ~5 hash inserts —
        // `extents`, `recycled`, `current`, `claim_epoch`, `fill_unknown`). The checkpoint goes
        // to a SEPARATE probe path, so the live run's file (if `CURVE_PERSIST=1` armed one) is
        // never touched — `checkpoint` aimed anywhere but the armed path leaves its accounting
        // alone, by its own doc comment. The fresh pool over `main.db` is built OUTSIDE the timer:
        // it is the same constant the catalog column already pays, and the question is the
        // arena's deserialisation, not a second copy of that constant.
        //
        // Nothing is written through the reopened store: `reopen_from_checkpoint` arms the probe
        // path but writes only on a later claim, and the store is dropped without one.
        let probe = dir.join("probe.arena");
        hd.store.checkpoint(&probe).expect("probe checkpoint");
        // How many `current` entries the image must carry. Historically one per branch, so `done`.
        // In the production layout trunk writes too, and every restart clears `current`
        // ("Never resume filling a restored extent", `load_state`), so the count is the store's own.
        let n_current = if arms.is_some() { hd.store.current_arena_count() } else { done };
        let img = std::fs::read(&probe).expect("read probe image");
        let pool2 = {
            let f = std::fs::OpenOptions::new().read(true).write(true).open(&main_path).unwrap();
            Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())))
        };
        let t_arena = Instant::now();
        let reopened = ArenaPageStore::reopen_from_checkpoint(pool2, Arc::clone(&re), &probe)
            .expect("arena reopen");
        let arena_ms = t_arena.elapsed().as_secs_f64() * 1000.0;
        // POSITIVE CONTROL for the arena column — see `reload_control` for what it proves, and for
        // why its first version (plain byte equality) could never pass.
        let reload = reopened.state_bytes();
        let reload_ctl = match reload_control(&img, &reload, n_current) {
            Ok(()) => "ok".to_string(),
            Err(why) => format!("NO:{why}"),
        };
        // ...and the control must be able to FIRE. Once, at the first checkpoint, feed it three
        // reloads that are wrong in the three ways that matter; each must be refused, or the
        // column above means nothing and the run stops here rather than print it.
        if !negatives_checked {
            negatives_checked = true;
            // (1) A load that skipped the map entirely: a store assembled at the same base with
            //     NO image loaded, which is exactly what `reopen` without `load_file` produces.
            let pool3 = {
                let f = std::fs::OpenOptions::new().read(true).write(true).open(&main_path).unwrap();
                Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())))
            };
            let unloaded = ArenaPageStore::reopen(pool3, Arc::clone(&re), base)
                .expect("assemble unloaded store")
                .state_bytes();
            // (2) A load that kept `current` — the premise of the control's first version.
            // (3) A load that got one extent byte wrong: the last byte before `current`.
            let cur = current_section_offset(&img).expect("walk the probe image");
            let mut flipped = reload.clone();
            flipped[cur - 1] ^= 1;
            let neg = [
                ("unloaded", reload_control(&img, &unloaded, n_current)),
                ("kept-current", reload_control(&img, &img, n_current)),
                ("flipped-extent", reload_control(&img, &flipped, n_current)),
            ];
            for (name, r) in &neg {
                assert!(r.is_err(), "reload control PASSED the {name} negative: it cannot fire");
            }
            println!(
                "  # reload-control negatives at N={done}: {}",
                neg.iter().map(|(n, r)| format!("{n}={r:?}")).collect::<Vec<_>>().join("  ")
            );
        }
        // NOT a load control (review finding 3): `load_state` stores `live_pages` straight from
        // the image HEADER, so this reads N whether or not a single extent was deserialised. It
        // proves the header round-trips, nothing more.
        let arena_pages = reopened.live_page_count().unwrap_or(u32::MAX);
        drop(reopened);
        drop(re);
        let _ = std::fs::remove_file(&probe);

        println!(
            "  {:>8}   {:>9.1}   {:>7.1}   {:>8.1}   {:>12.0}   {:>14.0}   {:>12.0}   {:>10}   {:>9.3}   {:>11}   {:>12.1}   {:>11.3}   {:>11}   {:>11}",
            done,
            actually as f64 / secs,
            data as f64 / 1e6,
            alloc as f64 / 1e6,
            data as f64 / done as f64,
            alloc as f64 / done as f64,
            cbytes as f64 / done as f64,
            hd.store.live_page_count().unwrap_or(0),
            reopen_ms,
            re_live,
            img.len() as f64 / done as f64,
            arena_ms,
            arena_pages,
            reload_ctl,
        );

        // ---- READ-VS-N, at this checkpoint (bench/read_vs_n/PREREG.md) -------------------------
        if let Some(arms) = arms {
            // G1's in-path fire (A7.5): a real fork at the first checkpoint that `branches` never
            // records, so the catalog holds one live branch more than this run created.
            if fire == Fire::ExtraBranch && checkpoint_index == 0 {
                hd.cat.fork(BranchId::TRUNK, lease).expect("extra-branch fire: fork");
            }
            // G1: the branches read below are exactly the ones this run created, all live.
            let live = hd.cat_concrete.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
            if branches.len() != done || live != done as u64 + 1 {
                failures.push(format!(
                    "G1 N={done}: {} branches recorded, {live} live in the catalog (want {done} and {})",
                    branches.len(),
                    done + 1
                ));
            }
            if arms.read {
                let rt = hd.runtime.clone().expect("the production runtime");
                // Flush the way production does before a timed read, so no window pays a dirty
                // write-back: WAL, then the whole pool, then fsync (`TxnManager::checkpoint`).
                db.as_ref()
                    .expect("the production database is open")
                    .txn
                    .checkpoint()
                    .expect("checkpoint before the read arm");
                let h = catalog_height(&hd.cat_concrete);
                // Only `control-cold` needs these, and collecting them is O(N) catalog reads.
                let cold_pages: Vec<PageId> = if fire == Fire::ControlCold {
                    branches.iter().map(|&b| rt.root_of(b).expect("root_of")).collect()
                } else {
                    Vec::new()
                };
                warm_up(&rt, &branches);
                for &threads in &READ_THREADS {
                    let arms_tot = read_phase(
                        &rt,
                        &hd.store,
                        &branches,
                        trunk_root,
                        &cold_pages,
                        threads,
                        read_k,
                        checkpoint_index,
                        fire,
                    );
                    let row = ReadRow { n: done, h, threads, arms: arms_tot };
                    print_read_row(&row);
                    read_guards(&row, fire, &mut failures);
                    read_rows.push(row);
                }
            }
            if arms.restart {
                // H4's in-path fire (A7.5): the D40 crash-orphan shape, made on purpose. A fork
                // claims an EMPTY extent, then its record is marked `Reaped` without the reaper, so
                // the extent is charged to an owner that no longer exists at that generation. The
                // child's open sweep must free it. Done BEFORE the arena count, which includes it.
                if fire == Fire::OrphanExtent {
                    let orphan = hd.cat.fork(BranchId::TRUNK, lease).expect("orphan fire: fork").branch_id;
                    let arena = hd.store.arena_for(orphan).expect("orphan fire: claim an extent");
                    // A9.2: the premise, asserted. The child restores this extent with its fill
                    // unknown, and `resolve_fill` probes from page 0 and stops at the first page
                    // that fails to read. So the extent is empty to the open sweep exactly when its
                    // FIRST page does not read. Same probe, same page, here.
                    let (start, _) = hd.store.extent_range(arena).expect("orphan fire: the extent exists");
                    assert!(
                        hd.store.read_page(start).is_err(),
                        "orphan-extent fire did not inject: page {start}, the first of the claimed \
                         extent, reads as a valid page, so the open sweep's fill probe would count \
                         it and rightly leave the orphan alone"
                    );
                    hd.cat
                        .set_state(orphan, BranchState::Live, BranchState::Reaped)
                        .expect("orphan fire: mark reaped");
                }
                // Counted BEFORE the close, independently of anything the child reports (H2, H3).
                let expect_arenas = hd.store.live_arenas().len() as u64;
                // H2's in-path fire (A8): one more REAL extent, claimed for trunk AFTER that count.
                // The parent has just reopened (or, at the first checkpoint, trunk's one-page first
                // extent is full), so `arena_for` must claim a new extent — asserted, so a fire that
                // injected nothing panics instead of passing unfired. Trunk is live: H3, H4 and G1
                // cannot see it.
                if fire == Fire::ExtraExtent {
                    hd.store.arena_for(BranchId::TRUNK).expect("extra-extent fire: claim an extent");
                    assert_eq!(
                        hd.store.live_arenas().len() as u64,
                        expect_arenas + 1,
                        "extra-extent fire: arena_for reused trunk's extent, so H2 has nothing to see"
                    );
                }
                // Not `lease`: that names the LeaseDeadline every fork in this loop uses.
                let (lease_stats, closed) = db.take().expect("the production database is open").close();
                lease_end(&lease_stats, &format!("N={done} (the parent's close before the restart)"), &mut failures);
                // A21.1: returned to `main` through `failures`, not a panic, so the summary prints it.
                if let Err(e) = closed {
                    failures.push(format!("CLOSE N={done} (the parent's close before the restart): {e}"));
                }
                // Every handle into the files goes before the child opens them.
                drop(hd);
                // What the child is about to replay (A7.1, A7.3), sized after the clean close.
                let bytes = |ext: &str| {
                    std::fs::metadata(format!("{db_path_str}.{ext}")).map(|m| m.len()).unwrap_or(0)
                };
                let mut row = RestartRow {
                    n: done,
                    expect_arenas,
                    expect_live: live,
                    wal_bytes: bytes("wal"),
                    tel_bytes: bytes("tel"),
                    prov_bytes: bytes("provenance"),
                    m_rows,
                    ..Default::default()
                };
                // H6's in-path fire (A12.2): the marker a failed index undo leaves, at the first restart.
                if fire == Fire::StaleMarker && checkpoint_index == 0 {
                    let marker = stale_marker(&db_path_str);
                    std::fs::write(&marker, "READ-VS-N stale-marker fire\n").expect("stale-marker fire: write");
                    assert!(marker.exists(), "stale-marker fire did not inject: {} is absent", marker.display());
                }
                // H1's in-path fire: the parent holds the lock file, so the child's open refuses.
                let held = (fire == Fire::ChildLocked).then(|| {
                    DbLock::acquire(Path::new(&db_path_str)).expect("child-locked fire: hold the lock")
                });
                let is_last = checkpoints.last() == Some(&target);
                let child_fire = match fire {
                    Fire::NoClusterTime if is_last => "no-cluster-time",
                    _ => "none",
                };
                no_cluster_time_injected |= child_fire == "no-cluster-time";
                match run_child(&db_path_str, child_fire) {
                    Ok((child, raw)) => {
                        row.child = child;
                        row.raw = raw;
                    }
                    Err(why) => failures.push(format!("H1 N={done}: {why}")),
                }
                drop(held);
                // The parent's own reopen: the same open, in a warm process. A comparison only.
                let reopened = parent_open(&db_path_str, &mut failures);
                row.parent_total_us = reopened.timings.total.as_micros() as u64;
                row.parent_lease_us = reopened.timings.lease_start.as_micros() as u64;
                hd = Handles::of(&reopened);
                db = Some(reopened);
                if !row.child.is_empty() {
                    restart_guards(&row, fire, &mut failures);
                }
                print_restart_row(&row);
                restart_rows.push(row);
            }
            if arms.merge {
                // Axis (i): N grows, M fixed. The batch must start on an EMPTY `State`, which
                // lives in memory, so it runs right after an open: the restart arm's, or its own.
                if !arms.restart {
                    let open = db.take().expect("the production database is open");
                    drop(hd);
                    let reopened = reopen_for_merges(open, &db_path_str, &mut failures);
                    hd = Handles::of(&reopened);
                    db = Some(reopened);
                }
                let open = db.as_ref().expect("the production database is open");
                let live_before = hd.cat_concrete.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
                let start = open.runtime.state_sizes().applied;
                if start != (fire == Fire::WrongStart) as usize {
                    failures.push(format!(
                        "M2 N={done}: the axis-(i) batch began with {start} applied entries, so M \
                         was not held fixed"
                    ));
                }
                let mut ones = Vec::with_capacity(merge_k);
                // Right after an open, whose fresh `TxnManager` counts commits from zero.
                let mut since_ckpt = 0u64;
                for _ in 0..merge_k {
                    let one = merge_cycle(open, next_merge_row, fire, &mut since_ckpt);
                    next_merge_row += 1;
                    m_rows += one.applied_to_target as u64;
                    merge_guards(&format!("axis i N={done}"), &one, fire, &mut merge_delta, &mut failures);
                    ones.push(one);
                }
                let live_after = hd.cat_concrete.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
                let want = live_before + (fire == Fire::WrongLiveMerge) as u64;
                if live_after != want {
                    failures.push(format!(
                        "M5 N={done}: {live_after} live branches after the merge batch, {want} expected"
                    ));
                }
                let row = MergeRow { axis: "i", n: done, m: ones.len(), ones };
                print_merge_row(&row);
                merge_rows.push(row);
            }
            checkpoint_index += 1;
        }

        if data >= budget {
            stopped_early = Some((done, data));
            break;
        }
        // The disk, not just this run's share of it. See `free_bytes`.
        if let Some(free) = free_bytes(&main_path) {
            if free < FREE_FLOOR {
                println!(
                    "  STOPPING: {:.1} GiB free, floor is {:.0} GiB. Not this run's budget -- the \
                     DISK. Reported as a stop, not a result.",
                    free as f64 / (1u64 << 30) as f64,
                    FREE_FLOOR as f64 / (1u64 << 30) as f64,
                );
                stopped_early = Some((done, data));
                break;
            }
        }
    }
    if fire == Fire::NoClusterTime && !no_cluster_time_injected {
        failures.push(
            "H5: CURVE_FIRECHECK=no-cluster-time was requested but never injected: the run stopped \
             before the last checkpoint (or the restart arm is off), so H5 was not tested"
                .into(),
        );
    }

    println!();
    match stopped_early {
        Some((n, bytes)) => {
            let alloc_b = allocated_bytes(&main_path).unwrap_or(0);
            let alloc_per = alloc_b as f64 / n as f64;
            let per_branch = bytes as f64 / n as f64;
            println!("Allocated (blocks*512) rather than merely addressed: {:.0} B/branch, {:.2} GB total.",
                     alloc_per, alloc_b as f64 / 1e9);
            println!("  -> 10^6 branches x {:.0} allocated B = {:.2} TB actually on disk.",
                     alloc_per, alloc_per * 1e6 / 1e12);
            println!("STOPPED EARLY ON SPACE at N = {n}, data file {:.2} GB ({:.0} bytes/branch).",
                     bytes as f64 / 1e9, per_branch);
            println!("Extrapolated (ARITHMETIC, NOT MEASURED) to the objective:");
            println!("  10^6 branches x {:.0} B = {:.2} TB of data file.", per_branch,
                     per_branch * 1e6 / 1e12);
            println!("bench/curve_to_1e6.txt was reached fork-only, on the one workload that does not");
            println!("pay this. Fix is geometric extent growth (SCALE-DESIGN D31 option 1), NOT");
            println!("lowering ARENA_EXTENT_PAGES -- reclamation is per-extent on purpose.");
            if arms.is_none() {
                verdict(per_branch);
            }
        }
        None => {
            let data = std::fs::metadata(&main_path).map(|m| m.len()).unwrap_or(0);
            println!("DID NOT stop early within the budget: N = {done}, {:.0} bytes/branch.",
                     data as f64 / done.max(1) as f64);
            if arms.is_none() {
                verdict(data as f64 / done.max(1) as f64);
            }
        }
    }
    if arms.is_some_and(|a| a.merge) {
        // ---- ARM 2, axis (ii): N fixed at the last checkpoint, M grows (PREREG A3) -------------
        // One open, then merges up to each target. Nothing reopens in between, so `applied`,
        // `captures` and `merges` only grow; each target reports the last MERGE_BLOCK merges.
        let open = db.take().expect("the production database is open");
        drop(hd);
        let reopened = reopen_for_merges(open, &db_path_str, &mut failures);
        hd = Handles::of(&reopened);
        db = Some(reopened);
        let open = db.as_ref().expect("the production database is open");
        // A13.1's judge fire: one retained `Ddl` record, created before any merge. Its own
        // checkpoint (`ddl_checkpointed`) truncates first and resets the commit counter, and the
        // record is logged after it, so the log past its base is that record alone.
        //
        // A14.1: the injection is the record's RETENTION in `schema_log`, which is what every later
        // checkpoint re-appends; that is asserted. The base is expected NOT to move: after
        // `reopen_for_merges` the log is empty past its base (D227), so the DDL's truncation stores
        // the base it already had. A moved base is refused below (A15.4), not asserted.
        if fire == Fire::CkptDdl {
            let mut plain = Session::with_runtime(Arc::clone(&open.runtime));
            let base0 = open.txn.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
            exec_sql(open, &mut plain, CKPT_DDL_SQL).expect("ckpt-ddl fire: create the table");
            let base1 = open.txn.wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
            let past = open.txn.wal.next_lsn.load(std::sync::atomic::Ordering::SeqCst) - base1;
            let dir_root: u32 = open
                .catalog
                .lock()
                .get_table("ckpt_ddl")
                .map(|t| t.first_directory_page_id)
                .expect("ckpt-ddl fire: the table exists after CREATE TABLE");
            let retained = open.txn.retained_shape(dir_root).is_some();
            assert!(
                retained && past > 0,
                "ckpt-ddl fire did not inject: retained={retained}, {past} bytes past the base"
            );
            println!(
                "  CKPT-DDL base_moved={} retained={} bytes_past_base={past}",
                (base1 != base0) as u8,
                retained as u8
            );
            // A15.4: refused here, not only by the verdict script. A moved base means records sat
            // past it at the DDL's checkpoint, which A14.1 expects never to happen (D227); A12.1's
            // `a = 0` and `runs = M` for every CKPT line this run prints rest on the same premise.
            // The check sees only that the base moved, and the message names only that (A16.2).
            if base1 != base0 {
                failures.push(
                    "A14.1: ckpt-ddl's base MOVED across the DDL: records sat past the base at the DDL's \
                     checkpoint, and A14.1 expects none (D227). A12.1's a = 0 and runs = M rest on the same \
                     premise; amend the pre-registration before reading (f3)"
                        .into(),
                );
            }
        }
        let live_before = hd.cat_concrete.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
        let mut m_done = 0usize;
        let mut since_ckpt = 0u64;
        // A11.3: the ns of this period's applied merges that did not checkpoint.
        let mut period_ns: Vec<u128> = Vec::new();
        let mut axis_ckpts = 0usize;
        // A11.6's in-path fire: a real pin held across the whole axis, so no checkpoint truncates.
        let pin = (fire == Fire::PinnedCheckpoint).then(|| open.txn.wal.pin_durable());
        for &target in &merge_targets {
            let mut block = Vec::new();
            while m_done < target {
                let one = merge_cycle(open, next_merge_row, fire, &mut since_ckpt);
                next_merge_row += 1;
                m_rows += one.applied_to_target as u64;
                m_done += 1;
                merge_guards(&format!("axis ii M={m_done}"), &one, fire, &mut merge_delta, &mut failures);
                if one.applied_to_target && one.checkpointed {
                    axis_ckpts += 1;
                    period_ns.sort_unstable();
                    // Every checkpointing merge is a point (A13.7): an empty period costs the time
                    // fit its median, never the JUDGED bytes line its point.
                    ckpt_points.push(CkptPoint {
                        m: m_done,
                        runs: one.runs_after,
                        replay_bytes: one.replay_bytes,
                        ns: one.nanos,
                        median: period_ns.get(period_ns.len() / 2).map(|&m| m as f64),
                        period: one.period.unwrap_or(0),
                    });
                    period_ns.clear();
                } else if one.applied_to_target {
                    period_ns.push(one.nanos);
                }
                if m_done + MERGE_BLOCK > target {
                    block.push(one);
                }
            }
            let row = MergeRow { axis: "ii", n: done, m: m_done, ones: block };
            print_merge_row(&row);
            merge_rows.push(row);
        }
        drop(pin);
        // A13.9: the judge fire needs a truncating checkpoint to move the line it is for.
        if fire == Fire::CkptDdl && !ckpt_points.iter().any(|p| p.replay_bytes.is_some()) {
            failures.push(format!(
                "A12.1: CURVE_FIRECHECK=ckpt-ddl was requested but no axis-(ii) checkpoint truncated \
                 ({m_done} merges; the targets must reach one checkpoint interval), so the judge was \
                 not moved"
            ));
        }
        if fire == Fire::PinnedCheckpoint && axis_ckpts == 0 {
            failures.push(format!(
                "M6: CURVE_FIRECHECK=pinned-checkpoint was requested but no axis-(ii) merge \
                 checkpointed under the pin ({m_done} merges; the targets must reach one checkpoint \
                 interval), so M6 was not tested in path"
            ));
        }
        let live_after = hd.cat_concrete.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
        let want = live_before + (fire == Fire::WrongLiveMerge) as u64;
        if live_after != want {
            failures.push(format!(
                "M5 axis ii: {live_after} live branches after {m_done} merges, {want} expected"
            ));
        }
    }
    // A18.2: the final close runs BEFORE the summary, and a failure goes into `failures`, so it
    // suppresses "every guard held" and prints among the other NOT A RESULT lines.
    if let Some(open) = db.take() {
        let (lease_stats, closed) = open.close();
        lease_end(&lease_stats, "(the final close)", &mut failures);
        if let Err(e) = closed {
            failures.push(format!("the production database did not close cleanly at the end: {e}"));
        }
    }
    if let Some(arms) = arms {
        // D61's space verdict is about ITS layout. The production file also holds the SQL catalog
        // and a 32,736-page hole below the arena floor, so bytes/branch here is not its question.
        println!("(D61's space verdict is not printed: the production layout puts the arena above a");
        println!(" headroom region, so file bytes/branch is not the quantity that verdict judges.)");
        read_vs_n_summary(
            arms,
            &read_rows,
            &restart_rows,
            &merge_rows,
            &ckpt_points,
            fire,
            &mut failures,
            &mut ns_void,
        );
    } else {
        // Only the summary prints `failures`; without it a non-zero exit would carry no reason (A11.4).
        for f in failures.iter() {
            println!("NOT A RESULT: {f}");
        }
    }
    let code = if failures.is_empty() && ns_void.is_empty() { 0 } else { 2 };
    drop(hd);
    // The run directory is removed by `_cleanup` — here, before a non-zero exit skips destructors,
    // and on the way out otherwise, panic or not.
    drop(_cleanup);
    if code != 0 {
        std::process::exit(code);
    }
}

/// G7's measure of the box (PREREG A15.1, A16.3): arm 1's control ns, max over min across N, per
/// thread count, with its row count. It is the only drift instrument this harness has, so it is
/// printed beside every time number the summary reports, with what it cannot see: it samples only
/// arm 1's read phases, and a max/min across N is blind to a shift common to every N.
fn g7_drift(read_rows: &[ReadRow], arm_ran: bool) -> String {
    if !arm_ran {
        return "G7: not measured: arm 1 did not run, so no time here has a drift measure".into();
    }
    let parts: Vec<String> = READ_THREADS
        .iter()
        .filter_map(|&t| {
            let cs: Vec<f64> =
                read_rows.iter().filter(|r| r.threads == t).map(|r| r.arms[1].ns_per_read()).collect();
            if cs.len() < 2 {
                return (cs.len() == 1).then(|| format!("T={t}: 1 row, no across-N measure"));
            }
            let lo = cs.iter().cloned().reduce(f64::min)?;
            let hi = cs.iter().cloned().reduce(f64::max)?;
            Some(format!("T={t} {:.3}x over {} rows", hi / lo, cs.len()))
        })
        .collect();
    if parts.is_empty() {
        return "G7: arm 1 ran but left no row, so no time here has a drift measure".into();
    }
    format!(
        "G7 (arm 1's control, max/min across N; band 1.5: above it, every ns column at every T is NOT A \
         RESULT, while arm 3's values and R7 are not voided by it): {}. It sees only arm 1's read phases: axis (ii) \
         runs after the last of them, arm 3's opens follow them rather than coincide, and a max/min across N \
         cannot see a shift common to every N",
        parts.join(", ")
    )
}

/// The READ-VS-N summary: the per-N curves the pre-registration is judged against, the two guards
/// that can only be judged across N (G5, G7), and the verdict lines. Evaluating the predictions
/// themselves is the reader's job, against `bench/read_vs_n/PREREG.md` — the harness prints facts
/// and guard results, and does not grade its own run.
fn read_vs_n_summary(
    arms: Arms,
    read_rows: &[ReadRow],
    restart_rows: &[RestartRow],
    merge_rows: &[MergeRow],
    ckpt_points: &[CkptPoint],
    fire: Fire,
    failures: &mut Vec<String>,
    ns_void: &mut Vec<String>,
) {
    println!();
    println!("=== READ-VS-N SUMMARY ===================================================================");
    if arms.merge {
        // Arm 2. Axis (i) is read against N, axis (ii) against M; the slope columns are local
        // log-log slopes from the previous row of the same axis.
        for (axis, against) in [("i", "N"), ("ii", "M")] {
            let rows: Vec<&MergeRow> = merge_rows.iter().filter(|r| r.axis == axis).collect();
            // A13.3: Q10 is REPORTED, not judged. Its time columns (the median, the local slope of
            // the median from the previous row, and the typical merge's mean ± SE) move with the box
            // as much as with N or M, and nothing here controls for that. The judged content of
            // "merge cost against N and M" is the integers: Q1–Q9 and the replay bytes (A12.1).
            // A10.1: the checkpoint is amortized over its MEASURED period (`MergeOne::period`), not
            // over 256: the trigger waits for an empty active-transaction table, and a cycle may
            // commit more than once. Not over the printed blocks' checkpoint share either: axis
            // (ii)'s blocks end at multiples of 256 on purpose, so they over-sample that merge.
            println!("arm 2, axis ({axis}) — per-merge, against {against}; ns slope on the MEDIAN (reported, A13.3):");
            println!("  {}", g7_drift(read_rows, arms.read));
            println!(
                "         N        M  ns median   slope    ns mean     V_hi   slope(V_hi)   V_cell  captures  attested  \
                 seq tup  c.fault  ret runs  ckpts    ckpt ns  period   amort ns   typ ns ± SE"
            );
            let mut prev: Option<(usize, f64, f64)> = None;
            for r in &rows {
                let x = if axis == "i" { r.n } else { r.m };
                let (ns, vhi) = (r.ns_median(), r.mean(|o| o.v_hi));
                let (s_ns, s_v) = match prev {
                    // A zero V_hi (the first rows of a batch) has no logarithm; say so, not NaN.
                    Some((px, pns, pv)) if pv > 0.0 && vhi > 0.0 => (
                        format!("{:>7.3}", slope(pns, ns, px, x)),
                        format!("{:>13.3}", slope(pv, vhi, px, x)),
                    ),
                    Some((px, pns, _)) => (format!("{:>7.3}", slope(pns, ns, px, x)), format!("{:>13}", "-")),
                    None => (format!("{:>7}", "-"), format!("{:>13}", "-")),
                };
                let last = r.ones.last().copied().unwrap_or_default();
                let (ckpts, ckpt_ns, _) = r.ckpt();
                // The checkpoint's excess over the typical merge, and the amortized cost it implies.
                // Its growth with M is judged on the replay BYTES in `print_ckpt_fit` (A12.1).
                let excess = ckpt_ns.map(|c| c - ns);
                let periods: Vec<u64> = r.ones.iter().filter_map(|o| o.period).collect();
                let period = (!periods.is_empty())
                    .then(|| periods.iter().sum::<u64>() as f64 / periods.len() as f64);
                let amort = match (excess, period) {
                    (Some(e), Some(p)) => format!("{:>10.0}", ns + e / p),
                    _ => format!("{:>10}", "-"),
                };
                let period = period.map(|p| format!("{p:>7.1}")).unwrap_or_else(|| format!("{:>7}", "-"));
                let typ = r
                    .typ_mean_se()
                    .map(|(m, se)| format!("{:>13}", format!("{m:.0}±{se:.0}")))
                    .unwrap_or_else(|| format!("{:>13}", "-"));
                println!(
                    "  {:>8} {:>8} {:>10.0} {s_ns} {:>10.0} {:>8.1} {s_v} {:>8.2} {:>9} {:>9} {:>8.2} {:>8.3} {:>9} {:>6} \
                     {:>10} {period} {amort} {typ}",
                    r.n,
                    r.m,
                    ns,
                    r.mean(|o| o.nanos as u64),
                    vhi,
                    r.mean(|o| o.v_cell),
                    last.after.captures,
                    last.attested_after,
                    r.mean(|o| o.seq_tuples),
                    r.mean(|o| o.census.faults),
                    last.runs_after,
                    ckpts,
                    ckpt_ns.map(|c| format!("{c:.0}")).unwrap_or_else(|| "-".into()),
                );
                prev = Some((x, ns, vhi));
            }
            println!(
                "  axis ({axis}): `period` = merge cycles since the previous checkpoint (or the open), \
                 the checkpointing one included; `amort ns` = median + (ckpt ns - median) / period (A10.1)."
            );
            println!(
                "  axis ({axis}): `slope` spans the previous row to this one ({}); `typ ns ± SE` is the mean \
                 and sd/sqrt(n) over the block's applied merges that did not checkpoint: a WITHIN-block error over \
                 consecutive merges, not a noise floor for comparing rows (A14.4). Reported, not judged (A13.3).",
                rows.iter().map(|r| (if axis == "i" { r.n } else { r.m }).to_string()).collect::<Vec<_>>().join(" -> ")
            );
            if axis == "ii" {
                print_ckpt_fit(ckpt_points);
            }
            if rows.len() < 2 {
                failures.push(format!("arm 2 axis ({axis}): {} row(s); one point is not a curve", rows.len()));
            }
        }
    }
    if arms.read {
        for &t in &READ_THREADS {
            let rows: Vec<&ReadRow> = read_rows.iter().filter(|r| r.threads == t).collect();
            println!("arm 1, T={t}:");
            println!("         N   h   branch ns   control ns    ratio   slope(branch)   slope(ratio)   c.att/rd  c.fault/rd  d.fault/rd");
            let mut prev: Option<(usize, f64, f64)> = None;
            for r in &rows {
                let [branch, control, _] = &r.arms;
                let (b, c) = (branch.ns_per_read(), control.ns_per_read());
                let (sb, sr) = match prev {
                    Some((pn, pb, pr)) => (
                        format!("{:>15.3}", slope(pb, b, pn, r.n)),
                        format!("{:>14.3}", slope(pr, b / c, pn, r.n)),
                    ),
                    None => (format!("{:>15}", "-"), format!("{:>14}", "-")),
                };
                println!(
                    "  {:>8} {:>3} {:>11.1} {:>12.1} {:>8.3} {sb} {sr} {:>10.3} {:>11.3} {:>11.3}",
                    r.n,
                    r.h,
                    b,
                    c,
                    b / c,
                    branch.per(branch.cat.attempts),
                    branch.per(branch.cat.faults),
                    branch.per(branch.data.faults),
                );
                prev = Some((r.n, b, b / c));
                // G5: a control that missed the pool at this N is not a control at this N.
                if control.data.faults > 0 {
                    ns_void.push(format!(
                        "G5 N={} T={t}: the control faulted {} times; its page was evicted, so it \
                         depends on N here",
                        r.n, control.data.faults
                    ));
                }
            }
            // A15.5: every slope column names its span, and the judged forms are said here.
            println!(
                "  T={t}: `slope(branch)` and `slope(ratio)` span the previous row to this one (N {}). \
                 The CLASS is the verdict script's OLS over the saturated segment (d.fault >= 0.9): BOUNDED \
                 only when the raw and ratio slopes are both < 0.3, INCONCLUSIVE when they disagree (A15.2). \
                 KNEE is conjunctive at T=1: HELD only when the raw knee and ratio(1e6)/ratio(256) are both in \
                 [4, 25], INCONCLUSIVE when they disagree (A16.1). RESIDENT and T8DIR are reported.",
                rows.iter().map(|r| r.n.to_string()).collect::<Vec<_>>().join(" -> ")
            );
            // G7: the control's ns across N. It cannot depend on N, so if it moved, the box did.
            // A16.3: one row is no across-N measure, so no ratio is printed for it.
            let cs: Vec<f64> = rows.iter().map(|r| r.arms[1].ns_per_read()).collect();
            if cs.len() == 1 {
                println!("  control ns at T={t}: 1 row, no across-N measure");
            }
            if let (true, Some(lo), Some(hi)) = (
                cs.len() >= 2,
                cs.iter().cloned().reduce(f64::min),
                cs.iter().cloned().reduce(f64::max),
            ) {
                let moved = hi / lo;
                println!(
                    "  control ns max/min across N at T={t}: {moved:.3} over {} rows (band 1.5: above it, every ns \
                     column at every T is NOT A RESULT)",
                    cs.len()
                );
                if moved > 1.5 {
                    ns_void.push(format!(
                        "G7 T={t}: the control moved {moved:.2}x across N (band 1.5x); the box moved, \
                         so no ns column at ANY T can be read as a function of N (PREREG G7, A16.3)"
                    ));
                }
            }
            if rows.len() < 2 {
                failures.push(format!("arm 1 T={t}: {} checkpoint(s) read; one point is not a curve", rows.len()));
            }
        }
    }
    if arms.restart {
        println!("arm 3 (fresh-process open; microsecond timers in the child, printed here in ms):");
        println!("  {}", g7_drift(read_rows, arms.read));
        // A7.1/A9.3/A11.1: `recovered` judges D216's before/after pair. `wal B` = 24 judges only the
        // merge-OFF control, since after D216 a merge-on log still holds re-appended declarations.
        // `recover`, `rebuild` and `m rows` size it. A9.1: warmth is `child net` (total less recover
        // and rebuild, which only the child pays before D216) against `parent total`.
        // A14.2: the load flag (D65's L2). The readings of the counted rows, start and end, set the
        // median (the upper one for an even count, as every median here); a row is flagged when its
        // larger reading exceeds 1.5 × that median. A flag, never a guard: the slopes it qualifies
        // are reported, not judged.
        //
        // A15.6: which rows are counted is ONE rule with the verdict script, whose `rrows` leaves out
        // H6 rows. An H6 open
        // rebuilt for the stale-index marker and is NOT A RESULT for every arm-3 value, so it sets
        // no baseline and carries no flag. (`stale` absent reads as u64::MAX, which H6 also takes.)
        let is_h6 = |r: &RestartRow| r.get("stale") != 0;
        // A20.7: ONE binding for the rows arm 3 counts (a child line, not H6). The L2 statement and
        // the ARM3 refusal both read it; Y1 was these two filters diverging.
        let counted = restart_rows.iter().filter(|r| !r.child.is_empty() && !is_h6(*r)).count();
        let mut loads: Vec<u64> = restart_rows
            .iter()
            .filter(|r| !r.child.is_empty() && !is_h6(*r))
            .flat_map(|r| [r.get("load_start_centi"), r.get("load_end_centi")])
            .filter(|&l| l != u64::MAX)
            .collect();
        loads.sort_unstable();
        let load_median = loads.get(loads.len() / 2).copied();
        let centi = |l: u64| if l == u64::MAX { format!("{:>6}", "-") } else { format!("{:>6.2}", l as f64 / 100.0) };
        println!("         N   total ms   slope   lease_start ms   slope   arena ms   us/visit   1st-pass ms   parent total ms   parent lease ms   recover ms   rebuild ms    wal B   m rows  recovered  child net ms   load0   load1   L2");
        let mut prev: Option<(usize, f64, f64)> = None;
        // A15.3: which rows L2 can vouch for, and which it cannot.
        let (mut flagged, mut unavailable, mut partly) = (Vec::new(), Vec::new(), Vec::new());
        for r in restart_rows.iter().filter(|r| !r.child.is_empty()) {
            // A17.2: an H6 row is excluded from every arm-3 value here, for real: it prints only its
            // N, and the slope anchor `prev` skips it, so no later row's slope is taken from it.
            if is_h6(r) {
                println!(
                    "  {:>8}  H6: NOT A RESULT (stale-index marker at open); its values are in its RESTART and \
                     RESTART-RAW lines",
                    r.n
                );
                continue;
            }
            let total = r.get("total_us") as f64 / 1000.0;
            let lease = r.get("lease_start_us") as f64 / 1000.0;
            let (st, sl) = match prev {
                Some((pn, pt, pl)) => (
                    format!("{:>7.3}", slope(pt, total, pn, r.n)),
                    format!("{:>7.3}", slope(pl, lease, pn, r.n)),
                ),
                None => (format!("{:>7}", "-"), format!("{:>7}", "-")),
            };
            let (l0, l1) = (r.get("load_start_centi"), r.get("load_end_centi"));
            let hi = [l0, l1].into_iter().filter(|&l| l != u64::MAX).max();
            let readings = [l0, l1].into_iter().filter(|&l| l != u64::MAX).count();
            let l2 = match (hi, load_median) {
                (Some(h), Some(m)) => {
                    if readings < 2 {
                        partly.push(r.n);
                    }
                    if (h as f64) > 1.5 * (m as f64) {
                        flagged.push(r.n);
                        "1"
                    } else {
                        "0"
                    }
                }
                _ => {
                    unavailable.push(r.n);
                    "UNAVAILABLE"
                }
            };
            let (load0, load1) = (centi(l0), centi(l1));
            println!(
                "  {:>8} {:>10.3} {st} {:>16.3} {sl} {:>10.3} {:>10.3} {:>13.3} {:>17.3} {:>17.3} {:>12.3} {:>12.3} {:>8} {:>8} {:>10} {:>13.3}  {load0}  {load1} {l2:>4}",
                r.n,
                total,
                lease,
                r.get("arena_us") as f64 / 1000.0,
                r.get("lease_start_us") as f64 / r.get("open_visits").max(1) as f64,
                r.get("first_pass_us") as f64 / 1000.0,
                r.parent_total_us as f64 / 1000.0,
                r.parent_lease_us as f64 / 1000.0,
                r.get("recover_us") as f64 / 1000.0,
                r.get("rebuild_us") as f64 / 1000.0,
                r.wal_bytes,
                r.m_rows,
                r.get("recovered"),
                total - r.get("recover_us").saturating_add(r.get("rebuild_us")) as f64 / 1000.0,
            );
            prev = Some((r.n, total, lease));
        }
        println!(
            "  arm 3: the `total` and `lease_start` slopes span the previous non-H6 row to this one (N {}); both \
             are REPORTED (A14.2), as are R6 and R8-share (A15.1).",
            restart_rows
                .iter()
                .filter(|r| !r.child.is_empty() && !is_h6(*r))
                .map(|r| r.n.to_string())
                .collect::<Vec<_>>()
                .join(" -> ")
        );
        println!(
            "  arm 3: load0/load1 = the child's 1-min load at its start and end; L2 = 1 when the row's larger \
             reading exceeds 1.5 x the run's median reading ({}), H6 rows left out of the median and the table \
             (A15.6, the verdict script's rule). The time slopes and R5's magnitude are REPORTED \
             (A14.2); O(N) is judged on R1-R3's integers.",
            load_median.map(|m| format!("{:.2}", m as f64 / 100.0)).unwrap_or_else(|| "-".into())
        );
        // A15.3: a clean-load statement only when every counted row has both readings. A16.8: and
        // only when a row was counted at all, since "every counted row" is vacuous over none.
        if counted == 0 {
            println!("  L2: no row was counted.");
        } else if counted < 2 {
            // A18.8: one row's larger reading is its own median, so L2 cannot fire on it.
            println!("  L2: not judged (fewer than two counted rows).");
        } else if !unavailable.is_empty() || !partly.is_empty() {
            println!(
                "  L2 is incomplete: UNAVAILABLE (no load reading) at N={unavailable:?}; one reading only at \
                 N={partly:?}; L2 = 1 at N={flagged:?}. No clean-load statement is made for this run."
            );
        } else if flagged.is_empty() {
            println!("  L2: no row carries L2 = 1, and every counted row has both readings.");
        } else {
            println!("  L2 = 1 at N={flagged:?}: read those rows' times as load-contaminated.");
        }
        println!(
            "  L2's blind spot (A15.3): its instrument is two point readings of a 1-minute exponentially damped \
             average, so around an open of a few seconds both readings describe mostly the minute BEFORE it. \
             L2 = 0 on a short open is not evidence of a quiet open."
        );
        // A16.4: the rows left out of every arm-3 value, named, so no value reads as covering them.
        let h6_rows: Vec<usize> =
            restart_rows.iter().filter(|r| !r.child.is_empty() && is_h6(*r)).map(|r| r.n).collect();
        println!(
            "  arm 3: H6 rows, excluded from every arm-3 value and from the load median: {}",
            if h6_rows.is_empty() { "none".to_string() } else { format!("N={h6_rows:?}") }
        );
        println!(
            "  arm 3: R7's step times (files, lock, recover, sql_catalog, branch_catalog, effect_log, runtime, \
             provenance) are REPORTED, not judged (A16.4). This table carries `recover` only; the other seven are \
             printed in the RESTART rows above, so L2 (this table) and G7 (this section's head) sit beside those \
             seven only by joining the rows on N (A18.6). recover_us is reported too; `recovered` judges its fact \
             (A16.5)."
        );
        println!(
            "  arm 3: warmth, `child net ms` (child total less recover and rebuild) against `parent total ms`, is \
             REPORTED, not judged: no band was registered for it before data (A21.2)."
        );
        // A18.1: an H6 row is no point of the curve (A17.2), so it is not counted here either.
        if counted < 2 {
            failures.push(format!(
                "arm 3: {counted} restart(s) counted (H6 rows left out); one point is not a curve"
            ));
        }
    }
    println!();
    if fire == Fire::CkptDdl {
        println!(
            "FIRECHECK {fire:?}: a JUDGE fire (A13.1). No harness guard reads the replay-bytes integer; the \
             replay-bytes line above must carry the pre-registered intercept. Every NOT A RESULT below is to be \
             graded (A18.3-A18.5, A20.2-A20.4): `A14.1: ckpt-ddl's base MOVED ...` = a finding (the premise \
             changed; amend before reading); `A12.1: CURVE_FIRECHECK=ckpt-ddl was requested but no axis-(ii) \
             checkpoint truncated ...` (A13.9) = DID_NOT_FIRE (the record was retained, but nothing re-appended \
             it); `the parent's first lease pass did not finish ...` (PARENT_PASS), `the production database did \
             not close cleanly ...` and `CLOSE ...` (CLOSE, A21.1) and `LEASE ...` = findings, each ALSO a finding beside A13.9's line \
             (A20.2); any other = a finding."
        );
    } else if fire != Fire::None {
        // A20.1: what must fire and what is allowed beside it, by id, from `Fire::expects`.
        let (must, may) = fire.expects();
        println!(
            "FIRECHECK {fire:?}: must appear below: {}; allowed extras (PREREG): {}; any other is a finding \
             (A20.1).",
            must.join(", "),
            if may.is_empty() { "none".to_string() } else { may.join(", ") }
        );
    }
    if failures.is_empty() && ns_void.is_empty() {
        println!("GUARDS: every guard held. The counters and the ns columns stand as printed.");
    }
    for f in failures.iter() {
        println!("NOT A RESULT: {f}");
    }
    for v in ns_void.iter() {
        println!("NOT A RESULT (the ns columns only; the integer counters stand): {v}");
    }
}
