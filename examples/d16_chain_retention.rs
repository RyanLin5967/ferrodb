//! D16: what a chain of reaped interior branches keeps ALLOCATED while its leaf lives.
//!
//! SCALE-DESIGN D16 chose option 1: a `Reaped` interior branch that still has a live descendant
//! keeps counting as a live child of its own parent, so its pages stay parked. The cost it
//! recorded — "a space leak proportional to chain length, which is the exact shape MCTS
//! generates" — was never measured. This is the instrument. The predictions, the guards and what
//! every other outcome would mean are in `bench/d16_chain/PREREG.md`, committed before this file.
//!
//!   d16_chain_retention [depths,comma,separated] [pages_per_level] [arms,comma,separated]
//!
//! Defaults: `1,10,100,1000 2 fanout,chain,overwrite`. `D16_PERSIST=0` turns off persistence of the
//! free-space map, which is ON by default because `cli.rs` ships it on.
//!
//! **Arms.** `chain`: trunk -> b1 -> ... -> bD, every level writes P fresh pages and then forks the
//! next, and only the leaf survives. `fanout`: D siblings with the same writes, the last one
//! survives, so there are no interiors. `overwrite`: the chain again, but every level overwrites
//! its parent's pages through `cow_page`, so no interior page is the current version of anything
//! the leaf reads. **`overwrite` is the only arm in which retention and leak are the same number.**
//! In `chain` the leaf can still read every interior page, so keeping them is required.
//!
//! **The reap is the production one.** `Reaper::reap_expired` runs `expired_candidates` (deepest
//! first) and then `reap_if_still_expired` for each branch: the two halves
//! `lease_thread::scan_once` runs. Every reap ends in its own drain. A D183 number was retracted
//! because its test hand-rolled a reap that skipped `drain_pending`, so nothing here reaps any
//! other way. After each sweep the harness runs `collect_orphaned_extents`, the collector the lease
//! thread runs, and only then takes the census, so what it reports is the steady state and not a
//! transient.
//!
//! **`drain_pending()` runs only AFTER the census, as a probe.** Production never calls it outside a
//! reap (PREREG amendment 1 corrects an earlier claim that it did). Run before the census, it would
//! clean up after a reap that skipped its own drain, and the retention columns would read 0 while
//! the defect was live. Run after, whatever it releases is reported as `extra_drain` /
//! `after_leaf_drain`, which are predicted to be 0.
//!
//! **The instrument is the layer that pays: arena pages still allocated.** They are counted twice,
//! once by the store's net counter and once by enumerating every live extent's allocated pages,
//! because a net count can hide a leak and a release that cancel out. The file length is not used:
//! freed extents go back to the free-space map and the file never shrinks.
//!
//! Depths are the outer loop and `fanout` runs first by default, so the cheap rows for a depth are
//! printed before its expensive ones. Under a live leaf, sweep 1 of a chain arm is expected to cost
//! on the order of P·D³ CHILD-span scans (PREREG, "Secondary"). Every phase prints a flushed
//! `progress` line first, so a run stopped by `timeout` names the cell and phase it stopped in.
//!
//! **Fire-checks inject inside the engine, not here.** `bench/d16_chain/firecheck.py` applies one
//! source mutant at a time to `src/`, rebuilds, and checks every cell's verdict, every `compare`
//! line, the slot line, the build flag and the exit code against PREREG A1.6 and A2.3-A2.4. This
//! file has no fault switch of its own. A forced result at the call site would test nothing inside
//! the path being measured.
//!
//! Each half of a two-part guard has its own id (G1/G1b, G2/G2b, G3/G3b, G4/G4b; PREREG A2.2), so a
//! fire-check can say which half fired.
//!
//! Exit 0: every guard held and every prediction matched. Exit 1: the guards held but a prediction
//! did not (that is a result, printed as MISMATCH). Exit 2: NOT A RESULT. A dirty or unknown build
//! stamp (G0) always exits 2. The per-cell verdicts are still printed, because a fire-check build is
//! dirty by construction.
use std::collections::BTreeSet;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, BranchState, LeaseDeadline, PageId};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

/// Lease deadline of every branch the first sweep must reap.
const DOOMED_DEADLINE: u64 = 1_000;
/// Lease deadline of the survivor. Only the second sweep (control a) reaches it.
const SURVIVOR_DEADLINE: u64 = 2_000;

/// The extent sizes pre-registered in `bench/d16_chain/PREREG.md`: one page first, each next extent
/// double the last, capped at 256. They are written out here, not read from `ferrodb`, so the
/// reserved-page prediction does not come from the code it checks.
const MODEL_FIRST_EXTENT: u64 = 1;
const MODEL_EXTENT_CAP: u64 = 256;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    Fanout,
    Chain,
    Overwrite,
}

impl Arm {
    fn parse(s: &str) -> Option<Arm> {
        match s {
            "fanout" => Some(Arm::Fanout),
            "chain" => Some(Arm::Chain),
            "overwrite" => Some(Arm::Overwrite),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Arm::Fanout => "fanout",
            Arm::Chain => "chain",
            Arm::Overwrite => "overwrite",
        }
    }
}

/// One cell's directory, removed when the cell is done. It is the LAST field of [`Rig`], so the
/// store and the catalog that hold files inside it are dropped before it is removed.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Rig {
    catalog: Arc<dyn BranchCatalog>,
    store: Arc<ArenaPageStore>,
    reaper: TwoTierReaper,
    _scratch: Scratch,
}

/// A fresh database per cell, so no cell inherits another's pages, ids or pending log.
fn rig(arm: Arm, depth: usize, persist: bool) -> Result<Rig, String> {
    let dir = std::env::temp_dir()
        .join(format!("ferrodb-d16chain-{}-{}-{}", std::process::id(), arm.name(), depth));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e:?}", dir.display()))?;
    let scratch = Scratch(dir.clone());
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(dir.join("main.db"))
        .map_err(|e| format!("open main.db: {e:?}"))?;
    let disk = DiskManager::new(file).map_err(|e| format!("DiskManager::new: {e:?}"))?;
    let pool = Arc::new(BufferPoolManager::new(Arc::new(disk)));
    // The catalog that ships, on a real file. `LogBranchCatalog` is not run: PREREG, "Not covered".
    let catalog: Arc<dyn BranchCatalog> = Arc::new(
        TableBranchCatalog::open_sidecar(&dir.join("branches.branchcat"), 1)
            .map_err(|e| format!("open the branch catalog: {e:?}"))?,
    );
    let base = pool.disk_manager.high_water().map_err(|e| format!("high_water: {e:?}"))?;
    let store = Arc::new(
        ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), base)
            .map_err(|e| format!("ArenaPageStore::new: {e:?}"))?,
    );
    if persist {
        store.checkpoint_to(dir.join("main.db.arena"));
    }
    let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));
    Ok(Rig { catalog, store, reaper, _scratch: scratch })
}

/// Write `p` fresh pages into `branch`, one epoch each, the way `branch_curve_writes` and
/// `d31_reap_cost` do.
fn write_fresh(rig: &Rig, branch: BranchId, level: usize, p: usize) -> Result<Vec<PageId>, String> {
    let mut pages = Vec::with_capacity(p);
    for _ in 0..p {
        let epoch = rig.catalog.next_epoch();
        let id = rig
            .store
            .alloc_for(branch, PageType::BTreeLeaf, epoch)
            .map_err(|e| format!("alloc_for {branch}: {e:?}"))?;
        let h = rig.store.read_page(id).map_err(|e| format!("read_page {id}: {e:?}"))?;
        let mut f = h.write();
        f.data[PAGE_HEADER_SIZE] = (level & 0xff) as u8;
        stamp_checksum(&mut f.data);
        pages.push(id);
    }
    Ok(pages)
}

/// Overwrite each of `parents` in `branch` through the store's real copy-on-write entry point.
/// Every answer must be a fresh private copy that leaves the original to its owner (G6). One that
/// is not is counted in `bad` and never repaired, because repairing it would change what is
/// measured.
fn overwrite(
    rig: &Rig,
    branch: BranchId,
    level: usize,
    parents: &[PageId],
    bad: &mut u64,
) -> Result<Vec<PageId>, String> {
    let mut pages = Vec::with_capacity(parents.len());
    for &prev in parents {
        let epoch = rig.catalog.next_epoch();
        let cp = rig
            .store
            .cow_page(prev, branch, epoch)
            .map_err(|e| format!("cow_page {prev} in {branch}: {e:?}"))?;
        if !cp.copied || cp.retire_previous {
            *bad += 1;
        }
        {
            let mut f = cp.handle.write();
            f.data[PAGE_HEADER_SIZE] = (level & 0xff) as u8;
            stamp_checksum(&mut f.data);
        }
        pages.push(cp.page_id);
    }
    Ok(pages)
}

/// What a fixture made: the branches sweep 1 must reap and the one it must leave alone.
struct Built {
    doomed: Vec<BranchId>,
    survivor: BranchId,
    /// G6: `cow_page` answers that were not a fresh private copy. Always 0 outside `overwrite`.
    bad_cows: u64,
}

fn build_chain(rig: &Rig, depth: usize, p: usize, overwrites: bool) -> Result<Built, String> {
    let mut doomed = Vec::with_capacity(depth.saturating_sub(1));
    let mut survivor = None;
    let mut parent = BranchId::TRUNK;
    let mut parent_pages: Vec<PageId> = Vec::new();
    let mut bad_cows = 0u64;
    for level in 1..=depth {
        let deadline = if level == depth { SURVIVOR_DEADLINE } else { DOOMED_DEADLINE };
        let b = rig
            .catalog
            .fork(parent, LeaseDeadline(deadline))
            .map_err(|e| format!("fork level {level} off {parent}: {e:?}"))?
            .branch_id;
        // Written BEFORE the next level forks, so every page here is born before its child's fork
        // epoch and sits inside the window the interval rule protects.
        parent_pages = if overwrites && level > 1 {
            overwrite(rig, b, level, &parent_pages, &mut bad_cows)?
        } else {
            write_fresh(rig, b, level, p)?
        };
        if level == depth {
            survivor = Some(b);
        } else {
            doomed.push(b);
        }
        parent = b;
    }
    let survivor = survivor.ok_or_else(|| "depth 0 builds no chain".to_string())?;
    Ok(Built { doomed, survivor, bad_cows })
}

fn build_fanout(rig: &Rig, depth: usize, p: usize) -> Result<Built, String> {
    let mut doomed = Vec::with_capacity(depth.saturating_sub(1));
    let mut survivor = None;
    for k in 1..=depth {
        let deadline = if k == depth { SURVIVOR_DEADLINE } else { DOOMED_DEADLINE };
        let b = rig
            .catalog
            .fork(BranchId::TRUNK, LeaseDeadline(deadline))
            .map_err(|e| format!("fork sibling {k}: {e:?}"))?
            .branch_id;
        write_fresh(rig, b, k, p)?;
        if k == depth {
            survivor = Some(b);
        } else {
            doomed.push(b);
        }
    }
    let survivor = survivor.ok_or_else(|| "depth 0 builds no fanout".to_string())?;
    Ok(Built { doomed, survivor, bad_cows: 0 })
}

/// One reading of the store, split by owner. Both halves of every pair must agree (G2).
#[derive(Clone, Copy, Default)]
struct Census {
    /// `PageStore::live_page_count`, the store's net counter.
    counter_pages: u64,
    /// `allocated_pages(arena).len()` summed over `live_arenas()`: membership, not a net count.
    enum_pages: u64,
    reaped_pages: u64,
    survivor_pages: u64,
    /// Owned by neither the doomed set nor the survivor. Must be 0 (G5).
    other_pages: u64,
    /// `reserved_page_count`, the store's net counter.
    counter_reserved: u64,
    /// Sizes of every live extent, from `extent_range`.
    enum_reserved: u64,
    reaped_reserved: u64,
    reaped_extents: u64,
    live_extents: u64,
    /// `pending_len`: pages parked in the pending-free log.
    pending: u64,
}

/// `doomed` is the fixture's own list, not the reaper's answer, so the split cannot agree with
/// the reaper merely because it came from the reaper.
fn census(store: &ArenaPageStore, doomed: &BTreeSet<u64>, survivor: u64) -> Result<Census, String> {
    let mut c = Census {
        counter_pages: store.live_page_count().map_err(|e| format!("live_page_count: {e:?}"))?
            as u64,
        counter_reserved: store.reserved_page_count() as u64,
        pending: store.pending_len() as u64,
        ..Census::default()
    };
    for (arena, owner) in store.live_arenas() {
        let pages = store.allocated_pages(arena).len() as u64;
        // `None` would mean the extent vanished between the two reads. Counting it as 0 makes G2
        // fire rather than hiding it.
        let size = store.extent_range(arena).map(|(_, n)| n as u64).unwrap_or(0);
        c.live_extents += 1;
        c.enum_pages += pages;
        c.enum_reserved += size;
        if doomed.contains(&owner.id) {
            c.reaped_pages += pages;
            c.reaped_reserved += size;
            c.reaped_extents += 1;
        } else if owner.id == survivor {
            c.survivor_pages += pages;
        } else {
            c.other_pages += pages;
        }
    }
    Ok(c)
}

fn check_census(c: &Census, when: &str, fails: &mut Vec<String>) {
    if c.counter_pages != c.enum_pages {
        fails.push(format!(
            "G2 {when}: live_page_count says {} pages, the live extents enumerate {}",
            c.counter_pages, c.enum_pages
        ));
    }
    if c.counter_reserved != c.enum_reserved {
        fails.push(format!(
            "G2b {when}: reserved_page_count says {}, the live extents' sizes sum to {}",
            c.counter_reserved, c.enum_reserved
        ));
    }
    if c.other_pages != 0 {
        fails.push(format!(
            "G5 {when}: {} pages are owned by a branch this fixture did not make",
            c.other_pages
        ));
    }
}

/// Every pre-registered quantity for one cell. Measured and predicted values use this one struct,
/// so they print under the same keys and are compared field by field.
#[derive(Clone, Copy)]
struct Outcome {
    retained_pages: u64,
    retained_reserved: u64,
    retained_extents: u64,
    pending: u64,
    survivor_pages: u64,
    extra_drain: u64,
    orphans: u64,
    after_leaf_pages: u64,
    after_leaf_reserved: u64,
    after_leaf_extents: u64,
    after_leaf_pending: u64,
    after_leaf_drain: u64,
    after_leaf_orphans: u64,
}

impl Outcome {
    fn fields(&self) -> [(&'static str, u64); 13] {
        [
            ("retained_pages", self.retained_pages),
            ("retained_reserved", self.retained_reserved),
            ("retained_extents", self.retained_extents),
            ("pending", self.pending),
            ("survivor_pages", self.survivor_pages),
            ("extra_drain", self.extra_drain),
            ("orphans", self.orphans),
            ("after_leaf_pages", self.after_leaf_pages),
            ("after_leaf_reserved", self.after_leaf_reserved),
            ("after_leaf_extents", self.after_leaf_extents),
            ("after_leaf_pending", self.after_leaf_pending),
            ("after_leaf_drain", self.after_leaf_drain),
            ("after_leaf_orphans", self.after_leaf_orphans),
        ]
    }

    /// PREREG's prediction table: every interior page is retained under a live leaf in `chain`
    /// and `overwrite`, none in `fanout`, and nothing at all once the survivor goes.
    fn predicted(arm: Arm, depth: usize, p: usize) -> Outcome {
        let p = p as u64;
        let pinned_branches = match arm {
            Arm::Fanout => 0,
            Arm::Chain | Arm::Overwrite => (depth - 1) as u64,
        };
        let (extents, reserved) = extent_model(p);
        Outcome {
            retained_pages: p * pinned_branches,
            retained_reserved: reserved * pinned_branches,
            retained_extents: extents * pinned_branches,
            pending: p * pinned_branches,
            survivor_pages: p,
            extra_drain: 0,
            orphans: 0,
            after_leaf_pages: 0,
            after_leaf_reserved: 0,
            after_leaf_extents: 0,
            after_leaf_pending: 0,
            after_leaf_drain: 0,
            after_leaf_orphans: 0,
        }
    }
}

/// `(extents, reserved pages)` one branch holds after writing `pages` pages, by the pre-registered
/// doubling rule.
fn extent_model(pages: u64) -> (u64, u64) {
    let (mut extents, mut reserved, mut next) = (0u64, 0u64, MODEL_FIRST_EXTENT);
    while reserved < pages {
        extents += 1;
        reserved += next;
        next = (next * 2).min(MODEL_EXTENT_CAP);
    }
    (extents, reserved)
}

struct Cell {
    arm: Arm,
    depth: usize,
    measured: Outcome,
    written: u64,
    reaped: usize,
    total_after_sweep1: u64,
    /// Of the D forks made after sweep 2, how many were handed a slot id this fixture had used:
    /// the number of id slots the reaps released. PREREG amendment 1, A1.4. Not judged here.
    slots_recycled: usize,
    sweep1_ms: f64,
    sweep2_ms: f64,
    guard_failures: Vec<String>,
}

/// G7: a drain that returned early leaves its touched extents in `deferred`, for a later narrowed
/// sweep that production runs only on the lease thread's cadence. None may be left after a sweep
/// here.
fn check_deferred(reaper: &TwoTierReaper, when: &str, fails: &mut Vec<String>) {
    let left = reaper.deferred_len();
    if left != 0 {
        fails.push(format!(
            "G7 {when}: {left} arenas were left in the reaper's deferred set by a drain that did \
             not finish its sweep"
        ));
    }
}

/// One flushed line before each phase. A run killed by `timeout` then names the cell and phase it
/// stopped in, and its last finished row cannot be mistaken for the whole run.
fn progress(tag: &str, phase: &str) {
    println!("progress  {tag} phase={phase}");
    let _ = std::io::stdout().flush();
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

fn run_cell(arm: Arm, depth: usize, p: usize, persist: bool, tag: &str) -> Result<Cell, String> {
    let rig = rig(arm, depth, persist)?;
    let built = match arm {
        Arm::Fanout => build_fanout(&rig, depth, p)?,
        Arm::Chain => build_chain(&rig, depth, p, false)?,
        Arm::Overwrite => build_chain(&rig, depth, p, true)?,
    };
    let doomed: BTreeSet<u64> = built.doomed.iter().map(|b| b.id).collect();
    let survivor = built.survivor;
    let mut fails: Vec<String> = Vec::new();

    // ---- G1: the fixture fired ------------------------------------------------------------------
    let before = census(&rig.store, &doomed, survivor.id)?;
    check_census(&before, "before sweep 1", &mut fails);
    let want_written = (p * depth) as u64;
    if before.enum_pages != want_written {
        fails.push(format!(
            "G1: {} pages allocated before sweep 1, the fixture wrote P*D = {want_written}",
            before.enum_pages
        ));
    }
    let want_depth = match arm {
        Arm::Fanout => 1,
        Arm::Chain | Arm::Overwrite => depth as u32,
    };
    match rig.catalog.get(survivor) {
        Ok(r) if r.depth == want_depth => {}
        Ok(r) => fails.push(format!("G1b: survivor {survivor} has depth {}, not {want_depth}", r.depth)),
        Err(e) => fails.push(format!("G1b: survivor {survivor} unreadable before sweep 1: {e:?}")),
    }
    if built.bad_cows != 0 {
        fails.push(format!(
            "G6: {} cow_page calls were not a fresh private copy of an ancestor's page",
            built.bad_cows
        ));
    }

    // ---- sweep 1: every branch but the survivor ------------------------------------------------
    progress(tag, "sweep1");
    let t = Instant::now();
    let reaped1 = rig
        .reaper
        .reap_expired(DOOMED_DEADLINE)
        .map_err(|e| format!("sweep 1 reap_expired: {e:?}"))?;
    let sweep1_ms = ms(t);
    let got1: BTreeSet<u64> = reaped1.iter().map(|b| b.id).collect();
    if got1 != doomed || reaped1.len() != built.doomed.len() {
        fails.push(format!(
            "G3: sweep 1 reaped {} branches ({} distinct), expected exactly the {} non-survivors",
            reaped1.len(),
            got1.len(),
            built.doomed.len()
        ));
    }
    // The collector the lease thread runs after a sweep. Predicted to free nothing here: every
    // retained page is still pinned, so no extent is both ownerless and empty.
    let orphans1 = rig
        .reaper
        .collect_orphaned_extents()
        .map_err(|e| format!("collect_orphaned_extents after sweep 1: {e:?}"))?;
    check_deferred(&rig.reaper, "after sweep 1", &mut fails);

    // ---- G4: the survivor lives and the doomed are gone ----------------------------------------
    match rig.catalog.get(survivor) {
        Ok(r) if r.state == BranchState::Live => {}
        Ok(r) => fails.push(format!("G4b: survivor {survivor} reads {:?} after sweep 1", r.state)),
        Err(e) => fails.push(format!("G4b: survivor {survivor} unreadable after sweep 1: {e:?}")),
    }
    let not_reaped = built
        .doomed
        .iter()
        .filter(|b| !matches!(rig.catalog.get_raw(b.id), Ok(r) if r.state == BranchState::Reaped))
        .count();
    if not_reaped != 0 {
        fails.push(format!("G4: {not_reaped} doomed branches do not read Reaped after sweep 1"));
    }
    let after1 = census(&rig.store, &doomed, survivor.id)?;
    check_census(&after1, "after sweep 1", &mut fails);
    // The probe, AFTER the census: anything it releases was reclaimable and left parked by a reap.
    let drained1 = rig
        .reaper
        .drain_pending()
        .map_err(|e| format!("drain_pending probe after sweep 1: {e:?}"))?;
    check_deferred(&rig.reaper, "after the sweep-1 probe", &mut fails);

    // ---- sweep 2, control (a): the survivor goes too -------------------------------------------
    progress(tag, "sweep2");
    let t = Instant::now();
    let reaped2 = rig
        .reaper
        .reap_expired(SURVIVOR_DEADLINE)
        .map_err(|e| format!("sweep 2 reap_expired: {e:?}"))?;
    let sweep2_ms = ms(t);
    if reaped2.len() != 1 || reaped2[0].id != survivor.id {
        let got: Vec<String> = reaped2.iter().map(|b| b.to_string()).collect();
        fails.push(format!("G3: sweep 2 reaped [{}], expected only {survivor}", got.join(", ")));
    }
    let orphans2 = rig
        .reaper
        .collect_orphaned_extents()
        .map_err(|e| format!("collect_orphaned_extents after sweep 2: {e:?}"))?;
    check_deferred(&rig.reaper, "after sweep 2", &mut fails);
    let after2 = census(&rig.store, &doomed, survivor.id)?;
    check_census(&after2, "after sweep 2", &mut fails);
    let drained2 = rig
        .reaper
        .drain_pending()
        .map_err(|e| format!("drain_pending probe after sweep 2: {e:?}"))?;

    let refused = rig.reaper.refused_reaps();
    if refused != 0 {
        fails.push(format!("G3b: the reaper refused {refused} reaps"));
    }

    // ---- the id-slot probe (A1.4), after every census ------------------------------------------
    // `fork` hands out a released slot before it mints a new id, so of D fresh forks, the ones that
    // land on an id this fixture used are exactly the slots the reaps released. Nothing is counted
    // after this, so the forks cannot move a page number.
    progress(tag, "slots");
    let fixture_ids: BTreeSet<u64> = doomed.iter().copied().chain([survivor.id]).collect();
    let mut slots_recycled = 0usize;
    for k in 0..depth {
        match rig.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)) {
            Ok(r) if fixture_ids.contains(&r.branch_id.id) => slots_recycled += 1,
            Ok(_) => {}
            Err(e) => {
                fails.push(format!("G8: slot probe fork {k} failed after both sweeps: {e:?}"));
                break;
            }
        }
    }

    Ok(Cell {
        arm,
        depth,
        measured: Outcome {
            retained_pages: after1.reaped_pages,
            retained_reserved: after1.reaped_reserved,
            retained_extents: after1.reaped_extents,
            pending: after1.pending,
            survivor_pages: after1.survivor_pages,
            extra_drain: drained1 as u64,
            orphans: orphans1 as u64,
            after_leaf_pages: after2.enum_pages,
            after_leaf_reserved: after2.enum_reserved,
            after_leaf_extents: after2.live_extents,
            after_leaf_pending: after2.pending,
            after_leaf_drain: drained2 as u64,
            after_leaf_orphans: orphans2 as u64,
        },
        written: before.enum_pages,
        reaped: reaped1.len(),
        total_after_sweep1: after1.enum_pages,
        slots_recycled,
        sweep1_ms,
        sweep2_ms,
        guard_failures: fails,
    })
}

fn print_fields(label: &str, tag: &str, o: &Outcome) {
    let kv: Vec<String> = o.fields().iter().map(|(k, v)| format!("{k}={v}")).collect();
    println!("{label} {tag} {}", kv.join(" "));
}

/// Parse a comma list, REFUSING the whole run on any token it cannot read. Silently dropping a
/// token would run a smaller experiment under the requested one's name.
fn parse_list<T>(raw: &str, what: &str, parse: impl Fn(&str) -> Option<T>) -> Result<Vec<T>, String> {
    let mut out = Vec::new();
    for tok in raw.split(',') {
        let tok = tok.trim();
        match parse(tok) {
            Some(v) => out.push(v),
            None => return Err(format!("cannot parse {what} {tok:?} in {raw:?}")),
        }
    }
    Ok(out)
}

fn arg<'a>(args: &'a [String], i: usize, default: &'a str) -> &'a str {
    args.get(i).map(String::as_str).unwrap_or(default)
}

fn parse_args(args: &[String]) -> Result<(Vec<usize>, usize, Vec<Arm>), String> {
    let depths = parse_list(arg(args, 1, "1,10,100,1000"), "depth", |s| {
        s.parse::<usize>().ok().filter(|&d| d >= 1)
    })?;
    let raw_p = arg(args, 2, "2");
    let p = raw_p.trim().parse::<usize>().ok().filter(|&p| p >= 1).ok_or_else(|| {
        format!("cannot parse pages_per_level {raw_p:?}: it must be an integer >= 1, or nothing is written")
    })?;
    let arms = parse_list(arg(args, 3, "fanout,chain,overwrite"), "arm", Arm::parse)?;
    Ok((depths, p, arms))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let prov = ferrodb::build_provenance();
    let sha = prov.split_whitespace().nth(1).unwrap_or("unknown").to_string();
    // G0, RUN-LEVEL. A row that cannot name the code it ran is not attributable to anything, so the
    // run exits 2. The per-cell verdicts are still computed: a fire-check build is dirty by
    // construction, and its cells are what it exists to read. Every line's tag carries `build=`, so a
    // row copied out of this output still says what it was taken on.
    let build = if sha == "unknown" {
        "unknown"
    } else if prov.contains("+DIRTY") {
        "DIRTY"
    } else {
        "clean"
    };
    let provenance_failure = if build != "clean" {
        Some(format!("G0: build provenance is {prov:?}, so no row can be attributed to a commit"))
    } else {
        None
    };
    let (depths, p, arms) = match parse_args(&args) {
        Ok(v) => v,
        Err(e) => {
            println!("NOT A RESULT: {e}. Nothing was run.");
            std::process::exit(2);
        }
    };
    let persist = std::env::var("D16_PERSIST").map(|v| v != "0").unwrap_or(true);
    let arm_names: Vec<&str> = arms.iter().map(|a| a.name()).collect();
    let (model_extents, model_reserved) = extent_model(p as u64);

    println!("D16 chain retention: arena pages a reaped interior chain keeps allocated while its leaf lives.");
    println!("build {prov}");
    println!("pre-registration: bench/d16_chain/PREREG.md, committed before this harness");
    println!(
        "catalog: TableBranchCatalog on a sidecar file. Free-space map persistence: {}",
        if persist { "ON (as cli.rs ships it)" } else { "OFF (D16_PERSIST=0)" }
    );
    println!("reap: Reaper::reap_expired only, i.e. expired_candidates (deepest first) + reap_if_still_expired,");
    println!("  the halves lease_thread::scan_once runs. Every reap ends in its own drain. Each sweep is then");
    println!("  followed by collect_orphaned_extents (the lease thread's collector) and the census; a");
    println!("  drain_pending PROBE runs after the census, and whatever it releases is reported, not hidden.");
    println!("slots: after both sweeps, D forks off trunk; `slots_recycled` = how many reused a slot this");
    println!("  fixture had used, i.e. id slots released. Predicted per tree in PREREG A1.4, not judged here.");
    println!(
        "P = {p} pages per level. depths {depths:?}. arms [{}]. Extent model {MODEL_FIRST_EXTENT} doubling to \
         {MODEL_EXTENT_CAP}: {p} pages -> {model_extents} extents, {model_reserved} reserved pages per branch",
        arm_names.join(", ")
    );
    println!("instrument: live_page_count AND the allocated pages enumerated over live_arenas, which must agree;");
    println!("  reserved_page_count AND the live extents' sizes, which must agree");
    println!("per cell: `measured` and `predicted` print the same keys; `verdict` is MATCH, MISMATCH <keys>, or");
    println!("  NOT A RESULT <guards>; `context` is not pre-registered and its wall-clock ms are load-sensitive");
    if let Some(f) = &provenance_failure {
        println!("NOT A RESULT: {f}");
    }
    println!();

    let mut cells: Vec<Cell> = Vec::new();
    let mut not_a_result = 0usize;
    let mut mismatches = 0usize;
    for &depth in &depths {
        for &arm in &arms {
            let tag = format!("arm={} D={depth} P={p} sha={sha} build={build}", arm.name());
            progress(&tag, "build");
            let cell = match run_cell(arm, depth, p, persist, &tag) {
                Ok(c) => c,
                Err(e) => {
                    not_a_result += 1;
                    println!("verdict   {tag} NOT A RESULT: harness error: {e}");
                    continue;
                }
            };
            let want = Outcome::predicted(arm, depth, p);
            print_fields("measured ", &tag, &cell.measured);
            print_fields("predicted", &tag, &want);
            let got_fields = cell.measured.fields();
            let want_fields = want.fields();
            let wrong: Vec<&str> = got_fields
                .iter()
                .zip(want_fields.iter())
                .filter(|(g, w)| g.1 != w.1)
                .map(|(g, _)| g.0)
                .collect();
            let per_interior = if depth > 1 {
                format!("{:.3}", cell.measured.retained_pages as f64 / (depth - 1) as f64 / p as f64)
            } else {
                "n/a".to_string()
            };
            println!(
                "context   {tag} written={} reaped={} total_pages_after_sweep1={} \
                 retained_pages_per_reaped_branch_per_page={per_interior} retained_reserved_bytes={} \
                 sweep1_ms={:.1} sweep2_ms={:.1}",
                cell.written,
                cell.reaped,
                cell.total_after_sweep1,
                cell.measured.retained_reserved * PAGE_SIZE as u64,
                cell.sweep1_ms,
                cell.sweep2_ms,
            );
            println!(
                "slots     {tag} slots_recycled={} of fixture_ids={depth} (A1.4: main lineage predicts \
                 chain/overwrite 1, fanout D; D200 predicts D in every arm)",
                cell.slots_recycled
            );
            // The cell's own verdict, from G1-G8 and the predictions. G0 is appended, never
            // substituted, so a fire-check can still read what the cell said.
            let run_level = match &provenance_failure {
                Some(f) => format!(" [run is NOT A RESULT: {f}]"),
                None => String::new(),
            };
            if !cell.guard_failures.is_empty() {
                not_a_result += 1;
                println!(
                    "verdict   {tag} NOT A RESULT: {}{run_level}",
                    cell.guard_failures.join(" | ")
                );
            } else if !wrong.is_empty() {
                mismatches += 1;
                println!("verdict   {tag} MISMATCH {}{run_level}", wrong.join(","));
            } else {
                println!("verdict   {tag} MATCH{run_level}");
            }
            cells.push(cell);
        }
    }

    println!();
    // The slope the brief asked for, over the clean cells only. It is the same quantity as the
    // per-cell `retained_pages_per_reaped_branch_per_page`, taken across the depth axis, so a
    // one-depth run still reports it cell by cell.
    for &arm in &arms {
        let mut pts: Vec<(usize, u64)> = cells
            .iter()
            .filter(|c| c.arm == arm && c.guard_failures.is_empty())
            .map(|c| (c.depth, c.measured.retained_pages))
            .collect();
        pts.sort_unstable();
        let pre_registered = match arm {
            Arm::Fanout => "0",
            Arm::Chain | Arm::Overwrite => "1",
        };
        match (pts.first(), pts.last()) {
            (Some(&(d0, r0)), Some(&(d1, r1))) if d1 > d0 => {
                let slope = (r1 as f64 - r0 as f64) / (d1 - d0) as f64 / p as f64;
                println!(
                    "slope     arm={} P={p} sha={sha} retained pages per level per page written = {slope:.4} \
                     over D={d0}..{d1} (pre-registered {pre_registered})",
                    arm.name()
                );
            }
            _ => println!(
                "slope     arm={} P={p} sha={sha} needs two clean depths; see each context line",
                arm.name()
            ),
        }
    }
    // Pre-registered: `overwrite` retains exactly what `chain` does, because the interval rule has
    // no input saying a child superseded a page. A difference means it can see that after all.
    for &depth in &depths {
        let clean = |a: Arm| {
            cells.iter().find(|c| c.arm == a && c.depth == depth && c.guard_failures.is_empty())
        };
        if let (Some(ch), Some(ow)) = (clean(Arm::Chain), clean(Arm::Overwrite)) {
            let ch_fields = ch.measured.fields();
            let ow_fields = ow.measured.fields();
            let differ: Vec<&str> = ch_fields
                .iter()
                .zip(ow_fields.iter())
                .filter(|(a, b)| a.1 != b.1)
                .map(|(a, _)| a.0)
                .collect();
            if differ.is_empty() {
                println!("compare   D={depth} P={p} sha={sha} overwrite == chain on every key (pre-registered: equal)");
            } else {
                mismatches += 1;
                println!(
                    "compare   D={depth} P={p} sha={sha} MISMATCH overwrite != chain on {} (pre-registered: \
                     equal; the rule distinguished superseded pages)",
                    differ.join(",")
                );
            }
        }
    }

    let planned = depths.len() * arms.len();
    if let Some(f) = &provenance_failure {
        println!(
            "NOT A RESULT: {f}. The {planned} cell verdicts above ({not_a_result} guard or harness \
             failures, {mismatches} mismatches) describe unattributable code. Exit 2."
        );
        std::process::exit(2);
    }
    if not_a_result > 0 || cells.is_empty() {
        println!("NOT A RESULT: {not_a_result} of {planned} cells failed a guard or the harness. Exit 2.");
        std::process::exit(2);
    }
    if mismatches > 0 {
        println!(
            "RESULT WITH MISMATCH: all {planned} cells held their guards; {mismatches} pre-registered \
             predictions failed. Read PREREG, 'What each other outcome would mean'. Exit 1."
        );
        std::process::exit(1);
    }
    println!("RESULT: all {planned} cells held their guards and matched the pre-registration. Exit 0.");
}
