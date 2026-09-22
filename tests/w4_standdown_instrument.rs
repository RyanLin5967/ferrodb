//! W4 check 3 — the instrument's own fire-check.
//!
//! The number this instrument produces is a stand-down FRACTION, and the dangerous answer is a
//! small one: "W4 survives, stand-downs are rare" is exactly what a counter that never increments
//! also says. So the counter is not trusted until it has been made to fire on purpose, and shown
//! not to fire when the condition is absent.
//!
//! Four things are pinned here:
//!
//! 1. **`None` and `Some(0)` are distinguishable**, in both builds. Without the feature
//!    `counts()` must answer `None`; with it, `Some`. A harness that could not tell those apart
//!    would report "never stood down" for a build that was not measuring.
//! 2. **The stand-down counter fires** when a writer is announced.
//! 3. **It does not fire spuriously** — the same call with no writer announced increments
//!    `admitted` and leaves `stood_down` alone.
//! 4. **`announce_unattributed` is a live detector**, not a field that is zero by construction:
//!    an acquirer with no site tag makes it move.
//!
//! The counters are process-global statics, so every test here takes `GATE` and asserts on a
//! DELTA rather than an absolute. Deltas also survive another test in this binary touching a
//! `ServerContext`, which absolute values would not.

use std::fs::OpenOptions;
use std::sync::{Arc, Mutex, MutexGuard};

#[cfg(feature = "w4-standdown-count")]
use std::sync::atomic::AtomicBool;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::pgwire::standdown;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

static GATE: Mutex<()> = Mutex::new(());

fn gate() -> MutexGuard<'static, ()> {
    match GATE.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

// Only the feature-gated tests below build a context; without the feature this file is one
// assertion about `counts()` answering `None`, and that needs no engine.
#[cfg_attr(not(feature = "w4-standdown-count"), allow(dead_code))]
fn ctx(dir: &tempfile::TempDir, name: &str) -> Arc<ServerContext> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(name))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{name}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    Arc::new(ServerContext::new(catalog, bp, txn, Arc::new(AgentRuntime::new())))
}

/// Whether this build carries the counters at all — and the whole reason `counts()` is an
/// `Option`. Asserted in BOTH directions so neither build can quietly answer the other's answer.
#[test]
fn not_instrumented_is_a_different_fact_from_zero() {
    let _g = gate();
    let seen = standdown::counts();
    if cfg!(feature = "w4-standdown-count") {
        assert!(
            seen.is_some(),
            "built with `w4-standdown-count` and `counts()` still answered None: the harness \
             would refuse a build that is in fact measuring"
        );
    } else {
        assert!(
            seen.is_none(),
            "built WITHOUT `w4-standdown-count` and `counts()` answered {seen:?}: an \
             uninstrumented build must not be able to report a zero, because a harness would \
             read that zero as 'never stood down'"
        );
    }
}

#[cfg(feature = "w4-standdown-count")]
#[test]
fn the_stand_down_counter_fires_when_a_writer_is_announced() {
    let _g = gate();
    let dir = tempfile::tempdir().unwrap();
    let c = ctx(&dir, "fires.db");
    let slot = Arc::new(AtomicBool::new(false));
    c.register_reader(Arc::clone(&slot));

    // Force the condition: hold the exclusive catalog, which announces a writer.
    let guard = c.catalog();
    let before = standdown::counts().expect("instrumented");
    assert!(
        c.begin_read(&slot).is_none(),
        "the premise of this test did not hold: begin_read admitted while a writer was announced"
    );
    let after = standdown::counts().expect("instrumented");
    drop(guard);

    assert_eq!(
        after.stood_down - before.stood_down,
        1,
        "a forced stand-down did not move the stand-down counter, so a zero from this instrument \
         means nothing"
    );
    assert_eq!(
        after.admitted - before.admitted,
        0,
        "a stand-down also counted as an admission; the two arms are not exclusive"
    );
}

#[cfg(feature = "w4-standdown-count")]
#[test]
fn the_counter_does_not_fire_when_no_writer_is_announced() {
    let _g = gate();
    let dir = tempfile::tempdir().unwrap();
    let c = ctx(&dir, "quiet.db");
    let slot = Arc::new(AtomicBool::new(false));
    c.register_reader(Arc::clone(&slot));

    let before = standdown::counts().expect("instrumented");
    let pass = c.begin_read(&slot).expect("nothing is announced, so this must be admitted");
    let after = standdown::counts().expect("instrumented");
    drop(pass);

    assert_eq!(
        after.admitted - before.admitted,
        1,
        "an admitted read did not move the admitted counter, so the DENOMINATOR of the reported \
         fraction is not what it claims"
    );
    assert_eq!(
        after.stood_down - before.stood_down,
        0,
        "the stand-down counter moved with no writer announced: it fires spuriously and any \
         fraction it produces is an artefact"
    );
}

/// `announce_unattributed` exists so a breakdown cannot silently omit an acquirer. That is only
/// worth printing if it can move, so here it is made to move: `ctx.catalog()` called directly is
/// exactly an acquirer with no site tag.
#[cfg(feature = "w4-standdown-count")]
#[test]
fn an_untagged_acquirer_shows_up_as_unattributed() {
    let _g = gate();
    let dir = tempfile::tempdir().unwrap();
    let c = ctx(&dir, "untagged.db");

    let before = standdown::counts().expect("instrumented");
    drop(c.catalog());
    let after = standdown::counts().expect("instrumented");

    assert_eq!(
        after.announce_total - before.announce_total,
        1,
        "an exclusive acquisition did not reach the total counter, so `announce_total` is not a \
         total and the unattributed figure is meaningless"
    );
    assert_eq!(
        after.announce_unattributed() - before.announce_unattributed(),
        1,
        "an acquirer with no site tag did not raise `announce_unattributed`: the detector that \
         is supposed to catch an acquirer nobody counted cannot fire"
    );
}

/// **The `LeaseScan` tag fires — and knowing that is what makes `ann_lease=0` readable.**
///
/// Every row of the W4 check 3 main run reports `ann_lease=0`, and a zero from a detector that
/// has never been made to fire says nothing at all. It is worth separating two claims that the
/// bare zero collapses:
///
/// * *the tag works* — asserted here, by calling the reaper's own `RuntimeLock` seam and watching
///   `announce_lease_scan` move by exactly one;
/// * *the reaper announced nothing during an arm* — which is TRUE but is true BY CONSTRUCTION and
///   therefore measures nothing. `LeaseScan` is tagged in `impl RuntimeLock for ServerContext`,
///   which the lease thread reaches only from `scan_once`'s per-candidate reap loop —
///   `expired_candidates` is computed OUTSIDE the lock and an empty candidate list takes the lock
///   zero times, at any scan interval. A branch expires `DEFAULT_LEASE_MILLIS` after it is
///   forked, hardcoded at 15 minutes (`src/agent_sql/runtime.rs`, `LeaseDeadline::from_now`), and
///   no arm runs for 15 minutes.
///
/// ⇒ So `W4_LEASE_MS` moves the scan CADENCE and cannot make the reaper announce; only an EXPIRED
/// BRANCH can. A fire-check that lowers the interval and expects `ann_lease` to become non-zero
/// tests the wrong knob. The reaper's contribution to the announcer set is UNMEASURED by that
/// harness, and this test is the boundary of what is known: the tag is live, the population is
/// empty.
#[cfg(feature = "w4-standdown-count")]
#[test]
fn the_lease_scan_tag_fires_when_the_reaper_takes_the_statement_lock() {
    use ferrodb::branch::lease_thread::RuntimeLock;

    let _g = gate();
    let dir = tempfile::tempdir().unwrap();
    let c = ctx(&dir, "leasescan.db");

    let before = standdown::counts().expect("instrumented");
    let mut ran = false;
    let mut body = || ran = true;
    // The exact seam the lease thread uses; nothing here is a stand-in for it.
    RuntimeLock::with_runtime_lock(&*c, &mut body);
    let after = standdown::counts().expect("instrumented");

    assert!(ran, "with_runtime_lock returned without running its body");
    assert_eq!(
        after.announce_lease_scan - before.announce_lease_scan,
        1,
        "the reaper took the statement lock and `announce_lease_scan` did not move, so every \
         `ann_lease=0` in the main run is a dead counter rather than an absent announcer"
    );
    assert_eq!(
        after.announce_total - before.announce_total,
        1,
        "the reaper's acquisition did not reach `announce_total`"
    );
    assert_eq!(
        after.announce_unattributed() - before.announce_unattributed(),
        0,
        "the reaper's acquisition was tagged, so it must NOT also count as unattributed"
    );
}

/// The does-not-fire-spuriously half of the test above: an acquisition that is NOT the reaper's
/// must leave `announce_lease_scan` alone. Without this the test above passes for a counter that
/// increments on every acquisition and merely happens to be named after the reaper.
#[cfg(feature = "w4-standdown-count")]
#[test]
fn the_lease_scan_tag_does_not_fire_for_an_ordinary_acquirer() {
    let _g = gate();
    let dir = tempfile::tempdir().unwrap();
    let c = ctx(&dir, "leasescan_neg.db");

    let before = standdown::counts().expect("instrumented");
    drop(c.catalog());
    let after = standdown::counts().expect("instrumented");

    assert_eq!(
        after.announce_total - before.announce_total,
        1,
        "the acquisition did not reach the total counter"
    );
    assert_eq!(
        after.announce_lease_scan - before.announce_lease_scan,
        0,
        "an acquisition with no reaper behind it moved the LeaseScan tag, so that column does not \
         mean what its name says and a reaper-caused stand-down could not be told from any other"
    );
}
