//! E8 — base backup, proved on the exact scenario that defeated E4.
//!
//! E4's end-to-end test converges at 40 rows. It does so for a reason that does not generalise:
//! 40 rows is under the checkpoint threshold, so the primary's WAL still begins at LSN 0 and a
//! replica starting with an empty file has missed nothing. At 2000 rows the primary checkpoints
//! (every 256 commits) and **truncates**, and the same replica dies applying a record to a page
//! that has no earlier state to apply it to.
//!
//! This file runs at 2000 rows deliberately, and asserts the truncation actually happened before
//! claiming anything — otherwise the whole test would silently degrade into a second copy of E4.
//!
//! Three claims, in order:
//!
//! 1. The primary really did truncate (anti-vacuity: without this the rest proves nothing).
//! 2. **Without** a base backup, a replica cannot start. This is E4's failure, pinned so that it
//!    stays understood rather than being rediscovered.
//! 3. **With** one, the replica converges, judged by comparing page bytes on disk.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

const PAGE_SIZE: usize = 4096;

/// Refuse to run against a stale example binary. `cargo test` does not rebuild examples, so
/// without this a test that spawns one can silently exercise a build from before the change under
/// test — which is exactly how E4's first fire-check passed while the defect was live.
fn assert_example_is_fresh(bin: &Path) {
    let bin_time = std::fs::metadata(bin)
        .unwrap_or_else(|e| panic!("{} is missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .expect("mtime");
    if let Some(src_time) = walk_newest(Path::new("src")) {
        assert!(
            bin_time >= src_time,
            "{} is older than src/ — cargo test does not rebuild examples, so this would test a \
             stale binary. Run: cargo build --examples",
            bin.display()
        );
    }
}

fn walk_newest(dir: &Path) -> Option<std::time::SystemTime> {
    let mut newest = None;
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
        let t = if p.is_dir() {
            walk_newest(&p)
        } else {
            std::fs::metadata(&p).ok().and_then(|m| m.modified().ok())
        };
        if let Some(t) = t {
            newest = Some(match newest {
                Some(cur) if cur > t => cur,
                _ => t,
            });
        }
    }
    newest
}

fn example_bin(name: &str) -> PathBuf {
    let mut p = std::env::current_exe().expect("test exe");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    let out = p.join("examples").join(name);
    assert_example_is_fresh(&out);
    out
}

struct Primary {
    child: Child,
    /// Kept open so a test can go on reading the primary's stdout after startup — E6's `SYNC_OK`
    /// arrives long after the readiness lines do.
    lines: std::io::Lines<BufReader<std::process::ChildStdout>>,
    addr: String,
    start_lsn: u64,
    durable_lsn: u64,
    backup_dir: String,
    backup_start: u64,
    backup_end: u64,
}

impl Drop for Primary {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_primary(db: &Path, rows: u32, hot: bool) -> Primary {
    start_primary_mode(db, rows, if hot { Some("hot") } else { None }, &[])
}

fn start_primary_mode(
    db: &Path,
    rows: u32,
    mode: Option<&str>,
    env: &[(&str, &str)],
) -> Primary {
    let mut cmd = Command::new(example_bin("repl_primary"));
    cmd.arg(db).arg("127.0.0.1:0").arg(rows.to_string());
    if let Some(m) = mode {
        cmd.arg(m);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn primary");

    let stdout = child.stdout.take().expect("piped");
    let mut lines = BufReader::new(stdout).lines();
    let (mut addr, mut start, mut durable) = (None, None, None);
    let (mut bdir, mut bstart, mut bend) = (None, None, None);
    while addr.is_none()
        || start.is_none()
        || durable.is_none()
        || bdir.is_none()
        || bstart.is_none()
        || bend.is_none()
    {
        match lines.next() {
            Some(Ok(l)) if l.starts_with("LISTENING ") => {
                addr = Some(l.trim_start_matches("LISTENING ").to_string())
            }
            Some(Ok(l)) if l.starts_with("START ") => {
                start = l.trim_start_matches("START ").parse().ok()
            }
            Some(Ok(l)) if l.starts_with("DURABLE ") => {
                durable = l.trim_start_matches("DURABLE ").parse().ok()
            }
            Some(Ok(l)) if l.starts_with("BACKUP_START ") => {
                bstart = l.trim_start_matches("BACKUP_START ").parse().ok()
            }
            Some(Ok(l)) if l.starts_with("BACKUP_END ") => {
                bend = l.trim_start_matches("BACKUP_END ").parse().ok()
            }
            Some(Ok(l)) if l.starts_with("BACKUP ") => {
                bdir = Some(l.trim_start_matches("BACKUP ").to_string())
            }
            Some(Ok(_)) => continue,
            _ => panic!("primary exited before announcing readiness"),
        }
    }
    Primary {
        child,
        lines,
        addr: addr.unwrap(),
        start_lsn: start.unwrap(),
        durable_lsn: durable.unwrap(),
        backup_dir: bdir.unwrap(),
        backup_start: bstart.unwrap(),
        backup_end: bend.unwrap(),
    }
}

/// Restore a backup and follow the primary to its frontier. Returns the replica's stdout.
fn catch_up(primary: &Primary, rdb: &Path) -> String {
    let out = Command::new(example_bin("repl_replica"))
        .arg(rdb)
        .arg(&primary.addr)
        .arg("0") // deliberately wrong; the backup label must override it
        .arg(&primary.backup_dir)
        .output()
        .expect("spawn restored replica");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        out.status.success(),
        "a replica restored from a base backup failed:\nstdout: {stdout}\nstderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

/// Compare every page of two database files. Returns the differing page ids.
fn differing_pages(a: &Path, b: &Path) -> (usize, Vec<usize>) {
    let p = std::fs::read(a).expect("read a");
    let r = std::fs::read(b).expect("read b");
    let pages = p.len() / PAGE_SIZE;
    let mut differing = Vec::new();
    for id in 0..pages {
        match (page(&p, id), page(&r, id)) {
            (Some(pp), Some(rp)) if pp == rp => {}
            _ => differing.push(id),
        }
    }
    (pages, differing)
}

fn page(bytes: &[u8], id: usize) -> Option<&[u8]> {
    bytes.get(id * PAGE_SIZE..(id + 1) * PAGE_SIZE)
}

/// 2000 rows: past the checkpoint threshold, so the primary truncates and E4's approach fails.
const ROWS: u32 = 2000;

#[test]
fn a_replica_restored_from_a_base_backup_converges_past_a_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let pdb = dir.path().join("primary.db");
    let primary = start_primary(&pdb, ROWS, false);

    // (1) Anti-vacuity. If the primary has NOT truncated, this test is a duplicate of E4's and
    // proves nothing about base backups, so it must fail rather than pass quietly.
    assert!(
        primary.start_lsn > 0,
        "the primary's WAL still begins at LSN 0, so it never checkpointed at {ROWS} rows — this \
         test is not exercising the truncation it exists for. Raise ROWS or check \
         CHECKPOINT_INTERVAL."
    );
    assert!(
        primary.backup_start >= primary.start_lsn,
        "the backup starts at {} which is below the log base {}; it was unusable the moment it \
         was taken",
        primary.backup_start,
        primary.start_lsn
    );

    // (2) Without a base backup, from the log's base: this is E4's failure, and it must still fail.
    let bare = Command::new(example_bin("repl_replica"))
        .arg(dir.path().join("bare.db"))
        .arg(&primary.addr)
        .arg(primary.start_lsn.to_string())
        .output()
        .expect("spawn bare replica");
    assert!(
        !bare.status.success(),
        "a replica with no base backup started successfully against a TRUNCATED log. Either the \
         primary stopped truncating or the applier stopped checking — both change what E8 claims.\n\
         stdout: {}",
        String::from_utf8_lossy(&bare.stdout)
    );

    // (3) With one, it converges.
    let rdb = dir.path().join("replica.db");
    let stdout = catch_up(&primary, &rdb);
    assert!(stdout.contains("RESTORED "), "the replica did not restore a backup: {stdout}");

    let applied: u64 = stdout
        .lines()
        .find(|l| l.starts_with("APPLIED "))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no APPLIED line: {stdout}"));
    assert_eq!(
        applied, primary.durable_lsn,
        "the replica stopped short of the primary's durable frontier"
    );

    // The part that actually decides it: bytes on disk, not either process's opinion.
    let (pages, differing) = differing_pages(&pdb, &rdb);
    assert!(pages >= 2, "the primary is only {pages} page(s), so a byte comparison is near-vacuous");
    assert!(
        differing.is_empty(),
        "{} of {pages} pages differ between primary and replica after a restore + catch-up: {:?}",
        differing.len(),
        &differing[..differing.len().min(12)]
    );
}

/// **A backup taken while the primary is still writing**, and the boundary it exposes.
///
/// This is the one that tests the design. A quiescent backup has an empty `[start_lsn, end_lsn]`
/// window, so it proves the base image is copied correctly and proves nothing about the window —
/// and the window is the whole reason the copy is allowed to run without stopping the world.
///
/// Here the copy runs on a thread while `INSERT`s continue, so pages genuinely are captured at
/// different points in the log: an early page is missing changes, a late page already has them. If
/// replaying `[start_lsn, ..]` did not repair the first group, or were not idempotent on the
/// second, the comparison below would not hold.
///
/// # What this measured, which is not what it was first written to assert
///
/// The first version of this test compared the WHOLE file and failed on 14 of 35 pages. That was
/// the test being wrong, not the code: splitting the failures by whether the WAL describes the page
/// gave `DIFF_DESCRIBED []` and `DIFF_UNDESCRIBED [0,1,2,3,9,10,13,16,...]`. Every page physical
/// replication can carry converged exactly; every page that diverged was one it provably cannot.
///
/// So this asserts both halves, because each is worthless without the other:
///
/// - Every WAL-described page converges — the redo-idempotence argument the module rests on.
/// - Pages outside the WAL do *not*, and are frozen at backup time.
///
/// **The consequence, stated plainly: a hot backup does not by itself give a usable replica in
/// this engine.** It carries the catalog and the heap directory only as of the instant it ran, and
/// nothing afterwards updates them. The cold backup taken after the work is the usable one, which
/// is what the other test in this file exercises. This is E4's "the catalog is not replicated"
/// limit, and the measurement widens it: it is not only the catalog, it is every page written
/// outside the log.
#[test]
fn a_backup_taken_while_the_primary_is_writing_still_converges() {
    let dir = tempfile::tempdir().unwrap();
    let pdb = dir.path().join("primary.db");
    let primary = start_primary(&pdb, ROWS, true);

    // Anti-vacuity: if nothing was written during the copy this is just the quiescent test again.
    assert!(
        primary.backup_end > primary.backup_start,
        "the backup window is empty ({} .. {}), so no write raced the copy and this test is a \
         duplicate of the quiescent one",
        primary.backup_start,
        primary.backup_end
    );

    let rdb = dir.path().join("replica.db");
    let stdout = catch_up(&primary, &rdb);
    let applied: u64 = stdout
        .lines()
        .find(|l| l.starts_with("APPLIED "))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no APPLIED line: {stdout}"));
    assert_eq!(applied, primary.durable_lsn, "the replica stopped short of the frontier");

    let described = pages_described_by_wal(&dir.path().join("primary.db.wal"));
    assert!(!described.is_empty(), "the WAL describes no pages, so convergence would be vacuous");

    let (pages, differing) = differing_pages(&pdb, &rdb);
    assert!(pages >= 2, "only {pages} page(s); the comparison would be near-vacuous");

    let (described_diff, undescribed_diff): (Vec<usize>, Vec<usize>) =
        differing.iter().partition(|id| described.contains(id));

    // The design claim: everything replication can carry, it carried.
    assert!(
        described_diff.is_empty(),
        "{} WAL-described page(s) differ after a HOT backup + catch-up, window {}..{}: {:?}. \
         These are pages redo was supposed to repair, so this is a real divergence.",
        described_diff.len(),
        primary.backup_start,
        primary.backup_end,
        &described_diff[..described_diff.len().min(12)]
    );

    // The limit, pinned so it cannot quietly disappear. If this ever fires, pages outside the WAL
    // are somehow reaching the replica, and the limitation documented here, in E4's test, in
    // README, DEMO and ARCHITECTURE is stale and must be rewritten together.
    assert!(
        !undescribed_diff.is_empty(),
        "every page converged, including ones the WAL never describes. A hot backup is documented \
         as freezing those at backup time — if that is no longer true the docs are wrong."
    );
}

/// Pages the primary's WAL actually describes — the only ones physical replication can carry.
fn pages_described_by_wal(wal_path: &Path) -> std::collections::BTreeSet<usize> {
    use ferrodb::wal::log::{RecKind, WalManager};
    use std::sync::atomic::Ordering;

    let wal = WalManager::new(wal_path.to_path_buf()).expect("open wal");
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    let mut pages = std::collections::BTreeSet::new();
    while lsn < end {
        let Ok((rec, next)) = wal.read_record(lsn) else { break };
        if let RecKind::HeapInsert { page_id, .. }
        | RecKind::HeapDelete { page_id, .. }
        | RecKind::HeapUpdate { page_id, .. } = &rec.kind
        {
            pages.insert(*page_id as usize);
        }
        lsn = next;
    }
    pages
}

/// The base backup carries the catalog, because it copies data pages rather than WAL records.
///
/// This is a real difference from log shipping alone, where the catalog is written outside the WAL
/// and therefore never reaches the replica. It is asserted so that the two claims cannot drift
/// apart: E4's test pins "the catalog is not replicated by the WAL", and this pins "the base
/// backup is how it gets there instead".
#[test]
fn the_base_backup_carries_pages_the_wal_never_describes() {
    use ferrodb::wal::log::{RecKind, WalManager};
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let pdb = dir.path().join("primary.db");
    let primary = start_primary(&pdb, 300, false);

    let wal = WalManager::new(dir.path().join("primary.db.wal")).expect("open wal");
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    let mut described = std::collections::BTreeSet::new();
    while lsn < end {
        let Ok((rec, next)) = wal.read_record(lsn) else { break };
        if let RecKind::HeapInsert { page_id, .. }
        | RecKind::HeapDelete { page_id, .. }
        | RecKind::HeapUpdate { page_id, .. } = &rec.kind
        {
            described.insert(*page_id as usize);
        }
        lsn = next;
    }

    let image = std::fs::read(Path::new(&primary.backup_dir).join("base.db")).expect("read image");
    let p = std::fs::read(&pdb).expect("read primary");
    let pages = (p.len() / PAGE_SIZE).min(image.len() / PAGE_SIZE);

    let mut carried_but_undescribed = 0;
    for id in 1..pages {
        if described.contains(&id) {
            continue;
        }
        let (Some(pp), Some(ip)) = (page(&p, id), page(&image, id)) else { continue };
        if pp == ip && pp.iter().any(|b| *b != 0) {
            carried_but_undescribed += 1;
        }
    }
    assert!(
        carried_but_undescribed > 0,
        "every non-empty page in the backup was also described by the surviving WAL, so this test \
         cannot tell the two mechanisms apart"
    );
}

/// **E5 — kill the replica mid-stream, restart it, and it must resume rather than restart.**
///
/// The first replica is aborted at a known point (`FERRODB_REPLICA_ABORT_AFTER_BATCHES`) rather
/// than by racing it, so the midpoint is deterministic and the test cannot pass by accidentally
/// finishing before the kill. The second is given **no backup directory at all**: if it did not
/// genuinely resume from the state the first one left, it would start from LSN 0 against a log
/// whose base has moved and be refused.
///
/// The ordering being tested is the replica's half of the durability rule: progress is recorded
/// only after the pages it describes are durable, so a crash leaves the state file behind the
/// pages and never ahead. Behind is repaired by idempotent redo; ahead would be a replica claiming
/// an LSN whose pages never reached disk.
///
/// It uses the **hot** backup, and that is not a detail — the first attempt used the cold one and
/// its own anti-vacuity guard refused it: a backup taken after the work ends AT the frontier
/// (`start 364001 end 364001`), so there was nothing left to stream, no batches, and nothing a
/// kill could interrupt. Only a backup taken early leaves a stream to be interrupted.
#[test]
fn a_replica_killed_mid_stream_resumes_from_its_own_lsn_and_converges() {
    let dir = tempfile::tempdir().unwrap();
    let pdb = dir.path().join("primary.db");
    let rdb = dir.path().join("replica.db");
    let primary = start_primary(&pdb, ROWS, true);

    // First run: restore the backup, apply two batches, then die like a killed process.
    let first = Command::new(example_bin("repl_replica"))
        .arg(&rdb)
        .arg(&primary.addr)
        .arg("0")
        .arg(&primary.backup_dir)
        .env("FERRODB_REPLICA_ABORT_AFTER_BATCHES", "2")
        .output()
        .expect("spawn replica");
    let first_out = String::from_utf8_lossy(&first.stdout).to_string();
    assert!(
        !first.status.success(),
        "the replica was asked to abort after 2 batches and exited cleanly instead, so nothing was \
         interrupted: {first_out}"
    );
    assert!(first_out.contains("ABORTING"), "it did not reach the abort point: {first_out}");

    // Anti-vacuity: it must have got PART of the way. Neither 'nowhere' nor 'all the way' tests a
    // resume.
    let state_path = format!("{}.replstate", rdb.display());
    let recorded: u64 = std::fs::read_to_string(&state_path)
        .expect("the replica recorded no progress at all, so there is nothing to resume from")
        .trim()
        .parse()
        .expect("replstate is not a number");
    assert!(
        recorded > primary.backup_start,
        "the replica recorded {recorded}, which is no further than the backup it started from ({})",
        primary.backup_start
    );
    assert!(
        recorded < primary.durable_lsn,
        "the replica recorded {recorded}, which is already the primary's frontier ({}) — it \
         finished before the kill, so this is not a mid-stream restart",
        primary.durable_lsn
    );

    // Second run: NO backup directory. It must pick up from the state file.
    let second = Command::new(example_bin("repl_replica"))
        .arg(&rdb)
        .arg(&primary.addr)
        .arg("0")
        .output()
        .expect("respawn replica");
    let out = String::from_utf8_lossy(&second.stdout).to_string();
    assert!(
        second.status.success(),
        "the restarted replica failed:\nstdout: {out}\nstderr: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(
        out.contains(&format!("RESUMED {recorded}")),
        "the replica did not resume from {recorded}: {out}"
    );

    let applied: u64 = out
        .lines()
        .find(|l| l.starts_with("APPLIED "))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no APPLIED line: {out}"));
    assert_eq!(applied, primary.durable_lsn, "the resumed replica did not reach the frontier");

    // And it is byte-identical where physical replication can make it so, judged from disk.
    // The standard is the WAL-described pages, for the reason measured in the hot-backup test
    // above: pages written outside the log are frozen at backup time and no amount of streaming
    // moves them.
    let described = pages_described_by_wal(&dir.path().join("primary.db.wal"));
    assert!(!described.is_empty(), "the WAL describes no pages; convergence would be vacuous");
    let (pages, differing) = differing_pages(&pdb, &rdb);
    assert!(pages >= 2, "only {pages} page(s); near-vacuous");
    let described_diff: Vec<usize> =
        differing.into_iter().filter(|id| described.contains(id)).collect();
    assert!(
        described_diff.is_empty(),
        "{} WAL-described page(s) differ after a mid-stream kill and resume: {:?}",
        described_diff.len(),
        &described_diff[..described_diff.len().min(12)]
    );
}

/// **E6 — synchronous commit: the primary waits for a replica before calling it done.**
///
/// Two halves, and the second is the one that matters. A synchronous-commit implementation that
/// only ever gets tested with a healthy replica has not been tested at all — the interesting
/// question is what it does when the promise *cannot* be kept, and the wrong answers (block
/// forever, or quietly commit anyway and report success) are both easy to ship by accident.
#[test]
fn synchronous_commit_waits_for_a_replica_and_says_so_when_there_is_none() {
    // Half one: with a replica, the wait is satisfied.
    let dir = tempfile::tempdir().unwrap();
    let pdb = dir.path().join("primary.db");
    let mut primary = start_primary_mode(&pdb, 300, Some("sync"), &[("FERRODB_SYNC_TIMEOUT_SECS", "30")]);
    let durable = primary.durable_lsn;

    let rdb = dir.path().join("replica.db");
    let replica_out = catch_up(&primary, &rdb);
    assert!(replica_out.contains("APPLIED "), "the replica never caught up: {replica_out}");

    let verdict = primary
        .lines
        .by_ref()
        .filter_map(|l| l.ok())
        .find(|l| l.starts_with("SYNC_OK") || l.starts_with("SYNC_TIMEOUT"))
        .expect("the primary never resolved its synchronous commit");
    assert!(
        verdict.starts_with("SYNC_OK"),
        "a replica caught up to the frontier and the commit still was not acknowledged: {verdict}"
    );
    assert!(
        verdict.contains(&durable.to_string()),
        "the primary acknowledged a different lsn than its frontier {durable}: {verdict}"
    );

    // Half two: with no replica at all, it must REFUSE rather than report success.
    let dir2 = tempfile::tempdir().unwrap();
    let mut alone = start_primary_mode(
        &dir2.path().join("lonely.db"),
        50,
        Some("sync"),
        &[("FERRODB_SYNC_TIMEOUT_SECS", "1")],
    );
    let verdict = alone
        .lines
        .by_ref()
        .filter_map(|l| l.ok())
        .find(|l| l.starts_with("SYNC_OK") || l.starts_with("SYNC_TIMEOUT"))
        .expect("the primary never resolved its synchronous commit without a replica");
    assert!(
        verdict.starts_with("SYNC_TIMEOUT"),
        "synchronous commit reported success with no replica connected: {verdict}"
    );
    assert!(
        verdict.contains("asynchronous durability"),
        "the refusal hides that durability was downgraded: {verdict}"
    );
    assert!(
        verdict.contains("Nothing was rolled back"),
        "the refusal does not say what happened to the data: {verdict}"
    );
}
