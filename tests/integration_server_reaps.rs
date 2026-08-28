//! F11 — exit criterion 8 ("THE THESIS") against the **shipped binaries**, not a harness.
//!
//! The criterion is *"branches abandoned with NO client cooperation are reaped on lease expiry;
//! allocated page count returns to baseline"*. It has been true of the branch engine since B8 and
//! it was **not** true of anything anyone runs: `reap_expired` and `resume_interrupted_reaps` had no
//! caller outside tests and `examples/agent_isolation_demo.rs`, and `src/` contained no background
//! thread at all. So the reaper was correct code nothing ran.
//!
//! Every test here therefore spawns a real binary — `ferrodb` (the CLI) or `examples/pgserver` (the
//! server) — and reads the database's own files afterwards. Nothing here can pass by calling a
//! constructor the binaries do not call.
//!
//! # How each test is kept from passing vacuously
//!
//! * **The branch must really have allocated pages.** "Page count returned to baseline" is
//!   trivially true of a counter that never moved, so each phase asserts the counter moved before
//!   reading anything into its return.
//! * **The lease must be the reason.** In the resume tests the branch's lease has *not* expired, so
//!   a lease scan has no business touching it and only the resume can explain its pages coming
//!   back. Those tests assert the scan reaped nothing.
//! * **No client action, literally.** In the reap phase the CLI is spawned with an idle stdin and
//!   sent no SQL at all, and `pgserver` is spawned and never connected to. Not one statement, and
//!   in the server's case not one socket.
//!
//! # Where the page numbers come from, and what each one can prove
//!
//! `<db>.arena` is the durable free-space map, and `ArenaPageStore` rewrites it on every **extent**
//! event — a claim, a free, a slow-path retire, a pending-free drain — plus once more on a clean
//! exit. So:
//!
//! * `reserved_page_count` is exact whenever the map is read, because it only ever changes at an
//!   extent event, which is the moment the map is written.
//! * `live_page_count` counts individual pages, and those are handed out **between** extent events.
//!   It is exact after a clean exit (the CLI reaches `store.checkpoint`), and after a reap (the
//!   `free_arena` rewrite carries the live count as of that moment). A `SIGKILL` in the middle of a
//!   write-heavy phase would leave it lagging, which is why the phases that write finish cleanly.
//!
//! Both are read here by the *test*, out of the file, rather than reported by the process under
//! test.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::lease_thread::{LeaseThread, RuntimeLock};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::record::BranchRecord;
use ferrodb::branch::types::{ArenaId, BranchId, BranchState, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, PageLinks};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::cow::{CowPageLinks, PageStore};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;

/// Ordinary-table pages reserved below the arena floor. Small for the reason
/// `tests/integration_cli_agent_isolation.rs` gives: the 32736 default puts the arena's first page
/// ~128 MB into the file, which is 128 MB of real zeroes on a filesystem without sparse files —
/// NTFS, which CI runs.
const HEADROOM: u32 = 256;

/// Scan period for the phase that is meant to reap. Short so the test does not wait; the wait is
/// still on an observed marker rather than on a sleep.
const BRISK_SCAN_MILLIS: u64 = 100;

/// Scan period for the phases that must NOT reap. Longer than any of these tests, so a reap here
/// could only have come from the resume.
const NEVER_SCAN_MILLIS: u64 = 3_600_000;

/// Longest any test waits for a binary to say it did something it should do promptly.
const PATIENCE: Duration = Duration::from_secs(60);

// ---- the shipped binaries ----------------------------------------------------------------------

/// `<db>.<suffix>`, the way both binaries name their side files.
///
/// Spelled out rather than via `Path::with_extension`, which would have to be handed the whole
/// `db.branches` compound to mean this and reads as though it replaced `.db`.
fn side(db: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}.{suffix}", db.display()))
}

/// Refuse to run against a stale example binary.
///
/// Lifted verbatim in intent from `tests/integration_pgwire.rs`: **`cargo test` does not rebuild
/// examples**, so a test that spawns one can silently exercise a build from before the change under
/// test. That is not hypothetical — that file's first fire-check passed while the injected defect
/// was live, because the binary predated it.
fn assert_example_is_fresh(bin: &Path) {
    let bin_time = std::fs::metadata(bin)
        .unwrap_or_else(|e| {
            panic!("{} is missing ({e}); run: cargo build --examples", bin.display())
        })
        .modified()
        .expect("mtime");
    let own_src = bin
        .file_stem()
        .map(|s| Path::new("examples").join(format!("{}.rs", s.to_string_lossy())))
        .and_then(|p| std::fs::metadata(p).ok())
        .and_then(|m| m.modified().ok());
    let newest_src = [walk_newest(Path::new("src")), own_src].into_iter().flatten().max();
    if let Some(src_time) = newest_src {
        assert!(
            bin_time >= src_time,
            "{} is older than src/ or examples/ — cargo test does not rebuild examples, so this \
             would test a stale binary. Run: cargo build --examples",
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
                None => t,
                Some(cur) if t > cur => t,
                Some(cur) => cur,
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
    // `EXE_SUFFIX` is "" on unix and ".exe" on Windows; hardcoding the unix name has broken every
    // example-spawning test in this repo on the Windows runner at least once.
    let out = p.join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert_example_is_fresh(&out);
    out
}

/// A `ferrodb` CLI process whose stdout and stderr are drained as they arrive.
///
/// Drained on threads, not at exit, because these tests wait on a line the process prints *while it
/// is still running*. Reading readiness rather than sleeping is the same rule
/// `tests/integration_pgwire.rs` follows for `LISTENING`, and it is what makes the reap phase
/// deterministic instead of timing-dependent.
struct Cli {
    child: Child,
    stdin: Option<std::process::ChildStdin>,
    log: Arc<Mutex<String>>,
    readers: Vec<std::thread::JoinHandle<()>>,
}

fn spawn_cli(db: &Path, scan_millis: u64) -> Cli {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ferrodb"))
        .arg(db)
        .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
        .env("FERRODB_LEASE_SCAN_MILLIS", scan_millis.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ferrodb");
    let stdin = child.stdin.take();
    let log = Arc::new(Mutex::new(String::new()));
    let mut readers = Vec::new();
    let sources: Vec<Box<dyn Read + Send>> = vec![
        Box::new(child.stdout.take().expect("piped stdout")),
        Box::new(child.stderr.take().expect("piped stderr")),
    ];
    for src in sources {
        let log = Arc::clone(&log);
        readers.push(std::thread::spawn(move || {
            let mut src = src;
            let mut buf = [0u8; 4096];
            loop {
                match src.read(&mut buf) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        log.lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push_str(&String::from_utf8_lossy(&buf[..n]));
                    }
                }
            }
        }));
    }
    Cli { child, stdin, log, readers }
}

impl Cli {
    fn send(&mut self, sql: &str) {
        self.stdin.as_mut().expect("stdin still open").write_all(sql.as_bytes()).expect("write sql");
        self.stdin.as_mut().unwrap().flush().expect("flush sql");
    }

    fn output(&self) -> String {
        self.log.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Wait until the process has printed `marker`, or fail quoting everything it did print.
    fn wait_for(&self, marker: &str) {
        let deadline = Instant::now() + PATIENCE;
        while Instant::now() < deadline {
            if self.output().contains(marker) {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("ferrodb never printed {marker:?} in {PATIENCE:?}. Its output:\n{}", self.output());
    }

    /// Close stdin so the REPL exits through its normal path — which is what reaches
    /// `store.checkpoint`, and therefore what makes the durable page counts exact.
    fn finish(mut self) -> String {
        drop(self.stdin.take());
        let status = self.child.wait().expect("wait for ferrodb");
        for r in self.readers.drain(..) {
            let _ = r.join();
        }
        let out = self.output();
        assert!(status.success(), "ferrodb exited {:?}. Its output:\n{out}", status.code());
        out
    }
}

/// Run the CLI to completion over `sql`, refusing on any statement error.
fn cli_run(db: &Path, scan_millis: u64, sql: &str) -> String {
    let mut cli = spawn_cli(db, scan_millis);
    cli.send(sql);
    let out = cli.finish();
    assert!(
        !out.contains("error:"),
        "a statement failed, so nothing measured after it means anything:\n{out}"
    );
    out
}

/// A `pgserver` process whose stderr is captured to a file the test can read while it runs.
///
/// To a FILE and not a pipe, for the reason `tests/integration_system_views_wire.rs` gives: nothing
/// drains a pipe until the child is over, so a full pipe buffer would block the server.
struct Server {
    child: Child,
    stderr_path: PathBuf,
    db: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_server(db: &Path, scan_millis: u64) -> Server {
    let stderr_path = side(db, "stderr");
    let mut child = Command::new(example_bin("pgserver"))
        .arg(db)
        .arg("127.0.0.1:0")
        .env("FERRODB_LEASE_SCAN_MILLIS", scan_millis.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).expect("create stderr sink")))
        .spawn()
        .expect("spawn pgserver");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next() {
            Some(Ok(l)) if l.starts_with("LISTENING ") => break,
            Some(Ok(_)) => continue,
            _ => panic!(
                "pgserver exited before it started listening. Its stderr:\n{}",
                std::fs::read_to_string(&stderr_path).unwrap_or_default()
            ),
        }
    }
    Server { child, stderr_path, db: db.to_path_buf() }
}

impl Server {
    fn stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }

    fn wait_for_stderr(&self, marker: &str) {
        let deadline = Instant::now() + PATIENCE;
        while Instant::now() < deadline {
            if self.stderr().contains(marker) {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("pgserver never printed {marker:?} in {PATIENCE:?}. Its stderr:\n{}", self.stderr());
    }

    /// Kill the server and clear the lock it cannot release.
    ///
    /// `DbLock` is an `O_EXCL` create and its own header states the trade: a process killed with
    /// `SIGKILL` leaves the file behind and the next open refuses until somebody removes it, because
    /// a liveness check that guesses permissively reintroduces the aliasing the lock prevents. This
    /// is that somebody. There is no clean shutdown to use instead — `pgwire::serve` blocks on
    /// `listener.incoming()` for the life of the process.
    fn kill_and_unlock(mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let out = self.stderr();
        let _ = std::fs::remove_file(side(&self.db, "lock"));
        out
    }
}

// ---- reading the database's own files -----------------------------------------------------------

struct ArenaState {
    live: u32,
    reserved: u32,
    arenas: Vec<(ArenaId, BranchId)>,
    branches: Vec<BranchRecord>,
}

impl ArenaState {
    fn branch(&self, id: u64) -> &BranchRecord {
        self.branches
            .iter()
            .find(|r| r.branch_id.id == id)
            .unwrap_or_else(|| panic!("no record for branch {id} in {:?}", self.branches))
    }

    /// The single non-trunk branch these fixtures create.
    fn only_agent_branch(&self) -> &BranchRecord {
        let mut found = self.branches.iter().filter(|r| !r.branch_id.is_trunk());
        let first = found.next().expect("no agent branch in the catalog");
        assert!(found.next().is_none(), "expected exactly one agent branch: {:?}", self.branches);
        first
    }
}

/// Read `<db>.arena` and `<db>.branches` as the files the binaries left behind.
///
/// This opens the store read-only in effect: `reopen_from_checkpoint` derives the arena base from
/// the image and nothing here calls `checkpoint_to`, so the inspection cannot write over what it is
/// measuring.
fn arena_state(db: &Path) -> ArenaState {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(db)
        .expect("open the database file");
    let dm = Arc::new(DiskManager::new(file).expect("disk manager"));
    let bp = Arc::new(BufferPoolManager::new(dm));
    let branches =
        Arc::new(LogBranchCatalog::open(&side(db, "branches"), 1).expect("branch catalog"));
    let store =
        ArenaPageStore::reopen_from_checkpoint(bp, Arc::clone(&branches), &side(db, "arena"))
            .expect("reattach to the arena");
    ArenaState {
        live: store.live_page_count().expect("live page count"),
        reserved: store.reserved_page_count(),
        arenas: store.live_arenas(),
        branches: branches.all_branches().expect("branch records"),
    }
}

/// Rewrite one branch record in `<db>.branches`, the way a fixture must when it cannot wait out a
/// fifteen-minute lease or crash a process mid-reap on purpose.
///
/// The append-only log's last write per id slot wins, so this is the same mutation the engine makes.
fn amend_branch(db: &Path, id: u64, amend: impl FnOnce(&mut BranchRecord)) {
    let catalog = LogBranchCatalog::open(&side(db, "branches"), 1).expect("branch catalog");
    let mut rec = catalog.get_raw(id).expect("branch record");
    amend(&mut rec);
    catalog.put(&rec).expect("rewrite the branch record");
}

// ---- the fixture both binaries are tested against ----------------------------------------------

/// A database with a populated trunk and one agent branch that holds pages of its own and was
/// walked away from: no `MERGE`, no `ABANDON`, the client simply gone.
///
/// Returns `(baseline, populated)` — the arena as of before the agent session, and as of after it.
fn abandoned_branch_fixture(db: &Path) -> (ArenaState, ArenaState) {
    // Phase 1 — an ordinary database. `NEVER_SCAN_MILLIS` so no scan can interfere with the
    // measurement; there is nothing expired to reap in any case.
    cli_run(
        db,
        NEVER_SCAN_MILLIS,
        "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
         INSERT INTO inv VALUES (1, 10);\n",
    );
    let baseline = arena_state(db);

    // Phase 2 — an agent session that writes and is then abandoned. `stage()` mirrors a staged row
    // onto the branch's own copy-on-write tree, which is what makes this branch hold real pages
    // rather than a map.
    let out = cli_run(
        db,
        NEVER_SCAN_MILLIS,
        "BEGIN AGENT SESSION AS 'pricing' RUN 'r_1';\n\
         INSERT INTO inv VALUES (2, 20);\n\
         SELECT * FROM inv;\n",
    );
    assert!(
        out.contains("2 | 20"),
        "the agent could not see its own write, so this fixture's branch wrote nothing:\n{out}"
    );
    let populated = arena_state(db);

    // The whole thesis is about pages coming back, so the branch must have taken some.
    assert!(
        populated.live > baseline.live,
        "the agent session allocated no pages ({} -> {}), so 'the page count returned to baseline' \
         would be a fact about a counter that never moved",
        baseline.live,
        populated.live
    );
    assert!(
        populated.reserved > baseline.reserved,
        "the agent session claimed no extent ({} -> {})",
        baseline.reserved,
        populated.reserved
    );
    let branch = populated.only_agent_branch();
    assert_eq!(branch.state, BranchState::Live, "the fixture's branch must be live and abandoned");
    assert!(!branch.arenas.is_empty(), "the branch holds no arena, so nothing can be returned");

    (baseline, populated)
}

// ------------------------------------------------------------------------------------------------
// THE THESIS — exit criterion 8, through each shipped binary.
// ------------------------------------------------------------------------------------------------

#[test]
fn the_cli_reaps_an_abandoned_branch_with_no_client_action_and_pages_return_to_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("reap.db");
    let (baseline, populated) = abandoned_branch_fixture(&db);
    let branch = populated.only_agent_branch().branch_id;

    // The lease expires. Compressed in time rather than waited out: the deadline is a durable field
    // and `is_expired_at` is a pure comparison, so a deadline in the past is exactly the state a
    // fifteen-minute wait would produce.
    amend_branch(&db, branch.id, |r| r.lease_deadline = LeaseDeadline(0));

    // NO CLIENT ACTION. The process is started and sent nothing at all — not one statement — and
    // the only thing waited on is the line its own lease thread prints.
    let cli = spawn_cli(&db, BRISK_SCAN_MILLIS);
    cli.wait_for("lease: reaped");
    let out = cli.finish();

    assert!(
        out.contains(&format!("{branch}")),
        "the reap did not name the branch it took; the log has to be usable as evidence:\n{out}"
    );
    assert!(
        out.contains("with no client cooperation"),
        "the reap was not reported as non-cooperative:\n{out}"
    );

    let after = arena_state(&db);
    assert_eq!(
        after.live, baseline.live,
        "allocated page count did not return to baseline ({} at baseline, {} after the branch \
         wrote, {} after the reap)",
        baseline.live, populated.live, after.live
    );
    assert_eq!(
        after.reserved, baseline.reserved,
        "the extent did not go back to the free space map, it merely stopped growing"
    );
    let rec = after.branch(branch.id);
    assert_eq!(rec.state, BranchState::Reaped);
    assert!(rec.arenas.is_empty(), "a reaped branch still holds arenas: {:?}", rec.arenas);
    assert!(
        !after.arenas.iter().any(|(_, owner)| owner.id == branch.id),
        "the store still lists an extent owned by the reaped branch: {:?}",
        after.arenas
    );
}

#[test]
fn the_server_reaps_an_abandoned_branch_without_one_socket_being_opened() {
    // The same claim about `pgserver`, which is the binary with no interactive user at all. The
    // fixture is built with the CLI because both binaries are the same engine over the same files,
    // and the claim under test is about what the *server* does with a database it is handed: it is
    // started, never connected to, and reaps.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("reap.db");
    let (baseline, populated) = abandoned_branch_fixture(&db);
    let branch = populated.only_agent_branch().branch_id;
    amend_branch(&db, branch.id, |r| r.lease_deadline = LeaseDeadline(0));

    let server = start_server(&db, BRISK_SCAN_MILLIS);
    server.wait_for_stderr("lease: reaped");
    let stderr = server.kill_and_unlock();

    assert!(
        stderr.contains(&format!("{branch}")) && stderr.contains("with no client cooperation"),
        "the server's own log does not name the branch it reaped:\n{stderr}"
    );

    let after = arena_state(&db);
    // Exact even though the server was killed: the last thing that touched the map was the reap's
    // own `free_arena`, which rewrites it, and nothing allocated in this phase because nothing
    // connected. See the module header on which of these two numbers survives a `SIGKILL`.
    assert_eq!(
        after.live, baseline.live,
        "allocated page count did not return to baseline ({} at baseline, {} after the branch \
         wrote, {} after the reap)",
        baseline.live, populated.live, after.live
    );
    assert_eq!(after.reserved, baseline.reserved, "the extent did not go back");
    assert_eq!(after.branch(branch.id).state, BranchState::Reaped);
}

#[test]
fn a_live_lease_survives_a_server_that_scans_continuously() {
    // The negative control, and the one that matters most: a collector that reclaimed
    // unconditionally would pass both tests above and be catastrophic. Same fixture, same brisk
    // scan, lease left alone.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("keep.db");
    let (_baseline, populated) = abandoned_branch_fixture(&db);
    let branch = populated.only_agent_branch().branch_id;

    let server = start_server(&db, BRISK_SCAN_MILLIS);
    // Wait for the scan to have run many times over, so "it has not reaped yet" and "it does not
    // reap a live lease" are not the same observation.
    std::thread::sleep(Duration::from_millis(BRISK_SCAN_MILLIS * 20));
    let stderr = server.kill_and_unlock();
    assert!(!stderr.contains("lease: reaped"), "a live lease was reaped:\n{stderr}");
    assert!(!stderr.contains("lease: NOT reaping"), "a standalone node refused the clock:\n{stderr}");
    assert!(!stderr.contains("lease: scan failed"), "the scan errored:\n{stderr}");

    let after = arena_state(db.as_path());
    assert_eq!(after.live, populated.live, "a page was reclaimed from a live branch");
    assert_eq!(after.reserved, populated.reserved);
    assert_eq!(after.branch(branch.id).state, BranchState::Live);
}

// ------------------------------------------------------------------------------------------------
// The cooperative door: a branch that ENDS must give its pages back too.
// ------------------------------------------------------------------------------------------------

#[test]
fn a_merged_branch_gives_its_extent_back_without_any_lease_scan() {
    // The quieter half of what F11 wired. `AgentRuntime::seal` has two paths, and without a reaper
    // attached it takes the one that marks a merged or abandoned branch `Reaped` through the
    // `BranchCatalog` trait and **never frees the extents the branch allocated**. Both shipped
    // binaries were on that path, so every `MERGE` and every `ABANDON` leaked the branch's pages
    // until the file was rebuilt.
    //
    // `NEVER_SCAN_MILLIS` is the whole point: no lease scan can fire, so what is measured here is
    // the reaper being attached to the runtime and nothing else.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("merged.db");
    cli_run(
        &db,
        NEVER_SCAN_MILLIS,
        "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
         INSERT INTO inv VALUES (1, 10);\n",
    );
    let baseline = arena_state(&db);

    let out = cli_run(
        &db,
        NEVER_SCAN_MILLIS,
        "BEGIN AGENT SESSION AS 'pricing' RUN 'r_1';\n\
         INSERT INTO inv VALUES (2, 20);\n\
         MERGE;\n",
    );
    assert!(out.contains("Clean"), "a merge with no competing write was not Clean:\n{out}");
    assert!(!out.contains("lease: reaped"), "a lease scan fired and stole the claim:\n{out}");

    let after = arena_state(&db);
    let branch = after.only_agent_branch();
    assert_eq!(branch.state, BranchState::Reaped, "the merged branch was not retired");
    assert!(branch.arenas.is_empty(), "the merged branch still holds arenas: {:?}", branch.arenas);
    assert!(
        !after.arenas.iter().any(|(_, owner)| owner.id == branch.branch_id.id),
        "the store still lists an extent owned by the merged branch: {:?}",
        after.arenas
    );
    // `reserved` and not `live`: publishing the merged row writes into TRUNK's own tree, which
    // legitimately leaves trunk holding more pages than the baseline. What must come back is the
    // *branch's* extent, and that is exactly what this number counts.
    assert_eq!(
        after.reserved, baseline.reserved,
        "the merged branch's extent never went back to the free space map ({} at baseline, {} \
         after the merge): every MERGE leaks it",
        baseline.reserved, after.reserved
    );
}

// ------------------------------------------------------------------------------------------------
// A crash mid-reap must not strand a branch.
// ------------------------------------------------------------------------------------------------

#[test]
fn the_server_finishes_a_reap_a_crash_interrupted_before_it_serves_anything() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("resume.db");
    let (baseline, populated) = abandoned_branch_fixture(&db);
    let branch = populated.only_agent_branch().branch_id;

    // What a crash in the middle of `reap` leaves: the record marked `Reaping` durably — which
    // `reap` writes *before* it frees anything, precisely so the evidence exists — with the
    // branch's extents still charged to it. **The lease is left alone**, so it has not expired and
    // a lease scan has no business touching this branch at all.
    amend_branch(&db, branch.id, |r| r.state = BranchState::Reaping);
    assert_eq!(arena_state(&db).branch(branch.id).state, BranchState::Reaping);

    // `NEVER_SCAN_MILLIS`: no scan can fire during this test beyond the one at startup, and that
    // one finds nothing expired. Only the resume can explain the pages coming back.
    let server = start_server(&db, NEVER_SCAN_MILLIS);
    server.wait_for_stderr("lease: finished 1 reap(s) a crash interrupted");
    let stderr = server.kill_and_unlock();
    assert!(
        !stderr.contains("lease: reaped"),
        "the lease scan claimed this branch, but its lease had not expired — so the reaper is \
         ignoring deadlines and this test is measuring the wrong thing:\n{stderr}"
    );

    let after = arena_state(&db);
    assert_eq!(
        after.branch(branch.id).state,
        BranchState::Reaped,
        "a branch left `Reaping` by a crash stays unreadable with its pages charged to it forever \
         unless something finishes the reap"
    );
    assert_eq!(
        after.live, baseline.live,
        "the interrupted reap's pages did not come back ({} at baseline, {} before the resume, {} \
         after)",
        baseline.live, populated.live, after.live
    );
    assert_eq!(after.reserved, baseline.reserved);
}

#[test]
fn a_healthy_database_resumes_nothing_on_startup() {
    // The other half of the test above: "1 interrupted reap(s) finished" is only evidence if the
    // same line can say zero.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("healthy.db");
    abandoned_branch_fixture(&db);
    let server = start_server(&db, NEVER_SCAN_MILLIS);
    server.wait_for_stderr("interrupted reap(s) finished on startup");
    let stderr = server.kill_and_unlock();
    assert!(
        stderr.contains("0 interrupted reap(s) finished on startup"),
        "a database with nothing half-reaped reported a resume:\n{stderr}"
    );
}

// ------------------------------------------------------------------------------------------------
// The scan interval knob refuses rather than defaulting — through the binaries.
// ------------------------------------------------------------------------------------------------

#[test]
fn both_binaries_refuse_to_start_on_an_unusable_scan_interval() {
    // A knob that silently fell back would let an operator who set five seconds run on thirty and
    // read the delay as a reaper that does not work. Checked through the binaries, because a unit
    // test on the parser cannot see whether either `main` acts on the refusal.
    let dir = tempfile::tempdir().unwrap();
    for (n, bad) in ["off", "0", "-1"].iter().enumerate() {
        let db = dir.path().join(format!("bad{n}.db"));
        let cli = Command::new(env!("CARGO_BIN_EXE_ferrodb"))
            .arg(&db)
            .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
            .env("FERRODB_LEASE_SCAN_MILLIS", bad)
            .stdin(Stdio::null())
            .output()
            .expect("spawn ferrodb");
        let text = String::from_utf8_lossy(&cli.stderr).to_string();
        assert!(
            !cli.status.success() && text.contains("FERRODB_LEASE_SCAN_MILLIS"),
            "the CLI accepted FERRODB_LEASE_SCAN_MILLIS={bad:?} (exit {:?}):\n{text}",
            cli.status.code()
        );

        let sdb = dir.path().join(format!("badsrv{n}.db"));
        let srv = Command::new(example_bin("pgserver"))
            .arg(&sdb)
            .arg("127.0.0.1:0")
            .env("FERRODB_LEASE_SCAN_MILLIS", bad)
            .output()
            .expect("spawn pgserver");
        let text = String::from_utf8_lossy(&srv.stderr).to_string();
        assert!(
            !srv.status.success() && text.contains("FERRODB_LEASE_SCAN_MILLIS"),
            "pgserver accepted FERRODB_LEASE_SCAN_MILLIS={bad:?} (exit {:?}):\n{text}",
            srv.status.code()
        );
    }
}

// ------------------------------------------------------------------------------------------------
// Never guess the time — the rule that cannot be tested in the lib's own test binary.
// ------------------------------------------------------------------------------------------------

/// F4's clock, in this binary rather than the lib's: the authority is process-scoped, and a unit
/// test that joined a cluster would make every sibling lease-taking test in the lib refuse.
/// `src/cluster/tests.rs` says so in its own header, so the rule is pinned here.
#[test]
fn a_node_that_does_not_know_the_clusters_time_refuses_to_reap_rather_than_guessing() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("clock.db"))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = bp.disk_manager.high_water().unwrap() + HEADROOM;
    let store = Arc::new(ArenaPageStore::new(bp, Arc::clone(&catalog), base).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .unwrap(),
    );
    let reaper = Arc::new(
        TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store))
            .with_links(Arc::new(CowPageLinks) as Arc<dyn PageLinks>),
    );

    // Built while standalone: on a cluster member `alloc_arena` refuses without a leader grant, and
    // this test is about the clock, not about grants.
    let baseline = store.live_page_count().unwrap();
    let expired = catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id;
    for r in 0..120u64 {
        runtime
            .put_row(expired, "inv", r, &[Value::Integer(r as i32), Value::Varchar("x".into())])
            .unwrap();
    }
    let with_pages = store.live_page_count().unwrap();
    assert!(with_pages > baseline, "the branch wrote no pages, so nothing is at stake here");

    // Now the process is a cluster member that has applied no `LeaseTick`. It does not know the
    // time, and reaping is destructive and unrecoverable — a `BranchId` generation makes a wrong
    // reap permanent — so the only admissible answer is to refuse and say so.
    let scope = ferrodb::cluster::ClusterScope::joined(ferrodb::consensus::NodeId(1));
    let gate = Arc::new(NoOpGate);
    let lease = LeaseThread::start(
        Arc::clone(&reaper),
        Arc::clone(&runtime),
        gate as Arc<dyn RuntimeLock>,
        Duration::from_millis(20),
    )
    .unwrap();

    let deadline = Instant::now() + PATIENCE;
    while lease.stats().refused < 3 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let refusing = lease.stats();
    assert!(
        refusing.refused >= 3,
        "the scan did not refuse on a node with no cluster time; it either reaped or stalled: \
         {refusing:?}"
    );
    assert_eq!(refusing.reaped, 0, "an expired branch was reaped on a clock this node cannot read");
    assert_eq!(
        catalog.get_raw(expired.id).unwrap().state,
        BranchState::Live,
        "the branch was touched without a cluster time"
    );
    assert_eq!(store.live_page_count().unwrap(), with_pages, "a page was freed on a guessed clock");

    // The refusal must be a refusal and not a stop: once the cluster says what time it is, the same
    // scan reaps. A detector that never stops firing is as useless as one that never fires.
    ferrodb::cluster::apply_lease_tick(u64::MAX / 2).expect("apply a tick as a member");
    let deadline = Instant::now() + PATIENCE;
    while lease.stats().reaped == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let stats = lease.stop();
    drop(scope);
    assert!(stats.reaped >= 1, "the scan never resumed after the tick arrived: {stats:?}");
    assert_eq!(catalog.get_raw(expired.id).unwrap().state, BranchState::Reaped);
    assert_eq!(
        store.live_page_count().unwrap(),
        baseline,
        "the pages did not come back once the reap was allowed to happen"
    );
}

/// A gate that holds nothing, for the one test whose subject is the clock rather than the barrier.
struct NoOpGate;

impl RuntimeLock for NoOpGate {
    fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
        body();
    }
}
