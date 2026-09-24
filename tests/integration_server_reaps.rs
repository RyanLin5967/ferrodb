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
//! `<db>.arena` is the durable free-space map, and `ArenaPageStore` makes it durable on every
//! **extent** event — a claim, a free, a slow-path retire, a pending-free drain — plus once more on
//! a clean exit. So:
//!
//! ⚠ **D81 CHANGED THE MECHANISM AND NOT THE PROPERTY, AND THIS PARAGRAPH USED TO NAME THE
//! MECHANISM.** It said `ArenaPageStore` *"rewrites it"* on every extent event. It no longer
//! rewrites: `<db>.arena` is `[image][tail record]*`, a claim appends 45 bytes and a free 25, and
//! the whole image is rewritten only when the tail outgrows its share of it. **What every number
//! below rests on is untouched** — a record still reaches the device, fsynced, at each extent
//! event, so "the map is durable as of the last extent event" is exactly as true as before. Only
//! the sentence describing HOW was false, and a header that mis-names the mechanism is how the
//! next reader concludes these assertions are stale when they are not.
//!
//! * `reserved_page_count` is exact whenever the map is read, because it only ever changes at an
//!   extent event, which is the moment the map is made durable.
//! * `live_page_count` counts individual pages, and those are handed out **between** extent events.
//!   It is exact after a clean exit (the CLI reaches `store.checkpoint`), and after a reap (the
//!   `free_arena` record carries the live count as of that moment — it is written as an ABSOLUTE
//!   for this reason). A `SIGKILL` in the middle of a
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
use ferrodb::branch::TableBranchCatalog;
use ferrodb::branch::lease_thread::{LeaseThread, RuntimeLock};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::record::BranchRecord;
use ferrodb::branch::types::{ArenaId, BranchId, BranchState, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
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
        // **A `src/**/tests_*.rs` file does not link into an example binary**, so editing one
        // cannot make that binary stale — and `cargo build --examples` correctly does not rebuild
        // for it, because those modules are `#[cfg(test)]` and are not part of the lib's non-test
        // fingerprint. Counting them made this guard fire on a tree whose examples WERE fresh:
        // one edit to `src/consensus/tests_transport.rs` failed 53 tests across 5 targets.
        // The convention is enforced, not assumed — see `test_only_sources_are_cfg_test_gated`.
        if p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("tests_")) {
            continue;
        }
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
    // REFUSE if the catalog is missing rather than open one. `open_sidecar` would CREATE an empty
    // catalog over a missing file, and this fixture would then see a database with no branches and
    // assert against it - which is exactly how the switchover's durability bug stayed quiet until
    // it was driven end to end. A missing catalog is a failure, not an empty one.
    let cat_path = side(db, "branchcat");
    assert!(
        cat_path.exists(),
        "no branch catalog at {}: the binaries wrote none, so there is nothing to inspect and an \
         empty one would make every assertion below vacuous",
        cat_path.display()
    );
    let branches =
        Arc::new(TableBranchCatalog::open_sidecar(&cat_path, 1).expect("branch catalog"));
    let store =
        ArenaPageStore::reopen_from_checkpoint(bp, Arc::clone(&branches) as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>, &side(db, "arena"))
            .expect("reattach to the arena");
    ArenaState {
        live: store.live_page_count().expect("live page count"),
        reserved: store.reserved_page_count(),
        arenas: store.live_arenas(),
        branches: branches.scan().expect("scan").collect::<Result<Vec<_>, _>>().expect("branch records"),
    }
}

/// Open `<db>.branchcat` and hand the fixture the record it is about to amend.
///
/// **D41 — a fixture may no longer rewrite a whole record, because nothing may.** `amend_branch`
/// used to take a `FnMut(&mut BranchRecord)` and `put` the result; its two amendments were a lease
/// deadline and a state, and each now has its own catalog operation. Both commit — flush and fsync
/// — so the amended record is on disk before the next binary opens the database.
fn open_branchcat(db: &Path) -> TableBranchCatalog {
    let cat_path = side(db, "branchcat");
    assert!(cat_path.exists(), "no branch catalog at {} to amend", cat_path.display());
    TableBranchCatalog::open_sidecar(&cat_path, 1).expect("branch catalog")
}

/// Expire a branch's lease, the way a fixture must when it cannot wait out a fifteen-minute one.
fn expire_lease(db: &Path, id: u64) {
    let catalog = open_branchcat(db);
    let rec = catalog.get_raw(id).expect("branch record");
    catalog.renew_lease(rec.branch_id, LeaseDeadline(0)).expect("expire the lease");
}

/// Leave a branch durably `Reaping`, the way a crash mid-reap does, without crashing a process.
fn interrupt_reap(db: &Path, id: u64) {
    let catalog = open_branchcat(db);
    let rec = catalog.get_raw(id).expect("branch record");
    catalog
        .set_state(rec.branch_id, rec.state, BranchState::Reaping)
        .expect("mark the record Reaping");
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
    expire_lease(&db, branch.id);

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
    expire_lease(&db, branch.id);

    let server = start_server(&db, BRISK_SCAN_MILLIS);
    server.wait_for_stderr("lease: reaped");
    let stderr = server.kill_and_unlock();

    assert!(
        stderr.contains(&format!("{branch}")) && stderr.contains("with no client cooperation"),
        "the server's own log does not name the branch it reaped:\n{stderr}"
    );

    let after = arena_state(&db);
    // Exact even though the server was killed: the last thing that touched the map was the reap's
    // own `free_arena`, which makes it durable, and nothing allocated in this phase because
    // nothing connected. See the module header on which of these two numbers survives a `SIGKILL`.
    //
    // ⚠ **D81: "which rewrites it" was the old spelling and is now false** — `free_arena` appends
    // a record and fsyncs rather than rewriting the image. This assertion is unaffected, and that
    // is a fact about the record's CONTENTS rather than luck: the free record carries `live_pages`
    // as an ABSOLUTE snapshot, not a delta (`arena.rs` writes it beside the extent, and the replay
    // beside `TAIL_EXTENT_FREED` explains the asymmetry — `reserved_pages` can be a delta because
    // it only moves on the two paths that write a record, while `live_pages` moves on every
    // `alloc_in_arena`/`release_page`, which persist nothing). So a restore is exactly as fresh as
    // it was when every claim rewrote the whole image, which is what keeps `after.live` exact here.
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
    interrupt_reap(&db, branch.id);
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
    //
    // **And each refusal must leave no lock behind.** `pgserver` refuses with `process::exit`,
    // which does not run destructors, so parsing this variable after `DbLock::acquire` would strand
    // `<db>.lock` — and the operator who then corrected the variable would be told the database is
    // already open by a process that no longer exists. That is exactly what the first version of
    // this wiring did; the assertion below is what would have caught it.
    let dir = tempfile::tempdir().unwrap();
    for (n, bad) in ["off", "0", "-1", "86400001", ""].iter().enumerate() {
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
        assert!(
            !side(&db, "lock").exists(),
            "the CLI refused FERRODB_LEASE_SCAN_MILLIS={bad:?} but left {} behind; the next open \
             with the variable corrected would be refused as already-in-use",
            side(&db, "lock").display()
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
        assert!(
            !side(&sdb, "lock").exists(),
            "pgserver refused FERRODB_LEASE_SCAN_MILLIS={bad:?} but left {} behind — its refusal \
             path is `process::exit`, which does not drop the DbLock",
            side(&sdb, "lock").display()
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
    let store = Arc::new(ArenaPageStore::new(bp, Arc::clone(&catalog) as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>, base).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .unwrap(),
    );
    let reaper = Arc::new(TwoTierReaper::new(
        Arc::clone(&catalog) as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>,
        Arc::clone(&store),
    ));

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
    while lease.stats().refused_scans < 3 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let refusing = lease.stats();
    assert!(
        refusing.refused_scans >= 3,
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

// ------------------------------------------------------------------------------------------------
// F1 — downtime is not charged to a lease. Chubby §2.9: while the authority is down the lease
// timer is STOPPED, which is equivalent to extending every lease by the outage. §2.8: a deadline
// may move later, never earlier.
//
// Source: `artie-research/frontier/research_reclaim-with-live-children.md` §5 F1. Before the fix a
// server that restarted after an outage longer than a branch's remaining lease reaped that branch
// on its first scan: the agent never had a chance to act, and the design could not tell
// "abandoned before the outage" from "expired because of the outage".
//
// Both tests below use REAL downtime, measured on the wall clock the binaries anchor their lease
// clock to, and change nothing but the one lease deadline — through the same offline catalog
// operation `expire_lease` uses. Nothing here reaches for a mark or a clock the binaries do not
// read themselves, so the tests compile, and mean the same thing, before and after the fix.
// ------------------------------------------------------------------------------------------------

/// How long the lease in [`a_lease_that_lapsed_only_while_the_server_was_down_survives_the_first_scan_after_restart`]
/// had left when the database went down. It is also the margin the post-restart window runs
/// inside: the branch must still be live for this long after the restart, so a test process that
/// is descheduled for less than this cannot turn a correct server into a failure.
const LEASE_LEFT_AT_SHUTDOWN_MILLIS: u64 = 5_000;

/// How long past that deadline the database stays down. Any positive value makes the lease lapse
/// during the outage; one second keeps it unambiguous at millisecond clock resolution.
const DOWN_PAST_DEADLINE_MILLIS: u64 = 1_000;

/// Slack for comparing the test's wall-clock readings with a binary's lease clock. Each binary
/// anchors its lease clock to this same wall clock at its first reading and then advances it
/// monotonically, so the two disagree only by clock-rate drift over a few seconds, or by a wall
/// step during the test. A second is far above the first and states the second as a failure.
const CLOCK_SLACK_MILLIS: u64 = 1_000;

/// Milliseconds since the unix epoch on this machine's wall clock — the clock each binary anchors
/// its lease clock to when it starts. Read here directly, rather than through `LeaseDeadline`, so
/// that the test's notion of "now" is not the subject's.
fn wall_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("wall clock before 1970")
        .as_millis() as u64
}

/// Set a branch's lease deadline offline, to exactly `deadline`. The general form of
/// [`expire_lease`], through the same narrow catalog operation.
fn set_lease(db: &Path, id: u64, deadline: u64) {
    let catalog = open_branchcat(db);
    let rec = catalog.get_raw(id).expect("branch record");
    catalog.renew_lease(rec.branch_id, LeaseDeadline(deadline)).expect("set the lease");
}

/// The downtime a restart measured, as the server itself printed it.
///
/// Read from the server's own line so that the assertion on the moved deadline compares two
/// numbers the SERVER produced — the extension it applied and the downtime it measured — rather
/// than a downtime the test estimated from outside and the server never saw.
fn printed_downtime(stderr: &str) -> u64 {
    const LEAD: &str = "lease: clock resumed after ";
    let at = stderr.find(LEAD).unwrap_or_else(|| {
        panic!(
            "the server never said it resumed the lease clock, so no downtime was measured and \
             nothing below can be about a restart grace. Its stderr:\n{stderr}"
        )
    });
    let digits: String =
        stderr[at + LEAD.len()..].chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().unwrap_or_else(|e| {
        panic!("the resume line carries no downtime in milliseconds ({e}). Its stderr:\n{stderr}")
    })
}

#[test]
fn a_lease_that_lapsed_only_while_the_server_was_down_survives_the_first_scan_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("grace.db");
    let (baseline, populated) = abandoned_branch_fixture(&db);
    let branch = populated.only_agent_branch().branch_id;

    // The fixture's last binary has exited, so whatever it last knew about the lease clock is at
    // or before this reading.
    let shutdown = wall_millis();
    // At shutdown the branch had `LEASE_LEFT_AT_SHUTDOWN_MILLIS` left...
    let deadline = shutdown + LEASE_LEFT_AT_SHUTDOWN_MILLIS;
    set_lease(&db, branch.id, deadline);
    // ...and the database stays down until well past it. Real downtime, not a rewritten clock: the
    // lease lapses while nothing is running, which is the whole of F1's scenario.
    while wall_millis() < deadline + DOWN_PAST_DEADLINE_MILLIS {
        std::thread::sleep(Duration::from_millis(50));
    }

    // The mark the server will measure its downtime from, read offline BEFORE it starts, through a
    // read-only accessor (D198 adversary, C4). With the test's own clock readings either side of
    // the server's resume, it bounds the downtime the server reports from outside the server.
    let mark = open_branchcat(&db)
        .last_alive_mark()
        .expect("read the last-alive mark")
        .expect("the fixture's binaries each resumed and marked, so the catalog must hold a mark");
    let t_spawn = wall_millis();
    let server = start_server(&db, BRISK_SCAN_MILLIS);
    // Printed after `LeaseThread::start` returns, i.e. after the resume and after the scan thread
    // has been spawned — so the window below is measured from a point at which scanning has begun.
    server.wait_for_stderr("pgserver: lease scan every");
    let t_seen = wall_millis();
    // Ten scan intervals. Before the fix the first of them reaps the branch.
    std::thread::sleep(Duration::from_millis(BRISK_SCAN_MILLIS * 10));
    let early = server.stderr();
    assert!(
        !early.contains("lease: reaped"),
        "a lease that lapsed only while the server was down was reaped on the first scan after \
         the restart — F1. The branch had {LEASE_LEFT_AT_SHUTDOWN_MILLIS}ms left when the \
         database stopped, and the agent never had a chance to act. Its stderr:\n{early}"
    );
    // D198 removed an assertion here, `early.contains("1 live lease(s) extended")`: it pinned a
    // count of rewritten leases, and the O(1) restart rewrites none and counts none (a count would
    // be an O(live branches) walk at open). Its purpose — that the survival above is a lease that
    // was kept and not a scan that never ran — is carried by `printed_downtime`, which panics
    // without the resume line, and by the exact-deadline assertion below, which is unchanged.
    let downtime = printed_downtime(&early);
    // Independent of the server's own report (C4): its resume read its clock after `t_spawn` and
    // before `t_seen`, and measured from `mark`. A server that inflated the downtime it measured
    // AND the shift it applied by the same amount passes the exact-deadline assertion below, and
    // fails this.
    assert!(
        downtime + CLOCK_SLACK_MILLIS >= t_spawn - mark && downtime <= t_seen - mark + CLOCK_SLACK_MILLIS,
        "the server measured {downtime}ms of downtime from mark {mark}, but it resumed between \
         {t_spawn} and {t_seen} by the test's clock, so the downtime must lie in [{}, {}] (slack \
         {CLOCK_SLACK_MILLIS}ms). Its stderr:\n{early}",
        (t_spawn - mark).saturating_sub(CLOCK_SLACK_MILLIS),
        t_seen - mark + CLOCK_SLACK_MILLIS
    );
    assert!(
        downtime >= LEASE_LEFT_AT_SHUTDOWN_MILLIS + DOWN_PAST_DEADLINE_MILLIS,
        "the server measured {downtime}ms of downtime, but it was down for at least \
         {}ms by the wall clock — it is crediting less than the outage, so a lease that lapsed \
         late in it would still be reaped. Its stderr:\n{early}",
        LEASE_LEFT_AT_SHUTDOWN_MILLIS + DOWN_PAST_DEADLINE_MILLIS
    );

    // **A lease, not an exemption.** Once the remainder it had at shutdown runs out, the same
    // scan reaps it with no client action — exit criterion 8 still holds for this branch.
    server.wait_for_stderr("lease: reaped");
    let stderr = server.kill_and_unlock();
    assert!(
        stderr.contains(&format!("{branch}")),
        "the reap that ended the preserved lease did not name the branch:\n{stderr}"
    );

    let after = arena_state(&db);
    let rec = after.branch(branch.id);
    assert_eq!(rec.state, BranchState::Reaped, "the preserved lease was never enforced");
    // `mark_reaped` does not touch the deadline, so the reaped record still carries the one the
    // restart wrote.
    assert_eq!(
        rec.lease_deadline.0,
        deadline + downtime,
        "the restart moved the deadline by something other than the downtime it measured \
         ({deadline} + {downtime}). Chubby's rule is exactly the outage: less charges downtime to \
         the lease, more hands out time nobody was owed"
    );
    assert_eq!(
        after.live, baseline.live,
        "allocated page count did not return to baseline once the preserved lease ran out ({} at \
         baseline, {} after the branch wrote, {} after the reap)",
        baseline.live, populated.live, after.live
    );
    assert_eq!(after.reserved, baseline.reserved, "the extent did not go back");
}

#[test]
fn a_lease_that_had_already_expired_before_the_server_went_down_is_still_reaped_after_restart() {
    // The control for the test above, and the half of F1 that must NOT change: stopping the timer
    // during an outage keeps what a lease had left, it does not hand a lease back to a branch that
    // had already run out while the database was up. `the_server_reaps_an_abandoned_branch_without
    // _one_socket_being_opened` pins the degenerate deadline 0; this pins a realistic one, an hour
    // before the shutdown. A restart that gave every live branch a fresh window — the obvious lazy
    // fix — passes the test above and fails this one.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("expired.db");
    let (baseline, populated) = abandoned_branch_fixture(&db);
    let branch = populated.only_agent_branch().branch_id;

    let shutdown = wall_millis();
    let deadline = shutdown - 3_600_000;
    set_lease(&db, branch.id, deadline);

    let server = start_server(&db, BRISK_SCAN_MILLIS);
    server.wait_for_stderr("lease: reaped");
    let stderr = server.kill_and_unlock();
    assert!(
        stderr.contains(&format!("{branch}")) && stderr.contains("with no client cooperation"),
        "the server's own log does not name the branch it reaped:\n{stderr}"
    );

    let after = arena_state(&db);
    assert_eq!(
        after.branch(branch.id).state,
        BranchState::Reaped,
        "a lease that expired an hour before the shutdown survived the restart"
    );
    assert_eq!(
        after.live, baseline.live,
        "allocated page count did not return to baseline ({} at baseline, {} after the branch \
         wrote, {} after the reap)",
        baseline.live, populated.live, after.live
    );
    assert_eq!(after.reserved, baseline.reserved, "the extent did not go back");
}
