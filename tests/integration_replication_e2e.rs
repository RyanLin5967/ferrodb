//! E4 — replication end to end: two processes, a real socket, real SQL.
//!
//! The primary runs genuine `CREATE TABLE` / `INSERT` statements, so what it ships is the log of
//! work going through the heap and the transaction manager — not records manufactured for the
//! test. The replica connects over TCP, follows the stream, and must converge.
//!
//! **Convergence is judged by page bytes**, read off both files afterwards, rather than by either
//! process's opinion of itself. A replica that reports "caught up" while holding different data is
//! precisely the failure worth catching.
//!
//! # The limitation this test pins
//!
//! Physical WAL replication carries **only what the WAL describes**. In this engine the catalog is
//! written outside the log, so a replica receives the heap pages and **not the schema**: it has the
//! rows but nothing to interpret them with. That is asserted below rather than left for someone to
//! discover, because "we replicate the database" would be the wrong summary of what this does.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

const PAGE_SIZE: usize = 4096;


/// Refuse to run against a stale example binary.
///
/// `cargo test` does NOT rebuild examples — confirmed by mtime — so a test that spawns one can
/// silently exercise a build from before the change under test. That is not a hypothetical: the
/// first fire-check of this file passed while the injected defect was live, because the replica
/// binary predated it.
///
/// A test that cannot observe the code it claims to test is worse than no test, so this refuses
/// with an instruction instead of passing.
fn assert_example_is_fresh(bin: &Path) {
    let bin_time = std::fs::metadata(bin)
        .unwrap_or_else(|e| panic!("{} is missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .expect("mtime");
    let newest_src = walk_newest(Path::new("src"));
    if let Some(src_time) = newest_src {
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
    let entries = std::fs::read_dir(dir).ok()?;
    for e in entries.flatten() {
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
    let out = p.join("examples").join(name);
    assert_example_is_fresh(&out);
    out
}

struct Primary {
    child: Child,
    addr: String,
    start_lsn: u64,
    durable_lsn: u64,
}

impl Drop for Primary {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start a primary, wait for it to announce readiness rather than sleeping and hoping.
fn start_primary(db: &Path, rows: u32) -> Primary {
    let mut child = Command::new(example_bin("repl_primary"))
        .arg(db)
        .arg("127.0.0.1:0")
        .arg(rows.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn primary");

    let stdout = child.stdout.take().expect("piped");
    let mut lines = BufReader::new(stdout).lines();
    let (mut addr, mut start, mut durable) = (None, None, None);
    while addr.is_none() || start.is_none() || durable.is_none() {
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
            Some(Ok(_)) => continue,
            _ => panic!("primary exited before announcing readiness"),
        }
    }
    Primary {
        child,
        addr: addr.unwrap(),
        start_lsn: start.unwrap(),
        durable_lsn: durable.unwrap(),
    }
}

/// Pages the primary's WAL actually describes — the only ones physical replication can carry.
fn pages_described_by_wal(wal_path: &Path) -> std::collections::BTreeSet<u32> {
    use ferrodb::wal::log::{RecKind, WalManager};
    use std::sync::atomic::Ordering;

    let wal = WalManager::new(wal_path.to_path_buf()).expect("open wal");
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    let mut pages = std::collections::BTreeSet::new();
    while lsn < end {
        let Ok((rec, next)) = wal.read_record(lsn) else { break };
        match &rec.kind {
            RecKind::HeapInsert { page_id, .. }
            | RecKind::HeapDelete { page_id, .. }
            | RecKind::HeapUpdate { page_id, .. } => {
                pages.insert(*page_id);
            }
            _ => {}
        }
        lsn = next;
    }
    pages
}

fn page(bytes: &[u8], id: u32) -> Option<&[u8]> {
    let start = id as usize * PAGE_SIZE;
    bytes.get(start..start + PAGE_SIZE)
}

#[test]
fn a_replica_converges_on_the_primary_over_tcp() {
    let dir = tempfile::tempdir().unwrap();
    let pdb = dir.path().join("primary.db");
    let rdb = dir.path().join("replica.db");

    let primary = start_primary(&pdb, 40);

    let out = Command::new(example_bin("repl_replica"))
        .arg(&rdb)
        .arg(&primary.addr)
        .arg(primary.start_lsn.to_string())
        .output()
        .expect("spawn replica");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "replica failed: {stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The replica's own claim, which is necessary but not sufficient.
    assert!(stdout.contains("APPLIED"), "replica did not report progress: {stdout}");
    let applied: u64 = stdout
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("APPLIED lsn");
    assert_eq!(
        applied, primary.durable_lsn,
        "the replica stopped short of the primary's durable frontier"
    );

    // Now the part that actually decides it: the bytes on disk.
    let described = pages_described_by_wal(&dir.path().join("primary.db.wal"));
    assert!(
        !described.is_empty(),
        "the primary's WAL describes no heap pages, so convergence would be vacuous"
    );

    let p = std::fs::read(&pdb).expect("read primary");
    let r = std::fs::read(&rdb).expect("read replica");
    for id in &described {
        let pp = page(&p, *id).unwrap_or_else(|| panic!("primary lacks page {id}"));
        let rp = page(&r, *id).unwrap_or_else(|| panic!("replica lacks page {id}"));
        let differing = pp.iter().zip(rp).filter(|(a, b)| a != b).count();
        assert_eq!(
            differing, 0,
            "page {id} differs in {differing} bytes between primary and replica, despite the \
             replica reporting it had caught up"
        );
    }
}

/// **The limitation, asserted rather than left to be discovered.**
///
/// The catalog is written outside the WAL, so it is not replicated. A replica ends up with the
/// heap pages and no schema — the rows are there, and nothing on the replica can interpret them.
/// If this ever stops being true, this test fails and the claim in the module doc, the README and
/// DEMO.md all have to be revisited together.
#[test]
fn the_catalog_is_not_replicated_and_that_is_a_stated_limit() {
    let dir = tempfile::tempdir().unwrap();
    let pdb = dir.path().join("primary.db");
    let rdb = dir.path().join("replica.db");

    let primary = start_primary(&pdb, 20);
    let out = Command::new(example_bin("repl_replica"))
        .arg(&rdb)
        .arg(&primary.addr)
        .arg(primary.start_lsn.to_string())
        .output()
        .expect("spawn replica");
    assert!(out.status.success(), "replica failed");

    let described = pages_described_by_wal(&dir.path().join("primary.db.wal"));
    let p = std::fs::read(&pdb).expect("read primary");
    let r = std::fs::read(&rdb).expect("read replica");

    // Every page the WAL does NOT describe is untouched on the replica: it was materialised empty
    // and never written. That is the boundary of physical WAL replication in this engine.
    let total_pages = (p.len() / PAGE_SIZE) as u32;
    let mut unreplicated = Vec::new();
    for id in 1..total_pages {
        if described.contains(&id) {
            continue;
        }
        let (Some(pp), Some(rp)) = (page(&p, id), page(&r, id)) else { continue };
        if pp != rp {
            unreplicated.push(id);
        }
    }
    assert!(
        !unreplicated.is_empty(),
        "every page matched, including ones the WAL never described — if the catalog is now \
         replicated, the documented limitation is stale and must be rewritten"
    );
}
