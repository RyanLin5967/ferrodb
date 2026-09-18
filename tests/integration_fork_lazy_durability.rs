//! A fork that never wrote must leave **nothing** behind a SIGKILL. (`SCALE-DESIGN.md` D6, opt 2)
//!
//! This is the falsifier the design entry names, and it was written and watched FAIL before the
//! feature existed. `TableBranchCatalog::fork` used to fsync before returning, so every one of
//! these speculative forks was on disk and all four assertions below failed.
//!
//! **The contract it pins down.** Durability moved to the branch's FIRST WRITE. A fork that has
//! written nothing is allowed to vanish in a crash, because the loss is observably identical to
//! the branch being reaped a moment later — generations make a stale handle a hard
//! `BranchError::Reaped`, and reaping is already non-cooperative and lease-driven, so the contract
//! already permits it. What is NOT allowed is a *partial* fork surviving: an id that was minted,
//! a page parked in the file, or an index entry pointing at a branch with no record. Each of
//! those is a GC correctness hole, and each is asserted against here.
//!
//! The companion test `integration_fork_kill9` asserts the other half — that a fork which DID
//! write survives — and the two together are the whole durability contract.
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;

fn example_bin(name: &str) -> PathBuf {
    let mut p = std::env::current_exe().expect("test exe");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    let out = p.join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    let bin_time = std::fs::metadata(&out)
        .unwrap_or_else(|e| panic!("{} missing ({e}); run: cargo build --examples", out.display()))
        .modified()
        .expect("mtime");
    // Same staleness guard the other example-spawning tests use: `cargo test` does not rebuild
    // examples, and a stale victim would be testing yesterday's durability path.
    if let Ok(src) = std::fs::metadata("src/branch/table_catalog.rs").and_then(|m| m.modified()) {
        assert!(
            bin_time >= src,
            "{} is older than src/branch/table_catalog.rs — run: cargo build --examples",
            out.display()
        );
    }
    out
}

#[test]
fn a_fork_that_never_wrote_leaves_nothing_behind_sigkill() {
    let dir = std::env::temp_dir().join(format!("ferrodb-lazykill9-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("tmp dir");
    let cat_path = dir.join("branches.branchcat");

    // Create the catalog here, in this process, and close it. The victim then only ever OPENS it,
    // so every byte that differs afterwards was written by a fork.
    //
    // The explicit flush+sync is not tidiness: `create_with_header` leaves the header and the
    // tree's first pages DIRTY IN THE POOL, and nothing on the create path fsyncs. Dropping the
    // catalog without this leaves a file holding only the allocation bitmap, and the victim's
    // reopen then fails the header-magic check — which is how this harness first "failed", with
    // zero acknowledged forks and every assertion below vacuous.
    {
        let cat = TableBranchCatalog::open_sidecar(&cat_path, 1).expect("create catalog");
        let pool = cat.pool_handle();
        pool.flush_all().expect("flush fresh catalog");
        pool.disk_manager.sync().expect("sync fresh catalog");
        drop(cat);
    }
    let before = std::fs::read(&cat_path).expect("read fresh catalog");
    assert!(!before.is_empty(), "fresh catalog file is empty; the harness created nothing");

    let mut child = Command::new(example_bin("fork_no_write_kill9"))
        .arg(&cat_path)
        .arg("8")
        .arg("64")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn fork_no_write_kill9");

    let mut out = child.stdout.take().expect("stdout");
    let mut acked = String::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut buf = [0u8; 4096];
    while !acked.contains("QUIESCED") && std::time::Instant::now() < deadline {
        match out.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => acked.push_str(&String::from_utf8_lossy(&buf[..n])),
            Err(_) => break,
        }
    }
    assert!(acked.contains("QUIESCED"), "victim never quiesced; acks so far: {}", acked.lines().count());
    child.kill().expect("SIGKILL");
    let _ = child.wait();

    let ids: Vec<u64> = acked.lines().filter_map(|l| l.trim().parse().ok()).collect();
    // A run that collected nothing has not passed: with zero acknowledged forks every assertion
    // below is vacuous, which is the exact shape of false green this repo keeps finding.
    assert_eq!(ids.len(), 512, "expected 8x64 acknowledged forks, got {}", ids.len());

    // ---- 1. NO PARKED PAGE. The strongest form available: the file is byte-identical. -----------
    let after = std::fs::read(&cat_path).expect("read catalog after kill");
    assert_eq!(
        after.len(),
        before.len(),
        "the catalog file GREW by {} bytes across {} forks that wrote nothing",
        after.len() as i64 - before.len() as i64,
        ids.len()
    );
    if after != before {
        let first = (0..before.len()).find(|&i| after[i] != before[i]).unwrap();
        panic!(
            "the catalog file CHANGED at byte {first} (page {}) across {} forks that wrote \
             nothing — a speculative fork parked something on disk",
            first / 4096,
            ids.len()
        );
    }

    // ---- 2. NO ORPHAN ID, and 3. NO INDEX ENTRY. ------------------------------------------------
    let cat = TableBranchCatalog::open_sidecar(Path::new(&cat_path), 1).expect("reopen catalog");
    let survivors: Vec<u64> = ids.iter().copied().filter(|&id| cat.get_raw(id).is_ok()).collect();
    assert!(
        survivors.is_empty(),
        "{} of {} forks that wrote NOTHING survived SIGKILL (first few: {:?}) — a speculative fork \
         is still paying for durability it does not need",
        survivors.len(),
        ids.len(),
        &survivors[..survivors.len().min(8)]
    );
    assert!(
        !cat.has_live_children(BranchId::TRUNK.id).expect("has_live_children"),
        "trunk still lists live children after a crash that lost every one of them — the CHILD \
         index outlived the records it points at, which is the GC correctness hole"
    );
    assert_eq!(
        BranchCatalog::live_count(&cat),
        1,
        "live_count is not 1 (trunk alone) after every speculative fork was lost"
    );

    // ---- 4. The catalog is not merely broken: it still works, and the ids come back. ------------
    assert!(cat.get_raw(BranchId::TRUNK.id).is_ok(), "trunk unreadable after reopen");
    let fresh = cat.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).expect("fork after reopen");
    assert_eq!(
        fresh.branch_id.id, 1,
        "next_id advanced past the lost forks: the first fork after the crash got id {} instead \
         of 1, so {} ids were minted durably by forks that left no record",
        fresh.branch_id.id,
        fresh.branch_id.id - 1
    );

    let _ = std::fs::remove_dir_all(&dir);
}
