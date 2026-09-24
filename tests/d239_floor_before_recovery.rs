//! D239 — the arena region must be reserved BEFORE recovery runs.
//!
//! `open_recovered` opens the file, runs `recover`, opens the catalog, rebuilds every index and
//! checkpoints, and every entry point only THEN attaches the arena store, whose `reopen` is the
//! only thing that tells the page allocator where the arena starts. Until then the allocator knows
//! no region, and arena pages never set bitmap bits, so every one of them reads as free. A rebuild
//! that needs more pages than the table region has left is handed arena pages, writes index nodes
//! over live branch data, the checkpoint makes that durable, and `reopen`'s bitmap check then
//! refuses this open and every later one. No crash is needed: a clean exit after any DDL makes the
//! next open rebuild (D216), and a secondary index whose backfill split its root records a root
//! that frees one page of a many-page tree (D222), so that rebuild needs more pages than it frees.
//! `frontier/d229_candidates_adversary.md` E1 (artie-research `b3c3469`).
//!
//! The behavioural test drives the shipped binary. Every binary opens through D204's
//! `wal::recovery::open_recovered` (`tests/open_path_allowlist.rs`), so the order is also pinned by
//! reading that one function's source.

use std::path::Path;
use std::process::{Command, Output, Stdio};

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::storage::disk_manager::PAGE_SIZE;

/// Pages of table region below the arena floor. Small enough that one session fills it.
const HEADROOM: u32 = 512;
/// Rows behind the secondary index: enough that its backfill splits the root (D222).
const INDEXED_ROWS: u32 = 2_000;
/// Wide rows inserted until the table region refuses. About four fit a page, so this is well past
/// the headroom; every insert after the region is full errors, which is the point.
const FILLER_ROWS: u32 = 3_000;

const REGION_FULL: &str = "no free page below the reserved arena region";
const OVERLAP_REFUSAL: &str = "overlaps pages the bitmap allocator owns";

/// One stdout line with the CLI's prompts taken off. The binary prints `ferrodb=> ` with no newline
/// before reading each line (`run_cli`'s read loop), so a statement's output lands on its prompt's
/// line, and a statement that printed only to stderr leaves its prompt in front of the next one.
fn without_prompts(line: &str) -> &str {
    line.trim().trim_start_matches("ferrodb=> ").trim()
}

/// Run the real binary on `db` with `sql` as its stdin. The SQL goes through a file, not a pipe:
/// a pipe written in full before the output is read deadlocks once the child's stdout fills.
fn ferrodb(db: &Path, sql: &str, tag: &str) -> Output {
    let input = db.with_extension(format!("{tag}.sql"));
    std::fs::write(&input, sql).expect("write the session's SQL");
    Command::new(env!("CARGO_BIN_EXE_ferrodb"))
        .arg(db)
        .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
        .stdin(Stdio::from(std::fs::File::open(&input).expect("open the session's SQL")))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run ferrodb")
}

/// The bytes of every page from `base` to the end of the longer of the two files, the shorter one
/// read as zeros past its end. A page the arena never wrote reads as zeros either way, so a spill
/// that EXTENDS the file into the region is caught as well as one that overwrites.
fn changed_pages(before: &[u8], after: &[u8], base: u32) -> Vec<u32> {
    let from = base as usize * PAGE_SIZE;
    let end = before.len().max(after.len());
    let page = |b: &[u8], at: usize| -> Vec<u8> {
        let mut p = b.get(at..(at + PAGE_SIZE).min(b.len())).unwrap_or(&[]).to_vec();
        p.resize(PAGE_SIZE, 0);
        p
    };
    (from..end)
        .step_by(PAGE_SIZE)
        .filter(|&at| page(before, at) != page(after, at))
        .map(|at| (at / PAGE_SIZE) as u32)
        .collect()
}

/// **D239.** A full table region, then an open that rebuilds, must not write into the arena.
///
/// Session one fills the table region after creating an index whose backfill splits its root, and
/// exits cleanly. Session two only opens: its recovery rebuilds every index, and the rebuild needs
/// more pages than it frees. With the region unreserved during recovery those pages come from the
/// arena. With it reserved, the rebuild refuses with the table-region-full message and the arena
/// is untouched. That open still fails part-way through its rebuild (D229 schedule (a), not this
/// row). The refusal is also the fixture's proof that the rebuild really asked for more pages than
/// the region had, so an open that succeeds fails the premise check at the end.
#[test]
fn an_open_that_rebuilds_into_a_full_table_region_never_writes_the_arena() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("d239.db");

    let mut sql = String::from("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);\n");
    for i in 1..=INDEXED_ROWS {
        sql.push_str(&format!("INSERT INTO t VALUES ({i}, {});\n", INDEXED_ROWS - i));
    }
    sql.push_str("CREATE INDEX ix ON t (v);\n");
    sql.push_str("CREATE TABLE f (id INTEGER NOT NULL, pad VARCHAR(1000));\n");
    let pad = "x".repeat(900);
    for i in 1..=FILLER_ROWS {
        sql.push_str(&format!("INSERT INTO f VALUES ({i}, '{pad}');\n"));
    }
    let first = ferrodb(&db, &sql, "fill");
    let out = String::from_utf8_lossy(&first.stdout).to_string();
    let err = String::from_utf8_lossy(&first.stderr).to_string();
    assert!(first.status.success(), "fixture: session one failed: {err}");
    assert_eq!(
        out.lines().filter(|l| without_prompts(l) == "ok").count(),
        3,
        "fixture: CREATE TABLE t, CREATE INDEX and CREATE TABLE f must all succeed: {err}"
    );
    assert!(
        out.lines().filter(|l| without_prompts(l) == "(1 row affected)").count()
            >= INDEXED_ROWS as usize,
        "fixture: not every indexed row went in"
    );
    let errors: Vec<&str> = err.lines().filter(|l| l.starts_with("error:")).collect();
    assert!(!errors.is_empty(), "fixture: the filler never filled the table region");
    assert!(
        errors.iter().all(|l| l.contains(REGION_FULL)),
        "fixture: an error other than a full table region: {errors:?}"
    );

    let arena = std::fs::read(format!("{}.arena", db.display())).expect("the arena checkpoint");
    let base = ArenaPageStore::base_page_in_state(&arena).expect("the arena's base");
    let before = std::fs::read(&db).unwrap();

    let second = ferrodb(&db, "", "reopen");
    let err2 = String::from_utf8_lossy(&second.stderr).to_string();
    let after = std::fs::read(&db).unwrap();

    let changed = changed_pages(&before, &after, base);
    assert!(
        changed.is_empty(),
        "D239: reopening wrote arena page(s) {changed:?} (the arena starts at {base}): recovery's \
         rebuild was handed arena pages because the region was not yet reserved. stderr: {err2}"
    );
    assert!(
        !err2.contains(OVERLAP_REFUSAL),
        "D239: the reopen was refused because the bitmap now owns arena pages, which it can only \
         have taken during recovery: {err2}"
    );
    // The premise, checked last so that a claim failure above reports first: the reopen's rebuild
    // needed more pages than the table region had, so it was refused for a full region (D229
    // schedule (a) is why it fails rather than finishing). An open that SUCCEEDS means the rebuild
    // fit, and then this fixture never reached the allocation D239 is about: a fix to D216 (no
    // rebuild after a clean exit) or D222 (the rebuild frees the whole old tree) does that. Then
    // re-derive the fixture; do not read the assertions above as evidence.
    assert!(
        !second.status.success() && err2.contains(REGION_FULL),
        "premise: the reopen was not refused for a full table region, so its rebuild never asked \
         for a page the region did not have and this test proved nothing about D239. stderr: {err2}"
    );
}

/// Source lines with whole-line comments removed, so prose mentioning a call cannot satisfy or
/// break the order check.
fn code_of(path: &str) -> String {
    let text = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap_or_else(|e| panic!("read {path}: {e}"));
    text.lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// **D239.** The one open path registers the persisted arena floor before `recover`.
///
/// `tests/open_path_allowlist.rs` keeps every binary on `wal::recovery::open_recovered` and keeps
/// `recover(` out of everything else, so pinning the order inside that one body pins it for the
/// CLI, pgserver, `table_dump` and `crash_mid_merge` alike. pgserver never rebuilds, so the binary
/// test above cannot reach it: its exposure is recovery's own allocation (`add_to_directory` takes
/// a new page when a directory page is full).
///
/// The body is cut the way the allowlist cuts it: from `pub fn open_recovered(` to the first `}` at
/// column 0 after it.
#[test]
fn the_open_path_reserves_the_arena_floor_before_recovery() {
    let code = code_of("src/wal/recovery.rs");
    let start = code
        .find("pub fn open_recovered(")
        .expect("src/wal/recovery.rs has no `open_recovered`; point this check at the open path");
    let rest = &code[start..];
    let end = rest.find("\n}").expect("`open_recovered` has no closing brace at column 0");
    let body = &rest[..end];
    let recover_at = body
        .find("recover(&txn)")
        .expect("`open_recovered` no longer calls `recover(&txn)`; this check no longer fits it");
    match body.find("reserve_persisted_floor(") {
        Some(reserve_at) => assert!(
            reserve_at < recover_at,
            "D239: `open_recovered` reserves the arena floor only after recovery has run"
        ),
        None => panic!("D239: `open_recovered` never reserves the arena floor before recovery"),
    }
}
