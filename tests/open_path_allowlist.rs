//! D204 — **one open path runs recovery, and every binary uses it.** An allowlist, in the shape of
//! `tests/lock_order_allowlist.rs`.
//!
//! # What this guards
//!
//! `wal::recovery::recover` redoes and undoes heap records and nothing else. Index pages are not
//! logged, so a database is correct after recovery only once every tree has been rebuilt from the
//! recovered heap (`rebuild_indexes`). Until D204 each entry point spelled the sequence out for itself,
//! and two of them drifted. `cli::run_cli` rebuilt; `examples/pgserver.rs` never had, since D9. So
//! after a crash, pgserver lost committed rows by key and admitted a second row under their key
//! (`tests/pgserver_crash_rebuilds_indexes.rs`). Now there is one function,
//! `wal::recovery::open_recovered`, and this test keeps it the only way in:
//!
//! - no file under `src/` or `examples/` calls `recover(` or `Catalog::open(` except
//!   `src/wal/recovery.rs`, which defines the sequence. `Catalog::open(` is here as well because a
//!   binary that opens a catalog without recovering at all is the same hazard, one step earlier;
//! - both production entry points, `src/cli/cli.rs` and `examples/pgserver.rs`, call
//!   `open_recovered(`. Without this floor, deleting every opener would pass;
//! - `open_recovered`'s own body calls `recover(` and THEN `rebuild_indexes(`.
//!
//! An allowlist and not a denylist, for `lock_order_allowlist.rs`'s reason: the entry point this
//! has to catch is the one nobody has written yet.
//!
//! # Blind spots, stated
//!
//! - `tests/` is not scanned. Tests call `recover` directly, because recovery is what they test.
//! - Text after a file's first `#[cfg(test)]` line is not scanned. `src/` files keep their unit
//!   tests in a trailing `mod tests` that opens databases by hand (`catalog.rs`, `executor.rs`).
//!   Production code written after a test module would be invisible to this.
//! - It reads text. A call made through a re-export under another name, or built by a macro, is
//!   not seen.
//!
//! # If this fails
//!
//! You opened a database without `open_recovered`. Call it instead. If a binary genuinely must not
//! rebuild, add it to `ALLOWED` with the reason, and expect that reason to be read.

use std::path::{Path, PathBuf};

/// The only file that may run recovery or open a catalog directly.
const ALLOWED: &[&str] = &["src/wal/recovery.rs"];

/// Production entry points. Each must open through the shared function.
const ENTRY_POINTS: &[&str] = &["src/cli/cli.rs", "examples/pgserver.rs"];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}")) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The production part of a file: CRLF normalised, stopped at the first `#[cfg(test)]` line, and
/// with `//` comments dropped so prose ABOUT recovery does not read as a call.
fn production_text(src: &str) -> String {
    let mut out = String::new();
    for line in src.replace("\r\n", "\n").lines() {
        if line.trim() == "#[cfg(test)]" {
            break;
        }
        out.push_str(match line.find("//") {
            Some(i) => &line[..i],
            None => line,
        });
        out.push('\n');
    }
    out
}

fn line_at(code: &str, at: usize) -> String {
    let start = code[..at].rfind('\n').map_or(0, |i| i + 1);
    let end = code[at..].find('\n').map_or(code.len(), |i| at + i);
    code[start..end].trim().to_string()
}

/// Calls of the free function `recover(` and of `Catalog::open(`.
///
/// Not counted: `fn recover(` (the definition), `.recover(` (a method: `cow::dedup` has one), a
/// longer identifier ending in `recover`, and `LogBranchCatalog::open(`/`TableBranchCatalog::open(`
/// (branch catalogs, which are not the table catalog). A path call such as
/// `recovery::recover(` IS counted: `:` is not an identifier character.
fn open_sites(code: &str) -> Vec<String> {
    let mut sites = Vec::new();
    for (at, _) in code.match_indices("recover(") {
        let before = &code[..at];
        if before.chars().next_back().is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            continue;
        }
        if before.trim_end().ends_with("fn") {
            continue;
        }
        sites.push(line_at(code, at));
    }
    for (at, _) in code.match_indices("Catalog::open(") {
        if code[..at].chars().next_back().is_some_and(|c| c.is_alphanumeric() || c == '_') {
            continue;
        }
        sites.push(line_at(code, at));
    }
    sites
}

fn rel(root: &Path, p: &Path) -> String {
    p.strip_prefix(root).unwrap_or(p).to_string_lossy().replace('\\', "/")
}

#[test]
fn only_the_open_path_runs_recovery_or_opens_a_catalog() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_files(&root.join("src"), &mut files);
    rust_files(&root.join("examples"), &mut files);
    assert!(files.len() > 20, "walked src/ and examples/ and found {} .rs files: the walk is broken", files.len());

    let mut offenders = Vec::new();
    let mut allowed_sites = 0;
    for f in &files {
        let name = rel(root, f);
        let code = production_text(&std::fs::read_to_string(f).unwrap());
        let sites = open_sites(&code);
        if ALLOWED.contains(&name.as_str()) {
            allowed_sites += sites.len();
        } else {
            offenders.extend(sites.into_iter().map(|s| format!("{name}: {s}")));
        }
    }
    assert!(
        offenders.is_empty(),
        "these open a database without `wal::recovery::open_recovered`, so after a crash they can \
         run with index trees the recovered heap no longer matches:\n  {}",
        offenders.join("\n  ")
    );
    // Anti-vacuity: the allowed file really does call both, so a renamed function cannot turn every
    // site into zero and this test into a clean bill of health.
    assert!(
        allowed_sites >= 2,
        "found {allowed_sites} call(s) of `recover(`/`Catalog::open(` in src/wal/recovery.rs outside its \
         tests; `open_recovered` should hold one of each. The scanner or the function has moved"
    );
}

#[test]
fn both_production_entry_points_open_through_the_shared_function() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for entry in ENTRY_POINTS {
        let code = production_text(&std::fs::read_to_string(root.join(entry)).unwrap_or_else(|e| {
            panic!("{entry} is not where this test expects an entry point: {e}")
        }));
        assert!(
            code.contains("open_recovered("),
            "{entry} does not call `open_recovered`, so it opens the database some other way"
        );
    }
}

#[test]
fn the_shared_function_recovers_and_then_rebuilds() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let code = production_text(&std::fs::read_to_string(root.join("src/wal/recovery.rs")).unwrap());
    let start = code
        .find("pub fn open_recovered(")
        .expect("`pub fn open_recovered(` is not in src/wal/recovery.rs: renamed or moved");
    let body = &code[start..];
    let body = &body[..body.find("\n}\n").expect("unterminated open_recovered")];
    let recovered = body.find("recover(&").expect("open_recovered does not call recover");
    let rebuilt = body.find("rebuild_indexes(").expect("open_recovered does not call rebuild_indexes");
    assert!(recovered < rebuilt, "open_recovered rebuilds the indexes BEFORE recovering the heap they are built from");
}

/// Positive and negative controls for the scanner, on text written here, so a scanner that
/// matched nothing (or everything) cannot pass the tests above.
#[test]
fn the_scanner_catches_a_bare_opener_and_ignores_everything_else() {
    let counts = |src: &str| open_sites(&production_text(src)).len();
    assert_eq!(counts("let r = recover(&txn)?;"), 1, "a bare call was missed");
    assert_eq!(counts("let r = ferrodb::wal::recovery::recover(&txn)?;"), 1, "a path call was missed");
    assert_eq!(counts("let c = Catalog::open(bp.clone(), 1)?;"), 1, "a catalog open was missed");
    assert_eq!(counts("let c = crate::catalog::catalog::Catalog::open(bp, 1)?;"), 1, "a path catalog open was missed");

    assert_eq!(counts("pub fn recover(txn: &TxnManager) -> Result<bool, FerroError> {"), 0, "the definition was counted");
    assert_eq!(counts("let lost = self.dedup.recover();"), 0, "a method called recover was counted");
    assert_eq!(counts("let c = LogBranchCatalog::open(&p, 1)?;"), 0, "a branch catalog was counted");
    assert_eq!(counts("let c = TableBranchCatalog::open(pool, root)?;"), 0, "a branch catalog was counted");
    assert_eq!(counts("let db = open_recovered(&path, &lock)?;"), 0, "the shared function was counted");
    assert_eq!(counts("    // recover(&txn) is what this used to do"), 0, "a comment was counted");
    assert_eq!(
        counts("let a = recover(&txn)?;\n#[cfg(test)]\nmod tests {\n    fn t() { recover(&txn); }\n}\n"),
        1,
        "the cut at #[cfg(test)] is wrong: it must count the call before it and none after"
    );
}
