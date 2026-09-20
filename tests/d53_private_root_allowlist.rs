//! D53 — **who is allowed to open a B+tree with a PRIVATE root cell.** An allowlist, not a denylist.
//!
//! # What this guards
//!
//! `BPlusTreeManager::open(root, bp)` wraps the given root in a fresh, private `AtomicU32`. Two
//! handles opened that way over one tree hold two independent root pointers, so a split through
//! one is invisible to the other and the root-split retry in `read_leaf_for` compares a private
//! value against itself. D53 fixed that with a shared cell (`Catalog::root_cell` +
//! `open_shared`) — and then wired it into `plan::open_table` **and missed the optimizer's index-
//! scan path and full-text search**, which is the path every point lookup takes. The D53 wiring
//! test checked that the registry hands out one cell; it never checked that every statement-path
//! caller *asks* for it. This test does.
//!
//! `open()` is still legitimate for a caller that owns the tree ALONE for the duration: recovery
//! (nothing else is running), ALTER's in-place rewrite (under the exclusive catalog lock),
//! `drop_table`'s `free_all` (same), the `None =>` fallback for a tree the catalog has not
//! registered (nothing else can hold a handle on it either), and tests that build their own tree.
//! Everything else is a statement path, and a statement path must share.
//!
//! An allowlist rather than a denylist on purpose: a denylist names the sites someone already
//! found, and the whole point is the site nobody has written yet.
//!
//! # If this test fails
//!
//! You opened a B+tree with a private root cell somewhere a concurrent statement can also open
//! it. Use `catalog.root_cell(table, column)` + `open_shared`, with `open` only as the `None =>`
//! fallback. Read this module doc before adding to ALLOWED.

use std::path::{Path, PathBuf};

/// Files where a private-cell `open(` is permitted, with the reason each is a sole owner.
const ALLOWED: &[&str] = &[
    "src/storage/index.rs",         // defines the API, and its own tests
    "src/wal/recovery.rs",          // nothing else runs during recovery
    "src/catalog/alter.rs",         // in-place rewrite under the exclusive catalog lock
    "src/catalog/catalog.rs",       // drop_table's free_all, under the exclusive lock
    "src/storage/index_fulltext.rs",// builds the tree it returns; the caller registers it
    "src/branch/table_catalog.rs",  // the branch catalog's own long-lived tree (one handle)
];

/// A floor on the number of `open_shared` sites, so a rename of the shared constructor cannot
/// make this guard pass by finding nothing to complain about.
const MIN_SHARED_SITES: usize = 6;

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

fn strip_line_comments(src: &str) -> String {
    src.lines()
        .map(|l| match l.find("//") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Private-cell openings that are NOT the `None =>` fallback of a shared lookup.
fn private_sites(code: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in code.lines() {
        let is_open = line.contains("BPlusTreeManager::<") && line.contains(">::open(")
            || line.contains("BPlusTreeManager::open(");
        if !is_open {
            continue;
        }
        // The sanctioned fallback shape: `None => BPlusTreeManager::<..>::open(..)`, reachable
        // only for a tree the catalog never registered.
        if line.trim_start().starts_with("None =>") {
            continue;
        }
        out.push(line.trim().to_string());
    }
    out
}

fn shared_sites(code: &str) -> usize {
    code.matches("::open_shared(").count()
}

#[test]
fn every_statement_path_opens_the_shared_root_cell() {
    let mut files = Vec::new();
    rust_files(Path::new("src"), &mut files);
    let mut offenders: Vec<(String, Vec<String>)> = Vec::new();
    let mut shared = 0;
    for path in files {
        let rel = path.to_string_lossy().replace('\\', "/");
        let code = strip_line_comments(&std::fs::read_to_string(&path).expect("read source"));
        shared += shared_sites(&code);
        // Test modules build their own trees and are sole owners. Only the part of the file
        // BEFORE `#[cfg(test)]` is a statement path.
        let non_test = match code.find("#[cfg(test)]") {
            Some(i) => &code[..i],
            None => &code[..],
        };
        let sites = private_sites(non_test);
        if sites.is_empty() || ALLOWED.contains(&rel.as_str()) {
            continue;
        }
        offenders.push((rel, sites));
    }

    assert!(
        offenders.is_empty(),
        "B+tree opened with a PRIVATE root cell on a statement path: {offenders:#?}\n\
         Two handles over one tree with private cells cannot see each other's root splits, and \
         the retry in read_leaf_for compares a private value against itself. Use \
         catalog.root_cell(..) + open_shared, keeping open() only as the `None =>` fallback. \
         Read this test's module doc before adding to ALLOWED."
    );

    assert!(
        shared >= MIN_SHARED_SITES,
        "found only {shared} open_shared sites, expected at least {MIN_SHARED_SITES}. Either the \
         shared constructor was renamed and this guard is searching for a spelling that no longer \
         exists, or the statement paths stopped sharing -- both of which would make this pass by \
         finding nothing. Update the pattern or the floor deliberately."
    );
}
