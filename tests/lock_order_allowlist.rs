//! D23 — **who is allowed to take a page latch at all.** An allowlist, not a denylist.
//!
//! # What this guards, and why the other guard needs it
//!
//! `src/storage/page_latch.rs` asserts at runtime that no page latch is taken while the calling
//! thread is inside the buffer pool. That assertion can only see pool locks it was told about:
//! the `BufferPoolManager` methods that open a pool section, and `frame_read`/`frame_write`. The
//! tree has roughly forty *other* `frames[i].read()/.write()` sites — `heap_file_manager`,
//! `catalog`, `cow`, `wal/recovery`, `branch/arena` — and those are invisible to it.
//!
//! That is sound for exactly one reason: **none of those modules takes a page latch**, so none of
//! them can produce the `frame -> page latch` inversion. This test is what keeps that reason true.
//! Add a page latch to `heap_file_manager.rs` and this fails immediately, rather than the runtime
//! assertion silently not covering you.
//!
//! An allowlist rather than a denylist on purpose: a denylist would only name the modules someone
//! already thought of, and the whole point is to catch the module nobody has written yet.
//!
//! # If this test fails
//!
//! You added a page latch outside `src/storage/index.rs` / `src/storage/range_scan.rs`. Either
//! move the latching into the tree, or route every frame lock in your new module through
//! `BufferPoolManager::frame_read`/`frame_write` and add the file here — but read
//! `src/storage/page_latch.rs`'s ordering discipline first, because a second latching module also
//! has to respect "down the tree, rightward along the leaf chain, never up, never left".

use std::path::{Path, PathBuf};

/// The only modules permitted to acquire a page latch.
const ALLOWED: &[&str] = &["src/storage/index.rs", "src/storage/range_scan.rs"];

/// Defines the latch API and latches in its own unit tests; not a caller.
const DEFINES_THE_API: &str = "src/storage/page_latch.rs";

/// A floor on how many acquisitions the allowed files must contain.
///
/// Without it this test passes vacuously the day someone renames the accessor: zero matches
/// everywhere would read as "nobody latches", which is a clean bill of health for a tree that has
/// silently lost its latching. Measured at 16 sites when this was written (15 in `index.rs`, 1 in
/// `range_scan.rs`); the floor is set below that so ordinary refactoring does not trip it, but an
/// API rename or a wholesale removal does.
const MIN_EXPECTED_SITES: usize = 10;

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

/// Drop `//`-style comments so prose ABOUT latching does not read as latching. Every file in this
/// crate uses line comments; there are no `/* */` blocks to worry about.
fn strip_line_comments(src: &str) -> String {
    src.lines()
        .map(|l| match l.find("//") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Count page-latch acquisitions. Both spellings in the tree: `range_scan` reaches the pool field
/// directly, `index.rs` goes through its `latches()` helper.
fn latch_sites(code: &str) -> usize {
    const PATTERNS: &[&str] = &[
        "page_latches.read(",
        "page_latches.write(",
        "latches().read(",
        "latches().write(",
    ];
    code.lines()
        .filter(|l| PATTERNS.iter().any(|p| l.contains(p)))
        .count()
}

#[test]
fn only_the_tree_may_take_a_page_latch() {
    let src = Path::new("src");
    assert!(src.is_dir(), "run from the package root; `src/` not found");

    let mut files = Vec::new();
    rust_files(src, &mut files);
    assert!(!files.is_empty(), "walked src/ and found no .rs files - the walk is broken");

    let mut offenders: Vec<(String, usize)> = Vec::new();
    let mut allowed_sites = 0usize;

    for path in &files {
        // Normalise to forward slashes so the allowlist reads the same on Windows.
        let rel = path.to_string_lossy().replace('\\', "/");
        if rel == DEFINES_THE_API {
            continue;
        }
        let code = strip_line_comments(&std::fs::read_to_string(path).expect("read source"));
        let sites = latch_sites(&code);
        if sites == 0 {
            continue;
        }
        if ALLOWED.contains(&rel.as_str()) {
            allowed_sites += sites;
        } else {
            offenders.push((rel, sites));
        }
    }

    assert!(
        offenders.is_empty(),
        "page latches taken outside the allowlist: {offenders:?}.\n\
         The runtime lock-order assertion in src/storage/page_latch.rs cannot see frame locks \
         taken through raw `frames[i]` indexing, so it does NOT cover these modules. Read this \
         test's module doc before adding to ALLOWED."
    );

    assert!(
        allowed_sites >= MIN_EXPECTED_SITES,
        "found only {allowed_sites} page-latch acquisitions in {ALLOWED:?}, expected at least \
         {MIN_EXPECTED_SITES}. Either the tree stopped latching, or the accessor was renamed and \
         this guard is now searching for a spelling that no longer exists - which would make it \
         pass by finding nothing. Update PATTERNS."
    );
}

/// Split an `impl` block into methods at the 4-space `fn` boundary. Crude, and sufficient: every
/// method in `buffer_pool.rs` is written at that indentation.
fn methods_of(code: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for line in code.lines() {
        let is_sig = line.starts_with("    pub fn ") || line.starts_with("    fn ");
        if is_sig {
            let name = line.split("fn ").nth(1).unwrap_or(line).split('(').next().unwrap_or("?");
            out.push((name.trim().to_string(), String::new()));
        }
        if let Some(last) = out.last_mut() {
            last.1.push_str(line);
            last.1.push('\n');
        }
    }
    out
}

/// Does this method body take one of the buffer pool's own locks?
fn takes_a_pool_lock(body: &str) -> bool {
    const LOCKS: &[&str] =
        &["arc_cache.lock(", "page_table.read(", "page_table.write(", "frames["];
    LOCKS.iter().any(|p| body.contains(p))
}

/// **The other half of the runtime assertion: every pool method that locks must SAY so.**
///
/// The thread-local depth counter in `src/storage/page_latch.rs` only sees pool locks that opened
/// a pool section. A `BufferPoolManager` method that locks without calling `enter_pool()` is a
/// hole in it — and a silent one, because nothing about the method looks wrong.
///
/// This is aimed squarely at a rewrite of `fetch_page`: `S22-bufpool-latch` is reworking the
/// pool's locking, and a reworked method that drops the marker would restore the original,
/// undetectable inversion.
#[test]
fn every_pool_method_that_locks_opens_a_pool_section() {
    let whole = strip_line_comments(
        &std::fs::read_to_string("src/buffer/buffer_pool.rs").expect("read buffer_pool.rs"),
    );
    // Production methods only. The file's own `#[cfg(test)] mod tests` holds frame locks
    // deliberately (checking pin counts and frame contents) and takes no page latch, so a marker
    // there would be noise. That exemption is safe because those tests are still covered by
    // `only_the_tree_may_take_a_page_latch` above, which scans this file whole: a page latch
    // appearing in them is flagged there.
    let cut = whole.find("#[cfg(test)]").unwrap_or(whole.len());
    assert!(cut > 1000, "truncation point looks wrong - this guard would inspect almost nothing");
    let code = &whole[..cut];
    let methods = methods_of(code);
    assert!(
        methods.len() > 5,
        "parsed only {} methods out of buffer_pool.rs - the splitter is broken, so this guard \
         would pass by inspecting nothing",
        methods.len()
    );

    let mut locking = 0usize;
    let missing: Vec<&str> = methods
        .iter()
        .filter(|(_, body)| takes_a_pool_lock(body))
        .inspect(|_| locking += 1)
        .filter(|(_, body)| !body.contains("enter_pool()"))
        .map(|(name, _)| name.as_str())
        .collect();

    assert!(
        missing.is_empty(),
        "BufferPoolManager methods take a pool lock without opening a pool section: {missing:?}.\n\
         Add `let _pool = enter_pool();` as the first statement. Without it the lock-order \
         assertion in src/storage/page_latch.rs cannot see that this thread is inside the pool, \
         and a page latch taken from here deadlocks silently instead of failing a test."
    );
    assert!(
        locking >= 8,
        "only {locking} locking methods found in buffer_pool.rs, expected at least 8 - the lock \
         patterns this guard searches for have probably been renamed, so it is now checking \
         nothing"
    );
}

/// Force the detector to fire: the scanner must actually flag a latch acquisition in a module that
/// is not on the allowlist. Without this, a broken `latch_sites` would make the test above pass by
/// matching nothing at all.
#[test]
fn the_scanner_catches_an_unlisted_latcher() {
    let planted = "fn sneaky(bp: &BufferPoolManager) {\n    let _g = bp.page_latches.write(7);\n}\n";
    assert_eq!(latch_sites(&strip_line_comments(planted)), 1, "the scanner missed a real acquisition");

    let commented = "// let _g = bp.page_latches.write(7);\n//! mentions page_latches.read( in prose\n";
    assert_eq!(
        latch_sites(&strip_line_comments(commented)),
        0,
        "the scanner fired on a comment - it would flag every file that merely discusses latching"
    );
}

/// Same treatment for the pool-section scanner: plant a method that locks without a marker and
/// confirm it is seen, then one that has the marker and confirm it is not.
#[test]
fn the_scanner_catches_a_pool_method_missing_its_marker() {
    let unmarked = "    pub fn sloppy(&self) {\n        let f = self.frames[0].write().unwrap();\n    }\n";
    let parsed = methods_of(unmarked);
    assert_eq!(parsed.len(), 1, "the method splitter missed a method");
    assert!(takes_a_pool_lock(&parsed[0].1), "the lock scanner missed a real frame lock");
    assert!(!parsed[0].1.contains("enter_pool()"), "planted method should have no marker");

    let marked = "    pub fn tidy(&self) {\n        let _pool = enter_pool();\n        let f = self.frames[0].write().unwrap();\n    }\n";
    let parsed = methods_of(marked);
    assert!(takes_a_pool_lock(&parsed[0].1) && parsed[0].1.contains("enter_pool()"));
}
