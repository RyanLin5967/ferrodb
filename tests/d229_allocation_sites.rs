//! D229 §7 F6 — **every site that takes a page from the main file's allocator is on a list.** An
//! allowlist, in the shape of `tests/lock_order_allowlist.rs`.
//!
//! # What this guards
//!
//! The recovery rebuild no longer walks an old tree to free it. Before it builds, a reset frees
//! every allocated page that no logged structure reaches (the bitmap chain, the catalog chain, each
//! table's heap and time-travel directories and the pages they list) and whose bytes are a B+tree
//! node or all zeros (SCALE-DESIGN "D229" (b)). That identity test is sound only while the
//! B+trees are the ONLY owners of main-file pages outside that keep set. The design review
//! enumerated every owner from the allocator's callers and found exactly that
//! (`frontier/d229_design_review.md` §1.1), and said this tripwire is what keeps it true (§1.2):
//! a new owner the keep set does not know about, whose pages can be zeros, would be freed by the
//! next rebuilding open while it still holds data.
//!
//! So each file that takes a page, and how many sites it has, is listed with the reason its pages
//! are safe from the reset. A new site, in a listed file or a new one, fails here until someone
//! has answered that question for it.
//!
//! # What counts as a site
//!
//! In a file's production text (`//` comments removed, and every item under `#[cfg(test)]`
//! removed; files named `tests_*.rs` are test-only by `lock_order_allowlist`'s convention):
//! - `new_page()`, a buffer pool page;
//! - `.allocate()`, a page straight from a `DiskManager`;
//! - a line naming `BPlusTreeManager` with `::create(`, a new tree's root.
//!
//! # Blind spots, stated
//!
//! - Spelling, not types: a site reached through another name (a wrapper, a function pointer) is
//!   not seen until the wrapper's own site is, and the wrapper's file is on the list.
//! - The `#[cfg(test)]` cut reads braces and `;`, skipping string literals but not char literals or
//!   raw strings. A `'{'` inside a test item mis-cuts it, which changes a count and fails here:
//!   the loud direction.

use std::path::{Path, PathBuf};

/// Each file with a site, its number of sites, and why the reset cannot free a live page of it.
const ALLOWED: &[(&str, usize, &str)] = &[
    ("src/branch/table_catalog.rs", 2, "the branch catalog's own pool: the sidecar `{db}.branchcat` in production, never the main file"),
    ("src/buffer/buffer_pool.rs", 1, "`new_page` itself, the one door to the main file's allocator"),
    ("src/catalog/catalog.rs", 5, "the catalog chain (in the keep set) and each table's primary, B-tree and full-text roots (rebuilt)"),
    ("src/cow/store.rs", 2, "`CowStore` extents: constructed only by tests and examples (review §1.1)"),
    ("src/storage/heap_file_manager.rs", 3, "heap directories and data pages: in the keep set through the catalog"),
    ("src/storage/index.rs", 4, "B+tree nodes: the root, a leaf split, a root split, an internal split (rebuilt)"),
    ("src/wal/recovery.rs", 3, "the rebuild's fresh trees"),
];

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

/// Where the item that starts `s` ends: its first `;` at depth 0, or the brace that closes its
/// first block.
fn item_end(s: &str) -> usize {
    let (mut depth, mut in_str, mut escaped) = (0i32, false, false);
    for (i, c) in s.char_indices() {
        if in_str {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_str = false;
            }
            continue;
        }
        match c {
            '"' => in_str = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return i + 1;
                }
            }
            ';' if depth == 0 => return i + 1,
            _ => {}
        }
    }
    s.len()
}

fn production_text(src: &str) -> String {
    let code: String = src
        .lines()
        .map(|l| match l.find("//") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n");
    const TAG: &str = "#[cfg(test)]";
    let mut out = String::new();
    let mut rest = code.as_str();
    while let Some(at) = rest.find(TAG) {
        out.push_str(&rest[..at]);
        let item = &rest[at + TAG.len()..];
        rest = &item[item_end(item)..];
    }
    out.push_str(rest);
    out
}

fn sites(code: &str) -> usize {
    code.matches("new_page()").count()
        + code.matches(".allocate()").count()
        + code.lines().filter(|l| l.contains("BPlusTreeManager") && l.contains("::create(")).count()
}

#[test]
fn every_page_taken_from_the_main_allocator_is_on_the_list() {
    let mut files = Vec::new();
    rust_files(Path::new("src"), &mut files);
    assert!(files.len() > 20, "walked src/ and found {} .rs files: the walk is broken", files.len());

    let mut found: Vec<(String, usize)> = Vec::new();
    for path in files {
        let rel = path.to_string_lossy().replace('\\', "/");
        if path.file_name().is_some_and(|n| n.to_string_lossy().starts_with("tests_")) {
            continue;
        }
        let n = sites(&production_text(&std::fs::read_to_string(&path).expect("read source")));
        if n > 0 {
            found.push((rel, n));
        }
    }
    found.sort();
    let listed: Vec<(String, usize)> = ALLOWED.iter().map(|(f, n, _)| (f.to_string(), *n)).collect();
    assert_eq!(
        found, listed,
        "the sites that take a page from the main file's allocator changed. For each new or moved \
         site, answer the question in this file's module doc (can the recovery reset free a live page \
         of it?) before updating ALLOWED"
    );
}

/// The scanner fires on a planted site, cuts a test item, and ignores a comment, so the list above
/// cannot pass by the scanner seeing nothing.
#[test]
fn the_scanner_counts_a_planted_site_and_skips_test_items_and_comments() {
    assert_eq!(sites(&production_text("fn f(bp: &B) { let p = bp.new_page()?; }\n")), 1, "a new_page() site was missed");
    assert_eq!(sites(&production_text("fn f(d: &D) { let p = d.allocate()?; }\n")), 1, "an allocate() site was missed");
    assert_eq!(
        sites(&production_text("let t = BPlusTreeManager::<Value, RecordId>::create(bp.clone())?;\n")),
        1,
        "a tree creation was missed"
    );
    assert_eq!(sites(&production_text("// bp.new_page() in a comment\n")), 0, "a comment was counted");
    let file = "#[cfg(test)]\n#[path = \"tests_x.rs\"]\nmod tests_x;\nfn f(bp: &B) { bp.new_page(); }\n#[cfg(test)]\nmod tests {\n    fn g(bp: &B) { bp.new_page(); let s = \"}\"; }\n}\nfn h(bp: &B) { bp.new_page(); }\n";
    assert_eq!(sites(&production_text(file)), 2, "the #[cfg(test)] cut is wrong: it must drop the test module and keep the two sites around it");
}
