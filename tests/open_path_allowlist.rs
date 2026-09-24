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
//! - nothing under `src/` or `examples/` reaches `recover` or opens the table catalog outside the
//!   BODY of `open_recovered` in `src/wal/recovery.rs`. Not even the rest of that file: a second
//!   sequence written beside the first is the same drift as one written in an example. Opening a
//!   catalog is here as well because a binary that opens one without recovering at all is the same
//!   hazard, one step earlier;
//! - both production entry points, `src/cli/cli.rs` and `examples/pgserver.rs`, call
//!   `open_recovered(`. Without this floor, deleting every opener would pass;
//! - `open_recovered`'s own body holds exactly one `recover` and one catalog open, and calls
//!   `recover(` before `rebuild_indexes(`.
//!
//! An allowlist and not a denylist, for `lock_order_allowlist.rs`'s reason: the entry point this
//! has to catch is the one nobody has written yet.
//!
//! # What counts as a site
//!
//! The scanner reads a file's PRODUCTION text: comments and the insides of string and char literals
//! are blanked, and every item under `#[cfg(test)]` is removed, item by item. Then it tokenises, so
//! spacing does not matter. A site is:
//! - `recover`, or a rename of it (`recover as X`), when called, or when named by a path
//!   (`recovery::recover`, including a `use` of it or taking it as a value). Not its definition,
//!   and not a method of that name (`.recover(`);
//! - `Catalog::open`, or `X::open` for a rename of `Catalog` (`Catalog as X`, `type X = Catalog;`),
//!   called or not, and `<Catalog>::open`;
//! - `Self::open` inside an `impl` whose self type is `Catalog` or a rename of it.
//!
//! Renames are collected over EVERY scanned file, because one file can export a rename that another
//! uses. `LogBranchCatalog::open`/`TableBranchCatalog::open` are other types and do not count.
//!
//! # Amended after the read-vs-n review (`frontier/read_vs_n_review.md` §2, item 7)
//!
//! Until this amendment the scan stopped at a file's FIRST `#[cfg(test)]` line. That hid all of
//! `wal/log.rs` after line 65 and `branch/arena.rs` after line 991, because a single test-only
//! field or method is enough to trigger it. It also missed `Self::open(` inside `impl Catalog`, a
//! renamed `Catalog`, a call after a `//` inside a string, and `open (` with a space. Each gap has
//! a planted case in `the_scanner_sees_what_the_first_version_missed`, and a planted edit to a real
//! file in the lane runner (W1–W6), which must fail this test.
//!
//! # Blind spots, stated
//!
//! - `tests/` is not scanned. Tests call `recover` directly, because recovery is what they test.
//! - Modules are not resolved. A test-only FILE (declared `#[cfg(test)] mod x;` in its parent, like
//!   `consensus/tests_log.rs`) is scanned as production, so an opener in it FAILS this test. That
//!   is the loud direction; move the opener into an inline `#[cfg(test)]` item.
//! - Only `#[cfg(test)]` removes text. `#[cfg(all(test, ..))]` and every other cfg are scanned as
//!   production, which is also the loud direction.
//! - The end of a `#[cfg(test)]` item is found by bracket depth: an item keyword (`fn`, `mod`,
//!   `impl`, ...) ends at `;` or at the close of its first block; anything else (a field, an arm, a
//!   statement) also ends at a comma. A comma inside `<...>` of a field's type ends it early, and
//!   the rest is scanned. That is loud too.
//! - It reads tokens, not types. A macro that builds the call from pieces, a trait method that
//!   forwards to `Catalog::open` under another name, `<Catalog as Trait>::open`, or a function
//!   pointer passed on under another name after the path was taken, is not seen.
//! - The order check reads text order in the body, so `if false && ..` around the rebuild would
//!   pass it. That the rebuild RUNS is measured by behaviour instead:
//!   `tests/pgserver_crash_rebuilds_indexes.rs` and
//!   `wal::recovery::tests::a_failed_index_undo_makes_the_next_open_rebuild_even_when_the_log_holds_no_heap_or_clr_record`.
//!
//! # If this fails
//!
//! You opened a database without `open_recovered`. Call it instead. There is deliberately no
//! per-file exemption list. A binary that genuinely must not rebuild needs a change to the shared
//! function, reviewed as one, not a second way in.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The shared function, and the file it lives in. Its body is the only text where a site may be.
const SHARED_FILE: &str = "src/wal/recovery.rs";
const SHARED_FN: &str = "pub fn open_recovered(";

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

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// `src` with every comment, and the inside of every string and char literal, replaced by spaces.
/// Newlines are kept, so a line number in the result is a line number in the file. Handles `//`,
/// nested `/* */`, `"..."` with escapes, raw strings (`r#"..."#`, `br"..."`), and char literals as
/// distinct from lifetimes (`'a`).
fn blank_comments_and_literals(src: &str) -> String {
    let c: Vec<char> = src.chars().collect();
    let mut out = c.clone();
    let blank = |out: &mut Vec<char>, from: usize, to: usize| {
        for ch in &mut out[from..to.min(c.len())] {
            if *ch != '\n' {
                *ch = ' ';
            }
        }
    };
    let n = c.len();
    let at = |i: usize| c.get(i).copied();
    let mut i = 0;
    while i < n {
        // A raw string starts at an `r` (or `br`) that does not end an identifier, then `#`s, then `"`.
        let raw_start = c[i] == 'r'
            && (i == 0 || !is_ident_char(c[i - 1]) || (c[i - 1] == 'b' && (i < 2 || !is_ident_char(c[i - 2]))));
        let hashes = if raw_start { (i + 1..n).take_while(|&j| c[j] == '#').count() } else { 0 };
        if c[i] == '/' && at(i + 1) == Some('/') {
            let end = (i..n).find(|&j| c[j] == '\n').unwrap_or(n);
            blank(&mut out, i, end);
            i = end;
        } else if c[i] == '/' && at(i + 1) == Some('*') {
            let (mut depth, mut j) = (0usize, i);
            while j < n {
                if c[j] == '/' && at(j + 1) == Some('*') {
                    depth += 1;
                    j += 2;
                } else if c[j] == '*' && at(j + 1) == Some('/') {
                    depth -= 1;
                    j += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    j += 1;
                }
            }
            blank(&mut out, i, j);
            i = j;
        } else if raw_start && at(i + 1 + hashes) == Some('"') {
            let body = i + 2 + hashes;
            let close = (body..n)
                .find(|&k| c[k] == '"' && (1..=hashes).all(|h| at(k + h) == Some('#')))
                .unwrap_or(n);
            blank(&mut out, body, close);
            i = close + 1 + hashes;
        } else if c[i] == '"' {
            let mut j = i + 1;
            while j < n && c[j] != '"' {
                j += if c[j] == '\\' { 2 } else { 1 };
            }
            blank(&mut out, i + 1, j);
            i = j + 1;
        } else if c[i] == '\'' {
            if at(i + 1) == Some('\\') {
                // The escaped character is at i + 2, so the closing quote is searched from i + 3:
                // `'\''` would otherwise close on its own escaped quote.
                match (i + 3..n.min(i + 12)).find(|&k| c[k] == '\'') {
                    Some(k) => {
                        blank(&mut out, i + 1, k);
                        i = k + 1;
                    }
                    None => i += 1,
                }
            } else if at(i + 2) == Some('\'') {
                blank(&mut out, i + 1, i + 2);
                i += 3;
            } else {
                i += 1; // a lifetime or a label
            }
        } else {
            i += 1;
        }
    }
    out.into_iter().collect()
}

#[derive(Debug, PartialEq)]
enum Tok {
    Ident(String),
    Punct(char),
}

/// The tokens of blanked text, each with its byte offset. A number is an `Ident`, which is harmless:
/// nothing here compares one.
fn tokens(code: &str) -> Vec<(usize, Tok)> {
    let mut out = Vec::new();
    let mut it = code.char_indices().peekable();
    while let Some((at, ch)) = it.next() {
        if ch.is_whitespace() {
            continue;
        }
        if is_ident_char(ch) {
            let mut word = String::from(ch);
            while let Some(&(_, next)) = it.peek() {
                if !is_ident_char(next) {
                    break;
                }
                word.push(next);
                it.next();
            }
            out.push((at, Tok::Ident(word)));
        } else {
            out.push((at, Tok::Punct(ch)));
        }
    }
    out
}

fn punct(t: &[(usize, Tok)], k: usize, p: char) -> bool {
    matches!(t.get(k), Some((_, Tok::Punct(c))) if *c == p)
}

fn ident(t: &[(usize, Tok)], k: usize) -> Option<&str> {
    match t.get(k) {
        Some((_, Tok::Ident(s))) => Some(s.as_str()),
        _ => None,
    }
}

/// The index of the bracket that closes the one at `open`, or `t.len()` if it never closes.
fn close_of(t: &[(usize, Tok)], open: usize) -> usize {
    let mut depth = 0i32;
    for (k, (_, tok)) in t.iter().enumerate().skip(open) {
        match tok {
            Tok::Punct('(' | '[' | '{') => depth += 1,
            Tok::Punct(')' | ']' | '}') => {
                depth -= 1;
                if depth == 0 {
                    return k;
                }
            }
            _ => {}
        }
    }
    t.len()
}

/// One past the last token of the item under a `#[cfg(test)]` whose `]` is just before `j`.
fn item_end(t: &[(usize, Tok)], mut j: usize) -> usize {
    // More attributes on the same item, such as `#[path = "tests_x.rs"]`.
    while punct(t, j, '#') && punct(t, j + 1, '[') {
        j = close_of(t, j + 1) + 1;
    }
    // Is it an item? Look past `pub`, `pub(crate)`, `unsafe`, `async`, `default`.
    let mut k = j;
    while let Some(w) = ident(t, k) {
        if !matches!(w, "pub" | "unsafe" | "async" | "default") {
            break;
        }
        k += 1;
        if punct(t, k, '(') {
            k = close_of(t, k) + 1;
        }
    }
    const ITEMS: &[&str] =
        &["fn", "mod", "impl", "struct", "enum", "trait", "union", "use", "const", "static", "type", "macro_rules", "extern"];
    let is_item = ident(t, k).is_some_and(|w| ITEMS.contains(&w));
    let mut depth = 0i32;
    for (k, (_, tok)) in t.iter().enumerate().skip(j) {
        match tok {
            Tok::Punct('(' | '[' | '{') => depth += 1,
            Tok::Punct(c @ (')' | ']' | '}')) => {
                if depth == 0 {
                    return k; // it was the last thing in its container
                }
                depth -= 1;
                if depth == 0 && *c == '}' {
                    return k + 1;
                }
            }
            Tok::Punct(';') if depth == 0 => return k + 1,
            Tok::Punct(',') if depth == 0 && !is_item => return k + 1,
            _ => {}
        }
    }
    t.len()
}

/// Everything from `from` to the close of the block that contains it: what `#![cfg(test)]` covers.
fn enclosing_end(t: &[(usize, Tok)], from: usize) -> usize {
    let mut depth = 0i32;
    for (k, (_, tok)) in t.iter().enumerate().skip(from) {
        match tok {
            Tok::Punct('(' | '[' | '{') => depth += 1,
            Tok::Punct(')' | ']' | '}') => {
                if depth == 0 {
                    return k;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    t.len()
}

/// Blank, keeping newlines, every item under `#[cfg(test)]` (a test module, a test-only method,
/// field, `use` or `#[path]` module declaration), and the rest of the enclosing module under
/// `#![cfg(test)]`. Item by item: production code after a test item is still scanned.
fn strip_test_items(code: &str) -> String {
    let t = tokens(code);
    let mut bytes = code.as_bytes().to_vec();
    let mut i = 0;
    while i < t.len() {
        let inner = punct(&t, i + 1, '!');
        let a = if inner { i + 2 } else { i + 1 };
        let is_cfg_test = punct(&t, i, '#')
            && punct(&t, a, '[')
            && ident(&t, a + 1) == Some("cfg")
            && punct(&t, a + 2, '(')
            && ident(&t, a + 3) == Some("test")
            && punct(&t, a + 4, ')')
            && punct(&t, a + 5, ']');
        if !is_cfg_test {
            i += 1;
            continue;
        }
        let end_tok = if inner { enclosing_end(&t, a + 6) } else { item_end(&t, a + 6) };
        let end = t.get(end_tok).map_or(code.len(), |(at, _)| *at);
        for b in &mut bytes[t[i].0..end] {
            if *b != b'\n' {
                *b = b' ';
            }
        }
        i = end_tok.max(i + 1);
    }
    String::from_utf8(bytes).expect("blanking whole tokens keeps the text UTF-8")
}

/// The production part of a file: CRLF normalised, comments and literals blanked, test items
/// removed. Line numbers still match the file.
fn production_text(src: &str) -> String {
    strip_test_items(&blank_comments_and_literals(&src.replace("\r\n", "\n")))
}

/// The names the scanned files give the table catalog and `recover`: the originals, every `N as X`
/// rename of one of them, and every `type X = path::Catalog;`. Collected over all files at once,
/// and repeated until nothing new appears, so a rename of a rename is found too.
struct Names {
    catalog: BTreeSet<String>,
    recover: BTreeSet<String>,
}

fn names(codes: &[String]) -> Names {
    let mut n = Names {
        catalog: BTreeSet::from(["Catalog".to_string()]),
        recover: BTreeSet::from(["recover".to_string()]),
    };
    let toks: Vec<Vec<(usize, Tok)>> = codes.iter().map(|c| tokens(c)).collect();
    loop {
        let before = n.catalog.len() + n.recover.len();
        for t in &toks {
            for k in 0..t.len() {
                if let (Some(a), Some("as"), Some(x)) = (ident(t, k), ident(t, k + 1), ident(t, k + 2)) {
                    if n.catalog.contains(a) {
                        n.catalog.insert(x.to_string());
                    }
                    if n.recover.contains(a) {
                        n.recover.insert(x.to_string());
                    }
                }
                if let (Some("type"), Some(x)) = (ident(t, k), ident(t, k + 1)) {
                    if punct(t, k + 2, '=') {
                        let (mut m, mut last) = (k + 3, None);
                        while let Some(seg) = ident(t, m) {
                            last = Some(seg);
                            if punct(t, m + 1, ':') && punct(t, m + 2, ':') {
                                m += 3;
                            } else {
                                m += 1;
                                break;
                            }
                        }
                        if punct(t, m, ';') && last.is_some_and(|l| n.catalog.contains(l)) {
                            n.catalog.insert(x.to_string());
                        }
                    }
                }
            }
        }
        if n.catalog.len() + n.recover.len() == before {
            return n;
        }
    }
}

fn line_at(code: &str, at: usize) -> String {
    let start = code[..at].rfind('\n').map_or(0, |i| i + 1);
    let end = code[at..].find('\n').map_or(code.len(), |i| at + i);
    code[start..end].trim().to_string()
}

struct Site {
    kind: &'static str,
    line: usize,
    text: String,
}

/// Every site in `code` (production text), in the order found. See the module doc for what counts.
fn open_sites(code: &str, n: &Names) -> Vec<Site> {
    let t = tokens(code);
    let site = |kind: &'static str, at: usize| Site {
        kind,
        line: code[..at].matches('\n').count() + 1,
        text: line_at(code, at),
    };
    let mut out = Vec::new();
    for k in 0..t.len() {
        let Some(w) = ident(&t, k) else { continue };
        if n.recover.contains(w) {
            let definition = k > 0 && ident(&t, k - 1) == Some("fn");
            let method = k > 0 && punct(&t, k - 1, '.');
            let path = k >= 2 && punct(&t, k - 1, ':') && punct(&t, k - 2, ':');
            if !definition && !method && (path || punct(&t, k + 1, '(')) {
                out.push(site("recover", t[k].0));
            }
        }
        if n.catalog.contains(w) {
            let m = if punct(&t, k + 1, '>') { k + 2 } else { k + 1 };
            if punct(&t, m, ':') && punct(&t, m + 1, ':') && ident(&t, m + 2) == Some("open") {
                out.push(site("Catalog::open", t[k].0));
            }
        }
        if w == "impl" {
            let Some(open) = (k + 1..t.len()).find(|&m| punct(&t, m, '{')) else { continue };
            // The self type: after `for` if the header has one, else after `impl` and its generics.
            let mut s = (k + 1..open).find(|&m| ident(&t, m) == Some("for")).map_or(k + 1, |m| m + 1);
            if s == k + 1 && punct(&t, s, '<') {
                let mut depth = 0i32;
                while s < open {
                    if punct(&t, s, '<') {
                        depth += 1;
                    } else if punct(&t, s, '>') {
                        depth -= 1;
                        if depth == 0 {
                            s += 1;
                            break;
                        }
                    }
                    s += 1;
                }
            }
            let mut self_type = None;
            while let Some(seg) = ident(&t, s) {
                self_type = Some(seg);
                if punct(&t, s + 1, ':') && punct(&t, s + 2, ':') {
                    s += 3;
                } else {
                    break;
                }
            }
            if self_type.is_some_and(|ty| n.catalog.contains(ty)) {
                for m in open..close_of(&t, open) {
                    if ident(&t, m) == Some("Self")
                        && punct(&t, m + 1, ':')
                        && punct(&t, m + 2, ':')
                        && ident(&t, m + 3) == Some("open")
                    {
                        out.push(site("Self::open in impl Catalog", t[m].0));
                    }
                }
            }
        }
    }
    out
}

/// `(the text with the shared function's body blanked, the body)`. The body runs from
/// `pub fn open_recovered(` to the first `}` at column 0 after it. Panics if the function is gone,
/// because every assertion that uses this would otherwise pass against an empty body.
fn split_shared_body(code: &str) -> (String, String) {
    let start = code
        .find(SHARED_FN)
        .unwrap_or_else(|| panic!("`{SHARED_FN}` is not in {SHARED_FILE}: renamed or moved"));
    let len = code[start..].find("\n}\n").expect("unterminated open_recovered") + 3;
    let body = code[start..start + len].to_string();
    let blanked: String = body.chars().map(|c| if c == '\n' { '\n' } else { ' ' }).collect();
    (format!("{}{}{}", &code[..start], blanked, &code[start + len..]), body)
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

    let texts: Vec<(String, String)> = files
        .iter()
        .map(|f| (rel(root, f), production_text(&std::fs::read_to_string(f).unwrap())))
        .collect();
    let n = names(&texts.iter().map(|(_, code)| code.clone()).collect::<Vec<_>>());

    let mut offenders = Vec::new();
    let mut shared = None;
    for (name, code) in &texts {
        let mut code = code.clone();
        if name == SHARED_FILE {
            let (outside, inside) = split_shared_body(&code);
            shared = Some(open_sites(&inside, &n));
            code = outside;
        }
        offenders.extend(
            open_sites(&code, &n).into_iter().map(|s| format!("{name}:{}: {} ({})", s.line, s.text, s.kind)),
        );
    }
    assert!(
        offenders.is_empty(),
        "these open a database without `wal::recovery::open_recovered`, so after a crash they can \
         run with index trees the recovered heap no longer matches:\n  {}",
        offenders.join("\n  ")
    );
    // Anti-vacuity, and no second sequence inside the shared body: exactly one of each, in order.
    // A renamed function or a scanner that finds nothing cannot pass this.
    let shared = shared.unwrap_or_else(|| panic!("{SHARED_FILE} was not among the scanned files"));
    let kinds: Vec<&str> = shared.iter().map(|s| s.kind).collect();
    assert_eq!(
        kinds,
        vec!["recover", "Catalog::open"],
        "`open_recovered` must hold exactly one `recover` and then exactly one catalog open. The \
         scanner or the function has moved, or a second sequence was written inside it"
    );
}

#[test]
fn both_production_entry_points_open_through_the_shared_function() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for entry in ENTRY_POINTS {
        let code = production_text(&std::fs::read_to_string(root.join(entry)).unwrap_or_else(|e| {
            panic!("{entry} is not where this test expects an entry point: {e}")
        }));
        let t = tokens(&code);
        assert!(
            (0..t.len()).any(|k| ident(&t, k) == Some("open_recovered") && punct(&t, k + 1, '(')),
            "{entry} does not call `open_recovered`, so it opens the database some other way"
        );
    }
}

/// D250 review 1's F7, the lead's ruling (lane `lane_d250_drop_logged.md` §3.7 test 12): a DROP the
/// open completed must also be forgotten by the agent runtime (B9), and that forget is unskippable
/// only if each production entry point builds its runtime through `OpenedDatabase::attach_runtime`,
/// the one door that runs it. The list is private to `wal::recovery`, so an entry point cannot run
/// the loop itself; what this catches is one that wraps its runtime in an `Arc` on its own and never
/// reaches the door, or that names the list at all.
#[test]
fn both_production_entry_points_build_their_runtime_through_the_opened_database() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for entry in ENTRY_POINTS {
        let code = production_text(&std::fs::read_to_string(root.join(entry)).unwrap_or_else(|e| {
            panic!("{entry} is not where this test expects an entry point: {e}")
        }));
        let t = tokens(&code);
        let doors = (1..t.len())
            .filter(|&k| punct(&t, k - 1, '.') && ident(&t, k) == Some("attach_runtime") && punct(&t, k + 1, '('))
            .count();
        assert_eq!(
            doors, 1,
            "{entry} calls `.attach_runtime(` {doors} times, not once: a runtime built without it keeps the \
             provenance of every table whose DROP the open completed"
        );
        assert!(
            !(0..t.len()).any(|k| ident(&t, k) == Some("completed_drops")),
            "{entry} names `completed_drops`: the list belongs to `OpenedDatabase::attach_runtime`, and an \
             entry point that reads it has a way round the door"
        );
    }
}

#[test]
fn the_shared_function_recovers_and_then_rebuilds() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let (_, body) = split_shared_body(&production_text(&std::fs::read_to_string(root.join(SHARED_FILE)).unwrap()));
    let recovered = body.find("recover(&").expect("open_recovered does not call recover");
    let rebuilt = body.find("rebuild_indexes(").expect("open_recovered does not call rebuild_indexes");
    assert!(recovered < rebuilt, "open_recovered rebuilds the indexes BEFORE recovering the heap they are built from");
}

/// The number of sites in `src`, scanned as one file on its own.
fn count(src: &str) -> usize {
    let code = production_text(src);
    open_sites(&code, &names(&[code.clone()])).len()
}

/// Positive and negative controls for the scanner, on text written here, so a scanner that
/// matched nothing (or everything) cannot pass the tests above.
#[test]
fn the_scanner_catches_a_bare_opener_and_ignores_everything_else() {
    assert_eq!(count("let r = recover(&txn)?;"), 1, "a bare call was missed");
    assert_eq!(count("let r = ferrodb::wal::recovery::recover(&txn)?;"), 1, "a path call was missed");
    assert_eq!(count("let c = Catalog::open(bp.clone(), 1)?;"), 1, "a catalog open was missed");
    assert_eq!(count("let c = crate::catalog::catalog::Catalog::open(bp, 1)?;"), 1, "a path catalog open was missed");

    assert_eq!(count("pub fn recover(txn: &TxnManager) -> Result<bool, FerroError> {"), 0, "the definition was counted");
    assert_eq!(count("let lost = self.dedup.recover();"), 0, "a method called recover was counted");
    assert_eq!(count("let c = LogBranchCatalog::open(&p, 1)?;"), 0, "a branch catalog was counted");
    assert_eq!(count("let c = TableBranchCatalog::open(pool, root)?;"), 0, "a branch catalog was counted");
    assert_eq!(count("let db = open_recovered(&path, &lock)?;"), 0, "the shared function was counted");
    assert_eq!(count("let recovered = true; let c = Catalog::create(bp)?;"), 0, "a near-miss name was counted");
    assert_eq!(count("    // recover(&txn) is what this used to do"), 0, "a line comment was counted");
    assert_eq!(count("/* recover(&txn) /* nested */ Catalog::open(bp, 1) */"), 0, "a block comment was counted");
    assert_eq!(count(r#"let m = "recover(&txn) and Catalog::open(bp, 1)";"#), 0, "a string was counted");
    assert_eq!(count(r##"let m = r#"Catalog::open(bp, 1) "}"#;"##), 0, "a raw string was counted");
    assert_eq!(
        count("let a = recover(&txn)?;\n#[cfg(test)]\nmod tests {\n    fn t() { recover(&txn); }\n}\n"),
        1,
        "the cut at #[cfg(test)] is wrong: it must count the call before it and none inside the test module"
    );

    // The shared file is not exempt as a whole: a second sequence beside `open_recovered` counts.
    let file = "pub fn recover(t: &T) -> R {\n}\npub fn open_recovered(p: &Path) -> R {\n    let r = recover(&t)?;\n    let c = Catalog::open(bp, 1)?;\n}\nfn second_way_in() {\n    recover(&t);\n}\n";
    let code = production_text(file);
    let n = names(&[code.clone()]);
    let (outside, inside) = split_shared_body(&code);
    let kinds: Vec<&str> = open_sites(&inside, &n).iter().map(|s| s.kind).collect();
    assert_eq!(kinds, vec!["recover", "Catalog::open"], "the shared body's two calls were not both found, in order");
    let beside = open_sites(&outside, &n);
    assert_eq!(beside.len(), 1, "a recover( call beside the shared function was not counted");
    assert_eq!(beside[0].line, 8, "a site's line number does not match the file");

    // Blanking keeps every line, so a reported line is the file's line.
    let src = "a\n// b\n\"c\nd\"\n/* e\nf */\n#[cfg(test)]\nmod t {\n}\ng\n";
    assert_eq!(production_text(src).lines().count(), src.lines().count(), "blanking changed the line count");
}

/// **One planted case per gap the read-vs-n review found in the first version**
/// (`frontier/read_vs_n_review.md` §2). Each was missed then, and each must be counted now.
/// The lane runner plants the same shapes into real files (W1–W6).
#[test]
fn the_scanner_sees_what_the_first_version_missed() {
    // The first-`#[cfg(test)]` cut, in the three shapes the tree has: a test-only method
    // (`branch/arena.rs`), a test-only struct field (`wal/log.rs`), a `#[path]` test module
    // declaration (`consensus/log.rs`). The production code AFTER each must still be scanned.
    let method = "impl Store {\n    #[cfg(test)]\n    fn debug_only(&self) { let _ = '{'; }\n    pub fn reopen(&self, t: &T) {\n        let _ = crate::wal::recovery::recover(t);\n    }\n}\n";
    assert_eq!(count(method), 1, "a recover( after a test-only method was missed (gap A)");
    let field = "pub struct Wal {\n    #[cfg(test)]\n    pub(crate) fail_next: std::collections::HashMap<u64, u64>,\n    next: u64,\n}\nimpl WalPin {\n    fn f(bp: B) { let _ = Catalog::open(bp, 1); }\n}\n";
    assert_eq!(count(field), 1, "a Catalog::open( after a test-only field was missed (gap B)");
    let path_mod = "#[cfg(test)]\n#[path = \"tests_x.rs\"]\nmod tests_x;\nfn f(bp: B) { let _ = Catalog::open(bp, 1); }\n";
    assert_eq!(count(path_mod), 1, "a Catalog::open( after a #[path] test module declaration was missed");
    // And test items are still removed, whole, wherever they sit, whatever braces their literals hold.
    let mixed = "fn a(t: &T) { recover(t); }\n#[cfg(test)]\nmod tests {\n    fn t<'a>(x: &'a T) { let _ = \"}\"; let _ = '}'; recover(&txn); Catalog::open(bp, 1); }\n}\nfn after(t: &T) { recover(t); }\n";
    assert_eq!(count(mixed), 2, "the test module was not removed whole, or the code after it was not scanned");

    // `Self::open` inside `impl Catalog` (gap D), and not inside another type's impl.
    assert_eq!(count("impl Catalog {\n    pub fn load(bp: B) -> R { Self::open(bp, 1) }\n}\n"), 1, "Self::open in impl Catalog was missed (gap D)");
    assert_eq!(count("impl<'a> crate::catalog::catalog::Catalog {\n    fn load(bp: B) -> R { Self::open(bp, 1) }\n}\n"), 1, "Self::open in a path-named impl was missed");
    assert_eq!(count("impl TableBranchCatalog {\n    pub fn load(p: P) -> R { Self::open(p, 1) }\n}\n"), 0, "Self::open in another type's impl was counted");

    // A renamed catalog (gap E), in the same file and across files, and a renamed `recover`.
    assert_eq!(count("use crate::catalog::catalog::Catalog as SqlCatalog;\nfn f(bp: B) { let _ = SqlCatalog::open(bp, 1); }\n"), 1, "a renamed Catalog was missed (gap E)");
    assert_eq!(count("pub type Tables = crate::catalog::catalog::Catalog;\nfn f(bp: B) { let _ = Tables::open(bp, 1); }\n"), 1, "a type alias of Catalog was missed");
    let exports = production_text("pub use crate::catalog::catalog::Catalog as SqlCatalog;\n");
    let uses = production_text("fn f(bp: B) { let _ = SqlCatalog::open(bp, 1); }\n");
    assert_eq!(open_sites(&uses, &names(&[exports, uses.clone()])).len(), 1, "a rename exported from another file was missed");
    assert_eq!(count("use crate::wal::recovery::recover as replay;\nfn f(t: &T) { replay(t); }\n"), 2, "a renamed recover: the import and the call are two sites");

    // A `//` inside a string before the call (gap F), and a space before the paren (gap G).
    assert_eq!(count("let u = \"file://x\"; let _ = Catalog::open(bp, 1);"), 1, "a call after a // inside a string was missed (gap F)");
    assert_eq!(count("let _ = Catalog::open (bp, 1);"), 1, "`Catalog::open (` with a space was missed (gap G)");
    assert_eq!(count("let r = recover (&txn)?;"), 1, "`recover (` with a space was missed");
    assert_eq!(count("let f = crate::wal::recovery::recover;"), 1, "recover taken as a value was missed");
    assert_eq!(count("let _ = <Catalog>::open(bp, 1);"), 1, "`<Catalog>::open` was missed");
}
