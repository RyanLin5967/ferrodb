//! D230 review 3, F3(b) — **the CLI's clean exit runs the catalog persist before the arena
//! checkpoint.** This checks WORDING, not an outcome, and says so here.
//!
//! `tests/d230_clean_exit_repersists_the_catalog.rs` runs `cli::exit_sequence` and shows that the
//! catalog persist in it is load-bearing (mutants M6–M10). What no test can run is the binary's own
//! exit: `run_cli` reads a REPL from stdin and cannot be handed a catalog whose persist fails. So one
//! edit would pass every outcome test:
//! - M12: `run_cli` goes back to calling `checkpoint_for_exit` and the arena checkpoint inline
//!   instead of calling `exit_sequence`.
//!
//! This test reads `src/cli/cli.rs`, with `//` comments stripped, and fails on it.
//!
//! # Blind spots
//!
//! It matches text. A call under another name (an alias, a wrapper), a call in dead code, a string
//! holding the pattern, or a pattern inside a `/* */` comment (only `//` is stripped) would all
//! satisfy it. It is the same kind of guard as `d53_private_root_allowlist.rs`, for the same reason:
//! the outcome is not reachable from a test.

/// `path` under the crate root, with every `//` comment removed.
fn code_only(path: &str) -> String {
    let full = format!("{}/{path}", env!("CARGO_MANIFEST_DIR"));
    let src = std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("read {full}: {e}"));
    src.lines()
        .map(|l| match l.find("//") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The text of a top-level function in `src`: from `signature` to the first line that is `}` alone.
fn body_of<'a>(src: &'a str, signature: &str) -> &'a str {
    let start = src
        .find(signature)
        .unwrap_or_else(|| panic!("`{signature}` not found: this guard is aimed at code that has moved"));
    let len = src[start..]
        .find("\n}\n")
        .unwrap_or_else(|| panic!("`{signature}` has no closing line: this guard is aimed at code that has moved"));
    &src[start..start + len]
}

#[test]
fn run_cli_ends_through_exit_sequence_which_persists_the_catalog_before_the_arena_checkpoint() {
    let cli = code_only("src/cli/cli.rs");
    let run_cli = body_of(&cli, "pub fn run_cli(");
    assert_eq!(
        run_cli.matches("exit_sequence(").count(),
        1,
        "`run_cli` must end through `exit_sequence`, exactly once: it is the only exit code a test \
         runs (M12)"
    );
    assert_eq!(
        run_cli.matches("store.checkpoint(").count(),
        0,
        "`run_cli` checkpoints the arena itself instead of through `exit_sequence`, so the exit the \
         binary runs is not the one the tests run (M12)"
    );
    let exit = body_of(&cli, "pub fn exit_sequence(");
    let persist = exit
        .find("checkpoint_for_exit(")
        .unwrap_or_else(|| panic!("`exit_sequence` must persist the catalog before its checkpoint (M9)"));
    let arena = exit
        .find("store.checkpoint(")
        .unwrap_or_else(|| panic!("`exit_sequence` must checkpoint the arena"));
    assert!(persist < arena, "`exit_sequence` must persist the catalog and checkpoint the log before the arena checkpoint");
}
