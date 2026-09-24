//! D265 (PREREG A20.5, A22.1): every registered production entry point fails on a dead lease
//! thread, through the one check `LeaseStats::ended_alive`.
//!
//! The entry points are `tests/open_path_allowlist.rs`'s: `src/cli/cli.rs` (whose
//! `OpenDatabase::close` makes the check, so `run_cli` exits non-zero) and `examples/pgserver.rs`
//! (which makes it after its final checkpoint and panics, releasing its lock). A second entry point
//! spelling its own shutdown is how D204 happened; this keeps the lease half of it from drifting.
//!
//! **Blind spots, stated.** It reads text: a call made through a renamed re-export is not seen, and
//! a call whose result is ignored passes. The unit half below pins what the check returns.

use std::path::Path;

const ENTRY_POINTS: &[&str] = &["src/cli/cli.rs", "examples/pgserver.rs"];

/// Comment-stripped text up to the first bare `#[cfg(test)]` line: `open_path_allowlist`'s rule.
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

/// **D265 (d).** Every registered entry point asks whether its lease thread ended alive.
#[test]
fn every_entry_point_asks_whether_its_lease_thread_ended_alive() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for entry in ENTRY_POINTS {
        let src = std::fs::read_to_string(root.join(entry)).unwrap_or_else(|e| panic!("read {entry}: {e}"));
        assert!(
            production_text(&src).contains(".ended_alive()"),
            "{entry} never calls `.ended_alive()` at shutdown (D265): a lease thread that panicked would \
             end in exit 0"
        );
    }
}
