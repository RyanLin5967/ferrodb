pub mod error;
pub mod catalog;
pub mod storage;
pub mod buffer;
pub mod parser;
pub mod execution;
pub mod planner;
pub mod cli;
pub mod binder;
pub mod optimizer;
pub mod wal;

// agent-isolation layer
pub mod agent_sql;
pub mod branch;
pub mod cow;
pub mod tel;
pub mod pgwire;
pub mod provenance;
pub mod replication;
pub mod consensus;
/// F4: the node-local counters that must become cluster state, and the guards that refuse.
pub mod cluster;

/// The commit this binary was built from, rendered for the head of a `bench/` file.
///
/// Returns e.g. `"at 1a2b3c4d5e6f"`, or `"at 1a2b3c4d5e6f +DIRTY (uncommitted changes — this
/// commit does NOT describe the code that ran)"` when the tree had uncommitted tracked edits.
///
/// **Every harness in `examples/` should print this as its first line.** The reason is in
/// `build.rs`: a measurement banked on 2026-09-20 was run seven commits behind the branch it was
/// reported against, and nothing caught it because 120 of the 151 files in `bench/` named no
/// commit. A stamp that travels inside the binary cannot be forgotten and cannot disagree with
/// the code that ran.
///
/// ⚠ It describes BUILD time, not RUN time. A tree edited while a run is in flight still produces
/// a stamp that looks clean; comparing the tree before and after a run is `verify-suite.sh`'s job
/// and this does not replace it.
pub fn build_provenance() -> String {
    let commit = env!("FERRODB_BUILD_COMMIT");
    if env!("FERRODB_BUILD_DIRTY") == "1" {
        format!(
            "at {commit} +DIRTY (uncommitted changes — this commit does NOT describe the code that ran)"
        )
    } else {
        format!("at {commit}")
    }
}

#[cfg(test)]
mod build_provenance_tests {
    /// The stamp must be a real commit, not the `unknown` fallback, when built in a git tree.
    ///
    /// This is the anti-vacuity check for the whole mechanism: `build.rs` degrades to `unknown`
    /// rather than failing the build when git is unavailable, which is the right call for a
    /// library but means the useful case and the useless one compile identically. If this test
    /// ever reports `unknown` in CI, the stamp is decorative and every `bench/` header carrying it
    /// is decorative too.
    #[test]
    fn the_build_stamp_names_a_real_commit() {
        let c = env!("FERRODB_BUILD_COMMIT");
        assert_ne!(
            c, "unknown",
            "the build stamp fell back to `unknown`, so provenance in bench/ headers is decorative"
        );
        assert_eq!(c.len(), 12, "expected a 12-char short sha, got {c:?}");
        assert!(
            c.chars().all(|ch| ch.is_ascii_hexdigit()),
            "build stamp {c:?} is not hex"
        );
    }

    /// The rendered form must SHOUT when the tree was dirty. A stamp that hides it is worse than
    /// none: a clean-looking sha next to a number invites exactly the trust a half-applied edit
    /// does not deserve.
    #[test]
    fn a_dirty_tree_is_loud_in_the_rendered_provenance() {
        let rendered = super::build_provenance();
        assert!(rendered.starts_with("at "), "unexpected shape: {rendered:?}");
        if env!("FERRODB_BUILD_DIRTY") == "1" {
            assert!(rendered.contains("+DIRTY"), "dirty build did not say so: {rendered:?}");
            assert!(
                rendered.contains("does NOT describe the code that ran"),
                "the dirty suffix must say what it means, not just flag it: {rendered:?}"
            );
        } else {
            assert!(!rendered.contains("DIRTY"), "clean build claimed dirty: {rendered:?}");
        }
    }
}
