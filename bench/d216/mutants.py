#!/usr/bin/env python3
"""D216 / D227 / D234 mutants, each applied ALONE to a throwaway checkout of the tip.

Usage: python3 bench/d216/mutants.py <tip-sha> [--target-dir DIR]

For each mutant: restore the tree, apply one exact replacement (refused unless the pattern matches
exactly once), run the named cargo test targets, and record KILLED when every expected test is
reported FAILED, SURVIVED otherwise. A mutant marked expect=SURVIVE is a stated blind spot.
A CONTROL run of every target on the unmutated tip comes first: a test failing there is never
credited to a mutant, and a verdict is VOID when its target failed to compile, timed out, ran no
test, or exited nonzero with no FAILED test.

Raw cargo output goes to bench/d216/raw/<mutant>.txt beside this script, and one summary line per
mutant to bench/d216/raw/summary.txt. Commit them by explicit path BEFORE reading them.

The throwaway tree is `/Users/idide/wt/ferrodb-d216-mutants.noindex` (a `.noindex` name, for Spotlight),
created detached at <tip-sha> and removed at the end, whatever happened. It refuses to start if that
path already exists, rather than reusing somebody else's tree.
"""
import os
import re
import signal
import subprocess
import sys

REPO = "/Users/idide/projects/ferrodb"
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "raw")
TREE = "/Users/idide/wt/ferrodb-d216-mutants.noindex"
TIMEOUT = 3600  # seconds per cargo invocation

LIB = ("--lib", "wal::")
D227 = ("--test", "d227_restart_keeps_declarations")
D234 = ("--test", "d234_declared_runs_reach_the_log")
HISTORY = ("--test", "d234_decoder_history_still_collapses")
PGSERVER = ("--test", "pgserver_crash_rebuilds_indexes")
# Not a cargo-test argument list: `cargo build --examples`, for targets that spawn example binaries
# (the staleness guard refuses a binary older than src/).
BUILD_EXAMPLES = ("BUILD_EXAMPLES",)

R = "wal::recovery::tests::"
T = "wal::txn::tests::"
L = "wal::log::tests::"

# (name, file, old, new, targets, expected failing tests, expect)
MUTANTS = [
    ("M1_rebuild_ignores_marker", "src/wal/recovery.rs",
     "    let rebuild = recovered || stale;\n", "    let rebuild = recovered;\n",
     [LIB], [R + "the_stale_marker_still_forces_the_rebuild_over_a_log_of_declarations",
             R + "a_failed_index_undo_makes_the_next_open_rebuild_even_when_the_log_is_empty",
             R + "the_stale_marker_is_honoured_over_an_empty_log"], "KILL"),
    ("M2_no_rebuild_call", "src/wal/recovery.rs",
     "    if rebuild {\n        rebuild_indexes(&mut catalog, &bp)?;\n    }\n", "    if rebuild {\n    }\n",
     [LIB], [R + "a_crashed_uncommitted_heap_write_still_forces_the_rebuild",
             R + "a_committed_row_whose_heap_page_reached_disk_still_forces_the_rebuild",
             R + "the_stale_marker_still_forces_the_rebuild_over_a_log_of_declarations"], "KILL"),
    ("M3_checkpoint_only_after_rebuild", "src/wal/recovery.rs",
     "    if rebuild || holds_records || declares {\n", "    if rebuild {\n",
     [LIB], [R + "the_open_discards_a_previous_processs_run_declarations",
             R + "a_checkpoint_after_a_restart_still_declares_every_table",
             R + "an_open_over_an_empty_log_still_declares_every_table"], "KILL"),
    ("M4_any_record_is_stale", "src/wal/recovery.rs",
     "    Ok(!touched.is_empty())\n", "    Ok(true)\n",
     [LIB], [R + "a_clean_restart_after_ddl_does_not_rebuild_the_indexes",
             R + "a_clean_restart_after_a_run_declaration_does_not_rebuild_the_indexes",
             R + "a_log_with_no_data_records_rebuilds_nothing_and_still_raises_the_id_watermark",
             R + "recovery_appends_nothing_for_a_log_of_declarations"], "KILL"),
    ("M5_txn0_is_a_loser", "src/wal/recovery.rs",
     ".filter(|id| *id != 0 && !ended.contains(id))", ".filter(|id| !ended.contains(id))",
     [LIB], [R + "recovery_appends_nothing_for_a_log_of_declarations"], "KILL"),
    ("M6_gate_skips_unlogged_pages", "src/buffer/buffer_pool.rs",
     "                0 => wal.flush_through(frame.wal_mark.load(Ordering::Relaxed))?,\n",
     "                0 => {}\n",
     [LIB], [R + "an_index_page_reaches_disk_only_after_the_log_records_it_depends_on"], "KILL"),
    ("M7_gate_flushes_whole_log", "src/buffer/buffer_pool.rs",
     "                0 => wal.flush_through(frame.wal_mark.load(Ordering::Relaxed))?,\n",
     "                0 => wal.flush()?,\n",
     [LIB], [R + "an_index_page_whose_records_are_durable_does_not_flush_the_log"], "KILL"),
    ("M8_flush_up_to_ge", "src/wal/log.rs",
     "        if self.flushed_lsn.load(Ordering::SeqCst) > lsn {\n",
     "        if self.flushed_lsn.load(Ordering::SeqCst) >= lsn {\n",
     [LIB], [L + "flush_up_to_writes_a_record_that_starts_at_the_flushed_point",
             R + "a_commit_is_durable_when_everything_before_it_was_already_flushed",
             R + "a_heap_page_waits_for_its_own_record_when_it_starts_at_the_flushed_point"], "KILL"),
    ("M9_txn_id_zero_issuable", "src/wal/txn.rs",
     "        let start = wal.header_txn_id.max(1);\n", "        let start = wal.header_txn_id;\n",
     [LIB], [R + "transaction_id_zero_is_never_handed_out"], "KILL"),
    ("M10_no_table_refill", "src/wal/recovery.rs",
     "    for rec in declarations {\n        txn.retain_ddl(&rec);\n    }\n",
     "    for _rec in declarations {\n    }\n",
     [LIB, D227], [R + "a_checkpoint_after_a_restart_still_declares_every_table",
                   R + "an_open_over_an_empty_log_still_declares_every_table",
                   "a_restarted_process_still_declares_its_tables_and_runs"], "KILL"),
    ("M11_cli_declares_no_runs", "src/cli/cli.rs",
     "    txn.declare_runs_of(&**runtime.provenance())?;\n", "",
     [D227], ["a_restarted_process_still_declares_its_tables_and_runs"], "KILL"),
    ("M12_marker_not_durable", "src/wal/txn.rs",
     "        let written = crate::storage::atomic_file::replace_atomically(\n"
     "            &crate::storage::atomic_file::OsFileOps,\n"
     "            &marker,\n"
     "            format!(\"txn {txn_id}: {e}\\n\").as_bytes(),\n"
     "        );\n",
     "        let written = std::fs::write(&marker, format!(\"txn {txn_id}: {e}\\n\"));\n",
     [LIB], [], "SURVIVE"),
    ("M13_pgserver_declares_no_runs", "examples/pgserver.rs",
     "    txn.declare_runs_of(&**runtime.provenance()).unwrap_or_else(|e| panic!(\"pgserver: {e}\"));\n", "",
     [BUILD_EXAMPLES, PGSERVER], [], "SURVIVE"),
    ("M14_unconditional_replay", "src/wal/txn.rs",
     "        if truncation == Truncation::Truncated {\n", "        if true {\n",
     [LIB, D234], [T + "a_checkpoint_a_pin_kept_from_truncating_re_declares_nothing",
                   "a_run_declared_at_open_reaches_the_log_while_every_checkpoint_is_pinned"], "KILL"),
    ("M15_declare_runs_of_only_retains", "src/wal/txn.rs",
     "                self.wal.append(0, 0, &RecKind::RunIdentity { run })?;\n                wrote = true;\n",
     "                let _ = run;\n",
     [D234], ["a_run_declared_at_open_reaches_the_log_while_every_checkpoint_is_pinned"], "KILL"),
    ("M16_open_ignores_declarations", "src/wal/recovery.rs",
     "    if rebuild || holds_records || declares {\n", "    if rebuild || holds_records {\n",
     [LIB], [R + "an_open_over_an_empty_log_still_declares_every_table"], "KILL"),
    # The D234 adversary's F1: each half of the replay held to the rule on its own.
    ("M17_unconditional_schema_replay", "src/wal/txn.rs",
     "        if truncation == Truncation::Truncated {\n",
     "        self.replay_schema()?;\n        if truncation == Truncation::Truncated {\n",
     [LIB], [T + "a_checkpoint_a_pin_kept_from_truncating_re_declares_nothing"], "KILL"),
    ("M18_unconditional_run_replay", "src/wal/txn.rs",
     "        if truncation == Truncation::Truncated {\n",
     "        self.replay_runs()?;\n        if truncation == Truncation::Truncated {\n",
     [LIB, D234], [T + "a_checkpoint_a_pin_kept_from_truncating_re_declares_nothing",
                   "a_run_declared_at_open_reaches_the_log_while_every_checkpoint_is_pinned"], "KILL"),
    # The D234 adversary's F7: the decoder's history collapse, whose natural trigger D234 removed.
    ("M19_history_never_collapses", "src/replication/logical.rs",
     "        if drop.is_empty() {\n            return;\n        }\n",
     "        if true {\n            return;\n        }\n",
     [HISTORY], ["identical_declarations_under_a_held_pin_do_not_grow_the_decoders_history"], "KILL"),
]

FAILED_LINE = re.compile(r"^test (\S+) \.\.\. FAILED$")
RESULT_LINE = re.compile(r"^test result: \w+\. (\d+) passed; (\d+) failed")


def git(*args, cwd=None, check=True):
    return subprocess.run(["git", *args], cwd=cwd, check=check, capture_output=True, text=True)


def run(cmd, env):
    """Run `cmd` in the throwaway tree in its own process group; on timeout kill the whole group, so
    no test binary or spawned ferrodb outlives it. Returns (rc, output)."""
    p = subprocess.Popen(cmd, cwd=TREE, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         text=True, start_new_session=True)
    try:
        out, _ = p.communicate(timeout=TIMEOUT)
        return p.returncode, out
    except subprocess.TimeoutExpired:
        os.killpg(p.pid, signal.SIGKILL)
        out, _ = p.communicate()
        return 124, (out or "") + "\n<TIMEOUT: process group killed>"


def run_targets(targets, env):
    """Run each target. Returns (log, failed test names, problems): a problem is a nonzero exit with
    no FAILED test to account for it, a compile error, a timeout, or a target that ran no test."""
    log, failed, problems = [], set(), []
    for target in targets:
        if target == BUILD_EXAMPLES:
            cmd = ["cargo", "build", "--examples"]
        else:
            cmd = ["cargo", "test", *target]
        rc, out = run(cmd, env)
        log.append(f"$ {' '.join(cmd)}\nrc={rc}\n{out}")
        mine = set()
        ran = 0
        for line in out.splitlines():
            line = line.strip()
            m = FAILED_LINE.match(line)
            if m:
                mine.add(m.group(1))
            r = RESULT_LINE.match(line)
            if r:
                ran += int(r.group(1)) + int(r.group(2))
        failed |= mine
        label = " ".join(target)
        if "error[E" in out or "could not compile" in out:
            problems.append(f"{label}: COMPILE-ERROR")
        elif rc == 124:
            problems.append(f"{label}: TIMEOUT")
        elif target != BUILD_EXAMPLES and ran == 0:
            problems.append(f"{label}: RAN-NOTHING")
        elif rc != 0 and not mine:
            problems.append(f"{label}: rc={rc} with no FAILED test")
    return log, failed, problems


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    tip = sys.argv[1]
    env = dict(os.environ)
    if "--target-dir" in sys.argv:
        env["CARGO_TARGET_DIR"] = sys.argv[sys.argv.index("--target-dir") + 1]
    if os.path.exists(TREE):
        sys.exit(f"refusing: {TREE} already exists; remove it deliberately or pick another path")
    os.makedirs(OUT, exist_ok=True)
    git("worktree", "add", "--detach", TREE, tip, cwd=REPO)
    summary = []
    try:
        # CONTROL: every target the mutants use, on the unmutated tip. A test that already fails here
        # cannot be credited to a mutant, and a target that cannot run makes every verdict on it void.
        all_targets = []
        for m in MUTANTS:
            for t in m[4]:
                if t not in all_targets:
                    all_targets.append(t)
        log, control_failed, control_problems = run_targets(all_targets, env)
        open(os.path.join(OUT, "CONTROL.txt"), "w").write("\n".join(log))
        summary.append(f"CONTROL failed={sorted(control_failed)} problems={control_problems}")
        if control_problems or control_failed:
            summary.append("CONTROL is not clean: every verdict below is VOID until it is")

        for name, path, old, new, targets, expected, expect in MUTANTS:
            git("checkout", "--", ".", cwd=TREE)
            if git("status", "--porcelain", "--untracked-files=no", cwd=TREE).stdout.strip():
                raise SystemExit(f"{name}: the throwaway tree is not clean before applying it")
            full = os.path.join(TREE, path)
            text = open(full).read()
            count = text.count(old)
            if count != 1:
                summary.append(f"{name} PATTERN-MISMATCH ({count} matches) expect={expect}")
                continue
            open(full, "w").write(text.replace(old, new))
            log, failed, problems = run_targets(targets, env)
            open(os.path.join(OUT, f"{name}.txt"), "w").write("\n".join(log))
            credited = failed - control_failed
            extra = sorted(credited - set(expected))
            if problems:
                verdict = f"VOID ({problems})"
            elif expected:
                missing = [t for t in expected if t not in credited]
                verdict = "KILLED" if not missing else f"SURVIVED (not failed: {missing})"
            else:
                verdict = "SURVIVED" if not credited else "KILLED-UNEXPECTEDLY"
            if extra:
                verdict += f" (also failed: {extra})"
            summary.append(f"{name} {verdict} expect={expect}")
    finally:
        git("checkout", "--", ".", cwd=TREE, check=False)
        git("worktree", "remove", "--force", TREE, cwd=REPO, check=False)
        open(os.path.join(OUT, "summary.txt"), "w").write("\n".join(summary) + "\n")
    print("\n".join(summary))


if __name__ == "__main__":
    main()
