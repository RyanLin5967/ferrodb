#!/usr/bin/env python3
"""D216 / D227 / D234 / D247 / D252 mutants, each applied ALONE to a throwaway checkout of the tip.

Usage: python3 bench/d216/mutants.py <tip-sha> [--target-dir DIR] [--only M26,...] [--skip M26,...]

At `d216-clean-restart`, run with `--skip M29_history_covers_to_the_bound,M30_refusal_passes_its_rows,M31_only_the_refused_commit_is_held,M32_refusal_clamps_to_the_last_row,M33_gap_fill_covers_to_the_cursor`:
those five mutate code only `d252-caught-up-pin` has, under a test target (the lib) both tips have.

A mutant whose `--test` target the tip does not have is reported NOT-AT-TIP and left out of
everything, CONTROL included: at `d216-clean-restart` that is M26-M28, and nothing else. A pattern
that matches anything but once, where the targets exist, is a PATTERN-MISMATCH, never an absence.
Mutants left out by `--only` or `--skip` are listed in the summary as SKIPPED.

`--only` runs the named mutants and nothing else, and its CONTROL covers only their targets. On
`d252-caught-up-pin`, run `--only M26_cursor_stops_at_the_commit,M27_txn_end_after_the_flush,M28_pin_never_moves,M29_history_covers_to_the_bound,M30_refusal_passes_its_rows,M31_only_the_refused_commit_is_held,M32_refusal_clamps_to_the_last_row,M33_gap_fill_covers_to_the_cursor`
until the lead rules on the lane report's D252 ⚖: that branch's commit change takes away the
buffered `TxnEnd` two D216 gate negative controls use as their premise, so they fail there, a full
CONTROL is not clean, and every verdict would be VOID.

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
HISTORY = ("--test", "d234_decoder_history_still_collapses")
ARENA = ("--test", "d247_arena_page_write_back_flushes_no_log")
STREAM = ("--lib", "replication::stream::")
S = "replication::stream::tests::"
D252 = ("--test", "d252_caught_up_subscription_lets_the_log_truncate")
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
     "            LogDependency::Through => wal.flush_through(frame.wal_mark.load(Ordering::Relaxed))?,\n",
     "            LogDependency::Through => {}\n",
     [LIB], [R + "an_index_page_reaches_disk_only_after_the_log_records_it_depends_on"], "KILL"),
    ("M7_gate_flushes_whole_log", "src/buffer/buffer_pool.rs",
     "            LogDependency::Through => wal.flush_through(frame.wal_mark.load(Ordering::Relaxed))?,\n",
     "            LogDependency::Through => wal.flush()?,\n",
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
                   "a_restarted_process_redeclares_every_table_and_no_run_it_did_not_bind"], "KILL"),
    ("M12_marker_not_durable", "src/wal/txn.rs",
     "        let written = crate::storage::atomic_file::replace_atomically(\n"
     "            &crate::storage::atomic_file::OsFileOps,\n"
     "            &marker,\n"
     "            format!(\"txn {txn_id}: {e}\\n\").as_bytes(),\n"
     "        );\n",
     "        let written = std::fs::write(&marker, format!(\"txn {txn_id}: {e}\\n\"));\n",
     [LIB], [], "SURVIVE"),
    # D234, the lead's mutant: the unconditional replay restored.
    ("M14_unconditional_replay", "src/wal/txn.rs",
     "            Truncation::Kept { newest_pin, .. } => {\n                if newest_pin > self.schema_declared_at.load(Ordering::SeqCst) {\n                    self.replay_schema()?;\n                }\n            }\n",
     "            Truncation::Kept { .. } => {\n                self.replay_schema()?;\n                self.replay_runs()?;\n            }\n",
     [LIB], [T + "a_kept_checkpoint_re_declares_no_run_and_the_schema_only_when_a_pin_has_passed_it"], "KILL"),
    # The D234 adversary's F1 and the lead's 09:47Z rule: each part held on its own.
    ("M17_unconditional_schema_replay", "src/wal/txn.rs",
     "            Truncation::Kept { newest_pin, .. } => {\n                if newest_pin > self.schema_declared_at.load(Ordering::SeqCst) {\n                    self.replay_schema()?;\n                }\n            }\n",
     "            Truncation::Kept { .. } => {\n                self.replay_schema()?;\n            }\n",
     [LIB], [T + "a_kept_checkpoint_re_declares_no_run_and_the_schema_only_when_a_pin_has_passed_it"], "KILL"),
    ("M18_runs_replayed_under_a_pin", "src/wal/txn.rs",
     "                    self.replay_schema()?;\n                }\n            }\n",
     "                    self.replay_schema()?;\n                    self.replay_runs()?;\n                }\n            }\n",
     [LIB], [T + "a_kept_checkpoint_re_declares_no_run_and_the_schema_only_when_a_pin_has_passed_it"], "KILL"),
    ("M23_schema_never_redeclared_under_a_pin", "src/wal/txn.rs",
     "            Truncation::Kept { newest_pin, .. } => {\n                if newest_pin > self.schema_declared_at.load(Ordering::SeqCst) {\n                    self.replay_schema()?;\n                }\n            }\n",
     "            Truncation::Kept { .. } => {}\n",
     [LIB], [T + "a_kept_checkpoint_re_declares_no_run_and_the_schema_only_when_a_pin_has_passed_it"], "KILL"),
    ("M24_oldest_pin_decides", "src/wal/txn.rs",
     "            Truncation::Kept { newest_pin, .. } => {\n",
     "            Truncation::Kept { oldest_pin: newest_pin, .. } => {\n",
     [LIB], [T + "a_kept_checkpoint_re_declares_no_run_and_the_schema_only_when_a_pin_has_passed_it"], "KILL"),
    ("M25_declaration_position_never_recorded", "src/wal/txn.rs",
     "        self.schema_declared_at.store(self.wal.next_lsn.load(Ordering::SeqCst), Ordering::SeqCst);\n", "",
     [LIB], [T + "a_kept_checkpoint_re_declares_no_run_and_the_schema_only_when_a_pin_has_passed_it"], "KILL"),
    ("M16_open_ignores_declarations", "src/wal/recovery.rs",
     "    if rebuild || holds_records || declares {\n", "    if rebuild || holds_records {\n",
     [LIB], [R + "an_open_over_an_empty_log_still_declares_every_table"], "KILL"),
    # The D234 adversary's F7: the decoder's history collapse, whose natural trigger D234 removed.
    ("M19_history_never_collapses", "src/replication/logical.rs",
     "        if drop.is_empty() {\n            return;\n        }\n",
     "        if true {\n            return;\n        }\n",
     [HISTORY], ["identical_declarations_under_a_held_pin_do_not_grow_the_decoders_history"], "KILL"),
    # D247: an arena page must be recognised positively, and a table page only when it names itself.
    ("M20_no_page_id_check", "src/buffer/buffer_pool.rs",
     "    let names_itself = page_id.is_some_and(|id| data[1..5] == id.to_be_bytes());\n",
     "    let names_itself = page_id.is_some() || true;\n",
     [ARENA], ["writing_back_a_dirty_arena_page_does_not_flush_the_log"], "KILL"),
    ("M21_no_arena_recognition", "src/buffer/buffer_pool.rs",
     "        _ if crate::cow::page_header::verify_checksum(data) => LogDependency::Nothing,\n", "",
     [ARENA], ["writing_back_a_dirty_arena_page_does_not_flush_the_log"], "KILL"),
    # The D227 run half, withdrawn at 09:44Z: an open that re-declares every run ever interned.
    ("M22_open_redeclares_runs", "src/wal/recovery.rs",
     "    let rebuild = recovered || stale;\n",
     "    for run in crate::provenance::DurableProvenanceStore::open(format!(\"{}.provenance\", db_path.display()))?.runs()? {\n"
     "        txn.declare_run(run)?;\n    }\n    let rebuild = recovered || stale;\n",
     [D227], ["a_restarted_process_redeclares_every_table_and_no_run_it_did_not_bind"], "KILL"),
    # D252: a caught-up subscription lets the log truncate. Each half, and the pin that carries it.
    ("M26_cursor_stops_at_the_commit", "src/replication/stream.rs",
     "            (None, None) => decoded.walked_to.max(emitted_max),\n",
     "            (None, None) => emitted_max,\n",
     [D252], ["a_subscription_that_has_read_every_commit_lets_the_checkpoint_truncate",
              "a_subscription_passes_a_durable_rollback_after_the_last_commit"], "KILL"),
    ("M27_txn_end_after_the_flush", "src/wal/txn.rs",
     "        let end = self.append_chained(txn_id, &RecKind::TxnEnd);\n        self.wal.flush_up_to(commit_lsn)?;\n        let _ = end?;\n",
     "        self.wal.flush_up_to(commit_lsn)?;\n        let _ = self.append_chained(txn_id, &RecKind::TxnEnd)?;\n",
     [D252], ["a_subscription_that_has_read_every_commit_lets_the_checkpoint_truncate"], "KILL"),
    ("M28_pin_never_moves", "src/replication/stream.rs",
     "            let next = self.wal.pin(pumped.cursor)?;\n            let old = std::mem::replace(&mut self.pin, next);\n            drop(old);\n",
     "",
     [D252], ["a_subscription_that_has_read_every_commit_lets_the_checkpoint_truncate",
              "a_subscription_passes_a_durable_rollback_after_the_last_commit"], "KILL"),
    # The D252 review's B1: the decoder's history covered through a bound inside a record.
    ("M29_history_covers_to_the_bound", "src/replication/logical.rs",
     "            history.covered_through = history.covered_through.max(out.walked_to);\n",
     "            history.covered_through = history.covered_through.max(to_lsn);\n",
     [STREAM], [S + "a_bound_inside_a_commit_does_not_break_the_next_pump",
                S + "a_large_backlog_is_delivered_in_bounded_batches"], "KILL"),
    # Found under D252: a refusal's cursor passed the refused transaction's rows when another
    # transaction committed inside it.
    ("M30_refusal_passes_its_rows", "src/replication/stream.rs",
     "        let next = refused_from.map_or(next, |first| next.min(first));\n",
     "        let next = refused_from.map_or(next, |_first| next);\n",
     [STREAM], [S + "a_refused_transaction_is_not_stepped_over_by_one_that_committed_inside_it",
                S + "every_commit_a_refusal_holds_back_keeps_its_rows"], "KILL"),
    # D252 review 3: the clamp covers every held-back commit (not only the refused one), at its FIRST
    # row; and review 2's finding 1, the gap fill's watermark on a boundary.
    ("M31_only_the_refused_commit_is_held", "src/replication/stream.rs",
     ".filter(|e| e.commit_lsn >= c).map(|e| e.lsn).min());\n",
     ".filter(|e| e.commit_lsn == c).map(|e| e.lsn).min());\n",
     [STREAM], [S + "every_commit_a_refusal_holds_back_keeps_its_rows"], "KILL"),
    ("M32_refusal_clamps_to_the_last_row", "src/replication/stream.rs",
     ".filter(|e| e.commit_lsn >= c).map(|e| e.lsn).min());\n",
     ".filter(|e| e.commit_lsn >= c).map(|e| e.lsn).max());\n",
     [STREAM], [S + "every_commit_a_refusal_holds_back_keeps_its_rows"], "KILL"),
    # The one-detector item: the snapshot install asks `Truncation`; this mutant ignores the answer.
    ("M34_install_ignores_a_kept_log", "src/consensus/snapshot.rs",
     "        if let crate::wal::log::Truncation::Kept { oldest_pin, .. } = self.wal.truncate(0)? {\n",
     "        if let crate::wal::log::Truncation::Kept { oldest_pin, .. } = { let _ = self.wal.truncate(0)?; crate::wal::log::Truncation::Truncated } {\n",
     [("--test", "integration_cluster_snapshot", "an_install_under_a_wal_pin_is_refused_and_names_the_pin")],
     ["an_install_under_a_wal_pin_is_refused_and_names_the_pin"], "KILL"),
    ("M33_gap_fill_covers_to_the_cursor", "src/replication/logical.rs",
     "                history.covered_through = scanned_to;\n",
     "                history.covered_through = from_lsn;\n                let _ = scanned_to;\n",
     [STREAM], [S + "a_resume_cursor_inside_a_record_does_not_break_later_pumps"], "KILL"),
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
    mutants = MUTANTS
    skipped = []
    for flag in ("--only", "--skip"):
        if flag in sys.argv:
            at = sys.argv.index(flag) + 1
            if at >= len(sys.argv):
                sys.exit(f"refusing: {flag} needs a comma-separated list of mutant names")
            named = sys.argv[at].split(",")
            unknown = [w for w in named if w not in {m[0] for m in MUTANTS}]
            if unknown or not named:
                sys.exit(f"refusing: {flag} names no mutant or unknown ones: {unknown}")
            keep = flag == "--only"
            skipped += [m[0] for m in mutants if (m[0] in named) != keep]
            mutants = [m for m in mutants if (m[0] in named) == keep]
    env = dict(os.environ)
    if "--target-dir" in sys.argv:
        env["CARGO_TARGET_DIR"] = sys.argv[sys.argv.index("--target-dir") + 1]
    if os.path.exists(TREE):
        sys.exit(f"refusing: {TREE} already exists; remove it deliberately or pick another path")
    os.makedirs(OUT, exist_ok=True)
    git("worktree", "add", "--detach", TREE, tip, cwd=REPO)
    summary = [f"{name} SKIPPED (by --only/--skip)" for name in skipped]
    try:
        # NOT-AT-TIP: a mutant whose test target the tip does not have is reported and left out,
        # CONTROL included, so one script serves `d216-clean-restart` (no D252 test) and
        # `d252-caught-up-pin`. Reported, never silent: the pre-registration names which mutants each
        # tip must list here.
        present = []
        for m in mutants:
            name, path, old = m[0], m[1], m[2]
            count = open(os.path.join(TREE, path)).read().count(old)
            missing = [t[1] for t in m[4] if t != BUILD_EXAMPLES and t[0] == "--test"
                       and not os.path.exists(os.path.join(TREE, "tests", t[1] + ".rs"))]
            # Only a missing TEST TARGET makes a mutant absent by design; a pattern that matches
            # nothing where its targets exist is drift, and is reported as a mismatch (the D252
            # review's m5: counting 0 as absent would hide a D216 pattern that had stopped matching).
            if missing:
                summary.append(f"{name} NOT-AT-TIP (absent targets {missing}; pattern matches {count}) expect={m[6]}")
            elif count != 1:
                summary.append(f"{name} PATTERN-MISMATCH ({count} matches) expect={m[6]}")
            else:
                present.append(m)
        mutants = present
        # CONTROL: every target the mutants use, on the unmutated tip. A test that already fails here
        # cannot be credited to a mutant, and a target that cannot run makes every verdict on it void.
        all_targets = []
        for m in mutants:
            for t in m[4]:
                if t not in all_targets:
                    all_targets.append(t)
        log, control_failed, control_problems = run_targets(all_targets, env)
        open(os.path.join(OUT, "CONTROL.txt"), "w").write("\n".join(log))
        summary.append(f"CONTROL failed={sorted(control_failed)} problems={control_problems}")
        if control_problems or control_failed:
            summary.append("CONTROL is not clean: every verdict below is VOID until it is")

        for name, path, old, new, targets, expected, expect in mutants:
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
