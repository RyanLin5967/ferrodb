#!/usr/bin/env python3
"""F10 mutant firing. Break one rule at a time; the named test MUST fail.

A rule with no mutant is a rule nobody has shown matters. Every entry below names the rule, the
exact edit that breaks it, and the test that has to notice. Run from the worktree root:

    PATH="$HOME/.cargo/bin:$PATH" python3 scratchpad/mutants-f10.py

Refuses if the tree is dirty, and restores every file it touched even on a crash.

**Two mutants kill by aborting the test binary rather than failing an assertion** — the ones that
remove a recursion bound, where the detection IS the stack overflow. On macOS an abort wakes
`ReportCrash` and `spindump`, which held the dead process for over two minutes in the 2026-08-28
run at load 6, so budget minutes rather than seconds for those two points. The `timeout 600` per
test is what bounds it. Measured, not guessed: `ps -o state=` showed the aborted binary at 0.0%%
CPU in state SN with `ReportCrash daemon` and `spindump` both live.
"""
import subprocess, sys, os, shutil, tempfile

LOG = "src/tel/log.rs"
TESTS = "src/tel/tests_durable_log.rs"
ITEST = "tests/integration_durable_tel.rs"

MUTANTS = [
    # (name, file, find, replace, tests that must fail)
    ("retry-writes-a-second-copy", LOG,
     "            Some(Reappend::Retry) => return Ok(()),",
     "            Some(Reappend::Retry) => encode_extend(frame, 0, 0, 0)?,",
     ["a_retried_frame_does_not_double_its_add_across_a_restart"]),

    ("growth-rewrites-the-whole-frame", LOG,
     "            Some(Reappend::Grew { ops, guards, claims }) => {\n                encode_extend(frame, ops, guards, claims)?\n            }",
     "            Some(Reappend::Grew { ops: _, guards: _, claims: _ }) => {\n                encode_extend(frame, 0, 0, 0)?\n            }",
     ["a_growing_frame_replays_to_its_final_contents_and_not_to_the_sum_of_its_appends"]),

    ("no-arithmetic-hole-check", LOG,
     "                    if (prior_ops, prior_guards, prior_claims)\n                        != (frame.ops.len(), frame.guards.len(), frame.claims.len())",
     "                    if false && (prior_ops, prior_guards, prior_claims)\n                        != (frame.ops.len(), frame.guards.len(), frame.claims.len())",
     ["a_delta_whose_prior_counts_do_not_line_up_is_refused"]),

    ("undeclared-extend-is-healed", LOG,
     "                    let frame = built.get_mut(&(branch, txn)).ok_or_else(|| {",
     "                    built.entry((branch, txn)).or_insert_with(|| { order.push((branch, txn)); TxnFrame::new(txn, branch, CommitHash::ZERO, 0, 1) });\n                    let frame = built.get_mut(&(branch, txn)).ok_or_else(|| {",
     ["an_extend_for_a_frame_the_file_never_declared_is_refused"]),

    ("second-open-overwrites", LOG,
     "                    if built.contains_key(&(branch, txn)) {",
     "                    if false && built.contains_key(&(branch, txn)) {",
     ["a_second_open_for_one_key_is_refused"]),

    ("trailing-bytes-ignored", LOG,
     "    if at != body.len() {",
     "    if false && at != body.len() {",
     ["an_unknown_tag_and_a_record_with_trailing_bytes_are_both_refused"]),

    ("torn-tail-not-healed", LOG,
     "            file.set_len(good_end)\n                .map_err(|e| FerroError::Io(format!(\"{name}: truncate torn tail: {e}\")))?;",
     "            let _ = good_end;",
     ["a_partial_append_is_discarded_reported_and_then_written_over"]),

    ("torn-tail-not-reported", LOG,
     "        Ok((RecoveryReport { frames, extensions, discarded_tail_bytes }, good_end))",
     "        Ok((RecoveryReport { frames, extensions, discarded_tail_bytes: 0 }, good_end))",
     ["a_partial_append_is_discarded_reported_and_then_written_over"]),

    ("header-crc-unchecked", LOG,
     "        if crc32(&header[0..8]) != u32::from_be_bytes(header[8..12].try_into().unwrap()) {",
     "        if false && crc32(&header[0..8]) != u32::from_be_bytes(header[8..12].try_into().unwrap()) {",
     ["a_foreign_file_and_a_damaged_header_are_both_refused"]),

    ("unguarded-string-length", LOG,
     "    if s.len() > u16::MAX as usize {",
     "    if false && s.len() > u16::MAX as usize {",
     ["a_value_too_long_for_its_length_prefix_is_refused_rather_than_truncated"]),

    ("no-guard-depth-cap-on-decode", LOG,
     "fn take_guard_expr(body: &[u8], at: &mut usize, depth: u32) -> Result<GuardExpr, FerroError> {\n    if depth > MAX_GUARD_DEPTH {",
     "fn take_guard_expr(body: &[u8], at: &mut usize, depth: u32) -> Result<GuardExpr, FerroError> {\n    if false && depth > MAX_GUARD_DEPTH {",
     ["a_guard_nested_past_the_cap_is_refused_on_the_way_in_and_on_the_way_out"]),

    ("no-guard-depth-cap-on-encode", LOG,
     "    if depth > MAX_GUARD_DEPTH {\n        // `owner.source_text` and NOT",
     "    if false && depth > MAX_GUARD_DEPTH {\n        // `owner.source_text` and NOT",
     ["a_guard_nested_past_the_cap_is_refused_on_the_way_in_and_on_the_way_out"]),

    ("index-accepts-before-the-record-lands", LOG,
     "        Self::write_record(&*inner.file, &rec, at)?;\n        self.mem.append(frame)?;",
     "        self.mem.append(frame)?;\n        Self::write_record(&*inner.file, &rec, at)?;",
     ["a_failed_append_leaves_a_store_that_still_works_and_a_file_that_still_opens"]),

    ("torn-first-header-bricks-the-log", LOG,
     "        let (recovery, end) = if len <= HEADER_SIZE {",
     "        let (recovery, end) = if len == 0 {",
     ["a_crash_at_any_point_during_an_append_loses_nothing_already_acknowledged",
      "a_foreign_file_and_a_damaged_header_are_both_refused"]),

    ("frames_for-loses-its-ordering", LOG,
     "        out.sort_by_key(|f| (f.seq, f.txn_id.0));",
     "        out.reverse();",
     ["frames_come_back_in_sequence_order_per_branch"]),

    # --- the rules the adversarial pass added ----------------------------------------------------
    ("read-error-swallowed-as-end-of-file", LOG,
     "            read_or_refuse(file, &mut rec, offset, name)?;",
     "            if pread_all(file, &mut rec, offset).is_err() { break offset; }",
     ["a_read_error_mid_file_refuses_the_open_and_destroys_nothing"]),

    ("extend-drops-guards-and-claims", LOG,
     "    put_tail(&mut body, &frame.ops[ops..], &frame.guards[guards..], &frame.claims[claims..])?;",
     "    let _ = (guards, claims);\n    put_tail(&mut body, &frame.ops[ops..], &[], &[])?;",
     ["a_growth_whose_tail_carries_guards_and_claims_replays_with_them"]),

    ("nan-delta-breaks-a-retry", LOG,
     "        (Delta::Float(x), Delta::Float(y)) => x.total_cmp(y) == std::cmp::Ordering::Equal,\n        _ => a == b,",
     "        _ => a == b,",
     ["a_nan_delta_does_not_turn_a_retry_into_a_contradiction"]),

    ("non-canonical-boolean-accepted", LOG,
     "            1 => Value::Boolean(true),",
     "            1 | 2 => Value::Boolean(true),",
     ["a_boolean_byte_that_is_neither_zero_nor_one_is_refused"]),

    ("depth-refusal-walks-the-tree-it-refuses", LOG,
     '                None => "it was synthesised and carries no source text".to_string(),',
     '                None => owner.violated_predicate(),',
     ["a_guard_nested_past_the_cap_is_refused_on_the_way_in_and_on_the_way_out"]),

    ("mem-replaces-instead-of-extending", LOG,
     "                    let stored = &mut frames[i];\n                    stored.ops.extend_from_slice(&frame.ops[ops..]);\n                    stored.guards.extend_from_slice(&frame.guards[guards..]);\n                    stored.claims.extend_from_slice(&frame.claims[claims..]);",
     "                    let _ = (ops, guards, claims);\n                    frames[i] = frame.clone();",
     ["retyping_a_stored_value_under_growth_cannot_split_the_two_stores"]),

    # The integration test's own anti-vacuity. If the "durable" arm is quietly in-memory, every
    # restart assertion in tests/integration_durable_tel.rs must fail — which is what stops that
    # file from passing because a runtime happens to hold its frames in the same process.
    ("the-durable-arm-is-secretly-in-memory", ITEST,
     '        DurableEffectLog::default_for_database(db.to_str().unwrap()).unwrap()\n    } else {',
     '        Arc::new(MemEffectLog::new())\n    } else {',
     ["itest:an_agent_tasks_frames_and_its_merge_survive_a_process_restart",
      "itest:wide_typed_values_survive_the_restart_with_their_bytes_intact"]),
]


def run(cmd, timeout=900):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)


def named_tests_fail(tests):
    """Run each named test alone. Returns (all_failed, per_test_verdict).

    ONE cargo invocation per test, filtered by the bare function name — `cargo test`'s filter is a
    substring match on the full test path, and every name here is unique in the crate. An earlier
    version chained two invocations with `||` and then parsed their concatenated output, which
    classified a KILLED mutant as "collected nothing": the second run's "0 passed; 0 failed" landed
    in the same buffer as the first run's "1 failed". A harness that mis-reports a killed mutant as
    a survivor is worse than no harness, so the verdicts below are mutually exclusive and anything
    unrecognised is an explicit error rather than a default.
    """
    # **Build first, untimed-ish, so the per-test timeout measures the TEST.** The first version
    # put a 150s bound around `cargo test`, which under load 6 on this machine spent all of it in
    # the rebuild the mutant had just invalidated — so mutant 1 reported "TIMED OUT with no verdict"
    # and would have read as a survivor. A timeout that can expire in the build is not a verdict
    # about the mutant.
    # **Build only the target under test.** `cargo build --lib --tests` relinks all ~75 integration
    # binaries after every mutation — measured at over ten minutes each on this machine at load 5 —
    # and every mutant here needs exactly one of them. `--no-run` is what makes it a build.
    targets = {
        "--test integration_durable_tel" if t.startswith("itest:") else "--lib" for t in tests
    }
    for tgt in targets:
        b = run(f"timeout 900 cargo test {tgt} --no-run 2>&1")
        if "error[E" in (b.stdout + b.stderr) or "could not compile" in (b.stdout + b.stderr):
            return False, {t: "BUILD ERROR" for t in tests}

    verdicts = {}
    for t in tests:
        # 150s and not 600. Two mutants kill by aborting the test binary, and macOS's ReportCrash
        # then holds the corpse — measured at over three minutes at load 6 — so a generous timeout
        # buys nothing but wall clock. The abort message reaches the pipe immediately, before the
        # hold, so it is still in `out` when the timeout fires; a timeout that carries it is a kill,
        # and one that carries nothing is reported as a timeout rather than counted either way.
        # An `itest:` prefix names a test in the integration target rather than the lib.
        target = "--test integration_durable_tel" if t.startswith("itest:") else "--lib"
        name = t[len("itest:"):] if t.startswith("itest:") else t
        r = run(f"timeout 200 cargo test {target} {name} 2>&1")
        out = r.stdout + r.stderr
        timed_out = r.returncode == 124
        if "error[E" in out or "could not compile" in out:
            verdicts[t] = "BUILD ERROR"
        elif "0 passed; 0 failed" in out:
            verdicts[t] = "COLLECTED NOTHING (the filter matched no test)"
        elif "1 passed; 0 failed" in out:
            verdicts[t] = "PASSED (mutant survived)"
        elif "0 passed; 1 failed" in out or "panicked" in out:
            verdicts[t] = "failed (killed)"
        elif ("overflowed its stack" in out or "fatal runtime error" in out
              or "SIGABRT" in out or "signal: 6" in out or "signal: 11" in out):
            # A mutant that removes a recursion bound does not fail a test, it aborts the whole
            # test binary. That is the mutant being killed, loudly — and it has to be recognised
            # explicitly, because an abort prints no `test result:` line at all and would otherwise
            # fall through to UNRECOGNISED and read as a survivor.
            verdicts[t] = "failed (killed by a process abort: %s)" % (
                "stack overflow" if "overflowed its stack" in out else "signal")
        elif timed_out:
            verdicts[t] = "TIMED OUT with no verdict in its output"
        else:
            verdicts[t] = "UNRECOGNISED: " + " / ".join(
                l.strip() for l in out.splitlines() if "test result:" in l
            )
    return all(v.startswith("failed") for v in verdicts.values()), verdicts


def main():
    only = set(sys.argv[1:])
    if run("git status --porcelain -- src/ tests/").stdout.strip():
        print("REFUSING: src/ or tests/ is dirty; commit first so a restore is exact.")
        return 2

    baseline = run("timeout 900 cargo test --lib tel::log 2>&1")
    b = baseline.stdout + baseline.stderr
    if "test result: ok" not in b:
        print("REFUSING: the baseline is not green.\n" + b[-3000:])
        return 2
    print("baseline: " + [l for l in b.splitlines() if "test result:" in l][-1])

    rows = []
    for name, path, find, repl, tests in MUTANTS:
        if only and name not in only:
            continue
        src = open(path).read()
        n = src.count(find)
        if n != 1:
            rows.append((name, f"NOT APPLIED: the anchor matches {n} times", {}))
            print(f"[{name}] NOT APPLIED (anchor matches {n} times)")
            continue
        shutil.copy(path, path + ".orig")
        try:
            open(path, "w").write(src.replace(find, repl))
            ok, verdicts = named_tests_fail(tests)
            rows.append((name, "KILLED" if ok else "SURVIVED", verdicts))
            print(f"[{name}] {'KILLED' if ok else 'SURVIVED'}  {verdicts}")
        finally:
            shutil.move(path + ".orig", path)

    after = run("timeout 900 cargo test --lib tel::log 2>&1")
    a = after.stdout + after.stderr
    print("restored: " + [l for l in a.splitlines() if "test result:" in l][-1])
    if run("git status --porcelain -- src/ tests/").stdout.strip():
        print("WARNING: the tree is dirty after the run; a restore did not complete.")
        return 2

    print("\n=== summary ===")
    survived = [r for r in rows if r[1] != "KILLED"]
    for name, verdict, v in rows:
        print(f"{verdict:>12}  {name}")
        for t, res in v.items():
            print(f"              {t}: {res}")
    print(f"\n{len(rows) - len(survived)}/{len(rows)} killed")
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
