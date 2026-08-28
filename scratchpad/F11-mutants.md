# F11 — the mutant record

Every rule the lease thread implements, broken on purpose, with the test named against it required
to fail. Each mutant was confirmed to **compile** first, because a mutant that does not build prints
nothing and looks exactly like a surviving one.

Applied by `scratchpad/f11_mutate.py <name>`, run by `scratchpad/f11_fire.sh <name> [lib-filter]
[integration-filter]`, restored with `git checkout -- src/` after each. Reproducible in a minute
rather than trusted from this file.

## Verdict

| # | mutant | rule it breaks | test that died |
|---|---|---|---|
| M1 | `with_lock` runs the body without taking the lock | never reap inside a merge | `a_scan_does_not_reap_while_a_merge_holds_the_runtime_lock` |
| M2 | `start` skips `resume_interrupted_reaps` | resume before scanning | `start_finishes_a_reap_a_crash_interrupted_before_any_scan_runs`, `the_server_finishes_a_reap_a_crash_interrupted_before_it_serves_anything` |
| M3 | the clock falls back to `SystemTime::now()` | never guess the time | `a_node_that_does_not_know_the_clusters_time_refuses_to_reap_rather_than_guessing` |
| M4 | `scan_once` returns before scanning | the scan happens at all | `the_thread_reaps_an_expired_branch_with_nobody_asking_it_to`, `the_cli_reaps_an_abandoned_branch_...` |
| M5 | `reap_expired(u64::MAX)` — deadlines ignored | reap only what expired | `an_unexpired_branch_survives_every_scan`, `a_live_lease_survives_a_server_that_scans_continuously` |
| M6 | `forget_reaped_branches` never called | a reaped branch's workspace goes too | `a_reaped_branchs_workspace_is_forgotten_without_the_client_saying_anything` |
| M7 | `Halt::wait` sleeps instead of waiting on the condvar | stopping is prompt | **survived the first firing** — see below — then `a_signalled_halt_returns_at_once_...` and `stop_does_not_wait_out_the_scan_interval` |
| M8 | `Drop for LeaseThread` does nothing | a dropped thread is stopped | `dropping_the_lease_thread_stops_it` |
| M9 | `FERRODB_LEASE_SCAN_MILLIS` falls back to the default | refuse, never default | `the_scan_interval_knob_refuses_every_value_it_cannot_use`, `both_binaries_refuse_to_start_on_an_unusable_scan_interval` |
| M10 | `with_reaper` dropped from the CLI's runtime | a retired branch gives its extent back | `a_merged_branch_gives_its_extent_back_without_any_lease_scan` |

Nine killed on the first firing. **M7 survived**, which is the finding this pass exists for; the
test it survived was racy and has been replaced. Two mutants (M6, M9) had to be rewritten because
the first version did not apply or did not compile — a mutant that does not build prints nothing and
looks exactly like a surviving one, so neither was counted until it built.

Raw cargo output for every re-fired run is in `scratchpad/raw/`.

## Baseline, with no mutation applied

```
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 1092 filtered out; finished in 0.23s
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 1092 filtered out; finished in 0.23s
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 43.20s
```

### M1-no-runtime-lock
- compiles: yes
- `cargo test --lib a_scan_does_not_reap_while_a_merge_holds_the_runtime_lock`:
      ---- branch::lease_thread::tests::a_scan_does_not_reap_while_a_merge_holds_the_runtime_lock stdout ----
      thread 'branch::lease_thread::tests::a_scan_does_not_reap_while_a_merge_holds_the_runtime_lock' (22444721) panicked at src/branch/lease_thread/tests.rs:168:5:
      assertion `left == right` failed: nothing may be reaped inside the lock
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1102 filtered out; finished in 0.22s

### M2-no-resume
- compiles: yes
- `cargo test --lib start_finishes_a_reap_a_crash_interrupted_before_any_scan_runs`:
      ---- branch::lease_thread::tests::start_finishes_a_reap_a_crash_interrupted_before_any_scan_runs stdout ----
      thread 'branch::lease_thread::tests::start_finishes_a_reap_a_crash_interrupted_before_any_scan_runs' (22497358) panicked at src/branch/lease_thread/tests.rs:214:5:
      assertion `left == right` failed: start must report the reap it finished, not do it silently
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1102 filtered out; finished in 0.00s
- `cargo test --test integration_server_reaps the_server_finishes_a_reap_a_crash_interrupted_before_it_serves_anything`:
      ---- the_server_finishes_a_reap_a_crash_interrupted_before_it_serves_anything stdout ----
      thread 'the_server_finishes_a_reap_a_crash_interrupted_before_it_serves_anything' (22509473) panicked at tests/integration_server_reaps.rs:303:9:
      pgserver never printed "lease: finished 1 reap(s) a crash interrupted" in 60s. Its stderr:
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 7 filtered out; finished in 153.99s

### M3-guess-the-clock
- compiles: yes
- `cargo test --test integration_server_reaps a_node_that_does_not_know_the_clusters_time_refuses_to_reap_rather_than_guessing`:
      ---- a_node_that_does_not_know_the_clusters_time_refuses_to_reap_rather_than_guessing stdout ----
      thread 'a_node_that_does_not_know_the_clusters_time_refuses_to_reap_rather_than_guessing' (22923907) panicked at tests/integration_server_reaps.rs:773:5:
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 7 filtered out; finished in 60.01s

### M4-scan-does-nothing
- compiles: yes
- `cargo test --lib the_thread_reaps_an_expired_branch_with_nobody_asking_it_to`:
      ---- branch::lease_thread::tests::the_thread_reaps_an_expired_branch_with_nobody_asking_it_to stdout ----
      
      thread 'branch::lease_thread::tests::the_thread_reaps_an_expired_branch_with_nobody_asking_it_to' (23031981) panicked at src/branch/lease_thread/tests.rs:95:5:
      waited 10s for the background scan to reap the expired branch and it never happened
      note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1102 filtered out; finished in 10.01s
      
      error: test failed, to rerun pass `--lib`
- `cargo test --test integration_server_reaps the_cli_reaps_an_abandoned_branch_with_no_client_action_and_pages_return_to_baseline`:
      ---- the_cli_reaps_an_abandoned_branch_with_no_client_action_and_pages_return_to_baseline stdout ----
      
      thread 'the_cli_reaps_an_abandoned_branch_with_no_client_action_and_pages_return_to_baseline' (23044664) panicked at tests/integration_server_reaps.rs:219:9:
      ferrodb never printed "lease: reaped" in 60s. Its output:
      ferrodb: type .exit to quit
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 7 filtered out; finished in 124.86s
      
      error: test failed, to rerun pass `--test integration_server_reaps`

### M5-reap-unconditionally
- compiles: yes
- `cargo test --lib an_unexpired_branch_survives_every_scan`:
      ---- branch::lease_thread::tests::an_unexpired_branch_survives_every_scan stdout ----
      
      thread 'branch::lease_thread::tests::an_unexpired_branch_survives_every_scan' (23308201) panicked at src/branch/lease_thread/tests.rs:303:5:
      assertion `left == right` failed: a live lease was reaped
        left: 1
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1102 filtered out; finished in 0.04s
      
      error: test failed, to rerun pass `--lib`
- `cargo test --test integration_server_reaps a_live_lease_survives_a_server_that_scans_continuously`:
      ---- a_live_lease_survives_a_server_that_scans_continuously stdout ----
      
      thread 'a_live_lease_survives_a_server_that_scans_continuously' (23314698) panicked at tests/integration_server_reaps.rs:541:5:
      a live lease was reaped:
      pgserver: lease scan every 100ms; 0 interrupted reap(s) finished on startup
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 7 filtered out; finished in 74.27s
      
      error: test failed, to rerun pass `--test integration_server_reaps`

### M7-stop-sleeps-the-interval
- compiles: yes
- `cargo test --lib stop_does_not_wait_out_the_scan_interval`:
      test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1102 filtered out; finished in 0.00s
      

### M8-drop-does-not-stop
- compiles: yes
- `cargo test --lib dropping_the_lease_thread_stops_it`:
      ---- branch::lease_thread::tests::dropping_the_lease_thread_stops_it stdout ----
      
      thread 'branch::lease_thread::tests::dropping_the_lease_thread_stops_it' (23496791) panicked at src/branch/lease_thread/tests.rs:381:5:
      assertion `left == right` failed: the scan thread is still running after its owner dropped
        left: 19
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1102 filtered out; finished in 0.23s
      
      error: test failed, to rerun pass `--lib`

### M9-knob-defaults
- compiles: yes
- `cargo test --lib -- the_scan_interval_knob_refuses_every_value_it_cannot_use`:
      ---- branch::lease_thread::tests::the_scan_interval_knob_refuses_every_value_it_cannot_use stdout ----
      
      thread 'branch::lease_thread::tests::the_scan_interval_knob_refuses_every_value_it_cannot_use' (23664297) panicked at src/branch/lease_thread/tests.rs:460:44:
      called `Result::unwrap_err()` on an `Ok` value: 30s
      note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1103 filtered out; finished in 0.00s
      
      error: test failed, to rerun pass `--lib`
- `cargo test --test integration_server_reaps -- both_binaries_refuse_to_start_on_an_unusable_scan_interval`:
      ---- both_binaries_refuse_to_start_on_an_unusable_scan_interval stdout ----
      
      thread 'both_binaries_refuse_to_start_on_an_unusable_scan_interval' (23670049) panicked at tests/integration_server_reaps.rs:686:9:
      the CLI accepted FERRODB_LEASE_SCAN_MILLIS="off" (exit Some(0)):
      
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 7 filtered out; finished in 16.23s
      
      error: test failed, to rerun pass `--test integration_server_reaps`

### M6-never-forget
- compiles: yes
- `cargo test --lib -- a_reaped_branchs_workspace_is_forgotten_without_the_client_saying_anything`:
      ---- branch::lease_thread::tests::a_reaped_branchs_workspace_is_forgotten_without_the_client_saying_anything stdout ----
      
      thread 'branch::lease_thread::tests::a_reaped_branchs_workspace_is_forgotten_without_the_client_saying_anything' (23695219) panicked at src/branch/lease_thread/tests.rs:95:5:
      waited 10s for the abandoned session to be reaped and forgotten and it never happened
      note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1103 filtered out; finished in 10.01s
      
      error: test failed, to rerun pass `--lib`

### M7-stop-sleeps-the-interval
- compiles: yes
- `cargo test --lib -- a_signalled_halt_returns_at_once_instead_of_sleeping_the_interval stop_does_not_wait_out_the_scan_interval`:
      ---- branch::lease_thread::tests::a_signalled_halt_returns_at_once_instead_of_sleeping_the_interval stdout ----
      
      thread 'branch::lease_thread::tests::a_signalled_halt_returns_at_once_instead_of_sleeping_the_interval' (23713767) panicked at src/branch/lease_thread/tests.rs:359:5:
      the signalled wait to return had not happened 10s after it was signalled
      note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
      ---- branch::lease_thread::tests::stop_does_not_wait_out_the_scan_interval stdout ----
      
      thread 'branch::lease_thread::tests::stop_does_not_wait_out_the_scan_interval' (23713768) panicked at src/branch/lease_thread/tests.rs:359:5:
      stop() to return had not happened 10s after it was signalled
      
      test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 1102 filtered out; finished in 10.27s
      
      error: test failed, to rerun pass `--lib`

### M10-reaper-not-attached
- compiles: yes
- `cargo test --test integration_server_reaps -- a_merged_branch_gives_its_extent_back_without_any_lease_scan`:
      ---- a_merged_branch_gives_its_extent_back_without_any_lease_scan stdout ----
      
      thread 'a_merged_branch_gives_its_extent_back_without_any_lease_scan' (23771235) panicked at tests/integration_server_reaps.rs:589:5:
      the store still lists an extent owned by the merged branch: [(ArenaId(1), BranchId { id: 0, generation: 0 }), (ArenaId(2), BranchId { id: 1, generation: 0 })]
      note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
      test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 7 filtered out; finished in 49.33s
      
      error: test failed, to rerun pass `--test integration_server_reaps`
