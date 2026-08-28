# F8 — raw measurements

All numbers below are pasted from the runs that produced them. Toolchain on PATH via
`export PATH="$HOME/.cargo/bin:$PATH"`; every command bounded with `timeout`.

## Suite

`cargo test --lib` at commit fa0d92b: **844 passed, 0 failed** (27.97s).
Baseline before this branch: 820 lib tests. F8 adds 24.

## Safety sweeps, release, 100 000 seeds each

    FERRODB_SIM_SEEDS=100000 cargo test --release --lib \
      consensus::sim::tests_sim::at_most_one_leader_per_term_across_a_chaos_sweep -- --exact --nocapture

    at_most_one_leader_per_term: 100000 seeds, 346982 elections, 3013990 committed rounds, max term 12
    test result: ok. 1 passed; 0 failed ... finished in 34.61s

    a_committed_round_is_never_lost: 100000 seeds, 3013050 committed rounds, 410028 crashes, 336111 restarts
    test result: ok. 1 passed; 0 failed ... finished in 35.55s

The first 100 000-seed run of the first of these FAILED, on seed 1592682576, with
`two leaders in one term`. That was a defect in the simulator's own crash model, not in the
protocol: see `sweep-100k.txt` for the failing run and `sweep-100k-after-crashfix.txt` for the
same sweeps after the fix.

## Fault model, 60 seeds of chaos (what the sweeps actually did)

    Report { seeds: 60, ticks: 14400, sent: 44357, delivered: 33173, dropped_partition: 8191,
             dropped_loss: 3010, dropped_down: 2558, duplicated: 2729, crashes: 266, restarts: 209,
             partitions: 1230, one_way_partitions: 892, heals: 507, discarded_entries: 13,
             unsent_at_crash: 10, elections: 222, proposals: 4798, refusals: 3103,
             committed_rounds: 1708, max_term: 9, max_overlap_ticks: 0 }

892 of 1230 partitions were one-way. `max_overlap_ticks: 0` — no two nodes ever both believed
they led, on any seed, at any instant.

## Files

- `cargo-test-lib.txt` — full `cargo test --lib` output.
- `sweep-10k.txt` — the 10 000-seed sweeps.
- `sweep-100k.txt` — the 100 000-seed run that found the crash-model defect.
- `sweep-100k-after-crashfix.txt` — the same sweeps, clean.
