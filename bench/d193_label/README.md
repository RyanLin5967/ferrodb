# D193 — raw evidence for the "production DIFF path" label correction

Branch `d193-diff-label`, cut from `main` at `fc9556a`. The label fix is commit `180618a`; every
file here was produced under `/tmp/ferrodb-suite.lock` and committed before being interpreted.
Full report: `artie-research/frontier/lane_d193_label.md`.

| file | instrument | result |
|---|---|---|
| `p1_base_fc9556a_list.out` | `cargo test --test integration_production_diff_wiring -- --list` at `fc9556a` | 4 tests |
| `p1_after_180618a_list.out` | same, at `180618a` | the same 4 names. **Test-count delta 0**, as pre-registered |
| `p3_build_release_180618a.log` | `cargo build --release --example d103_production_diff_curve` | rc=0 (`session_b_180618a.txt`) |
| `d103_rerun_at_180618a.txt` (+ `.stderr`) | the corrected generator, run from a scratch cwd | new title/Subject/verdict; stamp `built at 180618a047ba` (no `+DIRTY`); all 40 table integers identical to `bench/d103_production_diff_curve.txt` |
| `p4_test_run_180618a.out` | `cargo test --test integration_production_diff_wiring` | 4 passed, 0 failed |
| `cargo_doc_180618a.log` | `cargo doc --no-deps --lib` | rc=0; none of its 170 warnings is on a line D193 edited (all pre-existing) |
| `session_b_180618a.txt` | the locked session's own rc lines | tree clean at `180618a` before and after |

What this does NOT show: the full suite was not run by this lane (the lead runs it at landing).
`DIFF <branch>` not reaching `page_changeset_with_cost` was established by reading source, not by
any binary here; the generator says so in its Subject line.
