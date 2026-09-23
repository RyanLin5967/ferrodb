# D187 — pre-registration, written BEFORE the runs it predicts

Branch `d187-null-index-scan`, base `main @ 0e20c72`. Instrument for every number below:
`cargo test --test d187_null_index_scan -- --test-threads=1`, run under the shared suite lock
(`/tmp/ferrodb-suite.lock`, acquired with the same atomic `mkdir` protocol `tools/verify-suite.sh`
uses). Amendments append only.

The test file has **11 tests**. Counted from the `#[test]` attributes in
`tests/d187_null_index_scan.rs` and cross-checked against the `running N tests` line of the two runs
already taken — not from a grep, because `#[test]` occurrences are not test counts.

## Run A — AFTER (fix present, commits 6654409 + 01e417e + 47e04eb + this one)

Predicted: `test result: ok. 11 passed; 0 failed`, **rc=0**.

## Run B — BEFORE on the restructured test (src reverted to 6654409, tests kept)

This run exists because the first BEFORE asserted inline and aborted at the first mismatch, so only
`v < 5` ever got a before-number. Predicted **7 failed / 4 passed, rc=101**.

### Predicted failures (7)

| test | predicted observation |
|---|---|
| `secondary_excluded_upper_drops_null` | 6 rows `[Null, Null, Null, Null, Null, Integer(1)]`, want 1 |
| `secondary_included_upper_drops_null` | 6 rows, same shape, want 1 |
| `primary_excluded_upper_drops_null` | 5 rows `[Null, 1, 2, 3, 4]`, want 4 |
| `primary_included_upper_drops_null` | 6 rows `[Null, 1, 2, 3, 4, 5]`, want 5 |
| `null_bound_literal_is_not_turned_into_bounds` | `Some((0, Included(Null), Included(Null)))`, want `None` |
| `a_null_bound_reaching_either_scan_yields_no_rows` | secondary 5 rows, want 0 |
| `sql_index_path_answers_correctly` | **4 of 8 shapes wrong**, detailed below |

### Predicted passes (4) — the controls, which must pass UNFIXED

`control_secondary_lower_bound_already_excludes_null`,
`control_primary_lower_bound_already_excludes_null`,
`control_primary_excluded_lower_bound_already_excludes_null`,
`fully_unbounded_scan_keeps_nulls`.

If any of these fails first, the fix is aimed at the wrong mechanism and the diagnosis is wrong.

### `sql_index_path_answers_correctly` — the eight shapes, predicted individually

Table: 500 rows with `v` NULL, one `v = 1`, 2500 spread `v = 100 + 3k` (max 7597). 3001 rows.

| predicate | predicted got | want | predicted plan | why |
|---|---|---|---|---|
| `v < 5` | **501** | 1 | index | 500 NULL entries sort first and `Null > 5` is false |
| `w = 1 AND v < 5` | **501** | 1 | index + Filter | `w = 1` holds for every row, so the residual Filter removes nothing |
| `v <= 4` | **501** | 1 | index | same, via the `Included` arm |
| `v = NULL` | **500** | 0 | index | `bound_selectivity`'s `lo == hi` branch gives `1/2501`, selective enough that the index wins; scan then emits exactly the NULL run |
| `v >= NULL` | 0 | 0 | **seq** | `lo` falls back to `min`, `hi` to `max`, so `sel = 1.0` → 3001 rows → index loses on cost |
| `v > NULL` | 0 | 0 | **seq** | `secondary_scan_lower(Excluded(_))` is `None`, so `index_scan_lowerable` rejects the conjunct outright |
| `v <= NULL` | 0 | 0 | **seq** | `hi` falls back to `max` → `sel = 1.0` → index loses |
| `v < NULL` | 0 | 0 | **seq** | same; and even on the index it would stop immediately, since `Null >= Null` is true |

⚠ **Four of the NULL-literal shapes are predicted to PASS before the fix, and that is not the fix
working.** They pass because the cost model happens to choose a sequential scan, whose `Filter`
rejects NULLs correctly. They are the end-to-end guarantee, not the falsifier. The falsifier for the
planner defect is `null_bound_literal_is_not_turned_into_bounds`, which is a pure function and
cannot be rescued by a plan choice. `v >= NULL` returning the whole table is reachable in principle
— it is what the bounds say — but on THIS fixture the cost model does not pick the index for it, so
this run will not demonstrate it end to end and I will not claim it does.

## What would falsify the diagnosis

- Any control failing before the fix.
- `v < 5` answering 1 before the fix (then the 501 came from somewhere else).
- Run A leaving any test red.
- `sql_index_path_answers_correctly` reporting a **seq** plan for `v < 5` in either run — the
  headline assertion fires and the row counts mean nothing, whatever they say.
