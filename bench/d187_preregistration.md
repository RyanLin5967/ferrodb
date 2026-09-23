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

---

## Amendment 1 — the defect reaches WRITES, added before the runs

Found while reviewing my own diff, from `plan.rs::build_scan`, which ends in
`lower(optimize(pushdown(logical), catalog)?, ..)`. D178 deliberately routed `UPDATE`/`DELETE`
through the same planning path as `SELECT` so there would be exactly one. The consequence for D187
is that **both scans are reached by DML**, so the predicate that returns 501 rows to a `SELECT`
hands 501 rows to a `DELETE`.

New test `sql_delete_does_not_destroy_null_rows`. **Test count is now 12, not 11.**

Predicted, on the same 3001-row `seeded_wide()` fixture:

| run | `affected` | survivors | verdict |
|---|---|---|---|
| Run B (before fix) | **501** | **2500** | FAIL — 500 NULL rows destroyed by a predicate they do not satisfy |
| Run A (after fix) | 1 | 3000 | pass |

Revised totals: **Run A = 12 passed / 0 failed, rc=0. Run B = 8 failed / 4 passed, rc=101.**

⚠ The prediction that this test FAILS before the fix rests on `build_index_scan` making the same
choice for the DELETE as for the SELECT — same predicate, same table, same statistics. If DML
instead takes a sequential scan here, its `Filter` answers correctly, the test passes before the fix
and is **vacuous**. The `explain_plan` assertion inside it does not settle that, because it explains
the SELECT rather than the DELETE; it is there to catch the fixture drifting off the index path
entirely. So: if Run B shows this test passing, the correct conclusion is "DML did not take the
index on this fixture", NOT "DML is unaffected" — and the claim that this is a data-loss bug is then
UNPROVEN and must not be made on this evidence.

This is the severity escalation of the whole row — wrong answer to wrong write — so it is the claim
most worth being wrong about, and it is pre-registered with its own falsifier for that reason.

## Amendment 2 — `UPDATE` covered separately, added before the runs

`Update::execute` and `Delete::execute` are different code consuming the same scan, so proving one
says nothing about the other. New test `sql_update_does_not_overwrite_null_rows`. **Test count is
now 13.**

Assigns to `w` because `Update` refuses to assign to column 0. The `w = 9` recount is an INDEPENDENT
instrument: `w` carries no index, so that query is a sequential scan whose `Filter` is known-correct
for NULLs, and it catches a bug that writes 501 rows while reporting 1.

| run | `affected` | rows carrying `w = 9` | verdict |
|---|---|---|---|
| Run B (before fix) | **501** | **501** | FAIL |
| Run A (after fix) | 1 | 1 | pass |

Revised totals: **Run A = 13 passed / 0 failed, rc=0. Run B = 9 failed / 4 passed, rc=101.**

Same falsifier as Amendment 1: if DML does not take the index on this fixture, this test passes
before the fix, is vacuous, and the write-corruption claim is UNPROVEN.
