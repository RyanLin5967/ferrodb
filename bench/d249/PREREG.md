# D249 — pre-registration, written BEFORE the fix (nothing built)

Branch `d249-alter-encodable`, cut from `9aa6968` (main) in `/Users/idide/wt/ferrodb-d249.noindex`. Quiet mode:
nothing on this branch has been compiled or run, so every line below is a prediction. Amendments append only.
Source: `artie-research frontier/d230_review3.md` F3(c), routed by the lead as ledger D249.

**Instrument:** `timeout 1800 cargo test --no-fail-fast --test d249_alter_encodable`, at default QoS, under the
shared suite lock. The unit tests below run with `timeout 1800 cargo test --no-fail-fast --lib
catalog::catalog_page::tests::`.

## The defect (READ at `9aa6968`)

- **The refusal is late.** `resulting_schema`'s `RenameColumn` and `AddColumn` arms (`catalog/alter.rs:189-225`)
  check a new column name for existence and duplicates only.
- **Nothing undoes it.** `Catalog::finish` does `entry.schema = new_schema;` (`:727`) and then `self.persist()?;`
  (`:730`).
- **The encoder does refuse.** It rejects a column name over its `u8` length prefix, through `fits_u8`, guard 4/8
  (`catalog/catalog_page.rs:99-104`, `:165-168`).
- **Nothing upstream bounds identifier length.** D141 says so (`catalog.rs:452-455`), and D141's own test drives a
  300-byte name from SQL.
- **So a refusal leaves the long name behind.** `ALTER TABLE t RENAME COLUMN v TO <300 bytes>`, or `ADD COLUMN` of
  such a name, is refused at `finish`, but the long name stays in the in-memory map, and every later `persist` in the
  process refuses too. That is the D141 wedge, for ALTER. D141 fixed CREATE TABLE with `persist_or_undo`.
- **An undo is the wrong fix here.** `finish` runs after the heap rewrite. An undo would leave rows in the new
  shape under the old schema, which is exactly the state I19 eliminated. `finish`'s own doc says: "Anything that can
  refuse belongs before the rewrite."

## Red tests at `baa1d3c` (additions only: `tests/d249_alter_encodable.rs`, `175 0`)

Both use a table `t (id INTEGER NOT NULL, v INTEGER)` holding the row `(1, 7)`, and a 300-byte name. The premise is
asserted: `name.len() > u8::MAX`. Each test checks three things:

1. The statement is refused with the encoder's `Unrepresentable { len: 300, limit: 255 }`, and the message names
   "the name of column" and `in table "t"`.
2. The table keeps its old columns `[id, v]`, and `SELECT * FROM t` returns exactly `[[1, 7]]`.
3. A later `CREATE TABLE`, which is a `persist`, succeeds.

| key | test |
|---|---|
| **AR** | `a_rename_to_a_column_name_the_catalog_cannot_hold_is_refused_and_wedges_nothing` |
| **AA** | `an_added_column_whose_name_the_catalog_cannot_hold_is_refused_and_wedges_nothing` |

**Run R at `baa1d3c` (predicted): 0 passed, 2 failed.**

- Both refusal checks PASS at the base, because the base's refusal comes from the same encoder, at `finish`.
- Both tests then FAIL at `assert_unchanged`'s column assertion, "…the catalog in memory holds the new shape…":
  - AR's left side is `["id", <300 bytes>]`;
  - AA's left side is `["id", "v", <300 bytes>]`.

## The fix, as it will be made

- **One function, and it is the encoder.** `catalog_page::refuse_unless_encodable(entry: &TableEntry)` refuses an
  entry that cannot be written, and asks the encoder's own two questions:
  1. **Does the entry fit a page at all?** `CatalogPage::new(0).has_space(entry)` is what `persist` uses to place
     entries. An entry larger than an empty page refuses as `Unrepresentable`, naming the table, with
     `len = HEADER_SIZE + entry.length()` and `limit = PAGE_SIZE`. This arm is also what keeps the second step safe:
     `serialize` has no bounds check, so an oversized entry would panic there.
  2. **Does it encode?** A one-entry `CatalogPage` is serialized through `CatalogPage::serialize` itself.
  - **There is no second length check.** `catalog_page.rs`'s header says why: a second guard upstream masks every
    mutant of the encoder's own guards, and `serialize` is "the single authority". Calling `serialize` keeps it the
    single authority.
- **It runs in `Catalog::plan_alters`**, the half that "decides a chain of alterations and writes nothing at all".
  - It runs after the final shape is known and before `prepare_rewrite`, on the entry exactly as `apply_plan` and
    `finish` will install it: the old entry, with `schema = <final shape>`, and every index and full-text index column
    renamed as the plan renames it.
  - The rename is extracted from `apply_plan` into one function, `rename_indexed_columns`, which both callers use, so
    the checked entry and the installed one cannot drift.
  - `plan_alters` is also what `AgentRuntime::merge` calls (`agent_sql/runtime.rs:5030`), so a merge that carries such
    an edit is refused before anything is written as well.
- **`finish` is unchanged.** Its `persist` stays the commit point, and after the check it can no longer fail for this
  reason.
- **Not in `resulting_schema`.** That function sees a `Schema`, not the `TableEntry` the encoder writes: not the
  table's own name, and not its indexes. The branch path (`stage_schema_edit`) therefore refuses such an edit at
  MERGE, in `plan_alters`, before anything is written. That is safe but late feedback, and it is stated rather than
  fixed.

## Post-fix unit tests (`catalog_page.rs`'s test module, additions only; mutant-only red, since they call the new function)

| key | test | pins |
|---|---|---|
| **U1** | `an_entry_larger_than_an_empty_page_is_refused_rather_than_serialized` | 16 columns with 255-byte names is more than one page. The result must be `Err(Unrepresentable { limit: PAGE_SIZE, .. })`, and must not panic |
| **U2** | `the_longest_expressible_column_name_is_encodable` | a 255-byte column name is accepted: the boundary control, so the check cannot be "refuse everything" |

## Run G at the fix (predicted)

- `--test d249_alter_encodable`: **2 passed**.
- `--lib catalog::catalog_page::tests::`: every existing test plus U1 and U2 passes.
- **Per-target: 2579 + 4 = 2583.** The 2579 is MEASURED on main's tree (`verify-d187-9e76a4c/SUMMARY.txt`,
  `passed=2579` per-target; `9e76a4c`'s tree is `9aa6968`'s). No test here is platform-gated.

## Mutants (cut from the fix; predicted)

| mutant | what it does | fails |
|---|---|---|
| **E1 `precheck_removed`** | the `plan_alters` call deleted | AR, AA (as at the base) |
| **E2 `precheck_in_finish`** | the check moved into `finish`, i.e. after the heap rewrite (the wrong fix) | AA only, at the row assertion: the rewritten `(1, 7, NULL)` no longer reads back as `(1, 7)` under `[id, v]`. AR survives it, because a rename rewrites nothing that changes the row's layout. That is why AA reads the row |
| **E3 `oversize_arm_removed`** | the `has_space` arm deleted | U1 (a panic in `serialize`) |

## Also found, and routed rather than fixed (INFERRED from READ at `9aa6968`)

- **The oversize case exists on CREATE TABLE too.** A `TableEntry` larger than an empty catalog page (for example 16
  columns with 255-byte names: 16 × 258 > 4096 − 23) can never be placed.
  - `persist`'s loop (`catalog.rs:474-556`) then breaks without adding it, links a NEW catalog page, and repeats.
  - It allocates one page per turn until `new_page` refuses: about 32,736 pages at the CLI's
    `DEFAULT_ARENA_HEADROOM` (`cli/cli.rs:35`).
  - Only then does it return an error. For CREATE TABLE, `persist_or_undo` undoes the entry, but the chain of empty
    catalog pages stays.
- ALTER is covered by U1's arm above. CREATE TABLE is D141's path, and needs its own row.

---

## Amendment 1 — a third red test, and E2's prediction corrected, before the fix (nothing built)

**The error.** The PREREG above predicts that AA kills E2 (the check moved into `finish`, after the rewrite), at
AA's row assertion. That is wrong (READ `storage/tuple.rs`):

- A NULL still occupies its type's width (`:110-118`).
- The null bitmap is `(ncols + 7) / 8` bytes (`:26`, `:147`).
- So AA's row, rewritten from `(1, 7)` to `(1, 7, NULL)`, is the same one-byte bitmap followed by `id`, `v` and four
  zero bytes, and reads back as `(1, 7)` under `[id, v]`.

**AA therefore survives E2 (INFERRED), and so does AR.**

**The new test, `d42549f` (additions only, `47 0`).** **A9** =
`an_added_ninth_column_the_catalog_cannot_hold_is_refused_before_any_row_is_rewritten`.

- An 8-column table with the row `(1..=8)`, then `ADD COLUMN <300 bytes> INTEGER`.
- It must be refused by name, the catalog must keep 8 columns, `SELECT *` must return exactly `(1..=8)`, and a later
  CREATE TABLE must succeed.
- A ninth column makes the bitmap two bytes. A row rewritten under nine columns is then misread under eight, or
  refused by `deserialize`.
- **So A9 is the test that pins WHERE the refusal happens, not only that it does.**

**Run R, amended (predicted): at `d42549f`, 0 passed, 3 failed.**

- AR and AA fail as registered.
- A9 fails at its column-count assertion: the base's in-memory schema holds 9 columns.

**Mutant table, amended:**

| mutant | fails |
|---|---|
| E1 `precheck_removed` | AR, AA, A9 |
| E2 `precheck_in_finish` | **A9 only**, at its row assertion (or its `SELECT` erroring) |
| E3 `oversize_arm_removed` | U1 |

**Per-target, amended: 2579 + 5 = 2584** (AR, AA, A9, U1, U2).

---

## Amendment 2 — the fix, tests and mutants recorded, before any run (nothing built)

| commit | what |
|---|---|
| `baa1d3c` | red tests AR, AA (additions only, `175 0`) |
| `ab53915` | this PREREG |
| `d42549f` | red test A9 (additions only, `47 0`) |
| `7d72862` | amendment 1 |
| `24c0ead` | **the fix**: `catalog_page::refuse_unless_encodable`, called from `Catalog::plan_alters` on the entry `finish` will install; `rename_indexed_columns` extracted from `apply_plan` |
| `76d2c36` | unit tests U1, U2 (additions only) |
| `44f67db` | mutants E1–E3, cut from `76d2c36` by `bench/d249/make_mutants.py`, each passing `git apply --check` |

- **Removed code lines, in `git diff 9aa6968 44f67db -- src/ tests/`:** only the rename loop that `apply_plan` held
  inline. It now lives in `rename_indexed_columns`, with the same two lists in the same chain order, called by
  `apply_plan` under the same "any rename?" condition, so `apply_plan` still fetches the entry only when there is a
  rename. No test line is removed.
- **Counts:** `git diff 9aa6968 44f67db | grep -cE '^\+[[:space:]]*#\[test\]'` = **5**, and 0 are removed. Those
  are AR, AA and A9 in the new target `d249_alter_encodable`, and U1 and U2 in the lib. No test is `cfg`-gated.
  **Per-target: 2579 + 5 = 2584** (2579 measured on main's tree).
- **Run G (predicted):** `--test d249_alter_encodable` 3 passed. `--lib catalog::catalog_page::tests::`: every test
  passes, U1 and U2 included.
- **Mutant commands:**
  - E1 and E2 on `--test d249_alter_encodable`: E1 fails AR, AA and A9; E2 fails A9 only.
  - E3 on `--lib catalog::catalog_page::tests::`: it fails U1, through a panic in `serialize`.
- **Collateral (every test must pass):** `--lib catalog::`; then `--test d141_long_identifier`,
  `--test integration_alter_column`, `--test integration_alter_refusal_paths`,
  `--test integration_alter_refusal_safety`, `--test integration_merge_ddl_atomicity`, `--test adversarial_i20` and
  `--test adv_f4_false_refusal`.
  - These are the targets that drive ALTER, and the merge path through `plan_alters`.
  - The pre-check refuses only an entry `persist` would itself have refused, so none of them should move.
