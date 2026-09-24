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

---

## Amendment 3 — the review's F1, F2 and F4–F7, written BEFORE their code (nothing built)

Source: `artie-research frontier/d249_review.md` @ `13577b1`, verdict SOUND-WITH-CAVEATS: the fix is sound and the
evidence is weak. These are the lead's decisions. The review's F3 is a new row, D254, on the child branch
`d254-catalog-persist-bound` with its own PREREG.

### F1 and F6: amendment 1's premise was wrong, and E2 is re-registered

- **Retracted** (append-only): amendment 1's "a ninth column makes the bitmap two bytes; a row rewritten under
  nine columns is then misread under eight, or refused by `deserialize`". Both halves are false (READ
  `storage/tuple.rs`):
  - **Alignment padding hides the extra bitmap byte.** Every INTEGER, value or NULL, is padded to a 4-byte boundary
    measured from the start of the tuple (`get_padding(4, …)`, `:72`, `:124`, `:180`), after the 24-byte version
    header (`:9`). Eight columns give 24 + 1 = 25, padded to 28. Nine columns give 24 + 2 = 26, also padded to 28.
    Every INTEGER sits at the same offset in both layouts, so the eight-column reader reads the nine-column row
    correctly.
  - **`deserialize` has no refusal.** It checks no length, and an overrun is a slice panic (`:186`, `:217-218`).
- **So E2 survives A9, as well as AR and AA.** Before this amendment, no registered test pinned where the refusal
  happens.
- **A33** = `a_thirty_third_column_the_catalog_cannot_hold_is_refused_before_any_row_is_rewritten`, additions only.
  - The setup: `t` with 32 INTEGER columns (`id NOT NULL`, `c1..c31`), holding `(1..=32)`, then
    `ADD COLUMN <300 bytes> INTEGER`.
  - It must be refused by name, the catalog must keep 32 columns, `SELECT *` must return exactly `(1..=32)`, and a
    later CREATE TABLE must succeed.
  - **The layout arithmetic.** 32 columns give 24 + 4 = 28, with no padding. 33 columns give 24 + 5 = 29, padded to
    32. So every value moves.
  - **The premise, asserted without becoming the expected value:** `Tuple::serialize((1..=32) + NULL, 33-column
    schema)` followed by `.deserialize(32-column schema)` must NOT equal `(1..=32)`. If a future layout change
    makes the fixture non-discriminating, this fails loudly.
- **A9 stays**, because it still pins the wedge. Its doc comment repeats the false premise and is corrected
  (comment only; no assertion moves).

### F2: the index-rename half of the pre-check is unpinned. E4, and test AI

- **E4 `index_renames_not_checked`** deletes `rename_indexed_columns(&mut installed, actions);` from `plan_alters`.
  It survives AR, AA, A9, A33, U1 and U2.
  - Wherever a renamed index name exceeds 255 bytes, the same name is already in the schema. The column-name guard
    (4/8) fires before the index-name guards (6/8, 8/8).
  - So index renames change the answer only through the size arm.
- **AI** = `a_rename_that_grows_an_indexed_column_past_a_page_is_refused_by_size`, additions only. The numbers are
  READ lengths and INFERRED sums, per the review:
  - **Setup:** `t` with `id`, `v`, and 14 filler INTEGER columns with distinct 255-byte names; `CREATE INDEX` on
    `v`; then `RENAME COLUMN v TO <255 bytes>`.
  - **Before the rename** the entry is 23 + 3645 = 3668 bytes. **After it** the entry is 3668 + 254 (schema) + 254
    (index) = 4176, which is over 4096, so the size arm refuses it as a `Constraint` naming `"t"` and "catalog page".
  - **The premises**, asserted through `CatalogPage::has_space` on an empty page:
    - the fully renamed entry does NOT fit;
    - the entry with only the schema renamed DOES fit (3922). That second premise is what makes the test see E4.
  - **Then:** the table keeps `v`, its index still covers `v`, and a later CREATE TABLE succeeds.
  - **Predicted kills:**
    - E4: the check passes at 3922. The rename-only chain skips the rewrite, `finish` installs, and `persist`
      cannot place the 4176-byte entry. AI fails at its schema assertion.
    - E1: the same failure.
    - E3: `serialize` panics on the oversized entry.
- **The hazard, bounded.** Under E1 or E4, AI reaches `persist`'s unbounded page loop (D254). So AI's harness
  calls `dm.reserve_region("test floor", 256, u32::MAX)` before `Catalog::create`, which makes allocation refuse
  past page 255 instead of filling the disk. AR, AA, A9 and A33 get the same bound, because E1 sends them into the
  same `persist`.
  - **Correction.** Under E1 those four do not loop: a 300-byte name refuses in the length guard, which `persist`
    reaches only after placing the entry. Only an oversized entry loops. They are bounded anyway, as a guard.

### F4: two in-tree comments the fix made false

`plan_alters`' "Every refusal lives in `resulting_schema` … an agent is told at the moment it types the statement
exactly what it would be told at merge" (`alter.rs:556-559`), and `stage_schema_edit`'s "Validated now … with the
same rules `Catalog::alter_table` applies" (`agent_sql/runtime.rs:2545-2547`), are both amended to name the
exception:

- the encoder's check is made at `plan_alters`, which a branch reaches only at MERGE;
- so a staged rename or column that the catalog cannot encode is refused at merge, before anything is written, not
  when typed.

The check is NOT moved into staging (the lead's decision). Comments only.

### F5: the size arm's error variant (a record of a decision already in the code)

The fix plan registered U1 as `Err(Unrepresentable { limit: PAGE_SIZE, .. })`. The code (`24c0ead`) and U1 use
`FerroError::Constraint`. That switch was made without an amendment, and this records it and its reasons:

- `Unrepresentable` is documented as arithmetic on a length field (`error.rs:73-86`), and it explicitly says a
  page-size limit is not its case.
- `NotEnoughSpace` is the page-size variant, but it carries no text, so it cannot name the table the operator has
  to change.
- `Constraint` carries a message and is what this module already uses for a refusal the user can act on.

### F7: a plan held across a change to its own table

- **The latent state.** `plan_alters` checks a clone of the entry. If a caller held the plan while the same table's
  entry changed, for example by a CREATE INDEX, `apply_plan` would install renames on an entry nobody checked. No
  caller does that today, but `apply_plan`'s own comment says "a plan can be held across a statement".
- **The fix.** `AlterPlan` stores the entry it checked. `apply_plan`, before anything it writes (before
  `reserve_free_space`), rebuilds that entry from the LIVE one, with the plan's final shape and renames applied. It
  refuses with a `Constraint` if the result differs from what was checked. This is a staleness check, not a second
  encoder guard.
- **ST** = `a_plan_held_across_a_change_to_its_own_table_is_refused`, additions only, red first.
  - The steps, through the public `Catalog` API: `plan_alters(RENAME COLUMN v TO w)`, then `create_index(t, v)`,
    then `apply_plan(plan)`.
  - It must be refused, and the table must keep `v`, with its index on `v`.
  - **Red at the tip before F7:** `apply_plan` installs the rename, so the refusal assertion fails.
- **E5 `staleness_check_removed`** deletes the comparison. It fails ST.

### Counts and runs, amended (predicted)

- **New tests:** A33, AI and ST are integration tests in `d249_alter_encodable`, so that target has **6** tests.
  - ST is red against the tip before F7's code; A33 and AI pass there.
  - The target's red run R at `d42549f` is unchanged: 3 tests, all red.
- **Per-target: 2579 + 8 = 2587** (AR, AA, A9, A33, AI, ST, U1, U2). The 2579 was measured on main's tree.

| mutant | fails |
|---|---|
| E1 | AR, AA, A9, A33, AI |
| E2 | **A33 only**, at its row assertion, whose first value reads `16777216` (the review's MODEL) |
| E3 | U1, and AI (a panic in `serialize`) |
| E4 | AI |
| E5 | ST |

---

## Amendment 4 — review 2's G1–G5, written BEFORE their code (nothing built)

Source: `artie-research frontier/d249_review2.md` @ `4c2bf91`, a review of `98432bc..27dd0a1`, verdict
SOUND-WITH-CAVEATS. These are the lead's decisions. An earlier amendment 4, recording `0369d88..27dd0a1`, was
refused by the handoff gate before it was written. This is the first amendment 4 on record, and it covers both.

### What `0369d88..27dd0a1` did (the record the refused amendment would have made)

| commit | what |
|---|---|
| `0369d88` | tests A33, AI and ST (additions); A9's doc comment corrected; `Db::new` bounds the allocator (`reserve_region("test floor", 256, u32::MAX)`) |
| `fa2e9a3` | F7: `AlterPlan.checked` and `apply_plan`'s staleness check. F4: two comments name the exception |
| `27dd0a1` | E4 and E5; E1–E5 cut from `fa2e9a3` |

### G2 and G3: the staleness check compares the entry AS READ, and does not overwrite the live schema

- **What is wrong at `fa2e9a3`.** `checked` is the entry after the plan's shape and renames. The check rebuilt the
  live entry by overwriting its schema with the plan's final shape, then compared.
  - **It is blind to a schema change** (G3). Another ALTER between plan and apply would vanish from the catalog,
    and rows converted from the pre-change heap would be installed.
  - **It turned E4 into a test of the staleness check** (G2). E4 removes the renames from `installed`, which was
    also `checked`, so E4 died by comparison and never reached the encoder.
- **The fix.** `AlterPlan` stores `read: TableEntry`, the entry exactly as `plan_alters` read it, and `checked` is
  removed. `apply_plan` refuses, before anything it writes, unless the LIVE entry equals `read`, with nothing
  overwritten.
  - `checked` was a deterministic function of `read`, the plan's shapes and its actions, so "live == read" implies
    what the old comparison implied.
  - It also covers the schema.
- **Its blind spot, stated in the code.** A committed INSERT, UPDATE or DELETE that moves no root changes the heap
  and not the entry, so the check does not see it. The plan's `prepared` rows would then not describe the heap.
  That stays with the caller's discipline, as the merge's comment already says ("a plan's decision is only valid for
  the heap it read", `agent_sql/runtime.rs:5003-5006`). The heading and message are narrowed to "the table's
  catalog entry has changed since".
- **ST is strengthened.** It now asserts the refusal is a `Constraint` whose message contains "changed since", not
  any `Err`, so an unrelated refusal (a future quiesce change, say) cannot pass it. This is the review's note, and
  it applies to this lane's own unrun test.
- **E6 is not registered.** With `live == read`, E4 no longer passes through the staleness check (the live entry and
  `read` are both untouched by the plan). So E4 measures what F2 asked for: whether the encoder is asked about the
  entry with the renamed index.

### G4: `apply_plan`'s doc

"The failures it can still meet are environmental" is false. `apply_plan` has two non-environmental refusals, and
the doc will name both, and why neither fires inside `AgentRuntime::merge`:

- the quiesce re-check: a merge holds no transaction open;
- the staleness check: a merge applies each table's plan with no statement in between that touches that table's
  entry. `apply_plan(Y)` changes only Y's entry (review 2, §4).

### G5: A9's name and message carry the retracted reason

- **Renamed.** `an_added_ninth_column_the_catalog_cannot_hold_is_refused_before_any_row_is_rewritten` becomes
  **`an_added_ninth_column_the_catalog_cannot_hold_is_refused_and_wedges_nothing`**. It pins the wedge, and not the
  placement.
- **Its row-assertion message** loses "it was rewritten under nine, whose null bitmap is a byte wider (I19)".
- **No assertion moves.** The key stays A9.

### G1: the mutant table, re-registered against the tree it runs on

**The tree.** The D249 mutant run applies each patch to **the D249 branch tip**, the commit amendment 5 will name.
That tree does NOT contain D254's fix. On D254's child branch, `persist` refuses an oversized entry before placing
anything, which moves E1's and E4's AI failure from the error assertion to the schema assertion. D254's own PREREG
registers the mutants for that tree.

| mutant | fails (on the D249 tip, no D254) |
|---|---|
| E1 `precheck_removed` | AR, AA, A9 and A33 (a wedge: the refused name is installed, then `persist` refuses on its length); **AI at its error assertion**. The staleness check passes, `finish` installs, and `persist` loops until the test floor refuses with `Io("no free page below … 256 …")`, which is not a `Constraint` |
| E2 `precheck_in_finish` | **A33** at its row assertion (the review's MODEL: `16777216` first), **and AI** at its "holds the renamed column or index" assertion. `apply_plan` renames the live index to `w…` before `finish`, then E2's check refuses with the correct size error, which leaves an index naming a column that does not exist |
| E3 `oversize_arm_removed` | U1 (run on `--lib catalog::catalog_page::tests::`). AI would also panic in `serialize`, but that command does not run AI |
| E4 `index_renames_not_checked` | **AI only, at its error assertion.** The pre-check sees 3922 and passes. The staleness check passes too, because `live == read`. `finish` installs, and `persist` loops until the floor's `Io` refusal |
| E5 `staleness_check_removed` | ST |

**Under the check at `fa2e9a3`, E4 also failed three existing tests**, each by the staleness refusal on a rename of an
indexed column (review 2, G1):

- `integration_alter_column::renaming_an_indexed_column_leaves_the_table_queryable`;
- `integration_merge_ddl_atomicity::an_index_follows_its_column_across_the_renames_a_merge_applies`;
- `integration_merge_ddl_atomicity::a_rename_follows_its_column_into_the_fulltext_index_too`.

**Under the `live == read` check, E4 no longer fails them** (INFERRED). Their entries are far below a page, so
leaving the index renames out of the checked entry changes nothing the encoder answers. The staleness check no
longer involves the renames either. These three are named so that a collateral run under E4, if one is ever made,
has a registered prediction: **they pass**.

### Counts (unchanged by this amendment)

- `d249_alter_encodable` has **6** tests, and the lib gains U1 and U2. **Per-target: 2579 (measured) + 8 = 2587.**
- **R2 at `0369d88`** (predicted): `d249_alter_encodable` **5 passed, 1 failed (ST)**. A33 and AI pass there,
  because the pre-check is present.

---

## Amendment 5 — amendment 4's changes recorded; the D249 mutant tree named (nothing built)

| commit | what |
|---|---|
| `547de64` | amendment 4 |
| `c5e57b8` | `AlterPlan.read`, the entry as read, replaces `checked`. `apply_plan` refuses unless `live == read`, with nothing overwritten, and its blind spot is stated. The doc names both refusals (G4) |
| `e6af639` | A9 renamed to `an_added_ninth_column_the_catalog_cannot_hold_is_refused_and_wedges_nothing`, and its row message cut (G5). ST asserts `Constraint` containing "changed since" |
| `bed42db` | E5 re-anchored on `if self.require_table(&table)? != &read`; E1–E5 cut from `e6af639`, each passing `git apply --check`. E1, E2 and E4 have the same edits as before |

- **Code at `c5e57b8`** (comment lines filtered from `git diff 27dd0a1 c5e57b8 -- src/catalog/alter.rs`):
  - the field and its initialiser: `read: entry.clone()`;
  - the destructured name;
  - the old 13-line rebuild-and-compare block, replaced by a 6-line `if … != &read { return Err(Constraint) }`.
- **Test lines changed at `e6af639`:** A9's fn name and one message line; ST's `assert!(applied.is_err(), …)`,
  replaced by a `match` that requires the staleness `Constraint`. The ST change strengthens an assertion and does
  not weaken one.
- **Counts at `bed42db`:** `d249_alter_encodable` has 6 tests, and `git diff 9aa6968 bed42db` adds 8 `#[test]` and
  removes 0 (READ). **Per-target: 2587.**
- **The D249 mutant tree is `bed42db`** (this commit's parent carries no code change after it; amendment 4's G1
  table applies to it). It does not contain D254's fix.
