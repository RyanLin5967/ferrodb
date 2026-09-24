# D235: a rebuild keeps D16 pins. Pre-registration and mutant register

Written before any run; amendments go below, append-only. Branch `d235-rebuild-keeps-d16-pins`,
worktree `/Users/idide/wt/ferrodb-d235.noindex`, base `9aa6968`. Quiet mode: nothing here has been
compiled or run.

| commit | what |
|---|---|
| `0ff7016` | failing-first tests, compiled against base API only |
| `6589552` | the fix: `record::parent_entry_holders`, used by `LogBranchCatalog::index` and `TableBranchCatalog::migrate_from`; stale comments corrected in place; unit tests for the rule |

## RED at `0ff7016` (tests from that commit, `src/` = base)

| command | expected |
|---|---|
| `cargo test --test d235_rebuild_keeps_d16_pins` | **2 run: 1 passed, 1 failed.** The table arm is the CONTROL and PASSES. The log arm FAILS at `after the restart: GP (pinned through the reaped interior B) reads as childless`, after every before-restart premise and every after-restart premise has passed (B, P1, P2 read Reaped; GP, C, GP2, C2 read Live). A failure anywhere else, or a failing control, means the fixture is wrong: stop. |
| `cargo test --lib branch::table_catalog::tests::a_migrated_catalog_answers_every_query_the_way_its_source_does` | **1 failed**, at `max_live_child for 9`. Id 9 is the pinned grandparent under the fixture's fork order, INFERRED by counting forks. The fixture's own premise (`fixture: … does not pin in the SOURCE`) must PASS. |

## GREEN at the tip

| command | expected |
|---|---|
| `cargo test --test d235_rebuild_keeps_d16_pins` | **2 passed** |
| `cargo test --lib branch::table_catalog::tests::a_migrated_catalog_answers_every_query_the_way_its_source_does` | **1 passed** |
| `cargo test --lib branch::record::d235_parent_entry_holders` | **5 passed** |
| collateral: `cargo test --lib branch::catalog`, `--lib branch::table_catalog`, `--lib branch::record`, `--lib branch::reaper`, then `--test d16_transitive_visibility --test d18_reaper_asks_the_catalog --test d60_depth_is_not_capped --test integration_cluster_snapshot` (the last one reaches `reload_from` through `PageStoreSnapshots`). Take counts from `-- --list` first. | 0 failed |
| per-target suite | **2579 + 7 = 2586 passed, 0 failed.** 2579 is main's per-target count at `9aa6968`, READ from FAN-QUEUE rows 12–15. The +7 are 2 tests in `tests/d235_rebuild_keeps_d16_pins.rs` and 5 in `record.rs::d235_parent_entry_holders`. The migration test was extended, not added. |

## Mutants: each restores the skip at one site

Apply each ALONE, in a throwaway copy of the tip. Each anchor occurs once (`git grep -nF` at `6589552`).

| # | file | anchor → replacement | what it restores | expected |
|---|---|---|---|---|
| M1 | `src/branch/record.rs` | `        let mut up = parent;` → `        let mut up: Option<u64> = None;` | the shared rule without its walk: only non-`Reaped` records hold an entry, i.e. the `9aa6968` rule at BOTH sites | d235 log arm RED at the same line as the RED phase (table arm green); migration test RED; `d235_parent_entry_holders`: **4 red, 1 green** (`a_fully_reaped_subtree_holds_nothing_even_under_a_live_parent` stays green) |
| M2 | `src/branch/catalog.rs` | `            if !holders.contains(id) {` → `            if r.state == BranchState::Reaped {` | the skip in `LogBranchCatalog::index` only | d235 log arm RED (GP's array comes back as its last stored snapshot, taken before B forked, i.e. empty); table arm, migration test and unit tests green |
| M3 | `src/branch/table_catalog.rs` | `            if holders.contains(&child) {` → `            if nodes.get(&child).map(\|n\| n.1) != Some(BranchState::Reaped) {` | the skip in `migrate_from` only | migration test RED; d235 tests and unit tests green |

**What the mutants cannot tell you.** None of them distinguishes a rule that attaches EVERY record
with a parent from the correct one: stale entries for reaped leaves resolve as "not live" in the
table catalog. The log catalog's replay tests
(`live_children_is_derived_at_replay_and_excludes_reaped_children`,
`a_parent_listing_only_reaped_children_comes_back_with_an_empty_array`) are what fail such an
over-attaching rule on the log side. The migration test's `next id minted` check and the per-id
`has_live_children` comparisons are what fail it on the table side.

---

## Amendment 1 — 2026-09-24, after a fresh-context review of `8d2d0f0`; before any run

Still nothing compiled or run. The review found no compile error and no defect in the `src/`
change, and every RED, GREEN and mutant cell above traced to its registration. What it found, and
what `4f4373f` changed:

- **The "What the mutants cannot tell you" paragraph above was WRONG about the table side.** At
  `8d2d0f0` the migration test could NOT fail an over-attaching `migrate_from`:
  - The one reaped record the correct rule leaves unattached is `spare`. Its stale entry resolves as
    "not live" while its slot is free.
  - No window probed its epoch, and the next id minted still matched.
  - Fixed: the test re-asks every window after the final fork recycles `spare`'s slot. A stale entry
    then names a live branch.
- **Nothing told the rule apart from "only `Live` holds an entry"** (review F2). That rule drops
  `Quarantined` and `Reaping` children, which is the data-loss direction. Fixed in two places:
  - the migration test probes every record's own fork epoch, so `held` is now probed;
  - a new unit test covers `Quarantined` and `Reaping` children under reaped parents.
- **The walk ignored generations** (review F3, leak-direction only). It now stops at a slot whose
  current `branch_id` differs from the parent handle the child recorded. A new unit test covers it.
- **M3's replacement text in the table above is invalid Rust as pasted**, because the `\|` escapes
  are Markdown. The node tuple also grew a field. Use the exact texts below.

### Exact mutant texts at `4f4373f` (each anchor occurs once; apply each ALONE)

```
M1  src/branch/record.rs
    -        let mut up = parent;
    +        let mut up: Option<BranchId> = None;
M2  src/branch/catalog.rs
    -            if !holders.contains(id) {
    +            if r.state == BranchState::Reaped {
M3  src/branch/table_catalog.rs
    -            if holders.contains(&child) {
    +            if nodes.get(&child).map(|n| n.2) != Some(BranchState::Reaped) {
M4  src/branch/record.rs            (new: "only Live holds an entry")
    -        if state == BranchState::Reaped {
    +        if state != BranchState::Live {
```

### Expected at `4f4373f`

| run | expected |
|---|---|
| `--test d235_rebuild_keeps_d16_pins` | 2 passed (unchanged) |
| `--lib branch::table_catalog::tests::a_migrated_catalog_answers_every_query_the_way_its_source_does` | 1 passed |
| `--lib branch::record::d235_parent_entry_holders` | **7 passed** (5 + the Quarantined/Reaping case + the recycled-slot case) |
| per-target suite | **2579 + 9 = 2588 passed, 0 failed** (2 in `tests/d235_…` + 7 unit) |
| M1 | d235 log arm RED (table arm green); migration test RED; unit **6 red, 1 green** (`a_fully_reaped_subtree_holds_nothing_even_under_a_live_parent`) |
| M2 | d235 log arm RED only |
| M3 | migration test RED only |
| M4 | unit `a_quarantined_or_reaping_child_holds_its_entry_and_pins_a_reaped_parent` RED; migration test RED at a window on `held`'s epoch; d235 tests green |

The RED phase at `0ff7016` is unchanged: that commit's tests are what run in it.
