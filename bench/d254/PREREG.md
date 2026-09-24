# D254 — pre-registration, written BEFORE the fix (nothing built)

Branch `d254-catalog-persist-bound`, a CHILD of `d249-alter-encodable` @ `06b0188`, D249's final tip. It lives in
`/Users/idide/wt/ferrodb-d254.noindex`. Quiet mode: nothing on this branch has been compiled or run, so every line
below is a prediction. Amendments append only.

Source: the D249 lane's §5 candidate, confirmed and widened by `d249_review.md` @ `13577b1` (F3), and made ledger D254
by the lead, lead-verified at `9aa6968` `catalog.rs:474-525`.

**Instrument:** `timeout 1800 cargo test --no-fail-fast --test d254_catalog_persist_bound`, at default QoS, under the
shared suite lock. Its `-- --list` is the authority for the count.

## The defect (READ at `9aa6968`)

- **`Catalog::persist` never places an entry that cannot fit an empty page.**
  - It sorts the entries by name (`catalog.rs:482-483`) and walks them with a shared `peek` iterator (`:484`).
  - It adds each entry only while `page.has_space(entry)` (`:498`), and `break`s at the first that does not fit.
  - An entry larger than an EMPTY page (`HEADER_SIZE + entry.length() > PAGE_SIZE`) fits no page. The iterator
    never advances past it, and `has_more` stays true (`:505`).
  - When `next_catalog_page == 0`, it calls `new_page()?` (`:508-509`), which allocates and writes a zero page. It
    stamps and links that page, serializes the current page (which holds only the entries before the oversized one),
    and moves on (`:553`). This repeats until `new_page` fails.
- **`allocate` refuses only at an unbounded region** (`disk_manager.rs:399-431`).
  - The CLI and `examples/pgserver.rs` set that region at `DEFAULT_ARENA_HEADROOM` = 32,736 pages.
  - A `DiskManager` with no region never refuses (INFERRED).
- **Ordinary DDL reaches it** (the review's F3, READ-derived):
  - CREATE TABLE with 150 INTEGER columns of 25-byte names (4,200 bytes of columns);
  - CREATE INDEX on a table near the limit, which adds `5 + len(column)` bytes;
  - CREATE FULLTEXT INDEX, the same.
  - Nothing upstream bounds column count times name bytes.
- **What it leaves:**
  - the table region used up;
  - a catalog image holding only the entries sorted before the refused one, so a restart would lose the tables
    after it (INFERRED);
  - the pages that `create_table` (`:160-162`), `create_index` (`:233`) and `create_fulltext_index` allocated
    before persisting, which are never freed. `persist_or_undo` undoes the map, not the pages.
  - The empty chain is reclaimed by the next successful `persist` (the review, READ).

## Red tests at `1c20cbe` (additions only: `tests/d254_catalog_persist_bound.rs`, `279 0`)

**The shared setup.** Every harness calls `reserve_region("test floor", 256, u32::MAX)` before `Catalog::create`, so
the unfixed loop refuses at page 256 instead of filling the disk. Every harness also holds a table `zz`, which sorts
after every refused name.

**Each test asserts three things:**

- **the refusal**: the size refusal, a `Constraint` whose message names the table and "catalog page";
- **no page allocated**: `DiskManager::bitmap_high_water` is unchanged. The fixture frees nothing, so there is no
  hole below the mark for an allocation to hide in;
- **the catalog is intact**: in memory, AND on its pages as `Catalog::open` over the same pool reads them back, with
  `zz` still present and the refused object absent.

Each premise is asserted through `CatalogPage::has_space`.

| key | test |
|---|---|
| **CT** | `a_create_table_whose_entry_exceeds_a_page_is_refused_and_leaves_the_catalog_and_the_pages_alone`: 150 columns of 25 bytes. A later CREATE TABLE must still work |
| **CI** | `a_create_index_that_grows_an_entry_past_a_page_is_refused_and_leaves_the_catalog_and_the_pages_alone`: 15 columns of 255 bytes (3,911 with the header, which fits), then an index on the second (4,171, which does not) |
| **CF** | `a_create_fulltext_index_that_grows_an_entry_past_a_page_is_refused_and_leaves_the_catalog_and_the_pages_alone`: the same shape, with the second column `VARCHAR(20)`, and a full-text index on it |
| **CP** | `persist_refuses_an_entry_no_page_can_hold_before_writing_any_page`: an oversized entry is inserted into `catalog.tables` directly, then `persist()` is called. This is the only test that reaches `persist`'s own check, because the DDL paths refuse before it |

**Run R at `1c20cbe` (predicted): 0 passed, 4 failed.** Each fails at its refusal assertion. At this tree, `persist`
loops until the test floor refuses, so the error is `Io("no free page below …")` and not a `Constraint`.

## The fix, as it will be made

1. **`persist` asks the encoder about every entry before it writes any page.** After sorting, and before the first
   `fetch_page`, it runs `for e in &sorted { refuse_unless_encodable(e)?; }`. This is D249's function, which asks
   `has_space` on an empty page and then `serialize`s a one-entry page.
   - It closes the loop and the truncated image for every caller, including future ones.
   - It also closes D141's partial-image window: an encoder refusal on catalog page k used to leave pages before k
     rewritten.
2. **`create_table` checks the entry before its three allocations.** It builds the `TableEntry` with placeholder
   roots (0), checks it, and only then allocates and fills in the real roots. Roots are fixed-width `u32`, so the
   placeholder cannot change the answer.
3. **`create_index` and `create_fulltext_index` check the entry with the new index record** (root 0), before
   `BPlusTreeManager::create`.
4. **`refuse_unless_encodable`'s doc names its callers.**

**Why four checks and not one.** The pre-pass in `persist` is the correctness fix: no loop, and no truncated image.
The three DDL checks stop the pages those statements allocate before persisting from leaking. Each has its own
killer:

- CP drives `persist` directly, so the DDL checks cannot mask the pre-pass.
- CT, CI and CF each assert no allocation. With only the pre-pass, each would pass its refusal and catalog
  assertions and fail its high-water assertion.

**Cost.** Every `persist` now serializes every entry twice: once in the pre-pass, and once for real. `persist` was
already O(tables) per DDL, so this is a constant factor of 2 on DDL, and nothing grows with rows or branches.

## Run G at the fix (predicted)

- `--test d254_catalog_persist_bound`: its list shows **4**, and all **4 pass**.
- **Per-target:** D249's 2588 + 4 = **2592** on macOS. The 2579 at its base is measured on main's tree.

## Mutants (cut from the fix; predicted)

| mutant | what it does | fails |
|---|---|---|
| **P1 `persist_prepass_removed`** | the `persist` pre-pass deleted | **CP**: the error is `Io`, `zz` is lost from the pages, and the high water rises. CT, CI and CF survive it, because the DDL checks refuse first |
| **P2 `create_table_check_removed`** | `create_table`'s early check deleted | **CT**, at its high-water assertion. The 3 pages are allocated before `persist`'s pre-pass refuses with the same `Constraint` |
| **P3 `create_index_check_removed`** | `create_index`'s early check deleted | **CI**, at high water (the index's root page) |
| **P4 `create_fulltext_check_removed`** | `create_fulltext_index`'s early check deleted | **CF**, at high water |

### D249's mutants on THIS tree (D249's amendment 4, G1, named the tree difference)

With the pre-pass in `persist`, a D249 mutant that sends an oversized entry to `persist` gets a `Constraint`, not the
loop:

- **E1 → AI at its SCHEMA assertion**, not at its error assertion. The error is the size `Constraint` naming `"t"`
  and "catalog page", so that assertion passes; then the in-memory rename is found installed. AR, AA, A9 and A33 are
  unchanged.
- **E4 → AI, the same way.**
- E2, E5 and E7 are unchanged.
- **E3 now panics on more tests.** `refuse_unless_encodable`'s `has_space` arm guards every caller, so removing it
  makes U1, AI, CT, CI, CF and CP all panic in `serialize`.

D254's run script runs E1 and E4 on `d249_alter_encodable` to confirm the moved assertion.
