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

---

## Amendment 1 — the fix and mutants recorded, before any run (nothing built)

| commit | what |
|---|---|
| `1c20cbe` | red tests CT, CI, CF, CP (additions only, `279 0`) |
| `d8bf380` | this PREREG |
| `64d6d90` | **the fix**: `persist`'s pre-pass; the checks in `create_table` (before its three allocations), `create_index` and `create_fulltext_index` (before the tree); `refuse_unless_encodable`'s doc |
| `1654b9b` | mutants P1–P4, cut from `64d6d90` by `bench/d254/make_mutants.py`, each passing `git apply --check`. D249's E1 and E4 (`bench/d249/mutants/`) also apply to this tree, since `alter.rs` is unchanged here |

- **Code removed at `64d6d90`** (`git diff 06b0188 64d6d90 -- src/`): one import line, widened to add
  `refuse_unless_encodable`, and `create_table`'s three allocations and entry literal.
  - They are re-ordered, not deleted: the entry is built first with placeholder roots and checked, then the pages
    are allocated and the roots filled in. The fields and values are unchanged.
  - `catalog_page.rs` changes only a doc comment.
- **Counts:**
  - `d254_catalog_persist_bound` has **4** `#[test]`, with no `cfg`, `ignore` or macro;
  - `git diff 06b0188 1654b9b` adds 4, and from `9aa6968` it adds 13, with 0 removed.
- **Per-target: 2579 + 13 = 2592** (2579 measured on main's tree).
- **The list step is the authority:** `cargo test --test d254_catalog_persist_bound -- --list` is predicted to show 4.

---

## Amendment 2 — review 1's R3, R6 and R7: text, one message string, and CI/CF's page assertion (nothing built)

Source: `artie-research frontier/d254_review1.md` @ `8ecc169`, which reviews `1eb4da3`. Verdict: SOUND for the
scoped claim. These are the lead's decisions. **No logic changes.** One error-message string is reworded, and CI and
CF gain an assertion.

### R7: the cost, restated

The PREREG's "every `persist` serializes each entry twice … a constant factor of 2 on DDL" is wrong on two counts.

- **The pre-pass costs more per entry than a second serialize.** It builds a fresh `CatalogPage`, makes a deep
  `entry.clone()` (the name, every column name, both index vectors), and calls a `serialize` that zero-fills a whole
  4096-byte page for that one entry. For clones and entry bytes the factor is 2. For buffer traffic it is about
  4073 / mean entry bytes: about 115× for a 35-byte entry (review R7).
- **It is paid on DML too, not only on DDL.** `persist` also runs whenever a DML statement moves a root: `sync_roots`
  or `sync_fulltext_roots` (`executor.rs:516-552`, from `insert`, `update` and `delete`) call
  `update_{primary,index,fulltext}_root`, which call `persist` (`catalog.rs:406`, `:419`, `:433`).
- **Still O(tables).** Nothing grows with rows or branches, and T (tables) is not one of this project's scale axes.
- **D270 removes it:** the page images serialized once ARE the encoder's answer.

### R3: every route by which `persist` can still fail after its first page write

D254 closes the ENCODER routes. The non-encoder routes remain, and there are five (review 1, READ at `1eb4da3`,
`catalog.rs:518-586`):

1. `new_page` refuses (the table region is full) at the old chain's end (`:541`). **This is reachable, not merely
   environmental.** With the catalog on k ≥ 2 pages and the region holding exactly what the DDL itself allocates,
   a statement that makes the catalog need page k+1 rewrites pages 1..k-1 and then fails.
2. `fetch_page(curr)` fails at turn 2 or later (`:519`).
3. `CatalogPage::deserialize` of an existing chain page fails at turn 2 or later (`:523`).
4. `fetch_page(new_id)` fails after `new_page` succeeded (`:549`). That leaves a partial image, and a page
   allocated but never linked.
5. The orphan tail (`fetch_page`, `deserialize` or `delete_page`, `:571-582`) fails AFTER the whole new image was
   written. This is the reverse divergence: the pages hold the new image, the refused object included, while
   `persist_or_undo` rolls memory back.

**What each leaves.** Routes 1–4 rewrite pages 1..k-1 and leave page k and the old tail. Entries can be lost from the
image, and the refused object can be recorded on the pages. D254's claim is scoped to SIZE refusals, and holds.
**D270** (the child branch) closes routes 1–4 and moves route 5's fetches ahead of every write.

### R6: edges, recorded

- **The v1 → v2 exactly-full-entry wedge** (INFERRED, pre-existing in effect).
  - A v1 catalog page carries no full-text count byte. So an entry written in v1 at exactly 4,073 bytes is 4,074 in
    v2, and 23 + 4,074 = 4,097.
  - On such a database, `persist`'s pre-pass refuses EVERY persist, including `rebuild_indexes` at open, so the
    database will not open.
  - Before D254 the same persist looped and truncated the image. D254 turns damage into a clean refusal, but the
    result is still a hard wedge.
  - It is also the one case where a DDL's own check passes and the pre-pass refuses on ANOTHER entry, which leaks
    the DDL's pages. No test is registered, because it needs a hand-built v1 page at the exact size.
- **The refusal's wording.** `refuse_unless_encodable`'s message ends "refused before anything was written", which
  is false when `persist`'s pre-pass is what fires: an ALTER's heap, or a DDL's pages, may already be written. It
  becomes **"refused before any catalog page was written"**, which is true at every call site. The one string
  changes. The tests match "catalog page" and the table's quoted name, both of which the new text still carries.
- **CI and CF passed `absent = "m"` to `assert_catalog_intact`**, a table that exists in neither test, so that half
  was vacuous.
  - The helper's `absent` becomes `Option<&str>`: CT and CP pass `Some("m")`, and CI and CF pass `None`.
  - CI and CF gain the assertion that was missing: the refused index is absent ON THE PAGES. They check
    `Catalog::open(..)`'s entry for `t`: `indexes` is empty for CI, and `fulltext_indexes` is empty for CF.
  - These strengthen assertions and weaken none. Predictions are unchanged: they pass at the tip, and no registered
    mutant changes outcome, because every mutant's refusal still comes before any page write.
- **D249's E3 patch no longer applies to this tree** after the wording change. Its context includes the message's
  second line. It is registered as not run here (amendment 1), and it still applies to D249's own tree.

### Counts

No test is added or removed. The target has 4 tests, and per-target stays 2592.

---

## Amendment 3 — amendment 2's changes recorded (nothing built)

| commit | what |
|---|---|
| `d8956c1` | amendment 2 |
| `8ef6572` | the size refusal's text: "refused before any catalog page was written" (one string) |
| `e5e2332` | CI and CF assert the refused index is absent from the catalog's pages; `assert_catalog_intact`'s `absent` becomes `Option<&str>` |

- **`catalog.rs` is unchanged since `64d6d90`,** so P1–P4 re-cut from `e5e2332` are byte-identical to the committed
  patches. The re-cut left the tree clean (`git status --porcelain`: 0).
- **D249's E1 and E4 still apply** (`git apply --check`, rc 0).
- **The run's mutant base moves to `e5e2332`,** where `src/` and `tests/` equal the tip's.
- **The counts are unchanged:** the target has 4 tests, and per-target is 2592.
