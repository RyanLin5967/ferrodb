# D270 — pre-registration, written BEFORE the fix (nothing built)

Branch `d270-catalog-persist-allocate-first`, a CHILD of `d254-catalog-persist-bound` @ `6ead01b`, D254's tip with
review 1's text amendments applied, in `/Users/idide/wt/ferrodb-d270.noindex`.

- The lead named `1eb4da3` as the parent. This branches from `6ead01b`, which contains `1eb4da3` plus review 1's
  amendments (text, one message string, and CI/CF's page assertion), so that D270 lands after D254 with no merge
  in between.

Quiet mode: nothing on this branch has been compiled or run, so every line below is a prediction. Amendments append
only. Source: `d254_review1.md` @ `8ecc169`, R3 and R7, made ledger D270 by the lead.

**Instrument:** `timeout 1800 cargo test --no-fail-fast --test d270_catalog_persist_allocate_first`. Its
`-- --list` is the authority for the count.

## The defect (READ at `6ead01b`, `catalog.rs`, `fn persist`)

`persist` reads, allocates and writes the chain in one loop. Turn k:

1. fetches and deserializes page k;
2. places entries;
3. allocates a new page only when page k is the old chain's end;
4. THEN writes page k.

**So any `Err` at turn k ≥ 2 leaves pages 1..k-1 rewritten**, with page k and the old tail unchanged. The image can
lose tables that the new layout pushed forward, and it can record the table whose statement was refused while
`persist_or_undo` removes it from memory. Review 1's R3 lists the routes:

- `new_page` refuses at the old chain's end. This is reachable: the table region is full, and the catalog is on 2 or
  more pages.
- `fetch_page` or `deserialize` fails at turn 2 or later.
- `fetch_page(new_id)` fails after an allocation.
- The orphan tail fails after the whole new image is written.

**The cost (R7).** D254's pre-pass adds a deep clone and a one-entry page `serialize` per entry to every `persist`,
DML root moves included.

## Red tests at `1793298` (additions only: `tests/d270_catalog_persist_allocate_first.rs`, `228 0`)

**The fixture.** Every harness calls `reserve_region("test floor", 256, u32::MAX)` before `Catalog::create`. It
builds eight tables `b1`..`b8`, each with four INTEGER columns of 240-byte names: 991 bytes of entry, so four fit
a page and five do not. That makes a catalog of **exactly two pages**, a premise asserted by walking the chain.

| key | test | the route |
|---|---|---|
| **NP** | `a_refused_allocation_for_the_catalog_leaves_its_pages_as_they_were` | allocation |
| **FR** | `a_catalog_page_that_does_not_read_back_is_found_before_any_page_is_written` | reading |

**NP.**

- **The premise:** adding a same-shaped `aaa` needs a THIRD page. That is `pages_for`, a greedy placement through
  `CatalogPage::has_space` in name order, which is the placement `persist` makes.
- **The setup:** the test exhausts the region with `DiskManager::allocate` until it refuses, then frees exactly three
  pages: the ones CREATE TABLE allocates for itself. The catalog's third page then has nowhere to go.
- **The assertions:**
  - the refusal is the region's `Io("no free page below …")`;
  - `aaa` is absent in memory;
  - **the set of tables on the pages** (`Catalog::open` over the same pool) **is exactly what it was before**.

**FR.**

- **The setup:** the second chain page's format byte is set to `0xEE` in the pool, so it no longer deserializes. That
  premise is asserted. It stands in for any failed fetch or decode of a later chain page, injected inside the real
  `deserialize` on real bytes.
- **The statement:** `CREATE TABLE aaa (id INTEGER NOT NULL)`, a small table that sorts first, so a rewritten page 1
  would differ.
- **The assertions:**
  - the refusal is the page's `Corruption`;
  - `aaa` is absent in memory;
  - **page 1's bytes are identical to before.**

**Run R at `1793298` (predicted): 0 passed, 2 failed.**

- **NP fails at its pages assertion.** Turn 1 rewrites page 1 as `[aaa, b1, b2, b3]`, then turn 2's `new_page`
  refuses. The pages then read `{aaa, b1, b2, b3, b5, b6, b7, b8}`: `b4` is lost, and the refused `aaa` is
  recorded.
- **FR fails at its page-1 assertion.** Turn 1 rewrites page 1 as `[aaa, b1, b2, b3, b4]`, then turn 2's
  `deserialize` refuses.
- Both refusal assertions pass at the base.

## The fix, as it will be made

`persist` becomes phases. Each phase finishes before the next begins:

1. **Lay out** (memory only). Place the sorted entries greedily into page images, as the loop did. An entry that
   fits no EMPTY page is refused there, by `refuse_unless_encodable`, whose `has_space` arm returns the size
   `Constraint`. The D254 per-entry pre-pass is removed.
2. **Serialize every page image ONCE**, with placeholder links. Any length-prefix refusal happens here. **The page
   images are the encoder's answer**, so there is no separate per-entry `serialize`. That is R7's cost removed, back
   to the pre-D254 P page serializes and one clone per entry.
3. **Walk the existing chain, reads only,** for the page ids it owns. It checks every page's format and stops on a
   cycle (a `Corruption`, where the loop used to spin).
4. **Allocate every missing page, before any catalog write.** Each is stamped as an empty catalog page, as the loop
   did. If one refuses, the pages this call allocated are freed and the error is returned. The old chain is
   untouched.
5. **Write** each target page in order: fetch, stamp its id and its successor's into its image (a new
   `CatalogPage::stamp_links`, which `serialize` also uses), copy, unpin.
6. **Free the surplus:** the old chain's pages past the new end, which are already unlinked.

**What this closes.**

- The `new_page` route: all allocation happens in step 4, before any catalog write.
- The `deserialize` route at turn 2 or later, and the first fetch of every existing chain page: step 3.
- The `fetch_page(new_id)` route: step 4 stamps each new page before anything links to it.

**The residual, stated in the code.**

- **An I/O failure in step 5.** A `fetch_page` of a target page can fail mid-write, for example when evicting a dirty
  frame fails. Pages already written then hold the new image and the rest the old one. Each link still points at a
  readable page, because every fresh page was stamped in step 4, but the image is mixed.
- **Step 6** can fail after the image is complete (`fetch_page` or `delete_page` of a surplus page). The error is
  returned, which is the reverse divergence: the pages hold the new image while `persist_or_undo` rolls memory back.
- Step 5 is not pinned ahead. Pinning every target page first would make the writes infallible, but a catalog of
  more pages than the pool has frames (1,024) could then never persist. That would be a new wall, and it is not
  taken.
- **Correction to D254's amendment 2** (append-only there): it says D270 "moves route 5's fetches ahead of every
  write". That is wrong. Step 3 moves the surplus pages' DESERIALIZE ahead. Their fetch and delete stay after the
  write, in step 6.

**Also not closed:** a CREATE TABLE refused by a `persist` failure that is not an encoder refusal (NP's region-full
case) still leaks the three pages it allocated first. `persist_or_undo` undoes the map, not the pages. That is
pre-existing, and NP asserts nothing about it.

## Run G at the fix (predicted)

- `--test d270_catalog_persist_allocate_first`: its list shows **2**, and both pass.
- D254's `d254_catalog_persist_bound`: all 4 still pass. CP now refuses in step 1's layout, with the same size
  `Constraint` and before any page write.
- **Per-target: 2592 + 2 = 2594** (2579 measured on main's tree).

## Mutants (cut from the fix; predicted)

| mutant | what it does | fails |
|---|---|---|
| **Q1 `first_page_written_before_allocating`** | writes page 1's new image between step 3 and step 4 | **NP** (its pages assertion). FR survives, because step 3 refuses before Q1's write |
| **Q2 `first_page_written_before_the_walk`** | writes page 1's new image before step 3 | **FR and NP** |
| **Q3 `layout_size_refusal_removed`** | step 1 no longer refuses an entry that fits no empty page | **CP** (D254's test): `add_entry` returns `NotEnoughSpace`, not the size `Constraint`. CT, CI and CF survive, because their DDL checks refuse first |

---

## Amendment 1 — the fix and mutants recorded; Q2's FR prediction corrected (nothing built)

| commit | what |
|---|---|
| `1793298` | red tests NP, FR (additions only, `228 0`) |
| `ce06adc` | this PREREG |
| `05878dc` | **the fix**: `persist` in six phases; `stamped_catalog_page`, `release_catalog_pages`, `write_catalog_page`; `CatalogPage::stamp_links`, which `serialize` now uses; `refuse_unless_encodable`'s doc |
| `3b37071` | mutants Q1–Q3, cut from `05878dc` by `bench/d270/make_mutants.py`, each passing `git apply --check` |

- **What was removed at `05878dc`:** the old one-loop body of `persist`, including D254's per-entry pre-pass, which
  the layout and the single serialize replace; and `serialize`'s two link lines, now one `stamp_links` call.
  `git diff 6ead01b 05878dc -- src/` is `catalog.rs` `121 69` and `catalog_page.rs` `14 4`.
- **Which D254 mutants still apply here.**
  - P2, P3 and P4 apply, because the DDL checks are unchanged.
  - P1 (the `persist` pre-pass) does not apply: the pre-pass no longer exists. Its role, refusing an oversized
    entry inside `persist`, is Q3's here.
  - Both facts are from `git apply --check`.
- **Q2's FR prediction, corrected.** Q2 writes page 1's new image before the walk, with a `next` of 0, because the
  walk has not yet read page 1's successor. That cuts the chain.
  - The walk then never reaches the damaged page 2, the statement SUCCEEDS, and FR fails at its
    `.expect("… cannot be rewritten")`, not at the page-1 assertion.
  - NP fails at its pages assertion: page 1 holds `[aaa, b1, b2, b3]`, and allocating the two missing pages
    refuses.
  - **Q2 → FR (at the refusal expectation) and NP.**
- **Counts:**
  - `d270_catalog_persist_allocate_first` has **2** `#[test]`, with no `cfg`, `ignore` or macro;
  - `git diff 6ead01b 3b37071` adds 2, and from `9aa6968` it adds 15, with 0 removed.
- **Per-target: 2579 + 15 = 2594.**
- **The list step is the authority:** `cargo test --test d270_catalog_persist_allocate_first -- --list` is predicted
  to show 2.

| mutant | fails |
|---|---|
| Q1 | NP (pages assertion) |
| Q2 | FR (the statement succeeds) and NP (pages assertion) |
| Q3 | CP, in D254's target: `NotEnoughSpace` instead of the size `Constraint` |
