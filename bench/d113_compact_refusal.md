# D113 — a refused cell insert left the node compacted and unstamped

Branch `D113-compact-refusal`, worktree `/Users/idide/wt/ferrodb-D113-compact-refusal`,
rebased onto `8249c50`. The full battery (all four arms, including the fire-check) ran at
`3f680f9` — raw log `bench/d113_battery.log`, earlier pre-rebase run kept as
`bench/d113_battery_d6771d8.log`. `8249c50` touches neither `src/cow` nor `src/branch`, and the
cow suite was re-confirmed after the rebase:

```
=== cargo test --lib cow @ d5ced61 ===
test result: ok. 163 passed; 0 failed; 1 ignored; 0 measured; 1447 filtered out; finished in 28.28s
```

`d5ced61` is the code tip that green names. Only this file changes above it.

## The defect, as confirmed

`NodeMut::insert_cell_at` and `NodeMut::replace_cell_at` compact the cell heap before they
refuse. The doc comment claimed a refusal left the node "untouched"; it did not. Both `btree`
callers then drop the page's write guard immediately after a refusal and run a **fallible**
step — `alloc_for`, once per cut — before anything restamps the checksum. Failing there
returned `Err` with the page compacted and carrying a checksum for bytes that had moved, and
`CowStore::read_page` verifies on every fetch: a page whose rows were untouched and correct
answered *"refusing to return a torn page"*.

The lead's reading of the mechanism was correct in every particular I checked.

## Where the brief needed correcting

**1. `leaf_put`'s exposure is real in the code but not reachable through `CowTree::insert`.**
A leaf refuses an insert only above 3046 of its 4060 bytes. The D89 content-defined chunker
targets `NODE_CAPACITY >> 3` = 507, so that is a ~6x tail run. Measured:

| build | leaves | mean occupancy | max | leaves that could refuse a max entry |
|---|---|---|---|---|
| 2000 keys, 16 B values | 149 | 496 | 1776 | 0 |
| 8000 keys, 16 B values | 584 | 506 | 1924 | 0 |
| 8000 keys, 60 B values | 1281 | 505 | 2268 | 0 |
| 2000 keys, 300 B values | 1265 | 507 | 1284 | 0 |

Zero, in every build. `internal_relink` is the reachable caller: its level is still
byte-balanced, so internal nodes pack to capacity. The leaf path is pinned at the node level.

**2. `a6` and `a7` are not one bug, and the distinction decided where the fix goes.**
`a6` is compaction-on-refusal. `a7`'s page is dirty for a different reason: the separators that
*did* land mutated it and nothing stamped after them. They share a fix only because
`internal_relink`'s loop has exactly one exit — a refusal — and a refusal now restamps, which
covers the earlier placements as a side effect. That is a structural argument, and it is
written at both call sites so nobody adds a second stamp "to be safe".

**3. A third caller of `replace_cell_at` the brief did not mention**, `src/branch/delta.rs:812`.
Inside `#[cfg(test)]` (block opens at :685) and asserts the call returns `true`, so it never
takes the refusal path — but it measures page diffs in bytes (1373 B against 2552 B on its
fixture), which is why the stamp went on the refusal return rather than inside `compact()`.

## The fix

`NodeMut` now holds the whole page rather than only the payload, and both refusal returns go
through `restamp_after_refusal`, which stamps the checksum and returns `false`. The
postcondition: **a cell insert that returns `Ok(false)` leaves a page that verifies** —
reachable only after `compact()`, because `write_cell` writes nothing when it returns `None`.

- Not inside `compact()`: it also runs on the success path, where the caller immediately
  dirties the page and restamps anyway, so a stamp there is dead work on the insert path and
  would perturb `branch::delta`'s compaction-cost fixture.
- Not restoring the pre-compaction bytes: that means snapshotting 4 KB on the insert path to
  serve an error case.
- No second stamp at either `btree` call site: a redundant downstream stamp masks every mutant
  of the one in front of it.

The module brief's "nothing here may touch the header" gains a carve-out for the checksum and
nothing else.

## Other callers of `compact()`

Exactly two, both in `node.rs` — `insert_cell_at` and `replace_cell_at`. Both fixed.

Enumerating the wider pattern from the only mutation entry point, `NodeMut::new`: 12 sites in
`btree.rs`, 8 in `node.rs`, 2 in `branch/delta.rs`, 1 in `cow/diff.rs`. Every `btree.rs`
write-guard scope stamps before dropping, including D108's `unlink_up` and the delete path; the
`delta.rs` and `diff.rs` sites are `#[cfg(test)]` and use `init`/`fill_leaf`/a `replace_cell_at`
asserted to succeed. No other production path mutates a node and drops a guard without stamping.

## Evidence — battery at `3f680f9`, one lock hold, `load_at_acquire=4`

**Arm 1 — the D113 probes with the fix**

```
running 5 tests
test ...a_starved_insert_loses_no_row_that_the_unstarved_control_keeps ... ignored, ... Not D113.
refused insert rewrote 213 bytes
partial promotion placed 1/3 separators
refused replace rewrote 213 bytes
test cow::tests_compact_refusal::a_refused_insert_leaves_a_page_that_verifies ... ok
test cow::tests_compact_refusal::a_partial_promotion_leaves_a_page_that_verifies ... ok
test cow::tests_compact_refusal::a_refused_replace_leaves_a_page_that_verifies ... ok
starved failures: 300 (300 rewrote page bytes, 8 left a page compacted in place)
test cow::tests_compact_refusal::a_starved_allocation_never_leaves_an_unverifiable_page ... ok
test result: ok. 4 passed; 0 failed; 1 ignored; 0 measured; 1606 filtered out; finished in 27.10s
```

**Arm 2 — fire-check: the stamp deleted, the same probes must go red**

```
 src/cow/node.rs | 2 +-
refused replace rewrote 209 bytes
refused insert rewrote 209 bytes
partial promotion placed 1/3 separators
test cow::tests_compact_refusal::a_partial_promotion_leaves_a_page_that_verifies ... FAILED
test cow::tests_compact_refusal::a_refused_insert_leaves_a_page_that_verifies ... FAILED
test cow::tests_compact_refusal::a_refused_replace_leaves_a_page_that_verifies ... FAILED
test cow::tests_compact_refusal::a_starved_allocation_never_leaves_an_unverifiable_page ... FAILED
test result: FAILED. 0 passed; 4 failed; 1 ignored; 0 measured; 1606 filtered out; finished in 0.72s
restore OK: node.rs matches HEAD and the pre-mutation backup byte for byte
```

Every probe goes red when the guard is removed, **including the end-to-end one** — the
assertion was run against the value it exists to prevent, not only against the correct one.
That check earned its keep: before the fixture concentrated its probe inserts, the end-to-end
probe recorded 493 starved failures and **passed** without once reaching the path it claims to
test.

The two arms also settle the byte count: **209 bytes rewritten with the stamp removed, 213 with
it**. The four are the checksum, measured rather than inferred.

**Arm 3 — `cargo test --lib cow`**

```
test result: ok. 163 passed; 0 failed; 1 ignored; 0 measured; 1447 filtered out; finished in 27.79s
```

## Separate finding: a starved insert is not atomic and loses committed rows

Not D113, and it wants its own row.

```
rows missing -- control: 0 base,  0 gap
                starved: 28 base, 165 gap
```

Both arms insert the identical 820 keys; the starved arm merely has one allocation-starved
attempt per key before the successful one. The control losing **zero** is what makes the
starved arm's losses attributable to the abort rather than to the key sequence.

The alarming half is the gap figure: every one of those 220 keys was *eventually* inserted by a
later call that returned `Ok`, and 165 are still missing at the end.

Mechanism, stated as the hypothesis it is: on the trunk every page is private, so `cow_page`
returns the same page and the descent mutates it **in place**; a split that fails part-way
leaves the leaf truncated to its first piece with the remaining pieces allocated and
unreferenced. I have not instrumented that path, so treat the mechanism as unverified — the
measurement is not.

Same family as D112 — a correct happy path with a broken error path. The fix is a design choice
(shadow even a private page across a split, or stage the relink), not a patch. The probe is
`#[ignore]`d rather than deleted, numbers in its doc comment; it fails with the D113 fix present
and with it reverted, so it discriminates nothing about D113.

## Separate finding: internal-node headroom, and what makes a refusal reachable

Both rows 1500 inserts, 8-byte values.

| key size | leaves | internals | separator cost | per full node | occupancy mean / max (cap 4060) | internals that would refuse the next separator |
|---|---|---|---|---|---|---|
| 200 B | 634 | 63 | 217 B | 18 | 2180 / 3689 | **0** |
| 400 B | 1245 | 247 | 417 B | 9 | 2100 / 3753 | **1** |

400-byte keys do push it over: the "3689 of 4060, just short" margin becomes a refusal once the
separator is coarse enough that a node's last admissible separator leaves under one separator of
headroom. Mean occupancy is ~52% in both rows — the split-in-half-then-refill signature — so it
is the tail that matters, and the tail is set by how many separators a node holds.

**Key size alone was not the lever.** With 400-byte keys scattered across the key space the
sweep still recorded 493 starved failures and reached the refusal path **zero** times, because a
promotion has to arrive at a node that is full right then. What made it reachable was
concentrating every probe insert into one key gap, so every leaf split promotes into the same
parent and that parent fills in ~9 promotions: 300 starved failures, **8** refusals. Anyone
reproducing this should copy the concentration, not just the key size.
