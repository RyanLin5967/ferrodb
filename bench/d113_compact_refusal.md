# D113 — a refused cell insert left the node compacted and unstamped

Branch `D113-compact-refusal`, worktree `/Users/idide/wt/ferrodb-D113-compact-refusal`,
rebased onto `1bf8abe` (after D108-neighbour-merge and D112-unlink-free-last landed).

## The defect, as confirmed

`NodeMut::insert_cell_at` and `NodeMut::replace_cell_at` compact the cell heap before they
refuse. The doc comment claimed a refusal left the node "untouched"; it did not. Both `btree`
callers then drop the page's write guard immediately after a refusal and run a **fallible**
step — `alloc_for`, once per cut — before anything restamps the checksum. Failing there
returned `Err` with the page compacted and carrying a checksum for bytes that had moved, and
`CowStore::read_page` verifies on every fetch: a page whose rows were untouched and correct
answered *"refusing to return a torn page"*.

The lead's reading of the mechanism was correct in every particular I checked.

## Where the lead's reading needed correcting

**1. `leaf_put`'s exposure is real in the code but not reachable through `CowTree::insert`.**
A leaf only refuses an insert when it is fuller than `NODE_CAPACITY - entry`, i.e. above 3046
of 4060 bytes. The D89 content-defined chunker targets `NODE_CAPACITY >> 3` = 507 bytes, so
that is a ~6x tail run. Measured on builds of 2000–8000 keys at three value sizes:

| build | leaves | mean occupancy | max | leaves that could refuse a max entry |
|---|---|---|---|---|
| 2000 keys, 16 B values | 149 | 496 | 1776 | 0 |
| 8000 keys, 16 B values | 584 | 506 | 1924 | 0 |
| 8000 keys, 60 B values | 1281 | 505 | 2268 | 0 |
| 2000 keys, 300 B values | 1265 | 507 | 1284 | 0 |

Zero, in every build. `internal_relink` is the reachable one: its level is still byte-balanced,
so internal nodes do pack to capacity. The node-level tests pin the leaf path regardless.

**2. `a6` and `a7` are not one bug, and the distinction decided where the fix goes.**
`a6` is the compaction-on-refusal defect. `a7`'s page is dirty for a different reason: the
separators that *did* land mutated it and nothing stamped after them. They happen to share a
fix only because `internal_relink`'s loop has exactly one exit — a refusal — and a refusal now
restamps. That is a real structural argument rather than a coincidence, and it is written into
the code at both sites so the next reader does not add a second stamp "to be safe".

**3. There is a third caller of `replace_cell_at` the brief did not mention**, at
`src/branch/delta.rs:812`. It is inside `#[cfg(test)]` (block opens at :685) and asserts the
call returns `true`, so it never takes the refusal path — but it measures page diffs in bytes
(1373 B against 2552 B on its fixture), which is why the stamp went on the refusal return
rather than inside `compact()`.

## The fix

`NodeMut` now holds the whole page rather than just the payload, and both refusal returns go
through `restamp_after_refusal`, which stamps the checksum and returns `false`. This
establishes the postcondition **a cell insert that returns `Ok(false)` leaves a page that
verifies** — reachable only after `compact()`, because `write_cell` writes nothing when it
returns `None`.

Not inside `compact()`: it also runs on the success path, where the caller immediately dirties
the page and restamps anyway, so a stamp there is dead work on the insert path and would
perturb `branch::delta`'s compaction-cost fixture. Not restoring the pre-compaction bytes:
that means snapshotting 4 KB on the insert path to serve an error case. No second stamp at
either `btree` call site: a redundant downstream stamp masks every mutant of the one in front
of it.

The module brief's "nothing here may touch the header" gains an explicit carve-out for the
checksum and nothing else.

## Other callers of `compact()`

Exactly two, both in `node.rs` — `insert_cell_at:344` and `replace_cell_at:368`. Both fixed.

Enumerating the wider pattern from the only mutation entry point, `NodeMut::new`: 12 sites in
`btree.rs`, 8 in `node.rs` (its own tests plus the impl), 2 in `branch/delta.rs`, 1 in
`cow/diff.rs`. Every `btree.rs` write-guard scope stamps before dropping, including D108's new
`unlink_up` (:989) and the delete path (:377); the `delta.rs` and `diff.rs` sites are
`#[cfg(test)]` and use `init`/`fill_leaf`/a `replace_cell_at` asserted to succeed. No other
production path mutates a node and drops a guard without stamping.

## Evidence

(filled in below)
