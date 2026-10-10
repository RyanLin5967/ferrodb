# Branches, COW pages, and reclamation — source evidence

Inspected against `fc9556a742f61884a3011ccc28e78f160a8d57fe`. This report describes actual call paths, not broad README promises. Existing tests below were inspected; this worker did not run them or modify application code.

## 1. Diagram boundary that must remain visible

**Ordinary SQL heap and agent COW tree are different stores.** An agent SQL SELECT scans the current committed heap and overlays its private workspace rows/deletions. It does not read a complete fork-time snapshot of ordinary tables from its COW root.

- `runtime.rs:2152` `visible_rows_where`: `scan_table_where` first, then private `Present`/`Deleted` entries replace/remove matching row IDs; predicates apply to both images.
- `runtime.rs:6771`, especially `6779`: base scan builds `ReadView { snapshot: read_snapshot_cached(), txn_id: 0 }` for the call.
- `runtime.rs:3098`: staging writes first-touch before-images and current after-images into workspace, accumulates operations/guards, appends TEL (`3129`), then mirrors after-images to COW (`3141`).
- `tests/integration_trunk_tree_authority.rs:170`: ordinary trunk rows are in the heap; trunk's COW tree remains empty. Its root nevertheless names a real arena page (`194`).
- `tests/integration_branch_pages.rs:315`: a branch page diff does not contain the original heap row's before-image because ordinary base rows are not in this tree.

**Recommended main diagram:** show a solid SQL-read edge from heap to overlay, a solid edge from workspace to overlay, and a separate staging edge to COW mirror. Do not put COW between every SQL reader and its data. Do not portray the tree as the ordinary SQL index.

## 2. What a branch and a fork actually contain

`BranchRecord` (`src/branch/record.rs:18`) contains identity/generation, parent ID, fork epoch, root page ID, lease deadline, state, owned arenas, depth, and capability envelope. The production catalog represents live-child relationships with indexed keys; do not draw the record's `live_children` vector as production authority.

`PageId` is `u32` (`src/branch/types.rs:14`), an addressable page number, not an in-memory pointer or content hash. `root_page_id` names the node from which the complete COW tree can be reached. "Root" is a position: a small tree can have one root that is also a leaf.

Fork has two coordinated sharing operations:

1. **Durable page root:** `fork_child_parts` assigns `child.root_page_id = parent_root_page_id` (`record.rs:244`); no user-data tree walk or copy.
2. **Runtime workspace:** `begin_session_as_staged` clones the parent's immutable `rows`, `base_rows`, `tables`, and `base_shapes` roots (`runtime.rs:1737`). Each `PersistentMap::clone` is one `Arc` root clone (`persistent_map.rs:190`). The map is an immutable weight-balanced binary search tree, not the disk B+ tree.

The parent and child then rebind their own roots on writes. Neither changes a shared immutable workspace node, and a child does not search through ancestors to find a row. `PersistentMap::insert` copies a search path (`persistent_map.rs:324`). Parent private writes made after fork remain invisible to the child; **this is separate from current committed heap visibility**.

Production metadata is `TableBranchCatalog`, a B+ tree in `{db}.branchcat` with its own buffer pool (`table_catalog.rs:162`). The catalog tree's own root is distinct from the COW root ID stored as a field in a branch record.

`fork_staged` (`table_catalog.rs:920`) obtains an epoch, validates parent generation/state, inherits its envelope, allocates/reuses a branch ID, writes the child record and parent-child index under one logical lock, and returns a durability ticket. The durable form waits after releasing that lock (`903`). `durable` flushes the catalog pool and syncs its file (`373`), allowing group commit.

**Cost statement:** fork avoids copying the entire parent data set. It still incurs catalog/index work, identity/provenance work, synchronization, and durability work. Map-root cloning is O(1), but an inherited transaction list can be cloned (`runtime.rs:1751`); whole-session fork is not literally a single assignment. Nothing here establishes that branches are cheaper than normal SQL transactions.

## 3. Exact page/tree drawing

Every COW page is 4 KB. Its first **24 bytes** are the COW header (`src/cow/page_header.rs:1`):

| Bytes | Field | Meaning |
|---|---|---|
| 0–7 | birth epoch | Logical allocation event used by sharing/GC |
| 8–11 | arena ID | Identifies the allocation extent/owner |
| 12–15 | CRC32 | Detects corrupted/torn page contents |
| 16 | page type | Internal, leaf, etc. |
| 17 | flags | Includes private flag; ownership/epoch checks still determine mutation safety |
| 18–23 | reserved | Header padding/reserved bytes |
| 24 onward | payload | The actual tree node |

The payload is a slotted node (`src/cow/node.rs:19`): entry count, heap-end offset, leftmost child ID, slot array, and variable-length cells. An **internal** cell stores separator key + child page ID. A **leaf** cell stores key + value. Child IDs are payload; the branch's root ID is branch metadata, not an every-page header field.

For `PagedRows`, a key is `table_id:u32 || row_id:u64` in big-endian order (`src/agent_sql/paged_rows.rs:36`). The leaf value is the encoded typed row (`205`). A single COW tree can therefore hold staged rows from several tables. Its leaves store row values, unlike the ordinary SQL index's key-to-row-location role.

**No leaf sibling pointers** (`cow/node.rs:14`): ordered iteration uses a descent stack (`cow/btree.rs:1305`). Links between leaf neighbors would make a copy require neighbor pointer updates, undermining local shadowing.

## 4. Read and write algorithms

**Read:** branch record → root ID → internal separator/child choices → leaf key lookup. `CowTree::get` (`btree.rs:214`) only receives root and key; it has no branch ancestry traversal. `ArenaPageStore::read_page` (`arena.rs:1999`) fetches through the buffer pool and verifies checksum. `PageHandle` pins the cached frame; writes mark it dirty, dropping the handle unpins it (`cow/mod.rs:62`, `89`, `121`). A cache miss loads a page; a hit reuses memory. Fork does not preload the tree.

**Write:** descend to leaf → obtain writable page → edit/split leaf → update ancestor child pointers → return resulting root. `CowTree::insert` (`btree.rs:466`) and `relink_up` (`942`) implement this. The runtime records a changed root using `BranchCatalog::set_root` (`runtime.rs:1486`; `table_catalog.rs:1124`).

`ArenaPageStore::cow_page` (`arena.rs:2008`) makes the key decision:

```text
privacy barrier = max(branch's fork epoch, latest live child's fork epoch)

writer owns page's arena AND page.birth_epoch >= barrier ?
    yes → edit the existing private page in place
    no  → allocate page in writer's arena; copy payload; stamp new header
```

Ownership means any arena owned by this branch, not only its current allocation arena (`arena.rs:2035`). A parent's page that a child could see must be shadowed on a later parent write. An inherited page is not the child's property to retire.

Path copying stops if neither a child pointer nor separator changes (`btree.rs:954`). New splits can propagate upward and create a new root (`967`). Leaf layout uses key hashes and entry sizes to choose content-defined boundaries, with page-capacity limits (`btree.rs:787`; `chunker.rs:255`); updates/deletes can repair boundaries by merging right. Thus the simple path-only example below assumes no split/boundary repair.

`WriteJournal` (`btree.rs:147`) saves before-images for pages mutated in place and delays retirement of replaced pages. On a tree-operation error it restores those pages (`416`); successful operations retire old owned pages. Newly allocated unreachable pages can remain until branch cleanup rather than risk freeing reachable data.

**Atomicity caution:** this is one in-memory tree-operation rollback mechanism, not a WAL transaction and not a power-failure guarantee. `stage_all` updates workspace, TEL, and pages in sequence; do not draw them as one atomic durable write. Publishing a root reference and committing ordinary SQL rows are different operations.

## 5. Concrete before/after example for the visual

Illustrative IDs, assuming all shown pages are shared and the update needs no split/boundary repair:

```text
AFTER FORK                         AFTER CHILD CHANGES RIGHT LEAF

Parent record: root=7              Parent record: root=7
Child record:  root=7              Child record:  root=30
                │                                    │
           [7: internal]          [7: original]    [30: copied internal]
             /      \              /     \          /          \
      [12: leaf] [19: leaf]    [12: shared] [19: original]     [31: changed copy]
```

Exact edges after update: `Parent → 7`, `7 → 12`, `7 → 19`; `Child → 30`, `30 → 12`, `30 → 31`. Page 19 is copied because data changes; page 7 is copied because its child pointer changes; page 12 stays shared. Only the child's branch record receives root 30. A later edit of private page 31 can be in place if no new sharing/split/boundary repair requires copying.

This drawing should be labeled **COW branch page tree**, not "entire ordinary SQL database at fork." To show real runtime data flow, put it alongside the workspace + heap overlay.

## 6. Diff, structural merge, and optional facilities

- Production page diff uses `cow::diff::diff` with `PageIdentity` (`runtime.rs:2096`, `2106`). Simultaneous descent aligns key spans; equal page IDs skip a shared subtree without loading it (`cow/diff.rs:1030`). It does not enumerate every node before skipping.
- Old `CowTree::diff` (`btree.rs:309`) still enumerates both trees' page sets. Do not use its complexity to describe the current runtime page-diff path.
- SQL's semantic merge uses recorded operations; `cow::merge3` is a separate structural three-way library. Trees alone see 10/15/13 as conflicting values; recorded `Add(5)` and `Add(3)` can explain a compatible result of 18 (`merge3.rs:1`).
- `subtree_cid` computes optional fingerprints; it does not assign page addresses or control liveness (`cow/cid.rs:1`, `358`). `dedup.rs:1` explicitly remains unwired; do not draw global content-addressed/refcounted storage.
- `delta_against_base` can compute a page delta, but `cow_page` still stores whole pages; no compressed delta chain is on the live read/write path (`arena.rs:887`).

## 7. Arenas, epochs, leases, and reaping

An arena is a contiguous allocation extent owned by one branch. Extents grow 1, 2, 4, …, 256 pages, then remain capped at 256 (`branch/types.rs:271`, `297`). This avoids reserving 1 MiB for the first 4 KB write while retaining bulk-free efficiency for larger branches. A branch can own several arenas.

`ArenaPageStore` uses the ordinary main-file buffer pool but a separate reserved page region, coordinated with the ordinary allocator. Arena allocation/free state is persisted through checkpoints/tail records; restored metadata reconstructs allocation ownership (`arena.rs:1872`, `2190`). The arena floor is fixed for an existing DB; ordinary-table growth cannot silently overlap it (`storage/disk_manager.rs:385`).

Keep three independent concepts separate:

| Concept | Purpose |
|---|---|
| Logical epoch | Monotonic ordering of page births/forks/frees; `next_epoch` increments a counter (`table_catalog.rs:895`) |
| Lease deadline | When an abandoned branch is eligible for cleanup; default agent lease 15 minutes (`runtime.rs:96`), scan default 30 seconds (`lease_thread.rs:120`) |
| Branch generation | Fences stale handles after a branch ID is recycled (`record.rs:294`, `316`) |

The safe-free rule is **no live child fork epoch in `[page birth, page logical-free epoch)`** (`record.rs:1267`). Such a child might still reach the old page. A pinned free becomes pending; otherwise the page can be reused (`arena.rs:2132`). This is conservative lineage protection, not per-page reference counting or whole-database reachability tracing.

Reaping (`reaper.rs:619`) proceeds:

1. Validate generation; refuse trunk; transition durably to `Reaping`, which rejects reads.
2. Ask the catalog for live descendants through child relationships. Childless: free owned arenas wholesale. Otherwise: evaluate page lifetime intervals and park still-needed pages.
3. Mark `Reaped`, bump generation, clear arena ownership from branch record.
4. Detach only when its subtree has no live child; release reusable ID; retry pending frees.

**Interior ancestors cannot simply disappear.** `detach_from_parent` (`reaper.rs:242`) retains the link for a reaped parent that has a live descendant, preserving that descendant's claim on grandparent pages. Once the last descendant disappears, it cascades upward. The production catalog resolves these descendant relationships (`table_catalog.rs:753`). Data reads avoid ancestry, but cleanup/control work can depend on ancestry depth/fanout.

On startup `resume_interrupted_reaps` handles durable `Reaping` records deepest-first and sweeps orphaned extents (`reaper.rs:185`). This cleanup recovery does not imply unfinished agent SQL workspaces are reconstructed. Branch GC also is not vacuuming heap MVCC row history.

## 8. Proposed atlas nodes and labeled edges

| From → To | Label |
|---|---|
| BEGIN AGENT SESSION → Branch catalog | Create child metadata; inherit root; index parent-child relationship |
| Parent workspace → Child workspace | Share immutable map roots at fork |
| Branch catalog → COW root | `root_page_id` points into main-file arena pages |
| SQL base heap → Agent read overlay | Current committed row versions |
| Workspace → Agent read overlay | Private after-images and delete markers override base |
| Agent edit → Workspace | First-touch before-image + latest private state |
| Agent edit → TEL | Typed intent and guard predicates |
| Agent edit → PagedRows → CowTree | Encode staged row; mirror private after-image |
| CowTree → ArenaPageStore | Request private page or shadow copy |
| ArenaPageStore → Buffer pool → Main file | Pin/load/dirty/flush 4 KB pages |
| CowTree → Branch catalog | Publish changed root ID |
| Parent/child roots → Page diff | Paired descent; skip shared page IDs |
| Workspace + TEL → Semantic merge | Conflict/admission logic; separate from structural merge library |
| Successful merge → Ordinary SQL heap/WAL | Publish accepted row changes; covered by other atlas section |
| Finish/abandon/lease expiry → Reaper | Retire branch |
| Reaper → Catalog + Arena store | Preserve live descendants; return safe pages/extents |
| Startup → Reaper | Resume interrupted cleanup; not session reconstruction |

## 9. Existing checks that directly support these claims

- `tests/integration_cow_branch.rs:64`: multi-page tree fork allocates no data pages; child root is parent root and gets all values by ordinary descent.
- `tests/integration_trunk_tree_authority.rs:122`, `170`, `194`: branch rows physically exist on pages; merge reaches heap; trunk heap/tree boundary is explicit.
- `tests/integration_branch_pages.rs:127`, `160`, `193`, `224`, `245`, `315`: mirror agreement, sibling isolation, deletes, allocation, and missing base image in page diff.
- `tests/d27_fork_shares_without_leaking.rs:130`, `183`, `241`, `338`: post-fork parent edits, siblings, ancestor-independent reads, and schema sharing.
- `src/branch/arena.rs:2800`: private-page in-place mutation; `src/cow/tests_isolation.rs:212` covers analogous tree/store behavior.
- `tests/integration_production_diff_wiring.rs:192`, `257`, `287`: real runtime diff cost, identical roots requiring zero reads, full differences requiring traversal.
- `tests/d18_reaper_asks_the_catalog.rs:115`, `121`: parent reap retains pages visible to live child on both catalogs.
- `tests/d16_transitive_visibility.rs:74`, `81`: pruning interior parent retains grandchild's protection. Source comments still cite an old `s18` filename; current file is `d16`.
- `tests/d15_concurrent_fork_and_reap.rs:166`, `169`: concurrent fork/reap for both catalogs.
- `tests/integration_server_reaps.rs:492`, `540`, `613`, `667`: autonomous cleanup, merged-branch reclamation, startup completing interrupted reap.

## 10. Accuracy traps for final review

Do not say: fork copies one data page; root is a RAM pointer; child IDs live in the COW header; all B+ tree leaves are linked; every write always copies a root path; every agent SQL read comes from COW; a branch freezes every heap table; branch before-image always means fork-time heap image; root publication alone is crash-atomic; branch GC removes MVCC history; content addressing is the production page identity; branches are inherently cheaper than transactions; durable branch pages imply recoverable unfinished SQL sessions.
