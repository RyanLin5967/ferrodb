# D16 chain retention — PRE-REGISTRATION

Written and committed BEFORE the harness (`examples/d16_chain_retention.rs`) exists. Amendments
are append-only, at the bottom, each dated and committed before the run it amends.

- worktree: `/Users/idide/wt/ferrodb-d16-chain-arm.noindex`
- branch: `d16-chain-arm`
- base: `9aa6968` (main)
- produced under QUIET MODE: nothing here has been compiled or run.

## The claim under test

SCALE-DESIGN D16, "The decision: option 1": a `Reaped` interior branch that still
`has_live_children()` keeps counting as a live child of its own parent, so its pages stay parked
while any descendant lives. The recorded cost, never measured:

> an abandoned interior branch's arenas stay pinned by one live descendant — a space leak
> proportional to chain length, which is the exact shape MCTS generates.

This measures the retention. Whether that retention is a *leak* is a separate question, and one
arm below exists to answer it (see "What `chain` cannot tell you").

## Fixture

Every (arm, D) cell gets a fresh directory, a fresh `TableBranchCatalog` sidecar (the shipped
catalog), a fresh `ArenaPageStore`, and a fresh `TwoTierReaper`. The free-space map is persisted
through `checkpoint_to`, as `src/cli/cli.rs` does (`D16_PERSIST=0` turns it off; the header says
which). Single-threaded, so every count is fixed by control flow.

"Write a page" is `PageStore::alloc_for(branch, BTreeLeaf, next_epoch())` followed by a payload
byte and `stamp_checksum`: the calls `examples/branch_curve_writes.rs` and
`examples/d31_reap_cost.rs` use. P pages per branch. Default **P = 2**, chosen so that pages,
extents and reserved pages are three different numbers (geometric extents 1 + 2 → 2 extents,
3 reserved pages, 2 allocated).

Leases: every branch that is to be reaped first gets `LeaseDeadline(1_000)`; the survivor gets
`LeaseDeadline(2_000)`.

**Reaping goes through `Reaper::reap_expired` and nothing else.** It is the two halves that
`lease_thread::scan_once` runs (`expired_candidates`, deepest first, then
`reap_if_still_expired` per candidate), back to back. Each reap ends in `drain_pending_seeded`.
No reap is hand-rolled, and no reap skips the drain (the D183 retraction).

- **sweep 1:** `reap_expired(1_000)` — reaps every branch except the survivor.
- then `drain_pending()` once more and `collect_orphaned_extents()` once — the two collectors
  production also runs — so the reported retention is the steady state, not a transient.
- **sweep 2 (control a):** `reap_expired(2_000)` — reaps the survivor, then the same two
  collectors.

Arms, all with the same total writes (P·D pages) and the same number of branches (D):

| arm | topology | writes | survivor |
|---|---|---|---|
| `chain` | trunk → b1 → … → bD; each bk writes P fresh pages, then forks b(k+1) | fresh pages | the leaf bD |
| `fanout` (control b) | trunk → b1 … bD, all siblings | fresh pages | the last sibling |
| `overwrite` (added by me, not in the brief) | the `chain` topology | b1 writes P fresh pages; each b(k+1) overwrites its parent's P pages through `PageStore::cow_page` | the leaf bD |

Control (a) is sweep 2, run in every arm.

## Instruments (at the layer that pays: arena pages, not rows)

1. `PageStore::live_page_count()` — the store's net counter.
2. A **membership enumeration**: for every `(arena, owner)` in `live_arenas()`,
   `allocated_pages(arena).len()`, bucketed by owner id into *reaped-set*, *survivor*, *other*.
3. `reserved_page_count()`, and the same membership split over `extent_range(arena)` sizes.
4. `pending_len()` — pages parked in the pending-free log.
5. Wall-clock ms per sweep. **Secondary, load-sensitive, no prediction rests on it.**

`retained_pages` = instrument 2's reaped-set bucket after sweep 1 and both collectors.
The file length is NOT used: freed extents return to the free-space map and the file never
shrinks, so file length is a high-water mark, not retention.

## Guards — any failure prints `NOT A RESULT` on the row and the run exits 2

- G0 provenance: the build stamp is a real sha and not `+DIRTY`.
- G1 fixture fired: before sweep 1, enumerated pages = P·D exactly, and the survivor's depth is
  D (`chain`, `overwrite`) or 1 (`fanout`).
- G2 instruments agree at every census: `live_page_count` = enumeration, and
  `reserved_page_count` = the sum of live extent sizes. (A net count can hide offsetting errors.)
- G3 sweep 1 reaped exactly the D−1 expected ids; sweep 2 reaped exactly the survivor;
  `refused_reaps()` = 0.
- G4 after sweep 1 the survivor reads `Live` and every reaped id reads `Reaped`.
- G5 the *other* bucket is 0 at every census (no page is owned by a branch the fixture did not make).
- G6 `overwrite` only: every `cow_page` returned `copied = true` and `retire_previous = false`.
- A run with no parsable depth or arm refuses and exits 2.

## Predictions (exact integers; single-threaded, so exact equality, not a fit)

At P = 2, D ∈ {1, 10, 100, 1000}:

| quantity after sweep 1 | `chain` | `overwrite` | `fanout` |
|---|---|---|---|
| reaped | D−1 | D−1 | D−1 |
| retained_pages | **P·(D−1)** = 0, 18, 198, 1998 | **identical to `chain`** | **0** |
| retained reserved pages | 3·(D−1) = 0, 27, 297, 2997 | identical to `chain` | 0 |
| retained extents | 2·(D−1) | identical | 0 |
| pending | P·(D−1) | identical | 0 |
| survivor pages | P | P | P |
| extra `drain_pending` released | 0 | 0 | 0 |
| `collect_orphaned_extents` freed | 0 | 0 | 0 |

After sweep 2 (control a), every arm: enumerated pages 0, reserved 0, pending 0, live extents 0,
orphan collector frees 0.

**Slope of retained pages against D, per page written per level: 1 for `chain` and `overwrite`,
0 for `fanout`, 0 after the leaf goes.**

The pin's own share is P·(D−2), not P·(D−1): b(D−1)'s pages are held by the ordinary interval rule,
because its direct child is the leaf and the leaf is `Live`. The D16 pin is what holds b1 … b(D−2).

## What each other outcome would mean

- **`chain` = P at every D ≥ 2 (flat).** Only the leaf's direct parent is protected; the pin does
  not hold. In this fixture those pages are readable by the live leaf, so that is **D16's data loss
  back**, not a saving. Stop and report it as a correctness regression.
- **`chain` = 0.** Even the direct-child rule failed (D18 class). Correctness regression.
- **`chain` between P and P·(D−1), or not a multiple of P.** The pin holds to some depth and then
  breaks. `retained/P + 1` is that depth; report it. Also a correctness regression.
- **`chain` > P·(D−1)** with every guard green. Interior-owned pages exceed what the interiors
  wrote, so something on the reap path allocates. A new finding; the D16 cost line does not
  cover it.
- **retained reserved ≠ 3·(D−1) while retained pages match.** Extent accounting differs from the
  doubling rule (`next_extent_pages`). The page claim stands; the disk cost is the reserved number.
- **pending ≠ retained_pages.** Some retained pages are not on the pending log, so no drain can
  ever return them: an orphan leak with a different mechanism from the pin.
- **after-leaf > 0.** The cascade does not unwind: retention outlives the last descendant, which is
  a permanent leak and worse than D16's recorded cost. = P·(D−1) means the drain never retested;
  a fraction of it means the cascade stops partway up.
- **`fanout` > 0.** The fast path retains pages, so `chain`'s number cannot be attributed to the
  pin alone; subtract `fanout` at the same D before reading `chain`.
- **`overwrite` ≠ `chain`.** The rule can see that a descendant superseded a page. That would
  falsify my reading that it cannot (below). Lower `overwrite` = the leak is smaller than the
  retention.
- **orphan collector or extra drain frees > 0 after sweep 1.** The retention after sweep 1 was a
  transient that production's periodic collectors recover; the steady-state number is the one
  after them, and D16's cost is smaller than sweep 1 alone suggests.

## What `chain` cannot tell you — why `overwrite` exists

The brief's fixture writes FRESH pages. A fresh page b(k) wrote before forking b(k+1) is one the
leaf can read: its root is b(k)'s root at fork. **Keeping it is required, not leaked.** Any correct
reclaimer keeps it, including D16's rejected option 3, which never reaps the interiors at all. So
`chain` measures what the pin holds, not what it wastes.

In `overwrite`, every interior page has been replaced by a copy in its child before the child
forks on, and `cow_page` still stores whole pages (D102: "no chain is written yet"). No interior
page is the current version of anything the leaf reads, so everything `overwrite` retains is
garbage. The interval rule tests `[birth, free)` against a live child's fork epoch and has no input
that says a child superseded the page (READ: `retire_arenas_by_rule`, `live_child_in_epoch_range`),
hence the prediction that the two arms retain the same count.

If that holds, the "leak proportional to chain length" is real in `overwrite`. **INFERRED, not
tested here:** D16's options 2 and 3 would retain the same set, because all three keep the interval
rule and none of them gives it that missing input. The leak would then belong to the rule, not to
option 1.

## Secondary: time (no prediction; reported for FAN-QUEUE budgeting)

INFERRED from reading, and independently in `frontier/lane_wall21_reap_walk.md` §1.3: under a live
leaf, sweep 1 in `chain` and `overwrite` is expected to cost Θ(P·D³) CHILD-span scans. The
deepest-first order reaps b(D−1) first; each reap's drain retests the whole pending log; and each
retest of a page owned m levels above the leaf walks m spans. At D = 1000 that is about
1.7·10⁸·P span scans. `fanout` and sweep 2 are linear. No duration is predicted. If wall21's fix
lands first, the time changes and **every retention count must not**: a changed count under that
fix means the fix broke the pin.

## Not covered

- `LogBranchCatalog` (only the shipped `TableBranchCatalog` is run).
- Concurrency (D15), crash and reopen, and replicas.
- Catalog-side retention: the interior id slots `release_id` refuses while pinned, which wall21 F1
  says the cascade never releases. That is records, not arena pages.
- A real B+tree. `overwrite` models supersession at page granularity. A tree would also shadow
  root-to-leaf path pages at every level, adding garbage to `chain` that this fixture does not
  write.

## Run

```
cargo build --release --example d16_chain_retention
timeout 1800 target/release/examples/d16_chain_retention 1,10,100 2 fanout,chain,overwrite
timeout 14400 target/release/examples/d16_chain_retention 1000 2 fanout,chain,overwrite
```

Exit 0 = every guard held and every prediction matched. Exit 1 = guards held, and at least one
pre-registered prediction did not (a result, printed as `MISMATCH`). Exit 2 = `NOT A RESULT`.

---

## Amendment 1 — 2026-09-24, committed before the harness change it describes

Still quiet mode: nothing has been compiled or run, so no number has been seen.

### A1.1 Correction: `drain_pending()` is NOT a collector production runs

"Fixture" above says the harness runs "`drain_pending()` once more and `collect_orphaned_extents()`
once — the two collectors production also runs". **That is false for the drain.**
`git grep -n "\.drain_pending(" -- src/` at `9aa6968` finds callers only inside `reaper.rs`'s
`#[cfg(test)]` module. Production drains only inside each `reap` (`drain_pending_seeded`). Outside a
reap it runs just one collector, `collect_orphans_if_due`, from `lease_thread.rs` after the reap
lock is released. That is the `deferred` residue plus the full `collect_orphaned_extents` scan.

It matters for more than wording. An extra drain run BEFORE the census would clean up after a reap
that skipped its own drain (the D183 retraction's class), and the retention columns would then read
0 while the defect was live.

**New order, after each sweep:** `reap_expired` → `collect_orphaned_extents` (production's periodic
collector; equal to `collect_orphans_if_due`'s work when `deferred` is empty, which new guard G7
asserts) → G4 → **census (the reported numbers)** → `drain_pending()` as a **probe**. The probe is
predicted to release 0. A non-zero probe means a reap left reclaimable pages parked. The probe runs
after the census, so it cannot hide what it finds. The predicted integers do not change: in the
predicted world the probe releases nothing.

### A1.2 New guards

- **G7:** `TwoTierReaper::deferred_len() == 0` after each sweep's collector: no drain returned early
  with touched extents left unswept.
- **G8:** the slot probe (A1.4) could fork.
- **G0 is now run-level.** A dirty or unknown build stamp makes the whole run NOT A RESULT (exit 2),
  and every line's tag says `build=DIRTY` or `build=unknown`. The per-cell verdict from G1–G8 and the
  predictions is still printed. That is what lets a fire-check, which always builds dirty, be read.

### A1.3 D200 does not move any retained-page prediction

D200 (the cascade never releases a detached ancestor's id slot) is fixed on `wall21-reaped-subtree`
@ `17cbd4c`. It is not on main. The page predictions do not depend on it (READ, `git show 206af08
1107752`):
- `release_id` writes a FREE_ID key. Retention is decided by `live_child_in_epoch_range` over CHILD
  entries, which release does not touch, and a released slot keeps its record, so the drain's
  `get_raw(owner)` still answers.
- `extent_is_collectable` treats an owner as gone once its generation moved or it reads `Reaped`.
  Both held before any release.
- The fixture never forks after a reap until the slot probe (A1.4), which runs after every census.
  So no slot is recycled underneath a page count.

### A1.4 Secondary: an id-slot probe (D200 on the depth axis). This binary does not judge it

After sweep 2's census, the harness forks D branches off trunk and counts how many were handed a
slot id the fixture had used. `fork` takes a free slot before it mints a new id, so the count is the
number of slots released. It prints as a `slots` line. It is not in the MATCH/MISMATCH verdict,
because its expected value depends on the tree:

| tree | `chain`, `overwrite` | `fanout` |
|---|---|---|
| main lineage without D200 (`9aa6968`) | **1** at every D (only the leaf's own reap releases; D−1 interior slots leak) | **D** |
| `wall21-reaped-subtree` @ `17cbd4c` (D200) | **D** | **D** |

### A1.5 Cost note, reconciled with wall21

wall21's pre-registered counter covers the three per-reap liveness questions: Θ(D²) span scans for
the interior sweep, `3·D(D−1)/2`. That is READ from `17cbd4c:tests/wall21_reaped_chain_liveness.rs`.
The Θ(P·D³) figure above adds the drain's retests. Each reap retests the whole pending log, and
`lane_wall21_reap_walk.md` §1.3 gives the same Θ(P·D³). It is INFERRED in both lanes. No duration is
predicted.

### A1.6 Fire-checks: mutants injected INSIDE the measured path

Each mutant is a one-anchor source replacement in the engine, never in the harness. It is applied
by `bench/d16_chain/firecheck.py`, which refuses a dirty `src/` and any anchor that does not occur
exactly once, and it is restored from git after every build. Each anchor was checked with
`git grep -nF` at `9aa6968`: 1 hit each. Harness arguments: `1,3,10 2 fanout,chain,overwrite`.
D = 3 is the first depth at which the D16 pin, rather than the ordinary rule, holds a page.

Expected per-cell verdict (`M` = MISMATCH with exactly these keys; `N` = NOT A RESULT with exactly
these guard ids; `=` = MATCH):

| # | mutant (file, anchor) | what it removes | `fanout` | `chain` and `overwrite` |
|---|---|---|---|---|
| M0 | none, clean build | — | = | = (and exit 0) |
| M1 | `table_catalog.rs` `live_child_at`: the D16 arm's guard → `if false` | **the pin** (red arm) | = | D=1 `=`; D=3,10 **M** retained_pages, retained_reserved, retained_extents, pending (b(D−1)'s P pages only) |
| M2 | `reaper.rs` `reap`: `drain_pending_seeded(own_arenas)` dropped | **a reap's own drain** (control a; D183's class) | = | D=1 `=`; D=3,10 **M** after_leaf_pages, after_leaf_reserved, after_leaf_extents, after_leaf_pending, after_leaf_drain |
| M3 | `reaper.rs` `reap` fast path: `free_arena` dropped | **the fast path's free** (control b) | D=1 **M** after_leaf_pages, after_leaf_reserved, after_leaf_extents; D=3,10 **M** retained_pages, retained_reserved, retained_extents, after_leaf_pages, after_leaf_reserved, after_leaf_extents | every D **M** after_leaf_pages, after_leaf_reserved, after_leaf_extents |
| M4 | `arena.rs` `release_page`: `live_pages.fetch_sub(1)` dropped | the net counter's decrement (G2) | = | D=1 `=`; D=3,10 **N** G2 |
| M5 | `arena.rs` `cow_page`: the in-place test → `if true` | the copy (G6, G1) | = | `chain` = ; `overwrite` D=1 `=`, D=3,10 **N** G1, G6 |
| M6 | `reaper.rs` `reap_expired`: `candidates` → `.skip(1)` | one reap per sweep (G3, G4) | D=1 **N** G3; D=3,10 **N** G3, G4 | D=1 **N** G3; D=3,10 **N** G3, G4 |
| M7 | `reaper.rs` `drain_pending_seeded`: `guard.swept = true` dropped | the drain's disarm (G7) | **N** G7 | **N** G7 |

Plus A0 on the clean build: an unparsable argument (`1,x`) must print `NOT A RESULT` and exit 2.

A mutant whose cells differ from its row has not fired as registered. Report the difference; do not
re-register it. Guards not fire-checked here, stated as blind spots: G5 (a page owned by a branch the
fixture never made) and G8.

### A1.7 Run commands, superseding "Run" above

```
cargo build --release --example d16_chain_retention
timeout 1800 target/release/examples/d16_chain_retention 1,10,100 2 fanout,chain,overwrite
timeout 14400 target/release/examples/d16_chain_retention 1000 2 fanout,chain,overwrite
timeout 7200 python3 bench/d16_chain/firecheck.py
```

---

## Amendment 2 — 2026-09-24, after a fresh-context review; committed before the change it describes

Still nothing compiled or run. A read-only reviewer traced all 63 cells of A1.6 (7 mutants × 9 cells)
through source at `e7acf39` and found every registration correct. It also found no compile error
against the real signatures. Neither statement is a build. Its findings, each re-checked against
source before acting, change the following.

### A2.1 Two of my own claims, corrected where they were made

- **A1.1 named the wrong mechanism.** The ordering rule (census before the drain probe) is right,
  but the column it protects is **after_leaf**, not retained. During sweep 1 every parked page is
  pinned by the live leaf, so an early probe releases nothing there, and under a skipped-drain defect
  (M2) the sweep-1 cells still MATCH. What an early probe would hide is sweep 2: it would zero
  after_leaf_* under M2.
- **A1.2 overstated G7.** Every early return in `drain_pending_seeded` propagates out of `reap`
  (`reaper.rs`, the `?` on `drain_pending_seeded`). `reap_if_still_expired` then turns it into
  `Refused`, which G3 and G3b catch first. G7 therefore catches only residue in `deferred` by a route
  that does not error, such as a drain that never disarms (M7). M7 proves G7 reads `deferred`. It does
  not prove G7 catches the early-return case A1.2 named, which G3 masks.

### A2.2 Each half of a two-part guard gets its own id

So that a fire-check can prove WHICH half fired:

| id | condition | id | condition |
|---|---|---|---|
| G1 | P·D pages written | **G1b** | survivor depth is D (chain, overwrite) or 1 (fanout) |
| G2 | page counter = enumeration | **G2b** | reserved counter = sum of live extent sizes |
| G3 | each sweep reaped exactly its expected set | **G3b** | `refused_reaps() == 0` |
| G4 | every doomed id reads Reaped after sweep 1 | **G4b** | the survivor reads Live after sweep 1 |

G7 is also checked after the sweep-1 probe, so residue from that probe is labelled with its own
phase rather than with sweep 2's. Every A1.6 row keeps its meaning under the split: M4 fires G2
(pages), M5 fires G1 (written) and G6, M6 fires G3 and G4 (doomed).

### A2.3 Six more mutants, each injected inside the measured path

Anchors were checked with `git grep -nF` at `9aa6968`: one hit each in the named file. Arguments
are as in A1.6. Notation as in A1.6.

| # | mutant (file, anchor) | what it removes | `fanout` | `chain` | `overwrite` |
|---|---|---|---|---|---|
| M8 | `reaper.rs` `drain_pending_seeded`: `self.sweep_touched_extents(&guard.touched)?;` dropped | the drain's own extent sweep | = | D=1 `=`; D=3,10 **M** after_leaf_orphans | as chain |
| M9 | `arena.rs` `retire_arenas_by_rule`: park only if not a recorded shadow BASE (`shadow_base` values) | **negative control for chain == overwrite**: a rule that sees supersession | = | = | D=1 `=`; D=3,10 **M** retained_pages, retained_reserved, retained_extents, pending |
| M10 | `record.rs` `fork_child_parts`: `depth = parent_depth` | depth (G1b) | **N** G1b | **N** G1b | **N** G1b |
| M11 | `arena.rs` `free_arena`: `reserved_pages.fetch_sub` dropped | the reserved counter (G2b) | **N** G2b | **N** G2b | **N** G2b |
| M12 | `reaper.rs` `reap_if_still_expired`: `Ok(_) => Ok(ReapOutcome::Reaped)` becomes `refuse(..)` | a reap that happens but reports refused (G3b) | **N** G3, G3b | **N** G3, G3b | **N** G3, G3b |
| M13 | `table_catalog.rs` `release_id`: `if reusable {` → `if false && reusable {` | every id-slot release | = | = | = |

Notes, INFERRED from reading:
- **M8.** The drain releases every interior page in sweep 2 but leaves the emptied extents. The
  harness's collector, which runs before the census, frees them, so after_leaf_orphans = 2(D−1) and
  the census reads 0.
- **M9's exact retained counts.** Only the key set above is registered and checked. The counts are
  D=3: 2 pages, D=10: 10 pages, against 4 and 18. `MAX_CHAIN_DEPTH` = 8 stops the level-10 copy
  recording a base, and each release drops the released page's own `shadow_base` key, so under
  deepest-first reaping every other level is left parked.
- **M10.** Depth is read only by `expired_candidates`' sort, whose fork-epoch tiebreak is still
  deepest first in a chain, so the retention cells would MATCH if G1b did not fire.
- **M13.** No page changes. It is judged on the slot line (A2.4).

### A2.4 What the fire-check script judges, beyond the verdict cells

- **G0.** Every mutant run must print `build=DIRTY` on every verdict line and exit 2. Each baseline
  must print `build=clean` and exit 0. This fire-checks the run-level G0 and proves every mutant
  binary was built from the mutated tree.
- **The chain == overwrite comparison.** Expected per D: `eq` (a `compare` line saying equal),
  `differ {keys}`, or `absent` (no line, because a cell failed a guard).
  - M0, M1, M2, M3, M8, M13: `eq` at every D.
  - M4, M5: `eq` at D=1, `absent` at D=3,10.
  - M6, M7, M10, M11, M12: `absent` at every D.
  - M9: `eq` at D=1; `differ` {retained_pages, retained_reserved, retained_extents, pending} at D=3,10.
- **The slot line.** A baseline passes if it matches EITHER registered tree state from A1.4: main
  lineage (chain and overwrite 1, fanout D) or D200 (D everywhere). The script prints which one.
  M13 must read 0 in every cell.

### A2.5 Blind spots that remain, and why

No single-anchor mutant I found inside the path can fire these in this fixture:
- **the sweep-1 `extra_drain` and `orphans`.** After sweep 1, every chain page is pinned by the live
  leaf, and fanout's reaps free whole extents. So nothing is reclaimable-but-parked, and no extent is
  empty-and-ownerless.
- **`survivor_pages`.** It would need a page attributed to the wrong owner.
- **G4b.** Every path that reaps the survivor in sweep 1 goes through the deadline index and trips
  G3 first.
- **G5 and G8.**

### A2.6 Script safety

Fixes to what the review found:
- SIGTERM and SIGHUP now unwind through `finally`. Before this, `timeout` sent SIGTERM and left the
  mutant in `src/`.
- Output goes to a fresh `bench/d16_chain/firecheck/<head>-<utc>/`, and the script refuses if that
  directory exists. Before this, a re-run overwrote uncommitted raw files, or dirtied committed
  ones, which made `M0-baseline-after` fail by construction.
- Before every mutant, the file must equal its HEAD blob, and `src/` and `examples/` must be clean.
  Before this, cleanliness was checked once.
- cargo and the harness run in their own process group, and a timeout kills the whole group, so
  no orphaned `rustc` keeps writing to `target/`.
- The script refuses unless cwd is the repository top level, and it never creates `build.rs`.

Registered command: `timeout 14400 python3 bench/d16_chain/firecheck.py` (13 mutant rebuilds plus
two baselines).

---

## Amendment 3 — 2026-09-24, after a second fresh-context review; committed before the change

Still nothing compiled or run. The second reviewer traced all 54 cells of M8–M13 at `203e3f5` and
confirmed every A2.3 and A2.4 registration, including M9's alternating pattern (levels 9, 7, 5, 3, 1
parked at D=10). It confirmed that the G1b–G4b split changes no A1.6 row. It also confirmed that
every anchor occurs once and every replacement type-checks by reading. Changes:

### A3.1 Corrections to my own text

- **A2.4 overclaimed G0.** `build=DIRTY` proves the binary came from a dirty tree, not from THIS
  mutant. What identifies the mutant is that every mutant's registered pattern differs from the
  baseline's and from every other mutant's, plus the exact values in A3.2.
- **A2.5's reason for G4b was wrong.** The harness reports every failing guard, so a mutant that
  reaped the survivor in sweep 1 would print G3 AND G4b. G4b is unfired because no single anchor can
  make sweep 1 reap the survivor. Two independent filters exclude it: `expired_before`'s deadline
  span, then `is_expired_at`. It is not unfired because G3 masks it.
- **M9 proves less than a negative control usually does.** In every M9 cell where `compare` reads
  `differ`, overwrite's own cell already reads MISMATCH on the same four keys. So M9 shows the
  comparison can print `differ`. It does not show the comparison catches anything the cells miss.
  Stated here rather than claimed.

### A3.2 Exact values, now checked by the script (P = 2; INFERRED traces, now registered)

| mutant | cell | values |
|---|---|---|
| M1 | chain, overwrite at D=3 and D=10 | retained_pages 2, retained_reserved 3, retained_extents 2, pending 2 |
| M2 | chain, overwrite at D=3 | after_leaf_pages 4, _reserved 6, _extents 4, _pending 4, _drain 4 |
| M2 | chain, overwrite at D=10 | after_leaf_pages 18, _reserved 27, _extents 18, _pending 18, _drain 18 |
| M3 | fanout D=1 | after_leaf_pages 2, _reserved 3, _extents 2 |
| M3 | fanout D=3 | retained_pages 4, _reserved 6, _extents 4; after_leaf_pages 6, _reserved 9, _extents 6 |
| M3 | fanout D=10 | retained_pages 18, _reserved 27, _extents 18; after_leaf_pages 20, _reserved 30, _extents 20 |
| M3 | chain, overwrite at every D | after_leaf_pages 2, _reserved 3, _extents 2 |
| M8 | chain, overwrite at D=3, D=10 | after_leaf_orphans 4, 18 |
| M9 | overwrite D=3 | retained_pages 2, retained_reserved 3, retained_extents 2, pending 2 |
| M9 | overwrite D=10 | retained_pages 10, retained_reserved 15, retained_extents 10, pending 10 |

### A3.3 Harness

G7 is also checked after the sweep-2 probe. Every A1.6 and A2.3 guard-id set is unchanged: under M7
the extra check prints G7 again, and at baseline it reads 0.

### A3.4 Script

- **Restore without git.** The script writes back the bytes it read, with SIGTERM and SIGHUP
  ignored while it does, then checks the blob against HEAD. `git checkout --` needs the index lock,
  and a stale lock left by a killed build would strand the mutant.
- **HEAD is pinned at start.** It is re-checked before every build, and every verdict line's `sha=`
  must equal it.
- **Whole-tree cleanliness.** The start-up check covers every tracked file, not just `src/` and
  `examples/`, because the build flag reads every tracked file.
- **Which slot state is expected** is decided by the source: D200's
  `unreleased_reaped_candidates` present in `src/` means D200, and absent means main lineage. This
  replaces accepting either state.
- **The two baselines must be identical.** Their `measured`, `slots` and `verdict` lines are
  compared, not merely each checked against the registration.
- **Pinned environment.** Harness runs pin `D16_PERSIST=1`. `RUSTFLAGS` is recorded in each run's
  `ENV.txt`.
- **M5's replacement** becomes `if true || (owns_it && header.birth_epoch >= barrier) {` so that it
  leaves no unused variable and still builds under `-D warnings`. Its registered verdicts are
  unchanged.
- **Output and exit codes.** Build logs are saved next to the run's output. An interrupted run exits
  3, distinct from 1 (differed) and 2 (could not run).
