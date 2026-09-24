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
