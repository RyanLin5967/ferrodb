# W4 — the per-statement lock and `forget_reaped_branches`

Status: done, measured, committed. Evidence files in this directory are the raw runs; every number
below names the file it came from.

## What was actually wrong

`AgentRuntime` holds one `Mutex<State>`, taken by every statement that touches branch state.
`forget_reaped_branches` held it across work proportional to the number of **open sessions**, in
two places, not one:

| phase | holds the lock? | cost | how it was found |
|---|---|---|---|
| 1 — walk `state.workspaces` | yes, one acquisition | O(sessions), plus a `format!` heap allocation per entry | named in the brief |
| 2 — ask the catalog | **no** | O(candidates) catalog reads | already correct, and already documented as such |
| 3 — remove the dead ones | yes | **O(removed × sessions)** — `capture_is_protected` scanned `workspaces.values()` per branch | found while reading, not from the brief |

Phase 3 was the larger half and the first baseline could not see it: that fixture had nothing
reap-eligible, so phase 3 never ran. The corrected baseline is `statement-lock-BEFORE.txt` table 2.

**A correction to the brief's framing, which was otherwise right.** The brief said the cost under
the lock is O(open sessions) and not O(total branches). That is true of phase 1 and it is what the
comment at the top of the function says. It is *not* true of phase 3, whose cost is the product.

## The measurement

`examples/statement_lock_sweep.rs`. A prober thread takes the state lock every 50 µs
(`quarantine_reason` against an empty map — as close to a bare acquire/release as the public API
gets) and times each acquisition; a second thread fires one operation per period. Three arms share
that protocol, and two of them are controls:

- `idle` — nothing takes the lock. **Negative control.** Flat at ~300–700 ns across four decades of
  S, before and after, so a stall in the other arms is the lock rather than the instrument.
- `sweep` — the thing under test.
- `activ` — `run_activity()`, which walks every workspace under the lock by construction and which
  this work deliberately does not touch. **Positive control.** After the fix it still separates
  from `idle` by four orders of magnitude at S=10⁵ (31.7 ms vs 738 ns, `statement-lock-AFTER.txt`),
  which is what rules out "the fix merely stopped the instrument seeing anything".

Two design decisions in the harness that changed the answer:

- **Fire on a period, not back to back.** At a ~100 % duty cycle the prober records a queue behind
  many holds instead of the length of one, and the duty cycle then varies with S by itself — the
  comparison becomes a measurement of the harness.
- **Interleave the arms, do not block them.** A blocked chunk-size comparison produced an ordering
  that did not reproduce, because this machine runs an agent fleet and a noisy interval landed on
  one arm only. Round-robin fixed it. `forget-chunk-selection.txt` and
  `statement-lock-BEFORE-AFTER.txt` are both round-robin.

## Result

`statement-lock-BEFORE-AFTER.txt` — before and after binaries interleaved, 9 rounds each, S open
sessions with 64 reaped, one sweep. Largest wait suffered by an unrelated statement, nanoseconds:

| S | before (med) | after (med) | | before (max) | after (max) |
|---|---|---|---|---|---|
| 1 000 | 168 000 | 94 958 | 1.8× | 583 542 | 230 875 |
| 10 000 | 4 280 292 | 214 667 | 19.9× | 5 260 625 | 1 064 083 |
| 100 000 | 129 031 750 | 681 667 | **189×** | 187 449 125 | 12 279 375 |

The before column at 10⁵ is 111–187 ms across all nine rounds — tight, which is what a structural
cost looks like. **The after column's spread is not the lock:** this machine's idle control reached
13 ms during the same period with nothing holding the lock at all, so at 10⁵ the after numbers sit
at or below the floor this machine can resolve. The claim that survives the noise is the before
column, which is far above it.

One honest trade, visible in `statement-lock-AFTER.txt` table 1: under a *continuously* sweeping
regime the prober's **median** uncontended acquisition rises (42 ns → 55 µs at S=10⁵) because the
lock is now handed over ~98 times per sweep instead of once. The tail falls 22×. A latency bound is
a claim about the worst case, so this is the right side of the trade — and the continuous regime is
not the real one anyway: the lease thread sweeps once per interval.

## Options considered, and why the others were rejected

**Chosen: bound phase 1 with a cursor-chunked walk, and replace phase 3's scan with a maintained
reverse index.** Phase 1 takes `FORGET_CHUNK` (1024) entries per acquisition behind a `BTreeMap`
cursor; phase 3 reads `State::txn_refs`, a count maintained by the only two doors into
`workspaces`. Neither adds a mechanism a caller has to know about.

- **Snapshot the key set under the lock and do the work outside** (named in the brief). Adopted in
  part — using `ws.name` instead of `format!("b_{id}")` removes one heap allocation per workspace
  from under the lock — but rejected as the whole answer: the walk is still O(sessions) in a single
  acquisition, so at 10⁵ it is still one unbounded hold. It lowers the constant; it does not bound
  anything.
- **An epoch/generation check so the survivors need no second pass** (named in the brief). Rejected
  on the call sites: both production callers (`lease_thread.rs:314` and `:421`) only call the sweep
  *after* a reap returned a non-empty list, so a "nothing has changed since last time" fast path
  never fires where the cost is. It also does nothing to bound the walk in the case that does run.
- **An O(reaped) notification inbox** — the reaper pushes reaped ids, the sweep drains them.
  Rejected: `simulate.rs:410` calls the sweep as a general GC pass with no reaped list to hand, so a
  full reconciliation has to remain regardless. The inbox would be a second mechanism *plus* a new
  invariant — every door that reaps must notify — whose silent rot reintroduces the unbounded
  growth this function exists to prevent, with no backstop. More parts, same walk.
- **`RwLock<State>`.** Phase 1 only reads, so readers would stop blocking each other. Rejected:
  writers still block, and writing statements are the ones that matter; the lock is acquired in
  ~28 places and converting them is a wide, risky refactor well outside this change, for a partial
  win.
- **A separate mutex for `workspaces`.** Rejected on an explicit warning already in the repo:
  `integration_runtime_concurrency.rs` records that the escrow pool's no-over-commit property is
  protected by this mutex's *breadth*, not by the arithmetic — "if that mutex is ever split or
  narrowed, this test is what should catch it". That trades a latency problem for a correctness
  one.
- **Bound phase 3's scan instead of indexing it.** Rejected: `capture_is_protected` answers yes/no
  over all live workspaces, and there is no early exit that does not change the answer. An index is
  the only way to make it sublinear.

### Why `FORGET_CHUNK` is 1024 and not smaller

`forget-chunk-selection.txt`, round-robin over 9 rounds at S=10⁵, worst stall in µs:

| chunk | median | worst |
|---|---|---|
| 64 | 757 | 17 644 |
| 256 | 280 | 2 908 |
| 1024 | 462 | **574** |
| 4096 | 1 080 | 1 281 |

The cost is **not monotone**, which is the opposite of the obvious guess: a smaller chunk holds the
lock for less time per acquisition but takes it far more often, and every acquisition is another
chance to be descheduled while holding it. 256 wins the median and loses the tail by 5×; 1024 stayed
inside 419–574 µs on every single round. Chosen on the tail, because a latency bound is a claim
about the worst case.

## A data-loss race closed on the way

Phase 2 runs with the lock **released**, and the catalog recycles a reaped branch's id *slot*
(`release_id` returns it, `fork` pops it) while both `workspaces` and `names` are keyed by the slot
alone. So a session forking into slot 5 inside that window took the same map key and the same `b_5`
name as the branch that had just died there, and phase 3 removed the **live agent's** workspace,
released its escrow and unbound its name — on the strength of an answer the catalog had given about
a different generation.

This predates the change; chunking would have widened it. Phase 3 now re-reads the whole `BranchId`,
generation included, before removing anything. `tests/w4_sweep_slot_recycle.rs` drives the window
deterministically through a catalog seam (`BranchCatalog::get` is called from exactly one place in
the sweep) rather than forking in a loop and hoping — and with the re-validation deleted it fails,
naming the slot.

## Two things measured and deliberately NOT fixed

1. **`run_activity` is the same wall, still standing.** It walks every workspace under the same lock
   and stalls statements for 22–31 ms at S=10⁵ (`statement-lock-AFTER.txt`, `activ` arm). It is left
   alone for two reasons: it is a diagnostic system view rather than a hot path, and it is this
   measurement's positive control — fixing it would remove the evidence that the prober can still
   see an O(sessions) hold at all. If it is fixed later, a replacement control has to be built in
   the same commit or the harness stops proving anything.
2. **Workspace lookups are generation-blind.** `blind_writes` — and every other
   `state.workspaces.get(&branch.id)` — is keyed by the id slot alone, so a caller holding a stale
   `BranchId` is answered about the slot's *new* occupant instead of being refused. The catalog
   refuses a stale generation (`BranchError::Reaped`); the runtime's workspace map has no such
   check. Reproduction is recorded in `tests/w4_sweep_slot_recycle.rs` at the point that measured
   it. Closing it means a generation check at every workspace lookup — a different change from this
   one, and a wider one.
