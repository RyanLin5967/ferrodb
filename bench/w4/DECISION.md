# W4 — the per-statement lock and `forget_reaped_branches`

Status: done, measured, committed. Evidence files in this directory are the raw runs; every number
below names the file it came from.

**Read addenda 5 AND 6 before acting on this record; there are six, and each of the last two moves
something every addendum before it called open.** Addendum 6 (D158 item 1) closes the
generation-blind `workspaces` map that FOUR separate "still open" lists in this file leave standing
— and it was not merely open, it was a **live cross-agent read and a live cross-agent delete over
pgwire**, not the latent hazard those lists describe. Everything in the sections above is about
`AgentRuntime`'s `Mutex<State>` and holds. It is not the only lock the sweep runs under, and the
brief's 39.3 s headline is about the other one — the pgwire per-statement mutex that `scan_once`
held across the *entire* call,
phase 2 included. Addendum 1 found it and said the work here did not touch it; addendum 1 and
addendum 3 both left it on their "still open" lists.

**Addendum 5 (D98) closes it, with a control arm and a before/after measured on this machine**
(`bench/d98_outer_runtime_lock.txt`): reaps per acquisition of that mutex went from rising 100×
across 100× the branch count to flat, and a statement at 10⁵ branches went from waiting the whole
51.6 s sweep to a 161.8 ms p99. Two claims elsewhere in this file are corrected there — the
reconciliation is no longer inside the statement lock, and the unpinnable 39.3 s citation is now
unnecessary rather than merely labelled. Sections above are kept as written; do not quote a "still
open" list from addendum 1 or 3 without reading addendum 5 first.

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
2. ⛔ **SUPERSEDED BY ADDENDUM 6 — DO NOT ACT ON THIS ITEM.** It was right that the lookups were
   generation-blind and wrong to file it as something measured and left: it was reachable over
   pgwire and it crossed agents. As written —
   *"**Workspace lookups are generation-blind.** `blind_writes` — and every other
   `state.workspaces.get(&branch.id)` — is keyed by the id slot alone, so a caller holding a stale
   `BranchId` is answered about the slot's new occupant instead of being refused. The catalog
   refuses a stale generation (`BranchError::Reaped`); the runtime's workspace map has no such
   check. Reproduction is recorded in `tests/w4_sweep_slot_recycle.rs` at the point that measured
   it. Closing it means a generation check at every workspace lookup — a different change from this
   one, and a wider one."*
   The map is now keyed by the whole `BranchId`, which is neither "a generation check at every
   lookup" nor wider — it is one key change that deletes the need for the check. See addendum 6.

## Addendum — the OTHER lock, found after this record said "done"

The record above is about `AgentRuntime`'s `Mutex<State>`, and everything it claims about that lock
holds. It is not the only lock the sweep runs under, and the brief's headline number is about the
other one.

`scan_once` (`src/branch/lease_thread.rs`) runs its **entire** body inside `with_lock(lock, ..)`.
That lock is `ServerContext::catalog()` — the pgwire server's per-statement mutex (the
`RuntimeLock` impl in the same file), or `CatalogLock` for the CLI. So chunking `State` changes
nothing about it: the outer lock is taken once, before the sweep starts, and released once, after it
finishes. Between those two points every statement in the database is blocked, for the whole sweep,
including **phase 2** — the O(open sessions) run of catalog reads that `State` chunking deliberately
does not cover because phase 2 holds no `State` lock at all.

That is what S15's 39.3 s at 10⁶ open sessions is made of (`bench/runtime_at_1e6.txt`, W4), and it
is the number the brief asked about: *"39.3 s HOLDING the server's per-statement lock"*. The
chunk-and-index change fixes the 129 ms → 0.68 ms `State` stall and leaves that untouched.

> **⚠ That citation does not resolve — see "Corrections to this record's own earlier claims" at the
> end.** `bench/runtime_at_1e6.txt` is on branch `S15-runtime-at-1e6` and is not in this worktree,
> the number was not reproduced here, and three different versions of the row are in circulation.
> The paragraph is left as written; the evidence this section actually rests on is
> `statement-lock-FASTPATH.txt`, in this directory.

### What was done about it

`scan_once` stops re-deriving a list it was already handed. `reap_expired` returns exactly the
branches it reaped; the sweep then walked every open session and asked the catalog about each one to
rediscover them. The successful tick now calls `AgentRuntime::forget_branches(&reaped)`, which is
O(branches actually reaped) and touches the catalog `reaped.len()` times instead of
`open_sessions` times. It is chunked by the same `FORGET_CHUNK`, because a simulation can expire
thousands of candidates at once and an O(that) hold is the same wall through the other door.

**This is not the "notification inbox" rejected above.** That option was a stateful queue the reaper
pushes into, carrying a new invariant — every door that reaps must notify — whose silent rot
reintroduces unbounded growth with no backstop. This is a stateless argument on a call that already
had the list in hand, and the reconciliation is kept and still runs:

- `simulate.rs` calls `forget_reaped_branches()` as a general GC pass, unchanged.
- `LeaseThread::start` calls it after a crash resume, unchanged.
- `scan_once`'s **error** arm now calls it, which is new and is the point. `reap_expired`
  accumulates reaped ids and then discards that vector if a later branch fails (`reaper.rs`, the
  `Err(e) => return Err(e)` arm; likewise if `sweep_empty_extents` fails after a clean loop). Those
  branches are gone from the catalog and nothing will ever name them again. Before this, a failed
  scan swept anyway and happened to catch them; a purely list-driven design would have leaked
  exactly them. So the reconciliation runs on precisely the path where the list is known to be
  incomplete.

`tests/w4_forget_branches.rs` pins all three properties: the fast path drops what it is handed and
is idempotent; it refuses a branch the catalog still holds, so one wrong id cannot delete a working
agent's session; and a branch reaped but never reported is still found by the reconciliation. The
third is the leak guard. Verified by mutation rather than by being green — with `forget_one_branch`
stubbed to return `false`, two of the three fail; the third correctly does not, because it asserts
that *nothing* is forgotten and a no-op satisfies that.

### Result

`statement-lock-FASTPATH.txt`, same harness and same prober as every other file here, with the two
arms measured against one fixture: `recon` is `forget_reaped_branches()` (the backstop, O(sessions))
and `fast` is `forget_branches(&reaped)` (what a successful tick now costs, O(reaped)).

Largest wait an unrelated statement suffered (`stall`) and the sweep's own wall time (`wall`),
median of 5 fixtures, nanoseconds. **`wall` is the column that matters for the server's statement
lock**, because `scan_once` holds that lock for the whole call — `stall` is only the `State` mutex.

**⚠ SUPERSEDED TABLE — kept so the record shows what it said. Its `stall` column was produced by
the uncorrected instrument (see 14d796c); do not quote it. The re-measured table follows.**

| S | arm | stall_med | wall_med |
|---|---|---|---|
| 1 000 | recon | 104 875 | 269 208 |
| 1 000 | **fast** | **833** | **76 417** |
| 10 000 | recon | 716 583 | 4 054 250 |
| 10 000 | **fast** | **118 875** | **257 708** |
| 100 000 | recon | 530 125 | 24 493 958 |
| 100 000 | **fast** | **56 542** | **135 458** |

#### Re-measured with the corrected instrument

`statement-lock-FASTPATH.txt` was re-run at the merged tip. Two probe spacings, because one cannot
answer both questions: **50 µs** is what every earlier artifact here used, so those rows compare
with BEFORE/AFTER; **5 µs** is tight enough to resolve the fast arm, whose sweep is ~100 µs and can
fall between two 50 µs probes. Both runs are kept and the WORSE of the two is quoted, because on a
loaded machine they disagree by ~30 % and picking the better one would hide that. `wall_med`,
nanoseconds:

| S | recon (A / B) | fast (A / B) |
|---|---|---|
| 1 000 | 233 708 / 282 000 | 77 042 / 80 750 |
| 10 000 | 2 032 792 / 2 279 042 | 115 000 / 113 375 |
| 100 000 | 23 075 667 / 23 579 666 | 131 625 / 174 709 |

The shape is the result, not the ratio. `recon`'s wall rises **99x (A) / 84x (B)** across 100x S —
the O(open sessions) term, measured twice. `fast`'s rises **1.7x / 2.2x**, which is no trend: it is
O(gone), and `gone` is fixed at 64. At S=10⁵ that is **175x (A) / 135x (B)** less time holding the
pgwire statement lock, and unlike `recon` it does not worsen as more agents connect.

**The conclusion is unchanged from the superseded table, and that is the point of re-running rather
than annotating.** The instrument defect touched `stall` only — `wall` is timed around the sweep
call itself — so the 91x/181x quoted above and the 99x/175x measured here are the same result with
and without the fix. The `stall` column really did move: `recon` at 10⁵ went 2 566 µs → 1 002 µs
max, so the old maxima were carrying warm-up noise exactly as 14d796c said.

**The blind cell is a RESULT, not a limitation of the harness.** Run A reports 1 blind fixture at
S=1 000 `fast` and exits non-zero because of it. Read that as the finding it is: at 50 µs spacing
the prober cannot catch the fast arm's lock hold, *because that hold is around 100 µs and fits
between two probes*. The harness saying "too short to see" is the claim this lane is making. A
reader who gets `--` and a non-zero exit learns more than one handed a plausible number — which is
exactly what the uncorrected instrument would have produced here, by crediting the sweep with
warm-up it did not cause. Run B at 5 µs resolves the cell (0 blind, exit 0), and the two together
say the hold is real but under ~100 µs, which no single spacing states on its own.

Run B's idle control (396–455 ns) is comparable to run A's, and that check is what licenses the
tighter probe: a 10× busier prober is itself contention, so a spacing that bought sensitivity by
inflating the idle arm would have made the instrument into the thing it measures. The idle arm
holding flat is the evidence that it did not.

### What this still does not fix

The reconciliation's own wall time is unchanged — it is still O(open sessions) catalog reads, and on
the error path it still runs inside the server's statement lock. That path is rare by construction
(it needs `reap_expired` to fail), but it is not free, and a database that is failing to reap is
exactly one that may then also stall. Bounding it properly means either moving the sweep outside
`with_lock` — which opens a window where a statement can touch a workspace whose branch is already
reaped — or teaching the reaper to report what it freed before it failed. Neither was measured here.

## Addendum 2 — closing two holes the review found in the index itself

A fresh-context review of the diff produced six findings. One was wrong (it reported
`tests/w4_sweep_slot_recycle.rs` as not compiling, on the strength of a trait method `add_arena`
that does not exist in `BranchCatalog` — the reviewer said it had not run cargo, and the test
compiles and passes). Four were real. Two of those are the ones below; they are in the index this
record introduced, so they are its debt to pay.

**1. `insert_workspace` stranded the displaced workspace's capture.** A recycled id slot makes
`BTreeMap::insert` land on an occupied key and hand back the workspace it displaced. That
workspace's branch is dead by construction — nothing else could be in its slot — and the code
dropped its `txn_refs` but never its `state.captures` entry, because `captures.remove` is reached
only through `forget_captures_unless_published` and this path did not call it. One permanently
unreferenced capture per recycled slot: the unbounded growth `forget_reaped_branches` exists to
prevent, arriving through a door that is not the sweep. It now calls
`forget_captures_unless_published`, so the *published* rule applies here exactly as it does on the
reap path — asserted in both directions by
`a_recycled_slot_does_not_strand_the_displaced_workspaces_capture`.

**2. The claim that the suite checks the index was overstated, and is now made true.** This record
and the `txn_refs` doc comment both said the `debug_assert` in `capture_is_protected` re-derives the
answer on every call, "so the whole test suite is a differential test of this index". It is not.
`capture_is_protected` has exactly one non-test caller — `forget_captures_unless_published` — which
runs on the abandon and reap arms and nowhere else; a fork or a published merge never reaches it. So
a desync introduced when a workspace was **inserted** stayed invisible until some later abandon
happened to ask about that particular txn.

That mattered beyond this file: the hard constraint being handed to D27 — that `workspaces` has
exactly two doors because they maintain this index, and a rewrite calling `BTreeMap::insert`
directly desyncs it into the F6 data loss — was resting on a guard that would not have caught it.

`State::audit_txn_refs` now re-derives the **whole** index by brute force and compares, in debug
builds only, called from both doors. Every fork and every seal in every debug test run now checks
it, which is what the constraint needs to be real rather than advice. `cargo test --lib` is
1279 passed / 0 failed with it live.

Both detectors are proven to fire rather than assumed to:
`the_door_audit_fires_on_a_desynchronised_index` desyncs the index **upward** (a reference to a txn
no workspace holds) and the audit catches it at a door. The downward direction is caught earlier and
more loudly by `drop_txn_refs`, which refuses to decrement below zero — discovered by that test's
first draft tripping it instead, which is recorded in the test rather than tidied away.

### Corrections to this record's own earlier claims

- "Two things measured and deliberately NOT fixed", item 2, said workspace lookups are
  generation-blind and left it. The **escrow half of that is now fixed** (`82b4eb0`): `Pool::claimed`
  and `Pool::spent` are keyed by the whole `BranchId`. ⛔ The sentence that followed — *"What
  remains open is the `workspaces` map itself — `blind_writes` and every other
  `state.workspaces.get(&branch.id)` still answers a stale `BranchId` about the slot's new
  occupant"* — **is false from addendum 6 onward**: the map is keyed by the whole `BranchId` too.
- The 189x headline earlier in this record is a claim about `Mutex<State>` and nothing else. The
  addendum above is right that the server's per-statement lock is the outer `RuntimeLock`, held
  across the entire sweep including phase 2, and that chunking `State` does not touch it. The
  harness measures `AgentRuntime` directly with no `RuntimeLock` at all, so it was never measuring
  that lock. Both numbers are real; neither is the other.

- **The "39.3 s at 10⁶ open sessions" citation was unpinnable and is now labelled as such.** The
  addendum above, and two code comments that quoted it, cited `bench/runtime_at_1e6.txt`. That file
  is on branch `S15-runtime-at-1e6` (commit `0ac1931`) and **does not exist in this worktree**, so
  every reader of those comments was sent to a path they could not open. Worse, three different
  versions of that row are in circulation — the ledger's "at 10⁶ branches", which team-lead's brief
  says is wrong; S15's own "at 10⁶ open sessions"; and the brief's bare "39.3 s" — and **none of
  them was measured here.** At roughly 1 KB of `Workspace` per session, 10⁶ *open sessions* is a
  large amount of resident state to have actually held, which is a reason to doubt the row rather
  than to repeat it.

  The comments in `lease_thread.rs` and `runtime.rs::forget_branches` now rest on
  `statement-lock-FASTPATH.txt`, which is in this directory and which I ran: the reconciliation's
  wall time rises 91× across 100× open sessions (269 µs → 24.5 ms at 10⁵) while the fast path shows
  no trend. S15's figure is still named, with its branch and commit, as **motivation and not
  evidence**. Nothing in this work's argument depended on it: the direction of the fix follows from
  the wall column's slope, which was measured on this machine in this tree.

## Addendum 3 — the last three review findings, and a number this record had wrong

Closing out the same fresh-context review. Six findings: one wrong, five real. Addendum 2 took two
of them; these are the rest.

**The instrument was crediting the sweep with its own warm-up, and it moved a number in this
record.** `oneshot` took the prober's maximum over the whole thread lifetime — which includes the
20 ms warm-up before the sweep starts and the tail after it ends, intervals where nothing holds the
lock at all. This record already noted that the idle control on this machine reaches tens of
milliseconds with the fleet running, the same magnitude as the entire AFTER column, so the
contamination was not hypothetical; the record simply did not connect the two. Samples are now
filtered to those that **overlap** the sweep, and a run where none overlap **refuses** rather than
reporting 0 — a sweep shorter than the 50 µs probe interval has not been measured as fast, it has
not been measured.

Re-measured at S=10⁵ (`statement-lock-AFTER-corrected-window.txt`): the median is unchanged and the
tail falls 6× — 682 µs → 608 µs median, 12 279 µs → 2 022 µs max. So the AFTER column's outliers
were noise, exactly as the review said, and the "at or below the floor this machine can resolve"
hedge earlier in this record was covering for a fixable instrument rather than a real limit.

**Corrected headline, S=10⁵, 64 reaped, worse of two runs:**

| | before | after | factor |
|---|---|---|---|
| median stall | 129.0 ms | 0.61 ms | **212×** |
| max stall | 187.4 ms | 2.02 ms | **93×** |

The BEFORE column is unaffected — 111–187 ms across nine rounds dwarfs a ~20 ms noise floor. Two
independent AFTER runs are kept in the artifact because they disagree by ~2× on a loaded machine;
the worse one is what is quoted here. The FASTPATH table in addendum 1 was produced by the same
uncorrected instrument, so its `stall` maxima are inflated the same way. Its `wall` column — which
that addendum correctly identifies as the one that matters for the server's statement lock — is
unaffected, and so is its conclusion.

**The skip path stranded the dead branch's escrow.** When phase 3 finds the slot recycled it
refuses to remove the live branch's workspace, and it was skipping `escrow.release` along with
everything else. The dead branch's claim then sat in the pool with nothing alive that could ever
give it back — the resource-stranding the ABANDON arm of `seal` exists to prevent, and the mirror
image of the bug the re-validation fixed. It releases now, and it is only correct to do so *because*
the ledger is keyed by the whole `BranchId` (`82b4eb0`): the release names the dead generation and
leaves the reborn branch's own claims, a different key, alone. `w4_sweep_slot_recycle` now claims 12
of a 20-unit pool for the doomed branch and 3 for the racer inside the window, and asserts 17
unclaimed afterwards; with the release deleted it reports 5, naming the stranded units.

**"Nothing is missed permanently" was stronger than the call sites support** — the reconciliation
runs on `scan_once`'s error arm or when a simulation starts, not on a timer. Reworded in place.

**The finding that was wrong**, recorded so it is not re-raised: the review reported
`tests/w4_sweep_slot_recycle.rs` as failing to compile for want of a `BranchCatalog::add_arena`
implementation. There is no such method on the trait, and the test compiles and passes. The reviewer
said it had not run cargo. Checking beat believing it, which is the same rule this record applies to
its own numbers.

### Still open after all of this

- `run_activity` remains an O(open sessions) hold on the same lock and remains this measurement's
  positive control. Unchanged from the note above: fixing it needs a replacement control in the same
  commit.
- ⛔ *"`workspaces` lookups are still generation-blind. The escrow half is fixed; the map is not."*
  **CLOSED by addendum 6** — the map is keyed by the whole `BranchId`.
- The reconciliation's own wall time is still O(open sessions) on the error path, inside the
  server's statement lock. Addendum 1's closing note stands.

- **The oneshot instrument defect (14d796c) reached this record's own addendum, and the fix to the
  harness needed a fix of its own.** The addendum's first result table is marked superseded above
  and re-measured beneath it; the conclusion did not move, because the defect touched `stall` and
  the claim rests on `wall`.

  The refusal 14d796c added — no probe overlapped the sweep, so report nothing rather than zero —
  was right and fired on its first real use. But it was an `assert!`, so **one unobservable cell
  killed the whole run**: the re-measurement lost the S=10⁵ row, which is the row the headline
  quotes. It is now refused per fixture instead: the cell prints `--`, the run continues, and the
  process exits non-zero if any fixture was blind. No number is invented for a sweep nobody saw,
  and the cells that were measured survive. The probe spacing is a parameter for the same reason —
  a sweep too short to see at 50 µs is a fact about the instrument, and the answer is to change the
  instrument and say so, not to quote the blind cell as fast.

## Addendum 4 — re-measured on the merged base, and the number moved

This record's headline was measured against a base 25 commits behind `agent-isolation`, and D27 had
since replaced `Workspace`'s data fields with a persistent ordered map — the structure
`forget_reaped_branches` walks. Caught by team-lead with `git merge-base --is-ancestor`, which is
the check that works: the log *looked* merged, because `5c8583a` reads "Merge commit ... into
W4-review-followups", but that merged this lane's own sub-branch rather than the base.

**The post-merge pair supersedes every earlier number in this file.** Both arms built from the
merged base, so all 25 upstream commits are in both and only this lane's change differs. `gone = 64`,
so phase 3 is reachable — a `gone = 0` fixture measures phase 1 only.

| S | before (med) | after (med) | factor |
|---|---|---|---|
| 10 000 | 2.85 ms | 0.152 ms | 18.8× |
| 100 000 | **96.3 ms** | **0.347 ms** | **278×** |

The distributions do not overlap at either S: at 10⁵ the worst `after` (0.425 ms) is below the best
`before` (79.3 ms). Controls on the same binary: negative (idle) flat at 450 ns, positive
(`run_activity`) still 20.9 ms — separating from idle by ~46 000×, so the instrument is not blind.

**Both arms moved, and the pre-merge pair (129.0 ms → 0.68 ms, 189×) is superseded rather than
corrected.** The `before` arm got *faster* under D27 (129 → 96 ms) and the `after` arm got faster
too (681 → 347 µs). A number measured against a base that no longer exists is not a number about
the code being shipped.

### What the merge broke, and what held

Nothing in the index moved, and that was a prediction rather than luck: `txn_refs_of` reads only
`ws.txn` and `ws.inherited`, neither of which D27 touches, and `State::workspaces` is still a
`BTreeMap`. The two doors survived — the only raw `workspaces.insert`/`remove` in the file are still
inside them, so D27's rewritten `begin_session_as` still maintains the index. That was the specific
hazard and it was checked, not assumed.

What did break was this lane's *tests*: the unit fixture built a `Workspace` by hand with the old
field types, and D33 added `BranchCatalog::add_arena`, which the test catalog decorator did not
implement.

**That second one inverts an earlier ruling in this record.** Addendum 2 records a review finding —
"`w4_sweep_slot_recycle.rs` does not compile, missing `add_arena`" — as WRONG, and it was: no such
method existed at this lane's base. It was right about the tree the lane was landing on and wrong
about the tree the lane was on. A false positive and a true positive are the same text here; only
the base differs, which is the same lesson as the headline.

**And it failed silently.** `w4_sweep_slot_recycle` printed no `test result` line at all, because a
target that does not compile prints none — so a loop grepping for `^test result` skipped it, and the
absence read as a pass. Zero collected is not a pass; the blank had to be chased rather than
scrolled past.

## Addendum 5 — D98: the outer lock this record twice called open, closed and measured

Addendum 1 identified the OUTER lock and said the chunk-and-index work did not touch it. Addendum
1's closing section and addendum 3's "still open" list both left it open. This closes it, and the
number addendum 1 could not pin — it cited `bench/runtime_at_1e6.txt`, which is not in this
worktree — is now measured here instead of quoted.

**Evidence: `bench/d98_outer_runtime_lock.txt`.** Harness `examples/outer_runtime_lock.rs`, driver
`bench/d98_run.sh`, before/after as two builds of the identical harness differing only in `src/`,
run A B B A inside the fleet measure lock (taken per round, `load_at_acquire` 8 and 12).

### What was wrong

`scan_once` held ONE acquisition of the per-statement mutex across the clock read, the candidate
query, every reap on the tick and the reconciliation. So a statement waited for the whole sweep,
and a sweep is O(branches that expired) — which in an agent fleet is O(agents).

### The result, as counts first

Counts are integers and fleet load cannot move them; the durations below are upper bounds taken at
load 8-12 on a box that runs a dozen build agents. `reap/acq` is branches reaped per acquisition of
the statement mutex.

| N | K | reap/acq before | reap/acq after | statements completed during sweep, before → after |
|---|---|---|---|---|
| 1 000 | 10 | 5.00 | 0.91 | 31 → 8 077 |
| 10 000 | 100 | 50.00 | 0.99 | 45 → 98 106 |
| 100 000 | 1 000 | **500.00** | **1.00** | **7 → 73 139** |

**The shape, which is the part that transfers.** Across 100× the branch count with K rising in
proportion (the agent workload), `reap/acq` before rises **100.0×** — 5.00 → 50.00 → 500.00 — and
after is flat at **1.1×** (0.91 → 0.99 → 1.00). With K held FIXED at 64 it is flat both before and
after (32.00 at every N), which is the control that says the cost is a function of *what expired*
and not of *total branches*: the deadline index already handles total branches, and D2 is why.

### The result, as durations (upper bounds, load 8-12)

Worst `after` against best `before`, so the factor is the smallest the data supports:

| N | K | p99 before | p99 after | factor | sweep wall before → after |
|---|---|---|---|---|---|
| 1 000 | 64 | 3 857 ms | 67.2 ms | 57× | 3.86 s → 3.65 s |
| 10 000 | 100 | 6 357 ms | 102.1 ms | 62× | 6.36 s → 8.63 s |
| 100 000 | 1 000 | **44 562 ms** | **161.8 ms** | **275×** | 44.5 s → 73.9 s |

At N=10⁵ the median statement waited **51.6 s** in one before-run, against a sweep wall of 51.59 s
— i.e. the entire sweep, which is the signature of the defect rather than a coincidence.

**The cost is real and is in the table: the sweep itself is up to ~1.6× slower** (44.5 s → 73.9 s
at 10⁵), because it now yields the lock a thousand times and re-contends with clients that are
actually running. For a background reclamation task against a 15-minute lease and a 30-second scan
interval that is the right trade, but it is a trade and not a free win.

### THE CONTROL, which is what licenses any of this

`free` runs the identical fixture, the identical K, and the identical reaps under a PRIVATE mutex
nobody else holds. If 10⁵ branches were slow because of residency rather than the lock, it would
degrade too. It does not: its p99 is 20.9 µs at N=10⁵/K=1000 against the `idle` floor's 40.1 µs —
**indistinguishable from doing no sweep at all**, while reaping all 1 000 branches. The before
arm's 94× slope across N is therefore the lock, not the data.

### Two bounds, not one — and finding this needed the counters

Chunking the reap loop bounds how long the sweep HOLDS the lock. It did **not** bound how long a
statement WAITS for it. `std::sync::Mutex` is unfair: the sweep unlocked and relocked in a tight
loop and was re-granted before any waiter was scheduled. Measured at N=500, K=64: `reap/acq` was a
correctly-bounded 3.76 **while client p99 was still 3.78 s against a 3.79 s sweep** — the whole
sweep. A correct-looking bound that buys nothing is worse than none, because it reads as done.

`REAP_YIELD` (1 ms between chunks) is the second bound. `yield_now()` would not do: it is a hint
the scheduler may decline while this thread is still runnable, which is exactly the losing case.

**This was found because the harness reports counts alongside durations.** The count said bounded,
the latency said not bounded, and the disagreement was the finding. A latency-only harness would
have reported the first attempt as a success.

### REAP_CHUNK is 1, and the argument for 4 was wrong

Its doc claimed a chunk of 1 would make the sweep "as slow as the queue is long". Measured, same
fixture, only the constant changed:

| REAP_CHUNK | client p99 | statements done during sweep | sweep wall |
|---|---|---|---|
| 4 | 366 ms | 4 080 | 5.03 s |
| 1 | **66.6 ms** | **16 288** | **3.66 s** |

Better on every column including the one the argument was protecting: with `REAP_YIELD` the
clients drain in microseconds instead of fighting the sweep, so the sweep finished *sooner* at one
reap per acquisition. The constant's doc now carries the refuted argument and this table.

### Corrections to this record's earlier claims

- Addendum 1, "What this still does not fix", said the reconciliation "on the error path still runs
  inside the server's statement lock". **It no longer does.** Its wall time is unchanged and still
  O(open sessions); what changed is that it is no longer inside that lock. Addendum 1 worried that
  moving work out "opens a window where a statement can touch a workspace whose branch is already
  reaped" — that window was already open, because `forget_reaped_branches` has released its own
  state lock every `FORGET_CHUNK` entries since that chunking landed, and a statement reaching such
  a workspace finds a `Reaped` branch that `check_readable` rejects.
- Addendum 1's "39.3 s at 10⁶ open sessions" citation was already labelled unpinnable. It stays
  unpinned and is now also unnecessary: 51.6 s at 10⁵ branches with 10³ expired is measured here,
  on this machine, in a file in this directory.
- The `statement_lock_sweep.rs` header claimed twice that `forget_branches` is "what `scan_once`
  calls on every successful tick". `scan_once` does not call `reap_expired` any more and calls
  `forget_branches` once per chunk; those rows are an upper bound on one call. Corrected in place.

### Decision: the debug-only `audit_txn_refs` is bounded

It is O(open sessions) at both doors, so a debug fixture opening N sessions is O(N²). Measured on
one fixture in both profiles: release per-branch cost is flat (3 685 → 3 143 → 3 344 µs at
N=4k/8k/16k) while debug's excess over it RISES (1 373 → 2 660 → 3 396 µs). Debug already costs 2×
at 16k and the factor grows, so at the 10⁶ `SCALE-DESIGN.md` targets a debug build stops being a
way to reproduce anything — a reproduction trap, not merely slow.

Capped at `AUDIT_FULL_MAX` (1024) workspaces. **The cap is not a weakening**: reaching a map of
size M requires M door crossings from zero, so a door bypassing `insert_workspace`/
`remove_workspace` is fully audited long before the map outgrows the cap. Sampling or a cadence
would have been a weakening; a prefix of the crossings is not. Above the cap it prints one
`AUDIT_TXN_REFS_DOWNGRADED` line naming the threshold, so the downgrade is never silent, and the
blind spot it does have — a door correct below the cap and wrong above it — is named in the guard
itself. Fire-checked both directions: the notice prints exactly once at N=2000 and never at N=200.
`cargo test --lib` never trips it, which is how 1024 was checked rather than asserted (the old note
claimed "the largest fork loop anywhere in tests/ is ~200" without one).

### Still open

- `run_activity` remains an O(open sessions) hold on `AgentRuntime`'s state lock and remains this
  measurement's positive control. Unchanged.
- ⛔ *"`workspaces` lookups are still generation-blind; the escrow half is fixed, the map is not."*
  **CLOSED by addendum 6** — the map is keyed by the whole `BranchId`.
- The reconciliation's wall time is still O(open sessions). It is out of the statement lock now,
  which is what this row was about, but it is not cheaper.
- `REAP_YIELD` is a fixed 1 ms, so its cost is a function of how fast a reap is — under 2% at the
  ~58 ms/reap measured here, dominant if reaps ever get 100× faster. Stated at the constant, with
  the reason a conditional yield was rejected (it declines to yield in exactly the fast regime
  where starvation returns).

## Addendum 6 — D158 item 1: the generation-blind `workspaces` map, and it was LIVE

**This record called it "measured and deliberately NOT fixed" (item 2), and three later "still
open" lists repeated that. All four were too generous.** The brief that reopened it asked for the
deflation first — *if every path into those eleven sites re-validates against the catalog inside
the same statement lock, the window is closed by construction and this is latent, not live.* It
does not, and it is not. What follows is the refutation, then the fix.

### Why no race has to be won

| link | where | what it does |
|---|---|---|
| a connection caches its branch | `execution/session.rs`, `Session::agent: Option<AgentSession>` | set once by `BEGIN AGENT SESSION` |
| and hands it to the runtime unchecked | `agent_sql/dispatch.rs`, `run_in_session` | `let branch = a.branch` — never re-read from the catalog |
| nothing renews the lease | `renew_lease` has **no caller in `src`** outside the catalogs' own tests | so the branch expires with the connection open |
| the sweep reaps it and frees the slot | `reaper.rs` `set_state(..Reaped)` (bumps the generation) then `release_id` | `lease_thread.rs` then calls `runtime.forget_branches` |
| the next session pops that slot | `catalog.rs` `fork` | same slot, generation + 1 |

`DEFAULT_LEASE_MILLIS` is 15 minutes. An agent that pauses longer than that — one LLM call — comes
back holding a `BranchId` that names another agent's workspace. The SELECT path in particular
(`visible_rows_where`, then `record_read`) consults the catalog **not at all**.

### What it cost, measured over the real statement path

`tests/w4_stale_branch_crosses_agents.rs` runs `Scanner` → `Parser` → `executor::run` with two
`execution::session::Session`s on one `AgentRuntime` — the pgwire server's own shape. Against
`708822e` + the test, both fail:

```
thread 'a_reaped_sessions_select_must_not_read_the_new_occupants_staged_row' panicked:
  connection A's session was reaped and slot 1 now belongs to agent-b at generation 1, yet
  A's SELECT on b1@g0 was answered: qty = Some(999). ... 999 is agent-b's staged, unmerged row.

thread 'a_reaped_sessions_abandon_must_not_retire_the_new_occupants_session' panicked:
  assertion `left == right` failed: b_1 must still name agent-b's live branch b1@g1;
  A's ABANDON of the dead b1@g0 unbound it
    left: None
   right: Some(BranchId { id: 1, generation: 1 })
```

So: a cross-agent **read** of unmerged rows, and a cross-agent **delete** of a live session. The
second is the exact hazard `forget_one_branch`'s own comment describes — *"would then delete a LIVE
agent's workspace, release its escrow and unbind its name"* — reached through `seal`, which empties
`workspaces` and `names` **before** its first catalog read, so its eventual `Reaped` refusal arrives
after the damage. Only the reaper's write path had ever taken that argument.

### The fix

`State::workspaces` is `BTreeMap<BranchId, Workspace>`. Not a generation field plus eleven
comparisons: a stale generation is a different key, so it MISSES, and a site added later cannot
forget a check it never has to write. `forget_one_branch`'s hand-written re-validation is deleted
with the need for it — one authority, the key, rather than two.

One thing the slot-keyed map was giving away free had to be paid for explicitly: a recycled slot
used to COLLIDE, and `BTreeMap::insert` handed back the displaced workspace, which is what
`a_recycled_slot_does_not_strand_the_displaced_workspaces_capture` covers. At a new generation it
no longer collides, so `insert_workspace` evicts the slot's previous occupant by range scan. **That
is the one place where fixing this defect could have re-opened another**, and it is guarded by that
existing test plus a new `workspaces.len() == 1` assertion in it.

### Still open, and narrowed on purpose

`quarantine_reasons` remains keyed by the slot. Every path that reads or writes it passes
`self.branches.get()` first, so a recycled slot is not reachable through them; what IS reachable is
that `seal` never clears it, so a reaped quarantined branch leaves its reason for the slot's next
occupant to read. That is a wrong sentence in a diagnostic, not a cross-agent answer about data — a
correctness fix with no failing test behind it is how the item above got mis-filed in the first
place, so it is recorded at the field instead of bundled here.
