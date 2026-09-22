# D123 — PRE-REGISTRATION. Committed BEFORE the first measurement. Amendments append-only.

**Row:** attribute the unexplained 0.1532 ms of fork's serial section, and establish whether that
section is the ceiling at all.

**This is a measurement row. Nothing here optimises anything.** Every code change on this branch is
a MEASUREMENT SCAFFOLD and must never merge — same status as `D35-gate-stubtouch`
(`src/buffer/buffer_pool.rs:158`). Only the `bench/d123_*` artifacts are citable.

---

## 0. THE PREMISES I INHERITED, AND WHETHER I VERIFIED THEM

| # | premise | source | verified? |
|---|---|---|---|
| P1 | fork plateaus ~4,382/sec at 64 threads; 256 threads gives ~4,351 | `bench/fork_concurrency_after.txt` | READ, re-measuring |
| P2 | `throughput = 1/serial_time` agreed 4,386 vs 4,382.7 | same file | READ, arithmetic checks |
| P3 | effective serial section = 0.228 ms | `1/4382.7` — a DERIVED quantity, not an observation | ✅ it is derived |
| P4 | named components sum to 0.0748 ms of tree work | `bench/serial_section_profile.txt`, **measured at 1 THREAD, UNCONTENDED** | ✅ read the profiler at `table_catalog.rs:1331` |
| P5 | "one fsync = 3.574 ms" is a RESIDUAL, not a measurement | `3.690 − 0.11017 = 3.580` | ✅ arithmetic reproduces |
| P6 | W-B's bar / the adversary's kill | `frontier/INVENTION-TRIGGER.md:148` | ✅ read verbatim |

⚠ **P3 AND P4 ARE NOT THE SAME KIND OF NUMBER, AND THE 0.1532 ms IS THE GAP BETWEEN TWO REGIMES,
NOT A MEASURED QUANTITY.** P3 is `wall/forks` at 64 threads. P4 is a sum of uncontended
single-thread microbenchmarks. Subtracting them assumes the operations cost the same under
64-thread contention as alone, which is the thing in question. The subtraction is the hypothesis,
not the evidence.

## 0b. AN ARITHMETIC OBJECTION TO THE ADVERSARY, PRE-REGISTERED AS A PREDICTION, NOT A RESULT

`INVENTION-TRIGGER.md:148` says a closed 64-thread harness is *"fsync-bound at 64 forks per 3.574 ms
round, so even a serial section driven to ZERO cannot clear the bar."*

That bound evaluates to **64 / 3.574 ms = 17,907 forks/sec**, which is **4.09x ABOVE the measured
4,382/sec plateau.** A bound sitting 4x above the observation cannot be what pins the observation.
The bound also scales with T, so it predicts 256 threads ≈ 71,600/sec; `fork_concurrency_after.txt`
measured 4,350.8 there. **I predict F1 will NOT fire and the adversary's inference is wrong.**
Stating it in advance so that if the measurement disagrees with me, the disagreement is on record.

---

## 1. THE TWO MODELS, AND WHAT EACH FORBIDS

* **M-SERIAL** — throughput = `1 / S`, S the effective serialization interval of the `logical`
  critical section. Forbids: throughput rising with T once T is large; throughput unchanged when
  work is added under the lock.
* **M-FSYNC** — throughput = `T / F`, F the fsync round duration. Forbids: throughput flat in T;
  throughput falling when a few microseconds are added under the lock.

They are discriminated by the **thread axis** and by the **added-work axis**, independently.

---

## 2. FALSIFIERS

### F1 (inherited) — IS THE SECTION THE CEILING?
Drive the serial section toward zero and re-measure at 64 threads.
* **F1 FIRES** (section is NOT the ceiling, and 22 candidates were aimed wrong) if throughput at
  stub level L3 is within **±10%** of the L0 baseline.
* **F1 does not fire** if throughput rises by **>10%**.

⛔ **THE STUB MUST NOT DELETE THE FSYNC.** `serial_section_profile`'s own comment records that macOS
short-circuits `F_FULLFSYNC` when the file has nothing pending (0.010 ms), so a stub that leaves no
dirty page measures a system with no durability and would look like a spectacular win for the wrong
reason. Every stub level therefore **keeps `write_header()` + `stage()`**, which guarantees ≥1 dirty
page per fork. Levels: L0 full · L1 drop `write_record` · L2 also drop both parent lookups and the
FREE_ID scan · L3 only `write_header` + `stage`. Reported alongside `syncs_issued()`; **any level
whose fsync count collapses is VOID and reported as void, not as a result.**

### F2 (inherited) — IS THE 0.1532 ms CONTENTION?
Per-phase in-place instrumentation at **T = 1, 8, 64**.
* **F2 FIRES** (not contention) if the per-fork residual does not grow with T.
* Not fired if it grows monotonically in T.

### F3 (inherited) — ATTRIBUTION FLOOR
If <50% of the 0.1532 ms lands on a **named, measured** mechanism, the answer reported is
**"unattributed"**. A plausible unmeasured mechanism does not count and will not be written down as
if it did.

### ⭐ F4 (added) — HOLD vs HANDOFF. THE DECOMPOSITION THAT MAKES F2 ANSWERABLE
Only one thread can hold `logical` at a time, so per fork:

```
S_eff  = wall / forks            (= 1/throughput, the 0.228 ms)
hold   = mean time from lock acquired to lock released
gap    = S_eff − hold            (the lock sitting IDLE between a release and the next acquire)
U      = hold / S_eff            (lock utilisation, ≤ 1 by construction)
```

* **H-HANDOFF** — the 0.1532 ms is mutex park/unpark handoff. Predicts `hold ≈ 0.075 ms`,
  `gap ≈ 0.153 ms`, **U ≈ 33%**.
* **H-INFLATION** — the 0.1532 ms is inside the section (cache-line bouncing, page latches,
  allocator, eviction). Predicts `gap ≈ 0`, `hold ≈ 0.228 ms`, **U ≈ 100%**, and at least one named
  phase inflating from T=1 to T=64.

These are mutually exclusive and exhaustive over the per-fork interval. **U is the single number
that decides it**, and U is a ratio of two durations measured by ONE instrument in ONE process, so
the 46x load spread largely cancels.

### ⭐ F5 (added) — THE MARGINAL COST OF SERIAL WORK. NO CORRECTNESS COMPROMISE AT ALL
Add `k ∈ {0, 2, 4, 8, 16}` extra `upsert`s under `logical` and fit `1/throughput` against k.
An upsert costs `u = 0.01344 ms` uncontended (`serial_section_profile.txt`).

* slope ≈ `u` ⇒ added work costs what it costs alone ⇒ the 0.1532 ms is a **fixed per-fork
  overhead** (handoff), not an inflation of the work. Supports H-HANDOFF.
* slope ≈ `3.05 · u` ⇒ work under the lock is 3.05x more expensive under contention, which is
  exactly `0.228 / 0.0748`. Supports H-INFLATION and **attributes the residual by construction.**
* slope ≈ 0 ⇒ **M-FSYNC**, and F1 fires.

The **intercept** is an independent estimate of S that never touches the 1-thread profiler, so it
tests P3 without inheriting P4's regime error. This arm is purely additive — no stub, no durability
change — so it is valid even if every stub level turns out void.

### F6 (added) — THE THREAD AXIS
T ∈ {1, 8, 64, 128, 256, 512}. M-FSYNC requires throughput ∝ T. M-SERIAL requires it flat past the
knee. Sharpest single discriminator and it costs one run.

---

## 3. INSTRUMENT, AND ITS BLIND SPOTS STATED IN ADVANCE

* Per-thread `thread_local` accumulators merged at thread exit. **Never a shared atomic in the hot
  path** — a global `fetch_add` from 64 threads is itself cache-line bouncing and would manufacture
  the effect F4 is trying to detect.
* `Instant::elapsed` quantises to **41.67 ns** on this box. Every quantity reported is a SUM over
  ≥2,000 samples (≥ 5,400 ticks for a 0.228 ms section), so quantisation is ≤0.02%. **No
  single-operation duration is reported.**
* **Probe perturbation is measured, not assumed:** the same binary runs with the probe compiled in
  but disabled by an `AtomicBool`, and the L0 throughput of probe-on vs probe-off is reported. If
  they differ by >2% the probe is perturbing and every duration it produced is quarantined.
* `~/wt/logs/measure-lock.sh` held for every timed run; `load_at_acquire` stamped on every duration
  as an **upper bound**.
* Provenance: `ferrodb::build_provenance()` printed by every harness.

## 4. WHAT I WILL NOT DO

* Not optimise anything. Not merge. Not push.
* Not report a mechanism I did not put a counter on (F3).
* Not quote any stub level whose `syncs_issued()` collapsed.
* Not carry P4's 1-thread numbers into a 64-thread claim without saying which regime each came from.

---

# AMENDMENT 1 — written BEFORE any result was read. A seam in my own instrument.

`probe::record(PH_HOLD, ..)` fires at the end of the critical-section block, but **Rust drops in
reverse declaration order and `_g` is declared FIRST, so it drops LAST.** Between my `PH_HOLD`
stamp and the actual unlock, these still run *with the lock held*:

* `parent_core: CoreRecord` drops,
* `parent_envelope: Option<Vec<u8>>` drops,
* the guard `_g` itself unlocks.

So `gap = S_eff − HOLD` is **not** purely handoff. It is `unlock + those drops + park/unpark +
scheduler latency`. Freeing heap under 64 threads is itself a named candidate (allocator
contention), so if `gap` comes back large I may not attribute it to handoff without splitting it.

**Pre-registered rule, so that this cannot be decided after seeing the number:**

* If `gap` ≤ 0.02 ms, the seam is immaterial and no follow-up is run.
* If `gap` > 0.02 ms, **a second scaffold iteration is REQUIRED** before any attribution claim:
  add `PH_DROPS` (explicit `drop(parent_core)`/`drop(parent_envelope)` bracketed) and `PH_UNLOCK`
  (explicit `drop(_g)` bracketed), so `gap` splits into measured parts and a true residual.
  Until that runs, `gap` is reported as **unattributed**, per F3 — not as "handoff".

# AMENDMENT 2 — a wrong number inherited from the source artifact, recorded for the retraction.

`bench/fork_concurrency_after.txt` states "one fsync 3.574 ms". Reproducing it:
`3.690 − 0.11017 = 3.580`, not 3.574. `3.690 − 0.1148 = 3.575` does reproduce it — **0.1148 is the
SUPERSEDED value of SUM**, corrected to 0.11017 in `serial_section_profile.txt`. So the residual was
computed against the old sum and never recut when the sum was fixed. Immaterial to every conclusion
here (0.006 ms on a 3.58 ms quantity), but it is a stale derived number sitting in an artifact
others quote, and it is the same class of error as the regime mix-up this row exists to check.

# AMENDMENT 3 — what "extra ms / k" buys, and its bias.

F5's extra upserts hit a **fixed key set**, so every thread touches the same few pages. That is the
BEST case for cache residency and the WORST case for cache-line bouncing. Therefore:
`extra_ms(T=64)/k ÷ extra_ms(T=1)/k` is a same-instrument, same-work contention multiplier, but it
is **biased toward detecting line bouncing and biased against detecting eviction or page-diversity
effects.** If it comes back ≈1.0x, that rules out line-bouncing-on-shared-pages specifically; it
does not rule out a mechanism that needs many distinct pages. Said here so the negative result is
not overread later.

# ⭐ AMENDMENT 4 — A THIRD REGIME MISMATCH IN THE INHERITED SUBTRACTION. Written before any result.

The two numbers being subtracted were not taken on the same tree, and nobody has said so.

* **0.228 ms** comes from `fork_concurrency.rs`, which opens an **EMPTY** catalog and times 4,000
  forks. The tree grows 0 → 4,000 keys *during* the timed window.
* **0.0748 ms** comes from `serial_section_profile`, which does `const WARM: usize = 20_000` first,
  precisely so that "a profile taken on an empty tree measures the best case of every descent".

So the named components were priced on a **5x deeper tree** than the throughput they are subtracted
from. Every one of those seven components is a B+tree descent. The subtraction therefore mixes
**three** regimes, not the one the brief names: 1-thread vs 64-thread, uncontended vs contended,
**and 20k-key vs 0→4k-key**. The depth mismatch pushes in the direction of making the named share
look LARGER than it is in the throughput regime, i.e. the true unattributed share at 4k keys may be
**more** than 67.2%, not less.

My harness is immune to this by construction — `HOLD` and `S_eff` come from the same run on the same
tree — but comparability to `bench/fork_concurrency_after.txt` is not. Therefore:

**PRE-REGISTERED, BEFORE SEEING ANY NUMBER:** the battery is run a second time end to end with
`FERRODB_D123_WARM=0`, which reproduces `fork_concurrency`'s exact regime.

* **Reproduction check:** the warm=0, stub=0, T=64 arm must land within **±15%** of 4,382 forks/sec.
  If it does not, my harness is measuring a different system from the one the inherited numbers
  describe, **and every comparison to them is withdrawn** — I report the discrepancy rather than
  the attribution.
* The warm=20,000 battery keeps its own numbers; the two are reported side by side and never mixed
  into one subtraction. That is the error this row exists to stop repeating.

# ⭐ AMENDMENT 5 — A CLOSURE IDENTITY THAT TESTS THE INSTRUMENT ITSELF. Written before any result.

In a closed harness every thread's cycle is exactly `wait → hold → durable`, and each thread
completes `forks/T` forks in the wall time. So, per fork:

```
WAIT + HOLD_TOTAL + DURABLE  ==  T × S_eff        (T = threads, S_eff = wall/forks)
```

All three terms are measured by the probe and `T × S_eff` comes from the wall clock, so this is a
**closure check on the whole instrument**, not another hypothesis. It needs no rebuild — the three
phases are already in the `phases` table.

* Closes to within **±5%** ⇒ the probe accounts for the entire loop; there is no unmeasured limb,
  and the F4 decomposition can be quoted.
* Falls short by more than that ⇒ there is time in the cycle the probe never sees, and **every
  attribution is downgraded to a lower bound** until the missing limb is found. I will say so
  rather than quote the partition.

**A prediction that follows from it, recorded now so it cannot be retrofitted:** at T=64 with
S_eff ≈ 0.228 ms, the cycle is ≈14.6 ms, of which the section is 0.228 ms. So `WAIT` must dominate
— roughly 11-14 ms — because 64 threads queue at `logical` while only ~13.8 get through per fsync
round. **If `DURABLE` dominates instead and `WAIT` is small, the threads are not queueing on the
mutex at all, the serial section is not what they are waiting for, and F1 should fire.** That makes
the WAIT/DURABLE split a second, independent read on Q2 from the same run.

---

# AMENDMENT 6 — THE CLEAN F1 RE-RUN. Written before it is run; the first battery is already read.

The first battery (run by the lead on the rate-limit window; see `bench/D123_LEAD_RAN_THIS.md`)
answered F1, F2, F4, F5 and F6. Two defects in it are worth one more lock acquisition, and nothing
else is.

**D1 — F1 was measured with the probe ON.** `mode_stub` prints `HOLD`, which exists only with the
probe on, so the F1 ratio was taken between two instrumented configurations. F1's load-bearing
columns are `forks/sec` and `vs L0`; neither needs the probe. `mode_f1` turns it off.

**D2 — one run per cell, and one cell was a 2x outlier.** warm=0, probe OFF, T=64 returned
2796.6/sec while every other 64-thread warm=0 arm that evening sat at 4788–5256. Read as an effect,
that single cell says the probe nearly doubles throughput (+87.95%). `mode_f1` and `mode_perturb2`
replicate 5x and report **medians with the full min–max spread**.

**Level order is rotated between reps** (rep r starts at level r mod 4): run in fixed order, any
monotone drift in the box lands entirely on the last level, which is L3, which is the level the
verdict rests on.

### Decision rules, fixed now
* **F1 fires** iff L3's median is within ±10% of L0's median. Otherwise it does not fire.
* **If the L3/L0 ratio is smaller than the within-level min–max spread, the result is NOISE** and is
  reported as "F1 undecided at this N", not as a verdict.
* **perturb2 (a)** — probe ON vs OFF over both positions. >2% ⇒ the probe perturbs and every
  duration in this row is quarantined. ≤2% ⇒ the +87.95% was a transient and the durations stand.
* **perturb2 (b)** — 2nd-run vs 1st-run over both configs. Large (b) with small (a) ⇒ the outlier
  was position in the process, not the instrument.

### The void guard, restated because the first run made it necessary
`syncs` FALLS as the section gets faster (496→250 at T=64). That is group commit batching more
forks behind a shorter critical section, **not** lost durability. The check is therefore
**conservation**: `syncs × forks-per-sync` must still equal N. A level that lost durability fails
that; a level that merely batched better passes it. Printed per level.

### What is NOT re-run, and why
F2, F4, F5, F6 rest on ratios taken *within a single arm* (U(hold), the 64/1 per-phase column, the
per-upsert cost at fixed k), which a common-mode transient cannot fabricate. They are replicated
opportunistically (`phases` ×3, `extra` ×2) as a consistency check, not because they are in doubt.

### ⚠ A correction to `bench/d123_raw/00_LEAD_CONTAMINATION.txt`
That note says the 4.34 s `cargo build --lib` landed on "`20_threads.txt` — the F1 arm". The arm is
right and the label is wrong: **`20_threads` is F6. F1 is `40_stub`**, which ran afterwards
(mtimes 18:40 vs 18:43; the build was at 18:37–18:38 local). So F1 carries no known contamination,
and F6 — whose verdict is `measured/bound` 1.00x→0.03x, nowhere near any boundary — carries one
that cannot reach it.

---

# AMENDMENT 7 — TWO ELIMINATIONS FROM ALREADY-BANKED DATA, AND THE ONE ARM STILL MISSING.

Written after reading the first battery, before the F1 re-run's numbers exist.

### Eliminated: BUFFER-POOL EVICTION. Free, from data already on disk.
`MAX_BUFFER_POOL_PAGES = 1024` (`src/buffer/buffer_pool.rs:442`). The warm=20,000 tree (~100k keys)
strains that pool; the warm=0 tree (~40k keys at most, and starting empty) does not. If eviction
drove the inflation, the LARGE tree would inflate more. It inflates **less**:

| 64/1 ratio | warm=20k (large tree) | warm=0 (small tree) |
|---|---|---|
| write_record | 2.29x | **2.36x** |
| child_key_insert | 2.43x | **2.61x** |
| HOLD | 1.61x | **1.71x** |

Wrong direction, both phases, both totals. **Eviction is not the mechanism.** No const change and no
extra arm was needed to establish it, and none will be run.

### Eliminated: EVERY CANDIDATE THAT NEEDS TWO THREADS INSIDE THE SECTION.
`fork` does all of its tree work under `logical`, so **only one thread is ever inside**. Therefore
in-section work cannot be slowed by another section-holder — not by page-latch contention between
forkers, not by allocator contention between forkers, not by cache-line bouncing between forkers.
This is structural, not statistical. Combined with U(hold) ≈ 97%, which puts the time inside the
section, **the counterparty must be something running OUTSIDE the lock** — and the only thing that
does is `durable()` → `pool.flush_all()` + `disk_manager.sync()`, the group-commit flush.

### The surviving named mechanism, and the four signals that select it
Group-commit flush interference: the lock holder's NEW-page writes collide with the concurrent
flusher draining dirty pages.
1. Only the two phases that insert NEW keys inflate (2.29x, 2.43x).
2. Reads and single-page writes get **faster** (0.74–0.92x) — a hotter pool, no collision.
3. F5's fixed-key upserts, which re-dirty pages that are already dirty, are **0.68x** — cheaper.
4. Inflation tracks `f/sync`, i.e. how many forks' worth of new pages each flush must drain:
   f/sync 1.0 → 4.0 → 17.5 against write_record 1.00x → 1.59x → 2.29x. Monotone.

### ⚠ THE ARM I HAVE NOT RUN, STATED PLAINLY PER F3
I have not run an arm with the flush removed, because removing it removes durability and macOS then
short-circuits `F_FULLFSYNC` — the confound this row has guarded against throughout. So the
mechanism is **named by elimination plus four selectivity counters plus a structural argument, not
by direct intervention.**

**The one arm that would close it**, to be run only if the F1 re-run leaves lock time: a stub level
that skips `child_key_insert` ONLY, keeping `write_record`, then `phases` at T=64. If write_record's
own cost falls when the OTHER new-key insert is removed — fewer new pages per fork for the flusher
to drain — the mechanism is confirmed by intervention. If write_record is unchanged, the flush is
not the counterparty and the mechanism reverts to unattributed. **Pre-registered either way.**

# AMENDMENT 8 — A DETECTOR THIS ROW NEEDED AND DID NOT HAVE.

Two runs in this row produced files that LOOK like results and are not: the lead's novelty attempt
(correct banner, correct preamble, no table) and, earlier, a binary that did not contain the mode
being asked for (would have exited 2 on "unknown mode"). Both would pass a skim.

⇒ Every `run()` in this row's scripts appends `# harness_exit=$?`. **The detector is the ABSENCE of
that line, and absence is what nobody checks.** Recorded here rather than in a commit message
because the next reader of bench/d123_* needs it: a d123 artifact with no `# harness_exit=0` at its
foot is a truncated run and must not be quoted.

⇒ And the companion check, which established that two banked binaries never contained `mode_novelty`
at all: **run the harness with a deliberately bad mode and read the list it admits.** The presence
of a built binary says nothing about which arms it has.

# AMENDMENT 9 — A CONFOUND IN THE NOVELTY ARM, CHECKED BEFORE ITS RESULT WAS READ.

The new-key arm inserts k=8 NEW keys per fork; over 8,000 forks that is 64,000 keys added to the
tree, against 8 keys total for the fixed-key arm. The new-key arm therefore runs against a tree that
grows far more during its window, and deeper descents would inflate its cost for a reason that has
nothing to do with page novelty.

⇒ **It cancels, and here is why the criterion is stated the way it is.** The verdict is NOT
`new_cost / fixed_cost`. It is the T=64/T=1 ratio computed SEPARATELY WITHIN each config:

    fixed:  cost(T=64) / cost(T=1)
    new:    cost(T=64) / cost(T=1)

Both thread counts of a given config insert the same 64,000 (or 8) keys, so the tree growth is
identical in numerator and denominator and divides out. What the two ratios then compare is how each
kind of work RESPONDS to concurrency, which is the question, rather than how expensive each kind of
work is, which is not.

⇒ Consequence to state when reporting: the new-key arm's ABSOLUTE ms/upsert will be higher at both
thread counts, and that difference is the tree, not contention. Only the ratios are quoted.

# ⛔ AMENDMENT 10 — THE FIRST NOVELTY RUN IS VOID. MY OWN RULE, ON THE ARM THAT DECIDES ROUND 4.

`bench/d123_final/40_novelty_CONFOUNDED_unrotated.txt` is retained as evidence and MUST NOT be
quoted as the verdict. It read FIXED 1.03x, NEW 1.54x — in the predicted direction — and that is
precisely why it cannot stand.

**THE DEFECT.** `mode_novelty` iterated `for [false, true]` with no rotation, so FIXED always ran
FIRST and NEW always SECOND, at every thread count. Across that run:
    # load_at_run_start:  3.79
    # load_at_run_end:   13.64
Load rose 3.6x monotonically (a `cargo test --release --lib` started in a sibling worktree while
the lock was held). So the NEW arm ran systematically later, under systematically heavier load,
than the FIXED arm it is compared against — **and the confound pushes in the SAME DIRECTION as the
finding.** I cannot say how much of 1.54x-vs-1.03x is novelty and how much is "ran later".

This is instance N of the rule this row exists to enforce, committed by me, in the arm the whole
round turns on: **same instrument, different moment.** `mode_f1` rotates level order every rep and
says so in its banner. I knew rotation mattered and did not carry it into this mode. Caught by the
team lead reading the load stamps, not by me.

**THE FIX, and it is the same one that rescued F1 from "undecided".** All FOUR cells
— (T=1,fixed) (T=1,new) (T=64,fixed) (T=64,new) — now run ADJACENTLY inside one rep, in an order
rotated each rep, with a wall-clock stamp per rep. The verdict is a PER-REP CONTRAST:

    R_fixed = cost_fixed(T=64) / cost_fixed(T=1)     within one rep
    R_new   = cost_new(T=64)   / cost_new(T=1)       within one rep
    CONTRAST = R_new / R_fixed                       median over reps

Drift is common-mode across four adjacent cells and cancels. **Decision rule, fixed now:
CONTRAST >> 1 ⇒ novelty is the variable and the counterparty is the group-commit flush.
CONTRAST ≈ 1 ⇒ UNATTRIBUTED per F3, reported as a complete answer.** The count of reps with
CONTRAST > 1.00x is reported alongside the median, because a count of draws needs no estimator.

**PROVENANCE NOTE.** The void run was `build at 4427df251512` while the F1 arms were `8665941`.
Checked rather than assumed: `git diff 8665941..4427df2 -- src examples build.rs Cargo.toml` is 14
lines in `examples/d123_serial_attribution.rs`, all of them `mode_f1`'s per-rep timestamps. It does
not touch `mode_novelty`, `extra_new_keys`, or the fork extra-upsert path. So the binary moved but
the code under test did not, and the void is the design, not the build.
