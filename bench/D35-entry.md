## D35 — the buffer-pool HIT path: the mutex was the wrong suspect

**VERDICT (BP-Wrapper, as dispatched): MICRO — DO NOT BUILD.** Measured upper bound 1.33x at 16
threads, slope unchanged. **VERDICT (what the measurement found instead — take the page table off
the hit path): shape change AND 14.9x — BUILD.**

Evidence: `bench/d35_gate_stubtouch.txt` on branch `D35-gate-stubtouch` (`ad46887`), 3 interleaved
reps, RESIDENT arm, parameters byte-identical to `bench/s22_bufpool_before_after.txt`. Medians:

| arm | 1T | 2T | 4T | 8T | 16T | 16T/1T |
|---|---|---|---|---|---|---|
| BASE (unmodified S22 lane) | 21,306,377 | 6,136,496 | 3,916,018 | 2,954,761 | 3,022,360 | **x0.142** |
| STUB (`touch` deleted) | 31,227,162 | 6,027,971 | 3,942,831 | 3,494,620 | 4,014,630 | **x0.129** |
| C1 (page table off hit path) | 47,989,438 | 18,298,134 | 26,387,738 | 36,653,509 | 44,924,760 | **x0.936** |

All 9 runs `HARNESS_EXIT=0`; the harness's own MISS and STAMP guards passed on every one.

### 1. The dispatched answer, and why the measurement kills it

BP-Wrapper (Ding, Jiang, Zhang, ICDE 2009): batch hit-path policy updates into per-thread FIFOs and
let whoever next holds the policy lock drain them, so an arbitrary replacement policy stops being a
contention point. It is the correct standard answer **to the question as posed**, and the question
as posed was wrong.

STUB deletes `self.arc_cache.lock().unwrap().touch(page_id)` from the hit path entirely. That is the
**upper bound** on BP-Wrapper, on the per-frame-stamp variant, on any batching scheme — a real
batcher costs more than zero. Result: 1-thread throughput rises 47% (21.3M → 31.2M), 16-thread rises
33% (3.02M → 4.01M), and **the slope does not change sign — x0.142 → x0.129, marginally worse.**
The ratio degrades precisely because the constant-factor win lands harder at 1 thread than at 16,
which is the signature of a curve that is contention-bound by something else.

This is the same result S22 already recorded about D25 — "single-thread throughput improved about
30%; scaling did not improve at all" (`buffer_pool.rs` module doc). The brief said the bar is a
complexity-class or shape change and not a constant. **BP-Wrapper here is a constant.**

### 2. What actually holds the slope — and it was in the file the whole time

The RESIDENT hit LOOP is `fetch_page` + `unpin_page`. Between them, per iteration:

* `fetch_page` → `try_pin_resident`: `page_table.read()`, then `frames[i].read()`, then
  `pin_counter.fetch_add`.
* `unpin_page` (`:475`): `page_table.read()` **again**, then `frames[i].read()` again, then
  `pin_counter.fetch_update`.

That is **four RwLock read acquisitions and two atomic RMWs per hit**, before `touch`. A Rust
`RwLock`'s reader count is a single process-wide cache line that every reader atomically RMWs — it
contends exactly like a mutex, it is simply not spelled `Mutex`. The `arc_cache` mutex was one
synchronisation point out of five, which is why removing it bought 33% and nothing else.

**C1** keeps `touch` deleted and additionally resolves `page_id → frame` through a lock-free
direct-mapped mirror (`Vec<AtomicUsize>`, `frame_i + 1`, 0 = absent, `Release`/`Acquire`), in
**both** `fetch_page` and `unpin_page`, maintained at all four page-table write sites. It keeps the
frame latch and the pin, so **C1 is correct, not a ceiling**: x0.936, and the curve *rises*
monotonically from 2 threads (18.3M) to 16 (44.9M). Against BASE at 16 threads that is **14.9x**;
against STUB, **11.2x**.

**Why C1 is sound.** The module doc already states the invariant it relies on: "The frame latch is
the arbiter. A page table lookup only ever produces a *candidate* frame." The mirror produces the
same candidate by a cheaper route; the `frame.page_id != Some(page_id)` re-check under the frame
latch is untouched, so a stale mirror entry costs a retry and cannot hand out a wrong frame. For the
`unpin_page` half the argument is different and must be stated: a *pinned* page cannot be evicted
(`is_pinned` blocks it), so its mirror entry cannot go stale between the pin and the matching unpin.

### 3. Deflation of the option that won

* **Prior art.** PostgreSQL partitions its buffer mapping table (`NUM_BUFFER_PARTITIONS`) for
  exactly this reason. LeanStore and Umbra go further with one atomic state word per page plus
  optimistic version validation. **ferrodb's own `buffer_pool.rs` module doc already cites
  "PostgreSQL's ... partitioned buffer mapping table"** — the prior art was named in the file being
  fixed, which is rule 1a's failure mode exactly.
* **Isn't this just X?** It is a lock-free (or partitioned) page table. Yes. The non-obvious part is
  only that it was never the suspect: the whole lane was pointed at the replacement policy.
* **Isn't this just the sharding that was already REJECTED?** **No, and the distinction is the
  point.** Sharding `arc_cache` was rejected because ARC's adaptivity is *global by construction* —
  N caches of capacity/N adapt to a 1/N sample, a hot shard cannot borrow from a cold one, and the
  cost lands on hit RATE where a throughput benchmark cannot see it. A page table carries **no
  policy and no adaptivity**: it is an exact map from page id to frame. Partition it by page id and
  every lookup still goes to exactly one partition and returns exactly the same answer. **There is
  no hit-rate surface to damage**, because ARC is not touched at all.
* **Is the premise true?** Measured, three reps, above.
* **Does this repo already do it?** No. `page_table: RwLock<HashMap<u32, usize>>`, one lock,
  process-wide, taken twice per hit.

### 4. Options rejected

* **BP-Wrapper / per-thread FIFOs, and the per-frame atomic stamp variant.** Both lose to the STUB
  measurement: their shared upper bound is 1.33x with the slope unchanged. Worth revisiting *after*
  the page table is fixed, when `touch` may become the next binding constraint — not before.
* **Shard `arc_cache`** — as previously rejected: adaptivity is global, cost lands on hit rate.
* **Replace ARC with CLOCK** — as previously rejected: a policy change in a concurrency fix's
  clothes. Note the measurement makes it *doubly* wrong: the hit path's problem was never the policy.
* **Pinned page handles / cursors (cut the number of `fetch_page` calls).** Real work, belongs to
  `D26-range-scan-cursor`. It cuts traffic volume, not per-call contention, and the harness fetches
  in a tight loop so it cannot move this benchmark. Compounds on top of the page-table fix.

### 5. What would falsify the decision

**Throughput.** Falsified if the 16T/1T ratio does not clear x0.5 on a merge-ready implementation
(C1 measured x0.936; the margin is for a partitioned hash table being dearer than a direct-mapped
array). The multiplier is not the criterion and the sign of the slope is — the two reps of
`s22_bufpool_before_after.txt` disagree 40% on multipliers under fleet load.

**Hit rate — stated specifically, because a throughput benchmark cannot see a hit-rate loss.** This
is the check that killed the sharding option, and the page-table fix has an unusually strong answer
to it: **ARC is not modified at all**, so the eviction sequence must be *bit-identical*, not merely
close. The gate is therefore an equality assertion, not a tolerance:

1. **Deterministic.** Replay a fixed reference trace (Zipf, sequential scan, captured B-tree
   descent) single-threaded through old and new. Assert the **sequence of evicted page ids is
   identical**. Any difference at all is a bug, because nothing in ARC changed.
2. **Concurrent.** OVERSUBSCRIBED arm — the RESIDENT arm reports `reads_per_fetch = 0.000` and is
   structurally **blind** to hit rate. The harness already emits `reads_per_fetch` (1.000 today) as
   a column. 16 threads, ≥10 seeds, falsified if the mean rises at all beyond run-to-run noise.
3. **Negative control, mandatory — the gate is void without it.** Corrupt the mirror deliberately
   (skip one of the four maintenance sites, or drop the `frame.page_id` re-check) and confirm tier 1
   fails and `wrong` becomes non-zero in the harness's STAMP GUARD column. A detector that has never
   fired is not a clean result. **This one has already half-fired for real:** the C2 arm (mirror
   only, no frame latch, no pin) panicked in setup with `allocate: NotEnoughSpace` rather than
   reporting a comfortable number, which is the guards working.

**Also falsifying:** a partitioned table whose memory cost is unacceptable. The direct-mapped mirror
measured here is O(max page id) — fine at 16,384 pages, not at millions. A merge-ready version is a
partitioned hash table (Postgres's shape), and its extra indirection must be re-measured, not
assumed to land where C1 landed.

### 6. Scope and honesty notes

Measured: the RESIDENT hit path only. **Not measured:** the OVERSUBSCRIBED arm under C1 (the
mirror's maintenance cost falls on the miss path, which is where evictions clear entries), and hit
rate — no trace-replay harness exists yet, so tiers 1–3 above are specified and unbuilt. The
`buffer_pool.rs` edits on `D35-gate-stubtouch` are a **measurement scaffold and must not be merged**:
the `FERRO_D35_ARM` switch, the deleted `touch`, and a direct-mapped mirror sized for a benchmark.
I did not read the ICDE paper — no web access — so every claim about BP-Wrapper here is derived from
ferrodb's own ARC and from the STUB measurement, not quoted from Ding et al.
