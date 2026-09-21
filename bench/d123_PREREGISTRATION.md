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
