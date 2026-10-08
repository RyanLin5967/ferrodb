# READ-VS-N: pre-registration (arms 1 and 3; arm 2 pending its brief)

Written 2026-09-24 on branch `read-vs-n` at base `9aa6968`. **Written before any harness change and before any
measurement.** Nothing below was fitted to a number. Amendments go at the bottom, append-only. Each is committed
before the code or run it amends.

Tags: **READ** means read in source at `9aa6968`, cited by symbol so the citation survives line drift.
**INFERRED** means arithmetic or reasoning from what was read, not measured. No cell here is **MEASURED**.

## 0. The arms

| arm | question | status |
|---|---|---|
| 1 READ | what a point read on ONE branch costs as N, the number of live written branches, grows | pre-registered below |
| 2 MERGE | merge cost against N and against the number of prior merges M | **the brief did not arrive** (asked the lead 2026-09-24). Pre-registered by amendment before any arm-2 code |
| 3 RESTART | what the FULL production open costs at each N | pre-registered below |

The gap for arm 1: `bench/d61_write_curve_1e6_run2.txt` says in its own text that it does not cover "anything about
concurrent READ performance at 10^6 branches". The gap for arm 3: D65 times two calls of the open path. The D65
run-4 adversary (`artie-research frontier/d65_run4_adversary.md` §1c) lists what D65 leaves out: WAL recovery, the
`.tel` and `.provenance` replays, and `LeaseThread::start`'s synchronous orphan sweep.

## 1. The instrument

The instrument is `examples/branch_curve_writes.rs`, the D61 harness, with D65's run-4 extension merged in from
`d65-reopen-curve` @ `08ca677`. It is extended, not replaced. `CURVE_ARMS=read,restart` switches it to a
**production layout**. Every other invocation runs the historical D61/D65 code path unchanged.

**The production layout.** The database is opened by `ferrodb::cli::cli::open_database`, which is `run_cli`'s open
sequence moved into a function that `run_cli` now calls. The harness and the shipped binary therefore run the same
code, and drift between them is not expressible. Consequences, READ from `run_cli`:

* the SQL catalog sits in the main file;
* the arena floor is the high-water mark plus 32,736 pages of headroom (`DEFAULT_ARENA_HEADROOM`);
* the branch catalog is `{db}.branchcat` with its own pool;
* free-space-map persistence is ON;
* a lease thread runs.

The parent opens with a one-day scan interval, so its lease thread sweeps once, on its first pass, and then sleeps.
The parent waits for that pass to finish before timing anything. Two reasons: the leases never expire, and a
periodic sweep during a timed window would inject catalog-pool churn at random moments.

**The workload.**

* Before any fork, trunk writes one row: table `t`, row 1, value `BigInt(-1)`.
* Each branch is forked with `branches.fork(TRUNK, lease)`, then writes its own row 1 = `BigInt(branch id)` with
  `AgentRuntime::put_row`.
* Both calls run under the statement lock (`CatalogLock`), as the CLI and pgwire run statements.
* `put_row` copies trunk's leaf into the branch's arena and publishes it with `set_root`, so each branch owns
  exactly one page.
* Nothing in this workload writes `.tel` or `.provenance` frames. Arm 3 therefore times those replays over EMPTY
  logs: **their dependence on N is NOT measured here**, only their fixed cost. The report must say so.

## 2. Arm 1: the point read

A point read on branch b is `AgentRuntime::get_row(b, "t", 1)`. That is two calls, and the harness makes both
itself so it can bracket each one:

1. `runtime.root_of(b)`, which is `branches.get(b)?.root_page_id` (the catalog phase, `c.*`);
2. `runtime.storage().get(root, table_id("t"), 1)`, a CowTree descent from that root; here the root is a leaf, so
   this is one `read_page` (the data phase, `d.*`).

At each checkpoint N:

* warm up, untimed: every branch once, in order, up to 65,536 branches, then 16,384 random reads;
* then time K = 16,384 reads per arm at FIXED threads, first 1, then 8;
* arms are interleaved in blocks of 64 reads, the arm order rotates per block (a Latin square), and a barrier per
  block keeps all threads on the same arm, so an arm's contention cannot depend on how long the other arms take.

| arm | read | depends on N through |
|---|---|---|
| `branch` | a branch chosen uniformly among the N; its own row | everything under test |
| `trunk-id` | trunk's root page, by id, no catalog | **nothing**: the control |
| `trunk-cat` | `get_row(TRUNK)` | catalog height only (its leaves are hot) |

**Counters.** New module `src/buffer/read_census.rs`, thread-local, observing only. It is thread-local because a
shared counter on the hit path is the wall D44, D51 and D58 removed. The counters, in order:

* `desc`: `read_leaf_for` calls;
* `att`: optimistic attempts;
* `opt`: `read_page_optimistic` copies;
* `omiss`: copies that found the page not resident, or torn;
* `walk`: B-link right-walk hops;
* `lat`: latched fallbacks;
* `fetch`: `fetch_page` calls;
* `fault`: pages read from the file, i.e. **buffer-pool misses**;
* `hop`: range-scan leaves after the first.

**The path (READ).**

* `TableBranchCatalog::get` = `core` (one `tree.search` of `keys::record`) + `hydrate` (one `tree.range_scan` over
  `keys::arenas_of`, plus `envelope_bytes`, one `tree.search` of `keys::envelope`).
* Each of the three enters `BPlusTreeManager::read_leaf_for`. That descent is optimistic (D58). If any page on the
  path is not resident under its page-table hint (`read_page_optimistic` → `None`), it restarts from the root, up
  to `RESTARTS = 16` attempts. After that it falls back to `read_leaf_for_latched`, which faults pages in through
  `fetch_page`.
* The hint can also be absent for a page that IS resident, when a mirror-slot collision took it
  (`PageTable::lookup`: "None means ask the map, never not resident").
* Every `BufferPoolManager::new` has `MAX_BUFFER_POOL_PAGES = 1024` frames. The branch catalog has its own pool.
  So there are two 4 MiB pools, and neither grows with N.

**Sizes (INFERRED).**

* D61 measured 334 catalog bytes per branch, so the catalog has about N/12.3 pages and fits its pool up to
  N ≈ 12,500.
* The data pool holds about 1,000 branch pages alongside trunk's and the SQL catalog's.
* Internal fanout is about 90–185. Tree height h is therefore 2 up to N ≈ 2,000, 3 up to about 2–4 × 10^5, and
  4 at 10^6.

| # | prediction | class | basis |
|---|---|---|---|
| P1 | `c.desc` = **3.000** per branch read at every N | FLAT | READ: `get` = `core` + `hydrate` |
| P2 | on every arm-row where `c.att` = `c.desc`: `c.opt` = `c.desc`·h + `c.walk` **exactly**, with h walked independently from the root | identity | READ: `descend_optimistic` reads one page per level, plus one per hop |
| P3 | `c.walk` ≤ ~1.02, flat. The envelope search looks up an ABSENT key, and every branch's lookup lands on the same span-boundary leaf (0 or 1 hop). About 1% of arena entries open a new leaf | FLAT | READ: `tag::ENVELOPE` span is empty; INFERRED 1% |
| P4 | `c.hop` ≈ 0.01, flat | FLAT | INFERRED, the same 1% |
| P5 | `d.fetch` = **1.000**. `d.fault` ≈ 0 up to N ≈ 1,000, then ≈ 1 − ~1,000/N, saturating at 1 | SATURATING | READ: `CowTree::get` from a leaf root = one `read_page` |
| P6 | `c.fault` ≈ 0 up to N ≈ 12,500, then rising to **1.8–3.0** at 10^6. The record leaf and the arena leaf are random; the envelope leaf, the root and most internals stay hot | SATURATING | INFERRED |
| P7 | at T=1, `c.att` = `c.desc` + 15·`c.lat` **exactly**. `c.lat` ≈ cold descents per read, about 2 at 10^6. So `c.att` → ~33 and `c.opt` → ~33·h ≈ 130, against 3h + walk ≈ 7–13 while resident | STEP ×~10, then flat | READ: `read_leaf_for`, `RESTARTS` |
| P8 | `trunk-id`: every `c.*` = **0**, `d.fetch` = 1, `d.fault` = 0, at every N | FLAT | by construction |
| P9 | `trunk-cat`: `c.desc` = 3, `c.att` = 3, `c.opt` = 3h + walk, `c.fault` = 0 | log-N steps | READ |

**So, per read:**

* descents stay FLAT;
* pages touched grow as O(log N), from the tree height, times a bounded constant that jumps by up to ×16 for a
  descent whose leaf is cold;
* misses are bounded: at most ~3 catalog misses and 1 data miss;
* **there is no term in N.**

The source predicts neither "flat" nor plain "log N". It predicts log N with a one-time multiplicative step at the
pool knee. The step comes from the D58 restart loop, not from I/O.

⚠ READ, and recorded as a finding rather than a prediction: the doc on `read_leaf_for` says "on any page that is
not resident under its hint, the latched path answers". The code answers only after 16 full optimistic attempts.
Each attempt re-copies and re-deserialises the resident prefix of the path. P7 is the cost of that, and this arm
measures it.

**ns/read: the slope class, with a band.** The counters are the primary evidence, because they do not depend on
load. ns is secondary, and the control guards it.

* **CLASS: BOUNDED STEP, not O(N).** On the saturated segment (every checkpoint with `d.fault` ≥ 0.9, about
  N ≥ 16,000), the log-log slope of branch ns/read against N is **< 0.3**. For comparison: O(N) gives ~1.0, O(√N)
  gives 0.5, and one height step gives ≤ ~0.2.
* **Knee magnitude (INFERRED):** ns/read(10^6) / ns/read(250) at T=1 lies in **[4, 25]**. P7's re-copies dominate
  it, not `pread`: 48 GiB of RAM keeps every file in the page cache.
* **Resident magnitude (INFERRED):** branch ns/read at N = 250, T=1, lies in [5, 60] µs. D40's note measured a
  hydrate at ~24 µs/row and a record descent at ~5 µs.
* **T=8 (direction only):** the T=8/T=1 latency ratio is larger at 10^6 than at 250. Each fault takes the global
  `page_table` write lock, `arc_cache` and `in_transit`, and each latched descent takes the root's latch stripe.

## 3. Arm 3: the full production open

**Protocol.** At each checkpoint N the parent:

1. counts live arenas and live branches independently (`store.live_arenas().len()`, `branches.live_count()`);
2. closes cleanly with `OpenDatabase::close`: lease stop, `txn.checkpoint`, then `store.checkpoint`;
3. spawns ITSELF as a fresh process (`--open-only <db> <arenas> <live>`).

The child runs `open_database` under one timer and records the per-phase timings it returns. It then waits for the
lease thread's first pass to finish, closes cleanly, and prints one tagged line. The parent parses that line,
checks it, and reopens in-process to continue. The in-process reopen is timed too, as a warm-process comparison.

The OS page cache is warm in both processes, because purging it needs root. Every result is therefore a
**warm-cache** result.

**Counters.**

* `sweep_visits` and `sweep_descents`: already in `TwoTierReaper` (D40's instruments).
* New `open_sweep_visits`: the synchronous sweep's share, recorded inside `resume_interrupted_reaps`, where nothing
  else can be sweeping.
* New `LeaseStats::finished`: passes that reached the end of `scan_once`, the D88 orphan sweep included.
* The census, on the opening thread.

**The path (READ).** `run_cli`'s steps, in order:

1. `DbLock::acquire`;
2. pool + WAL + `recover`;
3. `Catalog::open`;
4. `TableBranchCatalog::default_for_database` (→ `open_sidecar`);
5. `ArenaPageStore::reopen_from_checkpoint`;
6. `DurableEffectLog::default_for_database` (the `.tel` replay);
7. `AgentRuntime::reopen_with_storage` (one `get(TRUNK)`, one page read);
8. `with_durable_provenance` (the `.provenance` replay);
9. `LeaseThread::start`, which calls `resume_interrupted_reaps`, which ends, unconditionally, in
   `collect_orphaned_extents`: one `extent_is_collectable` (one `get_raw` = 3 B+tree descents) per entry of
   `store.live_arenas()`.

`live_arenas()` is sorted by arena id, and ids are handed out in near fork order, so the sweep walks the RECORD and
ARENA spans nearly sequentially.

⚠ READ, and a prediction of this file: **every open sweeps TWICE.** `resume_interrupted_reaps` calls
`collect_orphaned_extents` directly. It never stamps `last_orphan_sweep_ms`, which starts at `ORPHAN_SWEEP_NEVER`.
The lease thread runs `scan_once` as soon as it is spawned, before its first wait. `scan_once` ends in
`collect_orphans_if_due`, whose gate is open at `NEVER`. So the full sweep runs again: on the lease thread, outside
the statement lock (D88), and against the same catalog pool.

| # | prediction | class | basis |
|---|---|---|---|
| R1 | `open_sweep_visits` = live arenas = **N + 1** (one per branch, plus trunk's), exactly | LINEAR, exact | READ |
| R2 | `sweep_descents` over the open = `open_sweep_visits` (one `get_raw` per visit). Census `c.desc` on the opening thread = 3·visits + a constant ≤ 12 | LINEAR, exact | READ: `extent_is_collectable` |
| R3 | first-pass visits (`sweep_visits` after the first pass, minus `open_sweep_visits`) = **N + 1** again | the double sweep | READ, above |
| R4 | freed at open = **0**: a cleanly closed database has no orphans | — | READ |
| R5 | `lease_start` ms is LINEAR in N: slope over [64,000 … 10^6] in [0.8, 1.2]. At 10^6 it lies in **[10, 90] s** (INFERRED: ~30 µs per `get_raw`, from D40's note, × 10^6) | LINEAR | INFERRED |
| R6 | `arena` ms is linear (D65 run 4 measured b_hi 1.10, 0.52 s at 10^6 with one extent per branch) and ≤ 1/10 of `lease_start` at 10^6 | LINEAR | MEASURED BY D65, restated |
| R7 | `lock`, `recover`, `sql_catalog`, `branch_catalog`, `effect_log`, `runtime`, `provenance`: FLAT, each ≤ 50 ms at every N. WAL, `.tel` and `.provenance` are empty or checkpointed here, so this proves their fixed cost only | FLAT | READ + INFERRED |
| R8 | total open is dominated by `lease_start`: ≥ 80% at 10^6. The log-log slope of total over [64,000 … 10^6] is in [0.8, 1.2] | LINEAR | INFERRED |
| R9 | the first pass finishes ~`lease_start` after the open returns, because it repeats the same work | — | INFERRED from R3 |
| R10 | cold catalog pages hit the sweep only sequentially: census `c.fault` per visit ≈ 1/28 + 1/97 ≈ **0.05**, and `c.att` per visit ≈ 3 + 15 × 0.05 ≈ 3.7. If the arena order were random it would be ~2 faults and ~33 attempts per visit | FLAT per visit | INFERRED from the sort |

**What other outcomes would mean:**

* `open_sweep_visits` ≠ live arenas: the sweep visits something other than the extents. Nothing in the column can
  be attributed until that is explained.
* First-pass visits = 0: the gate IS stamped by the open sweep, and R3 is wrong. Say so where R3 was written.
* `lease_start` flat: the sweep is not O(N), which contradicts R1. Check R1's counter first.
* ms per visit growing with N (slope > 1.2): the sweep's cold pages are not sequential, and P7's restart
  amplification applies. Compare R10.
* A flat phase in R7 growing: a replay this workload was supposed to leave empty is not empty. Name the file.
* Parent reopen ≪ child open: process warmth (allocator, TLB, code) is a large share. Quote the child, which is the
  restart.

## 4. Guards: the harness prints NOT A RESULT and exits 2 on any failure

**Arm 1.**

* G1: the recorded branch count = N, and `live_count` = N + 1.
* G2: every read returned the row that branch wrote (`[BigInt(id)]`, trunk `[BigInt(-1)]`), and every `trunk-id`
  read passed its checksum.
* G3: the census fired: `branch` `c.desc` > 0 and `d.fetch` > 0.
* G4: `trunk-id` has every `c.*` = 0 and `d.fetch` = reads, exactly.
* G5: `trunk-id` `d.fault` = 0 at that N; otherwise that N's ns columns are NOT A RESULT.
* G6: P2's identity holds.
* G7: `trunk-id` ns/read, per thread count, max/min across N ≤ **1.5**; otherwise every ns column is NOT A RESULT.

**Arm 3.**

* H1: the child exits 0 and prints exactly one parseable line.
* H2: R1's equality against the parent's independent count.
* H3: the reopened database has `live_count` = N + 1 (D65's positive control, untimed).
* H4: freed at open = 0.
* H5: the first pass finished within its bound (60 s + 10 × `lease_start`); otherwise the first-pass columns are
  NOT A RESULT.

**Both.** Fewer than two checkpoints is not a curve.

Every guard has a forced-fire mode, `CURVE_FIRECHECK=<mode>`, which must make that guard, and only that guard, print
NOT A RESULT:

* `wrong-page` (G2);
* `census-off` (G3);
* `control-catalog` (G4);
* `control-cold` (G5);
* `wrong-height` (G6);
* `control-drift` (G7);
* `wrong-arenas` (H2);
* `wrong-live` (H3).

The library counters have their own forced fires in `tests/read_vs_n_census.rs`.

## 5. The run owed

Quiet mode: none of this has run. The run: checkpoints 250, 1000, 4000, 16000, 64000, 256000, 1000000; 8 fork
threads; an 8 GB budget. The exact commands are in `artie-research frontier/lane_read_vs_n.md`.

## Amendments (append-only)

**A1, 2026-09-24. Written after the code (`5bd7dfb`) and before any build or run. It changes no prediction; it records
where the implementation's protocol differs from the text above.**

1. The child takes one argument, `--open-only <db>`. The parent keeps its independent counts (live arenas, live
   branches) and makes the H2/H3 comparisons itself. The `wrong-arenas` and `wrong-live` fire modes offset the
   parent's expected value.
2. **R2 as checked.** `sweep_descents` is shared with the lease thread's first pass, so the open's share cannot be
   read off it. The child reports `descents_total` and `visits_total` after the first pass, and R2 becomes
   `descents_total = visits_total` (one `get_raw` per visit, over both sweeps). The census is per thread, so its
   `c.desc` over the open covers the opening thread only, which is the synchronous sweep: R2's "3·visits + ≤ 12"
   applies to it unchanged.
3. The D65 reload control (merged from `d65-reopen-curve`) compares the image's `current` count with
   `store.current_arena_count()` in the production layout, not with N. Trunk writes here, and every restart
   clears `current` (`load_state`: "Never resume filling a restored extent"), so N would make the control fail
   for a reason unrelated to the load. The historical path still passes N.
4. Before timing, arm 1 flushes with `TxnManager::checkpoint`: WAL, then the whole pool, then fsync. Reads never
   dirty a page, so no timed window pays a write-back.

**A2, 2026-09-24, before any build or run.** The first checkpoint is **256**, not 250. The fork phase forks
`per × threads` branches with `per = segment / threads`, so with 8 threads a target of 250 would have measured N =
248 and labelled it 250. The run's checkpoints are therefore 256, 1000, 4000, 16000, 64000, 256000, 1000000; each
segment divides by 8. Every "250" in sections 2 and 3 now reads "256", and no prediction moves: 256 sits in the
same regime (all resident, h = 2). The `control-drift` fire mode spins 10·2^checkpoint µs per read rather than
2^checkpoint µs, so it clears G7's 1.5 band by construction between the first two checkpoints.

**A3, 2026-09-24. ARM 2, MERGE vs N and vs M: its pre-registration, written from the lead's resent brief BEFORE any
arm-2 code. Section 0's "brief did not arrive" is superseded by this entry. Line numbers are `9aa6968`'s, from
`git show 9aa6968:src/agent_sql/runtime.rs | grep -n`, and each is paired with a symbol.**

*What this measures, and what it does not.* This is `9aa6968`'s behaviour, the **BEFORE arm**. Two of the walls on
this axis have fixes on other branches: wall #17 (DIFF) on `wall17-diff-scan`, and wall #18 (REVERT's rescan) on
`wall18-revert-rescan`. Neither branch is merged here. This arm issues neither DIFF nor REVERT:

* DIFF is not on the MERGE path. Its only caller is `dispatch.rs:465` (`runtime.diff`). Wall #17's before-curve is
  D191's (FAN-QUEUE #6), and this arm does not duplicate it.
* What this arm does measure is #18's side note: `highest_applied_seq` (`:6310`), which `publish_evaluation_as`
  calls once per merge (`:5165`, `fresh_reservation(highest_applied_seq(&state.applied), base)`).
* Wall #12 (the per-cell rescan) is fixed at `9aa6968` by D86's index. `concurrent_op` reads it (`:5342`,
  `state.applied_at_cell`), so this arm sees wall #12's AFTER state.

*Whole-log passes on the merge path, found by READING `merge` (`:3387`) → `evaluate_merge` (`:4280`) →
`publish_evaluation_as` (`:4905`) → `record_applied` (`:5362`) → `attest_merge` (`:3711`) → `seal` (`:5654`):*

* **exactly one**: `highest_applied_seq`, `applied.iter().map(|a| a.seq).max()`. It visits every entry, because
  `max` consumes the whole iterator.
* Every other access to a growing collection is a keyed lookup or an insert: `workspaces.get`,
  `captures.get` / `.entry` (`:5477`), `versions.insert` (`:5410`), `merges.insert` (`:5482`),
  `published_txns.insert` (`:5681`), and `applied_at_cell`, D86's index.
* The pass over `state.workspaces` in `capture_is_protected` is a `debug_assert`, and `seal` reaches it only for an
  UNpublished branch (`:5713`).
* The other passes over `applied` in the file are cherry-pick (`RuntimeCherryLog::project`), REVERT (`undo_txn`)
  and DIFF (`:3207`). None is on this path.

*The workload.* A SQL table `m (id INTEGER NOT NULL, v INTEGER)` is created on trunk once, with a plain session. A
merge cycle, run through `execution::executor::run` under the statement lock, as `run_cli`'s `execute_sql` does:

1. `BEGIN AGENT SESSION AS 'mK';`
2. `INSERT INTO m VALUES (id, id);`, with an id never used before, so every cycle writes a NEW row and a new cell;
3. `MERGE;`, the ONLY statement inside the timer and the counter brackets.

The cycles run sequentially on one thread.

*Why an INSERT.* The publish step issues `Stmt::Insert` for a new row (`PendingWrite::Insert`, `:6677`). An UPDATE
would be `build_scan`'s O(table) seq scan (D176/D178), a known wall on a different axis, and would bury this one.

*`State` is in memory.* `reopen_with_storage` builds `State::default()` (`:1408`), so `applied`, `captures` and
`merges` are EMPTY after every open (READ). M therefore counts merges since the last open.

*The two separable axes:*

* **(i) N grows, M fixed.** At every checkpoint, K = 64 cycles (`CURVE_MERGE_K`) run immediately after an open,
  so `applied` starts at 0. With the restart arm on, the open is the restart arm's reopen; otherwise the merge arm
  closes and reopens first. The N live idle branches are the fork phase's. They live in the CATALOG and the ARENA
  (they persist) and are NOT open sessions in the runtime's `State`, which a restart empties anyway. **This axis is
  "N live written branches", not "N open sessions".** W4 measured the session axis.
* **(ii) N fixed, M grows.** After the last checkpoint, reopen, then run cycles up to M = 256, 1024, 4096, 16384
  (`CURVE_MERGE_M`). Each target reports the last 64 merges before it.

*Per-merge counters.* All are integers, bracketing `MERGE;` alone:

* `V_hi`: entries `highest_applied_seq` visited. A new observing static, `MERGE_APPLIED_VISITED`, incremented by
  the entries actually iterated.
* `V_cell`: entries read through D86's index in `concurrent_op`. New static, `MERGE_CELL_INDEX_VISITED`.
* `applied`, `captures`, `merges`, `versions`, `workspaces` before and after, from a new observing accessor,
  `AgentRuntime::state_sizes`.
* `attested_len` (existing).
* `seq_scan_counters` tuples and `index_scan_counters` (existing, D176).
* `wal::log::FSYNC_CALLS` (existing).
* The read census on the merging thread: `c.desc`, `c.att`, `c.fault`.

| # | per-merge prediction | axis (i): N grows | axis (ii): M grows | basis |
|---|---|---|---|---|
| Q1 | `V_hi` = `applied` length before that merge, **exactly** | flat: batch mean a(K−1)/2 at every N | **LINEAR**: a·M; the total over M merges is Θ(M²) | READ `:6310`, `:5165` |
| Q2 | Δ`applied` per merge = a, a constant (INFERRED a ∈ {1, 2}: one op per written cell or per row) | flat | flat | READ `push_applied` `:5392` |
| Q3 | `V_cell` = 0: a new row's cell has no history in D86's index | flat 0 | flat 0 | READ `:5342` |
| Q4 | `captures` +1 per merge (created at `begin_session` `:1783`, kept by `seal` for a published branch) | +K per batch, reset at open | **LINEAR** in M | READ `:872`, `:5713` |
| Q5 | `merges` +1 and `versions` +1 per merge; `workspaces` returns to its pre-merge value | same | LINEAR / LINEAR / flat | READ `:5482`, `:5410`, `:5685` |
| Q6 | `attested_len` +3 per cycle (fork `:1674`, merge `:5241`, reap `seal`) — wall #19 | +3K per batch | **LINEAR** in M | READ |
| Q7 | seq-scan tuples per merge = **0** (Insert publish; the pk point lookup is an index scan, D176) | flat 0 | flat 0, although the table grows by one row per merge | READ `:6677` + D176 |
| Q8 | index scans, FSYNC calls and census `c.desc` per merge: constants | flat | flat | READ |
| Q9 | census `c.fault` on the merging thread ≈ 0 at every N: the catalog keys a merge touches are its own new record and trunk's newest child entries, which are hot, unlike arm 1's random keys | flat ≈ 0 | flat ≈ 0 | INFERRED from `keys::child` ordering |
| Q10 | ns/merge | **FLAT**: ns(10^6)/ns(1000) in [0.7, 1.5]; no N-dependent pass on the path | **flat within noise to M = 16,384**: the one growing term is `V_hi` × ~1–3 ns per entry (INFERRED), ≤ ~50 µs against a fsync-bound fixed cost of ms. The wall shows in the COUNTER at this M, not the clock. Log-log slope of ns over M in [−0.1, +0.2] | INFERRED |

*What other outcomes would mean:*

* `V_hi` ≠ `applied` before: the counter is mis-wired, or the call runs more or less than once per merge. Nothing
  in the arm can be read until that is explained.
* `V_cell` > 0: the workload wrote a cell with history, and the fixture is wrong.
* Seq-scan tuples > 0: some step reached `build_scan`, and the tuples will grow with the table, which grows with
  M. That is a second M-dependence, and it is D178's.
* `captures`, `merges` or `attested` flat: something prunes them, which contradicts the field docs. Say so where
  those docs are.
* ns/merge growing in M faster than `V_hi` × 3 ns explains: another growing term. Compare `captures` and
  `attested`, and profile before naming it.
* ns/merge growing in N on axis (i): a catalog cost on the merge path that the source reading missed. `c.fault`
  and `c.desc` say whether it is residency or count.

*Guards* (NOT A RESULT, exit 2), each with a forced-fire mode:

| guard | condition | fire mode |
|---|---|---|
| M1 | every `MERGE` reported `applied_to_target` (a quarantined merge returns `Ok` having done nothing, the D68 lesson) | `merge-quarantined` quarantines the branch first |
| M2 | axis (i)'s batch started at `applied` = 0 | `wrong-start` |
| M3 | Q1's identity at every merge | `wrong-visits` |
| M4 | Δ`applied` constant across every merge of the run | `wrong-delta` |
| M5 | live branches after each batch = before it | `wrong-live-merge` |

`wrong-start`, `wrong-visits`, `wrong-delta` and `wrong-live-merge` offset the expected value, which tests the
comparison. `merge-quarantined` is a real injection. The new statics get a forced fire of their own in
`tests/read_vs_n_merge_census.rs`, a separate test binary, so no parallel test can move them.

**A4, 2026-09-24, before any build or run. The consolidation the lead decided: D204's one open path.**

`read-vs-n` now contains `rollback-index-orphan` @ `d7891d5` (merged at `003e50d`). The file, the pool, the WAL,
`recover`, the catalog and the index rebuild run in exactly one function, `wal::recovery::open_recovered`, and
`tests/open_path_allowlist.rs` fails if they run anywhere else. `cli::open_database` calls it and keeps only the
half it does not cover:

* the branch catalog;
* the arena;
* the `.tel` log;
* the runtime;
* the `.provenance` log;
* the reaper;
* the lease start.

Consequences for arm 3, and none changes a prediction's class or band:

1. Steps 1–4 are timed INSIDE `open_recovered` (`BootTimings`: `files`, `recover`, `catalog`, `rebuild`,
   observing only) and handed back in `OpenedDatabase::timings`. R7's `recover` and `sql_catalog` are now
   `boot.files + boot.recover` and `boot.catalog`, and the child prints all four. R7's flat prediction covers
   all four.
2. **`rebuild` = 0 at every checkpoint (READ).** `open_recovered` rebuilds only `if recovered`, and the parent
   closes cleanly (`TxnManager::checkpoint`) before every child open, so the log has nothing to replay. The
   rollback-index-orphan report says every restart with a non-empty log pays an O(rows) rebuild. **That is a
   crash-restart cost, and this arm does not measure it.** It measures clean restarts only. A crash arm would
   `kill -9` the parent instead of closing it; it is not built.
3. The per-target suite now contains that branch's tests: 2611 run, passed=2609 failed=2 at `d7891d5` by its own
   report, plus this lane's 5.

**A5, 2026-09-24, before any build or run. The lead's consolidation, applied in full. A4's "`open_database` keeps
the second half" is WITHDRAWN.**

`cli::open_database` no longer exists (`61ddba9`). `read-vs-n` contains `rollback-index-orphan` at its tip
`00f4c39` (merged at `b054ad5`). The open path is the shipped one:

* `run_cli` → `open_recovered` (D204), then `run_cli`'s own branch catalog, arena, `.tel`, runtime, `.provenance`,
  reaper and lease start.

*Arm 3's protocol, as it now stands:*

1. **The measured open is `run_cli` itself.** The restart child calls it on the database, with stdin held by the
   parent.
   * Timings are observing-only, taken where each step runs. `open_recovered`'s steps are timed inside it
     (`BootTimings`: `files`, `recover`, `catalog`, `rebuild`). The rest are timed inside `run_cli`
     (`cli::OpenTimings`), which leaves a `cli::last_open_report()` behind.
   * The only code shape changed in `run_cli`: the runtime builder chain is split so that `.provenance` can be
     timed on its own.
2. **The first lease pass (R3, now ledger D209) is inside the run by construction.**
   * A helper thread in the child watches the process-wide `lease_thread::PASSES_FINISHED` and says so on stderr.
     `LeaseStats::finished` is withdrawn in its favour, because `run_cli`'s lease thread is private to it.
   * Only then does the parent send `.exit`, and `run_cli` shuts down as it does for a user.
   * `sweep_visits_at_close` therefore holds both sweeps, and R3's first-pass visits are
     `sweep_visits_at_close − open_sweep_visits`.
   * `first_pass_us` is the time from the call into `run_cli` to the end of that pass, less the open's `total`.
3. **R7, amended (the step names change).** The flat steps are:
   * `lock`;
   * `boot.files`, `boot.recover`, `boot.catalog` and `boot.rebuild`. Recovery and the rebuild are now ONE
     function's steps. `rebuild` is 0 on every clean restart, and covers the rebuild whether it came from the log
     or from the stale-index marker;
   * `branch_catalog`, `effect_log`, `runtime`, `provenance`.

   Each is ≤ 50 ms at every N. `arena` (R6) and `lease_start` (R5) keep their linear predictions.
4. **The parent's own opens are setup, not results.**
   * The database is CREATED by `run_cli` in a child, so its layout is the binary's. The parent then reopens it
     with a copy of `run_cli`'s reopen half over `open_recovered`, because `run_cli` hands out no handles and arms
     1 and 2 need them.
   * That copy is never timed as a result. Its drift blind spot is written on it: `HarnessDb` in the harness.
   * A drift cannot mismeasure the restart, which is always `run_cli`. It could make arms 1 and 2 run on a runtime
     wired differently from the binary's.
5. H3's live count is read after `run_cli` has returned and released the lock, from a read-only
   `open_sidecar` of `{db}.branchcat`. It is untimed and O(N).

*Paper-facing scope (F4 is ledger D212).* `State` is rebuilt empty at every open, so walls #12, #17 and #18's Θ(M²)
over M merges is **per process lifetime**, not per database. M in arm 2 counts merges since the last open, and every
axis-(ii) number is read that way. Separately, REVERT refuses a merge from before a restart (D212), which is safe.

**A6, 2026-09-24, before any build or run. A5's protocol is WITHDRAWN, and A4 stands again.**

A5 followed a lead message ordering `cli::open_database` dropped. That message was written against the lane report
at `e6773e8` (06:03:57Z): its "proposes calling `open_recovered` inside it" is that report's §7. At 06:09:13Z the
lead's own commit `23994bd` (artie-research) VERIFIED `f298d9e`: `open_database` calls `open_recovered`, every other
`Catalog::open(` sits under `#[cfg(test)]`, and the tripwire's rules hold. That commit **accepted the deviation**.

`5678e28` reverts A5's code change (`61ddba9`). The code stands as follows:

* `run_cli` → `cli::open_database` → `wal::recovery::open_recovered` (steps 1–4, timed inside it as `BootTimings`)
  → the second half, written once;
* the restart child calls `open_database`, which is what `run_cli` calls;
* the parent does the same, so no harness copy of production code exists;
* `LeaseStats::finished` is back, and `lease_thread::PASSES_FINISHED` and `cli::last_open_report` are gone.

The tip merge (`b054ad5`) stays.

*Still true from A5, and kept:*

* **item 3**, R7's step names: `lock`, `boot.files`, `boot.recover`, `boot.catalog` and `boot.rebuild` (0 on every
  clean restart, and covering a stale-index rebuild), then `branch_catalog`, `effect_log`, `runtime` and
  `provenance`. All flat, each ≤ 50 ms.
* **the paper-facing scope paragraph** (D212: Θ(M²) is per process lifetime).

*The follow-up the lead recorded, unchanged here:* pgserver calls `open_recovered` but spells out the second half
itself, so that half lives in two places. Making pgserver share `open_database`'s second half is a later lane.

**D209 (ledger), 2026-09-24, appended on branch `d209-open-sweep-once` before any build or run. It changes no
prediction for `7a68d8b`. It records what R3 and R9 become on a tree that contains the D209 fix (`4da75e4`).**

R3 predicts the double sweep, and for `7a68d8b` it stands: that run is D209's BEFORE. `4da75e4` stamps the orphan
sweep's cadence when the open's sweep finishes (`LeaseThread::start` → `TwoTierReaper::orphan_sweep_finished_at`,
read on `scan_once`'s clock after `resume_interrupted_reaps` returns). On any tree that contains it:

* **R3 becomes: first-pass visits = 0 at every N, exactly**, because this harness's leases are `LeaseDeadline(u64::MAX)`
  and the first pass reaps nothing. A pass that reaps still adds its reaps' narrowed-sweep visits. §3's line for that
  outcome ("the gate IS stamped by the open sweep, and R3 is wrong") then describes the fix, not an error in R3.
* **R9 no longer holds.** The first pass does no O(N) work, so `1st-pass ms` does not grow with N.
* **Unchanged:** R1, R4–R8 and R10, because the open's own sweep is untouched; R2 as A1 checks it
  (`descents_total = visits_total`), now over one sweep; H5, because the pass still finishes and only skips the sweep.

**A7, 2026-09-24, before any build or run. The fresh-context review of `7a68d8b`
(`artie-research frontier/read_vs_n_review.md` @ `e9fee7b`) found run (h) unsound as configured. Each finding and what
it changes:**

1. **F1: every restart child in run (h) recovers and rebuilds.** Lead-verified; now ledger **D216**, and lane
   `d216-clean-restart` fixes it in recovery. It is not fixed here. The mechanism, READ by the review:
   * `TxnManager::checkpoint_locked` truncates the WAL, then re-appends `replay_schema()` and `replay_runs()`.
   * So the parent's clean close (`txn.checkpoint`) leaves one `Ddl` per table and one `RunIdentity` per run declared
     in that process.
   * The child's `recover` returns `true`, and `open_recovered` rebuilds every index.

   **A4 item 2 and A6's "rebuild = 0 on every clean restart" are WITHDRAWN.** They are false whenever arm 2 runs,
   because arm 2's `CREATE TABLE m` and its one-run-per-session merges put those declarations in the log.

   Pre-registered in their place, as a **before/after pair like D209**:

   * **BEFORE D216** (this branch, `9aa6968` + `00f4c39`):
     * `rebuild_us > 0` at EVERY checkpoint with `merge` on. It is O(rows): `rebuild_indexes` over every table's
       heap, plus the checkpoint fsync. Table `m` holds K·(i−1) rows at checkpoint i, printed as `m_rows`. At K = 64
       and 7 checkpoints that is at most 384 rows, so the fsync should dominate. Band (INFERRED): 0.5–200 ms per row.
       The class claim is "nonzero at every checkpoint"; the magnitude is secondary.
     * `recover_us > 0`: replay of the re-appended records. The count is printed as the child's WAL bytes.
   * **AFTER D216:** `rebuild_us = 0` and `recover_us ≈ files-scale` on every clean restart. This harness re-run on
     D216's fix is the after arm.
   * With `merge` OFF (`CURVE_ARMS=read,restart`), `rebuild_us = 0` BEFORE D216 too: nothing declares a table or a
     run (the review, INFERRED). That is the control for the pair.
2. **F2: the parent dropped measured fields.**
   * The parent now echoes the child's `RESTART_RESULT` line verbatim, as a `RESTART-RAW` line.
   * The `RESTART` row adds `lock`, `files`, `sql_catalog`, `rebuild`, `runtime`, `descents_total`, `wal_bytes`,
     `tel_bytes`, `prov_bytes` and `m_rows`.
   * Nothing the child measured is dropped.
   * The printed "recover ms" stays `boot.recover` alone. A4 item 1's `files + recover` is now two columns.
3. **F3: §1 is AMENDED rather than splitting the run, and why.**
   * Arm 2 writes `.tel` (one effect frame per staged statement) and `.provenance` (one interned run per session),
     and arm 3 replays both.
   * §1's "Nothing in this workload writes `.tel` or `.provenance` frames" is **false whenever `merge` is on**.
   * The arms stay together in run (h), for two reasons:
     * (a) D216's before/after pair needs the table and run declarations that only arm 2 produces;
     * (b) each replay term can be attributed to its own input instead of to N. Every `RESTART` row prints the
       `.tel`, `.provenance` and `.wal` byte sizes the child is about to replay.
   * R7's `effect_log` and `provenance` predictions become: **linear in their file's bytes, and ≤ 50 ms at these
     sizes**. The files grow by one batch of K sessions per checkpoint, so their dependence on checkpoint index is
     the harness's own writing, and the row says so beside it.
   * Arm 3's pure N-curve (`lease_start`, the sweep) is unaffected: it reads live arenas, which merges do not grow.
     Merged branches are reaped, so M5 and H2 hold them flat.
4. **F4: A3's enumeration missed one O(M) pass.** `TxnManager::commit` checkpoints automatically every
   `FERRODB_CHECKPOINT_INTERVAL` (default 256) commits. The checkpoint re-appends every run declared in this
   process, which on axis (ii) is one per merge, so it is O(M), plus a pool flush and fsyncs. It runs inside the
   timed `MERGE;` of every 256th merge. The axis-(ii) targets are multiples of 256, so each block's last merge pays
   it. Therefore:
   * per merge, the harness records `db.txn.retained_runs()` before and after, and a `checkpointed` flag. The flag is
     true when `commits_since_checkpoint` did not rise across the merge: a commit always raises it, and a checkpoint
     resets it to 0;
   * each `MERGE` row prints `retained runs` and `ckpts` (checkpointed merges in the block);
   * **Q10 is judged on the MEDIAN ns/merge**, and the summary's axis-(ii) slope is computed from the median. The
     mean is printed beside it, not judged.
   * A3's enumeration now reads: one whole-log pass per merge (`highest_applied_seq`), plus an O(runs retained) pass
     on every 256th merge (the auto-checkpoint). Predicted: `ckpts` = 1 per axis-(ii) block when 64 ≤ 256, `retained
     runs` = M exactly (one run per cycle), and the median flat within noise.
5. **F5: fire modes.** "Every guard has a forced-fire mode" was false for G1, H1, H4 and H5, and H2's mode injected
   only at the comparison. Added, each IN PATH (a real state change the measured code sees):

   | mode | guard | injection |
   |---|---|---|
   | `extra-branch` | G1 | a real fork at the first checkpoint that `branches` never records; live = N + 2 from then on |
   | `child-locked` | H1 | the parent HOLDS `{db}.lock` while the child opens; the child's `DbLock::acquire` refuses and it exits non-zero |
   | `wrong-sweep-read` | H2 | the child reports the reaper's REAL `sweep_visits()` read after the first pass (~2(N+1)) in place of `open_sweep_visits()` |
   | `orphan-extent` | H4 | before each restart, a real fork claims an empty extent with `arena_for` and is then marked `Reaped` via `set_state` (the D40 crash-orphan shape); the child's open sweep frees it, so freed = 1 |
   | `no-cluster-time` | H5 | at the LAST checkpoint only, the child joins a cluster with no applied `LeaseTick` (`ClusterScope::joined`); its first pass refuses before the orphan sweep, so `LeaseStats::finished` never rises |

   Expected extras: `extra-branch` fires G1 at every checkpoint; `child-locked` also fires "arm 3: one point is not a
   curve". The comparison-only modes (`wrong-page`, `census-off`, `wrong-height`, `wrong-arenas`, `wrong-live`,
   `wrong-start`, `wrong-visits`, `wrong-delta`, `wrong-live-merge`) are kept and are labelled comparison checks:
   they prove that the comparison and its NOT A RESULT line work, and nothing more.
   `tests/read_vs_n_lease_census.rs` (its own binary, one test) tests `open_sweep_visits` and `LeaseStats::finished`:
   * `open_sweep_visits` equals an independently counted `live_arenas()`, and stays unchanged when a later sweep
     moves `sweep_visits`;
   * `finished` stays 0 while a pass refuses (no cluster time), and rises once a standalone pass completes.
6. **Scope notes from the review, recorded:**
   * Arm 3 measures the **CLI's** restart. pgserver's second half differs: `MemEffectLog`, no durable provenance,
     no `txn.checkpoint` at exit (review §1b).
   * Arm 1's T=8 rows read without the statement lock, which the shipped CLI and pgwire always hold. **T=8 is
     library concurrency and must never be quoted as production throughput.**
   * A6's "no harness copy of production code exists" is overstated: `exec_sql` re-spells `cli::execute_sql` for
     one statement.
   * R2's `descents_total` counts `extent_is_collectable` calls, not B+tree descents. The census `c.desc/v` is the
     real check.

**A8, 2026-09-24, before any build or run. H2's fire mode, re-aimed so that it discriminates on BOTH trees.**

*Why.* Lane D209 (`d209-open-sweep-once` @ `afe44d9`, landing AFTER this lane) stamps the open's sweep, so the
lease thread's first pass visits 0 arenas and `sweep_visits` after the first pass equals `open_sweep_visits`.
A7.5's `wrong-sweep-read` reports that counter in place of `open_sweep_visits`. On a tree with D209 the two numbers
are equal, so H2 could not fire and the fire would pass silently. **`wrong-sweep-read` is WITHDRAWN.** The harness now
refuses its name.

*The replacement, `extra-extent` (H2, in path).*

* After the parent counts live arenas for H2 and before its clean close, it claims ONE more real extent, for trunk,
  with `arena_for`.
* The parent has just reopened, so `current` is cleared ("Never resume filling a restored extent"). At the first
  checkpoint, trunk's one-page first extent is already full. Either way `arena_for` must claim a new extent.
* The harness asserts that it did: `live_arenas()` must be the count + 1, or the fire panics rather than pass
  unfired.
* The child's OPEN sweep, which D209 does not change, then visits one arena more than the parent counted, and H2 fires.
* The extent's owner is trunk, which is live. So H3 (live count), H4 (freed; an owner that is alive is never
  collectable) and G1 are untouched.
* It fires at every checkpoint. D209 changes only the FIRST LEASE PASS, and this fire never touches it.

*R3 becomes a before/after pair, like D216's.* BEFORE D209: first-pass visits = N + 1. AFTER D209: 0.

*Which tree each fire mode is proven on.* **None is proven yet: nothing has run (quiet mode).** FAN-QUEUE step (f)
proves them on `read-vs-n` at its tip, which is before both D209 and D216. After D209 and D216 merge, step (f) must be
re-run on the merged tree for the modes whose premise either fix touches. Each mode's sensitivity, INFERRED from the
fix descriptions:

| mode | guard | touched by D209? | touched by D216? | re-prove on the merged tree? |
|---|---|---|---|---|
| `extra-extent` | H2 | no: the open sweep is unchanged | no | yes, cheap |
| `orphan-extent` | H4 | no: the orphan is collected by the open sweep | no | yes, cheap |
| `no-cluster-time` | H5 | no: the refusal happens before the orphan sweep, in any pass | no | yes, cheap |
| `child-locked` | H1 | no | no | no |
| `extra-branch` | G1 | no | no | no |
| every comparison-only mode (A7.5) | — | no | no | no |
| the D216 control smoke (e2) and rebuild pair | A7.1 | no | **yes**: the AFTER half | yes: step (i) |

The lane report's FAN-QUEUE row carries this as its re-run instruction.

**A9, 2026-09-24, before any build or run. The re-review of `7a68d8b..c2d87ac` (artie-research
`frontier/read_vs_n_rereview.md` @ `70b9237`) returned SOUND-WITH-CAVEATS. Each caveat, and what changes:**

1. **Bands (C1, C3, C4, C9).**
   * **`boot.rebuild` has ONE band.**
     * A6's inclusion of `boot.rebuild` in R7's "flat, each ≤ 50 ms" is WITHDRAWN.
     * A7.1's band stands, with its unit: **0.5–200 ms per RESTART row (one child open)**, `merge` on, BEFORE D216.
       "Per row" means per child open, not per heap row.
     * The AFTER half, and the merge-off control, are judged by STATE: `recovered = 0` and `wal B = 24`, the
       header-only log (`HEADER_SIZE`, `wal/log.rs`). The timer is secondary: `rebuild_us < 1,000`.
   * **"files-scale" is now a number.** After D216, `recover_us ≤ 5,000` (5 ms) per restart (INFERRED: a stat and a
     24-byte header read). The BEFORE half is judged by state too: `recovered = 1` and `wal B > 24` in every
     merge-on row.
   * **The auto-checkpointing merge's own cost is printed (C1).**
     * Each MERGE row prints `ckpt ns`, the mean ns of the block's APPLIED, checkpointed merges ("-" when there is
       none), and `ckpt fsync`.
     * Pre-registered: **`ckpt ns − ns median` is linear in M**, the retained runs replayed. Its log-log slope over
       M ∈ [1024, 16384] is in [0.5, 1.2] (INFERRED: the replay appends M records before the flush and the fsyncs).
     * **Q10's median verdict is about the typical merge, not the amortized cost.** The amortized cost per merge is
       the median plus (`ckpt ns` − median)/256, which is arithmetic from printed fields. The summary prints it
       beside the median.
   * **§3's "parent reopen ≪ child open means process warmth" is RESTATED (C3).** Before D216, with `merge` on, only
     the child recovers and rebuilds; the child's own checkpoint empties the log, so the parent's reopen cannot.
     Warmth is therefore judged on `child total − boot.recover − boot.rebuild` against `parent total`, which the
     summary prints as `child net ms`. After D216 the raw totals compare.
   * **A7.3's "M5 and H2 hold them flat" is replaced by R1 (C9).** R1's printed `open visits` = N + 1 is the judge.
     H2 compares that count with the parent's own count; M5 compares live branches, not arenas.
2. **`orphan-extent` asserts its premise (C6).**
   * A restored extent's fill is unknown (`arena.rs` `load_state`). The open sweep's `resolve_fill` probes the
     extent's pages from `next_free` = 0, and stops at the first one that fails to read.
   * The fire therefore asserts, right after its claim, that the claimed extent's FIRST page does not read through
     `read_page`: the same probe, the same page. If it reads, the fire panics ("did not inject") rather than
     passing silently.
   * **The harness also REFUSES `CURVE_FIRECHECK=orphan-extent` with `merge` on.** Merges free extents, and a
     recycled extent can hold a stale checksummed page. The restriction was only in the lane doc; it is now in the
     code.
3. **The D216 pair and the control are judged by state (C8).** The child prints `recovered` (a new, observing
   `OpenDatabase::recovered`, copied from `OpenedDatabase::recovered`), and every RESTART row carries it.
   * control: `recovered = 0` and `wal B = 24`;
   * before D216, merge on: `recovered = 1` and `wal B > 24`;
   * after D216: `recovered = 0`.
4. **The checkpoint flag is cross-checked against state (C7).**
   * `ckpts` counts APPLIED merges only.
   * New guard **M6**: on every applied merge, the counter-derived `checkpointed` must equal "`wal.base_lsn`
     moved". `base_lsn` moves only on truncation, which only a checkpoint performs, and an applied merge's commit
     guarantees there is something to truncate. A mismatch is NOT A RESULT.
   * Its fire mode, `wrong-ckpt-flag`, inverts the flag in the comparison. It is a comparison check only.
5. **The test is independent (C2).** In `tests/read_vs_n_lease_census.rs`:
   * `open_sweep_visits` is compared with the arenas the TEST claimed (the distinct `ArenaId`s `arena_for` returned)
     plus trunk's arenas as the CATALOG records them (`get(TRUNK).arenas`). Both are independent of the store's
     `live_arenas()` list, which the sweep iterates.
   * `finished` is pinned to AFTER the sweep: when it is first seen ≥ 1, `sweep_visits() == 2 × expected`. The
     interval is a day, so no third sweep can have begun. This is R3's BEFORE-D209 shape, which the harness's R3 read
     relies on.
   * **D209's fix changes it to `1 × expected`** (the first pass sweeps nothing). That change belongs to D209's red
     test, and the lane report says so.
   * A7.5's "independently counted" was false for the previous version, and is now true.
6. **Minor (C10).**
   * `no-cluster-time` prints NOT A RESULT when it was requested but never injected (the run stopped before the last
     checkpoint).
   * The `Fire` enum doc no longer says every mode injects at the call site.
   * `LeaseStats::finished`'s doc says the pass REACHED the end of `scan_once`, the orphan sweep ATTEMPTED: a sweep
     that failed is reported and still counts.

**A10, 2026-09-24, before any build or run. Found while implementing A9.1's amortized column, by reading
`TxnManager::commit`'s trigger (`wal/txn.rs`, the `due` test) and the harness's axis-(ii) sampling.**

1. **A9.1's divisor 256 is replaced by the MEASURED period.**
   * **Why 256 is wrong.** The auto-checkpoint fires only when the counter reaches `checkpoint_interval()`
     AND the active-transaction table is empty, so it can be deferred. A merge cycle (`BEGIN AGENT SESSION`,
     `INSERT`, `MERGE;`) can also commit more than once. "/256" assumed neither.
   * **Why the observed share is also wrong.** Axis (ii) prints the LAST 64 merges before each M target, and the
     targets are multiples of 256, so every block ends on the checkpointing merge. Pooled over the printed
     blocks, the checkpoint share is 4/256, not 1/256.
   * **The measurement.** Each applied merge that checkpointed carries `period`: the merge cycles since the
     previous checkpoint, or since the open, that merge included. The harness counts the cycles itself and
     zeroes the count at every open, whose fresh `TxnManager` counts commits from zero.
   * The summary prints `period` and `amort ns` = median + (`ckpt ns` − median) / period.
   * **Pre-registered:** on axis (ii), `period` = 256 at M = 1024, 4096 and 16384. Those three periods run between
     two checkpoints. At M = 256 the period runs from the open and is also expected to be 256. INFERRED: one
     commit per cycle, and no concurrent transaction at the publish's commit. A different value is a finding
     about the trigger, not a failed guard: `amort ns` is computed from the measured value either way.
   * On axis (i), a 64-merge batch after an open is expected to contain no checkpoint, so `ckpt ns`, `period`
     and `amort ns` print "-".

**A10.2, same day, still before any build or run. M6's one legitimate disagreement, read in `wal/log.rs` `truncate`.**

* `truncate` returns early and keeps the log when a WAL pin sits below the frontier. A checkpoint under a pin
  therefore resets the commit counter WITHOUT moving `base_lsn`, and M6 fires.
* That firing is correct: the merge paid a checkpoint that reclaimed nothing, so its `ckpt ns` is not the
  truncating cost A9.1 describes.
* The pins' only sources are replication: `backup.rs`, `snapshot.rs`, `stream.rs`, and
  `TxnManager::begin_snapshot_read`. None is on the harness's path (READ, `git grep 'pin(\|pin_durable('`). So M6
  is expected silent on every clean run.
* **The fire-check.**
  * On (e), `wrong-ckpt-flag` fires M6 on every applied merge. No merge there checkpoints, and the inverted flag
    claims each one did while `base_lsn` stayed put.
  * The guard's other direction, silence on a merge that checkpoints, is carried by run (g).

**A11, 2026-09-24, before any build or run. Review 3 of `a039bab..1b8676b` (artie-research
`frontier/read_vs_n_review3.md` @ `ad854e9`) returned SOUND-WITH-CAVEATS. Each finding, and what changes:**

1. **E1: D216's AFTER half is judged on `recovered = 0` alone.**
   * **Withdrawn:** A9.1's "`recovered = 0` and `wal B = 24`" for the AFTER half. D216's fix
     (`d216-clean-restart` @ `a46b7d1`) still re-appends the retained DDL and run declarations at every checkpoint
     (`txn.rs` `checkpoint_locked`). So a correct merge-on row prints `recovered = 0` with `wal B > 24`. A9.3 and
     FAN-QUEUE row (i) already say `recovered = 0` only; that now holds everywhere.
   * **`wal B = 24` judges the merge-OFF control only.**
   * **A9.1's reason for `recover_us ≤ 5,000` was wrong for merge-on rows.** It said "a stat and a 24-byte header
     read". After D216 the child's `recover` still analyses every record in the log, and a merge-on log holds the
     re-appended declarations: the runs bound since the parent's last open (K per axis-(i) batch: 8 in (e), 64 in
     (h)) plus any retained DDL. The bound stands (INFERRED: at most one DDL record and 64 run declarations). The
     RESTART row's `wal B` prints that input.
   * **E4.** After D216, `recovered` is the fixed `recover`'s own verdict. So `rebuild_us < 1,000` is the
     independent cross-check. A row with `recovered = 0` and `rebuild_us ≥ 1,000` is a finding against D216, not a
     pass.
   * `OpenDatabase::recovered`'s doc no longer says it is "whether it rebuilt". The stale-index marker also forces a
     rebuild, with `recovered` false. Nothing in this harness writes the marker (INFERRED, per review 3).
2. **E2: the test parks the first pass INSIDE its sweep.** The 10 ms poll almost never landed inside a µs sweep. Now
   `tests/read_vs_n_lease_census.rs` gives the reaper a delegating `BranchCatalog`, `ParkFirstPassSweep`.
   * **The seam.** It holds the first `get_raw` made off the thread that built it. `LeaseThread::start` runs the
     open sweep on the caller's thread, and a pass with nothing expired reaches `get_raw` only in the orphan sweep.
     So the parked call is the first pass's sweep, first extent. `tests/w4_sweep_slot_recycle.rs` uses the same
     shape.
   * **While parked, the test asserts:**
     * its premise, `sweep_visits == live + 1` (the open sweep's `live`, plus the extent counted just before
       `get_raw`);
     * `finished == 0`.
   * **After release,** it asserts that the pass finished, then `sweep_visits == 2 × live` (review 3's minor point on
     the order).
   * **The mutant it must kill:** `finished.fetch_add` moved above `collect_orphans_if_due`. While parked, that
     mutant shows `finished = 1`.
   * The wait is bounded by `PATIENCE` on both sides.
   * **D209:** the first pass sweeps nothing, so it never parks and `wait_parked` fails. D209's red test replaces
     this block (A9.5 already assigned it the change).
3. **E3: linearity of the checkpoint's excess is judged on a linear fit WITH AN INTERCEPT, over every checkpoint.**
   * **Withdrawn:**
     * A9.1's log-log band [0.5, 1.2]. For `c + a·M` the log-log slope is `aM/(c + aM)` < 1, and it drops below 0.5
       whenever the fixed cost exceeds the replay's.
     * The summary's per-row `slope(ckpt-med)` column. Its M = 1024 row spanned 256→1024, outside the stated range,
       so removing the column removes the mis-span.
   * **Why a fit rather than subtracting the M-independent term first.** That term is the WAL flush, `bp.flush_all`,
     the disk sync, `truncate`'s two syncs and the replay flushes. It runs inside the same commit as the replay, so
     the harness cannot time it apart. The intercept estimates it from the data.
   * **Why every checkpoint.** Each printed row holds ONE checkpointing merge, so a row's excess carries a single
     fsync's jitter. Axis (ii) has one checkpoint per 256 merges, 64 at M = 16,384, and the fit averages them.
   * **Each point** is excess = the checkpointing merge's ns − the median ns of the applied merges in its own period
     that did not checkpoint, i.e. the typical merge at that M. The points print raw (`CKPT M= ns= period_median_ns=
     excess_ns= period=`). Then come the fit over all points and the fits over the lower and upper halves (split at
     the middle point).
   * **Judged:**
     * the all-points slope `a` > 0;
     * `a_hi / a_lo` in [0.5, 2.0]: the slope does not drift with M, so the excess is linear.
   * **Reading the outcomes:**
     * A ratio outside that range, with both halves' `a` > 0, says the replay is not linear in M.
     * `a` ≤ 0 says the replay's growth is below the jitter at these M. That is inconclusive, not flat.
     * The magnitude of `a` is not pre-registered.
   * `amort ns` (A10.1) is unchanged.
4. **E6: a fire mode without `CURVE_ARMS` is refused.** It panics at startup, because every mode breaks a READ-VS-N
   guard. The historical path also prints `failures` itself, so a non-zero exit always carries its reason.
5. **E5: the `Fire` taxonomy has three kinds.**
   * COMPARISON: an offset, swap or dropped delta at the harness's own comparison.
   * CONTROL: changes what the timed control arm does. These are `control-catalog`, `control-cold` and
     `control-drift`. The branch path is untouched.
   * IN PATH: a real state change the measured code sees. `merge-quarantined` moves here: its quarantine is real,
     and MERGE sees it.
6. **E7, ADDED: an in-path fire for M6, `pinned-checkpoint`.**
   * **What it does.** Axis (ii) holds a real `wal.pin_durable()` for the whole axis. Each auto-checkpoint then
     resets the commit counter while `truncate` keeps the log: A10.2's one legitimate disagreement, produced by the
     engine.
   * **Why add it.** M6's only fire so far inverted a flag at the comparison.
   * **Its run** needs an M target of at least one checkpoint interval:
     `CURVE_FIRECHECK=pinned-checkpoint CURVE_ARMS=merge CURVE_MERGE_K=8 CURVE_MERGE_M=256,512` on checkpoints
     `256,2048`.
   * **Expected:** rc = 2, M6 at `axis ii M=256` and `axis ii M=512`, and no other guard. The fit prints
     "not computable" (two points).
   * **Refusal.** If no axis-(ii) merge checkpointed under the pin, it prints NOT A RESULT naming that. So it is not
     in the (f) list, whose M targets stop at 128.

**A12, 2026-09-24, before any build or run. Review 4 of `1b8676b..e4094cc` (artie-research
`frontier/read_vs_n_review4.md` @ `a696181`) returned SOUND-WITH-CAVEATS. E2's seam is sound. The lead's decisions on
R1–R5, and what changes:**

1. **R1: the replay's O(M) is judged on an INTEGER, not the clock.**
   * **Withdrawn:** A11.3's judged verdict on the time fit (`a > 0` and `a_hi / a_lo` in [0.5, 2.0]). It has no
     noise floor. Review 4's Monte Carlo of its arithmetic:
     * under a CONSTANT excess it says "linear" 10% of the time, and it can never say "constant";
     * at a replay-to-jitter ratio of 2, it calls an exactly linear replay "not linear" 37% of the time.
   * **Judged:** `replay_bytes`, printed raw on each axis-(ii) `CKPT` line, against `runs`.
     * `replay_bytes` is the WAL bytes appended after the checkpoint's truncation: `next_lsn − base_lsn` read once
       the MERGE returns. That is everything `replay_schema` and `replay_runs` appended, and nothing else. It prints
       "-" when the base did not move (no truncation: a pin).
     * Control flow fixes the count, so no load can move it.
   * **b, derived from the encoder** (READ `wal/log.rs` `WalManager::append` and `RecKind::serialize`,
     `provenance/mod.rs` `RunEntity`, `agent_sql/runtime.rs` `begin_session_as_staged`):
     * frame = 4 length + 8 lsn + 8 prev_lsn + 8 txn_id + kind + 4 CRC = 32 + kind;
     * kind `RunIdentity` = 1 tag + 4 `prov_id` (u32) + (2 + 12) agent id + (2 + 9) run id `<unnamed>` + (2 + 11)
       model `unspecified` + (2 + 11) version `unspecified` + 32 prompt hash (all-zero, no PROMPT) + 8 `started_at` +
       8 parent id + 4 generation = **108**;
     * **b = 140 bytes per retained run.**
   * **The agent id is now fixed-width, `mc` + 10 digits** (`mc{id:010}`), so every declaration encodes to the same
     size. Under `mc{id}` the size grew with the id's digit count, and the line could not be exact.
   * **a** is the retained DDL records. **At this tree it is 0:**
     * axis (ii) starts at `reopen_for_merges`, whose fresh `TxnManager` has an empty `schema_log`, and nothing
       refills it at open (ledger D227);
     * no merge here logs DDL (INFERRED: the publish logs DDL only for a schema rewrite, `runtime.rs` near
       `ctx.txn.log_ddl(record)`).
   * **Pre-registered:** `replay_bytes = 140 · runs` exactly, at every truncating checkpoint. The printed bytes fit
     reads slope = 140.000, intercept = 0.0, max |residual| = 0.0.
     * At the default M targets: 64 `CKPT` lines, `runs` = 256, 512, …, 16384 (runs = M, INFERRED: one declaration
       per merge, none refilled).
     * Any residual is a finding: a record of another size entered the replay.
     * If D227's fix merges first, the intercept becomes the retained DDL's constant size. The slope of 140 and the
       zero residual stand.
   * **Reported, never judged: the time fit.** Excess against `runs`, over all points and over each half, each with
     `a ± SE(a)`, `c` and the residual sd.
2. **R2: A11.1's E4 bullet is RETRACTED. So is review 3 §1's premise that `rebuild_us` was an independent check.**
   * **Why.** `rebuild_us` is timed inside `if recovered || stale` (`recovery.rs` in `open_recovered` at
     `e4094cc`; the same gate at `a46b7d1:src/wal/recovery.rs:458`). So it is a consequence of `recovered`, not a
     check on it. "`recovered = 0` with `rebuild_us ≥ 1,000`, a finding against D216" could come only from the
     D205 marker, and nothing judged it.
   * **D216's AFTER half stays judged on `recovered = 0` alone, BECAUSE the flag is the rebuild's gate.** A
     regression that still rebuilds must set it to 1, or trip the marker.
   * **Residual-work checks (reported):** `recover_us ≤ 5,000` (A11.1), and `child net ms` against `m rows`.
   * **The marker is WIRED rather than deleted: guard H6.**
     * Before its open, the child reads `stale_indexes_marker(<db>.wal)`, the engine's own path helper, before the
       open can remove it. It prints `stale` (0/1) on its `RESTART_RESULT` line and in the RESTART row.
     * H6 is NOT A RESULT when `stale` is not 0. Nothing in this harness rolls back, so a marker means an index undo
       failed in the parent, and that restart's rebuild is D205's, not a clean restart's.
     * **In-path fire `stale-marker`:** before the FIRST restart, the parent writes the marker file
       (`mark_indexes_stale` writes it, and `open_recovered` reads only whether it exists) and asserts that it
       exists.
     * **Expected:** rc = 2, H6 at the first checkpoint only (the child's open removes the marker after
       rebuilding), and no other guard.
   * `OpenDatabase::recovered`'s doc now says the flag is the rebuild's gate, not something the rebuild's time
     checks.
3. **R3: a fire mode whose arm is off is refused at startup, whatever the arm.**
   * `Fire::needs` maps every mode to its arm. It is an exhaustive `match` with no `_` arm, so a new mode cannot
     compile without naming one. The mapping:
     * read: G2–G7's modes;
     * merge: M1–M6's modes;
     * restart: H1–H6's modes;
     * any: G1's `extra-branch`, because every arm set checks it.
   * `Fire::refusal` returns the reason, and `main` panics with it before anything is created. It covers three
     cases: no `CURVE_ARMS`; the arm is off; `orphan-extent` with merge on (A9.2, moved here).
   * **The check that the refusal fires is run (f0).** Five commands, each expected to exit 101 with its refusal text
     and to create no run directory:
     * `pinned-checkpoint` with `read` (the case review 4 found);
     * `wrong-page` with `merge`;
     * `child-locked` with `read,merge`;
     * `wrong-visits` with `CURVE_ARMS` unset;
     * `orphan-extent` with `merge,restart`.
   * The negative control is (f) itself: every mode runs with its arm on.
4. **R4: both fits' x is `runs`** (`retained_runs` after the merge, the replay's input), not `m_done`. Each `CKPT` line
   prints both. `CkptPoint.m`'s doc no longer calls it "the run declarations it re-appended".
5. **R5:**
   * Each half's range is printed (`runs a..=b, n points`). For an odd n, the middle point belongs to the upper half.
   * A11.6's "the fit prints not computable (two points)" was imprecise. At two points each half has one point and
     prints "not computable". The all-points time fit prints the exact two-point line with SE "-".
   * In (f2), under the pin, no checkpoint truncates. So every `replay_bytes` is "-", and the bytes fit prints "not
     computable (0 truncating checkpoints)".
6. **Counts.**
   * Fire modes: 21. `stale-marker` is new.
   * The (f) list on the (e) command has 19 of them: all except `orphan-extent` (its own restart-only run) and
     `pinned-checkpoint` ((f2)).
   * New `#[test]`s over `9aa6968`: still 41.

**A13, 2026-09-24, before any build or run. Review 5 of `e4094cc..94413b1` (artie-research
`frontier/read_vs_n_review5.md` @ `b9c6147`) returned SOUND-WITH-CAVEATS. It found A12.1's integer judge sound, and
moved `runs = M` and `a = 0` to READ. The lead's decisions on C1–C7, and what changes:**

1. **C1: an in-path JUDGE fire, `ckpt-ddl`, proves A12.1's judge can move.**
   * **What it does.** Right after axis (ii)'s `reopen_for_merges`, and before any merge, the harness runs one real
     `CREATE TABLE ckpt_ddl (c INTEGER);` (`CKPT_DDL_SQL`).
     * The executor logs that table's `Ddl` record AFTER the DDL's own checkpoint (`ddl_checkpointed`), which
       truncates the log and zeroes the commit counter.
     * `log_ddl` keeps the record in `schema_log`, so every axis-(ii) checkpoint re-appends it through
       `replay_schema`.
   * **D, the record's size, derived by hand from the encoder** (READ `wal/log.rs` `RecKind::serialize`'s `Ddl` arm
     and its decoder; `execution/executor.rs`'s `CreateTable` arm):
     * frame = 32, as in A12.1;
     * kind = 1 tag (9) + 1 op (`CreateTable` = 0) + 4 `dir_root` (u32) + 4 `time_travel_root` (u32) + (2 + 8)
       table `ckpt_ddl` + 2 column count (u16) + (2 + 1) column `c` + 1 type tag + 1 nullable = **27**;
     * **D = 59 bytes.** The type tag is one byte for either integer type, and the nullable flag is one byte either
       way, so D does not depend on how the parser maps `INTEGER` or on nullability.
   * **Its run is (f3):** `CURVE_FIRECHECK=ckpt-ddl CURVE_ARMS=merge CURVE_MERGE_K=8 CURVE_MERGE_M=256,512` on
     checkpoints `256,2048`. Pre-registered:
     * `CKPT-DDL base_moved=1 bytes_past_base=59`: the record alone past the new base. The fire asserts that the base
       moved and that the byte count is non-zero, and panics "did not inject" otherwise;
     * `CKPT M=256 runs=256 replay_bytes=35899` and `CKPT M=512 runs=512 replay_bytes=71739`, i.e. `59 + 140·runs`,
       each with `period=256`, because the DDL's checkpoint zeroed the counter before the first merge;
     * the bytes fit reads slope = 140.000, intercept = 59.0, max |residual| = 0.0;
     * rc = 0 and `GUARDS: every guard held`, because no harness guard reads the integer. The `FIRECHECK` line
       names it a JUDGE fire.
   * **The verdict script's A12.1 rule (intercept 0.0) must FAIL on (f3), with intercept 59.** That failure is the
     fire-check: the ENGINE, not the harness, produced a non-zero intercept. If the rule passes on (f3), that is a
     finding against the judge.
   * `Fire` gains a fourth kind, JUDGE: a real state change that moves a judged integer, where no harness guard fires.
2. **C2: (f0)'s "creates no run directory" is WITHDRAWN.** It cannot fail: `RemoveOnDrop` deletes the directory on
   unwind too.
   * **Chosen: print a line at the directory's creation, and assert it is ABSENT.** The harness prints
     `RUN DIR <path>` to stderr on the line after `create_dir_all`. stderr leaves every stdout table unchanged, and
     (f0) captures `2>&1`.
   * Each (f0) command must therefore show rc = 101, its refusal text, and no `RUN DIR` line.
   * The refusal already precedes the directory (READ: `Fire::refusal` runs before `temp_dir`). The line moves that
     ORDER from read to measured. A refusal moved after the directory's creation would print the line first, and
     (f0) would catch it.
3. **C3: Q10's time bands are WITHDRAWN from judgment, on both axes.**
   * **Why axis (ii):**
     * its band is a log-log slope of the median over M in [−0.1, +0.2], and M rises with wall time, so box drift
       aliases onto M;
     * over one factor-4 row, a 13% drop in the median fails it (ln 0.87 / ln 4 = −0.100), and so does a 32% rise;
     * there is no control arm.
   * **Why axis (i):** it has the same defect, extending the lead's decision to the same row.
     * Its ratio ns(10^6)/ns(1000) in [0.7, 1.5] compares two medians taken hours apart;
     * a 30% drop or a 50% rise fails it;
     * a ratio needs one instrument and one moment.
   * **The judged content of "merge cost against N and M" is the INTEGERS:**
     * Q1: `V_hi` = the `applied` length before the merge, exactly. That is the O(M) term, and it is flat in N;
     * Q2–Q9: constants and zeros, on both axes;
     * A12.1's replay bytes.
   * **Q10's own claim, that the O(M) pass is invisible in TIME at M ≤ 16,384, is inherently a time claim. It is now
     REPORTED.** Each arm-2 summary row prints:
     * the median;
     * the local log-log slope of the median, whose span is printed on a legend line (the previous row to this one:
       axis (ii) 256 → 1024 → 4096 → 16384 at the defaults; axis (i) the N checkpoints);
     * a new column, `typ ns ± SE`: the mean and sd/√n over the block's applied merges that did not checkpoint.
   * **Why not a same-moment control arm for MERGE.** M grows only within one process lifetime (`State` resets at
     every open, D212). A control at M = 0 measured at the same moment would be a second database interleaved with
     this one. That doubles the arm's fsync load on the very box it is meant to control for, and the integers
     already carry the claim.
   * **Not changed, stated:**
     * Arm 1's ns predictions keep G7, their drift control.
     * Arm 3's time bands stay judged, because their spans are long and their bands wide:
       * R5 and R8: a slope band of ±0.2 over N from 64,000 to 10^6 (a factor of 15.6) tolerates a drift of about
         ±70% (ln 1.7 / ln 15.6 ≈ 0.19);
       * A9.1's `rebuild` band spans 400× (0.5–200 ms);
       * R7 is an absolute ≤ 50 ms bound.
     * Each of those is also backed by a judged integer: R1–R4 and R10; `recovered`.
4. **C4: withdrawn in the text, where A12.2 only relabelled them.** `recover_us ≤ 5,000` (A9.1; A11.1's "the bound
   stands") and `rebuild_us < 1,000` (A9.1's "secondary", A9.3) are **reported, not judged** (A12.2).
   * D216's AFTER half is judged on `recovered = 0` alone.
   * The (e2) control is judged on `recovered = 0` and `wal B = 24`.
5. **C5: A12.2's wording is corrected.** `rebuild_us` is timed AROUND the gate, not inside it: `t` starts before
   `if recovered || stale`, and `rebuild` is the elapsed time after that block. With the gate false, it times an
   empty block. The conclusion stands: the time is a consequence of the flag, not a check on it.
6. **C6:** `stale_marker` is moved above `open_only_child`'s doc comment, which had been documenting it. Each item
   now carries its own doc, and no attribute is orphaned.
7. **C7: every applied checkpointing axis-(ii) merge is a CKPT point.**
   * Before, a point needed a non-empty time period (`period_ns`), a TIME-fit requirement that also gated the
     JUDGED bytes line.
   * A point whose period is empty is reachable only at `FERRODB_CHECKPOINT_INTERVAL=1`. It prints
     `period_median_ns=-` and `excess_ns=-`, and it is left out of the time fit only.
   * The bytes line takes every point.
   * The time fit's range labels read "n points, k with a period median".
   * The run header now prints `FERRODB_CHECKPOINT_INTERVAL` (or "unset"), so a `period` that is not 256 can be read
     against it.
8. **Counts.**
   * Fire modes: 22 = 10 comparison + 3 control + 8 in path + 1 judge. `ckpt-ddl` is new.
   * The (f) list on the (e) command stays at 19. `ckpt-ddl` runs as (f3).
   * New `#[test]`s over `9aa6968`: still 41.
9. **A13.9, the same pattern as A11.6.** If `ckpt-ddl` is requested but no axis-(ii) checkpoint truncated (the M
   targets stop below one interval), the run prints NOT A RESULT naming that. The judge was never moved, so a run
   that exits 0 cannot be read as the fire having happened. The (e) command's M targets (64, 128) would do exactly
   this, which is why `ckpt-ddl` runs as (f3).

**A14, 2026-09-24, before any build or run. Review 6 of `94413b1..1a59386` (artie-research
`frontier/read_vs_n_review6.md` @ `2c08024`) returned UNSOUND on one defect (U1), with everything else in A13 sound.
The lead's decisions on U1, U2 and C2–C5, and what changes:**

1. **U1: `ckpt-ddl`'s injection check asserted a false premise. It is replaced by the state that moves the judge.**
   * **The defect.** The check required `base1 != base0`. Right after `reopen_for_merges` the log is EMPTY past its
     base: the open's own checkpoint runs in a new `TxnManager` whose declaration lists are empty (D227), and nothing
     appends before the fire. So the DDL's truncation stores the base it already had, and (f3) would have panicked
     "did not inject" before any merge. A13.1's `base_moved=1` and "rc = 0" were unreachable. The printed
     `base_moved=1` was a literal, so no run could have shown otherwise.
   * **The injection itself was right.** The record is retained, and the intercept is exactly D = 59 (review 6 §1a,
     §1b).
   * **The new check** (`ckpt-ddl`, before any merge):
     * look up `ckpt_ddl`'s `first_directory_page_id` under the catalog lock;
     * require `txn.retained_shape(dir_root).is_some()`, i.e. the record sits in `schema_log`, which is what every
       later checkpoint re-appends;
     * require `bytes_past_base > 0`.
     * Either failing panics "did not inject". `past > 0` alone would also pass for a bare, unretained append, and
       retention is what moves the judge.
   * **`base_moved` is now COMPUTED** (1 when the base moved across the DDL, else 0), and the line also prints
     `retained=1`.
   * **Pre-registered for (f3): `CKPT-DDL base_moved=0 retained=1 bytes_past_base=59`**, and rc = 0. Everything else
     in A13.1 stands: the CKPT totals 35899 and 71739, the fit's intercept 59.0, and the verdict script's A12.1 rule
     failing.
   * **`base_moved=0` also witnesses D227** ("the reopen leaves nothing past the base"). D227's fix would flip it to 1,
     and would change A12.1's `a = 0` and `runs = M` for every run. Such a change is a finding to amend, not a
     failure of this fire.
2. **U2: arm 3's time slopes and R5's magnitude are REPORTED, not judged.**
   * **The defect in A13.3's premise.**
     * R5-slope and R8-slope are OLS fits over one child open at each of N = 64,000, 256,000 and 10^6. The middle
       point carries 0.6% of an endpoint's weight, so each is a two-point slope.
     * "±70%" held only for a true slope of exactly 1.0. The band is also R5's model uncertainty: D65 run 4 measured
       its analogue at 1.10.
     * This box's 1-min load reached 3.67× its median in D65 run 4. So a spike at the 64,000 endpoint can make a
       superlinear wall read as LINEAR.
   * **Withdrawn from judgment:** R5-slope, R8-slope and R5-mag (`lease_start` at 10^6 in [10, 90] s). Each is
     printed and reported.
   * **Still judged:**
     * the O(N) claim, exactly, by R1–R3 (`open_sweep_visits` = N + 1, `sweep_descents` = visits, first-pass visits =
       N + 1);
     * R6 and R8-share, which are ratios within one child open;
     * A9.1's rebuild band;
     * R7;
     * G7, which fails conservatively.
   * **The load flag, pre-registered BEFORE the run.** It is the same threshold as D65's L2.
     * The child reads the 1-minute load average at its start (before the open) and at its end (after the close). It
       uses `/proc/loadavg`, or `sysctl -n vm.loadavg` where that is absent, the same reader as
       `examples/d97_attestation.rs`.
     * It prints both, ×100 as integers, on its `RESTART_RESULT` line: `load_start_centi` and `load_end_centi`
       (u64::MAX when unavailable).
     * The arm-3 summary prints both per row, plus **`L2`**: 1 when the row's larger reading exceeds **1.5 × the median
       of every reading in the run**, else 0.
     * A reported slope whose endpoint row carries `L2 = 1` is read as load-contaminated. This is a flag, never a
       guard.
   * **Arm 1's CLASS is judged on `slope(ratio)`, not on raw branch ns** (the lead's option, taken). The ratio is
     branch over control at the same moment, already printed. Under G7's admitted 1.5× drift, a raw slope over
     16,000…10^6 can move by up to ln 1.5 / ln 62.5 ≈ 0.098, against a < 0.3 threshold whose predicted value is
     ≤ ~0.2.
     * The class band is unchanged: < 0.3 on the saturated segment.
     * The raw `slope(branch)` is reported.
     * The knee [4, 25] and the resident magnitude [5, 60] µs stay as registered, under G7.
3. **C2: the header prints the interval the ENGINE uses.** `wal::txn::checkpoint_interval()` becomes `pub`, a
   visibility change only, and the header prints its value beside the raw environment variable. Before, a `0`, which
   the engine maps to 1, or an unparseable value, which falls back to 256, printed verbatim.
4. **C3: `typ ns ± SE` is a WITHIN-BLOCK standard error.** It is sd/√n over consecutive merges of one block. It is not
   a noise floor for comparing rows, since between-block drift is outside it. The legend says so. It is reported
   only.
5. **C4: `RUN DIR` is printed after `RemoveOnDrop` is armed.** A failed stderr write can then no longer leak the empty
   directory. (f0)'s criterion is unchanged: the line is absent on a refusal.
6. **C5: the stale "guard" wordings are fixed.**
   * `Fire::needs`'s doc now reads "the arm this mode needs".
   * The refusal now reads "it needs the <arm> arm, which CURVE_ARMS does not run". So (f0)'s expected texts are
     `is refused: it needs the Merge arm` / `Read arm` / `Restart arm`.

**A15, 2026-09-24, before any build or run. Review 7 of `1a59386..347a0e6` (artie-research
`frontier/read_vs_n_review7.md` @ `fcc08e5`) returned SOUND-WITH-CAVEATS. U1 and U2 are fixed as scoped. The lead's
decisions on W1–W8, and what changes:**

1. **No claim-deciding verdict is judged on wall time alone (W1, W2).**
   * **KNEE is judged on `ratio(10^6) / ratio(256)` at T=1.** `ratio` is branch ns over the control's ns at the same
     (N, T), measured at the same moment by the interleaved, rotated arms, and already printed. The band [4, 25] is
     unchanged. The raw ns ratio is reported beside it.
   * **Made REPORTED, not judged:**
     * T8DIR. Its T=1 and T=8 phases run one after the other, with no same-moment control, and no margin argued from
       a quiet box is accepted.
     * RESIDENT. It is an absolute band that no across-N guard can protect: a run-wide slowdown moves every row alike.
     * R6 and R8-share. Their terms time DISJOINT, sequential phases of one open (`arena`, `lease_start`, `total`), so
       A14.2's "ratios within one child open" was not "one moment". Both also rest on the single 10^6 open whose
       magnitude A14.2 withdrew.
   * **A9.1's `rebuild` band (0.5–200 ms) is REPORTED.** Load can lift a no-op into its lower edge. D216's halves are
     decided by `recovered`, an integer.
   * **R7's ≤ 50 ms bounds stay judged, only in the direction load cannot manufacture.** Load only inflates a time, so
     HELD cannot be produced by load. A NOT HELD is printed with L2 and G7 beside it, and is read as load-qualified
     before it is called a finding.
   * **G7 stays the only time guard.** It only voids, the conservative direction. **Its measure is printed beside every
     time number the harness reports:**
     * per T in arm 1's section, as before;
     * as a G7 line in arm 2's and arm 3's sections;
     * or "not measured" when arm 1 is off, so those times have no drift measure at all.
2. **SLOPE is BOUNDED only when the raw slope and the ratio's slope AGREE (W3).**
   * The OLS over the saturated segment (every checkpoint with `d.fault` ≥ 0.9), per T, is computed for BOTH raw
     `slope(branch)` and `slope(ratio)`:
     * BOUNDED when both are < 0.3;
     * GROWING when both are ≥ 0.3;
     * **INCONCLUSIVE when they disagree.**
   * **Why.** `slope(ratio)` alone cannot tell box drift from an N-dependence the control shares with the branch, for
     example the branch's memory traffic evicting the control's lines at large N. Inside G7's band that drift biases
     the ratio's slope toward BOUNDED by up to ln 1.5 / ln 62.5 = 0.098. So drift now fails toward INCONCLUSIVE,
     never toward BOUNDED.
   * **Fixtures the verdict script must carry.** Its maintainer owns it; the lane does not edit it:
     * (a) `box_drift` (raw slope +0.34, a control drifting ×1.45 inside G7, ratio slope < 0.3) must grade
       **INCONCLUSIVE**, no longer BOUNDED;
     * (b) its mirror (raw slope < 0.3, ratio slope ≥ 0.3, e.g. a control that speeds up with N) must grade
       INCONCLUSIVE;
     * (c) both < 0.3 → BOUNDED and both ≥ 0.3 → GROWING, as before.
3. **L2 says when it cannot see (W5).**
   * A row with NEITHER load reading prints `L2=UNAVAILABLE`, and it is unguarded.
   * A row with one reading takes its L2 from that reading, and is listed as partly read.
   * The arm-3 summary names the flagged rows, the UNAVAILABLE rows and the partly read rows. It prints "no row
     carries L2 = 1" ONLY when every counted row has both readings and none is flagged.
   * **The blind spot, in the legend.** L2's instrument is two point readings of a 1-minute exponentially damped
     average. Around an open of a few seconds, both readings describe mostly the minute BEFORE it, so L2 = 0 on a
     short open is not evidence of a quiet open. This is a stated limit, not fixed: the flag qualifies only reported
     values.
   * The verdict script applies the same rule.
4. **`base_moved=1` is refused by the HARNESS too (the U1 residual).**
   * When `ckpt-ddl`'s base moves across the DDL, the harness prints NOT A RESULT (rc 2), naming A14.1: the reopen left
     records past the base, so D227's premise, and with it A12.1's `a = 0` and `runs = M`, no longer holds, and the
     pre-registration must be amended before (f3) is read.
   * (f3)'s expectation is unchanged: `base_moved=0` and no such line.
5. **Every slope column names its span (W7).**
   * Arm 1: `slope(branch)` and `slope(ratio)` span the previous row to this one, and the legend lists the N
     sequence. The CLASS is the verdict script's OLS over the saturated segment under item 2's rule, and KNEE is item
     1's ratio.
   * Arm 3: the `total` and `lease_start` slopes span the previous row to this one, with the N sequence listed.
6. **One load-median rule (W8).**
   * The median is taken over every available reading of every row with a child result, EXCLUDING H6 rows. An H6 open
     rebuilt for the stale-index marker, and is NOT A RESULT for every arm-3 value.
   * The verdict script's `rrows` already excludes H6 rows, so the harness adopts its rule.
   * An H6 row prints L2 as `H6`, is not flagged, and is not counted.
7. **W4 and W6 are not the lane's.**
   * W4: the verdict script reads this file from the live worktree, pinned by sha256; that is fail-closed.
   * W6: FAN-QUEUE #18's command cell is the lead's.
   * The lane's row, under `## FAN-QUEUE ROW (for the lead)`, is its source.
8. **Counts.** Fire modes: 22, unchanged. New `#[test]`s over `9aa6968`: still 41.

**A16, 2026-09-24, before any build or run. Review 8 of `347a0e6..344cf33` (artie-research
`frontier/read_vs_n_review8.md` @ `e9d084c`) returned SOUND-WITH-CAVEATS. Every A15 decision is present, and the type
pass is clean. The lead's decisions on V1–V8 and V10 follow. V9 and the script's side of V1 and V2 belong to the
verdict script's maintainer.**

1. **V1: KNEE is conjunctive, like SLOPE.**
   * Two knees at T=1:
     * the raw knee, branch ns(10^6) / branch ns(256);
     * the ratio knee, ratio(10^6) / ratio(256).
   * **HELD** when both lie in [4, 25]; **MISSED** when both lie outside it; **INCONCLUSIVE** when they disagree.
   * **Why.** ln KNEE(ratio) = slope(ratio) × ln 3906, so the ratio knee carries A15.2's defect: a control slowed at 10^6
     by the branch's traffic lowers it by up to 1.5× inside G7.
   * **Fixtures for the script** (its maintainer's), mirroring A15.2's:
     * (a) raw knee outside the band with the ratio knee inside → INCONCLUSIVE;
     * (b) the mirror → INCONCLUSIVE;
     * (c) agreement unchanged.
   * The arm-1 legend names the rule.
2. **V2: the `FIRECHECK CkptDdl` line and the comment at the fire are corrected.**
   * Since `5607324` a harness check DOES read `base_moved` (A15.4). The line therefore now reads: no harness guard
     reads the replay-bytes integer; the only NOT A RESULT that may appear below is A14.1's refusal of a moved base;
     the replay-bytes line must carry the pre-registered intercept.
   * The fire's comment no longer says the base's movement is "printed, not asserted".
   * The A14.1 refusal now names what it measures: "records sat past the base at the DDL's checkpoint; A14.1 expects
     none (D227)". It no longer names a cause the check cannot see.
   * The script's regex and a fixture for the rc 2 output are its maintainer's.
3. **V3 and V10: every G7 line says what it can see, what it cannot, and what exceeding the band voids.**
   * **Can see:** arm 1's read phases only. The value is printed per T with its row count.
   * **Cannot see:**
     * axis (ii), which runs after arm 1's last read phase;
     * arm 3's opens, which follow each N's read phases rather than coinciding with them;
     * a shift common to every N, which a max/min across N cannot show.
   * **Row counts:**
     * one row prints "1 row, no across-N measure", never a bare `1.000x`;
     * "arm 1 did not run" (the arm is off) and "arm 1 ran but left no row" are printed separately.
   * **The void's scope matches PREREG G7 and the script: above 1.5, EVERY ns column at every T is NOT A RESULT.**
     * The harness's G7 message now says so. Before, it said "at T={t}", which was narrower than the rule.
     * Each "band 1.5" line says so too, and says that arm 3's and R7's values are not voided by it.
4. **V4: R7 is REPORTED, not judged.**
   * **Why.**
     * Judged only on NOT HELD, a NOT HELD had no registered path to a finding: L2 and G7 cannot clear it.
     * Its FLAT class was never judged: a step growing as O(N) under 50 ms at 10^6 read HELD.
   * **No integer is available to judge FLAT on (READ).** The child prints per-step TIMERS only.
     * Its census and visit counters span the whole open.
     * The replayed files' byte sizes (`wal B`, `tel B`, `prov B`) are the replay's inputs, not a step's work, and grow
       with the workload by design (A7.3).
     * So no R7 step is judged, on time or on an integer.
   * Each step's time is reported with L2 and G7 beside it.
   * The harness names the H6 rows excluded from every arm-3 value on its own line. The script names them in its
     verdicts.
5. **V5: R7-recover's `recover_us > 0` (A7.1) is REPORTED.** It cannot fail: a µs timer around a function that at
   least reads a header never reads 0. The same fact is judged as an integer by R7-state: `recovered = 1` and
   `wal B > 24` before D216.
6. **V6: authorship of two A15.1 bullets.** A15's header reads "the lead's decisions", but two A15.1 bullets were the
   LANE's extensions, not the lead's:
   * "A9.1's rebuild band is REPORTED": review 8 §3 found it sound, and it stands as the lane's;
   * "R7's ≤ 50 ms bounds stay judged, only in the direction load cannot manufacture": superseded by item 4, the lead's.
7. **V7: A15.2's claim is narrowed.** "Drift fails toward INCONCLUSIVE, never toward BOUNDED" holds for ONE drift
   mechanism at a time.
   * **Two mechanisms can cancel inside G7's own instrument:**
     * a box that runs faster at large N lowers the raw slope;
     * a control slowed at large N by the branch's traffic lowers the ratio slope.
   * With control × box ≈ constant, G7 reads ≈ 1.0 while BOTH slopes drop, by 0.081 at a 1.4× effect.
   * KNEE's conjunction (item 1) shares this residual.
   * This is a stated blind spot, not a fix. An independent box instrument during the read phases would see it, but it
     is not added.
8. **V8: zero counted rows print "L2: no row was counted"**, never the vacuous "every counted row has both readings".
9. **Nits.**
   * The arm-1 legend reads "the raw knee, RESIDENT and T8DIR" where it read "raw ns".
   * The arm-3 comment's superseded "every reading in the run" sentence is corrected.
   * "UNAVAILABLE (unguarded)" reads "UNAVAILABLE (no load reading)", since a flag is not a guard.
10. **Counts.** Fire modes: 22. New `#[test]`s over `9aa6968`: 41. Both unchanged.

**A17, 2026-09-24, before any build or run. Review 9 of `344cf33..38d97a2` (artie-research
`frontier/read_vs_n_review9.md` @ `0e596f2`) returned SOUND-WITH-CAVEATS. No false verdict can print today: the verdict
script refuses the un-repinned output. The lead's decisions on X1, X2 and X5 follow. X3 and X4 belong to the verdict
script's maintainer. Item 4 is the lane's own.**

1. **X1: A16.2's "the only NOT A RESULT that may appear below is A14.1's" is WITHDRAWN. It was false.**
   * In the same mode, A13.9's refusal ("ckpt-ddl was requested but no axis-(ii) checkpoint truncated") prints below
     that line, and so does a failed close.
   * The `FIRECHECK CkptDdl` line now names every NOT A RESULT that may appear below it:
     * A14.1's refusal of a moved base (A15.4);
     * A13.9's refusal, which means the fire did not inject;
     * a failed close (item 4).
   * Any other NOT A RESULT is a finding.
   * No harness guard reads the replay-bytes integer, and the replay-bytes line must carry the pre-registered
     intercept. Both are unchanged.
2. **X2: H6 rows are excluded from every arm-3 value in the summary, for real.**
   * **Before**, the table updated its slope anchor (`prev`) for every row, H6 rows included. So the next row's
     `total` and `lease_start` slopes were computed from the H6 open, and the span legend listed the H6 N, while the
     line two below it said H6 rows were excluded.
   * **Now:**
     * an H6 row prints only its N and "H6: NOT A RESULT (stale-index marker at open); its values are in its RESTART
       and RESTART-RAW lines";
     * the anchor skips it, so no slope uses it;
     * the span legend lists only non-H6 rows;
     * the load median already left it out (A15.6).
   * **Pre-registered against the (f) `stale-marker` fire** (H6 at N = 256 only, on the (e) command):
     * the 256 row prints the H6 line;
     * the 2048 row's slopes print "-", since there is no earlier non-H6 row;
     * the span legend reads "N 2048";
     * the H6 line names `N=[256]`;
     * the load median is the 2048 row's readings;
     * rc 2, with H6 as the only guard.
3. **X5: the R7 line names `files` and states the join.**
   * The line now lists `files`, which A5 folded into R7's recover and A6 lists as a flat step, alongside lock,
     recover, sql_catalog, branch_catalog, effect_log, runtime and provenance.
   * It says R7's step times are printed in the RESTART rows, not in the summary table. So "L2 and G7 beside them"
     holds only by joining those rows to the summary on N.
4. **The lane's own: a failed close now sets rc 2.** The harness fixed its exit code BEFORE the final close, so an
   unclean close printed NOT A RESULT under rc 0. Review 9 §3 noted this as pre-existing, outside its delta. A close
   failure is now a failure like any other.
5. **Counts.** Fire modes: 22. New `#[test]`s over `9aa6968`: 41. Both unchanged.

**A18, 2026-09-24, before any build or run. Review 10 of `38d97a2..4513060` (artie-research
`frontier/read_vs_n_review10.md` @ `3fdebb7`) returned SOUND-WITH-CAVEATS. No false verdict can print: the verdict
script refuses the un-repinned output. The lead's decisions on Y1–Y8 follow. The script's side (the Y2 fixture, its
loose `H6` cell pattern, Y3's grading names) belongs to its maintainer.**

1. **Y1: arm 3's point count counts only non-H6 rows.**
   * `measured`, the count behind "arm 3: N restart(s) measured; one point is not a curve", now leaves H6 rows out.
     Before, it counted them, so A17.2's own (f) `stale-marker` shape (one H6 row and one counted row) printed no
     refusal while its curve had one point. An all-H6 run had zero points and no refusal.
   * **(f) is amended.** The `stale-marker` fire is allowed ONE extra, the ARM3 refusal "arm 3: 1 restart(s)
     measured; one point is not a curve", in the same pattern as `control-cold`'s allowed G7.
     * Its expectation becomes: rc 2, H6 at 256, and that ARM3 line; any other guard is a finding.
     * A17.2's "H6 as the only guard" is withdrawn.
   * An all-H6 run prints "arm 3: 0 restart(s) measured; one point is not a curve".
2. **Y2: the final close runs BEFORE the summary, and its error goes into `failures`.**
   * Before, the close ran after the summary had printed "GUARDS: every guard held", so a failed close printed its
     NOT A RESULT under that line (rc 2 since A17.4).
   * Now a failed close suppresses the GUARDS line and prints among the other NOT A RESULT lines. The rc follows from
     `failures` like every other failure. A17.4's "a failure like any other" is now true, and A17.4's separate rc
     patch is folded into that.
3. **Y3: A13.9's refusal is described correctly.** A17.1 and the `FIRECHECK CkptDdl` line said it meant "the fire did
   not inject". That is false: A14.1 asserts the record's retention before any merge, and failing it panics "did not
   inject". A13.9's refusal can only follow a successful injection. It means the record was retained but no axis-(ii)
   checkpoint truncated to re-append it: **DID_NOT_FIRE**, the judge never moved.
4. **Y4: the line says what each NOT A RESULT GRADES as**, rather than which ones "may appear":
   * A14.1's refusal of a moved base: a FINDING. The premise changed; amend before reading.
   * A13.9's refusal: DID_NOT_FIRE.
   * PARENT_PASS and a failed close: FINDINGS (FIRED_WITH_EXTRA). See item 5.
   * Any other: a finding.
5. **Y5: PARENT_PASS is named on the line.** On a loaded box, `parent_open`'s "the parent's first lease pass did not
   finish within {bound}" can print under (f3)'s own command: the first open, every checkpoint's reopen, and axis
   (ii)'s reopen.
6. **Y6: the R7 line names which steps the summary table carries.** `recover` IS in the table (`recover ms`). The
   other seven (`files`, `lock`, `sql_catalog`, `branch_catalog`, `effect_log`, `runtime`, `provenance`) are only in
   the RESTART rows, so L2 and G7 sit beside those seven only by joining on N.
7. **Y7: A15.6's "An H6 row prints L2 as `H6`" is SUPERSEDED by A17.2.** An H6 row prints only its N and its
   `H6: NOT A RESULT …` line, so no L2 cell reads `H6`. The FAN-QUEUE row's (h) expectation is corrected. The script's
   loose pattern that still accepts an `H6` cell is its maintainer's to drop.
8. **Y8: L2 is not judged on fewer than two counted rows.** With one counted row, the median is that row's own upper
   reading, so `max > 1.5 × median` is false by construction, and the clean-load statement came from an instrument
   that could not fire.
   * With `counted < 2` the summary prints "L2: not judged (fewer than two counted rows)."
   * `counted == 0` keeps A16.8's "no row was counted."
9. **Counts.** Fire modes: 22. New `#[test]`s over `9aa6968`: 41. Both unchanged.

**A19, 2026-09-24, before any build or run. D239's floor read gets its own timer (relayed by lane `d209-open-sweep` at
the lead's request; its report is artie-research `frontier/lane_d239_floor_before_recovery.md`, §2 and §9).**

1. **The change coming in.** D239 lands as `d239-on-16` @ `c4efffb`, on #16 @ `ed6e901`, after #16. It adds one call,
   `ArenaPageStore::reserve_persisted_floor`, inside `wal::recovery::open_recovered`, between `DiskManager::new` and
   `BufferPoolManager::new`, before `recover`.
   * The call reads the whole `{db}.arena` image and checks its CRC32. That is O(arena extents), about 48 B each.
   * The arena store's reopen repeats the same read after the open returns.
   * On this lane it would fall inside `boot.files`, which R7 lists as a flat step. So `files` would silently carry
     an O(N) read.
2. **At this tree the call does not exist.** The lane now adds:
   * `BootTimings::floor`, a `Duration`, zero at this tree;
   * the child prints `floor_us=` after `files_us=` on its `RESTART_RESULT` line;
   * the RESTART row prints `floor ms` next to `files ms`.
3. **The merge resolution, pre-registered for whoever merges D239 into this lane.** Time the call exactly as variant P
   does (`7be2904`, which is D239 built on this lane's `38d97a2`):
   * `let floor = Instant::now();` before the `.arena` path is built;
   * `timings.floor = floor.elapsed();` right after `reserve_persisted_floor(..)?`;
   * `timings.files = t.elapsed().saturating_sub(timings.floor);`.
4. **Pre-registered values.**
   * At this tree: `floor_us = 0` in every RESTART row.
   * At a tree containing the D239 call: `floor_us > 0` in every RESTART row. Every child opens a database whose
     `.arena` image the parent's close wrote, and a whole-file read plus a CRC does not finish in under 1 µs
     (INFERRED). A row with `floor_us = 0` there means the timer was not wired at the merge. That is a finding, and it
     means arm 3's `files` cannot be read as flat.
   * `floor` is REPORTED, like every arm-3 time since A16.4. It is expected to be linear in the arena image's bytes,
     which the RESTART row does not print.
5. **#16 @ `ed6e901`, which D239 sits on, keeps this lane's merge-arm premises (READ, `git show ed6e901:src/wal/txn.rs`,
   `checkpoint_or_keep_held`).**
   * A checkpoint that TRUNCATES still resets the commit counter and then replays schema and runs. So A12.1's
     `replay_bytes` and M6 hold.
   * A checkpoint KEPT FOR OWED releases returns before truncating and before resetting the counter. So the
     `checkpointed` flag is false and the base does not move: M6 agrees.
   * A checkpoint kept by a PIN resets the counter and leaves the base where it was, which is still A10.2's
     disagreement. So `pinned-checkpoint` still fires M6. It now replays nothing, which is D234's fix, and the CKPT
     lines still print `replay_bytes=-`.
   * Owed releases do not arise in this workload (INFERRED: nothing here fails a release).
6. **Counts.** Fire modes: 22. New `#[test]`s over `9aa6968`: 41. Both unchanged.

**A20, 2026-09-24, before any build or run. Review 11 of `4513060..5ec8f47` (artie-research
`frontier/read_vs_n_review11.md` @ `7e41bcd`) returned SOUND-WITH-CAVEATS. All of Z1–Z6 still stand at `edbe154`. The
lead's decisions on Z1–Z6 follow, plus ledger D265, which is this lane's.**

1. **Z1: the generic FIRECHECK line prints, by guard id, what this mode must fire and what it is allowed besides.**
   * Before, it said "exactly the guard this mode breaks must appear below, and no other". That was false for four
     modes the pre-registration allows extras on.
   * `Fire::expects()` is an exhaustive `match`, like `needs()`, so a new mode cannot compile without declaring both
     sets:
     * `stale-marker`: H6, plus ARM3 (A18.1);
     * `child-locked`: H1, plus ARM3;
     * `control-cold`: G5, plus G7;
     * `merge-quarantined`: M1, plus M5 and G1;
     * every other mode: its one guard, and "none";
     * `ckpt-ddl` keeps its own line (item 3).
   * **The verdict script must CROSS-CHECK the printed ids against its own table and refuse on a mismatch**, so the
     two copies cannot drift. Its `FIRECHECK_RE` takes this text.
2. **Z2: the precedence under (f3), registered.** When A13.9's refusal ("A12.1: CURVE_FIRECHECK=ckpt-ddl was requested
   but no axis-(ii) checkpoint truncated …") prints, the fire's verdict is DID_NOT_FIRE. EVERY co-printed A14.1
   refusal, PARENT_PASS, failed close (CLOSE) or LEASE line (item 4) is ALSO a finding in the verdict class, not only
   an info line.
3. **Z3: the `FIRECHECK CkptDdl` line quotes each graded line's leading words**, rather than labels those lines do not
   print. "Is GRADED" becomes "is to be graded", since the harness does not grade its own run. The quoted lines:
   * `A14.1: ckpt-ddl's base MOVED …`;
   * `A12.1: CURVE_FIRECHECK=ckpt-ddl was requested but no axis-(ii) checkpoint truncated …` (A13.9);
   * `the parent's first lease pass did not finish …` (PARENT_PASS);
   * `the production database did not close cleanly …` (CLOSE);
   * `LEASE …` (item 4).
4. **Z4: A18.2 extends to the lease thread's end state.**
   * **The harness reads the `LeaseStats` at EVERY close:** the child's, the reopen before a merge batch, the parent's
     before each restart, and the final one. A thread that ended with `panicked` (item 5), `failed > 0` or
     `refused_branches > 0` pushes a `LEASE <where>: …` failure.
   * **The child** prints `lease_panicked=`, `lease_failed=`, `lease_refused_branches=` and `close_ok=` on its
     `RESTART_RESULT` line, and no longer panics on a failed close. The parent's `restart_guards` pushes `LEASE N=<n>
     (the child's close): …` for the first three, and `CLOSE N=<n> (the child's close)` for `close_ok = 0`.
   * A missing field reads as `u64::MAX`, so it fires.
   * The mid-run closes still panic on a failed close, as before, but only after their LEASE line is pushed.
5. **D265 (this lane's, ledger @ `e75426c`): a panicked lease scan thread says so, and the CLI fails.**
   * **The engine change.**
     * `LeaseStats` gains `panicked: bool`, set by a guard in the scan thread's body when the thread unwinds
       (`std::thread::panicking()` in its `Drop`). `stats()` and `stop()` return it.
     * `OpenDatabase::close` returns `Err` when it is set, after its checkpoints have run, so `run_cli` exits
       non-zero.
     * `run_cli` prints the stats line when it is set.
     * `Drop` still only reports.
   * **The test seam, compiled into unit tests only (`#[cfg(test)]`).** `scan_once` panics once, at its top, for a
     reaper armed by address. So one test kills one lease thread and no other thread in the test binary.
   * **Red first, on the base API.** The red commit adds the seam and two unit tests in
     `src/branch/lease_thread/tests.rs`:
     * (a) a scan thread that panicked says so in what `stop()` returns. This is read through `Debug`, so it compiles
       before the field exists;
     * (b) the shipped close (`cli::open_database`, then `close`) errs when its lease thread died.
     * **Both FAIL at the red commit, and pass at the fix.**
   * **The fix commit** adds (c), a negative control: a healthy thread's `stop()` reads `panicked = false`.
   * **The mutant, pre-registered:** drop the flag (the guard's `Drop` does not store). (a) and (b) must FAIL, and (c)
     still passes.
6. **Z5: the ARM3 refusal reads "arm 3: N restart(s) counted (H6 rows left out); one point is not a curve".** A18.1's
   "measured" text is withdrawn: "measured" was false beside H6 rows that were measured.
7. **Z6: one binding.** `counted` is computed once, and both the L2 statement and the ARM3 refusal read it.
8. **Counts.**
   * Fire modes: 22.
   * New `#[test]`s over `9aa6968`: **44**, the previous 41 plus (a), (b) and (c).
   * The lib's own test count rises by 3.

**A21, 2026-09-24, before any build or run. Two lead decisions from the verdict script's round 4 (artie-research
FAN-QUEUE #18 note, `8049b26`).**

1. **A mid-run close RETURNS its failure to `main`; it no longer panics.**
   * **The defect.** A20.4 pushed each mid-run close's LEASE line into `failures`, then `expect`ed the close's
     `Result`. A close that failed, for example on a dead lease thread (D265), panicked before the summary could print
     the LEASE line. So a lease-thread death showed up only as a panic.
   * **Now**, both mid-run closes (the one before a merge batch, and the parent's before each restart) push
     `CLOSE <where>: <error>` into `failures` and continue. The run reopens and carries on, and the summary prints
     every LEASE and CLOSE line with the others, as A18.2 does for the final close.
   * **Stated limit.** An OPEN that fails still panics, as before: nothing can run without a database. A failure that
     already sits in `failures` is lost then, and that exit is the panic, rc 101.
2. **(h)'s warmth comparison is REPORTED, not judged.**
   * The comparison is `child net ms` (child total less `recover` and `rebuild`) against `parent total ms` (A9.1's
     restatement of §3).
   * **Why reported.** No band was registered for it before data. A9.1 says "compared", §3's "parent reopen ≪ child
     open" names no number, and a band chosen now, with the lane this near to (h), could not be shown to predate the
     data.
   * The arm-3 summary prints a line saying so.
3. **Counts.** Fire modes: 22. New `#[test]`s over `9aa6968`: 44. Both unchanged.

**A22, 2026-09-24, before any build or run. Review 12 of `edbe154..03a7ea0` (artie-research
`frontier/read_vs_n_review12.md` @ `4c12762`, read at `6d1c587`) returned SOUND-WITH-CAVEATS. D265 holds. The lead's
decisions on A1–A4, A7–A9, B3 and B4 follow.**

1. **A1: D265's exit extends to pgserver, the other registered entry point.**
   * **One check serves both entry points: `LeaseStats::ended_alive()`**, which returns `Err` when the scan thread
     panicked.
     * `OpenDatabase::close` returns its result after the checkpoints, as `run_cli` needs.
     * `examples/pgserver.rs` calls it after its final arena checkpoint and PANICS on `Err`. That follows the file's
       own rule: a panic rather than `process::exit`, so `<db>.lock` is released. The server therefore exits
       non-zero.
   * **Red first.** `tests/d265_entry_points_check_the_lease.rs`, (d): every registered entry point's production text
     (`src/cli/cli.rs`, `examples/pgserver.rs`, cut at the first bare `#[cfg(test)]` line as in
     `open_path_allowlist`) calls `.ended_alive()`.
     * It reads files only, so it compiles at the red commit, where it FAILS.
     * The fix commit adds (e): `ended_alive()` returns `Err` for `panicked = true` and `Ok` for a default
       `LeaseStats`.
   * **Mutant:** delete pgserver's `.ended_alive()` call. (d) must FAIL.
   * **Stated limit, unchanged.** No production reader polls `stats()` during a session. A server that is killed,
     rather than shut down, never surfaces the death.
2. **A2: no attribute line in `lease_thread.rs`'s production region.**
   * **The defect.** The red commit's `#[cfg(test)] scan_seam::fire(reaper);` hid 248 lines from the d53 tripwire:
     `tests/d53_private_root_allowlist.rs` cuts at the first SUBSTRING `#[cfg(test)]`. The view shrank from 866 lines
     to 618, as review 12 measured.
   * **Now:**
     * `scan_once` calls `scan_seam::fire(reaper)` unconditionally;
     * at the END of the file, a `#[cfg(not(test))]` module defines `fire` as an empty `#[inline(always)]` fn;
     * then the `#[cfg(test)]` module holds the real seam;
     * then `mod tests`.
   * `#[cfg(not(test))]` does not contain the substring `#[cfg(test)]`, so both tripwires again read every line of
     production code.
   * The d53 test is not touched.
3. **A3: (a4)'s expectation is corrected.**
   * At the tip, tests (a)–(c) pass, and libtest prints a passing test's captured output nowhere. The lease thread
     inherits that capture, so the seam's panic text ("D265 test seam: …") is NOT in (a4)'s file.
   * `lease: the scan thread panicked …` IS in it: `report()` writes with `writeln!(std::io::stderr(), …)`, which the
     capture does not intercept.
   * The seam's text appears in (a5), where (a) and (b) fail and their capture is printed.
4. **A4: the FIRECHECK line prints each id WITH where the verdict script grades it.**
   * `Fire::expects()` now returns `(id, At)` pairs. `At` is `Any`, `First`, `Last`, `Every`, `AfterFirst` or
     `AxisTwo`. They print as `N=<n>` or `axis ii M=<m>`, taken from the run's registered checkpoints and M targets.
   * At the registered commands:
     * control-cold: G5 at N=2048 (`Last`);
     * merge-quarantined: M1; allowed M5, and G1 at N=2048 (`AfterFirst`);
     * extra-branch: G1 at every checkpoint;
     * extra-extent: H2 at every checkpoint;
     * orphan-extent: H4 at every checkpoint;
     * no-cluster-time: H5 at N=2048 (`Last`);
     * pinned-checkpoint: M6 at every axis-(ii) target;
     * stale-marker: H6 at N=256 (`First`); allowed ARM3.
   * The line also says that the script grades further shape rules it does not print: A17.2's shape, H2's +1, H4's
     `freed = 1`, and (f2)'s CKPT lines.
5. **B3: the parent's two mid-run CLOSE lines are mirrored on stderr**, as `the parent's close failed (<where>): <error>`,
   like the child's. A reopen that panics after a failed close can no longer lose the cause. The verdict script needs
   the pattern.
6. **A7: a dead lease thread is not waited out as a slow one.**
   * `wait_first_pass` now returns Done, TimedOut or Died, reading `stats().panicked` (D265).
   * The parent's open pushes `LEASE (the parent's open): the lease thread died on its first pass …` instead of
     PARENT_PASS, whose "a sweep may overlap a timed window" is false for a dead thread.
   * The child's H5 is not pushed when `lease_panicked ≠ 0`. Its LEASE line names the cause.
7. **A8, A9 and B4 (nits).**
   * A20.4's block moves after H6's check, so H6's comment heads H6's `if` again.
   * Arm 3's counted ROWS are bound once (`counted_rows`), and read for L2, ARM3, the load median and the span legend.
   * The restart's byte-size comment no longer says "clean close" (A21.1).
8. **Counts.** Fire modes: 22. New `#[test]`s over `9aa6968`: **46**, the previous 44 plus (d) and (e).
