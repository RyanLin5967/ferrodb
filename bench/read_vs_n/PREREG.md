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
