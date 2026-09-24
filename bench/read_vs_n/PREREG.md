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
