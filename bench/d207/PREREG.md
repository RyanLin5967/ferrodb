# D207 — pre-registration, written BEFORE any build or run

Branch `d207-transport-port`, base main `9aa6968`. Red tests at `6ceb719`. The fix follows this file.
Nothing on this branch has been compiled or run (quiet mode), so every line below is a prediction.
Amendments append only.

**Instrument**, for every count below: `timeout 1800 cargo test --lib consensus::transport::tests::`
from the worktree, at default QoS (never `taskpolicy -b`), under the shared suite lock. The filter
is a substring match on the module path `consensus::transport::tests::` (`src/lib.rs:21`
`pub mod consensus`, `src/consensus/mod.rs:62` `pub mod transport`, `transport.rs` `mod tests` at the
file's end). The five new names occur once each in `src/`, so no other module matches.

**Counts.** `src/consensus/tests_transport.rs` has 45 `#[test]` at `9aa6968` and 50 at `6ceb719`.
The file has no test macros, and only one new `#[cfg]`: the macOS test. So the module runs **50
tests on macOS** and **49 on Linux and Windows**. Counted from the attributes, not from a run.

## Run R — RED, at `6ceb719` (tests added, `src/consensus/transport.rs` untouched)

Predicted on macOS: **compiles; `45 passed; 5 failed`**, rc=101. On Linux and Windows: `45 passed;
4 failed` (no macOS test). A compile error is a failure of this pre-registration: record it, then
fix it in its own commit.

| test | predicted failure (the assertion that fires, with its values) |
|---|---|
| `a_connection_whose_socket_setup_fails_releases_the_slot_it_reserved` | `leaked == 0` fails with **left 4, right 0**, after its 10 s wait. The first 4 connections reserve a slot and leak it at the failed setup; connections 5–8 are refused as cap-full. `taken == 8` passes before it. |
| `a_peer_that_resets_before_accept_cannot_fill_the_connection_cap` (macOS) | the `answered` assertion: the real peer gets EOF instead of a handshake, with **`live_inbound_conns=4`** and `refused_conns` between 1 and **13** (13 if all 16 resets reach the failed-setup exit: 12 cap-full resets plus the real peer). ⚠ CONDITIONAL on 207d362's recorded trigger (below). |
| `one_unreachable_peer_cannot_queue_more_than_the_default_byte_bound` | `held <= 67108864` fails: **`held` = 88081332 bytes in 12 frames**. The frame is 7340111 bytes (5 header + 21 envelope + 1 kind + 24 + 4 count + 16 entry + 1 tag + 8 lsn + 4 len + 7340032 payload), so `fit` = 9 and the fixture guard passes (12 < 1024, 12 > 9). |
| `one_frame_cannot_spend_more_config_nodes_than_its_whole_budget` | the `Ok(_)` arm panics: "five configurations of 1024 members in one frame were accepted". |
| `a_frame_whose_configurations_exceed_the_budget_is_refused_to_its_sender` | the `Ok(f)` arm panics: "…was framed (**20735** bytes)". 20735 = 5 + 45 + 5 × 4137. |

All 45 tests that exist at `9aa6968` pass.

## Run G — GREEN, at the fix commit

Predicted: **compiles; 50 passed, 0 failed** on macOS (49 on Linux and Windows).

Specific values the new tests read on the fixed tree:

- setup-failure test: `taken` 8, `leaked` 0, `refused_conns` **exactly 8**. The slot is released at
  each failure, so the map never holds more than 1 and nothing is refused as cap-full.
- reset test (macOS): answered; `refused_conns` ≥ 1 (only failed setups can be refused, because the cap
  of 4 is never reached); `live_inbound_conns` returns to 0.
- byte-bound test: `held` = 9 × 7340111 = **66060999** ≤ 67108864; queue terms **[4..=12]**;
  `dropped_to(n2)` **3**.
- frame-budget decode test: refused with "budget is left" and "entry 4 of 5"; four configurations decode.
- encoder twin: refused with a message containing "budget"; four configurations round-trip.

## The trigger the macOS test depends on, and what each outcome means

207d362's message records, as a reviewer's measurement on this platform, that a peer which connects
and RSTs before `accept()` leaves a socket on which `SO_RCVTIMEO` returns EINVAL, 40 of 40 trials.
**I have not reproduced it.** Two ways it could fail to hold, each visible in the runs:

- `accept` does not hand back a reset socket (for example, std's `accept` returns an error when the
  kernel gives no peer address). Then nothing is reserved, and on BOTH trees the real peer is
  answered and `refused_conns` is 0. Run R fails at the anti-vacuity assertion instead of
  `answered`. That falsifies the claim that an unauthenticated remote can reach this exit on macOS.
  The portable test still pins the exit. D207's severity is then "any setup failure leaks" rather
  than "a remote can make the node deaf".
- The setup succeeds on a reset socket. Same observable, same reading.

## What would falsify the diagnosis

- The setup-failure test passing at `6ceb719`, or failing there with `leaked` ≠ 4.
- The byte-bound test at `6ceb719` failing at any assertion other than `held`, or with a `held` other
  than 88081332.
- Any of the 45 existing tests failing at `6ceb719`. The red commit touches no line that exists at
  `9aa6968` (`git diff 9aa6968 6ceb719 --numstat` = `352 0`).

---

## Amendment 1 — added after the fix commits and BEFORE any run (still nothing built)

Commits since the red set: `cfe9cf5` the fix; `9287b34` four tests that need the ported API;
`dc6bf4e` a failed spawn is counted too; `48f0265` a test pinning the guard's lifetime. The mutants
are generated from `48f0265` by `bench/d207/make_mutants.py 48f0265` into `bench/d207/mutants/`,
and every patch passes `git apply --check` against that tree (checked; applying is not a build).

**Counts at `48f0265`**: 55 `#[test]` in `tests_transport.rs`, one of them macOS-only, so the module
runs **55 tests on macOS** and 54 on Linux and Windows. Ten are new to D207:

| key | test | since |
|---|---|---|
| A | `a_connection_whose_socket_setup_fails_releases_the_slot_it_reserved` | `6ceb719` (red) |
| B | `a_peer_that_resets_before_accept_cannot_fill_the_connection_cap` (macOS) | `6ceb719` (red) |
| C | `one_unreachable_peer_cannot_queue_more_than_the_default_byte_bound` | `6ceb719` (red) |
| D | `one_frame_cannot_spend_more_config_nodes_than_its_whole_budget` | `6ceb719` (red) |
| E | `a_frame_whose_configurations_exceed_the_budget_is_refused_to_its_sender` | `6ceb719` (red) |
| F | `the_outbound_byte_count_returns_to_zero_as_the_sender_drains` | `9287b34` |
| G | `whichever_bound_binds_first_and_a_frame_over_the_whole_byte_bound_is_still_queued_alone` | `9287b34` |
| H | `learners_spend_the_same_frame_budget_as_members` | `9287b34` |
| I | `the_frame_budget_is_four_maximal_member_lists` | `9287b34` |
| J | `an_open_connection_holds_its_slot_until_it_closes` | `48f0265` |

Two pre-existing tests are named below: **CAP** =
`concurrent_inbound_connections_are_capped_and_the_refusals_are_counted`, **MIL** =
`a_configuration_naming_more_nodes_than_the_wire_allows_is_refused_before_any_id_is_read`.

F–J cannot be red against `9aa6968`: F, G and I use fields or a constant that do not exist there,
and the tip's test file does not compile against it (`fast()` names `queue_bytes`). H and J would
compile there in isolation; H would fail there and J would pass. Their red evidence is the mutants
below, not a run on main.

### Run G, amended: GREEN at `48f0265` (replaces "at the fix commit")

Predicted: **compiles; 55 passed, 0 failed** on macOS (54 on Linux and Windows). A–E read the
values already registered above. F: queue empty, `bytes` 0. G: all three cases as coded. H: refused
with "budget is left" at "entry 2 of 3"; two decode; the encoder refuses three with "budget"; two
round-trip. I: passes. J: `refused_conns` exactly 20, `live_inbound_conns` exactly 4, then 0.

### Mutants — run the whole module once per mutant, from a clean `48f0265`

Command per mutant (M8 excepted): `timeout 900 cargo test --no-fail-fast --lib consensus::transport::tests::`.
Predicted on macOS; on Linux and Windows drop B. "Fails" lists every test predicted to fail; the other
tests of the 55 pass.

| mutant | what it puts back | fails | the assertion that fires |
|---|---|---|---|
| M1 `setup_exit_forgets_slot` | **the D207 leak** — the slot outlives a failed setup | A, B | A: `leaked` left **4**, right 0. B: `answered` false (conditional, as in Run R) |
| M2 `setup_exit_uncounted` | a failed setup closed with no number attached | A, B | A: `refused_conns` left **0**, right 8. B: the anti-vacuity `refused_conns > 0` |
| M3 `slot_released_at_thread_start` | the guard dropped at the top of `conn_loop` | J | J: `refused_conns` left < 20, right 20. CAP may also fail, by timing; not scored either way |
| M4 `byte_bound_removed` | **the D207 outbound defect** — a depth-only bound | C, G | C: `held` **88081332**. G: "must keep the ten newest", left `[1..=15]` |
| M5 `drop_not_refunded` | bytes not given back when a frame is dropped | C, G | C: `queue.len()` left **1**, right 9. G: "the byte counter disagrees" (150 vs 10) |
| M6 `pop_not_refunded` | bytes not given back at the sender's pop | F | F: `bytes` left **460** (20 frames x 23 bytes), right 0 |
| M7 `oversized_frame_refused` | a frame over the whole bound is dropped | G | G: "must be queued, alone", left `[1, 2, 3, 4, 5]` |
| M8 `empty_guard_removed` | the drop loop does not stop at an empty queue | G never returns | run G alone: `timeout 300 cargo test --lib -- --exact consensus::transport::tests::whichever_bound_binds_first_and_a_frame_over_the_whole_byte_bound_is_still_queued_alone` → **rc=124** |
| M9 `decode_budget_not_charged` | **the D207 frame defect** — the budget is never spent | D, H | D: the `Ok` arm, "five configurations … accepted". H: "three configurations … accepted" |
| M10 `budget_checked_before_cap` | the shadowing that made `1b3a6a6` edit MIL's assertion | MIL | MIL: "was not refused by the node cap: … budget is left" |
| M11 `learners_uncharged` | only members spend the budget | H | H: the decoder half's `Ok` arm |
| M12 `encoder_budget_removed` | the sender frames what every peer refuses | E, H | E: "was framed (**20735** bytes)". H: "were framed (**24749** bytes)" |
| M13 `fresh_budget_per_entry` | a per-configuration budget, which is `1b3a6a6`'s own premise | D, H | as M9 |
| M14 `spawn_failure_uncounted` | a failed spawn not counted | **none: SURVIVES** | unkillable: it needs `pthread_create` to fail, which no test can arrange |

Each patch must apply and leave the tree dirty, or the run is refused rather than scored. A mutant
that fails MORE tests than listed is a finding about coupling, not a pass; one that fails FEWER is a
guard that cannot fire and blocks landing.

### Collateral, at `48f0265`

- `timeout 1800 cargo build --examples` (the integration tests spawn `examples/consensus_node.rs`).
- `timeout 1800 cargo test --no-fail-fast --lib consensus::` — every test passes.
- `timeout 1800 cargo test --no-fail-fast --test d154_length_prefix_refusal --test integration_cluster_agents --test integration_cluster_snapshot --test integration_server_stdout`
  — every test passes. These are every target outside `src/consensus/` that names the transport or
  the node (`git grep -ln "consensus::transport\|consensus::node\|TransportOptions\|from_listener"`).
  INFERRED: I did not read them all. A failure means the 64 MiB byte bound or the frame budget
  changed behaviour on a path I did not read. The first suspect is snapshot transfer, whose 1 MiB
  chunks (`SNAPSHOT_CHUNK_BYTES`) now meet a byte bound as well as the depth.
- Per-target suite: `VERIFY_MODE=per-target tools/verify-suite.sh d207`. Predicted **2589 passed,
  0 failed** on macOS: main's 2579 plus the ten tests above. The 2579 is the lead's measurement at
  `9aa6968` (FAN-QUEUE #9). I have not reproduced it, so it is INFERRED here.

---

## Amendment 2 — the rest of 207d362 that the lead scoped in, written BEFORE its fix (nothing built)

Lead decision (2026-09-24): port three more items, red first. (1) Refuse `idle_deadline = 0` and
`max_inbound_conns = 0` at bind. (2) The cap-full backoff. (3) The lost wakeup at the two redial
waits. **Not** the `inbox_bytes ≥ MAX_FRAME_BYTES` floor, which is a decision for Ryan.

Red tests at **`2466ffc`**, added on top of `196f9aa`; additions only (`138 0`). The module has 59
`#[test]` there, one of them macOS-only, so it runs **59 on macOS** and 58 on Linux and Windows.

| key | test |
|---|---|
| K | `a_zero_idle_deadline_is_refused_at_bind` |
| L | `a_connection_cap_of_zero_is_refused_at_bind` |
| P | `refusals_at_a_full_cap_are_paced_by_the_poll_interval` |
| W | `a_shutdown_during_a_dial_does_not_wait_out_the_reconnect_delay` |

### Run R2 — RED at `2466ffc` (the D207 fix present, these four items not)

Same instrument. Predicted on macOS: **compiles; 55 passed, 4 failed**, rc=101.

| key | predicted failure |
|---|---|
| K | the `Ok` arm: "a zero idle_deadline was accepted at bind" |
| L | the `Ok` arm: "a max_inbound_conns of zero was accepted at bind" |
| P | `c1 - c0 == 8` passes. `took >= 700ms` fails, with `took` **under 300 ms**. Unpaced, the 8 refusals finish within one or two 100 ms accept polls of the last connect. INFERRED bound: a loaded box could stretch it, and past 700 ms P would pass on this tree. That would be a missed red, never a false one, and is read as "re-run on a quieter box". |
| W | shutdown takes **between 30 s and 31 s**: the whole `reconnect_delay`. The `< 10 s` assertion fails |

### Run G2 — GREEN at the fix

Predicted: **compiles; 59 passed, 0 failed** on macOS (58 elsewhere). K and L are refused by name.
P: all 8 are refused, and `took` ≥ 700 ms (guaranteed by the sleeps, and load can only add to it).
W: shutdown returns in **under 1 s**: the dial sees the stop flag within one 5 ms poll, and the
sender then leaves without waiting. J and CAP each take about 20 x 5 ms longer, well inside their
deadlines.

---

## Amendment 3 — the fix for amendment 2, and the mutants re-cut (still nothing built)

The fix is `dacc149`, followed by `cb05329`. `cb05329` drops the redial helper's bool: `stopped` is only
ever set after the stop flag (`shutdown` and `stop_started`, READ), so the loop head already takes that exit.
All 18 mutants are re-generated from **`cb05329`** by `bench/d207/make_mutants.py cb05329`, and each passes
`git apply --check`. M1–M14 have the same edits as in amendment 1; only their hunk line numbers moved.

**Run G2, restated at `cb05329` and at the tip:** **59 passed, 0 failed** on macOS (58 elsewhere), with
the values in amendment 2.

### The mutant table, re-cut for 59 tests (it supersedes amendment 1's table)

Same command per mutant; M8 is still run alone. Every test not listed passes.

| mutant | fails | changed from amendment 1 |
|---|---|---|
| M1 | A, B | — |
| M2 | A, B | — |
| M3 | **J, P** (CAP may also fail, by timing; not scored) | **P added.** Its precondition `live_inbound_conns == 1` reads 0, because `drop(_registration)` runs before the connection thread reads the handshake, so it has already run by the time the client has the reply. Deterministic. |
| M4 | C, G | — |
| M5 | C, G | — |
| M6 | F | — |
| M7 | G | — |
| M8 | G never returns, rc=124 | — |
| M9 | D, H | — |
| M10 | MIL | — |
| M11 | H | — |
| M12 | E, H | — |
| M13 | D, H | — |
| M14 | **none: SURVIVES** by design | — |
| M15 `zero_idle_deadline_accepted` | K | new. The `Ok` arm |
| M16 `zero_cap_accepted` | L | new. The `Ok` arm |
| M17 `cap_full_unpaced` | P | new. `took` under 300 ms against the 700 ms floor. On a box loaded enough to stretch it past 700 ms, P passes and M17 survives. That reads as "re-run quieter", not as the backoff being untestable. |
| M18 `lost_wakeup` | W | new. Shutdown takes 30 to 31 s |

**Per-target, restated:** **2593 passed, 0 failed** on macOS = main's 2579 (INFERRED, the lead's number)
plus the 14 D207 tests.

### What is still not ported, stated so it can be decided

The `inbox_bytes ≥ MAX_FRAME_BYTES` floor. It would refuse the existing
`an_undrained_inbox_is_bounded_in_bytes_and_every_refusal_is_counted` at its own setup. At `9aa6968`:

- the fixture at `tests_transport.rs:1528` is `opts.inbox_bytes = 4096`;
- `:1531` is `Transport::from_listener(…).unwrap()`, which would panic on the new refusal;
- the premises built on 4096 would all need changing too: the 1 KiB payload, the `for _ in 0..40` writes at
  `:1560`, the assertion `t.inbox_bytes() <= 4096` at `:1575`, and `const SENT: u64 = 40` at `:1594`,
  against which `drained + inbound_dropped() == SENT` is checked.

`207d362` made exactly that edit (fixture → `MAX_FRAME_BYTES`, 1 MiB payloads, 16 frames written from a
thread, the bound assertion → `<= MAX_FRAME_BYTES`). It is a test edit, and the lead has filed it as
Ryan's decision.

---

## Amendment 4 — the fresh review of `dd9d1e1`, written BEFORE its fixes (nothing built)

Source: `artie-research frontier/d207_review.md` @ `1b37542`, verdict SOUND-WITH-CAVEATS. The lead
verified F1 and F2 and ruled as follows. Remove the quadratic at its source, then remove the per-frame
budget: removed rather than guarded, because it no longer protects anything and it causes the stall.
Changes to my own unlanded tests go through this amendment.

### Every step that decoding one configuration costs, READ at `dd9d1e1`

M is the member count and L the learner count, each ≤ `MAX_CONFIG_NODES`.

| step | where | cost |
|---|---|---|
| the two id lists, strictly ascending | `decode_node_list` | O(M + L) |
| voter/learner disjointness | `first_common`, a two-pointer merge | O(M + L) |
| `Config::new`: collect, `sort`, `dedup` | `config.rs:54-58` | O(M log M) |
| `with_learners`: collect, `sort`, `dedup` | `config.rs:62-64` | O(L log L) |
| **`with_learners`: `l.retain(\|n\| !self.members.contains(n))`** | **`config.rs:67`** | **O(L × M), the only quadratic** |
| the canonical comparison back against the wire | `decode_config` | O(M + L) |

The rest of a frame's decode is linear in its bytes: the entry loop, `take_bytes`, and `decode_catalog`,
whose `RecKind::deserialize` has one loop, over the columns. `transport.rs` has no other `contains`, `find` or
`position` in code (READ: `grep`). `members` is always sorted and deduplicated, because its field is private
and every constructor goes through `Config::new` or is empty (READ `config.rs:28-125`). So the fix is a
`binary_search` per learner. That makes a configuration O((M + L) log(M + L)), and a frame linear in its
bytes up to that log.

Claims this falsifies, to be rewritten in the fix:

- `transport.rs:784`, the per-config refusal: "the comparisons a configuration costs are quadratic in its
  two list lengths";
- `:137`, the doc for `MAX_CONFIG_NODES`: "the quadratic term is about a million comparisons".

`:795` is the budget's own message, and it goes with the budget.

### Run R3 — RED at **`01bd8ae`** (the tree at `dd9d1e1` plus one test)

**Q** = `a_leaders_full_catch_up_append_of_membership_entries_is_delivered`. The module has **60** tests on
macOS there (59 elsewhere). Predicted: **59 passed, 1 failed**. Q panics at `a.send` with "…refused to its
sender: a configuration holds 1024 nodes, but only 0 of this frame's 4096-node budget is left…". The
encoder's budget runs out after two configurations of 1024 + 1024 ids, so the third is refused.

### Test changes, registered here before they are made

- **Remove D, E, H and I.** They assert the budget, which is removed. D and E are still run and
  still fail as registered in Run R at `6ceb719`, where they exist.
- **C** (review F5): pin `FRAME_LEN = 7_340_111` and `FIT = 9` as literals. The encoder's length becomes
  a premise that is asserted, not the source of the expected values. C's three dependent assertions
  are otherwise unchanged.
- **W** (review F4): `poll_interval` goes from 5 ms to 200 ms, and "deterministic" is rewritten.
  - On the fixed tree W stays deterministic.
  - On the red tree, and under M18, the red needs `shutdown`'s locked set-and-notify, a few µs, to land
    before the sender's dial next checks the stop flag. That moment is spread over a whole poll, so the
    chance of a missed red is about µs / 200 ms (INFERRED), down from about µs / 5 ms. M18's kill stays
    probabilistic, and it is recorded as such.
- **New post-fix test X**, `removing_voters_from_a_learner_list_is_a_binary_search_per_learner`. It
  drives the new `config::retain_absent` with an `Ord` type that counts comparisons: 1024 items against
  512 sorted references (the evens).
  - Correctness: the odd 512 remain.
  - Cost: **≤ 1024 x 11 + 512 = 11,776** comparisons. That is one `binary_search` of at most 10 per item,
    plus at most 511 for the ascending `debug_assert`.
  - A per-item scan would cost about 512 x 256.5 + 512 x 512 = **393,472** (INFERRED arithmetic).

### Correction to line 27 (review F6)

The itemisation reads "21 envelope". The envelope is **16** bytes: `from` 4, `to` 4, `term` 8. With 16,
the parts add up to 7,340,111. The total and everything computed from it (88,081,332; 66,060,999;
`fit` 9) were already right.

### Fix, then Run G3 at the tip

The fix:

- `config::retain_absent` does a `binary_search` per item, and `with_learners` calls it.
- `MAX_FRAME_CONFIG_NODES`, its const assert and the budget threaded through both halves of the codec are
  removed; those functions go back to their `9aa6968` signatures.
- The doc and the two refusal messages for `MAX_CONFIG_NODES` are rewritten to what is left.
- The failed `try_clone` in `accept_loop` is counted in `refused_conns` (review F3).

Predicted at the tip: **57 tests on macOS (56 elsewhere): 60 − D − E − H − I + X. All pass.** Q delivers
the Append unchanged. Per-target: **2591** on macOS = 2579 (INFERRED, the lead's number) + A, B, C, F, G, J,
K, L, P, W, Q and X.

---

## Amendment 5 — after amendment 4's fixes, BEFORE any run (still nothing built)

Commits since amendment 4 (`576a7f9`):

| commit | what |
|---|---|
| `e5bb853` | the registered test edits: D, E, H and I removed; C pinned; W's poll set to 200 ms |
| `ad7442e` | the fix: `config::retain_absent`; the budget removed; the failed `try_clone` counted |
| `ce0f3a6` | test **X** |

**Counts at `ce0f3a6`:** 57 `#[test]` in `tests_transport.rs`, one of them macOS-only, so **57 on macOS**
and 56 elsewhere, matching amendment 4.

The codec half of `transport.rs`, everything before "Reading frames off a socket", differs from `9aa6968`
in only two hunks: the `MAX_CONFIG_NODES` doc and its decoder refusal message (READ:
`git diff 9aa6968 ce0f3a6 -- src/consensus/transport.rs`). MIL still finds "over the" and "limit" in
the new message.

### Run G3 — GREEN at `ce0f3a6` and at the tip

- **57 passed, 0 failed** on macOS (56 elsewhere).
- Q: the 64-entry Append is delivered unchanged.
- X: `compared` is **at most 11,776**. INFERRED: std's `binary_search` makes 10 comparisons over 512
  items, so about 10,240 + 511 = **10,751**. `kept` is the 512 odd ids.
- All other values are as in amendments 1–3.

### Collateral, restated

The same command list as amendment 1. `--lib consensus::` now also covers the behaviour of
`retain_absent`, through `tests_contract`, `tests_membership`, `tests_log` and `tests_replicate`, which
all build learner lists: all pass. Per-target: **2591** on macOS.

**New: a dead-code control**, `RUSTFLAGS="-D duplicate_macro_attributes -D dead_code" timeout 1800
cargo build --lib`, the CI gate. At the tip: **rc=0**. It is the negative control for M20.

### The mutant table, re-cut for 57 tests (supersedes amendment 3's)

Generated from `ce0f3a6`; every patch passes `git apply --check`. M9–M13 mutated the budget, which no
longer exists, and are retired rather than renumbered. Same command per mutant, except M8 and M20.
Every test not listed passes.

| mutant | fails | notes |
|---|---|---|
| M1 | A, B | — |
| M2 | A, B | — |
| M3 | J, P | CAP may also fail, by timing; not scored |
| M4 | C, G | — |
| M5 | C, G | — |
| M6 | F | — |
| M7 | G | — |
| M8 | G never returns | run alone: rc=124 |
| M14 | **none: SURVIVES** | unkillable: needs `pthread_create` to fail |
| M15 | K | — |
| M16 | L | — |
| M17 | P | on a loaded box, a pass here is a missed red |
| M18 | W | very likely, not certain (amendment 4) |
| M19 `retain_scans` | **X** | `compared` about **393,983** (393,472 + 511) against the 11,776 ceiling. Q still passes, just slower, which is the point: the defect is cost |
| M20 `with_learners_scans_directly` | **none in the module run: SURVIVES** | Behaviour is identical, and X drives the helper, not `with_learners`. **Killed by the CI gate instead:** the dead-code build fails with "function `retain_absent` is never used", because nothing else in a non-test build calls it. The control at the tip is rc=0 |
| M21 `try_clone_uncounted` | **none: SURVIVES** | unkillable: needs `dup` to fail (EMFILE) |
| M22 `retain_inverted` | **X, Q** | X: the evens survive instead of the odds. Q: its fixture assertion `(1024, 1024)` reads `(1024, 0)`. The round-trip tests still pass, because `cfg()` loses its learner on both sides alike |

**Per-target, restated:** 2591 passed, 0 failed on macOS. That is 2579 (INFERRED, the lead's number)
plus A, B, C, F, G, J, K, L, P, W, Q and X.

### Found while fixing F1, and outside D207 (READ at `9aa6968`, INFERRED reachability)

The same stall, by a route that predates D207:

- `slice_from` caps an `Append` at 64 entries by count only (`replicate.rs:209-215`);
- each entry may be up to `MAX_ENTRY_BYTES` = 8 MiB − 4096 (`log.rs:105`);
- the encoder refuses a frame over `MAX_FRAME_BYTES` (`transport.rs:181`);
- `node.rs:742` discards that error.

So a follower 64 entries behind, with entries averaging over 128 KiB, would be offered a batch that is
refused on every turn. D207 removes only the route it added, the budget. Sizing a batch by bytes in
the leader, or counting refused sends, is a separate row for the lead.

---

## Amendment 6 — D220, written BEFORE its fix (nothing built)

The lead confirmed amendment 5's pre-existing stall at `9aa6968` and filed it as ledger row **D220**, to be fixed in
the leader: counting the refusals would still leave the follower stuck.

**Red tests** at **`9c4fa44`**, in `src/consensus/tests_replicate.rs`. Both compile against `9aa6968`: every
helper and every codec function they call is there (READ). The file adds lines only (`143 0`).

- **R** = `a_follower_far_behind_on_large_entries_catches_up_over_the_real_wire`.
  - The leader holds 64 entries of 1 MiB; the follower holds nothing.
  - Every message goes the way `node.rs` sends it: `encode_signed`, dropped if refused, then `decode_verified`.
  - It gets up to 20 heartbeats.
- **E** = `an_append_is_cut_where_a_signed_frame_would_overflow_and_not_a_byte_before`.
  - Two entries are sized by `encode_signed` itself to fill a signed frame exactly, and then to overrun it by one
    byte.
  - It expects 2 entries at the exact fit and 1 at one byte over, and every built Append must encode signed.

**Instrument:** `timeout 1800 cargo test --lib consensus::replicate::tests_replicate::`. The module has 55
`#[test]` at `9aa6968` and **57** at `9c4fa44`; there is no `#[cfg]` and no macro.

### Run R4 — RED at `9c4fa44`

Predicted: **55 passed, 2 failed.**

- **R** fails at `refused == 0`, with **left 20**. The Append for every heartbeat carries all 64 entries, about
  64 MiB, and the encoder's `room` check refuses it. The follower never answers.
- **E**: the exact-fit case passes (2 entries, and it encodes signed). The one-byte-over case fails at
  `entries.len() == want`, with **left 2, right 1**.
  - INFERRED arithmetic: `probe_body` = 45 + (29 + 4,194,304) + 29 + 32 = 4,194,439, so `exact` = **4,194,169**.

The transport module is unchanged by this commit: 57 passed.

### The fix, as it will be made

- **In `transport.rs`**, two measurements that come from the encoder rather than from arithmetic:
  - `entry_wire_len(e)` is the length `encode_entry` writes;
  - `append_entries_budget()` is `MAX_FRAME_BYTES`, less the unsigned body of an Append carrying no entries (which it
    encodes to measure), less `signing::MAC_LEN`. The state machine cannot know whether its transport signs.
- **In `replicate.rs`**, `entries_from` caps a batch at `MAX_ENTRIES_PER_APPEND` entries **and** at that byte
  budget. `LogTail::slice_from` always takes the first entry, and each later one only while the running total fits.
- **At least one entry is always sent.** An entry the log admits is at most `MAX_ENTRY_BYTES` = `MAX_FRAME_BYTES` −
  4096 on disk (`log.rs:105`, READ). Its wire form is at most **21 bytes** longer than its disk payload. READ:
  - the wire `Catalog` is the disk `Catalog` plus a u32 length, the RecKind tag and two page-id roots
    (`wal/log.rs:368-411`, `consensus/log.rs:1094`);
  - `WalBatch`, `Membership` and the fixed-size commands encode the same way in both.

  So one entry always fits the budget, with about 4,000 bytes to spare.

### Run G4 — GREEN at the fix

- `consensus::replicate::tests_replicate::`: **57 passed.**
- R: `refused` 0, and the follower holds round 64. INFERRED:
  - each Append carries 7 entries, because 7 × 1,048,605 = 7,340,235 ≤ the budget of 8,388,531, and 8 would take
    8,388,840;
  - that makes 10 Appends in the first heartbeat, under the keep-sending rule at `replicate.rs:1207-1218`;
  - about 22 messages are delivered in all.
- E: 2 entries at the exact fit, 1 at one byte over, and both encode signed.
- The existing `one_append_carries_a_bounded_number_of_entries` still gets 64: its 200 tiny entries bind on count.
- `consensus::transport::tests::` stays at **57 passed**; the fix adds no test there.

Per-target on macOS: **2593** = 2591 + R + E.

### Other senders that build a multi-part frame (READ, from `Body`'s variants in `consensus/mod.rs`)

| variant | how big it can get |
|---|---|
| `PreVote`, `PreVoteResp`, `RequestVote`, `RequestVoteResp`, `AppendResp`, `InstallSnapshotResp` | fixed-size |
| `InstallSnapshot` | one chunk per message: `snapshot.chunk(acked)` (`snapshot.rs:900`), at most `SNAPSHOT_CHUNK_BYTES` = 1 MiB (`:195`, `:569-573`), plus metadata whose configuration the cap limits to 1024 + 1024 ids. Well under 8 MiB, and not a count-capped batch |
| `Append` | the only batch, and D220 |

The transport's outbox is not a frame builder. It queues frames that are already encoded, and D207 bounded it in
bytes.

### Mutants for D220 (made from the fix commit; predicted against the replicate module)

| mutant | what it puts back | fails |
|---|---|---|
| **M23 `append_count_only`** | the count-only cap (`entries_from` passes `usize::MAX` bytes), as the lead asked | **R** (`refused` 20), **E** (one-byte-over case: 2 entries) |
| M24 `mac_not_reserved` | a budget without `MAC_LEN` | **E** (one-byte-over case: 2 entries) |
| M25 `cut_one_early` | `>=` for `>` | **E** (exact case: 1 entry, want 2) |
| M26 `envelope_not_reserved` | a budget without the Append envelope | **E** (one-byte-over case: 2 entries) |
| M27 `empty_when_first_too_big` | no at-least-one rule | **none: SURVIVES**. No entry the log admits exceeds the budget (above), so the rule never binds on legal input |

---

## Amendment 7 — D220's fix made, mutants cut, before any run (nothing built)

The fix is **`9bd9c7f`**. It follows amendment 6's plan with one addition: `encode_signed`'s body construction is
extracted, unchanged, as `message_body`, so that `append_entries_budget` measures exactly what `encode_signed`
frames. Test counts are unchanged:

- the transport module has 57 tests (the fix adds none there);
- the replicate module has 57.

**All 22 mutants are now re-generated from `9bd9c7f`**, and every patch passes `git apply --check`. M1–M22 have
the same edits; only their hunk line numbers moved. M23–M27 are new, with the kill sets and survivals registered in
amendment 6.

**Command for M23–M27:** `timeout 900 cargo test --no-fail-fast --lib consensus::replicate::tests_replicate::`.
They mutate the Append batch, which that module tests. The transport module is not run for them: they change
nothing its tests exercise except the refactored `message_body`, and no mutant touches that.

**Run G4 at the tip:** `consensus::replicate::tests_replicate::` gives **57 passed**, and
`consensus::transport::tests::` gives **57 passed**. Per-target: **2593**.

---

## Amendment 8 — D223 (the re-review of `dd9d1e1..5a86ad8`), written BEFORE its fix (nothing built)

Source: `artie-research frontier/d207_rereview.md` @ `66901bf`, verdict SOUND-WITH-CAVEATS. The lead verified R1,
and R1 and R2 are one hazard, now ledger row **D223**.

The ruling:

- one admission check, at proposal, before both the in-memory append and the disk;
- it equals the wire's own limits, taken from the encoder;
- `Transport::send` counts its refusals.

It also asked for the corrections listed below.

### Red tests at **`cb1b287`** (they compile against `5a86ad8`; additions only)

| key | test | module |
|---|---|---|
| **T1** | `a_membership_the_wire_cannot_carry_is_refused_at_proposal_and_never_reaches_the_log` (1025 learners) | `consensus::replicate::tests_replicate::` |
| **T2** | `a_wal_batch_one_byte_over_a_frame_is_refused_at_proposal_and_one_at_the_limit_is_admitted`, with the boundary as literals: budget 8,388,531, WalBatch overhead 29 | same |
| **T3** | `the_largest_entry_one_frame_can_carry_is_storable_and_reads_back` | `consensus::log::tests_log::` |

Counts: tests_replicate goes from 57 to **59**, and tests_log from 48 to **49**. Neither file has a `#[cfg]` or a macro.

### Run R5 — RED at `cb1b287`

- **replicate module: 57 passed, 2 failed.**
  - T1 fails at `after == before`, with **left 1, right 0**: `on_propose` appends anything.
  - T2's at-limit case passes. Its one-byte-over case fails at `after == before`, with **left 1, right 0**.
- **log module: 48 passed, 1 failed.** T3 panics with **`TooLarge { bytes: 8388539, limit: 8384512 }`**. The disk
  frame is 4 + 8 + 8 + (1 + 8 + 4 + 8,388,502) + 4 = 8,388,539, and `MAX_ENTRY_BYTES` = 8,388,608 − 4096.

### The fix, as it will be made

- **`transport::admit_entry(&Entry)`** encodes the entry with `encode_entry` and refuses in two cases:
  - the encoder refuses it, with the encoder's own error (a configuration over `MAX_CONFIG_NODES`, a name over
    u16, ...);
  - its length is over `append_entries_budget()`.

  Every limit it applies is the encoder's own.
- **`on_propose`** builds the candidate entry and calls `admit_entry` **before** `append_own_entry`. A refusal goes
  out as `Action::Refuse`, the same channel `NotLeader` uses, and neither the tail nor the disk sees the entry.
  - `append_own_entry`'s other caller is the election `NoOp`, one byte, which is always admissible; its doc will say
    so.
  - A follower's entries come from a frame the wire already carried.
- **The disk: `MAX_ENTRY_BYTES` becomes `MAX_FRAME_BYTES`.**
  - A disk frame is `24 + payload`, and a wire entry is `16 + command`. The command equals the disk payload for
    every kind except `Catalog`, where it is 13 bytes longer (re-review R3, READ).
  - So a disk frame is at most the wire entry + 8.
  - Anything one frame can carry, even unsigned (at most `MAX_FRAME_BYTES` − 45 on the wire), therefore has a disk
    frame of at most `MAX_FRAME_BYTES` − 37.
  - The disk then never refuses what the wire delivered, whether through admission or from a peer.
  - The read bound `MAX_FRAME` = `MAX_ENTRY_BYTES` + 64 follows with it.
  - `snapshot.rs`'s `MAX_SIDECAR_BYTES` (32 × `MAX_ENTRY_BYTES`) grows by 128 KiB. It is a receive ceiling, checked
    the same way on both sides.
- **Is the disk's own check now redundant, and should it stay? It stays, and it is not a second guard on the same
  condition.**
  - Admission binds at the wire's limit, which is 8,388,531 bytes of wire entry.
  - The disk check binds at a disk frame over 8 MiB, which is **strictly above anything any frame can carry** (37
    bytes of margin, unsigned). So it can never fire on an admitted or delivered entry, and cannot mask admission's
    mutants.
  - What it still guards is the log format's own pairing: a frame the log writes must be one its scan will read
    back (`MAX_FRAME`), for any caller of `RoundLog::append`.
  - Its mutants are killed at its own layer by the landed `an_entry_too_large_for_the_transport_is_refused_at_append`,
    which is written against the constant symbolically and still holds.
  - Removing it would let a direct caller write a frame the recovery scan refuses as corrupt: durable, and unreadable.
- **`Transport::send` counts every refusal:**
  - a new `unencodable` counter, for an encode refusal (this is the lead's ask);
  - a new `unaddressable` counter, for a send to itself or to a node with no address;
  - a stopped transport is already counted in `refused_after_stop`.

  With these, the comment beside `node.rs:742` ("counted by the transport") becomes true for every `Err`, and it will
  name the counters.

### Test changes, registered here before they are made

- **E**: pin `probe_body == 4_194_439` (45 + 4,194,333 + 29 + 32) as a literal premise (re-review R4).
- **Q**: fix the stale comment at `tests_transport.rs` ~2530 ("capped by count and nothing else"); comment only (R5).
- **New post-fix test T4** in the transport module, `every_send_the_transport_refuses_is_counted`:
  - an Append with a 1025-member configuration raises `unencodable()` by 1;
  - a send to itself and a send to an unconfigured node raise `unaddressable()` by 2;
  - `sent()` stays 0.
- **New post-fix test T5** in the replicate module, `an_entry_that_fits_no_frame_is_still_offered_alone`:
  - a leader seeded **around admission** (through `seed`, as a driver bug or a pre-D223 log would do) with a
    1025-learner Membership entry;
  - asserts `entries_from(1).len() == 1`;
  - this kills M27.

### Corrections to the record (append-only; the lines above stand as written)

- **Amendment 6's "at most 21 bytes longer"** (PREREG ~line 451) **is 13.** The page-id roots are u32 (READ
  `wal/log.rs:164-165`, `:397-398`), so it is 4 + 1 + 8. As whole entries, a `Catalog` wire entry is the disk frame
  + 5, and everything else is the disk frame − 8. The conclusion stood: 4,014 bytes spare against the old disk
  limit.
- **Amendment 6's M27 row** (PREREG ~line 491, "the rule never binds on legal input") **was false.** Before D223, a
  1025-learner Membership was admitted by `on_propose` and the disk alike. `entry_wire_len` returns `usize::MAX`
  for it, so the at-least-one rule bound. M27 survived only because no test existed. After D223 it cannot bind on
  anything that went through proposal, but it can on an entry placed around admission, and T5 kills it.
- The same false claim sits in `replicate.rs:214-215` and will be corrected in the fix.

### Run G5 — GREEN at the fix

| module | tests | predicted |
|---|---|---|
| replicate | 60 | all pass: 59 + T5 |
| log | 49 | all pass |
| transport | 58 on macOS (57 elsewhere) | all pass: 57 + T4 |

Per-target on macOS: **2598** = 2593 + T1 + T2 + T3 + T4 + T5.

### Mutants for D223 (cut from the fix; predicted)

| mutant | what it puts back | fails | modules run |
|---|---|---|---|
| M28 `admission_removed` | `on_propose` appends without admitting | T1, T2 (over case) | replicate |
| M29 `admission_cut_one_early` | `>=` for `>` in `admit_entry` | T2 (at-limit case refused) | replicate |
| M30 `admission_swallows_encoder_error` | an encoder refusal treated as admissible | T1 | replicate |
| M31 `disk_limit_below_wire` | `MAX_ENTRY_BYTES` back to `MAX_FRAME_BYTES − 4096` | T3 | log |
| M32 `unencodable_uncounted` | no count on an encode refusal | T4 | transport |
| M33 `unaddressable_uncounted` | no count on the two addressing refusals | T4 | transport |
| M34 `entry_wire_len_short` | `entry_wire_len` reports one byte less (the re-review asked for a mutant here) | E (one byte over: 2 entries), T3 (its premise `entry_wire_len == 8,388,531`) | replicate + log |
| M27 (re-cut) | no at-least-one rule | **T5** (was: survives) | replicate |

---

## Amendment 9 — D223's fix, tests and mutants, before any run (nothing built)

| commit | what |
|---|---|
| `c590265` | the registered test edits: E pins `probe_body == 4_194_439`; Q's comment corrected |
| `20d8d44` | the fix, as amendment 8 described |
| `010d3c4` | post-fix tests T4 and T5 |

Two small additions beyond amendment 8:

- `send`'s no-address message now names `unaddressable` instead of "every meter reads healthy". The one test that
  reads that message matches only its unchanged prefix, "no address is configured for n2".
- The module header no longer says a send after shutdown is "refused rather than counted". It is both, and it
  already was.

**Counts at `010d3c4`**, READ from the attributes:

| module | tests |
|---|---|
| `tests_replicate.rs` | 60 |
| `tests_log.rs` | 49 |
| `tests_transport.rs` | 58: 57 on non-macOS |

These match Run G5's predictions.

**All 29 mutants are re-generated from `010d3c4`**; every patch passes `git apply --check`. The edits to M1–M27 are
unchanged; only their hunk line numbers moved. M28–M34 are new, cut as amendment 8 registered them.

**Commands per D223 mutant:**

| mutants | command |
|---|---|
| M27, M28, M29, M30 | the replicate module |
| M31 | the log module, `timeout 900 cargo test --no-fail-fast --lib consensus::log::tests_log::` |
| M32, M33 | the transport module |
| M34 | both modules, `timeout 900 cargo test --no-fail-fast --lib -- consensus::replicate::tests_replicate:: consensus::log::tests_log::` (two libtest filters) |

Predicted kills are amendment 8's table, with M27 now killed by T5. **Run G5 at the tip**: replicate 60 passed, log
49 passed, transport 58 passed. Per-target **2598**.

---

## Amendment 10 — lane D73's three transport observations, before any run (nothing built)

Source: `artie-research frontier/lane_d73_pump_clock.md` §9. The lead forwarded it and asked that each observation be
verified. Doc commit **`749d399`**; the mutants are re-cut from it. Only hunk line numbers moved; all 29 patches
pass `git apply --check`. Test counts are unchanged: transport 58, replicate 60, log 49.

1. **O-2, "accept_loop may leak a slot and a descriptor when `set_nonblocking` or `set_read_timeout` fails": REAL
   at `9aa6968`, and it IS D207.**
   - READ: `transport.rs:1844` `map.insert(id, mine)`, then `:1849-1853` `continue` with no release.
   - It is fixed on this branch by `cfe9cf5`. `ConnRegistration` is made in the same critical section that reserves
     the slot, and it drops at that `continue`, which releases the slot and the dup'd descriptor `mine` it holds.
     The accepted `stream` itself is a local that drops at the same `continue`.
   - Its red test is **A** (`6ceb719`), registered in Run R at `leaked` 4. A's registry reading empty means both
     descriptors are gone, because the registry holds the only other one.
   - I found no other leak path at the tip, READ `accept_loop` at `749d399`:
     - a failed `try_clone` takes no slot, and `stream` drops;
     - cap-full: `mine` is never inserted and drops, and `stream` drops;
     - a failed spawn drops the closure;
     - every `conn_loop` exit drops `_registration` and `stream`.
2. **O-1, "the 60 s idle close may delay real failover on the wall clock": VERIFIED by reading.** The timing is
   INFERRED, not measured.
   - The receiver closes a connection silent for longer than `idle_deadline`: `conn_loop`,
     `if last_heard.elapsed() > opts.idle_deadline` at `:2167` (line numbers at `ec08152`; `749d399` only moved
     docs), and the default is 60 s (`:1155`).
   - During stable leadership, followers send only to the leader: an ack or refusal addressed to the Append's sender
     (`replicate.rs:828`, `:850`). Campaign frames go out only when an election starts (`election.rs:350-357`,
     `:401`). So each follower-to-follower connection goes silent and is closed by its receiver.
   - The sender never reads its socket after the handshake (`sender_loop`, `:1859` on; only `dial` reads), so it
     does not see the close.
   - The next write into the closed connection succeeds locally and the peer answers with a reset. It is the
     following write that fails, is counted (`:1929-1935`), and makes the sender redial. The repo already records
     this TCP behaviour: `tests_transport.rs:1871`, "the first write after a FIN may succeed, the peer answers RST,
     and the next one fails".
   - **So when the leader dies after 60 s of stable leadership, each survivor's first campaign frame to the other is
     lost uncounted, and its second is lost counted.** The vote they need only crosses on a later election round.
   - Not fixed here; §3 names the options.
3. **"The transport's doc claims every loss is counted": NARROWED** in `749d399`.
   - The module header, the `Counters` doc and the `idle_deadline` doc now say exactly which losses are counted:
     every drop and refusal the transport decides on, including D223's `unencodable` and `unaddressable`.
   - They also say which losses cannot be counted: frames TCP loses after they left this process; the first frame
     after a peer's idle close; and what a receiver discards after closing on an unknown tag, an undecodable,
     truncated or over-long frame, or the idle deadline, where only the idle close is counted.
   - They cannot all be made true by counting, because TCP gives no delivery receipt.

**Predictions are unchanged** from amendments 8 and 9. `749d399` touches doc comments only; no test reads them.

*Erratum to amendment 10, appended:* item 2 says "§3 names the options". It should say that the lane report
(`lane_d207_transport.md` §6, "D73 observations") names the options for O-1.

---

## Amendment 11 — the D223 review's caveats F2–F6, written BEFORE their fixes (nothing built)

Source: `artie-research frontier/d223_review.md` @ `67c547e`, verdict SOUND-WITH-CAVEATS.

### Red tests at **`3083397`** (log module; they compile against `1b8d290`; additions only)

| key | test | at `3083397` |
|---|---|---|
| **V1** | `a_frame_over_the_pre_d223_read_bound_makes_an_older_build_refuse_the_log` | **FAILS** at `!accepted_by_a_pre_d223_build(live)`: the header is still version 1 |
| **V2** | `a_log_of_ordinary_frames_stays_readable_by_an_older_build` (the control) | passes |
| **V3** | `a_checkpoint_keeps_the_mark_while_a_large_frame_survives` | **FAILS** at the same assertion, after the checkpoint |
| **V4** | `no_crash_can_leave_a_large_frame_under_a_header_an_older_build_accepts` (sweep) | **FAILS** before the sweep, on the unfaulted run: `assert_no_frame_an_older_build_would_trim` names file `a` or `b` with an 8,388,539-byte frame under a version-1 header |
| **U** | `the_largest_entry_an_unsigned_frame_carries_is_delivered_and_stored` (F6) | passes. Its red is the mutant `MAX_ENTRY_BYTES = MAX_FRAME_BYTES − 69`, **M35** below |

The tests read the header's version straight from the file bytes, not from the log. They define "a pre-D223
build accepts it" as magic, CRC and version 1. READ at `9aa6968`: `log.rs:311` refuses any other version with
`Corrupt`. The pre-D223 read bound is the literal 8,384,576 (READ `9aa6968:log.rs:105`, `:109`).

**Run R6 at `3083397`:** the log module has 54 tests: **51 passed, 3 failed** (V1, V3, V4).

### The fix for F2, as it will be made

**The version rule:**

- `VERSION` becomes **2**, and a new `LEGACY_VERSION = 1` is added.
- The header now carries its own version, and `decode` accepts 1 or 2.
- A new log starts at 1.
- **Before the first frame over `LEGACY_READ_BOUND` = 8,384,576 is written, the log switches to version 2.** It
  uses the same two-file switch a checkpoint uses: every current frame is copied to the spare and fsynced, then the
  version-2 header with `generation + 1` is written and fsynced, then the old file is retired. The large frame is
  appended only after that.
- A crash anywhere in the switch leaves either the old version-1 file (with no large frame) or the new version-2
  one. That is what V4 sweeps.
- A checkpoint keeps the version it finds; it is monotonic.
- The switch code is extracted from `discard_prefix`, unchanged, into one function both callers use, so there is
  one copy of the ordering.

**`MAX_FRAME` becomes a format constant**, the literal 8,388,672 (= `MAX_FRAME_BYTES` + 64), with
`const _: () = assert!(MAX_ENTRY_BYTES + 64 <= MAX_FRAME)`. Raising the write bound past it then fails the build,
which forces the next format version instead of moving the read bound silently. This is the review's suggestion.

**Why it refuses:**

- A pre-D223 build's `Header::decode` returns `Err(Corrupt("… format version 2 …"))` on a version-2 header (READ).
- `open` propagates that error, and a refused open is exactly what the module's header promises in place of a
  silent reinitialisation.
- A log that never held a large frame stays at version 1, so it can still be downgraded (V2).

**The rolling-upgrade stall**, stated rather than fixed (INFERRED from the review's arithmetic; latent, because
nothing in production proposes a `WalBatch`):

- A post-D223 leader admits a `WalBatch` payload of up to 8,388,502 bytes.
- A pre-D223 follower's disk refuses a disk frame over 8,384,512, which is a payload over 8,384,475.
- So a batch of 8,384,476–8,388,502 bytes stalls every old follower. Its `Persist` fails, and it never
  acknowledges.

**Decision 10's `REPL_VERSION` bump (Ryan's)** would turn that stall into a loud refusal.

- READ: `replication/mod.rs:73`, `REPL_VERSION = 2`.
- `read_handshake` refuses any version other than its own (`:227`), and the consensus transport uses that
  handshake in both `dial` and `recv_handshake`.
- With REPL_VERSION at 3, a pre-bump and a post-bump node refuse each other at the handshake. That is counted in
  `refused_handshakes` and names both versions.
- So an old follower is not stalled by a large batch; it is refused, loudly, and so is every other mixed pair. There
  is then no mixed-version cluster at all, which also rules out a rolling upgrade across that boundary.
- Without decision 10, the constraint is operational: **upgrade every node before any proposal carries a
  `WalBatch` payload over 8,384,475 bytes.**

This lane does not bump REPL_VERSION: that is decision 10's test edit, and the decision is Ryan's.

### The other caveats, as they will be fixed

- **F3.** `transport::TransportCounters`, a plain snapshot of every counter, from `Transport::counters()` and exposed
  as `Node::transport_counters()`.
  - It is read-only by construction: it hands out a copy, not `&Transport`, whose `send` and `shutdown` take `&self`.
  - Post-fix test **NC** (`tests_node.rs`): a node with configuration {1, 2, 3} and an EMPTY peer map is driven
    past its election timeout. Its pre-vote to 2 and 3 has no address, so `transport_counters().unaddressable ≥ 2`
    and `unencodable == 0`, read from the running node.
- **F4.** Narrow the comment at `node.rs:744-745`: a frame dropped from the queue is `dropped`, and the one frame whose
  write failed is `lost_in_flight`. Frames TCP loses after a successful write, frames cleared at shutdown, and a push
  that races a shutdown are not counted.
- **F5. `tel/log.rs` keeps its old limit.**
  - `MAX_APPEND_BYTES` becomes `MAX_FRAME_BYTES − 4096` directly, which is the value it had.
  - Its reason, "a delta larger than one replication frame is a delta that could never be shipped", needs a bound at
    or below what ONE entry inside one `Append` can carry.
  - `MAX_ENTRY_BYTES` is now the disk's bound, the whole frame, which is ABOVE that. Following it would admit TEL
    records that could never be replicated, the opposite of its stated purpose.
  - The TEL behaviour is unchanged, so no TEL test moves.
- **F6.** Test U (above).

### Predictions after the fix (Run G6)

| module | tests | predicted |
|---|---|---|
| log | 54 | all pass |
| node | 13 + NC = 14 | all pass (READ: 13 `#[test]` in `tests_node.rs` at `1b8d290`) |
| transport | 58 | unchanged |
| replicate | 60 | unchanged |

**Per-target: 2598 + 5 + 1 = 2604** on macOS.

### Mutants for these caveats

| mutant | what it does | fails |
|---|---|---|
| **M35 `disk_bound_below_unsigned`** | `MAX_ENTRY_BYTES = MAX_FRAME_BYTES − 69` (the review's survivor) | U |
| **M36 `no_version_raise`** | the raise before a large frame is skipped | V1, V3, V4 |
| **M37 `raise_after_the_frame`** | the frame is written first, and the version raised after it | V4 only, at the points between |
| **M38 `checkpoint_writes_legacy`** | a checkpoint always writes version 1 | V3 |
| **M39 `always_raise`** | every log is born at version 2 | V2 |
| **M40 `counters_not_wired`** | `Node::transport_counters` reports `unaddressable` 0 | NC |

---

## Amendment 12 — the D223 review's fixes, tests and mutants, before any run (nothing built)

| commit | what |
|---|---|
| `3083397` | red tests V1–V4 and U, as amendment 11 registered them |
| `dd6304a` | amendment 11 |
| `85df0da` | F2: format version 2, raised before the first frame over 8,384,576; `MAX_FRAME` a literal; the switch extracted from `discard_prefix` |
| `bce1f60` | F3: `TransportCounters` and `Node::transport_counters()`. F4: `node.rs`'s loss comment narrowed |
| `aaaadb4` | F3: post-fix test NC |
| `a8fdd80` | F5: `tel/log.rs` keeps `MAX_FRAME_BYTES − 4096`, no longer derived from the consensus disk bound |
| `31541c7` | F5, follow-on: two docs D223 made stale (`snapshot.rs`'s `MAX_SIDECAR_BYTES` derivation, `membership.rs`'s "appends every command"). Comments only |
| `36e0a81` | F7: `Node::propose`'s doc names admission refusals as well as `NotLeader`. Comment only |
| `38efedb` | mutants M35–M40; all 35 regenerated from `36e0a81` |

Beyond amendment 11's plan, the code also does these things:

- `RoundLog`'s `Debug` prints the version.
- `Header::decode`'s refusal now names both versions this build reads. The one test that reads that message
  (`tests_log.rs:750-756`) writes `VERSION + 1` and matches only "format version". With `VERSION = 2` that is 3, which
  is still refused.
- `TransportCounters`'s `Debug` includes `refused_after_stop`, which the transport's own `Debug` had left out.
- **The version is monotonic across truncation too.** A suffix truncation that removes the only large frame leaves
  the log at version 2. An older build then refuses a log it could have read. That is conservative, and no test
  depends on it.

**Why a downgrade refuses rather than falls back** (READ at `9aa6968`):

- `open` decodes BOTH headers with `headers[i] = Header::decode(&buf)?;` (`:413`). So a version-2 header in either
  file refuses the open. It does not demote to the other file.
- The switch retires the superseded file with `set_len(0)` (`:850`). So a completed raise leaves no version-1 file
  holding the large frame.

**No TEL test pins the append limit:** `git grep -nE "8_?384_?512|8_?388_?608|MAX_FRAME_BYTES" -- src/tel/ tests/`
returns only `tel/log.rs`'s own lines 494, 501 and 507. So F5 moves no test. TEL's modules are added to the collateral
runs because the constant's expression changed.

### Corrections to the record (append-only; the lines above stand as written)

- **Amendment 11's G6 node row is wrong.** It says "13 + NC = 14" and "READ: 13 `#[test]` in `tests_node.rs` at
  `1b8d290`".
  - The count is **12** at `1b8d290` and **13** at `36e0a81`. Instrument:
    `git show <sha>:src/consensus/tests_node.rs | grep -cE '^\s*#\[test\]'`. The file has no `#[cfg` and no
    `#[ignore]`.
  - Per-target 2604 is unaffected, because it was computed from the added tests.
    `git diff 1b8d290 36e0a81 | grep -cE '^\+\s*#\[test\]'` returns 6, and the same count with `-` returns 0. Then
    2598 + 6 = 2604.
  - The 2598 is Run G5's prediction at `010d3c4`, never measured. `010d3c4..1b8d290` adds and removes no
    `#[test]` (same instrument).
- **F8: Run R5 (amendment 8) and Run R6 (amendment 11) read as measurements. Both are predictions**; nothing on this
  branch has been compiled or run. Read them as:
  - "Predicted: T3 panics with `TooLarge { bytes: 8388539, limit: 8384512 }`";
  - "Predicted: the log module, 51 passed and 3 failed".

### Run G6 — GREEN at the tip (predicted)

| module | tests | predicted |
|---|---|---|
| log | 54 | all pass |
| node, `consensus::node::tests_node::` | 13 (12 + NC) | all pass |
| transport | 58 (57 off macOS) | all pass, unchanged |
| replicate | 60 | all pass, unchanged |

**Per-target: 2604** on macOS (predicted).

### Commands per mutant

| mutants | command | predicted to fail |
|---|---|---|
| M35 | the log module, `timeout 1800 cargo test --no-fail-fast --lib consensus::log::tests_log::` | U only. The signed-maximum test stores a disk frame of 8,388,539, and M35's bound is exactly 8,388,539, so that test survives it |
| M36 | the log module | V1, V3, V4 |
| M37 | the log module | V4 only, at crash points between the large frame's write and the switch's retirement |
| M38 | the log module | V3 |
| M39 | the log module | V2 |
| M40 | the node module, `timeout 1800 cargo test --no-fail-fast --lib consensus::node::tests_node::` | NC |

The M35 arithmetic (INFERRED, `log.rs:1130`): a disk frame is `4 + 8 + 8 + payload + 4`, and U's payload is
`WalBatch`'s 13-byte header plus 8,388,563 − 29 bytes. Together that is 8,388,571 > 8,388,539, so M35 refuses U.

**How M37 fails** (INFERRED): its raise copies the large frame into the spare, which is fine once the switch
completes. But under `WriteThrough` the frame is durable under the version-1 header from `write_batch` until the
switch retires that file, and V4's sweep crashes inside that window.

All 35 patches pass `git apply --check` against `38efedb`'s tree. M1–M34 keep their edits, and only hunk context
moved: each patch's `^[-+]` lines were compared with its copy at `36e0a81`, and eight patches were regenerated with
identical edits.

---

## Amendment 13 — review 4's caveats C1–C4, written BEFORE the tests (nothing built)

Source: `artie-research frontier/d207_review4.md` @ `6867645`, review of `1b8d290..d815c1b`, verdict
SOUND-WITH-CAVEATS. F2's mechanism holds and nothing blocks. This amendment closes C1's test gap, records C2–C4, and
corrects one citation. **No `src/` code changes**: tests are added and comments corrected.

### C1: four tests, post-fix, each with mutant-only red

Every large append in V1, V3, V4 and U is a one-entry batch on a log at floor 0 (the review, READ). So three unsafe
mutants survive every registered test, and V4 never reopens with this build. The four tests below are additions
only, to `tests_log.rs`. They pass on the tip, and their red evidence is the mutants that follow.

| key | test | what it pins |
|---|---|---|
| **W1** | `a_mixed_batch_raises_the_version_whichever_entry_is_large` | a `[small, large]` batch and a `[large, small]` batch, each on a version-1 log, leave a live header an older build refuses. Each is on its own fabric, and each panic names its order |
| **W2** | `the_version_is_raised_exactly_above_the_pre_d223_read_bound` | a disk frame of exactly the test's own literal 8,384,576 bytes leaves the log at version 1, and one of 8,384,577 raises it. Each fixture checks its stored frame length from the image bytes |
| **W3** | `a_version_raise_on_a_checkpointed_log_keeps_the_floor_and_every_round` | a log at floor 2@1 holding 3@1 and 4@2 takes a large round 5. After a reopen: floor 2, floor term 1, every round 3–5 intact, and a header an older build refuses |
| **W4** | `no_crash_during_a_version_raise_loses_a_round_this_build_reads` | V4's sweep (both durabilities, three shapes, every faultable operation of the first large append after rounds 1–3), with a reopen by THIS build at every point. The log must open with floor 0, `last_round` 3 or 4, and rounds 1–3 unchanged. Round 4, if present, must be the large entry |

**The fixtures for W1 and W2 are sized in disk bytes.** A `WalBatch` disk frame is `24 + 13 + bytes` (READ
`encode_frame`, and `encode_command`'s `WalBatch` arm: tag 1, `start_lsn` 8, length 4). W1's large entry is a disk
frame of `PRE_D223_READ_BOUND + 1`, a size one real `Append` can carry beside a small entry, as the review asked.

### Mutants for C1 (cut from the tests' commit; log module)

| mutant | what it does | fails |
|---|---|---|
| **M41 `raise_only_when_all_large`** | the batch rule becomes `all` | W1 (its first half, `[small, large]`) |
| **M42 `raise_on_first_only`** | `frames.first()` | W1's `[small, large]` half |
| **M43 `raise_on_last_only`** | `frames.last()` | W1's `[large, small]` half |
| **M44 `legacy_bound_raised`** | `LEGACY_READ_BOUND = 8_388_538`, the top of the unsafe range the review names | W2's 8,384,577 half |
| **M45 `raise_floor_term_is_last_term`** | the raise passes `self.last_term()` as the floor term | W3. The reopen's scan starts at `prev_term` 2 and trims round 3@1 onward, so `last_round` reads 2 |
| **M46 `raise_rewrites_the_header_in_place`** | the design this lane rejected: the raise rewrites the live file's header in place, without the switch | W4 only. A torn or corrupt in-place header on a log that holds frames leaves no readable header, and `open` refuses rather than reinitialise. V4 passes it, because a header that cannot be read is also one an older build refuses |

M41–M43 together make both halves of W1 necessary. M44 would also be killed by any value in
(8,384,576, 8,388,538]. A LOWER bound (the safe direction) fails W2's 8,384,576 half. So W2 pins the constant
exactly. All of these kill sets are INFERRED.

### Records

- **C2: the rolling-upgrade constraint is per disk frame, for every command kind.**
  - An older follower refuses any disk frame over **8,384,512 bytes** (`9aa6968:log.rs:1022`,
    `total > MAX_ENTRY_BYTES`). An older build trims any frame over 8,384,576. Both are measured by `encode_frame`,
    whatever the command.
  - Amendment 11's "`WalBatch` payload over 8,384,475" is the `WalBatch` case of that: a disk frame is the payload
    plus 37.
  - The constraint, stated generally: **until every node runs this build, no committed entry may exceed 8,384,512
    disk bytes**, or decision 10 must land.
  - It is latent. `src/` proposes only `Command::Branch`, and `WalBatch` is built only in an example, the
    simulator and tests (the review, READ). But `Node::propose` is `pub`, so an embedder could still propose one.
- **C3: a downgrade stall.**
  - Disk frames of **8,384,513–8,384,576 bytes** are above every older build's write bound and at or below its read
    bound. They stay at version 1, correctly, because an older build reads them.
  - But after a downgrade, an older follower that lacks such a round refuses to store it. It never acknowledges, and
    it stalls until a snapshot covers that round.
  - Nothing is lost, and no version mark can fix it: the follower that stalls is the one that does not hold the
    frame.
  - Downgrading across D223 is therefore safe only when no committed entry over 8,384,512 disk bytes is still needed
    by a follower. Decision 10 would also cover it.
- **C4: a log can reach version 2 without holding a large frame.** The raise comes before the write, so the log stays
  at version 2 in three cases: a large append that then fails (`write_batch` returns `Io`), a crash before the frame
  is durable, or a later truncation. Amendment 12 named only the truncation. The exact rule is: **a log to which no
  large frame was ever submitted stays version 1.** That is conservative, not unsafe.
- These three are recorded in `log.rs`'s docs as well, in a comment-only commit.

### Correction to amendment 11 (append-only; A11 stands as written)

Amendment 11 says "READ at `9aa6968`: `log.rs:311` refuses any other version with `Corrupt`". **At `9aa6968` that
line is `:297`** (`if version != VERSION {`, inside `Header::decode` at `:288`). `:311` is its position at
`1b8d290`/`3083397`. Instrument: `git show 9aa6968:src/consensus/log.rs | sed -n 288,300p`.

### Run G7 at the tip (predicted)

| module | tests | predicted |
|---|---|---|
| log | 54 + W1–W4 = **58** | all pass |
| node | 13 | unchanged |
| transport | 58 (57 off macOS) | unchanged |
| replicate | 60 | unchanged |

**Per-target: 2604 + 4 = 2608** on macOS. Command for M41–M46: the log module,
`timeout 1800 cargo test --no-fail-fast --lib consensus::log::tests_log::`.
