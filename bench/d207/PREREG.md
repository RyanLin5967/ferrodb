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
