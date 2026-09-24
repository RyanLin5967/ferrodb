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
