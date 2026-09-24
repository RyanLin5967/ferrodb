# D224 — pre-registration, written BEFORE the fix (nothing built)

Branch `d224-idle-failover`, cut from `1b8d290` (the D207 lane's tip) in its own worktree, so that the D207 range
under review stays fixed. Quiet mode: nothing on this branch has been compiled or run, so every line below is a
prediction. Amendments append only.

**Instrument:** `timeout 1800 cargo test --no-fail-fast --lib consensus::transport::tests::`, at default QoS, under
the shared suite lock.

## The defect (the lead's ruling, from the D207 lane's O-1)

- The receiver closes a connection that has been silent longer than `idle_deadline`, which defaults to 60 s.
- Followers send each other nothing while a leader holds.
  - Acks and refusals go to the Append's sender (`replicate.rs:828`, `:850`).
  - Campaign frames go out only when an election starts (`election.rs:350-357`, `:401`).
- The sender never reads its socket after the handshake, so it does not see the close. Its next frame is lost
  uncounted; the one after fails, is counted in `lost_in_flight`, and makes it redial.
- **After 60 s of stable leadership, a survivor's first campaign frame to the other survivor is lost.**

The comment beside the idle close says "Consensus heartbeats every few ticks, so silence past this deadline is not a
slow peer — it is a gone one". That is false for follower-to-follower links.

## Red test at `9b0f4af` (compiles against `1b8d290`; additions only)

**I** = `a_frame_sent_after_the_peer_idle_closed_the_link_arrives_on_the_first_attempt`.

- `idle_deadline` is 300 ms.
- A→B carries one frame, then stays silent until B closes it. That is observed as `b.idle_closed() >= 1` and
  `b.live_inbound_conns() == 0`; the second reading only falls after `conn_loop`'s final `shutdown`.
- Then **one** send.

**Counts:** the transport module has 58 `#[test]` at `1b8d290` and 59 at `9b0f4af`. One is macOS-only, so
**59 on macOS**, 58 elsewhere.

**Run R — RED at `9b0f4af`:** **58 passed, 1 failed.** I fails at `got == Some(2)` with **left `None`**, and its
message reports `lost_in_flight=0`. The frame went into the closed connection: the write succeeded locally, the peer
answered with a reset, and nothing was counted.

## The fix (the lead's decision) and the gap threshold (mine)

**The mechanism.** Before the first write after an idle gap, the sender makes a non-blocking `peek` for the peer's
EOF, and redials first if the link is closed.

- The frame is carried to the new connection, not lost.
- Busy links never reach the gate. It needs no protocol change. It also catches a peer that restarted during a gap.

**The threshold: `G = idle_deadline / 2`**, derived rather than a new knob. It is bounded on both sides.

- **Lower bound: above the longest gap on any link consensus keeps busy.**
  - The leader heartbeats every `heartbeat` = 3 ticks (`mod.rs:473`) of `tick` = 50 ms (`node.rs:157`): 150 ms.
  - Every heartbeat is an Append that each follower answers at once, so the leader→follower and follower→leader
    links have gaps of about 150 ms.
  - At the default, G = 30 s = 200 heartbeats. A link would have to miss 200 heartbeats in a row to be probed, and
    at 10–20 ticks (0.5–1 s, `mod.rs:453-455`) an election has long since started.
- **Upper bound: at most `idle_deadline`, so every link the receiver may have closed is probed.**
  - The receiver closes only after it has been silent for longer than `idle_deadline`, measured from its last read.
    That read is no earlier than the sender's last write.
  - So a gap under G < `idle_deadline` means the receiver cannot have closed.
- **Why half, and not the deadline itself.**
  - The probe then lands on links still open while their gap is in `[G, idle_deadline)`, and the write resets the
    receiver's clock.
  - The only links it can still misjudge are those whose gap lands within about one `poll_interval` plus a round
    trip of the receiver's close. For those, the close and the probe race (see "Residual").
  - A follower-to-follower link idle for a whole term is far past that window.

**Premise, stated:** both ends run the same `idle_deadline`. Every node takes it from `TransportOptions::default()`
through `NodeOptions`. A receiver with a *shorter* deadline than the sender's `2G` could close a link before the
sender's gap reaches G.

**Residual (INFERRED):**

- A gap within about one `poll_interval` plus a round trip of the receiver's close can still lose one frame
  uncounted: the probe sees an open link just before the receiver closes it.
- A peer that restarts during a gap shorter than G is not caught.

**Counters:**

- `idle_probes`: probes taken.
- `idle_redials`: probes that found the link closed and redialled.

They make "busy links pay nothing" a count rather than a claim.

**Also:** the idle-close comment's premise is corrected, and so are D223's docs that describe the idle close's cost
(the module header and `idle_deadline`).

## Run G — GREEN at the fix plus its post-fix tests

**The transport module: 61 passed** on macOS (60 elsewhere). That is 59 + **B** + **P**:

| key | test | predicted |
|---|---|---|
| I | as registered above | the frame arrives; `lost_in_flight` is 0 |
| **B** | `a_busy_link_is_never_probed`, the control. `idle_deadline` 2 s, so G = 1 s. 150 frames 20 ms apart, about 3 s in all, so the link lives well past G. | **`idle_probes` == 0**, and all 150 arrive in order |
| **P** | `an_idle_closed_link_is_probed_once_and_redialled`, I's scenario with the counters read | **`idle_probes` == 1, `idle_redials` == 1**, `lost_in_flight` == 0 |

B's failure direction under load: B fails only if a 1 s stall lands between two sends, so it can give a false red,
never a false green. On a loaded box, a B failure is read as "re-run quieter".

## Mutants (cut from the fix; transport module)

| mutant | what it does | fails |
|---|---|---|
| **N1 `probe_removed`** | the probe is deleted | I, P |
| N2 `probe_every_frame` | the gate is always open: the rejected unconditional per-frame peek | B |
| N3 `probe_result_ignored` | the probe is taken but never redials | I, P (`idle_redials` 0) |
| N4 `frame_not_carried` | the frame is dropped when the probe finds the link closed | I |
| N5 `last_use_not_refreshed` | a successful write does not reset the gap, so every frame after G since the dial is probed | B |

## Per-target

**2598 + 3 = 2601** on macOS: the D207 lane's 2598 (INFERRED; not yet run) plus I, B and P.

---

## Amendment 1 — the fix, tests and mutants recorded, before any run (nothing built)

| commit | what |
|---|---|
| `3cbbc80` | the fix |
| `12fb897` | post-fix tests **B** and **P** |

The transport module now has 61 `#[test]`, as Run G predicted. The mutants N1–N5 are generated from `12fb897` by
`bench/d224/make_mutants.py 12fb897`, and every patch passes `git apply --check`.

- **N1 and N3** are written as `if false && …`, so the helpers stay referenced and the mutant builds without new
  warnings.
- **N2** is `if true || …`, which opens the gate for every frame.
- **N4** drops the frame instead of carrying it.
- **N5** removes the refresh after a successful write.

**P, refined:** it reads `idle_probes` and `idle_redials` just before the one send and asserts that each rose by
**exactly 1**, so on a loaded box a probe of the first frame cannot be mistaken for this one. `lost_in_flight` must
be 0.

**B, refined:** it is 25 frames 100 ms apart, closer to a heartbeat's 150 ms pace, with a lower-bound check that the
link lived at least 2 s, which is twice the 1 s gate.

**Collateral** (every test must pass):

- `timeout 1800 cargo test --no-fail-fast --lib consensus::`, which covers `tests_signing` (real transports),
  `tests_node` and the rest;
- `timeout 1800 cargo build --examples`, then
  `timeout 1800 cargo test --no-fail-fast --test integration_cluster_agents --test integration_cluster_snapshot --test integration_consensus_failover`.

**Errata to the original registration** (append-only):

- `heartbeat: 3` is at `consensus/mod.rs:472`, not `:473`;
- `election_base` and the jittered timeout are at `:452-454`, not `:453-455` (READ at `12fb897`).
