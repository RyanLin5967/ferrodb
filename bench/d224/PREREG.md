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

---

## Amendment 2 — the D224 review's caveats, written BEFORE their fixes (nothing built)

Source: `artie-research frontier/d224_review.md` @ `d23d425`, verdict SOUND-WITH-CAVEATS. The mechanism stands. What
follows corrects the record and the tests around it, and closes one hole (the review's F4, which the lead verified).

### Red test at **`92373ff`** (additions only; it compiles against `7d9567f`)

**H** = `a_link_left_holding_a_refusal_is_redialled_after_an_idle_gap`.

- **The hole.** A refused handshake does not close silently. `conn_loop` still sends its own handshake, then an
  `Error` frame, then closes (READ `7d9567f:transport.rs:2265-2278`). The dialler reads exactly the six handshake
  bytes and keeps the connection. The `Error` frame and the FIN stay unread on it, so every `peek` returns
  `Ok(1)`. `peer_has_closed` reads `Ok(_)` as "alive" (`:2068-2069`), which makes the probe blind on that link for
  good.
- **The test.** A hand-rolled peer writes exactly those bytes and closes. It reads all six of A's handshake bytes
  first, so the close is a FIN and not a reset, and A writes nothing on that connection. After 600 ms (the gate is
  150 ms), A sends ONE frame. The frame must arrive on a second connection, through one redial, with
  `lost_in_flight` 0.
- **Counts:** the transport module has 62 `#[test]` at `92373ff`, one of them macOS-only. Instrument:
  `grep -cE '^\s*#\[test\]'`, with one `#[cfg(target_os = "macos")]` at `:2054`.

**Run R2 at `92373ff` (predicted):** **61 passed, 1 failed** on macOS (60/1 elsewhere). H fails at its "A never
redialled a link holding an unread refusal and a FIN" panic, after the 5 s accept deadline, reporting
`idle_probes +1, idle_redials +0, lost_in_flight 0`. The probe ran and found the link alive; the frame went into the
closed connection, which answered with a reset, and no later write came to fail.

### The fixes, as they will be made

1. **`peer_has_closed`: `Ok(_) => true`.** A sender only writes to its link. The one path on which the accepting side
   writes after the handshake is the refusal, and it always closes. So an unread byte means either a refusal or a
   state the protocol does not produce while the link is open. Either way the frame is carried to a redial, which
   costs nothing. The doc's "after the handshake the accepting side never writes" is corrected.
2. **`idle_redials` no longer counts a shutdown.** `Transport::shutdown` and `stop_started` both set `stop` before
   they shut `st.live` (READ `:1823-1829`, `:1849-1855`). So a close caused by the shutdown is always seen with
   `stop` set, and a `stop` check before the count skips it. The carried frame is then dropped uncounted, as the
   queue is (`:1826`).
   - **No test and no mutant.** The window between taking a frame and the peek is microseconds wide, so a test
     cannot aim at it. The fix is stated as untested.
3. **The premise is restated, in `idle_probe_gap`'s doc, `idle_deadline`'s doc and here.**
   - The design needs only `D_r ≥ D_s / 2`: no node's `idle_deadline` may be below half of another's. Equal
     deadlines are not needed.
   - If the premise is violated, gaps in `(D_r, D_s / 2)` are closed but never probed. The pre-D224 loss returns
     for that band only; there is no new failure mode.
   - It is not enforced. The handshake carries no options (`:2096`), and `NodeOptions.transport` is a per-node pub
     field (`node.rs:126`).
   - In-repo, every node uses the default: `git grep -n idle_deadline -- src tests examples` finds no setter
     outside `transport.rs` and `tests_transport.rs` (READ).
4. **What "half" buys: tolerance, not a narrower race.** The race sits at the receiver's close wherever the gate is,
   as long as the gate is at most D. Half tolerates a receiver whose deadline is as low as half the sender's. The
   original's "why half" bullet ("the probe then lands on links still open…") is withdrawn as a reason.
5. **The residual costs two frames, not one** (the review's F7). If the probe sees the link open just before the
   close, the write succeeds and refreshes `last_used`. The frame after it, due under the gate, is not probed, so it
   fails against the reset and is counted. That is the full pre-D224 cost, at a small probability. The docs that say
   "one frame" are corrected.
6. **The restart claim is RETRACTED.** The original's mechanism bullet says "It also catches a peer that restarted
   during a gap". That is too broad.
   - **Caught:** a restart is caught only when the next write comes at least G after the previous one, which in
     practice means idle follower↔follower links.
   - **Not caught:**
     - a busy leader↔follower link, which costs two heartbeats;
     - a peer that restarts mid-election, because campaign frames go out 0.5–1 s apart;
     - a host that reboots, which sends no FIN or reset until we write;
     - a peer shut down mid-handshake, which H now covers through fix 1.
7. **Busy links pay two clock reads per frame, not nothing**, in the doc at the gate. The carried frame's policy
   also gets a sentence. It sits outside the queue's drop-oldest bound and goes first on the new connection, the
   stalest frame first; consensus refuses stale terms, so that is harmless.

### Test changes, registered here before they are made

- **I and P: add a 200 ms settle** after the wait loop, before the one send (the review's F2).
  - The wait proves only that B *sent* its FIN (`live_inbound_conns` falls after `conn_loop`'s `shutdown`). On XNU,
    loopback delivery can lag under load, so the peek could run before A's kernel processed the FIN.
  - The red at `9b0f4af` is unaffected: nothing probes there, so the frame is lost whatever the delay. Run R measures
    `9b0f4af`'s own I.
  - The gap only grows further past the 150 ms gate.
- **B: `idle_deadline` 8 s (gate 4 s), 60 frames 100 ms apart, and `elapsed ≥ 5 s`** (gate + 1 s), measured from an
  `Instant` taken before `pair()`.
  - B records `t_send[k]` before each send and `t_recv[k]` after each receive, and prints
    `bound = max(t_recv[1] − t0, max_k (t_recv[k+1] − t_send[k]))` in its `idle_probes == 0` message. That bound is
    an upper bound on every gap between A's writes.
  - `bound < gate` means the probe is a defect. `bound ≥ gate` means the run stalled, so re-run it. **Both stay red.**
- **`tests_transport.rs:1680-1682`** still says "Consensus heartbeats every few ticks, so silence past the deadline
  is a gone peer". That is the premise D224 corrected in `conn_loop`, so it is replaced. Comment only.

### Mutants, corrected and extended (the review's F1)

| mutant | fails | change from amendment 1 |
|---|---|---|
| N1 `probe_removed` | I, P, H | + H |
| N2 `probe_every_frame` | B, P, `a_broken_connection_is_reconnected_and_the_frame_lost_to_it_is_counted` | + P (the carried frame is probed again on the new link, so `idle_probes` rises by 2) and + the broken-connection test (every frame is probed, the probe sees the FIN, and the redial blocks in `dial` against a listener not yet accepting, so `lost_in_flight` stays 0) |
| N3 `probe_result_ignored` | I, P, H | + H |
| N4 `frame_not_carried` | I, P, H | + P (at `expect_recv` for term 2) and + H (the redial happens, but the frame never arrives) |
| N5 `last_use_not_refreshed` | B | unchanged. H's gap is counted from the dial, so it does not see N5 |
| **N6 `refusal_bytes_read_as_alive`** (new) | H | `Ok(_) => true` reverted to `false`, which is the code at `7d9567f` |

The N2 row's reasoning is the review's (INFERRED there, and here). A failure outside a row is reported as an
unregistered kill, never folded in.

### Run G2 at the tip (predicted)

- **The transport module: 62 passed on macOS** (61 elsewhere), which is 61 plus H.
- **Per-target: 2601 + 1 = 2602** on macOS. The 2598 under it is the D207 lane's prediction and has never been run.

### Errata (append-only; the lines above stand as written)

- The original says "at 10–20 ticks (0.5–1 s, `mod.rs:453-455`)". The election timeout is `10 + rng % 10`, which is
  **10–19 ticks** (0.5–0.95 s). Amendment 1 corrected the line numbers, not the range.
- The lane report and the FAN-QUEUE row say "All 6 commits". `1b8d290..7d9567f` holds **5**
  (`git rev-list --count 1b8d290..7d9567f`).
