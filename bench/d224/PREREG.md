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

---

## Amendment 3 — the review's fixes, test changes and mutants recorded, before any run (nothing built)

| commit | what |
|---|---|
| `92373ff` | red test H (amendment 2) |
| `bb390f2` | amendment 2 |
| `b05aaa3` | the fix: `Ok(_) => true`, with its doc corrected; the `stop` check before `idle_redials` counts; the premise, why half, the two-frame residual and the restart scope restated in `idle_probe_gap`, `idle_deadline` and the module header; busy links' two clock reads; the carried frame's policy |
| `92e28fa` | the registered test changes, exactly as amendment 2 lists them: I and P settle, B's new margin and bound, and the stale comment at `tests_transport.rs:1680-1682` |
| `a9d568c` | N6 added; N1–N6 regenerated from `92e28fa` |

- **Code changed by `b05aaa3`: three lines**, all else is comments. They are the `Ok(_)` arm and a three-line
  `if stop.load(..) { break; }` before the redial count. Instrument: `git show b05aaa3` with comment lines filtered
  out.
- **Assertions changed by `92e28fa`: B's only.** Its elapsed bound goes from 2 s to `gate + 1 s` = 5 s, and its
  `idle_probes == 0` message now prints the bound, as registered. I and P each gain a sleep and a comment. No
  assertion of theirs moves.
- **Counts at `a9d568c`:** 62 `#[test]` in `tests_transport.rs`, one macOS-only. Run G2 stands: **62 passed on
  macOS**, per-target **2602**.
- **Mutants:** all six patches pass `git apply --check` against `a9d568c`'s tree. N1–N5 keep their edits: each
  patch's `^[-+]` lines were compared with its copy at `7d9567f`, and only hunk context moved. Kill sets are
  amendment 2's table.
- **Command, every mutant:** `timeout 1800 cargo test --no-fail-fast --lib consensus::transport::tests::`.

**Erratum to amendment 3:** `b05aaa3` changes **four** code lines, not three: one `Ok(_)` arm and the three added lines
of the `stop` check.

---

## Amendment 4 — the merge of D207's final tip, and the probe's meters on the node, written BEFORE the code (nothing built)

The lead's instruction: make the merge obligation a commit, not a note. So D224 lands as a branch that CONTAINS
D207 (#19), and nothing needs doing by hand between the two landings.

**`4462bd2` merges `1eaf1a7`**, which the lead declared D207's FINAL tip after a lead check. It is a `--no-ff`
merge, with no rebase; `49ba420` and `1eaf1a7` are both its parents. It merged without conflict. What it leaves
incoherent, read from the merged tree:

- D207's `TransportCounters` is documented as "every meter a transport keeps", and `Transport::counters()` fills
  16 fields.
- The merged `Transport` has **18** counter accessors (`pub fn …(&self) -> u64|usize`). The two missing are D224's
  `idle_probes` and `idle_redials`.
- So on the merged tree, `Node::transport_counters()` hides the probe. `Transport`'s `Debug` merged coherently: it
  prints both sides' fields.

**Counts on the merged tree at `4462bd2`** (READ, `grep -cE '^\s*#\[test\]'`): transport 62 (one macOS-only),
node 13, log 58, replicate 60.

### The fix, as it will be made

- Add `idle_probes: u64` and `idle_redials: u64` to `TransportCounters`, and fill both in `Transport::counters()`
  from the accessors. The struct's doc then holds again: 18 fields, 18 accessors.

### Post-fix test, with mutant-only red

It cannot be red against the merge: it reads fields that do not exist there, and a test that fails to compile
takes its whole binary down with it.

**K** = `the_idle_probe_meters_are_readable_from_a_running_node` (`tests_node.rs`).

- **Setup.**
  - A node is configured {1, 2}, with `idle_deadline` 300 ms (so the gate is 150 ms) and a 1 ms tick.
  - Peer 2 is a hand-rolled listener. It accepts the node's own dial, reads its six handshake bytes, answers with a
    handshake, and closes. The node has sent nothing else, so the close is a FIN.
  - The test waits 400 ms, sets `next_tick` in the past, and polls once. The node campaigns, and its first frame to 2
    is probed after a gap of at least 400 ms since the dial. The probe finds the FIN and redials.
- **The premise is read from the transport itself**, through `n.net.idle_probes()` and `n.net.idle_redials()`
  (`tests_node` is a child module of `node`), not through the snapshot under test. It must reach ≥ 1 each within
  10 s, or the test fails as vacuous.
- **Then** `n.transport_counters()` must show `idle_probes ≥ 1` and `idle_redials ≥ 1`. Both counters only rise and
  the premise was read first, so there is no race.

### Mutants (transport.rs; the node module)

| mutant | what it does | fails |
|---|---|---|
| **N7 `counters_drop_idle_probes`** | `counters()` reports `idle_probes: 0` | K |
| **N8 `counters_drop_idle_redials`** | `counters()` reports `idle_redials: 0` | K |

- These are "dropped" fields in the only form that compiles. A struct literal without the field is a compile
  error, which would fail everything and discriminate nothing.
- Command: `timeout 1800 cargo test --no-fail-fast --lib consensus::node::tests_node::`.
- N1–N6 are regenerated from the new source tip, and their kill sets stand (amendment 2).

### Run G3 at the tip (predicted)

| module | tests | predicted |
|---|---|---|
| transport | 62 (61 off macOS) | all pass, unchanged by this amendment |
| node | 13 + K = **14** | all pass |
| log | 58 | all pass (D207's) |
| replicate | 60 | all pass |

**Per-target: 2613** on macOS = D207's 2608 at `1eaf1a7` (a prediction, never run) + I, B, P, H + K.

---

## Amendment 5 — the merge's obligation met, recorded before any run (nothing built)

| commit | what |
|---|---|
| `4462bd2` | merges D207's final `1eaf1a7` (`--no-ff`, no conflict) |
| `420cda4` | amendment 4 |
| `5445fa7` | `idle_probes` and `idle_redials` added to `TransportCounters` and `Transport::counters()` (`+4 −0`) |
| `4a5c95c` | test K, additions only (`60 0`) |
| `18b08a8` | N7 and N8 added; N1–N8 regenerated from `4a5c95c` |

- **After `5445fa7`, `TransportCounters` has 18 fields and `Transport` has 18 counter accessors.** READ:
  `grep -n 'pub fn [a-z_]*(&self) -> \(u64\|usize\)'` against the struct. Its "every meter" doc holds again. No other
  struct literal of it exists in `src/`, `tests/` or `examples/` (READ, `git grep 'TransportCounters {'`). D207's
  M40 builds one with `..self.net.counters()`, so it still compiles.
- **Counts at `18b08a8`:** transport 62, **node 14**, log 58, replicate 60. Run G3 stands, and per-target is
  **2613**.
- **Mutants:** all eight patches pass `git apply --check` against `18b08a8`'s tree. N1–N6 keep their edits: each
  patch's `^[-+]` lines were compared with its copy at `859f1b6`, and only hunk context moved.
- **Commands:**
  - N1–N6: the transport module, kill sets from amendment 2;
  - N7 and N8: the node module, `timeout 1800 cargo test --no-fail-fast --lib consensus::node::tests_node::`,
    each killed by K.
- **D207's mutant patches under `bench/d207/`** were cut against D207's own tree. Their transport context predates
  D224, so they are D207's run script's to apply, on `d207-transport-port`. They are not for this merged tree.

---

## Amendment 6 — review 2's caveats G1–G4 and nits, written BEFORE their code (nothing built)

Source: `artie-research frontier/d224_review2.md` @ `2883f78`, a review of `7d9567f..49ba420`, verdict
SOUND-WITH-CAVEATS. The fix is correct: a healthy link never has readable bytes at the probe, and the `stop` check is
right. What follows corrects claims, adds one test, registers two mutants, and strengthens test K.

**Numbering.** N7 and N8 are already the counter mutants (amendment 4). So the gate mutant the lead called "N7" is
registered here as **N9**, and the reset-arm survivor as **N10**.

### G1: how to read Run R2 (a note before any run)

R2 runs the whole transport module at `92373ff`. At that commit I and P have no 200 ms settle, and B still has the
old margin (`idle_deadline` 2 s, 25 frames). Those changes came in `92e28fa`.

- **In R2 only:** a failure of I or P that shows a probe without a redial, or any failure of B, is the known exposure
  of a frozen commit (the first review's F2 and F3). Report it as that. It is not a D224 regression and not a
  registration miss, and it is not folded into R2's count.
- R2's evidence is H's own signature: "A never redialled", `idle_probes +1, idle_redials +0, lost_in_flight 0`.
  That stands either way.
- **H's FIN premise** (review 2, Q3): H passing in R2, or passing under N6, means the close was a reset rather than
  a FIN, so the premise failed. It is not a flake to re-run. Only readable bytes produce the red signature.

### G2: the carried frame CAN be lost; claims retracted, residual stated

- **The mechanism** (READ at `bf1fd63`):
  - A dial resets `last_used` (`transport.rs:2002`).
  - So the carried frame, and any frame queued behind it, goes out unprobed.
  - If that dial lands on a peer that is shutting down mid-handshake, `dial` returns `Ok` with `[Error][FIN]`
    waiting. That happens when the peer's `stop` is set while its `conn_loop` is inside `recv_handshake`.
  - The carried frame is then written into that link and lost **uncounted**. The next frame fails and is counted.
    That is the whole pre-D224 cost.
- **Retracted** (append-only; the lines stand as written):
  - the original "The frame is carried to the new connection, not lost" (the mechanism bullet);
  - amendment 2 item 1's "which costs nothing";
  - amendment 2 item 6's last bullet, "a peer shut down mid-handshake, which H now covers through fix 1". That
    holds only when the refused link then sits idle for at least G. H passes because of its 600 ms of silence.
- **The code and lane text that say the same is corrected:**
  - the `carried` comment ("costs the frame nothing");
  - `peer_has_closed`'s `Ok(_)` comment ("costs it nothing");
  - `idle_probe_gap`'s doc, which gains the residual;
  - the module header;
  - lane §2.
- **What bounds it: consensus retransmission.**
  - A pre-candidate re-campaigns every election timeout until it hears a leader. `voter_tick` starts a campaign
    whenever `since_heard` reaches `election_timeout` (`election.rs:52-60`), and `start_precampaign` resets
    both (`:324-334`).
  - A leader re-sends from each peer's `next` on every heartbeat: `leader_tick` (`election.rs:118-122`), then
    `broadcast_heartbeat`, then `bcast_append` (`replicate.rs:759-761`).
  - So a lost frame costs one retransmission interval, never a round. READ.
- **Not taken:** a non-blocking peek at the end of `dial`. It would catch only an `Error` frame that arrived with the
  handshake, not one still in flight. Closing the residual fully needs the acceptor to send its verdict, which is the
  protocol change the lead rejected. The lead's scope here is the claims.
- **D224's own scenario is unaffected:** two surviving followers redial each other, and neither is shutting down.

### G3: the premise's boundary, pinned with UNEQUAL deadlines

**T** = `a_link_closed_by_a_peer_with_half_the_idle_deadline_is_still_probed` (`tests_transport.rs`, additions only).

- **Setup.** Sender A has `idle_deadline` 2 s, so its gate is 1 s. Receiver B has 1 s. That is `D_r = D_s / 2`, the
  premise's boundary. The two transports are built directly, because `pair()` gives both ends the same options.
- **The run.**
  - A→B carries one frame, then stays silent until B closes it (`b.idle_closed() ≥ 1`,
    `b.live_inbound_conns() == 0`).
  - It settles for 300 ms, then sends ONCE.
  - The frame must arrive, with `idle_probes` +1, `idle_redials` +1, and `lost_in_flight` 0.
- **Margins** (INFERRED).
  - B closes only after more than 1 s of silence, and A's last write came before B's last read. So the gap is at
    least 1 s + 300 ms: it clears the fix's 1 s gate by 300 ms, and load can only widen it.
  - **N9 `gate_is_the_whole_deadline`** makes the gate `idle_deadline` = 2 s. The gap is then about 1.3 s, under the
    gate, so the frame goes into the closed link. T fails, with no second send to rescue it.
  - N9 is killed only while the gap stays under 2 s. A stall of about 0.7 s lets N9 survive: a false survivor,
    never a false red on the fix. N9 survives I, P, H, K and B, since their gaps all exceed D (review 2).

### G4: the reset arm is a KNOWN SURVIVOR

**N10 `reset_read_as_alive`** replaces `Err(e) => !matches!(…WouldBlock | Interrupted…)` with `Err(_) => false`.

- **Predicted: SURVIVES** every test (INFERRED). Every closed-link test produces a FIN (I, P, T, K) or unread bytes
  (H).
- **Why no test:**
  - A reset at the probe is not portable to produce. On Linux, a peer that closes without reading A's handshake sends
    one.
  - On macOS, `SHUT_RD` flushes the receive buffer and the close is a FIN (review 2, INFERRED from memory of both
    stacks).
  - A test that is red on one platform and vacuous on the other would be worse than none.
- It is registered and run so that its survival is a recorded measurement, not a surprise.

### K, strengthened: EQUALITY with the transport's own meters

K now asserts that `n.transport_counters()`'s `idle_probes` and `idle_redials` EQUAL `n.net.idle_probes()` and
`n.net.idle_redials()`, not just that they are nonzero.

- To avoid a race, the snapshot is bracketed by two direct readings. It is compared only when the two readings are
  identical, retried every 10 ms for up to 100 tries, and the test fails if the meters never hold still.
- The ≥ 1 premise stays in front of it.
- N7 and N8 still fail K, now at the equality.

### Nits

- `tests_transport.rs:2733`'s section comment says "busy links pay nothing". It will say two clock reads per frame.
- The module header's "only in the narrow race" will name all three costs: the race, the band a violated premise
  opens, and a redial onto a refusal.
- **N3's failure site in P is `expect_recv`**, before the `idle_redials` assertion. The original table's
  "(`idle_redials` 0)" named the wrong line.
- **B's `t0.elapsed() >= gate + 1 s` cannot fail as written**, because the 60 × 100 ms sleeps alone take 6 s.
  - **It is KEPT, as a fixture guard**, and its comment says so. If `FRAMES` or the sleep is cut until the link no
    longer outlives the gate by a second, the guard fails loudly instead of letting B go quietly vacuous.
  - The sleeps are the real anti-vacuity.
- H's comment "left as `conn_loop` leaves it" is exact only for the refusal after all six bytes were read. The
  `stop` and deadline refusals return before reading them, so on Linux their close is a reset behind the `Error`
  frame. Under the fix both read as closed. The comment will say so.
- `idle_probe_gap` says the peer's clock runs "from its last read". It runs from its last complete frame
  (`last_heard`). The bound holds up to the delay between A's write returning and its refresh, which sits inside the
  stated residual race. The doc will say so.

### Counts and Run G3, amended (predicted)

| module | tests |
|---|---|
| transport | 62 + T = **63** (62 off macOS) |
| node | 14 |
| log | 58 |
| replicate | 60 |

**Per-target, re-derived.**

- Instrument: `git diff 9aa6968 <sha> | grep -cE '^\+\s*#\[test\]'`, with 0 removed in every case, added to D207's
  base of 2579 (never measured).
- `d815c1b`: +25, so 2604. `1eaf1a7`: +29, so 2608. `bf1fd63`: +34, so 2613. With T: **2614**.
- The lead's 2608 comes from review 2 and reproduces exactly: it is the merge of `d815c1b` with `49ba420`,
  2604 + I, B, P, H. The merge that was actually made took D207's final `1eaf1a7`, which adds W1–W4. K and T come
  on top of that.

---

## Amendment 7 — review 2's changes recorded, before any run (nothing built)

| commit | what |
|---|---|
| `5c6e330` | amendment 6 |
| `8a2b344` `8dda35b` | G2 and the doc nits in `transport.rs`, **comments only**, then a reflow of one comment |
| `bdf6b0e` | test T (additions), plus three test comments corrected. B's elapsed check is now labelled a fixture guard; its assertion is unchanged |
| `d9824d2` | K asserts equality, bracketed, as amendment 6 registered |
| `5b7d98d` | N9 and N10; N1–N10 regenerated from `d9824d2` |

- **No code line in `transport.rs` changed since `bf1fd63`.** Instrument: `git diff bf1fd63 5b7d98d --
  src/consensus/transport.rs`, keeping `^[-+]` lines and dropping comment lines, leaves 0.
- **The removed test lines are four comment lines in `tests_transport.rs` and K's four lines in `tests_node.rs`.**
  K's lines are the two `>= 1` assertions, their comment and the snapshot binding. The new equality assertion
  covers them, because the premise (≥ 1 each) still stands in front of it.
- **Counts at `5b7d98d`:** transport **63**, node 14, log 58, replicate 60. `git diff 9aa6968 5b7d98d` adds 35
  `#[test]` and removes 0. **Per-target: 2579 + 35 = 2614** (predicted; the base is D207's, never measured).
- **Mutants:** all ten patches pass `git apply --check` against `5b7d98d`'s tree. N1–N8 keep their edits; only hunk
  context moved.

| mutant | module | predicted |
|---|---|---|
| N1–N6 | transport | as amendment 2 |
| N7, N8 | node | K, at the equality |
| **N9** | transport | **T** only |
| **N10** | transport | **SURVIVES**, a known survivor (amendment 6, G4). A kill is an unregistered result, and it is reported as one |

---

## Amendment 8 — review 3's R1–R5, written BEFORE their code (nothing built)

Source: `artie-research frontier/d224_review3.md` @ `68e5a9d`, a review of `49ba420..c797187`, verdict
SOUND-WITH-CAVEATS. Its findings: the merge equals git's own; K cannot race; G2's retractions are complete; and no
code line in `transport.rs` changed after `bf1fd63`. The rest corrects registrations and claims, adds one test,
strengthens K, and registers three mutants. The only changes to `transport.rs` are to comments.

### R1: T also kills N1–N4. Corrected kill sets (before any run)

T joined the transport module at `bdf6b0e`, and N1–N6 run on that module. Traced by review 3 (INFERRED):

- N1 and N3 lose the frame, so T fails at `got`.
- N2 probes the carried frame again on the fresh link, so T fails at `idle_probes − probes == 1` (it is 2).
- N4 drops the frame, so T fails at `got`.

With test L (below) added, the full table reads:

| mutant | fails (transport module unless noted) |
|---|---|
| N1 | I, P, H, **T**, **L** |
| N2 | B, P, `a_broken_connection_is_reconnected_and_the_frame_lost_to_it_is_counted`, **T** |
| N3 | I, P, H, **T**, **L** |
| N4 | I, P, H, **T**, **L** |
| N5 | B |
| N6 | H |
| N7, N8 | K (node module) |
| N9 | T |
| **N10** | **L** (no longer a survivor; see R2) |
| **N11**, **N12** | K (node module; see R3) |

- **L under N1–N4.** Under N1, N3 and N4, L's frame never arrives on a second connection. Under N1 and N3, the write
  into the reset link fails, is counted, and drops the frame, and the redial then carries nothing. Under N4, the
  frame is dropped at the probe.
- **L under N2:** it passes. The second probe on the fresh link finds it alive, and L asserts only redials +1 and
  `lost_in_flight` 0, as H does.
- **Where this table supersedes older ones:** amendment 2's table, A7's "N1–N6 as amendment 2", lane §3 and §4, item
  (d) of the FAN-QUEUE row, and the run script's comments.

### R2: N10 is killable. Test L, and the "not portable" reason retracted

- **Retracted** (append-only): amendment 6's G4 reason, "a reset at the probe is not portable to produce". The same
  reason sits in `make_mutants.py`'s N10 comment and in lane §4. The repo already produces a reset on close with no
  dependency: `a_peer_that_resets_before_accept_cannot_fill_the_connection_cap` sets `SO_LINGER {1, 0}` through an
  `extern "C" setsockopt` (`tests_transport.rs:2054-2104`, READ).
- **L** = `a_link_the_peer_reset_is_redialled_after_an_idle_gap`, additions only. It is H's shape, with a reset in
  place of the refusal:
  - the hand-rolled peer reads all six of A's bytes, writes its handshake, sets `SO_LINGER {1, 0}`, and drops the
    socket, which sends a reset and no FIN;
  - after 600 ms (gate 150 ms), ONE send must arrive on a second connection, with `idle_redials` +1 and
    `lost_in_flight` 0.
- **How `peek` sees it** (INFERRED from memory of both stacks, as review 3 says): `peek` returns an error on both
  XNU and Linux. XNU returns the pending `so_error`; Linux returns `sk_err`, since no FIN set `SOCK_DONE`. The fix takes
  the `Err` arm and redials. Under N10 the probe calls the link alive, and the write into the reset socket fails and
  drops the frame.
- **Platforms: `cfg(any(target_os = "macos", all(target_os = "linux", any(target_arch = "x86_64",
  target_arch = "aarch64"))))`**, with each platform's own constants:
  - macOS: `SOL_SOCKET` `0xffff`, `SO_LINGER` `0x80`, as the existing test uses;
  - Linux's asm-generic values: 1 and 13.

  `struct linger` is two `int`s on both. Other targets, Windows among them, do not compile L, so N10 is scored on
  macOS and Linux only.

### R3: K ends with UNEQUAL meters

K ended with `idle_probes == idle_redials == 1`, so a `counters()` that swapped the two, or filled one from the other,
passed its equality. K's fixture now leaves **probes ≥ 2 and redials 1** (a registered test change, since K has never
run):

1. The node's link to 2 stays OPEN through a first poll past the gate. Its first frame is probed, found alive and
   written. The test reads that frame from `c1` to prove it, and drains the rest.
2. Then `c1` closes. After another gap past the gate, a second poll campaigns again. That frame is probed, finds the
   close, and redials.

- **Premise:** `n.net.idle_redials() ≥ 1`, and `n.net.idle_probes() > n.net.idle_redials()`, both read from the
  transport. The meters must differ, or a swap would pass.
- **Assertion:** unchanged. The snapshot equals the direct readings, bracketed.
- **Mutants (the node module):**
  - **N11 `counters_swap_probes_and_redials`**, the two fields swapped in `counters()`;
  - **N12 `counters_redials_read_from_probes`**, `idle_redials: self.idle_probes()`.

  K fails both at the equality. N7 and N8 still fail it too.

### R4: the claim "never a false red on the fix" is corrected, and T's comment with it

- **The gate measures from A's REFRESH, not A's write.** `last_used = Instant::now()` runs only after `write_all`
  returns. B's 1 s clock starts when B finishes reading that frame, which on loopback is usually before A's refresh.
- **So the fix can fail T in two narrow windows** (INFERRED):
  - **Route 1:** A's refresh comes more than about 300 ms after B's read, from a stall or a SIGSTOP between the
    write returning and the refresh. Nothing is probed, and T fails with `idle_probes +0`.
  - **Route 2:** A's kernel has not processed B's FIN by the end of the 300 ms settle. T fails with
    `idle_probes +1, idle_redials +0`.
- Every other stall widens the gap, and can only make N9 survive falsely.
- **Amendment 6's "load can only widen it" and "never a false red on the fix" are retracted.** A T failure is read
  by its message, as above.
- **T pins the gate below about 0.65 D, not at D/2** (1.3 s against D = 2 s). A gate of 0.6 D would pass T and still
  break the stated tolerance.

**Also corrected, comments only** (review 3's INFO):

- **"Two frames" is a lower bound.** Every write made before the peer's reset reaches this socket is lost uncounted,
  and only the first after it is counted. The module header and `idle_probe_gap` will say "at least two". The
  retransmission bound still covers this.
- **The re-campaign citation.** `voter_tick` is at `election.rs:53-61`, not `:52-60`. It campaigns only when
  `may_campaign()`; otherwise no campaign was due, so no lost frame goes unresent. The doc gains the condition.

### R5: 2579 WAS measured; 2614 is a macOS figure

- **Retracted:** amendment 6's and amendment 7's "(the base is D207's, never measured)".
- READ: `/Users/idide/wt/logs/lead-land-0924/verify-d187-9e76a4c/SUMMARY.txt` says
  `mode=per-target rc=0 passed=2579 failed=0 build_errors=0 head=9e76a4c`.
- `git diff --quiet 9e76a4c 9aa6968` exits 0, so that is main's tree, measured on this Mac.
- **Per-target at the new tip (predicted), and how the delta is built:**
  - 2579 + 35 + L = **2615 on macOS**.
  - Linux x86_64 and aarch64: 2614. D207's macOS-only test drops out, and L stays in.
  - Other targets: 2613.
  - K's changes add no test.

### Run G4 at the new tip (predicted)

| module | macOS | notes |
|---|---|---|
| transport | 63 + L = **64** | 63 on Linux x86_64 and aarch64 |
| node | 14 | K strengthened, no test added |
| log | 58 | |
| replicate | 60 | |
