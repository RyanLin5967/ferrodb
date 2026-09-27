# F1: the listener answers a pending connection at once, not after a 50-ms accept poll

FAN-QUEUE row; engineering, not an invention lane. Lead-authorised 2026-09-27. Lane r11-dist.

- Branch `d224-f1-accept.noindex`, cut from `d224-idle-failover` @ `4097a1e` (the branch tip). The lead's brief cited
  `7d9567f`, which is an ancestor of the tip. The tip adds D224 reviews 2-3 and their tests and mutants N1-N12.
- Never landed from here. The full suite runs at landing, through lockrun.

## The defect (source, at 4097a1e)
- The listener is set nonblocking (`transport.rs` `Transport::start`, `set_nonblocking(true)`). `accept_loop` sleeps
  `poll_interval` (default 50 ms, one election tick) on every WouldBlock.
- So a connection that arrives while the accept thread sleeps waits out the sleep before its handshake is answered, and
  `dial` blocks on that answer.
- D224 redials a follower-to-follower link after the peer's idle close, and that redial carries a survivor's campaign
  frame. Election timeouts are drawn in whole ticks (`election.rs` `draw_timeout`, [10, 20) ticks). Two survivors whose
  timers expire within a tick of each other both pre-campaign before either one's PreVote lands, and the vote splits.
- r11-dist DC2 (artie-research `frontier/round11/r11-dist`, raw 74ab3673) ran the real `Consensus` in virtual time over
  salts 1..10,000:
  - 14.96 splits per 100 first failovers at the 50-ms poll;
  - 0.21 per 100 when the accept answers at once;
  - 0.06 per 100 with links never idle-closed.
  - DC2 is a MODEL of the transport. Its assumptions are under an adversary (r11-dist-refute-measure), and quoting 14.96
    is gated on that report.

## Deflation
- Raft section 9.3 (Ongaro and Ousterhout, 2014) already requires broadcast time well below the election timeout spread:
  randomized timeouts make split votes rare only when one server's campaign reaches the others before theirs start.
  - Here the spread is quantized to 50-ms ticks, and the first frame after an idle close could itself cost up to a tick.
- etcd's rafthttp keeps long-lived streaming connections per peer, so its campaign frames never wait on an accept.
- Waking a blocking `accept` at shutdown by connecting to the listener is the ordinary zero-dependency pattern.
  (Recalled from tiny_http's `Server` drop; not re-read, unverified.)
- Nothing here is new. This row is ordinary engineering.

## The change
1. RED commit 323c965 (observation only; behaviour unchanged):
   - meter `accepts_after_sleep`: a connection taken on the first `accept` after the accept thread slept, whatever the
     sleep's reason, is one that waited out the sleep;
   - accessor, `TransportCounters` field and `Debug` field;
   - test `a_redial_after_an_idle_close_is_accepted_without_waiting_for_an_accept_poll`.
   - The test uses no API the fix adds. The meter is the red commit's own instrument, as D224's counters were D224's.
2. FIX:
   - The listener is set BLOCKING in `start`.
   - The WouldBlock arm is merged into the failed-accept arm (one poll of pacing, as before).
   - A connection taken after the stop flag is closed unserved and the loop leaves.
   - `shutdown` wakes the accept thread by connecting to `wake_addr(local_addr)`, the listener's own address with an
     unspecified IP mapped to loopback, then joins it.
   - `wake_accept` retries a failed connect every `poll_interval` for at most `handshake_deadline`. Past that it DETACHES
     the thread rather than join for ever. That is a stated blind spot: a node that cannot connect to its own listening
     address.
   - The accept handle is kept apart from the sender threads.
   - `poll_interval`'s doc no longer claims the accept poll.

## Registered predictions (before any run; lib test binary, filter `consensus::`, `taskpolicy -b`)
- R (red, at 323c965):
  - The new test FAILS on its final assertion only, with (A, B) = (>= 1, >= 1). A counts B's startup dial; B counts A's
    redial. Every premise assertion before it passes: the idle close, exactly one redial, the frame arriving, no loss.
  - Every other `consensus::` test passes, as at the base: the meter changes no behaviour.
  - The consensus test count is registered below from `--list`, before any run.
- G (green, at the fix commit): every `consensus::` test passes, the new one with (0, 0). Named risks:
  - `a_peer_that_resets_before_accept_cannot_fill_the_connection_cap` (macOS) says "a slow accept poll, so the resets
    below land while the loop is asleep". Under F1 the accept does not poll. Its trigger (EINVAL on `SO_RCVTIMEO`) still
    needs only the RST to land before the SERVER sets its read timeout, which it does after a thread wake, `accept`, a
    `dup`, a lock and a `fcntl`. The client closes two syscalls after `connect` returns.
    - PREDICTED: it still passes.
    - If its anti-vacuity assertion (`refused_conns() > 0`) fails, F1 has removed the test's trigger. That is a fixture
      decision (a test edit) for the lead, and it is NOT made on this branch.
  - `shutdown_joins_every_thread_and_closes_the_listener`: passes. The wake joins the accept thread, so the listener is
    closed when `shutdown` returns.
  - `refusals_at_a_full_cap_are_paced_by_the_poll_interval`: passes. Pacing is unchanged.
  - `a_connection_whose_socket_setup_fails_releases_the_slot_it_reserved`: passes. It hands `accept_loop` its own
    nonblocking listener; the merged arm sleeps its ZERO poll exactly as the WouldBlock arm did.
- Mutants, each applied to the fix commit alone; killed = the named test fails:
  - M1 `listener_polled_again` (`set_nonblocking(false)` becomes `true` in `start`): the new test fails with
    (>= 1, >= 1).
  - M2 `shutdown_never_connects` (the connect in `wake_accept` never attempted): `shutdown` detaches after
    `handshake_deadline` (5 s in `fast()`), and `shutdown_joins_every_thread_and_closes_the_listener` fails because
    the listener is still accepting.
  - M3 `stop_not_checked_after_accept`: predicted to SURVIVE. The wake connection is served, reads EOF at its
    handshake, and the loop leaves at its head, so the check saves a thread spawn and is not load-bearing. Registered
    as a stated survivor.
- Counter band for the protocol consequence: DC2's Zero arm, < 1 split per 100 first failovers (measured 0.21 on the
  model).

## Counts, from `--list` of the red binary (target/debug/deps/ferrodb-d929e200b07daf14 at 323c965), before any run
- `consensus::`: **381 tests**, 0 ignored. By module: election 35, log 58, membership 25, node 14, replicate 60, signing 52,
  sim 24, snapshot 38, contract 10, transport 65. The macOS-only reset test is included on this host.
- R: **380 passed, 1 failed** (the new test). G: **381 passed, 0 failed**.
- A failure of any OTHER test at R belongs to the base, not to F1. It is re-run alone, and reported with its panic line.
- Registered 2026-09-27T09:03:43Z.

## Amendment 1 (2026-09-27T09:07:39Z; after R and G, before any mutant or repeat run)
- R held: 380 passed, 1 failed (the new test, final assertion, (A, B) = (1, 2)). Raw 4eac4f2.
- G held: 381 passed, 0 failed, the reset test included. Raw 2cd07b4.
- Mutants: the three patches in `mutants/`, each against the fix commit ca771fb, run by `fire.sh` over all 381
  `consensus::` tests. transport.rs is restored from git before each run.
  - M1: killed by the new test only.
  - M2: killed by `shutdown_joins_every_thread_and_closes_the_listener`. Other tests may also fail, since every shutdown
    now detaches its accept thread after 5 s; the run is slower.
  - M3: survives (381 passed).
- Repeats, to separate a race from a result (one binary each, the single test, 20 runs):
  - the new test on the RED binary: 0 of 20 pass;
  - the new test on the GREEN binary: 20 of 20 pass;
  - `a_peer_that_resets_before_accept_cannot_fill_the_connection_cap` on the GREEN binary: 20 of 20 pass (the named
    F1 risk). Any failure there is reported with its panic line, as a fixture question for the lead, and not edited.

## RESULT (2026-09-27T09:13:07Z)
- R (raw 4eac4f2): 380 passed, 1 failed. The new test failed on its final assertion with (1, 2). **As registered.**
- G (raw 2cd07b4): 381 passed, 0 failed, every named risk included. **As registered.**
- Repeats (raw 2722963):

  | Test | Binary | Result |
  |---|---|---|
  | new test | red | 0/20 pass; all 20 fail at :3159, (1,1) x6 and (1,2) x14 |
  | new test | green | 20/20 pass |
  | reset test (macOS) | green | 20/20 pass |

  **As registered.**
- Mutants (raw 36d5c04; five distinct binaries by sha256; transport.rs restored from git before each run, tree clean after):

  | Mutant | Tests | Result | Registered |
  |---|---|---|---|
  | M1 `listener_polled_again` | 380 passed, 1 failed | **KILLED** by the new test alone, at :3159 with (1, 1) | as registered |
  | M2 `shutdown_never_connects` | 379 passed, 2 failed | **KILLED** | as registered |
  | M3 `stop_not_checked_after_accept` | 381 passed | **SURVIVED** | the stated survivor |

  - M2 was killed by `shutdown_joins_every_thread_and_closes_the_listener` (:1095, "the listener was still accepting
    after shutdown").
  - M2 also failed `a_shutdown_during_a_dial_does_not_wait_out_the_reconnect_delay`: shutdown took 60.2 s. That test
    sets `handshake_deadline` = 60 s, which is the wake's bound, so the fix's bounded detach is measured. (That test's
    own message blames the reconnect delay; under M2 the cause is the wake.)
  - M3 survived because the check saves a thread spawn and is not load-bearing.
- Not run here, by the lead's scope: the full suite, including `tests/integration_consensus_failover.rs` and the cluster
  integration targets that start and stop transports. It runs at landing, through lockrun.

## Amendment 2 (2026-09-27T09:18:02Z): F1 is judged on the REAL transport, not DC2. Registered before any instrument is built or run.
The lead's ruling after the DC2 adversary (r11-dist-refute-measure, artie-research 32604b84; REPORT c12bf4e1):
- DC2 confirms the real `Consensus` split condition: a split happens iff the first-frame latency L exceeds the survivors'
  expiry gap delta.
- DC2's 14.96 and 0.21 per 100 are production-MODEL estimates.
- F1's "< 1 per 100" holds only if the first-frame latency after an immediate accept, thread spawn included, is under
  about 1.65 ms at the box's load.
- The red test and mutants M1-M3 above stand as they are.

**Where it runs.**
- The r11-dist harness worktree (`r11-dist.noindex`) with this branch merged, plus ONE measurement-only commit that is never
  landed.
  - A runtime arm switch: the lib static `R11_ACCEPT_POLL` makes `start` leave the listener nonblocking. That is exactly
    M1, which is the base accept path: the failed-accept arm sleeps `poll_interval` on WouldBlock, as base's WouldBlock arm
    did.
  - A bounded per-transport event trace, with instants on one process clock:
    - accept-loop wakes from a sleep;
    - accepted connections;
    - each connection's first frame delivered to the inbox, with its sender's id;
    - the sender side: redial found, dial start, dial done, first write;
    - a Node log of role transitions with instants, and the tick grid (`next_tick`).
- So base and F1 are the SAME binary. They are interleaved per salt: even salt indexes run base first, odd ones F1 first.

**Failover design (instrument 6).**
- `firstfail`, N = 10, the production idle deadline (60 s), a 62-s idle before the stop, and the 50-ms tick.
- Random per-node phases: the three `Node::start`s run in a salted random order, each at T0 + U(0, 50 ms). Each node's accept
  grid and tick grid keep their production relation: the transport starts inside `Node::start`, just before `next_tick` is
  set.
- 30 salts from DC1, each with a winner at least 2 ticks below both survivors, so that start offsets cannot change the winner:
  - gap 0: 2, 3, 5, 10, 21, 37, 66, 91, 98, 106, 123, 129;
  - gap 1: 7, 27, 28, 42, 46, 48, 52, 64, 65, 70, 82, 84;
  - gap >= 2: 1, 4, 6, 8, 9, 14.
- 60 failovers in 4 lockrun holds of 15, each under run.sh's 1200-s timeout, with the 2-s process monitor.
- No load gate: the question is latency AT this box's load. Every hold is scored under A16's refined rule, and its load is
  printed beside every number. Pairing makes the base/F1 contrast robust to load drift.
- Gap classes are taken post hoc from `timers_at_stop`.

**Predictions** (INFERRED unless stated).
- (3) L3 = first frame delivered at the receiver minus dial start at the sender, per redial, 2 per failover:
  - base: spread over about 0-55 ms, median 15-35 ms, at most 10% under 1.65 ms;
  - F1: median AND p90 under 1.65 ms.
  - If F1's median is 1.65 ms or more, F1's "< 1 per 100" is REFUTED at this load.
  - Reported beside it: dial to first write (sender side) and the accept wait (accepted minus dial start).
- (1) base arm: the accept phase at each redial, psi = (accepted_at_receiver - receiver's tick grid) mod 50 ms, pooled
  (about 60 values). Predicted: a KS test against U(0, 50) is not rejected at 0.05.
  - The adversary's correlated-start concern predicts the opposite: clustering. Either way this settles item (1).
- (6) splits, where a split means both survivors became Candidate in one term:
  - base: gap 0 about 8/12 (registered range 5-11); gap 1 about 2/12 (0-5); gap >= 2 0/6.
  - F1: gap 0 at most 2/12; gap 1 0/12; gap >= 2 0/6.
- The mechanism, per failover: take E = the survivor that pre-campaigned first, delta = t_pc(other) - t_pc(E), and
  L_E = E's PreVote first frame at the other minus t_pc(E). Predicted: split iff L_E > delta, in at least 95% of the
  failovers where both are measured.
- Phases: over gap-0 failovers, delta is spread over [0, 50) ms, with no 10-ms bin holding more than half the values.

## RESULT 2 (2026-09-27T11:09:36Z): amendment 2 on the real transport, holds 1-4 (56 of 60 failovers; hold 5 is queued in lockrun)
- Raw is in artie-research `frontier/round11/r11-dist/raw/`, banked unread: f1i_h1 98e752df, h2 2cd46583, h3 b6652c42,
  h4 a728319f, each with its .procs.txt.
- Analysis at 7056c947, by `f1i_analyze.py` (c4d04c32), which was committed before the data.
- Binary: r11-dist.noindex 85d4401 (this branch merged at 8db6416, plus measurement-only instruments).

**(3) L3, from dial start to the first frame delivered at the receiver, spawn included (56 redials per arm):**

| | Base | F1 |
|---|---|---|
| median | 29.4 ms | 0.215 ms |
| p90 | 49.3 ms | 0.315 ms |
| p99 | 59.3 ms | 0.342 ms |
| max | 60.0 ms | 0.345 ms |
| redials under 1.65 ms | 1/56 | 56/56 |

- Base HELD. F1 median and p90 under 1.65 ms HELD.
- Load: 8.2-72.0 across the holds.

**(1) Base accept phase against the receiver's tick grid:** KS D = 0.134, p = 0.246 (n = 56); 10-ms bins
[10, 11, 6, 13, 16]. Not rejected. HELD.

**(6) Splits:**

| gap | Base | F1 |
|---|---|---|
| 0 | 9/11 | 0/11 |
| 1 | 2/11 | 0/11 |
| >= 2 | 0/6 | 0/6 |

- All six bands HELD.
- Mechanism: split == (L_E > delta) in 28/28 failovers per arm. HELD.

**FAILED: "no 10-ms bin of gap-0 delta holds more than half" on the base arm.**
- Bins [7, 1, 1, 1, 1] (F1: [5, 2, 2, 0, 2]).
- The salted start offsets fell close together for most gap-0 salts: survivor offset differences 2.2-34.3 ms, 7 of 11 below
  13 ms. The arms share offsets per salt.
- The base gap-0 rate 9/11 sits above 2/3 because delta skews small, which is consistent with the 28/28 mechanism result.

**Load verdict:** every hold is VOID under A16's refined rule, and under the strict rule, from other lanes' rustc, dolt and node
processes. There was no load gate by registration. The arms are paired within each hold. Load can only lengthen F1's latency,
so its measured maximum is an upper bound for this box.

**Standing:** F1's "< 1 per 100" is now backed by measured real-transport latency (at most 0.345 ms, against the 1.65-ms bound)
and by 0 splits in 28 real failovers, 11 of them equal-draw pairs, where the paired base split 9. The production base rate
(DC2's 14.96 per 100) is still model-only.
