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
