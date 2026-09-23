# Certification — 2d4e1c2..2e62bce (D169)

| gate | result |
|---|---|
| `verify-suite.sh` (per-target) | `rc=0 passed=2529 failed=0 build_errors=0 head=2e62bce` |
| `verify-suite.sh` (go) | `rc=0 passed=97 failed=0` |
| `certify-head.sh` | `OK — suite head=2e62bce == landing 2e62bce` |
| `staleness.sh` | `0 behind, 510 ahead — COVERED` |
| `prepush.sh` | `OK` (incl. `cargo check --target x86_64-pc-windows-msvc`) |

**+0 @ 2d4e1c2, absolute 2529 per-target.** Correct: the only source change is a `REAP_PERSIST`
env knob and a `syncs_issued()` counter in `examples/d31_reap_cost.rs`; no `#[test]` added or
removed. ⚠ **MODE:** 2529 is **per-target**; `whole` reads 2531 (per-target + 2 doctests).

## What landed, and what was corrected in it

**D169** measured D81's own stated risk and **falsified my pre-registered prediction**: the
persistence ON/OFF ratio has log-log slope **0.062** against ~1.0 registered. The prereg's outcome 2
required naming what bounds it rather than reporting "no effect" — it is **D81 itself**
(`arena.rs:2378`: the reaper's fast path also appends a 16-byte delta, which I had mis-scoped).

The result then went to an adversary (`frontier/reap_fsync_adversary.md`, **SURVIVES-NARROWED**)
and **five corrections were applied before landing**, all visible in the artifact rather than
silently replaced:

1. **"Exactly 4.00 fsyncs per branch" is FIXTURE-BOUND.** `d31_reap_cost` forks every branch from
   TRUNK, so it only measures childless leaves of a Live parent. True range:
   **`2 ≤ syncs ≤ 3 + (reaped-ancestor chain length)`**; a shallowest-first expiry gives 3.12/branch.
2. **The phase table's ORDER was backwards** — and the order it printed is the one `reaper.rs:690`
   names, in capitals, as a historical **correctness bug** ("MARK REAPED FIRST, THEN DETACH").
3. **§4 cited `reap_expired`, which has no production caller.** Production sweeps via
   `lease_thread::scan_once`. I cited the harness while claiming something about the engine.
4. **The named fix was the wrong primitive.** Not group commit — `stage()`-without-`durable()`,
   which already exists here as D159's `fork_staged`/`await_fork_durable`.
5. **The per-fsync cost was re-measured** rather than divided out of a duration: `sync_all` is
   **~3.0 ms, flat from 1 to 64 dirty pages** (`F_FULLFSYNC`). It is the barrier, not the bytes.

## ⛔ Also in this range: four FALSE provenance claims retracted in place

`d168_prereg`, `d168_persistence_penalty_at_head`, `d169_prereg` and `d169_reap_is_fsync_bound`
each said **"measure lock HELD"**. That was false: I held `~/wt/artie-research/.measure.lock`, a
path I invented that **nothing references**, instead of the fleet lock `~/wt/logs/.measure.lock`.
I had exclusivity against nobody. The numbers are probably unaffected (`pgrep -x cargo` read 0 at
both launches) but that is a different instrument and luck, not the cited guard. ⭐ It stayed
invisible because **a lock you own alone never contends and never reports that it is doing
nothing.** See `frontier/FLEET.md` for the fix (use the script; `trap … EXIT` in the same command).
