# F11 — the server must actually reap

Branch `F11-server-reaps`, worktree `~/wt/ferrodb-F11-server-reaps`. Commits `8c2dfdb`..`d3208cf`.

This is the worktree copy. The dispatcher's copy is
`~/wt/artie-research/build-G/F11-server-reaps.md` and says the same thing.

## The brief's premise, checked first

The brief said ledger row S2b was stale and it is: `src/cli/cli.rs:88` and `examples/pgserver.rs:83`
both construct `ArenaPageStore`, and `pgwire::ServerContext` holds a storage-backed `AgentRuntime`,
so both shipped entry points have a branch engine with real pages to reap from. Confirmed by reading
the constructors rather than by grep.

What was missing was the caller. `Reaper::reap_expired` and `TwoTierReaper::resume_interrupted_reaps`
had no caller outside `tests/` and `examples/agent_isolation_demo.rs`, and `src/` contained no
background thread at all.

**A second leak, not in the brief, found on the way in and closed:** neither binary called
`AgentRuntime::with_reaper`, so `seal` took its no-reaper path — a merged or abandoned branch was
marked `Reaped` through the `BranchCatalog` trait and **its extents were never freed**. Every `MERGE`
and every `ABANDON` in the shipped CLI and server leaked the branch's pages until the file was
rebuilt. `a_merged_branch_gives_its_extent_back_without_any_lease_scan` pins it, and mutant M10
(dropping `with_reaper` again) kills that test.

## What was built

`src/branch/lease_thread.rs` + `src/branch/lease_thread/tests.rs` (12 tests), plus the wiring in
`src/cli/cli.rs` and `examples/pgserver.rs`, and `tests/integration_server_reaps.rs` (8 tests).

Three rules, each with a named test and a mutant:

1. **Resume before scanning.** `LeaseThread::start` runs `resume_interrupted_reaps` on the
   *caller's* thread, under the runtime lock, before it spawns anything — so a caller holding an
   `Ok` knows no half-reaped branch is left, and a failure is returned rather than lost in a
   background thread.
2. **Never reap inside a merge.** The lock a merge holds is the pgwire **catalog mutex**:
   `pgwire::serve` documents it as taken outermost for the duration of one statement, and
   `pgwire::extended::Statement::execute` is the single place both the simple and extended protocols
   run SQL, so `MERGE` is strictly inside it. The scan takes that same lock through a `RuntimeLock`
   trait — **no new lock and no new lock order**. The CLI, which had no mutex because it was
   single-threaded, gets `CatalogLock` and locks per statement, the identical rule by hand.
3. **Never guess the time.** `LeaseDeadline::try_now_millis` → `cluster::lease_now_millis`. A
   cluster member with no applied `LeaseTick` **refuses to reap** and says so, counted in
   `LeaseStats::refused`, rather than substituting a local reading.

Stoppable cleanly: a `Condvar`, so `stop()` does not wait out the interval, and `Drop` stops and
joins so a lease thread cannot outlive the store it scans.

`FERRODB_LEASE_SCAN_MILLIS` (default 30000) sets the period. An unusable value makes the process
**refuse to start**; there is deliberately no value that disables the scan.

## Evidence

* `scratchpad/F11-mutants.md` — 11 mutants, raw cargo output in `scratchpad/raw/`.
* `scratchpad/F11-suite.txt` — per-target suite under CI's exact `RUSTFLAGS`. **1715 pass, 0 fail.**
* `scratchpad/F11-reruns.txt` — the four targets the loaded batch could not finish, 3/3 each.
* `scratchpad/F11-final.txt` — confirmation run after the last fix, with its scope named.
* Go suite: `ok github.com/RyanLin5967/ferrodb/cdc-consumer 47.944s`.

## Two things the mutant pass found, which is what it is for

* **M7 survived.** `stop_does_not_wait_out_the_scan_interval` passed against a `Halt::wait` that
  slept instead of waiting on the condvar. The test was racy: `stop()` signals then joins, and
  `wait` checks the flag before parking, so the signal usually landed first and neither
  implementation ever slept. Replaced by two tests that prove the waiter is parked first and
  **poll an atomic rather than joining** — a mutant that parks for an hour must not park the test.
* **A defect I shipped, found by reviewing my own diff against the `DbLock` semantics.** `pgserver`
  refuses a bad interval with `process::exit`, which does not run destructors, so parsing the
  variable after `DbLock::acquire` stranded `<db>.lock`; the operator who then corrected the
  variable would be told the database was already open by a process that no longer existed. Both
  binaries now parse before they touch a file, `LeaseThread::start` failure panics (unwinds, drops
  the lock) instead of exiting, and the test asserts no lock file survives a refusal. Mutant M11.

## Records corrected

`DEMO.md` items 4 and 6 asserted "no background thread runs in a shipped ferrodb process" and "an
abandoned SQL session never frees pages, and on the page-backed path that now leaks". Both were true
when written and this row made both false; corrected in place, with what they used to say quoted.
`README.md` gained the `FERRODB_LEASE_SCAN_MILLIS` paragraph.

`LEDGER.md` was **not** touched: other agents were active on this machine and it is a shared file.

## Not done, and why

No fresh-context adversarial review by a subagent. This session was instructed not to call the Agent
tool, so the review above is a self-review of the committed diff. Worth commissioning separately.
