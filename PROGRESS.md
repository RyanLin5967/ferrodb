# Phase F — per-lane working notes

Each section below is one agent's own scratch record, kept verbatim from its lane branch at the
point that lane was merged into `agent-isolation`. They are HISTORY, not status: a "next action"
here describes what that agent was about to do inside its own worktree, and every lane named in
this file has since been merged. The authority for current state is `LEDGER.md`.

---

# F3 — transport

**Done and committed.**
- `94b0e50` — codec for every `Body`/`Command`/`BranchOp` variant + `Entry`/`Config`/`SnapshotMeta`;
  resumable `FrameReader`; thread-per-peer `Transport`; `REPL_VERSION` 1→2 and `CONSENSUS_TAG`
  added to `src/replication/mod.rs`. 30 tests.
- `4c85d22` — fd leak (`try_clone` dups the descriptor; registry never drained), zero-duration
  validation, and the M19 test rewritten to isolate the tag check. 34 tests.
- Mutant batch 1: 19 fired, 18 killed, **M19 SURVIVED** (fixed in `4c85d22`), **M17 was a false
  kill** (did not compile). Log: `~/wt/logs/F3-mutants.txt`.

**Uncommitted in the tree right now (waves 2 and 3 of the adversarial review):**
11 further defects found by a fresh-context review of `4c85d22`, all verified against the code
before fixing. Most serious: `decode_config` was O(learners×members) — one 8 MiB frame carries 2M
node ids, ≈10^12 comparisons, a remote DoS. Also: orphaned threads on a failed spawn, unbounded
inbound mpsc, uncounted write-failure loss, unbounded inbound connections, no idle deadline, `dial`
ignoring `stop`, concurrent `shutdown` not a barrier, `send`-after-stop silently discarding,
`try_clone` busy-loop, and `as u16` truncation in the Catalog encode path.

**Single next action.** Read the result of `cargo test --lib consensus::transport` (log:
`~/wt/logs/F3-t.log`; wave-3 tests were appended after it started, so re-run). Then: commit, run
mutant batch 2 (`/private/tmp/.../mutants2.py`, M17/M20–M23) plus `m04_evidence.py`, then
`VERIFY_MODE=per-target tools/verify-suite.sh` for the full number, then write
`scratchpad/F3-transport.md` and `/Users/idide/wt/artie-research/build-F/F3-transport.md`.

**Machine note.** Six or seven sibling agents run cargo concurrently; builds take minutes, not
seconds. Judge a run by its output advancing, never by elapsed time.

---

# F5 — membership changes

**Done:** read DISTRIBUTED.md §F5, mod.rs (frozen), config.rs, election.rs, F1's tests and mutant
driver. Design written to `scratchpad/F5-design.md` and committed.

**Now:** adversarial review of the design (the precondition's counting set is the whole row), then
implement `src/consensus/membership.rs`.

**Next action:** write `src/consensus/membership.rs` — `Change`, `plan_change`, `begin_membership`,
`note_config_in_log`, `note_config_ack`, `apply_committed_config`.
