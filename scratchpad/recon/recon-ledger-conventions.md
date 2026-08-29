Facts, with citations.

## 1. Ledger location and format

`/Users/idide/wt/artie-research/LEDGER.md` (389,805 bytes, mtime 2026-08-28 08:54). **No `LEDGER.md` at the repo root** — `ls /Users/idide/wt/ferrodb-F10-durable-tel/LEDGER.md` → "No such file or directory"; `grep -rl "overnight work ledger" --include='*.md'` over the worktree returned nothing. Companion: `/Users/idide/wt/artie-research/LEDGER-INTEGRATION.md` (21,499 bytes).

Header / legend, `LEDGER.md:1-24`:

```
1  # ferrobranch — overnight work ledger
2
3  **This file is the single source of truth for what is done and what is next.**
4  A cron-launched session has NO memory of the conversation. It knows only this file and the repo.
5
6  Repo: `/Users/idide/projects/ferrodb`  ·  Branch: `agent-isolation`  ·  **NEVER `git push`.**
7  Toolchain: `export PATH="$HOME/.cargo/bin:$PATH"` (not on default PATH).
8  Design authority: `/Users/idide/wt/artie-research/DESIGN.md` (~12KB). Do NOT read `research/` — 350KB, it exhausts context.
9
10 Status values: `OPEN` · `IN_PROGRESS <iso8601>` · `DONE` · `BLOCKED <one-line reason>`
...
21 A row becomes `DONE` only when the Evidence column holds output from a command actually run.
22 Never mark DONE from intent. Never edit a test to make a row pass.
```

Three distinct table shapes are in use:
- `LEDGER.md:674` — `| # | Row | Status | Exit criterion |` (Phase E table; an Evidence blob is appended as a trailing cell in practice, see E78a at `:678`).
- `LEDGER.md:1774-1775` — `| Row | What | Status | Exit criterion |` (the 2026-08-28 recon rows: E79c, F10, F11).
- `LEDGER.md:1818-1819` — `| Where | Items | Removed when | State |` (the F-cleanup allow table).

Dated log lines use a `·`-separated form, e.g. `LEDGER.md:663`: `- <date> · <who> · <row> · <status> · <count> · <one-line what>`.

### F10 — full row (`LEDGER.md:1777`)

```
| F10 | **The Typed Effect Log has no durable implementation.** `MemEffectLog { frames: Mutex<Vec<TxnFrame>> }` is the only non-test `EffectLog` in the crate (`src/tel/log.rs:88`; the only other impl is a `NoLog` stub inside a `merge_engine` test). Merges are computed **from** those frames, so on a cluster a promoted leader holds none of the effect log its predecessor was merging against, and no follower can verify a merge it did not host. This is the largest single gap between the agent layer and a distributed one. | OPEN | a durable `EffectLog` exists and is the runtime's default; frames survive a restart; a merge computed after a restart agrees with one computed before it |
```

This is also the only "typed effect log" row (`grep -ni "typed effect log" LEDGER.md` → line 1777 only). Its two claims verify in this tree: `src/tel/log.rs:88` `impl EffectLog for MemEffectLog {` and `src/agent_sql/merge_engine.rs:573` `impl EffectLog for NoLog {` are the only two `impl EffectLog for` in `src/`, `tests/`, `examples/`.

Adjacent context, `LEDGER.md:1780-1784`:
```
**Why F10 and F11 belong to Phase F rather than being deferred:** both are places where "it is
node-local and that is fine" was a defensible single-node argument that stops being defensible the
moment a branch can outlive the node hosting it. `escrow.rs`'s own header makes that argument in so
many words — node-local state is acceptable "because branches do not survive a restart either" —
and going distributed is precisely the change that invalidates it.
```

### F-cleanup — full section (`LEDGER.md:1810-1826`)

```
1810 ### F-cleanup — the transitional `dead_code` allows, tracked so they are removed rather than forgotten
1811
1812 CI builds with `-D dead_code`. Phase F lands one lane at a time, so each lane's seams are unread
1813 until the lane that calls them arrives, and the branch cannot be green in between without an allow.
1814 Every one is **scoped to the item**, never to a module — a module-wide allow would also silence
1815 genuinely dead code added later, which is the defect the gate exists to catch, and the lint would
1816 then read as "on" while protecting nothing.
1817
1818 | Where | Items | Removed when | State |
1819 |---|---|---|---|
1820 | `src/consensus/mod.rs` | the `Consensus` struct, `may_campaign` | `election.rs` + `replicate.rs` are both implemented | **REMOVED 2026-08-28** (`f0f26a4`) |
1821 | `src/consensus/election.rs` | `apply_config`, `observe_config_at`, `observe_quorum_watermark` | their named callers exist — `replicate.rs` for the latter two, the command applier for the first | **REMOVED** — the file now carries only a note that they once existed |
1822 | `src/consensus/membership.rs` | `begin_membership` | `on_propose` calls it | **STILL PRESENT — condition unmet, and the reason is a defect. See F-membership-ungated below.** |
1823
1824 Each removal was verified the way the conditions ask: rebuild under CI's exact
1825 `RUSTFLAGS="-D duplicate_macro_attributes -D dead_code"`, not a bare `cargo build`, which only warns.
```

Also relevant, the removal protocol as a check (`LEDGER.md:1856-1861`):
```
**The removal is itself a check, not a chore.** Delete the attribute: if the build still passes it
was doing nothing; if it fails, the named caller is missing and *that is the bug*.
```

### E79 — full rows

Table row, `LEDGER.md:682`:
```
| E79 | Wire B5's durable provenance into `src/agent_sql/runtime.rs` — three `prov_store` swaps (`:276`, `:332`, `:369`), two `prompt_hash` sites, one `bind_run` on the SQL write path. Attribution is one of the product's four selling points and currently emits `"writer":null`. | **DONE** (prompt_hash split to E79b — it needs a SQL surface, not wiring) | 1349 passed / 0 failed / 0 build errors, Go 97 / 0, at `8914ffd`; both halves fire-checked |
```

Log line, `LEDGER.md:663`:
```
- 2026-08-27 · claude · E79 · DONE · 1349 tests (+97 Go), 0 failed · agent merges now ship a writer on the feed and provenance survives a reopen; both fire-checked with anti-vacuity halves. prompt_hash split to E79b: it needs a SQL surface to carry a prompt, which is a feature rather than wiring
```

E79 carries two further blocks that qualify that DONE:
- `LEDGER.md:1284` — `### ⚠ CORRECTION 2026-08-20 — E79's edit sites are stale, and three of them do not exist`
- `LEDGER.md:1746` — `### ⚠ CORRECTION 2026-08-28 — E79's "outlives the process" was half of what it claimed`, resolving into row E79c at `LEDGER.md:1776`:
```
| E79c | **`who_wrote_row` must read the durable store, not an in-memory map.** Also fix `authors_of` and the `ferro_row_authors` view, which read the same two maps. The `durable.rs` module header is additionally STALE — it says "nothing constructs it yet" and "wiring it is one line in each of `runtime.rs:276, :332, :369`", but `with_durable_provenance` has existed since E79 and `cli.rs` uses it; those line numbers have moved. | OPEN | a row's author is answerable by `who_wrote_row` after a full process restart, and the test drives the **API**, not `provenance().attribute()` |
```
Also `LEDGER.md:1743` (a status-summary table): `| E79b | \`prompt_hash\` is \`[0u8; 32]\`; needs a SQL surface (\`BEGIN AGENT SESSION ... PROMPT '...'\`) reaching \`prompt_digest\`. | OPEN |`

## 2. Sibling test-module registration — NOT from `mod.rs`

`src/consensus/mod.rs` registers exactly one test module, and it is the only one without `#[path]` (`src/consensus/mod.rs:64-65`):
```
64  #[cfg(test)]
65  mod tests_contract;
```
`tests_log.rs`, `tests_election.rs`, `tests_sim.rs` are each declared **from the implementation file beside them**, with `#[cfg(test)] + #[path]`:

`src/consensus/log.rs:1381-1386`:
```
1381 // `mod.rs` is frozen for this phase and carries no `mod tests_log`, so the test module is declared
1382 // here. `#[path]` on a module that is not inside an inline block resolves against the directory of
1383 // this file, which is where the brief says the test file lives.
1384 #[cfg(test)]
1385 #[path = "tests_log.rs"]
1386 mod tests_log;
```

`src/consensus/election.rs:37-39`:
```
37 #[cfg(test)]
38 #[path = "tests_election.rs"]
39 mod tests_election;
```

`src/consensus/sim.rs:1694-1700`:
```
1694 // The house style is a sibling `tests_*.rs` declared from `mod.rs`, and this deviates from it for
1695 // one reason: `mod.rs` is the shared contract and is frozen for this phase, so the only line added
1696 // there is the `pub mod sim;` without which this file is not compiled at all. Attaching the tests
1697 // here costs one attribute and no edit to a file five other agents are building against.
1698 #[cfg(test)]
1699 #[path = "tests_sim.rs"]
1700 mod tests_sim;
```
So the stated house style is "a sibling `tests_*.rs` declared from `mod.rs`" (`sim.rs:1694`), and every Phase F lane deviated to `#[cfg(test)] #[path] mod` in the impl file because `mod.rs` was frozen. `mod.rs` also documents that a child must be named there at all (`src/consensus/mod.rs:57-60`): "a module that `mod.rs` does not name is not compiled at all, and nothing outside this file can declare a child of `consensus`."

Sibling test files present in `src/consensus/`: `tests_contract.rs`, `tests_election.rs`, `tests_log.rs`, `tests_membership.rs`, `tests_node.rs`, `tests_replicate.rs`, `tests_sim.rs`, `tests_transport.rs`.

## 3. `src/tel/mod.rs` — no sibling test file registered

`src/tel/mod.rs` is 52 lines total. `grep -n 'cfg(test)|mod |#\[path'` over it yields **only** `pub mod` declarations; `grep -rn '#\[path' src/tel/` → no matches at all. There is no `tests_*.rs` file in `src/tel/` (`ls src/tel/` = capture.rs, engine.rs, frame.rs, guard.rs, ids.rs, log.rs, merge.rs, mod.rs, op.rs, schema_merge.rs). `src/tel` instead uses **inline** `#[cfg(test)] mod tests` at the bottom of each impl file: `capture.rs:253`, `engine.rs:1149`, `guard.rs:368`, `log.rs:129`, `merge.rs:227`, `op.rs:204`, `schema_merge.rs:426`.

`src/tel/mod.rs:14-40` verbatim (line numbers are the file's):
```
14
15 pub mod capture;
16 pub mod engine;
17 pub mod frame;
18 pub mod guard;
19 pub mod ids;
20 pub mod log;
21 pub mod merge;
22 pub mod op;
23 pub mod schema_merge;
24
25 pub use capture::{capture_assignment, capture_guard, to_guard_expr, ColMap, RowSnapshot};
26 pub use engine::{dedup_by_txn, ComposedState, Deduped, Side, ThreeWayMerger};
27 pub use frame::{SchemaVer, TxnFrame};
28 pub use log::MemEffectLog;
29 pub use guard::{ArithOp, CmpOp, Guard, GuardContext, GuardExpr};
30 pub use ids::{ColId, Dot, RowId, TableId, TxnId};
31 pub use merge::{
32     ColumnPolicyLookup, ConflictKind, ConflictReport, Diff, DiscardedWrite, MergeOutcome,
33     MergePolicy, Merger,
34 };
35 pub use op::{Delta, EscrowClaim, Op, OpKind};
36 pub use schema_merge::{
37     merge_schema, ColumnShapeReq, SchemaEdit, SchemaMerge, SchemaMergeOutcome, SchemaPredicate,
38 };
39
40 use crate::error::FerroError;
```
Immediately after (`src/tel/mod.rs:42-52`) is the `EffectLog` trait: `append(&self, frame: &TxnFrame) -> Result<(), FerroError>` and `frames_for(&self, branch: crate::branch::types::BranchId, from_seq: u64) -> Result<Vec<TxnFrame>, FerroError>`.

## 4. Cargo.toml — `[dependencies]` is empty; `tempfile` IS a dev-dependency

Full file, `/Users/idide/wt/ferrodb-F10-durable-tel/Cargo.toml` (10 lines, no `[[bin]]`/`[[test]]`/workspace sections):
```
1  [package]
2  name = "ferrodb"
3  version = "0.1.0"
4  edition = "2024"
5
6  [dependencies]
7
8  [dev-dependencies]
9  tempfile = "3.27.0"
10 proptest = "1.11.0"
```
`[dependencies]` (line 6) has zero entries. `[dev-dependencies]` holds exactly `tempfile = "3.27.0"` and `proptest = "1.11.0"`.

## 5. Per-target test counting

`/Users/idide/wt/ferrodb-F10-durable-tel/tools/verify-suite.sh` (174 lines) is the sanctioned instrument. Key facts:

- Header states why it is versioned in-repo (`tools/verify-suite.sh:4-8`): the old scratchpad copy under `/private/tmp` was wiped by the 2026-08-20 restart, breaking `LEDGER.md`'s Phase E and `RESUME-CHECKPOINT.md` instructions. "Anything a ledger tells the future to execute has to be versioned with the code."
- Modes: `VERIFY_MODE` = `whole` (default) or `per-target`; anything else is a refusal (`:66-70`).
- `per-target` runs `cargo test --lib --no-fail-fast` (`:98`) then `cargo test --test "$t" --no-fail-fast` for each `ls tests/*.rs` basename (`:110-112`), concatenating into one `$LOG`.
- Rationale for per-target (`:61-65`): "a single whole-suite run on this machine is not durable: with an agent fleet live it gets starved or SIGTERMed mid-suite, and a partial log's total reads as a regression (that has happened here: `1067` from a run killed inside integration_pgwire)."
- Every target must emit a `test result:` line after its own `=== target ` marker or the script refuses (`verdict_since_marker`, `:89-94`, `:104-109`, `:115-120`); the `--lib` check exists because a killed `--lib` would drop "~778 tests" silently.
- Other refusals: examples not rebuilt first (`:51-56`), tree moved during the run — HEAD or dirty-count differs (`:152-158`), zero tests collected (`:164-167`), Go suite absent/`go` missing/zero collected (`:129-150`). Go runs **after** Rust, never beside it, for the documented port race (`:124-126`).
- Env knobs: `VERIFY_OUT`, `VERIFY_TIMEOUT` (default `BOUND=7200`, `:43`).
- Final line is the gate: `[ "$f" = "0" ] && [ "$be" = "0" ] && [ "$rc" = "0" ] && [ "$gorc" = "0" ] && [ "$gf" = "0" ]` (`:174`).

Other per-target invocations found: `bench/b7_fire_checks.py:67,77,87,94,104,124,130,136,142,163,169,179,185` (e.g. `cargo test --lib replication::stream`); `scratchpad/run_suite.py:27` (`# per-target suite run, {len(targets)} targets`); `scratchpad/mutants.py:257` (`timeout 600 cargo test --lib consensus::election::tests_election::{t} -- --exact`); `scratchpad/fire-mutants.py:5`; `scratchpad/F1-election.md:171,178,189,250`; `scratchpad/F8-evidence/README.md:8,40`.

Ledger evidence lines quote this format, e.g. `LEDGER.md:658`: `E77-I21-verify-1746Z: mode=per-target rc=0 passed=1281 failed=0 build_errors=0 head=2add3d4` … "all **75/75 targets with a verdict line each** under per-target mode".

## 6. CI and `-D dead_code` — yes

`/Users/idide/wt/ferrodb-F10-durable-tel/.github/workflows/tests.yml` (the only test workflow; `release.yml` is the other file).

`.github/workflows/tests.yml:67`:
```
      RUSTFLAGS: "-D duplicate_macro_attributes -D dead_code"
```
It is job-level `env` on the `test` job, matrix `os: [ubuntu-latest, macos-latest, windows-latest]` (`:31-32`), so it applies to every cargo invocation in the job.

Surrounding rationale, `.github/workflows/tests.yml:44-66` (abridged to the load-bearing lines):
```
44      # THE DISABLED-TEST GATE — two lints, promoted to errors.
46      # On 2026-08-17 a `#[test]` in src/tel/guard.rs was found attached to the wrong function. E60
47      # had inserted two new tests between an existing test's attribute and its body, so the
48      # attribute landed on a new test that already had one and the original became an ordinary
49      # private fn. It had not run since. `cargo test` said nothing, because rustc reports both
50      # halves as WARNINGS: `duplicate_macro_attributes` on the doubled attribute and `dead_code` on
51      # the orphaned function.
...
59      # So the guard is the lint, not the count. These two are narrow and long-stable, deliberately
60      # NOT `-D warnings`: the toolchain here floats on `stable`, and a blanket deny turns any new
61      # lint in a future release into a red build on an unrelated pull request, which is how a guard
62      # gets switched off. Measured before turning it on: zero rustc warnings across all three
63      # runners on run 32006658708.
65      # The cost is real and accepted: a helper used under only one `#[cfg]` now fails the build on
66      # the other platforms rather than warning.
```

Test steps: `.github/workflows/tests.yml:304` `- run: cargo build --examples`, `:306` `- run: cargo test` (whole-suite, not per-target), `:297-299` Go tests in `cdc-consumer` (`go test ./...`), and `:321-344` the "Checkpoint gate" re-running five named targets under `FERRODB_CHECKPOINT_INTERVAL=1` with an explicit refusal if a named `tests/$t.rs` is missing (`:334-343`).

Triggers, `.github/workflows/tests.yml:12-15`: `push: branches: ["main", "agent-isolation"]`, `pull_request:`, `workflow_dispatch: {}` — with a comment at `:4-11` explaining `agent-isolation` is listed deliberately because "silence read as green".

## Related tree state (not asked, load-bearing for the above)

`/Users/idide/wt/ferrodb-F10-durable-tel/PROGRESS.md` is 15 lines and is F10's own working note, headed `# F10 — a durable Typed Effect Log`, with sections `**Done**` / `**Doing now**` / `**Next action**`. `PROGRESS.md:7-8` records the ownership constraint: "F9 holds src/agent_sql/runtime.rs, F11 holds src/cli/cli.rs and examples/pgserver.rs, both live in this wave. So the entry-point wiring is NOT mine to edit." `PROGRESS.md:15`: "Write `DurableEffectLog` into src/tel/log.rs over the `Storage` seam."

`/Users/idide/wt/artie-research/RESUME-CHECKPOINT.md:25` — "**PHASE F IS COMPLETE as of 2026-08-28 (run 6). Nothing remains. Do not re-enter it.**"; `:37` — "F6/F7/F9/F10/F11, I22 and I23 were explicit non-goals and remain untouched and OPEN in `LEDGER.md`."