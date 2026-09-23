//! D130 — is the group-commit BATCH set by the THREAD COUNT? The pgwire layer.
//!
//! ```text
//! cargo run --release --example d130_pgwire_batch -- [forks_per_thread] [T,T,T]
//! ```
//!
//! # The question, and why it needs its own harness
//!
//! `src/branch/group_commit.rs` is single-stage leader/follower commit-when-ready: the leader reads
//! `let target = st.requested` at the instant it starts its fsync, so a batch is exactly whatever
//! had taken a ticket when the leader looked. D130 pre-registers a sweep over the thread count `T`
//! at a FIXED rung and reads `f/sync ÷ T`:
//!
//!   1. `f/sync ÷ T` ≈ 0.5, FLAT in T   ⇒ a wake-up race; the batch is pinned at half the threads.
//!   2. `f/sync ÷ T` FALLS as T rises   ⇒ the batch is bounded by something that is NOT the thread
//!                                        count — arrival, or a serial section that admits only
//!                                        `D/S` forks per fsync.
//!   3. `f/sync ÷ T` ≈ 1.0 at some T    ⇒ the batch already captures everyone.
//!
//! `examples/fork_concurrency.rs` answers that at the DIRECT layer (`TableBranchCatalog::fork`,
//! holding `logical` and nothing else). This file answers it at the layer production runs, because
//! Amendment 1 to the ceiling pre-registration binds every number to its layer and E.6 measured
//! that these are **not the same experiment**: through pgwire the locks held are
//! `ServerContext::catalog()` → `AgentRuntime`'s `Mutex<State>` → `TableBranchCatalog::logical`,
//! outermost first, and the fork statement is not admitted to `try_run_read`'s shared path.
//!
//! ⚠ **BAND, 2026-09-22 (D159). The paragraph above describes the tree it was written against and
//! two of its clauses have since stopped being true. It is banded rather than repaired, so a reader
//! of the banked `bench/d130_batch_vs_threads.txt` can still see which tree that run measured.**
//!
//! * **The nesting is wrong even for the tree it was written against.** `AgentRuntime`'s
//!   `Mutex<State>` is never held while `TableBranchCatalog::logical` is taken — `begin_session_as`
//!   forks *first* (`runtime.rs:1475-1477`) and locks `state` *after* (`:1512`), so they are
//!   siblings under the catalog mutex, not nested. `runtime.rs:1095-1100` states the runtime's one
//!   lock order and it is the opposite of the chain above: the catalog lock *"must not be reached
//!   while `state` is held"*.
//! * **The fsync is no longer under `ServerContext::catalog()`.** D159 split the fork into stage +
//!   durable (`BranchCatalog::fork_staged` / `await_fork_durable`): pgwire drops the catalog guard
//!   and only then awaits the sync, so forkers can meet inside `CommitGroup::wait_durable`. The
//!   fork statement is still **not** admitted to `try_run_read`'s shared path — that clause stands —
//!   and everything except the sync still runs under the catalog guard, deliberately.
//! * ⚠ **What this does NOT license.** D159 amendment 4 predicts `f/sync` stays near 1.00 on this
//!   harness anyway, because this harness gives every fork a **unique run id** (`r<thread>_<iter>`,
//!   see `run_ids` below) and so pays `prov_store.intern`'s own fsync per fork, under `state`,
//!   inside the catalog guard. **That is a second serialiser this change does not touch.** A flat
//!   arm P here is therefore the predicted reading, not evidence that the split did nothing — the
//!   split's own evidence is `tests/d159_fork_sync_is_deferred.rs`, which is deterministic.
//!   * ⛔⛔ **CORRECTED 2026-09-23 (S5), FROM SOURCE: THE CLAUSE ABOVE IS WRONG ABOUT THE FSYNC,
//!     AND IT IS THE CLAUSE THE WHOLE "WHY IS ×17.3 INVISIBLE" QUESTION HAS BEEN RESTING ON.**
//!     `prov_store.intern` **issues no fsync in this harness, and none in `examples/pgserver.rs`
//!     either.** `AgentRuntime::with_storage` constructs
//!     `prov_store: Arc::new(MemProvenanceStore::new())`; the durable store arrives only through
//!     the builder `AgentRuntime::with_durable_provenance`, and the only caller of that builder in
//!     the whole tree outside tests is **`src/cli/cli.rs`**. Neither `rig()` below nor
//!     `examples/pgserver.rs` calls it. `MemProvenanceStore::intern` takes an in-memory
//!     `RwLock` write and touches no disk; the `file.sync_data()` in
//!     `DurableProvenanceStore::append_locked` is never reached from a pgwire fork.
//!     ⇒ What survives is weaker and worth stating exactly: `intern` is still called **with the
//!     runtime's `state` mutex held**, so it is still a serialised section on every fork — but an
//!     in-memory one, not a disk round trip, and a microsecond-scale critical section is not a
//!     plausible explanation for `f/sync` pinned at exactly 1.00.
//!     ⇒ This is why the S5 arm below is a **2×2** and not the single knob it was commissioned as.
//!     Removing a run id's uniqueness under `D130_PROV=mem` removes an in-memory insert and
//!     nothing else; the fsync this bullet describes only exists to be removed under
//!     `D130_PROV=durable`.
//!
//! # The instrument
//!
//! `TableBranchCatalog::syncs_issued()` is a counter on the server's own catalog, read from inside
//! this process before and after the client phase. `f/sync` is therefore a ratio of two counters
//! taken by one instrument over one run, which is what makes it usable on a loaded box. **No
//! timing is reported and none should be added**: the ladder's own header already labels
//! statements/sec an upper bound, each fork here is a TCP round trip, and a throughput number from
//! this harness would be a fact about the socket.
//!
//! ⛔⛔ **BAND, 2026-09-23 (S5). "USABLE ON A LOADED BOX" IS TRUE OF THIS INSTRUMENT'S ERROR AND
//! FALSE OF THE QUANTITY IT MEASURES, AND ELSEWHERE THIS FILE SAYS THE STRONGER THING OUTRIGHT —
//! "so it is load-immune", in the banner `main` prints into every output file. Both readings have
//! been believed and acted on. The paragraph above is left standing so a reader of a banked run can
//! see what that run was taken to mean.**
//!
//! The distinction the original wording misses:
//!
//! * **A count fixed by CONTROL FLOW is load-immune.** Same input, same count, every time — no
//!   clock is consulted and no scheduler decision changes it.
//! * **A count fixed by CONCURRENCY is a timing measurement wearing an integer's clothes.**
//!   `f/sync` is one of these. It is a BATCH SIZE, and `src/branch/group_commit.rs`'s leader reads
//!   `st.requested` at the instant it begins its fsync — so the batch is whatever happened to have
//!   arrived by then, which is a function of the fsync latency `D` and the arrival rate. The model
//!   `bench/d130_run.sh` names in its own WHY THE LOCK comment is `min(1/S, T/D)`, and **`D` is
//!   exactly what a loaded device moves.** That script has this right; this header did not.
//!
//! ⇒ **Slower forks produce LARGER batches, not noisier ones.** Measured, not reasoned about:
//! `bench/d130_at_head_default_arm.txt`, MODE=direct, three reps under one lock hold. Rep 2's
//! per-cell wall time ran 1.5×–2.9× the other two at every `T ≥ 4` (T=8: 18.357 s against 6.329
//! and 6.750) and it returned the LARGEST ratios at the top of the ladder — 24.0 and 28.6 at T=64
//! and T=128, against 17.5/16.6 and 18.0/16.7. One rep, one direction, with a mechanism.
//!
//! ⛔ **AND THE SUITE LOCK DOES NOT MAKE THE BOX QUIET.** It serialises *measurements*; a
//! teammate's `cargo build` contends for the same device and takes no lock at all. Rep 2 happened
//! under a held lock.
//!
//! ⇒ **What to do about it: quote an INTERVAL over reps, not a point, and never compare a ratio
//! taken under load against one taken quiet.** The qualitative readings this harness was built for
//! survive — arm P pinned at exactly 1.00 across eight rungs is a structural fact (no two forks
//! ever shared an fsync) that no amount of load manufactures — but any specific magnitude is a
//! fact about the box as much as about the code.
//!
//! # Two arms, because the connection shape is a confound
//!
//! * **P — conn per fork.** One TCP connection, one `BEGIN AGENT SESSION`, disconnect. This is the
//!   clean ratio: the only agent statement on the wire is the fork, so `syncs` cannot contain work
//!   that is not a fork. Its cost is that a thread spends time connecting rather than queueing, so
//!   fewer threads are simultaneously inside `wait_durable` than `T` suggests. **That is a reason
//!   this arm could report a small batch for an instrument reason, which is why arm S exists.**
//! * **S — persistent conn.** One connection per thread, `BEGIN AGENT SESSION` then `ABANDON`,
//!   repeatedly. A thread returns to the fork immediately, so the arrival rate is not capped by
//!   connection setup. Its cost is the mirror image: `ABANDON` retires a branch through the same
//!   catalog, so this arm's `syncs` includes syncs that are **not** forks and its `f/sync` is a
//!   LOWER bound on the fork batch. The two arms bracket the confound; neither alone does.
//!
//! No `LeaseThread`. In production the lease scan takes the same outermost mutex and would issue
//! catalog work of its own on a timer, landing syncs in a ratio that is supposed to be
//! attributable to client statements. Leaving it out can only make the batch look LARGER, which is
//! the direction that would falsify outcome 2 — the safe direction for the reading being tested.
//!
//! # What it refuses to report
//!
//! - A cell where the server named a different number of distinct branches than the clients asked
//!   for. The loop counter is this harness's bookkeeping; the branch names are the server's.
//! - A cell where the sync counter did not move. A counter wired to nothing prints a clean-looking
//!   ratio, and `f/sync` with a zero denominator is not a small batch, it is no measurement.
//! - Any SQL error from any statement. A refused `BEGIN AGENT SESSION` is a fork that did not
//!   happen, and counting the arm around it reports a mediated path as an unmediated one.
//! - A cell whose run ids do not have the shape the mode asked for: `distinct` refuses unless the
//!   count of DISTINCT ids equals the forks, `shared` refuses unless there is exactly ONE distinct
//!   id, it is the pinned one, and it was echoed back once per statement. Neither is a weakening of
//!   the other — `shared` cannot use a distinct count as its witness by construction, so it counts
//!   echoes instead, and it additionally pins the value. See `run_ids` and `RunIdMode`.
//! - A `shared` cell whose provenance store does not hold **exactly one** entity afterwards
//!   (`ProvId(1)` must resolve, `ProvId(2)` must not). Both halves, so that a probe which answered
//!   `Err` to everything could not certify a cell where `intern` was never called.
//! - A `D130_PROV=durable` cell whose `prov.log` is absent or empty. `syncs_issued()` counts the
//!   CATALOG's fsyncs and never the provenance store's, so a `durable` knob that silently did
//!   nothing would print exactly the table a working one prints. The log is the store's own
//!   statement that it was installed and appended.
//!
//! Every refusal exits non-zero, so a run that could not see its subject cannot be read as a run
//! that saw nothing wrong.
//!
//! # S5 — THE RUN-ID ARM. `D130_RUN_ID=distinct|shared`
//!
//! ⭐ **PRE-REGISTERED 2026-09-23, BEFORE A SINGLE CELL OF THIS ARM HAD BEEN RUN.** Nothing below
//! was written with a number in front of it; the smoke run that proved the knob does not disturb
//! the default arm was a 12-fork proof pass and is labelled as such in `bench/d130_run_id_arm.txt`.
//!
//! The `⚠ What this does NOT license` bullet above *names* a second serialiser and then does not
//! test it. This knob tests it — **and testing it required first discovering that the serialiser
//! as described does not exist on this path.** See the ⛔⛔ correction under that bullet: the rig's
//! runtime, and `examples/pgserver.rs`'s, both build `MemProvenanceStore`, so `intern` performs no
//! fsync here. What is true is that `intern` runs with `AgentRuntime`'s `state` mutex held
//! (`begin_session_as_staged` locks `state`, then calls `intern` under it), inside the
//! statement-wide catalog guard pgwire holds — a serialised section on every fork, but an
//! in-memory one.
//!
//! So the arm is a **2×2**, and the axes are independent:
//!
//! | | `D130_RUN_ID=distinct` | `D130_RUN_ID=shared` |
//! |---|---|---|
//! | **`D130_PROV=mem`** (default, = today, = `pgserver.rs`) | the banked run | **negative control** |
//! | **`D130_PROV=durable`** (= `src/cli/cli.rs`) | fsync per fork, PRESENT | **the actual test** |
//!
//! ⛔ **`mem × shared` is a NEGATIVE CONTROL, not the experiment.** There is no fsync under `mem`,
//! so the run-id knob removes an in-memory hashmap insert. **It must NOT move `f/sync`.** If it
//! does, the knob is doing something other than what it claims and every other cell is suspect.
//! ⭐ **`durable × shared` vs `durable × distinct` is the comparison that answers S5**, because it
//! is the only pair that differs by an fsync-under-a-global-mutex and nothing else.
//!
//! * **`distinct` (the DEFAULT, and today's behaviour, unchanged.)** Every fork carries
//!   `(a<thread>, r<thread>_<iter>)`. Both stores key on `(agent_id, run_id)`
//!   (`src/provenance/store.rs`, `let key = (run.agent_id.clone(), run.run_id.clone())`), so that
//!   key is new every time: a fresh entity under `mem`, and under `durable` a fresh entity **plus**
//!   `append_locked`'s `file.sync_data()`.
//! * **`shared`.** Every fork in every thread carries the one tuple `(a0, r0_0)` — byte for byte
//!   the statement thread 0 issues on its first iteration in `distinct` mode. The first fork of the
//!   cell interns; every fork after it takes the repeat path
//!   (`MemProvenanceStore::intern` returns `Ok(existing)` on a key hit, and
//!   `DurableProvenanceStore::intern` wraps that with
//!   `let before = self.mem.run_count(); … if self.mem.run_count() == before { return Ok(id); }`,
//!   returning **before** `append_locked`). Under `durable` that fsync is gone for all but one fork
//!   of the entire cell; under `mem` there was never one, and only the insert is gone.
//!
//! **Both halves of the key are pinned, because both halves ARE the key.** Sharing only the run id
//! would leave `(a<thread>, r0_0)` distinct per thread and so one intern per thread — 128 of them
//! at T=128, concentrated at the start of the cell, which is precisely the high-T end where the
//! reading has to be trusted. The knob is named for the run id and documented as what it is: the
//! provenance *actor tuple*.
//!
//! ## The discriminator, in the column this file actually prints
//!
//! `f/sync` is **forks ÷ syncs**. The banked pgwire run reads P = 1.00 at every T — one catalog
//! fsync per fork, no batch at all. `syncs_issued()` counts the **catalog's** fsyncs only, so
//! turning on `durable` does not add to the denominator; it adds a disk round trip *inside*
//! `state`, and can only change the ratio by changing how forks batch.
//!
//! **Read A/B/C off the `durable` row, comparing `shared` against `distinct` at the same `T`.**
//!
//!   **A.** `durable × shared`'s `f/sync` RISES above 1.00 and keeps rising with T while
//!          `durable × distinct` stays ≈1.00 ⇒ `intern`'s fsync under `state` **was** a binding
//!          serialiser. Group commit's batch was invisible to a client because no two forks could
//!          be inside `wait_durable` at once, and D159's split was correct but bottled behind a
//!          term it did not touch.
//!   **B.** BOTH `durable` arms stay ≈1.00 at every T ⇒ `intern` is **NOT** the binding term even
//!          when it fsyncs. A third serialiser bounds the pgwire fork path and this arm has
//!          excluded the named suspect **in the one configuration where the suspect exists**.
//!          That is an equally real result and is not a failed run.
//!   **C.** `durable × shared` rises but flattens at some batch `B` < T ⇒ `intern` was *a*
//!          serialiser and something else bounds the batch at `B`. Outcome 2 of the ladder at the
//!          top of this file, one layer in.
//!
//! ⛔ **And the reading that is NOT one of A/B/C: `mem × distinct` ≈ `mem × shared` ≈ `durable ×
//! distinct` ≈ 1.00 tells you nothing about `intern` at all** — it is the same flat 1.00 the
//! banked run already reports, measured three more times. Only the `durable` pair moves the
//! fsync, so only the `durable` pair can answer the question. A sweep that runs `mem` alone
//! reproduces the header's old, false premise instead of testing it.
//!
//! ⚠ **The dispatch brief for this arm phrased the prediction as "the SHARED arm's `f/sync` must
//! FALL below 1.00 as T rises".** That is outcome A with the ratio inverted — *syncs per fork*
//! falls below 1.00 exactly when *forks per sync* rises above it — and this file prints forks÷syncs,
//! so A above is written in the column that exists. Recorded rather than silently corrected,
//! because which direction was pre-registered is the entire content of a pre-registration.
//!
//! ## Why `shared` cannot be refused by the store, and what happens if that ever changes
//!
//! `RunEntity::same_actor` counts `parent_branch`, so a repeat whose parent differs is refused
//! rather than reused. Every fork here forks from trunk — `BEGIN AGENT SESSION` binds
//! `parent: current.unwrap_or(BranchId::TRUNK)` and this harness never has a session open when it
//! issues one (arm P connects fresh; arm S `ABANDON`s first) — so the tuple is constant and the
//! repeat is a lookup. If that stops being true the store returns an error, the server returns an
//! `ErrorResponse`, and `refuse_on_error` exits non-zero. **The failure mode is loud, not a quietly
//! wrong ratio**, which is the only reason it is acceptable to depend on that fact at all.
//!
//! ## What this arm is NOT
//!
//! * **Arm D cannot respond to the knob — but its rows still move, and the invariant is
//!   SYSTEMATIC, not per-cell.** The positive control calls `TableBranchCatalog::fork` directly,
//!   never builds a `RunEntity` and never reads the environment, so no code path carries the knob
//!   into it. ⚠ **An earlier draft of this very paragraph said its rows "must read the same in both
//!   modes". That is false, and the smoke pass caught it before this arm had any evidence in it.**
//!   Arm D is itself a batch race — exhibiting one is the reason it exists — so its sync count is
//!   stochastic at a fixed `T`. Measured on the default knob alone, at the smoke ladder `f=4,
//!   T=2`, 30 consecutive reps of this binary on 2026-09-23: **25 reps read 8 syncs (f/sync 1.00),
//!   4 read 7 (1.14), 1 read 6 (1.33)**. A single `shared` cell reading 7 therefore sits inside the
//!   DEFAULT arm's own distribution and is not evidence of anything.
//!   ⇒ The check is that arm D shows no **trend** across modes — reproducible across reps, moving
//!   with `T`. Reading one differing arm-D cell as a leak is reading this harness's noise as a
//!   finding, and reading arm D's agreement in a single pair of runs as proof of no leak is the
//!   same error pointed the other way.
//! * **A `shared` cell is not a database anyone would run.** One run id for every fork is a
//!   provenance store with one entity in it and attribution that answers nothing. It is a
//!   term-removal control. The only thing it licenses is the comparison against `distinct` at the
//!   same `T`, in the same process, on the same counter.

use std::collections::BTreeSet;
use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper, TableBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::{serve, ServerContext};
use ferrodb::provenance::{ProvId, ProvenanceStore};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;
const PROTOCOL_V3: i32 = 196_608;

// ---------------------------------------------------------------------------------------------
// a minimal protocol-v3 frontend
// ---------------------------------------------------------------------------------------------
//
// Hand-rolled because this repo has zero runtime dependencies and adding a driver to answer a
// counting question would be the larger change. It is a real client on a real socket: startup
// packet, simple `Query`, replies read to `ReadyForQuery`. Nothing here reaches into the server's
// internals, which is the entire point — a harness that drove `AgentRuntime` directly would be
// measuring the DIRECT layer again under a pgwire label, which is the failure E.6 exists to name.

struct Client {
    w: TcpStream,
    r: BufReader<TcpStream>,
}

/// What one round trip returned: every text field of every `DataRow`, and the first error.
struct Reply {
    texts: Vec<String>,
    error: Option<String>,
}

impl Client {
    fn connect(addr: std::net::SocketAddr) -> std::io::Result<Client> {
        let w = TcpStream::connect(addr)?;
        w.set_nodelay(true)?;
        let r = BufReader::new(w.try_clone()?);
        let mut c = Client { w, r };
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        for (k, v) in [("user", "d130"), ("database", "d130")] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        c.w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
        c.w.write_all(&body)?;
        c.w.flush()?;
        let hello = c.read_to_ready()?;
        if let Some(e) = hello.error {
            return Err(std::io::Error::other(format!("startup refused: {e}")));
        }
        Ok(c)
    }

    fn query(&mut self, sql: &str) -> std::io::Result<Reply> {
        let mut body = Vec::with_capacity(sql.len() + 1);
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        self.w.write_all(b"Q")?;
        self.w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
        self.w.write_all(&body)?;
        self.w.flush()?;
        self.read_to_ready()
    }

    /// `Terminate`. Sent explicitly so the server's connection thread returns before the counter is
    /// read, rather than being reaped by a dropped socket at an unknown moment.
    fn terminate(&mut self) -> std::io::Result<()> {
        self.w.write_all(b"X")?;
        self.w.write_all(&4i32.to_be_bytes())?;
        self.w.flush()
    }

    fn read_to_ready(&mut self) -> std::io::Result<Reply> {
        let mut out = Reply { texts: Vec::new(), error: None };
        loop {
            let mut tag = [0u8; 1];
            self.r.read_exact(&mut tag)?;
            let mut lenb = [0u8; 4];
            self.r.read_exact(&mut lenb)?;
            let len = i32::from_be_bytes(lenb);
            if len < 4 {
                return Err(std::io::Error::other(format!("backend message length {len}")));
            }
            let mut body = vec![0u8; (len - 4) as usize];
            self.r.read_exact(&mut body)?;
            match tag[0] {
                b'Z' => return Ok(out),
                b'E' => {
                    if out.error.is_none() {
                        out.error = Some(decode_error(&body));
                    }
                }
                b'D' => decode_row(&body, &mut out.texts),
                _ => {}
            }
        }
    }
}

/// `ErrorResponse` is (byte code, C string)* terminated by a zero byte; `M` is the message.
fn decode_error(body: &[u8]) -> String {
    let mut i = 0usize;
    let mut msg = String::new();
    while i < body.len() && body[i] != 0 {
        let code = body[i];
        i += 1;
        let start = i;
        while i < body.len() && body[i] != 0 {
            i += 1;
        }
        let text = String::from_utf8_lossy(&body[start..i]).to_string();
        if code == b'M' {
            msg = text;
        }
        i += 1;
    }
    msg
}

/// `DataRow` is a field count then (i32 length, bytes) per field; `-1` is NULL.
fn decode_row(body: &[u8], out: &mut Vec<String>) {
    if body.len() < 2 {
        return;
    }
    let fields = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut i = 2usize;
    for _ in 0..fields {
        if i + 4 > body.len() {
            return;
        }
        let n = i32::from_be_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
        i += 4;
        if n < 0 {
            out.push(String::new());
            continue;
        }
        let n = n as usize;
        if i + n > body.len() {
            return;
        }
        out.push(String::from_utf8_lossy(&body[i..i + n]).to_string());
        i += n;
    }
}

/// The branch identities a reply announced, as the SERVER named them.
///
/// Half of the independent witness that a fork happened. Matching `b_<digits>` over every text
/// field rather than indexing one column keeps it from silently reading the wrong field if
/// `session_started_columns()` is ever reordered — a wrong column returns no matches, which fails
/// loudly, instead of returning a plausible string.
///
/// ⛔ **A BRANCH NAME IS NOT UNIQUE ACROSS A RUN THAT ABANDONS**, which is why this is only half.
/// `TableBranchCatalog::fork` recycles a retired slot when one is free (`table_catalog.rs`, "Recycle
/// a retired slot if one is free, otherwise mint a new one"); the identity that survives is the
/// (id, generation) pair, and `b_<id>` drops the generation. Counting DISTINCT names as the fork
/// total therefore under-counts arm S by the recycling factor — it reported 2 for 8 forks at T=2,
/// which is what the proof pass refused on. The run id below is the half that stays unique.
fn branch_names(reply: &Reply) -> Vec<String> {
    reply
        .texts
        .iter()
        .filter(|s| {
            s.strip_prefix("b_").is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
        })
        .cloned()
        .collect()
}

/// The run ids a reply echoed back, as the SERVER recorded them.
///
/// A `SessionStarted` row exists only for a statement that succeeded, so every id this returns is
/// the server saying a fork happened — and unlike the branch name it cannot be deflated by slot
/// recycling, because the catalog never invents a run id.
///
/// ⚠ **Which statistic of these ids is the witness depends on [`RunIdMode`], and the parser does
/// not.** In `distinct` mode each `BEGIN AGENT SESSION` carries `r<thread>_<iteration>`, unique
/// across the whole cell, and the witness is the count of DISTINCT ids. In `shared` mode every
/// statement carries the one pinned id, so a distinct count is 1 by construction and carries no
/// information; the witness there is the number of ECHOES plus the requirement that the single
/// distinct value is the pinned one. The pinned id is deliberately spelled in the same
/// `r<digits>_<digits>` shape, so this filter is identical in both modes rather than being a second
/// parser that could drift from the first.
fn run_ids(reply: &Reply) -> Vec<String> {
    reply
        .texts
        .iter()
        .filter(|s| {
            s.strip_prefix('r').and_then(|t| t.split_once('_')).is_some_and(|(a, b)| {
                !a.is_empty()
                    && !b.is_empty()
                    && a.bytes().all(|c| c.is_ascii_digit())
                    && b.bytes().all(|c| c.is_ascii_digit())
            })
        })
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------------------------
// the server-side rig
// ---------------------------------------------------------------------------------------------

struct Rig {
    branches: Arc<TableBranchCatalog>,
    /// The runtime's own provenance store, kept for the same reason `branches` is: it is the only
    /// way to ask whether `intern` actually took its repeat path, and no SQL surface exposes that
    /// (`ferro_runs` is one row per LIVE BRANCH, not one per interned entity).
    prov: Arc<dyn ProvenanceStore>,
    addr: std::net::SocketAddr,
    /// The rig's directory. Named (not `_dir`) because the durable arm reads `prov.log` out of it
    /// to prove `D130_PROV=durable` installed a store that actually wrote something.
    dir: PathBuf,
}

/// Build one pgwire server, the shape `examples/pgserver.rs` ships, and start serving.
///
/// The `Arc<TableBranchCatalog>` is kept because `syncs_issued()` is an inherent method on the
/// concrete catalog and not on the `BranchCatalog` trait the runtime holds — the server has no SQL
/// surface that exposes the counter, so the only way to read it is to be the process that built it.
fn rig(root: &Path, tag: &str, prov: ProvMode) -> Rig {
    let dir = root.join(format!("d130-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("rig dir");
    let db = dir.join("ferro.db").to_string_lossy().into_owned();

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&db)
        .expect("open db");
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let wal = Arc::new(WalManager::new(format!("{db}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let catalog = Catalog::create(bp.clone()).unwrap();

    let branches: Arc<TableBranchCatalog> =
        Arc::new(TableBranchCatalog::default_for_database(&db, FIRST_CATALOG_PAGE_ID).unwrap());
    let base = bp.disk_manager.high_water().expect("high water") + 32_736;
    let store: Arc<ArenaPageStore> = Arc::new(
        ArenaPageStore::new(bp.clone(), branches.clone() as Arc<dyn BranchCatalog>, base)
            .expect("arena"),
    );
    // The reaper is part of the shipped shape (`pgserver.rs`): without it every `ABANDON` marks a
    // branch `Reaped` and leaks its pages. Arm S is the one that abandons, and its ratio is already
    // declared a lower bound for exactly this reason.
    let reaper = Arc::new(TwoTierReaper::new(
        branches.clone() as Arc<dyn BranchCatalog>,
        store.clone(),
    ));
    // ⚠ The `Mem` arm constructs EXACTLY what this function has always constructed — the builder
    // is applied only under `Durable`, so the default rig's shape is unchanged rather than
    // reconstructed to look unchanged.
    let mut runtime = AgentRuntime::with_storage(
        branches.clone() as Arc<dyn BranchCatalog>,
        Arc::new(MemEffectLog::new()),
        store.clone() as Arc<dyn PageStore>,
    )
    .expect("storage-backed runtime");
    if prov == ProvMode::Durable {
        runtime = runtime
            .with_durable_provenance(dir.join("prov.log"))
            .expect("durable provenance store");
    }
    let runtime = Arc::new(runtime.with_reaper(reaper as Arc<dyn Reaper>));
    let prov_store = Arc::clone(runtime.provenance());

    let ctx = Arc::new(ServerContext::new(catalog, bp, txn, runtime));
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    // Detached: the cell ends when its clients have been joined, and a server parked in
    // `incoming()` with nobody connected issues no syncs.
    std::thread::spawn(move || {
        let _ = serve(listener, ctx);
    });
    Rig { branches, prov: prov_store, addr, dir }
}

// ---------------------------------------------------------------------------------------------
// the run-id knob
// ---------------------------------------------------------------------------------------------

/// The pinned actor tuple for [`RunIdMode::Shared`].
///
/// Chosen to be exactly what thread 0 iteration 0 issues in `Distinct` mode, so the two modes are
/// the same statement at `T=1, f=1` and differ only in how the tuple varies after that.
const SHARED_AGENT: &str = "a0";
const SHARED_RUN: &str = "r0_0";

/// Which provenance store the rig's runtime is built with — the other half of the S5 2×2.
///
/// ⛔ **`Mem` is the DEFAULT because it is what this rig has always built and what
/// `examples/pgserver.rs` builds.** `AgentRuntime::with_storage` sets
/// `prov_store: Arc::new(MemProvenanceStore::new())`, and `with_durable_provenance` is a *builder*
/// that neither this file nor `pgserver.rs` has ever called — only `src/cli/cli.rs` does. So under
/// `Mem` there is **no `intern` fsync to remove**, and `D130_RUN_ID=shared` removes only an
/// in-memory insert. That combination is the arm's negative control, not its test.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ProvMode {
    /// `MemProvenanceStore` — today's rig, and `pgserver.rs`'s. `intern` is a `RwLock` write and
    /// touches no disk.
    Mem,
    /// `DurableProvenanceStore` — the store `src/cli/cli.rs` installs, whose `append_locked` ends
    /// in `file.sync_data()`. Only here does a fork pay an `intern` fsync, and only here can
    /// removing it mean anything.
    Durable,
}

impl ProvMode {
    /// `D130_PROV`, or `Mem`. Unknown values refuse, for the reason `RunIdMode::from_env` gives.
    fn from_env() -> ProvMode {
        match std::env::var("D130_PROV").ok().as_deref() {
            None | Some("") | Some("mem") => ProvMode::Mem,
            Some("durable") => ProvMode::Durable,
            Some(other) => {
                eprintln!(
                    "d130: REFUSING. D130_PROV={other:?} is neither `mem` nor `durable`. \
                     Defaulting a mode knob would bank one arm's table under the other's name."
                );
                std::process::exit(1);
            }
        }
    }

    fn tag(self) -> &'static str {
        match self {
            ProvMode::Mem => "mem",
            ProvMode::Durable => "durable",
        }
    }
}

/// Which provenance actor tuple each fork carries — the S5 arm. See the `D130_RUN_ID` section of
/// this file's header for the pre-registration; this type only implements it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RunIdMode {
    /// `(a<thread>, r<thread>_<iter>)` — a key the store has never seen, so every fork pays
    /// `intern`'s append and fsync. Today's behaviour and the default.
    Distinct,
    /// `(a0, r0_0)` for every fork of every thread, so `intern` is a lookup after the first.
    Shared,
}

impl RunIdMode {
    /// `D130_RUN_ID`, or `Distinct`.
    ///
    /// ⛔ An unrecognised value REFUSES rather than falling back to the default. A typo that
    /// silently produced the default arm would bank a `distinct` table under a `shared` label,
    /// which is the one failure this knob can cause that a reader cannot see.
    fn from_env() -> RunIdMode {
        match std::env::var("D130_RUN_ID").ok().as_deref() {
            None | Some("") | Some("distinct") => RunIdMode::Distinct,
            Some("shared") => RunIdMode::Shared,
            Some(other) => {
                eprintln!(
                    "d130: REFUSING. D130_RUN_ID={other:?} is neither `distinct` nor `shared`. \
                     Defaulting a mode knob would bank one arm's table under the other's name."
                );
                std::process::exit(1);
            }
        }
    }

    fn tag(self) -> &'static str {
        match self {
            RunIdMode::Distinct => "distinct",
            RunIdMode::Shared => "shared",
        }
    }

    /// The `(agent, run)` tuple thread `th` carries on iteration `i`.
    fn identity(self, th: usize, i: usize) -> (String, String) {
        match self {
            RunIdMode::Distinct => (format!("a{th}"), format!("r{th}_{i}")),
            RunIdMode::Shared => (SHARED_AGENT.to_string(), SHARED_RUN.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// arms
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    /// One connection per fork; a single `BEGIN AGENT SESSION` on each, then `Terminate`.
    PerFork,
    /// One connection per thread; `BEGIN AGENT SESSION` then `ABANDON`, repeatedly.
    Persistent,
}

impl Arm {
    fn tag(self) -> &'static str {
        match self {
            Arm::PerFork => "P",
            Arm::Persistent => "S",
        }
    }
    fn label(self) -> &'static str {
        match self {
            Arm::PerFork => "P  conn per fork, BEGIN only",
            Arm::Persistent => "S  persistent conn, BEGIN+ABANDON",
        }
    }
}

/// ⭐ THE POSITIVE CONTROL. `T` threads calling `fork` on the rig's OWN catalog, no socket.
///
/// **Without this the pgwire result is an unfalsified negative.** Arms P and S both report a batch
/// of 1.00 and 0.20, flat in `T`; read alone, that is indistinguishable from a harness that could
/// never have reported anything else — a counter read at the wrong moment, a thread pool that
/// serialises the clients, a rig whose catalog is not the one the server uses. This arm reads the
/// SAME `syncs_issued()` on the SAME `Arc<TableBranchCatalog>` the running server was built from,
/// in the same process, and differs only in that it does not go through pgwire. If it shows a
/// batch while P and S do not, the instrument can see batching and the wire layer has none.
///
/// A pgwire server IS listening throughout. It contributes nothing because nobody connects to it,
/// which is a fact this arm demonstrates rather than assumes.
fn run_direct_control(t: usize, f: usize, root: &Path, prov: ProvMode) -> Cell {
    let rig = rig(root, &format!("D-{}-t{t}", prov.tag()), prov);
    let before = rig.branches.syncs_issued();

    let lease = LeaseDeadline(u64::MAX);
    let mut ids: BTreeSet<u64> = BTreeSet::new();
    std::thread::scope(|s| {
        let mut hs = Vec::with_capacity(t);
        for _ in 0..t {
            let cat = Arc::clone(&rig.branches);
            hs.push(s.spawn(move || {
                let mut mine = Vec::with_capacity(f);
                for _ in 0..f {
                    mine.push(cat.fork(BranchId::TRUNK, lease).expect("fork").branch_id.id);
                }
                mine
            }));
        }
        for h in hs {
            ids.extend(h.join().expect("control thread"));
        }
    });

    let after = rig.branches.syncs_issued();
    // Nothing is abandoned here either, so no slot is retired and every fork must mint a fresh id.
    if ids.len() != t * f {
        eprintln!(
            "d130: REFUSING. D  direct control at T={t}: asked for {} forks, the catalog minted {} \
             distinct branch ids.",
            t * f,
            ids.len()
        );
        std::process::exit(1);
    }
    if after <= before {
        eprintln!(
            "d130: REFUSING. D  direct control at T={t}: the catalog issued 0 fsyncs across {} \
             forks.",
            t * f
        );
        std::process::exit(1);
    }
    Cell { forks: ids.len(), syncs: after - before, prov_bytes: None }
}

struct Cell {
    forks: usize,
    syncs: u64,
    /// Bytes in the rig's `prov.log`, under `D130_PROV=durable` only. `None` under `mem`, where
    /// no such file exists, and for arm D, which never goes through the runtime.
    prov_bytes: Option<u64>,
}

fn refuse_on_error(r: &Reply, what: &str) {
    if let Some(e) = &r.error {
        eprintln!(
            "d130: REFUSING. `{what}` was rejected by the server: {e}\n     A refused statement is \
             work that did not happen; counting the arm around it would report a batch this run \
             never formed."
        );
        std::process::exit(1);
    }
}

/// Run one cell: `t` client threads, `f` forks each, on a freshly built server.
fn run_cell(arm: Arm, mode: RunIdMode, prov: ProvMode, t: usize, f: usize, root: &Path) -> Cell {
    let rig = rig(root, &format!("{}-{}-{}-t{t}", arm.tag(), mode.tag(), prov.tag()), prov);

    // Snapshot AFTER the rig is built. `Catalog::create`, the arena and the runtime's construction
    // are fixture cost; charging their syncs to the clients would inflate `f/sync`'s denominator
    // and push the answer toward outcome 2 for a reason that has nothing to do with batching.
    let before = rig.branches.syncs_issued();

    let mut reported: BTreeSet<String> = BTreeSet::new();
    let mut runs: BTreeSet<String> = BTreeSet::new();
    // Echoes, not distinct values. In `Shared` mode the distinct set collapses to one entry by
    // construction, so the count that still carries information is how many times the server sent
    // a run id back — once per `SessionStarted`, i.e. once per fork that actually happened.
    let mut run_hits = 0usize;
    let mut named = 0usize;
    let mut asked = 0usize;
    let addr = rig.addr;
    std::thread::scope(|s| {
        let mut hs = Vec::with_capacity(t);
        for th in 0..t {
            hs.push(s.spawn(move || {
                let mut names: Vec<String> = Vec::new();
                let mut mine_runs: Vec<String> = Vec::new();
                let mut named = 0usize;
                let mut asked = 0usize;
                let mut persistent = match arm {
                    Arm::Persistent => Some(Client::connect(addr).expect("connect")),
                    Arm::PerFork => None,
                };
                for i in 0..f {
                    let (agent, run) = mode.identity(th, i);
                    let sql = format!("BEGIN AGENT SESSION AS '{agent}' RUN '{run}';");
                    match arm {
                        Arm::PerFork => {
                            let mut c = Client::connect(addr).expect("connect");
                            let r = c.query(&sql).expect("BEGIN AGENT SESSION round trip");
                            refuse_on_error(&r, "BEGIN AGENT SESSION");
                            let b = branch_names(&r);
                            named += b.len();
                            names.extend(b);
                            mine_runs.extend(run_ids(&r));
                            asked += 1;
                            let _ = c.terminate();
                        }
                        Arm::Persistent => {
                            let c = persistent.as_mut().expect("persistent client");
                            let r = c.query(&sql).expect("BEGIN AGENT SESSION round trip");
                            refuse_on_error(&r, "BEGIN AGENT SESSION");
                            let b = branch_names(&r);
                            named += b.len();
                            names.extend(b);
                            mine_runs.extend(run_ids(&r));
                            asked += 1;
                            let r = c.query("ABANDON;").expect("ABANDON round trip");
                            refuse_on_error(&r, "ABANDON");
                        }
                    }
                }
                if let Some(mut c) = persistent {
                    let _ = c.terminate();
                }
                (names, mine_runs, named, asked)
            }));
        }
        for h in hs {
            let (n, r, nm, a) = h.join().expect("client thread");
            reported.extend(n);
            run_hits += r.len();
            runs.extend(r);
            named += nm;
            asked += a;
        }
    });

    let after = rig.branches.syncs_issued();

    // THE WITNESS. `asked` is this harness's own loop counter and is not evidence of anything;
    // these three are the server's.
    //
    // (a) every reply carried exactly one branch name, so every statement minted a branch;
    // (b) the distinct run ids equal the statements issued, so no statement was silently skipped
    //     or double-counted — and a run id, unlike a branch name, cannot be deflated by slot
    //     recycling;
    // (c) for arm P only, the distinct branch names also equal the forks. Nothing is abandoned in
    //     that arm, so no slot is ever retired and none can be recycled; the stronger check is
    //     therefore valid there and is kept. It is NOT valid for arm S, and was the proof pass's
    //     refusal.
    if named != asked {
        eprintln!(
            "d130: REFUSING. {} at T={t}: {asked} statements returned {named} branch names. A \
             `SessionStarted` row without a branch is not a fork.",
            arm.label()
        );
        std::process::exit(1);
    }
    // ⚠ The run-id witness is the ONE check the S5 knob changes, and it is split rather than
    // loosened: `Shared` cannot use a distinct count, because pinning the tuple is the whole point
    // of the arm and would make that count 1 at every T. It pays for the statistic it gives up by
    // ALSO pinning the value — `Distinct` never checks WHAT the ids were, only how many were
    // distinct, so the shared branch below is the strictly stronger of the two, not a weakening.
    match mode {
        RunIdMode::Distinct => {
            if runs.len() != asked {
                eprintln!(
                    "d130: REFUSING. {} at T={t}: asked for {asked} forks but the server echoed {} \
                     distinct run ids. The fork total cannot be taken from this harness's own \
                     counter when the two witnesses disagree.",
                    arm.label(),
                    runs.len()
                );
                std::process::exit(1);
            }
        }
        RunIdMode::Shared => {
            if runs.len() != 1 || runs.iter().next().map(String::as_str) != Some(SHARED_RUN) {
                eprintln!(
                    "d130: REFUSING. {} at T={t}: D130_RUN_ID=shared pins every fork to run id \
                     `{SHARED_RUN}`, but the server echoed {} distinct ids: {:?}. If the ids are \
                     not the pinned one, `prov_store.intern` was not taking the repeat path and \
                     this arm removed nothing.",
                    arm.label(),
                    runs.len(),
                    runs
                );
                std::process::exit(1);
            }
            if run_hits != asked {
                eprintln!(
                    "d130: REFUSING. {} at T={t}: asked for {asked} forks but the server echoed a \
                     run id {run_hits} times. In shared mode the echo count is the witness that a \
                     statement ran, and it disagrees with this harness's own counter.",
                    arm.label()
                );
                std::process::exit(1);
            }
        }
    }
    if arm == Arm::PerFork && reported.len() != asked {
        eprintln!(
            "d130: REFUSING. {} at T={t}: {asked} forks but only {} distinct branch names. Nothing \
             is abandoned in this arm, so no retired slot exists to recycle and every fork must \
             have minted a fresh id.",
            arm.label(),
            reported.len()
        );
        std::process::exit(1);
    }
    if after <= before {
        eprintln!(
            "d130: REFUSING. {} at T={t}: the catalog issued {} fsyncs across {asked} forks. A \
             denominator of zero is not a large batch, it is an instrument that did not move.",
            arm.label(),
            after - before
        );
        std::process::exit(1);
    }

    // ⭐ DID `D130_PROV=durable` ACTUALLY INSTALL THE DURABLE STORE?
    //
    // Without this the provenance axis is unfalsifiable. `syncs_issued()` counts the CATALOG's
    // fsyncs and never the provenance store's, so a `durable` knob that silently did nothing would
    // print character for character the table a working one prints — the exact shape of a knob
    // that looks like it is being tested and is not.
    //
    // `DurableProvenanceStore::open` creates the log, and every NEW entity appends a frame ending
    // in `file.sync_data()`. A non-empty file is therefore the store itself saying both that it is
    // installed and that it did the work whose removal this arm measures. Under `mem` there is no
    // file and nothing is claimed.
    let prov_bytes = if prov == ProvMode::Durable {
        let log = rig.dir.join("prov.log");
        match std::fs::metadata(&log) {
            Ok(m) if m.len() > 0 => Some(m.len()),
            Ok(_) => {
                eprintln!(
                    "d130: REFUSING. {} at T={t}: D130_PROV=durable, but {} is EMPTY after {asked} \
                     forks. The durable store appends a frame per new run entity, so an empty log \
                     means nothing was interned through it and this arm removed nothing.",
                    arm.label(),
                    log.display()
                );
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!(
                    "d130: REFUSING. {} at T={t}: D130_PROV=durable, but {} does not exist ({e}). \
                     The runtime is still on MemProvenanceStore, so the knob did nothing and every \
                     `durable` cell in this run is mislabelled.",
                    arm.label(),
                    log.display()
                );
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    // ⭐ DID THE ARM ACTUALLY ENGAGE? The mechanism check, and the reason outcome B is readable.
    //
    // `shared` is worth running only if `intern` really took its repeat path. If it did, the
    // store holds exactly ONE entity for the whole cell — nothing else in a cell interns, and
    // `ABANDON` does not — so `ProvId(1)` resolves and `ProvId(2)` does not.
    //
    // ⛔ BOTH halves are asserted, and that is what stops this being a guard that cannot fail. A
    // probe answering `Err` to everything would satisfy "`ProvId(2)` is absent" on its own and
    // certify a cell in which `intern` was never called at all. Requiring `ProvId(1)` to RESOLVE
    // forces the probe to demonstrate it can see an entity before it is believed about the absence
    // of a second one.
    //
    // There is no SQL surface for this: `ferro_runs` is one row per LIVE BRANCH, not one per
    // interned entity, so in arm P it counts the forks in either mode and answers a different
    // question. Reading the store the server was built with is the same move `syncs_issued()`
    // already makes, for the same reason.
    if mode == RunIdMode::Shared {
        if rig.prov.lookup(ProvId(1)).is_err() {
            eprintln!(
                "d130: REFUSING. {} at T={t}: D130_RUN_ID=shared, but the provenance store holds \
                 no entity at ProvId(1) after {asked} forks. `intern` was never reached, so this \
                 cell removed nothing and the probe below would pass vacuously.",
                arm.label()
            );
            std::process::exit(1);
        }
        if rig.prov.lookup(ProvId(2)).is_ok() {
            eprintln!(
                "d130: REFUSING. {} at T={t}: D130_RUN_ID=shared pins one actor tuple, but the \
                 store holds a SECOND entity at ProvId(2) after {asked} forks. `intern` minted a \
                 new run instead of taking its repeat path, so this arm did not remove the term it \
                 claims to remove.",
                arm.label()
            );
            std::process::exit(1);
        }
    }

    // The run ids, not `reported.len()`: they are the witness that survives slot recycling, and in
    // arm S the distinct branch names are FEWER than the forks by construction. Which *statistic*
    // of them is the count differs by mode for the reason given at the refusal above — and in
    // `Distinct` the two are provably the same number, since `runs.len() == asked` was just
    // enforced and `run_hits >= runs.len()`. The match is kept anyway so the default arm's fork
    // total comes from the expression it has always come from.
    let forks = match mode {
        RunIdMode::Distinct => runs.len(),
        RunIdMode::Shared => run_hits,
    };
    Cell { forks, syncs: after - before, prov_bytes }
}

// ---------------------------------------------------------------------------------------------
// forcing the instrument to fire, and to stay silent
// ---------------------------------------------------------------------------------------------

/// The counter must move when a fork happens and must NOT move when nothing does.
///
/// Without the first half the whole table is unfalsifiable: a counter wired to nothing prints the
/// same clean ratio a real one would. Without the second half a counter that ticks on something
/// else — a background flush, a timer — would inflate every denominator and manufacture outcome 2.
fn self_check(root: &Path, prov: ProvMode) {
    let rig = rig(root, "selfcheck", prov);

    let quiet_a = rig.branches.syncs_issued();
    let quiet_b = rig.branches.syncs_issued();
    if quiet_b != quiet_a {
        eprintln!(
            "d130: REFUSING. the sync counter moved by {} with no work between two reads. \
             Something other than a commit is ticking it, so every denominator below is inflated.",
            quiet_b - quiet_a
        );
        std::process::exit(1);
    }

    let before = rig.branches.syncs_issued();
    rig.branches
        .fork(BranchId::TRUNK, LeaseDeadline(u64::MAX))
        .expect("self-check fork");
    let after = rig.branches.syncs_issued();
    if after != before + 1 {
        eprintln!(
            "d130: REFUSING. one serial fork moved the sync counter by {}, not 1. The instrument \
             does not count what it claims to count, so no cell below means anything.",
            after - before
        );
        std::process::exit(1);
    }
    println!(
        "instrument self-check: two idle reads moved the counter by 0, and one serial fork moved \
         it by exactly 1. OK"
    );
}

// ---------------------------------------------------------------------------------------------

/// ⛔ REFUSE TO RUN INSIDE SOMEONE ELSE'S MEASUREMENT.
///
/// **Added 2026-09-23 (S5) because the author of this function ran this harness during another
/// lane's suite, having thought about it first.** The reasoning was: twelve forks, two seconds,
/// against a suite already running beside three `cargo` builds. The reasoning was sound and the
/// call was still wrong, for a structural reason worth writing into the code rather than a comment:
/// **the lock exists precisely to take that judgement away from the person who wants the box.**
/// Everyone's own contribution looks negligible — that is the mechanism by which contention
/// accumulates. A check you have to remember is a check you will eventually skip, and that one was
/// skipped knowingly, which is the harder version to guard against.
///
/// So this is a refusal, not a warning, and it has no override flag. If you genuinely need the box
/// while a suite holds it, take the lock — `bench/d130_run.sh` does, and running under it is the
/// supported path. A `D130_IGNORE_SUITE_LOCK=1` would just reinstate the judgement call this
/// exists to remove.
///
/// **How it avoids refusing its own runner.** `bench/d130_run.sh` acquires the lock, writes its
/// `$$` into `<lock>/owner`, and exports `D130_SUITE_LOCK_OWNER=$$`. This function proceeds only
/// when that variable matches the pid actually recorded in the lock file — so it is grounded in
/// observable state, not in a caller's say-so. Claiming to hold the lock requires having written
/// the lock.
///
/// **It refuses when it cannot parse its own input.** A lock directory that exists with an
/// unreadable or malformed `owner` is refused, never waved through: a guard that falls back to
/// "allow" when confused is not a guard. The one case that proceeds is a genuinely STALE lock —
/// the recorded holder is gone — which is the same rule, and the same reasoning,
/// `bench/d130_run.sh` uses when it breaks a dead holder's lock.
fn refuse_if_another_suite_holds_the_lock() {
    let lock = std::env::var("SUITE_LOCK").unwrap_or_else(|_| "/tmp/ferrodb-suite.lock".to_string());
    let dir = Path::new(&lock);
    if !dir.exists() {
        return; // Nobody is measuring. Nothing to refuse.
    }
    let owner_path = dir.join("owner");
    let owner = match std::fs::read_to_string(&owner_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "d130: REFUSING. {lock} exists but its `owner` could not be read ({e}). A suite \
                 lock this harness cannot parse is one it must not run through: falling back to \
                 `allow` here would make the guard decorative."
            );
            std::process::exit(1);
        }
    };
    let holder = owner.split_whitespace().next().unwrap_or("");
    let holder_pid: i32 = match holder.parse() {
        Ok(p) => p,
        Err(_) => {
            eprintln!(
                "d130: REFUSING. {lock} is held but its `owner` does not start with a pid (it \
                 reads {owner:?}). See above: unparseable means refuse, not proceed."
            );
            std::process::exit(1);
        }
    };
    // `kill -0`, not `ps`: `ps -e`/`-a`/`-x` override `-p` and will happily print a plausible line
    // for the WRONG process, which is how a dead holder gets read as alive.
    let alive = std::process::Command::new("kill")
        .args(["-0", &holder_pid.to_string()])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !alive {
        eprintln!(
            "d130: the suite lock at {lock} records pid {holder_pid}, which is gone. Treating it \
             as stale and proceeding — the same rule bench/d130_run.sh applies."
        );
        return;
    }
    if std::env::var("D130_SUITE_LOCK_OWNER").ok().as_deref() == Some(holder) {
        return; // Our own runner holds it; this is the supported path.
    }
    eprintln!(
        "d130: REFUSING. the machine-wide suite lock {lock} is held by a LIVE pid {holder_pid} \
         ({}). Running now would put this harness's fsyncs inside someone else's measurement.\n\
         \n     There is no override, deliberately. `f/sync` is a BATCH SIZE and therefore a \
         function of the fsync latency a loaded device moves, so contention does not merely add \
         noise to a d130 number — it INFLATES it (see this file's header band). Two seconds of \
         'negligible' work is how that accumulates.\n\
         \n     Run it under `bench/d130_run.sh`, which acquires the lock and exports \
         D130_SUITE_LOCK_OWNER, or wait for the holder to finish.",
        owner.trim()
    );
    std::process::exit(1);
}

fn main() {
    refuse_if_another_suite_holds_the_lock();
    let f: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(40);
    let threads: Vec<usize> = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "1,2,4,8,16,32,64,128".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .filter(|&t: &usize| t > 0)
        .collect();
    if threads.is_empty() || f == 0 {
        eprintln!("d130: REFUSING. an empty thread list or zero forks per thread collects nothing.");
        std::process::exit(1);
    }
    let mode = RunIdMode::from_env();
    let prov = ProvMode::from_env();

    let root = std::env::temp_dir().join(format!("ferrodb-d130-{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("root");

    println!("D130 — IS THE GROUP-COMMIT BATCH SET BY THE THREAD COUNT? MODE=pgwire.");
    println!();
    println!("⚠ LAYER: the shipped pgwire server over a real TCP socket, one client connection per");
    println!("  thread or per fork, forks issued as `BEGIN AGENT SESSION`. Locks held, outermost");
    println!("  first: `ServerContext::catalog()` -> `AgentRuntime` state -> `TableBranchCatalog`'s");
    println!("  `logical`. E.6 measured that this is NOT the same experiment as MODE=direct, which");
    println!("  is `examples/fork_concurrency.rs`. Amendment 1 binds each number to its layer.");
    println!();
    println!("  f/sync   = forks the server named, divided by fsyncs the catalog issued. A ratio of");
    println!("             two counters taken by one instrument over one run, so it needs no clock.");
    println!("             ⛔ BUT IT IS NOT LOAD-IMMUNE, AND THIS LINE USED TO SAY IT WAS. f/sync is");
    println!("             a BATCH SIZE, so it is a function of the fsync latency D that a loaded");
    println!("             device moves: min(1/S, T/D). SLOWER FORKS GIVE LARGER BATCHES — measured,");
    println!("             bench/d130_at_head_default_arm.txt, where the one rep running 1.5-2.9x");
    println!("             slower returned the LARGEST ratios (28.6 vs 18.0/16.7 at T=128). The");
    println!("             suite lock does not help: a teammate's cargo build takes no lock. Quote");
    println!("             an INTERVAL over reps, never a point. See this file's header band.");
    println!("  f/sync÷T = the pre-registered discriminator. ≈0.5 flat ⇒ outcome 1 (wake-up race);");
    println!("             falling in T ⇒ outcome 2 (the batch is not bounded by the threads);");
    println!("             ≈1.0 ⇒ outcome 3 (the batch already captures everyone).");
    println!();
    println!("⛔ NO THROUGHPUT IS REPORTED. Each fork is a TCP round trip and the box is shared;");
    println!("   a rate from this harness would be a fact about the socket. The counts are the");
    println!("   evidence.");
    println!();
    match mode {
        RunIdMode::Distinct => {
            println!("RUN-ID MODE: distinct  (D130_RUN_ID unset or `distinct` — the default arm)");
            println!("  Every fork carries `(a<thread>, r<thread>_<iter>)`, a key the provenance");
            println!("  store has never seen, so `intern` mints a NEW entity on every fork, while");
            println!("  AgentRuntime's `state` mutex is held. Whether minting one also costs an");
            println!("  FSYNC depends entirely on WHICH STORE — see PROV STORE below; under the");
            println!("  default `mem` it does not. D130_RUN_ID=shared removes the mint.");
        }
        RunIdMode::Shared => {
            println!("RUN-ID MODE: shared  (D130_RUN_ID=shared — THE S5 TERM-REMOVAL ARM)");
            println!("  Every fork in every thread carries the one tuple `({SHARED_AGENT}, {SHARED_RUN})`, so");
            println!("  `prov_store.intern` takes its repeat path after the first fork of the cell");
            println!("  and its fsync under `state` is GONE. ⛔ Not a configuration anyone would");
            println!("  run: one run id for the whole cell is attribution that answers nothing.");
            println!("  These rows mean only what they mean against `distinct` at the same T.");
            println!("  Arm D builds no RunEntity and cannot respond to the knob — but it IS a batch");
            println!("  race, so one differing D cell is this harness's noise, not a leak. Only a");
            println!("  reproducible TREND across modes invalidates a run; header has the spread.");
        }
    }
    match prov {
        ProvMode::Mem => {
            println!("PROV STORE: mem  (D130_PROV unset or `mem` — MemProvenanceStore, the default)");
            println!("  ⛔ `intern` ISSUES NO FSYNC IN THIS ARM. AgentRuntime::with_storage builds");
            println!("  MemProvenanceStore; only `with_durable_provenance` installs the store whose");
            println!("  append_locked calls file.sync_data(), and neither this rig nor");
            println!("  examples/pgserver.rs calls it — only src/cli/cli.rs does. So `shared` here");
            println!("  removes an in-memory insert: this is the NEGATIVE CONTROL and must not move");
            println!("  f/sync. Run D130_PROV=durable for the arm where the fsync exists to remove.");
        }
        ProvMode::Durable => {
            println!("PROV STORE: durable  (D130_PROV=durable — DurableProvenanceStore)");
            println!("  The store src/cli/cli.rs installs. Every NEW (agent, run) costs an append");
            println!("  ending in file.sync_data(), taken while AgentRuntime's `state` mutex is");
            println!("  held. ⭐ THIS is the arm where D130_RUN_ID actually removes an fsync, and");
            println!("  the durable distinct/shared pair is the comparison that answers S5.");
            println!("  ⚠ NOT the shape examples/pgserver.rs ships — see the header.");
        }
    }
    println!("  The arm is pre-registered in this file's header, written before any cell was run.");
    println!();
    println!("# start: {}  load: {}", stamp(), loadavg());
    println!();

    self_check(&root, prov);
    println!();

    // The mode is repeated on the column header, not just in the banner above, so a row lifted out
    // of this table into a note still carries the arm it came from. It is header text: no cell,
    // no count and no ratio below depends on it.
    println!(
        "  arm                                threads    forks    syncs    f/sync   f/sync÷T   \
         [run-id: {}  prov: {}]",
        mode.tag(),
        prov.tag()
    );
    let row = |label: &str, t: usize, c: &Cell| {
        let per = c.forks as f64 / c.syncs as f64;
        // The trailing `prov.log` field appears only under D130_PROV=durable, so the default
        // table's rows are byte-identical to what they have always been. It is the durable
        // store's own appended bytes: ~one Run frame per NEW actor tuple, so a `shared` cell's
        // log is a single frame where the matching `distinct` cell's holds one per fork. That
        // contrast is the mechanism visible in the output rather than argued in a comment.
        let tail = match c.prov_bytes {
            Some(b) => format!("   prov.log={b}B"),
            None => String::new(),
        };
        println!(
            "  {:32}  {:5}   {:6}   {:6}   {:7.2}   {:8.4}{}",
            label, t, c.forks, c.syncs, per, per / t as f64, tail
        );
    };
    for arm in [Arm::PerFork, Arm::Persistent] {
        for &t in &threads {
            row(arm.label(), t, &run_cell(arm, mode, prov, t, f, &root));
        }
        println!();
    }
    for &t in &threads {
        row("D  POSITIVE CONTROL, no socket", t, &run_direct_control(t, f, &root, prov));
    }
    println!();
    println!("⭐ ARM D IS THE FALSIFIER FOR ARMS P AND S. Same counter, same Arc<TableBranchCatalog>,");
    println!("   same process, a pgwire server listening throughout — it simply does not go through");
    println!("   the socket. If D shows a batch and P/S do not, the instrument can see batching and");
    println!("   the wire layer has none. If D is ALSO flat at 1.00, then nothing above is a fact");
    println!("   about pgwire and every cell in this run is a fact about this harness.");

    println!("# run-id mode: {}  prov store: {}", mode.tag(), prov.tag());
    println!("# end: {}  load: {}", stamp(), loadavg());
    let _ = std::fs::remove_dir_all(&root);
}

fn stamp() -> String {
    std::process::Command::new("date")
        .arg("+%Y-%m-%dT%H:%M:%SZ")
        .env("TZ", "UTC")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

/// The box is shared, and this number is load-BEARING rather than decoration.
///
/// ⛔ Corrected 2026-09-23 (S5): this doc used to say `f/sync` is "load-immune by construction, but
/// a reader deserves to see what the machine was doing rather than take that on trust." The second
/// clause was right for the wrong reason. `f/sync` is a batch size and therefore a function of
/// fsync latency, so the load a run saw is part of what its ratios MEAN, not a courtesy for a
/// sceptical reader. See the band in this file's header.
fn loadavg() -> String {
    std::process::Command::new("uptime")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.split("load averages:").nth(1).map(|t| t.trim().to_string()))
        .unwrap_or_else(|| "unknown".into())
}
