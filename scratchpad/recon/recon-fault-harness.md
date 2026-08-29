# The crash-fault harness

All paths relative to `/Users/idide/wt/ferrodb-F10-durable-tel`.

Note on the brief: the fault-sweep tests are in `src/consensus/tests_log.rs` (confirmed, `use crate::storage::sim::{...}` at `src/consensus/tests_log.rs:15`), and a second, larger body of usage is in `tests/sim_durability.rs`. `src/storage/sim.rs` is **not** `#[cfg(test)]`-gated — `pub mod sim;` at `src/storage/mod.rs:14` with no attribute — so it is reachable from both unit tests and integration tests.

---

## 1. `SimFabric` — full public surface

`pub struct SimFabric { plan: Option<FaultPlan>, durability: Durability, state: Mutex<FabricState> }` — `src/storage/sim.rs:286-290`.

Constructors (all return `Arc<Self>`):

| signature | line |
|---|---|
| `pub fn clean(durability: Durability) -> Arc<Self>` | `src/storage/sim.rs:315` |
| `pub fn with_fault(plan: FaultPlan, durability: Durability) -> Arc<Self>` | `src/storage/sim.rs:319` |
| `pub fn from_images(images: BTreeMap<String, Vec<u8>>, plan: Option<FaultPlan>, durability: Durability) -> Arc<Self>` | `src/storage/sim.rs:342-346` |

Methods:

| signature | line | what |
|---|---|---|
| `pub fn set_write_atomicity(&self, name: &str, unit: u64)` | `:379` | see §3 |
| `pub fn set_verified_from(&self, name: &str, offset: u64)` | `:395` | region of `name` at/above which a write may be *garbled* rather than merely lost; below it `Corrupt` degrades to a drop |
| `pub fn open(self: &Arc<Self>, name: &str) -> Arc<SimStorage>` | `:401` | file handle; **does not** advance the op counter (`:399-400`) |
| `pub fn op_count(&self) -> u64` | `:408` | next op index; used as a `mark` to window a sweep |
| `pub fn trace(&self) -> Vec<TraceOp>` | `:412` | |
| `pub fn faultable_ops(&self) -> Vec<u64>` | `:418` | indices a fault could break, in order |
| `pub fn trace_digest(&self) -> u32` | `:429` | "were the same writes issued" |
| `pub fn image_digest(&self) -> u32` | `:436` | "did the same bytes end up on disk" (over `durable`) |
| `pub fn fired(&self) -> Option<FiredFault>` | `:447` | |
| `pub fn crashed(&self) -> bool` | `:451` | |
| `pub fn durable_image(&self) -> BTreeMap<String, Vec<u8>>` | `:456` | bytes that survived |
| `pub fn restart(&self) -> Arc<SimFabric>` | `:466` | fresh fault-free fabric over the survivors, **carrying `atomic_unit` and `verified_from` forward** (`:468-476`) |

`SimStorage` (`:704-717`): `pub fn fabric(&self) -> &Arc<SimFabric>`, `pub fn name(&self) -> &str`; `impl Storage for SimStorage` at `:719-743` (`pwrite`/`pread`/`sync_all`/`sync_data`/`set_len`/`len`, each one `execute` call).

**Getting an `Arc<dyn Storage>` for file A and file B** — `fabric.open(name)` returns `Arc<SimStorage>`, which coerces at the call site because `SimStorage: Storage`. The real idiom, `src/consensus/tests_log.rs:18-23`:

```rust
const A: &str = "rounds.a";
const B: &str = "rounds.b";

fn open_on(fabric: &std::sync::Arc<SimFabric>) -> Result<RoundLog, LogError> {
    RoundLog::with_storage(fabric.open(A), fabric.open(B))
}
```
against `pub fn with_storage(a: Arc<dyn Storage>, b: Arc<dyn Storage>) -> Result<RoundLog, LogError>` at `src/consensus/log.rs:404`.

**How many files does one fabric hold?** Unbounded — `files: BTreeMap<String, FileImage>` at `src/storage/sim.rs:268`, keyed by name, entries created lazily by `open()` (`:403`, `or_insert_with(|| FileImage::new(Vec::new()))`) or seeded wholesale by `from_images` (`:350-352`). One fabric is one *machine*, which is the point: the op counter is global across every file (`:282-285`), so "operation index N" means the same thing on file A and file B. Existing users hold exactly two: `rounds.a`/`rounds.b` (`src/consensus/tests_log.rs:18-19`) and `sim.db`/`sim.wal` (`tests/sim_durability.rs:52-53`).

---

## 2. `FaultPlan` / `FaultKind` / `WriteShape` / `Durability`

```rust
pub enum Durability { WriteThrough, SyncOnly }          // src/storage/sim.rs:73-86
pub enum FaultKind { TearWrite, DropWrite, CorruptWrite, FailSync, DropSetLen } // :91-109
pub enum WriteShape { Drop, Tear, Corrupt }             // :114-122
```
`Durability::WriteThrough` (`:74-79`): every `Ok` write is durable at once, a sync is a no-op that can still fail. `SyncOnly` (`:80-85`): writes land in `visible` only; a sync copies `visible` into `durable` (`:626-631`); a crash loses the rest. Applied in `write_into` (`:688-700`) and `Req::SetLen` (`:632-638`).

```rust
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FaultPlan {          // src/storage/sim.rs:126-136
    pub seed: u64,
    pub at_op: u64,             // fires at the first *faultable* op with index >= this
    pub shape: WriteShape,      // seed-derived
    pub tear_pick: u64,         // seed-derived: tear boundary / garbled-byte position
}
```

Derivation (all three are constructors on `FaultPlan`, `:144-170`):

- `pub fn from_seed(seed: u64, op_count: u64) -> FaultPlan` — `:144`. Everything including `at_op` comes from the seed: `Rng::new(seed)`, `at_op = rng.below(op_count.max(1))`, then `shape`, then `tear_pick`. `op_count` must come from a **fault-free census run of the same workload** (`:141-142`).
- `pub fn at(at_op: u64, seed: u64) -> FaultPlan` — `:158`. Caller picks the op; seed picks the shape, but `at_op` is *mixed into the stream*: `Rng::new(seed ^ at_op.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x5DEE_CE66_D0D1_6E01)` (`:159`). The doc records the measurement that forced this: without the mixing a 60-point sweep produced 50 dropped writes and zero torn ones (`:154-157`).
- `pub fn at_shaped(at_op: u64, seed: u64, shape: WriteShape) -> FaultPlan` — `:168`, `FaultPlan { shape, ..Self::at(at_op, seed) }`. This is what a sweep uses, to put **every** shape at every point.

`shape_from(n) = match n % 3 { 0 => Drop, 1 => Tear, _ => Corrupt }` — `:173-179`. PRNG is hand-written SplitMix64, `pub struct Rng(u64)` with `new`/`next_u64`/`below` — `:50-69`; never clock-seeded, deliberately (`:48-49`).

**"Did the fault actually fire?"** — `fabric.fired() -> Option<FiredFault>` (`:447`), plus `fabric.crashed() -> bool` (`:451`).

```rust
pub struct FiredFault {         // src/storage/sim.rs:183-193
    pub op_index: u64,
    pub file: String,
    pub kind: FaultKind,
    pub offset: u64,
    pub len: usize,             // bytes the op was asked to write (0 for a sync)
    pub kept: usize,            // bytes that landed; 0 for a drop.
                                // For CorruptWrite, reused as the INDEX of the wrong byte (:676-686)
}
```

Every fault-injecting test in the repo asserts this explicitly, e.g. `src/consensus/tests_log.rs:659` `assert!(fired.is_some(), "{where_}: no fault fired, so this point tested nothing");` and `:516`, `:1302`, `:1344`, `:1373`.

Three gotchas the API forces on a caller, all in `execute` (`:487-559`):

- The plan fires at the first faultable op **at or above** `at_op` (`:498`, `index >= p.at_op`), and reads/`len` are counted but never faultable (`:230-232`), so `fired.op_index` can exceed `at_op` (`:129-131`).
- `WriteShape::Tear` degrades to `DropWrite` when `len < 2`, and the recorded `kind` is chosen by `kept > 0` (`:521-537`). So compare the fault that **fired**, not the one planned — see `tests/sim_durability.rs:1148`, `if fired.kind != FaultKind::TearWrite || fired.file != DB { continue }`.
- `WriteShape::Corrupt` is only honoured when `unit <= 1 && len > 0 && offset >= verified_from` (`:511-515`); otherwise it falls through to the tear/drop branch.
- A fault fires **at most once per fabric** (`:496`, `st.fired.is_none()`).

---

## 3. `set_write_atomicity(&self, name: &str, unit: u64)` — `src/storage/sim.rs:379-382`

Declares that writes to `name` tear only at multiples of `unit` bytes, measured in **absolute file offsets** ("a device tears at a sector boundary, not at a boundary relative to whatever the caller happened to pass" — `:526-529`). Implementation: `unit.max(1)`, stored in `atomic_unit` (`:380-381`); consumed at `:530-536`, rounding the tear end down: `let abs_end = offset + raw as u64; let aligned = abs_end - (abs_end % unit); aligned.saturating_sub(offset)`. It also *disables* `WriteShape::Corrupt` for that file (`:511-513` requires `unit <= 1`).

Default is 1 byte (`:505`, `unwrap_or(&1)`).

**When a caller must set it** — the doc states the rule as one question, "does this file's *reader* verify what it reads?" (`:357-378`):
- Set it (to the write unit) for a file whose writer has **no** way to detect a partial write. `heap_page::Page`'s `checksum` field is written and read back verbatim and never computed by anything, so a torn table page is undetectable by design and the engine's durability rests on the page write being all-or-nothing (`:368-372`).
- Leave it at 1 for a file whose reader verifies — the WAL, where every frame carries a CRC32 and `scan_valid_end` stops at the first frame that fails (`:365-367`). Byte granularity there finds real bugs.

In practice: `tests/sim_durability.rs:125` sets `f.set_write_atomicity(DB, PAGE_SIZE as u64)` for the database file, and `:129` sets `f.set_verified_from(WAL, wal_header_len())` for the WAL (its 24-byte header carries no checksum, `:387-394`). `src/consensus/tests_log.rs` calls **neither** — the round log leaves unit 1 and `verified_from` 0 for both files, because every frame is CRC'd. The known gap that unit-1 pages expose is pinned deliberately by `a_torn_table_page_is_served_as_a_row_that_was_never_written` in `tests/sim_durability.rs` (`:376-378`).

Because `from_images` does **not** carry the knobs over, every `from_images` in `tests/sim_durability.rs` re-sets them (`:520-521`, `:548-549`, `:1423-1424`, `:1486-1488`, `:1497-1499`). `restart()` **does** carry them (`src/storage/sim.rs:466-477`).

---

## 4. How the crash is modelled, and how a test reopens

**A fault ends the run.** `execute` applies the partial write if there is one, then sets `st.fired` and `st.crashed = true` and returns the error (`src/storage/sim.rs:590-610`):

- `TearWrite` → `write_into(img, durability, offset, &buf[..f.kept])` (`:595`)
- `CorruptWrite` → full length, `garbled[f.kept] ^= 0xFF` (`:596-602`)
- `DropWrite` / `FailSync` / `DropSetLen` → nothing lands (`:603`)
- error text: `"simulated fault: {kind:?} at operation {index}"` (`:296-298`)

**After the fault**, every later operation — reads and length queries included — returns an error and changes nothing: `already_crashed` is checked at `:494`, the op is still counted and traced with `ok: false` (`:565-582`), and then `:584-586` returns `crashed_err(kind.what())` → `"simulated crash: the process is gone, {a write|a read|a flush|a truncate|a length query} did not happen"` (`:292-294`, `:218-226`). The rationale is at `:19-23`: modelling a torn write as a *short count* would be wrong, since both callers in this codebase loop on short counts.

**Reopening after the crash — do not reuse the crashed fabric.** Two calls do it, and both build a *new* fabric over the surviving image:

- `let restarted = f.restart();` then `open_on(&restarted)` — the normal way. `restart()` (`:466-478`) = `SimFabric::from_images(self.durable_image(), None, self.durability)` plus the atomicity/verified knobs copied forward. Used at `src/consensus/tests_log.rs:662-666`, `:539-540`, `:555-556`, `:1376-1379`.
- `SimFabric::from_images(fabric.durable_image(), plan, durability)` directly, when the test wants to hand-edit the surviving bytes first (`src/consensus/tests_log.rs:168-178`, `:702-706`, `:746-754`) or to re-plant a *new* plan on the same starting image, which is exactly how a sweep gets a fresh identical `base` for every point (`:637-639`, `:654`).

`durable_image()` is the seam between the two: `BTreeMap<String, Vec<u8>>` of the `durable` halves (`:456-462`). Tests read the image directly to answer questions they must not ask the object under test — e.g. `live_file(&images)` at `src/consensus/tests_log.rs:76-94`, "read out of the images rather than out of the `RoundLog` — a test that asked the object would be asking the thing under test".

---

## 5. Copy-pasteable sweep sketch

Condensed from the real `sweep_a_checkpoint`, **`src/consensus/tests_log.rs:629-692`**, called by the `#[test] a_crash_at_any_point_during_a_checkpoint_loses_nothing` at `:612-627` (which runs it under both `Durability` arms and asserts `total >= 24` so a sweep that collected nothing cannot pass). The `where_` string at `:658` is what prints the seed, the op index, the shape and the durability model on every failure.

```rust
fn sweep_a_checkpoint(durability: Durability) -> usize {
    const SEED: u64 = 0xF0B;

    // 1. Build a starting image with a normal, fault-free run, then take the surviving bytes.
    let fabric = SimFabric::clean(durability);
    let mut log = open_on(&fabric).unwrap();
    fill(&mut log, 6, 1..=10);
    log.discard_prefix(3, 6).unwrap();          // one checkpoint already done
    drop(log);
    let base = fabric.restart().durable_image();

    // 2. Census run: same workload, no fault, to learn WHICH op indices are faultable.
    let census = SimFabric::from_images(base.clone(), None, durability);
    let mut l = open_on(&census).unwrap();
    let mark = census.op_count();               // window the sweep to the operation under test
    l.discard_prefix(6, 6).unwrap();
    let points: Vec<u64> = census.faultable_ops().into_iter().filter(|i| *i >= mark).collect();
    assert!(points.len() >= 4, "only {} faultable ops under {durability:?}", points.len());

    // 3. One run per (op, shape). Every point starts from the same image.
    let mut ran = 0usize;
    for at in points {
        for shape in [WriteShape::Drop, WriteShape::Tear, WriteShape::Corrupt] {
            let f = SimFabric::from_images(base.clone(), Some(FaultPlan::at_shaped(at, SEED, shape)), durability);
            let mut broken = open_on(&f).unwrap();
            let _ = broken.discard_prefix(6, 6);            // expected to fail
            let fired = f.fired();
            let where_ = format!("{durability:?} seed {SEED:#x} at op {at} shape {shape:?}");
            assert!(fired.is_some(), "{where_}: no fault fired, so this point tested nothing");
            drop(broken);

            // 4. Reopen over the survivors and assert the invariant.
            let restarted = f.restart();
            let after = match open_on(&restarted) {
                Ok(a) => a,
                Err(e) => panic!("{where_} ({:?}) left a log that will not open: {e}", fired.unwrap()),
            };
            let floor = after.snapshot_round();
            assert!(floor == 3 || floor == 6, "{where_} ({:?}) left floor {floor}", fired.clone().unwrap());
            assert_eq!(after.last_round(), 10, "{where_} ({:?}) lost rounds off the end", fired.clone().unwrap());
            for r in floor + 1..=10 {
                assert_eq!(after.entry(r).unwrap(), entry(6, r),
                    "{where_} ({:?}) lost or changed round {r}", fired.clone().unwrap());
            }
            ran += 1;
        }
    }
    ran
}
```

Two narrower variants of the same census→aim→reopen shape, if a single aimed point is wanted rather than a sweep: `src/consensus/tests_log.rs:488-517` (find the first faultable op at/above `mark`, drop it, assert the log poisons) and `:1275-1312` (locate one specific `TraceOp` by `(file, kind, offset, len)` in the census trace, then fault at that index **and** at `index + 1`, because "the plan cannot name a sync directly" — `:1292-1293`).

---

## 6. No filesystem, no tempdir

**No.** Stated at `src/storage/sim.rs:32-33`: "**No `File`, no `TempDir`, no syscalls.** The image is a `Vec<u8>`, so a sweep of two hundred crash points costs no disk at all." Confirmed by the imports — `src/storage/sim.rs:40-45` are the entire `use` list: `std::collections::BTreeMap`, `std::io`, `std::sync::{Arc, Mutex}`, `crate::storage::storage::Storage`, `crate::wal::log::crc32`. No `std::fs`, no `tempfile`, and the backing store is `durable: Vec<u8>` / `visible: Vec<u8>` in `FileImage` (`:252-258`). Zero runtime dependencies: the PRNG is hand-written (`:35-38`, `:50`) and the digests reuse the crate's own `crc32`.

The one filesystem-touching test in `tests_log.rs` is deliberately *outside* the fabric — `the_log_opens_on_real_files_and_recovers_from_them`, `src/consensus/tests_log.rs:1113-1132`, using `tempfile::tempdir()` and `RoundLog::open(&base)`, whose comment states its purpose: "Everything above runs on the simulated fabric. This runs the path a database actually takes, so a divergence between `impl Storage for File` and the fabric cannot hide" (`:1115-1116`).