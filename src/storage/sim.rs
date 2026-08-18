//! A deterministic simulation of durable storage, so a crash can be *aimed* instead of hoped for.
//!
//! # What this is for
//!
//! Recovery is the code nothing exercises. Before this module the only way to test it was to write a
//! file, close it, hand-edit the bytes, and reopen — which stages the *aftermath* of a crash at a
//! point the test author chose by hand. Two whole classes of fault could not be expressed at all:
//! a write that was interrupted **part-way through**, and a fault at an operation nobody thought to
//! pick. This module replaces the file with an in-memory image behind
//! [`Storage`](crate::storage::storage::Storage), counts every operation, and breaks the one it is
//! told to.
//!
//! # The model, stated so its limits are visible
//!
//! * **Three fault kinds, matching the three ways durable IO actually loses data:** a write that
//!   lands only partially ([`FaultKind::TearWrite`]), a write that lands not at all
//!   ([`FaultKind::DropWrite`]), and a flush that reports failure ([`FaultKind::FailSync`]).
//!   `set_len` is treated as a write that did not land ([`FaultKind::DropSetLen`]).
//! * **A fault ends the run.** After it fires the fabric is `crashed`: every later operation returns
//!   an error and changes nothing. This is the point. Modelling a torn write as a *short count* would
//!   be wrong — both callers in this codebase loop on short counts, so a short write is a retry, not
//!   a loss. A real torn write happens because the process stopped existing, and nothing it would
//!   have done next happened either.
//! * **Reads are counted but never faulted.** Stated blind spot: a failed read leaves the durable
//!   image untouched, so it cannot produce the class of bug this harness hunts (a database that comes
//!   back wrong). Media errors on read are not covered here.
//! * **One lock over both files.** IO from every thread is serialised through
//!   [`SimFabric`]'s mutex, so the recorded operation order is a total order. That is what makes a
//!   seed reproducible. It also means this fabric does **not** reproduce the interleavings of genuinely
//!   concurrent IO; it reproduces one deterministic interleaving. Concurrency lives in
//!   `tests/integration_wal_concurrency.rs`, not here.
//! * **No `File`, no `TempDir`, no syscalls.** The image is a `Vec<u8>`, so a sweep of two hundred
//!   crash points costs no disk at all.
//!
//! # Zero runtime dependencies
//!
//! The PRNG is a hand-written SplitMix64 ([`Rng`]) and the digests reuse the crate's own
//! [`crc32`](crate::wal::log::crc32) — the same implementation the WAL trusts for record integrity.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use crate::storage::storage::Storage;
use crate::wal::log::crc32;

/// SplitMix64. Small, well-distributed, and hand-written because this crate has no runtime
/// dependencies. Deliberately *not* seeded from the clock anywhere: a seed a test did not choose is a
/// crash point nobody can reproduce.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform-ish in `[0, n)`. `n == 0` yields 0.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }
}

/// How a crash treats writes that have not been flushed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Durability {
    /// Every write that returns `Ok` is durable immediately; a sync is a no-op that can still fail.
    ///
    /// This is the model the fault sweep uses, and the reason is interpretability: the injected fault
    /// becomes the *only* thing the crash lost, so when an invariant breaks at operation N it broke
    /// because of the fault at N. It corresponds to a device with no volatile write cache.
    WriteThrough,
    /// Writes land in a volatile cache; only a sync makes them durable, and a crash loses the rest.
    ///
    /// Strictly harsher than [`Durability::WriteThrough`] and the more realistic of the two. Kept
    /// separate rather than made the default because a failure under it is ambiguous — it can mean
    /// the fault broke recovery, or that the workload never promised durability at that point.
    SyncOnly,
}

/// What kind of break to stage. Chosen from the operation's type, not guessed, so a plan always
/// fires: a write gets torn or dropped, a flush gets failed, a truncate gets skipped.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum FaultKind {
    /// The first `kept` bytes of the write land, the rest never do.
    TearWrite,
    /// The write lands nowhere.
    DropWrite,
    /// The flush returns an error.
    FailSync,
    /// The truncate/extend does not happen.
    DropSetLen,
}

/// Which operation to break, and how. Everything here is derived from a seed and is `Copy`, so a
/// failing sweep point can be pasted into a one-line reproducer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FaultPlan {
    pub seed: u64,
    /// The fault fires at the first *faultable* operation whose index is at or above this. Reads are
    /// counted in the index space but are never faultable, so the fired index can be higher.
    pub at_op: u64,
    /// Seed-derived: tear a write rather than dropping it.
    pub tear: bool,
    /// Seed-derived: where inside the buffer the tear lands.
    pub tear_pick: u64,
}

impl FaultPlan {
    /// A plan derived **entirely** from the seed, including *where* the crash happens.
    ///
    /// `op_count` comes from a fault-free census run of the same workload, so the chosen point is
    /// always inside the run. This is the constructor that makes "two seeds crash in two different
    /// places" a checkable claim.
    pub fn from_seed(seed: u64, op_count: u64) -> FaultPlan {
        let mut rng = Rng::new(seed);
        let at_op = rng.below(op_count.max(1));
        let tear = rng.next_u64() % 2 == 0;
        let tear_pick = rng.next_u64();
        FaultPlan { seed, at_op, tear, tear_pick }
    }

    /// A plan at a caller-chosen operation index; the seed still chooses the shape of the break.
    ///
    /// `at_op` is mixed into the stream, not just carried alongside it. Without that the shape is a
    /// function of the seed alone, so a sweep over a hundred operations with one seed tears none of
    /// them or all of them — measured: a 60-point sweep produced 50 dropped writes and **zero** torn
    /// ones, which quietly removed a whole fault kind from the range being swept.
    pub fn at(at_op: u64, seed: u64) -> FaultPlan {
        let mut rng = Rng::new(seed ^ at_op.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x5DEE_CE66_D0D1_6E01);
        let tear = rng.next_u64() % 2 == 0;
        let tear_pick = rng.next_u64();
        FaultPlan { seed, at_op, tear, tear_pick }
    }

    /// A plan at a chosen operation with a chosen shape, so a sweep can put **both** shapes at every
    /// point instead of taking whichever one the seed happened to pick. The seed still chooses where
    /// inside the buffer a tear lands.
    pub fn at_shaped(at_op: u64, seed: u64, tear: bool) -> FaultPlan {
        FaultPlan { tear, ..Self::at(at_op, seed) }
    }
}

/// What actually happened, as opposed to what was planned. Compared across runs to prove a seed
/// reproduces a crash point; printed when an invariant breaks so the point is nameable.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FiredFault {
    pub op_index: u64,
    pub file: String,
    pub kind: FaultKind,
    pub offset: u64,
    /// Bytes the operation was asked to write (0 for a sync).
    pub len: usize,
    /// Bytes that actually landed. `0` for a drop, `len` never (that would not be a tear).
    pub kept: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpKind {
    Pwrite,
    Pread,
    SyncAll,
    SyncData,
    SetLen,
    Len,
}

impl OpKind {
    fn tag(self) -> u8 {
        match self {
            OpKind::Pwrite => 1,
            OpKind::Pread => 2,
            OpKind::SyncAll => 3,
            OpKind::SyncData => 4,
            OpKind::SetLen => 5,
            OpKind::Len => 6,
        }
    }

    /// For the "the process is gone" message, so an error names what was refused.
    fn what(self) -> &'static str {
        match self {
            OpKind::Pwrite => "a write",
            OpKind::Pread => "a read",
            OpKind::SyncAll | OpKind::SyncData => "a flush",
            OpKind::SetLen => "a truncate",
            OpKind::Len => "a length query",
        }
    }

    /// Reads and length queries cannot lose data, so they are never broken. See the module blind-spot
    /// note.
    fn faultable(self) -> bool {
        matches!(self, OpKind::Pwrite | OpKind::SyncAll | OpKind::SyncData | OpKind::SetLen)
    }
}

/// One recorded operation. The sequence of these *is* the "byte sequence" the exit criteria talk
/// about: what was written, where, and in what order.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TraceOp {
    pub index: u64,
    pub file: String,
    pub kind: OpKind,
    pub offset: u64,
    pub len: usize,
    /// CRC of the bytes handed to a write. `0` for everything else — a read's content is a function
    /// of the image, which is digested separately, and folding it in here would make the trace
    /// depend on uninitialised buffer tails.
    pub data_crc: u32,
    /// `false` when this operation is the injected fault, or when it came after one.
    pub ok: bool,
}

struct FileImage {
    /// The bytes that survive a crash.
    durable: Vec<u8>,
    /// The bytes a reader in this process sees. Equal to `durable` under
    /// [`Durability::WriteThrough`].
    visible: Vec<u8>,
}

impl FileImage {
    fn new(bytes: Vec<u8>) -> Self {
        FileImage { durable: bytes.clone(), visible: bytes }
    }
}

struct FabricState {
    next_op: u64,
    files: BTreeMap<String, FileImage>,
    /// Per file, the unit a torn write can tear at: 1 byte by default, or a whole page for a file
    /// whose writer is entitled to assume page-atomic writes. See
    /// [`SimFabric::set_write_atomicity`].
    atomic_unit: BTreeMap<String, u64>,
    trace: Vec<TraceOp>,
    trace_digest_bytes: Vec<u8>,
    fired: Option<FiredFault>,
    crashed: bool,
}

/// The simulated machine: a set of files, a global operation counter, one fault plan, and a trace.
///
/// Shared by every [`SimStorage`] handle opened from it, which is what makes "operation index N"
/// mean the same thing across the database file and the WAL.
pub struct SimFabric {
    plan: Option<FaultPlan>,
    durability: Durability,
    state: Mutex<FabricState>,
}

fn crashed_err(what: &str) -> io::Error {
    io::Error::other(format!("simulated crash: the process is gone, {what} did not happen"))
}

fn fault_err(kind: FaultKind, index: u64) -> io::Error {
    io::Error::other(format!("simulated fault: {kind:?} at operation {index}"))
}

impl SimFabric {
    /// Lock the state, **tolerating poison**.
    ///
    /// A fault-injection harness exists to make code die in the middle of things, and the tests
    /// around it deliberately catch panics. A poisoned mutex here would turn "the workload panicked
    /// at operation 41" into "the harness panicked while asking which operation it was", which
    /// destroys the only evidence. The state is a byte image and a counter, not an invariant that a
    /// half-finished operation can break: every mutation in `execute` happens after every fallible
    /// step, so recovering the inner value is safe.
    fn lock(&self) -> std::sync::MutexGuard<'_, FabricState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A fabric with no fault: the census run. Records the trace and the operation count a sweep
    /// needs.
    pub fn clean(durability: Durability) -> Arc<Self> {
        Self::with_plan(None, durability)
    }

    pub fn with_fault(plan: FaultPlan, durability: Durability) -> Arc<Self> {
        Self::with_plan(Some(plan), durability)
    }

    fn with_plan(plan: Option<FaultPlan>, durability: Durability) -> Arc<Self> {
        Arc::new(SimFabric {
            plan,
            durability,
            state: Mutex::new(FabricState {
                next_op: 0,
                files: BTreeMap::new(),
                atomic_unit: BTreeMap::new(),
                trace: Vec::new(),
                trace_digest_bytes: Vec::new(),
                fired: None,
                crashed: false,
            }),
        })
    }

    /// A fabric holding `images` as its starting contents. This is how a restart works: the bytes
    /// that survived are all the new process gets.
    pub fn from_images(
        images: BTreeMap<String, Vec<u8>>,
        plan: Option<FaultPlan>,
        durability: Durability,
    ) -> Arc<Self> {
        let f = Self::with_plan(plan, durability);
        {
            let mut st = f.lock();
            for (name, bytes) in images {
                st.files.insert(name, FileImage::new(bytes));
            }
        }
        f
    }

    /// Declare that writes to `name` tear only at multiples of `unit` bytes, absolutely (a tear ends
    /// at a `unit` boundary in the file, the way a device tears at a sector boundary).
    ///
    /// **This is a statement about the fault model, and it is the most consequential knob here.** With
    /// `unit == 1` — the default — a 4 KiB page write can land 3924 bytes and leave the last 172 stale.
    /// Whether that is a fault a given file's *writer* is allowed to be broken by depends entirely on
    /// whether that writer has a way to detect it:
    ///
    /// * The WAL is built for it. Every frame carries a CRC32 and `scan_valid_end` walks the chain and
    ///   stops at the first frame that fails, so a torn tail is expected and survivable. Byte
    ///   granularity is the right model there and finds real bugs.
    /// * Ordinary table pages are **not**. `heap_page::Page` has a `checksum` field that is written
    ///   verbatim and read back verbatim and never computed by anything (contrast
    ///   `cow::page_header::stamp_checksum`, which the branch arena really does verify). So a torn
    ///   table page is undetectable by design, and the engine's durability rests on a page write being
    ///   all-or-nothing.
    ///
    /// Setting `PAGE_SIZE` for the database file therefore does not paper over a bug; it states the
    /// assumption the engine already makes, so that a sweep over it tests recovery rather than
    /// re-deriving a known gap at forty different offsets. The gap itself is pinned by
    /// `a_torn_table_page_is_served_as_a_row_that_was_never_written` in `tests/sim_durability.rs`,
    /// which sets the unit back to 1 on purpose.
    pub fn set_write_atomicity(&self, name: &str, unit: u64) {
        let unit = unit.max(1);
        self.lock().atomic_unit.insert(name.to_string(), unit);
    }

    /// A handle on one file. Creating it is not an operation — a real `open` is not part of the
    /// durable-IO surface this models — so it does not move the counter.
    pub fn open(self: &Arc<Self>, name: &str) -> Arc<SimStorage> {
        let mut st = self.lock();
        st.files.entry(name.to_string()).or_insert_with(|| FileImage::new(Vec::new()));
        drop(st);
        Arc::new(SimStorage { fabric: Arc::clone(self), name: name.to_string() })
    }

    pub fn op_count(&self) -> u64 {
        self.lock().next_op
    }

    pub fn trace(&self) -> Vec<TraceOp> {
        self.lock().trace.clone()
    }

    /// Indices of every operation a fault could break, in order. The sweep walks exactly this list,
    /// so no faultable point is skipped and no point is wasted on a read.
    pub fn faultable_ops(&self) -> Vec<u64> {
        self.state
            .lock()
            .unwrap()
            .trace
            .iter()
            .filter(|o| o.kind.faultable())
            .map(|o| o.index)
            .collect()
    }

    /// A single number over the whole operation sequence: kind, file, offset, length and, for writes,
    /// the bytes. Two runs agreeing on this agree on *what was written, where, and in what order*.
    pub fn trace_digest(&self) -> u32 {
        crc32(&self.lock().trace_digest_bytes)
    }

    /// A digest of the surviving bytes of every file. Answers a different question from
    /// [`SimFabric::trace_digest`]: not "were the same writes issued" but "did the same image end up
    /// on disk".
    pub fn image_digest(&self) -> u32 {
        let st = self.lock();
        let mut buf = Vec::new();
        for (name, img) in st.files.iter() {
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&(img.durable.len() as u64).to_be_bytes());
            buf.extend_from_slice(&img.durable);
        }
        crc32(&buf)
    }

    pub fn fired(&self) -> Option<FiredFault> {
        self.lock().fired.clone()
    }

    pub fn crashed(&self) -> bool {
        self.lock().crashed
    }

    /// The bytes that survived the crash, per file.
    pub fn durable_image(&self) -> BTreeMap<String, Vec<u8>> {
        self.state
            .lock()
            .unwrap()
            .files
            .iter()
            .map(|(n, f)| (n.clone(), f.durable.clone()))
            .collect()
    }

    /// The machine after the reboot: a fresh, fault-free fabric holding only what survived, with the
    /// same write-atomicity model — rebooting does not change the hardware.
    pub fn restart(&self) -> Arc<SimFabric> {
        let fresh = SimFabric::from_images(self.durable_image(), None, self.durability);
        let units = self.lock().atomic_unit.clone();
        fresh.lock().atomic_unit = units;
        fresh
    }

    /// Perform one operation: claim the next index, decide whether this is the one to break, record
    /// it in the trace, and mutate the image — **all in one critical section**.
    ///
    /// It is one section deliberately. Claiming the index in one lock and mutating the image in a
    /// second would let two concurrent callers land their bytes in the opposite order from the one
    /// the trace records, which is the check-then-act shape that makes a "deterministic" trace a
    /// lie. The operation index and the byte it produces are decided together or not at all.
    fn execute(&self, name: &str, offset: u64, req: Req<'_>) -> io::Result<u64> {
        let kind = req.kind();
        let len = req.len();
        let mut st = self.lock();
        let index = st.next_op;
        st.next_op += 1;

        let already_crashed = st.crashed;
        let fire = !already_crashed
            && st.fired.is_none()
            && kind.faultable()
            && self.plan.is_some_and(|p| index >= p.at_op);

        let mut fired: Option<FiredFault> = None;
        if fire {
            let plan = self.plan.expect("checked by `fire`");
            fired = Some(match kind {
                OpKind::Pwrite => {
                    // A tear needs at least one byte on each side of the boundary. A one-byte write
                    // cannot be torn, so it is dropped instead — and `kept` records which happened,
                    // so a test compares the fault that fired, not the one that was planned.
                    let raw = if plan.tear && len >= 2 {
                        1 + (plan.tear_pick % (len as u64 - 1)) as usize
                    } else {
                        0
                    };
                    // Round the boundary down to this file's atomic unit, measured in absolute file
                    // offsets: a device tears at a sector boundary, not at a boundary relative to
                    // whatever the caller happened to pass. With the default unit of 1 this is a no-op.
                    let unit = *st.atomic_unit.get(name).unwrap_or(&1);
                    let kept = if unit <= 1 {
                        raw
                    } else {
                        let abs_end = offset + raw as u64;
                        let aligned = abs_end - (abs_end % unit);
                        aligned.saturating_sub(offset) as usize
                    };
                    let fk = if kept > 0 { FaultKind::TearWrite } else { FaultKind::DropWrite };
                    FiredFault { op_index: index, file: name.to_string(), kind: fk, offset, len, kept }
                }
                OpKind::SyncAll | OpKind::SyncData => FiredFault {
                    op_index: index,
                    file: name.to_string(),
                    kind: FaultKind::FailSync,
                    offset: 0,
                    len: 0,
                    kept: 0,
                },
                OpKind::SetLen => FiredFault {
                    op_index: index,
                    file: name.to_string(),
                    kind: FaultKind::DropSetLen,
                    offset,
                    len: 0,
                    kept: 0,
                },
                OpKind::Pread | OpKind::Len => unreachable!("not faultable"),
            });
        }

        let data_crc = match &req {
            Req::Pwrite(buf) => crc32(buf),
            _ => 0,
        };
        let ok = !already_crashed && fired.is_none();
        st.trace.push(TraceOp {
            index,
            file: name.to_string(),
            kind,
            offset,
            len,
            data_crc,
            ok,
        });
        let d = &mut st.trace_digest_bytes;
        d.extend_from_slice(&index.to_be_bytes());
        d.extend_from_slice(name.as_bytes());
        d.push(kind.tag());
        d.extend_from_slice(&offset.to_be_bytes());
        d.extend_from_slice(&(len as u64).to_be_bytes());
        d.extend_from_slice(&data_crc.to_be_bytes());
        d.push(ok as u8);

        if already_crashed {
            return Err(crashed_err(kind.what()));
        }
        let durability = self.durability;
        let img = st.files.get_mut(name).expect("open() created the file");

        if let Some(f) = fired {
            // Apply the partial write, if any, then close the machine down. Everything the workload
            // would have done next happened to a process that no longer exists.
            if let (OpKind::Pwrite, Req::Pwrite(buf)) = (kind, &req) {
                if f.kept > 0 {
                    write_into(img, durability, offset, &buf[..f.kept]);
                }
            }
            let err = fault_err(f.kind, f.op_index);
            st.fired = Some(f);
            st.crashed = true;
            return Err(err);
        }

        match req {
            Req::Pwrite(buf) => {
                write_into(img, durability, offset, buf);
                Ok(buf.len() as u64)
            }
            Req::Pread(buf) => {
                let start = offset as usize;
                if start >= img.visible.len() {
                    return Ok(0);
                }
                let n = buf.len().min(img.visible.len() - start);
                buf[..n].copy_from_slice(&img.visible[start..start + n]);
                Ok(n as u64)
            }
            Req::SyncAll | Req::SyncData => {
                if durability == Durability::SyncOnly {
                    img.durable = img.visible.clone();
                }
                Ok(0)
            }
            Req::SetLen => {
                img.visible.resize(offset as usize, 0);
                if durability == Durability::WriteThrough {
                    img.durable.resize(offset as usize, 0);
                }
                Ok(0)
            }
            Req::Len => Ok(img.visible.len() as u64),
        }
    }
}

/// One operation, with its payload. `offset` travels separately because `set_len` reuses it as the
/// new length.
enum Req<'a> {
    Pwrite(&'a [u8]),
    Pread(&'a mut [u8]),
    SyncAll,
    SyncData,
    SetLen,
    Len,
}

impl Req<'_> {
    fn kind(&self) -> OpKind {
        match self {
            Req::Pwrite(_) => OpKind::Pwrite,
            Req::Pread(_) => OpKind::Pread,
            Req::SyncAll => OpKind::SyncAll,
            Req::SyncData => OpKind::SyncData,
            Req::SetLen => OpKind::SetLen,
            Req::Len => OpKind::Len,
        }
    }

    fn len(&self) -> usize {
        match self {
            Req::Pwrite(b) => b.len(),
            Req::Pread(b) => b.len(),
            _ => 0,
        }
    }
}

fn write_into(img: &mut FileImage, durability: Durability, offset: u64, bytes: &[u8]) {
    let end = offset as usize + bytes.len();
    if img.visible.len() < end {
        img.visible.resize(end, 0);
    }
    img.visible[offset as usize..end].copy_from_slice(bytes);
    if durability == Durability::WriteThrough {
        if img.durable.len() < end {
            img.durable.resize(end, 0);
        }
        img.durable[offset as usize..end].copy_from_slice(bytes);
    }
}

/// One file's handle on a [`SimFabric`]. Hand it to `DiskManager::with_storage` or
/// `WalManager::with_storage`.
pub struct SimStorage {
    fabric: Arc<SimFabric>,
    name: String,
}

impl SimStorage {
    pub fn fabric(&self) -> &Arc<SimFabric> {
        &self.fabric
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Storage for SimStorage {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.fabric.execute(&self.name, offset, Req::Pwrite(buf)).map(|n| n as usize)
    }

    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.fabric.execute(&self.name, offset, Req::Pread(buf)).map(|n| n as usize)
    }

    fn sync_all(&self) -> io::Result<()> {
        self.fabric.execute(&self.name, 0, Req::SyncAll).map(|_| ())
    }

    fn sync_data(&self) -> io::Result<()> {
        self.fabric.execute(&self.name, 0, Req::SyncData).map(|_| ())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.fabric.execute(&self.name, len, Req::SetLen).map(|_| ())
    }

    fn len(&self) -> io::Result<u64> {
        self.fabric.execute(&self.name, 0, Req::Len)
    }
}
