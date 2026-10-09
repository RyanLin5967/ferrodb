//! **D115 instrument — where the per-statement write path spends its time, split by phase.**
//!
//! The row claims `stage_all` is quadratic in a session's op count because it clones the whole
//! `TxnFrame` once per statement. That is a reading of the source, and a reading of the source has
//! now named the wrong mechanism for a slope on this exact file three times (D68, D69-REOPEN,
//! D114). So this module does not assume the clone is the cost: it times **every** phase of
//! `stage_all` separately, plus the two other per-statement costs that are also O(ops-so-far) and
//! are NOT the clone:
//!
//! * `extends()` in `tel::log` compares the stored frame's ops against the new frame's prefix —
//!   one `op_eq` per already-stored op, every statement.
//! * `frame_eq()` compares every accumulated GUARD before it reaches the op-count short-circuit,
//!   so a statement that pushes no guard pays a full recursive walk of the session's guards.
//! * `MemEffectLog::append` finds the open frame with a **linear scan** over every frame the log
//!   holds — a different axis (sessions, not ops), counted apart for that reason.
//!
//! Four O(n)-or-worse terms in series are indistinguishable from one term three times as big, which is
//! D99's lesson applied here: each one is counted on its own axis.
//!
//! # The integers are the claim; the nanoseconds are an upper bound
//!
//! This box runs a build fleet, so a duration is an upper bound and nothing better. Each phase
//! therefore reports BOTH a nanosecond total and an integer that does not move under load:
//! `CLONE_OPS` is how many ops were copied, `EXTENDS_OP_CMP` how many op comparisons were made,
//! `POSITION_SCAN` how many frames the index walked. A quadratic in the integers is a property of
//! the code; a quadratic in the nanoseconds is a property of the code *and* the machine.
//!
//! # Cost of the instrument itself
//!
//! Five `Instant::now()` pairs and about ten relaxed `fetch_add`s per statement — a fixed cost of
//! roughly a few hundred nanoseconds, paid once per statement whatever the op count. It cannot
//! manufacture a slope on an axis it does not vary with, and at the smallest point on the axis it
//! is visible in `STAGE_NS` as a constant floor, which is the honest place for it to show up.
//!
//! Every counter is process-global and monotone. Read the pair with [`snapshot`] before and after
//! a phase and subtract; nothing here is scoped to a thread.

use std::sync::atomic::{AtomicU64, Ordering};

/// Wall time inside `stage_all`, whole function.
pub static STAGE_NS: AtomicU64 = AtomicU64::new(0);

/// Calls to `stage_all`. One per statement that stages at least one row.
pub static STAGE_CALLS: AtomicU64 = AtomicU64::new(0);

/// The decide half: envelope `admit`, the escrow batch check, and `charge_row_writes`.
pub static DECIDE_NS: AtomicU64 = AtomicU64::new(0);

/// The apply half, EXCLUDING the frame clone: workspace map inserts and op/guard pushes.
pub static APPLY_NS: AtomicU64 = AtomicU64::new(0);

/// `ws.frame.clone()` alone. The term the row names.
pub static CLONE_NS: AtomicU64 = AtomicU64::new(0);

/// Ops copied by those clones, summed. The integer form of the same term: with `delta` fixed
/// and one op per statement this is `W(W+1)/2` and nothing else.
pub static CLONE_OPS: AtomicU64 = AtomicU64::new(0);

/// Guards copied by those clones, summed.
pub static CLONE_GUARDS: AtomicU64 = AtomicU64::new(0);

/// `self.log.append(&frame)` alone.
pub static APPEND_NS: AtomicU64 = AtomicU64::new(0);

/// **Dropping the cloned frame.** Not an afterthought: `Vec<Op>` runs `Op`'s destructor per
/// element, so a deep copy costs a deep FREE as well, and the free happens after the last span
/// any obvious instrumentation covers. The first version of this probe timed five phases and left
/// 40% of `stage_all` unaccounted for at every point of the axis — this is that 40%. A clone's
/// cost is `clone + drop`, and attributing only the clone understates the term by roughly half.
pub static DROP_NS: AtomicU64 = AtomicU64::new(0);

/// The copy-on-write mirror loop (`put_row`/`delete_row`), which the row does not mention and
/// which is the other candidate for a per-statement cost that grows.
pub static MIRROR_NS: AtomicU64 = AtomicU64::new(0);

/// Rows mirrored.
pub static MIRROR_ROWS: AtomicU64 = AtomicU64::new(0);

/// `op_eq` calls made by `extends()` — the prefix comparison the mem log runs on every
/// re-append. O(ops already stored) per statement, same axis as the clone.
pub static EXTENDS_OP_CMP: AtomicU64 = AtomicU64::new(0);

/// `op_eq` calls made by `frame_eq()` — the retry check, which short-circuits on op count and
/// so is expected to be ~0 when the frame grew.
pub static EQ_OP_CMP: AtomicU64 = AtomicU64::new(0);

/// Guards compared by `frame_eq()`. Runs BEFORE the op-count short-circuit and only
/// short-circuits on guard COUNT, so a statement that pushes no guard pays a full recursive
/// walk of every guard the session has accumulated. A third O(ops) term, on the same axis as
/// the clone and firing under a condition the other two do not share.
pub static EQ_GUARD_CMP: AtomicU64 = AtomicU64::new(0);

/// Frames walked by `MemEffectLog::append`'s `position()` scan, summed.
pub static POSITION_SCAN: AtomicU64 = AtomicU64::new(0);

/// Ops actually copied into the stored frame by `extend_from_slice`. The tail, so this should
/// track the statement's own op count and NOT the session's.
pub static EXTEND_TAIL_OPS: AtomicU64 = AtomicU64::new(0);

/// Add `n` to a counter. Relaxed: these are diagnostics, never a happens-before edge.
#[inline]
pub fn bump(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

/// One `stage_all` call, recorded into [`STAGE_NS`] and [`STAGE_CALLS`] when the span is DROPPED.
///
/// Recording on drop is what makes every exit count: an early `?` return, a refusal, and an
/// unwinding panic all drop the span, so none of them can leave `STAGE_NS` short. Create it as the
/// first statement of the body and bind it to a named `_`-prefixed variable (`let _span = ...`),
/// never to `_`, which would drop it on the spot.
pub struct StageSpan(std::time::Instant);

impl StageSpan {
    #[inline]
    pub fn start() -> Self {
        StageSpan(std::time::Instant::now())
    }
}

impl Drop for StageSpan {
    #[inline]
    fn drop(&mut self) {
        bump(&STAGE_NS, self.0.elapsed().as_nanos() as u64);
        bump(&STAGE_CALLS, 1);
    }
}

/// Every counter's current value, in the declaration order of [`FIELDS`]. Read twice and subtract
/// elementwise to scope the whole set to one phase.
pub fn snapshot() -> Vec<u64> {
    all().iter().map(|c| c.load(Ordering::Relaxed)).collect()
}

/// The counters, in a fixed order, paired with [`FIELDS`].
pub fn all() -> &'static [&'static AtomicU64] {
    &ALL
}

static ALL: [&AtomicU64; 16] = [
    &STAGE_NS,
    &STAGE_CALLS,
    &DECIDE_NS,
    &APPLY_NS,
    &CLONE_NS,
    &CLONE_OPS,
    &CLONE_GUARDS,
    &APPEND_NS,
    &DROP_NS,
    &MIRROR_NS,
    &MIRROR_ROWS,
    &EXTENDS_OP_CMP,
    &EQ_OP_CMP,
    &EQ_GUARD_CMP,
    &POSITION_SCAN,
    &EXTEND_TAIL_OPS,
];

/// Names for [`snapshot`]'s positions, so a harness prints a labelled row rather than a tuple
/// whose order it has to keep in step by hand.
pub const FIELDS: &[&str] = &[
    "stage_ns",
    "stage_calls",
    "decide_ns",
    "apply_ns",
    "clone_ns",
    "clone_ops",
    "clone_guards",
    "append_ns",
    "drop_ns",
    "mirror_ns",
    "mirror_rows",
    "extends_op_cmp",
    "eq_op_cmp",
    "eq_guard_cmp",
    "position_scan",
    "extend_tail_ops",
];

/// `after - before`, elementwise. Panics on a shorter `before`, which can only be a harness bug.
pub fn delta(before: &[u64], after: &[u64]) -> Vec<u64> {
    assert_eq!(before.len(), after.len(), "snapshot widths differ");
    before.iter().zip(after).map(|(b, a)| a - b).collect()
}

/// Look one field up out of a [`delta`] by name. Panics on an unknown name.
pub fn field(d: &[u64], name: &str) -> u64 {
    let i = FIELDS
        .iter()
        .position(|f| *f == name)
        .unwrap_or_else(|| panic!("no such stage_probe field: {name}"));
    d[i]
}

const _: () = {
    // The two tables are read POSITIONALLY by `field`, so a mismatch would silently mislabel every
    // number a harness prints. Checked against each other rather than against a literal: a literal
    // is a third place to keep in step, and it went stale the first time a counter was added.
    assert!(FIELDS.len() == ALL.len());
};
