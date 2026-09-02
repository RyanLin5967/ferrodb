//! F10 — the Typed Effect Log **through the shipped binary**, not through `src/tel/`'s own tests.
//!
//! `src/tel/tests_durable_log.rs` proves `DurableEffectLog` is a correct store: it replays, it
//! heals a torn tail, it refuses damage. What it cannot prove is that the program anyone actually
//! runs *uses* it, and for the whole life of that store the program did not. `DurableEffectLog`
//! landed with its constructors, its format and its crash sweep, and `grep -rn DurableEffectLog
//! src/` returned only the `tel` module and its own tests — the CLI handed the runtime
//! `Arc::new(MemEffectLog::new())` on both of its arms.
//!
//! That is the same defect the comment above the provenance wiring in `cli.rs` describes and fixes
//! one line below it: the rows keep their stamp and the table mapping them to an agent is gone.
//! The effect log sat four lines up with the identical defect and no such line, because the fix
//! was applied to the layer someone was looking at rather than to the class of problem.
//!
//! # What makes this test non-vacuous
//!
//! "The `.tel` file exists" passes against a runtime that creates the file and then writes every
//! frame to a `MemEffectLog` anyway — the header alone is 12 bytes and a brand-new store writes it
//! eagerly. So existence is checked, and then the file is **reopened in this process**, which is a
//! different process from the one that wrote it, and asked for the frames themselves. The ops are
//! matched against the row the SQL actually inserted, so an empty log, a truncated log, or a log
//! belonging to some other database all fail.
//!
//! Fire-checked by reverting `cli.rs` to `Arc::new(MemEffectLog::new())` and re-running: the file
//! is never created and the test fails on its first assertion.

use ferrodb::branch::types::BranchId;
use ferrodb::catalog::column::Value;
use ferrodb::tel::op::OpKind;
use ferrodb::tel::{DurableEffectLog, EffectLog};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// Ordinary-table pages reserved below the arena floor. Same reasoning as
/// `integration_cli_agent_isolation.rs`: the default floor puts the arena's first page ~128 MB in,
/// which is 128 MB of real zeroes on a filesystem without sparse files (NTFS, which CI runs).
const HEADROOM: u32 = 256;

/// Feed SQL to the real binary and return everything it printed.
///
/// `CARGO_BIN_EXE_*` rather than a hand-built `target/debug/...` path: it is correct on Windows
/// without a `.exe` suffix that gets forgotten.
fn ferrodb(db: &Path, sql: &str) -> String {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ferrodb"))
        .arg(db)
        .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ferrodb");

    child.stdin.take().unwrap().write_all(sql.as_bytes()).expect("write sql");
    let out = child.wait_with_output().expect("wait for ferrodb");

    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "ferrodb exited {:?} on:\n{sql}\n--- output ---\n{text}",
        out.status.code()
    );
    text
}

/// Fail loudly on a statement that errored. Without this a typo in the SQL turns every assertion
/// below into "the frame was not there", which is exactly what this test is trying to detect, and
/// it would then pass for the wrong reason.
fn assert_no_errors(out: &str, what: &str) {
    assert!(
        !out.contains("error:"),
        "{what} reported an error, so nothing after it means anything:\n{out}"
    );
}

/// Every op the log holds, swept across the branch ids a short session can have used.
///
/// Swept rather than asserted at one id on purpose: the agent's `BranchId` is assigned by the
/// branch catalog and is not printed by any statement this test runs, so pinning it here would be
/// hard-coding a number the product is free to change. The sweep asks the same question without
/// depending on the answer — and it is a sweep over the store's own `frames_for`, which is the
/// method merge reads through, not a private field.
fn all_ops(log: &DurableEffectLog) -> Vec<ferrodb::tel::op::Op> {
    let mut ops = Vec::new();
    for id in 0..8u64 {
        for generation in 0..3u32 {
            let branch = BranchId::new(id, generation);
            for frame in log.frames_for(branch, 0).expect("frames_for on a healthy log") {
                ops.extend(frame.ops.iter().cloned());
            }
        }
    }
    ops
}

#[test]
fn the_shipped_binary_writes_its_effect_frames_to_disk_and_they_outlive_the_process() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("tel.db");

    let setup = ferrodb(
        &db,
        "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
         INSERT INTO inv VALUES (1, 10);\n",
    );
    assert_no_errors(&setup, "the setup session");

    // A second process, so the runtime takes `cli.rs`'s `reopen_with_storage` arm — the one whose
    // entire job is attaching to a tree another process wrote, and the one an in-memory effect log
    // hands that tree's rows, that tree's provenance and no effects at all.
    let agent = ferrodb(
        &db,
        "BEGIN AGENT SESSION AS 'pricing' RUN 'r_1';\n\
         INSERT INTO inv VALUES (2, 20);\n",
    );
    assert_no_errors(&agent, "the agent session");

    // A THIRD process, and a second agent session. This is what makes the accumulation assertion
    // below mean something: two agent sessions in two different processes, so a log that is
    // truncated or reinitialised by whichever process opened it last cannot hold both.
    let second = ferrodb(
        &db,
        "BEGIN AGENT SESSION AS 'restock' RUN 'r_2';\n\
         INSERT INTO inv VALUES (3, 30);\n",
    );
    assert_no_errors(&second, "the second agent session");

    let tel = db.with_extension("db.tel");
    assert!(
        tel.exists(),
        "the shipped binary wrote no effect log at {}, so the runtime is still being handed a \
         MemEffectLog and every merge it computes begins from an empty history",
        tel.display()
    );

    // Reopened HERE — a different process from the one that wrote every frame in it. This is the
    // step `MemEffectLog` cannot survive and the reason the assertion is not merely on the file.
    let log = DurableEffectLog::open(&tel).expect("reopen the effect log the binary wrote");

    assert_eq!(
        log.discarded_tail_bytes(),
        0,
        "the log the binary left behind has a torn tail, so it was not durable at exit: {log:?}"
    );
    assert!(
        !log.is_empty(),
        "the effect log file exists but holds no frames, which is what a runtime that creates the \
         file and then appends to a different store in memory leaves behind: {log:?}"
    );

    // The frames are the writes this test actually made, not somebody else's and not noise. A
    // `RowCreate` carries the full initial image, so the inserted row is matchable.
    let ops = all_ops(&log);
    let holds = |a: i32, b: i32| {
        ops.iter().any(|op| match &op.kind {
            OpKind::RowCreate(image) => {
                image.contains(&Value::Integer(a)) && image.contains(&Value::Integer(b))
            }
            _ => false,
        })
    };

    // `restock` wrote in the process that ran most recently. Without this the test would pass
    // against a log that stopped accepting appends after its first process.
    assert!(
        holds(3, 30),
        "no frame carries the last agent session's INSERT (3, 30); the binary is not appending \
         to the durable log. ops = {ops:?}"
    );

    // **The assertion that distinguishes a durable store from a file that is merely created.**
    // `pricing` wrote `(2, 20)` in a process that had already exited before `restock`'s process
    // started, so this frame can only be here because it was read back off disk. A store that
    // reinitialised on open would hold `restock`'s frame alone and every other assertion in this
    // test would still pass.
    assert!(
        holds(2, 20),
        "the earlier agent session's INSERT (2, 20) is gone, so a later process truncated the log \
         instead of appending to it — frames do not survive a restart. ops = {ops:?}"
    );
}
