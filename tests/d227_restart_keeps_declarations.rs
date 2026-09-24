//! D227 — a restarted process still declares every table at its checkpoints, through the shipped
//! binary, and does NOT re-declare every run it ever knew.
//!
//! A checkpoint truncates the WAL and then re-appends what the running process retained:
//! `replay_schema` for DDL, `replay_runs` for agent runs. Both lists were filled only by `log_ddl`
//! and `declare_run` in the process that ran the statement, and nothing refilled them at an open.
//! So the first checkpoint after any restart left a log that named no table and no writer, against
//! the checkpoint's own comment that a reader starting at the new base otherwise "has no way to know
//! what any table is". The shipped feeds seed their decoders from the catalog and did not notice. A
//! reader that has only the log (an archived log, or a `LogicalDecoder::blank`) did.
//!
//! The first process creates a table and merges one agent's write, and its clean exit (a checkpoint)
//! declares both. The second process runs no DDL and no agent session. It writes one plain row and
//! exits cleanly, and its exit checkpoint must declare the table again.
//!
//! **The run half is the lead's decision of 09:44Z (SCALE-DESIGN "D227 run half"), not a refill.**
//! This test first required the run to be re-declared too, and `TxnManager::declare_runs_of` did
//! that from the provenance store. But that made every open append, and every later checkpoint
//! re-append, a declaration for every run ever interned: one per branch ever created (new-wall
//! audit round 2). An event's writer travels in its own transaction's `RunIdentity` binding, so a
//! txn-0 declaration serves only `Decoded::runs` and the slot-collision check. Runs are declared as
//! they are bound, in the process that binds them, and the assertion is now that the second
//! process re-declares NONE of the first process's runs. That is the guard against the wall coming
//! back.
//!
//! Pre-registered from source, UNBUILT: FAILS at `00f4c39` at the table assertion (the second
//! process's checkpoints re-declare nothing). The run assertion holds at `00f4c39`, FAILS at
//! `6553e67` (where `declare_runs_of` re-declared every run), and holds again once it is removed.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;

use ferrodb::replication::logical::{Decoded, LogicalDecoder};
use ferrodb::wal::log::WalManager;

/// Pages below the arena floor; see `tests/integration_cli_agent_isolation.rs` for why it is small.
const HEADROOM: u32 = 256;

/// Feed SQL to the real binary and return everything it printed. Verbatim in substance from
/// `tests/integration_cli_agent_isolation.rs`.
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
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "ferrodb exited {:?} on:\n{sql}\n--- output ---\n{text}", out.status.code());
    assert!(!text.contains("error:"), "a statement failed, so nothing after it means anything:\n{sql}\n--- output ---\n{text}");
    text
}

/// Decode the whole retained log of `db` with a decoder that knows nothing but what the log says.
fn decode_log_alone(db: &Path) -> Decoded {
    let wal = WalManager::new(format!("{}.wal", db.display()).into()).expect("open the log");
    let (base, end) = (wal.base_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst));
    LogicalDecoder::blank().decode(&wal, base, end).expect("decode the retained log")
}

fn declares_table(out: &Decoded, table: &str) -> bool {
    out.schema_changes.iter().any(|(_, t, _)| t == table)
}

fn declares_agent(out: &Decoded, agent: &str) -> bool {
    out.runs.values().any(|r| r.agent_id == agent)
}

#[test]
fn a_restarted_process_still_declares_its_tables_and_runs() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("d227.db");

    let first = ferrodb(
        &db,
        "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
         BEGIN AGENT SESSION AS 'restock-agent' RUN 'r1';\n\
         INSERT INTO inv VALUES (1, 10);\n\
         MERGE;\n",
    );
    assert!(first.contains("Clean"), "premise failed: the merge did not come back Clean:\n{first}");
    let after_first = decode_log_alone(&db);
    assert!(
        declares_table(&after_first, "inv") && declares_agent(&after_first, "restock-agent"),
        "premise failed: the first process's clean exit did not declare the table and the run: \
         tables {:?}, runs {:?}",
        after_first.schema_changes,
        after_first.runs
    );

    ferrodb(&db, "INSERT INTO inv VALUES (2, 20);\n");
    let after_second = decode_log_alone(&db);
    assert!(
        declares_table(&after_second, "inv"),
        "after a restart, the exit checkpoint declared no table: {:?}",
        after_second.schema_changes
    );
    assert!(
        !declares_agent(&after_second, "restock-agent"),
        "a process that bound no run re-declared one it never saw: an open or a checkpoint is re-declaring every \
         run ever interned (the D227 run half, withdrawn at 09:44Z): {:?}",
        after_second.runs
    );
}
