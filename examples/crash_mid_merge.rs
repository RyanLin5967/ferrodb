//! D8 harness — a process that merges, and can be killed part-way through publishing.
//!
//! Run by `tests/integration_crash_safety.rs`, which spawns it with `FERRODB_CRASH_AFTER_ROWS`
//! set to a row index. The merge publishes every row in one transaction; the crash point sits
//! inside that loop, so the process dies with the transaction open and some rows written.
//!
//! Usage: `crash_mid_merge <db-path> <phase>` where phase is `seed` or `merge`.

use std::path::Path;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::wal::recovery::{open_recovered, OpenedDatabase};

/// The three rows a merge will publish. Row 1 is the sentinel the test reads back.
const ROWS: [(i32, i32); 3] = [(1, 100), (2, 200), (3, 300)];
/// What the agent sets each row to. Distinct from the seed so a partial apply is visible.
const MERGED: i32 = 999;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: crash_mid_merge <db-path> <seed|merge>");
        std::process::exit(2);
    }
    let path = args[1].clone();
    let phase = args[2].clone();

    // Single-writer lock, taken before the file is opened. Two processes on one database both build
    // an ArenaPageStore from the same checkpoint and hand the same pages to different branches, and
    // every such page still passes its checksum - so refusing here is the only detection point.
    //
    // Held for the whole run: `_db_lock` releases on the way out, including on an early return.
    let _db_lock = ferrodb::storage::db_lock::DbLock::acquire(std::path::Path::new(&path))
        .unwrap_or_else(|e| { eprintln!("crash_mid_merge: {e}"); std::process::exit(1); });

    // ─── MEASUREMENT CHANGE, D204 ────────────────────────────────────────────────────────────────
    // This harness is the instrument behind `tests/integration_crash_safety.rs`, so the change is
    // stated here and not only in a commit message.
    //
    // BEFORE: it opened the file itself, ran `recover`, and never rebuilt an index. `let _ =
    // recovered;` discarded the one signal that a rebuild was owed. The comment above it read
    // "Same opening sequence the CLI uses, so recovery is the real one", and that was false: the
    // CLI rebuilt.
    //
    // AFTER: `open_recovered`, the function the CLI and pgserver also call. A phase that opens a
    // database whose log holds a data record (`merge` and `read`, never a first `seed`) now rebuilds
    // every index from the recovered heap and checkpoints, before it runs anything. D216 narrowed
    // "not empty" to "holds a data record". `seed` ends in `flush_all`, not a checkpoint, so its
    // rows are still in the log that `merge` opens. `read` rebuilds when the merge before it left a
    // data record, which a merge that crashed before its first write does not.
    //
    // WHAT THAT CAN MOVE, by reading the source (not by a run): nothing the test reads.
    // - `read` reports `SELECT id, qty FROM inventory;`, a sequential scan, so its STATE does not
    //   depend on the trees.
    // - `merge`'s `UPDATE … WHERE id = k` does read the primary index. `seed` already ended in
    //   `flush_all`, so those trees were on disk before; now they are rebuilt from the same rows.
    // - The checkpoint truncates the log before the merge begins, so `read`'s recovery replays only
    //   the merge's own records: less to redo, and the same open transaction to undo.
    // ─────────────────────────────────────────────────────────────────────────────────────────────
    let OpenedDatabase { bp, txn, mut catalog, .. } = open_recovered(Path::new(&path), &_db_lock)
        .unwrap_or_else(|e| panic!("crash_mid_merge: {e}"));

    let runtime = Arc::new(AgentRuntime::new());
    let mut session = Session::with_runtime(runtime);

    let exec = |sql: &str, s: &mut Session, cat: &mut Catalog| -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert!(parser.errors.is_empty(), "parse errors in: {sql}");
        run(stmts.remove(0), cat, bp.clone(), txn.clone(), s).unwrap_or_else(|e| {
            eprintln!("{sql} failed: {e}");
            std::process::exit(3);
        })
    };

    if phase == "seed" {
        exec("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut session, &mut catalog);
        for (id, qty) in ROWS {
            exec(
                &format!("INSERT INTO inventory VALUES ({id}, {qty});"),
                &mut session,
                &mut catalog,
            );
        }
        bp.flush_all().unwrap();
        println!("seeded");
        return;
    }

    if phase == "read" {
        // Read every row back after recovery and print them for the test to parse.
        let out = exec("SELECT id, qty FROM inventory;", &mut session, &mut catalog);
        let mut got: Vec<(i32, i32)> = match out {
            Outcome::Rows(rows) => rows
                .iter()
                .filter_map(|r| match (r.first(), r.get(1)) {
                    (
                        Some(ferrodb::catalog::column::Value::Integer(a)),
                        Some(ferrodb::catalog::column::Value::Integer(b)),
                    ) => Some((*a, *b)),
                    _ => None,
                })
                .collect(),
            _ => {
                eprintln!("expected rows");
                std::process::exit(4);
            }
        };
        got.sort_unstable();
        let rendered: Vec<String> = got.iter().map(|(a, b)| format!("{a}:{b}")).collect();
        println!("STATE {}", rendered.join(","));
        return;
    }

    // phase == "merge": one agent changes every row, then merges. The crash point is inside the
    // publish loop, so the process can die with some rows applied and the transaction open.
    exec("BEGIN AGENT SESSION AS 'crash-agent' RUN 'r_c';", &mut session, &mut catalog);
    for (id, _) in ROWS {
        exec(
            &format!("UPDATE inventory SET qty = {MERGED} WHERE id = {id};"),
            &mut session,
            &mut catalog,
        );
    }
    exec("MERGE;", &mut session, &mut catalog);
    bp.flush_all().unwrap();
    println!("merged");
}
