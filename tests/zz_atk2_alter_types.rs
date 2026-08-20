//! I19 attack pass 2 — the TYPE and LAYOUT space of a refused `ALTER TABLE`.
//!
//! Written in a fresh worktree to try to FALSIFY: "an ALTER that is refused leaves the table
//! exactly as it was — the same raw heap bytes for every live tuple, the same catalog shape, the
//! same values, the same primary-index answers, the same change-feed DDL records — and that holds
//! after a checkpoint, a flush and a reopen into a fresh buffer pool."
//!
//! The committed suite covers ADD COLUMN of a BIGINT and INTEGER -> BIGINT. This one covers
//! INTEGER -> DECIMAL and BIGINT -> DECIMAL (digit text: the widest growth in the allowlist),
//! VARCHAR(n) -> VARCHAR(m), the null-bitmap 8-column boundary, 9/16/17-column tables,
//! FLOAT/BOOLEAN/TIMESTAMP neighbours, a heap spread over many pages with the oversized row in the
//! MIDDLE of scan order, tombstones, NULLs in the retyped column, secondary + full-text indexes,
//! and the time-travel heap.
//!
//! Every sweep asserts it actually produced a refusal, and prints its refusal/success counts.

use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::{DataType, Value};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn db() -> Db {
    open_at(tempfile::tempdir().unwrap())
}

fn open_at(dir: tempfile::TempDir) -> Db {
    let path = dir.path().join("alter.db");
    let fresh = !path.exists();
    let file =
        std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = if fresh {
        Catalog::create(bp.clone()).unwrap()
    } else {
        Catalog::open(bp.clone(), 1).unwrap()
    };
    let wal = Arc::new(WalManager::new(dir.path().join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { dir, catalog, wal, bp, txn, runtime, session }
}

impl Db {
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                p.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        let mut session =
            std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out =
            run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut session);
        self.session = session;
        out
    }

    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    /// A read that must not panic. The original defect panicked the reading thread inside
    /// `Tuple::deserialize`, so it is caught rather than allowed to abort the process.
    fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.try_sql(sql)))
            .map_err(|_| format!("`{sql}` PANICKED the reading thread"))?;
        match out {
            Ok(Outcome::Rows(r)) => Ok(r),
            Ok(_) => Err(format!("`{sql}` did not return rows")),
            Err(e) => Err(format!("`{sql}` errored: {e}")),
        }
    }

    fn heap_at(&self, root: u32) -> Vec<(u32, u16, Vec<u8>)> {
        if root == 0 {
            return Vec::new();
        }
        HeapFileManager::open(root, self.bp.clone())
            .scan()
            .map(|r| match r {
                Ok((rid, t)) => (rid.page_id, rid.slot_num, t.data),
                Err(e) => (u32::MAX, u16::MAX, e.to_string().into_bytes()),
            })
            .collect()
    }

    /// Every DDL record with its full payload, so the retained declaration's shape is visible.
    fn feed_full(&self) -> Vec<String> {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        ferrodb::replication::logical::LogicalDecoder::new(&self.catalog)
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode")
            .schema_changes
            .iter()
            .map(|(l, t, c)| format!("lsn={l:?} {t} {c:?}"))
            .collect()
    }

    fn feed(&self) -> Vec<String> {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        ferrodb::replication::logical::LogicalDecoder::new(&self.catalog)
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode")
            .schema_changes
            .iter()
            .map(|(_, t, c)| format!("{t}:{c:?}"))
            .collect()
    }
}

/// Everything that has to be identical for a refusal to have changed nothing.
#[derive(Debug, PartialEq, Clone)]
struct Snap {
    shape: Vec<(String, DataType, bool)>,
    roots: (u32, u32, u32),
    idx: Vec<(String, u32)>,
    ft: Vec<(String, u32)>,
    heap: Vec<(u32, u16, Vec<u8>)>,
    tt: Vec<(u32, u16, Vec<u8>)>,
    queries: Vec<Result<Vec<Vec<Value>>, String>>,
    by_key: Vec<(String, String)>,
    idx_answers: Vec<(String, String)>,
    feed: Vec<String>,
}

fn snap(d: &mut Db, table: &str, queries: &[&str], keys: &[Value], probes: &[(Value, Value)]) -> Snap {
    let e = d.catalog.get_table(table).unwrap_or_else(|| panic!("no table {table}")).clone();
    let shape =
        e.schema.columns.iter().map(|c| (c.name.clone(), c.data_type.clone(), c.nullable)).collect();
    let heap = d.heap_at(e.first_directory_page_id);
    let tt = d.heap_at(e.time_travel_root);
    let primary = BPlusTreeManager::<Value, ferrodb::storage::heap_file_manager::RecordId>::open(
        e.primary_index_root,
        d.bp.clone(),
    );
    let by_key = keys
        .iter()
        .map(|k| (format!("{k:?}"), format!("{:?}", primary.search(k))))
        .collect();
    // Secondary and full-text trees are both `<(Value, Value), ()>` keyed (value, primary key).
    let mut idx_answers = Vec::new();
    for ind in e.indexes.iter() {
        let t = BPlusTreeManager::<(Value, Value), ()>::open(ind.root_page_id, d.bp.clone());
        for p in probes {
            idx_answers.push((
                format!("ix {} {:?}", ind.column_name, p),
                format!("{:?}", t.search(&(p.0.clone(), p.1.clone()))),
            ));
        }
    }
    for ind in e.fulltext_indexes.iter() {
        let t = BPlusTreeManager::<(Value, Value), ()>::open(ind.root_page_id, d.bp.clone());
        for p in probes {
            idx_answers.push((
                format!("ft {} {:?}", ind.column_name, p),
                format!("{:?}", t.search(&(p.0.clone(), p.1.clone()))),
            ));
        }
    }
    let feed = d.feed();
    let mut answers = Vec::new();
    for q in queries {
        answers.push(d.rows(q));
    }
    Snap {
        shape,
        roots: (e.first_directory_page_id, e.primary_index_root, e.time_travel_root),
        idx: e.indexes.iter().map(|i| (i.column_name.clone(), i.root_page_id)).collect(),
        ft: e.fulltext_indexes.iter().map(|i| (i.column_name.clone(), i.root_page_id)).collect(),
        heap,
        tt,
        queries: answers,
        by_key,
        idx_answers,
        feed,
    }
}

/// The first difference between two snapshots, described well enough to act on.
fn diff(a: &Snap, b: &Snap) -> Option<String> {
    if a.shape != b.shape {
        return Some(format!("CATALOG SHAPE changed: {:?} -> {:?}", a.shape, b.shape));
    }
    if a.roots != b.roots {
        return Some(format!(
            "ROOTS (dir, primary, time_travel) changed: {:?} -> {:?}",
            a.roots, b.roots
        ));
    }
    if a.idx != b.idx {
        return Some(format!("SECONDARY INDEX roots changed: {:?} -> {:?}", a.idx, b.idx));
    }
    if a.ft != b.ft {
        return Some(format!("FULLTEXT INDEX roots changed: {:?} -> {:?}", a.ft, b.ft));
    }
    for (name, x, y) in [("HEAP", &a.heap, &b.heap), ("TIME-TRAVEL HEAP", &a.tt, &b.tt)] {
        if x.len() != y.len() {
            let rx: Vec<_> = x.iter().map(|r| (r.0, r.1)).collect();
            let ry: Vec<_> = y.iter().map(|r| (r.0, r.1)).collect();
            return Some(format!(
                "{name} live tuple count {} -> {}; rids {rx:?} -> {ry:?}",
                x.len(),
                y.len()
            ));
        }
        for (p, q) in x.iter().zip(y.iter()) {
            if (p.0, p.1) != (q.0, q.1) {
                return Some(format!("{name} rid moved: {:?} -> {:?}", (p.0, p.1), (q.0, q.1)));
            }
            if p.2 != q.2 {
                let at = p.2.iter().zip(q.2.iter()).position(|(m, n)| m != n);
                return Some(format!(
                    "{name} BYTES CHANGED at rid {:?}: len {} -> {}, first differing byte index \
                     {at:?}, before[..48]={:?}, after[..48]={:?}",
                    (p.0, p.1),
                    p.2.len(),
                    q.2.len(),
                    &p.2[..p.2.len().min(48)],
                    &q.2[..q.2.len().min(48)]
                ));
            }
        }
    }
    if a.queries != b.queries {
        return Some(format!("QUERY ANSWERS changed: {:?} -> {:?}", a.queries, b.queries));
    }
    if a.by_key != b.by_key {
        return Some(format!("PRIMARY INDEX answers changed: {:?} -> {:?}", a.by_key, b.by_key));
    }
    if a.idx_answers != b.idx_answers {
        return Some(format!(
            "SECONDARY/FULLTEXT INDEX answers changed: {:?} -> {:?}",
            a.idx_answers, b.idx_answers
        ));
    }
    if a.feed != b.feed {
        return Some(format!("CHANGE FEED DDL records changed: {:?} -> {:?}", a.feed, b.feed));
    }
    None
}

struct Sweep {
    refusals: usize,
    successes: usize,
    unbuildable: usize,
    refusing_widths: Vec<usize>,
}

/// For each width: build the fixture, snapshot, run `alter`. On a refusal, compare in memory and
/// then again after checkpoint + flush + sync + reopen into a fresh buffer pool and catalog.
#[allow(clippy::too_many_arguments)]
fn run_sweep(
    label: &str,
    table: &str,
    pads: std::ops::RangeInclusive<usize>,
    build: &dyn Fn(&mut Db, usize) -> bool,
    alter: &str,
    queries: &[&str],
    keys: &[Value],
    probes: &[(Value, Value)],
    max_refusals: usize,
) -> Sweep {
    let mut s = Sweep { refusals: 0, successes: 0, unbuildable: 0, refusing_widths: Vec::new() };
    for pad in pads {
        if s.refusals >= max_refusals {
            break;
        }
        let (dir, before, err) = {
            let mut d = db();
            if !build(&mut d, pad) {
                s.unbuildable += 1;
                continue;
            }
            let before = snap(&mut d, table, queries, keys, probes);
            let Err(e) = d.try_sql(alter) else {
                s.successes += 1;
                continue;
            };
            s.refusals += 1;
            s.refusing_widths.push(pad);
            println!(
                "  {label}: pad={pad} REFUSED. live tuples={}, distinct heap pages={}, \
                 time-travel tuples={}, feed={:?}\n    refusal: {e}",
                before.heap.len(),
                {
                    let mut ps: Vec<u32> = before.heap.iter().map(|r| r.0).collect();
                    ps.sort_unstable();
                    ps.dedup();
                    ps.len()
                },
                before.tt.len(),
                before.feed,
            );
            if !before.idx_answers.is_empty() {
                println!("    index probes: {:?}", before.idx_answers);
            }
            println!("    primary-index probes: {:?}", before.by_key);
            let after = snap(&mut d, table, queries, keys, probes);
            if let Some(what) = diff(&before, &after) {
                panic!(
                    "{label}: pad={pad}: a REFUSED `{alter}` changed the table IN MEMORY.\n  \
                     refusal was: {e}\n  {what}"
                );
            }
            d.txn.checkpoint().expect("checkpoint");
            d.bp.flush_all().unwrap();
            d.bp.disk_manager.sync().unwrap();
            (d.dir, before, e.to_string())
        };
        let mut r = open_at(dir);
        let after = snap(&mut r, table, queries, keys, probes);
        // The change feed is compared separately across the reopen. A checkpoint truncates the
        // WAL, so a *pre-existing* DDL record can vanish from what the decoder sees for reasons
        // that have nothing to do with the refused ALTER — see
        // `control_a_successful_add_column_across_a_checkpoint_with_no_refused_alter`, which
        // measures that on its own. What must hold here is the direction that the refusal could
        // cause: the reopened feed may not contain a record the pre-ALTER feed did not.
        let (mut b, mut a) = (before.clone(), after.clone());
        b.feed.clear();
        a.feed.clear();
        if let Some(what) = diff(&b, &a) {
            panic!(
                "{label}: pad={pad}: a REFUSED `{alter}` changed the table DURABLY — the \
                 difference survived checkpoint + flush + sync + a fresh buffer pool.\n  \
                 refusal was: {err}\n  {what}"
            );
        }
        for rec in after.feed.iter() {
            assert!(
                before.feed.contains(rec),
                "{label}: pad={pad}: a REFUSED `{alter}` put a DDL record on the change feed that \
                 was not there before it: {rec:?} (before={:?}, after reopen={:?})",
                before.feed,
                after.feed
            );
        }
    }
    println!(
        "{label}: refusals={} at widths {:?}, successes={}, unbuildable={}",
        s.refusals, s.refusing_widths, s.successes, s.unbuildable
    );
    assert!(
        s.refusals > 0,
        "{label}: the sweep produced NO refusal, so it measured nothing. successes={}, \
         unbuildable={}",
        s.successes,
        s.unbuildable
    );
    s
}

const KEYS2: [Value; 2] = [Value::Integer(1), Value::Integer(2)];

// ---------------------------------------------------------------------------------------------
// INTEGER -> DECIMAL and BIGINT -> DECIMAL: digit text, the widest growth in the allowlist
// ---------------------------------------------------------------------------------------------

#[test]
fn int_to_decimal_refused_changes_nothing() {
    run_sweep(
        "INTEGER->DECIMAL",
        "t",
        4008..=4040,
        &|d, pad| {
            d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(4100));");
            d.sql("INSERT INTO t VALUES (1, 7, 'small');");
            d.try_sql(&format!("INSERT INTO t VALUES (2, 1000000000, '{}');", "x".repeat(pad)))
                .is_ok()
        },
        "ALTER TABLE t ALTER COLUMN n TYPE DECIMAL;",
        &["SELECT id, n FROM t;", "SELECT id, a FROM t;"],
        &KEYS2,
        &[],
        3,
    );
}

#[test]
fn bigint_to_decimal_refused_changes_nothing() {
    run_sweep(
        "BIGINT->DECIMAL",
        "t",
        4000..=4040,
        &|d, pad| {
            d.sql("CREATE TABLE t (id INTEGER NOT NULL, n BIGINT, a VARCHAR(4100));");
            d.sql("INSERT INTO t VALUES (1, 7, 'small');");
            d.try_sql(&format!(
                "INSERT INTO t VALUES (2, 9223372036854775807, '{}');",
                "x".repeat(pad)
            ))
            .is_ok()
        },
        "ALTER TABLE t ALTER COLUMN n TYPE DECIMAL;",
        &["SELECT id, n FROM t;", "SELECT id, a FROM t;"],
        &KEYS2,
        &[],
        3,
    );
}

/// `VarcharWider` claims to move no byte at all. Two things measured: it never refuses at any
/// width, and after the alter SUCCEEDS the heap bytes are still identical.
#[test]
fn varchar_widening_refuses_nothing_and_moves_no_byte() {
    let mut successes = 0;
    let mut refusals = Vec::new();
    for pad in [8usize, 1000, 4000, 4020, 4028, 4029, 4030, 4031, 4032] {
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(64), b VARCHAR(4100));");
        d.sql("INSERT INTO t VALUES (1, 'ab', 'small');");
        if d
            .try_sql(&format!("INSERT INTO t VALUES (2, 'cd', '{}');", "x".repeat(pad)))
            .is_err()
        {
            continue;
        }
        let q = ["SELECT id, a, b FROM t;"];
        let before = snap(&mut d, "t", &q, &KEYS2, &[]);
        match d.try_sql("ALTER TABLE t ALTER COLUMN a TYPE VARCHAR(4000);") {
            Err(e) => {
                refusals.push((pad, e.to_string()));
                let after = snap(&mut d, "t", &q, &KEYS2, &[]);
                if let Some(what) = diff(&before, &after) {
                    panic!("VARCHAR widening: pad={pad}: a REFUSED alter changed the table: {what}");
                }
            }
            Ok(_) => {
                successes += 1;
                let after = snap(&mut d, "t", &q, &KEYS2, &[]);
                assert_eq!(
                    before.heap, after.heap,
                    "pad={pad}: VARCHAR(n)->VARCHAR(m) is documented to move no byte, but the \
                     heap bytes changed"
                );
                assert_eq!(before.tt, after.tt, "pad={pad}: the time-travel heap changed");
            }
        }
    }
    println!("VARCHAR widening: successes={successes}, refusals={refusals:?}");
    assert!(successes > 0, "the sweep never once ran the alter");
}

// ---------------------------------------------------------------------------------------------
// Layout: the null bitmap's 8-column boundary, and 9 / 16 / 17-column tables
// ---------------------------------------------------------------------------------------------

/// Eight columns -> nine. The bitmap grows from one byte to two, so **every** column after it
/// shifts, on top of whatever the appended column costs.
#[test]
fn add_column_across_the_eight_column_boundary() {
    run_sweep(
        "ADD COLUMN 8->9 (bitmap 1->2 bytes)",
        "t",
        3990..=4040,
        &|d, pad| {
            d.sql(
                "CREATE TABLE t (id INTEGER NOT NULL, c1 INTEGER, c2 INTEGER, c3 INTEGER, \
                 c4 INTEGER, c5 INTEGER, c6 INTEGER, a VARCHAR(4100));",
            );
            d.sql("INSERT INTO t VALUES (1, 1, 2, 3, 4, 5, 6, 'small');");
            d.try_sql(&format!(
                "INSERT INTO t VALUES (2, 1, 2, 3, 4, 5, 6, '{}');",
                "x".repeat(pad)
            ))
            .is_ok()
        },
        "ALTER TABLE t ADD COLUMN c7 VARCHAR(20);",
        &["SELECT id, c6 FROM t;", "SELECT id, a FROM t;"],
        &KEYS2,
        &[],
        3,
    );
}

fn wide_table_ddl(ncols: usize) -> (String, String, String) {
    // id, then ncols-2 INTEGERs, then the trailing VARCHAR that carries the padding.
    let mut cols = vec!["id INTEGER NOT NULL".to_string()];
    let mut vals_small = vec!["1".to_string()];
    let mut vals_big = vec!["2".to_string()];
    for i in 1..=(ncols - 2) {
        cols.push(format!("c{i} INTEGER"));
        vals_small.push("7".to_string());
        vals_big.push("1000000000".to_string());
    }
    cols.push("a VARCHAR(4100)".to_string());
    (
        format!("CREATE TABLE t ({});", cols.join(", ")),
        format!("INSERT INTO t VALUES ({}, 'small');", vals_small.join(", ")),
        format!("INSERT INTO t VALUES ({}, '{{PAD}}');", vals_big.join(", ")),
    )
}

#[test]
fn retype_in_the_middle_of_nine_sixteen_and_seventeen_column_tables() {
    for ncols in [9usize, 16, 17] {
        let (ddl, ins_small, ins_big) = wide_table_ddl(ncols);
        let mid = (ncols - 2) / 2;
        let alter = format!("ALTER TABLE t ALTER COLUMN c{mid} TYPE DECIMAL;");
        let fixed = 4 * (ncols - 2) + 32;
        let hi = 4069usize.saturating_sub(fixed);
        run_sweep(
            &format!("{ncols}-column table, retype c{mid} INTEGER->DECIMAL"),
            "t",
            hi.saturating_sub(30)..=hi,
            &|d, pad| {
                d.sql(&ddl);
                d.sql(&ins_small);
                d.try_sql(&ins_big.replace("{PAD}", &"x".repeat(pad))).is_ok()
            },
            &alter,
            &[
                &format!("SELECT id, c{mid} FROM t;"),
                "SELECT id, a FROM t;",
            ],
            &KEYS2,
            &[],
            2,
        );
    }
}

/// FLOAT (8-align), BOOLEAN (1-align) and TIMESTAMP (8-align) either side of the retyped column,
/// so the conversion changes the alignment padding of its neighbours and not only its own width.
#[test]
fn mixed_alignment_neighbours() {
    run_sweep(
        "FLOAT/BOOLEAN/TIMESTAMP neighbours, INTEGER->DECIMAL",
        "t",
        3980..=4040,
        &|d, pad| {
            // `CAST(... AS TIMESTAMP)` is not accepted inside a VALUES list, so TIMESTAMP is
            // exercised as NULL — the NULL arm of `Tuple::serialize` still lays down an 8-byte
            // 8-aligned slot for it — and a real BIGINT provides the non-null 8-aligned neighbour.
            d.sql(
                "CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, f FLOAT, ok BOOLEAN, \
                 big BIGINT, ts TIMESTAMP, a VARCHAR(4100));",
            );
            d.sql("INSERT INTO t VALUES (1, 7, 1.5, TRUE, 111, NULL, 'small');");
            d.try_sql(&format!(
                "INSERT INTO t VALUES (2, 1000000000, 2.5, FALSE, 9223372036854775807, NULL, \
                 '{}');",
                "x".repeat(pad)
            ))
            .is_ok()
        },
        "ALTER TABLE t ALTER COLUMN n TYPE DECIMAL;",
        &["SELECT id, n, f, ok, big, ts FROM t;", "SELECT id, a FROM t;"],
        &KEYS2,
        &[],
        3,
    );
}

// ---------------------------------------------------------------------------------------------
// Many pages, tombstones, NULLs, indexes, the time-travel heap
// ---------------------------------------------------------------------------------------------

/// 60 rows over ~20 pages with the oversized row in the MIDDLE of scan order, so a rewrite that
/// wrote before it checked would have converted roughly half the heap.
#[test]
fn many_pages_with_the_oversized_row_in_the_middle() {
    let keys: Vec<Value> = (1..=61).map(Value::Integer).collect();
    run_sweep(
        "60 rows over many pages, oversized row in the middle",
        "t",
        4008..=4040,
        &|d, pad| {
            d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(4100));");
            let body = "y".repeat(1300);
            for id in 1..=30 {
                d.sql(&format!("INSERT INTO t VALUES ({id}, {id}, '{body}');"));
            }
            let ok = d
                .try_sql(&format!(
                    "INSERT INTO t VALUES (61, 1000000000, '{}');",
                    "x".repeat(pad)
                ))
                .is_ok();
            for id in 31..=60 {
                d.sql(&format!("INSERT INTO t VALUES ({id}, {id}, '{body}');"));
            }
            ok
        },
        "ALTER TABLE t ALTER COLUMN n TYPE DECIMAL;",
        &["SELECT id, n FROM t;"],
        &keys,
        &[],
        2,
    );
}

/// Tombstones keep their slot, and a NULL in the retyped column takes a different width in the
/// old type than in the new one.
#[test]
fn tombstones_and_nulls_in_the_retyped_column() {
    let keys = [Value::Integer(1), Value::Integer(2), Value::Integer(3), Value::Integer(4)];
    run_sweep(
        "tombstones + NULL in the retyped column",
        "t",
        4008..=4040,
        &|d, pad| {
            d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(4100));");
            d.sql("INSERT INTO t VALUES (1, 7, 'small');");
            d.sql("INSERT INTO t VALUES (2, NULL, 'nulls');");
            d.sql("INSERT INTO t VALUES (4, 9, 'doomed');");
            d.sql("DELETE FROM t WHERE id = 4;");
            d.try_sql(&format!("INSERT INTO t VALUES (3, 1000000000, '{}');", "x".repeat(pad)))
                .is_ok()
        },
        "ALTER TABLE t ALTER COLUMN n TYPE DECIMAL;",
        &["SELECT id, n FROM t;", "SELECT id, a FROM t;"],
        &keys,
        &[],
        3,
    );
}

/// A secondary index, a full-text index, and a time-travel heap with superseded versions in it.
#[test]
fn secondary_and_fulltext_indexes_and_the_time_travel_heap() {
    let keys = [Value::Integer(1), Value::Integer(2), Value::Integer(3)];
    let probes = [
        (Value::Integer(7), Value::Integer(1)),
        (Value::Integer(8), Value::Integer(2)),
        (Value::Integer(1000000000), Value::Integer(3)),
        (Value::Varchar("alpha".to_string()), Value::Integer(1)),
        (Value::Varchar("beta".to_string()), Value::Integer(2)),
        (Value::Varchar("gamma".to_string()), Value::Integer(3)),
    ];
    run_sweep(
        "secondary + fulltext index, populated time-travel heap",
        "inv",
        4000..=4040,
        &|d, pad| {
            d.sql(
                "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER, body VARCHAR(64), \
                 a VARCHAR(4100));",
            );
            d.sql("CREATE INDEX ix ON inv (qty);");
            d.sql("CREATE FULLTEXT INDEX fx ON inv (body);");
            d.sql("INSERT INTO inv VALUES (1, 7, 'alpha beta', 'small');");
            d.sql("INSERT INTO inv VALUES (2, 8, 'beta gamma', 'small2');");
            // Two updates on one row, so the time-travel heap holds superseded versions.
            d.sql("UPDATE inv SET qty = 77 WHERE id = 1;");
            d.sql("UPDATE inv SET qty = 7 WHERE id = 1;");
            d.try_sql(&format!(
                "INSERT INTO inv VALUES (3, 1000000000, 'gamma delta', '{}');",
                "x".repeat(pad)
            ))
            .is_ok()
        },
        "ALTER TABLE inv ALTER COLUMN qty TYPE DECIMAL;",
        &[
            "SELECT id, qty FROM inv;",
            "SELECT id FROM inv WHERE qty = 7;",
            "SELECT id, body FROM inv;",
        ],
        &keys,
        &probes,
        2,
    );
}

/// A refusal after a *successful* alter. The table is already one ADD COLUMN past its original
/// shape, so a refusal that reverted or half-applied anything would show up as the first alter
/// coming undone rather than as the second one landing.
#[test]
fn a_refusal_after_a_successful_alter_leaves_the_earlier_one_intact() {
    run_sweep(
        "ADD COLUMN (succeeds) then INTEGER->DECIMAL (refused)",
        "t",
        3990..=4040,
        &|d, pad| {
            d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(4100));");
            d.sql("INSERT INTO t VALUES (1, 7, 'small');");
            if d
                .try_sql(&format!("INSERT INTO t VALUES (2, 1000000000, '{}');", "x".repeat(pad)))
                .is_err()
            {
                return false;
            }
            // Must succeed at this width, or the fixture is not testing what it says.
            if d.try_sql("ALTER TABLE t ADD COLUMN tag VARCHAR(8);").is_err() {
                return false;
            }
            d.sql("UPDATE t SET tag = 'k' WHERE id = 1;");
            true
        },
        "ALTER TABLE t ALTER COLUMN n TYPE DECIMAL;",
        &["SELECT id, n, tag FROM t;", "SELECT id, a FROM t;"],
        &KEYS2,
        &[],
        3,
    );
}

/// **Control, no refused ALTER anywhere.** Does the change-feed DDL record for a *successful*
/// `ADD COLUMN` survive a checkpoint, a flush and a reopen on its own? Needed because the durable
/// comparison in `run_sweep` saw that record disappear, and a difference a refusal did not cause
/// is not a refusal defect.
#[test]
fn control_a_successful_add_column_across_a_checkpoint_with_no_refused_alter() {
    let (dir, before) = {
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(4100));");
        d.sql("INSERT INTO t VALUES (1, 7, 'small');");
        d.sql(&format!("INSERT INTO t VALUES (2, 1000000000, '{}');", "x".repeat(4022)));
        d.sql("ALTER TABLE t ADD COLUMN tag VARCHAR(8);");
        d.sql("UPDATE t SET tag = 'k' WHERE id = 1;");
        let f = d.feed();
        d.txn.checkpoint().expect("checkpoint");
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
        (d.dir, f)
    };
    let r = open_at(dir);
    let after = r.feed();
    println!("CONTROL: feed before checkpoint = {before:?}");
    println!("CONTROL: feed after checkpoint + reopen = {after:?}");
    println!("CONTROL: full records after reopen = {:?}", r.feed_full());
    // The question the asymmetry raises: `CreateTable` survived the truncation and `AddColumn` did
    // not, so does the retained `CreateTable` carry the post-ALTER shape? If it names `tag`, no
    // shape information was lost and the missing event is a presentation difference, not a hole.
    // MEASURED, and it is *not* a refusal defect — this control has no refused ALTER in it:
    //   before = ["t:CreateTable", "t:AddColumn { column: \"tag\" }"]
    //   after  = ["t:CreateTable"]
    // Whether shape information is actually lost is NOT settled here: `SchemaChange::CreateTable`
    // renders with no payload at all in this projection, so the retained declaration's columns are
    // not visible to this instrument. Recorded as an out-of-scope observation, not a finding.
    assert!(
        after.len() <= before.len() && after.iter().all(|r| before.contains(r)),
        "the reopen INVENTED a DDL record: before={before:?}, after={after:?}"
    );
}
