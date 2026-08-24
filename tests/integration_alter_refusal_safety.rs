//! I19 — a **refused** `ALTER TABLE` must leave the table exactly as it was.
//!
//! # The defect these tests were written against
//!
//! `review-B11.md` finding 1, re-derived here. `rewrite_heap` converted the heap one tuple at a
//! time with no size precheck, and `HeapFileManager::update`'s `NotEnoughSpace` branch deletes the
//! slot and unpins the page **dirty** before attempting the insert that replaces it. So a widened
//! tuple that fits no page took the row with it: the `?` unwound with the row already gone,
//! `Catalog::finish` never ran, and the rows already converted sat on disk in the NEW shape under
//! the OLD catalog. The rewrite is unlogged, so recovery could not repair it. Three measured
//! outcomes, all reproduced below: a row silently disappears; a retype leaves the table
//! permanently unqueryable (a panic in `Tuple::deserialize`, surviving checkpoint/flush/reopen);
//! or reads return silently wrong numbers.
//!
//! # What "exactly as it was" is measured as
//!
//! Not "the same number of rows" — the raw heap bytes of every live tuple, keyed by `RecordId`,
//! plus the catalog shape, plus the values a `SELECT` returns, plus what the primary index answers
//! for every key. A version header, a relocation or a single converted column would all show up.
//! Row counts were what made the original defect survive review: the *first* form of it loses one
//! row out of two, and a test that counts rows after an error it expected reads as a pass.
//!
//! The exit criterion is stricter still and is
//! [`a_refused_alter_survives_a_checkpoint_a_flush_and_a_fresh_buffer_pool`]: the comparison is
//! made after `checkpoint()`, `flush_all()`, `sync()` and a reopen into a **new**
//! `BufferPoolManager` and `Catalog`, because the damage the review measured was durable and an
//! in-memory-only check would have missed half of it.
//!
//! # Anti-vacuity
//!
//! Every fixture here is a width sweep, and each sweep **asserts that it actually produced the
//! condition under test** — a refusal, and in [`the_precheck_does_not_refuse_a_row_that_fits`] a
//! success as well. A sweep that silently found no refusing width would otherwise pass while
//! measuring nothing, which is exactly how the original defect got past a green suite.
//!
//! Ported from the reviewer's scratch reproductions (`zz_verify6.rs::v6_*`,
//! `zz_verify10.rs::t1_*`, `zz_verify_agent_claims.rs::v1_*`/`::v4_*`/`::v5_*`), which lived in a
//! throwaway `git archive` tree and were never committed to any branch.

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
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::storage::heap_page::MAX_TUPLE_SIZE;
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

/// Open the database in `dir`, creating it if it is not there.
///
/// Called a second time on the same directory to reopen — which is the point: the `BufferPoolManager`
/// and the `Catalog` are built fresh, so nothing the first handle cached can carry the answer.
fn open_at(dir: tempfile::TempDir) -> Db {
    open_maybe_recovering(dir, false)
}

/// Reopen the way the CLI does after a crash: build the `TxnManager`, run `recover`, and only then
/// open the catalog. Used by [`a_refused_alter_survives_a_crash_with_no_clean_flush`], where the
/// point is what the WAL replays rather than what was written through.
fn open_recovering(dir: tempfile::TempDir) -> Db {
    open_maybe_recovering(dir, true)
}

fn open_maybe_recovering(dir: tempfile::TempDir, recover: bool) -> Db {
    let path = dir.path().join("alter.db");
    let fresh = !path.exists();
    let file =
        std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("alter.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    if recover {
        ferrodb::wal::recovery::recover(&txn).expect("recover");
    }
    let catalog = if fresh {
        Catalog::create(bp.clone()).unwrap()
    } else {
        Catalog::open(bp.clone(), 1).unwrap()
    };
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
        let out = run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut session);
        self.session = session;
        out
    }

    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    /// A `SELECT` that must not panic. The defect's second form panics the reading thread inside
    /// `Tuple::deserialize`, so the read is caught rather than allowed to abort the test process
    /// with no comparison made.
    fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.try_sql(sql)))
            .map_err(|_| format!("`{sql}` PANICKED the reading thread"))?;
        match out {
            Ok(Outcome::Rows(r)) => Ok(r),
            Ok(_) => Err(format!("`{sql}` did not return rows")),
            Err(e) => Err(format!("`{sql}` errored: {e}")),
        }
    }

    /// Run one statement in a caller-supplied session, which is how an agent branch is driven.
    fn exec(&mut self, sql: &str, session: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "`{sql}` did not parse");
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), session)
    }

    fn shape(&self, table: &str) -> Vec<(String, DataType)> {
        self.catalog
            .get_table(table)
            .unwrap_or_else(|| panic!("no table {table}"))
            .schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone()))
            .collect()
    }

    /// The raw bytes of every live tuple, with the slot it lives in.
    ///
    /// This is the load-bearing measurement. Values decoded through a `SELECT` go through the
    /// catalog, so a heap converted under a schema the catalog does not have can read back as
    /// plausible numbers (the defect's third form). Bytes cannot.
    fn heap(&self, table: &str) -> Vec<(RecordId, Vec<u8>)> {
        let root = self.catalog.get_table(table).unwrap().first_directory_page_id;
        HeapFileManager::open(root, self.bp.clone())
            .scan()
            .map(|r| {
                let (rid, tuple) = r.unwrap();
                (rid, tuple.data)
            })
            .collect()
    }

    /// The DDL records the change feed carries, as `table:variant` strings.
    ///
    /// A refusal that had already logged the DDL record would leave every consumer told the column
    /// exists, which is the same lie as the lost row told to a different reader.
    fn schema_changes(&self) -> Vec<String> {
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

    /// What the primary index answers for each key, so a relocation that repointed the index —
    /// or failed to — is visible too.
    fn by_key(&self, table: &str, keys: &[Value]) -> Vec<(Value, Option<RecordId>)> {
        let root = self.catalog.get_table(table).unwrap().primary_index_root;
        let ix = BPlusTreeManager::<Value, RecordId>::open(root, self.bp.clone());
        keys.iter().map(|k| (k.clone(), ix.search(k).unwrap())).collect()
    }
}

/// Everything that has to be identical for a refusal to have changed nothing.
#[derive(Debug, PartialEq)]
struct Snapshot {
    shape: Vec<(String, DataType)>,
    heap: Vec<(RecordId, Vec<u8>)>,
    rows: Result<Vec<Vec<Value>>, String>,
    by_key: Vec<(Value, Option<RecordId>)>,
}

fn snapshot(d: &mut Db, table: &str, select: &str, keys: &[Value]) -> Snapshot {
    Snapshot {
        shape: d.shape(table),
        heap: d.heap(table),
        rows: d.rows(select),
        by_key: d.by_key(table, keys),
    }
}

/// The refusal has to be *this* refusal.
///
/// Any `Err` makes a table-unchanged assertion pass, including one raised for an unrelated reason
/// before the rewrite was ever reached — which would make every test in this file vacuous. So the
/// message is pinned: it must name the widening, the page limit, and the fact that nothing was
/// written.
fn assert_is_the_size_refusal(e: &FerroError, table: &str) {
    let msg = e.to_string();
    for needle in ["would widen", "bytes a tuple can occupy", "Nothing has been written", table] {
        assert!(
            msg.contains(needle),
            "the ALTER was refused, but not by the size precheck — no {needle:?} in: {msg}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// The three measured outcomes of finding 1
// ---------------------------------------------------------------------------------------------

/// **Form 1 — a row silently disappears.** `zz_verify6.rs::v6_*` and `::v4_*`.
///
/// An `ADD COLUMN` on a table holding one row within a couple of bytes of the page-tuple limit.
/// The statement reports failure; before the fix `SELECT id FROM t` then returned one row instead
/// of two, with no error at all.
#[test]
fn a_refused_add_column_leaves_every_row_byte_identical() {
    let mut refusals = 0;
    // 4034/4035 are the two widths at which the row still fits a page but the widened row does
    // not; the neighbours on either side are swept so the fixture cannot drift into vacuity
    // unnoticed.
    for pad in 4028usize..=4040 {
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(4100));");
        d.sql("INSERT INTO t VALUES (1, 'small');");
        let big = "x".repeat(pad);
        if d.try_sql(&format!("INSERT INTO t VALUES (2, '{big}');")).is_err() {
            continue; // the row itself does not fit a page; nothing to alter.
        }

        let before = snapshot(&mut d, "t", "SELECT id, a FROM t;", &[Value::Integer(1), Value::Integer(2)]);
        let feed_before = d.schema_changes();
        let res = d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);");
        let Err(e) = res else { continue }; // this width fits; covered by the sweep below.
        refusals += 1;

        // The data comparison comes first deliberately: a regression here should fail naming the
        // rows it lost, not the wording of a message.
        let after = snapshot(&mut d, "t", "SELECT id, a FROM t;", &[Value::Integer(1), Value::Integer(2)]);
        assert_eq!(
            before, after,
            "pad={pad}: a REFUSED `ALTER TABLE t ADD COLUMN note VARCHAR(20)` changed the table. \
             The statement reported failure ({e}), so nothing should have moved."
        );
        assert_eq!(
            feed_before,
            d.schema_changes(),
            "pad={pad}: a REFUSED ADD COLUMN still told the change feed the column exists"
        );
        assert_is_the_size_refusal(&e, "t");
    }
    assert!(
        refusals > 0,
        "no width in 4028..=4040 produced a refused ADD COLUMN, so this test measured nothing. \
         The tuple layout moved: recompute the window from MAX_TUPLE_SIZE ({MAX_TUPLE_SIZE})."
    );
}

/// **Form 2 — the table becomes permanently unqueryable.** `zz_verify_agent_claims.rs::v1_*`.
///
/// `INTEGER -> BIGINT` on a four-column table: the retyped column moves to an 8-byte boundary and
/// every column after it shifts, so the wide row grows by 4 bytes and crosses the limit. Before
/// the fix the small row was converted, the wide one deleted, and every subsequent read panicked
/// at `tuple.rs:239` with `range end index 140 out of range for slice of length 54`.
#[test]
fn a_refused_retype_leaves_every_row_byte_identical() {
    let mut refusals = 0;
    for pad in 2010usize..=2016 {
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
        d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
        let big = "x".repeat(pad);
        if d.try_sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');")).is_err() {
            continue;
        }

        let keys = [Value::Integer(1), Value::Integer(2)];
        let before = snapshot(&mut d, "t", "SELECT id, n FROM t;", &keys);
        let res = d.try_sql("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;");
        let Err(e) = res else { continue };
        refusals += 1;

        let after = snapshot(&mut d, "t", "SELECT id, n FROM t;", &keys);
        assert_eq!(
            before, after,
            "pad={pad}: a REFUSED `ALTER COLUMN n TYPE BIGINT` changed the table ({e})."
        );
        assert_is_the_size_refusal(&e, "t");
        // Named separately because it is the specific harm: the panic is what made the table
        // unqueryable rather than merely short a row, and `Snapshot` equality would be satisfied
        // by two identical panics.
        assert!(
            after.rows.is_ok(),
            "pad={pad}: the table is unreadable after a refused retype: {:?}",
            after.rows
        );
    }
    assert!(refusals > 0, "no width in 2010..=2016 produced a refused retype; the sweep is vacuous");
}

/// **Form 3 — reads return silently wrong numbers.** The review's third variant: `SELECT id,qty,x`
/// returned `[[1, 0, 1522159703584512]]` where it had returned `[[1,42,777],[2,43,888]]`.
///
/// Same mechanism as form 2, but the columns after the retyped one are fixed-width, so the
/// half-converted row decodes without going out of bounds and the wrong values are returned with
/// no error and no panic. This is the form no amount of error checking at the call site would have
/// caught.
#[test]
fn a_refused_retype_does_not_leave_a_readable_wrong_answer() {
    let mut refusals = 0;
    for pad in 4014usize..=4026 {
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, qty INTEGER, s VARCHAR(4100), x BIGINT);");
        let big = "x".repeat(pad);
        d.sql("INSERT INTO t VALUES (1, 42, 'small', 777);");
        if d.try_sql(&format!("INSERT INTO t VALUES (2, 43, '{big}', 888);")).is_err() {
            continue;
        }

        let keys = [Value::Integer(1), Value::Integer(2)];
        let before = snapshot(&mut d, "t", "SELECT id, qty, x FROM t;", &keys);
        assert_eq!(
            before.rows.as_ref().unwrap(),
            &vec![
                vec![Value::Integer(1), Value::Integer(42), Value::BigInt(777)],
                vec![Value::Integer(2), Value::Integer(43), Value::BigInt(888)],
            ],
            "pad={pad}: the fixture did not start from the values it claims to"
        );

        let res = d.try_sql("ALTER TABLE t ALTER COLUMN qty TYPE BIGINT;");
        let Err(e) = res else { continue };
        refusals += 1;

        let after = snapshot(&mut d, "t", "SELECT id, qty, x FROM t;", &keys);
        assert_eq!(before, after, "pad={pad}: a REFUSED retype changed what the table reads back ({e})");
        assert_is_the_size_refusal(&e, "t");
    }
    assert!(refusals > 0, "no width in 4014..=4026 produced a refused retype; the sweep is vacuous");
}

/// **An ordinary schema, no oversized column anywhere.** `zz_verify10.rs::t1_*`.
///
/// 20 x `VARCHAR(200)` plus a small tail — nothing about this table looks like it is near a limit,
/// and every column is well under one. It is the row that is near the limit, and any row within
/// ~8 bytes of it is in scope. This is the fixture that shows the defect is not confined to
/// deliberately pathological schemas.
#[test]
fn an_ordinary_wide_row_is_not_destroyed_by_a_refused_add_column() {
    let cols: Vec<String> = (1..=20).map(|i| format!("c{i} VARCHAR(200)")).collect();
    let select = "SELECT id, c1, c20 FROM t;";
    let keys = [Value::Integer(1), Value::Integer(2)];
    let mut refusals = 0;
    for fill in [190usize, 195, 197, 198, 199, 200] {
        let mut d = db();
        d.sql(&format!("CREATE TABLE t (id INTEGER NOT NULL, {}, tail VARCHAR(30));", cols.join(", ")));
        let small: Vec<String> = (1..=20).map(|_| "'s'".to_string()).collect();
        d.sql(&format!("INSERT INTO t VALUES (1, {}, 'z');", small.join(", ")));
        let y = "y".repeat(fill);
        let wide: Vec<String> = (1..=20).map(|_| format!("'{y}'")).collect();
        if d.try_sql(&format!("INSERT INTO t VALUES (2, {}, 'zzz');", wide.join(", "))).is_err() {
            continue;
        }

        let before = snapshot(&mut d, "t", select, &keys);
        let res = d.try_sql("ALTER TABLE t ADD COLUMN extra BIGINT;");
        let Err(e) = res else { continue };
        refusals += 1;

        let after = snapshot(&mut d, "t", select, &keys);
        assert_eq!(
            before, after,
            "fill={fill}: an ordinary 22-column table of VARCHAR(200)s lost data to a REFUSED \
             `ALTER TABLE t ADD COLUMN extra BIGINT` ({e})"
        );
        assert_is_the_size_refusal(&e, "t");
    }
    assert!(refusals > 0, "no fill width produced a refused ADD COLUMN; the sweep is vacuous");
}

// ---------------------------------------------------------------------------------------------
// The exit criterion
// ---------------------------------------------------------------------------------------------

/// **The exit criterion for I19.** `zz_verify_agent_claims.rs::v5_*`.
///
/// The damage the review measured was durable: after `checkpoint()`, a flush and a reopen the
/// panic was identical, because the heap pages on disk really did hold the new shape. So the
/// comparison that matters is not made in the process that ran the ALTER. Everything is written
/// through, the database is reopened into a **fresh** `BufferPoolManager` and `Catalog` — nothing
/// cached carries over — and the table is compared against the snapshot taken before the refusal.
///
/// The last two assertions are the ones a byte comparison alone would not make: the table must
/// still be *usable* under its old shape, and the column the refused statement was trying to add
/// must still be addable once the row that blocked it is narrowed.
#[test]
fn a_refused_alter_survives_a_checkpoint_a_flush_and_a_fresh_buffer_pool() {
    let select = "SELECT id, n FROM t;";
    let keys = [Value::Integer(1), Value::Integer(2)];
    let big = "x".repeat(2014);

    let (dir, before) = {
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
        d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
        d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));

        let before = snapshot(&mut d, "t", select, &keys);
        let e = d
            .try_sql("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;")
            .err()
            .expect("the fixture must produce a REFUSED alter, or it measures nothing");
        assert_is_the_size_refusal(&e, "t");

        assert_eq!(
            d.schema_changes(),
            vec!["t:CreateTable".to_string()],
            "a REFUSED ALTER put a column-level DDL record on the feed"
        );

        d.txn.checkpoint().expect("checkpoint");
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
        (d.dir, before)
    };

    let mut r = open_at(dir);
    let after = snapshot(&mut r, "t", select, &keys);
    assert_eq!(
        before, after,
        "a REFUSED ALTER changed the table durably: the difference survived a checkpoint, a flush \
         and a reopen into a fresh buffer pool"
    );

    // Still a working table, not just an intact one.
    r.sql("INSERT INTO t VALUES (3, 300, 'more', 'more');");
    assert_eq!(r.rows(select).unwrap().len(), 3, "the table no longer accepts writes");

    // And the refusal was about this row, not about the statement: narrow the row and the same
    // ALTER goes through.
    r.sql("UPDATE t SET b = 'narrow' WHERE id = 2;");
    r.sql("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;");
    assert_eq!(
        r.shape("t")[1],
        ("n".to_string(), DataType::BigInt),
        "after narrowing the row that blocked it, the retype still did not land"
    );
    // Sorted by key, not asserted in scan order: a `SELECT` with no `ORDER BY` returns rows in
    // physical order, and the retype that just succeeded relocated the wide row to a later page.
    let mut carried = r.rows("SELECT id, n FROM t;").unwrap();
    carried.sort_by(|a, b| match (&a[0], &b[0]) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        _ => panic!("the key column is not an INTEGER: {a:?} {b:?}"),
    });
    assert_eq!(
        carried,
        vec![
            vec![Value::Integer(1), Value::BigInt(100)],
            vec![Value::Integer(2), Value::BigInt(200)],
            vec![Value::Integer(3), Value::BigInt(300)],
        ],
        "the retype that finally ran did not carry every row across"
    );
}

// ---------------------------------------------------------------------------------------------
// The other ways in, and the other side of a restart
// ---------------------------------------------------------------------------------------------

/// The rewrite is also reached from `MERGE`, and on that route the refusal must be no less safe.
///
/// `AgentRuntime::merge` runs a branch's staged schema edits through the same
/// `Catalog::alter_table`, so an edit an agent typed against a narrow table can meet a row that has
/// since grown. What is asserted is the target: its heap, its shape, its index answers and its
/// rows, byte for byte.
///
/// # The scope of this test is exactly one staged edit on a branch that wrote no rows, and that is
/// not an accident of the fixture
///
/// `MERGE` is not atomic across its parts. It commits the branch's row writes in one transaction
/// and *then* applies the staged schema edits one at a time, each with its own `Catalog::finish`,
/// `persist`, flush and DDL record. So a refusal on any edit leaves everything before it durable:
///
/// - a branch that also wrote rows has those rows on the target — measured as `SELECT id, n`
///   going from 2 rows to 3 with `id=1`'s value changed, after a `MERGE` that returned `Err`;
/// - a branch staging `RENAME COLUMN a TO a2` *then* the retype has the rename permanently applied
///   — `SELECT a` answers before the refusal and errors after it, and a `RenameColumn` DDL record
///   reaches the feed.
///
/// Both were found by a fresh-context adversarial pass and confirmed by an independent skeptic,
/// which also established what is NOT wrong with them: across each refusal the heap is
/// byte-identical, no tuple moved, no row was lost and no value changed. The refused ALTER does
/// what this row requires of it; what is not atomic is the statement wrapped around it, which is
/// `review-B11.md` finding 5 and needs the publish and the edits to share one commit point.
/// Reproductions: branch `I19-atk4-paths` commit `0dee647`, and `I19-atk4-LENS` commits `7078256`
/// and `502d8ed`.
///
/// So this test says what it can honestly say — a lone edit, no row writes — and the paragraph
/// above says what it cannot, rather than a name implying the whole route is covered.
#[test]
fn a_lone_schema_edit_refused_at_merge_leaves_the_target_exactly_as_it_was() {
    let keys = [Value::Integer(1), Value::Integer(2)];
    let big = "x".repeat(2014);

    for (setup, stmt) in [
        (
            "CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));",
            "ALTER TABLE t ALTER COLUMN n TYPE BIGINT;",
        ),
        (
            "CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));",
            "ALTER TABLE t ADD COLUMN note VARCHAR(20);",
        ),
    ] {
        let mut d = db();
        d.sql(setup);
        d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
        d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));
        let before = snapshot(&mut d, "t", "SELECT id, n FROM t;", &keys);

        let mut agent = Session::with_runtime(d.runtime.clone());
        d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut agent).expect("open a branch");
        d.exec(stmt, &mut agent).expect("the branch must accept the edit: it is legal against the shape");
        let merged = d.exec("MERGE;", &mut agent);
        let e = merged.err().unwrap_or_else(|| panic!("`{stmt}` was not refused at MERGE"));
        assert_is_the_size_refusal(&e, "t");

        let after = snapshot(&mut d, "t", "SELECT id, n FROM t;", &keys);
        assert_eq!(before, after, "`{stmt}` refused at MERGE still changed the target ({e})");
    }
}

/// A refusal followed by a crash with **no clean flush**, replayed through `recover`.
///
/// The rewrite is unlogged on purpose, so the WAL has nothing to say about it either way — which
/// means a crash after a refusal must not be able to *invent* half of one. The buffer pool is
/// deliberately not flushed: the reopen sees whatever was written through plus whatever the WAL
/// replays, which is the only state a real crash leaves.
#[test]
fn a_refused_alter_survives_a_crash_with_no_clean_flush() {
    let dir = {
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
        d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
        let big = "x".repeat(2014);
        d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));
        d.wal.flush().unwrap();
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();

        let e = d
            .try_sql("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;")
            .err()
            .expect("the fixture must produce a refused ALTER");
        assert_is_the_size_refusal(&e, "t");
        // The crash: the WAL is durable, the buffer pool is not flushed, nothing is synced.
        d.wal.flush().unwrap();
        d.dir
    };

    let mut r = open_recovering(dir);
    assert_eq!(
        r.shape("t"),
        vec![
            ("id".to_string(), DataType::Integer),
            ("n".to_string(), DataType::Integer),
            ("a".to_string(), DataType::Varchar(2100)),
            ("b".to_string(), DataType::Varchar(2100)),
        ],
        "recovery installed a shape the refused ALTER never got as far as installing"
    );
    let rows = r.rows("SELECT id, n FROM t;").expect("the table must be readable after recovery");
    assert_eq!(rows.len(), 2, "recovery did not bring both rows back: {rows:?}");
}

/// Refusing the same statement repeatedly, with a checkpoint each time, changes nothing — and the
/// statement still works once the row that blocked it is narrowed.
///
/// A guard that leaked a little on every attempt would pass a single-shot test. The checkpoint is in
/// the loop because `review-B11.md` finding 8 turns on what a checkpoint does to schema records.
#[test]
fn repeated_refusals_leave_the_table_unchanged_and_the_statement_still_works() {
    let keys = [Value::Integer(1), Value::Integer(2)];
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    let big = "x".repeat(2014);
    d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));
    let before = snapshot(&mut d, "t", "SELECT id, n FROM t;", &keys);

    for attempt in 0..3 {
        let e = d
            .try_sql("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;")
            .err()
            .unwrap_or_else(|| panic!("attempt {attempt} was not refused"));
        assert_is_the_size_refusal(&e, "t");
        assert_eq!(
            before,
            snapshot(&mut d, "t", "SELECT id, n FROM t;", &keys),
            "attempt {attempt} changed the table"
        );
        d.txn.checkpoint().expect("checkpoint");
        assert_eq!(
            before,
            snapshot(&mut d, "t", "SELECT id, n FROM t;", &keys),
            "the checkpoint after attempt {attempt} changed the table"
        );
    }

    d.sql("UPDATE t SET b = 'narrow' WHERE id = 2;");
    d.sql("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;");
    assert_eq!(d.shape("t")[1], ("n".to_string(), DataType::BigInt));
    assert_eq!(d.rows("SELECT id, n FROM t;").unwrap().len(), 2);
}

// ---------------------------------------------------------------------------------------------
// The guard must not fire when it should not
// ---------------------------------------------------------------------------------------------

/// A precheck that refused everything would satisfy every test above.
///
/// The same sweep, asserting that widths on the other side of the boundary still succeed — and
/// that the sweep saw **both** outcomes, so it cannot pass with the guard stuck in either
/// position. The success side also checks the rows came across, not just that the statement
/// returned `Ok`.
#[test]
fn the_precheck_does_not_refuse_a_row_that_fits() {
    let mut refused: Vec<usize> = Vec::new();
    // (pad, the widened row's measured size in bytes)
    let mut allowed: Vec<(usize, usize)> = Vec::new();
    for pad in 4028usize..=4036 {
        let mut d = db();
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(4100));");
        d.sql("INSERT INTO t VALUES (1, 'small');");
        let big = "x".repeat(pad);
        if d.try_sql(&format!("INSERT INTO t VALUES (2, '{big}');")).is_err() {
            continue;
        }
        match d.try_sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);") {
            Err(e) => {
                assert_is_the_size_refusal(&e, "t");
                refused.push(pad);
            }
            Ok(_) => {
                assert_eq!(
                    d.shape("t").len(),
                    3,
                    "pad={pad}: the ALTER reported success and the column is not there"
                );
                let rows = d.rows("SELECT id, note FROM t;").unwrap();
                assert_eq!(rows.len(), 2, "pad={pad}: the successful ALTER lost a row");
                assert_eq!(rows[1], vec![Value::Integer(2), Value::Null]);
                // The widened row's size, measured from the bytes on the page rather than
                // recomputed from the layout, so the boundary assertion below is not the
                // precheck's own arithmetic handed back to it.
                let widest = d.heap("t").iter().map(|(_, b)| b.len()).max().unwrap();
                allowed.push((pad, widest));
            }
        }
    }
    assert!(
        !allowed.is_empty() && !refused.is_empty(),
        "the sweep must straddle the boundary to prove the precheck is neither always-on nor \
         always-off: allowed={allowed:?} refused={refused:?}"
    );
    let (widest_pad, widest_bytes) = *allowed.iter().max().unwrap();
    assert!(
        widest_pad < *refused.iter().min().unwrap(),
        "the boundary is not monotonic in row width, so the precheck is not measuring row width: \
         allowed={allowed:?} refused={refused:?}"
    );
    // **The boundary is exact, and this is the assertion that says so.** One character of pad is
    // one byte of tuple, and the sweep is contiguous across the boundary, so exactly one width in
    // it widens a row to precisely `MAX_TUPLE_SIZE`. That width must be the last one accepted: a
    // precheck refusing at `>=` instead of `>` would stop one byte early and land here, and every
    // other assertion in this file would still pass.
    assert_eq!(
        widest_bytes, MAX_TUPLE_SIZE,
        "the widest row the ALTER accepted came to {widest_bytes} bytes, not the {MAX_TUPLE_SIZE} \
         a page can hold, so the precheck is off by {} byte(s): allowed={allowed:?} \
         refused={refused:?}",
        MAX_TUPLE_SIZE as i64 - widest_bytes as i64
    );
}

// ---------------------------------------------------------------------------------------------
// The layer underneath
// ---------------------------------------------------------------------------------------------

/// The same destruction one layer down, reached without an `ALTER` at all.
///
/// `HeapFileManager::update`'s relocation branch deletes the slot and unpins the page dirty before
/// the insert that replaces it, so growing a row past what any page can hold destroyed it. The
/// `ALTER` was one caller of that, not the cause, and fixing only `rewrite_heap` would have left
/// the branch itself live for the next caller.
///
/// Driven through `HeapFileManager` directly rather than through `UPDATE ... SET`, because that is
/// where the harm is. A statement's update is logged (`txn: Some`), so the delete is a WAL record
/// and the statement's abort undoes it — measured: the heap is byte-identical afterwards, and
/// `integration_statement_atomicity` already covers that path. Every heap opened with
/// `HeapFileManager::open` has `txn: None`, which is what `rewrite_heap` uses, and there the delete
/// is unrecoverable. Measured before the guard existed: one live tuple to zero, with the slot
/// reading `SlotDeleted`.
#[test]
fn an_unlogged_update_too_large_for_any_page_refuses_instead_of_deleting_the_row() {
    use ferrodb::catalog::column::Column;
    use ferrodb::catalog::schema::Schema;
    use ferrodb::storage::tuple::Tuple;

    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.path().join("heap.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let _catalog = Catalog::create(bp.clone()).unwrap();
    let heap = HeapFileManager::new(bp.clone()).unwrap();
    assert!(heap.txn.is_none(), "this test is about the unlogged path; it is logged");

    let schema = Schema::new(vec![
        Column::new("id".to_string(), DataType::Integer, false),
        Column::new("a".to_string(), DataType::Varchar(4100), true),
    ]);
    let original =
        Tuple::serialize(&[Value::Integer(1), Value::Varchar("keep me".to_string())], &schema, 1)
            .unwrap();
    let rid = heap.insert(original).unwrap();
    let before: Vec<(RecordId, Vec<u8>)> =
        heap.scan().map(|r| r.unwrap()).map(|(rid, t)| (rid, t.data)).collect();

    // Inside the declared VARCHAR(4100) — so the refusal cannot come from the type — and outside
    // what a 4096-byte page can hold.
    let oversized =
        Tuple::serialize(&[Value::Integer(1), Value::Varchar("x".repeat(4090))], &schema, 1).unwrap();
    assert!(
        oversized.data.len() > MAX_TUPLE_SIZE,
        "the fixture is not oversized ({} bytes) and measures nothing",
        oversized.data.len()
    );
    let err = heap.update(rid, oversized).err().expect("a tuple no page can hold must be refused");

    let after: Vec<(RecordId, Vec<u8>)> =
        heap.scan().map(|r| r.unwrap()).map(|(rid, t)| (rid, t.data)).collect();
    assert_eq!(before, after, "a REFUSED unlogged update destroyed the row it could not grow ({err})");
    assert!(heap.read(rid).is_ok(), "the row's slot was deleted by an update that reported failure");
}

/// **The table region being full is an ordinary end-state, and a refused ALTER must survive it.**
///
/// Found by a fresh-context adversarial pass against the first version of this fix, and confirmed by
/// an independent skeptic that reproduced it from its own fixture with two controls. The row-width
/// precheck is not enough on its own: it answers "can any page hold this tuple", and a relocation
/// also needs there to *be* a page. `DiskManager::allocate` refuses once the region below the
/// copy-on-write arena floor is full, and that floor is set to `high_water + DEFAULT_ARENA_HEADROOM`
/// (32736 pages, ~128 MB) when the database is created and cannot be moved afterwards — so every
/// database has this end-state, and the allocator's own message for it is "the table region is
/// full". Under the old delete-then-insert order the measured result was row 1 gone, the primary
/// index still pointing at its deleted slot, and all of it surviving checkpoint, flush and a reopen.
///
/// The fixture packs one data page exactly, so **every** row has to relocate when the column is
/// appended, and then closes the allocator. Two things are asserted, in this order: the same ALTER
/// on the same heap with the allocator open must SUCCEED and carry all rows (otherwise the fixture
/// is measuring a shape the ALTER cannot do anyway), and with the allocator closed it must be
/// refused and change nothing.
#[test]
fn a_refused_alter_changes_nothing_when_the_page_allocator_is_exhausted() {
    // A tuple of 24 header + 1 bitmap + 3 pad + 4 id + 2 + 60 = 94 bytes, so 41 of them plus their
    // slot entries occupy 4018 of a page's 4073 usable bytes and the 42nd would not fit. The
    // appended column costs 2 bytes, which the 55 that are left cannot absorb in place.
    const ROWS: i32 = 41;
    let pad = "y".repeat(60);
    let select = "SELECT id, v FROM t;";
    let keys: Vec<Value> = (1..=ROWS).map(Value::Integer).collect();

    let fill = |d: &mut Db| {
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));");
        for i in 1..=ROWS {
            d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
        }
        let pages: std::collections::BTreeSet<u32> =
            d.heap("t").iter().map(|(rid, _)| rid.page_id).collect();
        assert_eq!(
            pages.len(),
            1,
            "the fixture must pack exactly one data page or the rows need not relocate: {pages:?}"
        );
        assert_eq!(d.heap("t").len(), ROWS as usize);
    };

    // Control: with the allocator open, this exact ALTER works and every row survives it.
    {
        let mut d = db();
        fill(&mut d);
        d.sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);");
        assert_eq!(d.shape("t").len(), 3);
        assert_eq!(
            d.rows(select).unwrap().len(),
            ROWS as usize,
            "the control lost rows, so the fixture cannot tell exhaustion from an impossible ALTER"
        );
    }

    // The real case: close the allocator by putting the arena floor at the high-water mark, which
    // is what a database that has grown into its headroom looks like.
    let (dir, before) = {
        let mut d = db();
        fill(&mut d);
        let before = snapshot(&mut d, "t", select, &keys);

        let floor = d.bp.disk_manager.high_water().unwrap();
        d.bp.disk_manager.reserve_from(floor).unwrap();
        assert!(
            d.bp.new_page().is_err(),
            "the fixture did not actually close the allocator, so it measures nothing"
        );

        let e = d
            .try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);")
            .err()
            .expect("an ALTER that cannot obtain a page must be refused");
        let msg = e.to_string();
        assert!(
            msg.contains("the reserved arena region at page"),
            "refused for some other reason than the exhausted table region: {msg}"
        );

        let after = snapshot(&mut d, "t", select, &keys);
        assert_eq!(
            before, after,
            "a REFUSED ALTER changed the table when the page allocator was exhausted ({e})"
        );

        d.txn.checkpoint().expect("checkpoint");
        d.bp.flush_all().unwrap();
        d.bp.disk_manager.sync().unwrap();
        (d.dir, before)
    };

    let mut r = open_at(dir);
    let after = snapshot(&mut r, "t", select, &keys);
    assert_eq!(
        before, after,
        "the damage from a refused ALTER against an exhausted allocator survived a checkpoint, a \
         flush and a reopen into a fresh buffer pool"
    );
}

/// The same exhaustion, but arriving **part way through** the conversion.
///
/// The test above fails on its first row, so the reserve-before-delete order alone is enough there.
/// This one leaves the page just enough slack for the first rows to be widened in place, so the
/// allocator is not consulted until several tuples on disk are already in the new shape. Refusing at
/// that point leaves rows of two shapes under one schema — the table reads back as wrong values or a
/// panic, which is the harm the row-width precheck exists to prevent, arriving by another route.
/// Only asking for the space **before** pass 2 starts prevents it, and that is what
/// `reserve_free_space` is for.
#[test]
fn a_refused_alter_converts_nothing_when_the_allocator_dies_part_way() {
    // 39 tuples of 94 bytes plus their slots leave 251 free bytes. Widening one in place costs 96
    // of them (the grow branch of `Page::update` re-lays the tuple and abandons the old bytes), so
    // the first two rows convert and the third has to relocate.
    const ROWS: i32 = 39;
    let pad = "y".repeat(60);
    let select = "SELECT id, v FROM t;";
    let keys: Vec<Value> = (1..=ROWS).map(Value::Integer).collect();

    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(120));");
    for i in 1..=ROWS {
        d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
    }
    let pages: std::collections::BTreeSet<u32> =
        d.heap("t").iter().map(|(rid, _)| rid.page_id).collect();
    assert_eq!(pages.len(), 1, "the fixture must pack one page: {pages:?}");

    let before = snapshot(&mut d, "t", select, &keys);
    let floor = d.bp.disk_manager.high_water().unwrap();
    d.bp.disk_manager.reserve_from(floor).unwrap();
    assert!(d.bp.new_page().is_err(), "the fixture did not close the allocator");

    let e = d
        .try_sql("ALTER TABLE t ADD COLUMN w VARCHAR(10);")
        .err()
        .expect("an ALTER that cannot obtain a page must be refused");

    let after = snapshot(&mut d, "t", select, &keys);
    assert_eq!(
        before, after,
        "a REFUSED ALTER left some rows converted and some not: rows of two shapes under one \
         schema is what makes the table unreadable ({e})"
    );
}

/// The heap-layer half of the exhaustion case, one level below any `ALTER`.
///
/// `update`'s relocation branch used to free the source slot and *then* look for a destination, so
/// an allocator that had nothing left took the row with it. The `ALTER` above is one caller of that;
/// this is the branch itself, on an unlogged heap where there is no undo record to fall back on.
/// The tuple here is well under `MAX_TUPLE_SIZE`, so the size guard cannot be what saves it — the
/// only thing that can is obtaining the destination before giving up the source.
#[test]
fn an_unlogged_update_that_cannot_obtain_a_page_refuses_instead_of_deleting_the_row() {
    use ferrodb::catalog::column::Column;
    use ferrodb::catalog::schema::Schema;
    use ferrodb::storage::tuple::Tuple;

    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.path().join("heap.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let _catalog = Catalog::create(bp.clone()).unwrap();
    let heap = HeapFileManager::new(bp.clone()).unwrap();
    let schema = Schema::new(vec![
        Column::new("id".to_string(), DataType::Integer, false),
        Column::new("v".to_string(), DataType::Varchar(120), true),
    ]);

    // Pack one page, so the row that grows cannot stay where it is.
    let mut rids = Vec::new();
    let pad = "y".repeat(60);
    for i in 1..=41 {
        let t = Tuple::serialize(&[Value::Integer(i), Value::Varchar(pad.clone())], &schema, 1).unwrap();
        rids.push(heap.insert(t).unwrap());
    }
    let pages: std::collections::BTreeSet<u32> = rids.iter().map(|r| r.page_id).collect();
    assert_eq!(pages.len(), 1, "the fixture must pack exactly one page: {pages:?}");

    // Close the allocator: this is a database that has grown into its arena headroom.
    let floor = bp.disk_manager.high_water().unwrap();
    bp.disk_manager.reserve_from(floor).unwrap();
    assert!(bp.new_page().is_err(), "the fixture did not close the allocator");

    let before: Vec<(RecordId, Vec<u8>)> =
        heap.scan().map(|r| r.unwrap()).map(|(rid, t)| (rid, t.data)).collect();
    let grown = Tuple::serialize(
        &[Value::Integer(1), Value::Varchar("y".repeat(70))],
        &schema,
        1,
    )
    .unwrap();
    assert!(
        grown.data.len() < MAX_TUPLE_SIZE,
        "the tuple must be one a page could hold, or the size guard is what refuses it"
    );
    let err = heap.update(rids[0], grown).err().expect("no page to relocate into must be refused");

    let after: Vec<(RecordId, Vec<u8>)> =
        heap.scan().map(|r| r.unwrap()).map(|(rid, t)| (rid, t.data)).collect();
    assert_eq!(
        before, after,
        "a REFUSED unlogged update destroyed the row it could not relocate ({err})"
    );
    assert!(heap.read(rids[0]).is_ok(), "the row's slot was deleted by an update that failed");
}

/// An ALTER that needs **many** reserved pages must finish, and must carry every row.
///
/// `reserve_free_space` grows the heap before pass 2 so an exhausted allocator is a refusal rather
/// than a half-converted heap. Its first version asked `find_or_make_page` for the space, which
/// *found* the empty page it had just added — an empty page has exactly the free span that search
/// looks for — so it added one page and then spun. Every fixture in this file needed at most one
/// page and passed; `integration_alter_column::a_lookup_by_key_still_finds_a_row_the_rewrite_moved`
/// is the test that hung, and it hung with no output rather than failing, which is why a bound is
/// on this one.
///
/// 200 rows of ~214 bytes fill a dozen pages with almost no slack, so appending a column has to
/// reserve about eleven more. The assertion is not just that it terminates: every row must come
/// back, by key as well as by scan, because a reservation that grows the heap is also a reservation
/// that moves rows between pages.
#[test]
fn an_alter_that_must_reserve_many_pages_completes_and_keeps_every_row() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, pad VARCHAR(200));");
    let pad = "z".repeat(180);
    for i in 1..=200 {
        d.sql(&format!("INSERT INTO t VALUES ({i}, '{pad}');"));
    }
    let pages: std::collections::BTreeSet<u32> =
        d.heap("t").iter().map(|(rid, _)| rid.page_id).collect();
    assert!(
        pages.len() >= 10,
        "the fixture must span enough pages to need more than one reserved: {} page(s)",
        pages.len()
    );

    d.sql("ALTER TABLE t ADD COLUMN note VARCHAR(20);");

    assert_eq!(d.shape("t").len(), 3);
    let rows = d.rows("SELECT id, note FROM t;").unwrap();
    assert_eq!(rows.len(), 200, "the rewrite lost rows");
    assert!(rows.iter().all(|r| r[1] == Value::Null), "the appended column is not NULL everywhere");
    let keys: Vec<Value> = (1..=200).map(Value::Integer).collect();
    let answers = d.by_key("t", &keys);
    let unreachable: Vec<&Value> =
        answers.iter().filter(|(_, rid)| rid.is_none()).map(|(k, _)| k).collect();
    assert!(unreachable.is_empty(), "rows the primary index can no longer find: {unreachable:?}");
}

/// `MAX_TUPLE_SIZE` is the number the precheck refuses against, and the precheck is only correct if
/// it is the real boundary of `Page::insert`. Pinned by measurement in both directions rather than
/// by restating the arithmetic: a fresh page takes a tuple of exactly `MAX_TUPLE_SIZE` bytes and
/// refuses one byte more.
#[test]
fn max_tuple_size_is_the_boundary_page_insert_actually_enforces() {
    use ferrodb::storage::heap_page::Page;
    use ferrodb::storage::tuple::Tuple;

    let mut page = Page::empty(1);
    assert!(
        page.insert(Tuple::new(vec![0u8; MAX_TUPLE_SIZE + 1])).is_err(),
        "a fresh page accepted a tuple of MAX_TUPLE_SIZE + 1 bytes, so the constant is too small \
         and the precheck refuses ALTERs that would have worked"
    );
    let mut page = Page::empty(1);
    assert!(
        page.insert(Tuple::new(vec![0u8; MAX_TUPLE_SIZE])).is_ok(),
        "a fresh page refused a tuple of exactly MAX_TUPLE_SIZE bytes, so the constant is too \
         large and the precheck lets through a row that will be destroyed"
    );
}
