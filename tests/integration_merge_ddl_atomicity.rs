//! E82 — a `MERGE` decides everything before it writes anything.
//!
//! `tests/integration_alter_refusal_paths.rs` (I19-atk4) is the fixture set that found the defect:
//! a refusal on the *k*-th staged schema edit left `1..k-1` applied and the branch's rows
//! published. These are the detectors for the rules the fix introduces, each of which the I19
//! fixtures do not reach:
//!
//! - a row the merge is about to PUBLISH is measured against the shape it will land in;
//! - a chain of edits is measured at every step, not only at the last one;
//! - a refusal names the edit that earned it;
//! - the rows land in the shape the merge's own edits produced;
//! - an index follows its column across renames a merge applies.

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
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    #[allow(dead_code)]
    dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("merge.db");
    let file =
        std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("merge.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let catalog = Catalog::create(bp.clone()).unwrap();
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    Db { dir, catalog, wal, bp, txn, runtime, session }
}

impl Db {
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let mut session =
            std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out = self.exec(sql, &mut session);
        self.session = session;
        out
    }
    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }
    fn exec(&mut self, sql: &str, session: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                p.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), session)
    }
    fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        match self.sql(sql) {
            Outcome::Rows(r) => r,
            _ => panic!("`{sql}` did not return rows"),
        }
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
    fn ddl(&self) -> Vec<String> {
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
    /// Open a branch and hand back its session.
    fn branch(&mut self, name: &str) -> Session {
        let mut s = Session::with_runtime(self.runtime.clone());
        self.exec(&format!("BEGIN AGENT SESSION AS '{name}';"), &mut s).expect("branch");
        s
    }
}

const WIDE: &str =
    "CREATE TABLE t (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));";
const RETYPE: &str = "ALTER TABLE t ALTER COLUMN n TYPE BIGINT;";

/// The row width at which a row fits the table as it stands and does **not** fit it once
/// `n INTEGER -> BIGINT` has moved every column after `n` along.
///
/// Probed rather than written down. The number is a function of the tuple layout — the null
/// bitmap, the alignment padding, the version header — and a constant here would go stale silently
/// the first time any of those moved, leaving a test that still passes while measuring nothing.
fn width_the_retype_pushes_over() -> usize {
    for w in 1980..2030usize {
        let big = "x".repeat(w);
        let mut probe = db();
        probe.sql(WIDE);
        if probe.try_sql(&format!("INSERT INTO t VALUES (1, 100, '{big}', '{big}');")).is_err() {
            continue;
        }
        if probe.try_sql(RETYPE).is_ok() {
            continue;
        }
        return w;
    }
    panic!("no width where a row fits now and the retype pushes it over");
}

/// A width where `ADD COLUMN` still fits and the retype behind it does not, so a three-edit chain
/// can be refused by its LAST edit rather than by an earlier one.
fn width_where_add_fits_and_retype_does_not() -> usize {
    for w in 1980..2030usize {
        let big = "x".repeat(w);
        let mut probe = db();
        probe.sql(WIDE);
        if probe.try_sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');")).is_err() {
            continue;
        }
        if probe.try_sql("ALTER TABLE t RENAME COLUMN a TO a2;").is_err() {
            continue;
        }
        if probe.try_sql("ALTER TABLE t ADD COLUMN c1 VARCHAR(20);").is_err() {
            continue;
        }
        if probe.try_sql(RETYPE).is_ok() {
            continue;
        }
        return w;
    }
    panic!("no width where a rename and an ADD fit and the retype does not");
}

// -------------------------------------------------------------------------------------------
// RULE: a row this merge is about to PUBLISH is measured against the shape it will land in,
// before anything is written.
//
// The I19 fixtures cannot reach this: their oversized row is already on the target, so the plan's
// own pass over the heap finds it. Here every row on the target is small and the row that does not
// fit is one the branch is about to publish — invisible to a check that only looks at the heap.
// -------------------------------------------------------------------------------------------
#[test]
fn a_row_the_merge_would_publish_too_wide_for_the_shape_it_lands_in_refuses_the_whole_merge() {
    let w = width_the_retype_pushes_over();
    let big = "x".repeat(w);
    let mut d = db();
    d.sql(WIDE);
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    let shape_before = d.shape("t");
    let heap_before = d.heap("t");
    let ddl_before = d.ddl();
    let rows_before = d.rows("SELECT id, n FROM t;");

    let mut agent = d.branch("agent-wide");
    d.exec(&format!("INSERT INTO t VALUES (3, 300, '{big}', '{big}');"), &mut agent)
        .expect("the branch takes a row that fits the table as it stands");
    d.exec(RETYPE, &mut agent).expect("the branch accepts the edit");

    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
    let msg = e.to_string();
    eprintln!("--- refusal = {msg}");

    // **State first, message second.** The rule this test names is that the WHOLE merge is
    // refused; the wording is how it is refused. Asserting the wording first means a mutant that
    // breaks the rule dies on the prose and never reaches the assertions that state the rule.
    assert_eq!(shape_before, d.shape("t"), "the refused MERGE altered the table anyway: {msg}");
    assert_eq!(ddl_before, d.ddl(), "the refused MERGE reached the change feed: {msg}");
    assert_eq!(heap_before, d.heap("t"), "the refused MERGE wrote to the heap: {msg}");
    assert_eq!(rows_before, d.rows("SELECT id, n FROM t;"), "the refused MERGE published rows");

    // Which refusal it was. Only the first needle discriminates — the other two appear in the
    // plan's row-width refusal too — so the plan's own marker is asserted ABSENT. Without that,
    // this test would pass on a refusal raised by the wrong check.
    assert!(msg.contains("would publish a row into"), "not the publish precheck: {msg}");
    assert!(
        !msg.contains("would widen"),
        "this was the PLAN's row-width refusal, not the publish precheck — the plan cannot see a \
         row that is not on the target yet, so a merge refused here is being refused for the \
         wrong reason: {msg}"
    );
}

// -------------------------------------------------------------------------------------------
// RULE: a chain is measured at EVERY step, and the refusal names the edit that earned it.
//
// The chain is laid down as one pass, so only the final width is a physical constraint. Measuring
// only the final one would still refuse here — and would blame the wrong edit, which is the visible
// half of a group whose semantics have drifted from the statements it stands for.
// -------------------------------------------------------------------------------------------
#[test]
fn a_chain_refused_at_its_first_edit_names_that_edit_and_not_the_last() {
    let w = width_the_retype_pushes_over();
    let big = "x".repeat(w);
    let mut d = db();
    d.sql(WIDE);
    d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));
    let shape_before = d.shape("t");
    let ddl_before = d.ddl();

    let mut agent = d.branch("agent-first");
    d.exec(RETYPE, &mut agent).expect("stage retype");
    d.exec("ALTER TABLE t RENAME COLUMN a TO a2;", &mut agent).expect("stage rename");

    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
    let msg = e.to_string();
    eprintln!("--- refusal = {msg}");
    assert!(
        msg.contains("edit 1 of 2"),
        "the refusal blamed the wrong edit of the chain, or blamed none: {msg}"
    );
    assert!(msg.contains("RetypeColumn"), "the refusal did not name the edit: {msg}");
    assert_eq!(shape_before, d.shape("t"), "the refused MERGE altered the table anyway");
    assert_eq!(ddl_before, d.ddl(), "the refused MERGE reached the change feed");
}

// -------------------------------------------------------------------------------------------
// RULE: a merge whose k-th edit is refused leaves NO edit applied and the rows unpublished.
//
// The exit criterion itself, at k = 3. The I19 fixtures reach k = 2; a group is not a group until
// something past the second element has to be held back too.
// -------------------------------------------------------------------------------------------
#[test]
fn a_merge_refused_at_its_third_edit_leaves_neither_of_the_first_two_applied() {
    let w = width_where_add_fits_and_retype_does_not();
    let big = "x".repeat(w);
    let mut d = db();
    d.sql(WIDE);
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));
    let shape_before = d.shape("t");
    let heap_before = d.heap("t");
    let ddl_before = d.ddl();
    let rows_before = d.rows("SELECT id, n FROM t;");

    let mut agent = d.branch("agent-three");
    d.exec("ALTER TABLE t RENAME COLUMN a TO a2;", &mut agent).expect("stage rename");
    d.exec("ALTER TABLE t ADD COLUMN c1 VARCHAR(20);", &mut agent).expect("stage add");
    d.exec(RETYPE, &mut agent).expect("stage retype");
    // Four values, not five: a staged `ADD COLUMN` is recorded on the branch and not applied to
    // it, so the branch still binds `INSERT` against the shared four-column shape. The merge is
    // what carries the row into the shape its own edits produce.
    d.exec("INSERT INTO t VALUES (3, 300, 'small', 'small');", &mut agent)
        .expect("and a row, so the merge has both halves to hold back");

    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
    let msg = e.to_string();
    eprintln!("--- refusal = {msg}");

    assert_eq!(shape_before, d.shape("t"), "the refused MERGE left an edit applied: {msg}");
    assert_eq!(ddl_before, d.ddl(), "the refused MERGE emitted a DDL record anyway: {msg}");
    assert_eq!(heap_before, d.heap("t"), "the refused MERGE wrote to the heap: {msg}");
    assert_eq!(rows_before, d.rows("SELECT id, n FROM t;"), "the refused MERGE published rows");

    // Re-read off disk: an in-memory-only half-application is still a half-application, and the
    // catalog is persisted outside the WAL.
    d.bp.flush_all().unwrap();
    d.bp.disk_manager.sync().unwrap();
    let reread = Catalog::open(d.bp.clone(), 1).unwrap();
    let on_disk: Vec<String> = reread
        .get_table("t")
        .unwrap()
        .schema
        .columns
        .iter()
        .map(|c| format!("{}:{:?}", c.name, c.data_type))
        .collect();
    eprintln!("--- shape RE-READ FROM DISK = {on_disk:?}");
    assert_eq!(
        on_disk,
        vec!["id:Integer", "n:Integer", "a:Varchar(2100)", "b:Varchar(2100)"],
        "the refused MERGE persisted an edit to disk"
    );

    // Last, and deliberately last: which edit was blamed is test
    // `a_chain_refused_at_its_first_edit_names_that_edit_and_not_the_last`'s rule, not this one.
    // Asserted ahead of the state above, it killed this test on the naming rule under a mutant
    // that broke the atomicity rule, and the disk re-read — the strongest assertion here — was
    // never reached.
    assert!(msg.contains("edit 3 of 3"), "the refusal blamed the wrong edit: {msg}");
}

// -------------------------------------------------------------------------------------------
// RULE: the rows a merge publishes land in the shape that merge's OWN edits produced.
//
// The schema now goes first, so a row is written directly into the altered shape rather than
// written narrow and widened afterwards. A row carrying its pre-merge type into a retyped column
// is not a smaller version of that: `Tuple::serialize` picks the width from the VALUE and
// `deserialize` picks it from the SCHEMA, so it is refused rather than stored wrong.
// -------------------------------------------------------------------------------------------
#[test]
fn a_merge_publishes_its_rows_into_the_shape_its_own_edits_produced() {
    let mut d = db();
    d.sql(WIDE);
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");

    let mut agent = d.branch("agent-land");
    d.exec("ALTER TABLE t ADD COLUMN c1 VARCHAR(20);", &mut agent).expect("stage add");
    d.exec(RETYPE, &mut agent).expect("stage retype");
    d.exec("UPDATE t SET n = 999 WHERE id = 1;", &mut agent).expect("update on the branch");
    // Four values: see the note in the three-edit test — a staged `ADD COLUMN` does not change
    // what the branch binds against, so `c1` is the merge's to fill.
    d.exec("INSERT INTO t VALUES (2, 200, 'small', 'small');", &mut agent).expect("insert");
    d.exec("MERGE;", &mut agent).expect("the merge must land");

    assert_eq!(
        d.shape("t"),
        vec![
            ("id".into(), DataType::Integer),
            ("n".into(), DataType::BigInt),
            ("a".into(), DataType::Varchar(2100)),
            ("b".into(), DataType::Varchar(2100)),
            ("c1".into(), DataType::Varchar(20)),
        ]
    );
    let mut rows = d.rows("SELECT id, n, c1 FROM t;");
    assert_eq!(rows.len(), 2, "the merge published the wrong number of rows: {rows:?}");
    rows.sort_by_key(|r| match r[0] {
        Value::Integer(i) => i,
        _ => 0,
    });
    eprintln!("--- rows after the merge = {rows:?}");
    // Both the row that was already there and the one the merge published read back as BIGINT.
    // A row published under the old shape and left there would come back as `Integer`.
    assert_eq!(rows[0][1], Value::BigInt(999), "the updated row is not in the produced shape");
    assert_eq!(rows[1][1], Value::BigInt(200), "the published row is not in the produced shape");
    // Both rows are NULL in the appended column: the row already on the target because the
    // rewrite appended it, the published one because `conform_row` did. A row that reached the
    // heap under the four-column shape would not read back with a fifth column at all.
    assert_eq!(rows[0][2], Value::Null, "the pre-existing row lost the appended column");
    assert_eq!(rows[1][2], Value::Null, "the published row lost the appended column");
}

// -------------------------------------------------------------------------------------------
// RULE: an index follows its column across every rename a merge applies, in the chain's order.
//
// `IndexInfo.column_name` is resolved to an ordinal by `open_table` on every WRITE, and a miss is
// `KeyNotFound` — so the whole table stops accepting writes. A read proves nothing here: the read
// path declines to use an index it cannot resolve and falls back to a scan, which succeeds either
// way. This is the merge-path twin of `integration_alter_column`'s fire-checked plain-ALTER test.
// -------------------------------------------------------------------------------------------
#[test]
fn an_index_follows_its_column_across_the_renames_a_merge_applies() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (1, 10);");
    d.sql("CREATE INDEX ix ON inv (qty);");

    let mut agent = d.branch("agent-ix");
    d.exec("ALTER TABLE inv RENAME COLUMN qty TO amount;", &mut agent).expect("stage rename 1");
    d.exec("ALTER TABLE inv RENAME COLUMN amount TO quantity;", &mut agent)
        .expect("stage rename 2");
    d.exec("MERGE;", &mut agent).expect("the merge must land");

    assert_eq!(
        d.shape("inv"),
        vec![("id".into(), DataType::Integer), ("quantity".into(), DataType::Integer)],
        "the chain did not end where the last rename said"
    );
    // The write is the assertion. An index left pointing at `qty` — or at the intermediate
    // `amount`, if the renames were applied out of order — makes every one of these `KeyNotFound`.
    d.sql("UPDATE inv SET quantity = 11 WHERE id = 1;");
    assert_eq!(d.rows("SELECT quantity FROM inv WHERE id = 1;")[0][0], Value::Integer(11));
    d.sql("INSERT INTO inv VALUES (2, 20);");
    d.sql("DELETE FROM inv WHERE id = 2;");
}

// -------------------------------------------------------------------------------------------
// RULE: a merge cannot narrow a row and retype the column behind it in the SAME merge, and the
// refusal says which order works.
//
// This is the one behaviour the fix takes away, pinned here rather than left to be discovered.
// A merge decides its schema edits against the target as it stands when the merge begins, because
// the heap rewrite is unlogged and cannot be decided against a heap that is still moving —
// publishing first and deciding after is precisely what E82 was. So a narrowing this same merge is
// carrying has not landed when the edit is judged, and re-running the merge refuses identically
// forever unless the refusal says what to do instead.
//
// Two merges do work, and the second half of this test is what makes that a fact rather than an
// assumption: an `UPDATE` supersedes the wide version rather than leaving it in the main heap, so
// once the narrowing has landed the retype has nothing left to refuse.
// -------------------------------------------------------------------------------------------
#[test]
fn narrowing_a_row_and_retyping_behind_it_takes_two_merges_and_the_refusal_says_so() {
    let w = width_the_retype_pushes_over();
    let big = "x".repeat(w);
    let mut d = db();
    d.sql(WIDE);
    d.sql("INSERT INTO t VALUES (1, 100, 'small', 'small');");
    d.sql(&format!("INSERT INTO t VALUES (2, 200, '{big}', '{big}');"));

    let mut both = d.branch("agent-both");
    d.exec("UPDATE t SET a = 'small', b = 'small' WHERE id = 2;", &mut both)
        .expect("narrow on the branch");
    d.exec(RETYPE, &mut both).expect("stage the retype behind it");
    let e = d.exec("MERGE;", &mut both).err().expect("one merge cannot do both");
    let msg = e.to_string();
    eprintln!("--- refusal = {msg}");
    assert!(
        msg.contains("the narrowing has to reach the target first"),
        "the refusal gave a statement's advice to a merge, which cannot act on it: {msg}"
    );
    assert_eq!(d.shape("t")[1].1, DataType::Integer, "the refused merge retyped anyway");
    assert_eq!(
        d.rows("SELECT a FROM t WHERE id = 2;")[0][0],
        Value::Varchar(big.clone()),
        "the refused merge published the narrowing anyway"
    );

    // The order the refusal names.
    let mut narrow = d.branch("agent-narrow");
    d.exec("UPDATE t SET a = 'small', b = 'small' WHERE id = 2;", &mut narrow).expect("narrow");
    d.exec("MERGE;", &mut narrow).expect("the narrowing merges on its own");
    let mut retype = d.branch("agent-retype");
    d.exec(RETYPE, &mut retype).expect("stage the retype");
    d.exec("MERGE;", &mut retype).expect("and now the retype merges");
    assert_eq!(d.shape("t")[1].1, DataType::BigInt, "the two-merge path did not land");
    assert_eq!(d.rows("SELECT n FROM t WHERE id = 1;")[0][0], Value::BigInt(100));
}

// -------------------------------------------------------------------------------------------
// RULE: a row the merge would publish with a NULL in a NOT NULL column refuses the whole merge,
// before any schema edit is applied.
//
// Nothing on the branch path catches this: `branch_insert` proves arity and a branch-visible
// duplicate key, and the binder carries `nullable` without refusing on it, so the agent's INSERT
// is accepted. `InsertOp::execute` refuses it — at publication, which with the schema applied
// first is after the merge has already altered the table. Deciding it here is what keeps the
// merge's refusal and its writes on the same side of the line.
// -------------------------------------------------------------------------------------------
#[test]
fn a_row_the_merge_would_publish_with_a_null_in_a_not_null_column_refuses_the_whole_merge() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER NOT NULL);");
    d.sql("INSERT INTO t VALUES (1, 10);");
    let shape_before = d.shape("t");
    let ddl_before = d.ddl();
    let rows_before = d.rows("SELECT id, v FROM t;");

    let mut agent = d.branch("agent-null");
    d.exec("ALTER TABLE t ADD COLUMN note VARCHAR(20);", &mut agent).expect("stage add");
    d.exec("INSERT INTO t VALUES (2, NULL);", &mut agent)
        .expect("the branch takes it — nothing on that path checks NOT NULL");

    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
    let msg = e.to_string();
    eprintln!("--- refusal = {msg}");

    assert_eq!(shape_before, d.shape("t"), "the refused MERGE added the column anyway: {msg}");
    assert_eq!(ddl_before, d.ddl(), "the refused MERGE reached the change feed: {msg}");
    assert_eq!(rows_before, d.rows("SELECT id, v FROM t;"), "the refused MERGE published rows");
    assert!(
        msg.contains("declared NOT NULL"),
        "refused, but not by the publish precheck — a refusal from inside the publish transaction \
         arrives after the schema is already durable: {msg}"
    );
}

// -------------------------------------------------------------------------------------------
// RULE: a rename follows its column into BOTH index lists.
//
// A `TableEntry` keeps ordinary and full-text indexes in separate vectors and both record their
// column by NAME. `planner::plan` resolves each with `position(...).ok_or(KeyNotFound)`, so an
// index left pointing at the old name is not stale — it is unfindable, and every write against
// the table stops working. The pre-refactor rename arm walked only `indexes`, so this held for
// the plain `ALTER TABLE` path too and had done since `CREATE FULLTEXT INDEX` existed.
//
// The write is the assertion, for the reason `integration_alter_column` gives: the read path
// declines to use an index it cannot resolve and falls back to a scan, which succeeds either way.
// -------------------------------------------------------------------------------------------
#[test]
fn a_rename_follows_its_column_into_the_fulltext_index_too() {
    let mut d = db();
    d.sql("CREATE TABLE docs (id INTEGER NOT NULL, body VARCHAR(200));");
    d.sql("INSERT INTO docs VALUES (1, 'the quick brown fox');");
    d.sql("CREATE FULLTEXT INDEX ix ON docs (body);");

    let mut agent = d.branch("agent-ft");
    d.exec("ALTER TABLE docs RENAME COLUMN body TO text;", &mut agent).expect("stage rename");
    d.exec("MERGE;", &mut agent).expect("the merge must land");

    assert_eq!(
        d.shape("docs"),
        vec![("id".into(), DataType::Integer), ("text".into(), DataType::Varchar(200))]
    );
    d.sql("INSERT INTO docs VALUES (2, 'a lazy dog');");
    d.sql("UPDATE docs SET text = 'the quick red fox' WHERE id = 1;");
    d.sql("DELETE FROM docs WHERE id = 2;");
    assert_eq!(
        d.rows("SELECT text FROM docs WHERE id = 1;")[0][0],
        Value::Varchar("the quick red fox".into())
    );
}

// -------------------------------------------------------------------------------------------
// RULE: planning a merge's schema edits writes NOTHING — not even an empty page.
//
// `Catalog::plan_alters` says "write nothing at all" and `AgentRuntime::merge` leans on it: EVERY
// table is planned before any row is measured, so a reservation taken while planning the first
// table can be followed by a refusal about the second — and that refusal's message ends "Nothing
// has been written". `reserve_free_space` appends empty pages through `add_empty_page`, so it
// belongs in `apply_plan` and not in the plan.
//
// Two tables, named so the alphabetical order the merge walks them in is the order this needs:
// `aa`'s retype fits and makes every row grow, which is what forces a reservation; `zz`'s does not
// fit, which is what refuses after it. One table cannot show this — its own plan refuses before
// its own reservation is reached.
//
// A tuple scan cannot see the residue either: appended empty pages hold no tuples, which is why
// every other refusal test here would pass with the reservation back in the planning half. The
// page high-water mark is what sees it.
// -------------------------------------------------------------------------------------------
#[test]
fn planning_a_refused_merge_does_not_even_append_an_empty_page() {
    let over = "x".repeat(width_the_retype_pushes_over());
    let mut d = db();
    d.sql("CREATE TABLE aa (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
    // Rows that leave room for the retype but nearly fill their pages, so growing them has to
    // reserve space rather than being absorbed.
    let mid = "y".repeat(1900);
    for i in 1..6 {
        d.sql(&format!("INSERT INTO aa VALUES ({i}, {i}00, '{mid}', '{mid}');"));
    }
    d.sql("CREATE TABLE zz (id INTEGER NOT NULL, n INTEGER, a VARCHAR(2100), b VARCHAR(2100));");
    d.sql(&format!("INSERT INTO zz VALUES (1, 100, '{over}', '{over}');"));

    let pages_before = d.bp.disk_manager.high_water().unwrap();
    let aa_before = d.heap("aa");
    let aa_shape_before = d.shape("aa");

    let mut agent = d.branch("agent-pages");
    d.exec("ALTER TABLE aa ALTER COLUMN n TYPE BIGINT;", &mut agent).expect("stage aa retype");
    d.exec("ALTER TABLE zz ALTER COLUMN n TYPE BIGINT;", &mut agent).expect("stage zz retype");
    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused, by zz");
    eprintln!("--- refusal = {e}");
    assert!(e.to_string().contains("'zz'"), "the refusal came from the wrong table: {e}");

    let pages_after = d.bp.disk_manager.high_water().unwrap();
    eprintln!("--- page high-water before = {pages_before}, after = {pages_after}");
    assert_eq!(aa_shape_before, d.shape("aa"), "the refused MERGE altered the first table");
    assert_eq!(aa_before, d.heap("aa"), "the refused MERGE moved a tuple in the first table");
    assert_eq!(
        pages_before, pages_after,
        "planning the first table allocated {} page(s), and then the merge was refused with a \
         message that says nothing had been written",
        pages_after.saturating_sub(pages_before)
    );
}
