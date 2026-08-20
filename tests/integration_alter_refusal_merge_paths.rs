//! I19 attack pass: routes into `rewrite_heap` other than a plain `ALTER TABLE`.
//!
//! The claim under attack: "an ALTER TABLE that is refused leaves the table EXACTLY as it was".
//! These tests reach the same refusal through `MERGE` (`AgentRuntime::apply_merge` ->
//! `Catalog::alter_table`) and ask what the STATEMENT left behind, not what the alter left behind.

use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
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
    open_at(tempfile::tempdir().unwrap(), true)
}

fn open_at(dir: tempfile::TempDir, fresh: bool) -> Db {
    let path = dir.path().join("alter.db");
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
    fn try_in(&mut self, sql: &str, session: &mut Session) -> Result<Outcome, FerroError> {
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

    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let mut s =
            std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out = self.try_in(sql, &mut s);
        self.session = s;
        out
    }

    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.try_in(sql, s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.try_sql(sql)))
            .map_err(|_| format!("`{sql}` PANICKED the reading thread"))?;
        match out {
            Ok(Outcome::Rows(r)) => Ok(r),
            Ok(_) => Err(format!("`{sql}` did not return rows")),
            Err(e) => Err(format!("`{sql}` errored: {e}")),
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

    fn by_key(&self, table: &str, keys: &[Value]) -> Vec<(Value, Option<RecordId>)> {
        let root = self.catalog.get_table(table).unwrap().primary_index_root;
        let ix = BPlusTreeManager::<Value, RecordId>::open(root, self.bp.clone());
        keys.iter().map(|k| (k.clone(), ix.search(k).unwrap())).collect()
    }
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    shape: Vec<(String, DataType)>,
    heap: Vec<(RecordId, Vec<u8>)>,
    ids: Result<Vec<Vec<Value>>, String>,
    by_key: Vec<(Value, Option<RecordId>)>,
    feed: Vec<String>,
}

fn snapshot(d: &mut Db) -> Snapshot {
    let keys = [Value::Integer(1), Value::Integer(2), Value::Integer(3)];
    Snapshot {
        shape: d.shape("t"),
        heap: d.heap("t"),
        ids: d.rows("SELECT id FROM t;"),
        by_key: d.by_key("t", &keys),
        feed: d.schema_changes(),
    }
}

fn assert_is_the_size_refusal(e: &FerroError) {
    let msg = e.to_string();
    for needle in ["would widen", "bytes a tuple can occupy", "Nothing has been written"] {
        assert!(
            msg.contains(needle),
            "refused, but not by the size precheck — no {needle:?} in: {msg}"
        );
    }
}

/// A table holding one small row and one row `pad` bytes wide. `None` when the wide row itself
/// does not fit a page, which is not the case under test.
fn near_limit_table(pad: usize) -> Option<Db> {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(4100));");
    d.sql("INSERT INTO t VALUES (1, 'small');");
    let big = "x".repeat(pad);
    if d.try_sql(&format!("INSERT INTO t VALUES (2, '{big}');")).is_err() {
        return None;
    }
    Some(d)
}

// -------------------------------------------------------------------------------------------
// 1. A MERGE whose schema half is refused for size: what happened to the rows it published?
// -------------------------------------------------------------------------------------------

#[test]
fn a_merge_refused_for_size_still_published_its_rows_to_the_target() {
    let mut refusals = 0;
    let mut leaked = Vec::new();
    for pad in 4020usize..=4045 {
        let Some(mut d) = near_limit_table(pad) else { continue };

        let mut a = Session::with_runtime(d.runtime.clone());
        d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut a);
        d.exec("INSERT INTO t VALUES (3, 'branchrow');", &mut a);
        d.exec("ALTER TABLE t ADD COLUMN note VARCHAR(20);", &mut a);

        let before = snapshot(&mut d);
        assert!(
            !format!("{:?}", before.ids).contains("Integer(3)"),
            "pad={pad}: the branch row was on the target before MERGE; fixture is wrong"
        );

        let res = d.try_in("MERGE;", &mut a);
        let Err(e) = res else { continue };
        refusals += 1;
        assert_is_the_size_refusal(&e);

        let after = snapshot(&mut d);
        let ids_after = format!("{:?}", after.ids);
        if ids_after.contains("Integer(3)") || after != before {
            leaked.push(format!(
                "pad={pad}\n  MERGE reported: {e}\n  ids before: {:?}\n  ids after:  {:?}\n  \
                 heap tuples before/after: {}/{}\n  shape before/after: {:?} / {:?}\n  \
                 feed before/after: {:?} / {:?}",
                before.ids,
                after.ids,
                before.heap.len(),
                after.heap.len(),
                before.shape,
                after.shape,
                before.feed,
                after.feed
            ));
        }
    }
    assert!(refusals > 0, "no pad in 4020..=4045 produced a refused MERGE; MAX={MAX_TUPLE_SIZE}");
    assert!(
        leaked.is_empty(),
        "a MERGE that reported FAILURE changed the target table:\n{}",
        leaked.join("\n")
    );
}

/// Same statement, then MERGE again. `apply_merge` returns `Err` before `seal`, so the branch is
/// never retired — does its already-published row get published a SECOND time?
#[test]
#[ignore = "evidence probe: panics to print what it observed; run with --ignored"]
fn a_branch_whose_merge_was_refused_can_publish_its_rows_again() {
    let mut refusals = 0;
    let mut doubled = Vec::new();
    for pad in 4020usize..=4045 {
        let Some(mut d) = near_limit_table(pad) else { continue };
        let mut a = Session::with_runtime(d.runtime.clone());
        d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut a);
        d.exec("INSERT INTO t VALUES (3, 'branchrow');", &mut a);
        d.exec("ALTER TABLE t ADD COLUMN note VARCHAR(20);", &mut a);

        let Err(e1) = d.try_in("MERGE;", &mut a) else { continue };
        refusals += 1;
        assert_is_the_size_refusal(&e1);
        let heap_after_first = d.heap("t").len();

        let second = d.try_in("MERGE;", &mut a);
        let verdict = match &second {
            Ok(Outcome::Agent(AgentOutput::Merge(r))) => format!("Ok(applied={})", r.applied_to_target),
            Ok(o) => format!("Ok(other outcome: {})", matches!(o, Outcome::Ok)),
            Err(e) => format!("Err({e})"),
        };
        let heap_after_second = d.heap("t").len();
        doubled.push(format!(
            "pad={pad}: first MERGE Err; heap tuples after first = {heap_after_first}; \
             second MERGE -> {verdict}; heap tuples after second = {heap_after_second}; \
             ids = {:?}",
            d.rows("SELECT id FROM t;")
        ));
        break;
    }
    assert!(refusals > 0, "no pad produced a refused MERGE");
    panic!("OBSERVED (this test is a probe, not an assertion):\n{}", doubled.join("\n"));
}

// -------------------------------------------------------------------------------------------
// 2. Two schema edits on one branch, the second refused: is the first left applied?
// -------------------------------------------------------------------------------------------

#[test]
fn a_merge_refused_on_its_second_edit_leaves_the_first_edit_applied() {
    let mut refusals = 0;
    let mut partials = Vec::new();
    for pad in 4010usize..=4045 {
        let Some(mut d) = near_limit_table(pad) else { continue };
        let mut a = Session::with_runtime(d.runtime.clone());
        d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut a);
        d.exec("ALTER TABLE t ADD COLUMN c1 VARCHAR(20);", &mut a);
        d.exec("ALTER TABLE t ADD COLUMN c2 VARCHAR(20);", &mut a);

        let before = snapshot(&mut d);
        let Err(e) = d.try_in("MERGE;", &mut a) else { continue };
        refusals += 1;
        let after = snapshot(&mut d);
        if after != before {
            partials.push(format!(
                "pad={pad}\n  MERGE reported: {e}\n  shape before: {:?}\n  shape after:  {:?}\n  \
                 feed before: {:?}\n  feed after:  {:?}\n  heap tuples before/after: {}/{}",
                before.shape.iter().map(|c| c.0.clone()).collect::<Vec<_>>(),
                after.shape.iter().map(|c| c.0.clone()).collect::<Vec<_>>(),
                before.feed,
                after.feed,
                before.heap.len(),
                after.heap.len()
            ));
        }
    }
    assert!(refusals > 0, "no pad in 4010..=4045 produced a refused two-edit MERGE");
    assert!(
        partials.is_empty(),
        "a MERGE that reported FAILURE left part of its schema change applied:\n{}",
        partials.join("\n")
    );
}

// -------------------------------------------------------------------------------------------
// 3. Two branches: the first merges, the second is refused for size.
// -------------------------------------------------------------------------------------------

#[test]
fn a_second_branchs_refused_merge_leaves_the_target_as_the_first_branch_left_it() {
    let mut refusals = 0;
    let mut leaked = Vec::new();
    for pad in 4010usize..=4045 {
        let Some(mut d) = near_limit_table(pad) else { continue };
        let mut a = Session::with_runtime(d.runtime.clone());
        let mut b = Session::with_runtime(d.runtime.clone());
        d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut a);
        d.exec("BEGIN AGENT SESSION AS 'agent-b';", &mut b);
        d.exec("ALTER TABLE t ADD COLUMN c1 VARCHAR(20);", &mut a);
        d.exec("INSERT INTO t VALUES (3, 'from-b');", &mut b);
        d.exec("ALTER TABLE t ADD COLUMN c2 VARCHAR(20);", &mut b);

        // A's merge may itself be refused; that is a different case, skip it.
        if d.try_in("MERGE;", &mut a).is_err() {
            continue;
        }
        let before = snapshot(&mut d);
        let Err(e) = d.try_in("MERGE;", &mut b) else { continue };
        refusals += 1;
        let after = snapshot(&mut d);
        if after != before {
            leaked.push(format!(
                "pad={pad}\n  B's MERGE reported: {e}\n  ids before: {:?}\n  ids after:  {:?}\n  \
                 shape before: {:?}\n  shape after:  {:?}\n  heap tuples before/after: {}/{}",
                before.ids,
                after.ids,
                before.shape.iter().map(|c| c.0.clone()).collect::<Vec<_>>(),
                after.shape.iter().map(|c| c.0.clone()).collect::<Vec<_>>(),
                before.heap.len(),
                after.heap.len()
            ));
        }
    }
    assert!(refusals > 0, "no pad produced a refused second-branch MERGE");
    assert!(
        leaked.is_empty(),
        "the second branch's REFUSED merge changed the target:\n{}",
        leaked.join("\n")
    );
}

// -------------------------------------------------------------------------------------------
// 4. `stage_schema_edit` itself: does an agent learn about the size problem at stage time?
// -------------------------------------------------------------------------------------------

#[test]
#[ignore = "evidence probe: panics to print what it observed; run with --ignored"]
fn staging_an_edit_that_cannot_be_applied_reports_nothing_at_stage_time() {
    let Some(mut d) = near_limit_table(4035) else { panic!("fixture: 4035 did not fit") };
    let mut a = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut a);
    let staged = d.try_in("ALTER TABLE t ADD COLUMN note VARCHAR(20);", &mut a);
    let plain = d.try_sql("ALTER TABLE t ADD COLUMN note2 VARCHAR(20);");
    panic!(
        "OBSERVED: staging on a branch -> {}; the same ALTER on the shared table -> {}",
        match &staged {
            Ok(_) => "Ok".to_string(),
            Err(e) => format!("Err({e})"),
        },
        match &plain {
            Ok(_) => "Ok".to_string(),
            Err(e) => format!("Err({e})"),
        }
    );
}

/// The two questions the earlier probes left open:
///  (a) exactly what the SECOND merge reports about a branch the first merge failed on, and
///  (b) whether the row a failed MERGE published survives a checkpoint, a flush and a reopen into
///      a completely fresh buffer pool.
#[test]
#[ignore = "evidence probe: panics to print what it observed; run with --ignored"]
fn probe_second_merge_report_and_durability_of_the_leaked_row() {
    let mut d = near_limit_table(4034).expect("fixture: 4034 did not fit");
    let mut a = Session::with_runtime(d.runtime.clone());
    d.exec("BEGIN AGENT SESSION AS 'agent-a';", &mut a);
    d.exec("INSERT INTO t VALUES (3, 'branchrow');", &mut a);
    d.exec("ALTER TABLE t ADD COLUMN note VARCHAR(20);", &mut a);
    let first = d.try_in("MERGE;", &mut a).err().map(|e| e.to_string()).expect("expected Err");
    let ids_after_first = format!("{:?}", d.rows("SELECT id FROM t;"));

    let second = d.try_in("MERGE;", &mut a);
    let detail = match &second {
        Ok(Outcome::Agent(AgentOutput::Merge(r))) => format!(
            "Ok: applied_to_target={} outcome={} rows={} schema_reports={} text={}",
            r.applied_to_target,
            r.outcome.name(),
            r.rows.len(),
            r.schema.len(),
            format!("{r}").replace('\n', " | ")
        ),
        Ok(_) => "Ok (not a merge report)".to_string(),
        Err(e) => format!("Err({e})"),
    };
    let shape_after_second = d.shape("t").iter().map(|c| c.0.clone()).collect::<Vec<_>>();

    // Durability: checkpoint what we can, flush, sync, then reopen into a fresh pool + catalog.
    let ckpt = d.try_sql("CHECKPOINT;").map(|_| "ok").unwrap_or("no CHECKPOINT statement");
    d.bp.flush_all().unwrap();
    d.bp.disk_manager.sync().unwrap();
    let dir = d.dir;
    drop(d.catalog);
    let mut d2 = open_at(dir, false);
    let reopened_ids = format!("{:?}", d2.rows("SELECT id FROM t;"));
    let reopened_heap = d2.heap("t").len();
    let reopened_shape = d2.shape("t").iter().map(|c| c.0.clone()).collect::<Vec<_>>();

    panic!(
        "OBSERVED\n  first MERGE: Err({first})\n  ids after first: {ids_after_first}\n  \
         second MERGE: {detail}\n  shape after second: {shape_after_second:?}\n  \
         CHECKPOINT: {ckpt}\n  after reopen into a FRESH buffer pool: ids={reopened_ids} \
         heap_tuples={reopened_heap} shape={reopened_shape:?}"
    );
}
