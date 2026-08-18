//! B9 — the observability views over the agent layer, from SQL.
//!
//! # What is under test
//!
//! That `SELECT` over each of the five views returns **typed rows with real column names**, that a
//! quarantined branch shows up with the reason it is held, and — the half that decides whether any
//! of the rest means anything — that an **empty** view is distinguishable from a broken one.
//!
//! The empty/populated pairing is the shape that has to be tested and is easy to skip. A view that
//! always returned zero rows would satisfy "SELECT works" completely: no error, a well-formed
//! result, a plausible answer. So every view here is read twice, once when it must be empty and once
//! when it must not, and the emptiness assertion also checks that the **column list is still fully
//! present** — that is what a client uses to tell "nothing is quarantined" from "this view is
//! broken", and it is the only difference between the two on the wire.
//!
//! The wire half of the criterion — read back through a real client rather than asserted in Rust —
//! is `tests/pg/pg_views_client.py`, driven by `integration_system_views_wire.rs`.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::types::BranchState;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::catalog::system_views::{NamedRows, SystemView, VIEW_NAMES};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("views.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("views.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    /// A view read, as the typed result the wire would carry.
    fn view(&mut self, sql: &str) -> NamedRows {
        let mut s = self.session();
        match self.ok(sql, &mut s) {
            Outcome::Table(t) => t,
            other => panic!("{sql} did not return a typed table: {}", outcome_kind(&other)),
        }
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE oncall (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO oncall VALUES (1, 100);", &mut s);
        self.ok("INSERT INTO oncall VALUES (2, 200);", &mut s);
        self.ok("INSERT INTO oncall VALUES (3, 300);", &mut s);
    }
}

/// `Result<Outcome, _>::expect_err` needs `Debug` on `Outcome` and `Outcome` does not have it (it
/// holds boxed trait objects). This says the same thing without adding a derive to a shared enum.
fn refusal(r: Result<Outcome, FerroError>, what: &str) -> FerroError {
    match r {
        Err(e) => e,
        Ok(out) => panic!("{what}: the statement was accepted, returning {}", outcome_kind(&out)),
    }
}

fn outcome_kind(o: &Outcome) -> &'static str {
    match o {
        Outcome::Rows(_) => "Outcome::Rows",
        Outcome::Affected(_) => "Outcome::Affected",
        Outcome::Explain(_) => "Outcome::Explain",
        Outcome::Agent(_) => "Outcome::Agent",
        Outcome::Table(_) => "Outcome::Table",
        Outcome::Ok => "Outcome::Ok",
    }
}

fn text_of(v: &Value) -> String {
    match v {
        Value::Varchar(s) => s.clone(),
        Value::Decimal(d) => d.clone(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Timestamp(ms) => ms.to_string(),
        Value::Null => "NULL".into(),
    }
}

/// Pull one named column out of a view result, rendered as text.
fn column(rows: &NamedRows, name: &str) -> Vec<String> {
    let at = rows
        .column_index(name)
        .unwrap_or_else(|| panic!("no column '{name}'; got {:?}", rows.header()));
    rows.rows.iter().map(|r| text_of(&r[at])).collect()
}

/// **Every view is selectable and every view announces its own columns, with no rows to infer from.**
///
/// The breaking shape is the *empty* one. A view that answered `SELECT` with zero rows AND zero
/// columns — which is what `Outcome::Rows` would have produced, since pgwire derives its field list
/// from the first row — is indistinguishable from a view whose materialiser is broken or whose name
/// no longer resolves. Nothing else in this file would catch that, because every other test here
/// looks at a populated view.
#[test]
fn an_empty_view_still_announces_every_declared_column() {
    let mut db = Db::new();
    for name in VIEW_NAMES {
        let declared = SystemView::by_name(name).expect("resolves").columns();
        let got = db.view(&format!("SELECT * FROM {name};"));
        assert_eq!(
            got.header(),
            declared.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
            "{name} did not announce its declared columns"
        );
        for (a, b) in got.columns.iter().zip(declared.iter()) {
            assert_eq!(a.data_type, b.data_type, "{name}.{} announced the wrong type", a.name);
            assert_eq!(a.nullable, b.nullable, "{name}.{} announced the wrong nullability", a.name);
        }
        assert_eq!(a_qualifier(&got), name, "{name} did not qualify its columns with its own name");
    }
    // ferro_branches is never empty — the trunk is always a branch — and that is itself the
    // anti-vacuity anchor for this test: if the materialiser were returning nothing at all, this
    // would fail while every "must be empty" assertion below passed.
    let branches = db.view("SELECT * FROM ferro_branches;");
    assert_eq!(branches.len(), 1, "only the trunk exists yet: {:?}", branches.rows);
    assert_eq!(column(&branches, "branch_id"), vec!["0"]);
    assert_eq!(column(&branches, "state"), vec!["Live"]);
    assert_eq!(column(&branches, "parent_id"), vec!["NULL"], "the trunk has no parent");

    for name in ["ferro_runs", "ferro_row_authors", "ferro_quarantine", "ferro_run_activity"] {
        assert!(
            db.view(&format!("SELECT * FROM {name};")).is_empty(),
            "{name} had rows before any agent ran"
        );
    }
}

fn a_qualifier(rows: &NamedRows) -> String {
    rows.columns.first().map(|c| c.qualifier.clone()).unwrap_or_default()
}

/// **Populated: every view that can hold a row does hold one, with the right values in it.**
///
/// The counterpart to the test above. Together they are the anti-vacuity pair the brief demands —
/// neither one alone distinguishes a working view from a view that always answers the same way.
#[test]
fn every_view_is_populated_once_an_agent_has_run() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'restock' RUN 'r_7' MODEL 'claude-opus-5/2026-05';", &mut a);
    db.ok("SELECT qty FROM oncall WHERE id = 1;", &mut a);
    db.ok("UPDATE oncall SET qty = qty - 5 WHERE id = 1;", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;

    // ferro_branches: trunk plus the agent's branch.
    let branches = db.view("SELECT * FROM ferro_branches;");
    assert_eq!(branches.len(), 2, "trunk plus one agent branch: {:?}", branches.rows);
    assert_eq!(column(&branches, "branch_id"), vec!["0".to_string(), branch.id.to_string()]);
    assert_eq!(column(&branches, "parent_id")[1], "0", "the agent branch forked from trunk");
    assert_eq!(column(&branches, "depth")[1], "1");
    // The trunk's lease is `u64::MAX`. As a BIGINT it would be -1; the digits are the point.
    assert_eq!(column(&branches, "lease_deadline")[0], u64::MAX.to_string());

    // ferro_runs: which agent + run + model (criterion 9).
    let runs = db.view("SELECT * FROM ferro_runs;");
    assert_eq!(runs.len(), 1, "{:?}", runs.rows);
    assert_eq!(column(&runs, "agent_id"), vec!["restock"]);
    assert_eq!(column(&runs, "run_id"), vec!["r_7"]);
    assert_eq!(column(&runs, "model"), vec!["claude-opus-5"]);
    assert_eq!(column(&runs, "model_version"), vec!["2026-05"]);
    assert_eq!(column(&runs, "prompt_hash")[0].len(), 64, "a sha-width hex string");
    assert_eq!(column(&runs, "branch_name"), vec![format!("b_{}", branch.id)]);

    // ferro_run_activity: the per-run write and read counts, while the work is in flight.
    let act = db.view("SELECT * FROM ferro_run_activity;");
    assert_eq!(act.len(), 1, "{:?}", act.rows);
    assert_eq!(column(&act, "agent_id"), vec!["restock"]);
    assert_eq!(column(&act, "staged_rows"), vec!["1"], "one row was written on the branch");
    assert_eq!(
        column(&act, "rows_read_exact"),
        vec!["1"],
        "one point read was retained as an exact version"
    );
    assert_eq!(
        column(&act, "blind_writes"),
        vec!["0"],
        "the row written is the row read, so nothing was written blind"
    );
    assert!(
        column(&act, "ops_captured")[0].parse::<u64>().unwrap() >= 1,
        "the UPDATE captured no typed op: {:?}",
        act.rows
    );

    // ferro_row_authors answers about PUBLISHED rows, so it is still empty here — and that is a
    // fact about the design, not a gap: `run_of` reads the workspace and `seal` drops it, so
    // authorship has to survive the merge somewhere else, which is what `row_author` is for.
    assert!(
        db.view("SELECT * FROM ferro_row_authors;").is_empty(),
        "a row was attributed before any merge published it"
    );

    db.ok("MERGE;", &mut a);

    let authors = db.view("SELECT * FROM ferro_row_authors;");
    assert_eq!(authors.len(), 1, "the merge published one row: {:?}", authors.rows);
    assert_eq!(column(&authors, "table_name"), vec!["oncall"]);
    assert_eq!(column(&authors, "row_id"), vec!["1"]);
    assert_eq!(column(&authors, "agent_id"), vec!["restock"]);
    assert_eq!(column(&authors, "run_id"), vec!["r_7"]);

    // And the run view is empty again: the workspace is gone with the merge. Stated as an
    // assertion so the two views' different lifetimes are pinned rather than assumed.
    assert!(
        db.view("SELECT * FROM ferro_runs;").is_empty(),
        "ferro_runs answered for a branch whose workspace the merge dropped"
    );
    assert!(db.view("SELECT * FROM ferro_run_activity;").is_empty());
}

/// **A quarantined branch appears in the quarantine view WITH its reason.**
///
/// Quarantine is reached the way B1 made it reachable — the read-premise check at merge admission —
/// and not by calling `AgentRuntime::quarantine` directly. Calling it directly would test the view
/// against a state nothing in the system produces.
///
/// The breaking shape: two branches that each read the SAME row and each write a DIFFERENT one. The
/// merge engine compares only the cells a branch wrote, so nothing overlaps and both merges are
/// accepted without a read-set check. A workload where two agents contend on the same row would
/// never reach this path.
#[test]
fn a_quarantined_branch_shows_up_with_the_reason_it_is_held() {
    let mut db = Db::new();
    db.seed();

    assert!(
        db.view("SELECT * FROM ferro_quarantine;").is_empty(),
        "something was held before any merge ran"
    );

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r_a';", &mut a);
    db.ok("SELECT qty FROM oncall WHERE id = 1;", &mut a);

    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r_b';", &mut b);
    db.ok("SELECT qty FROM oncall WHERE id = 1;", &mut b);
    let held = b.agent.as_ref().unwrap().branch;

    db.ok("UPDATE oncall SET qty = 111 WHERE id = 1;", &mut a);
    db.ok("UPDATE oncall SET qty = 222 WHERE id = 2;", &mut b);

    db.ok("MERGE;", &mut a);
    db.ok("MERGE;", &mut b);

    assert_eq!(
        db.runtime.branches().get(held).expect("record").state,
        BranchState::Quarantined,
        "the premise check did not hold B, so there is nothing for the view to show"
    );

    let q = db.view("SELECT * FROM ferro_quarantine;");
    assert_eq!(q.len(), 1, "the held branch is not in the view: {:?}", q.rows);
    assert_eq!(column(&q, "branch_id"), vec![held.id.to_string()]);
    assert_eq!(column(&q, "generation"), vec![held.generation.to_string()]);
    assert_eq!(column(&q, "branch"), vec![held.to_string()]);

    let reason = &column(&q, "reason")[0];
    assert!(
        reason.contains("read-premise") && reason.contains("changed in the base"),
        "the view has the branch but not what it is held for: {reason}"
    );
    assert_ne!(reason, "NULL", "the reason came through as SQL NULL");

    // The branch is also in ferro_branches, in state Quarantined. `live_branches` filters to `Live`
    // and could never show it, which is why the branches view is built on `all_branches`.
    let states = db.view("SELECT branch_id, state FROM ferro_branches;");
    let at = states.rows.iter().position(|r| text_of(&r[0]) == held.id.to_string());
    assert_eq!(
        at.map(|i| text_of(&states.rows[i][1])),
        Some("Quarantined".to_string()),
        "a held branch is missing or mislabelled in ferro_branches: {:?}",
        states.rows
    );
}

/// **Projection and `WHERE` over a view narrow the columns AND the rows.**
///
/// This is what makes the output schema come from the plan rather than from the view definition:
/// `SELECT reason FROM ...` must advertise one column called `reason`, not all four.
#[test]
fn a_view_can_be_projected_and_filtered() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'first' RUN 'r_1';", &mut a);
    let first = a.agent.as_ref().unwrap().branch;
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'second' RUN 'r_2';", &mut b);

    let all = db.view("SELECT * FROM ferro_runs;");
    assert_eq!(all.len(), 2, "two open sessions: {:?}", all.rows);

    let one = db.view(&format!("SELECT agent_id, run_id FROM ferro_runs WHERE branch_id = {};", first.id));
    assert_eq!(one.header(), vec!["agent_id", "run_id"], "the projection did not narrow the columns");
    assert_eq!(one.len(), 1, "the WHERE did not narrow the rows: {:?}", one.rows);
    assert_eq!(column(&one, "agent_id"), vec!["first"]);

    // A predicate that matches nothing still returns the projected column list.
    let none = db.view("SELECT run_id FROM ferro_runs WHERE branch_id = 9999;");
    assert!(none.is_empty());
    assert_eq!(none.header(), vec!["run_id"]);

    // An alias qualifies the view's columns, exactly as it does for a table.
    let aliased = db.view("SELECT r.agent_id FROM ferro_runs r;");
    assert_eq!(aliased.header(), vec!["agent_id"]);
    assert_eq!(aliased.len(), 2);

    // A computed projection column: named `?column?` and, crucially, NOT typed from the
    // placeholder. Its declared type here is the binder's `Integer` guess while the value is a
    // BOOLEAN, which is the mismatch `pgwire::fields_of` has to resolve from the value.
    let computed = db.view("SELECT branch_id = 9999 FROM ferro_runs;");
    assert_eq!(computed.header(), vec!["?column?"]);
    assert!(
        matches!(computed.rows[0][0], Value::Boolean(false)),
        "a comparison in the select list did not evaluate: {:?}",
        computed.rows
    );

    // An unknown column in a view is a bind error naming the view, not a silent empty result.
    let err = {
        let mut s = db.session();
        refusal(db.exec("SELECT nosuchcolumn FROM ferro_runs;", &mut s), "an unknown view column")
    };
    assert!(err.to_string().contains("nosuchcolumn"), "{err}");
}

/// **A table may not take a system view's name.**
///
/// The state this makes unrepresentable: `executor::run` resolves a view name before any other
/// route, so a table called `ferro_quarantine` would accept every INSERT and answer every SELECT
/// from the view. Writes in, nothing out, no error at any point.
#[test]
fn a_table_cannot_shadow_a_system_view() {
    let mut db = Db::new();
    let mut s = db.session();
    for name in VIEW_NAMES {
        let err = refusal(
            db.exec(&format!("CREATE TABLE {name} (id INTEGER NOT NULL);"), &mut s),
            "creating a table over a view name",
        );
        assert!(err.to_string().contains(name), "the refusal does not name the collision: {err}");
        assert!(
            err.to_string().contains("system view"),
            "the refusal does not say why: {err}"
        );
    }
    // Anti-vacuity: the guard refuses view names and nothing else. Without this half, a guard that
    // rejected every CREATE TABLE would pass the loop above.
    db.ok("CREATE TABLE ferro_something_else (id INTEGER NOT NULL);", &mut s);
    db.ok("CREATE TABLE ferro (id INTEGER NOT NULL);", &mut s);
    db.ok("INSERT INTO ferro VALUES (1);", &mut s);
    match db.ok("SELECT id FROM ferro;", &mut s) {
        Outcome::Rows(r) => assert_eq!(r.len(), 1, "an ordinary table near a view name broke"),
        other => panic!("a real table answered as {}", outcome_kind(&other)),
    }
}

/// **Both refusals a view makes, and the fact that dropping the qualifier works.**
#[test]
fn a_view_refuses_as_of_branch_and_a_join_rather_than_answering_something_else() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r_a';", &mut a);
    let name = format!("b_{}", a.agent.as_ref().unwrap().branch.id);

    let err = refusal(
        db.exec(&format!("SELECT * FROM ferro_runs AS OF BRANCH {name};"), &mut a),
        "AS OF BRANCH on a system view",
    );
    assert!(err.to_string().contains("system view"), "{err}");
    assert!(err.to_string().contains(&name), "the refusal does not name the branch asked for: {err}");

    let err = refusal(
        db.exec("SELECT * FROM ferro_runs JOIN oncall ON ferro_runs.branch_id = oncall.id;", &mut a),
        "joining a system view",
    );
    assert!(err.to_string().contains("join"), "{err}");

    // Anti-vacuity for both: the same statement without the qualifier, and the same view on its
    // own, are fine. A `run_select` that refused everything would satisfy the two assertions above.
    let ok = db.view("SELECT * FROM ferro_runs;");
    assert_eq!(ok.len(), 1);
}

/// **A view is readable from inside an agent session.**
///
/// The reason the dispatch arm is the first check in `run` and not a fallback after the agent
/// routes: inside a session, an ordinary `SELECT` goes to `run_in_session` → `runtime.select` →
/// `require_table`, which does not know view names and would answer `unknown table`. An agent asking
/// what the branch engine thinks of its own branch is the main thing these views are for.
#[test]
fn an_agent_can_read_the_views_from_inside_its_own_session() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'introspect' RUN 'r_i';", &mut a);
    db.ok("UPDATE oncall SET qty = 7 WHERE id = 2;", &mut a);

    let out = db.ok("SELECT agent_id, staged_rows FROM ferro_run_activity;", &mut a);
    match out {
        Outcome::Table(t) => {
            assert_eq!(t.header(), vec!["agent_id", "staged_rows"]);
            assert_eq!(column(&t, "agent_id"), vec!["introspect"]);
            assert_eq!(column(&t, "staged_rows"), vec!["1"]);
        }
        other => panic!("a view read inside a session returned {}", outcome_kind(&other)),
    }

    // The session is still open and still functional afterwards — the view read did not consume or
    // disturb it.
    assert!(a.agent.is_some());
    let after = db.ok("SELECT qty FROM oncall WHERE id = 2;", &mut a);
    match after {
        Outcome::Rows(r) => assert_eq!(text_of(&r[0][0]), "7"),
        other => panic!("{}", outcome_kind(&other)),
    }
}

/// **The counters count what the run did, and not what a sibling did.**
///
/// The breaking shape: TWO concurrent runs. A counter derived from a runtime-wide total rather than
/// from each workspace would give both branches the same numbers, and a single-agent workload could
/// never tell the difference.
#[test]
fn run_activity_counts_are_per_run_and_not_a_shared_total() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'writer' RUN 'r_w';", &mut a);
    db.ok("UPDATE oncall SET qty = 1 WHERE id = 1;", &mut a);
    db.ok("UPDATE oncall SET qty = 2 WHERE id = 2;", &mut a);
    db.ok("UPDATE oncall SET qty = 3 WHERE id = 3;", &mut a);

    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'reader' RUN 'r_r';", &mut b);
    db.ok("SELECT qty FROM oncall WHERE id = 1;", &mut b);

    let act = db.view("SELECT agent_id, staged_rows, rows_read_exact, blind_writes FROM ferro_run_activity;");
    assert_eq!(act.len(), 2, "{:?}", act.rows);

    let mut by_agent: Vec<(String, String, String, String)> = act
        .rows
        .iter()
        .map(|r| (text_of(&r[0]), text_of(&r[1]), text_of(&r[2]), text_of(&r[3])))
        .collect();
    by_agent.sort();
    assert_eq!(
        by_agent,
        vec![
            // reader: nothing staged, one exact read, no blind write.
            ("reader".into(), "0".into(), "1".into(), "0".into()),
            // writer: three rows staged, nothing read, so all three are blind writes.
            ("writer".into(), "3".into(), "0".into(), "3".into()),
        ],
        "the counters are not per-run"
    );
}

/// **An agent statement crosses as typed columns, not as one `Debug` string.**
///
/// The breaking shape is any client that wants a *field*: before B9 a `MERGE` arrived as a single
/// `text` column holding `MergeReport { merge_id: "m_1", ... }`, so reading `applied_to_target` out
/// of it meant parsing Rust. `applied_to_target` is the field that says whether the target was
/// touched at all, which is exactly what a held merge has to communicate.
#[test]
fn an_agent_result_has_typed_columns_a_client_can_read() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    let started = match db.ok("BEGIN AGENT SESSION AS 'typed' RUN 'r_t';", &mut a) {
        Outcome::Agent(out) => out.to_rows(),
        other => panic!("{}", outcome_kind(&other)),
    };
    assert_eq!(
        started.header(),
        vec!["branch_id", "generation", "branch_name", "agent_id", "run_id", "prov_id", "txn_id"]
    );
    assert_eq!(started.len(), 1);
    assert_eq!(column(&started, "agent_id"), vec!["typed"]);

    db.ok("UPDATE oncall SET qty = qty - 5 WHERE qty >= 5;", &mut a);

    let diff = match db.ok("DIFF;", &mut a) {
        Outcome::Agent(out) => out.to_rows(),
        other => panic!("{}", outcome_kind(&other)),
    };
    assert!(diff.column_index("row_id").is_some(), "{:?}", diff.header());
    assert_eq!(diff.len(), 3, "three rows matched the guard: {:?}", diff.rows);
    assert_eq!(column(&diff, "change"), vec!["UPDATE"; 3]);
    assert!(
        column(&diff, "guard_predicates").iter().all(|p| p.contains("qty")),
        "the guard did not come through per row: {:?}", diff.rows
    );

    let merge = match db.ok("MERGE;", &mut a) {
        Outcome::Agent(out) => out.to_rows(),
        other => panic!("{}", outcome_kind(&other)),
    };
    assert!(merge.column_index("applied_to_target").is_some(), "{:?}", merge.header());
    assert!(!merge.is_empty(), "a merge with a verdict returned no rows");
    assert!(
        column(&merge, "applied_to_target").iter().all(|v| v == "true"),
        "{:?}",
        merge.rows
    );
    assert!(column(&merge, "merge_id")[0].starts_with("m_"));

    // Every variant must be non-empty except an empty DIFF, and every declared column count must
    // match every row's width. A `to_rows` that returned a schema wider than its rows would put
    // values under the wrong names for the whole result.
    for (what, rows) in [("session", &started), ("diff", &diff), ("merge", &merge)] {
        for r in &rows.rows {
            assert_eq!(
                r.len(),
                rows.columns.len(),
                "{what} produced a row of {} against {} columns",
                r.len(),
                rows.columns.len()
            );
        }
    }
}

/// **An empty DIFF still carries its column list, and a merge with nothing to do still has a verdict.**
#[test]
fn an_empty_agent_result_is_distinguishable_from_a_broken_one() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'idle' RUN 'r_idle';", &mut a);

    let diff = match db.ok("DIFF;", &mut a) {
        Outcome::Agent(out) => out.to_rows(),
        other => panic!("{}", outcome_kind(&other)),
    };
    assert!(diff.is_empty(), "an untouched branch has no changes");
    assert!(!diff.columns.is_empty(), "an empty DIFF dropped its columns too");
    assert!(diff.column_index("change").is_some(), "{:?}", diff.header());

    // A merge with no rows must STILL report a verdict. This is the case a per-row-only shape would
    // drop entirely, and it is the shape a gate-held merge takes.
    let merge = match db.ok("MERGE;", &mut a) {
        Outcome::Agent(out) => out.to_rows(),
        other => panic!("{}", outcome_kind(&other)),
    };
    assert_eq!(merge.len(), 1, "a merge with no row outcomes reported nothing at all");
    assert_eq!(column(&merge, "outcome"), vec!["Clean"]);
    assert_eq!(column(&merge, "applied_to_target"), vec!["true"]);
    assert_eq!(column(&merge, "table_name"), vec!["NULL"], "no row, so the row columns are NULL");
}


/// **A table that already carries a view's name keeps its rows.**
///
/// The breaking shape is a database written BEFORE these views existed. `Catalog::load` rebuilds
/// `tables` straight from the catalog pages and never passes through `create_table`, so the
/// collision guard cannot see such a table — and with the view answering first, every row in it would
/// be unreachable: writes accepted, reads answering the view, no error anywhere. That is precisely
/// the state the guard exists to prevent, arriving through the one door the guard does not watch.
///
/// The state is reproduced the way `load` produces it: the entry is put into `Catalog::tables` under
/// the colliding key directly, bypassing `create_table`. Nothing else in this file reaches it,
/// because every other test creates its tables through SQL.
#[test]
fn a_table_that_predates_the_views_still_answers_for_its_own_name() {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE legacy (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    db.ok("INSERT INTO legacy VALUES (7, 70);", &mut s);

    // Exactly what `Catalog::load` would hand back for an old database that has a table called
    // `ferro_runs`: a real entry, real pages, under a name `create_table` would now refuse.
    let mut entry = db.catalog.tables.remove("legacy").expect("the table exists");
    entry.name = "ferro_runs".to_string();
    db.catalog.tables.insert("ferro_runs".to_string(), entry);

    match db.ok("SELECT id, qty FROM ferro_runs;", &mut s) {
        Outcome::Rows(r) => {
            assert_eq!(r.len(), 1, "the table's row is unreachable: {r:?}");
            assert_eq!(text_of(&r[0][0]), "7");
            assert_eq!(text_of(&r[0][1]), "70");
        }
        Outcome::Table(t) => panic!(
            "the view answered for a name a real table holds, so the table's rows are unreachable: {:?}",
            t.header()
        ),
        other => panic!("{}", outcome_kind(&other)),
    }
    // Writes reach the table too, not a refusal about a view.
    db.ok("INSERT INTO ferro_runs VALUES (8, 80);", &mut s);
    match db.ok("SELECT id FROM ferro_runs;", &mut s) {
        Outcome::Rows(r) => assert_eq!(r.len(), 2, "the INSERT did not land in the table: {r:?}"),
        other => panic!("{}", outcome_kind(&other)),
    }

    // Anti-vacuity: a view whose name NOTHING has claimed still answers as a view in the same
    // database. Without this half, a `view_for` that always returned `None` would pass above.
    let q = db.view("SELECT * FROM ferro_quarantine;");
    assert_eq!(q.header(), vec!["branch_id", "generation", "branch", "reason"]);
}

/// **Read-only is a refusal that names the view, not `unknown table`.**
///
/// Before this, every write shape fell through to `require_table` and answered `unknown table
/// 'ferro_runs'` — about a name the very next `SELECT` resolves. A right refusal with a wrong reason
/// sends the reader hunting for a typo that is not there.
#[test]
fn every_write_shape_against_a_view_refuses_by_name() {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE oncall (id INTEGER NOT NULL, qty INTEGER);", &mut s);

    let statements = [
        "INSERT INTO ferro_runs VALUES (1, 2);",
        "UPDATE ferro_runs SET run_id = 'x';",
        "DELETE FROM ferro_runs;",
        "DROP TABLE ferro_runs;",
        "CREATE INDEX ix ON ferro_runs (run_id);",
        "ANALYZE ferro_runs;",
    ];
    for sql in statements {
        let err = refusal(db.exec(sql, &mut s), sql);
        let msg = err.to_string();
        assert!(msg.contains("ferro_runs"), "{sql}: {msg}");
        assert!(msg.contains("read-only system view"), "{sql}: {msg}");
        assert!(
            !msg.contains("unknown table"),
            "{sql} still answers as if the name did not exist: {msg}"
        );
    }

    // EXPLAIN has no plan to describe, and says so rather than answering `unknown table`.
    let err = refusal(db.exec("EXPLAIN SELECT * FROM ferro_quarantine;", &mut s), "EXPLAIN of a view");
    assert!(err.to_string().contains("ferro_quarantine"), "{err}");
    assert!(err.to_string().contains("no physical plan"), "{err}");
    assert!(!err.to_string().contains("unknown table"), "{err}");

    // Anti-vacuity, twice over: the same shapes against a real table still work, and EXPLAIN of a
    // real table still explains. A blanket refusal keyed on the statement kind would pass above.
    db.ok("INSERT INTO oncall VALUES (1, 10);", &mut s);
    db.ok("UPDATE oncall SET qty = 11 WHERE id = 1;", &mut s);
    db.ok("ANALYZE oncall;", &mut s);
    match db.ok("EXPLAIN SELECT * FROM oncall;", &mut s) {
        Outcome::Explain(text) => assert!(!text.trim().is_empty(), "EXPLAIN produced nothing"),
        other => panic!("{}", outcome_kind(&other)),
    }
    db.ok("DELETE FROM oncall;", &mut s);
    db.ok("DROP TABLE oncall;", &mut s);
}
