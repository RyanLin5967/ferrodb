use crate::catalog::column::DataType;
use crate::wal::log::DdlOp;
use crate::wal::txn::DdlRecord;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::agent_sql::dispatch::{is_agent_stmt, run_agent_stmt, run_in_session, AgentOutput};
use crate::binder::binder::BoundExpr;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::provenance::{ProvId, ProvenanceStore};
use crate::catalog::column::Value;
use crate::catalog::schema::Schema;
use crate::execution::index_handle::IndexHandle;
use crate::execution::session::Session;
use crate::parser::parser::{Stmt};
use crate::planner::plan::{Plan, explain, plan};
use crate::storage::index::BPlusTreeManager;
use crate::wal::txn::{ReadView, TxnManager};
use crate::{error::FerroError};
use crate::storage::heap_file_manager::RecordId;
use crate::parser::scanner::TokenType;

pub trait Executor {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>>;
}

pub trait Modify {
    fn execute(&mut self, catalog: &mut Catalog) -> Result<usize, FerroError>;

    /// Attribute every version this statement writes to `id`.
    ///
    /// Default is a no-op, which leaves versions unattributed — `ProvId::NONE`, the honest answer
    /// for a write made outside any agent session. Only statements run inside an agent session
    /// get an author, and it is attached here rather than threaded through `plan()` so the
    /// planner keeps knowing nothing about sessions.
    fn set_author(&mut self, _prov: Arc<dyn ProvenanceStore>, _id: ProvId) {}
}

pub enum Outcome {
    Rows(Vec<Vec<Value>>),
    Affected(usize),
    Explain(String),
    /// The structured result of an agent-session statement.
    Agent(AgentOutput),
    Ok,
}

pub fn run(stmt: Stmt, catalog: &mut Catalog, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>, session: &mut Session) -> Result<Outcome, FerroError> {
    // Agent-session statements, and any read explicitly qualified with AS OF BRANCH.
    if is_agent_stmt(&stmt) {
        return run_agent_stmt(stmt, catalog, bp, txn, session);
    }
    // Inside an agent session, DML is captured on that session's branch instead of being applied
    // to the shared tables — that is what makes a branch's writes invisible until MERGE.
    if session.agent.is_some()
        && matches!(stmt, Stmt::Select { .. } | Stmt::Insert { .. } | Stmt::Update { .. } | Stmt::Delete { .. })
    {
        return run_in_session(stmt, catalog, bp, txn, session);
    }
    match stmt {
        Stmt::Begin => {
            if session.current.is_some() {
                return Err(FerroError::Txn("txn already started".into()))
            }
            session.current = Some(txn.begin()?);
            Ok(Outcome::Ok)
        }
        Stmt::Commit => match session.current.take() {
            Some(id) => {
                txn.commit(id)?;
                Ok(Outcome::Ok)
            }
            None => Err(FerroError::Txn("not in active txn".into()))
        }
        Stmt::Rollback => match session.current.take() {
            Some(id) => {
                txn.abort(id)?;
                Ok(Outcome::Ok)
            }
            None => Err(FerroError::Txn("not in active txn".into()))
        }
        Stmt::CreateIndex { table, column_name , ..} => {
            if session.current.is_some() {
                return Err(FerroError::Txn("DDL not allowed in txn".into()))
            }
            catalog.create_index(&table, &column_name)?;
            txn.checkpoint()?;
            return Ok(Outcome::Ok)
        }
        Stmt::CreateTable { table, columns } => {
            if session.current.is_some() {
                return Err(FerroError::Txn("DDL not allowed in txn".into()))
            }
            let name = table.clone();
            let spec: Vec<(String, DataType, bool)> = columns
                .iter()
                .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                .collect();
            catalog.create_table(table, Schema{columns})?;
            txn.checkpoint()?;

            // Logged AFTER the checkpoint, and that ordering is not stylistic: `checkpoint`
            // truncates the WAL, so a DDL record written before it would be discarded by the very
            // call that follows it — the record would exist for microseconds and no reader would
            // ever see one.
            //
            // This record does not drive recovery; the catalog is still authoritative for the
            // running database. It exists so a reader of the log knows what the tables were, which
            // is what lets the change feed carry schema instead of assuming today's catalog
            // describes yesterday's rows.
            if let Some(entry) = catalog.get_table(&name) {
                // Through `log_ddl`, not a bare append: the record must be RETAINED, because the
                // next checkpoint truncates the log and would otherwise erase it. Creating a second
                // table is itself a checkpoint, so a bare append meant the first table's schema
                // survived exactly until the second one was created.
                txn.log_ddl(DdlRecord {
                    op: DdlOp::CreateTable,
                    table: name.clone(),
                    dir_root: entry.first_directory_page_id,
                    time_travel_root: entry.time_travel_root,
                    columns: spec,
                })?;
            }
            return Ok(Outcome::Ok)
        }
        Stmt::Analyze { table } => {
            catalog.analyze(&table)?;
            return Ok(Outcome::Ok)
        }
        Stmt::Explain(s) => {
            let text = explain(*s, catalog)?;
            return Ok(Outcome::Explain(text));
        }
        dml => {
            if matches!(dml, Stmt::Select { .. }) {
                let view = Arc::new(match session.current {
                    Some(txn_id) => ReadView { snapshot: txn.snapshot_of(txn_id)?, txn_id},
                    None => ReadView { snapshot: txn.read_snapshot(), txn_id: 0 }
                });
                match plan(dml, catalog, bp.clone(), None, view)? {
                    Plan::Read(mut root) => {
                        let mut res = Vec::new();
                        loop {
                            let (_, values) = match root.next() {
                                Some(Ok((r, v))) => (r, v),
                                Some(Err(e)) => return Err(e),
                                None =>{break;}
                            };
                            res.push(values);
                        }
                        return Ok(Outcome::Rows(res))
                    }
                    Plan::Write(_) => unreachable!()
                }
            } else {
                let (txn_id, implicit) = match session.current {
                    Some(id) => (id, false),
                    None => (txn.begin()?, true)
                };
                let view = match txn.snapshot_of(txn_id) {
                    Ok(snapshot) => Arc::new(ReadView { snapshot, txn_id }),
                    Err(e) => {
                        txn.abort(txn_id)?;
                        session.current = None;
                        return Err(e)
                    }
                };
                let planned = match plan(dml, catalog, bp.clone(), Some((txn.clone(), txn_id)), view) {
                    Ok(p) => p,
                    Err(e) => {
                        txn.abort(txn_id)?;
                        session.current = None;
                        return Err(e);
                    }
                };
                match planned {
                    // No author is attached here, and that is not an omission. DML inside an
                    // agent session returns above at `run_in_session`, so a write reaching this
                    // arm is by definition outside every session and has no agent to name — the
                    // version stays `ProvId::NONE`. Rows an agent wrote are stamped where they
                    // actually reach shared storage: the merge publish path in
                    // `agent_sql::runtime`, which carries the run down to `Modify::set_author`.
                    Plan::Write(mut op) => match op.execute(catalog) {
                        Ok(count) => {
                            if implicit { txn.commit(txn_id)? };
                            return Ok(Outcome::Affected(count))
                        }
                        Err(e) => {
                            txn.abort(txn_id)?;
                            session.current = None;
                            Err(e)
                        }
                    },
                    Plan::Read(_) => unreachable!()
                }
            }
        }
    }
}

pub fn sync_roots(table: &str, schema: &Schema, primary: &BPlusTreeManager<Value, RecordId>, secondaries: &[IndexHandle], catalog: &mut Catalog) -> Result<(), FerroError> {
    let cur_primary = primary.root_page_id.load(Ordering::Relaxed);
    let stored_primary = catalog.get_table(table).ok_or(FerroError::KeyNotFound)?.primary_index_root;
    if cur_primary != stored_primary {
        catalog.update_primary_root(table, cur_primary)?;
    }
    for handle in secondaries {
        let cur = handle.tree.root_page_id.load(Ordering::Relaxed);
        let col_name = schema.columns[handle.col_index].name.clone();
        let stored = catalog.get_table(table).and_then(|e| e.indexes.iter().find(|i| i.column_name == col_name).map(|i| i.root_page_id));
        if stored != Some(cur) {
            catalog.update_index_root(table, &col_name, cur)?;
        }
    }
    Ok(())
}
pub fn evaluate(expr: &BoundExpr, row: &[Value]) -> Result<Value, FerroError> {
    return match expr {
        BoundExpr::Literal(v) => Ok(v.clone()),
        BoundExpr::BinaryOp { left, operator, right } => {
            let l = evaluate(left, row)?;
            let r = evaluate(right ,row)?;

            match operator {
                TokenType::Plus | TokenType::Minus | TokenType::Star | TokenType::Slash => arithmetic(&l, &r, operator),
                TokenType::Equal | TokenType::BangEqual | TokenType::Less | TokenType::LessEqual
                | TokenType::Greater | TokenType::GreaterEqual => compare(&l, &r, operator),
                TokenType::And | TokenType::Or => logical(&l, &r, operator),
                _ => Err(FerroError::Parse("invalid binary op".into()))
            }
        }
        BoundExpr::UnaryOp { operator, right } => {
            let v = evaluate(right, row)?;
            match operator {
                TokenType::Minus => match v {
                    Value::Integer(i) => Ok(Value::Integer(-i)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    // `checked_neg` rather than `-i`: negating i64::MIN has no i64 result, and
                    // wrapping it back to itself would turn the most negative timestamp in the
                    // table into itself with the opposite meaning.
                    Value::BigInt(i) => i.checked_neg().map(Value::BigInt)
                        .ok_or_else(|| FerroError::Parse(format!("negating {i} overflows BIGINT"))),
                    Value::Timestamp(ms) => ms.checked_neg().map(Value::Timestamp)
                        .ok_or_else(|| FerroError::Parse(format!("negating {ms} overflows TIMESTAMP"))),
                    Value::Decimal(d) => Ok(Value::Decimal(match d.strip_prefix('-') {
                        Some(rest) => rest.to_string(),
                        None => format!("-{d}"),
                    })),
                    Value::Null => Ok(Value::Null),
                    _ => Err(FerroError::Parse("unary minus non numeric".into()))
                },
                TokenType::Not => match v {
                    Value::Boolean(b) => Ok(Value::Boolean(!b)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(FerroError::Parse("not on non boolean".into()))
                },
                _ => Err(FerroError::Parse("invalid unary op".into()))
            }
        }
        BoundExpr::Column(idx) => row.get(*idx).cloned().ok_or_else(|| FerroError::Parse(format!("row missing column at {}", idx)))
    }
}

fn arithmetic(l: &Value, r: &Value, op: &TokenType) -> Result<Value, FerroError> {
    match (l, r) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(a), Value::Integer(b)) => {
            let res = match op {
                TokenType::Plus => a + b,
                TokenType::Minus => a - b,
                TokenType::Star => a * b,
                TokenType::Slash => {
                    if *b == 0 {return Err(FerroError::Parse("div by 0".into()))}
                    a/b
                }
                _ => return Err(FerroError::Parse("invalid arithmetic op".into()))
            };
            Ok(Value::Integer(res))
        }
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(float_arith(*a, *b, *op)?)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(float_arith(*a as f64, *b, *op)?)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(float_arith(*a, *b as f64, *op)?)),

        // ---- the i64-backed wide types ----------------------------------------------------
        //
        // Without these, `SELECT big + 1` and `UPDATE t SET big = big + 1` both failed with
        // "can't add non numbers" — for two operands that are unambiguously numbers. A 64-bit
        // integer column that cannot be incremented is not a usable integer type; counting is
        // most of what one is for.
        //
        // Widening an i32 to i64 is exact, so mixing BIGINT with INTEGER promotes rather than
        // refuses. The result stays `BigInt` rather than detouring through f64: past 2^53 a
        // double cannot tell one increment from the next, so an f64 route would turn a hard
        // error into a silently wrong count, which is the worse of the two.
        (Value::BigInt(a), Value::BigInt(b)) => int_arith(*a, *b, op).map(Value::BigInt),
        (Value::BigInt(a), Value::Integer(b)) => int_arith(*a, *b as i64, op).map(Value::BigInt),
        (Value::Integer(a), Value::BigInt(b)) => int_arith(*a as i64, *b, op).map(Value::BigInt),

        // A TIMESTAMP is a point in time, not a count, so the algebra is not the same as BIGINT's:
        // shifting one by a number of milliseconds gives another instant, while differencing two
        // gives an elapsed count that is no longer an instant. Anything else — a product of two
        // instants, an instant divided by a date — has no meaning to give, so it is refused rather
        // than assigned one.
        (Value::Timestamp(a), Value::Timestamp(b)) if matches!(op, TokenType::Minus) => {
            int_arith(*a, *b, op).map(Value::BigInt)
        }
        (Value::Timestamp(a), Value::Integer(b)) if is_shift(op) => {
            int_arith(*a, *b as i64, op).map(Value::Timestamp)
        }
        (Value::Timestamp(a), Value::BigInt(b)) if is_shift(op) => {
            int_arith(*a, *b, op).map(Value::Timestamp)
        }
        // `1000 + ts` is the same instant as `ts + 1000`; `1000 - ts` is not an instant at all.
        (Value::Integer(a), Value::Timestamp(b)) if matches!(op, TokenType::Plus) => {
            int_arith(*a as i64, *b, op).map(Value::Timestamp)
        }
        (Value::BigInt(a), Value::Timestamp(b)) if matches!(op, TokenType::Plus) => {
            int_arith(*a, *b, op).map(Value::Timestamp)
        }

        // Mixing an exact i64 with a float is the one place exactness is deliberately given up,
        // and only because the statement asked for it by naming a float operand. It follows the
        // rule INTEGER already used, so `big * 1.5` behaves the way `qty * 1.5` always has.
        (Value::BigInt(a), Value::Float(b)) => Ok(Value::Float(float_arith(*a as f64, *b, *op)?)),
        (Value::Float(a), Value::BigInt(b)) => Ok(Value::Float(float_arith(*a, *b as f64, *op)?)),

        // DECIMAL is stored as the digit text the user wrote, with no scaled-integer backing —
        // see the type's own doc, which says this engine stores and ships decimals rather than
        // adding them. Routing it through f64 to make an answer appear would round exactly the
        // digits the type exists to keep, so the refusal is deliberate and says which type it is
        // refusing instead of claiming a DECIMAL is not a number.
        (Value::Decimal(_), _) | (_, Value::Decimal(_)) => Err(FerroError::Parse(
            "DECIMAL arithmetic is not supported: this engine stores and ships exact decimals \
             rather than computing on them, and evaluating one as a float would round away the \
             digits the type exists to preserve"
                .into(),
        )),

        _ => Err(FerroError::Parse("can't add non numbers".into()))
    }
}

/// Does `op` shift a TIMESTAMP along the line rather than combine two of them?
fn is_shift(op: &TokenType) -> bool {
    matches!(op, TokenType::Plus | TokenType::Minus)
}

/// `i64` arithmetic that refuses to wrap.
///
/// The i32 path above uses bare operators, which panic in a debug build and WRAP in a release one.
/// Wrapping is the failure mode that matters here: a BIGINT column exists to hold values near the
/// i64 extremes, `9223372036854775807 + 1` is reachable from ordinary SQL, and silently answering
/// `-9223372036854775808` would be a wrong number reported as a success.
fn int_arith(a: i64, b: i64, op: &TokenType) -> Result<i64, FerroError> {
    let overflow = || FerroError::Parse(format!("64-bit integer overflow in `{a} {op:?} {b}`"));
    match op {
        TokenType::Plus => a.checked_add(b).ok_or_else(overflow),
        TokenType::Minus => a.checked_sub(b).ok_or_else(overflow),
        TokenType::Star => a.checked_mul(b).ok_or_else(overflow),
        TokenType::Slash => {
            if b == 0 {
                return Err(FerroError::Parse("div by 0".into()));
            }
            // i64::MIN / -1 is the one division that overflows.
            a.checked_div(b).ok_or_else(overflow)
        }
        _ => Err(FerroError::Parse("invalid arithmetic op".into())),
    }
}

fn float_arith(a: f64, b: f64, op: TokenType) -> Result<f64, FerroError> {
    Ok(match op {
        TokenType::Plus => a + b,
        TokenType::Minus => a-b,
        TokenType::Star => a * b,
        TokenType::Slash => {
            if b == 0.0 {return Err(FerroError::Parse("div by 0".into()));}
            a/b
        }
        _ => return Err(FerroError::Parse("invalid arithmetic op".into()))
    })
}

fn compare(l: &Value, r: &Value, op: &TokenType) -> Result<Value, FerroError> {
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Ok(Value::Null)
    }
    let res = match op {
        TokenType::Equal => l== r,
        TokenType::BangEqual => l != r,
        TokenType::Less => l < r,
        TokenType::LessEqual => l <= r,
        TokenType::Greater => l > r,
        TokenType::GreaterEqual => l >= r,
        _ => return Err(FerroError::Parse("invalid comparison op".into()))
    };
    Ok(Value::Boolean(res))
}

fn logical(l: &Value, r: &Value, op: &TokenType) -> Result<Value, FerroError> {
    let lb = as_bool_opt(l)?;
    let rb = as_bool_opt(r)?;
    let res = match op {
        TokenType::And => match (lb, rb) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None
        }
        TokenType::Or => match (lb, rb) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None
        }
        _ => return Err(FerroError::Parse("invalid logical op".into()))
    };
    Ok(res.map_or(Value::Null, Value::Boolean))
}

fn as_bool_opt(v: &Value) -> Result<Option<bool>, FerroError> {
    match v {
        Value::Boolean(b) => Ok(Some(*b)),
        Value::Null => Ok(None),
        _ => Err(FerroError::Parse("expected bool".into()))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::ops::Bound;
    use super::*;
    use crate::parser::scanner::Scanner;
    use crate::parser::parser::Parser;
    use crate::storage::disk_manager::DiskManager;
use crate::storage::heap_file_manager::HeapFileManager;
use crate::wal::log::WalManager;
    use tempfile::tempdir;

    fn parse_one(sql: &str) -> Result<Stmt, FerroError> {
        let chars: Vec<char> = sql.chars().collect();
        let tokens = Scanner::new(chars, Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(parser.errors.remove(0))
        }
        stmts.into_iter().next().ok_or(FerroError::SqlParseError("no statement found".into()))
    }

    fn exec(sql: &str, catalog: &mut Catalog, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>) -> Result<Outcome, FerroError> {
        let mut session = Session::new();
        run(parse_one(sql)?, catalog, bp, txn, &mut session)
    }

    fn setup() -> (Catalog, Arc<BufferPoolManager>, tempfile::TempDir, Arc<TxnManager>) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("exec.db");
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("test.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        (catalog, bp, dir, txn)
    } 

    fn seed() -> (Catalog, Arc<BufferPoolManager>, tempfile::TempDir, Arc<TxnManager>) {
        let (mut c, bp, dir, txn) = setup();
        exec("CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(50));", &mut c, bp.clone(), txn.clone()).unwrap();
        for s in [
            "INSERT INTO users VALUES (1, 'alice');",
            "INSERT INTO users VALUES (2, 'bob');",
            "INSERT INTO users VALUES (3, 'carol');",
        ] {
            exec(s, &mut c, bp.clone(), txn.clone()).unwrap();
        }
        (c, bp, dir, txn)
    }

    fn seed_join() -> (Catalog, Arc<BufferPoolManager>, tempfile::TempDir, Arc<TxnManager>) {
        let (mut c, bp, dir, txn) = setup();
        exec("CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(50));", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("CREATE TABLE posts (id INTEGER NOT NULL, user_id INTEGER, title VARCHAR(50));", &mut c, bp.clone(), txn.clone()).unwrap();
        for s in [
            "INSERT INTO users VALUES (1, 'alice');",
            "INSERT INTO users VALUES (2, 'bob');",
            "INSERT INTO users VALUES (3, 'carol');",
            "INSERT INTO posts VALUES (10, 1, 'hi');",
            "INSERT INTO posts VALUES (11, 1, 'yo');",
            "INSERT INTO posts VALUES (12, 2, 'sup');",
            "INSERT INTO posts VALUES (13, 99, 'orphan');",
        ] {
            exec(s, &mut c, bp.clone(), txn.clone()).unwrap();
        }
        exec("ANALYZE users;", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("ANALYZE posts;", &mut c, bp.clone(), txn.clone()).unwrap();
        (c, bp, dir, txn)
    }

    fn name_title(rs: &[Vec<Value>]) -> Vec<(String, Option<String>)> {
        let mut v: Vec<(String, Option<String>)> = rs.iter().map(|r| {
            let name = match &r[0] { Value::Varchar(s) => s.clone(), _ => panic!() };
            let title = match &r[1] {
                Value::Varchar(s) => Some(s.clone()),
                Value::Null => None,
                _ => panic!()
            };
            (name, title)
        }).collect();
        v.sort();
        v
    }

    fn two_names(rs: &[Vec<Value>]) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = rs.iter().map(|r| {
            let a = match &r[0] {Value::Varchar(s) => s.clone(), _ => panic!()};
            let b = match &r[1] {Value::Varchar(s) => s.clone(), _ => panic!()};
            (a, b)
        }).collect();
        v.sort();
        v
    }

    fn rows(out: Outcome) -> Vec<Vec<Value>> {
        match out {
            Outcome::Rows(r) => r,
            _ => panic!("expected rows")
        }
    }

    fn affected(out: Outcome) -> usize {
        match out {
            Outcome::Affected(a) => a,
            _ => panic!("expected affected")
        }
    }

    fn sorted_ids(rs: &[Vec<Value>]) -> Vec<i32> {
        let mut v: Vec<i32> = rs.iter().map(|r| match &r[0] {Value::Integer(i) => *i, _ => panic!()}).collect();
        v.sort();
        v
    }

    fn exec_s(sql: &str, catalog: &mut Catalog, bp: &Arc<BufferPoolManager>, txn: &Arc<TxnManager>, session: &mut Session) -> Result<Outcome, FerroError>{
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty());
        run(stmts.remove(0), catalog, bp.clone(), txn.clone(), session)
    }

    #[test]
    fn test_analyze_basic() {
        let (mut c, _bp, _d, _txn) = seed();
        c.analyze("users").unwrap();
        let stats = c.stats.get("users").unwrap();
        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.columns[0].distinct, 3);
        assert_eq!(stats.columns[0].nulls, 0);
        assert_eq!(stats.columns[0].min, Some(Value::Integer(1)));
        assert_eq!(stats.columns[0].max, Some(Value::Integer(3)));

        assert_eq!(stats.columns[1].distinct, 3);
        assert_eq!(stats.columns[1].nulls, 0);
        assert_eq!(stats.columns[1].min, Some(Value::Varchar("alice".into())));
        assert_eq!(stats.columns[1].max, Some(Value::Varchar("carol".into())));
    }

    #[test]
    fn test_analyze_nulls_duplicates() {
        let (mut c, bp, _d, txn) = setup();
        exec("CREATE TABLE t (id INTEGER NOT NULL, val INTEGER);", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("INSERT INTO t VALUES (1, 10);", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("INSERT INTO t VALUES (2, 10);", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("INSERT INTO t VALUES (3, NULL);", &mut c, bp.clone(), txn).unwrap();
        c.analyze("t").unwrap();
        let stats = c.stats.get("t").unwrap();

        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.columns[1].distinct, 1);
        assert_eq!(stats.columns[1].nulls, 1);
        assert_eq!(stats.columns[1].min, Some(Value::Integer(10)));
        assert_eq!(stats.columns[1].max, Some(Value::Integer(10)));
    }

    #[test]
    fn test_analyze_empty_table() {
        let (mut c, bp, _d, txn) = setup();
        exec("CREATE TABLE a (id INTEGER NOT NULL);", &mut c, bp.clone(), txn.clone()).unwrap();
        c.analyze("a").unwrap();
        let stats = c.stats.get("a").unwrap();

        assert_eq!(stats.row_count, 0);
        assert_eq!(stats.columns[0].distinct, 0);
        assert_eq!(stats.columns[0].nulls, 0);
        assert_eq!(stats.columns[0].min, None);
        assert_eq!(stats.columns[0].max, None);
    }

    #[test]
    fn test_analyze_unknown_table_error() {
        let (mut c, _bp, _d, _txn) = setup();
        assert!(c.analyze("idk").is_err());
    }
    #[test]
    fn test_inner_join() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(name_title(&r), vec![("alice".into(), Some("hi".into())), ("alice".into(), Some("yo".into())), ("bob".into(), Some("sup".into()))]);
    }

    #[test]
    fn test_inner_join_with_keyword() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn test_join_select_star() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT * FROM users u INNER JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 3);
        assert!(r.iter().all(|row| row.len() == 5));
        assert!(r.iter().any(|row| row == &vec![
            Value::Integer(1), Value::Varchar("alice".into()),
            Value::Integer(10), Value::Integer(1), Value::Varchar("hi".into())
        ]));
    }

    #[test]
    fn test_join_with_where() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.id = 1;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(name_title(&r), vec![("alice".into(), Some("hi".into())), ("alice".into(), Some("yo".into()))]);
    }

    #[test]
    fn test_join_no_match() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.id;", &mut c, bp.clone(), txn).unwrap());
        assert!(r.is_empty());
    }

    #[test]
    fn test_left_join() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(name_title(&r), vec![("alice".into(), Some("hi".into())), ("alice".into(), Some("yo".into())), ("bob".into(), Some("sup".into())), ("carol".into(), None)]);
    }
    
    #[test]
    fn test_left_outer_keyword() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u LEFT OUTER JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 4);
    }

    #[test]
    fn test_left_no_match() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(name_title(&r), vec![("alice".into(), None), ("bob".into(), None), ("carol".into(), None)]);
    }

    #[test]
    fn test_self_join() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT a.name, b.name FROM users a JOIN users b ON a.id = b.id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(two_names(&r), vec![("alice".into(), "alice".into()), ("bob".into(), "bob".into()), ("carol".into(), "carol".into())]);
    }

    #[test]
    fn test_unsupported_join_type_error() {
        let (mut c, bp, _d, txn) = seed_join();
        assert!(exec("SELECT u.name, p.title FROM users u RIGHT JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).is_err());
    }

    #[test]
    fn test_select_all() {
        let (mut c, bp, _d, txn) = seed();
        let r = rows(exec("SELECT * FROM users;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 3);
        assert_eq!(sorted_ids(&r), vec![1,2,3]);
    }

    #[test]
    fn test_filter() {
        let (mut c, bp, _d, txn) = seed();
        let r = rows(exec("SELECT * FROM users WHERE id = 2;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Integer(2));
        assert_eq!(r[0][1], Value::Varchar("bob".into()));
    }

    #[test]
    fn test_comparison_filter() {
        let (mut c, bp, _d, txn) = seed();
        let r = rows(exec("SELECT * FROM users WHERE id > 1;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(sorted_ids(&r), vec![2, 3]);
    }

    #[test]
    fn test_projection(){ 
        let (mut c, bp, _d, txn) = seed();
        let r = rows(exec("SELECT name FROM users WHERE id = 1;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].len(), 1);
        assert_eq!(r[0][0], Value::Varchar("alice".into()));
    }

    #[test]
    fn test_update_then_select() {
        let (mut c, bp, _d, txn) = seed();
        assert_eq!(affected(exec("UPDATE users SET name = 'ALICE' WHERE id = 1;", &mut c, bp.clone(), txn.clone()).unwrap()), 1);
        let r = rows(exec("SELECT name FROM users WHERE id = 1;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r[0][0], Value::Varchar("ALICE".into()));
    }

    #[test]
    fn test_delete_then_select() {
        let (mut c, bp, _d, txn) = seed();
        assert_eq!(affected(exec("DELETE FROM users WHERE id = 2;", &mut c, bp.clone(), txn.clone()).unwrap()), 1);
        let r = rows(exec("SELECT * FROM users;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(sorted_ids(&r), vec![1,3]);
    }

    #[test]
    fn test_duplicate_primary_key_errors() {
        let (mut c, bp, _d, txn) = seed();
        assert!(exec("INSERT INTO users VALUES (1, 'dup');", &mut c, bp.clone(), txn).is_err());
    }

    /// **A deleted primary key can be used again.**
    ///
    /// It could not, and the error said "use UPDATE to change the existing row" while `SELECT`
    /// showed no such row. An index entry outlives the row it points at - DELETE stamps `end_ts` on
    /// the version in place and leaves the entry alone - so the uniqueness check was asking the
    /// index a question only the heap can answer.
    ///
    /// Reachable in three statements, which is how a reader would have found it.
    #[test]
    fn a_deleted_primary_key_can_be_inserted_again() {
        let (mut c, bp, _d, txn) = seed();

        // The seed already holds id 1, so removing it must free the key.
        exec("DELETE FROM users WHERE id = 1;", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("INSERT INTO users VALUES (1, 'reused');", &mut c, bp.clone(), txn.clone())
            .expect("a deleted primary key was still reported as taken");

        // Exactly one row with that key, and it is the new one - not two entries in a unique index.
        let r = exec("SELECT * FROM users WHERE id = 1;", &mut c, bp.clone(), txn.clone()).unwrap();
        let rows = rows(r);
        assert_eq!(rows.len(), 1, "expected one row for the reused key, got {rows:?}");
        assert!(
            format!("{rows:?}").contains("reused"),
            "the reused key returned the old row rather than the new one: {rows:?}"
        );
    }

    /// **The stale entry has to be removed, not shadowed.**
    ///
    /// `insert_entry` appends at the binary-search position; it does not overwrite. So reusing a key
    /// without removing the dead entry first leaves two entries for one key in a unique index, and
    /// `search` returns whichever one binary search lands on - here, the dead one. The next insert of
    /// that key then reads `end_ts != 0`, concludes the row is gone, and accepts a genuine duplicate.
    ///
    /// Measured through the shipped binary with the removal commented out: `INSERT (1,10); DELETE
    /// id=1; INSERT (1,99); INSERT (1,777)` left `1 | 99` and `1 | 777` both live under a unique
    /// primary key. That is why this test exists and why the fix deletes rather than shadows - a
    /// point the other two tests in this group do not reach, since one reuse is all they perform.
    #[test]
    fn reusing_a_key_does_not_let_the_next_duplicate_through() {
        let (mut c, bp, _d, txn) = seed();

        exec("DELETE FROM users WHERE id = 1;", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("INSERT INTO users VALUES (1, 'reused');", &mut c, bp.clone(), txn.clone()).unwrap();

        // Key 1 is live again, so this must be refused exactly as any other duplicate is.
        assert!(
            exec("INSERT INTO users VALUES (1, 'third');", &mut c, bp.clone(), txn.clone()).is_err(),
            "a duplicate was accepted after the key had been reused; the index is holding a dead \
             entry alongside the live one"
        );

        let r = exec("SELECT * FROM users WHERE id = 1;", &mut c, bp.clone(), txn.clone()).unwrap();
        let got = rows(r);
        assert_eq!(got.len(), 1, "a unique primary key ended up with several live rows: {got:?}");
    }

    /// Anti-vacuity for the above: a key whose row is still there is still refused. Without this, an
    /// insert that never checked uniqueness at all would satisfy the test above.
    #[test]
    fn a_live_primary_key_is_still_refused() {
        let (mut c, bp, _d, txn) = seed();
        assert!(
            exec("INSERT INTO users VALUES (1, 'dup');", &mut c, bp.clone(), txn).is_err(),
            "a duplicate of a LIVE primary key was accepted"
        );
    }

    #[test]
    fn not_null_violation_errors() {
        let (mut c, bp, _d, txn) = seed();
        assert!(exec("INSERT INTO users VALUES (NULL, 'x');", &mut c, bp.clone(), txn).is_err())
    }
 
    #[test]
    fn root_split_persists_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("reopen.db");
        let n = 1000;

        {
            let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
            let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
            let mut c = Catalog::create(bp.clone()).unwrap();
            let wal = Arc::new(WalManager::new(dir.path().join("reopen.wal")).unwrap());
            let txn = Arc::new(TxnManager::new(wal, bp.clone()));
            exec("CREATE TABLE nums (id INTEGER NOT NULL);", &mut c, bp.clone(), txn.clone()).unwrap();
            for i in 0..n {
                exec(&format!("INSERT INTO nums VALUES ({});", i), &mut c, bp.clone(), txn.clone()).unwrap();
            }
            bp.flush_all().unwrap();
        }

        {
            let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
            let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
            let c = Catalog::open(bp.clone(), 1).unwrap();
            let entry = c.get_table("nums").unwrap();
            let tree = BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp.clone());
            let all = tree.range_scan(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(all.len(), n as usize);
        }
    }

    #[test]
    fn test_arithmetic() {
        let int4 = Value::Integer(4);
        let int2 = Value::Integer(2);
        let float2 = Value::Float(2.0);
        let null = Value::Null;

        assert_eq!(arithmetic(&int4, &int2, &TokenType::Plus).unwrap(), Value::Integer(6));
        assert_eq!(arithmetic(&int4, &int2, &TokenType::Minus).unwrap(), Value::Integer(2));
        assert_eq!(arithmetic(&int4, &int2, &TokenType::Star).unwrap(), Value::Integer(8));
        assert_eq!(arithmetic(&int4, &int2, &TokenType::Slash).unwrap(), Value::Integer(2));    
        assert_eq!(arithmetic(&int4, &float2, &TokenType::Plus).unwrap(), Value::Float(6.0));
        assert_eq!(arithmetic(&float2, &int4, &TokenType::Star).unwrap(), Value::Float(8.0));
        assert_eq!(arithmetic(&int4, &null, &TokenType::Plus).unwrap(), Value::Null);
        assert!(arithmetic(&int4, &Value::Integer(0), &TokenType::Slash).is_err());
    }

    #[test]
    fn test_comparison() {
        let int5 = Value::Integer(5);
        let int10 = Value::Integer(10);

        assert_eq!(compare(&int5, &int10, &TokenType::Less).unwrap(), Value::Boolean(true));
        assert_eq!(compare(&int5, &int5, &TokenType::Equal).unwrap(), Value::Boolean(true));
        assert_eq!(compare(&int5, &int10, &TokenType::BangEqual).unwrap(), Value::Boolean(true));
        assert_eq!(compare(&int5, &Value::Null, &TokenType::Greater).unwrap(), Value::Null);
    }

    #[test]
    fn test_logical() {
        let t = Value::Boolean(true);
        let f = Value::Boolean(false);
        let n = Value::Null;

        assert_eq!(logical(&t, &t, &TokenType::And).unwrap(), Value::Boolean(true));
        assert_eq!(logical(&t, &f, &TokenType::And).unwrap(), Value::Boolean(false));
        assert_eq!(logical(&t, &n, &TokenType::And).unwrap(), Value::Null);
        assert_eq!(logical(&f, &n, &TokenType::And).unwrap(), Value::Boolean(false));
        assert_eq!(logical(&t, &f, &TokenType::Or).unwrap(), Value::Boolean(true));
        assert_eq!(logical(&f, &f, &TokenType::Or).unwrap(), Value::Boolean(false));
        assert_eq!(logical(&t, &n, &TokenType::Or).unwrap(), Value::Boolean(true));
        assert_eq!(logical(&f, &n, &TokenType::Or).unwrap(), Value::Null);
    }

    #[test]
    fn test_unary() {
        let e_minus = BoundExpr::UnaryOp {
            operator: TokenType::Minus,
            right: Box::new(BoundExpr::Literal (Value::Integer(5)))
        };
        assert_eq!(evaluate(&e_minus, &[]).unwrap(), Value::Integer(-5));
        let e_not = BoundExpr::UnaryOp {
            operator: TokenType::Not,
            right: Box::new(BoundExpr::Literal(Value::Boolean(true)))
        };
        assert_eq!(evaluate(&e_not, &[]).unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_block_commits_atomically() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, s: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.into_iter().next().unwrap(), catalog, bp.clone(), txn.clone(), s)
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL);", &mut catalog, &mut s).unwrap();
        exec("BEGIN;", &mut catalog, &mut s).unwrap();
        exec("INSERT INTO t VALUES (1);", &mut catalog, &mut s).unwrap();
        exec("INSERT INTO t VALUES (2);", &mut catalog, &mut s).unwrap();
        exec("COMMIT;", &mut catalog, &mut s).unwrap();
        match exec("SELECT id FROM t;", &mut catalog, &mut s).unwrap() {
            Outcome::Rows(r) => assert_eq!(r.len(), 2),
            _ => panic!()
        }
    }

    #[test]
    fn test_block_rollback_discards_everything() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, s: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.into_iter().next().unwrap(), catalog, bp.clone(), txn.clone(), s)
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL);", &mut catalog, &mut s).unwrap();
        exec("BEGIN;", &mut catalog, &mut s).unwrap();
        exec("INSERT INTO t VALUES (1);", &mut catalog, &mut s).unwrap();
        exec("ROLLBACK;", &mut catalog, &mut s).unwrap();
        match exec("SELECT id FROM t;", &mut catalog, &mut s).unwrap() {
            Outcome::Rows(r) => assert!(r.is_empty()),
            _ => panic!()
        }
        assert!(matches!(exec("COMMIT;", &mut catalog, &mut s), Err(FerroError::Txn(_))));
    }

    #[test]
    fn test_error_aborts_everything() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, s: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.into_iter().next().unwrap(), catalog, bp.clone(), txn.clone(), s)
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL);", &mut catalog, &mut s).unwrap();
        exec("BEGIN;", &mut catalog, &mut s).unwrap();
        exec("INSERT INTO t VALUES (1);", &mut catalog, &mut s).unwrap();
        assert!(exec("INSERT INTO idk VALUES (1);", &mut catalog,  &mut s).is_err());
        match exec("SELECT id FROM t;", &mut catalog, &mut s).unwrap() {
            Outcome::Rows(r) => assert!(r.is_empty()),
            _ => panic!()
        }
        assert!(matches!(exec("COMMIT;", &mut catalog, &mut s), Err(FerroError::Txn(_))));
    }

    #[test]
    fn test_rejects_ddl_inside_block() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, s: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.into_iter().next().unwrap(), catalog, bp.clone(), txn.clone(), s)
        };
        assert!(matches!(exec("COMMIT;", &mut catalog, &mut s), Err(FerroError::Txn(_))));
        assert!(matches!(exec("ROLLBACK;", &mut catalog, &mut s), Err(FerroError::Txn(_))));
        exec("BEGIN;", &mut catalog, &mut s).unwrap();
        assert!(matches!(exec("BEGIN;", &mut catalog, &mut s), Err(FerroError::Txn(_))));
        assert!(matches!(exec("CREATE TABLE idk (id INTEGER NOT NULL);", &mut catalog, &mut s), Err(FerroError::Txn(_))));
        exec("ROLLBACK;", &mut catalog, &mut s).unwrap();
    }

    #[test]
    fn test_insert_stamps_begin_ts() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, s: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.into_iter().next().unwrap(), catalog, bp.clone(), txn.clone(), s)
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL, str VARCHAR(10));", &mut catalog, &mut s).unwrap();
        let expected = txn.next_txn_id.load(Ordering::SeqCst);
        exec("INSERT INTO t VALUES (1, 'a');", &mut catalog, &mut s).unwrap();
        let entry = catalog.get_table("t").unwrap();
        let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
        let (_, tuple) = heap.scan().next().unwrap().unwrap();
        let h = tuple.version_header().unwrap();
        assert_eq!(h.begin_ts, expected);
        assert_eq!(h.end_ts, 0);
        assert_eq!(h.prev(), None);
    }

    /// **The third arm: a delete that has not committed does not free the key.**
    ///
    /// `deleted_for_me` is `end_ts != 0 && view.is_commited_for_me(end_ts)`, and the two tests above
    /// only ever reach the first conjunct - they delete and reuse in one autocommit view, where the
    /// deletion is always the reader's own. Dropping the visibility half entirely would leave both of
    /// them passing while any `end_ts` at all freed the key, including one stamped by a transaction
    /// that goes on to roll back.
    ///
    /// The safe direction is the strict one: a reader who cannot see the deletion must not be allowed
    /// to take the key, because the deleting transaction may still abort and get its row back.
    #[test]
    fn an_uncommitted_delete_does_not_free_the_primary_key() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s1 = Session::new();
        let mut s2 = Session::new();
        exec_s("CREATE TABLE t (id INTEGER NOT NULL);", &mut catalog, &bp, &txn, &mut s1).unwrap();
        exec_s("INSERT INTO t VALUES (1);", &mut catalog, &bp, &txn, &mut s1).unwrap();

        // s1 deletes and holds the transaction open, so nobody else can see the deletion yet.
        exec_s("BEGIN;", &mut catalog, &bp, &txn, &mut s1).unwrap();
        exec_s("DELETE FROM t WHERE id = 1;", &mut catalog, &bp, &txn, &mut s1).unwrap();

        // s2 must still be refused: for s2 the row is there.
        let r = exec_s("INSERT INTO t VALUES (1);", &mut catalog, &bp, &txn, &mut s2);
        assert!(
            r.is_err(),
            "an uncommitted delete freed the primary key for another session; if s1 rolls back, two \
             live rows share it"
        );

        // Anti-vacuity: once it commits, the key is free. Without this the test above is satisfied by
        // an insert that refuses every duplicate regardless of visibility - the old behaviour.
        exec_s("COMMIT;", &mut catalog, &bp, &txn, &mut s1).unwrap();
        exec_s("INSERT INTO t VALUES (1);", &mut catalog, &bp, &txn, &mut s2)
            .expect("the key was still held after the delete committed");
    }

    #[test]
    fn test_uncommitted_writes_invisible_across_sessions() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s1 = Session::new();
        let mut s2 = Session::new();
        exec_s("CREATE TABLE t (id INTEGER NOT NULL);", &mut catalog, &bp, &txn, &mut s1).unwrap();
        exec_s("BEGIN;", &mut catalog, &bp, &txn, &mut s1).unwrap();
        exec_s("INSERT INTO t VALUES (1);", &mut catalog, &bp, &txn, &mut s1).unwrap();

        let r = rows(exec_s("SELECT id FROM t;", &mut catalog, &bp, &txn, &mut s2).unwrap());
        assert!(r.is_empty());
        let r = rows(exec_s("SELECT id FROM t;", &mut catalog, &bp, &txn, &mut s1).unwrap());
        assert_eq!(r.len(), 1);
        exec_s("COMMIT;", &mut catalog, &bp, &txn, &mut s1).unwrap();
        let r = rows(exec_s("SELECT id FROM t;", &mut catalog, &bp, &txn, &mut s2).unwrap());
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn test_snapshot_pins_at_begin() {
        let (mut catalog, bp, _dir, txn) = setup();
        let (mut s1, mut s2) = (Session::new(), Session::new());

        exec_s("CREATE TABLE t (id INTEGER NOT NULL);", &mut catalog, &bp, &txn, &mut s1).unwrap();
        exec_s("BEGIN;", &mut catalog, &bp, &txn, &mut s2).unwrap();
        let r = rows(exec_s("SELECT id FROM t;", &mut catalog, &bp, &txn, &mut s2).unwrap());
        assert!(r.is_empty());
        
        exec_s("INSERT INTO t VALUES (1);", &mut catalog, &bp, &txn, &mut s1).unwrap();
        let r = rows(exec_s("SELECT id FROM t;", &mut catalog, &bp, &txn, &mut s2).unwrap());
        assert!(r.is_empty());
    }

    
}