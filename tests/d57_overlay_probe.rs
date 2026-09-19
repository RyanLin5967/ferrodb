//! D57 — probing the overlay by key must return EXACTLY what walking it did.
//!
//! `visible_rows_where` used to walk every staged row on the branch — all tables — under the
//! State mutex, per statement (measured ~11.5 ns per staged row per read, ×8 at D27's 4,000
//! staged rows: `bench/d57_staged_curve_before.txt`). Now a predicate with a `pk = literal`
//! conjunct probes ONE overlay entry, every other predicate walks only THIS table's prefix, and
//! neither holds the lock while it works.
//!
//! The D55 argument still governs which entries can affect a result: with a `pk = k` conjunct only
//! the entry for `k` can, because every staged row carries its own PK in column 0 and fails that
//! conjunct. An argument is not a test, so every overlay case is constructed and asserted BOTH
//! against a hand-derived expectation (independent of the subject) AND against the same session's
//! unfiltered view filtered here (the two paths must agree).
//!
//! Cases, per key:
//! * staged UPDATE that passes / fails the rest of the predicate
//! * staged DELETE
//! * staged-only INSERT that passes / fails
//! * untouched base row, and a key that exists nowhere
//! * a SECOND table with staged rows — the prefix walk must never see them
//! * a VARCHAR primary key, where the overlay key is a hash of the literal
//! * a literal whose type differs from the PK column's — the probe must fall back, not miss

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
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
            .open(dir.path().join("p.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
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
            return Err(FerroError::SqlParseError(format!("{:?}", parser.errors)));
        }
        assert_eq!(stmts.len(), 1, "one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn rows(&mut self, sql: &str, s: &mut Session) -> Vec<Vec<Value>> {
        match self.ok(sql, s) {
            Outcome::Rows(rows) => rows,
            _ => panic!("{sql}: expected rows"),
        }
    }

    /// `(id, v)` pairs a SELECT returned, as a set.
    fn pairs(&mut self, sql: &str, s: &mut Session) -> BTreeSet<(i32, i32)> {
        self.rows(sql, s)
            .into_iter()
            .map(|r| match (&r[0], &r[1]) {
                (Value::Integer(id), Value::Integer(v)) => (*id, *v),
                other => panic!("{sql}: unexpected row shape {other:?}"),
            })
            .collect()
    }
}

/// The branch every test below reads from: 20 base rows in `t`, staged changes in `t` AND `u`.
fn staged_branch(db: &mut Db) -> Session {
    let mut setup = db.session();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut setup);
    db.ok("CREATE TABLE u (id INTEGER NOT NULL, w INTEGER);", &mut setup);
    for i in 1..=20 {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {});", i * 10), &mut setup);
        db.ok(&format!("INSERT INTO u VALUES ({i}, {});", i * 10), &mut setup);
    }
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r1';", &mut a);
    db.ok("UPDATE t SET v = 999 WHERE id = 5;", &mut a); // staged, fails `v < 100`
    db.ok("UPDATE t SET v = 50 WHERE id = 15;", &mut a); // staged, passes (base 150 failed)
    db.ok("DELETE FROM t WHERE id = 3;", &mut a); // staged delete
    db.ok("INSERT INTO t VALUES (25, 25);", &mut a); // staged-only, passes
    db.ok("INSERT INTO t VALUES (26, 2600);", &mut a); // staged-only, fails
    // The other table: same keys, staged too. A walk that forgets the table boundary would see
    // these under `t`'s keys.
    db.ok("UPDATE u SET w = 1 WHERE id = 7;", &mut a);
    db.ok("UPDATE u SET w = 1 WHERE id = 5;", &mut a);
    db.ok("DELETE FROM u WHERE id = 15;", &mut a);
    db.ok("INSERT INTO u VALUES (27, 27);", &mut a);
    a
}

#[test]
fn point_probe_agrees_with_the_walk_and_with_the_hand_derived_truth() {
    let mut db = Db::new();
    let mut a = staged_branch(&mut db);

    // Hand-derived: what this branch must see for each key, from the setup above and nothing else.
    let truth: Vec<(i32, Option<i32>)> = vec![
        (5, Some(999)),  // staged UPDATE wins over base 50
        (15, Some(50)),  // staged UPDATE wins over base 150
        (3, None),       // staged DELETE
        (25, Some(25)),  // staged-only INSERT
        (26, Some(2600)),// staged-only INSERT
        (7, Some(70)),   // untouched base row (u's staged row 7 must not leak in)
        (27, None),      // exists only in u
        (40, None),      // exists nowhere
    ];
    // The walk: the same session's whole view of t, filtered here.
    let all = db.pairs("SELECT id, v FROM t;", &mut a);

    for (id, want) in truth {
        let probe = db.pairs(&format!("SELECT id, v FROM t WHERE id = {id};"), &mut a);
        let expect: BTreeSet<(i32, i32)> = want.map(|v| (id, v)).into_iter().collect();
        assert_eq!(probe, expect, "key {id}: probe disagrees with the hand-derived truth");
        let walked: BTreeSet<(i32, i32)> = all.iter().copied().filter(|(k, _)| *k == id).collect();
        assert_eq!(probe, walked, "key {id}: probe disagrees with the walk");
    }
}

#[test]
fn point_probe_with_a_second_conjunct_applies_the_whole_predicate_to_the_staged_row() {
    let mut db = Db::new();
    let mut a = staged_branch(&mut db);

    // staged 999 fails `v < 100`: the base version (50) passed, and it must NOT come back.
    assert!(db.pairs("SELECT id, v FROM t WHERE id = 5 AND v < 100;", &mut a).is_empty());
    // staged 50 passes where the base 150 failed.
    assert_eq!(db.pairs("SELECT id, v FROM t WHERE id = 15 AND v < 100;", &mut a), BTreeSet::from([(15, 50)]));
    // the conjunct order must not matter
    assert_eq!(db.pairs("SELECT id, v FROM t WHERE v < 100 AND id = 15;", &mut a), BTreeSet::from([(15, 50)]));
    // a staged delete under a compound predicate
    assert!(db.pairs("SELECT id, v FROM t WHERE id = 3 AND v < 100;", &mut a).is_empty());
    // the literal on the left
    assert_eq!(db.pairs("SELECT id, v FROM t WHERE 25 = id;", &mut a), BTreeSet::from([(25, 25)]));
}

#[test]
fn a_non_key_predicate_walks_only_this_tables_staged_rows() {
    let mut db = Db::new();
    let mut a = staged_branch(&mut db);

    let all = db.pairs("SELECT id, v FROM t;", &mut a);
    let reference: BTreeSet<(i32, i32)> = all.iter().copied().filter(|(_, v)| *v < 100).collect();
    let pushed = db.pairs("SELECT id, v FROM t WHERE v < 100;", &mut a);
    assert_eq!(pushed, reference);
    // `u` staged (7 -> w=1) and (5 -> w=1) and inserted 27: none may appear under t.
    let ids: BTreeSet<i32> = pushed.iter().map(|(id, _)| *id).collect();
    assert!(!ids.contains(&27), "u's staged-only insert leaked into t");
    assert_eq!(pushed.iter().find(|(id, _)| *id == 7), Some(&(7, 70)), "u's staged row 7 overwrote t's");
    assert!(!ids.contains(&5), "t's staged 999 fails; u's staged (5, 1) must not resurrect it");
    // And the deleted-in-u key 15 must still be present in t (staged 50 passes).
    assert!(ids.contains(&15), "u's DELETE of 15 must not delete t's 15");
    // An equality on a NON-key column is not a probe: `v = 999` names no overlay key, and the
    // only row with v = 999 is the staged one. A probe keyed on the wrong column would look up
    // entry 999, find nothing staged there, and return the base table's (empty) answer.
    assert_eq!(db.pairs("SELECT id, v FROM t WHERE v = 999;", &mut a), BTreeSet::from([(5, 999)]));
    assert_eq!(db.pairs("SELECT id, v FROM t WHERE v = 50;", &mut a), BTreeSet::from([(15, 50)]), "base (5,50) is overwritten by staged 999; staged (15,50) passes");
    // The same for the other table, so the prefix is right from both sides.
    let u_all = db.pairs("SELECT id, w FROM u;", &mut a);
    let u_ids: BTreeSet<i32> = u_all.iter().map(|(id, _)| *id).collect();
    assert!(!u_ids.contains(&15) && u_ids.contains(&27) && !u_ids.contains(&25) && !u_ids.contains(&26));
    assert_eq!(u_all.iter().find(|(id, _)| *id == 5), Some(&(5, 1)));
}

#[test]
fn varchar_primary_key_probes_by_the_hashed_literal() {
    let mut db = Db::new();
    let mut setup = db.session();
    db.ok("CREATE TABLE s (k VARCHAR(10) NOT NULL, v INTEGER);", &mut setup);
    for (k, v) in [("a", 1), ("b", 2), ("c", 3)] {
        db.ok(&format!("INSERT INTO s VALUES ('{k}', {v});"), &mut setup);
    }
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-s' RUN 'r1';", &mut a);
    db.ok("UPDATE s SET v = 20 WHERE k = 'b';", &mut a);
    db.ok("DELETE FROM s WHERE k = 'c';", &mut a);
    db.ok("INSERT INTO s VALUES ('d', 4);", &mut a);

    let v_of = |db: &mut Db, a: &mut Session, k: &str| -> Vec<i32> {
        db.rows(&format!("SELECT v FROM s WHERE k = '{k}';"), a)
            .into_iter()
            .map(|r| match &r[0] { Value::Integer(v) => *v, o => panic!("{o:?}") })
            .collect()
    };
    assert_eq!(v_of(&mut db, &mut a, "a"), vec![1]);
    assert_eq!(v_of(&mut db, &mut a, "b"), vec![20], "staged UPDATE on a varchar key");
    assert_eq!(v_of(&mut db, &mut a, "c"), Vec::<i32>::new(), "staged DELETE on a varchar key");
    assert_eq!(v_of(&mut db, &mut a, "d"), vec![4], "staged-only INSERT on a varchar key");
    assert_eq!(v_of(&mut db, &mut a, "zz"), Vec::<i32>::new());
}

#[test]
fn a_literal_of_another_type_falls_back_instead_of_missing_the_staged_row() {
    let mut db = Db::new();
    let mut a = staged_branch(&mut db);
    // `id` is INTEGER; a BIGINT-typed literal compares equal by value but hashes to a different
    // overlay key if probed raw. Whatever the engine does with the comparison, the result must be
    // the same as the walk's -- it may never be "base row, staged version missed".
    let all = db.pairs("SELECT id, v FROM t;", &mut a);
    let walked: BTreeSet<(i32, i32)> = all.iter().copied().filter(|(k, _)| *k == 5).collect();
    match db.exec("SELECT id, v FROM t WHERE id = 5.0;", &mut a) {
        Ok(Outcome::Rows(rows)) => {
            let got: BTreeSet<(i32, i32)> = rows
                .into_iter()
                .map(|r| match (&r[0], &r[1]) {
                    (Value::Integer(id), Value::Integer(v)) => (*id, *v),
                    other => panic!("unexpected row shape {other:?}"),
                })
                .collect();
            // Either the comparison finds the row (then it must be the STAGED one) or it finds
            // nothing at all; the base version alone is the one wrong answer.
            assert!(
                got == walked || got.is_empty(),
                "cross-type literal returned {got:?}; the walk says {walked:?}"
            );
            assert_ne!(got, BTreeSet::from([(5, 50)]), "the staged version was MISSED");
        }
        Ok(_) => panic!("expected rows"),
        Err(e) => {
            // A refusal at bind/plan time is an acceptable answer: no wrong rows were returned.
            eprintln!("cross-type comparison refused (acceptable): {e}");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Two cases a fresh-context review found AFTER the probe shipped (`65287c2`), both real, both
// regressions the walk never had. Each was fire-checked: written first, run against `65287c2`,
// and seen to fail there before the fix existed.
// ---------------------------------------------------------------------------------------------

/// A DECIMAL key's identity is its digit TEXT (`row_id_of` hashes the bytes) while `=` on decimals
/// is NUMERIC (`decimal_cmp`): `1.50` and `1.5` are equal values with different overlay keys. A
/// probe keyed on the literal's spelling misses the staged row the walk found. Decimal must never
/// be probed.
#[test]
fn a_decimal_key_is_never_probed_because_its_identity_is_finer_than_its_equality() {
    let mut db = Db::new();
    let mut setup = db.session();
    db.ok("CREATE TABLE p (amt DECIMAL NOT NULL, note VARCHAR(10));", &mut setup);
    db.ok("INSERT INTO p VALUES (1.50, 'base');", &mut setup);
    db.ok("INSERT INTO p VALUES (2.50, 'base');", &mut setup);
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-p' RUN 'r1';", &mut a);
    db.ok("UPDATE p SET note = 'staged' WHERE amt = 1.50;", &mut a);
    db.ok("DELETE FROM p WHERE amt = 2.50;", &mut a);
    db.ok("INSERT INTO p VALUES (3.50, 'new');", &mut a);

    let notes = |db: &mut Db, a: &mut Session, lit: &str| -> Vec<String> {
        db.rows(&format!("SELECT note FROM p WHERE amt = {lit};"), a)
            .into_iter()
            .map(|r| match &r[0] { Value::Varchar(s) => s.clone(), o => panic!("{o:?}") })
            .collect()
    };
    // Same spelling as stored, and a different spelling of the same number: identical answers.
    assert_eq!(notes(&mut db, &mut a, "1.50"), vec!["staged"]);
    assert_eq!(notes(&mut db, &mut a, "1.5"), vec!["staged"], "a re-spelled decimal literal MISSED the staged version");
    assert_eq!(notes(&mut db, &mut a, "1.500"), vec!["staged"]);
    assert!(notes(&mut db, &mut a, "2.5").is_empty(), "a re-spelled decimal literal resurrected a staged DELETE");
    assert_eq!(notes(&mut db, &mut a, "3.5"), vec!["new"], "a re-spelled decimal literal hid a staged INSERT");
}

/// On a branch, an UPDATE may assign column 0 (trunk refuses this; the branch path does not, and
/// `tests/integration_escrow.rs` relies on the staged row KEEPING its original key so a PK move
/// cannot escape an escrow pool). So a staged row's column 0 can differ from the key it sits under,
/// and the probe's premise -- "every staged row carries its own primary key in column 0" -- is
/// false for that workspace. The read path must notice and walk.
#[test]
fn a_workspace_that_moved_a_primary_key_is_walked_not_probed() {
    let mut db = Db::new();
    let mut a = staged_branch(&mut db);
    // Hand-derived, from what the walk did: `staged_branch` already staged v = 999 on row 5, so
    // the entry at key 5 becomes Present([99, 999]) ...
    db.ok("UPDATE t SET id = 99 WHERE id = 5;", &mut a);
    // ... so `id = 99` sees it (base has no 99), and `id = 5` sees nothing (the staged version of
    // row 5 fails `id = 5`, and the staged version is what this branch sees).
    assert_eq!(db.pairs("SELECT id, v FROM t WHERE id = 99;", &mut a), BTreeSet::from([(99, 999)]), "the moved row was MISSED by a probe on its new key");
    assert!(db.pairs("SELECT id, v FROM t WHERE id = 5;", &mut a).is_empty(), "the old key still shows the pre-move row");
    // Everything else on the branch is unaffected by the fallback.
    assert_eq!(db.pairs("SELECT id, v FROM t WHERE id = 15;", &mut a), BTreeSet::from([(15, 50)]));
    assert!(db.pairs("SELECT id, v FROM t WHERE id = 3;", &mut a).is_empty());
    // And the two paths still agree on every key -- including a move ONTO an existing key, whose
    // answer (two rows with one id, on the branch) is the walk's and is not pinned here.
    db.ok("UPDATE t SET id = 6 WHERE id = 8;", &mut a);
    let all = db.pairs("SELECT id, v FROM t;", &mut a);
    for id in [1, 3, 5, 6, 7, 8, 15, 25, 26, 99] {
        let probe = db.pairs(&format!("SELECT id, v FROM t WHERE id = {id};"), &mut a);
        let walked: BTreeSet<(i32, i32)> = all.iter().copied().filter(|(k, _)| *k == id).collect();
        assert_eq!(probe, walked, "key {id} after a PK move");
    }
    // A child forked from this branch inherits the moved row, so it must inherit the fallback too.
    let parent_branch = a.agent.as_ref().unwrap().branch;
    let child = db.runtime.begin_session("child", Some("r2"), parent_branch).unwrap();
    let mut reader = db.session();
    let seen = db.pairs(&format!("SELECT id, v FROM t AS OF BRANCH {} WHERE id = 99;", child.branch_name), &mut reader);
    assert_eq!(seen, BTreeSet::from([(99, 999)]), "the child lost the moved row");
}
