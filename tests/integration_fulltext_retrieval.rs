//! **Removed 2026-08-28: `the_readme_s_documented_search_transcript_is_what_the_engine_prints`.**
//! It executed a block the README used to contain. The README was rewritten from 905 lines
//! to ~135 and no longer documents that command, so the test had no fixture left — and a
//! test whose fixture is gone does not fail, it silently covers nothing, which is the exact
//! failure the E50/E51 rule exists to prevent. The behaviour itself is still tested by the
//! rest of this file; only the README-transcript check went.
//!
//! B8 — keyword retrieval on the existing secondary-index machinery.
//!
//! A posting list is not a new data structure here. A secondary index is
//! `BPlusTreeManager<(Value, Value), ()>` keyed `(column_value, primary_key)`; put a *token* in the
//! first component and the same tree is an inverted index, with `range_scan` over a token's prefix
//! as the read of its posting list. So this file tests a feature built from the parts that were
//! already load-bearing, and the claims it pins are the ones that break when they are reused
//! slightly differently:
//!
//! - **one posting per `(token, primary key)`**, however often the word occurs and however many
//!   times write history visits the value. `BPlusTreeLeafPage::insert_entry` inserts at the
//!   binary-search position rather than overwriting, so a duplicate is *stored*, and the row then
//!   comes back once per copy. This is E66's rule, and a full-text index reaches it three ways a
//!   secondary index cannot: a word repeated inside one value, a value updated away and back, and a
//!   build-from-heap over a primary key that was deleted and re-inserted.
//! - **a dead posting is left in place and filtered by visibility**, exactly as a dead secondary
//!   entry is, and the read path additionally re-checks that the token is still in the version it
//!   resolved.
//! - **ranking is a ranking.** Every assertion about order here is written so that a scorer which
//!   ranks all rows equally would fail it: the expected best row is never the one a tie would pick.
//! - **the index survives a crash** through `wal::recovery::rebuild_indexes`, which is why no WAL
//!   record was added for it.
//!
//! Instrument note: `postings` below reports what is **stored**, by scanning the whole tree, not
//! what a search chose to return. Row counts alone cannot tell a de-duplicated index from a search
//! that happened to filter a duplicate out, and the duplicate is the defect.

use std::collections::HashSet;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::BPlusTreePage;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::{rebuild_indexes, recover};
use ferrodb::wal::txn::TxnManager;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn stack(dir: &Path) -> (Arc<BufferPoolManager>, Arc<WalManager>, Arc<TxnManager>) {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.join("ft.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.join("ft.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    (bp, wal, txn)
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let (bp, _wal, txn) = stack(dir.path());
    let catalog = Catalog::create(bp.clone()).unwrap();
    Db { _dir: dir, catalog, bp, txn, session: Session::new() }
}

impl Db {
    fn sql(&mut self, sql: &str) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    /// The error a statement produces, for the refusals.
    fn err(&mut self, sql: &str) -> String {
        let tokens = match Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens() {
            Ok(t) => t,
            Err(e) => return e.to_string(),
        };
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        if !p.errors.is_empty() {
            return p.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
        }
        match run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session) {
            Ok(_) => panic!("`{sql}` was accepted; it must be refused"),
            Err(e) => e.to_string(),
        }
    }

    fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        match self.sql(sql) {
            Outcome::Rows(r) => r,
            other => panic!("expected rows from `{sql}`, got {:?}", std::mem::discriminant(&other)),
        }
    }

    /// The primary keys a statement returns, in the order it returned them.
    fn ids(&mut self, sql: &str) -> Vec<i32> {
        self.rows(sql)
            .into_iter()
            .map(|r| match r[0] {
                Value::Integer(i) => i,
                ref other => panic!("expected an Integer primary key, got {other:?}"),
            })
            .collect()
    }

    /// The BM25 score of each returned row: the last column.
    fn scores(&mut self, sql: &str) -> Vec<f64> {
        self.rows(sql)
            .into_iter()
            .map(|r| match r[r.len() - 1] {
                Value::Float(f) => f,
                ref other => panic!("expected a Float score in the last column, got {other:?}"),
            })
            .collect()
    }

    /// Every posting **stored** in a full-text index, tree order. See the instrument note above.
    fn postings(&self, table: &str, column: &str) -> Vec<(String, i32)> {
        let entry = self.catalog.get_table(table).expect("table");
        let root = entry
            .fulltext_indexes
            .iter()
            .find(|i| i.column_name == column)
            .unwrap_or_else(|| panic!("no full-text index on {table}.{column}"))
            .root_page_id;
        let tree = BPlusTreeManager::<(Value, Value), ()>::open(root, self.bp.clone());
        tree.range_scan(Bound::Unbounded, Bound::Unbounded)
            .expect("scan the posting tree")
            .map(|r| {
                let ((token, pk), ()) = r.expect("posting");
                let token = match token {
                    Value::Varchar(s) => s,
                    other => panic!("a posting's first component must be a token, got {other:?}"),
                };
                let pk = match pk {
                    Value::Integer(i) => i,
                    other => panic!("unexpected primary key {other:?}"),
                };
                (token, pk)
            })
            .collect()
    }

    fn postings_for(&self, table: &str, column: &str, token: &str) -> Vec<i32> {
        self.postings(table, column)
            .into_iter()
            .filter(|(t, _)| t == token)
            .map(|(_, pk)| pk)
            .collect()
    }
}

/// Three short documents with overlapping words, indexed after they are written.
fn seeded() -> Db {
    let mut d = db();
    d.sql("CREATE TABLE docs (id INTEGER NOT NULL, body VARCHAR(200));");
    d.sql("INSERT INTO docs VALUES (1, 'wireless charger for a phone');");
    d.sql("INSERT INTO docs VALUES (2, 'wired charger');");
    d.sql("INSERT INTO docs VALUES (3, 'phone case');");
    d.sql("CREATE FULLTEXT INDEX fx ON docs (body);");
    d
}

#[test]
fn a_token_lookup_returns_exactly_the_rows_containing_it() {
    let mut d = seeded();

    let mut charger = d.ids("SEARCH docs (body) FOR 'charger';");
    charger.sort();
    assert_eq!(charger, vec![1, 2], "'charger' is in rows 1 and 2 and nothing else");

    let mut phone = d.ids("SEARCH docs (body) FOR 'phone';");
    phone.sort();
    assert_eq!(phone, vec![1, 3]);

    assert_eq!(d.ids("SEARCH docs (body) FOR 'wired';"), vec![2]);

    // Anti-vacuity: a token in no row returns nothing, so the lookups above are selecting rather
    // than returning the table. A search that always returned everything would satisfy nothing here.
    assert!(
        d.ids("SEARCH docs (body) FOR 'nonesuch';").is_empty(),
        "a token that is in no row must return no rows"
    );

    // The tokenizer's contract, through SQL: case-insensitive, and punctuation is a separator.
    assert_eq!(d.ids("SEARCH docs (body) FOR 'CHARGER';").len(), 2);
    assert_eq!(d.ids("SEARCH docs (body) FOR 'charger!';").len(), 2);
}

#[test]
fn each_row_carries_its_score_as_a_trailing_column() {
    let mut d = seeded();
    let rows = d.rows("SEARCH docs (body) FOR 'charger';");
    assert_eq!(rows[0].len(), 3, "two table columns plus the score");
    for s in d.scores("SEARCH docs (body) FOR 'charger';") {
        assert!(s > 0.0 && s.is_finite(), "a matched row must have a real positive score, got {s}");
    }
}

/// **The de-duplication case, and the one that bites.**
///
/// Breaking shape: `UPDATE body = <anything>` then `UPDATE body = <the original text>`. The first
/// update leaves the original tokens posted — deliberately, because a snapshot older than it must
/// still find the row by them — so the second update posts every one of them again over entries that
/// are still there. `insert_entry` appends rather than overwrites, so without the probe the tree
/// holds two `(wireless, 1)` entries and the search returns row 1 twice.
///
/// Measured at both levels: the stored postings, and the rows a search returns.
#[test]
fn a_value_updated_away_and_back_returns_the_row_exactly_once() {
    let mut d = seeded();
    assert_eq!(d.postings_for("docs", "body", "wireless"), vec![1], "one posting to begin with");

    d.sql("UPDATE docs SET body = 'nothing to see' WHERE id = 1;");
    assert!(
        d.ids("SEARCH docs (body) FOR 'wireless';").is_empty(),
        "the row no longer contains 'wireless', so the stale posting must not return it"
    );

    d.sql("UPDATE docs SET body = 'wireless charger for a phone' WHERE id = 1;");

    assert_eq!(
        d.postings_for("docs", "body", "wireless"),
        vec![1],
        "the value came back to a posting that was still there: exactly one entry, not two"
    );
    assert_eq!(
        d.ids("SEARCH docs (body) FOR 'wireless';"),
        vec![1],
        "the row must come back once, not once per posting"
    );

    // The same for a token the row shares with another row, so the check is not special to a
    // unique word.
    let mut charger = d.ids("SEARCH docs (body) FOR 'charger';");
    charger.sort();
    assert_eq!(charger, vec![1, 2]);
    assert_eq!(d.postings_for("docs", "body", "charger"), vec![1, 2]);

    // The other half of UPDATE maintenance, and the half the away-and-back case cannot see: a word
    // the row has NEVER held must become findable, which only happens if the update posts it. Every
    // token above was already posted by the INSERT, so without this assertion an UPDATE path that
    // maintained nothing at all would satisfy this test.
    d.sql("UPDATE docs SET body = 'wireless charger for a fridge' WHERE id = 1;");
    assert_eq!(
        d.ids("SEARCH docs (body) FOR 'fridge';"),
        vec![1],
        "a word introduced by an UPDATE has to be posted, not just left to the next rebuild"
    );
    assert_eq!(d.postings_for("docs", "body", "fridge"), vec![1]);
}

/// Breaking shape: one value containing the same word twice. A secondary index cannot reach this —
/// it posts one entry per row — but a full-text index posts one per token, so `'cat cat'` would post
/// `(cat, 1)` twice from a single `INSERT`, before any update or delete exists.
#[test]
fn a_word_repeated_in_one_value_posts_one_entry_not_two() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    d.sql("CREATE FULLTEXT INDEX fx ON t (body);");
    d.sql("INSERT INTO t VALUES (1, 'cat cat CAT cat');");

    assert_eq!(d.postings_for("t", "body", "cat"), vec![1], "four occurrences, one posting");
    assert_eq!(d.ids("SEARCH t (body) FOR 'cat';"), vec![1], "and the row is returned once");

    // Anti-vacuity: the repetition is still visible to the scorer, which counts occurrences in the
    // row it reads rather than in the index.
    d.sql("INSERT INTO t VALUES (2, 'cat mouse');");
    assert_eq!(
        d.ids("SEARCH t (body) FOR 'cat';"),
        vec![1, 2],
        "the row saying 'cat' four times must outrank the row saying it once"
    );
}

/// The same rule on the path `INSERT` opens: a primary key that was deleted and re-inserted.
///
/// Breaking shape: `DELETE FROM t WHERE id = 1;` then `INSERT INTO t VALUES (1, <text sharing a
/// word>);`. `DELETE` leaves the postings in place, so the insert lands on entries that already
/// exist.
#[test]
fn re_inserting_a_deleted_primary_key_does_not_double_the_row() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    d.sql("CREATE FULLTEXT INDEX fx ON t (body);");
    d.sql("INSERT INTO t VALUES (1, 'alpha beta');");
    d.sql("DELETE FROM t WHERE id = 1;");
    d.sql("INSERT INTO t VALUES (1, 'alpha gamma');");

    assert_eq!(d.postings_for("t", "body", "alpha"), vec![1], "one posting for the shared token");
    assert_eq!(d.ids("SEARCH t (body) FOR 'alpha';"), vec![1]);
    // 'beta' belongs only to the deleted version: its posting survives and must return nothing.
    assert_eq!(d.postings_for("t", "body", "beta"), vec![1], "the dead posting is left in place");
    assert!(
        d.ids("SEARCH t (body) FOR 'beta';").is_empty(),
        "a token only the deleted version had must not return the live row"
    );
}

/// The same shape again, this time through the **build from heap** rather than a write path.
///
/// Breaking shape: `DELETE` then re-`INSERT` of one primary key, *then* `CREATE FULLTEXT INDEX`.
/// `DELETE` stamps `end_ts` and leaves the slot, so the main heap holds two rows with that key and
/// the backfill sees both. `Catalog::create_index`'s backfill does not de-duplicate and gets away
/// with it because a secondary key is `(value, pk)`; a token key is reached twice by any word the two
/// versions share.
#[test]
fn a_backfill_over_a_re_used_primary_key_does_not_double_the_row() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    d.sql("INSERT INTO t VALUES (1, 'alpha beta');");
    d.sql("DELETE FROM t WHERE id = 1;");
    d.sql("INSERT INTO t VALUES (1, 'alpha gamma');");
    d.sql("INSERT INTO t VALUES (2, 'alpha delta');");
    // Premise: both versions really are in the heap the backfill will scan.
    assert_eq!(d.ids("SELECT * FROM t;").len(), 2, "two live rows");

    d.sql("CREATE FULLTEXT INDEX fx ON t (body);");

    assert_eq!(
        d.postings_for("t", "body", "alpha"),
        vec![1, 2],
        "one posting per primary key, though the heap holds two versions of key 1"
    );
    let mut alpha = d.ids("SEARCH t (body) FOR 'alpha';");
    alpha.sort();
    assert_eq!(alpha, vec![1, 2], "each row once");
}

/// A deleted row is not returned, and a live one is.
///
/// Breaking shape: any `DELETE`. Nothing removes the postings of a deleted row, so the read path is
/// the only thing standing between a tombstoned version and the result set. The second half is the
/// anti-vacuity: a filter that dropped everything would satisfy the first half alone.
#[test]
fn a_deleted_row_drops_out_of_a_search_and_a_live_one_does_not() {
    let mut d = seeded();
    d.sql("DELETE FROM docs WHERE id = 2;");

    assert_eq!(
        d.postings_for("docs", "body", "charger"),
        vec![1, 2],
        "the deleted row's posting is still stored — this is what visibility has to filter"
    );
    assert_eq!(
        d.ids("SEARCH docs (body) FOR 'charger';"),
        vec![1],
        "row 2 is deleted, so only row 1 comes back"
    );
    assert!(
        d.ids("SEARCH docs (body) FOR 'wired';").is_empty(),
        "'wired' was only in the deleted row"
    );
}

/// **Ranking is not degenerate**, pinned one BM25 component at a time.
///
/// Every case is built so the expected winner has the **higher** primary key, and ties break towards
/// the lower one. A scorer that returns the same number for every row therefore returns the wrong
/// order — and so does one that drops any single component of the formula, which is why the three
/// cases vary tf, document length and idf **separately** rather than all at once.
#[test]
fn ranking_separates_rows_that_an_equal_scorer_would_tie() {
    // (a) term frequency. Same length, same term, different counts.
    let mut d = db();
    d.sql("CREATE TABLE tf (id INTEGER NOT NULL, body VARCHAR(200));");
    d.sql("CREATE FULLTEXT INDEX fx ON tf (body);");
    d.sql("INSERT INTO tf VALUES (1, 'widget filler filler');");
    d.sql("INSERT INTO tf VALUES (2, 'widget widget widget');");
    assert_eq!(
        d.ids("SEARCH tf (body) FOR 'widget';"),
        vec![2, 1],
        "three occurrences must outrank one, though row 2 loses a tie"
    );
    assert_eq!(d.ids("SEARCH tf (body) FOR 'widget' TOP 1;"), vec![2], "and the bound keeps the best");

    // (b) document length. Same term, same count, different lengths.
    let mut d = db();
    d.sql("CREATE TABLE dl (id INTEGER NOT NULL, body VARCHAR(300));");
    d.sql("CREATE FULLTEXT INDEX fx ON dl (body);");
    d.sql("INSERT INTO dl VALUES (1, 'gadget and a great many other words padding this row out');");
    d.sql("INSERT INTO dl VALUES (2, 'gadget');");
    assert_eq!(
        d.ids("SEARCH dl (body) FOR 'gadget';"),
        vec![2, 1],
        "the shorter document must outrank the longer one at equal term frequency"
    );

    // (c) inverse document frequency. Same length, same count, terms of different rarity.
    let mut d = db();
    d.sql("CREATE TABLE idf (id INTEGER NOT NULL, body VARCHAR(200));");
    d.sql("CREATE FULLTEXT INDEX fx ON idf (body);");
    d.sql("INSERT INTO idf VALUES (1, 'common alpha');");
    d.sql("INSERT INTO idf VALUES (2, 'rare alpha');");
    d.sql("INSERT INTO idf VALUES (3, 'common alpha');");
    d.sql("INSERT INTO idf VALUES (4, 'common alpha');");
    let ranked = d.ids("SEARCH idf (body) FOR 'rare common';");
    assert_eq!(
        ranked[0], 2,
        "the row matching the rare term must come first; got {ranked:?}"
    );
    assert_eq!(ranked.len(), 4, "all four rows match one of the two terms");
    let scores = d.scores("SEARCH idf (body) FOR 'rare common';");
    assert!(
        scores[0] > scores[1],
        "the scores themselves must differ, not just the order: {scores:?}"
    );
    assert!(
        scores[1..].iter().all(|s| (s - scores[1]).abs() < 1e-12),
        "the three rows matching only the common term should tie: {scores:?}"
    );

    // (d) a document matching two query terms beats one matching a single term.
    let mut d = db();
    d.sql("CREATE TABLE both (id INTEGER NOT NULL, body VARCHAR(200));");
    d.sql("CREATE FULLTEXT INDEX fx ON both (body);");
    d.sql("INSERT INTO both VALUES (1, 'beta padding padding padding padding');");
    d.sql("INSERT INTO both VALUES (2, 'alpha beta');");
    assert_eq!(
        d.ids("SEARCH both (body) FOR 'alpha beta';"),
        vec![2, 1],
        "matching both terms must outrank matching one"
    );
}

/// The operator's own bound: `DEFAULT_TOP_K` when the statement is silent, `TOP k` when it is not,
/// and never more rows than either.
///
/// Breaking shape: a table with more matches than the default. Without a bound the search returns
/// all of them; with a bound that is not applied to the *best* rows it returns an arbitrary k.
#[test]
fn the_operator_bounds_its_own_result_and_keeps_the_best_rows() {
    let mut d = db();
    d.sql("CREATE TABLE many (id INTEGER NOT NULL, body VARCHAR(200));");
    d.sql("CREATE FULLTEXT INDEX fx ON many (body);");
    // 25 rows all containing 'token'; the LAST one is the shortest, so it scores highest and a tie
    // would not put it first.
    for i in 1..=24 {
        d.sql(&format!("INSERT INTO many VALUES ({i}, 'token filler filler filler filler');"));
    }
    d.sql("INSERT INTO many VALUES (25, 'token');");

    let defaulted = d.ids("SEARCH many (body) FOR 'token';");
    assert_eq!(defaulted.len(), 10, "the default bound is 10 rows, whatever the match count");
    assert_eq!(defaulted[0], 25, "and it is the best 10, not the first 10 the scan found");

    assert_eq!(d.ids("SEARCH many (body) FOR 'token' TOP 3;").len(), 3);
    assert_eq!(d.ids("SEARCH many (body) FOR 'token' TOP 3;")[0], 25);
    assert_eq!(d.ids("SEARCH many (body) FOR 'token' TOP 25;").len(), 25, "the bound can be raised");
    assert_eq!(
        d.ids("SEARCH many (body) FOR 'token' TOP 100;").len(),
        25,
        "and it does not pad: 100 asked, 25 exist"
    );
    // Anti-vacuity for the bound: a query matching fewer rows than the default returns them all.
    assert_eq!(d.ids("SEARCH many (body) FOR 'token' TOP 1;").len(), 1);
}

/// A posting tree that has split records its new root, so a reader that opens the catalog afresh
/// finds every posting.
///
/// Breaking shape: enough postings to split the root leaf — about 300 fit in one 4KB leaf. Without
/// `sync_fulltext_roots` the catalog keeps pointing at the pre-split root, which after the split is
/// an interior node holding a *subset* of the tree, and a search silently returns short.
#[test]
fn a_split_posting_tree_records_its_new_root() {
    let mut d = db();
    d.sql("CREATE TABLE big (id INTEGER NOT NULL, body VARCHAR(100));");
    d.sql("CREATE FULLTEXT INDEX fx ON big (body);");
    const N: i32 = 200;
    for i in 1..=N {
        // two distinct tokens per row: a shared one and a unique one, so the tree holds ~400 entries
        d.sql(&format!("INSERT INTO big VALUES ({i}, 'shared uniq{i}');"));
    }
    let root_after_writes = d
        .catalog
        .get_table("big")
        .unwrap()
        .fulltext_indexes
        .iter()
        .find(|i| i.column_name == "body")
        .unwrap()
        .root_page_id;

    // Premise, measured rather than assumed: the tree really did split, so the root moved. A root
    // that is still a leaf means this test would pass with no root bookkeeping at all.
    let stored = d.postings("big", "body");
    assert_eq!(stored.len(), 2 * N as usize, "one shared posting per row, plus one unique per row");
    let tree = BPlusTreeManager::<(Value, Value), ()>::open(root_after_writes, d.bp.clone());
    assert!(
        matches!(tree.read_node(root_after_writes).unwrap(), BPlusTreePage::Internal(_)),
        "premise failed: {} postings did not split the root leaf, so nothing here needs a new root \
         to be recorded — re-size the workload rather than relaxing the assertion",
        stored.len()
    );

    // Re-open the catalog from its pages, which is what a new connection does.
    let reopened = Catalog::open(d.bp.clone(), 1).expect("reopen the catalog");
    let recorded = reopened
        .get_table("big")
        .unwrap()
        .fulltext_indexes
        .iter()
        .find(|i| i.column_name == "body")
        .unwrap()
        .root_page_id;
    assert_eq!(recorded, root_after_writes, "the catalog must have the tree's current root");

    let found = d.ids("SEARCH big (body) FOR 'shared' TOP 500;");
    assert_eq!(found.len(), N as usize, "every row must still be reachable through the split tree");
    let unique: HashSet<i32> = found.into_iter().collect();
    assert_eq!(unique.len(), N as usize, "and each exactly once");
}

/// **The index survives a crash and recover**, through the existing rebuild path.
///
/// The crash is the in-process one the recovery unit tests use: drop every handle at a scope
/// boundary without a checkpoint, so the buffer pool goes with it, then rebuild the stack over the
/// same files. Index pages are not WAL-logged — there is no index record in `RecKind` — so the
/// index is rebuilt from the heap rather than recovered, and this test asserts the premise as well
/// as the result: **before** the rebuild the index is short, which is what makes the rebuild the
/// thing being tested.
#[test]
fn the_index_survives_a_crash_and_recover() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (bp, _wal, txn) = stack(dir.path());
        let mut catalog = Catalog::create(bp.clone()).unwrap();
        let mut session = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, session: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
            run(stmts.remove(0), catalog, bp.clone(), txn.clone(), session).unwrap()
        };
        exec("CREATE TABLE docs (id INTEGER NOT NULL, body VARCHAR(200));", &mut catalog, &mut session);
        // The index exists before the rows do, so every posting below is written by the INSERT path
        // and lands in pages that the crash then loses.
        exec("CREATE FULLTEXT INDEX fx ON docs (body);", &mut catalog, &mut session);
        for i in 1..=5 {
            exec(
                &format!("INSERT INTO docs VALUES ({i}, 'wireless charger number {i}');"),
                &mut catalog,
                &mut session,
            );
        }
        // no checkpoint, no flush: scope exit is the crash
    }

    let (bp, _wal, txn) = stack(dir.path());
    assert!(recover(&txn).unwrap(), "the log held the inserts, so recovery must have work to do");
    let mut catalog = Catalog::open(bp.clone(), 1).unwrap();

    // Premise, asserted rather than assumed: the posting tree on disk is missing rows the heap has.
    // If this ever fails because the pages happened to be flushed, the test is no longer measuring
    // the rebuild and should be re-sized, not relaxed.
    let stale_root = catalog
        .get_table("docs")
        .unwrap()
        .fulltext_indexes
        .iter()
        .find(|i| i.column_name == "body")
        .unwrap()
        .root_page_id;
    let stale = BPlusTreeManager::<(Value, Value), ()>::open(stale_root, bp.clone())
        .range_scan(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .count();
    assert!(
        stale < 5,
        "premise failed: the crashed index already holds {stale} postings, so this test would pass \
         without the rebuild"
    );

    rebuild_indexes(&mut catalog, &bp).unwrap();

    let mut d = Db { _dir: dir, catalog, bp, txn, session: Session::new() };
    assert_eq!(d.ids("SELECT * FROM docs;").len(), 5, "the heap survived, which is the input");
    let mut found = d.ids("SEARCH docs (body) FOR 'wireless' TOP 20;");
    found.sort();
    assert_eq!(found, vec![1, 2, 3, 4, 5], "every row is findable again after the rebuild");
    assert_eq!(
        d.postings_for("docs", "body", "wireless"),
        vec![1, 2, 3, 4, 5],
        "one posting per row, not one per rebuild"
    );
    assert_eq!(d.ids("SEARCH docs (body) FOR 'number';").len(), 5);
    assert!(d.ids("SEARCH docs (body) FOR 'nonesuch';").is_empty());
}

/// A rebuild of an already-correct index leaves it correct and de-duplicated — the case where the
/// rebuild runs over postings that are all present.
#[test]
fn a_rebuild_of_a_correct_index_does_not_double_its_postings() {
    let mut d = seeded();
    assert_eq!(d.postings_for("docs", "body", "charger"), vec![1, 2]);

    rebuild_indexes(&mut d.catalog, &d.bp).unwrap();

    assert_eq!(
        d.postings_for("docs", "body", "charger"),
        vec![1, 2],
        "rebuilding must not add a second copy of a posting that already existed"
    );
    let mut charger = d.ids("SEARCH docs (body) FOR 'charger';");
    charger.sort();
    assert_eq!(charger, vec![1, 2]);
}

/// The rebuild de-duplicates too, over the shape that puts two slots with one primary key in the
/// heap it scans.
///
/// Breaking shape: `DELETE` then re-`INSERT` of one key, then a recovery rebuild. `DELETE` stamps
/// `end_ts` and leaves the slot, so `rows` inside `rebuild_indexes` holds both versions and every
/// token they share is reached twice. `insert_entry` appends, so a crash would replace a correct
/// index with a double-counting one — the search would return that row once per copy, which is worse
/// than losing the index outright because nothing looks wrong.
#[test]
fn a_rebuild_over_a_re_used_primary_key_does_not_double_the_row() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    d.sql("CREATE FULLTEXT INDEX fx ON t (body);");
    d.sql("INSERT INTO t VALUES (1, 'alpha beta');");
    d.sql("DELETE FROM t WHERE id = 1;");
    d.sql("INSERT INTO t VALUES (1, 'alpha gamma');");
    // Premise: both versions are in the heap the rebuild will scan, so the duplicate is reachable.
    let heap_rows = d.catalog.get_table("t").unwrap().first_directory_page_id;
    let scanned = ferrodb::storage::heap_file_manager::HeapFileManager::open(heap_rows, d.bp.clone())
        .scan()
        .count();
    assert_eq!(scanned, 2, "premise failed: the heap holds {scanned} slots, so there is no duplicate to de-duplicate");

    rebuild_indexes(&mut d.catalog, &d.bp).unwrap();

    assert_eq!(
        d.postings_for("t", "body", "alpha"),
        vec![1],
        "one posting for the token both heap versions share"
    );
    assert_eq!(d.ids("SEARCH t (body) FOR 'alpha';"), vec![1], "and the row once");
}

/// **A pre-existing defect this feature found, pinned where it was found.**
///
/// The bug is in the *primary* index rebuild, not in anything full-text: `rebuild_indexes` inserted
/// one entry per heap **slot**, and `DELETE` leaves its slot behind, so a re-used primary key ended
/// up with two entries under one key. `insert_entry` appends rather than overwrites, so `search`
/// returned whichever copy binary search landed on — and when that was the tombstone, a point lookup
/// of a live row answered with nothing.
///
/// Breaking shape: `INSERT (1, ..); DELETE id = 1; INSERT (1, ..);` then a recovery rebuild, then a
/// point lookup. Measured before the fix: primary entries `[(1, {5,1}), (1, {5,0})]`, `search(1)` →
/// the tombstoned `{5,0}`, `SELECT * FROM t WHERE id = 1;` → zero rows while `SELECT * FROM t;`
/// returned the row. B8 found it because a full-text search resolves every posting through the
/// primary index, so the search returned nothing for a row whose postings were correct.
#[test]
fn a_rebuild_resolves_a_re_used_primary_key_to_the_live_version() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    d.sql("INSERT INTO t VALUES (1, 'alpha beta');");
    d.sql("DELETE FROM t WHERE id = 1;");
    d.sql("INSERT INTO t VALUES (1, 'alpha gamma');");
    d.sql("INSERT INTO t VALUES (2, 'delta');");

    rebuild_indexes(&mut d.catalog, &d.bp).unwrap();

    let entry = d.catalog.get_table("t").unwrap();
    let tree = BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, d.bp.clone());
    let entries: Vec<(Value, RecordId)> = tree
        .range_scan(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(entries.len(), 2, "one entry per key, not one per heap slot: {entries:?}");

    // The entry for key 1 must be the live version, which is the one a scan of the table returns.
    assert_eq!(
        d.ids("SELECT * FROM t WHERE id = 1;"),
        vec![1],
        "a point lookup must find the live row it plainly has"
    );
    assert_eq!(d.rows("SELECT * FROM t;").len(), 2);
    assert_eq!(d.ids("SELECT * FROM t WHERE id = 2;"), vec![2]);
}

/// A full-text index must not be mistaken for a B-tree index on the same column.
///
/// Breaking shape: `WHERE body = '<whole value>'` on a column that has **only** a full-text index.
/// `optimizer::has_index` reports any column named in `TableEntry::indexes` as range-scannable and
/// hands it to `SecondaryIndexScan`, which compares the first key component against a whole-value
/// bound. A token index registered in that list would answer this equality with the rows whose
/// *token* equalled the value — that is, none of them.
#[test]
fn a_fulltext_index_is_not_offered_to_the_range_scanner() {
    let mut d = seeded();
    assert_eq!(
        d.ids("SELECT * FROM docs WHERE body = 'wired charger';"),
        vec![2],
        "an equality on the indexed column must still find the row by its whole value"
    );

    // And both kinds can coexist on one column, each answering its own kind of question.
    d.sql("CREATE INDEX bx ON docs (body);");
    assert_eq!(d.ids("SELECT * FROM docs WHERE body = 'wired charger';"), vec![2]);
    let mut charger = d.ids("SEARCH docs (body) FOR 'charger';");
    charger.sort();
    assert_eq!(charger, vec![1, 2]);
    assert_eq!(
        d.catalog.get_table("docs").unwrap().indexes.len(),
        1,
        "the B-tree index went in the B-tree list"
    );
    assert_eq!(d.catalog.get_table("docs").unwrap().fulltext_indexes.len(), 1);
}

/// Every refusal, each with the case it must still allow. A refusal with no allowed neighbour is
/// indistinguishable from a feature that never works.
#[test]
fn the_refusals_refuse_and_the_neighbouring_cases_are_allowed() {
    let mut d = db();
    d.sql("CREATE TABLE docs (id INTEGER NOT NULL, body VARCHAR(200), qty INTEGER);");

    // A non-VARCHAR column has no text to tokenize.
    let e = d.err("CREATE FULLTEXT INDEX bad ON docs (qty);");
    assert!(e.contains("VARCHAR"), "the refusal must say why: {e}");
    // allowed:
    d.sql("CREATE FULLTEXT INDEX fx ON docs (body);");

    // The same column twice.
    let e = d.err("CREATE FULLTEXT INDEX again ON docs (body);");
    assert!(e.to_lowercase().contains("index already exists"), "{e}");

    // Unknown column and unknown table keep the shared messages.
    let e = d.err("CREATE FULLTEXT INDEX fx ON docs (nosuch);");
    assert!(e.contains("no such column"), "{e}");
    let e = d.err("CREATE FULLTEXT INDEX fx ON nosuch (body);");
    assert!(e.contains("unknown table"), "{e}");

    // Searching a column with no full-text index names the statement that would fix it.
    let e = d.err("SEARCH docs (qty) FOR 'x';");
    assert!(e.contains("CREATE FULLTEXT INDEX"), "the refusal must say how to fix it: {e}");

    // A query with no tokens is refused, not answered with an empty result.
    d.sql("INSERT INTO docs VALUES (1, 'hello world', 3);");
    let e = d.err("SEARCH docs (body) FOR '###';");
    assert!(e.contains("no tokens"), "{e}");
    // allowed, and it finds the row:
    assert_eq!(d.ids("SEARCH docs (body) FOR 'hello';"), vec![1]);
    // a query that tokenizes but matches nothing is a legitimate empty answer, not an error
    assert!(d.ids("SEARCH docs (body) FOR 'absent';").is_empty());

    // TOP 0 would return nothing whatever the data says.
    let e = d.err("SEARCH docs (body) FOR 'hello' TOP 0;");
    assert!(e.contains("TOP must be at least 1"), "{e}");
    assert_eq!(d.ids("SEARCH docs (body) FOR 'hello' TOP 1;"), vec![1]);

    // DDL inside a transaction, like every other DDL statement here.
    d.sql("BEGIN;");
    let e = d.err("CREATE FULLTEXT INDEX tx ON docs (body);");
    assert!(e.contains("DDL not allowed in txn"), "{e}");
    // ...and a search inside one is fine, on that transaction's snapshot
    assert_eq!(d.ids("SEARCH docs (body) FOR 'hello';"), vec![1]);
    d.sql("COMMIT;");

    // Inside an agent session the index cannot see the branch's writes, so the read is refused
    // rather than answered from the shared tables.
    d.sql("BEGIN AGENT SESSION AS 'a';");
    let e = d.err("SEARCH docs (body) FOR 'hello';");
    assert!(e.contains("agent session"), "{e}");
    d.sql("ABANDON;");
    // allowed again once the session is over
    assert_eq!(d.ids("SEARCH docs (body) FOR 'hello';"), vec![1]);
}

/// A `NULL` in the indexed column posts nothing and matches nothing, and does not stop the rows
/// around it from being indexed.
#[test]
fn a_null_value_is_indexed_as_no_tokens() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(100));");
    d.sql("CREATE FULLTEXT INDEX fx ON t (body);");
    d.sql("INSERT INTO t VALUES (1, NULL);");
    d.sql("INSERT INTO t VALUES (2, 'present');");

    assert!(d.postings("t", "body").iter().all(|(_, pk)| *pk == 2), "only row 2 has tokens");
    assert_eq!(d.ids("SEARCH t (body) FOR 'present';"), vec![2]);

    // and a value updated from NULL to text becomes findable
    d.sql("UPDATE t SET body = 'arrived' WHERE id = 1;");
    assert_eq!(d.ids("SEARCH t (body) FOR 'arrived';"), vec![1]);
}

/// A token longer than the index can store is skipped, and the words beside it are not.
///
/// Breaking shape: one alphanumeric run longer than a B+tree leaf's payload budget. `VARCHAR(600)`
/// permits it; a leaf holding a single oversized entry cannot be split, so without the cap the
/// `INSERT` panics inside the tree instead of the word simply not being indexed.
#[test]
fn an_over_long_token_is_skipped_and_its_neighbours_are_indexed() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, body VARCHAR(600));");
    d.sql("CREATE FULLTEXT INDEX fx ON t (body);");
    let long = "x".repeat(400);
    d.sql(&format!("INSERT INTO t VALUES (1, 'before {long} after');"));

    assert_eq!(d.ids("SEARCH t (body) FOR 'before';"), vec![1]);
    assert_eq!(d.ids("SEARCH t (body) FOR 'after';"), vec![1]);
    let tokens: Vec<String> = d.postings("t", "body").into_iter().map(|(t, _)| t).collect();
    assert_eq!(tokens, vec!["after".to_string(), "before".to_string()], "the long run is not stored");
}

/// Two full-text indexes on one table, on different columns, do not share a tree.
#[test]
fn two_fulltext_indexes_on_one_table_stay_separate() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, title VARCHAR(100), body VARCHAR(200));");
    d.sql("CREATE FULLTEXT INDEX tx ON t (title);");
    d.sql("CREATE FULLTEXT INDEX bx ON t (body);");
    d.sql("INSERT INTO t VALUES (1, 'alpha', 'beta');");
    d.sql("INSERT INTO t VALUES (2, 'beta', 'alpha');");

    assert_eq!(d.ids("SEARCH t (title) FOR 'alpha';"), vec![1]);
    assert_eq!(d.ids("SEARCH t (body) FOR 'alpha';"), vec![2]);
    assert_eq!(d.postings_for("t", "title", "alpha"), vec![1]);
    assert_eq!(d.postings_for("t", "body", "alpha"), vec![2]);
}

/// `DROP TABLE` takes the posting tree with it, and the table can be recreated and re-indexed.
#[test]
fn dropping_a_table_takes_its_posting_tree() {
    let mut d = seeded();
    assert_eq!(d.catalog.get_table("docs").unwrap().fulltext_indexes.len(), 1);
    d.sql("DROP TABLE docs;");
    assert!(d.catalog.get_table("docs").is_none());

    d.sql("CREATE TABLE docs (id INTEGER NOT NULL, body VARCHAR(200));");
    d.sql("INSERT INTO docs VALUES (7, 'fresh start');");
    d.sql("CREATE FULLTEXT INDEX fx ON docs (body);");
    assert_eq!(d.ids("SEARCH docs (body) FOR 'fresh';"), vec![7]);
}
