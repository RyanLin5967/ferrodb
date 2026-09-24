//! D222 — **`CREATE INDEX` and `CREATE FULLTEXT INDEX` must record the tree's root as it stands
//! AFTER the backfill, not as it stood when the empty tree was created.**
//!
//! # The defect, at `9aa6968`
//!
//! `Catalog::create_index` read `new_root_id` from the fresh tree straight after
//! `BPlusTreeManager::create`, and only then inserted every heap row; `create_fulltext_index` did
//! the same. The first split of the tree's only leaf is a ROOT split, which allocates a new root
//! page and stores it into the tree's cell (`insert_into_parent`, the `stack.is_empty()` arm), and
//! leaves the page `create` returned holding the left half. Every later split of that leaf keeps
//! the left half in place too, so the recorded page ends the backfill as the LEFTMOST LEAF. The
//! catalog recorded it as the root, `sync_root_cells` seeded the shared cell (D53) from the record,
//! and every later statement "descended" from the leftmost leaf:
//!
//! - **A lookup** reaches its key only through the leaf walk, which gives up after 64 hops
//!   (`read_leaf_for`'s `RIGHT_WALK`, D58) and falls back to the latched descent, which does not
//!   walk at all. A secondary scan then opens in the leftmost leaf, follows `next`, and yields
//!   every entry below the key — `SecondaryIndexScan` enforces an INCLUDED lower bound only
//!   through its start key — and a posting scan stops at the first foreign token and finds nothing.
//! - **An INSERT** lands in the leftmost leaf whatever its key. The leaf chain is then out of
//!   order, and a lookup of any key right of that leaf stops there, even one hop away, because the
//!   walk stops at the first leaf whose largest key is not below the key.
//!
//! # Fixture geometry, derived from source — and asserted, not trusted
//!
//! Every key is `(Varchar of 200 bytes, Integer)`: `1+2+200 + 1+4 = 208` bytes, and `()` adds
//! nothing (`index_page.rs`). A leaf is full at `27 + 20*208 = 4187 >= 4096`, so the 20th entry
//! splits it 10/10, and an ascending backfill leaves about 10 entries per leaf: [`ROWS`] = 1000
//! rows make about 100 leaves, and row [`FAR`] = 950 sits about 95 hops right of the leftmost
//! leaf. The hop count is MEASURED off the leaf chain before any answer is read, so if the geometry
//! is not what this paragraph says, the test fails at that premise and not at the answer.
//!
//! # What each test asserts before it believes an answer
//!
//! 1. **The backfill split the root.** The tree began as one leaf, so a chain of more than one leaf
//!    means its root moved away from the page `create` returned.
//! 2. **The far probe is past the walk** (the lookup tests): more than [`RIGHT_WALK`] leaves right
//!    of the leftmost leaf. Nearer than that, the walk rescues a stale root and nothing can go red.
//! 3. **The SELECT is the index path and only the index path**: `Index scan on t (col 1, [..])`
//!    with no `Filter` and no sequential scan. A residual `Filter` would discard the extra rows and
//!    hide the defect. `SEARCH` has no planner and exactly one access path — `FullTextSearch::open`
//!    reads `postings_for_token`'s scan of the posting tree — so there is no plan to assert for it.
//! 4. **A control probe answers**, so a red below is about WHERE the key is, not an index that is
//!    broken everywhere.
//!
//! Then the answer through SQL, then the recorded root compared with the tree's post-backfill root:
//! the one page whose descent reaches every leaf of the chain, in chain order.
//!
//! Every expected id comes from the fixture's construction — id `i` holds [`value`]`(i)` or
//! [`token`]`(i)`, a bijection — and never from the engine. The two INSERT tests pick their probe
//! off the chain (the first key of the second leaf) and check it against that bijection first.
//!
//! # A third symptom: `DROP TABLE` leaked the tree
//!
//! `drop_table` frees each index from its RECORDED root, and `free_subtree` on a leaf frees that
//! one page. So at `9aa6968` dropping a table whose index was built by a splitting backfill
//! returned the leftmost leaf and leaked every other page of the tree. `wal::recovery::
//! rebuild_indexes` frees the old tree from the same record and leaked the same way; that path is
//! not tested here.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::binder::binder::Binder;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::{explain_plan, optimize, pushdown};
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::{BPlusTreeLeafPage, BPlusTreePage};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// `BPlusTreeManager::read_leaf_for`'s `RIGHT_WALK` (D58): the `next` hops the lock-free descent
/// takes before it gives up. A key this close to a stale start is still found.
const RIGHT_WALK: usize = 64;
/// Rows in every fixture, ids `1..=ROWS`.
const ROWS: i32 = 1000;
/// The row the past-the-walk lookups probe: about 95 leaves right of the leftmost leaf.
const FAR: i32 = 950;
/// The control row: inside the leftmost leaf, which a stale root still reaches.
const NEAR: i32 = 5;
/// Padding after the 6-character prefix, so every key's text is 200 bytes.
const PAD: usize = 194;
/// Rows in the `DROP TABLE` test: about 30 leaves, enough to split the root, and no hop count to
/// meet because nothing there is looked up.
const LEAK_ROWS: i32 = 300;

type Tree = BPlusTreeManager<(Value, Value), ()>;
type Leaf = BPlusTreeLeafPage<(Value, Value), ()>;

/// Row `i`'s indexed value. Zero-padded, so value order is id order.
fn value(i: i32) -> String {
    format!("k{i:05}{}", "x".repeat(PAD))
}

/// Row `i`'s document: ONE token, because every character is alphanumeric, and 200 bytes, under
/// `MAX_TOKEN_BYTES`. Zero-padded, so token order is id order.
fn token(i: i32) -> String {
    format!("w{i:05}{}", "x".repeat(PAD))
}

/// A value, and a token, that sorts after every fixture key: `z` is after both `k` and `w`.
fn last_key(i: i32) -> String {
    format!("z{i:05}{}", "x".repeat(PAD))
}

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("d222.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d222.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { _dir: dir, catalog, bp, txn, session: Session::new() }
    }

    fn sql(&mut self, sql: &str) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        assert_eq!(stmts.len(), 1, "one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    /// The first column of every returned row, which every query here makes the primary key.
    fn ids(&mut self, sql: &str) -> Vec<i32> {
        match self.sql(sql) {
            Outcome::Rows(rows) => rows
                .into_iter()
                .map(|r| match r[0] {
                    Value::Integer(i) => i,
                    ref other => panic!("expected an Integer id from `{sql}`, got {other:?}"),
                })
                .collect(),
            other => panic!("expected rows from `{sql}`, got {:?}", std::mem::discriminant(&other)),
        }
    }

    /// The physical plan the optimizer chooses, rendered. Builds no executor.
    fn explain(&self, sql: &str) -> String {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        let logical = Binder::new(&self.catalog).bind(stmts.remove(0)).expect("bind");
        let physical = optimize(pushdown(logical), &self.catalog).expect("optimize");
        explain_plan(&physical, &self.catalog)
    }

    /// The root the catalog RECORDED for `t.v`'s B-tree index.
    fn secondary_root(&self) -> u32 {
        self.catalog.get_table("t").expect("table t").indexes.iter()
            .find(|i| i.column_name == "v").expect("an index on t.v").root_page_id
    }

    /// The root the catalog RECORDED for `docs.body`'s full-text index.
    fn fulltext_root(&self) -> u32 {
        self.catalog.get_table("docs").expect("table docs").fulltext_indexes.iter()
            .find(|i| i.column_name == "body").expect("a full-text index on docs.body").root_page_id
    }
}

/// `t` holding rows `1..=ROWS`, THEN `CREATE INDEX`, so every entry is written by the backfill.
/// `ANALYZE` last, for margin rather than necessity: with statistics `v` is unique and the index
/// costs 17 against the filtered sequential scan's 92 (`cost_model.rs`, `row_width` 24+4+255 = 283,
/// so 72 heap pages); without them the estimate is 10 rows and the index still wins, but only 89 to
/// 92, one constant away from the plan premise 3 refuses.
fn secondary_fixture() -> Db {
    let mut d = Db::new();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(255));");
    for i in 1..=ROWS {
        d.sql(&format!("INSERT INTO t VALUES ({i}, '{}');", value(i)));
    }
    d.sql("CREATE INDEX iv ON t (v);");
    d.sql("ANALYZE t;");
    d
}

/// `docs` holding rows `1..=ROWS`, THEN `CREATE FULLTEXT INDEX`, so every posting is written by
/// the backfill.
fn fulltext_fixture() -> Db {
    let mut d = Db::new();
    d.sql("CREATE TABLE docs (id INTEGER NOT NULL, body VARCHAR(255));");
    for i in 1..=ROWS {
        d.sql(&format!("INSERT INTO docs VALUES ({i}, '{}');", token(i)));
    }
    d.sql("CREATE FULLTEXT INDEX fx ON docs (body);");
    d
}

/// Every leaf of the tree, left to right, read off the LEAF CHAIN.
///
/// The recorded root is used only to reach some leaf; from there the walk goes `prev` to the head
/// and `next` to the tail, so the chain does not depend on whether the recorded root is the real
/// one. Bounded, because a chain that loops is itself a defect and must fail rather than hang.
fn leaf_chain(bp: &Arc<BufferPoolManager>, recorded_root: u32) -> Vec<Leaf> {
    let tree = Tree::open(recorded_root, bp.clone());
    let bound = 10 * ROWS as usize;
    let mut leaf = tree.leftmost_leaf().expect("descend to a leaf");
    let mut steps = 0;
    while let Some(prev) = leaf.prev {
        steps += 1;
        assert!(steps < bound, "the leaf chain's prev pointers loop");
        leaf = tree.read_leaf(prev).expect("read a leaf");
    }
    let mut chain = Vec::new();
    loop {
        let next = leaf.next;
        chain.push(leaf);
        assert!(chain.len() < bound, "the leaf chain's next pointers loop");
        match next {
            Some(n) => leaf = tree.read_leaf(n).expect("read a leaf"),
            None => return chain,
        }
    }
}

/// The leaves a descent from `page` reaches, left to right. From the tree's real root that is the
/// whole chain in chain order; from any other page it is a strict part of it.
fn leaves_under(tree: &Tree, page: u32, out: &mut Vec<u32>) {
    match tree.read_node(page).expect("read a tree page") {
        BPlusTreePage::Leaf(_) => out.push(page),
        BPlusTreePage::Internal(node) => {
            for child in node.child_ptrs {
                leaves_under(tree, child, out);
            }
        }
    }
}

/// The position in `chain` of the leaf holding `key`: 0 is the leftmost leaf, so this is also the
/// number of `next` hops from it.
fn leaf_holding(chain: &[Leaf], key: &(Value, Value)) -> Option<usize> {
    chain.iter().position(|l| l.key_arr.contains(key))
}

/// Premise 1.
fn assert_backfill_split_the_root(chain: &[Leaf], what: &str) {
    assert!(
        chain.len() > 1,
        "premise failed: {what}'s tree is still ONE leaf after backfilling {ROWS} rows, so its root \
         never moved and a root read before the backfill would be right by accident. Re-size the \
         fixture rather than relax this."
    );
}

/// Premise 3: the index scan is the WHOLE access path.
fn assert_index_path(plan: &str, sql: &str) {
    assert!(
        plan.contains("Index scan on t (col 1, [")
            && !plan.contains("Filter")
            && !plan.contains("Sequential scan"),
        "premise failed: `{sql}` must plan as a bare secondary index scan. A sequential scan never \
         touches the index, and a residual Filter would discard the rows a mispositioned scan \
         yields. Plan:\n{plan}"
    );
}

/// The recorded root compared with the tree's post-backfill root: the one page whose descent
/// reaches every leaf of the chain, in chain order.
fn assert_recorded_root_is_the_post_backfill_root(
    bp: &Arc<BufferPoolManager>,
    recorded_root: u32,
    chain: &[Leaf],
    what: &str,
) {
    let mut reached = Vec::new();
    leaves_under(&Tree::open(recorded_root, bp.clone()), recorded_root, &mut reached);
    let leaves: Vec<u32> = chain.iter().map(|l| l.page_id).collect();
    assert!(
        reached == leaves,
        "{what}: the recorded root, page {recorded_root}, is not the tree's post-backfill root. A \
         descent from it reaches {} of the {} leaves{}.",
        reached.len(),
        leaves.len(),
        if leaves.first() == Some(&recorded_root) {
            "; it IS the leftmost leaf — the page `create` returned, which the first root split left \
             behind as the left half"
        } else {
            ""
        }
    );
}

/// A keyed lookup past the leaf walk returns exactly its own row after `CREATE INDEX`.
///
/// At `9aa6968` the scan opens in the leftmost leaf, walks `next`, and yields every row from the
/// second leaf up to and including `FAR`: about 940 rows for a predicate that matches one.
#[test]
fn a_lookup_past_the_right_walk_after_create_index_returns_exactly_its_row() {
    let mut d = secondary_fixture();
    let chain = leaf_chain(&d.bp, d.secondary_root());

    assert_backfill_split_the_root(&chain, "t.v");
    let far = (Value::Varchar(value(FAR)), Value::Integer(FAR));
    let hops = leaf_holding(&chain, &far).expect("premise failed: row FAR has no index entry");
    assert!(
        hops > RIGHT_WALK,
        "premise failed: row {FAR} is {hops} leaves right of the leftmost leaf, inside the \
         {RIGHT_WALK}-hop walk that rescues a stale root, so this test cannot go red. Re-size the \
         fixture rather than relax this."
    );
    let near = (Value::Varchar(value(NEAR)), Value::Integer(NEAR));
    assert_eq!(leaf_holding(&chain, &near), Some(0), "premise failed: the control row must sit in the leftmost leaf");

    let near_sql = format!("SELECT id FROM t WHERE v = '{}';", value(NEAR));
    let far_sql = format!("SELECT id FROM t WHERE v = '{}';", value(FAR));
    assert_index_path(&d.explain(&near_sql), &near_sql);
    assert_index_path(&d.explain(&far_sql), &far_sql);
    assert_eq!(d.ids(&near_sql), vec![NEAR], "control: a key in the leftmost leaf must answer");

    let got = d.ids(&far_sql);
    assert!(
        got == vec![FAR],
        "`v = value({FAR})` must return exactly row {FAR}, which is {hops} leaves right of the \
         leftmost leaf; it returned {} rows, starting {:?}",
        got.len(),
        &got[..got.len().min(5)]
    );

    assert_recorded_root_is_the_post_backfill_root(&d.bp, d.secondary_root(), &chain, "t.v");
}

/// An `INSERT` after `CREATE INDEX` lands in the leaf that covers its key, and every other row
/// stays reachable.
///
/// At `9aa6968` the new entry lands in the leftmost leaf, whose largest key then sorts after every
/// key in the tree. The probe — the first key of the SECOND leaf, one hop from the leftmost and so
/// answered correctly before the INSERT — then stops at the leftmost leaf, meets the new key first,
/// and returns nothing.
#[test]
fn an_insert_after_create_index_lands_in_the_leaf_that_covers_its_key() {
    let mut d = secondary_fixture();
    let chain = leaf_chain(&d.bp, d.secondary_root());
    assert_backfill_split_the_root(&chain, "t.v");

    let (probe_v, probe_id) = match &chain[1].key_arr[0] {
        (Value::Varchar(v), Value::Integer(id)) => (v.clone(), *id),
        other => panic!("premise failed: an index entry of t.v is {other:?}"),
    };
    assert_eq!(probe_v, value(probe_id), "premise failed: the fixture's id <-> value bijection");
    let probe_sql = format!("SELECT id FROM t WHERE v = '{probe_v}';");
    assert_index_path(&d.explain(&probe_sql), &probe_sql);
    assert_eq!(
        d.ids(&probe_sql),
        vec![probe_id],
        "control, BEFORE the INSERT: the second leaf's first key is one hop from the leftmost leaf \
         and must answer whatever root was recorded"
    );

    let new_id = ROWS + 1;
    d.sql(&format!("INSERT INTO t VALUES ({new_id}, '{}');", last_key(new_id)));
    let new_sql = format!("SELECT id FROM t WHERE v = '{}';", last_key(new_id));
    assert_index_path(&d.explain(&new_sql), &new_sql);

    let got = d.ids(&probe_sql);
    assert!(
        got == vec![probe_id],
        "AFTER the INSERT, row {probe_id} — untouched, and one leaf right of the leftmost — must \
         still be found by its key; it returned {got:?}"
    );
    let got = d.ids(&new_sql);
    assert!(
        got == vec![new_id],
        "the inserted row must be found by its own key and nothing else with it; it returned {} \
         rows, starting {:?}",
        got.len(),
        &got[..got.len().min(5)]
    );

    let chain = leaf_chain(&d.bp, d.secondary_root());
    let key = (Value::Varchar(last_key(new_id)), Value::Integer(new_id));
    let at = leaf_holding(&chain, &key).expect("the INSERT posted no entry into t.v's index");
    assert_eq!(
        at,
        chain.len() - 1,
        "the new key sorts after every other key, so it belongs in the LAST leaf; it is in leaf {at} \
         of {}",
        chain.len()
    );
}

/// A `SEARCH` for a token past the leaf walk finds its row after `CREATE FULLTEXT INDEX`.
///
/// At `9aa6968` the posting scan opens in the leftmost leaf, steps to the second, meets a foreign
/// token there, and stops: zero rows.
#[test]
fn a_search_past_the_right_walk_after_create_fulltext_index_finds_its_row() {
    let mut d = fulltext_fixture();
    let chain = leaf_chain(&d.bp, d.fulltext_root());

    assert_backfill_split_the_root(&chain, "docs.body");
    let far = (Value::Varchar(token(FAR)), Value::Integer(FAR));
    let hops = leaf_holding(&chain, &far).expect("premise failed: row FAR has no posting");
    assert!(
        hops > RIGHT_WALK,
        "premise failed: row {FAR}'s posting is {hops} leaves right of the leftmost leaf, inside the \
         {RIGHT_WALK}-hop walk that rescues a stale root, so this test cannot go red. Re-size the \
         fixture rather than relax this."
    );
    let near = (Value::Varchar(token(NEAR)), Value::Integer(NEAR));
    assert_eq!(leaf_holding(&chain, &near), Some(0), "premise failed: the control posting must sit in the leftmost leaf");
    assert_eq!(
        d.ids(&format!("SEARCH docs (body) FOR '{}';", token(NEAR))),
        vec![NEAR],
        "control: a token in the leftmost leaf must be found"
    );

    let got = d.ids(&format!("SEARCH docs (body) FOR '{}';", token(FAR)));
    assert_eq!(
        got,
        vec![FAR],
        "the token of row {FAR}, {hops} leaves right of the leftmost leaf, must find exactly that row"
    );

    assert_recorded_root_is_the_post_backfill_root(&d.bp, d.fulltext_root(), &chain, "docs.body");
}

/// An `INSERT` after `CREATE FULLTEXT INDEX` posts into the leaf that covers its token, and every
/// other token stays findable. The full-text twin of the `CREATE INDEX` test above.
#[test]
fn an_insert_after_create_fulltext_index_posts_into_the_leaf_that_covers_its_token() {
    let mut d = fulltext_fixture();
    let chain = leaf_chain(&d.bp, d.fulltext_root());
    assert_backfill_split_the_root(&chain, "docs.body");

    let (probe_token, probe_id) = match &chain[1].key_arr[0] {
        (Value::Varchar(t), Value::Integer(id)) => (t.clone(), *id),
        other => panic!("premise failed: a posting of docs.body is {other:?}"),
    };
    assert_eq!(probe_token, token(probe_id), "premise failed: the fixture's id <-> token bijection");
    let probe_sql = format!("SEARCH docs (body) FOR '{probe_token}';");
    assert_eq!(
        d.ids(&probe_sql),
        vec![probe_id],
        "control, BEFORE the INSERT: the second leaf's first token is one hop from the leftmost \
         leaf and must be found whatever root was recorded"
    );

    let new_id = ROWS + 1;
    d.sql(&format!("INSERT INTO docs VALUES ({new_id}, '{}');", last_key(new_id)));

    assert_eq!(
        d.ids(&probe_sql),
        vec![probe_id],
        "AFTER the INSERT, row {probe_id} — untouched, and one leaf right of the leftmost — must \
         still be found by its token"
    );
    assert_eq!(
        d.ids(&format!("SEARCH docs (body) FOR '{}';", last_key(new_id))),
        vec![new_id],
        "the inserted row must be found by its own token"
    );

    let chain = leaf_chain(&d.bp, d.fulltext_root());
    let key = (Value::Varchar(last_key(new_id)), Value::Integer(new_id));
    let at = leaf_holding(&chain, &key).expect("the INSERT posted nothing into docs.body's index");
    assert_eq!(
        at,
        chain.len() - 1,
        "the new token sorts after every other token, so its posting belongs in the LAST leaf; it \
         is in leaf {at} of {}",
        chain.len()
    );
}

/// Build `t` with `LEAK_ROWS` rows and an index on `v` — created BEFORE the rows when
/// `index_first`, so the INSERT path builds it and `sync_roots` records every root move, and AFTER
/// them otherwise, so the backfill builds it — then drop and rebuild it twice.
///
/// Returns the highest allocated page with the second copy built, and again with the third built.
/// The first build-and-drop absorbs one-off growth, as in `catalog.rs`'s
/// `dropping_a_table_returns_its_pages_including_the_time_travel_heap`. The allocator hands out the
/// lowest free page, and identical statements need identical pages, so if the drop returned every
/// page the third copy lands exactly on the pages the second one freed and the two numbers match.
fn highest_page_across_a_drop(index_first: bool) -> (u32, u32) {
    let mut d = Db::new();
    let build = |d: &mut Db| {
        d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(255));");
        if index_first {
            d.sql("CREATE INDEX iv ON t (v);");
        }
        for i in 1..=LEAK_ROWS {
            d.sql(&format!("INSERT INTO t VALUES ({i}, '{}');", value(i)));
        }
        if !index_first {
            d.sql("CREATE INDEX iv ON t (v);");
        }
    };
    build(&mut d);
    d.sql("DROP TABLE t;");
    build(&mut d);
    let chain = leaf_chain(&d.bp, d.secondary_root());
    assert!(
        chain.len() > 1,
        "premise failed: {LEAK_ROWS} rows left t.v's tree one leaf, so there is no split tree to \
         leak and this measures nothing"
    );
    let peak = d.bp.disk_manager.bitmap_high_water().expect("read the allocation bitmap");
    d.sql("DROP TABLE t;");
    build(&mut d);
    let after = d.bp.disk_manager.bitmap_high_water().expect("read the allocation bitmap");
    (peak, after)
}

/// `DROP TABLE` returns every page of an index that `CREATE INDEX` built by backfill.
///
/// The control is the same table and the same rows with the index created FIRST, so no backfill
/// happens and the recorded root is kept current by `sync_roots`. It must not move at the base or
/// with the fix; if it does, something other than D222 is leaking and the second arm says nothing.
#[test]
fn drop_table_returns_every_page_of_an_index_built_by_backfill() {
    let (control_peak, control_after) = highest_page_across_a_drop(true);
    assert_eq!(
        control_after, control_peak,
        "control: with the index created BEFORE the rows, rebuilding an identical table after a \
         DROP moved the highest allocated page from {control_peak} to {control_after}. Something \
         other than the backfill's root read is leaking, so the arm below cannot be read."
    );

    let (peak, after) = highest_page_across_a_drop(false);
    assert_eq!(
        after, peak,
        "with the index built by CREATE INDEX's backfill, rebuilding an identical table after a \
         DROP moved the highest allocated page from {peak} to {after}: the drop did not return the \
         index's pages. `drop_table` frees from the recorded root, and a root read before the \
         backfill is the leftmost leaf, which frees one page."
    );
}
