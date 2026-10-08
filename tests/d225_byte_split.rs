//! D225 — a B+tree page splits by BYTES, not by entry count.
//!
//! # The defect, at `9aa6968`
//!
//! `BPlusTreeLeafPage::split` cut at `mid = key_arr.len() / 2`. With variable-width keys the wide
//! entries sort together, so a count split can put all of them in one half, and that half can be
//! larger than a page. `serialize` then copies it into the fixed page slice and panics, out of
//! range. Found by `d208-review2` A1 (`artie-research/frontier/d208_review2.md`), whose schedule
//! this file runs: an index on `VARCHAR(255)`, 16 rows of `'a'`, then 16 rows of 247 `'b'`s.
//!
//! - A narrow entry is `('a', pk)`: `Varchar` 1 tag + 2 length + 1 byte, `Integer` 1 + 4, so 9 bytes.
//! - A wide entry is `('b' x 247, pk)`: 1 + 2 + 247 + 5 = 255 bytes.
//! - After 31 rows the root leaf holds 16·9 + 15·255 = 3969 bytes, plus a 27-byte header = 3996,
//!   under the 4096-byte page, so nothing has split yet.
//! - The 32nd row makes it 4224 bytes, which is full. `mid = 16` puts the sixteen wide entries,
//!   4080 bytes, in the new right leaf, and 27 + 4080 = 4107 does not fit in 4096. At `9aa6968`
//!   that INSERT panics inside `serialize`, after the heap row and the primary entry are written.
//!
//! A full-text posting tree is the same tree type, keyed `(token, pk)`, and a 247-character token
//! is under `MAX_TOKEN_BYTES` (255), so the same numbers reach it: the twin below.
//!
//! # Instruments
//!
//! The premise reads the index's root page straight from the pool and measures every entry with
//! the page encoding (`BTreeSerialize`), so "the leaf holds the mixed widths" is observed rather
//! than inferred from the INSERTs. The checks after the split walk every page of the tree and
//! range-scan it, which is independent of whichever plan the SELECT gets. Every expected value is
//! written from the fixture's arithmetic, never computed by the code under test.

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::{DataType, Value};
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::error::FerroError;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::{admit_entry, BPlusTreeLeafPage, BPlusTreePage, BTreeSerialize};
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::rebuild_indexes;
use ferrodb::wal::txn::TxnManager;

/// A secondary index and a posting tree are both this.
type Tree = BPlusTreeManager<(Value, Value), ()>;

/// 16 narrow rows, then 16 wide ones.
const NARROW_ROWS: i32 = 16;
const WIDE_ROWS: i32 = 16;
/// Length of the wide value, in characters and (being ASCII) in bytes.
const WIDE_LEN: usize = 247;
/// `('a', pk)`: 1 + 2 + 1 + 5.
const NARROW_BYTES: usize = 9;
/// `('b' x 247, pk)`: 1 + 2 + 247 + 5.
const WIDE_BYTES: usize = 255;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn stack(dir: &Path) -> (Arc<BufferPoolManager>, Arc<TxnManager>) {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.join("d225.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.join("d225.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    (bp, txn)
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let (bp, txn) = stack(dir.path());
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
            .unwrap_or_else(|e| panic!("`{}` failed: {e}", abbreviate(sql)))
    }

    /// The error a statement is refused with. Panics if it is accepted.
    fn err(&mut self, sql: &str) -> String {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{}`: {:?}", abbreviate(sql), p.errors);
        match run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session) {
            Ok(_) => panic!("`{}` was accepted; it must be refused", abbreviate(sql)),
            Err(e) => e.to_string(),
        }
    }

    /// The first column of every returned row, as an `Integer`, sorted.
    fn ids(&mut self, sql: &str) -> Vec<i32> {
        let rows = match self.sql(sql) {
            Outcome::Rows(r) => r,
            other => panic!("expected rows from `{}`, got {:?}", abbreviate(sql), std::mem::discriminant(&other)),
        };
        let mut ids: Vec<i32> = rows
            .into_iter()
            .map(|r| match r[0] {
                Value::Integer(i) => i,
                ref other => panic!("expected an Integer id, got {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Every row a SELECT returns.
    fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        match self.sql(sql) {
            Outcome::Rows(r) => r,
            other => panic!("expected rows from `{}`, got {:?}", abbreviate(sql), std::mem::discriminant(&other)),
        }
    }

    /// The plan text `EXPLAIN` gives for `sql`.
    fn plan(&mut self, sql: &str) -> String {
        match self.sql(&format!("EXPLAIN {sql}")) {
            Outcome::Explain(text) => text,
            _ => panic!("EXPLAIN {}: expected a plan", abbreviate(sql)),
        }
    }

    /// Put `vals` into `table`'s heap directly, below every write path, as a build before D225
    /// could have stored it. Stamped with the `begin_ts` of the table's first row, which a
    /// committed SQL INSERT wrote, so a statement's snapshot sees it.
    fn legacy_row(&self, table: &str, vals: &[Value]) {
        let entry = self.catalog.get_table(table).expect("table").clone();
        let heap = HeapFileManager::open(entry.first_directory_page_id, self.bp.clone());
        let (_, first) = heap
            .scan()
            .next()
            .expect("premise: the table holds a committed row to copy the stamp from")
            .expect("read it");
        let begin_ts = first.version_header().expect("its header").begin_ts;
        heap.insert(Tuple::serialize(vals, &entry.schema, begin_ts).expect("encode the legacy row"))
            .expect("put it in the heap");
    }

    /// The root the catalog records for the secondary index on `table.column`.
    fn index_root(&self, table: &str, column: &str) -> u32 {
        self.catalog
            .get_table(table)
            .expect("table")
            .indexes
            .iter()
            .find(|i| i.column_name == column)
            .unwrap_or_else(|| panic!("no index on {table}.{column}"))
            .root_page_id
    }

    /// The root the catalog records for the full-text index on `table.column`.
    fn fulltext_root(&self, table: &str, column: &str) -> u32 {
        self.catalog
            .get_table(table)
            .expect("table")
            .fulltext_indexes
            .iter()
            .find(|i| i.column_name == column)
            .unwrap_or_else(|| panic!("no full-text index on {table}.{column}"))
            .root_page_id
    }
}

/// A statement with a 247-character literal in it is unreadable in a failure message.
fn abbreviate(sql: &str) -> String {
    match sql.char_indices().nth(100) {
        Some((cut, _)) if sql.len() > 120 => format!("{}...({} bytes)", &sql[..cut], sql.len()),
        _ => sql.to_string(),
    }
}

fn entry_bytes(k: &(Value, Value)) -> usize {
    let mut buf = Vec::new();
    k.serialize(&mut buf);
    buf.len()
}

/// **Premise:** the root is still one leaf, holding the 16 narrow entries followed by `wide`
/// wide ones, and it is not yet full. Measured on the page, not assumed from the INSERTs.
fn assert_root_leaf_holds_the_mixed_widths(tree: &Tree, root: u32, wide: usize) {
    match tree.read_node(root).expect("read the root") {
        BPlusTreePage::Leaf(leaf) => {
            let sizes: Vec<usize> = leaf.key_arr.iter().map(entry_bytes).collect();
            let narrow = NARROW_ROWS as usize;
            assert_eq!(sizes.len(), narrow + wide, "premise: the root leaf holds every row's entry");
            assert!(
                sizes[..narrow].iter().all(|&s| s == NARROW_BYTES),
                "premise: the first {narrow} entries are the 9-byte narrow ones, got {:?}",
                &sizes[..narrow]
            );
            assert!(
                sizes[narrow..].iter().all(|&s| s == WIDE_BYTES),
                "premise: the last {wide} entries are the 255-byte wide ones"
            );
            // 16·9 + 15·255 = 3969 bytes, + 27 header = 3996 < 4096.
            assert_eq!(sizes.iter().sum::<usize>(), narrow * NARROW_BYTES + wide * WIDE_BYTES);
            assert!(!leaf.is_full(), "premise: the leaf must not be full before the last row");
        }
        BPlusTreePage::Internal(_) => panic!("premise failed: the root split before the last row"),
    }
}

/// Walk every page reachable from `root`: none may be at or over the full threshold, and every
/// internal node must have one more child than keys. Returns `(leaves, internal nodes)`.
fn assert_every_page_is_under_the_threshold<K, V>(tree: &BPlusTreeManager<K, V>, root: u32) -> (usize, usize)
where
    K: Ord + Clone + BTreeSerialize,
    V: Ord + Clone + BTreeSerialize,
{
    let mut pending = vec![root];
    let (mut leaves, mut internals) = (0, 0);
    while let Some(id) = pending.pop() {
        match tree.read_node(id).expect("read a tree page") {
            BPlusTreePage::Leaf(leaf) => {
                assert!(!leaf.is_full(), "leaf page {id} ({} entries) was left at or over the page", leaf.key_arr.len());
                leaves += 1;
            }
            BPlusTreePage::Internal(node) => {
                assert!(!node.is_full(), "internal page {id} ({} keys) was left at or over the page", node.key_arr.len());
                assert_eq!(node.child_ptrs.len(), node.key_arr.len() + 1, "internal page {id} is malformed");
                pending.extend(&node.child_ptrs);
                internals += 1;
            }
        }
    }
    (leaves, internals)
}

/// The entries the review's schedule posts, in key order: `('a', 1..=16)` then `(wide, 17..=32)`.
fn expected_entries(wide: &str) -> Vec<(Value, Value)> {
    let mut e: Vec<(Value, Value)> =
        (1..=NARROW_ROWS).map(|i| (Value::Varchar("a".into()), Value::Integer(i))).collect();
    e.extend((NARROW_ROWS + 1..=NARROW_ROWS + WIDE_ROWS).map(|i| (Value::Varchar(wide.to_string()), Value::Integer(i))));
    e
}

fn scan_all(tree: &Tree) -> Vec<(Value, Value)> {
    tree.range_scan(Bound::Unbounded, Bound::Unbounded)
        .expect("scan the tree")
        .map(|r| r.expect("an entry").0)
        .collect()
}

/// **The review's SQL reproducer, on a secondary index.** Red at `9aa6968`: the 32nd INSERT panics
/// inside `serialize`.
#[test]
fn the_32nd_insert_into_a_mixed_width_varchar_index_splits_by_bytes() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(255));");
    d.sql("CREATE INDEX iv ON t (v);");
    let wide = "b".repeat(WIDE_LEN);
    for i in 1..=NARROW_ROWS {
        d.sql(&format!("INSERT INTO t VALUES ({i}, 'a');"));
    }
    for i in NARROW_ROWS + 1..NARROW_ROWS + WIDE_ROWS {
        d.sql(&format!("INSERT INTO t VALUES ({i}, '{wide}');"));
    }

    let root_before = d.index_root("t", "v");
    assert_root_leaf_holds_the_mixed_widths(&Tree::open(root_before, d.bp.clone()), root_before, 15);

    // The 32nd row. At 9aa6968 this is the panic.
    d.sql(&format!("INSERT INTO t VALUES ({}, '{wide}');", NARROW_ROWS + WIDE_ROWS));

    let root = d.index_root("t", "v");
    assert_ne!(root, root_before, "premise: the 32nd row must have split the root leaf");
    let tree = Tree::open(root, d.bp.clone());
    assert_eq!(assert_every_page_is_under_the_threshold(&tree, root), (2, 1), "one split: two leaves under one new root");
    assert_eq!(scan_all(&tree), expected_entries(&wide), "the index lost, duplicated or reordered an entry");

    assert_eq!(d.ids("SELECT id FROM t WHERE v = 'a';"), (1..=NARROW_ROWS).collect::<Vec<_>>());
    assert_eq!(
        d.ids(&format!("SELECT id FROM t WHERE v = '{wide}';")),
        (NARROW_ROWS + 1..=NARROW_ROWS + WIDE_ROWS).collect::<Vec<_>>()
    );
}

/// **The full-text twin.** The same widths reach a posting tree: `('a', pk)` and a 247-character
/// token under `MAX_TOKEN_BYTES`. Red at `9aa6968` on the same panic.
#[test]
fn the_32nd_insert_into_a_mixed_width_posting_tree_splits_by_bytes() {
    let mut d = db();
    d.sql("CREATE TABLE docs (id INTEGER NOT NULL, body VARCHAR(255));");
    d.sql("CREATE FULLTEXT INDEX fx ON docs (body);");
    let wide = "b".repeat(WIDE_LEN);
    for i in 1..=NARROW_ROWS {
        d.sql(&format!("INSERT INTO docs VALUES ({i}, 'a');"));
    }
    for i in NARROW_ROWS + 1..NARROW_ROWS + WIDE_ROWS {
        d.sql(&format!("INSERT INTO docs VALUES ({i}, '{wide}');"));
    }

    let root_before = d.fulltext_root("docs", "body");
    assert_root_leaf_holds_the_mixed_widths(&Tree::open(root_before, d.bp.clone()), root_before, 15);

    d.sql(&format!("INSERT INTO docs VALUES ({}, '{wide}');", NARROW_ROWS + WIDE_ROWS));

    let root = d.fulltext_root("docs", "body");
    assert_ne!(root, root_before, "premise: the 32nd row must have split the posting tree's root leaf");
    let tree = Tree::open(root, d.bp.clone());
    assert_eq!(assert_every_page_is_under_the_threshold(&tree, root), (2, 1), "one split: two leaves under one new root");
    assert_eq!(scan_all(&tree), expected_entries(&wide), "the posting tree lost, duplicated or reordered a posting");

    // TOP 100: the default is 10, which would truncate either answer.
    assert_eq!(d.ids("SEARCH docs (body) FOR 'a' TOP 100;"), (1..=NARROW_ROWS).collect::<Vec<_>>());
    assert_eq!(
        d.ids(&format!("SEARCH docs (body) FOR '{wide}' TOP 100;")),
        (NARROW_ROWS + 1..=NARROW_ROWS + WIDE_ROWS).collect::<Vec<_>>()
    );
}

/// **Every page stays under the threshold whatever the widths, at every level.**
///
/// 2000 byte-string keys into one tree, in an order and with widths from a fixed LCG. The widths
/// are bimodal on purpose, half narrow (8 to 23 bytes) and half wide (1700 to 1899): a leaf holds
/// at most two wide entries, so three wide entries landing in one half of a split is exactly the
/// count split's failure, and wide separators keep internal nodes to a handful of keys, so
/// internal splits happen constantly as well. Every page reachable from the root must be under
/// the full threshold, and every key must still be found, in order.
///
/// At `9aa6968` this is INFERRED red, not run (quiet mode): a count split of a leaf whose upper
/// half holds three wide entries puts over 5000 bytes in one half, so `serialize` panics.
#[test]
fn random_widths_never_leave_a_page_at_or_over_the_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.path().join("widths.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::create(bp).unwrap();

    let mut state: u64 = 0x2250_d225;
    let mut next = move || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (state >> 33) as u32
    };
    const N: u32 = 2000;
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(N as usize);
    for i in 0..N {
        // A random 4-byte prefix sets the insertion order, the index keeps keys unique, and the
        // length sets the width. An entry is (4 + len) + (4 + 4) bytes: at most 1911, under the
        // 2034-byte entry bound, so no insert here may be refused.
        let mut k = next().to_be_bytes().to_vec();
        k.extend_from_slice(&i.to_be_bytes());
        let len = (if next() % 2 == 0 { 8 + next() % 16 } else { 1700 + next() % 200 }) as usize;
        k.resize(len, 0x5A);
        tree.insert(k.clone(), i.to_be_bytes().to_vec())
            .unwrap_or_else(|e| panic!("insert {i} of a {}-byte key failed: {e}", k.len()));
        keys.push(k);
    }

    let root = tree.root_page_id.load(std::sync::atomic::Ordering::Acquire);
    let (leaves, internals) = assert_every_page_is_under_the_threshold(&tree, root);
    // About 1000 wide entries at two or fewer per leaf is at least 500 leaves; a few children per
    // internal node is well over 10 internal nodes. Floors far below both, so they fail only when
    // the fixture stopped splitting.
    assert!(leaves > 100, "only {leaves} leaves: the fixture did not split enough to mean anything");
    assert!(internals > 10, "only {internals} internal nodes: no internal splits were exercised");
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(tree.search(k).expect("search"), Some((i as u32).to_be_bytes().to_vec()), "key {i} was lost");
    }
    let scanned: Vec<Vec<u8>> = tree
        .range_scan(Bound::Unbounded, Bound::Unbounded)
        .expect("scan")
        .map(|e| e.expect("entry").0)
        .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(scanned, sorted, "the scan lost, duplicated or reordered keys");
}

// ---- The entry bound, from SQL -------------------------------------------------------------------
//
// A byte split exists for every entry up to `MAX_ENTRY_BYTES` (2034 bytes, key and value as
// stored; its doc has the proof), and a larger one is refused by name. These pin the bound at the
// byte through each SQL write path, and that a refused row leaves nothing behind. They compile
// against `9aa6968` and are red there for the plain reason that no bound existed: the rows over it
// were accepted.

/// **A secondary value whose entry is over the bound is refused by name, and the refused row
/// writes nothing.**
///
/// `(v, id)` under an INTEGER id is 3 + len + 5 bytes: 2026 characters is 2034 (admitted), 2027 is
/// 2035 (refused). "Writes nothing" is the half that needs `execution::insert` to ask the bound
/// BEFORE the heap write. Were the tree left to refuse it, the heap row and the primary entry
/// would already be written, the abort would undo only the heap, and id 2 could never be inserted
/// again: its primary entry would point at a deleted slot. So the test inserts id 2 again.
#[test]
fn a_secondary_value_over_the_entry_bound_is_refused_by_name_and_writes_nothing() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(3000));");
    d.sql("CREATE INDEX iv ON t (v);");
    d.sql(&format!("INSERT INTO t VALUES (1, '{}');", "x".repeat(2026)));

    let e = d.err(&format!("INSERT INTO t VALUES (2, '{}');", "y".repeat(2027)));
    assert!(e.contains("index entry too large: 2035 bytes"), "not the named refusal: {}", abbreviate(&e));

    d.sql("INSERT INTO t VALUES (2, 'short');");
    assert_eq!(d.ids("SELECT id FROM t;"), vec![1, 2], "the refused row left something behind");
    assert_eq!(d.ids("SELECT id FROM t WHERE v = 'short';"), vec![2]);
    assert_eq!(d.ids(&format!("SELECT id FROM t WHERE v = '{}';", "x".repeat(2026))), vec![1]);
}

/// **A VARCHAR primary key whose entry is over the bound is refused by name.** A primary entry is
/// the key and a 6-byte `RecordId`: 3 + 2025 + 6 = 2034 is admitted, 2026 characters is refused.
#[test]
fn a_varchar_primary_key_over_the_entry_bound_is_refused_by_name() {
    let mut d = db();
    d.sql("CREATE TABLE k (id VARCHAR(3000) NOT NULL, n INTEGER);");
    d.sql(&format!("INSERT INTO k VALUES ('{}', 1);", "p".repeat(2025)));
    let e = d.err(&format!("INSERT INTO k VALUES ('{}', 2);", "q".repeat(2026)));
    assert!(e.contains("index entry too large: 2035 bytes"), "not the named refusal: {}", abbreviate(&e));
    assert_eq!(d.ids("SELECT n FROM k;"), vec![1], "the refused row left something behind");
}

/// **An UPDATE to a value whose entry is over the bound is refused, and the row keeps its value.**
///
/// The row grows from 6 characters to 2027, so the heap is likely to move it, and the primary
/// entry would then be repointed before the secondary write. `execution::update` asks the bound
/// before either. The lookup by id goes through the primary index when the planner uses it; a
/// primary entry left pointing at an undone slot is what it would find.
#[test]
fn an_update_to_a_value_over_the_entry_bound_is_refused_and_the_row_keeps_its_value() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(3000));");
    d.sql("CREATE INDEX iv ON t (v);");
    d.sql("INSERT INTO t VALUES (1, 'before');");

    let e = d.err(&format!("UPDATE t SET v = '{}' WHERE id = 1;", "z".repeat(2027)));
    assert!(e.contains("index entry too large: 2035 bytes"), "not the named refusal: {}", abbreviate(&e));
    assert_eq!(d.ids("SELECT id FROM t WHERE v = 'before';"), vec![1], "the refused update changed the row");
    assert_eq!(d.ids("SELECT id FROM t WHERE id = 1;"), vec![1], "the row is unreachable by its key");

    let at = "z".repeat(2026);
    d.sql(&format!("UPDATE t SET v = '{at}' WHERE id = 1;"));
    assert_eq!(d.ids(&format!("SELECT id FROM t WHERE v = '{at}';")), vec![1]);
}

/// **A multi-row UPDATE is refused before its FIRST row is written, not at the row that fails.**
///
/// Two rows under VARCHAR primary keys of different lengths, set to one 2020-character value.
/// Row `'a'` makes (3 + 2020) + (3 + 1) = 2027 bytes of entry, admitted; the 20-character key
/// makes (3 + 2020) + (3 + 20) = 2046, refused. Asked row by row inside the write loop, row `'a'`
/// would already be written when row two is refused, and the abort undoes only the heap. Two
/// 1300-character filler rows share their heap page, so row `'a'` cannot grow in place: the heap
/// moves it and its primary entry is repointed before the refusal (INFERRED from
/// `HeapFileManager::update`, not measured). Both rows must read back unchanged, by value and by
/// key.
#[test]
fn a_multi_row_update_is_refused_before_its_first_row_is_written() {
    let mut d = db();
    d.sql("CREATE TABLE u (id VARCHAR(100) NOT NULL, n INTEGER, v VARCHAR(3000));");
    d.sql("CREATE INDEX iu ON u (v);");
    let long_key = "k".repeat(20);
    d.sql("INSERT INTO u VALUES ('a', 1, 'x');");
    d.sql(&format!("INSERT INTO u VALUES ('{long_key}', 2, 'x');"));
    d.sql(&format!("INSERT INTO u VALUES ('f1', 10, '{}');", "f".repeat(1300)));
    d.sql(&format!("INSERT INTO u VALUES ('f2', 11, '{}');", "g".repeat(1300)));
    // Premise (ii), asserted rather than assumed: the lookup by key below reads the primary index.
    let plan = d.plan("SELECT n FROM u WHERE id = 'a';");
    assert!(plan.contains("Index scan on u (col 0"), "premise failed: the key lookup is not a primary index scan: {plan}");

    // Review 5 R4, allocator probe: the refused statement must write nothing. Under a late
    // refusal the first row's write begins with its old version going to the time-travel heap,
    // whose first data page is allocated then, before and independently of the relocation, which
    // allocates a second page. The abort gives neither page back, and D202's undo frees none, so
    // the file's high-water mark rises on d225 alone and on main alike (INFERRED; review 6 §3).
    // Blind spot: allocation is first-fit, so a free hole below the mark would take one allocation
    // unseen. These fixtures free no page before the probe, so they have none.
    let pages_before = d.bp.disk_manager.high_water().unwrap();
    let e = d.err(&format!("UPDATE u SET v = '{}' WHERE n < 3;", "z".repeat(2020)));
    assert!(e.contains("index entry too large: 2046 bytes"), "not the named refusal: {}", abbreviate(&e));
    assert_eq!(d.bp.disk_manager.high_water().unwrap(), pages_before, "the refused UPDATE allocated a page: it wrote a row first");
    assert_eq!(d.ids("SELECT n FROM u WHERE v = 'x';"), vec![1, 2], "the refused update changed a row");
    assert_eq!(d.ids("SELECT n FROM u WHERE id = 'a';"), vec![1], "row 'a' is unreachable by its key");
    assert_eq!(d.ids(&format!("SELECT n FROM u WHERE id = '{long_key}';")), vec![2]);
}

/// **The NOT NULL refusal of a multi-row UPDATE also comes before its first write.** Not D225's
/// defect, the same shape: the check sat in the write loop, so a later row's NULL was refused
/// after an earlier row was written and moved, and the abort undoes only the heap. It now runs in
/// the pre-pass with the entry bound. Row `'k'` would set a NOT NULL column to NULL.
///
/// Row `'a'` must be one the heap MOVES, or the test cannot tell the two orders apart. By the
/// tuple layout (24-byte header, 1-byte null bitmap, 2-byte length per VARCHAR) it is 2037 bytes
/// as inserted and 4036 after `a = b`: under `MAX_TUPLE_SIZE` (4069), so the update is legal, and
/// over the roughly 650 bytes its page has left beside the filler, so it relocates (INFERRED from
/// `HeapFileManager::update`, not measured). A `b` of 2020 characters would make 4076 bytes, over
/// the tuple limit, and the old order would then fail on that instead: a red for the wrong reason.
#[test]
fn a_multi_row_update_refused_for_not_null_writes_no_row_first() {
    let mut d = db();
    d.sql("CREATE TABLE u (id VARCHAR(100) NOT NULL, n INTEGER, a VARCHAR(3000) NOT NULL, b VARCHAR(3000));");
    d.sql(&format!("INSERT INTO u VALUES ('a', 1, 'x', '{}');", "z".repeat(2000)));
    d.sql("INSERT INTO u VALUES ('k', 2, 'x', NULL);");
    d.sql(&format!("INSERT INTO u VALUES ('f1', 10, '{}', NULL);", "f".repeat(1300)));
    // Premise (ii), asserted rather than assumed: the lookup by key below reads the primary index.
    let plan = d.plan("SELECT n FROM u WHERE id = 'a';");
    assert!(plan.contains("Index scan on u (col 0"), "premise failed: the key lookup is not a primary index scan: {plan}");

    // Review 5 R4, allocator probe: the refused statement must write nothing. Under a late
    // refusal the first row's write begins with its old version going to the time-travel heap,
    // whose first data page is allocated then, before and independently of the relocation, which
    // allocates a second page. The abort gives neither page back, and D202's undo frees none, so
    // the file's high-water mark rises on d225 alone and on main alike (INFERRED; review 6 §3).
    // Blind spot: allocation is first-fit, so a free hole below the mark would take one allocation
    // unseen. These fixtures free no page before the probe, so they have none.
    let pages_before = d.bp.disk_manager.high_water().unwrap();
    let e = d.err("UPDATE u SET a = b WHERE n < 3;");
    assert!(e.contains("is declared NOT NULL"), "not the NOT NULL refusal: {}", abbreviate(&e));
    assert_eq!(d.bp.disk_manager.high_water().unwrap(), pages_before, "the refused UPDATE allocated a page: it wrote a row first");
    assert_eq!(d.ids("SELECT n FROM u WHERE id = 'a';"), vec![1], "row 'a' is unreachable by its key");
    assert_eq!(d.ids("SELECT n FROM u WHERE a = 'x';"), vec![1, 2], "the refused update changed a row");
}

/// **CREATE INDEX over a row too wide for an index entry is refused by name, registers nothing,
/// and leaks nothing, however often it is retried.**
///
/// An unindexed `VARCHAR(3000)` column may hold 2100 characters; an index on it would need a
/// (3 + 2100) + 5 = 2108-byte entry. The backfill's half-built tree must be freed on the refusal.
/// Freed pages are handed out again lowest-first, so if it is, every retry reuses the same pages
/// and the file's high-water mark stops moving; a leak raises it on every retry. Same for a
/// full-text index, whose posting `('y' x 255, 'p' x 1800)` is (3 + 255) + (3 + 1800) = 2061
/// bytes; the row itself is admitted, its primary entry being 3 + 1800 + 6 = 1809.
#[test]
fn a_create_index_the_entry_bound_refuses_registers_nothing_and_leaks_nothing() {
    let mut d = db();
    d.sql("CREATE TABLE w (id INTEGER NOT NULL, v VARCHAR(3000));");
    d.sql("INSERT INTO w VALUES (1, 'short');");
    d.sql(&format!("INSERT INTO w VALUES (2, '{}');", "w".repeat(2100)));
    let e = d.err("CREATE INDEX iw ON w (v);");
    assert!(e.contains("index entry too large: 2108 bytes"), "not the named refusal: {}", abbreviate(&e));
    let settled = d.bp.disk_manager.high_water().unwrap();
    for _ in 0..3 {
        d.err("CREATE INDEX iw ON w (v);");
    }
    assert_eq!(d.bp.disk_manager.high_water().unwrap(), settled, "each refused CREATE INDEX leaked its tree");
    assert!(d.catalog.get_table("w").unwrap().indexes.is_empty(), "a refused index was registered");

    d.sql("CREATE TABLE f (id VARCHAR(2000) NOT NULL, body VARCHAR(300));");
    d.sql(&format!("INSERT INTO f VALUES ('{}', '{}');", "p".repeat(1800), "y".repeat(255)));
    let e = d.err("CREATE FULLTEXT INDEX ff ON f (body);");
    assert!(e.contains("index entry too large: 2061 bytes"), "not the named refusal: {}", abbreviate(&e));
    let settled = d.bp.disk_manager.high_water().unwrap();
    for _ in 0..3 {
        d.err("CREATE FULLTEXT INDEX ff ON f (body);");
    }
    assert_eq!(d.bp.disk_manager.high_water().unwrap(), settled, "each refused full-text index leaked its tree");
    assert!(d.catalog.get_table("f").unwrap().fulltext_indexes.is_empty(), "a refused full-text index was registered");
}

/// **A crash-recovery rebuild over a row an earlier build indexed is refused by name before any
/// tree is freed.**
///
/// A build before D225 could store an entry over the bound whenever its count split happened to
/// fit. This one cannot write such a row through SQL at all, so the row is put in the heap
/// directly: `(v, id)` would be (3 + 2100) + 5 = 2108 bytes. The rebuild frees each old tree
/// before refilling it and persists the catalog only at the end, so a refusal part-way would
/// leave the catalog naming freed pages. It must refuse before the first free.
///
/// The roots alone cannot show that: pages are reallocated lowest-first, so a rebuild that frees
/// a root gets the same page back. Three things show it, one per place the refusal could land:
///
/// - after a refill: the old primary tree never held key 2 (row 2 went into the heap directly),
///   and any rebuild adds it;
/// - after a fresh tree is created on a freed root: that tree is empty, so key 1 is gone;
/// - between a free and the next allocation: the freed root is the lowest free page, so the next
///   page handed out is that root. This one holds whether or not the root's image reached disk.
#[test]
fn a_rebuild_over_a_row_an_earlier_build_indexed_is_refused_before_anything_is_freed() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(3000));");
    d.sql("CREATE INDEX iv ON t (v);");
    d.sql("INSERT INTO t VALUES (1, 'kept');");
    let entry = d.catalog.get_table("t").unwrap().clone();
    HeapFileManager::open(entry.first_directory_page_id, d.bp.clone())
        .insert(Tuple::serialize(&[Value::Integer(2), Value::Varchar("w".repeat(2100))], &entry.schema, 1).unwrap())
        .unwrap();
    let index_before = d.index_root("t", "v");

    let e = rebuild_indexes(&mut d.catalog, &d.bp).expect_err("the rebuild must refuse the oversized row");
    let msg = e.to_string();
    assert!(
        msg.contains("cannot rebuild the indexes of 't'") && msg.contains("index entry too large: 2108 bytes"),
        "not the named refusal: {}",
        abbreviate(&msg)
    );
    assert_eq!(d.catalog.get_table("t").unwrap().primary_index_root, entry.primary_index_root, "the primary tree was rebuilt");
    let primary = BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, d.bp.clone());
    assert!(primary.search(&Value::Integer(1)).unwrap().is_some(), "the old primary tree lost key 1");
    assert_eq!(
        primary.search(&Value::Integer(2)).unwrap(),
        None,
        "the primary tree holds key 2, which only a rebuild adds: it was freed and rebuilt before the refusal"
    );
    let next = d.bp.new_page().unwrap();
    assert!(
        next != entry.primary_index_root && next != index_before,
        "page {next}, an old root, came back from the allocator: it was freed before the refusal"
    );
    assert_eq!(d.index_root("t", "v"), index_before, "the secondary tree was rebuilt");
    let old = Tree::open(index_before, d.bp.clone());
    assert_eq!(
        old.search(&(Value::Varchar("kept".into()), Value::Integer(1))).unwrap(),
        Some(()),
        "the old secondary tree no longer answers: it was freed"
    );
}

// ---- Review 4 (PREREG amendment 1): legacy data and the pre-checks' untested arms --------------
//
// "Legacy" means data a build before D225 could have written: an entry over `MAX_ENTRY_BYTES`
// that its count split happened to accept. This build cannot write one through any path, so it is
// put below them: into the heap with `legacy_row`, or into an index page directly.

/// **F1a — an ALTER that would re-point a legacy oversized primary key is refused before any row
/// moves.** The legacy key is 2100 characters, a (3 + 2100) + 6 = 2109-byte primary entry.
/// `commit_rewrite` re-points the entry of every row it moves, through `upsert`, which would
/// refuse it part way through the heap, after earlier rows were converted in place. The ALTER is
/// asked the bound for every key before the first tuple moves. Red at `72e1620`: the ALTER is
/// accepted (the legacy row has no primary entry to re-point).
#[test]
fn an_alter_that_would_repoint_a_legacy_oversized_key_is_refused_before_any_row_moves() {
    let mut d = db();
    d.sql("CREATE TABLE k (id VARCHAR(3000) NOT NULL, n INTEGER);");
    d.sql("INSERT INTO k VALUES ('a', 1);");
    d.legacy_row("k", &[Value::Varchar("p".repeat(2100)), Value::Integer(2)]);

    let e = d.err("ALTER TABLE k ALTER COLUMN n TYPE BIGINT;");
    assert!(
        e.contains("this ALTER would re-point the primary-index entry") && e.contains("index entry too large: 2109 bytes"),
        "not the named refusal: {}",
        abbreviate(&e)
    );
    // Review 5: the one remedy that works, also for a deleted row.
    assert!(e.contains("copy the table's live rows into a new table"), "the refusal does not name the remedy: {e}");
    // Review 6 F1: and no in-place advice, which a legacy key cannot take.
    assert!(!e.to_lowercase().contains("shorten"), "the refusal suggests shortening in place: {e}");
    assert_eq!(
        d.catalog.get_table("k").unwrap().schema.columns[1].data_type,
        DataType::Integer,
        "the refused ALTER installed the new shape"
    );
    assert_eq!(d.ids("SELECT n FROM k;"), vec![1, 2], "the refused ALTER changed a row");
}

/// **F1b — an UPDATE of a row under a legacy oversized primary key is refused before it is
/// written.** Same 2109-byte key. The UPDATE may have to re-point the key's entry if the heap
/// moves the row, which is not known until the row is written, so the pre-pass asks for every row.
/// `SET n = 3` is the same size, so the heap updates in place: without the check the UPDATE
/// would succeed, which is what makes this a killer. Red at `72e1620`: the UPDATE is accepted.
#[test]
fn an_update_of_a_row_under_a_legacy_oversized_key_is_refused_before_it_is_written() {
    let mut d = db();
    d.sql("CREATE TABLE k (id VARCHAR(3000) NOT NULL, n INTEGER);");
    d.sql("INSERT INTO k VALUES ('a', 1);");
    d.legacy_row("k", &[Value::Varchar("p".repeat(2100)), Value::Integer(2)]);

    let e = d.err("UPDATE k SET n = 3 WHERE n = 2;");
    assert!(e.contains("index entry too large: 2109 bytes"), "not the named refusal: {}", abbreviate(&e));
    // Review 5: a key cannot be UPDATEd, and DELETE then INSERT would leave the deleted tuple
    // under the key for good, so the message names the one remedy and never suggests that.
    assert!(e.contains("copy the table's live rows into a new table"), "the refusal does not name the remedy: {e}");
    // Case-sensitive on purpose (review 6 F8): the remedy itself says a "deleted" row under such
    // a key stays in the table. What must not appear is the statement DELETE offered as advice. A lowercase
    // "delete the row" suggestion would pass this guard; the text is one constant, reviewed.
    assert!(!e.contains("DELETE"), "the refusal suggests DELETE, which creates the problem for a key: {e}");
    assert!(!e.to_lowercase().contains("shorten"), "the refusal suggests shortening in place: {e}");
    assert_eq!(d.ids("SELECT n FROM k;"), vec![1, 2], "the refused UPDATE changed a row");
}

/// **F1c, the merged-tree test — a no-cut refusal after the heap write leaves no dangling primary
/// entry.** RED BY DESIGN on `d225-byte-split` alone; green on the merge with #16.
///
/// The secondary index's root leaf is written to its page directly, as an earlier build could
/// leave it: `('m' x 2992, 100)` of 3000 bytes and `('z' x 1061, 101)` of 1069, exactly the
/// 4069-byte body, full. A new `('a' x 2026, 1)` is 2034 bytes, which every pre-check admits, and
/// sorts first: cutting after it leaves 4069 on the right, which is full, and cutting after `m`
/// leaves 5034 on the left. So the tree refuses inside the secondary insert, AFTER the heap write
/// and the primary upsert, where no executor pre-check can see it. The abort undoes the heap row.
/// Only D202 (#16, `record_primary_write`) undoes the primary entry; without it, id 1 points at a
/// deleted slot and the INSERT that reuses it fails with `SlotDeleted`.
#[test]
fn a_no_cut_refusal_after_the_heap_write_leaves_no_dangling_primary_entry() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(3000));");
    d.sql("CREATE INDEX iv ON t (v);");
    let root = d.index_root("t", "v");
    let mut legacy = BPlusTreeLeafPage::<(Value, Value), ()>::new(root);
    legacy.insert_entry((Value::Varchar("m".repeat(2992)), Value::Integer(100)), ());
    legacy.insert_entry((Value::Varchar("z".repeat(1061)), Value::Integer(101)), ());
    assert_eq!(legacy.payload_len(), 4069, "premise: exactly the leaf body");
    assert!(legacy.is_full(), "premise: an earlier build left it full");
    let image = legacy.serialize().expect("premise: a leaf of exactly the body serializes");
    {
        let frame_i = d.bp.fetch_page(root).unwrap();
        let mut frame = d.bp.frame_write(frame_i);
        frame.data = image;
        drop(frame);
        d.bp.unpin_page(root, true);
    }
    let first = (Value::Varchar("a".repeat(2026)), Value::Integer(1));
    assert_eq!(entry_bytes(&first), 2034, "premise: 3 + 2026 + 5");
    admit_entry(&first, &()).expect("premise: every pre-check admits it");

    let e = d.err(&format!("INSERT INTO t VALUES (1, '{}');", "a".repeat(2026)));
    assert!(e.contains("no split point"), "not the no-cut refusal: {}", abbreviate(&e));
    // Red here on d225 alone: "the slot is delted".
    d.sql("INSERT INTO t VALUES (1, 'short');");
    assert_eq!(d.ids("SELECT id FROM t WHERE id = 1;"), vec![1]);
    assert_eq!(d.ids("SELECT id FROM t WHERE v = 'short';"), vec![1]);
}

/// **S1 — an INSERT whose posting is over the bound, into an existing full-text index, is refused
/// and writes nothing.** A posting `('y' x 255, 'p' x 1800)` is (3 + 255) + (3 + 1800) = 2061
/// bytes; the row itself is admitted (primary entry 3 + 1800 + 6 = 1809). "Writes nothing" is the
/// part the INSERT's full-text pre-check exists for: without it the tree refuses in `post_tokens`,
/// after the heap write and the primary upsert, and re-inserting the key fails with `SlotDeleted`
/// (on d225 alone; #16's undo would repair it).
#[test]
fn an_insert_whose_posting_is_over_the_bound_into_an_existing_full_text_index_writes_nothing() {
    let mut d = db();
    d.sql("CREATE TABLE f (id VARCHAR(2000) NOT NULL, n INTEGER, body VARCHAR(300));");
    d.sql("CREATE FULLTEXT INDEX ff ON f (body);");
    let pk = "p".repeat(1800);

    let e = d.err(&format!("INSERT INTO f VALUES ('{pk}', 1, '{}');", "y".repeat(255)));
    assert!(e.contains("index entry too large: 2061 bytes"), "not the named refusal: {}", abbreviate(&e));

    d.sql(&format!("INSERT INTO f VALUES ('{pk}', 2, 'short');"));
    assert_eq!(d.ids("SELECT n FROM f;"), vec![2], "the refused row left something behind");
    let root = d.fulltext_root("f", "body");
    assert_eq!(
        scan_all(&Tree::open(root, d.bp.clone())),
        vec![(Value::Varchar("short".into()), Value::Varchar(pk.clone()))],
        "the posting tree holds something the refused row posted"
    );
}

/// **S2 — an UPDATE whose new posting is over the bound is refused before the row moves.** The
/// same 2061-byte posting, reached by UPDATE. The row grows 250 bytes beside a 2000-byte filler
/// on its page, so the heap relocates it (INFERRED from `HeapFileManager::update`, not measured)
/// and would re-point its primary entry before `post_tokens` refused. The filler's one
/// 2000-character run is over `MAX_TOKEN_BYTES`, so it posts nothing. The key lookup's plan is
/// asserted, not assumed.
#[test]
fn an_update_whose_new_posting_is_over_the_bound_is_refused_before_the_row_moves() {
    let mut d = db();
    d.sql("CREATE TABLE g (id VARCHAR(2000) NOT NULL, n INTEGER, body VARCHAR(3000));");
    d.sql("CREATE FULLTEXT INDEX fg ON g (body);");
    let pk = "p".repeat(1800);
    d.sql(&format!("INSERT INTO g VALUES ('{pk}', 1, 'short');"));
    d.sql(&format!("INSERT INTO g VALUES ('f', 10, '{}');", "g".repeat(2000)));
    let plan = d.plan(&format!("SELECT n FROM g WHERE id = '{pk}';"));
    assert!(plan.contains("Index scan on g (col 0"), "premise failed: the key lookup is not a primary index scan: {}", abbreviate(&plan));

    // Review 5 R4, allocator probe: the refused statement must write nothing. Under a late
    // refusal the first row's write begins with its old version going to the time-travel heap,
    // whose first data page is allocated then, before and independently of the relocation, which
    // allocates a second page. The abort gives neither page back, and D202's undo frees none, so
    // the file's high-water mark rises on d225 alone and on main alike (INFERRED; review 6 §3).
    // Blind spot: allocation is first-fit, so a free hole below the mark would take one allocation
    // unseen. These fixtures free no page before the probe, so they have none.
    let pages_before = d.bp.disk_manager.high_water().unwrap();
    let e = d.err(&format!("UPDATE g SET body = '{}' WHERE n = 1;", "y".repeat(255)));
    assert!(e.contains("index entry too large: 2061 bytes"), "not the named refusal: {}", abbreviate(&e));
    assert_eq!(d.bp.disk_manager.high_water().unwrap(), pages_before, "the refused UPDATE allocated a page: it wrote the row first");
    assert_eq!(d.ids(&format!("SELECT n FROM g WHERE id = '{pk}';")), vec![1], "the row is unreachable by its key");
    assert_eq!(d.ids("SELECT n FROM g WHERE body = 'short';"), vec![1], "the refused UPDATE changed the row");
}

/// **S3, primary arm — a crash-recovery rebuild over a legacy oversized primary key is refused by
/// name before anything is freed.** The 2109-byte key from F1a. The primary tree is the first one
/// the rebuild frees, so the discriminators are about it: the catalog still names its root, the
/// old tree still answers for key `'a'`, and the next page the allocator hands out is not that
/// root (a freed root is the lowest free page).
#[test]
fn a_rebuild_over_a_legacy_oversized_primary_key_is_refused_before_anything_is_freed() {
    let mut d = db();
    d.sql("CREATE TABLE k (id VARCHAR(3000) NOT NULL, n INTEGER);");
    d.sql("INSERT INTO k VALUES ('a', 1);");
    d.legacy_row("k", &[Value::Varchar("p".repeat(2100)), Value::Integer(2)]);
    let root = d.catalog.get_table("k").unwrap().primary_index_root;

    let e = rebuild_indexes(&mut d.catalog, &d.bp).expect_err("the rebuild must refuse the legacy key");
    let msg = e.to_string();
    assert!(
        msg.contains("cannot rebuild the indexes of 'k'")
            && msg.contains("its primary entry")
            && msg.contains("index entry too large: 2109 bytes"),
        "not the named refusal: {}",
        abbreviate(&msg)
    );
    assert!(msg.contains("copy the table's live rows into a new table"), "the refusal does not name the remedy: {msg}");
    assert!(!msg.to_lowercase().contains("shorten"), "the refusal suggests shortening in place: {msg}");
    assert_eq!(d.catalog.get_table("k").unwrap().primary_index_root, root, "the primary tree was rebuilt");
    let primary = BPlusTreeManager::<Value, RecordId>::open(root, d.bp.clone());
    assert!(primary.search(&Value::Varchar("a".into())).unwrap().is_some(), "the old primary tree lost key 'a'");
    let next = d.bp.new_page().unwrap();
    assert_ne!(next, root, "the primary root came back from the allocator: it was freed before the refusal");
}

/// **S3, full-text arm — the same, for a legacy posting over the bound.** The legacy row is
/// `('p' x 1800, 'y' x 255)`: its primary entry (1809) is admitted and its posting (2061) is not.
/// The rebuild would free the primary tree first and rebuild it, adding the legacy key, then free
/// the posting tree. So: the old primary tree must not hold the legacy key (only a rebuild adds
/// it), the old posting tree must still hold `('kept', 'a')`, and the next page handed out must
/// not be either root.
#[test]
fn a_rebuild_over_a_legacy_oversized_posting_is_refused_before_anything_is_freed() {
    let mut d = db();
    d.sql("CREATE TABLE f (id VARCHAR(2000) NOT NULL, body VARCHAR(300));");
    d.sql("CREATE FULLTEXT INDEX ff ON f (body);");
    d.sql("INSERT INTO f VALUES ('a', 'kept');");
    let pk = "p".repeat(1800);
    d.legacy_row("f", &[Value::Varchar(pk.clone()), Value::Varchar("y".repeat(255))]);
    let primary_root = d.catalog.get_table("f").unwrap().primary_index_root;
    let posting_root = d.fulltext_root("f", "body");

    let e = rebuild_indexes(&mut d.catalog, &d.bp).expect_err("the rebuild must refuse the legacy posting");
    let msg = e.to_string();
    assert!(
        msg.contains("cannot rebuild the indexes of 'f'")
            && msg.contains("a posting in the full-text index on 'body'")
            && msg.contains("index entry too large: 2061 bytes"),
        "not the named refusal: {}",
        abbreviate(&msg)
    );
    let primary = BPlusTreeManager::<Value, RecordId>::open(primary_root, d.bp.clone());
    assert_eq!(primary.search(&Value::Varchar(pk)).unwrap(), None, "the primary tree was rebuilt: it holds the legacy key");
    assert_eq!(
        Tree::open(posting_root, d.bp.clone()).search(&(Value::Varchar("kept".into()), Value::Varchar("a".into()))).unwrap(),
        Some(()),
        "the old posting tree no longer answers: it was freed"
    );
    let next = d.bp.new_page().unwrap();
    assert!(next != primary_root && next != posting_root, "page {next}, an old root, came back from the allocator");
}

// ---- Review 6 (PREREG amendment 3): an ALTER that widens an indexed column; a backfill over a
// ---- deleted row ----------------------------------------------------------------------------------

/// **H1 — an ALTER that widens an indexed column past the bound is refused before any row moves.**
///
/// `n INTEGER` is indexed and the key is 2023 characters. INSERT admits every entry: the primary
/// is (3 + 2023) + 6 = 2032 and the secondary 5 + 2026 = 2031. `TYPE BIGINT` turns the secondary
/// entry into 9 + 2026 = 2035. The rewrite itself writes no index entry, so before the fix it was
/// accepted, and the next crash recovery rebuilt the index from the heap, refused 2035, and did not
/// open: a database this build wrote. Red at `ef64ffd`: "was accepted; it must be refused".
#[test]
fn an_alter_that_widens_an_indexed_column_past_the_bound_is_refused_before_any_row_moves() {
    let mut d = db();
    d.sql("CREATE TABLE t (id VARCHAR(3000) NOT NULL, n INTEGER);");
    d.sql("CREATE INDEX i ON t (n);");
    d.sql(&format!("INSERT INTO t VALUES ('{}', 7);", "p".repeat(2023)));

    let e = d.err("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;");
    assert!(
        e.contains("this ALTER would widen") && e.contains("index entry too large: 2035 bytes"),
        "not the named refusal: {}",
        abbreviate(&e)
    );
    assert!(e.contains("copy the table's live rows into a new table"), "the refusal does not name the remedy: {e}");
    assert!(!e.to_lowercase().contains("shorten"), "the refusal suggests shortening in place: {e}");
    // Review 7 K3: the refusal says how far the key is from fitting under the new type (2035 - 2034).
    assert!(e.contains("shorter by at least 1 bytes"), "the refusal does not say how much shorter the key must be: {e}");
    assert_eq!(d.catalog.get_table("t").unwrap().schema.columns[1].data_type, DataType::Integer, "the refused ALTER installed the new shape");
    assert_eq!(d.ids("SELECT n FROM t;"), vec![7], "the refused ALTER changed the row");
}

/// **H1's control — widening to exactly the bound is accepted, and the database it writes reopens.**
/// The key is 2022 characters: 9 + 2025 = 2034. The ALTER goes through, and the crash-recovery
/// rebuild, which widens the secondary entry from the heap, admits it.
#[test]
fn an_alter_that_widens_an_indexed_column_to_exactly_the_bound_is_accepted_and_recovers() {
    let mut d = db();
    d.sql("CREATE TABLE t (id VARCHAR(3000) NOT NULL, n INTEGER);");
    d.sql("CREATE INDEX i ON t (n);");
    d.sql(&format!("INSERT INTO t VALUES ('{}', 7);", "p".repeat(2022)));

    d.sql("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;");
    assert_eq!(d.catalog.get_table("t").unwrap().schema.columns[1].data_type, DataType::BigInt);
    // `matches!`, not `assert_eq!`: `Value::eq` compares numbers by value, so it cannot tell
    // `BigInt(7)` from `Integer(7)` (review 7 K7).
    let rows = d.rows("SELECT n FROM t;");
    assert!(
        matches!(rows.as_slice(), [r] if matches!(r.as_slice(), [Value::BigInt(7)])),
        "the retyped row did not read back as BIGINT 7: {rows:?}"
    );
    rebuild_indexes(&mut d.catalog, &d.bp).expect("the database this build wrote must reopen");
}

/// **F2 — CREATE INDEX over a deleted row whose value is too long names the table copy, and the
/// copy works in this build.**
///
/// `v` is unindexed, so INSERT admits no entry for it. `(1, 'x' x 2100)` is inserted and deleted;
/// DELETE stamps the tuple and leaves it, and nothing purges it. The backfill must index every
/// tuple the heap holds, because an older snapshot may need a deleted one, so `('x' x 2100, 1)`,
/// (3 + 2100) + 5 = 2108 bytes, refuses the index, on every retry. The refusal says the row is
/// deleted and names the copy. Then the copy is carried out, as the refusal words it, with this
/// build: SELECT the live rows, INSERT each into a new table, DROP the old one, CREATE it again,
/// copy back. After that the index builds. Red at `ef64ffd` on the message (the tree's own text).
#[test]
fn a_create_index_over_a_deleted_long_row_names_the_table_copy_and_the_copy_works() {
    let mut d = db();
    let shape = "(id INTEGER NOT NULL, v VARCHAR(3000))";
    d.sql(&format!("CREATE TABLE t {shape};"));
    d.sql(&format!("INSERT INTO t VALUES (1, '{}');", "x".repeat(2100)));
    d.sql("INSERT INTO t VALUES (2, 'kept');");
    d.sql("DELETE FROM t WHERE id = 1;");

    let e = d.err("CREATE INDEX iv ON t (v);");
    assert!(
        e.contains("index entry too large: 2108 bytes")
            && e.contains("The row is deleted")
            && e.contains("copy the table's live rows into a new table")
            && !e.to_lowercase().contains("shorten"),
        "not the named backfill refusal: {}",
        abbreviate(&e)
    );
    let again = d.err("CREATE INDEX iv ON t (v);");
    assert!(again.contains("index entry too large: 2108 bytes"), "the tombstone stopped refusing: {}", abbreviate(&again));

    // The remedy, step by step, in this build.
    let live = d.rows("SELECT id, v FROM t;");
    assert_eq!(live, vec![vec![Value::Integer(2), Value::Varchar("kept".into())]], "premise: one live row");
    let insert_all = |d: &mut Db, table: &str, rows: &[Vec<Value>]| {
        for r in rows {
            match (&r[0], &r[1]) {
                (Value::Integer(id), Value::Varchar(v)) => {
                    d.sql(&format!("INSERT INTO {table} VALUES ({id}, '{v}');"));
                }
                other => panic!("unexpected row shape {other:?}"),
            }
        }
    };
    d.sql(&format!("CREATE TABLE t_copy {shape};"));
    insert_all(&mut d, "t_copy", &live);
    d.sql("DROP TABLE t;");
    d.sql(&format!("CREATE TABLE t {shape};"));
    let copied = d.rows("SELECT id, v FROM t_copy;");
    insert_all(&mut d, "t", &copied);
    d.sql("DROP TABLE t_copy;");

    d.sql("CREATE INDEX iv ON t (v);");
    assert_eq!(d.ids("SELECT id FROM t WHERE v = 'kept';"), vec![2], "the rebuilt table lost its row");
}

// ---- Review 7 (PREREG amendment 4) ---------------------------------------------------------------

/// A database with an agent runtime, for the tests that MERGE a branch. Its shape is copied from
/// `integration_merge_ddl_atomicity.rs`.
struct MergeDb {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    session: Session,
}

fn merge_db() -> MergeDb {
    let dir = tempfile::tempdir().unwrap();
    let (bp, txn) = stack(dir.path());
    let catalog = Catalog::create(bp.clone()).unwrap();
    let runtime = Arc::new(AgentRuntime::new());
    let session = Session::with_runtime(runtime.clone());
    MergeDb { _dir: dir, catalog, bp, txn, runtime, session }
}

impl MergeDb {
    fn exec(&mut self, sql: &str, session: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{}`: {:?}", abbreviate(sql), p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), session)
    }

    fn sql(&mut self, sql: &str) -> Outcome {
        let mut session = std::mem::replace(&mut self.session, Session::with_runtime(self.runtime.clone()));
        let out = self.exec(sql, &mut session);
        self.session = session;
        out.unwrap_or_else(|e| panic!("`{}` failed: {e}", abbreviate(sql)))
    }

    fn branch(&mut self, name: &str) -> Session {
        let mut s = Session::with_runtime(self.runtime.clone());
        self.exec(&format!("BEGIN AGENT SESSION AS '{name}';"), &mut s).expect("open the branch");
        s
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

    /// Every row a trunk SELECT returns.
    fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        match self.sql(sql) {
            Outcome::Rows(r) => r,
            _ => panic!("`{}` did not return rows", abbreviate(sql)),
        }
    }
}

/// **H2: a MERGE whose row would land over the bound is refused before the schema moves.**
///
/// The trunk has `t (id VARCHAR(3000) NOT NULL, n INTEGER)`, indexed on `n`, and no rows, so the
/// plan's own check (review 6 H1, trunk rows only) has nothing to ask. The branch inserts
/// `('p' x 2023, 7)`, whose entry under INTEGER is 5 + 2026 = 2031, and stages `n TYPE BIGINT`.
/// At landing that entry is 9 + 2026 = 2035. The INSERT executor that publishes the row refuses
/// it, but only after `apply_plan` has retyped `t` durably: E82's half-merge. The landing check
/// must refuse first. State is asserted before wording. Red at `3f9f41c`: "the refused MERGE
/// retyped the table anyway".
#[test]
fn a_merge_whose_row_would_land_over_the_bound_is_refused_before_the_schema_moves() {
    let mut d = merge_db();
    d.sql("CREATE TABLE t (id VARCHAR(3000) NOT NULL, n INTEGER);");
    d.sql("CREATE INDEX i ON t (n);");
    let shape_before = d.shape("t");
    let heap_before = d.heap("t");

    let mut agent = d.branch("agent-h2");
    d.exec(&format!("INSERT INTO t VALUES ('{}', 7);", "p".repeat(2023)), &mut agent)
        .expect("the branch takes the row: its entry is 2031 bytes under INTEGER");
    d.exec("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;", &mut agent).expect("the branch stages the retype");

    let e = d.exec("MERGE;", &mut agent).err().expect("MERGE must be refused");
    let msg = e.to_string();
    assert_eq!(shape_before, d.shape("t"), "the refused MERGE retyped the table anyway: {}", abbreviate(&msg));
    assert_eq!(heap_before, d.heap("t"), "the refused MERGE wrote to the heap");
    assert!(
        msg.contains("this MERGE would publish a row into 't'") && msg.contains("index entry too large: 2035 bytes"),
        "not the landing check's refusal: {}",
        abbreviate(&msg)
    );
}

/// **K6: a chain is refused at the step that widens an entry past the bound**, as the statements
/// typed one at a time would be. The trunk holds `('p' x 2023, 7)`, indexed on `n`. A branch
/// stages `n TYPE BIGINT` (entry 9 + 2026 = 2035) and then `n TYPE DECIMAL`. The final entry,
/// `Decimal("7")`, is 4 + 2026 = 2030, which fits. Measuring only the final shape accepts the
/// chain; the first edit alone would be refused. Red at `3f9f41c`: the MERGE is accepted.
#[test]
fn a_chain_is_refused_at_the_step_that_widens_an_entry_past_the_bound() {
    let mut d = merge_db();
    d.sql("CREATE TABLE t (id VARCHAR(3000) NOT NULL, n INTEGER);");
    d.sql("CREATE INDEX i ON t (n);");
    d.sql(&format!("INSERT INTO t VALUES ('{}', 7);", "p".repeat(2023)));
    let shape_before = d.shape("t");

    let mut agent = d.branch("agent-k6");
    d.exec("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;", &mut agent).expect("stage edit 1");
    d.exec("ALTER TABLE t ALTER COLUMN n TYPE DECIMAL;", &mut agent).expect("stage edit 2");

    let msg = match d.exec("MERGE;", &mut agent) {
        Ok(_) => panic!("`MERGE;` was accepted; it must be refused"),
        Err(e) => e.to_string(),
    };
    assert_eq!(shape_before, d.shape("t"), "the refused MERGE retyped the table anyway: {}", abbreviate(&msg));
    assert!(
        msg.contains("index entry too large: 2035 bytes") && msg.contains("at edit 1 of 2"),
        "not refused at the edit that widened the entry: {}",
        abbreviate(&msg)
    );
}

/// **K5: a VARCHAR widening over a legacy entry it does not grow is accepted.** `v VARCHAR(2500)`
/// is indexed, and a legacy row puts `(2, 'x' x 2100)` in the heap: an entry of
/// (3 + 2100) + 5 = 2108 bytes, which only an earlier build could store. `TYPE VARCHAR(3000)`
/// moves no byte of it, so it writes nothing that was not already there, and it must not be
/// refused. Red at `3f9f41c`: refused with "this ALTER would widen".
#[test]
fn a_varchar_widening_over_a_legacy_entry_it_does_not_grow_is_accepted() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(2500));");
    d.sql("CREATE INDEX iv ON t (v);");
    d.sql("INSERT INTO t VALUES (1, 'short');");
    d.legacy_row("t", &[Value::Integer(2), Value::Varchar("x".repeat(2100))]);

    d.sql("ALTER TABLE t ALTER COLUMN v TYPE VARCHAR(3000);");
    assert_eq!(d.catalog.get_table("t").unwrap().schema.columns[1].data_type, DataType::Varchar(3000));
}

/// **K1's control: the backfill says a LIVE row is live.** Without it, the deleted arm's own
/// sentence would be the only one ever asserted, and a refusal that got the arm wrong would pass.
/// This passes at `3f9f41c` too: it is a control, not a red test.
#[test]
fn a_create_index_over_a_live_long_row_says_the_row_is_live() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(3000));");
    d.sql(&format!("INSERT INTO t VALUES (2, '{}');", "x".repeat(2100)));
    let e = d.err("CREATE INDEX iv ON t (v);");
    assert!(
        e.contains("index entry too large: 2108 bytes") && e.contains("The row is live") && !e.contains("The row is deleted"),
        "not the live arm of the backfill refusal: {}",
        abbreviate(&e)
    );
}

/// **K4: the full-text backfill asks the bound of a deleted row's postings.** A deleted
/// `('p' x 1800, 'y' x 255)`: primary entry 1809, posting (3 + 255) + (3 + 1800) = 2061. Without
/// the backfill's own check, the tree refuses with its generic text and no row or arm is named.
#[test]
fn a_create_fulltext_index_over_a_deleted_row_with_an_over_bound_posting_is_refused_by_the_backfill() {
    let mut d = db();
    d.sql("CREATE TABLE f (id VARCHAR(2000) NOT NULL, body VARCHAR(300));");
    let pk = "p".repeat(1800);
    d.sql(&format!("INSERT INTO f VALUES ('{pk}', '{}');", "y".repeat(255)));
    d.sql(&format!("DELETE FROM f WHERE id = '{pk}';"));
    let e = d.err("CREATE FULLTEXT INDEX ff ON f (body);");
    assert!(
        e.contains("cannot build the index on 'f.body'")
            && e.contains("index entry too large: 2061 bytes")
            && e.contains("The row is deleted"),
        "not the backfill's refusal: {}",
        abbreviate(&e)
    );
}

/// **K2, a merged-tree test: a deleted long row is replaced by inserting its key again, and the
/// index then builds.** RED BY DESIGN on `d225-byte-split` alone; green on the merge with #16.
///
/// F2's fixture: `(1, 'x' x 2100)` inserted and deleted, so CREATE INDEX is refused. The remedy
/// the refusal names for a deleted row whose key fits: INSERT the key again with a value that fits,
/// remove it again, and create the index. On #16 (`4296723`) the re-inserted row takes the dead
/// version's slot and the long dead version moves to the table's history, so the backfill no
/// longer meets it. Without #16 the re-insert takes a new slot, the long tuple stays, and the
/// last CREATE INDEX fails with "cannot build the index".
#[test]
fn a_deleted_long_row_is_replaced_by_inserting_its_key_again_and_the_index_then_builds() {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(3000));");
    d.sql(&format!("INSERT INTO t VALUES (1, '{}');", "x".repeat(2100)));
    d.sql("INSERT INTO t VALUES (2, 'kept');");
    d.sql("DELETE FROM t WHERE id = 1;");
    let e = d.err("CREATE INDEX iv ON t (v);");
    assert!(e.contains("The row is deleted"), "premise: the backfill refuses the deleted row: {}", abbreviate(&e));

    d.sql("INSERT INTO t VALUES (1, 'x');");
    d.sql("DELETE FROM t WHERE id = 1;");
    // Red here on d225 alone: "cannot build the index".
    d.sql("CREATE INDEX iv ON t (v);");
    assert_eq!(d.ids("SELECT id FROM t WHERE v = 'kept';"), vec![2]);
}

// ---- PREREG amendment 5: the bound asked when a row is typed on a branch --------------------------

/// **A branch INSERT over the bound in the branch's own shape is refused when it is typed.** The
/// branch stages `n TYPE BIGINT` FIRST, so `('p' x 2023, 7)` would land with a secondary entry of
/// 9 + 2026 = 2035 bytes, one over the bound, although it is 5 + 2026 = 2031 in the table's shape
/// as it stands. The agent is told when it types the row, and nothing is staged. The control,
/// `('p' x 2022, 8)`, is 2034 and is taken. Red at `c96d166`: the INSERT is accepted.
///
/// The MERGE's landing check is the second guard, for a row typed BEFORE the edit that pushes it
/// over, which this check cannot see; its own test is
/// `a_merge_whose_row_would_land_over_the_bound_is_refused_before_the_schema_moves`.
#[test]
fn a_branch_insert_over_the_bound_in_the_branch_shape_is_refused_when_typed() {
    let mut d = merge_db();
    d.sql("CREATE TABLE t (id VARCHAR(3000) NOT NULL, n INTEGER);");
    d.sql("CREATE INDEX i ON t (n);");

    let mut agent = d.branch("agent-typed-insert");
    d.exec("ALTER TABLE t ALTER COLUMN n TYPE BIGINT;", &mut agent).expect("the branch stages the retype");
    let msg = match d.exec(&format!("INSERT INTO t VALUES ('{}', 7);", "p".repeat(2023)), &mut agent) {
        Ok(_) => panic!("the branch took a row whose index entry is over the bound in its own shape"),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.contains("index entry too large: 2035 bytes")
            && msg.contains("on this branch")
            && msg.contains("Nothing has been staged"),
        "not the typing-time refusal: {}",
        abbreviate(&msg)
    );
    d.exec(&format!("INSERT INTO t VALUES ('{}', 8);", "p".repeat(2022)), &mut agent)
        .expect("the control, 2034 bytes in the branch's shape, is taken");

    d.exec("MERGE;", &mut agent).expect("the merge lands the retype and the control row");
    assert_eq!(d.shape("t")[1].1, DataType::BigInt, "the merge did not land the retype");
    let rows = d.rows("SELECT n FROM t;");
    assert!(
        matches!(rows.as_slice(), [r] if matches!(r.as_slice(), [Value::BigInt(8)])),
        "the trunk must hold exactly the control row: {rows:?}"
    );
}

/// **A branch UPDATE to a value over the bound is refused when it is typed.** `v` is indexed and
/// `'x' x 2027` makes the entry (3 + 2027) + 5 = 2035 bytes. The UPDATE writes that entry, so it is
/// asked, as the UPDATE executor and the landing check ask it. The control, `'x' x 2026`, is 2034
/// and is taken. Red at `c96d166`: the UPDATE is accepted.
#[test]
fn a_branch_update_over_the_bound_is_refused_when_typed() {
    let mut d = merge_db();
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(3000));");
    d.sql("CREATE INDEX iv ON t (v);");
    d.sql("INSERT INTO t VALUES (1, 'short');");

    let mut agent = d.branch("agent-typed-update");
    let msg = match d.exec(&format!("UPDATE t SET v = '{}' WHERE id = 1;", "x".repeat(2027)), &mut agent) {
        Ok(_) => panic!("the branch took an UPDATE whose index entry is over the bound"),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.contains("index entry too large: 2035 bytes")
            && msg.contains("on this branch")
            && msg.contains("Nothing has been staged"),
        "not the typing-time refusal: {}",
        abbreviate(&msg)
    );
    let at = "x".repeat(2026);
    d.exec(&format!("UPDATE t SET v = '{at}' WHERE id = 1;"), &mut agent)
        .expect("the control, 2034 bytes, is taken");

    d.exec("MERGE;", &mut agent).expect("the merge lands the control");
    assert_eq!(d.rows("SELECT v FROM t WHERE id = 1;"), vec![vec![Value::Varchar(at)]], "the trunk row is not the control");
}
