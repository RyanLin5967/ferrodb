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

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::{BPlusTreePage, BTreeSerialize};
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

    let e = d.err(&format!("UPDATE u SET v = '{}' WHERE n < 3;", "z".repeat(2020)));
    assert!(e.contains("index entry too large: 2046 bytes"), "not the named refusal: {}", abbreviate(&e));
    assert_eq!(d.ids("SELECT n FROM u WHERE v = 'x';"), vec![1, 2], "the refused update changed a row");
    assert_eq!(d.ids("SELECT n FROM u WHERE id = 'a';"), vec![1], "row 'a' is unreachable by its key");
    assert_eq!(d.ids(&format!("SELECT n FROM u WHERE id = '{long_key}';")), vec![2]);
}

/// **The NOT NULL refusal of a multi-row UPDATE also comes before its first write.** Not D225's
/// defect, the same shape: the check sat in the write loop, so a later row's NULL was refused
/// after an earlier row was written and moved, and the abort undoes only the heap. It now runs in
/// the pre-pass with the entry bound. Row `'a'` grows by 2020 characters beside a filler, so it
/// cannot stay in place (INFERRED, not measured); row `'k'` would set a NOT NULL column to NULL.
#[test]
fn a_multi_row_update_refused_for_not_null_writes_no_row_first() {
    let mut d = db();
    d.sql("CREATE TABLE u (id VARCHAR(100) NOT NULL, n INTEGER, a VARCHAR(3000) NOT NULL, b VARCHAR(3000));");
    d.sql(&format!("INSERT INTO u VALUES ('a', 1, 'x', '{}');", "z".repeat(2020)));
    d.sql("INSERT INTO u VALUES ('k', 2, 'x', NULL);");
    d.sql(&format!("INSERT INTO u VALUES ('f1', 10, '{}', NULL);", "f".repeat(1300)));

    let e = d.err("UPDATE u SET a = b WHERE n < 3;");
    assert!(e.contains("is declared NOT NULL"), "not the NOT NULL refusal: {}", abbreviate(&e));
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
/// a root gets the same page back. What shows it is the primary tree's CONTENT. Row 2 went into
/// the heap directly, so the old primary tree never held key 2, and any rebuild of it adds it.
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
    assert_eq!(d.index_root("t", "v"), index_before, "the secondary tree was rebuilt");
    let old = Tree::open(index_before, d.bp.clone());
    assert_eq!(
        old.search(&(Value::Varchar("kept".into()), Value::Integer(1))).unwrap(),
        Some(()),
        "the old secondary tree no longer answers: it was freed"
    );
}
