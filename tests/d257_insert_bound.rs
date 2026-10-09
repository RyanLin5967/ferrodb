//! D257: an INSERT of a tuple no page can hold added a page to the heap on every attempt.
//!
//! RED FIRST, against `17e4c26`, and UNBUILT: written in quiet mode, where no compiler runs. It uses
//! only API that exists at `17e4c26` (and at `9aa6968`), so it compiles against the base, and each
//! test is predicted red or green there for the reason its own message names. Pre-registration:
//! artie-research `frontier/lane_d257_insert_bound.md` §3.
//!
//! - `HeapFileManager::find_or_make_page` asked the directory for `len + 4` bytes. No directory entry
//!   can exceed `PAGE_SIZE - HEADER_SIZE`, so for `len > MAX_TUPLE_SIZE` it always fell through to
//!   `add_empty_page`, which allocates a page and lists it, unlogged, before `insert_into` found the
//!   tuple did not fit. Every refused attempt left one more page in the file and the directory.
//! - It computed that `len + 4` as `len as u16 + SLOT_ENTRY_SIZE as u16`, which panics (debug) or
//!   wraps (release) at 65532..=65535 and truncates at 65536 and above.
//! - An UPDATE to such a row wrote the old version into the time-travel heap before the heap refused
//!   the new one. The abort's undo deletes the slot but not the directory's claim on its space.
//! - At `9aa6968` the refused insert also leaked its page pin. D237 (`f651096`) closed that before
//!   this lane began, so the pin assertions here are GREEN at `17e4c26`; T7 is what fails if the
//!   guard is removed from `insert_into`.
//!
//! What each test pins, and the mutant it kills (`bench/d257/firecheck.sh`):
//! - T1 `an_oversize_insert_is_refused_before_any_page_is_allocated`: the mechanism, at the heap.
//!   Kills MA (no refusal) and MF (the refusal does not name the limit).
//! - T2 `a_tuple_of_exactly_the_limit_is_still_accepted`: the control that the refusal is not one
//!   byte early. Kills MB.
//! - T3 `two_thousand_refused_inserts_leave_the_pool_and_the_file_as_they_were`: the ledger's volume
//!   claim. Kills MA, MF.
//! - T4 `the_space_arithmetic_refuses_rather_than_wrapping_at_the_u16_boundary`: the u16 lengths,
//!   each refused as a `Constraint` (review 1 R6c: an `Internal` naming the limit is not the
//!   refusal). Kills MA, MB (its control arm), MF, and MG (the refusal narrowed below 65532).
//! - T5 `an_oversize_sql_insert_changes_no_page_of_the_table`: the same through SQL. Kills MA, MF.
//! - T6 `an_oversize_sql_update_writes_nothing_to_the_time_travel_heap`: the time-travel sibling.
//!   Kills MA, MD (the executor's check removed), MF.
//! - T7 `an_insert_refused_inside_its_page_releases_the_pin`: forces `insert_into`'s own error path
//!   with a directory entry that overstates a full page. Kills ME (the hand-written unpin back).

use std::fs::OpenOptions;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::storage::heap_page::{HEADER_SIZE, MAX_TUPLE_SIZE};
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::BPlusTreePage;
use ferrodb::storage::page_directory::PageDirectory;
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

// ---------------------------------------------------------------------------------------------
// Instruments
// ---------------------------------------------------------------------------------------------

/// A heap with no catalog, on its own file.
fn heap_fixture() -> (HeapFileManager, Arc<BufferPoolManager>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("heap.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let heap = HeapFileManager::new(bp.clone()).unwrap();
    (heap, bp, dir)
}

/// Every frame holding a pin, as `(page, pins)`, sorted. Read off the frames, not asked of the pool.
fn pinned(bp: &BufferPoolManager) -> Vec<(Option<u32>, u16)> {
    let mut out: Vec<(Option<u32>, u16)> = bp
        .frames
        .iter()
        .map(|f| {
            let f = f.read().unwrap();
            (f.page_id, f.pin_counter.load(Ordering::Relaxed))
        })
        .filter(|&(_, n)| n > 0)
        .collect();
    out.sort();
    out
}

/// Pins held on `page`. A page no frame holds has none.
fn pins(bp: &BufferPoolManager, page: u32) -> u16 {
    pinned(bp).iter().filter(|(p, _)| *p == Some(page)).map(|&(_, n)| n).sum()
}

/// Where the allocator says the file ends: past every page it has handed out.
fn high_water(bp: &BufferPoolManager) -> u32 {
    bp.disk_manager.high_water().unwrap()
}

/// The directory chain from `first_dir`, one entry per page: `(data page, free space it claims)`,
/// then `(directory page, u16::MAX)`. Read through the pool, so what a statement dirtied is seen.
fn directory(bp: &BufferPoolManager, first_dir: u32) -> Vec<(u32, u16)> {
    let mut out = Vec::new();
    let mut dir_id = first_dir;
    while dir_id != 0 {
        let frame_i = bp.fetch_page(dir_id).unwrap();
        let dir = PageDirectory::deserialize(bp.frames[frame_i].read().unwrap().data);
        bp.unpin_page(dir_id, false);
        out.extend(dir.entries.iter().map(|e| (e.page_id, e.free_space)));
        out.push((dir_id, u16::MAX));
        dir_id = dir.next_page_directory;
    }
    out
}

/// Every page a heap owns: each data page its directory lists, then each directory page.
fn heap_pages(bp: &BufferPoolManager, first_dir: u32) -> Vec<u32> {
    directory(bp, first_dir).into_iter().map(|(p, _)| p).collect()
}

/// Every page of a primary tree.
fn tree_pages(bp: &Arc<BufferPoolManager>, root: u32) -> Vec<u32> {
    let tree = BPlusTreeManager::<Value, RecordId>::open(root, bp.clone());
    let mut out = Vec::new();
    let mut stack = vec![root];
    while let Some(page) = stack.pop() {
        out.push(page);
        if let BPlusTreePage::Internal(node) = tree.read_node(page).unwrap() {
            stack.extend(node.child_ptrs.iter().copied());
        }
    }
    out.sort();
    out
}

/// Does a refusal name the limit and the length it refused? Both, so a message that says only
/// "too large" does not pass: the caller cannot act on a limit it is not told.
fn names_the_limit(msg: &str, len: usize) -> bool {
    msg.contains(&MAX_TUPLE_SIZE.to_string()) && msg.contains(&len.to_string())
}

/// The refusal itself: a `Constraint` that names the limit and the length. An `Internal` naming
/// both is NOT it: that reports the statement's fault as a fault in the server.
fn is_the_refusal(e: &FerroError, len: usize) -> bool {
    matches!(e, FerroError::Constraint(_)) && names_the_limit(&e.to_string(), len)
}

// ---------------------------------------------------------------------------------------------
// The SQL fixture (d237's)
// ---------------------------------------------------------------------------------------------

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
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
            .open(dir.path().join("d257.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d257.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _dir: dir }
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{}`: {:?}", &sql[..sql.len().min(80)], p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("`{}` failed: {e}", &sql[..sql.len().min(80)]))
    }

    fn rows(&mut self, sql: &str, s: &mut Session) -> Vec<Vec<Value>> {
        match self.ok(sql, s) {
            Outcome::Rows(rows) => rows,
            _ => panic!("`{sql}` did not return rows"),
        }
    }

    /// `(heap pages, time-travel pages, primary tree pages)` of `name`, each sorted.
    fn table_pages(&self, name: &str) -> (Vec<u32>, Vec<u32>, Vec<u32>) {
        let e = self.catalog.get_table(name).expect("table exists");
        let mut heap = heap_pages(&self.bp, e.first_directory_page_id);
        let mut tt = heap_pages(&self.bp, e.time_travel_root);
        heap.sort();
        tt.sort();
        (heap, tt, tree_pages(&self.bp, e.primary_index_root))
    }

    fn heap_tuples(&self, name: &str) -> usize {
        let e = self.catalog.get_table(name).expect("table exists");
        HeapFileManager::open(e.first_directory_page_id, self.bp.clone())
            .scan()
            .map(|r| r.unwrap_or_else(|e| panic!("scanning {name}'s heap failed: {e}")))
            .count()
    }
}

// ---------------------------------------------------------------------------------------------
// The heap
// ---------------------------------------------------------------------------------------------

#[test]
fn an_oversize_insert_is_refused_before_any_page_is_allocated() {
    let (heap, bp, _dir) = heap_fixture();
    heap.insert(Tuple::new(vec![1u8; 16])).unwrap();
    let pages = heap_pages(&bp, heap.first_directory_page_id);
    let hw = high_water(&bp);
    let pinned_before = pinned(&bp);

    let len = MAX_TUPLE_SIZE + 1;
    let err = heap
        .insert(Tuple::new(vec![2u8; len]))
        .expect_err("a tuple one byte past what a page holds was accepted");

    // The durable harm first, so a regression fails naming it rather than the wording.
    let after = heap_pages(&bp, heap.first_directory_page_id);
    assert_eq!(
        after, pages,
        "a refused {len}-byte insert (`{err}`) changed the heap's pages: {} before, {} after",
        pages.len(),
        after.len()
    );
    assert_eq!(high_water(&bp), hw, "a refused {len}-byte insert (`{err}`) moved the allocator's high-water mark");
    assert_eq!(pinned(&bp), pinned_before, "a refused {len}-byte insert (`{err}`) left a page pinned");
    assert!(
        names_the_limit(&err.to_string(), len),
        "the refusal must name the limit ({MAX_TUPLE_SIZE}) and the length ({len}): `{err}`"
    );
}

#[test]
fn a_tuple_of_exactly_the_limit_is_still_accepted() {
    let (heap, bp, _dir) = heap_fixture();
    heap.insert(Tuple::new(vec![1u8; 16])).unwrap();
    let pinned_before = pinned(&bp);

    let bytes = vec![9u8; MAX_TUPLE_SIZE];
    let rid = heap
        .insert(Tuple::new(bytes.clone()))
        .unwrap_or_else(|e| panic!("a tuple of exactly MAX_TUPLE_SIZE ({MAX_TUPLE_SIZE}) bytes was refused: {e}"));
    assert!(heap.read(rid).unwrap().data == bytes, "the {MAX_TUPLE_SIZE}-byte tuple did not read back as written");
    heap.find_or_make_page(MAX_TUPLE_SIZE)
        .unwrap_or_else(|e| panic!("find_or_make_page({MAX_TUPLE_SIZE}) refused the largest tuple a page holds: {e}"));
    assert_eq!(pinned(&bp), pinned_before, "accepting a {MAX_TUPLE_SIZE}-byte tuple left a page pinned");
}

#[test]
fn two_thousand_refused_inserts_leave_the_pool_and_the_file_as_they_were() {
    const ATTEMPTS: usize = 2000;
    let (heap, bp, _dir) = heap_fixture();
    heap.insert(Tuple::new(vec![1u8; 16])).unwrap();
    let pages = heap_pages(&bp, heap.first_directory_page_id);
    let hw = high_water(&bp);
    let pinned_before = pinned(&bp);

    let mut accepted: Vec<RecordId> = Vec::new();
    let mut named = 0usize;
    let mut unnamed: Option<String> = None;
    for i in 0..ATTEMPTS {
        let len = MAX_TUPLE_SIZE + 1 + i % 7;
        match heap.insert(Tuple::new(vec![3u8; len])) {
            Ok(rid) => accepted.push(rid),
            Err(e) if names_the_limit(&e.to_string(), len) => named += 1,
            Err(e) => {
                unnamed.get_or_insert_with(|| format!("{len} bytes: `{e}`"));
            }
        }
    }

    assert!(accepted.is_empty(), "{} of {ATTEMPTS} over-page tuples were accepted, at {accepted:?}", accepted.len());
    let after = heap_pages(&bp, heap.first_directory_page_id);
    let hw_after = high_water(&bp);
    assert!(
        after == pages && hw_after == hw,
        "{ATTEMPTS} refused inserts changed the file: the heap lists {} pages, {} before; the high-water \
         mark is {hw_after}, {hw} before; pinned frames now {:?}, before {pinned_before:?}",
        after.len(),
        pages.len(),
        pinned(&bp)
    );
    assert_eq!(pinned(&bp), pinned_before, "{ATTEMPTS} refused inserts left pages pinned");
    assert_eq!(named, ATTEMPTS, "{} refusal(s) did not name the limit; the first: {unnamed:?}", ATTEMPTS - named);

    let rid = heap
        .insert(Tuple::new(vec![4u8; 32]))
        .unwrap_or_else(|e| panic!("after {ATTEMPTS} refusals an ordinary insert failed: {e}"));
    assert!(heap.read(rid).unwrap().data == vec![4u8; 32], "the ordinary insert did not read back");
}

#[test]
fn the_space_arithmetic_refuses_rather_than_wrapping_at_the_u16_boundary() {
    // 4070: one past the limit. 65531: `+ 4` reaches u16::MAX exactly. 65532 and 65535: `+ 4`
    // overflows a u16. 65536, 65540, 131082: `as u16` truncates to 0, 4 and 10.
    const LENGTHS: [usize; 7] = [MAX_TUPLE_SIZE + 1, 65531, 65532, 65535, 65536, 65540, 131_082];
    let (heap, bp, _dir) = heap_fixture();
    // A page with nearly all of its space free, which a wrapped `needed` would be answered by.
    heap.insert(Tuple::new(vec![1u8; 16])).unwrap();
    let pages = heap_pages(&bp, heap.first_directory_page_id);
    let hw = high_water(&bp);

    let mut wrong: Vec<(usize, String)> = Vec::new();
    for len in LENGTHS {
        match catch_unwind(AssertUnwindSafe(|| heap.find_or_make_page(len))) {
            Err(_) => wrong.push((len, "find_or_make_page PANICKED".into())),
            Ok(Ok(page)) => wrong.push((len, format!("find_or_make_page returned page {page}"))),
            Ok(Err(e)) if is_the_refusal(&e, len) => {}
            Ok(Err(e)) => wrong.push((len, format!("find_or_make_page refused, but not with the refusal: {e:?}"))),
        }
    }
    let len = 65536;
    match heap.insert(Tuple::new(vec![5u8; len])) {
        Ok(rid) => wrong.push((len, format!("insert accepted it at {rid:?}"))),
        Err(e) if is_the_refusal(&e, len) => {}
        Err(e) => wrong.push((len, format!("insert refused, but not with the refusal: {e:?}"))),
    }
    assert!(
        wrong.is_empty(),
        "every length past {MAX_TUPLE_SIZE} must be refused with a Constraint naming the limit: {wrong:#?}"
    );

    let after = heap_pages(&bp, heap.first_directory_page_id);
    assert_eq!(after, pages, "asking for space past the limit changed the heap's pages");
    assert_eq!(high_water(&bp), hw, "asking for space past the limit moved the allocator's high-water mark");

    heap.find_or_make_page(MAX_TUPLE_SIZE)
        .unwrap_or_else(|e| panic!("find_or_make_page({MAX_TUPLE_SIZE}) refused the largest tuple a page holds: {e}"));
}

#[test]
fn an_insert_refused_inside_its_page_releases_the_pin() {
    let (heap, bp, _dir) = heap_fixture();
    // 2004 + 2069 = 4073 = PAGE_SIZE - HEADER_SIZE: one page, exactly full. Both are under the
    // limit, so an off-by-one in the refusal cannot break the fixture.
    let a = heap.insert(Tuple::new(vec![5u8; 2000])).unwrap();
    let b = heap.insert(Tuple::new(vec![6u8; 2065])).unwrap();
    assert_eq!(a.page_id, b.page_id, "premise: the two tuples share one page");
    let p = a.page_id;
    let claimed = directory(&bp, heap.first_directory_page_id).into_iter().find(|&(q, _)| q == p).map(|(_, f)| f);
    assert_eq!(claimed, Some(0), "premise: page {p} is exactly full");

    // The directory overstates it, as a stale entry would. The insert goes to `p` and fails there.
    heap.update_directory_entry(p, (PAGE_SIZE - HEADER_SIZE) as u16).unwrap();
    let hw = high_water(&bp);
    let pinned_before = pinned(&bp);
    let err = heap
        .insert(Tuple::new(vec![7u8; 100]))
        .expect_err("premise: a 100-byte insert into a full page succeeded");
    assert!(
        matches!(err, FerroError::NotEnoughSpace),
        "premise: the refusal must come from inside the page (NotEnoughSpace), not `{err}`"
    );
    assert_eq!(high_water(&bp), hw, "premise: the allocator was involved, so the page's own refusal was not reached");

    assert_eq!(pins(&bp, p), 0, "an insert refused inside page {p} (`{err}`) left it pinned");
    assert_eq!(pinned(&bp), pinned_before, "an insert refused inside page {p} (`{err}`) left a page pinned");
}

// ---------------------------------------------------------------------------------------------
// Through SQL
// ---------------------------------------------------------------------------------------------

#[test]
fn an_oversize_sql_insert_changes_no_page_of_the_table() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(5000));", &mut s);
    db.ok("INSERT INTO t VALUES (1, 'small');", &mut s);
    let pages = db.table_pages("t");
    let hw = high_water(&db.bp);
    let pinned_before = pinned(&db.bp);

    let err = match db.exec(&format!("INSERT INTO t VALUES (2, '{}');", "x".repeat(4100)), &mut s) {
        Ok(_) => panic!("an INSERT of a row no page can hold succeeded"),
        Err(e) => e,
    };

    let after = db.table_pages("t");
    assert_eq!(after, pages, "a refused INSERT (`{err}`) changed t's pages (heap, time travel, primary)");
    assert_eq!(high_water(&db.bp), hw, "a refused INSERT (`{err}`) moved the allocator's high-water mark");
    assert_eq!(pinned(&db.bp), pinned_before, "a refused INSERT (`{err}`) left a page pinned");
    assert!(
        matches!(err, FerroError::Constraint(_)),
        "a row too wide is the statement's fault, so it must be refused as a Constraint (this \
         build's class for every row-width refusal, SQLSTATE 23000), not as a server fault \
         (XX000): `{err}`"
    );
    assert!(err.to_string().contains(&MAX_TUPLE_SIZE.to_string()), "the refusal must name the limit ({MAX_TUPLE_SIZE}): `{err}`");
    assert_eq!(db.heap_tuples("t"), 1, "the refused row reached the heap");

    db.ok("INSERT INTO t VALUES (3, 'ok');", &mut s);
    db.ok("DROP TABLE t;", &mut s);
}

#[test]
fn an_oversize_sql_update_writes_nothing_to_the_time_travel_heap() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, a VARCHAR(5000));", &mut s);
    db.ok("INSERT INTO t VALUES (1, 'small');", &mut s);
    let (heap, tt, primary) = db.table_pages("t");
    let hw = high_water(&db.bp);
    let pinned_before = pinned(&db.bp);

    let err = match db.exec(&format!("UPDATE t SET a = '{}' WHERE id = 1;", "x".repeat(4100)), &mut s) {
        Ok(_) => panic!("an UPDATE to a row no page can hold succeeded"),
        Err(e) => e,
    };

    let (heap_after, tt_after, primary_after) = db.table_pages("t");
    assert_eq!(
        tt_after, tt,
        "a refused UPDATE (`{err}`) changed the time-travel heap's pages: it wrote the old version \
         there before the new one was refused"
    );
    assert_eq!((heap_after, primary_after), (heap, primary), "a refused UPDATE (`{err}`) changed t's heap or primary tree");
    assert_eq!(high_water(&db.bp), hw, "a refused UPDATE (`{err}`) moved the allocator's high-water mark");
    assert_eq!(pinned(&db.bp), pinned_before, "a refused UPDATE (`{err}`) left a page pinned");
    assert!(
        matches!(err, FerroError::Constraint(_)),
        "a row too wide is the statement's fault, so it must be refused as a Constraint (this \
         build's class for every row-width refusal), not as a server fault: `{err}`"
    );
    assert!(err.to_string().contains(&MAX_TUPLE_SIZE.to_string()), "the refusal must name the limit ({MAX_TUPLE_SIZE}): `{err}`");
    assert_eq!(
        db.rows("SELECT a FROM t WHERE id = 1;", &mut s),
        vec![vec![Value::Varchar("small".into())]],
        "the refused UPDATE changed row 1"
    );

    // Control: an UPDATE that fits is not refused by the new check.
    db.ok("UPDATE t SET a = 'fits' WHERE id = 1;", &mut s);
    assert_eq!(db.rows("SELECT a FROM t WHERE id = 1;", &mut s), vec![vec![Value::Varchar("fits".into())]]);
}
