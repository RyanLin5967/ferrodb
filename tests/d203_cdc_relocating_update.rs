//! D203 — an UPDATE whose row no longer fits its page must reach the change feed as ONE `UPDATE`.
//!
//! `HeapFileManager::update` updates in place when it can. When the new tuple does not fit the page,
//! its relocation arm frees the slot and inserts the row on another page. It logs that as a
//! `HeapDelete` of the live row followed by a `HeapInsert` of the new one, not as a `HeapUpdate`.
//! The logical decoder maps `HeapDelete` to `DELETE` and `HeapInsert` to `INSERT` record by record,
//! so a consumer is told the row was deleted and a new one inserted. The final state at a sink that
//! orders by LSN comes out right; the event kind does not, and anything that acts on each event
//! (counts updates, audits deletions, fires a trigger) is wrong.
//!
//! Postgres logs a non-HOT update whose new version lands on another page as ONE `XLOG_HEAP_UPDATE`
//! carrying both tuple locations, and logical decoding turns it into one `UPDATE` (RECALLED, not
//! checked here). Physical placement is not a logical change, and the feed must not show it.
//!
//! Every test asserts its own premise: the row really moved (or really stayed put), read from the
//! primary index, which is repointed exactly when the row relocates. Pre-registered in
//! `bench/d203_cdc_relocating_update/prereg.md`.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::logical::{ChangeOp, Decoded, LogicalDecoder};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("reloc.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("reloc.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: dir, catalog, wal, bp, txn, session: Session::new() }
}

impl Db {
    fn sql(&mut self, sql: &str) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{}`: {:?}", short(sql), p.errors);
        if let Err(e) = run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session) {
            panic!("`{}` failed: {e}", short(sql));
        }
    }

    /// Where the primary index says key `id` of `docs` lives now.
    fn rid_of(&self, id: i32) -> RecordId {
        let root = self.catalog.get_table("docs").expect("table docs").primary_index_root;
        BPlusTreeManager::<Value, RecordId>::open(root, self.bp.clone())
            .search(&Value::Integer(id))
            .expect("index search")
            .unwrap_or_else(|| panic!("key {id} is not in the primary index"))
    }

    fn decode(&self) -> Decoded {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        let decoder = LogicalDecoder::new(&self.catalog);
        assert!(decoder.known_tables() > 0, "the decoder resolved no tables");
        let out = decoder
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode");
        assert!(out.is_complete(), "the feed is incomplete, so no claim about it is sound: {out:?}");
        out
    }
}

/// A long SQL string, shortened for a message.
fn short(sql: &str) -> String {
    if sql.len() <= 80 { sql.to_string() } else { format!("{}…", &sql[..80]) }
}

/// Every event for key `id` of `docs`, in feed order: its kind and its row images.
fn history(out: &Decoded, id: i32) -> Vec<(&'static str, ChangeOp)> {
    out.events
        .iter()
        .filter(|e| e.table == "docs")
        .filter(|e| {
            // A DELETE describes the row that went away, so its key is in the BEFORE image.
            let row = match &e.op {
                ChangeOp::Insert { new } | ChangeOp::Update { new, .. } => Some(new),
                ChangeOp::Delete { old } => Some(old),
                _ => None,
            };
            matches!(row.and_then(|r| r.first()), Some(Value::Integer(k)) if *k == id)
        })
        .map(|e| (e.op.name(), e.op.clone()))
        .collect()
}

fn kinds(h: &[(&'static str, ChangeOp)]) -> Vec<&'static str> {
    h.iter().map(|(k, _)| *k).collect()
}

/// A page nearly full: row 1 small, rows 2 and 3 about 1.9 KB each, all three on one 4 KB page.
/// Growing row 1 to 600 bytes then cannot fit the page's remaining free span (about 150 bytes), so
/// `HeapFileManager::update` takes its relocation arm. Sizes are from `Tuple::serialize`'s layout:
/// 24-byte version header, null bitmap, then each column aligned (READ-FROM-SOURCE).
fn nearly_full_page(d: &mut Db) {
    d.sql("CREATE TABLE docs (id INTEGER NOT NULL, body VARCHAR(2000));");
    d.sql("INSERT INTO docs VALUES (1, 'short');");
    d.sql(&format!("INSERT INTO docs VALUES (2, '{}');", "y".repeat(1900)));
    d.sql(&format!("INSERT INTO docs VALUES (3, '{}');", "z".repeat(1900)));
}

/// **The red test.** At `9aa6968` key 1 reads `INSERT, DELETE, INSERT`: the relocation surfaces as a
/// deletion and a fresh row. It must read `INSERT, UPDATE`, with the UPDATE carrying the row before
/// and after, exactly as an in-place update does.
#[test]
fn a_relocating_update_is_one_update_event() {
    let mut d = db();
    nearly_full_page(&mut d);
    let before = d.rid_of(1);
    let body = "w".repeat(600);
    d.sql(&format!("UPDATE docs SET body = '{body}' WHERE id = 1;"));
    let after = d.rid_of(1);
    assert_ne!(
        before, after,
        "fixture: the UPDATE did not relocate row 1, so this test is not about relocation"
    );

    let out = d.decode();
    let h = history(&out, 1);
    assert_eq!(
        kinds(&h),
        vec!["INSERT", "UPDATE"],
        "a relocating UPDATE reached the feed as something other than one UPDATE"
    );
    assert_eq!(
        h[1].1,
        ChangeOp::Update {
            old: vec![Value::Integer(1), Value::Varchar("short".into())],
            new: vec![Value::Integer(1), Value::Varchar(body)],
        },
        "the UPDATE does not carry the row before and after"
    );
    // The rows it did not touch are untouched in the feed too.
    assert_eq!(kinds(&history(&out, 2)), vec!["INSERT"]);
    assert_eq!(kinds(&history(&out, 3)), vec!["INSERT"]);
}

/// **Control: an UPDATE that fits in place.** Same page, same fixture, but the new body is no longer
/// than the old, so the row stays in its slot and the log holds one `HeapUpdate`. One UPDATE at
/// `9aa6968` and after the pairing rule alike; if this ever moves, the rule reached a record it was
/// not about.
#[test]
fn an_update_that_fits_in_place_is_one_update_event_and_stays_put() {
    let mut d = db();
    nearly_full_page(&mut d);
    let before = d.rid_of(2);
    d.sql(&format!("UPDATE docs SET body = '{}' WHERE id = 2;", "Y".repeat(1900)));
    assert_eq!(before, d.rid_of(2), "fixture: the same-size UPDATE moved row 2");

    let out = d.decode();
    assert_eq!(kinds(&history(&out, 2)), vec!["INSERT", "UPDATE"]);
}

/// **Control: a DELETE and an INSERT adjacent in one transaction stay two events.** A SQL DELETE is a
/// `HeapUpdate` that stamps `end_ts`, never a `HeapDelete`, so it can never be the first half of a
/// relocation. Re-inserting the same key right after it (E63's reuse) must stay `DELETE, INSERT`, and
/// so must an INSERT of an unrelated key. A pairing rule keyed on "a delete, then an insert of the
/// same key" instead of on the relocation's own record would turn the first into an UPDATE.
#[test]
fn a_delete_and_an_insert_adjacent_in_one_transaction_stay_two_events() {
    let mut d = db();
    nearly_full_page(&mut d);
    d.sql("BEGIN;");
    d.sql("DELETE FROM docs WHERE id = 1;");
    d.sql("INSERT INTO docs VALUES (1, 'again');");
    d.sql("INSERT INTO docs VALUES (9, 'nine');");
    d.sql("COMMIT;");

    let out = d.decode();
    assert_eq!(
        kinds(&history(&out, 1)),
        vec!["INSERT", "DELETE", "INSERT"],
        "a same-transaction delete and reinsert of one key was merged into something else"
    );
    assert_eq!(kinds(&history(&out, 9)), vec!["INSERT"]);
}
