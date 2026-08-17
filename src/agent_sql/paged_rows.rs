//! Row storage on copy-on-write pages.
//!
//! This is the piece that was missing between the SQL surface and the branch engine. Exit
//! criteria 1 and 8 are statements about *data pages* — "forking copies zero of them", "an
//! abandoned branch returns them" — but agent-session rows lived in a `BTreeMap` inside
//! `Workspace`, so those criteria could only ever be demonstrated against the branch engine as a
//! component, never through the shipped API. Measuring "zero pages copied" while nothing writes
//! pages at all is a vacuous zero, and must not be claimed as the criterion.
//!
//! [`PagedRows`] puts rows on the same [`CowTree`] the branch engine already shadows, so a
//! branch's rows are reachable from its own root page and nothing else. Isolation is then a
//! property of the page graph rather than of a map that happens not to be shared.
//!
//! **Key layout** is `table_id` (4 bytes big-endian) then `row_id` (8 bytes big-endian). Both are
//! big-endian so that the tree's `Ord`-on-bytes agrees with numeric order: that is what makes one
//! table's rows a single contiguous range, and therefore what makes [`PagedRows::scan_table`] a
//! range scan instead of a full walk with a filter.
//!
//! **Value layout** reuses the existing [`BTreeSerialize`] encoding rather than inventing a
//! second one, so there is exactly one definition of how a `Value` is written to a page.

use std::sync::Arc;

use crate::catalog::column::Value;
use crate::cow::CowTree;
use crate::cow::PageStore;
use crate::error::FerroError;
use crate::branch::types::{BranchId, Epoch, PageId};
use crate::storage::index_page::BTreeSerialize;


/// Bytes in an encoded key: 4 for the table, 8 for the row.
pub const ROW_KEY_LEN: usize = 12;

/// `table_id` then `row_id`, both big-endian — see the module note on why the order matters.
pub fn row_key(table_id: u32, row_id: u64) -> [u8; ROW_KEY_LEN] {
    let mut k = [0u8; ROW_KEY_LEN];
    k[0..4].copy_from_slice(&table_id.to_be_bytes());
    k[4..12].copy_from_slice(&row_id.to_be_bytes());
    k
}

/// Lower bound for one table's range: `(table_id, 0)`.
pub fn table_lo(table_id: u32) -> [u8; ROW_KEY_LEN] {
    row_key(table_id, 0)
}

/// Upper bound (exclusive) for one table's range: `(table_id + 1, 0)`.
///
/// Returns a `Vec` rather than a fixed array because the last table id has no successor and no
/// 12-byte key can serve as its exclusive bound: `[0xFF; 12]` is exactly `row_key(u32::MAX,
/// u64::MAX)`, so using it drops that row from its own table's scan. A 13-byte key with the same
/// leading bytes sorts above every 12-byte key (equal prefix, greater length), which is a bound
/// no real key can reach. Wrapping to zero instead would make the final table scan empty and
/// report a populated table as having no rows.
pub fn table_hi(table_id: u32) -> Vec<u8> {
    match table_id.checked_add(1) {
        Some(next) => row_key(next, 0).to_vec(),
        None => vec![0xFF; ROW_KEY_LEN + 1],
    }
}

/// Split an encoded key back into its parts.
pub fn split_row_key(key: &[u8]) -> Result<(u32, u64), FerroError> {
    if key.len() != ROW_KEY_LEN {
        return Err(FerroError::Corruption(format!(
            "row key must be {ROW_KEY_LEN} bytes, got {}",
            key.len()
        )));
    }
    let table = u32::from_be_bytes(key[0..4].try_into().unwrap());
    let row = u64::from_be_bytes(key[4..12].try_into().unwrap());
    Ok((table, row))
}

/// How many bytes the value starting at `bytes[0]` occupies, or an error if it is truncated.
///
/// `Value::deserialize` indexes its input unchecked and **panics** on a short slice. A page can be
/// short for reasons that are not bugs in this file — a torn write, a corrupt image, a key from an
/// older layout — and none of those should abort the process. So the span is validated here first
/// and the existing decoder is only ever handed a slice it can read.
fn value_span(bytes: &[u8]) -> Result<usize, FerroError> {
    let truncated =
        || FerroError::Corruption("row value ends mid-cell; the encoded page is truncated".into());
    let tag = *bytes.first().ok_or_else(truncated)?;
    let span = match tag {
        0 => 5,                    // Integer:   tag + i32
        2 => 9,                    // Float:     tag + f64
        3 => 2,                    // Boolean:   tag + u8
        4 => 1,                    // Null:      tag
        5 => 9,                    // BigInt:    tag + i64
        7 => 9,                    // Timestamp: tag + i64
        // Varchar (1) and Decimal (6) share the tag + u16 length + payload layout.
        //
        // These MUST be kept in step with `Value::serialize` in `storage::index_page`. When the
        // wide types were added there, this function was not updated, and the result was worse
        // than a decode error: `encode_row` succeeded, so the write landed, and every later
        // `decode_row` of that row failed with "unknown value tag" — a BIGINT/DECIMAL/TIMESTAMP
        // cell was write-only on a page-backed agent branch, which `AgentRuntime::page_changeset`
        // hits for the before and after image of every changed row. A tag this function does not
        // know is not a cell it can skip, so there is no safe fallback: it has to be exhaustive.
        //
        // `wide_typed_cells_survive_a_round_trip_through_a_branchs_pages` in
        // `tests/integration_branch_pages.rs` drives that path from SQL, so the reachability this
        // paragraph asserts is checked rather than only claimed.
        1 | 6 => {
            if bytes.len() < 3 {
                return Err(truncated());
            }
            3 + u16::from_be_bytes(bytes[1..3].try_into().unwrap()) as usize
        }
        other => {
            return Err(FerroError::Corruption(format!(
                "unknown value tag {other} in an encoded row"
            )))
        }
    };
    if bytes.len() < span {
        return Err(truncated());
    }
    Ok(span)
}

/// Encode a row as `count` (u16 big-endian) followed by each cell.
pub fn encode_row(vals: &[Value]) -> Result<Vec<u8>, FerroError> {
    let count = u16::try_from(vals.len()).map_err(|_| {
        FerroError::Internal(format!("a row of {} cells exceeds the u16 count", vals.len()))
    })?;
    let mut buf = Vec::with_capacity(2 + vals.len() * 5);
    buf.extend_from_slice(&count.to_be_bytes());
    for v in vals {
        v.serialize(&mut buf);
    }
    Ok(buf)
}

/// Decode a row written by [`encode_row`].
///
/// Refuses a short buffer, an unknown tag, and trailing bytes. Trailing bytes matter: they mean
/// the reader and the writer disagree about the layout, and continuing would hand back a row that
/// silently lost cells.
pub fn decode_row(bytes: &[u8]) -> Result<Vec<Value>, FerroError> {
    if bytes.len() < 2 {
        return Err(FerroError::Corruption("encoded row is missing its cell count".into()));
    }
    let count = u16::from_be_bytes(bytes[0..2].try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(count);
    let mut at = 2;
    for i in 0..count {
        let rest = &bytes[at..];
        let span = value_span(rest).map_err(|e| {
            FerroError::Corruption(format!("cell {i} of {count}: {e}"))
        })?;
        let (v, used) = Value::deserialize(rest)?;
        debug_assert_eq!(used, span, "value_span disagrees with Value::deserialize");
        out.push(v);
        at += span;
    }
    if at != bytes.len() {
        return Err(FerroError::Corruption(format!(
            "encoded row has {} trailing byte(s) after {count} cells; reader and writer disagree \
             about the layout",
            bytes.len() - at
        )));
    }
    Ok(out)
}

/// One row's change between two roots, decoded.
#[derive(Debug, Clone, PartialEq)]
pub struct PageRowChange {
    pub table: u32,
    pub row: u64,
    /// `None` means the row did not exist at the fork point.
    pub before: Option<Vec<Value>>,
    /// `None` means the row was deleted on the branch.
    pub after: Option<Vec<Value>>,
}

/// Rows on copy-on-write pages, one tree per branch root.
///
/// Every mutating call returns the branch's **new root page id**, because a copy-on-write write
/// does not update in place — the caller must store it or the write is invisible. That is why the
/// root is a return value and not an interior field.
pub struct PagedRows {
    tree: CowTree,
}

impl PagedRows {
    pub fn new(store: Arc<dyn PageStore>) -> Self {
        PagedRows { tree: CowTree::new(store) }
    }

    /// The underlying tree, for callers that need page-level accounting (criterion 1 measures it).
    pub fn tree(&self) -> &CowTree {
        &self.tree
    }

    /// Create an empty root page for `branch`.
    pub fn create_root(&self, branch: BranchId, epoch: Epoch) -> Result<PageId, FerroError> {
        self.tree.create(branch, epoch)
    }

    /// Insert or replace one row. Returns the new root.
    pub fn put(
        &self,
        root: PageId,
        branch: BranchId,
        epoch: Epoch,
        table_id: u32,
        row_id: u64,
        vals: &[Value],
    ) -> Result<PageId, FerroError> {
        let key = row_key(table_id, row_id);
        let val = encode_row(vals)?;
        self.tree.insert(root, branch, epoch, &key, &val)
    }

    /// Read one row. `None` means absent, which is distinct from a row whose cells are all `Null`.
    pub fn get(
        &self,
        root: PageId,
        table_id: u32,
        row_id: u64,
    ) -> Result<Option<Vec<Value>>, FerroError> {
        let key = row_key(table_id, row_id);
        match self.tree.get(root, &key)? {
            Some(bytes) => Ok(Some(decode_row(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Remove one row. Returns the new root, unchanged when the row was absent.
    pub fn delete(
        &self,
        root: PageId,
        branch: BranchId,
        epoch: Epoch,
        table_id: u32,
        row_id: u64,
    ) -> Result<PageId, FerroError> {
        let key = row_key(table_id, row_id);
        self.tree.delete(root, branch, epoch, &key)
    }

    /// Every row of one table, in `row_id` order.
    pub fn scan_table(
        &self,
        root: PageId,
        table_id: u32,
    ) -> Result<Vec<(u64, Vec<Value>)>, FerroError> {
        let lo = table_lo(table_id);
        let hi = table_hi(table_id);
        let entries = self.tree.range_scan(root, Some(&lo), Some(&hi))?;
        let mut out = Vec::with_capacity(entries.len());
        for (k, v) in entries {
            let (t, row) = split_row_key(&k)?;
            // A key outside the requested table means the range bounds are wrong, which would
            // otherwise show up as one table quietly reading another's rows.
            if t != table_id {
                return Err(FerroError::Internal(format!(
                    "range scan for table {table_id} returned a key from table {t}"
                )));
            }
            out.push((row, decode_row(&v)?));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::arena::ArenaPageStore;
    use crate::branch::catalog::LogBranchCatalog;
    use crate::branch::BranchCatalog;
    use crate::buffer::buffer_pool::BufferPoolManager;
    use crate::storage::disk_manager::DiskManager;

    const ARENA_BASE: u32 = 1024;

    fn store() -> (tempfile::TempDir, Arc<LogBranchCatalog>, Arc<ArenaPageStore>) {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("paged_rows.db"))
            .unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let pool = Arc::new(BufferPoolManager::new(Arc::clone(&dm)));
        let catalog = Arc::new(LogBranchCatalog::in_memory(1));
        let store =
            Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), ARENA_BASE).unwrap());
        (dir, catalog, store)
    }

    fn rows() -> (tempfile::TempDir, Arc<LogBranchCatalog>, PagedRows) {
        let (dir, catalog, store) = store();
        let pr = PagedRows::new(store as Arc<dyn PageStore>);
        (dir, catalog, pr)
    }

    // ---- codec ---------------------------------------------------------------------------

    /// The name is a promise: **every** variant, not the ones that existed when it was written.
    ///
    /// It previously listed only the five original variants, and that omission was not cosmetic.
    /// `encode_row` delegates to `Value::serialize`, which learned the wide types' tags, while
    /// `value_span` here did not — so a `BIGINT`, `DECIMAL` or `TIMESTAMP` cell on a page-backed
    /// agent branch encoded cleanly and then failed **every** later read with "unknown value tag".
    /// A write-only cell is worse than a rejected write, because nothing reports it at write time.
    ///
    /// Adding a variant to `Value` without adding it here must fail this test, which is the whole
    /// point of enumerating them exhaustively rather than sampling.
    #[test]
    fn every_value_variant_round_trips() {
        let row = vec![
            Value::Integer(-42),
            Value::Float(1.5),
            Value::Varchar("widget".into()),
            Value::Boolean(true),
            Value::Null,
            Value::Varchar(String::new()),
            // The wide types, at the extremes that a narrower encoding would lose.
            Value::BigInt(i64::MAX),
            Value::BigInt(i64::MIN),
            Value::BigInt(9007199254740993),
            Value::Decimal("123456789012345678901234567890.12345678901234567890".into()),
            Value::Decimal("1.50".into()),
            Value::Timestamp(1_700_000_000_123),
            Value::Timestamp(i64::MIN),
        ];
        let bytes = encode_row(&row).unwrap();
        let back = decode_row(&bytes).unwrap();
        assert_eq!(back, row);
        // `Value`'s PartialEq is numeric, so `Decimal("1.50") == Decimal("1.5")`. Check the bytes
        // themselves too, or a decoder that dropped the trailing zero would pass the line above.
        assert!(
            matches!(&back[10], Value::Decimal(d) if d == "1.50"),
            "decimal scale was lost in the page encoding: {:?}",
            back[10]
        );
    }

    /// A cell of every variant must also survive **individually**, so a failure names the variant
    /// rather than pointing at one long row.
    #[test]
    fn each_wide_variant_round_trips_on_its_own() {
        for v in [
            Value::BigInt(i64::MAX),
            Value::BigInt(-1),
            Value::Decimal("-0.00000000000000000001".into()),
            Value::Decimal("0.0".into()),
            Value::Timestamp(-1),
            Value::Timestamp(0),
        ] {
            let bytes = encode_row(std::slice::from_ref(&v)).unwrap();
            let back = decode_row(&bytes)
                .unwrap_or_else(|e| panic!("{v:?} encoded but would not decode: {e}"));
            assert_eq!(back, vec![v.clone()], "{v:?} did not survive the page encoding");
        }
    }

    #[test]
    fn an_empty_row_round_trips() {
        let bytes = encode_row(&[]).unwrap();
        assert_eq!(decode_row(&bytes).unwrap(), Vec::<Value>::new());
    }

    /// The reason `value_span` exists. `Value::deserialize` indexes unchecked, so without the
    /// span check every one of these truncations is a panic rather than an error.
    #[test]
    fn a_truncated_row_is_refused_rather_than_panicking() {
        let full = encode_row(&[
            Value::Integer(7),
            Value::Varchar("hello".into()),
            Value::Float(2.5),
        ])
        .unwrap();
        // Every proper prefix must be refused, not panic and not silently decode.
        for cut in 0..full.len() {
            let err = decode_row(&full[..cut]);
            assert!(err.is_err(), "prefix of {cut} byte(s) decoded instead of being refused");
        }
        assert!(decode_row(&full).is_ok(), "the untruncated row must still decode");
    }

    #[test]
    fn an_unknown_tag_is_refused() {
        let mut bytes = encode_row(&[Value::Integer(1)]).unwrap();
        bytes[2] = 99; // the cell's tag
        let e = decode_row(&bytes).unwrap_err();
        assert!(format!("{e}").contains("unknown value tag"), "got {e}");
    }

    #[test]
    fn trailing_bytes_are_refused() {
        let mut bytes = encode_row(&[Value::Integer(1)]).unwrap();
        bytes.push(0);
        let e = decode_row(&bytes).unwrap_err();
        assert!(format!("{e}").contains("trailing"), "got {e}");
    }

    // ---- key layout ----------------------------------------------------------------------

    #[test]
    fn keys_sort_in_numeric_row_order() {
        // The whole point of big-endian: byte order must agree with numeric order, or a range
        // scan returns rows in an order that has nothing to do with row_id.
        let mut keys: Vec<[u8; ROW_KEY_LEN]> =
            [500u64, 2, 1, 300, 0, u64::MAX, 256].iter().map(|r| row_key(7, *r)).collect();
        keys.sort();
        let ids: Vec<u64> = keys.iter().map(|k| split_row_key(k).unwrap().1).collect();
        assert_eq!(ids, vec![0, 1, 2, 256, 300, 500, u64::MAX]);
    }

    #[test]
    fn a_table_range_excludes_its_neighbours() {
        let lo = table_lo(7);
        let hi = table_hi(7);
        assert!(row_key(6, u64::MAX)[..] < lo[..]);
        assert!(row_key(7, 0)[..] >= lo[..] && row_key(7, u64::MAX)[..] < hi[..]);
        assert!(row_key(8, 0)[..] >= hi[..]);
    }

    #[test]
    fn the_last_table_id_still_has_a_usable_upper_bound() {
        // u32::MAX + 1 wraps to 0, which would make the final table's range empty and report a
        // populated table as having no rows.
        let hi = table_hi(u32::MAX);
        assert!(row_key(u32::MAX, u64::MAX)[..] < hi[..], "the last table's rows fell outside its own range");
    }

    #[test]
    fn split_rejects_a_wrong_length_key() {
        assert!(split_row_key(&[0u8; 11]).is_err());
        assert!(split_row_key(&[0u8; 13]).is_err());
        assert!(split_row_key(&[0u8; 12]).is_ok());
    }

    // ---- storage -------------------------------------------------------------------------

    #[test]
    fn a_row_survives_a_round_trip_through_pages() {
        let (_d, cat, pr) = rows();
        let e = cat.next_epoch();
        let root = pr.create_root(BranchId::TRUNK, e).unwrap();
        let row = vec![Value::Integer(5), Value::Varchar("bolt".into())];
        let root = pr.put(root, BranchId::TRUNK, e, 1, 42, &row).unwrap();
        assert_eq!(pr.get(root, 1, 42).unwrap(), Some(row));
        assert_eq!(pr.get(root, 1, 43).unwrap(), None, "an absent row must read as None");
    }

    #[test]
    fn a_scan_returns_only_the_requested_table_in_row_order() {
        let (_d, cat, pr) = rows();
        let e = cat.next_epoch();
        let mut root = pr.create_root(BranchId::TRUNK, e).unwrap();
        for (t, r) in [(1u32, 3u64), (1, 1), (2, 9), (1, 2), (2, 1)] {
            root = pr
                .put(root, BranchId::TRUNK, e, t, r, &[Value::Integer(r as i32)])
                .unwrap();
        }
        let got: Vec<u64> = pr.scan_table(root, 1).unwrap().into_iter().map(|(r, _)| r).collect();
        assert_eq!(got, vec![1, 2, 3], "table 1 must yield its own rows, sorted");
        let got2: Vec<u64> = pr.scan_table(root, 2).unwrap().into_iter().map(|(r, _)| r).collect();
        assert_eq!(got2, vec![1, 9]);
        assert!(pr.scan_table(root, 3).unwrap().is_empty(), "an unused table must scan empty");
    }

    #[test]
    fn a_delete_removes_only_its_own_row() {
        let (_d, cat, pr) = rows();
        let e = cat.next_epoch();
        let mut root = pr.create_root(BranchId::TRUNK, e).unwrap();
        for r in 1..=3u64 {
            root = pr.put(root, BranchId::TRUNK, e, 1, r, &[Value::Integer(r as i32)]).unwrap();
        }
        root = pr.delete(root, BranchId::TRUNK, e, 1, 2).unwrap();
        let got: Vec<u64> = pr.scan_table(root, 1).unwrap().into_iter().map(|(r, _)| r).collect();
        assert_eq!(got, vec![1, 3]);
        assert_eq!(pr.get(root, 1, 2).unwrap(), None);
    }

    /// The ordering check above is about bytes; this is the claim that actually matters — a row
    /// in the very last table is reachable by a scan of that table. With a 12-byte saturating
    /// bound this returned empty, because the bound equalled the row's own key.
    #[test]
    fn the_last_table_scans_its_own_rows() {
        let (_d, cat, pr) = rows();
        let e = cat.next_epoch();
        let root = pr.create_root(BranchId::TRUNK, e).unwrap();
        let root = pr
            .put(root, BranchId::TRUNK, e, u32::MAX, u64::MAX, &[Value::Integer(1)])
            .unwrap();
        let got = pr.scan_table(root, u32::MAX).unwrap();
        assert_eq!(
            got,
            vec![(u64::MAX, vec![Value::Integer(1)])],
            "the last table's last row fell outside its own scan range"
        );
    }

    /// The property criterion 2 is really about, stated at the page layer: a write on one branch
    /// is invisible from the other branch's root, because the two roots reach different pages.
    #[test]
    fn a_write_under_one_root_is_invisible_from_another_root() {
        let (_d, cat, pr) = rows();
        let e0 = cat.next_epoch();
        let base = pr.create_root(BranchId::TRUNK, e0).unwrap();
        let base = pr.put(base, BranchId::TRUNK, e0, 1, 1, &[Value::Integer(100)]).unwrap();

        let child = cat.fork(BranchId::TRUNK, crate::branch::types::LeaseDeadline::from_now(60_000)).unwrap();
        let e1 = cat.next_epoch();
        // The child starts from the parent's root — that is the fork, and it copies no page.
        let child_root = pr
            .put(base, child.branch_id, e1, 1, 1, &[Value::Integer(999)])
            .unwrap();

        assert_eq!(
            pr.get(base, 1, 1).unwrap(),
            Some(vec![Value::Integer(100)]),
            "the parent's root must still see the pre-fork value"
        );
        assert_eq!(
            pr.get(child_root, 1, 1).unwrap(),
            Some(vec![Value::Integer(999)]),
            "the child's root must see its own write"
        );
        assert_ne!(base, child_root, "a copy-on-write update must produce a new root");
    }
}
