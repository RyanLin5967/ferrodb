use crate::{catalog::column::Value, error::FerroError, storage::{disk_manager::PAGE_SIZE, heap_file_manager::RecordId}};
use crate::storage::index_fulltext::{distinct_tokens, indexed_text, posting_key};
#[derive(PartialEq, Debug)]
pub struct BPlusTreeInternalPage<K> {
    pub page_type: u8,
    pub page_id: u32,
    pub lsn: u64,
    pub checksum: u32,
    pub num_keys: u16,
    pub key_arr: Vec<K>, // prim: Value, sec: (Value, Value)
    pub child_ptrs: Vec<u32>,
}

#[derive(PartialEq, Debug)]
pub struct BPlusTreeLeafPage<K, V> {
    pub page_type: u8,
    pub page_id: u32,
    pub lsn: u64,
    pub checksum: u32,
    pub num_keys: u16,
    pub next: Option<u32>,
    pub prev: Option<u32>,
    pub key_arr: Vec<K>, // prim: Value, sec: (Value, Value)
    pub vals: Vec<V> // prim: RecordId, sec: 0
}

pub enum BPlusTreePage<K, V> {
    Internal(BPlusTreeInternalPage<K>),
    Leaf(BPlusTreeLeafPage<K, V>)
}
pub const BPLUS_INTERNAL_TYPE: u8 = 2;
pub const BPLUS_LEAF_TYPE: u8 = 3;
const INTERNAL_HEADER_SIZE: usize = 19;
const LEAF_HEADER_SIZE: usize = 27;

/// **The largest entry, key plus value as stored, that any B+tree in this engine admits: 2034
/// bytes.** D225.
///
/// [`admit_entry`] refuses anything larger, by name, before a tree latches or writes, because
/// above this size a leaf split is not guaranteed to exist. For the trees in this engine that is:
///
/// - a primary index, `Value -> RecordId` (6 bytes): a key of at most 2028 bytes, so a `VARCHAR`
///   primary key of at most 2025 bytes of text (1 tag + 2 length + the text);
/// - a secondary index, `(value, pk) -> ()`: the value and the primary key together at most 2034;
///   a `VARCHAR` value under an `INTEGER` key (5 bytes) at most 2026 bytes of text;
/// - a posting tree, `(token, pk) -> ()`: a token is at most `MAX_TOKEN_BYTES` = 255, so any
///   primary key of at most 2034 - 258 = 1776 bytes as stored;
/// - the branch catalog, `Vec<u8> -> Vec<u8>` (4-byte length prefix each): key and value bytes
///   together at most 2026.
///
/// # Why this number: a leaf split always exists up to it, and not one byte past it
///
/// Let `B = PAGE_SIZE - LEAF_HEADER_SIZE` = 4069, a leaf's body. `is_full` is `bytes >= B`, so a
/// leaf that is not full holds `S <= B - 1` bytes. A split is only ever needed when ONE entry of
/// `c` bytes has just been added to such a leaf — an insert, or a replace after removing the old
/// entry. If the new entry is first or last, cutting next to it gives halves of `c` and `S` bytes,
/// both under `B`. Otherwise call the old entries' bytes to its left `L` and to its right `R`, so
/// `L + R = S`: cutting just before it gives `L` and `c + R`, just after it gives `L + c` and `R`.
/// Both fail only if `c + R >= B` **and** `L + c >= B`, i.e. `S + 2c >= 2B`, which with
/// `S <= B - 1` needs `2c >= B + 1`, i.e. `c >= 2035`. So every `c <= B / 2` = 2034 has a cut with
/// both halves under the threshold, **whatever the widths of the entries already there**, and
/// [`BPlusTreeLeafPage::split_point`] considers every cut, so it finds one.
///
/// The bound is tight. Two 2034-byte entries (4068 bytes, not full) with a 2035-byte entry
/// between them have no such cut: either half holding the new entry is 4069 bytes, which is full.
/// `max_entry_bytes_is_tight` pins both sides.
///
/// **Internal pages need nothing tighter.** A separator is a leaf key, so at most this many bytes.
/// An internal node is full at `C = PAGE_SIZE - INTERNAL_HEADER_SIZE` = 4077 bytes of keys and
/// 4-byte child pointers; it holds at most `C` on disk, and at most `C + k + 4` once a separator of
/// `k` bytes is added. Take the largest cut whose left half (the keys before the pushed one, and
/// one more pointer than keys) is under `C`; pushing up the first key leaves a left half of one
/// pointer, so such a cut exists. If it is the last key, the right half is one pointer. Otherwise
/// moving one key further would put the left at `C` or more, so the left already holds at least
/// `C - k' - 4` bytes (`k'` the pushed key) and the right half, everything else, is at most
/// `(C + k + 4) - C + 4 = k + 8` bytes: under `C` for any `k` a leaf admits. A new root is one
/// key and two pointers, `k + 8` again.
///
/// **What the bound does not cover: data an earlier build wrote.** Its count split admitted any
/// entry that happened to fit, so a database can hold entries over this bound, and each place
/// that meets one refuses by name rather than guessing:
///
/// - a leaf left at exactly `B` bytes around such an entry, where [`BPlusTreeLeafPage::split_point`]
///   can find no cut: the insert that needs the split is refused. **This one no executor
///   pre-check can see**, because it depends on the page, not the row: it can fire inside a
///   secondary or posting insert AFTER the heap write and the primary upsert. What keeps that from
///   leaving a primary entry pointing at an undone slot is the abort undoing the primary-index
///   writes it recorded (D202, `TxnManager::record_primary_write`, on `rollback-index-orphan`).
///   **D225 therefore lands after D202**; `a_no_cut_refusal_after_the_heap_write_leaves_no_dangling_primary_entry`
///   is red without it;
/// - a heap row whose entry is over the bound, met by the crash-recovery rebuild: refused before
///   any tree is freed (`wal::recovery`), and the database does not open until the remedy below
///   has been applied;
/// - a primary key over the bound, met by an ALTER that rewrites rows or an UPDATE of that row:
///   refused before the first row is written (`catalog::alter::prepare_rewrite`, the UPDATE
///   pre-pass), because either may have to re-point the key's entry;
/// - a branch whose stored capability envelope is over the bound: rewrites of its record that
///   leave the envelope unchanged go through (`TableBranchCatalog::envelope_write`), so its lease
///   can be renewed and it can be reaped; a CHANGED envelope over the bound is refused, which
///   includes `charge_row_writes` and a `restrict_envelope` that leaves it over the bound;
/// - such a branch cannot be **forked**: the child inherits the envelope at the same encoded size
///   (`CapabilityEnvelope::inherited` changes only fixed-width counters), so `write_record_new`
///   refuses it, and on a recycled id `envelope_write` does, unless the reaped occupant's bytes
///   happen to equal the child's, when the fork succeeds harmlessly;
/// - a branch-catalog leaf left exactly full around an over-bound entry refuses **mid-record**:
///   `write_record` removes the branch's old state and deadline keys before it upserts the new
///   ones, so a no-cut refusal there leaves the branch in neither span. D202's undo does not cover
///   the branch catalog;
/// - a `LogBranchCatalog` holding an over-bound envelope **fails to open**: `open` migrates it
///   through `migrate_from`, whose `write_record` puts the envelope into a fresh tree and is
///   refused.
///
/// **The remedies.** For the rows of a table on a database this build has open, [`OPEN_TABLE_REMEDY`];
/// for one crash recovery refuses to open, [`RECOVERY_REMEDY`]. Written for the tree D225 lands on,
/// which has #16 (D202): there, INSERTing a deleted row's key again writes the new row into the
/// dead version's slot and moves the dead version to the table's history (#16's `4296723`). So a
/// deleted row whose KEY fits has an in-place remedy (insert the key again with values that fit,
/// then delete it again if unwanted), and nothing else removes a deleted tuple (there is no
/// VACUUM). A key OVER the bound has none: it cannot be updated, and inserting it again is
/// refused, so only the table copy reaches it. On `d225-byte-split` alone, before #16 is merged,
/// the in-place route does not work: the re-inserted row takes a new slot and the dead tuple stays.
/// For the envelope cases the remedy is `restrict_envelope` to an envelope below the bound, which
/// is admitted; after it, charges and forks work again. The mid-record and migration cases have
/// none in this build.
///
/// **This build writes nothing over the bound itself**, and that includes an ALTER that widens an
/// indexed column (`catalog::alter::prepare_rewrite` asks every step, review 6 H1 and review 7 K6)
/// and a MERGE that publishes rows into a shape it changes (`refuse_if_the_row_cannot_land`,
/// review 7 H2). **One residual is not a bound failure but its root cause:** a deleted tuple stays
/// until its key is inserted again, so a deleted row whose value is too long for an index entry
/// refuses every CREATE INDEX on that column until then, or until the table is copied. The
/// refusals name that; they cannot cure it.
pub const MAX_ENTRY_BYTES: usize = (PAGE_SIZE - LEAF_HEADER_SIZE) / 2;

/// What a refusal of an entry over [`MAX_ENTRY_BYTES`] tells the user to do on a database THIS
/// build has open, when the row's KEY is the cause or nothing in place reaches it: the ALTER,
/// UPDATE and CREATE INDEX refusals. D225, reviews 5 to 7.
///
/// Every step is one this build can take. SELECT reads the rows, an INSERT under a key and values
/// that fit is admitted, and `drop_table` frees the trees without asking the bound. It spells out
/// the steps this SQL surface lacks (INSERT … SELECT, RENAME TABLE) and the two the copy needs
/// that are easy to miss: the indexes are created again, and the table's history is lost. It never
/// suggests a delete, which for a key over the bound leaves the tuple behind for good.
pub const OPEN_TABLE_REMEDY: &str = "With this build: copy the table's live rows into a new table, \
    each under a key and values whose index entries fit the bound (there is no INSERT ... SELECT, \
    so SELECT the rows and INSERT each one), DROP the old table, CREATE it again under its old \
    name, copy the rows back and CREATE its indexes again (there is no RENAME TABLE). The copy \
    discards the table's history: DROP frees its time-travel heap and its dead versions, so no \
    older snapshot can read them any more. No in-place change repairs a key over the bound: a \
    primary key cannot be updated and one over the bound cannot be inserted again, so a deleted \
    row under it stays in the table (there is no VACUUM).";

/// What the crash-recovery refusal tells the user to do, with a build that can still open the
/// database, because this one refuses to. D225, reviews 5 to 7.
pub const RECOVERY_REMEDY: &str = "This build cannot open the database until that is repaired. \
    With a build before D225: for a live row, UPDATE its value to fit; for a deleted row whose key \
    fits, INSERT that key again with values that fit, if that build writes a re-inserted key into \
    its dead version's slot (#16's D202 build does), then remove it again if it is not wanted; \
    otherwise copy the table's live rows into a new table, each under a key and values whose index \
    entries fit the bound, DROP the old table, CREATE it again under its old name, copy the rows \
    back and CREATE its indexes again. The copy discards the table's history: DROP frees its \
    time-travel heap and its dead versions. A primary key over the bound can be neither updated \
    nor inserted again, so for it only the copy works.";

/// Which index an entry belongs to, so a refusal can name it. D225, review 7 K9.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryOf {
    /// The primary index: `(key, RecordId)`.
    Primary,
    /// The secondary index on the column at this position: `((value, key), ())`.
    Secondary(usize),
    /// A posting in the full-text index on the column at this position: `((token, key), ())`.
    Posting(usize),
}

/// Bytes `t` occupies on a page. One definition, so the split, the fullness test and the entry
/// bound cannot disagree about what an entry costs.
pub fn serialized_len<T: BTreeSerialize>(t: &T) -> usize {
    let mut buf = Vec::new();
    t.serialize(&mut buf);
    buf.len()
}

/// Whether an entry of `len` bytes is over [`MAX_ENTRY_BYTES`]. **The one comparison with the
/// bound**: every other check, [`entry_over_bound`] and [`first_entry_over_bound`] included, asks
/// this. D225, review 7.
pub fn is_over_bound(len: usize) -> bool {
    len > MAX_ENTRY_BYTES
}

/// The size of an entry that is over [`MAX_ENTRY_BYTES`], or `None` when it is admitted. The
/// measurement behind [`admit_entry`], for refusals that give their own remedy. D225, review 6.
pub fn entry_over_bound<K: BTreeSerialize, V: BTreeSerialize>(key: &K, value: &V) -> Option<usize> {
    let len = serialized_len(key) + serialized_len(value);
    is_over_bound(len).then_some(len)
}

/// The size of every index entry `row` makes, **measured on the trees' own entry types**, in this
/// order: the primary entry when `primary` is set, then the secondary index on each position in
/// `secondary`, then every posting of each position in `fulltext`. `row[0]` is the key. D225,
/// review 7 K9.
///
/// **The one builder of entry shapes.** Every pre-check asks this instead of building
/// `(value, key)` or a posting itself: INSERT, UPDATE's pre-pass, the ALTER, both backfills,
/// recovery and the MERGE's landing check. So none of them can drift from what the trees store.
/// Two of those guard paths with no backstop before a free (the ALTER, which writes no secondary
/// entry, and recovery), where a drift would be review 6's H1 again.
pub fn row_entry_sizes(
    row: &[Value],
    primary: bool,
    secondary: &[usize],
    fulltext: &[usize],
) -> Result<Vec<(EntryOf, usize)>, FerroError> {
    let key = row
        .first()
        .ok_or_else(|| FerroError::Internal("a row with no columns has no key to index".into()))?;
    let column = |p: usize| {
        row.get(p).ok_or_else(|| {
            FerroError::Internal(format!("an index names column {p} of a {}-column row", row.len()))
        })
    };
    let mut out = Vec::new();
    if primary {
        out.push((EntryOf::Primary, serialized_len(key) + serialized_len(&RecordId::new(0, 0))));
    }
    for &p in secondary {
        let entry = (column(p)?.clone(), key.clone());
        out.push((EntryOf::Secondary(p), serialized_len(&entry) + serialized_len(&())));
    }
    for &p in fulltext {
        if let Some(text) = indexed_text(column(p)?)? {
            for token in distinct_tokens(text) {
                out.push((EntryOf::Posting(p), serialized_len(&posting_key(&token, key)) + serialized_len(&())));
            }
        }
    }
    Ok(out)
}

/// The first of `row`'s index entries that is over [`MAX_ENTRY_BYTES`], with its size, in
/// [`row_entry_sizes`]'s order. D225, review 7.
pub fn first_entry_over_bound(
    row: &[Value],
    primary: bool,
    secondary: &[usize],
    fulltext: &[usize],
) -> Result<Option<(EntryOf, usize)>, FerroError> {
    Ok(row_entry_sizes(row, primary, secondary, fulltext)?
        .into_iter()
        .find(|&(_, len)| is_over_bound(len)))
}

/// The index entries an UPDATE from `old` to `new` writes besides the primary re-point: the
/// `secondary` positions whose value changed, and the `fulltext` positions whose indexed text
/// changed. The UPDATE executor's pre-pass asks these, and so does a MERGE's landing check for
/// the Update it will publish through that executor, so the two ask the same set (review 7 H2).
/// Asking more would refuse an UPDATE that writes nothing new.
pub fn entries_an_update_writes(
    old: &[Value],
    new: &[Value],
    secondary: &[usize],
    fulltext: &[usize],
) -> Result<(Vec<usize>, Vec<usize>), FerroError> {
    // Borrowed, not cloned: the UPDATE pre-pass calls this for every row it plans.
    fn at(row: &[Value], p: usize) -> Result<&Value, FerroError> {
        row.get(p).ok_or_else(|| {
            FerroError::Internal(format!("an index names position {p} of a {}-column row", row.len()))
        })
    }
    let mut changed = Vec::new();
    for &p in secondary {
        if at(old, p)? != at(new, p)? {
            changed.push(p);
        }
    }
    let mut retokenized = Vec::new();
    for &p in fulltext {
        if indexed_text(at(old, p)?)? != indexed_text(at(new, p)?)? {
            retokenized.push(p);
        }
    }
    Ok((changed, retokenized))
}

/// The named part of every entry-bound refusal, and no remedy: the wrappers that carry
/// [`OPEN_TABLE_REMEDY`] or [`RECOVERY_REMEDY`] must not also say "shorten the indexed value",
/// which [`admit_entry`]'s own message does. D225, review 6.
pub fn entry_too_large(len: usize) -> String {
    format!(
        "index entry too large: {len} bytes (key and value as stored), over the B+tree entry \
         limit MAX_ENTRY_BYTES = {MAX_ENTRY_BYTES}"
    )
}

/// Refuse an entry larger than [`MAX_ENTRY_BYTES`], naming the limit. D225.
///
/// `BPlusTreeManager::insert` and `upsert` call this before they latch or write anything. The DML
/// paths (`execution::insert`, `execution::update`) call it earlier still, for every entry a row
/// will add, so a refused row writes nothing at all; `execution::insert` says why that matters.
///
/// `Constraint`, not `Unrepresentable`: no length field is too narrow here, and a page could hold
/// the entry. This is a limit the split needs, the same kind as PostgreSQL's "index row size
/// exceeds maximum". It reaches a client as a statement error (`23000` in `pgwire::sqlstate_of`),
/// not as an internal one, because the fix is to shorten the value.
pub fn admit_entry<K: BTreeSerialize, V: BTreeSerialize>(key: &K, value: &V) -> Result<(), FerroError> {
    match entry_over_bound(key, value) {
        Some(len) => Err(entry_refusal(len)),
        None => Ok(()),
    }
}

/// [`admit_entry`]'s refusal, for a pre-check that measured with [`first_entry_over_bound`] and
/// refuses NEW data (INSERT, UPDATE's changed values), where shortening the value is the remedy.
pub fn entry_refusal(len: usize) -> FerroError {
    FerroError::Constraint(format!(
        "index entry too large: {len} bytes (key and value as stored) does not fit under the \
         B+tree entry limit MAX_ENTRY_BYTES = {MAX_ENTRY_BYTES}, the largest size for which \
         every leaf split can leave both halves under a {PAGE_SIZE}-byte page; shorten the \
         indexed value"
    ))
}
// const CHILD_POINTER_SIZE: usize = 4;
// HEADER: |page_type (1)|page_id (4)|lsn (8)|checksum (4)|num_keys (2)|
impl<K: BTreeSerialize> BPlusTreeInternalPage<K> {

    pub fn new(page_id: u32) -> Self {
        BPlusTreeInternalPage { page_id, page_type: BPLUS_INTERNAL_TYPE, lsn: 0, checksum: 0, num_keys: 0, key_arr: Vec::new(), child_ptrs: Vec::new() }
    }

    pub fn serialize(&self) -> Result<[u8; PAGE_SIZE], FerroError>{
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0] = BPLUS_INTERNAL_TYPE;
        bytes[1..5].copy_from_slice(&self.page_id.to_be_bytes());
        bytes[5..13].copy_from_slice(&self.lsn.to_be_bytes());
        bytes[13..17].copy_from_slice(&self.checksum.to_be_bytes());
        bytes[17..19].copy_from_slice(&self.num_keys.to_be_bytes());

        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        // ⛔ D225: refuse a node that does not fit, rather than panic slicing past the page.
        if INTERNAL_HEADER_SIZE + buf.len() + 4 * self.child_ptrs.len() > PAGE_SIZE {
            return Err(FerroError::Internal(format!(
                "internal page {} holds {} bytes of keys and child pointers, over the {}-byte body \
                 of a {PAGE_SIZE}-byte page; refused rather than written. A split should have \
                 prevented this (D225)",
                self.page_id,
                buf.len() + 4 * self.child_ptrs.len(),
                PAGE_SIZE - INTERNAL_HEADER_SIZE
            )));
        }
        bytes[INTERNAL_HEADER_SIZE..INTERNAL_HEADER_SIZE + buf.len()].copy_from_slice(&buf);
        for (i, child_ptr) in self.child_ptrs.iter().enumerate() {
            bytes[INTERNAL_HEADER_SIZE + buf.len() + i*4..INTERNAL_HEADER_SIZE + buf.len() + i*4 +4].copy_from_slice(&child_ptr.to_be_bytes());
        }
        
        Ok(bytes)
    }
    
    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        let page_type = u8::from_be_bytes(bytes[0..1].try_into().unwrap());
        let page_id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
        let lsn = u64::from_be_bytes(bytes[5..13].try_into().unwrap());
        let checksum = u32::from_be_bytes(bytes[13..17].try_into().unwrap());
        let num_keys = u16::from_be_bytes(bytes[17..19].try_into().unwrap());

        let mut offset = INTERNAL_HEADER_SIZE;
        let mut key_arr = Vec::new();
        for _ in 0..num_keys {
            let (key, consumed) = K::deserialize(&bytes[offset..])?;
            key_arr.push(key);
            offset += consumed;
        }
        let mut child_ptrs = Vec::new();
        for i  in 0..num_keys + 1 {
            child_ptrs.push(u32::from_be_bytes(bytes[offset+ i as usize*4..offset+i as usize*4+ 4].try_into().unwrap()));
        }

        Ok(Self { page_type, page_id, lsn, checksum, num_keys, key_arr, child_ptrs })
    }
}

// HEADER: |page_type (1)|page_id (4)|lsn (8)|checksum (4)|num_keys (2)|next (4)|prev (4)|
impl<K: BTreeSerialize, V: BTreeSerialize> BPlusTreeLeafPage<K, V> {

    pub fn new(page_id: u32) -> Self {
        BPlusTreeLeafPage { page_id, page_type: BPLUS_LEAF_TYPE, lsn: 0, checksum: 0, num_keys: 0, next: None, prev: None, key_arr: Vec::new(), vals: Vec::new() }
    }

    pub fn serialize(&self) -> Result<[u8; PAGE_SIZE], FerroError>{
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0] = BPLUS_LEAF_TYPE;
        bytes[1..5].copy_from_slice(&self.page_id.to_be_bytes());
        bytes[5..13].copy_from_slice(&self.lsn.to_be_bytes());
        bytes[13..17].copy_from_slice(&self.checksum.to_be_bytes());
        bytes[17..19].copy_from_slice(&self.num_keys.to_be_bytes());
        match self.next {
            Some(next) => bytes[19..23].copy_from_slice(&next.to_be_bytes()),
            None => bytes[19..23].copy_from_slice(&[0u8; 4]),
        } 
        match self.prev {
            Some(prev) => bytes[23..27].copy_from_slice(&prev.to_be_bytes()),
            None => bytes[23..27].copy_from_slice(&[0u8; 4]),
        }

        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        let mut buff = Vec::new();
        for v in &self.vals {
            v.serialize(&mut buff);
        }
        // ⛔ D225: refuse a leaf that does not fit, rather than panic slicing past the page. This
        // copy was the panic `d208_review2` A1 reached from ordinary SQL.
        if LEAF_HEADER_SIZE + buf.len() + buff.len() > PAGE_SIZE {
            return Err(FerroError::Internal(format!(
                "leaf page {} holds {} bytes of entries, over the {}-byte body of a \
                 {PAGE_SIZE}-byte page; refused rather than written. A split should have \
                 prevented this (D225)",
                self.page_id,
                buf.len() + buff.len(),
                PAGE_SIZE - LEAF_HEADER_SIZE
            )));
        }
        bytes[LEAF_HEADER_SIZE..LEAF_HEADER_SIZE + buf.len()].copy_from_slice(&buf);
        bytes[LEAF_HEADER_SIZE + buf.len()..LEAF_HEADER_SIZE + buf.len() + buff.len()].copy_from_slice(&buff);

        Ok(bytes)
    }

    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        let page_type = u8::from_be_bytes(bytes[0..1].try_into().unwrap());
        let page_id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
        let lsn = u64::from_be_bytes(bytes[5..13].try_into().unwrap());
        let checksum = u32::from_be_bytes(bytes[13..17].try_into().unwrap());
        let num_keys = u16::from_be_bytes(bytes[17..19].try_into().unwrap());
        let next = match u32::from_be_bytes(bytes[19..23].try_into().unwrap()) {
            0 => None,
            n => Some(n)
        };
        let prev = match u32::from_be_bytes(bytes[23..27].try_into().unwrap()) {
            0 => None,
            p => Some(p)
        };

        let mut key_arr = Vec::new();
        let mut offset = LEAF_HEADER_SIZE;
        for _ in 0..num_keys {
            let (key, consumed) = K::deserialize(&bytes[offset..])?;
            key_arr.push(key);
            offset += consumed;
        }

        let mut vals = Vec::new();
        for _ in 0..num_keys {
            let (val, consumed) = V::deserialize(&bytes[offset..])?;
            vals.push(val);
            offset += consumed;
        }
        Ok(Self { page_type, page_id, lsn, checksum, num_keys, next, prev, key_arr, vals })
    }
}

pub trait BTreeSerialize {
    fn serialize(&self, buf: &mut Vec<u8>);
    fn deserialize(bytes: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized;
}
impl BTreeSerialize for Value { // primary
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Value::Integer(i) => { // tag 0
                buf.push(0);
                buf.extend_from_slice(&i.to_be_bytes());
            }
            Value::Varchar(s) => { // tag 1, etc...
                buf.push(1);
                buf.extend_from_slice(&(s.as_bytes().len() as u16).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Float(f) => {
                buf.push(2); 
                buf.extend_from_slice(&f.to_be_bytes());
            }
            Value::Boolean(b) => {
                buf.push(3);
                buf.push(*b as u8);
            }
            Value::Null => {
                buf.push(4);
            }
            // Tags 0..4 are fixed by every index page already on disk; the wide types take the
            // next free numbers so an existing tree keeps deserialising.
            Value::BigInt(v) => {
                buf.push(5);
                buf.extend_from_slice(&v.to_be_bytes());
            }
            Value::Decimal(d) => {
                buf.push(6);
                buf.extend_from_slice(&(d.as_bytes().len() as u16).to_be_bytes());
                buf.extend_from_slice(d.as_bytes());
            }
            Value::Timestamp(ms) => {
                buf.push(7);
                buf.extend_from_slice(&ms.to_be_bytes());
            }
        }
    }

    fn deserialize(bytes: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized { // (Value, consumed)
        let tag = bytes[0];
        match tag {
            0 => {
                let i = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
                Ok((Value::Integer(i), 5))
            }
            1 => {
                let len = u16::from_be_bytes(bytes[1..3].try_into().unwrap()) as usize;
                let s = String::from_utf8(bytes[3..3+len].to_vec()).map_err(|_| FerroError::Corruption("bad utf8".into()))?;
                Ok((Value::Varchar(s), 3 + len))
            }
            2 => {
                let f = f64::from_be_bytes(bytes[1..9].try_into().unwrap());
                Ok((Value::Float(f), 9))
            }
            3 => {
                let b = bytes[1] != 0;
                Ok((Value::Boolean(b), 2))
            }
            4 => {
                Ok((Value::Null, 1))
            }
            5 => {
                let v = i64::from_be_bytes(bytes[1..9].try_into().unwrap());
                Ok((Value::BigInt(v), 9))
            }
            6 => {
                let len = u16::from_be_bytes(bytes[1..3].try_into().unwrap()) as usize;
                let s = String::from_utf8(bytes[3..3+len].to_vec()).map_err(|_| FerroError::Corruption("bad utf8".into()))?;
                Ok((Value::Decimal(s), 3 + len))
            }
            7 => {
                let ms = i64::from_be_bytes(bytes[1..9].try_into().unwrap());
                Ok((Value::Timestamp(ms), 9))
            }
            _ => Err(FerroError::Io(String::from("invalid tag value")))
        }
    }
}

impl BTreeSerialize for RecordId {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.page_id.to_be_bytes());
        buf.extend_from_slice(&self.slot_num.to_be_bytes());
    }

    fn deserialize(bytes: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized {
        if bytes.len() < 6 {
            return Err(FerroError::NotEnoughSpace)
        }
        let page_id = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let slot_num = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
        Ok ((RecordId::new(page_id, slot_num), 6))
    }
}

/// Byte strings, for indexes whose key is a composite the caller encodes itself.
///
/// The branch catalog uses this for both key and value: one tree holds its records and every index
/// over them, separated by a leading tag byte (`branch::tree_keys`). That works because `Vec<u8>`'s
/// `Ord` is **lexicographic**, so a big-endian encoded integer sorts in numeric order and a range
/// query over a tag prefix is a contiguous scan. See `SCALE-DESIGN.md` D2b.
///
/// Length-prefixed rather than delimited: a delimiter would have to be escaped out of the payload,
/// and an encoded page id can contain any byte including a delimiter.
impl BTreeSerialize for Vec<u8> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.len() as u32).to_be_bytes());
        buf.extend_from_slice(self);
    }

    fn deserialize(bytes: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized {
        if bytes.len() < 4 {
            return Err(FerroError::NotEnoughSpace);
        }
        let len = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        // A length that runs past the buffer is a torn or misaligned entry. Returning a truncated
        // value would hand the caller a silently short record, which for a branch record means a
        // deserialize error at best and a wrong root page id at worst.
        if bytes.len() < 4 + len {
            return Err(FerroError::NotEnoughSpace);
        }
        Ok((bytes[4..4 + len].to_vec(), 4 + len))
    }
}

impl BTreeSerialize for () {
    fn serialize(&self, _: &mut Vec<u8>) {}
    fn deserialize(_: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized { Ok(((), 0)) }
}
impl BTreeSerialize for (Value, Value) { // secondary
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf);
        self.1.serialize(buf);
    }

    fn deserialize(bytes: &[u8]) -> Result<(Self, usize), FerroError> where Self: Sized {    
        let (first, len1) = Value::deserialize(bytes)?;
        let (second, len2) = Value::deserialize(&bytes[len1..])?;
        Ok(((first, second), len1+len2))
    }
}

impl <K: BTreeSerialize, V: BTreeSerialize> BPlusTreePage<K, V> {
    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        match bytes[0] {
            BPLUS_INTERNAL_TYPE => Ok(BPlusTreePage::Internal(BPlusTreeInternalPage::deserialize(bytes)?)),
            BPLUS_LEAF_TYPE => Ok(BPlusTreePage::Leaf(BPlusTreeLeafPage::deserialize(bytes)?)),
            _ => Err(FerroError::Io(String::from("invalid page type header")))
        }
    }
}

// |      K1       |       K2       |       K3       |
// |    k < K1  |K1 <= k < K2 | K2 <= k < K3 |k >= K4|
impl <K: Ord + Clone + BTreeSerialize> BPlusTreeInternalPage<K> {

    // find position of a key (or where it should go) returns index (search and insert use)
    pub fn binary_search(&self, key: &K) -> usize {
        match self.key_arr.binary_search(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        }
    }

    // binary search the separator keys, retrn matching child ptr index
    pub fn find_child(&self, key: &K) -> u32{
        let index = match self.key_arr.binary_search(key) {
            Ok(pos) => pos+ 1,
            Err(pos) => pos,
        };
        self.child_ptrs[index]
    }
    // insert a separator key and its chidl pointer at a position (for child splits and pushes key up)
    pub fn insert_key_child(&mut self, index: usize, key: K, child_ptr: u32){
        self.key_arr.insert(index, key);
        self.child_ptrs.insert(index+1, child_ptr);
        self.num_keys += 1;
    }
    /// Where to split this node: the index of the key that moves UP. The keys before it stay here
    /// with their children; the keys after it move to the new node with theirs.
    ///
    /// **By bytes — D225**, by the same rule as [`BPlusTreeLeafPage::split_point`]: among the cuts
    /// that leave both halves under the full threshold, the one that best balances their bytes,
    /// with ties going to the count midpoint `len / 2`. That is where the count split always cut,
    /// so a node whose keys are all one width splits exactly where it did before. A half may keep
    /// no key and one child. The count split produced that too, for a two-key node, and every
    /// reader handles it (`find_child` on no keys is child 0).
    ///
    /// `None` when no cut qualifies. [`MAX_ENTRY_BYTES`] shows that cannot happen to a node that
    /// has just taken one separator a leaf admits.
    pub fn split_point(&self) -> Option<usize> {
        let n = self.key_arr.len();
        if n == 0 || self.child_ptrs.len() != n + 1 {
            return None;
        }
        let sizes: Vec<usize> = self.key_arr.iter().map(serialized_len).collect();
        let total: usize = sizes.iter().sum();
        let under = |bytes: usize| INTERNAL_HEADER_SIZE + bytes < PAGE_SIZE;
        let count_mid = n / 2;
        // ((byte imbalance, distance from the count midpoint), cut), smallest first.
        let mut best: Option<((usize, usize), usize)> = None;
        let mut before = 0; // bytes of the keys left of the pushed one
        for m in 0..n {
            let left = before + 4 * (m + 1);
            let right = (total - before - sizes[m]) + 4 * (n - m);
            before += sizes[m];
            if !under(left) || !under(right) {
                continue;
            }
            let rank = (left.abs_diff(right), m.abs_diff(count_mid));
            if best.is_none_or(|(b, _)| rank < b) {
                best = Some((rank, m));
            }
        }
        best.map(|(_, m)| m)
    }

    /// Push key `mid` up and move the keys after it, with their children, to a new node at
    /// `new_page_id`. Returns the pushed key and the new node.
    ///
    /// ⛔ **The D225 guard: refuses, and leaves this node exactly as it was, if either half is at
    /// or over the full threshold.** [`Self::split_point`] never picks such a cut, so this fires
    /// only if the choice and the page encoding ever disagree about what a key costs, or a caller
    /// passes some other cut. Before D225 the answer to that was a panic in `serialize`.
    pub fn split_at(&mut self, mid: usize, new_page_id: u32) -> Result<(K, Self), FerroError> {
        let n = self.key_arr.len();
        if mid >= n || self.child_ptrs.len() != n + 1 {
            return Err(FerroError::Internal(format!(
                "internal page {}: cannot push up key {mid} of {n} (it has {} children)",
                self.page_id,
                self.child_ptrs.len()
            )));
        }
        let num_keys_before = self.num_keys;
        let mid_key = self.key_arr.remove(mid);

        let mut new_node = Self {
            page_id: new_page_id,
            page_type: BPLUS_INTERNAL_TYPE,
            lsn: 0,
            checksum: 0,
            num_keys: 0,
            key_arr: self.key_arr.split_off(mid),
            child_ptrs: self.child_ptrs.split_off(mid + 1)
        };
        new_node.num_keys = new_node.key_arr.len() as u16;
        self.num_keys = mid as u16;
        if self.is_full() || new_node.is_full() {
            let refusal = FerroError::Internal(format!(
                "splitting internal page {} at key {mid} of {n} would leave a half at or over the \
                 {PAGE_SIZE}-byte page ({} and {} bytes of keys and pointers); refused, and the \
                 node is left unsplit (D225)",
                self.page_id,
                self.payload_len(),
                new_node.payload_len()
            ));
            self.key_arr.push(mid_key);
            self.key_arr.append(&mut new_node.key_arr);
            self.child_ptrs.append(&mut new_node.child_ptrs);
            self.num_keys = num_keys_before;
            return Err(refusal);
        }
        Ok((mid_key, new_node))
    }

    /// The byte split, unwrapped, for the unit tests that pin its shape. **Test-only on purpose:**
    /// a production caller has to handle a refusal, and `BPlusTreeManager` does, through
    /// [`Self::split_point`] and [`Self::split_at`].
    #[cfg(test)]
    pub fn split(&mut self, new_page_id: u32) -> (K, Self) {
        let mid = self.split_point().expect("no cut leaves both halves under the page");
        self.split_at(mid, new_page_id).expect("the D225 guard refused split_point's own cut")
    }

    /// Bytes of keys and child pointers, the part of the page `is_full` measures.
    pub fn payload_len(&self) -> usize {
        self.key_arr.iter().map(serialized_len).sum::<usize>() + 4 * self.child_ptrs.len()
    }
    // does adding one more entry exceed capacity? triggers splits
    pub fn is_full(&self) -> bool { 
        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        let len = buf.len();
        let child_ptrs_size = self.child_ptrs.len() * 4;
        return INTERNAL_HEADER_SIZE + len + child_ptrs_size>= PAGE_SIZE;
    }
    // fewer than min entries (capacity/2) triggers merge/redistribute
    pub fn is_underfull(&self) -> bool {
        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        let child_ptrs_size = self.child_ptrs.len() * 4;
        INTERNAL_HEADER_SIZE + buf.len() + child_ptrs_size < (PAGE_SIZE - INTERNAL_HEADER_SIZE)/2
    }
}

impl<K: BTreeSerialize + Ord + Clone, V: Clone + BTreeSerialize + Ord> BPlusTreeLeafPage<K,V> {
    pub fn is_full(&self) -> bool {
        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        for val in &self.vals {
            val.serialize(&mut buf);
        }
        return buf.len() + LEAF_HEADER_SIZE >= PAGE_SIZE;
    } 
    pub fn is_underfull(&self) -> bool{
        let mut buf = Vec::new();
        for k in &self.key_arr {
            k.serialize(&mut buf);
        }
        for val in &self.vals {
            val.serialize(&mut buf);
        }
        LEAF_HEADER_SIZE + buf.len() < (PAGE_SIZE - LEAF_HEADER_SIZE)/2
    }
    pub fn binary_search(&self, key: &K) -> usize {
        match self.key_arr.binary_search(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        }
    }

    // insert a key,value at the correct sorted position
    pub fn insert_entry(&mut self, key: K, value: V){ 
        let index = self.binary_search(&key);
        self.key_arr.insert(index, key);
        self.vals.insert(index, value);
        self.num_keys += 1;
    }

    // find and remove a key/value
    pub fn remove_entry(&mut self, key: &K) -> Result<(), FerroError>{ 
        let index = match self.key_arr.binary_search(key) {
            Ok(i) => i,
            Err(_) => return Err(FerroError::KeyNotFound)
        };
        
        self.key_arr.remove(index);
        self.vals.remove(index);
        if self.num_keys > 0 {
            self.num_keys -= 1;
        }
        Ok(())
    }

    // return value for a key, or None
    pub fn get(&self, key: &K) -> Result<Option<&V>, FerroError> { 
        let index = match self.key_arr.binary_search(key) {
            Ok(i) => i,
            Err(_) => return Ok(None)
        };
        return Ok(Some(&self.vals[index]));
    }

    /// Where to split this leaf: the index of the first entry that moves to the new right leaf.
    ///
    /// **By bytes, not by count — D225.** The count split cut at `len / 2`, which is safe only
    /// when every entry has the same width. With variable-width keys the wide entries sort
    /// together, one half could hold all of them and be larger than a page, and `serialize`
    /// panicked copying it (`d208_review2` A1: a `VARCHAR` index, 32 ordinary INSERTs). This is
    /// PostgreSQL's `_bt_findsplitloc` in its plain form: among the cuts that leave BOTH halves
    /// non-empty and under the full threshold, take the one that best balances their bytes. Ties
    /// go to the cut nearest `len / 2`, so a leaf whose entries are all one width (every INTEGER,
    /// BIGINT or TIMESTAMP key) splits exactly where the count split did.
    ///
    /// `None` when no cut qualifies. [`MAX_ENTRY_BYTES`] proves that cannot happen after one
    /// admitted entry lands in a leaf that was not full; the caller refuses by name if it does.
    pub fn split_point(&self) -> Option<usize> {
        let n = self.key_arr.len();
        if self.vals.len() != n {
            return None;
        }
        let sizes: Vec<usize> = self
            .key_arr
            .iter()
            .zip(&self.vals)
            .map(|(k, v)| serialized_len(k) + serialized_len(v))
            .collect();
        let total: usize = sizes.iter().sum();
        let under = |bytes: usize| LEAF_HEADER_SIZE + bytes < PAGE_SIZE;
        let count_mid = n / 2;
        // ((byte imbalance, distance from the count midpoint), cut), smallest first.
        let mut best: Option<((usize, usize), usize)> = None;
        let mut left = 0;
        for m in 1..n {
            left += sizes[m - 1];
            let right = total - left;
            if !under(left) || !under(right) {
                continue;
            }
            let rank = (left.abs_diff(right), m.abs_diff(count_mid));
            if best.is_none_or(|(b, _)| rank < b) {
                best = Some((rank, m));
            }
        }
        best.map(|(_, m)| m)
    }

    /// Move the entries from `mid` on to a new leaf at `new_page_id`, copy the first of them up as
    /// the separator (it also stays in the leaf), and link the new leaf in after this one. Returns
    /// the separator and the new leaf.
    ///
    /// The old right neighbour's `prev` still names this leaf; it is another page, so repointing it
    /// is the caller's job (`BPlusTreeManager` does it under that page's latch).
    ///
    /// ⛔ **The D225 guard: refuses, and leaves this leaf exactly as it was, if either half is at or
    /// over the full threshold.** [`Self::split_point`] never picks such a cut, so this fires only
    /// if the choice and the page encoding ever disagree about what an entry costs, or a caller
    /// passes some other cut. Before D225 the answer to that was a panic in `serialize`.
    pub fn split_at(&mut self, mid: usize, new_page_id: u32) -> Result<(K, Self), FerroError> {
        let n = self.key_arr.len();
        if mid == 0 || mid >= n || self.vals.len() != n {
            return Err(FerroError::Internal(format!(
                "leaf page {}: cannot split before entry {mid} of {n}; both halves must keep an entry",
                self.page_id
            )));
        }
        let (next_before, num_keys_before) = (self.next, self.num_keys);
        let mid_key = self.key_arr[mid].clone();

        let mut new_node = Self {
            page_id: new_page_id,
            page_type: BPLUS_LEAF_TYPE,
            lsn: 0,
            checksum: 0,
            num_keys: 0,
            key_arr: self.key_arr.split_off(mid),
            vals: self.vals.split_off(mid),
            next: self.next, // new node points to old next
            prev: Some(self.page_id) // new node points back to self
        };
        self.next = Some(new_node.page_id); // self now points to new node
        new_node.num_keys = new_node.key_arr.len() as u16;
        self.num_keys = self.key_arr.len() as u16;
        if self.is_full() || new_node.is_full() {
            let refusal = FerroError::Internal(format!(
                "splitting leaf page {} before entry {mid} of {n} would leave a half at or over \
                 the {PAGE_SIZE}-byte page ({} and {} bytes of entries); refused, and the leaf is \
                 left unsplit (D225)",
                self.page_id,
                self.payload_len(),
                new_node.payload_len()
            ));
            self.key_arr.append(&mut new_node.key_arr);
            self.vals.append(&mut new_node.vals);
            self.next = next_before;
            self.num_keys = num_keys_before;
            return Err(refusal);
        }
        Ok((mid_key, new_node))
    }

    /// The byte split, unwrapped, for the unit tests that pin its shape. **Test-only on purpose:**
    /// a production caller has to handle a refusal, and `BPlusTreeManager` does, through
    /// [`Self::split_point`] and [`Self::split_at`].
    #[cfg(test)]
    pub fn split(&mut self, new_page_id: u32) -> (K, Self) {
        let mid = self.split_point().expect("no cut leaves both halves under the page");
        self.split_at(mid, new_page_id).expect("the D225 guard refused split_point's own cut")
    }

    /// Bytes of keys and values, the part of the page `is_full` measures.
    pub fn payload_len(&self) -> usize {
        self.key_arr.iter().map(serialized_len).sum::<usize>() + self.vals.iter().map(serialized_len).sum::<usize>()
    }

    pub fn upper_bound(&self, key: &K) -> usize {
        match self.key_arr.binary_search(key) {
            Ok(pos) => pos + 1,  // skip the equal key (keys are unique)
            Err(pos) => pos,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_i_p() -> Result<(), FerroError> {
        let mut internal = BPlusTreeInternalPage::<Value>::new(1);
        internal.key_arr = vec![Value::Integer(10), Value::Integer(20)];
        internal.child_ptrs = vec![100, 200, 300]; // N keys -> N+1 ptrs
        internal.num_keys = 2;

        let bytes = internal.serialize()?;
        let de = BPlusTreeInternalPage::<Value>::deserialize(bytes)?;
        assert_eq!(internal, de);
        Ok(())
    }

    #[test]
    fn test_roundtrip_i_s() -> Result<(), FerroError> {
        let mut internal = BPlusTreeInternalPage::<(Value, Value)>::new(1);
        internal.key_arr = vec![
            (Value::Integer(10), Value::Integer(1)),
            (Value::Integer(20), Value::Integer(2)),
        ];
        internal.child_ptrs = vec![100, 200, 300];
        internal.num_keys = 2;

        let bytes = internal.serialize()?;
        let de = BPlusTreeInternalPage::<(Value, Value)>::deserialize(bytes)?;
        assert_eq!(internal, de);
        Ok(())
    }

    #[test]
    fn test_roundtrip_l_p() -> Result<(), FerroError> {
        let mut leaf = BPlusTreeLeafPage::<Value, RecordId>::new(2);
        leaf.key_arr = vec![Value::Integer(5), Value::Integer(15)];
        leaf.vals = vec![RecordId::new(7, 3), RecordId::new(8, 4)];
        leaf.num_keys = 2;
        leaf.next = Some(3);
        leaf.prev = None;

        let bytes = leaf.serialize()?;
        let de = BPlusTreeLeafPage::<Value, RecordId>::deserialize(bytes)?;
        assert_eq!(leaf, de);
        Ok(())
    }

    #[test]
    fn test_roundtrip_l_s() -> Result<(), FerroError> {
        let mut leaf = BPlusTreeLeafPage::<(Value, Value), ()>::new(2);
        leaf.key_arr = vec![
            (Value::Varchar("toronto".into()), Value::Integer(1)),
            (Value::Varchar("toronto".into()), Value::Integer(2)),
        ];
        leaf.vals = vec![(), ()];
        leaf.num_keys = 2;
        leaf.next = Some(5);
        leaf.prev = Some(1);

        let bytes = leaf.serialize()?;
        let de = BPlusTreeLeafPage::<(Value, Value), ()>::deserialize(bytes)?;
        assert_eq!(leaf, de);
        Ok(())
    }

    #[test]
    fn test_internal_find_child() {
        let mut node = BPlusTreeInternalPage::<Value>::new(1);
        node.key_arr = vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)];
        node.child_ptrs = vec![100, 200, 300, 400];
        node.num_keys = 3;

        assert_eq!(node.find_child(&Value::Integer(5)), 100);
        assert_eq!(node.find_child(&Value::Integer(10)), 200);
        assert_eq!(node.find_child(&Value::Integer(15)), 200);
        assert_eq!(node.find_child(&Value::Integer(30)), 400);
        assert_eq!(node.find_child(&Value::Integer(99)), 400);
    }

    #[test]
    fn test_internal_insert_key_child() {
        let mut node = BPlusTreeInternalPage::<Value>::new(1);
        node.key_arr = vec![Value::Integer(10), Value::Integer(30)];
        node.child_ptrs = vec![100, 200, 300];
        node.num_keys = 2;

        node.insert_key_child(1, Value::Integer(20), 250);
        assert_eq!(node.key_arr, vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)]);
        assert_eq!(node.child_ptrs, vec![100, 200, 250, 300]);
        assert_eq!(node.num_keys, 3);
    }

    #[test]
    fn test_internal_split() {
        let mut node = BPlusTreeInternalPage::<Value>::new(1);
        node.key_arr = vec![Value::Integer(10), Value::Integer(20), Value::Integer(30), Value::Integer(40)];
        node.child_ptrs = vec![1,2,3,4,5];
        node.num_keys = 4;

        let (mid_key, new_node) = node.split(2);
        assert_eq!(mid_key, Value::Integer(30));
        assert_eq!(node.key_arr, vec![Value::Integer(10), Value::Integer(20)]);
        assert_eq!(node.child_ptrs, vec![1,2,3]);
        assert_eq!(node.num_keys, 2);
        assert_eq!(new_node.key_arr, vec![Value::Integer(40)]);
        assert_eq!(new_node.child_ptrs, vec![4,5]);
        assert_eq!(new_node.num_keys, 1);
        assert_eq!(node.child_ptrs.len(), node.key_arr.len() + 1);
        assert_eq!(new_node.child_ptrs.len(), new_node.key_arr.len() + 1);
    }

    #[test]
    fn test_leaf_insert_sorted() {
        let mut leaf = BPlusTreeLeafPage::<Value, RecordId>::new(1);
        leaf.insert_entry(Value::Integer(20), RecordId::new(1, 0));
        leaf.insert_entry(Value::Integer(10), RecordId::new(2, 0));
        leaf.insert_entry(Value::Integer(30), RecordId::new(3, 0));

        assert_eq!(leaf.key_arr, vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)]);
        assert_eq!(leaf.vals[0], RecordId::new(2,0));
        assert_eq!(leaf.num_keys, 3);
    }

    #[test]
    fn test_leaf_get() {
        let mut leaf = BPlusTreeLeafPage::<Value, RecordId>::new(1);
        leaf.insert_entry(Value::Integer(10), RecordId::new(5,2));
        leaf.insert_entry(Value::Integer(20), RecordId::new(6,7));

        assert_eq!(leaf.get(&Value::Integer(10)).unwrap(), Some(&RecordId::new(5,2)));
        assert_eq!(leaf.get(&Value::Integer(99)).unwrap(), None);
    }

    #[test]
    fn test_leaf_remove() {
        let mut leaf = BPlusTreeLeafPage::<Value, RecordId>::new(1);
        leaf.insert_entry(Value::Integer(10), RecordId::new(5,2));
        leaf.insert_entry(Value::Integer(20), RecordId::new(6,7));
        leaf.remove_entry(&Value::Integer(10)).unwrap();
        assert_eq!(leaf.key_arr, vec![Value::Integer(20)]);
        assert_eq!(leaf.num_keys, 1);
        assert!(leaf.remove_entry(&Value::Integer(99)).is_err());
    }

    #[test]
    fn test_leaf_split() {
        let mut leaf = BPlusTreeLeafPage::<Value, RecordId>::new(1);
        for i in 0..4 {
            leaf.insert_entry(Value::Integer(i*10), RecordId::new(i as u32, 0));
        }
        leaf.next = Some(99);
        let (mid_key, new_node) = leaf.split(2);

        assert_eq!(mid_key, Value::Integer(20));
        assert_eq!(leaf.key_arr, vec![Value::Integer(0), Value::Integer(10)]);
        assert_eq!(new_node.key_arr, vec![Value::Integer(20), Value::Integer(30)]);
        assert_eq!(new_node.key_arr[0], mid_key);
        assert_eq!(leaf.next, Some(new_node.page_id));
        assert_eq!(new_node.next, Some(99));
        assert_eq!(new_node.prev, Some(leaf.page_id));
        assert_eq!(leaf.num_keys, 2);
        assert_eq!(new_node.num_keys, 2);
    }

    #[test]
    fn test_leaf_composite_keys() {
        let mut leaf = BPlusTreeLeafPage::<(Value, Value), ()>::new(1);
        leaf.insert_entry((Value::Varchar("toronto".into()), Value::Integer(2)), ());
        leaf.insert_entry((Value::Varchar("toronto".into()), Value::Integer(1)), ());
        leaf.insert_entry((Value::Varchar("ottawa".into()), Value::Integer(5)), ());

        assert_eq!(leaf.key_arr[0], (Value::Varchar("ottawa".into()), Value::Integer(5)));
        assert_eq!(leaf.key_arr[1], (Value::Varchar("toronto".into()), Value::Integer(1)));
        assert_eq!(leaf.key_arr[2], (Value::Varchar("toronto".into()), Value::Integer(2)));
    }

    /// Bytes one key occupies on a page, measured with the page encoding itself.
    fn d225_bytes(k: &(Value, Value)) -> usize {
        let mut buf = Vec::new();
        k.serialize(&mut buf);
        buf.len()
    }

    /// **D225 — a leaf splits by BYTES, so both halves of a mixed-width leaf fit.**
    ///
    /// The leaf is `d208_review2` A1's, built in memory: 16 `('a', pk)` entries of 9 bytes, then 16
    /// `('b' x 247, pk)` of 255 (`Varchar` is 1 tag + 2 length + the bytes, `Integer` 1 + 4, `()`
    /// nothing). 4224 bytes against a 4069-byte body, so it is full and must split. The count split
    /// cut at `mid = 16` and put all sixteen wide entries, 4080 bytes, in the right half: over the
    /// body, so `serialize` panicked slicing past the page. Red at `9aa6968` on the right half's
    /// `is_full` assertion, before any serialize runs.
    ///
    /// The balanced cut is 24, by arithmetic rather than by asking the subject: 16·9 + 8·255 = 2184
    /// left and 8·255 = 2040 right, 144 apart; 23 gives 1929 / 2295 (366 apart) and 25 gives
    /// 2439 / 1785. Pinning it separates a byte split from "count, unless that overflows", which
    /// would cut at 17.
    #[test]
    fn a_mixed_width_leaf_splits_into_two_halves_that_fit() {
        let mut leaf = BPlusTreeLeafPage::<(Value, Value), ()>::new(1);
        for pk in 1..=16 {
            leaf.insert_entry((Value::Varchar("a".into()), Value::Integer(pk)), ());
        }
        for pk in 17..=32 {
            leaf.insert_entry((Value::Varchar("b".repeat(247)), Value::Integer(pk)), ());
        }
        // Premise: the leaf holds the mixed widths the reproducer's arithmetic assumes, narrow first.
        assert_eq!(leaf.key_arr.len(), 32);
        assert!(leaf.key_arr[..16].iter().all(|k| d225_bytes(k) == 9), "premise: 16 narrow 9-byte entries first");
        assert!(leaf.key_arr[16..].iter().all(|k| d225_bytes(k) == 255), "premise: 16 wide 255-byte entries after");
        assert!(leaf.is_full(), "premise: 4224 bytes is over the 4069-byte body, so this leaf must split");

        let (separator, right) = leaf.split(2);
        assert!(!leaf.is_full(), "the left half is at or over the page: {} entries", leaf.key_arr.len());
        assert!(!right.is_full(), "the right half is at or over the page: {} entries", right.key_arr.len());
        assert_eq!(leaf.key_arr.len(), 24, "not the byte-balanced cut (16·9 + 8·255 | 8·255)");
        assert_eq!(right.key_arr.len(), 8);
        assert_eq!((leaf.num_keys, right.num_keys), (24, 8));
        assert_eq!((leaf.vals.len(), right.vals.len()), (24, 8));
        assert_eq!(right.key_arr[0], separator, "the separator is the right half's first key");
        assert!(leaf.key_arr.last() < right.key_arr.first(), "the halves are out of key order");
        assert_eq!(leaf.next, Some(2), "the left half must publish the new page as its next");
        leaf.serialize().expect("the left half serializes");
        right.serialize().expect("the right half serializes");
    }

    /// **D225 — the same defect one level up, in an INTERNAL split.**
    ///
    /// 32 separators: 17 narrow `('a', pk)` of 9 bytes, then 15 wide `('b' x 260, pk)` of 268. Keys
    /// 17·9 + 15·268 = 4173 plus 33 child pointers (132) = 4305 bytes against a 4077-byte body, so
    /// it is full. The count split pushes key 16 (the last narrow one) up and keeps 15 wide keys
    /// and 16 pointers on the right: 4020 + 64 = 4084 bytes, over the body, so `serialize` would
    /// slice past the page. A tree reaches this state: before the 32nd separator arrived the node
    /// held 4305 - 268 - 4 = 4033 bytes, under the threshold. Red at `9aa6968` on the right half's
    /// `is_full`.
    ///
    /// The balanced cut pushes key 24 up: left 17·9 + 7·268 + 25·4 = 2129, right 7·268 + 8·4 =
    /// 1908, 221 apart; pushing 23 gives 1857 / 2180 (323 apart) and 25 gives 2401 / 1636.
    #[test]
    fn a_mixed_width_internal_node_splits_into_two_halves_that_fit() {
        let mut node = BPlusTreeInternalPage::<(Value, Value)>::new(1);
        for pk in 1..=17 {
            node.key_arr.push((Value::Varchar("a".into()), Value::Integer(pk)));
        }
        for pk in 18..=32 {
            node.key_arr.push((Value::Varchar("b".repeat(260)), Value::Integer(pk)));
        }
        node.child_ptrs = (100..133).collect();
        node.num_keys = 32;
        assert!(node.key_arr[..17].iter().all(|k| d225_bytes(k) == 9), "premise: 17 narrow 9-byte keys first");
        assert!(node.key_arr[17..].iter().all(|k| d225_bytes(k) == 268), "premise: 15 wide 268-byte keys after");
        assert_eq!(node.child_ptrs.len(), 33, "premise: one more child than keys");
        assert!(node.is_full(), "premise: 4305 bytes is over the 4077-byte body, so this node must split");

        let all_keys = node.key_arr.clone();
        let (up, right) = node.split(2);
        assert!(!node.is_full(), "the left half is at or over the page: {} keys", node.key_arr.len());
        assert!(!right.is_full(), "the right half is at or over the page: {} keys", right.key_arr.len());
        assert_eq!(up, all_keys[24], "not the byte-balanced cut (push key 24)");
        assert_eq!((node.key_arr.len(), right.key_arr.len()), (24, 7));
        assert_eq!((node.num_keys, right.num_keys), (24, 7));
        assert_eq!(node.child_ptrs.len(), node.key_arr.len() + 1);
        assert_eq!(right.child_ptrs.len(), right.key_arr.len() + 1);
        // Every key exactly once, in order, with the pushed one between the halves; every pointer
        // exactly once, in order.
        let mut keys = node.key_arr.clone();
        keys.push(up.clone());
        keys.extend(right.key_arr.iter().cloned());
        assert_eq!(keys, all_keys);
        let mut ptrs = node.child_ptrs.clone();
        ptrs.extend(&right.child_ptrs);
        assert_eq!(ptrs, (100..133).collect::<Vec<u32>>());
        node.serialize().expect("the left half serializes");
        right.serialize().expect("the right half serializes");
    }

    /// The reproducer's leaf: 16 narrow 9-byte entries, then 16 wide 255-byte ones (4224 bytes).
    fn d225_mixed_leaf() -> BPlusTreeLeafPage<(Value, Value), ()> {
        let mut leaf = BPlusTreeLeafPage::<(Value, Value), ()>::new(1);
        for pk in 1..=16 {
            leaf.insert_entry((Value::Varchar("a".into()), Value::Integer(pk)), ());
        }
        for pk in 17..=32 {
            leaf.insert_entry((Value::Varchar("b".repeat(247)), Value::Integer(pk)), ());
        }
        leaf
    }

    /// The internal reproducer: 17 narrow 9-byte keys, 15 wide 268-byte ones, 33 children.
    fn d225_mixed_node() -> BPlusTreeInternalPage<(Value, Value)> {
        let mut node = BPlusTreeInternalPage::<(Value, Value)>::new(1);
        for pk in 1..=17 {
            node.key_arr.push((Value::Varchar("a".into()), Value::Integer(pk)));
        }
        for pk in 18..=32 {
            node.key_arr.push((Value::Varchar("b".repeat(260)), Value::Integer(pk)));
        }
        node.child_ptrs = (100..133).collect();
        node.num_keys = 32;
        node
    }

    /// **D225 — `MAX_ENTRY_BYTES` is exactly the largest entry a split always admits.**
    ///
    /// Both sides of the bound, from the proof in its doc. Two 2034-byte entries hold 4068 bytes,
    /// one under the 4069-byte body, so the leaf is not full. A new entry between them has a cut at
    /// 2034 bytes, and none at 2035, because either half holding it is then 4069 bytes, which is
    /// full. A `(Varchar, Integer)` key with a `()` value is `len + 8` bytes for `len` characters.
    #[test]
    fn max_entry_bytes_is_tight() {
        assert_eq!(MAX_ENTRY_BYTES, 2034, "(4096 - 27) / 2");
        let entry = |c: &str, len: usize| (Value::Varchar(c.repeat(len)), Value::Integer(1));
        for (middle, cut_exists) in [(2026usize, true), (2027, false)] {
            let mut leaf = BPlusTreeLeafPage::<(Value, Value), ()>::new(1);
            leaf.insert_entry(entry("a", 2026), ());
            leaf.insert_entry(entry("c", 2026), ());
            assert!(!leaf.is_full(), "premise: 2 x 2034 = 4068 bytes is under the body");
            leaf.insert_entry(entry("b", middle), ());
            assert_eq!(d225_bytes(&leaf.key_arr[1]), middle + 8, "premise: the new entry sits in the middle");
            assert!(leaf.is_full(), "premise: the third entry makes the leaf full");
            match leaf.split_point() {
                Some(mid) => {
                    assert!(cut_exists, "a {}-byte entry found cut {mid}: the bound is not tight", middle + 8);
                    let (_, right) = leaf.split_at(mid, 2).expect("split_point's own cut");
                    assert!(!leaf.is_full() && !right.is_full());
                }
                None => assert!(!cut_exists, "a {}-byte entry, within the bound, found no cut", middle + 8),
            }
        }
    }

    /// **D225 — the proof, exercised: once one admitted entry lands in a leaf that was not full,
    /// a cut exists and `split_at` accepts it.**
    ///
    /// 20000 leaves from a fixed LCG. Each is filled with entries of 12 to 2034 bytes up to a
    /// random total between 2034 and 4068, so it is not full, and then takes one more entry of up
    /// to `MAX_ENTRY_BYTES` at a random position: first, last or between. Every leaf that is then
    /// full must have a cut, and both halves must come out under the threshold. The floor on how
    /// many split is there so a fixture that stopped splitting fails rather than passes.
    #[test]
    fn every_admitted_entry_leaves_a_cut() {
        let mut state: u64 = 0xd225;
        let mut next = move |bound: u64| -> u64 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) % bound
        };
        // A 4-byte key is 8 bytes as stored and a value 4 + its length, so an entry is 12 + len.
        let widths = (MAX_ENTRY_BYTES - 12 + 1) as u64;
        let mut splits = 0;
        for trial in 0..20_000 {
            let mut leaf = BPlusTreeLeafPage::<Vec<u8>, Vec<u8>>::new(1);
            let target = 2034 + next(2035) as usize;
            let (mut total, mut last_key) = (0usize, 0u32);
            loop {
                let size = 12 + next(widths) as usize;
                if total + size > target {
                    break;
                }
                last_key += 2; // even keys, so an odd one can go anywhere between them
                leaf.insert_entry(last_key.to_be_bytes().to_vec(), vec![0u8; size - 12]);
                total += size;
            }
            assert!(!leaf.is_full(), "premise: trial {trial} built a full leaf");
            let size = 12 + next(widths) as usize;
            let at = 2 * next(last_key as u64 / 2 + 1) as u32 + 1;
            leaf.insert_entry(at.to_be_bytes().to_vec(), vec![0u8; size - 12]);
            if !leaf.is_full() {
                continue;
            }
            splits += 1;
            let mid = leaf.split_point().unwrap_or_else(|| {
                panic!(
                    "trial {trial}: {} bytes in {} entries after a {size}-byte insert has no cut",
                    leaf.payload_len(),
                    leaf.key_arr.len()
                )
            });
            let (_, right) = leaf.split_at(mid, 2).unwrap_or_else(|e| panic!("trial {trial}: {e}"));
            assert!(!leaf.is_full() && !right.is_full(), "trial {trial}: a half is at or over the page");
        }
        assert!(splits > 1_000, "only {splits} of 20000 trials needed a split; the fixture tests nothing");
    }

    /// **D225 — a page whose entries all have one width splits exactly where the count split did.**
    ///
    /// This is what leaves every INTEGER-, BIGINT- and TIMESTAMP-keyed tree, and every test that
    /// measured one, unchanged. For one width the best-balanced cut ties at most two ways, and the
    /// tie goes to `len / 2`. Every size a tree reaches: up to 370 11-byte leaf entries (369 is the
    /// most a leaf holds without being full) and up to 453 5-byte internal keys (452 likewise).
    #[test]
    fn a_fixed_width_page_splits_where_the_count_split_did() {
        for n in 2..=370usize {
            let mut leaf = BPlusTreeLeafPage::<Value, RecordId>::new(1);
            for i in 0..n {
                leaf.insert_entry(Value::Integer(i as i32), RecordId::new(i as u32, 0));
            }
            assert_eq!(leaf.split_point(), Some(n / 2), "a leaf of {n} fixed-width entries");
        }
        for n in 1..=453usize {
            let mut node = BPlusTreeInternalPage::<Value>::new(1);
            node.key_arr = (0..n).map(|i| Value::Integer(i as i32)).collect();
            node.child_ptrs = (0..=n as u32).collect();
            node.num_keys = n as u16;
            assert_eq!(node.split_point(), Some(n / 2), "an internal node of {n} fixed-width keys");
        }
    }

    /// **D225, review 7 K4 (M4b): a leaf's tie goes to the count midpoint.** Entries of 30, 10,
    /// 10, 10 and 10 bytes (`(Varchar, Integer)`, 3 + len + 5, sorted by the string). Cut 1 leaves
    /// 30 | 40 and cut 2 leaves 40 | 30, both 10 bytes apart; `n / 2` = 2 breaks the tie. Without
    /// the tie-break the first of the two, cut 1, wins. The fixed-width test cannot see this: for
    /// one width the tied cuts sit either side of `n / 2` only when `n` is odd, and there the
    /// first of them IS `n / 2`.
    #[test]
    fn a_leaf_tie_goes_to_the_count_midpoint() {
        let mut leaf = BPlusTreeLeafPage::<(Value, Value), ()>::new(1);
        leaf.insert_entry((Value::Varchar("a".repeat(22)), Value::Integer(1)), ());
        for (pk, c) in [(2, "b"), (3, "c"), (4, "d"), (5, "e")] {
            leaf.insert_entry((Value::Varchar(c.repeat(2)), Value::Integer(pk)), ());
        }
        let sizes: Vec<usize> =
            leaf.key_arr.iter().zip(&leaf.vals).map(|(k, v)| serialized_len(k) + serialized_len(v)).collect();
        assert_eq!(sizes, vec![30, 10, 10, 10, 10], "premise: the entry widths");
        assert_eq!(leaf.split_point(), Some(2), "a tie must go to the count midpoint");
    }

    /// **D225's guard, forced to fire: a cut that leaves a half full is refused, and the page is
    /// left exactly as it was.** The cut is the count split's own, on both reproducer pages.
    #[test]
    fn split_at_refuses_a_cut_that_leaves_a_half_full_and_restores_the_page() {
        let mut leaf = d225_mixed_leaf();
        let before = (leaf.key_arr.clone(), leaf.vals.clone(), leaf.next, leaf.num_keys);
        let e = leaf.split_at(16, 2).expect_err("the count cut leaves 4080 bytes on the right");
        assert!(e.to_string().contains("would leave a half at or over"), "not the D225 guard: {e}");
        assert_eq!((leaf.key_arr.clone(), leaf.vals.clone(), leaf.next, leaf.num_keys), before, "the refusal changed the leaf");
        assert_eq!(leaf.split_point(), Some(24), "the restored leaf must still split at its byte cut");
        leaf.split_at(24, 2).expect("the byte cut is accepted");

        let mut node = d225_mixed_node();
        let before = (node.key_arr.clone(), node.child_ptrs.clone(), node.num_keys);
        let e = node.split_at(16, 2).expect_err("the count cut leaves 4084 bytes on the right");
        assert!(e.to_string().contains("would leave a half at or over"), "not the D225 guard: {e}");
        assert_eq!((node.key_arr.clone(), node.child_ptrs.clone(), node.num_keys), before, "the refusal changed the node");
        assert_eq!(node.split_point(), Some(24), "the restored node must still split at its byte cut");
        node.split_at(24, 2).expect("the byte cut is accepted");

        // A cut outside the page is refused rather than indexing past it.
        assert!(d225_mixed_leaf().split_at(0, 2).is_err(), "a cut leaving the left half empty");
        assert!(d225_mixed_leaf().split_at(32, 2).is_err(), "a cut past the last entry");
        assert!(d225_mixed_node().split_at(32, 2).is_err(), "pushing up a key that does not exist");
    }

    /// **`serialize` refuses a page over its body instead of panicking**: the last line of D225's
    /// defence, and the panic `d208_review2` A1 reached. The boundary is exact: a body of exactly
    /// the page serializes (an earlier build could leave one, full but legal), one byte more is
    /// refused.
    #[test]
    fn serialize_refuses_an_oversized_page_rather_than_panicking() {
        let wide = |pk: i32, len: usize| (Value::Varchar("b".repeat(len)), Value::Integer(pk));
        // Leaf body 4069: 15 x 255 + one of 244 (3 + 236 + 5) fits exactly; 245 is one over.
        for (last_len, fits) in [(236usize, true), (237, false)] {
            let mut leaf = BPlusTreeLeafPage::<(Value, Value), ()>::new(7);
            for pk in 0..15 {
                leaf.insert_entry(wide(pk, 247), ());
            }
            leaf.insert_entry(wide(99, last_len), ());
            let body = 15 * 255 + last_len + 8;
            match leaf.serialize() {
                Ok(_) => assert!(fits, "a {body}-byte leaf body serialized past the page"),
                Err(e) => {
                    assert!(!fits, "a {body}-byte leaf body, which fits, was refused: {e}");
                    assert!(e.to_string().contains(&format!("leaf page 7 holds {body} bytes")), "{e}");
                }
            }
        }
        // Internal body 4077: 14 x 268 + one of 261 (3 + 253 + 5) + 16 pointers fits exactly.
        for (last_len, fits) in [(253usize, true), (254, false)] {
            let mut node = BPlusTreeInternalPage::<(Value, Value)>::new(7);
            for pk in 0..14 {
                node.key_arr.push(wide(pk, 260));
            }
            node.key_arr.push(wide(99, last_len));
            node.child_ptrs = (1..=16).collect();
            node.num_keys = 15;
            let body = 14 * 268 + last_len + 8 + 16 * 4;
            match node.serialize() {
                Ok(_) => assert!(fits, "a {body}-byte internal body serialized past the page"),
                Err(e) => {
                    assert!(!fits, "a {body}-byte internal body, which fits, was refused: {e}");
                    assert!(e.to_string().contains(&format!("internal page 7 holds {body} bytes")), "{e}");
                }
            }
        }
    }

    /// **The entry bound, at the byte, on both halves of an entry.** A secondary key under `()`,
    /// and a primary key under its 6-byte `RecordId`: the value counts.
    #[test]
    fn admit_entry_refuses_one_byte_over_the_bound_by_name() {
        let at = (Value::Varchar("x".repeat(2026)), Value::Integer(1));
        let over = (Value::Varchar("x".repeat(2027)), Value::Integer(1));
        assert_eq!((d225_bytes(&at), d225_bytes(&over)), (2034, 2035), "premise: 3 + len + 5");
        admit_entry(&at, &()).expect("an entry exactly at the bound is admitted");
        let e = admit_entry(&over, &()).expect_err("one byte over is refused");
        let msg = e.to_string();
        assert!(
            msg.contains("index entry too large: 2035 bytes") && msg.contains("MAX_ENTRY_BYTES = 2034"),
            "not the named refusal: {msg}"
        );
        assert!(matches!(e, FerroError::Constraint(_)), "a statement error, not an internal one: {e:?}");

        admit_entry(&Value::Varchar("x".repeat(2025)), &RecordId::new(1, 1)).expect("3 + 2025 + 6 = 2034");
        admit_entry(&Value::Varchar("x".repeat(2026)), &RecordId::new(1, 1)).expect_err("3 + 2026 + 6 = 2035");
    }
}