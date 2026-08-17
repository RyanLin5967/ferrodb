use std::fmt::{ Display, Formatter, Result };
use std::error;
//add more types
#[derive(Debug)]
pub enum FerroError {
    // `Parse(String)` was here and is deliberately gone — E71.
    //
    // It had 41 construction sites and, once each was classified by who could cause it, **not one of
    // them was a parse failure**. Real SQL problems have always used `SqlParseError`. What `Parse`
    // actually held was arithmetic a user's expression could not evaluate, invariants this code is
    // supposed to maintain, on-disk corruption, and a handful of catalog lookups that belonged in
    // `Bind` (E67 moved most of those; `planner::plan` still had two).
    //
    // Removed rather than left empty. A variant meaning "one of four unrelated things" is how the
    // grab-bag formed in the first place, and leaving it available is an invitation to refill it.
    Io(String),
    NotEnoughSpace,
    SlotDeleted,
    KeyNotFound,
    EmptyList,
    PagePinned,
    SqlParseError(String),
    IndexAlreadyExists,
    Constraint(String),
    OnlyDML,
    Bind(String),
    Wal(String),
    Txn(String),
    // agent-isolation layer
    Branch(String),
    Cow(String),
    Merge(String),
    /// A cell has no value here — distinct from a failed read of one.
    ///
    /// Merge needs to tell "the LCA never had this cell" (expected for a row this merge is
    /// creating) from "reading the LCA failed" (a real fault). Collapsing both into `Merge`
    /// meant `.ok()` at the call site silently turned a disk failure into "no LCA value".
    CellAbsent(String),
    Provenance(String),

    // ---- E71: the three classes that used to share `Parse` -------------------------------------
    //
    // `Parse` had 41 construction sites covering four unrelated failure classes: real SQL problems,
    // arithmetic a user's expression could not evaluate, invariants this code is supposed to maintain,
    // and bytes on disk that are not what they should be. All four rendered as `parsing error:`, so
    // nothing reading logs by class could tell a typo from a corrupted catalog page.
    //
    // The last one is why this matters beyond tidiness: a corruption reported as a parse error is a
    // corruption nobody pages on.
    /// A user's expression could not be evaluated — division by zero, arithmetic on a non-number, an
    /// overflow. The statement is well-formed; the values are the problem.
    Eval(String),
    /// An invariant this code is supposed to maintain did not hold. Not caused by input or by disk.
    ///
    /// These are the arms that are unreachable given the caller — a `compare` reached with an operator
    /// that is not a comparison, a row shorter than its own schema. They were `Parse`, which told the
    /// reader their SQL was malformed when the fault is in ferrodb.
    Internal(String),
    /// Bytes read back from disk are not what was written: a non-UTF8 stored name, an unknown tag, a
    /// record that ends mid-field.
    ///
    /// Deliberately loud in `Display`. Every other error here describes something a caller did; this one
    /// describes damage, and it must not read like a syntax complaint.
    Corruption(String),
}

impl Display for FerroError {
    fn fmt(&self, f: &mut Formatter<'_> ) -> Result {
        match self {
            FerroError::Eval(e) => write!(f, "evaluation error: {}", e),
            FerroError::Internal(e) => write!(f, "internal error (this is a bug in ferrodb): {}", e),
            FerroError::Corruption(e) => write!(f, "DATA CORRUPTION: {}", e),
            FerroError::Io(e) => write!(f, "io error: {}", e),
            FerroError::NotEnoughSpace => write!(f, "not enough space in page"),
            FerroError::SlotDeleted => write!(f, "the slot is delted"),
            FerroError::KeyNotFound => write!(f, "key wasn't found"),
            FerroError::EmptyList => write!(f, "linked hash set is empty"),
            FerroError::PagePinned => write!(f, "page is pinned"),
            FerroError::SqlParseError(s) => write!(f, "sql parsing error: {}", s),
            FerroError::IndexAlreadyExists => write!(f, "index already exists"),
            FerroError::Constraint(s) => write!(f, "constraint error: {}", s),
            FerroError::OnlyDML => write!(f, "only supports dml"),
            FerroError::Bind(s) => write!(f, "binding error: {}", s),
            FerroError::Wal(s) => write!(f, "wal error: {}", s),
            FerroError::Txn(s) => write!(f, "txn error: {}", s),
            FerroError::Branch(s) => write!(f, "branch error: {}", s),
            FerroError::Cow(s) => write!(f, "cow page store error: {}", s),
            FerroError::Merge(s) => write!(f, "merge error: {}", s),
            FerroError::CellAbsent(s) => write!(f, "cell absent: {}", s),
            FerroError::Provenance(s) => write!(f, "provenance error: {}", s),
        }
    }
}

impl error::Error for FerroError {}