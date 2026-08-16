use std::fmt::{ Display, Formatter, Result };
use std::error;
//add more types
#[derive(Debug)]
pub enum FerroError {
    Parse(String),
    Io(String),
    NotEnoughSpace,
    SlotDeleted,
    KeyNotFound,
    EmptyList,
    PagePinned,
    SqlParseError(String),
    IndexAlreadyExists,
    Contraint(String),
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
}

impl Display for FerroError {
    fn fmt(&self, f: &mut Formatter<'_> ) -> Result {
        match self {
            FerroError::Parse(e) => write!(f, "parsing error: {}", e),
            FerroError::Io(e) => write!(f, "io error: {}", e),
            FerroError::NotEnoughSpace => write!(f, "not enough space in page"),
            FerroError::SlotDeleted => write!(f, "the slot is delted"),
            FerroError::KeyNotFound => write!(f, "key wasn't found"),
            FerroError::EmptyList => write!(f, "linked hash set is empty"),
            FerroError::PagePinned => write!(f, "page is pinned"),
            FerroError::SqlParseError(s) => write!(f, "sql parsing error: {}", s),
            FerroError::IndexAlreadyExists => write!(f, "index already exists"),
            FerroError::Contraint(s) => write!(f, "contraint error: {}", s),
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