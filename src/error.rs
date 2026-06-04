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
        }
    }
}

impl error::Error for FerroError {}