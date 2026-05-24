use std::fmt::{ Display, Formatter, Result };
use std::error;

//add more types
#[derive(Debug)]
pub enum FerroError {
    Parse(String),
    Io(String),
    NotEnoughSpace,
    SlotDeleted,
}

impl Display for FerroError {
    fn fmt(&self, f: &mut Formatter<'_> ) -> Result {
        match self {
            FerroError::Parse(e) => write!(f, "parsing error: {}", e),
            FerroError::Io(e) => write!(f, "io error: {}", e),
            FerroError::NotEnoughSpace => write!(f, "not enough space in page"),
            FerroError::SlotDeleted => write!(f, "the slot is delted"),
            
        }
    }
}

impl error::Error for FerroError {}