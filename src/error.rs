use std::io;
use std::fmt::{ Display, Formatter, Result };
use std::error;

//add more types
#[derive(Debug)]
pub enum FerroError {
    Parse(String),
    Io(String),
}

impl Display for FerroError {
    fn fmt(&self, f: &mut Formatter<'_> ) -> Result {
        match self {
            FerroError::Parse(e) => write!(f, "parsing error: {}", e),
            FerroError::Io(e) => write!(f, "io error: {}", e)
        }
    }
}

impl error::Error for FerroError {}