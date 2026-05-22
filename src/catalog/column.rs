pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool
}

// add support for more later
pub enum DataType {
    Integer,
    Float, 
    Varchar(u16),
    Boolean,
}

#[derive(PartialEq)]
pub enum Value {
    Integer(i32),
    Float(f64),
    Varchar(String),
    Boolean(bool),
    Null
}