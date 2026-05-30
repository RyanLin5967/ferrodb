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

#[derive(Debug, Clone)]
pub enum Value {
    Integer(i32),
    Float(f64),
    Varchar(String),
    Boolean(bool),
    Null
}

impl Column {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Column {name, data_type, nullable}
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Varchar(a), Value::Varchar(b)) => a.cmp(b),
            (Value::Null, Value::Null) => Ordering::Equal, 
            (Value::Null, _) => Ordering::Less, // so nulls are first
            (_, Value::Null) => Ordering::Greater,
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for Value {}