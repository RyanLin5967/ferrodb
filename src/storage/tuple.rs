use crate::catalog::schema::Schema;
use crate::catalog::column::{DataType, Value};
use crate::error::FerroError;

pub struct Tuple {
    pub data: Vec<u8>
}

pub const VERSION_HEADER_SIZE: usize = 24;
// |begin_ts (8)|end_ts (8)|prev_page (4)|prev_slot (2)|reserved (2)|
pub struct VersionHeader {
    pub begin_ts: u64,
    pub end_ts: u64,
    pub prev_page: u32,
    pub prev_slot: u16,
}

impl Tuple {
    pub fn new(data: Vec<u8>) -> Self{
        Tuple {data}
    }
    pub fn serialize(values: &[Value], schema: &Schema, begin_ts: u64) -> Result<Self, FerroError>{
        if values.len() != schema.columns.len() {
            return Err(FerroError::Internal(format!("serialize was given {} values for a {}-column schema; a tuple written against the wrong schema is unreadable later", values.len(), schema.columns.len())))
        }
        let mut null_bitmap = vec![0u8; (schema.columns.len() + 7)/8];
        let mut bytes: Vec<u8> = Vec::new();
        // fill bitmap
        for (i, _) in values.iter().enumerate() {
            if values[i] == Value::Null {
                let byte_index = i/8;
                let bit_index = i%8;
                null_bitmap[byte_index] |= 1 << bit_index;
            }
        }
        bytes.extend_from_slice(&begin_ts.to_be_bytes());
        bytes.extend_from_slice(&0u64.to_be_bytes());
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        // A value whose variant disagrees with its column's declared type is not a warning, it is
        // silent corruption: `serialize` picks the width from the VALUE and `deserialize` picks it
        // from the SCHEMA, so a Varchar written into a BIGINT column lays down a 2-byte length
        // prefix where the reader will take 8 bytes of whatever follows. Every column after it in
        // the row then reads at the wrong offset. Refuse instead.
        for (i, value) in values.iter().enumerate() {
            if !value_fits(value, &schema.columns[i].data_type) {
                return Err(FerroError::Constraint(format!(
                    "column '{}' is declared {:?} but was given {:?}",
                    schema.columns[i].name, schema.columns[i].data_type, value
                )))
            }
        }
        bytes.extend_from_slice(&null_bitmap);
        // add serialized values + padding between them (no padding between tuples)
        // formula: padding = (align - (len & (align - 1))) & (align - 1)
        // or (align - (len % align)) % align
        for (i , value) in values.iter().enumerate() {
            match value {
                Value::Boolean(b) => {
                    let padding = get_padding(1, bytes.len());
                    bytes.resize(bytes.len() + padding, 0);
                    bytes.push(*b as u8);

                },
                Value::Float(f) => {
                    let padding = get_padding(8, bytes.len());
                    bytes.resize(bytes.len() + padding, 0);
                    bytes.extend_from_slice(&f.to_be_bytes());
                },
                Value::Integer(i) => {
                    let padding = get_padding(4, bytes.len());
                    bytes.resize(bytes.len() + padding, 0);
                    bytes.extend_from_slice(&i.to_be_bytes());
                },
                // BIGINT and TIMESTAMP are both a fixed 8-byte big-endian i64. They are stored
                // full width rather than narrowed to whatever the current row happens to fit in:
                // a width that depends on the value is a width the reader cannot know.
                Value::BigInt(v) => {
                    let padding = get_padding(8, bytes.len());
                    bytes.resize(bytes.len() + padding, 0);
                    bytes.extend_from_slice(&v.to_be_bytes());
                },
                Value::Timestamp(ms) => {
                    let padding = get_padding(8, bytes.len());
                    bytes.resize(bytes.len() + padding, 0);
                    bytes.extend_from_slice(&ms.to_be_bytes());
                },
                // DECIMAL is digit text, so it reuses the pascal-string layout VARCHAR already
                // uses rather than inventing a second variable-length encoding.
                Value::Decimal(d) => {
                    let str_bytes = d.as_bytes();
                    if str_bytes.len() > u16::MAX as usize {
                        return Err(FerroError::Constraint(format!("decimal is {} bytes, over the {} the length prefix can hold", str_bytes.len(), u16::MAX)))
                    }
                    bytes.extend_from_slice(&(str_bytes.len() as u16).to_be_bytes());
                    bytes.extend_from_slice(str_bytes);
                },
                // use pascal string, doesn't need padding
                Value::Varchar(c) => {
                    let str_bytes = c.as_bytes();
                    if let DataType::Varchar(max) = &schema.columns[i].data_type {
                        if str_bytes.len() > *max as usize {
                            return Err(FerroError::Constraint(format!("varchar exceeds declared len: {}", max)))
                        }
                    }
                    bytes.extend_from_slice(&(str_bytes.len() as u16).to_be_bytes());
                    bytes.extend_from_slice(str_bytes);
                },
                Value::Null => {
                    let data_type = &schema.columns[i].data_type;
                    match data_type {
                        DataType::Boolean => {
                            let padding = get_padding(1, bytes.len());
                            bytes.resize(bytes.len() + padding, 0);
                            bytes.push(0u8);
                        },
                        DataType::Float => {
                            let padding = get_padding(8, bytes.len());
                            bytes.resize(bytes.len() + padding, 0);
                            bytes.extend_from_slice(&[0u8; 8]);
                        },
                        DataType::Integer => {
                            let padding = get_padding(4, bytes.len());
                            bytes.resize(bytes.len() + padding, 0);
                            bytes.extend_from_slice(&[0u8; 4]);
                        },
                        DataType::BigInt | DataType::Timestamp => {
                            let padding = get_padding(8, bytes.len());
                            bytes.resize(bytes.len() + padding, 0);
                            bytes.extend_from_slice(&[0u8; 8]);
                        },
                        DataType::Decimal | DataType::Varchar(_) => {
                            bytes.extend_from_slice(&[0u8; 2]);
                        },
                    }
                }
            }
        }
        Ok(Tuple {data: bytes})
    }

    pub fn deserialize(&self, schema: &Schema) -> Result<Vec<Value>, FerroError>  {
        let mut values: Vec<Value> = Vec::new();
        let mut offset: usize = VERSION_HEADER_SIZE;

        let bitmap_len = (schema.columns.len() + 7)/8;
        let bitmap = &self.data[offset..bitmap_len + offset];
        
        offset += bitmap_len;

        for (i, column) in schema.columns.iter().enumerate() {
            let data_ty = &column.data_type;
            match data_ty {
                DataType::Boolean => {
                    let padding = get_padding(1, offset);
                    offset += padding;
                    if (bitmap[i/8] & (1 << i % 8)) != 0 {
                        values.push(Value::Null);
                        offset += 1;
                        continue;
                    }
                    values.push(Value::Boolean(self.data[offset] != 0));
                    offset += 1;
                },
                DataType::Float => {
                    let padding = get_padding(8, offset);
                    offset += padding;
                    if (bitmap[i/8] & (1 << i % 8)) != 0 {
                        values.push(Value::Null);
                        offset += 8;
                        continue;
                    }
                    let float_bytes = &self.data[offset..offset+8];
                    let float = f64::from_be_bytes(float_bytes.try_into().unwrap());
                    values.push(Value::Float(float));
                    offset += 8;
                },
                DataType::Integer => {
                    let padding = get_padding(4, offset);
                    offset += padding;
                    if (bitmap[i/8] & (1 << i % 8)) != 0 {
                        values.push(Value::Null);
                        offset += 4;
                        continue;
                    }
                    let int_bytes = &self.data[offset..offset+4];
                    let int = i32::from_be_bytes(int_bytes.try_into().unwrap());
                    values.push(Value::Integer(int));
                    offset += 4;
                },
                DataType::BigInt => {
                    let padding = get_padding(8, offset);
                    offset += padding;
                    if (bitmap[i/8] & (1 << i % 8)) != 0 {
                        values.push(Value::Null);
                        offset += 8;
                        continue;
                    }
                    let int_bytes = &self.data[offset..offset+8];
                    values.push(Value::BigInt(i64::from_be_bytes(int_bytes.try_into().unwrap())));
                    offset += 8;
                },
                DataType::Timestamp => {
                    let padding = get_padding(8, offset);
                    offset += padding;
                    if (bitmap[i/8] & (1 << i % 8)) != 0 {
                        values.push(Value::Null);
                        offset += 8;
                        continue;
                    }
                    let ms_bytes = &self.data[offset..offset+8];
                    values.push(Value::Timestamp(i64::from_be_bytes(ms_bytes.try_into().unwrap())));
                    offset += 8;
                },
                DataType::Decimal => {
                    let len_bytes = &self.data[offset..offset + 2];
                    let len = u16::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
                    offset += 2;
                    if (bitmap[i/8] & (1 << i % 8)) != 0 {
                        values.push(Value::Null);
                        offset += len;
                        continue;
                    }
                    let str_bytes = &self.data[offset..offset + len];
                    let text = std::str::from_utf8(str_bytes).map_err(|_| FerroError::Corruption("decimal column held invalid utf8".into()))?;
                    values.push(Value::Decimal(text.to_string()));
                    offset += len;
                },
                DataType::Varchar(_) => {
                    let len_bytes = &self.data[offset..offset + 2];
                    let len = u16::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
                    offset += 2;
                    if (bitmap[i/8] & (1 << i % 8)) != 0 {
                        values.push(Value::Null);
                        offset += len;
                        continue;
                    }
                    let str_bytes = &self.data[offset..offset + len];
                    values.push(Value::Varchar(std::str::from_utf8(str_bytes).map(|s| s.to_string()).unwrap()));
                    offset += len;
                },
            }
        }
        Ok(values)
    }

    pub fn version_header(&self) -> Result<VersionHeader, FerroError> {
        if self.data.len() < VERSION_HEADER_SIZE {
            return Err(FerroError::Io("tuple size < version header size".into()));
        }
        let begin_ts = u64::from_be_bytes(self.data[0..8].try_into().unwrap());
        let end_ts = u64::from_be_bytes(self.data[8..16].try_into().unwrap());
        let prev_page = u32::from_be_bytes(self.data[16..20].try_into().unwrap());
        let prev_slot = u16::from_be_bytes(self.data[20..22].try_into().unwrap());
        Ok(VersionHeader { begin_ts, end_ts, prev_page, prev_slot })
    }
}

impl VersionHeader {
    pub fn prev(&self) -> Option<(u32, u16)> {
        (self.prev_page != 0).then_some((self.prev_page, self.prev_slot))
    }
}

/// Whether a value can be written into a column of this declared type without the on-disk width
/// disagreeing between `serialize` and `deserialize`. NULL fits anything: the null bitmap carries
/// it and the placeholder bytes are sized from the schema.
pub fn value_fits(value: &Value, ty: &DataType) -> bool {
    match (value, ty) {
        (Value::Null, _) => true,
        (Value::Integer(_), DataType::Integer) => true,
        (Value::Float(_), DataType::Float) => true,
        (Value::Varchar(_), DataType::Varchar(_)) => true,
        (Value::Boolean(_), DataType::Boolean) => true,
        (Value::BigInt(_), DataType::BigInt) => true,
        (Value::Decimal(_), DataType::Decimal) => true,
        (Value::Timestamp(_), DataType::Timestamp) => true,
        _ => false,
    }
}

pub fn get_padding(align: usize, buff_size: usize) -> usize {
    return (align - (buff_size & (align - 1))) & (align - 1)
}

#[cfg(test)]
mod tests {

    use crate::catalog::schema::Schema;
    use crate::catalog::column::{Column, DataType, Value};
    use crate::storage::tuple::Tuple;

    /// **A row whose arity does not match its schema is refused.**
    ///
    /// Found by a mutation sweep, not by reading: deleting this check left all 820 tests green. It
    /// sits on a live path — `insert.rs` and `update.rs` both call `serialize` and propagate the
    /// error — and serialising a row with the wrong number of values writes a tuple that the
    /// deserialiser will later read against a schema it does not match, which is corruption that
    /// surfaces far from its cause.
    ///
    /// The binder rejects a wrong-arity `INSERT` before it gets here, so this is a defensive check
    /// on an internal API rather than a live bug. That is exactly the kind that rots: it is only
    /// load-bearing for a caller that does not exist yet, and nothing would have told that caller's
    /// author it had stopped working.
    #[test]
    fn serialising_a_row_that_does_not_match_its_schema_is_refused() {
        let schema = Schema::new(vec![
            Column::new(String::from("id"), DataType::Integer, false),
            Column::new(String::from("v"), DataType::Integer, true),
        ]);
        let short = vec![Value::Integer(1)];
        let long = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];

        assert!(
            Tuple::serialize(&short, &schema, 0).is_err(),
            "a row with too few values was serialised against a 2-column schema"
        );
        assert!(
            Tuple::serialize(&long, &schema, 0).is_err(),
            "a row with too many values was serialised against a 2-column schema"
        );

        // Anti-vacuity: the matching arity serialises, so the refusals are about the count and not
        // about `serialize` rejecting everything.
        let ok = vec![Value::Integer(1), Value::Integer(2)];
        Tuple::serialize(&ok, &schema, 0).expect("a correctly shaped row was refused");
    }
    
    #[test]
    pub fn test_se_and_deserialize() { 
        let columns = vec![
            Column::new(String::from("test1"), DataType::Integer, false),
            Column::new(String::from("test2"), DataType::Float, true),
            Column::new(String::from("test3"), DataType::Varchar(2), false),
            Column::new(String::from("test4"), DataType::Boolean, true)
            ];
        let values = vec![Value::Integer(67), Value::Float(6.7), Value::Varchar(String::from("67")), Value::Boolean(false)];
        let schema = Schema::new(columns);
        let tuple = Tuple::serialize(&values, &schema, 0).unwrap();
        let de_values = Tuple::deserialize(&tuple, &schema).unwrap();
        assert_eq!(values, de_values);
    }

    #[test]
    fn test_null(){
        let columns = vec![
            Column::new(String::from("test1"), DataType::Integer, false),
            Column::new(String::from("test2"), DataType::Float, true),
            Column::new(String::from("test3"), DataType::Varchar(10), false),
            Column::new(String::from("test4"), DataType::Boolean, true)
        ];
        
        let values = vec![
            Value::Integer(42), 
            Value::Null,
            Value::Varchar(String::from("short")),
            Value::Null
        ];
        
        let schema = Schema::new(columns);        
        let tuple = Tuple::serialize(&values, &schema, 0).unwrap();
        let de_values = Tuple::deserialize(&tuple, &schema).unwrap();
        
        assert_eq!(values, de_values);
    }

    #[test]
    fn test_versioned_tuple_roundtrip() {
        let schema = Schema::new(vec![
            Column::new(String::from("i"), DataType::Integer, false), 
            Column::new(String::from("f"), DataType::Float, true),
            Column::new(String::from("s"), DataType::Varchar(10), false)
        ]);
        let values = vec![Value::Integer(5), Value::Null, Value::Varchar("hi".into())];
        let tuple = Tuple::serialize(&values, &schema, 1).unwrap();
        let h = tuple.version_header().unwrap();
        assert_eq!((h.begin_ts, h.end_ts, h.prev()), (1, 0, None));
        assert_eq!(tuple.deserialize(&schema).unwrap(), values);
    }
}