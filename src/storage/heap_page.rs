use crate::catalog::schema::Schema;
use crate::catalog::column::{DataType, Value};
use crate::error::FerroError;
pub struct Tuple {
    null_bitmap: Vec<u8>,
    data: Vec<u8>
}

pub struct SlotEntry {
    offset: u16,
    length: u16
}

pub struct Page {
    page_id: u32,
    num_slots: u16,
    slot_arr: Vec<SlotEntry>,
    tuples: Vec<u8>
}

impl Tuple {

    pub fn serialize(&self, values: &[Value], schema: &Schema) -> Result<Vec<u8>, FerroError>{
        if values.len() != schema.columns.len() {
            return Err(FerroError::Parse(String::from("values is not the same length as columns")))
        }
        let mut null_bitmap = vec![0u8; (schema.columns.len() + 7)/8];
        let mut bytes: Vec<u8> = Vec::new();
        // fill bitmap
        for (i, value) in values.iter().enumerate() {
            if values[i] == Value::Null {
                let byte_index = i/8;
                let bit_index = i%8;
                null_bitmap[byte_index] |= 1 << bit_index;
            }
        }
        bytes.extend_from_slice(&null_bitmap);
        // add serialized values + padding between them (no padding between tuples)
        // formula: padding = (align - (len & (align - 1))) & (align - 1)
        // or (align - (len % align)) % align
        for (i , value) in values.iter().enumerate() {
            match value {
                Value::Boolean(b) => {
                    let align = 1;
                    let padding = (align - (bytes.len() & (align - 1))) & (align - 1);
                    bytes.resize(bytes.len() + padding, 0);
                    bytes.push(*b as u8);

                },
                Value::Float(f) => {
                    let align = 8;
                    let padding = (align - (bytes.len() & (align - 1))) & (align - 1);
                    bytes.resize(bytes.len() + padding, 0);
                    bytes.extend_from_slice(&f.to_be_bytes());
                },
                Value::Integer(i) => {
                    let align = 4;
                    let padding = (align - (bytes.len() & (align - 1))) & (align - 1);
                    bytes.resize(bytes.len() + padding, 0);
                    bytes.extend_from_slice(&i.to_be_bytes());
                },
                // use pascal string, doesn't need padding
                Value::Varchar(c) => {
                    let str_bytes = c.as_bytes();
                    bytes.push(str_bytes.len() as u8);
                    bytes.extend_from_slice(str_bytes);
                },
                Value::Null => {
                    let data_type = &schema.columns[i].data_type;
                    match data_type {
                        DataType::Boolean => {
                            let align = 1;
                            let padding = (align - (bytes.len() & (align - 1))) & (align - 1);
                            bytes.resize(bytes.len() + padding, 0);
                            bytes.push(0u8);
                        },
                        DataType::Float => {
                            let align = 8;
                            let padding = (align - (bytes.len() & (align - 1))) & (align - 1);
                            bytes.resize(bytes.len() + padding, 0);
                            bytes.extend_from_slice(&[0u8; 8]);
                        },
                        DataType::Integer => {
                            let align = 4;
                            let padding = (align - (bytes.len() & (align - 1))) & (align - 1);
                            bytes.resize(bytes.len() + padding, 0);
                            bytes.extend_from_slice(&[0u8; 4]);
                        },
                        DataType::Varchar(c) => {
                            bytes.resize(bytes.len() + 1, 0);
                        },
                    }
                }
            }
        }
        Ok(bytes)
    }

    // have to make sure byte order is same as schema
    pub fn deserialize(&self, bytes: &[u8], schema: &Schema) -> Result<Vec<Value>, FerroError>  {
        let values: Vec<Value> = Vec::new();
        

        Ok(values)
    }
}