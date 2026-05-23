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
                        DataType::Varchar(c) => {
                            bytes.resize(bytes.len() + 1, 0);
                        },
                    }
                }
            }
        }
        Ok(bytes)
    }

    pub fn deserialize(&self, bytes: &[u8], schema: &Schema) -> Result<Vec<Value>, FerroError>  {
        let mut values: Vec<Value> = Vec::new();
        let mut offset: usize = 0;

        let bitmap_len = (schema.columns.len() + 7)/8;
        let bitmap = &bytes[offset..bitmap_len];
        
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
                    values.push(Value::Boolean(bytes[offset] != 0));
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
                    let float_bytes = &bytes[offset..offset+8];
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
                    let int_bytes = &bytes[offset..offset+4];
                    let int = i32::from_be_bytes(int_bytes.try_into().unwrap());
                    values.push(Value::Integer(int));
                    offset += 4;
                },
                DataType::Varchar(_) => {
                    let len_bytes = &bytes[offset..offset + 1];
                    let len = u8::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
                    offset += 1;
                    if (bitmap[i/8] & (1 << i % 8)) != 0 {
                        values.push(Value::Null);
                        offset += len;
                        continue;
                    }
                    let str_bytes = &bytes[offset..offset + len];
                    values.push(Value::Varchar(std::str::from_utf8(str_bytes).map(|s| s.to_string()).unwrap()));
                    offset += len;
                },
            }
        }
        Ok(values)
    }

}
pub fn get_padding(align: usize, buff_size: usize) -> usize {
    return (align - (buff_size & (align - 1))) & (align - 1)
}