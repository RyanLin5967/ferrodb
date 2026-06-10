use crate::{catalog::{column::{DataType, Value}, schema::Schema}, error::FerroError, parser::{scanner::TokenType}};

pub enum BoundExpr {
    BinaryOp {
        left: Box<BoundExpr>,
        operator: TokenType,
        right: Box<BoundExpr>
    },
    UnaryOp {
        operator: TokenType,
        right: Box<BoundExpr>
    },
    Literal(Value),
    ColumnRef(usize),
}

pub struct BoundColumn {
    pub qualifier: String, 
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

pub struct Scope {
    pub columns: Vec<BoundColumn>, // index = offset in combined row
}

impl Scope {
    pub fn new() -> Self{
        Self {columns: Vec::new()}
    }

    // adds table's cols using column's qualifier
    pub fn add_table(&mut self, qualifier: &str, schema: &Schema) -> Result<(), FerroError> {
        let cols = &schema.columns;
        if self.columns.iter().any(|c| c.qualifier == qualifier) {
            return Err(FerroError::Bind(format!("duplicate table/alias: {}", qualifier)))
        }
        for i in 0..cols.len() {
            self.columns.push(BoundColumn{qualifier: qualifier.into(), name: cols[i].name.clone(), data_type: cols[i].data_type.clone(), nullable: cols[i].nullable});
        }
        Ok(())
    }

    // qualified (Some) or bare (None) column -> index, checks for unknown table/column
    pub fn resolve(&self, table: Option<&str>, column: &str) -> Result<usize, FerroError>{
        if table.is_some() {
            if let Some(idx) = self.columns.iter().position(|c| c.qualifier == table.expect("") && c.name == column) {
                Ok(idx)
            } else {
                return Err(FerroError::Bind("unknown column".into()))
            }
        } else {
            let mut found = None;
            for (i, col) in self.columns.iter().enumerate() {
                if col.name == column {
                    if found.is_some() {
                        return Err(FerroError::Bind("ambiguous column".into()))
                    } 
                    found = Some(i)
                }
            }
            found.ok_or(FerroError::Bind("not found".into()))
        }
    }

    // '*' (None) -> all indices, 'q.*' (Some) -> that table's indicies
    pub fn expand_star(&self, qualifier: Option<&str>) -> Result<Vec<usize>, FerroError>{
        match qualifier {
            Some(q) => {
                let mut indexes = Vec::new();
                for (i, col) in self.columns.iter().enumerate() {
                    if col.qualifier == q {
                        indexes.push(i);
                    }
                }
                if indexes.is_empty() {
                    return Err(FerroError::Bind(format!("unknown table/alias: {}", q)));
                }
                Ok(indexes)
            }
            None => {
                return Ok((0..self.columns.len()).collect())
            }
        }
    }
}

