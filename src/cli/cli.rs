use std::{fs::OpenOptions, io, path::Path, sync::Arc};
use std::io::Write;
use crate::execution::executor::run;
use crate::execution::session::Session;
use crate::parser::parser::Parser;
use crate::parser::scanner::Scanner;
use crate::wal::log::WalManager;
use crate::wal::recovery::{rebuild_indexes, recover};
use crate::wal::txn::TxnManager;
use crate::{buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, column::Value}, error::FerroError, execution::executor::Outcome, storage::disk_manager::DiskManager};
const FIRST_CATALOG_PAGE_ID: u32 = 1;

// super basic cli, make better later
pub fn run_cli(db_path: &str) -> Result<(), FerroError> {
    let existed = Path::new(db_path).exists();
    let file = OpenOptions::new().read(true).write(true).create(true).open(db_path).map_err(|e|FerroError::Io(e.to_string()))?;
    let dm = Arc::new(DiskManager::new(file)?);
    let bp = Arc::new(BufferPoolManager::new(dm));
    let wal = Arc::new(WalManager::new(format!("{}.wal", db_path).into())?);
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    let mut session = Session::new();
    bp.attach_wal(wal.clone());    
    let recovered = recover(&txn)?;
    let mut catalog = if existed {
        Catalog::open(bp.clone(), FIRST_CATALOG_PAGE_ID)?
    } else {
        Catalog::create(bp.clone())?
    };
    if recovered {
        rebuild_indexes(&mut catalog, &bp)?;
        txn.checkpoint()?;
    }
    println!("ferrodb: type .exit to quit");
    let stdin = io::stdin();
    let mut buffer = String::new();

    loop {
        print!("{}", if buffer.trim().is_empty() {"ferrodb=> "} else {"     ...? "});
        io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        if buffer.trim().is_empty() {
            let t = line.trim();
            if t == ".exit" {break;}
        }
        buffer.push_str(&line);

        if let Some(pos) = buffer.rfind(';') {
            let complete = buffer[..=pos].to_string();
            buffer = buffer[pos + 1..].to_string();
            execute_sql(&complete, &mut catalog, bp.clone(), txn.clone(), &mut session);
        }
    }
    txn.checkpoint()?;
    println!("bye bye");
    Ok(())
}

fn execute_sql(sql: &str, catalog: &mut Catalog, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>, session: &mut Session) {
    let tokens = match Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens() {
        Ok(t) => t,
        Err(e) => { 
            eprintln!("fatal error: {}", e);
            return;
        }
    };
    let mut parser = Parser::new(tokens);
    let stmts = parser.parse();
    if !parser.errors.is_empty() {
        for e in &parser.errors { eprintln!("parser error: {}", e)}
        return;
    }
    for stmt in stmts {
        match run(stmt, catalog, bp.clone(), txn.clone(), session) {
            Ok(out) => print_outcome(&out),
            Err(e) => eprintln!("error: {}", e),
        }
    }
}

fn print_outcome(out: &Outcome) {
    match out {
        Outcome::Rows(rows) => {
            for row in rows {
                let cells: Vec<String> = row.iter().map(display_value).collect();
                println!("{}", cells.join(" | "));
            }
            println!("({} row{})", rows.len(), if rows.len() == 1 {""} else{"s"});
        }
        Outcome::Affected(n) => println!("({} row{} affected)", n, if *n == 1 {""} else {"s"}),
        Outcome::Explain(s) => println!("{}", s.trim_end()),
        Outcome::Ok => println!("ok"),
    }
}

fn display_value(v: &Value) -> String {
    match v {
        Value::Boolean(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Varchar(s)=> s.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Null => "NULL".to_string(),
    }
}