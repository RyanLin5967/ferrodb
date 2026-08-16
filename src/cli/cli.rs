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
use crate::agent_sql::runtime::AgentRuntime;
use crate::branch::arena::ArenaPageStore;
use crate::branch::catalog::LogBranchCatalog;
use crate::branch::BranchCatalog;
use crate::cow::PageStore;
use crate::tel::MemEffectLog;
const FIRST_CATALOG_PAGE_ID: u32 = 1;

/// Placeholder root recorded for trunk before a real tree exists. `AgentRuntime::with_storage`
/// replaces it with a page it allocates; `reopen_with_storage` refuses if the recorded root does
/// not read back as a page this engine wrote, so the placeholder cannot be mistaken for a tree.
const TRUNK_ROOT_PLACEHOLDER: u32 = 1;

/// Pages of ordinary table growth reserved below the arena floor. One full bitmap's worth (32736
/// pages, ~128 MB at 4 KB pages), which is a real cap: past it, `INSERT` fails with "no free page
/// below the reserved arena region". `FERRODB_ARENA_HEADROOM` raises it at creation time.
///
/// The value is consulted **once**, when a database's arena is first created, and is then persisted
/// in the arena checkpoint. Changing the variable later cannot move an existing database's floor -
/// which is the point, because moving it would put pages the arena already owns back into the
/// ordinary allocator's circulation.
const DEFAULT_ARENA_HEADROOM: u32 = 32_736;

fn arena_headroom() -> u32 {
    std::env::var("FERRODB_ARENA_HEADROOM")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_ARENA_HEADROOM)
}

// super basic cli, make better later
pub fn run_cli(db_path: &str) -> Result<(), FerroError> {
    let existed = Path::new(db_path).exists();
    let file = OpenOptions::new().read(true).write(true).create(true).open(db_path).map_err(|e|FerroError::Io(e.to_string()))?;
    let dm = Arc::new(DiskManager::new(file)?);
    let bp = Arc::new(BufferPoolManager::new(dm));
    let wal = Arc::new(WalManager::new(format!("{}.wal", db_path).into())?);
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
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

    // The agent runtime is built HERE, after the catalog, and that order is load-bearing: the
    // arena floor must sit at or above the disk manager's high-water mark, and `Catalog::create`
    // allocates pages. Building the store first would put the arena underneath pages the catalog
    // is about to hand out, and two allocators would issue the same page.
    //
    // Until this existed, `Session::new()` gave every connection a runtime with `storage: None`,
    // so an agent session's writes lived in a `BTreeMap` and the copy-on-write branch engine -
    // zero-copy fork, lease reaping, shadow paging - was reachable only from tests. The engine was
    // real and the binary did not use it.
    let branches_path = format!("{db_path}.branches");
    let arena_path = format!("{db_path}.arena");
    let branches = Arc::new(LogBranchCatalog::open(
        Path::new(&branches_path),
        TRUNK_ROOT_PLACEHOLDER,
    )?);

    // A checkpoint on disk is the only thing that says where the arena starts. Its absence means
    // "no arena has ever been created here", which is a different situation from "reattach", and
    // `reopen_from_checkpoint` refuses rather than guessing a base.
    let arena_exists = Path::new(&arena_path).exists();
    let store: Arc<ArenaPageStore> = Arc::new(if arena_exists {
        ArenaPageStore::reopen_from_checkpoint(bp.clone(), branches.clone(), Path::new(&arena_path))?
    } else {
        // The arena owns `[base, inf)` and the ordinary allocator is confined to `[0, base)`, so
        // `base` is a hard ceiling on how far ordinary tables can grow. The first version of this
        // used the high-water mark directly and left the ordinary allocator *zero* pages: the very
        // first `CREATE TABLE` in the shipped binary died with "no free page below the reserved
        // arena region at 2". The floor has to be high-water plus deliberate headroom.
        //
        // Expressed as headroom rather than an absolute page number so it means the same thing for
        // a database created just now and one that already has pages: an existing database gets its
        // arena above what it has already allocated, still with room to grow. Costs nothing up
        // front - bitmap pages are chained on demand, so a distant floor is only a bound check.
        let base = bp.disk_manager.high_water()?.saturating_add(arena_headroom());
        ArenaPageStore::new(bp.clone(), branches.clone(), base)?
    });
    // Persist the free-space map whenever the arena claims a new extent, not only at exit. The
    // exit checkpoint below is still worth taking - it captures the final partial extent - but it
    // is the one a `kill -9` or a power cut never reaches, and a map older than the durable branch
    // catalog is a map that re-issues pages the catalog still points at.
    store.checkpoint_to(std::path::PathBuf::from(&arena_path));

    let runtime = Arc::new(if arena_exists {
        AgentRuntime::reopen_with_storage(
            branches.clone() as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            store.clone() as Arc<dyn PageStore>,
        )?
    } else {
        AgentRuntime::with_storage(
            branches.clone() as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            store.clone() as Arc<dyn PageStore>,
        )?
    });
    let mut session = Session::with_runtime(runtime);
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
    // Persist where the arena starts and what it has allocated. Without this the next open finds
    // no checkpoint, refuses to reattach, and the branch tree written this session is unreachable.
    store.checkpoint(Path::new(&arena_path))?;
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
        Outcome::Agent(a) => println!("{}", a),
        Outcome::Ok => println!("ok"),
    }
}

fn display_value(v: &Value) -> String {
    match v {
        Value::Boolean(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Varchar(s)=> s.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        // The stored digits verbatim: rendering a decimal through a float here would undo the
        // whole point of storing it as digits.
        Value::Decimal(d) => d.to_string(),
        Value::Timestamp(ms) => ms.to_string(),
        Value::Null => "NULL".to_string(),
    }
}