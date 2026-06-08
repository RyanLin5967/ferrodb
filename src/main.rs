use ferrodb::cli::cli;

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| "ferro.db".to_string());
    if let Err(e) = cli::run_cli(&path) {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}