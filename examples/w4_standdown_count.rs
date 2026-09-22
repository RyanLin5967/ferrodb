//! **W4 check 3 — does standing down stay rare?** A COUNT, over a real socket.
//!
//! `ServerContext::begin_read`'s own doc claims standing down happens *"only while DDL is
//! actually in flight"*. W4 proposes moving DML onto that shared path; a statement that stands
//! down takes the exclusive catalog mutex anyway, so **if the claim is wrong W4 buys nothing**,
//! whichever design option wins behind it. This measures the claim.
//!
//! # Why this is not a timing experiment
//!
//! The answer is a ratio of two integers — stand-downs over attempts — and the denominator here
//! is a **fixed statement budget**, not a fixed duration. Nothing in the reported number moves
//! when the box is busy, which is the only reason this can be run on a machine with a measured
//! 46x quiet-vs-loaded spread. Durations are printed as upper bounds and stamped with load; no
//! conclusion rests on one.
//!
//! # The layer, stated because it has invalidated three results in this project
//!
//! The **server** here is the shipped path, unmodified: `pgwire::serve` -> `handle` -> the
//! simple-query protocol -> `extended::Statement::execute` -> `begin_read`. The **client** is a
//! minimal v3 client in this file, over a real `TcpStream` on a real port. A number taken by
//! calling `ServerContext::begin_read` directly — as `examples/d50_reader_scaling.rs` and four
//! others do — is a *different experiment*, and `Counts::is_pure_wire_layer()` is checked here so
//! the artifact can say which one this was instead of the prose asserting it.
//!
//! # Arms (`ARM` argv[1])
//!
//! | arm | announcers present | what it answers |
//! |---|---|---|
//! | `read_only` | the lease scan only | the floor; should be ~0 |
//! | `agent` | forks + abandons | **the number that decides W4** |
//! | `merge:<w>` | forks + `w` in-session writes + MERGE | merge-heavy, over a DML-announcer axis |
//! | `ddl` | CREATE/DROP TABLE | the case the source says is the only one that matters |
//! | `dml_control` | plain INSERTs on main | **today's** announcer population, which W4 REMOVES |
//! | `firecheck` | a thread holding the exclusive mutex | proves the counter can fire at all |
//!
//! `merge:<w>` is an axis, not a point. Under W4 an in-session INSERT would stop announcing, so a
//! single merge-heavy arm measured today over-counts by exactly the DML announcements. Holding the
//! fork and MERGE cadence fixed and sweeping `w` gives a slope whose **intercept at w = 0** is the
//! W4 number; a single point could not separate the two announcer populations at all.
//!
//! `dml_control` is the arm W4 makes go away. It is labelled a control and must never be quoted
//! as a W4 number.
//!
//! # What this refuses rather than reports
//!
//! * counters `None` — the build was not instrumented, which is **not** "never stood down"
//! * zero attempts — a run that collected nothing has not passed
//! * an attempt that did not come through pgwire — the layer claim would be false
//! * a workload that did not produce the announcer it is named for (no MERGE ran in a merge arm)
//! * any `ErrorResponse` from any statement — a workload of failing statements measures nothing
//!
//! Every one exits non-zero.
//!
//! ```text
//! cargo build --release --features standdown_count --example w4_standdown_count
//! ./target/release/examples/w4_standdown_count <arm> <lease-scan-millis>
//! ```

use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::lease_thread::{LeaseThread, RuntimeLock};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::standdown::{self, Verb};
use ferrodb::pgwire::{serve, ServerContext};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

// ---- the statement budget. FIXED, which is what makes the denominator load-immune -------------

/// Reader connections in every arm. The stand-down rate is a property of the announcer population
/// and the reader population together, so readers are held constant across arms.
const READERS: usize = 8;
/// SELECTs per reader connection.
const READS_EACH: usize = 500;
/// Connections running agent work (forks / merges / DDL / DML), per arm that has them.
const WORKERS: usize = 4;
/// Agent cycles per worker connection.
const CYCLES: usize = 40;
/// In-session SELECTs per cycle in the `agent` arm.
const AGENT_READS: usize = 4;
/// Rows seeded into the base table.
const SEED_ROWS: i64 = 200;
/// Pages reserved below the arena floor. Small, for the reason `integration_server_reaps.rs`
/// gives: the 32736 default puts the arena's first page ~128 MB into the file.
const HEADROOM: u32 = 256;

// ---- a minimal PostgreSQL v3 client -----------------------------------------------------------

/// One client connection. Written against the protocol, not against the server's encoder: the
/// only thing it has to get right is putting the same bytes on the socket that a driver would.
struct Wire {
    w: TcpStream,
    r: BufReader<TcpStream>,
}

impl Wire {
    fn connect(addr: std::net::SocketAddr) -> std::io::Result<Wire> {
        let s = TcpStream::connect(addr)?;
        s.set_nodelay(true)?;
        let mut c = Wire { r: BufReader::new(s.try_clone()?), w: s };

        // The TLS probe every real client sends first. The server answers a bare 'N'.
        c.w.write_all(&8i32.to_be_bytes())?;
        c.w.write_all(&80_877_103i32.to_be_bytes())?;
        c.w.flush()?;
        let mut n = [0u8; 1];
        c.r.read_exact(&mut n)?;
        assert_eq!(n[0], b'N', "server claimed TLS support it does not have");

        let params = b"user\0ferro\0database\0ferro\0application_name\0w4_standdown\0\0";
        let payload_len = 4 + params.len();
        c.w.write_all(&((payload_len + 4) as i32).to_be_bytes())?;
        c.w.write_all(&196_608i32.to_be_bytes())?;
        c.w.write_all(params)?;
        c.w.flush()?;
        c.drain_to_ready("startup")?;
        Ok(c)
    }

    /// One simple query, run to `ReadyForQuery`.
    ///
    /// An `ErrorResponse` is returned as `Err`, never swallowed: a workload whose statements all
    /// fail still increments `begin_read`, so silently tolerating errors is exactly how this
    /// harness would report a confident number for a workload that never ran.
    fn q(&mut self, sql: &str) -> std::io::Result<()> {
        let bytes = sql.as_bytes();
        self.w.write_all(b"Q")?;
        self.w.write_all(&((bytes.len() + 5) as i32).to_be_bytes())?;
        self.w.write_all(bytes)?;
        self.w.write_all(&[0])?;
        self.w.flush()?;
        self.drain_to_ready(sql)
    }

    fn drain_to_ready(&mut self, what: &str) -> std::io::Result<()> {
        let mut err: Option<String> = None;
        loop {
            let mut tag = [0u8; 1];
            self.r.read_exact(&mut tag)?;
            let mut lb = [0u8; 4];
            self.r.read_exact(&mut lb)?;
            let len = i32::from_be_bytes(lb);
            assert!(len >= 4, "message {} claims {len} bytes", tag[0] as char);
            let mut body = vec![0u8; (len - 4) as usize];
            self.r.read_exact(&mut body)?;
            match tag[0] {
                b'E' => err = Some(decode_error(&body)),
                b'Z' => break,
                _ => {}
            }
        }
        match err {
            None => Ok(()),
            Some(e) => Err(std::io::Error::other(format!("{what} -> {e}"))),
        }
    }
}

fn decode_error(body: &[u8]) -> String {
    let mut out = Vec::new();
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let f = body[i];
        let start = i + 1;
        let end = body[start..].iter().position(|b| *b == 0).map(|p| start + p).unwrap_or(body.len());
        if f == b'M' || f == b'C' {
            out.push(String::from_utf8_lossy(&body[start..end]).to_string());
        }
        i = end + 1;
    }
    out.join(" ")
}

// ---- the engine, wired exactly as `examples/pgserver.rs` wires it ------------------------------

struct Rig {
    ctx: Arc<ServerContext>,
    addr: std::net::SocketAddr,
    _lease: LeaseThread,
    _dir: tempfile::TempDir,
}

fn build(scan: Duration) -> Rig {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("w4.db");
    let dbs = db.to_string_lossy().to_string();

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&db)
        .expect("open db");
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let wal = Arc::new(WalManager::new(format!("{dbs}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let catalog = Catalog::create(bp.clone()).unwrap();

    let branches: Arc<TableBranchCatalog> =
        Arc::new(TableBranchCatalog::default_for_database(&dbs, 1).expect("branch catalog"));
    let base = bp.disk_manager.high_water().expect("high water") + HEADROOM;
    let store: Arc<ArenaPageStore> = Arc::new(
        ArenaPageStore::new(bp.clone(), branches.clone() as Arc<dyn BranchCatalog>, base)
            .expect("arena"),
    );
    store.checkpoint_to(dir.path().join("w4.db.arena"));

    let reaper = Arc::new(TwoTierReaper::new(
        branches.clone() as Arc<dyn BranchCatalog>,
        store.clone(),
    ));
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches.clone() as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            store.clone() as Arc<dyn PageStore>,
        )
        .expect("storage-backed runtime")
        .with_reaper(reaper.clone() as Arc<dyn Reaper>),
    );

    let ctx = Arc::new(ServerContext::new(catalog, bp, txn, runtime.clone()));
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    {
        let ctx = Arc::clone(&ctx);
        std::thread::spawn(move || {
            let _ = serve(listener, ctx);
        });
    }

    // THE LEASE SCAN — the announcer a workload cannot control. It takes
    // `ServerContext::catalog()` (`branch::lease_thread`'s `impl RuntimeLock for ServerContext`),
    // which drains readers, so every scan is an announcement. Started here, at the interval this
    // arm was given, because "can you separate the reaper's cadence?" is the failure mode this
    // measurement was pre-registered to be able to report.
    let lease = LeaseThread::start(reaper, runtime, ctx.clone() as Arc<dyn RuntimeLock>, scan)
        .expect("lease thread");

    Rig { ctx, addr, _lease: lease, _dir: dir }
}

// ---- arms --------------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arm {
    ReadOnly,
    Agent,
    /// In-session writes per branch. The DML-announcer axis.
    Merge(usize),
    Ddl,
    DmlControl,
    FireCheck,
}

fn parse_arm(s: &str) -> Arm {
    match s {
        "read_only" => Arm::ReadOnly,
        "agent" => Arm::Agent,
        "ddl" => Arm::Ddl,
        "dml_control" => Arm::DmlControl,
        "firecheck" => Arm::FireCheck,
        other => match other.strip_prefix("merge:") {
            Some(w) => Arm::Merge(w.parse().expect("merge:<writes per branch>")),
            None => panic!(
                "unknown arm {other:?}; one of read_only agent merge:<w> ddl dml_control firecheck"
            ),
        },
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arm = parse_arm(args.get(1).map(String::as_str).unwrap_or("read_only"));
    let scan_millis: u64 = args.get(2).map(|s| s.parse().expect("millis")).unwrap_or(30_000);

    let rig = build(Duration::from_millis(scan_millis));
    let load_at_start = loadavg();

    // ---- setup, on its own connection, entirely OUTSIDE the measured window -------------------
    let mut setup = Wire::connect(rig.addr).expect("setup connection");
    setup.q("CREATE TABLE t (id INTEGER, v INTEGER);").expect("create");
    for i in 0..SEED_ROWS {
        setup.q(&format!("INSERT INTO t VALUES ({i}, {});", i * 7)).expect("seed");
    }

    // ---- open and WARM every connection before the window ------------------------------------
    // A connection's first statement always takes the catalog mutex to build its snapshot
    // (`read_catalog`), and therefore always announces. Warming them here, then resetting, keeps
    // connection setup out of the number instead of subtracting an estimate of it afterwards.
    let n_workers = match arm {
        Arm::ReadOnly => 0,
        Arm::FireCheck => 0,
        _ => WORKERS,
    };
    let mut reader_conns: Vec<Wire> =
        (0..READERS).map(|_| Wire::connect(rig.addr).expect("reader connection")).collect();
    for c in reader_conns.iter_mut() {
        c.q("SELECT id, v FROM t;").expect("warm a reader");
    }
    // The agent arms need a FRESH connection per cycle: a connection whose session has been
    // merged or abandoned is inert, so reusing one would measure a stream of refusals. They are
    // all opened and warmed here so that the per-cycle connect does not land inside the window.
    let mut worker_conns: Vec<Vec<Wire>> = (0..n_workers)
        .map(|_| {
            let per = match arm {
                Arm::Agent | Arm::Merge(_) => CYCLES,
                _ => 1,
            };
            (0..per)
                .map(|_| {
                    let mut c = Wire::connect(rig.addr).expect("worker connection");
                    c.q("SELECT id FROM t;").expect("warm a worker");
                    c
                })
                .collect()
        })
        .collect();

    // ---- the measured window -----------------------------------------------------------------
    standdown::reset();
    let t0 = Instant::now();
    let load_at_acquire = loadavg();
    let statements = AtomicU64::new(0);

    let gate = Arc::new(Barrier::new(READERS + n_workers + usize::from(arm == Arm::FireCheck)));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    std::thread::scope(|s| {
        for (ri, conn) in reader_conns.iter_mut().enumerate() {
            let gate = Arc::clone(&gate);
            let statements = &statements;
            s.spawn(move || {
                gate.wait();
                for k in 0..READS_EACH {
                    let id = ((ri * READS_EACH + k) as i64) % SEED_ROWS;
                    conn.q(&format!("SELECT id, v FROM t WHERE id = {id};")).expect("read");
                    statements.fetch_add(1, Ordering::Relaxed);
                }
            });
        }

        for (wi, conns) in worker_conns.iter_mut().enumerate() {
            let gate = Arc::clone(&gate);
            let statements = &statements;
            s.spawn(move || {
                gate.wait();
                match arm {
                    Arm::Agent => {
                        for (k, c) in conns.iter_mut().enumerate() {
                            c.q(&format!(
                                "BEGIN AGENT SESSION AS 'a{wi}' RUN 'r{wi}_{k}';"
                            ))
                            .expect("fork");
                            for _ in 0..AGENT_READS {
                                c.q("SELECT id, v FROM t;").expect("in-session read");
                            }
                            c.q("ABANDON;").expect("abandon");
                            statements
                                .fetch_add(2 + AGENT_READS as u64, Ordering::Relaxed);
                        }
                    }
                    Arm::Merge(w) => {
                        for (k, c) in conns.iter_mut().enumerate() {
                            c.q(&format!(
                                "BEGIN AGENT SESSION AS 'm{wi}' RUN 'r{wi}_{k}';"
                            ))
                            .expect("fork");
                            for j in 0..w {
                                let id = ((k * 31 + j) as i64) % SEED_ROWS;
                                c.q(&format!("UPDATE t SET v = v + 1 WHERE id = {id};"))
                                    .expect("in-session write");
                            }
                            c.q("MERGE;").expect("merge");
                            statements.fetch_add(2 + w as u64, Ordering::Relaxed);
                        }
                    }
                    Arm::Ddl => {
                        let c = &mut conns[0];
                        for k in 0..CYCLES {
                            c.q(&format!("CREATE TABLE d{wi}_{k} (id INTEGER);")).expect("ddl");
                            c.q(&format!("DROP TABLE d{wi}_{k};")).expect("ddl drop");
                            statements.fetch_add(2, Ordering::Relaxed);
                        }
                    }
                    Arm::DmlControl => {
                        let c = &mut conns[0];
                        for k in 0..CYCLES * 10 {
                            c.q(&format!("INSERT INTO t VALUES ({}, 1);", 10_000 + k))
                                .expect("insert");
                            statements.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Arm::ReadOnly | Arm::FireCheck => unreachable!("no workers in this arm"),
                }
            });
        }

        // THE POSITIVE CONTROL. A detector that found nothing is not a clean result until it has
        // been forced to fire: this thread holds the exclusive catalog mutex outright, so
        // `writer_active` is set for essentially the whole window and every reader MUST stand
        // down. If `read_only` reports ~0 and this reports ~0 too, the instrument is broken and
        // the floor means nothing.
        if arm == Arm::FireCheck {
            let ctx = Arc::clone(&rig.ctx);
            let gate = Arc::clone(&gate);
            let stop = Arc::clone(&stop);
            s.spawn(move || {
                gate.wait();
                while !stop.load(Ordering::Relaxed) {
                    let g = ctx.catalog();
                    std::thread::sleep(Duration::from_micros(200));
                    drop(g);
                }
            });
        }
    });
    stop.store(true, Ordering::Relaxed);

    let elapsed = t0.elapsed();
    let c = standdown::counts();

    // ---- refusals ----------------------------------------------------------------------------
    let attempts = match c.attempts() {
        Some(a) => a,
        None => {
            eprintln!(
                "REFUSED: the counters are None. This binary was built WITHOUT \
                 --features standdown_count, so it measured nothing. That is not \"never stood \
                 down\"; rebuild with the feature."
            );
            std::process::exit(2);
        }
    };
    if attempts == 0 {
        eprintln!("REFUSED: zero attempts. A run that collected nothing has not passed.");
        std::process::exit(3);
    }
    match c.is_pure_wire_layer() {
        Some(true) => {}
        other => {
            eprintln!(
                "REFUSED: {} of {attempts} attempts were not recorded at the pgwire call site \
                 (is_pure_wire_layer={other:?}). Something drove begin_read without going through \
                 the wire, so this is not a pgwire-layer number and E.6 says the two differ.",
                attempts as i64
                    - c.wire_admitted.as_ref().map(sum).unwrap_or(0) as i64
                    - c.wire_stood_down.as_ref().map(sum).unwrap_or(0) as i64
            );
            std::process::exit(4);
        }
    }
    // The workload must have produced the announcer it is named for.
    let excl = c.wire_exclusive.clone().unwrap();
    let need: &[(Verb, u64)] = match arm {
        Arm::Agent => &[(Verb::Fork, 1)],
        Arm::Merge(_) => &[(Verb::Fork, 1), (Verb::Merge, 1)],
        Arm::Ddl => &[(Verb::Ddl, 1)],
        Arm::DmlControl => &[(Verb::Dml, 1)],
        Arm::ReadOnly | Arm::FireCheck => &[],
    };
    for (v, min) in need {
        let got = excl.iter().find(|(k, _)| k == v).map(|(_, n)| *n).unwrap_or(0);
        if got < *min {
            eprintln!(
                "REFUSED: arm {arm:?} recorded {got} exclusive-path {} statements, expected at \
                 least {min}. The announcer this arm is named for never ran.",
                v.name()
            );
            std::process::exit(5);
        }
    }
    if arm == Arm::FireCheck && c.stood_down.unwrap() == 0 {
        eprintln!(
            "REFUSED: the fire-check arm held the exclusive catalog mutex for the whole window \
             and NOT ONE attempt stood down. The counter cannot fire, so every zero this \
             instrument reports is meaningless."
        );
        std::process::exit(6);
    }

    // ---- report -------------------------------------------------------------------------------
    let arm_name = match arm {
        Arm::ReadOnly => "read_only".to_string(),
        Arm::Agent => "agent".to_string(),
        Arm::Merge(w) => format!("merge:{w}"),
        Arm::Ddl => "ddl".to_string(),
        Arm::DmlControl => "dml_control".to_string(),
        Arm::FireCheck => "firecheck".to_string(),
    };
    println!("--- w4 stand-down count -------------------------------------------------");
    println!("arm                  {arm_name}");
    println!("layer                pgwire (TCP -> serve -> simple query -> begin_read)");
    println!("lease_scan_millis    {scan_millis}");
    println!("client_statements    {}", statements.load(Ordering::Relaxed));
    println!("attempts             {attempts}");
    println!("admitted             {}", c.admitted.unwrap());
    println!("stood_down           {}", c.stood_down.unwrap());
    println!(
        "STAND_DOWN_FRACTION  {:.6}",
        c.fraction().expect("attempts>0 was checked above")
    );
    println!("announcements_total  {}", c.announced.unwrap());
    println!(
        "announce_by_statement {}",
        excl.iter().filter(|(_, n)| *n > 0).map(|(v, n)| format!("{}={n}", v.name())).collect::<Vec<_>>().join(" ")
    );
    println!(
        "announce_unattributed {}  (lease scan + read_catalog snapshot refreshes)",
        c.unattributed_announcements().unwrap()
    );
    println!(
        "stood_down_by_verb   {}",
        c.wire_stood_down
            .as_ref()
            .unwrap()
            .iter()
            .filter(|(_, n)| *n > 0)
            .map(|(v, n)| format!("{}={n}", v.name()))
            .collect::<Vec<_>>()
            .join(" ")
            + if c.stood_down.unwrap() == 0 { "(none)" } else { "" }
    );
    println!(
        "ran_shared_by_verb   {}",
        c.wire_ran_shared
            .as_ref()
            .unwrap()
            .iter()
            .filter(|(_, n)| *n > 0)
            .map(|(v, n)| format!("{}={n}", v.name()))
            .collect::<Vec<_>>()
            .join(" ")
    );
    // Per-verb stand-down rate: the SELECT row is the closest proxy for what a W4-ified DML would
    // experience, because a SELECT is the statement that already behaves the way W4 wants DML to.
    let adm = c.wire_admitted.clone().unwrap();
    let sd = c.wire_stood_down.clone().unwrap();
    for v in Verb::ALL {
        let a = adm.iter().find(|(k, _)| *k == v).map(|(_, n)| *n).unwrap_or(0);
        let d = sd.iter().find(|(k, _)| *k == v).map(|(_, n)| *n).unwrap_or(0);
        if a + d > 0 {
            println!(
                "  {:<8} attempts={:<7} stood_down={:<7} fraction={:.6}",
                v.name(),
                a + d,
                d,
                d as f64 / (a + d) as f64
            );
        }
    }
    // Upper bounds only, stamped. Nothing above depends on these.
    println!(
        "wall_upper_bound_s   {:.2}   load_at_acquire={load_at_acquire:.2} load_at_start={load_at_start:.2}",
        elapsed.as_secs_f64()
    );
}

fn sum(v: &Vec<(Verb, u64)>) -> u64 {
    v.iter().map(|(_, n)| n).sum()
}

/// 1-minute load average, via `sysctl` — the box this runs on is macOS and has no `/proc`.
fn loadavg() -> f64 {
    let out = std::process::Command::new("/usr/sbin/sysctl")
        .arg("-n")
        .arg("vm.loadavg")
        .output();
    match out {
        Ok(o) => String::from_utf8_lossy(&o.stdout)
            .trim()
            .trim_matches(|c| c == '{' || c == '}' || c == ' ')
            .split_whitespace()
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(f64::NAN),
        Err(_) => f64::NAN,
    }
}
