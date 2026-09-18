//! Does page-fault throughput scale with CONCURRENCY? (S22)
//!
//! # The claim under test
//!
//! `BufferPoolManager::fetch_page` takes `self.arc_cache.lock()` — one process-wide `Mutex` — and
//! holds it to the end of the function on **every** path, including across
//! `DiskManager::read`. If that is what the code does, then every page miss serialises every other
//! thread in the process for the duration of a read syscall, and aggregate fault throughput is flat
//! as threads rise.
//!
//! Flat aggregate throughput is the signature. Fixed work PER THREAD means total work rises with
//! the thread count, so:
//!
//! * perfect serialisation -> wall time rises linearly -> **aggregate faults/sec constant**
//! * perfect parallelism   -> wall time constant       -> **aggregate faults/sec linear in threads**
//!
//! # The instrument, and why it is a modelled delay rather than a real disk
//!
//! A real file on this machine is served from the OS page cache, so a `pread` costs a couple of
//! microseconds and the serialised window is too small to separate from ordinary lock contention.
//! Making it a real device read would need a working set larger than 48 GB of RAM.
//!
//! So the default mode replaces the file with an in-memory image behind
//! [`Storage`](ferrodb::storage::storage::Storage) — the seam `storage::sim` already uses — and
//! sleeps a **known** duration per `pread`. That turns the question into arithmetic: with a delay of
//! `D` per read, perfect serialisation is `1/D` faults/sec at every thread count, and perfect
//! parallelism is `N/D`. The harness prints both bounds next to the measurement so a reader does not
//! have to take the interpretation on trust.
//!
//! `thread::sleep` and not a spin: a spin burns a core, and 16 spinning threads on an 18-core box
//! that is also running other work would measure the scheduler instead of the lock. A blocking read
//! releases its core, which is what `sleep` models.
//!
//! `--real` runs the same sweep against a real file for comparison. Its numbers are WEAKER evidence,
//! not stronger: a warm page cache shrinks the serialised window, so serialisation that is still
//! visible there is visible despite the instrument rather than because of it.
//!
//! # Two guards, because a benchmark that cannot see the effect reports a comfortable number
//!
//! 1. **MISS GUARD.** The delay only applies to reads that actually reach storage. If the working
//!    set fitted in the pool the threads would be measuring cache hits, the delay would never fire,
//!    and the sweep would report a large, flat, meaningless number. The storage counts its own
//!    `pread`s and the harness REFUSES a run where reads are not close to fetches.
//! 2. **STAMP GUARD.** Every page carries its own id in its first four bytes and every fetch checks
//!    it. A change that makes this faster by serving the wrong page is not a faster buffer pool, and
//!    this harness fails rather than reports it.
//!
//!   cargo run --release --example bufpool_fault_concurrency -- [FETCHES_PER_THREAD] [T,T,T] [DELAY_US] [REPEATS]
//!   cargo run --release --example bufpool_fault_concurrency -- 500 1,2,4,8,16 500
//!   cargo run --release --example bufpool_fault_concurrency -- --real 4000 1,2,4,8,16 0 40

use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::storage::Storage;

/// Far more pages than the pool has frames (1024), so every thread's working set is evicted out
/// from under it and the fetches are genuine faults. The MISS GUARD below checks that rather than
/// assuming it.
const PAGES: u32 = 8192;

/// An in-memory image that charges a fixed, known price per read.
///
/// The sleep happens **before** the lock is taken, so the modelled IO of two threads overlaps. A
/// sleep inside the critical section would serialise the threads inside the instrument and the
/// harness would measure itself.
struct DelayStorage {
    image: RwLock<Vec<u8>>,
    /// Nanoseconds to sleep per `pread`. Zero during setup, set for the measured phase.
    delay_ns: AtomicU64,
    reads: AtomicU64,
    writes: AtomicU64,
}

impl DelayStorage {
    fn new() -> Self {
        DelayStorage {
            image: RwLock::new(Vec::new()),
            delay_ns: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
        }
    }
    fn set_delay(&self, d: Duration) {
        self.delay_ns.store(d.as_nanos() as u64, Ordering::Relaxed);
    }
}

impl Storage for DelayStorage {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        let mut img = self.image.write().unwrap();
        let end = offset as usize + buf.len();
        if img.len() < end {
            img.resize(end, 0);
        }
        img[offset as usize..end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let d = self.delay_ns.load(Ordering::Relaxed);
        if d > 0 {
            std::thread::sleep(Duration::from_nanos(d));
        }
        let img = self.image.read().unwrap();
        let start = offset as usize;
        if start >= img.len() {
            return Ok(0);
        }
        let n = buf.len().min(img.len() - start);
        buf[..n].copy_from_slice(&img[start..start + n]);
        Ok(n)
    }

    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.image.write().unwrap().resize(len as usize, 0);
        Ok(())
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.image.read().unwrap().len() as u64)
    }
}

/// A real `File`, counting its reads.
///
/// `--real` exists so the result does not rest entirely on a modelled delay. It would be worth
/// little if the MISS GUARD could not run there: an uninstrumented file mode is a mode in which the
/// harness cannot tell a page fault from a cache hit, which is the one thing it has to be able to
/// tell. So the real mode goes through the same [`Storage`] seam and keeps the same counter.
struct CountingFile {
    file: std::fs::File,
    reads: AtomicU64,
}

impl Storage for CountingFile {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.file.pwrite(buf, offset)
    }
    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.file.pread(buf, offset)
    }
    fn sync_all(&self) -> io::Result<()> {
        Storage::sync_all(&self.file)
    }
    fn sync_data(&self) -> io::Result<()> {
        Storage::sync_data(&self.file)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        Storage::set_len(&self.file, len)
    }
    fn len(&self) -> io::Result<u64> {
        Storage::len(&self.file)
    }
}

fn stamp(data: &mut [u8; PAGE_SIZE], page_id: u32) {
    data[0..4].copy_from_slice(&page_id.to_be_bytes());
}

fn read_stamp(data: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_be_bytes([data[0], data[1], data[2], data[3]])
}

/// Allocate and stamp `PAGES` pages, then flush, so every page exists on disk and can be faulted in.
fn populate(bp: &BufferPoolManager) -> Vec<u32> {
    let mut ids = Vec::with_capacity(PAGES as usize);
    for _ in 0..PAGES {
        let id = bp.new_page().expect("allocate");
        let idx = bp.fetch_page(id).expect("fetch for stamping");
        {
            let mut frame = bp.frames[idx].write().unwrap();
            stamp(&mut frame.data, id);
        }
        bp.unpin_page(id, true);
        ids.push(id);
    }
    bp.flush_all().expect("flush");
    ids
}

struct Run {
    threads: usize,
    elapsed: Duration,
    fetches: usize,
    refused: usize,
    wrong: usize,
    reads: u64,
}

/// One point of the sweep.
///
/// Each thread walks a **disjoint** slice of the page space. That is deliberate: two threads wanting
/// the SAME page is a different question (same-page contention), and mixing it in here would leave
/// the result ambiguous. Disjoint pages isolate exactly the claim — one thread's miss blocking
/// another thread that wants nothing to do with that page.
fn sweep_point(
    bp: &Arc<BufferPoolManager>,
    ids: &[u32],
    threads: usize,
    per_thread: usize,
    repeats: usize,
    read_counter: &AtomicU64,
    resident: bool,
) -> Run {
    let refused = Arc::new(AtomicUsize::new(0));
    let wrong = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));

    // **Cold pool at every point, and this line is load-bearing.** Without it the first draft of
    // this harness reported 3146 faults/s at 4 threads -- a x1.91 "speedup" that was nothing of the
    // kind. Points share one page space and lower thread counts leave their working set resident,
    // so the 4-thread point re-HIT the 1000 pages the 2-thread point had just loaded: the MISS GUARD
    // below measured 0.519 reads per fetch and refused the run. Half of those "faults" never touched
    // storage, so the modelled IO never fired for them and the number was about cache hits.
    //
    // `invalidate_all` drops every frame WITHOUT writing back, which is safe here precisely because
    // this harness only ever unpins clean (`unpin_page(id, false)`); it would be data loss in a
    // workload that dirtied pages.
    // **Warm or cold, decided once, before anything is counted.**
    //
    // The RESIDENT arm exists because the two arms fail for different reasons and a fix for one
    // need not be a fix for the other. The fault arm measures the miss path, where the cost is a
    // read syscall made under a lock. This arm measures the HIT path, where no IO happens at all
    // and the only thing a fetch can contend on is the pool's own bookkeeping. A change that takes
    // IO out from under the global lock moves the first curve and need not move this one.
    //
    // Warming happens before `reads_before` is sampled, so the reads that fill the pool are not
    // charged to the measurement and the HIT GUARD in `main` sees the steady state rather than the
    // fill.
    // Warming is a LOOP WITH A CHECK, not a single pass, because a single pass does not work and
    // the first version of this arm shipped believing it did. Measured: at the first sweep point
    // one pass left 511 of the 512 pages still absent, the timed window faulted them in, and the
    // HIT GUARD failed the run -- which is exactly what that guard is for. Subsequent points were
    // clean, so a warm-up that "looked fine" at points 2..n would have hidden a broken point 1.
    //
    // Re-fetching until the page table actually holds the working set makes residency a checked
    // fact rather than an assumption, and `passes` is reported so a change that makes warming
    // harder shows up as a number rather than as a mysteriously slow first point.
    let mut passes = 0usize;
    if resident {
        for attempt in 1..=16 {
            for &id in ids {
                if bp.fetch_page(id).is_ok() {
                    bp.unpin_page(id, false);
                }
            }
            passes = attempt;
            let pt = bp.page_table.read().unwrap();
            if ids.iter().all(|id| pt.contains_key(id)) {
                break;
            }
        }
        let pt = bp.page_table.read().unwrap();
        let missing = ids.iter().filter(|id| !pt.contains_key(id)).count();
        drop(pt);
        if missing > 0 {
            eprintln!(
                "# WARNING: {missing} of {} pages still not resident after {passes} warm passes; \
                 the HIT GUARD below will fail this point.",
                ids.len()
            );
        }
    }
    let reads_before = read_counter.load(Ordering::Relaxed);
    let mut elapsed = Duration::ZERO;

    // `repeats` exists to lengthen the measured window, not to change the workload. The real-file
    // mode serves reads from the OS page cache in about a microsecond, so a single pass takes a few
    // MILLISECONDS -- far too short to quote on a machine that is also running other work. Each
    // repeat is timed separately and summed, and the pool is dropped cold in between, so every
    // fetch in every repeat is still a genuine fault. The MISS GUARD checks that rather than
    // trusting this comment.
    for _ in 0..repeats {
        if !resident {
            bp.invalidate_all().expect("cold pool between repeats: nothing should be pinned here");
        }

        let start = Instant::now();
        std::thread::scope(|s| {
            for t in 0..threads {
                let bp = Arc::clone(bp);
                let refused = Arc::clone(&refused);
                let wrong = Arc::clone(&wrong);
                let done = Arc::clone(&done);
                // Disjoint per thread: thread `t` takes every `threads`-th slot. Two threads
                // wanting the SAME page is a different question, and mixing it in here would leave
                // the result ambiguous about which effect it had measured.
                s.spawn(move || {
                    for k in 0..per_thread {
                        let slot = (t + k * threads) % ids.len();
                        let id = ids[(slot * 4099) % ids.len()];
                        let Ok(idx) = bp.fetch_page(id) else {
                            refused.fetch_add(1, Ordering::Relaxed);
                            continue;
                        };
                        let got = read_stamp(&bp.frames[idx].read().unwrap().data);
                        bp.unpin_page(id, false);
                        if got != id {
                            wrong.fetch_add(1, Ordering::Relaxed);
                        }
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        elapsed += start.elapsed();
    }

    Run {
        threads,
        elapsed,
        fetches: done.load(Ordering::Relaxed),
        refused: refused.load(Ordering::Relaxed),
        wrong: wrong.load(Ordering::Relaxed),
        reads: read_counter.load(Ordering::Relaxed) - reads_before,
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let real = args.iter().any(|a| a == "--real");
    // The working set fits the pool, so every fetch is a HIT and the miss path is never taken.
    let resident = args.iter().any(|a| a == "--resident");
    let pos: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();

    let per_thread: usize = pos.first().and_then(|s| s.parse().ok()).unwrap_or(500);
    let thread_counts: Vec<usize> = pos
        .get(1)
        .map(|s| s.as_str())
        .unwrap_or("1,2,4,8,16")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let delay = Duration::from_micros(pos.get(2).and_then(|s| s.parse().ok()).unwrap_or(500));
    let repeats: usize = pos.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);

    // A working set that FITS in the 1024 frames, so the pool never evicts and never reads.
    const RESIDENT_PAGES: usize = 512;

    println!("# S22 buffer pool fault concurrency");
    println!(
        "# pages={} pool_frames=1024 fetches_per_thread={per_thread} repeats={repeats}",
        if resident { RESIDENT_PAGES as u32 } else { PAGES }
    );
    println!(
        "# arm={}",
        if resident {
            "RESIDENT (working set fits the pool; every fetch is a cache HIT)"
        } else {
            "OVERSUBSCRIBED (working set exceeds the pool; every fetch is a page FAULT)"
        }
    );
    println!(
        "# mode={}",
        if real { "real file (warm OS page cache)".to_string() } else { format!("modelled IO, {} us per read", delay.as_micros()) }
    );
    println!("# host: {} cores", std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0));
    if let Ok(la) = std::fs::read_to_string("/proc/loadavg") {
        println!("# loadavg: {}", la.trim());
    }

    // Both modes go through the same [`Storage`] seam and keep the same read counter; they differ
    // only in what backs the bytes and whether the read is charged a modelled price.
    enum Instrument {
        Delayed(Arc<DelayStorage>),
        Real(Arc<CountingFile>),
    }
    impl Instrument {
        fn reads(&self) -> &AtomicU64 {
            match self {
                Instrument::Delayed(s) => &s.reads,
                Instrument::Real(s) => &s.reads,
            }
        }
        fn modelled(&self) -> bool {
            matches!(self, Instrument::Delayed(_))
        }
    }

    let (bp, ids, inst): (Arc<BufferPoolManager>, Vec<u32>, Instrument) = if real {
        let dir = std::env::temp_dir().join(format!("ferro-s22-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bufpool.db");
        let _ = std::fs::remove_file(&path);
        let f = std::fs::OpenOptions::new()
            .create(true).read(true).write(true).truncate(true).open(&path).unwrap();
        let st = Arc::new(CountingFile { file: f, reads: AtomicU64::new(0) });
        let dm = Arc::new(DiskManager::with_storage(st.clone() as Arc<dyn Storage>).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        let ids = populate(&bp);
        println!("# file: {}", path.display());
        (bp, ids, Instrument::Real(st))
    } else {
        let st = Arc::new(DelayStorage::new());
        let dm = Arc::new(DiskManager::with_storage(st.clone() as Arc<dyn Storage>).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        let ids = populate(&bp); // delay is still zero here, so setup is not charged
        st.set_delay(delay);
        (bp, ids, Instrument::Delayed(st))
    };

    println!();
    println!("threads\twall_s\tfetches\tfaults_per_s\treads\treads_per_fetch\trefused\twrong");

    let working_set: &[u32] = if resident { &ids[..RESIDENT_PAGES.min(ids.len())] } else { &ids };

    let mut results: Vec<Run> = Vec::new();
    for &t in &thread_counts {
        let r = sweep_point(&bp, working_set, t, per_thread, repeats, inst.reads(), resident);
        let per_s = r.fetches as f64 / r.elapsed.as_secs_f64();
        let rpf = if r.fetches > 0 { r.reads as f64 / r.fetches as f64 } else { 0.0 };
        println!(
            "{}\t{:.3}\t{}\t{:.0}\t{}\t{:.3}\t{}\t{}",
            r.threads, r.elapsed.as_secs_f64(), r.fetches, per_s, r.reads, rpf, r.refused, r.wrong
        );
        results.push(r);
    }

    // ---- Guards. A run that trips one of these is not a measurement. ----
    let mut failed = false;

    let total_wrong: usize = results.iter().map(|r| r.wrong).sum();
    if total_wrong > 0 {
        eprintln!("\nSTAMP GUARD FAILED: {total_wrong} fetches returned another page's bytes.");
        failed = true;
    }

    let total_fetches: usize = results.iter().map(|r| r.fetches).sum();
    if total_fetches == 0 {
        eprintln!("\nEMPTY RUN: zero fetches completed. This measured nothing.");
        failed = true;
    }

    // MISS GUARD, in BOTH modes. Every fetch within a point asks for a DISTINCT page and the pool
    // is cold at the start of each point, so a healthy run is 1.000 reads per fetch. The threshold
    // is set just under that rather than loosely, because loosening it is what would let the
    // residency bug this guard already caught back in.
    for r in &results {
        let rpf = r.reads as f64 / r.fetches.max(1) as f64;
        if resident {
            // HIT GUARD, the mirror image. This arm claims to measure the hit path, and it only
            // does so while the pool is actually serving these fetches from memory. If the working
            // set has started missing — an eviction bug, or a pool smaller than it says — the
            // number is about page faults again and the arm is measuring the other thing.
            if rpf > 0.02 {
                eprintln!(
                    "\nHIT GUARD FAILED at {} threads: {:.3} reads per fetch. The resident arm is \
                     supposed to fit in the pool and never reach storage, so this number is about \
                     page faults rather than about cache hits.",
                    r.threads, rpf
                );
                failed = true;
            }
        } else if rpf < 0.98 {
            eprintln!(
                "\nMISS GUARD FAILED at {} threads: {:.3} reads per fetch. The working set is \
                 being served from the pool, so most of these fetches never reached storage and \
                 this number is not about page faults.",
                r.threads, rpf
            );
            failed = true;
        }
    }

    // CALIBRATION. At one thread the pool cannot be serialising anything against anything, so that
    // row is the serialised bound: it is what every other row would report if the pool let exactly
    // one fault proceed at a time. In the modelled mode it is also checkable against 1/delay.
    if let Some(one) = results.iter().find(|r| r.threads == 1) {
        let base = one.fetches as f64 / one.elapsed.as_secs_f64();
        if inst.modelled() {
            let ideal = 1.0 / delay.as_secs_f64();
            println!("\n# calibration: 1 thread measured {base:.0} faults/s, 1/delay = {ideal:.0} faults/s");
            println!("# (sleep granularity puts measured below ideal; the RATIO across thread counts is the result)");
        }
        println!("\n# serialised bound = {base:.0} faults/s at every thread count");
        println!("# threads\tmeasured\tvs_1_thread\tperfect_scaling");
        for r in &results {
            let m = r.fetches as f64 / r.elapsed.as_secs_f64();
            println!(
                "# {}\t{:.0}\t\tx{:.2}\t\tx{:.2}",
                r.threads, m, m / base, r.threads as f64
            );
        }
    }

    if failed {
        std::process::exit(1);
    }
}
