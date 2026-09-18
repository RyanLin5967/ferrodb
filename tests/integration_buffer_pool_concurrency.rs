//! D19 — the buffer pool's eviction path under concurrency.
//!
//! D18 fixed a lost-write race in `fetch_page`'s *no-evict* miss path. The **eviction** path has
//! the same shape and was never reached by that test, because eight threads never filled 1024
//! frames: it reads the page table, drops it, flushes, overwrites the frame, and only then
//! republishes the mapping. Check-then-act across independent locks, again.
//!
//! It is also the worse of the two. `frame.data` is replaced *before* the page table is updated,
//! so a concurrent lookup of the evicted page resolves to a frame that already holds the **new**
//! page's bytes. That serves WRONG DATA rather than merely losing a write, and wrong data is the
//! failure a storage engine has no way to apologise for.
//!
//! Each page is stamped with its own id, so "did this fetch return the right page" is decidable
//! from the bytes alone rather than from anything the pool reports about itself.

use std::collections::BTreeSet;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use ferrodb::storage::storage::Storage;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

/// More pages than the pool has frames (1024), so eviction is forced rather than hoped for.
const PAGES: u32 = 1600;

fn stamp(data: &mut [u8; PAGE_SIZE], page_id: u32) {
    data[0..4].copy_from_slice(&page_id.to_be_bytes());
}

fn read_stamp(data: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_be_bytes([data[0], data[1], data[2], data[3]])
}

fn pool(tag: &str) -> (tempfile::TempDir, Arc<BufferPoolManager>, Vec<u32>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));

    // Stamp every page with its own id, then flush so the contents are on disk and the frames can
    // be evicted and reloaded.
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
    (dir, bp, ids)
}

/// Sequential control. If this ever fails, the concurrent result below says nothing about
/// concurrency — it would just mean eviction is broken outright.
#[test]
fn every_page_reads_back_its_own_stamp_single_threaded() {
    let (_d, bp, ids) = pool("seq");
    for &id in &ids {
        let idx = bp.fetch_page(id).expect("fetch");
        let got = read_stamp(&bp.frames[idx].read().unwrap().data);
        bp.unpin_page(id, false);
        assert_eq!(got, id, "page {id} came back holding page {got}");
    }
}

/// **The one D19 exists for.** Many threads, far more pages than frames, every fetch checked
/// against the page it asked for.
#[test]
fn concurrent_fetches_never_return_another_pages_bytes() {
    let (_d, bp, ids) = pool("conc");
    const THREADS: usize = 8;
    const FETCHES: usize = 400;

    let wrong = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            let wrong = Arc::clone(&wrong);
            let checked = Arc::clone(&checked);
            s.spawn(move || {
                for k in 0..FETCHES {
                    // Deterministic but thread-dependent, so the threads collide on some pages
                    // and diverge on others. No RNG, so a failure is reproducible.
                    let id = ids[((k * 7919) + t * 131) % ids.len()];
                    let Ok(idx) = bp.fetch_page(id) else { continue };
                    let got = read_stamp(&bp.frames[idx].read().unwrap().data);
                    bp.unpin_page(id, false);
                    checked.fetch_add(1, Ordering::Relaxed);
                    if got != id {
                        wrong.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    let n = checked.load(Ordering::Relaxed);
    // A run where every fetch was refused would report zero mismatches while testing nothing.
    assert!(
        n > THREADS * FETCHES / 2,
        "only {n} fetches completed of {}; the pool refused most of them and this proves little",
        THREADS * FETCHES
    );
    assert_eq!(
        wrong.load(Ordering::Relaxed),
        0,
        "{} of {n} concurrent fetches returned another page's bytes",
        wrong.load(Ordering::Relaxed)
    );
}

/// The page table must never point two live pages at one frame, which is the state that lets a
/// fetch return the wrong bytes in the first place.
#[test]
fn the_page_table_never_maps_two_pages_to_one_frame() {
    let (_d, bp, ids) = pool("table");
    const THREADS: usize = 8;

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            s.spawn(move || {
                for k in 0..300 {
                    let id = ids[((k * 4099) + t * 61) % ids.len()];
                    if bp.fetch_page(id).is_ok() {
                        bp.unpin_page(id, false);
                    }
                }
            });
        }
    });

    let pt = bp.page_table.read().unwrap();
    let mut seen: std::collections::HashMap<usize, u32> = std::collections::HashMap::new();
    for (&page, &frame) in pt.iter() {
        if let Some(&other) = seen.get(&frame) {
            panic!("frame {frame} is mapped from both page {other} and page {page}");
        }
        seen.insert(frame, page);
    }
    assert!(!seen.is_empty(), "the page table is empty, so nothing was checked");
}

/// A frame's own record of which page it holds must agree with the table that points at it.
/// Disagreement here is precisely the window where `frame.data` has been replaced but the mapping
/// has not caught up.
#[test]
fn every_frame_agrees_with_the_page_table_about_which_page_it_holds() {
    let (_d, bp, ids) = pool("agree");
    const THREADS: usize = 8;

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            s.spawn(move || {
                for k in 0..300 {
                    let id = ids[((k * 3571) + t * 97) % ids.len()];
                    if bp.fetch_page(id).is_ok() {
                        bp.unpin_page(id, false);
                    }
                }
            });
        }
    });

    let pt = bp.page_table.read().unwrap();
    for (&page, &frame) in pt.iter() {
        let f = bp.frames[frame].read().unwrap();
        assert_eq!(
            f.page_id,
            Some(page),
            "the table says frame {frame} holds page {page}, the frame says {:?}",
            f.page_id
        );
        assert_eq!(
            read_stamp(&f.data),
            page,
            "frame {frame} is labelled page {page} but contains page {}",
            read_stamp(&f.data)
        );
    }
}

/// **Reading a page that does not exist must be repeatable, not fatal.**
///
/// It was not. `ArcCache::request` inserts the requested page into its resident set *before*
/// `fetch_page` tries to load it, so a failed load left the cache claiming the page was resident
/// while the page table had no frame for it. The second read took the `Hit` branch, indexed the
/// page table with `[]`, and **panicked the whole process** with `no entry found for key`.
///
/// Three lines reproduce it, and probing whether a page exists is an ordinary thing to do — it is
/// exactly what attaching to an existing branch tree has to do before descending into a root.
#[test]
fn reading_a_page_that_does_not_exist_twice_errors_twice_instead_of_panicking() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("absent.db"))
        .unwrap();
    let bp = BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap()));

    let first = bp.fetch_page(1);
    assert!(first.is_err(), "an empty file returned a page 1: {:?}", first.is_ok());

    // The line that used to abort the process.
    let second = bp.fetch_page(1);
    assert!(second.is_err(), "the second read succeeded where the first failed");

    // And a third, because the fix is that the cache no longer accumulates a false claim — one
    // retry passing by luck would not show that.
    assert!(bp.fetch_page(1).is_err());

    // A page that DOES exist must still be readable afterwards, or the fix would be "break the
    // cache" rather than "keep it honest".
    let real = bp.new_page().expect("allocate");
    assert!(bp.fetch_page(real).is_ok(), "a real page became unreadable after failed probes");
    bp.unpin_page(real, false);
}

// =================================================================================================
// S22 — the two invariants the pool-wide lock used to enforce structurally.
//
// Until S22, `fetch_page` held one process-wide mutex from its first line to every return, across
// `DiskManager::read`. That made both invariants below true by construction and made them true for
// a reason that cost every thread in the process a serialised disk read. S22 replaced the single
// lock with a per-frame latch plus an in-transit marker, so both invariants are now enforced by a
// protocol rather than by exclusion — and a protocol is a thing that can be got wrong.
//
// Each test states the invariant, and each asserts something a "tidied up afterwards" pool would
// still fail: the first samples the invariant *continuously* rather than at the end, and the second
// counts reads that reached the disk rather than inspecting only the state left behind.
// =================================================================================================

/// A real file that counts its `pread`s, so a test can assert how many times a page reached disk.
///
/// This is the instrument for both tests below. "The pool ended up consistent" is a much weaker
/// claim than "the disk was touched exactly once": a pool that loaded one page into two frames and
/// then tidied one away would satisfy the former and fail the latter.
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

/// The same populated pool as [`pool`], over storage whose reads are counted.
fn counting_pool(tag: &str) -> (tempfile::TempDir, Arc<BufferPoolManager>, Vec<u32>, Arc<CountingFile>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let storage = Arc::new(CountingFile { file, reads: AtomicU64::new(0) });
    let counter = Arc::clone(&storage);
    let dm = Arc::new(DiskManager::with_storage(storage as Arc<dyn Storage>).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));

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
    (dir, bp, ids, counter)
}

/// **INVARIANT 1: a pinned page is never evicted.**
///
/// A pin is a promise that the caller may keep reading the frame index it was handed. Breaking it
/// does not lose a write — it serves *another page's bytes* to a reader that is still holding the
/// index, which is the failure a storage engine cannot apologise for.
///
/// The check is a watcher thread sampling the invariant for the whole run, not an assertion at the
/// end. That distinction is the point of the test: an eviction that happened and was subsequently
/// repaired leaves a consistent end state, and an end-state assertion would call it a pass.
#[test]
fn a_pinned_page_is_never_evicted_however_hard_the_pool_churns() {
    let (_d, bp, ids, counter) = counting_pool("pinned");

    // Pinned here and NOT unpinned until the churn is over.
    let pinned = ids[0];
    let home = bp.fetch_page(pinned).expect("pin the page under test");
    assert_eq!(
        read_stamp(&bp.frames[home].read().unwrap().data),
        pinned,
        "precondition: the pinned frame must start out holding its own page"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let violations: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let samples = Arc::new(AtomicU64::new(0));

    let watcher = {
        let (bp, stop, violations, samples) =
            (Arc::clone(&bp), Arc::clone(&stop), Arc::clone(&violations), Arc::clone(&samples));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                samples.fetch_add(1, Ordering::Relaxed);
                let note = |s: String| violations.lock().unwrap().push(s);

                let mapped = bp.page_table.read().unwrap().get(&pinned).copied();
                if mapped != Some(home) {
                    note(format!(
                        "the page table moved pinned page {pinned} out of frame {home} (now {mapped:?})"
                    ));
                }
                let frame = bp.frames[home].read().unwrap();
                if frame.page_id != Some(pinned) {
                    note(format!(
                        "frame {home} was relabelled to {:?} while page {pinned} was pinned in it",
                        frame.page_id
                    ));
                }
                if frame.pin_counter.load(Ordering::Relaxed) == 0 {
                    note(format!(
                        "frame {home} lost its pin count while page {pinned} was still held"
                    ));
                }
                let got = read_stamp(&frame.data);
                if got != pinned {
                    note(format!(
                        "frame {home} holds page {got}'s bytes while page {pinned} is pinned in it"
                    ));
                }
            }
        })
    };

    // Churn: more distinct pages than the pool has frames, so eviction is forced by pigeonhole.
    let reads_before = counter.reads.load(Ordering::Relaxed);
    let mut churn = Vec::new();
    for t in 0..8usize {
        let (bp, ids) = (Arc::clone(&bp), ids.clone());
        churn.push(std::thread::spawn(move || {
            for k in 0..3000usize {
                let id = ids[(t * 7 + k * 13) % ids.len()];
                if id == pinned {
                    continue;
                }
                if let Ok(idx) = bp.fetch_page(id) {
                    let _ = read_stamp(&bp.frames[idx].read().unwrap().data);
                    bp.unpin_page(id, false);
                }
            }
        }));
    }
    for h in churn {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    watcher.join().unwrap();
    let evictions_proxy = counter.reads.load(Ordering::Relaxed) - reads_before;

    // ANTI-VACUITY, both halves. Without these the test passes on a pool that never evicted
    // anything and on a watcher that never ran.
    assert!(
        evictions_proxy > PAGES as u64,
        "the churn caused only {evictions_proxy} disk reads for {PAGES} pages, so eviction \
         pressure was not actually applied and this test proves nothing about pinning"
    );
    let n = samples.load(Ordering::Relaxed);
    assert!(
        n > 1000,
        "the watcher only sampled the invariant {n} times, which is too few to have overlapped \
         the churn: this test would pass without checking anything"
    );

    let v = violations.lock().unwrap();
    assert!(
        v.is_empty(),
        "a pinned page was evicted. {} violations across {n} samples; first 5:\n{}",
        v.len(),
        v.iter().take(5).cloned().collect::<Vec<_>>().join("\n")
    );
    drop(v);

    bp.unpin_page(pinned, false);
}

/// **INVARIANT 2: two threads faulting the same page never load it into two frames.**
///
/// This is the orphaned-frame race. The page table resolves the page to one of the two frames, so
/// every write that lands in the other is silently lost. The old code prevented it by holding a
/// pool-wide lock across the whole miss path; S22 prevents it with the `in_transit` set, so the
/// second thread waits on *that page* rather than on the pool.
///
/// The decisive assertion is the read count, not the end state. A pool that loaded the page twice
/// and left one frame orphaned would still show a single page-table entry.
#[test]
fn concurrent_fetches_of_one_cold_page_read_it_once_into_one_frame() {
    let (_d, bp, ids, counter) = counting_pool("onepage");

    const ROUNDS: usize = 40;
    const THREADS: usize = 16;

    for round in 0..ROUNDS {
        // Cold start, so the fetch below is a guaranteed miss and the threads genuinely race to
        // load it. Nothing is pinned or dirty between rounds, so this cannot refuse.
        bp.invalidate_all().expect("nothing is pinned between rounds");
        let target = ids[(round * 37) % ids.len()];

        let before = counter.reads.load(Ordering::Relaxed);
        let barrier = Arc::new(Barrier::new(THREADS));
        let mut hs = Vec::new();
        for _ in 0..THREADS {
            let (bp, barrier) = (Arc::clone(&bp), Arc::clone(&barrier));
            hs.push(std::thread::spawn(move || {
                barrier.wait();
                let idx = bp.fetch_page(target).expect("fetch the target page");
                let got = read_stamp(&bp.frames[idx].read().unwrap().data);
                bp.unpin_page(target, false);
                (idx, got)
            }));
        }
        let got: Vec<(usize, u32)> = hs.into_iter().map(|h| h.join().unwrap()).collect();
        let reads = counter.reads.load(Ordering::Relaxed) - before;

        // 1. Every thread was handed the same frame.
        let frames_used: BTreeSet<usize> = got.iter().map(|&(i, _)| i).collect();
        assert_eq!(
            frames_used.len(),
            1,
            "round {round}: {THREADS} concurrent fetches of page {target} were handed \
             {} different frames {frames_used:?} — the page is in the pool twice and every write \
             to the frame the page table does not name is lost",
            frames_used.len()
        );

        // 2. And the pool itself carries the label exactly once.
        let labelled: Vec<usize> = (0..bp.frames.len())
            .filter(|&i| bp.frames[i].read().unwrap().page_id == Some(target))
            .collect();
        assert_eq!(
            labelled.len(),
            1,
            "round {round}: page {target} is labelled in frames {labelled:?}"
        );

        // 3. The strong form, and the anti-vacuity check in the same assertion. `> 1` means the
        //    page was loaded more than once even if the pool tidied up afterwards; `0` would mean
        //    the page was already resident and the round raced nothing at all.
        assert_eq!(
            reads, 1,
            "round {round}: {THREADS} concurrent fetches of cold page {target} caused {reads} \
             disk reads, expected exactly 1"
        );

        // 4. And every thread saw that page's own bytes.
        for &(i, s) in &got {
            assert_eq!(
                s, target,
                "round {round}: frame {i} returned page {s}'s bytes for a fetch of page {target}"
            );
        }
    }
}
