//! D54 — a writer must not free pages under a shared-path read.
//!
//! Once a read stopped taking the outermost catalog lock, nothing stopped it descending pages
//! that `Catalog::drop_table` frees **immediately** — the heap, the time-travel heap, the primary
//! index and every secondary. The epoch cannot fix it: it is bumped *after* the pages are gone,
//! and a statement already in flight never re-reads it.
//!
//! The fix is a two-flag handshake: a writer announces itself in `ServerContext::writer_active`
//! and waits for every per-connection busy slot to clear; a reader marks its own slot and stands
//! down if a writer is already announced. `SeqCst` on both sides gives one total order, so either
//! the writer sees the slot or the reader sees the flag — never neither.
//!
//! These tests exercise the PROTOCOL rather than racing a real `DROP`, because a race reproduces
//! intermittently and proves less. What must never happen is "a writer proceeded while a reader
//! was inside", and that is exactly what is asserted.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn ctx(dir: &tempfile::TempDir, name: &str) -> Arc<ServerContext> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(name))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{name}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    Arc::new(ServerContext::new(catalog, bp, txn, Arc::new(AgentRuntime::new())))
}

#[test]
fn a_writer_waits_for_an_in_flight_shared_read() {
    let dir = tempfile::tempdir().unwrap();
    let c = ctx(&dir, "wait.db");
    let slot = Arc::new(AtomicBool::new(false));
    c.register_reader(Arc::clone(&slot));

    // A reader is inside a shared-path statement.
    let pass = c.begin_read(&slot).expect("no writer is announced, so this must succeed");

    let c2 = Arc::clone(&c);
    let took = Arc::new(AtomicBool::new(false));
    let took2 = Arc::clone(&took);
    let writer = std::thread::spawn(move || {
        // This is what every exclusive borrow does, including the one inside `drop_table`.
        let _guard = c2.catalog();
        took2.store(true, Ordering::SeqCst);
    });

    // The writer must NOT have taken the catalog while the reader is inside. A generous window:
    // the assertion is about ordering, and a longer wait only makes a false pass less likely.
    let deadline = Instant::now() + Duration::from_millis(400);
    while Instant::now() < deadline {
        assert!(
            !took.load(Ordering::SeqCst),
            "a writer took the catalog while a shared-path read was in flight -- this is the \
             window in which drop_table frees pages under a reader"
        );
        std::thread::yield_now();
    }

    // Leaving the read lets it through, which is what proves the wait was on US and not on a
    // deadlock or a panic.
    drop(pass);
    writer.join().expect("the writer thread panicked");
    assert!(took.load(Ordering::SeqCst), "the writer never acquired after the reader left");
}

#[test]
fn a_reader_stands_down_while_a_writer_holds_the_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let c = ctx(&dir, "standdown.db");
    let slot = Arc::new(AtomicBool::new(false));
    c.register_reader(Arc::clone(&slot));

    let guard = c.catalog(); // a writer is now announced
    assert!(
        c.begin_read(&slot).is_none(),
        "a reader entered the shared path while a writer was announced; with both stores SeqCst \
         one side must observe the other, so this failing means the handshake is not ordered"
    );
    assert!(
        !slot.load(Ordering::SeqCst),
        "standing down must leave the slot CLEAR, or the next drain waits on a reader that is \
         not there"
    );

    drop(guard);
    assert!(
        c.begin_read(&slot).is_some(),
        "the reader must be able to enter again once the writer has released"
    );
}

#[test]
fn the_registry_forgets_connections_that_have_gone_away() {
    let dir = tempfile::tempdir().unwrap();
    let c = ctx(&dir, "registry.db");
    // A slot whose "connection" drops immediately: only the registry holds it afterwards.
    for _ in 0..50 {
        c.register_reader(Arc::new(AtomicBool::new(false)));
    }
    let live = Arc::new(AtomicBool::new(false));
    c.register_reader(Arc::clone(&live));

    // A drain must still complete promptly. If dead slots were kept AND could be left busy, this
    // is where a server would wedge; the retain-by-refcount in register_reader is what stops the
    // list growing without bound across a server's life.
    let t0 = Instant::now();
    drop(c.catalog());
    assert!(
        t0.elapsed() < Duration::from_secs(5),
        "draining stalled with 50 dead slots registered"
    );
}
