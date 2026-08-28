#!/usr/bin/env python3
"""Apply one named F11 mutant to the working tree. Restore with `git checkout --`."""
import sys, pathlib

LT = 'src/branch/lease_thread.rs'
CLI = 'src/cli/cli.rs'

def sub(path, old, new, count=1):
    p = pathlib.Path(path)
    s = p.read_text()
    n = s.count(old)
    assert n == count, f"{path}: expected {count} occurrence(s) of\n{old!r}\ngot {n}"
    p.write_text(s.replace(old, new))

M = {}

def m(name):
    def d(f):
        M[name] = f
        return f
    return d

@m('M1-no-runtime-lock')
def _():
    sub(LT, """    let mut once = Some(f);
    let mut out = None;
    let mut body = || {
        if let Some(f) = once.take() {
            out = Some(f());
        }
    };
    lock.with_runtime_lock(&mut body);
    out.expect(
        "a RuntimeLock implementation returned without running the lease scan. It must call its \\
         body exactly once; skipping it silently stops a database reaping.",
    )""",
    """    let _ = lock;
    f()""")

@m('M2-no-resume')
def _():
    sub(LT, "let resumed = with_lock(&*lock, || reaper.resume_interrupted_reaps())?;",
            "let resumed: Vec<BranchId> = Vec::new();")

@m('M3-guess-the-clock')
def _():
    sub(LT, """            Err(e) => {
                counters.refused.fetch_add(1, Ordering::SeqCst);
                report(format!(
                    "lease: NOT reaping — this node does not know the cluster's time ({e}). \\
                     Expired branches keep their pages until a LeaseTick is applied; reaping on a \\
                     local clock is the divergence this refusal exists to prevent."
                ));
                return;
            }""",
    """            Err(_e) => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),""")

@m('M4-scan-does-nothing')
def _():
    sub(LT, """    counters.attempts.fetch_add(1, Ordering::SeqCst);
    with_lock(lock, || {""",
    """    counters.attempts.fetch_add(1, Ordering::SeqCst);
    if true {
        let _ = (reaper, runtime, lock);
        return;
    }
    with_lock(lock, || {""")

@m('M5-reap-unconditionally')
def _():
    sub(LT, "match reaper.reap_expired(now) {",
            "let _ = now;\n        match reaper.reap_expired(u64::MAX) {")

@m('M6-never-forget')
def _():
    # Two sites: the resume in `start` and the scan in `scan_once`. Both, so the mutant is
    # "this process never tells the runtime that a branch it holds a workspace for has gone".
    sub(LT, "let forgotten = runtime.forget_reaped_branches();",
            "let _ = &runtime;\n                let forgotten = 0usize;", count=2)

@m('M7-stop-sleeps-the-interval')
def _():
    sub(LT, """        let (guard, _timeout) = self
            .wake
            .wait_timeout(guard, interval)
            .unwrap_or_else(PoisonError::into_inner);
        *guard""",
    """        drop(guard);
        std::thread::sleep(interval);
        *self.stopping.lock().unwrap_or_else(PoisonError::into_inner)""")

@m('M8-drop-does-not-stop')
def _():
    sub(LT, """impl Drop for LeaseThread {
    fn drop(&mut self) {
        self.shutdown();
    }
}""",
    """impl Drop for LeaseThread {
    fn drop(&mut self) {}
}""")

@m('M9-knob-defaults')
def _():
    sub(LT, """    let millis: u64 = match raw.trim().parse() {
        Ok(n) => n,
        Err(e) => return refuse(format!("it takes a whole number of milliseconds ({e})")),
    };
    if millis == 0 {
        return refuse("a zero interval is not a scan period".into());
    }
    if millis > MAX_SCAN_MILLIS {
        return refuse(format!("it is longer than the {MAX_SCAN_MILLIS}ms ceiling"));
    }
    Ok(Duration::from_millis(millis))""",
    """    if false {
        return refuse(String::new());
    }
    let millis: u64 = raw.trim().parse().unwrap_or(DEFAULT_SCAN_MILLIS);
    let millis = if millis == 0 || millis > MAX_SCAN_MILLIS { DEFAULT_SCAN_MILLIS } else { millis };
    Ok(Duration::from_millis(millis))""")

@m('M10-reaper-not-attached')
def _():
    sub(CLI, """        .with_durable_provenance(format!("{db_path}.provenance"))?
        // Retiring a branch now reclaims it. Without this, `seal` took its no-reaper path: a
        // merged or abandoned branch was marked `Reaped` and its extents were never freed, so
        // every `MERGE` and every `ABANDON` in this CLI leaked the branch's pages.
        .with_reaper(reaper.clone() as Arc<dyn Reaper>),""",
    """        .with_durable_provenance(format!("{db_path}.provenance"))?,""")
    sub(CLI, "use crate::branch::{BranchCatalog, Reaper};", "use crate::branch::BranchCatalog;")

if __name__ == '__main__':
    if len(sys.argv) != 2 or sys.argv[1] not in M:
        print('\n'.join(M), file=sys.stderr); sys.exit(2)
    M[sys.argv[1]]()
    print(f"applied {sys.argv[1]}")
