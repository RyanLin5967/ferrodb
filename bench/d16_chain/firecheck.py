#!/usr/bin/env python3
"""D16 chain fire-check: every mutant removes ONE mechanism inside the measured path.

Every mutant edits the engine (`src/`). None edits `examples/d16_chain_retention.rs`: a forced result
at the call site would exercise nothing inside the reaper, the catalog or the store. The pattern is
`bench/d154_mutate.py`'s. What this adds is recorded in `bench/d16_chain/PREREG.md` A2.6, A3.4 and
amendments 4-6:

  * It REFUSES to start unless the cwd is the repository top level and no tracked file is modified.
    It pins HEAD at start. Before EVERY build it re-checks the whole tree and HEAD, and before every
    mutant, the target file's blob.
  * It applies and restores the mutated file atomically (a sibling temp file, then `os.replace`),
    from the exact bytes it read, with SIGTERM, SIGHUP and SIGINT BLOCKED meanwhile: a signal
    arriving then is delivered once the write is done, not lost. The first signal makes every later
    one record-only, so nothing can interrupt the unwind. Every ending goes through `finish`, which
    stops letting signals interrupt, restores, and verifies against HEAD. No git: `git checkout --`
    needs the index lock, and a stale lock left by a killed build would strand the mutant. If the
    file holds anything but what the script last put there (HEAD's bytes included, once the mutant
    reached disk), another writer changed it mid-run. Their bytes are saved to the run directory
    before HEAD's go back, and the run stops with exit 2.
  * Raw output goes to a NEW directory per run, `bench/d16_chain/firecheck/<head>-<utc>/`, with the
    environment in ENV.txt and every build log beside its run. A re-run can neither overwrite an
    earlier run's raw files nor dirty a committed copy of them.
  * cargo and the harness run in their own process group, and a timeout kills the whole group.
  * A mutant counts only if EVERY cell's verdict, every `compare` line, the registered exact values,
    the build flag, the sha and the exit code equal the ones pre-registered in PREREG A1.6, A2.3-A2.4
    and A3.2. "Something went red" is not enough: the red has to come from the detector the mutant
    was aimed at.
  * There is deliberately NO "the two baselines are identical" check. A passing baseline prints
    MATCH in all 9 cells, against predictions that depend only on (arm, D, P), with slots, sha and
    build pinned exactly. Two passing baselines are identical by construction, so such a check could
    never fire (amendment 4).

Run from the worktree root, on a committed tree:

    timeout 14400 python3 bench/d16_chain/firecheck.py

Commit the run's output directory before interpreting it.

Exit 0: both baselines as registered, A0 refuses, and every mutant fired exactly as registered.
Exit 1: something differed from its registration, or a build failed. Exit 2: could not run, or must
not continue: wrong cwd, dirty tree, HEAD moved, the first baseline failed, an anchor did not occur
exactly once, a failed `git grep`, a restore that does not match HEAD, or another writer's edit. This
takes precedence over exit 3. Exit 3: interrupted by SIGTERM, SIGHUP or SIGINT, with the mutated
file, if any, restored and verified against HEAD.
"""
import datetime
import os
import pathlib
import re
import signal
import subprocess
import sys
import traceback

EXAMPLE = "d16_chain_retention"
ARGS = ["1,3,10", "2", "fanout,chain,overwrite"]
DEPTHS = (1, 3, 10)
ARMS = ("fanout", "chain", "overwrite")
BUILD_TIMEOUT = 1800
RUN_TIMEOUT = 1800
# The symbol D200 adds (`wall21-reaped-subtree` @ 17cbd4c). Its presence in `src/` decides which
# registered slot state (A1.4) a baseline must show, so a merge of D200 cannot be mistaken for a
# regression, and nothing else can pass as D200.
D200_MARKER = "unreleased_reaped_candidates"

# Keys, exactly as the harness prints them.
RP, RR, RE, PE = "retained_pages", "retained_reserved", "retained_extents", "pending"
AP, AR, AE = "after_leaf_pages", "after_leaf_reserved", "after_leaf_extents"
APD, AD, AO = "after_leaf_pending", "after_leaf_drain", "after_leaf_orphans"

MATCH = ("=", frozenset())
EQ = ("eq", frozenset())
ABSENT = ("absent", frozenset())


def mismatch(*keys):
    return ("M", frozenset(keys))


def guards(*ids):
    return ("N", frozenset(ids))


def differ(*keys):
    return ("differ", frozenset(keys))


# ---- expected verdict per cell: PREREG A1.6 (M1-M7) and A2.3 (M8-M13) --------------------------

def expect_m1(arm, d):
    """The pin removed: only b(D-1)'s pages, held by the ordinary rule, stay."""
    return MATCH if arm == "fanout" or d == 1 else mismatch(RP, RR, RE, PE)


def expect_m2(arm, d):
    """A reap skips its drain: the leaf's reap leaves every interior page parked."""
    return MATCH if arm == "fanout" or d == 1 else mismatch(AP, AR, AE, APD, AD)


def expect_m3(arm, d):
    """The fast path frees nothing: every childless reap keeps its extents."""
    if arm == "fanout":
        return mismatch(AP, AR, AE) if d == 1 else mismatch(RP, RR, RE, AP, AR, AE)
    return mismatch(AP, AR, AE)


def expect_m4(arm, d):
    """release_page stops decrementing the page counter: G2 after the drain that releases."""
    return MATCH if arm == "fanout" or d == 1 else guards("G2")


def expect_m5(arm, d):
    """cow_page mutates in place: overwrite writes P pages, not P*D, and G6 counts the bad copies."""
    return MATCH if arm != "overwrite" or d == 1 else guards("G1", "G6")


def expect_m6(arm, d):
    """reap_expired skips its first (deepest) candidate in every sweep."""
    return guards("G3") if d == 1 else guards("G3", "G4")


def expect_m7(arm, d):
    """The drain never disarms its DeferTouched guard, so deferred is never empty after a reap."""
    return guards("G7")


def expect_m8(arm, d):
    """The drain does not sweep what it emptied: the collector frees those extents instead."""
    return MATCH if arm == "fanout" or d == 1 else mismatch(AO)


def expect_m9(arm, d):
    """A rule that releases a recorded shadow base: only overwrite has shadow bases."""
    return MATCH if arm != "overwrite" or d == 1 else mismatch(RP, RR, RE, PE)


def expect_m10(arm, d):
    return guards("G1b")


def expect_m11(arm, d):
    return guards("G2b")


def expect_m12(arm, d):
    return guards("G3", "G3b")


def expect_m13(arm, d):
    return MATCH


# ---- expected `compare` line per depth: PREREG A2.4 ----------------------------------------------

def cmp_eq(d):
    return EQ


def cmp_eq_then_absent(d):
    return EQ if d == 1 else ABSENT


def cmp_absent(d):
    return ABSENT


def cmp_m9(d):
    return EQ if d == 1 else differ(RP, RR, RE, PE)


# ---- expected slot line: PREREG A1.4 and A2.4 ----------------------------------------------------

def slots_main_lineage(arm, d):
    return d if arm == "fanout" else 1


def slots_d200(arm, d):
    return d


def slots_none(arm, d):
    return 0


# ---- exact registered values: PREREG A3.2 --------------------------------------------------------

def chains(depths, values):
    return {(arm, d): values(d) for arm in ("chain", "overwrite") for d in depths}


VALUES = {
    "M1-pin": chains((3, 10), lambda d: {RP: 2, RR: 3, RE: 2, PE: 2}),
    "M2-reap-skips-drain": chains((3, 10), lambda d: {
        AP: 2 * (d - 1), AR: 3 * (d - 1), AE: 2 * (d - 1), APD: 2 * (d - 1), AD: 2 * (d - 1)}),
    "M3-fast-path-frees-nothing": {
        ("fanout", 1): {AP: 2, AR: 3, AE: 2},
        ("fanout", 3): {RP: 4, RR: 6, RE: 4, AP: 6, AR: 9, AE: 6},
        ("fanout", 10): {RP: 18, RR: 27, RE: 18, AP: 20, AR: 30, AE: 20},
        **chains(DEPTHS, lambda d: {AP: 2, AR: 3, AE: 2}),
    },
    "M8-drain-skips-its-sweep": chains((3, 10), lambda d: {AO: 2 * (d - 1)}),
    "M9-rule-sees-supersession": {
        ("overwrite", 3): {RP: 2, RR: 3, RE: 2, PE: 2},
        ("overwrite", 10): {RP: 10, RR: 15, RE: 10, PE: 10},
    },
}


# (label, file, exact committed text, mutant text, cell expectation, compare expectation, slots).
# `slots` is None where the slot line is not judged. Each anchor was checked to occur exactly once
# in its file at 9aa6968 with `git grep -nF`; this script re-checks before applying.
MUTANTS = [
    ("M1-pin", "src/branch/table_catalog.rs",
     "            Some(_) if BranchCatalog::has_live_children(self, child_id)? => {\n",
     "            Some(_) if false => {\n",
     expect_m1, cmp_eq, None),
    ("M2-reap-skips-drain", "src/branch/reaper.rs",
     "        freed += self.drain_pending_seeded(own_arenas)?;\n",
     "        let _ = own_arenas;\n",
     expect_m2, cmp_eq, None),
    ("M3-fast-path-frees-nothing", "src/branch/reaper.rs",
     "                freed += self.store.free_arena(arena)?;\n",
     "                let _ = arena;\n",
     expect_m3, cmp_eq, None),
    ("M4-page-counter", "src/branch/arena.rs",
     "            self.live_pages.fetch_sub(1, Ordering::SeqCst);\n",
     "            // M4: the page counter is not decremented\n",
     expect_m4, cmp_eq_then_absent, None),
    # `true ||` rather than `true`, so `owns_it` and `barrier` stay used and the mutant builds even
    # under `-D warnings` (A3.4).
    ("M5-cow-in-place", "src/branch/arena.rs",
     "        if owns_it && header.birth_epoch >= barrier {\n",
     "        if true || (owns_it && header.birth_epoch >= barrier) {\n",
     expect_m5, cmp_eq_then_absent, None),
    ("M6-skip-a-candidate", "src/branch/reaper.rs",
     "        for rec in candidates {\n",
     "        for rec in candidates.into_iter().skip(1) {\n",
     expect_m6, cmp_absent, None),
    ("M7-drain-never-disarms", "src/branch/reaper.rs",
     "        // Disarmed only here, only after the sweep returned Ok.\n        guard.swept = true;\n",
     "        // M7: never disarmed.\n",
     expect_m7, cmp_absent, None),
    ("M8-drain-skips-its-sweep", "src/branch/reaper.rs",
     "        self.sweep_touched_extents(&guard.touched)?;\n",
     "        // M8: the drain does not sweep what it touched\n",
     expect_m8, cmp_eq, None),
    ("M9-rule-sees-supersession", "src/branch/arena.rs",
     "                if !self.catalog.live_child_in_epoch_range(\n",
     "                let superseded =\n"
     "                    self.state.lock().unwrap().shadow_base.values().any(|&(b, _, _)| b == page_id);\n"
     "                if superseded || !self.catalog.live_child_in_epoch_range(\n",
     expect_m9, cmp_m9, None),
    ("M10-depth", "src/branch/record.rs",
     "        let depth = parent_depth.saturating_add(1);\n",
     "        let depth = parent_depth;\n",
     expect_m10, cmp_absent, None),
    ("M11-reserved-counter", "src/branch/arena.rs",
     "            self.reserved_pages.fetch_sub(pages, Ordering::SeqCst);\n",
     "            // M11: the reserved counter is not decremented\n",
     expect_m11, cmp_absent, None),
    ("M12-reap-reports-refused", "src/branch/reaper.rs",
     "            Ok(_) => Ok(ReapOutcome::Reaped),\n",
     "            Ok(_) => Ok(self.refuse(BranchError::NotWritable(branch).into())),\n",
     expect_m12, cmp_absent, None),
    ("M13-no-id-release", "src/branch/table_catalog.rs",
     "        if reusable {\n",
     "        if false && reusable {\n",
     expect_m13, cmp_eq, slots_none),
]

VERDICT = re.compile(r"^verdict\s+arm=(\S+) D=(\d+) P=(\d+) sha=(\S+) build=(\S+) (.*)$")
MEASURED = re.compile(r"^measured\s+arm=(\S+) D=(\d+) P=\d+ sha=\S+ build=\S+ (.*)$")
COMPARE = re.compile(r"^compare\s+D=(\d+) P=\d+ sha=\S+ (.*)$")
SLOTS = re.compile(r"^slots\s+arm=(\S+) D=(\d+) .*?slots_recycled=(\d+) ")


class Terminated(BaseException):
    """BaseException, like KeyboardInterrupt, so no `except Exception` below can swallow a signal
    and mislabel it as a failed restore."""


HANDLED = (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
# The first signal received, by name, or None. `finish` reports it on every exit path, including
# every exit 2 that takes precedence over the interrupt.
INTERRUPTED = None


def record_only(signum, _frame):
    """Note a signal and carry on. Installed after the first signal and for the whole of `finish`, so
    nothing can interrupt the restore-and-verify that ends every run (amendment 6)."""
    global INTERRUPTED
    if INTERRUPTED is None:
        INTERRUPTED = signal.Signals(signum).name


def stop_interrupting():
    for s in HANDLED:
        signal.signal(s, record_only)


def on_signal(signum, _frame):
    """ONE-SHOT. The first SIGTERM, SIGHUP or SIGINT makes every later one record-only, then unwinds
    through the `finally`s that restore the mutated file. Python's default for SIGTERM and SIGHUP is to
    die running none of them. A second signal, which GNU `timeout` can send by signalling the child and
    then its group, can no longer interrupt that unwind."""
    stop_interrupting()
    record_only(signum, _frame)
    raise Terminated(INTERRUPTED)


class held_signals:
    """Block the handled signals for a critical section; one that arrives is delivered at the end
    of it, not lost."""

    def __enter__(self):
        signal.pthread_sigmask(signal.SIG_BLOCK, set(HANDLED))

    def __exit__(self, *_exc):
        signal.pthread_sigmask(signal.SIG_UNBLOCK, set(HANDLED))
        return False


def git(*args):
    # Its own session, so a terminal Ctrl-C reaches only this script, whose handler decides what an
    # interrupt means, and never kills a `git` mid-check (review 6).
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True,
                          start_new_session=True).stdout.strip()


def text(x):
    """`TimeoutExpired` can carry bytes even under `text=True`; normalise either to str."""
    if isinstance(x, bytes):
        return x.decode(errors="replace")
    return x or ""


def run_group(cmd, timeout, env=None):
    """Run `cmd` in its own process group. On timeout or a signal, kill the WHOLE group: killing
    only the direct child leaves `rustc` grandchildren writing into `target/` under the next build."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            start_new_session=True, env=env)
    try:
        out, err = proc.communicate(timeout=timeout)
        return proc.returncode, text(out), text(err)
    except BaseException as e:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        out, err = proc.communicate()
        if isinstance(e, subprocess.TimeoutExpired):
            return 124, text(out), text(err) + f"\n# TIMEOUT after {timeout}s; process group killed"
        raise


def target_dir():
    return pathlib.Path(os.environ.get("CARGO_TARGET_DIR", "target"))


def build(outdir, label):
    # `build.rs` stamps the commit and the dirty flag, and it reruns only when `.git/HEAD` or
    # `.git/index` changes. A mutant edits neither, so in a main checkout the stamp could lag the
    # tree. Touching `build.rs` changes its mtime, not its content, and forces the stamp to be
    # recomputed. `main` has already checked that it exists, so this never creates one.
    pathlib.Path("build.rs").touch(exist_ok=True)
    rc, out, err = run_group(["cargo", "build", "--release", "--example", EXAMPLE], BUILD_TIMEOUT)
    (outdir / f"{label}.build.txt").write_text(f"# rc={rc}\n" + out + err)
    return rc, out + err


def run(outdir, label, args):
    exe = target_dir() / "release" / "examples" / EXAMPLE
    env = dict(os.environ, D16_PERSIST="1")
    rc, stdout, stderr = run_group([str(exe), *args], RUN_TIMEOUT, env)
    (outdir / f"{label}.txt").write_text(
        f"# {label}: {exe} {' '.join(args)}\n# rc={rc}\n" + stdout + stderr)
    return rc, stdout


def parse(stdout):
    """Verdict and measured values per cell, compare per depth, slots per cell, and the build flags
    and shas the verdict lines carried."""
    cells, values, compares, slots, builds, shas = {}, {}, {}, {}, set(), set()
    for line in stdout.splitlines():
        m = VERDICT.match(line)
        if m:
            arm, d, _p, sha, build_flag, rest = m.groups()
            builds.add(build_flag)
            shas.add(sha)
            rest = rest.split(" [run is NOT A RESULT", 1)[0]
            if rest == "MATCH":
                v = MATCH
            elif rest.startswith("MISMATCH "):
                v = mismatch(*rest[len("MISMATCH "):].split(","))
            elif rest.startswith("NOT A RESULT: harness error"):
                v = ("H", frozenset([rest]))
            elif rest.startswith("NOT A RESULT: "):
                ids = set()
                for seg in rest[len("NOT A RESULT: "):].split(" | "):
                    g = re.match(r"(G\d+b?)\b", seg)
                    ids.add(g.group(1) if g else "unparsed:" + seg[:60])
                v = ("N", frozenset(ids))
            else:
                v = ("?", frozenset([rest]))
            cells[(arm, int(d))] = v
            continue
        m = MEASURED.match(line)
        if m:
            kv = dict(tok.split("=", 1) for tok in m.group(3).split())
            values[(m.group(1), int(m.group(2)))] = {k: int(v) for k, v in kv.items()}
            continue
        m = COMPARE.match(line)
        if m:
            d, rest = int(m.group(1)), m.group(2)
            if rest.startswith("overwrite == chain"):
                compares[d] = EQ
            elif rest.startswith("MISMATCH overwrite != chain on "):
                keys = rest[len("MISMATCH overwrite != chain on "):].split(" ", 1)[0]
                compares[d] = differ(*keys.split(","))
            else:
                compares[d] = ("?", frozenset([rest]))
            continue
        m = SLOTS.match(line)
        if m:
            slots[(m.group(1), int(m.group(2)))] = int(m.group(3))
    return cells, values, compares, slots, builds, shas


def show(v):
    kind, s = v
    return kind if not s else f"{kind} {{{', '.join(sorted(s))}}}"


def compare_cells(cells, expect):
    diffs = []
    for arm in ARMS:
        for d in DEPTHS:
            want, got = expect(arm, d), cells.get((arm, d))
            if got is None:
                diffs.append(f"arm={arm} D={d}: NO VERDICT LINE (registered {show(want)})")
            elif got != want:
                diffs.append(f"arm={arm} D={d}: got {show(got)}, registered {show(want)}")
    for c in sorted(set(cells) - {(a, d) for a in ARMS for d in DEPTHS}):
        diffs.append(f"unplanned cell {c}")
    return diffs


def compare_lines(compares, expect):
    diffs = []
    for d in DEPTHS:
        want, got = expect(d), compares.get(d, ABSENT)
        if got != want:
            diffs.append(f"compare D={d}: got {show(got)}, registered {show(want)}")
    return diffs


def compare_values(values, registered):
    diffs = []
    for cell, want in sorted(registered.items()):
        got = values.get(cell, {})
        for key, v in sorted(want.items()):
            if got.get(key) != v:
                diffs.append(f"arm={cell[0]} D={cell[1]} {key}: got {got.get(key)}, registered {v}")
    return diffs


def slot_diffs(slots, expect):
    return [f"slots arm={a} D={d}: got {slots.get((a, d))}, registered {expect(a, d)}"
            for a in ARMS for d in DEPTHS if slots.get((a, d)) != expect(a, d)]


# The file this script has mutated and not yet restored, the exact BYTES it read from that file, and
# the bytes it wrote there. `restore` writes back ONLY those bytes, to ONLY that file, and only while
# it is set. Bytes, not text: the sources hold non-ASCII, and a text round trip depends on the
# locale's encoding and on newline translation, either of which could make the "restored" file
# differ from HEAD.
APPLIED = None
APPLIED_BYTES = None
APPLIED_MUTANT = None
# True once the mutant's bytes are on disk. Set with signals held, together with the write.
APPLIED_ON_DISK = False
# The last file ever mutated, so an interrupt can verify it against HEAD; where evidence goes; and
# where another writer's bytes were saved, if restore found any.
LAST_PATH = None
EVIDENCE_DIR = None
FOREIGN_EDIT = None
# Whether the loop has already named FOREIGN_EDIT, so `finish` does not name it twice.
FOREIGN_REPORTED = False


def atomic_write(path, data):
    """Write `data` to `path` so that a reader, or a crash, sees the old bytes or the new ones and
    never a truncated file. The temp name does not end in `.rs`, so cargo never compiles it."""
    tmp = pathlib.Path(str(path) + ".d16-firecheck.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def restore():
    """Put HEAD's bytes back in the mutated file. Signals are BLOCKED, not ignored: one that lands
    here is delivered once the write is done, so `timeout` still ends the run instead of being lost.

    Blind spot (amendment 5): a foreign write landing between this read and the `os.replace` is
    overwritten and not saved."""
    global APPLIED, APPLIED_BYTES, APPLIED_MUTANT, APPLIED_ON_DISK, FOREIGN_EDIT
    with held_signals():
        if APPLIED is None:
            return
        p = pathlib.Path(APPLIED)
        current = p.read_bytes() if p.exists() else b""
        # What should be there now: the mutant once it reached disk, the original before. Anything
        # else, INCLUDING HEAD's bytes after the mutant reached disk (a foreign checkout, stash or
        # reset), is another writer's edit (amendment 6). Keep it as evidence before HEAD's go back.
        expected = APPLIED_MUTANT if APPLIED_ON_DISK else APPLIED_BYTES
        if current != expected:
            dest = (EVIDENCE_DIR or pathlib.Path(".")) / (APPLIED.replace("/", "_") + ".foreign-edit")
            dest.write_bytes(current)
            FOREIGN_EDIT = str(dest)
        atomic_write(p, APPLIED_BYTES)
        APPLIED, APPLIED_BYTES, APPLIED_MUTANT, APPLIED_ON_DISK = None, None, None, False


def apply_mutant(path, original, mutated):
    """Record what is about to be applied, then apply it, with signals held across both, so no
    interrupt can observe one without the other."""
    global APPLIED, APPLIED_BYTES, APPLIED_MUTANT, APPLIED_ON_DISK, LAST_PATH
    with held_signals():
        APPLIED_BYTES, APPLIED_MUTANT, LAST_PATH, APPLIED_ON_DISK = original, mutated, path, False
        APPLIED = path
        atomic_write(pathlib.Path(path), mutated)
        APPLIED_ON_DISK = True


def tree_moved(head_full):
    """Why this run must not build again, or None. Checked before every build."""
    dirty = tree_dirty()
    if dirty:
        return f"tracked files are modified:\n{dirty}"
    now = git("rev-parse", "HEAD")
    if now != head_full:
        return f"HEAD moved from {head_full} to {now}"
    return None


def at_head(path):
    return git("hash-object", path) == git("rev-parse", f"HEAD:{path}")


def tree_dirty():
    # Every tracked file, because `build.rs` stamps DIRTY for any of them, not only src/ and
    # examples/. Untracked files (this run's own output) do not count, and do not count there either.
    return git("status", "--porcelain", "--untracked-files=no")


def baseline(outdir, label, expect_slots, head):
    rc, out = build(outdir, label)
    if rc != 0:
        print(f"{label}: BUILD FAILED on the clean tree\n{out[-3000:]}", flush=True)
        return False
    rc, stdout = run(outdir, label, ARGS)
    cells, _values, compares, slots, builds, shas = parse(stdout)
    diffs = compare_cells(cells, lambda a, d: MATCH) + compare_lines(compares, cmp_eq)
    diffs += slot_diffs(slots, expect_slots)
    if rc != 0:
        diffs.append(f"rc={rc}, registered 0")
    if builds != {"clean"}:
        diffs.append(f"build flags {sorted(builds)}, registered ['clean']")
    if shas != {head}:
        diffs.append(f"sha {sorted(shas)}, pinned {head}")
    print(f"{label}: rc={rc} "
          f"{'ALL AS REGISTERED' if not diffs else 'FAILED: ' + '; '.join(diffs[:8])}", flush=True)
    return not diffs


def main():
    global EVIDENCE_DIR, FOREIGN_REPORTED
    top = git("rev-parse", "--show-toplevel")
    here = pathlib.Path.cwd().resolve()
    if here != pathlib.Path(top).resolve() or not pathlib.Path("build.rs").is_file():
        print(f"REFUSED: run from the repository top level ({top}); cwd is {here}")
        return 2
    unknown = sorted(set(VALUES) - {m[0] for m in MUTANTS})
    if unknown:
        print(f"REFUSED: VALUES names no mutant: {unknown}. A misspelt key would check nothing.")
        return 2
    dirty = tree_dirty()
    if dirty:
        print(f"REFUSED: tracked files are modified; the build would stamp DIRTY, and a restore would "
              f"overwrite an edit to any file it mutates:\n{dirty}")
        return 2
    head_full = git("rev-parse", "HEAD")
    head = git("rev-parse", "--short=12", "HEAD")
    grep = subprocess.run(["git", "grep", "-q", "-F", D200_MARKER, "HEAD", "--", "src/"])
    if grep.returncode not in (0, 1):
        print(f"REFUSED: `git grep` for the D200 marker failed (rc={grep.returncode}); it must not "
              f"read as 'absent'.")
        return 2
    d200 = grep.returncode == 0
    expect_slots = slots_d200 if d200 else slots_main_lineage
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    outdir = pathlib.Path("bench/d16_chain/firecheck") / f"{head}-{stamp}"
    outdir.mkdir(parents=True, exist_ok=False)
    EVIDENCE_DIR = outdir
    (outdir / "ENV.txt").write_text(
        f"head={head_full}\nd200_marker_in_src={d200}\n"
        f"RUSTFLAGS={os.environ.get('RUSTFLAGS', '<unset>')}\n"
        f"CARGO_TARGET_DIR={os.environ.get('CARGO_TARGET_DIR', '<unset>')}\n"
        f"harness D16_PERSIST=1 (pinned)\nargs={' '.join(ARGS)}\n")
    print(f"D16 chain fire-check at {head}; slot state expected: "
          f"{'D200 present' if d200 else 'main lineage (D200 absent)'}; raw output in {outdir}",
          flush=True)

    if not baseline(outdir, "M0-baseline", expect_slots, head):
        print("STOP: the unmutated build must be as registered before any mutant can mean anything.")
        return 2

    # A0: the harness refuses an argument it cannot parse, instead of running a smaller experiment.
    rc, stdout = run(outdir, "A0-bad-argument", ["1,x", "2", "chain"])
    a0 = rc == 2 and "Nothing was run" in stdout
    print(f"A0-bad-argument: rc={rc} {'REFUSED as registered' if a0 else 'DID NOT REFUSE'}",
          flush=True)

    failures = [] if a0 else ["A0-bad-argument"]
    for label, path, old, new, expect, expect_cmp, mutant_slots in MUTANTS:
        moved = tree_moved(head_full)
        if moved or not at_head(path):
            print(f"{label}: REFUSED, the tree changed underneath this run: {moved or path}",
                  flush=True)
            return 2
        p = pathlib.Path(path)
        original = p.read_bytes()
        n = original.count(old.encode())
        if n != 1:
            print(f"{label}: NOT RUN, the anchor occurs {n}x in {path}, not once. The source moved; "
                  f"re-register this mutant.", flush=True)
            return 2
        mutated = original.replace(old.encode(), new.encode())
        try:
            apply_mutant(path, original, mutated)
            rc, out = build(outdir, label)
            if rc != 0:
                print(f"{label}: BUILD FAILED, so the mutant never ran\n{out[-2000:]}", flush=True)
                failures.append(label)
                continue
            rc, stdout = run(outdir, label, ARGS)
        finally:
            # The ordinary restore. A signal landing anywhere in here is a Terminated
            # (BaseException): nothing below catches it, and `__main__` restores and verifies
            # instead. A `return 2` here does discard an in-flight interrupt, but `__main__` still
            # reports it through INTERRUPTED.
            try:
                restore()
            except Exception as e:
                print(f"{label}: RESTORE FAILED ({e!r}); {path} may not hold HEAD's bytes. "
                      f"Stopping.{foreign_note()}", flush=True)
                FOREIGN_REPORTED = FOREIGN_REPORTED or FOREIGN_EDIT is not None
                return 2
            if not at_head(path):
                print(f"{label}: RESTORE FAILED, {path} does not match HEAD. Stopping.{foreign_note()}",
                      flush=True)
                FOREIGN_REPORTED = FOREIGN_REPORTED or FOREIGN_EDIT is not None
                return 2
            if FOREIGN_EDIT:
                print(f"{label}: {path} was changed by another writer during this run. Their bytes "
                      f"are saved at {FOREIGN_EDIT}; HEAD's are back (verified). Stopping.",
                      flush=True)
                FOREIGN_REPORTED = True
                return 2
        cells, values, compares, slots, builds, shas = parse(stdout)
        diffs = compare_cells(cells, expect) + compare_lines(compares, expect_cmp)
        diffs += compare_values(values, VALUES.get(label, {}))
        # G0 fire-check: a mutant binary is built from a dirty tree, so every line must say so and
        # the run must exit 2 whatever its cells said. It proves the tree was dirty, not which
        # mutant it was (A3.1); the registered pattern and values above identify the mutant.
        if builds != {"DIRTY"}:
            diffs.append(f"build flags {sorted(builds)}, registered ['DIRTY']")
        if shas != {head}:
            diffs.append(f"sha {sorted(shas)}, pinned {head}")
        if rc != 2:
            diffs.append(f"rc={rc}, registered 2 (G0)")
        if mutant_slots is not None:
            diffs += slot_diffs(slots, mutant_slots)
        if diffs:
            failures.append(label)
            print(f"{label}: rc={rc} DIFFERED from its registration:", flush=True)
            for d in diffs:
                print(f"    {d}", flush=True)
        else:
            print(f"{label}: rc={rc} FIRED AS REGISTERED ({len(cells)} cells, "
                  f"{len(compares)} compare lines, {len(VALUES.get(label, {}))} valued cells)",
                  flush=True)

    # The restored tree must pass on its own. It is not compared with the first baseline: two passing
    # baselines are identical by construction (amendment 4), so that comparison could never fire.
    moved = tree_moved(head_full)
    if moved:
        print(f"M0-baseline-after: REFUSED, the tree changed underneath this run: {moved}")
        return 2
    if not baseline(outdir, "M0-baseline-after", expect_slots, head):
        failures.append("M0-baseline-after")

    print()
    if failures:
        print(f"RESULT: {len(failures)} did not behave as registered: {', '.join(failures)}")
        return 1
    print(f"RESULT: both baselines as registered, A0 refuses, and all {len(MUTANTS)} mutants fired "
          f"as registered")
    return 0


def finish(code):
    """The one exit path (amendments 5-6). Whatever `main` did: stop letting signals interrupt, restore,
    verify the last mutated file against HEAD, and let a tree not at HEAD, or a foreign edit, override
    every other outcome. An interrupt is reported on every path."""
    stop_interrupting()
    try:
        restore()
    except Exception as e:
        print(f"RESTORE FAILED ({e!r}); {LAST_PATH} may not hold HEAD's bytes. Exit 2."
              f"{foreign_note()}{interrupt_note()}")
        return 2
    try:
        verified = LAST_PATH is None or at_head(LAST_PATH)
    except Exception as e:
        print(f"Could not verify {LAST_PATH} against HEAD ({e!r}). Exit 2."
              f"{foreign_note()}{interrupt_note()}")
        return 2
    if not verified:
        print(f"{LAST_PATH} does NOT match HEAD. Exit 2.{foreign_note()}{interrupt_note()}")
        return 2
    if FOREIGN_EDIT:
        if not FOREIGN_REPORTED:
            print(f"{LAST_PATH} had been changed by another writer; their bytes are saved at "
                  f"{FOREIGN_EDIT}, and HEAD's are back (verified).")
        print(f"Exit 2 (foreign edit).{interrupt_note()}")
        return 2
    checked = ("no mutant was ever applied" if LAST_PATH is None
               else f"{LAST_PATH} matches HEAD (verified)")
    if code == 3:
        print(f"INTERRUPTED by {INTERRUPTED}; {checked}. Exit 3.")
    elif INTERRUPTED:
        print(f"An interrupt ({INTERRUPTED}) also arrived; exit {code} takes precedence; {checked}.")
    return code


def interrupt_note():
    return f" An interrupt ({INTERRUPTED}) also arrived." if INTERRUPTED else ""


def foreign_note():
    """Name a foreign edit on EVERY exit that mentions the file, including the ones where the file
    then also failed its HEAD check (review 6, R6-1)."""
    if FOREIGN_EDIT is None:
        return ""
    return f" Another writer had changed it; their bytes are saved at {FOREIGN_EDIT}."


if __name__ == "__main__":
    for s in HANDLED:
        signal.signal(s, on_signal)
    outcome = 1
    try:
        try:
            outcome = main()
        except Exception:
            stop_interrupting()
            traceback.print_exc()
            outcome = 1
        # Disarm INSIDE the try that catches a first signal. A signal landing anywhere up to here
        # becomes outcome 3; after this line every signal is record-only, so nothing between here and
        # the end of `finish` can escape with a traceback and skip the restore.
        stop_interrupting()
    except Terminated:
        outcome = 3
    sys.exit(finish(outcome))
