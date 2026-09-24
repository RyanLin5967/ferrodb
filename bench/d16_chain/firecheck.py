#!/usr/bin/env python3
"""D16 chain fire-check: every mutant removes ONE mechanism inside the measured path.

Every mutant edits the engine (`src/`). None edits `examples/d16_chain_retention.rs`: a forced result
at the call site would exercise nothing inside the reaper, the catalog or the store. The pattern is
`bench/d154_mutate.py`'s. What this adds is recorded in `bench/d16_chain/PREREG.md` A2.6:

  * It REFUSES to start unless the cwd is the repository top level and `src/` and `examples/` are
    clean. It re-checks both, and that the file equals its HEAD blob, before EVERY mutant. Restoring
    from git would otherwise destroy uncommitted work, which is how `git checkout --` has eaten a fix
    here before.
  * It restores ONLY the file it mutated, and only while a mutant is applied. After every restore,
    the file's blob hash must equal HEAD's. SIGTERM (what `timeout` sends) and SIGHUP unwind through
    the same `finally`.
  * Raw output goes to a NEW directory per run, `bench/d16_chain/firecheck/<head>-<utc>/`. A re-run
    can neither overwrite an earlier run's raw files nor dirty a committed copy of them.
  * cargo and the harness run in their own process group, and a timeout kills the whole group.
  * A mutant counts only if EVERY cell's verdict, every `compare` line, the build flag and the exit
    code equal the ones pre-registered in PREREG A1.6 and A2.3-A2.4. "Something went red" is not
    enough: the red has to come from the detector the mutant was aimed at.

Run from the worktree root, on a committed tree:

    timeout 14400 python3 bench/d16_chain/firecheck.py

Commit the run's output directory before interpreting it.

Exit 0: both clean baselines MATCH everywhere, A0 refuses, and every mutant fired exactly as
registered. Exit 1: something differed from its registration, or a build failed. Exit 2: could not
run (wrong cwd, dirty tree, the first baseline failed, an anchor did not occur exactly once, or a
restore did not match HEAD).
"""
import datetime
import os
import pathlib
import re
import signal
import subprocess
import sys

EXAMPLE = "d16_chain_retention"
ARGS = ["1,3,10", "2", "fanout,chain,overwrite"]
DEPTHS = (1, 3, 10)
ARMS = ("fanout", "chain", "overwrite")
BUILD_TIMEOUT = 1800
RUN_TIMEOUT = 1800

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
# A baseline may match EITHER registered tree state; the script says which. M13 must read 0.

def slots_main_lineage(arm, d):
    return d if arm == "fanout" else 1


def slots_d200(arm, d):
    return d


def slots_none(arm, d):
    return 0


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
    ("M5-cow-in-place", "src/branch/arena.rs",
     "        if owns_it && header.birth_epoch >= barrier {\n",
     "        if true {\n",
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
COMPARE = re.compile(r"^compare\s+D=(\d+) P=\d+ sha=\S+ (.*)$")
SLOTS = re.compile(r"^slots\s+arm=(\S+) D=(\d+) .*?slots_recycled=(\d+) ")


class Terminated(Exception):
    pass


def on_signal(signum, _frame):
    # Raising turns SIGTERM/SIGHUP into an ordinary unwind, so every `finally` below restores the
    # mutated file. Python's default action for both is to die without running any of them.
    raise Terminated(f"signal {signum}")


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout.strip()


def text(x):
    """`TimeoutExpired` can carry bytes even under `text=True`; normalise either to str."""
    if isinstance(x, bytes):
        return x.decode(errors="replace")
    return x or ""


def run_group(cmd, timeout):
    """Run `cmd` in its own process group. On timeout, kill the WHOLE group: killing only the
    direct child leaves `rustc` grandchildren writing into `target/` under the next build."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            start_new_session=True)
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


def build():
    # `build.rs` stamps the commit and the dirty flag, and it reruns only when `.git/HEAD` or
    # `.git/index` changes. A mutant edits neither, so in a main checkout the stamp could lag the
    # tree. Touching `build.rs` changes its mtime, not its content, and forces the stamp to be
    # recomputed. `main` has already checked that it exists, so this never creates one.
    pathlib.Path("build.rs").touch(exist_ok=True)
    rc, out, err = run_group(["cargo", "build", "--release", "--example", EXAMPLE], BUILD_TIMEOUT)
    return rc, out + err


def run(outdir, label, args):
    exe = target_dir() / "release" / "examples" / EXAMPLE
    rc, stdout, stderr = run_group([str(exe), *args], RUN_TIMEOUT)
    (outdir / f"{label}.txt").write_text(
        f"# {label}: {exe} {' '.join(args)}\n# rc={rc}\n" + stdout + stderr)
    return rc, stdout


def parse(stdout):
    """Verdict per cell, compare per depth, slots per cell, and the build flags seen."""
    cells, compares, slots, builds = {}, {}, {}, set()
    for line in stdout.splitlines():
        m = VERDICT.match(line)
        if m:
            arm, d, _p, _sha, build_flag, rest = m.groups()
            builds.add(build_flag)
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
    return cells, compares, slots, builds


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


def slot_diffs(slots, expect):
    return [f"slots arm={a} D={d}: got {slots.get((a, d))}, registered {expect(a, d)}"
            for a in ARMS for d in DEPTHS if slots.get((a, d)) != expect(a, d)]


# The one file this script has mutated and not yet restored, or None. `restore` touches ONLY that
# file, and only while it is set, so no path through this script can check out anything else.
APPLIED = None


def restore():
    global APPLIED
    if APPLIED is None:
        return
    subprocess.run(["git", "checkout", "--", APPLIED], check=True)
    APPLIED = None


def at_head(path):
    return git("hash-object", path) == git("rev-parse", f"HEAD:{path}")


def tree_dirty():
    return git("status", "--porcelain", "--untracked-files=no", "--", "src/", "examples/")


def baseline(outdir, label):
    rc, out = build()
    if rc != 0:
        print(f"{label}: BUILD FAILED on the clean tree\n{out[-3000:]}", flush=True)
        return False
    rc, stdout = run(outdir, label, ARGS)
    cells, compares, slots, builds = parse(stdout)
    diffs = compare_cells(cells, lambda a, d: MATCH) + compare_lines(compares, cmp_eq)
    if rc != 0:
        diffs.append(f"rc={rc}, registered 0")
    if builds != {"clean"}:
        diffs.append(f"build flags {sorted(builds)}, registered ['clean']")
    main_diffs, d200_diffs = slot_diffs(slots, slots_main_lineage), slot_diffs(slots, slots_d200)
    if not main_diffs:
        tree = "main lineage (D200 absent)"
    elif not d200_diffs:
        tree = "D200 present"
    else:
        tree = "NEITHER registered state"
        diffs += main_diffs
    print(f"{label}: rc={rc} slots={tree} "
          f"{'ALL AS REGISTERED' if not diffs else 'FAILED: ' + '; '.join(diffs[:8])}", flush=True)
    return not diffs


def main():
    global APPLIED
    top = git("rev-parse", "--show-toplevel")
    here = pathlib.Path.cwd().resolve()
    if here != pathlib.Path(top).resolve() or not pathlib.Path("build.rs").is_file():
        print(f"REFUSED: run from the repository top level ({top}); cwd is {here}")
        return 2
    dirty = tree_dirty()
    if dirty:
        print(f"REFUSED: src/ or examples/ has uncommitted changes; restoring from git would destroy "
              f"them:\n{dirty}")
        return 2
    head = git("rev-parse", "--short=12", "HEAD")
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    outdir = pathlib.Path("bench/d16_chain/firecheck") / f"{head}-{stamp}"
    outdir.mkdir(parents=True, exist_ok=False)
    print(f"D16 chain fire-check at {head}; harness args {' '.join(ARGS)}; raw output in {outdir}",
          flush=True)

    if not baseline(outdir, "M0-baseline"):
        print("STOP: the unmutated build must be as registered before any mutant can mean anything.")
        return 2

    # A0: the harness refuses an argument it cannot parse, instead of running a smaller experiment.
    rc, stdout = run(outdir, "A0-bad-argument", ["1,x", "2", "chain"])
    a0 = rc == 2 and "Nothing was run" in stdout
    print(f"A0-bad-argument: rc={rc} {'REFUSED as registered' if a0 else 'DID NOT REFUSE'}",
          flush=True)

    failures = [] if a0 else ["A0-bad-argument"]
    for label, path, old, new, expect, expect_cmp, expect_slots in MUTANTS:
        dirty = tree_dirty()
        if dirty or not at_head(path):
            print(f"{label}: REFUSED, the tree changed underneath this run:\n{dirty or path}",
                  flush=True)
            return 2
        p = pathlib.Path(path)
        original = p.read_text()
        n = original.count(old)
        if n != 1:
            print(f"{label}: NOT RUN, the anchor occurs {n}x in {path}, not once. The source moved; "
                  f"re-register this mutant.", flush=True)
            return 2
        try:
            APPLIED = path
            p.write_text(original.replace(old, new))
            rc, out = build()
            if rc != 0:
                print(f"{label}: BUILD FAILED, so the mutant never ran\n{out[-2000:]}", flush=True)
                failures.append(label)
                continue
            rc, stdout = run(outdir, label, ARGS)
        finally:
            restore()
            if not at_head(path):
                print(f"{label}: RESTORE FAILED, {path} does not match HEAD. Stopping.", flush=True)
                return 2
        cells, compares, slots, builds = parse(stdout)
        diffs = compare_cells(cells, expect) + compare_lines(compares, expect_cmp)
        # G0 fire-check: a mutant binary is built from a dirty tree, so every line must say so and
        # the run must exit 2 whatever its cells said.
        if builds != {"DIRTY"}:
            diffs.append(f"build flags {sorted(builds)}, registered ['DIRTY']")
        if rc != 2:
            diffs.append(f"rc={rc}, registered 2 (G0)")
        if expect_slots is not None:
            diffs += slot_diffs(slots, expect_slots)
        if diffs:
            failures.append(label)
            print(f"{label}: rc={rc} DIFFERED from its registration:", flush=True)
            for d in diffs:
                print(f"    {d}", flush=True)
        else:
            print(f"{label}: rc={rc} FIRED AS REGISTERED ({len(cells)} cells, "
                  f"{len(compares)} compare lines)", flush=True)

    # The restored tree must measure exactly as the first baseline did, and stamp clean again.
    if not baseline(outdir, "M0-baseline-after"):
        failures.append("M0-baseline-after")

    print()
    if failures:
        print(f"RESULT: {len(failures)} did not behave as registered: {', '.join(failures)}")
        return 1
    print(f"RESULT: both baselines as registered, A0 refuses, and all {len(MUTANTS)} mutants fired "
          f"as registered")
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGHUP, on_signal)
    try:
        sys.exit(main())
    finally:
        # A no-op unless a signal or an exception landed between applying a mutant and restoring it.
        restore()
