#!/usr/bin/env python3
"""D16 chain fire-check: every mutant removes ONE mechanism inside the measured path.

Every mutant edits the engine (`src/`). None edits `examples/d16_chain_retention.rs`: a forced result
at the call site would exercise nothing inside the reaper, the catalog or the store. The pattern is
`bench/d154_mutate.py`'s, plus three changes:

  * it REFUSES to start on a dirty `src/` or `examples/`. Restoring with `git checkout -- src/` would
    otherwise destroy uncommitted work, which is how `git checkout --` has eaten a fix here before.
  * after every restore, each mutated file's blob hash must equal HEAD's. "Restored" is checked, not
    assumed.
  * a mutant counts only if EVERY cell's verdict equals the one pre-registered in
    `bench/d16_chain/PREREG.md`, amendment 1, A1.6. "Something went red" is not enough: the red has
    to come from the detector the mutant was aimed at.

Run from the worktree root, on a committed tree:

    timeout 7200 python3 bench/d16_chain/firecheck.py

Raw harness output for every run goes to `bench/d16_chain/firecheck/<label>.txt`. Commit those
files before interpreting them.

Exit 0: both clean baselines MATCH everywhere, A0 refuses, and every mutant fired exactly as
registered. Exit 1: a mutant differed, or a build failed. Exit 2: could not run (dirty tree, the
first baseline failed, or an anchor did not occur exactly once).
"""
import os
import pathlib
import re
import subprocess
import sys

EXAMPLE = "d16_chain_retention"
ARGS = ["1,3,10", "2", "fanout,chain,overwrite"]
DEPTHS = (1, 3, 10)
ARMS = ("fanout", "chain", "overwrite")
OUT = pathlib.Path("bench/d16_chain/firecheck")
BUILD_TIMEOUT = 1800
RUN_TIMEOUT = 1800

# Keys, exactly as the harness prints them.
RP, RR, RE, PE = "retained_pages", "retained_reserved", "retained_extents", "pending"
AP, AR, AE = "after_leaf_pages", "after_leaf_reserved", "after_leaf_extents"
APD, AD = "after_leaf_pending", "after_leaf_drain"

MATCH = ("=", frozenset())


def mismatch(*keys):
    return ("M", frozenset(keys))


def guards(*ids):
    return ("N", frozenset(ids))


def expect_m1(arm, d):
    """The pin removed: only b(D-1)'s pages, held by the ordinary rule, stay."""
    if arm == "fanout" or d == 1:
        return MATCH
    return mismatch(RP, RR, RE, PE)


def expect_m2(arm, d):
    """A reap skips its drain: the leaf's reap leaves every interior page parked."""
    if arm == "fanout" or d == 1:
        return MATCH
    return mismatch(AP, AR, AE, APD, AD)


def expect_m3(arm, d):
    """The fast path frees nothing: every childless reap keeps its extents."""
    if arm == "fanout":
        return mismatch(AP, AR, AE) if d == 1 else mismatch(RP, RR, RE, AP, AR, AE)
    return mismatch(AP, AR, AE)


def expect_m4(arm, d):
    """release_page stops decrementing the net counter: G2 after the drain that releases."""
    if arm == "fanout" or d == 1:
        return MATCH
    return guards("G2")


def expect_m5(arm, d):
    """cow_page mutates in place: the overwrite arm writes P pages, not P*D, and G6 counts it."""
    if arm != "overwrite" or d == 1:
        return MATCH
    return guards("G1", "G6")


def expect_m6(arm, d):
    """reap_expired skips its first (deepest) candidate in every sweep."""
    return guards("G3") if d == 1 else guards("G3", "G4")


def expect_m7(arm, d):
    """The drain never disarms its DeferTouched guard, so deferred is never empty after a reap."""
    return guards("G7")


# (label, file, exact committed text, mutant text, expectation). Each anchor was checked to occur
# exactly once in `src/` at 9aa6968 with `git grep -nF`; this script re-checks before applying.
MUTANTS = [
    ("M1-pin", "src/branch/table_catalog.rs",
     "            Some(_) if BranchCatalog::has_live_children(self, child_id)? => {\n",
     "            Some(_) if false => {\n",
     expect_m1),
    ("M2-reap-skips-drain", "src/branch/reaper.rs",
     "        freed += self.drain_pending_seeded(own_arenas)?;\n",
     "        let _ = own_arenas;\n",
     expect_m2),
    ("M3-fast-path-frees-nothing", "src/branch/reaper.rs",
     "                freed += self.store.free_arena(arena)?;\n",
     "                let _ = arena;\n",
     expect_m3),
    ("M4-net-counter", "src/branch/arena.rs",
     "            self.live_pages.fetch_sub(1, Ordering::SeqCst);\n",
     "            // M4: the net counter is not decremented\n",
     expect_m4),
    ("M5-cow-in-place", "src/branch/arena.rs",
     "        if owns_it && header.birth_epoch >= barrier {\n",
     "        if true {\n",
     expect_m5),
    ("M6-skip-a-candidate", "src/branch/reaper.rs",
     "        for rec in candidates {\n",
     "        for rec in candidates.into_iter().skip(1) {\n",
     expect_m6),
    ("M7-drain-never-disarms", "src/branch/reaper.rs",
     "        // Disarmed only here, only after the sweep returned Ok.\n        guard.swept = true;\n",
     "        // M7: never disarmed.\n",
     expect_m7),
]

VERDICT = re.compile(
    r"^verdict\s+arm=(\S+) D=(\d+) P=(\d+) sha=(\S+) build=(\S+) (.*)$")


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout.strip()


def target_dir():
    return pathlib.Path(os.environ.get("CARGO_TARGET_DIR", "target"))


def text(x):
    """`TimeoutExpired` carries bytes even under `text=True`; normalise either to str."""
    if isinstance(x, bytes):
        return x.decode(errors="replace")
    return x or ""


def build():
    # `build.rs` stamps the commit and the dirty flag, and it reruns only when `.git/HEAD` or
    # `.git/index` changes. A mutant edits neither. In the main checkout, a mutant built after a
    # clean build would therefore keep the clean stamp, and the restored tree would keep the DIRTY
    # one. Touching `build.rs` changes its mtime, not its content (git stays clean), and forces the
    # stamp to be recomputed on every build here.
    pathlib.Path("build.rs").touch()
    try:
        r = subprocess.run(["cargo", "build", "--release", "--example", EXAMPLE],
                           capture_output=True, text=True, timeout=BUILD_TIMEOUT)
        return r.returncode, r.stdout + r.stderr
    except subprocess.TimeoutExpired as e:
        return 124, text(e.stdout) + text(e.stderr) + f"\n# BUILD TIMEOUT after {BUILD_TIMEOUT}s"


def run(label, args):
    exe = target_dir() / "release" / "examples" / EXAMPLE
    try:
        r = subprocess.run([str(exe), *args], capture_output=True, text=True, timeout=RUN_TIMEOUT)
        rc, stdout, stderr = r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired as e:
        # The harness prints a flushed `progress` line before every phase, so the partial output
        # names the cell and phase that did not finish.
        rc, stdout = 124, text(e.stdout)
        stderr = text(e.stderr) + f"\n# RUN TIMEOUT after {RUN_TIMEOUT}s"
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{label}.txt").write_text(
        f"# {label}: {exe} {' '.join(args)}\n# rc={rc}\n" + stdout + stderr)
    return rc, stdout


def parse(stdout):
    """{(arm, D): (kind, frozenset)} from the verdict lines, plus the build flags they carried."""
    cells, builds = {}, set()
    for line in stdout.splitlines():
        m = VERDICT.match(line)
        if not m:
            continue
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
                g = re.match(r"(G\d)\b", seg)
                ids.add(g.group(1) if g else "unparsed:" + seg[:60])
            v = ("N", frozenset(ids))
        else:
            v = ("?", frozenset([rest]))
        cells[(arm, int(d))] = v
    return cells, builds


def show(v):
    kind, s = v
    return kind if not s else f"{kind} {{{', '.join(sorted(s))}}}"


def compare(cells, expect):
    """Every planned cell present and equal to its registration. Returns a list of differences."""
    diffs = []
    for arm in ARMS:
        for d in DEPTHS:
            want = expect(arm, d)
            got = cells.get((arm, d))
            if got is None:
                diffs.append(f"arm={arm} D={d}: NO VERDICT LINE (want {show(want)})")
            elif got != want:
                diffs.append(f"arm={arm} D={d}: got {show(got)}, registered {show(want)}")
    extra = set(cells) - {(a, d) for a in ARMS for d in DEPTHS}
    for c in sorted(extra):
        diffs.append(f"unplanned cell {c}")
    return diffs


# The one file this script has mutated and not yet restored, or None. `restore` touches ONLY that
# file, and only when it is set. A blanket `git checkout -- src/` in a `finally` would also run on the
# dirty-tree refusal path, and destroy the very work the refusal exists to protect.
APPLIED = None


def restore():
    global APPLIED
    if APPLIED is None:
        return
    subprocess.run(["git", "checkout", "--", APPLIED], check=True)
    APPLIED = None


def restored_exactly(path):
    return git("hash-object", path) == git("rev-parse", f"HEAD:{path}")


def baseline(label):
    rc, out = build()
    if rc != 0:
        print(f"{label}: BUILD FAILED on the clean tree\n{out[-3000:]}", flush=True)
        return False
    rc, stdout = run(label, ARGS)
    cells, builds = parse(stdout)
    diffs = compare(cells, lambda a, d: MATCH)
    ok = rc == 0 and not diffs and builds == {"clean"}
    print(f"{label}: rc={rc} builds={sorted(builds)} "
          f"{'ALL MATCH' if ok else 'FAILED: ' + '; '.join(diffs[:6])}", flush=True)
    return ok


def main():
    global APPLIED
    dirty = git("status", "--porcelain", "--", "src/", "examples/")
    if dirty:
        print(f"REFUSED: src/ or examples/ has uncommitted changes; restoring from git would destroy "
              f"them:\n{dirty}")
        return 2
    head = git("rev-parse", "--short=12", "HEAD")
    print(f"D16 chain fire-check at {head}; harness args {' '.join(ARGS)}", flush=True)

    if not baseline("M0-baseline"):
        print("STOP: the unmutated build must MATCH everywhere before any mutant can mean anything.")
        return 2

    # A0: the harness refuses an argument it cannot parse, instead of running a smaller experiment.
    rc, stdout = run("A0-bad-argument", ["1,x", "2", "chain"])
    a0 = rc == 2 and "Nothing was run" in stdout
    print(f"A0-bad-argument: rc={rc} {'REFUSED as registered' if a0 else 'DID NOT REFUSE'}",
          flush=True)

    failures = [] if a0 else ["A0-bad-argument"]
    for label, path, old, new, expect in MUTANTS:
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
            rc, stdout = run(label, ARGS)
        finally:
            restore()
            if not restored_exactly(path):
                print(f"{label}: RESTORE FAILED, {path} does not match HEAD. Stopping.", flush=True)
                return 2
        cells, _ = parse(stdout)
        diffs = compare(cells, expect)
        if diffs:
            failures.append(label)
            print(f"{label}: rc={rc} DIFFERED from its registration:", flush=True)
            for d in diffs:
                print(f"    {d}", flush=True)
        else:
            print(f"{label}: rc={rc} FIRED AS REGISTERED in all {len(cells)} cells", flush=True)

    # The restored tree must measure exactly as the first baseline did, and stamp clean again.
    if not baseline("M0-baseline-after"):
        failures.append("M0-baseline-after")

    print()
    if failures:
        print(f"RESULT: {len(failures)} did not behave as registered: {', '.join(failures)}")
        return 1
    print(f"RESULT: both baselines MATCH, A0 refuses, and all {len(MUTANTS)} mutants fired as registered")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        # A no-op unless an interrupt landed between applying a mutant and restoring it.
        restore()
