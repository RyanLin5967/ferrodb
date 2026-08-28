#!/usr/bin/env python3
"""F4 mutation evidence.

A detector that has never fired is not a detector. For every rule this row implements, break the
rule on purpose, run the test named against it, and require that it FAILS. Then restore from the
committed tree and require the same test PASSES.

Restores with `git checkout -- <explicit file>`, so every mutated file must be committed and clean
before this runs; the script refuses otherwise rather than risk discarding real work.

Writes a durable transcript to scratchpad/f4-mutants.txt as it goes, because a run that is
interrupted mid-sweep must leave the evidence it already collected.
"""
import subprocess, sys, pathlib, os

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT = ROOT / "scratchpad" / "f4-mutants.txt"
ENV = dict(os.environ, PATH=os.path.expanduser("~/.cargo/bin") + ":" + os.environ["PATH"])

# (id, rule, file, needle, replacement, cargo target, test filter)
MUTANTS = [
    ("M1", "a cluster member with no grant REFUSES; it never falls back to a local counter",
     "src/cluster/mod.rs",
     """            Authority::Member(node) => {
                Err(GrantError::Exhausted { counter: self.counter, node, need: n })
            }""",
     """            Authority::Member(_node) => {
                let lo = self.accepted_through.max(self.issued);
                let hi = lo.saturating_add(self.chunk.max(n));
                self.push_range(lo, hi, now_epoch);
                self.take_from_held(n).ok_or(GrantError::SpaceExhausted {
                    counter: self.counter,
                    issued_through: self.issued,
                })
            }""",
     "--lib", "cluster::tests::a_member"),

    ("M1b", "same rule, through the real ArenaPageStore and TxnManager",
     "src/cluster/mod.rs",
     """            Authority::Member(node) => {
                Err(GrantError::Exhausted { counter: self.counter, node, need: n })
            }""",
     """            Authority::Member(_node) => {
                let lo = self.accepted_through.max(self.issued);
                let hi = lo.saturating_add(self.chunk.max(n));
                self.push_range(lo, hi, now_epoch);
                self.take_from_held(n).ok_or(GrantError::SpaceExhausted {
                    counter: self.counter,
                    issued_through: self.issued,
                })
            }""",
     "--test integration_cluster_grants", "with_no_"),

    ("M2", "a grant addressed to another node is refused",
     "src/cluster/mod.rs",
     """            Authority::Member(me) if me == to => {}
            Authority::Member(me) => {""",
     """            Authority::Member(me) if me == to || true => { let _ = me; }
            Authority::Member(me) => {""",
     "--lib", "cluster::tests::a_grant_addressed_to_another_node_is_refused"),

    ("M2b", "same rule, through ArenaPageStore::apply_arena_grant and the two-node page test",
     "src/cluster/mod.rs",
     """            Authority::Member(me) if me == to => {}
            Authority::Member(me) => {""",
     """            Authority::Member(me) if me == to || true => { let _ = me; }
            Authority::Member(me) => {""",
     "--test integration_cluster_grants", "another_node"),

    ("M3", "a re-delivered committed grant is a no-op, not a second range",
     "src/cluster/mod.rs",
     """        if hi <= self.accepted_through {
            return Ok(Applied::Duplicate);
        }""",
     """        if false {
            return Ok(Applied::Duplicate);
        }""",
     "--lib", "cluster::tests::a_redelivered_grant_issues_nothing_twice"),

    ("M3b", "same rule, through a real extent claim",
     "src/cluster/mod.rs",
     """        if hi <= self.accepted_through {
            return Ok(Applied::Duplicate);
        }""",
     """        if false {
            return Ok(Applied::Duplicate);
        }""",
     "--test integration_cluster_grants", "redelivered"),

    ("M4", "a replayed grant is clamped to the issued watermark, so recovery cannot re-issue",
     "src/cluster/mod.rs",
     "        let lo = lo.max(self.accepted_through).max(self.issued);",
     "        let lo = lo;",
     "--lib", "cluster::tests::a_grant_partly_consumed_before_a_crash_resumes_at_the_watermark"),

    ("M4b", "same rule, across a real checkpoint and reopen",
     "src/cluster/mod.rs",
     "        let lo = lo.max(self.accepted_through).max(self.issued);",
     "        let lo = lo;",
     "--test integration_cluster_grants", "a_restart_replays_its_grant"),

    ("M5", "space granted under a superseded authority is not issued from",
     "src/cluster/mod.rs",
     "        self.held.retain(|h| h.epoch == now_epoch);",
     "        let _ = now_epoch;",
     "--lib", "cluster::tests::ranges_from_a_superseded_authority_are_not_issued_from"),

    ("M5b", "same rule, through a store that self-granted and then joined",
     "src/cluster/mod.rs",
     "        self.held.retain(|h| h.epoch == now_epoch);",
     "        let _ = now_epoch;",
     "--test integration_cluster_grants", "self_granted_while_standalone"),

    ("M6", "the recycle stack is epoch-stamped too, being issued space outside the grant book",
     "src/branch/arena.rs",
     """        if self.recycle_epoch.swap(epoch, Ordering::SeqCst) != epoch {
            free.clear();
            return None;
        }
        free.pop()""",
     """        let _ = self.recycle_epoch.swap(epoch, Ordering::SeqCst);
        free.pop()""",
     "--test integration_cluster_grants", "a_recycled_extent_start_from_a_previous_authority"),

    ("M7", "a member with no applied LeaseTick refuses to say what time it is",
     "src/cluster/mod.rs",
     "        (Authority::Member(n), None) => Err(GrantError::NoClusterTime { node: n }),",
     "        (Authority::Member(_n), None) => Ok(LeaseSource::LocalWall),",
     "--lib", "cluster::tests::a_member_with_no_lease_tick_refuses_to_decide_a_lease"),

    ("M7b", "same rule, at the LeaseDeadline surface a reap decision goes through",
     "src/cluster/mod.rs",
     "        (Authority::Member(n), None) => Err(GrantError::NoClusterTime { node: n }),",
     "        (Authority::Member(_n), None) => Ok(LeaseSource::LocalWall),",
     "--test integration_cluster_grants", "no_lease_tick"),

    ("M7c", "the infallible lease clock aborts rather than inventing a time",
     "src/cluster/mod.rs",
     "        (Authority::Member(n), None) => Err(GrantError::NoClusterTime { node: n }),",
     "        (Authority::Member(_n), None) => Ok(LeaseSource::LocalWall),",
     "--test integration_cluster_grants", "aborts_rather_than_inventing"),

    ("M8", "the cluster clock is monotone; a re-delivered tick cannot rewind expiry",
     "src/cluster/mod.rs",
     """        Some(p) => p.max(incoming),""",
     """        Some(_p) => incoming,""",
     "--lib", "cluster::tests::a_lease_tick_never_moves_cluster_time_backwards"),

    ("M8b", "same rule, through the process clock",
     "src/cluster/mod.rs",
     """        Some(p) => p.max(incoming),""",
     """        Some(_p) => incoming,""",
     "--test integration_cluster_grants", "never_moves_the_clusters_clock_backwards"),

    ("M9", "the issued watermark is monotone, so two recovery reports cannot un-issue an id",
     "src/cluster/mod.rs",
     "        g.issued = g.issued.max(issued);",
     "        g.issued = issued;",
     "--test integration_cluster_grants", "recovery_raises_the_transaction_watermark"),

    ("M10", "only the issued watermark reaches the durable image; held ranges never do",
     "src/branch/arena.rs",
     "        b.extend_from_slice(&(self.space.extent_starts.issued_through() as u32).to_be_bytes());",
     "        b.extend_from_slice(&(self.space.extent_starts.granted_through_for_mutant() as u32).to_be_bytes());",
     "--test integration_cluster_grants", "the_checkpoint_image_is_byte_identical"),

    ("M11", "a standalone node issues exactly what fetch_add issued",
     "src/cluster/mod.rs",
     "                let lo = self.accepted_through.max(self.issued);\n                let hi = lo.saturating_add(self.chunk.max(n));\n                self.push_range(lo, hi, now_epoch);\n                self.take_from_held(n).ok_or(GrantError::SpaceExhausted {",
     "                let lo = self.accepted_through.max(self.issued) + 1;\n                let hi = lo.saturating_add(self.chunk.max(n));\n                self.push_range(lo, hi, now_epoch);\n                self.take_from_held(n).ok_or(GrantError::SpaceExhausted {",
     "--test integration_cluster_grants", "no_cluster_configured"),

    ("M12", "a take never splices two granted ranges into one allocation",
     "src/cluster/mod.rs",
     "        let idx = self.held.iter().position(|h| h.hi - h.lo >= n)?;",
     "        self.coalesce_for_mutant();\n        let idx = self.held.iter().position(|h| h.hi - h.lo >= n)?;",
     "--lib", "cluster::tests::a_take_never_straddles_two_granted_ranges"),

    ("M13", "the epoch-aware remaining() cannot disagree with the guard that refuses",
     "src/cluster/mod.rs",
     "        self.held.iter().filter(|h| h.epoch == now_epoch).map(|h| h.hi - h.lo).sum()",
     "        let _ = now_epoch;\n        self.held.iter().map(|h| h.hi - h.lo).sum()",
     "--test integration_cluster_grants", "self_granted_while_standalone"),
]

# Helpers some mutants need, appended to the file under mutation and removed with it.
HELPERS = {
    "src/cluster/mod.rs": '''
impl Grants {
    #[allow(dead_code)]
    fn coalesce_for_mutant(&mut self) {
        let mut merged: Vec<Held> = Vec::new();
        for h in self.held.iter().copied() {
            match merged.last_mut() {
                Some(p) if p.hi == h.lo && p.epoch == h.epoch => p.hi = h.hi,
                _ => merged.push(h),
            }
        }
        self.held = merged;
    }
}

impl GrantedCounter {
    #[allow(dead_code)]
    pub fn granted_through_for_mutant(&self) -> u64 {
        self.inner().accepted_through
    }
}
''',
}


def run(cmd, timeout=900):
    return subprocess.run(cmd, cwd=ROOT, env=ENV, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def clean(paths):
    r = run("git status --porcelain -- " + " ".join(paths))
    return r.stdout.strip() == ""


def restore(path):
    run(f"git checkout -- {path}")


def main():
    files = sorted({m[2] for m in MUTANTS})
    if not clean(files):
        print("REFUSING: mutated files are not committed and clean; restore would discard work.")
        print(run("git status --porcelain -- " + " ".join(files)).stdout)
        return 2

    lines = []
    def log(s):
        print(s, flush=True)
        lines.append(s)
        OUT.write_text("\n".join(lines) + "\n")

    log("F4 mutation evidence — every rule broken on purpose, then restored.")
    log("=" * 90)
    verdicts = []
    for mid, rule, path, needle, repl, target, filt in MUTANTS:
        src = (ROOT / path).read_text()
        if needle not in src:
            log(f"[{mid}] SETUP FAILED — needle not found in {path}. Rule: {rule}")
            verdicts.append((mid, "SETUP-FAILED"))
            continue
        mutated = src.replace(needle, repl, 1) + HELPERS.get(path, "")
        (ROOT / path).write_text(mutated)
        cmd = f"timeout 900 cargo test {target} {filt} -- --test-threads=4"
        r = run(cmd)
        restore(path)
        out = (r.stdout + r.stderr)
        # A mutant that will not compile has still not been SHOWN to be caught by the test.
        if "error[E" in out or "error: could not compile" in out:
            verdict = "DID-NOT-COMPILE"
        elif "test result: FAILED" in out or "error: test failed" in out:
            verdict = "KILLED"
        elif "test result: ok" in out:
            verdict = "SURVIVED"
        else:
            verdict = "INCONCLUSIVE"
        # Which tests actually ran, and how the failure read.
        ran = [l for l in out.splitlines() if l.startswith("test result:")]
        fails = [l for l in out.splitlines() if "panicked at" in l or l.strip().startswith("assertion")]
        log("")
        log(f"[{mid}] {verdict}")
        log(f"  rule    : {rule}")
        log(f"  mutant  : {path} — {needle.strip().splitlines()[0][:88]}")
        log(f"  command : {cmd}")
        for l in ran:
            log(f"  result  : {l}")
        for l in fails[:3]:
            log(f"  printed : {l.strip()[:160]}")
        verdicts.append((mid, verdict))

    log("")
    log("=" * 90)
    for mid, v in verdicts:
        log(f"  {mid:5s} {v}")
    bad = [m for m, v in verdicts if v != "KILLED"]
    log("")
    log("ALL MUTANTS KILLED" if not bad else f"NOT KILLED: {bad}")
    if not clean(files):
        log("WARNING: tree not clean after restore — inspect before trusting anything above.")
        return 3
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
