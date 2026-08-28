#!/usr/bin/env python3
"""Break each F2 rule on purpose, run the suite, and record what fired.

A test that has not been seen to fail is not evidence. This applies one exact-string mutation to
src/consensus/replicate.rs at a time, runs `cargo test --lib consensus::`, records which named
tests failed, and restores the file. It refuses to report anything if the baseline is not green or
if a mutation did not apply, because a mutant that never compiled is indistinguishable from one
that was killed.
"""

import os
import re
import shutil
import subprocess
import sys

SRC = "src/consensus/replicate.rs"
LOG = "scratchpad/F2-mutants.log"
ENV = dict(os.environ, PATH=os.path.expanduser("~/.cargo/bin") + ":" + os.environ["PATH"])

# (id, rule it breaks, exact source text, replacement)
MUTANTS = [
    ("M1", "quorum counted over `next` instead of `matched`",
     "                Some(p) if p.diverged.is_none() => p.matched,",
     "                Some(p) if p.diverged.is_none() => p.next,"),

    ("M2", "Raft 5.4.2 dropped: an inherited round commits by replica count",
     "        if self.term_at(q) != Some(self.hard.term) {",
     "        if false {"),

    ("M3", "the 5.4.2 term check reads the log TAIL instead of the round being committed",
     "        if self.term_at(q) != Some(self.hard.term) {",
     "        if self.last_term != self.hard.term {"),

    ("M4", "the conflict hint is `prev_round`: a backwards probe, one trip per diverged round",
     "                let hint = self.first_round_of_term_run(prev_round);",
     "                let hint = prev_round;"),

    ("M5", "the follower acknowledges on receipt instead of on fsync",
     "            out.push(Action::Persist { entries: fresh });",
     "            out.push(Action::Persist { entries: fresh });\n            out.push(self.acknowledgement(from));"),

    ("M6", "the follower clamps the leader's commit to its own tail, not to what the append proved",
     "        let learned = leader_commit.min(self.agreed());",
     "        let learned = leader_commit.min(self.last_round);"),

    ("M7", "the follower will truncate a COMMITTED round",
     "            if r <= self.commit {",
     "            if false {"),

    ("M8", "the divergence detector never fires",
     "            && mine != digest",
     "            && mine != digest\n            && false"),

    ("M9", "an acknowledgement may go backwards",
     "        if matched > p.matched {\n            p.matched = matched;\n        }",
     "        p.matched = matched;"),

    ("M10", "a refusal claims progress the leader has not matched",
     "            body: Body::AppendResp { success: false, matched: 0, hint, digest: 0 },",
     "            body: Body::AppendResp { success: false, matched: self.durable, hint, digest: 0 },"),

    ("M11", "the digest drops its length prefix, so two framings of one byte stream agree",
     "    let h = fnv64_update(h, &(bytes.len() as u64).to_be_bytes());",
     "    let h = h;"),

    ("M12", "work is applied ahead of this node's own durability",
     "        let through = self.commit.min(self.durable);",
     "        let through = self.commit;"),

    ("M13", "quorum counted over the whole progress map, so learners and strangers count",
     "        for n in self.cfg.members() {",
     "        for n in &self.progress.keys().copied().collect::<Vec<_>>()[..] {"),

    ("M14", "the log-destruction detector is disarmed",
     "                assert!(\n                    last <= base,",
     "                assert!(\n                    true || last <= base,"),

    ("M15", "the leader counts itself twice: once as a peer entry and once as its own durability",
     "            if *n == self.self_id {\n                held.push(self.durable);\n                continue;\n            }",
     "            if *n == self.self_id {\n                held.push(self.durable.max(self.progress.get(n).map(|p| p.matched).unwrap_or(0)));\n                held.push(self.durable);\n                continue;\n            }"),

    ("M16", "a truncation does not retract the durability it was about",
     "            if self.durable > self.last_round {\n                self.durable = self.last_round;\n            }",
     ""),

    ("M17", "F1's unjoined observable is never called, so a joining node can never campaign",
     "        self.observe_quorum_watermark(leader_commit);",
     ""),

    ("M18", "a heartbeat that carries nothing is not answered",
     "            out.push(self.acknowledgement(from));\n        } else {",
     "        } else {"),

    ("M19", "an acknowledgement claims this node's whole durable log, not the agreed prefix",
     "        let matched = self.durable.min(self.agreed());",
     "        let matched = self.durable;"),

    ("M20", "an acknowledgement claims log length instead of durability",
     "        let matched = self.durable.min(self.agreed());",
     "        let matched = self.last_round.min(self.agreed());"),

    ("M21", "the agreed prefix is not reset when this node adopts a different leader",
     "        if adopting {",
     "        if false {"),

    ("M22", "the leader counts an acknowledgement for a round it does not hold",
     "        if matched > self.last_round {\n            return;\n        }",
     ""),

    ("M23", "a stale Persisted from before a truncation vouches for what replaced it",
     "        if term >= rewritten_in {",
     "        if true {"),

    ("M24", "a replayed refusal demands a snapshot from a healthy peer",
     "                // snapshot here was the defect: it silenced a healthy peer permanently, because\n                // `send_append_to` then refuses to send it anything and only a successful ack —\n                // which can no longer arrive — clears the flag.\n                return;",
     "                self.progress.entry(from).or_default().needs_snapshot = true;\n                return;"),

    ("M25", "a malformed entry list reaches the log and aborts the process",
     "        let malformed = entries",
     "        let malformed = false && entries"),

    ("M26", "the digest is not chained: every round is folded from the base",
     "        let prev = self.digests.last().copied().unwrap_or(self.base_digest);",
     "        let prev = self.base_digest;"),

    ("M27", "a success answer pulls `next` backwards",
     "        p.next = p.next.max(p.matched + 1);",
     "        p.next = p.matched + 1;"),

    ("M28", "the reserved no-claim value is not substituted",
     "    if h == 0 { FNV_OFFSET } else { h }",
     "    h"),

    ("M29", "the leader stops sending after an ack, so catch-up stalls a heartbeat per batch",
     "        if !self.advance_commit(out) && advanced && self.progress[&from].matched < self.last_round {\n            self.send_append_to(from, out);\n        }",
     "        self.advance_commit(out);\n        let _ = advanced;"),

    ("M30", "F1's stale-configuration observable is never called",
     "            self.observe_config_at(at);",
     ""),
]


def run_tests():
    p = subprocess.run(
        ["cargo", "test", "--lib", "consensus::"],
        env=ENV, capture_output=True, text=True, timeout=900,
    )
    out = p.stdout + p.stderr
    if "error[" in out or "error: could not compile" in out:
        return "BUILD-FAILED", []
    failed = re.findall(r"^\s{4}(consensus::\S+)$", out, re.M)
    m = re.search(r"test result: (\w+)\. (\d+) passed; (\d+) failed", out)
    if not m:
        return "NO-RESULT-LINE", []
    return f"{m.group(1)} ({m.group(2)} passed, {m.group(3)} failed)", sorted(set(failed))


def main():
    original = open(SRC).read()
    lines = []

    status, failed = run_tests()
    lines.append(f"BASELINE: {status}")
    if not status.startswith("ok"):
        lines.append("REFUSING: the baseline is not green, so nothing below would mean anything.")
        open(LOG, "w").write("\n".join(lines) + "\n")
        print("\n".join(lines))
        return 1

    killed = 0
    for mid, rule, old, new in MUTANTS:
        if original.count(old) != 1:
            lines.append(f"\n{mid}  {rule}\n  REFUSING: the target text appears "
                         f"{original.count(old)} times, not once. A mutant that did not apply is "
                         f"indistinguishable from one that was killed.")
            continue
        open(SRC, "w").write(original.replace(old, new, 1))
        status, failed = run_tests()
        open(SRC, "w").write(original)
        verdict = "KILLED" if failed else ("BUILD-FAILED" if "BUILD" in status else "SURVIVED")
        if failed:
            killed += 1
        lines.append(f"\n{mid}  {rule}\n  {verdict}: {status}")
        for f in failed:
            lines.append(f"    - {f.split('::')[-1]}")
        # Written after EVERY mutant, not once at the end: a timeout kill would otherwise throw
        # away the whole battery, which has happened once already on this machine.
        open(LOG, "w").write("\n".join(lines) + "\n")

    lines.append(f"\nTOTAL: {killed}/{len(MUTANTS)} mutants killed by a named test.")
    open(SRC, "w").write(original)
    status, _ = run_tests()
    lines.append(f"RESTORED: {status}")
    open(LOG, "w").write("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0 if killed == len(MUTANTS) else 2


if __name__ == "__main__":
    # The file is restored on EVERY exit path, including SIGTERM and a timeout kill. A mutant left
    # applied in the tree is a defect committed by accident, which is worse than any measurement
    # this script produces is worth.
    import signal

    ORIGINAL = open(SRC).read()

    def restore(*_a):
        open(SRC, "w").write(ORIGINAL)
        sys.exit(143)

    signal.signal(signal.SIGTERM, restore)
    signal.signal(signal.SIGINT, restore)
    try:
        code = main()
    except BaseException:
        open(SRC, "w").write(ORIGINAL)
        raise
    open(SRC, "w").write(ORIGINAL)
    sys.exit(code)
