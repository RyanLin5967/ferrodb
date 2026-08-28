#!/usr/bin/env python3
"""F6's mutant battery.

Each entry breaks ONE rule on purpose and names the test that must die of it. A test that has not
been seen to fail is not evidence; a rule with no mutant is a rule nobody has shown matters.

Every mutation is an exact, unique string replacement, so a mutation that no longer applies is
reported as SKIPPED rather than silently doing nothing. The tree is restored from git after every
mutant and verified clean at the end.
"""
import subprocess, sys, pathlib, os

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT = ROOT / "scratchpad" / "F6-mutants.txt"
ENV = dict(os.environ, PATH=os.path.expanduser("~/.cargo/bin") + ":" + os.environ.get("PATH", ""))

SNAP = "src/consensus/snapshot.rs"
REPL = "src/consensus/replicate.rs"
NODE = "src/consensus/node.rs"

# (id, file, old, new, test-name, target)  target: "lib" or an integration test target name
M = [
    # ---- what a receiver can decide from the envelope alone -----------------------------------
    ("M01", SNAP, "        if self.config.is_empty() {",
     "        if false {",
     "a_snapshot_with_no_configuration_is_refused_because_its_receiver_could_not_count_a_majority", "lib"),
    ("M02", SNAP, "        if self.total_bytes > MAX_SNAPSHOT_BYTES {",
     "        if false {",
     "a_snapshot_claiming_an_implausible_size_is_refused_before_anything_is_allocated_for_it", "lib"),
    ("M03", SNAP, "        if self.last_round == 0 {\n            // Round 0 means",
     "        if false {\n            // Round 0 means",
     "a_snapshot_at_round_zero_is_refused", "lib"),

    # ---- the payload format --------------------------------------------------------------------
    ("M04", SNAP, "        if bytes[0..8] != MAGIC {", "        if false {",
     "a_payload_header_that_is_not_one_is_refused_rather_than_guessed_at", "lib"),
    ("M05", SNAP, "        if version != FORMAT_VERSION {", "        if false {",
     "a_payload_header_that_is_not_one_is_refused_rather_than_guessed_at", "lib"),
    ("M06", SNAP, "        if claimed != actual {", "        if false {",
     "a_payload_header_that_is_not_one_is_refused_rather_than_guessed_at", "lib"),
    ("M07", SNAP, "        if self.image_len != page_bytes {", "        if false {",
     "a_header_whose_page_count_contradicts_its_image_length_is_refused", "lib"),
    ("M08", SNAP, "        if self.page_count == 0 {", "        if false {",
     "a_zero_page_image_is_refused_because_installing_it_would_look_like_success", "lib"),

    # ---- the sender ----------------------------------------------------------------------------
    ("M09", SNAP, "        if m.last_round > self.applied {", "        if false {",
     "a_leader_refuses_to_serve_a_snapshot_above_what_its_engine_has_applied", "lib"),
    ("M10", SNAP, "            Some(t) if t == m.last_term => {}", "            Some(_) => {}",
     "a_leader_refuses_a_snapshot_whose_term_disagrees_with_its_own_log", "lib"),
    ("M11", SNAP, "        if snap.header.base_digest != self.digest_at(m.last_round).unwrap_or(0) {",
     "        if false {",
     "a_leader_refuses_a_snapshot_whose_base_digest_is_not_its_own", "lib"),
    ("M12", SNAP, "                p.sending = None;\n                p.needs_snapshot = false;\n                p.next = cur.round() + 1;",
     "                p.sending = None;\n                p.needs_snapshot = false;\n                p.next = cur.round() + 1;\n                p.matched = cur.round();",
     "a_completed_transfer_moves_next_and_never_matched", "lib"),
    ("M13", SNAP, "        self.progress.entry(from).or_default().silent = 0;\n\n        let Some(cur)",
     "        let Some(cur)",
     "a_transfer_ack_resets_the_peers_silence_so_a_slow_install_does_not_cost_the_leader_its_lease", "lib"),
    ("M14", SNAP, "        if !self.cfg.is_known(from) {\n            // A removed node keeps running",
     "        if false {\n            // A removed node keeps running",
     "an_answer_from_a_removed_node_does_not_resurrect_its_progress", "lib"),
    ("M15", SNAP, "        if received_through == cur.acked {", "        if false {",
     "a_receiver_that_lost_a_transfer_rewinds_it_rather_than_deadlocking_it", "lib"),
    ("M16", SNAP, "        let done = received_through == cur.total();\n        {",
     "        if received_through <= cur.acked { return; }\n        let done = received_through == cur.total();\n        {",
     "a_receiver_that_lost_a_transfer_rewinds_it_rather_than_deadlocking_it", "lib"),

    # ---- the receiver --------------------------------------------------------------------------
    ("M17", SNAP, "        if offset != cur.received {", "        if false {",
     "a_chunk_at_the_wrong_offset_is_answered_with_the_resume_point_and_never_buffered", "lib"),
    ("M18", SNAP, "        if cur.received + data.len() as u64 > meta.total_bytes {", "        if false {",
     "a_chunk_that_would_run_past_the_declared_length_is_refused", "lib"),
    ("M19", SNAP, "            if cur.body_digest != cur.header.body_digest {", "            if false {",
     "a_payload_whose_body_digest_does_not_match_is_refused_before_the_driver_is_told", "lib"),
    ("M20", SNAP, "            if header.last_round != meta.last_round\n                || header.last_term != meta.last_term\n                || header.total_bytes() != meta.total_bytes\n            {",
     "            if false {",
     "an_envelope_and_a_payload_that_disagree_are_refused", "lib"),
    ("M21", SNAP, "        if meta.last_round <= self.snapshot_round {", "        if false {",
     "a_snapshot_older_than_this_nodes_floor_is_acknowledged_whole_and_not_installed", "lib"),
    ("M22", SNAP, "        if self.term_at(meta.last_round) == Some(meta.last_term) {", "        if false {",
     "a_snapshot_of_a_round_this_node_already_holds_is_not_installed", "lib"),
    ("M23", SNAP, "                Some(c) if c.header == header && c.received > 0 => {",
     "                Some(c) if c.meta == meta && c.received > 0 => {",
     "a_second_snapshot_supersedes_the_one_in_flight", "lib"),
    ("M24", SNAP, "            if cur.received != meta.total_bytes {",
     "            if false {",
     "a_transfer_that_claims_to_be_done_at_the_wrong_length_is_refused", "lib"),

    # ---- the install -----------------------------------------------------------------------------
    ("M25", SNAP, "            cur.complete = true;\n        }\n\n        let n = cur.received;",
     "            cur.complete = true;\n        }\n        if cur.complete {\n            self.progress.entry(self.self_id).or_default().receiving = Some(cur.clone());\n            let _ = self.finish_install(out);\n        }\n\n        let n = cur.received;",
     "nothing_moves_until_the_install_is_reported_durable", "lib"),
    ("M26", SNAP, "        self.durable = cur.meta.last_round;\n\n        // A snapshot is committed state",
     "        self.durable = cur.meta.last_round;\n        self.unjoined = false;\n\n        // A snapshot is committed state",
     "completing_an_install_sets_the_watermark_and_leaves_unjoined_to_the_next_append", "lib"),
    ("M27", SNAP, "        self.install_snapshot_tail(cur.meta.last_round, cur.header.base_digest);",
     "        self.install_snapshot_tail(cur.meta.last_round, 0);",
     "an_installed_snapshot_anchors_the_digest_chain_where_the_leader_is", "lib"),
    ("M28", SNAP, "        self.note_config_in_log(cur.meta.config.clone(), out)?;",
     "        let _ = self.note_config_in_log(cur.meta.config.clone(), out);",
     "an_install_whose_configuration_is_damage_latches_this_node_out_of_office", "lib"),
    ("M29", SNAP, "        self.applied = self.applied.max(cur.meta.last_round);",
     "        // mutant: applied left behind the floor",
     "an_install_discards_the_whole_log_and_the_floor_moves_with_it", "lib"),
    ("M30", SNAP, "        let adopting = self.leader != Some(from);\n        if self.role != Role::Follower || adopting || self.hard.term != term {\n            self.become_follower(term, Some(from), out);\n        } else {\n            self.since_heard = 0;\n        }",
     "        let adopting = self.leader != Some(from);",
     "receiving_state_adopts_the_sender_as_leader_so_a_long_transfer_is_not_an_election", "lib"),

    # ---- checkpointing this node's own log --------------------------------------------------------
    ("M31", SNAP, "        if through > self.applied {", "        if false {",
     "a_checkpoint_above_what_the_engine_has_applied_is_refused", "lib"),
    ("M32", REPL, "        self.base = base;\n        self.base_digest = base_digest;",
     "        self.base = base;\n        self.base_digest = 0;",
     "a_checkpoint_keeps_the_digests_of_the_rounds_it_keeps", "lib"),
    ("M33", REPL, "        self.commit = snapshot_round;\n        self.applied = snapshot_round;",
     "        self.commit = 0;\n        self.applied = 0;",
     "a_restart_above_a_snapshot_floor_comes_back_at_the_floor_and_not_at_zero", "lib"),
    ("M34", REPL, "            self.send_snapshot_chunk_to(peer, out);\n            return;",
     "            return;",
     "a_peer_below_the_floor_is_sent_state_and_never_entries", "lib"),

    # ---- the driver (integration) ------------------------------------------------------------------
    ("M35", NODE, "        if self.installed_round == Some(round) {", "        if false {",
     "a_follower_partitioned_past_log_retention_rejoins_by_snapshot_and_converges",
     "integration_cluster_snapshot"),
    ("M36", NODE, "        let through = self.applied.saturating_sub(keep);",
     "        let through = 0;",
     "a_follower_partitioned_past_log_retention_rejoins_by_snapshot_and_converges",
     "integration_cluster_snapshot"),
    ("M37", NODE, "        if !starting && !continuing {",
     "        if false {",
     "consensus::node::tests_node::a_chunk_the_driver_cannot_place_is_refused_by_name", "lib"),
    ("M38", NODE, "            applied: floor,", "            applied: 0,",
     "a_node_restarted_above_a_snapshot_floor_comes_back_on_it",
     "integration_cluster_snapshot"),
    ("M39", SNAP, "        self.pool.invalidate_all()?;",
     "        // mutant: the pool keeps frames of the database that was just replaced",
     "a_captured_payload_installs_back_to_the_same_pages",
     "integration_cluster_snapshot"),
]


def run(cmd, timeout=1500):
    return subprocess.run(cmd, cwd=ROOT, env=ENV, capture_output=True, text=True, timeout=timeout)


def restore(rel):
    run(["git", "checkout", "--", rel])


def clean():
    return run(["git", "diff", "--quiet"]).returncode == 0


def main():
    only = sys.argv[1:] or None
    lines = []
    killed = survived = skipped = 0
    assert clean(), "the tree is dirty before the battery started; commit first"

    for mid, rel, old, new, test, target in M:
        if only and mid not in only:
            continue
        path = ROOT / rel
        src = path.read_text()
        n = src.count(old)
        if n != 1:
            lines.append(f"{mid} SKIPPED  ({rel}: the needle matched {n} times, not 1) -> {test}")
            skipped += 1
            print(lines[-1], flush=True)
            continue
        path.write_text(src.replace(old, new, 1))
        try:
            if target == "lib":
                path = test if test.startswith("consensus::") else \
                    f"consensus::snapshot::tests_snapshot::{test}"
                cmd = ["cargo", "test", "--lib", path, "--", "--exact"]
            else:
                cmd = ["cargo", "test", "--test", target, test, "--", "--exact"]
            r = run(cmd)
            out = r.stdout + r.stderr
            if "error[E" in out or "could not compile" in out:
                verdict, extra = "SKIPPED", "did not compile"
                skipped += 1
            elif r.returncode != 0:
                verdict = "KILLED"
                killed += 1
                extra = next(
                    (l.strip() for l in out.splitlines()
                     if "panicked at" in l or "assertion" in l or "FAILED" in l),
                    "non-zero exit",
                )
            else:
                verdict = "SURVIVED"
                survived += 1
                extra = "the test passed against a broken rule"
        except subprocess.TimeoutExpired:
            verdict, extra = "KILLED", "timed out (a rule broken into a stall is still caught)"
            killed += 1
        finally:
            restore(rel)
        lines.append(f"{mid} {verdict:9s} {test}\n      {rel}: {old.strip().splitlines()[0][:100]}\n      -> {extra[:220]}")
        print(lines[-1], flush=True)

    assert clean(), "THE TREE IS DIRTY AFTER THE BATTERY — a mutant was not restored"
    summary = f"\n{killed} killed, {survived} SURVIVED, {skipped} skipped, of {killed+survived+skipped} fired"
    OUT.write_text("\n".join(lines) + summary + "\n")
    print(summary)
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
