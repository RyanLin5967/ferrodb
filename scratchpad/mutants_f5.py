#!/usr/bin/env python3
"""F5 mutant driver.

F1's `scratchpad/mutants.py` parameterised for this row: same driver, same NOT-APPLIED discipline,
a different subject file, test path and mutant list. Not a generalisation of it, because that file
is F1's evidence and rewriting it would put this row's changes inside another row's artifact; the
duplication is the driver loop only, and the loop is what is already proven.

For each rule implemented in `src/consensus/membership.rs`, break that rule on purpose and require
the test named against it to FAIL. A mutant whose anchor does not match is reported as NOT-APPLIED
and counted as a failure of this driver, never as a killed mutant: a substitution that silently did
nothing looks exactly like a killed mutant from the outside.
"""
import subprocess, sys, os, json, shutil

SRC = "src/consensus/membership.rs"
BAK = "scratchpad/membership.rs.orig"
OUT = "scratchpad/mutants_f5.json"
LOG = "scratchpad/mutants_f5.log"
TESTPATH = "consensus::membership::tests_membership::"
ENV = dict(os.environ, PATH=os.path.expanduser("~/.cargo/bin") + ":" + os.environ["PATH"])

LEADER_GATE = """        if self.role != Role::Leader {
            return Err(FerroError::NotLeader { leader: None });
        }"""

# (id, description, [(anchor, replacement, expected_count)], [tests that must now fail])
MUTANTS = [
 # ---------------------------------------------------------------- rule 1: the log, not the apply
 ("M1a", "the configuration follows apply progress, not the log (the seam records but does not install)",
  [("""        self.acked.insert(self.self_id, at);
        self.apply_config(cfg, out);
        Ok(())""",
    """        self.acked.insert(self.self_id, at);
        let _ = (cfg, out);
        Ok(())""", 1)],
  ["the_configuration_a_node_counts_against_is_the_newest_in_its_log"]),

 ("M1b", "the leader does not count against the configuration it has just appended",
  [("""        self.acked.retain(|n, _| cfg.is_known(*n));
        self.apply_config(cfg.clone(), out);
        Ok(())""",
    """        self.acked.retain(|n, _| cfg.is_known(*n));
        let _ = out;
        Ok(())""", 1)],
  ["a_change_is_refused_until_the_configuration_in_force_is_durable_on_a_majority",
   "three_grows_to_five_one_node_at_a_time_and_each_change_waits_for_the_last"]),

 ("M1c", "a truncation cannot lower the configuration (the report is made monotone)",
  [("""        if cfg.at() == self.cfg.at() {""",
    """        if cfg.at() <= self.cfg.at() {
            self.acked.insert(self.self_id, self.cfg.at());
            return Ok(());
        }
        if cfg.at() == self.cfg.at() {""", 1)],
  ["a_configuration_report_may_move_down_after_a_truncation_and_a_repeat_is_idempotent"]),

 # ---------------------------------------------------------------- provenance
 ("M2", "absent provenance permits instead of refusing",
  [("""        if !self.acked.contains_key(&self.self_id) {
            return Err(FerroError::Constraint(format!(""",
    """        if !self.acked.contains_key(&self.self_id) {
            return Ok(());
        }
        #[allow(unreachable_code)]
        if !self.acked.contains_key(&self.self_id) {
            return Err(FerroError::Constraint(format!(""", 1)],
  ["a_configuration_with_no_recorded_provenance_refuses_rather_than_permits"]),

 # ---------------------------------------------------------------- rule 2: a majority holds it
 ("M3", "a change may begin without the configuration in force being on a majority",
  [("        if !self.cfg.has_quorum(holders) {", "        if false {\n            let _ = holders;", 1)],
  ["a_change_is_refused_until_the_configuration_in_force_is_durable_on_a_majority",
   "a_change_that_grows_the_voter_set_waits_for_a_majority_of_the_larger_set"]),

 ("M4", "acknowledgements are matched on the version alone, not the (version, term) pair",
  [("            .filter(|n| self.acked.get(n).is_some_and(|a| *a >= want))",
    "            .filter(|n| self.acked.get(n).is_some_and(|a| a.version >= want.version))", 1)],
  ["an_acknowledgement_of_another_terms_configuration_of_the_same_version_does_not_count"]),

 ("M5", "learners are counted toward the majority",
  [("""        self.cfg
            .members()
            .iter()
            .filter(|n| self.acked.get(n).is_some_and(|a| *a >= want))
            .count()""",
    """        self.cfg
            .members()
            .iter()
            .chain(self.cfg.learners())
            .filter(|n| self.acked.get(n).is_some_and(|a| *a >= want))
            .count()""", 1)],
  ["a_learners_acknowledgement_is_not_counted_toward_a_majority"]),

 ("M6", "a departed member's entry is left in `acked`",
  [("        self.acked.retain(|n, _| cfg.is_known(*n));\n", "", 2)],
  ["a_departed_members_acknowledgement_is_dropped_when_the_change_that_dropped_it_is_installed"]),

 ("M7a", "a peer's acknowledgement may move backwards",
  [("""        let e = self.acked.entry(node).or_default();
        if at > *e {
            *e = at;
        }""",
    """        let e = self.acked.entry(node).or_default();
        *e = at;""", 1)],
  ["a_peers_acknowledgement_never_moves_backwards_and_a_peer_cannot_state_this_nodes"]),

 ("M7b", "a peer report may state this node's own record",
  [("        if node == self.self_id || !self.cfg.is_known(node) {",
    "        if !self.cfg.is_known(node) {", 1)],
  ["a_peers_acknowledgement_never_moves_backwards_and_a_peer_cannot_state_this_nodes"]),

 ("M7c", "a node outside the configuration is recorded",
  [("        if node == self.self_id || !self.cfg.is_known(node) {",
    "        if node == self.self_id {", 1)],
  ["a_departed_members_acknowledgement_is_dropped_when_the_change_that_dropped_it_is_installed"]),

 # ---------------------------------------------------------------- rule 3: the erratum
 ("M8", "a leader may change membership before committing an entry of its own term",
  [("        if own_term == OwnTermCommitted::Yes {\n            return Ok(());\n        }",
    "        if true {\n            let _ = own_term;\n            return Ok(());\n        }", 1)],
  ["a_membership_change_is_refused_until_this_leader_has_committed_an_entry_of_its_own_term"]),

 # ---------------------------------------------------------------- only a leader
 ("M9", "a node that does not lead may change membership",
  [(LEADER_GATE, "", 4)],
  ["only_a_leader_may_begin_a_membership_change"]),

 # ---------------------------------------------------------------- a learner first
 ("M10a", "a node being added is admitted straight as a voter",
  [("                Standing::Absent => Ok(self.cfg.adding_learner(n, term)),",
    "                Standing::Absent => Ok(self.cfg.adding(n, term)),", 1)],
  ["a_node_being_added_joins_as_a_learner_and_never_straight_as_a_voter"]),

 ("M10b", "the gate permits absent -> voter in one step",
  [("""            (Standing::Absent, Standing::Voter) => Err(FerroError::Constraint(format!(
                "refused to add {n} directly as a voter: a node being added joins as a learner \\
                 first, because counting a node that holds none of the log enlarges the \\
                 denominator without enlarging the set that can answer — availability falls at the \\
                 moment an operator believes they are raising it."
            ))),""",
    "            (Standing::Absent, Standing::Voter) => Ok(()),", 1)],
  ["a_node_being_added_joins_as_a_learner_and_never_straight_as_a_voter"]),

 ("M11a", "a learner is promoted whatever it holds",
  [("            Some(m) if m >= self.commit => Ok(()),", "            Some(_) => Ok(()),", 1)],
  ["a_learner_is_promoted_only_once_it_holds_the_leaders_committed_round"]),

 ("M11b", "a promotion is judged satisfied when there is no replication progress at all",
  [("        match self.progress.get(&n).map(|p| p.matched) {",
    "        match self.progress.get(&n).map(|p| p.matched).or(Some(Round::MAX)) {", 1)],
  ["a_promotion_is_refused_for_a_node_with_no_replication_progress_at_all"]),

 ("M11c", "a promotion requires a non-zero commit, so a new cluster can never grow",
  [("            Some(m) if m >= self.commit => Ok(()),",
    "            Some(m) if self.commit > 0 && m >= self.commit => Ok(()),", 1)],
  ["a_learner_is_promoted_only_once_it_holds_the_leaders_committed_round"]),

 # ---------------------------------------------------------------- a demotion before a removal
 ("M12a", "a voter may be removed in one step from the planner",
  [("""                Standing::Voter => Err(FerroError::Constraint(format!(
                    "refused to remove voter {n}: demote it to learner first, so that it receives \\
                     the configuration that stops it voting. A voter dropped outright is never \\
                     told: it keeps a configuration containing itself and campaigns at a cluster it \\
                     has left for ever."
                ))),""",
    "                Standing::Voter => Ok(self.cfg.removing(n, term)),", 1)],
  ["a_voter_is_demoted_before_it_can_be_removed"]),

 ("M12b", "the gate permits voter -> absent in one step",
  [("""            (Standing::Voter, Standing::Absent) => Err(FerroError::Constraint(format!(
                "refused to remove voter {n} in one step: a voter is demoted to learner first, so \\
                 that it receives the configuration that stops it voting. Dropped outright it keeps \\
                 a configuration containing itself, the leader drops it from `progress` in the same \\
                 step so no further Append can reach it, and it campaigns at a cluster it has left \\
                 for ever."
            ))),""",
    "            (Standing::Voter, Standing::Absent) => Ok(()),", 1)],
  ["a_voter_is_demoted_before_it_can_be_removed"]),

 ("M13", "a leader may demote itself",
  [("        if n != self.self_id {\n            return Ok(());\n        }",
    "        if true {\n            return Ok(());\n        }", 1)],
  ["a_leader_may_not_demote_itself"]),

 # ---------------------------------------------------------------- the shape of one change
 ("M14a", "a change may move two nodes",
  [("        if moved.len() != 1 {", "        if moved.len() > 2 {", 1)],
  ["a_change_moves_exactly_one_node"]),

 ("M14b", "a change that moves nobody is accepted as a harmless no-op",
  [("        if moved.len() != 1 {", "        if moved.len() > 1 {", 1)],
  ["a_change_moves_exactly_one_node"]),

 ("M15a", "a change need not be one version from the set in force",
  [("        if cfg.version != self.cfg.version + 1 {", "        if false {", 1)],
  ["a_change_is_refused_unless_it_is_one_version_and_this_term"]),

 ("M15b", "a change may carry a dead leader's term",
  [("        if cfg.term != self.hard.term {", "        if false {", 1)],
  ["a_change_is_refused_unless_it_is_one_version_and_this_term"]),

 ("M16", "an empty voter set may be proposed",
  [("""        if cfg.is_empty() {
            return Err(FerroError::Constraint(format!(""",
    """        if false {
            return Err(FerroError::Constraint(format!(""", 1)],
  ["a_change_may_not_leave_a_cluster_with_no_voters"]),

 # ---------------------------------------------------------------- damage
 ("M17a", "an empty voter set arriving from the log is installed",
  [("""        if cfg.is_empty() {
            return Err(self.latch_damaged(""",
    """        if false {
            return Err(self.latch_damaged(""", 1)],
  ["a_configuration_that_is_damage_latches_this_node_out_of_office"]),

 ("M17b", "two different configurations may claim one (version, term)",
  [("            if cfg != self.cfg {", "            if false {", 1)],
  ["a_configuration_that_is_damage_latches_this_node_out_of_office"]),

 ("M17c", "damage is reported but does not take the node out of office",
  [("""        let term = self.hard.term;
        self.become_follower(term, None, out);
        self.behind = true;""", "        let _ = out;", 1)],
  ["a_configuration_that_is_damage_latches_this_node_out_of_office"]),

 # ---------------------------------------------------------------- F1's flags
 ("M18", "installing a configuration clears `unjoined` as well as `behind`",
  [("        self.apply_config(cfg, out);\n        Ok(())",
    "        self.apply_config(cfg, out);\n        self.unjoined = false;\n        Ok(())", 1)],
  ["installing_a_configuration_clears_behind_and_leaves_unjoined_alone",
   "three_grows_to_five_one_node_at_a_time_and_each_change_waits_for_the_last"]),
]


def run(cmd, timeout=900):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, env=ENV, timeout=timeout)


def main():
    shutil.copy(SRC, BAK)
    orig = open(BAK).read()
    results = []
    log = open(LOG, "w")
    try:
        for mid, desc, edits, tests in MUTANTS:
            s = orig
            applied = True
            why = ""
            for anchor, repl, count in edits:
                nmatch = s.count(anchor)
                if nmatch != count:
                    applied = False
                    why = f"anchor matched {nmatch} times, expected {count}: {anchor[:70]!r}"
                    break
                s = s.replace(anchor, repl)
            if not applied:
                results.append(dict(id=mid, desc=desc, status="NOT-APPLIED", detail=why))
                print(f"{mid}: NOT-APPLIED — {why}", flush=True)
                continue

            open(SRC, "w").write(s)
            per = {}
            compiled = True
            for t in tests:
                r = run(f"timeout 600 cargo test --lib {TESTPATH}{t} -- --exact")
                blob = r.stdout + r.stderr
                if "error[" in blob or "error: could not compile" in blob:
                    compiled = False
                    per[t] = "DID-NOT-COMPILE"
                    log.write(f"\n===== {mid} {t} COMPILE FAILURE =====\n{blob[-4000:]}\n")
                    continue
                ran = "test result:" in blob
                failed = " FAILED" in blob or "test result: FAILED" in blob
                per[t] = ("KILLED" if failed else "SURVIVED") if ran else "DID-NOT-RUN"
                first = ""
                for line in blob.splitlines():
                    if line.strip().startswith("assertion") or "panicked at" in line:
                        first = line.strip()
                        break
                if not first:
                    for i, line in enumerate(blob.splitlines()):
                        if "stdout ----" in line:
                            first = "\n".join(blob.splitlines()[i + 1:i + 8]).strip()
                            break
                log.write(f"\n===== {mid} :: {t} :: {per[t]} =====\n{desc}\n{blob[-3500:]}\n")
                per[t + "::msg"] = first[:700]
            status = "KILLED" if all(v == "KILLED" for k, v in per.items() if not k.endswith("::msg")) else "SURVIVED"
            if not compiled:
                status = "DID-NOT-COMPILE"
            results.append(dict(id=mid, desc=desc, status=status, per=per))
            print(f"{mid}: {status} — {desc}", flush=True)
            open(SRC, "w").write(orig)
    finally:
        open(SRC, "w").write(orig)
        log.close()
    json.dump(results, open(OUT, "w"), indent=1)
    bad = [r for r in results if r["status"] != "KILLED"]
    print(f"\n{len(results)} mutants, {len(results)-len(bad)} killed, {len(bad)} not killed")
    for r in bad:
        print("  NOT KILLED:", r["id"], r["status"], r["desc"])
    sys.exit(1 if bad else 0)


main()
