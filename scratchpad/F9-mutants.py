#!/usr/bin/env python3
"""F9 mutant driver. Applies one deliberate defect at a time, runs the test named against the
rule it breaks, and records whether that test died. A rule whose mutant survives is a rule the
suite does not actually check."""
import subprocess, sys, os

SRC = 'src/agent_sql/cluster.rs'

M = [
 ("M1  a WAL batch does not move the base",
  [("        Command::WalBatch { .. } => true,", "        Command::WalBatch { .. } => false,")],
  "a_merge_racing_a_committed_write_to_its_base_is_re_evaluated_not_applied"),

 ("M2  the base-moved comparison is >= instead of >",
  [("        if self.last_base_move > base_round {", "        if self.last_base_move >= base_round {")],
  "a_merge_whose_base_round_is_exactly_the_last_base_move_applies"),

 ("M3  a re-evaluated merge moves the base too",
  [("                self.remember(round, verdict.clone());",
    "                self.last_base_move = round;\n                self.remember(round, verdict.clone());")],
  "a_re_evaluated_merge_does_not_itself_move_the_base"),

 ("M4  the cluster branch id drops the owning node",
  [("        Ok(ClusterBranchId(((node.0 as u64) << LOCAL_BITS) | local.id))",
    "        Ok(ClusterBranchId(local.id))")],
  "two_nodes_forking_at_the_same_local_id_get_different_cluster_ids"),

 ("M5  an oversized local id is packed rather than refused",
  [("        if local.id > LOCAL_MASK {", "        if false && local.id > LOCAL_MASK {")],
  "a_local_branch_id_too_large_for_the_namespace_is_refused_not_truncated"),

 ("M6  a fork waits for its own commit",
  [("""            Ok(fork_round) => {
                lock(&self.cost).proposals += 1;""",
    """            Ok(fork_round) => {
                lock(&self.cost).proposals += 1;
                self.pump_until("MUTANT", |l| l.last_applied() >= fork_round)?;""")],
  "a_fork_and_a_hundred_writes_cost_no_quorum_round_trip_and_the_merge_costs_one"),

 ("M7  the ledger has no idempotence guard",
  [("        if entry.round <= self.last_applied {", "        if false {")],
  "a_re_delivered_round_is_a_no_op"),

 ("M8  a colliding fork overwrites the branch already there",
  [("                if let Some(existing) = self.branches.get(&id.0) {",
    "                if let Some(existing) = self.branches.get(&u64::MAX) {")],
  "a_fork_whose_child_id_is_already_taken_is_refused_not_overwritten"),

 ("M9  the cluster merge does not check quarantine",
  [("        if self.runtime.branches().get(branch)?.state == BranchState::Quarantined {",
    "        if false && self.runtime.branches().get(branch)?.state == BranchState::Quarantined {")],
  "a_merge_the_gate_declines_costs_no_consensus_at_all"),

 ("M10 a fork does not check leadership before creating the branch",
  [("""    ) -> Result<ClusterSession, FerroError> {
        self.require_leader()?;
        let parent_cid""",
    """    ) -> Result<ClusterSession, FerroError> {
        let parent_cid""")],
  "a_fork_on_a_node_that_does_not_lead_is_refused_and_creates_nothing"),

 ("M11 leadership is not re-checked while waiting on a round",
  [("""            match self.repl.leader() {
                Some(n) if n == self.node => {}
                other => return Err(not_leader(other, &self.client_addresses)),
            }
            self.repl.pump()?;""",
    "            self.repl.pump()?;")],
  "a_merge_that_loses_the_leadership_while_waiting_refuses_rather_than_blocking"),

 ("M12 a LeaseTick moves the base",
  [("        Command::LeaseTick { .. } => false,", "        Command::LeaseTick { .. } => true,")],
  "heartbeat_rounds_do_not_re_evaluate_a_merge"),

 ("M13 a re-evaluated merge is published anyway",
  [("                Some(MergeVerdict::Applied { .. }) => {",
    "                Some(MergeVerdict::Applied { .. }) | Some(MergeVerdict::ReEvaluate { .. }) => {")],
  "a_merge_racing_a_committed_change_to_its_base_is_re_evaluated_rather_than_silently_applied"),

 ("M14 a merge of a branch no fork created is applied",
  [("""            None => {
                return MergeVerdict::Refused {
                    branch: id,
                    why: format!(
                        "round {round} merges {id}, whose fork no committed round created. A \\
                         merge of a branch the cluster never agreed exists is refused, not applied"
                    ),
                }
            }""", "            None => {}")],
  "a_merge_for_a_branch_whose_fork_never_committed_is_refused"),

 ("M15 a second reap of one branch is allowed",
  [("                        ReplicatedState::Reaped { generation: g, .. } => BranchEffect::Rejected {",
    "                        ReplicatedState::Reaped { generation: g, .. } if false => BranchEffect::Rejected {")],
  "a_reap_is_a_replicated_decision_and_is_refused_twice"),

 ("M16 the verdict depends on something that is not in the log",
  [("    rejections: VecDeque<String>,\n}", "    rejections: VecDeque<String>,\n    mutant_instance: u64,\n}"),
   ("            rejections: VecDeque::new(),\n        }",
    "            rejections: VecDeque::new(),\n            mutant_instance: {\n                static S: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);\n                S.fetch_add(1, std::sync::atomic::Ordering::SeqCst)\n            },\n        }"),
   ("        if self.last_base_move > base_round {",
    "        if self.last_base_move > base_round || self.mutant_instance % 2 == 1 {")],
  "every_node_reaches_the_same_verdict_for_every_merge_command"),

 ("M18 a merge whose fork the cluster refused just times out",
  [("""        if let Err(timeout) = self.pump_until(&format!("the fork of {cid}"), |l| l.get(cid).is_some())
        {
            let refusal = lock(&self.ledger)
                .rejections()
                .into_iter()
                .rev()
                .find(|r| r.contains(&cid.to_string()));
            return Err(match refusal {
                Some(w) => FerroError::Merge(format!(
                    "{cid} cannot be merged: the cluster refused its fork \u2014 {w}"
                )),
                None => timeout,
            });
        }""",
    """        self.pump_until(&format!("the fork of {cid}"), |l| l.get(cid).is_some())?;""")],
  "a_merge_whose_fork_the_cluster_refused_says_so_rather_than_timing_out"),

 ("M17 the re-evaluation bound is 100x looser",
  [("                    if reevaluations > self.max_reevaluations {",
    "                    if reevaluations > self.max_reevaluations * 100 {")],
  "a_merge_whose_base_never_stops_moving_is_refused_with_the_round_that_moved"),
 ("M19 the orphan sweep is not filtered by owner",
  [("""        let orphans = lock(&self.ledger).orphans_of(dead);""",
    """        let orphans: Vec<ClusterBranchId> =
            lock(&self.ledger).all().filter(|b| b.state.is_live()).map(|b| b.id).collect();""")],
  "a_promoted_leader_disposes_of_a_dead_nodes_branches_by_a_replicated_decision"),

 ("M20 a node may declare itself dead and sweep its own branches",
  [("        if dead == self.node {", "        if false && dead == self.node {")],
  "a_node_cannot_sweep_its_own_branches_as_orphans"),

 ("M21 a reap is decided locally instead of through the log",
  [("""        let round =
            self.repl.propose(Command::Branch { op: BranchOp::Reap { branch: id.0, generation } })?;
        lock(&self.cost).proposals += 1;
        Ok(round)""",
    """        let _ = (id, generation);
        Ok(self.repl.committed_head())""")],
  "a_reap_goes_through_the_log_and_carries_the_generation"),

 ("M22 the verdict depends on something that is not in the log (liveness half)",
  [("    rejections: VecDeque<String>,\n}", "    rejections: VecDeque<String>,\n    mutant_instance: u64,\n}"),
   ("            rejections: VecDeque::new(),\n        }",
    "            rejections: VecDeque::new(),\n            mutant_instance: {\n                static S: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);\n                S.fetch_add(1, std::sync::atomic::Ordering::SeqCst)\n            },\n        }"),
   ("        if self.last_base_move > base_round {",
    "        if self.last_base_move > base_round || self.mutant_instance % 2 == 1 {")],
  "no_two_nodes_disagree_about_whether_a_branch_is_live"),

 ("M23 the branch applier swallows the rounds instead of chaining them on",
  [("""        match self.next.as_mut() {
            Some(n) => n.apply(entry),
            None => Ok(()),
        }""", "        Ok(())")],
  "a_followers_committed_log_holds_the_merge_and_not_one_agent_row"),

 ("M24 the trunk is packed like any other branch instead of being id 0 everywhere",
  [("        if local.id == 0 {", "        if false && local.id == 0 {")],
  "the_trunk_is_the_same_branch_on_every_node_and_has_no_owner"),

 ("M25 node 0 may own a branch",
  [("        if node.0 == 0 {", "        if false && node.0 == 0 {")],
  "node_zero_cannot_own_a_branch"),

 ("M26 a merge that applied does not move the base",
  [("                    self.last_base_move = round;\n", "\n")],
  "a_merge_that_applied_moves_the_base_for_the_merge_behind_it"),

 ("M27 an unknown fork parent is treated as the trunk instead of refused",
  [("                match self.branches.get(&parent_id.0) {",
    "                match self.branches.get(&0u64) {")],
  "a_fork_off_a_parent_the_cluster_does_not_hold_is_refused"),

 ("M28 the orphan list is not filtered by the node that died",
  [("""    pub fn orphans_of(&self, dead: NodeId) -> Vec<ClusterBranchId> {
        self.live_owned_by(dead)
    }""",
    """    pub fn orphans_of(&self, dead: NodeId) -> Vec<ClusterBranchId> {
        let _ = dead;
        self.branches
            .values()
            .filter(|b| b.state.is_live() && b.id != ClusterBranchId::TRUNK)
            .map(|b| b.id)
            .collect()
    }""")],
  "a_branch_whose_owner_died_is_named_as_lost_work_and_its_merged_sibling_is_not"),

]

def run(test):
    r = subprocess.run(
        ["cargo","test","--test","integration_cluster_agents",test,"--","--exact","--test-threads=1"],
        capture_output=True, text=True, timeout=900)
    for line in r.stdout.splitlines():
        if line.startswith("test result:"):
            return line.strip()
    if "error[" in r.stderr or "error:" in r.stderr:
        return "DID NOT COMPILE: " + next((l for l in r.stderr.splitlines() if l.startswith("error")), "?")
    return "NO RESULT LINE"

ONLY = list(sys.argv[1:])
if ONLY:
    M = [m for m in M if m[0].split()[0] in ONLY]
    assert M, f"no mutant matched {ONLY}"

base = open(SRC).read()
out = []
# Anti-vacuity: every named test must pass on the clean tree first, or a "kill" proves nothing.
print("== clean tree ==")
for name, _, test in M:
    line = run(test)
    print(f"  {test}: {line}")
    out.append(("clean", name, test, line))
    assert line.startswith("test result: ok. 1 passed"), f"{test} does not pass clean: {line}"

print("== mutants ==")
for name, subs, test in M:
    s = base
    for old, new in subs:
        assert old in s, f"{name}: substitution target not found:\n{old[:120]}"
        s = s.replace(old, new, 1)
    open(SRC,'w').write(s)
    try:
        line = run(test)
    finally:
        open(SRC,'w').write(base)
    killed = "FAILED" in line or "DID NOT COMPILE" in line
    print(f"  {name}\n      {test}\n      -> {line}  [{'KILLED' if killed else 'SURVIVED'}]")
    out.append(("mutant", name, test, line))

open(SRC,'w').write(base)
print("== restored ==")
mode = 'a' if ONLY else 'w'
with open('scratchpad/F9-mutants.md', mode) as f:
    if mode == 'w':
        f.write("# F9 mutants\n\nEach rule, the mutant that breaks it, and what the test named against it printed.\n")
        f.write("Driver: `scratchpad/F9-mutants.py`. Every test was first shown to pass on the clean tree,\n")
        f.write("or a kill would prove nothing.\n\n")
    else:
        f.write("\n## Later run: " + ", ".join(ONLY) + "\n\n")
    for kind, name, test, line in out:
        f.write(f"- **{kind}** `{test}` — {name}\n  - `{line}`\n")
