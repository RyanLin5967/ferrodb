#!/usr/bin/env python3
"""F1 mutant driver.

For each rule implemented in `src/consensus/election.rs`, break that rule on purpose and require
the test named against it to FAIL. A mutant whose anchor does not match is reported as
NOT-APPLIED and counted as a failure of this driver, never as a surviving mutant: a substitution
that silently did nothing looks exactly like a killed mutant from the outside.
"""
import subprocess, sys, os, json, shutil

SRC = "src/consensus/election.rs"
BAK = "scratchpad/election.rs.orig"
OUT = "scratchpad/mutants.json"
LOG = "scratchpad/mutants.log"
ENV = dict(os.environ, PATH=os.path.expanduser("~/.cargo/bin") + ":" + os.environ["PATH"])

# (id, description, [(anchor, replacement, expected_count)], [tests that must now fail])
MUTANTS = [
 ("M1a", "election restriction compares (round, term) instead of (term, round)",
  [("(cand_term, cand_round) >= (self.last_term, self.last_round)",
    "(cand_round, cand_term) >= (self.last_round, self.last_term)", 1)],
  ["the_election_restriction_compares_last_term_before_last_round"]),

 ("M1b", "election restriction compares rounds only, ignoring the term",
  [("(cand_term, cand_round) >= (self.last_term, self.last_round)",
    "cand_round >= self.last_round", 1)],
  ["the_election_restriction_compares_last_term_before_last_round",
   "the_election_restriction_is_applied_to_pre_votes_too"]),

 ("M2", "a pre-campaign raises the term",
  [("        self.role = Role::PreCandidate;\n        self.leader = None;",
    "        self.hard.term = self.hard.term.saturating_add(1);\n        self.role = Role::PreCandidate;\n        self.leader = None;", 1)],
  ["a_pre_campaign_does_not_raise_this_nodes_term_and_writes_nothing_durable"]),

 ("M3a", "answering a pre-vote consumes the real vote and writes hard state",
  [("        // The answer carries the term it is *about*, not ours.",
    "        if granted {\n            self.hard.voted_for = Some(from);\n            out.push(Action::PersistHardState { term: self.hard.term, voted_for: Some(from) });\n        }\n        // The answer carries the term it is *about*, not ours.", 1)],
  ["answering_a_pre_vote_neither_raises_the_voters_term_nor_writes_hard_state"]),

 ("M3b", "answering a pre-vote adopts the hypothetical term",
  [("        // The answer carries the term it is *about*, not ours.",
    "        if granted {\n            self.hard.term = asked_term;\n        }\n        // The answer carries the term it is *about*, not ours.", 1)],
  ["answering_a_pre_vote_neither_raises_the_voters_term_nor_writes_hard_state"]),

 ("M4", "votes counted against the live configuration instead of the captured campaign",
  [("        if !granted {\n            return;\n        }\n        self.votes.insert(from);\n        if self.campaign.as_ref().is_some_and(|c| c.has_quorum(self.votes.len())) {\n            self.become_leader(out);\n        }",
    "        if !granted {\n            return;\n        }\n        self.votes.insert(from);\n        if self.cfg.has_quorum(self.votes.len()) {\n            self.become_leader(out);\n        }", 1)],
  ["a_membership_change_mid_campaign_does_not_move_the_denominator"]),

 ("M5", "a vote from outside the campaign configuration is counted",
  [("        if !self.campaign.as_ref().is_some_and(|c| c.contains(from)) {\n            return;\n        }\n", "", 2)],
  ["a_vote_from_a_node_outside_the_campaign_configuration_is_not_counted"]),

 ("M6a", "the granted vote is sent before it is made durable",
  [("""            out.push(Action::PersistHardState {
                term: self.hard.term,
                voted_for: Some(from),
            });
        }

        out.push(Action::Send(Message {
            from: self.self_id,
            to: from,
            term: self.hard.term,
            body: Body::RequestVoteResp { granted },
        }));""",
    """        }

        out.push(Action::Send(Message {
            from: self.self_id,
            to: from,
            term: self.hard.term,
            body: Body::RequestVoteResp { granted },
        }));
        if granted {
            out.push(Action::PersistHardState {
                term: self.hard.term,
                voted_for: Some(from),
            });
        }""", 1)],
  ["a_granted_vote_is_made_durable_before_it_is_sent", "a_node_does_not_vote_twice_in_one_term"]),

 ("M6b", "a candidate asks for votes before its own vote is durable",
  [("""        out.push(Action::PersistHardState {
            term: self.hard.term,
            voted_for: Some(self.self_id),
        });
        out.push(Action::RoleChanged {
            role: Role::Candidate,""",
    """        out.push(Action::RoleChanged {
            role: Role::Candidate,""", 1),
   ("""                body: Body::RequestVote { last_term, last_round },
            }));
        }
    }""",
    """                body: Body::RequestVote { last_term, last_round },
            }));
        }
        out.push(Action::PersistHardState {
            term: self.hard.term,
            voted_for: Some(self.self_id),
        });
    }""", 1)],
  ["the_campaign_configuration_is_captured_in_the_same_step_that_raises_the_term"]),

 ("M7", "a node may vote twice in one term",
  [("""        let unpledged = match self.hard.voted_for {
            None => true,
            // Re-answering the same candidate is not a second vote; it is the same vote, and a
            // transport that duplicates or retries must not turn it into a refusal.
            Some(v) => v == from,
        };""", "        let unpledged = true;", 1)],
  ["a_node_does_not_vote_twice_in_one_term",
   "one_term_never_elects_two_leaders_even_when_every_node_campaigns"]),

 ("M8", "a pre-vote answer about any term is counted",
  [("        if asked_term != self.hard.term.saturating_add(1) {\n            return;\n        }\n", "", 1)],
  ["a_pre_vote_answer_about_another_term_is_not_counted"]),

 ("M9", "a node with a live leader still grants pre-votes",
  [("            && !self.leader_is_live()\n            // (3) The election restriction.",
    "            // (3) The election restriction.", 1)],
  ["a_pre_vote_is_refused_by_a_node_that_is_still_being_served_by_a_leader",
   "a_node_campaigning_into_a_wall_never_raises_the_clusters_term"]),

 ("M10", "a node with a live leader still grants a real vote at the current term",
  [("            && !self.leader_is_live()\n            && self.log_is_at_least_as_complete(last_term, last_round);",
    "            && self.log_is_at_least_as_complete(last_term, last_round);", 1)],
  ["a_vote_in_the_current_term_is_refused_by_a_node_that_still_has_a_live_leader"]),

 ("M11", "`behind` / `unjoined` / not-a-voter no longer block a campaign",
  [("        if self.may_campaign() {", "        if true {", 1)],
  ["a_node_that_knows_its_configuration_is_stale_does_not_campaign",
   "an_unjoined_node_is_not_cleared_by_any_number_of_ticks",
   "a_node_absent_from_its_own_configuration_never_campaigns"]),

 ("M12", "`unjoined` cleared by a timer",
  [("        self.since_heard = self.since_heard.saturating_add(1);",
    "        self.since_heard = self.since_heard.saturating_add(1);\n        if self.unjoined && self.since_heard >= self.election_timeout {\n            self.unjoined = false;\n        }", 1)],
  ["an_unjoined_node_is_not_cleared_by_any_number_of_ticks"]),

 ("M13", "`unjoined` requires a non-zero watermark, so an empty cluster can never elect",
  [("        if self.unjoined && self.durable >= watermark {",
    "        if self.unjoined && watermark > 0 && self.durable >= watermark {", 1)],
  ["joining_an_empty_cluster_still_lets_a_new_member_stand"]),

 ("M14", "`unjoined` cleared by any watermark at all, without comparing to this node's store",
  [("        if self.unjoined && self.durable >= watermark {", "        if self.unjoined {", 1)],
  ["an_unjoined_node_stands_only_once_it_holds_what_a_quorum_holds"]),

 ("M15", "applying a configuration clears `unjoined` too",
  [("        self.cfg = cfg;\n        self.behind = false;",
    "        self.cfg = cfg;\n        self.behind = false;\n        self.unjoined = false;", 1)],
  ["an_unjoined_node_is_not_cleared_by_any_number_of_ticks"]),

 ("M16a", "the lease window is the election timeout",
  [("        if self.since_quorum >= self.lease {", "        if self.since_quorum >= self.election_timeout {", 1)],
  ["a_leader_that_stops_hearing_from_a_majority_demotes_itself_before_any_peer_can_win"]),

 ("M16b", "the lease window is the election base",
  [("        if self.since_quorum >= self.lease {", "        if self.since_quorum >= self.election_base {", 1)],
  ["a_leader_that_stops_hearing_from_a_majority_demotes_itself_before_any_peer_can_win"]),

 ("M17", "the lease measures the MOST silent voter instead of a majority",
  [("        ages.get(self.cfg.quorum().saturating_sub(1)).copied().unwrap_or(u32::MAX)",
    "        ages.last().copied().unwrap_or(u32::MAX)", 1)],
  ["a_leader_that_keeps_hearing_from_a_majority_keeps_its_office"]),

 ("M18", "the lease is satisfied by any single answer, not a majority",
  [("        ages.get(self.cfg.quorum().saturating_sub(1)).copied().unwrap_or(u32::MAX)",
    "        ages.first().copied().unwrap_or(u32::MAX)", 1)],
  ["a_leader_that_hears_only_from_a_minority_still_demotes"]),

 ("M19", "a new leader appends no term-establishing entry",
  [("        out.push(Action::Persist { entries: vec![entry] });", "        let _ = entry;", 1)],
  ["a_new_leader_appends_a_no_op_of_its_own_term_before_it_can_commit_anything"]),

 ("M20", "a new leader records optimism as evidence (matched = next)",
  [("                Progress { next, matched: 0, silent: 0, needs_snapshot: false },",
    "                Progress { next, matched: next, silent: 0, needs_snapshot: false },", 1)],
  ["a_new_leader_starts_every_peer_with_no_evidence_and_optimistic_next"]),

 ("M21", "a new leader waits for the heartbeat interval before asserting its office",
  [("        // Assert the office immediately rather than on the next heartbeat boundary: every follower\n        // is already counting down, and up to a whole heartbeat interval of that countdown is\n        // avoidable.\n        self.broadcast_heartbeat(out);",
    "", 1)],
  ["a_new_leader_asserts_its_office_at_once_and_then_on_its_heartbeat_interval"]),

 ("M22", "the election timeout is drawn once and never redrawn",
  [("        self.since_heard = 0;\n        self.election_timeout = self.draw_timeout();\n\n        out.push(Action::RoleChanged {\n            role: Role::PreCandidate,",
    "        self.since_heard = 0;\n\n        out.push(Action::RoleChanged {\n            role: Role::PreCandidate,", 1)],
  ["every_campaign_redraws_its_timeout_inside_the_documented_window"]),

 ("M23", "a timeout campaigns for real instead of pre-voting first",
  [("        if self.may_campaign() {\n            self.start_precampaign(out);\n            return;\n        }",
    "        if self.may_campaign() {\n            self.become_candidate(out);\n            return;\n        }", 1)],
  ["a_campaign_that_wins_nothing_is_retried_from_the_pre_vote_and_not_from_a_raised_term"]),

 ("M24", "a campaign in flight is left running after this node learns it is stale",
  [("        if self.role != Role::Follower {\n            let term = self.hard.term;\n            self.become_follower(term, None, out);\n        }\n        self.since_heard = 0;",
    "        self.since_heard = 0;", 1)],
  ["a_campaign_in_flight_is_abandoned_when_this_node_learns_it_is_stale"]),

 ("M25", "a pre-candidate counts real votes it never asked for",
  [("    fn on_request_vote_resp(&mut self, from: NodeId, term: Term, granted: bool, out: &mut Vec<Action>) {\n        if self.role != Role::Candidate {\n            return;\n        }",
    "    fn on_request_vote_resp(&mut self, from: NodeId, term: Term, granted: bool, out: &mut Vec<Action>) {", 1)],
  ["a_pre_candidate_does_not_count_real_votes_it_never_asked_for"]),

 ("M26", "a node that is its own majority still waits for somebody to answer",
  [("        // A single-voter configuration is its own majority; there is nobody to ask.\n        if self.campaign.as_ref().is_some_and(|c| c.has_quorum(self.votes.len())) {\n            self.become_candidate(out);\n            return;\n        }\n",
    "", 1)],
  ["a_single_voter_configuration_elects_itself_without_asking_anybody"]),

 ("M27", "the election timeout is a constant, so nothing is spread and nothing replays distinctly",
  [("        let base = self.election_base.max(1);\n        base + (self.rng.next_u32() % base)",
    "        self.election_base.max(1)", 1)],
  ["a_campaign_replays_identically_from_a_seed_and_its_windows_are_not_all_equal",
   "every_campaign_redraws_its_timeout_inside_the_documented_window"]),

 ("M28", "a configuration report at the version already held marks a current node stale",
  [("        if at > self.cfg.at() {", "        if at >= self.cfg.at() {", 1)],
  ["an_older_configuration_report_does_not_mark_a_current_node_stale"]),
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
                n = s.count(anchor)
                if n != count:
                    applied = False
                    why = f"anchor matched {n} times, expected {count}: {anchor[:70]!r}"
                    break
                s = s.replace(anchor, repl)
            if not applied:
                results.append(dict(id=mid, desc=desc, status="NOT-APPLIED", detail=why))
                print(f"{mid}: NOT-APPLIED — {why}", flush=True)
                continue

            open(SRC, "w").write(s)
            filt = " ".join(f"consensus::election::tests_election::{t}" for t in tests)
            # one invocation per named test, so each is scored on its own
            per = {}
            compiled = True
            for t in tests:
                r = run(f"timeout 600 cargo test --lib consensus::election::tests_election::{t} -- --exact")
                blob = r.stdout + r.stderr
                if "error[" in blob or "error: could not compile" in blob:
                    compiled = False
                    per[t] = "DID-NOT-COMPILE"
                    log.write(f"\n===== {mid} {t} COMPILE FAILURE =====\n{blob[-4000:]}\n")
                    continue
                ran = "test result:" in blob
                failed = " FAILED" in blob or "test result: FAILED" in blob
                if not ran:
                    per[t] = "DID-NOT-RUN"
                else:
                    per[t] = "KILLED" if failed else "SURVIVED"
                first = ""
                for line in blob.splitlines():
                    if "panicked at" in line or line.strip().startswith("assertion"):
                        first = line.strip()
                        break
                if not first:
                    for i, line in enumerate(blob.splitlines()):
                        if "stdout ----" in line:
                            first = "\n".join(blob.splitlines()[i + 1:i + 6]).strip()
                            break
                log.write(f"\n===== {mid} :: {t} :: {per[t]} =====\n{desc}\n{blob[-3500:]}\n")
                per[t + "::msg"] = first[:600]
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
