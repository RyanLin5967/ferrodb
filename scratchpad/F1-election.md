# F1 — election

`src/consensus/election.rs` (owned, implemented) and `src/consensus/tests_election.rs` (new, 35
tests). **`src/consensus/mod.rs` is untouched** — the test module is wired in from `election.rs`
with `#[cfg(test)] #[path = "tests_election.rs"] mod tests_election;`, so no declaration had to be
added to the frozen file.

## What was built

`on_tick` and the four vote-message handlers, plus the role transitions they drive.

**`on_tick`.** A leader ages every peer's `Progress::silent`, measures its lease, and heartbeats on
its interval; everyone else counts down to a campaign. A node blocked from campaigning restarts its
countdown and abandons any campaign in flight, but **neither flag is cleared by the tick** — time is
not evidence about a configuration or about a log.

**The leader lease.** `since_quorum` is *derived* from the per-peer silences rather than accumulated
in a counter of its own: sort the voters' `silent` values (self counts as zero) and take the
quorum-th smallest, which *is* the age of the most recent majority acknowledgement. Two counters of
one fact drift invisibly; one does not. The window is `lease` (8), which is strictly below
`election_base` (10) and therefore below every timeout any peer can draw from `[10, 20)`. Measured,
not asserted: the leader demotes on tick 8 of silence.

**A leader outside its own configuration** is refused *before* the lease rather than by it — such a
node is not one of the voters whose silence the lease measures, so a check placed after the lease
could never fire, and a guard that cannot fire is not a guard. (This was the first version; it was
moved after noticing the lease answered it first.)

**Pre-vote.** `PreCandidate` raises no term, writes no hard state, and asks about `term + 1`. A voter
grants only if the hypothetical is genuinely ahead, it is **not currently being served by a leader**,
and the election restriction passes. That middle clause is the wall: without it a node that lost one
link collects pre-votes from a healthy cluster and deposes a leader that never stopped working.
Pre-vote answers carry the term they are *about*, so an answer to an earlier campaign cannot be
counted toward this one.

**The term raise is one step.** `become_candidate` raises the term, casts this node's vote for
itself, and captures `campaign = cfg.clone()` together, because they are one decision: a membership
change between the raise and the capture moves the denominator underneath a campaign already in
flight. Votes are counted against `campaign`, never `cfg`, and a vote from a node outside `campaign`
is not counted at all.

**`PersistHardState` before `Send`.** Emitted on both kinds of vote — a grant given to somebody, and
the vote a candidate casts for itself — and **re-emitted even on a duplicate request**, which costs
one fsync on a retransmission and buys an invariant that can be checked mechanically on every step
rather than only on the first. The test harness (`step_checked`) checks exactly that on every action
list every test in the file produces, including the 3000-tick three-node run.

**Taking office.** `become_leader` resets `progress` (`matched: 0` — the leader has no evidence about
any peer; `next` is optimism), appends the term-establishing `Command::NoOp` of its own term, and
heartbeats at once rather than waiting up to a full interval while every follower counts down.

## The two flags, and the three seams

`behind` and `unjoined` gate campaigning, so the *rules* live here; the *evidence* arrives in
another owner's handler. Three `pub(crate)` methods are the seam:

| method | called by | effect |
|---|---|---|
| `observe_quorum_watermark(commit)` | `replicate.rs`, on an `Append` | the **only** thing that clears `unjoined`: clears it iff `durable >= watermark` |
| `observe_config_at(CfgAt)` | `replicate.rs` / `snapshot.rs`, when a peer reports a newer configuration | the **only** thing that sets `behind` |
| `apply_config(Config)` | the caller, when a `Membership` command commits | the **only** thing that clears `behind` — and deliberately **not** `unjoined` |

`unjoined` clears on a comparison, never a timer, and never on "holds some rounds": **a cluster with
an empty log reports a watermark of zero**, which every node matches, so a member joining an empty
cluster clears at once instead of being left unable ever to stand. Both directions are tested
(`joining_an_empty_cluster_still_lets_a_new_member_stand`, and mutant M13, which requires
`watermark > 0` and is killed by it).

These three are currently `dead_code` warnings, in the same class as the pre-existing warning on
`may_campaign` in `mod.rs`: their callers are not built yet. **`replicate.rs` must call
`observe_quorum_watermark`, or no node added to a running cluster can ever campaign.**

## Tests — 35 named tests in `src/consensus/tests_election.rs`

Every step in the file goes through `step_checked`, which re-asserts the durability rule on **every**
action list any test produces: a `RequestVoteResp { granted: true }`, or a `RequestVote` ask, must be
preceded in the same list by the `PersistHardState` recording the vote it reports. A rule checked in
one test is a rule checked on one path.

| rule (DISTRIBUTED.md §F1) | tests |
|---|---|
| election restriction, `(last_term, last_round)` lexicographic | `the_election_restriction_compares_last_term_before_last_round`, `the_election_restriction_is_applied_to_pre_votes_too` |
| pre-vote must not raise the term | `a_pre_campaign_does_not_raise_this_nodes_term_and_writes_nothing_durable`, `answering_a_pre_vote_neither_raises_the_voters_term_nor_writes_hard_state`, `a_node_campaigning_into_a_wall_never_raises_the_clusters_term` |
| `campaign` captured in the step that raises the term | `the_campaign_configuration_is_captured_in_the_same_step_that_raises_the_term`, `a_membership_change_mid_campaign_does_not_move_the_denominator`, `a_vote_from_a_node_outside_the_campaign_configuration_is_not_counted`, `a_pre_vote_answer_about_another_term_is_not_counted`, `a_pre_candidate_does_not_count_real_votes_it_never_asked_for` |
| `PersistHardState` before the `Send` of any vote | `a_granted_vote_is_made_durable_before_it_is_sent`, `a_node_does_not_vote_twice_in_one_term`, `a_new_term_is_a_new_vote`, `one_term_never_elects_two_leaders_even_when_every_node_campaigns` |
| the wall a partitioned node campaigns into | `a_pre_vote_is_refused_by_a_node_that_is_still_being_served_by_a_leader`, `a_vote_in_the_current_term_is_refused_by_a_node_that_still_has_a_live_leader` |
| `behind`, cleared only by applying a configuration | `a_node_that_knows_its_configuration_is_stale_does_not_campaign`, `behind_is_cleared_by_applying_a_configuration_and_by_nothing_else`, `an_older_configuration_report_does_not_mark_a_current_node_stale`, `a_campaign_in_flight_is_abandoned_when_this_node_learns_it_is_stale` |
| `unjoined`, cleared by an observable and never a timer | `an_unjoined_node_is_not_cleared_by_any_number_of_ticks`, `an_unjoined_node_stands_only_once_it_holds_what_a_quorum_holds`, `joining_an_empty_cluster_still_lets_a_new_member_stand` |
| the leader lease | `a_leader_that_stops_hearing_from_a_majority_demotes_itself_before_any_peer_can_win`, `a_leader_that_keeps_hearing_from_a_majority_keeps_its_office`, `a_leader_that_hears_only_from_a_minority_still_demotes`, `a_leader_removed_from_its_own_configuration_steps_down` |
| taking office | `a_new_leader_appends_a_no_op_of_its_own_term_before_it_can_commit_anything`, `a_new_leader_asserts_its_office_at_once_and_then_on_its_heartbeat_interval`, `a_new_leader_starts_every_peer_with_no_evidence_and_optimistic_next`, `a_single_voter_configuration_elects_itself_without_asking_anybody`, `a_node_absent_from_its_own_configuration_never_campaigns` |
| randomized, replayable timeouts | `every_campaign_redraws_its_timeout_inside_the_documented_window`, `a_campaign_that_wins_nothing_is_retried_from_the_pre_vote_and_not_from_a_raised_term`, `a_campaign_replays_identically_from_a_seed_and_its_windows_are_not_all_equal` |

Two of these are more than unit tests. `one_term_never_elects_two_leaders_even_when_every_node_campaigns`
hand-routes three `Consensus` instances for 3000 ticks and asserts, after every delivery, that no
term has ever had two leaders; `Append` bodies are dropped (`replicate.rs` would panic in its
`unimplemented!`), which costs nothing it measures and makes the case harder — with no heartbeats
landing, the run is nothing but overlapping campaigns.
`a_node_campaigning_into_a_wall_never_raises_the_clusters_term` runs a healthy leader and follower
beside a partitioned peer for 500 ticks and asserts nobody's term moved.

Both refuse to pass vacuously: the first requires `leaders.len() > 1`, `delivered > 100` and
`dropped > 0`; the second requires `asks > 20` and `refusals > 20`.

## Mutants — 32 fired, 32 killed

Driver: `scratchpad/mutants.py` (record in `scratchpad/mutants.json`, transcripts in
`scratchpad/mutants.log`). It refuses to score a mutant whose anchor did not match exactly the
expected number of times and reports `NOT-APPLIED`, because a substitution that silently did nothing
looks exactly like a killed mutant from the outside. Each mutant is applied to `election.rs`, each
named test run with `--exact`, and the file restored.

| # | rule broken on purpose | named test that must fail | what it printed |
|---|---|---|---|
| M1a | election restriction compares (round, term) instead of (term, round) | `the_election_restriction_compares_last_term_before_last_round` | **KILLED** — a vote was granted to a candidate whose log ends in an OLDER term but at a higher round; electing it truncates the voter's round 2, which may be acknowledged |
| M1b | election restriction compares rounds only, ignoring the term | `the_election_restriction_compares_last_term_before_last_round` | **KILLED** — a vote was granted to a candidate whose log ends in an OLDER term but at a higher round; electing it truncates the voter's round 2, which may be acknowledged |
| ↳ |  | `the_election_restriction_is_applied_to_pre_votes_too` | **KILLED** — a pre-vote ignored the election restriction |
| M2 | a pre-campaign raises the term | `a_pre_campaign_does_not_raise_this_nodes_term_and_writes_nothing_durable` | **KILLED** — assertion `left == right` failed: a pre-campaign raised the campaigning node's own term left: 1 right: 0 |
| M3a | answering a pre-vote consumes the real vote and writes hard state | `answering_a_pre_vote_neither_raises_the_voters_term_nor_writes_hard_state` | **KILLED** — assertion `left == right` failed: a pre-vote consumed the voter's real vote left: Some(NodeId(1)) right: None |
| M3b | answering a pre-vote adopts the hypothetical term | `answering_a_pre_vote_neither_raises_the_voters_term_nor_writes_hard_state` | **KILLED** — assertion `left == right` failed: answering a pre-vote raised the voter's term — the disruption pre-vote exists to prevent, arriving through the mechanism meant to stop it left: 1 right: 0 |
| M4 | votes counted against the live configuration instead of the captured campaign | `a_membership_change_mid_campaign_does_not_move_the_denominator` | **KILLED** — assertion `left != right` failed: two votes elected a leader of a campaign that asked five nodes — the denominator moved underneath the campaign left: Leader right: Leader |
| M5 | a vote from outside the campaign configuration is counted | `a_vote_from_a_node_outside_the_campaign_configuration_is_not_counted` | **KILLED** — assertion `left == right` failed: a stranger's pre-vote carried the campaign left: Candidate right: PreCandidate |
| M6a | the granted vote is sent before it is made durable | `a_granted_vote_is_made_durable_before_it_is_sent` | **KILLED** — a vote reached the wire before it reached the disk: RequestVoteResp { granted: true } at index 2 of [ PersistHardState { term: 1, |
| ↳ |  | `a_node_does_not_vote_twice_in_one_term` | **KILLED** — a vote reached the wire before it reached the disk: RequestVoteResp { granted: true } at index 2 of [ PersistHardState { term: 1, |
| M6b | a candidate asks for votes before its own vote is durable | `the_campaign_configuration_is_captured_in_the_same_step_that_raises_the_term` | **KILLED** — a vote reached the wire before it reached the disk: RequestVote { last_term: 0, last_round: 0 } at index 1 of [ RoleChanged { role: Candidate, |
| M7 | a node may vote twice in one term | `a_node_does_not_vote_twice_in_one_term` | **KILLED** — a second candidate of the same term was also granted a vote — both can now reach a majority of the same three nodes, which is two leaders of one term |
| ↳ |  | `one_term_never_elects_two_leaders_even_when_every_node_campaigns` | **KILLED** — assertion `left == right` failed: term 2 elected 2 leaders: {NodeId(1), NodeId(3)} left: 2 right: 1 |
| M8 | a pre-vote answer about any term is counted | `a_pre_vote_answer_about_another_term_is_not_counted` | **KILLED** — assertion `left == right` failed: an answer about the term we are already IN carried the campaign left: Candidate right: PreCandidate |
| M9 | a node with a live leader still grants pre-votes | `a_pre_vote_is_refused_by_a_node_that_is_still_being_served_by_a_leader` | **KILLED** — a node still hearing from its leader told a peer it could win, which raises the term on a healthy cluster |
| ↳ |  | `a_node_campaigning_into_a_wall_never_raises_the_clusters_term` | **KILLED** — assertion `left == right` failed: a node campaigning into a wall raised its own term left: 35 right: 1 |
| M10 | a node with a live leader still grants a real vote at the current term | `a_vote_in_the_current_term_is_refused_by_a_node_that_still_has_a_live_leader` | **KILLED** — a node being served by a leader of term 4 voted for a rival in term 4 |
| M11 | `behind` / `unjoined` / not-a-voter no longer block a campaign | `a_node_that_knows_its_configuration_is_stale_does_not_campaign` | **KILLED** — assertion `left == right` failed: a node holding a stale configuration campaigned left: PreCandidate right: Follower |
| ↳ |  | `an_unjoined_node_is_not_cleared_by_any_number_of_ticks` | **KILLED** — assertion `left == right` failed: a node holding none of the log campaigned left: PreCandidate right: Follower |
| ↳ |  | `a_node_absent_from_its_own_configuration_never_campaigns` | **KILLED** — assertion `left == right` failed: a node that has been removed from the cluster stood for election in it left: PreCandidate right: Follower |
| M12 | `unjoined` cleared by a timer | `an_unjoined_node_is_not_cleared_by_any_number_of_ticks` | **KILLED** — a timer cleared `unjoined` |
| M13 | `unjoined` requires a non-zero watermark, so an empty cluster can never elect | `joining_an_empty_cluster_still_lets_a_new_member_stand` | **KILLED** — joining a cluster with an empty log left a member that can never stand |
| M14 | `unjoined` cleared by any watermark at all, without comparing to this node's store | `an_unjoined_node_stands_only_once_it_holds_what_a_quorum_holds` | **KILLED** — a watermark this node cannot match cleared the flag |
| M15 | applying a configuration clears `unjoined` too | `an_unjoined_node_is_not_cleared_by_any_number_of_ticks` | **KILLED** — applying a configuration cleared `unjoined` as well — they are cleared by different evidence, and knowing the voter set says nothing about holding a single round |
| M16a | the lease window is the election timeout | `a_leader_that_stops_hearing_from_a_majority_demotes_itself_before_any_peer_can_win` | **KILLED** — assertion `left == right` failed: the leader demoted after 18 ticks, not on its lease window of 8 left: 18 right: 8 |
| M16b | the lease window is the election base | `a_leader_that_stops_hearing_from_a_majority_demotes_itself_before_any_peer_can_win` | **KILLED** — assertion `left == right` failed: the leader demoted after 10 ticks, not on its lease window of 8 left: 10 right: 8 |
| M17 | the lease measures the MOST silent voter instead of a majority | `a_leader_that_keeps_hearing_from_a_majority_keeps_its_office` | **KILLED** — assertion `left == right` failed: a leader hearing from a majority demoted itself at tick 7 left: Follower right: Leader |
| M18 | the lease is satisfied by any single answer, not a majority | `a_leader_that_hears_only_from_a_minority_still_demotes` | **KILLED** — a leader hearing from two of five never demoted |
| M19 | a new leader appends no term-establishing entry | `a_new_leader_appends_a_no_op_of_its_own_term_before_it_can_commit_anything` | **KILLED** — a new leader appended nothing, so it can never commit anything |
| M20 | a new leader records optimism as evidence (matched = next) | `a_new_leader_starts_every_peer_with_no_evidence_and_optimistic_next` | **KILLED** — assertion `left == right` failed: a new leader claimed evidence about a peer's log that it does not have — quorum is counted over `matched` left: 1 right: 0 |
| M21 | a new leader waits for the heartbeat interval before asserting its office | `a_new_leader_asserts_its_office_at_once_and_then_on_its_heartbeat_interval` | **KILLED** — assertion `left == right` failed: a new leader did not tell both peers at once; every follower is already counting down left: 0 right: 2 |
| M22 | the election timeout is drawn once and never redrawn | `every_campaign_redraws_its_timeout_inside_the_documented_window` | **KILLED** — every campaign drew the same timeout, so two nodes campaign in lockstep for ever: {10} |
| M23 | a timeout campaigns for real instead of pre-voting first | `a_campaign_that_wins_nothing_is_retried_from_the_pre_vote_and_not_from_a_raised_term` | **KILLED** — no pre-campaign started in 44 ticks (role candidate, term 2) |
| M24 | a campaign in flight is left running after this node learns it is stale | `a_campaign_in_flight_is_abandoned_when_this_node_learns_it_is_stale` | **KILLED** — condition not reached in 500 ticks (role pre-candidate, term 0) |
| M25 | a pre-candidate counts real votes it never asked for | `a_pre_candidate_does_not_count_real_votes_it_never_asked_for` | **KILLED** — assertion `left == right` failed: a pre-candidate was elected by real votes it never asked for left: Leader right: PreCandidate |
| M26 | a node that is its own majority still waits for somebody to answer | `a_single_voter_configuration_elects_itself_without_asking_anybody` | **KILLED** — condition not reached in 100 ticks (role pre-candidate, term 0) |
| M27 | the election timeout is a constant, so nothing is spread and nothing replays distinctly | `a_campaign_replays_identically_from_a_seed_and_its_windows_are_not_all_equal` | **KILLED** — every campaign waited exactly the same number of ticks ({10}), so two nodes campaign in lockstep for ever, which is a split vote that repeats |
| ↳ |  | `every_campaign_redraws_its_timeout_inside_the_documented_window` | **KILLED** — every campaign drew the same timeout, so two nodes campaign in lockstep for ever: {10} |
| M28 | a configuration report at the version already held marks a current node stale | `an_older_configuration_report_does_not_mark_a_current_node_stale` | **KILLED** — an equal or older configuration report marked a current node stale |

### The one that survived first, and what it found

**M27** (`draw_timeout` returns a constant) was killed by
`every_campaign_redraws_its_timeout_inside_the_documented_window` but **survived**
`a_campaign_replays_identically_from_a_seed_...`. That was a defect in my test, not in the code:
its second assertion claimed to detect "every node draws the same timeout and campaigns in
lockstep", but `Consensus::new` draws the *first* timeout in the frozen `mod.rs` from the seed, so
eight seeds diverge on their first campaign whatever `draw_timeout` does. The test was measuring the
initial draw and reporting it as the whole rule.

Fixed by making the test measure what it claims: it now observes, through `step`, the tick each of a
node's first campaigns actually begins on, and requires the gaps between them not to be all equal.
M27 is killed by it now. The suite was re-run and M22/M27 re-fired against the corrected test.

## Numbers, pasted

```
$ cargo test --lib consensus::
running 45 tests
test result: ok. 45 passed; 0 failed; 0 ignored; 0 measured; 810 filtered out; finished in 0.01s
```
(35 in `tests_election.rs`, 10 pre-existing in `tests_contract.rs`.)

```
$ cargo test --lib
running 855 tests
test result: ok. 855 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 27.83s
```
855 = 820 + the 35 added, and the 820 is measured rather than remembered: the filtered run above
reports `810 filtered out` — the tests outside `consensus::` — and `consensus::` held exactly the 10
`tests_contract` tests before this change. Run to a durable file
(`scratchpad/lib-suite.txt`, exit 0) rather than watched, because a full `cargo test` in this
worktree is liable to be SIGTERMed mid-suite by the surrounding fleet.

```
$ cargo test --tests --no-run
COMPILE_EXIT=0   # 81 test executables built
```
The integration targets are not run here: they need the environment (python drivers, freshly built
example binaries) and none of them touch `consensus::`, so running them would be evidence about the
environment rather than about this change.

```
$ cargo build --lib
warning: methods `apply_config`, `observe_config_at`, and `observe_quorum_watermark` are never used
```
The three seams above. Same class as the warning already on `mod.rs`'s `may_campaign` before this
change; their callers are F2's and F5's files.

## What I found, for the agents downstream

**1. `Consensus` cannot read its own log, so `Body::Append.entries` cannot be filled from inside the
state machine.** The struct carries `last_term`, `last_round`, `commit`, `applied`, `durable`,
`snapshot_round`, `snapshot_term` — and no log handle. `Action::Persist { entries }` writes; nothing
reads back. This costs F1 nothing (a heartbeat is `entries: []`, and the only `prev` the state
machine can name truthfully is its own tail `(last_round, last_term)` — a follower that is behind
answers with the `hint`, which is the documented one-step back-up rather than a special case). **It
is a real problem for F2**: shipping actual entries needs either a new field on the frozen
`Consensus`, or the caller intercepting `Action::Send` and filling `entries` from `log.rs` before
the wire. I did not edit `mod.rs`; flagging it as the brief asks.

**2. `mod.rs`'s `become_follower` resets `since_heard = 0` before `on_vote_msg` ever runs**, so a
node that refuses a higher-term candidate — because *its own* log is more complete — has still
restarted its own election countdown on that candidate's behalf. etcd resets only on a grant. This
is liveness, not safety: the node with the most complete log is repeatedly delayed from standing by
the stale candidate it just refused. It cannot be fixed from `election.rs`, because the reset has
already happened by the time the handler is called; it would need `mod.rs` to defer the reset to the
vote handler. Worked around by doing nothing (the effect is a delay, not a wrong outcome).

**3. A granted higher-term vote costs two `PersistHardState` in one step** — `become_follower`
emits `(term, None)`, then `on_request_vote` emits `(term, Some(candidate))`. Correct, and the
second is the binding one; it is two fsyncs where one would do. Same cause as (2): the frozen
`mod.rs` acts before the handler.

**4. `PreVoteResp` cannot teach a lagging node the real term**, because `mod.rs` classes it as
`hypothetical`. That is right — the point of pre-vote is that the hypothetical moves nobody's term —
but it means a node whose term is behind converges only via an `Append` or a `RequestVote`. F8's
simulator should not expect a pre-vote-only exchange to converge terms.

**5. `acked` is untouched.** Its own doc puts it in `membership.rs` (F5): it is the leader's evidence
that a configuration change reached a majority of the set that created it. The F1 half of "the pair,
not the version" is in `observe_config_at`, which compares the whole `CfgAt` — mutant M28 relaxes
`>` to `>=` and is killed.

**6. `a_new_term_is_a_new_vote` has a test but no mutant**, because the rule it tests
(`voted_for` cleared on a term change) lives in the frozen `mod.rs::become_follower`. Every other
test in the file has at least one mutant named against it.

**7. A node does not filter vote requests by whether the asker is in its own configuration**, and
that is deliberate rather than an omission: during a membership change a voter's configuration lags
the leader's, and filtering there deadlocks the change. The disruption case is handled by the wall
(`leader_is_live`) and by `may_campaign` on the asking side.

## Measured against the committed tree, not the working directory

```
$ git archive HEAD | tar -x -C <fresh dir> && cd <fresh dir> && cargo test --lib consensus::
running 45 tests
test result: ok. 45 passed; 0 failed; 0 ignored; 0 measured; 810 filtered out; finished in 0.02s
EXIT=0
```
`git status --short -- src/ tests/ examples/` is empty, so nothing untracked is sitting on the import
path — the usual way a deletion or a new module passes locally and fails for whoever receives it.

## Measured, not asserted

* The leader demotes on tick **8** of silence (`lease`), with `election_base` **10** and every drawn
  timeout in `[10, 20)` — strictly shorter than the shortest window any peer can draw. The test
  counts the ticks rather than reading the field, and M16a/M16b (lease := `election_timeout`,
  lease := `election_base`) are both killed by it.
* `src/consensus/mod.rs`, `config.rs`, `replicate.rs` and `tests_contract.rs` are byte-identical to
  `HEAD` (`git diff --stat` over them is empty). The test module is reached from `election.rs` via
  `#[path]`, so the frozen file needed no `mod` declaration.
