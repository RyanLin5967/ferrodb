# F9 mutants

Each rule, the mutant that breaks it, and what the test named against it printed.
Driver: `bench/evidence/F9-mutants.py`. Every test was first shown to pass on the clean tree,
or a kill would prove nothing.

- **clean** `a_merge_racing_a_committed_write_to_its_base_is_re_evaluated_not_applied` — M1  a WAL batch does not move the base
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `a_merge_whose_base_round_is_exactly_the_last_base_move_applies` — M2  the base-moved comparison is >= instead of >
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `a_re_evaluated_merge_does_not_itself_move_the_base` — M3  a re-evaluated merge moves the base too
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `two_nodes_forking_at_the_same_local_id_get_different_cluster_ids` — M4  the cluster branch id drops the owning node
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `a_local_branch_id_too_large_for_the_namespace_is_refused_not_truncated` — M5  an oversized local id is packed rather than refused
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `a_fork_and_a_hundred_writes_cost_no_quorum_round_trip_and_the_merge_costs_one` — M6  a fork waits for its own commit
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.06s`
- **clean** `a_re_delivered_round_is_a_no_op` — M7  the ledger has no idempotence guard
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `a_fork_whose_child_id_is_already_taken_is_refused_not_overwritten` — M8  a colliding fork overwrites the branch already there
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `a_merge_the_gate_declines_costs_no_consensus_at_all` — M9  the cluster merge does not check quarantine
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.02s`
- **clean** `a_fork_on_a_node_that_does_not_lead_is_refused_and_creates_nothing` — M10 a fork does not check leadership before creating the branch
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.02s`
- **clean** `a_merge_that_loses_the_leadership_while_waiting_refuses_rather_than_blocking` — M11 leadership is not re-checked while waiting on a round
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.02s`
- **clean** `heartbeat_rounds_do_not_re_evaluate_a_merge` — M12 a LeaseTick moves the base
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `a_merge_racing_a_committed_change_to_its_base_is_re_evaluated_rather_than_silently_applied` — M13 a re-evaluated merge is published anyway
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.03s`
- **clean** `a_merge_for_a_branch_whose_fork_never_committed_is_refused` — M14 a merge of a branch no fork created is applied
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `a_reap_is_a_replicated_decision_and_is_refused_twice` — M15 a second reap of one branch is allowed
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `every_node_reaches_the_same_verdict_for_every_merge_command` — M16 the verdict depends on something that is not in the log
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **clean** `a_merge_whose_fork_the_cluster_refused_says_so_rather_than_timing_out` — M18 a merge whose fork the cluster refused just times out
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.03s`
- **clean** `a_merge_whose_base_never_stops_moving_is_refused_with_the_round_that_moved` — M17 the re-evaluation bound is 100x looser
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.02s`
- **mutant** `a_merge_racing_a_committed_write_to_its_base_is_re_evaluated_not_applied` — M1  a WAL batch does not move the base
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `a_merge_whose_base_round_is_exactly_the_last_base_move_applies` — M2  the base-moved comparison is >= instead of >
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `a_re_evaluated_merge_does_not_itself_move_the_base` — M3  a re-evaluated merge moves the base too
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `two_nodes_forking_at_the_same_local_id_get_different_cluster_ids` — M4  the cluster branch id drops the owning node
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `a_local_branch_id_too_large_for_the_namespace_is_refused_not_truncated` — M5  an oversized local id is packed rather than refused
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `a_fork_and_a_hundred_writes_cost_no_quorum_round_trip_and_the_merge_costs_one` — M6  a fork waits for its own commit
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.07s`
- **mutant** `a_re_delivered_round_is_a_no_op` — M7  the ledger has no idempotence guard
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `a_fork_whose_child_id_is_already_taken_is_refused_not_overwritten` — M8  a colliding fork overwrites the branch already there
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `a_merge_the_gate_declines_costs_no_consensus_at_all` — M9  the cluster merge does not check quarantine
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.04s`
- **mutant** `a_fork_on_a_node_that_does_not_lead_is_refused_and_creates_nothing` — M10 a fork does not check leadership before creating the branch
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.08s`
- **mutant** `a_merge_that_loses_the_leadership_while_waiting_refuses_rather_than_blocking` — M11 leadership is not re-checked while waiting on a round
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.05s`
- **mutant** `heartbeat_rounds_do_not_re_evaluate_a_merge` — M12 a LeaseTick moves the base
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `a_merge_racing_a_committed_change_to_its_base_is_re_evaluated_rather_than_silently_applied` — M13 a re-evaluated merge is published anyway
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.07s`
- **mutant** `a_merge_for_a_branch_whose_fork_never_committed_is_refused` — M14 a merge of a branch no fork created is applied
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `a_reap_is_a_replicated_decision_and_is_refused_twice` — M15 a second reap of one branch is allowed
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `every_node_reaches_the_same_verdict_for_every_merge_command` — M16 the verdict depends on something that is not in the log
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.00s`
- **mutant** `a_merge_whose_fork_the_cluster_refused_says_so_rather_than_timing_out` — M18 a merge whose fork the cluster refused just times out
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.06s`
- **mutant** `a_merge_whose_base_never_stops_moving_is_refused_with_the_round_that_moved` — M17 the re-evaluation bound is 100x looser
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.06s`

## Later run: M19, M20, M21

- **clean** `a_promoted_leader_disposes_of_a_dead_nodes_branches_by_a_replicated_decision` — M19 the orphan sweep is not filtered by owner
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.04s`
- **clean** `a_node_cannot_sweep_its_own_branches_as_orphans` — M20 a node may declare itself dead and sweep its own branches
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.03s`
- **clean** `a_reap_goes_through_the_log_and_carries_the_generation` — M21 a reap is decided locally instead of through the log
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.03s`
- **mutant** `a_promoted_leader_disposes_of_a_dead_nodes_branches_by_a_replicated_decision` — M19 the orphan sweep is not filtered by owner
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.03s`
- **mutant** `a_node_cannot_sweep_its_own_branches_as_orphans` — M20 a node may declare itself dead and sweep its own branches
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.06s`
- **mutant** `a_reap_goes_through_the_log_and_carries_the_generation` — M21 a reap is decided locally instead of through the log
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.08s`

## Later run: M22, M23

- **clean** `no_two_nodes_disagree_about_whether_a_branch_is_live` — M22 the verdict depends on something that is not in the log (liveness half)
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
- **clean** `a_followers_committed_log_holds_the_merge_and_not_one_agent_row` — M23 the branch applier swallows the rounds instead of chaining them on
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.69s`
- **mutant** `no_two_nodes_disagree_about_whether_a_branch_is_live` — M22 the verdict depends on something that is not in the log (liveness half)
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
- **mutant** `a_followers_committed_log_holds_the_merge_and_not_one_agent_row` — M23 the branch applier swallows the rounds instead of chaining them on
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.79s`

## Later run: M27, M28

- **clean** `a_fork_off_a_parent_the_cluster_does_not_hold_is_refused` — M27 an unknown fork parent is treated as the trunk instead of refused
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
- **clean** `a_branch_whose_owner_died_is_named_as_lost_work_and_its_merged_sibling_is_not` — M28 the orphan list is not filtered by the node that died
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
- **mutant** `a_fork_off_a_parent_the_cluster_does_not_hold_is_refused` — M27 an unknown fork parent is treated as the trunk instead of refused
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
- **mutant** `a_branch_whose_owner_died_is_named_as_lost_work_and_its_merged_sibling_is_not` — M28 the orphan list is not filtered by the node that died
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`

## Later run: M24, M25, M26

- **clean** `the_trunk_is_the_same_branch_on_every_node_and_has_no_owner` — M24 the trunk is packed like any other branch instead of being id 0 everywhere
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
- **clean** `node_zero_cannot_own_a_branch` — M25 node 0 may own a branch
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
- **clean** `a_merge_that_applied_moves_the_base_for_the_merge_behind_it` — M26 a merge that applied does not move the base
  - `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
- **mutant** `the_trunk_is_the_same_branch_on_every_node_and_has_no_owner` — M24 the trunk is packed like any other branch instead of being id 0 everywhere
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
- **mutant** `node_zero_cannot_own_a_branch` — M25 node 0 may own a branch
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
- **mutant** `a_merge_that_applied_moves_the_base_for_the_merge_behind_it` — M26 a merge that applied does not move the base
  - `test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.00s`
