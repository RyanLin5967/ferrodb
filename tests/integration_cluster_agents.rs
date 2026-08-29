//! F9 — agent isolation on a cluster.
//!
//! Design authority: `DISTRIBUTED.md` §F9 and its "What happens to an agent's in-flight branch
//! when its node dies". Three claims are on trial here and each is tested where it can actually
//! fail:
//!
//! 1. **An agent's speculative work does not reach a quorum; only the accepted result does.**
//!    Tested by *counting* — `ConsensusCost` — against three real nodes, three real TCP sockets and
//!    three real round logs. Not by timing: a claim about round trips that is asserted with a
//!    stopwatch passes on a fast machine whatever the code does.
//!
//! 2. **A merge carries the base round its gate verdict was computed against, and a merge whose
//!    base is no longer the committed head is re-evaluated, not applied.** Tested against a
//!    *scripted* log, because the window it closes is between a gate's read and its merge's
//!    commit, and waiting for a real cluster to land a write in that window is testing on
//!    whichever interleaving the machine happened to produce. The script puts the write there on
//!    purpose, every time.
//!
//! 3. **Every node reaches the same verdict for one merge command.** Tested by driving one log
//!    through several independent ledgers and requiring their answers to be identical — which is
//!    the property that makes the rule a *cluster* ordering rule and not a local optimisation.
//!
//! # Forced to fire
//!
//! Every rule below has a named test and a mutant that was applied on purpose, run, and seen to
//! kill that test. The mutants and what each printed are listed in
//! `scratchpad/F9-agents-cluster.md`. A test nobody has watched fail is not evidence.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ferrodb::agent_sql::cluster::{
    moves_base, BranchApplier, BranchEffect, BranchLedger, ClusterAgents, ClusterBranchId,
    ClusterSession, MergeVerdict, NodeReplicator, Replicated, ReplicatedState,
};
use ferrodb::agent_sql::runtime::{AgentRuntime, ExecCtx, RunIdentity};
use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::consensus::config::Config;
use ferrodb::consensus::node::{Applier, Node, NodeOptions};
use ferrodb::consensus::{BranchOp, Command, Entry, NodeId, Round};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

fn entry(round: Round, command: Command) -> Entry {
    Entry { term: 1, round, command }
}

fn wal(n: u64) -> Command {
    Command::WalBatch { start_lsn: n, bytes: vec![n as u8; 4] }
}

fn fork_op(child: ClusterBranchId, parent: ClusterBranchId) -> Command {
    Command::Branch {
        op: BranchOp::Fork { child: child.0, parent: parent.0, fork_epoch: 1, lease_millis: 900_000 },
    }
}

fn merge_op(branch: ClusterBranchId, base_round: Round) -> Command {
    Command::Branch { op: BranchOp::Merge { branch: branch.0, base_round } }
}

/// Feed a whole log to a fresh ledger, in order, exactly as a node's applier would.
fn drive(entries: &[Entry]) -> BranchLedger {
    let mut l = BranchLedger::new();
    for e in entries {
        l.apply(e);
    }
    l
}

// =================================================================================================
// The cluster branch namespace
// =================================================================================================

/// **The rule:** a replicated `BranchOp::Fork` names a branch by an id that no other node can
/// mint. Both catalogs in this crate mint from a node-local `AtomicU64`, so without a partitioned
/// namespace two nodes both propose `Fork { child: 1 }` and the second silently claims the first's
/// arenas — which the reaper then frees.
#[test]
fn two_nodes_forking_at_the_same_local_id_get_different_cluster_ids() {
    let local = BranchId::new(1, 0);
    let a = ClusterBranchId::of(N1, local).unwrap();
    let b = ClusterBranchId::of(N2, local).unwrap();
    assert_ne!(
        a, b,
        "n1 and n2 both minted local branch 1 and got one cluster id: a replicated fork from each \
         would have the second overwrite the first"
    );
    assert_eq!(a.owner(), Some(N1));
    assert_eq!(b.owner(), Some(N2));
    assert_eq!(a.local_id(), 1);
    assert_eq!(b.local_id(), 1);
}

/// The half that makes the id worth having: it says where the rows are. That is the question a
/// promoted leader has to answer about every branch it inherits.
#[test]
fn a_cluster_branch_id_names_the_node_that_holds_its_rows() {
    let b = ClusterBranchId::of(N2, BranchId::new(7, 0)).unwrap();
    assert!(b.rows_are_on(N2));
    assert!(!b.rows_are_on(N1), "n1 claimed rows that only n2 ever held");
    assert_eq!(format!("{b}"), "n2/b7");
}

/// The trunk is the replicated database, not one node's branch, so it must be the same id
/// everywhere or two nodes' forks would name different parents.
#[test]
fn the_trunk_is_the_same_branch_on_every_node_and_has_no_owner() {
    for n in [N1, N2, N3] {
        assert_eq!(ClusterBranchId::of(n, BranchId::TRUNK).unwrap(), ClusterBranchId::TRUNK);
    }
    assert_eq!(ClusterBranchId::TRUNK.owner(), None);
    assert!(ClusterBranchId::TRUNK.rows_are_on(N3), "the trunk's rows are on every node");
    assert_eq!(format!("{}", ClusterBranchId::TRUNK), "trunk");
}

/// FORCED FIRE. Truncation is the failure this type exists to prevent, so the boundary is crossed
/// on purpose and the refusal is required.
#[test]
fn a_local_branch_id_too_large_for_the_namespace_is_refused_not_truncated() {
    let fits = BranchId::new((1u64 << 40) - 1, 0);
    let does_not = BranchId::new(1u64 << 40, 0);

    let ok = ClusterBranchId::of(N1, fits).expect("the largest id that fits must be accepted");
    assert_eq!(ok.local_id(), (1u64 << 40) - 1, "the largest id that fits came back changed");
    assert_eq!(ok.owner(), Some(N1));

    let e = ClusterBranchId::of(N1, does_not).unwrap_err();
    assert!(
        format!("{e}").contains("refusing rather than truncating"),
        "an id one past the namespace was accepted, so two of one node's branches now share a \
         cluster identity: {e}"
    );
}

/// Node 0 would mint cluster id `0 << 40 | local`, which collides with the trunk for local id 0 and
/// sits in the trunk's numeric neighbourhood for everything else. Refused rather than documented.
#[test]
fn node_zero_cannot_own_a_branch() {
    let e = ClusterBranchId::of(NodeId(0), BranchId::new(1, 0)).unwrap_err();
    assert!(format!("{e}").contains("node id 0 cannot own a branch"), "{e}");
}

// =================================================================================================
// What moves the base
// =================================================================================================

/// **The rule, as a table.** Named individually rather than in a loop, because the useful failure
/// message is "LeaseTick was classified as base-moving", not "the table changed".
#[test]
fn only_commands_that_can_change_what_the_gate_read_move_the_base() {
    // Yes: these change rows or the shape of the tables the gate re-checked its guards against.
    assert!(moves_base(&wal(1)), "a WAL batch changes rows and must move the base");
    assert!(
        moves_base(&Command::Catalog {
            op: ferrodb::wal::log::DdlOp::CreateTable,
            table: "inventory".into(),
            columns: Vec::new(),
        }),
        "a DDL command changes the shape a guard was re-checked against"
    );
    assert!(
        moves_base(&merge_op(ClusterBranchId(1), 1)),
        "a merge that applies publishes rows into the target"
    );

    // No: and each of these is load-bearing for liveness, not merely an omission. LeaseTick and
    // NoOp are what a healthy idle cluster produces, so classifying either as base-moving would
    // re-evaluate every merge on every cluster for ever.
    assert!(!moves_base(&Command::LeaseTick { unix_millis: 1 }), "the cluster clock moved the base");
    assert!(!moves_base(&Command::NoOp), "a term-establishing NoOp moved the base");
    assert!(
        !moves_base(&Command::ArenaGrant { node: N1, first_page: 64, page_count: 64 }),
        "an extent grant moved the base"
    );
    assert!(!moves_base(&Command::TxnIdRange { node: N1, lo: 1, hi: 100 }));
    assert!(!moves_base(&Command::Checkpoint), "discarding a WAL prefix is not a logical change");
    assert!(!moves_base(&Command::Membership { config: Config::new([N1], 1, 0) }));
    assert!(
        !moves_base(&fork_op(ClusterBranchId(1), ClusterBranchId::TRUNK)),
        "a fork copies zero data pages (DESIGN.md criterion 1) and cannot move a base"
    );
    assert!(!moves_base(&Command::Branch { op: BranchOp::Abandon { branch: 1 } }));
    assert!(!moves_base(&Command::Branch { op: BranchOp::Reap { branch: 1, generation: 1 } }));
}

// =================================================================================================
// The ledger: the ordering rule, as a pure function of the committed log
// =================================================================================================

/// The ordinary case. Nothing moved between the base the gate read and the round that linearized
/// the merge, so the verdict it carries is still a verdict about this database.
#[test]
fn a_merge_whose_base_is_still_the_committed_head_applies() {
    let b = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let log = vec![
        entry(1, wal(1)),
        entry(2, fork_op(b, ClusterBranchId::TRUNK)),
        entry(3, Command::LeaseTick { unix_millis: 1_000 }),
        entry(4, Command::NoOp),
        // The gate read the head as of round 1; rounds 2..4 move nothing.
        entry(5, merge_op(b, 1)),
    ];
    let l = drive(&log);
    assert_eq!(
        l.verdict_at(5),
        Some(&MergeVerdict::Applied { branch: b, base_round: 1 }),
        "a merge was re-evaluated although nothing between its base and its round could change \
         what the gate read"
    );
    assert_eq!(l.get(b).unwrap().state, ReplicatedState::Merged { at: 5 });
}

/// **The headline rule.** A write committed after the gate read its base makes the verdict a
/// verdict about a database that no longer exists, so the command applies to nothing and the
/// proposer re-evaluates.
#[test]
fn a_merge_racing_a_committed_write_to_its_base_is_re_evaluated_not_applied() {
    let b = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let log = vec![
        entry(1, fork_op(b, ClusterBranchId::TRUNK)),
        // The gate reads the head: round 1.
        entry(2, wal(2)), // somebody else's write lands first
        entry(3, merge_op(b, 1)),
    ];
    let l = drive(&log);
    assert_eq!(
        l.verdict_at(3),
        Some(&MergeVerdict::ReEvaluate { branch: b, base_round: 1, moved_at: 2 }),
        "a merge scored against round 1 was applied even though round 2 changed the base under it"
    );
    assert_eq!(
        l.get(b).unwrap().state,
        ReplicatedState::Live,
        "a re-evaluated merge sealed the branch anyway, so the retry will be refused as not-live"
    );
    assert_eq!(
        l.last_base_move(),
        2,
        "a merge that published nothing moved the base, so every later merge re-evaluates for ever"
    );
}

/// The `>` in the rule, not `>=`. The base round *is* the committed head the evaluation read, so a
/// move at exactly that round is one the gate saw. `>=` would re-evaluate every merge proposed
/// after any write, which is every merge on a busy cluster.
#[test]
fn a_merge_whose_base_round_is_exactly_the_last_base_move_applies() {
    let b = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let log = vec![
        entry(1, fork_op(b, ClusterBranchId::TRUNK)),
        entry(2, wal(2)),
        // The gate read the head *after* round 2, so round 2 is inside the state it scored.
        entry(3, merge_op(b, 2)),
    ];
    let l = drive(&log);
    assert_eq!(
        l.verdict_at(3),
        Some(&MergeVerdict::Applied { branch: b, base_round: 2 }),
        "a merge was re-evaluated against a write its own gate had already read"
    );
}

/// The detector must not fire on the rounds a healthy idle cluster produces, or a merge can never
/// land. This is the "shown to stay quiet" half of forcing it to fire.
#[test]
fn heartbeat_rounds_do_not_re_evaluate_a_merge() {
    let b = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let mut log = vec![entry(1, fork_op(b, ClusterBranchId::TRUNK))];
    let mut r = 2;
    for tick in 0..50 {
        log.push(entry(r, Command::LeaseTick { unix_millis: 1_000 + tick }));
        r += 1;
        log.push(entry(r, Command::ArenaGrant { node: N1, first_page: 64, page_count: 64 }));
        r += 1;
    }
    log.push(entry(r, merge_op(b, 1)));
    let l = drive(&log);
    assert!(
        matches!(l.verdict_at(r), Some(MergeVerdict::Applied { .. })),
        "100 rounds of cluster housekeeping re-evaluated a merge: {:?}",
        l.verdict_at(r)
    );
}

/// A merge that was re-evaluated published nothing, so it must not itself count as a base move —
/// otherwise the first re-evaluation poisons every merge behind it and none ever lands.
#[test]
fn a_re_evaluated_merge_does_not_itself_move_the_base() {
    let a = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let b = ClusterBranchId::of(N1, BranchId::new(2, 0)).unwrap();
    let log = vec![
        entry(1, fork_op(a, ClusterBranchId::TRUNK)),
        entry(2, fork_op(b, ClusterBranchId::TRUNK)),
        entry(3, wal(3)),
        entry(4, merge_op(a, 2)), // re-evaluated: round 3 moved the base
        entry(5, merge_op(b, 3)), // scored after round 3, and round 4 published nothing
    ];
    let l = drive(&log);
    assert!(matches!(l.verdict_at(4), Some(MergeVerdict::ReEvaluate { .. })));
    assert_eq!(
        l.verdict_at(5),
        Some(&MergeVerdict::Applied { branch: b, base_round: 3 }),
        "a merge that published nothing was counted as a base move, so the merge behind it was \
         re-evaluated for no reason"
    );
}

/// A merge that *applied* published rows, so every verdict scored against an earlier base is now
/// stale. The conditional half of `moves_base`, which only the ledger can decide.
#[test]
fn a_merge_that_applied_moves_the_base_for_the_merge_behind_it() {
    let a = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let b = ClusterBranchId::of(N1, BranchId::new(2, 0)).unwrap();
    let log = vec![
        entry(1, fork_op(a, ClusterBranchId::TRUNK)),
        entry(2, fork_op(b, ClusterBranchId::TRUNK)),
        entry(3, merge_op(a, 2)), // applies, publishes rows
        entry(4, merge_op(b, 2)), // scored against the same base a moved
    ];
    let l = drive(&log);
    assert!(matches!(l.verdict_at(3), Some(MergeVerdict::Applied { .. })));
    assert_eq!(
        l.verdict_at(4),
        Some(&MergeVerdict::ReEvaluate { branch: b, base_round: 2, moved_at: 3 }),
        "two branches merged against one base and the second was applied on a verdict the first \
         had already invalidated"
    );
}

/// **The property that makes this a cluster rule.** Several nodes, one log, one answer.
#[test]
fn every_node_reaches_the_same_verdict_for_every_merge_command() {
    let a = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let b = ClusterBranchId::of(N2, BranchId::new(1, 0)).unwrap();
    let log = vec![
        entry(1, fork_op(a, ClusterBranchId::TRUNK)),
        entry(2, fork_op(b, ClusterBranchId::TRUNK)),
        entry(3, wal(3)),
        entry(4, merge_op(a, 1)),
        entry(5, merge_op(b, 3)),
        entry(6, Command::LeaseTick { unix_millis: 5 }),
        entry(7, merge_op(a, 6)),
    ];
    // Three independent ledgers, as three nodes would have. None of them holds a single row of
    // either branch; only n1 and n2 ever did.
    let ledgers: Vec<BranchLedger> = (0..3).map(|_| drive(&log)).collect();
    for round in 1..=7u64 {
        let first = ledgers[0].verdict_at(round).cloned();
        for (i, l) in ledgers.iter().enumerate() {
            assert_eq!(
                l.verdict_at(round).cloned(),
                first,
                "node {i} disagreed with node 0 about round {round}: two nodes disagreeing about \
                 whether a merge applied is two databases"
            );
        }
    }
    for (i, l) in ledgers.iter().enumerate() {
        assert_eq!(l.get(a).map(|x| x.state), ledgers[0].get(a).map(|x| x.state), "node {i}");
        assert_eq!(l.get(b).map(|x| x.state), ledgers[0].get(b).map(|x| x.state), "node {i}");
        assert_eq!(l.last_base_move(), ledgers[0].last_base_move(), "node {i}");
    }
    // Anti-vacuity: the log must actually contain both answers, or agreement is agreement about
    // nothing.
    assert!(matches!(ledgers[0].verdict_at(4), Some(MergeVerdict::ReEvaluate { .. })));
    assert!(matches!(ledgers[0].verdict_at(5), Some(MergeVerdict::Applied { .. })));
}

/// A committed round may be re-delivered. Applying a fork twice would refuse itself as a
/// collision, and applying a merge twice would move the base twice.
#[test]
fn a_re_delivered_round_is_a_no_op() {
    let b = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let mut l = BranchLedger::new();
    let f = entry(1, fork_op(b, ClusterBranchId::TRUNK));
    let m = entry(2, merge_op(b, 1));
    assert_eq!(l.apply(&f), BranchEffect::Forked(b));
    assert_eq!(l.apply(&f), BranchEffect::AlreadyApplied, "a re-delivered fork was applied again");
    assert!(matches!(l.apply(&m), BranchEffect::Merged(MergeVerdict::Applied { .. })));
    assert_eq!(l.apply(&m), BranchEffect::AlreadyApplied, "a re-delivered merge was applied again");
    assert_eq!(l.last_applied(), 2);
    assert!(l.rejections().is_empty(), "a re-delivery was reported as a rejection: {:?}", l.rejections());
}

/// A merge of a branch the cluster never agreed exists is a proposer that has lost track of its
/// own state. Refused, not applied — applying it would seal a branch that is not there and leave
/// the proposer believing its rows landed.
#[test]
fn a_merge_for_a_branch_whose_fork_never_committed_is_refused() {
    let b = ClusterBranchId::of(N1, BranchId::new(9, 0)).unwrap();
    let l = drive(&[entry(1, merge_op(b, 0))]);
    match l.verdict_at(1) {
        Some(MergeVerdict::Refused { why, .. }) => {
            assert!(why.contains("no committed round created"), "{why}")
        }
        other => panic!("a merge of an unforked branch produced {other:?}"),
    }
    assert!(l.get(b).is_none(), "a refused merge created the branch it could not find");
}

/// FORCED FIRE for the namespace. The packed id makes this impossible between *nodes*; the ledger
/// still refuses it, because a collision reaching here means something upstream is minting ids it
/// does not own, and overwriting the first branch is how the reaper is told to free arenas that
/// belong to a live one.
#[test]
fn a_fork_whose_child_id_is_already_taken_is_refused_not_overwritten() {
    let b = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let l = drive(&[
        entry(1, fork_op(b, ClusterBranchId::TRUNK)),
        entry(2, fork_op(b, ClusterBranchId::TRUNK)),
    ]);
    assert_eq!(l.get(b).unwrap().forked_at, 1, "the second fork overwrote the first branch");
    assert!(
        l.rejections().iter().any(|r| r.contains("refusing rather than overwriting")),
        "a colliding fork was accepted silently: {:?}",
        l.rejections()
    );
}

/// Reaping is destructive and a `BranchId` generation makes it unrecoverable, which is exactly why
/// it is a committed decision. A second reap is refused rather than repeated.
#[test]
fn a_reap_is_a_replicated_decision_and_is_refused_twice() {
    let b = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let l = drive(&[
        entry(1, fork_op(b, ClusterBranchId::TRUNK)),
        entry(2, Command::Branch { op: BranchOp::Reap { branch: b.0, generation: 1 } }),
        entry(3, Command::Branch { op: BranchOp::Reap { branch: b.0, generation: 2 } }),
    ]);
    assert_eq!(l.get(b).unwrap().state, ReplicatedState::Reaped { at: 2, generation: 1 });
    assert!(
        l.rejections().iter().any(|r| r.contains("already reaped at generation 1")),
        "{:?}",
        l.rejections()
    );
}

/// A fork off a branch no round created, or off one already merged, is refused: the parent's
/// `live_children` is what the reaper reads, and a child whose parent does not know about it is
/// the GC correctness hole `branch/catalog.rs` puts both halves of a fork in one write to avoid.
#[test]
fn a_fork_off_a_parent_the_cluster_does_not_hold_is_refused() {
    let ghost = ClusterBranchId::of(N2, BranchId::new(4, 0)).unwrap();
    let child = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let l = drive(&[entry(1, fork_op(child, ghost))]);
    assert!(l.get(child).is_none(), "a child of a parent nobody holds was created");
    assert!(l.rejections().iter().any(|r| r.contains("which no committed round created")), "{:?}", l.rejections());
}

/// **Exit criterion 9, at the branch level:** two nodes never disagree about whether a branch is
/// live. Every disposal — merge, abandon, reap — is a committed decision, so nodes driving one log
/// hold identical answers about every branch in it. Nothing here reads a clock, which is the point:
/// wall clocks disagree, and reaping is destructive and unrecoverable.
#[test]
fn no_two_nodes_disagree_about_whether_a_branch_is_live() {
    let a = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let child = ClusterBranchId::of(N1, BranchId::new(2, 0)).unwrap();
    let b = ClusterBranchId::of(N2, BranchId::new(1, 0)).unwrap();
    let d = ClusterBranchId::of(N3, BranchId::new(1, 0)).unwrap();
    let log = vec![
        entry(1, fork_op(a, ClusterBranchId::TRUNK)),
        entry(2, fork_op(b, ClusterBranchId::TRUNK)),
        entry(3, fork_op(child, a)),
        entry(4, fork_op(d, ClusterBranchId::TRUNK)),
        // An hour of clock skew between the nodes would change nothing below: no node decides any
        // of this from its own clock, and the tick is just another round.
        entry(5, Command::LeaseTick { unix_millis: 1_000 }),
        entry(6, merge_op(a, 4)),
        entry(7, Command::Branch { op: BranchOp::Abandon { branch: b.0 } }),
        entry(8, Command::Branch { op: BranchOp::Reap { branch: d.0, generation: 1 } }),
    ];
    let ledgers: Vec<BranchLedger> = (0..3).map(|_| drive(&log)).collect();
    let states: Vec<Vec<(ClusterBranchId, ReplicatedState)>> =
        ledgers.iter().map(|l| l.all().map(|x| (x.id, x.state)).collect()).collect();
    for (i, st) in states.iter().enumerate() {
        assert_eq!(st, &states[0], "node {i} holds a different opinion of which branches are live");
    }

    // Anti-vacuity: all four dispositions must actually be in there, or three nodes are agreeing
    // that nothing happened.
    let get = |id: ClusterBranchId| states[0].iter().find(|(x, _)| *x == id).unwrap().1;
    assert_eq!(get(a), ReplicatedState::Merged { at: 6 });
    assert_eq!(get(b), ReplicatedState::Abandoned { at: 7 });
    assert_eq!(get(child), ReplicatedState::Live);
    assert_eq!(get(d), ReplicatedState::Reaped { at: 8, generation: 1 });
    assert_eq!(get(ClusterBranchId::TRUNK), ReplicatedState::Live);
}

// =================================================================================================
// What a node's death does to a branch, and what a promoted leader inherits
// =================================================================================================

/// `DISTRIBUTED.md`: *a branch is a transaction.* What survives is what was merged; what was in
/// flight is gone, and the surviving nodes can *name* it rather than discover it.
#[test]
fn a_branch_whose_owner_died_is_named_as_lost_work_and_its_merged_sibling_is_not() {
    let in_flight = ClusterBranchId::of(N2, BranchId::new(1, 0)).unwrap();
    let merged = ClusterBranchId::of(N2, BranchId::new(2, 0)).unwrap();
    let mine = ClusterBranchId::of(N1, BranchId::new(1, 0)).unwrap();
    let l = drive(&[
        entry(1, fork_op(in_flight, ClusterBranchId::TRUNK)),
        entry(2, fork_op(merged, ClusterBranchId::TRUNK)),
        entry(3, fork_op(mine, ClusterBranchId::TRUNK)),
        entry(4, merge_op(merged, 3)),
    ]);

    // n2 dies. n1 is promoted and asks what n2 was working on.
    assert_eq!(
        l.orphans_of(N2),
        vec![in_flight],
        "the promoted leader did not name the branch n2 was mid-flight on, or named one that had \
         already merged"
    );
    assert_eq!(
        l.merges_owned_by(N2),
        vec![(merged, 4)],
        "what a promoted leader inherits is the merge; it was not there"
    );
    assert!(!in_flight.rows_are_on(N1), "n1 believes it holds rows that only n2 ever had");
    assert_eq!(l.live_owned_by(N1), vec![mine], "n1's own branch was swept up as an orphan");
}

// =================================================================================================
// A real runtime against a scripted log
// =================================================================================================

/// A single-node database with the whole agent surface on it.
struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Db {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("f9.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("f9.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 100);", &mut s);
    }

    fn main_qty(&mut self, id: i32) -> Option<i32> {
        let mut s = self.session();
        let sql = format!("SELECT qty FROM inventory WHERE id = {id};");
        match self.ok(&sql, &mut s) {
            Outcome::Rows(rows) => rows.first().and_then(|r| match r.first() {
                Some(Value::Integer(i)) => Some(*i),
                _ => None,
            }),
            _ => panic!("expected rows from: {sql}"),
        }
    }

    /// Run a statement inside an agent session, exactly as a connection that said
    /// `BEGIN AGENT SESSION` would: the write goes into the branch's private buffer and nothing
    /// touches the shared tables.
    fn on_branch(&mut self, cs: &ClusterSession, sql: &str) {
        let mut s = self.session();
        s.agent = Some(cs.session.clone());
        self.ok(sql, &mut s);
    }
}

/// **A log this test writes.**
///
/// Everything proposed is immediately committed, and `pump` hands one entry at a time to the
/// ledger — so the interleaving that matters can be placed exactly rather than waited for.
/// `inject` is the whole point: it puts a base-moving round between a gate's read and its merge's
/// commit, on purpose, every run.
struct Scripted {
    node: NodeId,
    ledger: Arc<Mutex<BranchLedger>>,
    log: Mutex<Vec<Entry>>,
    applied: Mutex<usize>,
    /// Commands appended immediately *before* the next proposal. Drained unless `sticky`.
    inject: Mutex<Vec<Command>>,
    sticky: Mutex<bool>,
    leader: Mutex<Option<NodeId>>,
    /// When set, the next `pump` deposes this node and applies nothing — a leader that loses
    /// office while a caller is waiting on a round.
    depose_on_pump: Mutex<bool>,
}

impl Scripted {
    fn new(node: NodeId, ledger: Arc<Mutex<BranchLedger>>) -> Arc<Scripted> {
        Arc::new(Scripted {
            node,
            ledger,
            log: Mutex::new(Vec::new()),
            applied: Mutex::new(0),
            inject: Mutex::new(Vec::new()),
            sticky: Mutex::new(false),
            leader: Mutex::new(Some(node)),
            depose_on_pump: Mutex::new(false),
        })
    }

    /// Land `c` immediately before whatever is proposed next — the race, scripted.
    fn inject_before_next_proposal(&self, c: Command) {
        lock(&self.inject).push(c);
    }

    /// Keep injecting before *every* proposal: a base under sustained write load.
    fn inject_before_every_proposal(&self, c: Command) {
        lock(&self.inject).push(c);
        *lock(&self.sticky) = true;
    }

    fn step_down(&self) {
        *lock(&self.leader) = Some(NodeId(99));
    }

    /// Lose office on the next turn of the driver, and stop making progress — which is what a
    /// deposed leader looks like to something waiting on a round.
    fn depose_on_next_pump(&self) {
        *lock(&self.depose_on_pump) = true;
    }

    /// Apply everything proposed. Stands for the time an agent spends working after its fork.
    fn settle(&self) {
        loop {
            let (a, n) = (*lock(&self.applied), lock(&self.log).len());
            if a >= n {
                return;
            }
            self.pump().unwrap();
        }
    }

    fn log_commands(&self) -> Vec<Command> {
        lock(&self.log).iter().map(|e| e.command.clone()).collect()
    }
}

impl Replicated for Scripted {
    fn propose(&self, c: Command) -> Result<Round, FerroError> {
        if *lock(&self.leader) != Some(self.node) {
            return Err(FerroError::NotLeader { leader: None });
        }
        let injected: Vec<Command> = if *lock(&self.sticky) {
            lock(&self.inject).clone()
        } else {
            lock(&self.inject).drain(..).collect()
        };
        let mut log = lock(&self.log);
        for i in injected {
            let r = log.len() as u64 + 1;
            log.push(entry(r, i));
        }
        let r = log.len() as u64 + 1;
        log.push(entry(r, c));
        Ok(r)
    }

    fn committed_head(&self) -> Round {
        lock(&self.log).len() as u64
    }

    fn pump(&self) -> Result<(), FerroError> {
        if *lock(&self.depose_on_pump) {
            *lock(&self.leader) = Some(NodeId(99));
            return Ok(());
        }
        let mut applied = lock(&self.applied);
        let next = {
            let log = lock(&self.log);
            if *applied >= log.len() {
                return Ok(());
            }
            log[*applied].clone()
        };
        lock(&self.ledger).apply(&next);
        *applied += 1;
        Ok(())
    }

    fn leader(&self) -> Option<NodeId> {
        *lock(&self.leader)
    }
}

/// A seeded database, a scripted log, and a coordinator over both.
fn scripted_cluster() -> (Db, Arc<Scripted>, ClusterAgents) {
    let mut db = Db::new();
    db.seed();
    let ledger = Arc::new(Mutex::new(BranchLedger::new()));
    let repl = Scripted::new(N1, ledger.clone());
    let agents = ClusterAgents::new(N1, db.runtime.clone(), repl.clone(), ledger);
    (db, repl, agents)
}

fn agent(name: &'static str) -> RunIdentity<'static> {
    RunIdentity { agent_id: name, run_id: Some("r_1"), ..RunIdentity::default() }
}

/// **The headline.** A write commits between the gate's read and the merge's own round, so the
/// verdict the merge carries is about a database that no longer exists. It is re-evaluated — not
/// applied, and not silently dropped either.
#[test]
fn a_merge_racing_a_committed_change_to_its_base_is_re_evaluated_rather_than_silently_applied() {
    let (mut db, repl, agents) = scripted_cluster();
    let cs = agents.fork(agent("pricing"), BranchId::TRUNK).unwrap();
    repl.settle();
    db.on_branch(&cs, "UPDATE inventory SET qty = 42 WHERE id = 1;");

    // The race, placed rather than waited for: this lands at the round after the base the gate is
    // about to read, and before the merge command's own round.
    repl.inject_before_next_proposal(wal(7));

    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let report = agents.merge(&mut ctx, cs.branch()).expect("the merge must land on its retry");

    assert_eq!(
        report.reevaluations, 1,
        "a write committed after the gate read its base did not force a re-evaluation: the merge \
         was applied on a verdict about a database that had already moved"
    );
    assert!(report.report.applied_to_target, "the re-evaluated merge never published");
    assert_eq!(db.main_qty(1), Some(42), "the branch's write did not reach main after re-evaluation");
    assert_eq!(agents.cost().reevaluations, 1);

    // At the log level: the first merge command really was refused, and a second one really was
    // proposed. Without both halves this passes for a merge that was never raced at all.
    let l = lock(agents.ledger());
    assert!(
        matches!(l.verdict_at(3), Some(MergeVerdict::ReEvaluate { moved_at: 2, .. })),
        "round 3 should be the merge whose base round 2 invalidated, and is {:?}",
        l.verdict_at(3)
    );
    assert!(
        matches!(l.verdict_at(4), Some(MergeVerdict::Applied { base_round: 3, .. })),
        "round 4 should be the re-proposed merge, scored against round 3: {:?}",
        l.verdict_at(4)
    );
    assert_eq!(
        repl.log_commands().len(),
        4,
        "expected fork, the racing write, the refused merge and the re-proposed merge"
    );
}

/// A base that keeps moving cannot be merged against, and the honest answer is to say so with the
/// round that moved — not to spin for ever. Optimistic concurrency starves under sustained
/// conflict; `DESIGN.md` §4 chose optimistic, so this is the chosen semantics being stated.
#[test]
fn a_merge_whose_base_never_stops_moving_is_refused_with_the_round_that_moved() {
    let (mut db, repl, agents) = scripted_cluster();
    let agents = agents.with_max_reevaluations(2);
    let cs = agents.fork(agent("pricing"), BranchId::TRUNK).unwrap();
    repl.settle();
    db.on_branch(&cs, "UPDATE inventory SET qty = 42 WHERE id = 1;");
    repl.inject_before_every_proposal(wal(7));

    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let e = agents.merge(&mut ctx, cs.branch()).unwrap_err();
    let msg = format!("{e}");
    assert!(msg.contains("re-evaluated 3 times"), "the refusal did not say how often: {msg}");
    assert!(msg.contains("most recently at round"), "the refusal did not name the round: {msg}");
    assert_eq!(db.main_qty(1), Some(100), "a starved merge published anyway");
    assert_eq!(
        db.runtime.branches().get(cs.branch()).unwrap().state,
        ferrodb::branch::types::BranchState::Live,
        "a starved merge destroyed the branch, so the agent cannot retry"
    );
}

/// **The performance claim, counted.** A fork and a hundred writes block on nothing; the merge is
/// the one thing that pays.
#[test]
fn a_fork_and_a_hundred_writes_cost_no_quorum_round_trip_and_the_merge_costs_one() {
    let (mut db, repl, agents) = scripted_cluster();
    let cs = agents.fork(agent("pricing"), BranchId::TRUNK).unwrap();

    assert_eq!(
        agents.cost().quorum_waits,
        0,
        "the fork blocked on a quorum; an agent that must wait for consensus to start is the cost \
         this design exists to avoid"
    );
    assert_eq!(agents.cost().proposals, 1, "the fork's metadata did not reach the log");

    repl.settle();
    for i in 0..100 {
        db.on_branch(&cs, &format!("UPDATE inventory SET qty = {} WHERE id = 1;", 100 - i));
    }
    assert_eq!(
        agents.cost(),
        ferrodb::agent_sql::cluster::ConsensusCost { proposals: 1, quorum_waits: 0, reevaluations: 0 },
        "a hundred speculative writes reached the log; only the accepted result may"
    );

    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let report = agents.merge(&mut ctx, cs.branch()).unwrap();
    assert!(report.report.applied_to_target);
    assert_eq!(
        agents.cost(),
        ferrodb::agent_sql::cluster::ConsensusCost { proposals: 2, quorum_waits: 1, reevaluations: 0 },
        "the merge did not cost exactly one proposal and one round trip"
    );
    assert_eq!(db.main_qty(1), Some(1));

    // And the log carries the two branch commands and nothing else: a hundred agent writes left no
    // trace in it at all.
    let cmds = repl.log_commands();
    assert_eq!(cmds.len(), 2, "the log carries {} commands, not 2: {cmds:?}", cmds.len());
    assert!(cmds.iter().all(|c| matches!(c, Command::Branch { .. })));
}

/// A merge the gate declines is a node-local decision. Proposing it would spend a quorum round
/// trip to agree on something that was never going to be published.
#[test]
fn a_merge_the_gate_declines_costs_no_consensus_at_all() {
    let (mut db, repl, agents) = scripted_cluster();
    let cs = agents.fork(agent("suspect"), BranchId::TRUNK).unwrap();
    repl.settle();
    db.on_branch(&cs, "UPDATE inventory SET qty = 42 WHERE id = 1;");
    db.runtime.quarantine(cs.branch(), "held for this test").unwrap();

    let before = agents.cost();
    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let e = agents.merge(&mut ctx, cs.branch()).unwrap_err();
    assert!(format!("{e}").contains("quarantined"), "{e}");
    assert_eq!(
        agents.cost().proposals,
        before.proposals,
        "a merge that could never publish still spent a consensus round"
    );
    assert_eq!(lock(agents.ledger()).get(cs.cluster_id).unwrap().state, ReplicatedState::Live);
}

/// A fork the cluster refused never arrives, so a merge that waits for it would otherwise end in a
/// timeout that says nothing. The ledger recorded why; the refusal must carry it.
#[test]
fn a_merge_whose_fork_the_cluster_refused_says_so_rather_than_timing_out() {
    let (mut db, repl, agents) = scripted_cluster();
    let agents = agents.with_pump_budget(200);

    let parent = agents.fork(agent("first"), BranchId::TRUNK).unwrap();
    repl.settle();

    // The cluster disposes of the parent — a lease expiry, an operator, a promoted leader sweeping
    // a dead node's work — in the same breath as this node forks a child off it. Injected rather
    // than proposed through `abandon`, so the parent stays live *locally* and the local fork
    // succeeds: the whole point is a branch that exists here and not there.
    repl.inject_before_next_proposal(Command::Branch {
        op: BranchOp::Abandon { branch: parent.cluster_id.0 },
    });
    let child = agents.fork(agent("second"), parent.branch()).unwrap();
    repl.settle();
    db.on_branch(&child, "UPDATE inventory SET qty = 42 WHERE id = 1;");

    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let e = agents.merge(&mut ctx, child.branch()).unwrap_err();
    let msg = format!("{e}");
    assert!(
        msg.contains("the cluster refused its fork"),
        "the merge timed out instead of reporting why the fork never arrived: {msg}"
    );
    assert!(
        msg.contains("Abandoned"),
        "the refusal did not carry the ledger's reason, so an operator has to go looking: {msg}"
    );
    assert_eq!(db.main_qty(1), Some(100), "a branch the cluster never accepted published anyway");
}

/// **What a promoted leader does with a dead node's work.** Its rows are gone — they were
/// node-local by design and were never replicated — so what is left is a disposal decision, and it
/// has to be a *replicated* one: two nodes disagreeing about whether one of these is live ends in
/// a reap, and a reap is unrecoverable.
#[test]
fn a_promoted_leader_disposes_of_a_dead_nodes_branches_by_a_replicated_decision() {
    let (_db, repl, agents) = scripted_cluster();
    let mine = agents.fork(agent("mine"), BranchId::TRUNK).unwrap();
    let theirs_a = ClusterBranchId::of(N2, BranchId::new(1, 0)).unwrap();
    let theirs_b = ClusterBranchId::of(N2, BranchId::new(2, 0)).unwrap();
    // n2's forks reached the log, as every fork does. Its rows never did, and never could.
    repl.propose(fork_op(theirs_a, ClusterBranchId::TRUNK)).unwrap();
    repl.propose(fork_op(theirs_b, ClusterBranchId::TRUNK)).unwrap();
    repl.settle();
    assert!(!agents.rows_are_here(theirs_a), "this node claims rows only n2 ever held");
    assert!(agents.rows_are_here(mine.cluster_id));

    let disposed = agents.abandon_orphans_of(N2).unwrap();
    assert_eq!(
        disposed.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![theirs_a, theirs_b],
        "the sweep did not name exactly what n2 was working on"
    );
    repl.settle();

    let l = lock(agents.ledger());
    assert!(matches!(l.get(theirs_a).unwrap().state, ReplicatedState::Abandoned { .. }));
    assert!(matches!(l.get(theirs_b).unwrap().state, ReplicatedState::Abandoned { .. }));
    assert_eq!(
        l.get(mine.cluster_id).unwrap().state,
        ReplicatedState::Live,
        "the sweep took this node's own live branch with it"
    );
}

/// A node does not declare itself dead, and abandoning every branch it is working on is not a
/// recovery step.
#[test]
fn a_node_cannot_sweep_its_own_branches_as_orphans() {
    let (_db, _repl, agents) = scripted_cluster();
    let e = agents.abandon_orphans_of(N1).unwrap_err();
    assert!(format!("{e}").contains("is this node"), "{e}");
}

/// Reaping is destructive and unrecoverable, so it is proposed by whoever runs the lease scan and
/// **decided** by the log — never by one node's clock.
#[test]
fn a_reap_goes_through_the_log_and_carries_the_generation() {
    let (_db, repl, agents) = scripted_cluster();
    let cs = agents.fork(agent("done"), BranchId::TRUNK).unwrap();
    repl.settle();
    agents.abandon(&cs).unwrap();
    repl.settle();

    let round = agents.propose_reap(cs.cluster_id, 1).unwrap();
    repl.settle();
    assert_eq!(
        lock(agents.ledger()).get(cs.cluster_id).unwrap().state,
        ReplicatedState::Reaped { at: round, generation: 1 },
        "the reap did not reach the log, so a second node could still believe the branch is live"
    );
}

/// Exit criterion 10's anti-vacuity half, at this layer: a write to a node that does not lead is
/// **refused**, never silently served. A branch created on a follower would take writes no quorum
/// will ever see.
#[test]
fn a_fork_on_a_node_that_does_not_lead_is_refused_and_creates_nothing() {
    let (_db, repl, agents) = scripted_cluster();
    repl.step_down();
    let e = agents.fork(agent("pricing"), BranchId::TRUNK).unwrap_err();
    assert!(matches!(e, FerroError::NotLeader { .. }), "a follower accepted a fork: {e}");
    assert!(repl.log_commands().is_empty(), "a refused fork still reached the log");
    assert_eq!(agents.cost().proposals, 0);
    // **And it created nothing.** A branch record minted on a node that cannot commit is garbage
    // the cluster never agreed on: it burns an id and an epoch, and it pins whatever arenas it is
    // charged until something reaps it. The leadership check therefore comes *before* the local
    // fork, not after it.
    let records = _db.runtime.branches().all_branches().unwrap();
    assert_eq!(
        records.len(),
        1,
        "a refused fork left {} branch records behind, not just the trunk: {:?}",
        records.len(),
        records.iter().map(|r| (r.branch_id, r.state)).collect::<Vec<_>>()
    );
}

/// A merge that loses its leader **while waiting on a round** must refuse, rather than block a
/// client until the pump budget runs out. The check is inside the wait loop and not only at its
/// entry, because losing office during the wait is the case that actually happens.
#[test]
fn a_merge_that_loses_the_leadership_while_waiting_refuses_rather_than_blocking() {
    let (mut db, repl, agents) = scripted_cluster();
    let agents = agents.with_pump_budget(1_000);
    // Deliberately not settled: the fork is proposed and not yet applied, so the merge must wait.
    let cs = agents.fork(agent("pricing"), BranchId::TRUNK).unwrap();
    db.on_branch(&cs, "UPDATE inventory SET qty = 42 WHERE id = 1;");
    repl.depose_on_next_pump();

    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let e = agents.merge(&mut ctx, cs.branch()).unwrap_err();
    assert!(
        matches!(e, FerroError::NotLeader { .. }),
        "a leader deposed mid-wait blocked until its budget ran out instead of refusing: {e}"
    );
    assert_eq!(db.main_qty(1), Some(100), "a deposed leader published a merge");
}

/// A merge that loses its leader mid-flight must refuse and publish nothing, rather than block a
/// client until a budget runs out.
#[test]
fn a_merge_that_loses_the_leadership_mid_flight_refuses_and_publishes_nothing() {
    let (mut db, repl, agents) = scripted_cluster();
    let cs = agents.fork(agent("pricing"), BranchId::TRUNK).unwrap();
    repl.settle();
    db.on_branch(&cs, "UPDATE inventory SET qty = 42 WHERE id = 1;");
    repl.step_down();

    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let e = agents.merge(&mut ctx, cs.branch()).unwrap_err();
    assert!(matches!(e, FerroError::NotLeader { .. }), "a deposed leader completed a merge: {e}");
    assert_eq!(db.main_qty(1), Some(100), "a deposed leader published a merge");
}

// =================================================================================================
// Three real nodes, three real sockets, three real round logs
// =================================================================================================

/// Three `Node`s in one process, each with its own listener, directory and round log.
///
/// One process rather than three, deliberately: `tests/integration_consensus_failover.rs` already
/// owns the three-process, `kill -9` proof of the *driver*. What is on trial here is what an agent
/// costs, and that is a question about counts on the coordinator, which needs the coordinator and
/// the cluster in one address space to be asked at all. The consensus underneath is entirely real —
/// real elections, real appends over TCP, real fsyncs.
struct Fleet {
    reps: Vec<Arc<NodeReplicator>>,
    ledgers: Vec<Arc<Mutex<BranchLedger>>>,
    /// Every command each node actually applied, in order. Behind the branch ledger via
    /// [`BranchApplier::chained`], which is how a real server hangs its WAL applier off the same
    /// node: the ledger must see every round, not only the branch ones.
    tallies: Vec<Arc<Mutex<Vec<Command>>>>,
    _dirs: Vec<tempfile::TempDir>,
}

/// Records what a node committed, so "an agent's rows never reached a quorum" can be *read* off a
/// follower rather than inferred from a counter on the leader.
struct Tally(Arc<Mutex<Vec<Command>>>);

impl Applier for Tally {
    fn apply(&mut self, e: &Entry) -> Result<(), FerroError> {
        lock(&self.0).push(e.command.clone());
        Ok(())
    }
}

impl Fleet {
    fn start(n: u32) -> Arc<Fleet> {
        let listeners: Vec<TcpListener> =
            (0..n).map(|_| TcpListener::bind("127.0.0.1:0").expect("an ephemeral port")).collect();
        let addrs: BTreeMap<NodeId, SocketAddr> = listeners
            .iter()
            .enumerate()
            .map(|(i, l)| (NodeId(i as u32 + 1), l.local_addr().unwrap()))
            .collect();
        let cfg = Config::new((1..=n).map(NodeId), 1, 0);

        let mut reps = Vec::new();
        let mut ledgers = Vec::new();
        let mut tallies = Vec::new();
        let mut dirs = Vec::new();
        for (i, l) in listeners.into_iter().enumerate() {
            let id = NodeId(i as u32 + 1);
            let dir = tempfile::tempdir().unwrap();
            // The transport refuses a peer map containing this node's own id.
            let peers: BTreeMap<NodeId, SocketAddr> =
                addrs.iter().filter(|(k, _)| **k != id).map(|(k, v)| (*k, *v)).collect();
            let ledger = Arc::new(Mutex::new(BranchLedger::new()));
            let tally = Arc::new(Mutex::new(Vec::new()));
            // Distinct seeds: two nodes drawing the same election timeout split every vote.
            let opts = NodeOptions::new(dir.path(), peers, 0x5eed ^ (i as u64 + 1).wrapping_mul(0x9E37_79B9))
                .tick_of(Duration::from_millis(20));
            let applier = BranchApplier::chained(ledger.clone(), Box::new(Tally(tally.clone())));
            let node = Node::start(id, cfg.clone(), l, opts, applier).unwrap();
            reps.push(Arc::new(NodeReplicator::new(node, Duration::from_millis(1))));
            ledgers.push(ledger);
            tallies.push(tally);
            dirs.push(dir);
        }
        Arc::new(Fleet { reps, ledgers, tallies, _dirs: dirs })
    }

    /// One turn of every node's driver loop.
    ///
    /// In a real server this is a thread of its own and nothing an agent does drives it. Here it
    /// is called explicitly so the whole test stays deterministic — but it must be called *while*
    /// an agent works, not only around it: a leader whose driver is starved of ticks for longer
    /// than an election timeout is deposed by its own peers, which is correct behaviour and has
    /// nothing to do with what is on trial.
    fn pump_all(&self) {
        for r in &self.reps {
            r.pump().expect("a node's driver failed");
        }
    }

    /// Drive until one node holds office, **every** node agrees it does, it has committed its own
    /// term-establishing round, and that has held for `STABLE` consecutive turns. Returns its
    /// index.
    ///
    /// All four clauses earn their place. "One node believes it leads" is true for a few turns of
    /// every election that is about to be lost, and a test that forks against such a node fails
    /// with `NotLeader` from a cluster behaving perfectly — which is a flake that looks exactly
    /// like the bug this file is about.
    fn elect(&self) -> usize {
        const STABLE: usize = 50;
        let mut who: Option<usize> = None;
        let mut held_for = 0usize;
        for _ in 0..100_000 {
            self.pump_all();
            let claiming: Vec<usize> = (0..self.reps.len())
                .filter(|i| self.reps[*i].leader() == Some(NodeId(*i as u32 + 1)))
                .collect();
            let settled = claiming.len() == 1 && {
                let l = NodeId(claiming[0] as u32 + 1);
                self.reps.iter().all(|r| r.leader() == Some(l))
                    && self.reps[claiming[0]].committed_head() >= 1
            };
            if settled {
                if who == Some(claiming[0]) {
                    held_for += 1;
                } else {
                    who = Some(claiming[0]);
                    held_for = 1;
                }
                if held_for >= STABLE {
                    return claiming[0];
                }
            } else {
                who = None;
                held_for = 0;
            }
        }
        panic!("no stable leader after 100000 turns of a three-node cluster");
    }

    /// Drive until every node has applied through `round`.
    fn settle_to(&self, round: Round) {
        for _ in 0..20_000 {
            if self.ledgers.iter().all(|l| lock(l).last_applied() >= round) {
                return;
            }
            self.pump_all();
        }
        panic!(
            "round {round} did not reach every node; applied = {:?}",
            self.ledgers.iter().map(|l| lock(l).last_applied()).collect::<Vec<_>>()
        );
    }

    fn shutdown(&self) {
        for r in &self.reps {
            r.shutdown();
        }
    }
}

/// What a coordinator on node `me` sees.
///
/// `propose`, `committed_head` and `leader` go to that node's own [`NodeReplicator`] — the real
/// seam. `pump` drives every node, because in one process nobody else is polling the followers'
/// sockets and a leader alone commits nothing.
struct FleetSeam {
    me: usize,
    fleet: Arc<Fleet>,
}

impl Replicated for FleetSeam {
    fn propose(&self, c: Command) -> Result<Round, FerroError> {
        self.fleet.reps[self.me].propose(c)
    }
    fn committed_head(&self) -> Round {
        self.fleet.reps[self.me].committed_head()
    }
    fn pump(&self) -> Result<(), FerroError> {
        for r in &self.fleet.reps {
            r.pump()?;
        }
        Ok(())
    }
    fn leader(&self) -> Option<NodeId> {
        self.fleet.reps[self.me].leader()
    }
}

/// **The claim, against a real cluster.** A fork and its agent's writes block on nothing; the
/// merge is the one thing that reaches a quorum, and every node ends up agreeing it did.
#[test]
fn on_three_real_nodes_only_the_merge_reaches_a_quorum() {
    // The database is built *before* the cluster is asked who leads: creating files and running
    // DDL takes long enough to starve a driver nobody is turning, and a leader deposed by its own
    // peers because the test was busy is not a finding.
    let mut db = Db::new();
    db.seed();

    let fleet = Fleet::start(3);
    let leader = fleet.elect();
    let me = NodeId(leader as u32 + 1);
    let agents = ClusterAgents::new(
        me,
        db.runtime.clone(),
        Arc::new(FleetSeam { me: leader, fleet: fleet.clone() }),
        fleet.ledgers[leader].clone(),
    );

    let cs = agents.fork(agent("pricing"), BranchId::TRUNK).unwrap();
    assert_eq!(
        agents.cost().quorum_waits,
        0,
        "the fork blocked on a quorum against a real cluster"
    );

    // The agent works. Nothing here touches consensus at all — the cluster's driver turns beside
    // it, exactly as a server's own thread would, and neither one waits for the other.
    for i in 0..25 {
        db.on_branch(&cs, &format!("UPDATE inventory SET qty = {} WHERE id = 1;", 100 - i));
        fleet.pump_all();
    }
    assert_eq!(agents.cost().quorum_waits, 0, "an agent's writes blocked on a quorum");
    assert_eq!(agents.cost().proposals, 1, "an agent's writes reached the replicated log");

    // Time passes and the fork's metadata commits, as it would while the agent was working.
    fleet.settle_to(cs.fork_round);

    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let report = agents.merge(&mut ctx, cs.branch()).expect("the merge must commit");
    drop(ctx);

    assert!(report.report.applied_to_target, "the merge did not publish");
    assert_eq!(db.main_qty(1), Some(76));
    assert_eq!(
        agents.cost().quorum_waits,
        1,
        "the merge cost {} round trips, not one",
        agents.cost().quorum_waits
    );
    assert_eq!(agents.cost().proposals, 2, "fork and merge, and nothing else, reach the log");

    let merge_round = report.merge_round.expect("the merge was linearized by a round");
    fleet.settle_to(merge_round);

    // **Every node agrees, and none of them holds a row of it.** The branch is n1's; its rows were
    // never replicated, and what reached the other two is the decision.
    for (i, l) in fleet.ledgers.iter().enumerate() {
        let l = lock(l);
        assert_eq!(
            l.get(cs.cluster_id).map(|b| b.state),
            Some(ReplicatedState::Merged { at: merge_round }),
            "node {} does not agree that {} merged at round {merge_round}",
            i + 1,
            cs.cluster_id
        );
        assert!(
            matches!(l.verdict_at(merge_round), Some(MergeVerdict::Applied { .. })),
            "node {} computed a different verdict for round {merge_round}: {:?}",
            i + 1,
            l.verdict_at(merge_round)
        );
        assert!(l.rejections().is_empty(), "node {} rejected an op: {:?}", i + 1, l.rejections());
    }
    fleet.shutdown();
}

/// A follower must refuse, never silently serve. Exit criterion 10's anti-vacuity half against a
/// real cluster: the refusal names where to reconnect.
#[test]
fn on_a_real_cluster_a_fork_on_a_follower_is_refused_and_names_the_leader() {
    let db = Db::new();

    let fleet = Fleet::start(3);
    let leader = fleet.elect();
    let follower = (leader + 1) % 3;
    let mut book = BTreeMap::new();
    book.insert(NodeId(leader as u32 + 1), "127.0.0.1:65001".to_string());
    let agents = ClusterAgents::new(
        NodeId(follower as u32 + 1),
        db.runtime.clone(),
        Arc::new(FleetSeam { me: follower, fleet: fleet.clone() }),
        fleet.ledgers[follower].clone(),
    )
    .with_client_addresses(book);

    let e = agents.fork(agent("pricing"), BranchId::TRUNK).unwrap_err();
    match &e {
        FerroError::NotLeader { leader: Some(a) } => {
            assert_eq!(a, "127.0.0.1:65001", "the refusal sent the client to the wrong place")
        }
        other => panic!("a follower accepted an agent session: {other}"),
    }
    assert!(format!("{e}").contains("reconnect there"));
    assert_eq!(agents.cost().proposals, 0, "a refused fork still reached the log");
    fleet.shutdown();
}

/// **Read off a follower, not inferred from the leader.** A node that never ran the agent holds,
/// in its committed log, its leader's term-establishing `NoOp`, the fork, and the merge — and not
/// one of the agent's rows. That is `DISTRIBUTED.md`'s claim stated as a thing you can look at.
#[test]
fn a_followers_committed_log_holds_the_merge_and_not_one_agent_row() {
    let mut db = Db::new();
    db.seed();

    let fleet = Fleet::start(3);
    let leader = fleet.elect();
    let follower = (leader + 1) % 3;
    let agents = ClusterAgents::new(
        NodeId(leader as u32 + 1),
        db.runtime.clone(),
        Arc::new(FleetSeam { me: leader, fleet: fleet.clone() }),
        fleet.ledgers[leader].clone(),
    );

    let cs = agents.fork(agent("pricing"), BranchId::TRUNK).unwrap();
    for i in 0..30 {
        db.on_branch(&cs, &format!("UPDATE inventory SET qty = {} WHERE id = 1;", 100 - i));
        fleet.pump_all();
    }
    fleet.settle_to(cs.fork_round);

    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let report = agents.merge(&mut ctx, cs.branch()).unwrap();
    drop(ctx);
    fleet.settle_to(report.merge_round.unwrap());

    let cmds = lock(&fleet.tallies[follower]).clone();
    let forks = cmds.iter().filter(|c| matches!(c, Command::Branch { op: BranchOp::Fork { .. } })).count();
    let merges = cmds.iter().filter(|c| matches!(c, Command::Branch { op: BranchOp::Merge { .. } })).count();
    let batches = cmds.iter().filter(|c| matches!(c, Command::WalBatch { .. })).count();

    // Anti-vacuity first: a follower that applied nothing at all would pass the interesting
    // assertion trivially.
    assert_eq!(forks, 1, "the follower did not receive the fork: {cmds:?}");
    assert_eq!(merges, 1, "the follower did not receive the merge: {cmds:?}");
    assert_eq!(
        batches, 0,
        "an agent's speculative rows reached a follower as {batches} WAL batch(es); only the \
         accepted result may reach a quorum"
    );
    assert!(
        cmds.iter().all(|c| matches!(c, Command::NoOp | Command::Branch { .. })),
        "a follower's committed log carries more than the term-establishing NoOp and the two \
         branch commands: {cmds:?}"
    );
    fleet.shutdown();
}

/// **The whole of the performance argument, at the log.** A hundred agent writes across three
/// branches leave nothing in the replicated log but their forks and their merges.
#[test]
fn a_hundred_agent_writes_across_three_branches_leave_only_forks_and_merges_in_the_log() {
    let mut db = Db::new();
    db.seed();
    {
        let mut s = db.session();
        db.ok("INSERT INTO inventory VALUES (2, 100);", &mut s);
        db.ok("INSERT INTO inventory VALUES (3, 100);", &mut s);
    }

    let fleet = Fleet::start(3);
    let leader = fleet.elect();
    let me = NodeId(leader as u32 + 1);
    let agents = ClusterAgents::new(
        me,
        db.runtime.clone(),
        Arc::new(FleetSeam { me: leader, fleet: fleet.clone() }),
        fleet.ledgers[leader].clone(),
    );

    let mut merged = Vec::new();
    for row in 1..=3 {
        let cs = agents.fork(agent("fanout"), BranchId::TRUNK).unwrap();
        fleet.settle_to(cs.fork_round);
        for i in 0..100 {
            db.on_branch(
                &cs,
                &format!("UPDATE inventory SET qty = {} WHERE id = {row};", 100 - i),
            );
            fleet.pump_all();
        }
        let bp = db.bp.clone();
        let txn = db.txn.clone();
        let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
        let r = agents.merge(&mut ctx, cs.branch()).unwrap_or_else(|e| panic!("branch {row}: {e}"));
        drop(ctx);
        assert!(r.report.applied_to_target, "branch {row} did not publish");
        merged.push(r.merge_round.unwrap());
    }

    assert_eq!(
        agents.cost().proposals,
        6,
        "300 agent writes and 3 merges cost {} proposals; the design says one per fork and one per \
         accepted result",
        agents.cost().proposals
    );
    assert_eq!(agents.cost().quorum_waits, 3, "one round trip per merge, and no others");
    for row in 1..=3 {
        assert_eq!(db.main_qty(row), Some(1), "row {row} did not get its merged value");
    }
    fleet.settle_to(*merged.iter().max().unwrap());
    fleet.shutdown();
}
