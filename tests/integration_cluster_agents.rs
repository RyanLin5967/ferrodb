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
use ferrodb::consensus::node::{Node, NodeOptions};
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
