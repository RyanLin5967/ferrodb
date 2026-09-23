# D173 ADVERSARY (second attempt) — I TRIED TO REFUTE IT AND COULD NOT. THE WINDOW FIRES.

Agent `d173-adversary-2`. Worktree `/Users/idide/wt/ferrodb-d173-adversary-2`, branch
`d173-adversary-2`, commits `ddc1514` and `3e83b96` on top of `4912302`. **Nothing pushed, nothing
on main, no production file changed** (the one production edit was a mutant, applied and restored
inside this session — see §5).

Instrument for every number below: `cargo test --test integration_cluster_agents -- --nocapture
--test-threads=1` on this machine, raw output banked in
`/Users/idide/wt/ferrodb-d173-adversary-2/bench/d173_adversary2/`.

---

## 0. VERDICT

| exit item | answer |
|---|---|
| (1) can the CI signature be made to fire from a forced lease death? | **YES. 8 of 8 runs.** |
| (2) which `hold_leader` site is the slow one? | **the retry arm.** Structurally necessary, and corroborated by 36 measurements of the other sites. |
| (3) does losing leadership always truncate the proposed entry? | **NO — and it is worse than the D173 row thought.** The entry survives even when, at the instant of deposition, it is on **one node's log only**. |

⭐ **The claim in `d173_output_commit.md` is upheld, and its own self-narrowing paragraph ("the
window is rarer and harder to force than my first write-up implied") is WRONG.** That paragraph was
written on the first adversary's last words. Those words were right about `pump_until`'s ordering
and wrong about what it implies for the window. See §4.

---

## 1. THE HARNESS, AND WHY IT IS NOT A LIE AT THE SEAM

`tests/integration_cluster_agents.rs`, new `StarvePeersOnMerge`. From the moment a
`BranchOp::Merge` is proposed it drives **only the proposing node**. Its peers answer nothing,
`Progress::silent` grows on every leader tick, and `election.rs:leader_tick` finds
`since_quorum >= lease` (lease = 8 ticks, `mod.rs:471`, tick = 20 ms) and calls
`become_follower(term, None)`.

**No answer is faked anywhere.** `leader()` returns the node's own state machine's answer; `propose`
goes to the real `NodeReplicator`; the sockets are real. The only thing the harness owns is *who
gets driven*, which is exactly what `Fleet::pump_all`'s own doc-comment says a test owns — "a leader
whose driver is starved of ticks for longer than an election timeout is deposed by its own peers,
which is correct behaviour". This models a descheduled driver thread, which is what a CI runner at
**460 ms per `pump_all` turn** (6467 ms / 14 turns, from the D173 row's own log excerpt) is.

⛔ This is **not** the first adversary's `DeposeOnceAfterMergePropose`, which returned
`someone_else()` from `leader()`. I did not run that seam and quote nothing from it.

**Control, and it runs first:** `d173_control_the_same_script_without_starvation_merges_and_publishes`
— the identical script on a plain `FleetSeam` merges at round 3 and publishes (`qty=Some(98)`).
Without it, a failure below would only be evidence that the script fails.

---

## 2. EXIT ITEM (1) — IT FIRES, 8 OF 8

`d173_a_lease_death_after_propose_refuses_a_caller_whose_merge_committed`. One representative run
(`bench/d173_adversary2/target_rep4.txt`), verbatim:

    D173 attempt1 took ms=127 result=Err("not the leader, and this node does not currently know
      who is - an election is in progress or this node is partitioned from the cluster. Retry
      shortly; do not treat this as the leader being down.")
    D173 at-deposition AtDeposition { round: 3, own_committed: 2, own_applied: 2,
      tails: [2, 2, 3], leaders: [Some(NodeId(3)), Some(NodeId(3)), None] }
    D173 first-poll-after-release: tails=[3, 3, 3] (at deposition [2, 2, 3])
      leaders=[Some(NodeId(3)), Some(NodeId(3)), None]
    D173 survival: states=[Some(Merged { at: 3 }), Some(Merged { at: 3 }), Some(Merged { at: 3 })]
      heads=[4, 4, 4] tails=[4, 4, 4] leaders=[Some(NodeId(1)), Some(NodeId(1)), Some(NodeId(1))]
    D173 retry-error merge error: round 6 merges n3/b1, which is Merged { at: 3 }
    test result: ok. 37 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 7.79s

Line by line:

1. **The caller got `NotLeader`** 127 ms after calling `merge`, from a real lease death.
2. ⭐ **`own_applied: 2` and `own_committed: 2`, both BELOW the merge round 3.** This is the first
   adversary's objection, measured and dismissed: this node had **not** learned the commit, so
   `pump_until`'s `done` check did **not** beat the leadership check. The observation is taken
   inside the seam, at the very `leader()` call `pump_until` makes — not reconstructed afterwards.
3. `tails: [2, 2, 3]` — at the instant of deposition the entry was on **the leader's log alone**.
4. The retry is refused with **exactly the CI signature**, round gap 3 (rounds: 1 = n3's NoOp,
   2 = fork, 3 = merge #1, 4 = n1's NoOp after it won the election, 5 = n3's NoOp after it took
   office back, 6 = merge #2). CI sample 1 had gap 3, sample 2 gap 2.
5. The last assertion in the test — not printed because it passes — is that `db.main_qty(1)` is
   unchanged: **the merge the cluster committed was never published, and the rows are nowhere.**

**Reproductions: 8 of 8** (`run2`, `target_rep1..7`). Two different leader nodes drew the short
straw across those runs — `n3/b1` and `n1/b1` — which matches the CI's own n3/n1 pair and is what a
generic window predicts. Whole target green every time: 37 passed, 0 failed.

⚠ **Full disclosure on the ninth banked file.** `run1` is an EARLIER harness that did not yet have
`regain_leader`. Its experiment reached the deposition and the `NotLeader` exactly as above
(`own_applied: 2 < round: 3`, `tails: [2, 2, 3]`) and then failed in the *recovery* stage — that
failure is the 354 168 ms measurement quoted in §3c and §6.2. It is banked unedited. It is not a
failed reproduction of the window, and I am not counting it as a successful one either.

---

## 3. ⭐ EXIT ITEM (2) — THE DISCRIMINATOR: **THE RETRY ARM**, and it cannot be otherwise

### 3a. First, a correction to the premise

`d173_output_commit.md` says `fleet.hold_leader(leader)` "appears **twice**" in
`a_hundred_agent_writes_across_three_branches_leave_only_forks_and_merges_in_the_log`. It appears
**three** times. On `main` (`4912302`), `grep -n "hold_leader(leader)"`:

    1442  followers-log/pre-fork      1539  hundred-writes/pre-fork
    1450  followers-log/pre-merge     1549  hundred-writes/pre-merge
    1482  followers-log/retry-arm     1576  hundred-writes/retry-arm

and the sibling test `a_followers_committed_log_holds_the_merge_and_not_one_agent_row` has the same
three. Six sites, two tests. Both tests can produce the panic; the CI string named branch 2 / branch
3, so it was `hundred-writes`. All six are now labelled in my tree — the `LOOPCOUNT` line carries
`site=<test>/<role>`.

### 3b. The answer is structurally forced

The panic string `round N merges X, which is Merged { at: M }` comes from `merge_verdict`'s
not-live arm (`cluster.rs:609`, `fn merge_verdict` at `:595`) reaching `Err(e) => panic!("branch {row}: {e}")`. Therefore:

- the ledger already held `Merged { at: M }` when the retry proposed, so **a merge of that branch
  had already committed**;
- the branch is forked fresh in that loop iteration, so the only candidate is **attempt 0 of the
  same loop**;
- attempt 0 did not return `Ok` (the loop would have broken), and it did not return a non-`NotLeader`
  error (it would have panicked then), so **it returned `NotLeader` and went through the retry arm**;
- the retry arm's `hold_leader` is the **last statement before control returns to `agents.merge`**.
  No other `hold_leader` can run between it and the panic.

⇒ **The `turns=14 ms=6467` line is `hundred-writes/retry-arm`.** The alternative reading — that the
slow line was `pre-merge` — requires a retry-arm line to have printed between it and the panic, and
the D173 row's own timeline places the panic 220 ms after the slow line with nothing in between.

### 3c. And it is corroborated by measurement, not only by argument

Across 3 full-target runs I collected **36 labelled `hold_leader` observations** (12 per run).
Every single one is a `pre-fork` or `pre-merge` site, and every single one reads
`turns=0 bound=100000 ms=0..4` — `turns=0` is the *only* value that occurs, across all 7 runs.
**The retry-arm sites printed zero times** — on a quiet machine the retry arm never runs at all.

That asymmetry is not an accident of load. The `pre-*` sites run when leadership is already stable,
so they cost one turn. The retry-arm site is the only one that runs *immediately after a
`NotLeader`*, i.e. the only one that must pay for an election. For comparison, on this machine:

| what | turns | ms |
|---|---|---|
| `pre-fork` / `pre-merge`, leadership stable (36 obs) | 0 | 0–4 |
| taking office back after a **real** deposition, with a forced head start (8 obs) | 65–80 | 698–736 |
| `hold_leader` after a real deposition, **no** head start (1 obs) | 100000 (bound) | 354168 |
| CI, the line in question | 14 | 6467 |

⚠ **`turns` is not comparable across machines and the D173 row should stop treating it as if it
were.** `Node::poll` catches up *whole missed ticks* per call, so on a runner at 460 ms/turn one
`pump_all` advances ~23 ticks. CI's `turns=14` is ~320 ticks of protocol time — far more than my
65–80 turns at ~1 tick each. The `turns < bound` conclusion (converged, not a D73 stall) still
holds; any reading of `turns` as "fast" does not.

---

## 4. ⭐⭐ EXIT ITEM (3) — NO, AND THE WINDOW IS **WIDER** THAN THE ROW CLAIMED

**Losing leadership does not truncate the proposed entry, and the entry does not even have to have
reached a quorum first.**

Measured, one poll of each node immediately after the starvation is lifted and **before any
election has happened** (`leaders` still `[Some(N3), Some(N3), None]`):

    tails=[2, 2, 3]   at the deposition — the entry is on the ex-leader ALONE
    tails=[3, 3, 3]   after ONE poll each

The mechanism, and it is read from the source not guessed: `Node::propose` (`node.rs:423`) pushes
`Event::Propose` and calls `self.drain()?` **inline**, so the `Append` carrying the entry is written
to the peers' sockets before `propose` returns — before the lease dies, before anything is voted on.
TCP delivers it ahead of every later message, so each peer appends the entry on its **first** poll,
which is also the message that refreshes its election timer. By the time anyone can campaign, the
entry is on all three logs, so §5.4.1's election restriction (`election.rs:535`,
`log_is_at_least_as_complete`, compared `(term, round)` term-first) leaves no electable leader
without it, and §5.4.2 (`replicate.rs:867`, `advance_commit`, which refuses to commit an inherited
round directly) commits it as a side effect of the new leader's own `NoOp`. Observed: `heads`
`[4,4,4]`, all three ledgers `Merged { at: 3 }`.

⇒ ⭐ **The first adversary's last words were right about the ORDER and wrong about the CONSEQUENCE.**
`pump_until` is indeed `(leader check) -> (pump) -> (done check)`, and `done` does beat the
leadership check when this node learns the commit. But the window does **not** require the entry to
reach a quorum before the lease dies — it requires only that the entry has been **sent**, and
`propose` sends it synchronously. There is no interval between "the entry is in the log" and "the
entry is on its way to a quorum" for the window to have to thread. **`d173_output_commit.md`'s
narrowing paragraph should be struck**; I would have believed it, and it is false.

⛔ **What this does NOT say: the survival is a race, not a guarantee, and that is the finding, not a
caveat.** Had the proposer died for good with the `Append` still unread, a peer pair could have won
on `(T, N-1)` and truncated the entry — the machinery for that is `replicate.rs:1042`
(`truncate_from`), covered green by
`tests_replicate.rs:628 a_follower_truncates_its_conflicting_suffix_and_the_leader_recovers_in_one_extra_message`
and `:669 a_truncation_retracts_the_durability_it_was_about`. **Committed-or-truncated is genuinely
undetermined at the moment `NotLeader` is returned, and the caller is handed a value whose type says
"refused".** That *is* output commit, exactly.

---

## 5. THE MUTATION — WHICH LINE PRODUCES THE `NotLeader`

A test nobody has watched fail is not evidence, and a `NotLeader` has four possible producers on
this path (`require_leader` at 1243, the pre-propose `pump_until`, `NodeReplicator::propose`'s
refusal, and `pump_until` at 1263).

Mutant M1′, applied to `src/agent_sql/cluster.rs` and restored afterwards with
`git restore --source=HEAD --worktree` (`git checkout --` restores the index and has eaten a fix on
this project): keep the `self.repl.leader()` call inside `pump_until` — so the harness still observes
the deposition — and delete only the refusal. Result
(`bench/d173_adversary2/mutant_m1.txt`, rc=101):

    D173 attempt1 took ms=143252 result=Err("merge error: round 3, the merge of n1/b1 was not
      applied within 100000 turns of the consensus driver. Refusing rather than blocking a client
      for ever; nothing was published")
    D173 at-deposition AtDeposition { round: 3, own_committed: 2, own_applied: 2,
      tails: [3, 2, 2], leaders: [None, Some(NodeId(1)), Some(NodeId(1))] }
    panicked at tests/integration_cluster_agents.rs:1862:5:
      the caller must see the transient-by-contract error, not something else: merge error: ...
    test result: FAILED. 0 passed; 1 failed

**The same deposition, a different exit.** `cluster.rs:1263` is the sole producer of the `NotLeader`
the caller saw. Mutant confirmed absent after restore (`grep -c "MUTANT M1"` → 0, tree clean).

---

## 6. WHAT I COULD NOT MAKE FIRE

1. ⛔ **I could not force the TRUNCATING arm by measurement.** To put the entry on the proposer
   alone *and keep it there* I would have to stop the `Append` reaching the peers' sockets, and
   `propose` writes it there synchronously over a real TCP transport with no drop knob. The arm is
   argued from source plus the existing green consensus tests named in §4 — **it is a citation, not
   a measurement I made**, and exit item (3) is therefore answered one-sided: I proved survival, I
   did not prove truncability here.
2. ⛔ **I did not reproduce the panic through the real failing test's own `hold_leader` retry arm.**
   After a genuine deposition `hold_leader(original)` cannot recover — measured: 100 000 turns /
   354 168 ms with `leaders = [Some(N1), Some(N1), Some(N1))]`, because the ex-leader becomes a
   healthy follower and never campaigns again. My harness therefore uses a new
   `Fleet::regain_leader`, which starves whoever holds office until the ex-leader can take it back.
   CI recovers with plain `hold_leader` because *every* driver is slow at once there; mine starves
   only the peers. **So §3's answer rests on the structural argument plus the 36-observation
   asymmetry, not on a log I generated with the literal `hold_leader` line.**
3. ⚠ **The harness is timing-sensitive by construction** (it races a 160 ms lease against a real
   scheduler). 8 for 8 on a quiet machine; I did not run it under load, and no fleet measure lock
   was held or exists in this repo.

---

## 7. WHAT LANDS WHERE

Everything is on `d173-adversary-2` and **must not go to main as-is** — `StarvePeersOnMerge`,
`regain_leader` and the two `d173_*` tests are an adversary's instruments. The one piece that
arguably *should* land on its own is the **`site=` label on `hold_leader`'s `LOOPCOUNT` line**: it
is three tokens, it weakens nothing, and its absence is precisely what made this question need an
adversary. Six call sites, `tests/integration_cluster_agents.rs`.

⛔ The repair to `merge` itself is still not proposed here, and the ledger's `⛔ WHAT NOT TO DO`
section stands unchanged: do not widen the retry arm, do not weaken an assertion. What this run adds
to that design decision is that the in-doubt state is **reachable in 127 ms on a three-node cluster
with one starved driver**, not narrow.
