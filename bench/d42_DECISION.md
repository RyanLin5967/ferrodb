# D42 — a 45 s expiry now reports WHETHER it is a verdict

Status: built, both falsifier directions forced, committed. Every number below names the file it
came from. Design entry: `SCALE-DESIGN.md` D42 in the artie-research repo.

**Three of this record's findings CORRECT that design entry.** They are in §4, and two of them
invert claims the entry treats as settled. Read §4 before quoting the design.

## 1. The problem

`tests/integration_consensus_failover.rs` waits on a 45 s wall-clock budget. That budget has
expired four times under agent-fleet load — `suite-d19-merge-0245Z`, `suite-E79c-0457Z`,
`suite-E79c-1036Z`, D40 run 1 — on a test that passes alone in ~3.1 s. Each cost a re-run and twice
nearly cost a wrong diagnosis. A wall-clock deadline cannot distinguish "the cluster failed" from
"this process was descheduled"; it measures elapsed time either way.

⛔ `ELECTION_BUDGET` is unchanged at 45 s and must stay so. Raising it is "never edit a test to make
it pass" wearing a constant, and 45 s is already 15x the observed need. What changed is what an
expiry is allowed to MEAN.

## 2. What was built

At expiry the test reads two signals over the second half of the budget and classifies:

| signal | what it measures | threshold | calibrated from |
|---|---|---|---|
| B — self-scheduling | iterations of the poll loop / nominal. `pump` uses `try_recv` and never blocks, so this loop is pure sleep and its count is a direct in-process measure of whether THIS THREAD ran. | starved below **0.085** | a decade below the measured floor 0.849 |
| C — child CPU | CPU-seconds consumed by the node child processes per live node per wall second. | starved below **0.00043** | a decade below the measured floor 0.0043 |

Both floors are from `bench/d42_childcpu_probe.txt`, measured in one window on an idle 3-node
cluster BEFORE either falsifier ran. An idle cluster is the correct lower bound because a broken one
burns strictly more — confirmed in §5, where the broken cluster's survivor read 0.00755, 1.8x the
idle floor.

Either signal reading STARVED yields INCONCLUSIVE; otherwise FAILED, with the original panic text
preserved. A third state, CLASSIFIER-BROKEN, covers the instrument failing on a platform where it
should work — it refuses rather than silently degrading to one signal.

All five wait sites are covered: the four `wait_for` call sites and the hand-rolled loop before the
data-loss assertion. That fifth one matters most — a starved expiry there reports acknowledged DATA
LOSS, the property the whole of Phase F exists to buy, and a fix covering only the helper would have
left the worst false RED in place.

The verdict rides the refusal channel `tools/verify-suite.sh` already owns ("a run that collected
nothing has not passed"). It exits 4, prints no total, writes no `SUMMARY.txt`, and writes
`INCONCLUSIVE.txt`, which `tools/certify-head.sh` refuses to land. **Strictly more conservative than
the red it replaces** — it cannot turn a failure into a pass, only into a non-verdict.

## 3. Where the classifier is deliberately not the answer

An injectable logical clock in the nodes deletes the ambiguous outcome instead of classifying it,
and is the right long-term shape (FoundationDB, TigerBeetle, `madsim`). It is `SCALE-DESIGN.md` D42
option D and is not taken here because it changes `src/`'s consensus timers to fix a harness problem.

## 4. Corrections to the design entry

### 4a. Option C's premise is wrong by four orders of magnitude

The entry says a healthy cluster shows "~40 s of child CPU over 45 s of wall". Measured
(`bench/d42_childcpu_probe.txt`): three idle nodes consume **0.58 CPU-seconds over 45.21 s across
three nodes** — 0.4 % of a core each, not 90 %. These nodes block in `recv_timeout(min(5ms,
until_tick))` (`src/consensus/node.rs:436`); they do not spin.

The DIRECTION option C rests on survives, and §5 confirms it. The magnitude does not, and a
threshold taken from the design's number would have called every run on this machine starved.

### 4b. The pre-registered falsifier shape does not reproduce on this machine

D42 pre-registered "14x oversubscription, require INCONCLUSIVE". Three attempts:

| attempt | shape | result |
|---|---|---|
| `d42_fire_starved.txt` run 1 | 252 spinners, loadavg 137 | test **PASSED** in 4.76 s |
| `d42_fire_starved.txt` run 2 | `nice -n 20` + 504 spinners, loadavg 254 | test **PASSED** in 8.12 s |
| `d42_load_sweep.txt` | six shapes, loadavg 269 → 829 | both signals FLAT; self-scheduling fell only 0.78 → 0.55 |

Ambient CPU load is not a lever here. macOS's timeshare scheduler demotes pure spinners, and a
process asking for 0.4 % of a core keeps getting it however many are queued. **The precondition the
falsifier needs — a 45 s budget actually expiring — was never reached, so the classifier never ran.**
That is a failed reproduction, not a failed detector, and the distinction is the whole reason these
negative runs are kept rather than deleted.

The load shape was amended; **the exit criterion was not weakened**. See §5.

### 4c. The design's weighting of its own two signals is inverted for this target

The entry presents B and C as co-equal, each covering the other's blind spot. Measured, for THIS
test, **starving the test thread cannot produce a timeout at all.** The loop is
`pump; pred; deadline; sleep`, `pump` drains an mpsc queue fed by reader threads, and the predicate
is evaluated over the ACCUMULATED transcript. A descheduled thread simply resumes, drains everything
the nodes said while it was away, and finds the predicate already satisfied — it PASSES. The budget
can only expire if the NODES failed to progress.

So signal B is a cheap in-process backstop, and signal C — introduced only to cover B's blind spot —
is carrying this row. B is retained: it costs nothing, it is the only signal available if `ps` is
missing, and the asymmetry is a property of this test's structure rather than of the mechanism.

## 5. Both falsifier directions, forced

| | falsifier 1 — must fire | falsifier 2 — must NOT fire |
|---|---|---|
| file | `d42_fire_starved_children.txt` | `d42_fire_broken.txt` |
| setup | nodes SIGSTOPped for 60 s, test thread left scheduled | second node killed: 1 of 3 alive, quorum unreachable, quiet machine |
| self-schedule | 1887 / 2251 = **0.8384 HEALTHY** | 1882 / 2251 = **0.8364 HEALTHY** |
| child CPU | **0.00000 STARVED** | **0.00755 HEALTHY** |
| verdict | **INCONCLUSIVE** | **FAILED**, at 45 s, original panic text unchanged |
| test result | FAILED in 45.38 s | FAILED in 47.54 s |

**Signal C fired ALONE in falsifier 1 while B read healthy.** That is the blind-spot coverage claim
of §2, measured rather than argued, and it is the single most load-bearing result here.

The two child-CPU readings are 0.00000 and 0.00755 against a 0.00043 threshold: the threshold sits
17.5x below the broken case and above zero. The pre-registered kill condition — a genuinely broken
cluster earning INCONCLUSIVE — did not occur.

Falsifier 1 is aimed with SIGSTOP rather than ambient load. That is not a weaker check: the
classifier's claim is exactly "this process, or its children, did not get the CPU", and SIGSTOP puts
the system in that state deterministically instead of hoping load wanders into it. From every
observable the classifier has, a stopped child and a starved child are identical.

Anti-vacuity is enforced inside the script: the keeper logs which pids it stopped, and the run
refuses as VACUOUS if it stopped none. **This fired for real** — an early version waited a fixed
2.5 s for READY and, on a quiet machine, the whole test finished in 3.15 s inside that wait. The
keeper stopped nothing and the run would have read as "the classifier did not fire". The guard
caught it; the trigger is now a condition (all three nodes LISTENing) rather than a timer.

## 6. The unloaded path is unchanged

`d42_happy_path.txt`: 3/3 pass in 3.89 / 5.13 / 5.59 s at loadavg ~13.7, no new output. The
classifier takes no `ps` sample and prints nothing unless a wait passes the halfway mark of its
budget, so a healthy ~3 s run never touches it.

## 7. Named blind spots

* **C's resolution.** `ps` reports CPU to centiseconds, so over a 45 s window with two live nodes
  the threshold sits about four `ps` ticks above zero. C fires only when children got very nearly no
  CPU. That is conservative in the safe direction — it will not manufacture an INCONCLUSIVE for a
  healthy-but-failing cluster — at the cost of missing MODERATE child starvation.
* **B on this target** is near-vestigial, per §4c.
* **Non-unix.** `ps` is unavailable, so C cannot speak and the verdict rests on B alone. The message
  says so explicitly rather than degrading silently.
* **Neither signal detects a starved run that still PASSES.** Nothing here makes a passing run more
  trustworthy; it only stops a starved expiry from being recorded as a failure.
* **The fleet-load mechanism itself is still uncharacterised.** §4b shows CPU oversubscription is
  not it. The four historical false REDs happened under concurrent suites, which add memory pressure
  and fork storms; memory pressure was deliberately not swept, because inducing swap on a box shared
  with an agent fleet risks OOM-killing another agent's multi-hour run. So this row proves the
  classifier separates starved from broken, and does NOT prove that fleet load produces exactly the
  starvation shape it detects.

## 8. Guards fire-checked

Every detector added by this row has been forced to fire. The list is here because an unfired
detector is indistinguishable from one that has quietly stopped existing.

| detector | forced by | required outcome |
|---|---|---|
| signal C (child starvation) | `d42_fire_starved_children.sh` | INCONCLUSIVE |
| the classifier NOT misfiring | `d42_fire_broken.sh` | FAILED, two-signal form |
| CLASSIFIER-BROKEN | same script, `BREAK_PS=1` — a really-failing `ps` first on PATH | a REFUSAL, not a verdict |
| verify-suite.sh's marker detection | `d42_fire_channel.sh` end to end | exit 4, no total, no `SUMMARY.txt` |
| marker drift Rust ↔ shell | `verify-suite-selftest.sh` part 3 | both literals present |
| `certify-head.sh`, BOTH directions | `verify-suite-selftest.sh` part 3 | refuses INCONCLUSIVE, still certifies an honest green |

The `certify-head` check runs in both directions deliberately: checking only that it refuses would
pass equally for a guard that refuses unconditionally.

`BREAK_PS` breaks the instrument rather than setting a flag that tells the code to pretend the
instrument is broken. The distinction matters — a flag would exercise a branch, not the failure.

## 9. Running it

`bench/d42_verify_all.sh <throwaway-worktree>` runs arms 1–5 under ONE acquisition of the
machine-wide suite lock. Per-arm locking was the first shape and it is wrong on a box running an
agent fleet: it queues behind another suite once per arm, and another suite can start BETWEEN two
arms, so the arms get measured against different machine states while being reported as one result.

Called without a throwaway worktree it reports arm 4 as SKIPPED and exits non-zero, rather than
reporting the must-fire half alone — which would be a detector nobody had tried to make misfire.

## 10. ⛔ An incident caused by this row's own load harness, and the fix

**The first version of `d42_fire_starved.sh` left 317 orphaned processes at ppid=1, still running
1.5 hours after their parent died, at loadavg 282. It froze this shared box twice and starved every
other project on it, while presenting as "the machine is slow".** It is recorded here because the
harness is committed and would otherwise do it again to the next person who runs it.

Three defects compounded, and the order matters:

1. the burner body was `while :` / `while True` — **unbounded**, so an abandoned one never stops;
2. cleanup did `kill -9 "$!"`, and `$!` is the **`timeout` WRAPPER's** pid. Killing `timeout` does
   not kill the `sh`/`python3` it spawned: the grandchild survives, reparents to init, and now has
   nothing enforcing its bound at all. **The cleanup created the orphans it existed to prevent;**
3. `disown` detached them explicitly, defeating even SIGHUP.

Fix (1) is the load-bearing one — a self-terminating burner needs no parent, no `timeout` and no
trap — so every burner now carries its own wall-clock deadline, checked between bounded bursts of
arithmetic. `timeout` and the trap are kept as belt-and-braces, cleanup kills the wrapper's
DESCENDANTS via `_kill_tree`, and `disown` is gone.

`bench/d42_fire_orphans.sh` forces the exact incident state rather than trusting the fix:
it SIGKILLs the parent (so no trap can run) **and** SIGKILLs the `timeout` wrappers (so no bound is
enforced), leaving bare burners at ppid=1. Both directions are required, because "they are all
gone" would also pass for burners that never started:

| direction | required | measured (`d42_fire_orphans.txt`) |
|---|---|---|
| A — anti-vacuity | orphans at ppid=1 must EXIST after the kills | **8** |
| B — the fix | none alive after their own deadline | **0** |

⚠ **This is also a caution about the project rule that produced it.** "Oversubscribe 14x to force a
guard that won't fire" is sound for an IN-PROCESS race window, which is what it was measured on. It
is the instruction most likely to spawn exactly these orphans, and — per §4b — it does not even work
for starving a process on this machine. Prefer SIGSTOP aimed at named pids; if external load really
is needed, the burner must be self-terminating before anything else about it is considered.
