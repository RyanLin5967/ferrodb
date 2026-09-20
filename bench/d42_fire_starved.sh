#!/usr/bin/env bash
# bench/d42_fire_starved.sh — ⛔ GUTTED ON PURPOSE. THIS SCRIPT IS A RECORD, NOT A TOOL.
#
# It used to generate machine-wide CPU load (NCPU x 28 = 504 burners on this box) to try to force
# D42's classifier to fire. **It froze this shared machine twice and locked its owner out three
# times in one day.** The body has been removed rather than the file, so the next person does not
# reinvent the same shape. `bench/` is not a cargo target, so removing it is inert for the suite.
#
# ── WHAT IT MEASURED, WHICH IS WHY THE FILE SURVIVES ───────────────────────────────────────────
#
# The pre-registered falsifier for D42 was "force INCONCLUSIVE under 14x oversubscription". That
# instrument does not work, and proving so is this script's entire contribution:
#
#   * 252 spinners (14x), loadavg 137          -> test PASSED in 4.76 s
#   * `nice -n 20` + 504 spinners (28x), lg 254 -> test PASSED in 8.02 s
#   * six shapes swept (bench/d42_load_sweep.txt), loadavg 269 -> 829: both signals nearly FLAT,
#     self-scheduling falling only 0.78 -> 0.55
#
# macOS's timeshare scheduler demotes pure CPU spinners and keeps a mostly-sleeping process
# responsive. A process asking for 0.4 % of a core keeps getting it however many spinners queue
# behind it. So the precondition the falsifier needs — a 45 s budget actually EXPIRING — was never
# reached and the classifier never ran at all.
#
# ⭐ AND AT HIGHER LOAD THE ARM IS WORSE THAN USELESS: at cpu-28x the sweep cannot elect a leader
# within 45 s, so there is no healthy floor to calibrate against and the arm REFUSES in both
# directions. It would have "passed" while testing the wrong thing — starving EVERYTHING fires both
# signals at once and therefore proves nothing about their composition, which is the one property
# the two-signal design needed demonstrated.
#
# ⇒ SUPERSEDED BY `bench/d42_fire_starved_children.sh`, which SIGSTOPs the consensus_node CHILD
#   processes and leaves the test thread scheduled. That is strictly better: targeted, no
#   machine-wide load, nothing to clean up, and it isolates signal C firing ALONE — the exact blind
#   spot the design named. It produced the INCONCLUSIVE half of the falsifier.
#
# ── WHY THE BODY IS GONE RATHER THAN FIXED ─────────────────────────────────────────────────────
#
# Four defects were found in it, and the fourth is the reason "fixed" is not a state this shape can
# reach cheaply:
#
#   1. the burner body was `while :` — UNBOUNDED, so an abandoned one never stops;
#   2. cleanup killed `$!`, the `timeout` WRAPPER's pid. Killing `timeout` does not kill the `sh`
#      it spawned; the grandchild survives, reparents to init, and nothing enforces its bound. The
#      cleanup created the orphans it existed to prevent. Result: 317 orphans at ppid=1, 1.5 hours
#      old, loadavg 282;
#   3. `disown` detached them, defeating even SIGHUP;
#   4. ⭐ **A SELF-TERMINATING BURNER IS ONLY BOUNDED TO WITHIN ONE INNER PASS, AND THAT TERM GROWS
#      WITH THE CONTENTION THE HARNESS ITSELF CREATES.** The deadline is tested BETWEEN passes of a
#      non-interruptible inner loop, so the overshoot is worst exactly when the bound matters.
#      Measured by another session: burners at **05:15 elapsed against a 240 s deadline**. A
#      SIGSTOPped burner is worse still — it never reaches its own deadline check at all, so
#      suspending them is not a safe way to park them either.
#
# If anything like this is ever built again: the inner loop must be SMALL (2000 iterations, not
# 200000) so the deadline check fires often enough to mean something; kill by a unique TAG in the
# command line rather than by a recorded pid; and fire-check the cleanup by killing the PARENT and
# confirming nothing sits at ppid=1 (`bench/d42_fire_orphans.sh` does this).
#
# The full account is in `bench/d42_DECISION.md` §10. The raw negative result this script produced
# is preserved in `bench/d42_fire_starved.txt`.

cat >&2 <<'EOS'
REFUSING — bench/d42_fire_starved.sh has been gutted deliberately and will not run.

It generated machine-wide CPU load, froze this shared box twice, and locked its owner out
three times. Its finding is already recorded in bench/d42_fire_starved.txt and
bench/d42_DECISION.md, and re-running it would add nothing: at 28x oversubscription the
cluster cannot elect a leader at all, so the arm cannot yield a verdict in either direction.

Use bench/d42_fire_starved_children.sh instead. It SIGSTOPs the node child processes and
leaves the test thread scheduled — targeted, no machine-wide load, and it is what actually
produced the INCONCLUSIVE half of the falsifier.
EOS
exit 4
