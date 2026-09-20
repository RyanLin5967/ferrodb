#!/usr/bin/env bash
# bench/d42_load_sweep.sh — ⛔ GUTTED ON PURPOSE. THIS SCRIPT IS A RECORD, NOT A TOOL.
#
# It swept six machine-wide load shapes (up to NCPU x 28 = 504 CPU burners plus 128 fsync loops) to
# find which one starves `integration_consensus_failover.rs`. It is gutted for the same reason as
# `bench/d42_fire_starved.sh`, which shares its burner shape: that shape froze this shared machine
# twice and locked its owner out three times in one day. `bench/` is not a cargo target, so removing
# the body is inert for the suite.
#
# ── THE FINDING, WHICH IS THE MOST USEFUL THING D42 PRODUCED ───────────────────────────────────
#
# Raw data in `bench/d42_load_sweep.txt`. Both of D42's signals, measured by the same probe that
# calibrated their thresholds (`bench/d42_childcpu_probe.py`), across rising load:
#
#   shape                     loadavg   self-schedule   child CPU/node-wall-s
#   baseline                    720        0.6136             0.0037
#   cpu-14x                     810        0.5873             0.0038
#   io-64                       575        0.7582             0.0047
#   cpu-14x + io-64             777        0.5496             0.0031
#   nice20 + cpu-14x            737        0.3358             0.0021
#   nice20 + cpu-28x            906        REFUSED — no leader elected within 45 s
#   nice20 + cpu-28x + io-64    903        REFUSED — no leader elected within 45 s
#
# Two things follow, and both corrected the design entry:
#
#   1. **Ambient CPU load is not a lever on this machine.** Both signals stay nearly flat from
#      loadavg 269 to 829 — self-scheduling falls only 0.78 -> 0.55, nowhere near its 0.085
#      threshold. macOS's timeshare scheduler demotes pure spinners and keeps a mostly-sleeping
#      process responsive. The pre-registered "14x oversubscription" falsifier therefore cannot
#      reach its own precondition: no wait ever expires, so the classifier never runs.
#
#   2. ⭐ **Past a certain load the arm is worse than useless.** At cpu-28x no leader is elected at
#      all, so there is no healthy floor to calibrate against and the probe REFUSES in both
#      directions. Starving EVERYTHING fires both signals at once, which proves nothing about their
#      composition — and composition was the one property the two-signal design needed shown. The
#      arm would have "passed" while testing the wrong thing.
#
# ⇒ The falsifier that works is `bench/d42_fire_starved_children.sh`: SIGSTOP the consensus_node
#   CHILD processes, leave the test thread scheduled. Targeted, no machine-wide load, nothing to
#   clean up, and it isolates signal C firing ALONE while B stays healthy — the exact blind spot
#   the design named.
#
# ── WHY NOT JUST FIX THE BURNERS ───────────────────────────────────────────────────────────────
#
# Because the fix has a hole that opens under exactly the conditions this script creates. A
# "self-terminating" burner tests its deadline only BETWEEN passes of a non-interruptible inner
# loop, and that pass gets slower as the box gets more contended — so the overshoot is worst
# precisely when the bound matters. Measured by another session: burners at 05:15 elapsed against a
# 240 s deadline. And a SIGSTOPped burner never reaches its deadline check at all, so suspending
# them is not a safe way to park them either.
#
# If anything like this is ever rebuilt: inner loop of 2000, not 200000, so the deadline check
# fires often enough to mean something; kill by a unique TAG in the command line, never by a
# recorded pid; and fire-check the cleanup by killing the PARENT and confirming nothing is left at
# ppid=1 — `bench/d42_fire_orphans.sh` does exactly that.
#
# Full account: `bench/d42_DECISION.md` §4b and §10.

cat >&2 <<'EOS'
REFUSING — bench/d42_load_sweep.sh has been gutted deliberately and will not run.

It generated machine-wide CPU and fsync load. That shape froze this shared box twice and
locked its owner out three times. Its finding is already recorded in bench/d42_load_sweep.txt
and bench/d42_DECISION.md §4b, and re-running it would add nothing.

bench/d42_childcpu_probe.py still works and is the calibration instrument; it spawns a
3-node cluster and nothing else. bench/d42_fire_starved_children.sh is the falsifier that
actually fires, using SIGSTOP on named pids rather than machine-wide load.
EOS
exit 4
