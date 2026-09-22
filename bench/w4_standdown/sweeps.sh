#!/bin/sh
# W4 check 3 — the two sweeps and the reaper fire-check.
#
# One stand-down fraction at one client mix cannot tell a property of the DESIGN from a property
# of the mix that produced it, so the answer is delivered as curves over two axes:
#
#   announcers  — how many of the 8 clients run exclusive statements at all.
#   fork_every  — how often an announcer client actually forks, the rest of its rounds being
#                 reads. This is the OFFERED-LOAD axis and it is deliberately a STATEMENT-COUNT
#                 ratio, not a sleep: a sleep would make the answer depend on wall-clock latencies
#                 and therefore on what else is running on this box.
#
# Plus a fire-check the numbers demand: every main-run row read `ann_lease=0`, i.e. the reaper
# announced nothing at the shipped 30s cadence. A detector that found nothing is not a clean
# result until it has been forced to fire, so the last run drops the lease interval to 50ms and
# the LeaseScan tag must become non-zero.
#
# `extended` only: it is what asyncpg and pg8000 do, and the artifact's section 5 has already
# settled why the other two protocol rows differ.
#
# SERIAL on purpose — two points running at once would be measuring each other.
#
# Usage: bench/w4_standdown/sweeps.sh <out-dir>
set -eu
OUT="${1:?usage: sweeps.sh <out-dir>}"
BIN=./target/release/examples/w4_standdown_count
[ -x "$BIN" ] || { echo "no $BIN -- build with --features w4-standdown-count" >&2; exit 1; }
mkdir -p "$OUT"

for n in 1 2 4 6 7; do
    echo "== announcers=$n of 8 =="
    W4_CLIENTS=8 W4_ROUNDS=200 W4_ANNOUNCERS="$n" W4_ARMS=agent W4_PROTOS=extended \
        timeout 900 "$BIN" > "$OUT/announcers_$n.txt" 2> "$OUT/announcers_$n.err" \
        || { echo "  REFUSED:" >&2; head -2 "$OUT/announcers_$n.err" >&2; }
done

for k in 1 2 4 8 16 32; do
    echo "== fork_every=$k =="
    W4_CLIENTS=8 W4_ROUNDS=200 W4_FORK_EVERY="$k" W4_ARMS=agent W4_PROTOS=extended \
        timeout 900 "$BIN" > "$OUT/fork_every_$k.txt" 2> "$OUT/fork_every_$k.err" \
        || { echo "  REFUSED:" >&2; head -2 "$OUT/fork_every_$k.err" >&2; }
done

echo "== reaper fire-check: lease scan every 50ms =="
W4_CLIENTS=8 W4_ROUNDS=200 W4_LEASE_MS=50 W4_ARMS=readonly W4_PROTOS=extended \
    timeout 900 "$BIN" > "$OUT/lease_50ms.txt" 2> "$OUT/lease_50ms.err" \
    || { echo "  REFUSED:" >&2; head -2 "$OUT/lease_50ms.err" >&2; }
echo "sweeps complete"
