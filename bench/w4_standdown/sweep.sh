#!/bin/sh
# W4 check 3 — the announcer-mix sweep.
#
# One stand-down fraction at one client mix cannot tell a property of the DESIGN from a property
# of the mix that produced it. This sweeps the number of announcer clients (forkers) against a
# fixed total, so the reported quantity is a curve.
#
# Runs are SERIAL on purpose. The instrument is an integer and load-immune, but the quantity it
# measures is a contention probability, and two sweep points running at once would be measuring
# each other. The zero-announcer point is the `readonly` arm, which the main run already has.
#
# Usage: bench/w4_standdown/sweep.sh <out-dir>
set -eu
OUT="${1:?usage: sweep.sh <out-dir>}"
BIN=./target/release/examples/w4_standdown_count
[ -x "$BIN" ] || { echo "no $BIN -- build with --features w4-standdown-count" >&2; exit 1; }
mkdir -p "$OUT"
for n in 1 2 4 6 7; do
    echo "== announcers=$n of 8 =="
    W4_CLIENTS=8 W4_ROUNDS=400 W4_ANNOUNCERS="$n" W4_ARMS=agent \
        timeout 900 "$BIN" > "$OUT/announcers_$n.txt" 2> "$OUT/announcers_$n.err" || {
            echo "REFUSED at announcers=$n:" >&2; cat "$OUT/announcers_$n.err" >&2; exit 1; }
done
echo "sweep complete"
