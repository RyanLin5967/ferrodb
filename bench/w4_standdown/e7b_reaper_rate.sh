#!/bin/sh
# E7b — E7 CORRECTED. E7 fired the detector but did NOT drive the axis, and the reason was a
# SCALE ERROR IN MY ARM, not anything about the engine.
#
# E7 ran readonly/extended at 200 rounds. That arm is 1600 cached-prepared reads served in-process
# over loopback and it finishes almost instantly: `minted` came back 1, 1, 2, 6 against expiry
# cadences of 200/50/10/2 ms, and solving those four points for a common window gives
# fork ≈ 2 ms and a MEASURED WINDOW OF ≈ 24 ms. A 10 ms scan timer fires about twice in 24 ms, so
# ann_lease could never have exceeded a handful however fast branches expired.
#
# ⇒ The fix is not a faster knob, it is a LONGER WINDOW. Rounds are raised ~250x so the measured
# window is seconds rather than milliseconds, and the scan interval drops to 1 ms so the reaper
# gets thousands of opportunities instead of two.
#
# expiry_ms=0 remains the control and must still read EXACTLY 0.000000 with ann_lease=0: a 1 ms
# scan cadence with nothing expired must announce nothing, which is `lease_thread.rs:66`'s claim
# tested rather than read.
set -eu
OUT="${1:?usage: e7b_reaper_rate.sh <out-dir>}"
BIN=./target/release/examples/w4_standdown_count
[ -x "$BIN" ] || { echo "REFUSED: no $BIN" >&2; exit 1; }
mkdir -p "$OUT"
for ms in 0 32 8 2 1; do
    echo "== e7b_expiry_$ms =="
    W4_CLIENTS=8 W4_ROUNDS=50000 W4_ARMS=readonly W4_PROTOS=extended \
        W4_LEASE_MS=1 W4_EXPIRY_MS="$ms" \
        timeout 1800 "$BIN" > "$OUT/e7b_expiry_$ms.txt" 2> "$OUT/e7b_expiry_$ms.err" \
        || { echo "  REFUSED:" >&2; head -3 "$OUT/e7b_expiry_$ms.err" >&2; }
done
echo "E7b complete"
