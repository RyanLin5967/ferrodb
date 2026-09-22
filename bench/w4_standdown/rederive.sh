#!/bin/sh
# W4 check 3 — the RE-DERIVATION and the adversarial checks on the instrument.
#
# Five campaigns, each into its own file, SERIAL on purpose: the quantity is a contention
# probability, so two campaigns running at once would be measuring each other.
#
#   E1  reproduce   — the five main arms at the banked configuration, byte-for-byte the same env.
#   E2  ext_split   — the fourth protocol row, which splits "did the client re-parse?" from
#                     "how long ago did it release the catalog?".
#   E3  self        — ONE client. Any stand-down here is self-contention by construction.
#   E4  calibrate   — the same arm and protocol driven to both ends by one knob (reaper cadence).
#   E6  offered     — the fork-rate axis, because one point cannot tell a property of the design
#                     from a property of the mix that produced it.
#
# (E5, load sensitivity, is driven by rederive_load.sh because it needs a burner beside it.)
#
# Usage: bench/w4_standdown/rederive.sh <out-dir> [campaigns...]
set -eu
OUT="${1:?usage: rederive.sh <out-dir> [campaigns...]}"
shift || true
WANT="${*:-E1 E2 E3 E4 E6}"
BIN=./target/release/examples/w4_standdown_count
mkdir -p "$OUT"
rc=0

# The binary is re-checked before EVERY point, not once at the top: a previous run of this
# experiment lost three arms to another process on this box deleting `target/` midway through, and
# the failure arrived as a message from `timeout` rather than from anything that knew what the run
# was for.
run() {
    name=$1; shift
    [ -x "$BIN" ] || { echo "REFUSED: $BIN vanished before $name" >&2; exit 1; }
    echo "== $name =="
    env "$@" timeout 1800 "$BIN" > "$OUT/$name.txt" 2> "$OUT/$name.err" || {
        echo "  REFUSED (rows completed before the refusal are kept):" >&2
        head -3 "$OUT/$name.err" >&2
        rc=1
    }
}

has() { case " $WANT " in *" $1 "*) return 0;; *) return 1;; esac; }

# ---- E1: reproduce the banked table, at the banked configuration -------------------------------
if has E1; then
    for arm in readonly agent agent_dml merge ddl; do
        run "e1_arm_$arm" W4_CLIENTS=8 W4_ROUNDS=400 W4_ARMS="$arm"
    done
fi

# ---- E2: the fourth protocol row --------------------------------------------------------------
# `agent` is the deciding arm, so the discriminator is run there. All four rows in ONE process so
# they share a build, a box and a minute; they are still serial arms inside it.
if has E2; then
    run e2_protoladder W4_CLIENTS=8 W4_ROUNDS=400 W4_ARMS=agent \
        W4_PROTOS=simple,ext_reparse,ext_split,extended
    run e2_protoladder_readonly W4_CLIENTS=8 W4_ROUNDS=400 W4_ARMS=readonly \
        W4_PROTOS=simple,ext_reparse,ext_split,extended
fi

# ---- E3: does a connection stand down against its OWN announcement? ----------------------------
# ONE client, so there is no second connection to contend with. The lease scan is pushed out past
# the arm's life so the reaper cannot be the announcer either; what remains is this connection and
# nothing else.
#
#   e3_self_reader   — a pure reader under `simple`, which takes the exclusive catalog for its own
#                      `Parse` IMMEDIATELY BEFORE its own `begin_read`. This is the sharpest case:
#                      if the parse guard outlived `parse_one` the fraction would be 1.00.
#   e3_self_forker   — one connection alternating a fork (announce) with a read, so the
#                      announcement and the attempt are the same connection one statement apart.
if has E3; then
    run e3_self_reader W4_CLIENTS=1 W4_ROUNDS=400 W4_ARMS=readonly W4_LEASE_MS=3600000 \
        W4_PROTOS=simple,ext_reparse,ext_split,extended
    run e3_self_forker W4_CLIENTS=1 W4_ROUNDS=400 W4_ARMS=agent W4_ANNOUNCERS=1 \
        W4_FORK_EVERY=2 W4_LEASE_MS=3600000 W4_PROTOS=simple,ext_reparse,ext_split,extended
    # The control for e3: the SAME single-client config with a second client added. If e3 reads
    # zero and this reads non-zero, the zero is a fact about contention and not about a counter
    # that stopped counting.
    run e3_control_two W4_CLIENTS=2 W4_ROUNDS=400 W4_ARMS=readonly W4_LEASE_MS=3600000 \
        W4_PROTOS=simple,ext_reparse,ext_split,extended
fi

# ---- E4: force the counter to fire at BOTH ends -------------------------------------------------
# Same arm, same protocol, same client mix. ONE knob moves: the reaper's scan interval, which is
# the only announcer in a read-only arm under `extended` (a cached prepared statement never
# touches the exclusive catalog). A ratio that never reaches its ends is not calibrated.
#
#   lease=3600000  the announcer is switched off       -> the fraction must be 0.00
#   lease=0        the announcer runs with no sleep    -> the fraction must approach 1.00
# and three points in between, so the ends are read off a DIAL and not off two special cases.
if has E4; then
    for ms in 3600000 1000 100 10 1 0; do
        run "e4_lease_$ms" W4_CLIENTS=8 W4_ROUNDS=200 W4_ARMS=readonly W4_PROTOS=extended \
            W4_LEASE_MS="$ms"
    done
fi

# ---- E6: the offered-load axis ------------------------------------------------------------------
# The banked run is ONE point at fork_every=1 — every forker statement is a fork. Whether check 3
# kills W4 depends on where a real workload sits on this curve, not on that point.
if has E6; then
    for k in 1 2 4 8 16 32 64; do
        run "e6_fork_every_$k" W4_CLIENTS=8 W4_ROUNDS=200 W4_ARMS=agent W4_PROTOS=extended \
            W4_FORK_EVERY="$k"
    done
    for n in 1 2 4 6 7; do
        run "e6_announcers_$n" W4_CLIENTS=8 W4_ROUNDS=200 W4_ARMS=agent W4_PROTOS=extended \
            W4_ANNOUNCERS="$n"
    done
fi

echo "rederive complete rc=$rc"
exit "$rc"
