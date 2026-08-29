#!/bin/bash
# Re-run, standalone, the targets the sweep left unresolved. Results go to their OWN file so the
# sweep's original verdicts stand as evidence rather than being overwritten.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
OUT=/Users/idide/wt/artie-research/build-G
mkdir -p "$OUT/suite-f10-rerun"
RESULTS="$OUT/suite-f10-rerun.tsv"
: > "$RESULTS"
for t in integration_cdc_publication integration_base_backup integration_cdc_go_consumer integration_consensus_failover; do
    for attempt in 1 2; do
        log="$OUT/suite-f10-rerun/$t-$attempt.log"
        printf 'load-before\t%s\n' "$(uptime | sed 's/.*averages: //')" >> "$RESULTS"
        timeout 1800 cargo test --test "$t" --no-fail-fast > "$log" 2>&1
        rc=$?
        v=$(grep 'test result:' "$log" | tail -1)
        if [ -z "$v" ]; then
            printf '%s\tattempt%s\tSILENT\trc=%s\n' "$t" "$attempt" "$rc" >> "$RESULTS"
            echo "SILENT $t attempt $attempt rc=$rc"
        else
            p=$(grep -o '[0-9]* passed' "$log" | awk '{s+=$1} END {print s+0}')
            f=$(grep -o '[0-9]* failed' "$log" | awk '{s+=$1} END {print s+0}')
            printf '%s\tattempt%s\tpassed=%s\tfailed=%s\trc=%s\n' "$t" "$attempt" "$p" "$f" "$rc" >> "$RESULTS"
            echo "$t attempt $attempt passed=$p failed=$f rc=$rc"
        fi
    done
done
cat "$RESULTS"
