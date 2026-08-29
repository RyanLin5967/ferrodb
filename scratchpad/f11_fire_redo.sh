#!/usr/bin/env bash
set -uo pipefail
cd /Users/idide/wt/ferrodb-F11-server-reaps
./scratchpad/f11_fire.sh M6-never-forget a_reaped_branchs_workspace_is_forgotten_without_the_client_saying_anything
./scratchpad/f11_fire.sh M7-stop-sleeps-the-interval "a_signalled_halt_returns_at_once_instead_of_sleeping_the_interval stop_does_not_wait_out_the_scan_interval"
./scratchpad/f11_fire.sh M10-reaper-not-attached "" a_merged_branch_gives_its_extent_back_without_any_lease_scan
echo "REDO DONE"
