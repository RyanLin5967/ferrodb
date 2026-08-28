#!/usr/bin/env bash
set -uo pipefail
cd /Users/idide/wt/ferrodb-F11-server-reaps
./scratchpad/f11_fire.sh M4-scan-does-nothing the_thread_reaps_an_expired_branch_with_nobody_asking_it_to the_cli_reaps_an_abandoned_branch_with_no_client_action_and_pages_return_to_baseline
./scratchpad/f11_fire.sh M5-reap-unconditionally an_unexpired_branch_survives_every_scan a_live_lease_survives_a_server_that_scans_continuously
./scratchpad/f11_fire.sh M6-never-forget a_reaped_branchs_workspace_is_forgotten_without_the_client_saying_anything
./scratchpad/f11_fire.sh M7-stop-sleeps-the-interval stop_does_not_wait_out_the_scan_interval
./scratchpad/f11_fire.sh M8-drop-does-not-stop dropping_the_lease_thread_stops_it
./scratchpad/f11_fire.sh M9-knob-defaults the_scan_interval_knob_refuses_every_value_it_cannot_use both_binaries_refuse_to_start_on_an_unusable_scan_interval
./scratchpad/f11_fire.sh M10-reaper-not-attached "" a_merged_branch_gives_its_extent_back_without_any_lease_scan
echo "ALL MUTANTS DONE"
