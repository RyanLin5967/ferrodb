#!/usr/bin/env python3
"""D246 mutants (PREREG A2, A3): each one breaks one property the §6.1 and publish-order tests claim.

    python3 bench/d246/mutants.py --check <sha>   every pattern matches EXACTLY one site in <sha>'s
                                                  committed blob (git show; no build, no compute)
    python3 bench/d246/mutants.py <name>          apply one mutant to the working tree it is run in,
                                                  refusing unless its pattern matches exactly once

The runner is bench/d219/mutants.py's, loaded from that file rather than copied, so the
exact-one-match refusal is the validated one. Only the table below is D246's.

M1 and M2 are the pgserver mutants in run.sh (a restored file, a changed path), not pattern edits.
"""
import importlib.util
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("d219_mutants", os.path.join(HERE, "..", "d219", "mutants.py"))
runner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(runner)

DURABLE = "src/provenance/durable.rs"
SESSION = "src/agent_sql/session.rs"
RUNTIME = "src/agent_sql/runtime.rs"

MUTANTS = {
    # The fork's run synced inside begin_session_as_staged again: under `state`, inside the guard.
    "M3_intern_under_the_guard": (
        RUNTIME,
        "        let prov = self.prov_store.intern_pending(&RunEntity::new(\n",
        "        let prov = self.prov_store.intern(&RunEntity::new(\n",
    ),
    # complete() acknowledges the fork without awaiting its run's record.
    "M4_complete_skips_the_run": (
        SESSION,
        "            Some((store, prov)) => store.await_run(prov),\n            None => Ok(()),\n",
        "            Some((_store, _prov)) => Ok(()),\n            None => Ok(()),\n",
    ),
    # A forgotten complete() leaves the run's record owed for ever.
    "M5_drop_skips_the_run": (
        SESSION,
        "        if let Some((store, prov)) = self.run.take() {\n            let _ = store.await_run(prov);\n        }\n",
        "        let _ = self.run.take();\n",
    ),
    # The publish declares the run to the log before its record is durable.
    "M6_publish_before_the_run_is_durable": (
        RUNTIME,
        "        if !snapshot.prov.is_none() {\n            self.provenance().await_run(snapshot.prov)?;\n        }\n",
        "",
    ),
    # await_run holds the file lock across its group sync.
    "M7_run_sync_under_the_lock": (
        DURABLE,
        "            seq\n        };\n        self.group.wait_durable(seq, || self.sync_runs())?;\n",
        "            self.group.wait_durable(seq, || self.sync_runs())?;\n            seq\n        };\n",
    ),
    # A record already WRITTEN is treated as durable, though its sync may still be in flight.
    "M8_written_run_is_not_awaited": (
        DURABLE,
        "            let written = file.queued - file.pending.len() as u64;\n            if seq > written {\n",
        "            let written = file.queued - file.pending.len() as u64;\n            if seq <= written {\n                return Ok(());\n            }\n            if seq > written {\n",
    ),
    # A synchronous append's sync no longer marks the group durable.
    "M9_synchronous_sync_covers_nothing": (
        DURABLE,
        "        self.group.covered_all();\n",
        "",
    ),
    # intern returns a repeat whose record is still pending, without awaiting it.
    "M10_intern_repeat_does_not_await": (
        DURABLE,
        "                None if !file.run_seqs.contains_key(&id) => return Ok(id),\n",
        "                None if true => return Ok(id),\n",
    ),
}

if __name__ == "__main__":
    runner.MUTANTS = MUTANTS
    runner.main()
