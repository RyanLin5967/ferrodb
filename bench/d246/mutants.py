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
ALTER = "src/catalog/alter.rs"

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
    # The group sync holds the file lock across the fsync. (PREREG A4b: since the post-sync poison
    # check takes the lock inside sync_runs, holding it in await_run across the wait would
    # self-deadlock, so the mutant moves sync_runs' own acquisition ahead of the fsync instead.)
    "M7_run_sync_under_the_lock": (
        DURABLE,
        [
            (
                "    fn sync_runs(&self) -> Result<(), FerroError> {\n        #[cfg(test)]\n",
                "    fn sync_runs(&self) -> Result<(), FerroError> {\n        let _file = self.file.lock().unwrap();\n        #[cfg(test)]\n",
            ),
            (
                "        let _file = self.file.lock().map_err(|_| {\n"
                "            FerroError::Provenance(format!(\n"
                "                \"{}: the provenance lock was poisoned by a panicking writer; refusing to sync\",\n"
                "                self.path.display()\n"
                "            ))\n"
                "        })?;\n"
                "        self.refuse_if_poisoned()?;\n",
                "        self.refuse_if_poisoned()?;\n",
            ),
        ],
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
    # ---- PREREG A4 ----------------------------------------------------------------------------
    # A repeat run's fork syncs anyway: per FORK, not per new run.
    "M11_repeat_run_syncs": (
        DURABLE,
        "                return Ok(());\n            };\n            let written = file.queued",
        "                drop(file);\n                return self.sync_runs();\n            };\n            let written = file.queued",
    ),
    # D219's M12 at its D246 site: pending records written AFTER the call's own.
    "M15_pending_written_last": (
        DURABLE,
        "            out.pending[..n].iter().map(Vec::as_slice).chain(bodies.iter().copied()).collect();\n",
        "            bodies.iter().copied().chain(out.pending[..n].iter().map(Vec::as_slice)).collect();\n",
    ),
    # D219's M4 at its D246 site: the sync before the write. Pre-registered SURVIVOR.
    "M16_sync_before_write": (
        DURABLE,
        "        let written_pending = out.pending.len();\n"
        "        if !self.write_locked(out, written_pending, bodies)? {\n"
        "            return Ok(());\n"
        "        }\n"
        "        out.file\n"
        "            .sync_data()\n"
        "            .map_err(|e| FerroError::Provenance(e.to_string()))?;\n",
        "        let written_pending = out.pending.len();\n"
        "        out.file\n"
        "            .sync_data()\n"
        "            .map_err(|e| FerroError::Provenance(e.to_string()))?;\n"
        "        if !self.write_locked(out, written_pending, bodies)? {\n"
        "            return Ok(());\n"
        "        }\n",
    ),
    # await_run takes its tickets before its write. Pre-registered SURVIVOR.
    "M17_ticket_before_write": (
        DURABLE,
        [
            (
                "                    if let Err(e) = self.write_locked(&file, runs_ahead, &[]) {\n",
                "                    self.group.tickets(runs_ahead as u64);\n"
                "                    if let Err(e) = self.write_locked(&file, runs_ahead, &[]) {\n",
            ),
            (
                "                    file.pending.drain(..runs_ahead).for_each(drop);\n"
                "                    self.group.tickets(runs_ahead as u64);\n",
                "                    file.pending.drain(..runs_ahead).for_each(drop);\n",
            ),
        ],
    ),
    # A synchronous append marks the group durable before its fsync. Pre-registered SURVIVOR.
    "M18_covered_before_sync": (
        DURABLE,
        [
            (
                "        out.file\n"
                "            .sync_data()\n"
                "            .map_err(|e| FerroError::Provenance(e.to_string()))?;\n"
                "        // Cleared only once",
                "        self.group.tickets(written_pending as u64);\n"
                "        self.group.covered_all();\n"
                "        out.file\n"
                "            .sync_data()\n"
                "            .map_err(|e| FerroError::Provenance(e.to_string()))?;\n"
                "        // Cleared only once",
            ),
            (
                "        self.group.tickets(written_pending as u64);\n"
                "        self.group.covered_all();\n"
                "        Ok(())\n",
                "        Ok(())\n",
            ),
        ],
    ),
    # The run sync trusts its own fsync: no poison check after it.
    "M19_no_poison_check_after_the_run_sync": (
        DURABLE,
        "        let _file = self.file.lock().map_err(|_| {\n"
        "            FerroError::Provenance(format!(\n"
        "                \"{}: the provenance lock was poisoned by a panicking writer; refusing to sync\",\n"
        "                self.path.display()\n"
        "            ))\n"
        "        })?;\n"
        "        self.refuse_if_poisoned()?;\n",
        "",
    ),
    # await_run writes every pending record, stamps included, and leaves them unsynced.
    "M20_await_writes_every_pending_record": (
        DURABLE,
        "                    file.pending.iter().take_while(|b| b.first() == Some(&TAG_RUN)).count();\n",
        "                    file.pending.len();\n",
    ),
    # A poisoned store queues a pending run.
    "M21_intern_pending_ignores_the_poison": (
        DURABLE,
        "        self.refuse_if_poisoned()?;\n"
        "        let (id, body) = self.intern_locked(run)?;\n"
        "        if let Some(body) = body {\n",
        "        let (id, body) = self.intern_locked(run)?;\n"
        "        if let Some(body) = body {\n",
    ),
    # await_run on a poisoned store writes a pending run record after the failed append.
    "M22_await_run_ignores_the_poison": (
        DURABLE,
        "                self.refuse_if_poisoned()?;\n"
        "                let runs_ahead =\n",
        "                let runs_ahead =\n",
    ),
    # A3's await back after the schema apply: a refusal there returns with the edits installed.
    "M23_await_after_the_schema_apply": (
        RUNTIME,
        [
            (
                "        if !snapshot.prov.is_none() {\n"
                "            self.provenance().await_run(snapshot.prov)?;\n"
                "        }\n"
                "        let provenance = ProvenanceFlush::new(",
                "        let provenance = ProvenanceFlush::new(",
            ),
            (
                "        // **Reserve the version sequence BEFORE the rows become visible.**\n",
                "        if !snapshot.prov.is_none() {\n"
                "            self.provenance().await_run(snapshot.prov)?;\n"
                "        }\n"
                "        // **Reserve the version sequence BEFORE the rows become visible.**\n",
            ),
        ],
    ),
    # A failed completion leaves the connection inside the session it refused.
    "M24_failed_complete_keeps_the_session": (
        SESSION,
        "        if completed.is_err() {\n            *agent = None;\n        }\n",
        "        let _ = &agent;\n",
    ),
    # A refused restamp returns at once, leaving the stamps queued before it pending.
    "M25_refused_restamp_leaves_stamps_pending": (
        ALTER,
        "                let queued =\n"
        "                    moved.iter().try_for_each(|(rid, who)| store.stamp_pending(*rid, *who));\n"
        "                let flushed = store.flush();\n"
        "                queued.and(flushed)?;\n",
        "                for (rid, who) in &moved {\n"
        "                    store.stamp_pending(*rid, *who)?;\n"
        "                }\n"
        "                store.flush()?;\n",
    ),
    # ---- PREREG A4c ----------------------------------------------------------------------------
    # D219's M19 re-cut: the rewrite never stamps its moved rows.
    "M26_d219m19_rewrite_does_not_stamp": (
        ALTER,
        "                let queued =\n"
        "                    moved.iter().try_for_each(|(rid, who)| store.stamp_pending(*rid, *who));\n"
        "                let flushed = store.flush();\n"
        "                queued.and(flushed)?;\n",
        "",
    ),
    # D219's M22 re-cut: a failed flush is swallowed.
    "M27_d219m22_flush_error_swallowed": (
        ALTER,
        "                queued.and(flushed)?;\n",
        "                queued?;\n                let _ = flushed;\n",
    ),
    # D219's M23 re-cut: every restamp refusal is swallowed.
    "M28_d219m23_stamp_refusal_swallowed": (
        ALTER,
        "                let queued =\n"
        "                    moved.iter().try_for_each(|(rid, who)| store.stamp_pending(*rid, *who));\n",
        "                let queued: Result<(), FerroError> = {\n"
        "                    for (rid, who) in &moved {\n"
        "                        let _ = store.stamp_pending(*rid, *who);\n"
        "                    }\n"
        "                    Ok(())\n"
        "                };\n",
    ),
    # D219's M24 re-cut: the rewrite syncs once per moved row again.
    "M29_d219m24_rewrite_stamps_eagerly": (
        ALTER,
        "|(rid, who)| store.stamp_pending(*rid, *who));\n",
        "|(rid, who)| store.stamp(*rid, *who));\n",
    ),
    # await_run refuses on a poisoned store even a run that is already durable (R2-D5 undone).
    "M30_await_refuses_durable_runs": (
        DURABLE,
        "            let Some(&seq) = file.run_seqs.get(&id) else {\n",
        "            self.refuse_if_poisoned()?;\n            let Some(&seq) = file.run_seqs.get(&id) else {\n",
    ),
    # The sync handle shares the append descriptor's error cursor again. Pre-registered SURVIVOR.
    "M31_shared_description": (
        DURABLE,
        "        let sync_handle = OpenOptions::new()\n"
        "            .write(true)\n"
        "            .open(&path)\n",
        "        let sync_handle = file\n"
        "            .try_clone()\n",
    ),
    # The post-fsync poison check without the lock. Pre-registered SURVIVOR.
    "M32_post_check_without_the_lock": (
        DURABLE,
        "        let _file = self.file.lock().map_err(|_| {\n"
        "            FerroError::Provenance(format!(\n"
        "                \"{}: the provenance lock was poisoned by a panicking writer; refusing to sync\",\n"
        "                self.path.display()\n"
        "            ))\n"
        "        })?;\n"
        "        self.refuse_if_poisoned()?;\n",
        "        self.refuse_if_poisoned()?;\n",
    ),
    # A completion site that does not close the session it opened. Pre-registered SURVIVORS.
    "M33_executor_site_uses_complete": (
        "src/execution/executor.rs",
        "        d.complete_for(&mut session.agent)?;\n",
        "        d.complete()?;\n",
    ),
    "M34_dispatch_site_uses_complete": (
        "src/agent_sql/dispatch.rs",
        "        d.complete_for(&mut session.agent)?;\n",
        "        d.complete()?;\n",
    ),
    "M35_pgwire_site_uses_complete": (
        "src/pgwire/extended.rs",
        "                                d.complete_for(&mut session.agent)?;\n",
        "                                d.complete()?;\n",
    ),
}

if __name__ == "__main__":
    runner.MUTANTS = MUTANTS
    runner.main()
