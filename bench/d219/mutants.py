#!/usr/bin/env python3
"""D219 mutants: each one breaks one property the D219 tests claim to guard.

    python3 bench/d219/mutants.py --check <sha>   every pattern matches EXACTLY one site in <sha>'s
                                                  committed blob (git show; no build, no compute)
    python3 bench/d219/mutants.py <name>          apply one mutant to the working tree it is run in,
                                                  refusing unless its pattern matches exactly once

A mutant whose pattern matches zero or several sites is REFUSED, never applied: a pattern that
silently matched nothing would report every test "surviving" a mutation that never happened.

Two mutants are pre-registered to SURVIVE, and are run so each blind spot is measured rather than
asserted:
  M4  syncing before the write is invisible to every test here: nothing fault-injects the file.
  M11 the explicit flush after record_applied only REPORTS a flush failure; the guard's Drop still
      makes the stamps durable before the MERGE returns, and no test can make a flush fail. It is
      also a no-op in every reachable merge today: a published version always has an applied op,
      so record_applied's write carries every publish stamp, and the rewrite's are flushed earlier.
"""
import subprocess
import sys

DURABLE = "src/provenance/durable.rs"
DEFERRED = "src/provenance/deferred.rs"
STORE = "src/provenance/store.rs"
RUNTIME = "src/agent_sql/runtime.rs"

MUTANTS = {
    # One sync per record again: the batch is appended body by body.
    "M1_sync_per_record": (
        DURABLE,
        "        if let Err(e) = self.append_all_locked(&mut file, &bodies, &self.syncs.row_authors) {\n",
        "        if let Err(e) = bodies.iter().try_for_each(|b| self.append_locked(&mut file, b, &self.syncs.row_authors)) {\n",
    ),
    # A batch that deduplicates: one sync, but a different file (a two-column update's second
    # record is gone).
    "M2_dedupe_the_batch": (
        RUNTIME,
        "        self.prov_store.stamp_rows(&authored, snapshot.prov)?;\n",
        "        authored.dedup();\n        self.prov_store.stamp_rows(&authored, snapshot.prov)?;\n",
    ),
    # Authorship never written before MERGE returns.
    "M3_skip_the_batch": (
        RUNTIME,
        "        self.prov_store.stamp_rows(&authored, snapshot.prov)?;\n",
        "        let _ = &authored;\n",
    ),
    # Sync issued BEFORE the write it is meant to cover. Pre-registered SURVIVOR (see docstring).
    "M4_sync_before_write": (
        DURABLE,
        "        pwrite_all(&out.file, &frames, end)\n"
        "            .map_err(|e| FerroError::Provenance(format!(\"append to {}: {e}\", self.path.display())))?;\n"
        "        out.file\n"
        "            .sync_data()\n"
        "            .map_err(|e| FerroError::Provenance(e.to_string()))?;\n",
        "        out.file\n"
        "            .sync_data()\n"
        "            .map_err(|e| FerroError::Provenance(e.to_string()))?;\n"
        "        pwrite_all(&out.file, &frames, end)\n"
        "            .map_err(|e| FerroError::Provenance(format!(\"append to {}: {e}\", self.path.display())))?;\n",
    ),
    # The in-memory refusal ignored, so a batch naming an uninterned run reaches the file.
    "M5_ignore_the_memory_refusal": (
        DURABLE,
        "        self.mem.stamp_rows(rows, id)?;\n",
        "        let _ = self.mem.stamp_rows(rows, id);\n",
    ),
    # An empty batch treated as a write: it takes the lock and the poison check, so a poisoned
    # store REFUSES a batch that writes nothing (killed by the poisoned half of the empty-batch test).
    "M6_empty_batch_is_a_write": (
        DURABLE,
        "        if rows.is_empty() {\n"
        "            // Nothing to record is not a write: not refused by a poisoned store, and no sync.\n"
        "            return Ok(());\n"
        "        }\n",
        "",
    ),
    # The interned-run guard disabled in the one implementation both stores share.
    "M7_no_interned_guard": (
        STORE,
        "        if inner.runs.get(id.0 as usize - 1).is_none() {\n"
        "            return Err(FerroError::Provenance(match rows.len() {\n",
        "        if false && inner.runs.get(id.0 as usize - 1).is_none() {\n"
        "            return Err(FerroError::Provenance(match rows.len() {\n",
    ),
    # The instrument miscounts: row-authorship syncs booked as physical stamps.
    "M8_counter_books_the_wrong_kind": (
        DURABLE,
        "        if let Err(e) = self.append_all_locked(&mut file, &bodies, &self.syncs.row_authors) {\n",
        "        if let Err(e) = self.append_all_locked(&mut file, &bodies, &self.syncs.stamps) {\n",
    ),
    # ---- the whole-exit fix (86e1762) ------------------------------------------------------------
    # The publish loop stamps through the eager store again: one sync per published version.
    "M9_publish_stamps_eager": (
        RUNTIME,
        "            let author = Some((Arc::clone(&prov), snapshot.prov));\n",
        "            let author = Some((Arc::clone(self.provenance()), snapshot.prov));\n",
    ),
    # The ALTER rewrite stamps through the eager store again: one sync per moved row.
    "M10_rewrite_stamps_eager": (
        RUNTIME,
        "                .plan_alters(&report.table, &actions, &ctx.txn, Some(&prov))\n",
        "                .plan_alters(&report.table, &actions, &ctx.txn, Some(self.provenance()))\n",
    ),
    # No explicit flush after record_applied. Pre-registered SURVIVOR (see docstring).
    "M11_no_explicit_flush": (
        RUNTIME,
        "        provenance.flush()?;\n",
        "",
    ),
    # Pending records written AFTER the caller's, so file order is no longer index order.
    "M12_pending_written_last": (
        DURABLE,
        "            out.pending.iter().map(Vec::as_slice).chain(bodies.iter().copied()).collect();\n",
        "            bodies.iter().copied().chain(out.pending.iter().map(Vec::as_slice)).collect();\n",
    ),
    # A pending stamp queued even when the index refused it.
    "M13_pending_ignores_the_refusal": (
        DURABLE,
        "        self.mem.stamp(rid, id)?;\n        file.pending.push(stamp_body(rid, id));\n",
        "        let _ = self.mem.stamp(rid, id);\n        file.pending.push(stamp_body(rid, id));\n",
    ),
    # The guard's Drop does not write: an early exit leaves stamps in the index and not the file.
    "M14_guard_drop_does_not_flush": (
        DEFERRED,
        "            let _ = self.store.flush();\n",
        "",
    ),
    # The store's Drop does not write its pending stamps.
    "M15_store_drop_does_not_flush": (
        DURABLE,
        "        let _ = self.flush();\n",
        "",
    ),
    # Pending records never cleared: every later append writes them again.
    "M16_pending_never_cleared": (
        DURABLE,
        "        out.pending.clear();\n",
        "",
    ),
    # ---- the schema-phase flush and the review fixes (eff03e8) -----------------------------------
    # A poisoned store queues a pending stamp it can never write.
    "M17_pending_ignores_the_poison": (
        DURABLE,
        "        self.refuse_if_poisoned()?;\n        self.mem.stamp(rid, id)?;\n        file.pending.push(stamp_body(rid, id));\n",
        "        self.mem.stamp(rid, id)?;\n        file.pending.push(stamp_body(rid, id));\n",
    ),
    # A flush with nothing pending refused by a poisoned store: the emptiness check comes second.
    "M18_flush_checks_poison_first": (
        DURABLE,
        "        if file.pending.is_empty() {\n            return Ok(());\n        }\n        self.refuse_if_poisoned()?;\n",
        "        self.refuse_if_poisoned()?;\n        if file.pending.is_empty() {\n            return Ok(());\n        }\n",
    ),
    # The rewrite's stamps wait for the merge's final sync again (review F1).
    "M19_no_schema_phase_flush": (
        RUNTIME,
        "            provenance.flush_so_far()?;\n",
        "",
    ),
}


def refuse(msg):
    print(f"REFUSING: {msg}")
    sys.exit(4)


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--check":
        sha = sys.argv[2]
        for name, (path, old, _new) in MUTANTS.items():
            blob = subprocess.run(
                ["git", "show", f"{sha}:{path}"], capture_output=True, text=True, check=True
            ).stdout
            n = blob.count(old)
            print(f"{name}: {n} match(es) in {sha}:{path}")
            if n != 1:
                refuse(f"{name} matches {n} sites in {sha}:{path}")
        print(f"all {len(MUTANTS)} patterns match exactly one site in {sha}")
        return
    if len(sys.argv) != 2 or sys.argv[1] not in MUTANTS:
        refuse(f"usage: mutants.py --check <sha> | mutants.py <{'|'.join(MUTANTS)}>")
    name = sys.argv[1]
    path, old, new = MUTANTS[name]
    with open(path) as f:
        text = f.read()
    n = text.count(old)
    if n != 1:
        refuse(f"{name} matches {n} sites in {path}")
    with open(path, "w") as f:
        f.write(text.replace(old, new))
    print(f"applied {name} to {path}")


if __name__ == "__main__":
    main()
