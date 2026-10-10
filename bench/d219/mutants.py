#!/usr/bin/env python3
"""D219 mutants: each one breaks one property the D219 tests claim to guard.

    python3 bench/d219/mutants.py --check <sha>   every pattern matches EXACTLY one site in <sha>'s
                                                  committed blob (git show; no build, no compute)
    python3 bench/d219/mutants.py <name>          apply one mutant to the working tree it is run in,
                                                  refusing unless its pattern matches exactly once

A mutant whose pattern matches zero or several sites is REFUSED, never applied: a pattern that
silently matched nothing would report every test "surviving" a mutation that never happened. A
mutant is (path, old, new), or (path, [(old, new), ...]) for one that must edit two places at once;
every pair must match exactly once.

M10 (the rewrite given the eager store instead of the stamper) was REMOVED at f513a37: since
7999830 the rewrite queues and flushes on whatever store it is handed, so the two are the same
program — an equivalent mutant, which can only "survive".

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
ALTER = "src/catalog/alter.rs"

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
    # ---- the rewrite's stamps, written after finish (see run.sh FIX) ---------------------------------
    # A rewrite's moved rows are never stamped at all.
    "M19_rewrite_does_not_stamp": (
        ALTER,
        "                for (rid, who) in &moved {\n                    store.stamp_pending(*rid, *who)?;\n                }\n                store.flush()?;\n",
        "",
    ),
    # The flush runs whether or not this rewrite moved an attributed row: a store poisoned with
    # someone else's pending records fails a plain ALTER of an unattributed table (review 4 F1).
    "M20_flush_even_if_nothing_moved": (
        ALTER,
        "            if !moved.is_empty() {\n",
        "            if true {\n",
    ),
    # The stamps written BEFORE finish (as the rewrite loop used to): a refusal or a failed flush
    # leaves the rewrite under the old catalog, the I19 state (reviews 4 F1, 5 F1).
    "M21_stamps_before_finish": (
        ALTER,
        "        self.finish(&table, new_schema, primary_root_now, carried)?;\n",
        "        if let Some(store) = &prov {\n            for (rid, who) in &moved {\n                store.stamp_pending(*rid, *who)?;\n            }\n            store.flush()?;\n        }\n        self.finish(&table, new_schema, primary_root_now, carried)?;\n",
    ),
    # A failed flush swallowed: the ALTER reports success with its stamps not durable.
    "M22_flush_error_swallowed": (
        ALTER,
        "                store.flush()?;\n",
        "                let _ = store.flush();\n",
    ),
    # A refused stamp swallowed: the ALTER reports success with its moved rows unattributed.
    "M23_stamp_refusal_swallowed": (
        ALTER,
        "                    store.stamp_pending(*rid, *who)?;\n",
        "                    let _ = store.stamp_pending(*rid, *who);\n",
    ),
    # ---- review 6 ---------------------------------------------------------------------------------
    # The rewrite's stamps synced one by one again (a plain ALTER pays one sync per moved row).
    "M24_rewrite_stamps_eagerly": (
        ALTER,
        "                    store.stamp_pending(*rid, *who)?;\n",
        "                    store.stamp(*rid, *who)?;\n",
    ),
    # A moved row stamped at the rid it LEFT.
    "M25_stamp_the_old_rid": (
        ALTER,
        "                moved.push((new_rid, who));\n",
        "                moved.push((rid, who));\n",
    ),
    # The boundary: one moved attributed row is not enough.
    "M26_more_than_one_moved_row": (
        ALTER,
        "            if !moved.is_empty() {\n",
        "            if moved.len() > 1 {\n",
    ),
    # ---- PREREG A1 (50e175b) ------------------------------------------------------------------------
    # The writable probe deleted: a poisoned store's ALTER is installed, then refused (test 3 red).
    "M28_no_writable_probe": (
        ALTER,
        "                store.check_writable()?;\n",
        "",
    ),
    # The probe asked even when no row is attributed: an ALTER that stamps nothing is refused by a
    # store poisoned by someone else (test 1 red).
    "M29_probe_without_attribution": (
        ALTER,
        "            if prepared.iter().any(|p| p.prov.is_some()) {\n",
        "            if true {\n",
    ),
    # The epoch bump AFTER the provenance block: a failed flush leaves readers on the old shape.
    "M27_epoch_bump_after_provenance": (
        ALTER,
        [
            (
                "        self.epoch_bump();\n        // **D219 — the moved rows' stamps",
                "        // **D219 — the moved rows' stamps",
            ),
            (
                "        Ok(shapes[1..].iter().map(shape_of).collect())\n",
                "        self.epoch_bump();\n        Ok(shapes[1..].iter().map(shape_of).collect())\n",
            ),
        ],
    ),
    # ---- PREREG A3 (review 7 F5) ---------------------------------------------------------------
    # The MERGE's plan-phase probe deleted: a merge on a poisoned store installs its schema first.
    "M30_no_merge_writable_probe": (
        RUNTIME,
        "        if !pending.is_empty() {\n            prov.check_writable()?;\n        }\n",
        "",
    ),
    # The probe asked even when the merge publishes nothing: a schema-only merge refused.
    "M31_merge_probe_unconditional": (
        RUNTIME,
        "        if !pending.is_empty() {\n            prov.check_writable()?;\n",
        "        if true {\n            prov.check_writable()?;\n",
    ),
}


def pairs(edit):
    """`[old, new]` or `[[(old, new), ...]]`, as unpacked from a MUTANTS value, as a list of pairs."""
    return edit[0] if len(edit) == 1 else [tuple(edit)]


def refuse(msg):
    print(f"REFUSING: {msg}")
    sys.exit(4)


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--check":
        sha = sys.argv[2]
        for name, (path, *edit) in MUTANTS.items():
            blob = subprocess.run(
                ["git", "show", f"{sha}:{path}"], capture_output=True, text=True, check=True
            ).stdout
            for old, _new in pairs(edit):
                n = blob.count(old)
                print(f"{name}: {n} match(es) in {sha}:{path}")
                if n != 1:
                    refuse(f"{name} matches {n} sites in {sha}:{path}")
        print(f"all {len(MUTANTS)} patterns match exactly one site in {sha}")
        return
    if len(sys.argv) != 2 or sys.argv[1] not in MUTANTS:
        refuse(f"usage: mutants.py --check <sha> | mutants.py <{'|'.join(MUTANTS)}>")
    name = sys.argv[1]
    path, *edit = MUTANTS[name]
    with open(path) as f:
        text = f.read()
    for old, new in pairs(edit):
        n = text.count(old)
        if n != 1:
            refuse(f"{name} matches {n} sites in {path}")
        text = text.replace(old, new)
    with open(path, "w") as f:
        f.write(text)
    print(f"applied {name} to {path}")


if __name__ == "__main__":
    main()
