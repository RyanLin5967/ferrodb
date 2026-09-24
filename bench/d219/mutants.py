#!/usr/bin/env python3
"""D219 mutants: each one breaks one property the D219 tests claim to guard.

    python3 bench/d219/mutants.py --check <sha>   every pattern matches EXACTLY one site in <sha>'s
                                                  committed blob (git show; no build, no compute)
    python3 bench/d219/mutants.py <name>          apply one mutant to the working tree it is run in,
                                                  refusing unless its pattern matches exactly once

A mutant whose pattern matches zero or several sites is REFUSED, never applied: a pattern that
silently matched nothing would report every test "surviving" a mutation that never happened.

M4 is the one mutant pre-registered to SURVIVE: syncing before the write is invisible to every test
here, because nothing fault-injects the file underneath the store. It is run so the blind spot is
measured rather than asserted.
"""
import subprocess
import sys

DURABLE = "src/provenance/durable.rs"
STORE = "src/provenance/store.rs"
RUNTIME = "src/agent_sql/runtime.rs"

MUTANTS = {
    # One sync per record again: the batch is appended body by body.
    "M1_sync_per_record": (
        DURABLE,
        "        if let Err(e) = self.append_all_locked(&file, &bodies) {\n",
        "        if let Err(e) = bodies.iter().try_for_each(|b| self.append_locked(&file, b)) {\n",
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
        "        pwrite_all(file, &frames, end)\n"
        "            .map_err(|e| FerroError::Provenance(format!(\"append to {}: {e}\", self.path.display())))?;\n"
        "        file.sync_data()\n"
        "            .map_err(|e| FerroError::Provenance(e.to_string()))?;\n",
        "        file.sync_data()\n"
        "            .map_err(|e| FerroError::Provenance(e.to_string()))?;\n"
        "        pwrite_all(file, &frames, end)\n"
        "            .map_err(|e| FerroError::Provenance(format!(\"append to {}: {e}\", self.path.display())))?;\n",
    ),
    # The in-memory refusal ignored, so a batch naming an uninterned run reaches the file.
    "M5_ignore_the_memory_refusal": (
        DURABLE,
        "        self.mem.stamp_rows(rows, id)?;\n",
        "        let _ = self.mem.stamp_rows(rows, id);\n",
    ),
    # An empty batch treated as a write: it reaches the append, which refuses a tagless body and
    # poisons the store — so a MERGE of zero ops would poison provenance for the process.
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
        "            Some(TAG_ROW_AUTHOR) => Ok(&self.row_authors),\n",
        "            Some(TAG_ROW_AUTHOR) => Ok(&self.stamps),\n",
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
