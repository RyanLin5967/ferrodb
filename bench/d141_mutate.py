#!/usr/bin/env python3
"""D141 fire-check: revert each of the eight guards to the raw cast it replaced, one at a time,
and record whether the fire-check test kills that mutant.

A guard that cannot fire is worse than none. `every_length_prefix_refuses_the_first_value_it
_cannot_express` passing proves the eight guards collectively refuse; it does NOT prove that each
one is individually load-bearing, because a guard earlier in `serialize` could be catching a case
the test attributes to a later one. This script settles that by removing each in turn.

SURVIVED on any row means the fire-check does not actually exercise that guard.
"""
import subprocess, sys, pathlib

SRC = pathlib.Path("src/catalog/catalog_page.rs")
TEST = "catalog_page::tests::every_length_prefix_refuses_the_first_value_it_cannot_express"

# (guard, guarded text as committed, the raw cast it replaced)
MUTANTS = [
    ("guard 1 (table count)",
     '        let num_entries = fits_u16(|| "the catalog page\'s table count".to_string(), self.entries.len())?;\n',
     '        let num_entries = self.entries.len() as u16;\n'),
    ("guard 2 (table name)",
     '            bytes[offset] = fits_u8(|| format!("the name of table {}", elide(&entry.name)), name_bytes.len())?;\n',
     '            bytes[offset] = name_bytes.len() as u8;\n'),
    ("guard 3 (column count)",
     '            let num_columns = fits_u16(\n'
     '                || format!("the column count of table {}", elide(&entry.name)),\n'
     '                entry.schema.columns.len(),\n'
     '            )?;\n',
     '            let num_columns = entry.schema.columns.len() as u16;\n'),
    ("guard 4 (column name)",
     '                bytes[offset] = fits_u8(\n'
     '                    || format!("the name of column {} in table {}", elide(&col.name), elide(&entry.name)),\n'
     '                    col_name_bytes.len(),\n'
     '                )?;\n',
     '                bytes[offset] = col_name_bytes.len() as u8;\n'),
    ("guard 5 (index count)",
     '            let num_indexes = fits_u8(\n'
     '                || format!("the index count of table {}", elide(&entry.name)),\n'
     '                entry.indexes.len(),\n'
     '            )?;\n',
     '            let num_indexes = entry.indexes.len() as u8;\n'),
    ("guard 6 (indexed-column name)",
     '                bytes[offset] = fits_u8(\n'
     '                    || format!("the indexed-column name {} in table {}", elide(&ind.column_name), elide(&entry.name)),\n'
     '                    ind_name_bytes.len(),\n'
     '                )?;\n',
     '                bytes[offset] = ind_name_bytes.len() as u8;\n'),
    ("guard 7 (full-text index count)",
     '            let num_fulltext = fits_u8(\n'
     '                || format!("the full-text index count of table {}", elide(&entry.name)),\n'
     '                entry.fulltext_indexes.len(),\n'
     '            )?;\n',
     '            let num_fulltext = entry.fulltext_indexes.len() as u8;\n'),
    ("guard 8 (full-text indexed-column name)",
     '                bytes[offset] = fits_u8(\n'
     '                    || format!("the full-text indexed-column name {} in table {}", elide(&ft.column_name), elide(&entry.name)),\n'
     '                    ft_name_bytes.len(),\n'
     '                )?;\n',
     '                bytes[offset] = ft_name_bytes.len() as u8;\n'),
]


def restore():
    subprocess.run(["git", "checkout", "--", str(SRC)], check=True)


def main():
    restore()
    baseline = subprocess.run(
        ["cargo", "test", "--lib", TEST], capture_output=True, text=True, timeout=900)
    print(f"BASELINE (unmutated): rc={baseline.returncode} "
          f"{'PASS' if baseline.returncode == 0 else 'FAIL — the fire-check must pass before it can kill anything'}",
          flush=True)
    if baseline.returncode != 0:
        print(baseline.stdout[-2000:])
        return 1

    survivors = []
    for name, guarded, raw in MUTANTS:
        original = SRC.read_text()
        if original.count(guarded) != 1:
            print(f"{name}: SKIPPED — its guarded text appears {original.count(guarded)} times, not once", flush=True)
            survivors.append(name)
            continue
        SRC.write_text(original.replace(guarded, raw))
        r = subprocess.run(["cargo", "test", "--lib", TEST],
                           capture_output=True, text=True, timeout=900)
        restore()

        out = r.stdout + r.stderr
        if r.returncode == 0:
            print(f"{name}: *** SURVIVED *** — the fire-check does not exercise this guard", flush=True)
            survivors.append(name)
            continue
        # Why it died: the guard's own "DID NOT FIRE", or an unchecked write reaching a slice index.
        if "DID NOT FIRE" in out:
            why = next(l.strip() for l in out.splitlines() if "DID NOT FIRE" in l)
        elif "range end index" in out or "out of range" in out:
            why = next(l.strip() for l in out.splitlines()
                       if "range end index" in l or "out of range" in l)
        else:
            why = next((l.strip() for l in out.splitlines() if "panicked at" in l), "rc!=0")
        print(f"{name}: KILLED — {why}", flush=True)

    print()
    if survivors:
        print(f"RESULT: {len(survivors)} SURVIVOR(S): {survivors}")
        return 1
    print(f"RESULT: all {len(MUTANTS)} guards KILLED by {TEST}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        restore()
