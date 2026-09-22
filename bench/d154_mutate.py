#!/usr/bin/env python3
"""D154 fire-check: prove the guard and every `?` that propagates it are load-bearing.

Two classes of mutant:

  * THE GUARD ITSELF (1) — revert `write_str`'s `u16::try_from` to the raw `as u16` cast it
    replaced. If this survives, the whole row is decorative.
  * THE PROPAGATION (16) — replace each call site's `?` with `let _ = ...`, i.e. encode the value
    and DROP the refusal. This is the realistic mutant: a missing `?` on a `Result` is a WARNING,
    not an error, and CI denies only `dead_code`, so a site can silently swallow the refusal and
    the build stays green.

A SURVIVOR means the fire-check does not actually exercise that site.
"""
import subprocess, sys, pathlib

TESTS = ["--test", "d154_length_prefix_refusal"]

# (label, file, exact committed text, the mutant)
MUTANTS = [
    ("THE GUARD ITSELF (write_str's u16 check)", "src/wal/log.rs",
     '    let len = u16::try_from(s.len()).map_err(|_| FerroError::Unrepresentable {\n'
     '        what: what.to_string(),\n'
     '        len: s.len(),\n'
     '        limit: u16::MAX as usize,\n'
     '    })?;\n'
     '    buffer.extend_from_slice(&len.to_be_bytes());\n',
     '    buffer.extend_from_slice(&(s.len() as u16).to_be_bytes());\n'),
]

# The propagation mutants, generated from their call text.
PROP = [
    ("Ddl / table name", "src/wal/log.rs",
     '                write_str(buffer, table, "a table name")?;\n'),
    ("Ddl / column name", "src/wal/log.rs",
     '                    write_str(buffer, name, "a column name")?;\n'),
    ("AlterColumn::Add", "src/wal/log.rs",
     '                                write_str(buffer, column, "an added column\'s name")?;\n'),
    ("AlterColumn::Rename / from", "src/wal/log.rs",
     '                                write_str(buffer, from, "a renamed column\'s old name")?;\n'),
    ("AlterColumn::Rename / to", "src/wal/log.rs",
     '                                write_str(buffer, to, "a renamed column\'s new name")?;\n'),
    ("AlterColumn::Retype", "src/wal/log.rs",
     '                                write_str(buffer, column, "a retyped column\'s name")?;\n'),
    ("RunIdentity / agent_id", "src/wal/log.rs",
     '                write_str(buffer, &run.agent_id, "an agent id")?;\n'),
    ("RunIdentity / run_id", "src/wal/log.rs",
     '                write_str(buffer, &run.run_id, "a run id")?;\n'),
    ("RunIdentity / model", "src/wal/log.rs",
     '                write_str(buffer, &run.model, "a model name")?;\n'),
    ("RunIdentity / model_version", "src/wal/log.rs",
     '                write_str(buffer, &run.model_version, "a model version")?;\n'),
    ("Clr / recursive redo", "src/wal/log.rs",
     '                redo.serialize(buffer)?;\n'),
    ("provenance / agent_id", "src/provenance/durable.rs",
     '        write_str(&mut tail, &run.agent_id, "an agent id")?;\n'),
    ("provenance / run_id", "src/provenance/durable.rs",
     '        write_str(&mut tail, &run.run_id, "a run id")?;\n'),
    ("provenance / model", "src/provenance/durable.rs",
     '        write_str(&mut tail, &run.model, "a model name")?;\n'),
    ("provenance / model_version", "src/provenance/durable.rs",
     '        write_str(&mut tail, &run.model_version, "a model version")?;\n'),
]
for label, f, call in PROP:
    indent = call[: len(call) - len(call.lstrip())]
    dropped = indent + "let _ = " + call.strip()[:-1].rstrip("?") + ";\n"
    MUTANTS.append((label, f, call, dropped))

# The column-count guard is a block, not a one-liner.
MUTANTS.append((
    "Ddl / column count", "src/wal/log.rs",
    '                let n_cols = u16::try_from(columns.len()).map_err(|_| FerroError::Unrepresentable {\n'
    '                    what: "a DDL record\'s column count".to_string(),\n'
    '                    len: columns.len(),\n'
    '                    limit: u16::MAX as usize,\n'
    '                })?;\n'
    '                buffer.extend_from_slice(&n_cols.to_be_bytes());\n',
    '                buffer.extend_from_slice(&(columns.len() as u16).to_be_bytes());\n'))


def restore():
    subprocess.run(["git", "checkout", "--", "src/"], check=True)


def main():
    restore()
    base = subprocess.run(["cargo", "test", *TESTS], capture_output=True, text=True, timeout=1800)
    print(f"BASELINE (unmutated): rc={base.returncode} "
          f"{'PASS' if base.returncode == 0 else 'FAIL — must pass before it can kill anything'}",
          flush=True)
    if base.returncode != 0:
        print(base.stdout[-3000:])
        return 1

    survivors, killed = [], 0
    for label, f, old, new in MUTANTS:
        p = pathlib.Path(f)
        original = p.read_text()
        if original.count(old) != 1:
            print(f"{label}: SKIPPED — pattern appears {original.count(old)}x, not once", flush=True)
            survivors.append(label + " (pattern not found)")
            continue
        p.write_text(original.replace(old, new))
        r = subprocess.run(["cargo", "test", *TESTS], capture_output=True, text=True, timeout=1800)
        out = r.stdout + r.stderr
        restore()

        if r.returncode == 0:
            print(f"{label}: *** SURVIVED *** — the fire-check does not exercise this site", flush=True)
            survivors.append(label)
        else:
            why = next((l.strip() for l in out.splitlines()
                        if "DID NOT FIRE" in l or "panicked at" in l or "error[" in l), "rc!=0")
            print(f"{label}: KILLED — {why[:150]}", flush=True)
            killed += 1

    print()
    print(f"RESULT: {killed} killed, {len(survivors)} survived, of {len(MUTANTS)}")
    if survivors:
        for s in survivors:
            print(f"  SURVIVOR: {s}")
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        restore()
