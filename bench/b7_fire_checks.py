#!/usr/bin/env python3
"""B7 fire-checks: mutate each guard so its property is gone, and require the tests to notice.

Run from the repo root:  python3 bench/b7_fire_checks.py   (output: bench/b7_fire_checks.txt)

Why this is a script and not a paragraph. A fire-check that lives only in prose cannot be re-run, and
this one has to be: a mutant that does not COMPILE prints nothing and looks exactly like a surviving
one, which has produced four false passes in this repository. So every mutant below is applied to the
real tree, the build's exit status is recorded before any red is believed, the named tests are run,
and the mutation is reverted with `git checkout` — and the script exits non-zero if any mutant either
failed to build or SURVIVED. It is a gate, not a log.

Each mutant removes a property outright rather than one of two sufficient sites for it. Where two
sites enforce related rules over different inputs — rows and declared shapes, the producer's check and
the consumer's — they are mutated separately, and the table records which tests each one turned red.

Refuses to run on a dirty tree: it reverts files with `git checkout --`, which on a dirty tree would
discard someone's uncommitted work.
"""

import subprocess
import sys
import time

ROOT = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True,
                      check=True).stdout.strip()
GO = "/opt/homebrew/bin/go"
LOG = []


def log(line=""):
    print(line, flush=True)
    LOG.append(line)


def run(cmd, cwd=ROOT, timeout=1800):
    t = time.time()
    p = subprocess.run(cmd, cwd=cwd, shell=True, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout + p.stderr, time.time() - t


def patch(path, old, new):
    full = f"{ROOT}/{path}"
    s = open(full).read()
    if s.count(old) != 1:
        raise SystemExit(f"MUTANT ANCHOR NOT UNIQUE in {path}: found {s.count(old)} occurrences.\n"
                         f"The code moved under this script; fix the anchor rather than the count.")
    open(full, "w").write(s.replace(old, new, 1))


def revert(path):
    run(f"git checkout -- {path}")


# (id, what the mutant removes, file, anchor, replacement, build cmd, test cmd, expect-substring)
RUST_BUILD = "cargo build --tests"
MUTANTS = [
    ("M1", "the cursor is computed over every DECODED event and refused events are merely filtered "
           "out - the naive refusal",
     "src/replication/stream.rs",
     """        let refused_at = candidates
            .iter()
            .enumerate()
            .find_map(|(i, e)| self.publication.check(e).err().map(|r| (i, r)));""",
     """        let refused_at: Option<(usize, Refusal)> = None;
        let _ = &candidates;""",
     RUST_BUILD, "cargo test --lib replication::stream", None),

    ("M1b", "the batch is truncated at the offending EVENT instead of at the start of its commit",
     "src/replication/stream.rs",
     """                let boundary = candidates
                    .iter()
                    .position(|e| e.commit_lsn == commit)
                    .expect("the refused event's own commit is in the batch");""",
     """                let boundary = at;
                let _ = commit;""",
     RUST_BUILD, "cargo test --test integration_cdc_publication", None),

    ("M2", "the column allowlist inside jsonl::row_into",
     "src/replication/jsonl.rs",
     """                if !mask.publishes(name) {
                    continue;
                }""",
     """                if false && !mask.publishes(name) {
                    continue;
                }""",
     RUST_BUILD, "cargo test --lib replication::jsonl", None),

    ("M3", "an undecided table falls through to publishing everything",
     "src/replication/publication.rs",
     """                    Some(cols) if !cols.is_empty() => Ok(Some(cols)),""",
     """                    Some(cols) if !cols.is_empty() => Ok(Some(cols)),
                    _ if true => Ok(None),""",
     RUST_BUILD, "cargo test --lib replication", None),

    ("M5", "the CREATE_TABLE declared shape is no longer projected",
     "src/replication/jsonl.rs",
     """                if !mask.publishes(&c.name) {
                    continue;
                }""",
     """                if false && !mask.publishes(&c.name) {
                    continue;
                }""",
     RUST_BUILD, "cargo test --lib replication::jsonl", None),

    ("M6", "write_feed writes as it renders instead of rendering the whole batch first",
     "src/replication/jsonl.rs",
     """    let lines: Vec<String> = events
        .iter()
        .map(|e| {
            let mask = publication.check(e)?;
            line_with_mask(e, &mask)
        })
        .collect::<Result<_, Refusal>>()?;
    let n = lines.len();
    for line in lines {""",
     """    let mut n = 0;
    for line in events.iter().map(|e| {
        let mask = publication.check(e)?;
        n += 1;
        line_with_mask(e, &mask)
    }) {
        let line = line?;""",
     RUST_BUILD, "cargo test --lib replication::jsonl", None),

    ("M7", "a value with no column name no longer refuses",
     "src/replication/publication.rs",
     """            if image.len() > e.columns.len() {""",
     """            if false && image.len() > e.columns.len() {""",
     RUST_BUILD, "cargo test --lib replication::publication", None),

    ("R1", "two columns with one name are no longer ambiguous - the name-keyed mask decides for both",
     "src/replication/publication.rs",
     """            if !seen.insert(c.as_str()) {""",
     """            if false && !seen.insert(c.as_str()) {""",
     RUST_BUILD, "cargo test --lib replication::publication", None),

    ("R2", "an excluded table is refused like an undecided one, so the cursor stalls on it",
     "src/replication/stream.rs",
     """        let excluded = decoded_events.iter().filter(|e| self.publication.excludes(&e.table)).count();""",
     """        let excluded = 0usize;""",
     RUST_BUILD, "cargo test --lib replication::stream", None),

    ("R3", "the refusal is looked for in the raw batch instead of among the events that would be "
           "delivered",
     "src/replication/stream.rs",
     """        let refused_at = candidates
            .iter()
            .enumerate()
            .find_map(|(i, e)| self.publication.check(e).err().map(|r| (i, r)));
        let (refused, refusal, refused_commit) = match refused_at {""",
     """        let raw: Vec<&ChangeEvent> = decoded_events.iter().collect();
        let refused_at = raw
            .iter()
            .enumerate()
            .find_map(|(i, e)| self.publication.check(e).err().map(|r| (i, r)))
            .map(|(i, r)| {
                let commit = raw[i].commit_lsn;
                let at = candidates.iter().position(|c| c.commit_lsn >= commit).unwrap_or(0);
                (at, r)
            });
        let (refused, refusal, refused_commit) = match refused_at {""",
     RUST_BUILD, "cargo test --lib replication::stream", None),

    ("R4", "a row whose present values are all withheld is emitted as {} instead of refused",
     "src/replication/jsonl.rs",
     """    if written == 0 && !values.is_empty() {""",
     """    if false && written == 0 && !values.is_empty() {""",
     RUST_BUILD, "cargo test --lib replication::jsonl", None),

    ("R6", "a non-identifier name in the declaration is trimmed into a shorter one instead of refused",
     "src/replication/publication.rs",
     """                if !is_identifier(col) {
                    return Err(bad_publication(lineno, &not_an_identifier("column", col)));
                }""",
     """                if false && !is_identifier(col) {
                    return Err(bad_publication(lineno, &not_an_identifier("column", col)));
                }""",
     RUST_BUILD, "cargo test --lib replication::publication", None),

    ("R7", "`#` is honoured mid-line again, which is what let a name be shortened into a real column",
     "src/replication/publication.rs",
     """            let line = raw.trim();""",
     """            let line = match raw.find('#') { Some(at) => &raw[..at], None => raw }.trim();""",
     RUST_BUILD, "cargo test --lib replication::publication", None),
]

GO_MUTANTS = [
    ("G1", "the consumer's forward check (decodeLine)",
     "cdc-consumer/main.go",
     """	if err := activePublication.check(&e, n); err != nil {
		return nil, err
	}""",
     """	if false {
		if err := activePublication.check(&e, n); err != nil {
			return nil, err
		}
	}""",
     "Consumer|Sink|Precision"),

    ("G2", "the consumer's narrower-producer check",
     "cdc-consumer/main.go",
     """	if err := activePublication.checkShape(&e, n); err != nil {
		return nil, err
	}""",
     """	if false {
		if err := activePublication.checkShape(&e, n); err != nil {
			return nil, err
		}
	}""",
     "Shape"),

    ("G3", "the raw-token pass: duplicate keys, keys outside the envelope, non-scalar row values",
     "cdc-consumer/main.go",
     """	if err := activePublication.policeRawLine(line, n); err != nil {
		return nil, err
	}""",
     """	if false {
		if err := activePublication.policeRawLine(line, n); err != nil {
			return nil, err
		}
	}""",
     "Duplicate|Envelope|NonScalar"),

    ("G4", "the before image of a schema op is exempt from the column rule again",
     "cdc-consumer/publication.go",
     """			if isSchema(e.Op) && side.what == "after image" && col == "columns" {
				continue
			}""",
     """			if isSchema(e.Op) {
				continue
			}""",
     "DropsBeforeImage"),

    ("G5", "the type report no longer checks the columns it prints",
     "cdc-consumer/main.go",
     """				if err := activePublication.refuseUnpublished(table, col, "the type report for", i+1); err != nil {
					return err
				}""",
     """				if false {
					if err := activePublication.refuseUnpublished(table, col, "the type report for", i+1); err != nil {
						return err
					}
				}""",
     "Precision"),

    ("G6", "an excluded table is treated as merely unpublished, losing the policy-skew signal",
     "cdc-consumer/publication.go",
     """	if p.Excluded[e.Table] {
		return fmt.Errorf("line %d: publication %q EXCLUDES table %q, so no event for it should be in "+
			"this feed at all; a producer honouring this policy drops them. Seeing one means the two "+
			"ends are running different policies", n, p.Name, e.Table)
	}
	cols, decided := p.published(e.Table)""",
     """	cols, decided := p.published(e.Table)""",
     "ExcludedTable"),

    ("G7", "a non-identifier name in the declaration is accepted by the consumer's parser",
     "cdc-consumer/publication.go",
     """			if !isIdentifier(col) {
				return nil, fmt.Errorf("publication declaration, line %d: %s", lineno, notAnIdentifier("column", col))
			}""",
     """			if false && !isIdentifier(col) {
				return nil, fmt.Errorf("publication declaration, line %d: %s", lineno, notAnIdentifier("column", col))
			}""",
     "ParsePublicationRefuses"),
]


def main():
    dirty, _, _ = run("git diff --quiet && git diff --cached --quiet")
    if dirty != 0:
        raise SystemExit("refusing to run on a dirty tree: this script reverts files with "
                         "`git checkout --`, which would discard uncommitted work.")
    head = run("git rev-parse --short HEAD")[1].strip()
    log(f"B7 fire-checks against {head}")
    log(f"generated by bench/b7_fire_checks.py; re-run it to regenerate")
    log("")
    log("A mutant PASSES the fire-check when the build succeeds AND the tests fail. A mutant that")
    log("does not build proves nothing; a mutant whose tests still pass means nothing tests that")
    log("property.")
    log("")
    survivors = []

    for mid, what, path, old, new, build, test, _ in MUTANTS:
        log(f"=== {mid}: {what}")
        log(f"    file: {path}")
        patch(path, old, new)
        try:
            bcode, bout, bsec = run(build)
            log(f"    build: exit {bcode} ({bsec:.0f}s)")
            if bcode != 0:
                log("    BUILD FAILED - the mutant proves nothing. Last lines:")
                for l in bout.strip().splitlines()[-6:]:
                    log(f"      {l}")
                survivors.append(f"{mid} (did not build)")
                continue
            tcode, tout, tsec = run(test)
            failed = [l for l in tout.splitlines() if " ... FAILED" in l]
            log(f"    tests: `{test}` exit {tcode} ({tsec:.0f}s), {len(failed)} failing")
            for l in failed[:8]:
                log(f"      {l.strip()}")
            if tcode == 0:
                log("    *** MUTANT SURVIVED: no test noticed the property was gone ***")
                survivors.append(mid)
        finally:
            revert(path)
        log("")

    for mid, what, path, old, new, pattern in GO_MUTANTS:
        log(f"=== {mid}: {what}")
        log(f"    file: {path}")
        patch(path, old, new)
        try:
            bcode, bout, bsec = run(f"{GO} build ./...", cwd=f"{ROOT}/cdc-consumer")
            log(f"    build: exit {bcode} ({bsec:.0f}s)")
            if bcode != 0:
                log("    BUILD FAILED - the mutant proves nothing. Last lines:")
                for l in bout.strip().splitlines()[-6:]:
                    log(f"      {l}")
                survivors.append(f"{mid} (did not build)")
                continue
            tcode, tout, tsec = run(f"{GO} test -run '{pattern}' ./...", cwd=f"{ROOT}/cdc-consumer")
            failed = [l for l in tout.splitlines() if l.startswith("--- FAIL")]
            log(f"    tests: `go test -run '{pattern}'` exit {tcode} ({tsec:.0f}s), {len(failed)} failing")
            for l in failed[:8]:
                log(f"      {l.strip()}")
            if tcode == 0:
                log("    *** MUTANT SURVIVED: no test noticed the property was gone ***")
                survivors.append(mid)
        finally:
            revert(path)
        log("")

    log(f"{len(MUTANTS) + len(GO_MUTANTS)} mutants, {len(survivors)} survivors")
    if survivors:
        log("SURVIVORS: " + ", ".join(survivors))
    out = f"{ROOT}/bench/b7_fire_checks.txt"
    with open(out, "w") as f:
        f.write("\n".join(LOG) + "\n")
    print(f"\nwrote {out}")
    # A clean tree afterwards is part of the result: a mutation left behind is a mutation shipped.
    if run("git diff --quiet")[0] != 0:
        raise SystemExit("FAILED: the tree is dirty after the run; a mutation was left behind")
    sys.exit(1 if survivors else 0)


if __name__ == "__main__":
    main()
