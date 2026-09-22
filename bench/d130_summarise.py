#!/usr/bin/env python3
"""Turn a d130_run.sh raw artifact into the two f/sync ÷ T tables D130 pre-registered.

    bench/d130_summarise.py bench/d130_batch_vs_threads.txt

Why this exists rather than arithmetic in a report: `f/sync ÷ T` is the discriminator the row
turns on, and a column divided by hand is a number with no instrument. This reads the harnesses'
own printed columns and divides them, so the table in the artifact and the table in the report
come from the same place.

It REFUSES rather than printing a partial table:
  - if either mode contributed no rows (a summariser that silently finds nothing prints a clean
    empty table, which reads exactly like a run that had nothing to say);
  - if a rep is missing any T the other reps had, since a ratio compared across an incomplete
    sweep is what makes a saturating curve look like a rising one.
"""
import sys
from collections import defaultdict

def numbers(line):
    """The whitespace-separated tokens of `line`, as floats, or None if any token is not one.

    Positional-from-the-right rather than a regex over the whole row: the pgwire arm label
    contains spaces and a comma, and a lazy `.*?` in front of the numeric columns can swallow a
    leading digit and shift every field by one. Tokenising cannot do that.
    """
    toks = line.split()
    out = []
    for t in toks:
        try:
            out.append(float(t))
        except ValueError:
            return None
    return out


def main(path):
    mode = None
    rep = 0
    # (mode_key, T) -> list of (forks, syncs)
    cells = defaultdict(list)

    for line in open(path, encoding="utf-8", errors="replace"):
        # A mode starts ONLY on the RUN SCRIPT's banner, which sits at column 0. The pgwire binary
        # prints its own header containing the string "MODE=pgwire", indented, and the run script
        # emits a PROOF PASS before either sweep -- so a substring test would fold the proof
        # pass's rows into the evidence and nothing downstream could tell. Column 0 is what
        # separates the script's frame from the binaries' own output.
        if line.startswith("PROOF PASS"):
            mode = None
            continue
        if line.startswith("MODE=direct"):
            mode, rep = "direct", 0
            continue
        if line.startswith("MODE=pgwire"):
            mode, rep = "pgwire", 0
            continue
        if line.startswith("--- direct rep") or line.startswith("--- pgwire rep"):
            rep += 1
            continue
        if mode == "direct":
            # threads forks seconds forks/sec per-fork-ms fsyncs forks/fsync
            n = numbers(line)
            if n is not None and len(n) == 7:
                cells[("direct", int(n[0]))].append((int(n[1]), int(n[5])))
        elif mode == "pgwire":
            stripped = line.strip()
            if not stripped or stripped[0] not in "PSD" or not stripped[1:2].isspace():
                continue
            # <arm label>  T forks syncs f/sync f/sync÷T -- the last five are the numbers.
            n = numbers(" ".join(stripped.split()[-5:]))
            if n is None or len(n) != 5:
                continue
            key = {
                "P": "pgwire P (conn per fork, BEGIN only)",
                "S": "pgwire S (persistent conn, BEGIN+ABANDON)",
                "D": "pgwire D (POSITIVE CONTROL, no socket)",
            }[stripped[0]]
            cells[(key, int(n[0]))].append((int(n[1]), int(n[2])))

    if not cells:
        sys.exit("d130_summarise: REFUSING — parsed no rows from %s. An empty table is not a "
                 "result about the sweep, it is a result about this parser." % path)

    keys = sorted({k for k, _ in cells}, key=lambda s: (s != "direct", s))
    for key in keys:
        ts = sorted(t for k, t in cells if k == key)
        if not ts:
            continue
        reps = {len(cells[(key, t)]) for t in ts}
        if len(reps) != 1:
            sys.exit("d130_summarise: REFUSING — %s has %s reps across its T values. A ratio "
                     "compared across an incomplete sweep makes a saturating curve look like a "
                     "rising one." % (key, sorted(reps)))
        nrep = reps.pop()
        print()
        print("MODE=%s   (%d rep%s per cell)" % (key, nrep, "" if nrep == 1 else "s"))
        # The growth column is named after the T it is actually divided by, not after the T the
        # sweep was SUPPOSED to start at. A smoke run without T=1 would otherwise print a column
        # headed "vs T=1" whose base is T=64.
        print("      T     forks    syncs    f/sync   f/sync÷T   f/sync vs T=%d" % ts[0])
        base = None
        for t in ts:
            forks = sum(f for f, _ in cells[(key, t)])
            syncs = sum(s for _, s in cells[(key, t)])
            if syncs == 0:
                sys.exit("d130_summarise: REFUSING — %s T=%d issued 0 fsyncs. A denominator of "
                         "zero is not a large batch." % (key, t))
            per = forks / syncs
            if base is None:
                base = per
            print("  %5d   %7d   %6d   %7.2f   %8.4f   %12.2fx"
                  % (t, forks, syncs, per, per / t, per / base))

    print()
    print("READ IT THIS WAY — the three outcomes D130 pre-registered, in its own words:")
    print("  1. `f/sync ÷ T` ≈ 0.5, flat in T  ⇒ the wake-up race is real.")
    print("  2. `f/sync ÷ T` FALLS as T rises  ⇒ the batch is bounded by something that is not")
    print("     the thread count — arrival, or an fsync that is not the serialization point at")
    print("     all. The `T/2` reading is wrong and this row shrinks to a note.")
    print("  3. `f/sync ÷ T` ≈ 1.0 at some T   ⇒ the race explanation is falsified outright.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
