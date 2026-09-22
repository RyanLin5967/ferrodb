#!/usr/bin/env python3
"""D133 — check the measured rows against the closed form DERIVED FROM THE CODE.

The expected values here are not fitted to the measurement and are not obtained by calling the
subject. They are read off the three facts the code states, and the check is whether the engine
agrees:

  1. One `Reaper::reap` parks P pages, where P is the number of pages the branch wrote before its
     child forked (`arena.rs:1585-1600`: a page is parked iff a live child can see it).
  2. Nothing is ever released while those children live, so `drain_pending_seeded`'s `moved` stays
     false and the fixed-point loop runs exactly ONE pass per reap (`reaper.rs:410`).
  3. `take_pending` is `std::mem::take` of the WHOLE vec (`arena.rs:800`), so that one pass walks
     every entry parked so far, not only this branch's.

  =>  pend_peak(N)     = P * N
      passes(N)        = N
      entry_visits(N)  = sum_{i=1..N} P*i = P * N * (N+1) / 2

If (3) were false -- if the drain walked only the reaped branch's own entries -- `entry_visits`
would be P*N, LINEAR. The quadratic is the whole claim, so it is what this asserts.

Exits non-zero if no rows were found, if any row disagrees, or if the `leaf` control is missing:
a checker that parses nothing passes vacuously, and this project has lost results to exactly that.

Usage: bench/d133_check.py bench/d133_pending_len.txt [--pages 4] [--expect-fail]
"""
import re
import sys

ROW = re.compile(
    r"^\s*(?:P=(?P<p>\d+)\s+)?(?P<shape>fan|chain|leaf)\s+(?P<persist>on|off)\s+"
    r"(?P<n>\d+)\s+(?P<pend_end>\d+)\s+(?P<peak>\d+)\s+(?P<passes>\d+)\s+(?P<visits>\d+)\s+"
    r"(?P<ms>[\d.]+)\s+(?P<per>[\d.]+)\s*$"
)


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    expect_fail = "--expect-fail" in sys.argv
    default_pages = 4
    if "--pages" in sys.argv:
        default_pages = int(sys.argv[sys.argv.index("--pages") + 1])
    if not args:
        print("REFUSED: no artifact given.", file=sys.stderr)
        return 2

    rows, bad, shapes = 0, [], set()
    for line in open(args[0]):
        m = ROW.match(line.rstrip("\n"))
        if not m:
            continue
        rows += 1
        shape = m.group("shape")
        shapes.add(shape)
        n = int(m.group("n"))
        pages = int(m.group("p")) if m.group("p") else default_pages
        # The control: a childless leaf has no live child, so nothing can be parked and nothing
        # can be walked. Expected values here are 0 by the same rule 1, with P effectively 0.
        p = 0 if shape == "leaf" else pages
        want_peak = p * n
        want_passes = n if p else 0
        want_visits = p * n * (n + 1) // 2
        got = (int(m.group("peak")), int(m.group("passes")), int(m.group("visits")))
        want = (want_peak, want_passes, want_visits)
        if got != want:
            bad.append(f"{shape:<5} persist={m.group('persist'):<3} N={n:<5} P={pages}: "
                       f"got peak/passes/visits {got}, closed form says {want}")
        if int(m.group("pend_end")) != int(m.group("peak")):
            bad.append(f"{shape:<5} persist={m.group('persist'):<3} N={n:<5}: pend_end "
                       f"{m.group('pend_end')} != peak {m.group('peak')} — the queue DID drain, "
                       f"which the 'children stay live' premise says it cannot")

    if rows == 0:
        print("REFUSED: parsed zero rows. A checker that reads nothing passes vacuously.",
              file=sys.stderr)
        return 2
    if "leaf" not in shapes:
        print("REFUSED: no `leaf` control row. Without the forced negative the other rows only "
              "show that a counter counts something.", file=sys.stderr)
        return 2

    if expect_fail:
        # Fire-check mode: the caller has planted a wrong row and this must NOTICE.
        if not bad:
            print("FIRE-CHECK FAILED: the checker passed a file it was told disagrees.",
                  file=sys.stderr)
            return 2
        print(f"FIRE-CHECK OK: {len(bad)} disagreement(s) caught:")
        for b in bad:
            print(f"  - {b}")
        return 0

    if bad:
        print(f"DISAGREEMENT — {len(bad)} row(s) do not match the closed form:", file=sys.stderr)
        for b in bad:
            print(f"  - {b}", file=sys.stderr)
        return 1
    print(f"OK — all {rows} rows (shapes: {', '.join(sorted(shapes))}) match the closed form "
          f"pend=P*N, passes=N, visits=P*N*(N+1)/2 EXACTLY, and no row's queue ever drained.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
