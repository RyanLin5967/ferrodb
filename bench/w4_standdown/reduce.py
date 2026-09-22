#!/usr/bin/env python3
"""W4 check 3 — reduce a directory of harness outputs into one table.

Reads the HEADLINE block of every `*.txt` the harness wrote and prints one row per
(file, arm, proto), carrying the shared-today fraction that check 3 turns on.

⚠ Refuses rather than reporting: a file with no HEADLINE block, or a directory with no files,
exits non-zero. A reducer that silently skips an arm turns a failed run into a short table, which
reads exactly like a run that had fewer arms.
"""
import re
import sys
from pathlib import Path

HEAD = "## HEADLINE"
ROW = re.compile(
    r"^(?P<arm>\S+)\s+(?P<proto>\S+)\s+\|\s+"
    r"(?P<sh_att>\d+)\s+(?P<sh_stood>\d+)\s+(?P<sh_pct>\S+)\s+\|\s+"
    r"(?P<dml_att>\d+)\s+(?P<dml_pct>\S+)\s+\|\s+"
    r"(?P<exc_att>\d+)\s+(?P<exc_pct>\S+)\s+\|\s+(?P<all_pct>\S+)\s*$"
)


def parse(path: Path):
    lines = path.read_text().splitlines()
    try:
        start = next(i for i, l in enumerate(lines) if l.startswith(HEAD))
    except StopIteration:
        raise SystemExit(f"REFUSED: {path} has no {HEAD} block -- the run did not finish")
    header = [l for l in lines if l.startswith("# clients=")]
    cfg = header[0] if header else "# clients=?"
    rows = []
    for line in lines[start + 2:]:
        if not line.strip():
            break
        m = ROW.match(line)
        if m:
            rows.append(m.groupdict())
    if not rows:
        raise SystemExit(f"REFUSED: {path} has a {HEAD} block with no rows")
    return cfg, rows


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("usage: reduce.py <file-or-dir> [...]")
    files = []
    for a in sys.argv[1:]:
        p = Path(a)
        files.extend(sorted(p.glob("*.txt")) if p.is_dir() else [p])
    if not files:
        raise SystemExit("REFUSED: no input files -- a run that collected nothing has not passed")
    print(f"{'source':<22} {'arm':<10} {'proto':<12} {'sh_att':>7} {'sh_stood':>9} "
          f"{'SHARED_TODAY':>13} {'dml':>8} {'exclusive':>10}")
    for f in files:
        cfg, rows = parse(f)
        for r in rows:
            print(f"{f.name:<22} {r['arm']:<10} {r['proto']:<12} {r['sh_att']:>7} "
                  f"{r['sh_stood']:>9} {r['sh_pct']:>13} {r['dml_pct']:>8} {r['exc_pct']:>10}")
        print(f"#   {f.name}: {cfg}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
