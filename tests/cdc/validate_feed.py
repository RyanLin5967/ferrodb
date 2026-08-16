#!/usr/bin/env python3
"""Validate a ferrodb JSON Lines change feed with a parser that shares no code with the producer.

Written against the JSON spec and the documented envelope, deliberately NOT against
src/replication/jsonl.rs. An encoder checked with its own decoder agrees with itself about a
shared misreading; that is the same reason tests/pg/pg_client.py exists.

Usage: validate_feed.py <feed.jsonl>
Exits non-zero on any violation. On success prints "OK <lines>" then one "ROW <json>" per record,
with sorted keys, so the caller can compare content it already knows.
"""
import json
import sys


def reject_constant(tok):
    # Python's json ACCEPTS bare NaN/Infinity by default, which would let exactly the bug this
    # feed is designed to avoid slip through unnoticed. Refuse them so the check is strict.
    raise ValueError(f"non-standard JSON token {tok!r}: a strict parser would reject this document")


def main():
    if len(sys.argv) != 2:
        print("usage: validate_feed.py <feed.jsonl>", file=sys.stderr)
        return 2
    path = sys.argv[1]
    with open(path, "rb") as fh:
        raw = fh.read()

    if not raw:
        print("feed is empty; a feed that collected nothing has not passed", file=sys.stderr)
        return 1
    if not raw.endswith(b"\n"):
        print("feed does not end with a newline; the last record may be truncated", file=sys.stderr)
        return 1

    # Must be valid UTF-8. JSON is a Unicode format and a consumer will decode it as such.
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        print(f"feed is not valid UTF-8: {e}", file=sys.stderr)
        return 1

    lines = text.split("\n")[:-1]
    if not lines:
        print("no records", file=sys.stderr)
        return 1

    required = {"table", "op", "txn", "lsn", "commit_lsn", "commit_end_lsn", "before", "after"}
    rows = []
    for i, line in enumerate(lines, 1):
        try:
            obj = json.loads(line, parse_constant=reject_constant)
        except Exception as e:
            print(f"line {i} is not valid JSON: {e}\n  {line[:200]}", file=sys.stderr)
            return 1
        if not isinstance(obj, dict):
            print(f"line {i} is not a JSON object", file=sys.stderr)
            return 1
        missing = required - set(obj)
        if missing:
            print(f"line {i} is missing {sorted(missing)}", file=sys.stderr)
            return 1
        op = obj["op"]
        if op not in ("INSERT", "UPDATE", "DELETE"):
            print(f"line {i} has unknown op {op!r}", file=sys.stderr)
            return 1
        # before/after presence must agree with op, or a consumer cannot branch on op alone.
        if op == "INSERT" and obj["before"] is not None:
            print(f"line {i}: INSERT carries a before image", file=sys.stderr)
            return 1
        if op == "DELETE" and obj["after"] is not None:
            print(f"line {i}: DELETE carries an after image", file=sys.stderr)
            return 1
        if op == "UPDATE" and (obj["before"] is None or obj["after"] is None):
            print(f"line {i}: UPDATE is missing one of its images", file=sys.stderr)
            return 1
        for side in ("before", "after"):
            if obj[side] is not None and not isinstance(obj[side], dict):
                print(f"line {i}: {side} is not an object", file=sys.stderr)
                return 1
        for k in ("txn", "lsn", "commit_lsn", "commit_end_lsn"):
            if not isinstance(obj[k], int):
                print(f"line {i}: {k} is not an integer", file=sys.stderr)
                return 1
        if obj["commit_end_lsn"] <= obj["commit_lsn"]:
            print(f"line {i}: commit_end_lsn is not past commit_lsn", file=sys.stderr)
            return 1
        rows.append(obj)

    # Commit order, checked independently of the producer's claim to provide it.
    last = 0
    for i, obj in enumerate(rows, 1):
        if obj["commit_lsn"] < last:
            print(f"line {i}: commit_lsn went backwards", file=sys.stderr)
            return 1
        last = obj["commit_lsn"]

    print(f"OK {len(rows)}")
    for obj in rows:
        print("ROW " + json.dumps(obj, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
