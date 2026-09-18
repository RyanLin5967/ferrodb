#!/usr/bin/env python3
"""Assemble the post-compaction handoff for ferrodb, MOST-CRITICAL-FIRST, under a byte budget.

WHY THIS EXISTS (2026-09-17). ferrodb had a PreCompact hook and nothing else. It banked both
halves of the handoff to disk correctly -- and then the compaction happened, a fresh context
started, and NOTHING TOLD IT THE FILES EXISTED. Five tool calls went by before Ryan asked
"did the hook actually work? it should stop you from doing literally everything else, until
you have read the full handoff." He was right. A hook that writes a perfect handoff no one
reads has done nothing: BANKING IS NOT DELIVERY, AND DELIVERY IS NOT A READ.

Two rules inherited from ~/projects/dbresearch, both of which it paid for:

  1. THE HARNESS TRUNCATES A LARGE HOOK PAYLOAD. There, 64 of 66 KB never reached the session
     while the hook's own text claimed it was "injected in full". So this script budgets ITSELF,
     emits most-critical-first, and prints a truncation ledger naming whatever did not fit.
  2. ORDER IS NOT COSMETIC. That same hook printed Ryan's words OLDEST-FIRST, so the surviving
     2 KB served three-day-old instructions as current. Verbatim words here are NEWEST FIRST.

The untrimmed payload goes to .sessionstart_full.txt with five proof tokens spread through it.
The tokens exist ONLY in that file; the sentinel beside it holds their hashes. That is what
makes pretooluse_handoff_read.sh an OUTCOME test rather than a wording test -- see its header.
"""
import hashlib, io, os, re, secrets, subprocess, sys, datetime, glob

R = "/Users/idide/projects/ferrodb"
A = "/Users/idide/wt/artie-research"
HOOKS = os.path.join(R, ".claude/hooks")
FULL = os.path.join(HOOKS, ".sessionstart_full.txt")
SENTINEL = os.path.join(HOOKS, ".sessionstart_sentinel")
BUDGET = 11000                      # stdout bytes. Measured ceiling elsewhere: ~15 KB inline.

def sh(cmd, timeout=20):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                              timeout=timeout).stdout.strip()
    except Exception as e:
        return "(failed: %s)" % e

def read(path, limit=None):
    try:
        with io.open(path, encoding="utf-8", errors="replace") as f:
            t = f.read()
        return t[:limit] if limit else t
    except Exception:
        return ""

def section(title, body, priority):
    return {"title": title, "body": body.rstrip(), "priority": priority}

# ---------------------------------------------------------------------------------------
# 1. THE CONSTRAINTS. Tiny, and first, because they are the ones whose violation is
#    irreversible. Everything else on this page is recoverable work.
# ---------------------------------------------------------------------------------------
CONSTRAINTS = """\
  - NEVER force-push, anywhere, for any reason. Not once.
  - push/merge to main is authorised ON THIS REPO ONLY, and ONLY behind a verified green suite.
  - NEVER edit a test to make it pass. A failing test means the code is wrong.
  - NEVER `git add -A` or `git add .` -- stage by explicit file list.
  - Bound every long command: `timeout 900 cargo test`, `timeout 600 cargo build`.
  - Never write into / merge / checkout / rebase a worktree while a run against it is in flight.
  - DO NOT read /Users/idide/wt/artie-research/research/ -- 350 KB; an agent already died on it.
  - export PATH="$HOME/.cargo/bin:$PATH" in every shell. Worktrees via ~/.claude/bin/wt new.
  - Deflate every idea BEFORE presenting it: prior art, "isn't this just X", is the premise even
    true, does the system already do it. Twelve inflated pitches were killed by Ryan, not by me."""

# ---------------------------------------------------------------------------------------
# 2. WHERE THE WORK IS -- live, not as banked, because the tree may have moved since.
# ---------------------------------------------------------------------------------------
def where():
    out = []
    out.append("  branch: %s" % sh("git -C %s rev-parse --abbrev-ref HEAD" % R))
    out.append("  HEAD:")
    out.append(re.sub(r"(?m)^", "    ", sh("git -C %s log --oneline -6" % R)))
    dirty = sh("git -C %s status --porcelain" % R)
    out.append("  uncommitted (== unbanked == at risk): %s"
               % (("\n" + re.sub(r"(?m)^", "    ", dirty)) if dirty else "clean"))
    return "\n".join(out)

# ---------------------------------------------------------------------------------------
# 3. THE OPEN LEDGER ROWS. This is the single next action, and it is the thing a summary
#    loses first because it lives in another repo entirely.
# ---------------------------------------------------------------------------------------
def ledger():
    txt = read(os.path.join(A, "SCALE-LEDGER.md"))
    rows = [l for l in txt.splitlines() if l.startswith("|") and "OPEN" in l]
    if not rows:
        return "  (no OPEN rows found in SCALE-LEDGER.md -- verify by reading it; do not assume done)"
    out = []
    for l in rows:
        cells = [c.strip() for c in l.strip("|").split("|")]
        rid = cells[0] if cells else "?"
        wall = re.sub(r"\s+", " ", cells[1])[:600] if len(cells) > 1 else ""
        exit_c = re.sub(r"\s+", " ", cells[3])[:600] if len(cells) > 3 else ""
        out.append("  [%s] %s" % (rid, wall))
        if exit_c:
            out.append("        EXIT: %s" % exit_c)
    return "\n".join(out)

# ---------------------------------------------------------------------------------------
# 4. WHAT IS STILL LINEAR. The objective is 10^6 branches; linear is a DEFECT here, not a
#    baseline. Computed live, so it cannot go stale the way a written list does.
# ---------------------------------------------------------------------------------------
def linear():
    hits = sh("grep -rn 'all_records()\\|live_branches()\\|all_branches()' %s/src --include='*.rs' "
              "| grep -v 'fn all_records\\|fn live_branches\\|fn all_branches' "
              "| grep -v '/tests\\.rs\\|#\\[cfg(test)\\]'" % R, timeout=30)
    if not hits:
        return "  (grep found no O(N) catalog dumps -- if that is a surprise, the grep is wrong,\n"\
               "   not the world. Widen it before believing it.)"
    out = []
    for l in hits.splitlines()[:14]:
        out.append("  " + l.replace(R + "/", "")[:150])
    out.append("")
    out.append("  Any O(N) walk that runs REPEATEDLY -- per sweep, per fork, per statement -- is a")
    out.append("  bug at 10^6 even when it is fast today. reap_expired cloned EVERY record every")
    out.append("  30s to find the handful that expired; ordered by deadline that is one descent.")
    return "\n".join(out)

# ---------------------------------------------------------------------------------------
# 5. DESIGN DECISIONS AND THEIR FALSIFIERS. A decision with no falsifier is an opinion.
# ---------------------------------------------------------------------------------------
def design():
    txt = read(os.path.join(A, "SCALE-DESIGN.md"))
    if not txt:
        return "  (SCALE-DESIGN.md unreadable -- that is a blocker, it is the decision record)"
    keep = []
    for para in txt.split("\n\n"):
        if re.search(r"(?i)^#+ *D\d|decision|falsif|rejected", para):
            keep.append(re.sub(r"\s+", " ", para).strip()[:700])
    return "\n".join("  - " + k for k in keep[:10]) or "  (no decision paragraphs matched)"

# ---------------------------------------------------------------------------------------
# 6-8. From the banked prose half: Ryan's words NEWEST FIRST, the last tool calls, live agents.
# ---------------------------------------------------------------------------------------
def prose_parts():
    cands = sorted(glob.glob(os.path.join(R, "COMPACTION_HANDOFF_*_AUTO.md")))
    cands = [c for c in cands if ".STALE-" not in c]
    if not cands:
        return ("  (NO BANKED PROSE HALF EXISTS. The PreCompact extractor never ran or refused.)",
                "", "")
    txt = read(cands[-1])
    if txt.lstrip().startswith("# REFUSED"):
        head = "\n".join("  " + l for l in txt.splitlines()[:12])
        return ("  THE PROSE EXTRACTOR REFUSED FOR THIS COMPACTION. Its banner:\n" + head, "", "")
    msgs = re.findall(r"(?m)^### (\d{4}-\d\d-\d\dT[\d:]+)\n((?:^> .*\n)+)", txt)
    words = []
    for ts, block in reversed(msgs[-14:]):                 # NEWEST FIRST -- see module docstring
        body = re.sub(r"(?m)^> ?", "", block).strip()
        words.append("  [%s] %s" % (ts, re.sub(r"\s+", " ", body)[:420]))
    tools = [l for l in txt.splitlines() if l.startswith("| 20")]
    calls = "\n".join("  " + re.sub(r"\s+", " ", l)[:150] for l in tools[-12:])
    ag = re.findall(r"(?m)^- `[^`]+`.*$", txt)
    agents = "\n".join("  " + a[:150] for a in ag[-8:]) or "  (none recorded in this slice)"
    return ("\n".join(words) or "  (no verbatim messages extracted)", calls, agents)

words, calls, agents = prose_parts()

SECTIONS = [
    section("1. CONSTRAINTS THAT ARE NEVER RELAXED", CONSTRAINTS, 0),
    section("2. WHERE THE WORK IS RIGHT NOW (live git, not banked)", where(), 1),
    section("3. OPEN LEDGER ROWS -- the single next action", ledger(), 2),
    section("4. WHAT IS STILL LINEAR (the objective is 10^6 branches)", linear(), 3),
    section("5. RYAN'S OWN WORDS -- NEWEST FIRST, verbatim", words, 4),
    section("6. DESIGN DECISIONS + FALSIFIERS", design(), 5),
    section("7. THE LAST TOOL CALLS BEFORE THE CUT", calls, 6),
    section("8. AGENTS DISPATCHED IN THAT SLICE -- any may still be LIVE", agents, 7),
]

# --- the full payload, with five proof tokens spread through it -------------------------
tokens = ["HANDOFF-READ-PROOF-" + secrets.token_hex(16) for _ in range(5)]
full = [u"ferrodb POST-COMPACTION HANDOFF -- FULL PAYLOAD, %s"
        % datetime.datetime.now().isoformat(timespec="seconds"),
        u"Read this WHOLE file. Five proof tokens are spread through it; the gate needs all five,",
        u"so a head or a tail of this file cannot clear it. That is deliberate.", u""]
per = max(1, len(SECTIONS) // len(tokens))
ti = 0
for i, s in enumerate(SECTIONS):
    full.append(u"=" * 78)
    full.append(u"## " + s["title"])
    full.append(u"=" * 78)
    full.append(s["body"])
    full.append(u"")
    if ti < len(tokens) and (i % per == 0 or i == len(SECTIONS) - 1):
        full.append(u"    [checkpoint %d/5] %s" % (ti + 1, tokens[ti])); full.append(u"")
        ti += 1
while ti < len(tokens):
    full.append(u"    [checkpoint %d/5] %s" % (ti + 1, tokens[ti])); ti += 1
io.open(FULL, "w", encoding="utf-8").write(u"\n".join(full) + u"\n")
io.open(SENTINEL, "w", encoding="utf-8").write(
    u"\n".join(hashlib.sha256(t.encode()).hexdigest() for t in tokens) + u"\n")

# --- budgeted stdout, most-critical-first, with a truncation ledger ----------------------
emitted, dropped, used = [], [], 0
header = ("=" * 78 + "\n"
          "ferrodb SESSION START -- A COMPACTION OR RESUME JUST HAPPENED.\n"
          "This is a BUDGETED DIGEST (%d B). It is NOT the handoff.\n"
          "THE FULL HANDOFF IS ON DISK AND MUST BE READ BEFORE ANY OTHER WORK:\n"
          "    cat %s\n"
          "Every tool call is REFUSED until all five of its proof tokens reach the transcript.\n"
          "A pointer to a file is not a read -- that is the failure this exists to prevent.\n"
          + "=" * 78 + "\n") % (BUDGET, FULL)
used += len(header)
for s in sorted(SECTIONS, key=lambda x: x["priority"]):
    block = "\n## %s\n%s\n" % (s["title"], s["body"])
    if used + len(block) > BUDGET:
        dropped.append(s["title"]); continue
    emitted.append(block); used += len(block)
sys.stdout.write(header)
sys.stdout.write("".join(emitted))
if dropped:
    sys.stdout.write("\n## TRUNCATED -- these did NOT fit in the digest and exist ONLY in %s:\n"
                     % FULL)
    for d in dropped:
        sys.stdout.write("  - %s\n" % d)
sys.stdout.write("\nRead %s NOW, in full, before any other tool call.\n" % FULL)
