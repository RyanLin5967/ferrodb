#!/usr/bin/env python3
"""Write the DATED, prose-bearing half of the compaction handoff, automatically.

WHY THIS EXISTS (2026-09-11): precompact_bank_state.sh banked the MACHINE half itself and
left a sentence asking the session to write the PROSE half. That ask is delivered at the
exact moment the context is being destroyed and nothing enforces it, so on 2026-09-11 it
was not done and SessionStart fell back to a handoff two days old. This project's own
record already names that failure: A REQUIRED-JUSTIFICATION FIELD IS NOT A GATE; A GATE IS
ONE THAT RUNS. Three prose items are fully machine-extractable and they are the three that
a summary loses first:
  1. RYAN'S OWN WORDS, verbatim  -- the standing rule is to search the candidate's own
     words, never a scanner's paraphrase, and a summary is a paraphrase.
  2. THE LAST TOOL CALLS         -- the single next action is reconstructible from these
     and from nothing else; an in-memory plan dies with the session.
  3. AGENTS DISPATCHED THIS SESSION -- a live agent invisible to the next session is lost
     work; 133 lost-work agents is the banked figure.
It REFUSES (exit 2, loud banner in the file) rather than writing a quiet empty handoff:
a run that collected nothing has not passed.
"""
import io, json, os, re, sys, datetime

ROOT = "/Users/idide/projects/ferrodb"
TAIL_BYTES = 40_000_000          # transcripts here reach 328 MB; never parse the whole file
MAX_USER_MSGS = 40
MAX_TOOLCALLS = 40

def main():
    tp = sys.argv[1] if len(sys.argv) > 1 else ""
    today = datetime.date.today().isoformat()
    out = os.path.join(ROOT, "COMPACTION_HANDOFF_%s_AUTO.md" % today)
    src = sys.argv[2] if len(sys.argv) > 2 else "unknown"

    def refuse(why):
        """A refusal must REPLACE the handoff file, never append to it.

        MEASURED 2026-09-17 13:08:06, on this hook's FIRST REAL FIRING. The old code appended
        its banner, so the file kept the header of an earlier successful run --
        "# HANDOFF 2026-09-17 -- THE PROSE HALF" -- while the refusal sat 11 KB below it. The
        top of a file is what a reader reads. A stale handoff advertising itself as current is
        strictly WORSE than no handoff: the next session acts on six-minute-old tool calls
        believing they are live. So rotate the stale file out from under the canonical name and
        leave that name holding nothing but the refusal.
        """
        stamp = datetime.datetime.now().isoformat(timespec="seconds")
        rotated = None
        if os.path.exists(out) and os.path.getsize(out) > 0:
            rotated = out[:-3] + ".STALE-" + stamp.replace(":", "") + ".md"
            try:
                os.rename(out, rotated)
            except Exception:
                rotated = None
        with io.open(out, "w", encoding="utf-8") as f:
            f.write(u"# REFUSED %s - NO PROSE HANDOFF WAS EXTRACTED FOR THIS COMPACTION.\n" % stamp)
            f.write(u"# %s\n" % why)
            f.write(u"# transcript source, as resolved by the shell hook: %s\n" % src)
            f.write(u"#\n# THIS IS A BLOCKER, NOT A CLEAN RUN. The verbatim words, the last tool\n")
            f.write(u"# calls and the live agents for THIS compaction do not exist anywhere else.\n")
            f.write(u"# Reconstruct them from the transcript by hand THIS TURN, before acting.\n")
            if rotated:
                f.write(u"#\n# An EARLIER handoff was rotated out of the way to:\n#   %s\n" % rotated)
                f.write(u"# It describes a DIFFERENT compaction. Do not read it as current state.\n")
            f.write(u"# Raw payload this hook received: %s\n"
                    % os.path.join(ROOT, ".claude/hooks/.last_precompact_payload.json"))
        return 2

    if not tp or not os.path.exists(tp):
        return refuse("No usable transcript path reached the extractor (arg was %r)." % tp)

    size = os.path.getsize(tp)
    with open(tp, "rb") as f:
        if size > TAIL_BYTES:
            f.seek(size - TAIL_BYTES)
            f.readline()                      # discard the partial first line
        blob = f.read().decode("utf-8", "replace")

    rows = []
    for ln in blob.splitlines():
        ln = ln.strip()
        if not ln.startswith("{"):
            continue
        try:
            rows.append(json.loads(ln))
        except Exception:
            pass

    def ts(d):
        return (d.get("timestamp") or "")[:19]

    SKIP = ("<system-reminder", "<local-command", "<command-", "[SYSTEM NOTIFICATION",
            "<task-notification", "<bash-stdout", "<bash-stderr", "Another Claude session sent",
            "This session is being continued from a previous conversation")
    users, tools, agents = [], [], []
    for d in rows:
        t = d.get("type")
        if t == "user" and not d.get("isMeta"):
            c = (d.get("message") or {}).get("content")
            if isinstance(c, str):
                txt = c
            elif isinstance(c, list):
                txt = " ".join(b.get("text", "") for b in c
                               if isinstance(b, dict) and b.get("type") == "text")
            else:
                txt = ""
            txt = txt.strip()
            if txt and not any(txt.startswith(s) for s in SKIP):
                users.append((ts(d), txt))
        elif t == "assistant":
            c = (d.get("message") or {}).get("content") or []
            if not isinstance(c, list):
                continue
            for b in c:
                if isinstance(b, dict) and b.get("type") == "tool_use":
                    inp = b.get("input") or {}
                    lbl = (inp.get("description") or inp.get("command")
                           or inp.get("file_path") or inp.get("prompt") or "")
                    lbl = re.sub(r"\s+", " ", str(lbl))[:170]
                    tools.append((ts(d), b.get("name"), lbl))
                    if b.get("name") == "Agent":
                        agents.append((ts(d), inp.get("name") or "(unnamed)",
                                       re.sub(r"\s+", " ", str(inp.get("description") or ""))[:90]))

    # FORCED-FIRE DISCIPLINE: an empty extraction is a refusal, never a quiet clean handoff.
    if not users and not tools:
        return refuse("Parsed %d transcript rows and extracted ZERO user messages AND ZERO "
                      "tool calls. That is an instrument failure, not a quiet session." % len(rows))

    # ⛔ A HOLE CLOSED THE SAME HOUR IT WAS OPENED: SessionStart picks the handoff by `ls -1t`,
    # so this regenerated file would outrank a HAND-WRITTEN handoff for the same day every time,
    # hiding exactly the judgement half it cannot produce. Name the hand-written one at the top.
    hand = sorted(f for f in os.listdir(ROOT)
                  if f.startswith("COMPACTION_HANDOFF_") and f.endswith(".md")
                  and "_AUTO" not in f)
    hand_today = [f for f in hand if today in f]

    L = []
    A = L.append
    A("# ██ HANDOFF %s — THE PROSE HALF, WRITTEN BY THE PreCompact HOOK, NOT BY THE SESSION." % today)
    if hand_today:
        A("#")
        A("# ⛔ READ THE HAND-WRITTEN HANDOFF FIRST — IT HOLDS THE JUDGEMENT THIS FILE CANNOT EXTRACT:")
        for f in hand_today:
            A("#     %s" % os.path.join(ROOT, f))
        A("# This file is EXTRACTED and is regenerated on every compaction, so it will always look")
        A("# newer than that one. Newer is not better here.")
    elif hand:
        A("#")
        A("# ⚠ NO HAND-WRITTEN HANDOFF FOR TODAY. The newest is %s -- treat every live-state claim" % hand[-1])
        A("# in it as expired, and write today's judgement half.")
    A("# Generated %s from the last %d MB of %s (%d rows parsed)."
      % (datetime.datetime.now().isoformat(timespec="seconds"), TAIL_BYTES // 1_000_000,
         os.path.basename(tp), len(rows)))
    A("# Transcript resolved via: %s" % src)
    if "GUESS" in src:
        A("# \u26d4 THAT SOURCE IS A GUESS. The hook could not get a transcript path from its own")
        A("# \u26d4 payload and fell back to the newest recently-written transcript for this")
        A("# \u26d4 project. If another ferrodb session was live, THESE ARE ITS WORDS, NOT YOURS.")
    A("# ⚠ This is EXTRACTED, not summarised. It carries no judgement and no state claims.")
    A("# The machine half is COMPACTION_STATE_AUTO.md. Read both.")
    A("")
    A("## ⛔ RYAN'S OWN WORDS — the last %d real user messages, VERBATIM." % min(len(users), MAX_USER_MSGS))
    A("## Search the words themselves, never a summary of them: a summary is a paraphrase,")
    A("## and this project has twice manufactured false alarms from a scanner's paraphrase.")
    A("")
    for t, x in users[-MAX_USER_MSGS:]:
        A("### %s" % t)
        for line in x.splitlines():
            A("> " + line)
        A("")
    A("## THE LAST %d TOOL CALLS — where the session actually was when it was cut."
      % min(len(tools), MAX_TOOLCALLS))
    A("## An in-memory plan dies with the session; this is what survives of it.")
    A("")
    A("| when | tool | what |")
    A("|---|---|---|")
    for t, n, lbl in tools[-MAX_TOOLCALLS:]:
        A("| %s | %s | %s |" % (t, n, lbl.replace("|", "\\|")))
    A("")
    A("## AGENTS DISPATCHED IN THIS SLICE — %d. Any of these may still be LIVE." % len(agents))
    A("## A live agent invisible to the next session is lost work; call ListAgents before deciding.")
    A("")
    for t, nm, desc in agents[-30:]:
        A("- `%s`  %s — %s" % (t, nm, desc))
    if not agents:
        A("- (none in this slice)")
    A("")

    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    print("precompact_prose: wrote %s (%d user msgs, %d tool calls, %d agents)"
          % (os.path.basename(out), len(users), len(tools), len(agents)))
    return 0

if __name__ == "__main__":
    sys.exit(main())
