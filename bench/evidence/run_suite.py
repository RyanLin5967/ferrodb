#!/usr/bin/env python3
"""Run the suite one target at a time, recording each result durably.

Per-target rather than one `cargo test`: a single whole-suite run that is interrupted reports as a
pass, and a partial run is indistinguishable from a complete one afterwards. Each target's own
`test result:` line is written here as it lands, so an interrupted run is visibly incomplete.
"""
import json, os, re, subprocess, sys

REPO = "/Users/idide/wt/ferrodb-E79b-prompt-hash"
os.chdir(REPO)
ENV = dict(os.environ, PATH=os.path.expanduser("~/.cargo/bin") + ":" + os.environ["PATH"])
OUT = "scratchpad/E79b-suite.txt"

meta = json.loads(subprocess.run(
    ["cargo","metadata","--no-deps","--format-version","1"],
    capture_output=True, text=True, env=ENV, check=True).stdout)
targets = [("--lib", None)]
for t in meta["packages"][0]["targets"]:
    if "test" in t["kind"]:
        targets.append(("--test", t["name"]))

out = open(OUT, "w")
def say(m):
    print(m, flush=True); out.write(m + "\n"); out.flush()

say(f"# per-target suite run, {len(targets)} targets")
tot_pass = tot_fail = tot_ign = 0
bad = []
for kind, name in targets:
    cmd = ["cargo","test",kind] + ([name] if name else []) + ["--no-fail-fast"]
    label = name or "lib"
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, env=ENV, timeout=1800)
    except subprocess.TimeoutExpired:
        say(f"{label:50s} TIMEOUT"); bad.append(label); continue
    text = r.stdout + r.stderr
    lines = [l.strip() for l in text.splitlines() if l.startswith("test result:")]
    if not lines:
        say(f"{label:50s} NO RESULT LINE (a target that collected nothing has not passed)")
        bad.append(label); continue
    p = f = i = 0
    for l in lines:
        m = re.search(r"(\d+) passed; (\d+) failed; (\d+) ignored", l)
        if m: p += int(m[1]); f += int(m[2]); i += int(m[3])
    tot_pass += p; tot_fail += f; tot_ign += i
    status = "ok" if f == 0 and r.returncode == 0 else "FAILED"
    if status == "FAILED":
        bad.append(label)
        for l in text.splitlines():
            if l.startswith("test ") and l.rstrip().endswith("FAILED"):
                say(f"      {l.strip()}")
    say(f"{label:50s} {status:7s} {p:4d} passed  {f:3d} failed  {i:3d} ignored")

say(f"\nTOTAL: {tot_pass} passed, {tot_fail} failed, {tot_ign} ignored across {len(targets)} targets")
say("FAILING TARGETS: " + (", ".join(bad) if bad else "none"))
out.close()
sys.exit(1 if bad else 0)
