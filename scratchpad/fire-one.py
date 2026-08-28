import os, re, subprocess, sys, signal
SRC="src/consensus/replicate.rs"
ENV=dict(os.environ, PATH=os.path.expanduser("~/.cargo/bin")+":"+os.environ["PATH"])
OLD, NEW = sys.argv[1], sys.argv[2]
ORIG=open(SRC).read()
def restore(*a):
    open(SRC,"w").write(ORIG); sys.exit(143)
signal.signal(signal.SIGTERM, restore); signal.signal(signal.SIGINT, restore)
assert ORIG.count(OLD)==1, f"target appears {ORIG.count(OLD)} times, not once"
try:
    open(SRC,"w").write(ORIG.replace(OLD,NEW,1))
    p=subprocess.run(["cargo","test","--lib","consensus::"],env=ENV,capture_output=True,text=True,timeout=900)
    out=p.stdout+p.stderr
    failed=sorted(set(re.findall(r"^\s{4}(consensus::\S+)$", out, re.M)))
    m=re.search(r"test result: (\w+)\. (\d+) passed; (\d+) failed", out)
    print("STATUS:", m.group(0) if m else ("BUILD-FAILED" if "error[" in out else "NO-RESULT"))
    print("VERDICT:", "KILLED" if failed else "SURVIVED")
    for f in failed: print("   -", f.split("::")[-1])
finally:
    open(SRC,"w").write(ORIG)
