import re, sys
raw = open('/Users/idide/wt/ferrodb-D81-fsm-append/bench/d81_curve_raw2.txt').read()
arms = {}
cur = None
for line in raw.splitlines():
    m = re.match(r'^ARM: (\S+ \S+)\s', line)
    if m:
        cur = m.group(1); arms[cur] = {}; continue
    m = re.match(r'^\s+(\d+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s*$', line)
    if m and cur:
        L=int(m.group(1))
        arms[cur][L] = dict(med=float(m.group(3)), mapb=int(m.group(6)), reps=int(m.group(7)),
                            apps=int(m.group(8)), fsy=int(m.group(9)), kb=int(m.group(10)),
                            parkms=int(m.group(11)), usp=float(m.group(12)))
Ls=[500,1000,2000,4000]
def mean(a,b): return (a+b)/2

print("parsed arms:", sorted(arms))
for cell in ["BEFORE/OFF","BEFORE/ON","AFTER/OFF","AFTER/ON"]:
    r1=arms["R1 "+cell]; r2=arms["R2 "+cell]
    print(f"{cell:12s} med  mean:", [round(mean(r1[L]['med'],r2[L]['med']),1) for L in Ls])
    print(f"{cell:12s} usp  mean:", [round(mean(r1[L]['usp'],r2[L]['usp'])) for L in Ls])
print()
for tag in ["BEFORE","AFTER"]:
    on=[mean(arms["R1 "+tag+"/ON"][L]['med'],arms["R2 "+tag+"/ON"][L]['med']) for L in Ls]
    off=[mean(arms["R1 "+tag+"/OFF"][L]['med'],arms["R2 "+tag+"/OFF"][L]['med']) for L in Ls]
    print(tag,"cycle ON/OFF:", [round(a/b,2) for a,b in zip(on,off)])
    on=[mean(arms["R1 "+tag+"/ON"][L]['usp'],arms["R2 "+tag+"/ON"][L]['usp']) for L in Ls]
    off=[mean(arms["R1 "+tag+"/OFF"][L]['usp'],arms["R2 "+tag+"/OFF"][L]['usp']) for L in Ls]
    print(tag,"park  ON/OFF:", [round(a/b,2) for a,b in zip(on,off)])
print()
for tag in ["BEFORE","AFTER"]:
    a=arms["R1 "+tag+"/ON"]
    print(tag, "claims        :", [L+120 for L in Ls])
    print(tag, "fsyncs/claim  :", [round(a[L]['fsy']/(L+120),3) for L in Ls])
    print(tag, "bytes/claim   :", [round(a[L]['kb']*1024/(L+120)) for L in Ls])
    print(tag, "total KB      :", sum(a[L]['kb'] for L in Ls))
    # integers identical across rounds?
    b=arms["R2 "+tag+"/ON"]
    same = all(a[L]['fsy']==b[L]['fsy'] and a[L]['kb']==b[L]['kb'] for L in Ls)
    print(tag, "counters identical across rounds:", same)
print()
print("BEFORE bytes/claim vs 24L+3000:",
      [(round(arms["R1 BEFORE/ON"][L]['kb']*1024/(L+120)), 24*L+3000) for L in Ls])
bt=sum(arms["R1 BEFORE/ON"][L]['kb'] for L in Ls); at=sum(arms["R1 AFTER/ON"][L]['kb'] for L in Ls)
print("total KB before/after:", bt, at, "ratio", round(bt/at,1))
print("L=4000 KB ratio:", round(arms["R1 BEFORE/ON"][4000]['kb']/arms["R1 AFTER/ON"][4000]['kb'],1))
print("map bytes L=4000 before:", arms["R1 BEFORE/ON"][4000]['mapb'], "D79 had 195929, diff",
      arms["R1 BEFORE/ON"][4000]['mapb']-195929)
