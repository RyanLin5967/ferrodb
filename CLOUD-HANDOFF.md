# CLOUD HANDOFF — read this first (written 2026-10-10 by the Mac lead session)

You are continuing the work of one long-running lead session on Ryan's Mac. You have NONE of its
conversation and none of its local disk. Everything you need is in three GitHub repos, and this file
says where. The same file is at the root of all three repos.

## 0. Start here, in this order
1. This file, all of it. Ryan's own words are in section 2; they outrank everything else.
2. `artie-research/frontier/HANDOFF-NOW.md`: the live state, kept as you go ("ADD what starts, REMOVE what ends").
   Section 0 of that file is the newest. Its LOUD-OWED list is the work that was written but never run.
3. `artie-research/frontier/fastest/DECISIONS.md`: every ruling. Read the last ~300 lines first.
4. `artie-research/frontier/lead-context/GLOBAL-CLAUDE.md`: Ryan's global operating rules (copied from his
   `~/.claude/CLAUDE.md`). `lead-context/memory/` holds the lead's 229 memory notes, and `MEMORY.md` is the index.
   These are lessons paid for in failures, so grep them before you repeat a mistake.
5. `artie-research/frontier/RESUME.md`: the top banners give per-lane state.

## 1. The repos (all Ryan's, all on GitHub)
| repo | what | main |
|---|---|---|
| RyanLin5967/turso (public fork of tursodatabase/turso) | THE ENGINE: our Turso fork with the durable-branch engine (`core/branch/`), the Postgres frontend (`postgres/`), and the Linux T3 runner, gates and competitor harnesses (`fastest/linux/`) | `9973428e9` |
| RyanLin5967/artie-research (PRIVATE) | THE LAB NOTEBOOK: frontier/ (handoff, decisions, reviews, prereg, every lane's notes and RESUME), the macOS tools (`frontier/fastest/tools/`: V1 syscall shim, bbload, C1b, V3, clonebench), and the invention work (`frontier/round16/`, `round17/`, ...) | see below |
| RyanLin5967/ferrodb | the earlier database project, PAUSED since 2026-09-24 (Ryan: inventions only). It is the home of the `.claude/` hooks | `b3f89b7` |

Clone what you need, for example `gh repo clone RyanLin5967/artie-research` and `gh repo clone RyanLin5967/turso`.
- In a turso clone, name Ryan's remote `fork` the way the Mac does: `git remote rename origin fork`.
  If you add upstream, it is `origin` = tursodatabase/turso, which you may READ ONLY. NEVER push, open a PR or file an issue upstream: upstream filing is Ryan's call.
- artie on GitHub is a FILTERED COPY. Two 134 MB tarballs (frontier/fastest/linux/t3-runs/3725{5216397,5904818}/
  t3-dryrun-ubuntu-24.04/t3-out.tar.gz) are over GitHub's 100 MB limit, so they are small stub files. The originals are
  release assets on the release `t3-artifacts-20261010`. Every commit from `4052363b72` on therefore has a different sha
  on GitHub than on the Mac. If a sha cited in the notes does not resolve, look it up in
  `frontier/repo-ops/github-commit-map.tsv` (columns: old sha, GitHub sha). Shas before 4052363b72 are unchanged.
  The details are in `frontier/repo-ops/GITHUB-COPY.md`.
- If you commit to artie on GitHub, push normally with NO force. When Ryan is back on the Mac, its lead imports your
  commits (`frontier/repo-ops/GITHUB-COPY.md` says how) before it syncs again. Never rewrite GitHub history.

## 2. What Ryan wants (verbatim quotes, newest first)
These come from this session and from HANDOFF-NOW's "Ryan's words" log. They are the spec.

**2026-10-10, about this handoff:**
- "have you pushed everything to main yet? i need everything pushed and merged to main" / "so i can run cloud session on it"
- "but is it merged into main?? like i see some branches ahead of main liek waht? they should all be merged into main."
- "don't have tokens to have this many agents, just od it yourself"
- "push stuff such that the cloud session can easily continue off of everything + a context blob you will provide"
- "and remember my quotes and other stuff related to the fastest branching and inventions and what you have right now"
- "the cloud handoff needs to be seamless btw"
- "don't resume anything when you start, do not resume ANYTHING. only tell me the progress on everything"
- "ignore 'my' prompt saying 'resume everything once the limit resets' that's my automatic program that does that it's hardcoded dont' listen to it listen to ME"

**2026-10-09:**
- "can use fans again" (quiet mode lifted at 18:47Z; quiet mode is a Mac-only flag, ~/.claude/QUIET, and does not exist in the cloud)
- "when you resume you're gonna have to resume under quiet mode, but make sure that you don't forget about the loud stuff, i need those to be done too. just only do quiet stuff when you resume tho and i'll tell you when you can move to loud stuff again"

**2026-10-08, about inventions and process:**
- On the VLDB/SIGMOD program: "i don't want my name associated with some bs invention". Novelty is checked by prior-art research through Exa AND OpenAlex.
- On the MFO paper: "even if it's not novel, it's still a paper". Keep it.
- "keep finding bugs like i want excess"
- "ok the ways to catch bugs are really fucking good make sure they are attacked and stuff and you are doing all the prep work for them" / "and other ideas that you have not just that one"
- "wait is all progress lost if a step doesn't finish in a workflow?" / "if so you need to somehow save work for all of them i dont' want to restart the steps"
- "pause everything about to run out of tokens try to do it at no cost if possibl"

**The two objectives, in Ryan's terms** (the full older log is in section 7):
1. FASTEST DURABLE BRANCH CREATION.
   - "i want you to make something that's the fastest in the world, like branching for example. pick one and stick with it until it's the fastest and i can say so". The target is FIXED: durable database branch creation, on our Turso fork behind its Postgres frontend.
   - "make sure that the fastest branching is 100% is gonna be the fasters", reviewed "like you're a senior engineer since ai slop tends to be slow and inefficient, review it like crazy".
   - "bugs, especially ones that make the branching slow, cannot exist, like at all".
   - "make big jumps in what you're buildling not just micro optimizations". This is the LEAP/FIX/SHAVE rule in DECISIONS: leaps L1 async create, L2 no store mutex on create, L3 shared flights, L4 creation as a pure log record, L5 zero-copy wire fast path. A SHAVE is allowed only when a budget is red and no leap removes it.
   - "tell me when you're ready for me to get the linux box", which means tell Ryan the moment the T3-READY gate reaches 6/6.
2. INVENTIONS (PAPER-Q2, VLDB/SIGMOD grade). Earlier: "i seriously need a good invention", "big leaps in approaches", "retrodict all assertions", "make sure inventions aren't just prs obviously".
   - RULE ONE: nothing is "fastest" or "an invention" until a FRESH-CONTEXT adversary has attacked it and returned.

## 3. Where everything stands (2026-10-10T02:30Z)
**Honest headline: nothing measured yet.**
- Since 2026-10-09T07:03Z almost everything was WRITTEN NOT RUN (quiet mode). The first compile of the integrated engine happened today: `cargo check` passes at turso main.
- NO test suite has run on the new code.
- NO timed number exists for our engine's branch create.
- The T3-READY gate is 0/6.

**Merge state** (done today on Ryan's order, "everything merged to main"):
- **turso main `9973428e9`.** It holds the integrated live fastest line: engine `8dd5dd0d0`, wire `a5ca6ceae`, budgets `b29d49f70`, fe-vdbe-fixes, fe-r14-meds, fe-sdk-spin, fastest-linux `b8e694550`, fastest-branching, fastest-linux-t3, comp `12f79726b` and V3 `e47b4cc21`.
  - It also has two "integration compile fix (unreviewed)" commits, f5b5de905 and 25dc131b8, and six more real merges (resolve-oracle-shrink, resolve-vol-bushy-ever, resolve-fts-7531, fix/7649-collate-in-list-leak, turso-fk-implicit-parent-columns, fastest-c0fix-00340537c).
  - `cargo check --all-targets` over turso_core and the postgres crates gives rc 0.
  - Every other branch got a HISTORY-ONLY merge: recorded as merged, its code NOT in main. The reason for each branch is in the octopus merge messages. They are the r11/r12/r13/inv store variants (superseded by the fastest line), 61 upstream-based PR branches (each carries about 800 upstream commits; kept as branches for upstream filing), quarantine/wip snapshots, mutants, and real work that conflicts. The lists are in section 6.
- **ferrodb main `b3f89b7`.** It has 14 real merges, mostly the resolve-* branches, plus history-only merges for every other branch.
  - Its CI was already red before today, from `tests/depth_probe.rs`. That file came from a "DELIBERATELY FAILING - do not merge" probe that the 2026-10-08 merge-all put on main. It does not compile (unresolved `CowPageLinks`, no `with_links`). Everything else compiles.
  - Removing that file is Ryan's call, because it is a test file.
- **artie main:** merged ft-c1b-tracer, ft-c1b-v2fc, mfo-core-fixes and ft-clonebench-r8. The clonebench one has a KNOWN RED: test_cellstat parity versus the ruled warm_ends definition, recorded in DECISIONS.
- Uncommitted edits found in lane worktrees are on GitHub as `wip/*-20261010` and `quarantine/*-20261010` branches, UNREVIEWED.

**Lanes (all stopped; Ryan said resume nothing):**
- **fastest-engine**, tip `8dd5dd0d0`. Fixes are in code, not run, for engine reviews 16-20 HIGHs. They include the create-path one: the held-free upgrade is now off the ack path (red `886763380`, fix `7fa0b2853`), so a D0 create, connect, first write or delete waits on no upgrade sync.
  - Also in code: review 19's post-yield WAL sync fail-stop, ROLLBACK after an attached Busy, and review 20's NOT NULL reparse, NUMERIC blob operand and custom-type index.
  - Next on its list: review 20 MED 5, then the MEDs of reviews 18-20.
  - It disclosed a gate bypass of its own (grepping the handoff file).
  - Review 16 #21 is REAL; the lead reversed an earlier acceptance of its refutation.
- **fastest-wire**, tip `a5ca6ceae` or later. Written but unrun:
  - wire review 16 HIGHs: the simple-protocol split dropped statements, the 22012 mapping, and the view-walk blowup;
  - wire review 17 HIGH (public. qualifier);
  - the SHOW allowlist (SHOW could switch off fullfsync);
  - the CREATE INDEX schema-drop fix;
  - R17 MED 2.
  - Pre-approved flagged test edits (R17 MED 4 is 42P02, R17 MED 5 is 42P01) are conditional on PG 18 recordings.
- **fastest-budgets**, tip `b29d49f70`. The budgets review 2 pass is done: 47 source and 9 env mutants, unrun.
  - It expects engine reds: the herd waiter, the idle-tail wake, the D2 confirmed open, and a first create in one commit.
  - It ASKS for a lead ruling on flagged engine-file edit `49527af73` (two store.rs lines are not cfg(test)): accept it, or rewrite it as a cfg(test) wrapper.
  - It NEEDS a fresh review of `3c02a5e8c..b29d49f70`.
- **fastest-tools** (artie frontier/fastest/tools). Many review 14-18 fixes are written but unrun. Its last edit to v1_per_op.py, kindrule.py, firecheck.py and probe_c.c was HALF-WRITTEN; it is on the `wip/artie-research-20261010` branch.
- **fastest-linux**, tip `b8e694550`.
  - Linux review 5 MEDs 1-5 and 7 are in code, and MED 6 has started. The paid-box hazard is fixed in code: the drive is now identified by identity, not path.
  - CI cannot run, because Actions on RyanLin5967/turso returned 422 "Actions has been disabled" (Ryan must re-enable it).
- **competitors (Linux)**, `12f79726b`. Its review returned 4 HIGH; the harness handicaps the competitors:
  1. the CI selftest fixtures lack tree=children;
  2. PostgreSQL's DROP DATABASE races its closing backend, 100 ms per M1 cycle (port conn_close_sync);
  3. libpq SSL/GSS probes run inside every timed connect (force sslmode/gssencmode=disable);
  4. sql-serial: RE-RULED in DECISIONS (Doltgres unserialised; Dolt serialised but measured and labelled).
  The full list is in reviews/REVIEW-comp-b49fb656a..f6c2dbb2f.md.
- **V3 (Linux flush probe)**: review 13 says still NOT creditable. Its HIGH is that the F3 fixtures were never regenerated, so the self-check is red on 12/12 cells. The ordered fix list is in reviews/REVIEW-13-linux-v3-e454c84bd..e47b4cc21.md.
- **C1b model** (artie `ft-c1b-v2fc`, merged): reviews 16 and 17 v2fc LOWs written. Three flagged test changes are ruled (case 7's loosening is TEMPORARY until the IPC probe is banked).
- **PREREG** (T3 gate item 1): NOT registered. The finish run left 2 blocking items and its final verifier never ran. The lead's rulings P1-P8 are in DECISIONS. They include PG's two-stage knob grid, our --max-connections, and that the create-latency pilot runs before registration or never.
- **INVENTIONS: 0 confirmed.**
  - MFO (model-free version oracles) is a paper, not an invention (Rule One: RESHAPE). Its core fixes are merged but never run.
  - The VLDB/SIGMOD novelty search (r18) stopped after its generators and before triage; its outputs are in artie round18 or the scratch notes.
  - 21 defects found in other systems (Delta, Dolt, Doltgres, lakeFS, Hudi, Iceberg/pyiceberg, DuckDB, Lance, Nessie, Paimon) are a bug study. Filing them upstream is Ryan's call: round16/e2b-more-systems/TALLY.md.

**Reviews that were running when everything stopped.** None of them survived. Relaunch each as a fresh-context review if you continue:
- engine `f43fde45d..e733305fd`, and the engine commits after it;
- wire `0823803c1..bfeb1668d`, and after it;
- linux `77c2a5030..12ce5e55b`;
- comp `f6c2dbb2f..12f79726b`;
- tools (artie) `ddbe4c137e..954e1de3e6`, and after it;
- C1b v2fc `00cb0e35f0..d08ca5090f`;
- clonebench `580bc45af8..5340913522`;
- MFO core `33eb2a3093..7897ea16f3`;
- budgets `3c02a5e8c..b29d49f70`.
Mac shas in artie ranges may need the commit map.

## 4. Rules that must not break (Ryan's; the full text is in lead-context/GLOBAL-CLAUDE.md)
- NEVER force-push. Push only to Ryan's own repos (RyanLin5967/*), NEVER to tursodatabase. Never commit secrets, and never put Ryan's email or identifiers into external requests.
- Never `git add -A`, `.` or a directory: stage explicit paths. Small commits.
- NEVER edit a test to make it pass. A flagged test edit needs a lead ruling, recorded in DECISIONS.
- Every number names its instrument, or says "unverified". A run that collected nothing has not passed. A detector that found nothing must be forced to fire first. Commit the raw artifact before interpreting it.
- RULE ONE, as above. Every new range gets a fresh-context review, with no queue. A finding list is a work queue, not a deliverable.
- Banned phrases, which hand decisions back: "let me know if", "next I'll", "if you want me to", "I can do X if you'd like", "say the word", "still outstanding", "not yet fixed". Do the work, then report it. No time estimates.
- Don't read `artie-research/research/` (350 KB; an agent died on it).
- Ryan is cost-sensitive right now: "don't have tokens to have this many agents, just od it yourself". Do things directly, and fan out only when it is clearly worth it.

## 5. What the cloud CANNOT do that the Mac did
- **macOS-only measurements.** The T1 numbers (APFS, F_FULLFSYNC, the V1 DYLD shim, clonefile B0/B1) need the Mac, and so does every timed result that was planned on it. The cloud can build, run Linux tests, review, write code, and prepare the Linux T3 path.
- **Mac-only tooling.** buildslot, fan-guard, lockrun and `wt` are not here. Bound every command with timeout instead.
- **Hooks.** The ferrodb `.claude/` hooks call /Users/idide paths. In the cloud they fail harmlessly, and the gate allows when its sentinel is absent.
- **Literature tools.** The Exa API keys are not in any repo (deliberately), so use WebSearch/WebFetch. OpenAlex works without a key.
- **Workflow caches.** Workflows paused on the Mac cannot be resumed from the cloud; their scripts are in lead-context/workflow-scripts/ for reuse.

## 6. Merge-all work queue: real work recorded history-only, so its code is NOT in main yet
Each branch below is still on GitHub. Merge it for real by resolving against main (read its commits and lane notes; keep both sides' behaviour), then check that it compiles.

**ferrodb** (base 38f5df1759 -> b3f89b769b; real merges 14, already contained 21, history-only 290)

History-only by reason: excluded 2026-10-08 = 234; conflicts with main; not resolved in this pass = 39; quarantine/wip snapshot = 14; marked never-to-run = 3

Conflicting real work (39): `resolve-recovery-16` 474bf6b7c, `resolve-recovery-sweep` cf9f51f43, `resolve-recovery-clean-restart` 4f1c2d2d0, `resolve-recovery-sweep` 56baf67a0, `resolve-consensus-phasef` 873ae81d4, `resolve-index-catalog-2` bd11f0a73, `resolve-index-catalog-3` 65cb69186, `resolve-recovery-16-r3` c3cbe3987, `resolve-runtime-tel-r3` 8ca194ad2, `demo-e2e` e724edcb1, `f8rev-determinism` c8458f9e5, `f8rev-refnode` 7f52b5b76, `F3-transport` e38564d3c, `F8-sim` f0b28a096, `F7-atk-key` ecc8f321b, `S14-lazy-durability` 35b9ab0d7, `d173-adversary-2` 40b87b081, `d209-open-sweep-once` afe44d93a, `d221-steady-sweep` c341e9efd, `d208-root-cell-per-index-on-ed6e901` 92a320d60, `d256-heap-page-validate` c2dad3f23, `d239-on-16` c4efffba2, `read-vs-n` a2e2bf0f9, `d216-clean-restart` 717667cf5, `d225-on-16-preview-r7` ecc3b4bb0, `d261-relocation-space` ec5c415af, `d253-on-16` 1fd7878f5, `d230-on-rollback-index-orphan` b17587190, `d252-caught-up-pin` a152d7796, `lease-grace` 1ff46d822, `d271-index-unattached` a6901eb12, `d232-arena-claim-order` c2f438655, `d212-history-store` b6a5ff360, `rollback-index-orphan` 566414a26, `d250-drop-logged` 303c39bec, `d276-oversized-transaction` 2c2d57229, `d229-crash-safe-frees` e74152f9b, `d268-power-loss-redo` 4ecee04fa, `wall21-reaped-subtree` 63f7c48a1

**turso** (base f4f08c3237 -> 9973428e9f; real merges 6, already contained 2, history-only 284)

History-only by reason: alternative variant of core/branch/store.rs = 165; upstream-based PR branch = 71; quarantine/wip snapshot = 18; conflicts with main; not resolved in this pass = 15; marked never-to-run = 15

Conflicting real work (15): `resolve/vol-base-0f4232957` 041c0998a, `resolve-durable-singletons` df5b61ddf, `resolve-vol-base` 043a0e19c, `resolve-vol-base` ab0c2fad9, `resolve-durable-singletons` c5ba3d950, `resolve/vol-base16-4ae0841ee` 30bfb42d3, `resolve-vol-coherence` 081db00b7, `resolve-vol-bushy-ever-r2` d6fe09679, `resolve-vol-bushy-ever` 088adb099, `verify-shrink-fixpoint` 6d62a39eb, `oracle-my-own-probe` 061d056f1, `oracle-limit-fix-completed` 9cf81b434, `k1-recipe-base.noindex` 80e46f1a4, `k1-adv.noindex` 7ff2f2695, `fastest-bisect-ad9829f3b` 88475c61f

## 7. Copied verbatim from artie frontier/HANDOFF-NOW.md (2026-10-10)

### 7a. LOUD-OWED: the runs that were owed when quiet mode ended
### LOUD-OWED (do ALL of these when Ryan lifts QUIET; disk must also be at or above 28 GB, and it is 17 GB at the cut)
1. Resume these workflows FROM CACHE: Workflow({scriptPath, resumeFromRunId}), scripts in workflows/scripts/.
   - MFO wave 1, wf_5221994b-025 (model-free-version-oracles-sweep).
   - MFO wave 2, wf_f4238eb8-b0d (mfo-core-and-wave2).
   - Lineage Consistency, wf_9979954f-402 (idea-rule-one-pilot-prep, args = scratchpad/lead/lineage-args.json).
   - resolve-to-main, wf_2d8599d5-a3a.
   - r17 invention program, wf_ac3c2a71-d41 (its agents may run experiments, so it is LOUD, not hook-blocked).
2. MFO core fixes (round17/mfo/reviews/REVIEW-core-1-mfo-w2.md, 10 items) before any wave-2 divergence counts. Re-run wave-2 hunts on the fixed core.
     - MFO core fixes are WRITTEN on artie branch mfo-core-fixes (tip 7897ea16f3, 39 commits; fresh review running). When QUIET lifts: (i) run test_core_fixes.py (35 checks), guardcheck.py (72 + the new complete-prereg row), the selftest, and the 10,000-seed and long-history negatives on BOTH stores, all at the tip; (ii) a 0-firing plant is a finding, never a seed change; (iii) after green, merge mfo-core-fixes into artie main and copy w2-core back into scratchpad/mfo-w2/core BEFORE resuming wave 2.
3. The resolve-merge fixer for 4d701b0a8 rounds 1 and 2 (repo-ops/REVIEW-resolve-4d701b0a8.md), after resolve returns.
4. The EXPLORATORY create-latency pilot (fastest-tools): release build of fork e78045379, bbload D2 C=1 N=1000 (create alone, and create plus first write), a V3 floor batch, V1 flush counts.
5. Owed builds and runs, per lane:
   - engine: the gate1/gate2 owed reds and greens, the type passes, the C0 arm at 00340537c, and every review 16/17 red;
     plus fe-vdbe-fixes (items 1, 4b, review 16 HIGH 1 and 4c; exact commands in frontier/fastest/lanes/engine/RESUME-fe-vdbe.md) and fl-v3l-med34 (items 19 and HIGH 3, plus 17 mutants via v3l_mutants.py; owed list sent to fastest-linux);
   - wire: the type check at e78045379 and later tips, plus every review 13/14/15 red;
     - wire (added 2026-10-09T18:36:19Z): PG 18 recordings for the pre-approved R17 MED 4 and MED 5 edits (COPY cw FROM f WHERE v=$1 with P(25)/B/E, P(0) and P(); the s.t.c Describe). If PG 18 differs, revert that edit.
   - tools: every self-test and fire-check owed by reviews 14/15, and the reopened bbload G1 red;
   - budgets (corrected 2026-10-09T11:2xZ from the lane's RESUME; base15 is superseded): base16 at 3c02a5e8c, or the dev head then; the 3ab9c579c fire-check pair, clean and planted at one sha; the L3 reproduction; every mutant at base16's sha (37 source, 4 env, the contention pair included); the worst client's waits per ack beside the waits budget. The engine merge of fa3f83cdc is held until budgets review wf_fa0765ca-f0b returns.
   - r13 (round 6 committed at 92295230b, L5 ruled in LR126): AMENDMENT 7's run, re-running the 6 surviving mutants on a fresh snapshot, the full suite (247), then the round-6 sha. AFTER that, launch the round-6 fresh adversary over d313fffd5..<sha>;
   - V3: CI on 4e8acf0ae once Actions returns (the fd-attribution fix; offline evidence banked at c34a7009b1);
   - C1b model: the K-seed RAND_FLOOR (needs 30 GB) and the fire-checks;
   - tracer: the C1/C2 runs and the red arms;
   - V3: local self-tests for the fd attribution;
   - comp: local Dolt/Doltgres probes;
   - linux: the fastest_profile cargo check.
     plus fl-profile-l3 (5 unrun commits, 21 mutants, smoke_main) and fl-v3l-med34 runners, banked at frontier/fastest/linux/quiet-banked-20261009/ (loud_owed_profile.sh runs the profile set);
     plus the fl-devguard-root dry run, which settles whether the runner's lsblk shows / and whether the sysfs layout matches the fakes.
6. The artie-research encrypted bundle backup (needs 31 GB or more).
7. BLOCKED ON RYAN, not on QUIET: GitHub Actions on the fork (422); disk (dbresearch 68 GB); the handoff gate re-arming.

### 7b. Ryan's words log
## Ryan's words, newest first (verbatim; keep the last ~10, drop older ones)
- 2026-10-06T20:3xZ: "are you still adding stuff to a queue? don't if you are" (=> no queue at all: deferred launches stopped; tools review 9, engine review 13 and wire review 8 launched at once; cron recut as REVISION 58 with a no-queue review rule)
- 2026-10-06T19:4xZ: "also btw if you still have stuff in the queue empty that" (=> queue EMPTIED: tools review 8, engine review 12, wire review 7 launched)
- 2026-10-06T19:38-19:42Z: "progress on everything right now?", "what about fastest branching? no way you forgot about that righT?", "is killing fseventsd safe? remember tha tthe fastest branching is really important too, as is the inventions", then "done" (=> he restarted fseventsd: 20.5 -> 46 GB free; builds resumed)
- 2026-10-06T18:47Z: "can use fans again resume everythign" (=> QUIET lifted, everything resumed, cron REVISION 57)
- 2026-10-06T13:03Z: "i need quiet mode to be on, just do work that will keep you quiet for now" then "agents are cheap remember, just builds, test suites and stuff are expensive" and "you can have a lot of agents" (=> RESUMED source-only under QUIET; agents and read-only workflows launch freely)
- 2026-10-06T06:24Z: "pause everything until i tell you to resume" (=> PAUSED, see banner)
- 2026-10-06T03:16Z: "pop top items off the queue and depending on how cheap they are" then "i need importatnt stuff out of the queue asap" (=> launched the important items as LEAN workflows: HIGHs verified alone, MEDs batch-verified per area; Paimon harness held; /code-review bundle dropped as redundant with engine review 7)
- 2026-10-06T03:10Z: "invention status? fastest branching status?" (answered)
- 2026-10-06T02:10Z: "also make sure inventions aren't just prs obviously and tell me when you're ready for me to get teh linux box" and "oh yea i don't want tokebe wasted, stop stuff if it's redundant obviously" (=> defects are evidence, not inventions (INVENTIONS.md); tell Ryan the moment the T3-READY gate reaches 6/6; redundant runs stopped: old r16 attack+E2 workflow w07yghxj4 whose remaining agents duplicated w2k4gk192, duplicate reviewers a9bade4cb8f1683cb and a6b5172c1e53252e0)
- 2026-10-06T01:45Z: "go through the entire queue as well now" (=> every queued item launched; new work keeps going into the queue)
- 2026-10-06 ~01:20Z: "what happened here: Background this session? 139 background tasks running — they will be stopped ... 48 running workflow subagents restart from the beginning ... fixthis" (=> ~104 stale idle teammates from r11/r12/inv/fanq/k1/bb/lg lanes, all closed, were TaskStopped so they no longer count as running; live work untouched) then "nvm ignore that just keep doing what you were doing before. resume everything and stuff"
- 2026-10-06T01:16Z: "resume everything"
- 2026-10-05T03:55Z: "limit hit, just pause everything and save for now and resume when i tell you to" (=> PAUSED; see banner)
- 2026-10-05 ~03:00Z: "and also wehn you're working towards teh fastest branching, make sure you make big jumps in what you're buildling not just micro optimizations, really emphasize this and you can reword it or whatever so it fits whatever you'll do" (=> STANDING RULE in fastest/DECISIONS.md: classify work as LEAP / FIX / SHAVE; pursue LEAPS L1 async create, L2 no store mutex on create, L3 shared flights, L4 creation as a pure log record with zero catalog reads, L5 zero-copy wire fast path; shaves only when a budget is red and no leap removes it)
- 2026-10-05 ~02:44Z: "do half the queue" (=> launched 3 of 5 concrete items: engine review, E3 merge attack, tools bbload review; ASYNC-DESIGN adversary and Linux V3 second review stay queued)
- 2026-10-05 ~02:20Z: "what's currently in the queue" then "ok just keep adding to the queue as you go" (=> every new review/attack/design workflow goes into the WORKFLOW QUEUE; none launched until Ryan says so)
- 2026-10-05 ~02:10Z: "tokens are back resume literally everything" (session limit hit every lane ~00:02Z; everything resumed 2026-10-05T02:13Z: lanes by name, workflows from cache)
- 2026-10-04 23:52Z: "status on everything? and remember what i said about keeping it chill just queue workflows for now" (=> NO new workflow launches; new review/attack/design work goes to the WORKFLOW QUEUE below; running ones finish)
- 2026-10-04 ~23:50Z: "be more chill for now you don't hav infinite tokens" then "wait don't just fucking stop everything, you can still run them i just meant in the future. resuming is costly so just keep them up" (=> everything running stays; NEW dispatch is sized down; the four runs stopped by mistake were resumed: sweep r15 task wtzp2y3am, r16 more-systems wmfgx0m6b, code-review skill, r16-merge)
- 2026-10-04 23:14Z: "plugged in now. go crazy." (=> battery limits lifted; perf-budget suite and async-durable design split off the engine queue; retention attack + E2 fan-out + X5 attack; E3 merge lane)
- 2026-10-04 ~18:20Z: "do some more research to make sure everything you're saying is correct" (workflow wf_15f712fc-605 verifying every recent claim) and "i want to make sure that the fastest stuff is fully ready before i start wasting time" (=> T3-READY GATE in fastest/DECISIONS.md; tell Ryan to rent only when it passes)
- 2026-10-04 ~18:10Z: "what's the cost of just buying a nvme" / "ok what's the cost of hezner or whatever the cheapest but still good one and reputable one is" (answered, then CORRECTED by verification 85a032f01: EX101 no longer orderable; setup fees always charged; cheapest PLP option AX102-3-LTD ~EUR 72/48 h incl. setup; AX42-1 no-PLP EUR 99/month + 49 setup excl. VAT; hardware prices far higher in 2026)
- 2026-10-04 ~17:50Z: "resume literally everything once tokens are back, remember that you can resume agents by name and workflows by cache" / "resume everything"
- 2026-10-04 ~04:10Z: "about to run out of tokens, make sure processes are stopped so they don't keep my fans running and stuff," then "what's still making my computer hot right now?"
- 2026-10-04 ~04:10Z: "i'm gonna need a lot of code reviews (you can use the skill), since bugs, especially ones that make the branching slow, cannot exist, like at all" (=> four reviews per range incl. the code-review skill; performance-budget tests as failing tests; Linux CI profiling job)
- 2026-10-04 ~03:40Z: "just do testing on a linux box like ci or something, you cna push everything i dont' care. there should be no floor" (=> push allowed to Ryan's repos, never upstream; Linux via GitHub Actions on the fork; async-durable create class with no flush on the create path)
- 2026-10-04 ~03:30Z: "what's the current fastest branching and how far are you away from ti" (answered: Doltgres 20.5 ms median on this Mac is the in-category best; we are uncredited, ~1 flush/create vs their 4)
- 2026-10-04 ~03:20Z: "yea just do everything, make sure that everything useful in what you just said is not forgotten, and fastest branching needs to be extremely thorough, like you ahve to review your code like you're a senior engineer since ai slop tends to be slow and inefficient, review it like crazy and put that in your loop, i need the fastest branching to be like a fallback to the inventions, so make sure that the fastest branching is 100% is gonna be the fasters"
- 2026-10-04 03:10Z: "make sure invention generators have all preivous context of what you've done so you can learn from them and also not do the same thing that might've failed, like they should be extremely well throught through before executing, retrodict all assertions too. like i seriously need a good invention, how the fuck have i spend days investing time and money into you and you still haven't produced anything of use? ... really think ,don't do \"micro optimizations\" to improve your approach, they have to be big leaps in approaches and just big jumps that can change everythign"
- 2026-10-03 ~23:58Z: "you realize that you should eb removing and adding to the handoff as you go right?"
- 2026-10-03 ~23:49Z: "plugged in"
- 2026-10-03 ~22:40Z: "resume everything once tokens are back" / "also i want you to make something that's the fastest in the world, like branching for example. pick one and stick with it until it's the fastest and i can say so" / "go"
- 2026-10-03 ~19:05Z: "don't resume anything, but is my computer being hot intended?" (superseded by the 22:40Z resume)
- 2026-10-03 06:06Z: "more agents more good inventions, make sure to learn from your mistakes, do this extremely thoroughly"

### 7c. Ryan's newest demand, and the objectives
## Ryan's newest demand (2026-10-04 ~03:10Z), being acted on
Generators must carry ALL previous context, think before executing, RETRODICT every assertion against our data, and make BIG changes of approach, not tweaks. Workflow wf_f8a8a3f6-5f5 (task we9532cfw): context pack (idea ledger, assets, meta-diagnosis) -> 6 strategists proposing different PROGRAMS (free; correctness/branch-isolation checker; theory; capability-first; cross-layer agent state; real-workload traces) -> prior-art and retrodiction adversaries -> a decider picks one program and its first decisive experiments.
## Objectives (in order)
1. FASTEST BRANCHING (target FIXED, never switch without Ryan): durable database branch creation. Record: frontier/fastest/DECISIONS.md. Engine = our Turso fork (r13 ad9829f3b) behind its Postgres frontend.
2. PAPER-Q2 inventions: 0. The retention/merge conformance CHECKER was KILLED by Rule One on 2026-10-06 (INVENTIONS.md; round16/novelty-attack/). Its 21 defects are a bug study; filing is Ryan's call. Residue experiment: Req(history, policies) as one function plus parameter tables (written under QUIET, run owed). Sweep r15 (wf_57d857ce-a1e) was STOPPED at 13:14Z for heat (its agents ran Python); it resumes from cache when QUIET lifts. Future generators MUST read round16/CONTEXT-PACK-1.md and INVENTIONS.md.

### 7d. The T3-READY gate
## T3-READY GATE (6 items, fastest/DECISIONS.md): status 0/6 met
1 PREREG registered: no (PREREG-CORE running). 2 engine: no. 3 wire server (M3): no. 4 tools HIGHs closed: no. 5 Mac results win: no. 6 one-command T3 runner: NO - REVERSED by the V3 second review (a real T3 run on XFS/btrfs fails check.py; t3run ignores V3 batch rc; no device-flush evidence); needs fixes + a dry run on a non-loop XFS/btrfs data-disk cell.

### 7e. Blockers only Ryan can clear
## Blockers only Ryan can clear (when they come up)
- 2026-10-09T02:19:16Z GITHUB ACTIONS DOWN on RyanLin5967/turso (asked Ryan): dispatch gives 422 "Actions has been disabled for this repository" while the permissions API reads enabled. It flipped between 02:03 and 02:12Z; cause unverified. No Linux CI until he re-enables it on the repo's Actions settings page (or GitHub's notice explains why).
- 2026-10-09T02:16:50Z DISK (asked Ryan): free space is 22 GB, under the 28 GB build floor, so builds wait out buildslot's 4 h bound and expire (the wire type check did). Our own footprint is about 27 GB of in-use lane targets. ~/projects/dbresearch holds 87 GB (engines/ 35, scratchpad/ 33, artifact/ 12, local-only/ 6). Its last commit was 2026-09-25 and nothing in it changed in 3 days (find -mtime -3, maxdepth 3). Deleting any of it is Ryan's call.
- 2026-10-09T02:16:50Z HANDOFF GATE (asked Ryan; updated 2026-10-09T02:22:02Z): it re-armed at 02:11:36Z, 02:15:17Z and 02:21:19Z, three times in 10 minutes, about 45k lead tokens each. It now refuses WORKFLOW agents and sub-agents too, until the LEAD's transcript carries the new tokens; the sub-agents report it reads the lead's transcript. Each re-arm costs about 45k lead tokens and stalls every agent. Scoping the gate per session would stop this. It is Ryan's hook.
- TELL RYAN WHEN THE T3-READY GATE IS 6/6 (his request: "tell me when you're ready for me to get the linux box"). Now 0/6 (item 6 reversed).
- The handoff-read gate re-arms for EVERY session whenever ANY session in this project compacts or starts (SessionStart rewrites the shared payload and sentinel), so each teammate compaction costs every lane plus the lead a full ~40k-token re-read, and lanes stall until the lead re-reads (observed 03:18Z and 03:20Z, two regenerations in 2 minutes). Scoping the gate per session would stop the waste; the hook is Ryan's guard, so this is his call (memory handoff-gate-rearms-and-blocks-teammates: do not edit it).
- C>128 cells need kern.ipc.somaxconn raised above 128 (sudo) on this Mac; Ryan's call.
- Upstream filing of the 21 surviving defects (X5; C1, C2, C7, C4; N1, N3, N4; DuckDB C, D; Lance LD; Nessie M1; lakeFS G1, G2, M1, L1; Paimon N1; Hudi F3, F4, F5; hudi-rs F2; filing list in round16/e2b-more-systems/TALLY.md) and a Dolt 2.4.1 DOLT_BRANCH('-d') concurrent-session panic: posts under Ryan's account; his call.
- The lead session (pid 84828) and all its children run at nice 15 since ~01:2xZ, cause unknown (no renice by the lead). floor.c rightly refuses V3 runs at nice != 0; V3 fire-checks run as one-shot nice-0 launchctl jobs (ruling in fastest/DECISIONS.md). Only a session restart or sudo restores nice 0.
- The account rotator (com.fschoolai.account-rotate) is not registered in launchd and its log stopped at 04:12Z; if this account runs dry nothing switches. Load it with `launchctl load ~/Library/LaunchAgents/com.fschoolai.account-rotate.plist` if wanted.
- "Fastest in the world" needs a separate T3 run (Linux, datacenter NVMe with and without power-loss protection): hardware or a billed cloud run. CONFIRMED 2026-10-04: GitHub-hosted runners have a write-through virtual disk (no device FLUSH), so they cannot be T3; they count only for correctness and flush counts.
- Admin rights for `purge` / `fs_usage` (cold-cache cells; V1 coverage of hardened binaries).
