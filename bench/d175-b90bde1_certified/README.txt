D175 CERTIFICATION — the d55 arena/server commutation coverage.

RANGE CERTIFIED: 742e6f9..b90bde1  (2 commits, tests only, +410 lines, NO src/ change)

THE VERDICT, with its base and its MODE — quoting the Rust line, not the Go one:
  d55arena-b90bde1: mode=per-target rc=0 passed=2531 failed=0 build_errors=0 head=b90bde1

  ⇒ +2 @ 742e6f9, mode=per-target (baseline 2529). The two new tests, exactly.
  ⚠ The console's LAST rc= line is the GO suite (`go rc=0 passed=97`) and is a DIFFERENT
    instrument counting a DIFFERENT thing. A monitor grepping for `rc=` grabs that one. Quoting
    97 as the suite total would have been wrong by construction; the mode is what caught it.

THE THREE GATES, run not predicted (tools/land-gate.sh, 2026-09-23 11:01Z):
  [1/3] certify-head  OK   suite head=b90bde1 == landing b90bde1
  [2/3] staleness     COVERED   0 behind, 2 ahead of 742e6f9
  [3/3] prepush       OK   FFI gate, -D dead_code build --examples, check --all-targets,
                           and the x86_64-pc-windows-msvc cfg check (which does NOT link)

⚠ CI COLOUR OF THE BASE AT LANDING TIME, recorded rather than discovered later: main was RED on
  windows at 742e6f9 for **D173** — a real, reproducible (2 of 2), already-recorded defect in
  `a_hundred_agent_writes_across_three_branches_leave_only_forks_and_merges_in_the_log`. It is
  unrelated to this branch, which touches no `src/`. Landing onto it is deliberate and is on the
  record here, not glossed.

⚠ WHAT A GREEN HERE DOES NOT MEAN — from the branch's own report, and written into both test
  files' module docs: the six READ cases are backend-insensitive BY CONSTRUCTION
  (`visible_rows_where` folds the in-memory BTreeMap; `self.storage` is not on that path), proven
  by disabling `stage_all`'s mirror block and watching all six still pass. The page-growth guard
  is what stops the arena test decaying into a second copy of the map-backed one.
