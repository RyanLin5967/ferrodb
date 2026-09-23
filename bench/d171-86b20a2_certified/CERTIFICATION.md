# Certification — 3e71a2b..86b20a2 (D171 corrections)

| gate | result |
|---|---|
| `verify-suite.sh` (per-target) | `rc=0 passed=2529 failed=0 build_errors=0 head=86b20a2` |
| `verify-suite.sh` (go) | `rc=0 passed=97 failed=0` |
| `certify-head.sh` | `OK — suite head=86b20a2 == landing 86b20a2` |
| `staleness.sh` | `0 behind, 527 ahead — COVERED` |
| `prepush.sh` | `OK` |

**+0 @ 3e71a2b, absolute 2529 per-target** (MODE: per-target; `whole` reads 2531). `bench/*.txt`
only — no source change, no `#[test]` touched. Acquired at **`load_at_acquire=3`**; acquire, build,
suite and all three gates ran as ONE command with `trap … EXIT`, released on exit.

## ⛔ LANDED ONTO A RED CI, DELIBERATELY AND WITH THE RED RECORDED FIRST

**CI run 35835903937 on the PARENT commit `3e71a2b` FAILED** — ubuntu ✅ macOS ✅ **windows ✗**
(`34 passed; 1 failed`). This range was landed anyway, and the reasoning is on the record rather
than implied:
- The local suite is **green at the landing commit** (2529/0, head-matched), which is the standing
  authorisation condition.
- **These commits cannot be the cause**: `3e71a2b` and this range are `bench/*.txt` only, atop a
  green `ac6adb8`. The failing test is `integration_cluster_agents`, untouched.
- **The red is recorded as its own row (D173) BEFORE this push**, so landing cannot bury it.
- A fresh CI run yields a **second sample of an intermittent**, which D173 needs.

⚠ **This is not a precedent for landing on red generally.** It rests on all four conditions above,
and particularly on the failure being recorded and provably independent of the diff.

## What this range contains — three corrections and a reversal

1. **My ~70 ns un-retracted** (`bench/d170_d71_rerun_RAW.txt`, head of file). Recomputed under
   x = N/2: **64.0 / 64.0 / 69.5 ns/statement**, matching D171's independent 67.61. **The NUMBER
   was right; the UNIT was wrong** (W=S made the axes inseparable), **and my stated reason for
   withdrawing it — saturation — was wrong in the other direction** (saturation caps W; the term
   is S-keyed). I withdrew a correct number for an incorrect reason and replaced it with one low
   by exactly 2×.
2. **The "second wall" section BANDED, not repaired** (`bench/d71_point_update_curve.txt`). All
   seven points intact. It now says their sweep **mixed a W-keyed walk term with an S-keyed
   statement term and read the sum as a ratio**, and forbids quoting any rate from those points
   (segment-wise: −520, +440, +480, −15, +70, +296 ns/stmt — **two NEGATIVE**, at a load the file
   records as 99→23).
3. **The D170 certification**, which itself records that its own artifact contained a retraction
   later found wrong.

⭐ **METHOD LESSON, which cost both parties a number: A RATE IS NOT A NUMBER UNTIL ITS DENOMINATOR
CONVENTION IS STATED.** A median-of-growing-session instrument samples at ~N/2; a pre-loaded
fixed-window instrument sees ~S. Mixing them manufactured a "6% agreement", now withdrawn.

## Instruments that must never land

`D170_NOPUSHDOWN`, `D170_STUB`, `D172_*` timers, and five arm counters live on branch
`d170-adversary`. **Verified absent from main at this commit**, with a positive control
(`unprobeable_rows`, the real field, returns 11 hits in `runtime.rs`).
⛔ `D170_NOPUSHDOWN` restores an O(table) scan on all three agent WRITE paths and **no test can
see it** — D170 measured that removing the pushdown leaves the answer CORRECT and changes only the
complexity.
