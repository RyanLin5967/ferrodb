# Certification — 160a154..37b0e7d (D168)

**All three gates, run in order, against the commit being landed.**

| gate | result |
|---|---|
| `verify-suite.sh` (per-target) | `rc=0 passed=2529 failed=0 build_errors=0 head=37b0e7d` |
| `verify-suite.sh` (go) | `rc=0 passed=97 failed=0` |
| `certify-head.sh` | `OK — suite head=37b0e7d == landing 37b0e7d` |
| `staleness.sh` | `0 behind, 506 ahead — COVERED` |
| `prepush.sh` | `OK` (incl. `cargo check --target x86_64-pc-windows-msvc`) |

**+0 @ 160a154, absolute 2529 per-target** — matching D167's pre-registered 2529. Correct:
every change in this range is a comment, a bench artifact, or a new bench file. No `#[test]`
was added or removed.

⚠ **MODE MATTERS:** 2529 is the **per-target** count. `whole` mode reads 2531 (per-target + 2
doctests). Quoting one against the other manufactures a phantom regression.

## ⚠ THE FIRST RUN OF THIS SUITE WAS VOID AND WAS DISCARDED, NOT INTERPRETED

Run `d168-land` reached 134 verdicts with **16 target failures** and is not in this bank. Cause:
I edited `src/branch/version_graph.rs` **after launching the suite**, so the prebuilt example
binaries went stale under it and the staleness guard refused them —

    target/debug/examples/repl_primary is older than src/ or examples/ — cargo test does not
    rebuild examples, so this would test a stale binary. Run: cargo build --examples

Every one of the 16 finished in 0.00–0.16 s, the signature of a guard refusal rather than a test
failure. **A run whose tree moved underneath it is not a result.** Killed it, committed the tree,
`cargo build --examples`, relaunched as `d168-land2` — which is what this bank contains, taken
with the tree clean and committed at `37b0e7d` before launch and unmodified throughout.

## What landed

- `bench/d168_prereg.txt` — pre-registration, committed BEFORE the first measurement.
- `bench/d168_persistence_penalty_at_head.txt` — the result: the free-space-map persistence
  penalty is **flat in N (1.36× → 1.36× across N=500..4,000)** where D80 measured it growing
  1.88× → 3.29× before D81 landed. Confirms D81 in the headline harness.
- Three stale in-tree claims **banded** (harness comment, D79's 24 TB projection, D80's headline)
  — each asserted a wall D81 deleted.
- ORDPATH re-attributed to O'Neil et al., SIGMOD 2004 (was "Li & Moon 2001" in two files).
