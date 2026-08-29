# F7 — signed inter-node traffic: DONE

**Done:** everything. `src/consensus/signing.rs` (HMAC-SHA256 over the crate's own SHA-256, a key
file guarded by a full component walk of its path, constant-time compare, the frame layer), the
verify-before-decode hook in `src/consensus/transport.rs` behind explicit signed constructors,
`NodeOptions::signed_with` in `node.rs` so the row is reachable from the driver, and
`src/consensus/tests_signing.rs` — 53 tests.

**Verified:** 1144 lib tests, 84/84 integration targets (603 tests), and the same 1144 against a
`git archive` of the committed tree under CI's flags. 25 mutants fired, 22 killed; the three
survivors each changed something and are recorded. Mutant table and every correction:
`scratchpad/F7-signing.md`. Raw suite and baseline evidence: `scratchpad/F7-evidence/`.

**Summary written to:** `/Users/idide/wt/artie-research/build-G/F7-signing.md`.

**Next action:** none — the row is finished. Nothing pushed; branch `F7-signing` is local.
