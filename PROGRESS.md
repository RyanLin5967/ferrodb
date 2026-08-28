# F7 — signed inter-node traffic

**Done:** `src/consensus/signing.rs` (HMAC-SHA256 over the crate's own SHA-256, a key file with a
mode + directory check, constant-time compare, the frame layer); the verify-before-decode hook in
`src/consensus/transport.rs` behind `Transport::from_listener_with_key` / `bind_with_key`;
`src/consensus/tests_signing.rs` — 35 tests, all green; 11 mutants fired and all killed, table in
`scratchpad/F7-signing.md`.

**Doing now:** running the suite per target to confirm nothing else moved.

**Next action:** run `cargo test --lib` and each integration target, then write the summary to
`/Users/idide/wt/artie-research/build-G/F7-signing.md`.
