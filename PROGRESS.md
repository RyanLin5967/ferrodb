# F7 — signed inter-node traffic

**Done:** read DISTRIBUTED.md, phase-f-tasks.md, consensus/mod.rs (frozen), transport.rs
integration surface, provenance/sha256.rs (the hash HMAC will be built on). Verified the RFC 4231
HMAC-SHA256 vectors against CPython's `hmac`/`hashlib` (OpenSSL) so the test expectations come from
outside this crate.

**Doing now:** writing `src/consensus/signing.rs` — HMAC-SHA256 over ferrodb's own SHA-256, a
key loaded from a file with a mode check, and constant-time comparison.

**Next action:** write signing.rs, then the verify hook in transport.rs, then tests_signing.rs.
