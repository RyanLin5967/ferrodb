//! F3 — carrying `Message`s between nodes over the existing replication framing.
//!
//! **OWNER: agent F3.** Reuses `tag | u32 len | body` and the `0xFEDB` handshake from
//! `crate::replication`; `REPL_VERSION` goes to 2 so a v1 peer is refused at the handshake rather
//! than misparsed several frames later. `std::net` and `std::thread` only — this crate carries zero
//! runtime dependencies and that is a product claim.
