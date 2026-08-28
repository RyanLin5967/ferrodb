//! F0 — the durable round log: term-stamped, contiguous, and recoverable after a checkpoint.
//!
//! **OWNER: agent F0.** This is the store, not the protocol. See `DISTRIBUTED.md` §F0 for why a
//! round and not an LSN.
