//! F7 — authenticating node-to-node traffic.
//!
//! **OWNER: agent F7.** An unauthenticated peer that can speak this protocol can claim a later term
//! and demote a healthy leader, so the transport must refuse a message it cannot authenticate
//! rather than pass it to the state machine.
