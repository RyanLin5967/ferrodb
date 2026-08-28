pub mod error;
pub mod catalog;
pub mod storage;
pub mod buffer;
pub mod parser;
pub mod execution;
pub mod planner;
pub mod cli;
pub mod binder;
pub mod optimizer;
pub mod wal;

// agent-isolation layer
pub mod agent_sql;
pub mod branch;
pub mod cow;
pub mod tel;
pub mod pgwire;
pub mod provenance;
pub mod replication;
pub mod consensus;
/// F4: the node-local counters that must become cluster state, and the guards that refuse.
pub mod cluster;
