use std::{collections::HashMap, sync::{Arc, Mutex, atomic::AtomicU64}};

use crate::wal::log::WalManager;

pub struct TransactionManager {
    pub wal: Arc<WalManager>,
    pub next_txn_id: AtomicU64,
    pub att: Mutex<HashMap<u64, TxnEntry>>
}

pub struct TxnEntry {
    pub status: TxnStatus,
    pub last_lsn: u64
}

pub enum TxnStatus {
    Running, 
    Commiting,
    Aborting
}

