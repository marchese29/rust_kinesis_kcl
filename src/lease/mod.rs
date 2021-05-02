use std::sync::Arc;

use tokio::sync::RwLock;

mod broker;
pub(crate) mod manager;
mod renewer;
mod taker;

pub(crate) type SharedLease = Arc<RwLock<Lease>>;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) struct Lease {
    pub(crate) lease_key: String,
    pub(crate) lease_owner: Option<String>,
    pub(crate) last_renewal_nanos: u64,
    pub(crate) lease_counter: u64,
}

impl Lease {
    pub(crate) fn is_expired(&self) -> bool {
        todo!()
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) struct ShardInfo {
    pub(crate) shard_id: String,
}

impl ShardInfo {
    fn from_lease(_lease: &Lease) -> Self {
        todo!()
    }
}
