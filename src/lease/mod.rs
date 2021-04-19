use std::{collections::HashSet, sync::Arc};

use dynomite::Item;
use rusoto_kinesis::Shard;
use tokio::join;

use self::manager::{LeaseRenewer, LeaseTaker};

mod manager;
mod shard;

pub(crate) struct LeaseManager {
    initialized: bool,
    lease_taker: LeaseTaker,
    lease_renewer: LeaseRenewer,
}

impl LeaseManager {
    pub(crate) fn new() -> Self {
        todo!()
    }

    pub(crate) fn initialize(&self) {
        todo!()
    }

    pub(crate) fn start(self: Arc<Self>) {
        assert!(
            self.initialized,
            "Attempted to start lease manager before initializing"
        );
        let this = self.clone();
        tokio::spawn(async move { this.lease_taker.run().await });

        let this = self.clone();
        tokio::spawn(async move { this.lease_renewer.run().await });
    }

    pub(crate) async fn get_owned_leases(&self) -> HashSet<ShardInfo> {
        todo!()
    }

    pub(crate) async fn shutdown(&self) {
        join!(self.lease_taker.shutdown(), self.lease_renewer.shutdown());
    }
}

#[derive(Item)]
pub(crate) struct Lease {
    #[dynomite(partition_key)]
    lease_key: String,
}

impl Lease {
    fn for_shard(shard: &Shard) -> Self {
        Self {
            lease_key: shard.shard_id.clone(),
        }
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
