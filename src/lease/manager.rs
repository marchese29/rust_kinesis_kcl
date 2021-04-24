use std::{collections::HashSet, sync::Arc};
use tokio::join;

use super::{renewer::LeaseRenewer, taker::LeaseTaker, ShardInfo};
use crate::util::{RunAtFixedInterval, RunWithFixedDelay};

pub(crate) struct LeaseManager {
    initialized: bool,
    lease_taker: Arc<LeaseTaker>,
    lease_renewer: Arc<LeaseRenewer>,
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
