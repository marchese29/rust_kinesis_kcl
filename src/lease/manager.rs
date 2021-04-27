use std::{collections::HashSet, sync::Arc};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::sync::Notify;

use super::{renewer::LeaseRenewer, taker::LeaseTaker, ShardInfo};
use crate::util::runnable::{run_at_fixed_interval, run_with_fixed_delay};

pub(crate) struct LeaseManager {
    initialized: AtomicBool,
    lease_taker: Arc<LeaseTaker>,
    lease_renewer: Arc<LeaseRenewer>,
    shutdown: Arc<Notify>,
}

impl LeaseManager {
    pub(crate) fn new() -> Self {
        todo!()
    }

    pub(crate) fn initialize(&self) {
        self.initialized.store(true, Ordering::SeqCst);
        todo!()
    }

    pub(crate) fn start(&self) {
        assert!(
            self.initialized.load(Ordering::SeqCst),
            "Attempted to start lease manager before initializing"
        );
        tokio::spawn(run_with_fixed_delay(
            self.lease_taker.clone(),
            Duration::from_secs(10),
            self.shutdown.clone(),
        ));
        tokio::spawn(run_at_fixed_interval(
            self.lease_renewer.clone(),
            Duration::from_secs(10),
            self.shutdown.clone(),
        ));
    }

    pub(crate) async fn get_owned_leases(&self) -> HashSet<ShardInfo> {
        todo!()
    }

    pub(crate) async fn shutdown(&self) {
        self.shutdown.notify_waiters();
        self.shutdown.notified().await;
    }
}
