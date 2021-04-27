use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::util::runnable::PeriodicRunnable;

use super::{Lease, fetcher::LeaseFetcher, renewer::LeaseRenewer};

pub(crate) struct LeaseTaker {
    lease_fetcher: Arc<LeaseFetcher>,
    all_leases: Arc<RwLock<HashMap<String, Arc<Lease>>>>,
    lease_renewer: Arc<LeaseRenewer>,
}

impl LeaseTaker {

    async fn take_leases(&self) -> Vec<Arc<Lease>> {
        todo!()
    }

    async fn update_leases_from_source(&self) {
        let source_leases = self.lease_fetcher.list_all_leases().await;
        for lease in source_leases {
            let all_leases = self.all_leases.write().await;
            if let Some(old_lease) = all_leases.get(&lease.lease_key) {
                if old_lease.get_lease_counter().await == lease.get_lease_counter().await {

                } else {

                }
            } else {
                if lease.lease_owner.is_none() {
                    lease.set_last_renewal_nanos(0).await;
                } else {
                    // TODO: last scan time
                }
            }
        }
    }
}

#[async_trait]
impl PeriodicRunnable for LeaseTaker {
    async fn run_once(&self) {
        self.lease_renewer.add_leases(self.take_leases().await).await;
    }
}
