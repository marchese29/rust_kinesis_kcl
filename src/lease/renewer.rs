use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::util::runnable::PeriodicRunnable;

use super::{broker::LeaseBroker, SharedLease};

pub(crate) struct LeaseRenewer {
    leases: RwLock<HashMap<String, SharedLease>>,
    lease_broker: Arc<LeaseBroker>,
}

impl LeaseRenewer {
    pub(crate) async fn add_leases(&self, leases: Vec<SharedLease>) {
        let mut leases_guard = self.leases.write().await;
        for lease in leases {
            let l = lease.read().await;
            leases_guard.insert(l.lease_key.clone(), lease.clone());
        }
    }

    async fn renew_lease(&self, lease: SharedLease) -> bool {
        let mut renewed_lease = false;
        let lease_guard = lease.read().await;
        if !lease_guard.is_expired() {
            drop(lease_guard); // The broker might need the lock
            renewed_lease = self.lease_broker.renew_lease(lease.clone()).await;
        }
        renewed_lease
    }
}

#[async_trait]
impl PeriodicRunnable for LeaseRenewer {
    async fn run_once(&self) {
        // Step 1: Try renewing leases, note any that are expired
        let mut expired_leases = Vec::new();
        {
            let leases_guard = self.leases.read().await;
            for (lease_key, shared_lease) in leases_guard.iter() {
                if !self.renew_lease(shared_lease.clone()).await {
                    expired_leases.push(lease_key.clone());
                }
            }
        }

        // Step 2: Remove the expired leases from our renewal pool
        let mut leases_guard = self.leases.write().await;
        for expired_lease in expired_leases.iter() {
            leases_guard.remove(expired_lease);
        }
    }
}
