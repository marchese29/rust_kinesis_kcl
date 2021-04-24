use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{Mutex, RwLock};

use crate::util::runnable::PeriodicRunnable;

use super::Lease;

pub(crate) struct LeaseRenewer {
    leases: RwLock<HashMap<String, Arc<Lease>>>,
}

impl LeaseRenewer {
    fn new(run_interval: Duration) -> Self {
        Self {
            leases: RwLock::new(HashMap::new()),
        }
    }

    async fn renew_lease(&self, lease: Arc<Lease>) -> bool {
        todo!()
    }
}

#[async_trait]
impl PeriodicRunnable for LeaseRenewer {
    async fn run_once(&self) {
        // Step 1: Configure a renewer for each lease, grab a handle and keep track of expirations
        let expired_leases = Arc::new(Mutex::new(Vec::new()));
        {
            let leases_guard = self.leases.read().await;
            for (lease_key, lease) in leases_guard.iter() {
                if !self.renew_lease(lease.clone()).await {
                    expired_leases.lock().await.push(lease_key.clone());
                }
            }
        }

        // Step 2: Remove the expired leases from our renewal pool
        {
            let mut leases_guard = self.leases.write().await;
            for expired_lease in expired_leases.lock().await.iter() {
                leases_guard.remove(expired_lease);
            }
        }
    }
}
