use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::{Mutex, Notify, RwLock};

use crate::util::RunAtFixedInterval;

use super::Lease;

pub(crate) struct LeaseRenewer {
    leases: RwLock<HashMap<String, Arc<Lease>>>,
    run_interval: Duration,

    should_shutdown: AtomicBool,
    shutdown: Notify,
    shutdown_signal: Notify,
}

impl LeaseRenewer {
    fn new(run_interval: Duration) -> Self {
        Self {
            leases: RwLock::new(HashMap::new()),
            run_interval,
            should_shutdown: AtomicBool::new(false),
            shutdown: Notify::new(),
            shutdown_signal: Notify::new(),
        }
    }

    async fn renew_lease(&self, lease: Arc<Lease>) -> bool {
        todo!()
    }

    pub(crate) async fn shutdown(&self) {
        self.should_shutdown.store(true, Ordering::SeqCst);
        self.shutdown_signal.notify_waiters();
        self.shutdown.notified().await;
    }
}

#[async_trait]
impl RunAtFixedInterval for LeaseRenewer {
    fn should_shutdown(&self) -> bool {
        self.should_shutdown.load(Ordering::SeqCst)
    }

    async fn await_shutdown_signal(&self) {
        self.shutdown_signal.notified().await;
    }

    fn notify_shutdown_complete(&self) {
        self.shutdown.notify_waiters();
    }

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

    fn get_interval(&self) -> Duration {
        self.run_interval
    }
}
