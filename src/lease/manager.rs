use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::{Notify, RwLock};

use super::Lease;

pub(crate) struct LeaseTaker {
    renewer: Arc<LeaseRenewer>,
    should_shutdown: AtomicBool,
    shutdown: Notify,
}

impl LeaseTaker {
    fn new(renewer: Arc<LeaseRenewer>) -> Self {
        Self {
            renewer: renewer.clone(),
            should_shutdown: AtomicBool::new(false),
            shutdown: Notify::new(),
        }
    }

    pub(crate) async fn run(&self) {
        todo!()
    }

    pub(crate) async fn shutdown(&self) {
        self.should_shutdown.store(true, Ordering::SeqCst);
        self.shutdown.notified().await;
    }
}

pub(crate) struct LeaseRenewer {
    leases: RwLock<Vec<Arc<Lease>>>,
    should_shutdown: AtomicBool,
    shutdown: Notify,
}

impl LeaseRenewer {
    fn new() -> Self {
        Self {
            leases: RwLock::new(Vec::new()),
            should_shutdown: AtomicBool::new(false),
            shutdown: Notify::new(),
        }
    }

    pub(crate) async fn run(&self) {
        todo!()
    }

    pub(crate) async fn shutdown(&self) {
        self.should_shutdown.store(true, Ordering::SeqCst);
        self.shutdown.notified().await;
    }
}
