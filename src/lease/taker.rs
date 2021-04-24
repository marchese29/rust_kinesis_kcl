use async_trait::async_trait;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use tokio::sync::Notify;

use crate::util::RunWithFixedDelay;

pub(crate) struct LeaseTaker {
    run_delay: Duration,
    should_shutdown: AtomicBool,
    shutdown: Notify,
    shutdown_signal: Notify,
}

impl LeaseTaker {
    fn new(run_delay: Duration) -> Self {
        Self {
            run_delay,
            should_shutdown: AtomicBool::new(false),
            shutdown: Notify::new(),
            shutdown_signal: Notify::new(),
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.should_shutdown.store(true, Ordering::SeqCst);
        self.shutdown_signal.notify_waiters();
        self.shutdown.notified().await;
    }
}

#[async_trait]
impl RunWithFixedDelay for LeaseTaker {
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
        todo!()
    }

    fn get_delay(&self) -> Duration {
        self.run_delay
    }
}
