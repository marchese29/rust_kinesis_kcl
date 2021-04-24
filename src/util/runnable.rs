use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::sync::Notify;

pub(crate) async fn run_at_fixed_interval<T: PeriodicRunnable>(
    runnable: Arc<T>,
    interval: Duration,
    shutdown: Arc<Notify>,
) {
    let mut last_loop_time = Instant::now();
    loop {
        let mut shutdown_signal = false;
        tokio::select! {
            _ = shutdown.notified() => { shutdown_signal = true }
            _ = runnable.run_once() => {}
        }

        if !shutdown_signal && Instant::now() - last_loop_time < interval {
            tokio::select! {
                _ = shutdown.notified() => { shutdown_signal = true }
                _ = tokio::time::sleep(last_loop_time + interval - Instant::now()) => {}
            }
        }
        last_loop_time = Instant::now();

        if shutdown_signal {
            break;
        }
    }

    runnable.before_shutdown_complete().await;
    shutdown.notify_waiters();
}

pub(crate) async fn run_with_fixed_delay<T: PeriodicRunnable>(
    runnable: Arc<T>,
    delay: Duration,
    shutdown: Arc<Notify>,
) {
    loop {
        let mut shutdown_signal = false;
        tokio::select! {
            _ = shutdown.notified() => { shutdown_signal = true }
            _ = runnable.run_once() => {}
        }

        if !shutdown_signal {
            tokio::select! {
                _ = shutdown.notified() => { shutdown_signal = true }
                _ = tokio::time::sleep(delay) => {}
            }
        }

        if shutdown_signal {
            break;
        }
    }

    runnable.before_shutdown_complete().await;
    shutdown.notify_waiters();
}

#[async_trait]
pub(crate) trait PeriodicRunnable: Send + Sync {
    async fn run_once(&self);
    async fn before_shutdown_complete(&self) {}
}
