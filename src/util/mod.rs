use std::time::{Duration, Instant};

use async_trait::async_trait;

#[async_trait]
pub trait RunWithFixedDelay: Send + Sync {
    fn should_shutdown(&self) -> bool;
    async fn await_shutdown_signal(&self);
    async fn before_shutdown_complete(&self) {}
    fn notify_shutdown_complete(&self);

    /// Executes the logic that will occur at the fixed delay
    async fn run_once(&self);

    fn get_delay(&self) -> Duration;

    async fn run(&self) {
        while !self.should_shutdown() {
            self.run_once().await;

            tokio::select! {
                _ = self.await_shutdown_signal() => {}
                _ = tokio::time::sleep(self.get_delay()) => {}
            }
        }

        self.before_shutdown_complete().await;
        self.notify_shutdown_complete();
    }
}

#[async_trait]
pub trait RunAtFixedInterval: Send + Sync {
    fn should_shutdown(&self) -> bool;
    async fn await_shutdown_signal(&self);
    async fn before_shutdown_complete(&self) {}
    fn notify_shutdown_complete(&self);

    /// Executes the logic that will occur at a fixed interval
    async fn run_once(&self);

    fn get_interval(&self) -> Duration;

    async fn run(&self) {
        let mut last_loop_time = Instant::now();
        while !self.should_shutdown() {
            self.run_once().await;

            if Instant::now() - last_loop_time < self.get_interval() {
                tokio::select! {
                    _ = self.await_shutdown_signal() => {}
                    _ = tokio::time::sleep(last_loop_time + self.get_interval() - Instant::now()) => {}
                }
            }
            last_loop_time = Instant::now();
        }

        self.before_shutdown_complete().await;
        self.notify_shutdown_complete();
    }
}
