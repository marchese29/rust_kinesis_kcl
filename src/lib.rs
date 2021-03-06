use async_trait::async_trait;
use rusoto_kinesis::KinesisClient;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use util::runnable::{run_at_fixed_interval, PeriodicRunnable};

use interface::processor::RecordProcessor;
use lease::{manager::LeaseManager, ShardInfo};
use rusoto_core::region::Region;
use tokio::sync::Notify;
use worker::ShardWorker;

pub mod interface;
mod kinesis;
mod lease;
pub mod util;
mod worker;

/// TODO
pub struct WorkerScheduler {
    processor_factory: fn() -> Box<dyn RecordProcessor>,
    lease_manager: Arc<LeaseManager>,
    consumers: Mutex<HashMap<ShardInfo, Arc<ShardWorker>>>,
    kinesis: Arc<KinesisClient>,
    shutdown: Arc<Notify>,
}

impl WorkerScheduler {
    /// TODO
    pub fn new(processor_factory: fn() -> Box<dyn RecordProcessor>) -> Self {
        Self {
            processor_factory,
            lease_manager: Arc::new(LeaseManager::new()),
            consumers: Mutex::new(HashMap::new()),
            kinesis: Arc::new(KinesisClient::new(Region::UsEast1)),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// TODO
    pub fn initialize(&self) {
        self.lease_manager.initialize();
    }

    /// TODO
    pub async fn run(self: Arc<Self>) {
        self.lease_manager.start();
        run_at_fixed_interval(self.clone(), Duration::from_secs(10), self.shutdown.clone()).await;
    }

    /// TODO
    pub async fn shutdown(&self) {
        self.shutdown.notify_waiters();
        self.shutdown.notified().await;
    }

    async fn shutdown_all_consumers(&self) {
        let mut consumers = self.consumers.lock().await;
        let mut handles = Vec::new();
        consumers
            .values()
            .filter(|&consumer| !consumer.is_shutdown())
            .for_each(|consumer| handles.push(consumer.await_shutdown()));
        futures::future::join_all(handles).await;
        consumers.clear();
    }
}

#[async_trait]
impl PeriodicRunnable for WorkerScheduler {
    async fn run_once(&self) {
        // Step 1: Launch consumers if we need to
        let mut assigned_shards = HashSet::<ShardInfo>::new();
        {
            let mut consumers_guard = self.consumers.lock().await;
            for shard in self.lease_manager.get_owned_leases().await {
                // TODO: don't launch children until parents are done
                if !consumers_guard.contains_key(&shard) {
                    consumers_guard.insert(
                        shard.clone(),
                        Arc::new(ShardWorker::new(
                            shard.clone(),
                            self.kinesis.clone(),
                            self.processor_factory,
                        )),
                    );
                    consumers_guard
                        .get(&shard)
                        .expect("Awkward")
                        .clone()
                        .start();
                }

                assigned_shards.insert(shard);
            }
        }

        // Step 2: Clean up consumers for leases we no longer own
        let mut expired_shards = Vec::new();
        {
            let consumers_guard = self.consumers.lock().await;
            let mut expiring_consumers = Vec::new();
            for (shard, consumer) in consumers_guard
                .iter()
                .filter(|&(shard, _)| !assigned_shards.contains(shard))
            {
                if !consumer.is_shutdown() {
                    expiring_consumers.push(consumer.await_shutdown());
                }
                expired_shards.push(shard.clone());
            }
            futures::future::join_all(expiring_consumers).await;
        }
        if !expired_shards.is_empty() {
            let mut consumers_guard = self.consumers.lock().await;
            for shard in expired_shards.iter() {
                consumers_guard.remove(shard);
            }
        }

        // Step 3: TODO: Leader stuff
    }

    async fn before_shutdown_complete(&self) {
        self.shutdown_all_consumers().await;
        self.lease_manager.shutdown().await;
    }
}
