use rusoto_kinesis::KinesisClient;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, time::sleep};

use interface::processor::RecordProcessor;
use lease::{LeaseManager, ShardInfo};
use rusoto_core::region::Region;
use tokio::{join, sync::Notify};
use worker::ShardWorker;

pub mod interface;
mod kinesis;
mod lease;
mod worker;

/// TODO
pub struct WorkerScheduler {
    // Client-provided: generates new RecordProcessor instances
    processor_factory: fn() -> Box<dyn RecordProcessor>,

    // Bookkeeping processes (not visible to clients)
    lease_manager: Arc<LeaseManager>,

    // Represents current worker state
    consumers: Mutex<HashMap<ShardInfo, Arc<ShardWorker>>>,

    kinesis: Arc<KinesisClient>,

    should_shutdown: AtomicBool,
    shutdown_signal: Notify,
    shutdown_notifier: Notify,
}

impl WorkerScheduler {
    /// TODO
    pub fn new(processor_factory: fn() -> Box<dyn RecordProcessor>) -> Self {
        Self {
            processor_factory,
            lease_manager: Arc::new(LeaseManager::new()),
            consumers: Mutex::new(HashMap::new()),
            kinesis: Arc::new(KinesisClient::new(Region::UsEast1)),
            should_shutdown: AtomicBool::new(false),
            shutdown_signal: Notify::new(),
            shutdown_notifier: Notify::new(),
        }
    }

    /// TODO
    pub fn initialize(&self) {
        self.lease_manager.initialize();
    }

    /// TODO
    pub async fn run(&self) {
        self.lease_manager.clone().start();

        let mut last_loop_time = Instant::now();
        while !self.should_shutdown.load(Ordering::SeqCst) {
            let mut killed = false;
            tokio::select! {
                _ = self.shutdown_signal.notified() => { killed = true; }
                _ = self.run_once() => {}
            }

            // Limit our polling frequency
            if !killed && Instant::now() - last_loop_time < Duration::from_secs(10) {
                // Delay until next cycle, or until we get the signal to shutdown
                tokio::select! {
                    _ = self.shutdown_signal.notified() => {}
                    _ = sleep(last_loop_time + Duration::from_secs(10) - Instant::now()) => {}
                }
            }
            last_loop_time = Instant::now();
        }

        self.shutdown_all_consumers().await;

        // Close out any bookkeeping processes
        join!(self.lease_manager.shutdown());
        self.shutdown_notifier.notify_waiters();
    }

    async fn run_once(&self) {
        let mut assigned_shards = HashSet::<ShardInfo>::new();
        {
            let mut guard = self.consumers.lock().await;
            for shard in self.lease_manager.get_owned_leases().await {
                if !guard.contains_key(&shard) {
                    guard.insert(
                        shard.clone(),
                        Arc::new(ShardWorker::new(
                            shard.clone(),
                            self.kinesis.clone(),
                            self.processor_factory,
                        )),
                    );
                    guard.get(&shard).expect("Awkward").clone().start();
                }

                assigned_shards.insert(shard);
            }
        }

        // Step 2: Clean up consumers for leases we no longer own
        let mut expired_shards = Vec::new();
        {
            let guard = self.consumers.lock().await;
            let mut expiring_consumers = Vec::new();
            for (shard, consumer) in guard
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
            let mut guard = self.consumers.lock().await;
            for shard in expired_shards.iter() {
                guard.remove(shard);
            }
        }

        // Step 3: TODO: Leader stuff
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

    /// TODO
    pub async fn shutdown(&self) {
        self.should_shutdown.store(true, Ordering::SeqCst);
        self.shutdown_signal.notify_waiters();
        self.shutdown_notifier.notified().await;
    }
}
