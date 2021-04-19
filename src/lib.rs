use rusoto_kinesis::KinesisClient;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::time::sleep;

use interface::processor::RecordProcessor;
use lease::{LeaseManager, ShardInfo};
use rusoto_core::region::Region;
use tokio::{join, sync::Notify};
use worker::ShardWorker;

pub mod interface;
pub mod kinesis;
pub mod lease;
pub mod worker;

pub struct WorkerScheduler {
    processor_factory: fn() -> Box<dyn RecordProcessor>,

    lease_manager: Arc<LeaseManager>,

    consumers: HashMap<ShardInfo, Arc<ShardWorker>>,

    kinesis: Arc<KinesisClient>,

    should_shutdown: AtomicBool,
    shutdown_notifier: Notify,
}

impl WorkerScheduler {
    pub fn new(processor_factory: fn() -> Box<dyn RecordProcessor>) -> Self {
        Self {
            processor_factory,
            lease_manager: Arc::new(LeaseManager::new()),
            consumers: HashMap::new(),
            kinesis: Arc::new(KinesisClient::new(Region::UsEast1)),
            should_shutdown: AtomicBool::new(false),
            shutdown_notifier: Notify::new(),
        }
    }

    pub fn initialize(&self) {
        self.lease_manager.initialize();
    }

    pub async fn run(&mut self) {
        self.lease_manager.clone().start();

        let mut last_loop_time = Instant::now();
        while !self.should_shutdown.load(Ordering::SeqCst) {
            let mut assigned_shards = HashSet::<ShardInfo>::new();
            for shard in self.lease_manager.get_owned_leases().await {
                // Spin up a new consumer if we need one
                if !self.consumers.contains_key(&shard) {
                    self.consumers.insert(
                        shard.clone(),
                        Arc::new(ShardWorker::new(
                            shard.clone(),
                            self.kinesis.clone(),
                            self.processor_factory,
                        )),
                    );
                    self.consumers.get(&shard).expect("Awkward").clone().start();
                }

                assigned_shards.insert(shard);
            }

            // Clean up consumers for leases we no longer own
            // TODO: Ignoring cuz I'm tired of fighting the borrow checker, but we need to remove workers which are shut down
            let mut expiring_consumers = Vec::new();
            for (_, expiring_consumer) in self.consumers.iter() {
                if !expiring_consumer.is_shutdown() {
                    expiring_consumers.push(expiring_consumer.await_shutdown());
                }
            }
            if !expiring_consumers.is_empty() {
                // TODO: We don't need to await these until after the leader work
                futures::future::join_all(expiring_consumers).await;
            }

            // TODO: Do lease coordination if we're the leader

            // We don't want to be polling too frequently
            if Instant::now() - last_loop_time < Duration::from_secs(10) {
                sleep(last_loop_time + Duration::from_secs(10) - Instant::now()).await;
            }
            last_loop_time = Instant::now();
        }

        // Close out the remaining consumers
        let mut handles = Vec::new();
        self.consumers
            .values()
            .filter(|&consumer| !consumer.is_shutdown())
            .for_each(|consumer| handles.push(consumer.await_shutdown()));
        futures::future::join_all(handles).await;
        self.consumers.clear();

        // Close out any bookkeeping processes
        join!(self.lease_manager.shutdown());
        self.shutdown_notifier.notify_waiters();
    }

    pub async fn shutdown(&self) {
        self.should_shutdown.store(true, Ordering::SeqCst);
        self.shutdown_notifier.notified().await;
    }
}
