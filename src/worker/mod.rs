use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use futures::StreamExt;
use rusoto_kinesis::{
    Kinesis, KinesisClient, StartingPosition, SubscribeToShardEventStreamItem,
    SubscribeToShardInput,
};
use tokio::sync::Notify;

use crate::{
    interface::{
        processor::{InitializationInput, ProcessRecordsInput, RecordProcessor},
        record::KinesisClientRecord,
    },
    lease::ShardInfo,
};

pub(crate) struct ShardWorker {
    shard_info: ShardInfo,
    record_processor: Box<dyn RecordProcessor>,

    kinesis: Arc<KinesisClient>,

    should_shutdown: AtomicBool,
    shutdown: Notify,
    is_shutdown: AtomicBool,
}

impl ShardWorker {
    pub(crate) fn new(
        shard_info: ShardInfo,
        kinesis: Arc<KinesisClient>,
        factory: fn() -> Box<dyn RecordProcessor>,
    ) -> Self {
        Self {
            shard_info,
            record_processor: factory(),
            kinesis,
            should_shutdown: AtomicBool::new(false),
            shutdown: Notify::new(),
            is_shutdown: AtomicBool::new(false),
        }
    }

    pub(crate) fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            self.record_processor
                .initialize(InitializationInput {
                    shard_id: self.shard_info.shard_id.clone(),
                    // TODO: checkpoint stuff
                    pending_checkpoint_state: None,
                })
                .await;

            let mut res = self
                .kinesis
                .subscribe_to_shard(SubscribeToShardInput {
                    consumer_arn: "TODO".to_string(),
                    shard_id: self.shard_info.shard_id.clone(),
                    starting_position: StartingPosition {
                        sequence_number: None,
                        timestamp: None,
                        type_: "TRIM_HORIZON".to_string(),
                    },
                })
                .await
                .expect("I don't like errors");

            while let Some(Ok(item)) = res.event_stream.next().await {
                if self.should_shutdown.load(Ordering::SeqCst) {
                    break;
                }
                match item {
                    SubscribeToShardEventStreamItem::SubscribeToShardEvent(event) => {
                        let records = event
                            .records
                            .iter()
                            .map(|record| KinesisClientRecord::from_record(record.clone()))
                            .collect();
                        self.record_processor
                            .process_records(ProcessRecordsInput {
                                records,
                                is_at_shard_end: false,   // TODO
                                child_shards: Vec::new(), // TODO
                            })
                            .await; // TODO: Errors and better awaiting
                    }
                    _ => break,
                }
            }

            self.record_processor.shutdown_requested().await;
            self.shutdown.notify_waiters();
        });
    }

    pub(crate) async fn await_shutdown(&self) {
        self.should_shutdown.store(true, Ordering::SeqCst);
        self.shutdown.notified().await;
        self.is_shutdown.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::SeqCst)
    }
}
