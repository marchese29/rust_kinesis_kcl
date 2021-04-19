use async_trait::async_trait;
use bytes::Bytes;
use rusoto_kinesis::ChildShard;

use super::record::KinesisClientRecord;

pub struct InitializationInput {
    pub shard_id: String,
    pub pending_checkpoint_state: Option<Bytes>,
}

pub struct ProcessRecordsInput {
    pub records: Vec<KinesisClientRecord>,
    pub is_at_shard_end: bool,
    pub child_shards: Vec<ChildShard>,
}

#[async_trait]
pub trait RecordProcessor: Send + Sync {
    async fn initialize(&self, input: InitializationInput);
    async fn process_records(&self, input: ProcessRecordsInput);
    async fn lease_lost(&self);
    async fn shard_ended(&self);
    async fn shutdown_requested(&self);
}
