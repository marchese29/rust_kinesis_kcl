// use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use rusoto_kinesis::Record;

pub struct KinesisClientRecord {
    pub sequence_number: String,
    // approximate_arrival_timestamp: Instant,
    pub data: Bytes,
    pub partition_key: String,
    pub encryption_type: Option<String>,
    pub sub_sequence_number: Option<u64>,
    pub explicit_hash_key: Option<String>,
    pub aggregated: bool,
}

impl KinesisClientRecord {
    pub(crate) fn from_record(record: Record) -> Self {
        // let now = Instant::now();
        // let epoch = now - now;
        Self {
            sequence_number: record.sequence_number,
            // approximate_arrival_timestamp: epoch
            //     + Duration::from_secs_f64(
            //         record
            //             .approximate_arrival_timestamp
            //             .expect("Record without arrival timestamp"),
            //     ),
            data: record.data,
            partition_key: record.partition_key,
            encryption_type: record.encryption_type,
            sub_sequence_number: None,
            explicit_hash_key: None,
            aggregated: false,
        }
    }
}
