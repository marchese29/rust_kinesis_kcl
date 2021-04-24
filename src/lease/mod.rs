use dynomite::Item;
use rusoto_kinesis::Shard;

pub(crate) mod manager;
mod renewer;
mod shard;
mod taker;

#[derive(Item)]
pub(crate) struct Lease {
    #[dynomite(partition_key)]
    pub(crate) lease_key: String,
}

impl Lease {
    fn for_shard(shard: &Shard) -> Self {
        Self {
            lease_key: shard.shard_id.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) struct ShardInfo {
    pub(crate) shard_id: String,
}

impl ShardInfo {
    fn from_lease(_lease: &Lease) -> Self {
        todo!()
    }
}
