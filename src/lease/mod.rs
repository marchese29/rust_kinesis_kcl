use dynomite::Item;
use tokio::sync::RwLock;

mod fetcher;
pub(crate) mod manager;
mod renewer;
mod shard;
mod taker;

struct MutableLeaseComponents {
    last_renewal_nanos: u64,
    lease_counter: u64,
}

pub(crate) struct Lease {
    pub(crate) lease_key: String,
    pub(crate) lease_owner: Option<String>,
    mutable_components: RwLock<MutableLeaseComponents>,
}

impl Lease {
    pub(crate) async fn get_last_renewal_nanos(&self) -> u64 {
        let inner = self.mutable_components.read().await;
        inner.last_renewal_nanos
    }

    pub(crate) async fn set_last_renewal_nanos(&self, last_renewal_nanos: u64) {
        let mut inner = self.mutable_components.write().await;
        inner.last_renewal_nanos = last_renewal_nanos;
    }

    pub(crate) async fn get_lease_counter(&self) -> u64 {
        let inner = self.mutable_components.read().await;
        inner.lease_counter
    }

    pub(crate) async fn set_lease_counter(&self, lease_counter: u64) {
        let mut inner = self.mutable_components.write().await;
        inner.lease_counter = lease_counter;
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
