use std::{
    collections::HashMap,
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};

use rusoto_core::RusotoError;
use rusoto_kinesis::{Kinesis, KinesisClient, ListShardsError, ListShardsInput, Shard};
use tokio::sync::RwLock;

use crate::kinesis::StreamDescriptor;

type APIError = RusotoError<ListShardsError>;

struct ShardCache {
    cache: HashMap<String, Arc<Shard>>,
    last_cache_update: Option<Instant>,
    cache_ttl: Duration,
}

impl ShardCache {
    fn new(cache_ttl: Duration) -> Self {
        Self {
            cache: HashMap::new(),
            last_cache_update: None,
            cache_ttl,
        }
    }

    fn get_shard(&self, shard_id: String) -> Option<Arc<Shard>> {
        if self.cache_is_expired() {
            None
        } else {
            if let Some(shard) = self.cache.get(&shard_id) {
                Some(Arc::clone(shard))
            } else {
                None
            }
        }
    }

    fn get_all_shards(&self) -> Option<Arc<Vec<Arc<Shard>>>> {
        if self.cache_is_expired() {
            return Some(Arc::new(Vec::new()));
        }

        let mut result = Vec::new();
        self.cache
            .values()
            .for_each(|value| result.push(Arc::clone(value)));
        Some(Arc::new(result))
    }

    fn refresh(&mut self, shards: &Vec<Arc<Shard>>) {
        self.cache = HashMap::new();
        shards.iter().for_each(|shard| {
            let r = Arc::clone(shard);
            self.cache.insert(r.shard_id.clone(), r);
        });
        self.last_cache_update = Some(Instant::now());
    }

    fn cache_is_expired(&self) -> bool {
        if let Some(last_updated_time) = self.last_cache_update {
            last_updated_time + self.cache_ttl < Instant::now()
        } else {
            true
        }
    }
}

pub(crate) struct StreamLayout {
    descriptor: StreamDescriptor,
    kinesis_client: KinesisClient,
    cache: RwLock<ShardCache>,
}

impl StreamLayout {
    pub(crate) fn new(
        descriptor: StreamDescriptor,
        kinesis_client: KinesisClient,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            descriptor,
            kinesis_client,
            cache: RwLock::new(ShardCache::new(cache_ttl)),
        }
    }

    pub(crate) async fn list_shards(&self) -> Result<Vec<Arc<Shard>>, APIError> {
        if let Some(shards) = self.cache.read().await.get_all_shards() {
            // Theoretically dangerous, but I know there is only one owner at this point
            Ok(Arc::try_unwrap(shards).unwrap())
        } else {
            self.refresh_cache().await?;
            if let Some(shards) = self.cache.read().await.get_all_shards() {
                Ok(Arc::try_unwrap(shards).unwrap())
            } else {
                Ok(Vec::new())
            }
        }
    }

    pub(crate) async fn get_shard(&self, shard_id: &str) -> Result<Option<Arc<Shard>>, APIError> {
        if let Some(shard) = self.cache.read().await.get_shard(shard_id.to_owned()) {
            Ok(Some(shard.clone()))
        } else {
            self.refresh_cache().await?;
            Ok(self.cache.read().await.get_shard(shard_id.to_owned()))
        }
    }

    async fn refresh_cache(&self) -> Result<(), APIError> {
        let shards = self.get_shards().await?;
        self.cache.write().await.refresh(&shards);
        Ok(())
    }

    async fn get_shards(&self) -> Result<Vec<Arc<Shard>>, APIError> {
        let mut result: Vec<Arc<Shard>> = Vec::new();
        let request = ListShardsInput {
            exclusive_start_shard_id: None,
            max_results: None,
            next_token: None,
            shard_filter: None,
            stream_creation_timestamp: None,
            stream_name: Some(self.descriptor.stream_name.clone()),
        };
        let mut response = self.kinesis_client.list_shards(request).await?;
        response
            .shards
            .unwrap_or(Vec::new())
            .iter()
            .for_each(|shard| result.push(Arc::new(shard.clone())));
        while let Some(ref next_token) = response.next_token {
            let req = ListShardsInput {
                exclusive_start_shard_id: None,
                max_results: None,
                next_token: Some(next_token.to_string()),
                shard_filter: None,
                stream_creation_timestamp: None,
                stream_name: Some(self.descriptor.stream_name.clone()),
            };
            response = self.kinesis_client.list_shards(req).await?;
            response
                .shards
                .unwrap_or(Vec::new())
                .iter()
                .for_each(|shard| result.push(Arc::new(shard.clone())));
        }
        Ok(result)
    }
}
