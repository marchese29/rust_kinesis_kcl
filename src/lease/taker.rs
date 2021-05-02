use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use rand::{seq::SliceRandom, thread_rng};
use tokio::sync::RwLock;

use crate::util::runnable::PeriodicRunnable;

use super::{broker::LeaseBroker, renewer::LeaseRenewer, SharedLease};

pub(crate) struct LeaseTaker {
    lease_broker: Arc<LeaseBroker>,
    all_leases: RwLock<HashMap<String, SharedLease>>,
    lease_renewer: Arc<LeaseRenewer>,
    last_scan_time: AtomicU64,
    worker_identifier: String,
    max_allowed_leases: usize,
    max_steals_per_run: usize,
}

fn current_nano_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Awkward")
        .as_nanos()
        .try_into()
        .unwrap()
}

impl LeaseTaker {
    async fn take_leases(&self) -> Vec<SharedLease> {
        self.update_leases_from_source().await;

        let mut expired_leases = Vec::new();
        {
            let all_leases = self.all_leases.read().await;
            for shared_lease in all_leases.values() {
                let lease = shared_lease.read().await;
                if lease.is_expired() {
                    expired_leases.push(Arc::clone(shared_lease));
                }
            }
        }

        let mut taken_leases = Vec::new();
        for lease in self.find_leases_to_take(&mut expired_leases).await {
            if self
                .lease_broker
                .take_lease(lease.clone(), &self.worker_identifier)
                .await
            {
                {
                    let mut lease_guard = lease.write().await;
                    lease_guard.last_renewal_nanos = current_nano_time();
                }
                taken_leases.push(lease.clone());
            }
        }

        taken_leases
    }

    async fn update_leases_from_source(&self) {
        let source_leases = self.lease_broker.list_all_leases().await;
        self.last_scan_time
            .store(current_nano_time(), Ordering::SeqCst);

        let mut not_updated = HashSet::new();
        {
            let all_leases = self.all_leases.read().await;
            for key in all_leases.keys() {
                not_updated.insert(key.clone());
            }
        }

        // Update and add to our leases from the information the source gave us
        let mut all_leases = self.all_leases.write().await;
        for shared_source_lease in source_leases {
            let mut source_lease = shared_source_lease.write().await;
            let existing_lease =
                all_leases.insert(source_lease.lease_key.clone(), shared_source_lease.clone());
            not_updated.remove(&source_lease.lease_key);
            if let Some(old_shared_lease) = existing_lease {
                let old_lease = old_shared_lease.read().await;
                if old_lease.lease_counter == source_lease.lease_counter {
                    source_lease.last_renewal_nanos = old_lease.last_renewal_nanos;
                } else {
                    source_lease.last_renewal_nanos = self.last_scan_time.load(Ordering::SeqCst);
                }
            } else {
                if source_lease.lease_owner.is_none() {
                    source_lease.last_renewal_nanos = 0;
                } else {
                    source_lease.last_renewal_nanos = self.last_scan_time.load(Ordering::SeqCst);
                }
            }
        }

        // If the source didn't give us a lease, then it no longer exists
        for key in not_updated.iter() {
            all_leases.remove(key);
        }
    }

    async fn find_leases_to_take(&self, expired_leases: &mut Vec<SharedLease>) -> Vec<SharedLease> {
        if self.all_leases.read().await.len() == 0 {
            return Vec::new();
        }

        let lease_counts = self.get_lease_counts(expired_leases).await;
        let mut target_count = 0;
        {
            let all_leases = self.all_leases.read().await;
            if lease_counts.len() >= all_leases.len() {
                target_count = 1;
            } else {
                let worker_mod = all_leases.len() % lease_counts.len();
                target_count = all_leases.len() / lease_counts.len();
                if worker_mod > 0 {
                    target_count += 1;
                }
                if target_count > self.max_allowed_leases {
                    target_count = self.max_allowed_leases;
                }
            }
        }

        let curr_lease_count = *lease_counts
            .get(&self.worker_identifier)
            .expect("Worker is un-accounted for");
        let mut available_slots = target_count - curr_lease_count;

        if available_slots == 0 {
            return Vec::new();
        }

        let mut result = Vec::new();
        if expired_leases.len() > 0 {
            // Try taking some of the expired leases at random
            expired_leases.shuffle(&mut thread_rng());
            while available_slots > 0 && expired_leases.len() > 0 {
                result.push(expired_leases.pop().expect("Awkward").clone());
                available_slots -= 1;
            }
        } else {
            // Consider taking from someone else since there's nothing lying around for us
            let leases_to_steal = self
                .get_leases_to_steal(&lease_counts, available_slots, target_count)
                .await;
            for lease_to_steal in leases_to_steal {
                result.push(lease_to_steal.clone());
            }
        }

        result
    }

    async fn get_lease_counts(&self, expired_leases: &Vec<SharedLease>) -> HashMap<String, usize> {
        let mut lease_counts = HashMap::<String, usize>::new();
        let mut expired = HashSet::new();
        for expired_lease in expired_leases {
            expired.insert(expired_lease.read().await.lease_key.clone());
        }

        let all_leases = self.all_leases.read().await;
        for shared_lease in all_leases.values() {
            let lease = shared_lease.read().await;
            if !expired.contains(&lease.lease_key) {
                let lease_owner = lease
                    .lease_owner
                    .clone()
                    .expect("Working with an un-owned lease");
                let curr_count = lease_counts.get(&lease_owner);
                if let Some(&old_count) = curr_count {
                    lease_counts.insert(lease_owner, old_count + 1);
                } else {
                    lease_counts.insert(lease_owner, 1);
                }
            }
        }

        if lease_counts.get(&self.worker_identifier).is_none() {
            lease_counts.insert(self.worker_identifier.clone(), 0);
        }
        lease_counts
    }

    async fn get_leases_to_steal(
        &self,
        lease_counts: &HashMap<String, usize>,
        needed_leases: usize,
        target: usize,
    ) -> Vec<SharedLease> {
        assert!(lease_counts.len() > 0);
        let (busiest_worker, &busiest_count) = lease_counts
            .iter()
            .max_by_key(|&(_, &v)| v)
            .expect("Awkward");

        let mut leases_to_steal: usize = 0;
        if busiest_count >= target && needed_leases > 0 {
            let stealable_lease_count = busiest_count - target;
            if stealable_lease_count > needed_leases {
                leases_to_steal = needed_leases;
            } else {
                leases_to_steal = stealable_lease_count;
            }

            if needed_leases > 1 && leases_to_steal == 0 {
                leases_to_steal = 1;
            }

            if self.max_steals_per_run < leases_to_steal {
                leases_to_steal = self.max_steals_per_run;
            }
        }

        if leases_to_steal <= 0 {
            return Vec::new();
        }

        let mut stealable_leases = Vec::new();
        for lease in self.all_leases.read().await.values() {
            let lease_guard = lease.read().await;
            if lease_guard.lease_owner.eq(&Some(busiest_worker.clone())) {
                stealable_leases.push(lease.clone());
            }
        }

        stealable_leases.shuffle(&mut thread_rng());

        let mut result = Vec::new();
        while result.len() < leases_to_steal && result.len() < stealable_leases.len() {
            let idx = result.len();
            result.push(stealable_leases.get(idx).expect("Awkward").clone());
        }

        result
    }
}

#[async_trait]
impl PeriodicRunnable for LeaseTaker {
    async fn run_once(&self) {
        self.lease_renewer
            .add_leases(self.take_leases().await)
            .await;
    }
}
