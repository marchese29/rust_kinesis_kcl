use std::sync::Arc;

use super::Lease;

pub(crate) struct LeaseFetcher {}

impl LeaseFetcher {
    pub(crate) async fn list_all_leases(&self) -> Vec<Arc<Lease>> {
        todo!()
    }
}