use async_trait::async_trait;

use crate::util::runnable::PeriodicRunnable;

pub(crate) struct LeaseTaker {}

#[async_trait]
impl PeriodicRunnable for LeaseTaker {
    async fn run_once(&self) {
        todo!()
    }
}
