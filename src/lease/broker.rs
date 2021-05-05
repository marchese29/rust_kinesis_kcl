use super::SharedLease;

pub(crate) struct LeaseBroker {}

impl LeaseBroker {
    pub(crate) async fn list_all_leases(&self) -> Vec<SharedLease> {
        todo!()
    }

    pub(crate) async fn take_lease(&self, _lease: SharedLease, _worker: &str) -> bool {
        todo!()
    }

    pub(crate) async fn renew_lease(&self, _lease: SharedLease) -> bool {
        todo!()
    }
}
