use dynomite::Item;

mod shard;

#[derive(Item)]
pub(crate) struct Lease {
    #[dynomite(partition_key)]
    lease_key: String,
    lease_owner: String,
}
