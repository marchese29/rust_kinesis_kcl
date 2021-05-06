use dynomite::AttributeError::{self, MissingField};
use std::sync::Arc;

use dynomite::{
    dynamodb::{DynamoDb, DynamoDbClient, ScanInput},
    FromAttributes,
};
use tokio::sync::RwLock;

use crate::{lease::Lease, util::exception::Exception};

use super::SharedLease;

static LEASE_TABLE: &str = "lease_table";

pub(crate) struct LeaseBroker {
    dynamo_client: DynamoDbClient,
}

impl LeaseBroker {
    pub(crate) async fn list_all_leases(&self) -> Result<Vec<SharedLease>, Exception> {
        // TODO: Paging
        let input = ScanInput {
            attributes_to_get: None,
            conditional_operator: None,
            consistent_read: Some(true),
            exclusive_start_key: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            filter_expression: None,
            index_name: None,
            limit: None,
            projection_expression: None,
            return_consumed_capacity: None,
            scan_filter: None,
            segment: None,
            select: None,
            table_name: LEASE_TABLE.to_string(),
            total_segments: None,
        };

        let mut all_leases = Vec::new();
        match self.dynamo_client.scan(input).await {
            Ok(res) => {
                if let Some(items) = res.items {
                    for attr_item in items {
                        match Lease::from_attrs(attr_item) {
                            Ok(lease) => {
                                all_leases.push(Arc::new(RwLock::new(lease)));
                            }
                            Err(err) => match err {
                                AttributeError::InvalidFormat => {
                                    return Err(Exception::NonRetryable(
                                        "Attribute contains an invalid format".to_string(),
                                    ))
                                }
                                AttributeError::InvalidType => {
                                    return Err(Exception::NonRetryable(
                                        "Attribute contains invalid type".to_string(),
                                    ))
                                }
                                MissingField { name } => {
                                    return Err(Exception::NonRetryable(format!(
                                        "Attribute '{}' was missing",
                                        name
                                    )))
                                }
                            },
                        }
                    }
                }
            }
            Err(_err) => {
                todo!("Handle dynamo errors")
            }
        }

        Ok(all_leases)
    }

    pub(crate) async fn take_lease(&self, _lease: SharedLease, _worker: &str) -> bool {
        todo!()
    }

    pub(crate) async fn renew_lease(&self, _lease: SharedLease) -> bool {
        todo!()
    }
}
