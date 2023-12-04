use crate::error::Result;
use crate::namespace::strip_prefix;
use crate::{LeaseClient, LeaseTimeToLiveOptions, LeaseTimeToLiveResponse};

pub struct LeaseClientPrefix {
    pfx: Vec<u8>,
    lease: LeaseClient,
}

impl LeaseClientPrefix {
    /// Wrap a Lease interface to filter for only keys with a prefix
    /// and remove that prefix when fetching attached keys through TimeToLive.
    pub fn new(lease: LeaseClient, pfx: Vec<u8>) -> Self {
        Self { pfx, lease }
    }

    pub async fn time_to_live(
        &mut self,
        id: i64,
        options: Option<LeaseTimeToLiveOptions>,
    ) -> Result<LeaseTimeToLiveResponse> {
        let mut resp = self.lease.time_to_live(id, options).await?;
        resp.take_mut_keys(|keys| {
            keys.into_iter()
                .filter_map(|mut key| {
                    if key.starts_with(&self.pfx) {
                        strip_prefix(&self.pfx, &mut key);
                        Some(key)
                    } else {
                        None
                    }
                })
                .collect()
        });
        Ok(resp)
    }
}
