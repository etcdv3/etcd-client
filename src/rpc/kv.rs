//! Etcd KV Operations.

pub use crate::rpc::pb::etcdserverpb::range_request::{SortOrder, SortTarget};

use crate::error::Result;
use crate::rpc::pb::etcdserverpb::kv_client::KvClient as PbKvClient;
use crate::rpc::pb::etcdserverpb::{
    PutRequest as PbPutRequest, PutResponse as PbPutResponse, RangeRequest as PbRangeRequest,
    RangeRequest, RangeResponse as PbRangeResponse,
};
use crate::rpc::{KeyValue, ResponseHeader};
use tonic::transport::Channel;
use tonic::{Interceptor, IntoRequest, Request};

/// Client for KV operations.
#[repr(transparent)]
pub struct KvClient {
    inner: PbKvClient<Channel>,
}

impl KvClient {
    /// Create a kv client.
    #[inline]
    pub fn new(channel: Channel, interceptor: Option<Interceptor>) -> Self {
        let inner = match interceptor {
            Some(it) => PbKvClient::with_interceptor(channel, it),
            None => PbKvClient::new(channel),
        };

        Self { inner }
    }

    /// Puts the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    #[inline]
    pub async fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        options: PutOptions,
    ) -> Result<PutResponse> {
        let resp = self
            .inner
            .put(options.with_kv(key, value))
            .await?
            .into_inner();
        Ok(PutResponse::new(resp))
    }

    /// Gets the key or a range of keys from the store.
    #[inline]
    pub async fn get(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: GetOptions,
    ) -> Result<GetResponse> {
        let resp = self
            .inner
            .range(options.with_key(key.into()))
            .await?
            .into_inner();
        Ok(GetResponse::new(resp))
    }
}

/// Options for `Put` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct PutOptions(PbPutRequest);

impl PutOptions {
    /// Set key-value pair.
    #[inline]
    fn with_kv(mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.0.key = key.into();
        self.0.value = value.into();
        self
    }

    /// Create a `PutOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbPutRequest {
            key: Vec::new(),
            value: Vec::new(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        })
    }

    /// Lease is the lease ID to associate with the key in the key-value store. A lease
    /// value of 0 indicates no lease.
    #[inline]
    pub const fn with_lease(mut self, lease: i64) -> Self {
        self.0.lease = lease;
        self
    }

    /// If prev_kv is set, etcd gets the previous key-value pair before changing it.
    /// The previous key-value pair will be returned in the put response.
    #[inline]
    pub const fn with_prev_key(mut self) -> Self {
        self.0.prev_kv = true;
        self
    }

    /// If ignore_value is set, etcd updates the key using its current value.
    /// Returns an error if the key does not exist.
    #[inline]
    pub const fn with_ignore_value(mut self) -> Self {
        self.0.ignore_value = true;
        self
    }

    /// If ignore_lease is set, etcd updates the key using its current lease.
    /// Returns an error if the key does not exist.
    #[inline]
    pub const fn with_ignore_lease(mut self) -> Self {
        self.0.ignore_lease = true;
        self
    }
}

impl IntoRequest<PbPutRequest> for PutOptions {
    #[inline]
    fn into_request(self) -> Request<PbPutRequest> {
        Request::new(self.0)
    }
}

/// Response for `Put` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct PutResponse(PbPutResponse);

impl PutResponse {
    /// Create a new `PutResponse` from pb put response.
    #[inline]
    fn new(resp: PbPutResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// If prev_kv is set in the request, the previous key-value pair will be returned.
    #[inline]
    pub fn prev_key(&self) -> Option<&KeyValue> {
        self.0.prev_kv.as_ref().map(From::from)
    }

    /// Takes the prev_key out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_prev_key(&mut self) -> Option<KeyValue> {
        self.0.prev_kv.take().map(KeyValue::new)
    }
}

/// Options for `Get` operation.
#[derive(Debug, Default, Clone)]
pub struct GetOptions {
    req: PbRangeRequest,
    with_prefix: bool,
    with_from_key: bool,
}

impl GetOptions {
    /// Sets key.
    #[inline]
    fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.req.key = key.into();
        self
    }

    /// Creates a `GetOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self {
            req: PbRangeRequest {
                key: Vec::new(),
                range_end: Vec::new(),
                limit: 0,
                revision: 0,
                sort_order: 0,
                sort_target: 0,
                serializable: false,
                keys_only: false,
                count_only: false,
                min_mod_revision: 0,
                max_mod_revision: 0,
                min_create_revision: 0,
                max_create_revision: 0,
            },
            with_prefix: false,
            with_from_key: false,
        }
    }

    /// Specifies the range of 'Get'.
    /// Returns the keys in the range [key, end_key).
    /// `end_key` must be lexicographically greater than start key.
    #[inline]
    pub fn with_range(mut self, end_key: impl Into<Vec<u8>>) -> Self {
        self.req.range_end = end_key.into();
        self
    }

    /// Gets all keys >= key.
    #[inline]
    pub const fn with_from_key(mut self) -> Self {
        self.with_from_key = true;
        self.with_prefix = false;
        self
    }

    /// Gets all keys prefixed with key.
    #[inline]
    pub const fn with_prefix(mut self) -> Self {
        self.with_prefix = true;
        self.with_from_key = false;
        self
    }

    /// Limits the number of keys returned for the request. When limit is set to 0,
    /// it is treated as no limit.
    #[inline]
    pub const fn with_limit(mut self, limit: i64) -> Self {
        self.req.limit = limit;
        self
    }

    /// The point-in-time of the key-value store to use for the range.
    /// If revision is less or equal to zero, the range is over the newest key-value store.
    /// If the revision has been compacted, ErrCompacted is returned as a response.
    #[inline]
    pub const fn with_revision(mut self, revision: i64) -> Self {
        self.req.revision = revision;
        self
    }

    /// Sets the order for returned sorted results.
    /// It requires 'with_range' and/or 'with_prefix' to be specified too.
    #[inline]
    pub fn with_sort(mut self, target: SortTarget, order: SortOrder) -> Self {
        if target == SortTarget::Key && order == SortOrder::Ascend {
            // If order != SortOrder::None, server fetches the entire key-space,
            // and then applies the sort and limit, if provided.
            // Since by default the server returns results sorted by keys
            // in lexicographically ascending order, the client should ignore
            // SortOrder if the target is SortTarget::Key.
            self.req.sort_order = SortOrder::None as i32;
        } else {
            self.req.sort_order = order as i32;
        }
        self.req.sort_target = target as i32;
        self
    }

    /// Sets the get request to use serializable member-local reads.
    /// Get requests are linearizable by default; linearizable requests have higher
    /// latency and lower throughput than serializable requests but reflect the current
    /// consensus of the cluster. For better performance, in exchange for possible stale reads,
    /// a serializable get request is served locally without needing to reach consensus
    /// with other nodes in the cluster.
    #[inline]
    pub const fn with_serializable(mut self) -> Self {
        self.req.serializable = true;
        self
    }

    /// Returns only the keys and not the values.
    #[inline]
    pub const fn with_keys_only(mut self) -> Self {
        self.req.keys_only = true;
        self
    }

    /// Returns only the count of the keys in the range.
    #[inline]
    pub const fn with_count_only(mut self) -> Self {
        self.req.count_only = true;
        self
    }

    /// Sets the lower bound for returned key mod revisions; all keys with
    /// lesser mod revisions will be filtered away.
    #[inline]
    pub const fn with_min_mod_revision(mut self, revision: i64) -> Self {
        self.req.min_mod_revision = revision;
        self
    }

    /// Sets the upper bound for returned key mod revisions; all keys with
    /// greater mod revisions will be filtered away.
    #[inline]
    pub const fn with_max_mod_revision(mut self, revision: i64) -> Self {
        self.req.max_mod_revision = revision;
        self
    }

    /// Sets the lower bound for returned key create revisions; all keys with
    /// lesser create revisions will be filtered away.
    #[inline]
    pub const fn with_min_create_revision(mut self, revision: i64) -> Self {
        self.req.min_create_revision = revision;
        self
    }

    /// max_create_revision is the upper bound for returned key create revisions; all keys with
    /// greater create revisions will be filtered away.
    #[inline]
    pub const fn with_max_create_revision(mut self, revision: i64) -> Self {
        self.req.max_create_revision = revision;
        self
    }
}

/// Get prefix end key of `key`.
#[inline]
fn get_prefix(key: &[u8]) -> Vec<u8> {
    for (i, v) in key.iter().enumerate().rev() {
        if *v < 0xFF {
            let mut end = Vec::from(&key[..=i]);
            end[i] = *v + 1;
            return end;
        }
    }

    // next prefix does not exist (e.g., 0xffff);
    vec![0]
}

impl IntoRequest<PbRangeRequest> for GetOptions {
    #[inline]
    fn into_request(mut self) -> Request<RangeRequest> {
        if self.with_from_key {
            if self.req.key.is_empty() {
                self.req.key = vec![b'\0'];
            }
            self.req.range_end = vec![b'\0'];
        } else if self.with_prefix {
            if self.req.key.is_empty() {
                self.req.key = vec![b'\0'];
                self.req.range_end = vec![b'\0'];
            } else {
                self.req.range_end = get_prefix(&self.req.key);
            }
        }
        Request::new(self.req)
    }
}

/// Response for `Get` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct GetResponse(PbRangeResponse);

impl GetResponse {
    /// Create a new `GetResponse` from pb get response.
    #[inline]
    fn new(resp: PbRangeResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// The list of key-value pairs matched by the `Get` request.
    /// kvs is empty when count is requested.
    #[inline]
    pub fn kvs(&self) -> &[KeyValue] {
        unsafe { &*(self.0.kvs.as_slice() as *const _ as *const [KeyValue]) }
    }

    /// Indicates if there are more keys to return in the requested range.
    #[inline]
    pub fn more(&self) -> bool {
        self.0.more
    }

    /// The number of keys within the range when requested.
    #[inline]
    pub fn count(&self) -> i64 {
        self.0.count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_prefix() {
        assert_eq!(get_prefix(b"foo1").as_slice(), b"foo2");
        assert_eq!(get_prefix(b"\xFF").as_slice(), b"\0");
        assert_eq!(get_prefix(b"foo\xFF").as_slice(), b"fop");
    }
}
