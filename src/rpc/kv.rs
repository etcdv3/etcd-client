//! Etcd KV Operations.

use super::pb::etcdserverpb;

use crate::error::Result;
use crate::rpc::pb::etcdserverpb::{PutRequest as PbPutRequest, PutResponse as PbPutResponse};
use crate::rpc::{KeyValue, ResponseHeader};
use etcdserverpb::kv_client::KvClient as PbKvClient;
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

    /// Put the given key into the key-value store.
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
}

/// Options for `put` operation.
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
    pub fn new() -> Self {
        Self(PbPutRequest::default())
    }

    /// Lease is the lease ID to associate with the key in the key-value store. A lease
    /// value of 0 indicates no lease.
    #[inline]
    pub fn with_lease(mut self, lease: i64) -> Self {
        self.0.lease = lease;
        self
    }

    /// If prev_kv is set, etcd gets the previous key-value pair before changing it.
    /// The previous key-value pair will be returned in the put response.
    #[inline]
    pub fn with_prev_key(mut self, prev_key: bool) -> Self {
        self.0.prev_kv = prev_key;
        self
    }

    /// If ignore_value is set, etcd updates the key using its current value.
    /// Returns an error if the key does not exist.
    #[inline]
    pub fn with_ignore_value(mut self, ignore_value: bool) -> Self {
        self.0.ignore_value = ignore_value;
        self
    }

    /// If ignore_lease is set, etcd updates the key using its current lease.
    /// Returns an error if the key does not exist.
    #[inline]
    pub fn with_ignore_lease(mut self, ignore_lease: bool) -> Self {
        self.0.ignore_lease = ignore_lease;
        self
    }
}

impl IntoRequest<PbPutRequest> for PutOptions {
    #[inline]
    fn into_request(self) -> Request<PbPutRequest> {
        Request::new(self.0)
    }
}

/// Response for `put` operation.
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
