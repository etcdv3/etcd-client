//! Etcd Lease RPC.

use crate::error::Result;
use crate::rpc::pb::etcdserverpb::lease_client::LeaseClient as PbLeaseClient;
use crate::rpc::pb::etcdserverpb::{
    LeaseGrantRequest as PbLeaseGrantRequest, LeaseGrantResponse as PbLeaseGrantResponse,
    LeaseKeepAliveRequest as PbLeaseKeepAliveRequest,
    LeaseKeepAliveResponse as PbLeaseKeepAliveResponse, LeaseLeasesRequest as PbLeaseLeasesRequest,
    LeaseLeasesResponse as PbLeaseLeasesResponse, LeaseRevokeRequest as PbLeaseRevokeRequest,
    LeaseRevokeResponse as PbLeaseRevokeResponse, LeaseStatus,
    LeaseTimeToLiveRequest as PbLeaseTimeToLiveRequest,
    LeaseTimeToLiveResponse as PbLeaseTimeToLiveResponse,
};

use crate::rpc::ResponseHeader;
use crate::Error;

use tokio::sync::mpsc::channel;
use tonic::transport::Channel;
use tonic::{Interceptor, IntoRequest, Request};

/// Client for lease operations.
#[repr(transparent)]
pub struct LeaseClient {
    inner: PbLeaseClient<Channel>,
}

impl LeaseClient {
    /// Creates a `LeaseClient`.
    #[inline]
    pub fn new(channel: Channel, interceptor: Option<Interceptor>) -> Self {
        let inner = match interceptor {
            Some(it) => PbLeaseClient::with_interceptor(channel, it),
            None => PbLeaseClient::new(channel),
        };

        Self { inner }
    }

    /// `grant` creates a lease which expires if the server does not receive a keepAlive
    /// within a given time to live period. All keys attached to the lease will be expired and
    /// deleted if the lease expires. Each expired key generates a delete event in the event history.
    #[inline]
    pub async fn grant(
        &mut self,
        ttl: i64,
        options: Option<LeaseGrantOptions>,
    ) -> Result<LeaseGrantResponse> {
        let resp = self
            .inner
            .lease_grant(options.unwrap_or_default().with_ttl(ttl))
            .await?
            .into_inner();
        Ok(LeaseGrantResponse::new(resp))
    }

    /// `revoke` revokes a lease. All keys attached to the lease will expire and be deleted.
    #[inline]
    pub async fn revoke(
        &mut self,
        id: i64,
        options: Option<LeaseRevokeOptions>,
    ) -> Result<LeaseRevokeResponse> {
        let resp = self
            .inner
            .lease_revoke(options.unwrap_or_default().with_id(id))
            .await?
            .into_inner();
        Ok(LeaseRevokeResponse::new(resp))
    }

    /// `keep_alive` keeps the lease alive by streaming keep alive requests from the client
    /// to the server and streaming keep alive responses from the server to the client.
    #[inline]
    pub async fn keep_alive(
        &mut self,
        id: i64,
        options: Option<LeaseKeepAliveOptions>,
    ) -> Result<LeaseKeepAliveResponse> {
        let (mut sender, receiver) = channel::<PbLeaseKeepAliveRequest>(100);
        sender
            .send(options.unwrap_or_default().with_id(id).into())
            .await
            .map_err(|e| Error::LeaseKeepAliveError(e.to_string()))?;

        let mut stream = self.inner.lease_keep_alive(receiver).await?.into_inner();
        let resp = match stream.message().await? {
            Some(resp) => resp,
            None => {
                return Err(Error::LeaseKeepAliveError(
                    "failed to create lease keep alive".to_string(),
                ));
            }
        };

        Ok(LeaseKeepAliveResponse::new(resp))
    }

    /// `time_to_live` retrieves lease information.
    pub async fn time_to_live(
        &mut self,
        id: i64,
        options: Option<LeaseTimeToLiveOptions>,
    ) -> Result<LeaseTimeToLiveResponse> {
        let resp = self
            .inner
            .lease_time_to_live(options.unwrap_or_default().with_id(id))
            .await?
            .into_inner();
        Ok(LeaseTimeToLiveResponse::new(resp))
    }

    /// `leases` lists all existing leases.
    pub async fn leases(&mut self) -> Result<LeaseLeasesResponse> {
        let resp = self
            .inner
            .lease_leases(PbLeaseLeasesRequest {})
            .await?
            .into_inner();
        Ok(LeaseLeasesResponse::new(resp))
    }
}

/// Options for `leaseGrant` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct LeaseGrantOptions(PbLeaseGrantRequest);

impl LeaseGrantOptions {
    /// Set ttl
    #[inline]
    fn with_ttl(mut self, ttl: i64) -> Self {
        self.0.ttl = ttl;
        self
    }

    /// Set id
    pub fn with_id(mut self, id: i64) -> Self {
        self.0.id = id;
        self
    }

    /// Creates a `LeaseGrantOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbLeaseGrantRequest { ttl: 0, id: 0 })
    }
}

impl From<LeaseGrantOptions> for PbLeaseGrantRequest {
    #[inline]
    fn from(options: LeaseGrantOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbLeaseGrantRequest> for LeaseGrantOptions {
    #[inline]
    fn into_request(self) -> Request<PbLeaseGrantRequest> {
        Request::new(self.into())
    }
}

/// Response for `leaseGrant` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseGrantResponse(PbLeaseGrantResponse);

impl LeaseGrantResponse {
    /// Create a new `LeaseGrantResponse` from pb lease grant response.
    #[inline]
    const fn new(resp: PbLeaseGrantResponse) -> Self {
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

    /// TTL is the server chosen lease time-to-live in seconds
    #[inline]
    pub const fn ttl(&self) -> i64 {
        self.0.ttl
    }

    /// ID is the lease ID for the granted lease.
    #[inline]
    pub const fn id(&self) -> i64 {
        self.0.id
    }

    /// Error message if return error.
    #[inline]
    pub fn error(&self) -> &[u8] {
        self.0.error.as_ref()
    }
}

/// Options for `leaseRevoke` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct LeaseRevokeOptions(PbLeaseRevokeRequest);

impl LeaseRevokeOptions {
    /// Set id
    fn with_id(mut self, id: i64) -> Self {
        self.0.id = id;
        self
    }

    /// Creates a `LeaseRevokeOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbLeaseRevokeRequest { id: 0 })
    }
}

impl From<LeaseRevokeOptions> for PbLeaseRevokeRequest {
    #[inline]
    fn from(options: LeaseRevokeOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbLeaseRevokeRequest> for LeaseRevokeOptions {
    #[inline]
    fn into_request(self) -> Request<PbLeaseRevokeRequest> {
        Request::new(self.into())
    }
}

/// Response for `leaseRevoke` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseRevokeResponse(PbLeaseRevokeResponse);

impl LeaseRevokeResponse {
    /// Create a new `LeaseRevokeResponse` from pb lease revoke response.
    #[inline]
    const fn new(resp: PbLeaseRevokeResponse) -> Self {
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
}

/// Options for `leaseKeepAlive` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct LeaseKeepAliveOptions(PbLeaseKeepAliveRequest);

impl LeaseKeepAliveOptions {
    /// Set id
    fn with_id(mut self, id: i64) -> Self {
        self.0.id = id;
        self
    }

    /// Creates a `LeaseKeepAliveOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbLeaseKeepAliveRequest { id: 0 })
    }
}

impl From<LeaseKeepAliveOptions> for PbLeaseKeepAliveRequest {
    #[inline]
    fn from(options: LeaseKeepAliveOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbLeaseKeepAliveRequest> for LeaseKeepAliveOptions {
    #[inline]
    fn into_request(self) -> Request<PbLeaseKeepAliveRequest> {
        Request::new(self.into())
    }
}

/// Response for `leaseKeepAlive` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseKeepAliveResponse(PbLeaseKeepAliveResponse);

impl LeaseKeepAliveResponse {
    /// Create a new `LeaseKeepAliveResponse` from pb lease KeepAlive response.
    #[inline]
    const fn new(resp: PbLeaseKeepAliveResponse) -> Self {
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

    /// TTL is the new time-to-live for the lease.
    #[inline]
    pub const fn ttl(&self) -> i64 {
        self.0.ttl
    }

    /// ID is the lease ID for the keep alive request.
    #[inline]
    pub const fn id(&self) -> i64 {
        self.0.id
    }
}

/// Options for `leaseTimeToLive` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct LeaseTimeToLiveOptions(PbLeaseTimeToLiveRequest);

impl LeaseTimeToLiveOptions {
    /// ID is the lease ID for the lease.
    fn with_id(mut self, id: i64) -> Self {
        self.0.id = id;
        self
    }

    /// keys is true to query all the keys attached to this lease.
    pub fn with_keys(mut self, keys: bool) -> Self {
        self.0.keys = keys;
        self
    }

    /// Creates a `LeaseTimeToLiveOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbLeaseTimeToLiveRequest { id: 0, keys: false })
    }
}

impl From<LeaseTimeToLiveOptions> for PbLeaseTimeToLiveRequest {
    #[inline]
    fn from(options: LeaseTimeToLiveOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbLeaseTimeToLiveRequest> for LeaseTimeToLiveOptions {
    #[inline]
    fn into_request(self) -> Request<PbLeaseTimeToLiveRequest> {
        Request::new(self.into())
    }
}

/// Response for `leaseTimeToLive` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseTimeToLiveResponse(PbLeaseTimeToLiveResponse);

impl LeaseTimeToLiveResponse {
    /// Create a new `LeaseTimeToLiveResponse` from pb lease TimeToLive response.
    #[inline]
    const fn new(resp: PbLeaseTimeToLiveResponse) -> Self {
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

    /// TTL is the remaining TTL in seconds for the lease; the lease will expire in under TTL+1 seconds.
    #[inline]
    pub const fn ttl(&self) -> i64 {
        self.0.ttl
    }

    /// ID is the lease ID from the keep alive request.
    #[inline]
    pub const fn id(&self) -> i64 {
        self.0.id
    }

    /// GrantedTTL is the initial granted time in seconds upon lease creation/renewal.
    #[inline]
    pub const fn granted_ttl(&self) -> i64 {
        self.0.granted_ttl
    }

    /// Keys is the list of keys attached to this lease.
    #[inline]
    pub fn keys(&self, idx: usize) -> &[u8] {
        self.0.keys.get(idx).unwrap()
    }
}

/// Response for `leaseLeases` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct LeaseLeasesResponse(PbLeaseLeasesResponse);

impl LeaseLeasesResponse {
    /// Create a new `LeaseLeasesResponse` from pb lease Leases response.
    #[inline]
    const fn new(resp: PbLeaseLeasesResponse) -> Self {
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

    /// get leases status
    #[inline]
    pub fn take_leases(&self) -> &[LeaseStatus] {
        unsafe { &*(self.0.leases.as_slice() as *const _ as *const [LeaseStatus]) }
    }
}

