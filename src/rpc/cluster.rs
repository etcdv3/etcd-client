//! Etcd Cluster RPC.

use crate::caller::{ClientCaller, ClientCallerBuilder};
use crate::error::Result;
use crate::intercept::InterceptedChannel;
use crate::rpc::pb::etcdserverpb::cluster_client::ClusterClient as PbClusterClient;
use crate::rpc::pb::etcdserverpb::{
    Member as PbMember, MemberAddRequest as PbMemberAddRequest,
    MemberAddResponse as PbMemberAddResponse, MemberListRequest as PbMemberListRequest,
    MemberListResponse as PbMemberListResponse, MemberPromoteRequest as PbMemberPromoteRequest,
    MemberPromoteResponse as PbMemberPromoteResponse, MemberRemoveRequest as PbMemberRemoveRequest,
    MemberRemoveResponse as PbMemberRemoveResponse, MemberUpdateRequest as PbMemberUpdateRequest,
    MemberUpdateResponse as PbMemberUpdateResponse,
};
use crate::rpc::ResponseHeader;
use tonic::{IntoRequest, Request};

type Client = PbClusterClient<InterceptedChannel>;

/// Client for Cluster operations.
#[repr(transparent)]
#[derive(Clone)]
pub struct ClusterClient {
    inner: ClientCaller<Client>,
}

impl ClusterClient {
    /// Creates an Cluster client.
    #[inline]
    pub(crate) fn new(builder: ClientCallerBuilder) -> Self {
        Self {
            inner: builder.build(Client::new),
        }
    }

    /// Adds a new member into the cluster.
    #[inline]
    pub async fn member_add(
        &mut self,
        urls: impl Into<Vec<String>>,
        options: Option<MemberAddOptions>,
    ) -> Result<MemberAddResponse> {
        async fn member_add_impl(
            client: &mut Client,
            options: MemberAddOptions,
        ) -> Result<MemberAddResponse> {
            let resp = client.member_add(options).await?.into_inner();
            Ok(MemberAddResponse::new(resp))
        }
        self.inner
            .do_call(options.unwrap_or_default().with_urls(urls), member_add_impl)
            .await
    }

    /// Removes an existing member from the cluster.
    #[inline]
    pub async fn member_remove(&mut self, id: u64) -> Result<MemberRemoveResponse> {
        async fn member_remove_impl(
            client: &mut Client,
            options: MemberRemoveOptions,
        ) -> Result<MemberRemoveResponse> {
            let resp = client.member_remove(options).await?.into_inner();
            Ok(MemberRemoveResponse::new(resp))
        }
        self.inner
            .do_call(MemberRemoveOptions::new().with_id(id), member_remove_impl)
            .await
    }

    /// Updates the member configuration.
    #[inline]
    pub async fn member_update(
        &mut self,
        id: u64,
        url: impl Into<Vec<String>>,
    ) -> Result<MemberUpdateResponse> {
        async fn member_update_impl(
            client: &mut Client,
            options: MemberUpdateOptions,
        ) -> Result<MemberUpdateResponse> {
            let resp = client.member_update(options).await?.into_inner();
            Ok(MemberUpdateResponse::new(resp))
        }
        self.inner
            .do_call(
                MemberUpdateOptions::new().with_option(id, url),
                member_update_impl,
            )
            .await
    }

    /// Lists all the members in the cluster.
    #[inline]
    pub async fn member_list(&mut self) -> Result<MemberListResponse> {
        async fn member_list_impl(
            client: &mut Client,
            options: PbMemberListRequest,
        ) -> Result<MemberListResponse> {
            let resp = client.member_list(options).await?.into_inner();
            Ok(MemberListResponse::new(resp))
        }
        self.inner
            .do_call(
                PbMemberListRequest {
                    linearizable: false,
                },
                member_list_impl,
            )
            .await
    }

    /// Promotes a member from raft learner (non-voting) to raft voting member.
    #[inline]
    pub async fn member_promote(&mut self, id: u64) -> Result<MemberPromoteResponse> {
        async fn member_promote_impl(
            client: &mut Client,
            options: MemberPromoteOptions,
        ) -> Result<MemberPromoteResponse> {
            let resp = client.member_promote(options).await?.into_inner();
            Ok(MemberPromoteResponse::new(resp))
        }
        self.inner
            .do_call(MemberPromoteOptions::new().with_id(id), member_promote_impl)
            .await
    }
}

/// Options for `MemberAdd` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct MemberAddOptions(PbMemberAddRequest);

impl MemberAddOptions {
    #[inline]
    fn with_urls(mut self, urls: impl Into<Vec<String>>) -> Self {
        self.0.peer_ur_ls = urls.into();
        self
    }

    /// Creates a `MemberAddOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbMemberAddRequest {
            peer_ur_ls: Vec::new(),
            is_learner: false,
        })
    }

    /// Sets the member as a learner.
    #[inline]
    pub const fn with_is_learner(mut self) -> Self {
        self.0.is_learner = true;
        self
    }
}

impl From<MemberAddOptions> for PbMemberAddRequest {
    #[inline]
    fn from(options: MemberAddOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbMemberAddRequest> for MemberAddOptions {
    #[inline]
    fn into_request(self) -> Request<PbMemberAddRequest> {
        Request::new(self.into())
    }
}

/// Response for `MemberAdd` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct MemberAddResponse(PbMemberAddResponse);

impl MemberAddResponse {
    /// Create a new `MemberAddResponse` from pb cluster response.
    #[inline]
    const fn new(resp: PbMemberAddResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Get the member information of the added member.
    #[inline]
    pub fn member(&self) -> Option<&Member> {
        self.0.member.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// Get the member list after adding the new member.
    #[inline]
    pub fn member_list(&self) -> &[Member] {
        unsafe { &*(self.0.members.as_slice() as *const _ as *const [Member]) }
    }
}

/// Options for `MemberRemove` operation.
#[derive(Debug, Default, Clone)]
// #[repr(transparent)]
pub struct MemberRemoveOptions(PbMemberRemoveRequest);

impl MemberRemoveOptions {
    /// Set id
    #[inline]
    fn with_id(mut self, id: u64) -> Self {
        self.0.id = id;
        self
    }

    /// Creates a `MemberRemoveOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbMemberRemoveRequest { id: 0 })
    }
}

impl From<MemberRemoveOptions> for PbMemberRemoveRequest {
    #[inline]
    fn from(options: MemberRemoveOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbMemberRemoveRequest> for MemberRemoveOptions {
    #[inline]
    fn into_request(self) -> Request<PbMemberRemoveRequest> {
        Request::new(self.into())
    }
}

/// Response for `MemberRemove` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct MemberRemoveResponse(PbMemberRemoveResponse);

impl MemberRemoveResponse {
    /// Create a new `MemberRemoveResponse` from pb cluster response.
    #[inline]
    const fn new(resp: PbMemberRemoveResponse) -> Self {
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

    /// A list of all members after removing the member
    #[inline]
    pub fn members(&self) -> &[Member] {
        unsafe { &*(self.0.members.as_slice() as *const _ as *const [Member]) }
    }
}

/// Options for `MemberUpdate` operation.
#[derive(Debug, Default, Clone)]
// #[repr(transparent)]
pub struct MemberUpdateOptions(PbMemberUpdateRequest);

impl MemberUpdateOptions {
    #[inline]
    fn with_option(mut self, id: u64, url: impl Into<Vec<String>>) -> Self {
        self.0.id = id;
        self.0.peer_ur_ls = url.into();
        self
    }

    /// Creates a `MemberUpdateOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbMemberUpdateRequest {
            id: 0,
            peer_ur_ls: Vec::new(),
        })
    }
}

impl From<MemberUpdateOptions> for PbMemberUpdateRequest {
    #[inline]
    fn from(options: MemberUpdateOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbMemberUpdateRequest> for MemberUpdateOptions {
    #[inline]
    fn into_request(self) -> Request<PbMemberUpdateRequest> {
        Request::new(self.into())
    }
}

/// Response for `MemberUpdate` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct MemberUpdateResponse(PbMemberUpdateResponse);

impl MemberUpdateResponse {
    /// Create a new `MemberUpdateResponse` from pb cluster response.
    #[inline]
    const fn new(resp: PbMemberUpdateResponse) -> Self {
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

    /// A list of all members after updating the member.
    #[inline]
    pub fn members(&self) -> &[Member] {
        unsafe { &*(self.0.members.as_slice() as *const _ as *const [Member]) }
    }
}

/// Response for `MemberList` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct MemberListResponse(PbMemberListResponse);

impl MemberListResponse {
    /// Creates a new `MemberListResponse` from pb Member List response.
    #[inline]
    const fn new(resp: PbMemberListResponse) -> Self {
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

    /// A list of all members associated with the cluster.
    #[inline]
    pub fn members(&self) -> &[Member] {
        unsafe { &*(self.0.members.as_slice() as *const _ as *const [Member]) }
    }
}

/// Cluster member.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Clone, PartialEq)]
#[repr(transparent)]
pub struct Member(PbMember);

impl Member {
    /// Member id.
    #[inline]
    pub const fn id(&self) -> u64 {
        self.0.id
    }

    /// The human-readable name of the member. If the member is not started, the name will be an empty string.
    #[inline]
    pub fn name(&self) -> &str {
        &self.0.name
    }

    /// The list of URLs the member exposes to the cluster for communication.
    #[inline]
    pub fn peer_urls(&self) -> &[String] {
        &self.0.peer_ur_ls
    }

    /// The list of URLs the member exposes to clients for communication. If the member is not started, client URLs will be empty.
    #[inline]
    pub fn client_urls(&self) -> &[String] {
        &self.0.client_ur_ls
    }

    /// Indicates if the member is raft learner.
    #[inline]
    pub const fn is_learner(&self) -> bool {
        self.0.is_learner
    }
}

impl From<&PbMember> for &Member {
    #[inline]
    fn from(src: &PbMember) -> Self {
        unsafe { &*(src as *const _ as *const Member) }
    }
}

/// Options for `MemberPromote` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct MemberPromoteOptions(PbMemberPromoteRequest);

impl MemberPromoteOptions {
    /// Set id
    #[inline]
    fn with_id(mut self, id: u64) -> Self {
        self.0.id = id;
        self
    }

    /// Creates a `MemberPromoteOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbMemberPromoteRequest { id: 0 })
    }
}

impl From<MemberPromoteOptions> for PbMemberPromoteRequest {
    #[inline]
    fn from(options: MemberPromoteOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbMemberPromoteRequest> for MemberPromoteOptions {
    #[inline]
    fn into_request(self) -> Request<PbMemberPromoteRequest> {
        Request::new(self.into())
    }
}

/// Response for `MemberPromote` operation.
#[cfg_attr(feature = "pub-response-field", visible::StructFields(pub))]
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct MemberPromoteResponse(PbMemberPromoteResponse);

impl MemberPromoteResponse {
    /// Create a new `MemberPromoteResponse` from pb cluster response.
    #[inline]
    const fn new(resp: PbMemberPromoteResponse) -> Self {
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

    /// A list of all members after promoting the member.
    #[inline]
    pub fn members(&self) -> &[Member] {
        unsafe { &*(self.0.members.as_slice() as *const _ as *const [Member]) }
    }
}
