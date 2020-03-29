//! Etcd Auth RPC.

use super::pb::authpb;
use super::pb::etcdserverpb;

pub use authpb::permission::Type as PermissionType;
pub use authpb::Permission;
pub use authpb::Role;
pub use authpb::User;
pub use authpb::UserAddOptions;
pub use etcdserverpb::auth_client::AuthClient as PbAuthClient;

use etcdserverpb::{
    AuthDisableRequest as PbAuthDisableRequest, AuthDisableResponse as PbAuthDisableResponse,
    AuthEnableRequest as PbAuthEnableRequest, AuthEnableResponse as PbAuthEnableResponse,
    AuthenticateRequest as PbAuthenticateRequest, AuthenticateResponse as PbAuthenticateResponse,
};

use crate::error::Result;
use crate::rpc::ResponseHeader;
use std::string::String;
use tonic::transport::Channel;
use tonic::{Interceptor, IntoRequest, Request};

/// Client for Auth operations.
#[repr(transparent)]
pub struct AuthClient {
    inner: PbAuthClient<Channel>,
    // token: String
}

impl AuthClient {
    /// Creates an auth client.
    #[inline]
    pub fn new(channel: Channel, interceptor: Option<Interceptor>) -> Self {
        let inner = match interceptor {
            Some(it) => PbAuthClient::with_interceptor(channel, it),
            None => PbAuthClient::new(channel),
        };

        Self { inner }
    }

    /// auth_enable enables authentication.
    #[inline]
    pub async fn auth_enable(&mut self) -> Result<AuthEnableResponse> {
        let options: Option<AuthEnableOptions> = Some(AuthEnableOptions::new());
        let resp = self
            .inner
            .auth_enable(options.unwrap_or_default())
            .await?
            .into_inner();
        Ok(AuthEnableResponse::new(resp))
    }

    /// auth_disable disables authentication.
    #[inline]
    pub async fn auth_disable(&mut self) -> Result<AuthDisableResponse> {
        let options: Option<AuthDisableOptions> = Some(AuthDisableOptions::new());
        let resp = self
            .inner
            .auth_disable(options.unwrap_or_default())
            .await?
            .into_inner();
        Ok(AuthDisableResponse::new(resp))
    }

    /// Authenticate processes an authenticate request.
    #[inline]
    pub async fn authenticate(
        &mut self,
        name: String,
        password: String,
    ) -> Result<AuthenticateResponse> {
        let options: Option<AuthenticateOptions> = Some(AuthenticateOptions::new());
        let resp = self
            .inner
            .authenticate(options.unwrap_or_default().with_user(name, password))
            .await?
            .into_inner();
        Ok(AuthenticateResponse::new(resp))
    }
}

/// Options for `AuthEnable` operation.
#[derive(Debug, Default, Clone)]
// #[repr(transparent)]
pub struct AuthEnableOptions(PbAuthEnableRequest);

impl AuthEnableOptions {
    /// Creates a `AuthEnableOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbAuthEnableRequest {})
    }
}

impl From<AuthEnableOptions> for PbAuthEnableRequest {
    #[inline]
    fn from(options: AuthEnableOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbAuthEnableRequest> for AuthEnableOptions {
    #[inline]
    fn into_request(self) -> Request<PbAuthEnableRequest> {
        Request::new(self.into())
    }
}

/// Response for `AuthEnable` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct AuthEnableResponse(PbAuthEnableResponse);

impl AuthEnableResponse {
    /// Create a new `AuthEnableResponse` from pb auth response.
    #[inline]
    const fn new(resp: PbAuthEnableResponse) -> Self {
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

/// Options for `AuthDisable` operation.
#[derive(Debug, Default, Clone)]
// #[repr(transparent)]
pub struct AuthDisableOptions(PbAuthDisableRequest);

impl AuthDisableOptions {
    /// Creates a `AuthDisableOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbAuthDisableRequest {})
    }
}

impl From<AuthDisableOptions> for PbAuthDisableRequest {
    #[inline]
    fn from(options: AuthDisableOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbAuthDisableRequest> for AuthDisableOptions {
    #[inline]
    fn into_request(self) -> Request<PbAuthDisableRequest> {
        Request::new(self.into())
    }
}

/// Response for `AuthDisable` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct AuthDisableResponse(PbAuthDisableResponse);

impl AuthDisableResponse {
    /// Create a new `AuthDisableResponse` from pb auth response.
    #[inline]
    const fn new(resp: PbAuthDisableResponse) -> Self {
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

/// Options for `Authenticate` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct AuthenticateOptions(PbAuthenticateRequest);

impl AuthenticateOptions {
    /// Set user's name and password.
    #[inline]
    fn with_user(mut self, name: String, password: String) -> Self {
        self.0.name = name;
        self.0.password = password;
        self
    }

    /// Creates a `AuthenticateOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbAuthenticateRequest {
            name: String::new(),
            password: String::new(),
        })
    }
}

impl From<AuthenticateOptions> for PbAuthenticateRequest {
    #[inline]
    fn from(options: AuthenticateOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbAuthenticateRequest> for AuthenticateOptions {
    #[inline]
    fn into_request(self) -> Request<PbAuthenticateRequest> {
        Request::new(self.into())
    }
}

/// Response for `Authenticate` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct AuthenticateResponse(PbAuthenticateResponse);

impl AuthenticateResponse {
    /// Create a new `AuthenticateResponse` from pb auth response.
    #[inline]
    const fn new(resp: PbAuthenticateResponse) -> Self {
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

    /// token is an authorized token that can be used in succeeding RPCs
    #[inline]
    pub fn token(&self) -> &String {
        &self.0.token
    }
}
