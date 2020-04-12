//! Etcd Auth RPC.

use super::pb::authpb;
use super::pb::etcdserverpb;

pub use authpb::permission::Type as PermissionType;
pub use authpb::Permission as PbPermission;
pub use authpb::Role;
pub use authpb::User;
pub use authpb::UserAddOptions;
pub use etcdserverpb::auth_client::AuthClient as PbAuthClient;

use etcdserverpb::{
    AuthDisableRequest as PbAuthDisableRequest, AuthDisableResponse as PbAuthDisableResponse,
    AuthEnableRequest as PbAuthEnableRequest, AuthEnableResponse as PbAuthEnableResponse,
    AuthRoleAddRequest as PbAuthRoleAddRequest, AuthRoleAddResponse as PbAuthRoleAddResponse,
    AuthRoleDeleteRequest as PbAuthRoleDeleteRequest,
    AuthRoleDeleteResponse as PbAuthRoleDeleteResponse, AuthRoleGetRequest as PbAuthRoleGetRequest,
    AuthRoleGetResponse as PbAuthRoleGetResponse,
    AuthRoleGrantPermissionRequest as PbAuthRoleGrantPermissionRequest,
    AuthRoleGrantPermissionResponse as PbAuthRoleGrantPermissionResponse,
    AuthRoleListRequest as PbAuthRoleListRequest, AuthRoleListResponse as PbAuthRoleListResponse,
    AuthRoleRevokePermissionRequest as PbAuthRoleRevokePermissionRequest,
    AuthRoleRevokePermissionResponse as PbAuthRoleRevokePermissionResponse,
    AuthenticateRequest as PbAuthenticateRequest, AuthenticateResponse as PbAuthenticateResponse,
};

use crate::error::Result;
use crate::rpc::get_prefix;
use crate::rpc::ResponseHeader;
use std::fmt;
use std::string::String;
use tonic::transport::Channel;
use tonic::{Interceptor, IntoRequest, Request};

/// Client for Auth operations.
#[repr(transparent)]
pub struct AuthClient {
    inner: PbAuthClient<Channel>,
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

    /// Enables authentication.
    #[inline]
    pub async fn auth_enable(&mut self) -> Result<AuthEnableResponse> {
        let resp = self
            .inner
            .auth_enable(AuthEnableOptions::new())
            .await?
            .into_inner();
        Ok(AuthEnableResponse::new(resp))
    }

    /// Disables authentication.
    #[inline]
    pub async fn auth_disable(&mut self) -> Result<AuthDisableResponse> {
        let resp = self
            .inner
            .auth_disable(AuthDisableOptions::new())
            .await?
            .into_inner();
        Ok(AuthDisableResponse::new(resp))
    }

    /// Processes an authenticate request.
    #[inline]
    pub async fn authenticate(
        &mut self,
        name: String,
        password: String,
    ) -> Result<AuthenticateResponse> {
        let resp = self
            .inner
            .authenticate(AuthenticateOptions::new().with_user(name, password))
            .await?
            .into_inner();
        Ok(AuthenticateResponse::new(resp))
    }

    /// Add role
    #[inline]
    pub async fn role_add(&mut self, name: impl Into<String>) -> Result<RoleAddResponse> {
        let resp = self
            .inner
            .role_add(RoleAddOptions::new(name.into()))
            .await?
            .into_inner();
        Ok(RoleAddResponse::new(resp))
    }

    /// Delete role
    #[inline]
    pub async fn role_delete(&mut self, name: impl Into<String>) -> Result<RoleDeleteResponse> {
        let resp = self
            .inner
            .role_delete(RoleDeleteOptions::new(name.into()))
            .await?
            .into_inner();
        Ok(RoleDeleteResponse::new(resp))
    }

    /// Get role
    #[inline]
    pub async fn role_get(&mut self, name: impl Into<String>) -> Result<RoleGetResponse> {
        let resp = self
            .inner
            .role_get(RoleGetOptions::new(name.into()))
            .await?
            .into_inner();
        Ok(RoleGetResponse::new(resp))
    }

    /// List role
    #[inline]
    pub async fn role_list(&mut self) -> Result<RoleListResponse> {
        let resp = self
            .inner
            .role_list(AuthRoleListOptions {})
            .await?
            .into_inner();
        Ok(RoleListResponse::new(resp))
    }

    /// Grant role permission
    #[inline]
    pub async fn role_grant_permission(
        &mut self,
        name: impl Into<String>,
        perm: Permission,
    ) -> Result<RoleGrantPermissionResponse> {
        let resp = self
            .inner
            .role_grant_permission(RoleGrantPermissionOptions::new(name.into(), perm))
            .await?
            .into_inner();
        Ok(RoleGrantPermissionResponse::new(resp))
    }

    /// Revoke role permission
    #[inline]
    pub async fn role_revoke_permission(
        &mut self,
        name: impl Into<String>,
        key: impl Into<Vec<u8>>,
        options: Option<RoleRevokePermissionOption>,
    ) -> Result<RoleRevokePermissionResponse> {
        let resp = self
            .inner
            .role_revoke_permission(
                options
                    .unwrap_or_default()
                    .with_name(name.into())
                    .with_key(key.into()),
            )
            .await?
            .into_inner();
        Ok(RoleRevokePermissionResponse::new(resp))
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
    /// Creates a new `AuthenticateResponse` from pb auth response.
    #[inline]
    const fn new(resp: PbAuthenticateResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[allow(dead_code)]
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[allow(dead_code)]
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// An authorized token that can be used in succeeding RPCs
    #[inline]
    pub fn token(&self) -> &str {
        &self.0.token
    }
}

/// Options for `RoleAddOptions` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct RoleAddOptions(PbAuthRoleAddRequest);

impl RoleAddOptions {
    /// Creates a `RoleAddOptions`.
    #[inline]
    pub fn new(name: String) -> Self {
        Self(PbAuthRoleAddRequest { name })
    }
}

impl From<RoleAddOptions> for PbAuthRoleAddRequest {
    #[inline]
    fn from(options: RoleAddOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbAuthRoleAddRequest> for RoleAddOptions {
    #[inline]
    fn into_request(self) -> Request<PbAuthRoleAddRequest> {
        Request::new(self.into())
    }
}

/// Response for role add operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RoleAddResponse(PbAuthRoleAddResponse);

impl RoleAddResponse {
    /// Create a new `RoleAddResponse` from pb role add response.
    #[inline]
    const fn new(resp: PbAuthRoleAddResponse) -> Self {
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

/// Options for delete role operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct RoleDeleteOptions(PbAuthRoleDeleteRequest);

impl RoleDeleteOptions {
    /// Creates a `RoleDeleteOptions` to delete role.
    #[inline]
    pub fn new(name: String) -> Self {
        Self(PbAuthRoleDeleteRequest { role: name })
    }
}

impl From<RoleDeleteOptions> for PbAuthRoleDeleteRequest {
    #[inline]
    fn from(options: RoleDeleteOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbAuthRoleDeleteRequest> for RoleDeleteOptions {
    #[inline]
    fn into_request(self) -> Request<PbAuthRoleDeleteRequest> {
        Request::new(self.into())
    }
}

/// Response for delete role operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RoleDeleteResponse(PbAuthRoleDeleteResponse);

impl RoleDeleteResponse {
    /// Create a new `RoleDeleteResponse` from pb role delete response.
    #[inline]
    const fn new(resp: PbAuthRoleDeleteResponse) -> Self {
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

/// Options for get role operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct RoleGetOptions(PbAuthRoleGetRequest);

impl RoleGetOptions {
    /// Creates a `RoleGetOptions` to get role.
    #[inline]
    pub fn new(name: String) -> Self {
        Self(PbAuthRoleGetRequest { role: name })
    }
}

impl From<RoleGetOptions> for PbAuthRoleGetRequest {
    #[inline]
    fn from(options: RoleGetOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbAuthRoleGetRequest> for RoleGetOptions {
    #[inline]
    fn into_request(self) -> Request<PbAuthRoleGetRequest> {
        Request::new(self.into())
    }
}

/// Role access permission.
#[derive(Debug, Clone)]
pub struct Permission {
    inner: PbPermission,
    has_prefix: bool,
    has_range_end: bool,
}

impl From<i32> for PermissionType {
    #[inline]
    fn from(perm_type: i32) -> Self {
        match perm_type {
            0 => PermissionType::Read,
            1 => PermissionType::Write,
            2 => PermissionType::Readwrite,
            _ => unreachable!(),
        }
    }
}

impl Permission {
    /// Create a permission with operation type and key
    #[inline]
    pub fn new(perm_type: PermissionType, key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: PbPermission {
                perm_type: perm_type.into(),
                key: key.into(),
                range_end: Vec::new(),
            },
            has_prefix: false,
            has_range_end: false,
        }
    }

    /// Create a read permission with key
    #[inline]
    pub fn read(key: impl Into<Vec<u8>>) -> Self {
        Permission::new(PermissionType::Read, key)
    }

    /// Create a write permission with key
    #[inline]
    pub fn write(key: impl Into<Vec<u8>>) -> Self {
        Permission::new(PermissionType::Write, key)
    }

    /// Create a readwrite permission with key
    #[inline]
    pub fn readwrite(key: impl Into<Vec<u8>>) -> Self {
        Permission::new(PermissionType::Readwrite, key)
    }

    /// Set range end for the permission
    #[inline]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.inner.range_end = range_end.into();
        self.has_range_end = true;
        self.has_prefix = false;
        self
    }

    /// Set the permission with prefix
    #[inline]
    pub fn with_prefix(mut self) -> Self {
        // if true there should be no range end, set the range end
        self.has_prefix = true;
        self
    }

    /// The key in bytes. An empty key is not allowed.
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.inner.key
    }

    /// The range end in bytes. maybe empty
    #[inline]
    pub fn range_end(&self) -> &[u8] {
        &self.inner.range_end
    }

    /// The key in string. An empty key is not allowed.
    #[inline]
    pub fn key_str(&self) -> Result<&str> {
        std::str::from_utf8(self.key()).map_err(From::from)
    }

    /// The key in string. An empty key is not allowed.
    ///
    /// # Safety
    /// This function is unsafe because it does not check that the bytes of the key are valid UTF-8.
    /// If this constraint is violated, undefined behavior results,
    /// as the rest of Rust assumes that [`&str`]s are valid UTF-8.
    #[inline]
    pub unsafe fn key_str_unchecked(&self) -> &str {
        std::str::from_utf8_unchecked(self.key())
    }

    /// The range end in string.
    #[inline]
    pub fn range_end_str(&self) -> Result<&str> {
        std::str::from_utf8(self.range_end()).map_err(From::from)
    }

    /// The range end in string.
    ///
    /// # Safety
    /// This function is unsafe because it does not check that the bytes of the key are valid UTF-8.
    /// If this constraint is violated, undefined behavior results,
    /// as the rest of Rust assumes that [`&str`]s are valid UTF-8.
    #[inline]
    pub unsafe fn range_end_str_unchecked(&self) -> &str {
        std::str::from_utf8_unchecked(self.key())
    }

    /// Get the operation type of permission
    #[inline]
    pub const fn get_type(&self) -> i32 {
        self.inner.perm_type
    }

    /// Whether permission is a range scope
    #[inline]
    pub const fn is_range(&self) -> bool {
        self.has_range_end
    }

    /// Whether permission is a prefix scope
    #[inline]
    pub const fn is_prefix(&self) -> bool {
        self.has_prefix
    }
}

impl From<&PbPermission> for Permission {
    #[inline]
    fn from(src: &PbPermission) -> Self {
        let mut perm = Permission {
            inner: PbPermission {
                perm_type: src.perm_type,
                key: src.key.clone(),
                range_end: src.range_end.clone(),
            },
            has_range_end: false,
            has_prefix: false,
        };

        if !perm.inner.range_end.is_empty() {
            perm.has_range_end = true;
            let prefix = get_prefix(&perm.inner.key);
            if prefix.as_slice().eq(perm.inner.range_end.as_slice()) {
                perm.has_prefix = true;
            }
        }
        perm
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut text = String::new();
        if self.is_range() {
            text += "[";
            text += self.key_str().unwrap();
            text = text + "," + self.range_end_str().unwrap() + ")";

            if self.is_prefix() {
                text = text + " (prefix " + self.key_str().unwrap() + ")";
            }
        } else {
            text += self.key_str().unwrap();
        }

        write!(f, "{}", text)
    }
}

impl From<Permission> for PbPermission {
    #[inline]
    fn from(mut perm: Permission) -> Self {
        if perm.has_prefix {
            if perm.inner.key.is_empty() {
                perm.inner.key = vec![b'\0'];
                perm.inner.range_end = vec![b'\0'];
            } else {
                perm.inner.range_end = get_prefix(&perm.inner.key);
            }
        }
        perm.inner
    }
}

/// Response for get role operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RoleGetResponse(PbAuthRoleGetResponse);

impl RoleGetResponse {
    /// Create a new `RoleGetResponse` from pb role get response.
    #[inline]
    const fn new(resp: PbAuthRoleGetResponse) -> Self {
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

    /// The list of permissions by the `Get` request.
    #[inline]
    pub fn permissions(&self) -> Vec<Permission> {
        let mut perms = Vec::new();
        for p in &self.0.perm {
            perms.push(p.into());
        }
        perms
    }
}

impl fmt::Display for RoleGetResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut read_perm = String::from("KV Read:\n");
        let mut write_perm = String::from("KV Write:\n");
        for perm in self.permissions() {
            if perm.get_type() == 0 || perm.get_type() == 2 {
                read_perm = read_perm + perm.to_string().as_str() + "\n";
            }

            if perm.get_type() == 1 || perm.get_type() == 2 {
                write_perm = write_perm + perm.to_string().as_str() + "\n";
            }
        }

        write!(f, "{}\n{}", read_perm, write_perm)
    }
}

/// Options for list role operation.
use PbAuthRoleListRequest as AuthRoleListOptions;

/// Response for list role operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RoleListResponse(PbAuthRoleListResponse);

impl RoleListResponse {
    /// Create a new `RoleListResponse` from pb role list response.
    #[inline]
    const fn new(resp: PbAuthRoleListResponse) -> Self {
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

    /// Get roles in response
    #[inline]
    pub fn roles(&self) -> &[String] {
        self.0.roles.as_slice()
    }
}

/// Options for grant role permission operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct RoleGrantPermissionOptions(PbAuthRoleGrantPermissionRequest);

impl RoleGrantPermissionOptions {
    /// Create a "RoleGrantPermissionOptions" to grant role permission
    #[inline]
    pub fn new(name: String, perm: Permission) -> Self {
        Self(PbAuthRoleGrantPermissionRequest {
            name,
            perm: Some(perm.into()),
        })
    }
}

impl From<RoleGrantPermissionOptions> for PbAuthRoleGrantPermissionRequest {
    #[inline]
    fn from(options: RoleGrantPermissionOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbAuthRoleGrantPermissionRequest> for RoleGrantPermissionOptions {
    #[inline]
    fn into_request(self) -> Request<PbAuthRoleGrantPermissionRequest> {
        Request::new(self.into())
    }
}

/// Response for grant role permission operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RoleGrantPermissionResponse(PbAuthRoleGrantPermissionResponse);

impl RoleGrantPermissionResponse {
    /// Create a new `RoleGrantPermissionResponse` from pb role grant permission response.
    #[inline]
    const fn new(resp: PbAuthRoleGrantPermissionResponse) -> Self {
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

/// Options for grant role permission operation.
#[derive(Debug, Default, Clone)]
pub struct RoleRevokePermissionOption {
    req: PbAuthRoleRevokePermissionRequest,
    has_range_end: bool,
    has_prefix: bool,
}

impl RoleRevokePermissionOption {
    /// Create a new `RoleRevokePermissionOption` from pb role revoke permission.
    #[inline]
    pub fn new() -> Self {
        Self {
            req: PbAuthRoleRevokePermissionRequest {
                role: String::default(),
                key: Vec::new(),
                range_end: Vec::new(),
            },
            has_range_end: false,
            has_prefix: false,
        }
    }

    /// Sets name.
    #[inline]
    fn with_name(mut self, name: String) -> Self {
        self.req.role = name;
        self
    }

    /// Sets key.
    #[inline]
    fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.req.key = key.into();
        self
    }

    /// Set range end.
    #[inline]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.req.range_end = range_end.into();
        self.has_prefix = false;
        self
    }

    /// Set prefix.
    #[inline]
    pub fn with_prefix(mut self) -> Self {
        self.has_prefix = true;
        self
    }
}

impl From<RoleRevokePermissionOption> for PbAuthRoleRevokePermissionRequest {
    #[inline]
    fn from(mut option: RoleRevokePermissionOption) -> Self {
        if option.has_prefix {
            if option.req.key.is_empty() {
                option.req.key = vec![b'\0'];
                option.req.range_end = vec![b'\0'];
            } else {
                option.req.range_end = get_prefix(&option.req.key);
            }
        }
        option.req
    }
}

impl IntoRequest<PbAuthRoleRevokePermissionRequest> for RoleRevokePermissionOption {
    #[inline]
    fn into_request(self) -> Request<PbAuthRoleRevokePermissionRequest> {
        Request::new(self.into())
    }
}

/// Response for revoke role permission operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RoleRevokePermissionResponse(PbAuthRoleRevokePermissionResponse);

impl RoleRevokePermissionResponse {
    /// Create a new `RoleRevokePermissionResponse` from pb role revoke permission response.
    #[inline]
    const fn new(resp: PbAuthRoleRevokePermissionResponse) -> Self {
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
