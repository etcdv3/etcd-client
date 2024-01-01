//! An [etcd](https://github.com/etcd-io/etcd) v3 API client for Rust.
//! It provides asynchronous client backed by [tokio](https://github.com/tokio-rs/tokio) and [tonic](https://github.com/hyperium/tonic).
//!
//! # Supported APIs
//!
//! - KV
//! - Watch
//! - Lease
//! - Auth
//! - Maintenance
//! - Cluster
//! - Lock
//! - Election
//!
//! # Usage
//!
//! Add this to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! etcd-client = "0.12"
//! tokio = { version = "1.0", features = ["full"] }
//! ```
//!
//! To get started using `etcd-client`:
//!
//! ```rust
//! use etcd_client::{Client, Error};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let mut client = Client::connect(["localhost:2379"], None).await?;
//!     // put kv
//!     client.put("foo", "bar", None).await?;
//!     // get kv
//!     let resp = client.get("foo", None).await?;
//!     if let Some(kv) = resp.kvs().first() {
//!         println!("Get kv: {{{}: {}}}", kv.key_str()?, kv.value_str()?);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Examples
//!
//! Examples can be found in [`etcd-client/examples`](https://github.com/etcdv3/etcd-client/tree/master/examples).
//!
//! # Feature Flags
//!
//! - `tls`: Enables the `rustls`-based TLS connection. Not
//! enabled by default.
//! - `tls-roots`: Adds system trust roots to `rustls`-based TLS connection using the
//! `rustls-native-certs` crate. Not enabled by default.
//! - `pub-response-field`: Exposes structs used to create regular `etcd-client` responses
//! including internal protobuf representations. Useful for mocking. Not enabled by default.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod auth;
mod channel;
mod client;
mod error;
mod namespace;
mod openssl_tls;
mod rpc;
mod vec;

pub use crate::client::{Client, ConnectOptions};
pub use crate::error::Error;
pub use crate::namespace::{KvClientPrefix, LeaseClientPrefix};
pub use crate::rpc::auth::{
    AuthClient, AuthDisableResponse, AuthEnableResponse, AuthenticateResponse, Permission,
    PermissionType, RoleAddResponse, RoleDeleteResponse, RoleGetResponse,
    RoleGrantPermissionResponse, RoleListResponse, RoleRevokePermissionOptions,
    RoleRevokePermissionResponse, UserAddOptions, UserAddResponse, UserChangePasswordResponse,
    UserDeleteResponse, UserGetResponse, UserGrantRoleResponse, UserListResponse,
    UserRevokeRoleResponse,
};
pub use crate::rpc::cluster::{
    ClusterClient, Member, MemberAddOptions, MemberAddResponse, MemberListResponse,
    MemberPromoteResponse, MemberRemoveResponse, MemberUpdateResponse,
};
pub use crate::rpc::election::{
    CampaignResponse, ElectionClient, LeaderKey, LeaderResponse, ObserveStream, ProclaimOptions,
    ProclaimResponse, ResignOptions, ResignResponse,
};
pub use crate::rpc::kv::{
    CompactionOptions, CompactionResponse, Compare, CompareOp, DeleteOptions, DeleteResponse,
    GetOptions, GetResponse, KvClient, PutOptions, PutResponse, SortOrder, SortTarget, Txn, TxnOp,
    TxnOpResponse, TxnResponse,
};
pub use crate::rpc::lease::{
    LeaseClient, LeaseGrantOptions, LeaseGrantResponse, LeaseKeepAliveResponse,
    LeaseKeepAliveStream, LeaseKeeper, LeaseLeasesResponse, LeaseRevokeResponse, LeaseStatus,
    LeaseTimeToLiveOptions, LeaseTimeToLiveResponse,
};
pub use crate::rpc::lock::{LockClient, LockOptions, LockResponse, UnlockResponse};
pub use crate::rpc::maintenance::{
    AlarmAction, AlarmMember, AlarmOptions, AlarmResponse, AlarmType, DefragmentResponse,
    HashKvResponse, HashResponse, MaintenanceClient, MoveLeaderResponse, SnapshotResponse,
    SnapshotStreaming, StatusResponse,
};
pub use crate::rpc::watch::{
    Event, EventType, WatchClient, WatchFilterType, WatchOptions, WatchResponse, WatchStream,
    Watcher,
};
pub use crate::rpc::{KeyValue, ResponseHeader};

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub use tonic::transport::{Certificate, ClientTlsConfig as TlsOptions, Identity};

#[cfg(feature = "tls-openssl")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls-openssl")))]
pub use crate::openssl_tls::{OpenSslClientConfig, OpenSslResult, SslConnectorBuilder};

/// Exposes internal protobuf representations used to create regular public response types.
#[cfg(feature = "pub-response-field")]
#[cfg_attr(docsrs, doc(cfg(feature = "pub-response-field")))]
pub mod proto {
    pub use crate::rpc::pb::etcdserverpb::AlarmMember as PbAlarmMember;
    pub use crate::rpc::pb::etcdserverpb::{
        AlarmResponse as PbAlarmResponse, AuthDisableResponse as PbAuthDisableResponse,
        AuthEnableResponse as PbAuthEnableResponse, AuthRoleAddResponse as PbAuthRoleAddResponse,
        AuthRoleDeleteResponse as PbAuthRoleDeleteResponse,
        AuthRoleGetResponse as PbAuthRoleGetResponse,
        AuthRoleGrantPermissionResponse as PbAuthRoleGrantPermissionResponse,
        AuthRoleListResponse as PbAuthRoleListResponse,
        AuthRoleRevokePermissionResponse as PbAuthRoleRevokePermissionResponse,
        AuthUserAddResponse as PbAuthUserAddResponse,
        AuthUserChangePasswordResponse as PbAuthUserChangePasswordResponse,
        AuthUserDeleteResponse as PbAuthUserDeleteResponse,
        AuthUserGetResponse as PbAuthUserGetResponse,
        AuthUserGrantRoleResponse as PbAuthUserGrantRoleResponse,
        AuthUserListResponse as PbAuthUserListResponse,
        AuthUserRevokeRoleResponse as PbAuthUserRevokeRoleResponse,
        AuthenticateResponse as PbAuthenticateResponse, CompactionResponse as PbCompactionResponse,
        Compare as PbCompare, DefragmentResponse as PbDefragmentResponse,
        DeleteRangeResponse as PbDeleteResponse, HashKvResponse as PbHashKvResponse,
        HashResponse as PbHashResponse, LeaseGrantResponse as PbLeaseGrantResponse,
        LeaseKeepAliveResponse as PbLeaseKeepAliveResponse,
        LeaseLeasesResponse as PbLeaseLeasesResponse, LeaseRevokeResponse as PbLeaseRevokeResponse,
        LeaseStatus as PbLeaseStatus, LeaseTimeToLiveResponse as PbLeaseTimeToLiveResponse,
        Member as PbMember, MemberAddResponse as PbMemberAddResponse,
        MemberListResponse as PbMemberListResponse,
        MemberPromoteResponse as PbMemberPromoteResponse,
        MemberRemoveResponse as PbMemberRemoveResponse,
        MemberUpdateResponse as PbMemberUpdateResponse, MoveLeaderResponse as PbMoveLeaderResponse,
        PutResponse as PbPutResponse, RangeResponse as PbRangeResponse,
        ResponseHeader as PbResponseHeader, SnapshotResponse as PbSnapshotResponse,
        StatusResponse as PbStatusResponse, TxnResponse as PbTxnResponse,
        WatchResponse as PbWatchResponse,
    };
    pub use crate::rpc::pb::mvccpb::Event as PbEvent;
    pub use crate::rpc::pb::mvccpb::KeyValue as PbKeyValue;
    pub use crate::rpc::pb::v3electionpb::{
        CampaignResponse as PbCampaignResponse, LeaderKey as PbLeaderKey,
        LeaderResponse as PbLeaderResponse, ProclaimResponse as PbProclaimResponse,
        ResignResponse as PbResignResponse,
    };
    pub use crate::rpc::pb::v3lockpb::{
        LockResponse as PbLockResponse, UnlockResponse as PbUnlockResponse,
    };
}
