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
//! etcd-client = "0.5"
//! tokio = { version = "1.0", features = ["full"] }
//! ```
//!
//! To get started using `etcd-client`:
//!
//! ```Rust
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

mod client;
mod error;
mod rpc;

pub use crate::client::{Client, ConnectOptions};
pub use crate::error::Error;
pub use crate::rpc::auth::{
    AuthDisableResponse, AuthEnableResponse, Permission, PermissionType, RoleAddResponse,
    RoleDeleteResponse, RoleGetResponse, RoleGrantPermissionResponse, RoleListResponse,
    RoleRevokePermissionOptions, RoleRevokePermissionResponse, UserAddOptions, UserAddResponse,
    UserChangePasswordResponse, UserDeleteResponse, UserGetResponse, UserGrantRoleResponse,
    UserListResponse, UserRevokeRoleResponse,
};
pub use crate::rpc::cluster::{
    Member, MemberAddOptions, MemberAddResponse, MemberListResponse, MemberPromoteResponse,
    MemberRemoveResponse, MemberUpdateResponse,
};
pub use crate::rpc::election::{
    CampaignResponse, LeaderKey, LeaderResponse, ObserveStream, ProclaimOptions, ProclaimResponse,
    ResignOptions, ResignResponse,
};
pub use crate::rpc::kv::{
    CompactionOptions, CompactionResponse, Compare, CompareOp, DeleteOptions, DeleteResponse,
    GetOptions, GetResponse, PutOptions, PutResponse, SortOrder, SortTarget, Txn, TxnOp,
    TxnOpResponse, TxnResponse,
};
pub use crate::rpc::lease::{
    LeaseGrantOptions, LeaseGrantResponse, LeaseKeepAliveResponse, LeaseKeepAliveStream,
    LeaseKeeper, LeaseLeasesResponse, LeaseRevokeResponse, LeaseStatus, LeaseTimeToLiveOptions,
    LeaseTimeToLiveResponse,
};
pub use crate::rpc::lock::{LockOptions, LockResponse, UnlockResponse};
pub use crate::rpc::maintenance::{
    AlarmAction, AlarmMember, AlarmOptions, AlarmResponse, AlarmType, DefragmentResponse,
    HashKvResponse, HashResponse, MoveLeaderResponse, SnapshotResponse, SnapshotStreaming,
    StatusResponse,
};
pub use crate::rpc::watch::{
    Event, EventType, WatchFilterType, WatchOptions, WatchResponse, WatchStream, Watcher,
};
pub use crate::rpc::{KeyValue, ResponseHeader};
#[cfg(feature = "tls")]
pub use tonic::transport::{Certificate, ClientTlsConfig as TlsOptions, Identity};
