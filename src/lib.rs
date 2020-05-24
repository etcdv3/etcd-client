//! An etcd v3 API client library.

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
    HashKvResponse, HashResponse, SnapshotResponse, SnapshotStreaming, StatusResponse,
};
pub use crate::rpc::watch::{
    Event, EventType, WatchFilterType, WatchOptions, WatchResponse, WatchStream, Watcher,
};
pub use crate::rpc::{KeyValue, ResponseHeader};
