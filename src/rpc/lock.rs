//! Etcd Lock RPC.

use super::pb::v3lockpb;

pub use v3lockpb::lock_client::LockClient;
pub use v3lockpb::{LockRequest, LockResponse, UnlockRequest, UnlockResponse};
