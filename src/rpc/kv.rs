//! Etcd KV RPC.

use super::pb::etcdserverpb;
use super::pb::mvccpb;

pub use etcdserverpb::kv_client::KvClient;
pub use mvccpb::KeyValue;
