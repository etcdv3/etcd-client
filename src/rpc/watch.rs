//! Etcd Watch RPC.

use super::pb::etcdserverpb;

pub use etcdserverpb::watch_client::WatchClient;
pub use etcdserverpb::watch_create_request::FilterType as WatchFilterType;
pub use etcdserverpb::watch_request::RequestUnion as WatchRequestUnion;
pub use etcdserverpb::WatchCancelRequest;
pub use etcdserverpb::WatchCreateRequest;
pub use etcdserverpb::WatchProgressRequest;
pub use etcdserverpb::WatchRequest;
pub use etcdserverpb::WatchResponse;
