//! Etcd Auth RPC.

use super::pb::authpb;
use super::pb::etcdserverpb;

pub use authpb::permission::Type as PermissionType;
pub use authpb::Permission;
pub use authpb::Role;
pub use authpb::User;
pub use authpb::UserAddOptions;
pub use etcdserverpb::auth_client::AuthClient;
