//! An etcd v3 API client library.

mod client;
mod error;
mod rpc;

pub use crate::client::{AsyncClient, Client};
pub use crate::error::Error;
pub use crate::rpc::kv::{PutOptions, PutResponse};
pub use crate::rpc::{KeyValue, ResponseHeader};
