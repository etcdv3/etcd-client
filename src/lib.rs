//! An etcd v3 API client library.

mod client;
mod error;
mod rpc;

pub use crate::client::{AsyncClient, Client};
pub use crate::error::Error;
pub use crate::rpc::kv::{
    DeleteOptions, DeleteResponse, GetOptions, GetResponse, PutOptions, PutResponse, SortOrder,
    SortTarget,
};
pub use crate::rpc::{KeyValue, ResponseHeader};

/// Get client for testing.
#[doc(hidden)]
#[cfg(test)]
pub fn get_client() -> Client {
    Client::connect(["localhost:2379"]).expect("Failed to connect to `localhost:2379`")
}
