//! An etcd v3 API client library.

mod client;
mod error;
mod rpc;

pub use crate::client::{AsyncClient, Client};
pub use crate::error::Error;
pub use crate::rpc::kv::{
    CompactionOptions, CompactionResponse, Compare, CompareOp, DeleteOptions, DeleteResponse,
    GetOptions, GetResponse, PutOptions, PutResponse, SortOrder, SortTarget, Txn, TxnOp,
    TxnOpResponse, TxnResponse,
};
pub use crate::rpc::watch::{
    Event, EventType, SyncWatcher, WatchFilterType, WatchIterator, WatchOptions, WatchResponse,
    WatchStream, Watcher,
};
pub use crate::rpc::{KeyValue, ResponseHeader};
use std::future::Future;
use tokio::runtime::Runtime;

/// This trait provide `block_on` method for `Future`.
trait FutureBlockOn: Future {
    fn block_on(self, runtime: &mut Runtime) -> Self::Output;
}

impl<T: Future> FutureBlockOn for T {
    #[inline]
    fn block_on(self, runtime: &mut Runtime) -> Self::Output {
        runtime.block_on(self)
    }
}

/// Get client for testing.
#[doc(hidden)]
#[cfg(test)]
pub fn get_client() -> Client {
    Client::connect(["localhost:2379"]).expect("Failed to connect to `localhost:2379`")
}
