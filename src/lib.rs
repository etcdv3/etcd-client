//! An etcd v3 API client library.

mod client;
mod error;
mod rpc;

pub use crate::client::Client;
pub use crate::error::Error;
pub use crate::rpc::kv::{
    CompactionOptions, CompactionResponse, Compare, CompareOp, DeleteOptions, DeleteResponse,
    GetOptions, GetResponse, PutOptions, PutResponse, SortOrder, SortTarget, Txn, TxnOp,
    TxnOpResponse, TxnResponse,
};
pub use crate::rpc::watch::{
    Event, EventType, WatchFilterType, WatchOptions, WatchResponse, WatchStream, Watcher,
};
pub use crate::rpc::{KeyValue, ResponseHeader};
