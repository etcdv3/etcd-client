//! Asynchronous client & synchronous client.

use crate::error::{Error, Result};
use crate::rpc::kv::{
    CompactionOptions, CompactionResponse, DeleteOptions, DeleteResponse, GetOptions, GetResponse,
    KvClient, PutOptions, PutResponse, Txn, TxnResponse,
};
use crate::rpc::watch::{
    SyncWatcher, WatchClient, WatchIterator, WatchOptions, WatchStream, Watcher,
};
use crate::FutureBlockOn;
use tokio::runtime::Runtime;
use tonic::transport::Channel;

/// Asynchronous `etcd` client using v3 API.
pub struct AsyncClient {
    kv: KvClient,
    watch: WatchClient,
}

impl AsyncClient {
    /// Connect to `etcd` servers from given `endpoints`.
    pub async fn connect<E: AsRef<str>, S: AsRef<[E]>>(endpoints: S) -> Result<Self> {
        const HTTP_PREFIX: &str = "http://";

        let endpoints = {
            let mut eps = Vec::new();
            for e in endpoints.as_ref() {
                let e = e.as_ref();
                let channel = if e.starts_with(HTTP_PREFIX) {
                    Channel::builder(e.parse()?)
                } else {
                    let e = HTTP_PREFIX.to_owned() + e;
                    Channel::builder(e.parse()?)
                };
                eps.push(channel);
            }
            eps
        };

        let channel = match endpoints.len() {
            0 => return Err(Error::InvalidArgs(String::from("empty endpoints"))),
            1 => endpoints[0].connect().await?,
            _ => Channel::balance_list(endpoints.into_iter()),
        };

        let kv = KvClient::new(channel.clone(), None);
        let watch = WatchClient::new(channel, None);

        Ok(Self { kv, watch })
    }

    /// Put the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    #[inline]
    pub async fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        options: Option<PutOptions>,
    ) -> Result<PutResponse> {
        self.kv.put(key, value, options).await
    }

    /// Gets the key from the key-value store.
    #[inline]
    pub async fn get(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<GetOptions>,
    ) -> Result<GetResponse> {
        self.kv.get(key, options).await
    }

    /// Deletes the given key from the key-value store.
    #[inline]
    pub async fn delete(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResponse> {
        self.kv.delete(key, options).await
    }

    /// Compacts the event history in the etcd key-value store. The key-value
    /// store should be periodically compacted or the event history will continue to grow
    /// indefinitely.
    #[inline]
    pub async fn compact(
        &mut self,
        revision: i64,
        options: Option<CompactionOptions>,
    ) -> Result<CompactionResponse> {
        self.kv.compact(revision, options).await
    }

    /// Processes multiple operations in a single transaction.
    /// A txn request increments the revision of the key-value store
    /// and generates events with the same revision for every completed operation.
    /// It is not allowed to modify the same key several times within one txn.
    #[inline]
    pub async fn txn(&mut self, txn: Txn) -> Result<TxnResponse> {
        self.kv.txn(txn).await
    }

    /// Watches for events happening or that have happened. Both input and output
    /// are streams; the input stream is for creating and canceling watcher and the output
    /// stream sends events. The entire event history can be watched starting from the
    /// last compaction revision.
    #[inline]
    pub async fn watch(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<WatchOptions>,
    ) -> Result<(Watcher, WatchStream)> {
        self.watch.watch(key, options).await
    }
}

/// Synchronous `etcd` client using v3 API.
pub struct Client {
    runtime: Runtime,
    async_client: AsyncClient,
}

impl Client {
    /// Connect to `etcd` servers from given `endpoints`.
    #[inline]
    pub fn connect<E: AsRef<str>, S: AsRef<[E]>>(endpoints: S) -> Result<Self> {
        let mut runtime = Runtime::new()?;

        let async_client = AsyncClient::connect(endpoints).block_on(&mut runtime)?;

        Ok(Self {
            runtime,
            async_client,
        })
    }

    /// Put the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    #[inline]
    pub fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        options: Option<PutOptions>,
    ) -> Result<PutResponse> {
        self.async_client
            .put(key, value, options)
            .block_on(&mut self.runtime)
    }

    /// Gets the key from the key-value store.
    #[inline]
    pub fn get(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<GetOptions>,
    ) -> Result<GetResponse> {
        self.async_client
            .get(key, options)
            .block_on(&mut self.runtime)
    }

    /// Deletes the given key from the key-value store.
    #[inline]
    pub fn delete(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResponse> {
        self.async_client
            .delete(key, options)
            .block_on(&mut self.runtime)
    }

    /// Compacts the event history in the etcd key-value store. The key-value
    /// store should be periodically compacted or the event history will continue to grow
    /// indefinitely.
    #[inline]
    pub fn compact(
        &mut self,
        revision: i64,
        options: Option<CompactionOptions>,
    ) -> Result<CompactionResponse> {
        self.async_client
            .compact(revision, options)
            .block_on(&mut self.runtime)
    }

    /// Processes multiple operations in a single transaction.
    /// A txn request increments the revision of the key-value store
    /// and generates events with the same revision for every completed operation.
    /// It is not allowed to modify the same key several times within one txn.
    #[inline]
    pub fn txn(&mut self, txn: Txn) -> Result<TxnResponse> {
        self.async_client.txn(txn).block_on(&mut self.runtime)
    }

    /// Watches for events happening or that have happened. Both input and output
    /// are streams; the input stream is for creating and canceling watcher and the output
    /// stream sends events. The entire event history can be watched starting from the
    /// last compaction revision.
    #[inline]
    pub fn watch(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<WatchOptions>,
    ) -> Result<(SyncWatcher, WatchIterator)> {
        let (w, s) = self
            .async_client
            .watch(key, options)
            .block_on(&mut self.runtime)?;
        Ok((SyncWatcher::new(w)?, WatchIterator::new(s)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{get_client, Compare, CompareOp, TxnOp, TxnOpResponse};

    #[test]
    fn test_put() {
        let mut client = get_client();
        client.put("put", "123", None).unwrap();

        // overwrite with prev key
        {
            let resp = client
                .put("put", "456", Some(PutOptions::new().with_prev_key()))
                .unwrap();
            let prev_key = resp.prev_key();
            assert!(prev_key.is_some());
            let prev_key = prev_key.unwrap();
            assert_eq!(prev_key.key(), b"put");
            assert_eq!(prev_key.value(), b"123");
        }

        // overwrite again with prev key
        {
            let resp = client
                .put("put", "789", Some(PutOptions::new().with_prev_key()))
                .unwrap();
            let prev_key = resp.prev_key();
            assert!(prev_key.is_some());
            let prev_key = prev_key.unwrap();
            assert_eq!(prev_key.key(), b"put");
            assert_eq!(prev_key.value(), b"456");
        }
    }

    #[test]
    fn test_get() {
        let mut client = get_client();
        client.put("get10", "10", None).unwrap();
        client.put("get11", "11", None).unwrap();
        client.put("get20", "20", None).unwrap();
        client.put("get21", "21", None).unwrap();

        // get key
        {
            let resp = client.get("get11", None).unwrap();
            assert_eq!(resp.count(), 1);
            assert_eq!(resp.more(), false);
            assert_eq!(resp.kvs().len(), 1);
            assert_eq!(resp.kvs()[0].key(), b"get11");
            assert_eq!(resp.kvs()[0].value(), b"11");
        }

        // get from key
        {
            let resp = client
                .get(
                    "get11",
                    Some(GetOptions::new().with_from_key().with_limit(2)),
                )
                .unwrap();
            assert_eq!(resp.more(), true);
            assert_eq!(resp.kvs().len(), 2);
            assert_eq!(resp.kvs()[0].key(), b"get11");
            assert_eq!(resp.kvs()[0].value(), b"11");
            assert_eq!(resp.kvs()[1].key(), b"get20");
            assert_eq!(resp.kvs()[1].value(), b"20");
        }

        // get prefix keys
        {
            let resp = client
                .get("get1", Some(GetOptions::new().with_prefix()))
                .unwrap();
            assert_eq!(resp.count(), 2);
            assert_eq!(resp.more(), false);
            assert_eq!(resp.kvs().len(), 2);
            assert_eq!(resp.kvs()[0].key(), b"get10");
            assert_eq!(resp.kvs()[0].value(), b"10");
            assert_eq!(resp.kvs()[1].key(), b"get11");
            assert_eq!(resp.kvs()[1].value(), b"11");
        }
    }

    #[test]
    fn test_delete() {
        let mut client = get_client();
        client.put("del10", "10", None).unwrap();
        client.put("del11", "11", None).unwrap();
        client.put("del20", "20", None).unwrap();
        client.put("del21", "21", None).unwrap();

        // delete key
        {
            let resp = client.delete("del11", None).unwrap();
            assert_eq!(resp.deleted(), 1);
            let resp = client
                .get("del11", Some(GetOptions::new().with_count_only()))
                .unwrap();
            assert_eq!(resp.count(), 0);
        }

        // delete a range of keys
        {
            let resp = client
                .delete("del11", Some(DeleteOptions::new().with_range("del22")))
                .unwrap();
            assert_eq!(resp.deleted(), 2);
            let resp = client
                .get(
                    "del11",
                    Some(GetOptions::new().with_range("del22").with_count_only()),
                )
                .unwrap();
            assert_eq!(resp.count(), 0);
        }
    }

    #[test]
    fn test_compact() {
        let mut client = get_client();
        let rev0 = client
            .put("compact", "0", None)
            .unwrap()
            .header()
            .unwrap()
            .revision();
        let rev1 = client
            .put("compact", "1", None)
            .unwrap()
            .header()
            .unwrap()
            .revision();

        // before compacting
        let rev0_resp = client
            .get("compact", Some(GetOptions::new().with_revision(rev0)))
            .unwrap();
        assert_eq!(rev0_resp.kvs()[0].value(), b"0");
        let rev1_resp = client
            .get("compact", Some(GetOptions::new().with_revision(rev1)))
            .unwrap();
        assert_eq!(rev1_resp.kvs()[0].value(), b"1");

        client.compact(rev1, None).unwrap();

        // after compacting
        let result = client.get("compact", Some(GetOptions::new().with_revision(rev0)));
        assert!(result.is_err());
        let rev1_resp = client
            .get("compact", Some(GetOptions::new().with_revision(rev1)))
            .unwrap();
        assert_eq!(rev1_resp.kvs()[0].value(), b"1");
    }

    #[test]
    fn test_txn() {
        let mut client = get_client();
        client.put("txn01", "01", None).unwrap();

        // transaction 1
        {
            let resp = client
                .txn(
                    Txn::new()
                        .when(&[Compare::value("txn01", CompareOp::Equal, "01")][..])
                        .and_then(
                            &[TxnOp::put(
                                "txn01",
                                "02",
                                Some(PutOptions::new().with_prev_key()),
                            )][..],
                        )
                        .or_else(&[TxnOp::get("txn01", None)][..]),
                )
                .unwrap();

            assert!(resp.succeeded());
            let op_responses = resp.op_responses();
            assert_eq!(op_responses.len(), 1);

            match op_responses[0] {
                TxnOpResponse::Put(ref resp) => assert_eq!(resp.prev_key().unwrap().value(), b"01"),
                _ => panic!("unexpected response"),
            }

            let resp = client.get("txn01", None).unwrap();
            assert_eq!(resp.kvs()[0].key(), b"txn01");
            assert_eq!(resp.kvs()[0].value(), b"02");
        }

        // transaction 2
        {
            let resp = client
                .txn(
                    Txn::new()
                        .when(&[Compare::value("txn01", CompareOp::Equal, "01")][..])
                        .and_then(&[TxnOp::put("txn01", "02", None)][..])
                        .or_else(&[TxnOp::get("txn01", None)][..]),
                )
                .unwrap();

            assert!(!resp.succeeded());
            let op_responses = resp.op_responses();
            assert_eq!(op_responses.len(), 1);

            match op_responses[0] {
                TxnOpResponse::Get(ref resp) => assert_eq!(resp.kvs()[0].value(), b"02"),
                _ => panic!("unexpected response"),
            }
        }
    }

    #[test]
    fn test_watch() {
        let mut client = get_client();

        let (mut watcher, mut iter) = client.watch("watch01", None).unwrap();

        client.put("watch01", "01", None).unwrap();

        let resp = iter.next().unwrap().unwrap();
        assert_eq!(resp.watch_id(), watcher.watch_id());
        assert_eq!(resp.events().len(), 1);

        let kv = resp.events()[0].kv().unwrap();
        assert_eq!(kv.key(), b"watch01");
        assert_eq!(kv.value(), b"01");

        watcher.cancel().unwrap();

        let resp = iter.next().unwrap().unwrap();
        assert_eq!(resp.watch_id(), watcher.watch_id());
        assert!(resp.canceled());

        assert!(iter.next().is_none());
    }
}
