//! Asynchronous client & synchronous client.

use crate::error::{Error, Result};
use crate::rpc::kv::{
    CompactionOptions, CompactionResponse, DeleteOptions, DeleteResponse, GetOptions, GetResponse,
    KvClient, PutOptions, PutResponse, Txn, TxnResponse,
};
use crate::rpc::lease::{
    LeaseClient, LeaseGrantOptions, LeaseGrantResponse, LeaseKeepAliveOptions,
    LeaseKeepAliveResponse, LeaseLeasesResponse, LeaseRevokeOptions,
    LeaseRevokeResponse, LeaseTimeToLiveOptions, LeaseTimeToLiveResponse,
};
use crate::rpc::watch::{WatchClient, WatchOptions, WatchStream, Watcher};
use tonic::transport::Channel;

/// Asynchronous `etcd` client using v3 API.
pub struct Client {
    kv: KvClient,
    watch: WatchClient,
    lease: LeaseClient,
}

impl Client {
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
        let watch = WatchClient::new(channel.clone(), None);
        let lease = LeaseClient::new(channel, None);

        Ok(Self { kv, watch, lease })
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

    /// `grant` creates a lease which expires if the server does not receive a keepAlive
    /// within a given time to live period. All keys attached to the lease will be expired and
    /// deleted if the lease expires. Each expired key generates a delete event in the event history.
    #[inline]
    pub async fn grant(
        &mut self,
        ttl: i64,
        options: Option<LeaseGrantOptions>,
    ) -> Result<LeaseGrantResponse> {
        self.lease.grant(ttl, options).await
    }

    /// `revoke` revokes a lease. All keys attached to the lease will expire and be deleted.
    #[inline]
    pub async fn revoke(
        &mut self,
        id: i64,
        options: Option<LeaseRevokeOptions>,
    ) -> Result<LeaseRevokeResponse> {
        self.lease.revoke(id, options).await
    }

    /// `keep_alive` keeps the lease alive by streaming keep alive requests from the client
    /// to the server and streaming keep alive responses from the server to the client.
    #[inline]
    pub async fn keep_alive(
        &mut self,
        id: i64,
        options: Option<LeaseKeepAliveOptions>,
    ) -> Result<LeaseKeepAliveResponse> {
        self.lease.keep_alive(id, options).await
    }

    /// `time_to_live` retrieves lease information.
    pub async fn time_to_live(
        &mut self,
        id: i64,
        options: Option<LeaseTimeToLiveOptions>,
    ) -> Result<LeaseTimeToLiveResponse> {
        self.lease.time_to_live(id, options).await
    }

    /// `leases` lists all existing leases.
    pub async fn leases(&mut self) -> Result<LeaseLeasesResponse> {
        self.lease.leases().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Compare, CompareOp, EventType, TxnOp, TxnOpResponse};

    /// Get client for testing.
    async fn get_client() -> Result<Client> {
        Client::connect(["localhost:2379"]).await
    }

    #[tokio::test]
    async fn test_put() -> Result<()> {
        let mut client = get_client().await?;
        client.put("put", "123", None).await?;

        // overwrite with prev key
        {
            let resp = client
                .put("put", "456", Some(PutOptions::new().with_prev_key()))
                .await?;
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
                .await?;
            let prev_key = resp.prev_key();
            assert!(prev_key.is_some());
            let prev_key = prev_key.unwrap();
            assert_eq!(prev_key.key(), b"put");
            assert_eq!(prev_key.value(), b"456");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get() -> Result<()> {
        let mut client = get_client().await?;
        client.put("get10", "10", None).await?;
        client.put("get11", "11", None).await?;
        client.put("get20", "20", None).await?;
        client.put("get21", "21", None).await?;

        // get key
        {
            let resp = client.get("get11", None).await?;
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
                .await?;
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
                .await?;
            assert_eq!(resp.count(), 2);
            assert_eq!(resp.more(), false);
            assert_eq!(resp.kvs().len(), 2);
            assert_eq!(resp.kvs()[0].key(), b"get10");
            assert_eq!(resp.kvs()[0].value(), b"10");
            assert_eq!(resp.kvs()[1].key(), b"get11");
            assert_eq!(resp.kvs()[1].value(), b"11");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<()> {
        let mut client = get_client().await?;
        client.put("del10", "10", None).await?;
        client.put("del11", "11", None).await?;
        client.put("del20", "20", None).await?;
        client.put("del21", "21", None).await?;

        // delete key
        {
            let resp = client.delete("del11", None).await?;
            assert_eq!(resp.deleted(), 1);
            let resp = client
                .get("del11", Some(GetOptions::new().with_count_only()))
                .await?;
            assert_eq!(resp.count(), 0);
        }

        // delete a range of keys
        {
            let resp = client
                .delete("del11", Some(DeleteOptions::new().with_range("del22")))
                .await?;
            assert_eq!(resp.deleted(), 2);
            let resp = client
                .get(
                    "del11",
                    Some(GetOptions::new().with_range("del22").with_count_only()),
                )
                .await?;
            assert_eq!(resp.count(), 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_compact() -> Result<()> {
        let mut client = get_client().await?;
        let rev0 = client
            .put("compact", "0", None)
            .await?
            .header()
            .unwrap()
            .revision();
        let rev1 = client
            .put("compact", "1", None)
            .await?
            .header()
            .unwrap()
            .revision();

        // before compacting
        let rev0_resp = client
            .get("compact", Some(GetOptions::new().with_revision(rev0)))
            .await?;
        assert_eq!(rev0_resp.kvs()[0].value(), b"0");
        let rev1_resp = client
            .get("compact", Some(GetOptions::new().with_revision(rev1)))
            .await?;
        assert_eq!(rev1_resp.kvs()[0].value(), b"1");

        client.compact(rev1, None).await?;

        // after compacting
        let result = client
            .get("compact", Some(GetOptions::new().with_revision(rev0)))
            .await;
        assert!(result.is_err());
        let rev1_resp = client
            .get("compact", Some(GetOptions::new().with_revision(rev1)))
            .await?;
        assert_eq!(rev1_resp.kvs()[0].value(), b"1");

        Ok(())
    }

    #[tokio::test]
    async fn test_txn() -> Result<()> {
        let mut client = get_client().await?;
        client.put("txn01", "01", None).await?;

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
                .await?;

            assert!(resp.succeeded());
            let op_responses = resp.op_responses();
            assert_eq!(op_responses.len(), 1);

            match op_responses[0] {
                TxnOpResponse::Put(ref resp) => assert_eq!(resp.prev_key().unwrap().value(), b"01"),
                _ => panic!("unexpected response"),
            }

            let resp = client.get("txn01", None).await?;
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
                .await?;

            assert!(!resp.succeeded());
            let op_responses = resp.op_responses();
            assert_eq!(op_responses.len(), 1);

            match op_responses[0] {
                TxnOpResponse::Get(ref resp) => assert_eq!(resp.kvs()[0].value(), b"02"),
                _ => panic!("unexpected response"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_watch() -> Result<()> {
        let mut client = get_client().await?;

        let (mut watcher, mut stream) = client.watch("watch01", None).await?;

        client.put("watch01", "01", None).await?;

        let resp = stream.message().await?.unwrap();
        assert_eq!(resp.watch_id(), watcher.watch_id());
        assert_eq!(resp.events().len(), 1);

        let kv = resp.events()[0].kv().unwrap();
        assert_eq!(kv.key(), b"watch01");
        assert_eq!(kv.value(), b"01");
        assert_eq!(resp.events()[0].event_type(), EventType::Put);

        watcher.cancel().await?;

        let resp = stream.message().await?.unwrap();
        assert_eq!(resp.watch_id(), watcher.watch_id());
        assert!(resp.canceled());

        Ok(())
    }

    #[tokio::test]
    async fn test_grant_revoke() -> Result<()> {
        let mut client = get_client().await?;
        let resp = client.grant(123, None).await?;
        assert_eq!(resp.ttl(), 123);
        let id = resp.id();
        client.revoke(id, None).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_keep_alive() -> Result<()> {
        let mut client = get_client().await?;

        let resp = client.grant(60, None).await?;
        assert_eq!(resp.ttl(), 60);
        let id = resp.id();

        let resp = client.keep_alive(id, None).await?;
        assert_eq!(resp.id(), id);
        assert_eq!(resp.ttl(), 60);

        client.revoke(id, None).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_time_to_live() -> Result<()> {
        let mut client = get_client().await?;
        let leaseid = 100;
        let resp = client
            .grant(60, Some(LeaseGrantOptions::new().with_id(leaseid)))
            .await?;
        assert_eq!(resp.ttl(), 60);
        assert_eq!(resp.id(), leaseid);

        let resp = client.time_to_live(leaseid, None).await?;
        assert_eq!(resp.id(), leaseid);
        assert_eq!(resp.granted_ttl(), 60);

        client.revoke(leaseid, None).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_leases() -> Result<()> {
        let mut client = get_client().await?;
        let resp = client.grant(60, None).await?;
        assert_eq!(resp.ttl(), 60);
        let lease1 = resp.id();

        let resp = client.grant(60, None).await?;
        assert_eq!(resp.ttl(), 60);
        let lease2 = resp.id();

        let resp = client.grant(60, None).await?;
        assert_eq!(resp.ttl(), 60);
        let lease3 = resp.id();

        let resp = client.leases().await?;
        let lease_status = resp.take_leases();
        assert_eq!(lease_status[0].id, lease1);
        assert_eq!(lease_status[1].id, lease2);
        assert_eq!(lease_status[2].id, lease3);

        client.revoke(lease1, None).await?;
        client.revoke(lease2, None).await?;
        client.revoke(lease3, None).await?;
        Ok(())
    }
}
