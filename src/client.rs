//! Asynchronous client & synchronous client.

use crate::error::{Error, Result};
use crate::rpc::auth::Permission;
use crate::rpc::auth::{AuthClient, AuthDisableResponse, AuthEnableResponse};
use crate::rpc::auth::{
    RoleAddResponse, RoleDeleteResponse, RoleGetResponse, RoleGrantPermissionResponse,
    RoleListResponse, RoleRevokePermissionOptions, RoleRevokePermissionResponse,
};
use crate::rpc::kv::{
    CompactionOptions, CompactionResponse, DeleteOptions, DeleteResponse, GetOptions, GetResponse,
    KvClient, PutOptions, PutResponse, Txn, TxnResponse,
};
use crate::rpc::lease::{
    LeaseClient, LeaseGrantOptions, LeaseGrantResponse, LeaseKeepAliveStream, LeaseKeeper,
    LeaseLeasesResponse, LeaseRevokeResponse, LeaseTimeToLiveOptions, LeaseTimeToLiveResponse,
};
use crate::rpc::lock::{LockClient, LockOptions, LockResponse, UnlockResponse};
use crate::rpc::watch::{WatchClient, WatchOptions, WatchStream, Watcher};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Channel;
use tonic::Interceptor;

/// Asynchronous `etcd` client using v3 API.
pub struct Client {
    kv: KvClient,
    watch: WatchClient,
    lease: LeaseClient,
    lock: LockClient,
    auth: AuthClient,
}

impl Client {
    /// Connect to `etcd` servers from given `endpoints`.
    pub async fn connect<E: AsRef<str>, S: AsRef<[E]>>(
        endpoints: S,
        options: Option<ConnectOptions>,
    ) -> Result<Self> {
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

        let interceptor = if let Some(connect_options) = options {
            if let Some((name, password)) = connect_options.user {
                let mut tmp_auth = AuthClient::new(channel.clone(), None);
                let resp = tmp_auth.authenticate(name, password).await?;
                let token: MetadataValue<Ascii> = resp.token().parse()?;

                Some(Interceptor::new(move |mut request| {
                    let metadata = request.metadata_mut();
                    // authorization for http::header::AUTHORIZATION
                    metadata.insert("authorization", token.clone());
                    Ok(request)
                }))
            } else {
                None
            }
        } else {
            None
        };

        let kv = KvClient::new(channel.clone(), interceptor.clone());
        let watch = WatchClient::new(channel.clone(), interceptor.clone());
        let lease = LeaseClient::new(channel.clone(), interceptor.clone());
        let lock = LockClient::new(channel.clone(), interceptor.clone());
        let auth = AuthClient::new(channel, interceptor);

        Ok(Self {
            kv,
            watch,
            lease,
            lock,
            auth,
        })
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

    /// Creates a lease which expires if the server does not receive a keepAlive
    /// within a given time to live period. All keys attached to the lease will be expired and
    /// deleted if the lease expires. Each expired key generates a delete event in the event history.
    #[inline]
    pub async fn lease_grant(
        &mut self,
        ttl: i64,
        options: Option<LeaseGrantOptions>,
    ) -> Result<LeaseGrantResponse> {
        self.lease.grant(ttl, options).await
    }

    /// Revokes a lease. All keys attached to the lease will expire and be deleted.
    #[inline]
    pub async fn lease_revoke(&mut self, id: i64) -> Result<LeaseRevokeResponse> {
        self.lease.revoke(id).await
    }

    /// Keeps the lease alive by streaming keep alive requests from the client
    /// to the server and streaming keep alive responses from the server to the client.
    #[inline]
    pub async fn lease_keep_alive(
        &mut self,
        id: i64,
    ) -> Result<(LeaseKeeper, LeaseKeepAliveStream)> {
        self.lease.keep_alive(id).await
    }

    /// Retrieves lease information.
    #[inline]
    pub async fn lease_time_to_live(
        &mut self,
        id: i64,
        options: Option<LeaseTimeToLiveOptions>,
    ) -> Result<LeaseTimeToLiveResponse> {
        self.lease.time_to_live(id, options).await
    }

    /// Lists all existing leases.
    #[inline]
    pub async fn leases(&mut self) -> Result<LeaseLeasesResponse> {
        self.lease.leases().await
    }

    /// Lock acquires a distributed shared lock on a given named lock.
    /// On success, it will return a unique key that exists so long as the
    /// lock is held by the caller. This key can be used in conjunction with
    /// transactions to safely ensure updates to etcd only occur while holding
    /// lock ownership. The lock is held until Unlock is called on the key or the
    /// lease associate with the owner expires.
    #[inline]
    pub async fn lock(
        &mut self,
        name: impl Into<Vec<u8>>,
        options: Option<LockOptions>,
    ) -> Result<LockResponse> {
        self.lock.lock(name, options).await
    }

    /// Unlock takes a key returned by Lock and releases the hold on lock. The
    /// next Lock caller waiting for the lock will then be woken up and given
    /// ownership of the lock.
    #[inline]
    pub async fn unlock(&mut self, key: impl Into<Vec<u8>>) -> Result<UnlockResponse> {
        self.lock.unlock(key).await
    }

    /// Enables authentication.
    #[inline]
    pub async fn auth_enable(&mut self) -> Result<AuthEnableResponse> {
        self.auth.auth_enable().await
    }

    /// Disables authentication.
    #[inline]
    pub async fn auth_disable(&mut self) -> Result<AuthDisableResponse> {
        self.auth.auth_disable().await
    }

    /// Add role.
    #[inline]
    pub async fn role_add(&mut self, name: impl Into<String>) -> Result<RoleAddResponse> {
        self.auth.role_add(name).await
    }

    /// Delete role.
    #[inline]
    pub async fn role_delete(&mut self, name: impl Into<String>) -> Result<RoleDeleteResponse> {
        self.auth.role_delete(name).await
    }

    /// Get role.
    #[inline]
    pub async fn role_get(&mut self, name: impl Into<String>) -> Result<RoleGetResponse> {
        self.auth.role_get(name).await
    }

    /// List role.
    #[inline]
    pub async fn role_list(&mut self) -> Result<RoleListResponse> {
        self.auth.role_list().await
    }

    /// Grant role permission.
    #[inline]
    pub async fn role_grant_permission(
        &mut self,
        name: impl Into<String>,
        perm: Permission,
    ) -> Result<RoleGrantPermissionResponse> {
        self.auth.role_grant_permission(name, perm).await
    }

    /// Revoke role permission.
    #[inline]
    pub async fn role_revoke_permission(
        &mut self,
        name: impl Into<String>,
        key: impl Into<Vec<u8>>,
        options: Option<RoleRevokePermissionOptions>,
    ) -> Result<RoleRevokePermissionResponse> {
        self.auth.role_revoke_permission(name, key, options).await
    }
}

/// Options for `Connect` operation.
#[derive(Debug, Default, Clone)]
pub struct ConnectOptions {
    /// user is a pair values of name and password
    user: Option<(String, String)>,
}

impl ConnectOptions {
    /// name is the identifier for the distributed shared lock to be acquired.
    #[inline]
    pub fn with_user(mut self, name: impl Into<String>, password: impl Into<String>) -> Self {
        self.user = Some((name.into(), password.into()));
        self
    }

    /// Creates a `ConnectOptions`.
    #[inline]
    pub const fn new() -> Self {
        ConnectOptions { user: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Compare, CompareOp, EventType, TxnOp, TxnOpResponse};

    /// Get client for testing.
    async fn get_client() -> Result<Client> {
        Client::connect(["localhost:2379"], None).await
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
        let resp = client.lease_grant(123, None).await?;
        assert_eq!(resp.ttl(), 123);
        let id = resp.id();
        client.lease_revoke(id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_keep_alive() -> Result<()> {
        let mut client = get_client().await?;

        let resp = client.lease_grant(60, None).await?;
        assert_eq!(resp.ttl(), 60);
        let id = resp.id();

        let (mut keeper, mut stream) = client.lease_keep_alive(id).await?;
        keeper.keep_alive().await?;

        let resp = stream.message().await?.unwrap();
        assert_eq!(resp.id(), keeper.id());
        assert_eq!(resp.ttl(), 60);

        client.lease_revoke(id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_time_to_live() -> Result<()> {
        let mut client = get_client().await?;
        let leaseid = 200;
        let resp = client
            .lease_grant(60, Some(LeaseGrantOptions::new().with_id(leaseid)))
            .await?;
        assert_eq!(resp.ttl(), 60);
        assert_eq!(resp.id(), leaseid);

        let resp = client.lease_time_to_live(leaseid, None).await?;
        assert_eq!(resp.id(), leaseid);
        assert_eq!(resp.granted_ttl(), 60);

        client.lease_revoke(leaseid).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_leases() -> Result<()> {
        let lease1 = 100;
        let lease2 = 101;
        let lease3 = 102;

        let mut client = get_client().await?;
        let resp = client
            .lease_grant(60, Some(LeaseGrantOptions::new().with_id(lease1)))
            .await?;
        assert_eq!(resp.ttl(), 60);
        assert_eq!(resp.id(), lease1);

        let resp = client
            .lease_grant(60, Some(LeaseGrantOptions::new().with_id(lease2)))
            .await?;
        assert_eq!(resp.ttl(), 60);
        assert_eq!(resp.id(), lease2);

        let resp = client
            .lease_grant(60, Some(LeaseGrantOptions::new().with_id(lease3)))
            .await?;
        assert_eq!(resp.ttl(), 60);
        assert_eq!(resp.id(), lease3);

        let resp = client.leases().await?;
        let leases: Vec<_> = resp.leases().iter().map(|status| status.id()).collect();
        assert!(leases.contains(&lease1));
        assert!(leases.contains(&lease2));
        assert!(leases.contains(&lease3));

        client.lease_revoke(lease1).await?;
        client.lease_revoke(lease2).await?;
        client.lease_revoke(lease3).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_lock() -> Result<()> {
        let mut client = get_client().await?;
        let resp = client.lock("lock-test", None).await?;
        let key = resp.key();
        let key_str = std::str::from_utf8(key)?;
        assert!(key_str.starts_with("lock-test/"));

        client.unlock(key.clone()).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_auth() -> Result<()> {
        let mut client = get_client().await?;
        client.auth_enable().await?;

        // after enable auth, must operate by authenticated client
        let resp = client.put("auth-test", "value", None).await;
        if let Ok(_) = resp {
            assert!(false);
        }

        // connect with authenticate, the user must already exists
        let options = Some(ConnectOptions::new().with_user(
            "root",    // user name
            "rootpwd", // password
        ));
        let mut client_auth = Client::connect(["localhost:2379"], options).await?;
        client_auth.put("auth-test", "value", None).await?;

        client_auth.auth_disable().await?;

        // after disable auth, operate ok
        let mut client = get_client().await?;
        client.put("auth-test", "value", None).await?;

        Ok(())
    }
}
