//! Asynchronous client & synchronous client.

use crate::error::{Error, Result};
use crate::rpc::kv::{
    DeleteOptions, DeleteResponse, GetOptions, GetResponse, KvClient, PutOptions, PutResponse,
};
use std::future::Future;
use tokio::runtime::Runtime;
use tonic::transport::Channel;

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

/// Asynchronous `etcd` client using v3 API.
pub struct AsyncClient {
    kv: KvClient,
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

        let kv = KvClient::new(channel, None);

        Ok(Self { kv })
    }

    /// Put the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    #[inline]
    pub async fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<PutResponse> {
        self.kv.put(key, value, PutOptions::new()).await
    }

    /// Put the given key into the key-value store with options.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    #[inline]
    pub async fn put_with_options(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        options: PutOptions,
    ) -> Result<PutResponse> {
        self.kv.put(key, value, options).await
    }

    /// Gets the key from the key-value store.
    #[inline]
    pub async fn get(&mut self, key: impl Into<Vec<u8>>) -> Result<GetResponse> {
        self.kv.get(key, GetOptions::new()).await
    }

    /// Gets the key or a range of keys from the key-value store.
    #[inline]
    pub async fn get_with_options(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: GetOptions,
    ) -> Result<GetResponse> {
        self.kv.get(key, options).await
    }

    /// Deletes the given key from the key-value store.
    #[inline]
    pub async fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<DeleteResponse> {
        self.kv.delete(key, DeleteOptions::new()).await
    }

    /// Deletes the given key or a range of keys from the key-value store.
    #[inline]
    pub async fn delete_with_options(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: DeleteOptions,
    ) -> Result<DeleteResponse> {
        self.kv.delete(key, options).await
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
    ) -> Result<PutResponse> {
        self.async_client
            .put(key, value)
            .block_on(&mut self.runtime)
    }

    /// Put the given key into the key-value store with options.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    #[inline]
    pub fn put_with_options(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        options: PutOptions,
    ) -> Result<PutResponse> {
        self.async_client
            .put_with_options(key, value, options)
            .block_on(&mut self.runtime)
    }

    /// Gets the key from the key-value store.
    #[inline]
    pub fn get(&mut self, key: impl Into<Vec<u8>>) -> Result<GetResponse> {
        self.async_client.get(key).block_on(&mut self.runtime)
    }

    /// Gets the key or a range of keys from the key-value store.
    #[inline]
    pub fn get_with_options(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: GetOptions,
    ) -> Result<GetResponse> {
        self.async_client
            .get_with_options(key, options)
            .block_on(&mut self.runtime)
    }

    /// Deletes the given key from the key-value store.
    #[inline]
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<DeleteResponse> {
        self.async_client.delete(key).block_on(&mut self.runtime)
    }

    /// Deletes the given key or a range of keys from the key-value store.
    #[inline]
    pub fn delete_with_options(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: DeleteOptions,
    ) -> Result<DeleteResponse> {
        self.async_client
            .delete_with_options(key, options)
            .block_on(&mut self.runtime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::get_client;

    #[test]
    fn test_put() {
        let mut client = get_client();
        client.put("put", "123").unwrap();

        // overwrite with prev key
        {
            let resp = client
                .put_with_options("put", "456", PutOptions::new().with_prev_key())
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
                .put_with_options("put", "789", PutOptions::new().with_prev_key())
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
        client.put("get10", "10").unwrap();
        client.put("get11", "11").unwrap();
        client.put("get20", "20").unwrap();
        client.put("get21", "21").unwrap();

        // get key
        {
            let resp = client.get("get11".as_bytes()).unwrap();
            assert_eq!(resp.count(), 1);
            assert_eq!(resp.more(), false);
            assert_eq!(resp.kvs().len(), 1);
            assert_eq!(resp.kvs()[0].key(), b"get11");
            assert_eq!(resp.kvs()[0].value(), b"11");
        }

        // get from key
        {
            let resp = client
                .get_with_options("get11", GetOptions::new().with_from_key().with_limit(2))
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
                .get_with_options("get1", GetOptions::new().with_prefix())
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
        client.put("del10", "10").unwrap();
        client.put("del11", "11").unwrap();
        client.put("del20", "20").unwrap();
        client.put("del21", "21").unwrap();

        // delete key
        {
            let resp = client.delete("del11").unwrap();
            assert_eq!(resp.deleted(), 1);
            let resp = client
                .get_with_options("del11", GetOptions::new().with_count_only())
                .unwrap();
            assert_eq!(resp.count(), 0);
        }

        // delete a range of keys
        {
            let resp = client
                .delete_with_options("del11", DeleteOptions::new().with_range("del22"))
                .unwrap();
            assert_eq!(resp.deleted(), 2);
            let resp = client
                .get_with_options(
                    "del11",
                    GetOptions::new().with_range("del22").with_count_only(),
                )
                .unwrap();
            assert_eq!(resp.count(), 0);
        }

        // delete all keys
        {
            let _resp = client
                .delete_with_options("\0", DeleteOptions::new().with_range("\0"))
                .unwrap();
            let resp = client
                .get_with_options("\0", GetOptions::new().with_range("\0").with_count_only())
                .unwrap();
            assert_eq!(resp.count(), 0);
        }
    }
}
