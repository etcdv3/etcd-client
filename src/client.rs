//! Asynchronous client & synchronous client.

use crate::error::{Error, Result};
use crate::rpc::kv::{KvClient, PutOptions, PutResponse};
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

        let kv = KvClient::new(channel.clone(), None);

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put() {
        let mut client = Client::connect(["localhost:2379"]).unwrap();
        client.put("abc", "123").unwrap();

        // overwrite with prev key
        {
            let resp = client
                .put_with_options("abc", "456", PutOptions::new().with_prev_key(true))
                .unwrap();
            let prev_key = resp.prev_key();
            assert!(prev_key.is_some());
            let prev_key = prev_key.unwrap();
            assert_eq!(prev_key.key(), b"abc");
            assert_eq!(prev_key.value(), b"123");
        }

        // overwrite again with prev key
        {
            let resp = client
                .put_with_options("abc", "789", PutOptions::new().with_prev_key(true))
                .unwrap();
            let prev_key = resp.prev_key();
            assert!(prev_key.is_some());
            let prev_key = prev_key.unwrap();
            assert_eq!(prev_key.key(), b"abc");
            assert_eq!(prev_key.value(), b"456");
        }
    }
}
