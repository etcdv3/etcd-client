#![cfg(feature = "tls-openssl")]

use std::{sync::Arc, task::Poll};

use http::{Request, Uri};
use hyper::client::HttpConnector;
use hyper_openssl::HttpsConnector;
use openssl::{
    pkey::PKey,
    ssl::{SslConnector, SslConnectorBuilder, SslMethod},
    x509::X509,
};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    body::BoxBody,
    transport::{Channel, Endpoint},
};
use tower::{balance::p2c::Balance, buffer::Buffer, discover::Change, load::Load, Service};

use super::error::Result;

pub type TonicRequest = Request<BoxBody>;
pub type Buffered<T> = Buffer<T, TonicRequest>;
pub type Balanced<T> = Balance<T, TonicRequest>;
pub type OpenSslChannel = Buffered<Balanced<OpenSslDiscover<Uri>>>;
pub type OpenSslDiscover<K> = ReceiverStream<Result<Change<K, FairLoadedChannel>>>;
/// Allow the user manually apply some config.
/// It should be a shared pure function to keep the config `Clone` and pure.
pub type ManualConfig = Arc<dyn Fn(&mut SslConnectorBuilder) -> Result<()> + Send + Sync + 'static>;

#[repr(transparent)]
pub struct FairLoadedChannel(Channel);

impl Load for FairLoadedChannel {
    type Metric = usize;

    fn load(&self) -> Self::Metric {
        0
    }
}

impl Service<TonicRequest> for FairLoadedChannel {
    type Response = <Channel as Service<TonicRequest>>::Response;
    type Error = <Channel as Service<TonicRequest>>::Error;
    type Future = <Channel as Service<TonicRequest>>::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        self.0.call(req)
    }
}

/// Create a balanced channel using the OpenSSL config.
/// Note that the tls configuration would be ignored.
pub fn balanced_channel(
    options: OpenSslClientConfig,
) -> (OpenSslChannel, Sender<Change<Uri, Endpoint>>) {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let make_config = Arc::clone(&options.0);
    let tls_conn = with_ssl(
        move || {
            let mut b = SslConnector::builder(SslMethod::tls_client())?;
            make_config(&mut b)?;
            Ok(b)
        },
        rx,
    );
    let balance = Balance::new(tls_conn);
    // Note: the buffer should already be configured when creating the internal channels,
    // we wrap this in the buffer is just for making them `Clone`.
    let buffered = Buffer::new(balance, 1024);

    (buffered, tx)
}

fn with_ssl<K: Send + 'static>(
    ssl: impl Fn() -> Result<SslConnectorBuilder> + Send + Sync + 'static,
    mut incoming: Receiver<Change<K, Endpoint>>,
) -> OpenSslDiscover<K> {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let fut = async move {
        while let Some(x) = incoming.recv().await {
            let r = async {
                match x {
                    Change::Insert(name, e) => {
                        let mut http = HttpConnector::new();
                        http.enforce_http(false);
                        let https = HttpsConnector::with_connector(http, ssl()?)?;
                        let channel = e.connect_with_connector(https).await?;
                        Ok(Change::Insert(name, FairLoadedChannel(channel)))
                    }
                    Change::Remove(name) => Ok(Change::Remove(name)),
                }
            }
            .await;
            if tx.send(r).await.is_err() {
                return;
            }
        }
    };
    tokio::task::spawn(fut);
    ReceiverStream::new(rx)
}

#[derive(Clone)]
struct Secret(Box<[u8]>);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Secret").finish()
    }
}

#[derive(Clone)]
pub struct OpenSslClientConfig(ManualConfig);

impl Default for OpenSslClientConfig {
    fn default() -> Self {
        Self(Arc::new(|_| Ok(())))
    }
}

impl std::fmt::Debug for OpenSslClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OpenSslClientConfig")
            .field(&"callbacks")
            .finish()
    }
}

impl OpenSslClientConfig {
    pub fn manually(
        mut self,
        f: impl Fn(&mut SslConnectorBuilder) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        let inner = self.0;
        self.0 = Arc::new(move |cb| {
            inner(cb)?;
            f(cb)
        });
        self
    }

    pub fn ca_cert_pem(self, s: &[u8]) -> Self {
        let s = s.to_vec();
        self.manually(move |cb| {
            let ca = X509::from_pem(&s)?;
            cb.cert_store_mut().add_cert(ca)?;
            Ok(())
        })
    }

    pub fn client_cert_pem_and_key(self, cert_pem: &[u8], key_pem: &[u8]) -> Self {
        let cert_pem = cert_pem.to_vec();
        let key_pem = key_pem.to_vec();
        self.manually(move |cb| {
            let client = X509::from_pem(&cert_pem)?;
            let client_key = PKey::private_key_from_pem(&key_pem)?;
            cb.set_certificate(&client)?;
            cb.set_private_key(&client_key)?;
            Ok(())
        })
    }
}
