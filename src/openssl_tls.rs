#![cfg(feature = "tls-openssl")]

use std::task::Poll;

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

#[repr(transparent)]
pub struct FairLoadedChannel(Channel);

impl Load for FairLoadedChannel {
    type Metric = usize;

    fn load(&self) -> Self::Metric {
        0
    }
}

impl Service<http::Request<BoxBody>> for FairLoadedChannel {
    type Response = <Channel as Service<http::Request<BoxBody>>>::Response;
    type Error = <Channel as Service<http::Request<BoxBody>>>::Error;
    type Future = <Channel as Service<http::Request<BoxBody>>>::Future;

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

pub type OpenSslChannel = Buffer<Balance<OpenSslDiscover<Uri>, Request<BoxBody>>, Request<BoxBody>>;

/// Create a balanced channel using the OpenSSL config.
/// Note that the tls configuration would be ignored.
pub fn balanced_channel(
    options: OpenSslClientConfig,
) -> (OpenSslChannel, Sender<Change<Uri, Endpoint>>) {
    let make_conn = make_connector(options);
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let tls_conn = with_ssl(make_conn, rx);
    let balance = Balance::new(tls_conn);
    // Note: the buffer should already be configured when creating the internal channels,
    // we wrap this in the buffer is just for making them `Clone`.
    let buffered = Buffer::new(balance, 1024);

    (buffered, tx)
}

pub type OpenSslDiscover<K> = ReceiverStream<Result<Change<K, FairLoadedChannel>>>;

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

pub fn make_connector(opts: OpenSslClientConfig) -> impl Fn() -> Result<SslConnectorBuilder> {
    move || {
        let mut cb = SslConnector::builder(SslMethod::tls())?;
        if let Some(ref ca) = opts.ca_cert {
            let ca = X509::from_pem(ca)?;
            cb.cert_store_mut().add_cert(ca)?;
        }
        if let Some(ref client_id) = opts.client_cert {
            let client = X509::from_pem(&client_id.cert)?;
            let client_key = PKey::private_key_from_pem(&client_id.key.0)?;
            cb.set_certificate(&client)?;
            cb.set_private_key(&client_key)?;
        }
        // Hint for HTTP/2.
        cb.set_alpn_protos(b"\x02h2")?;
        Ok(cb)
    }
}

#[derive(Clone)]
struct Secret(Box<[u8]>);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Secret").finish()
    }
}

#[derive(Default, Clone, Debug)]
pub struct OpenSslClientConfig {
    ca_cert: Option<Box<[u8]>>,
    client_cert: Option<ClientIdentity>,
}

impl OpenSslClientConfig {
    pub fn ca_cert_pem(mut self, s: &[u8]) -> Self {
        self.ca_cert = Some(s.to_vec().into_boxed_slice());
        self
    }

    pub fn client_cert_pem_and_key(mut self, cert_pem: &[u8], key_pem: &[u8]) -> Self {
        self.client_cert = Some(ClientIdentity {
            cert: cert_pem.to_vec().into_boxed_slice(),
            key: Secret(key_pem.to_vec().into_boxed_slice()),
        });
        self
    }
}

#[derive(Clone, Debug)]
struct ClientIdentity {
    cert: Box<[u8]>,
    key: Secret,
}
