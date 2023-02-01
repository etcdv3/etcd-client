#![cfg(feature = "tls-openssl")]

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::ready,
    time::{Duration, Instant},
};

use http::{Request, Uri};
use hyper::client::HttpConnector;
use hyper_openssl::HttpsConnector;
use openssl::{
    error::ErrorStack,
    pkey::PKey,
    ssl::{SslConnector, SslMethod},
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

pub type SslConnectorBuilder = openssl::ssl::SslConnectorBuilder;
pub type OpenSslResult<T> = std::result::Result<T, ErrorStack>;
// Below are some type alias for make clearer types.
pub type TonicRequest = Request<BoxBody>;
pub type Buffered<T> = Buffer<T, TonicRequest>;
pub type Balanced<T> = Balance<T, TonicRequest>;
pub type OpenSslChannel = Buffered<Balanced<OpenSslDiscover<Uri>>>;
/// OpenSslDiscover is the backend for balanced channel based on OpenSSL transports.
/// Because `Channel::balance` doesn't allow us to provide custom connector, we must implement ourselves' balancer...
pub type OpenSslDiscover<K> = ReceiverStream<Result<Change<K, FailBackOff<Channel>>>>;

pub struct FailBackOff<S> {
    inner: S,
    handle: BackOffHandle,
}

impl<S> FailBackOff<S> {
    fn new(inner: S, back_off: Duration) -> Self {
        Self {
            inner,
            handle: BackOffHandle {
                backoff_time: back_off,
                last_failure: Arc::default(),
            },
        }
    }
}

#[derive(Clone)]
struct BackOffHandle {
    backoff_time: Duration,
    last_failure: Arc<Mutex<Option<Instant>>>,
}

impl BackOffHandle {
    fn fail(&self) {
        let mut failure = Mutex::lock(&self.last_failure).unwrap();
        *failure = Some(Instant::now());
    }

    fn failed(&self) -> bool {
        let mut failure = self.last_failure.lock().unwrap();
        if let Some(lf) = failure.as_mut() {
            if Instant::now().saturating_duration_since(*lf) > self.backoff_time {
                *failure = None;
            }
        }

        failure.is_some()
    }
}

pub struct TraceFailFuture<F> {
    inner: F,
    handle: BackOffHandle,
}

impl<F, T, E> Future for TraceFailFuture<F>
where
    F: Future<Output = std::result::Result<T, E>>,
{
    type Output = <F as Future>::Output;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // Safety: trivial projection.
        let (inner, handle) = unsafe {
            let this = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut this.inner), &this.handle)
        };

        let result = ready!(Future::poll(inner, cx));
        if result.is_err() {
            handle.fail();
        }
        result.into()
    }
}

impl<Req, S> Service<Req> for FailBackOff<S>
where
    S: Service<Req>,
{
    type Response = <S as Service<Req>>::Response;
    type Error = <S as Service<Req>>::Error;
    type Future = TraceFailFuture<<S as Service<Req>>::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        TraceFailFuture {
            inner: self.inner.call(req),
            handle: self.handle.clone(),
        }
    }
}

impl<S> Load for FailBackOff<S> {
    type Metric = bool;

    fn load(&self) -> Self::Metric {
        self.handle.failed()
    }
}

#[derive(Clone)]
pub struct OpenSslConnector(HttpsConnector<HttpConnector>);

impl std::fmt::Debug for OpenSslConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OpenSslConnector").finish()
    }
}

impl OpenSslConnector {
    pub fn create_default() -> OpenSslResult<Self> {
        let conf = OpenSslClientConfig::default();
        conf.build()
    }
}

#[cfg(feature = "tls")]
compile_error!(concat!(
    "**You should only enable one of `tls` and `tls-openssl`.** Reason: ",
    "For now, `tls-openssl` would take over the transport layer (sockets) to implement TLS based connection. ",
    "As a result, once using with `tonic`'s internal TLS implementation (which based on `rustls`), ", 
    "we may create TLS tunnels over TLS tunnels or directly fail because of some sorts of misconfiguration.")
);

/// Create a balanced channel using the OpenSSL config.
pub fn balanced_channel(
    connector: OpenSslConnector,
) -> Result<(OpenSslChannel, Sender<Change<Uri, Endpoint>>)> {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let tls_conn = create_openssl_discover(connector, rx);
    let balance = Balance::new(tls_conn);
    // Note: the buffer should already be configured when creating the internal channels,
    // we wrap this in the buffer is just for making them `Clone`.
    let buffered = Buffer::new(balance, 1024);

    Ok((buffered, tx))
}

/// Create a connector which dials TLS connections by openssl.
fn create_openssl_connector(builder: SslConnectorBuilder) -> OpenSslResult<OpenSslConnector> {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    let https = HttpsConnector::with_connector(http, builder)?;
    Ok(OpenSslConnector(https))
}

/// Create a discover which mapping Endpoints into SSL connections.
/// Because this would fully take over the transport layer by a security channel,
/// you should NOT enable `tonic/ssl` feature (or tonic may try to create SSL session over the security transport...).
fn create_openssl_discover<K: Send + 'static>(
    connector: OpenSslConnector,
    mut incoming: Receiver<Change<K, Endpoint>>,
) -> OpenSslDiscover<K> {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let fut = async move {
        while let Some(x) = incoming.recv().await {
            let r = async {
                match x {
                    Change::Insert(name, e) => {
                        let chan = e.connect_with_connector(connector.clone().0).await?;
                        Ok(Change::Insert(
                            name,
                            FailBackOff::new(chan, Duration::from_secs(30)),
                        ))
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

/// The configuration type for a openssl connection.
/// For best flexibility, we are making it a callback over `SslConnectorBuilder`.
/// Which allows users to fine-tweaking the detail of the SSL connection.
/// This isn't `Clone` due to the implementation.
pub struct OpenSslClientConfig(OpenSslResult<SslConnectorBuilder>);

impl Default for OpenSslClientConfig {
    fn default() -> Self {
        let get_builder = || {
            let mut b = SslConnector::builder(SslMethod::tls_client())?;
            // It seems gRPC doesn't support upgrade to HTTP/2,
            // if we haven't specified the protocol by ALPN, it would return a `GONE`.
            // "h2" is the ALPN name for HTTP/2, see:
            // https://www.iana.org/assignments/tls-extensiontype-values/tls-extensiontype-values.xhtml#alpn-protocol-ids
            b.set_alpn_protos(b"\x02h2")?;
            OpenSslResult::Ok(b)
        };
        Self(get_builder())
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
    /// Manually modify the SslConnectorBuilder by a pure function.
    pub fn manually(self, f: impl FnOnce(&mut SslConnectorBuilder) -> OpenSslResult<()>) -> Self {
        Self(self.0.and_then(|mut b| {
            f(&mut b)?;
            Ok(b)
        }))
    }

    /// Add a CA into the cert storage via the binary of the PEM file of CA cert.
    /// If the argument is empty, do nothing.
    pub fn ca_cert_pem(self, s: &[u8]) -> Self {
        if s.is_empty() {
            return self;
        }
        self.manually(move |cb| {
            let ca = X509::from_pem(&s)?;
            cb.cert_store_mut().add_cert(ca)?;
            Ok(())
        })
    }

    /// Add a client cert for the request.
    /// If any of the argument is empty, do nothing.
    pub fn client_cert_pem_and_key(self, cert_pem: &[u8], key_pem: &[u8]) -> Self {
        if cert_pem.is_empty() || key_pem.is_empty() {
            return self;
        }
        self.manually(|cb| {
            let client = X509::from_pem(&cert_pem)?;
            let client_key = PKey::private_key_from_pem(&key_pem)?;
            cb.set_certificate(&client)?;
            cb.set_private_key(&client_key)?;
            Ok(())
        })
    }

    pub(crate) fn build(self) -> OpenSslResult<OpenSslConnector> {
        self.0.and_then(|x| create_openssl_connector(x))
    }
}
