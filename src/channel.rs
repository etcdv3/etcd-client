use std::{future::Future, pin::Pin, task::ready};

use http::Uri;
use tokio::sync::mpsc::Sender;
use tonic::transport::Endpoint;
use tower::{discover::Change, util::BoxCloneService, Service};

/// A type alias to make the below types easier to represent.
pub type EndpointUpdater = Sender<Change<Uri, Endpoint>>;

/// Creates a balanced channel.
pub trait MakeBalancedChannel {
    type Error;

    /// Makes a new balanced channel, given the provided options.
    fn balanced_channel(
        self,
        buffer_size: usize,
    ) -> Result<(Channel, EndpointUpdater), Self::Error>;
}

/// Create a simple Tonic channel.
pub struct Tonic;

impl MakeBalancedChannel for Tonic {
    type Error = tonic::transport::Error;

    #[inline]
    fn balanced_channel(
        self,
        buffer_size: usize,
    ) -> Result<(Channel, EndpointUpdater), Self::Error> {
        let (chan, tx) = tonic::transport::Channel::balance_channel(buffer_size);
        Ok((Channel::Tonic(chan), tx))
    }
}

/// Create an Openssl-backed channel.
#[cfg(feature = "tls-openssl")]
pub struct Openssl {
    pub(crate) conn: crate::openssl_tls::OpenSslConnector,
}

#[cfg(feature = "tls-openssl")]
impl MakeBalancedChannel for Openssl {
    type Error = crate::error::Error;

    #[inline]
    fn balanced_channel(self, _: usize) -> Result<(Channel, EndpointUpdater), Self::Error> {
        let (chan, tx) = crate::openssl_tls::balanced_channel(self.conn)?;
        Ok((Channel::Openssl(chan), tx))
    }
}

type TonicRequest = http::Request<tonic::body::BoxBody>;
type TonicResponse = http::Response<tonic::body::BoxBody>;
pub type CustomChannel = BoxCloneService<TonicRequest, TonicResponse, tower::BoxError>;

/// Represents a channel that can be created by a
#[derive(Clone)]
pub enum Channel {
    /// A standard tonic channel.
    Tonic(tonic::transport::Channel),

    /// An OpenSSL channel.
    #[cfg(feature = "tls-openssl")]
    Openssl(crate::openssl_tls::OpenSslChannel),

    /// A custom Service impl, inside a Box.
    Custom(CustomChannel),
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel").finish_non_exhaustive()
    }
}

pub enum ChannelMultiFuture {
    Tonic(<tonic::transport::Channel as Service<TonicRequest>>::Future),
    #[cfg(feature = "tls-openssl")]
    Openssl(<crate::openssl_tls::OpenSslChannel as Service<TonicRequest>>::Future),
    Custom(<CustomChannel as Service<TonicRequest>>::Future),
}

impl std::future::Future for ChannelMultiFuture {
    type Output = Result<TonicResponse, tower::BoxError>;

    #[inline]
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // Safety: trivial projection
        unsafe {
            let this = self.get_unchecked_mut();
            match this {
                ChannelMultiFuture::Tonic(fut) => {
                    let fut = Pin::new_unchecked(fut);
                    let result = ready!(Future::poll(fut, cx));
                    result.map_err(|e| Box::new(e) as tower::BoxError).into()
                }
                #[cfg(feature = "tls-openssl")]
                ChannelMultiFuture::Openssl(fut) => {
                    let fut = Pin::new_unchecked(fut);
                    Future::poll(fut, cx)
                }
                ChannelMultiFuture::Custom(fut) => {
                    let fut = Pin::new_unchecked(fut);
                    Future::poll(fut, cx)
                }
            }
        }
    }
}

impl ChannelMultiFuture {
    #[inline]
    fn from_tonic(value: <tonic::transport::Channel as Service<TonicRequest>>::Future) -> Self {
        Self::Tonic(value)
    }

    #[cfg(feature = "tls-openssl")]
    #[inline]
    fn from_openssl(
        value: <crate::openssl_tls::OpenSslChannel as Service<TonicRequest>>::Future,
    ) -> Self {
        Self::Openssl(value)
    }

    #[inline]
    fn from_custom(value: <CustomChannel as Service<TonicRequest>>::Future) -> Self {
        Self::Custom(value)
    }
}

impl Service<TonicRequest> for Channel {
    type Response = TonicResponse;
    type Error = tower::BoxError;
    type Future = ChannelMultiFuture;

    #[inline]
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match self {
            Channel::Tonic(channel) => {
                let result = ready!(channel.poll_ready(cx));
                result.map_err(|e| Box::new(e) as tower::BoxError).into()
            }
            #[cfg(feature = "tls-openssl")]
            Channel::Openssl(openssl) => openssl.poll_ready(cx),
            Channel::Custom(custom) => custom.poll_ready(cx),
        }
    }

    #[inline]
    fn call(&mut self, req: TonicRequest) -> Self::Future {
        match self {
            Channel::Tonic(channel) => ChannelMultiFuture::from_tonic(channel.call(req)),
            #[cfg(feature = "tls-openssl")]
            Channel::Openssl(openssl) => ChannelMultiFuture::from_openssl(openssl.call(req)),
            Channel::Custom(custom) => ChannelMultiFuture::from_custom(custom.call(req)),
        }
    }
}
