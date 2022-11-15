#[cfg(not(feature = "tls-openssl"))]
pub use tonic::transport::Channel;

#[cfg(feature = "tls-openssl")]
pub use self::openssl::Channel;

#[cfg(feature = "tls-openssl")]
mod openssl {
    use crate::openssl_tls;

    /// Because we cannot create `Channel` by the balanced, cached channels,
    /// we cannot create clients (which explicitly requires `Channel` as argument) directly.
    ///
    /// This type alias would be useful to 'batch replace' the signature of `Client::new`.
    pub type Channel = openssl_tls::OpenSslChannel;
}
