#[cfg(not(feature = "tls-openssl"))]
pub use crate::channel::Channel;

#[cfg(feature = "tls-openssl")]
pub use self::openssl_chan::Channel;

#[cfg(feature = "tls-openssl")]
mod openssl_chan {
    use crate::openssl_tls;

    // FIXME: we cannot reference the impl Trait return value directly...
    pub type Channel = openssl_tls::OpenSslChannel;
}
