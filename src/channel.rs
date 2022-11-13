#[cfg(not(feature = "tls-openssl"))]
pub use crate::channel::Channel;

#[cfg(feature = "tls-openssl")]
pub use self::openssl_chan::Channel;

#[cfg(feature = "tls-openssl")]
mod openssl_chan {
    use crate::openssl_tls;

    pub type Channel = openssl_tls::OpenSslChannel;
}
