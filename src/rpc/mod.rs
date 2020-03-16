//! Etcd RPC interfaces.

mod pb;

pub mod auth;
pub mod cluster;
pub mod election;
pub mod kv;
pub mod lease;
pub mod lock;
pub mod maintenance;
pub mod watch;

use pb::etcdserverpb::ResponseHeader as PbResponseHeader;
use pb::mvccpb::KeyValue as PbKeyValue;

/// General `etcd` response header.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct ResponseHeader(PbResponseHeader);

impl ResponseHeader {
    /// Create a response header from pb header.
    #[inline]
    pub(crate) const fn new(header: PbResponseHeader) -> Self {
        Self(header)
    }

    /// The ID of the cluster which sent the response.
    #[inline]
    pub const fn cluster_id(&self) -> u64 {
        self.0.cluster_id
    }

    /// The ID of the member which sent the response.
    #[inline]
    pub const fn member_id(&self) -> u64 {
        self.0.member_id
    }

    /// The key-value store revision when the request was applied.
    /// For watch progress responses, the header.revision() indicates progress. All future events
    /// received in this stream are guaranteed to have a higher revision number than the
    /// header.revision() number.
    #[inline]
    pub const fn revision(&self) -> i64 {
        self.0.revision
    }

    /// The raft term when the request was applied.
    #[inline]
    pub const fn raft_term(&self) -> u64 {
        self.0.raft_term
    }
}

impl From<&PbResponseHeader> for &ResponseHeader {
    #[inline]
    fn from(src: &PbResponseHeader) -> Self {
        unsafe { &*(src as *const _ as *const ResponseHeader) }
    }
}

/// Key-value pair.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct KeyValue(PbKeyValue);

impl KeyValue {
    /// Create a KeyValue from pb kv.
    #[inline]
    pub(crate) const fn new(kv: PbKeyValue) -> Self {
        Self(kv)
    }

    /// The key in bytes. An empty key is not allowed.
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.0.key
    }

    /// The value held by the key, in bytes.
    #[inline]
    pub fn value(&self) -> &[u8] {
        &self.0.value
    }

    /// The revision of last creation on this key.
    #[inline]
    pub const fn create_revision(&self) -> i64 {
        self.0.create_revision
    }

    /// The revision of last modification on this key.
    #[inline]
    pub const fn mod_revision(&self) -> i64 {
        self.0.mod_revision
    }

    /// The version of the key. A deletion resets
    /// the version to zero and any modification of the key
    /// increases its version.
    #[inline]
    pub const fn version(&self) -> i64 {
        self.0.version
    }

    /// The ID of the lease that attached to key.
    /// When the attached lease expires, the key will be deleted.
    /// If lease is 0, then no lease is attached to the key.
    #[inline]
    pub const fn lease(&self) -> i64 {
        self.0.lease
    }
}

impl From<&PbKeyValue> for &KeyValue {
    #[inline]
    fn from(src: &PbKeyValue) -> Self {
        unsafe { &*(src as *const _ as *const KeyValue) }
    }
}

/// Get prefix end key of `key`.
#[inline]
fn get_prefix(key: &[u8]) -> Vec<u8> {
    for (i, v) in key.iter().enumerate().rev() {
        if *v < 0xFF {
            let mut end = Vec::from(&key[..=i]);
            end[i] = *v + 1;
            return end;
        }
    }

    // next prefix does not exist (e.g., 0xffff);
    vec![0]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_prefix() {
        assert_eq!(get_prefix(b"foo1").as_slice(), b"foo2");
        assert_eq!(get_prefix(b"\xFF").as_slice(), b"\0");
        assert_eq!(get_prefix(b"foo\xFF").as_slice(), b"fop");
    }
}
