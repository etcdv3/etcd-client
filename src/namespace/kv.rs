use crate::error::Result;
use crate::namespace::prefix_internal;
pub use crate::rpc::pb::etcdserverpb::{
    Compare as PbCompare, DeleteRangeResponse as PbDeleteResponse, PutResponse as PbPutResponse,
    RangeResponse as PbRangeResponse,
};
use crate::{
    take_mut, DeleteOptions, DeleteResponse, GetOptions, GetResponse, KvClient, PutOptions,
    PutResponse,
};

pub struct KvClientPrefix {
    pfx: Vec<u8>,
    kv: KvClient,
}

impl KvClientPrefix {
    pub fn new(kv: KvClient, pfx: Vec<u8>) -> Self {
        Self { pfx, kv }
    }

    pub async fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        options: Option<PutOptions>,
    ) -> Result<PutResponse> {
        let (key, _) = prefix_internal(&self.pfx, key.into(), vec![]);
        let mut resp = self.kv.put(key, value, options).await?;
        resp.take_mut_inner(|resp| self.strip_prefix_put_response(resp));
        Ok(resp)
    }

    pub async fn get(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<GetOptions>,
    ) -> Result<GetResponse> {
        let (key, _) = prefix_internal(&self.pfx, key.into(), vec![]);
        let mut resp = self.kv.get(key, options).await?;
        resp.take_mut_inner(|resp| self.strip_prefix_range_response(resp));
        Ok(resp)
    }

    pub async fn delete(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResponse> {
        let (key, _) = prefix_internal(&self.pfx, key.into(), vec![]);
        // TODO (@tisonkun) prefix range_end in options
        let mut resp = self.kv.delete(key, options).await?;
        resp.take_mut_inner(|resp| self.strip_prefix_delete_response(resp));
        Ok(resp)
    }

    fn strip_prefix_put_response(&self, mut resp: PbPutResponse) -> PbPutResponse {
        resp.prev_kv = resp.prev_kv.take().map(|mut kv| {
            kv.key = self.strip_prefix_key(kv.key);
            kv
        });
        resp
    }

    fn strip_prefix_range_response(&self, mut resp: PbRangeResponse) -> PbRangeResponse {
        for kv in resp.kvs.iter_mut() {
            take_mut(kv, |mut kv| {
                kv.key = self.strip_prefix_key(kv.key);
                kv
            });
        }
        resp
    }

    fn strip_prefix_delete_response(&self, mut resp: PbDeleteResponse) -> PbDeleteResponse {
        for kv in resp.prev_kvs.iter_mut() {
            take_mut(kv, |mut kv| {
                kv.key = self.strip_prefix_key(kv.key);
                kv
            });
        }
        resp
    }

    fn strip_prefix_key(&self, mut key: Vec<u8>) -> Vec<u8> {
        debug_assert!(
            key.starts_with(&self.pfx),
            "{key:?} does not start with {:?}",
            self.pfx
        );
        key.split_off(self.pfx.len())
    }
}
