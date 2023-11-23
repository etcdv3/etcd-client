use crate::error::Result;
use crate::namespace::prefix_internal;
use crate::rpc::pb::etcdserverpb::request_op::Request;
use crate::rpc::pb::etcdserverpb::response_op::Response;
pub use crate::rpc::pb::etcdserverpb::{
    Compare as PbCompare, DeleteRangeResponse as PbDeleteResponse, PutResponse as PbPutResponse,
    RangeResponse as PbRangeResponse, TxnRequest as PbTxnRequest, TxnResponse as PbTxnResponse,
};
use crate::rpc::pb::etcdserverpb::{RequestOp, ResponseOp};
use crate::{
    take_mut, DeleteOptions, DeleteResponse, GetOptions, GetResponse, KvClient, PutOptions,
    PutResponse, Txn, TxnResponse,
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

    pub async fn txn(&mut self, mut txn: Txn) -> Result<TxnResponse> {
        txn.take_mut_inner(|req| self.prefix_txn_request(req));
        let mut resp = self.kv.txn(txn).await?;
        resp.take_mut_inner(|resp| self.strip_prefix_txn_response(resp));
        Ok(resp)
    }

    fn prefix_txn_request(&self, mut req: PbTxnRequest) -> PbTxnRequest {
        let prefix_cmp = |mut cmp: PbCompare| {
            (cmp.key, cmp.range_end) = prefix_internal(&self.pfx, cmp.key, cmp.range_end);
            cmp
        };
        let prefix_op = |mut op: RequestOp| {
            op.request = op.request.map(|req| match req {
                Request::RequestRange(mut req) => {
                    (req.key, req.range_end) = prefix_internal(&self.pfx, req.key, req.range_end);
                    Request::RequestRange(req)
                }
                Request::RequestPut(mut req) => {
                    (req.key, _) = prefix_internal(&self.pfx, req.key, vec![]);
                    Request::RequestPut(req)
                }
                Request::RequestDeleteRange(mut req) => {
                    (req.key, req.range_end) = prefix_internal(&self.pfx, req.key, req.range_end);
                    Request::RequestDeleteRange(req)
                }
                Request::RequestTxn(req) => Request::RequestTxn(self.prefix_txn_request(req)),
            });
            op
        };
        req.compare = req.compare.drain(..).map(prefix_cmp).collect();
        req.success = req.success.drain(..).map(prefix_op).collect();
        req.failure = req.failure.drain(..).map(prefix_op).collect();
        req
    }

    fn strip_prefix_txn_response(&self, mut resp: PbTxnResponse) -> PbTxnResponse {
        let strip_prefix_op = |mut op: ResponseOp| {
            op.response = op.response.map(|res| match res {
                Response::ResponseRange(r) => {
                    Response::ResponseRange(self.strip_prefix_range_response(r))
                }
                Response::ResponsePut(r) => {
                    Response::ResponsePut(self.strip_prefix_put_response(r))
                }
                Response::ResponseDeleteRange(r) => {
                    Response::ResponseDeleteRange(self.strip_prefix_delete_response(r))
                }
                Response::ResponseTxn(r) => {
                    Response::ResponseTxn(self.strip_prefix_txn_response(r))
                }
            });
            op
        };
        resp.responses = resp.responses.drain(..).map(strip_prefix_op).collect();
        resp
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
