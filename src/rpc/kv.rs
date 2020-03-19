//! Etcd KV Operations.

pub use crate::rpc::pb::etcdserverpb::compare::CompareResult as CompareOp;
pub use crate::rpc::pb::etcdserverpb::range_request::{SortOrder, SortTarget};

use crate::error::Result;
use crate::rpc::pb::etcdserverpb::compare::{CompareTarget, TargetUnion};
use crate::rpc::pb::etcdserverpb::kv_client::KvClient as PbKvClient;
use crate::rpc::pb::etcdserverpb::request_op::Request as PbTxnOp;
use crate::rpc::pb::etcdserverpb::response_op::Response as PbTxnOpResponse;
use crate::rpc::pb::etcdserverpb::{
    CompactionRequest as PbCompactionRequest, CompactionRequest,
    CompactionResponse as PbCompactionResponse, Compare as PbCompare,
    DeleteRangeRequest as PbDeleteRequest, DeleteRangeRequest,
    DeleteRangeResponse as PbDeleteResponse, PutRequest as PbPutRequest,
    PutResponse as PbPutResponse, RangeRequest as PbRangeRequest, RangeResponse as PbRangeResponse,
    RequestOp as PbTxnRequestOp, TxnRequest as PbTxnRequest, TxnResponse as PbTxnResponse,
};
use crate::rpc::{get_prefix, KeyValue, ResponseHeader};
use tonic::transport::Channel;
use tonic::{Interceptor, IntoRequest, Request};

/// Client for KV operations.
#[repr(transparent)]
pub struct KvClient {
    inner: PbKvClient<Channel>,
}

impl KvClient {
    /// Creates a kv client.
    #[inline]
    pub fn new(channel: Channel, interceptor: Option<Interceptor>) -> Self {
        let inner = match interceptor {
            Some(it) => PbKvClient::with_interceptor(channel, it),
            None => PbKvClient::new(channel),
        };

        Self { inner }
    }

    /// Puts the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    #[inline]
    pub async fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        options: Option<PutOptions>,
    ) -> Result<PutResponse> {
        let resp = self
            .inner
            .put(options.unwrap_or_default().with_kv(key, value))
            .await?
            .into_inner();
        Ok(PutResponse::new(resp))
    }

    /// Gets the key or a range of keys from the store.
    #[inline]
    pub async fn get(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<GetOptions>,
    ) -> Result<GetResponse> {
        let resp = self
            .inner
            .range(options.unwrap_or_default().with_key(key.into()))
            .await?
            .into_inner();
        Ok(GetResponse::new(resp))
    }

    /// Deletes the given key or a range of keys from the key-value store.
    #[inline]
    pub async fn delete(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResponse> {
        let resp = self
            .inner
            .delete_range(options.unwrap_or_default().with_key(key.into()))
            .await?
            .into_inner();
        Ok(DeleteResponse::new(resp))
    }

    /// Compacts the event history in the etcd key-value store. The key-value
    /// store should be periodically compacted or the event history will continue to grow
    /// indefinitely.
    #[inline]
    pub async fn compact(
        &mut self,
        revision: i64,
        options: Option<CompactionOptions>,
    ) -> Result<CompactionResponse> {
        let resp = self
            .inner
            .compact(options.unwrap_or_default().with_revision(revision))
            .await?
            .into_inner();
        Ok(CompactionResponse::new(resp))
    }

    /// Processes multiple operations in a single transaction.
    /// A txn request increments the revision of the key-value store
    /// and generates events with the same revision for every completed operation.
    /// It is not allowed to modify the same key several times within one txn.
    #[inline]
    pub async fn txn(&mut self, txn: Txn) -> Result<TxnResponse> {
        let resp = self.inner.txn(txn).await?.into_inner();
        Ok(TxnResponse::new(resp))
    }
}

/// Options for `Put` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct PutOptions(PbPutRequest);

impl PutOptions {
    /// Set key-value pair.
    #[inline]
    fn with_kv(mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.0.key = key.into();
        self.0.value = value.into();
        self
    }

    /// Creates a `PutOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbPutRequest {
            key: Vec::new(),
            value: Vec::new(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        })
    }

    /// Lease is the lease ID to associate with the key in the key-value store. A lease
    /// value of 0 indicates no lease.
    #[inline]
    pub const fn with_lease(mut self, lease: i64) -> Self {
        self.0.lease = lease;
        self
    }

    /// If prev_kv is set, etcd gets the previous key-value pair before changing it.
    /// The previous key-value pair will be returned in the put response.
    #[inline]
    pub const fn with_prev_key(mut self) -> Self {
        self.0.prev_kv = true;
        self
    }

    /// If ignore_value is set, etcd updates the key using its current value.
    /// Returns an error if the key does not exist.
    #[inline]
    pub const fn with_ignore_value(mut self) -> Self {
        self.0.ignore_value = true;
        self
    }

    /// If ignore_lease is set, etcd updates the key using its current lease.
    /// Returns an error if the key does not exist.
    #[inline]
    pub const fn with_ignore_lease(mut self) -> Self {
        self.0.ignore_lease = true;
        self
    }
}

impl From<PutOptions> for PbPutRequest {
    #[inline]
    fn from(options: PutOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbPutRequest> for PutOptions {
    #[inline]
    fn into_request(self) -> Request<PbPutRequest> {
        Request::new(self.into())
    }
}

/// Response for `Put` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct PutResponse(PbPutResponse);

impl PutResponse {
    /// Create a new `PutResponse` from pb put response.
    #[inline]
    const fn new(resp: PbPutResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// If prev_kv is set in the request, the previous key-value pair will be returned.
    #[inline]
    pub fn prev_key(&self) -> Option<&KeyValue> {
        self.0.prev_kv.as_ref().map(From::from)
    }

    /// Takes the prev_key out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_prev_key(&mut self) -> Option<KeyValue> {
        self.0.prev_kv.take().map(KeyValue::new)
    }
}

/// Options for `Get` operation.
#[derive(Debug, Default, Clone)]
pub struct GetOptions {
    req: PbRangeRequest,
    with_prefix: bool,
    with_from_key: bool,
}

impl GetOptions {
    /// Sets key.
    #[inline]
    fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.req.key = key.into();
        self
    }

    /// Creates a `GetOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self {
            req: PbRangeRequest {
                key: Vec::new(),
                range_end: Vec::new(),
                limit: 0,
                revision: 0,
                sort_order: 0,
                sort_target: 0,
                serializable: false,
                keys_only: false,
                count_only: false,
                min_mod_revision: 0,
                max_mod_revision: 0,
                min_create_revision: 0,
                max_create_revision: 0,
            },
            with_prefix: false,
            with_from_key: false,
        }
    }

    /// Specifies the range of 'Get'.
    /// Returns the keys in the range [key, end_key).
    /// `end_key` must be lexicographically greater than start key.
    #[inline]
    pub fn with_range(mut self, end_key: impl Into<Vec<u8>>) -> Self {
        self.req.range_end = end_key.into();
        self
    }

    /// Gets all keys >= key.
    #[inline]
    pub const fn with_from_key(mut self) -> Self {
        self.with_from_key = true;
        self.with_prefix = false;
        self
    }

    /// Gets all keys prefixed with key.
    #[inline]
    pub const fn with_prefix(mut self) -> Self {
        self.with_prefix = true;
        self.with_from_key = false;
        self
    }

    /// Limits the number of keys returned for the request. When limit is set to 0,
    /// it is treated as no limit.
    #[inline]
    pub const fn with_limit(mut self, limit: i64) -> Self {
        self.req.limit = limit;
        self
    }

    /// The point-in-time of the key-value store to use for the range.
    /// If revision is less or equal to zero, the range is over the newest key-value store.
    /// If the revision has been compacted, ErrCompacted is returned as a response.
    #[inline]
    pub const fn with_revision(mut self, revision: i64) -> Self {
        self.req.revision = revision;
        self
    }

    /// Sets the order for returned sorted results.
    /// It requires 'with_range' and/or 'with_prefix' to be specified too.
    #[inline]
    pub fn with_sort(mut self, target: SortTarget, order: SortOrder) -> Self {
        if target == SortTarget::Key && order == SortOrder::Ascend {
            // If order != SortOrder::None, server fetches the entire key-space,
            // and then applies the sort and limit, if provided.
            // Since by default the server returns results sorted by keys
            // in lexicographically ascending order, the client should ignore
            // SortOrder if the target is SortTarget::Key.
            self.req.sort_order = SortOrder::None as i32;
        } else {
            self.req.sort_order = order as i32;
        }
        self.req.sort_target = target as i32;
        self
    }

    /// Sets the get request to use serializable member-local reads.
    /// Get requests are linearizable by default; linearizable requests have higher
    /// latency and lower throughput than serializable requests but reflect the current
    /// consensus of the cluster. For better performance, in exchange for possible stale reads,
    /// a serializable get request is served locally without needing to reach consensus
    /// with other nodes in the cluster.
    #[inline]
    pub const fn with_serializable(mut self) -> Self {
        self.req.serializable = true;
        self
    }

    /// Returns only the keys and not the values.
    #[inline]
    pub const fn with_keys_only(mut self) -> Self {
        self.req.keys_only = true;
        self
    }

    /// Returns only the count of the keys in the range.
    #[inline]
    pub const fn with_count_only(mut self) -> Self {
        self.req.count_only = true;
        self
    }

    /// Sets the lower bound for returned key mod revisions; all keys with
    /// lesser mod revisions will be filtered away.
    #[inline]
    pub const fn with_min_mod_revision(mut self, revision: i64) -> Self {
        self.req.min_mod_revision = revision;
        self
    }

    /// Sets the upper bound for returned key mod revisions; all keys with
    /// greater mod revisions will be filtered away.
    #[inline]
    pub const fn with_max_mod_revision(mut self, revision: i64) -> Self {
        self.req.max_mod_revision = revision;
        self
    }

    /// Sets the lower bound for returned key create revisions; all keys with
    /// lesser create revisions will be filtered away.
    #[inline]
    pub const fn with_min_create_revision(mut self, revision: i64) -> Self {
        self.req.min_create_revision = revision;
        self
    }

    /// `max_create_revision` is the upper bound for returned key create revisions; all keys with
    /// greater create revisions will be filtered away.
    #[inline]
    pub const fn with_max_create_revision(mut self, revision: i64) -> Self {
        self.req.max_create_revision = revision;
        self
    }
}

impl From<GetOptions> for PbRangeRequest {
    #[inline]
    fn from(mut options: GetOptions) -> Self {
        if options.with_from_key {
            if options.req.key.is_empty() {
                options.req.key = vec![b'\0'];
            }
            options.req.range_end = vec![b'\0'];
        } else if options.with_prefix {
            if options.req.key.is_empty() {
                options.req.key = vec![b'\0'];
                options.req.range_end = vec![b'\0'];
            } else {
                options.req.range_end = get_prefix(&options.req.key);
            }
        }
        options.req
    }
}

impl IntoRequest<PbRangeRequest> for GetOptions {
    #[inline]
    fn into_request(self) -> Request<PbRangeRequest> {
        Request::new(self.into())
    }
}

/// Response for `Get` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct GetResponse(PbRangeResponse);

impl GetResponse {
    /// Create a new `GetResponse` from pb get response.
    #[inline]
    const fn new(resp: PbRangeResponse) -> Self {
        Self(resp)
    }

    /// Get response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// The list of key-value pairs matched by the `Get` request.
    /// kvs is empty when count is requested.
    #[inline]
    pub fn kvs(&self) -> &[KeyValue] {
        unsafe { &*(self.0.kvs.as_slice() as *const _ as *const [KeyValue]) }
    }

    /// Indicates if there are more keys to return in the requested range.
    #[inline]
    pub const fn more(&self) -> bool {
        self.0.more
    }

    /// The number of keys within the range when requested.
    #[inline]
    pub const fn count(&self) -> i64 {
        self.0.count
    }
}

/// Options for `Delete` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct DeleteOptions(PbDeleteRequest);

impl DeleteOptions {
    /// Sets key.
    #[inline]
    fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.0.key = key.into();
        self
    }

    /// Creates a `DeleteOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbDeleteRequest {
            key: Vec::new(),
            range_end: Vec::new(),
            prev_kv: false,
        })
    }

    /// `end_key` is the key following the last key to delete for the range [key, end_key).
    /// If `end_key` is not given, the range is defined to contain only the key argument.
    /// If `end_key` is one bit larger than the given key, then the range is all the keys
    /// with the prefix (the given key).
    /// If `end_key` is '\0', the range is all keys greater than or equal to the key argument.
    #[inline]
    pub fn with_range(mut self, end_key: impl Into<Vec<u8>>) -> Self {
        self.0.range_end = end_key.into();
        self
    }

    /// If `prev_kv` is set, etcd gets the previous key-value pairs before deleting it.
    /// The previous key-value pairs will be returned in the delete response.
    #[inline]
    pub const fn with_prev_key(mut self) -> Self {
        self.0.prev_kv = true;
        self
    }
}

impl From<DeleteOptions> for PbDeleteRequest {
    #[inline]
    fn from(options: DeleteOptions) -> Self {
        options.0
    }
}

impl IntoRequest<PbDeleteRequest> for DeleteOptions {
    #[inline]
    fn into_request(self) -> Request<DeleteRangeRequest> {
        Request::new(self.into())
    }
}

/// Response for `Delete` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct DeleteResponse(PbDeleteResponse);

impl DeleteResponse {
    /// Create a new `DeleteResponse` from pb delete response.
    #[inline]
    const fn new(resp: PbDeleteResponse) -> Self {
        Self(resp)
    }

    /// Delete response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// The number of keys deleted by the delete request.
    #[inline]
    pub const fn deleted(&self) -> i64 {
        self.0.deleted
    }

    /// If `prev_kv` is set in the request, the previous key-value pairs will be returned.
    #[inline]
    pub fn prev_kvs(&self) -> &[KeyValue] {
        unsafe { &*(self.0.prev_kvs.as_slice() as *const _ as *const [KeyValue]) }
    }
}

/// Options for `Compact` operation.
#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct CompactionOptions(PbCompactionRequest);

impl CompactionOptions {
    /// Creates a `CompactionOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self(PbCompactionRequest {
            revision: 0,
            physical: false,
        })
    }

    /// The key-value store revision for the compaction operation.
    #[inline]
    const fn with_revision(mut self, revision: i64) -> Self {
        self.0.revision = revision;
        self
    }

    /// Physical is set so the RPC will wait until the compaction is physically
    /// applied to the local database such that compacted entries are totally
    /// removed from the backend database.
    #[inline]
    pub const fn with_physical(mut self) -> Self {
        self.0.physical = true;
        self
    }
}

impl IntoRequest<PbCompactionRequest> for CompactionOptions {
    #[inline]
    fn into_request(self) -> Request<CompactionRequest> {
        Request::new(self.0)
    }
}

/// Response for `Compact` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct CompactionResponse(PbCompactionResponse);

impl CompactionResponse {
    /// Create a new `CompactionResponse` from pb compaction response.
    #[inline]
    const fn new(resp: PbCompactionResponse) -> Self {
        Self(resp)
    }

    /// Compact response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }
}

/// Transaction comparision.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Compare(PbCompare);

impl Compare {
    /// Creates a new `Compare`.
    #[inline]
    fn new(
        key: impl Into<Vec<u8>>,
        cmp: CompareOp,
        target: CompareTarget,
        target_union: TargetUnion,
    ) -> Self {
        Self(PbCompare {
            result: cmp as i32,
            target: target as i32,
            key: key.into(),
            range_end: Vec::new(),
            target_union: Some(target_union),
        })
    }

    /// Compares the version of the given key.
    #[inline]
    pub fn version(key: impl Into<Vec<u8>>, cmp: CompareOp, version: i64) -> Self {
        Self::new(
            key,
            cmp,
            CompareTarget::Version,
            TargetUnion::Version(version),
        )
    }

    /// Compares the creation revision of the given key.
    #[inline]
    pub fn create_revision(key: impl Into<Vec<u8>>, cmp: CompareOp, revision: i64) -> Self {
        Self::new(
            key,
            cmp,
            CompareTarget::Create,
            TargetUnion::CreateRevision(revision),
        )
    }

    /// Compares the last modified revision of the given key.
    #[inline]
    pub fn mod_revision(key: impl Into<Vec<u8>>, cmp: CompareOp, revision: i64) -> Self {
        Self::new(
            key,
            cmp,
            CompareTarget::Mod,
            TargetUnion::ModRevision(revision),
        )
    }

    /// Compares the value of the given key.
    #[inline]
    pub fn value(key: impl Into<Vec<u8>>, cmp: CompareOp, value: impl Into<Vec<u8>>) -> Self {
        Self::new(
            key,
            cmp,
            CompareTarget::Value,
            TargetUnion::Value(value.into()),
        )
    }

    /// Compares the lease id of the given key.
    #[inline]
    pub fn lease(key: impl Into<Vec<u8>>, cmp: CompareOp, lease: i64) -> Self {
        Self::new(key, cmp, CompareTarget::Lease, TargetUnion::Lease(lease))
    }

    /// Sets the comparison to scan the range [key, end).
    #[inline]
    pub fn with_range(mut self, end: impl Into<Vec<u8>>) -> Self {
        self.0.range_end = end.into();
        self
    }

    /// Sets the comparison to scan all keys prefixed by the key.
    #[inline]
    pub fn with_prefix(mut self) -> Self {
        self.0.range_end = get_prefix(&self.0.key);
        self
    }
}

/// Transaction operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct TxnOp(PbTxnOp);

impl TxnOp {
    /// `Put` operation.
    #[inline]
    pub fn put(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        options: Option<PutOptions>,
    ) -> Self {
        TxnOp(PbTxnOp::RequestPut(
            options.unwrap_or_default().with_kv(key, value).into(),
        ))
    }

    /// `Get` operation.
    #[inline]
    pub fn get(key: impl Into<Vec<u8>>, options: Option<GetOptions>) -> Self {
        TxnOp(PbTxnOp::RequestRange(
            options.unwrap_or_default().with_key(key).into(),
        ))
    }

    /// `Delete` operation.
    #[inline]
    pub fn delete(key: impl Into<Vec<u8>>, options: Option<DeleteOptions>) -> Self {
        TxnOp(PbTxnOp::RequestDeleteRange(
            options.unwrap_or_default().with_key(key).into(),
        ))
    }

    /// `Txn` operation.
    #[inline]
    pub fn txn(txn: Txn) -> Self {
        TxnOp(PbTxnOp::RequestTxn(txn.into()))
    }
}

impl From<TxnOp> for PbTxnOp {
    #[inline]
    fn from(op: TxnOp) -> Self {
        op.0
    }
}

/// Transaction of multiple operations.
#[derive(Debug, Default, Clone)]
pub struct Txn {
    req: PbTxnRequest,
    c_when: bool,
    c_then: bool,
    c_else: bool,
}

impl Txn {
    /// Creates a new transaction.
    #[inline]
    pub const fn new() -> Self {
        Self {
            req: PbTxnRequest {
                compare: Vec::new(),
                success: Vec::new(),
                failure: Vec::new(),
            },
            c_when: false,
            c_then: false,
            c_else: false,
        }
    }

    /// Takes a list of comparison. If all comparisons passed in succeed,
    /// the operations passed into `and_then()` will be executed. Or the operations
    /// passed into `or_else()` will be executed.
    #[inline]
    pub fn when(mut self, compares: impl Into<Vec<Compare>>) -> Self {
        assert!(!self.c_when, "cannot call when twice");
        assert!(!self.c_then, "cannot call when after and_then");
        assert!(!self.c_else, "cannot call when after or_else");

        self.c_when = true;
        self.req.compare = unsafe { std::mem::transmute(compares.into()) };
        self
    }

    /// Takes a list of operations. The operations list will be executed, if the
    /// comparisons passed in `when()` succeed.
    #[inline]
    pub fn and_then(mut self, operations: impl Into<Vec<TxnOp>>) -> Self {
        assert!(!self.c_then, "cannot call and_then twice");
        assert!(!self.c_else, "cannot call and_then after or_else");

        self.c_then = true;
        self.req.success = operations
            .into()
            .into_iter()
            .map(|op| PbTxnRequestOp {
                request: Some(op.into()),
            })
            .collect();
        self
    }

    /// Takes a list of operations. The operations list will be executed, if the
    /// comparisons passed in `when()` fail.
    #[inline]
    pub fn or_else(mut self, operations: impl Into<Vec<TxnOp>>) -> Self {
        assert!(!self.c_else, "cannot call or_else twice");

        self.c_else = true;
        self.req.failure = operations
            .into()
            .into_iter()
            .map(|op| PbTxnRequestOp {
                request: Some(op.into()),
            })
            .collect();
        self
    }
}

impl From<Txn> for PbTxnRequest {
    #[inline]
    fn from(txn: Txn) -> Self {
        txn.req
    }
}

impl IntoRequest<PbTxnRequest> for Txn {
    #[inline]
    fn into_request(self) -> Request<PbTxnRequest> {
        Request::new(self.into())
    }
}

/// Transaction operation response.
#[derive(Debug, Clone)]
pub enum TxnOpResponse {
    Put(PutResponse),
    Get(GetResponse),
    Delete(DeleteResponse),
    Txn(TxnResponse),
}

/// Response for `Txn` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct TxnResponse(PbTxnResponse);

impl TxnResponse {
    /// Creates a new `Txn` response.
    #[inline]
    const fn new(resp: PbTxnResponse) -> Self {
        Self(resp)
    }

    /// Transaction response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// Returns `true` if the compare evaluated to true or `false` otherwise.
    #[inline]
    pub const fn succeeded(&self) -> bool {
        self.0.succeeded
    }

    /// Returns responses of transaction operations.
    #[inline]
    pub fn op_responses(&self) -> Vec<TxnOpResponse> {
        self.0
            .responses
            .iter()
            .map(|resp| match resp.response.as_ref().unwrap() {
                PbTxnOpResponse::ResponsePut(put) => {
                    TxnOpResponse::Put(PutResponse::new(put.clone()))
                }
                PbTxnOpResponse::ResponseRange(get) => {
                    TxnOpResponse::Get(GetResponse::new(get.clone()))
                }
                PbTxnOpResponse::ResponseDeleteRange(delete) => {
                    TxnOpResponse::Delete(DeleteResponse::new(delete.clone()))
                }
                PbTxnOpResponse::ResponseTxn(txn) => {
                    TxnOpResponse::Txn(TxnResponse::new(txn.clone()))
                }
            })
            .collect()
    }
}
