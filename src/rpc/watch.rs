//! Etcd Watch RPC.

pub use crate::rpc::pb::mvccpb::event::EventType;

use crate::error::{Error, Result};
use crate::rpc::pb::etcdserverpb::watch_client::WatchClient as PbWatchClient;
use crate::rpc::pb::etcdserverpb::watch_request::RequestUnion as WatchRequestUnion;
use crate::rpc::pb::etcdserverpb::{
    WatchCancelRequest, WatchCreateRequest, WatchProgressRequest, WatchRequest,
    WatchResponse as PbWatchResponse,
};
use crate::rpc::pb::mvccpb::Event as PbEvent;
use crate::rpc::{get_prefix, KeyValue, ResponseHeader};
use std::task::{Context, Poll};
use tokio::stream::Stream;
use tokio::sync::mpsc::{channel, Sender};
use tonic::codegen::Pin;
use tonic::transport::Channel;
use tonic::{Interceptor, Streaming};

/// Client for watch operations.
#[repr(transparent)]
pub struct WatchClient {
    inner: PbWatchClient<Channel>,
}

impl WatchClient {
    /// Creates a watch client.
    #[inline]
    pub fn new(channel: Channel, interceptor: Option<Interceptor>) -> Self {
        let inner = match interceptor {
            Some(it) => PbWatchClient::with_interceptor(channel, it),
            None => PbWatchClient::new(channel),
        };

        Self { inner }
    }

    /// Watches for events happening or that have happened. Both input and output
    /// are streams; the input stream is for creating and canceling watchers and the output
    /// stream sends events. One watch RPC can watch on multiple key ranges, streaming events
    /// for several watches at once. The entire event history can be watched starting from the
    /// last compaction revision.
    pub async fn watch(
        &mut self,
        key: impl Into<Vec<u8>>,
        options: Option<WatchOptions>,
    ) -> Result<(Watcher, WatchStream)> {
        let (mut sender, receiver) = channel::<WatchRequest>(100);
        sender
            .send(options.unwrap_or_default().with_key(key).into())
            .await
            .map_err(|e| Error::WatchError(e.to_string()))?;

        let mut stream = self.inner.watch(receiver).await?.into_inner();

        let watch_id = match stream.message().await? {
            Some(resp) => {
                assert!(resp.created, "not a create watch response");
                resp.watch_id
            }
            None => {
                return Err(Error::WatchError("failed to create watch".to_string()));
            }
        };

        Ok((Watcher::new(watch_id, sender), WatchStream::new(stream)))
    }
}

/// Options for `Watch` operation.
#[derive(Debug, Default, Clone)]
pub struct WatchOptions {
    req: WatchCreateRequest,
    with_prefix: bool,
}

impl WatchOptions {
    /// Sets key.
    #[inline]
    fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.req.key = key.into();
        self
    }

    /// Creates a new `WatchOptions`.
    #[inline]
    pub const fn new() -> Self {
        Self {
            req: WatchCreateRequest {
                key: Vec::new(),
                range_end: Vec::new(),
                start_revision: 0,
                progress_notify: false,
                filters: Vec::new(),
                prev_kv: false,
                watch_id: 0,
                fragment: false,
            },
            with_prefix: false,
        }
    }

    /// Sets the end of the range [key, end) to watch. If `end` is not given,
    /// only the key argument is watched. If `end` is equal to '\0', all keys greater than
    /// or equal to the key argument are watched.
    #[inline]
    pub fn with_range(mut self, end: impl Into<Vec<u8>>) -> Self {
        self.req.range_end = end.into();
        self
    }

    /// Watches all keys prefixed with key.
    #[inline]
    pub const fn with_prefix(mut self) -> Self {
        self.with_prefix = true;
        self
    }

    /// Sets the revision to watch from (inclusive). No `start_revision` is "now".
    #[inline]
    pub const fn with_start_revision(mut self, revision: i64) -> Self {
        self.req.start_revision = revision;
        self
    }

    /// `progress_notify` is set so that the etcd server will periodically send a `WatchResponse` with
    /// no events to the new watcher if there are no recent events. It is useful when clients
    /// wish to recover a disconnected watcher starting from a recent known revision.
    /// The etcd server may decide how often it will send notifications based on current load.
    #[inline]
    pub const fn with_progress_notify(mut self) -> Self {
        self.req.progress_notify = true;
        self
    }

    /// Filter the events at server side before it sends back to the watcher.
    #[inline]
    pub fn with_filters(mut self, filters: impl Into<Vec<WatchFilterType>>) -> Self {
        self.req.filters = filters.into().into_iter().map(|f| f as i32).collect();
        self
    }

    /// If `prev_kv` is set, created watcher gets the previous KV before the event happens.
    /// If the previous KV is already compacted, nothing will be returned.
    #[inline]
    pub const fn with_prev_key(mut self) -> Self {
        self.req.prev_kv = true;
        self
    }

    /// If `watch_id` is provided and non-zero, it will be assigned to this watcher.
    /// Since creating a watcher in etcd is not a synchronous operation,
    /// this can be used ensure that ordering is correct when creating multiple
    /// watchers on the same stream. Creating a watcher with an ID already in
    /// use on the stream will cause an error to be returned.
    #[inline]
    pub const fn with_watch_id(mut self, watch_id: i64) -> Self {
        self.req.watch_id = watch_id;
        self
    }

    /// Enables splitting large revisions into multiple watch responses.
    #[inline]
    pub const fn with_fragment(mut self) -> Self {
        self.req.fragment = true;
        self
    }
}

impl From<WatchOptions> for WatchCreateRequest {
    #[inline]
    fn from(mut options: WatchOptions) -> Self {
        if options.req.key.is_empty() {
            options.req.key = vec![b'\0'];
            options.req.range_end = vec![b'\0'];
        } else {
            options.req.range_end = get_prefix(&options.req.key);
        }
        options.req
    }
}

impl From<WatchOptions> for WatchRequest {
    #[inline]
    fn from(options: WatchOptions) -> Self {
        Self {
            request_union: Some(WatchRequestUnion::CreateRequest(options.into())),
        }
    }
}

impl From<WatchCancelRequest> for WatchRequest {
    #[inline]
    fn from(req: WatchCancelRequest) -> Self {
        Self {
            request_union: Some(WatchRequestUnion::CancelRequest(req)),
        }
    }
}

impl From<WatchProgressRequest> for WatchRequest {
    #[inline]
    fn from(req: WatchProgressRequest) -> Self {
        Self {
            request_union: Some(WatchRequestUnion::ProgressRequest(req)),
        }
    }
}

/// Watch filter type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum WatchFilterType {
    /// Filter out put event.
    NoPut = 0,
    /// Filter out delete event.
    NoDelete = 1,
}

/// Response for `Watch` operation.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct WatchResponse(PbWatchResponse);

impl WatchResponse {
    /// Creates a new `WatchResponse`.
    #[inline]
    const fn new(resp: PbWatchResponse) -> Self {
        Self(resp)
    }

    /// Watch response header.
    #[inline]
    pub fn header(&self) -> Option<&ResponseHeader> {
        self.0.header.as_ref().map(From::from)
    }

    /// Takes the header out of the response, leaving a [`None`] in its place.
    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    /// The ID of the watcher that corresponds to the response.
    #[inline]
    pub const fn watch_id(&self) -> i64 {
        self.0.watch_id
    }

    /// created is set to true if the response is for a create watch request.
    /// The client should record the watch_id and expect to receive events for
    /// the created watcher from the same stream.
    /// All events sent to the created watcher will attach with the same watch_id.
    #[allow(dead_code)]
    #[inline]
    const fn created(&self) -> bool {
        self.0.created
    }

    /// `canceled` is set to true if the response is for a cancel watch request.
    /// No further events will be sent to the canceled watcher.
    #[inline]
    pub const fn canceled(&self) -> bool {
        self.0.canceled
    }

    /// `compact_revision` is set to the minimum index if a watcher tries to watch
    /// at a compacted index.
    ///
    /// This happens when creating a watcher at a compacted revision or the watcher cannot
    /// catch up with the progress of the key-value store.
    ///
    /// The client should treat the watcher as canceled and should not try to create any
    /// watcher with the same start_revision again.
    #[inline]
    pub const fn compact_revision(&self) -> i64 {
        self.0.compact_revision
    }

    /// Indicates the reason for canceling the watcher.
    #[inline]
    pub fn cancel_reason(&self) -> &str {
        &self.0.cancel_reason
    }

    /// Events happened on the watched keys.
    #[inline]
    pub fn events(&self) -> &[Event] {
        unsafe { &*(self.0.events.as_slice() as *const _ as *const [Event]) }
    }
}

/// Watching event.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Event(PbEvent);

impl Event {
    /// The kind of event. If type is a `Put`, it indicates
    /// new data has been stored to the key. If type is a `Delete`,
    /// it indicates the key was deleted.
    #[inline]
    pub fn event_type(&self) -> EventType {
        match self.0.r#type {
            0 => EventType::Put,
            1 => EventType::Delete,
            i => panic!("unknown event {}", i),
        }
    }

    /// The KeyValue for the event.
    /// A `Put` event contains current kv pair.
    /// A `Put` event with `kv.version()==1` indicates the creation of a key.
    /// A `Delete` event contains the deleted key with
    /// its modification revision set to the revision of deletion.
    #[inline]
    pub fn kv(&self) -> Option<&KeyValue> {
        self.0.kv.as_ref().map(From::from)
    }

    /// The key-value pair before the event happens.
    #[inline]
    pub fn prev_kv(&self) -> Option<&KeyValue> {
        self.0.prev_kv.as_ref().map(From::from)
    }
}

/// The watching handle.
#[derive(Debug)]
pub struct Watcher {
    watch_id: i64,
    sender: Sender<WatchRequest>,
}

impl Watcher {
    /// Creates a new `Watcher`.
    #[inline]
    const fn new(watch_id: i64, sender: Sender<WatchRequest>) -> Self {
        Self { watch_id, sender }
    }

    /// The ID of the watcher.
    #[inline]
    pub const fn watch_id(&self) -> i64 {
        self.watch_id
    }

    /// Cancels this watcher.
    #[inline]
    pub async fn cancel(&mut self) -> Result<()> {
        let req = WatchCancelRequest {
            watch_id: self.watch_id,
        };
        self.sender
            .send(req.into())
            .await
            .map_err(|e| Error::WatchError(e.to_string()))
    }

    /// Requests a watch stream progress status be sent in the watch response stream as soon as
    /// possible.
    #[inline]
    pub async fn request_progress(&mut self) -> Result<()> {
        let req = WatchProgressRequest {};
        self.sender
            .send(req.into())
            .await
            .map_err(|e| Error::WatchError(e.to_string()))
    }
}

/// The watch response stream.
#[derive(Debug)]
pub struct WatchStream {
    stream: Streaming<PbWatchResponse>,
}

impl WatchStream {
    /// Creates a new `WatchStream`.
    #[inline]
    const fn new(stream: Streaming<PbWatchResponse>) -> Self {
        Self { stream }
    }

    /// Fetch the next message from this stream.
    #[inline]
    pub async fn message(&mut self) -> Result<Option<WatchResponse>> {
        match self.stream.message().await? {
            Some(resp) => Ok(Some(WatchResponse::new(resp))),
            None => Ok(None),
        }
    }
}

impl Stream for WatchStream {
    type Item = Result<WatchResponse>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().stream)
            .poll_next(cx)
            .map(|t| match t {
                Some(Ok(resp)) => Some(Ok(WatchResponse::new(resp))),
                Some(Err(e)) => Some(Err(From::from(e))),
                None => None,
            })
    }
}
