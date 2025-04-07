//! This module mainly provides [`EndpointUpdate`] trait.

use std::sync::Arc;

use http::Uri;
use tokio::sync::mpsc::Sender;
use tonic::transport::channel::Change as TonicChange;
use tonic::transport::Endpoint;
use tower::discover::Change as TowerChange;

/// The trait is responsible for updating the endpoint.
#[async_trait::async_trait]
pub trait EndpointUpdate: Send + Sync + 'static {
    /// Sends a endpoint change message.
    async fn send(&self, change: TowerChange<Uri, Endpoint>);
}

pub type EndpointUpdateRef = Arc<dyn EndpointUpdate + Send + Sync>;

pub struct TonicEndpointUpdater {
    pub sender: Sender<TonicChange<Uri, Endpoint>>,
}

#[async_trait::async_trait]
impl EndpointUpdate for TonicEndpointUpdater {
    async fn send(&self, change: TowerChange<Uri, Endpoint>) {
        let change = match change {
            TowerChange::Insert(key, svc) => TonicChange::Insert(key, svc),
            TowerChange::Remove(key) => TonicChange::Remove(key),
        };
        let _ = self.sender.send(change).await;
    }
}

pub struct TowerEndpointUpdater {
    pub sender: Sender<TowerChange<Uri, Endpoint>>,
}

#[async_trait::async_trait]
impl EndpointUpdate for TowerEndpointUpdater {
    async fn send(&self, change: TowerChange<Uri, Endpoint>) {
        let _ = self.sender.send(change).await;
    }
}
