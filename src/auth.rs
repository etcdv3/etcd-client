//! Authentication service.

use crate::error::Error;
use crate::AuthClient;

use http::{header::AUTHORIZATION, HeaderValue, Request};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use tower_service::Service;

#[derive(Debug, Clone)]
pub struct AuthService<S> {
    inner: S,
    token: Arc<RwLock<Option<HeaderValue>>>,
}

impl<S> AuthService<S> {
    #[inline]
    pub fn new(inner: S, token: Arc<RwLock<Option<HeaderValue>>>) -> Self {
        Self { inner, token }
    }
}

impl<S, Body, Response> Service<Request<Body>> for AuthService<S>
where
    S: Service<Request<Body>, Response = Response>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        if let Some(token) = self.token.read().unwrap().as_ref() {
            request.headers_mut().insert(AUTHORIZATION, token.clone());
        }

        self.inner.call(request)
    }
}

#[derive(Clone)]
pub struct AuthHandle {
    token: Arc<RwLock<Option<HeaderValue>>>,
    cli: AuthClient,
}

impl AuthHandle {
    #[inline]
    pub(crate) fn new(token: Arc<RwLock<Option<HeaderValue>>>, cli: AuthClient) -> Self {
        Self { token, cli }
    }

    /// Updates client authentication.
    pub async fn update_auth(&mut self, name: String, password: String) -> Result<(), Error> {
        let resp = self.cli.authenticate(name, password).await?;

        self.token.write().unwrap().replace(resp.token().parse()?);

        Ok(())
    }

    /// Removes client authentication.
    pub fn remove_auth(&mut self) {
        self.token.write().unwrap().take();
    }
}
