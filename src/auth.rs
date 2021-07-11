use std::{
    sync::Arc,
    task::{Context, Poll},
};

use http::{header::AUTHORIZATION, HeaderValue, Request};
use tower_service::Service;

#[derive(Debug, Clone)]
pub(crate) struct AuthService<S> {
    inner: S,
    token: Option<Arc<HeaderValue>>,
}

impl<S> AuthService<S> {
    pub(crate) fn new(inner: S, token: Option<Arc<HeaderValue>>) -> Self {
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

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        if let Some(token) = &self.token {
            request
                .headers_mut()
                .insert(AUTHORIZATION, token.as_ref().clone());
        }

        self.inner.call(request)
    }
}
