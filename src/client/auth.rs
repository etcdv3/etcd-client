use std::{
    sync::Arc,
    task::{Context, Poll},
};

use http::{header::AUTHORIZATION, HeaderValue, Request};
use tower::{Layer, Service};

#[derive(Debug, Clone)]
pub(crate) struct AuthLayer {
    token: Option<Arc<HeaderValue>>,
}

impl AuthLayer {
    pub(crate) fn new(token: Option<HeaderValue>) -> Self {
        Self {
            token: token.map(Arc::new),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AuthService<S> {
    token: Option<Arc<HeaderValue>>,
    inner: S,
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            token: self.token.clone(),
            inner,
        }
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
