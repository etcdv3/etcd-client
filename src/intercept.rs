use crate::lock::RwLockExt;
use std::sync::{Arc, RwLock};

use crate::channel::Channel;
use tonic::{
    metadata::{Ascii, AsciiMetadataValue, MetadataValue},
    service::{interceptor::InterceptedService, Interceptor as TonicInterceptor},
    GrpcMethod,
};

const REQUIRE_LEADER_KEY: &str = "hasleader";
const REQUIRE_LEADER_VALUE: &str = "true";
const TOKEN_HEADER: &str = "token";

/// An interceptor that conditionally attaches a leader requirement
/// to the underlying service.
#[derive(Clone)]
pub struct Interceptor {
    pub require_leader: bool,
    pub auth_token: Arc<RwLock<Option<MetadataValue<Ascii>>>>,
}

impl Interceptor {
    fn append_token(&mut self, request: &mut tonic::Request<()>) {
        if let Some(token) = self.auth_token.read_unpoisoned().as_ref() {
            request.metadata_mut().append(TOKEN_HEADER, token.clone());
        }
    }
}

impl TonicInterceptor for Interceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(method) = request.extensions().get::<GrpcMethod>() {
            match (method.service(), method.method()) {
                // `Authenticate` is how the token is obtained.
                ("etcdserverpb.Auth", "Authenticate") => {}
                (service, _)
                    if service.starts_with("etcdserverpb")
                        || service.starts_with("v3electionpb")
                        || service.starts_with("v3lockpb") =>
                {
                    // Add the authentication token if it exists. Which RPCs require it varies
                    // across etcd versions, e.g. `Cluster.MemberList` does since 3.6 (observed on
                    // 3.6.15 and 3.7.2, but not on 3.5.26), and the server accepts the token on
                    // those which don't.
                    self.append_token(&mut request);
                }
                _ => {
                    // For all other methods, e.g. health API, skip adding the token
                }
            }
        }

        if self.require_leader {
            request.metadata_mut().append(
                REQUIRE_LEADER_KEY,
                AsciiMetadataValue::from_static(REQUIRE_LEADER_VALUE),
            );
        }

        Ok(request)
    }
}

/// A type alias to make using this interceptor easier.
pub type InterceptedChannel = InterceptedService<Channel, Interceptor>;

#[cfg(test)]
mod tests {
    use super::*;

    fn token_of(service: &'static str, method: &'static str) -> Option<String> {
        let mut interceptor = Interceptor {
            require_leader: false,
            auth_token: Arc::new(RwLock::new(Some("token".parse().unwrap()))),
        };
        let mut request = tonic::Request::new(());
        request
            .extensions_mut()
            .insert(GrpcMethod::new(service, method));
        let request = interceptor.call(request).unwrap();
        request
            .metadata()
            .get(TOKEN_HEADER)
            .map(|v| v.to_str().unwrap().to_owned())
    }

    #[test]
    fn token_is_sent_to_etcd_services() {
        for (service, method) in [
            ("etcdserverpb.KV", "Range"),
            ("etcdserverpb.Watch", "Watch"),
            ("etcdserverpb.Lease", "LeaseGrant"),
            ("etcdserverpb.Lease", "LeaseKeepAlive"),
            ("etcdserverpb.Cluster", "MemberList"),
            ("etcdserverpb.Maintenance", "Status"),
            ("etcdserverpb.Auth", "UserAdd"),
            ("v3electionpb.Election", "Campaign"),
            ("v3lockpb.Lock", "Lock"),
        ] {
            assert_eq!(
                token_of(service, method).as_deref(),
                Some("token"),
                "{service}/{method}"
            );
        }
    }

    #[test]
    fn token_is_not_sent_to_authenticate_and_non_etcd_services() {
        for (service, method) in [
            ("etcdserverpb.Auth", "Authenticate"),
            ("grpc.health.v1.Health", "Check"),
        ] {
            assert_eq!(token_of(service, method), None, "{service}/{method}");
        }
    }
}
