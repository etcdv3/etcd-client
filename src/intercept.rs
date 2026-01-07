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
                ("etcdserverpb.Auth", "Authenticate")
                | ("etcdserverpb.Cluster", "MemberList")
                | ("etcdserverpb.Lease", _) => {
                    // Skip adding the token for all Lease methods
                }
                (service, _)
                    if service.starts_with("etcdserverpb")
                        || service.starts_with("v3electionpb")
                        || service.starts_with("v3lockpb") =>
                {
                    // Add the authentication token if it exists
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
