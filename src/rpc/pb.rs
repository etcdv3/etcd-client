//! Etcd gRPC API, auto-generated by prost.

#![allow(clippy::enum_variant_names)]
#![allow(clippy::derive_partial_eq_without_eq)]
#![allow(clippy::doc_lazy_continuation)]

pub mod authpb {
    tonic::include_proto!("authpb");
}

pub mod etcdserverpb {
    tonic::include_proto!("etcdserverpb");
}

pub mod mvccpb {
    tonic::include_proto!("mvccpb");
}

pub mod v3electionpb {
    tonic::include_proto!("v3electionpb");
}

pub mod v3lockpb {
    tonic::include_proto!("v3lockpb");
}
