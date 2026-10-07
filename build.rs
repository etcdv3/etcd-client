#[cfg(feature = "build-server")]
fn should_build_server() -> bool {
    true
}

#[cfg(not(feature = "build-server"))]
fn should_build_server() -> bool {
    false
}

fn main() {
    let proto_root = "proto";
    println!("cargo:rerun-if-changed={proto_root}");

    tonic_prost_build::configure()
        .build_server(should_build_server())
        .compile_protos(
            &[
                "proto/etcd/api/authpb/auth.proto",
                "proto/etcd/api/etcdserverpb/rpc.proto",
                "proto/etcd/api/mvccpb/kv.proto",
                "proto/etcd/server/etcdserver/api/v3election/v3electionpb/v3election.proto",
                "proto/etcd/server/etcdserver/api/v3lock/v3lockpb/v3lock.proto",
            ],
            &[proto_root],
        )
        .expect("Failed to compile proto files");
}
