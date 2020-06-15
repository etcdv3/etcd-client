fn main() {
    let proto_root = "proto";
    println!("cargo:rerun-if-changed={}", proto_root);

    tonic_build::configure()
        .build_server(false)
        .compile(
            &[
                "proto/auth.proto",
                "proto/kv.proto",
                "proto/rpc.proto",
                "proto/v3election.proto",
                "proto/v3lock.proto",
            ],
            &[proto_root],
        )
        .expect("Failed to compile proto files");
}
