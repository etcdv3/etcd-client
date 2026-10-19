# etcd-client

[![Minimum rustc version](https://img.shields.io/badge/rustc-1.80+-lightgray.svg)](https://github.com/etcdv3/etcd-client#rust-version-requirements)
[![Crate](https://img.shields.io/crates/v/etcd-client.svg)](https://crates.io/crates/etcd-client)
[![API](https://docs.rs/etcd-client/badge.svg)](https://docs.rs/etcd-client)
[![Etcd Client Test](https://github.com/etcdv3/etcd-client/actions/workflows/ci.yml/badge.svg)](https://github.com/etcdv3/etcd-client/actions/workflows/ci.yml)

[![License: Apache](https://img.shields.io/badge/License-Apache%202.0-red.svg)](LICENSE-APACHE)
OR
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE-MIT)

An [etcd](https://github.com/etcd-io/etcd) v3 API client for Rust.
It provides asynchronous client backed by [tokio](https://github.com/tokio-rs/tokio)
and [tonic](https://github.com/hyperium/tonic).

## Features

- etcd API v3
- asynchronous

## Supported APIs

- [x] KV
    - [x] RangeStream (etcd 3.7+)
- [x] Watch
- [x] Lease
- [x] Auth
- [x] Maintenance
- [x] Cluster
- [x] Lock
- [x] Election
- [x] Namespace

From etcd 3.7, the new RPC method [`RangeStream`](https://etcd.io/docs/v3.7/learning/api/#rangestream)
is added to the `KV` service. The `etcd-client` crate supports this new RPC method via
`KvClient::get_stream()` method without any feature flags since `v0.21.0`. Only `get_stream()` uses it,
the other methods, e.g. `KvClient::get()`, still send the regular `Range` request and work with all
the etcd versions. There is no fallback: calling `get_stream()` on an etcd server older than 3.7
fails with an `Unimplemented` gRPC status.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
etcd-client = "0.21"
tokio = { version = "1.0", features = ["full"] }
```

To get started using `etcd-client`:

```rust
use etcd_client::{Client, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = Client::connect(["localhost:2379"], None).await?;
    // put kv
    client.put("foo", "bar", None).await?;
    // get kv
    let resp = client.get("foo", None).await?;
    if let Some(kv) = resp.kvs().first() {
        println!("Get kv: {{{}: {}}}", kv.key_str()?, kv.value_str()?);
    }

    Ok(())
}
```

### Upgrade from pre 0.17 to 0.18

Since 0.18, the `WatchClient` API has changed to support watch stream error handling precisely.

The `WatchClient` is not a high level watcher, it is just a watch API stub. So it should be only
responsible for sending requests and receiving responses. Let the high level watcher decide what
to do if received an unexpected response.

```diff
- WatchClient::watch(key: impl Into<Vec<u8>>, options: Option<WatchOptions>) -> Result<(WatchResponse, Watcher, WatchStream)>
+ WatchClient::watch(key: impl Into<Vec<u8>>, options: Option<WatchOptions>) -> Result<WatchStream>
```

The new `WatchStream` is different from the old version. It represents underlying bidirectional
watch stream (HTTP 2 stream). So it can be used to send requests and receive responses and events.

It's the user's responsibility to check the received response is a response or an event, if it is
created successfully or not, or if it is a cancel response.

See [watch.rs](./examples/watch.rs) for example.

## Examples

Examples can be found in [`examples`](./examples).

## Feature Flags

- `tls`: Enables the `rustls`-based TLS connection using the `ring` libcrypto provider.
  Alias for `tls-ring`. Not enabled by default.
- `tls-ring`: Enables the `rustls`-based TLS connection using the `ring` libcrypto provider.
  Not enabled by default.
- `tls-aws-lc`: Enables the `rustls`-based TLS connection using the `aws-lc-rs` libcrypto
  provider. Not enabled by default.
- `tls-roots`: Alias for `tls-native-roots`. Not enabled by default.
- `tls-native-roots`: Adds system trust roots to `rustls`-based TLS connection using the
  `rustls-native-certs` crate. Not enabled by default.
- `tls-webpki-roots`: Adds Mozilla's trust roots to `rustls`-based TLS connection using the
  `webpki-roots` crate. Not enabled by default.
- `pub-response-field`: Exposes structs used to create regular `etcd-client` responses including
  internal protobuf representations. Useful for mocking. Not enabled by default.
- `tls-openssl`: Enables the `openssl`-based TLS connections. This would make your binary
  dynamically link to `libssl`.
- `tls-openssl-vendored`: Like `tls-openssl`, however compile openssl from source code and
  statically link to it.
- `build-server`: Builds a server variant of the etcd protobuf and re-exports it under the same
  `proto` package as the `pub-response-field` feature does.
- `raw-channel`: Allows the caller to construct the underlying Tonic channel used by the client.

## Test

We test this library with etcd 3.5, 3.6 and 3.7, see the version matrix in
[`.github/workflows/ci.yml`](./.github/workflows/ci.yml).

Note that we use a fixed `etcd` server URI (`localhost:2379`) to connect to etcd server.

The [`Justfile`](./Justfile) and the [`docker-compose.yml`](./docker-compose.yml) start etcd of
the version you want, you need [just](https://github.com/casey/just) and docker compose.
`just` lists the recipes.

```bash
# Start a single etcd in the background (v3.7.2 by default), run the tests, and remove it
just etcd
cargo test
just etcd-down

# Or do all of that in one go
just test
```

The recipes take the etcd version, the tag of the `quay.io/coreos/etcd` image, as the first argument.
To run the tests on etcd 3.5 or 3.6:

```bash
# etcd 3.5
just test v3.5.26

# etcd 3.6, with the etcd left running afterwards
just etcd v3.6.15
cargo test
just etcd-down

# The cluster test works the same way
just test-cluster v3.5.26
```

The tests that need etcd 3.7 (`test_get_stream` and `test_get_stream_chunked`) are ignored by
default, so `cargo test` passes on etcd 3.5 and 3.6. `just test-stream` is only for etcd 3.7, on
etcd 3.5 and 3.6 it fails with `Unimplemented: unknown method RangeStream for service
etcdserverpb.KV`.

The tests enable and disable the authentication by themselves with the user `root` (password
`rootpwd`) and expect the token TTL of the server to be 2 seconds. `just etcd` takes care of both.

Some test cases in `tests/client.rs` will be ignored by default:

- `test_get_stream` Needs etcd 3.7+ to run
- `test_get_stream_chunked` Needs etcd 3.7+ and `--max-request-bytes=10240` to run
- `test_cluster` Needs a multi-node etcd cluster to run, the 4th node is started as soon as the test
  adds it as a member.

```bash
# Run etcd v3.7 RangeStream tests, on an etcd with the small max-request-bytes
just test-stream

# Run cluster test, on a cluster of 3 nodes
just test-cluster

# Run all the tests on every etcd version, like the CI does
just test-all
```

## Rust version requirements

The minimum supported version is 1.80. The current `etcd-client` version is not guaranteed to
build on Rust versions earlier than the minimum supported version.

## License

Dual-licensed to be compatible with the Rust project.

Licensed under the Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0 or the
MIT license http://opensource.org/licenses/MIT, at your option. This file may not be copied,
modified, or distributed except according to those terms.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `etcd-client` by you, shall be licensed as Apache-2.0 and MIT, without any
additional terms or conditions.
