# etcd-client

[![Minimum rustc version](https://img.shields.io/badge/rustc-1.80+-lightgray.svg)](https://github.com/etcdv3/etcd-client#rust-version-requirements)
[![Crate](https://img.shields.io/crates/v/etcd-client.svg)](https://crates.io/crates/etcd-client)
[![API](https://docs.rs/etcd-client/badge.svg)](https://docs.rs/etcd-client)

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
`KvClient::get_stream()` method without any feature flags since `v0.21.0`, but it will not be used if the etcd
server version is less than 3.7.

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

We test this library with etcd 3.5.

Note that we use a fixed `etcd` server URI (`localhost:2379`) to connect to etcd server.

Some test cases in `tests/client.rs` will be ignored by default:

- `test_get_stream` Needs etcd 3.7+ to run
- `test_get_stream_chunked` Needs etcd 3.7+ to run
- `test_cluster` Needs a pre-configured etcd cluster to run. You can use `docker-compose` to start a 3-node etcd cluster for testing.

```bash
# Run etcd v3.7 RangeStream test
cargo test --test client test_get_stream -- --ignored

# Run cluster test, need a pre-configured etcd cluster
cargo test --test client test_cluster -- --ignored
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
