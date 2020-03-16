# etcdv3

[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An [etcd](https://github.com/etcd-io/etcd) v3 API client for Rust.
It provides asynchronous client backed by [tokio](https://github.com/tokio-rs/tokio) and [tonic](https://github.com/hyperium/tonic).

## Features

- etcd API v3
- asynchronous

## Supported APIs

- [x] KV
- [x] Watch
- [ ] Lease
- [ ] Cluster
- [ ] Maintenance
- [ ] Auth
- [ ] Election
- [ ] Lock

## Test

We test this library using etcd 3.4.

Notes that we use a fixed `etcd` server URI (localhost:2379) to connect to etcd server.

## Rust Version

`etcdv3` works on rust `1.39` and above as it requires support for the `async_await`
feature.

## License

This project is licensed under the MIT license ([LICENSE](LICENSE) or http://opensource.org/licenses/MIT).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `etcdv3` by you, shall be licensed as MIT, without any additional
terms or conditions.
