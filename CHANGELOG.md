# Changelog

The release notes of [etcd-client](https://github.com/etcdv3/etcd-client/releases), newest first.

The notes up to v0.21.0 are copied from the GitHub releases as they were published. From the next release on,
the changes are grouped as `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed` and `Security`,
see [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Fixed

- Refresh an expired authentication token for the calls of `AuthClient` too when automatic token refresh is enabled, except `authenticate`, by [@belltoy](https://github.com/belltoy) in [#126](https://github.com/etcdv3/etcd-client/pull/126)
- Send the authentication token to `Cluster.MemberList`. etcd 3.6 and later require it when authentication is enabled and refuse the request with `user name is empty` otherwise (observed on 3.6.15 and 3.7.2, while 3.5.26 does not require it), by [@belltoy](https://github.com/belltoy) in [#127](https://github.com/etcdv3/etcd-client/pull/127)
- Refresh an expired token once for concurrent calls instead of authenticating once per call, by [@belltoy](https://github.com/belltoy) in [#128](https://github.com/etcdv3/etcd-client/pull/128)

## v0.21.0 - 2026-10-09

### What's Changed

#### Features

- Add etcd 3.7 streaming range reads through `KvClient::get_stream()` and `GetStreamResponse`.
- Add `MaintenanceClient::downgrade()` to validate, enable, or cancel cluster downgrades.
- Expose etcd 3.6 response fields for storage version, database quota, downgrade information, hash revision, and snapshot version.
- Update bundled protobuf definitions to etcd v3.7.2.

#### Bug fixes

- Fix automatic token refresh for `Lock` and `Election` requests when etcd returns Unknown with an invalid authentication token error by [@askalt](https://github.com/askalt) in [#124](https://github.com/etcdv3/etcd-client/pull/124).

#### Maintenance

- Add etcd 3.7 CI coverage and streaming range tests.
- Refresh API documentation and improve test cleanup.

**Full Changelog**: https://github.com/etcdv3/etcd-client/compare/v0.20.0...v0.21.0

## v0.20.0 - 2026-08-27

### What's Changed

* Add system and Mozilla’s trust roots, by [@messense](https://github.com/messense) in [#116](https://github.com/etcdv3/etcd-client/pull/116)
* Add `aws-lc-rs` feature support, by [@mrthinger](https://github.com/mrthinger)  in [#120](https://github.com/etcdv3/etcd-client/pull/120)
* Add optional automatic token refresh, by [@askalt](https://github.com/askalt) in [#121](https://github.com/etcdv3/etcd-client/pull/121)
* Add auhorization token for lease methods, by [@Hakuyume](https://github.com/Hakuyume) in [#122](https://github.com/etcdv3/etcd-client/pull/122)

**Full Changelog**: https://github.com/etcdv3/etcd-client/compare/v0.19.0...v0.20.0

## v0.19.0 - 2026-06-09

### What's Changed

- Expose `watch_request::RequestUnion` as `PbWatchRequestUnion` by [@srporwal12](https://github.com/srporwal12) in [#117](https://github.com/etcdv3/etcd-client/pull/117)

### Breaking Changes

Fixed the `WatchStream::split` method introduced in v0.18.0, by [@belltoy](https://github.com/belltoy) in [#119](https://github.com/etcdv3/etcd-client/pull/119):

```diff
- WatchStream::split(self) -> (Sender<WatchRequest>, Streaming<PbWatchResponse>)
+ WatchStream::split(self) -> (WatchRequestSender, WatchResponseStream)
```

Read discussion details in [#118](https://github.com/etcdv3/etcd-client/issues/118).

**Full Changelog**: https://github.com/etcdv3/etcd-client/compare/v0.18.0...v0.19.0

## v0.18.0 - 2026-01-18

### What's Changed
* Simplify authentication (with interceptor) by [@belltoy](https://github.com/belltoy) in [#106](https://github.com/etcdv3/etcd-client/pull/106)
* Re-design watch stream API by [@belltoy](https://github.com/belltoy) in [#108](https://github.com/etcdv3/etcd-client/pull/108)
* Expose more protobuf structs to make transactions testable and mockable by [@g-murzik](https://github.com/g-murzik) in [#25](https://github.com/etcdv3/etcd-client/pull/25)
* fix: return error instead of panic when connect_with_balanced_channel by [@belltoy](https://github.com/belltoy) in [#78](https://github.com/etcdv3/etcd-client/issues/78)

### Breaking Changes

#### Upgrade from pre 0.17 to 0.18

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

**Full Changelog**: https://github.com/etcdv3/etcd-client/compare/v0.17.0...v0.18.0

## v0.17.0 - 2025-09-20

### What's Changed

* tonic: upgrade to tonic 0.14 (#112)

**Full Changelog**: https://github.com/etcdv3/etcd-client/compare/v0.16.1...v0.17.0

## v0.16.1 - 2025-07-15

### What's Changed

* Fix `Client` (`Channel`) `Sync` (#110)

**Full Changelog**: https://github.com/etcdv3/etcd-client/compare/v0.16.0...v0.16.1

## v0.16.0 - 2025-07-13

### What's Changed

* Update tonic to 0.13.1 and tower to 0.5
* Set large-error-threshold to 256 (#105)
* feat: allow caller to initialize the channel (#93)
* feat: make channel creation more flexible (#98)

**Full Changelog**: https://github.com/etcdv3/etcd-client/compare/v0.15.0...v0.16.0

## v0.15.0 - 2025-04-11

### What's Changed
* Use `{RwLock/Mutex}::clear_poison()` to remove `{RwLock/Mutex}::unwrap()` by [@davidli2010](https://github.com/davidli2010) in [#94](https://github.com/etcdv3/etcd-client/pull/94)
* fix: change MSRV badge to 1.80 by [@nmooscisco](https://github.com/nmooscisco) in [#96](https://github.com/etcdv3/etcd-client/pull/96)
* fix: make doctests in openssl_tls::transport compile by [@nmooscisco](https://github.com/nmooscisco) in [#97](https://github.com/etcdv3/etcd-client/pull/97)
* feat: add support for requiring a leader by [@nmooscisco](https://github.com/nmooscisco) in [#95](https://github.com/etcdv3/etcd-client/pull/95)
* feat(ci): run cargo clippy with all feature combinations by [@nmooscisco](https://github.com/nmooscisco) in [#99](https://github.com/etcdv3/etcd-client/pull/99)
* feat: add the ability to build a server by [@nmooscisco](https://github.com/nmooscisco) in [#101](https://github.com/etcdv3/etcd-client/pull/101)

### New Contributors
* [@nmooscisco](https://github.com/nmooscisco) made their first contribution in [#96](https://github.com/etcdv3/etcd-client/pull/96)

**Full Changelog**: https://github.com/etcdv3/etcd-client/compare/v0.14.1...v0.15.0

## v0.14.1 - 2025-01-28

Happy Chinese New Year! 🎉

* Enhance the example which showcases the snapshot maintenance operation. by [@renormalize](https://github.com/renormalize) in [#83](https://github.com/etcdv3/etcd-client/pull/83)
* lease keep alive return lease not found error by [@messense](https://github.com/messense) in [#87](https://github.com/etcdv3/etcd-client/pull/87)

## v0.14.0 - 2024-07-10

- Upgrade tonic to 0.12
- Upgrade prost to 0.13
- Upgrade http to 1.1

## v0.13.0 - 2024-06-01

- Support manually in-place authentication token update (#65 )
- Upgrade tonic to 0.11
- Bump tokio to 1.38

## v0.12.4 - 2024-01-01

- Implement namespace APIs (#71)
- Support tcp keepalive (#73)

## v0.12.3 - 2023-11-21

- Add GetResponse::take_kvs() & KeyValue::into_key_value()(#70)

## v0.12.2 - 2023-11-20

- [Implement take_prev_kvs for DeleteResponse](https://github.com/etcdv3/etcd-client/commit/6291b79268eaf559e90d0313ac669ea0d0e1e595)
- [tonic options max_decoding_message_size/max_encoding_message_size](https://github.com/etcdv3/etcd-client/commit/290dff8471725f46f75842af48687c5b87ac1f99)

## v0.12.1 - 2023-09-22

- Export `AuthenticateResponse` (#64)

## v0.12.0 - 2023-09-15

- Upgrade to tonic 0.10.0
- Bump tokio to 1.32
- Disable tower's default features
- Licensed under the Apache License or the MIT license

## v0.11.1 - 2023-05-03

- Upgrade to Rust 2021 edition

## v0.11.0 - 2023-04-30

- Upgrade tonic to 0.9.x
- Upgrade other dependencies

## v0.10.4 - 2023-03-29

- Fix doc.rs build

## v0.10.3 - 2023-02-24

- Add openssl support

## v0.10.2 - 2022-10-05

- Add `ConnectOptions::with_connect_timeout()` to apply a timeout to connecting to the endpoint

## v0.10.1 - 2022-08-11

- Expose the `keep_alive_while_idle` config

## v0.10.0 - 2022-08-02

- Upgrade tokio to 1.20.1
- Upgrade tonic to 0.8.0
- Upgrade prost to 0.11.0

## v0.9.2 - 2022-05-20

- Upgrade tokio to 1.18

## v0.9.1 - 2022-04-27

- Watcher can watch more keys

## v0.9.0 - 2022-04-05

- Upgrade tonic to 0.7.1 and prost to 0.10.0
- Dynamically add/remove endpoint

## v0.8.4 - 2022-03-04

- Add timeout to ConnectOptions
- Upgrade tokio to 1.17.0

## 0.8.3 - 2022-01-09

- Connect lazy for single endpoint
- Upgrade tokio to 1.15.0

## 0.8.2 - 2021-12-12

- Fix: allow KeyValue be exported by feature `pub-response-field`
- Upgrade tonic to 0.6.2

## 0.8.1 - 2021-11-27

- Update README and documents

## 0.8.0 - 2021-11-27

- Upgrade tonic to 0.6 and prost to 0.9

## 0.7.2 - 2021-09-04

- Support http2 keep alive
- Upgrade tokio to 1.11.0

## 0.7.1 - 2021-07-19

- Add `pub-response-field` feature to turn *Response related struct fields public and expose internal protobuf response representations.

## 0.7.0 - 2021-07-11

- Upgrade to tonic 0.5

## 0.6.5 - 2021-07-10

- Enhance DeleteOptions
- Upgrade tokio to 1.8.1

## 0.6.4 - 2021-06-11

Upgrade tokio to 1.6.1

## 0.6.3 - 2021-05-03

- Upgrade dependencies
- Update docs

## 0.6.2 - 2021-03-26

Get separate operation clients from Client.

## 0.6.1 - 2021-02-09

1. Upgrade tokio to 1.2.0
2. Fix typo

## 0.6.0 - 2021-01-17

Upgrade to Tokoio 1.0.

## 0.5.5 - 2021-01-17

Support `TLS` connection.

## 0.5.4 - 2020-10-23

Upgrade tonic to 0.3

## 0.5.3 - 2020-06-26

Support Clone operation on the client.

## 0.5.2 - 2020-06-15

Disable the use of rustfmt to format the output code.

## 0.5.1 - 2020-06-15

Move auto-generated files to OUT_DIR.

## 0.5.0 - 2020-06-14

Add election APIs.

## 0.4.0 - 2020-05-24

Add cluster APIs.

## 0.3.0 - 2020-05-21

Add maintanence and user APIs.

## 0.2.1 - 2020-04-16

Support role operations.

## 0.2.0 - 2020-04-04

- Support Lock APIs.
- Support user authentication.

## 0.1.1 - 2020-03-28

Supports Lease APIs.

## 0.1.0 - 2020-03-21

Basic KV and watch operations.
