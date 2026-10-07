#!/bin/sh

set -e

ETCD_ROOT=${ETCD_ROOT:-""}
BASE_DIR=$(dirname "$0")
REPO_ROOT=$(cd "$BASE_DIR/.." && pwd)
PROTO_DIR="$REPO_ROOT/proto"

ETCD_PROTO_AUTH=etcd/api/authpb/auth.proto
ETCD_PROTO_RPC=etcd/api/etcdserverpb/rpc.proto
ETCD_PROTO_KV=etcd/api/mvccpb/kv.proto
ETCD_PROTO_ELECTION=etcd/server/etcdserver/api/v3election/v3electionpb/v3election.proto
ETCD_PROTO_LOCK=etcd/server/etcdserver/api/v3lock/v3lockpb/v3lock.proto

if [ -z "$ETCD_ROOT" ]; then
    echo "ETCD_ROOT is not set. Please set it to the root directory of your etcd repository."
    exit 1
fi

if [ ! -d "$ETCD_ROOT" ]; then
    echo "ETCD_ROOT ($ETCD_ROOT) is not a valid directory. Please check your ETCD_ROOT."
    exit 1
fi

# Check if the proto files exist
_ETC_ROOT_TRIMMED=$(echo "$ETCD_ROOT" | sed 's:/*$::') # Remove trailing slashes
for proto in "$ETCD_PROTO_AUTH" "$ETCD_PROTO_RPC" "$ETCD_PROTO_KV" "$ETCD_PROTO_ELECTION" "$ETCD_PROTO_LOCK"; do
    if [ ! -f "${_ETC_ROOT_TRIMMED}/${proto}" ]; then
        echo "Proto file ${proto} does not exist in ${_ETC_ROOT_TRIMMED}. Please check your ETCD_ROOT."
        exit 1
    fi
done

# Copy the proto files to the proto directory
mkdir -p "$PROTO_DIR"
for proto in "$ETCD_PROTO_AUTH" "$ETCD_PROTO_RPC" "$ETCD_PROTO_KV" "$ETCD_PROTO_ELECTION" "$ETCD_PROTO_LOCK"; do
    dirname=$(dirname "$proto")
    mkdir -p "$PROTO_DIR/$dirname"
    cp "${_ETC_ROOT_TRIMMED}/${proto}" "$PROTO_DIR/$proto"
done
