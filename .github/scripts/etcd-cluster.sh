#!/usr/bin/env bash
# Start a 3-node etcd cluster for the cluster test.
# Usage: etcd-cluster.sh up|join <etcd version, e.g. v3.6.15>
#   up:   start node1..node3 (node1 serves the tests on localhost:2379)
#   join: wait until node4 is added as a member by the test, then start it
set -euo pipefail

image=quay.io/coreos/etcd:$2
base=node1=http://node1:2380,node2=http://node2:2380,node3=http://node3:2380

# start_node <index> <initial-cluster> <initial-cluster-state> [docker args]
start_node() {
  local i=$1 cluster=$2 state=$3
  shift 3
  docker run -d --name "node$i" --network etcd-ci "$@" \
    --env ETCD_NAME="node$i" \
    --env ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379 \
    --env ETCD_ADVERTISE_CLIENT_URLS="http://node$i:2379" \
    --env ETCD_LISTEN_PEER_URLS=http://0.0.0.0:2380 \
    --env ETCD_INITIAL_ADVERTISE_PEER_URLS="http://node$i:2380" \
    --env ETCD_INITIAL_CLUSTER="$cluster" \
    --env ETCD_INITIAL_CLUSTER_STATE="$state" \
    --env ETCD_INITIAL_CLUSTER_TOKEN=etcd-client-ci \
    "$image"
}

case $1 in
  up)
    docker network create etcd-ci
    start_node 1 "$base" new --publish 2379:2379
    start_node 2 "$base" new
    start_node 3 "$base" new
    until docker exec node1 etcdctl endpoint health --cluster; do sleep 1; done
    # member add right after startup is rejected with "unhealthy cluster" until peers settle
    sleep 10
    docker exec node1 etcdctl member list -w table
    ;;
  join)
    # The member is registered with its peer url before it is started.
    for _ in $(seq 120); do
      if docker exec node1 etcdctl member list | grep -q http://node4:2380; then
        start_node 4 "$base,node4=http://node4:2380" existing
        exit 0
      fi
      sleep 1
    done
    echo "node4 was never added" >&2
    exit 1
    ;;
esac
