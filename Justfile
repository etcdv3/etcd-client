# etcd of the version you want for the development and the tests, it needs docker compose.

versions := "v3.5.26 v3.6.15 v3.7.2"

# List all tasks
default:
    @just --list

# Start a single etcd in the background. The user root/rootpwd exists, the tests enable the auth by themselves
etcd version="v3.7.2" max_request_bytes="1572864":
    #!/usr/bin/env bash
    set -euo pipefail
    export ETCD_VERSION={{version}} ETCD_MAX_REQUEST_BYTES={{max_request_bytes}}
    docker compose --profile single up -d --remove-orphans
    for _ in $(seq 60); do
        docker compose exec -T etcd etcdctl endpoint health >/dev/null 2>&1 && break
        sleep 1
    done
    docker compose exec -T etcd etcdctl endpoint health >/dev/null || { docker compose logs --tail 20 etcd >&2; exit 1; }
    docker compose exec -T etcd etcdctl user add root --new-user-password=rootpwd
    docker compose exec -T etcd etcdctl user grant-role root root
    echo "etcd {{version}} is up on localhost:2379"

# Start a cluster of 3 etcd nodes in the background, node1 is on localhost:2379
etcd-cluster version="v3.7.2":
    #!/usr/bin/env bash
    set -euo pipefail
    export ETCD_VERSION={{version}}
    docker compose --profile cluster up -d --remove-orphans
    for _ in $(seq 60); do
        docker compose exec -T node1 etcdctl endpoint health --cluster >/dev/null 2>&1 && break
        sleep 1
    done
    docker compose exec -T node1 etcdctl endpoint health --cluster >/dev/null || { docker compose logs --tail 20 >&2; exit 1; }
    # member add right after startup is rejected with "unhealthy cluster" until the peers settle
    sleep 10
    echo "etcd {{version}} cluster is up, node1 is on localhost:2379"

# Wait until `test_cluster` adds node4 as a member, then start it
etcd-join version="v3.7.2":
    #!/usr/bin/env bash
    set -euo pipefail
    export ETCD_VERSION={{version}}
    for _ in $(seq 120); do
        if docker compose exec -T node1 etcdctl member list | grep -q http://node4:2380; then
            # The member is registered with its peer url before it is started.
            docker compose --profile join up -d node4
            exit 0
        fi
        sleep 1
    done
    echo "node4 was never added" >&2
    exit 1

# Stop and remove the etcd containers, their data is removed with them
etcd-down:
    @docker compose --profile single --profile cluster --profile join down -v --remove-orphans

# Run the tests on a single etcd of the version
test version="v3.7.2":
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just etcd-down' EXIT
    just etcd {{version}}
    cargo test

# Run the etcd 3.7 RangeStream tests, the server splits the responses for the small max-request-bytes
test-stream version="v3.7.2":
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just etcd-down' EXIT
    just etcd {{version}} 10240
    cargo test --test client test_get_stream -- --ignored

# Run the cluster test on a cluster of the version
test-cluster version="v3.7.2":
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just etcd-down' EXIT
    just etcd-cluster {{version}}
    just etcd-join {{version}} &
    cargo test --test client test_cluster -- --ignored
    wait

# Run all the tests on every etcd version, like the CI does
test-all:
    #!/usr/bin/env bash
    set -uo pipefail
    status=0
    for v in {{versions}}; do
        echo "=== etcd $v ==="
        just test $v || status=1
        just test-cluster $v || status=1
    done
    echo "=== etcd v3.7.2, small max-request-bytes ==="
    just test-stream v3.7.2 || status=1
    exit $status
