#!/usr/bin/env bash
set -euo pipefail

mkdir -p .harness
cargo run --quiet -p overmesh-harness -- doctor >/dev/null
token=$(cargo run --quiet -p overmesh-harness -- issue-token valid --principal caller)
cargo run --quiet -p overmesh-harness -- issue-token valid --principal gateway \
  >.harness/gateway-control-token.jwt
caller_authorization="Bearer ${token}"
control_authorization="Bearer $(cat .harness/gateway-control-token.jwt)"

create_container() {
  local port=$1
  local container=$2
  local status
  status=$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' -X PUT \
    -H "Authorization: ${caller_authorization}" \
    -H 'x-ms-version: 2025-11-05' \
    -H "x-ms-date: $(LC_ALL=C date -u '+%a, %d %b %Y %H:%M:%S GMT')" \
    -H 'Content-Length: 0' \
    "https://127.0.0.1:$port/devstoreaccount1/$container?restype=container")
  test "$status" = "201" || test "$status" = "409"
}

for port in 12100 12101 12102; do
  create_container "$port" overmesh-system
  create_container "$port" placement
done

if curl --silent --fail http://127.0.0.1:18081/healthz >/dev/null 2>&1; then
  echo "Port 18081 is already serving a gateway; refusing to reuse an unknown process." >&2
  exit 1
fi

cargo build --quiet -p overmesh-gateway
./target/debug/overmesh-gateway \
  --config gateway/config/local-three-node.yaml \
  >.harness/placement-gateway.log 2>&1 &
gateway_pid=$!

cleanup() {
  cargo run --quiet -p overmesh-harness -- fault reset >/dev/null 2>&1 || true
  if kill -0 "$gateway_pid" >/dev/null 2>&1; then
    kill -INT "$gateway_pid"
    wait "$gateway_pid" || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  if curl --silent --fail http://127.0.0.1:18081/healthz >/dev/null; then
    break
  fi
  if ! kill -0 "$gateway_pid" >/dev/null 2>&1; then
    cat .harness/placement-gateway.log >&2
    exit 1
  fi
  sleep 0.1
done
curl --silent --fail http://127.0.0.1:18081/healthz >/dev/null

find_path() {
  cargo run --quiet -p overmesh-harness -- find-placement "$1" "$2"
}

put_blob() {
  local path=$1
  local write_id=$2
  local expected=$3
  local status
  status=$(curl --silent --max-time 10 --output /dev/null --write-out '%{http_code}' -X PUT \
    -H "Authorization: ${caller_authorization}" \
    -H 'x-ms-version: 2025-11-05' \
    -H "x-overmesh-write-id: $write_id" \
    --data-binary "$write_id" \
    "http://127.0.0.1:18081$path" || true)
  test "$status" = "$expected"
}

node_port() {
  case "$1" in
    storage-a) echo 12100 ;;
    storage-b) echo 12101 ;;
    storage-c) echo 12102 ;;
    *) return 1 ;;
  esac
}

assert_head_placement() {
  local path=$1
  local first=$2
  local second=$3
  local path_hash
  path_hash=$(printf '/local-overmesh%s' "$path" | shasum -a 256 | awk '{print $1}')
  for node in storage-a storage-b storage-c; do
    local expected=404
    if [[ "$node" = "$first" || "$node" = "$second" ]]; then
      expected=200
    fi
    local status
    status=$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' \
      -H "Authorization: ${control_authorization}" \
      -H 'x-ms-version: 2025-11-05' \
      "https://127.0.0.1:$(node_port "$node")/devstoreaccount1/overmesh-system/heads/$path_hash.json")
    test "$status" = "$expected"
  done
}

path_ab=$(find_path storage-a storage-b)
path_ac=$(find_path storage-a storage-c)
path_bc=$(find_path storage-b storage-c)
test "$path_ab" != "$path_ac"
test "$path_ab" != "$path_bc"
test "$path_ac" != "$path_bc"

put_blob "$path_ab" "placement-ab-initial" 201
put_blob "$path_ac" "placement-ac-initial" 201
put_blob "$path_bc" "placement-bc-initial" 201
assert_head_placement "$path_ab" storage-a storage-b
assert_head_placement "$path_ac" storage-a storage-c
assert_head_placement "$path_bc" storage-b storage-c

cargo run --quiet -p overmesh-harness -- fault disable a >/dev/null
put_blob "$path_ab" "placement-ab-storage-a-down" 503
put_blob "$path_ac" "placement-ac-storage-a-down" 503
put_blob "$path_bc" "placement-bc-storage-a-down" 201
cargo run --quiet -p overmesh-harness -- fault enable a >/dev/null

echo "three-node placement and single-node write isolation passed"
