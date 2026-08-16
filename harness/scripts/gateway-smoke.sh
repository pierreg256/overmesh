#!/usr/bin/env bash
set -euo pipefail

mkdir -p .harness

cargo run --quiet -p overmesh-harness -- doctor >/dev/null
token=$(cargo run --quiet -p overmesh-harness -- issue-token valid --principal caller)
cargo run --quiet -p overmesh-harness -- issue-token valid --principal gateway \
  >.harness/gateway-control-token.jwt

create_container() {
  local replica_port=$1
  local container=$2
  local authorization_header="Bearer $token"
  local status
  status=$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' -X PUT \
    -H "Authorization: $authorization_header" \
    -H 'x-ms-version: 2025-11-05' \
    -H "x-ms-date: $(LC_ALL=C date -u '+%a, %d %b %Y %H:%M:%S GMT')" \
    -H 'Content-Length: 0' \
    "https://127.0.0.1:$replica_port/devstoreaccount1/$container?restype=container")
  test "$status" = "201" || test "$status" = "409"
}

for replica_port in 12100 12101; do
  create_container "$replica_port" overmesh-system
  create_container "$replica_port" commit
done

if curl --silent --fail http://127.0.0.1:18080/healthz >/dev/null 2>&1; then
  echo "Port 18080 is already serving a gateway; refusing to reuse an unknown process." >&2
  exit 1
fi

cargo build --quiet -p overmesh-gateway
./target/debug/overmesh-gateway \
  --config gateway/config/local.yaml \
  >.harness/gateway-smoke.log 2>&1 &
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
  if curl --silent --fail http://127.0.0.1:18080/healthz >/dev/null; then
    break
  fi
  if ! kill -0 "$gateway_pid" >/dev/null 2>&1; then
    cat .harness/gateway-smoke.log >&2
    exit 1
  fi
  sleep 0.1
done

curl --silent --fail http://127.0.0.1:18080/healthz >/dev/null

test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  http://127.0.0.1:18080/container/blob)" = "404"

test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -H 'Authorization: SharedKey account:forbidden' \
  -H 'x-ms-version: 2025-11-05' \
  http://127.0.0.1:18080/container/blob)" = "403"

test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  'http://127.0.0.1:18080/container/blob?sv=2025-11-05&sp=r&sig=forbidden')" = "403"

make validate-system

smoke_run="${HARNESS_RUN_ID:-local}-$$"
blob_path="/commit/smoke-v050-$smoke_run"
write_id="commit-smoke-v050-$smoke_run"
payload='hello overmesh v0.5.0'

missing_write_id_status=$(curl --silent --output .harness/missing-write-id-error.xml \
  --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  --data-binary "$payload" \
  "http://127.0.0.1:18080/commit/missing-write-id-$smoke_run")
if [[ "$missing_write_id_status" != "400" ]]; then
  echo "Missing-write-ID probe returned $missing_write_id_status instead of 400." >&2
  cat .harness/missing-write-id-error.xml >&2
  exit 1
fi
grep -q 'stable request ID is required' .harness/missing-write-id-error.xml

test "$(curl --silent --output /dev/null --dump-header .harness/commit-headers.txt \
  --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-ms-client-request-id: $write_id" \
  --data-binary "$payload" \
  "http://127.0.0.1:18080$blob_path")" = "201"

test "$(curl --silent --output /dev/null --dump-header .harness/commit-retry-headers.txt \
  --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: $write_id" \
  --data-binary "$payload" \
  "http://127.0.0.1:18080$blob_path")" = "201"
grep -qi '^x-overmesh-idempotent-replay: true' .harness/commit-retry-headers.txt

test "$(curl --silent --output /dev/null --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: $write_id" \
  --data-binary 'different payload' \
  "http://127.0.0.1:18080$blob_path")" = "409"

test "$(curl --silent --output /dev/null --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H 'If-None-Match: *' \
  -H "x-overmesh-write-id: commit-smoke-v050-new-$smoke_run" \
  --data-binary "$payload" \
  "http://127.0.0.1:18080$blob_path")" = "412"

logical_etag=$(awk 'tolower($1) == "etag:" {sub(/\r$/, "", $2); print $2}' \
  .harness/commit-headers.txt)
test -n "$logical_etag"
test "$(curl --silent --output /dev/null --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H 'If-Match: "stale"' \
  -H "x-overmesh-write-id: commit-smoke-v050-stale-$smoke_run" \
  --data-binary 'updated payload' \
  "http://127.0.0.1:18080$blob_path")" = "412"
test "$(curl --silent --output /dev/null --dump-header .harness/commit-update-headers.txt \
  --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "If-Match: $logical_etag" \
  -H "x-overmesh-write-id: commit-smoke-v050-update-$smoke_run" \
  --data-binary 'updated payload' \
  "http://127.0.0.1:18080$blob_path")" = "201"
grep -qi '^x-overmesh-logical-version: 2' .harness/commit-update-headers.txt
updated_etag=$(awk 'tolower($1) == "etag:" {sub(/\r$/, "", $2); print $2}' \
  .harness/commit-update-headers.txt)

test "$(curl --silent --output /dev/null --dump-header .harness/head-headers.txt \
  --write-out '%{http_code}' --head \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "http://127.0.0.1:18080$blob_path")" = "200"
grep -qi '^content-length: 15' .harness/head-headers.txt
grep -qi "^etag: $updated_etag" .harness/head-headers.txt

curl --silent --fail \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "http://127.0.0.1:18080$blob_path" \
  -o .harness/read-full.bin
test "$(cat .harness/read-full.bin)" = "updated payload"

test "$(curl --silent --output .harness/read-range.bin \
  --dump-header .harness/read-range-headers.txt --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H 'Range: bytes=1-6' \
  "http://127.0.0.1:18080$blob_path")" = "206"
test "$(cat .harness/read-range.bin)" = "pdated"
grep -qi '^content-range: bytes 1-6/15' .harness/read-range-headers.txt

test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "If-None-Match: $updated_etag" \
  "http://127.0.0.1:18080$blob_path")" = "304"
test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H 'If-Match: "stale"' \
  "http://127.0.0.1:18080$blob_path")" = "412"

boundary_path="/commit/range-boundary-$smoke_run"
dd if=/dev/zero of=.harness/range-boundary.bin bs=1048576 count=4 2>/dev/null
printf 'ABCDEFGHIJKLMNOP' >>.harness/range-boundary.bin
test "$(curl --silent --output /dev/null --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: range-boundary-$smoke_run" \
  --data-binary @.harness/range-boundary.bin \
  "http://127.0.0.1:18080$boundary_path")" = "201"
test "$(curl --silent --output .harness/range-boundary-read.bin \
  --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H 'x-ms-range: bytes=4194300-4194307' \
  "http://127.0.0.1:18080$boundary_path")" = "206"
test "$(od -An -tx1 .harness/range-boundary-read.bin | tr -d ' \n')" = "0000000041424344"

path_hash=$(printf '/local-overmesh%s' "$blob_path" | shasum -a 256 | awk '{print $1}')
head_key="heads/$path_hash.json"
curl --insecure --fail --silent \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12100/devstoreaccount1/overmesh-system/$head_key" \
  -o .harness/commit-head-a.json
curl --insecure --fail --silent \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12101/devstoreaccount1/overmesh-system/$head_key" \
  -o .harness/commit-head-b.json
cmp .harness/commit-head-a.json .harness/commit-head-b.json
manifest_info=$(cargo run --quiet -p overmesh-harness -- \
  verify-commit-manifest .harness/commit-head-a.json)
block_manifest_key=$(printf '%s' "$manifest_info" | cut -f5)
content_container=$(printf '%s' "$manifest_info" | cut -f6)
content_key=$(printf '%s' "$manifest_info" | cut -f7)
test -n "$block_manifest_key"
test -n "$content_container"
test -n "$content_key"
grep -q '"objectId":"00000000-0000-0000-0000-000000000001"' \
  .harness/commit-head-a.json
curl --insecure --fail --silent \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12100/devstoreaccount1/overmesh-system/$block_manifest_key" \
  -o .harness/block-manifest-a.json
curl --insecure --fail --silent \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12101/devstoreaccount1/overmesh-system/$block_manifest_key" \
  -o .harness/block-manifest-b.json
cmp .harness/block-manifest-a.json .harness/block-manifest-b.json
cargo run --quiet -p overmesh-harness -- verify-commit-manifest \
  .harness/commit-head-a.json \
  --block-manifest .harness/block-manifest-a.json >/dev/null

authorization_header="Bearer $token"
curl --insecure --fail --silent \
  -H "Authorization: $authorization_header" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12100/devstoreaccount1/$content_container/$content_key" \
  -o .harness/content-a.bin
curl --insecure --fail --silent \
  -H "Authorization: $authorization_header" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12101/devstoreaccount1/$content_container/$content_key" \
  -o .harness/content-b.bin
cmp .harness/content-a.bin .harness/content-b.bin
test "$(cat .harness/content-a.bin)" = "updated payload"

for replica_port in 12100 12101; do
  test "$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' -X PUT \
    -H "Authorization: $authorization_header" \
    -H 'x-ms-version: 2025-11-05' \
    -H 'x-ms-blob-type: BlockBlob' \
    --data-binary 'tampered data!!' \
    "https://127.0.0.1:$replica_port/devstoreaccount1/$content_container/$content_key")" = "201"
done
test "$(curl --silent --output /dev/null --write-out '%{http_code}' --head \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "http://127.0.0.1:18080$blob_path")" = "200"
if curl --silent --show-error --fail \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "http://127.0.0.1:18080$blob_path" \
  -o .harness/tampered-read.bin; then
  echo "GET unexpectedly returned content from a corrupted block." >&2
  exit 1
fi

high_water_key="high-water/$path_hash/current.json"
curl --insecure --fail --silent \
  -H "Authorization: $authorization_header" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12100/devstoreaccount1/overmesh-system/$high_water_key" \
  -o .harness/high-water-a.json
curl --insecure --fail --silent \
  -H "Authorization: $authorization_header" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12101/devstoreaccount1/overmesh-system/$high_water_key" \
  -o .harness/high-water-b.json
cmp .harness/high-water-a.json .harness/high-water-b.json

cargo run --quiet -p overmesh-harness -- fault disable b >/dev/null
test "$(curl --silent --output /dev/null --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: commit-outage-v050-$smoke_run" \
  --data-binary 'must not succeed' \
  "http://127.0.0.1:18080/commit/replica-outage-$smoke_run")" = "503"
cargo run --quiet -p overmesh-harness -- fault reset >/dev/null
