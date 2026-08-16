#!/usr/bin/env bash
set -euo pipefail

required=(
  OVERMESH_LIVE_RECONCILER_CONFIG
  OVERMESH_LIVE_ACCOUNT_A_BLOB_ENDPOINT
  OVERMESH_LIVE_ACCOUNT_B_BLOB_ENDPOINT
  OVERMESH_LIVE_ACCOUNT_A_RESOURCE_ID
  OVERMESH_LIVE_ACCOUNT_B_RESOURCE_ID
  OVERMESH_LIVE_CUSTOMER_CONTAINER
  OVERMESH_LIVE_ALLOWED_TOKEN
  OVERMESH_LIVE_DENIED_TOKEN
  OVERMESH_LIVE_ARM_TOKEN
)

for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required for the live Azure Storage authorization gate." >&2
    exit 2
  fi
done

storage_versions=${OVERMESH_LIVE_STORAGE_API_VERSIONS:-2025-11-05}
minimum_retention_days=${OVERMESH_LIVE_MINIMUM_RETENTION_DAYS:-7}
run_id="$$-$(date +%s)"
account_a_json=".harness/live-storage-account-a-$run_id.json"
account_b_json=".harness/live-storage-account-b-$run_id.json"
blob_a_json=".harness/live-storage-blob-a-$run_id.json"
blob_b_json=".harness/live-storage-blob-b-$run_id.json"
probe_body=".harness/live-storage-probe-$run_id.xml"

cleanup() {
  rm -f \
    "$account_a_json" \
    "$account_b_json" \
    "$blob_a_json" \
    "$blob_b_json" \
    "$probe_body"
}
trap cleanup EXIT

mkdir -p .harness

arm_get() {
  local resource_id=$1
  local api_version=$2
  local output=$3
  curl --fail --silent --show-error \
    -H "Authorization: Bearer $OVERMESH_LIVE_ARM_TOKEN" \
    "https://management.azure.com${resource_id}?api-version=${api_version}" \
    -o "$output"
}

arm_get "$OVERMESH_LIVE_ACCOUNT_A_RESOURCE_ID" 2023-05-01 "$account_a_json"
arm_get "$OVERMESH_LIVE_ACCOUNT_B_RESOURCE_ID" 2023-05-01 "$account_b_json"
arm_get \
  "$OVERMESH_LIVE_ACCOUNT_A_RESOURCE_ID/blobServices/default" \
  2023-05-01 \
  "$blob_a_json"
arm_get \
  "$OVERMESH_LIVE_ACCOUNT_B_RESOURCE_ID/blobServices/default" \
  2023-05-01 \
  "$blob_b_json"

python3 - \
  "$minimum_retention_days" \
  "$account_a_json" "$account_b_json" "$blob_a_json" "$blob_b_json" <<'PY'
import json
import sys

minimum_retention = int(sys.argv[1])
account_paths = sys.argv[2:4]
blob_paths = sys.argv[4:6]

for path in account_paths:
    with open(path, encoding="utf-8") as handle:
        properties = json.load(handle).get("properties", {})
    if properties.get("allowSharedKeyAccess") is not False:
        raise SystemExit(f"{path}: allowSharedKeyAccess must be false")
    if properties.get("publicNetworkAccess") != "Disabled":
        raise SystemExit(f"{path}: publicNetworkAccess must be Disabled")
    connections = properties.get("privateEndpointConnections", [])
    if not any(
        connection.get("properties", {})
        .get("privateLinkServiceConnectionState", {})
        .get("status") == "Approved"
        for connection in connections
    ):
        raise SystemExit(f"{path}: no approved private endpoint connection")

for path in blob_paths:
    with open(path, encoding="utf-8") as handle:
        properties = json.load(handle).get("properties", {})
    if properties.get("isVersioningEnabled") is not True:
        raise SystemExit(f"{path}: blob versioning must be enabled")
    retention = properties.get("deleteRetentionPolicy", {})
    if retention.get("enabled") is not True:
        raise SystemExit(f"{path}: blob soft delete must be enabled")
    if int(retention.get("days", 0)) < minimum_retention:
        raise SystemExit(
            f"{path}: blob soft-delete retention is below {minimum_retention} days"
        )
PY

if [[ -n "${OVERMESH_LIVE_RECONCILER_BIN:-}" ]]; then
  "$OVERMESH_LIVE_RECONCILER_BIN" \
    --config "$OVERMESH_LIVE_RECONCILER_CONFIG" \
    validate-runtime >/dev/null
else
  cargo run --quiet -p overmesh-reconciler -- \
    --config "$OVERMESH_LIVE_RECONCILER_CONFIG" \
    validate-runtime >/dev/null
fi

probe_conditional_put() {
  local endpoint=$1
  local allowed_token=$2
  local denied_token=$3
  local version=$4
  local probe_path=".overmesh-authorization-canary/object/$run_id-$version"
  local probe_url="${endpoint%/}/$OVERMESH_LIVE_CUSTOMER_CONTAINER/$probe_path"
  local status

  cleanup_conditional_put_canary() {
    local cleanup_status
    cleanup_status=$(curl --silent --show-error --output "$probe_body" --write-out '%{http_code}' \
      -X DELETE \
      --oauth2-bearer "$allowed_token" \
      -H "x-ms-version: $version" \
      -H "x-ms-date: $(LC_ALL=C date -u '+%a, %d %b %Y %H:%M:%S GMT')" \
      "$probe_url")
    if [[ "$cleanup_status" != "202" && "$cleanup_status" != "404" ]]; then
      echo "Conditional PUT canary cleanup returned $cleanup_status for $endpoint using $version." >&2
      cat "$probe_body" >&2
      return 1
    fi
  }

  status=$(printf '\0' | curl --silent --show-error --output "$probe_body" --write-out '%{http_code}' \
    -X PUT \
    --oauth2-bearer "$allowed_token" \
    -H "x-ms-version: $version" \
    -H "x-ms-date: $(LC_ALL=C date -u '+%a, %d %b %Y %H:%M:%S GMT')" \
    -H 'x-ms-blob-type: BlockBlob' \
    -H 'If-None-Match: *' \
    -H 'Content-Length: 1' \
    --data-binary @- \
    "$probe_url")
  if [[ "$status" != "201" ]]; then
    echo "Allowed conditional PUT probe returned $status instead of 201 for $endpoint using $version." >&2
    cat "$probe_body" >&2
    exit 1
  fi

  status=$(printf '\0' | curl --silent --show-error --output "$probe_body" --write-out '%{http_code}' \
    -X PUT \
    --oauth2-bearer "$denied_token" \
    -H "x-ms-version: $version" \
    -H "x-ms-date: $(LC_ALL=C date -u '+%a, %d %b %Y %H:%M:%S GMT')" \
    -H 'x-ms-blob-type: BlockBlob' \
    -H 'If-None-Match: *' \
    -H 'Content-Length: 1' \
    --data-binary @- \
    "$probe_url")
  if [[ "$status" != "403" ]]; then
    echo "Denied conditional PUT probe returned $status instead of 403 for $endpoint using $version." >&2
    cat "$probe_body" >&2
    cleanup_conditional_put_canary || true
    exit 1
  fi

  status=$(printf '\0' | curl --silent --show-error --output "$probe_body" --write-out '%{http_code}' \
    -X PUT \
    --oauth2-bearer "$allowed_token" \
    -H "x-ms-version: $version" \
    -H "x-ms-date: $(LC_ALL=C date -u '+%a, %d %b %Y %H:%M:%S GMT')" \
    -H 'x-ms-blob-type: BlockBlob' \
    -H 'If-None-Match: *' \
    -H 'Content-Length: 1' \
    --data-binary @- \
    "$probe_url")
  if [[ "$status" != "409" && "$status" != "412" ]]; then
    echo "Allowed conditional PUT retry returned $status instead of 409/412 for $endpoint using $version." >&2
    cat "$probe_body" >&2
    cleanup_conditional_put_canary || true
    exit 1
  fi

  cleanup_conditional_put_canary

  echo "Conditional PUT probe returned expected 201/403/$status and cleanup 202 for $endpoint using $version."
}

probe_absent_blob() {
  local method=$1
  local capability=$2
  local endpoint=$3
  local token=$4
  local version=$5
  local expected=$6
  local identity=$7
  local probe_path=".overmesh-authorization-canary/object/$run_id-$version"
  local status
  local curl_method=(-X "$method")
  local query_suffix=
  if [[ "$method" == "HEAD" ]]; then
    curl_method=(--head)
  elif [[ "$method" == "DELETE" ]]; then
    query_suffix='?snapshot=2000-01-01T00%3A00%3A00.0000000Z'
  fi
  status=$(curl --silent --show-error --output "$probe_body" --write-out '%{http_code}' \
    "${curl_method[@]}" \
    -H "Authorization: Bearer $token" \
    -H "x-ms-version: $version" \
    -H "x-ms-date: $(LC_ALL=C date -u '+%a, %d %b %Y %H:%M:%S GMT')" \
    "${endpoint%/}/$OVERMESH_LIVE_CUSTOMER_CONTAINER/$probe_path$query_suffix")
  if [[ "$status" != "$expected" ]]; then
    echo "$identity $method probe returned $status instead of $expected for $endpoint using $version." >&2
    cat "$probe_body" >&2
    exit 1
  fi
  echo "$identity $method probe returned expected $status for $endpoint using $version."
}

IFS=',' read -r -a versions <<<"$storage_versions"
for version in "${versions[@]}"; do
  for endpoint in \
    "$OVERMESH_LIVE_ACCOUNT_A_BLOB_ENDPOINT" \
    "$OVERMESH_LIVE_ACCOUNT_B_BLOB_ENDPOINT"; do
    probe_conditional_put \
      "$endpoint" \
      "$OVERMESH_LIVE_ALLOWED_TOKEN" \
      "$OVERMESH_LIVE_DENIED_TOKEN" \
      "$version"
    probe_absent_blob HEAD read "$endpoint" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" 404 allowed
    probe_absent_blob HEAD read "$endpoint" "$OVERMESH_LIVE_DENIED_TOKEN" "$version" 403 denied
    probe_absent_blob DELETE delete "$endpoint" "$OVERMESH_LIVE_DENIED_TOKEN" "$version" 403 denied
    probe_absent_blob DELETE delete "$endpoint" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" 404 allowed
  done
done

echo "Overmesh live Azure posture and Storage authorization capability gates passed."
