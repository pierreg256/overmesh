#!/usr/bin/env bash
set -euo pipefail

required=(
  OVERMESH_LIVE_GATEWAY_ENDPOINT
  OVERMESH_LIVE_ALLOWED_TOKEN
  OVERMESH_LIVE_RECONCILER_TOKEN
  OVERMESH_LIVE_RECONCILER_CONFIG
  OVERMESH_LIVE_RECONCILIATION_COLLECTION_CONFIG
  OVERMESH_LIVE_RECONCILER_BIN
  OVERMESH_LIVE_ACCOUNT_A_BLOB_ENDPOINT
  OVERMESH_LIVE_ACCOUNT_B_BLOB_ENDPOINT
  OVERMESH_LIVE_ACCOUNT_C_BLOB_ENDPOINT
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required for the live Azure reconciliation gate." >&2
    exit 2
  fi
done

for path in \
  "$OVERMESH_LIVE_RECONCILER_CONFIG" \
  "$OVERMESH_LIVE_RECONCILIATION_COLLECTION_CONFIG" \
  "$OVERMESH_LIVE_RECONCILER_BIN"; do
  if [[ ! -f "$path" ]]; then
    echo "$path does not exist." >&2
    exit 2
  fi
done

storage_version=${OVERMESH_LIVE_STORAGE_API_VERSIONS:-2025-11-05}
logical_account=${OVERMESH_LIVE_LOGICAL_ACCOUNT:-overmesh-v090}
run_id=${OVERMESH_LIVE_RECONCILIATION_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}
work_dir=${OVERMESH_LIVE_RECONCILIATION_WORK_DIR:-.harness/live-reconciliation/$run_id}
evidence_path=${OVERMESH_LIVE_RECONCILIATION_EVIDENCE_PATH:-"$work_dir/evidence.json"}
collection_delay_seconds=$(
  awk -F: '
    /^[[:space:]]*physicalCollectionDelaySeconds:/ {
      gsub(/[[:space:]]/, "", $2)
      print $2
      exit
    }
  ' "$OVERMESH_LIVE_RECONCILIATION_COLLECTION_CONFIG"
)
if [[ -z "$collection_delay_seconds" || ! "$collection_delay_seconds" =~ ^[0-9]+$ ]]; then
  echo "The collection config must declare physicalCollectionDelaySeconds." >&2
  exit 2
fi
production_collection_delay_seconds=$(
  awk -F: '
    /^[[:space:]]*physicalCollectionDelaySeconds:/ {
      gsub(/[[:space:]]/, "", $2)
      print $2
      exit
    }
  ' "$OVERMESH_LIVE_RECONCILER_CONFIG"
)
if [[ -z "$production_collection_delay_seconds" ||
  ! "$production_collection_delay_seconds" =~ ^[0-9]+$ ]]; then
  echo "The production config must declare physicalCollectionDelaySeconds." >&2
  exit 2
fi
isolated_environment=${OVERMESH_LIVE_RECONCILIATION_ISOLATED_ENVIRONMENT:-false}
if ((collection_delay_seconds < production_collection_delay_seconds)) &&
  [[ "$isolated_environment" != "true" ]]; then
  echo "Reduced retention is allowed only when OVERMESH_LIVE_RECONCILIATION_ISOLATED_ENVIRONMENT=true." >&2
  exit 2
fi

mkdir -p "$work_dir" "$(dirname "$evidence_path")"

declare -A endpoints=(
  [storage-a]="$OVERMESH_LIVE_ACCOUNT_A_BLOB_ENDPOINT"
  [storage-b]="$OVERMESH_LIVE_ACCOUNT_B_BLOB_ENDPOINT"
  [storage-c]="$OVERMESH_LIVE_ACCOUNT_C_BLOB_ENDPOINT"
)
canaries=()
body="$work_dir/response.body"
headers="$work_dir/response.headers"

gateway_url() {
  printf '%s%s' "${OVERMESH_LIVE_GATEWAY_ENDPOINT%/}" "$1"
}

path_hash() {
  printf '/%s%s' "$logical_account" "$1" | sha256sum | awk '{print $1}'
}

direct_url() {
  local replica=$1
  local container=$2
  local object=$3
  printf '%s/%s/%s' "${endpoints[$replica]%/}" "$container" "$object"
}

direct_status() {
  local method=$1
  local replica=$2
  local container=$3
  local object=$4
  curl --silent --show-error \
    --output /dev/null \
    --write-out '%{http_code}' \
    -X "$method" \
    --oauth2-bearer "$OVERMESH_LIVE_RECONCILER_TOKEN" \
    -H "x-ms-version: $storage_version" \
    "$(direct_url "$replica" "$container" "$object")"
}

direct_get() {
  local replica=$1
  local container=$2
  local object=$3
  local output=$4
  curl --fail --silent --show-error \
    --oauth2-bearer "$OVERMESH_LIVE_RECONCILER_TOKEN" \
    -H "x-ms-version: $storage_version" \
    "$(direct_url "$replica" "$container" "$object")" \
    -o "$output"
}

direct_put() {
  local replica=$1
  local container=$2
  local object=$3
  local input=$4
  curl --fail --silent --show-error \
    -X PUT \
    --oauth2-bearer "$OVERMESH_LIVE_RECONCILER_TOKEN" \
    -H "x-ms-version: $storage_version" \
    -H "x-ms-blob-type: BlockBlob" \
    --data-binary @"$input" \
    "$(direct_url "$replica" "$container" "$object")" \
    -o /dev/null
}

direct_delete() {
  local replica=$1
  local container=$2
  local object=$3
  local status
  status=$(
    curl --silent --show-error \
      --output /dev/null \
      --write-out '%{http_code}' \
      -X DELETE \
      --oauth2-bearer "$OVERMESH_LIVE_RECONCILER_TOKEN" \
      -H "x-ms-version: $storage_version" \
      "$(direct_url "$replica" "$container" "$object")"
  )
  [[ "$status" == "202" || "$status" == "404" ]]
}

gateway_put() {
  local path=$1
  local write_id=$2
  local payload=$3
  local if_match=${4:-}
  local args=()
  if [[ -n "$if_match" ]]; then
    args=(-H "If-Match: $if_match")
  fi
  printf '%s' "$payload" | curl --silent --show-error \
    --output "$body" \
    --dump-header "$headers" \
    --write-out '%{http_code}' \
    -X PUT \
    --oauth2-bearer "$OVERMESH_LIVE_ALLOWED_TOKEN" \
    -H "x-ms-version: $storage_version" \
    -H "x-overmesh-write-id: $write_id" \
    "${args[@]}" \
    --data-binary @- \
    "$(gateway_url "$path")"
}

gateway_delete() {
  local path=$1
  local status
  status=$(
    curl --silent --show-error \
      --output /dev/null \
      --write-out '%{http_code}' \
      -X DELETE \
      --oauth2-bearer "$OVERMESH_LIVE_ALLOWED_TOKEN" \
      -H "x-ms-version: $storage_version" \
      -H "x-overmesh-write-id: reconciliation-$run_id-cleanup-${path##*/}" \
      "$(gateway_url "$path")"
  )
  [[ "$status" == "202" || "$status" == "404" || "$status" == "409" ]]
}

cleanup() {
  local status=$?
  local path
  for path in "${canaries[@]}"; do
    gateway_delete "$path" >/dev/null 2>&1 || true
  done
  exit "$status"
}
trap cleanup EXIT

create_canary() {
  local path=$1
  local payload=$2
  local write_id=$3
  local status
  canaries+=("$path")
  status=$(gateway_put "$path" "$write_id" "$payload")
  if [[ "$status" != "201" ]]; then
    echo "Gateway canary creation for $path returned $status." >&2
    cat "$body" >&2
    exit 1
  fi
}

detect_replicas() {
  local head=$1
  local replica
  local found=()
  for replica in storage-a storage-b storage-c; do
    if [[ "$(direct_status HEAD "$replica" overmesh-system "$head")" == "200" ]]; then
      found+=("$replica")
    fi
  done
  if [[ "${#found[@]}" != "2" ]]; then
    echo "$head exists on ${#found[@]} replicas instead of two." >&2
    exit 1
  fi
  printf '%s\n%s\n' "${found[0]}" "${found[1]}"
}

run_cycle() {
  local config=$1
  local output=$2
  "$OVERMESH_LIVE_RECONCILER_BIN" \
    --config "$config" \
    once --full-scan >"$output"
}

assert_report_action() {
  local report=$1
  local head=$2
  local action=$3
  jq -e \
    --arg head "$head" \
    --arg action "$action" \
    '.blobs[] | select(.headObject == $head and .action == $action)' \
    "$report" >/dev/null
}

missing_path="/live-v090/reconciliation-missing-$run_id"
missing_payload="live missing replica repair $run_id"
missing_head="heads/$(path_hash "$missing_path").json"
create_canary "$missing_path" "$missing_payload" "reconciliation-$run_id-missing"
mapfile -t missing_replicas < <(detect_replicas "$missing_head")
missing_source=${missing_replicas[0]}
missing_target=${missing_replicas[1]}
direct_get "$missing_source" overmesh-system "$missing_head" "$work_dir/missing-source-head.json"
direct_delete "$missing_target" overmesh-system "$missing_head"
run_cycle "$OVERMESH_LIVE_RECONCILER_CONFIG" "$work_dir/missing-report.json"
assert_report_action "$work_dir/missing-report.json" "$missing_head" REPAIRED
direct_get "$missing_target" overmesh-system "$missing_head" "$work_dir/missing-repaired-head.json"
cmp "$work_dir/missing-source-head.json" "$work_dir/missing-repaired-head.json"

tampered_path="/live-v090/reconciliation-tampered-$run_id"
tampered_payload="trusted live reconciliation content $run_id"
tampered_head="heads/$(path_hash "$tampered_path").json"
create_canary "$tampered_path" "$tampered_payload" "reconciliation-$run_id-tampered"
mapfile -t tampered_replicas < <(detect_replicas "$tampered_head")
tampered_source=${tampered_replicas[0]}
tampered_target=${tampered_replicas[1]}
direct_get "$tampered_source" overmesh-system "$tampered_head" "$work_dir/tampered-head.json"
tampered_container=$(jq -r '.payload.contentContainer' "$work_dir/tampered-head.json")
tampered_object=$(jq -r '.payload.contentObject' "$work_dir/tampered-head.json")
printf 'malicious live reconciliation content %s' "$run_id" >"$work_dir/malicious.bin"
direct_put "$tampered_target" "$tampered_container" "$tampered_object" "$work_dir/malicious.bin"
run_cycle "$OVERMESH_LIVE_RECONCILER_CONFIG" "$work_dir/tampered-report.json"
assert_report_action "$work_dir/tampered-report.json" "$tampered_head" QUARANTINED
quarantine_object="quarantine/$(path_hash "$tampered_path").json"
[[ "$(direct_status HEAD "$tampered_source" overmesh-system "$quarantine_object")" == "200" ]]
[[ "$(direct_status HEAD "$tampered_target" overmesh-system "$quarantine_object")" == "200" ]]
direct_get \
  "$tampered_target" \
  "$tampered_container" \
  "$tampered_object" \
  "$work_dir/still-malicious.bin"
cmp "$work_dir/malicious.bin" "$work_dir/still-malicious.bin"

"$OVERMESH_LIVE_RECONCILER_BIN" \
  --config "$OVERMESH_LIVE_RECONCILER_CONFIG" \
  recover \
  --blob "/$logical_account$tampered_path" \
  --source-replica "$tampered_source" \
  >"$work_dir/recovery-report.json"
jq -e '.action == "RECOVERED" and .healthAfter == "HEALTHY"' \
  "$work_dir/recovery-report.json" >/dev/null
for replica in "$tampered_source" "$tampered_target"; do
  direct_get \
    "$replica" \
    "$tampered_container" \
    "$tampered_object" \
    "$work_dir/recovered-$replica.bin"
  printf '%s' "$tampered_payload" | cmp - "$work_dir/recovered-$replica.bin"
  [[ "$(direct_status HEAD "$replica" overmesh-system "$quarantine_object")" == "404" ]]
done

collection_path="/live-v090/reconciliation-collection-$run_id"
collection_v1="retained generation one $run_id"
collection_v2="active generation two $run_id"
collection_head="heads/$(path_hash "$collection_path").json"
create_canary "$collection_path" "$collection_v1" "reconciliation-$run_id-collection-v1"
logical_etag=$(
  awk 'tolower($1) == "etag:" {sub(/\r$/, "", $2); print $2}' "$headers"
)
[[ -n "$logical_etag" ]]
mapfile -t collection_replicas < <(detect_replicas "$collection_head")
collection_source=${collection_replicas[0]}
collection_target=${collection_replicas[1]}
direct_get \
  "$collection_source" \
  overmesh-system \
  "$collection_head" \
  "$work_dir/collection-v1-head.json"
old_container=$(jq -r '.payload.contentContainer' "$work_dir/collection-v1-head.json")
old_content=$(jq -r '.payload.contentObject' "$work_dir/collection-v1-head.json")
old_block_manifest=$(jq -r '.payload.blockManifestObject' "$work_dir/collection-v1-head.json")
status=$(
  gateway_put \
    "$collection_path" \
    "reconciliation-$run_id-collection-v2" \
    "$collection_v2" \
    "$logical_etag"
)
if [[ "$status" != "201" ]]; then
  echo "Gateway collection successor write returned $status." >&2
  cat "$body" >&2
  exit 1
fi
run_cycle "$OVERMESH_LIVE_RECONCILER_CONFIG" "$work_dir/pre-retention-report.json"
for replica in "$collection_source" "$collection_target"; do
  [[ "$(direct_status HEAD "$replica" "$old_container" "$old_content")" == "200" ]]
  [[ "$(direct_status HEAD "$replica" overmesh-system "$old_block_manifest")" == "200" ]]
done
sleep "$((collection_delay_seconds + 1))"
run_cycle \
  "$OVERMESH_LIVE_RECONCILIATION_COLLECTION_CONFIG" \
  "$work_dir/collection-report.json"
assert_report_action "$work_dir/collection-report.json" "$collection_head" GARBAGE_COLLECTED
for replica in "$collection_source" "$collection_target"; do
  [[ "$(direct_status HEAD "$replica" "$old_container" "$old_content")" == "404" ]]
  [[ "$(direct_status HEAD "$replica" overmesh-system "$old_block_manifest")" == "404" ]]
done

python3 - \
  "$run_id" \
  "$collection_delay_seconds" \
  "$production_collection_delay_seconds" \
  "$isolated_environment" \
  "$missing_source" \
  "$missing_target" \
  "$tampered_source" \
  "$tampered_target" \
  "$collection_source" \
  "$collection_target" \
  "$work_dir" \
  "$evidence_path" <<'PY'
import hashlib
import json
import pathlib
import sys
from datetime import datetime, timezone

(
    run_id,
    collection_delay,
    production_collection_delay,
    isolated_environment,
    missing_source,
    missing_target,
    tampered_source,
    tampered_target,
    collection_source,
    collection_target,
    work_dir,
    output_path,
) = sys.argv[1:]
work = pathlib.Path(work_dir)
output = pathlib.Path(output_path)

def digest(name):
    return hashlib.sha256((work / name).read_bytes()).hexdigest()

evidence = {
    "apiVersion": "evidence.overmesh.io/live-reconciliation/v1",
    "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "runId": run_id,
    "result": "passed",
    "isolatedValidationEnvironment": isolated_environment == "true",
    "checks": [
        {
            "check": "missing-replica-repair",
            "result": "passed",
            "sourceReplica": missing_source,
            "targetReplica": missing_target,
            "reportSha256": digest("missing-report.json"),
        },
        {
            "check": "tampered-content-quarantine",
            "result": "passed",
            "healthyReplica": tampered_source,
            "tamperedReplica": tampered_target,
            "reportSha256": digest("tampered-report.json"),
            "automaticRepairRefused": True,
        },
        {
            "check": "administrator-recovery",
            "result": "passed",
            "sourceReplica": tampered_source,
            "targetReplica": tampered_target,
            "reportSha256": digest("recovery-report.json"),
        },
        {
            "check": "retention-and-collection",
            "result": "passed",
            "replicas": [collection_source, collection_target],
            "testCollectionDelaySeconds": int(collection_delay),
            "productionCollectionDelaySeconds": int(production_collection_delay),
            "preRetentionReportSha256": digest("pre-retention-report.json"),
            "collectionReportSha256": digest("collection-report.json"),
        },
    ],
}
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(
    json.dumps(evidence, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

echo "Overmesh live Azure repair, quarantine, recovery, and collection gates passed."
