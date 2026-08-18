#!/usr/bin/env bash
set -euo pipefail

required=(
  OVERMESH_LIVE_GATEWAY_ENDPOINT
  OVERMESH_LIVE_ALLOWED_TOKEN
  OVERMESH_LIVE_RECONCILER_TOKEN
  OVERMESH_LIVE_ACCOUNT_A_BLOB_ENDPOINT
  OVERMESH_LIVE_ACCOUNT_B_BLOB_ENDPOINT
  OVERMESH_LIVE_ACCOUNT_C_BLOB_ENDPOINT
  OVERMESH_LIVE_PLACEMENT_PHASE
  OVERMESH_LIVE_PLACEMENT_RUN_ID
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required for the live placement gate." >&2
    exit 2
  fi
done

storage_version=${OVERMESH_LIVE_STORAGE_API_VERSIONS:-2025-11-05}
logical_account=${OVERMESH_LIVE_LOGICAL_ACCOUNT:-overmesh-v090}
path_ab=${OVERMESH_LIVE_PLACEMENT_PATH_AB:-/live-v090/placement-00001}
path_ac=${OVERMESH_LIVE_PLACEMENT_PATH_AC:-/live-v090/placement-00002}
path_bc=${OVERMESH_LIVE_PLACEMENT_PATH_BC:-/live-v090/placement-00000}
work_dir=${OVERMESH_LIVE_EVIDENCE_DIR:-/opt/overmesh-live}
evidence="$work_dir/placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID.jsonl"
body="$work_dir/placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID.body"
headers="$work_dir/placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID.headers"

mkdir -p "$work_dir"

gateway_url() {
  printf '%s%s' "${OVERMESH_LIVE_GATEWAY_ENDPOINT%/}" "$1"
}

record() {
  jq -nc \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg phase "$OVERMESH_LIVE_PLACEMENT_PHASE" \
    --arg check "$1" \
    --arg result "$2" \
    --arg detail "$3" \
    '{timestamp:$timestamp,phase:$phase,check:$check,result:$result,detail:$detail}' \
    >>"$evidence"
}

put_blob() {
  local path=$1
  local write_id=$2
  local payload=$3
  printf '%s' "$payload" | curl --silent --show-error \
    --output "$body" \
    --dump-header "$headers" \
    --write-out '%{http_code}' \
    -X PUT \
    --oauth2-bearer "$OVERMESH_LIVE_ALLOWED_TOKEN" \
    -H "x-ms-version: $storage_version" \
    -H "x-overmesh-write-id: $write_id" \
    --data-binary @- \
    "$(gateway_url "$path")"
}

get_blob() {
  local path=$1
  curl --fail --silent --show-error \
    --oauth2-bearer "$OVERMESH_LIVE_ALLOWED_TOKEN" \
    -H "x-ms-version: $storage_version" \
    "$(gateway_url "$path")"
}

delete_blob() {
  local path=$1
  local write_id=$2
  curl --silent --show-error \
    --output "$body" \
    --dump-header "$headers" \
    --write-out '%{http_code}' \
    -X DELETE \
    --oauth2-bearer "$OVERMESH_LIVE_ALLOWED_TOKEN" \
    -H "x-ms-version: $storage_version" \
    -H "x-overmesh-write-id: $write_id" \
    "$(gateway_url "$path")"
}

path_hash() {
  printf '/%s%s' "$logical_account" "$1" | sha256sum | awk '{print $1}'
}

direct_head_status() {
  local endpoint=$1
  local hash=$2
  curl --silent --show-error \
    --output /dev/null \
    --write-out '%{http_code}' \
    --head \
    --oauth2-bearer "$OVERMESH_LIVE_RECONCILER_TOKEN" \
    -H "x-ms-version: $storage_version" \
    "${endpoint%/}/overmesh-system/heads/$hash.json"
}

assert_placement() {
  local path=$1
  local first=$2
  local second=$3
  local hash
  hash=$(path_hash "$path")
  local node endpoint expected actual
  for node in a b c; do
    case "$node" in
      a) endpoint=$OVERMESH_LIVE_ACCOUNT_A_BLOB_ENDPOINT ;;
      b) endpoint=$OVERMESH_LIVE_ACCOUNT_B_BLOB_ENDPOINT ;;
      c) endpoint=$OVERMESH_LIVE_ACCOUNT_C_BLOB_ENDPOINT ;;
    esac
    expected=404
    if [[ "$node" == "$first" || "$node" == "$second" ]]; then
      expected=200
    fi
    actual=$(direct_head_status "$endpoint" "$hash")
    if [[ "$actual" != "$expected" ]]; then
      record "placement-$path-$node" FAIL "expected $expected, received $actual"
      exit 1
    fi
  done
  record "placement-$path" PASS "head exists only on storage-$first and storage-$second"
}

expect_put() {
  local path=$1
  local write_id=$2
  local payload=$3
  local expected=$4
  local status
  status=$(put_blob "$path" "$write_id" "$payload")
  if [[ "$status" != "$expected" ]]; then
    record "put-$path" FAIL "expected $expected, received $status: $(cat "$body")"
    exit 1
  fi
  if [[ "$expected" == "503" ]]; then
    grep -q '<Code>ServerBusy</Code>' "$body" || {
      record "put-$path" FAIL "503 did not contain ServerBusy"
      exit 1
    }
  fi
  record "put-$path" PASS "HTTP $status"
}

assert_content() {
  local path=$1
  local expected=$2
  local actual
  actual=$(get_blob "$path")
  if [[ "$actual" != "$expected" ]]; then
    record "get-$path" FAIL "content mismatch"
    exit 1
  fi
  record "get-$path" PASS "content matched"
}

baseline_payload() {
  printf 'baseline-%s-%s' "$OVERMESH_LIVE_PLACEMENT_RUN_ID" "$1"
}

outage_payload() {
  printf 'outage-%s-%s' "$OVERMESH_LIVE_PLACEMENT_RUN_ID" "$1"
}

case "$OVERMESH_LIVE_PLACEMENT_PHASE" in
  baseline)
    : >"$evidence"
    expect_put "$path_ab" "placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID-ab-base" "$(baseline_payload ab)" 201
    expect_put "$path_ac" "placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID-ac-base" "$(baseline_payload ac)" 201
    expect_put "$path_bc" "placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID-bc-base" "$(baseline_payload bc)" 201
    assert_content "$path_ab" "$(baseline_payload ab)"
    assert_content "$path_ac" "$(baseline_payload ac)"
    assert_content "$path_bc" "$(baseline_payload bc)"
    assert_placement "$path_ab" a b
    assert_placement "$path_ac" a c
    assert_placement "$path_bc" b c
    ;;
  outage)
    expect_put "$path_ab" "placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID-ab-outage" "$(outage_payload ab)" 503
    expect_put "$path_ac" "placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID-ac-outage" "$(outage_payload ac)" 503
    expect_put "$path_bc" "placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID-bc-outage" "$(outage_payload bc)" 201
    assert_content "$path_bc" "$(outage_payload bc)"
    record outage-isolation PASS "only replica sets containing storage-a failed"
    ;;
  recovery)
    expect_put "$path_ab" "placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID-ab-outage" "$(outage_payload ab)" 201
    expect_put "$path_ac" "placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID-ac-outage" "$(outage_payload ac)" 201
    expect_put "$path_bc" "placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID-bc-outage" "$(outage_payload bc)" 201
    assert_content "$path_ab" "$(outage_payload ab)"
    assert_content "$path_ac" "$(outage_payload ac)"
    assert_content "$path_bc" "$(outage_payload bc)"
    assert_placement "$path_ab" a b
    assert_placement "$path_ac" a c
    assert_placement "$path_bc" b c
    for path in "$path_ab" "$path_ac" "$path_bc"; do
      status=$(delete_blob "$path" "placement-$OVERMESH_LIVE_PLACEMENT_RUN_ID-clean-${path##*/}")
      [[ "$status" == "202" || "$status" == "404" ]] || {
        record "cleanup-$path" FAIL "expected 202/404, received $status"
        exit 1
      }
    done
    record recovery PASS "storage-a restored, failed writes retried, canaries deleted"
    ;;
  *)
    echo "OVERMESH_LIVE_PLACEMENT_PHASE must be baseline, outage, or recovery." >&2
    exit 2
    ;;
esac

echo "Overmesh live placement phase $OVERMESH_LIVE_PLACEMENT_PHASE passed."
