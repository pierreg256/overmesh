#!/usr/bin/env bash
set -euo pipefail

mkdir -p .harness
cargo run --quiet -p overmesh-harness -- doctor >/dev/null
token=$(cargo run --quiet -p overmesh-harness -- issue-token valid --principal caller)
cargo run --quiet -p overmesh-harness -- issue-token valid --principal reconciler \
  >.harness/reconciler-token.jwt
reconciler_token=$(cat .harness/reconciler-token.jwt)
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
  create_container "$replica_port" reconcile
done

reset_storage() {
  make dev-reset
  for replica_port in 12100 12101; do
    create_container "$replica_port" overmesh-system
    create_container "$replica_port" reconcile
  done
}

if curl --silent --fail http://127.0.0.1:18080/healthz >/dev/null 2>&1; then
  echo "Port 18080 is already serving a gateway; refusing to reuse an unknown process." >&2
  exit 1
fi

cargo build --quiet -p overmesh-gateway -p overmesh-reconciler
./target/debug/overmesh-gateway \
  --config gateway/config/local.yaml \
  >.harness/reconciler-gateway.log 2>&1 &
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
    cat .harness/reconciler-gateway.log >&2
    exit 1
  fi
  sleep 0.1
done
curl --silent --fail http://127.0.0.1:18080/healthz >/dev/null

storage_get() {
  local replica_port=$1
  local object_key=$2
  local output=$3
  curl --insecure --fail --silent \
    -H "Authorization: Bearer $token" \
    -H 'x-ms-version: 2025-11-05' \
    "https://127.0.0.1:$replica_port/devstoreaccount1/overmesh-system/$object_key" \
    -o "$output"
}

storage_put() {
  local replica_port=$1
  local object_key=$2
  local input=$3
  local content_type=$4
  curl --insecure --fail --silent --output /dev/null -X PUT \
    -H "Authorization: Bearer $token" \
    -H 'x-ms-version: 2025-11-05' \
    -H 'x-ms-blob-type: BlockBlob' \
    -H "Content-Type: $content_type" \
    --data-binary "@$input" \
    "https://127.0.0.1:$replica_port/devstoreaccount1/overmesh-system/$object_key"
}

storage_delete_if_exists() {
  local replica_port=$1
  local object_key=$2
  local status
  status=$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' \
    -H "Authorization: Bearer $reconciler_token" \
    -H 'x-ms-version: 2025-11-05' \
    "https://127.0.0.1:$replica_port/devstoreaccount1/overmesh-system/$object_key")
  if test "$status" = "404"; then
    return
  fi
  test "$status" = "200"
  curl --insecure --fail --silent --output /dev/null -X DELETE \
    -H "Authorization: Bearer $reconciler_token" \
    -H 'x-ms-version: 2025-11-05' \
    "https://127.0.0.1:$replica_port/devstoreaccount1/overmesh-system/$object_key"
}

reset_durable_cursor() {
  local object_key=$1
  for replica_port in 12100 12101; do
    storage_delete_if_exists "$replica_port" "$object_key"
  done
}

data_get() {
  local replica_port=$1
  local container=$2
  local object_key=$3
  local output=$4
  local authorization_header="Bearer $token"
  curl --insecure --fail --silent \
    -H "Authorization: $authorization_header" \
    -H 'x-ms-version: 2025-11-05' \
    "https://127.0.0.1:$replica_port/devstoreaccount1/$container/$object_key" \
    -o "$output"
}

data_put() {
  local replica_port=$1
  local container=$2
  local object_key=$3
  local input=$4
  local authorization_header="Bearer $token"
  curl --insecure --fail --silent --output /dev/null -X PUT \
    -H "Authorization: $authorization_header" \
    -H 'x-ms-version: 2025-11-05' \
    -H 'x-ms-blob-type: BlockBlob' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary "@$input" \
    "https://127.0.0.1:$replica_port/devstoreaccount1/$container/$object_key"
}

run_reconciler() {
  local output=$1
  reset_durable_cursor reconciler-cursors/head-discovery.json
  ./target/debug/overmesh-reconciler \
    --config reconciler/config/local.yaml once >"$output"
}

smoke_run="${HARNESS_RUN_ID:-local}-$$"
stage_blob="/reconcile/stage-$smoke_run"
stage_upload="stage-upload-$smoke_run"
stage_block_id='YmxvY2stMDAwMQ=='
stage_hash=$(printf '/local-overmesh%s' "$stage_blob" | shasum -a 256 | awk '{print $1}')
stage_upload_hash=$(printf '%s' "$stage_upload" | shasum -a 256 | awk '{print $1}')
stage_block_hash=$(printf '%s' "$stage_block_id" | shasum -a 256 | awk '{print $1}')
stage_metadata="staged-blocks/$stage_hash/$stage_upload_hash/$stage_block_hash.json"
test "$(curl --silent --output /dev/null --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: $stage_upload" \
  -H "x-overmesh-upload-id: $stage_upload" \
  --data-binary 'staged repair data' \
  "http://127.0.0.1:18080$stage_blob?comp=block&blockid=YmxvY2stMDAwMQ%3D%3D")" = "201"
test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "http://127.0.0.1:18080$stage_blob")" = "404"
storage_get 12100 "$stage_metadata" .harness/reconcile-stage-a.json
storage_get 12101 "$stage_metadata" .harness/reconcile-stage-b.json
cmp .harness/reconcile-stage-a.json .harness/reconcile-stage-b.json
curl --insecure --fail --silent --output /dev/null -X DELETE \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12101/devstoreaccount1/overmesh-system/$stage_metadata"
reset_durable_cursor reconciler-cursors/staged-block-metadata.json
reset_durable_cursor reconciler-cursors/staged-block-marker.json
run_reconciler .harness/reconcile-stage-repair-report.json
storage_get 12101 "$stage_metadata" .harness/reconcile-stage-repaired-b.json
cmp .harness/reconcile-stage-a.json .harness/reconcile-stage-repaired-b.json

repair_blob="/reconcile/repair-$smoke_run"
repair_hash=$(printf '/local-overmesh%s' "$repair_blob" | shasum -a 256 | awk '{print $1}')
repair_head="heads/$repair_hash.json"
repair_write_1="reconcile-repair-1-$smoke_run"

curl --silent --fail --output /dev/null -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: $repair_write_1" \
  --data-binary 'repair version one' \
  "http://127.0.0.1:18080$repair_blob"
storage_get 12101 "$repair_head" .harness/reconcile-v1-head.json
curl --insecure --fail --silent --output /dev/null -X DELETE \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12101/devstoreaccount1/overmesh-system/$repair_head"
run_reconciler .harness/reconcile-missing-report.json
grep -q '"healthBefore": "MISSING"' .harness/reconcile-missing-report.json
grep -q '"action": "REPAIRED"' .harness/reconcile-missing-report.json
storage_get 12100 "$repair_head" .harness/reconcile-head-a.json
storage_get 12101 "$repair_head" .harness/reconcile-head-b.json
cmp .harness/reconcile-head-a.json .harness/reconcile-head-b.json

repair_high_water="high-water/$repair_hash/current.json"
storage_get 12100 "$repair_high_water" .harness/reconcile-high-water-before-a.json
storage_get 12101 "$repair_high_water" .harness/reconcile-high-water-before-b.json
cmp .harness/reconcile-high-water-before-a.json .harness/reconcile-high-water-before-b.json
curl --insecure --fail --silent --output /dev/null -X DELETE \
  -H "Authorization: Bearer $reconciler_token" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12101/devstoreaccount1/overmesh-system/$repair_high_water"
run_reconciler .harness/reconcile-high-water-missing-report.json
grep -q '"healthBefore": "MISSING"' .harness/reconcile-high-water-missing-report.json
grep -q '"action": "REPAIRED"' .harness/reconcile-high-water-missing-report.json
storage_get 12100 "$repair_high_water" .harness/reconcile-high-water-a.json
storage_get 12101 "$repair_high_water" .harness/reconcile-high-water-b.json
cmp .harness/reconcile-high-water-a.json .harness/reconcile-high-water-b.json

curl --silent --fail --output /dev/null --dump-header .harness/reconcile-v1-headers.txt \
  -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: $repair_write_1" \
  --data-binary 'repair version one' \
  "http://127.0.0.1:18080$repair_blob"
logical_etag=$(awk 'tolower($1) == "etag:" {sub(/\r$/, "", $2); print $2}' \
  .harness/reconcile-v1-headers.txt)
test -n "$logical_etag"
curl --silent --fail --output /dev/null -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "If-Match: $logical_etag" \
  -H "x-overmesh-write-id: reconcile-repair-2-$smoke_run" \
  --data-binary 'repair version two' \
  "http://127.0.0.1:18080$repair_blob"
storage_put 12101 "$repair_head" .harness/reconcile-v1-head.json application/json
run_reconciler .harness/reconcile-drift-report.json
grep -q '"healthBefore": "TAMPERED"' .harness/reconcile-drift-report.json
grep -q '"action": "QUARANTINED"' .harness/reconcile-drift-report.json
./target/debug/overmesh-reconciler \
  --config reconciler/config/local.yaml recover \
  --blob "/local-overmesh$repair_blob" \
  --source-replica storage-a \
  >.harness/reconcile-drift-recovery-report.json
grep -q '"action": "RECOVERED"' .harness/reconcile-drift-recovery-report.json
storage_get 12100 "$repair_head" .harness/reconcile-v2-head-a.json
storage_get 12101 "$repair_head" .harness/reconcile-v2-head-b.json
cmp .harness/reconcile-v2-head-a.json .harness/reconcile-v2-head-b.json

reset_storage

tampered_blob="/reconcile/tampered-$smoke_run"
tampered_hash=$(printf '/local-overmesh%s' "$tampered_blob" | shasum -a 256 | awk '{print $1}')
tampered_head="heads/$tampered_hash.json"
curl --silent --fail --output /dev/null -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: reconcile-tampered-$smoke_run" \
  --data-binary 'trusted content' \
  "http://127.0.0.1:18080$tampered_blob"
storage_get 12100 "$tampered_head" .harness/reconcile-tampered-head.json
manifest_info=$(cargo run --quiet -p overmesh-harness -- \
  verify-commit-manifest .harness/reconcile-tampered-head.json)
content_container=$(printf '%s' "$manifest_info" | cut -f6)
content_key=$(printf '%s' "$manifest_info" | cut -f7)
test -n "$content_container"
test -n "$content_key"
printf 'malicious content' >.harness/malicious-content.bin
data_put 12101 "$content_container" "$content_key" .harness/malicious-content.bin
run_reconciler .harness/reconcile-tampered-report.json
grep -q '"healthBefore": "TAMPERED"' .harness/reconcile-tampered-report.json
grep -q '"healthAfter": "QUARANTINED"' .harness/reconcile-tampered-report.json
grep -q '"action": "QUARANTINED"' .harness/reconcile-tampered-report.json

quarantine_key="quarantine/$tampered_hash.json"
storage_get 12100 "$quarantine_key" .harness/reconcile-quarantine-a.json
storage_get 12101 "$quarantine_key" .harness/reconcile-quarantine-b.json
cmp .harness/reconcile-quarantine-a.json .harness/reconcile-quarantine-b.json
./target/debug/overmesh-reconciler \
  --config reconciler/config/local.yaml \
  verify-record .harness/reconcile-quarantine-a.json >/dev/null
data_get 12101 "$content_container" "$content_key" .harness/still-tampered.bin
cmp .harness/malicious-content.bin .harness/still-tampered.bin

test "$(curl --silent --output .harness/quarantined-put.xml --write-out '%{http_code}' \
  -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: quarantined-write-$smoke_run" \
  --data-binary 'must be rejected' \
  "http://127.0.0.1:18080$tampered_blob")" = "409"
grep -q '<Code>BlobQuarantined</Code>' .harness/quarantined-put.xml

./target/debug/overmesh-reconciler \
  --config reconciler/config/local.yaml recover \
  --blob "/local-overmesh$tampered_blob" \
  --source-replica storage-a \
  >.harness/reconcile-recovery-report.json
grep -q '"action": "RECOVERED"' .harness/reconcile-recovery-report.json
data_get 12100 "$content_container" "$content_key" .harness/recovered-a.bin
data_get 12101 "$content_container" "$content_key" .harness/recovered-b.bin
cmp .harness/recovered-a.bin .harness/recovered-b.bin
test "$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12100/devstoreaccount1/overmesh-system/$quarantine_key")" = "404"
test "$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12101/devstoreaccount1/overmesh-system/$quarantine_key")" = "404"

curl --insecure --fail --silent \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  'https://127.0.0.1:12100/devstoreaccount1/overmesh-system?restype=container&comp=list&prefix=audit/' \
  -o .harness/reconcile-audit-list.xml
audit_key=$(tr '<' '\n' <.harness/reconcile-audit-list.xml |
  sed -n 's/^Name>\([^<]*\).*/\1/p' | head -1)
test -n "$audit_key"
storage_get 12100 "$audit_key" .harness/reconcile-audit-a.json
storage_get 12101 "$audit_key" .harness/reconcile-audit-b.json
cmp .harness/reconcile-audit-a.json .harness/reconcile-audit-b.json
./target/debug/overmesh-reconciler \
  --config reconciler/config/local.yaml \
  verify-record .harness/reconcile-audit-a.json >/dev/null

reset_storage

delete_blob="/reconcile/delete-$smoke_run"
delete_hash=$(printf '/local-overmesh%s' "$delete_blob" | shasum -a 256 | awk '{print $1}')
delete_head="heads/$delete_hash.json"
delete_source_status=$(curl --silent --output .harness/reconcile-delete-source.xml \
  --write-out '%{http_code}' -X PUT \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: reconcile-delete-source-$smoke_run" \
  --data-binary 'retained before collection' \
  "http://127.0.0.1:18080$delete_blob")
if [[ "$delete_source_status" != "201" ]]; then
  cat .harness/reconcile-delete-source.xml >&2
  exit 1
fi
storage_get 12100 "$delete_head" .harness/reconcile-delete-old-head.json
delete_manifest_info=$(cargo run --quiet -p overmesh-harness -- \
  verify-commit-manifest .harness/reconcile-delete-old-head.json)
delete_block_manifest=$(printf '%s' "$delete_manifest_info" | cut -f5)
delete_content_container=$(printf '%s' "$delete_manifest_info" | cut -f6)
delete_content_key=$(printf '%s' "$delete_manifest_info" | cut -f7)
test -n "$delete_block_manifest"
test -n "$delete_content_container"
test -n "$delete_content_key"

test "$(curl --silent --output /dev/null --dump-header .harness/reconcile-delete-headers.txt \
  --write-out '%{http_code}' -X DELETE \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  -H "x-overmesh-write-id: reconcile-delete-$smoke_run" \
  "http://127.0.0.1:18080$delete_blob")" = "202"
grep -qi '^x-overmesh-logical-version: 2' .harness/reconcile-delete-headers.txt
test "$(curl --silent --output /dev/null --write-out '%{http_code}' --head \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "http://127.0.0.1:18080$delete_blob")" = "404"
for replica_port in 12100 12101; do
  test "$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' \
    -H "Authorization: Bearer $token" \
    -H 'x-ms-version: 2025-11-05' \
    "https://127.0.0.1:$replica_port/devstoreaccount1/$delete_content_container/$delete_content_key")" = "200"
done

storage_get 12100 "$delete_head" .harness/reconcile-tombstone-a.json
storage_get 12101 "$delete_head" .harness/reconcile-tombstone-b.json
cmp .harness/reconcile-tombstone-a.json .harness/reconcile-tombstone-b.json
grep -q '"state":"TOMBSTONED"' .harness/reconcile-tombstone-a.json
cargo run --quiet -p overmesh-harness -- \
  verify-commit-manifest .harness/reconcile-tombstone-a.json >/dev/null
run_reconciler .harness/reconcile-garbage-collection-report.json
grep -q '"healthBefore": "TOMBSTONED"' .harness/reconcile-garbage-collection-report.json
grep -q '"action": "GARBAGE_COLLECTED"' .harness/reconcile-garbage-collection-report.json
for replica_port in 12100 12101; do
  test "$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' \
    -H "Authorization: Bearer $token" \
    -H 'x-ms-version: 2025-11-05' \
    "https://127.0.0.1:$replica_port/devstoreaccount1/$delete_content_container/$delete_content_key")" = "404"
  test "$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' \
    -H "Authorization: Bearer $token" \
    -H 'x-ms-version: 2025-11-05' \
    "https://127.0.0.1:$replica_port/devstoreaccount1/overmesh-system/$delete_block_manifest")" = "404"
done
storage_get 12100 "$delete_head" .harness/reconcile-tombstone-after-gc-a.json
storage_get 12101 "$delete_head" .harness/reconcile-tombstone-after-gc-b.json
cmp .harness/reconcile-tombstone-after-gc-a.json .harness/reconcile-tombstone-after-gc-b.json
delete_high_water="high-water/$delete_hash/current.json"
storage_get 12100 "$delete_high_water" .harness/reconcile-delete-high-water-a.json
cmp .harness/reconcile-tombstone-after-gc-a.json .harness/reconcile-delete-high-water-a.json
compaction_checkpoint="high-water/$delete_hash/compaction/current.json"
storage_get 12100 "$compaction_checkpoint" .harness/reconcile-compaction-checkpoint-a.json
storage_get 12101 "$compaction_checkpoint" .harness/reconcile-compaction-checkpoint-b.json
cmp .harness/reconcile-compaction-checkpoint-a.json .harness/reconcile-compaction-checkpoint-b.json
cargo run --quiet -p overmesh-harness -- \
  verify-history-compaction-checkpoint \
  .harness/reconcile-compaction-checkpoint-a.json >/dev/null
gc_marker="garbage-collection/$delete_hash/00000000000000000001.json"
storage_get 12100 "$gc_marker" .harness/reconcile-gc-marker-a.json
storage_get 12101 "$gc_marker" .harness/reconcile-gc-marker-b.json
cmp .harness/reconcile-gc-marker-a.json .harness/reconcile-gc-marker-b.json
cargo run --quiet -p overmesh-harness -- \
  verify-garbage-collection-marker .harness/reconcile-gc-marker-a.json >/dev/null

storage_put 12100 "$delete_head" .harness/reconcile-delete-old-head.json application/json
storage_put 12101 "$delete_head" .harness/reconcile-delete-old-head.json application/json
storage_put 12100 "$delete_high_water" .harness/reconcile-delete-old-head.json application/json
storage_put 12101 "$delete_high_water" .harness/reconcile-delete-old-head.json application/json
replay_token=$(cargo run --quiet -p overmesh-harness -- issue-token valid --principal caller)
test "$(curl --silent --output .harness/reconcile-compaction-replay.xml \
  --write-out '%{http_code}' --head \
  -H "Authorization: Bearer $replay_token" \
  -H 'x-ms-version: 2025-11-05' \
  "http://127.0.0.1:18080$delete_blob")" = "503"
storage_put 12100 "$delete_high_water" .harness/reconcile-delete-high-water-a.json application/json
storage_put 12101 "$delete_high_water" .harness/reconcile-delete-high-water-a.json application/json
run_reconciler .harness/reconcile-anti-resurrection-report.json
grep -q '"healthBefore": "TAMPERED"' .harness/reconcile-anti-resurrection-report.json
grep -q '"action": "QUARANTINED"' .harness/reconcile-anti-resurrection-report.json

cargo run --quiet -p overmesh-harness -- fault disable b >/dev/null
if ./target/debug/overmesh-reconciler \
  --config reconciler/config/local.yaml once \
  >.harness/reconcile-outage-report.json 2>.harness/reconcile-outage-error.log; then
  echo "Reconciliation unexpectedly succeeded with an unavailable replica." >&2
  exit 1
fi
cargo run --quiet -p overmesh-harness -- fault reset >/dev/null
test "$(curl --insecure --silent --output /dev/null --write-out '%{http_code}' \
  -H "Authorization: Bearer $token" \
  -H 'x-ms-version: 2025-11-05' \
  "https://127.0.0.1:12100/devstoreaccount1/overmesh-system/quarantine/$repair_hash.json")" = "404"
