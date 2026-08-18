#!/usr/bin/env bash
set -euo pipefail

required=(
  OVERMESH_LIVE_GATEWAY_ENDPOINT
  OVERMESH_LIVE_CUSTOMER_CONTAINER
  OVERMESH_LIVE_ALLOWED_TOKEN
  OVERMESH_LIVE_DENIED_TOKEN
  OVERMESH_LIVE_ALLOWED_WRITE_MUTATOR
)

for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required for the live Azure gateway authorization gate." >&2
    exit 2
  fi
done

if [[ ! -f "$OVERMESH_LIVE_ALLOWED_WRITE_MUTATOR" || ! -x "$OVERMESH_LIVE_ALLOWED_WRITE_MUTATOR" ]]; then
  echo "OVERMESH_LIVE_ALLOWED_WRITE_MUTATOR must name an executable helper." >&2
  exit 2
fi

storage_versions=${OVERMESH_LIVE_STORAGE_API_VERSIONS:-2025-11-05}
run_id="$$-$(date +%s)"
probe_body=".harness/live-gateway-authz-$run_id.body"
probe_headers=".harness/live-gateway-authz-$run_id.headers"
block_list_xml=".harness/live-gateway-authz-$run_id-block-list.xml"
write_revoked=0
cleanup_specs=()

mkdir -p .harness
printf '%s' '<BlockList><Latest>YmxvY2stMDAwMQ==</Latest></BlockList>' >"$block_list_xml"

run_mutator() {
  local action=$1
  "$OVERMESH_LIVE_ALLOWED_WRITE_MUTATOR" "$action"
}

gateway_url() {
  local path=$1
  printf '%s%s' "${OVERMESH_LIVE_GATEWAY_ENDPOINT%/}" "$path"
}

remember_cleanup() {
  cleanup_specs+=("$1|$2|$3")
}

assert_authorization_denied() {
  local context=$1
  local status=$2
  if [[ "$status" != "403" ]]; then
    echo "$context returned $status instead of 403." >&2
    cat "$probe_body" >&2
    exit 1
  fi
  grep -qi '^x-ms-error-code: AuthorizationPermissionMismatch' "$probe_headers" || {
    echo "$context did not return x-ms-error-code AuthorizationPermissionMismatch." >&2
    cat "$probe_headers" >&2
    cat "$probe_body" >&2
    exit 1
  }
  grep -q '<Code>AuthorizationPermissionMismatch</Code>' "$probe_body" || {
    echo "$context did not return the AuthorizationPermissionMismatch body code." >&2
    cat "$probe_body" >&2
    exit 1
  }
}

gateway_put_blob() {
  local path=$1
  local token=$2
  local version=$3
  local write_id=$4
  local payload=$5
  printf '%s' "$payload" | curl --silent --show-error \
    --output "$probe_body" \
    --dump-header "$probe_headers" \
    --write-out '%{http_code}' \
    -X PUT \
    --oauth2-bearer "$token" \
    -H "x-ms-version: $version" \
    -H "x-overmesh-write-id: $write_id" \
    --data-binary @- \
    "$(gateway_url "$path")"
}

gateway_head_blob() {
  local path=$1
  local token=$2
  local version=$3
  curl --silent --show-error \
    --output "$probe_body" \
    --dump-header "$probe_headers" \
    --write-out '%{http_code}' \
    --head \
    --oauth2-bearer "$token" \
    -H "x-ms-version: $version" \
    "$(gateway_url "$path")"
}

gateway_put_block() {
  local path=$1
  local token=$2
  local version=$3
  local upload_id=$4
  local write_id=$5
  local payload=$6
  printf '%s' "$payload" | curl --silent --show-error \
    --output "$probe_body" \
    --dump-header "$probe_headers" \
    --write-out '%{http_code}' \
    -X PUT \
    --oauth2-bearer "$token" \
    -H "x-ms-version: $version" \
    -H "x-overmesh-upload-id: $upload_id" \
    -H "x-overmesh-write-id: $write_id" \
    --data-binary @- \
    "$(gateway_url "$path")?comp=block&blockid=YmxvY2stMDAwMQ%3D%3D"
}

gateway_get_block_list() {
  local path=$1
  local token=$2
  local version=$3
  local upload_id=$4
  local block_list_type=$5
  curl --silent --show-error \
    --output "$probe_body" \
    --dump-header "$probe_headers" \
    --write-out '%{http_code}' \
    --oauth2-bearer "$token" \
    -H "x-ms-version: $version" \
    -H "x-overmesh-upload-id: $upload_id" \
    "$(gateway_url "$path")?comp=blocklist&blocklisttype=$block_list_type"
}

gateway_put_block_list() {
  local path=$1
  local token=$2
  local version=$3
  local upload_id=$4
  local write_id=$5
  curl --silent --show-error \
    --output "$probe_body" \
    --dump-header "$probe_headers" \
    --write-out '%{http_code}' \
    -X PUT \
    --oauth2-bearer "$token" \
    -H "x-ms-version: $version" \
    -H "x-overmesh-upload-id: $upload_id" \
    -H "x-overmesh-write-id: $write_id" \
    -H 'Content-Type: application/xml' \
    --data-binary @"$block_list_xml" \
    "$(gateway_url "$path")?comp=blocklist"
}

gateway_delete_blob() {
  local path=$1
  local token=$2
  local version=$3
  local write_id=$4
  curl --silent --show-error \
    --output "$probe_body" \
    --dump-header "$probe_headers" \
    --write-out '%{http_code}' \
    -X DELETE \
    --oauth2-bearer "$token" \
    -H "x-ms-version: $version" \
    -H "x-overmesh-write-id: $write_id" \
    "$(gateway_url "$path")"
}

wait_for_revoked_replay() {
  local path=$1
  local version=$2
  local write_id=$3
  local payload=$4
  local status
  for _ in $(seq 1 30); do
    status=$(gateway_head_blob "$path" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version")
    case "$status" in
      200) ;;
      403)
        echo "Gateway replay read probe lost read permission after write revocation." >&2
        cat "$probe_headers" >&2
        cat "$probe_body" >&2
        exit 1
        ;;
      *)
        echo "Gateway replay read probe returned $status instead of 200 while waiting for write revocation." >&2
        cat "$probe_headers" >&2
        cat "$probe_body" >&2
        exit 1
        ;;
    esac

    status=$(gateway_put_blob "$path" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" "$write_id" "$payload")
    case "$status" in
      201)
        grep -qi '^x-overmesh-idempotent-replay: true' "$probe_headers" || {
          echo "Replay probe returned 201 without the idempotent replay marker." >&2
          cat "$probe_headers" >&2
          exit 1
        }
        sleep 5
        ;;
      403)
        assert_authorization_denied "Gateway idempotent replay after write revocation" "$status"
        echo "Gateway idempotent replay returned the expected 403 after write revocation for $version."
        return 0
        ;;
      *)
        echo "Gateway idempotent replay returned $status while waiting for write revocation." >&2
        cat "$probe_headers" >&2
        cat "$probe_body" >&2
        exit 1
        ;;
    esac
  done

  echo "Timed out waiting for the gateway idempotent replay to observe write revocation." >&2
  exit 1
}

cleanup_blob() {
  local path=$1
  local version=$2
  local write_id=$3
  local status
  for _ in $(seq 1 30); do
    status=$(gateway_delete_blob "$path" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" "$write_id") || {
      echo "Gateway cleanup request failed for $path." >&2
      cat "$probe_body" >&2
      exit 1
    }
    case "$status" in
      202|404)
        return 0
        ;;
      403)
        sleep 5
        ;;
      409)
        if grep -qi '^x-ms-error-code: LeaseAlreadyPresent' "$probe_headers"; then
          sleep 5
        else
          echo "Gateway cleanup for $path returned an unexpected 409." >&2
          cat "$probe_headers" >&2
          cat "$probe_body" >&2
          exit 1
        fi
        ;;
      *)
        echo "Gateway cleanup for $path returned $status instead of 202/404." >&2
        cat "$probe_headers" >&2
        cat "$probe_body" >&2
        exit 1
        ;;
    esac
  done
  echo "Timed out waiting to clean up $path after write restoration." >&2
  exit 1
}

wait_for_restored_block_list_commit() {
  local path=$1
  local version=$2
  local upload_id=$3
  local write_id=$4
  local status
  for _ in $(seq 1 30); do
    status=$(gateway_put_block_list "$path" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" "$upload_id" "$write_id")
    case "$status" in
      201)
        return 0
        ;;
      403)
        sleep 5
        ;;
      409)
        if grep -qi '^x-ms-error-code: LeaseAlreadyPresent' "$probe_headers"; then
          sleep 5
        else
          echo "Gateway Put Block List cleanup returned an unexpected 409." >&2
          cat "$probe_headers" >&2
          cat "$probe_body" >&2
          exit 1
        fi
        ;;
      *)
        echo "Gateway Put Block List cleanup commit returned $status instead of 201 while waiting for write restoration." >&2
        cat "$probe_headers" >&2
        cat "$probe_body" >&2
        exit 1
        ;;
    esac
  done
  echo "Timed out waiting for Gateway Put Block List to observe restored write permission." >&2
  exit 1
}

best_effort_cleanup() {
  local spec
  local path
  local version
  local write_id
  local status
  for spec in "${cleanup_specs[@]}"; do
    IFS='|' read -r path version write_id <<<"$spec"
    status=$(gateway_delete_blob "$path" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" "$write_id" || true)
    case "$status" in
      ""|202|404|403) ;;
      *)
        echo "Warning: best-effort cleanup for $path returned $status." >&2
        ;;
    esac
  done
}

cleanup() {
  local exit_code=$?
  if [[ "$write_revoked" -eq 1 ]]; then
    if ! run_mutator restore-write; then
      echo "Failed to restore write permission for the allowed live principal." >&2
      exit_code=1
    fi
  fi
  best_effort_cleanup
  rm -f "$probe_body" "$probe_headers" "$block_list_xml"
  exit "$exit_code"
}
trap cleanup EXIT

IFS=',' read -r -a versions <<<"$storage_versions"
for version in "${versions[@]}"; do
  replay_path="/$OVERMESH_LIVE_CUSTOMER_CONTAINER/authorization-canary/replay-$run_id-$version"
  replay_write_id="gateway-replay-$run_id-$version"
  replay_payload="gateway replay payload $run_id $version"
  denied_blob_path="/$OVERMESH_LIVE_CUSTOMER_CONTAINER/authorization-canary/denied-put-blob-$run_id-$version"
  denied_block_path="/$OVERMESH_LIVE_CUSTOMER_CONTAINER/authorization-canary/denied-put-block-$run_id-$version"
  denied_block_upload_id="gateway-denied-upload-$run_id-$version"
  # Reuse the replay blob so both denied operations exercise the same Ring replicas.
  blocked_path=$replay_path
  blocked_upload_id="gateway-blocked-upload-$run_id-$version"
  blocked_stage_write_id="gateway-blocked-stage-$run_id-$version"
  blocked_commit_write_id="gateway-blocked-commit-$run_id-$version"
  remember_cleanup "$replay_path" "$version" "gateway-cleanup-replay-$run_id-$version"
  remember_cleanup "$denied_blob_path" "$version" "gateway-cleanup-denied-blob-$run_id-$version"
  remember_cleanup "$denied_block_path" "$version" "gateway-cleanup-denied-block-$run_id-$version"

  status=$(gateway_put_blob "$denied_blob_path" "$OVERMESH_LIVE_DENIED_TOKEN" "$version" "gateway-denied-put-blob-$run_id-$version" "denied put blob")
  assert_authorization_denied "Denied Gateway Put Blob" "$status"

  status=$(gateway_put_block "$denied_block_path" "$OVERMESH_LIVE_DENIED_TOKEN" "$version" "$denied_block_upload_id" "gateway-denied-put-block-$run_id-$version" "denied put block")
  assert_authorization_denied "Denied Gateway Put Block" "$status"

  status=$(gateway_put_blob "$replay_path" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" "$replay_write_id" "$replay_payload")
  if [[ "$status" != "201" ]]; then
    echo "Allowed Gateway Put Blob setup returned $status instead of 201 for $version." >&2
    cat "$probe_headers" >&2
    cat "$probe_body" >&2
    exit 1
  fi

  status=$(gateway_put_block "$blocked_path" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" "$blocked_upload_id" "$blocked_stage_write_id" "staged block")
  if [[ "$status" != "201" ]]; then
    echo "Allowed Gateway Put Block setup returned $status instead of 201 for $version." >&2
    cat "$probe_headers" >&2
    cat "$probe_body" >&2
    exit 1
  fi

  status=$(gateway_get_block_list "$blocked_path" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" "$blocked_upload_id" uncommitted)
  if [[ "$status" != "200" ]] || ! grep -q '<UncommittedBlocks>' "$probe_body"; then
    echo "Allowed Gateway Get Block List setup failed before write revocation for $version." >&2
    cat "$probe_headers" >&2
    cat "$probe_body" >&2
    exit 1
  fi

  write_revoked=1
  run_mutator revoke-write
  wait_for_revoked_replay "$replay_path" "$version" "$replay_write_id" "$replay_payload"

  status=$(gateway_get_block_list "$blocked_path" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" "$blocked_upload_id" uncommitted)
  if [[ "$status" != "200" ]] || ! grep -q '<UncommittedBlocks>' "$probe_body"; then
    echo "Gateway Put Block List precondition lost read access after write revocation for $version." >&2
    cat "$probe_headers" >&2
    cat "$probe_body" >&2
    exit 1
  fi

  status=$(gateway_put_block_list "$blocked_path" "$OVERMESH_LIVE_ALLOWED_TOKEN" "$version" "$blocked_upload_id" "$blocked_commit_write_id")
  assert_authorization_denied "Gateway Put Block List after write revocation" "$status"

  run_mutator restore-write
  write_revoked=0

  wait_for_restored_block_list_commit \
    "$blocked_path" \
    "$version" \
    "$blocked_upload_id" \
    "$blocked_commit_write_id"

  cleanup_blob "$replay_path" "$version" "gateway-cleanup-replay-$run_id-$version"

done

echo "Overmesh live Azure gateway authorization probes passed."
