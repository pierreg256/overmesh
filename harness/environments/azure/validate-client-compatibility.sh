#!/usr/bin/env bash
set -euo pipefail

required=(
  OVERMESH_LIVE_GATEWAY_ENDPOINT
  OVERMESH_LIVE_CUSTOMER_CONTAINER
  OVERMESH_LIVE_ALLOWED_MANAGED_IDENTITY_CLIENT_ID
)

for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required for the live Azure client compatibility gate." >&2
    exit 2
  fi
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "The live Azure client compatibility gate only supports Linux hosts." >&2
  exit 2
fi

case "$(uname -m)" in
  x86_64|amd64) ;;
  *)
    echo "The live Azure client compatibility gate only supports x86_64 hosts." >&2
    exit 2
    ;;
esac

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../../.." && pwd)

install_root=${OVERMESH_LIVE_CLIENT_COMPAT_ROOT:-/opt/overmesh-live/client-compat}
run_id=${OVERMESH_LIVE_CLIENT_COMPAT_RUN_ID:-$(python3 - <<'PY'
from datetime import datetime, timezone
import secrets

print(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4))
PY
)}

work_dir=${OVERMESH_LIVE_CLIENT_COMPAT_WORK_DIR:-"$repo_root/.harness/live-client-compat/$run_id"}
results_dir="$work_dir/results"
logs_dir="$work_dir/logs"
downloads_dir="$install_root/downloads"
tools_dir="$install_root/tools"
state_dir="$install_root/state"
venvs_dir="$install_root/venvs"
evidence_path=${OVERMESH_LIVE_CLIENT_COMPAT_EVIDENCE_PATH:-"$work_dir/evidence.json"}
get_pip_script="$downloads_dir/get-pip.py"

project_version=${OVERMESH_LIVE_CLIENT_COMPAT_PROJECT_VERSION:-$(<"$repo_root/VERSION")}
commit=${OVERMESH_LIVE_CLIENT_COMPAT_COMMIT:-$(git -C "$repo_root" rev-parse HEAD)}

endpoint=${OVERMESH_LIVE_GATEWAY_ENDPOINT%/}
container=$OVERMESH_LIVE_CUSTOMER_CONTAINER
managed_identity_client_id=$OVERMESH_LIVE_ALLOWED_MANAGED_IDENTITY_CLIENT_ID

node_version=${OVERMESH_LIVE_CLIENT_COMPAT_NODE_VERSION:-20.19.0}
dotnet_version=${OVERMESH_LIVE_CLIENT_COMPAT_DOTNET_VERSION:-8.0.100}
azure_cli_version=${OVERMESH_LIVE_CLIENT_COMPAT_AZURE_CLI_VERSION:-2.76.0}
azcopy_version=${OVERMESH_LIVE_CLIENT_COMPAT_AZCOPY_VERSION:-10.27.1}

python_sdk_requirements="$repo_root/harness/environments/azure/client-compat/python/requirements.txt"
node_package_json="$repo_root/harness/environments/azure/client-compat/node/package.json"
node_entrypoint="$repo_root/harness/environments/azure/client-compat/node/overmesh-live-client-compat.mjs"
dotnet_project="$repo_root/harness/environments/azure/client-compat/dotnet/Overmesh.LiveClientCompat.csproj"
python_entrypoint="$repo_root/harness/environments/azure/client-compat/python/overmesh_live_client_compat.py"

python_sdk_venv="$venvs_dir/python-sdk"
azure_cli_venv="$venvs_dir/azure-cli"
node_runtime_dir="$install_root/node-runtime"
dotnet_root="$tools_dir/dotnet-$dotnet_version"
dotnet_install_script="$downloads_dir/dotnet-install.sh"
node_archive="$downloads_dir/node-v${node_version}-linux-x64.tar.gz"
node_root="$tools_dir/node-v${node_version}-linux-x64"
azcopy_archive="$downloads_dir/azcopy_linux_amd64_${azcopy_version}.tar.gz"
azcopy_root="$tools_dir/azcopy_linux_amd64_${azcopy_version}"

mkdir -p \
  "$work_dir" \
  "$results_dir" \
  "$logs_dir" \
  "$downloads_dir" \
  "$tools_dir" \
  "$state_dir" \
  "$venvs_dir"

timestamp_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

log() {
  printf '[%s] %s\n' "$(timestamp_utc)" "$*" >&2
}

require_base_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command '$1' is unavailable." >&2
    exit 2
  fi
}

for command_name in python3 curl jq git tar; do
  require_base_command "$command_name"
done

download_once() {
  local url=$1
  local destination=$2
  if [[ -f "$destination" ]]; then
    return 0
  fi
  local partial="${destination}.partial"
  rm -f "$partial"
  curl --fail --silent --show-error --location "$url" --output "$partial"
  mv "$partial" "$destination"
}

text_file_sha256() {
  python3 - "$1" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
print(hashlib.sha256(path.read_bytes()).hexdigest())
PY
}

write_text_file() {
  local path=$1
  shift
  printf '%s' "$*" >"$path"
}

write_pattern_file() {
  local path=$1
  local size_bytes=$2
  local label=$3
  python3 - "$path" "$size_bytes" "$label" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
size = int(sys.argv[2])
label = sys.argv[3].encode("utf-8")
chunk = (label + b"|") * 1024

with path.open("wb") as handle:
    remaining = size
    while remaining > 0:
        piece = chunk[:remaining]
        handle.write(piece)
        remaining -= len(piece)
PY
}

json_from_file() {
  jq -c . "$1"
}

log_excerpt_json() {
  local path=$1
  if [[ ! -f "$path" ]]; then
    jq -cn '[]'
    return 0
  fi
  tail -n 40 "$path" | jq -Rsc 'split("\n") | map(select(length > 0))'
}

write_unexpected_failure_result() {
  local client=$1
  local result_path=$2
  local prefix=$3
  local log_path=$4
  local tool_versions_json=$5
  local message=$6
  local log_excerpt
  log_excerpt=$(log_excerpt_json "$log_path")
  jq -n \
    --arg client "$client" \
    --arg result "failed" \
    --arg endpoint "$endpoint" \
    --arg container "$container" \
    --arg prefix "$prefix" \
    --arg timestamp "$(timestamp_utc)" \
    --arg commit "$commit" \
    --arg project_version "$project_version" \
    --arg log_path "$log_path" \
    --arg error "$message" \
    --argjson tool_versions "$tool_versions_json" \
    --argjson log_excerpt "$log_excerpt" \
    '{
      client: $client,
      result: $result,
      endpoint: $endpoint,
      container: $container,
      prefix: $prefix,
      timestamp_utc: $timestamp,
      commit: $commit,
      project_version: $project_version,
      tool_versions: $tool_versions,
      operations: [],
      log_path: $log_path,
      error: $error,
      log_excerpt: $log_excerpt
    }' >"$result_path"
}

ensure_python_sdk_venv() {
  local stamp="$python_sdk_venv/.requirements-sha256"
  local expected
  expected=$(text_file_sha256 "$python_sdk_requirements")
  if [[ ! -x "$python_sdk_venv/bin/python" || ! -x "$python_sdk_venv/bin/pip" || ! -f "$stamp" || "$(cat "$stamp")" != "$expected" ]]; then
    rm -rf "$python_sdk_venv"
    python3 -m venv --without-pip "$python_sdk_venv"
    download_once "https://bootstrap.pypa.io/get-pip.py" "$get_pip_script"
    "$python_sdk_venv/bin/python" "$get_pip_script" --quiet
    "$python_sdk_venv/bin/pip" --disable-pip-version-check install --quiet --upgrade pip
    "$python_sdk_venv/bin/pip" --disable-pip-version-check install --quiet -r "$python_sdk_requirements"
    printf '%s' "$expected" >"$stamp"
  fi
  python_sdk_python="$python_sdk_venv/bin/python"
}

ensure_azure_cli_venv() {
  local stamp="$azure_cli_venv/.azure-cli-version"
  if [[ ! -x "$azure_cli_venv/bin/az" || ! -f "$stamp" || "$(cat "$stamp")" != "$azure_cli_version" ]]; then
    rm -rf "$azure_cli_venv"
    python3 -m venv --without-pip "$azure_cli_venv"
    download_once "https://bootstrap.pypa.io/get-pip.py" "$get_pip_script"
    "$azure_cli_venv/bin/python" "$get_pip_script" --quiet
    "$azure_cli_venv/bin/pip" --disable-pip-version-check install --quiet --upgrade pip
    "$azure_cli_venv/bin/pip" --disable-pip-version-check install --quiet "azure-cli==$azure_cli_version"
    printf '%s' "$azure_cli_version" >"$stamp"
  fi
  az_bin="$azure_cli_venv/bin/az"
}

ensure_node_toolchain() {
  if [[ ! -x "$node_root/bin/node" || ! -x "$node_root/bin/npm" ]]; then
    download_once \
      "https://nodejs.org/dist/v${node_version}/node-v${node_version}-linux-x64.tar.gz" \
      "$node_archive"
    rm -rf "$node_root"
    tar -xzf "$node_archive" -C "$tools_dir"
  fi
  node_bin="$node_root/bin/node"
  npm_bin="$node_root/bin/npm"
}

ensure_node_runtime() {
  ensure_node_toolchain
  local stamp="$node_runtime_dir/.package-sha256"
  local expected
  expected=$(text_file_sha256 "$node_package_json")
  mkdir -p "$node_runtime_dir"
  if [[ ! -d "$node_runtime_dir/node_modules" || ! -f "$stamp" || "$(cat "$stamp")" != "$expected" ]]; then
    rm -rf \
      "$node_runtime_dir/node_modules" \
      "$node_runtime_dir/package-lock.json" \
      "$node_runtime_dir/overmesh-live-client-compat.mjs"
    cp "$node_package_json" "$node_runtime_dir/package.json"
    PATH="$node_root/bin:$PATH" "$npm_bin" \
      --prefix "$node_runtime_dir" install --silent --no-fund --no-audit
    printf '%s' "$expected" >"$stamp"
  fi
  cp "$node_entrypoint" "$node_runtime_dir/overmesh-live-client-compat.mjs"
}

ensure_dotnet_toolchain() {
  if [[ ! -x "$dotnet_root/dotnet" ]]; then
    download_once "https://dot.net/v1/dotnet-install.sh" "$dotnet_install_script"
    chmod +x "$dotnet_install_script"
    mkdir -p "$dotnet_root"
    bash "$dotnet_install_script" \
      --version "$dotnet_version" \
      --install-dir "$dotnet_root" \
      --architecture x64 \
      --os linux \
      --no-path
  fi
  dotnet_bin="$dotnet_root/dotnet"
}

python_tool_versions_json() {
  ensure_python_sdk_venv
  "$python_sdk_python" - <<'PY'
import json
import platform
from azure.identity import __version__ as identity_version
from azure.storage.blob import __version__ as blob_version

print(
    json.dumps(
        {
            "python": platform.python_version(),
            "azure_identity": identity_version,
            "azure_storage_blob": blob_version,
        }
    )
)
PY
}

node_tool_versions_json() {
  ensure_node_runtime
  "$node_bin" -e 'const fs=require("fs");const path=require("path");const root=process.argv[1];const storage=require(path.join(root,"node_modules","@azure","storage-blob","package.json"));const identity=require(path.join(root,"node_modules","@azure","identity","package.json"));console.log(JSON.stringify({node:process.version,"@azure/storage-blob":storage.version,"@azure/identity":identity.version}));' "$node_runtime_dir"
}

dotnet_tool_versions_json() {
  ensure_dotnet_toolchain
  jq -cn --arg dotnet "$("$dotnet_bin" --version)" '{dotnet_sdk: $dotnet}'
}

azure_cli_tool_versions_json() {
  ensure_azure_cli_venv
  "$az_bin" version --output json | jq -c '{
    azure_cli: .["azure-cli"],
    azure_cli_core: .["azure-cli-core"],
    azure_cli_telemetry: .["azure-cli-telemetry"],
    python: .python
  }'
}

azcopy_tool_versions_json() {
  ensure_azcopy_toolchain
  jq -cn --arg azcopy "$("$azcopy_bin" --version | tr -d '\r')" '{azcopy: $azcopy}'
}

ensure_azcopy_toolchain() {
  if [[ ! -x "$azcopy_root/azcopy" ]]; then
    download_once \
      "https://github.com/Azure/azure-storage-azcopy/releases/download/v${azcopy_version}/azcopy_linux_amd64_${azcopy_version}.tar.gz" \
      "$azcopy_archive"
    rm -rf "$azcopy_root"
    mkdir -p "$azcopy_root"
    tar -xzf "$azcopy_archive" -C "$azcopy_root" --strip-components=1
  fi
  azcopy_bin="$azcopy_root/azcopy"
}

run_python_sdk_client() {
  local client="azure-sdk-python"
  local prefix="client-compat/$run_id/$client"
  local client_dir="$work_dir/$client"
  local result_path="$results_dir/$client.json"
  local log_path="$logs_dir/$client.log"
  mkdir -p "$client_dir"

  local status
  set +e
  (
    set -e
    ensure_python_sdk_venv
    export OVERMESH_CLIENT_COMPAT_RESULT_PATH="$result_path"
    export OVERMESH_CLIENT_COMPAT_ENDPOINT="$endpoint"
    export OVERMESH_CLIENT_COMPAT_CONTAINER="$container"
    export OVERMESH_CLIENT_COMPAT_PREFIX="$prefix"
    export OVERMESH_CLIENT_COMPAT_RUN_ID="$run_id"
    export OVERMESH_CLIENT_COMPAT_COMMIT="$commit"
    export OVERMESH_CLIENT_COMPAT_PROJECT_VERSION="$project_version"
    export OVERMESH_CLIENT_COMPAT_MI_CLIENT_ID="$managed_identity_client_id"
    export OVERMESH_CLIENT_COMPAT_WORK_DIR="$client_dir"
    "$python_sdk_python" "$python_entrypoint"
  ) >"$log_path" 2>&1
  status=$?
  set -e

  if [[ ! -f "$result_path" ]]; then
    local tool_versions='{}'
    set +e
    tool_versions=$(python_tool_versions_json 2>/dev/null)
    [[ "$tool_versions" != "" ]] && jq -e . >/dev/null 2>&1 <<<"$tool_versions" || tool_versions='{}'
    set -e
    write_unexpected_failure_result \
      "$client" \
      "$result_path" \
      "$prefix" \
      "$log_path" \
      "$tool_versions" \
      "Unexpected Python SDK runner failure (exit $status)."
  fi

  return "$status"
}

run_node_sdk_client() {
  local client="azure-sdk-node"
  local prefix="client-compat/$run_id/$client"
  local client_dir="$work_dir/$client"
  local result_path="$results_dir/$client.json"
  local log_path="$logs_dir/$client.log"
  mkdir -p "$client_dir"

  local status
  set +e
  (
    set -e
    ensure_node_runtime
    export OVERMESH_CLIENT_COMPAT_RESULT_PATH="$result_path"
    export OVERMESH_CLIENT_COMPAT_ENDPOINT="$endpoint"
    export OVERMESH_CLIENT_COMPAT_CONTAINER="$container"
    export OVERMESH_CLIENT_COMPAT_PREFIX="$prefix"
    export OVERMESH_CLIENT_COMPAT_RUN_ID="$run_id"
    export OVERMESH_CLIENT_COMPAT_COMMIT="$commit"
    export OVERMESH_CLIENT_COMPAT_PROJECT_VERSION="$project_version"
    export OVERMESH_CLIENT_COMPAT_MI_CLIENT_ID="$managed_identity_client_id"
    export OVERMESH_CLIENT_COMPAT_WORK_DIR="$client_dir"
    "$node_bin" "$node_runtime_dir/overmesh-live-client-compat.mjs"
  ) >"$log_path" 2>&1
  status=$?
  set -e

  if [[ ! -f "$result_path" ]]; then
    local tool_versions='{}'
    set +e
    tool_versions=$(node_tool_versions_json 2>/dev/null)
    [[ "$tool_versions" != "" ]] && jq -e . >/dev/null 2>&1 <<<"$tool_versions" || tool_versions='{}'
    set -e
    write_unexpected_failure_result \
      "$client" \
      "$result_path" \
      "$prefix" \
      "$log_path" \
      "$tool_versions" \
      "Unexpected Node SDK runner failure (exit $status)."
  fi

  return "$status"
}

run_dotnet_sdk_client() {
  local client="azure-sdk-dotnet"
  local prefix="client-compat/$run_id/$client"
  local client_dir="$work_dir/$client"
  local result_path="$results_dir/$client.json"
  local log_path="$logs_dir/$client.log"
  mkdir -p "$client_dir"

  local status
  set +e
  (
    set -e
    ensure_dotnet_toolchain
    export DOTNET_ROOT="$dotnet_root"
    export PATH="$DOTNET_ROOT:$PATH"
    export DOTNET_CLI_HOME="$install_root/dotnet-home"
    export NUGET_PACKAGES="$install_root/nuget/packages"
    mkdir -p "$DOTNET_CLI_HOME" "$NUGET_PACKAGES"
    export OVERMESH_CLIENT_COMPAT_RESULT_PATH="$result_path"
    export OVERMESH_CLIENT_COMPAT_ENDPOINT="$endpoint"
    export OVERMESH_CLIENT_COMPAT_CONTAINER="$container"
    export OVERMESH_CLIENT_COMPAT_PREFIX="$prefix"
    export OVERMESH_CLIENT_COMPAT_RUN_ID="$run_id"
    export OVERMESH_CLIENT_COMPAT_COMMIT="$commit"
    export OVERMESH_CLIENT_COMPAT_PROJECT_VERSION="$project_version"
    export OVERMESH_CLIENT_COMPAT_MI_CLIENT_ID="$managed_identity_client_id"
    export OVERMESH_CLIENT_COMPAT_WORK_DIR="$client_dir"
    export OVERMESH_CLIENT_COMPAT_DOTNET_SDK_VERSION="$("$dotnet_bin" --version)"
    "$dotnet_bin" run \
      --project "$dotnet_project" \
      --configuration Release \
      --nologo
  ) >"$log_path" 2>&1
  status=$?
  set -e

  if [[ ! -f "$result_path" ]]; then
    local tool_versions='{}'
    set +e
    tool_versions=$(dotnet_tool_versions_json 2>/dev/null)
    [[ "$tool_versions" != "" ]] && jq -e . >/dev/null 2>&1 <<<"$tool_versions" || tool_versions='{}'
    set -e
    write_unexpected_failure_result \
      "$client" \
      "$result_path" \
      "$prefix" \
      "$log_path" \
      "$tool_versions" \
      "Unexpected .NET SDK runner failure (exit $status)."
  fi

  return "$status"
}

run_azure_cli_client() {
  local client="azure-cli"
  local prefix="client-compat/$run_id/$client"
  local client_dir="$work_dir/$client"
  local result_path="$results_dir/$client.json"
  local log_path="$logs_dir/$client.log"
  local operations_path="$client_dir/operations.json"
  local versions_path="$client_dir/versions.json"
  mkdir -p "$client_dir"

  printf '[]' >"$operations_path"
  printf '{}' >"$versions_path"
  record_operation() {
    local name=$1
    local result=$2
    local details=${3:-'{}'}
    local next="$operations_path.next"
    jq -c \
      --arg name "$name" \
      --arg result "$result" \
      --arg timestamp "$(timestamp_utc)" \
      --argjson details "$details" \
      '. + ([{name: $name, result: $result, timestamp_utc: $timestamp}] | map(. + $details))' \
      "$operations_path" >"$next"
    mv "$next" "$operations_path"
  }

  local simple_blob="$prefix/simple.txt"
  local block_blob="$prefix/block.bin"
  local simple_file="$client_dir/simple.txt"
  local block_file="$client_dir/block.bin"
  local simple_download="$client_dir/simple.downloaded.txt"
  local block_download="$client_dir/block.downloaded.bin"
  local delete_error=

  write_text_file "$simple_file" "client=azure-cli
run=$run_id
blob=simple
"
  write_pattern_file "$block_file" 2097152 "azure-cli-$run_id"

  local status=0
  local error_message=
  local show_json=
  local list_json=
  local token_json=
  local az_versions='{}'
  az_bin="$azure_cli_venv/bin/az"

  set +e
  (
    set -e
    ensure_azure_cli_venv
    azure_cli_tool_versions_json >"$versions_path"
    export AZURE_CONFIG_DIR="$client_dir/azure-config"
    mkdir -p "$AZURE_CONFIG_DIR"

    "$az_bin" login \
      --identity \
      --client-id "$managed_identity_client_id" \
      --allow-no-subscriptions \
      --output none \
      --only-show-errors
    record_operation "managed_identity_login" "passed" '{}'

    token_json=$("$az_bin" account get-access-token \
      --resource https://storage.azure.com/ \
      --output json \
      --only-show-errors)
    record_operation \
      "get_access_token" \
      "passed" \
      "$(jq -cn --arg expires "$(jq -r '.expiresOn // .expires_on // empty' <<<"$token_json")" '{expires_on: $expires}')"

    "$az_bin" storage blob upload \
      --auth-mode login \
      --blob-endpoint "$endpoint" \
      --container-name "$container" \
      --name "$simple_blob" \
      --file "$simple_file" \
      --overwrite false \
      --output none \
      --no-progress \
      --only-show-errors
    record_operation \
      "put_blob" \
      "passed" \
      "$(jq -cn --arg blob "$simple_blob" --arg mode 'tool-generated-x-ms-client-request-id' --arg sha "$(text_file_sha256 "$simple_file")" --argjson size "$(wc -c <"$simple_file")" '{blob: $blob, request_id_mode: $mode, sha256: $sha, size_bytes: $size}')"

    "$az_bin" storage blob upload \
      --auth-mode login \
      --blob-endpoint "$endpoint" \
      --container-name "$container" \
      --name "$block_blob" \
      --file "$block_file" \
      --overwrite false \
      --output none \
      --no-progress \
      --only-show-errors
    record_operation \
      "put_blob_large" \
      "passed" \
      "$(jq -cn --arg blob "$block_blob" --arg mode 'tool-generated-x-ms-client-request-id' --arg sha "$(text_file_sha256 "$block_file")" --argjson size "$(wc -c <"$block_file")" '{blob: $blob, request_id_mode: $mode, sha256: $sha, size_bytes: $size}')"

    "$az_bin" storage blob download \
      --auth-mode login \
      --blob-endpoint "$endpoint" \
      --container-name "$container" \
      --name "$simple_blob" \
      --file "$simple_download" \
      --overwrite true \
      --output none \
      --no-progress \
      --only-show-errors
    cmp -s "$simple_file" "$simple_download"
    record_operation \
      "get_blob" \
      "passed" \
      "$(jq -cn --arg blob "$simple_blob" --arg sha "$(text_file_sha256 "$simple_download")" '{blob: $blob, sha256: $sha}')"

    "$az_bin" storage blob download \
      --auth-mode login \
      --blob-endpoint "$endpoint" \
      --container-name "$container" \
      --name "$block_blob" \
      --file "$block_download" \
      --overwrite true \
      --output none \
      --no-progress \
      --only-show-errors
    cmp -s "$block_file" "$block_download"
    record_operation \
      "get_blob_large" \
      "passed" \
      "$(jq -cn --arg blob "$block_blob" --arg sha "$(text_file_sha256 "$block_download")" '{blob: $blob, sha256: $sha}')"

    show_json=$("$az_bin" storage blob show \
      --auth-mode login \
      --blob-endpoint "$endpoint" \
      --container-name "$container" \
      --name "$block_blob" \
      --output json \
      --only-show-errors)
    content_length=
    content_length=$(jq -r '.properties.contentLength // .contentLength // .size // empty' <<<"$show_json")
    [[ "$content_length" == "$(wc -c <"$block_file")" ]]
    record_operation \
      "head_blob" \
      "passed" \
      "$(jq -cn --arg blob "$block_blob" --argjson size "$content_length" '{blob: $blob, content_length: $size}')"

    list_json=$("$az_bin" storage blob list \
      --auth-mode login \
      --blob-endpoint "$endpoint" \
      --container-name "$container" \
      --prefix "$prefix/" \
      --output json \
      --only-show-errors)
    jq -e --arg simple "$simple_blob" --arg block "$block_blob" 'map(.name) | index($simple) and index($block)' <<<"$list_json" >/dev/null
    record_operation \
      "list_blobs" \
      "passed" \
      "$(jq -cn --argjson blobs "$(jq '[.[].name]' <<<"$list_json")" '{blobs: $blobs, count: ($blobs | length)}')"

    "$az_bin" storage blob delete \
      --auth-mode login \
      --blob-endpoint "$endpoint" \
      --container-name "$container" \
      --name "$simple_blob" \
      --output none \
      --only-show-errors
    record_operation \
      "delete_blob" \
      "passed" \
      "$(jq -cn --arg blob "$simple_blob" --arg mode 'tool-generated-x-ms-client-request-id' '{blob: $blob, request_id_mode: $mode}')"

    "$az_bin" storage blob delete \
      --auth-mode login \
      --blob-endpoint "$endpoint" \
      --container-name "$container" \
      --name "$block_blob" \
      --output none \
      --only-show-errors
    record_operation \
      "delete_blob_large" \
      "passed" \
      "$(jq -cn --arg blob "$block_blob" --arg mode 'tool-generated-x-ms-client-request-id' '{blob: $blob, request_id_mode: $mode}')"
  ) >"$log_path" 2>&1
  status=$?
  set -e

  if [[ "$status" -ne 0 ]]; then
    error_message="Azure CLI client compatibility validation failed (exit $status)."
  fi
  operations=$(cat "$operations_path")
  az_versions=$(cat "$versions_path")

  if [[ -x "${az_bin:-}" ]]; then
    set +e
    export AZURE_CONFIG_DIR="$client_dir/azure-config"
    "$az_bin" storage blob delete \
      --auth-mode login \
      --blob-endpoint "$endpoint" \
      --container-name "$container" \
      --name "$simple_blob" \
      --output none \
      --only-show-errors >/dev/null 2>&1
    "$az_bin" storage blob delete \
      --auth-mode login \
      --blob-endpoint "$endpoint" \
      --container-name "$container" \
      --name "$block_blob" \
      --output none \
      --only-show-errors >/dev/null 2>&1
    set -e
  fi

  jq -n \
    --arg client "$client" \
    --arg result "$(if [[ "$status" -eq 0 ]]; then printf passed; else printf failed; fi)" \
    --arg endpoint "$endpoint" \
    --arg container "$container" \
    --arg prefix "$prefix" \
    --arg timestamp "$(timestamp_utc)" \
    --arg commit "$commit" \
    --arg project_version "$project_version" \
    --arg log_path "$log_path" \
    --arg error "$error_message" \
    --argjson tool_versions "$az_versions" \
    --argjson operations "$operations" \
    '{
      client: $client,
      result: $result,
      endpoint: $endpoint,
      container: $container,
      prefix: $prefix,
      timestamp_utc: $timestamp,
      commit: $commit,
      project_version: $project_version,
      tool_versions: $tool_versions,
      operations: $operations,
      log_path: $log_path
    } + (if $error == "" then {} else {error: $error} end)' >"$result_path"

  return "$status"
}

run_azcopy_client() {
  local client="azcopy"
  local prefix="client-compat/$run_id/$client"
  local client_dir="$work_dir/$client"
  local result_path="$results_dir/$client.json"
  local log_path="$logs_dir/$client.log"
  local operations_path="$client_dir/operations.json"
  local versions_path="$client_dir/versions.json"
  mkdir -p "$client_dir"

  printf '[]' >"$operations_path"
  printf '{}' >"$versions_path"
  record_operation() {
    local name=$1
    local result=$2
    local details=${3:-'{}'}
    local next="$operations_path.next"
    jq -c \
      --arg name "$name" \
      --arg result "$result" \
      --arg timestamp "$(timestamp_utc)" \
      --argjson details "$details" \
      '. + ([{name: $name, result: $result, timestamp_utc: $timestamp}] | map(. + $details))' \
      "$operations_path" >"$next"
    mv "$next" "$operations_path"
  }

  local simple_blob="$prefix/simple.txt"
  local block_blob="$prefix/block.bin"
  local container_url="$endpoint/$container"
  local simple_url="$container_url/$simple_blob"
  local block_url="$container_url/$block_blob"
  local simple_file="$client_dir/simple.txt"
  local block_file="$client_dir/block.bin"
  local simple_download_dir="$client_dir/simple-download"
  local block_download_dir="$client_dir/block-download"
  local simple_download="$simple_download_dir/simple.txt"
  local block_download="$block_download_dir/block.bin"
  local azcopy_versions='{}'
  local error_message=
  local trusted_suffixes='*.azurefd.net'
  azcopy_bin="$azcopy_root/azcopy"

  mkdir -p "$simple_download_dir" "$block_download_dir"
  write_text_file "$simple_file" "client=azcopy
run=$run_id
blob=simple
"
  write_pattern_file "$block_file" 2097152 "azcopy-$run_id"

  local status=0
  set +e
  (
    set -e
    ensure_azcopy_toolchain
    azcopy_tool_versions_json >"$versions_path"
    export AZCOPY_AUTO_LOGIN_TYPE=MSI
    export AZCOPY_MSI_CLIENT_ID="$managed_identity_client_id"
    export AZCOPY_LOG_LOCATION="$client_dir/azcopy-logs"
    export AZCOPY_JOB_PLAN_LOCATION="$client_dir/azcopy-jobs"
    mkdir -p "$AZCOPY_LOG_LOCATION" "$AZCOPY_JOB_PLAN_LOCATION"

    "$azcopy_bin" copy "$simple_file" "$simple_url" \
      --from-to=LocalBlob \
      --overwrite=false \
      --trusted-microsoft-suffixes="$trusted_suffixes" \
      --log-level=INFO
    record_operation \
      "put_blob" \
      "passed" \
      "$(jq -cn --arg blob "$simple_blob" --arg mode 'tool-generated-x-ms-client-request-id' --arg sha "$(text_file_sha256 "$simple_file")" --argjson size "$(wc -c <"$simple_file")" '{blob: $blob, request_id_mode: $mode, sha256: $sha, size_bytes: $size}')"

    "$azcopy_bin" copy "$block_file" "$block_url" \
      --from-to=LocalBlob \
      --overwrite=false \
      --trusted-microsoft-suffixes="$trusted_suffixes" \
      --log-level=INFO
    record_operation \
      "put_blob_large" \
      "passed" \
      "$(jq -cn --arg blob "$block_blob" --arg mode 'tool-generated-x-ms-client-request-id' --arg sha "$(text_file_sha256 "$block_file")" --argjson size "$(wc -c <"$block_file")" '{blob: $blob, request_id_mode: $mode, sha256: $sha, size_bytes: $size}')"

    "$azcopy_bin" copy "$simple_url" "$simple_download_dir" \
      --from-to=BlobLocal \
      --overwrite=true \
      --trusted-microsoft-suffixes="$trusted_suffixes" \
      --log-level=INFO
    cmp -s "$simple_file" "$simple_download"
    record_operation \
      "get_blob" \
      "passed" \
      "$(jq -cn --arg blob "$simple_blob" --arg sha "$(text_file_sha256 "$simple_download")" '{blob: $blob, sha256: $sha}')"

    "$azcopy_bin" copy "$block_url" "$block_download_dir" \
      --from-to=BlobLocal \
      --overwrite=true \
      --trusted-microsoft-suffixes="$trusted_suffixes" \
      --log-level=INFO
    cmp -s "$block_file" "$block_download"
    record_operation \
      "get_blob_large" \
      "passed" \
      "$(jq -cn --arg blob "$block_blob" --arg sha "$(text_file_sha256 "$block_download")" '{blob: $blob, sha256: $sha}')"

    "$azcopy_bin" remove "$simple_url" \
      --from-to=BlobTrash \
      --trusted-microsoft-suffixes="$trusted_suffixes" \
      --log-level=INFO
    record_operation \
      "delete_blob" \
      "passed" \
      "$(jq -cn --arg blob "$simple_blob" --arg mode 'tool-generated-x-ms-client-request-id' '{blob: $blob, request_id_mode: $mode}')"

    "$azcopy_bin" remove "$block_url" \
      --from-to=BlobTrash \
      --trusted-microsoft-suffixes="$trusted_suffixes" \
      --log-level=INFO
    record_operation \
      "delete_blob_large" \
      "passed" \
      "$(jq -cn --arg blob "$block_blob" --arg mode 'tool-generated-x-ms-client-request-id' '{blob: $blob, request_id_mode: $mode}')"
  ) >"$log_path" 2>&1
  status=$?
  set -e

  if [[ "$status" -ne 0 ]]; then
    error_message="AzCopy client compatibility validation failed (exit $status)."
  fi
  operations=$(cat "$operations_path")
  azcopy_versions=$(cat "$versions_path")

  if [[ -x "${azcopy_bin:-}" ]]; then
    set +e
    export AZCOPY_AUTO_LOGIN_TYPE=MSI
    export AZCOPY_MSI_CLIENT_ID="$managed_identity_client_id"
    export AZCOPY_LOG_LOCATION="$client_dir/azcopy-logs"
    export AZCOPY_JOB_PLAN_LOCATION="$client_dir/azcopy-jobs"
    "$azcopy_bin" remove "$simple_url" \
      --from-to=BlobTrash \
      --trusted-microsoft-suffixes="$trusted_suffixes" \
      --log-level=ERROR >/dev/null 2>&1
    "$azcopy_bin" remove "$block_url" \
      --from-to=BlobTrash \
      --trusted-microsoft-suffixes="$trusted_suffixes" \
      --log-level=ERROR >/dev/null 2>&1
    set -e
  fi

  jq -n \
    --arg client "$client" \
    --arg result "$(if [[ "$status" -eq 0 ]]; then printf passed; else printf failed; fi)" \
    --arg endpoint "$endpoint" \
    --arg container "$container" \
    --arg prefix "$prefix" \
    --arg timestamp "$(timestamp_utc)" \
    --arg commit "$commit" \
    --arg project_version "$project_version" \
    --arg log_path "$log_path" \
    --arg error "$error_message" \
    --argjson tool_versions "$azcopy_versions" \
    --argjson operations "$operations" \
    '{
      client: $client,
      result: $result,
      endpoint: $endpoint,
      container: $container,
      prefix: $prefix,
      timestamp_utc: $timestamp,
      commit: $commit,
      project_version: $project_version,
      tool_versions: $tool_versions,
      operations: $operations,
      log_path: $log_path
    } + (if $error == "" then {} else {error: $error} end)' >"$result_path"

  return "$status"
}

overall_status=0

for client_runner in \
  run_dotnet_sdk_client \
  run_python_sdk_client \
  run_node_sdk_client \
  run_azure_cli_client \
  run_azcopy_client
do
  set +e
  ("$client_runner")
  client_status=$?
  set -e
  if [[ "$client_status" -ne 0 ]]; then
    overall_status=1
  fi
done

jq -s \
  --arg timestamp "$(timestamp_utc)" \
  --arg commit "$commit" \
  --arg project_version "$project_version" \
  --arg endpoint "$endpoint" \
  --arg container "$container" \
  --arg run_id "$run_id" \
  --arg install_root "$install_root" \
  '{
    timestamp_utc: $timestamp,
    commit: $commit,
    project_version: $project_version,
    endpoint: $endpoint,
    container: $container,
    run_id: $run_id,
    installation_root: $install_root,
    result: (if all(.[]; .result == "passed") then "passed" else "failed" end),
    clients: (sort_by(.client))
  }' "$results_dir"/*.json >"$evidence_path"

if [[ "$overall_status" -eq 0 ]]; then
  echo "Overmesh live Azure client compatibility probes passed. Evidence: $evidence_path"
else
  echo "Overmesh live Azure client compatibility probes failed. Evidence: $evidence_path" >&2
fi

exit "$overall_status"
