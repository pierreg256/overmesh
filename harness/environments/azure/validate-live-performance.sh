#!/usr/bin/env bash
set -euo pipefail

required=(
  OVERMESH_LIVE_GATEWAY_ENDPOINT
  OVERMESH_LIVE_ACCOUNT_A_BLOB_ENDPOINT
  OVERMESH_LIVE_CUSTOMER_CONTAINER
  OVERMESH_LIVE_ALLOWED_MANAGED_IDENTITY_CLIENT_ID
  OVERMESH_LIVE_PERFORMANCE_RING_VERSION
  OVERMESH_LIVE_PERFORMANCE_RING_HASH
  OVERMESH_LIVE_PERFORMANCE_DEPLOYMENT
  OVERMESH_LIVE_PERFORMANCE_ENVIRONMENT
  OVERMESH_LIVE_PERFORMANCE_ISOLATED_ENVIRONMENT
  OVERMESH_LIVE_PERFORMANCE_PUBLIC_KEY
  OVERMESH_LIVE_PERFORMANCE_WORKSPACE_ID
  OVERMESH_LIVE_PERFORMANCE_GATEWAY_APP_NAME
  OVERMESH_LIVE_PERFORMANCE_GATEWAY_RESOURCE_ID
  OVERMESH_LIVE_EVIDENCE_KEY_ID
  OVERMESH_LIVE_EVIDENCE_SIGNING_CLIENT_ID
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required for the live performance baseline." >&2
    exit 2
  fi
done
if [[ "$OVERMESH_LIVE_PERFORMANCE_ISOLATED_ENVIRONMENT" != "true" ]]; then
  echo "OVERMESH_LIVE_PERFORMANCE_ISOLATED_ENVIRONMENT must be true." >&2
  exit 2
fi

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "The live performance baseline only supports Linux hosts." >&2
  exit 2
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../../.." && pwd)
runner="$script_dir/performance/overmesh_live_performance.py"
collector="$script_dir/performance/collect_live_performance_telemetry.py"
comparator="$script_dir/performance/compare_live_performance.py"
validator="$script_dir/performance/validate_performance_evidence.py"
requirements="$script_dir/performance/requirements.txt"
builder="$script_dir/build-live-evidence.py"
signer="$script_dir/sign-live-evidence.sh"
contract=${OVERMESH_LIVE_PERFORMANCE_CONTRACT:-"$repo_root/harness/performance/live-v2.toml"}
install_root=${OVERMESH_LIVE_PERFORMANCE_ROOT:-/opt/overmesh-live/performance}
export AZURE_EXTENSION_DIR=${OVERMESH_LIVE_PERFORMANCE_AZURE_EXTENSION_DIR:-"$install_root/az-extensions"}
log_analytics_extension_version=${OVERMESH_LIVE_PERFORMANCE_LOG_ANALYTICS_EXTENSION_VERSION:-1.0.0b1}
venv="$install_root/venv"
downloads="$install_root/downloads"
stamp="$venv/.requirements-sha256"
get_pip="$downloads/get-pip.py"
run_id=${OVERMESH_LIVE_PERFORMANCE_RUN_ID:-$(date -u '+%Y%m%dT%H%M%SZ')}
work_dir=${OVERMESH_LIVE_PERFORMANCE_WORK_DIR:-"$repo_root/.harness/live-performance/$run_id"}
raw_output=${OVERMESH_LIVE_PERFORMANCE_RAW_EVIDENCE_PATH:-"$work_dir/raw-performance.json"}
client_output="$work_dir/client-performance.json"
evidence_dir=${OVERMESH_LIVE_PERFORMANCE_EVIDENCE_DIRECTORY:-"$work_dir/signed"}
evidence="$evidence_dir/performance-v010-evidence.json"
signature="$evidence_dir/performance-v010-evidence.sig.json"
checksums="$evidence_dir/SHA256SUMS"

for command_name in python3 curl git az; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command '$command_name' is unavailable." >&2
    exit 2
  fi
done

mkdir -p "$downloads" "$(dirname "$raw_output")" "$evidence_dir"
installed_log_analytics_version=$(
  az extension show \
    --name log-analytics \
    --query version \
    --output tsv 2>/dev/null || true
)
if [[ "$installed_log_analytics_version" != "$log_analytics_extension_version" ]]; then
  az extension add \
    --name log-analytics \
    --version "$log_analytics_extension_version" \
    --allow-preview true \
    --upgrade \
    --yes \
    --output none
fi
requirements_hash=$(python3 - "$requirements" <<'PY'
import hashlib
import pathlib
import sys

print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)
if [[ ! -x "$venv/bin/python" || ! -f "$stamp" || "$(<"$stamp")" != "$requirements_hash" ]]; then
  python3 -m venv --clear --without-pip "$venv"
  if [[ ! -f "$get_pip" ]]; then
    curl --fail --silent --show-error --location \
      https://bootstrap.pypa.io/get-pip.py \
      --output "$get_pip"
  fi
  "$venv/bin/python" "$get_pip" --quiet
  "$venv/bin/pip" --disable-pip-version-check install --quiet \
    --requirement "$requirements"
  printf '%s' "$requirements_hash" >"$stamp"
fi

export OVERMESH_LIVE_PERFORMANCE_RUN_ID=$run_id
export OVERMESH_LIVE_PERFORMANCE_COMMIT=${OVERMESH_LIVE_PERFORMANCE_COMMIT:-$(git -C "$repo_root" rev-parse HEAD)}
export OVERMESH_LIVE_PERFORMANCE_PROJECT_VERSION=${OVERMESH_LIVE_PERFORMANCE_PROJECT_VERSION:-$(<"$repo_root/VERSION")}

"$venv/bin/python" "$runner" \
  --contract "$contract" \
  --output "$client_output"

"$venv/bin/python" "$collector" \
  --evidence "$client_output" \
  --output "$raw_output"

comparison_arguments=(
  --current "$raw_output"
  --output "$raw_output"
)
if [[ -n "${OVERMESH_LIVE_PERFORMANCE_BASELINE_EVIDENCE:-}" ]]; then
  "$venv/bin/python" "$validator" \
    --evidence "$OVERMESH_LIVE_PERFORMANCE_BASELINE_EVIDENCE" \
    --contract "$contract" \
    --canonical
  comparison_arguments+=(
    --baseline "$OVERMESH_LIVE_PERFORMANCE_BASELINE_EVIDENCE"
  )
fi
"$venv/bin/python" "$comparator" "${comparison_arguments[@]}"

python3 "$builder" \
  --raw-bundle "$raw_output" \
  --output-directory "$evidence_dir" \
  --bundle-name "$(basename "$evidence")" \
  --public-key "$OVERMESH_LIVE_PERFORMANCE_PUBLIC_KEY"

"$venv/bin/python" "$validator" \
  --evidence "$evidence" \
  --contract "$contract" \
  --canonical

export OVERMESH_LIVE_EVIDENCE_PATH=$evidence
export OVERMESH_LIVE_EVIDENCE_SIGNATURE_PATH=$signature
bash "$signer"

python3 - "$evidence_dir" "$checksums" <<'PY'
import hashlib
import pathlib
import sys

directory = pathlib.Path(sys.argv[1])
output = pathlib.Path(sys.argv[2])
entries = []
for path in sorted(directory.iterdir()):
    if path.is_file() and path != output:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        entries.append(f"{digest}  {path.name}")
output.write_text("\n".join(entries) + "\n", encoding="utf-8")
PY

echo "Signed live performance evidence: $evidence_dir"
