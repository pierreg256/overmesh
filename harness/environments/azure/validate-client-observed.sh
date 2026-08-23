#!/usr/bin/env bash
# Drive a client-observed campaign.
#
# This is not the isolated performance gate. It produces evidence that the
# isolated validator and the comparator both refuse, and that can never become
# a baseline. Read harness/artifacts/client-observed/README.md first.
set -euo pipefail

required=(
  AZURE_TENANT_ID
  AZURE_CLIENT_ID
  OVERMESH_CLIENT_OBSERVED_DIRECT_ENDPOINT
  OVERMESH_CLIENT_OBSERVED_GATEWAY_ENDPOINT
  OVERMESH_CLIENT_OBSERVED_CONTAINER
  OVERMESH_CLIENT_OBSERVED_ISOLATED_ENVIRONMENT
  OVERMESH_CLIENT_OBSERVED_COUNTRY
  OVERMESH_CLIENT_OBSERVED_CONNECTION
  OVERMESH_CLIENT_OBSERVED_CORPORATE_PROXY
  OVERMESH_CLIENT_OBSERVED_VPN
  OVERMESH_CLIENT_OBSERVED_OS
  OVERMESH_CLIENT_OBSERVED_NOTE
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required for the client-observed campaign." >&2
    exit 2
  fi
done
if [[ "$OVERMESH_CLIENT_OBSERVED_ISOLATED_ENVIRONMENT" != "false" ]]; then
  echo "OVERMESH_CLIENT_OBSERVED_ISOLATED_ENVIRONMENT must be false." >&2
  exit 2
fi

# Pick the least exposing credential the environment already supplies. No
# credential is created here. The order matches credential_mode() in
# client_observed_campaign.py, and the campaign records the mode it used.
if [[ -n "${AZURE_CLIENT_CERTIFICATE_PATH:-}" ]]; then
  credential_mode=certificate
elif [[ -n "${AZURE_FEDERATED_TOKEN_FILE:-}" ]]; then
  credential_mode=workload-identity
elif [[ -n "${AZURE_CLIENT_SECRET:-}" ]]; then
  credential_mode=client-secret
else
  echo "Set AZURE_CLIENT_CERTIFICATE_PATH, AZURE_FEDERATED_TOKEN_FILE or" >&2
  echo "AZURE_CLIENT_SECRET for the dedicated service principal." >&2
  exit 2
fi

for command_name in python3 az azcopy git; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command '$command_name' is unavailable." >&2
    exit 2
  fi
done

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../../.." && pwd)
runner="$script_dir/performance/client_observed_campaign.py"
contract=${OVERMESH_CLIENT_OBSERVED_CONTRACT:-"$repo_root/harness/performance/client-observed-v1.toml"}

export OVERMESH_CLIENT_OBSERVED_RUN_ID=${OVERMESH_CLIENT_OBSERVED_RUN_ID:-$(date -u '+%Y%m%dT%H%M%SZ')}
export OVERMESH_CLIENT_OBSERVED_COMMIT=${OVERMESH_CLIENT_OBSERVED_COMMIT:-$(git -C "$repo_root" rev-parse HEAD)}
export OVERMESH_CLIENT_OBSERVED_PROJECT_VERSION=${OVERMESH_CLIENT_OBSERVED_PROJECT_VERSION:-$(<"$repo_root/VERSION")}

run_id=$OVERMESH_CLIENT_OBSERVED_RUN_ID
work_root=${OVERMESH_CLIENT_OBSERVED_WORK_ROOT:-"$repo_root/.harness/client-observed"}
# The runner owns and deletes work_dir/staging. Everything the operator needs
# to keep lives in the sibling artifacts directory, which the runner never
# touches, so a telemetry file collected at any point survives the campaign.
# The Azure CLI profile is a third sibling, owned only by this script and its
# trap, so it is never deleted underneath a running campaign.
work_dir="$work_root/$run_id"
staging_dir="$work_dir/staging"
artifacts_dir="$work_dir/artifacts"
client_evidence="$artifacts_dir/client-observed-raw.json"
# Override with OVERMESH_CLIENT_OBSERVED_TELEMETRY when the collector writes
# the server-side attribution file somewhere else.
telemetry=${OVERMESH_CLIENT_OBSERVED_TELEMETRY:-"$artifacts_dir/client-observed-telemetry.json"}
retained_dir="$repo_root/harness/artifacts/client-observed"
bundle=${OVERMESH_CLIENT_OBSERVED_BUNDLE:-"$retained_dir/client-observed-$run_id-evidence.json"}

mkdir -p "$artifacts_dir"

# Keep the Azure CLI token cache and any service-principal entry out of the
# operator's own ~/.azure profile, and out of every other campaign. This lives
# beside the staging directory, never inside it: the runner deletes staging
# while the campaign is still logged in, and the profile has to stay readable
# for every az invocation the runner dispatches.
azure_config_dir="$work_dir/azure-cli-config"
rm -rf "$azure_config_dir"
(umask 077 && mkdir -p "$azure_config_dir")
chmod 700 "$azure_config_dir"
export AZURE_CONFIG_DIR="$azure_config_dir"

logged_in=0
cleanup() {
  local status=$?
  if [[ "$logged_in" -eq 1 ]]; then
    az account clear --only-show-errors >/dev/null 2>&1 || true
    az logout --only-show-errors >/dev/null 2>&1 || true
  fi
  # The token cache and the service-principal entry live only here.
  rm -rf "$azure_config_dir"
  if [[ -d "$azure_config_dir" ]]; then
    echo "Failed to remove $azure_config_dir; remove it before reusing this host." >&2
  fi
  # The runner creates and deletes the staging directory itself. Remove any
  # payload it could not clean up after an abrupt termination.
  rm -rf "$staging_dir"
  return "$status"
}
trap cleanup EXIT INT TERM

# Refuse before spending a single request if the disclaimer is not published.
python3 "$runner" --contract "$contract" --check-publication --plan >/dev/null

case "$credential_mode" in
  certificate)
    # Only a file path reaches the command line; no secret material does.
    az login \
      --service-principal \
      --username "$AZURE_CLIENT_ID" \
      --certificate "$AZURE_CLIENT_CERTIFICATE_PATH" \
      --tenant "$AZURE_TENANT_ID" \
      --allow-no-subscriptions \
      --only-show-errors \
      --output none
    ;;
  workload-identity)
    # The Azure CLI has no stdin channel for a federated assertion, so the
    # short-lived token is visible in this process's argv while it logs in.
    echo "Note: the federated token appears in az login argv on this host." >&2
    az login \
      --service-principal \
      --username "$AZURE_CLIENT_ID" \
      --federated-token "$(<"$AZURE_FEDERATED_TOKEN_FILE")" \
      --tenant "$AZURE_TENANT_ID" \
      --allow-no-subscriptions \
      --only-show-errors \
      --output none
    ;;
  client-secret)
    # The Azure CLI cannot read --password from stdin: knack refuses to prompt
    # without a TTY, so piping the secret fails instead of being consumed. The
    # secret is therefore visible in this process's argv, to any local user who
    # can read /proc or run ps, for as long as the login runs. Use
    # AZURE_CLIENT_CERTIFICATE_PATH on a shared host.
    echo "Warning: the client secret appears in az login argv on this host." >&2
    echo "Prefer AZURE_CLIENT_CERTIFICATE_PATH to keep it out of argv." >&2
    az login \
      --service-principal \
      --username "$AZURE_CLIENT_ID" \
      --password "$AZURE_CLIENT_SECRET" \
      --tenant "$AZURE_TENANT_ID" \
      --allow-no-subscriptions \
      --only-show-errors \
      --output none
    ;;
esac
logged_in=1

python3 "$runner" \
  --contract "$contract" \
  --run \
  --work-root "$work_root" \
  --output "$client_evidence"

if [[ ! -f "$telemetry" ]]; then
  echo "Server telemetry is required at $telemetry before publication." >&2
  echo "Client measurements remain at $client_evidence." >&2
  exit 3
fi

python3 "$runner" \
  --contract "$contract" \
  --client-evidence "$client_evidence" \
  --telemetry "$telemetry" \
  --output "$bundle"

python3 "$runner" --contract "$contract" --validate "$bundle"
