#!/usr/bin/env bash
set -euo pipefail

required=(
  OVERMESH_LIVE_POSTURE_AUDIT_COMMAND
  OVERMESH_LIVE_POSTURE_MUTATOR
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required for the live Azure posture gate." >&2
    exit 2
  fi
done

for helper in \
  "$OVERMESH_LIVE_POSTURE_AUDIT_COMMAND" \
  "$OVERMESH_LIVE_POSTURE_MUTATOR"; do
  if [[ ! -f "$helper" || ! -x "$helper" ]]; then
    echo "$helper must be an executable helper." >&2
    exit 2
  fi
done

work_dir=${OVERMESH_LIVE_POSTURE_WORK_DIR:-.harness/live-posture}
evidence_path=${OVERMESH_LIVE_POSTURE_EVIDENCE_PATH:-"$work_dir/evidence.json"}
propagation_attempts=${OVERMESH_LIVE_POSTURE_PROPAGATION_ATTEMPTS:-36}
propagation_delay_seconds=${OVERMESH_LIVE_POSTURE_PROPAGATION_DELAY_SECONDS:-10}
baseline="$work_dir/baseline.json"
inherited_error="$work_dir/inherited-role.stderr"
path_error="$work_dir/path-condition.stderr"
recovered="$work_dir/recovered.json"
inherited_applied=0
path_applied=0

mkdir -p "$work_dir" "$(dirname "$evidence_path")"

cleanup() {
  local status=$?
  if [[ "$path_applied" == "1" ]]; then
    "$OVERMESH_LIVE_POSTURE_MUTATOR" remove-path-condition >/dev/null 2>&1 || true
  fi
  if [[ "$inherited_applied" == "1" ]]; then
    "$OVERMESH_LIVE_POSTURE_MUTATOR" remove-inherited-role >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT

run_audit() {
  local stdout=$1
  local stderr=$2
  "$OVERMESH_LIVE_POSTURE_AUDIT_COMMAND" >"$stdout" 2>"$stderr"
}

wait_for_success() {
  local output=$1
  local error=$2
  local attempt
  for attempt in $(seq 1 "$propagation_attempts"); do
    if run_audit "$output" "$error"; then
      jq -e '.apiVersion == "reconciler.overmesh.io/rbac-posture/v1"' "$output" \
        >/dev/null
      return 0
    fi
    sleep "$propagation_delay_seconds"
  done
  echo "RBAC posture did not return to a healthy state." >&2
  cat "$error" >&2
  return 1
}

wait_for_failure() {
  local expected=$1
  local output=$2
  local error=$3
  local attempt
  for attempt in $(seq 1 "$propagation_attempts"); do
    if ! run_audit "$output" "$error"; then
      if grep -Fq "$expected" "$error"; then
        return 0
      fi
    fi
    sleep "$propagation_delay_seconds"
  done
  echo "RBAC posture did not fail with the expected diagnostic: $expected" >&2
  cat "$error" >&2
  return 1
}

strip_ansi_file() {
  python3 - "$1" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
content = path.read_text(encoding="utf-8")
content = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", content)
path.write_text(content, encoding="utf-8")
PY
}

wait_for_success "$baseline" "$work_dir/baseline.stderr"

"$OVERMESH_LIVE_POSTURE_MUTATOR" apply-inherited-role
inherited_applied=1
wait_for_failure \
  "has effective blob data access to overmesh-system" \
  "$work_dir/inherited-role.stdout" \
  "$inherited_error"
"$OVERMESH_LIVE_POSTURE_MUTATOR" remove-inherited-role
inherited_applied=0
wait_for_success "$recovered" "$work_dir/inherited-recovery.stderr"

"$OVERMESH_LIVE_POSTURE_MUTATOR" apply-path-condition
path_applied=1
wait_for_failure \
  "role assignment condition depends on blob path" \
  "$work_dir/path-condition.stdout" \
  "$path_error"
"$OVERMESH_LIVE_POSTURE_MUTATOR" remove-path-condition
path_applied=0
wait_for_success "$recovered" "$work_dir/path-recovery.stderr"

strip_ansi_file "$inherited_error"
strip_ansi_file "$path_error"

python3 - \
  "$baseline" \
  "$inherited_error" \
  "$path_error" \
  "$recovered" \
  "$evidence_path" <<'PY'
import hashlib
import json
import pathlib
import sys
from datetime import datetime, timezone

baseline_path, inherited_path, path_path, recovered_path, output_path = map(
    pathlib.Path, sys.argv[1:]
)

def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
recovered = json.loads(recovered_path.read_text(encoding="utf-8"))
evidence = {
    "apiVersion": "evidence.overmesh.io/live-posture/v1",
    "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "result": "passed",
    "baseline": {
        "approvedSystemPrincipals": baseline["approvedSystemPrincipals"],
        "accounts": baseline["accounts"],
        "sha256": sha256(baseline_path),
    },
    "negativeProbes": [
        {
            "probe": "unapproved-inherited-account-role",
            "result": "rejected",
            "diagnostic": inherited_path.read_text(encoding="utf-8").strip(),
            "sha256": sha256(inherited_path),
        },
        {
            "probe": "path-dependent-abac-condition",
            "result": "rejected",
            "diagnostic": path_path.read_text(encoding="utf-8").strip(),
            "sha256": sha256(path_path),
        },
    ],
    "cleanup": {
        "result": "passed",
        "approvedSystemPrincipals": recovered["approvedSystemPrincipals"],
        "accounts": recovered["accounts"],
        "sha256": sha256(recovered_path),
    },
}
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(
    json.dumps(evidence, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

echo "Overmesh live Azure positive and negative RBAC posture gates passed."
