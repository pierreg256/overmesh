#!/usr/bin/env bash
set -euo pipefail

required=(
  OVERMESH_LIVE_EVIDENCE_PATH
  OVERMESH_LIVE_EVIDENCE_SIGNATURE_PATH
  OVERMESH_LIVE_EVIDENCE_KEY_ID
  OVERMESH_LIVE_EVIDENCE_SIGNING_CLIENT_ID
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required to sign live evidence." >&2
    exit 2
  fi
done

digest=$(sha256sum "$OVERMESH_LIVE_EVIDENCE_PATH" | awk '{print $1}')
digest_value=$(
  python3 -c \
    'import base64,sys; print(base64.urlsafe_b64encode(bytes.fromhex(sys.argv[1])).decode().rstrip("="))' \
    "$digest"
)
token=$(
  curl --fail --silent --show-error \
    -H Metadata:true \
    "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2019-08-01&resource=https%3A%2F%2Fvault.azure.net&client_id=$OVERMESH_LIVE_EVIDENCE_SIGNING_CLIENT_ID" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
)
sign_response=$(
  curl --fail --silent --show-error \
    -X POST \
    --oauth2-bearer "$token" \
    -H "Content-Type: application/json" \
    --data "{\"alg\":\"ES256\",\"value\":\"$digest_value\"}" \
    "$OVERMESH_LIVE_EVIDENCE_KEY_ID/sign?api-version=7.4"
)
signature=$(
  printf '%s' "$sign_response" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["value"])'
)
verify_response=$(
  curl --fail --silent --show-error \
    -X POST \
    --oauth2-bearer "$token" \
    -H "Content-Type: application/json" \
    --data "{\"alg\":\"ES256\",\"digest\":\"$digest_value\",\"value\":\"$signature\"}" \
    "$OVERMESH_LIVE_EVIDENCE_KEY_ID/verify?api-version=7.4"
)
verified=$(
  printf '%s' "$verify_response" |
    python3 -c 'import json,sys; print(str(json.load(sys.stdin)["value"]).lower())'
)
[[ "$verified" == "true" ]]

python3 - \
  "$digest" \
  "$OVERMESH_LIVE_EVIDENCE_KEY_ID" \
  "$signature" \
  "$OVERMESH_LIVE_EVIDENCE_SIGNATURE_PATH" <<'PY'
import hashlib
import json
import pathlib
import sys
from urllib.parse import urlsplit

digest, key_id, signature, output_path = sys.argv[1:]
parts = urlsplit(key_id)
host = "kv-" + hashlib.sha256(parts.netloc.encode("utf-8")).hexdigest()[:16]
redacted_key_id = f"https://{host}{parts.path}"
path = pathlib.Path(output_path)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(
    json.dumps(
        {
            "apiVersion": "evidence.overmesh.io/detached-signature/v1",
            "algorithm": "ES256",
            "sha256": digest,
            "keyId": redacted_key_id,
            "signature": signature,
            "verifiedByKeyVault": True,
        },
        indent=2,
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)
PY
